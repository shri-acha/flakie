use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Context;

use crate::config::TransferConfig;
use crate::source::{DownloadRequest, Source, UploadRequest};

/// A successfully downloaded object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedDownload {
    pub key: String,
    pub path: PathBuf,
}

/// A successfully uploaded object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedUpload {
    pub key: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompletedTransfer {
    key: String,
    path: PathBuf,
}

#[derive(Debug, Clone)]
enum Task {
    Download {
        key: String,
        path: PathBuf,
    },
    Upload {
        key: String,
        path: PathBuf,
        content_type: Option<String>,
    },
}

impl Task {
    fn key(&self) -> &str {
        match self {
            Self::Download { key, .. } | Self::Upload { key, .. } => key,
        }
    }

    fn path(&self) -> &Path {
        match self {
            Self::Download { path, .. } | Self::Upload { path, .. } => path,
        }
    }

    async fn execute<S: Source>(&self, source: &S) -> anyhow::Result<()> {
        match self {
            Self::Download { key, path } => source.download(key, path).await,
            Self::Upload {
                key,
                path,
                content_type,
            } => source.upload(path, key, content_type.as_deref()).await,
        }
    }
}

#[derive(Debug)]
struct QueuedTask {
    task: Task,
    attempts: u32,
}

/// Download every request with bounded concurrency and per-item retries.
///
/// Results are returned ordered by object key. If any item exhausts its
/// retries, the whole call fails and lists every failed key.
pub async fn download_all<S>(
    source: S,
    config: &TransferConfig,
    requests: Vec<DownloadRequest>,
) -> anyhow::Result<Vec<CompletedDownload>>
where
    S: Source + Clone + 'static,
{
    if requests.is_empty() {
        return Ok(Vec::new());
    }

    let tasks = requests
        .into_iter()
        .map(|request| Task::Download {
            key: request.key,
            path: request.path,
        })
        .collect();
    let completed = run_tasks(source, config, "download", tasks).await?;

    Ok(completed
        .into_iter()
        .map(|item| CompletedDownload {
            key: item.key,
            path: item.path,
        })
        .collect())
}

/// Upload every request with bounded concurrency and per-item retries.
///
/// Results are returned ordered by object key. If any item exhausts its
/// retries, the whole call fails and lists every failed key.
pub async fn upload_all<S>(
    source: S,
    config: &TransferConfig,
    requests: Vec<UploadRequest>,
) -> anyhow::Result<Vec<CompletedUpload>>
where
    S: Source + Clone + 'static,
{
    if requests.is_empty() {
        return Ok(Vec::new());
    }

    let tasks = requests
        .into_iter()
        .map(|request| Task::Upload {
            key: request.key,
            path: request.path,
            content_type: request.content_type,
        })
        .collect();
    let completed = run_tasks(source, config, "upload", tasks).await?;

    Ok(completed
        .into_iter()
        .map(|item| CompletedUpload {
            key: item.key,
            path: item.path,
        })
        .collect())
}

async fn run_tasks<S>(
    source: S,
    config: &TransferConfig,
    operation: &'static str,
    tasks: Vec<Task>,
) -> anyhow::Result<Vec<CompletedTransfer>>
where
    S: Source + Clone + 'static,
{
    if tasks.is_empty() {
        return Ok(Vec::new());
    }

    let max_attempts = config.max_attempts.max(1);
    let policy = Arc::new(RetryPolicy::new(config)?);
    let config = Arc::new(config.clone());
    let queue = Arc::new(Mutex::new(
        tasks
            .into_iter()
            .map(|task| QueuedTask { task, attempts: 0 })
            .collect::<VecDeque<_>>(),
    ));
    let completed = Arc::new(Mutex::new(Vec::<CompletedTransfer>::new()));
    let failed = Arc::new(Mutex::new(Vec::<String>::new()));

    let worker_count = config
        .concurrency
        .max(1)
        .min(queue.lock().expect("queue mutex poisoned").len());
    let mut workers = Vec::with_capacity(worker_count);
    for _ in 0..worker_count {
        workers.push(tokio::spawn(run_worker(
            source.clone(),
            Arc::clone(&queue),
            Arc::clone(&completed),
            Arc::clone(&failed),
            Arc::clone(&config),
            Arc::clone(&policy),
            operation,
        )));
    }

    for worker in workers {
        worker.await.context("transfer worker task failed")?;
    }

    let mut completed = std::mem::take(&mut *completed.lock().expect("completed mutex poisoned"));
    let mut failed = std::mem::take(&mut *failed.lock().expect("failed mutex poisoned"));
    failed.sort();

    if !failed.is_empty() {
        anyhow::bail!(
            "failed to {operation} {} item(s) (at most {max_attempts} attempts each): {}",
            failed.len(),
            failed.join(", ")
        );
    }

    completed.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(completed)
}

async fn run_worker<S: Source>(
    source: S,
    queue: Arc<Mutex<VecDeque<QueuedTask>>>,
    completed: Arc<Mutex<Vec<CompletedTransfer>>>,
    failed: Arc<Mutex<Vec<String>>>,
    config: Arc<TransferConfig>,
    policy: Arc<RetryPolicy>,
    operation: &'static str,
) {
    loop {
        let Some(mut queued) = queue.lock().expect("queue mutex poisoned").pop_front() else {
            return;
        };

        let result = if let Some(timeout) = config.attempt_timeout {
            match tokio::time::timeout(timeout, queued.task.execute(&source)).await {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!(
                    "transfer attempt timed out after {timeout:?}"
                )),
            }
        } else {
            queued.task.execute(&source).await
        };
        match result {
            Ok(()) => {
                tracing::info!(
                    operation,
                    key = %queued.task.key(),
                    path = %queued.task.path().display(),
                    "transfer completed"
                );
                completed
                    .lock()
                    .expect("completed mutex poisoned")
                    .push(CompletedTransfer {
                        key: queued.task.key().to_string(),
                        path: queued.task.path().to_path_buf(),
                    });
            }
            Err(error) => {
                queued.attempts += 1;
                let decision = if queued.attempts < config.max_attempts.max(1) {
                    policy.delay(&config, &queued, operation, &error)
                } else {
                    Ok(None)
                };
                if let Ok(Some(retry_delay)) = decision {
                    tracing::warn!(
                        operation,
                        key = %queued.task.key(),
                        attempts = queued.attempts,
                        %error,
                        "transfer failed; moving it to the back of the queue"
                    );
                    if !retry_delay.is_zero() {
                        tokio::time::sleep(retry_delay).await;
                    }
                    queue
                        .lock()
                        .expect("queue mutex poisoned")
                        .push_back(queued);
                } else {
                    let reason = match decision {
                        Err(policy_error) => {
                            format!("{error:#}; retry policy failed: {policy_error:#}")
                        }
                        _ => format!("{error:#}"),
                    };
                    tracing::error!(
                        operation,
                        key = %queued.task.key(),
                        attempts = queued.attempts,
                        %error,
                        "transfer failed; no further retries"
                    );
                    failed
                        .lock()
                        .expect("failed mutex poisoned")
                        .push(format!("{}: {reason}", queued.task.key()));
                }
            }
        }
    }
}

struct RetryPolicy {
    #[cfg(feature = "scripting")]
    script: Option<(rhai::Engine, rhai::AST)>,
}

impl RetryPolicy {
    fn new(_config: &TransferConfig) -> anyhow::Result<Self> {
        Ok(Self {
            #[cfg(feature = "scripting")]
            script: _config
                .retry_script
                .as_ref()
                .map(|script| {
                    let mut engine = rhai::Engine::new();
                    engine.set_max_operations(10_000);
                    engine.set_max_call_levels(16);
                    engine.set_max_expr_depths(32, 16);
                    engine.set_max_string_size(65_536);
                    engine.set_max_array_size(1_024);
                    engine.set_max_map_size(128);
                    let ast = engine.compile(script).context("compiling retry script")?;
                    Ok::<_, anyhow::Error>((engine, ast))
                })
                .transpose()?,
        })
    }

    fn delay(
        &self,
        config: &TransferConfig,
        queued: &QueuedTask,
        _operation: &str,
        _error: &anyhow::Error,
    ) -> anyhow::Result<Option<std::time::Duration>> {
        let delay = config
            .retry_backoff
            .as_ref()
            .map(|backoff| backoff.delay_for_attempt(queued.attempts - 1))
            .unwrap_or(config.retry_delay);
        #[cfg(feature = "scripting")]
        if let Some((engine, ast)) = &self.script {
            let mut scope = rhai::Scope::new();
            scope.push_constant("key", queued.task.key().to_string());
            scope.push_constant("operation", _operation.to_string());
            scope.push_constant("attempt", i64::from(queued.attempts));
            scope.push_constant("max_attempts", i64::from(config.max_attempts.max(1)));
            scope.push_constant("error", format!("{_error:#}"));
            scope.push_constant(
                "default_delay_ms",
                delay.as_millis().min(i64::MAX as u128) as i64,
            );
            let millis = engine
                .eval_ast_with_scope::<i64>(&mut scope, ast)
                .context("evaluating retry script")?;
            anyhow::ensure!(
                millis >= -1,
                "retry script must return -1 or a nonnegative delay in milliseconds"
            );
            return Ok((millis >= 0).then(|| std::time::Duration::from_millis(millis as u64)));
        }
        Ok(Some(delay))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;

    use super::{download_all, upload_all};
    use crate::config::TransferConfig;
    use crate::source::{DownloadRequest, Source, UploadRequest};

    #[derive(Clone)]
    struct MockSource {
        attempts: Arc<Mutex<HashMap<String, usize>>>,
        outcomes: Arc<Mutex<HashMap<String, VecDeque<bool>>>>,
        delay: Duration,
    }

    impl MockSource {
        fn new(outcomes: &[(&str, Vec<bool>)]) -> Self {
            Self {
                delay: Duration::ZERO,
                attempts: Arc::new(Mutex::new(HashMap::new())),
                outcomes: Arc::new(Mutex::new(
                    outcomes
                        .iter()
                        .map(|(key, values)| {
                            (
                                key.to_string(),
                                values.iter().copied().collect::<VecDeque<_>>(),
                            )
                        })
                        .collect(),
                )),
            }
        }

        fn attempt_count(&self, key: &str) -> usize {
            self.attempts
                .lock()
                .expect("attempts mutex poisoned")
                .get(key)
                .copied()
                .unwrap_or_default()
        }

        fn next_outcome(&self, key: &str) -> anyhow::Result<()> {
            let attempt = {
                let mut attempts = self.attempts.lock().expect("attempts mutex poisoned");
                let attempt = attempts.entry(key.to_string()).or_insert(0);
                *attempt += 1;
                *attempt
            };
            let succeeds = self
                .outcomes
                .lock()
                .expect("outcomes mutex poisoned")
                .get_mut(key)
                .and_then(|outcomes| outcomes.pop_front())
                .unwrap_or(true);
            if succeeds {
                Ok(())
            } else {
                anyhow::bail!("mock transfer failure on attempt {attempt}")
            }
        }
    }

    #[async_trait]
    impl Source for MockSource {
        async fn download(&self, key: &str, _path: &Path) -> anyhow::Result<()> {
            let result = self.next_outcome(key);
            tokio::time::sleep(self.delay).await;
            result
        }

        async fn upload(
            &self,
            _path: &Path,
            key: &str,
            _content_type: Option<&str>,
        ) -> anyhow::Result<()> {
            self.next_outcome(key)
        }

        async fn exists(&self, _key: &str) -> anyhow::Result<bool> {
            Ok(true)
        }

        async fn delete(&self, _key: &str) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn transfer_config() -> TransferConfig {
        TransferConfig {
            concurrency: 2,
            max_attempts: 3,
            retry_delay: Duration::ZERO,
            ..TransferConfig::default()
        }
    }

    #[tokio::test]
    async fn failed_download_is_requeued_and_retried() {
        let store = MockSource::new(&[("a.mbtiles", vec![false, true]), ("b.mbtiles", vec![true])]);
        let requests = vec![
            DownloadRequest::new("a.mbtiles", "/tmp/a.mbtiles"),
            DownloadRequest::new("b.mbtiles", "/tmp/b.mbtiles"),
        ];

        let completed = download_all(store.clone(), &transfer_config(), requests)
            .await
            .unwrap();

        assert_eq!(completed.len(), 2);
        assert_eq!(completed[0].key, "a.mbtiles");
        assert_eq!(completed[1].key, "b.mbtiles");
        assert_eq!(store.attempt_count("a.mbtiles"), 2);
        assert_eq!(store.attempt_count("b.mbtiles"), 1);
    }

    #[tokio::test]
    async fn download_that_keeps_failing_is_reported_after_max_attempts() {
        let store = MockSource::new(&[
            ("a.mbtiles", vec![false, false, false]),
            ("b.mbtiles", vec![true]),
        ]);
        let requests = vec![
            DownloadRequest::new("a.mbtiles", "/tmp/a.mbtiles"),
            DownloadRequest::new("b.mbtiles", "/tmp/b.mbtiles"),
        ];

        let result = download_all(store.clone(), &transfer_config(), requests).await;

        assert!(result.is_err());
        assert_eq!(store.attempt_count("a.mbtiles"), 3);
        assert_eq!(store.attempt_count("b.mbtiles"), 1);
    }

    #[tokio::test]
    async fn failed_upload_is_requeued_and_retried() {
        let store = MockSource::new(&[("a.mbtiles", vec![false, true]), ("b.mbtiles", vec![true])]);
        let requests = vec![
            UploadRequest::new("/tmp/a.mbtiles", "a.mbtiles", None::<String>),
            UploadRequest::new("/tmp/b.mbtiles", "b.mbtiles", None::<String>),
        ];

        let completed = upload_all(store.clone(), &transfer_config(), requests)
            .await
            .unwrap();

        assert_eq!(completed.len(), 2);
        assert_eq!(completed[0].key, "a.mbtiles");
        assert_eq!(completed[1].key, "b.mbtiles");
        assert_eq!(store.attempt_count("a.mbtiles"), 2);
        assert_eq!(store.attempt_count("b.mbtiles"), 1);
    }

    #[tokio::test]
    async fn empty_requests_return_empty() {
        let store = MockSource::new(&[]);

        let downloads = download_all(store.clone(), &transfer_config(), Vec::new())
            .await
            .unwrap();
        let uploads = upload_all(store.clone(), &transfer_config(), Vec::new())
            .await
            .unwrap();

        assert!(downloads.is_empty());
        assert!(uploads.is_empty());
    }

    #[tokio::test]
    async fn timeout_retries_and_reports_the_cause() {
        let mut store = MockSource::new(&[]);
        store.delay = Duration::from_secs(60);
        let config = TransferConfig {
            attempt_timeout: Some(Duration::from_millis(1)),
            ..transfer_config()
        };
        let error = download_all(
            store.clone(),
            &config,
            vec![DownloadRequest::new("slow", "slow")],
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        assert_eq!(store.attempt_count("slow"), 3);
    }

    #[test]
    fn queue_backoff_overrides_fixed_delay() {
        let config = TransferConfig {
            retry_backoff: Some(crate::BackoffConfig::new(1, Duration::from_millis(2))),
            ..transfer_config()
        };
        let queued = super::QueuedTask {
            task: super::Task::Download {
                key: "key".into(),
                path: "path".into(),
            },
            attempts: 3,
        };
        let policy = super::RetryPolicy::new(&config).unwrap();
        assert_eq!(
            policy
                .delay(&config, &queued, "download", &anyhow::anyhow!("failed"))
                .unwrap(),
            Some(Duration::from_millis(8))
        );
    }

    #[cfg(feature = "scripting")]
    #[tokio::test]
    async fn script_can_stop_retries_using_failure_context() {
        let store = MockSource::new(&[("stop", vec![false, true])]);
        let config = TransferConfig {
            retry_script: Some(
                r#"
                if key == "stop" && operation == "download" && attempt == 1
                    && max_attempts == 3 && error.contains("mock transfer failure") {
                    -1
                } else { default_delay_ms }
            "#
                .into(),
            ),
            ..transfer_config()
        };
        let error = download_all(
            store.clone(),
            &config,
            vec![DownloadRequest::new("stop", "stop")],
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("stop"));
        assert_eq!(store.attempt_count("stop"), 1);
    }

    #[cfg(feature = "scripting")]
    #[tokio::test]
    async fn script_can_retry_uploads() {
        let store = MockSource::new(&[("retry", vec![false, true])]);
        let config = TransferConfig {
            retry_script: Some("if operation == \"upload\" { 0 } else { -1 }".into()),
            ..transfer_config()
        };
        let completed = upload_all(
            store.clone(),
            &config,
            vec![UploadRequest::new("retry", "retry", None::<String>)],
        )
        .await
        .unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(store.attempt_count("retry"), 2);
    }

    #[cfg(feature = "scripting")]
    #[tokio::test]
    async fn invalid_script_fails_before_transferring() {
        let store = MockSource::new(&[]);
        let config = TransferConfig {
            retry_script: Some("let =".into()),
            ..transfer_config()
        };
        let error = download_all(
            store.clone(),
            &config,
            vec![DownloadRequest::new("key", "path")],
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("compiling retry script"));
        assert_eq!(store.attempt_count("key"), 0);
    }

    #[cfg(feature = "scripting")]
    #[tokio::test]
    async fn script_errors_and_infinite_loops_stop_the_item() {
        for script in ["loop {}", "true", "-2", "1 / 0"] {
            let store = MockSource::new(&[("key", vec![false, true])]);
            let config = TransferConfig {
                retry_script: Some(script.into()),
                ..transfer_config()
            };
            let error = download_all(
                store.clone(),
                &config,
                vec![DownloadRequest::new("key", "path")],
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("retry policy failed"), "{error}");
            assert_eq!(store.attempt_count("key"), 1);
        }
    }
}
