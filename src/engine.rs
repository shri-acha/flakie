use std::collections::VecDeque;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Context;

use crate::config::TransferConfig;
use crate::source::{DownloadRequest, Source, UploadRequest};

/// An item with a stable key used in logs, error messages, and retry scripts.
pub trait Keyed {
    /// Identifier for this item, such as an object key or URL.
    fn key(&self) -> &str;
}

impl Keyed for String {
    fn key(&self) -> &str {
        self
    }
}

impl Keyed for DownloadRequest {
    fn key(&self) -> &str {
        &self.key
    }
}

impl Keyed for UploadRequest {
    fn key(&self) -> &str {
        &self.key
    }
}

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

#[derive(Debug)]
struct QueuedItem<T> {
    index: usize,
    item: T,
    attempts: u32,
}

struct Shared<T, O, F> {
    queue: Mutex<VecDeque<QueuedItem<T>>>,
    completed: Mutex<Vec<(usize, T, O)>>,
    failed: Mutex<Vec<String>>,
    config: TransferConfig,
    policy: RetryPolicy,
    operation: String,
    run: F,
}

/// Run `run` for every item with bounded concurrency and per-item retries.
///
/// This is the generic engine behind [`download_all`] and [`upload_all`]. Each
/// attempt receives a clone of the item, so the job can be retried. `operation`
/// names the work in logs, errors, and retry scripts.
///
/// Results are returned in the order the items were given. If any item
/// exhausts its retries, the whole call fails and lists every failed key.
pub async fn run_all<T, O, F, Fut>(
    config: &TransferConfig,
    operation: &str,
    items: Vec<T>,
    run: F,
) -> anyhow::Result<Vec<(T, O)>>
where
    T: Keyed + Clone + Send + 'static,
    O: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<O>> + Send + 'static,
{
    if items.is_empty() {
        return Ok(Vec::new());
    }

    let max_attempts = config.max_attempts.max(1);
    let worker_count = config.concurrency.max(1).min(items.len());
    let shared = Arc::new(Shared {
        queue: Mutex::new(
            items
                .into_iter()
                .enumerate()
                .map(|(index, item)| QueuedItem {
                    index,
                    item,
                    attempts: 0,
                })
                .collect(),
        ),
        completed: Mutex::new(Vec::new()),
        failed: Mutex::new(Vec::new()),
        policy: RetryPolicy::new(config)?,
        config: config.clone(),
        operation: operation.to_string(),
        run,
    });

    let workers: Vec<_> = (0..worker_count)
        .map(|_| tokio::spawn(run_worker(Arc::clone(&shared))))
        .collect();
    for worker in workers {
        worker.await.context("transfer worker task failed")?;
    }

    let mut completed =
        std::mem::take(&mut *shared.completed.lock().expect("completed mutex poisoned"));
    let mut failed = std::mem::take(&mut *shared.failed.lock().expect("failed mutex poisoned"));
    failed.sort();

    if !failed.is_empty() {
        anyhow::bail!(
            "failed to {operation} {} item(s) (at most {max_attempts} attempts each): {}",
            failed.len(),
            failed.join(", ")
        );
    }

    completed.sort_by_key(|(index, _, _)| *index);
    Ok(completed
        .into_iter()
        .map(|(_, item, output)| (item, output))
        .collect())
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
    let completed = run_all(
        config,
        "download",
        requests,
        move |request: DownloadRequest| {
            let source = source.clone();
            async move { source.download(&request.key, &request.path).await }
        },
    )
    .await?;

    let mut completed: Vec<_> = completed
        .into_iter()
        .map(|(request, ())| CompletedDownload {
            key: request.key,
            path: request.path,
        })
        .collect();
    completed.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(completed)
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
    let completed = run_all(config, "upload", requests, move |request: UploadRequest| {
        let source = source.clone();
        async move {
            source
                .upload(&request.path, &request.key, request.content_type.as_deref())
                .await
        }
    })
    .await?;

    let mut completed: Vec<_> = completed
        .into_iter()
        .map(|(request, ())| CompletedUpload {
            key: request.key,
            path: request.path,
        })
        .collect();
    completed.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(completed)
}

async fn run_worker<T, O, F, Fut>(shared: Arc<Shared<T, O, F>>)
where
    T: Keyed + Clone + Send + 'static,
    O: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<O>> + Send + 'static,
{
    let config = &shared.config;
    let operation = shared.operation.as_str();
    loop {
        let Some(mut queued) = shared
            .queue
            .lock()
            .expect("queue mutex poisoned")
            .pop_front()
        else {
            return;
        };

        let attempt = (shared.run)(queued.item.clone());
        let result = if let Some(timeout) = config.attempt_timeout {
            match tokio::time::timeout(timeout, attempt).await {
                Ok(result) => result,
                Err(_) => Err(anyhow::anyhow!("attempt timed out after {timeout:?}")),
            }
        } else {
            attempt.await
        };
        match result {
            Ok(output) => {
                tracing::info!(operation, key = %queued.item.key(), "item completed");
                shared
                    .completed
                    .lock()
                    .expect("completed mutex poisoned")
                    .push((queued.index, queued.item, output));
            }
            Err(error) => {
                queued.attempts += 1;
                let decision = if queued.attempts < config.max_attempts.max(1) {
                    shared.policy.delay(
                        config,
                        queued.item.key(),
                        queued.attempts,
                        operation,
                        &error,
                    )
                } else {
                    Ok(None)
                };
                if let Ok(Some(retry_delay)) = decision {
                    tracing::warn!(
                        operation,
                        key = %queued.item.key(),
                        attempts = queued.attempts,
                        %error,
                        "item failed; moving it to the back of the queue"
                    );
                    if !retry_delay.is_zero() {
                        tokio::time::sleep(retry_delay).await;
                    }
                    shared
                        .queue
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
                        key = %queued.item.key(),
                        attempts = queued.attempts,
                        %error,
                        "item failed; no further retries"
                    );
                    shared
                        .failed
                        .lock()
                        .expect("failed mutex poisoned")
                        .push(format!("{}: {reason}", queued.item.key()));
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
        _key: &str,
        attempts: u32,
        _operation: &str,
        _error: &anyhow::Error,
    ) -> anyhow::Result<Option<std::time::Duration>> {
        let delay = config
            .retry_backoff
            .as_ref()
            .map(|backoff| backoff.delay_for_attempt(attempts - 1))
            .unwrap_or(config.retry_delay);
        #[cfg(feature = "scripting")]
        if let Some((engine, ast)) = &self.script {
            let mut scope = rhai::Scope::new();
            scope.push_constant("key", _key.to_string());
            scope.push_constant("operation", _operation.to_string());
            scope.push_constant("attempt", i64::from(attempts));
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

    use super::{download_all, run_all, upload_all};
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
        let store = MockSource::new(&[("a", vec![false, true]), ("b", vec![true])]);
        let requests = vec![
            DownloadRequest::new("a", "/tmp/a"),
            DownloadRequest::new("b", "/tmp/b"),
        ];

        let completed = download_all(store.clone(), &transfer_config(), requests)
            .await
            .unwrap();

        assert_eq!(completed.len(), 2);
        assert_eq!(completed[0].key, "a");
        assert_eq!(completed[1].key, "b");
        assert_eq!(store.attempt_count("a"), 2);
        assert_eq!(store.attempt_count("b"), 1);
    }

    #[tokio::test]
    async fn download_that_keeps_failing_is_reported_after_max_attempts() {
        let store = MockSource::new(&[("a", vec![false, false, false]), ("b", vec![true])]);
        let requests = vec![
            DownloadRequest::new("a", "/tmp/a"),
            DownloadRequest::new("b", "/tmp/b"),
        ];

        let result = download_all(store.clone(), &transfer_config(), requests).await;

        assert!(result.is_err());
        assert_eq!(store.attempt_count("a"), 3);
        assert_eq!(store.attempt_count("b"), 1);
    }

    #[tokio::test]
    async fn failed_upload_is_requeued_and_retried() {
        let store = MockSource::new(&[("a", vec![false, true]), ("b", vec![true])]);
        let requests = vec![
            UploadRequest::new("/tmp/a", "a", None::<String>),
            UploadRequest::new("/tmp/b", "b", None::<String>),
        ];

        let completed = upload_all(store.clone(), &transfer_config(), requests)
            .await
            .unwrap();

        assert_eq!(completed.len(), 2);
        assert_eq!(completed[0].key, "a");
        assert_eq!(completed[1].key, "b");
        assert_eq!(store.attempt_count("a"), 2);
        assert_eq!(store.attempt_count("b"), 1);
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

    #[tokio::test]
    async fn run_all_retries_arbitrary_jobs_and_keeps_input_order() {
        let store = MockSource::new(&[("z", vec![false, true]), ("a", vec![true])]);
        let jobs = vec!["z".to_string(), "a".to_string()];
        let job_store = store.clone();
        let completed = run_all(&transfer_config(), "measure", jobs, move |key: String| {
            let store = job_store.clone();
            async move {
                store.next_outcome(&key)?;
                Ok(key.len() + store.attempt_count(&key))
            }
        })
        .await
        .unwrap();

        assert_eq!(completed, vec![("z".to_string(), 3), ("a".to_string(), 2)]);
        assert_eq!(store.attempt_count("z"), 2);
    }

    #[tokio::test]
    async fn run_all_reports_operation_and_key_on_failure() {
        let error = run_all(
            &transfer_config(),
            "fetch",
            vec!["https://example.invalid/x".to_string()],
            |_key: String| async { Err::<(), _>(anyhow::anyhow!("connection reset")) },
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.starts_with("failed to fetch 1 item(s)"), "{error}");
        assert!(
            error.contains("https://example.invalid/x: connection reset"),
            "{error}"
        );
    }

    #[test]
    fn queue_backoff_overrides_fixed_delay() {
        let config = TransferConfig {
            retry_backoff: Some(crate::BackoffConfig::new(1, Duration::from_millis(2))),
            ..transfer_config()
        };
        let policy = super::RetryPolicy::new(&config).unwrap();
        assert_eq!(
            policy
                .delay(&config, "key", 3, "download", &anyhow::anyhow!("failed"))
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
