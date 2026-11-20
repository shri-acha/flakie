use std::collections::HashSet;
use std::path::PathBuf;

use crate::config::TransferConfig;
use crate::engine::{run_batch, BatchReport, Keyed};
use crate::source::{DownloadRequest, Source, UploadRequest};

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

/// Download every request, reporting successes and failures separately.
///
/// A failed download never stops the others; inspect
/// [`BatchReport::failed`] to see what is missing. Requests that share a
/// destination path are rejected before anything is transferred, because
/// they would race on the same file.
pub async fn download_batch<S>(
    source: S,
    config: &TransferConfig,
    requests: Vec<DownloadRequest>,
) -> anyhow::Result<BatchReport<DownloadRequest, ()>>
where
    S: Source + Clone + 'static,
{
    reject_duplicates(requests.iter().map(|request| &request.path), |path| {
        format!("destination path {}", path.display())
    })?;
    run_batch(
        config,
        "download",
        requests,
        move |request: DownloadRequest| {
            let source = source.clone();
            async move { source.download(&request.key, &request.path).await }
        },
    )
    .await
}

/// Upload every request, reporting successes and failures separately.
///
/// A failed upload never stops the others. Requests that share an object key
/// are rejected before anything is transferred, because they would race on
/// the same object.
pub async fn upload_batch<S>(
    source: S,
    config: &TransferConfig,
    requests: Vec<UploadRequest>,
) -> anyhow::Result<BatchReport<UploadRequest, ()>>
where
    S: Source + Clone + 'static,
{
    reject_duplicates(requests.iter().map(|request| &request.key), |key| {
        format!("object key {key}")
    })?;
    run_batch(config, "upload", requests, move |request: UploadRequest| {
        let source = source.clone();
        async move {
            source
                .upload(&request.path, &request.key, request.content_type.as_deref())
                .await
        }
    })
    .await
}

/// Download every request; fail with a [`BatchError`](crate::BatchError) if
/// any download fails.
///
/// Results are returned ordered by object key. Use [`download_batch`] to keep
/// the successful downloads when some fail.
pub async fn download_all<S>(
    source: S,
    config: &TransferConfig,
    requests: Vec<DownloadRequest>,
) -> anyhow::Result<Vec<CompletedDownload>>
where
    S: Source + Clone + 'static,
{
    let mut completed: Vec<_> = download_batch(source, config, requests)
        .await?
        .into_result()?
        .into_iter()
        .map(|(request, ())| CompletedDownload {
            key: request.key,
            path: request.path,
        })
        .collect();
    completed.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(completed)
}

/// Upload every request; fail with a [`BatchError`](crate::BatchError) if any
/// upload fails.
///
/// Results are returned ordered by object key. Use [`upload_batch`] to keep
/// the successful uploads when some fail.
pub async fn upload_all<S>(
    source: S,
    config: &TransferConfig,
    requests: Vec<UploadRequest>,
) -> anyhow::Result<Vec<CompletedUpload>>
where
    S: Source + Clone + 'static,
{
    let mut completed: Vec<_> = upload_batch(source, config, requests)
        .await?
        .into_result()?
        .into_iter()
        .map(|(request, ())| CompletedUpload {
            key: request.key,
            path: request.path,
        })
        .collect();
    completed.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(completed)
}

fn reject_duplicates<'a, V>(
    values: impl Iterator<Item = &'a V>,
    describe: impl Fn(&V) -> String,
) -> anyhow::Result<()>
where
    V: std::hash::Hash + Eq + ?Sized + 'a,
{
    let mut seen = HashSet::new();
    for value in values {
        anyhow::ensure!(
            seen.insert(value),
            "{} appears in more than one request",
            describe(value)
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;

    use super::*;
    use crate::engine::{run_all, BatchError};

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
                        .map(|(key, values)| (key.to_string(), values.iter().copied().collect()))
                        .collect(),
                )),
            }
        }

        fn attempt_count(&self, key: &str) -> usize {
            self.attempts
                .lock()
                .unwrap()
                .get(key)
                .copied()
                .unwrap_or_default()
        }

        fn next_outcome(&self, key: &str) -> anyhow::Result<()> {
            let attempt = {
                let mut attempts = self.attempts.lock().unwrap();
                let attempt = attempts.entry(key.to_string()).or_insert(0);
                *attempt += 1;
                *attempt
            };
            let succeeds = self
                .outcomes
                .lock()
                .unwrap()
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

        let error = download_all(store.clone(), &transfer_config(), requests)
            .await
            .unwrap_err();

        let batch = error.downcast_ref::<BatchError>().expect("BatchError");
        assert_eq!(batch.failures.len(), 1);
        assert_eq!(batch.failures[0].key, "a");
        assert_eq!(batch.failures[0].attempts, 3);
        assert_eq!(store.attempt_count("a"), 3);
        assert_eq!(store.attempt_count("b"), 1);
    }

    #[tokio::test]
    async fn download_batch_keeps_successes_when_one_item_fails() {
        let store = MockSource::new(&[
            ("a", vec![true]),
            ("bad", vec![false, false, false]),
            ("c", vec![false, true]),
        ]);
        let requests = vec![
            DownloadRequest::new("a", "/tmp/a"),
            DownloadRequest::new("bad", "/tmp/bad"),
            DownloadRequest::new("c", "/tmp/c"),
        ];

        let report = download_batch(store.clone(), &transfer_config(), requests)
            .await
            .unwrap();

        assert!(!report.is_success());
        let completed: Vec<_> = report
            .completed
            .iter()
            .map(|(request, ())| request.key.as_str())
            .collect();
        assert_eq!(completed, ["a", "c"]);
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.failed[0].item.key, "bad");
        assert_eq!(report.failed[0].attempts, 3);
        assert!(format!("{:#}", report.failed[0].error).contains("attempt 3"));
    }

    #[tokio::test]
    async fn duplicate_destinations_are_rejected_before_transferring() {
        let store = MockSource::new(&[]);
        let error = download_all(
            store.clone(),
            &transfer_config(),
            vec![
                DownloadRequest::new("a", "/tmp/same"),
                DownloadRequest::new("b", "/tmp/./same"),
            ],
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("more than one request"),
            "{error}"
        );
        assert_eq!(store.attempt_count("a"), 0);

        let error = upload_all(
            store.clone(),
            &transfer_config(),
            vec![
                UploadRequest::new("/tmp/a", "key", None::<String>),
                UploadRequest::new("/tmp/b", "key", None::<String>),
            ],
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("object key key"), "{error}");
        assert_eq!(store.attempt_count("key"), 0);
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
            error.contains("https://example.invalid/x: connection reset (after 3 attempt(s))"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn panicking_attempt_is_retried_like_a_failure() {
        let calls = Arc::new(Mutex::new(0));
        let job_calls = Arc::clone(&calls);
        let completed = run_all(
            &transfer_config(),
            "work",
            vec!["key".to_string()],
            move |_key: String| {
                let calls = Arc::clone(&job_calls);
                async move {
                    let call = {
                        let mut calls = calls.lock().unwrap();
                        *calls += 1;
                        *calls
                    };
                    if call == 1 {
                        panic!("boom");
                    }
                    Ok(call)
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(completed, vec![("key".to_string(), 2)]);

        let error = run_all(
            &transfer_config(),
            "work",
            vec!["key".to_string()],
            |_key: String| async {
                if true {
                    panic!("always");
                }
                Ok(())
            },
        )
        .await
        .unwrap_err();
        assert!(
            error.to_string().contains("attempt panicked: always"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn dropping_the_batch_cancels_in_flight_attempts() {
        let finished = Arc::new(Mutex::new(false));
        let job_finished = Arc::clone(&finished);
        let config = transfer_config();
        let batch = run_all(
            &config,
            "work",
            vec!["key".to_string()],
            move |_key: String| {
                let finished = Arc::clone(&job_finished);
                async move {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    *finished.lock().unwrap() = true;
                    Ok(())
                }
            },
        );
        assert!(tokio::time::timeout(Duration::from_millis(10), batch)
            .await
            .is_err());
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!*finished.lock().unwrap());
    }

    #[test]
    fn queue_backoff_overrides_fixed_delay() {
        let config = TransferConfig {
            retry_backoff: Some(crate::BackoffConfig::new(1, Duration::from_millis(2))),
            ..transfer_config()
        };
        let policy = crate::engine::RetryPolicy::new(&config).unwrap();
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
