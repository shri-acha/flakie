use std::collections::VecDeque;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use anyhow::Context;
use tokio::task::{JoinError, JoinHandle, JoinSet};

use crate::config::TransferConfig;

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

/// An item that exhausted its retries or was stopped by the retry policy.
#[derive(Debug)]
pub struct FailedItem<T> {
    pub item: T,
    /// Number of attempts made, including the last one.
    pub attempts: u32,
    /// Error from the last attempt. If the retry policy itself failed, that
    /// failure is attached as context.
    pub error: anyhow::Error,
}

/// Outcome of a batch whose items succeed or fail independently.
///
/// Both lists are in the order the items were submitted.
#[derive(Debug)]
pub struct BatchReport<T, O> {
    /// Name of the operation, as passed to [`run_batch`].
    pub operation: String,
    pub completed: Vec<(T, O)>,
    pub failed: Vec<FailedItem<T>>,
}

impl<T: Keyed, O> BatchReport<T, O> {
    /// Whether every item completed.
    pub fn is_success(&self) -> bool {
        self.failed.is_empty()
    }

    /// Return the completed items, or a [`BatchError`] describing every
    /// failure if any item failed.
    pub fn into_result(self) -> Result<Vec<(T, O)>, BatchError> {
        if self.failed.is_empty() {
            return Ok(self.completed);
        }
        let mut failures: Vec<_> = self
            .failed
            .into_iter()
            .map(|failed| ItemFailure {
                key: failed.item.key().to_string(),
                attempts: failed.attempts,
                error: failed.error,
            })
            .collect();
        failures.sort_by(|left, right| left.key.cmp(&right.key));
        Err(BatchError {
            operation: self.operation,
            failures,
        })
    }
}

/// One failed item inside a [`BatchError`].
#[derive(Debug)]
pub struct ItemFailure {
    pub key: String,
    pub attempts: u32,
    pub error: anyhow::Error,
}

/// Error returned by the all-or-nothing helpers when any item failed.
///
/// It can be recovered from an [`anyhow::Error`] with `downcast_ref`.
/// Failures are sorted by key.
#[derive(Debug)]
pub struct BatchError {
    pub operation: String,
    pub failures: Vec<ItemFailure>,
}

impl fmt::Display for BatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "failed to {} {} item(s): ",
            self.operation,
            self.failures.len()
        )?;
        for (index, failure) in self.failures.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(
                f,
                "{}: {:#} (after {} attempt(s))",
                failure.key, failure.error, failure.attempts
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for BatchError {}

#[derive(Debug)]
struct QueuedItem<T> {
    index: usize,
    item: T,
    attempts: u32,
}

struct Shared<T, O, F> {
    queue: Mutex<VecDeque<QueuedItem<T>>>,
    completed: Mutex<Vec<(usize, T, O)>>,
    failed: Mutex<Vec<(usize, FailedItem<T>)>>,
    config: TransferConfig,
    policy: RetryPolicy,
    operation: String,
    run: F,
}

/// Run `run` for every item with bounded concurrency and per-item retries,
/// reporting successes and failures separately.
///
/// One item failing never stops the others. Each attempt receives a clone of
/// the item and runs as its own Tokio task: a panic counts as a failed
/// attempt, and an attempt that exceeds `attempt_timeout` is cancelled.
/// Dropping the returned future cancels all in-flight attempts.
///
/// The outer `Err` is reserved for problems with the batch itself, such as a
/// retry script that does not compile.
pub async fn run_batch<T, O, F, Fut>(
    config: &TransferConfig,
    operation: &str,
    items: Vec<T>,
    run: F,
) -> anyhow::Result<BatchReport<T, O>>
where
    T: Keyed + Clone + Send + 'static,
    O: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = anyhow::Result<O>> + Send + 'static,
{
    if items.is_empty() {
        return Ok(BatchReport {
            operation: operation.to_string(),
            completed: Vec::new(),
            failed: Vec::new(),
        });
    }

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

    // Dropping a JoinSet aborts its tasks, so cancelling this future (or
    // returning early) does not leave workers running in the background.
    let mut workers = JoinSet::new();
    for _ in 0..worker_count {
        workers.spawn(run_worker(Arc::clone(&shared)));
    }
    while let Some(joined) = workers.join_next().await {
        joined.context("queue worker failed")?;
    }

    let mut completed =
        std::mem::take(&mut *shared.completed.lock().expect("completed mutex poisoned"));
    let mut failed = std::mem::take(&mut *shared.failed.lock().expect("failed mutex poisoned"));
    completed.sort_by_key(|(index, _, _)| *index);
    failed.sort_by_key(|(index, _)| *index);

    Ok(BatchReport {
        operation: operation.to_string(),
        completed: completed
            .into_iter()
            .map(|(_, item, output)| (item, output))
            .collect(),
        failed: failed.into_iter().map(|(_, failed)| failed).collect(),
    })
}

/// Like [`run_batch`], but fails with a [`BatchError`] if any item fails.
///
/// Results are returned in the order the items were given.
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
    Ok(run_batch(config, operation, items, run)
        .await?
        .into_result()?)
}

/// A spawned task that is aborted when its handle is dropped.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

fn join_error(error: JoinError) -> anyhow::Error {
    if !error.is_panic() {
        return anyhow::anyhow!("attempt was cancelled");
    }
    let payload = error.into_panic();
    let message = payload
        .downcast_ref::<&str>()
        .map(|message| message.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string());
    anyhow::anyhow!("attempt panicked: {message}")
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

        let attempt = AbortOnDrop(tokio::spawn((shared.run)(queued.item.clone())));
        let joined = match config.attempt_timeout {
            Some(timeout) => tokio::time::timeout(timeout, attempt)
                .await
                .unwrap_or_else(|_| {
                    Ok(Err(anyhow::anyhow!("attempt timed out after {timeout:?}")))
                }),
            None => attempt.await,
        };
        let result = joined.unwrap_or_else(|error| Err(join_error(error)));

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
                match decision {
                    Ok(Some(retry_delay)) => {
                        tracing::warn!(
                            operation,
                            key = %queued.item.key(),
                            attempts = queued.attempts,
                            error = format!("{error:#}"),
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
                    }
                    decision => {
                        let error = match decision {
                            Err(policy_error) => {
                                error.context(format!("retry policy failed: {policy_error:#}"))
                            }
                            _ => error,
                        };
                        tracing::error!(
                            operation,
                            key = %queued.item.key(),
                            attempts = queued.attempts,
                            error = format!("{error:#}"),
                            "item failed; no further retries"
                        );
                        shared.failed.lock().expect("failed mutex poisoned").push((
                            queued.index,
                            FailedItem {
                                item: queued.item,
                                attempts: queued.attempts,
                                error,
                            },
                        ));
                    }
                }
            }
        }
    }
}

pub(crate) struct RetryPolicy {
    #[cfg(feature = "scripting")]
    script: Option<(rhai::Engine, rhai::AST)>,
}

impl RetryPolicy {
    pub(crate) fn new(_config: &TransferConfig) -> anyhow::Result<Self> {
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

    pub(crate) fn delay(
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
