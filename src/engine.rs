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
            max_attempts,
            config.retry_delay,
            operation,
        )));
    }

    for worker in workers {
        worker.await.context("transfer worker task failed")?;
    }

    let mut completed =
        std::mem::take(&mut *completed.lock().expect("completed mutex poisoned"));
    let failed = std::mem::take(&mut *failed.lock().expect("failed mutex poisoned"));

    if !failed.is_empty() {
        anyhow::bail!(
            "failed to {operation} {} item(s) after {max_attempts} attempts each: {}",
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
    max_attempts: u32,
    retry_delay: std::time::Duration,
    operation: &'static str,
) {
    loop {
        let Some(mut queued) = queue
            .lock()
            .expect("queue mutex poisoned")
            .pop_front()
        else {
            return;
        };

        match queued.task.execute(&source).await {
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
                if queued.attempts < max_attempts {
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
                    tracing::error!(
                        operation,
                        key = %queued.task.key(),
                        attempts = queued.attempts,
                        %error,
                        "transfer failed after max attempts"
                    );
                    failed
                        .lock()
                        .expect("failed mutex poisoned")
                        .push(queued.task.key().to_string());
                }
            }
        }
    }
}

