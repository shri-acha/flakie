use std::path::{Path, PathBuf};

use async_trait::async_trait;

/// A data source that can move individual objects.
///
/// Implementations are expected to be cheap to clone so the transfer engine
/// can share one source across several concurrent workers.
#[async_trait]
pub trait Source: Send + Sync {
    /// Download `key` to `path`.
    async fn download(&self, key: &str, path: &Path) -> anyhow::Result<()>;

    /// Upload the file at `path` as `key`.
    async fn upload(
        &self,
        path: &Path,
        key: &str,
        content_type: Option<&str>,
    ) -> anyhow::Result<()>;

    /// Return whether `key` exists.
    async fn exists(&self, key: &str) -> anyhow::Result<bool>;

    /// Delete `key`.
    async fn delete(&self, key: &str) -> anyhow::Result<()>;
}

/// One object to download to a local path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadRequest {
    pub key: String,
    pub path: PathBuf,
}

impl DownloadRequest {
    pub fn new(key: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            key: key.into(),
            path: path.into(),
        }
    }
}

/// One local file to upload under an object key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadRequest {
    pub path: PathBuf,
    pub key: String,
    pub content_type: Option<String>,
}

impl UploadRequest {
    pub fn new(
        path: impl Into<PathBuf>,
        key: impl Into<String>,
        content_type: Option<impl Into<String>>,
    ) -> Self {
        Self {
            path: path.into(),
            key: key.into(),
            content_type: content_type.map(Into::into),
        }
    }
}
