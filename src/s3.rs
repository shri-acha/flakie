use std::fmt;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{
    retry::RetryConfig, BehaviorVersion, Credentials, Region, StalledStreamProtectionConfig,
};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::config::BackoffConfig;
use crate::source::Source;

/// Configuration for an S3-compatible object store.
#[derive(Clone, PartialEq, Eq)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub stalled_stream_grace_period: Duration,
    pub backoff: BackoffConfig,
    pub force_path_style: bool,
    pub sdk_max_attempts: u32,
    pub session_token: Option<String>,
    /// Preserve `.part` files after errors so a later call can resume.
    pub keep_partial_downloads: bool,
}

impl S3Config {
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            region: region.into(),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(120),
            stalled_stream_grace_period: Duration::from_secs(60),
            backoff: BackoffConfig::default(),
            force_path_style: true,
            sdk_max_attempts: 10,
            session_token: None,
            keep_partial_downloads: true,
        }
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            bucket: String::new(),
            access_key: String::new(),
            secret_key: String::new(),
            region: String::new(),
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(120),
            stalled_stream_grace_period: Duration::from_secs(60),
            backoff: BackoffConfig::default(),
            force_path_style: true,
            sdk_max_attempts: 10,
            session_token: None,
            keep_partial_downloads: true,
        }
    }
}

impl fmt::Debug for S3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("access_key", &self.access_key)
            .field("secret_key", &"<redacted>")
            .field("region", &self.region)
            .field("connect_timeout", &self.connect_timeout)
            .field("read_timeout", &self.read_timeout)
            .field(
                "stalled_stream_grace_period",
                &self.stalled_stream_grace_period,
            )
            .field("backoff", &self.backoff)
            .field("force_path_style", &self.force_path_style)
            .field("sdk_max_attempts", &self.sdk_max_attempts)
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .field("keep_partial_downloads", &self.keep_partial_downloads)
            .finish()
    }
}

/// S3-compatible object storage backed by the AWS SDK.
///
/// The SDK is configured with a custom endpoint and, by default, path-style
/// access, which most S3-compatible providers (Hetzner, MinIO, R2, ...) accept.
#[derive(Clone)]
pub struct S3Source {
    client: aws_sdk_s3::Client,
    bucket: String,
    backoff: BackoffConfig,
    keep_partial_downloads: bool,
}

impl S3Source {
    pub fn new(config: S3Config) -> Self {
        let sdk_config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .retry_config(RetryConfig::standard().with_max_attempts(config.sdk_max_attempts.max(1)))
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(config.connect_timeout)
                    .read_timeout(config.read_timeout)
                    .build(),
            )
            .stalled_stream_protection(
                StalledStreamProtectionConfig::enabled()
                    .grace_period(config.stalled_stream_grace_period)
                    .build(),
            )
            .credentials_provider(Credentials::new(
                config.access_key,
                config.secret_key,
                config.session_token,
                None,
                "flakie",
            ))
            .region(Region::new(config.region))
            .endpoint_url(config.endpoint.trim_end_matches('/'))
            .force_path_style(config.force_path_style)
            .build();

        Self {
            client: aws_sdk_s3::Client::from_conf(sdk_config),
            bucket: config.bucket,
            backoff: config.backoff,
            keep_partial_downloads: config.keep_partial_downloads,
        }
    }

    async fn download_once(&self, key: &str, part_path: &Path) -> anyhow::Result<()> {
        let offset = tokio::fs::metadata(part_path)
            .await
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(part_path)
            .await?;
        file.set_len(offset).await?;
        file.seek(SeekFrom::Start(offset)).await?;

        let mut request = self.client.get_object().bucket(&self.bucket).key(key);
        if offset > 0 {
            request = request.range(format!("bytes={offset}-"));
        }

        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                if offset > 0
                    && error
                        .as_service_error()
                        .map(|service_error| service_error.code() == Some("InvalidRange"))
                        .unwrap_or(false)
                {
                    return Ok(());
                }
                return Err(error.into());
            }
        };

        let mut body = response.body;
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        file.sync_all().await?;
        Ok(())
    }
}

#[async_trait]
impl Source for S3Source {
    async fn download(&self, key: &str, path: &Path) -> anyhow::Result<()> {
        let part_path = partial_download_path(path);
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let mut last_error = None;

        for attempt in 0..self.backoff.max_attempts.max(1) {
            match self.download_once(key, &part_path).await {
                Ok(()) => {
                    tokio::fs::rename(&part_path, path).await.with_context(|| {
                        format!("renaming downloaded object {} to {}", key, path.display())
                    })?;
                    return Ok(());
                }
                Err(error) => {
                    last_error = Some(error);
                    if attempt + 1 < self.backoff.max_attempts {
                        tokio::time::sleep(self.backoff.delay_for_attempt(attempt)).await;
                    }
                }
            }
        }

        if !self.keep_partial_downloads {
            let _ = tokio::fs::remove_file(&part_path).await;
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("download failed")))
    }

    async fn upload(
        &self,
        path: &Path,
        key: &str,
        content_type: Option<&str>,
    ) -> anyhow::Result<()> {
        let body = ByteStream::from_path(path)
            .await
            .with_context(|| format!("reading upload source {}", path.display()))?;
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body);
        if let Some(content_type) = content_type {
            request = request.content_type(content_type);
        }
        request
            .send()
            .await
            .with_context(|| format!("uploading {}", path.display()))?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(error) => {
                let service_error = error.into_service_error();
                if service_error.is_not_found() {
                    Ok(false)
                } else {
                    Err(service_error.into())
                }
            }
        }
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        Ok(())
    }
}

fn partial_download_path(path: &Path) -> PathBuf {
    let mut part_path = path.as_os_str().to_os_string();
    part_path.push(".part");
    PathBuf::from(part_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_download_path_appends_suffix() {
        let path = Path::new("/tmp/object.bin");

        assert_eq!(
            partial_download_path(path),
            PathBuf::from("/tmp/object.bin.part")
        );
    }

    #[test]
    fn s3_debug_redacts_credentials() {
        let mut config = S3Config::new("endpoint", "bucket", "access", "secret-value", "region");
        config.session_token = Some("session-value".into());
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret-value"));
        assert!(!debug.contains("session-value"));
    }
}
