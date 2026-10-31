use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use anyhow::Context;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{retry::RetryConfig, Credentials, Region, StalledStreamProtectionConfig};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::config::{BackoffConfig, S3Config};
use crate::source::Source;

/// S3-compatible object storage backed by the AWS SDK.
///
/// The SDK is configured with a custom endpoint and forced path-style access,
/// which is what Hetzner Object Storage requires.
#[derive(Clone)]
pub struct S3Source {
    client: aws_sdk_s3::Client,
    bucket: String,
    backoff: BackoffConfig,
}

impl S3Source {
    pub fn new(config: S3Config) -> Self {
        let sdk_config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .retry_config(RetryConfig::standard().with_max_attempts(10))
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
                None,
                None,
                "flakie",
            ))
            .region(Region::new(config.region))
            .endpoint_url(config.endpoint.trim_end_matches('/'))
            .force_path_style(true)
            .build();

        Self {
            client: aws_sdk_s3::Client::from_conf(sdk_config),
            bucket: config.bucket,
            backoff: config.backoff,
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
        let mut last_error = None;

        for attempt in 0..self.backoff.max_attempts {
            match self.download_once(key, &part_path).await {
                Ok(()) => {
                    if let Some(parent) = path.parent() {
                        tokio::fs::create_dir_all(parent)
                            .await
                            .with_context(|| format!("creating {}", parent.display()))?;
                    }
                    tokio::fs::rename(&part_path, path).await.with_context(|| {
                        format!(
                            "renaming downloaded object {} to {}",
                            key,
                            path.display()
                        )
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

        let _ = tokio::fs::remove_file(&part_path).await;
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
        let mut request = self.client.put_object().bucket(&self.bucket).key(key).body(body);
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

