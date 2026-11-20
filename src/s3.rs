use std::fmt;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::config::timeout::TimeoutConfig;
use aws_sdk_s3::config::{
    retry::RetryConfig, BehaviorVersion, Credentials, Region, StalledStreamProtectionConfig,
};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::config::{BackoffConfig, MultipartConfig};
use crate::multipart::{multipart_upload, MultipartSource, UploadedPart};
use crate::source::{Source, UploadRequest};

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
    /// Files at least this large are uploaded with a multipart upload.
    /// `None` always uses a single `PutObject` (limited to 5 GiB).
    pub multipart_threshold: Option<u64>,
    /// Part size, concurrency, and per-part retries for multipart uploads.
    pub multipart: MultipartConfig,
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
            ..Self::default()
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
            multipart_threshold: Some(64 * 1024 * 1024),
            multipart: MultipartConfig::default(),
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
            .field("multipart_threshold", &self.multipart_threshold)
            .field("multipart", &self.multipart)
            .finish()
    }
}

/// S3-compatible object storage backed by the AWS SDK.
///
/// The SDK is configured with a custom endpoint and, by default, path-style
/// access, which most S3-compatible providers (Hetzner, MinIO, R2, ...) accept.
///
/// Downloads resume through a `.part` file. The object's ETag is recorded
/// next to it (`.part.etag`), and a resumed request is only accepted if the
/// object still has that ETag; otherwise the download starts over. Large
/// uploads use [`multipart_upload`].
#[derive(Clone)]
pub struct S3Source {
    client: aws_sdk_s3::Client,
    bucket: String,
    backoff: BackoffConfig,
    keep_partial_downloads: bool,
    multipart_threshold: Option<u64>,
    multipart: MultipartConfig,
}

/// Result of one attempt to fetch the rest of an object into a `.part` file.
enum Fetch {
    Done,
    /// The partial file cannot be safely resumed and must be discarded.
    Restart(&'static str),
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
            multipart_threshold: config.multipart_threshold,
            multipart: config.multipart,
        }
    }

    async fn download_once(
        &self,
        key: &str,
        part_path: &Path,
        etag_path: &Path,
    ) -> anyhow::Result<()> {
        if let Fetch::Restart(reason) = self.fetch(key, part_path, etag_path).await? {
            tracing::warn!(key, reason, "discarding partial download");
            remove_if_present(part_path).await?;
            remove_if_present(etag_path).await?;
            if let Fetch::Restart(reason) = self.fetch(key, part_path, etag_path).await? {
                anyhow::bail!("download of {key} could not start: {reason}");
            }
        }
        Ok(())
    }

    /// Fetch the remainder of `key` into `part_path`, resuming when possible.
    async fn fetch(&self, key: &str, part_path: &Path, etag_path: &Path) -> anyhow::Result<Fetch> {
        let offset = tokio::fs::metadata(part_path)
            .await
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        let etag = if offset > 0 {
            match tokio::fs::read_to_string(etag_path).await {
                Ok(etag) if !etag.trim().is_empty() => Some(etag.trim().to_string()),
                _ => return Ok(Fetch::Restart("no recorded ETag for the partial file")),
            }
        } else {
            None
        };

        let mut request = self.client.get_object().bucket(&self.bucket).key(key);
        if let Some(etag) = &etag {
            request = request.range(format!("bytes={offset}-")).if_match(etag);
        }
        let response = match request.send().await {
            Ok(response) => response,
            Err(error) => {
                if let Some(etag) = &etag {
                    match error_status(&error) {
                        Some(412) => return Ok(Fetch::Restart("the object changed")),
                        Some(416) => return self.check_complete(key, etag, offset).await,
                        _ => {}
                    }
                }
                return Err(error).with_context(|| format!("requesting {key}"));
            }
        };

        // `If-Match` already guards this; the comparison also covers servers
        // that ignore conditional headers.
        if etag.is_some() && response.e_tag() != etag.as_deref() {
            return Ok(Fetch::Restart("the object changed"));
        }
        let total = if offset > 0 {
            match response.content_range().and_then(parse_content_range) {
                Some((start, total)) if start == offset => total,
                _ => return Ok(Fetch::Restart("the server ignored the requested range")),
            }
        } else {
            response
                .content_length()
                .and_then(|len| u64::try_from(len).ok())
        };
        if offset == 0 {
            match response.e_tag() {
                Some(etag) => tokio::fs::write(etag_path, etag)
                    .await
                    .with_context(|| format!("writing {}", etag_path.display()))?,
                None => remove_if_present(etag_path).await?,
            }
        }

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(part_path)
            .await
            .with_context(|| format!("opening {}", part_path.display()))?;
        file.set_len(offset).await?;
        file.seek(SeekFrom::Start(offset)).await?;

        let mut written = offset;
        let mut body = response.body;
        while let Some(chunk) = body.next().await {
            let chunk = chunk.with_context(|| format!("reading {key}"))?;
            file.write_all(&chunk).await?;
            written += chunk.len() as u64;
        }
        file.flush().await?;
        file.sync_all().await?;

        if let Some(total) = total {
            anyhow::ensure!(
                written == total,
                "download of {key} ended early: received {written} of {total} bytes"
            );
        }
        Ok(Fetch::Done)
    }

    /// Handle a 416 on resume: the partial file is complete only if it is
    /// exactly as long as the object it was started from.
    async fn check_complete(&self, key: &str, etag: &str, offset: u64) -> anyhow::Result<Fetch> {
        let head = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .if_match(etag)
            .send()
            .await;
        match head {
            Ok(head) if head.content_length() == i64::try_from(offset).ok() => Ok(Fetch::Done),
            Ok(_) => Ok(Fetch::Restart("the partial file is longer than the object")),
            Err(error) if error_status(&error) == Some(412) => {
                Ok(Fetch::Restart("the object changed"))
            }
            Err(error) => Err(error).with_context(|| format!("checking {key}")),
        }
    }

    async fn put_object(
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
}

#[async_trait]
impl Source for S3Source {
    async fn download(&self, key: &str, path: &Path) -> anyhow::Result<()> {
        let part_path = partial_download_path(path);
        let etag_path = partial_etag_path(path);
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let mut last_error = None;

        let max_attempts = self.backoff.max_attempts.max(1);
        for attempt in 0..max_attempts {
            match self.download_once(key, &part_path, &etag_path).await {
                Ok(()) => {
                    tokio::fs::rename(&part_path, path).await.with_context(|| {
                        format!("renaming downloaded object {} to {}", key, path.display())
                    })?;
                    remove_if_present(&etag_path).await?;
                    return Ok(());
                }
                Err(error) => {
                    last_error = Some(error);
                    if attempt + 1 < max_attempts {
                        tokio::time::sleep(self.backoff.delay_for_attempt(attempt)).await;
                    }
                }
            }
        }

        if !self.keep_partial_downloads {
            let _ = tokio::fs::remove_file(&part_path).await;
            let _ = tokio::fs::remove_file(&etag_path).await;
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("download failed")))
    }

    async fn upload(
        &self,
        path: &Path,
        key: &str,
        content_type: Option<&str>,
    ) -> anyhow::Result<()> {
        let size = tokio::fs::metadata(path)
            .await
            .with_context(|| format!("reading upload source {}", path.display()))?
            .len();
        if self
            .multipart_threshold
            .is_some_and(|threshold| size >= threshold)
        {
            let request = UploadRequest::new(path, key, content_type);
            return multipart_upload(self.clone(), &self.multipart, &request).await;
        }
        self.put_object(path, key, content_type).await
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

#[async_trait]
impl MultipartSource for S3Source {
    async fn create_multipart_upload(
        &self,
        key: &str,
        content_type: Option<&str>,
    ) -> anyhow::Result<String> {
        let output = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .set_content_type(content_type.map(str::to_string))
            .send()
            .await?;
        output
            .upload_id()
            .map(str::to_string)
            .context("server returned no upload id")
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
    ) -> anyhow::Result<String> {
        let output = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(i32::try_from(part_number).context("part number out of range")?)
            .body(ByteStream::from(data))
            .send()
            .await?;
        output
            .e_tag()
            .map(str::to_string)
            .context("server returned no ETag for part")
    }

    async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[UploadedPart],
    ) -> anyhow::Result<()> {
        let parts = parts
            .iter()
            .map(|part| {
                Ok(CompletedPart::builder()
                    .part_number(i32::try_from(part.number).context("part number out of range")?)
                    .e_tag(&part.token)
                    .build())
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await?;
        Ok(())
    }

    async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> anyhow::Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await?;
        Ok(())
    }
}

/// HTTP status of a failed request, if the server responded.
fn error_status<E>(error: &SdkError<E, HttpResponse>) -> Option<u16> {
    error
        .raw_response()
        .map(|response| response.status().as_u16())
}

/// Parse `bytes <start>-<end>/<total>` into the start offset and total size.
fn parse_content_range(value: &str) -> Option<(u64, Option<u64>)> {
    let (range, total) = value.strip_prefix("bytes ")?.split_once('/')?;
    let start = range.split_once('-')?.0.trim().parse().ok()?;
    let total = match total.trim() {
        "*" => None,
        total => Some(total.parse().ok()?),
    };
    Some((start, total))
}

async fn remove_if_present(path: &Path) -> anyhow::Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("removing {}", path.display())),
    }
}

fn partial_download_path(path: &Path) -> PathBuf {
    let mut part_path = path.as_os_str().to_os_string();
    part_path.push(".part");
    PathBuf::from(part_path)
}

fn partial_etag_path(path: &Path) -> PathBuf {
    let mut etag_path = path.as_os_str().to_os_string();
    etag_path.push(".part.etag");
    PathBuf::from(etag_path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_paths_append_suffixes() {
        let path = Path::new("/tmp/object.bin");

        assert_eq!(
            partial_download_path(path),
            PathBuf::from("/tmp/object.bin.part")
        );
        assert_eq!(
            partial_etag_path(path),
            PathBuf::from("/tmp/object.bin.part.etag")
        );
    }

    #[test]
    fn content_range_is_parsed() {
        assert_eq!(
            parse_content_range("bytes 100-199/200"),
            Some((100, Some(200)))
        );
        assert_eq!(parse_content_range("bytes 5-9/*"), Some((5, None)));
        assert_eq!(parse_content_range("items 0-1/2"), None);
        assert_eq!(parse_content_range("bytes x-1/2"), None);
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
