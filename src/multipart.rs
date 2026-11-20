use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Context;
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::config::MultipartConfig;
use crate::engine::{run_batch, Keyed};
use crate::source::UploadRequest;

/// Size constraints a multipart backend places on parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartLimits {
    /// Smallest size allowed for every part except the last.
    pub min_part_size: u64,
    /// Largest size allowed for any part.
    pub max_part_size: u64,
    /// Largest number of parts in one upload.
    pub max_parts: u32,
}

impl PartLimits {
    /// Amazon S3's limits, which most S3-compatible stores share: 5 MiB to
    /// 5 GiB per part and at most 10,000 parts.
    pub const S3: Self = Self {
        min_part_size: 5 * 1024 * 1024,
        max_part_size: 5 * 1024 * 1024 * 1024,
        max_parts: 10_000,
    };
}

impl Default for PartLimits {
    fn default() -> Self {
        Self::S3
    }
}

/// A part accepted by the backend, identified by the token it returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadedPart {
    /// One-based part number.
    pub number: u32,
    /// Opaque token the backend needs to assemble the part (an ETag for S3).
    pub token: String,
}

/// A backend that can upload one object as independently retried parts.
///
/// [`multipart_upload`] drives the protocol: it starts an upload, sends every
/// part through the retry queue, then completes the upload, or aborts it if
/// anything fails. Implementations only provide the four calls below.
/// Implementations should be cheap to clone.
#[async_trait]
pub trait MultipartSource: Send + Sync {
    /// Part size limits for this backend. Defaults to [`PartLimits::S3`].
    fn part_limits(&self) -> PartLimits {
        PartLimits::S3
    }

    /// Start an upload of `key` and return its upload id.
    async fn create_multipart_upload(
        &self,
        key: &str,
        content_type: Option<&str>,
    ) -> anyhow::Result<String>;

    /// Upload one part and return the token needed to complete the upload.
    /// The same part may be sent more than once if an attempt fails.
    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: Vec<u8>,
    ) -> anyhow::Result<String>;

    /// Assemble the object from `parts`, which are sorted by part number.
    async fn complete_multipart_upload(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[UploadedPart],
    ) -> anyhow::Result<()>;

    /// Discard an unfinished upload and any parts already stored.
    async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
struct PartSpec {
    label: String,
    number: u32,
    offset: u64,
    len: u64,
    path: Arc<PathBuf>,
    key: Arc<str>,
    upload_id: Arc<str>,
}

impl Keyed for PartSpec {
    fn key(&self) -> &str {
        &self.label
    }
}

/// Upload one file as a multipart upload.
///
/// The file is split into parts of `config.part_size` (raised as needed to fit
/// the source's [`PartLimits`]). Parts are uploaded concurrently and retried
/// individually according to `config.parts`. If any part exhausts its
/// retries, or the file changes while it is being uploaded, the upload is
/// aborted and an error is returned. If the returned future is dropped, the
/// upload is aborted in the background on the current Tokio runtime.
///
/// Each attempt reads its part from disk, so memory use is bounded by
/// `part_size * parts.concurrency`.
pub async fn multipart_upload<S>(
    source: S,
    config: &MultipartConfig,
    request: &UploadRequest,
) -> anyhow::Result<()>
where
    S: MultipartSource + Clone + 'static,
{
    let path = Arc::new(request.path.clone());
    let before = file_fingerprint(&path).await?;
    let part_size = plan_part_size(before.0, config.part_size, source.part_limits())?;

    let upload_id: Arc<str> = source
        .create_multipart_upload(&request.key, request.content_type.as_deref())
        .await
        .with_context(|| format!("starting multipart upload of {}", request.key))?
        .into();
    let mut guard = AbortGuard {
        source: source.clone(),
        key: request.key.clone(),
        upload_id: Some(Arc::clone(&upload_id)),
    };

    let key: Arc<str> = request.key.as_str().into();
    let parts = split_parts(before.0, part_size)
        .map(|(number, offset, len)| PartSpec {
            label: format!("{key} part {number}"),
            number,
            offset,
            len,
            path: Arc::clone(&path),
            key: Arc::clone(&key),
            upload_id: Arc::clone(&upload_id),
        })
        .collect();

    let result = async {
        let part_source = source.clone();
        let uploaded = run_batch(
            &config.parts,
            "upload part",
            parts,
            move |part: PartSpec| {
                let source = part_source.clone();
                async move {
                    let data = read_part(&part.path, part.offset, part.len).await?;
                    source
                        .upload_part(&part.key, &part.upload_id, part.number, data)
                        .await
                }
            },
        )
        .await?
        .into_result()?;

        anyhow::ensure!(
            file_fingerprint(&path).await? == before,
            "{} changed while it was being uploaded",
            path.display()
        );

        let mut parts: Vec<_> = uploaded
            .into_iter()
            .map(|(part, token)| UploadedPart {
                number: part.number,
                token,
            })
            .collect();
        parts.sort_by_key(|part| part.number);
        source
            .complete_multipart_upload(&request.key, &upload_id, &parts)
            .await
            .with_context(|| format!("completing multipart upload of {}", request.key))
    }
    .await;

    let upload_id = guard.upload_id.take().expect("upload id present");
    if result.is_err() {
        if let Err(abort_error) = source
            .abort_multipart_upload(&request.key, &upload_id)
            .await
        {
            tracing::warn!(
                key = %request.key,
                error = format!("{abort_error:#}"),
                "failed to abort multipart upload"
            );
        }
    }
    result
}

/// Aborts the upload in the background if `multipart_upload` is cancelled.
struct AbortGuard<S: MultipartSource + Clone + 'static> {
    source: S,
    key: String,
    upload_id: Option<Arc<str>>,
}

impl<S: MultipartSource + Clone + 'static> Drop for AbortGuard<S> {
    fn drop(&mut self) {
        let Some(upload_id) = self.upload_id.take() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let source = self.source.clone();
        let key = std::mem::take(&mut self.key);
        runtime.spawn(async move {
            if let Err(error) = source.abort_multipart_upload(&key, &upload_id).await {
                tracing::warn!(%key, error = format!("{error:#}"), "failed to abort cancelled multipart upload");
            }
        });
    }
}

async fn file_fingerprint(path: &Path) -> anyhow::Result<(u64, Option<SystemTime>)> {
    let metadata = tokio::fs::metadata(path)
        .await
        .with_context(|| format!("reading upload source {}", path.display()))?;
    Ok((metadata.len(), metadata.modified().ok()))
}

async fn read_part(path: &Path, offset: u64, len: u64) -> anyhow::Result<Vec<u8>> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("opening {}", path.display()))?;
    file.seek(SeekFrom::Start(offset)).await?;
    let mut data = vec![0; usize::try_from(len).context("part does not fit in memory")?];
    file.read_exact(&mut data).await.with_context(|| {
        format!(
            "reading {len} bytes at offset {offset} of {}",
            path.display()
        )
    })?;
    Ok(data)
}

fn plan_part_size(size: u64, preferred: u64, limits: PartLimits) -> anyhow::Result<u64> {
    let max_parts = u64::from(limits.max_parts.max(1));
    let part_size = preferred
        .max(limits.min_part_size)
        .max(size.div_ceil(max_parts))
        .max(1);
    anyhow::ensure!(
        part_size <= limits.max_part_size,
        "{size} bytes needs parts of {part_size} bytes, above the source's maximum of {}",
        limits.max_part_size
    );
    Ok(part_size)
}

/// Yield `(number, offset, len)` for each part; an empty file has one empty part.
fn split_parts(size: u64, part_size: u64) -> impl Iterator<Item = (u32, u64, u64)> {
    let count = size.div_ceil(part_size).max(1);
    (0..count).map(move |index| {
        let offset = index * part_size;
        (
            u32::try_from(index + 1).expect("part count bounded by max_parts"),
            offset,
            part_size.min(size - offset),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::config::TransferConfig;

    #[derive(Clone, Default)]
    struct MemoryStore {
        state: Arc<Mutex<State>>,
    }

    #[derive(Default)]
    struct State {
        uploads: HashMap<String, BTreeMap<u32, Vec<u8>>>,
        objects: HashMap<String, Vec<u8>>,
        part_failures: HashMap<u32, u32>,
        aborted: Vec<String>,
        next_id: u32,
    }

    #[async_trait]
    impl MultipartSource for MemoryStore {
        fn part_limits(&self) -> PartLimits {
            PartLimits {
                min_part_size: 4,
                max_part_size: 1024,
                max_parts: 100,
            }
        }

        async fn create_multipart_upload(
            &self,
            _key: &str,
            _content_type: Option<&str>,
        ) -> anyhow::Result<String> {
            let mut state = self.state.lock().unwrap();
            state.next_id += 1;
            let id = format!("upload-{}", state.next_id);
            state.uploads.insert(id.clone(), BTreeMap::new());
            Ok(id)
        }

        async fn upload_part(
            &self,
            _key: &str,
            upload_id: &str,
            part_number: u32,
            data: Vec<u8>,
        ) -> anyhow::Result<String> {
            let mut state = self.state.lock().unwrap();
            if let Some(remaining) = state.part_failures.get_mut(&part_number) {
                if *remaining > 0 {
                    *remaining -= 1;
                    anyhow::bail!("injected failure for part {part_number}");
                }
            }
            state
                .uploads
                .get_mut(upload_id)
                .context("unknown upload")?
                .insert(part_number, data);
            Ok(format!("etag-{part_number}"))
        }

        async fn complete_multipart_upload(
            &self,
            key: &str,
            upload_id: &str,
            parts: &[UploadedPart],
        ) -> anyhow::Result<()> {
            let mut state = self.state.lock().unwrap();
            let stored = state.uploads.remove(upload_id).context("unknown upload")?;
            let numbers: Vec<_> = parts.iter().map(|part| part.number).collect();
            let expected: Vec<_> = stored.keys().copied().collect();
            anyhow::ensure!(numbers == expected, "parts {numbers:?} != {expected:?}");
            for part in parts {
                anyhow::ensure!(part.token == format!("etag-{}", part.number));
            }
            state
                .objects
                .insert(key.to_string(), stored.into_values().flatten().collect());
            Ok(())
        }

        async fn abort_multipart_upload(&self, _key: &str, upload_id: &str) -> anyhow::Result<()> {
            let mut state = self.state.lock().unwrap();
            state.uploads.remove(upload_id);
            state.aborted.push(upload_id.to_string());
            Ok(())
        }
    }

    fn config(part_size: u64) -> MultipartConfig {
        MultipartConfig {
            part_size,
            parts: TransferConfig {
                concurrency: 3,
                max_attempts: 3,
                retry_delay: Duration::ZERO,
                ..TransferConfig::default()
            },
        }
    }

    fn temp_file(name: &str, contents: &[u8]) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("flakie-multipart-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[tokio::test]
    async fn uploads_parts_concurrently_and_retries_failed_parts() {
        let contents: Vec<u8> = (0..1000u32).map(|i| (i * 7 % 251) as u8).collect();
        let path = temp_file("retry.bin", &contents);
        let store = MemoryStore::default();
        store.state.lock().unwrap().part_failures.insert(3, 2);

        multipart_upload(
            store.clone(),
            &config(64),
            &UploadRequest::new(&path, "object", None::<String>),
        )
        .await
        .unwrap();

        let state = store.state.lock().unwrap();
        assert_eq!(state.objects["object"], contents);
        assert!(state.aborted.is_empty());
    }

    #[tokio::test]
    async fn exhausted_part_aborts_the_upload() {
        let path = temp_file("abort.bin", &[1; 100]);
        let store = MemoryStore::default();
        store.state.lock().unwrap().part_failures.insert(2, 10);

        let error = multipart_upload(
            store.clone(),
            &config(10),
            &UploadRequest::new(&path, "object", None::<String>),
        )
        .await
        .unwrap_err();

        assert!(format!("{error:#}").contains("object part 2"), "{error:#}");
        let state = store.state.lock().unwrap();
        assert!(state.objects.is_empty());
        assert_eq!(state.aborted, ["upload-1"]);
        assert!(state.uploads.is_empty());
    }

    #[tokio::test]
    async fn empty_and_small_files_use_a_single_part() {
        for (name, contents) in [("empty.bin", &b""[..]), ("small.bin", &b"abc"[..])] {
            let path = temp_file(name, contents);
            let store = MemoryStore::default();
            multipart_upload(
                store.clone(),
                &config(64),
                &UploadRequest::new(&path, "object", None::<String>),
            )
            .await
            .unwrap();
            assert_eq!(store.state.lock().unwrap().objects["object"], contents);
        }
    }

    #[test]
    fn part_size_respects_limits() {
        let limits = PartLimits {
            min_part_size: 5,
            max_part_size: 100,
            max_parts: 10,
        };
        assert_eq!(plan_part_size(50, 1, limits).unwrap(), 5);
        assert_eq!(plan_part_size(50, 20, limits).unwrap(), 20);
        assert_eq!(plan_part_size(500, 20, limits).unwrap(), 50);
        assert!(plan_part_size(1001, 20, limits).is_err());
    }

    #[test]
    fn parts_cover_the_file_exactly() {
        let parts: Vec<_> = split_parts(25, 10).collect();
        assert_eq!(parts, [(1, 0, 10), (2, 10, 10), (3, 20, 5)]);
        assert_eq!(split_parts(20, 10).count(), 2);
        assert_eq!(split_parts(0, 10).collect::<Vec<_>>(), [(1, 0, 0)]);
    }
}
