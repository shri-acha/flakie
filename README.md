# flakie

Configurable, resumable downloads and uploads from flaky sources.

`flakie` separates the data source from the retry policy used to move data.
Sources are cheap-to-clone async traits, while a small queue engine applies
bounded concurrency, per-item retries, and deterministic ordering. The first
built-in source is S3-compatible object storage, configured the same way
Hetzner Object Storage is configured.

## Usage

Downloading from Hetzner Object Storage:

```rust
use std::time::Duration;

use flakie::{download_all, DownloadRequest, S3Config, S3Source, TransferConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let source = S3Source::new(S3Config::new(
        "https://fsn1.your-objectstorage.com",
        "shards",
        std::env::var("S3_ACCESS_KEY")?,
        std::env::var("S3_SECRET_KEY")?,
        "fsn1",
    ));

    let requests = vec![
        DownloadRequest::new("runs/42/a.mbtiles", "/tmp/a.mbtiles"),
        DownloadRequest::new("runs/42/b.mbtiles", "/tmp/b.mbtiles"),
    ];

    let completed = download_all(
        source,
        &TransferConfig {
            concurrency: 4,
            max_attempts: 3,
            retry_delay: Duration::from_secs(1),
        },
        requests,
    )
    .await?;

    for item in completed {
        println!("downloaded {} to {}", item.key, item.path.display());
    }

    Ok(())
}
```

Uploading works the same way with `upload_all` and `UploadRequest`:

```rust
use flakie::{upload_all, UploadRequest};

let requests = vec![UploadRequest::new(
    "/tmp/a.mbtiles",
    "runs/42/a.mbtiles",
    Some("application/vnd.sqlite3"),
)];

let completed = upload_all(source, &config, requests).await?;
```

## Retry behavior

The queue engine follows the rules used by the MBTiles downloader:

- Items are pulled from a shared queue by a bounded number of workers.
- A failed item is moved to the back of the queue and retried after
  `TransferConfig::retry_delay`.
- After `TransferConfig::max_attempts` failures, the item is reported as
  failed. If any item fails, the whole call errors and lists every failed key.
- Successful results are returned ordered by object key.

On top of that, `S3Source` makes a single large object resilient:

- Downloads resume from the last successfully written byte through a `.part`
  file and S3 range requests.
- Each resumed object is retried with exponential backoff and jitter as
  configured by `BackoffConfig`.
- Uploads stream from disk with `ByteStream::from_path`, which restarts the
  file read when the SDK retries a request.

## Configuration

`TransferConfig` controls whole-item queue behavior:

- `concurrency`: number of items moved at once.
- `max_attempts`: attempts before an item is declared failed.
- `retry_delay`: fixed delay before a failed item goes back on the queue.

`S3Config` controls the S3 source:

- `endpoint`, `bucket`, `access_key`, `secret_key`, and `region` describe the
  object store. Endpoint and path-style access match Hetzner Object Storage.
- `connect_timeout`, `read_timeout`, and `stalled_stream_grace_period` tune the
  SDK HTTP client.
- `backoff` configures resumed-download retries.

`BackoffConfig` controls source-level backoff:

- `max_attempts`, `base_delay`, `max_delay`, and `jitter`. Delays double each
  attempt, cap at `max_delay`, and add up to `jitter` of random extra time.

## Custom sources

Implement `Source` to talk to another protocol:

```rust
use std::path::Path;

use async_trait::async_trait;
use flakie::Source;

struct MySource;

#[async_trait]
impl Source for MySource {
    async fn download(&self, key: &str, path: &Path) -> anyhow::Result<()> {
        // ...
    }

    async fn upload(
        &self,
        path: &Path,
        key: &str,
        content_type: Option<&str>,
    ) -> anyhow::Result<()> {
        // ...
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        // ...
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        // ...
    }
}
```

## Development

```sh
cargo test
cargo clippy --all-targets -- -D warnings
```
