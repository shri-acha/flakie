# flakie

Configurable, resumable retries for work against flaky sources.

`flakie` separates *what* is done from the retry policy used to do it. The
queue engine (`run_batch`, `run_all`) runs any keyed async job with bounded
concurrency, per-item retries, backoff, attempt timeouts, and optional Rhai
retry policies. `download_batch`/`upload_batch` (and their all-or-nothing
`*_all` forms) build on it to move files through any `Source`, and
`multipart_upload` sends one file as independently retried parts through any
`MultipartSource`. The built-in `S3Source` implements both, with resumable,
ETag-checked downloads for any S3-compatible object store (AWS, Hetzner,
MinIO, Cloudflare R2, ...).

Requires Rust 1.94.1 or newer with the default `s3` feature (an
`aws-sdk-s3` requirement).

## Features

- `s3` (default): `S3Source` and `S3Config`, backed by `aws-sdk-s3`. Disable
  default features to use the queue engine and custom sources without the AWS
  SDK.
- `scripting`: Rhai retry policies (see below).

## Usage

For a client project next to this checkout, add these dependencies to its
`Cargo.toml` (adjust the path to your local `flakie` directory):

```toml
[dependencies]
flakie = { path = "../flakie" }
anyhow = "1"
async-trait = "0.1"
tokio = { version = "1", features = ["full"] }
```

Downloading from an S3-compatible store (Hetzner Object Storage here):

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
        DownloadRequest::new("runs/42/a.bin", "/tmp/a.bin"),
        DownloadRequest::new("runs/42/b.bin", "/tmp/b.bin"),
    ];

    let completed = download_all(
        source,
        &TransferConfig {
            concurrency: 4,
            max_attempts: 3,
            retry_delay: Duration::from_secs(1),
            ..TransferConfig::default()
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

Uploading works the same way with `upload_all` and `UploadRequest`. This
function accepts an already configured source:

```rust
use flakie::{upload_all, S3Source, TransferConfig, UploadRequest};

async fn upload_files(source: S3Source) -> anyhow::Result<()> {
    let config = TransferConfig {
        concurrency: 2,
        ..TransferConfig::default()
    };
    let requests = vec![
        UploadRequest::new(
            "/tmp/a.bin",
            "runs/42/a.bin",
            Some("application/octet-stream"),
        ),
        UploadRequest::new("/tmp/notes.txt", "runs/42/notes.txt", None::<String>),
    ];

    for item in upload_all(source, &config, requests).await? {
        println!("uploaded {} as {}", item.path.display(), item.key);
    }
    Ok(())
}
```

Use the `Source` trait directly to check for or delete an object. These calls
use source and SDK behavior; they do not run through the queue retry policy:

```rust
use flakie::{S3Source, Source};

async fn delete_if_present(source: &S3Source, key: &str) -> anyhow::Result<()> {
    if source.exists(key).await? {
        source.delete(key).await?;
    }
    Ok(())
}
```

## Partial failures

The `*_all` functions are all-or-nothing: if any item fails, they return an
error, even though the other items finished. When you want to keep what
succeeded, use the `*_batch` form, which returns a `BatchReport`:

```rust
use flakie::{download_batch, DownloadRequest, S3Source, TransferConfig};

async fn download_what_we_can(
    source: S3Source,
    requests: Vec<DownloadRequest>,
) -> anyhow::Result<()> {
    let report = download_batch(source, &TransferConfig::default(), requests).await?;

    for (request, ()) in &report.completed {
        println!("downloaded {} to {}", request.key, request.path.display());
    }
    for failed in &report.failed {
        eprintln!(
            "gave up on {} after {} attempt(s): {:#}",
            failed.item.key, failed.attempts, failed.error
        );
    }
    Ok(())
}
```

Both lists are in submission order, and each failure carries the original
request back so it can be resubmitted. The outer `Result` only fails for
problems with the batch as a whole: a retry script that does not compile, or
two requests sharing a download destination (or upload key).

`report.into_result()` converts a report into the all-or-nothing form. The
error it returns, which is also what `*_all` return, is a `BatchError`. You can
recover it with `error.downcast_ref::<flakie::BatchError>()` to inspect each
failed key, its attempt count, and its error.

## Arbitrary jobs

`run_all` applies the same queue to any work. Items implement `Keyed` (already
implemented for `String`, `DownloadRequest`, and `UploadRequest`), which names
them in logs, errors, and retry scripts. Each attempt receives a clone of the
item, and results come back in input order alongside each job's output:

```rust
use std::time::Duration;

use flakie::{run_all, TransferConfig};

async fn fetch_lengths(urls: Vec<String>) -> anyhow::Result<Vec<(String, usize)>> {
    let config = TransferConfig {
        concurrency: 8,
        max_attempts: 4,
        attempt_timeout: Some(Duration::from_secs(30)),
        ..TransferConfig::default()
    };
    run_all(&config, "fetch", urls, |url: String| async move {
        let body = http_get(&url).await?; // your HTTP client call
        Ok(body.len())
    })
    .await
}
```

`run_batch` is the same with a `BatchReport` instead of all-or-nothing. The
`operation` argument (`"fetch"` here) appears in error messages and is the
`operation` constant seen by retry scripts. The job closure must be
`Send + Sync + 'static`, and its future `Send + 'static`; clone shared clients
into the future as `download_all` does with its source.

## Multipart uploads

`multipart_upload` splits one file into parts, uploads them concurrently with
per-part retries, and completes the upload. If a part exhausts its retries, or
the file changes while it is being read, the upload is aborted so no orphaned
parts are left behind. Dropping the future (for example, through an
`attempt_timeout`) also aborts the upload in the background.

`S3Source::upload` does this automatically for files of at least
`S3Config::multipart_threshold` (64 MiB by default). Smaller files use a single
`PutObject`. To use it with another backend, implement `MultipartSource`:

```rust
use async_trait::async_trait;
use flakie::{MultipartSource, PartLimits, UploadedPart};

#[derive(Clone)]
struct MyStore;

#[async_trait]
impl MultipartSource for MyStore {
    fn part_limits(&self) -> PartLimits {
        PartLimits { min_part_size: 1024 * 1024, max_part_size: 1 << 30, max_parts: 1000 }
    }

    async fn create_multipart_upload(&self, key: &str, content_type: Option<&str>)
        -> anyhow::Result<String> { todo!("start an upload and return its id") }

    async fn upload_part(&self, key: &str, upload_id: &str, part_number: u32, data: Vec<u8>)
        -> anyhow::Result<String> { todo!("store the part and return its token") }

    async fn complete_multipart_upload(&self, key: &str, upload_id: &str, parts: &[UploadedPart])
        -> anyhow::Result<()> { todo!("assemble parts, sorted by number") }

    async fn abort_multipart_upload(&self, key: &str, upload_id: &str)
        -> anyhow::Result<()> { todo!("discard the upload") }
}
```

Then call `multipart_upload(MyStore, &MultipartConfig::default(), &request)`.
`MultipartConfig` has two fields:

- `part_size`: preferred part size (default 16 MiB). It is raised as needed to
  meet the source's minimum part size and maximum part count.
- `parts`: a `TransferConfig` for parts: concurrency (default 4), attempts
  (default 5), backoff, and per-part timeouts.

Each part attempt reads its bytes from disk, so memory use is about
`part_size * concurrency`. An upload that fails as a whole is not resumed: if
the queue retries the item, all parts are sent again.

## Retry behavior

The queue engine:

- Items are pulled from a shared queue by a bounded number of workers.
- A failed item waits for `TransferConfig::retry_delay`, or its configured
  backoff/script delay, then moves to the back of the queue.
- After `TransferConfig::max_attempts` failures, the item is reported as
  failed. Other items are unaffected. `*_batch` functions report it, and
  `*_all` functions return a `BatchError` listing every failed key.
- Each attempt runs as its own Tokio task. A panic counts as a failed attempt,
  and dropping the batch future cancels every in-flight attempt.
- While a failed item waits out its retry delay, it holds its worker, so with
  `concurrency: 1` the queue pauses for that delay.
- `download_all` and `upload_all` return successful results ordered by object
  key; `run_all` returns them in input order.

On top of that, `S3Source` makes a single large object resilient:

- Downloads resume from the last successfully written byte through a `.part`
  file and S3 range requests. The object's ETag is stored next to it in
  `.part.etag`, and a resume uses `If-Match` so a replaced object is never
  spliced onto an old prefix. If the ETag no longer matches, is missing, or
  the partial file is longer than the object, the download restarts from
  zero. A partial file that already holds the whole object is accepted
  without downloading again.
- A finished download must have exactly the length the server reported, or
  it is retried. Content checksums are not verified.
- Each resumed object is retried with exponential backoff and jitter as
  configured by `BackoffConfig`.
- Uploads below `multipart_threshold` stream from disk with
  `ByteStream::from_path`, which restarts the file read when the SDK retries a
  request. Larger uploads use `multipart_upload`.

## Configuration

`TransferConfig` controls whole-item queue behavior for `run_batch`, `run_all`,
and the download and upload helpers:

- `concurrency`: number of items moved at once.
- `max_attempts`: attempts before an item is declared failed.
- `retry_delay`: fixed delay before a failed item goes back on the queue.
- `retry_backoff`: optional `BackoffConfig` replacing the fixed delay. The
  queue still uses `TransferConfig::max_attempts`; the backoff's attempt limit
  is ignored here.
- `attempt_timeout`: optional deadline for each attempt, including retries
  performed inside the source. A timeout cancels that attempt and counts as a
  failure. Custom sources must tolerate their transfer future being dropped.
- `retry_script`: optional retry policy, available with the `scripting` feature.

For example, use exponential queue retries with a two-minute attempt deadline:

```rust
use std::time::Duration;
use flakie::{BackoffConfig, TransferConfig};

let config = TransferConfig {
    concurrency: 4,
    max_attempts: 5,
    attempt_timeout: Some(Duration::from_secs(120)),
    retry_backoff: Some(BackoffConfig {
        base_delay: Duration::from_millis(250),
        max_delay: Some(Duration::from_secs(10)),
        jitter: Duration::from_millis(100),
        ..BackoffConfig::default()
    }),
    ..TransferConfig::default()
};
```

Zero concurrency or attempt limits are treated as one. Defaults preserve the
fixed queue retry delay and impose no attempt timeout. When using struct
literals, include `..TransferConfig::default()` to pick up new options.

`S3Config` controls the S3 source:

- `endpoint`, `bucket`, `access_key`, `secret_key`, and `region` describe the
  object store. Path-style access is the default, which most S3-compatible
  providers accept.
- `connect_timeout`, `read_timeout`, and `stalled_stream_grace_period` tune the
  SDK HTTP client.
- `backoff` configures resumed-download retries.
- `force_path_style` selects path-style addressing (default `true`); set it to
  `false` for virtual-hosted addressing.
- `sdk_max_attempts` controls SDK request retries (default `10`, minimum `1`).
- `session_token` supports temporary credentials and is redacted in debug output.
- `keep_partial_downloads` preserves `.part` files after errors (default `true`)
  so later calls can resume. Set it to `false` to remove them after exhausted
  source retries. Cancellation, including an engine timeout, leaves partial
  files in place. Download parent directories are created automatically.
- `multipart_threshold`: files at least this large use a multipart upload
  (default `Some(64 MiB)`). `None` always uses one `PutObject`, which S3
  limits to 5 GiB.
- `multipart`: the `MultipartConfig` for those uploads.

`BackoffConfig` controls source-level backoff:

- `max_attempts`, `base_delay`, `max_delay`, and `jitter`. Delays double each
  attempt, cap at `max_delay`, and add up to `jitter` of random extra time.

For example, configure a source using environment variables, with temporary
credentials when `S3_SESSION_TOKEN` is set:

```rust
use std::time::Duration;
use flakie::{BackoffConfig, S3Config, S3Source};

fn configured_source() -> anyhow::Result<S3Source> {
    let mut config = S3Config::new(
        std::env::var("S3_ENDPOINT")?,
        std::env::var("S3_BUCKET")?,
        std::env::var("S3_ACCESS_KEY")?,
        std::env::var("S3_SECRET_KEY")?,
        std::env::var("S3_REGION")?,
    );
    config.session_token = std::env::var("S3_SESSION_TOKEN").ok();
    config.force_path_style = true;
    config.sdk_max_attempts = 3;
    config.connect_timeout = Duration::from_secs(5);
    config.read_timeout = Duration::from_secs(60);
    config.stalled_stream_grace_period = Duration::from_secs(30);
    config.keep_partial_downloads = true;
    config.backoff = BackoffConfig {
        max_attempts: 4,
        base_delay: Duration::from_millis(500),
        max_delay: Some(Duration::from_secs(8)),
        jitter: Duration::from_millis(250),
    };
    Ok(S3Source::new(config))
}
```

The SDK, resumed-download backoff, and queue each have their own attempt limit.
Set `attempt_timeout` on the queue to bound an entire attempt across those
source-level retries. To resume a failed download in a later call, submit the
same key and destination path while its `.part` and `.part.etag` files are
still present.

## Rhai retry policies

Enable scripting explicitly:

```toml
[dependencies]
flakie = { path = "../flakie", features = ["scripting"] }
```

Set `retry_script` to inline Rhai. This policy uses the configured backoff,
stops on an access-denied error, and limits upload retries:

```rust
use std::time::Duration;
use flakie::{BackoffConfig, TransferConfig};

fn scripted_config() -> TransferConfig {
    TransferConfig {
        max_attempts: 5,
        retry_backoff: Some(BackoffConfig {
            base_delay: Duration::from_millis(500),
            max_delay: Some(Duration::from_secs(10)),
            jitter: Duration::ZERO,
            ..BackoffConfig::default()
        }),
        retry_script: Some(r#"
            if error.contains("AccessDenied") {
                -1
            } else if operation == "upload" && attempt >= 2 {
                -1
            } else {
                default_delay_ms
            }
        "#.into()),
        ..TransferConfig::default()
    }
}
```

To keep the policy in a separate file, save this as `retry.rhai` in the client
project's working directory:

```rhai
// Retry optional objects only once; use backoff for all other keys.
if key.starts_with("optional/") && attempt >= 2 {
    -1
} else {
    default_delay_ms
}
```

Load the file before starting a transfer batch:

```rust
use flakie::TransferConfig;

fn config_from_script_file() -> anyhow::Result<TransferConfig> {
    Ok(TransferConfig {
        max_attempts: 5,
        retry_script: Some(std::fs::read_to_string("retry.rhai")?),
        ..TransferConfig::default()
    })
}
```

Scripts return an integer: `-1` stops retries for this item, and a nonnegative
value gives the next delay in milliseconds (`0` retries immediately). The
available constants are `key`, `operation` (`"download"`, `"upload"`, or the
name passed to `run_batch`/`run_all`; `"upload part"` for multipart parts),
`attempt` (number of failures, starting at `1`), `max_attempts`, `error` (the
formatted error chain), and `default_delay_ms` (the fixed or backoff delay).
Use error text matching only when your source provides stable messages.

Policies are compiled once per nonempty batch and evaluated with a fresh scope
after failures while retries remain. They cannot exceed the queue attempt
limit. Syntax errors reject the batch before transfers start; runtime errors,
invalid return values, and resource-limit violations stop retries for the item
and appear alongside the original error in the batch failure. Other queued
items continue, and successful results keep their usual ordering.

The engine limits each evaluation to 10,000 operations, with additional bounds
on recursion, expression depth, strings, arrays, and maps. Module imports are
disabled, and no filesystem or network functions are registered. See the
[Rhai resource-limit documentation](https://rhai.rs/book/engine/options.html).

## Custom sources

Implement `Source` to talk to another storage protocol; for work that is not a
file transfer, use `run_all` directly. This working local-directory
source uses file copies and can be passed to the same queue engine. Keys in
this example are trusted paths relative to `root`:

```rust
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use flakie::{download_all, DownloadRequest, Source, TransferConfig};

#[derive(Clone)]
struct DirectorySource {
    root: PathBuf,
}

async fn copy_file(from: &Path, to: &Path) -> anyhow::Result<()> {
    if let Some(parent) = to.parent().filter(|path| !path.as_os_str().is_empty()) {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::copy(from, to).await?;
    Ok(())
}

#[async_trait]
impl Source for DirectorySource {
    async fn download(&self, key: &str, path: &Path) -> anyhow::Result<()> {
        copy_file(&self.root.join(key), path).await
    }

    async fn upload(
        &self,
        path: &Path,
        key: &str,
        _content_type: Option<&str>,
    ) -> anyhow::Result<()> {
        copy_file(path, &self.root.join(key)).await
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        match tokio::fs::metadata(self.root.join(key)).await {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        tokio::fs::remove_file(self.root.join(key)).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let source = DirectorySource { root: "./objects".into() };
    tokio::fs::create_dir_all(&source.root).await?;
    tokio::fs::write(source.root.join("hello.txt"), "hello from flakie\n").await?;

    let completed = download_all(
        source,
        &TransferConfig::default(),
        vec![DownloadRequest::new("hello.txt", "./downloads/hello.txt")],
    ).await?;
    println!("downloaded {}", completed[0].path.display());
    Ok(())
}
```

## Development

```sh
cargo test
cargo test --all-features
cargo test --no-default-features
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings
```

`tests/s3.rs` runs against a real S3-compatible server and is skipped unless
`FLAKIE_TEST_S3_ENDPOINT` is set. With a local MinIO:

```sh
docker run -d --rm --name flakie-minio -p 127.0.0.1:19000:9000 \
  -e MINIO_ROOT_USER=flakie -e MINIO_ROOT_PASSWORD=flakie-secret \
  minio/minio server /data
FLAKIE_TEST_S3_ENDPOINT=http://127.0.0.1:19000 \
FLAKIE_TEST_S3_ACCESS_KEY=flakie FLAKIE_TEST_S3_SECRET_KEY=flakie-secret \
  cargo test --test s3
```
