# flakie

Configurable, resumable downloads and uploads from flaky sources.

`flakie` separates the data source from the retry policy used to move data.
Sources are cheap-to-clone async traits, while a small queue engine applies
bounded concurrency, per-item retries, and deterministic ordering. The first
built-in source is S3-compatible object storage, configured the same way
Hetzner Object Storage is configured.

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
            "/tmp/a.mbtiles",
            "runs/42/a.mbtiles",
            Some("application/vnd.sqlite3"),
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

## Retry behavior

The queue engine follows the rules used by the MBTiles downloader:

- Items are pulled from a shared queue by a bounded number of workers.
- A failed item waits for `TransferConfig::retry_delay`, or its configured
  backoff/script delay, then moves to the back of the queue.
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
  object store. Endpoint and path-style access match Hetzner Object Storage.
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
same key and destination path while its `.part` file is still present.

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
available constants are `key`, `operation` (`"download"` or `"upload"`),
`attempt` (number of failures, starting at `1`), `max_attempts`, `error` (the
formatted error chain), and `default_delay_ms` (the fixed or backoff delay).
Use error text matching only when your source provides stable messages.

Policies are compiled once per nonempty batch and evaluated with a fresh scope
after failures while retries remain. They cannot exceed the queue attempt
limit. Syntax errors reject the batch before transfers start; runtime errors,
invalid return values, and resource-limit violations stop retries for the item
and appear alongside the original error in the batch failure. Other queued
items continue, and successful results remain sorted by key.

The engine limits each evaluation to 10,000 operations, with additional bounds
on recursion, expression depth, strings, arrays, and maps. Module imports are
disabled, and no filesystem or network functions are registered. See the
[Rhai resource-limit documentation](https://rhai.rs/book/engine/options.html).

## Custom sources

Implement `Source` to talk to another protocol. This working local-directory
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
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings
```
