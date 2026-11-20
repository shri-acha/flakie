//! Integration tests against a real S3-compatible server.
//!
//! Skipped unless `FLAKIE_TEST_S3_ENDPOINT` is set. For a local MinIO:
//!
//! ```sh
//! docker run -d --rm --name flakie-minio -p 127.0.0.1:19000:9000 \
//!   -e MINIO_ROOT_USER=flakie -e MINIO_ROOT_PASSWORD=flakie-secret \
//!   minio/minio server /data
//! FLAKIE_TEST_S3_ENDPOINT=http://127.0.0.1:19000 \
//! FLAKIE_TEST_S3_ACCESS_KEY=flakie FLAKIE_TEST_S3_SECRET_KEY=flakie-secret \
//!   cargo test --test s3 -- --test-threads=1
//! ```
//!
//! `FLAKIE_TEST_S3_BUCKET` (default `flakie-test`) is created if missing.
//! `FLAKIE_TEST_S3_REGION` defaults to `us-east-1`.
#![cfg(feature = "s3")]

use std::path::PathBuf;
use std::time::Duration;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use flakie::{
    download_batch, multipart_upload, BackoffConfig, DownloadRequest, MultipartConfig, S3Config,
    S3Source, Source, TransferConfig, UploadRequest,
};

struct Env {
    config: S3Config,
    client: aws_sdk_s3::Client,
    dir: PathBuf,
}

async fn env(test: &str) -> Option<Env> {
    let endpoint = std::env::var("FLAKIE_TEST_S3_ENDPOINT").ok()?;
    let var = |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.into());
    let mut config = S3Config::new(
        endpoint.clone(),
        var("FLAKIE_TEST_S3_BUCKET", "flakie-test"),
        var("FLAKIE_TEST_S3_ACCESS_KEY", "flakie"),
        var("FLAKIE_TEST_S3_SECRET_KEY", "flakie-secret"),
        var("FLAKIE_TEST_S3_REGION", "us-east-1"),
    );
    config.backoff = BackoffConfig {
        max_attempts: 2,
        base_delay: Duration::from_millis(10),
        max_delay: None,
        jitter: Duration::ZERO,
    };
    config.sdk_max_attempts = 2;

    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(Credentials::new(
                &config.access_key,
                &config.secret_key,
                None,
                None,
                "test",
            ))
            .region(Region::new(config.region.clone()))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .build(),
    );
    let _ = client.create_bucket().bucket(&config.bucket).send().await;

    let dir = std::env::temp_dir().join(format!("flakie-s3-{}-{test}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    Some(Env {
        config,
        client,
        dir,
    })
}

impl Env {
    fn source(&self) -> S3Source {
        S3Source::new(self.config.clone())
    }

    fn key(&self, name: &str) -> String {
        format!(
            "flakie-test/{}/{name}",
            self.dir.file_name().unwrap().to_string_lossy()
        )
    }

    async fn put(&self, key: &str, contents: &[u8]) -> String {
        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(contents.to_vec().into())
            .send()
            .await
            .unwrap()
            .e_tag()
            .unwrap()
            .to_string()
    }

    async fn get(&self, key: &str) -> Vec<u8> {
        self.client
            .get_object()
            .bucket(&self.config.bucket)
            .key(key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .to_vec()
    }

    /// Place a `.part` file (and optionally its ETag) next to `path`.
    fn seed_partial(&self, path: &std::path::Path, contents: &[u8], etag: Option<&str>) {
        std::fs::write(format!("{}.part", path.display()), contents).unwrap();
        if let Some(etag) = etag {
            std::fs::write(format!("{}.part.etag", path.display()), etag).unwrap();
        }
    }
}

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

#[tokio::test]
async fn upload_and_download_round_trip() {
    let Some(env) = env("roundtrip").await else {
        return;
    };
    let source = env.source();
    let contents = pattern(100_000, 1);
    let local = env.dir.join("local.bin");
    std::fs::write(&local, &contents).unwrap();
    let key = env.key("roundtrip.bin");

    source
        .upload(&local, &key, Some("application/octet-stream"))
        .await
        .unwrap();
    assert!(source.exists(&key).await.unwrap());

    let downloaded = env.dir.join("nested/dir/downloaded.bin");
    source.download(&key, &downloaded).await.unwrap();
    assert_eq!(std::fs::read(&downloaded).unwrap(), contents);
    assert!(!PathBuf::from(format!("{}.part", downloaded.display())).exists());
    assert!(!PathBuf::from(format!("{}.part.etag", downloaded.display())).exists());

    source.delete(&key).await.unwrap();
    assert!(!source.exists(&key).await.unwrap());
}

#[tokio::test]
async fn empty_object_round_trip() {
    let Some(env) = env("empty").await else {
        return;
    };
    let key = env.key("empty.bin");
    env.put(&key, b"").await;
    let path = env.dir.join("empty.bin");
    env.source().download(&key, &path).await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"");
}

#[tokio::test]
async fn resume_appends_to_a_partial_file_with_matching_etag() {
    let Some(env) = env("resume").await else {
        return;
    };
    let contents = pattern(50_000, 2);
    let key = env.key("resume.bin");
    let etag = env.put(&key, &contents).await;
    let path = env.dir.join("resume.bin");

    // A prefix that differs from the object proves the bytes were kept and
    // only the remainder was fetched.
    let marker = vec![0xAA; 1_000];
    env.seed_partial(&path, &marker, Some(&etag));
    env.source().download(&key, &path).await.unwrap();

    let downloaded = std::fs::read(&path).unwrap();
    assert_eq!(&downloaded[..1_000], &marker[..]);
    assert_eq!(&downloaded[1_000..], &contents[1_000..]);
}

#[tokio::test]
async fn stale_or_unverifiable_partials_restart_from_scratch() {
    let Some(env) = env("stale").await else {
        return;
    };
    let contents = pattern(50_000, 3);
    let key = env.key("stale.bin");
    let etag = env.put(&key, &contents).await;
    let source = env.source();

    let cases: [(&str, Vec<u8>, Option<&str>); 4] = [
        (
            "wrong-etag",
            vec![0xAA; 1_000],
            Some("\"0123456789abcdef\""),
        ),
        ("no-etag", vec![0xAA; 1_000], None),
        ("too-long", vec![0xAA; 60_000], Some(etag.as_str())),
        ("complete", contents.clone(), Some(etag.as_str())),
    ];
    for (name, partial, etag) in cases {
        let path = env.dir.join(name);
        env.seed_partial(&path, &partial, etag);
        source.download(&key, &path).await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), contents, "case {name}");
    }
}

#[tokio::test]
async fn object_replaced_between_attempts_is_not_spliced() {
    let Some(env) = env("replaced").await else {
        return;
    };
    let key = env.key("replaced.bin");
    let old_etag = env.put(&key, &pattern(50_000, 4)).await;
    let new_contents = pattern(50_000, 5);
    env.put(&key, &new_contents).await;

    let path = env.dir.join("replaced.bin");
    env.seed_partial(&path, &pattern(20_000, 4), Some(&old_etag));
    env.source().download(&key, &path).await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), new_contents);
}

#[tokio::test]
async fn multipart_upload_through_source_and_directly() {
    let Some(mut env) = env("multipart").await else {
        return;
    };
    let contents = pattern(12 * 1024 * 1024 + 123, 6);
    let local = env.dir.join("big.bin");
    std::fs::write(&local, &contents).unwrap();

    // Through `Source::upload`, above the threshold: three 5 MiB parts.
    env.config.multipart_threshold = Some(1);
    env.config.multipart.part_size = 1;
    let key = env.key("big.bin");
    env.source().upload(&local, &key, None).await.unwrap();
    assert_eq!(env.get(&key).await, contents);

    // Directly, including an empty file.
    let empty = env.dir.join("empty.bin");
    std::fs::write(&empty, b"").unwrap();
    let empty_key = env.key("empty-multipart.bin");
    multipart_upload(
        env.source(),
        &MultipartConfig::default(),
        &UploadRequest::new(&empty, &empty_key, None::<String>),
    )
    .await
    .unwrap();
    assert_eq!(env.get(&empty_key).await, b"");

    let uploads = env
        .client
        .list_multipart_uploads()
        .bucket(&env.config.bucket)
        .prefix(env.key(""))
        .send()
        .await
        .unwrap();
    assert!(uploads.uploads().is_empty(), "{:?}", uploads.uploads());
}

#[tokio::test]
async fn cancelled_multipart_upload_is_aborted() {
    let Some(env) = env("cancel").await else {
        return;
    };
    let local = env.dir.join("big.bin");
    std::fs::write(&local, pattern(30 * 1024 * 1024, 7)).unwrap();
    let key = env.key("cancelled.bin");

    let config = MultipartConfig {
        part_size: 5 * 1024 * 1024,
        parts: TransferConfig {
            concurrency: 1,
            ..MultipartConfig::default().parts
        },
    };
    let request = UploadRequest::new(&local, &key, None::<String>);
    let upload = multipart_upload(env.source(), &config, &request);
    // Long enough to create the upload, too short to finish six parts in series.
    let _ = tokio::time::timeout(Duration::from_millis(150), upload).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let uploads = env
        .client
        .list_multipart_uploads()
        .bucket(&env.config.bucket)
        .prefix(&key)
        .send()
        .await
        .unwrap();
    assert!(uploads.uploads().is_empty(), "{:?}", uploads.uploads());
}

#[tokio::test]
async fn missing_object_fails_alone_in_a_batch() {
    let Some(env) = env("batch").await else {
        return;
    };
    let key = env.key("present.bin");
    env.put(&key, b"present").await;

    let report = download_batch(
        env.source(),
        &TransferConfig {
            max_attempts: 1,
            ..TransferConfig::default()
        },
        vec![
            DownloadRequest::new(&key, env.dir.join("present.bin")),
            DownloadRequest::new(env.key("missing.bin"), env.dir.join("missing.bin")),
        ],
    )
    .await
    .unwrap();

    assert_eq!(report.completed.len(), 1);
    assert_eq!(report.completed[0].0.key, key);
    assert_eq!(report.failed.len(), 1);
    assert!(report.failed[0].item.key.ends_with("missing.bin"));
    assert_eq!(
        std::fs::read(env.dir.join("present.bin")).unwrap(),
        b"present"
    );
}
