//! Configurable, resumable work against flaky sources.
//!
//! `flakie` separates *what* is done from the retry policy used to do it. The
//! queue engine ([`run_all`]) runs any keyed async job with bounded
//! concurrency, per-item retries, backoff, attempt timeouts, and optional Rhai
//! retry policies. [`download_all`] and [`upload_all`] build on it to move
//! files through any [`Source`].
//!
//! With the default `s3` feature, [`S3Source`] provides a resumable source for
//! any S3-compatible object store.

mod config;
mod engine;
#[cfg(feature = "s3")]
mod s3;
mod source;

pub use config::{BackoffConfig, TransferConfig};
pub use engine::{download_all, run_all, upload_all, CompletedDownload, CompletedUpload, Keyed};
#[cfg(feature = "s3")]
pub use s3::{S3Config, S3Source};
pub use source::{DownloadRequest, Source, UploadRequest};
