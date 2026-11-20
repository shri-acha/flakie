//! Configurable, resumable work against flaky sources.
//!
//! `flakie` separates *what* is done from the retry policy used to do it. The
//! queue engine ([`run_batch`] and [`run_all`]) runs any keyed async job with
//! bounded concurrency, per-item retries, backoff, attempt timeouts, and
//! optional Rhai retry policies. [`download_batch`], [`upload_batch`], and
//! their all-or-nothing counterparts move files through any [`Source`], and
//! [`multipart_upload`] uploads one file in independently retried parts
//! through any [`MultipartSource`].
//!
//! With the default `s3` feature, [`S3Source`] provides a resumable source for
//! any S3-compatible object store.

mod config;
mod engine;
mod multipart;
#[cfg(feature = "s3")]
mod s3;
mod source;
mod transfer;

pub use config::{BackoffConfig, MultipartConfig, TransferConfig};
pub use engine::{run_all, run_batch, BatchError, BatchReport, FailedItem, ItemFailure, Keyed};
pub use multipart::{multipart_upload, MultipartSource, PartLimits, UploadedPart};
#[cfg(feature = "s3")]
pub use s3::{S3Config, S3Source};
pub use source::{DownloadRequest, Source, UploadRequest};
pub use transfer::{
    download_all, download_batch, upload_all, upload_batch, CompletedDownload, CompletedUpload,
};
