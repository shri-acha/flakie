//! Configurable, resumable downloads and uploads from flaky sources.
//!
//! `flakie` separates the source of data from the retry policy used to move
//! data. Sources are simple async traits, while the transfer engine applies
//! bounded concurrency, per-item retries, and stable ordering. The first
//! built-in source speaks S3, configured the same way Hetzner Object Storage
//! is configured.

mod config;
mod engine;
mod s3;
mod source;

pub use config::{BackoffConfig, S3Config, TransferConfig};
pub use engine::{download_all, upload_all, CompletedDownload, CompletedUpload};
pub use s3::S3Source;
pub use source::{DownloadRequest, Source, UploadRequest};
