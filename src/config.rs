use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Retry backoff used by sources that resume a single transfer mid-stream.
///
/// This is deliberately separate from [`TransferConfig`], which controls
/// whole-item retries in the queue engine. A source can use this to retry and
/// resume one large object, while the engine decides whether the item failed
/// entirely and should be placed back on the queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackoffConfig {
    /// Maximum number of attempts before the source gives up on an object.
    pub max_attempts: u32,
    /// Delay before the first retry. Later retries double this delay.
    pub base_delay: Duration,
    /// Optional upper bound for the exponential delay.
    pub max_delay: Option<Duration>,
    /// Random extra delay added to each retry, up to this duration.
    pub jitter: Duration,
}

impl BackoffConfig {
    pub fn new(max_attempts: u32, base_delay: Duration) -> Self {
        Self {
            max_attempts,
            base_delay,
            max_delay: None,
            jitter: Duration::ZERO,
        }
    }

    /// Delay to wait after `attempt` failures, where `attempt` is zero-based.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let exponential = self
            .base_delay
            .saturating_mul(2_u32.saturating_pow(attempt));
        let capped = match self.max_delay {
            Some(max) => exponential.min(max),
            None => exponential,
        };
        capped.saturating_add(random_jitter(self.jitter))
    }
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_secs(1),
            max_delay: None,
            jitter: Duration::from_secs(1),
        }
    }
}

/// Whole-item transfer settings used by the download and upload engines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferConfig {
    /// Number of items moved concurrently.
    pub concurrency: usize,
    /// Number of attempts made before an item is reported as failed.
    pub max_attempts: u32,
    /// Fixed delay before a failed item is moved back to the queue.
    pub retry_delay: Duration,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            concurrency: 1,
            max_attempts: 3,
            retry_delay: Duration::from_secs(1),
        }
    }
}

/// Configuration for an S3-compatible source such as Hetzner Object Storage.
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
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(120),
            stalled_stream_grace_period: Duration::from_secs(60),
            backoff: BackoffConfig::default(),
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
            .finish()
    }
}

fn random_jitter(max: Duration) -> Duration {
    if max.is_zero() {
        return Duration::ZERO;
    }

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let span = max.as_nanos().saturating_add(1);
    Duration::from_nanos((nanos % span) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_respects_cap() {
        let backoff = BackoffConfig {
            max_attempts: 4,
            base_delay: Duration::from_secs(1),
            max_delay: Some(Duration::from_secs(3)),
            jitter: Duration::ZERO,
        };

        assert_eq!(backoff.delay_for_attempt(0), Duration::from_secs(1));
        assert_eq!(backoff.delay_for_attempt(1), Duration::from_secs(2));
        assert_eq!(backoff.delay_for_attempt(2), Duration::from_secs(3));
        assert_eq!(backoff.delay_for_attempt(3), Duration::from_secs(3));
    }

    #[test]
    fn backoff_saturates_instead_of_panicking() {
        let backoff = BackoffConfig::new(2, Duration::from_secs(1));

        let delay = backoff.delay_for_attempt(100);

        assert_eq!(delay, Duration::from_secs(u32::MAX as u64));
    }
}
