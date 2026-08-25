//! Bounded cumulative duration-histogram state.

use std::{fmt, time::Duration};

/// Maximum number of configured finite duration histogram upper bounds.
pub const MAX_DURATION_BUCKETS: usize = 32;

/// Default cumulative upper bounds for Rustee duration histograms.
pub const DEFAULT_DURATION_BUCKETS: [Duration; 12] = [
    Duration::from_millis(1),
    Duration::from_millis(5),
    Duration::from_millis(10),
    Duration::from_millis(25),
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_millis(2500),
    Duration::from_secs(5),
    Duration::from_secs(10),
];

/// Invalid bounded duration-histogram configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurationHistogramConfigError {
    /// No finite histogram upper bound was configured.
    EmptyBuckets,
    /// More than [`MAX_DURATION_BUCKETS`] finite histogram upper bounds were configured.
    TooManyBuckets,
    /// A histogram upper bound was zero.
    ZeroBucket,
    /// Histogram upper bounds were duplicated or out of ascending order.
    UnorderedBuckets,
}

impl fmt::Display for DurationHistogramConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyBuckets => "at least one duration histogram bucket is required",
            Self::TooManyBuckets => "duration histogram supports at most 32 finite buckets",
            Self::ZeroBucket => "duration histogram buckets must be greater than zero",
            Self::UnorderedBuckets => "duration histogram buckets must be strictly increasing",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for DurationHistogramConfigError {}

/// Cumulative counts for a fixed, validated set of duration upper bounds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurationHistogram {
    buckets: Vec<Duration>,
    counts: Vec<u64>,
    total_duration: Duration,
}

impl DurationHistogram {
    /// Creates an empty cumulative histogram from strictly increasing non-zero upper bounds.
    ///
    /// # Errors
    ///
    /// Returns [`DurationHistogramConfigError`] when bounds are empty, too numerous, zero, or
    /// not strictly increasing.
    pub fn new(
        buckets: impl IntoIterator<Item = Duration>,
    ) -> Result<Self, DurationHistogramConfigError> {
        let mut collected = Vec::with_capacity(MAX_DURATION_BUCKETS);
        for bucket in buckets {
            if collected.len() == MAX_DURATION_BUCKETS {
                return Err(DurationHistogramConfigError::TooManyBuckets);
            }
            collected.push(bucket);
        }
        let buckets = collected;
        validate_buckets(&buckets)?;
        Ok(Self {
            counts: vec![0; buckets.len()],
            buckets,
            total_duration: Duration::ZERO,
        })
    }

    /// Records one completed operation duration into every matching cumulative bucket.
    pub fn observe(&mut self, duration: Duration) {
        for (upper_bound, count) in self.buckets.iter().zip(&mut self.counts) {
            if duration <= *upper_bound {
                *count = count.saturating_add(1);
            }
        }
        self.total_duration = self.total_duration.saturating_add(duration);
    }

    /// Iterates cumulative counts in ascending duration-upper-bound order.
    #[must_use]
    pub fn bucket_counts(&self) -> impl ExactSizeIterator<Item = (Duration, u64)> + '_ {
        self.buckets
            .iter()
            .copied()
            .zip(self.counts.iter().copied())
    }

    /// Returns the saturating sum of all observed durations.
    #[must_use]
    pub const fn total_duration(&self) -> Duration {
        self.total_duration
    }
}

fn validate_buckets(buckets: &[Duration]) -> Result<(), DurationHistogramConfigError> {
    if buckets.is_empty() {
        return Err(DurationHistogramConfigError::EmptyBuckets);
    }
    if buckets.len() > MAX_DURATION_BUCKETS {
        return Err(DurationHistogramConfigError::TooManyBuckets);
    }
    if buckets.iter().any(Duration::is_zero) {
        return Err(DurationHistogramConfigError::ZeroBucket);
    }
    if buckets.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(DurationHistogramConfigError::UnorderedBuckets);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        DEFAULT_DURATION_BUCKETS, DurationHistogram, DurationHistogramConfigError,
        MAX_DURATION_BUCKETS,
    };

    #[test]
    fn histogram_rejects_invalid_bounds_before_allocating_counts() {
        assert_eq!(
            DurationHistogram::new(std::iter::empty()).unwrap_err(),
            DurationHistogramConfigError::EmptyBuckets
        );
        assert_eq!(
            DurationHistogram::new([Duration::ZERO]).unwrap_err(),
            DurationHistogramConfigError::ZeroBucket
        );
        assert_eq!(
            DurationHistogram::new([Duration::from_secs(2), Duration::from_secs(1)]).unwrap_err(),
            DurationHistogramConfigError::UnorderedBuckets
        );
    }

    #[test]
    fn histogram_stops_collecting_after_the_first_excess_bucket() {
        let buckets = (1..=MAX_DURATION_BUCKETS + 1)
            .map(|seconds| Duration::from_secs(seconds as u64))
            .chain(std::iter::once_with(|| {
                panic!("histogram must not read past the first excess bucket")
            }));

        assert_eq!(
            DurationHistogram::new(buckets).unwrap_err(),
            DurationHistogramConfigError::TooManyBuckets
        );
    }

    #[test]
    fn histogram_keeps_bounds_counts_and_total_in_lockstep() {
        let mut histogram =
            DurationHistogram::new([Duration::from_millis(1), Duration::from_millis(5)]).unwrap();

        histogram.observe(Duration::from_millis(1));
        histogram.observe(Duration::from_millis(2));
        histogram.observe(Duration::from_millis(6));

        assert_eq!(
            histogram.bucket_counts().collect::<Vec<_>>(),
            [(Duration::from_millis(1), 1), (Duration::from_millis(5), 2),]
        );
        assert_eq!(histogram.total_duration(), Duration::from_millis(9));
        assert_eq!(DEFAULT_DURATION_BUCKETS.len(), 12);
    }

    #[test]
    fn histogram_saturates_bucket_counts_and_total_duration() {
        let mut histogram = DurationHistogram::new([Duration::from_millis(1)]).unwrap();
        histogram.counts.fill(u64::MAX);
        histogram.total_duration = Duration::MAX;

        histogram.observe(Duration::from_millis(1));

        assert_eq!(
            histogram.bucket_counts().collect::<Vec<_>>(),
            [(Duration::from_millis(1), u64::MAX)]
        );
        assert_eq!(histogram.total_duration(), Duration::MAX);
    }
}
