//! Counters, gauges and histograms, all of them atomics.
//!
//! No locks anywhere. A counter is incremented on the path that runs 250 times
//! a second per core and a gauge is set once a tick, and a mutex around either
//! of them would be a contention point introduced by the thing measuring the
//! contention. `Relaxed` ordering throughout, because nothing here orders
//! anything else: a scrape that reads a counter one increment behind is a
//! scrape that arrived a nanosecond earlier, and no decision is made on the
//! difference.
//!
//! Everything is `u64` or a `f64` bit pattern rather than a float in a lock.
//! Prometheus counters are monotonic integers in practice, and the two
//! quantities here that are genuinely fractional, a ratio and a lag, are
//! gauges that get written whole.

use std::sync::atomic::{AtomicU64, Ordering};

/// A number that only goes up.
#[derive(Debug, Default)]
pub struct Counter(AtomicU64);

impl Counter {
    /// A counter at zero.
    #[must_use]
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Add one.
    pub fn inc(&self) {
        self.add(1);
    }

    /// Add `n`.
    ///
    /// Saturating rather than wrapping. A counter that wraps looks to
    /// Prometheus like a process restart, and a fake restart in the middle of
    /// a crawl is worse than a number that stops moving after 18 quintillion
    /// pages.
    pub fn add(&self, n: u64) {
        let _ = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(n))
            });
    }

    /// What it holds.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A number that goes both ways.
#[derive(Debug, Default)]
pub struct Gauge(AtomicU64);

impl Gauge {
    /// A gauge at zero.
    #[must_use]
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Replace the value.
    pub fn set(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }

    /// What it holds.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A number that goes both ways and is not a whole one.
///
/// Stored as the bit pattern rather than as a fixed point integer, because the
/// two users are a ratio between zero and one and a lag in seconds, and a
/// fixed point scale that suited one would be wrong for the other.
#[derive(Debug, Default)]
pub struct FloatGauge(AtomicU64);

impl FloatGauge {
    /// A gauge at zero.
    #[must_use]
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Replace the value.
    pub fn set(&self, value: f64) {
        self.0.store(value.to_bits(), Ordering::Relaxed);
    }

    /// What it holds.
    #[must_use]
    pub fn get(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
}

/// How many buckets a histogram has, not counting the implicit `+Inf`.
pub const BUCKETS: usize = 12;

/// A histogram over a fixed set of upper bounds.
///
/// Fixed at construction and never resized, so an observation is one compare
/// loop and two atomic adds with no allocation and no lock. Twelve buckets is
/// enough to see a p99 move and few enough that thirty series of them is a few
/// kilobytes rather than a few megabytes.
///
/// The bounds are per family rather than shared. A fetch takes tens of
/// milliseconds to tens of seconds and a state operation takes microseconds,
/// and one set of bounds covering both would put every state operation in the
/// first bucket, which is a histogram that has learned nothing.
#[derive(Debug)]
pub struct Histogram {
    bounds: [f64; BUCKETS],
    counts: [AtomicU64; BUCKETS],
    /// Observations above the last bound, which is Prometheus's `+Inf` bucket
    /// minus everything below it.
    overflow: AtomicU64,
    sum: FloatGauge,
    count: Counter,
}

impl Histogram {
    /// A histogram over these upper bounds, which must be ascending.
    #[must_use]
    pub const fn new(bounds: [f64; BUCKETS]) -> Self {
        Self {
            bounds,
            counts: [const { AtomicU64::new(0) }; BUCKETS],
            overflow: AtomicU64::new(0),
            sum: FloatGauge::new(),
            count: Counter::new(),
        }
    }

    /// Record one measurement, in the family's own unit.
    ///
    /// Doc 15.4's histograms are all in seconds, and passing milliseconds to
    /// one of them is the mistake this cannot catch, which is why every call
    /// site converts at the call site rather than being handed a duration and
    /// guessing.
    pub fn observe(&self, value: f64) {
        // Linear rather than a binary search. Twelve compares on a predictable
        // branch beat a search with a data dependent one, and the bench says
        // the whole call is single digit nanoseconds.
        let mut slot = None;
        for (index, bound) in self.bounds.iter().enumerate() {
            if value <= *bound {
                slot = Some(index);
                break;
            }
        }
        match slot {
            Some(index) => {
                self.counts[index].fetch_add(1, Ordering::Relaxed);
            }
            None => {
                self.overflow.fetch_add(1, Ordering::Relaxed);
            }
        }
        // Read, add, store rather than a compare and swap loop. Two threads
        // racing here lose an observation from the sum and keep it in the
        // count, which shows up as a mean that is a fraction of a percent low
        // on a histogram nobody computes a mean from. A CAS loop on the hot
        // path to fix that would be the wrong trade.
        self.sum.set(self.sum.get() + value);
        self.count.inc();
    }

    /// The cumulative counts Prometheus wants, paired with their bounds.
    ///
    /// Cumulative and not per bucket: the text format's `le` is "less than or
    /// equal", so each bucket carries everything below it as well.
    #[must_use]
    pub fn cumulative(&self) -> [(f64, u64); BUCKETS] {
        let mut running = 0u64;
        let mut out = [(0.0, 0u64); BUCKETS];
        for (index, bound) in self.bounds.iter().enumerate() {
            running = running.saturating_add(self.counts[index].load(Ordering::Relaxed));
            out[index] = (*bound, running);
        }
        out
    }

    /// Everything observed, which is the `+Inf` bucket.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count.get()
    }

    /// The sum of every observation.
    #[must_use]
    pub fn sum(&self) -> f64 {
        self.sum.get()
    }
}

/// Bounds for something measured in milliseconds to tens of seconds: a fetch,
/// a DNS lookup, a publish step.
///
/// Doc 05's timeout is 30 seconds and doc 12.2 budgets 10 minutes for a
/// publish, so the top of the range has to be up there or every slow publish
/// lands in `+Inf` and the alarm in doc 15.6 has nothing to fire on.
pub const SECONDS_WIDE: [f64; BUCKETS] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 30.0, 600.0,
];

/// Bounds for something measured in microseconds to milliseconds: a state
/// round trip, an extract.
///
/// Doc 11.9 budgets 0.8 to 1.5 ms for an extract and doc 08.5's batched lease
/// is tens of microseconds a URL, so the interesting range is four orders of
/// magnitude below the one above.
pub const SECONDS_FINE: [f64; BUCKETS] = [
    0.000_01, 0.000_025, 0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.0025, 0.005, 0.01, 0.1, 1.0,
];

/// Bounds for doc 06's reputation, which is a score between zero and one.
pub const UNIT: [f64; BUCKETS] = [0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0];
