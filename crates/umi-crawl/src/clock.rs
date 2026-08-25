//! Where time comes from.
//!
//! Doc 11.1 says nothing in umi-frontier, umi-file or umi-publish reads a
//! clock, and the reason generalises: a function that reads the clock cannot
//! be replayed, cannot be tested without sleeping, and cannot be compared
//! across two machines. The row builder in [`page`](crate::page) follows that
//! rule by taking `fetched_at_ms` as an argument.
//!
//! The loop cannot, because something has to actually ask what time it is
//! before it can pass the answer down. This is that something, and making it a
//! trait keeps it to one place: a test drives a whole crawl through
//! [`FixedClock`] and gets the same rows every run, and production passes
//! [`SystemClock`] and nothing else in the crate knows the difference.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// What the loop needs from a clock.
pub trait Clock: Send + Sync {
    /// Milliseconds since the Unix epoch.
    ///
    /// Not required to be monotonic, because the value is a timestamp that
    /// goes in a published row and has to mean what everybody else means by
    /// it. Callers that want to measure a duration use `Instant` and do not
    /// come here.
    fn now_ms(&self) -> u64;
}

/// The wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        // A clock before 1970 is a broken machine rather than a case worth
        // handling, and returning zero from it is honest: every due time
        // computed from it is in the past, so the crawl works rather than
        // stalls, and every row it writes has an obviously wrong timestamp
        // instead of a plausible one.
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| {
                u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
            })
    }
}

/// A clock a test moves by hand.
///
/// Shared rather than owned, because the loop takes the clock and the test
/// still needs to advance it, and `&` plus an atomic is a smaller thing than
/// threading a mutex through the loop's signature.
#[derive(Debug)]
pub struct FixedClock(AtomicU64);

impl FixedClock {
    /// A clock stopped at `now_ms`.
    #[must_use]
    pub const fn at(now_ms: u64) -> Self {
        Self(AtomicU64::new(now_ms))
    }

    /// Move it forward.
    pub fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::Relaxed);
    }

    /// Put it somewhere specific.
    pub fn set(&self, now_ms: u64) {
        self.0.store(now_ms, Ordering::Relaxed);
    }
}

impl Clock for FixedClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl<T: Clock + ?Sized> Clock for std::sync::Arc<T> {
    fn now_ms(&self) -> u64 {
        (**self).now_ms()
    }
}
