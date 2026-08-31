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

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// What the loop needs from a clock.
#[async_trait::async_trait]
pub trait Clock: Send + Sync {
    /// Milliseconds since the Unix epoch.
    ///
    /// Not required to be monotonic, because the value is a timestamp that
    /// goes in a published row and has to mean what everybody else means by
    /// it. Callers that want to measure a duration use `Instant` and do not
    /// come here.
    fn now_ms(&self) -> u64;

    /// Wait until `when_ms`, and return at once if it has already gone by.
    ///
    /// Waiting is here rather than at the call site because a fetch held back
    /// by doc 07.6's politeness timer has to be held back in a test too, and a
    /// test that sleeps for real takes as long as the politeness delay it is
    /// checking. A crawl of one host at one request a second is a minute of
    /// wall time for sixty pages, which is correct in production and is not a
    /// test anybody will run.
    async fn sleep_until_ms(&self, when_ms: u64);
}

/// The wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

#[async_trait::async_trait]
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

    async fn sleep_until_ms(&self, when_ms: u64) {
        // Read once and subtract, rather than looping until the wall clock
        // passes the target. A wall clock can be stepped backwards by ntp,
        // and a loop that trusts it would hold a lease for however far back
        // it went. One sleep of a bounded length cannot do that.
        let now_ms = self.now_ms();
        if when_ms > now_ms {
            tokio::time::sleep(Duration::from_millis(when_ms - now_ms)).await;
        }
    }
}

/// A clock a test moves by hand.
///
/// Shared rather than owned, because the loop takes the clock and the test
/// still needs to advance it, and `&` plus an atomic is a smaller thing than
/// threading a mutex through the loop's signature.
///
/// It runs time forward on its own as well, and that part is not decoration.
/// See [`sleep_until_ms`](Clock::sleep_until_ms).
#[derive(Debug)]
pub struct FixedClock {
    now: AtomicU64,
    /// What the sleepers are waiting for, one entry each.
    waiting: Mutex<Vec<u64>>,
}

/// How many turns a sleeper gives the others before it moves the clock.
///
/// One is not enough. The sleeps of a window start in the order the leases
/// were claimed, so the earliest sleeper is usually the first to register, but
/// a task that has not been polled yet has registered nothing, and a clock
/// that jumped on the first turn could pass a slot before its owner asked for
/// it. Two turns is every task that was ready when this one started.
const TURNS: u32 = 2;

impl FixedClock {
    /// A clock stopped at `now_ms`.
    #[must_use]
    pub const fn at(now_ms: u64) -> Self {
        Self {
            now: AtomicU64::new(now_ms),
            waiting: Mutex::new(Vec::new()),
        }
    }

    /// Move it forward.
    pub fn advance(&self, ms: u64) {
        self.now.fetch_add(ms, Ordering::Relaxed);
    }

    /// Put it somewhere specific.
    pub fn set(&self, now_ms: u64) {
        self.now.store(now_ms, Ordering::Relaxed);
    }

    fn waiting(&self) -> std::sync::MutexGuard<'_, Vec<u64>> {
        // Nothing under this lock can panic, so recovering the guard is
        // recovering from a panic somewhere else.
        self.waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait::async_trait]
impl Clock for FixedClock {
    fn now_ms(&self) -> u64 {
        self.now.load(Ordering::Relaxed)
    }

    /// Let everybody else run, and move the clock only when this sleeper is
    /// the one with the least to wait for.
    ///
    /// The obvious fake clock jumps straight to `when_ms` and returns, and it
    /// is wrong in both directions once a tick runs its fetches as separate
    /// tasks. It never yields, so a sleeping lease is not actually behind
    /// anything and never learns that an origin asked the rest of its batch to
    /// wait. And the jump is a single shared number, so a lease waiting four
    /// seconds drags the clock forward under a lease waiting one, and both
    /// record the same time. Tests that check doc 07.6's spacing then read
    /// whatever order the executor happened to poll in.
    ///
    /// So this waits like a real sleep does. It yields until the clock reaches
    /// `when_ms`, and the only sleeper allowed to move the clock is the one
    /// with the earliest deadline, which moves it exactly to that deadline.
    /// Sleepers then wake in the order they would wake on a wall clock, each
    /// reading its own slot, and no test has to sleep for real to see it.
    async fn sleep_until_ms(&self, when_ms: u64) {
        if self.now_ms() >= when_ms {
            // Still a turn for everybody else. A politeness window nobody has
            // to wait for is not a reason to hold the executor.
            tokio::task::yield_now().await;
            return;
        }
        self.waiting().push(when_ms);
        let mut turns = 0;
        while self.now_ms() < when_ms {
            tokio::task::yield_now().await;
            turns += 1;
            if turns > TURNS && self.waiting().iter().min() == Some(&when_ms) {
                self.now.fetch_max(when_ms, Ordering::Relaxed);
            }
        }
        let mut waiting = self.waiting();
        if let Some(mine) = waiting.iter().position(|&at| at == when_ms) {
            waiting.swap_remove(mine);
        }
    }
}

#[async_trait::async_trait]
impl<T: Clock + ?Sized> Clock for std::sync::Arc<T> {
    fn now_ms(&self) -> u64 {
        (**self).now_ms()
    }

    async fn sleep_until_ms(&self, when_ms: u64) {
        (**self).sleep_until_ms(when_ms).await;
    }
}
