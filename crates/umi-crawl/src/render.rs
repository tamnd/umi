//! Doc 05.9's render budget, which is global rather than per host.
//!
//! The reason it is global is in the spec's first sentence: a fleet that
//! decides 20 percent of the web needs rendering will simply stop. Per host
//! escalation cannot see that. Doc 05.8 looks at one host, decides it serves a
//! client rendered shell and asks for a browser, and it is right every time,
//! and a hundred thousand hosts each being right on their own is how a crawler
//! ends up with eight tabs and a queue it will never finish.
//!
//! So the ladder decides which pages *want* a browser and this decides how many
//! of them get one. The two answers are meant to disagree, and what happens to
//! the difference is the other half of doc 05.9: it is deferred, not failed.
//! The lease goes back to the frontier unanswered, keeping its due time and its
//! priority, so the next tick offers the most important of them again. That is
//! the deferred queue, and it is the frontier rather than a second structure
//! here because the frontier already orders by priority and already survives a
//! restart, and a render queue in memory would do neither.
//!
//! # Why the numbers are asked for rather than configured
//!
//! Doc 05.9 works its example at 8 tabs over 2 seconds, so 4 pages a second.
//! The T3 bench measures 1.8 on server2 and 2.59 on server3, which makes the
//! spec's estimate optimistic by about a factor of two. Rather than write a
//! corrected constant that will be wrong on the next box, the budget asks the
//! browser pool what it is actually managing, every tick. The pool knows,
//! because it has been counting.

use std::sync::Mutex;
use std::time::Duration;

/// How much of a crawl may be rendered, and how much may be emulated.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RenderPolicy {
    /// Pages a second this process is aiming at, which is doc 01's 250 per
    /// server.
    ///
    /// The other half of doc 05.9's `min`, and the half that does not depend
    /// on the hardware. It is the target rather than the measured rate on
    /// purpose: a crawl that has slowed down for an unrelated reason should
    /// not also lose its browser.
    pub page_rate: f64,
    /// Doc 05.9's `max_render_fraction`, which is one percent.
    pub max_fraction: f64,
    /// How long a page may wait for a render slot before it is deferred.
    ///
    /// This is what makes the budget a queue rather than a gate. Rendering is
    /// slow enough that a page arriving a few seconds early should wait, and a
    /// page arriving a minute early should go back to the frontier so that the
    /// slot goes to whatever is most important when it comes free rather than
    /// to whatever asked first.
    ///
    /// It also caps how much of a tick's window the browser can hold: at most
    /// `rate * wait` leases can be waiting, which at 2 pages a second and five
    /// seconds is ten.
    pub wait: Duration,
    /// Doc 05.9's alert line for T2, which is 15 percent of volume.
    ///
    /// An alert and not a throttle. T2 is cheap on CPU and the reason to watch
    /// it is that a sudden spike usually means a vendor changed a default rule,
    /// which is a thing for a person to look at rather than for a crawler to
    /// route around.
    pub max_emulated: f64,
}

impl Default for RenderPolicy {
    fn default() -> Self {
        Self {
            page_rate: 250.0,
            max_fraction: 0.01,
            wait: Duration::from_secs(5),
            max_emulated: 0.15,
        }
    }
}

/// What the budget says about one page that wants a browser.
///
/// Three answers and not two, because "wait your turn" and "there is no queue
/// to wait in" are different situations with different right responses, and a
/// budget that answered no to both would stall a crawl on a machine that never
/// had a browser in the first place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Slot {
    /// Render it, no earlier than this millisecond.
    At(u64),
    /// Doc 05.9's deferred queue. There is a browser and it is busy, so this
    /// page goes back to the frontier and somebody asks again later.
    Defer,
    /// Nothing in reach renders at all.
    ///
    /// Not a deferral, because deferring assumes somebody will eventually be
    /// able to do the work and here nobody can. The page goes out at whatever
    /// rung the ladder does have, which is the same thing a build with no
    /// `emulation` feature does with a lease that asks for T2: the crawl keeps
    /// moving, the answer is probably a shell, and doc 05.8's weekly probe
    /// brings the host back down to a tier this fleet can actually serve.
    ///
    /// The alternative was tried on paper and it is worse. A deferred lease
    /// keeps its due time, so a tick would lease it, defer it, and lease it
    /// again on the next tick, for the week until the probe. That is a spin,
    /// and it spends the frontier's whole lease budget on pages nothing can
    /// fetch.
    NoBrowser,
}

/// The budget itself.
///
/// A virtual clock rather than a token bucket, which is the same arithmetic
/// with one fewer field: `next_ms` is when the next render may start, every
/// grant pushes it forward by one interval, and a caller that would have to
/// wait longer than [`RenderPolicy::wait`] is told no. There is no burst
/// allowance, because the thing being rationed is a browser and a browser
/// cannot catch up.
#[derive(Debug)]
pub struct RenderBudget {
    policy: RenderPolicy,
    bucket: Mutex<Bucket>,
}

#[derive(Debug, Default)]
struct Bucket {
    /// Milliseconds between two renders, or `None` for a process that cannot
    /// render at all.
    interval_ms: Option<u64>,
    /// The earliest the next render may start.
    next_ms: u64,
    granted: u64,
    deferred: u64,
}

impl RenderBudget {
    /// A budget with no browser behind it yet.
    ///
    /// It refuses everything until [`observe`](Self::observe) is told what the
    /// pool can do, which is the right way round: a crawl that has not found a
    /// browser has not got one.
    #[must_use]
    pub fn new(policy: RenderPolicy) -> Self {
        Self {
            policy,
            bucket: Mutex::new(Bucket::default()),
        }
    }

    /// Tell the budget what the browser pool is managing, in pages a second.
    ///
    /// `None` is a process with no browser. Doc 05.9's `min` is applied here,
    /// so this is the one place the two halves of the formula meet.
    pub fn observe(&self, capacity: Option<f64>) {
        let rate = capacity.map(|pool| pool.min(self.policy.page_rate * self.policy.max_fraction));
        let interval = match rate {
            // Rounded up, so a rate that does not divide a second evenly comes
            // out under the budget rather than over it.
            Some(rate) if rate > 0.0 => Some((1000.0 / rate).ceil() as u64),
            _ => None,
        };
        let mut bucket = self.lock();
        bucket.interval_ms = interval;
    }

    /// Take a slot for one render.
    ///
    /// A slot is only spent on [`Slot::At`]. The other two answers take
    /// nothing, so a tick full of pages that want a browser does not use up a
    /// budget it never got to spend.
    pub fn take(&self, now_ms: u64) -> Slot {
        let mut bucket = self.lock();
        let Some(interval) = bucket.interval_ms else {
            return Slot::NoBrowser;
        };
        let at = bucket.next_ms.max(now_ms);
        let wait_ms = u64::try_from(self.policy.wait.as_millis()).unwrap_or(u64::MAX);
        if at.saturating_sub(now_ms) > wait_ms {
            bucket.deferred += 1;
            return Slot::Defer;
        }
        bucket.next_ms = at.saturating_add(interval);
        bucket.granted += 1;
        Slot::At(at)
    }

    /// The budget in pages a second, which is zero for a process that cannot
    /// render.
    #[must_use]
    pub fn rate(&self) -> f64 {
        match self.lock().interval_ms {
            Some(interval) if interval > 0 => 1000.0 / interval as f64,
            _ => 0.0,
        }
    }

    /// Renders allowed since the process started.
    #[must_use]
    pub fn granted(&self) -> u64 {
        self.lock().granted
    }

    /// Renders deferred since the process started.
    ///
    /// A number that keeps climbing is not a fault. It is the fleet saying it
    /// has found more pages that need a browser than it has browser, which doc
    /// 05.9 answers with community fetchers rather than with more tabs.
    #[must_use]
    pub fn deferred(&self) -> u64 {
        self.lock().deferred
    }

    /// The policy this was built with.
    #[must_use]
    pub const fn policy(&self) -> &RenderPolicy {
        &self.policy
    }

    /// The lock, with a poisoned one taken anyway.
    ///
    /// Nothing under this lock can panic, so a poisoned mutex here means a
    /// panic somewhere else entirely, and refusing to render because of that
    /// would turn one unrelated bug into a stalled crawl.
    fn lock(&self) -> std::sync::MutexGuard<'_, Bucket> {
        self.bucket.lock().unwrap_or_else(|e| e.into_inner())
    }
}
