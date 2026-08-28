//! When to look at a page again, from doc 09.4.
//!
//! This lives in the trait crate rather than in each backend for one reason:
//! four backends with four refresh policies would make the crawl's freshness
//! depend on which store an operator picked, and freshness is the thing doc 01
//! claims over Common Crawl. So it is one function and every backend calls it.
//!
//! The model is the classic one. A page changes as a Poisson process with rate
//! lambda, we only ever observe "changed since last time" or "did not change",
//! and multiple changes between two visits look like one. The bias corrected
//! estimator for that is
//!
//! ```text
//! lambda_hat = -log((n - x + 0.5) / (n + 0.5)) / mean_interval
//! ```
//!
//! where `n` is the number of intervals we have actually served, `x` is how
//! many of them ended with different content, and `mean_interval` is the total
//! observation time divided by `n`. The half terms are the standard smoothing.
//! They matter at the top end: a page that changed on all four of four visits
//! would otherwise give an infinite rate instead of a large one. At the bottom
//! end they do nothing, and a page with no observed change gets a rate of
//! exactly zero, which is correct. Zero is not a claim that the page will never
//! change, it is a claim that we have no evidence of a change yet, and the
//! evidence rule below is what stops that turning into a six month nap.
//!
//! The revisit interval is not proportional to lambda, and this is the part
//! that gets implemented wrong most often. A page that changes every minute
//! cannot be kept fresh at any budget, so spending on it is waste. So the
//! policy is non monotonic: the interval falls as lambda rises, up to
//! [`CHURN_CEILING`], and then jumps back up to a daily sample because we
//! cannot represent the page faithfully and pretending otherwise burns capacity
//! other pages would use better.
//!
//! # Two rules that are not in doc 09.4
//!
//! The estimator on its own extrapolates from nothing. Two fetches an hour
//! apart with no change seen gives lambda zero, which clamps to
//! [`MAX_REFRESH`], and a page we have watched for one hour does not deserve a
//! six month interval. So the interval is also capped at twice the time we have
//! actually watched the page. That makes the walk upward geometric rather than
//! instant, and it falls away on its own once the observation window is longer
//! than the answer.
//!
//! One thing doc 09.4 promises and this does not quite deliver: a page above
//! the ceiling is supposed to drop to a daily sample, and in practice it parks
//! an hour or two apart instead. The reason is that a page changing every
//! minute and a page changing every hour look exactly the same from here if we
//! only look once an hour, so the estimate is a lower bound rather than a
//! measurement, and the ceiling only fires when that lower bound alone is
//! enough. Sampling faster to tell them apart costs more than it saves. The
//! real answer is the `Last-Modified` header, which gives the change time
//! rather than a bit saying something changed, and that is the publisher signal
//! doc 09.4 says beats the estimator.
//!
//! The second is [`K_Q16`], which doc 09.4 leaves as `k`. It is the expected
//! number of changes per visit we are aiming at, and it is a half, so a little
//! under 40% of refetches find something new. Larger wastes fetches, smaller
//! leaves the corpus stale.
//!
//! # Integers
//!
//! There is no floating point here. Doc 08.5 promises a crawl directory can be
//! copied from an x86 machine to an arm one and resumed, and `f64::ln` is not
//! required to give the same bits on both, so two coordinators could disagree
//! about when a page is due. The logarithm is computed in Q16 fixed point by
//! repeated squaring, which is exact arithmetic on integers and gives the same
//! answer everywhere.

use std::time::Duration;

use crate::types::LedgerRow;

/// The first refresh interval for a URL we have just fetched.
///
/// One fetch is not evidence of anything, so this is a fixed guess rather than
/// anything the estimator produced.
pub const INITIAL_REFRESH: Duration = Duration::from_secs(24 * 60 * 60);

/// The floor on refresh, from doc 09.4.
///
/// Without it a page whose extracted text includes a timestamp changes on every
/// fetch and turns into a hot loop against one origin.
pub const MIN_REFRESH: Duration = Duration::from_secs(5 * 60);

/// The ceiling on refresh, from doc 09.4. A page nothing has changed in months
/// still gets looked at, because the alternative is a corpus that quietly rots.
pub const MAX_REFRESH: Duration = Duration::from_secs(180 * 24 * 60 * 60);

/// What a page above [`CHURN_CEILING`] gets instead of a real interval. We are
/// no longer tracking it, we are sampling it.
pub const LONG_REFRESH: Duration = Duration::from_secs(24 * 60 * 60);

/// The mean time between changes below which a page is not worth tracking,
/// from doc 09.4. One change per ten minutes.
pub const CHURN_CEILING: Duration = Duration::from_secs(10 * 60);

/// The expected number of changes per visit the interval aims at, in Q16.
///
/// A half. See the module docs.
pub const K_Q16: u64 = 1 << 15;

/// ln(2) in Q16.
const LN2_Q16: u64 = 45_426;

/// How much longer than the observation window an interval may be.
const EVIDENCE_FACTOR: u64 = 2;

/// When to look at a URL again, given whether the content actually changed.
///
/// `row` is the row as it was **before** this fetch was applied, so
/// `last_fetch_ms` is the previous fetch and the gap between the two is the
/// interval that was actually served.
#[must_use]
pub fn next_due_after(row: &LedgerRow, changed: bool, now_ms: u64) -> u64 {
    now_ms.saturating_add(refresh_interval_ms(row, changed, now_ms))
}

/// The interval [`next_due_after`] is about to add, on its own.
///
/// Split out because the interval is the thing worth asserting on, and because
/// doc 09.5's refresh classes are a function of it rather than of the wall
/// clock.
#[must_use]
pub fn refresh_interval_ms(row: &LedgerRow, changed: bool, now_ms: u64) -> u64 {
    // A first fetch has nothing to compare against. `changed` is true for it by
    // construction, since the stored hash was zero, and treating that as
    // evidence would put every page on Earth on a twelve hour cycle after one
    // look at it.
    if row.fetch_count == 0 || row.last_fetch_ms == 0 || now_ms <= row.last_fetch_ms {
        return INITIAL_REFRESH.as_millis() as u64;
    }

    // Everything below counts intervals, not fetches. The first fetch opened
    // the window and did not close an interval, and its `change_count` of one
    // is the artefact described above rather than an observation.
    let observed_ms = row.observed_ms_after(now_ms);
    let intervals = u64::from(row.fetch_count);
    let changes = u64::from(row.change_count.saturating_sub(1)) + u64::from(changed);
    let mean_ms = (observed_ms / intervals).max(1);

    let interval_ms = estimate_ms(intervals, changes.min(intervals), mean_ms);

    // A working revalidator makes being wrong cheap: doc 09.4 puts a 304 at
    // about 500 bytes, against a page fetch of a hundred times that. So the
    // interval halves, which is the same as doubling the rate we are willing to
    // pay for. The row is the one from before this fetch, which is the right
    // one to ask: it says the origin sent a revalidator last time.
    let revalidated = row.etag_ref != LedgerRow::NO_ETAG || row.last_mod_ms != 0;
    let interval_ms = if revalidated {
        interval_ms / 2
    } else {
        interval_ms
    };

    // We have watched this page for `observed_ms` and nothing longer than that
    // is a measurement. See the module docs.
    let evidence_ms = observed_ms.saturating_mul(EVIDENCE_FACTOR);
    interval_ms.min(evidence_ms).clamp(
        MIN_REFRESH.as_millis() as u64,
        MAX_REFRESH.as_millis() as u64,
    )
}

/// The estimator itself, on nothing but the three numbers it needs.
///
/// `changes` is at most `intervals` and `mean_ms` is at least one, both
/// enforced by the caller.
fn estimate_ms(intervals: u64, changes: u64, mean_ms: u64) -> u64 {
    // -log((n - x + 0.5) / (n + 0.5)), in halves so it stays in integers, in
    // Q16. Zero when nothing has changed, which is the whole of the no evidence
    // case and is why this is not a division by it below.
    let numerator = 2 * (intervals - changes) + 1;
    let denominator = 2 * intervals + 1;
    let neg_log_q16 = ln_ratio_q16(denominator, numerator);
    if neg_log_q16 == 0 {
        return MAX_REFRESH.as_millis() as u64;
    }

    // lambda_hat is neg_log_q16 / mean_ms, so the churn test is that same
    // comparison with the division moved to the other side.
    let ceiling_ms = CHURN_CEILING.as_millis() as u64;
    if neg_log_q16.saturating_mul(ceiling_ms) >= mean_ms << 16 {
        return LONG_REFRESH.as_millis() as u64;
    }

    // interval = k / lambda_hat, with both k and the log in Q16 so the scale
    // cancels.
    let scaled = u128::from(K_Q16) * u128::from(mean_ms) / u128::from(neg_log_q16);
    u64::try_from(scaled).unwrap_or(u64::MAX)
}

/// `ln(num / den)` in Q16, for `num >= den > 0`, and zero below that.
///
/// Binary logarithm by repeated squaring, then a multiply by ln(2). Every step
/// is integer, so the answer does not depend on the machine. The error is under
/// one part in 65536, which is around three seconds on a day.
const fn ln_ratio_q16(num: u64, den: u64) -> u64 {
    if den == 0 || num <= den {
        return 0;
    }
    // The ratio in Q32, so squaring it fits in a u128 with room to spare.
    let one = 1_u128 << 32;
    let mut ratio = ((num as u128) << 32) / den as u128;
    let mut whole = 0_u64;
    while ratio >= one * 2 {
        ratio >>= 1;
        whole += 1;
    }

    // Squaring the mantissa doubles its logarithm, so the top bit of the result
    // after each squaring is the next bit of the fraction.
    let mut fraction = 0_u64;
    let mut bit = 1_u64 << 15;
    while bit != 0 {
        ratio = (ratio * ratio) >> 32;
        if ratio >= one * 2 {
            fraction |= bit;
            ratio >>= 1;
        }
        bit >>= 1;
    }

    (((whole << 16) | fraction) * LN2_Q16) >> 16
}

/// What a URL is being crawled for, from the table in doc 09.5.
///
/// A class is derived rather than assigned. It is read off the interval the
/// estimator last produced, so it is recomputed on every fetch for free and
/// there is nothing to keep in step: a page that starts changing daily is in
/// the daily class the moment the estimator notices, and a page that goes quiet
/// leaves it the same way.
///
/// A backend may still store it, and the SQLite one does. That is not a second
/// source of truth, it is an index key, and the rule there is that whatever
/// writes the schedule writes the class in the same statement.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[repr(u8)]
pub enum RefreshClass {
    /// Feed and sitemap driven, news front pages. Minutes.
    Realtime,
    /// High lambda on a host worth the bandwidth. One to six hours.
    Hourly,
    /// Active pages on quality hosts. One to three days.
    Daily,
    /// The general web. A week to a month.
    Weekly,
    /// Never observed to change. A month to six.
    Dormant,
    /// Never fetched. The largest share of the fleet, and the one doc 09.5
    /// says to spend down once coverage is respectable.
    #[default]
    Discovery,
}

/// An interval under an hour is realtime.
pub const REALTIME_UNDER_MS: u64 = 60 * 60 * 1000;
/// Under six hours is hourly.
pub const HOURLY_UNDER_MS: u64 = 6 * REALTIME_UNDER_MS;
/// Under three days is daily.
pub const DAILY_UNDER_MS: u64 = 3 * 24 * REALTIME_UNDER_MS;
/// Under thirty days is weekly, and anything slower is dormant.
pub const WEEKLY_UNDER_MS: u64 = 30 * 24 * REALTIME_UNDER_MS;

/// The classes in the order doc 09.5 lists them, for iteration.
pub const CLASSES: [RefreshClass; 6] = [
    RefreshClass::Realtime,
    RefreshClass::Hourly,
    RefreshClass::Daily,
    RefreshClass::Weekly,
    RefreshClass::Dormant,
    RefreshClass::Discovery,
];

impl RefreshClass {
    /// Where this class sits in [`CLASSES`] and in a [`Budget`].
    #[must_use]
    pub const fn index(self) -> usize {
        self.as_u8() as usize
    }

    /// The byte a state backend stores, which is the position in doc 09.5's
    /// table. Stable, because it is in a file.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Back from the stored byte, or `None` for a byte no version of this ever
    /// wrote.
    #[must_use]
    pub const fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Realtime),
            1 => Some(Self::Hourly),
            2 => Some(Self::Daily),
            3 => Some(Self::Weekly),
            4 => Some(Self::Dormant),
            5 => Some(Self::Discovery),
            _ => None,
        }
    }

    /// The class a row is in, from the interval it was last given.
    ///
    /// The boundaries are the table in doc 09.5 read as upper bounds, so the
    /// gaps in it (nothing there is between six hours and a day) fall to the
    /// slower of the two neighbours rather than to nothing.
    #[must_use]
    pub const fn of(fetch_count: u32, interval_ms: u64) -> Self {
        if fetch_count == 0 {
            return Self::Discovery;
        }
        match interval_ms {
            0..REALTIME_UNDER_MS => Self::Realtime,
            REALTIME_UNDER_MS..HOURLY_UNDER_MS => Self::Hourly,
            HOURLY_UNDER_MS..DAILY_UNDER_MS => Self::Daily,
            DAILY_UNDER_MS..WEEKLY_UNDER_MS => Self::Weekly,
            _ => Self::Dormant,
        }
    }

    /// The class of a row, from the schedule on it.
    ///
    /// The interval is the one the estimator produced on the last fetch, which
    /// is the gap between the fetch and the due time it set, rather than how
    /// overdue the row is now.
    #[must_use]
    pub const fn of_row(row: &LedgerRow) -> Self {
        Self::of(
            row.fetch_count,
            row.next_due_ms.saturating_sub(row.last_fetch_ms),
        )
    }

    /// The name doc 09.5 uses, for anything an operator reads.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Realtime => "realtime",
            Self::Hourly => "hourly",
            Self::Daily => "daily",
            Self::Weekly => "weekly",
            Self::Dormant => "dormant",
            Self::Discovery => "discovery",
        }
    }
}

/// How the fleet's capacity is split across the classes, from doc 09.5.
///
/// Percentages, in [`CLASSES`] order, summing to 100. Capacity is allocated to
/// classes rather than left to compete freely because a pure priority queue
/// lets discovery starve refresh or the reverse, depending on which happens to
/// score higher this week.
///
/// A share is a floor and not a cap. A class that has no work due gives its
/// capacity to whoever does, which is what stops a mostly idle realtime class
/// from shrinking every lease batch by 5 percent.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Budget {
    shares: [u8; 6],
}

impl Budget {
    /// The split in doc 09.5.
    pub const DEFAULT: Self = Self {
        shares: [5, 10, 20, 25, 5, 35],
    };

    /// A split of the caller's own, for doc 16's land grab where discovery is
    /// raised and lowered as coverage changes.
    ///
    /// Shares are in [`CLASSES`] order. They do not have to sum to 100: what
    /// matters is their ratio, and a caller that wants to switch a class off
    /// entirely gives it zero.
    #[must_use]
    pub const fn new(shares: [u8; 6]) -> Self {
        Self { shares }
    }

    /// The share of a batch this class is owed, in percent.
    #[must_use]
    pub const fn share(&self, class: RefreshClass) -> u8 {
        self.shares[class.index()]
    }

    /// How many of `max_urls` this class is owed.
    ///
    /// Rounded down, but never to zero for a class with a share, because a
    /// realtime quota of nothing in a batch of sixteen would keep the class off
    /// the wire entirely rather than rationing it.
    #[must_use]
    pub const fn quota(&self, class: RefreshClass, max_urls: u32) -> u32 {
        let total = self.total() as u32;
        let share = self.shares[class.index()] as u32;
        if share == 0 || total == 0 {
            return 0;
        }
        let quota = max_urls.saturating_mul(share) / total;
        if quota == 0 { 1 } else { quota }
    }

    const fn total(&self) -> u16 {
        let mut sum = 0_u16;
        let mut i = 0;
        while i < self.shares.len() {
            sum += self.shares[i] as u16;
            i += 1;
        }
        sum
    }
}

impl Default for Budget {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// One lease batch's worth of budget, as the scan sees it.
///
/// Both backends walk the frontier in priority order and ask this about every
/// row they are otherwise willing to hand out. A row the budget turns down is
/// not dropped: the caller keeps it aside and fills the batch with it if the
/// classes that were owed the capacity turn out not to want it.
#[derive(Clone, Copy, Debug)]
pub struct Quotas {
    quota: [u32; 6],
    taken: [u32; 6],
}

impl Quotas {
    /// The quotas for one batch.
    #[must_use]
    pub fn new(budget: &Budget, max_urls: u32) -> Self {
        let mut quota = [0_u32; 6];
        for class in CLASSES {
            quota[class.index()] = budget.quota(class, max_urls);
        }
        Self {
            quota,
            taken: [0; 6],
        }
    }

    /// Whether this class can take one more, counting it if so.
    pub fn take(&mut self, class: RefreshClass) -> bool {
        let index = class.index();
        if self.taken[index] >= self.quota[index] {
            return false;
        }
        self.taken[index] += 1;
        true
    }

    /// How many of the batch this class has taken.
    #[must_use]
    pub const fn taken(&self, class: RefreshClass) -> u32 {
        self.taken[class.index()]
    }
}
