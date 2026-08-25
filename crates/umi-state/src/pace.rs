//! Doc 07.6's adaptive rate limiter.
//!
//! The asymmetry is the whole design and it is worth stating before the code.
//! Slowing down costs us a few pages and costs the operator nothing. Not
//! slowing down costs the operator their site and costs us the right to be
//! there. So the backoff is fast and the recovery is slow, the configured rate
//! is a ceiling this can only lower, and there is no flag anywhere that raises
//! it.
//!
//! The step is doc 07.6's, exactly:
//!
//! ```text
//! 200 fast          -> 0.9   speed up gently
//! 200 slow (>2s)    -> 1.3   the origin is struggling
//! 429 or 503        -> 4.0   back off hard
//! connection error  -> 2.0
//! 5xx               -> 2.0
//! ```
//!
//! clamped to the host's floor and to a minute, and the point of the 1.3 rung
//! is that latency creep alone backs us off. A crawler that waits for an error
//! before easing up has already made the operator's afternoon worse; the whole
//! reason to watch latency is that it moves first.
//!
//! # Where this runs
//!
//! On the coordinator, inside [`complete`](crate::State::complete), not on the
//! fetcher. A fetcher reports what it observed, which is a fact about the
//! response, and the coordinator decides what to do about it, which is a
//! policy. Doc 07.6 needs a host's politeness timer to live on exactly one
//! machine for fleet wide enforcement to be structural rather than a matter of
//! trust, and that only holds if the number the timer is computed from is
//! computed there too. A fetcher that could hand back its own delay could hand
//! back a small one.
//!
//! # Why integers
//!
//! The multipliers are ratios of integers rather than floats, so that two
//! coordinators on different architectures replaying the same responses reach
//! the same delay to the millisecond. Doc 16's gate 1.2 wants a fetch to be
//! replayable and a scheduler that drifts by a rounding bit is a scheduler
//! that eventually hands out a different crawl.

use crate::types::{FailureKind, FetchResult, HostRow};

/// What one response looked like to the rate limiter.
///
/// Two numbers, both facts about the wire rather than judgements about it.
/// Everything that turns them into a delay is in this module, where all four
/// backends read the same copy of it.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Pace {
    /// Wall time from the start of the request to the last byte of the body.
    ///
    /// `None` when no request was made, which is the case for a lease that
    /// robots.txt or the scope excluded before anything went on the wire.
    /// That is a different thing from a request that came back in under a
    /// millisecond, and the two must not share a value: one is an observation
    /// of an origin and the other is the absence of one.
    pub latency_ms: Option<u32>,
    /// What `Retry-After` asked for, already resolved against the clock by the
    /// caller that had one. `None` when the response carried no such header,
    /// which is the overwhelmingly common case.
    pub retry_after_ms: Option<u32>,
}

/// Above this a 200 is doc 07.6's "slow" and we ease off.
pub const SLOW_MS: u32 = 2000;

/// A response has to come back inside this to count towards the fast floor.
///
/// Tighter than [`SLOW_MS`] on purpose, and the two are answering different
/// questions. `SLOW_MS` decides which way the delay moves next, where the
/// interesting case is an origin whose latency is creeping up under load.
/// This one decides whether a host has earned five requests a second, which is
/// the most this crawler will ever send anybody, and a site that takes a
/// second and a half to answer has not.
pub const FAST_MS: u32 = 500;

/// Consecutive fast responses before the floor drops to
/// [`HostRow::FAST_FLOOR_MS`].
///
/// Fifty is about a minute of clean answers at the default delay. Doc 07.6
/// also asks for the host to be in the top ten thousand by size, and that half
/// is not implemented: the size ranking comes with the pay level domain
/// statistics in milestone 3. Until it exists this is the conservative half of
/// the rule on its own, which errs towards the slower floor.
pub const FAST_STREAK: u16 = 50;

/// How the delay moves, as a ratio, from doc 07.6's table.
///
/// `None` is "this response says nothing about how the origin is holding up".
/// A 404 is a fast, cheap, correct answer about a page that is not there, and
/// a 410 is the same. Reading either as a reason to change our rate would mean
/// a crawl of a site with a lot of dead links quietly speeding up.
///
/// Nothing at all is an observation without a latency, because a latency is
/// what proves a request happened.
///
/// Both matches are exhaustive with no catch all arm. `FetchResult` and
/// `FailureKind` are non exhaustive to the crates above, and this is the crate
/// that defines them, so adding an outcome later has to be a compile error
/// here rather than a variant that silently changes nobody's rate.
const fn factor(result: &FetchResult, pace: Pace) -> Option<(u32, u32)> {
    let Some(latency_ms) = pace.latency_ms else {
        return None;
    };
    let answered = if latency_ms > SLOW_MS {
        Some((13, 10))
    } else {
        Some((9, 10))
    };

    match result {
        // A 304 is a successful conditional request and the cheapest thing an
        // origin ever does for us, so it counts exactly like a 200.
        FetchResult::Fetched { .. } | FetchResult::NotModified { .. } => answered,
        // Doc 13.2's content filter runs after the fetch, so a page dropped
        // for being a PDF was still a request the origin served and still says
        // how the origin is holding up. An exclusion with no request behind it
        // has no latency and never reaches this line.
        FetchResult::Excluded { .. } => answered,
        FetchResult::Failed { status, kind } => match kind {
            // Doc 08.3's `Blocked` is 429 and the challenge pages together,
            // and doc 07.6 puts 429 at 4.0. A challenge is not a rate signal,
            // but backing off from one costs a few pages and doc 05.8's tier
            // ladder is what actually answers it, so the two share a rung.
            FailureKind::Blocked => Some((4, 1)),
            // 503 is doc 07.6's other hard rung: it is an origin saying it is
            // out of capacity, which is the exact thing this limiter exists to
            // stop making worse. Every other 5xx is 2.0.
            FailureKind::ServerError => match status {
                Some(503) => Some((4, 1)),
                _ => Some((2, 1)),
            },
            FailureKind::Connect | FailureKind::Tls | FailureKind::Timeout => Some((2, 1)),
            // A 4xx, an over sized body, or a response we could not parse. All
            // three are answers, and none of them is about load.
            FailureKind::NotFound | FailureKind::Rejected | FailureKind::Malformed => None,
        },
        FetchResult::Gone { .. } => None,
    }
}

impl HostRow {
    /// The smallest gap this host may ever see, from doc 07.6.
    ///
    /// Nothing goes below [`HostRow::FAST_FLOOR_MS`], which is also the fleet
    /// wide cap of five requests a second to any one host, and a host only
    /// reaches it after [`FAST_STREAK`] consecutive fast answers.
    #[must_use]
    pub const fn floor_ms(&self) -> u32 {
        if self.fast_streak >= FAST_STREAK {
            Self::FAST_FLOOR_MS
        } else {
            Self::DEFAULT_FLOOR_MS
        }
    }

    /// Fold one response into this host's rate.
    ///
    /// Updates the adaptive delay, the fast streak, the failure counters and
    /// the politeness timer, in that order and all from the same observation,
    /// so a backend cannot apply half of it. A result that says nothing about
    /// load changes nothing at all, including the counters, because a lease
    /// that robots excluded never became a request.
    ///
    /// `finished_ms` is when the response landed. The timer only ever moves
    /// forward: [`lease`](crate::State::lease) has already spaced out whatever
    /// else it handed out for this host, and pulling the timer back to suit
    /// one completion would undo that.
    ///
    /// Returns whether anything changed, which is how a backend knows not to
    /// write. It matters more than it looks: a crawl of a disallowed site
    /// would otherwise create a host record per excluded url and doc 08.4's
    /// host count would start describing hosts we have never spoken to.
    pub fn observe(&mut self, result: &FetchResult, pace: Pace, finished_ms: u64) -> bool {
        let Some((numerator, denominator)) = factor(result, pace) else {
            return false;
        };

        let failed = matches!(result, FetchResult::Failed { .. });
        self.fetches = self.fetches.saturating_add(1);
        if failed {
            self.failures = self.failures.saturating_add(1);
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        } else {
            self.consecutive_failures = 0;
        }

        // The streak is what earns the 200 ms floor, so it takes a run of
        // clean fast answers to build and one bad answer to lose. Same
        // asymmetry as the delay itself.
        if !failed && pace.latency_ms.is_some_and(|ms| ms <= FAST_MS) {
            self.fast_streak = self.fast_streak.saturating_add(1);
        } else {
            self.fast_streak = 0;
        }

        // Widened to 64 bits before the multiply, because 60000 * 4 does not
        // fit the intent of a u32 clamp applied afterwards: it fits, but
        // 60000 * 13 with a larger rung added later would not, and a rate
        // limiter that wraps to a 3 ms delay is the worst bug in this file.
        let scaled = u64::from(self.adaptive_delay_ms) * u64::from(numerator)
            / u64::from(denominator.max(1));
        let next = u32::try_from(scaled).unwrap_or(Self::MAX_DELAY_MS);
        // The floor is read after the streak moves, so the response that
        // completes a streak is the one that unlocks the lower floor.
        self.adaptive_delay_ms = next.clamp(self.floor_ms(), Self::MAX_DELAY_MS);

        // `Retry-After` is honoured exactly, and taken as a minimum rather
        // than as the answer. An origin asking for 1 second while our own
        // delay is at 8 gets 8: it asked us to wait at least that long, and
        // waiting longer than an origin asked has never annoyed anybody.
        let own = self.adaptive_delay_ms.max(self.crawl_delay_ms.unwrap_or(0));
        let wait = u64::from(own.max(pace.retry_after_ms.unwrap_or(0)));
        self.next_allowed_ms = self.next_allowed_ms.max(finished_ms.saturating_add(wait));
        true
    }
}
