//! The values that cross the [`State`](crate::State) boundary.
//!
//! Every one of these is a plain data type with no behaviour beyond a few
//! constructors, because they are the wire between the scheduler and four
//! backends that share no code. Anything with a policy in it belongs on one
//! side or the other, not in the middle. The two exceptions are
//! [`next_due_after`](crate::next_due_after) and
//! [`retry_after_ms`](crate::retry_after_ms), which are here so that four
//! backends cannot quietly disagree about when a URL comes back.

use std::time::Duration;

use umi_types::{Digest, FetcherId, HostId, PldId, RowKey, Tier, Ulid, UrlKeyFull};

use crate::freshness::Budget;
use crate::pace::Pace;

// A fetcher needs this to build a conditional request and needs nothing else
// from the state layer, so it lives in umi-types and is re-exported here. Doc
// 04.5 is the reason: a community fetcher implements the protocol, not the
// scheduler, and making it link the state crate for one two field struct
// would be the wrong shape.
pub use umi_types::Revalidator;

/// A URL's fixed point score, from `docs/spec/09-frontier-and-freshness.md`
/// section 9.2.
///
/// The convention is that the whole `u16` range maps onto the unit interval,
/// so 0 is the lowest score expressible and [`Priority::MAX`] is the highest.
/// Scores are recomputed at lease time rather than maintained, so the value
/// stored on a row is the last one computed and is a hint, not a truth.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Priority(u16);

impl Priority {
    /// The floor. A row here is crawled only when nothing else is due.
    pub const MIN: Self = Self(0);
    /// What a candidate gets when nothing better is known, which is most of
    /// them at admission time.
    pub const DEFAULT: Self = Self(u16::MAX / 2);
    /// The ceiling, reserved in doc 09.5 for feed entries.
    pub const MAX: Self = Self(u16::MAX);

    /// Wrap a raw fixed point score.
    #[must_use]
    pub const fn from_raw(raw: u16) -> Self {
        Self(raw)
    }

    /// The raw fixed point score, which is what a ledger row stores.
    #[must_use]
    pub const fn raw(self) -> u16 {
        self.0
    }

    /// Build a score from a unit interval value, clamping anything outside it.
    #[must_use]
    pub fn from_unit(value: f32) -> Self {
        Self((value.clamp(0.0, 1.0) * f32::from(u16::MAX)) as u16)
    }
}

/// The identity of one outstanding unit of work.
///
/// Unique within one store's lifetime and monotonic, so a larger id is a later
/// lease. It is deliberately not derived from the URL: the same URL leased
/// twice gets two ids, which is how a late completion from a lease that
/// already expired can be told apart from the one that replaced it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct LeaseId(u64);

impl LeaseId {
    /// Wrap a raw id, for a backend reading one back off disk.
    #[must_use]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// The raw id.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// How a candidate URL came to our attention.
///
/// This is what decides between the frontier and the holding pen in
/// `docs/spec/06-trust-and-verification.md` section 6.2 layer 7. The judgement
/// is the caller's, because reputation lives in the coordinator and not in the
/// store, and the store only needs the answer.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Discovery {
    /// Fetched by this coordinator, or extracted by a fetcher above the 0.6
    /// reputation gate. Goes straight to the frontier.
    #[default]
    Trusted,
    /// Extracted by a fetcher below the gate. Goes to the holding pen keyed by
    /// that fetcher, and graduates under the rules in doc 06.2.
    Unverified(FetcherId),
    /// A seed, from `umi seed` or from a sitemap we fetched ourselves. Skips
    /// the depth ceiling, since depth is measured from the seeds.
    Seed,
}

/// A URL offered to the frontier, which will usually already be known.
///
/// The URL text is borrowed rather than owned because well over 95 percent of
/// candidates are dropped as already seen, per doc 08.1, and allocating a
/// `String` for each of 12500 per second in order to throw it away is the
/// single easiest way to lose the admission budget.
#[derive(Clone, Copy, Debug)]
pub struct Candidate<'a> {
    /// The keys, which the caller derives with [`RowKey::for_url`] so that
    /// canonicalisation has definitely happened.
    pub key: RowKey,
    /// The canonical URL. Must be the same string the key was derived from.
    pub url: &'a str,
    /// Link distance from the nearest seed.
    pub depth: u8,
    /// The score to store on a new row. Ignored for a URL already known.
    pub priority: Priority,
    /// When the link was seen, in milliseconds since the Unix epoch.
    pub discovered_ms: u64,
    /// Whether this goes to the frontier or the holding pen.
    pub discovery: Discovery,
}

impl<'a> Candidate<'a> {
    /// A candidate with the defaults: trusted, mid priority, depth zero.
    ///
    /// # Errors
    ///
    /// Returns [`CanonError`](umi_types::CanonError) when the URL is not a
    /// crawlable http(s) URL.
    pub fn new(url: &'a str, discovered_ms: u64) -> Result<Self, umi_types::CanonError> {
        Ok(Self {
            key: RowKey::for_url(url, None)?,
            url,
            depth: 0,
            priority: Priority::DEFAULT,
            discovered_ms,
            discovery: Discovery::Trusted,
        })
    }
}

/// What a batch of candidates turned into.
///
/// The four dispositions are exclusive and they sum to the batch length, which
/// [`AdmitReport::total`] exists to let a caller assert. `shard_misses` is the
/// number the operator watches, per doc 08.4: it is the state layer's cache
/// miss rate, and it is the difference between a crawl running at rate and a
/// crawl waiting on object storage.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct AdmitReport {
    /// Already in the seen set, so dropped.
    pub seen: u32,
    /// New, and now pending in the frontier.
    pub admitted: u32,
    /// New, but from an unverified fetcher, so parked in the holding pen.
    pub held: u32,
    /// Rejected by the block list, by the backend's configured scope, or
    /// because the ledger already records the URL as excluded.
    pub excluded: u32,
    /// Pay level domains whose shard had to be warmed from cold storage
    /// during this call. Zero on a backend that does not shard.
    pub shard_misses: u32,
}

impl AdmitReport {
    /// The number of candidates accounted for, which must equal the batch
    /// length. `shard_misses` is not part of it: it counts domains, not URLs.
    #[must_use]
    pub const fn total(&self) -> u32 {
        self.seen + self.admitted + self.held + self.excluded
    }
}

/// What the caller wants leased, and what it is able to do with it.
///
/// `now_ms` is passed in rather than read from the clock inside the backend.
/// That is not a convenience: politeness, lease expiry and refresh scheduling
/// are all functions of time, and a store that reads its own clock cannot be
/// replayed, which is what gate 1.2 in doc 16 asks for.
#[derive(Clone, Copy, Debug)]
pub struct LeaseRequest<'a> {
    /// Who the work is for. Recorded on the lease so a completion can be
    /// attributed, and checked against the tier rules in doc 05.7.
    pub fetcher: FetcherId,
    /// The caller's notion of now, in milliseconds since the Unix epoch.
    pub now_ms: u64,
    /// Ceiling on the number of leases returned. A backend may return fewer
    /// for any reason and returning zero is not an error.
    pub max_urls: u32,
    /// Ceiling per host within this call. Doc 07.6 already guarantees one
    /// request in flight per host, so this bounds the queue a fetcher holds,
    /// not the concurrency it may use.
    pub max_per_host: u32,
    /// The most expensive tier this fetcher will run. A URL whose host wants
    /// a more expensive tier is not offered.
    pub max_tier: Tier,
    /// How long the leases are good for. After this the coordinator may hand
    /// the same URLs to someone else.
    pub lease_for: Duration,
    /// Restrict to these pay level domains. Empty means no restriction.
    ///
    /// This is how doc 09.4 gets its locality: the scheduler knows which
    /// shards are resident and asks only for those, rather than asking for
    /// anything and paying an object GET in the middle of the loop.
    pub plds: &'a [PldId],
    /// How this batch is split across doc 09.5's refresh classes.
    ///
    /// A share is a floor rather than a cap, so a batch is only smaller than
    /// `max_urls` when the frontier really has nothing else due. Set every
    /// share equal to turn the split off and go back to pure priority order.
    pub budget: Budget,
}

impl<'a> LeaseRequest<'a> {
    /// A request with the shape a single fetcher usually wants: one batch,
    /// no domain restriction, a one minute lease.
    #[must_use]
    pub const fn new(fetcher: FetcherId, now_ms: u64, max_urls: u32) -> Self {
        Self {
            fetcher,
            now_ms,
            max_urls,
            max_per_host: 8,
            max_tier: Tier::Plain,
            lease_for: Duration::from_secs(60),
            plds: &[],
            budget: Budget::DEFAULT,
        }
    }
}

/// One URL, handed out and marked in flight.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Lease {
    /// The handle to quote back to [`complete`](crate::State::complete) or
    /// [`release`](crate::State::release).
    pub id: LeaseId,
    /// The keys, so the caller does not have to re-derive them.
    pub key: RowKey,
    /// The canonical URL to fetch.
    pub url: String,
    /// Link distance from the nearest seed, for the depth ceiling.
    pub depth: u8,
    /// The score this URL was chosen on.
    pub priority: Priority,
    /// How many times this URL has been fetched before, so a retry can be
    /// told from a first look.
    pub attempt: u32,
    /// The tier to start at, from the host's policy in doc 05.8. Never above
    /// the request's `max_tier`.
    pub tier: Tier,
    /// The earliest the fetcher may send the request, from the host's
    /// politeness timer. Usually now, but a batch covering one host carries
    /// staggered times.
    pub not_before_ms: u64,
    /// When the coordinator stops waiting.
    pub expires_ms: u64,
    /// What to put in `If-None-Match` and `If-Modified-Since`, when we have
    /// fetched this URL before and the host is not a known liar about it.
    pub revalidate: Option<Revalidator>,
}

/// What happened to one leased URL.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FetchOutcome {
    /// The lease this answers.
    pub lease: LeaseId,
    /// The URL it was for. Carried explicitly so a backend can find the row
    /// without keeping every outstanding lease in memory.
    pub key: RowKey,
    /// When the fetch finished, in milliseconds since the Unix epoch. Drives
    /// `last_fetch_ms` and the next due time.
    pub finished_ms: u64,
    /// The tier that actually produced the answer, which may be above the
    /// tier the lease suggested if the fetcher escalated.
    pub tier_used: Tier,
    /// The answer.
    pub result: FetchResult,
    /// What the response looked like to doc 07.6's rate limiter.
    ///
    /// Facts, not a decision: how long it took and what `Retry-After` said.
    /// The delay those turn into is computed by
    /// [`HostRow::observe`](crate::HostRow::observe) on the coordinator, so a
    /// fetcher cannot report itself a faster rate.
    pub pace: Pace,
}

/// The five things a fetch can conclude.
///
/// These are outcomes, not HTTP statuses. A 404 and a connection refused are
/// both failures as far as scheduling is concerned, and a 410 is the only
/// status that means stop asking.
#[derive(Clone, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum FetchResult {
    /// A body arrived and was extracted. `content_hash` is over the extracted
    /// text, not the response bytes, so a changed advertisement is not a
    /// changed page.
    Fetched {
        /// The HTTP status, which is a 2xx to be here.
        status: u16,
        /// Truncated blake3 of the extracted text, per doc 08.3.
        content_hash: [u8; 8],
        /// What to send next time.
        revalidate: Revalidator,
    },
    /// A conditional request held. No body, no extraction, and the content
    /// hash on the row stays as it was.
    NotModified {
        /// The status, which is 304.
        status: u16,
        /// A refreshed revalidator, if the origin sent one.
        revalidate: Revalidator,
    },
    /// It did not work and it is worth trying again later.
    Failed {
        /// The status, if we got far enough to have one.
        status: Option<u16>,
        /// What went wrong.
        kind: FailureKind,
    },
    /// The origin says this resource is gone for good, which is a 410 and
    /// nothing else. Never scheduled again.
    Gone {
        /// The status, which is 410.
        status: u16,
    },
    /// We are not allowed to have it, or we do not want it. Recorded rather
    /// than dropped, so that admitting the same URL again is free and so that
    /// a robots change can be found later and acted on.
    Excluded {
        /// Why.
        reason: ExcludeReason,
    },
}

/// The failure classes that scheduling treats differently.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum FailureKind {
    /// DNS or TCP did not get there.
    Connect,
    /// The TLS handshake failed. Distinct from `Connect` because doc 05.8
    /// treats a handshake failure that succeeds under a browser profile as a
    /// block signal rather than as an outage.
    Tls,
    /// The origin took too long.
    Timeout,
    /// A 5xx that is not a challenge.
    ServerError,
    /// A 4xx that is not 410. The resource may come back.
    NotFound,
    /// A bot management challenge, per the block signals in doc 05.8. Backs
    /// the host off rather than the URL.
    Blocked,
    /// The body was over the cap, or was not something we extract.
    Rejected,
    /// The response was structurally broken.
    Malformed,
}

/// Why a URL will not be fetched.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum ExcludeReason {
    /// robots.txt disallows it for us.
    Robots,
    /// The domain is on the block list from doc 07.7.
    BlockList,
    /// Outside the configured focused crawl scope.
    OutOfScope,
    /// Past the depth ceiling.
    TooDeep,
    /// A content type we do not index.
    ContentType,
}

/// Why a lease is coming back without an answer.
///
/// All four reschedule the URL immediately and none of them touch
/// `fail_streak`, because a fetcher going away says nothing about the URL.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[non_exhaustive]
pub enum NackReason {
    /// The deadline passed and the coordinator took the work back.
    Expired,
    /// The fetcher disconnected or stopped answering.
    FetcherGone,
    /// The fetcher will not do this work, usually because the host wants a
    /// tier it cannot run.
    Refused,
    /// An orderly drain, from `umi fetch` shutting down or the coordinator
    /// stopping.
    Shutdown,
}

/// What the scheduler knows about a URL. The state machine is small on
/// purpose: `Pending` is the only state the frontier ever looks at.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
#[repr(u8)]
pub enum UrlState {
    /// Known, due at `next_due_ms`, waiting for a fetcher.
    #[default]
    Pending = 0,
    /// Fetched at least once. Due again at `next_due_ms` for refresh.
    Fetched = 1,
    /// The last attempt failed. Due again after the backoff in
    /// [`retry_after_ms`](crate::retry_after_ms).
    Failed = 2,
    /// A 410. Terminal.
    Gone = 3,
    /// Robots, block list or scope says no. Terminal until the reason
    /// changes, which is why the reason is not stored: rechecking is cheap
    /// and a stale reason would be worse than none.
    Excluded = 4,
}

impl UrlState {
    /// Whether the frontier will ever offer this row again.
    #[must_use]
    pub const fn is_schedulable(self) -> bool {
        matches!(self, Self::Pending | Self::Fetched | Self::Failed)
    }

    /// Recover a state from the byte a stored row holds.
    #[must_use]
    pub const fn from_u8(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Pending),
            1 => Some(Self::Fetched),
            2 => Some(Self::Failed),
            3 => Some(Self::Gone),
            4 => Some(Self::Excluded),
            _ => None,
        }
    }
}

/// One row of the ledger from doc 08.3.
///
/// The trait never returns one of these, and that is deliberate: there is no
/// `get(url)`. It is public because it is the schema all four backends store
/// and the schema [`checkpoint`](crate::State::checkpoint) exports for DuckDB,
/// and three independent definitions of it would drift.
///
/// The URL text is not here. Doc 08.3 does not list it, the scheduler never
/// reads it, and at 100 billion rows it would dominate the "under 20 bytes per
/// known URL" target in doc 01. Backends keep the text in a separate per shard
/// pool and only join it in when building a [`Lease`].
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct LedgerRow {
    /// The 128 bit fingerprint, which is what makes a [`UrlKey`](umi_types::UrlKey)
    /// collision detectable rather than silent.
    pub url_key_full: UrlKeyFull,
    /// The host, for politeness and for the host record.
    pub host_id: HostId,
    /// Link distance from the nearest seed.
    pub depth: u8,
    /// The last score computed for this row.
    pub priority: Priority,
    /// Where it is in the state machine.
    pub state: UrlState,
    /// When to fetch or refetch, in milliseconds since the Unix epoch.
    pub next_due_ms: u64,
    /// When we last tried.
    pub last_fetch_ms: u64,
    /// The last time `content_hash` actually changed, which is what the
    /// refresh estimator in doc 12 runs on.
    pub last_change_ms: u64,
    /// How many times we have fetched it.
    pub fetch_count: u32,
    /// How many of those changed anything.
    pub change_count: u32,
    /// Truncated blake3 of the extracted text.
    pub content_hash: [u8; 8],
    /// Index into the shard's interned ETag pool, or
    /// [`LedgerRow::NO_ETAG`] for none. Interned because ETags repeat heavily
    /// within a site and storing them inline would double the row.
    pub etag_ref: u32,
    /// `Last-Modified` in milliseconds since the Unix epoch, or zero.
    pub last_mod_ms: u64,
    /// The last HTTP status seen.
    pub status: u16,
    /// The tier that produced the last answer.
    pub tier_used: Tier,
    /// Consecutive failures, which drives [`retry_after_ms`](crate::retry_after_ms).
    pub fail_streak: u8,
    /// How long we have watched this URL for, in seconds, summed over the
    /// intervals we actually served rather than measured from the first fetch.
    ///
    /// This is the denominator of the change rate estimator in
    /// [`freshness`](crate::freshness) and it is the only field there that is
    /// not already in doc 08.3. Seconds because four bytes of them is 136
    /// years, and a sum because a URL that was idle for a month while the crawl
    /// was stopped was not being watched during it.
    pub observed_secs: u32,
}

impl LedgerRow {
    /// The `etag_ref` of a row with no interned ETag.
    pub const NO_ETAG: u32 = u32::MAX;

    /// A fresh row for a URL that has only just been admitted.
    #[must_use]
    pub fn pending(key: &RowKey, url: &str, depth: u8, priority: Priority, due_ms: u64) -> Self {
        Self {
            url_key_full: UrlKeyFull::derive(url.as_bytes()),
            host_id: key.host,
            depth,
            priority,
            state: UrlState::Pending,
            next_due_ms: due_ms,
            etag_ref: Self::NO_ETAG,
            ..Self::default()
        }
    }

    /// The observation window this row will have once a fetch that finished at
    /// `now_ms` is applied to it, in milliseconds.
    ///
    /// A failed fetch moves `last_fetch_ms` without adding anything here, so
    /// after a run of failures the window is shorter than the wall clock gap
    /// the content hash actually spans. That biases the estimate towards
    /// fetching too often rather than too rarely, and the floor bounds how far
    /// it can go.
    #[must_use]
    pub const fn observed_ms_after(&self, now_ms: u64) -> u64 {
        // A first fetch has no previous look to measure from, and taking the
        // difference against a `last_fetch_ms` of zero would charge this row
        // with every second since 1970.
        let served = if self.last_fetch_ms == 0 {
            0
        } else {
            now_ms.saturating_sub(self.last_fetch_ms)
        };
        (self.observed_secs as u64)
            .saturating_mul(1000)
            .saturating_add(served)
    }

    /// The same window as a seconds counter to store back on the row.
    #[must_use]
    pub const fn observed_secs_after(&self, now_ms: u64) -> u32 {
        let ms = self.observed_ms_after(now_ms);
        let secs = ms / 1000;
        if secs > u32::MAX as u64 {
            u32::MAX
        } else {
            secs as u32
        }
    }

    /// Whether this row is due at `now_ms` and in a state that can be leased.
    #[must_use]
    pub const fn is_due(&self, now_ms: u64) -> bool {
        self.state.is_schedulable() && self.next_due_ms <= now_ms
    }
}

/// What we know about one host, from doc 08.3.
///
/// Small, and there are only about 50 million of them fleet wide, so this is
/// the one table that fits in memory even on server1.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct HostRow {
    /// The host this describes.
    pub host: HostId,
    /// Its pay level domain, so the per domain cap in doc 07.6 can be applied
    /// without reparsing.
    pub pld: PldId,
    /// The robots.txt cache entry, if we have fetched one.
    pub robots: Option<RobotsRef>,
    /// The current adaptive delay in milliseconds, from doc 07.6. Starts at
    /// 1000 and moves with observed behaviour.
    pub adaptive_delay_ms: u32,
    /// `Crawl-delay` from robots.txt, already clamped by `umi-robots`.
    pub crawl_delay_ms: Option<u32>,
    /// The earliest the next request to this host may be sent. This is the
    /// politeness timer, and it living on exactly one coordinator is what
    /// makes fleet wide rate limiting structural rather than a matter of
    /// trust.
    pub next_allowed_ms: u64,
    /// The tier ladder state for this host.
    pub tier: TierPolicy,
    /// The AIPREF `Content-Usage` value, propagated to published rows and not
    /// acted on, per doc 07.5.
    pub content_usage: Option<String>,
    /// Sitemap URLs, from robots.txt or from the well known paths.
    pub sitemaps: Vec<String>,
    /// Total fetches against this host.
    pub fetches: u64,
    /// How many of those failed.
    pub failures: u64,
    /// Consecutive failures right now, which drives the host level backoff.
    pub consecutive_failures: u16,
    /// Consecutive fast successful responses, which is what earns a host the
    /// 200 ms floor in [`floor_ms`](HostRow::floor_ms). Reset by anything that
    /// is not both fast and successful.
    pub fast_streak: u16,
    /// Blocked by an operator under doc 07.7. Never crawled, never admitted,
    /// and never silently reversed.
    pub blocked: bool,
    /// Blocked us for 30 consecutive days at the top of its tier ladder, per
    /// doc 05.8, so it is out of the frontier and probed monthly at T1.
    pub refusing: bool,
}

impl HostRow {
    /// The starting adaptive delay from doc 07.6.
    pub const INITIAL_DELAY_MS: u32 = 1000;
    /// The floor for a host we have not observed to be fast.
    pub const DEFAULT_FLOOR_MS: u32 = 1000;
    /// The floor for a large host with sustained fast responses. Nothing goes
    /// below this.
    pub const FAST_FLOOR_MS: u32 = 200;
    /// The ceiling on the adaptive delay.
    pub const MAX_DELAY_MS: u32 = 60_000;

    /// A host we have just heard of.
    #[must_use]
    pub fn new(host: HostId, pld: PldId) -> Self {
        Self {
            host,
            pld,
            adaptive_delay_ms: Self::INITIAL_DELAY_MS,
            ..Self::default()
        }
    }

    /// The gap to leave before the next request, which is the larger of the
    /// published `Crawl-delay` and our own adaptive delay.
    #[must_use]
    pub fn delay(&self) -> Duration {
        let ms = self.adaptive_delay_ms.max(self.crawl_delay_ms.unwrap_or(0));
        Duration::from_millis(u64::from(ms))
    }

    /// Whether this host may be fetched at all right now.
    #[must_use]
    pub fn is_fetchable(&self, now_ms: u64) -> bool {
        !self.blocked && !self.refusing && self.next_allowed_ms <= now_ms
    }
}

/// Where a host's robots.txt lives and how long we may believe it.
///
/// A digest rather than the parsed rules, because parsed rules belong to
/// `umi-robots` and a state layer that knew how to parse robots.txt would be
/// a state layer that has to be redeployed when the parser changes. The bodies
/// are content addressed, so many hosts sharing one hosting template share one
/// entry.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct RobotsRef {
    /// blake3 of the robots.txt body, or of the empty string when the fetch
    /// produced no body.
    pub digest: Digest,
    /// When we fetched it.
    pub fetched_ms: u64,
    /// When it stops being usable and has to be refetched.
    pub expires_ms: u64,
    /// Whether the fetch that produced this succeeded. A 5xx means disallow
    /// everything under RFC 9309 section 2.3.1, and that is a different
    /// situation from an empty file, which allows everything.
    pub authoritative: bool,
}

/// The per host tier ladder state from doc 05.8.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct TierPolicy {
    /// Where a fetch starts.
    pub preferred: Tier,
    /// Where it stops. Starts at [`Tier::Emulated`] for the public crawl and
    /// only reaches [`Tier::Rendered`] once a T3 fetch has actually produced
    /// meaningfully more text than T1 did.
    pub max: Tier,
    /// The cheapest tier that has worked recently.
    pub last_success: Tier,
    /// Block signals in a row, which drives the exponential host backoff.
    pub consecutive_blocks: u16,
    /// When we last tried a cheaper tier. De-escalation is the part crawlers
    /// forget, and without it one bad afternoon of bot management tuning pins
    /// a domain to browser rendering forever.
    pub last_probe_down_ms: u64,
    /// T1 returned a shell and T3 did not.
    pub render_required: bool,
    /// The origin sends a `Last-Modified` that moves without the content
    /// moving, so it is not worth conditioning on.
    pub weak_revalidator: bool,
    /// The origin sends a revalidator and then ignores it, so conditional
    /// requests cost a round trip and save nothing.
    pub lying_revalidator: bool,
}

impl TierPolicy {
    /// The ladder a host starts on: plain, escalating no further than
    /// emulated without evidence.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            preferred: Tier::Plain,
            max: Tier::Emulated,
            last_success: Tier::Plain,
            consecutive_blocks: 0,
            last_probe_down_ms: 0,
            render_required: false,
            weak_revalidator: false,
            lying_revalidator: false,
        }
    }

    /// The tier to start a fetch at, given what the fetcher can run.
    #[must_use]
    pub fn start_at(&self, fetcher_max: Tier) -> Tier {
        self.preferred.min(self.max).min(fetcher_max)
    }

    /// Whether this host is worth offering to a fetcher limited to
    /// `fetcher_max`. A host that only answers above what the fetcher can do
    /// is a wasted lease.
    #[must_use]
    pub fn reachable_by(&self, fetcher_max: Tier) -> bool {
        self.preferred.min(self.max) <= fetcher_max
    }
}

/// What one call to [`evict`](crate::State::evict) did.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct EvictReport {
    /// Shards sealed, uploaded and dropped locally.
    pub evicted: u32,
    /// Asked for but not resident, which is not an error.
    pub not_resident: u32,
    /// Kept because the domain still has leases in flight.
    pub in_use: u32,
    /// Bytes written to cold storage in the process.
    pub bytes_written: u64,
}

/// A consistent point in time snapshot, for publishing and for analytics.
///
/// The snapshot is what `umi checkpoint --format duckdb` attaches to and what
/// doc 15's dashboard queries. It is never the live store: DuckDB is not built
/// for 12500 point writes per second and pointing it at the hot path would be
/// a category error.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Checkpoint {
    /// Monotonic within one store. Later checkpoint, larger sequence, with no
    /// gaps promised.
    pub sequence: u64,
    /// When it was taken, in milliseconds since the Unix epoch.
    pub taken_ms: u64,
    /// The canonicalisation the keys in it were derived under. A consumer
    /// that reads a checkpoint under a different `canon/N` is looking at keys
    /// it cannot join against.
    pub canon_version: String,
    /// Where the snapshot is, when it is a file. `None` for a backend where
    /// the snapshot is a transaction rather than an artefact.
    pub path: Option<std::path::PathBuf>,
    /// blake3 of the snapshot bytes, when there are any.
    pub digest: Option<Digest>,
    /// The counters as of the snapshot, so a consumer does not have to scan
    /// it to know how big it is.
    pub stats: StateStats,
}

/// The counters an operator watches.
///
/// All of them are point in time and none of them are promised to be exact
/// under concurrent admission, because making them exact would mean a lock
/// across the hot path. They are for dashboards and for capacity, not for
/// accounting.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct StateStats {
    /// URLs in the seen set. This is the number that decides whether the
    /// sqlite backend is still the right one, per doc 08.5.
    pub urls_seen: u64,
    /// Rows waiting to be fetched for the first time.
    pub urls_pending: u64,
    /// Rows fetched at least once.
    pub urls_fetched: u64,
    /// Rows whose last attempt failed.
    pub urls_failed: u64,
    /// Rows that are terminally gone.
    pub urls_gone: u64,
    /// Rows excluded by robots, the block list or scope.
    pub urls_excluded: u64,
    /// Entries in the holding pen.
    pub urls_held: u64,
    /// Host records.
    pub hosts: u64,
    /// Leases handed out and not yet answered.
    pub leases_in_flight: u64,
    /// Pay level domains whose shard is local right now.
    pub resident_plds: u64,
    /// Shard warms since the store was opened. The rate of change of this is
    /// what tells an operator the resident set is too small.
    pub shard_misses: u64,
    /// What the store occupies locally, as best the backend can tell.
    pub bytes_on_disk: u64,
}

/// Which of doc 10's three streams a segment holds.
///
/// This is `StreamKind` from umi-file, written out again rather than imported.
/// The state layer does not depend on the file format and should not: doc 03
/// puts them side by side, the frontier has no business knowing how a shoal is
/// encoded, and a state backend that linked the writer would drag zstd into
/// every dashboard tool that opens the sqlite file. The discriminants match on
/// purpose, so the two can be converted with a three arm match at the one call
/// site that has both crates in scope, and a test in umi-crawl asserts they
/// still line up.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(u8)]
pub enum Stream {
    /// Crawled pages.
    Pages = 1,
    /// Doc 04 delivery receipts.
    Receipts = 2,
    /// Fetched robots.txt, raw and parsed.
    Robots = 3,
}

impl Stream {
    /// The value a backend stores.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Read one back, for a backend loading a row.
    ///
    /// Returns `None` for anything else, which a backend turns into
    /// [`StateError::Corrupt`](crate::StateError::Corrupt) rather than
    /// guessing. A row whose stream we cannot name is a row we cannot decide
    /// the GC rule on.
    #[must_use]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Pages),
            2 => Some(Self::Receipts),
            3 => Some(Self::Robots),
            _ => None,
        }
    }
}

/// Where a segment ended up on Hugging Face.
///
/// The three fields doc 08.3 lists as `remote_repo`, `remote_path` and
/// `remote_digest` are one value here rather than three nullable columns, and
/// that is the point. Doc 08.3 requires them to move from null to set in a
/// single write, so that a crash can leave a segment unpublished but can never
/// leave it half published in a way that satisfies doc 12.7's fourth
/// condition. Three separate `Option`s would make "repo set, digest still
/// null" a state the type system permits and a reviewer has to remember is
/// forbidden. One `Option` makes it unrepresentable.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RemoteCopy {
    /// The dataset repository, as `owner/name`.
    pub repo: String,
    /// The path inside it, which is doc 12.4's
    /// `data/<YYYYMMDD>/<ULID>.parquet`.
    pub path: String,
    /// blake3 of what was read back from the hub, not of what was sent.
    ///
    /// Doc 12.2 step 5 says the remote copy is verified independently, and an
    /// upload's own echo of the digest it was given is not independent. This
    /// field existing at all is the record that a read happened.
    pub digest: Digest,
}

/// One sealed segment, from doc 08.3.
///
/// A row is written when the segment is sealed and updated once when it is
/// published. It is never deleted, because the row outliving the file is
/// exactly what lets an operator answer "where did that segment go" after the
/// local copy is gone. A coordinator seals about a thousand a day at 128 MB
/// each, so a year of history is well under 100 MB and there is no reason to
/// prune.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SegmentRow {
    /// The segment's ULID, which is also its file name and its key.
    pub id: Ulid,
    /// Which stream it holds.
    pub stream: Stream,
    /// Where the `.umi` file was written. Still meaningful after the file is
    /// deleted, because it is how a reconciliation pass says what is missing.
    pub local_path: String,
    /// When the segment was sealed, in milliseconds since the Unix epoch.
    pub sealed_at_ms: u64,
    /// How many rows it holds.
    pub rows: u64,
    /// How many bytes the sealed file is.
    pub bytes: u64,
    /// blake3 of the sealed `.umi` file.
    pub local_digest: Digest,
    /// Where it is on the hub, once it is anywhere.
    pub remote: Option<RemoteCopy>,
    /// The `YYYYMMDD` of the manifest that lists it, once one does.
    pub manifest_day: Option<u32>,
    /// When the local file was deleted under doc 12.7, if it has been.
    pub deleted_at_ms: Option<u64>,
}

impl SegmentRow {
    /// Whether doc 12.7's fourth condition holds for this segment.
    ///
    /// Only the fourth. The other three are checked against the hub and the
    /// manifest by umi-publish's `gc::clear`, and this deliberately does not
    /// try to be the whole rule: a method on a state row that returned "safe
    /// to delete" would be a second place the rule lives, and doc 12.7 is
    /// emphatic that there is one.
    #[must_use]
    pub const fn ledger_complete(&self) -> bool {
        self.remote.is_some() && self.manifest_day.is_some()
    }

    /// Whether the local file should still be on disk.
    #[must_use]
    pub const fn local(&self) -> bool {
        self.deleted_at_ms.is_none()
    }
}

/// Which segments a caller wants back.
///
/// Three variants because there are three callers, and each one is a scan a
/// backend can answer from an index rather than by loading the table and
/// filtering. The publisher asks for [`Unpublished`](SegmentQuery::Unpublished)
/// when it starts, the GC pass asks for
/// [`Collectable`](SegmentQuery::Collectable), and doc 12.8's reconciliation
/// asks for a window.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SegmentQuery {
    /// Sealed and not on the hub. What the publisher picks up after a restart,
    /// including whatever was in flight when it stopped.
    Unpublished,
    /// On the hub, in a manifest, and the local file is still there. The set
    /// doc 12.7's rule is evaluated over. A segment is in this set the moment
    /// it is publishable and leaves it when the file is deleted, so an empty
    /// answer means there is nothing to collect and not that collection is
    /// stuck.
    Collectable,
    /// Everything sealed in a half open millisecond range, for doc 12.8.
    SealedBetween {
        /// Inclusive.
        from_ms: u64,
        /// Exclusive.
        to_ms: u64,
    },
}
