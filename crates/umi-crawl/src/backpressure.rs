//! Doc 15.3's backpressure ladder, with no clock and no disk of its own.
//!
//! Doc 01 says the fleet produces more data per day than it can store, doc 12
//! says publishing is on the critical path, and doc 12.7 says unpublished data
//! is never deleted. Those three together mean the crawl has to slow itself
//! down before the disk fills, automatically, and this is the thing that
//! decides when.
//!
//! Three ladders run side by side because they have three different causes and
//! three different answers. Disk pressure wants fewer bytes per page, which is
//! why its first rung stops rendering and shifts the mix towards revalidation.
//! CPU pressure wants fewer expensive pages, which is the same first move for a
//! different reason. Memory pressure wants a smaller working set, which neither
//! of the others touches. Running them as one number would mean the memory
//! ladder firing on server1 turned off community leasing, which has nothing to
//! do with anything.
//!
//! Everything here is a pure function of the signals it is handed and the time
//! it is told. It does not stat a filesystem, read `/proc` or call the clock,
//! because a ladder that reads the world is a ladder you can only test by
//! filling a disk, and doc 16's gate 3.6 is expensive enough to run once
//! deliberately rather than on every commit.
//!
//! # The rung that is not here
//!
//! There is no rung that deletes unpublished data to make room, and there is no
//! flag, threshold or operator override that adds one. Disk pressure is a
//! reason to crawl less. It is never a reason to lose pages that were already
//! fetched, because those pages cost somebody else's bandwidth and cannot be
//! got back without spending it again.

use std::fmt;

use umi_state::{Budget, CLASSES, RefreshClass};
use umi_types::Tier;

/// A gigabyte, since every disk threshold in doc 15.3 is written in them.
const GB: u64 = 1 << 30;

/// A minute in milliseconds, same reason.
const MINUTE_MS: u64 = 60_000;

/// Doc 15.3's descent rule: below the next lower threshold continuously for ten
/// minutes, one rung at a time.
///
/// The asymmetry is the whole point. Going up is immediate because the disk
/// does not wait, and coming down is slow because a ladder that descends the
/// moment space appears oscillates, and every transition throws away in flight
/// work. A crawler stuck one rung too high for ten minutes is slower than it
/// needed to be. A crawler flapping between rungs every few seconds does not
/// finish anything.
const HOLD_MS: u64 = 10 * MINUTE_MS;

/// Unpublished bytes at disk rungs 1, 2 and 3.
const DISK_UNPUBLISHED: [u64; 3] = [4 * GB, 8 * GB, 16 * GB];

/// Publish lag at disk rungs 1, 2 and 3.
const DISK_LAG_MS: [u64; 3] = [20 * MINUTE_MS, 45 * MINUTE_MS, 90 * MINUTE_MS];

/// Free disk at rungs 1, 2, 3 and 4. Rung 4 has no other trigger: running out
/// of space is the only thing that justifies refusing deliveries.
const DISK_FREE: [u64; 4] = [40 * GB, 25 * GB, 15 * GB, 5 * GB];

/// Doc 15.3's extraction queue trigger.
const CPU_QUEUE: u32 = 2000;

/// How long the extraction pool has to stay saturated before it counts.
const CPU_SATURATED_MS: u64 = 30_000;

/// How long CPU pressure has to persist before the second response.
///
/// Doc 15.3 gives one trigger and two answers in order, shed the expensive
/// tiers and then cut the lease rate, and it does not give a second threshold
/// to separate them. Reusing its own thirty seconds is the reading that adds
/// the least: the first answer gets one full saturation window to work before
/// the second one is tried.
const CPU_ESCALATE_MS: u64 = 30_000;

/// Percent of the RSS budget at memory rungs 1 and 2.
const MEMORY_PERCENT: [u64; 2] = [85, 95];

/// Doc 09's discovery share when nothing is wrong.
pub const DISCOVERY_NORMAL: f32 = 0.35;

/// Doc 15.3's discovery share once the disk ladder is off the floor. Cutting
/// discovery shifts the mix towards revalidation, and revalidation is mostly
/// 304 responses that cost almost no storage, so this buys most of the byte
/// reduction for very little page rate.
pub const DISCOVERY_UNDER_PRESSURE: f32 = 0.15;

/// Which ladder a transition happened on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Ladder {
    /// Bytes on the disk and how far behind publishing is.
    Disk,
    /// The extraction queue and the extraction pool.
    Cpu,
    /// Resident set against the budget.
    Memory,
}

impl fmt::Display for Ladder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Disk => "disk",
            Self::Cpu => "cpu",
            Self::Memory => "memory",
        })
    }
}

/// The signal that moved a ladder, so the log can say why rather than that.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Cause {
    /// Bytes written and not yet published.
    Unpublished,
    /// How far behind the publisher is.
    PublishLag,
    /// Bytes left on the filesystem.
    FreeDisk,
    /// Rows waiting to be extracted.
    ExtractQueue,
    /// The extraction pool with no idle worker.
    Extractor,
    /// Resident set against the budget.
    Rss,
    /// Nothing. This is a descent, and a descent is caused by ten minutes of
    /// the signals staying down rather than by any one of them.
    Recovered,
}

impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unpublished => "unpublished bytes",
            Self::PublishLag => "publish lag",
            Self::FreeDisk => "free disk",
            Self::ExtractQueue => "extract queue",
            Self::Extractor => "extractor saturated",
            Self::Rss => "resident set",
            Self::Recovered => "recovered",
        })
    }
}

/// What the ladders are watching, as the caller sees it.
///
/// Instantaneous, all of it. The one thing that needs a duration, doc 15.3's
/// thirty seconds of a saturated extraction pool, is timed in here rather than
/// by the caller, so that a caller which forgets to keep a timestamp cannot get
/// it wrong quietly.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Signals {
    /// Bytes on disk that doc 12 has not published yet.
    pub unpublished_bytes: u64,
    /// How far behind the publisher is, in milliseconds.
    pub publish_lag_ms: u64,
    /// Bytes free on the filesystem the segments are written to.
    pub free_disk_bytes: u64,
    /// Rows waiting for the extractor.
    pub extract_queue: u32,
    /// Whether the extraction pool has no idle worker right now.
    pub extractor_saturated: bool,
    /// Resident set size.
    pub rss_bytes: u64,
    /// The RSS budget. Zero turns the memory ladder off, which is what a
    /// caller that does not know its own budget should say rather than
    /// guessing at one.
    pub rss_budget_bytes: u64,
}

/// One ladder moving from one rung to another.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Transition {
    /// Which ladder.
    pub ladder: Ladder,
    /// The rung it was on.
    pub from: u8,
    /// The rung it is on now.
    pub to: u8,
    /// The signal that did it.
    pub cause: Cause,
    /// That signal's value, in its own units, so the log line can carry the
    /// number an operator would otherwise go looking for.
    pub value: u64,
}

impl fmt::Display for Transition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} backpressure {} to {}, {} {}",
            self.ladder, self.from, self.to, self.cause, self.value
        )
    }
}

/// What the crawl is allowed to do at the rungs it is currently on.
///
/// One struct rather than a number, because three ladders with different
/// answers have to be combined and the combination is always the most
/// restrictive of each field taken separately. A caller that asked for the
/// level and worked out the rest would have to know all three ladders.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Allowance {
    /// The highest tier the ladder will pay for.
    pub max_tier: Tier,
    /// Doc 15.3's level 2 rule: use T1 on a host where T1 has ever worked,
    /// even when the host record prefers T2.
    pub prefer_known_t1: bool,
    /// Doc 09's share of the budget that goes to discovery.
    pub discovery_share: f32,
    /// What to multiply the lease rate by. Zero means lease nothing, which is
    /// not the same as stopping: outstanding leases are still accepted,
    /// extracted, written and published.
    pub lease_scale: f32,
    /// Whether community fetchers get work from this coordinator. Their
    /// deliveries land on our disk too, and they have other coordinators.
    pub community_leasing: bool,
    /// Whether deliveries are accepted at all. False means doc 04's flow
    /// control answers with a `retry_after` so fetchers hold their results
    /// rather than dropping them.
    pub accept_deliveries: bool,
    /// Whether every open segment should be sealed now.
    pub seal_open_segments: bool,
    /// Whether doc 10's shoal cap goes to its 64 MB floor.
    pub shoal_cap_at_floor: bool,
    /// Whether to evict the least recently used cold state shards.
    pub evict_cold_shards: bool,
}

impl Allowance {
    /// Doc 09.5's split with discovery moved to whatever the ladder allows.
    ///
    /// The other five classes keep the shares the operator configured, and
    /// only discovery moves, because doc 15.3's rung is about not taking on
    /// new work rather than about refreshing differently. Shares are a ratio
    /// and not a percentage, so the arithmetic is to pick the discovery share
    /// `d` that makes `d / (rest + d)` come out at the fraction asked for.
    ///
    /// A budget whose other five classes are all zero is left alone. Scaling
    /// discovery against nothing has no answer, and the caller that wrote that
    /// budget asked for a crawl that does nothing but discover.
    #[must_use]
    pub fn budget(&self, configured: Budget) -> Budget {
        let mut shares = [0u8; 6];
        let mut rest = 0u32;
        for class in CLASSES {
            shares[class.index()] = configured.share(class);
            if class != RefreshClass::Discovery {
                rest += u32::from(shares[class.index()]);
            }
        }
        if rest == 0 {
            return configured;
        }
        // In percent, and capped below 100 so the divisor cannot reach zero.
        // The share is a fraction of the whole batch and the arithmetic below
        // needs it as a fraction of the rest.
        let percent = (self.discovery_share.clamp(0.0, 0.95) * 100.0) as u32;
        // Floored at one, because a discovery share of zero is a different
        // instruction: it stops the class entirely, and no rung of doc 15.3
        // asks for that.
        let discovery = (rest * percent).div_ceil(100 - percent).max(1);
        shares[RefreshClass::Discovery.index()] = u8::try_from(discovery).unwrap_or(u8::MAX);
        Budget::new(shares)
    }
}

impl Default for Allowance {
    fn default() -> Self {
        Self {
            max_tier: Tier::Rendered,
            prefer_known_t1: false,
            discovery_share: DISCOVERY_NORMAL,
            lease_scale: 1.0,
            community_leasing: true,
            accept_deliveries: true,
            seal_open_segments: false,
            shoal_cap_at_floor: false,
            evict_cold_shards: false,
        }
    }
}

/// One ladder's position, and how long it has been wanting to come down.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
struct Rung {
    level: u8,
    /// When the signals first fell below this rung, or nothing while they are
    /// still at or above it.
    easing_since_ms: Option<u64>,
}

impl Rung {
    /// Move towards `demand`, and say so when the rung changed.
    fn settle(
        &mut self,
        demand: u8,
        now_ms: u64,
        ladder: Ladder,
        cause: Cause,
        value: u64,
    ) -> Option<Transition> {
        if demand > self.level {
            let from = self.level;
            self.level = demand;
            self.easing_since_ms = None;
            return Some(Transition {
                ladder,
                from,
                to: demand,
                cause,
                value,
            });
        }
        if demand == self.level {
            // Back up to where we are. Whatever easing had accumulated is
            // spent, because doc 15.3 wants the ten minutes to be continuous.
            self.easing_since_ms = None;
            return None;
        }
        let since = *self.easing_since_ms.get_or_insert(now_ms);
        if now_ms.saturating_sub(since) < HOLD_MS {
            return None;
        }
        let from = self.level;
        self.level -= 1;
        // Restart rather than clear, so a ladder that has three rungs to give
        // back takes ten minutes over each of them rather than falling off the
        // top in one tick.
        self.easing_since_ms = Some(now_ms);
        Some(Transition {
            ladder,
            from,
            to: self.level,
            cause: Cause::Recovered,
            value,
        })
    }
}

/// Doc 15.3's three ladders, together.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Backpressure {
    disk: Rung,
    cpu: Rung,
    memory: Rung,
    /// When the extraction pool last became saturated with no idle worker.
    saturated_since_ms: Option<u64>,
    /// When CPU pressure last became true, for the escalation to rung 2.
    pressure_since_ms: Option<u64>,
}

impl Backpressure {
    /// A crawl with nothing wrong with it.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            disk: Rung {
                level: 0,
                easing_since_ms: None,
            },
            cpu: Rung {
                level: 0,
                easing_since_ms: None,
            },
            memory: Rung {
                level: 0,
                easing_since_ms: None,
            },
            saturated_since_ms: None,
            pressure_since_ms: None,
        }
    }

    /// Read the signals and move the ladders.
    ///
    /// Call it once a tick. Returns the transitions, at most one per ladder,
    /// and an empty vector on the ordinary tick where nothing moved. The
    /// vector is what the caller logs and exports: doc 15.3 asks for the
    /// signal that caused every transition, and there is no way to reconstruct
    /// it from the level afterwards.
    pub fn observe(&mut self, signals: &Signals, now_ms: u64) -> Vec<Transition> {
        let mut out = Vec::new();

        let (demand, cause, value) = disk_demand(signals);
        if let Some(moved) = self.disk.settle(demand, now_ms, Ladder::Disk, cause, value) {
            out.push(moved);
        }

        let (demand, cause, value) = self.cpu_demand(signals, now_ms);
        if let Some(moved) = self.cpu.settle(demand, now_ms, Ladder::Cpu, cause, value) {
            out.push(moved);
        }

        let (demand, value) = memory_demand(signals);
        if let Some(moved) = self
            .memory
            .settle(demand, now_ms, Ladder::Memory, Cause::Rss, value)
        {
            out.push(moved);
        }

        out
    }

    /// The rung one ladder is on.
    #[must_use]
    pub const fn level(&self, ladder: Ladder) -> u8 {
        match ladder {
            Ladder::Disk => self.disk.level,
            Ladder::Cpu => self.cpu.level,
            Ladder::Memory => self.memory.level,
        }
    }

    /// Whether anything is wrong at all, which is the cheap check the loop
    /// makes before it bothers with the rest.
    #[must_use]
    pub const fn normal(&self) -> bool {
        self.disk.level == 0 && self.cpu.level == 0 && self.memory.level == 0
    }

    /// What the crawl may do right now.
    #[must_use]
    pub fn allowance(&self) -> Allowance {
        let mut out = Allowance::default();

        // Disk. The order of the rungs is the order of the trades: fewer bytes
        // per page first, then fewer pages, then no new pages, then no new
        // bytes from anybody.
        if self.disk.level >= 1 {
            out.max_tier = out.max_tier.min(Tier::Emulated);
            out.discovery_share = DISCOVERY_UNDER_PRESSURE;
        }
        if self.disk.level >= 2 {
            out.lease_scale = out.lease_scale.min(0.5);
            out.community_leasing = false;
            out.prefer_known_t1 = true;
        }
        if self.disk.level >= 3 {
            out.lease_scale = 0.0;
        }
        if self.disk.level >= 4 {
            out.accept_deliveries = false;
            out.seal_open_segments = true;
        }

        // CPU. Shed the expensive tiers first, because a saturated extractor
        // eventually expires leases and doc 06 counts an expired lease against
        // a fetcher that did nothing wrong.
        if self.cpu.level >= 1 {
            out.max_tier = out.max_tier.min(Tier::Plain);
        }
        if self.cpu.level >= 2 {
            out.lease_scale = out.lease_scale.min(0.5);
        }

        // Memory. On server1 this is the ladder that actually fires.
        if self.memory.level >= 1 {
            out.shoal_cap_at_floor = true;
            out.evict_cold_shards = true;
        }
        if self.memory.level >= 2 {
            out.lease_scale = 0.0;
        }

        out
    }

    /// The CPU rung, which needs the two timers this struct is holding.
    fn cpu_demand(&mut self, signals: &Signals, now_ms: u64) -> (u8, Cause, u64) {
        let saturated_for = if signals.extractor_saturated {
            let since = *self.saturated_since_ms.get_or_insert(now_ms);
            now_ms.saturating_sub(since)
        } else {
            self.saturated_since_ms = None;
            0
        };

        let queued = signals.extract_queue > CPU_QUEUE;
        let stuck = saturated_for >= CPU_SATURATED_MS;
        if !queued && !stuck {
            self.pressure_since_ms = None;
            return (0, Cause::Recovered, 0);
        }

        // The queue is reported when both are true. It is a number an operator
        // can act on and "saturated" is not.
        let (cause, value) = if queued {
            (Cause::ExtractQueue, u64::from(signals.extract_queue))
        } else {
            (Cause::Extractor, saturated_for)
        };

        let since = *self.pressure_since_ms.get_or_insert(now_ms);
        let demand = if now_ms.saturating_sub(since) >= CPU_ESCALATE_MS {
            2
        } else {
            1
        };
        (demand, cause, value)
    }
}

impl fmt::Display for Backpressure {
    /// Doc 15.3's one line, for `umi status` and for the crawl log.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.normal() {
            return f.write_str("normal");
        }
        write!(
            f,
            "disk {} cpu {} memory {}",
            self.disk.level, self.cpu.level, self.memory.level
        )
    }
}

/// The disk rung the signals ask for, and which signal asked hardest.
///
/// The highest rung any signal reaches wins, and the cause reported is that
/// signal. Three signals with one answer between them means an operator
/// reading the log sees the number that moved rather than all three.
fn disk_demand(signals: &Signals) -> (u8, Cause, u64) {
    let mut demand = 0;
    let mut cause = Cause::Recovered;
    let mut value = 0;

    for (rung, limit) in DISK_UNPUBLISHED.iter().enumerate() {
        if signals.unpublished_bytes > *limit {
            let level = u8::try_from(rung).unwrap_or(0) + 1;
            if level > demand {
                demand = level;
                cause = Cause::Unpublished;
                value = signals.unpublished_bytes;
            }
        }
    }
    for (rung, limit) in DISK_LAG_MS.iter().enumerate() {
        if signals.publish_lag_ms > *limit {
            let level = u8::try_from(rung).unwrap_or(0) + 1;
            if level > demand {
                demand = level;
                cause = Cause::PublishLag;
                value = signals.publish_lag_ms;
            }
        }
    }
    for (rung, limit) in DISK_FREE.iter().enumerate() {
        if signals.free_disk_bytes < *limit {
            let level = u8::try_from(rung).unwrap_or(0) + 1;
            if level > demand {
                demand = level;
                cause = Cause::FreeDisk;
                value = signals.free_disk_bytes;
            }
        }
    }

    (demand, cause, value)
}

/// The memory rung, as a percentage of the budget.
///
/// A budget of zero is a caller saying it does not know, and the answer to not
/// knowing is to leave the ladder alone rather than to invent a number and
/// throttle a crawl over it.
fn memory_demand(signals: &Signals) -> (u8, u64) {
    if signals.rss_budget_bytes == 0 {
        return (0, 0);
    }
    let percent = signals
        .rss_bytes
        .saturating_mul(100)
        .checked_div(signals.rss_budget_bytes)
        .unwrap_or(0);
    let demand = if percent >= MEMORY_PERCENT[1] {
        2
    } else if percent >= MEMORY_PERCENT[0] {
        1
    } else {
        0
    };
    (demand, percent)
}
