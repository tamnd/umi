//! The twenty nine families doc 15.4 names, and nothing else.
//!
//! One struct, built once, shared by everything that produces a number. There
//! is no global and no lazy static, because a test that wants a clean set of
//! counters should be able to make one, and because a process wide singleton
//! would make the two crawls a single `umi` binary can run indistinguishable in
//! the output.
//!
//! Every field is behind an accessor rather than public, so that the only way
//! to reach a counter is through a label enum. Public arrays would let a caller
//! index with a number and put a fetch outcome in the tier slot.

use std::sync::Mutex;

use umi_types::{OutcomeCode, Tier};

use crate::labels::{
    AdmitResult, DiskRole, FetcherState, FrontierState, Label, Ladder, PublishStep, RobotsResult,
    StateOp, VerifyLayer, VerifyResult,
};
use crate::metric::{Counter, FloatGauge, Gauge, Histogram, SECONDS_FINE, SECONDS_WIDE, UNIT};

/// A metric with one label, stored one member per label value.
#[derive(Debug)]
pub struct Family<L: Label, M> {
    members: Box<[M]>,
    marker: std::marker::PhantomData<fn() -> L>,
}

impl<L: Label, M> Family<L, M> {
    /// A family whose members all come from `make`.
    fn build(mut make: impl FnMut() -> M) -> Self {
        Self {
            members: (0..L::COUNT).map(|_| make()).collect(),
            marker: std::marker::PhantomData,
        }
    }

    /// The member for this label value.
    pub fn get(&self, label: L) -> &M {
        // The index comes from the enum, which cannot be out of range, so this
        // never takes the panicking path.
        &self.members[label.index()]
    }

    /// Every member with the label value it belongs to, in index order.
    pub fn iter(&self) -> impl Iterator<Item = (L, &M)> {
        self.members
            .iter()
            .enumerate()
            .map(|(index, member)| (L::from_index(index), member))
    }
}

/// A metric with two labels, stored row major with `A` outermost.
#[derive(Debug)]
pub struct Family2<A: Label, B: Label, M> {
    members: Box<[M]>,
    marker: std::marker::PhantomData<fn() -> (A, B)>,
}

impl<A: Label, B: Label, M> Family2<A, B, M> {
    /// A family whose members all come from `make`.
    fn build(mut make: impl FnMut() -> M) -> Self {
        Self {
            members: (0..A::COUNT * B::COUNT).map(|_| make()).collect(),
            marker: std::marker::PhantomData,
        }
    }

    /// The member for this pair.
    pub fn get(&self, a: A, b: B) -> &M {
        &self.members[a.index() * B::COUNT + b.index()]
    }

    /// Every member with the pair it belongs to, in index order.
    pub fn iter(&self) -> impl Iterator<Item = (A, B, &M)> {
        self.members.iter().enumerate().map(|(index, member)| {
            (
                A::from_index(index / B::COUNT),
                B::from_index(index % B::COUNT),
                member,
            )
        })
    }
}

/// How many peers [`Peers`] will name before it stops.
///
/// Doc 03 has three coordinators. The cap is larger than that so a fourth box
/// during a migration is not a hole in the dashboard, and small enough that a
/// misconfiguration pointing at a thousand hosts costs eight series and not a
/// thousand.
pub const PEER_CAP: usize = 8;

/// Lag against named peers, for `umi_peer_lag_seconds`.
///
/// The one place in this crate where a label value is a runtime string, because
/// peer names come out of the config file and there is no enum that could know
/// them. It is bounded by [`PEER_CAP`] instead: once full it drops new names
/// silently, which loses a series and cannot lose the process.
///
/// A `Mutex` rather than atomics, because insertion has to be atomic against
/// the cap and a peer heartbeat is measured in seconds. This is not on any path
/// that runs 250 times a second.
#[derive(Debug, Default)]
pub struct Peers(Mutex<Vec<(Box<str>, f64)>>);

impl Peers {
    /// An empty set.
    #[must_use]
    pub fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }

    /// Record how far behind `peer` is, in seconds.
    ///
    /// A no-op for a name that is not already known once the cap is reached.
    pub fn set(&self, peer: &str, seconds: f64) {
        let mut peers = self.lock();
        if let Some(slot) = peers.iter_mut().find(|(name, _)| &**name == peer) {
            slot.1 = seconds;
            return;
        }
        if peers.len() < PEER_CAP {
            peers.push((peer.into(), seconds));
        }
    }

    /// Every peer and its lag, for the encoder.
    #[must_use]
    pub fn snapshot(&self) -> Vec<(Box<str>, f64)> {
        self.lock().clone()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<(Box<str>, f64)>> {
        // A poisoned lock here means a panic while holding a list of names and
        // floats, which cannot leave it inconsistent, and losing the metrics
        // because something else panicked would be the wrong response.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Everything doc 15.4 asks for.
///
/// Cheap to clone by reference and never by value. Wrap it in an `Arc` once at
/// startup and hand that out.
#[derive(Debug)]
pub struct Metrics {
    pages_fetched: Family2<Tier, OutcomeCode, Counter>,
    fetch_duration: Family<Tier, Histogram>,
    bytes_in: Counter,
    bytes_out: Counter,
    frontier_size: Family<FrontierState, Gauge>,
    admit: Family<AdmitResult, Counter>,
    shard_miss: Counter,
    state_bytes: Gauge,
    state_op_duration: Family<StateOp, Histogram>,
    hosts_backing_off: Gauge,
    robots_fetch: Family<RobotsResult, Counter>,
    render_pool_busy: Gauge,
    extract_queue_depth: Gauge,
    extract_duration: Histogram,
    segments_unpublished: Gauge,
    unpublished_bytes: Gauge,
    publish_lag: FloatGauge,
    publish_duration: Family<PublishStep, Histogram>,
    publish_failures: Family<PublishStep, Counter>,
    disk_free: Family<DiskRole, Gauge>,
    backpressure: Family<Ladder, Gauge>,
    fetchers_connected: Family<FetcherState, Gauge>,
    fetcher_reputation: Histogram,
    verify: Family2<VerifyLayer, VerifyResult, Counter>,
    disagreement_ratio: FloatGauge,
    quarantine_size: Gauge,
    dns_duration: Histogram,
    dns_failures: Counter,
    peer_lag: Peers,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Everything at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pages_fetched: Family2::build(Counter::new),
            fetch_duration: Family::build(|| Histogram::new(SECONDS_WIDE)),
            bytes_in: Counter::new(),
            bytes_out: Counter::new(),
            frontier_size: Family::build(Gauge::new),
            admit: Family::build(Counter::new),
            shard_miss: Counter::new(),
            state_bytes: Gauge::new(),
            state_op_duration: Family::build(|| Histogram::new(SECONDS_FINE)),
            hosts_backing_off: Gauge::new(),
            robots_fetch: Family::build(Counter::new),
            render_pool_busy: Gauge::new(),
            extract_queue_depth: Gauge::new(),
            extract_duration: Histogram::new(SECONDS_FINE),
            segments_unpublished: Gauge::new(),
            unpublished_bytes: Gauge::new(),
            publish_lag: FloatGauge::new(),
            publish_duration: Family::build(|| Histogram::new(SECONDS_WIDE)),
            publish_failures: Family::build(Counter::new),
            disk_free: Family::build(Gauge::new),
            backpressure: Family::build(Gauge::new),
            fetchers_connected: Family::build(Gauge::new),
            fetcher_reputation: Histogram::new(UNIT),
            verify: Family2::build(Counter::new),
            disagreement_ratio: FloatGauge::new(),
            quarantine_size: Gauge::new(),
            dns_duration: Histogram::new(SECONDS_WIDE),
            dns_failures: Counter::new(),
            peer_lag: Peers::new(),
        }
    }

    /// Pages fetched, by tier and outcome. `umi_pages_fetched_total`.
    #[must_use]
    pub fn pages_fetched(&self) -> &Family2<Tier, OutcomeCode, Counter> {
        &self.pages_fetched
    }

    /// How long a fetch took, by tier. `umi_fetch_duration_seconds`.
    #[must_use]
    pub fn fetch_duration(&self) -> &Family<Tier, Histogram> {
        &self.fetch_duration
    }

    /// Bytes read off the network. `umi_bytes_in_total`.
    #[must_use]
    pub fn bytes_in(&self) -> &Counter {
        &self.bytes_in
    }

    /// Bytes written to the network, requests and uploads. `umi_bytes_out_total`.
    #[must_use]
    pub fn bytes_out(&self) -> &Counter {
        &self.bytes_out
    }

    /// URLs in the frontier, by state. `umi_frontier_size`.
    #[must_use]
    pub fn frontier_size(&self) -> &Family<FrontierState, Gauge> {
        &self.frontier_size
    }

    /// Candidate URLs, by what became of them. `umi_admit_total`.
    #[must_use]
    pub fn admit(&self) -> &Family<AdmitResult, Counter> {
        &self.admit
    }

    /// Doc 08's number to watch: leases that wanted a shard not resident.
    /// `umi_shard_miss_total`.
    #[must_use]
    pub fn shard_miss(&self) -> &Counter {
        &self.shard_miss
    }

    /// Size of the state on disk. `umi_state_bytes`.
    #[must_use]
    pub fn state_bytes(&self) -> &Gauge {
        &self.state_bytes
    }

    /// How long a state operation took. `umi_state_op_duration_seconds`.
    #[must_use]
    pub fn state_op_duration(&self) -> &Family<StateOp, Histogram> {
        &self.state_op_duration
    }

    /// Hosts currently in backoff. `umi_hosts_backing_off`.
    #[must_use]
    pub fn hosts_backing_off(&self) -> &Gauge {
        &self.hosts_backing_off
    }

    /// robots.txt fetches, by result. `umi_robots_fetch_total`.
    #[must_use]
    pub fn robots_fetch(&self) -> &Family<RobotsResult, Counter> {
        &self.robots_fetch
    }

    /// Render slots in use. `umi_render_pool_busy`.
    #[must_use]
    pub fn render_pool_busy(&self) -> &Gauge {
        &self.render_pool_busy
    }

    /// Pages waiting to be extracted. `umi_extract_queue_depth`.
    #[must_use]
    pub fn extract_queue_depth(&self) -> &Gauge {
        &self.extract_queue_depth
    }

    /// How long an extract took. `umi_extract_duration_seconds`.
    #[must_use]
    pub fn extract_duration(&self) -> &Histogram {
        &self.extract_duration
    }

    /// Sealed segments not yet published. `umi_segments_unpublished`.
    #[must_use]
    pub fn segments_unpublished(&self) -> &Gauge {
        &self.segments_unpublished
    }

    /// Bytes in those segments. `umi_unpublished_bytes`.
    #[must_use]
    pub fn unpublished_bytes(&self) -> &Gauge {
        &self.unpublished_bytes
    }

    /// Age of the oldest unpublished segment. `umi_publish_lag_seconds`.
    #[must_use]
    pub fn publish_lag(&self) -> &FloatGauge {
        &self.publish_lag
    }

    /// How long a publish step took. `umi_publish_duration_seconds`.
    #[must_use]
    pub fn publish_duration(&self) -> &Family<PublishStep, Histogram> {
        &self.publish_duration
    }

    /// Publish steps that failed. `umi_publish_failures_total`.
    #[must_use]
    pub fn publish_failures(&self) -> &Family<PublishStep, Counter> {
        &self.publish_failures
    }

    /// Free space, by what the directory is for. `umi_disk_free_bytes`.
    #[must_use]
    pub fn disk_free(&self) -> &Family<DiskRole, Gauge> {
        &self.disk_free
    }

    /// Doc 15.3's rung, per ladder, zero when calm. `umi_backpressure_level`.
    #[must_use]
    pub fn backpressure(&self) -> &Family<Ladder, Gauge> {
        &self.backpressure
    }

    /// Fetchers on the protocol, by what they are doing.
    /// `umi_fetchers_connected`.
    #[must_use]
    pub fn fetchers_connected(&self) -> &Family<FetcherState, Gauge> {
        &self.fetchers_connected
    }

    /// Doc 06.5's scalar, sampled across the fleet. `umi_fetcher_reputation`.
    #[must_use]
    pub fn fetcher_reputation(&self) -> &Histogram {
        &self.fetcher_reputation
    }

    /// Verification decisions, by layer and outcome. `umi_verify_total`.
    #[must_use]
    pub fn verify(&self) -> &Family2<VerifyLayer, VerifyResult, Counter> {
        &self.verify
    }

    /// The milestone 4 number from doc 06.7.
    /// `umi_verify_disagreement_ratio`.
    #[must_use]
    pub fn disagreement_ratio(&self) -> &FloatGauge {
        &self.disagreement_ratio
    }

    /// Rows held for review. `umi_quarantine_size`.
    #[must_use]
    pub fn quarantine_size(&self) -> &Gauge {
        &self.quarantine_size
    }

    /// How long a DNS lookup took. `umi_dns_duration_seconds`.
    #[must_use]
    pub fn dns_duration(&self) -> &Histogram {
        &self.dns_duration
    }

    /// Lookups that did not answer. `umi_dns_failures_total`.
    #[must_use]
    pub fn dns_failures(&self) -> &Counter {
        &self.dns_failures
    }

    /// How far behind each coordinator is. `umi_peer_lag_seconds`.
    #[must_use]
    pub fn peer_lag(&self) -> &Peers {
        &self.peer_lag
    }
}
