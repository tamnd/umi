//! The per pay level domain rate limit from doc 09.3 and the domain heap from
//! doc 09.1, which turn out to be one structure rather than two.
//!
//! Doc 09.1 wants a heap of domains keyed by next allowed fetch time, and doc
//! 09.3 wants a token bucket per domain at 20 requests a second. A bucket
//! written the usual way, as a count of tokens plus the time it was last
//! topped up, is two numbers per domain and neither of them sorts the way the
//! heap needs, so the heap would carry a third kept in step with the other
//! two. Written the other way round it is one number, and that number sorts
//! correctly, so there is one structure here rather than two.
//!
//! The number is where the domain's schedule has got to: the time the last
//! request would have gone out if every request so far had been spaced exactly
//! one interval apart. A request is allowed when the schedule is no further
//! ahead of now than the burst tolerance, and issuing one pushes the schedule
//! forward by an interval. That is the same limit a token bucket enforces, a
//! schedule an interval ahead being a token spent, and it is exact in
//! integers. Doc 11.1's ban on floats in anything that decides an outcome
//! applies here too: two coordinators replaying the same crawl have to make
//! the same scheduling decisions, and a rate limiter that accumulates
//! fractional tokens does not give the same answer twice.

use std::collections::{BTreeSet, HashMap};

use umi_types::PldId;

/// How fast one pay level domain may be crawled.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Rate {
    interval_ms: u32,
    tolerance_ms: u32,
}

impl Rate {
    /// The cap from doc 09.3, in requests per second.
    pub const DEFAULT_PER_SECOND: u32 = 20;

    /// One interval, which is the slowest this can be set to.
    pub const SLOWEST: Self = Self {
        interval_ms: u32::MAX,
        tolerance_ms: 0,
    };

    /// A rate of `per_second` requests, allowing `burst` of them at once.
    ///
    /// The interval is rounded up rather than to nearest, so a rate that does
    /// not divide a second evenly comes out slightly under the number asked
    /// for rather than slightly over. Under is the safe direction: the number
    /// is a promise to an origin that did not agree to any of this.
    ///
    /// A `per_second` of zero is treated as one, and a `burst` of zero as one,
    /// because a domain that can never be fetched is a way to lose a crawl
    /// quietly and there is [`Rate::SLOWEST`] for anyone who means it.
    #[must_use]
    pub const fn new(per_second: u32, burst: u32) -> Self {
        let per_second = if per_second == 0 { 1 } else { per_second };
        let burst = if burst == 0 { 1 } else { burst };
        let interval_ms = 1000u32.div_ceil(per_second);
        Self {
            interval_ms,
            tolerance_ms: interval_ms * (burst - 1),
        }
    }

    /// The gap between two requests to the same domain, in milliseconds.
    #[must_use]
    pub const fn interval_ms(self) -> u32 {
        self.interval_ms
    }

    /// How far ahead of now the schedule may run, in milliseconds. A domain
    /// that has been idle can spend this all at once.
    #[must_use]
    pub const fn tolerance_ms(self) -> u32 {
        self.tolerance_ms
    }
}

impl Default for Rate {
    fn default() -> Self {
        // A burst of one second's worth. The requests inside a burst go to
        // different hosts under the same domain, since doc 07.6 already holds
        // each host to one request in flight, so this is twenty connections to
        // twenty machines rather than twenty to one.
        Self::new(Self::DEFAULT_PER_SECOND, Self::DEFAULT_PER_SECOND)
    }
}

/// Every domain the scheduler knows about, ordered by how far its schedule has
/// run.
///
/// Two collections over the same set, because both accesses are on the hot
/// path: the map answers "how much may this domain take" for a domain the
/// caller already has, and the set answers "which domains are ready" without
/// looking at the ones that are not.
///
/// The ordering is on the schedule itself rather than on the earliest time the
/// domain may next be fetched, and the difference matters. Those two are the
/// burst tolerance apart, and a domain that has spent less than its tolerance
/// is permitted right now, so ordering on the permitted time puts every
/// lightly used domain at zero and the tiebreak by domain id decides the
/// crawl. With a tick that can only visit a few domains, the same few win
/// every time and the rest never get a turn. Ordering on the schedule keeps
/// the domains apart: one that has taken eight requests sorts behind one that
/// has taken none, even though both may go now.
#[derive(Debug, Default)]
pub struct Gate {
    rate: Rate,
    schedule: HashMap<PldId, u64>,
    order: BTreeSet<(u64, PldId)>,
}

impl Gate {
    /// An empty gate at the given rate.
    #[must_use]
    pub fn new(rate: Rate) -> Self {
        Self {
            rate,
            schedule: HashMap::new(),
            order: BTreeSet::new(),
        }
    }

    /// The rate every domain in here is held to.
    #[must_use]
    pub fn rate(&self) -> Rate {
        self.rate
    }

    /// How many domains are being tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.schedule.len()
    }

    /// Whether any domain is being tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.schedule.is_empty()
    }

    /// Start tracking a domain, if it is not already tracked.
    ///
    /// A domain arrives here fully credited rather than throttled, because it
    /// has not been fetched yet and starting it in debt would delay the first
    /// request to every site we discover.
    pub fn note(&mut self, pld: PldId) {
        if !self.schedule.contains_key(&pld) {
            self.set(pld, 0);
        }
    }

    /// Stop tracking one domain.
    ///
    /// Forgetting a domain forgets what it has spent, so a domain evicted and
    /// warmed again inside a second gets its burst back. That is a real hole
    /// and it is left open on purpose: closing it means keeping a schedule for
    /// every domain we have ever seen rather than for the ones we are working,
    /// and eviction is doc 08.6's answer to a domain having gone idle, so a
    /// domain that has just been evicted is by definition one nothing has
    /// asked for.
    pub fn forget(&mut self, pld: PldId) {
        if let Some(at) = self.schedule.remove(&pld) {
            self.order.remove(&(at, pld));
        }
    }

    /// Forget every domain that is not in `keep`, which must be sorted.
    ///
    /// This is how eviction reaches the scheduler: doc 08.6 makes local disk a
    /// cache, so a domain that has been sealed and uploaded is no longer
    /// schedulable and its schedule is not worth carrying. It walks every
    /// domain being tracked, so it belongs on the eviction path and on doc
    /// 09.8's restart rebuild, and not on the scheduler tick. The length check
    /// in front makes the common call free.
    pub fn retain(&mut self, keep: &[PldId]) {
        if self.schedule.len() == keep.len() {
            return;
        }
        let gone: Vec<PldId> = self
            .schedule
            .keys()
            .filter(|pld| keep.binary_search(pld).is_err())
            .copied()
            .collect();
        for pld in gone {
            if let Some(at) = self.schedule.remove(&pld) {
                self.order.remove(&(at, pld));
            }
        }
    }

    /// The domains that may be fetched at `now_ms`, and how many requests each
    /// may take, most overdue first and at most `limit` of them.
    ///
    /// Doc 09.3 step 1 pops the domains whose next ready time has passed, and
    /// this is that pop, except that it does not remove anything: a domain
    /// leaves the ordering only when it is charged or evicted, so a tick that
    /// finds no work for a domain does not lose track of it.
    ///
    /// The order is by schedule and then by domain id, which is a total order
    /// over a set that does not depend on hash iteration, so the same gate at
    /// the same time always offers the same domains in the same sequence.
    #[must_use]
    pub fn ready(&self, now_ms: u64, limit: usize) -> Vec<(PldId, u32)> {
        let mut out = Vec::new();
        let horizon = now_ms.saturating_add(self.tolerance());
        for (at, pld) in &self.order {
            if *at > horizon || out.len() == limit {
                break;
            }
            let allowance = self.allowance(*pld, now_ms);
            if allowance > 0 {
                out.push((*pld, allowance));
            }
        }
        out
    }

    /// How many requests this domain may take at `now_ms`.
    ///
    /// Zero for a domain whose schedule has run further ahead of now than the
    /// burst tolerance allows, which is the whole of the rate limit.
    #[must_use]
    pub fn allowance(&self, pld: PldId, now_ms: u64) -> u32 {
        let at = self.schedule.get(&pld).copied().unwrap_or(0);
        let tolerance = self.tolerance();
        let ahead = at.saturating_sub(now_ms);
        if ahead > tolerance {
            return 0;
        }
        let slack = tolerance - ahead;
        let allowance = slack / u64::from(self.rate.interval_ms) + 1;
        u32::try_from(allowance).unwrap_or(u32::MAX)
    }

    /// Charge a domain for the requests that were actually issued.
    ///
    /// Charging after the fact rather than reserving up front is deliberate.
    /// A domain is offered an allowance and usually cannot fill it, because
    /// its hosts are inside their own politeness windows or it has run out of
    /// due URLs, and a reservation would spend the domain's budget on requests
    /// that were never sent.
    pub fn charge(&mut self, pld: PldId, taken: u32, now_ms: u64) {
        if taken == 0 {
            return;
        }
        // A schedule behind now means the domain has been idle, so it starts
        // again from now. Letting it stay behind would let idle time bank into
        // an unbounded burst, which is the failure a plain leaky bucket has
        // and the tolerance term exists to bound.
        let from = self.schedule.get(&pld).copied().unwrap_or(0).max(now_ms);
        let cost = u64::from(taken).saturating_mul(u64::from(self.rate.interval_ms));
        self.set(pld, from.saturating_add(cost));
    }

    /// When this domain may next be fetched, or `None` if it is not tracked.
    #[must_use]
    pub fn next_ready_ms(&self, pld: PldId) -> Option<u64> {
        self.schedule
            .get(&pld)
            .map(|at| at.saturating_sub(self.tolerance()))
    }

    fn tolerance(&self) -> u64 {
        u64::from(self.rate.tolerance_ms)
    }

    fn set(&mut self, pld: PldId, at: u64) {
        if let Some(old) = self.schedule.insert(pld, at) {
            self.order.remove(&(old, pld));
        }
        self.order.insert((at, pld));
    }
}

#[cfg(test)]
mod tests {
    use umi_types::PldId;

    use super::{Gate, Rate};

    fn pld(n: u8) -> PldId {
        PldId::derive(&[n])
    }

    #[test]
    fn a_rate_rounds_the_interval_up_so_it_never_runs_fast() {
        assert_eq!(Rate::new(20, 20).interval_ms(), 50);
        assert_eq!(Rate::new(20, 20).tolerance_ms(), 950);
        // Three a second is 333.33 ms and this takes 334, so it is 2.99 a
        // second rather than 3.003.
        assert_eq!(Rate::new(3, 1).interval_ms(), 334);
        assert_eq!(Rate::new(3, 1).tolerance_ms(), 0);
    }

    #[test]
    fn a_rate_of_zero_is_a_rate_of_one() {
        assert_eq!(Rate::new(0, 0).interval_ms(), 1000);
        assert_eq!(Rate::new(0, 0).tolerance_ms(), 0);
    }

    #[test]
    fn an_idle_domain_may_spend_its_burst_and_then_waits() {
        let mut gate = Gate::new(Rate::new(20, 20));
        gate.note(pld(1));
        assert_eq!(gate.allowance(pld(1), 0), 20);

        gate.charge(pld(1), 20, 0);
        assert_eq!(gate.allowance(pld(1), 0), 0);
        // One interval later, one more request.
        assert_eq!(gate.allowance(pld(1), 50), 1);
        assert_eq!(gate.allowance(pld(1), 500), 10);
        // And the burst is back after a full second of idling, not more.
        assert_eq!(gate.allowance(pld(1), 1000), 20);
        assert_eq!(gate.allowance(pld(1), 60_000), 20);
    }

    #[test]
    fn the_sustained_rate_is_the_rate_however_the_requests_are_grouped() {
        // Twenty seconds of a scheduler asking every 100 ms, which is doc
        // 09.3's tick, and taking everything it is offered.
        for group in [1u32, 3, 7, 20] {
            let mut gate = Gate::new(Rate::new(20, 20));
            gate.note(pld(1));
            let mut taken = 0u32;
            let mut now = 0u64;
            while now < 20_000 {
                let allowance = gate.allowance(pld(1), now).min(group);
                gate.charge(pld(1), allowance, now);
                taken += allowance;
                now += 100;
            }
            // Twenty seconds at twenty a second, plus the one burst at the
            // start. Nothing above that, whatever the grouping. A scheduler
            // that only takes one at a time is held to ten a second by its own
            // tick rather than by the gate, so the floor is whichever of the
            // two limits binds.
            let possible = group * 200;
            assert!(
                taken <= (20 * 20 + 20).min(possible),
                "{group} at a time gave {taken} in twenty seconds"
            );
            assert!(
                taken >= (20 * 19).min(possible),
                "{group} at a time only managed {taken}"
            );
        }
    }

    #[test]
    fn a_domain_that_has_been_used_sorts_behind_one_that_has_not() {
        // The ordering is on the schedule and not on the earliest permitted
        // time, and this is why. Eight requests is well inside the burst, so
        // both domains may go right now and both would report a permitted time
        // of zero. If that were the sort key the tiebreak by domain id would
        // decide, and a scheduler that can only visit a few domains a tick
        // would visit the same few forever.
        let mut gate = Gate::new(Rate::new(20, 20));
        gate.note(pld(1));
        gate.note(pld(2));
        gate.charge(pld(1), 8, 0);
        assert_eq!(gate.next_ready_ms(pld(1)), Some(0));
        assert_eq!(gate.next_ready_ms(pld(2)), Some(0));

        let ready: Vec<_> = gate.ready(0, 10).into_iter().map(|(p, _)| p).collect();
        assert_eq!(ready, vec![pld(2), pld(1)]);
    }

    #[test]
    fn a_domain_that_takes_nothing_is_not_charged() {
        let mut gate = Gate::new(Rate::new(20, 20));
        gate.note(pld(1));
        gate.charge(pld(1), 0, 5_000);
        assert_eq!(gate.next_ready_ms(pld(1)), Some(0));
        assert_eq!(gate.allowance(pld(1), 0), 20);
    }

    #[test]
    fn ready_is_ordered_and_capped_and_skips_the_domains_that_are_not() {
        let mut gate = Gate::new(Rate::new(20, 20));
        for n in 1..=4 {
            gate.note(pld(n));
        }
        assert_eq!(gate.ready(0, 10).len(), 4);
        assert_eq!(gate.ready(0, 2).len(), 2);

        gate.charge(pld(1), 20, 0);
        gate.charge(pld(2), 20, 0);
        let ready: Vec<_> = gate.ready(0, 10).into_iter().map(|(p, _)| p).collect();
        assert_eq!(ready.len(), 2);
        assert!(!ready.contains(&pld(1)) && !ready.contains(&pld(2)));

        // The same gate asked twice gives the same answer in the same order,
        // which is what a replay depends on.
        assert_eq!(gate.ready(0, 10), gate.ready(0, 10));
    }

    #[test]
    fn the_most_overdue_domain_comes_first() {
        let mut gate = Gate::new(Rate::new(20, 20));
        gate.note(pld(1));
        gate.note(pld(2));
        // Domain 2 was fetched recently, domain 1 a while ago, so domain 1 is
        // the more overdue of the two even though its id sorts either way.
        gate.charge(pld(2), 20, 10_000);
        let ready: Vec<_> = gate.ready(20_000, 10).into_iter().map(|(p, _)| p).collect();
        assert_eq!(ready, vec![pld(1), pld(2)]);
    }

    #[test]
    fn retain_drops_the_evicted_and_does_nothing_when_nothing_moved() {
        let mut gate = Gate::new(Rate::default());
        let mut all: Vec<PldId> = (1..=4).map(pld).collect();
        all.sort_unstable();
        for p in &all {
            gate.note(*p);
        }
        gate.retain(&all);
        assert_eq!(gate.len(), 4);

        let keep: Vec<PldId> = all.iter().copied().take(2).collect();
        gate.retain(&keep);
        assert_eq!(gate.len(), 2);
        assert_eq!(gate.ready(0, 10).len(), 2);
        for p in &keep {
            assert!(gate.next_ready_ms(*p).is_some());
        }
    }

    #[test]
    fn noting_a_domain_twice_does_not_refund_it() {
        let mut gate = Gate::new(Rate::new(20, 20));
        gate.note(pld(1));
        gate.charge(pld(1), 20, 0);
        gate.note(pld(1));
        assert_eq!(gate.allowance(pld(1), 0), 0);
    }

    #[test]
    fn an_untracked_domain_is_not_offered_but_would_be_allowed() {
        let gate = Gate::new(Rate::new(20, 20));
        assert!(gate.ready(0, 10).is_empty());
        assert_eq!(gate.next_ready_ms(pld(9)), None);
        // The allowance is the answer for a domain at time zero, so a caller
        // that asks about a domain the gate has not been told about gets the
        // permissive answer rather than a wrong denial.
        assert_eq!(gate.allowance(pld(9), 0), 20);
    }
}
