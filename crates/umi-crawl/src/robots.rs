//! The robots.txt every fetch is checked against, held in memory.
//!
//! Doc 14.10 says there is no `--ignore-robots` flag and no equivalent, ever,
//! which makes this cache load bearing rather than an optimisation: the loop
//! has no path that fetches a page without a decision from here first.
//!
//! It is in memory and not in the state layer because of what the numbers say.
//! At 250 pages a second a server touches a few thousand distinct hosts an
//! hour, and a robots.txt is a couple of kilobytes, so the whole working set is
//! tens of megabytes. The state layer keeps a [`RobotsRef`] per host anyway,
//! which is the digest and the expiry rather than the rules, so a coordinator
//! that restarts knows what it had and refetches the ones that matter. Parsing
//! rules on the way out of SQLite on every single fetch would put a database
//! round trip inside the hot loop for an answer that does not change for a day.
//!
//! One fetch per host at a time. Two hundred URLs on one host arriving together
//! is the normal case, not the rare one, and a cache that let all two hundred
//! discover the miss would send two hundred requests for the same file to an
//! origin that has done nothing wrong.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, OnceCell};
use umi_robots::{Decision, Provenance, Robots};
use umi_types::HostId;

use crate::fetch::Fetch;

/// Doc 07.4's robots.txt lifetime.
///
/// Twenty four hours, and the same number whether the fetch worked or not. A
/// host that 5xx'd its robots.txt is disallowed for a day under RFC 9309
/// 2.3.1.4, and retrying that every few minutes to see if it recovered is the
/// behaviour that gets a crawler blocked by the humans rather than by the file.
pub const TTL_MS: u64 = 24 * 60 * 60 * 1000;

/// A parsed robots.txt and when to throw it away.
#[derive(Clone, Debug)]
pub struct Entry {
    /// The rules.
    pub robots: Arc<Robots>,
    /// When it was fetched.
    pub fetched_ms: u64,
    /// When it stops counting.
    pub expires_ms: u64,
}

impl Entry {
    /// Whether this is still usable at `now_ms`.
    #[must_use]
    pub const fn fresh(&self, now_ms: u64) -> bool {
        now_ms < self.expires_ms
    }
}

/// Robots.txt per host, fetched once and shared.
#[derive(Default)]
pub struct RobotsCache {
    hosts: Mutex<HashMap<HostId, Arc<OnceCell<Entry>>>>,
}

impl RobotsCache {
    /// An empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many hosts are held.
    pub async fn len(&self) -> usize {
        self.hosts.lock().await.len()
    }

    /// Whether anything is held.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// What robots.txt says about `url`, fetching the file first if we do not
    /// have a fresh copy.
    ///
    /// The decision and the entry it came from, because the caller wants the
    /// crawl delay and the `Content-Usage` value off the same entry and going
    /// back for them would be a second lock.
    pub async fn decide<F: Fetch + ?Sized>(
        &self,
        fetch: &F,
        host: HostId,
        origin: &str,
        url: &str,
        now_ms: u64,
    ) -> (Decision, Entry) {
        let entry = self.entry(fetch, host, origin, now_ms).await;
        (entry.robots.allows_url(url), entry)
    }

    /// The entry for a host, fetching it if it is missing or stale.
    async fn entry<F: Fetch + ?Sized>(
        &self,
        fetch: &F,
        host: HostId,
        origin: &str,
        now_ms: u64,
    ) -> Entry {
        // Two locks on purpose. The map lock is held only long enough to find
        // or install the cell, and the fetch happens inside the cell, so a
        // slow origin blocks the hosts waiting on that origin and nothing
        // else. Holding the map lock across the fetch would serialise the
        // whole crawl behind whichever site is slowest, which at 250 pages a
        // second is the difference between a crawler and a queue.
        let cell = {
            let mut hosts = self.hosts.lock().await;
            let existing = hosts.entry(host).or_default();
            // A cell whose entry has expired is replaced rather than reset,
            // because a task that already has a clone of the old cell should
            // keep using the old rules for the rest of its fetch rather than
            // block on a refetch it did not ask for.
            if existing.get().is_some_and(|e| !e.fresh(now_ms)) {
                *existing = Arc::default();
            }
            Arc::clone(existing)
        };

        cell.get_or_init(|| async {
            let robots = fetch_robots(fetch, origin).await;
            Entry {
                robots: Arc::new(robots),
                fetched_ms: now_ms,
                expires_ms: now_ms + TTL_MS,
            }
        })
        .await
        .clone()
    }

    /// Put an entry in without fetching, which is how a coordinator restores
    /// what it had before a restart and how a test sets a host up.
    pub async fn insert(&self, host: HostId, entry: Entry) {
        let cell = OnceCell::new_with(Some(entry));
        self.hosts.lock().await.insert(host, Arc::new(cell));
    }

    /// Drop everything that has expired.
    ///
    /// Nothing calls this on a timer. The loop calls it when it seals a
    /// segment, which is every few minutes and is a moment when a millisecond
    /// of map walking costs nothing.
    pub async fn evict_expired(&self, now_ms: u64) -> usize {
        let mut hosts = self.hosts.lock().await;
        let before = hosts.len();
        hosts.retain(|_, cell| cell.get().is_none_or(|e| e.fresh(now_ms)));
        before - hosts.len()
    }
}

/// Fetch and parse one origin's robots.txt.
///
/// Every path here ends in a `Robots`, including the ones where the fetch
/// failed. RFC 9309 section 2.3.1 says what each case means and umi-robots
/// already encodes it, so the job here is only to get the status and the body
/// into [`Robots::for_status`] without inventing a fourth answer.
async fn fetch_robots<F: Fetch + ?Sized>(fetch: &F, origin: &str) -> Robots {
    let url = format!("{origin}/robots.txt");
    match fetch.fetch(&url, None).await {
        Ok(umi_fetch::Outcome::Ok(page)) => Robots::for_status(page.status, page.body.as_ref()),
        // A 304 cannot happen because nothing above sends a conditional
        // request for robots.txt, and a redirect off the origin is not this
        // origin's robots.txt. Both are the unreachable case rather than the
        // allow-all case: we asked and did not get an answer.
        Ok(umi_fetch::Outcome::Gone) => Robots::allow_all(Provenance::NotFound),
        Ok(
            umi_fetch::Outcome::NotModified { .. } | umi_fetch::Outcome::RedirectedOffDomain { .. },
        ) => Robots::disallow_all(Provenance::Unreachable),
        Ok(umi_fetch::Outcome::Failed { failure, .. }) => match failure {
            // A 4xx on robots.txt is the common case on the web: most sites do
            // not have one. RFC 9309 2.3.1.3 says that means no restrictions.
            umi_fetch::Failure::NotFound => Robots::allow_all(Provenance::NotFound),
            umi_fetch::Failure::ServerError | umi_fetch::Failure::RateLimited => {
                Robots::disallow_all(Provenance::ServerError)
            }
            _ => Robots::disallow_all(Provenance::Unreachable),
        },
        // `Outcome` is non_exhaustive, so a variant added later lands here.
        // Disallow rather than allow: a fetch whose result this build cannot
        // name is a fetch that did not answer the question, and the fail
        // closed direction is the only safe one for a file whose whole job is
        // to say no.
        Ok(_) | Err(_) => Robots::disallow_all(Provenance::Unreachable),
    }
}
