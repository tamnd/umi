//! The robots.txt every fetch is checked against, held in memory.
//!
//! Doc 14.10 says there is no `--ignore-robots` flag and no equivalent, ever,
//! which makes this cache load bearing rather than an optimisation: the loop
//! has no path that fetches a page without a decision from here first.
//!
//! It is in memory and not in the state layer because of what the numbers say.
//! At 250 pages a second a server touches a few thousand distinct hosts an
//! hour, and a robots.txt is a couple of kilobytes, so the whole working set is
//! tens of megabytes. The state layer keeps a [`umi_state::RobotsRef`] per host
//! anyway, which is the digest and the expiry rather than the rules, so a
//! coordinator that restarts knows what it had and refetches the ones that
//! matter. Parsing rules on the way out of SQLite on every single fetch would
//! put a database round trip inside the hot loop for an answer that does not
//! change for a day.
//!
//! One fetch per host at a time. Two hundred URLs on one host arriving together
//! is the normal case, not the rare one, and a cache that let all two hundred
//! discover the miss would send two hundred requests for the same file to an
//! origin that has done nothing wrong.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, ListBuilder, RecordBatch, StringBuilder, UInt8Builder, UInt16Builder, UInt32Builder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field};
use tokio::sync::{Mutex, OnceCell};
use umi_file::StreamKind;
use umi_robots::{Decision, Provenance, Robots};
use umi_types::{Digest, HostId, Tier};

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
    /// blake3 of the body the rules were parsed from, and of the empty string
    /// when the fetch produced no body at all.
    ///
    /// Kept because the rules cannot be compared. Two robots.txt files that
    /// parse to the same rules for our user agent can differ everywhere else,
    /// and doc 07.7's rule about a `Disallow` that appears later needs to know
    /// that the file changed rather than that our slice of it did.
    pub digest: Digest,
    /// When it was fetched.
    pub fetched_ms: u64,
    /// When it stops counting.
    pub expires_ms: u64,
    /// The status the fetch came back with, or zero when it never got a
    /// response at all.
    ///
    /// Kept because doc 07.4 publishes it and because it is the one field that
    /// separates the three ways a host ends up with no rules: it said so, it
    /// has no file, or it was down.
    pub status: u16,
    /// The raw text the rules were parsed from, for hosts that served one.
    ///
    /// Doc 07.4 publishes the raw file, not only our reading of it, so this is
    /// what the snapshot carries. A reader who wants to know what a site said
    /// to some other crawler can only get that from the bytes.
    ///
    /// `None` when no body arrived or the status was not a 2xx. A 404 body is
    /// somebody's HTML error page rather than a robots.txt, and keeping those
    /// would fill the corpus with pages that say "not found" in forty
    /// languages.
    ///
    /// Behind an `Arc` because every fetch on a host clones the entry and the
    /// file can be half a megabyte. The whole cache is hosts touched in the
    /// last day, tens of thousands of them at a couple of kilobytes each, so
    /// carrying the text costs tens of megabytes and not more.
    pub body: Option<Arc<str>>,
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

    /// Whether a fresh answer for this host is already in hand.
    ///
    /// A cell that exists but has not been filled yet counts as held, because
    /// the fetch it is waiting on is one somebody already started and a second
    /// caller would only queue behind it. What this is for is deciding whether
    /// a host needs a request spent on it, and a host with a fetch in flight
    /// does not.
    pub async fn holds(&self, host: HostId, now_ms: u64) -> bool {
        self.hosts
            .lock()
            .await
            .get(&host)
            .is_some_and(|cell| cell.get().is_none_or(|e| e.fresh(now_ms)))
    }

    /// What robots.txt says about `url`, fetching the file first if we do not
    /// have a fresh copy.
    ///
    /// The decision, the entry it came from, and whether this call is the one
    /// that fetched the file. The entry comes back because the caller wants
    /// the crawl delay and the `Content-Usage` value off it and going back for
    /// them would be a second lock. The flag comes back because doc 07.4's
    /// crawl delay belongs in the host record, and writing it there on every
    /// lease that read the file out of the cache would put a host lookup in
    /// the hot loop for an answer that changes once a day.
    ///
    /// `tier` is the tier the lease that triggered this asked for, and it has
    /// to be honoured rather than pinned to T1. A host whose bot management
    /// refuses a plain client refuses it on robots.txt too, and fetching that
    /// file at T1 on a host doc 05.8 has already moved to T2 would come back
    /// unreachable, disallow the whole host, and make T2 unreachable for the
    /// hosts it exists for.
    pub async fn decide<F: Fetch + ?Sized>(
        &self,
        fetch: &F,
        host: HostId,
        origin: &str,
        url: &str,
        tier: Tier,
        now_ms: u64,
    ) -> (Decision, Entry, bool) {
        let (entry, fetched) = self.entry(fetch, host, origin, tier, now_ms).await;
        (entry.robots.allows_url(url), entry, fetched)
    }

    /// The entry for a host, fetching it if it is missing or stale.
    ///
    /// Public because robots.txt carries more than rules. Doc 13.6's sitemap
    /// discovery reads the `Sitemap` lines off it and the crawl delay off the
    /// same entry, and neither of those is a decision about a URL, so
    /// [`decide`](Self::decide) is the wrong shape for them.
    pub async fn entry<F: Fetch + ?Sized>(
        &self,
        fetch: &F,
        host: HostId,
        origin: &str,
        tier: Tier,
        now_ms: u64,
    ) -> (Entry, bool) {
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

        // Set from inside the cell, which is the only place that knows. An
        // entry restored by a coordinator on boot, or put there by a test, has
        // a `fetched_ms` on it and did not cost a request, so a caller cannot
        // tell the two apart by looking at the entry.
        let mut fetched = false;
        let entry = cell
            .get_or_init(|| async {
                fetched = true;
                let got = fetch_robots(fetch, origin, tier).await;
                Entry {
                    robots: Arc::new(got.robots),
                    digest: got.digest,
                    fetched_ms: now_ms,
                    expires_ms: now_ms + TTL_MS,
                    status: got.status,
                    body: got.body,
                }
            })
            .await
            .clone();
        (entry, fetched)
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

/// Fetch one origin's robots.txt into an [`Entry`], with no cache in front of
/// it.
///
/// The bulk prefetch is what this is for. It walks a list of hosts it has never
/// met, asks each of them once, and writes the answer straight out to a
/// segment, so there is nothing for a cache to save it and a map holding a
/// hundred million entries would be the largest thing in the process. A crawl
/// wants [`RobotsCache::entry`] instead, because a crawl asks the same question
/// about the same host a few hundred times a minute.
///
/// `now_ms` is the fetch time and the base for the expiry, and it is passed in
/// rather than read here for the reason the rest of the crate does not read a
/// clock: two machines replaying the same run have to produce the same rows.
pub async fn fetch_entry<F: Fetch + ?Sized>(
    fetch: &F,
    origin: &str,
    tier: Tier,
    now_ms: u64,
) -> Entry {
    let got = fetch_robots(fetch, origin, tier).await;
    Entry {
        robots: Arc::new(got.robots),
        digest: got.digest,
        fetched_ms: now_ms,
        expires_ms: now_ms + TTL_MS,
        status: got.status,
        body: got.body,
    }
}

/// Fetch and parse one origin's robots.txt.
///
/// Every path here ends in a `Robots`, including the ones where the fetch
/// failed. RFC 9309 section 2.3.1 says what each case means and umi-robots
/// already encodes it, so the job here is only to get the status and the body
/// into [`Robots::for_status`] without inventing a fourth answer.
///
/// The one thing this does beyond that is follow redirects that leave the
/// registrable domain. Doc 04.7 says a fetcher stops at those and hands the
/// target back, which is right for a page, and doc 07.5 accepts the extra
/// round trip a robots.txt costs because of it. RFC 9309 2.3.1.2 asks for at
/// least five hops followed "even across authorities", and the rules that come
/// back apply to the origin we started from.
async fn fetch_robots<F: Fetch + ?Sized>(fetch: &F, origin: &str, tier: Tier) -> Fetched {
    let mut url = format!("{origin}/robots.txt");
    let mut hops = 0u32;
    // The empty digest until a body arrives, which is what every path that
    // never sees one keeps. A 5xx and a timeout are different answers about a
    // host and neither of them is a file, so neither has bytes to hash, and
    // giving them the same digest is honest rather than lossy: what tells the
    // two apart on the way back out is the authoritative flag.
    let mut digest = digest_of(b"");
    let mut status = 0u16;
    let mut body: Option<Arc<str>> = None;
    loop {
        // Never conditional, which `fetch_robots` now enforces by not taking a
        // revalidator. A stale robots.txt that a 304 confirmed is still a file
        // we are about to re-read from a cache we already dropped, so the
        // saving is nothing and the code path is one more thing to get wrong.
        // The rungs it took are not this function's business. A robots.txt
        // that came back over plain HTTP because the browser would not hand
        // back a `text/plain` body is still this origin's robots.txt.
        // The short leash is the other half of `fetch_robots`. The host that
        // holds a slot in the fetch window for ten seconds and then sends
        // nothing is not rare, and doc 05.4's page budget is the wrong one to
        // spend on it.
        let robots = match fetch.fetch_robots(&url, tier).await.map(|s| s.outcome) {
            Ok(umi_fetch::Outcome::Ok(page)) => {
                digest = digest_of(page.body.as_ref());
                status = page.status;
                if (200..300).contains(&page.status) {
                    // Lossy rather than strict. RFC 9309 says the file is
                    // UTF-8 and plenty of them are not, and a parser that
                    // read the bytes anyway should not be contradicted by a
                    // publisher that drops the file for being ill formed.
                    body = Some(Arc::from(
                        String::from_utf8_lossy(page.body.as_ref()).as_ref(),
                    ));
                }
                Robots::for_status(page.status, page.body.as_ref())
            }
            Ok(umi_fetch::Outcome::RedirectedOffDomain {
                redirects, target, ..
            }) => {
                // Every hop counts, the ones the client already followed on
                // the way to this one included, so a chain that crosses
                // domains twice cannot buy itself a fresh budget each time.
                hops += u32::try_from(redirects.len()).unwrap_or(u32::MAX);
                hops += 1;
                if hops > umi_robots::MAX_REDIRECTS {
                    return Fetched {
                        robots: Robots::disallow_all(Provenance::Unreachable),
                        digest,
                        status,
                        body,
                    };
                }
                url = target;
                continue;
            }
            // A 410 is a 404 that means it, so the rules are the same and only
            // the published status differs.
            Ok(umi_fetch::Outcome::Gone) => {
                status = 410;
                Robots::allow_all(Provenance::NotFound)
            }
            // A 304 cannot happen because nothing above sends a conditional
            // request for robots.txt. It is the unreachable case rather than
            // the allow-all case: we asked and did not get an answer.
            Ok(umi_fetch::Outcome::NotModified { .. }) => {
                Robots::disallow_all(Provenance::Unreachable)
            }
            Ok(umi_fetch::Outcome::Failed {
                failure,
                status: got,
                ..
            }) => {
                // A 404 is an answer and a timeout is not, and the published
                // status is the only column that tells them apart. Leaving
                // both at zero made the corpus say "we never heard back" about
                // the most common robots.txt result on the web.
                status = got.unwrap_or(0);
                match failure {
                    // The three where the origin answered and the answer was
                    // still not a file we could read. A 200 that went past the
                    // body cap arrives here with no bytes at all, and running
                    // the status rule on it would parse an empty file and call
                    // that permission. Fail closed instead: the site published
                    // rules and we did not manage to read one of them.
                    umi_fetch::Failure::TooLarge
                    | umi_fetch::Failure::Malformed
                    | umi_fetch::Failure::NotDocument => {
                        Robots::disallow_all(Provenance::Unreachable)
                    }
                    // Everything else is decided by the status, through the
                    // same function the success path uses. Routing on our
                    // client's failure kind instead used to give a 403 one
                    // answer when it arrived as a response and a different one
                    // when it arrived as a block, which put two readings of the
                    // same status in the published corpus.
                    _ => match got {
                        Some(code) => Robots::for_status(code, b""),
                        // No status means no answer, and doc 07.4's 5xx rule
                        // covers the case for the same reason.
                        None => Robots::disallow_all(Provenance::Unreachable),
                    },
                }
            }
            // `Outcome` is non_exhaustive, so a variant added later lands here.
            // Disallow rather than allow: a fetch whose result this build
            // cannot name is a fetch that did not answer the question, and the
            // fail closed direction is the only safe one for a file whose whole
            // job is to say no.
            Ok(_) | Err(_) => Robots::disallow_all(Provenance::Unreachable),
        };
        return Fetched {
            robots,
            digest,
            status,
            body,
        };
    }
}

/// What one robots.txt fetch produced.
///
/// A struct rather than a tuple because the last two exist only to be
/// published and a reader of the call site should not have to count positions
/// to work out which of two similar looking values is the body.
struct Fetched {
    robots: Robots,
    digest: Digest,
    status: u16,
    body: Option<Arc<str>>,
}

/// blake3 of a robots.txt body.
fn digest_of(body: &[u8]) -> Digest {
    Digest::from_bytes(*blake3::hash(body).as_bytes())
}

/// One host's robots.txt as doc 07.4 publishes it.
///
/// Built from an [`Entry`] and the host it belongs to, which is everything the
/// snapshot carries. The parsed half is a summary rather than the rules
/// themselves: a reader who wants the rules has the raw text in `body`, and a
/// reader who wants to know whether a host is worth queueing wants the four
/// numbers next to it without parsing anything.
///
/// The summary is our reading of the file, for our user agent. `rules` is the
/// count in the group that applied to us and `groups` is the count in the whole
/// file, so `groups` above one with `rules` at zero means the site wrote rules
/// for somebody else and left us the default.
#[derive(Clone, Debug)]
pub struct RobotsRow {
    /// The host the file was fetched from, without a scheme.
    pub host: String,
    /// When we fetched it.
    pub fetched_at_ms: u64,
    /// The HTTP status, or zero when the fetch never got a response.
    pub status: u16,
    /// The raw text, for a host that served one.
    pub body: Option<String>,
    /// How many user agent groups the whole file had.
    pub groups: u32,
    /// How many rules applied to us.
    pub rules: u32,
    /// `Crawl-delay` for our group, clamped the way doc 07.4 clamps it.
    pub crawl_delay_ms: Option<u32>,
    /// Whether the file lets us fetch the root path. One for yes, zero for no.
    ///
    /// The root rather than the whole host, because "does this site allow us"
    /// has no single answer for a file with rules in it. A zero here is the
    /// strong signal, since a site that disallows `/` for us disallows
    /// everything under it.
    pub allows_us: u8,
    /// Every `Sitemap` line in the file, in the order they appeared.
    pub sitemaps: Vec<String>,
    /// The file's AIPREF `Content-Usage`, rendered the same way the pages
    /// stream renders it so the two columns compare.
    pub content_usage: Option<String>,
}

impl RobotsRow {
    /// Build the published row for `host` out of what the cache holds.
    #[must_use]
    pub fn build(host: &str, entry: &Entry) -> Self {
        let robots = &entry.robots;
        Self {
            host: host.to_owned(),
            fetched_at_ms: entry.fetched_ms,
            status: entry.status,
            body: entry.body.as_deref().map(ToOwned::to_owned),
            groups: robots.group_count(),
            rules: u32::try_from(robots.rule_count()).unwrap_or(u32::MAX),
            // Milliseconds because the schema says so, and the value is
            // already clamped to five minutes by the parser, so the cast
            // cannot lose anything.
            crawl_delay_ms: robots
                .crawl_delay()
                .map(|d| u32::try_from(d.as_millis()).unwrap_or(u32::MAX)),
            allows_us: u8::from(robots.allows("/").is_allowed()),
            sitemaps: robots.sitemaps().to_vec(),
            content_usage: robots.usage().render(),
        }
    }
}

/// [`RobotsRow`]s into doc 10.5's robots batch.
///
/// The same shape as the page builder and for the same reason: rows go in one
/// at a time, the builder says when it has had enough, and `finish` produces a
/// batch that matches [`StreamKind::Robots`] exactly.
pub struct RobotsBuilder {
    host: StringBuilder,
    fetched_at_ms: UInt64Builder,
    status: UInt16Builder,
    body: StringBuilder,
    groups: UInt32Builder,
    rules: UInt32Builder,
    crawl_delay_ms: UInt32Builder,
    allows_us: UInt8Builder,
    sitemaps: ListBuilder<StringBuilder>,
    content_usage: StringBuilder,
    rows: usize,
    bytes: usize,
}

impl Default for RobotsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RobotsBuilder {
    /// The row half of the shoal cap.
    ///
    /// Larger than the page limit because a robots row is small. The body is
    /// capped at RFC 9309's 500 KiB and the median file is under two, so
    /// sixty five thousand rows is a shoal in the same size class as a page
    /// shoal of sixteen thousand.
    pub const ROW_LIMIT: usize = 65_536;

    /// The byte half of the shoal cap, doc 10.4's 32 MiB.
    pub const BYTE_LIMIT: usize = 32 << 20;

    /// An empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            host: StringBuilder::new(),
            fetched_at_ms: UInt64Builder::new(),
            status: UInt16Builder::new(),
            body: StringBuilder::new(),
            groups: UInt32Builder::new(),
            rules: UInt32Builder::new(),
            crawl_delay_ms: UInt32Builder::new(),
            allows_us: UInt8Builder::new(),
            sitemaps: ListBuilder::new(StringBuilder::new()).with_field(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                false,
            ))),
            content_usage: StringBuilder::new(),
            rows: 0,
            bytes: 0,
        }
    }

    /// How many rows have gone in.
    #[must_use]
    pub const fn rows(&self) -> usize {
        self.rows
    }

    /// Whether this shoal is full.
    #[must_use]
    pub const fn is_full(&self) -> bool {
        self.rows >= Self::ROW_LIMIT || self.bytes >= Self::BYTE_LIMIT
    }

    /// Whether anything has gone in.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Append one row.
    pub fn push(&mut self, row: &RobotsRow) {
        self.host.append_value(&row.host);
        self.fetched_at_ms.append_value(row.fetched_at_ms);
        self.status.append_value(row.status);
        self.body.append_option(row.body.as_deref());
        self.groups.append_value(row.groups);
        self.rules.append_value(row.rules);
        self.crawl_delay_ms.append_option(row.crawl_delay_ms);
        self.allows_us.append_value(row.allows_us);
        for sitemap in &row.sitemaps {
            self.sitemaps.values().append_value(sitemap);
        }
        self.sitemaps.append(true);
        self.content_usage
            .append_option(row.content_usage.as_deref());

        self.rows += 1;
        self.bytes += row.host.len()
            + row.body.as_ref().map_or(0, String::len)
            + row.sitemaps.iter().map(String::len).sum::<usize>()
            + row.content_usage.as_ref().map_or(0, String::len);
    }

    /// Finish the batch.
    #[must_use]
    pub fn finish(mut self) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.host.finish()),
            Arc::new(self.fetched_at_ms.finish()),
            Arc::new(self.status.finish()),
            Arc::new(self.body.finish()),
            Arc::new(self.groups.finish()),
            Arc::new(self.rules.finish()),
            Arc::new(self.crawl_delay_ms.finish()),
            Arc::new(self.allows_us.finish()),
            Arc::new(self.sitemaps.finish()),
            Arc::new(self.content_usage.finish()),
        ];
        RecordBatch::try_new(StreamKind::Robots.arrow(), columns)
            .expect("the robots builder matches doc 10.5")
    }
}
