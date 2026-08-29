//! Following a site's own list of its pages, doc 13.6.
//!
//! `umi-seed` reads a sitemap. This fetches one, and then the ones it points
//! at, which is a different job: it needs a fetcher, a robots decision per
//! request, a politeness delay between requests and a budget, and none of
//! those belong in a parser.
//!
//! # Why it is worth the requests
//!
//! A sitemap is the only place a site says what it has without being asked. On
//! a site with a million pages behind a search form, link following finds the
//! front page and stops, and the sitemap finds the million. It is also dated:
//! every entry can carry a `lastmod`, which doc 09.4 treats as the publisher
//! signal that beats anything the change rate estimator worked out for itself,
//! so a poll of one small file can bring forward exactly the pages that moved
//! and leave the rest alone. That is the cheapest freshness there is.
//!
//! # What bounds it
//!
//! Three numbers, and each is a real failure without it. [`MAX_DEPTH`] stops a
//! sitemap index that points at itself, and a visited set stops the shorter
//! cycles that a depth limit alone would walk three times. [`MAX_FILES`] is
//! doc 13.6's cap on documents per host, which is what stops an index of
//! fifty thousand indexes from becoming a crawl of its own. [`MAX_URLS`] is
//! the cap on what one host can put in the frontier this way, because a
//! sitemap is a number a site chooses and the frontier is memory we pay for.
//! The parser's own caps sit under all three and bound a single document.
//!
//! # What it will not do
//!
//! It will not leave the origin. A sitemap index is allowed to point anywhere
//! and plenty point at a CDN, but a fetch of another host from here would skip
//! the frontier, and with it that host's politeness window and its own robots
//! decision. Those are counted as [`SitemapReport::off_origin`] rather than
//! followed. The URLs inside a sitemap are a different matter: those go through
//! admission like any other candidate, and the scope, the robots check and the
//! politeness delay all apply to them at fetch time.
//!
//! It will not skip robots.txt. Every request this makes, including the
//! sitemap listed in robots.txt itself, is checked first, per doc 14.10.

use std::collections::HashSet;

use umi_frontier::{DiscoverReport, Frontier};
use umi_seed::sitemap::{Caps, Entry as Dated};
use umi_seed::{Feed, Sitemap};
use umi_state::{Discovery, HostRow, State};
use umi_types::{RowKey, Tier};

use crate::clock::Clock;
use crate::fetch::Fetch;
use crate::robots::RobotsCache;

/// How far a sitemap index is followed, from doc 09.
///
/// The same number the parser publishes, and it has to be: the parser owns
/// what a nested index is and this owns how many times it is worth following
/// one, and a reader looking for the rule should find one answer.
pub const MAX_DEPTH: u8 = umi_seed::sitemap::MAX_INDEX_DEPTH;

/// Doc 13.6's cap on sitemap documents fetched per host.
pub const MAX_FILES: u32 = 50_000;

/// Doc 13.6's cap on URLs admitted from one host's sitemaps.
pub const MAX_URLS: u64 = 50_000_000;

/// The cap doc 09 puts on a poll rather than a seed.
///
/// A poll runs against a host we already have, so what it is looking for is
/// the entries that moved, and reading fifty thousand of them is enough to
/// find that out. Seeding is the case that wants [`MAX_URLS`], because there
/// the sitemap is all we have.
pub const POLL_URLS: u64 = 50_000;

/// How many URLs go to the frontier at once.
const CHUNK: usize = 4096;

/// What following one origin's sitemaps turned into.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct SitemapReport {
    /// Sitemap documents fetched, including the ones that turned out to be
    /// indexes and the ones that were not sitemaps at all.
    pub files: u32,
    /// URL entries read out of them, before admission.
    pub urls: u64,
    /// URLs that were new and are now in the frontier.
    pub admitted: u32,
    /// URLs we already had whose next visit was brought forward, because the
    /// sitemap dated them later than our last fetch. On a poll this is the
    /// number that says whether the poll was worth making.
    pub refreshed: u32,
    /// Sitemap URLs robots.txt said no to.
    pub disallowed: u32,
    /// Sitemap references to another origin, which are counted and not
    /// followed.
    pub off_origin: u32,
    /// Whether a cap stopped this before the sitemaps ran out.
    pub truncated: bool,
}

/// The limits one call runs under.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SitemapLimits {
    /// The most documents to fetch.
    pub max_files: u32,
    /// The most URL entries to read.
    pub max_urls: u64,
    /// How deep to follow an index.
    pub max_depth: u8,
    /// What the frontier records these as. A seed is depth zero because that
    /// is where depth is measured from, and doc 09.7's depth cap applies to
    /// what they link to rather than to them.
    pub depth: u8,
}

impl Default for SitemapLimits {
    fn default() -> Self {
        Self::seeding()
    }
}

impl SitemapLimits {
    /// The budget for a first pass over a host we have nothing from.
    #[must_use]
    pub const fn seeding() -> Self {
        Self {
            max_files: MAX_FILES,
            max_urls: MAX_URLS,
            max_depth: MAX_DEPTH,
            depth: 0,
        }
    }

    /// The budget for a poll of a host we already have.
    #[must_use]
    pub const fn polling() -> Self {
        Self {
            max_urls: POLL_URLS,
            ..Self::seeding()
        }
    }
}

/// Follow `origin`'s sitemaps and admit what they list.
///
/// The starting points are the `Sitemap` lines in robots.txt and
/// `/sitemap.xml`, in that order, deduplicated. Most sites have one or the
/// other and a fair number have both pointing at the same file.
///
/// # Errors
///
/// Whatever the store reports on admission. A sitemap that does not exist, does
/// not parse or is not allowed is not an error: it is the ordinary case on most
/// of the web, and the report says what happened.
pub async fn discover<F, C, S>(
    fetch: &F,
    clock: &C,
    robots: &RobotsCache,
    frontier: &Frontier<S>,
    origin: &str,
    limits: SitemapLimits,
) -> Result<SitemapReport, umi_state::StateError>
where
    F: Fetch + ?Sized,
    C: Clock + ?Sized,
    S: State,
{
    let mut report = SitemapReport::default();
    let Ok(key) = RowKey::for_url(origin, None) else {
        return Ok(report);
    };
    let host = key.host;

    // One entry, read once. It gives the starting points, the crawl delay and
    // the decision for every request below, and refetching it per sitemap would
    // be three requests for one file on a host that has done nothing wrong.
    let (entry, _) = robots
        .entry(fetch, host, origin, Tier::Plain, clock.now_ms())
        .await;
    let default_ms = u64::from(HostRow::INITIAL_DELAY_MS);
    let delay_ms = entry.robots.crawl_delay().map_or(default_ms, |d| {
        u64::try_from(d.as_millis()).unwrap_or(default_ms)
    });

    let mut queue: Vec<(String, u8)> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    for listed in entry.robots.sitemaps() {
        push(&mut queue, &mut visited, listed.clone(), 0);
    }
    push(&mut queue, &mut visited, format!("{origin}/sitemap.xml"), 0);

    // The parser's own caps, tightened so that no single document can spend the
    // whole limits. A host whose first file lists fifty million URLs has said
    // what it has, and there is nothing left to learn from the other files.
    let caps = Caps {
        max_urls: usize::try_from(limits.max_urls).unwrap_or(usize::MAX),
        ..Caps::default()
    };

    let mut batch: Vec<(String, Option<u64>)> = Vec::new();
    let mut cursor = 0;
    while cursor < queue.len() {
        let (url, depth) = queue[cursor].clone();
        cursor += 1;

        if report.files >= limits.max_files || report.urls >= limits.max_urls {
            report.truncated = true;
            break;
        }
        // A sitemap on another host is somebody else's politeness window and
        // somebody else's robots.txt, and this loop has neither.
        if !same_origin(origin, &url) {
            report.off_origin += 1;
            continue;
        }
        if !entry.robots.allows_url(&url).is_allowed() {
            report.disallowed += 1;
            continue;
        }

        // Doc 07.6, the same rule the crawl loop follows. A sitemap index with
        // twenty files under it is twenty requests to one origin, and sending
        // them as fast as they parse is the mistake the origin sees.
        clock
            .sleep_until_ms(clock.now_ms().saturating_add(delay_ms))
            .await;
        let Some(body) = get(fetch, &url).await else {
            continue;
        };
        report.files += 1;

        let parsed = Sitemap::parse_with(&body, &caps);
        // A feed where a sitemap was expected. Sites list one under `Sitemap:`
        // often enough that giving up here would lose real URLs, and the two
        // parsers cannot be confused for each other: this only runs when the
        // sitemap reader found nothing at all.
        let dated: Vec<Dated> = if parsed.is_empty() {
            let feed = Feed::parse_with(&body, &caps);
            report.truncated |= feed.truncated;
            feed.entries
        } else {
            // The parser's cap and this one both stop the same walk, so a
            // document the parser cut short is truncation the caller has to
            // hear about. Without this a budget small enough to be spent
            // inside one document would look like a complete answer.
            report.truncated |= parsed.truncated;
            for nested in &parsed.sitemaps {
                if depth < limits.max_depth {
                    push(&mut queue, &mut visited, nested.url.clone(), depth + 1);
                }
            }
            parsed.urls
        };

        for found in dated {
            if report.urls >= limits.max_urls {
                report.truncated = true;
                break;
            }
            report.urls += 1;
            batch.push((found.url, found.lastmod_ms));
        }
        if batch.len() >= CHUNK {
            admit(
                frontier,
                &mut batch,
                clock.now_ms(),
                limits.depth,
                &mut report,
            )
            .await?;
        }
    }
    if cursor < queue.len() {
        report.truncated = true;
    }
    admit(
        frontier,
        &mut batch,
        clock.now_ms(),
        limits.depth,
        &mut report,
    )
    .await?;
    Ok(report)
}

/// Queue a sitemap URL unless we have already been told about it.
///
/// The visited set is what makes a cycle cost one fetch rather than the depth
/// limit's worth of them, and two indexes that list each other is a shape that
/// happens by accident rather than by malice.
fn push(queue: &mut Vec<(String, u8)>, visited: &mut HashSet<String>, url: String, depth: u8) {
    if visited.insert(url.clone()) {
        queue.push((url, depth));
    }
}

/// Fetch one sitemap, or nothing.
///
/// Never conditional. A sitemap is polled rather than stored, so there is no
/// revalidator to send and a 304 would leave us with no document to read.
async fn get<F: Fetch + ?Sized>(fetch: &F, url: &str) -> Option<Vec<u8>> {
    match fetch.fetch(url, None, Tier::Plain).await.map(|s| s.outcome) {
        Ok(umi_fetch::Outcome::Ok(page)) if (200..300).contains(&page.status) => {
            Some(page.body.to_vec())
        }
        _ => None,
    }
}

/// Hand a batch to the frontier and fold what came back into the report.
async fn admit<S: State>(
    frontier: &Frontier<S>,
    batch: &mut Vec<(String, Option<u64>)>,
    now_ms: u64,
    depth: u8,
    report: &mut SitemapReport,
) -> Result<(), umi_state::StateError> {
    if batch.is_empty() {
        return Ok(());
    }
    let links: Vec<(&str, Option<u64>)> = batch
        .iter()
        .map(|(url, lastmod_ms)| (url.as_str(), *lastmod_ms))
        .collect();
    let DiscoverReport { admitted, .. } = frontier
        .admit_dated(&links, depth, now_ms, Discovery::Trusted)
        .await?;
    report.admitted = report.admitted.saturating_add(admitted.admitted);
    report.refreshed = report.refreshed.saturating_add(admitted.refreshed);
    batch.clear();
    Ok(())
}

/// Whether `url` is on the origin we are allowed to fetch from.
///
/// A string comparison on the origin rather than on the host, so that a sitemap
/// on `http://` when we are on `https://` is off origin. That is the right
/// answer: it is a different port, a different set of headers and, on plenty of
/// sites, a different server.
fn same_origin(origin: &str, url: &str) -> bool {
    let Ok(parsed) = url::Url::parse(url) else {
        return false;
    };
    let host = match parsed.host_str() {
        Some(host) => host,
        None => return false,
    };
    let theirs = match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    };
    theirs == origin
}
