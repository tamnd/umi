//! Following sitemaps, against a fetcher made of canned answers.
//!
//! The parser has its own tests in `umi-seed` and they cover what a document
//! means. What is left for here is everything a parser cannot see: whether
//! robots.txt is read first, whether an index is followed and how far, whether
//! a cycle terminates, whether a cross origin reference is fetched, and
//! whether the dates come out the other side as a schedule.

use std::sync::Arc;

use umi_state::{MemoryState, State};

use crate::run_tests::{Canned, T0, crawler};
use crate::sitemap::{MAX_DEPTH, SitemapLimits};

const ORIGIN: &str = "https://example.com";

/// A sitemap listing `urls`, each with the same `lastmod`.
fn urlset(urls: &[&str], lastmod: &str) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\"?>\n<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">",
    );
    for url in urls {
        out.push_str(&format!(
            "<url><loc>{url}</loc><lastmod>{lastmod}</lastmod></url>"
        ));
    }
    out.push_str("</urlset>");
    out
}

/// A sitemap index pointing at `sitemaps`.
fn index(sitemaps: &[&str]) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\"?>\n<sitemapindex \
         xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">",
    );
    for url in sitemaps {
        out.push_str(&format!("<sitemap><loc>{url}</loc></sitemap>"));
    }
    out.push_str("</sitemapindex>");
    out
}

async fn empty() -> Arc<dyn State> {
    Arc::new(MemoryState::new())
}

#[tokio::test]
async fn a_sitemap_at_the_usual_place_is_found_without_being_told() {
    // No robots.txt, so nothing points anywhere, and `/sitemap.xml` is tried
    // anyway. Most sites that have one have it there and never mention it.
    let doc = urlset(
        &[
            "https://example.com/a",
            "https://example.com/b",
            "https://example.com/c",
        ],
        "2024-01-01",
    );
    let fetch = Canned::new().html(&format!("{ORIGIN}/sitemap.xml"), &doc);
    let crawler = crawler(fetch, empty().await);

    let report = crawler
        .seed_from_sitemaps(ORIGIN, SitemapLimits::seeding())
        .await
        .expect("the store is in memory");
    assert_eq!(report.files, 1);
    assert_eq!(report.urls, 3);
    assert_eq!(report.admitted, 3);
    assert!(!report.truncated);
    assert!(crawler.fetcher().asked_for(&format!("{ORIGIN}/robots.txt")));
}

#[tokio::test]
async fn the_sitemap_line_in_robots_is_read() {
    let doc = urlset(&["https://example.com/news/1"], "2024-06-01");
    let fetch = Canned::new()
        .robots(
            ORIGIN,
            "User-agent: *\nSitemap: https://example.com/news.xml\n",
        )
        .html(&format!("{ORIGIN}/news.xml"), &doc);
    let crawler = crawler(fetch, empty().await);

    let report = crawler
        .seed_from_sitemaps(ORIGIN, SitemapLimits::seeding())
        .await
        .expect("the store is in memory");
    assert_eq!(report.admitted, 1);
    assert!(crawler.fetcher().asked_for(&format!("{ORIGIN}/news.xml")));
}

#[tokio::test]
async fn robots_is_asked_before_the_sitemap_and_is_obeyed() {
    // Doc 14.10 has no exception for a sitemap. A site that disallows the path
    // its own robots.txt points at is confused, and the answer to a confused
    // site is still the one it wrote down.
    let doc = urlset(&["https://example.com/a"], "2024-01-01");
    let fetch = Canned::new()
        .robots(ORIGIN, "User-agent: *\nDisallow: /sitemap.xml\n")
        .html(&format!("{ORIGIN}/sitemap.xml"), &doc);
    let crawler = crawler(fetch, empty().await);

    let report = crawler
        .seed_from_sitemaps(ORIGIN, SitemapLimits::seeding())
        .await
        .expect("the store is in memory");
    assert_eq!(report.disallowed, 1);
    assert_eq!(report.files, 0);
    assert_eq!(report.admitted, 0);
    assert!(
        !crawler
            .fetcher()
            .asked_for(&format!("{ORIGIN}/sitemap.xml"))
    );
}

#[tokio::test]
async fn an_index_is_followed_to_the_documented_depth_and_no_further() {
    // A chain of indexes one deeper than the limit. The last one is the one
    // that must not be fetched, and it is the only place a URL lives, so the
    // count of admitted URLs is the assertion rather than a fetch log.
    let mut fetch = Canned::new().html(
        &format!("{ORIGIN}/sitemap.xml"),
        &index(&[&format!("{ORIGIN}/i1.xml")]),
    );
    for level in 1..=u32::from(MAX_DEPTH) {
        let next = format!("{ORIGIN}/i{}.xml", level + 1);
        fetch = fetch.html(&format!("{ORIGIN}/i{level}.xml"), &index(&[&next]));
    }
    let deepest = format!("{ORIGIN}/i{}.xml", u32::from(MAX_DEPTH) + 1);
    fetch = fetch.html(
        &deepest,
        &urlset(&["https://example.com/buried"], "2024-01-01"),
    );
    let crawler = crawler(fetch, empty().await);

    let report = crawler
        .seed_from_sitemaps(ORIGIN, SitemapLimits::seeding())
        .await
        .expect("the store is in memory");
    assert_eq!(report.admitted, 0, "the depth limit did not hold");
    assert!(!crawler.fetcher().asked_for(&deepest));
    // The root plus one document per level of depth allowed.
    assert_eq!(report.files, u32::from(MAX_DEPTH) + 1);
}

#[tokio::test]
async fn two_indexes_that_point_at_each_other_cost_two_fetches() {
    // Not a depth limit question. Without the visited set this walks the pair
    // until the depth runs out, which is four fetches of two files, and on a
    // real index of a hundred files it is a hundred times worse.
    let fetch = Canned::new()
        .html(
            &format!("{ORIGIN}/sitemap.xml"),
            &index(&[&format!("{ORIGIN}/other.xml")]),
        )
        .html(
            &format!("{ORIGIN}/other.xml"),
            &index(&[&format!("{ORIGIN}/sitemap.xml")]),
        );
    let crawler = crawler(fetch, empty().await);

    let report = crawler
        .seed_from_sitemaps(ORIGIN, SitemapLimits::seeding())
        .await
        .expect("the store is in memory");
    assert_eq!(report.files, 2);
}

#[tokio::test]
async fn a_sitemap_on_another_origin_is_counted_and_not_fetched() {
    let elsewhere = "https://cdn.example.net/sitemap.xml";
    let fetch = Canned::new()
        .html(&format!("{ORIGIN}/sitemap.xml"), &index(&[elsewhere]))
        .html(elsewhere, &urlset(&["https://example.com/a"], "2024-01-01"));
    let crawler = crawler(fetch, empty().await);

    let report = crawler
        .seed_from_sitemaps(ORIGIN, SitemapLimits::seeding())
        .await
        .expect("the store is in memory");
    assert_eq!(report.off_origin, 1);
    assert_eq!(report.admitted, 0);
    assert!(!crawler.fetcher().asked_for(elsewhere));
}

#[tokio::test]
async fn a_url_cap_stops_the_walk_and_says_so() {
    let doc = urlset(
        &[
            "https://example.com/a",
            "https://example.com/b",
            "https://example.com/c",
            "https://example.com/d",
        ],
        "2024-01-01",
    );
    let fetch = Canned::new().html(&format!("{ORIGIN}/sitemap.xml"), &doc);
    let crawler = crawler(fetch, empty().await);

    let limits = SitemapLimits {
        max_urls: 2,
        ..SitemapLimits::seeding()
    };
    let report = crawler
        .seed_from_sitemaps(ORIGIN, limits)
        .await
        .expect("the store is in memory");
    assert!(report.truncated);
    assert_eq!(report.urls, 2);
    assert_eq!(report.admitted, 2);
}

#[tokio::test]
async fn a_feed_listed_as_a_sitemap_is_read_as_a_feed() {
    // Sites do this often enough that giving up would lose real URLs, and the
    // fallback is safe because it only runs when the sitemap reader found
    // nothing at all.
    let feed = "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel>\
                <item><link>https://example.com/post/1</link>\
                <pubDate>Mon, 03 Jun 2024 09:00:00 GMT</pubDate></item>\
                </channel></rss>";
    let fetch = Canned::new()
        .robots(
            ORIGIN,
            "User-agent: *\nSitemap: https://example.com/rss.xml\n",
        )
        .html(&format!("{ORIGIN}/rss.xml"), feed);
    let crawler = crawler(fetch, empty().await);

    let report = crawler
        .seed_from_sitemaps(ORIGIN, SitemapLimits::seeding())
        .await
        .expect("the store is in memory");
    assert_eq!(report.admitted, 1);
}

#[tokio::test]
async fn a_missing_sitemap_is_not_an_error() {
    // The ordinary case on most of the web. Nothing at `/sitemap.xml`, nothing
    // in robots.txt, and the crawl carries on with the seeds it has.
    let crawler = crawler(Canned::new(), empty().await);
    let report = crawler
        .seed_from_sitemaps(ORIGIN, SitemapLimits::seeding())
        .await
        .expect("a 404 is not an error");
    assert_eq!(report.files, 0);
    assert_eq!(report.admitted, 0);
}

#[tokio::test]
async fn a_date_later_than_our_last_fetch_brings_a_known_url_forward() {
    // The whole point of keeping `lastmod`. The URL is already in the store
    // and already fetched, so it is not admitted again; what changes is when
    // it is next due, and the report says so.
    let url = "https://example.com/moves";
    let state = empty().await;
    let admitted = state
        .admit(&[umi_state::Candidate::new(url, T0).expect("a crawlable url")])
        .await
        .expect("the store is in memory");
    assert_eq!(admitted.admitted, 1);

    let leases = state
        .lease(&umi_state::LeaseRequest {
            fetcher: umi_types::FetcherId::LOCAL,
            now_ms: T0,
            max_urls: 4,
            max_per_host: 4,
            max_tier: umi_types::Tier::Plain,
            lease_for: std::time::Duration::from_secs(60),
            plds: &[],
            budget: umi_state::Budget::DEFAULT,
        })
        .await
        .expect("the store is in memory");
    let lease = leases.first().expect("the url was leased");
    state
        .complete(&[umi_state::FetchOutcome {
            lease: lease.id,
            key: lease.key,
            finished_ms: T0 + 500,
            tier_used: umi_types::Tier::Plain,
            pace: umi_state::Pace::default(),
            result: umi_state::FetchResult::Fetched {
                status: 200,
                content_hash: [1u8; 8],
                revalidate: umi_state::Revalidator::default(),
            },
        }])
        .await
        .expect("the store is in memory");

    // A sitemap dated after that fetch, served now.
    let doc = urlset(&[url], "2100-01-01");
    let fetch = Canned::new().html(&format!("{ORIGIN}/sitemap.xml"), &doc);
    let crawler = crawler(fetch, Arc::clone(&state));
    let report = crawler
        .seed_from_sitemaps(ORIGIN, SitemapLimits::polling())
        .await
        .expect("the store is in memory");
    assert_eq!(report.admitted, 0, "a known url was admitted again");
    assert_eq!(report.refreshed, 1, "the date did not move the schedule");
}

#[tokio::test]
async fn a_profile_can_take_the_well_known_path_out_and_keep_the_robots_lines() {
    // Doc 13.4's `seed.sitemaps = false` with `robots_sitemaps` left alone.
    // The site advertises a sitemap and also has one at the usual place, and
    // only the advertised one is fetched.
    let doc = urlset(&["https://example.com/news/1"], "2024-06-01");
    let fetch = Canned::new()
        .robots(
            ORIGIN,
            "User-agent: *\nSitemap: https://example.com/news.xml\n",
        )
        .html(&format!("{ORIGIN}/news.xml"), &doc)
        .html(
            &format!("{ORIGIN}/sitemap.xml"),
            &urlset(&["https://example.com/a"], "2024-01-01"),
        );
    let crawler = crawler(fetch, empty().await);

    let limits = SitemapLimits {
        well_known: false,
        ..SitemapLimits::seeding()
    };
    let report = crawler
        .seed_from_sitemaps(ORIGIN, limits)
        .await
        .expect("the store is in memory");
    assert_eq!(report.files, 1);
    assert_eq!(report.admitted, 1);
    assert!(crawler.fetcher().asked_for(&format!("{ORIGIN}/news.xml")));
    assert!(
        !crawler
            .fetcher()
            .asked_for(&format!("{ORIGIN}/sitemap.xml")),
        "the well known path was fetched after the profile turned it off"
    );
}

#[tokio::test]
async fn a_profile_can_take_the_robots_lines_out_and_keep_the_well_known_path() {
    let doc = urlset(&["https://example.com/a"], "2024-01-01");
    let fetch = Canned::new()
        .robots(
            ORIGIN,
            "User-agent: *\nSitemap: https://example.com/news.xml\n",
        )
        .html(
            &format!("{ORIGIN}/news.xml"),
            &urlset(&["https://example.com/news/1"], "2024-06-01"),
        )
        .html(&format!("{ORIGIN}/sitemap.xml"), &doc);
    let crawler = crawler(fetch, empty().await);

    let limits = SitemapLimits {
        from_robots: false,
        ..SitemapLimits::seeding()
    };
    let report = crawler
        .seed_from_sitemaps(ORIGIN, limits)
        .await
        .expect("the store is in memory");
    assert_eq!(report.files, 1);
    assert_eq!(report.admitted, 1);
    assert!(!crawler.fetcher().asked_for(&format!("{ORIGIN}/news.xml")));
}

#[tokio::test]
async fn turning_both_off_sends_no_requests_at_all() {
    // Not even robots.txt. The pass exists to fetch sitemaps and a pass with
    // no sitemaps to fetch has nothing to ask anybody.
    let fetch = Canned::new().html(
        &format!("{ORIGIN}/sitemap.xml"),
        &urlset(&["https://example.com/a"], "2024-01-01"),
    );
    let crawler = crawler(fetch, empty().await);

    let limits = SitemapLimits {
        from_robots: false,
        well_known: false,
        ..SitemapLimits::seeding()
    };
    let report = crawler
        .seed_from_sitemaps(ORIGIN, limits)
        .await
        .expect("the store is in memory");
    assert_eq!(report, crate::SitemapReport::default());
    assert!(!crawler.fetcher().asked_for(&format!("{ORIGIN}/robots.txt")));
}
