//! What the loop costs, against gate 1.1's 250 pages a second.
//!
//! `rows.rs` measures one row. This measures everything around it: the lease,
//! the robots decision, the extract, the row, the links back into the frontier
//! and the completions. Those are the parts nobody budgets for, and a loop that
//! spends a millisecond per page on bookkeeping has spent a quarter of gate
//! 1.1's budget before it has parsed anything.
//!
//! Nothing here opens a socket. The fetcher answers from memory, either
//! instantly or after a sleep drawn from a fixed latency distribution, which is
//! the only way to measure the loop rather than the internet. It also means the
//! numbers are the loop's ceiling and not a prediction of what a server does:
//! a real fetch costs TLS, HTTP parsing and a kernel round trip on top.
//!
//! The state layer here is `MemoryState`, so the lease and complete numbers are
//! a floor rather than the shipping default. Doc 08 makes SQLite the default
//! backend and a round trip to it is not free, which is why part 1b keeps that
//! cost on its own line instead of folding it into the total.
//!
//! Run it pinned, since an unpinned run on a machine that is also crawling
//! measures the scheduler:
//!
//! ```text
//! taskset -c 5 chrt --fifo 50 ./target/release/deps/tick-<hash> --bench
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use umi_crawl::clock::FixedClock;
use umi_crawl::fetch::Fetch;
use umi_crawl::page::PageRow;
use umi_crawl::robots::{Entry, TTL_MS};
use umi_crawl::run::{CrawlConfig, CrawlError, Crawler, Sink, TickReport};
use umi_fetch::outcome::{Page, Version};
use umi_fetch::{FetchError, Media, Outcome};
use umi_state::{Budget, Candidate, MemoryState, State};
use umi_types::{FetcherId, Revalidator, RowKey, Tier};

mod support;

use support::{MEDIAN_HTML, Run, best_of, html_of};

const T0: u64 = 1_760_000_000_000;

/// How many distinct bodies to generate.
///
/// More than one so the sketch and the digest see different bytes, and few
/// enough that generating them is not most of the benchmark. The loop does not
/// care which page it got.
const BODIES: usize = 32;

fn main() {
    println!("the doc 03 crawl loop, 150 KB pages, no network\n");

    let bodies: Vec<String> = (0..BODIES).map(|i| html_of(i, MEDIAN_HTML)).collect();
    println!(
        "input: {:.1} KB of html per page, {BODIES} distinct pages, every host \
         allow-all",
        bodies[0].len() as f64 / 1024.0
    );
    println!();

    let one_core = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let all_cores = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let instant = Arc::new(Wire::new(&bodies, false));

    // Part 1. No latency at all, so what is left is the loop's own work: the
    // lease round trip, the robots lookup, the extract, the row, the candidate
    // canonicalisation and the completions.
    println!("part 1: the loop's own cost, instant fetches, {CPU_HOSTS} hosts x {CPU_TICKS} ticks");
    println!(
        "{:<40}{:>12}{:>14}{:>12}",
        "runtime", "us/page", "pages/s", "of budget"
    );

    let cpu = best_of(
        3,
        || one_core.block_on(seeded(CPU_HOSTS)),
        |state| {
            one_core.block_on(async {
                let crawler = crawler(Arc::clone(&instant), state, 128, CPU_HOSTS);
                prime_robots(&crawler, CPU_HOSTS).await;
                run(&crawler, CPU_TICKS).await.rows
            })
        },
    );
    line("current_thread, one core", cpu);

    let spread = best_of(
        3,
        || all_cores.block_on(seeded(CPU_HOSTS)),
        |state| {
            all_cores.block_on(async {
                let crawler = crawler(Arc::clone(&instant), state, 128, CPU_HOSTS);
                prime_robots(&crawler, CPU_HOSTS).await;
                run(&crawler, CPU_TICKS).await.rows
            })
        },
    );
    line("multi_thread, every core", spread);

    println!();
    println!(
        "the two lines above are the same number and that is the finding. A tick\n\
         polls its futures on one task, so the extract and the row of every page\n\
         in the batch run on the core that called tick. A worker pool does not\n\
         help until the loop spawns, and a server reaches gate 1.1 by running one\n\
         of these per core rather than by giving one of them more cores."
    );

    // Part 1b. Five milliseconds a page is a number nobody can act on, so
    // account for it. The row builder is 1.47 ms of it and rows.rs measures
    // that; the two stages below are the ones no other benchmark covers.
    let base = url::Url::parse(&seed_url(0)).expect("parse");
    let extracted: Vec<_> = bodies
        .iter()
        .map(|body| umi_extract::extract(body.as_bytes(), &base))
        .collect();
    let links_per_page = extracted[0].links.links.len();

    println!();
    println!("part 1b: where a page's 5 ms goes, {links_per_page} links per page");
    println!(
        "{:<40}{:>12}{:>14}{:>12}",
        "stage", "us/page", "pages/s", "of budget"
    );

    let parse = best_of(
        5,
        || (),
        |()| {
            for body in &bodies {
                std::hint::black_box(umi_extract::extract(body.as_bytes(), &base));
            }
            bodies.len()
        },
    );
    line("umi_extract::extract", parse);

    let canon = best_of(
        5,
        || (),
        |()| {
            for e in &extracted {
                for link in &e.links.links {
                    std::hint::black_box(RowKey::for_url(&link.url, None).ok());
                }
            }
            extracted.len()
        },
    );
    line("RowKey::for_url over its links", canon);

    let accounted = parse.per_item() + canon.per_item();
    println!(
        "{:<40}{:>12.0}{:>14}{:>12}",
        "PageRow::build, from rows.rs", 1470.0, "", ""
    );
    println!(
        "{:<40}{:>12.0}{:>14}{:>12}",
        "lease, dedup, complete, the rest",
        (cpu.per_item().as_secs_f64() - accounted.as_secs_f64()) * 1e6 - 1470.0,
        "",
        ""
    );

    // Part 2. Now with the web's latency in it. The question is how big the
    // window has to be, and where making it bigger stops helping.
    println!();
    println!(
        "part 2: one tick over {WIRE_HOSTS} hosts with latency, 40 ms typical,\n\
         200 ms for one host in ten, 2000 ms for one in a hundred"
    );
    println!("{:<40}{:>12}{:>14}", "in flight", "ms/tick", "pages/s");

    let slow = Arc::new(Wire::new(&bodies, true));
    for window in [8, 32, 128, WIRE_HOSTS] {
        let measured = best_of(
            1,
            || one_core.block_on(seeded(WIRE_HOSTS)),
            |state| {
                one_core.block_on(async {
                    let crawler = crawler(Arc::clone(&slow), state, window, WIRE_HOSTS);
                    prime_robots(&crawler, WIRE_HOSTS).await;
                    run(&crawler, 1).await.rows
                })
            },
        );
        line_ms(&format!("{window} of {WIRE_HOSTS}"), measured);
    }

    println!();
    println!(
        "past a window of about 32 the tick stops getting faster, because what is\n\
         left is the one host that takes two seconds. That floor is the tick's\n\
         slowest single fetch and it is the number part 3 is about."
    );

    // Part 3. The claim in run.rs's module doc, measured. Same latencies, same
    // window, two ways of spending it.
    println!();
    println!("part 3: unordered against a barrier every {SHAPE_WINDOW}, {SHAPE_ITEMS} fetches");
    println!("{:<40}{:>12}{:>14}", "shape", "ms", "pages/s");

    let latencies: Vec<Duration> = (0..SHAPE_ITEMS).map(latency_of).collect();
    let no_barrier = best_of(
        1,
        || (),
        |()| {
            one_core.block_on(unordered(&latencies, SHAPE_WINDOW));
            latencies.len()
        },
    );
    line_ms("FuturesUnordered, topped up", no_barrier);
    let barrier = best_of(
        1,
        || (),
        |()| {
            one_core.block_on(chunked(&latencies, SHAPE_WINDOW));
            latencies.len()
        },
    );
    line_ms("join over fixed chunks", barrier);

    println!();
    println!(
        "the barrier pays the slow host once per chunk and unordered pays it once\n\
         for the whole tick, which here is {:.1}x. That is why the loop keeps the\n\
         window full rather than joining chunks, and the gap grows with the batch.",
        barrier.elapsed.as_secs_f64() / no_barrier.elapsed.as_secs_f64().max(1e-9)
    );

    // Part 4. Doc 13.2 says a scope is evaluated per candidate during
    // admission, and doc 13's own number for admission is 12500 candidates a
    // second. At 250 pages a second and 140 links a page the loop offers 35000
    // candidates a second, so a scope has to be cheaper than that or focused
    // mode is slower than general mode for a reason nobody would guess.
    let candidates: Vec<&str> = extracted
        .iter()
        .flat_map(|e| e.links.links.iter().map(|l| l.url.as_str()))
        .collect();

    println!();
    println!(
        "part 4: Scope::allows over {} real candidates from those pages",
        candidates.len()
    );
    println!(
        "{:<40}{:>12}{:>14}{:>12}",
        "scope", "ns/url", "urls/s", "of 35k"
    );

    for (name, scope) in scopes() {
        let measured = best_of(
            5,
            || (),
            |()| {
                for url in &candidates {
                    // Exactly what `Crawler::follow` asks, including the check
                    // that skips the parse. Measuring `allows` on its own would
                    // charge the general crawl for a URL parse it never does.
                    std::hint::black_box(!scope.filters_links() || scope.allows(url));
                }
                candidates.len()
            },
        );
        println!(
            "{:<40}{:>12.0}{:>14.0}{:>11.1}%",
            name,
            measured.per_item().as_secs_f64() * 1e9,
            measured.per_second(),
            35_000.0 / measured.per_second() * 100.0,
        );
    }

    println!();
    println!(
        "the URL parse is the whole cost. A matcher runs on a host string and a\n\
         path that url::Url already has, so sixteen matchers cost about what one\n\
         costs and the first one costs everything. The general crawl does not\n\
         parse at all, which is what Scope::filters_links is for, and a focused\n\
         crawl spends around two percent of a core on staying inside its scope."
    );
}

/// The scopes part 4 measures, from cheapest to most expensive.
fn scopes() -> Vec<(&'static str, umi_crawl::Scope)> {
    let host_suffix = umi_crawl::Scope::for_target("example.com").expect("target");
    let regex = umi_crawl::Scope::from_toml(
        "name = \"regex\"\ninclude = [{ url_regex = \"https://h[0-9]+\\\\.example/\" }]",
    )
    .expect("profile");
    let many = umi_crawl::Scope::from_toml(&format!(
        "name = \"many\"\ninclude = [{}]",
        (0..16)
            .map(|n| format!("{{ host_suffix = \"h{n}.example\" }}"))
            .collect::<Vec<_>>()
            .join(", ")
    ))
    .expect("profile");
    vec![
        (
            "general, no include and no exclude",
            umi_crawl::Scope::general(),
        ),
        ("one host_suffix", host_suffix),
        ("16 host_suffix, all of them tried", many),
        ("one url_regex", regex),
    ]
}

/// Hosts for part 1. Enough that a tick is a real batch, few enough that a
/// run is a few seconds.
const CPU_HOSTS: usize = 256;

/// Ticks for part 1. Doc 07.6 hands out one URL per host per politeness
/// window, so this is also how many pages each host gives up.
const CPU_TICKS: usize = 4;

/// Hosts for part 2, which is bounded by sleeping rather than by work.
const WIRE_HOSTS: usize = 512;

const SHAPE_ITEMS: usize = 512;
const SHAPE_WINDOW: usize = 128;

fn line(name: &str, run: Run) {
    let per = run.per_item().as_secs_f64();
    println!(
        "{:<40}{:>12.0}{:>14.0}{:>11.1}%",
        name,
        per * 1e6,
        1.0 / per,
        250.0 * per * 100.0
    );
}

fn line_ms(name: &str, run: Run) {
    println!(
        "{:<40}{:>12.0}{:>14.0}",
        name,
        run.elapsed.as_secs_f64() * 1e3,
        run.per_second()
    );
}

/// The window kept full, which is what [`Crawler::tick`] does.
async fn unordered(latencies: &[Duration], window: usize) {
    use futures_util::StreamExt;
    use futures_util::stream::FuturesUnordered;

    let mut queue = latencies.iter();
    let mut pending = FuturesUnordered::new();
    for wait in queue.by_ref().take(window) {
        pending.push(tokio::time::sleep(*wait));
    }
    while pending.next().await.is_some() {
        if let Some(wait) = queue.next() {
            pending.push(tokio::time::sleep(*wait));
        }
    }
}

/// A barrier every `window`, which is the obvious implementation and the one
/// run.rs's module doc says not to write.
async fn chunked(latencies: &[Duration], window: usize) {
    for chunk in latencies.chunks(window) {
        futures_util::future::join_all(chunk.iter().map(|wait| tokio::time::sleep(*wait))).await;
    }
}

/// What a fetch costs on host `n`.
///
/// A property of the host and not of the URL, because that is how the web
/// behaves: a slow site is slow for every page on it, which is exactly the case
/// a barrier handles badly.
const fn latency_of(host: usize) -> Duration {
    match host % 100 {
        0 => Duration::from_millis(2000),
        1..=9 => Duration::from_millis(200),
        _ => Duration::from_millis(40),
    }
}

/// A fetcher made of bytes already in memory.
struct Wire {
    pages: Vec<Outcome>,
    robots: Outcome,
    latency: bool,
}

impl Wire {
    fn new(bodies: &[String], latency: bool) -> Self {
        Self {
            pages: bodies
                .iter()
                .map(|body| ok_page(body, Media::Html))
                .collect(),
            robots: ok_page("User-agent: *\nAllow: /\n", Media::Text),
            latency,
        }
    }
}

#[async_trait::async_trait]
impl Fetch for Wire {
    async fn fetch(
        &self,
        url: &str,
        _revalidate: Option<&Revalidator>,
    ) -> Result<Outcome, FetchError> {
        if self.latency {
            tokio::time::sleep(latency_of(host_index(url))).await;
        }
        let mut out = if url.ends_with("/robots.txt") {
            self.robots.clone()
        } else {
            self.pages[url.len() % self.pages.len()].clone()
        };
        // The base URL for link resolution comes off the page, so it has to be
        // the URL that was asked for or every link on it resolves to the wrong
        // host and the frontier fills up with pages that do not exist.
        if let Outcome::Ok(page) = &mut out {
            page.final_url = url.to_owned();
        }
        Ok(out)
    }
}

/// Which host a URL is on, by the number in its name.
fn host_index(url: &str) -> usize {
    let mut n = 0;
    let mut seen = false;
    for byte in url.trim_start_matches("https://").bytes() {
        if byte.is_ascii_digit() {
            n = n * 10 + usize::from(byte - b'0');
            seen = true;
        } else if seen || byte == b'/' {
            break;
        }
    }
    n
}

fn ok_page(body: &str, media: Media) -> Outcome {
    let bytes = bytes::Bytes::from(body.as_bytes().to_vec());
    Outcome::Ok(Box::new(Page {
        final_url: String::new(),
        status: 200,
        version: Version::Http2,
        redirects: Vec::new(),
        headers_kept: vec![("content-type".to_owned(), "text/html".to_owned())],
        headers_digest: [7u8; 32],
        content_type: Some("text/html; charset=utf-8".to_owned()),
        media,
        body_digest: *blake3::hash(&bytes).as_bytes(),
        body: bytes,
        revalidate: Revalidator::default(),
        elapsed: Duration::from_millis(40),
    }))
}

/// Rows counted and thrown away.
///
/// Writing them costs another 169 us a row, which part 3 of `rows.rs`
/// measures, and putting it here would mix the two numbers. What this
/// benchmark is for is everything the loop does that no other benchmark
/// covers.
#[derive(Default)]
struct Counter(AtomicUsize);

#[async_trait::async_trait]
impl Sink for Counter {
    async fn take(&self, rows: &[PageRow]) -> Result<(), CrawlError> {
        self.0.fetch_add(rows.len(), Ordering::Relaxed);
        Ok(())
    }
}

fn config(in_flight: usize, hosts: usize) -> CrawlConfig {
    CrawlConfig {
        fetcher: FetcherId::LOCAL,
        // A tick has to be able to hold every host that is due, or the window
        // sweep in part 2 measures the batch size instead.
        batch: u32::try_from(hosts * 2).unwrap_or(u32::MAX),
        in_flight,
        max_per_host: 4,
        max_tier: Tier::Plain,
        lease_for: Duration::from_secs(60),
        max_depth: umi_frontier::MAX_DEPTH,
        scope: Arc::new(umi_crawl::Scope::general()),
        budget: Budget::DEFAULT,
        rate: umi_frontier::Rate::default(),
        // Every host here is its own pay level domain, and the default is 64
        // of them a tick. Left at the default, part 2 would sweep a window of
        // 512 over 64 domains and measure the domain cap rather than the
        // window, which is the same trap the batch size above avoids.
        max_domains: hosts,
    }
}

fn crawler(
    wire: Arc<Wire>,
    state: Arc<dyn State>,
    in_flight: usize,
    hosts: usize,
) -> Crawler<Arc<Wire>, Arc<FixedClock>> {
    Crawler::new(
        wire,
        state,
        Arc::new(FixedClock::at(T0)),
        config(in_flight, hosts),
    )
}

fn seed_url(host: usize) -> String {
    format!("https://h{host}.example/0")
}

/// A frontier with one URL on each of `hosts` hosts.
async fn seeded(hosts: usize) -> Arc<dyn State> {
    let state = Arc::new(MemoryState::new());
    let urls: Vec<String> = (0..hosts).map(seed_url).collect();
    let batch: Vec<Candidate<'_>> = urls
        .iter()
        .map(|url| {
            let mut c = Candidate::new(url, T0).expect("a crawlable url");
            c.discovery = umi_state::Discovery::Seed;
            c
        })
        .collect();
    state.admit(&batch).await.expect("admit");
    state
}

/// Put an allow-all robots.txt in for every host.
///
/// A cold cache costs one extra fetch per host on the first tick, which is real
/// but is not what either part is measuring: part 1 would count 512 extracts
/// where 256 pages were crawled, and part 2 would sleep twice per page.
async fn prime_robots<F: Fetch>(crawler: &Crawler<F, Arc<FixedClock>>, hosts: usize) {
    let robots = Arc::new(umi_robots::Robots::for_status(
        200,
        b"User-agent: *\nAllow: /\n",
    ));
    for host in 0..hosts {
        let key = RowKey::for_url(&seed_url(host), None).expect("canonicalise");
        crawler
            .robots()
            .insert(
                key.host,
                Entry {
                    robots: Arc::clone(&robots),
                    fetched_ms: T0,
                    expires_ms: T0 + TTL_MS,
                },
            )
            .await;
    }
}

/// Tick `n` times, moving the clock on between them.
///
/// Doc 07.6 allows one request per host per adaptive delay, which starts at a
/// second, so a clock that does not move leases every host once and then
/// nothing. A daemon sleeps here; this is that sleep without the sleeping, and
/// it is why the clock advance sits outside the work rather than inside it.
async fn run<F: Fetch>(crawler: &Crawler<F, Arc<FixedClock>>, n: usize) -> TickReport {
    let sink = Counter::default();
    let mut total = TickReport::default();
    for i in 0..n {
        let report = crawler.tick(&sink).await.expect("tick");
        total.leased += report.leased;
        total.rows += report.rows;
        total.fetched += report.fetched;
        total.bytes_fetched += report.bytes_fetched;
        total.links_seen += report.links_seen;
        total.links_admitted += report.links_admitted;
        if i + 1 < n {
            crawler.clock().advance(2000);
        }
    }
    assert_eq!(
        total.rows,
        sink.0.load(Ordering::Relaxed),
        "every row reaches the sink"
    );
    total
}
