//! What the default state backend costs, against doc 08.5's numbers.
//!
//! Doc 08.5 argues that SQLite is the right default up to a few hundred
//! million URLs and that `nami` takes over above that. The argument rests on
//! numbers, and until now none of them had been measured, which made this the
//! only crate on the hot path with no bar. It is on the hot path twice over:
//! doc 08.4 sizes the whole design around 12500 admissions a second, and
//! `lease` is called once per tick by every coordinator.
//!
//! ```text
//! operation      doc 08 says                       measured here
//! admit          12500 candidates a second         part 1
//! lease          1000 urls a call, index walk      part 2
//! complete       one call per tick, batched        part 3
//! stats          maintained counters, one row      part 4
//!                read, and the crawl loop's idle
//!                branch calls it
//! put_segment    durable, about 1000 a day         part 5
//! segments       partial index, not a table scan   part 5
//! resident       doc 09.8 rebuilds the domain      part 6
//!                schedule from it at startup
//! ```
//!
//! Part 4 is the one worth explaining. The crawl loop calls `stats` on every
//! idle tick, to tell pending urls waiting their turn apart from an empty
//! frontier, and doc 14.3's progress line calls it every five seconds. Both
//! are correct calls to make, so the cost has to stay flat as the ledger
//! grows. It used to be five `COUNT(*)` queries, which was 97 ms over 200000
//! urls on server3 and would have been 78 seconds at a hundred million, and
//! that is what schema version 3's counters replaced. The number this part
//! prints is now a single row read and the reason to keep printing it is to
//! notice the day it stops being one.
//!
//! Part 5 is doc 12.7's fourth GC condition. The interesting number is not the
//! rate, since a coordinator seals about a thousand segments a day and could
//! afford a second each. It is whether `segments(Collectable)` stays flat as
//! the table grows, because the collectable set shrinks while the table only
//! ever gets bigger, and a query that degraded into a table scan would get
//! slower every day for a year without anything looking wrong.
//!
//! Environment:
//!
//! * `UMI_BENCH_URLS` how many urls to fill the ledger with, default 200000
//! * `UMI_BENCH_SEGMENTS` how much segment history, default 365000, a year
//! * `UMI_BENCH_REPEAT` how many times to repeat the timed sections, default 3
//!
//! Run it pinned, because the numbers are shares of one core:
//!
//! ```text
//! cargo build --release -p umi-state-sqlite --benches
//! taskset -c 1 ./target/release/deps/state-<hash>
//! ```

use std::time::Instant;

use umi_state::{
    Candidate, Discovery, FetchOutcome, FetchResult, LeaseRequest, Pace, Priority, RemoteCopy,
    Revalidator, SegmentQuery, SegmentRow, State, Stream,
};
use umi_state_sqlite::SqliteState;
use umi_types::{Digest, FetcherId, RowKey, Tier, Ulid};

/// A fixed instant, so two runs are comparable. Doc 11.1's rule applies to a
/// benchmark as much as to the writer.
const T0: u64 = 1_700_000_000_000;

/// Doc 08.5's admission target, which part 1 is quoted against.
const ADMIT_TARGET: f64 = 12_500.0;

/// Doc 08.4's batch size, which every operation in the trait is shaped for.
const BATCH: usize = 1000;

/// How many hosts the urls are spread over. Real enough to matter: all the
/// per host indexes and the politeness join behave differently when a million
/// urls sit on one host than when they are spread thin, and doc 08.2's
/// `(pld, host, url_key)` ordering only pays off when there are many.
const HOSTS: usize = 2000;

fn main() {
    let urls = env("UMI_BENCH_URLS", 200_000);
    let history = env("UMI_BENCH_SEGMENTS", 365_000);
    let repeat = env("UMI_BENCH_REPEAT", 3);

    println!("umi-state-sqlite, {urls} urls over {HOSTS} hosts, best of {repeat}\n");

    let dir = tempfile::tempdir().expect("tempdir");
    let state = SqliteState::open(dir.path().join("state.umistate")).expect("a store");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("a runtime");

    runtime.block_on(async {
        admission(&state, urls, repeat).await;
        leasing(&state, repeat).await;
        completing(&state, repeat).await;
        counting(&state, urls, repeat).await;
        segments(&state, history, repeat).await;
        residency(repeat).await;
    });

    let bytes = std::fs::metadata(dir.path().join("state.umistate"))
        .map(|m| m.len())
        .unwrap_or(0);
    println!("part 7: the file, doc 01 targets under 20 bytes a known url");
    println!(
        "  {} urls, {} segment records, {:.1} MiB on disk, {:.1} B/url\n",
        urls,
        history,
        bytes as f64 / (1024.0 * 1024.0),
        bytes as f64 / urls as f64
    );
}

/// Part 1. Doc 08.5's 12500 candidates a second.
///
/// Timed on the first pass over each batch, because that is the pass that does
/// the work: the second pass over the same urls is the 95 percent already seen
/// case from doc 08.1, which is cheaper and is measured separately.
async fn admission(state: &SqliteState, urls: usize, repeat: usize) {
    println!("part 1: admit, doc 08.5 wants {ADMIT_TARGET:.0} candidates a second");
    println!(
        "  {:<24} {:>12} {:>14} {:>16}",
        "", "urls/s", "us per url", "core at target"
    );

    let all: Vec<String> = (0..urls).map(url).collect();
    let mut best = f64::MAX;
    for batch in all.chunks(BATCH) {
        let candidates: Vec<Candidate<'_>> = batch.iter().map(|u| candidate(u)).collect();
        let start = Instant::now();
        let report = state.admit(&candidates).await.expect("admit");
        let each = start.elapsed().as_secs_f64() / batch.len() as f64;
        best = best.min(each);
        assert_eq!(
            report.total() as usize,
            batch.len(),
            "every candidate lands"
        );
    }
    line("new urls", best);

    // The common case. Doc 08.1 says well over 95 percent of candidates are
    // already known, so this is the number that decides whether link admission
    // keeps up, not the one above.
    let mut seen = f64::MAX;
    for _ in 0..repeat {
        for batch in all.chunks(BATCH).take(20) {
            let candidates: Vec<Candidate<'_>> = batch.iter().map(|u| candidate(u)).collect();
            let start = Instant::now();
            state.admit(&candidates).await.expect("admit");
            seen = seen.min(start.elapsed().as_secs_f64() / batch.len() as f64);
        }
    }
    line("already seen", seen);
    println!();
}

/// Part 2. The scheduler's own call, once a tick.
async fn leasing(state: &SqliteState, repeat: usize) {
    println!("part 2: lease, one call a tick, doc 08.4 batches {BATCH}");
    println!(
        "  {:<24} {:>12} {:>14} {:>16}",
        "", "urls/s", "us per url", "core at 250/s"
    );

    let mut best = f64::MAX;
    let mut got = 0;
    for pass in 0..repeat {
        // Each pass moves the clock past the politeness window, so the second
        // pass is not measuring an empty result.
        let now = T0 + (pass as u64 + 1) * 60_000;
        let start = Instant::now();
        let leases = state
            .lease(&LeaseRequest {
                max_tier: Tier::Rendered,
                ..LeaseRequest::new(FetcherId::LOCAL, now, BATCH as u32)
            })
            .await
            .expect("lease");
        if leases.is_empty() {
            continue;
        }
        got = leases.len();
        best = best.min(start.elapsed().as_secs_f64() / leases.len() as f64);
        // Hand them back, so the next pass has the same work to do.
        let ids: Vec<_> = leases.iter().map(|l| l.id).collect();
        state
            .release(&ids, umi_state::NackReason::Refused)
            .await
            .expect("release");
    }
    println!("  {:<24} {got:>12} urls in the batch", "");
    at_rate("lease", best);
    println!();
}

/// Part 3. What a tick pays to record what it fetched.
///
/// Twice, because the order a batch arrives in is not a detail here. The
/// ledger is `WITHOUT ROWID` and keyed by `(pld, host, url_key)`, so the rows
/// are stored in that order and a batch that walks them in it reads and
/// writes each leaf page once, in file order. `lease` hands its answer back
/// in index order, so a bench that completes exactly what it just leased is
/// measuring the best case the table has and printing it as the number.
///
/// The crawl never sees that case. Completions come back in the order origins
/// answered, which against a blake3 derived key is noise, and every one of
/// them is a seek to a leaf nothing else in the batch is near. Both lines are
/// printed because the gap between them is the thing worth watching: it is
/// small while the ledger fits in the page cache and it is the whole cost
/// once it does not.
async fn completing(state: &SqliteState, repeat: usize) {
    println!("part 3: complete, one call a tick");
    println!(
        "  {:<24} {:>12} {:>14} {:>16}",
        "", "urls/s", "us per url", "core at 250/s"
    );

    for shuffled in [false, true] {
        completions(state, repeat, shuffled).await;
    }
    println!();
}

/// One of part 3's two lines.
async fn completions(state: &SqliteState, repeat: usize, shuffled: bool) {
    let mut best = f64::MAX;
    for pass in 0..repeat {
        // Every pass gets a minute of its own, the second line's passes after
        // the first line's. A `complete` only counts if it is newer than the
        // answer already on the row, so two passes sharing a clock would have
        // the second one do nothing and time it.
        let nth = pass + if shuffled { repeat } else { 0 };
        let now = T0 + (nth as u64 + 10) * 60_000;
        let leases = state
            .lease(&LeaseRequest {
                max_tier: Tier::Rendered,
                ..LeaseRequest::new(FetcherId::LOCAL, now, BATCH as u32)
            })
            .await
            .expect("lease");
        if leases.is_empty() {
            continue;
        }
        let mut outcomes: Vec<FetchOutcome> = leases
            .iter()
            .enumerate()
            .map(|(n, lease)| FetchOutcome {
                lease: lease.id,
                key: lease.key,
                finished_ms: now,
                tier_used: Tier::Plain,
                // A latency, so doc 07.6's rate limiter actually runs. It
                // reads and writes a host row per host in the batch and that
                // cost belongs in the number, not outside it.
                pace: Pace {
                    latency_ms: Some(120 + (n % 400) as u32),
                    retry_after_ms: None,
                },
                result: FetchResult::Fetched {
                    status: 200,
                    // Half the pages changed, so both branches of
                    // `next_due_after` are exercised rather than one.
                    content_hash: [(n % 2) as u8; 8],
                    // A real one, because interning an ETag is part of what
                    // `complete` costs and half of them repeat, which is what
                    // the pool exists for.
                    revalidate: Revalidator {
                        etag: Some(format!("\"{}\"", n % 500)),
                        last_modified_ms: Some(now - 3_600_000),
                    },
                },
            })
            .collect();
        if shuffled {
            scatter(&mut outcomes, nth as u64 + 1);
        }
        let start = Instant::now();
        state.complete(&outcomes).await.expect("complete");
        best = best.min(start.elapsed().as_secs_f64() / outcomes.len() as f64);
    }
    at_rate(
        if shuffled {
            "complete, fetch order"
        } else {
            "complete, key order"
        },
        best,
    );
}

/// Shuffle in place, the same way on every run.
///
/// A Fisher Yates with a xorshift behind it rather than a real generator. The
/// only two things asked of it are that the order stop matching the key and
/// that two runs somebody is comparing get the same one.
fn scatter<T>(items: &mut [T], seed: u64) {
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    for at in (1..items.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        items.swap(at, (state % (at as u64 + 1)) as usize);
    }
}

/// Part 4. What doc 14.3's progress line and the crawl loop's idle branch pay.
async fn counting(state: &SqliteState, urls: usize, repeat: usize) {
    println!("part 4: stats, the maintained counters over {urls} urls");

    let mut best = f64::MAX;
    for _ in 0..repeat {
        let start = Instant::now();
        state.stats().await.expect("stats");
        best = best.min(start.elapsed().as_secs_f64());
    }
    println!(
        "  {:<24} {:>10.2} ms   {:>8.2} us per url   {:.2}% of a core if called once a second",
        "one call",
        best * 1000.0,
        best * 1e6 / urls as f64,
        best * 100.0
    );
    println!();
}

/// Part 5. Doc 12.7's fourth condition, and whether it stays flat.
async fn segments(state: &SqliteState, history: usize, repeat: usize) {
    println!("part 5: segments, doc 12.7 condition 4 and doc 12.8's window");

    // A year of history, almost all of it published and collected, which is
    // what the table looks like on a box that has been running. The last
    // handful are live, which is the set the queries have to find.
    let mut best_put = f64::MAX;
    for chunk in (0..history).collect::<Vec<_>>().chunks(BATCH) {
        let rows: Vec<SegmentRow> = chunk.iter().map(|n| old(*n)).collect();
        let start = Instant::now();
        state.put_segment(&rows).await.expect("put_segment");
        best_put = best_put.min(start.elapsed().as_secs_f64() / rows.len() as f64);
    }
    println!(
        "  {:<24} {:>10.2} us per row, batched {BATCH} at a time, durable",
        "put_segment",
        best_put * 1e6
    );

    // One at a time is what actually happens: a segment seals, and one record
    // is written. That is the fsync, undivided.
    let mut single = f64::MAX;
    for n in 0..repeat {
        let row = live(history + n);
        let start = Instant::now();
        state.put_segment(&[row]).await.expect("put_segment");
        single = single.min(start.elapsed().as_secs_f64());
    }
    println!(
        "  {:<24} {:>10.2} ms, which is the fsync, once per sealed segment",
        "put_segment, one row",
        single * 1000.0
    );

    for (name, query) in [
        ("unpublished", SegmentQuery::Unpublished),
        ("collectable", SegmentQuery::Collectable),
        (
            "a day of history",
            SegmentQuery::SealedBetween {
                from_ms: T0,
                to_ms: T0 + 86_400_000,
            },
        ),
    ] {
        let mut best = f64::MAX;
        let mut found = 0;
        for _ in 0..repeat {
            let start = Instant::now();
            let rows = state.segments(query).await.expect("segments");
            best = best.min(start.elapsed().as_secs_f64());
            found = rows.len();
        }
        println!(
            "  {:<24} {:>10.3} ms over {history} records, {found} rows back",
            name,
            best * 1000.0
        );
    }
    println!();
}

fn line(what: &str, each: f64) {
    let rate = 1.0 / each;
    println!(
        "  {:<24} {:>12.0} {:>14.2} {:>15.2}%",
        what,
        rate,
        each * 1e6,
        ADMIT_TARGET * each * 100.0
    );
}

fn at_rate(what: &str, each: f64) {
    let rate = 1.0 / each;
    println!(
        "  {:<24} {:>12.0} {:>14.2} {:>15.2}%",
        what,
        rate,
        each * 1e6,
        250.0 * each * 100.0
    );
}

/// Part 6. Doc 09.8's restart, which rebuilds the domain rate limits from
/// [`State::resident`].
///
/// The thing being measured is the shape of the cost rather than the cost. A
/// crawl gets deeper over its life without getting much wider, so an answer
/// priced per url gets slower forever while an answer priced per domain settles.
/// Both shapes below hold the same number of urls, so if the two rows are far
/// apart the query is reading urls, and if they are close it is seeking between
/// domains the way it is meant to.
async fn residency(repeat: usize) {
    println!("part 6: resident, doc 09.8's restart rebuilds the schedule from this");
    println!(
        "  {:<24} {:>12} {:>14} {:>16}",
        "", "domains", "ms per call", "us per domain"
    );

    // 100000 urls each way. Wide is a crawl that has just been seeded from a
    // domain list and gone one hop; deep is the same crawl a month later.
    for (label, domains, per_domain) in [("wide", 50_000usize, 2usize), ("deep", 50, 2000)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let state = SqliteState::open(dir.path().join("state.umistate")).expect("a store");
        let all: Vec<String> = (0..domains * per_domain)
            .map(|n| format!("https://www.d{}.example/p/{n}", n % domains))
            .collect();
        for batch in all.chunks(BATCH) {
            let candidates: Vec<Candidate<'_>> = batch.iter().map(|u| candidate(u)).collect();
            state.admit(&candidates).await.expect("admit");
        }

        let mut best = f64::MAX;
        let mut found = 0;
        for _ in 0..repeat {
            let start = Instant::now();
            let resident = state.resident().await.expect("resident");
            best = best.min(start.elapsed().as_secs_f64());
            found = resident.len();
        }
        assert_eq!(found, domains, "every admitted domain is local");
        println!(
            "  {label:<24} {found:>12} {:>14.2} {:>16.2}",
            best * 1000.0,
            best * 1e6 / found as f64
        );
    }
    println!();
}

fn url(n: usize) -> String {
    format!("https://h{}.example.com/p/{n}", n % HOSTS)
}

fn candidate(url: &str) -> Candidate<'_> {
    Candidate {
        key: RowKey::for_url(url, None).expect("a crawlable url"),
        url,
        depth: 1,
        priority: Priority::DEFAULT,
        discovered_ms: T0,
        discovery: Discovery::Trusted,
        lastmod_ms: None,
    }
}

/// A segment sealed, published, listed in a manifest and already collected.
fn old(n: usize) -> SegmentRow {
    SegmentRow {
        remote: Some(RemoteCopy {
            repo: "open-index/umi-pages-2026w34-01".to_owned(),
            path: format!("data/20260825/{n}.parquet"),
            digest: Digest::from_bytes([2; 32]),
        }),
        manifest_day: Some(20_260_825),
        deleted_at_ms: Some(T0 + n as u64 * 1000 + 600_000),
        ..live(n)
    }
}

/// A segment that has just sealed and is still on disk.
fn live(n: usize) -> SegmentRow {
    let sealed_at_ms = T0 + n as u64 * 1000;
    SegmentRow {
        id: Ulid::new(sealed_at_ms, [(n % 251) as u8; 10]),
        stream: Stream::Pages,
        local_path: format!("./crawl/segments/{n}.umi"),
        sealed_at_ms,
        rows: 118_671,
        bytes: 128 << 20,
        local_digest: Digest::from_bytes([1; 32]),
        remote: None,
        manifest_day: None,
        deleted_at_ms: None,
    }
}

fn env(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}
