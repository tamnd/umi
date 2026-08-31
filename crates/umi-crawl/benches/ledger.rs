//! What the doc 05.7 supervised ledger costs, on the batch that has nothing
//! supervised in it.
//!
//! The ledger is an audit file for a tier that is meant to be rare, so almost
//! every batch it ever sees will contain no T4 rows at all. That makes the
//! number worth having the scan on a batch that finds nothing, because that is
//! the one the crawl pays on every tick for the rest of its life. A record that
//! costs a percent of a page's budget to not write anything is a record nobody
//! should ship.
//!
//! The other two numbers are here for scale rather than for a gate. A batch
//! that is entirely T4 is not a shape a real crawl produces, and the decorator
//! comparison exists so the cost of wrapping a sink can be read next to the cost
//! of the sink.
//!
//! Same house rules as the other benchmarks here: no criterion, best of five,
//! run it pinned.
//!
//! ```text
//! taskset -c 5 chrt --fifo 50 ./target/release/deps/ledger-<hash> --bench
//! ```

use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use umi_crawl::page::PageRow;
use umi_crawl::run::{CrawlError, Sink};
use umi_crawl::{Crawled, Recorded, SupervisedLedger};
use umi_extract::extract;
use umi_fetch::Outcome;
use umi_fetch::outcome::{Page, Version};
use umi_types::{FetcherId, Revalidator, RowKey, Tier, Verification};

mod support;

use support::{MEDIAN_HTML, Run, best, best_of, html_of};

const T0: u64 = 1_760_000_000_000;

/// How many rows in a batch.
///
/// The sink takes one tick's rows at a time and a tick runs a few hundred
/// leases, so this is the right order of magnitude. It also has to be large
/// enough that the per row number is not the stopwatch.
const BATCH: usize = 1024;

fn main() {
    println!("the doc 05.7 supervised ledger, best of 5\n");

    let bodies: Vec<String> = (0..8).map(|i| html_of(i, MEDIAN_HTML)).collect();
    let base = url::Url::parse("https://example.com/article").expect("parse");
    let rows: Vec<PageRow> = bodies
        .iter()
        .map(|body| {
            let outcome = ok_page(body);
            let e = extract(body.as_bytes(), &base);
            PageRow::build(&crawled(&outcome, &e, Tier::Plain))
        })
        .collect();
    let plain: Vec<PageRow> = (0..BATCH).map(|i| rows[i % rows.len()].clone()).collect();

    let mut supervised = plain.clone();
    for row in &mut supervised {
        row.tier_path = vec![Tier::Supervised as u8, Tier::Rendered as u8];
        row.tier_used = Tier::Rendered as u8;
    }

    println!("part 1: a batch of {BATCH} rows with no T4 row in it");
    println!(
        "{:<40}{:>10}{:>13}{:>12}",
        "stage", "ns/row", "rows/s", "of budget"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let ledger = SupervisedLedger::in_dir(dir.path());
    let scan = best(5, || {
        black_box(ledger.record(black_box(&plain))).expect("record");
        plain.len()
    });
    line("SupervisedLedger::record, nothing to write", scan);
    assert!(
        !ledger.path().exists(),
        "the scan wrote a file it had nothing to put in"
    );

    println!();
    println!("part 2: the same batch with every row leased at T4");
    println!(
        "{:<40}{:>10}{:>13}{:>12}",
        "stage", "ns/row", "rows/s", "of budget"
    );

    let write = best_of(
        5,
        || tempfile::tempdir().expect("tempdir"),
        |dir| {
            let ledger = SupervisedLedger::in_dir(dir.path());
            ledger.record(black_box(&supervised)).expect("record");
            supervised.len()
        },
    );
    line("SupervisedLedger::record, one line each", write);

    // Part 3. What wrapping a sink costs, against not wrapping it. The inner
    // sink here does nothing at all, so the difference is the whole decorator:
    // one extra async frame and one scan that finds nothing.
    println!();
    println!("part 3: the decorator the crawl loop actually installs");
    println!("{:<40}{:>10}{:>13}", "stage", "us/batch", "ns/row");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let counter = std::sync::Arc::new(Counter::default());
    let bare = best(5, || {
        runtime
            .block_on(counter.take(black_box(&plain)))
            .expect("take");
        plain.len()
    });
    batch_line("the sink on its own", bare);

    let wrapped = Recorded::new(ledger, std::sync::Arc::clone(&counter));
    let through = best(5, || {
        runtime
            .block_on(wrapped.take(black_box(&plain)))
            .expect("take");
        plain.len()
    });
    batch_line("Recorded around the same sink", through);

    println!();
    // Taken off the whole batch rather than off the per row numbers, because
    // the bare sink is one atomic add and rounds to nothing a row.
    let overhead = (through.elapsed.as_secs_f64() - bare.elapsed.as_secs_f64()) / BATCH as f64;
    println!(
        "wrapping costs {:.0} ns a row on a batch with no T4 in it, which at gate\n\
         1.1's 250 pages a second is {:.4} percent of one core. The scan is a\n\
         `tier_path.first()` on each row and nothing else, and it opens no file\n\
         and takes no lock until a batch has something in it to write.",
        overhead * 1e9,
        250.0 * overhead * 100.0
    );
    println!(
        "a batch that is all T4 costs {:.0} ns a row, so the audit file is never\n\
         the reason a supervised crawl is slow. It is rare by design and doc 05.7\n\
         keeps it that way, but the number would hold if it were not.",
        write.per_item().as_secs_f64() * 1e9
    );
}

fn line(name: &str, run: Run) {
    let per = run.per_item().as_secs_f64();
    println!(
        "{:<40}{:>10.0}{:>13.0}{:>11.4}%",
        name,
        per * 1e9,
        1.0 / per,
        250.0 * per * 100.0
    );
}

/// Part 3's shape, where a rows per second column would say infinity.
fn batch_line(name: &str, run: Run) {
    let whole = run.elapsed.as_secs_f64();
    println!(
        "{:<40}{:>10.2}{:>13.0}",
        name,
        whole * 1e6,
        whole * 1e9 / run.items as f64
    );
}

/// A sink that counts and does nothing else, so part 3 measures the wrapper.
#[derive(Default)]
struct Counter(AtomicUsize);

#[async_trait::async_trait]
impl Sink for Counter {
    async fn take(&self, rows: &[PageRow]) -> Result<(), CrawlError> {
        self.0.fetch_add(rows.len(), Ordering::Relaxed);
        Ok(())
    }
}

fn crawled<'a>(
    outcome: &'a Outcome,
    extracted: &'a umi_extract::Extracted,
    tier: Tier,
) -> Crawled<'a> {
    Crawled {
        url: "https://example.com/article",
        keys: RowKey::for_url("https://example.com/article", None).expect("canonicalise"),
        host: "example.com",
        fetched_at_ms: T0,
        outcome,
        extracted: Some(extracted),
        tier_used: tier,
        tier_path: &[Tier::Plain],
        robots_checked_ms: T0 - 3_600_000,
        content_usage: None,
        fetcher_id: FetcherId::LOCAL,
        verification: Verification::Local,
        crawl_profile: 0,
    }
}

fn ok_page(body: &str) -> Outcome {
    let bytes = bytes::Bytes::from(body.as_bytes().to_vec());
    Outcome::Ok(Box::new(Page {
        final_url: "https://example.com/article".to_owned(),
        status: 200,
        version: Version::Http2,
        redirects: Vec::new(),
        headers_kept: vec![("content-type".to_owned(), "text/html".to_owned())],
        headers_digest: [7u8; 32],
        content_type: Some("text/html; charset=utf-8".to_owned()),
        media: umi_fetch::Media::Html,
        body_digest: *blake3::hash(&bytes).as_bytes(),
        body: bytes,
        revalidate: Revalidator::default(),
        elapsed: Duration::from_millis(120),
    }))
}
