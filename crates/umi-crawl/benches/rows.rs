//! What one row costs, against gate 1.1's 250 pages a second.
//!
//! The row builder is the last stage of the crawl pipeline and it is the one
//! whose cost is easy to underestimate, because none of it looks expensive:
//! a chunk tree here, a sketch there, a digest, some Arrow appends. Added up
//! they are either a few percent of a page's budget or a third of it, and the
//! difference decides whether a server does 250 pages a second or 90.
//!
//! Deliberately not a criterion benchmark. This is a small number of large
//! measurements against a fixed target, best of five, and criterion's sampling
//! and outlier analysis would obscure the one number that matters. It also
//! needs no dev-dependency and no `--features` dance to run under `taskset`.
//!
//! Run it pinned, since an unpinned run on a machine that is also crawling
//! measures the scheduler:
//!
//! ```text
//! taskset -c 5 chrt --fifo 50 ./target/release/deps/rows-<hash> --bench
//! ```

use std::hint::black_box;
use std::time::Duration;

use umi_crawl::page::{PageBuilder, PageRow};
use umi_crawl::run::Sink;
use umi_crawl::{Crawled, SegmentInfo, SegmentSink, extract_digest};
use umi_extract::{Extracted, extract};
use umi_fetch::Outcome;
use umi_fetch::outcome::{Page, Version};
use umi_file::WriterConfig;
use umi_types::{FetcherId, Revalidator, RowKey, Tier, Verification};

mod support;

use support::{MEDIAN_HTML, Run, best, best_of, html_of};

const T0: u64 = 1_760_000_000_000;

fn main() {
    println!("the doc 10.5 row builder, best of 5, 150 KB pages\n");

    let bodies: Vec<String> = (0..64).map(|i| html_of(i, MEDIAN_HTML)).collect();
    let url = url::Url::parse("https://example.com/article").expect("parse");
    let extracted: Vec<Extracted> = bodies
        .iter()
        .map(|body| extract(body.as_bytes(), &url))
        .collect();
    let pages: Vec<Outcome> = bodies.iter().map(|body| ok_page(body)).collect();

    let sample = &extracted[0];
    let text_bytes = sample.text().len();
    println!(
        "input: {:.1} KB of html, {:.1} KB of text ({:.0}% of the page), {} links, \
         {} headings",
        bodies[0].len() as f64 / 1024.0,
        text_bytes as f64 / 1024.0,
        100.0 * text_bytes as f64 / bodies[0].len() as f64,
        sample.links.links.len(),
        sample.meta.headings.len()
    );
    println!();

    println!("part 1: one row, end to end");
    println!(
        "{:<34}{:>10}{:>13}{:>12}",
        "stage", "us/row", "rows/s", "of budget"
    );

    let whole = best(5, || {
        for (outcome, e) in pages.iter().zip(&extracted) {
            black_box(PageRow::build(&black_box(crawled(outcome, Some(e)))));
        }
        pages.len()
    });
    line("PageRow::build", whole);

    let digest = best(5, || {
        for e in &extracted {
            black_box(extract_digest(black_box(e)));
        }
        extracted.len()
    });
    line("  of which extract_digest", digest);

    let text = best(5, || {
        for e in &extracted {
            black_box(e.text());
        }
        extracted.len()
    });
    line("  of which the plain text", text);

    let sketch = best(5, || {
        for e in &extracted {
            black_box(umi_dedup::Content::of(&e.text()));
        }
        extracted.len()
    });
    line("  of which text plus sketch", sketch);

    let tree = best(5, || {
        for outcome in &pages {
            let body = outcome.page().map_or(&[][..], |p| p.body.as_ref());
            black_box(umi_dedup::ChunkTree::build(body));
        }
        pages.len()
    });
    line("  of which the chunk tree", tree);

    let rows: Vec<PageRow> = pages
        .iter()
        .zip(&extracted)
        .map(|(outcome, e)| PageRow::build(&crawled(outcome, Some(e))))
        .collect();

    // How many of these rows fit before doc 10.4 says to seal. Not 16384: a
    // page of this size carries enough markdown that the 32 MiB limit arrives
    // first, which is the whole reason `is_full` counts bytes.
    let per_shoal = {
        let mut builder = PageBuilder::new();
        let mut n = 0;
        while !builder.is_full() {
            builder.push(&rows[n % rows.len()]);
            n += 1;
        }
        n
    };

    println!();
    println!(
        "part 2: a whole shoal, {per_shoal} rows, which is where doc 10.4 seals\n\
         at {} MiB rather than at 16384 rows",
        PageBuilder::BYTE_LIMIT >> 20
    );
    println!(
        "{:<34}{:>10}{:>13}{:>12}",
        "stage", "us/row", "rows/s", "of budget"
    );

    let append = best(5, || {
        let mut builder = PageBuilder::new();
        for i in 0..per_shoal {
            builder.push(&rows[i % rows.len()]);
        }
        black_box(builder.rows());
        per_shoal
    });
    line("PageBuilder::push", append);

    let finish = best(5, || {
        let mut builder = PageBuilder::new();
        for i in 0..per_shoal {
            builder.push(&rows[i % rows.len()]);
        }
        black_box(builder.finish());
        per_shoal
    });
    line("push then finish", finish);

    // Part 3. The same rows, onto a real disk. Part 2 stops at an Arrow batch
    // in memory, and the writer is where the rest of doc 10 happens: every
    // column chunk is encoded, compressed and checksummed, the shoal directory
    // is written, and the footer is built at the seal. That is the last stage
    // between a fetch and a file somebody can publish, so it belongs in the
    // per page budget rather than in a footnote.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let batches = 2;
    let through = per_shoal * batches;
    println!();
    println!(
        "part 3: through the sink onto disk, {through} rows, {batches} shoals \
         and a seal"
    );
    println!(
        "{:<34}{:>10}{:>13}{:>12}",
        "stage", "us/row", "rows/s", "of budget"
    );

    let mut on_disk = 0u64;
    let mut logical = 0u64;
    let sunk = best_of(
        3,
        || tempfile::tempdir().expect("tempdir"),
        |dir| {
            let sink =
                SegmentSink::create(dir.path(), SegmentInfo::default(), WriterConfig::default())
                    .expect("create");
            runtime.block_on(async {
                for i in 0..batches {
                    let batch: Vec<PageRow> = (0..per_shoal)
                        .map(|n| rows[(i * per_shoal + n) % rows.len()].clone())
                        .collect();
                    sink.take(&batch).await.expect("take");
                }
            });
            let sealed = sink.finish().expect("finish").expect("a segment was open");
            on_disk = sealed.stats.encoded_bytes;
            logical = sealed.stats.logical_bytes;
            through
        },
    );
    line("SegmentSink::take plus the seal", sunk);

    println!();
    println!(
        "the file holds {:.1} KB per row against {:.1} KB of columns, so the\n\
         compression doc 10.2 budgets for is {:.1}x and a 128 MB segment holds\n\
         about {} rows.",
        on_disk as f64 / through as f64 / 1024.0,
        logical as f64 / through as f64 / 1024.0,
        logical as f64 / on_disk as f64,
        (128 << 20) / (on_disk / through as u64).max(1)
    );

    println!();
    let per_row = whole.per_item() + finish.per_item();
    let rows_per_second = 1.0 / per_row.as_secs_f64();
    println!(
        "one row all the way to a batch costs {:.0} us, which is {:.0} pages a\n\
         second on one core against gate 1.1's 250.",
        per_row.as_secs_f64() * 1e6,
        rows_per_second
    );
    println!(
        "at 250 pages a second the builder is using {:.1} percent of one core.",
        250.0 * per_row.as_secs_f64() * 100.0
    );
    let to_disk = whole.per_item() + sunk.per_item();
    println!(
        "one row all the way onto disk costs {:.0} us, which is {:.0} pages a\n\
         second on one core, or {:.1} percent of a core at gate 1.1's 250.",
        to_disk.as_secs_f64() * 1e6,
        1.0 / to_disk.as_secs_f64(),
        250.0 * to_disk.as_secs_f64() * 100.0
    );
    println!(
        "everything above except the chunk tree scales with text and not with\n\
         html, so the rate that transfers to other pages is {:.0} ns per byte of\n\
         text, or {:.2} ms for every 10 KB of it.",
        per_row.as_secs_f64() * 1e9 / text_bytes as f64,
        per_row.as_secs_f64() * 1e3 * 10240.0 / text_bytes as f64
    );
}

fn line(name: &str, run: Run) {
    let per = run.per_item().as_secs_f64();
    println!(
        "{:<34}{:>10.2}{:>13.0}{:>11.1}%",
        name,
        per * 1e6,
        1.0 / per,
        250.0 * per * 100.0
    );
}

fn crawled<'a>(outcome: &'a Outcome, extracted: Option<&'a Extracted>) -> Crawled<'a> {
    Crawled {
        url: "https://example.com/article",
        keys: RowKey::for_url("https://example.com/article", None).expect("canonicalise"),
        host: "example.com",
        fetched_at_ms: T0,
        outcome,
        extracted,
        tier_used: Tier::Plain,
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
        headers_kept: vec![
            ("content-type".to_owned(), "text/html".to_owned()),
            ("etag".to_owned(), "\"abc\"".to_owned()),
            (
                "last-modified".to_owned(),
                "Wed, 21 Oct 2026 07:28:00 GMT".to_owned(),
            ),
        ],
        headers_digest: [7u8; 32],
        content_type: Some("text/html; charset=utf-8".to_owned()),
        media: umi_fetch::Media::Html,
        body_digest: *blake3::hash(&bytes).as_bytes(),
        body: bytes,
        revalidate: Revalidator::default(),
        elapsed: Duration::from_millis(120),
    }))
}
