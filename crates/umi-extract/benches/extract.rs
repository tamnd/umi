//! What extraction costs per page, measured against doc 11.9's budget.
//!
//! Doc 01 gives extraction 3 to 8 ms per page per core and doc 16's gate 1.1
//! wants 250 pages per second on one server, so this prints the two numbers that
//! decide whether the design is right: milliseconds per page, and how many cores
//! 250 pages per second would need. A budget nobody measures is a wish.
//!
//! There is no criterion here on purpose. Criterion is a good harness for
//! comparing two implementations of a function at microsecond scale, and this is
//! a millisecond scale question about a corpus, where the interesting statistic
//! is the tail across documents rather than the variance across runs of one
//! document. The whole thing is `Instant` and a sort.
//!
//! The parse is timed on its own as well as inside the total, because the two
//! answers lead to different work. If html5ever is most of the number then no
//! amount of tuning the passes over the tree will move it and the fix is upstream
//! or is a cap on what we agree to parse. If the passes are most of it, they are
//! ours to fix.
//!
//! server1, server2 and server3 all run other work, so the first thing this
//! prints is whether the machine gave us a cpu or made us queue for one. A run
//! that was contended reports no verdict at all rather than a number somebody
//! might quote. See `scheduled`.
//!
//! ```text
//! cargo bench -p umi-extract                              # the golden corpus
//! UMI_BENCH_CORPUS=~/umi-bench/pages cargo bench -p umi-extract   # real pages
//! UMI_BENCH_REPEAT=20 cargo bench -p umi-extract          # a small corpus, more passes
//! UMI_BENCH_META=1 cargo bench -p umi-extract             # what doc 11.6 finds out there
//! chrt --fifo 50 cargo bench -p umi-extract               # take the cpu on a busy machine
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use html5ever::tendril::TendrilSink;
use markup5ever_rcdom::RcDom;
use umi_extract::{DescriptionSource, TitleSource, extract};
use url::Url;

/// Doc 05.4 caps a stored body at 512 KiB, so the fetcher never hands the
/// extractor more than this and neither does the bench. Measuring 1.3 MB WARC
/// payloads would be measuring a page that cannot reach us.
const CAP: usize = 512 * 1024;

/// How much runqueue wait, as a percentage of time actually on CPU, before the
/// run is called contended and the numbers are called noise.
///
/// Ten percent is generous. A quiet machine sits near zero.
const CONTENTION_LIMIT: u64 = 10;

/// Nanoseconds this thread spent on CPU and nanoseconds it spent waiting for a
/// CPU, straight from the scheduler.
///
/// `Instant` measures wall clock, and wall clock on a shared machine measures
/// the neighbours. server1, server2 and server3 all run other work, and at one
/// point during this branch server3 was at load 76 on eight cores, where a bare
/// `cat` spent 2 ms running and 77 ms queued. Numbers taken under that are not
/// a property of the extractor. So the run reports what the scheduler says and
/// refuses to pretend a contended measurement is a clean one.
///
/// `/proc/self/schedstat` is Linux with `CONFIG_SCHEDSTATS`, which is every
/// machine we run the gate on. Anywhere else this returns `None` and the report
/// simply does not have the line.
fn scheduled() -> Option<(u64, u64)> {
    let raw = fs::read_to_string("/proc/self/schedstat").ok()?;
    let mut fields = raw.split_ascii_whitespace();
    let running: u64 = fields.next()?.parse().ok()?;
    let queued: u64 = fields.next()?.parse().ok()?;
    // All zeros means the kernel was built without the accounting.
    if running == 0 {
        None
    } else {
        Some((running, queued))
    }
}

fn main() {
    let dir = std::env::var("UMI_BENCH_CORPUS")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("corpus"));
    // Three passes and take the best. The minimum is the honest statistic for
    // "what does this page cost": every source of error here adds time and none
    // of it subtracts, so the fastest run is the one with the least of somebody
    // else's work mixed into it. The mean would average our number with theirs.
    let repeat: usize = std::env::var("UMI_BENCH_REPEAT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3);

    let documents = load(&dir);
    if documents.is_empty() {
        eprintln!("no documents in {}", dir.display());
        std::process::exit(1);
    }

    let url = Url::parse("https://bench.example/page").expect("the url parses");

    // One untimed pass, because the first document through pays for the string
    // cache html5ever fills in lazily and that is a startup cost, not a per page
    // cost. At 100 billion pages the startup cost rounds to nothing.
    for (_, html) in &documents {
        std::hint::black_box(extract(html, &url));
    }

    // Each phase gets its own loop over the corpus. Interleaving them in one
    // loop looks tidier and measures something else: the second parse evicts the
    // first one's tree from cache and hands the allocator a heap in a different
    // state, so the extract timing picks up the cost of the measurement next to
    // it. Separate loops keep each phase measuring itself.
    let mut timings: Vec<(u128, usize, String)> = Vec::with_capacity(documents.len());
    let mut text_ns = 0u128;
    let before = scheduled();
    let wall = Instant::now();
    for (name, html) in &documents {
        let mut best = u128::MAX;
        for _ in 0..repeat {
            let start = Instant::now();
            let page = std::hint::black_box(extract(html, &url));
            best = best.min(start.elapsed().as_nanos());

            let start = Instant::now();
            std::hint::black_box(page.text_digest());
            text_ns += start.elapsed().as_nanos();
        }
        timings.push((best, html.len(), name.clone()));
    }

    // The same parse the extractor does, timed on its own. The tree is dropped
    // inside the measurement because building it and freeing it are both costs
    // the extractor pays.
    let mut parse_ns = 0u128;
    for (_, html) in &documents {
        let mut best = u128::MAX;
        for _ in 0..repeat {
            let start = Instant::now();
            let tree = html5ever::parse_document(RcDom::default(), Default::default())
                .from_utf8()
                .one(html.as_slice());
            drop(std::hint::black_box(tree));
            best = best.min(start.elapsed().as_nanos());
        }
        parse_ns += best;
    }
    let wall = wall.elapsed();
    let contention = match (before, scheduled()) {
        (Some((ran_before, queued_before)), Some((ran_after, queued_after))) => Some((
            ran_after.saturating_sub(ran_before),
            queued_after.saturating_sub(queued_before),
        )),
        _ => None,
    };

    // A digest per document, so that a change meant to be invisible can be shown
    // to be invisible on real pages rather than on the twenty three hand written
    // ones in the golden corpus. Dump before, change, dump after, diff. This is
    // how a byte scan that removed script and style ahead of the parser was
    // caught disagreeing with html5ever on five pages out of two thousand, which
    // no fixture had found and which ended that idea.
    if let Ok(path) = std::env::var("UMI_BENCH_DIGESTS") {
        let mut lines = String::new();
        for (name, html) in &documents {
            let page = extract(html, &url);
            lines.push_str(&format!(
                "{name} md={} text={}\n",
                hex::encode(blake3::hash(page.markdown.as_bytes()).as_bytes()),
                hex::encode(page.text_digest())
            ));
        }
        fs::write(&path, lines).unwrap_or_else(|error| panic!("cannot write {path}: {error}"));
        println!("per document digests written to {path}");
    }

    // What doc 11.6 actually finds on real pages. A metadata reader that returns
    // nothing passes every unit test that only checks the shape of what it
    // returns, and the way to catch that is to point it at two thousand pages
    // off the live web and read the percentages. The numbers are also the
    // argument for the precedence lists: if `og:title` never fires, it did not
    // need to be in the list.
    if std::env::var("UMI_BENCH_META").is_ok() {
        coverage(&documents, &url);
    }

    // Per document numbers for anyone who wants to know what predicts the tail.
    // The top five in the report tell you which pages are slow and nothing about
    // why, and the why is a question for a scatter plot rather than for a guess.
    if let Ok(path) = std::env::var("UMI_BENCH_CSV") {
        let mut csv = String::from("name,bytes,ns\n");
        for (ns, len, name) in timings.iter() {
            csv.push_str(&format!("{name},{len},{ns}\n"));
        }
        fs::write(&path, csv).unwrap_or_else(|error| panic!("cannot write {path}: {error}"));
        println!("per document timings written to {path}");
    }

    report(
        &dir,
        &mut timings,
        text_ns,
        parse_ns,
        repeat,
        wall.as_secs_f64(),
        contention,
    );
}

/// How often each of doc 11.6's fields is there to be found.
///
/// One extract per document, outside the timed loops, because this is a
/// question about the corpus rather than about the clock.
fn coverage(documents: &[(String, Vec<u8>)], url: &Url) {
    let mut counts = vec![0usize; ROWS.len()];
    let mut headings = 0usize;
    for (_, html) in documents {
        let page = extract(html, url);
        let meta = &page.meta;
        let hit = [
            meta.title.is_some(),
            meta.title_source == Some(TitleSource::Title),
            meta.title_source == Some(TitleSource::OpenGraph),
            meta.title_source == Some(TitleSource::Heading),
            meta.description.is_some(),
            meta.description_source == Some(DescriptionSource::Meta),
            meta.description_source == Some(DescriptionSource::OpenGraph),
            meta.description_source == Some(DescriptionSource::Twitter),
            meta.description_derived(),
            meta.canonical.is_some(),
            meta.published.is_some() || meta.modified.is_some(),
            !meta.headings.is_empty(),
            !meta.feeds.is_empty(),
            meta.declared_lang.is_some(),
            !meta.structured.types.is_empty(),
            meta.structured.published.is_some() || meta.structured.modified.is_some(),
            meta.structured.author.is_some(),
            meta.structured.headline.is_some(),
            meta.microdata,
            meta.rdfa,
            page.content_withheld.is_some(),
        ];
        for (count, hit) in counts.iter_mut().zip(hit) {
            *count += usize::from(hit);
        }
        headings += meta.headings.len();
    }

    let total = documents.len().max(1);
    println!("\ndoc 11.6 coverage over {total} documents");
    for (row, count) in ROWS.iter().zip(&counts) {
        println!("{row:>28}  {count:6}  {:3} percent", count * 100 / total);
    }
    // Tenths by integer division, because this crate does not do floating point
    // and a report line is not the place to start.
    let tenths = headings * 10 / total;
    println!(
        "{:>28}  {headings:6}  {}.{} per page",
        "headings kept",
        tenths / 10,
        tenths % 10
    );
}

/// The rows of the coverage report, in the order [`coverage`] fills them.
const ROWS: [&str; 21] = [
    "title",
    "  from title",
    "  from og:title",
    "  from first h1",
    "description",
    "  from meta",
    "  from og",
    "  from twitter",
    "  ours, not theirs",
    "canonical",
    "article dates",
    "headings",
    "feeds",
    "lang attribute",
    "json-ld types",
    "json-ld dates",
    "json-ld author",
    "json-ld headline",
    "microdata",
    "rdfa",
    "withheld, noindex",
];

fn load(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out: Vec<(String, Vec<u8>)> = fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", dir.display()))
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_file()
                && !matches!(
                    path.extension().and_then(|ext| ext.to_str()),
                    Some("md" | "txt" | "json")
                )
        })
        .filter_map(|path| {
            let name = path.file_name()?.to_string_lossy().into_owned();
            let mut body = fs::read(&path).ok()?;
            body.truncate(CAP);
            Some((name, body))
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn report(
    dir: &Path,
    timings: &mut [(u128, usize, String)],
    text_ns: u128,
    parse_ns: u128,
    repeat: usize,
    wall: f64,
    contention: Option<(u64, u64)>,
) {
    let count = timings.len();
    let bytes: usize = timings.iter().map(|(_, len, _)| len).sum();
    let total: u128 = timings.iter().map(|(ns, _, _)| ns).sum();

    // One sort, ascending, so the percentiles read off the front and the slowest
    // documents read off the back. Two views of the same order.
    timings.sort_by_key(|(ns, _, _)| *ns);
    let at = |percentile: usize| -> f64 {
        let index = (count.saturating_sub(1)) * percentile / 100;
        timings[index].0 as f64 / 1e6
    };

    let mean_ms = total as f64 / count as f64 / 1e6;
    let pages_per_second = 1000.0 / mean_ms;

    println!("corpus         {}", dir.display());
    println!("documents      {count}");
    println!(
        "size           {:.1} KiB total, {:.1} KiB mean",
        bytes as f64 / 1024.0,
        bytes as f64 / count as f64 / 1024.0
    );
    println!("passes         {repeat} (best of, per document)");
    println!("wall           {wall:.2} s");

    // Whether anyone should believe the rest of the output.
    let contended = match contention {
        Some((running, queued)) => {
            let ratio = queued.saturating_mul(100) / running.max(1);
            println!(
                "scheduler      {:.2} s on cpu, {:.2} s queued, {ratio} percent contention",
                running as f64 / 1e9,
                queued as f64 / 1e9
            );
            ratio > CONTENTION_LIMIT
        }
        None => {
            println!("scheduler      not available, cannot tell whether this machine was busy");
            false
        }
    };
    if contended {
        println!();
        println!("  CONTENDED. Another process took the cpu out from under this run, so the");
        println!("  per page numbers below are that process as much as they are extraction.");
        println!("  Re-run when the machine is quiet, or take the cpu with:");
        println!();
        println!("    chrt --fifo 50 cargo bench -p umi-extract");
    }
    println!();
    println!("per page       mean {mean_ms:.2} ms");
    println!(
        "               p50 {:.2} ms   p90 {:.2} ms   p99 {:.2} ms   max {:.2} ms",
        at(50),
        at(90),
        at(99),
        timings[count - 1].0 as f64 / 1e6
    );
    let parse_ms = parse_ns as f64 / count as f64 / 1e6;
    println!(
        "               html5ever parse {parse_ms:.2} ms of it, {:.0} percent",
        parse_ms / mean_ms * 100.0
    );
    println!(
        "               plain text and digest {:.2} ms on top",
        text_ns as f64 / (count * repeat) as f64 / 1e6
    );
    println!(
        "throughput     {pages_per_second:.0} pages/s/core, {:.1} MiB/s/core",
        bytes as f64 / 1024.0 / 1024.0 / (total as f64 / 1e9)
    );
    println!();
    println!(
        "doc 11.9       budget is 3 to 8 ms per page per core: {}",
        match (contended, mean_ms <= 8.0) {
            // A pass measured on a busy machine is luck, and a fail measured on
            // one is the machine. Neither is a verdict.
            (true, _) => "unknown, the run was contended",
            (false, true) => "met",
            (false, false) => "MISSED",
        }
    );
    println!(
        "doc 16 gate 1.1  250 pages/s needs {:.2} cores of extraction",
        250.0 / pages_per_second
    );
    println!();
    println!("slowest documents");
    for (ns, len, name) in timings.iter().rev().take(5) {
        println!(
            "  {:>8.2} ms  {:>7.1} KiB  {name}",
            *ns as f64 / 1e6,
            *len as f64 / 1024.0
        );
    }
}
