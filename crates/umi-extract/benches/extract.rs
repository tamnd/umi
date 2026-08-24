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
//! ```text
//! cargo bench -p umi-extract                              # the golden corpus
//! UMI_BENCH_CORPUS=~/umi-bench/pages cargo bench -p umi-extract   # real pages
//! UMI_BENCH_REPEAT=20 cargo bench -p umi-extract          # a small corpus, more passes
//! ```

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use umi_extract::extract;
use url::Url;

fn main() {
    let dir = std::env::var("UMI_BENCH_CORPUS")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("corpus"));
    let repeat: usize = std::env::var("UMI_BENCH_REPEAT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);

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

    let mut timings: Vec<(u128, usize, String)> = Vec::with_capacity(documents.len());
    let mut text_ns = 0u128;
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
    let wall = wall.elapsed();

    report(&dir, &mut timings, text_ns, repeat, wall.as_secs_f64());
}

fn load(dir: &PathBuf) -> Vec<(String, Vec<u8>)> {
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
            Some((name, fs::read(&path).ok()?))
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn report(
    dir: &PathBuf,
    timings: &mut [(u128, usize, String)],
    text_ns: u128,
    repeat: usize,
    wall: f64,
) {
    let count = timings.len();
    let bytes: usize = timings.iter().map(|(_, len, _)| len).sum();
    let total: u128 = timings.iter().map(|(ns, _, _)| ns).sum();

    let mut slowest: Vec<&(u128, usize, String)> = timings.iter().collect();
    slowest.sort_by(|a, b| b.0.cmp(&a.0));
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
    println!();
    println!("per page       mean {mean_ms:.2} ms");
    println!(
        "               p50 {:.2} ms   p90 {:.2} ms   p99 {:.2} ms   max {:.2} ms",
        at(50),
        at(90),
        at(99),
        timings[count - 1].0 as f64 / 1e6
    );
    println!(
        "               plain text and digest {:.2} ms of it",
        text_ns as f64 / (count * repeat) as f64 / 1e6
    );
    println!(
        "throughput     {pages_per_second:.0} pages/s/core, {:.1} MiB/s/core",
        bytes as f64 / 1024.0 / 1024.0 / (total as f64 / 1e9)
    );
    println!();
    println!(
        "doc 11.9       budget is 3 to 8 ms per page per core: {}",
        if mean_ms <= 8.0 { "met" } else { "MISSED" }
    );
    println!(
        "doc 16 gate 1.1  250 pages/s needs {:.2} cores of extraction",
        250.0 / pages_per_second
    );
    println!();
    println!("slowest documents");
    for (ns, len, name) in slowest.iter().take(5) {
        println!(
            "  {:>8.2} ms  {:>7.1} KiB  {name}",
            *ns as f64 / 1e6,
            *len as f64 / 1024.0
        );
    }
}
