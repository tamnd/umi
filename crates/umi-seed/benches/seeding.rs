//! How fast a seed list becomes candidates.
//!
//! The bar is doc 08.1's admission rate, about 12500 candidate URLs a second
//! per host. Seeding feeds admission, so if this is slower than that then
//! pointing `--seeder` at a large enumeration makes the crawl wait on the
//! seeder rather than on the network, and doc 13.7's whole on ramp is a
//! bottleneck instead of a convenience.
//!
//! Part 2 says where the time goes, because the answer decides what to do
//! about it. Line reading is ours to make faster. Canonicalisation is doc
//! 11.2's, it is the same work admission does anyway, and making it faster
//! helps the crawl and not just seeding.
//!
//! Part 4 is the other two sources doc 13.6 names, sitemaps and feeds. Same
//! bar, and it is the one that matters: a full sitemap is 50000 URLs in one
//! request, so if the parse is slower than admission then reading the file
//! costs more than fetching it did.
//!
//! Environment:
//!
//! * `UMI_BENCH_URLS` how many URLs, default 200000
//! * `UMI_BENCH_REPEAT` how many times to repeat, default 3
//!
//! ```text
//! cargo build --release -p umi-seed --benches
//! taskset -c 1 ./target/release/deps/seeding-<hash>
//! ```

use std::io::Write;
use std::time::Instant;

use umi_seed::{Limits, Source, seed};

/// Doc 08.1's admission rate for one host.
const ADMISSION: f64 = 12_500.0;

fn main() {
    let count = env("UMI_BENCH_URLS", 200_000);
    let repeat = env("UMI_BENCH_REPEAT", 3);

    println!("umi-seed, {count} URLs, best of {repeat}\n");

    let dir = tempfile::tempdir().expect("tempdir");
    let distinct = write_list(dir.path().join("distinct.txt"), count, Shape::Distinct);
    let repeated = write_list(dir.path().join("repeated.txt"), count, Shape::Repeated);
    let junk = write_list(dir.path().join("junk.txt"), count, Shape::Junk);

    println!("part 1: throughput, doc 08.1 admits about 12500 candidates/s a host");
    println!(
        "  {:<24} {:>12} {:>12} {:>16}",
        "list", "URLs/s", "MB/s", "vs admission"
    );
    for (name, path) in [
        ("all distinct", &distinct),
        ("one in eight distinct", &repeated),
        ("half of it not a URL", &junk),
    ] {
        let bytes = std::fs::metadata(path).expect("stat").len();
        let mut best = f64::MAX;
        let mut stats = None;
        for _ in 0..repeat {
            let start = Instant::now();
            let mut stream = seed(Source::File(path.clone()), Limits::default()).expect("start");
            let mut seen = 0u64;
            for item in &mut stream {
                if item.expect("no failure").url.is_empty() {
                    unreachable!("a canonical URL is never empty");
                }
                seen += 1;
            }
            best = best.min(start.elapsed().as_secs_f64());
            stats = Some((stream.stats(), seen));
        }
        let per_s = count as f64 / best;
        println!(
            "  {name:<24} {per_s:>12.0} {:>12.1} {:>15.1}x",
            bytes as f64 / 1e6 / best,
            per_s / ADMISSION
        );
        if let Some((stats, seen)) = stats {
            println!("      {stats}, {seen} handed back");
        }
    }
    println!();

    println!("part 2: where the time goes on the all distinct list");
    println!("  {:<24} {:>12} {:>12}", "stage", "ns/URL", "share");
    let urls = shape_list(count, Shape::Distinct);
    let read = time(repeat, || {
        let mut total = 0usize;
        for line in &urls {
            total += std::hint::black_box(line.trim().len());
        }
        total
    });
    // Each stage includes the ones before it, so the differences below are
    // what that stage added rather than two loops with different shapes.
    let canon = time(repeat, || {
        let mut total = 0usize;
        for line in &urls {
            if let Ok(url) = umi_types::canonicalize(line.trim(), None) {
                total += url.len();
            }
        }
        total
    });
    let keys = time(repeat, || {
        let mut total = 0usize;
        for line in &urls {
            if let Ok(url) = umi_types::canonicalize(line.trim(), None)
                && let Ok(row) = umi_types::RowKey::for_canonical(&url)
            {
                total += usize::from(row.url.as_bytes()[0]);
            }
        }
        total
    });
    let whole = keys;
    for (name, seconds) in [
        ("trim the line", read),
        ("canonicalise", canon - read),
        ("derive the three keys", keys - canon),
    ] {
        println!(
            "  {name:<24} {:>12.0} {:>11.1}%",
            seconds / count as f64 * 1e9,
            seconds / whole * 100.0
        );
    }
    println!(
        "  {:<24} {:>12.0} {:>11.1}%",
        "total",
        whole / count as f64 * 1e9,
        100.0
    );

    println!("\npart 3: the deduplication set");
    // 10 bytes a key plus the table's own control byte, at the load factor
    // hashbrown keeps. This is arithmetic rather than a measurement, because
    // reading the process resident size on Linux from a bench would measure
    // the allocator as much as the table.
    let per_key = (umi_types::UrlKey::LEN + 1) as f64 / 0.875;
    let cap = Limits::default().max_seen;
    println!(
        "  {:.1} bytes a key, {} keys, {:.0} MB at the cap",
        per_key,
        cap,
        per_key * cap as f64 / 1e6
    );
    println!("  past the cap the stream stops deduplicating rather than growing,");
    println!("  and doc 08's seen set catches the repeats at admission anyway.");

    println!("\npart 4: sitemaps and feeds, doc 13.6");
    println!(
        "  {:<24} {:>12} {:>12} {:>16}",
        "document", "URLs/s", "MB/s", "vs admission"
    );
    // 50000 is sitemaps.org's cap, so this is the largest single file that is
    // still in spec and is what a big site actually serves.
    let full = umi_seed::sitemap::MAX_URLS;
    for (name, doc, urls) in [
        ("sitemap", sitemap_doc(full, true), full),
        ("sitemap, no lastmod", sitemap_doc(full, false), full),
        ("sitemap index", index_doc(full), full),
        ("sitemap, plain text", text_doc(full), full),
        ("rss", rss_doc(full), full),
        ("atom", atom_doc(full), full),
    ] {
        let bytes = doc.len();
        let best = time(repeat, || {
            if name == "rss" || name == "atom" {
                umi_seed::Feed::parse(doc.as_bytes()).entries.len()
            } else {
                umi_seed::Sitemap::parse(doc.as_bytes()).all().count()
            }
        });
        let per_s = urls as f64 / best;
        println!(
            "  {name:<24} {per_s:>12.0} {:>12.1} {:>15.1}x",
            bytes as f64 / 1e6 / best,
            per_s / ADMISSION
        );
    }
    println!("  a full sitemap is a few MB and one request, so anything here that");
    println!("  beats admission means the fetch is the cost and the parse is not.");
}

/// A `<urlset>` with `count` URLs in it, with or without the date.
fn sitemap_doc(count: usize, lastmod: bool) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n",
    );
    for i in 0..count {
        out.push_str("  <url><loc>https://example.com/page/");
        out.push_str(&i.to_string());
        out.push_str("?q=1&amp;r=2</loc>");
        if lastmod {
            out.push_str("<lastmod>2026-08-28T12:30:45Z</lastmod>");
        }
        out.push_str("<changefreq>daily</changefreq><priority>0.8</priority></url>\n");
    }
    out.push_str("</urlset>\n");
    out
}

/// A `<sitemapindex>` pointing at `count` other files.
fn index_doc(count: usize) -> String {
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n",
    );
    for i in 0..count {
        out.push_str("  <sitemap><loc>https://example.com/sitemap-");
        out.push_str(&i.to_string());
        out.push_str(".xml</loc><lastmod>2026-08-28</lastmod></sitemap>\n");
    }
    out.push_str("</sitemapindex>\n");
    out
}

/// The plain text form, which is the cheapest of the three to read.
fn text_doc(count: usize) -> String {
    let mut out = String::new();
    for i in 0..count {
        out.push_str("https://example.com/page/");
        out.push_str(&i.to_string());
        out.push('\n');
    }
    out
}

fn rss_doc(count: usize) -> String {
    let mut out = String::from("<rss version=\"2.0\"><channel><title>Example</title>\n");
    for i in 0..count {
        out.push_str("<item><title>Post ");
        out.push_str(&i.to_string());
        out.push_str("</title><link>https://example.com/page/");
        out.push_str(&i.to_string());
        out.push_str("</link><guid isPermaLink=\"false\">tag:example.com,2026:");
        out.push_str(&i.to_string());
        out.push_str("</guid><pubDate>Fri, 28 Aug 2026 12:30:45 GMT</pubDate></item>\n");
    }
    out.push_str("</channel></rss>\n");
    out
}

fn atom_doc(count: usize) -> String {
    let mut out = String::from("<feed xmlns=\"http://www.w3.org/2005/Atom\">\n");
    for i in 0..count {
        out.push_str("<entry><title>Post ");
        out.push_str(&i.to_string());
        out.push_str("</title><link rel=\"alternate\" href=\"https://example.com/page/");
        out.push_str(&i.to_string());
        out.push_str("\"/><updated>2026-08-28T12:30:45Z</updated></entry>\n");
    }
    out.push_str("</feed>\n");
    out
}

/// What kind of list to write.
#[derive(Clone, Copy)]
enum Shape {
    /// Every line a different URL.
    Distinct,
    /// Eight lines per distinct URL, spelled eight different ways that doc
    /// 11.2 folds into one.
    Repeated,
    /// Half URLs, half things that are not.
    Junk,
}

fn shape_list(count: usize, shape: Shape) -> Vec<String> {
    (0..count)
        .map(|i| match shape {
            Shape::Distinct => format!("https://example{}.com/page/{i}?q={i}", i % 997),
            Shape::Repeated => {
                let page = i / 8;
                match i % 8 {
                    0 => format!("https://example.com/page/{page}"),
                    1 => format!("https://EXAMPLE.com/page/{page}"),
                    2 => format!("https://example.com:443/page/{page}"),
                    3 => format!("https://example.com/page/{page}#top"),
                    4 => format!("https://example.com/page/{page}?utm_source=x"),
                    5 => format!("https://example.com/./page/{page}"),
                    6 => format!("https://example.com/page/../page/{page}"),
                    _ => format!("https://example.com/page/{page}?utm_medium=y"),
                }
            }
            Shape::Junk => {
                if i % 2 == 0 {
                    format!("https://example{}.com/page/{i}", i % 997)
                } else {
                    format!("this is line {i} and it is not a URL")
                }
            }
        })
        .collect()
}

fn write_list(path: std::path::PathBuf, count: usize, shape: Shape) -> std::path::PathBuf {
    let mut file = std::io::BufWriter::new(std::fs::File::create(&path).expect("create"));
    for line in shape_list(count, shape) {
        writeln!(file, "{line}").expect("write");
    }
    file.flush().expect("flush");
    path
}

/// Best of `repeat` runs, in seconds.
fn time<T>(repeat: usize, mut run: impl FnMut() -> T) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..repeat {
        let start = Instant::now();
        std::hint::black_box(run());
        best = best.min(start.elapsed().as_secs_f64());
    }
    best
}

fn env(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}
