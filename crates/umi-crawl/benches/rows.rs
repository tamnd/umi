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
use std::time::{Duration, Instant};

use umi_crawl::page::{PageBuilder, PageRow};
use umi_crawl::{Crawled, extract_digest};
use umi_extract::{Extracted, extract};
use umi_fetch::Outcome;
use umi_fetch::outcome::{Page, Version};
use umi_types::{FetcherId, Revalidator, RowKey, Tier, Verification};

/// Doc 10.2's median index worthy page is about 150 KB of HTML.
const MEDIAN_HTML: usize = 150 * 1024;

/// How much of a real page is prose.
///
/// This number is the whole benchmark. Every stage below except the chunk tree
/// costs time proportional to the extracted text and not to the HTML that
/// carried it, so a generator that turns 150 KB of HTML into 150 KB of text
/// measures a page that does not exist and reports a cost ten times the real
/// one. The first version of this file did exactly that.
///
/// Ten percent is where real pages sit. A 150 KB article ships a stylesheet, a
/// tracker, a JSON-LD block, a nav, a sidebar and a footer, and what is left
/// over for the prose is one or two thousand words. Doc 11.9's budget of 0.8
/// to 1.5 ms on a 150 KB document is a budget for a document like that.
const TEXT_SHARE: f64 = 0.10;

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
    println!(
        "everything above except the chunk tree scales with text and not with\n\
         html, so the rate that transfers to other pages is {:.0} ns per byte of\n\
         text, or {:.2} ms for every 10 KB of it.",
        per_row.as_secs_f64() * 1e9 / text_bytes as f64,
        per_row.as_secs_f64() * 1e3 * 10240.0 / text_bytes as f64
    );
}

/// One measurement: how long, over how many items.
#[derive(Clone, Copy)]
struct Run {
    elapsed: Duration,
    items: usize,
}

impl Run {
    fn per_item(self) -> Duration {
        self.elapsed / u32::try_from(self.items).unwrap_or(u32::MAX)
    }
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

/// Best of `n`, because the worst case on a shared machine is the scheduler
/// and the best case is the code.
fn best(n: usize, mut body: impl FnMut() -> usize) -> Run {
    let mut best = Run {
        elapsed: Duration::MAX,
        items: 1,
    };
    for _ in 0..n {
        let at = Instant::now();
        let items = body();
        let elapsed = at.elapsed();
        if elapsed < best.elapsed {
            best = Run { elapsed, items };
        }
    }
    best
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

/// A page shaped like doc 10.2's median: an article with sections, 50 links
/// that mostly point at its own site, and a nav bar that the boilerplate
/// scorer has to throw away.
///
/// A pure function of the index, so two runs on two machines work on the same
/// bytes, which is the same rule `umi_file::sample` follows and for the same
/// reason.
fn html_of(seed: usize, target: usize) -> String {
    #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
    let prose_target = (target as f64 * TEXT_SHARE) as usize;

    let mut out = String::with_capacity(target + 4096);
    out.push_str(
        "<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'>\
         <title>An Article About Something</title>\
         <meta name='description' content='A description of the article, of \
         about the length a real one is.'>\
         <meta name='viewport' content='width=device-width, initial-scale=1'>\
         <link rel='canonical' href='https://example.com/article'>\
         <link rel='alternate' type='application/rss+xml' href='/feed.xml'>\
         <script type='application/ld+json'>{\"@context\":\"https://schema.org\",\
         \"@type\":\"NewsArticle\",\"headline\":\"An Article About Something\",\
         \"datePublished\":\"2026-01-02T03:04:05Z\",\
         \"dateModified\":\"2026-01-03T03:04:05Z\",\
         \"author\":{\"@type\":\"Person\",\"name\":\"A Writer\"}}</script>\
         </head><body>",
    );

    out.push_str("<header class='site-header'><nav class='site-nav'><ul>");
    for n in 0..40 {
        out.push_str(&format!(
            "<li class='nav-item nav-item-{n}'><a class='nav-link' \
             href='/section{n}' data-track='nav-{n}'>Section {n}</a></li>"
        ));
    }
    out.push_str("</ul></nav></header><main><article class='post'>");
    out.push_str("<h1 class='post-title'>An Article About Something</h1>");

    // Prose until there is as much of it as a real page of this size carries,
    // wrapped in the attribute heavy markup a template generator emits.
    let mut section = 0;
    let mut prose = 0;
    while prose < prose_target {
        let heading = format!("Part {section} of the article");
        out.push_str(&format!(
            "<section class='post-section' id='section-{section}'>\
             <h2 class='section-heading'>{heading}</h2>\
             <div class='section-body'><p class='para'>"
        ));
        prose += heading.len();
        for sentence in 0..8 {
            let text = format!(
                "This is sentence {sentence} of part {section} of article {seed}, and it \
                 says something ordinary about the subject in the way that a page \
                 on the web tends to. "
            );
            prose += text.len();
            out.push_str(&text);
        }
        out.push_str(&format!(
            "</p><p class='para'>It links to <a class='inline-link' \
             href='/section{section}/page-{seed}'>another page</a> and to \
             <a class='inline-link external' \
             href='https://elsewhere{section}.example/x' rel='nofollow'>somewhere \
             else</a>.</p></div></section>"
        ));
        prose += "It links to another page and to somewhere else. ".len();
        section += 1;
    }

    out.push_str("</article><aside class='sidebar'><ul>");
    for n in 0..30 {
        out.push_str(&format!(
            "<li><a class='related' href='/related/{n}'>Related {n}</a></li>"
        ));
    }
    out.push_str("</ul></aside></main><footer class='site-footer'><ul>");
    for n in 0..40 {
        out.push_str(&format!(
            "<li><a class='foot-link' href='/about/{n}'>About {n}</a></li>"
        ));
    }
    out.push_str("</ul><p>Copyright somebody.</p></footer>");

    // The rest of the page is the stylesheet, which is where the bytes of a
    // real 150 KB document mostly go and which carries no text at all. Padding
    // here rather than with more prose is what keeps the ratio honest.
    out.push_str("<style>");
    let mut rule = 0;
    while out.len() < target {
        out.push_str(&format!(
            ".c-{rule} .layout-grid > .cell:nth-child({}) {{ display:flex; \
             margin:0 auto {rule}px; padding:{rule}px .5rem; color:#3a3a3a; \
             border-bottom:1px solid rgba(0,0,0,.08); }}",
            rule % 12 + 1
        ));
        rule += 1;
    }
    out.push_str("</style><script>window.__t=window.__t||[];</script></body></html>");
    out
}
