//! Bits both benchmarks need: a page generator and a stopwatch.
//!
//! Shared rather than copied because the page generator is the part of a
//! benchmark that is easiest to get wrong, and two copies of it drift. The
//! first version of `rows.rs` produced a page that was 99 percent prose and
//! reported a cost ten times the real one, and a second copy of that mistake in
//! `tick.rs` would have made the two benchmarks agree with each other and with
//! nothing else.
//!
//! Each benchmark compiles this file separately, so a helper only one of them
//! uses looks dead to the other one.
#![allow(dead_code)]

use std::time::{Duration, Instant};

/// Doc 10.2's median index worthy page is about 150 KB of HTML.
pub const MEDIAN_HTML: usize = 150 * 1024;

/// How much of a real page is prose.
///
/// This number is the whole benchmark. Every stage of the row builder except
/// the chunk tree costs time proportional to the extracted text and not to the
/// HTML that carried it, so a generator that turns 150 KB of HTML into 150 KB
/// of text measures a page that does not exist.
///
/// Ten percent is where real pages sit. A 150 KB article ships a stylesheet, a
/// tracker, a JSON-LD block, a nav, a sidebar and a footer, and what is left
/// over for the prose is one or two thousand words. Doc 11.9's budget of 0.8 to
/// 1.5 ms on a 150 KB document is a budget for a document like that.
pub const TEXT_SHARE: f64 = 0.10;

/// One measurement: how long, over how many items.
#[derive(Clone, Copy)]
pub struct Run {
    pub elapsed: Duration,
    pub items: usize,
}

impl Run {
    pub fn per_item(self) -> Duration {
        self.elapsed / u32::try_from(self.items).unwrap_or(u32::MAX)
    }

    pub fn per_second(self) -> f64 {
        self.items as f64 / self.elapsed.as_secs_f64()
    }
}

/// Best of `n`, because the worst case on a shared machine is the scheduler
/// and the best case is the code.
pub fn best(n: usize, mut body: impl FnMut() -> usize) -> Run {
    best_of(n, || (), |()| body())
}

/// Best of `n` where each run needs its own state built first.
///
/// The setup is outside the stopwatch. A crawl benchmark has to hand every run
/// a fresh frontier, because a drained one leases nothing, and timing the
/// seeding of a quarter of a million URLs would measure the test harness.
pub fn best_of<S>(n: usize, mut setup: impl FnMut() -> S, mut body: impl FnMut(S) -> usize) -> Run {
    let mut best = Run {
        elapsed: Duration::MAX,
        items: 1,
    };
    for _ in 0..n {
        let state = setup();
        let at = Instant::now();
        let items = body(state);
        let elapsed = at.elapsed();
        if elapsed < best.elapsed {
            best = Run { elapsed, items };
        }
    }
    best
}

/// A page shaped like doc 10.2's median: an article with sections, 50 links
/// that mostly point at its own site, and a nav bar that the boilerplate
/// scorer has to throw away.
///
/// A pure function of the index, so two runs on two machines work on the same
/// bytes, which is the same rule `umi_file::sample` follows and for the same
/// reason.
pub fn html_of(seed: usize, target: usize) -> String {
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
