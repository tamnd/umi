//! Main content detection, which is doc 11.3 steps 1 to 3.
//!
//! Every number here is an integer and every threshold is an integer
//! comparison. Doc 11.1 rules out floating point in any scoring path that can
//! flip a decision, and link density is the one quantity that wants to be a
//! ratio, so it is carried in fixed point hundredths and compared against 66
//! rather than against 0.66. There is no `f32` or `f64` in this file and there
//! is a test in `lib.rs` that reads the source and says so.

use crate::dom::{Dom, Kind, ROOT, Tag};

/// A paragraph has to carry this many bytes before it counts as one.
///
/// A `<p>` holding a date, a byline or the word "Share" is not evidence of
/// prose, and boilerplate blocks are full of them.
const MIN_PARAGRAPH: u32 = 25;

/// What one qualifying paragraph is worth, in bytes of text.
const PARAGRAPH_BONUS: i64 = 25;

/// What one byte of link text costs.
///
/// Three rather than one, because a navigation block is mostly link text and
/// subtracting it once only brings it level with prose of the same length.
const LINK_PENALTY: i64 = 3;

/// What one matched boilerplate marker costs.
const MARKER_PENALTY: i64 = 150;

/// How many markers on one node are counted. A class list with fifteen of them
/// is not fifteen times as much boilerplate as one.
const MARKER_CAP: i64 = 3;

/// Doc 11.3 step 3: below this many bytes of text the winner is not trusted.
const MIN_TEXT: u32 = 200;

/// Doc 11.3 step 3: above this link density, in hundredths, the winner is not
/// trusted.
const MAX_DENSITY: u32 = 66;

/// The boilerplate markers, matched as ASCII lowercase substrings of `class`
/// and `id`.
///
/// This is the fixed published list doc 11.3 step 2 refers to. It ships as code
/// rather than as data because changing it changes extraction output, which is
/// a major version bump under doc 11.10, and a list that ships as a file invites
/// somebody to edit it without bumping anything.
///
/// `ad` is deliberately absent as a bare word. It matches `add`, `admin`,
/// `header` matches nothing useful on its own here, and a marker that fires on
/// `<div class="loading">` costs more than the advertising block it catches.
const MARKERS: [&str; 30] = [
    "advert",
    "banner",
    "breadcrumb",
    "byline",
    "comment",
    "cookie",
    "disqus",
    "footer",
    "masthead",
    "menu",
    "nav-",
    "navbar",
    "navigation",
    "newsletter",
    "pagination",
    "popular",
    "promo",
    "related",
    "share",
    "sidebar",
    "site-info",
    "skip-link",
    "social",
    "sponsor",
    "subscribe",
    "toolbar",
    "trending",
    "widget",
    "-ad-",
    "_ad_",
];

/// What a node carries, summed over its whole subtree.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Bytes of text after whitespace collapsing.
    pub text: u32,
    /// Bytes of that text that sit inside an `<a>`.
    pub link_text: u32,
    /// Descendant paragraphs carrying at least `MIN_PARAGRAPH` bytes.
    pub paragraphs: u32,
}

impl Stats {
    /// Link density in hundredths. Zero for a node with no text, which is the
    /// right answer for scoring even though the ratio is undefined.
    pub fn density(self) -> u32 {
        if self.text == 0 {
            0
        } else {
            self.link_text.saturating_mul(100) / self.text
        }
    }
}

/// The chosen subtree and what we know about the choice.
#[derive(Clone, Copy, Debug)]
pub struct Choice {
    /// The node the markdown is serialised from.
    pub root: usize,
    /// Doc 11.3 step 3 fired and we fell back to the whole `<body>`.
    pub uncertain: bool,
    /// The root came from step 1, an `<article>`, `<main>` or `articleBody`,
    /// rather than from scoring.
    pub declared: bool,
    /// Stats for the chosen root.
    pub stats: Stats,
    /// The chosen root's share of the body's text, in hundredths. One of doc
    /// 11.6's quality signals.
    pub share: u32,
}

/// Score every node in one pass and pick the content root.
pub fn choose(dom: &Dom) -> Choice {
    let stats = collect(dom);
    let body = dom.first(Tag::Body).unwrap_or(ROOT);

    let declared = declare(dom, &stats);
    let mut root = declared.or_else(|| best(dom, &stats)).unwrap_or(body);

    // Step 3. A winner that carries almost nothing, or that is almost all link
    // text, is not a winner. Falling back to the body keeps the links, which on
    // a directory page are the entire value of the page.
    let mut uncertain = false;
    if stats[root].text < MIN_TEXT || stats[root].density() > MAX_DENSITY {
        uncertain = true;
        root = body;
    }

    let body_text = stats[body].text;
    let share = if body_text == 0 {
        0
    } else {
        stats[root].text.saturating_mul(100) / body_text
    };

    Choice {
        root,
        uncertain,
        declared: declared.is_some() && !uncertain,
        stats: stats[root],
        share,
    }
}

/// Sum text, link text and paragraphs over every subtree.
///
/// The arena is built parent before child, so walking it backwards visits every
/// node after all of its descendants. That makes this one reverse loop instead
/// of a recursive post order walk, which matters on the deeply nested pages the
/// golden corpus covers.
fn collect(dom: &Dom) -> Vec<Stats> {
    let mut stats = vec![Stats::default(); dom.node_count()];
    for id in (0..dom.node_count()).rev() {
        let mut text = match dom.kind(id) {
            Kind::Text(raw) => collapsed_len(raw),
            _ => 0,
        };
        let mut link_text = 0u32;
        let mut paragraphs = 0u32;
        for &child in dom.children(id) {
            text = text.saturating_add(stats[child].text);
            link_text = link_text.saturating_add(stats[child].link_text);
            paragraphs = paragraphs.saturating_add(stats[child].paragraphs);
        }
        match dom.tag(id) {
            // Nested anchors are not legal HTML and html5ever will not build
            // them, so taking the whole subtree here cannot double count.
            Some(Tag::A) => link_text = text,
            Some(Tag::P) if text >= MIN_PARAGRAPH => paragraphs = paragraphs.saturating_add(1),
            _ => {}
        }
        stats[id] = Stats {
            text,
            link_text,
            paragraphs,
        };
    }
    stats
}

/// Doc 11.3 step 1, the declared root.
///
/// The spec says a document with `<article>`, `<main>` or `articleBody` hands us
/// the root. It does not say which one, and real pages have all three at once
/// and have eight `<article>` tags on an index page. The order is most explicit
/// first: a schema.org `articleBody` is an assertion by the publisher about this
/// exact node, `<article>` is a claim about a self contained composition, and
/// `<main>` is only a claim about the page. Within one kind the highest scoring
/// node wins, so an index page of teasers picks the longest teaser and then
/// almost always fails step 3 and falls back, which is the right outcome.
fn declare(dom: &Dom, stats: &[Stats]) -> Option<usize> {
    let body = dom.first(Tag::Body).unwrap_or(ROOT);
    let article_body = |id: usize| {
        dom.element(id).is_some_and(|element| {
            element.attr("itemprop") == Some("articleBody")
                || element.attr("property").is_some_and(|value| {
                    value.eq_ignore_ascii_case("articleBody")
                        || value.eq_ignore_ascii_case("schema:articleBody")
                })
        })
    };

    let pick = |wanted: &dyn Fn(usize) -> bool| -> Option<usize> {
        (0..dom.node_count())
            .filter(|&id| id != body && wanted(id))
            .max_by_key(|&id| (score(dom, stats, id), std::cmp::Reverse(id)))
    };

    pick(&article_body)
        .or_else(|| pick(&|id| dom.tag(id) == Some(Tag::Article)))
        .or_else(|| pick(&|id| dom.tag(id) == Some(Tag::Main)))
}

/// Doc 11.3 step 2, the highest scoring candidate container.
fn best(dom: &Dom, stats: &[Stats]) -> Option<usize> {
    (0..dom.node_count())
        .filter(|&id| dom.tag(id).is_some_and(Tag::is_candidate))
        // Ties go to the node that appeared first, because document order is
        // the only tiebreak that does not depend on how the arena was built.
        .max_by_key(|&id| (score(dom, stats, id), std::cmp::Reverse(id)))
}

/// One node's score.
fn score(dom: &Dom, stats: &[Stats], id: usize) -> i64 {
    let stat = stats[id];
    let Some(element) = dom.element(id) else {
        return i64::MIN;
    };
    let bonus = match element.tag {
        Tag::Article | Tag::Main => 150,
        Tag::Section => 25,
        Tag::Blockquote => -25,
        Tag::Td | Tag::Li => -50,
        _ => 0,
    };
    i64::from(stat.text) + i64::from(stat.paragraphs) * PARAGRAPH_BONUS
        - i64::from(stat.link_text) * LINK_PENALTY
        + bonus
        - markers(element.attr("class"), element.attr("id")) * MARKER_PENALTY
}

/// How many boilerplate markers this node's `class` and `id` match, capped.
fn markers(class: Option<&str>, id: Option<&str>) -> i64 {
    let mut hit = 0i64;
    for value in [class, id].into_iter().flatten() {
        // ASCII lowercase rather than `to_lowercase`, because doc 11.1 rules out
        // locale dependent case folding and a Turkish dotless i in a class name
        // must not change what we extract.
        let value = value.to_ascii_lowercase();
        for marker in MARKERS {
            if value.contains(marker) {
                hit += 1;
            }
        }
    }
    hit.min(MARKER_CAP)
}

/// The byte length of a run of text once whitespace is collapsed the way the
/// serialiser will collapse it.
fn collapsed_len(raw: &str) -> u32 {
    let mut total = 0usize;
    for (n, word) in raw.split_ascii_whitespace().enumerate() {
        if n > 0 {
            total += 1;
        }
        total += word.len();
    }
    u32::try_from(total).unwrap_or(u32::MAX)
}
