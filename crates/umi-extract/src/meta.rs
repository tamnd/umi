//! Metadata and snippets, which is doc 11.6.
//!
//! Every field here has a precedence list rather than a best guess, and the
//! lists come from the spec rather than from this file's judgement. That is doc
//! 11.1 again: two fetchers looking at the same bytes have to produce the same
//! title, and "whichever one looked more like a title" is not a rule two
//! implementations can agree on.
//!
//! Where a value is derived rather than declared, the row says so. A description
//! lifted out of the first paragraph is a different kind of fact from one the
//! author wrote, and a consumer that cannot tell them apart will end up quoting
//! our guess as the publisher's words.

use serde_json::Value;

use crate::dom::{Dom, Kind, Tag};
use crate::links::{LinkKind, Links, Rel};
use crate::text::plain_text;

/// Doc 11.6: the title is trimmed to this many bytes.
pub const MAX_TITLE: usize = 512;

/// Doc 11.6: a description taken from the first paragraph is cut to this many
/// bytes, on a word boundary.
pub const MAX_DERIVED_DESCRIPTION: usize = 300;

/// How long an author written description is allowed to be.
///
/// Doc 11.6 caps the derived one at 300 bytes and says nothing about the
/// declared one, which in practice means unbounded, and unbounded is not a
/// column. Some sites put the whole article in `meta[name=description]`. This is
/// generous enough that no real description is touched and small enough that the
/// page cannot smuggle its body through this field. Recorded for a spec edit.
pub const MAX_DESCRIPTION: usize = 1024;

/// Doc 11.6: at most this many headings.
pub const MAX_HEADINGS: usize = 64;

/// How long one heading is allowed to be.
///
/// Not in doc 11.6, which caps the count and not the length. A page with one
/// `<h1>` holding four kilobytes of text does not have a heading, and 64 of
/// those is a quarter of a megabyte of snippets on a row doc 10 budgets far less
/// for. Recorded for a spec edit.
pub const MAX_HEADING: usize = 300;

/// How many feed URLs are kept.
///
/// Doc 11.6 does not cap this. Sites that declare one feed per category declare
/// hundreds, and doc 09's realtime path wants a site's feeds rather than its
/// index of feeds. Recorded for a spec edit.
pub const MAX_FEEDS: usize = 16;

/// How long a single JSON-LD field is allowed to be.
const MAX_LD_FIELD: usize = 128;

/// How many `<meta>` elements are indexed.
///
/// A normal page has a dozen and the worst real page in the corpus has a few
/// hundred, all of them in the head. A page with more than this has them
/// somewhere the head is not, and doc 11.6's six fields are not down there.
const MAX_META: usize = 256;

/// How deep the JSON-LD walk goes.
///
/// `@graph` nests, and a hostile document nests further. Every field worth
/// having sits at the top of a block or one `@graph` down.
const MAX_LD_DEPTH: u32 = 8;

/// Where the title came from, in doc 11.6's precedence order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TitleSource {
    /// `<title>`.
    Title,
    /// `og:title`.
    OpenGraph,
    /// The first `<h1>` of the content, which is the last resort.
    Heading,
}

/// Where the description came from, in doc 11.6's precedence order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DescriptionSource {
    /// `meta[name=description]`.
    Meta,
    /// `og:description`.
    OpenGraph,
    /// `twitter:description`.
    Twitter,
    /// The first paragraph of the extracted markdown. This one is ours rather
    /// than the publisher's, which is what the flag on it is for.
    FirstParagraph,
}

/// One heading from the extracted subtree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Heading {
    /// 1, 2 or 3.
    pub level: u8,
    /// The heading text, collapsed and cut to [`MAX_HEADING`] bytes on a
    /// character boundary.
    pub text: String,
}

/// The five JSON-LD fields doc 11.6 keeps.
///
/// The blob is not kept. On a product page it routinely runs longer than the
/// page content, and five fields is what anybody actually reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Structured {
    /// Every `@type` seen, in document order, deduplicated.
    pub types: Vec<String>,
    /// `datePublished`, exactly as the page wrote it.
    pub published: Option<String>,
    /// `dateModified`, exactly as the page wrote it.
    pub modified: Option<String>,
    /// `author.name`, or `author` when it is a bare string.
    pub author: Option<String>,
    /// `headline`.
    pub headline: Option<String>,
}

/// Doc 11.6's metadata for one page.
///
/// The dates are four fields rather than two because doc 11.6 asks for separate
/// columns rather than one reconciled column. Reconciling them means picking one
/// to trust, they disagree constantly, and a consumer who can see both makes
/// that call with more context than we have here.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Meta {
    /// The title, cut to [`MAX_TITLE`] bytes.
    pub title: Option<String>,
    /// Which rule produced the title.
    pub title_source: Option<TitleSource>,
    /// The description.
    pub description: Option<String>,
    /// Which rule produced the description.
    pub description_source: Option<DescriptionSource>,
    /// `link[rel=canonical]`, or `rel=canonical` from the `Link` header,
    /// canonicalised by doc 11.2. Recorded, never acted on at crawl time.
    pub canonical: Option<String>,
    /// `article:published_time`.
    pub published: Option<String>,
    /// `article:modified_time`.
    pub modified: Option<String>,
    /// The JSON-LD fields.
    pub structured: Structured,
    /// The page carries microdata. Detected, not parsed, per doc 11.6.
    pub microdata: bool,
    /// The page carries RDFa. Detected, not parsed, per doc 11.6.
    pub rdfa: bool,
    /// `h1` through `h3` from the extracted subtree, in document order.
    pub headings: Vec<Heading>,
    /// RSS and Atom feeds, canonicalised. Doc 09's realtime path takes these,
    /// and doc 11.6 is right that this is the cheapest freshness signal on the
    /// web and that most crawlers walk past it.
    pub feeds: Vec<String>,
    /// The `lang` attribute on `<html>`, ASCII lowercased.
    ///
    /// Kept apart from detected language, which is separate work, because the
    /// publisher's claim and a detector's answer disagree often enough to be
    /// worth having both.
    pub declared_lang: Option<String>,
}

impl Meta {
    /// Whether the description is ours rather than the publisher's.
    pub fn description_derived(&self) -> bool {
        self.description_source == Some(DescriptionSource::FirstParagraph)
    }
}

/// Everything doc 11.6 asks for, for a page whose content we may keep.
///
/// `root` is doc 11.3's content root, so the headings are the article's rather
/// than the template's. `markdown` is the rendered body, which the description
/// fallback reads and nothing else here touches.
pub fn collect(
    dom: &Dom,
    root: usize,
    markdown: &str,
    links: &Links,
    link_header: Option<&str>,
) -> Meta {
    let index = meta_index(dom);
    let mut meta = common(dom, &index, links, link_header, true);
    if let Some((text, source)) = title(dom, &index, root) {
        meta.title = Some(text);
        meta.title_source = Some(source);
    }
    let (description, source) = description(dom, &index, markdown);
    meta.description = description;
    meta.description_source = source;
    meta.headings = headings(dom, root);
    meta
}

/// The part of doc 11.6 that survives a `noindex`.
///
/// Doc 11.4 withholds the markdown, the title, the description and the snippets,
/// and says the row is still written with everything else. Everything else is
/// this: where the page says it really lives, when it says it changed, which
/// vocabularies it uses and where its feeds are. None of that is content, all of
/// it is a fact about the frontier, and `noindex` is a statement about indexing
/// rather than about existing.
pub fn frontier(dom: &Dom, links: &Links, link_header: Option<&str>) -> Meta {
    common(dom, &meta_index(dom), links, link_header, false)
}

/// The fields that do not depend on the content root.
///
/// `content` is false on a withheld page, which keeps the dates and the types
/// and drops the headline and the author, because those two are the page's own
/// words under another name.
fn common(
    dom: &Dom,
    index: &[usize],
    links: &Links,
    link_header: Option<&str>,
    content: bool,
) -> Meta {
    Meta {
        canonical: canonical(links).or_else(|| link_header.and_then(header_canonical)),
        published: meta_content(dom, index, &["article:published_time"]),
        modified: meta_content(dom, index, &["article:modified_time"]),
        structured: structured(dom, content),
        microdata: dom.microdata(),
        rdfa: dom.rdfa(),
        feeds: feeds(links),
        declared_lang: declared_lang(dom),
        ..Meta::default()
    }
}

/// Doc 11.6's title precedence: `<title>`, then `og:title`, then the first `h1`.
fn title(dom: &Dom, index: &[usize], root: usize) -> Option<(String, TitleSource)> {
    if let Some(text) = dom.first(Tag::Title).map(|id| collapsed(dom, id))
        && !text.is_empty()
    {
        return Some((truncate(text, MAX_TITLE), TitleSource::Title));
    }
    if let Some(text) = meta_content(dom, index, &["og:title"]) {
        return Some((truncate(text, MAX_TITLE), TitleSource::OpenGraph));
    }
    // The first `h1` of the content, not of the document. A masthead `h1` with
    // the site's name in it is not this page's title, and the content root is
    // exactly the thing that knows the difference.
    let text = subtree(dom, root)
        .into_iter()
        .find(|&id| dom.tag(id) == Some(Tag::Heading(1)))
        .map(|id| collapsed(dom, id))
        .filter(|text| !text.is_empty())?;
    Some((truncate(text, MAX_TITLE), TitleSource::Heading))
}

/// Doc 11.6's description precedence, ending in the derived fallback.
fn description(
    dom: &Dom,
    index: &[usize],
    markdown: &str,
) -> (Option<String>, Option<DescriptionSource>) {
    for (names, source) in [
        (&["description"][..], DescriptionSource::Meta),
        (&["og:description"][..], DescriptionSource::OpenGraph),
        (&["twitter:description"][..], DescriptionSource::Twitter),
    ] {
        if let Some(text) = meta_content(dom, index, names) {
            return (Some(truncate(text, MAX_DESCRIPTION)), Some(source));
        }
    }
    let Some(paragraph) = first_paragraph(markdown) else {
        return (None, None);
    };
    (
        Some(on_word_boundary(paragraph, MAX_DERIVED_DESCRIPTION)),
        Some(DescriptionSource::FirstParagraph),
    )
}

/// The first prose paragraph of the markdown, as plain text.
///
/// Headings, list items, quotes, tables and fenced code are skipped. A page
/// whose first block is a heading already has a title, and repeating it as the
/// description helps nobody.
///
/// This reads lines rather than splitting on blank lines, because a fenced code
/// block with a blank line in it is one block to markdown and two to anything
/// that splits on a blank line, and the half carrying the closing fence does not
/// look like a fence.
fn first_paragraph(markdown: &str) -> Option<String> {
    let mut fence = false;
    let mut block = String::new();
    for line in markdown.lines() {
        if line.trim_start().starts_with("```") {
            fence = !fence;
            block.clear();
            continue;
        }
        if fence {
            continue;
        }
        if line.trim().is_empty() {
            if let Some(text) = prose(&block) {
                return Some(text);
            }
            block.clear();
            continue;
        }
        if !block.is_empty() {
            block.push(' ');
        }
        block.push_str(line.trim());
    }
    prose(&block)
}

/// One markdown block as plain text, if it is prose at all.
fn prose(block: &str) -> Option<String> {
    if block.is_empty()
        || block.starts_with('#')
        || block.starts_with('>')
        || block.starts_with('|')
        || block.starts_with("- ")
        || block.starts_with("---")
    {
        return None;
    }
    // An ordered list item, `1. ` and friends, which `starts_with` cannot spell
    // without knowing how many digits it has.
    let digits = block.chars().take_while(char::is_ascii_digit).count();
    if digits > 0 && block[digits..].starts_with(". ") {
        return None;
    }
    non_empty(plain_text(block))
}

/// `h1` through `h3` from the extracted subtree, in document order.
fn headings(dom: &Dom, root: usize) -> Vec<Heading> {
    let mut out = Vec::new();
    for id in subtree(dom, root) {
        let Some(Tag::Heading(level)) = dom.tag(id) else {
            continue;
        };
        if level > 3 {
            continue;
        }
        let text = collapsed(dom, id);
        if text.is_empty() {
            continue;
        }
        out.push(Heading {
            level,
            text: truncate(text, MAX_HEADING),
        });
        if out.len() == MAX_HEADINGS {
            break;
        }
    }
    out
}

/// The feeds, taken from the link pass rather than found again.
///
/// Doc 11.4 already decided which `<link>` elements are feeds and already
/// canonicalised them. Finding them twice is how the two answers end up
/// disagreeing on some page nobody looks at for a year.
fn feeds(links: &Links) -> Vec<String> {
    links
        .links
        .iter()
        .filter(|link| link.kind == LinkKind::Feed)
        .map(|link| link.url.clone())
        .take(MAX_FEEDS)
        .collect()
}

/// `link[rel=canonical]`, from the link pass for the same reason as the feeds.
fn canonical(links: &Links) -> Option<String> {
    links
        .links
        .iter()
        .find(|link| link.kind == LinkKind::Link && link.rel.has(Rel::CANONICAL))
        .map(|link| link.url.clone())
}

/// `rel=canonical` out of a `Link` header.
///
/// The grammar is RFC 8288: comma separated entries, each an angle bracketed
/// target followed by semicolon separated parameters. This reads the one
/// parameter it needs and no more, because doc 11.5 keeps the header verbatim
/// and anything else that wants a field out of it can have its own reader.
///
/// The target is not resolved or canonicalised here. It comes from the fetch
/// path, which is where the request URL lives, and a relative target in a `Link`
/// header resolves against the request URL rather than against `<base>`.
fn header_canonical(header: &str) -> Option<String> {
    for entry in entries(header) {
        let entry = entry.trim();
        let Some(rest) = entry.strip_prefix('<') else {
            continue;
        };
        let Some(close) = rest.find('>') else {
            continue;
        };
        let (target, params) = rest.split_at(close);
        let canonical = params[1..].split(';').any(|param| {
            let Some((name, value)) = param.split_once('=') else {
                return false;
            };
            name.trim().eq_ignore_ascii_case("rel")
                && value
                    .trim()
                    .trim_matches('"')
                    .split_ascii_whitespace()
                    .any(|token| token.eq_ignore_ascii_case("canonical"))
        });
        if canonical {
            return non_empty(target.trim().to_owned());
        }
    }
    None
}

/// Split a `Link` header on the commas that separate entries.
///
/// Not `split(',')`, because a comma is legal inside the angle brackets and
/// inside a quoted parameter, and both turn up: a target with a comma in a path
/// segment and a `title` holding a list are the two seen in real traffic.
fn entries(header: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut angle = false;
    let mut quoted = false;
    for (at, byte) in header.bytes().enumerate() {
        match byte {
            b'"' => quoted = !quoted,
            b'<' if !quoted => angle = true,
            b'>' if !quoted => angle = false,
            b',' if !quoted && !angle => {
                out.push(&header[start..at]);
                start = at + 1;
            }
            _ => {}
        }
    }
    out.push(&header[start..]);
    out
}

/// The `lang` attribute on `<html>`.
fn declared_lang(dom: &Dom) -> Option<String> {
    let value = dom
        .first(Tag::Html)
        .and_then(|id| dom.element(id))
        .and_then(|element| element.attr("lang"))?
        .trim();
    // ASCII folding rather than `to_lowercase`, because doc 11.1 rules out
    // locale dependent folding on anything published, and a Turkish locale folds
    // the `I` in `IT` to a dotless one.
    non_empty(truncate(value.to_ascii_lowercase(), 64))
}

/// Every `<meta>` element, in document order, found in one walk.
///
/// Doc 11.6 reads six fields out of these, and a scan per field is six walks of
/// an arena that has fifty thousand nodes in it on a bad page. A page has a
/// couple of dozen `<meta>` elements, so one walk and a short list is the whole
/// of the optimisation and it is worth about a millisecond a page.
fn meta_index(dom: &Dom) -> Vec<usize> {
    let mut index = Vec::new();
    for id in 0..dom.node_count() {
        if dom.tag(id) == Some(Tag::Meta) {
            index.push(id);
            if index.len() == MAX_META {
                break;
            }
        }
    }
    index
}

/// The content of the first `<meta>` whose `name` or `property` matches.
///
/// Both attributes, because OpenGraph says `property` and half the web writes
/// `name`, and a reader that insists on the correct one misses a third of the
/// pages that have the field.
fn meta_content(dom: &Dom, index: &[usize], names: &[&str]) -> Option<String> {
    for &id in index {
        let Some(element) = dom.element(id) else {
            continue;
        };
        // A `<meta charset>` has neither attribute and there is one of those on
        // most pages, so this skips the element rather than ending the scan.
        let Some(key) = element.attr("property").or_else(|| element.attr("name")) else {
            continue;
        };
        if !names
            .iter()
            .any(|name| key.trim().eq_ignore_ascii_case(name))
        {
            continue;
        }
        let content = collapse(element.attr("content").unwrap_or(""));
        if !content.is_empty() {
            return Some(content);
        }
    }
    None
}

/// The five JSON-LD fields, out of every block on the page.
fn structured(dom: &Dom, content: bool) -> Structured {
    let mut out = Structured::default();
    for block in dom.ld_json() {
        // A block that does not parse is skipped rather than being an error.
        // Invalid JSON-LD is extremely common and it is not a reason to lose the
        // rest of the page.
        let Ok(value) = serde_json::from_str::<Value>(block) else {
            continue;
        };
        walk_ld(&value, &mut out, content, 0);
    }
    out
}

/// Read one JSON-LD value, descending through arrays and `@graph`.
fn walk_ld(value: &Value, out: &mut Structured, content: bool, depth: u32) {
    if depth > MAX_LD_DEPTH {
        return;
    }
    match value {
        Value::Array(items) => {
            for item in items {
                walk_ld(item, out, content, depth + 1);
            }
        }
        Value::Object(object) => {
            for kind in types_of(object.get("@type")) {
                if !out.types.contains(&kind) {
                    out.types.push(kind);
                }
            }
            // First writer wins, matching every other precedence rule here, so
            // that a page with an `Article` block followed by a `WebPage` block
            // keeps the article's dates.
            fill(&mut out.published, object.get("datePublished"));
            fill(&mut out.modified, object.get("dateModified"));
            if content {
                fill(&mut out.headline, object.get("headline"));
                if out.author.is_none()
                    && let Some(name) = author_of(object.get("author"))
                {
                    out.author = Some(truncate(name, MAX_LD_FIELD));
                }
            }
            if let Some(graph) = object.get("@graph") {
                walk_ld(graph, out, content, depth + 1);
            }
        }
        _ => {}
    }
}

/// The `@type` values, which is a string or an array of them.
fn types_of(value: Option<&Value>) -> Vec<String> {
    let one = |text: &str| non_empty(truncate(collapse(text), MAX_LD_FIELD));
    match value {
        Some(Value::String(kind)) => one(kind).into_iter().collect(),
        Some(Value::Array(many)) => many
            .iter()
            .filter_map(Value::as_str)
            .filter_map(one)
            .collect(),
        _ => Vec::new(),
    }
}

/// `author` is a string, an object with a `name`, or an array of either.
fn author_of(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(name) => non_empty(collapse(name)),
        Value::Object(object) => non_empty(collapse(object.get("name")?.as_str()?)),
        Value::Array(many) => many.iter().find_map(|one| author_of(Some(one))),
        _ => None,
    }
}

/// Take a string field, if it is one and if we do not have it yet.
fn fill(slot: &mut Option<String>, value: Option<&Value>) {
    if slot.is_some() {
        return;
    }
    if let Some(text) = value.and_then(Value::as_str) {
        *slot = non_empty(truncate(collapse(text), MAX_LD_FIELD));
    }
}

/// A string, unless it is empty.
fn non_empty(text: String) -> Option<String> {
    (!text.is_empty()).then_some(text)
}

/// A subtree in document order.
///
/// Chrome stops it, the same way it stops the link pass's content mask: a
/// heading in a footer is not one of this page's headings.
fn subtree(dom: &Dom, root: usize) -> Vec<usize> {
    let mut order = Vec::new();
    let mut stack = vec![root];
    while let Some(id) = stack.pop() {
        if dom.chrome(id) {
            continue;
        }
        order.push(id);
        stack.extend(dom.children(id).iter().rev());
    }
    order
}

/// All the text under a node, whitespace collapsed.
fn collapsed(dom: &Dom, id: usize) -> String {
    let mut out = String::new();
    for node in subtree(dom, id) {
        if let Kind::Text(raw) = dom.kind(node) {
            for word in raw.split_ascii_whitespace() {
                if !out.is_empty() {
                    out.push(' ');
                }
                out.push_str(word);
            }
        }
        // Every caller cuts to this or less, so there is no reason to keep
        // walking a heading that turned out to be a page.
        if out.len() >= MAX_TITLE {
            break;
        }
    }
    out
}

/// Runs of ASCII whitespace to one space, trimmed.
fn collapse(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for word in text.split_ascii_whitespace() {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(word);
    }
    out
}

/// Cut to at most `max` bytes without splitting a character.
fn truncate(mut text: String, max: usize) -> String {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text
}

/// Cut to at most `max` bytes on a word boundary, which is what doc 11.6 asks
/// for on the derived description.
///
/// Falls back to a character boundary when the first word runs past the limit,
/// because a language that does not put spaces between words is not a reason to
/// return nothing.
fn on_word_boundary(text: String, max: usize) -> String {
    if text.len() <= max {
        return text;
    }
    let cut = truncate(text, max);
    match cut.rfind(' ') {
        Some(space) if space > 0 => cut[..space].to_owned(),
        _ => cut,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json(text: &str) -> Value {
        serde_json::from_str(text).expect("the fixture is valid json")
    }

    #[test]
    fn a_link_header_gives_up_its_canonical() {
        assert_eq!(
            header_canonical("<https://example.com/real>; rel=\"canonical\""),
            Some("https://example.com/real".to_owned())
        );
        // Several entries, and the canonical one is not first.
        assert_eq!(
            header_canonical(
                "<https://example.com/2>; rel=\"next\", <https://example.com/real>; rel=canonical"
            ),
            Some("https://example.com/real".to_owned())
        );
        assert_eq!(header_canonical("<https://example.com/2>; rel=next"), None);
        assert_eq!(header_canonical("nonsense"), None);
    }

    #[test]
    fn a_comma_inside_a_link_header_entry_does_not_split_it() {
        assert_eq!(
            header_canonical("<https://example.com/a,b>; rel=canonical"),
            Some("https://example.com/a,b".to_owned())
        );
        assert_eq!(
            header_canonical("<https://example.com/x>; title=\"one, two\"; rel=canonical"),
            Some("https://example.com/x".to_owned())
        );
    }

    #[test]
    fn a_derived_description_stops_at_a_space() {
        assert_eq!(on_word_boundary("one two three".to_owned(), 11), "one two");
        // A single word longer than the limit still comes back cut rather than
        // empty, which is what happens in a language written without spaces.
        let cut = on_word_boundary("\u{3042}".repeat(50), 10);
        assert_eq!(cut.len(), 9);
    }

    #[test]
    fn the_first_paragraph_skips_everything_that_is_not_prose() {
        let markdown = "# A heading\n\n- a list item\n\n> a quote\n\n| a | b |\n\nThe prose.";
        assert_eq!(first_paragraph(markdown).as_deref(), Some("The prose."));
        assert_eq!(first_paragraph("# Only a heading"), None);
        assert_eq!(first_paragraph(""), None);
        // Escapes come back out, because this is a snippet and not markdown.
        assert_eq!(
            first_paragraph(r"snake\_case").as_deref(),
            Some("snake_case")
        );
    }

    #[test]
    fn a_fenced_code_block_is_not_the_first_paragraph() {
        // The blank line inside the fence is the thing that trips a reader which
        // splits blocks on blank lines.
        let markdown = "```rust\nfn main() {}\n\nlet x = 1;\n```\n\nThe prose.";
        assert_eq!(first_paragraph(markdown).as_deref(), Some("The prose."));
    }

    #[test]
    fn an_author_is_a_string_an_object_or_a_list() {
        assert_eq!(author_of(Some(&json(r#""Ada""#))).as_deref(), Some("Ada"));
        assert_eq!(
            author_of(Some(&json(r#"{"name": "Ada"}"#))).as_deref(),
            Some("Ada")
        );
        assert_eq!(
            author_of(Some(&json(r#"[{"name": "Ada"}, "Grace"]"#))).as_deref(),
            Some("Ada")
        );
        assert_eq!(author_of(Some(&json("42"))), None);
        assert_eq!(author_of(Some(&json(r#"{"url": "/ada"}"#))), None);
    }

    #[test]
    fn a_type_is_one_string_or_a_list_of_them() {
        assert_eq!(types_of(Some(&json(r#""Article""#))), ["Article"]);
        assert_eq!(
            types_of(Some(&json(r#"["Article", "NewsArticle"]"#))),
            ["Article", "NewsArticle"]
        );
        assert!(types_of(Some(&json(r#"{"@id": "x"}"#))).is_empty());
        assert!(types_of(None).is_empty());
    }
}
