//! Turn a fetched HTML document into markdown, plain text and quality signals.
//!
//! This crate implements doc 11.3 and doc 11.4, and the parts of doc 11.6 that
//! do not need language detection. The rule it is built around is doc 11.1:
//! given the same
//! input bytes and the same version of this crate, the output is byte identical
//! on every machine, forever. Doc 04 pushes extraction to fetchers we do not
//! control and doc 06 compares digests across independent fetchers to decide
//! whether a delivery is honest, so a heuristic that drifts between two
//! machines is not a quality problem, it is a protocol failure.
//!
//! What that rules out, and what you will therefore not find in here: hash map
//! iteration anywhere output depends on it, `to_lowercase` on anything that
//! feeds a decision, floating point in any threshold, a wall clock, a random
//! number, and any dependence on how many threads are running. There is a test
//! at the bottom of this file that reads the source and fails if a float
//! appears in it.
//!
//! ```
//! use url::Url;
//!
//! let url = Url::parse("https://example.com/post").unwrap();
//! let page = umi_extract::extract(b"<article><h1>Hi</h1><p>Words.</p></article>", &url);
//! assert_eq!(page.markdown, "# Hi\n\nWords.");
//! assert_eq!(page.text(), "Hi Words.");
//! ```

mod dom;
mod links;
mod markdown;
mod meta;
mod score;
mod sink;
mod text;

use url::Url;

pub use links::{Link, LinkKind, Links, MAX_ANCHOR, MAX_LINKS, Rel, Robots};
pub use meta::{
    DescriptionSource, Heading, MAX_DERIVED_DESCRIPTION, MAX_DESCRIPTION, MAX_FEEDS, MAX_HEADING,
    MAX_HEADINGS, MAX_TITLE, Meta, Structured, TitleSource,
};
pub use text::plain_text;

/// The extractor version, which doc 11.10 says appears in the doc 04 receipt,
/// in the doc 10 segment header and as a column in the published Parquet.
///
/// It is compared exactly. A patch release must not change output for any
/// input, a minor release may add a field, and anything that changes what this
/// crate emits for an input it already handled is a major release.
pub const VERSION: &str = concat!("umi-extract/", env!("CARGO_PKG_VERSION"));

/// The quality signals from doc 11.6 that this crate can compute today.
///
/// Doc 11.6 lists seven. Six are here. `stopword_coverage` arrives with
/// language detection, which doc 11.6 lists as separate work. They are computed
/// and published, never applied:
/// nothing is dropped for scoring badly, because a consumer can filter on a
/// column and cannot recover a page we threw away.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Signals {
    /// Bytes of extracted text, after whitespace collapsing.
    pub text_bytes: u32,
    /// Link density in the extracted subtree, in hundredths.
    pub link_density: u32,
    /// Extracted text as a share of the raw document, in hundredths. A page
    /// that is 2 percent text and 98 percent markup is a template.
    pub extracted_share: u32,
    /// The extracted subtree's share of the body's text, in hundredths. Low
    /// means we cut a lot away, which is either a good extraction or a bad one,
    /// and the consumer gets to decide which.
    pub top_node_share: u32,
    /// Bytes of text dropped with `<script>` and `<style>`.
    pub dropped_bytes: u32,
    /// Links kept, after canonicalisation, deduplication and the 5000 cap.
    pub link_count: u32,
}

/// One extracted document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Extracted {
    /// The markdown, in the fixed CommonMark subset from doc 11.3.
    pub markdown: String,
    /// Doc 11.3 step 3 fired: the scored winner was too small or too link heavy
    /// to trust, so this is the whole body. The page is still a row.
    pub boilerplate_uncertain: bool,
    /// The content root came from an `<article>`, a `<main>` or a schema.org
    /// `articleBody` rather than from scoring.
    pub declared_root: bool,
    /// The document base, which is `<base href>` resolved against the final URL
    /// when the page carries one and the final URL otherwise. Every link in the
    /// markdown is absolute against this.
    pub base: Url,
    /// Doc 11.6's quality signals.
    pub signals: Signals,
    /// Doc 11.4's links, in document order.
    ///
    /// Present even when the content is withheld, because a page we may not
    /// index is still a fact about the frontier and its links are the part we
    /// were never told to forget.
    pub links: Links,
    /// What the page's `meta robots` said, merged with any `X-Robots-Tag` the
    /// caller passed to [`extract_with_headers`].
    pub robots: Robots,
    /// Doc 11.6's metadata and snippets.
    ///
    /// Thinned rather than emptied when the content is withheld: the title, the
    /// description and the headings go, and the canonical URL, the dates, the
    /// feeds and the vocabulary flags stay, because none of those are content
    /// and all of them are facts the frontier needs.
    pub meta: Meta,
    /// Why the content is not here, when it is not.
    ///
    /// When this is set, `markdown` is empty and so is everything derived from
    /// it. Withholding happens in this crate rather than downstream for the same
    /// reason doc 11.3's drop list is applied during the parse: a rule that
    /// every consumer has to remember is a rule that one of them will forget.
    pub content_withheld: Option<Withheld>,
    /// The version that produced this, for the receipt and the Parquet column.
    pub version: &'static str,
}

/// Why a row carries no content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Withheld {
    /// The page said `noindex`, in a `meta robots` tag or an `X-Robots-Tag`
    /// header. Doc 11.4 obeys it.
    Noindex,
}

impl Extracted {
    /// The plain text, which doc 11.3 says is not stored because regenerating
    /// it is cheaper than carrying it.
    pub fn text(&self) -> String {
        plain_text(&self.markdown)
    }

    /// blake3 over the plain text, which is doc 11.7's exact duplicate key.
    ///
    /// Over text and not over raw HTML, because two byte identical articles
    /// differ in their ad slots, and not over markdown, because heading levels
    /// shift between templates carrying the same prose.
    pub fn text_digest(&self) -> [u8; 32] {
        *blake3::hash(self.text().as_bytes()).as_bytes()
    }
}

/// Extract a document.
///
/// `url` is the final URL after redirects, which is what relative links resolve
/// against unless the page carries a `<base href>`. The bytes are decoded as
/// UTF-8 with invalid sequences replaced; transcoding from a declared charset
/// belongs to the caller, because the charset comes off the Content-Type header
/// as often as it comes off a `<meta>` tag and only the caller has both.
pub fn extract(html: &[u8], url: &Url) -> Extracted {
    extract_with_headers(html, url, Headers::default())
}

/// The response headers that change what extraction produces.
///
/// Doc 11.5 keeps sixteen headers and two of them say something this crate has
/// to act on, so those two are what it takes. Passing the pair rather than the
/// whole map keeps the input to this crate down to bytes, a URL and this, which
/// is the thing doc 11.1's "same input, same output" promise is measured
/// against.
#[derive(Clone, Copy, Debug, Default)]
pub struct Headers<'a> {
    /// `X-Robots-Tag`, which can withhold the content the same way a `meta
    /// robots` tag can.
    pub x_robots_tag: Option<&'a str>,
    /// `Link`, which is the other place a `rel=canonical` is allowed to live.
    /// Used only when the document does not carry one.
    pub link: Option<&'a str>,
}

/// Extract a document, with the response headers the caller saw.
///
/// Doc 11.4 obeys `noindex` from either the `X-Robots-Tag` header or the `meta
/// robots` tag, and only the fetch path has the header. Whichever says no, wins.
pub fn extract_with_headers(html: &[u8], url: &Url, headers: Headers<'_>) -> Extracted {
    let tree = dom::Dom::parse(html);
    let base = base_of(&tree, url);
    let choice = score::choose(&tree);

    let robots =
        links::robots(&tree).union(headers.x_robots_tag.map(Robots::parse).unwrap_or_default());
    let found = links::collect(&tree, choice.root, base.as_str(), robots);

    // The links are collected before this and kept regardless. Doc 11.4 is
    // explicit that a `noindex` page is still written as a row with its URL,
    // status, headers and link set, and that the links are still followed
    // unless the same directive also said `nofollow`.
    let withheld = robots.noindex.then_some(Withheld::Noindex);
    let body = if withheld.is_some() {
        String::new()
    } else {
        markdown::render(&tree, choice.root, Some(&base))
    };

    // Doc 11.4 withholds the title, the description and the snippets alongside
    // the markdown, and doc 11.6's other fields are not content, so a withheld
    // page takes the thinner of the two rather than nothing at all.
    let meta = if withheld.is_some() {
        meta::frontier(&tree, &found, headers.link)
    } else {
        meta::collect(&tree, choice.root, &body, &found, headers.link)
    };

    let raw = u32::try_from(html.len()).unwrap_or(u32::MAX);
    let signals = Signals {
        text_bytes: choice.stats.text,
        link_density: choice.stats.density(),
        extracted_share: choice
            .stats
            .text
            .saturating_mul(100)
            .checked_div(raw)
            .unwrap_or(0),
        top_node_share: choice.share,
        dropped_bytes: tree.dropped_bytes(),
        link_count: u32::try_from(found.links.len()).unwrap_or(u32::MAX),
    };

    Extracted {
        markdown: body,
        boilerplate_uncertain: choice.uncertain,
        declared_root: choice.declared,
        base,
        signals,
        links: found,
        robots,
        meta,
        content_withheld: withheld,
        version: VERSION,
    }
}

/// The document base.
///
/// A `<base href>` is resolved against the final URL, per the HTML standard,
/// and is ignored when it does not resolve or does not come out http or https.
/// Ignoring a junk base rather than failing matters because a broken `<base>`
/// tag is common and losing every link on the page over one is not a trade
/// worth making.
fn base_of(tree: &dom::Dom, url: &Url) -> Url {
    let Some(node) = tree.first(dom::Tag::Base) else {
        return url.clone();
    };
    let Some(href) = tree.element(node).and_then(|element| element.attr("href")) else {
        return url.clone();
    };
    match url.join(href.trim()) {
        Ok(base) if matches!(base.scheme(), "http" | "https") => base,
        _ => url.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(html: &str) -> Extracted {
        let url = Url::parse("https://example.com/a/b").expect("the test url parses");
        extract(html.as_bytes(), &url)
    }

    #[test]
    fn the_scoring_path_has_no_floating_point() {
        // Doc 11.1 rules out floating point in any threshold that can flip a
        // decision, and issue 3 asks for this to be checked rather than
        // promised. The check is the whole crate rather than the scoring
        // function, because a float that reaches scoring through a helper is
        // the same bug.
        for (name, source) in [
            ("lib.rs", include_str!("lib.rs")),
            ("dom.rs", include_str!("dom.rs")),
            ("links.rs", include_str!("links.rs")),
            ("meta.rs", include_str!("meta.rs")),
            ("score.rs", include_str!("score.rs")),
            ("markdown.rs", include_str!("markdown.rs")),
            ("text.rs", include_str!("text.rs")),
        ] {
            for float in ["f32", "f64"] {
                let hits = source.matches(float).count();
                // This file names them once each, in this test.
                let allowed = usize::from(name == "lib.rs");
                assert_eq!(hits, allowed, "{name} mentions {float}");
            }
        }
    }

    #[test]
    fn a_plain_article_comes_out_as_markdown() {
        // Long enough to clear step 3's 200 byte floor, because an article that
        // does not clear it falls back to the body by design and this test is
        // about the happy path rather than about the fallback.
        let out = page(
            "<html><body><article><h2>Title</h2>\
             <p>One sentence that is long enough to count, followed by a second one that \
             carries the paragraph over the two hundred bytes that step three of the \
             cascade insists on before it will trust anything at all.</p>\
             <ul><li>first</li><li>second</li></ul></article></body></html>",
        );
        assert_eq!(
            out.markdown,
            "## Title\n\nOne sentence that is long enough to count, followed by a second one \
             that carries the paragraph over the two hundred bytes that step three of the \
             cascade insists on before it will trust anything at all.\n\n- first\n- second"
        );
        assert!(out.declared_root);
        assert!(!out.boilerplate_uncertain);
    }

    #[test]
    fn the_boilerplate_around_an_article_is_left_out() {
        let out = page(
            "<html><body>\
             <nav><a href='/x'>home</a><a href='/y'>about</a></nav>\
             <div class='sidebar'><a href='/z'>related thing</a><a href='/w'>another</a></div>\
             <article><p>The article body is here and it is comfortably over the length that \
             step three of the cascade insists on before it will trust a winner, which takes \
             rather more words than you would think when you are writing the fixture.</p></article>\
             <footer>copyright</footer></body></html>",
        );
        assert!(out.markdown.starts_with("The article body is here"));
        assert!(!out.markdown.contains("home"));
        assert!(!out.markdown.contains("copyright"));
    }

    #[test]
    fn a_link_farm_falls_back_to_the_body_and_says_so() {
        let mut html = String::from("<html><body><div id='main'>");
        for n in 0..40 {
            html.push_str(&format!("<a href='/page/{n}'>link number {n}</a> "));
        }
        html.push_str("</div></body></html>");
        let out = page(&html);
        assert!(out.boilerplate_uncertain, "a link farm is not a safe win");
        assert!(
            out.markdown
                .contains("[link number 0](https://example.com/page/0)")
        );
        assert!(out.signals.link_density > 66);
    }

    #[test]
    fn a_thin_page_falls_back_rather_than_returning_nothing() {
        let out = page("<html><body><div><p>Too short.</p></div></body></html>");
        assert!(out.boilerplate_uncertain);
        assert_eq!(out.markdown, "Too short.");
    }

    #[test]
    fn scripts_and_styles_never_reach_the_output() {
        let out = page(
            "<html><body><article><p>Real words that go on for long enough to be believable \
             as the body of an actual page.</p>\
             <script>var tracking = 'do not index me';</script>\
             <style>.a { color: red }</style></article></body></html>",
        );
        assert!(!out.markdown.contains("tracking"));
        assert!(!out.markdown.contains("color"));
        assert!(out.signals.dropped_bytes > 0);
    }

    #[test]
    fn relative_links_resolve_against_the_base_tag_and_not_the_url() {
        let out = page(
            "<html><head><base href='https://cdn.example.org/root/'></head><body><article>\
             <p>A paragraph long enough to be trusted by the cascade, holding \
             <a href='deep/page'>a link</a> that has to resolve somewhere.</p>\
             </article></body></html>",
        );
        assert!(
            out.markdown
                .contains("[a link](https://cdn.example.org/root/deep/page)"),
            "{}",
            out.markdown
        );
        assert_eq!(out.base.as_str(), "https://cdn.example.org/root/");
    }

    #[test]
    fn relative_links_resolve_against_the_final_url_when_there_is_no_base() {
        let out = page(
            "<html><body><article><p>A paragraph long enough to be trusted by the cascade, \
             holding <a href='../c'>a link</a> that has to resolve somewhere.</p>\
             </article></body></html>",
        );
        assert!(
            out.markdown.contains("[a link](https://example.com/c)"),
            "{}",
            out.markdown
        );
    }

    #[test]
    fn an_image_keeps_its_alt_text_and_its_resolved_source() {
        let out = page(
            "<html><body><article><p>A paragraph long enough to be trusted by the cascade \
             with a picture in it: <img src='/pic.png' alt='a cat'></p></article></body></html>",
        );
        assert!(
            out.markdown
                .contains("[a cat](https://example.com/pic.png)"),
            "{}",
            out.markdown
        );
    }

    #[test]
    fn markdown_characters_in_text_are_escaped_and_come_back_out_in_plain_text() {
        let out = page(
            "<html><body><article><p>Use snake_case and *stars* and [brackets] in prose \
             that runs long enough for the cascade to trust it.</p></article></body></html>",
        );
        assert!(out.markdown.contains(r"snake\_case"));
        assert!(out.text().contains("snake_case"));
        assert!(out.text().contains("*stars*"));
        assert!(out.text().contains("[brackets]"));
    }

    #[test]
    fn a_code_block_keeps_its_whitespace_and_its_language() {
        let out = page(
            "<html><body><article><p>Here is a paragraph that exists only to get this page \
             over the length that step three insists on.</p>\
             <pre><code class='language-rust'>fn main() {\n    let x = 1;\n}</code></pre>\
             </article></body></html>",
        );
        assert!(
            out.markdown
                .contains("```rust\nfn main() {\n    let x = 1;\n}\n```"),
            "{}",
            out.markdown
        );
    }

    #[test]
    fn a_table_becomes_a_table() {
        let out = page(
            "<html><body><article><p>A paragraph that exists to get the page over the \
             length that step three of the cascade insists on before trusting.</p>\
             <table><tr><th>Name</th><th>Count</th></tr><tr><td>a</td><td>1</td></tr></table>\
             </article></body></html>",
        );
        assert!(
            out.markdown
                .contains("| Name | Count |\n| --- | --- |\n| a | 1 |"),
            "{}",
            out.markdown
        );
    }

    #[test]
    fn a_layout_table_is_transparent_and_the_data_table_inside_it_is_not() {
        let out = page(
            "<html><body><table><tr><td><a href=\"/\">Home</a></td>\
             <td><p>A paragraph that exists to get the page over the length that step \
             three of the cascade insists on before it will trust a winner.</p>\
             <table><tr><th>Date</th><th>High</th></tr><tr><td>1 June</td><td>04:12</td></tr>\
             </table></td></tr></table></body></html>",
        );
        assert!(
            out.markdown
                .contains("| Date | High |\n| --- | --- |\n| 1 June | 04:12 |"),
            "{}",
            out.markdown
        );
        // The outer table held the inner one, so it did not become a row of its
        // own with the whole page escaped into one cell.
        assert!(!out.markdown.contains("\\|"), "{}", out.markdown);
        // And the rows came out once, not once per enclosing table.
        assert_eq!(
            out.markdown.matches("1 June").count(),
            1,
            "{}",
            out.markdown
        );
    }

    #[test]
    fn list_items_survive_a_formatting_element_the_parser_reconstructed() {
        // The `<b>` is never closed, so html5ever carries it into the list and
        // the items are no longer direct children of the `<ul>`. A walk that
        // only looks at direct children loses all three of them.
        let out = page(
            "<html><body><p>A paragraph that exists to get the page over the length \
             that step three of the cascade insists on before it will trust a winner.\
             <b>Bold that never closes\
             <ul><li>An item</li><li>Another item</li><li>A third item</li></ul>\
             </body></html>",
        );
        // Bullets and all, not three loose paragraphs. The items come out bold
        // because the unclosed `<b>` really does carry into them, which is what
        // a browser shows too.
        for item in ["- **An item**", "- **Another item**", "- **A third item**"] {
            assert!(out.markdown.contains(item), "{}", out.markdown);
        }
    }

    #[test]
    fn a_form_wrapping_the_whole_page_does_not_delete_the_page() {
        // What WebForms serves. The form is the page, and the controls inside it
        // are still not content.
        let out = page(
            "<html><body><form id=\"aspnetForm\" runat=\"server\">\
             <input type=\"hidden\" name=\"__VIEWSTATE\" value=\"AAAA\">\
             <div id=\"content\"><h1>The gauge is back</h1>\
             <p>Repaired after four months out of service, and this paragraph runs on \
             long enough that step three of the cascade will trust the winner.</p></div>\
             <button>Submit</button></form></body></html>",
        );
        assert!(
            out.markdown.contains("# The gauge is back"),
            "{}",
            out.markdown
        );
        assert!(
            out.markdown.contains("Repaired after four months"),
            "{}",
            out.markdown
        );
        assert!(!out.markdown.contains("Submit"), "{}", out.markdown);
    }

    #[test]
    fn deeply_nested_markup_does_not_blow_the_stack() {
        // 8192 levels, thirty two times `dom::MAX_DEPTH` and well past what a
        // recursive walk of any of these passes would survive. The point of the
        // test is that it returns at all.
        let mut html = String::from("<html><body>");
        for _ in 0..8192 {
            html.push_str("<div>");
        }
        html.push_str(
            "The text at the bottom of a very deep hole, long enough that the \
             cascade has something to hold on to when it gets there.",
        );
        for _ in 0..8192 {
            html.push_str("</div>");
        }
        html.push_str("</body></html>");
        let out = page(&html);
        assert!(out.markdown.contains("very deep hole"));
    }

    #[test]
    fn the_same_input_extracts_to_the_same_bytes_every_time() {
        let html = "<html><body><article><p>Determinism is the whole point of this crate, \
                    so running it twice had better agree with itself.</p>\
                    <ul><li>a</li><li>b</li></ul></article></body></html>";
        let first = page(html);
        let second = page(html);
        assert_eq!(first, second);
        assert_eq!(first.text_digest(), second.text_digest());
    }

    #[test]
    fn text_is_normalised_to_nfc_and_not_nfkd_or_nfkc() {
        // A decomposed e with an acute, and a full width comma that NFKC would
        // destroy. Doc 11.3 picks NFC for exactly this reason.
        let out = page(
            "<html><body><article><p>Cafe\u{0301} and \u{FF0C} in a sentence that runs on \
             long enough for the cascade to keep it.</p></article></body></html>",
        );
        assert!(out.text().contains("Caf\u{00E9}"), "{}", out.text());
        assert!(out.text().contains('\u{FF0C}'), "{}", out.text());
    }

    #[test]
    fn the_version_is_stamped_on_every_row() {
        assert!(VERSION.starts_with("umi-extract/"));
        assert_eq!(page("<p>hi</p>").version, VERSION);
    }

    /// Long enough that step 3 of the cascade trusts the winner, so a links test
    /// is testing links rather than accidentally testing the fallback.
    const BODY: &str = "<p>A paragraph that exists to carry this page over the two hundred \
                        bytes that step three of the cascade insists on before it will trust \
                        a winner, which takes rather more words than you would think when \
                        you sit down to write one of these.</p>";

    fn link_of<'a>(out: &'a Extracted, url: &str) -> &'a Link {
        out.links
            .links
            .iter()
            .find(|link| link.url == url)
            .unwrap_or_else(|| panic!("no link to {url} in {:?}", out.links.links))
    }

    #[test]
    fn a_body_anchor_and_a_navigation_anchor_are_told_apart() {
        // The nav, the header and the footer are all dropped from the content by
        // doc 11.3 and all three still have to hand over their links, which is
        // the whole reason the arena keeps them.
        let out = page(&format!(
            "<html><body>\
             <header><a href='/logo'>Home</a></header>\
             <nav><a href='/about'>About</a></nav>\
             <div class='sidebar'><a href='/related'>Related</a></div>\
             <article>{BODY}<a href='/cited'>a source</a></article>\
             <footer><a href='/terms'>Terms</a></footer></body></html>"
        ));

        assert_eq!(
            link_of(&out, "https://example.com/cited").kind,
            LinkKind::Body
        );
        for nav in ["/logo", "/about", "/related", "/terms"] {
            let url = format!("https://example.com{nav}");
            assert_eq!(link_of(&out, &url).kind, LinkKind::Nav, "{url}");
        }
        // And none of it reached the content.
        assert!(!out.markdown.contains("Terms"), "{}", out.markdown);
        assert!(!out.markdown.contains("About"), "{}", out.markdown);
        assert!(out.markdown.contains("[a source]"), "{}", out.markdown);
    }

    #[test]
    fn a_nav_inside_the_content_root_is_still_navigation() {
        let out = page(&format!(
            "<html><body><article>{BODY}<a href='/cited'>a source</a>\
             <nav><a href='/next-page'>Next</a></nav></article></body></html>"
        ));
        assert_eq!(
            link_of(&out, "https://example.com/cited").kind,
            LinkKind::Body
        );
        assert_eq!(
            link_of(&out, "https://example.com/next-page").kind,
            LinkKind::Nav
        );
    }

    #[test]
    fn a_link_element_is_a_sitemap_a_feed_or_a_plain_link() {
        let out = page(&format!(
            "<html><head>\
             <link rel='sitemap' href='/sitemap.xml'>\
             <link rel='alternate' type='application/rss+xml' href='/feed.xml'>\
             <link rel='canonical' href='/canonical'>\
             </head><body><article>{BODY}</article></body></html>"
        ));
        assert_eq!(
            link_of(&out, "https://example.com/sitemap.xml").kind,
            LinkKind::Sitemap
        );
        assert_eq!(
            link_of(&out, "https://example.com/feed.xml").kind,
            LinkKind::Feed
        );
        let canonical = link_of(&out, "https://example.com/canonical");
        assert_eq!(canonical.kind, LinkKind::Link);
        assert!(canonical.rel.has(Rel::CANONICAL));
        // A `<link>` has no text, and an empty anchor is not the same as a
        // missing one.
        assert!(canonical.anchor.is_empty());
    }

    #[test]
    fn the_rel_bitmask_arrives_on_the_link() {
        let out = page(&format!(
            "<html><body><article>{BODY}\
             <a href='/paid' rel='NoFollow sponsored noopener made-up'>an ad</a>\
             <a href='/plain'>not an ad</a></article></body></html>"
        ));
        let paid = link_of(&out, "https://example.com/paid");
        assert!(paid.rel.has(Rel::NOFOLLOW));
        assert!(paid.rel.has(Rel::SPONSORED));
        assert!(paid.rel.has(Rel::NOOPENER));
        assert!(!paid.rel.has(Rel::UGC));
        assert_eq!(link_of(&out, "https://example.com/plain").rel, Rel::NONE);
        // `nofollow` on the link is recorded and not obeyed, so the link is here
        // rather than being dropped, which is the point of recording it.
        assert_eq!(out.signals.link_count, 2);
    }

    #[test]
    fn a_page_level_nofollow_marks_every_link_on_the_page() {
        let out = page(&format!(
            "<html><head><meta name='robots' content='nofollow'></head>\
             <body><article>{BODY}<a href='/one'>one</a></article>\
             <nav><a href='/two'>two</a></nav></body></html>"
        ));
        assert!(out.robots.nofollow);
        assert!(!out.robots.noindex);
        assert_eq!(out.links.links.len(), 2);
        for link in &out.links.links {
            assert!(link.rel.has(Rel::NOFOLLOW), "{link:?}");
        }
        // Page level nofollow says nothing about indexing, so the content stays.
        assert!(out.content_withheld.is_none());
        assert!(!out.markdown.is_empty());
    }

    #[test]
    fn a_noindex_page_is_still_a_row_and_still_has_its_links() {
        let out = page(&format!(
            "<html><head><meta name='robots' content='noindex'></head>\
             <body><article>{BODY}<a href='/one'>one</a></article></body></html>"
        ));
        assert_eq!(out.content_withheld, Some(Withheld::Noindex));
        assert!(out.markdown.is_empty());
        assert!(out.text().is_empty());
        // The links survive, and they are not marked nofollow, because `noindex`
        // on its own says nothing about following.
        assert_eq!(out.links.links.len(), 1);
        assert_eq!(out.links.links[0].url, "https://example.com/one");
        assert!(!out.links.links[0].rel.has(Rel::NOFOLLOW));
        assert_eq!(out.signals.link_count, 1);
    }

    #[test]
    fn an_x_robots_tag_header_withholds_the_same_way_the_meta_tag_does() {
        let url = Url::parse("https://example.com/a/b").expect("the test url parses");
        let html =
            format!("<html><body><article>{BODY}<a href='/one'>one</a></article></body></html>");
        let out = extract_with_headers(
            html.as_bytes(),
            &url,
            Headers {
                x_robots_tag: Some("noindex, nofollow"),
                ..Headers::default()
            },
        );
        assert_eq!(out.content_withheld, Some(Withheld::Noindex));
        assert!(out.robots.nofollow);
        assert_eq!(out.links.links.len(), 1);
        assert!(out.links.links[0].rel.has(Rel::NOFOLLOW));
    }

    #[test]
    fn a_robots_tag_naming_another_crawler_is_not_ours_to_obey() {
        let out = page(&format!(
            "<html><head><meta name='googlebot' content='noindex'></head>\
             <body><article>{BODY}</article></body></html>"
        ));
        assert!(out.content_withheld.is_none());
        assert!(!out.markdown.is_empty());
    }

    #[test]
    fn a_page_keeps_five_thousand_links_and_says_it_had_more() {
        let mut html = String::from("<html><body><article>");
        html.push_str(BODY);
        for n in 0..(MAX_LINKS + 500) {
            html.push_str(&format!("<a href='/p/{n}'>link {n}</a> "));
        }
        html.push_str("</article></body></html>");
        let out = page(&html);
        assert_eq!(out.links.links.len(), MAX_LINKS);
        assert!(out.links.truncated);
        assert_eq!(out.signals.link_count, MAX_LINKS as u32);
        // The first 5000 in document order, not an arbitrary 5000.
        assert_eq!(out.links.links[0].url, "https://example.com/p/0");
        assert_eq!(
            out.links.links[MAX_LINKS - 1].url,
            format!("https://example.com/p/{}", MAX_LINKS - 1)
        );
    }

    #[test]
    fn a_page_under_the_cap_does_not_claim_to_be_truncated() {
        let out = page(&format!(
            "<html><body><article>{BODY}<a href='/one'>one</a></article></body></html>"
        ));
        assert!(!out.links.truncated);
    }

    #[test]
    fn the_same_triple_twice_is_one_link_and_a_different_anchor_is_two() {
        let out = page(&format!(
            "<html><body><article>{BODY}\
             <a href='/same'>label</a><a href='/same'>label</a>\
             <a href='/same'>a different label</a></article></body></html>"
        ));
        assert_eq!(out.links.links.len(), 2);
        assert_eq!(out.links.links[0].anchor, "label");
        assert_eq!(out.links.links[1].anchor, "a different label");
    }

    #[test]
    fn schemes_that_are_not_http_are_dropped_and_counted_apart() {
        let out = page(&format!(
            "<html><body><article>{BODY}\
             <a href='mailto:someone@example.com'>mail</a>\
             <a href='javascript:void(0)'>menu</a>\
             <a href='tel:+15550100'>call</a>\
             <a href='/real'>real</a></article></body></html>"
        ));
        assert_eq!(out.links.links.len(), 1);
        assert_eq!(out.links.links[0].url, "https://example.com/real");
        assert_eq!(out.links.dropped_scheme, 3);
        assert_eq!(out.links.dropped, 0);
        // No trace of the address anywhere in what we would store.
        assert!(!format!("{:?}", out.links).contains("someone@example.com"));
    }

    #[test]
    fn anchor_text_is_the_whole_subtree_collapsed_and_capped() {
        let long = "word ".repeat(100);
        let out = page(&format!(
            "<html><body><article>{BODY}\
             <a href='/img'><img src='/i.png' alt='ignored'> <span>two</span>\n<b>words</b></a>\
             <a href='/long'>{long}</a></article></body></html>"
        ));
        assert_eq!(link_of(&out, "https://example.com/img").anchor, "two words");
        let cut = &link_of(&out, "https://example.com/long").anchor;
        assert!(cut.len() <= MAX_ANCHOR, "{} bytes", cut.len());
        assert!(cut.starts_with("word word"));
    }

    #[test]
    fn links_resolve_against_the_base_tag_like_the_markdown_does() {
        let out = page(&format!(
            "<html><head><base href='https://cdn.example.org/root/'></head>\
             <body><article>{BODY}<a href='deep/page'>a link</a></article></body></html>"
        ));
        assert_eq!(
            out.links.links[0].url,
            "https://cdn.example.org/root/deep/page"
        );
    }

    #[test]
    fn the_title_prefers_the_title_tag_then_open_graph_then_the_first_heading() {
        let out = page(&format!(
            "<html><head><title>The real title</title>\
             <meta property='og:title' content='The social title'></head>\
             <body><article><h1>The heading</h1>{BODY}</article></body></html>"
        ));
        assert_eq!(out.meta.title.as_deref(), Some("The real title"));
        assert_eq!(out.meta.title_source, Some(TitleSource::Title));

        let out = page(&format!(
            "<html><head><meta property='og:title' content='The social title'></head>\
             <body><article><h1>The heading</h1>{BODY}</article></body></html>"
        ));
        assert_eq!(out.meta.title.as_deref(), Some("The social title"));
        assert_eq!(out.meta.title_source, Some(TitleSource::OpenGraph));

        let out = page(&format!(
            "<html><body><article><h1>The heading</h1>{BODY}</article></body></html>"
        ));
        assert_eq!(out.meta.title.as_deref(), Some("The heading"));
        assert_eq!(out.meta.title_source, Some(TitleSource::Heading));
    }

    #[test]
    fn an_empty_title_tag_falls_through_to_the_next_rule() {
        // Templates ship `<title></title>` and a page with one has no title, not
        // an empty one.
        let out = page(&format!(
            "<html><head><title>  </title>\
             <meta property='og:title' content='The social title'></head>\
             <body><article>{BODY}</article></body></html>"
        ));
        assert_eq!(out.meta.title.as_deref(), Some("The social title"));
    }

    #[test]
    fn a_masthead_heading_is_not_the_title() {
        // The `h1` in the header is the site's name. The content root is what
        // tells it from the article's own heading.
        let out = page(&format!(
            "<html><body><header><h1>The Daily Example</h1></header>\
             <article><h1>What actually happened</h1>{BODY}</article></body></html>"
        ));
        assert_eq!(out.meta.title.as_deref(), Some("What actually happened"));
    }

    #[test]
    fn the_description_prefers_the_meta_tag_and_says_when_it_is_ours() {
        let out = page(&format!(
            "<html><head><meta name='description' content='What the author wrote'>\
             <meta property='og:description' content='The social one'></head>\
             <body><article>{BODY}</article></body></html>"
        ));
        assert_eq!(
            out.meta.description.as_deref(),
            Some("What the author wrote")
        );
        assert_eq!(out.meta.description_source, Some(DescriptionSource::Meta));
        assert!(!out.meta.description_derived());

        let out = page(&format!(
            "<html><head><meta name='twitter:description' content='The bird one'></head>\
             <body><article>{BODY}</article></body></html>"
        ));
        assert_eq!(
            out.meta.description_source,
            Some(DescriptionSource::Twitter)
        );
    }

    #[test]
    fn a_page_with_no_description_gets_one_from_its_first_paragraph() {
        let out = page(&format!(
            "<html><body><article><h2>A heading first</h2>{BODY}</article></body></html>"
        ));
        assert_eq!(
            out.meta.description_source,
            Some(DescriptionSource::FirstParagraph)
        );
        assert!(out.meta.description_derived());
        let description = out.meta.description.expect("the fallback produced one");
        // The heading is not the description, the prose is, and it is cut on a
        // word boundary rather than mid word.
        assert!(
            description.starts_with("A paragraph that exists"),
            "{description}"
        );
        assert!(description.len() <= MAX_DERIVED_DESCRIPTION);
        assert!(!description.ends_with(' '));
        assert!(BODY.contains(&description[..40]));
    }

    #[test]
    fn the_headings_come_from_the_content_and_stop_at_h3() {
        let out = page(&format!(
            "<html><body><nav><h2>Sections</h2></nav>\
             <article><h1>One</h1>{BODY}<h2>Two</h2><h3>Three</h3><h4>Four</h4></article>\
             <footer><h2>Contact</h2></footer></body></html>"
        ));
        let headings: Vec<_> = out
            .meta
            .headings
            .iter()
            .map(|heading| (heading.level, heading.text.as_str()))
            .collect();
        assert_eq!(headings, [(1, "One"), (2, "Two"), (3, "Three")]);
    }

    #[test]
    fn the_canonical_and_the_feeds_come_out_resolved() {
        let out = page(&format!(
            "<html><head><link rel='canonical' href='/real'>\
             <link rel='alternate' type='application/rss+xml' href='/feed.xml'>\
             <link rel='alternate' hreflang='fr' href='/fr/'>\
             </head><body><article>{BODY}</article></body></html>"
        ));
        assert_eq!(
            out.meta.canonical.as_deref(),
            Some("https://example.com/real")
        );
        // A translation is an alternate too, and it is not a feed.
        assert_eq!(out.meta.feeds, ["https://example.com/feed.xml"]);
    }

    #[test]
    fn a_link_header_carries_the_canonical_when_the_page_does_not() {
        let url = Url::parse("https://example.com/a/b").expect("the test url parses");
        let html = format!("<html><body><article>{BODY}</article></body></html>");
        let out = extract_with_headers(
            html.as_bytes(),
            &url,
            Headers {
                link: Some("<https://example.com/real>; rel=\"canonical\""),
                ..Headers::default()
            },
        );
        assert_eq!(
            out.meta.canonical.as_deref(),
            Some("https://example.com/real")
        );
    }

    #[test]
    fn the_document_wins_over_the_link_header() {
        let url = Url::parse("https://example.com/a/b").expect("the test url parses");
        let html = format!(
            "<html><head><link rel='canonical' href='/from-the-page'></head>\
             <body><article>{BODY}</article></body></html>"
        );
        let out = extract_with_headers(
            html.as_bytes(),
            &url,
            Headers {
                link: Some("<https://example.com/from-the-header>; rel=canonical"),
                ..Headers::default()
            },
        );
        assert_eq!(
            out.meta.canonical.as_deref(),
            Some("https://example.com/from-the-page")
        );
    }

    #[test]
    fn json_ld_gives_up_five_fields_and_keeps_none_of_the_rest() {
        let out = page(&format!(
            "<html><head><script type='application/ld+json'>{{\
             \"@context\": \"https://schema.org\", \"@type\": \"NewsArticle\",\
             \"headline\": \"What happened\", \"datePublished\": \"2026-01-02T03:04:05Z\",\
             \"dateModified\": \"2026-01-03T00:00:00Z\",\
             \"author\": {{\"@type\": \"Person\", \"name\": \"A Reporter\"}},\
             \"articleBody\": \"the whole article repeated again\"\
             }}</script></head><body><article>{BODY}</article></body></html>"
        ));
        let structured = &out.meta.structured;
        // The author's `Person` is not one of the page's types. The walk goes
        // down `@graph`, whose members are all statements about the page, and
        // not into an arbitrary nested object, where every `@type` is a fact
        // about a field rather than about the document.
        assert_eq!(structured.types, ["NewsArticle"]);
        assert_eq!(structured.headline.as_deref(), Some("What happened"));
        assert_eq!(structured.author.as_deref(), Some("A Reporter"));
        assert_eq!(
            structured.published.as_deref(),
            Some("2026-01-02T03:04:05Z")
        );
        assert_eq!(structured.modified.as_deref(), Some("2026-01-03T00:00:00Z"));
        // The blob is not kept, which is the point of keeping five fields.
        assert!(!format!("{structured:?}").contains("repeated again"));
        // And the script never reached the content.
        assert!(!out.markdown.contains("repeated again"), "{}", out.markdown);
    }

    #[test]
    fn json_ld_in_a_graph_is_still_read_and_broken_json_is_not_fatal() {
        let out = page(&format!(
            "<html><head>\
             <script type='application/ld+json'>{{not json at all</script>\
             <script type='text/javascript'>var datePublished = \"1999\";</script>\
             <script type='application/ld+json'>{{\"@graph\": [\
             {{\"@type\": \"WebPage\"}},\
             {{\"@type\": \"Article\", \"datePublished\": \"2026-05-06\"}}]}}</script>\
             </head><body><article>{BODY}</article></body></html>"
        ));
        assert_eq!(out.meta.structured.types, ["WebPage", "Article"]);
        assert_eq!(out.meta.structured.published.as_deref(), Some("2026-05-06"));
        // A plain script is a script, not structured data.
        assert!(!out.meta.structured.types.contains(&"1999".to_owned()));
    }

    #[test]
    fn microdata_and_rdfa_are_noticed_and_not_parsed() {
        let out = page(&format!(
            "<html><body><article itemscope itemtype='https://schema.org/Article'>\
             {BODY}</article></body></html>"
        ));
        assert!(out.meta.microdata);
        assert!(!out.meta.rdfa);

        let out = page(&format!(
            "<html><body><article typeof='Article'>{BODY}</article></body></html>"
        ));
        assert!(out.meta.rdfa);
        assert!(!out.meta.microdata);

        // A page with neither is not flagged by an `itemprop` on its own, which
        // turns up all over pages that carry no vocabulary.
        let out = page(&format!(
            "<html><body><article><span itemprop='name'>x</span>{BODY}</article></body></html>"
        ));
        assert!(!out.meta.microdata);
        assert!(!out.meta.rdfa);
    }

    #[test]
    fn the_declared_language_is_recorded_as_the_page_wrote_it() {
        let out = page(&format!(
            "<html lang='EN-GB'><body><article>{BODY}</article></body></html>"
        ));
        assert_eq!(out.meta.declared_lang.as_deref(), Some("en-gb"));
        let out = page(&format!(
            "<html><body><article>{BODY}</article></body></html>"
        ));
        assert_eq!(out.meta.declared_lang, None);
    }

    #[test]
    fn the_dates_stay_in_separate_columns_when_they_disagree() {
        let out = page(&format!(
            "<html><head><meta property='article:published_time' content='2026-01-01'>\
             <meta property='article:modified_time' content='2026-02-02'>\
             <script type='application/ld+json'>{{\"@type\": \"Article\",\
             \"datePublished\": \"2020-12-31\"}}</script></head>\
             <body><article>{BODY}</article></body></html>"
        ));
        assert_eq!(out.meta.published.as_deref(), Some("2026-01-01"));
        assert_eq!(out.meta.modified.as_deref(), Some("2026-02-02"));
        assert_eq!(out.meta.structured.published.as_deref(), Some("2020-12-31"));
        assert_eq!(out.meta.structured.modified, None);
    }

    #[test]
    fn a_withheld_page_keeps_the_frontier_metadata_and_loses_the_content_metadata() {
        let out = page(&format!(
            "<html lang='en'><head><meta name='robots' content='noindex'>\
             <title>A title nobody gets</title>\
             <meta name='description' content='A description nobody gets'>\
             <link rel='canonical' href='/real'>\
             <link rel='alternate' type='application/atom+xml' href='/feed'>\
             <meta property='article:modified_time' content='2026-03-03'>\
             <script type='application/ld+json'>{{\"@type\": \"Article\",\
             \"headline\": \"A headline nobody gets\", \"datePublished\": \"2026-03-01\",\
             \"author\": {{\"name\": \"Nobody\"}}}}</script></head>\
             <body><article><h1>A heading nobody gets</h1>{BODY}</article></body></html>"
        ));
        assert_eq!(out.content_withheld, Some(Withheld::Noindex));

        // Content, and content under another name, all gone.
        assert_eq!(out.meta.title, None);
        assert_eq!(out.meta.title_source, None);
        assert_eq!(out.meta.description, None);
        assert!(out.meta.headings.is_empty());
        assert_eq!(out.meta.structured.headline, None);
        assert_eq!(out.meta.structured.author, None);
        let dump = format!("{:?}", out.meta);
        for secret in ["nobody gets", "Nobody"] {
            assert!(!dump.contains(secret), "{dump}");
        }

        // Facts about the page, all kept.
        assert_eq!(
            out.meta.canonical.as_deref(),
            Some("https://example.com/real")
        );
        assert_eq!(out.meta.feeds, ["https://example.com/feed"]);
        assert_eq!(out.meta.modified.as_deref(), Some("2026-03-03"));
        assert_eq!(out.meta.structured.published.as_deref(), Some("2026-03-01"));
        assert_eq!(out.meta.structured.types, ["Article"]);
        assert_eq!(out.meta.declared_lang.as_deref(), Some("en"));
    }

    #[test]
    fn the_metadata_caps_hold() {
        let long = "word ".repeat(200);
        let out = page(&format!(
            "<html><head><title>{long}</title>\
             <meta name='description' content='{}'></head>\
             <body><article>{BODY}{}</article></body></html>",
            "d".repeat(4096),
            "<h2>a heading</h2>".repeat(MAX_HEADINGS + 10)
        ));
        assert_eq!(out.meta.title.expect("a title").len(), MAX_TITLE);
        assert_eq!(
            out.meta.description.expect("a description").len(),
            MAX_DESCRIPTION
        );
        assert_eq!(out.meta.headings.len(), MAX_HEADINGS);
    }
}
