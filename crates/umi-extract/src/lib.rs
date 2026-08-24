//! Turn a fetched HTML document into markdown, plain text and quality signals.
//!
//! This crate implements doc 11.3 and the parts of doc 11.6 that do not need
//! the link pass yet. The rule it is built around is doc 11.1: given the same
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
mod markdown;
mod score;
mod text;

use url::Url;

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
/// Doc 11.6 lists seven. Five are here. `link_count` arrives with the link pass
/// and `stopword_coverage` arrives with language detection, and both are listed
/// in doc 11.6 as separate work. They are computed and published, never applied:
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
    /// The version that produced this, for the receipt and the Parquet column.
    pub version: &'static str,
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
    let tree = dom::Dom::parse(html);
    let base = base_of(&tree, url);
    let choice = score::choose(&tree);
    let body = markdown::render(&tree, choice.root, Some(&base));

    let raw = u32::try_from(html.len()).unwrap_or(u32::MAX);
    let signals = Signals {
        text_bytes: choice.stats.text,
        link_density: choice.stats.density(),
        extracted_share: if raw == 0 {
            0
        } else {
            choice.stats.text.saturating_mul(100) / raw
        },
        top_node_share: choice.share,
        dropped_bytes: tree.dropped_bytes(),
    };

    Extracted {
        markdown: body,
        boilerplate_uncertain: choice.uncertain,
        declared_root: choice.declared,
        base,
        signals,
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
        let out = page(
            "<html><body><article><h2>Title</h2><p>One sentence that is long enough to count.</p>\
             <ul><li>first</li><li>second</li></ul></article></body></html>",
        );
        assert_eq!(
            out.markdown,
            "## Title\n\nOne sentence that is long enough to count.\n\n- first\n- second"
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
             step three of the cascade insists on before it will trust a winner.</p></article>\
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
        assert!(out.markdown.contains("[link number 0](https://example.com/page/0)"));
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
            out.markdown.contains("[a cat](https://example.com/pic.png)"),
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
    fn deeply_nested_markup_does_not_blow_the_stack() {
        // 20000 levels, which is well past `dom::MAX_DEPTH` and well past what
        // a recursive walk survives. The point of the test is that it returns.
        let mut html = String::from("<html><body>");
        for _ in 0..20_000 {
            html.push_str("<div>");
        }
        html.push_str("The text at the bottom of a very deep hole, long enough that the \
                       cascade has something to hold on to when it gets there.");
        for _ in 0..20_000 {
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
}
