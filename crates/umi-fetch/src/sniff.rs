//! What a response body actually is, as opposed to what it says it is.
//!
//! `Content-Type` is wrong often enough that a crawler which believes it will
//! feed PDFs to an HTML parser and skip HTML served as `application/octet-stream`.
//! Both happen at scale. The rule here is that a positive identification from
//! the bytes beats the declared type, and the declared type only decides the
//! cases the bytes leave open.
//!
//! This is a deliberately small subset of the WHATWG mimesniff algorithm. The
//! full algorithm exists to pick between forty odd image and audio formats so a
//! browser can choose a decoder. We only need to answer one question, which is
//! which extractor a body goes to, and the answer has five values.

/// What we will treat a body as, whatever the origin called it.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Media {
    /// Markup, for the extractor in `docs/spec/11-extraction-and-dedup.md`.
    Html,
    /// XML that is not HTML: a sitemap, a feed, or a document we do not index.
    Xml,
    /// PDF, which milestone 5 handles and milestone 1 records and skips.
    Pdf,
    /// Text with no markup in it. Kept, because `robots.txt` and plain text
    /// documents both land here.
    Text,
    /// Bytes we do not extract. Recorded as a fetch and not as a page.
    #[default]
    Binary,
}

impl Media {
    /// Whether milestone 1 has anything to do with a body of this kind.
    #[must_use]
    pub const fn is_extractable(self) -> bool {
        matches!(self, Self::Html | Self::Xml | Self::Text)
    }

    /// The name this appears under in a receipt or a report.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Html => "html",
            Self::Xml => "xml",
            Self::Pdf => "pdf",
            Self::Text => "text",
            Self::Binary => "binary",
        }
    }
}

/// How many bytes of the body the sniffer looks at.
///
/// The WHATWG algorithm uses 1445, which is enough for a doctype behind a
/// generous run of comments and whitespace. There is no reason to differ.
pub const SNIFF_BYTES: usize = 1445;

/// Decide what a body is from the declared type and the first bytes of it.
///
/// The declared type is only consulted when the bytes are inconclusive, which
/// in practice means text that starts with no recognisable marker. Anything
/// the bytes identify positively wins, because a body that begins `%PDF-` is a
/// PDF no matter what the `Content-Type` says, and treating it as the HTML it
/// claims to be produces a page of mojibake in the corpus.
#[must_use]
pub fn decide(declared: Option<&str>, head: &[u8]) -> Media {
    if let Some(from_bytes) = from_bytes(head) {
        return from_bytes;
    }
    if let Some(from_type) = declared.and_then(from_content_type) {
        return from_type;
    }
    if looks_textual(head) {
        Media::Text
    } else {
        Media::Binary
    }
}

/// The `Content-Type` essence, lowercased, with the parameters dropped.
///
/// `text/HTML; charset=UTF-8` and `text/html` are the same type and a
/// surprising number of origins send the first.
#[must_use]
pub fn essence(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

/// What the declared type says, for the types we act on.
fn from_content_type(content_type: &str) -> Option<Media> {
    let essence = essence(content_type);
    let media = match essence.as_str() {
        "text/html" | "application/xhtml+xml" => Media::Html,
        "text/xml" | "application/xml" | "application/rss+xml" | "application/atom+xml" => {
            Media::Xml
        }
        "application/pdf" | "application/x-pdf" => Media::Pdf,
        "application/json" | "application/ld+json" => Media::Text,
        other if other.starts_with("text/") => Media::Text,
        _ => return None,
    };
    Some(media)
}

/// What the bytes say, when they say anything.
fn from_bytes(head: &[u8]) -> Option<Media> {
    let head = strip_bom(head);

    if head.starts_with(b"%PDF-") {
        return Some(Media::Pdf);
    }
    if let Some(magic) = binary_magic(head) {
        return Some(magic);
    }

    // Leading whitespace before a doctype is common enough that skipping it is
    // part of the WHATWG algorithm rather than a kindness.
    let head = trim_leading_space(head);

    if starts_with_ignore_case(head, b"<?xml") {
        // An XHTML document served as XML is still markup we extract, and the
        // root element is the only thing that says which.
        return Some(if contains_ignore_case(head, b"<html") {
            Media::Html
        } else {
            Media::Xml
        });
    }
    if html_tag(head) {
        return Some(Media::Html);
    }
    None
}

/// The HTML patterns from the WHATWG table, each of which has to be followed by
/// a tag terminator so that `<a-custom-element>` is not read as `<a>`.
const HTML_TAGS: [&[u8]; 17] = [
    b"<!DOCTYPE HTML",
    b"<HTML",
    b"<HEAD",
    b"<SCRIPT",
    b"<IFRAME",
    b"<H1",
    b"<DIV",
    b"<FONT",
    b"<TABLE",
    b"<A",
    b"<STYLE",
    b"<TITLE",
    b"<B",
    b"<BODY",
    b"<BR",
    b"<P",
    b"<!--",
];

fn html_tag(head: &[u8]) -> bool {
    HTML_TAGS.iter().any(|tag| {
        starts_with_ignore_case(head, tag)
            && match head.get(tag.len()) {
                // A comment opener is its own terminator. Everything else has
                // to be followed by whitespace or the end of the tag.
                None => *tag == b"<!--",
                Some(byte) => byte.is_ascii_whitespace() || *byte == b'>',
            }
    })
}

/// Formats we will never extract, recognised so that a body declared as HTML
/// does not reach the parser as one.
fn binary_magic(head: &[u8]) -> Option<Media> {
    const MAGIC: [&[u8]; 12] = [
        b"\x1f\x8b",          // gzip
        b"PK\x03\x04",        // zip, and everything built on it
        b"\x89PNG\r\n\x1a\n", // png
        b"GIF87a",            //
        b"GIF89a",            //
        b"\xff\xd8\xff",      // jpeg
        b"RIFF",              // webp, wav
        b"\x00\x00\x01\x00",  // ico
        b"\x7fELF",           //
        b"MZ",                //
        b"\xd0\xcf\x11\xe0",  // old office
        b"\x28\xb5\x2f\xfd",  // zstd
    ];
    MAGIC
        .iter()
        .any(|magic| head.starts_with(magic))
        .then_some(Media::Binary)
}

/// Drop a byte order mark, which otherwise hides the marker behind it.
fn strip_bom(head: &[u8]) -> &[u8] {
    for bom in [b"\xef\xbb\xbf".as_slice(), b"\xff\xfe", b"\xfe\xff"] {
        if let Some(rest) = head.strip_prefix(bom) {
            return rest;
        }
    }
    head
}

fn trim_leading_space(head: &[u8]) -> &[u8] {
    let start = head
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(head.len());
    &head[start..]
}

/// Whether a body with no marker is text or bytes.
///
/// A NUL byte in the first kilobyte and a half is the signal every sniffer
/// uses, because text encodings that produce interior NULs are UTF-16 and
/// UTF-32, and those announce themselves with a BOM that is already gone by
/// the time we get here.
fn looks_textual(head: &[u8]) -> bool {
    !head.is_empty() && !head.contains(&0)
}

fn starts_with_ignore_case(head: &[u8], prefix: &[u8]) -> bool {
    head.len() >= prefix.len() && head[..prefix.len()].eq_ignore_ascii_case(prefix)
}

fn contains_ignore_case(head: &[u8], needle: &[u8]) -> bool {
    head.windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

#[cfg(test)]
mod tests {
    use super::{Media, decide, essence};

    #[test]
    fn a_pdf_declared_as_html_is_still_a_pdf() {
        // The case the issue names. Servers with a catch all handler send
        // text/html for everything, and a PDF through an HTML parser is a
        // page of mojibake in a published corpus.
        assert_eq!(
            decide(
                Some("text/html; charset=utf-8"),
                b"%PDF-1.7\n%\xe2\xe3\xcf\xd3"
            ),
            Media::Pdf
        );
    }

    #[test]
    fn html_declared_as_octet_stream_is_still_html() {
        assert_eq!(
            decide(
                Some("application/octet-stream"),
                b"<!DOCTYPE html>\n<html><head>"
            ),
            Media::Html
        );
    }

    #[test]
    fn leading_whitespace_does_not_hide_the_doctype() {
        assert_eq!(decide(None, b"\n\n\t  <!doctype html>"), Media::Html);
    }

    #[test]
    fn a_byte_order_mark_does_not_hide_it_either() {
        assert_eq!(decide(None, b"\xef\xbb\xbf<html lang=\"en\">"), Media::Html);
    }

    #[test]
    fn a_custom_element_is_not_an_anchor() {
        // `<A` matches the start of `<article-card>`, and without the tag
        // terminator check every web component page sniffs as HTML for the
        // wrong reason. This one is HTML anyway, so assert on the mechanism
        // with a body that is not.
        assert_eq!(decide(None, b"<amount>3</amount>"), Media::Text);
    }

    #[test]
    fn xml_and_xhtml_are_told_apart() {
        assert_eq!(
            decide(None, b"<?xml version=\"1.0\"?><urlset xmlns=\"...\">"),
            Media::Xml
        );
        assert_eq!(
            decide(
                None,
                b"<?xml version=\"1.0\"?><!DOCTYPE html><html xmlns=\"...\">"
            ),
            Media::Html
        );
    }

    #[test]
    fn a_gzip_body_is_binary_whatever_it_claims() {
        // Not the same thing as `Content-Encoding: gzip`, which the client
        // decodes before we see it. This is a .gz file served as a document.
        assert_eq!(
            decide(Some("text/html"), b"\x1f\x8b\x08\x00"),
            Media::Binary
        );
    }

    #[test]
    fn plain_text_falls_back_to_the_declared_type() {
        assert_eq!(decide(Some("text/plain"), b"User-agent: *\n"), Media::Text);
        assert_eq!(decide(Some("application/json"), b"{\"a\":1}"), Media::Text);
    }

    #[test]
    fn nothing_declared_and_no_marker_is_decided_by_the_bytes() {
        assert_eq!(decide(None, b"just some words"), Media::Text);
        assert_eq!(decide(None, b"\x00\x01\x02binary"), Media::Binary);
        assert_eq!(decide(None, b""), Media::Binary);
    }

    #[test]
    fn a_declared_type_we_do_not_know_is_not_a_reason_to_guess() {
        assert_eq!(
            decide(Some("application/wasm"), b"\x00asm\x01"),
            Media::Binary
        );
    }

    #[test]
    fn the_essence_drops_parameters_and_case() {
        assert_eq!(essence("text/HTML; charset=UTF-8"), "text/html");
        assert_eq!(essence("  application/pdf  "), "application/pdf");
    }

    #[test]
    fn only_markup_and_text_go_to_an_extractor() {
        assert!(Media::Html.is_extractable());
        assert!(Media::Xml.is_extractable());
        assert!(Media::Text.is_extractable());
        assert!(!Media::Pdf.is_extractable());
        assert!(!Media::Binary.is_extractable());
    }
}
