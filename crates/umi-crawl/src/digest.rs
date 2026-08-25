//! One digest over everything doc 11 extracted from a page.
//!
//! This is doc 04.5's `extract.digest` and doc 10.5's `extract_digest`
//! column. Two fetchers handed the same bytes must produce the same 32 bytes,
//! because that equality is the whole of doc 06.4's agreement check: a
//! coordinator that has two receipts for one URL compares these and, if they
//! match, has two independent parties saying the same thing about a page it
//! never fetched.
//!
//! # Why this is not CBOR
//!
//! Doc 04.5 originally said blake3 over the canonical CBOR of the extraction,
//! and that has been changed to what this file does. The reason is that
//! canonical CBOR is a serialisation format with choices in it. RFC 8949
//! section 4.2 gives two canonicalisation orderings, leaves float shortening
//! to the encoder, and lets an encoder pick between definite and indefinite
//! length for the same value. Every one of those is a place where two honest
//! implementations produce different bytes for the same data, and the failure
//! mode is not a parse error, it is a community fetcher whose receipts never
//! agree with anybody's and which quietly loses reputation for a bug in a
//! library it did not write.
//!
//! So there is no encoder. The digest is a walk over the extracted values in
//! a fixed order, each one tagged with a byte that says which field it is and
//! prefixed with its length. It is not a format anybody can decode, which is
//! the point: it is only ever compared. Writing a second implementation of it
//! in another language is an afternoon and there is nothing in it to disagree
//! about.
//!
//! # What goes in
//!
//! The values doc 10.5 stores, plus the ones doc 04.5's receipt names, and
//! nothing derived from timing or from the machine. No fetch duration, no
//! header set, no fetcher id. Two fetchers on different continents see
//! different headers from the same CDN and take different amounts of time, and
//! folding either in would make agreement impossible on purpose.
//!
//! The extractor version does go in, tagged first. Doc 11.1 says the same
//! input and the same version produce the same output, and says nothing about
//! two versions, so a digest that ignored the version would assert an
//! agreement the spec does not promise. A coordinator comparing receipts from
//! two extractor versions gets a disagreement, which is the honest answer.

use umi_extract::{Extracted, Heading};

/// The domain separator, so this digest can never collide with another one in
/// the tree that happens to hash the same bytes.
const DOMAIN: &[u8] = b"umi-extract-digest/1";

/// Which field each tagged value is.
///
/// The numbers are a published format for the same reason the codes in
/// `umi_types::OutcomeCode` are: a receipt digested by one build is checked by
/// another. Appended, never renumbered, and a retired tag stays reserved.
mod tag {
    pub const VERSION: u8 = 0;
    pub const MARKDOWN: u8 = 1;
    pub const TITLE: u8 = 2;
    pub const DESCRIPTION: u8 = 3;
    pub const CANONICAL: u8 = 4;
    pub const PUBLISHED: u8 = 5;
    pub const MODIFIED: u8 = 6;
    pub const DECLARED_LANG: u8 = 7;
    pub const HEADING: u8 = 8;
    pub const FEED: u8 = 9;
    pub const LINK: u8 = 10;
    pub const ROBOTS: u8 = 11;
    pub const WITHHELD: u8 = 12;
    pub const STRUCTURED_TYPE: u8 = 13;
    pub const STRUCTURED_SCALAR: u8 = 14;
    pub const COUNTS: u8 = 15;
}

/// Doc 04.5's `extract.digest` over one extraction.
#[must_use]
pub fn extract_digest(extracted: &Extracted) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN);
    let mut feed = Feed { hasher };

    feed.text(tag::VERSION, extracted.version);
    feed.text(tag::MARKDOWN, &extracted.markdown);

    let meta = &extracted.meta;
    feed.maybe(tag::TITLE, meta.title.as_deref());
    feed.maybe(tag::DESCRIPTION, meta.description.as_deref());
    feed.maybe(tag::CANONICAL, meta.canonical.as_deref());
    feed.maybe(tag::PUBLISHED, meta.published.as_deref());
    feed.maybe(tag::MODIFIED, meta.modified.as_deref());
    feed.maybe(tag::DECLARED_LANG, meta.declared_lang.as_deref());

    // Order is document order for headings and links and insertion order for
    // feeds, all of which doc 11 already fixes. Nothing is sorted here,
    // because sorting would hide a real disagreement: two extractors that
    // found the same links in a different order did not agree about the page.
    feed.count(tag::HEADING, meta.headings.len());
    for Heading { level, text } in &meta.headings {
        feed.byte(*level);
        feed.bytes(text.as_bytes());
    }

    feed.count(tag::FEED, meta.feeds.len());
    for url in &meta.feeds {
        feed.bytes(url.as_bytes());
    }

    feed.count(tag::LINK, extracted.links.links.len());
    for link in &extracted.links.links {
        feed.bytes(link.url.as_bytes());
        feed.bytes(link.anchor.as_bytes());
        feed.u16(link.rel.bits());
        feed.byte(link.kind.as_u8());
    }

    // Doc 11.4's directives, because a page that says noindex is a different
    // extraction from the same page that does not: the markdown is withheld.
    feed.count(tag::ROBOTS, 2);
    feed.byte(u8::from(extracted.robots.noindex));
    feed.byte(u8::from(extracted.robots.nofollow));

    feed.count(
        tag::WITHHELD,
        usize::from(extracted.content_withheld.is_some()),
    );
    if let Some(reason) = extracted.content_withheld {
        feed.byte(withheld_code(reason));
    }

    let structured = &meta.structured;
    feed.count(tag::STRUCTURED_TYPE, structured.types.len());
    for kind in &structured.types {
        feed.bytes(kind.as_bytes());
    }
    feed.count(tag::STRUCTURED_SCALAR, 4);
    feed.opt_bytes(structured.published.as_deref());
    feed.opt_bytes(structured.modified.as_deref());
    feed.opt_bytes(structured.author.as_deref());
    feed.opt_bytes(structured.headline.as_deref());

    // The two counts doc 04.5's receipt carries next to the digest. They are
    // redundant with the values above and they are in anyway, so that a
    // receipt whose counts were edited disagrees with its own digest rather
    // than merely looking odd.
    feed.count(tag::COUNTS, 2);
    feed.u32(extracted.signals.text_bytes);
    feed.u32(extracted.signals.link_count);

    // Microdata and RDFa are flags rather than parsed content, per doc 11.6,
    // and they are deliberately not in the digest. Detecting them depends on
    // how far the parser got through a malformed document, which is the one
    // thing two html5ever versions are least likely to agree about, and a
    // boolean nobody stores is not worth a disagreement over.

    *feed.hasher.finalize().as_bytes()
}

/// Doc 11.4's reason a body was withheld, as a byte.
///
/// `Withheld` is `non_exhaustive`, so this needs a fallback, and 255 is not a
/// wrong answer dressed as a right one: it says the body was withheld for a
/// reason this build cannot name, which is exactly what has happened. Two
/// fetchers on the same version still agree, and two on different versions
/// were already going to disagree on the version tag.
const fn withheld_code(reason: umi_extract::Withheld) -> u8 {
    match reason {
        umi_extract::Withheld::Noindex => 0,
        _ => u8::MAX,
    }
}

/// A tagged, length prefixed walk over values.
///
/// Every value that goes in is either a fixed width scalar or is prefixed with
/// its length, so no two different sequences of values produce the same byte
/// stream. Without the prefixes a title of `"ab"` followed by a description of
/// `"c"` would hash identically to a title of `"a"` and a description of
/// `"bc"`, which is the same trap the link set digest in `umi-dedup` avoids
/// with a separator byte and which is worth being explicit about twice.
struct Feed {
    hasher: blake3::Hasher,
}

impl Feed {
    /// A tag, then a count, which is how every repeated field starts. The
    /// count goes in even when it is zero, so an absent list and an empty one
    /// are the same thing and both differ from a list of one.
    fn count(&mut self, tag: u8, n: usize) {
        self.hasher.update(&[tag]);
        self.hasher.update(&(n as u64).to_le_bytes());
    }

    /// A tag and one string.
    fn text(&mut self, tag: u8, value: &str) {
        self.hasher.update(&[tag]);
        self.bytes(value.as_bytes());
    }

    /// A tag and a string that may be absent. Absent is a length of `u64::MAX`
    /// rather than a length of zero, because a page with no description and a
    /// page with an empty one are different pages.
    fn maybe(&mut self, tag: u8, value: Option<&str>) {
        self.hasher.update(&[tag]);
        self.opt_bytes(value);
    }

    fn opt_bytes(&mut self, value: Option<&str>) {
        match value {
            Some(text) => self.bytes(text.as_bytes()),
            None => {
                self.hasher.update(&u64::MAX.to_le_bytes());
            }
        }
    }

    fn bytes(&mut self, value: &[u8]) {
        self.hasher.update(&(value.len() as u64).to_le_bytes());
        self.hasher.update(value);
    }

    fn byte(&mut self, value: u8) {
        self.hasher.update(&[value]);
    }

    fn u16(&mut self, value: u16) {
        self.hasher.update(&value.to_le_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.hasher.update(&value.to_le_bytes());
    }
}
