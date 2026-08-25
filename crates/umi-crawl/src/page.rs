//! Doc 10.5's `pages` row, built from what a fetch and an extraction produced.
//!
//! Two types here and they do different jobs. [`PageRow`] is one row as a
//! value, which is what a test asserts on and what a fetcher would send. It
//! holds owned strings and it is the shape the rest of the codebase thinks in.
//! [`PageBuilder`] is the writer's side: it takes rows and appends them
//! straight into Arrow's column builders, so a shoal of 16384 rows exists once
//! as columns rather than twice as columns and structs. Push a row, drop it,
//! push the next.
//!
//! # Where the numbers come from
//!
//! Nothing in this file invents a value. `body_digest` and `content_length`
//! are the fetch's, `markdown` and `links` and the metadata are the
//! extraction's, the sketch and the chunk root are `umi-dedup`'s, and
//! `fetched_at_ms` is passed in rather than read from a clock, which is doc
//! 11.1's rule and the reason the whole builder is a pure function.
//!
//! The one place judgement was needed is what a row looks like when there was
//! no body, which is every 304, every error and every off domain redirect.
//! Doc 10.5 makes the digest columns non null, on the grounds that a row with
//! no body still has the digest of no body, so those rows carry the sketch and
//! the chunk root of the empty document. That is a real value and not a
//! placeholder: two error rows agree with each other, which is correct,
//! because they have the same content, which is none.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, ListBuilder, MapBuilder, MapFieldNames, RecordBatch,
    StringBuilder, StructBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Fields};
use umi_dedup::{ChunkTree, Content, Sketch};
use umi_extract::{Extracted, Link};
use umi_fetch::Outcome;
use umi_file::StreamKind;
use umi_types::{FetcherId, OutcomeCode, RowKey, Tier, Verification};

/// Which piece of the page a snippet came from.
///
/// Doc 10.2 budgets 400 bytes a row for "title, description, og, h1..h3",
/// which is the same handful of strings the dedicated columns already carry
/// plus the headings. The list exists so that a consumer building a search
/// result has one column to read instead of four, and so that the og values,
/// which are frequently different from the ones that won the doc 11.6
/// precedence, are not thrown away.
///
/// The discriminants are a published format. Appended, never renumbered.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(u8)]
pub enum SnippetKind {
    /// The title that won doc 11.6's precedence.
    Title = 0,
    /// The description that won it, author written or derived.
    Description = 1,
    /// An `h1`.
    H1 = 2,
    /// An `h2`.
    H2 = 3,
    /// An `h3`.
    H3 = 4,
    /// A JSON-LD `headline`, which is often the editorial title where the
    /// `<title>` element is the one with the site name bolted on.
    Headline = 5,
}

impl SnippetKind {
    /// Every kind, in code order.
    pub const ALL: [Self; 6] = [
        Self::Title,
        Self::Description,
        Self::H1,
        Self::H2,
        Self::H3,
        Self::Headline,
    ];

    /// The byte doc 10.5's `snippets.kind` column holds.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Recover a kind from a stored byte, or `None` for one this build does
    /// not know.
    #[must_use]
    pub const fn from_u8(byte: u8) -> Option<Self> {
        if (byte as usize) < Self::ALL.len() {
            Some(Self::ALL[byte as usize])
        } else {
            None
        }
    }

    /// The heading kind for an `h1`, `h2` or `h3`, and `None` for anything
    /// else. Doc 11.6 only collects those three.
    #[must_use]
    pub const fn for_heading(level: u8) -> Option<Self> {
        match level {
            1 => Some(Self::H1),
            2 => Some(Self::H2),
            3 => Some(Self::H3),
            _ => None,
        }
    }
}

/// One entry in the `snippets` column.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Snippet {
    /// Which piece of the page it is.
    pub kind: SnippetKind,
    /// The text, exactly as the corresponding column holds it.
    pub text: String,
}

/// One link, flattened to what doc 10.5 stores.
///
/// Narrower than [`umi_extract::Link`] on purpose. The extraction carries the
/// resolved URL and the anchor and the rel bits and the kind, and that is all
/// four of these, but it is `umi-extract`'s type and the file format should
/// not be pinned to it. If doc 11.4 ever adds a field, this is the one place
/// that has to decide whether the format grows a column.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RowLink {
    /// The canonical absolute target, already through doc 11.2.
    pub href: String,
    /// The anchor text, capped by doc 11.4 at [`umi_extract::MAX_ANCHOR`].
    pub anchor: String,
    /// The `rel` bits from [`umi_extract::Rel`].
    pub rel: u16,
    /// The kind from [`umi_extract::LinkKind`].
    pub kind: u8,
}

impl From<&Link> for RowLink {
    fn from(link: &Link) -> Self {
        Self {
            href: link.url.clone(),
            anchor: link.anchor.clone(),
            rel: link.rel.bits(),
            kind: link.kind.as_u8(),
        }
    }
}

/// Everything one crawl of one URL produced.
///
/// A borrowed view rather than an owned struct, because the caller already
/// holds all of it and the row builder only reads. The fields are in the order
/// the loop learns them: what was asked for, what came back, what came out of
/// it, and then the provenance that doc 06 needs.
#[derive(Debug)]
pub struct Crawled<'a> {
    /// The URL the lease was for, canonical, which is what the row is keyed
    /// on however many redirects were followed.
    pub url: &'a str,
    /// The keys derived from that URL by doc 11.2.
    pub keys: RowKey,
    /// The host, as text, because a consumer filtering by site should not have
    /// to reverse a hash.
    pub host: &'a str,
    /// When the fetch happened, in milliseconds since the Unix epoch. Passed
    /// in and never read from a clock, per doc 11.1.
    pub fetched_at_ms: u64,
    /// What the fetch turned into.
    pub outcome: &'a Outcome,
    /// What doc 11 made of the body, if there was a body worth extracting.
    pub extracted: Option<&'a Extracted>,
    /// The tier that produced this answer.
    pub tier_used: Tier,
    /// Every tier that was tried, cheapest first, ending in `tier_used`. Doc
    /// 05.10 publishes the distribution of these and it is the statistic the
    /// whole ladder exists to justify.
    pub tier_path: &'a [Tier],
    /// When robots was last checked for this host, from doc 07.
    pub robots_checked_ms: u64,
    /// The AIPREF `Content-Usage` value that applied, if any.
    pub content_usage: Option<&'a str>,
    /// Who fetched it. [`FetcherId::LOCAL`] for our own servers.
    pub fetcher_id: FetcherId,
    /// How much we know about that claim, from doc 06.
    pub verification: Verification,
    /// Which doc 13 crawl profile admitted the URL. Zero is the open crawl.
    pub crawl_profile: u32,
}

/// One row of doc 10.5's `pages`.
///
/// Owned, because it outlives the borrow of everything it came from, and
/// because a fetcher sends one of these over doc 04's protocol without a
/// segment writer anywhere in sight.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PageRow {
    /// The URL the lease was for.
    pub url: String,
    /// Where the redirects ended, or `None` when that is the same place. Doc
    /// 10.5 says null when it equals `url`, and that is most rows.
    pub final_url: Option<String>,
    /// Doc 11.2's 10 byte URL fingerprint.
    pub url_key: [u8; 10],
    /// The 8 byte pay level domain id.
    pub pld_id: [u8; 8],
    /// The host as text.
    pub host: String,
    /// Milliseconds since the Unix epoch.
    pub fetched_at_ms: u64,
    /// The HTTP status, or zero when we never got one.
    pub status: u16,
    /// Doc 04.5's outcome.
    pub outcome: OutcomeCode,
    /// The tier that produced it.
    pub tier_used: u8,
    /// Every tier tried, cheapest first.
    pub tier_path: Vec<u8>,
    /// `Content-Type` as sent, or `None` when there was no body.
    pub content_type: Option<String>,
    /// Body bytes as received, before decompression.
    pub content_length: u32,
    /// A BCP 47 primary subtag, padded to three bytes, or `None`.
    pub lang: Option<[u8; 3]>,
    /// Blake3 over the body bytes.
    pub body_digest: [u8; 32],
    /// Doc 04.5's blake3 tree over 16 KiB leaves of the body.
    pub chunk_root: [u8; 32],
    /// Doc 04.5's digest over the extraction.
    pub extract_digest: [u8; 32],
    /// The extracted markdown, or `None` when there is none to store, which
    /// includes a page that said `noindex`.
    pub markdown: Option<String>,
    /// Doc 11.6's title.
    pub title: Option<String>,
    /// Doc 11.6's description.
    pub description: Option<String>,
    /// `h1` through `h3` in document order.
    pub headings: Vec<String>,
    /// The same strings the dedicated columns hold, tagged, for a consumer
    /// that wants one column instead of four.
    pub snippets: Vec<Snippet>,
    /// The outlinks.
    pub links: Vec<RowLink>,
    /// Doc 11.5's sixteen headers and nothing else.
    pub headers_kept: Vec<(String, String)>,
    /// The AIPREF `Content-Usage` value that applied.
    pub content_usage: Option<String>,
    /// Doc 11.8's 64 MinHash values and the simhash over the same shingles.
    pub sketch: Sketch,
    /// Doc 11.7's exact duplicate key, which is blake3 over the plain text.
    ///
    /// Not a doc 10.5 column, and deliberately so: it is derivable from the
    /// markdown, and doc 10.5 stores the sketch instead because the sketch is
    /// not. It is on the row because the crawl loop wants it immediately, to
    /// ask the ledger whether this exact text is already somewhere else before
    /// deciding the row is worth writing.
    pub text_digest: [u8; 32],
    /// Plain text length, which doc 11.3 does not store as text.
    pub text_bytes: u32,
    /// How many links, which is redundant with `links` and is a column because
    /// a consumer filtering on it should not have to decode the list.
    pub link_count: u32,
    /// Who fetched it.
    pub fetcher_id: [u8; 32],
    /// Doc 06's verification level.
    pub verification: u8,
    /// When robots was last checked for the host.
    pub robots_checked_ms: u64,
    /// The doc 13 crawl profile.
    pub crawl_profile: u32,
}

impl PageRow {
    /// Build the row for one crawl.
    ///
    /// Pure: the same [`Crawled`] produces the same row on every machine, and
    /// that is what doc 16's gate 1.2 checks across three of them.
    #[must_use]
    pub fn build(crawled: &Crawled<'_>) -> Self {
        let outcome = crawled.outcome.code();
        let page = crawled.outcome.page();
        let body: &[u8] = page.map_or(&[], |page| page.body.as_ref());

        // The chunk tree is over the body bytes and the sketch is over the
        // plain text, which are two different things on purpose. Doc 04.5's
        // audit asks a fetcher for chunk 47 of what it downloaded, so the tree
        // has to be over what it downloaded. Doc 11.7's duplicate key is about
        // whether two pages say the same thing, so the sketch has to be over
        // the prose with the ad slots gone.
        let chunk_root = ChunkTree::build(body).root();

        // `Extracted::text` regenerates the plain text from the markdown,
        // which doc 11.3 says is cheaper than carrying it, and it is wanted
        // twice here: once for the exact duplicate digest and once for the
        // sketch. `Content::of` does both in one pass over it.
        let content = crawled
            .extracted
            .map_or_else(|| Content::of(""), |e| Content::of(&e.text()));

        let (status, content_type, content_length, body_digest, headers_kept, final_url) =
            match crawled.outcome {
                Outcome::Ok(page) => (
                    page.status,
                    page.content_type.clone(),
                    u32::try_from(page.body.len()).unwrap_or(u32::MAX),
                    page.body_digest,
                    page.headers_kept.clone(),
                    (page.final_url != crawled.url).then(|| page.final_url.clone()),
                ),
                Outcome::NotModified {
                    headers_kept,
                    headers_digest,
                    ..
                } => (
                    304,
                    None,
                    0,
                    // A 304 has no body, so there is no body digest to record.
                    // The header digest goes here rather than a run of zeroes,
                    // because two 304s from the same origin minutes apart
                    // should not look like the same row, and because doc 05.3
                    // needs a way to notice a revalidator that changed its
                    // mind about the cache directives without changing the
                    // body. It is documented as a 304 only meaning and the
                    // `outcome` column is how a reader tells which it is.
                    *headers_digest,
                    headers_kept.clone(),
                    None,
                ),
                Outcome::Gone => (410, None, 0, EMPTY_DIGEST, Vec::new(), None),
                Outcome::Failed { status, .. } => {
                    (status.unwrap_or(0), None, 0, EMPTY_DIGEST, Vec::new(), None)
                }
                Outcome::RedirectedOffDomain { target, status, .. } => (
                    *status,
                    None,
                    0,
                    EMPTY_DIGEST,
                    Vec::new(),
                    Some(target.clone()),
                ),
                // `Outcome` is `non_exhaustive`, so a variant added upstream
                // lands here rather than failing the build. A row that says
                // nothing about its status is wrong, but it is a great deal
                // less wrong than one that guesses.
                _ => (0, None, 0, EMPTY_DIGEST, Vec::new(), None),
            };

        let extracted = crawled.extracted;
        let withheld = extracted.is_some_and(|e| e.content_withheld.is_some());

        Self {
            url: crawled.url.to_owned(),
            final_url,
            url_key: *crawled.keys.url.as_bytes(),
            pld_id: *crawled.keys.pld.as_bytes(),
            host: crawled.host.to_owned(),
            fetched_at_ms: crawled.fetched_at_ms,
            status,
            outcome,
            tier_used: crawled.tier_used.as_u8(),
            tier_path: crawled.tier_path.iter().map(|t| t.as_u8()).collect(),
            content_type,
            content_length,
            lang: extracted.and_then(|e| lang_code(e.meta.declared_lang.as_deref())),
            body_digest,
            chunk_root,
            extract_digest: extracted.map_or(EMPTY_DIGEST, crate::extract_digest),
            // Doc 11.4: a `noindex` page keeps its URL, its status, its
            // headers and its links, and loses its prose. So these three go
            // null while `links` below does not.
            markdown: extracted.filter(|_| !withheld).map(|e| e.markdown.clone()),
            title: extracted
                .filter(|_| !withheld)
                .and_then(|e| e.meta.title.clone()),
            description: extracted
                .filter(|_| !withheld)
                .and_then(|e| e.meta.description.clone()),
            headings: extracted
                .filter(|_| !withheld)
                .map(|e| e.meta.headings.iter().map(|h| h.text.clone()).collect())
                .unwrap_or_default(),
            snippets: extracted
                .filter(|_| !withheld)
                .map(snippets_of)
                .unwrap_or_default(),
            links: extracted
                .map(|e| e.links.links.iter().map(RowLink::from).collect())
                .unwrap_or_default(),
            headers_kept,
            content_usage: crawled.content_usage.map(ToOwned::to_owned),
            sketch: content.sketch,
            text_digest: content.digest,
            text_bytes: content.text_bytes,
            link_count: extracted.map_or(0, |e| e.signals.link_count),
            fetcher_id: *crawled.fetcher_id.as_bytes(),
            verification: crawled.verification.as_u8(),
            robots_checked_ms: crawled.robots_checked_ms,
            crawl_profile: crawled.crawl_profile,
        }
    }

    /// How much variable width column data this row carries.
    ///
    /// What [`PageBuilder::bytes`] accumulates, so that a caller can ask a
    /// single row how big it is before deciding to start a new shoal for it.
    /// The URL and the host are in because a page of query string is a real
    /// thing, and the fixed width columns are out because they are the same
    /// for every row.
    #[must_use]
    pub fn variable_bytes(&self) -> usize {
        fn some(text: Option<&String>) -> usize {
            text.map_or(0, String::len)
        }

        self.url.len()
            + some(self.final_url.as_ref())
            + self.host.len()
            + some(self.content_type.as_ref())
            + some(self.markdown.as_ref())
            + some(self.title.as_ref())
            + some(self.description.as_ref())
            + some(self.content_usage.as_ref())
            + self.tier_path.len()
            + self.headings.iter().map(String::len).sum::<usize>()
            + self
                .snippets
                .iter()
                .map(|s| s.text.len() + 1)
                .sum::<usize>()
            + self
                .links
                .iter()
                .map(|l| l.href.len() + l.anchor.len() + 3)
                .sum::<usize>()
            + self
                .headers_kept
                .iter()
                .map(|(name, value)| name.len() + value.len())
                .sum::<usize>()
    }

    /// Whether two rows are near duplicates at a given Jaccard threshold.
    ///
    /// Doc 11.7's banding is 8 bands of 8 rows, which detects pairs at about
    /// 0.77 similarity, so that is the threshold to pass unless there is a
    /// reason not to. This is the exact comparison and not the banded one: the
    /// bands are how a candidate is found among millions, and this is how the
    /// candidate is confirmed.
    #[must_use]
    pub fn is_near_duplicate_of(&self, other: &Self, threshold: f32) -> bool {
        self.sketch.jaccard(&other.sketch) >= threshold
    }
}

/// Blake3 of nothing, which is the digest a row with no body carries.
const EMPTY_DIGEST: [u8; 32] = [
    0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6, 0xa0, 0x40, 0x4d, 0xea, 0x36, 0xdc, 0xc9, 0x49,
    0x9b, 0xcb, 0x25, 0xc9, 0xad, 0xc1, 0x12, 0xb7, 0xcc, 0x9a, 0x93, 0xca, 0xe4, 0x1f, 0x32, 0x62,
];

/// Doc 11.6's strings, tagged.
fn snippets_of(extracted: &Extracted) -> Vec<Snippet> {
    let meta = &extracted.meta;
    let mut out = Vec::with_capacity(meta.headings.len() + 3);
    if let Some(title) = &meta.title {
        out.push(Snippet {
            kind: SnippetKind::Title,
            text: title.clone(),
        });
    }
    if let Some(description) = &meta.description {
        out.push(Snippet {
            kind: SnippetKind::Description,
            text: description.clone(),
        });
    }
    if let Some(headline) = &meta.structured.headline {
        out.push(Snippet {
            kind: SnippetKind::Headline,
            text: headline.clone(),
        });
    }
    for heading in &meta.headings {
        if let Some(kind) = SnippetKind::for_heading(heading.level) {
            out.push(Snippet {
                kind,
                text: heading.text.clone(),
            });
        }
    }
    out
}

/// The three byte `lang` column from a declared language tag.
///
/// Only the primary subtag, so `en-GB` becomes `en\0`, which is what doc 10.5
/// asks for. A tag that is not two or three ASCII letters produces `None`
/// rather than a truncation, because `zh-Hant` truncated to `zh-` would be a
/// lie about a real distinction.
///
/// This is the declared language and not the detected one. Doc 11.6 wants
/// trigram detection over the first 4 KiB with anything under 0.5 confidence
/// stored as `und`, and that is milestone 2 work. Until it lands the column
/// holds what the publisher said, which is right more often than not and is
/// never a guess we made.
fn lang_code(declared: Option<&str>) -> Option<[u8; 3]> {
    let tag = declared?;
    let primary = tag.split('-').next()?;
    if !matches!(primary.len(), 2 | 3) || !primary.bytes().all(|b| b.is_ascii_lowercase()) {
        return None;
    }
    let mut out = [0u8; 3];
    out[..primary.len()].copy_from_slice(primary.as_bytes());
    Some(out)
}

/// Rows in, one Arrow batch out.
///
/// Doc 10.4 seals a shoal at 16384 rows or 32 MiB encoded, so that is the size
/// this is built for: push until the writer says the shoal is full, call
/// [`finish`](PageBuilder::finish), hand the batch to
/// `umi_file::SegmentWriter::push`, and start again. Reusing the builder after
/// `finish` is not possible on purpose, because Arrow's builders reset to
/// empty and a bug that pushed into a finished builder would silently write
/// half a shoal.
pub struct PageBuilder {
    url: StringBuilder,
    final_url: StringBuilder,
    url_key: FixedSizeBinaryBuilder,
    pld_id: FixedSizeBinaryBuilder,
    host: StringBuilder,
    fetched_at_ms: UInt64Builder,
    status: UInt16Builder,
    outcome: UInt8Builder,
    tier_used: UInt8Builder,
    tier_path: ListBuilder<UInt8Builder>,
    content_type: StringBuilder,
    content_length: UInt32Builder,
    lang: FixedSizeBinaryBuilder,
    body_digest: FixedSizeBinaryBuilder,
    chunk_root: FixedSizeBinaryBuilder,
    extract_digest: FixedSizeBinaryBuilder,
    markdown: StringBuilder,
    title: StringBuilder,
    description: StringBuilder,
    headings: ListBuilder<StringBuilder>,
    snippets: ListBuilder<StructBuilder>,
    links: ListBuilder<StructBuilder>,
    headers_kept: MapBuilder<StringBuilder, StringBuilder>,
    content_usage: StringBuilder,
    minhash: FixedSizeBinaryBuilder,
    simhash: UInt64Builder,
    text_bytes: UInt32Builder,
    link_count: UInt32Builder,
    fetcher_id: FixedSizeBinaryBuilder,
    verification: UInt8Builder,
    robots_checked_ms: UInt64Builder,
    crawl_profile: UInt32Builder,
    rows: usize,
    bytes: usize,
}

impl Default for PageBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// The struct fields of the `snippets` column, which have to match doc 10.5
/// exactly, names and nullability included, or `RecordBatch::try_new` refuses
/// a batch whose data is identical.
fn snippet_fields() -> Fields {
    Fields::from(vec![
        Field::new("kind", DataType::UInt8, false),
        Field::new("text", DataType::Utf8, false),
    ])
}

/// The struct fields of the `links` column.
fn link_fields() -> Fields {
    Fields::from(vec![
        Field::new("href", DataType::Utf8, false),
        Field::new("anchor", DataType::Utf8, false),
        Field::new("rel", DataType::UInt16, false),
        Field::new("kind", DataType::UInt8, false),
    ])
}

fn list_of<T: arrow::array::ArrayBuilder>(values: T, item: DataType) -> ListBuilder<T> {
    ListBuilder::new(values).with_field(Arc::new(Field::new("item", item, false)))
}

impl PageBuilder {
    /// An empty builder.
    #[must_use]
    pub fn new() -> Self {
        let snippets = snippet_fields();
        let links = link_fields();
        Self {
            url: StringBuilder::new(),
            final_url: StringBuilder::new(),
            url_key: FixedSizeBinaryBuilder::new(10),
            pld_id: FixedSizeBinaryBuilder::new(8),
            host: StringBuilder::new(),
            fetched_at_ms: UInt64Builder::new(),
            status: UInt16Builder::new(),
            outcome: UInt8Builder::new(),
            tier_used: UInt8Builder::new(),
            tier_path: list_of(UInt8Builder::new(), DataType::UInt8),
            content_type: StringBuilder::new(),
            content_length: UInt32Builder::new(),
            lang: FixedSizeBinaryBuilder::new(3),
            body_digest: FixedSizeBinaryBuilder::new(32),
            chunk_root: FixedSizeBinaryBuilder::new(32),
            extract_digest: FixedSizeBinaryBuilder::new(32),
            markdown: StringBuilder::new(),
            title: StringBuilder::new(),
            description: StringBuilder::new(),
            headings: list_of(StringBuilder::new(), DataType::Utf8),
            snippets: list_of(
                StructBuilder::new(
                    snippets.clone(),
                    vec![
                        Box::new(UInt8Builder::new()),
                        Box::new(StringBuilder::new()),
                    ],
                ),
                DataType::Struct(snippets),
            ),
            links: list_of(
                StructBuilder::new(
                    links.clone(),
                    vec![
                        Box::new(StringBuilder::new()),
                        Box::new(StringBuilder::new()),
                        Box::new(UInt16Builder::new()),
                        Box::new(UInt8Builder::new()),
                    ],
                ),
                DataType::Struct(links),
            ),
            headers_kept: MapBuilder::new(
                Some(MapFieldNames {
                    entry: "entries".to_owned(),
                    key: "key".to_owned(),
                    value: "value".to_owned(),
                }),
                StringBuilder::new(),
                StringBuilder::new(),
            )
            .with_values_field(Field::new("value", DataType::Utf8, false)),
            content_usage: StringBuilder::new(),
            minhash: FixedSizeBinaryBuilder::new(256),
            simhash: UInt64Builder::new(),
            text_bytes: UInt32Builder::new(),
            link_count: UInt32Builder::new(),
            fetcher_id: FixedSizeBinaryBuilder::new(32),
            verification: UInt8Builder::new(),
            robots_checked_ms: UInt64Builder::new(),
            crawl_profile: UInt32Builder::new(),
            rows: 0,
            bytes: 0,
        }
    }

    /// Doc 10.4's row half of the seal rule.
    pub const ROW_LIMIT: usize = 16_384;

    /// Doc 10.4's byte half of the seal rule.
    ///
    /// Doc 10.4 counts encoded bytes and this counts the bytes going in, which
    /// is the same number before compression and a larger one after. Sealing
    /// early is the safe direction: it costs a slightly smaller shoal and it
    /// keeps the builder inside the limit below.
    pub const BYTE_LIMIT: usize = 32 << 20;

    /// How many rows have gone in.
    #[must_use]
    pub const fn rows(&self) -> usize {
        self.rows
    }

    /// How many bytes of variable width column data have gone in.
    ///
    /// Markdown, links, snippets, headings and headers, which is everything
    /// whose size depends on the page rather than on the schema. The fixed
    /// width columns are ignored because they are `rows` times a constant and
    /// the caller can add that itself if it cares.
    #[must_use]
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Whether doc 10.4 says to seal this shoal.
    ///
    /// This is not advice. Arrow's `Utf8` arrays carry 32 bit offsets, so a
    /// single column of a batch cannot hold more than 2 GiB of text, and a
    /// builder pushed past that aborts inside Arrow with `byte array offset
    /// overflow` rather than returning anything a caller could handle. Doc
    /// 10.4's 32 MiB is sixty four times under that ceiling, so a caller that
    /// seals when told will never see it, and a caller that ignores this will
    /// eventually crash a crawler holding a segment's worth of unwritten
    /// pages.
    ///
    /// The row limit alone is not enough, and that is worth being plain about
    /// because it looked like it was. 16384 rows of a page carrying 150 KB of
    /// markdown is 2.4 GB in the `markdown` column, which is over the ceiling
    /// on its own before any of the other thirty one columns are counted.
    /// Pages that size are rare and a crawl of a hundred billion of them will
    /// meet sixteen thousand in a row eventually.
    #[must_use]
    pub const fn is_full(&self) -> bool {
        self.rows >= Self::ROW_LIMIT || self.bytes >= Self::BYTE_LIMIT
    }

    /// Whether anything has gone in.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Append one row.
    ///
    /// Every `expect` below is a width assertion against a fixed size column,
    /// and every one of them is against an array whose length is a constant in
    /// the type. They cannot fire, and they are `expect` rather than `unwrap`
    /// so that if the schema and the row ever drift apart the panic says which
    /// column.
    pub fn push(&mut self, row: &PageRow) {
        self.url.append_value(&row.url);
        self.final_url.append_option(row.final_url.as_deref());
        self.url_key
            .append_value(row.url_key)
            .expect("url_key is 10 bytes");
        self.pld_id
            .append_value(row.pld_id)
            .expect("pld_id is 8 bytes");
        self.host.append_value(&row.host);
        self.fetched_at_ms.append_value(row.fetched_at_ms);
        self.status.append_value(row.status);
        self.outcome.append_value(row.outcome.as_u8());
        self.tier_used.append_value(row.tier_used);
        for tier in &row.tier_path {
            self.tier_path.values().append_value(*tier);
        }
        self.tier_path.append(true);
        self.content_type.append_option(row.content_type.as_deref());
        self.content_length.append_value(row.content_length);
        match row.lang {
            Some(code) => self.lang.append_value(code).expect("lang is 3 bytes"),
            None => self.lang.append_null(),
        }
        self.body_digest
            .append_value(row.body_digest)
            .expect("body_digest is 32 bytes");
        self.chunk_root
            .append_value(row.chunk_root)
            .expect("chunk_root is 32 bytes");
        self.extract_digest
            .append_value(row.extract_digest)
            .expect("extract_digest is 32 bytes");
        self.markdown.append_option(row.markdown.as_deref());
        self.title.append_option(row.title.as_deref());
        self.description.append_option(row.description.as_deref());
        for heading in &row.headings {
            self.headings.values().append_value(heading);
        }
        self.headings.append(true);

        for snippet in &row.snippets {
            let values = self.snippets.values();
            values
                .field_builder::<UInt8Builder>(0)
                .expect("snippets has a kind field")
                .append_value(snippet.kind.as_u8());
            values
                .field_builder::<StringBuilder>(1)
                .expect("snippets has a text field")
                .append_value(&snippet.text);
            values.append(true);
        }
        self.snippets.append(true);

        for link in &row.links {
            let values = self.links.values();
            values
                .field_builder::<StringBuilder>(0)
                .expect("links has an href field")
                .append_value(&link.href);
            values
                .field_builder::<StringBuilder>(1)
                .expect("links has an anchor field")
                .append_value(&link.anchor);
            values
                .field_builder::<UInt16Builder>(2)
                .expect("links has a rel field")
                .append_value(link.rel);
            values
                .field_builder::<UInt8Builder>(3)
                .expect("links has a kind field")
                .append_value(link.kind);
            values.append(true);
        }
        self.links.append(true);

        for (name, value) in &row.headers_kept {
            self.headers_kept.keys().append_value(name);
            self.headers_kept.values().append_value(value);
        }
        self.headers_kept
            .append(true)
            .expect("one value was appended for every key");

        self.content_usage
            .append_option(row.content_usage.as_deref());
        self.minhash
            .append_value(row.sketch.to_bytes())
            .expect("the sketch is 256 bytes");
        self.simhash.append_value(row.sketch.simhash);
        self.text_bytes.append_value(row.text_bytes);
        self.link_count.append_value(row.link_count);
        self.fetcher_id
            .append_value(row.fetcher_id)
            .expect("fetcher_id is 32 bytes");
        self.verification.append_value(row.verification);
        self.robots_checked_ms.append_value(row.robots_checked_ms);
        self.crawl_profile.append_value(row.crawl_profile);
        self.rows += 1;
        self.bytes += row.variable_bytes();
    }

    /// Everything pushed so far, as a batch doc 10.5 accepts.
    ///
    /// # Panics
    ///
    /// If the columns built here stop matching [`StreamKind::Pages`]. That is
    /// a bug in this file and not something a caller can cause, and it is
    /// checked by a test that pushes one of every shape of row.
    #[must_use]
    pub fn finish(mut self) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.url.finish()),
            Arc::new(self.final_url.finish()),
            Arc::new(self.url_key.finish()),
            Arc::new(self.pld_id.finish()),
            Arc::new(self.host.finish()),
            Arc::new(self.fetched_at_ms.finish()),
            Arc::new(self.status.finish()),
            Arc::new(self.outcome.finish()),
            Arc::new(self.tier_used.finish()),
            Arc::new(self.tier_path.finish()),
            Arc::new(self.content_type.finish()),
            Arc::new(self.content_length.finish()),
            Arc::new(self.lang.finish()),
            Arc::new(self.body_digest.finish()),
            Arc::new(self.chunk_root.finish()),
            Arc::new(self.extract_digest.finish()),
            Arc::new(self.markdown.finish()),
            Arc::new(self.title.finish()),
            Arc::new(self.description.finish()),
            Arc::new(self.headings.finish()),
            Arc::new(self.snippets.finish()),
            Arc::new(self.links.finish()),
            Arc::new(self.headers_kept.finish()),
            Arc::new(self.content_usage.finish()),
            Arc::new(self.minhash.finish()),
            Arc::new(self.simhash.finish()),
            Arc::new(self.text_bytes.finish()),
            Arc::new(self.link_count.finish()),
            Arc::new(self.fetcher_id.finish()),
            Arc::new(self.verification.finish()),
            Arc::new(self.robots_checked_ms.finish()),
            Arc::new(self.crawl_profile.finish()),
        ];
        RecordBatch::try_new(StreamKind::Pages.arrow(), columns)
            .expect("the page builder matches doc 10.5")
    }
}
