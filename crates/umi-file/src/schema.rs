//! The three stream schemas from doc 10.5, as Arrow.
//!
//! Arrow rather than a type of our own because doc 10.10 makes a shoal into a
//! Parquet row group one to one, and the shortest path from a column chunk to a
//! Parquet page runs through Arrow. It also means the writer takes a
//! `RecordBatch` and the reader gives one back, so neither end of this crate
//! has a hand written builder for a thirty column schema.
//!
//! Doc 10.3: three stream kinds share the container and differ only in schema.
//! The header names the stream and the schema id, and a reader that does not
//! recognise the schema id refuses to open the file rather than guessing. That
//! keeps one writer, one reader and one crash story instead of three.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

use crate::codec::Codec;
use crate::{Error, Result};

/// Which of doc 10.3's three streams a segment carries.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[repr(u16)]
pub enum StreamKind {
    /// Crawled pages, which is the schema that matters.
    Pages = 1,
    /// Doc 04 receipts, one row per delivery, including the signature, so that
    /// anyone can re verify the published corpus against the fetcher keys
    /// without trusting us.
    Receipts = 2,
    /// Host, fetch time, status, raw text and the parsed decision summary from
    /// doc 07.4.
    Robots = 3,
}

impl StreamKind {
    /// The schema id written into the header.
    ///
    /// It moves when the schema changes in a way that would make an old reader
    /// wrong, which is not the same as when the format version moves. In
    /// practice both move together, because doc 10.11 says the answer to a
    /// format change is to drain the writers and restart them.
    #[must_use]
    pub const fn schema_id(self) -> u32 {
        match self {
            Self::Pages => 1,
            Self::Receipts => 2,
            Self::Robots => 3,
        }
    }

    /// Read the code back out of a header.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownStream`] for anything else, which is a file from a
    /// version we do not have.
    pub const fn from_code(code: u16) -> Result<Self> {
        match code {
            1 => Ok(Self::Pages),
            2 => Ok(Self::Receipts),
            3 => Ok(Self::Robots),
            other => Err(Error::UnknownStream(other)),
        }
    }

    /// The Arrow schema for this stream.
    #[must_use]
    pub fn arrow(self) -> SchemaRef {
        match self {
            Self::Pages => pages(),
            Self::Receipts => receipts(),
            Self::Robots => robots(),
        }
    }
}

fn nn(name: &str, ty: DataType) -> Field {
    Field::new(name, ty, false)
}

fn ok(name: &str, ty: DataType) -> Field {
    Field::new(name, ty, true)
}

fn utf8() -> DataType {
    DataType::Utf8
}

fn fixed(n: i32) -> DataType {
    DataType::FixedSizeBinary(n)
}

fn list(item: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", item, false)))
}

fn list_of(fields: Vec<Field>) -> DataType {
    DataType::List(Arc::new(Field::new(
        "item",
        DataType::Struct(Fields::from(fields)),
        false,
    )))
}

/// A string to string map, laid out the way Arrow wants it.
fn map() -> DataType {
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ])),
        false,
    );
    DataType::Map(Arc::new(entries), false)
}

/// Doc 10.5's `pages` schema.
///
/// Nullability follows what can genuinely be absent rather than what is
/// convenient. `final_url` is null when it equals `url`, which doc 10.5 says
/// outright. `markdown`, `title` and `description` are null on a response that
/// had no body to extract, which is every 304 and every error row, and those
/// are a large fraction of a revalidation heavy segment. The digests are not
/// null, because a row with no body still has the digest of no body.
fn pages() -> SchemaRef {
    Arc::new(Schema::new(vec![
        nn("url", utf8()),
        ok("final_url", utf8()),
        nn("url_key", fixed(10)),
        nn("pld_id", fixed(8)),
        nn("host", utf8()),
        nn("fetched_at_ms", DataType::UInt64),
        nn("status", DataType::UInt16),
        nn("outcome", DataType::UInt8),
        nn("tier_used", DataType::UInt8),
        nn("tier_path", list(DataType::UInt8)),
        ok("content_type", utf8()),
        nn("content_length", DataType::UInt32),
        ok("lang", fixed(3)),
        nn("body_digest", fixed(32)),
        nn("chunk_root", fixed(32)),
        nn("extract_digest", fixed(32)),
        ok("markdown", utf8()),
        ok("title", utf8()),
        ok("description", utf8()),
        nn("headings", list(utf8())),
        nn(
            "snippets",
            list_of(vec![
                Field::new("kind", DataType::UInt8, false),
                Field::new("text", DataType::Utf8, false),
            ]),
        ),
        nn(
            "links",
            list_of(vec![
                Field::new("href", DataType::Utf8, false),
                Field::new("anchor", DataType::Utf8, false),
                Field::new("rel", DataType::UInt16, false),
                Field::new("kind", DataType::UInt8, false),
            ]),
        ),
        nn("headers_kept", map()),
        ok("content_usage", utf8()),
        nn("minhash", fixed(256)),
        nn("simhash", DataType::UInt64),
        nn("text_bytes", DataType::UInt32),
        nn("link_count", DataType::UInt32),
        nn("fetcher_id", fixed(32)),
        nn("verification", DataType::UInt8),
        nn("robots_checked_ms", DataType::UInt64),
        nn("crawl_profile", DataType::UInt32),
    ]))
}

/// Doc 04's `Receipt`, flattened.
///
/// Flattened rather than nested because doc 10.5 says one row per delivery and
/// because the optional groups in doc 04 are optional as a bundle: a receipt
/// either has a response or it does not. Nesting them would put a validity bit
/// on the group and another on every field inside it, and a reader would have
/// to check both. Flat with nullable fields says the same thing once.
fn receipts() -> SchemaRef {
    Arc::new(Schema::new(vec![
        nn("version", DataType::UInt32),
        nn("lease_id", fixed(16)),
        nn("nonce", fixed(16)),
        nn("fetcher_id", fixed(32)),
        nn("url", utf8()),
        ok("final_url", utf8()),
        nn("fetched_at_ms", DataType::UInt64),
        nn("duration_ms", DataType::UInt32),
        nn("outcome", DataType::UInt8),
        nn("tier_used", DataType::UInt8),
        nn("tier_path", list(DataType::UInt8)),
        nn("method", utf8()),
        nn(
            "redirects",
            list_of(vec![
                Field::new("from", DataType::Utf8, false),
                Field::new("to", DataType::Utf8, false),
                Field::new("status", DataType::UInt16, false),
            ]),
        ),
        ok("ja4", utf8()),
        nn("http_version", utf8()),
        ok("status", DataType::UInt16),
        ok("headers_digest", fixed(32)),
        nn("headers_kept", map()),
        ok("content_length", DataType::UInt32),
        ok("content_type", utf8()),
        ok("body_digest", fixed(32)),
        ok("body_length", DataType::UInt32),
        ok("chunk_root", fixed(32)),
        ok("chunk_count", DataType::UInt32),
        nn("tls_chain_digests", list(fixed(32))),
        ok("tls_sni", utf8()),
        ok("tls_alpn", utf8()),
        ok("tls_not_before_ms", DataType::UInt64),
        ok("tls_not_after_ms", DataType::UInt64),
        ok("extractor", utf8()),
        ok("extract_digest", fixed(32)),
        ok("stability", DataType::UInt8),
        ok("link_count", DataType::UInt32),
        ok("text_bytes", DataType::UInt32),
        nn("signature", fixed(64)),
    ]))
}

/// Doc 07.4's robots snapshot, which doc 07.4 says is published in full.
///
/// The raw text is kept because doc 07 says the corpus is worth publishing for
/// its own sake and a parsed summary is our reading of it rather than the
/// thing itself.
fn robots() -> SchemaRef {
    Arc::new(Schema::new(vec![
        nn("host", utf8()),
        nn("fetched_at_ms", DataType::UInt64),
        nn("status", DataType::UInt16),
        ok("body", utf8()),
        nn("groups", DataType::UInt32),
        nn("rules", DataType::UInt32),
        ok("crawl_delay_ms", DataType::UInt32),
        nn("allows_us", DataType::UInt8),
        nn("sitemaps", list(utf8())),
        ok("content_usage", utf8()),
    ]))
}

/// Which encoding a leaf column gets, from doc 10.6.
///
/// Doc 10.6 is explicit that there is no sampling based auto selection: the
/// writer knows the column, the column's encoding is fixed by the schema, and
/// removing the choice removes a class of bug and a chunk of CPU. So this is a
/// lookup by name and not a heuristic.
///
/// The one thing the name is consulted for is the digest columns. They are
/// uniformly random by construction and doc 10.6 says the writer skips
/// compressing them explicitly rather than discovering it per chunk.
#[must_use]
pub fn codec_for(name: &str, ty: &DataType) -> Codec {
    // Doc 10.6, digests, minhash and simhash: stored raw.
    if is_incompressible(name) {
        return Codec::Raw;
    }
    match ty {
        // Doc 10.6, long text: the only column where a general purpose
        // compressor earns its keep.
        DataType::Utf8 if name == "markdown" || name == "body" => Codec::Zstd,
        // Doc 10.6, small enums: the values repeat hard, so the dictionary is
        // most of the column and the codes are a couple of bits a row.
        DataType::Utf8 if is_low_cardinality(name) => Codec::Dict,
        // Doc 10.6 wants FSST here, which is issue 90. Until then these are
        // plain plus zstd, which is what doc 10.10 says the Parquet side
        // already does with them and is the fallback doc 10.10 names if FSST
        // conversion turns out to dominate.
        DataType::Utf8 => Codec::Zstd,
        // Doc 10.6, fixed width integers and timestamps: frame of reference
        // against the chunk minimum, then bit packing.
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => Codec::Frame,
        DataType::FixedSizeBinary(_) => Codec::Raw,
        _ => Codec::Zstd,
    }
}

fn is_incompressible(name: &str) -> bool {
    matches!(
        leaf_of(name),
        "url_key"
            | "pld_id"
            | "body_digest"
            | "chunk_root"
            | "extract_digest"
            | "headers_digest"
            | "minhash"
            | "simhash"
            | "fetcher_id"
            | "signature"
            | "lease_id"
            | "nonce"
            | "item"
    )
}

fn is_low_cardinality(name: &str) -> bool {
    matches!(
        leaf_of(name),
        "host" | "content_type" | "lang" | "method" | "http_version" | "key" | "content_usage"
    )
}

/// The last path element, so that `links.href` and `href` get the same answer.
fn leaf_of(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

#[cfg(test)]
mod tests {
    use super::{StreamKind, codec_for};
    use crate::codec::Codec;
    use arrow::datatypes::DataType;

    #[test]
    fn every_stream_has_a_schema_and_a_distinct_id() {
        let kinds = [StreamKind::Pages, StreamKind::Receipts, StreamKind::Robots];
        for kind in kinds {
            assert!(!kind.arrow().fields().is_empty());
            assert_eq!(StreamKind::from_code(kind as u16).expect("known"), kind);
        }
        let ids: Vec<u32> = kinds.iter().map(|k| k.schema_id()).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len());
    }

    #[test]
    fn an_unknown_stream_code_is_refused_rather_than_guessed() {
        assert!(StreamKind::from_code(9).is_err());
    }

    #[test]
    fn the_random_columns_are_not_compressed() {
        // Doc 10.6: any attempt to compress these costs CPU to produce a
        // slightly larger output, and the writer skips them explicitly rather
        // than discovering it per chunk.
        for name in ["minhash", "body_digest", "url_key", "simhash", "signature"] {
            assert_eq!(codec_for(name, &DataType::UInt64), Codec::Raw, "{name}");
        }
    }

    #[test]
    fn the_one_column_worth_a_general_compressor_gets_one() {
        assert_eq!(codec_for("markdown", &DataType::Utf8), Codec::Zstd);
        assert_eq!(codec_for("host", &DataType::Utf8), Codec::Dict);
        assert_eq!(codec_for("status", &DataType::UInt16), Codec::Frame);
    }
}
