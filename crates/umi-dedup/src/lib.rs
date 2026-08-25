//! Content digests, MinHash, simhash and LSH banding.
//!
//! Specified in `docs/spec/11-extraction-and-dedup.md` sections 11.7 and 11.8,
//! and in `docs/spec/04-fetch-protocol.md` sections 4.5 and 4.6. Four things
//! live here and they are all pure functions of bytes.
//!
//! - [`text_digest`], the blake3 of the normalised plain text, which is doc
//!   11.7's exact duplicate key.
//! - [`Sketch`], the 64 value MinHash plus the 64 bit simhash, both computed in
//!   one pass over the same shingles, which is doc 11.8's near duplicate
//!   sketch and doc 04.6's stability comparison.
//! - [`ChunkTree`], the blake3 tree over 16 KiB leaves that lets a coordinator
//!   audit chunk 47 of a 3 MB body without transferring the other 2.9 MB.
//! - [`bands`], the 8 by 8 LSH banding that turns a sketch into eight bucket
//!   keys for the batch clustering job in doc 11.8.
//!
//! # Nothing here reads a clock or touches a disk
//!
//! Doc 11.1 asks that the same input bytes plus the same version produce byte
//! identical output on every machine, and doc 16's gate 1.2 measures it. Every
//! constant in here is derived from a fixed seed at compile time, the shingle
//! order is the order of the text, and there is no floating point anywhere on
//! the path that produces a stored value. [`Sketch::jaccard`] returns an `f32`
//! but it is a comparison and never lands in a column.
//!
//! # Cost
//!
//! Doc 11.9 budgets 0.8 to 1.5 ms for shingling plus the 64 value MinHash plus
//! the simhash on a 150 KB document, and that budget is second only to the
//! HTML parse. `benches/sketch.rs` is the measurement, and the shape of the
//! code here follows directly from the number: one xxh3 per shingle and then
//! 64 multiply, shift and min operations against that one hash, rather than 64
//! independent hashes of the shingle bytes. Hashing the bytes 64 times is the
//! textbook construction and it is roughly forty times too slow.

pub mod chunk;
pub mod shingle;
pub mod sketch;

#[cfg(test)]
mod tests;

pub use chunk::{CHUNK_BYTES, ChunkTree, verify_chunk};
pub use shingle::{SHINGLE, Shingles};
pub use sketch::{BAND_ROWS, BANDS, PERMUTATIONS, SKETCH_BYTES, Sketch, bands};

/// Doc 11.7's exact duplicate key: blake3-256 over the normalised plain text.
///
/// Over the text from doc 11.3 and not over the raw HTML, because two byte
/// identical articles differ in their ad slots and CSRF tokens, and not over
/// the markdown, because heading levels shift between templates carrying the
/// same prose.
///
/// This is the same value `umi_extract::Extracted::text_digest` returns. It is
/// repeated here so that a consumer holding published text can recompute the
/// key without pulling in the extractor.
#[must_use]
pub fn text_digest(text: &str) -> [u8; 32] {
    *blake3::hash(text.as_bytes()).as_bytes()
}

/// Doc 08.3's truncated content hash, the first 8 bytes of [`text_digest`].
///
/// The ledger stores 64 bits rather than 256 because it holds one row per
/// known URL and 24 extra bytes across 100 billion rows is 2.4 TB to answer a
/// question that only ever asks "did this change". A 64 bit collision between
/// two versions of one URL means one missed change on one page.
#[must_use]
pub fn content_hash(text: &str) -> [u8; 8] {
    let full = text_digest(text);
    let mut out = [0u8; 8];
    out.copy_from_slice(&full[..8]);
    out
}

/// Doc 04.6's `text_len_bucket`: the log2 bucket of the text length.
///
/// Two fetches of one URL agree only when their buckets are within one, which
/// is a deliberately coarse test. A page that grew by a paragraph is the same
/// page and a page that shrank from 40 KB to 400 bytes is a soft error page
/// wearing the same URL.
#[must_use]
pub fn len_bucket(text_bytes: usize) -> u32 {
    // `ilog2` panics on zero, and an empty extraction is a real outcome on a
    // page that is entirely images, so zero gets its own bucket rather than a
    // guard at every call site.
    match u32::try_from(text_bytes).unwrap_or(u32::MAX) {
        0 => 0,
        n => n.ilog2() + 1,
    }
}

/// Doc 04.6's `link_set`: blake3 over the sorted outlink set.
///
/// Sorted and deduplicated before hashing, because two fetchers serialising
/// the same page can emit the same links in a different order when the
/// document has parallel navigation blocks, and an order sensitive digest
/// would call that a disagreement. Compared more strictly than the text in
/// doc 04.6, at 0.95 Jaccard, because links steer the frontier and are the
/// highest value thing for a hostile fetcher to poison.
#[must_use]
pub fn link_set_digest(links: &[&str]) -> [u8; 32] {
    let mut sorted: Vec<&str> = links.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut hasher = blake3::Hasher::new();
    for link in sorted {
        hasher.update(link.as_bytes());
        // A separator, so that `["ab", "c"]` and `["a", "bc"]` are not the
        // same digest. Zero rather than a printable byte because a URL cannot
        // contain one after canonicalisation.
        hasher.update(&[0]);
    }
    *hasher.finalize().as_bytes()
}

/// Everything doc 11 asks a fetcher to compute about one page's content.
///
/// Built in one call because every field is a function of the same normalised
/// text and the sketch already walks it once. A caller that builds these
/// separately walks a 150 KB string three times for no reason.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Content {
    /// Doc 11.7's exact duplicate key.
    pub digest: [u8; 32],
    /// Doc 11.8's near duplicate sketch.
    pub sketch: Sketch,
    /// How many bytes of normalised text there were, which is doc 11.6's first
    /// quality signal and doc 04.6's length bucket before bucketing.
    pub text_bytes: u32,
}

impl Content {
    /// Digest and sketch the normalised text from doc 11.3.
    #[must_use]
    pub fn of(text: &str) -> Self {
        Self {
            digest: text_digest(text),
            sketch: Sketch::of(text),
            text_bytes: u32::try_from(text.len()).unwrap_or(u32::MAX),
        }
    }

    /// Doc 08.3's truncated content hash for the ledger row.
    #[must_use]
    pub fn content_hash(&self) -> [u8; 8] {
        let mut out = [0u8; 8];
        out.copy_from_slice(&self.digest[..8]);
        out
    }

    /// Doc 04.6's length bucket.
    #[must_use]
    pub fn len_bucket(&self) -> u32 {
        len_bucket(self.text_bytes as usize)
    }
}
