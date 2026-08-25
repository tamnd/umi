//! The `.umi` columnar container, from `docs/spec/10-umi-file-format.md`.
//!
//! It is tempting to read "single file columnar format inspired by DuckDB and
//! SQLite" as a request for a small analytical database. Doc 10.1 says that
//! would be a mistake, and the reason is the lifetime: a segment fills for
//! about 90 seconds, publishes for under 10 minutes, and is then deleted,
//! because doc 01 has 342 GB of free disk against 390 GB of daily output.
//! Nothing outside umi ever reads one of these files. Consumers read the
//! Parquet in `open-index/*`.
//!
//! So what this is, in doc 10.1's words, is a write optimised append only
//! columnar container with a strong crash story and a cheap path to Parquet.
//! Much closer to a WAL segment that happens to be columnar than to DuckDB. The
//! SQLite influence is the crash discipline of checksums plus a commit record
//! written last, and the DuckDB influence is the encoding cascade of cheap
//! lightweight codecs under one general purpose compressor. Neither is the
//! query engine.
//!
//! What that buys and what it costs is worth stating, because it explains every
//! decision below. Crash safety matters, because the process will be killed and
//! losing an hour of fetching is losing real money. Write cost matters, because
//! there are 2 vCPU per host and extraction already wants most of them.
//! Compactness matters, because disk is the binding constraint. Query
//! performance does not matter, because nothing queries it. Random access does
//! not matter, because the only reader is a sequential converter. Long term
//! stability of the layout does not matter, because no file survives an
//! upgrade.
//!
//! # Writing
//!
//! ```no_run
//! use umi_file::{Create, SegmentWriter, StreamKind, WriterConfig};
//!
//! # fn main() -> umi_file::Result<()> {
//! # let batch: arrow::array::RecordBatch = unimplemented!();
//! let mut writer = SegmentWriter::create(
//!     std::path::Path::new("/var/lib/umi/segments/01J.umi"),
//!     Create {
//!         stream: StreamKind::Pages,
//!         segment_id: [0u8; 16],
//!         coordinator: [0u8; 32],
//!         created_ms: 1_760_000_000_000,
//!         canon_version: 1,
//!         extractor_version: 1,
//!         crawl_profile: 0,
//!     },
//!     WriterConfig::default(),
//! )?;
//! writer.push(&batch)?;
//! let stats = writer.seal()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Reading
//!
//! ```no_run
//! use umi_file::Segment;
//!
//! # fn main() -> umi_file::Result<()> {
//! let segment = Segment::open(std::path::Path::new("/var/lib/umi/segments/01J.umi"))?;
//! for i in 0..segment.shoals() {
//!     let shoal = segment.shoal(i)?;
//!     shoal.verify()?;
//!     let batch = shoal.to_arrow(&["url", "status"])?;
//!     println!("{} rows", batch.num_rows());
//! }
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

pub mod codec;
pub mod column;
pub mod layout;
mod read;
pub mod sample;
mod schema;
mod write;

#[cfg(test)]
mod tests;

pub use codec::{Codec, Compression};
pub use column::{Column, Leaf, Width};
pub use layout::{ChunkEntry, Directory, Footer, Header, SegmentStats, ShoalIndex};
pub use read::{ColumnChunk, RecoveryReport, Segment, ShoalReader};
pub use schema::{StreamKind, codec_for};
pub use write::{Create, SegmentWriter, WriterConfig};

/// What can go wrong reading or writing a segment.
///
/// Every variant here is either "this is not our file" or "this file is torn",
/// and the two are kept apart on purpose. A caller that meets
/// [`NotSealed`](Error::NotSealed) has found a segment whose writer died, which
/// is an ordinary event with a defined response. A caller that meets
/// [`NotUmi`](Error::NotUmi) has found something else entirely.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The filesystem said no.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// The file does not start with `UMI1`.
    #[error("not a .umi file")]
    NotUmi,

    /// A format version this build does not have. Doc 10.11: the version exists
    /// so a reader can refuse an unknown file, not so old files keep working.
    #[error("format version {0} is not one this build knows")]
    Version(u16),

    /// A stream kind this build does not have.
    #[error("stream kind {0} is not one this build knows")]
    UnknownStream(u16),

    /// An encoding this build does not have. Doc 10.6: fail loudly, because a
    /// segment a reader cannot decode is a bug to fix within the hour.
    #[error("encoding {0} is not one this build knows")]
    UnknownCodec(u16),

    /// The file has no valid footer, so the writer did not finish. Not a
    /// failure on its own: it is what sends a caller to
    /// [`Segment::open_recover`].
    #[error("the segment was not sealed")]
    NotSealed,

    /// The file's columns do not match the schema its header claims, or a batch
    /// handed to the writer is not the segment's stream.
    #[error("the batch does not match the segment schema")]
    Schema,

    /// A structure did not parse, naming what it was.
    #[error("corrupt: {0}")]
    Corrupt(&'static str),

    /// A chunk did not match the checksum in its directory.
    #[error("checksum failed on column {0}")]
    ChecksumFailed(String),

    /// There is no shoal at that index.
    #[error("no shoal {0}")]
    NoSuchShoal(usize),

    /// There is no such column in the schema or in the shoal.
    #[error("no column {0}")]
    NoSuchColumn(String),

    /// An Arrow type none of the three schemas use, which means a schema change
    /// went in without this crate noticing.
    #[error("unsupported: {0}")]
    Unsupported(&'static str),

    /// A segment went past 4 GiB, which doc 10.3 caps at 128 MB. The offsets in
    /// a commit record are `u32`, which is what makes the record fit in 32
    /// bytes.
    #[error("the segment is too large")]
    TooLarge,

    /// The path is already there. A writer that truncated an existing segment
    /// would be a writer that can lose a published file.
    #[error("the segment already exists")]
    Exists,
}

/// The result type this crate returns.
pub type Result<T> = std::result::Result<T, Error>;
