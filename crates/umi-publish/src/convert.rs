//! `.umi` to Parquet, from `docs/spec/12-publishing.md` section 12.3.
//!
//! One segment becomes exactly one Parquet file and one shoal becomes exactly
//! one row group. Doc 12.3 calls that mapping deliberate and it is worth
//! restating why, because it is the thing that makes every other number in doc
//! 12 work. Conversion is streaming, so a 128 MB segment never needs 128 MB of
//! Arrow resident at once. A corrupted segment damages exactly one published
//! file rather than a range of them. And the row groups come out at doc 10.3's
//! shoal size of about 32 MiB, which is inside the range every query engine is
//! happy with, without anyone having to tune a second knob to match the first.
//!
//! The budget is doc 12.2's 30 seconds a segment at 0.4 of a core. The umi-file
//! bench says a full decode to Arrow is about 18 seconds per 128 MB, so the
//! Parquet write has 12 seconds, and that is the number to watch when this gets
//! measured for real.

use std::fs::File;
use std::path::Path;

use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use sha2::{Digest as _, Sha256};
use umi_file::{Segment, StreamKind};

use crate::{Error, Result};

/// Doc 12.3's page size. One MiB pages against 32 MiB row groups is 32 pages a
/// column chunk, which is enough for the page index to be worth reading and few
/// enough that the index itself stays small.
pub const PAGE_BYTES: usize = 1024 * 1024;

/// Doc 12.3 fixes zstd level 3, the same level doc 10.6 uses inside the
/// segment. Two different levels either side of the conversion would mean a
/// file whose size nobody could predict from the segment's.
pub const ZSTD_LEVEL: i32 = 3;

/// The columns doc 12.3 dictionary encodes.
///
/// Low cardinality columns that a consumer groups by. Everything else is left
/// alone: a dictionary on a column of 20000 distinct URLs is a dictionary the
/// size of the column plus an index, and parquet-rs falls back at write time
/// anyway, so turning it off by name saves the wasted attempt rather than the
/// wasted bytes.
pub const DICTIONARY_COLUMNS: [&str; 7] = [
    "host",
    "content_type",
    "lang",
    "outcome",
    "status",
    "tier_used",
    "verification",
];

/// The columns doc 12.3 puts a bloom filter on, and nowhere else.
///
/// These are the two point lookups anyone actually does, "is this URL in the
/// corpus" and "is this content in the corpus", and a filter turns each into
/// one row group read instead of a full scan. A filter on a column nobody
/// filters by is bytes we pay for and nobody uses.
///
/// Doc 12.3 names the second one `text_digest`, which is not a column in doc
/// 10.5's schema. The column that answers "is this content in the corpus" is
/// `body_digest`, so that is what gets the filter, and doc 12.3 needs the edit.
pub const BLOOM_COLUMNS: [&str; 2] = ["url_key", "body_digest"];

/// What one conversion produced.
///
/// The two digests are doc 12.5's, computed over the finished file rather than
/// over the Arrow that went into it. That is the one place doc 17.4's rule
/// about digesting logical values gives way, and deliberately: this digest's
/// only job is to prove that one specific file arrived intact.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Converted {
    /// How many rows went in and came out.
    pub rows: u64,
    /// How many row groups, which is how many shoals the segment had.
    pub row_groups: usize,
    /// The size of the Parquet file on disk.
    pub bytes: u64,
    /// blake3 of the file, which is what umi checks everywhere else.
    pub blake3: [u8; 32],
    /// sha256 of the file, which is what everyone else can check.
    pub sha256: [u8; 32],
    /// The earliest `fetched_at_ms` in the segment, for the manifest entry.
    pub first_ms: u64,
    /// The latest, likewise.
    pub last_ms: u64,
}

/// Convert one sealed segment into one Parquet file.
///
/// Every chunk checksum is verified on the way through, which is doc 12.2's
/// step 1 folded into step 2 rather than done as a separate pass. Doing it
/// separately would read the whole file twice for no benefit: the bytes are
/// already in memory when the shoal is decoded, and a shoal that fails here
/// stops the conversion before anything is published.
///
/// # Errors
///
/// Whatever the segment reader reports, including a failed checksum, and
/// whatever Parquet or the filesystem reports. A partial output file is left
/// behind on failure and the caller deletes it; there is no attempt to clean up
/// here, because a converter that removed evidence would make a corruption bug
/// much harder to look at.
pub fn convert(segment: &Segment, out: &Path) -> Result<Converted> {
    let stream = segment.header().stream;
    let file = File::create(out)?;
    let mut writer =
        ArrowWriter::try_new(file, segment.schema().clone(), Some(properties(stream)))?;

    let mut rows = 0u64;
    let mut row_groups = 0usize;
    for i in 0..segment.shoals() {
        let shoal = segment.shoal(i)?;
        shoal.verify()?;
        let batch = shoal.to_arrow(&[])?;
        rows += batch.num_rows() as u64;
        writer.write(&batch)?;
        // One shoal, one row group. `flush` closes the current group, and
        // without it parquet-rs would pack shoals together up to its own row
        // group limit and the one to one mapping doc 12.3 asks for would
        // quietly stop holding.
        writer.flush()?;
        row_groups += 1;
    }
    writer.close()?;

    let (blake3, sha256, bytes) = digest_file(out)?;
    let stats = segment.stats();
    Ok(Converted {
        rows,
        row_groups,
        bytes,
        blake3,
        sha256,
        first_ms: stats.first_ms,
        last_ms: stats.last_ms,
    })
}

/// Doc 12.3's writer settings, all of them, in one place.
///
/// Fixed rather than configurable. A published corpus whose encoding depends on
/// which host converted it is one where a consumer cannot predict what a file
/// costs to read, and there is no operational reason anyone would want to vary
/// these per segment.
#[must_use]
pub fn properties(stream: StreamKind) -> WriterProperties {
    let columns: Vec<String> = stream
        .arrow()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    let mut props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(ZSTD_LEVEL).unwrap_or_default(),
        ))
        .set_data_page_size_limit(PAGE_BYTES)
        // Doc 12.3 wants page level statistics, which is also what makes
        // parquet-rs write the column index. A consumer filtering on
        // `fetched_at_ms` should read three row groups and not the whole file,
        // and that only works if the min and max are there.
        .set_statistics_enabled(EnabledStatistics::Page)
        // One shoal is one row group and `flush` is what ends it, so this only
        // has to be large enough never to fire first. Doc 10.3 caps a shoal at
        // 16384 rows.
        .set_max_row_group_size(1 << 20)
        // Off by default and on by name below. Doc 12.3 lists seven columns,
        // and leaving it on everywhere would mean parquet-rs building a
        // dictionary for `url` and `markdown` and then discarding it.
        .set_dictionary_enabled(false);

    for name in DICTIONARY_COLUMNS {
        if columns.iter().any(|c| c == name) {
            props = props.set_column_dictionary_enabled(ColumnPath::from(name), true);
        }
    }
    for name in BLOOM_COLUMNS {
        if columns.iter().any(|c| c == name) {
            props = props.set_column_bloom_filter_enabled(ColumnPath::from(name), true);
        }
    }
    // Doc 12.3 declares no sorting columns. Doc 10's reorder window groups rows
    // by host, which makes the host statistics useful, but saying a file is
    // sorted when the window only makes it locally clustered would be a lie a
    // query engine acts on.
    props
        .set_sorting_columns(None)
        .set_encoding(Encoding::PLAIN)
        .build()
}

/// blake3, sha256 and length of a finished file, in one pass over it.
fn digest_file(path: &Path) -> Result<([u8; 32], [u8; 32], u64)> {
    use std::io::Read as _;

    let mut file = File::open(path)?;
    let mut blake = blake3::Hasher::new();
    let mut sha = Sha256::new();
    // A megabyte at a time. The file is 128 MB and reading it whole would put
    // that much in the publisher's heap alongside whatever the next segment is
    // already using.
    let mut buf = vec![0u8; PAGE_BYTES];
    let mut total = 0u64;
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        blake.update(&buf[..read]);
        sha.update(&buf[..read]);
        total += read as u64;
    }
    let sha: [u8; 32] = sha.finalize().into();
    Ok((*blake.finalize().as_bytes(), sha, total))
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(err: parquet::errors::ParquetError) -> Self {
        Self::Parquet(err.to_string())
    }
}
