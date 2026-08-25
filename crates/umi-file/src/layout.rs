//! The byte layouts from doc 10.4, in one place.
//!
//! Everything on disk is little endian and every multi byte field is written
//! and read through the helpers at the bottom of this file rather than through
//! a cast, because doc 11.1 wants the same bytes on every machine and a cast
//! would quietly give a different answer on a big endian one. Nobody is going
//! to run this on a big endian machine, but the rule is cheap to keep and the
//! alternative is a rule with an exception in it.
//!
//! Sizes are fixed rather than derived. The header is 4 KiB whatever is in it,
//! a commit record is 32 bytes, and a footer trailer is 24. Knowing those
//! without parsing anything is what makes the recovery scan in doc 10.7 a scan
//! rather than a guess.

use crate::{Error, Result};

/// The magic at both ends of the file, from doc 10.4.
///
/// At the start it identifies the file to `file(1)` and to a human. At the end
/// it is the seal marker: a file whose last four bytes are not this was not
/// closed cleanly and goes down the recovery path.
pub const MAGIC: [u8; 4] = *b"UMI1";

/// The tag on a commit record.
pub const COMMIT_TAG: [u8; 4] = *b"SHOL";

/// The header is fixed at 4 KiB, written once at create and never rewritten.
pub const HEADER_LEN: usize = 4096;

/// A commit record is 32 bytes, which is what makes the recovery scan able to
/// step through a file it has not parsed.
pub const COMMIT_LEN: usize = 32;

/// Footer length, footer digest and the trailing magic.
pub const TRAILER_LEN: usize = 4 + 16 + 4;

/// Column chunks are aligned to this, so that fixed width buffers can be handed
/// to Arrow as aligned slices. The padding costs a few hundred bytes per shoal
/// and doc 10.4 says not to optimise it away.
pub const ALIGN: usize = 64;

/// The format version in the header.
///
/// Doc 10.11: this exists so a reader can refuse an unknown file, not so old
/// files keep working. No segment lives long enough for a migration.
pub const FORMAT_VERSION: u16 = 1;

/// blake3 truncated to 128 bits, which is the checksum the whole format uses.
///
/// 128 bits rather than 256 because these guard against a writer bug producing
/// plausible garbage and against bit rot, not against an adversary, and the
/// directory carries one of these per column.
#[must_use]
pub fn digest128(bytes: &[u8]) -> [u8; 16] {
    let full = blake3::hash(bytes);
    let mut out = [0u8; 16];
    out.copy_from_slice(&full.as_bytes()[..16]);
    out
}

/// Round up to the next [`ALIGN`] boundary.
#[must_use]
pub const fn align_up(n: usize) -> usize {
    n.div_ceil(ALIGN) * ALIGN
}

/// What a segment holds, which the reader needs before it reads anything else.
///
/// Doc 10.4 pins the canonicalisation and extractor versions here rather than
/// on every row, which is what makes a segment self describing without paying
/// for it per page.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Header {
    /// Which of doc 10.3's three streams this is.
    pub stream: crate::StreamKind,
    /// The schema this file was written against. A reader that does not
    /// recognise it refuses to open the file rather than guessing.
    pub schema_id: u32,
    /// Identifies the segment for the whole of its life, including in the
    /// manifest doc 12 uploads.
    pub segment_id: [u8; 16],
    /// Which coordinator wrote it.
    pub coordinator: [u8; 32],
    /// When the file was created, in milliseconds since the Unix epoch.
    pub created_ms: u64,
    /// The canonicalisation version from doc 11.2.
    pub canon_version: u32,
    /// The extractor version.
    pub extractor_version: u32,
    /// The crawl profile from doc 13.
    pub crawl_profile: u32,
    /// The row cap the writer used, so a reader can tell a small shoal from a
    /// differently configured one.
    pub shoal_rows: u32,
    /// The encoded byte cap the writer used.
    pub shoal_bytes: u32,
}

impl Header {
    /// Lay the header out into its fixed 4 KiB.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![0u8; HEADER_LEN];
        out[0..4].copy_from_slice(&MAGIC);
        out[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        out[6..8].copy_from_slice(&(self.stream as u16).to_le_bytes());
        out[8..12].copy_from_slice(&self.schema_id.to_le_bytes());
        out[12..28].copy_from_slice(&self.segment_id);
        out[28..60].copy_from_slice(&self.coordinator);
        out[60..68].copy_from_slice(&self.created_ms.to_le_bytes());
        out[68..72].copy_from_slice(&self.canon_version.to_le_bytes());
        out[72..76].copy_from_slice(&self.extractor_version.to_le_bytes());
        out[76..80].copy_from_slice(&self.crawl_profile.to_le_bytes());
        out[80..84].copy_from_slice(&self.shoal_rows.to_le_bytes());
        out[84..88].copy_from_slice(&self.shoal_bytes.to_le_bytes());
        // Everything from here to the checksum is reserved and stays zero, so
        // that adding a field later does not move any field already in use.
        let sum = digest128(&out[..HEADER_LEN - 16]);
        out[HEADER_LEN - 16..].copy_from_slice(&sum);
        out
    }

    /// Read a header back, checking the magic, the version and the checksum in
    /// that order so the error says the most useful thing it can.
    ///
    /// # Errors
    ///
    /// [`Error::NotUmi`] when the magic is wrong, which is the case for a file
    /// that is not one of ours at all. [`Error::Version`] for a format we do
    /// not know. [`Error::Corrupt`] when the checksum fails.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(Error::NotUmi);
        }
        if bytes[0..4] != MAGIC {
            return Err(Error::NotUmi);
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != FORMAT_VERSION {
            return Err(Error::Version(version));
        }
        let sum = digest128(&bytes[..HEADER_LEN - 16]);
        if sum != bytes[HEADER_LEN - 16..HEADER_LEN] {
            return Err(Error::Corrupt("header checksum"));
        }
        let stream = crate::StreamKind::from_code(u16::from_le_bytes([bytes[6], bytes[7]]))?;
        let mut segment_id = [0u8; 16];
        segment_id.copy_from_slice(&bytes[12..28]);
        let mut coordinator = [0u8; 32];
        coordinator.copy_from_slice(&bytes[28..60]);
        Ok(Self {
            stream,
            schema_id: get_u32(bytes, 8),
            segment_id,
            coordinator,
            created_ms: get_u64(bytes, 60),
            canon_version: get_u32(bytes, 68),
            extractor_version: get_u32(bytes, 72),
            crawl_profile: get_u32(bytes, 76),
            shoal_rows: get_u32(bytes, 80),
            shoal_bytes: get_u32(bytes, 84),
        })
    }
}

/// The 32 byte record doc 10.7 writes after a shoal's bytes are durable.
///
/// Offsets and lengths are `u32` because a segment seals at 128 MB and the
/// writer refuses to go past 4 GiB. That is what makes the record fit in 32
/// bytes, and 32 bytes is what keeps two syncs a shoal affordable on server2's
/// rotational disks.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Commit {
    /// Which shoal this is, counting from zero. A scan that meets these out of
    /// order has found garbage that happens to checksum, which is a bug.
    pub index: u32,
    /// Where the shoal's first column chunk starts.
    pub offset: u32,
    /// How long the shoal is, from `offset` to the end of its directory.
    pub len: u32,
    /// blake3-128 over the shoal directory.
    pub dir_digest: [u8; 16],
}

impl Commit {
    /// Lay the record out.
    #[must_use]
    pub fn encode(&self) -> [u8; COMMIT_LEN] {
        let mut out = [0u8; COMMIT_LEN];
        out[0..4].copy_from_slice(&COMMIT_TAG);
        out[4..8].copy_from_slice(&self.index.to_le_bytes());
        out[8..12].copy_from_slice(&self.offset.to_le_bytes());
        out[12..16].copy_from_slice(&self.len.to_le_bytes());
        out[16..32].copy_from_slice(&self.dir_digest);
        out
    }

    /// Read a record back, or `None` if these bytes are not one.
    ///
    /// Returning an option rather than an error is the point: the recovery scan
    /// in doc 10.7 stops at the first thing that is not a commit record and
    /// treats everything before it as intact, so "not a record" is the ordinary
    /// end of the scan and not a failure.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < COMMIT_LEN || bytes[0..4] != COMMIT_TAG {
            return None;
        }
        let mut dir_digest = [0u8; 16];
        dir_digest.copy_from_slice(&bytes[16..32]);
        Some(Self {
            index: get_u32(bytes, 4),
            offset: get_u32(bytes, 8),
            len: get_u32(bytes, 12),
            dir_digest,
        })
    }
}

/// One column's entry in a shoal directory.
///
/// Doc 10.4 puts the directory after the column data, because the writer does
/// not know a chunk's encoded length until it has encoded it and writing the
/// directory first would mean a seek back or a two pass encode.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ChunkEntry {
    /// The leaf column name, which for a nested column is a path like
    /// `links.href`.
    pub name: String,
    /// Where the chunk starts, from the start of the file.
    pub offset: u32,
    /// How many bytes it is.
    pub len: u32,
    /// Which encoding from doc 10.6, so a reader that meets one it does not
    /// know can fail loudly rather than guess.
    pub codec: u16,
    /// How many of the values are null.
    pub nulls: u32,
    /// How many values there are, which for a list offsets column is one more
    /// than the number of lists.
    pub values: u32,
    /// blake3-128 over the chunk bytes.
    pub digest: [u8; 16],
}

impl ChunkEntry {
    fn encode_into(&self, out: &mut Vec<u8>) {
        let name = self.name.as_bytes();
        // A name longer than this is a bug in the schema rather than a thing
        // to support, and the cast has to be infallible somewhere.
        let name_len = u16::try_from(name.len()).unwrap_or(u16::MAX);
        out.extend_from_slice(&name_len.to_le_bytes());
        out.extend_from_slice(&name[..name_len as usize]);
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.extend_from_slice(&self.len.to_le_bytes());
        out.extend_from_slice(&self.codec.to_le_bytes());
        out.extend_from_slice(&self.nulls.to_le_bytes());
        out.extend_from_slice(&self.values.to_le_bytes());
        out.extend_from_slice(&self.digest);
    }
}

/// A shoal's directory: the row count and one entry per column.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Directory {
    /// How many rows are in the shoal.
    pub rows: u32,
    /// One per leaf column, in schema order.
    pub chunks: Vec<ChunkEntry>,
}

impl Directory {
    /// Lay the directory out.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + self.chunks.len() * 64);
        out.extend_from_slice(&self.rows.to_le_bytes());
        let count = u32::try_from(self.chunks.len()).unwrap_or(u32::MAX);
        out.extend_from_slice(&count.to_le_bytes());
        for chunk in &self.chunks {
            chunk.encode_into(&mut out);
        }
        out
    }

    /// Read a directory back.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] when it runs off the end, which after a crash means
    /// the scan has reached the shoal that was in flight.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut take = Take::new(bytes);
        let rows = take.u32()?;
        let count = take.u32()?;
        let mut chunks = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let name_len = take.u16()? as usize;
            let name = String::from_utf8(take.bytes(name_len)?.to_vec())
                .map_err(|_| Error::Corrupt("column name is not utf8"))?;
            chunks.push(ChunkEntry {
                name,
                offset: take.u32()?,
                len: take.u32()?,
                codec: take.u16()?,
                nulls: take.u32()?,
                values: take.u32()?,
                digest: take.array16()?,
            });
        }
        Ok(Self { rows, chunks })
    }
}

/// What a whole segment adds up to, carried in the footer.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct SegmentStats {
    /// Rows across every shoal.
    pub rows: u64,
    /// How many bytes the encoded column chunks take.
    pub encoded_bytes: u64,
    /// How many bytes those chunks held before encoding, so the compression
    /// ratio doc 10.2 budgets for can be read off a real file rather than
    /// estimated.
    pub logical_bytes: u64,
    /// The earliest and latest `fetched_at_ms` in the segment, or zero on a
    /// stream that has no such column.
    pub first_ms: u64,
    /// See [`first_ms`](Self::first_ms).
    pub last_ms: u64,
}

/// Where one shoal is, from the footer's point of view.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ShoalIndex {
    /// Where the shoal's first column chunk starts.
    pub offset: u32,
    /// How long the shoal is, from `offset` to the end of its directory.
    pub len: u32,
    /// How many rows it holds.
    pub rows: u32,
}

/// The footer, written last.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Footer {
    /// One per shoal, in order.
    pub shoals: Vec<ShoalIndex>,
    /// The leaf column names, in schema order, so a reader can check the file
    /// against the schema it thinks it has before it decodes anything.
    pub columns: Vec<String>,
    /// The segment totals.
    pub stats: SegmentStats,
}

impl Footer {
    /// Lay the footer out, without the trailer.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64 + self.shoals.len() * 12);
        out.extend_from_slice(
            &u32::try_from(self.shoals.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        for shoal in &self.shoals {
            out.extend_from_slice(&shoal.offset.to_le_bytes());
            out.extend_from_slice(&shoal.len.to_le_bytes());
            out.extend_from_slice(&shoal.rows.to_le_bytes());
        }
        out.extend_from_slice(
            &u32::try_from(self.columns.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        for name in &self.columns {
            let bytes = name.as_bytes();
            let len = u16::try_from(bytes.len()).unwrap_or(u16::MAX);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&bytes[..len as usize]);
        }
        out.extend_from_slice(&self.stats.rows.to_le_bytes());
        out.extend_from_slice(&self.stats.encoded_bytes.to_le_bytes());
        out.extend_from_slice(&self.stats.logical_bytes.to_le_bytes());
        out.extend_from_slice(&self.stats.first_ms.to_le_bytes());
        out.extend_from_slice(&self.stats.last_ms.to_le_bytes());
        out
    }

    /// Read a footer back.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] when it runs off the end.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut take = Take::new(bytes);
        let shoal_count = take.u32()?;
        let mut shoals = Vec::with_capacity(shoal_count as usize);
        for _ in 0..shoal_count {
            shoals.push(ShoalIndex {
                offset: take.u32()?,
                len: take.u32()?,
                rows: take.u32()?,
            });
        }
        let column_count = take.u32()?;
        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            let len = take.u16()? as usize;
            columns.push(
                String::from_utf8(take.bytes(len)?.to_vec())
                    .map_err(|_| Error::Corrupt("column name is not utf8"))?,
            );
        }
        Ok(Self {
            shoals,
            columns,
            stats: SegmentStats {
                rows: take.u64()?,
                encoded_bytes: take.u64()?,
                logical_bytes: take.u64()?,
                first_ms: take.u64()?,
                last_ms: take.u64()?,
            },
        })
    }
}

/// A bounds checked walk over a byte slice.
///
/// Everything that parses reaches for this rather than indexing, because the
/// input is a file that may have been truncated by a SIGKILL and a panic on a
/// slice index would turn a recoverable segment into a crashed process.
pub(crate) struct Take<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> Take<'a> {
    pub(crate) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    pub(crate) fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .at
            .checked_add(n)
            .ok_or(Error::Corrupt("length overflow"))?;
        let out = self
            .bytes
            .get(self.at..end)
            .ok_or(Error::Corrupt("short read"))?;
        self.at = end;
        Ok(out)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    pub(crate) fn u16(&mut self) -> Result<u16> {
        let b = self.bytes(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        let b = self.bytes(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        let b = self.bytes(8)?;
        let mut out = [0u8; 8];
        out.copy_from_slice(b);
        Ok(u64::from_le_bytes(out))
    }

    pub(crate) fn array16(&mut self) -> Result<[u8; 16]> {
        let b = self.bytes(16)?;
        let mut out = [0u8; 16];
        out.copy_from_slice(b);
        Ok(out)
    }

    pub(crate) const fn rest_from(&self) -> usize {
        self.at
    }
}

fn get_u32(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

fn get_u64(bytes: &[u8], at: usize) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(out)
}
