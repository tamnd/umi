//! The reader, and doc 10.7's recovery scan.
//!
//! The split between [`Segment::open`] and [`Segment::open_recover`] is the
//! design rather than an ergonomic detail. Normal opening refuses a torn file
//! instead of guessing, and recovery is something a caller asks for out loud.
//! Doc 12's publisher opens normally and treats a failure as "this segment is
//! not finished yet"; the recovery command opens the other way and says what it
//! found.
//!
//! # Not mmap
//!
//! Doc 10.9 says the reader mmaps the file. It does not, because the workspace
//! denies `unsafe_code` and mapping a file is unsafe by construction: the
//! mapping is invalidated if anything truncates the file underneath it, which
//! the recovery path in this very module does. Instead a shoal is read into one
//! buffer with a positioned read, and the Arrow buffers are built from slices of
//! it, which is the property doc 10.9 actually wanted. The cost is one memcpy
//! of a 32 MiB shoal, about 3 ms, against a conversion budget doc 10.10 puts at
//! 30 seconds for a whole 128 MB segment.

use std::fs::File;
use std::path::Path;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::codec::decode;
use crate::column::{Column, unflatten};
use crate::layout::{
    COMMIT_LEN, Commit, Directory, Footer, HEADER_LEN, Header, SegmentStats, ShoalIndex,
    TRAILER_LEN, digest128,
};
use crate::write::{FRAME_LEN, FRAME_TAG};
use crate::{Error, Result};

/// What a recovery found, so an operator gets a number rather than a shrug.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct RecoveryReport {
    /// Whether the file had a valid footer, in which case nothing was
    /// recovered and the other fields are the file as written.
    pub sealed: bool,
    /// How many shoals are intact and readable.
    pub shoals: u32,
    /// How many rows those shoals hold.
    pub rows: u64,
    /// The byte offset the intact prefix ends at. Doc 10.7: truncate the file
    /// here and continue appending, or seal it as a short segment and publish
    /// it, and both are safe.
    pub good_bytes: u64,
    /// How many bytes past that were written but never committed, which is the
    /// shoal that was in flight when the process died.
    pub lost_bytes: u64,
}

/// One `.umi` file, open for reading.
#[derive(Debug)]
pub struct Segment {
    file: File,
    header: Header,
    footer: Footer,
    schema: SchemaRef,
}

impl Segment {
    /// Open a sealed segment.
    ///
    /// # Errors
    ///
    /// [`Error::NotSealed`] when the trailing magic is missing or the footer
    /// digest fails, which after a crash is the expected answer and is what
    /// sends a caller to [`open_recover`](Self::open_recover). [`Error::NotUmi`]
    /// for a file that is not one of ours, [`Error::Version`] for a format we do
    /// not know, and [`Error::Schema`] when the file's columns do not match the
    /// schema its header claims.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        let header = read_header(&file)?;
        let schema = header.stream.arrow();

        if len < (HEADER_LEN + TRAILER_LEN) as u64 {
            return Err(Error::NotSealed);
        }
        let trailer = read_at(&file, len - TRAILER_LEN as u64, TRAILER_LEN)?;
        if trailer[TRAILER_LEN - 4..] != crate::layout::MAGIC {
            return Err(Error::NotSealed);
        }
        let footer_len =
            u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]) as u64;
        let footer_at = len
            .checked_sub(TRAILER_LEN as u64 + footer_len)
            .ok_or(Error::NotSealed)?;
        if footer_at < HEADER_LEN as u64 {
            return Err(Error::NotSealed);
        }
        let bytes = read_at(&file, footer_at, footer_len as usize)?;
        if digest128(&bytes) != trailer[4..20] {
            return Err(Error::NotSealed);
        }
        let footer = Footer::decode(&bytes)?;
        check_columns(&footer, &schema)?;
        Ok(Self {
            file,
            header,
            footer,
            schema,
        })
    }

    /// Open a segment that may be torn, recovering the committed prefix.
    ///
    /// Doc 10.7: scan forward from the header reading commit records, stop at
    /// the first one that fails its checksum, is truncated or points past EOF,
    /// and everything before it is intact and readable.
    ///
    /// A sealed file takes this path too and comes back with
    /// [`RecoveryReport::sealed`] set, so a caller that always recovers is
    /// correct and merely slower.
    ///
    /// # Errors
    ///
    /// [`Error::NotUmi`] and [`Error::Version`] as for [`open`](Self::open). A
    /// file whose header is intact and which has no valid shoals at all is not
    /// an error: it comes back with zero shoals, which is what a crash one
    /// second after create looks like.
    pub fn open_recover(path: &Path) -> Result<(Self, RecoveryReport)> {
        match Self::open(path) {
            Ok(segment) => {
                let report = RecoveryReport {
                    sealed: true,
                    shoals: u32::try_from(segment.footer.shoals.len()).unwrap_or(u32::MAX),
                    rows: segment.footer.stats.rows,
                    good_bytes: segment.file.metadata()?.len(),
                    lost_bytes: 0,
                };
                Ok((segment, report))
            }
            Err(Error::NotSealed) => Self::scan(path),
            Err(other) => Err(other),
        }
    }

    fn scan(path: &Path) -> Result<(Self, RecoveryReport)> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        let header = read_header(&file)?;
        let schema = header.stream.arrow();

        let mut shoals: Vec<ShoalIndex> = Vec::new();
        let mut stats = SegmentStats::default();
        let mut at = HEADER_LEN as u64;
        loop {
            let aligned = crate::layout::align_up(usize::try_from(at).unwrap_or(usize::MAX)) as u64;
            if aligned + FRAME_LEN as u64 > len {
                break;
            }
            let Ok(frame) = read_at(&file, aligned, FRAME_LEN) else {
                break;
            };
            if frame[0..4] != FRAME_TAG {
                break;
            }
            let body_len = u64::from(u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]));
            let rows = u32::from_le_bytes([frame[8], frame[9], frame[10], frame[11]]);
            let dir_offset = u64::from(u32::from_le_bytes([
                frame[12], frame[13], frame[14], frame[15],
            ]));
            let body_at = aligned + FRAME_LEN as u64;
            let commit_at = body_at + body_len;
            if commit_at + COMMIT_LEN as u64 > len || dir_offset > body_len {
                // The shoal that was in flight. Doc 10.7: a torn shoal has no
                // valid commit record and is simply not part of the file.
                break;
            }
            let Ok(record) = read_at(&file, commit_at, COMMIT_LEN) else {
                break;
            };
            let Some(commit) = Commit::decode(&record) else {
                break;
            };
            if commit.index as usize != shoals.len() || u64::from(commit.offset) != body_at {
                break;
            }
            let Ok(dir_bytes) = read_at(
                &file,
                body_at + dir_offset,
                (body_len - dir_offset) as usize,
            ) else {
                break;
            };
            if digest128(&dir_bytes) != commit.dir_digest {
                break;
            }
            shoals.push(ShoalIndex {
                offset: commit.offset,
                len: commit.len,
                rows,
            });
            stats.rows += u64::from(rows);
            stats.encoded_bytes += body_len;
            at = commit_at + COMMIT_LEN as u64;
        }

        let (names, _): (Vec<String>, Vec<_>) =
            crate::column::leaf_names(&schema).into_iter().unzip();
        let footer = Footer {
            shoals,
            columns: names,
            stats,
        };
        let report = RecoveryReport {
            sealed: false,
            shoals: u32::try_from(footer.shoals.len()).unwrap_or(u32::MAX),
            rows: stats.rows,
            good_bytes: at,
            lost_bytes: len.saturating_sub(at),
        };
        Ok((
            Self {
                file,
                header,
                footer,
                schema,
            },
            report,
        ))
    }

    /// What the file says about itself.
    #[must_use]
    pub const fn header(&self) -> &Header {
        &self.header
    }

    /// The segment totals. After a recovery these cover the intact prefix, not
    /// what the writer intended.
    #[must_use]
    pub const fn stats(&self) -> &SegmentStats {
        &self.footer.stats
    }

    /// The Arrow schema the file's stream kind implies.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// How many readable shoals there are.
    #[must_use]
    pub fn shoals(&self) -> usize {
        self.footer.shoals.len()
    }

    /// Read one shoal into memory.
    ///
    /// # Errors
    ///
    /// [`Error::NoSuchShoal`] past the end, and whatever the filesystem reports.
    pub fn shoal(&self, index: usize) -> Result<ShoalReader<'_>> {
        let at = self
            .footer
            .shoals
            .get(index)
            .ok_or(Error::NoSuchShoal(index))?;
        let bytes = read_at(&self.file, u64::from(at.offset), at.len as usize)?;
        // The directory is the tail of the shoal body, and its length is
        // whatever is left after the column chunks. The last chunk's offset plus
        // its length would give the same answer, but only after parsing the
        // directory, so the frame carries the split.
        let frame = read_at(
            &self.file,
            u64::from(at.offset) - FRAME_LEN as u64,
            FRAME_LEN,
        )?;
        let dir_offset = u32::from_le_bytes([frame[12], frame[13], frame[14], frame[15]]) as usize;
        let dir = Directory::decode(bytes.get(dir_offset..).ok_or(Error::Corrupt("directory"))?)?;
        Ok(ShoalReader {
            segment: self,
            base: at.offset,
            bytes,
            dir,
        })
    }
}

/// One shoal, read into memory and ready to decode.
#[derive(Debug)]
pub struct ShoalReader<'a> {
    segment: &'a Segment,
    base: u32,
    bytes: Vec<u8>,
    dir: Directory,
}

impl ShoalReader<'_> {
    /// How many rows the shoal holds.
    #[must_use]
    pub const fn rows(&self) -> usize {
        self.dir.rows as usize
    }

    /// One leaf column's chunk, by its dotted name.
    ///
    /// # Errors
    ///
    /// [`Error::NoSuchColumn`] when the shoal has no such leaf.
    pub fn column(&self, name: &str) -> Result<ColumnChunk<'_>> {
        let entry = self
            .dir
            .chunks
            .iter()
            .find(|chunk| chunk.name == name)
            .ok_or_else(|| Error::NoSuchColumn(name.to_owned()))?;
        let start = (entry.offset - self.base) as usize;
        let bytes = self
            .bytes
            .get(start..start + entry.len as usize)
            .ok_or(Error::Corrupt("chunk runs past the shoal"))?;
        Ok(ColumnChunk { entry, bytes })
    }

    /// Check every chunk against the checksum in the directory.
    ///
    /// Doc 10.7: the publisher in doc 12 verifies every chunk checksum as it
    /// converts, so nothing reaches Hugging Face without having been
    /// checksummed on the way out. Doc 10.7 puts the cost at about 3 percent of
    /// the conversion CPU and is clear that this is a guard against a writer bug
    /// producing plausible garbage rather than paranoia about disk hardware.
    ///
    /// # Errors
    ///
    /// [`Error::ChecksumFailed`] naming the first column that did not match.
    pub fn verify(&self) -> Result<()> {
        for entry in &self.dir.chunks {
            let chunk = self.column(&entry.name)?;
            if digest128(chunk.bytes) != entry.digest {
                return Err(Error::ChecksumFailed(entry.name.clone()));
            }
        }
        Ok(())
    }

    /// Materialise the shoal as Arrow.
    ///
    /// `cols` names top level columns and an empty slice means all of them. Doc
    /// 10.9 says a column subset is the only concession to projection and that
    /// it exists because doc 15's dashboard occasionally wants counts and status
    /// codes out of a local segment without decompressing 100 MB of markdown to
    /// get them.
    ///
    /// # Errors
    ///
    /// [`Error::NoSuchColumn`] for a name the schema does not have,
    /// [`Error::Corrupt`] when a chunk does not decode, and
    /// [`Error::UnknownCodec`] for an encoding this build does not know.
    pub fn to_arrow(&self, cols: &[&str]) -> Result<RecordBatch> {
        for name in cols {
            if self.segment.schema.field_with_name(name).is_err() {
                return Err(Error::NoSuchColumn((*name).to_owned()));
            }
        }
        let mut failed: Option<Error> = None;
        let mut fetch = |name: &str| -> Option<Column> {
            match self.column(name).and_then(|chunk| decode(chunk.bytes)) {
                Ok(column) => Some(column),
                Err(err) => {
                    failed.get_or_insert(err);
                    None
                }
            }
        };
        let batch = unflatten(&self.segment.schema, &mut fetch, cols, self.rows());
        match failed {
            Some(err) => Err(err),
            None => batch,
        }
    }
}

/// One column's bytes, with what the directory says about them.
#[derive(Clone, Copy, Debug)]
pub struct ColumnChunk<'a> {
    entry: &'a crate::layout::ChunkEntry,
    bytes: &'a [u8],
}

impl ColumnChunk<'_> {
    /// The dotted leaf name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.entry.name
    }

    /// How many values there are.
    #[must_use]
    pub const fn values(&self) -> u32 {
        self.entry.values
    }

    /// How many of them are null.
    #[must_use]
    pub const fn nulls(&self) -> u32 {
        self.entry.nulls
    }

    /// How many bytes the chunk takes on disk.
    #[must_use]
    pub fn encoded_bytes(&self) -> usize {
        self.bytes.len()
    }

    /// Which encoding it carries.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownCodec`] for an encoding this build does not know, which
    /// doc 10.6 says is a bug to fix within the hour and not a compatibility
    /// problem to route around.
    pub const fn codec(&self) -> Result<crate::Codec> {
        crate::Codec::from_code(self.entry.codec)
    }

    /// Decode the chunk into a leaf column.
    ///
    /// # Errors
    ///
    /// [`Error::Corrupt`] when the bytes do not parse.
    pub fn decode(&self) -> Result<Column> {
        decode(self.bytes)
    }
}

fn read_header(file: &File) -> Result<Header> {
    // A file with no room for a header is not one of ours, and saying so is
    // better than letting the short read come back as an io error, which would
    // send whoever is looking at it to go and check the disk.
    if file.metadata()?.len() < HEADER_LEN as u64 {
        return Err(Error::NotUmi);
    }
    let bytes = read_at(file, 0, HEADER_LEN)?;
    Header::decode(&bytes)
}

fn check_columns(footer: &Footer, schema: &SchemaRef) -> Result<()> {
    let expected: Vec<String> = crate::column::leaf_names(schema)
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    if footer.columns != expected {
        return Err(Error::Schema);
    }
    Ok(())
}

/// A positioned read that does not disturb the file cursor, so a `Segment` can
/// hand out shoals from behind a shared reference.
fn read_at(file: &File, offset: u64, len: usize) -> Result<Vec<u8>> {
    let mut out = vec![0u8; len];
    read_exact_at(file, offset, &mut out)?;
    Ok(out)
}

#[cfg(unix)]
fn read_exact_at(file: &File, offset: u64, buf: &mut [u8]) -> Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)?;
    Ok(())
}

#[cfg(windows)]
fn read_exact_at(file: &File, offset: u64, buf: &mut [u8]) -> Result<()> {
    use std::os::windows::fs::FileExt;
    let mut at = 0usize;
    while at < buf.len() {
        let read = file.seek_read(&mut buf[at..], offset + at as u64)?;
        if read == 0 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "short read",
            )));
        }
        at += read;
    }
    Ok(())
}
