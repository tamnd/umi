//! Reading part of a published file back, doc 08.6's ranged GET.
//!
//! An eviction writes a domain into one row group range of one frontier file
//! and records where it went. A warm is the other half: given that range, pull
//! back the rows and nothing else. The file is one of hundreds of thousands and
//! can be hundreds of megabytes, so the whole point is that a warm costs a few
//! hundred kilobytes rather than a file.
//!
//! # The footer, then the rows
//!
//! Parquet keeps its metadata at the end, so a file has to be opened from the
//! tail. [`footer`] does that in one read where it can and two where the footer
//! is bigger than [`PROBE`], and [`read_row_groups`] then takes one read for the
//! range itself, computed from the column chunk offsets the footer gave it.
//!
//! Those are two separate calls rather than one because a frontier file holds a
//! row group per domain and its footer is proportional to how many, so on a big
//! file the metadata is far more bytes than the rows a single warm wants. Read
//! once, warm many: a caller working through the domains that landed in one file
//! pays for the footer once and one range read per domain after that.
//!
//! # Why the sync reader
//!
//! The parquet crate has an async reader that would do the ranged reads by
//! itself. It is not used here, because it would want the `async` feature and
//! the futures stack behind it in order to save one thing we do not need saved:
//! the reads are already down to one per domain, each is awaited before any
//! decoding starts, and a warm is off the critical path by the time doc 08.6's
//! prefetch is doing its job. A small `ChunkReader` over the fetched span is
//! what makes the sync reader work on a partial file, and it is fourteen lines.

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::errors::ParquetError;
use parquet::file::metadata::{FooterTail, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::{ChunkReader, Length};

use crate::hub::Hub;
use crate::{Error, Result};

/// How much of the tail to ask for in the first request.
///
/// Sixty four kilobytes, which holds the footer of a file of about thirty row
/// groups and misses it on a bigger one. A frontier footer is the schema plus
/// per column statistics for twenty columns of every row group, and it measures
/// at roughly 1.9 kilobytes per row group, so a file of a thousand domains has
/// a footer of about two megabytes.
///
/// That is why the constant is not simply raised until it always fits. The
/// probe is spent whether it hits or not, so a probe sized for the worst file
/// is a megabyte wasted on every small one, and the miss only costs a round
/// trip rather than a re-read: the second request is for the footer exactly.
/// The cost that actually matters is amortised elsewhere, by
/// [`read_row_groups`] taking a footer rather than reading one, so a coordinator
/// warming a hundred domains out of one file reads it once.
pub const PROBE: u64 = 64 << 10;

/// The eight byte tail every Parquet file ends with, a length and `PAR1`.
const TAIL: u64 = 8;

/// Somewhere a byte range can be read from.
///
/// A trait and not a [`Hub`] argument because the reading here is worth testing
/// without a network, and because doc 04's fetcher protocol will want to serve
/// the same ranges out of a local mirror. Two methods, both of which a plain
/// file answers as easily as an object store.
pub trait Ranges {
    /// How long the whole file is.
    fn size(&self) -> u64;

    /// Read `len` bytes starting at `at`.
    ///
    /// # Errors
    ///
    /// Whatever the source reports. A short answer is an error and not a short
    /// read, because every caller here computed the range from the footer and
    /// has nothing sensible to do with half of it.
    fn read(&self, at: u64, len: u64) -> impl Future<Output = Result<Vec<u8>>> + Send;
}

/// One published file on the hub.
pub struct HubFile<'a> {
    hub: &'a Hub,
    repo: &'a str,
    path: &'a str,
    size: u64,
}

impl<'a> HubFile<'a> {
    /// Ask the hub how big the file is, which is where a ranged read starts.
    ///
    /// # Errors
    ///
    /// [`Error::Hub`] or [`Error::Transport`] if the hub will not answer, and
    /// [`Error::Parquet`] if there is no such file, which is a shard entry
    /// pointing at nothing.
    pub async fn open(hub: &'a Hub, repo: &'a str, path: &'a str) -> Result<Self> {
        let size = match hub.info(repo, path).await? {
            Some(remote) => remote.size,
            None => {
                return Err(Error::Parquet(format!(
                    "{repo} has no file at {path}, so the shard entry points at nothing"
                )));
            }
        };
        Ok(Self {
            hub,
            repo,
            path,
            size,
        })
    }
}

impl Ranges for HubFile<'_> {
    fn size(&self) -> u64 {
        self.size
    }

    async fn read(&self, at: u64, len: u64) -> Result<Vec<u8>> {
        self.hub.read_range(self.repo, self.path, at, len).await
    }
}

/// Read one row group range out of a published file.
///
/// `first` and `last` are inclusive and come straight from doc 08.6's local
/// index. The batches come back in file order, which for a frontier file is key
/// order, which is the order `State::restore` wants them in anyway.
///
/// The footer is an argument rather than something this reads for itself,
/// because one frontier file holds a row group per domain and a warm asks for
/// one domain. Reading a two megabyte footer to fetch ten kilobytes of rows,
/// and then reading it again for the next domain in the same file, would make
/// the metadata the whole cost of a warm. So the caller reads it once with
/// [`footer`] and keeps it for as long as it is working on that file.
///
/// # Errors
///
/// Whatever the source reports, and [`Error::Parquet`] if the range names row
/// groups the file does not have, or if the rows will not decode.
pub async fn read_row_groups<R: Ranges>(
    source: &R,
    metadata: &Arc<ParquetMetaData>,
    first: u32,
    last: u32,
) -> Result<Vec<RecordBatch>> {
    let span = span(metadata, first, last)?;
    let bytes = source.read(span.start, span.end - span.start).await?;
    decode(
        Arc::clone(metadata),
        first,
        last,
        source.size(),
        span.start,
        Bytes::from(bytes),
    )
}

/// Read and decode the file's footer.
///
/// # Errors
///
/// Whatever the source reports, and [`Error::Parquet`] if what came back is not
/// a Parquet footer.
pub async fn footer<R: Ranges>(source: &R) -> Result<ParquetMetaData> {
    let size = source.size();
    if size < TAIL {
        return Err(Error::Parquet(format!(
            "a file of {size} bytes is too short to be a Parquet file"
        )));
    }
    let want = PROBE.min(size);
    let tail = source.read(size - want, want).await?;
    let length = footer_length(&tail)?;
    if length + TAIL > size {
        return Err(Error::Parquet(format!(
            "the footer says it is {length} bytes and the file is {size}"
        )));
    }

    // The probe holds the whole footer on a small file. On a big one it does
    // not, and the second read is for the footer exactly, so the miss costs a
    // round trip rather than bytes read twice.
    let footer = if length + TAIL <= want {
        let end = tail.len() - TAIL as usize;
        Bytes::from(tail).slice(end - length as usize..end)
    } else {
        Bytes::from(source.read(size - length - TAIL, length).await?)
    };
    ParquetMetaDataReader::decode_metadata(&footer).map_err(parquet_error)
}

/// The declared footer length from the last eight bytes of a tail read.
fn footer_length(tail: &[u8]) -> Result<u64> {
    let at = tail
        .len()
        .checked_sub(TAIL as usize)
        .ok_or_else(|| Error::Parquet("the tail read came back shorter than eight bytes".into()))?;
    let found = FooterTail::try_from(&tail[at..]).map_err(parquet_error)?;
    if found.is_encrypted_footer() {
        return Err(Error::Parquet(
            "the footer is encrypted, which nothing in this project writes".into(),
        ));
    }
    Ok(found.metadata_length() as u64)
}

/// The byte range holding a row group range, from first column chunk to last.
///
/// One range and not one per column, because the columns of a row group are
/// written next to each other and a warm wants all twenty of them. Asking for
/// the span in one request is one round trip against twenty.
///
/// # Errors
///
/// [`Error::Parquet`] if the range is inverted or names a row group the file
/// does not have, which is a local index that has drifted from the corpus and
/// is worth an error rather than a short read.
pub fn span(metadata: &ParquetMetaData, first: u32, last: u32) -> Result<Range<u64>> {
    let groups = metadata.row_groups();
    if first > last || last as usize >= groups.len() {
        return Err(Error::Parquet(format!(
            "row groups {first} to {last} are not in a file that has {}",
            groups.len()
        )));
    }
    let mut start = u64::MAX;
    let mut end = 0;
    for group in &groups[first as usize..=last as usize] {
        for column in group.columns() {
            let (at, len) = column.byte_range();
            start = start.min(at);
            end = end.max(at + len);
        }
    }
    if start >= end {
        return Err(Error::Parquet(format!(
            "row groups {first} to {last} hold no columns"
        )));
    }
    Ok(start..end)
}

/// Decode a row group range out of the bytes that hold it.
///
/// `at` is where `bytes` starts in the file, and `size` is the whole file's
/// length, which the reader needs even though almost none of it is here.
///
/// # Errors
///
/// [`Error::Parquet`] if the rows will not decode, or if the reader asks for a
/// byte outside the window, which would mean [`span`] and this call disagree.
pub fn decode(
    metadata: Arc<ParquetMetaData>,
    first: u32,
    last: u32,
    size: u64,
    at: u64,
    bytes: Bytes,
) -> Result<Vec<RecordBatch>> {
    let window = Window { at, size, bytes };
    let groups: Vec<usize> = (first as usize..=last as usize).collect();
    // The footer is already decoded, so this hands it over rather than letting
    // the builder go and read it again, which on a window that holds only the
    // row groups would be a read of bytes we deliberately did not fetch.
    let reader_metadata =
        ArrowReaderMetadata::try_new(metadata, ArrowReaderOptions::new()).map_err(parquet_error)?;
    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(window, reader_metadata)
        .with_row_groups(groups)
        .build()
        .map_err(parquet_error)?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Parquet(e.to_string()))
}

/// The part of a file we actually fetched, pretending to be the whole file.
///
/// The sync Parquet reader wants a [`ChunkReader`] over the file, and asking
/// for the file is the thing this module exists to avoid. So `len` answers with
/// the real length, which is what the reader checks its offsets against, and
/// the reads are served out of the window. A read outside it is an error rather
/// than a short answer, because the only way one can happen is that the row
/// groups asked for here are not the ones [`span`] measured.
struct Window {
    /// Where `bytes` starts in the file.
    at: u64,
    /// How long the whole file is.
    size: u64,
    /// The bytes we have.
    bytes: Bytes,
}

impl Window {
    /// The window relative offset of a file offset, if it is in here.
    fn offset(&self, start: u64, length: u64) -> parquet::errors::Result<usize> {
        let end = start + length;
        if start < self.at || end > self.at + self.bytes.len() as u64 {
            return Err(ParquetError::General(format!(
                "the reader asked for {start}..{end} and the window holds {}..{}",
                self.at,
                self.at + self.bytes.len() as u64
            )));
        }
        Ok((start - self.at) as usize)
    }
}

impl Length for Window {
    fn len(&self) -> u64 {
        self.size
    }
}

impl ChunkReader for Window {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let from = self.offset(start, 0)?;
        Ok(bytes::Buf::reader(self.bytes.slice(from..)))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let from = self.offset(start, length as u64)?;
        Ok(self.bytes.slice(from..from + length))
    }
}

/// Parquet's error as this crate's, for the same reason [`Error::Parquet`] is a
/// string: a caller should not have to name the parquet crate to handle a warm.
fn parquet_error(cause: ParquetError) -> Error {
    Error::Parquet(cause.to_string())
}
