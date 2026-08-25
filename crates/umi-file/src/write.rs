//! The writer, and the crash discipline in doc 10.7.
//!
//! The writer will be killed. Doc 10.7 says to assume SIGKILL at the worst
//! possible byte offset, assume the box loses power, and design so the answer
//! is always "we lost at most the shoal in progress". That is what the commit
//! record buys: a shoal's column chunks and directory go down first, then
//! `fdatasync`, then a 32 byte record, then `fdatasync` again. A shoal with no
//! valid commit record after it is simply not part of the file, so a torn write
//! inside one cannot corrupt anything already committed.
//!
//! Two syncs a shoal, which at four shoals to a 128 MB segment and a segment
//! every 85 seconds is one sync every 10 seconds. Doc 10.7 says that is
//! affordable even on server2's rotational disks, and that it is exactly why
//! shoals are 32 MiB and not 4 MiB.
//!
//! # The frame header, which doc 10.4 does not have
//!
//! Doc 10.7 says recovery scans forward from the header reading commit records,
//! each one telling you where the next shoal starts. That cannot get going: the
//! first commit record sits after its shoal's data, and nothing before it says
//! how far away it is, so there is no way to find it without scanning for the
//! tag byte by byte and hoping no column chunk contains it.
//!
//! So each shoal is framed. A fixed 64 byte header goes down before the column
//! chunks and says how long the shoal body is, which lets the scan step to
//! where the commit record should be and check it. The frame is not a commit
//! and proves nothing: a shoal that was torn has a frame and no valid record,
//! and it is still not part of the file. The frame is also what keeps column
//! chunks on doc 10.4's 64 byte alignment, since a shoal that starts aligned
//! plus a 64 byte frame leaves the first chunk aligned too.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, SchemaRef};

use crate::codec::{Compression, encode};
use crate::column::{Leaf, flatten, leaf_names};
use crate::layout::{
    ChunkEntry, Commit, Directory, Footer, HEADER_LEN, Header, SegmentStats, ShoalIndex, align_up,
    digest128,
};
use crate::{Error, Result, StreamKind, codec_for};

/// The tag on a shoal frame header.
pub const FRAME_TAG: [u8; 4] = *b"SHFR";

/// How long a shoal frame header is. 64 rather than the 16 it needs, so that a
/// shoal starting on doc 10.4's 64 byte boundary leaves its first column chunk
/// on one too.
pub const FRAME_LEN: usize = 64;

/// How the writer is tuned, from doc 10.3 and doc 10.8.
///
/// Doc 10.8 is emphatic that the memory tradeoff is a runtime decision: doc
/// 03.4 caps `umid` on server1 at 1.5 GB RSS and doc 01 says server1 has
/// essentially no free memory, so the writer has to be able to run small on one
/// box and large on another without a rebuild.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct WriterConfig {
    /// How many rows a shoal takes before it seals. Doc 10.3: 16384, which only
    /// binds on segments dominated by cheap rows, meaning revalidation heavy
    /// segments full of 304 responses where a row is a few hundred bytes.
    pub shoal_rows: usize,
    /// How many encoded bytes a shoal takes before it seals. Doc 10.3: 32 MiB,
    /// which binds first at about 5600 rows on doc 10.2's 6 KB a page.
    pub shoal_bytes: usize,
    /// How many bytes of unencoded builders that works out to. Doc 10.8 says a
    /// 32 MiB encoded shoal is roughly 90 MB of builders, and the writer has to
    /// decide when to seal before it has encoded anything, so this is the cap
    /// it actually watches.
    pub builder_bytes: usize,
    /// How big a segment gets before it seals. Doc 10.3: 128 MB, and doc 01
    /// explains why not gigabytes, which is that a segment taking an hour to
    /// fill is an hour of data at risk and an hour of latency on every
    /// consumer.
    pub segment_bytes: u64,
    /// How long a segment runs before it seals. Doc 10.3: 15 minutes.
    pub segment_ms: u64,
    /// How the column chunks are compressed.
    pub compression: Compression,
}

impl WriterConfig {
    /// Doc 10.8's default budget.
    pub const DEFAULT_BUDGET: usize = 256 * 1024 * 1024;

    /// Doc 10.8's floor. Below this the shoal cap drops.
    pub const FLOOR_BUDGET: usize = 64 * 1024 * 1024;

    /// What doc 10.8's budget works out to at this much memory.
    ///
    /// Above the floor a shoal is 32 MiB and two are in flight. At or below it
    /// the shoal cap drops to 8 MiB and only one is in flight at a time, which
    /// costs compression ratio because dictionaries and symbol tables are
    /// trained on a quarter as much data. Doc 10.8 puts the expected cost at 8
    /// to 12 percent more bytes and says it is the correct trade on a box with
    /// zero free RAM.
    ///
    /// The cutoff is 192 MB rather than the 64 MB floor because that is what
    /// two 90 MB in flight shoals plus dictionary training actually need, and
    /// a budget between the two would otherwise promise a 32 MiB shoal it
    /// cannot hold.
    #[must_use]
    pub fn for_memory(budget: usize) -> Self {
        let budget = budget.max(Self::FLOOR_BUDGET);
        let shoal_bytes = if budget >= 192 * 1024 * 1024 {
            32 * 1024 * 1024
        } else {
            8 * 1024 * 1024
        };
        Self {
            shoal_rows: 16384,
            shoal_bytes,
            // Doc 10.8's own arithmetic: 90 MB of builders for a 32 MiB shoal.
            builder_bytes: shoal_bytes * 90 / 32,
            segment_bytes: 128 * 1000 * 1000,
            segment_ms: 15 * 60 * 1000,
            compression: Compression::default(),
        }
    }
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self::for_memory(Self::DEFAULT_BUDGET)
    }
}

/// What a segment is being created as.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Create {
    /// Which of doc 10.3's streams.
    pub stream: StreamKind,
    /// Identifies the segment for the whole of its life.
    pub segment_id: [u8; 16],
    /// Which coordinator is writing.
    pub coordinator: [u8; 32],
    /// When the file was created, in milliseconds since the Unix epoch.
    ///
    /// Passed in rather than read, for the same reason the frontier takes its
    /// clock as an argument: a component that reads its own clock cannot be
    /// replayed, and doc 16's gate 1.2 asks for exactly that.
    pub created_ms: u64,
    /// The canonicalisation version from doc 11.2.
    pub canon_version: u32,
    /// The extractor version.
    pub extractor_version: u32,
    /// The crawl profile from doc 13.
    pub crawl_profile: u32,
}

/// Appends rows to one `.umi` file.
///
/// One writer per file, which doc 10.11 says is by construction and not by
/// convention. There is no locking here because there is nothing to lock
/// against.
#[derive(Debug)]
pub struct SegmentWriter {
    file: File,
    header: Header,
    config: WriterConfig,
    schema: SchemaRef,
    plan: Vec<(String, DataType)>,
    at: u64,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    pending_bytes: usize,
    shoals: Vec<ShoalIndex>,
    stats: SegmentStats,
}

impl SegmentWriter {
    /// Create the file and write its header.
    ///
    /// The header is written and synced before any row is accepted, so a
    /// segment that exists at all is a segment a reader can identify. Doc 10.7
    /// does not sync the directory entry past file creation, and neither does
    /// this.
    ///
    /// # Errors
    ///
    /// Whatever the filesystem reports, and [`Error::Exists`] if the path is
    /// already there, because a writer that truncated an existing segment would
    /// be a writer that can lose a published file.
    pub fn create(path: &Path, create: Create, config: WriterConfig) -> Result<Self> {
        let schema = create.stream.arrow();
        let header = Header {
            stream: create.stream,
            schema_id: create.stream.schema_id(),
            segment_id: create.segment_id,
            coordinator: create.coordinator,
            created_ms: create.created_ms,
            canon_version: create.canon_version,
            extractor_version: create.extractor_version,
            crawl_profile: create.crawl_profile,
            shoal_rows: u32::try_from(config.shoal_rows).unwrap_or(u32::MAX),
            shoal_bytes: u32::try_from(config.shoal_bytes).unwrap_or(u32::MAX),
        };
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|err| match err.kind() {
                std::io::ErrorKind::AlreadyExists => Error::Exists,
                _ => Error::Io(err),
            })?;
        file.write_all(&header.encode())?;
        file.sync_data()?;
        Ok(Self {
            file,
            header,
            plan: leaf_names(&schema),
            schema,
            config,
            at: HEADER_LEN as u64,
            pending: Vec::new(),
            pending_rows: 0,
            pending_bytes: 0,
            shoals: Vec::new(),
            stats: SegmentStats::default(),
        })
    }

    /// Add rows, sealing a shoal whenever one of doc 10.3's caps is reached.
    ///
    /// Rows arrive in fetch completion order and are written in that order. Doc
    /// 10.5's reorder window, which groups by host inside 4096 rows to make URL
    /// prefix elision pay, is not here: it only earns anything once FSST lands
    /// in issue 90, and doc 10.5 says it comes out anyway if the win is under 5
    /// percent.
    ///
    /// # Errors
    ///
    /// [`Error::Schema`] when the batch is not this segment's stream, and
    /// whatever the filesystem reports when a shoal has to be sealed.
    pub fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema().fields() != self.schema.fields() {
            return Err(Error::Schema);
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.pending_rows += batch.num_rows();
        self.pending_bytes += batch.get_array_memory_size();
        self.pending.push(batch.clone());
        if self.pending_rows >= self.config.shoal_rows
            || self.pending_bytes >= self.config.builder_bytes
        {
            self.flush()?;
        }
        Ok(())
    }

    /// Seal whatever is buffered into a shoal, whether it hit a cap or not.
    ///
    /// This is what the caller reaches for when it wants the rows it has handed
    /// over to survive a kill, and it is what doc 12's publisher waits on before
    /// reading the committed prefix of an active segment.
    ///
    /// # Errors
    ///
    /// Whatever the filesystem or the encoder reports. An error here means the
    /// shoal was not committed, so the rows are still buffered and still lost
    /// if the process dies, which is the same position as before the call.
    pub fn flush(&mut self) -> Result<()> {
        if self.pending_rows == 0 {
            return Ok(());
        }
        let batches = std::mem::take(&mut self.pending);
        let batch =
            arrow::compute::concat_batches(&self.schema, &batches).map_err(|_| Error::Schema)?;
        self.pending_rows = 0;
        self.pending_bytes = 0;
        self.write_shoal(&batch)
    }

    fn write_shoal(&mut self, batch: &RecordBatch) -> Result<()> {
        let leaves = flatten(batch)?;
        let rows = u32::try_from(batch.num_rows()).unwrap_or(u32::MAX);

        // Pad to the alignment first, so that offsets computed against the body
        // are the offsets the chunks land on.
        let pad = align_up(usize::try_from(self.at).unwrap_or(usize::MAX))
            - usize::try_from(self.at).unwrap_or(usize::MAX);
        let frame_at = self.at + pad as u64;
        let body_at = frame_at + FRAME_LEN as u64;

        let mut body: Vec<u8> = Vec::new();
        let mut dir = Directory {
            rows,
            chunks: Vec::with_capacity(leaves.len()),
        };
        let mut logical = 0u64;
        for encoded in encode_all(&leaves, self.config.compression)? {
            let (leaf, bytes) = encoded;
            // Doc 10.4: a column chunk is aligned to 64 bytes so that bit
            // packed and fixed width buffers can be handed to the decoder as
            // aligned slices. The padding costs a few hundred bytes a shoal.
            body.resize(align_up(body.len()), 0);
            let offset = body_at + body.len() as u64;
            dir.chunks.push(ChunkEntry {
                name: leaf.name.clone(),
                offset: u32::try_from(offset).map_err(|_| Error::TooLarge)?,
                len: u32::try_from(bytes.len()).map_err(|_| Error::TooLarge)?,
                codec: codec_for(&leaf.name, &leaf.ty) as u16,
                nulls: u32::try_from(leaf.data.nulls()).unwrap_or(u32::MAX),
                values: u32::try_from(leaf.data.len()).unwrap_or(u32::MAX),
                digest: digest128(&bytes),
            });
            logical += leaf.data.logical_bytes() as u64;
            body.extend_from_slice(&bytes);
        }

        let dir_offset = body.len();
        let dir_bytes = dir.encode();
        body.extend_from_slice(&dir_bytes);

        let body_len = u32::try_from(body.len()).map_err(|_| Error::TooLarge)?;
        let mut frame = [0u8; FRAME_LEN];
        frame[0..4].copy_from_slice(&FRAME_TAG);
        frame[4..8].copy_from_slice(&body_len.to_le_bytes());
        frame[8..12].copy_from_slice(&rows.to_le_bytes());
        frame[12..16].copy_from_slice(
            &u32::try_from(dir_offset)
                .map_err(|_| Error::TooLarge)?
                .to_le_bytes(),
        );

        // The shoal's own bytes go down and are made durable before anything
        // claims they are there.
        if pad > 0 {
            self.file.write_all(&vec![0u8; pad])?;
        }
        self.file.write_all(&frame)?;
        self.file.write_all(&body)?;
        self.file.sync_data()?;

        let commit = Commit {
            index: u32::try_from(self.shoals.len()).unwrap_or(u32::MAX),
            offset: u32::try_from(body_at).map_err(|_| Error::TooLarge)?,
            len: body_len,
            dir_digest: digest128(&dir_bytes),
        };
        self.file.write_all(&commit.encode())?;
        self.file.sync_data()?;

        self.at = body_at + u64::from(body_len) + crate::layout::COMMIT_LEN as u64;
        self.shoals.push(ShoalIndex {
            offset: commit.offset,
            len: body_len,
            rows,
        });
        self.stats.rows += u64::from(rows);
        self.stats.encoded_bytes += u64::from(body_len);
        self.stats.logical_bytes += logical;
        self.note_times(batch);
        Ok(())
    }

    /// Track the segment's time span, for the footer statistics.
    ///
    /// Only the streams that have a `fetched_at_ms` have one, and the column is
    /// looked up by name rather than by position because the three schemas do
    /// not agree on where it sits.
    fn note_times(&mut self, batch: &RecordBatch) {
        let Some(column) = batch.column_by_name("fetched_at_ms") else {
            return;
        };
        let Some(times) = column.as_any().downcast_ref::<arrow::array::UInt64Array>() else {
            return;
        };
        for value in times.values() {
            if self.stats.first_ms == 0 || *value < self.stats.first_ms {
                self.stats.first_ms = *value;
            }
            self.stats.last_ms = self.stats.last_ms.max(*value);
        }
    }

    /// Whether doc 10.3's segment caps say to stop.
    ///
    /// The caller decides what to do about it, because sealing means opening
    /// the next segment and only the caller knows where.
    #[must_use]
    pub fn should_seal(&self, now_ms: u64) -> bool {
        self.at >= self.config.segment_bytes
            || now_ms.saturating_sub(self.header.created_ms) >= self.config.segment_ms
    }

    /// How many bytes are on disk, including the header.
    #[must_use]
    pub const fn bytes_written(&self) -> u64 {
        self.at
    }

    /// How many rows have been committed. Rows still buffered do not count,
    /// because they are not yet anything a crash would leave behind.
    #[must_use]
    pub const fn rows(&self) -> u64 {
        self.stats.rows
    }

    /// How many shoals have been committed.
    #[must_use]
    pub fn shoals(&self) -> usize {
        self.shoals.len()
    }

    /// The header, which is fixed for the file's life.
    #[must_use]
    pub const fn header(&self) -> &Header {
        &self.header
    }

    /// Flush, write the footer, and close.
    ///
    /// Doc 10.7: the footer is written last, followed by its length, its digest
    /// and the magic, in one write and one `fdatasync`. A file whose trailing
    /// magic is missing or whose footer digest fails falls back to the commit
    /// record scan.
    ///
    /// # Errors
    ///
    /// Whatever the filesystem reports. A segment that fails to seal is still
    /// readable through [`Segment::open_recover`](crate::Segment::open_recover)
    /// up to its last commit record, which is the whole point of the design.
    pub fn seal(mut self) -> Result<SegmentStats> {
        self.flush()?;
        let footer = Footer {
            shoals: self.shoals.clone(),
            columns: self.plan.iter().map(|(name, _)| name.clone()).collect(),
            stats: self.stats,
        };
        let bytes = footer.encode();
        let mut trailer = Vec::with_capacity(bytes.len() + crate::layout::TRAILER_LEN);
        trailer.extend_from_slice(&bytes);
        trailer.extend_from_slice(
            &u32::try_from(bytes.len())
                .map_err(|_| Error::TooLarge)?
                .to_le_bytes(),
        );
        trailer.extend_from_slice(&digest128(&bytes));
        trailer.extend_from_slice(&crate::layout::MAGIC);
        self.file.write_all(&trailer)?;
        self.file.sync_data()?;
        self.at += trailer.len() as u64;
        Ok(self.stats)
    }
}

/// Encode every leaf, one column chunk per task.
///
/// Doc 10.8 puts this on a rayon pool because columns are independent and the
/// encode of a full shoal is around 120 ms of single core work that we would
/// rather not add to the fetch loop's latency. The order of the output is the
/// order of the input whatever the pool does with it, because the directory has
/// to be deterministic: doc 11.1 wants the same input bytes and the same
/// version to give byte identical output on every machine, and a directory in
/// completion order would not.
fn encode_all(leaves: &[Leaf], how: Compression) -> Result<Vec<(&Leaf, Vec<u8>)>> {
    use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
    leaves
        .par_iter()
        .map(|leaf| {
            let codec = codec_for(&leaf.name, &leaf.ty);
            encode(&leaf.data, codec, how).map(|bytes| (leaf, bytes))
        })
        .collect::<Result<Vec<_>>>()
}
