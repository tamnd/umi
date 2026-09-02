//! A [`Sink`] that writes rows into doc 10's segments.
//!
//! The loop produces rows and does not care where they go. This is where they
//! go when the answer is a file: rows become shoals, shoals become a segment,
//! and a segment seals at doc 10.3's caps. What happens to a sealed segment
//! after that is the caller's problem, which is what keeps Parquet, manifests
//! and Hugging Face out of this file.
//!
//! # Time
//!
//! Nothing here reads a clock, and that is doc 11.1 rather than tidiness. Two
//! machines running the same build over the same input have to produce the same
//! bytes, and a writer that stamped a segment with the wall clock could not.
//! So both the timestamps this file needs come out of the rows: `created_ms` is
//! the earliest `fetched_at_ms` in the first batch, and the age half of the
//! seal rule is measured against the latest one seen since. A crawl that is
//! fetching pages is a crawl whose row times move, so the age cap still fires;
//! a crawl that has stopped fetching does not seal on age, which is correct,
//! because there is nothing to seal.
//!
//! # Identifiers
//!
//! Segment ULIDs are derived rather than drawn. Doc 12.4 wants 48 bits of
//! timestamp and 80 bits of entropy, and the entropy here is a digest over the
//! coordinator key and a counter. That gives every coordinator a distinct
//! stream of identifiers, gives one coordinator identifiers that never repeat,
//! and gives a test the same file names on every run. A random source would
//! have cost the last of those and bought nothing the first two do not already
//! provide.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use arrow::record_batch::RecordBatch;
use umi_file::{Create, SegmentWriter, WriterConfig};
use umi_file::{SegmentStats, StreamKind};
use umi_state::SpillRow;
use umi_types::Ulid;

use crate::frontier::FrontierBuilder;
use crate::page::{PageBuilder, PageRow};
use crate::robots::{RobotsBuilder, RobotsRow};
use crate::run::{CrawlError, Sink};

/// Everything about a segment that does not change from one to the next.
///
/// Doc 10.4's header carries the versions that produced the rows, and they are
/// per build rather than per segment, so they are given once here instead of
/// at every roll.
#[derive(Clone, Copy, Debug)]
pub struct SegmentInfo {
    /// Which stream this sink's segments carry.
    ///
    /// One per sink and not one per write, because doc 10.1's segment header
    /// names a single stream. A crawl that produces both pages and doc 07.4's
    /// robots snapshot opens two sinks over two directories.
    pub stream: StreamKind,
    /// Doc 04's coordinator key, which is also the entropy this sink derives
    /// its segment identifiers from.
    pub coordinator: [u8; 32],
    /// Doc 11.2's canonicalisation version.
    pub canon_version: u32,
    /// Doc 11.3's extractor version.
    pub extractor_version: u32,
    /// Doc 13.2's `Scope::id`, the same number that is on every row.
    pub crawl_profile: u32,
}

impl Default for SegmentInfo {
    fn default() -> Self {
        Self {
            stream: StreamKind::Pages,
            coordinator: [0u8; 32],
            // One each, which is what every segment written so far carries.
            // These are the numbers a reader compares to decide whether two
            // rows are comparable, so they move when doc 11.2 or doc 11.3
            // changes and not when this crate does.
            canon_version: 1,
            extractor_version: 1,
            crawl_profile: 0,
        }
    }
}

/// A segment that reached its cap and was closed.
#[derive(Clone, Debug)]
pub struct Sealed {
    /// Where it is.
    pub path: PathBuf,
    /// Its identifier, which is also its file name without the extension.
    pub id: Ulid,
    /// What went into it.
    pub stats: SegmentStats,
}

/// Rows into `.umi` segments in a directory.
///
/// One segment is open at a time. Doc 10.3 seals at 128 MB or 15 minutes,
/// whichever comes first, and both are in [`WriterConfig`] rather than here so
/// that a caller who wants small segments for a test does not need a second
/// way to say so.
pub struct SegmentSink {
    dir: PathBuf,
    info: SegmentInfo,
    config: WriterConfig,
    open: Mutex<Open>,
}

/// The mutable half, behind one lock.
///
/// A `std::sync::Mutex` and not the async one, because nothing inside the lock
/// awaits. Rows are encoded into memory and the file is only touched when a
/// shoal or a segment closes, so the critical section is CPU work that finishes
/// in microseconds. An async mutex here would add a scheduling point per batch
/// to protect against a wait that cannot happen.
struct Open {
    writer: Option<SegmentWriter>,
    id: Ulid,
    counter: u64,
    latest_ms: u64,
    sealed: Vec<Sealed>,
    rows: u64,
}

impl SegmentSink {
    /// The file extension doc 10.1 gives the container.
    pub const EXTENSION: &'static str = "umi";

    /// Open a sink over `dir`, creating the directory if it is not there.
    ///
    /// No segment is created yet. A crawl that leases nothing should leave no
    /// files behind, and a crawl that produces one row should leave one
    /// segment rather than an empty one and then a real one.
    ///
    /// # Errors
    ///
    /// Whatever creating the directory reports.
    pub fn create(
        dir: impl Into<PathBuf>,
        info: SegmentInfo,
        config: WriterConfig,
    ) -> std::io::Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            info,
            config,
            open: Mutex::new(Open {
                writer: None,
                id: Ulid::default(),
                counter: 0,
                latest_ms: 0,
                sealed: Vec::new(),
                rows: 0,
            }),
        })
    }

    /// The directory segments land in.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// How many rows have been written, across every segment including the
    /// open one.
    #[must_use]
    pub fn rows(&self) -> u64 {
        self.locked().rows
    }

    /// Take the segments that have sealed since the last call.
    ///
    /// Draining rather than borrowing, because the caller's next move is to
    /// convert each one and then forget it, and a list that grew for the life
    /// of a crawl would be a list with a hundred thousand entries in it by the
    /// end.
    pub fn sealed(&self) -> Vec<Sealed> {
        std::mem::take(&mut self.locked().sealed)
    }

    /// Close the open segment, if there is one.
    ///
    /// Called at the end of a crawl and at a checkpoint. Doc 16's gate 1.3 is
    /// about what a crash leaves behind, and the answer for an unsealed segment
    /// is that its committed shoals are readable and its buffered rows are not,
    /// which is why a clean stop calls this and a crash does not lose more than
    /// the last shoal.
    ///
    /// # Errors
    ///
    /// [`CrawlError::Sink`] if the seal failed, which leaves the partial file
    /// on disk for somebody to look at.
    pub fn finish(&self) -> Result<Option<Sealed>, CrawlError> {
        let mut open = self.locked();
        let sealed = seal(&mut open, &self.dir)?;
        if let Some(sealed) = sealed.clone() {
            open.sealed.push(sealed);
        }
        Ok(sealed)
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, Open> {
        // A poisoned lock means a panic inside a previous `take`, and every
        // path in there is memory. Recovering the guard rather than
        // propagating keeps a panic in one batch from turning into a crawler
        // that cannot write anything ever again.
        self.open.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Derive the next segment identifier.
    ///
    /// blake3 over the coordinator key and the counter, truncated to the 80
    /// bits doc 12.4 wants. Two coordinators never collide because their keys
    /// differ, one coordinator never repeats because its counter does not, and
    /// a test gets the same names twice because neither input is random.
    fn next_id(&self, counter: u64, created_ms: u64) -> Ulid {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.info.coordinator);
        // The stream is in the entropy because one coordinator now runs two
        // sinks. Without it the pages sink and the robots sink derive the same
        // identifiers from the same key and the same counter, and two segments
        // that agree on their name are one segment with half its rows missing.
        hasher.update(&(self.info.stream as u16).to_le_bytes());
        hasher.update(&counter.to_le_bytes());
        let mut entropy = [0u8; 10];
        entropy.copy_from_slice(&hasher.finalize().as_bytes()[..10]);
        Ulid::new(created_ms, entropy)
    }

    /// Open a segment stamped `created_ms`.
    fn open_segment(&self, open: &mut Open, created_ms: u64) -> Result<(), CrawlError> {
        let id = self.next_id(open.counter, created_ms);
        open.counter += 1;
        let path = self
            .dir
            .join(format!("{}.{}", id.to_text(), Self::EXTENSION));
        let writer = SegmentWriter::create(
            &path,
            Create {
                stream: self.info.stream,
                segment_id: *id.as_bytes(),
                coordinator: self.info.coordinator,
                created_ms,
                canon_version: self.info.canon_version,
                extractor_version: self.info.extractor_version,
                crawl_profile: self.info.crawl_profile,
            },
            self.config,
        )
        .map_err(sink_error)?;
        open.writer = Some(writer);
        open.id = id;
        Ok(())
    }
}

/// A row type that knows how to become one of doc 10.5's batches.
///
/// This exists so that [`SegmentSink`] can carry doc 07.4's robots snapshot as
/// well as doc 10's pages. Everything the sink does around a batch is the same
/// for both streams: derive an identifier, roll at the caps, seal, hand the
/// file to the caller. The only parts that differ are which builder encodes the
/// row and where its timestamp lives, so those are the only two things here.
pub trait Rows: Default {
    /// The row this builder takes.
    type Row;

    /// Which stream the rows belong to.
    ///
    /// Checked against the sink's own stream on every write, because a segment
    /// header that says `Pages` over robots batches would produce a file that
    /// writes without complaint and cannot be read.
    const KIND: StreamKind;

    /// Append one row.
    fn push(&mut self, row: &Self::Row);

    /// Whether this shoal has hit doc 10.4's caps.
    fn is_full(&self) -> bool;

    /// Whether anything has gone in.
    fn is_empty(&self) -> bool;

    /// Encode what has gone in.
    fn finish(self) -> RecordBatch;

    /// When the row was fetched, which is the clock this file reads instead of
    /// the wall clock. See the module docs.
    fn stamp(row: &Self::Row) -> u64;
}

impl Rows for PageBuilder {
    type Row = PageRow;
    const KIND: StreamKind = StreamKind::Pages;

    fn push(&mut self, row: &PageRow) {
        Self::push(self, row);
    }
    fn is_full(&self) -> bool {
        Self::is_full(self)
    }
    fn is_empty(&self) -> bool {
        Self::is_empty(self)
    }
    fn finish(self) -> RecordBatch {
        Self::finish(self)
    }
    fn stamp(row: &PageRow) -> u64 {
        row.fetched_at_ms
    }
}

impl Rows for RobotsBuilder {
    type Row = RobotsRow;
    const KIND: StreamKind = StreamKind::Robots;

    fn push(&mut self, row: &RobotsRow) {
        Self::push(self, row);
    }
    fn is_full(&self) -> bool {
        Self::is_full(self)
    }
    fn is_empty(&self) -> bool {
        Self::is_empty(self)
    }
    fn finish(self) -> RecordBatch {
        Self::finish(self)
    }
    fn stamp(row: &RobotsRow) -> u64 {
        row.fetched_at_ms
    }
}

impl Rows for FrontierBuilder {
    type Row = SpillRow;
    const KIND: StreamKind = StreamKind::Frontier;

    fn push(&mut self, row: &SpillRow) {
        Self::push(self, row);
    }
    fn is_full(&self) -> bool {
        Self::is_full(self)
    }
    fn is_empty(&self) -> bool {
        Self::is_empty(self)
    }
    fn finish(self) -> RecordBatch {
        Self::finish(self)
    }
    /// `next_due_ms` and not `last_fetch_ms`, which is the one place the
    /// frontier stream reads a different column than the other two.
    ///
    /// The rows worth spilling are rows nothing has fetched, so their
    /// `last_fetch_ms` is zero, and a segment whose stamps are all zero has an
    /// age of zero forever and never seals on it. `next_due_ms` is set on every
    /// row, it comes off the row rather than off a clock so the file stays
    /// reproducible, and it moves forward as the crawl does, which is what the
    /// age rule needs to fire between two bursts of eviction.
    fn stamp(row: &SpillRow) -> u64 {
        row.row.next_due_ms
    }
}

/// Where one call's rows ended up.
///
/// Doc 08.6's local index needs this and nothing else does, which is why the
/// numbers are shoals rather than bytes: doc 12 converts one shoal into one
/// Parquet row group, so the range here is the range a reader gives a ranged
/// GET without having to open the file first.
///
/// A call's rows are always contiguous and always inside one segment. The seal
/// check runs after the whole call rather than between its shoals, so a batch
/// that overshoots the size cap overshoots it rather than straddling two files,
/// and a caller that writes one domain per call gets a domain that lives in one
/// file with no gaps in it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Placement {
    /// The segment the rows went into.
    pub segment: Ulid,
    /// The first shoal holding them, counting from zero within the segment.
    pub first_group: u32,
    /// The last, inclusive. Equal to `first_group` for a call that fitted in
    /// one shoal, which is the common case.
    pub last_group: u32,
    /// How many rows the call wrote.
    pub rows: u64,
}

impl SegmentSink {
    /// Write a batch of rows, rolling the segment if this fills it.
    ///
    /// The stream is on the sink rather than on the call, so a caller that
    /// wants both streams opens two sinks over two directories. That is the
    /// honest shape: doc 10.1's segment header names one stream, and a file
    /// cannot hold two.
    ///
    /// # Errors
    ///
    /// [`CrawlError::Sink`] if `B` is not the stream this sink was opened for,
    /// or if the write or the seal failed.
    pub fn write<B: Rows>(&self, rows: &[B::Row]) -> Result<(), CrawlError> {
        self.put::<B>(rows, false).map(drop)
    }

    /// The same, but the rows get shoals of their own and the call says which.
    ///
    /// For doc 08.6's local index, and for nothing else. The index points a
    /// domain at a range of row groups, doc 12 turns one shoal into one row
    /// group, and a warm is a ranged GET of that range, so a domain sharing a
    /// shoal with the domain written before it is a domain you cannot warm
    /// without warming its neighbour too. To stop that this flushes whatever
    /// is buffered before it starts and again when it finishes, which costs a
    /// small row group per call and buys an exact warm.
    ///
    /// Not the default, because a page does not need to know which shoal it is
    /// in, a segment full of pages is read whole, and a flush per tick would
    /// turn doc 10.4's shoal caps into a suggestion.
    ///
    /// `None` for no rows, which writes nothing and opens nothing.
    ///
    /// # Errors
    ///
    /// The same as [`write`](Self::write).
    pub fn write_grouped<B: Rows>(&self, rows: &[B::Row]) -> Result<Option<Placement>, CrawlError> {
        self.put::<B>(rows, true)
    }

    fn put<B: Rows>(
        &self,
        rows: &[B::Row],
        grouped: bool,
    ) -> Result<Option<Placement>, CrawlError> {
        if B::KIND != self.info.stream {
            return Err(CrawlError::Sink(format!(
                "this sink writes {:?} and was handed {:?}",
                self.info.stream,
                B::KIND
            )));
        }
        if rows.is_empty() {
            return Ok(None);
        }
        let mut open = self.locked();

        let first_ms = rows.iter().map(B::stamp).min().unwrap_or(0);
        open.latest_ms = open
            .latest_ms
            .max(rows.iter().map(B::stamp).max().unwrap_or(0));
        if open.writer.is_none() {
            self.open_segment(&mut open, first_ms)?;
        }
        if grouped {
            flush(&mut open)?;
        }
        let segment = open.id;
        let first_group = shoals(&open);

        // Shoals, not one batch. A tick's batch is whatever the frontier
        // handed out, and doc 10.4's caps are about what a reader has to hold
        // in memory to decode one shoal, so the two numbers have nothing to do
        // with each other and the sink is where they are reconciled.
        let mut builder = B::default();
        for row in rows {
            builder.push(row);
            open.rows += 1;
            if builder.is_full() {
                let batch = std::mem::take(&mut builder).finish();
                push(&mut open, &batch)?;
            }
        }
        if !builder.is_empty() {
            let batch = builder.finish();
            push(&mut open, &batch)?;
        }
        if grouped {
            flush(&mut open)?;
        }
        // Read before the seal, because sealing takes the writer and there is
        // no shoal count to ask for afterwards. The last shoal is inclusive, and
        // the count is at least one on a grouped call since the rows were not
        // empty and the flush above committed them.
        let placement = Placement {
            segment,
            first_group,
            last_group: shoals(&open).saturating_sub(1),
            rows: rows.len() as u64,
        };

        // The roll happens after the batch, never in the middle of one. A
        // segment that closed halfway through a tick would put two file names
        // on one page's neighbours for no reason a reader could see, and the
        // cost of overshooting the cap by one batch is a few megabytes.
        let latest_ms = open.latest_ms;
        let full = open
            .writer
            .as_ref()
            .is_some_and(|w| w.should_seal(latest_ms));
        if full {
            let sealed = seal(&mut open, &self.dir)?;
            if let Some(sealed) = sealed {
                open.sealed.push(sealed);
            }
        }
        Ok(Some(placement))
    }
}

/// How many shoals the open segment has committed, or zero when none is open.
fn shoals(open: &Open) -> u32 {
    open.writer.as_ref().map_or(0, |w| w.shoals() as u32)
}

/// Commit whatever the writer has buffered, so the next row starts a shoal.
fn flush(open: &mut Open) -> Result<(), CrawlError> {
    match open.writer.as_mut() {
        Some(writer) => writer.flush().map_err(sink_error),
        None => Ok(()),
    }
}

#[async_trait::async_trait]
impl Sink for SegmentSink {
    async fn take(&self, rows: &[PageRow]) -> Result<(), CrawlError> {
        self.write::<PageBuilder>(rows)
    }
}

/// Push one batch into the open writer.
fn push(open: &mut Open, batch: &arrow::record_batch::RecordBatch) -> Result<(), CrawlError> {
    open.writer
        .as_mut()
        .ok_or_else(|| CrawlError::Sink("no segment is open".to_owned()))?
        .push(batch)
        .map_err(sink_error)
}

/// Close the open writer and describe what it produced.
fn seal(open: &mut Open, dir: &Path) -> Result<Option<Sealed>, CrawlError> {
    let Some(writer) = open.writer.take() else {
        return Ok(None);
    };
    let id = open.id;
    let stats = writer.seal().map_err(sink_error)?;
    Ok(Some(Sealed {
        path: dir.join(format!("{}.{}", id.to_text(), SegmentSink::EXTENSION)),
        id,
        stats,
    }))
}

fn sink_error(error: umi_file::Error) -> CrawlError {
    CrawlError::Sink(error.to_string())
}

/// The pages sink and the robots sink behind one [`Sink`].
///
/// Doc 10.1 gives a segment one stream, so a crawl that produces both writes
/// two files at a time. This is the join: the loop keeps handing rows to one
/// sink and does not learn that there are two, and the caller keeps two
/// directories of segments to convert and publish separately.
pub struct Streams {
    /// Doc 10's pages.
    pub pages: std::sync::Arc<SegmentSink>,
    /// Doc 07.4's robots snapshots.
    pub robots: std::sync::Arc<SegmentSink>,
}

#[async_trait::async_trait]
impl Sink for Streams {
    async fn take(&self, rows: &[PageRow]) -> Result<(), CrawlError> {
        self.pages.write::<PageBuilder>(rows)
    }

    async fn take_robots(&self, rows: &[RobotsRow]) -> Result<(), CrawlError> {
        self.robots.write::<RobotsBuilder>(rows)
    }
}
