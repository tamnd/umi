//! Doc 08.6's backlog, on its way out of the local store and into a file.
//!
//! This is the write half of an eviction. The state layer hands back a
//! [`SpillRow`] per ledger row it is about to drop, and this turns a run of
//! them into a batch matching [`StreamKind::Frontier`]. What happens to the
//! batch after that is the same path pages and robots already take: a segment,
//! a Parquet file, a manifest entry, a published dataset.
//!
//! It lives here rather than in `umi-frontier` because the [`Rows`] trait and
//! the sink that drives it are in this crate, and because `umi-frontier` is
//! about choosing what to fetch next rather than about columns. The name is
//! the stream's, not the crate's.
//!
//! # Why the columns are mostly nullable
//!
//! The rows worth spilling are overwhelmingly rows nothing has fetched. That
//! is the point of a backlog. So everything describing a fetch that already
//! happened is null when it has not happened, rather than zero: a zero
//! `last_fetch_ms` reads as a real fetch at the epoch, and a reader computing a
//! refetch interval off it would get 1970. The local store cannot do this,
//! since a fixed width row has nowhere to put a null, so it stores zero and
//! this is where the two conventions meet.
//!
//! [`Rows`]: crate::sink::Rows

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, RecordBatch, StringArray,
    StringBuilder, UInt8Array, UInt8Builder, UInt16Array, UInt16Builder, UInt32Array,
    UInt32Builder, UInt64Array, UInt64Builder,
};
use umi_file::StreamKind;
use umi_state::{LedgerRow, Priority, SpillRow, UrlState};
use umi_types::{HostId, PldId, RowKey, Tier, UrlKey, UrlKeyFull};

use crate::CrawlError;

/// [`SpillRow`]s into doc 10.5's frontier batch.
///
/// The same shape as the page and robots builders: rows go in one at a time,
/// the builder says when it has had enough, and `finish` produces a batch that
/// matches [`StreamKind::Frontier`] exactly.
pub struct FrontierBuilder {
    pld_id: FixedSizeBinaryBuilder,
    host_id: FixedSizeBinaryBuilder,
    url_key: FixedSizeBinaryBuilder,
    url_key_full: FixedSizeBinaryBuilder,
    url: StringBuilder,
    depth: UInt8Builder,
    priority: UInt16Builder,
    state: UInt8Builder,
    next_due_ms: UInt64Builder,
    last_fetch_ms: UInt64Builder,
    last_change_ms: UInt64Builder,
    fetch_count: UInt32Builder,
    change_count: UInt32Builder,
    observed_secs: UInt32Builder,
    content_hash: FixedSizeBinaryBuilder,
    etag: StringBuilder,
    last_mod_ms: UInt64Builder,
    status: UInt16Builder,
    tier_used: UInt8Builder,
    fail_streak: UInt8Builder,
    rows: usize,
    bytes: usize,
}

impl Default for FrontierBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FrontierBuilder {
    /// The row half of the shoal cap.
    ///
    /// Four times the page limit because a frontier row is small and fixed
    /// width apart from the URL and the ETag. Sixty five thousand of them is
    /// around eight megabytes before encoding, which is the same size class as
    /// a page shoal of sixteen thousand.
    pub const ROW_LIMIT: usize = 65_536;

    /// The byte half of the shoal cap, doc 10.4's 32 MiB.
    pub const BYTE_LIMIT: usize = 32 << 20;

    /// An empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pld_id: FixedSizeBinaryBuilder::new(PldId::LEN as i32),
            host_id: FixedSizeBinaryBuilder::new(HostId::LEN as i32),
            url_key: FixedSizeBinaryBuilder::new(UrlKey::LEN as i32),
            url_key_full: FixedSizeBinaryBuilder::new(UrlKeyFull::LEN as i32),
            url: StringBuilder::new(),
            depth: UInt8Builder::new(),
            priority: UInt16Builder::new(),
            state: UInt8Builder::new(),
            next_due_ms: UInt64Builder::new(),
            last_fetch_ms: UInt64Builder::new(),
            last_change_ms: UInt64Builder::new(),
            fetch_count: UInt32Builder::new(),
            change_count: UInt32Builder::new(),
            observed_secs: UInt32Builder::new(),
            content_hash: FixedSizeBinaryBuilder::new(8),
            etag: StringBuilder::new(),
            last_mod_ms: UInt64Builder::new(),
            status: UInt16Builder::new(),
            tier_used: UInt8Builder::new(),
            fail_streak: UInt8Builder::new(),
            rows: 0,
            bytes: 0,
        }
    }

    /// How many rows have gone in.
    #[must_use]
    pub const fn rows(&self) -> usize {
        self.rows
    }

    /// Whether this shoal is full.
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
    pub fn push(&mut self, spill: &SpillRow) {
        let row = &spill.row;
        self.pld_id
            .append_value(spill.key.pld.as_bytes())
            .expect("pld_id is 8 bytes");
        // From the key and not from `row.host_id`. The two agree on every row
        // the store wrote, and the key is the one the file is sorted by, so if
        // they ever disagreed the sorted column is the one a reader's row group
        // statistics were computed from.
        self.host_id
            .append_value(spill.key.host.as_bytes())
            .expect("host_id is 8 bytes");
        self.url_key
            .append_value(spill.key.url.as_bytes())
            .expect("url_key is 10 bytes");
        self.url_key_full
            .append_value(row.url_key_full.as_bytes())
            .expect("url_key_full is 16 bytes");
        self.url.append_value(&spill.url);
        self.depth.append_value(row.depth);
        self.priority.append_value(row.priority.raw());
        self.state.append_value(row.state as u8);
        self.next_due_ms.append_value(row.next_due_ms);
        self.last_fetch_ms.append_option(when(row.last_fetch_ms));
        self.last_change_ms.append_option(when(row.last_change_ms));
        self.fetch_count.append_value(row.fetch_count);
        self.change_count.append_value(row.change_count);
        self.observed_secs.append_value(row.observed_secs);
        // An all zero content hash is what a row that has never been fetched
        // carries, and it is not a digest anything hashed to. Writing it would
        // give a hundred million untouched rows the same non null hash and make
        // every change detector think they are duplicates of each other.
        if row.content_hash == [0u8; 8] {
            self.content_hash.append_null();
        } else {
            self.content_hash
                .append_value(row.content_hash)
                .expect("content_hash is 8 bytes");
        }
        self.etag.append_option(spill.etag.as_deref());
        self.last_mod_ms.append_option(when(row.last_mod_ms));
        // Status zero means no response ever arrived, which is not a status
        // code, so it is a null here for the same reason the timestamps are.
        self.status
            .append_option((row.status != 0).then_some(row.status));
        // Tier is nullable because a row nothing has fetched was not fetched at
        // any tier, and `Tier::Revalidate` is zero, so a zero would read as a
        // conditional request that never happened.
        self.tier_used
            .append_option((row.state != UrlState::Pending).then(|| row.tier_used.as_u8()));
        self.fail_streak.append_value(row.fail_streak);

        self.rows += 1;
        self.bytes += spill.url.len() + spill.etag.as_ref().map_or(0, String::len) + FIXED_BYTES;
    }

    /// Finish the batch.
    #[must_use]
    pub fn finish(mut self) -> RecordBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.pld_id.finish()),
            Arc::new(self.host_id.finish()),
            Arc::new(self.url_key.finish()),
            Arc::new(self.url_key_full.finish()),
            Arc::new(self.url.finish()),
            Arc::new(self.depth.finish()),
            Arc::new(self.priority.finish()),
            Arc::new(self.state.finish()),
            Arc::new(self.next_due_ms.finish()),
            Arc::new(self.last_fetch_ms.finish()),
            Arc::new(self.last_change_ms.finish()),
            Arc::new(self.fetch_count.finish()),
            Arc::new(self.change_count.finish()),
            Arc::new(self.observed_secs.finish()),
            Arc::new(self.content_hash.finish()),
            Arc::new(self.etag.finish()),
            Arc::new(self.last_mod_ms.finish()),
            Arc::new(self.status.finish()),
            Arc::new(self.tier_used.finish()),
            Arc::new(self.fail_streak.finish()),
        ];
        RecordBatch::try_new(StreamKind::Frontier.arrow(), columns)
            .expect("the frontier builder matches doc 10.5")
    }
}

/// The fixed width part of one row, for the byte half of the shoal cap.
///
/// The two keys, the two fingerprints, the content hash and the eleven scalar
/// columns. It is an estimate of the uncompressed size and not of the file,
/// which is what the cap wants: the cap exists so a shoal fits in memory while
/// it is being built.
const FIXED_BYTES: usize = PldId::LEN + HostId::LEN + UrlKey::LEN + UrlKeyFull::LEN + 8 + 40;

/// A timestamp column's value, or `None` for a row it never happened on.
///
/// Zero is the local store's way of writing "never", because a fixed width row
/// has nowhere to put a null. The published file has somewhere, and a zero
/// there would read as the Unix epoch.
const fn when(ms: u64) -> Option<u64> {
    if ms == 0 { None } else { Some(ms) }
}

/// A published frontier batch back into [`SpillRow`]s, for a warm.
///
/// The inverse of [`FrontierBuilder`], and the reason it is written as an
/// inverse rather than as a fresh parse is that the two have to agree column
/// for column forever. A builder that gains a column and a reader that does not
/// is a warm that silently drops a field, which for `observed_secs` means doc
/// 09.5's refresh estimator quietly restarts on a hundred million rows.
///
/// The nulls turn back into the local store's zeros. That direction is lossless
/// where the other one is not quite: the file distinguishes "never fetched"
/// from "fetched at the epoch" and the fixed width row does not, but nothing
/// was ever fetched at the epoch, so the round trip through a file and back is
/// exact for every row a store can hold.
///
/// `etag_ref` comes back as [`LedgerRow::NO_ETAG`] on every row no matter what
/// the file says, because it is an index into a pool that is local to the store
/// that wrote it. The text is carried in [`SpillRow::etag`] and
/// [`restore`](umi_state::State::restore) re-interns it.
///
/// # Errors
///
/// [`CrawlError::Frontier`] if the batch is not a frontier batch: a column
/// missing, a column of the wrong type, or a byte in the state or tier column
/// that is not one of the values those enums define. A published file is bytes
/// off a network, so none of that is an assertion.
pub fn read_frontier(batch: &RecordBatch) -> Result<Vec<SpillRow>, CrawlError> {
    let schema = StreamKind::Frontier.arrow();
    if batch.schema() != schema {
        return Err(CrawlError::Frontier(format!(
            "this is not a frontier batch: {} columns against {}",
            batch.num_columns(),
            schema.fields().len()
        )));
    }

    let pld_id = fixed(batch, 0, PldId::LEN)?;
    let host_id = fixed(batch, 1, HostId::LEN)?;
    let url_key = fixed(batch, 2, UrlKey::LEN)?;
    let url_key_full = fixed(batch, 3, UrlKeyFull::LEN)?;
    let url = text(batch, 4)?;
    let depth = u8s(batch, 5)?;
    let priority = u16s(batch, 6)?;
    let state = u8s(batch, 7)?;
    let next_due_ms = u64s(batch, 8)?;
    let last_fetch_ms = u64s(batch, 9)?;
    let last_change_ms = u64s(batch, 10)?;
    let fetch_count = u32s(batch, 11)?;
    let change_count = u32s(batch, 12)?;
    let observed_secs = u32s(batch, 13)?;
    let content_hash = fixed(batch, 14, 8)?;
    let etag = text(batch, 15)?;
    let last_mod_ms = u64s(batch, 16)?;
    let status = u16s(batch, 17)?;
    let tier_used = u8s(batch, 18)?;
    let fail_streak = u8s(batch, 19)?;

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        // The four keys and the url are the columns a row cannot be without,
        // because they are what a row is. The rest have a documented zero.
        let key = RowKey {
            pld: PldId::from_bytes(eight(pld_id.value(i), "pld_id")?),
            host: HostId::from_bytes(eight(host_id.value(i), "host_id")?),
            url: UrlKey::from_bytes(ten(url_key.value(i))?),
        };
        let state_byte = state.value(i);
        let row = LedgerRow {
            url_key_full: UrlKeyFull::from_bytes(sixteen(url_key_full.value(i))?),
            host_id: key.host,
            depth: depth.value(i),
            priority: Priority::from_raw(priority.value(i)),
            state: UrlState::from_u8(state_byte)
                .ok_or_else(|| CrawlError::Frontier(format!("{state_byte} is not a url state")))?,
            next_due_ms: next_due_ms.value(i),
            last_fetch_ms: or_zero(last_fetch_ms, i),
            last_change_ms: or_zero(last_change_ms, i),
            fetch_count: fetch_count.value(i),
            change_count: change_count.value(i),
            observed_secs: observed_secs.value(i),
            content_hash: if content_hash.is_null(i) {
                [0u8; 8]
            } else {
                eight(content_hash.value(i), "content_hash")?
            },
            // Always the sentinel. The pool this indexed belongs to whichever
            // store wrote the file, and the text is in `etag` beside it.
            etag_ref: LedgerRow::NO_ETAG,
            last_mod_ms: or_zero(last_mod_ms, i),
            status: if status.is_null(i) {
                0
            } else {
                status.value(i)
            },
            tier_used: if tier_used.is_null(i) {
                Tier::default()
            } else {
                let byte = tier_used.value(i);
                Tier::from_u8(byte)
                    .ok_or_else(|| CrawlError::Frontier(format!("{byte} is not a tier")))?
            },
            fail_streak: fail_streak.value(i),
        };
        rows.push(SpillRow {
            key,
            url: url.value(i).to_owned(),
            row,
            etag: (!etag.is_null(i)).then(|| etag.value(i).to_owned()),
        });
    }
    Ok(rows)
}

/// A timestamp column's value, with a null reading as the store's zero.
fn or_zero(column: &UInt64Array, i: usize) -> u64 {
    if column.is_null(i) {
        0
    } else {
        column.value(i)
    }
}

/// One fixed width binary column, checked for width as well as for type.
///
/// The width matters on its own. Two of these columns are eight bytes and
/// swapping them would pass a type check and put host ids in the pld column,
/// which is the failure the builder's tests exist to catch on the way out.
fn fixed(
    batch: &RecordBatch,
    at: usize,
    width: usize,
) -> Result<&FixedSizeBinaryArray, CrawlError> {
    let column = batch
        .column(at)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| CrawlError::Frontier(format!("column {at} is not fixed width binary")))?;
    if column.value_length() != width as i32 {
        return Err(CrawlError::Frontier(format!(
            "column {at} is {} bytes wide and should be {width}",
            column.value_length()
        )));
    }
    Ok(column)
}

/// One string column.
fn text(batch: &RecordBatch, at: usize) -> Result<&StringArray, CrawlError> {
    batch
        .column(at)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| CrawlError::Frontier(format!("column {at} is not a string")))
}

/// The four scalar column readers, which differ only in width.
macro_rules! scalar {
    ($name:ident, $array:ty, $what:literal) => {
        fn $name(batch: &RecordBatch, at: usize) -> Result<&$array, CrawlError> {
            batch
                .column(at)
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| CrawlError::Frontier(format!("column {at} is not {}", $what)))
        }
    };
}

scalar!(u8s, UInt8Array, "a u8");
scalar!(u16s, UInt16Array, "a u16");
scalar!(u32s, UInt32Array, "a u32");
scalar!(u64s, UInt64Array, "a u64");

/// Exactly eight bytes, named so the error says which column was short.
fn eight(bytes: &[u8], what: &str) -> Result<[u8; 8], CrawlError> {
    bytes
        .try_into()
        .map_err(|_| CrawlError::Frontier(format!("{what} is {} bytes and not 8", bytes.len())))
}

/// Exactly ten bytes, the url key.
fn ten(bytes: &[u8]) -> Result<[u8; 10], CrawlError> {
    bytes
        .try_into()
        .map_err(|_| CrawlError::Frontier(format!("url_key is {} bytes and not 10", bytes.len())))
}

/// Exactly sixteen bytes, the full url fingerprint.
fn sixteen(bytes: &[u8]) -> Result<[u8; 16], CrawlError> {
    bytes.try_into().map_err(|_| {
        CrawlError::Frontier(format!("url_key_full is {} bytes and not 16", bytes.len()))
    })
}
