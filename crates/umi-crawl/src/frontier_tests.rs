//! The frontier batch, checked column by column against the rows that went in.
//!
//! Two things are worth testing here and neither is the happy path. The first
//! is that the column order matches the schema, because a builder whose order
//! drifted writes a file that looks fine until somebody reads it and finds the
//! host ids in the pld column. The second is the nulls: an unfetched row is the
//! common case in this stream, so the columns that are null on it are the ones
//! a reader will meet first.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{UInt8Type, UInt16Type, UInt64Type};
use umi_state::{LedgerRow, Priority, SpillRow, UrlState};
use umi_types::{HostId, PldId, RowKey, Tier, UrlKey, UrlKeyFull};

use super::frontier::{FrontierBuilder, read_frontier};

/// The moment every row in this file is dated from, since nothing may read a
/// clock.
const T0: u64 = 1_760_000_000_000;

/// A row nothing has ever fetched, which is what most of a backlog is.
fn pending(n: u8) -> SpillRow {
    let url = format!("https://example.com/p{n}");
    SpillRow {
        key: RowKey {
            pld: PldId::from_bytes([1; 8]),
            host: HostId::from_bytes([2; 8]),
            url: UrlKey::from_bytes([n; 10]),
        },
        row: LedgerRow {
            url_key_full: UrlKeyFull::derive(url.as_bytes()),
            host_id: HostId::from_bytes([2; 8]),
            depth: 3,
            priority: Priority::DEFAULT,
            state: UrlState::Pending,
            next_due_ms: T0 + u64::from(n),
            etag_ref: LedgerRow::NO_ETAG,
            ..LedgerRow::default()
        },
        url,
        etag: None,
    }
}

/// The same row after a fetch that worked, so every nullable column has
/// something real in it.
fn fetched(n: u8) -> SpillRow {
    let mut spill = pending(n);
    spill.row.state = UrlState::Fetched;
    spill.row.last_fetch_ms = T0 - 1000;
    spill.row.last_change_ms = T0 - 2000;
    spill.row.last_mod_ms = T0 - 3000;
    spill.row.fetch_count = 4;
    spill.row.change_count = 2;
    spill.row.observed_secs = 86_400;
    spill.row.content_hash = [9; 8];
    spill.row.status = 200;
    spill.row.tier_used = Tier::Emulated;
    spill.row.fail_streak = 0;
    spill.etag = Some("W/\"abc\"".to_owned());
    spill
}

fn batch(rows: &[SpillRow]) -> arrow::record_batch::RecordBatch {
    let mut builder = FrontierBuilder::new();
    for row in rows {
        builder.push(row);
    }
    builder.finish()
}

#[test]
fn every_column_lands_where_the_schema_says() {
    let rows = [fetched(7)];
    let batch = batch(&rows);
    assert_eq!(batch.num_columns(), 20);
    assert_eq!(batch.num_rows(), 1);

    let schema = umi_file::StreamKind::Frontier.arrow();
    assert_eq!(batch.schema(), schema);

    assert_eq!(
        batch.column(0).as_fixed_size_binary().value(0),
        rows[0].key.pld.as_bytes()
    );
    assert_eq!(
        batch.column(1).as_fixed_size_binary().value(0),
        rows[0].key.host.as_bytes()
    );
    assert_eq!(
        batch.column(2).as_fixed_size_binary().value(0),
        rows[0].key.url.as_bytes()
    );
    assert_eq!(
        batch.column(3).as_fixed_size_binary().value(0),
        rows[0].row.url_key_full.as_bytes()
    );
    assert_eq!(batch.column(4).as_string::<i32>().value(0), rows[0].url);
    assert_eq!(batch.column(5).as_primitive::<UInt8Type>().value(0), 3);
    assert_eq!(
        batch.column(6).as_primitive::<UInt16Type>().value(0),
        Priority::DEFAULT.raw()
    );
    assert_eq!(
        batch.column(7).as_primitive::<UInt8Type>().value(0),
        UrlState::Fetched as u8
    );
    assert_eq!(
        batch.column(8).as_primitive::<UInt64Type>().value(0),
        T0 + 7
    );
    assert_eq!(batch.column(15).as_string::<i32>().value(0), "W/\"abc\"");
    assert_eq!(
        batch.column(18).as_primitive::<UInt8Type>().value(0),
        Tier::Emulated.as_u8()
    );
}

#[test]
fn a_row_nothing_has_fetched_writes_nulls_and_not_zeroes() {
    // The whole reason those columns are nullable. A zero `last_fetch_ms` in a
    // published file reads as a fetch at the Unix epoch, and a reader working
    // out a refetch interval off it would get fifty five years.
    let batch = batch(&[pending(1)]);
    for column in [9, 10, 14, 15, 16, 17, 18] {
        assert!(
            batch.column(column).is_null(0),
            "column {} should be null on an unfetched row",
            batch.schema().field(column).name()
        );
    }
    // The counters are not nullable, because zero fetches is a real count and
    // not a missing one.
    for column in [11, 12, 13, 19] {
        assert!(
            batch.column(column).is_valid(0),
            "column {} should be zero and not null",
            batch.schema().field(column).name()
        );
    }
}

#[test]
fn an_all_zero_content_hash_is_a_null_and_not_a_digest() {
    // Nothing hashes to eight zero bytes. Writing them would give every
    // untouched row the same non null hash, and a change detector reading the
    // corpus would call a hundred million of them duplicates of each other.
    let mut spill = fetched(2);
    spill.row.content_hash = [0; 8];
    let batch = batch(&[spill]);
    assert!(batch.column(14).is_null(0));
}

#[test]
fn the_stamp_is_the_due_time_so_a_backlog_still_seals_on_age() {
    use super::sink::Rows;
    // `last_fetch_ms` is zero on the rows this stream is made of, so a sink
    // reading it would see an age of zero forever and keep one segment open
    // across every eviction the crawl ever does.
    assert_eq!(<FrontierBuilder as Rows>::stamp(&pending(5)), T0 + 5);
}

#[test]
fn rows_go_in_until_the_cap_and_the_builder_says_so() {
    let mut builder = FrontierBuilder::new();
    assert!(builder.is_empty());
    assert!(!builder.is_full());
    builder.push(&pending(1));
    assert_eq!(builder.rows(), 1);
    assert!(!builder.is_empty());
    assert!(!builder.is_full());
}

#[test]
fn a_row_survives_the_round_trip_field_for_field() {
    // The whole reason the reader is written as an inverse. A builder that
    // gains a column and a reader that does not is a warm that silently drops
    // a field, and for `observed_secs` that means doc 09.5's refresh estimator
    // restarting on every row it touches.
    let rows = [pending(1), fetched(2), pending(3)];
    let back = read_frontier(&batch(&rows)).expect("read");

    assert_eq!(back.len(), rows.len());
    for (now, was) in back.iter().zip(rows.iter()) {
        assert_eq!(now.key, was.key);
        assert_eq!(now.url, was.url);
        assert_eq!(now.etag, was.etag);
        assert_eq!(now.row, was.row);
    }
}

#[test]
fn a_null_reads_back_as_the_zero_the_local_store_keeps() {
    // The two conventions meeting, in the direction a warm goes. Nothing was
    // ever fetched at the epoch, so a null timestamp and a zero timestamp mean
    // the same thing and the round trip is exact either way.
    let row = pending(4);
    let back = read_frontier(&batch(&[row.clone()])).expect("read");
    let got = &back[0];

    assert_eq!(got.row.last_fetch_ms, 0);
    assert_eq!(got.row.last_change_ms, 0);
    assert_eq!(got.row.last_mod_ms, 0);
    assert_eq!(got.row.status, 0);
    assert_eq!(got.row.content_hash, [0u8; 8]);
    assert_eq!(got.row.tier_used, Tier::default());
    assert_eq!(got.etag, None);
    assert_eq!(got.row, row.row);
}

#[test]
fn the_etag_comes_back_as_text_and_not_as_a_pool_index() {
    // `etag_ref` indexes a pool belonging to whichever store wrote the file, so
    // carrying the number across would point at some other store's ETag. The
    // reader always writes the sentinel and leaves the text for `restore` to
    // re-intern.
    let mut row = fetched(5);
    row.row.etag_ref = 4242;
    let back = read_frontier(&batch(&[row])).expect("read");

    assert_eq!(back[0].row.etag_ref, LedgerRow::NO_ETAG);
    assert_eq!(back[0].etag.as_deref(), Some("W/\"abc\""));
}

#[test]
fn a_batch_of_some_other_stream_is_refused() {
    // A warm reads bytes off a network. A shard entry pointing at a file of
    // pages should say so rather than decode twenty columns of the wrong thing.
    let pages = umi_file::sample::batch(umi_file::StreamKind::Pages, 2);
    let failure = read_frontier(&pages).expect_err("not a frontier batch");
    assert!(
        matches!(failure, crate::CrawlError::Frontier(_)),
        "{failure}"
    );
}

#[test]
fn a_state_byte_the_enum_does_not_define_is_refused() {
    // Same reason. The column is a u8 and the enum is not, so a file written by
    // a future version, or a corrupt one, has a byte here that means nothing to
    // this build, and guessing at it would put a row into the wrong queue.
    use arrow::array::{ArrayRef, UInt8Array};

    let good = batch(&[pending(6)]);
    let mut columns: Vec<ArrayRef> = good.columns().to_vec();
    columns[7] = std::sync::Arc::new(UInt8Array::from(vec![250u8]));
    let broken = arrow::record_batch::RecordBatch::try_new(good.schema(), columns).expect("batch");

    let failure = read_frontier(&broken).expect_err("250 is not a url state");
    assert!(
        matches!(failure, crate::CrawlError::Frontier(_)),
        "{failure}"
    );
}

#[test]
fn an_empty_batch_reads_back_as_no_rows() {
    // A domain with nothing in it is not an error, because `spill_into` can be
    // handed one and the caller decides what an empty answer means.
    let back = read_frontier(&batch(&[])).expect("read");
    assert!(back.is_empty());
}
