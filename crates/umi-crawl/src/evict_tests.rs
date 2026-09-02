//! Moving a domain out of the store and into a segment.
//!
//! The thing being checked is the placement, not the rows. A domain whose rows
//! all arrived is still a domain the local index cannot find if the row group
//! range is wrong, and the row group range is the only number here that nothing
//! else in the system would catch.

use umi_file::{StreamKind, WriterConfig};
use umi_state::{Candidate, Discovery, MemoryState, Priority, State};
use umi_types::{PldId, RowKey};

use crate::evict::spill_into;
use crate::sink::{SegmentInfo, SegmentSink};

const T0: u64 = 1_760_000_000_000;

/// A sink over a temporary directory, opened on the frontier stream.
fn sink(dir: &std::path::Path, config: WriterConfig) -> SegmentSink {
    SegmentSink::create(
        dir,
        SegmentInfo {
            stream: StreamKind::Frontier,
            ..SegmentInfo::default()
        },
        config,
    )
    .expect("create")
}

/// A store holding `n` urls on `host`, admitted and never fetched.
async fn store(host: &str, n: usize) -> (MemoryState, PldId) {
    let state = MemoryState::new();
    let urls: Vec<String> = (0..n).map(|i| format!("https://{host}/p{i}")).collect();
    let batch: Vec<Candidate<'_>> = urls
        .iter()
        .map(|url| Candidate {
            key: RowKey::for_url(url, None).expect("well formed"),
            url,
            depth: 1,
            priority: Priority::DEFAULT,
            discovered_ms: T0,
            discovery: Discovery::Trusted,
            lastmod_ms: None,
        })
        .collect();
    state.admit(&batch).await.expect("admit");
    let pld = RowKey::for_url(&urls[0], None).expect("well formed").pld;
    (state, pld)
}

#[tokio::test]
async fn a_domain_lands_in_one_segment_in_one_row_group() {
    let (state, pld) = store("example.com", 500).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let sink = sink(dir.path(), WriterConfig::default());

    let placement = spill_into(&state, pld, &sink)
        .await
        .expect("spill")
        .expect("the domain has rows");
    assert_eq!(placement.rows, 500);
    assert_eq!(placement.first_group, 0);
    assert_eq!(placement.last_group, 0);

    // Still resident. Evicting is publish, check, move the index, then unload,
    // and this is only the first of the four.
    assert_eq!(
        state.spill(pld, None, 8192).await.expect("spill").len(),
        500
    );
}

#[tokio::test]
async fn a_domain_bigger_than_one_page_still_comes_out_whole_and_in_order() {
    // The paging is in `read` and nothing above it should be able to tell. Two
    // and a bit pages, so the cursor is used twice and the last page is short.
    let (state, pld) = store("example.com", crate::evict::PAGE * 2 + 7).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let sink = sink(dir.path(), WriterConfig::default());

    let placement = spill_into(&state, pld, &sink)
        .await
        .expect("spill")
        .expect("the domain has rows");
    assert_eq!(placement.rows as usize, crate::evict::PAGE * 2 + 7);

    let sealed = sink.finish().expect("finish").expect("a segment was open");
    assert_eq!(sealed.stats.rows, placement.rows);
    assert_eq!(sealed.id, placement.segment);
}

#[tokio::test]
async fn two_domains_get_row_group_ranges_that_do_not_overlap() {
    // This is the whole point of one write call per domain. A warm reads the
    // range the index gives it, so two domains sharing a row group would mean
    // warming one of them downloads both.
    let dir = tempfile::tempdir().expect("tempdir");
    let sink = sink(dir.path(), WriterConfig::default());

    let (first, first_pld) = store("first.example", 300).await;
    let (second, second_pld) = store("second.example", 300).await;
    let a = spill_into(&first, first_pld, &sink)
        .await
        .expect("spill")
        .expect("rows");
    let b = spill_into(&second, second_pld, &sink)
        .await
        .expect("spill")
        .expect("rows");

    assert_eq!(a.segment, b.segment, "both went into the open segment");
    assert!(
        b.first_group > a.last_group,
        "{a:?} and {b:?} share a row group"
    );
}

#[tokio::test]
async fn a_domain_the_store_has_never_seen_is_nothing_rather_than_an_empty_file() {
    // A caller that got a placement here would write an index entry pointing at
    // zero rows, and a later warm would read a row group that has nothing of
    // this domain in it and conclude the backlog was lost.
    let state = MemoryState::new();
    let dir = tempfile::tempdir().expect("tempdir");
    let sink = sink(dir.path(), WriterConfig::default());

    let placement = spill_into(&state, PldId::from_bytes([7; 8]), &sink)
        .await
        .expect("spill");
    assert!(placement.is_none());
    assert_eq!(sink.rows(), 0);
    assert!(sink.finish().expect("finish").is_none());
}

#[tokio::test]
async fn a_domain_written_across_a_full_segment_stays_in_one_file() {
    // The invariant the local index rests on. The seal check runs after the
    // whole call and never between its shoals, so a domain that overshoots the
    // size cap overshoots it rather than ending up half in the next file.
    let (state, pld) = store("example.com", 5000).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let sink = sink(
        dir.path(),
        WriterConfig {
            segment_bytes: 4096,
            ..WriterConfig::default()
        },
    );

    let placement = spill_into(&state, pld, &sink)
        .await
        .expect("spill")
        .expect("rows");
    assert_eq!(placement.rows, 5000);

    // The segment sealed on the way out of that call, so it is in the drained
    // list rather than still open, and it holds every row.
    let sealed = sink.sealed();
    assert_eq!(sealed.len(), 1, "one file and not two");
    assert_eq!(sealed[0].id, placement.segment);
    assert_eq!(sealed[0].stats.rows, 5000);
}
