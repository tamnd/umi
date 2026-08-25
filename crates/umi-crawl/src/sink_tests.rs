//! The segment sink, checked by reading back what it wrote.
//!
//! Every test here opens the file again with `umi_file::Segment` rather than
//! trusting the writer's own report. A sink that counted rows correctly and
//! wrote a segment nobody can decode would pass any test that only looked at
//! the return values, and that is the failure this component actually has to
//! rule out.

use std::sync::Arc;

use umi_file::WriterConfig;
use umi_types::{FetcherId, RowKey, Tier, Verification};

use crate::page::{Crawled, PageRow};
use crate::run::Sink;
use crate::sink::{SegmentInfo, SegmentSink};

const T0: u64 = 1_760_000_000_000;

/// A row that is cheap to make and different from its neighbours.
fn row(n: usize, fetched_at_ms: u64) -> PageRow {
    let url = format!("https://example.com/page/{n}");
    let body = format!(
        "<html lang='en'><head><title>Page {n}</title></head><body><h1>Page {n}</h1>\
         <p>Some prose about subject {n}, at a length that extracts to something \
         a sketch can work on without being all boilerplate.</p></body></html>"
    );
    let base = url::Url::parse(&url).expect("parse");
    let extracted = umi_extract::extract(body.as_bytes(), &base);
    let outcome = umi_fetch::Outcome::Ok(Box::new(umi_fetch::outcome::Page {
        final_url: url.clone(),
        status: 200,
        version: umi_fetch::outcome::Version::Http2,
        redirects: Vec::new(),
        headers_kept: Vec::new(),
        headers_digest: [0u8; 32],
        content_type: Some("text/html".to_owned()),
        media: umi_fetch::Media::Html,
        body_digest: *blake3::hash(body.as_bytes()).as_bytes(),
        body: bytes::Bytes::from(body.into_bytes()),
        revalidate: umi_types::Revalidator::default(),
        elapsed: std::time::Duration::from_millis(40),
    }));
    PageRow::build(&Crawled {
        url: &url,
        keys: RowKey::for_url(&url, None).expect("canonicalise"),
        host: "example.com",
        fetched_at_ms,
        outcome: &outcome,
        extracted: Some(&extracted),
        tier_used: Tier::Plain,
        tier_path: &[Tier::Plain],
        robots_checked_ms: T0,
        content_usage: None,
        fetcher_id: FetcherId::LOCAL,
        verification: Verification::Local,
        crawl_profile: 7,
    })
}

fn rows(range: std::ops::Range<usize>, fetched_at_ms: u64) -> Vec<PageRow> {
    range.map(|n| row(n, fetched_at_ms)).collect()
}

/// A writer that rolls after a few hundred small rows.
///
/// Both caps have to come down together. `segment_bytes` is measured against
/// what is on disk, and nothing reaches the disk until a shoal commits, so a
/// small segment cap with the default 16384 row shoal cap would never fire: the
/// whole test would sit in one uncommitted shoal.
fn small() -> WriterConfig {
    WriterConfig {
        shoal_rows: 64,
        segment_bytes: 64 << 10,
        ..WriterConfig::default()
    }
}

/// Read a segment back and return its rows, checking every chunk on the way.
fn read(path: &std::path::Path) -> usize {
    let segment = umi_file::Segment::open(path).expect("open");
    let mut total = 0;
    for i in 0..segment.shoals() {
        let shoal = segment.shoal(i).expect("shoal");
        shoal.verify().expect("checksums hold");
        total += shoal.to_arrow(&[]).expect("decode").num_rows();
    }
    total
}

#[tokio::test]
async fn rows_go_in_and_come_back_out() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sink = SegmentSink::create(dir.path(), SegmentInfo::default(), WriterConfig::default())
        .expect("create");

    sink.take(&rows(0..100, T0)).await.expect("take");
    assert_eq!(sink.rows(), 100);
    assert!(sink.sealed().is_empty(), "nothing seals at 100 rows");

    let sealed = sink.finish().expect("finish").expect("a segment was open");
    assert_eq!(sealed.stats.rows, 100);
    assert_eq!(read(&sealed.path), 100);
}

#[tokio::test]
async fn an_empty_batch_writes_nothing_at_all() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sink = SegmentSink::create(dir.path(), SegmentInfo::default(), WriterConfig::default())
        .expect("create");

    sink.take(&[]).await.expect("take");
    assert!(sink.finish().expect("finish").is_none());
    // A crawl that leased nothing should leave a directory a person can delete
    // without wondering what the empty file was.
    let files = std::fs::read_dir(dir.path()).expect("read_dir").count();
    assert_eq!(files, 0);
}

#[tokio::test]
async fn a_segment_seals_on_size_and_the_next_one_opens_by_itself() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sink = SegmentSink::create(dir.path(), SegmentInfo::default(), small()).expect("create");

    for batch in 0..8 {
        sink.take(&rows(batch * 64..batch * 64 + 64, T0))
            .await
            .expect("take");
    }
    let rolled = sink.sealed();
    assert!(!rolled.is_empty(), "the size cap fired");

    let last = sink.finish().expect("finish");
    let mut total = 0;
    for sealed in rolled.iter().chain(last.iter()) {
        total += read(&sealed.path);
    }
    assert_eq!(total, 512, "every row is in exactly one segment");
    assert_eq!(sink.rows(), 512);
}

#[tokio::test]
async fn a_segment_seals_on_age_using_the_times_on_the_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = WriterConfig {
        segment_ms: 60_000,
        ..WriterConfig::default()
    };
    let sink = SegmentSink::create(dir.path(), SegmentInfo::default(), config).expect("create");

    sink.take(&rows(0..4, T0)).await.expect("take");
    assert!(sink.sealed().is_empty(), "one minute has not passed");

    // No clock anywhere. The rows carry the time, which is the whole point:
    // replaying this crawl on another machine seals in the same place.
    sink.take(&rows(4..8, T0 + 61_000)).await.expect("take");
    let rolled = sink.sealed();
    assert_eq!(rolled.len(), 1, "the age cap fired");
    assert_eq!(rolled[0].stats.rows, 8, "the batch that crossed it went in");
}

#[tokio::test]
async fn segment_names_are_derived_and_repeat_across_runs() {
    let names = |coordinator: [u8; 32]| async move {
        let dir = tempfile::tempdir().expect("tempdir");
        let sink = SegmentSink::create(
            dir.path(),
            SegmentInfo {
                coordinator,
                ..SegmentInfo::default()
            },
            small(),
        )
        .expect("create");
        for batch in 0..8 {
            // A second per batch, because a real crawl's rows move through
            // time and the sort order below depends on that. Doc 12.4's
            // ordering comes from the ULID's 48 bit timestamp, so two segments
            // created inside the same millisecond are ordered by their entropy
            // and not by when they sealed. At 128 MB a segment that does not
            // happen, and a test that pretended otherwise would be asserting a
            // property the format does not have.
            let at = T0 + batch as u64 * 1000;
            sink.take(&rows(batch as usize * 64..batch as usize * 64 + 64, at))
                .await
                .expect("take");
        }
        let mut out: Vec<String> = sink.sealed().iter().map(|s| s.id.to_text()).collect();
        if let Some(last) = sink.finish().expect("finish") {
            out.push(last.id.to_text());
        }
        out
    };

    let a = names([1u8; 32]).await;
    let b = names([1u8; 32]).await;
    let other = names([2u8; 32]).await;

    assert_eq!(a, b, "same coordinator, same input, same names");
    assert!(a.len() > 1);
    assert_eq!(
        a.iter().collect::<std::collections::HashSet<_>>().len(),
        a.len(),
        "one coordinator never repeats a name"
    );
    // Two coordinators writing into the same repository must not collide, and
    // this is the property that makes doc 12.6's batched commit safe without
    // any coordination between them.
    assert!(
        a.iter().all(|name| !other.contains(name)),
        "two coordinators never collide"
    );
    // Doc 12.4's sort order: a day folder listed by name is in seal order.
    let mut sorted = a.clone();
    sorted.sort();
    assert_eq!(a, sorted);
}

#[tokio::test]
async fn the_header_carries_the_scope_the_rows_were_stamped_with() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sink = SegmentSink::create(
        dir.path(),
        SegmentInfo {
            crawl_profile: 7,
            ..SegmentInfo::default()
        },
        WriterConfig::default(),
    )
    .expect("create");

    sink.take(&rows(0..4, T0)).await.expect("take");
    let sealed = sink.finish().expect("finish").expect("a segment was open");

    let segment = umi_file::Segment::open(&sealed.path).expect("open");
    assert_eq!(segment.header().crawl_profile, 7);
    assert_eq!(segment.header().created_ms, T0, "the earliest row's time");
    assert_eq!(&segment.header().segment_id, sealed.id.as_bytes());
}

#[tokio::test]
async fn a_crawler_can_write_straight_into_it() {
    // The reason this type exists, exercised end to end through the trait
    // object the loop takes rather than through the concrete type.
    let dir = tempfile::tempdir().expect("tempdir");
    let sink: Arc<dyn Sink> = Arc::new(
        SegmentSink::create(dir.path(), SegmentInfo::default(), WriterConfig::default())
            .expect("create"),
    );
    sink.take(&rows(0..10, T0)).await.expect("take");

    let files: Vec<_> = std::fs::read_dir(dir.path())
        .expect("read_dir")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .collect();
    assert_eq!(files.len(), 1, "one open segment");
    assert_eq!(
        files[0].extension().and_then(|e| e.to_str()),
        Some(SegmentSink::EXTENSION)
    );
}
