//! Ranged reads, against a real Parquet file and no network.
//!
//! The file is built the same way `tests` builds one, from the sample rows, so
//! what is read back here can be compared against what a whole file read gives.
//! The source underneath is a local file rather than a hub, which is the reason
//! [`Ranges`] is a trait: a warm is a footer, a range read and some arithmetic,
//! and none of it is about Hugging Face.

use std::path::Path;
use std::sync::Arc;

use arrow::array::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;
use umi_file::{Create, Segment, SegmentWriter, StreamKind, WriterConfig, sample};

use crate::convert::convert;
use crate::remote::{PROBE, Ranges, decode, footer, read_column, read_row_groups, span};
use crate::{Error, Result};

const T0: u64 = sample::T0;

/// A file on disk, standing in for a file on the hub.
struct LocalFile {
    path: std::path::PathBuf,
    size: u64,
    /// Every range that was asked for, so a test can say how many requests a
    /// warm cost as well as what it read.
    reads: std::sync::Mutex<Vec<(u64, u64)>>,
}

impl LocalFile {
    fn open(path: &Path) -> Self {
        let size = std::fs::metadata(path).expect("stat").len();
        Self {
            path: path.to_owned(),
            size,
            reads: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl Ranges for LocalFile {
    fn size(&self) -> u64 {
        self.size
    }

    async fn read(&self, at: u64, len: u64) -> Result<Vec<u8>> {
        use std::io::{Read as _, Seek as _};
        self.reads.lock().expect("reads").push((at, len));
        let mut file = std::fs::File::open(&self.path).expect("open");
        file.seek(std::io::SeekFrom::Start(at)).expect("seek");
        let mut buffer = vec![0u8; len as usize];
        file.read_exact(&mut buffer).expect("read");
        Ok(buffer)
    }
}

/// A Parquet file of `rows` frontier rows in row groups of 400.
fn parquet(dir: &Path, rows: usize) -> (std::path::PathBuf, RecordBatch) {
    std::fs::create_dir_all(dir).expect("mkdir");
    let umi = dir.join("segment.umi");
    let batch = sample::batch(StreamKind::Frontier, rows);
    let mut writer = SegmentWriter::create(
        &umi,
        Create {
            stream: StreamKind::Frontier,
            segment_id: *umi_types::Ulid::new(T0, [7u8; 10]).as_bytes(),
            coordinator: [9u8; 32],
            created_ms: T0,
            canon_version: 1,
            extractor_version: 1,
            crawl_profile: 0,
        },
        WriterConfig {
            shoal_rows: 400,
            ..WriterConfig::default()
        },
    )
    .expect("create");
    for at in (0..rows).step_by(400) {
        let take = 400.min(rows - at);
        writer.push(&batch.slice(at, take)).expect("push");
    }
    writer.seal().expect("seal");

    let out = dir.join("segment.parquet");
    let opened = Segment::open(&umi).expect("open");
    convert(&opened, &out).expect("convert");
    (out, batch)
}

/// Every row of a file, read the ordinary way, for comparing against.
fn whole(path: &Path) -> RecordBatch {
    let file = std::fs::File::open(path).expect("open");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("builder")
        .build()
        .expect("reader");
    let batches: Vec<RecordBatch> = reader.map(|b| b.expect("batch")).collect();
    let schema = batches.first().expect("a batch").schema();
    arrow::compute::concat_batches(&schema, &batches).expect("concat")
}

/// The batches a range read gives back, as one batch.
fn joined(batches: &[RecordBatch]) -> RecordBatch {
    let schema = batches.first().expect("a batch").schema();
    arrow::compute::concat_batches(&schema, batches).expect("concat")
}

async fn metadata(source: &LocalFile) -> Arc<ParquetMetaData> {
    Arc::new(footer(source).await.expect("footer"))
}

#[tokio::test]
async fn a_row_group_range_reads_back_exactly_the_rows_it_holds() {
    // The whole point. Row groups are 400 rows, so groups 2 and 3 are rows 800
    // to 1600, and a warm that read anything else would put the wrong domain
    // back in the ledger.
    let dir = tempfile::tempdir().expect("tempdir");
    let (path, _) = parquet(dir.path(), 2000);
    let source = LocalFile::open(&path);

    let metadata = metadata(&source).await;
    let batches = read_row_groups(&source, &metadata, 2, 3)
        .await
        .expect("read");
    let read = joined(&batches);
    assert_eq!(read.num_rows(), 800);
    assert_eq!(read, whole(&path).slice(800, 800));
}

#[tokio::test]
async fn a_warm_off_a_footer_already_in_hand_is_one_read_of_one_row_group() {
    // The claim doc 08.6 rests on. Once the footer is open, warming a domain
    // costs one request for that domain's bytes, and it is a row group rather
    // than anything to do with the size of the file.
    let dir = tempfile::tempdir().expect("tempdir");
    let (path, _) = parquet(dir.path(), 20_000);
    let source = LocalFile::open(&path);
    let metadata = metadata(&source).await;

    source.reads.lock().expect("reads").clear();
    read_row_groups(&source, &metadata, 0, 0)
        .await
        .expect("read");
    let reads = source.reads.lock().expect("reads").clone();
    assert_eq!(reads.len(), 1, "{reads:?}");

    let one_group = source.size() / 50;
    assert!(
        reads[0].1 < one_group * 2,
        "the row group read was {} against a row group of about {one_group}",
        reads[0].1
    );
}

#[tokio::test]
async fn a_small_footer_is_one_read_and_a_big_one_is_two() {
    // Why the footer is a call of its own. A frontier footer carries statistics
    // for twenty columns of every row group, so it grows with the number of
    // domains in the file and a fixed probe cannot always hold it. Missing costs
    // a round trip and not bytes read twice, which is the trade the probe makes.
    let dir = tempfile::tempdir().expect("tempdir");

    let (small, _) = parquet(&dir.path().join("small"), 2000);
    let source = LocalFile::open(&small);
    footer(&source).await.expect("footer");
    let reads = source.reads.lock().expect("reads").clone();
    assert_eq!(reads.len(), 1, "{reads:?}");
    assert_eq!(
        reads[0].1,
        PROBE.min(source.size()),
        "the probe is asked for whole, or the file is, whichever is smaller"
    );

    let (big, _) = parquet(&dir.path().join("big"), 20_000);
    let source = LocalFile::open(&big);
    footer(&source).await.expect("footer");
    let reads = source.reads.lock().expect("reads").clone();
    assert_eq!(reads.len(), 2, "{reads:?}");
    assert!(
        reads[1].1 > PROBE,
        "the second read is for a footer of {} that did not fit in {PROBE}",
        reads[1].1
    );
    assert!(
        reads[1].1 < source.size() / 4,
        "the footer is {} of {} bytes, which is not a footer any more",
        reads[1].1,
        source.size()
    );
}

#[tokio::test]
async fn the_last_row_group_reads_back_like_any_other() {
    // The one that ends at the footer rather than at another row group, which
    // is where an off by one in the span shows up.
    let dir = tempfile::tempdir().expect("tempdir");
    let (path, _) = parquet(dir.path(), 1200);
    let source = LocalFile::open(&path);

    let metadata = metadata(&source).await;
    let batches = read_row_groups(&source, &metadata, 2, 2)
        .await
        .expect("read");
    assert_eq!(joined(&batches), whole(&path).slice(800, 400));
}

#[tokio::test]
async fn a_range_the_file_does_not_have_is_an_error_and_not_a_short_read() {
    // A local index that has drifted from the corpus. Answering with whatever
    // is there would put some other domain's rows into this domain's ledger.
    let dir = tempfile::tempdir().expect("tempdir");
    let (path, _) = parquet(dir.path(), 800);
    let source = LocalFile::open(&path);

    let metadata = metadata(&source).await;
    let failure = read_row_groups(&source, &metadata, 1, 9)
        .await
        .expect_err("no such range");
    assert!(matches!(failure, Error::Parquet(_)), "{failure}");

    let inverted = read_row_groups(&source, &metadata, 1, 0)
        .await
        .expect_err("inverted");
    assert!(matches!(inverted, Error::Parquet(_)), "{inverted}");
}

#[tokio::test]
async fn a_window_that_is_missing_bytes_is_an_error_and_not_wrong_rows() {
    // `Window` answers the reader's offsets against the real file length, so a
    // read past what was fetched is caught rather than silently served from the
    // wrong place. That is what keeps `span` and `decode` honest about each
    // other.
    let dir = tempfile::tempdir().expect("tempdir");
    let (path, _) = parquet(dir.path(), 1200);
    let source = LocalFile::open(&path);
    let metadata = metadata(&source).await;
    let span = span(&metadata, 1, 1).expect("span");

    let short = source
        .read(span.start, (span.end - span.start) - 1)
        .await
        .expect("read");
    let failure = decode(
        metadata,
        1,
        1,
        source.size(),
        span.start,
        Bytes::from(short),
    )
    .expect_err("the last byte is missing");
    assert!(matches!(failure, Error::Parquet(_)), "{failure}");
}

#[tokio::test]
async fn something_that_is_not_a_parquet_file_is_refused_at_the_footer() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("not.parquet");
    std::fs::write(&path, vec![0u8; 4096]).expect("write");
    let source = LocalFile::open(&path);

    let failure = footer(&source).await.expect_err("not a parquet file");
    assert!(matches!(failure, Error::Parquet(_)), "{failure}");

    let empty = dir.path().join("empty.parquet");
    std::fs::write(&empty, []).expect("write");
    let failure = footer(&LocalFile::open(&empty))
        .await
        .expect_err("nothing at all");
    assert!(matches!(failure, Error::Parquet(_)), "{failure}");
}

#[tokio::test]
async fn the_spans_of_two_ranges_do_not_overlap() {
    // What doc 08.6's index rests on at the byte level. Two domains in adjacent
    // row groups are two reads that share nothing, so warming one of them does
    // not pull the other one down the wire.
    let dir = tempfile::tempdir().expect("tempdir");
    let (path, _) = parquet(dir.path(), 2000);
    let source = LocalFile::open(&path);
    let metadata = metadata(&source).await;

    let first = span(&metadata, 0, 1).expect("span");
    let second = span(&metadata, 2, 4).expect("span");
    assert!(
        first.end <= second.start,
        "{first:?} and {second:?} overlap, so a warm reads its neighbour too"
    );
}

#[tokio::test]
async fn one_column_reads_back_every_row_of_it_and_none_of_the_others() {
    // What `umi robots --known` is built on. The corpus is the source of truth
    // for which hosts have an answer, and the only column that says so is the
    // one with the hostname in it, so a run that had to download the bodies to
    // find that out would be reading fifty times what it needs.
    let dir = tempfile::tempdir().expect("tempdir");
    let (path, _) = parquet(dir.path(), 2000);
    let source = LocalFile::open(&path);
    let metadata = metadata(&source).await;

    let batches = read_column(&source, &metadata, "url").await.expect("read");
    let read = joined(&batches);
    assert_eq!(read.num_columns(), 1, "other columns came back too");
    assert_eq!(read.num_rows(), 2000);

    let all = whole(&path);
    let want = all.column_by_name("url").expect("url column");
    assert_eq!(read.column(0), want);
}

#[tokio::test]
async fn a_column_read_is_one_request_a_row_group_and_a_fraction_of_the_bytes() {
    // Both halves of the trade. More requests than reading the file, because a
    // column of two row groups is two places in the file, and far fewer bytes,
    // because everything between those places stays where it is.
    let dir = tempfile::tempdir().expect("tempdir");
    let (path, _) = parquet(dir.path(), 20_000);
    let source = LocalFile::open(&path);
    let metadata = metadata(&source).await;
    let groups = metadata.num_row_groups();

    source.reads.lock().expect("reads").clear();
    read_column(&source, &metadata, "url").await.expect("read");
    let reads = source.reads.lock().expect("reads").clone();
    assert_eq!(
        reads.len(),
        groups,
        "{} reads for {groups} row groups",
        reads.len()
    );

    let bytes: u64 = reads.iter().map(|(_, len)| len).sum();
    assert!(
        bytes < source.size() / 4,
        "the column read {bytes} bytes of a {} byte file, which is not a saving",
        source.size(),
    );
}

#[tokio::test]
async fn a_column_that_is_not_there_says_so_rather_than_reading_the_wrong_one() {
    // A silent wrong answer here is the worst outcome available: a run would
    // build its list of already answered hosts out of some other column and
    // then skip nothing, or skip the wrong things, without any sign of it.
    let dir = tempfile::tempdir().expect("tempdir");
    let (path, _) = parquet(dir.path(), 400);
    let source = LocalFile::open(&path);
    let metadata = metadata(&source).await;

    let failed = read_column(&source, &metadata, "host").await;
    assert!(
        matches!(&failed, Err(Error::Parquet(text)) if text.contains("host")),
        "{failed:?}",
    );
}
