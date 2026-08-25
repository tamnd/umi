//! The container against real shaped rows.
//!
//! Issue 11's stated bar is that all three stream kinds write and read and that
//! gate 1.3's crash test passes. The crash tests here do the thing doc 10.7
//! asks to be assumed: they take a segment that was written correctly, cut it
//! off at every byte offset that matters, and check that what comes back is the
//! committed prefix and never a panic, a wrong row count, or a plausible piece
//! of garbage.
//!
//! Nothing here reads a clock. Times are made up, which is the same rule the
//! frontier follows and for the same reason: doc 16's gate 1.2 wants a run to
//! be replayable and a component that reads its own clock cannot be.

use tempfile::TempDir;

use crate::sample::{self, EVERY_STREAM, T0};
use crate::{Create, Error, Segment, SegmentWriter, StreamKind, WriterConfig};

fn create(stream: StreamKind) -> Create {
    Create {
        stream,
        segment_id: [7u8; 16],
        coordinator: [9u8; 32],
        created_ms: T0,
        canon_version: 1,
        extractor_version: 4,
        crawl_profile: 0,
    }
}

#[test]
fn every_stream_kind_writes_and_reads_back_exactly() {
    // Issue 11's first bar. Not "reads back something plausible": the batch
    // that comes out has to equal the one that went in, column by column,
    // including which values are null and which are the empty string.
    let dir = TempDir::new().expect("tempdir");
    for stream in EVERY_STREAM {
        let path = dir.path().join(format!("{stream:?}.umi"));
        let written = sample::batch(stream, 500);
        let mut writer =
            SegmentWriter::create(&path, create(stream), WriterConfig::default()).expect("create");
        writer.push(&written).expect("push");
        let stats = writer.seal().expect("seal");
        assert_eq!(stats.rows, 500);

        let segment = Segment::open(&path).expect("open");
        assert_eq!(segment.header().stream, stream);
        assert_eq!(segment.shoals(), 1);
        let shoal = segment.shoal(0).expect("shoal");
        shoal.verify().expect("checksums");
        let read = shoal.to_arrow(&[]).expect("to_arrow");
        assert_eq!(read, written, "{stream:?} did not round trip");
    }
}

#[test]
fn the_header_survives_the_round_trip() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("header.umi");
    let create = create(StreamKind::Pages);
    let mut writer = SegmentWriter::create(&path, create, WriterConfig::default()).expect("create");
    writer.push(&sample::pages(10)).expect("push");
    writer.seal().expect("seal");

    let header = *Segment::open(&path).expect("open").header();
    assert_eq!(header.segment_id, create.segment_id);
    assert_eq!(header.coordinator, create.coordinator);
    assert_eq!(header.created_ms, create.created_ms);
    assert_eq!(header.canon_version, create.canon_version);
    assert_eq!(header.extractor_version, create.extractor_version);
    assert_eq!(header.schema_id, StreamKind::Pages.schema_id());
}

#[test]
fn many_shoals_read_back_in_order() {
    // Doc 10.3 puts four shoals in a segment, so the reader has to get the
    // second one right and not just the first.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("many.umi");
    let config = WriterConfig {
        shoal_rows: 100,
        ..WriterConfig::default()
    };
    let mut writer =
        SegmentWriter::create(&path, create(StreamKind::Pages), config).expect("create");
    let written = sample::pages(450);
    for start in (0..450).step_by(90) {
        writer.push(&written.slice(start, 90)).expect("push");
    }
    writer.seal().expect("seal");

    let segment = Segment::open(&path).expect("open");
    // Three and not five, because the writer does not split a batch it was
    // handed. The cap is a floor on when to seal rather than a ceiling on how
    // large a shoal gets, so 90 row pushes against a 100 row cap seal at 180,
    // 180 and then 90 at the end. That is the behaviour worth pinning: a
    // writer that split batches to hit the cap exactly would be one that can
    // put half of a fetch in one shoal and half in the next.
    assert_eq!(segment.shoals(), 3);
    assert_eq!(segment.stats().rows, 450);

    let mut at = 0usize;
    for i in 0..segment.shoals() {
        let shoal = segment.shoal(i).expect("shoal");
        shoal.verify().expect("checksums");
        let read = shoal.to_arrow(&[]).expect("to_arrow");
        assert_eq!(read, written.slice(at, shoal.rows()), "shoal {i}");
        at += shoal.rows();
    }
    assert_eq!(at, 450);
}

#[test]
fn a_projection_gives_the_columns_asked_for_and_no_others() {
    // Doc 10.9: the only concession to projection, so that doc 15's dashboard
    // can count status codes without decompressing 100 MB of markdown.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("project.umi");
    let mut writer =
        SegmentWriter::create(&path, create(StreamKind::Pages), WriterConfig::default())
            .expect("create");
    let written = sample::pages(200);
    writer.push(&written).expect("push");
    writer.seal().expect("seal");

    let segment = Segment::open(&path).expect("open");
    let shoal = segment.shoal(0).expect("shoal");
    let read = shoal.to_arrow(&["url", "status"]).expect("to_arrow");
    assert_eq!(read.num_columns(), 2);
    assert_eq!(read.num_rows(), 200);
    assert_eq!(read.schema().field(0).name(), "url");
    assert_eq!(read.column(0), written.column_by_name("url").expect("url"));
    assert_eq!(
        read.column(1),
        written.column_by_name("status").expect("status")
    );

    // And a name that is not in the schema is a refusal rather than an empty
    // column, because a typo in a dashboard query should say so.
    assert!(matches!(
        shoal.to_arrow(&["nonexistent"]),
        Err(Error::NoSuchColumn(_))
    ));
}

#[test]
fn the_same_rows_written_twice_give_byte_identical_files() {
    // Doc 11.1: same input bytes plus same version produce byte identical
    // output on every machine. The encode runs on a rayon pool, so this is
    // also the test that says the pool does not get to decide the layout.
    let dir = TempDir::new().expect("tempdir");
    let written = sample::pages(600);
    let mut files = Vec::new();
    for run in 0..2 {
        let path = dir.path().join(format!("determinism-{run}.umi"));
        let mut writer =
            SegmentWriter::create(&path, create(StreamKind::Pages), WriterConfig::default())
                .expect("create");
        writer.push(&written).expect("push");
        writer.seal().expect("seal");
        files.push(std::fs::read(&path).expect("read"));
    }
    assert_eq!(
        files[0].len(),
        files[1].len(),
        "the two runs are different sizes"
    );
    assert!(
        files[0] == files[1],
        "two identical runs produced different bytes"
    );
}

#[test]
fn a_torn_file_is_refused_by_open_and_recovered_by_open_recover() {
    // Gate 1.3. Doc 10.7 says to assume SIGKILL at the worst possible byte
    // offset, so this cuts the file across its whole length rather than at a
    // handful of interesting offsets. The step is prime so that it lands on
    // the structure boundaries from every direction over the length of the
    // file, which is where the writer has anything to get wrong.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("whole.umi");
    let config = WriterConfig {
        shoal_rows: 40,
        ..WriterConfig::default()
    };
    let mut writer =
        SegmentWriter::create(&path, create(StreamKind::Pages), config).expect("create");
    let written = sample::pages(160);
    for start in (0..160).step_by(40) {
        writer.push(&written.slice(start, 40)).expect("push");
    }
    writer.seal().expect("seal");
    let whole = std::fs::read(&path).expect("read");

    let torn = dir.path().join("torn.umi");
    let mut recovered_at_least_once = false;
    // Around 500 cuts whatever the sample grows into, so this stays a test and
    // does not turn into a benchmark. The step is forced odd so that it does
    // not land on the same alignment every time and miss a whole class of
    // offset.
    let step = (whole.len() / 500).max(1) | 1;
    for cut in (crate::layout::HEADER_LEN..whole.len()).step_by(step) {
        let _ = std::fs::remove_file(&torn);
        std::fs::write(&torn, &whole[..cut]).expect("write");

        // Doc 10.9: normal opening refuses a torn file rather than guessing.
        match Segment::open(&torn) {
            Ok(_) => panic!("a file cut at {cut} opened as if it were sealed"),
            Err(Error::NotSealed) => {}
            Err(other) => panic!("a file cut at {cut} failed with {other} rather than NotSealed"),
        }

        let (segment, report) = Segment::open_recover(&torn).expect("recover");
        assert!(!report.sealed);
        assert!(
            report.good_bytes <= cut as u64,
            "recovery at {cut} claimed {} good bytes",
            report.good_bytes
        );
        // Everything the recovery claims has to actually decode, and has to
        // equal the rows that went in at that position. A recovery that hands
        // back plausible garbage is worse than one that hands back nothing.
        let mut at = 0usize;
        for i in 0..segment.shoals() {
            let shoal = segment.shoal(i).expect("shoal");
            shoal
                .verify()
                .expect("a recovered shoal failed its own checksums");
            let read = shoal
                .to_arrow(&[])
                .expect("a recovered shoal did not decode");
            assert_eq!(
                read,
                written.slice(at, shoal.rows()),
                "cut {cut}, shoal {i}"
            );
            at += shoal.rows();
        }
        assert_eq!(at as u64, report.rows);
        if report.shoals > 0 {
            recovered_at_least_once = true;
        }
    }
    assert!(recovered_at_least_once, "no cut recovered any shoal at all");
}

#[test]
fn a_file_that_never_got_a_shoal_recovers_to_nothing_rather_than_failing() {
    // The crash one second after create. There is nothing to recover and that
    // is not an error, because the caller's next move is to delete the file
    // either way and an error would make it look like a disk problem.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("empty.umi");
    let writer = SegmentWriter::create(&path, create(StreamKind::Pages), WriterConfig::default())
        .expect("create");
    drop(writer);

    assert!(matches!(Segment::open(&path), Err(Error::NotSealed)));
    let (segment, report) = Segment::open_recover(&path).expect("recover");
    assert_eq!(segment.shoals(), 0);
    assert_eq!(report.rows, 0);
    assert_eq!(report.good_bytes, crate::layout::HEADER_LEN as u64);
}

#[test]
fn a_sealed_file_recovers_to_itself() {
    // A caller that always recovers should be correct and merely slower, which
    // is what lets the recovery command run on anything without a check first.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("sealed.umi");
    let mut writer =
        SegmentWriter::create(&path, create(StreamKind::Robots), WriterConfig::default())
            .expect("create");
    writer.push(&sample::robots(300)).expect("push");
    writer.seal().expect("seal");

    let (segment, report) = Segment::open_recover(&path).expect("recover");
    assert!(report.sealed);
    assert_eq!(report.lost_bytes, 0);
    assert_eq!(report.rows, 300);
    assert_eq!(segment.shoals(), 1);
}

#[test]
fn a_flipped_bit_in_a_chunk_is_caught_rather_than_decoded() {
    // Doc 10.7's bit rot guard. The point is not that disks flip bits, it is
    // that a writer bug producing plausible garbage should not reach Hugging
    // Face, and this is the cheapest place to catch it.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("rot.umi");
    let mut writer =
        SegmentWriter::create(&path, create(StreamKind::Pages), WriterConfig::default())
            .expect("create");
    writer.push(&sample::pages(100)).expect("push");
    writer.seal().expect("seal");

    let mut bytes = std::fs::read(&path).expect("read");
    // Somewhere in the middle of the column data, well clear of the header and
    // the footer.
    let at = crate::layout::HEADER_LEN + 4096;
    bytes[at] ^= 0x01;
    let rotted = dir.path().join("rotted.umi");
    std::fs::write(&rotted, &bytes).expect("write");

    let segment = Segment::open(&rotted).expect("open");
    let shoal = segment.shoal(0).expect("shoal");
    assert!(
        matches!(shoal.verify(), Err(Error::ChecksumFailed(_))),
        "a flipped bit passed verification"
    );
}

#[test]
fn a_file_that_is_not_ours_is_refused_at_the_magic() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("other.parquet");
    std::fs::write(&path, b"PAR1and then some bytes that go on for a while").expect("write");
    assert!(matches!(Segment::open(&path), Err(Error::NotUmi)));
    assert!(matches!(Segment::open_recover(&path), Err(Error::NotUmi)));
}

#[test]
fn a_writer_never_overwrites_a_segment_that_is_already_there() {
    // A writer that truncated an existing segment would be a writer that can
    // lose a file doc 12 is halfway through publishing.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("once.umi");
    let first = SegmentWriter::create(&path, create(StreamKind::Pages), WriterConfig::default());
    assert!(first.is_ok());
    let second = SegmentWriter::create(&path, create(StreamKind::Pages), WriterConfig::default());
    assert!(matches!(second, Err(Error::Exists)));
}

#[test]
fn a_batch_from_the_wrong_stream_is_refused() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("mismatch.umi");
    let mut writer =
        SegmentWriter::create(&path, create(StreamKind::Pages), WriterConfig::default())
            .expect("create");
    assert!(matches!(
        writer.push(&sample::robots(10)),
        Err(Error::Schema)
    ));
}

#[test]
fn the_memory_floor_drops_the_shoal_cap_rather_than_failing() {
    // Doc 10.8: under the floor the shoal cap drops to 8 MiB and one shoal is
    // in flight at a time, which costs ratio and is the correct trade on a box
    // with no free RAM. It has to be a configuration value, not a rebuild.
    let big = WriterConfig::for_memory(WriterConfig::DEFAULT_BUDGET);
    let small = WriterConfig::for_memory(WriterConfig::FLOOR_BUDGET);
    assert_eq!(big.shoal_bytes, 32 * 1024 * 1024);
    assert_eq!(small.shoal_bytes, 8 * 1024 * 1024);
    assert!(small.builder_bytes < big.builder_bytes);
    // And asking for less than the floor gets the floor rather than an error.
    assert_eq!(WriterConfig::for_memory(1024), small);
}

#[test]
fn a_small_writer_still_writes_a_file_the_normal_reader_can_read() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("small.umi");
    let config = WriterConfig::for_memory(WriterConfig::FLOOR_BUDGET);
    let written = sample::pages(300);
    let mut writer =
        SegmentWriter::create(&path, create(StreamKind::Pages), config).expect("create");
    writer.push(&written).expect("push");
    writer.seal().expect("seal");

    let segment = Segment::open(&path).expect("open");
    let mut rows = 0usize;
    for i in 0..segment.shoals() {
        let shoal = segment.shoal(i).expect("shoal");
        let read = shoal.to_arrow(&[]).expect("to_arrow");
        assert_eq!(read, written.slice(rows, shoal.rows()));
        rows += shoal.rows();
    }
    assert_eq!(rows, 300);
}

#[test]
fn the_segment_caps_from_doc_10_3_are_what_they_say() {
    let config = WriterConfig::default();
    assert_eq!(config.shoal_rows, 16384);
    assert_eq!(config.shoal_bytes, 32 * 1024 * 1024);
    assert_eq!(config.segment_bytes, 128_000_000);
    assert_eq!(config.segment_ms, 15 * 60 * 1000);

    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("caps.umi");
    let writer = SegmentWriter::create(&path, create(StreamKind::Pages), config).expect("create");
    assert!(!writer.should_seal(T0));
    assert!(!writer.should_seal(T0 + 14 * 60 * 1000));
    assert!(writer.should_seal(T0 + 15 * 60 * 1000));
}

#[test]
fn an_empty_batch_is_not_a_shoal() {
    // A tick that leased nothing hands the writer an empty batch, and a shoal
    // with no rows in it would be a shoal the reader has to have an opinion
    // about for no reason.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("nothing.umi");
    let mut writer =
        SegmentWriter::create(&path, create(StreamKind::Pages), WriterConfig::default())
            .expect("create");
    writer.push(&sample::pages(0)).expect("push");
    writer.flush().expect("flush");
    assert_eq!(writer.shoals(), 0);
    writer.seal().expect("seal");

    let segment = Segment::open(&path).expect("open");
    assert_eq!(segment.shoals(), 0);
    assert_eq!(segment.stats().rows, 0);
}
