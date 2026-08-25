//! End to end, on the sample rows from `umi_file::sample`.
//!
//! The point of using those rows rather than rows made up here is that they are
//! the same bytes the umi-file tests and the umi-file bench use, so a size or a
//! timing measured in one place means the same thing in the other. They are a
//! pure function of the row index, no clock and no randomness, which is what
//! lets a determinism test compare two files rather than two summaries.

use std::path::{Path, PathBuf};

use arrow::array::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::reader::{FileReader as _, SerializedFileReader};
use umi_file::{Create, Segment, SegmentWriter, StreamKind, WriterConfig, sample};

use crate::convert::{BLOOM_COLUMNS, DICTIONARY_COLUMNS, convert};
use crate::keys::{Role, SigningKey};
use crate::manifest::{FileEntry, Manifest, Verification, segment_text};
use crate::repo::{Family, locate};
use crate::{Error, Result};

const T0: u64 = sample::T0;
const COORDINATOR: &str = "server3";
const EXTRACTOR: &str = "umi-extract/0.0.1";

fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

/// Write a sealed segment of `rows` rows and hand back where it is.
///
/// The shoal cap is dropped to 400 so that a small test still produces several
/// row groups. One shoal is one row group, and a conversion test that only ever
/// saw one group would not be testing the mapping doc 12.3 asks for.
fn segment(dir: &Path, stream: StreamKind, rows: usize) -> (PathBuf, RecordBatch) {
    let path = dir.join("segment.umi");
    let batch = sample::batch(stream, rows);
    let config = WriterConfig {
        shoal_rows: 400,
        ..WriterConfig::default()
    };
    let mut writer = SegmentWriter::create(
        &path,
        Create {
            stream,
            segment_id: *umi_types::Ulid::new(T0, [7u8; 10]).as_bytes(),
            coordinator: [9u8; 32],
            created_ms: T0,
            canon_version: 1,
            extractor_version: 1,
            crawl_profile: 0,
        },
        config,
    )
    .expect("create");
    // Pushed in chunks so the writer seals shoals on its own rather than
    // getting one batch it will not split.
    for at in (0..rows).step_by(200) {
        let take = 200.min(rows - at);
        writer.push(&batch.slice(at, take)).expect("push");
    }
    writer.seal().expect("seal");
    (path, batch)
}

fn read_back(path: &Path) -> RecordBatch {
    let file = std::fs::File::open(path).expect("open parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("builder")
        .build()
        .expect("reader");
    let batches: Vec<RecordBatch> = reader.map(|b| b.expect("batch")).collect();
    let schema = batches.first().expect("at least one batch").schema();
    arrow::compute::concat_batches(&schema, &batches).expect("concat")
}

fn entry(path: &str, converted: &crate::Converted, segment_id: [u8; 16]) -> FileEntry {
    FileEntry {
        path: path.to_owned(),
        bytes: converted.bytes,
        rows: converted.rows,
        blake3: converted.blake3,
        sha256: converted.sha256,
        segment_ulid: segment_text(segment_id),
        coordinator: COORDINATOR.to_owned(),
        extractor: EXTRACTOR.to_owned(),
        fetched_at_min_ms: converted.first_ms,
        fetched_at_max_ms: converted.last_ms,
        verification: Verification {
            local: converted.rows,
            ..Verification::default()
        },
    }
}

#[test]
fn every_stream_converts_and_reads_back_exactly() {
    // The whole point of doc 12.3's "names unchanged, nothing flattened,
    // nothing renamed": a consumer reading the Parquet is reading exactly the
    // schema in doc 10.5. Comparing the Arrow that went in against the Arrow
    // that comes out is the strongest way to say that.
    for stream in sample::EVERY_STREAM {
        let dir = tempdir();
        let (umi, written) = segment(dir.path(), stream, 1000);
        let out = dir.path().join("segment.parquet");

        let opened = Segment::open(&umi).expect("open");
        let converted = convert(&opened, &out).expect("convert");

        assert_eq!(converted.rows, 1000, "{stream:?}");
        assert_eq!(converted.row_groups, opened.shoals(), "{stream:?}");
        assert_eq!(
            converted.bytes,
            std::fs::metadata(&out).expect("stat").len()
        );
        assert_eq!(read_back(&out), written, "{stream:?}");
    }
}

#[test]
fn one_shoal_is_one_row_group() {
    // Doc 12.3 makes this exact rather than approximate, and the reason is that
    // it is what makes a corrupted segment damage exactly one file and what
    // makes the row groups come out at doc 10.3's shoal size without anyone
    // tuning a second knob.
    let dir = tempdir();
    let (umi, _) = segment(dir.path(), StreamKind::Pages, 1000);
    let out = dir.path().join("segment.parquet");
    let opened = Segment::open(&umi).expect("open");
    convert(&opened, &out).expect("convert");

    let file = std::fs::File::open(&out).expect("open");
    let reader = SerializedFileReader::new(file).expect("reader");
    let meta = reader.metadata();
    assert_eq!(meta.num_row_groups(), opened.shoals());
    for i in 0..opened.shoals() {
        assert_eq!(
            meta.row_group(i).num_rows() as usize,
            opened.shoal(i).expect("shoal").rows(),
            "row group {i}"
        );
    }
}

#[test]
fn the_writer_settings_are_the_ones_doc_12_3_fixes() {
    let dir = tempdir();
    let (umi, _) = segment(dir.path(), StreamKind::Pages, 1000);
    let out = dir.path().join("segment.parquet");
    convert(&Segment::open(&umi).expect("open"), &out).expect("convert");

    let file = std::fs::File::open(&out).expect("open");
    let reader = SerializedFileReader::new(file).expect("reader");
    let meta = reader.metadata();
    let group = meta.row_group(0);

    for column in group.columns() {
        let name = column.column_path().string();
        assert!(
            matches!(column.compression(), Compression::ZSTD(_)),
            "{name} is not zstd"
        );
        assert!(
            column.offset_index_offset().is_some(),
            "{name} has no offset index"
        );
        assert!(
            column.column_index_offset().is_some(),
            "{name} has no column index"
        );
        // Doc 12.3 says statistics on for all orderable columns, and the
        // interesting failure is a column with none at all rather than one
        // whose min happens to be null.
        assert!(column.statistics().is_some(), "{name} has no statistics");

        let leaf = name.rsplit('.').next().unwrap_or(&name);
        if BLOOM_COLUMNS.contains(&leaf) {
            assert!(
                column.bloom_filter_offset().is_some(),
                "{name} should have a bloom filter"
            );
        } else {
            assert!(
                column.bloom_filter_offset().is_none(),
                "{name} should not have a bloom filter"
            );
        }
    }
}

#[test]
fn only_the_named_columns_are_dictionary_encoded() {
    // A dictionary on a column of 20000 distinct URLs is a dictionary the size
    // of the column plus an index. parquet-rs would fall back at write time
    // anyway, so this is about not paying for the attempt.
    let dir = tempdir();
    let (umi, _) = segment(dir.path(), StreamKind::Pages, 1000);
    let out = dir.path().join("segment.parquet");
    convert(&Segment::open(&umi).expect("open"), &out).expect("convert");

    let file = std::fs::File::open(&out).expect("open");
    let reader = SerializedFileReader::new(file).expect("reader");
    for column in reader.metadata().row_group(0).columns() {
        let name = column.column_path().string();
        let leaf = name.rsplit('.').next().unwrap_or(&name).to_owned();
        if !DICTIONARY_COLUMNS.contains(&leaf.as_str()) {
            assert!(
                column.dictionary_page_offset().is_none(),
                "{name} should not have a dictionary page"
            );
        }
    }
}

#[test]
fn the_same_segment_converts_to_the_same_bytes_twice() {
    // Doc 11.1's determinism rule, carried through to the published artifact.
    // Without it, gate 1.2 stops at the segment boundary and the thing anyone
    // actually downloads is unchecked.
    let dir = tempdir();
    let (umi, _) = segment(dir.path(), StreamKind::Pages, 800);
    let opened = Segment::open(&umi).expect("open");

    let first = dir.path().join("first.parquet");
    let second = dir.path().join("second.parquet");
    let a = convert(&opened, &first).expect("convert");
    let b = convert(&opened, &second).expect("convert");

    assert_eq!(a, b);
    assert_eq!(
        std::fs::read(&first).expect("read"),
        std::fs::read(&second).expect("read")
    );
}

#[test]
fn a_flipped_bit_in_the_segment_stops_the_conversion() {
    // Doc 12.2's step 1 is folded into step 2, so this is the test that says it
    // really happens rather than being skipped for speed.
    let dir = tempdir();
    let (umi, _) = segment(dir.path(), StreamKind::Pages, 600);

    let mut bytes = std::fs::read(&umi).expect("read");
    // Well past the header, so this lands in a column chunk rather than in
    // metadata that would fail to parse for a different reason.
    let at = bytes.len() / 2;
    bytes[at] ^= 0x40;
    let torn = dir.path().join("torn.umi");
    std::fs::write(&torn, &bytes).expect("write");

    let out = dir.path().join("torn.parquet");
    let opened = Segment::open(&torn).expect("open");
    let err = convert(&opened, &out).expect_err("a flipped bit should stop it");
    assert!(
        matches!(
            err,
            Error::Segment(umi_file::Error::ChecksumFailed(_) | umi_file::Error::Corrupt(_))
        ),
        "{err}"
    );
}

#[test]
fn a_manifest_signs_verifies_and_chains() {
    let dir = tempdir();
    let (umi, _) = segment(dir.path(), StreamKind::Pages, 500);
    let out = dir.path().join("segment.parquet");
    let opened = Segment::open(&umi).expect("open");
    let converted = convert(&opened, &out).expect("convert");

    let segment_id = opened.header().segment_id;
    let at = locate(
        Family::Pages,
        converted.first_ms,
        3,
        umi_types::Ulid::from_bytes(segment_id),
    );

    let key = SigningKey::from_seed(Role::Publishing, [5u8; 32]);
    let mut day = Manifest::new(&at.repo, &at.day, StreamKind::Pages, None);
    day.insert(entry(&at.path, &converted, segment_id));

    let signature = day.sign(&key).expect("sign");
    day.verify(&key.verifying(), &signature).expect("verify");

    let published = day.to_json().expect("json");
    let parsed = Manifest::parse(&published).expect("parse");
    assert_eq!(parsed, day);
    parsed
        .verify(&key.verifying(), &signature)
        .expect("verify what was published, not what was in memory");

    // The next day continues from it, which is doc 12.5's chain.
    let mut next = Manifest::new(
        &at.repo,
        "20260818",
        StreamKind::Pages,
        Some(day.digest().expect("digest")),
    );
    next.insert(entry("data/20260818/a.parquet", &converted, segment_id));
    assert!(next.follows(&day).expect("follows"));

    // And a manifest that claims to continue from a day it does not is caught
    // by recomputing the digest rather than by trusting the field.
    let mut tampered = day.clone();
    tampered.files[0].rows += 1;
    assert!(!next.follows(&tampered).expect("follows"));
}

#[test]
fn a_manifest_that_is_not_in_canonical_form_is_refused() {
    // The digest only means something if everyone computes it over the same
    // bytes, so a document that means the right thing but is spelled
    // differently has to be refused rather than accepted with a digest nobody
    // else would produce.
    let mut day = Manifest::new(
        "open-index/umi-pages-2026w34-03",
        "20260817",
        StreamKind::Pages,
        None,
    );
    day.insert(FileEntry {
        path: "data/20260817/a.parquet".to_owned(),
        bytes: 10,
        rows: 2,
        blake3: [1u8; 32],
        sha256: [2u8; 32],
        segment_ulid: segment_text([0u8; 16]),
        coordinator: COORDINATOR.to_owned(),
        extractor: EXTRACTOR.to_owned(),
        fetched_at_min_ms: T0,
        fetched_at_max_ms: T0 + 1,
        verification: Verification::default(),
    });
    let good = day.to_json().expect("json");
    Manifest::parse(&good).expect("the canonical form parses");

    let text = String::from_utf8(good).expect("utf8");

    // Pretty printed: same meaning, different bytes.
    let value: serde_json::Value = serde_json::from_str(&text).expect("value");
    let pretty = serde_json::to_vec_pretty(&value).expect("pretty");
    assert!(Manifest::parse(&pretty).is_err());

    // A digest that does not cover the document.
    let broken = text.replace("\"rows\":2", "\"rows\":3");
    assert!(Manifest::parse(broken.as_bytes()).is_err());

    // A version this build does not know.
    let future = text.replace("\"manifest_version\":1", "\"manifest_version\":2");
    assert!(Manifest::parse(future.as_bytes()).is_err());

    assert!(Manifest::parse(b"{}").is_err());
    assert!(Manifest::parse(b"not json at all").is_err());
}

#[test]
fn a_manifest_keeps_its_files_in_path_order() {
    let mut day = Manifest::new(
        "open-index/umi-pages-2026w34-03",
        "20260817",
        StreamKind::Pages,
        None,
    );
    let make = |name: &str| FileEntry {
        path: format!("data/20260817/{name}.parquet"),
        bytes: 1,
        rows: 1,
        blake3: [0u8; 32],
        sha256: [0u8; 32],
        segment_ulid: segment_text([0u8; 16]),
        coordinator: COORDINATOR.to_owned(),
        extractor: EXTRACTOR.to_owned(),
        fetched_at_min_ms: T0,
        fetched_at_max_ms: T0,
        verification: Verification::default(),
    };
    for name in ["c", "a", "b"] {
        day.insert(make(name));
    }
    let paths: Vec<&str> = day.files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        [
            "data/20260817/a.parquet",
            "data/20260817/b.parquet",
            "data/20260817/c.parquet"
        ]
    );
    assert!(day.contains("data/20260817/b.parquet"));
    assert!(!day.contains("data/20260817/d.parquet"));

    // Inserting the same path twice replaces rather than duplicates, which is
    // what makes doc 12.8's "adopt the orphan into the next manifest" safe to
    // run more than once.
    let mut again = make("b");
    again.rows = 99;
    day.insert(again);
    assert_eq!(day.files.len(), 3);
    assert_eq!(day.totals(), (101, 3));
}

#[test]
fn only_a_publishing_key_can_sign_a_manifest() {
    let day = Manifest::new(
        "open-index/umi-pages-2026w34-03",
        "20260817",
        StreamKind::Pages,
        None,
    );
    for role in [Role::CrawlIdentity, Role::Lease] {
        let wrong = SigningKey::from_seed(role, [5u8; 32]);
        assert!(matches!(day.sign(&wrong), Err(Error::Key)), "{role:?}");
        assert!(matches!(
            day.verify(&wrong.verifying(), &[0u8; 64]),
            Err(Error::Key)
        ));
    }
}

#[test]
fn the_full_pipeline_clears_the_file_for_deletion() -> Result<()> {
    // Doc 12.2's steps 1 through 8 with the network parts stubbed by hand, so
    // that the shape of the evidence a real uploader has to produce is written
    // down somewhere that compiles.
    use crate::gc::{Evidence, LedgerLocation, ManifestCommitted, ReadBack, Remote, clear, delete};

    let dir = tempdir();
    let (umi, _) = segment(dir.path(), StreamKind::Pages, 400);
    let opened = Segment::open(&umi)?;
    let out = dir.path().join("segment.parquet");
    let converted = convert(&opened, &out)?;

    let segment_id = opened.header().segment_id;
    let at = locate(
        Family::Pages,
        converted.first_ms,
        0,
        umi_types::Ulid::from_bytes(segment_id),
    );
    let file = entry(&at.path, &converted, segment_id);

    let key = SigningKey::from_seed(Role::Publishing, [5u8; 32]);
    let mut day = Manifest::new(&at.repo, &at.day, StreamKind::Pages, None);
    day.insert(file.clone());
    let signature = day.sign(&key)?;

    // Step 5, as if the bytes had come back over the network.
    let remote_bytes = std::fs::read(&out)?;
    let read_back = ReadBack {
        blake3: *blake3::hash(&remote_bytes).as_bytes(),
        full: true,
    };
    // Step 6, as if the manifest had been pushed and read back.
    let committed = Manifest::parse(&day.to_json()?)?;
    let manifest = ManifestCommitted {
        digest: committed.digest()?,
        signature_verified: committed.verify(&key.verifying(), &signature).is_ok(),
        references_file: committed.contains(&file.path),
    };

    let evidence = Evidence {
        remote: Some(Remote {
            bytes: remote_bytes.len() as u64,
        }),
        read_back: Some(read_back),
        manifest: Some(manifest),
        ledger: Some(LedgerLocation {
            repo: at.repo.clone(),
            path: at.path.clone(),
            blake3: file.blake3,
        }),
    };

    let cleared = clear(&at.repo, &file, &evidence).expect("all four conditions");
    delete(&out, cleared)?;
    assert!(!out.exists());
    // The segment itself goes too, and only after the Parquet made it.
    assert!(umi.exists());
    Ok(())
}

#[test]
fn a_truncated_upload_never_clears() {
    use crate::gc::{
        Blocked, Evidence, LedgerLocation, ManifestCommitted, ReadBack, Remote, clear,
    };

    let dir = tempdir();
    let (umi, _) = segment(dir.path(), StreamKind::Pages, 400);
    let out = dir.path().join("segment.parquet");
    let opened = Segment::open(&umi).expect("open");
    let converted = convert(&opened, &out).expect("convert");
    let file = entry(
        "data/20260817/a.parquet",
        &converted,
        opened.header().segment_id,
    );

    let short = &std::fs::read(&out).expect("read")[..converted.bytes as usize - 1];
    let evidence = Evidence {
        remote: Some(Remote {
            bytes: short.len() as u64,
        }),
        read_back: Some(ReadBack {
            blake3: *blake3::hash(short).as_bytes(),
            full: true,
        }),
        manifest: Some(ManifestCommitted {
            digest: [0u8; 32],
            signature_verified: true,
            references_file: true,
        }),
        ledger: Some(LedgerLocation {
            repo: "open-index/umi-pages-2026w34-03".to_owned(),
            path: file.path.clone(),
            blake3: file.blake3,
        }),
    };
    assert!(matches!(
        clear("open-index/umi-pages-2026w34-03", &file, &evidence),
        Err(Blocked::RemoteSize { .. })
    ));
}

#[test]
fn conversion_refuses_to_write_over_a_directory() {
    // Not an interesting case on its own, but it is the one filesystem error
    // that reaches `convert` before anything is read, and it should come back
    // as an io error rather than as a Parquet one.
    let dir = tempdir();
    let (umi, _) = segment(dir.path(), StreamKind::Pages, 100);
    let opened = Segment::open(&umi).expect("open");
    let err = convert(&opened, dir.path()).expect_err("a directory is not a file");
    assert!(matches!(err, Error::Io(_)), "{err}");
}
