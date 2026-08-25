//! What publishing costs, against doc 12.2's budget.
//!
//! Doc 12.2 gives the whole pipeline 10 minutes at p99 from segment seal to
//! local file deleted, and breaks that into per step numbers. Four of the eight
//! steps are local work this crate does, and those four are what this measures.
//! The other four are network and land with the Hugging Face client.
//!
//! ```text
//! step                                    doc 12.2 budget   measured here
//! 1  verify every chunk checksum          ~1 s              part 1, folded in
//! 2  convert shoals to Parquet            ~30 s at 0.4 core part 1
//! 3  digest the Parquet, blake3 + sha256  ~2 s              part 2
//! 5  verify the remote copy               ~3 s              part 5, local half
//! 6  append and sign the manifest         ~2 s              part 3
//! ```
//!
//! Step 2's budget is the one that matters. A segment seals every 90 seconds
//! per host at 250 pages a second, so 30 seconds of conversion is a third of a
//! core sustained, and doc 01 gives the box 2 vCPU with extraction wanting most
//! of one. If conversion goes over, the publisher falls behind production and
//! doc 15's backpressure ladder starts throttling the crawl, which is a gate
//! 1.1 failure arriving by a side door.
//!
//! Part 4 is not a budget, it is the disk arithmetic doc 12.4 sizes
//! repositories against. A 300 GB slice at the measured bytes a page is how
//! many pages one repository holds, and that is what decides whether a week is
//! 9 repositories or 30.
//!
//! Environment:
//!
//! * `UMI_BENCH_ROWS` how many page rows, default 20000
//! * `UMI_BENCH_REPEAT` how many times to repeat the timed sections, default 3
//!
//! Run it pinned, because the number that matters is a share of one core:
//!
//! ```text
//! cargo build --release -p umi-publish --benches
//! taskset -c 1 ./target/release/deps/publish-<hash>
//! ```

use std::path::Path;
use std::time::Instant;

use umi_file::{Create, Segment, SegmentWriter, StreamKind, WriterConfig, sample};
use umi_publish::convert::convert;
use umi_publish::keys::{Role, SigningKey};
use umi_publish::manifest::{FileEntry, Manifest, Verification, segment_text};

/// A made up creation time. Doc 11.1 keeps the output path clock free and a
/// benchmark that read a real one would not be comparable between two runs.
const CREATED_MS: u64 = sample::T0;

/// Doc 10.3's segment cap, which is what every per segment budget in doc 12.2
/// is quoted against.
const SEGMENT_BYTES: f64 = 128.0 * 1024.0 * 1024.0;

/// Doc 12.4's soft ceiling for one repository.
const SLICE_BYTES: f64 = 300.0 * 1e9;

/// Doc 12.4 sizes a day folder at about this many files.
const FILES_A_DAY: usize = 3100;

fn main() {
    let rows = env("UMI_BENCH_ROWS", 20_000);
    let repeat = env("UMI_BENCH_REPEAT", 3);

    println!("umi-publish, {rows} page rows, best of {repeat}\n");

    let dir = tempdir();
    let umi = dir.path().join("segment.umi");
    let umi_bytes = write_segment(&umi, rows);
    println!(
        "segment in: {:.1} MiB, {:.0} B/page\n",
        umi_bytes as f64 / (1024.0 * 1024.0),
        umi_bytes as f64 / rows as f64
    );

    let parquet = dir.path().join("segment.parquet");
    let converted = conversion(&umi, &parquet, rows, repeat);
    digests(&parquet, repeat);
    manifests(repeat);
    read_back(&parquet, converted, repeat);
    footprint(umi_bytes, converted, rows);
}

/// Part 5. Doc 12.2 step 5, the local half of verifying the remote copy.
///
/// Doc 12.7 asks for three 1 MiB ranges rather than a full re download and
/// costs that at about 3 seconds against 15, but both of those numbers are
/// network. What the publisher spends locally is a read of the staged file and
/// a blake3 over it, once for the splice and once again for the comparison
/// digest, and that is CPU on a box doc 01 gives 2 vCPU. If it turned out to be
/// seconds it would be worth restructuring; the point of measuring is to know
/// which.
fn read_back(parquet: &Path, bytes: u64, repeat: usize) {
    use umi_publish::gc::sample_ranges;

    println!("part 5: verifying the remote copy, doc 12.2 step 5 budgets 3 s");
    println!(
        "  {:<24} {:>12} {:>14} {:>16}",
        "", "ms", "s per 128 MB", "of the 3 s"
    );

    let seed = *blake3::hash(&std::fs::read(parquet).expect("read")).as_bytes();
    let mut sampled = f64::MAX;
    let mut whole = f64::MAX;
    for _ in 0..repeat {
        // The sampled path: read the staged copy, splice what came back over
        // it, digest the result. The fetch itself is not here, so this is the
        // CPU the publisher pays regardless of how fast the link is.
        let start = Instant::now();
        let mut spliced = std::fs::read(parquet).expect("read");
        for (at, len) in sample_ranges(bytes, 3, 1024 * 1024, &seed) {
            let at = at as usize;
            let end = (at + len as usize).min(spliced.len());
            let fetched = spliced[at..end].to_vec();
            spliced[at..end].copy_from_slice(&fetched);
        }
        let _ = blake3::hash(&spliced);
        sampled = sampled.min(start.elapsed().as_secs_f64());

        // The one in a hundred full path, for comparison. Same digest, no
        // read of the local file, because the bytes came off the wire.
        let start = Instant::now();
        let _ = blake3::hash(&spliced);
        whole = whole.min(start.elapsed().as_secs_f64());
    }

    let scale = SEGMENT_BYTES / bytes as f64;
    for (name, seconds) in [("sampled, spliced", sampled), ("full, digest only", whole)] {
        println!(
            "  {name:<24} {:>12.2} {:>14.2} {:>15.1}%",
            seconds * 1000.0,
            seconds * scale,
            seconds * scale / 3.0 * 100.0
        );
    }
    println!();
}

/// Part 1. Doc 12.2 step 2, the 30 second conversion budget.
fn conversion(umi: &Path, out: &Path, rows: usize, repeat: usize) -> u64 {
    println!("part 1: conversion, doc 12.2 step 2 budgets 30 s a segment");
    println!(
        "  {:<24} {:>12} {:>14} {:>16}",
        "", "pages/s", "s per 128 MB", "core at 250/s"
    );

    let segment = Segment::open(umi).expect("open");
    let mut best = f64::MAX;
    let mut bytes = 0u64;
    for _ in 0..repeat {
        let _ = std::fs::remove_file(out);
        let start = Instant::now();
        let converted = convert(&segment, out).expect("convert");
        best = best.min(start.elapsed().as_secs_f64());
        bytes = converted.bytes;
    }

    let pages_per_s = rows as f64 / best;
    // Per 128 MB of segment, because that is the unit doc 12.2 budgets. The
    // sample segment is smaller than a real one, so this is scaled rather than
    // measured directly, and it is the honest way to compare against a budget
    // written per segment.
    let per_segment = best * SEGMENT_BYTES / std::fs::metadata(umi).expect("stat").len() as f64;
    println!(
        "  {:<24} {pages_per_s:>12.0} {per_segment:>14.2} {:>15.2}%",
        "convert and verify",
        250.0 / pages_per_s * 100.0
    );
    if per_segment > 30.0 {
        println!("  OVER doc 12.2's 30 s. The publisher falls behind production.");
    } else {
        println!(
            "  inside the 30 s budget with {:.1} s to spare for the Parquet writer",
            30.0 - per_segment
        );
    }
    println!();
    bytes
}

/// Part 2. Doc 12.2 step 3, the two digests over the finished file.
///
/// Both are computed in one pass over the bytes in `convert`, so this measures
/// them on their own to say how much of part 1 they are. blake3 is roughly a
/// gigabyte a second a core and sha256 is roughly a quarter of that without
/// hardware acceleration, so the answer is expected to be all sha256.
fn digests(parquet: &Path, repeat: usize) {
    use sha2::Digest as _;

    println!("part 2: the two digests, doc 12.2 step 3 budgets 2 s a segment");
    println!(
        "  {:<12} {:>12} {:>14} {:>16}",
        "hash", "ms", "MB/s", "s per 128 MB"
    );

    let bytes = std::fs::read(parquet).expect("read");
    /// The name doc 12.5 publishes the digest under, and the hash itself.
    type Case = (&'static str, fn(&[u8]));

    let cases: [Case; 2] = [
        ("blake3", |b| {
            std::hint::black_box(blake3::hash(b));
        }),
        ("sha256", |b| {
            std::hint::black_box(sha2::Sha256::digest(b));
        }),
    ];
    for (name, run) in cases {
        let mut best = f64::MAX;
        for _ in 0..repeat {
            let start = Instant::now();
            run(&bytes);
            best = best.min(start.elapsed().as_secs_f64());
        }
        println!(
            "  {name:<12} {:>12.1} {:>14.0} {:>16.2}",
            best * 1e3,
            bytes.len() as f64 / 1e6 / best,
            best * SEGMENT_BYTES / bytes.len() as f64
        );
    }
    println!();
}

/// Part 3. Doc 12.2 step 6, building and signing a day's manifest.
///
/// A day folder holds about 3100 files, and the manifest is rebuilt, digested
/// and signed on every commit rather than appended to, because the digest
/// covers the whole document. Doc 12.6 batches into one commit per 32 files, so
/// a day is about 97 rebuilds of a manifest that grows to 3100 entries, and the
/// question is whether that quadratic shape matters. At 3100 entries it should
/// not. This is where we find out.
fn manifests(repeat: usize) {
    println!("part 3: the day manifest, doc 12.2 step 6 budgets 2 s");
    println!(
        "  {:<24} {:>10} {:>12} {:>14}",
        "files in the manifest", "bytes", "build ms", "sign ms"
    );

    let key = SigningKey::from_seed(Role::Publishing, [5u8; 32]);
    for count in [32usize, 512, FILES_A_DAY] {
        let mut best_build = f64::MAX;
        let mut best_sign = f64::MAX;
        let mut size = 0usize;
        for _ in 0..repeat {
            let start = Instant::now();
            let day = day_manifest(count);
            let json = day.to_json().expect("json");
            best_build = best_build.min(start.elapsed().as_secs_f64());
            size = json.len();

            let start = Instant::now();
            std::hint::black_box(day.sign(&key).expect("sign"));
            best_sign = best_sign.min(start.elapsed().as_secs_f64());
        }
        println!(
            "  {count:<24} {size:>10} {:>12.2} {:>14.2}",
            best_build * 1e3,
            best_sign * 1e3
        );
    }
    println!();
}

/// Part 4. What a page costs once published, which is doc 12.4's arithmetic.
fn footprint(umi_bytes: u64, parquet_bytes: u64, rows: usize) {
    println!("part 4: bytes a page, doc 12.4 sizes a slice at 300 GB");
    let umi_page = umi_bytes as f64 / rows as f64;
    let parquet_page = parquet_bytes as f64 / rows as f64;
    println!("  {:<24} {:>12} {:>14}", "", "B/page", "vs .umi");
    println!("  {:<24} {umi_page:>12.0} {:>14}", "the .umi segment", "-");
    println!(
        "  {:<24} {parquet_page:>12.0} {:>13.1}%",
        "the Parquet file",
        parquet_page / umi_page * 100.0
    );
    println!(
        "\n  a 300 GB slice holds {:.0} million pages",
        SLICE_BYTES / parquet_page / 1e6
    );
    println!(
        "  the fleet at 750 pages/s fills one in {:.1} days",
        SLICE_BYTES / parquet_page / 750.0 / 86_400.0
    );
    // Doc 12.4 plans on 9 repositories a week. Saying how many the measured
    // size implies is the point of printing this at all.
    let a_week = 750.0 * 604_800.0 * parquet_page / SLICE_BYTES;
    println!("  which is {a_week:.1} repositories a week against doc 12.4's 9");
}

/// Write a sealed sample segment and return its size.
fn write_segment(path: &Path, rows: usize) -> u64 {
    let batch = sample::pages(rows);
    let mut writer = SegmentWriter::create(
        path,
        Create {
            stream: StreamKind::Pages,
            segment_id: [7u8; 16],
            coordinator: [9u8; 32],
            created_ms: CREATED_MS,
            canon_version: 1,
            extractor_version: 1,
            crawl_profile: 0,
        },
        WriterConfig::default(),
    )
    .expect("create");
    writer.push(&batch).expect("push");
    writer.seal().expect("seal");
    std::fs::metadata(path).expect("stat").len()
}

/// A manifest with `count` plausible entries in it.
fn day_manifest(count: usize) -> Manifest {
    let mut day = Manifest::new(
        "open-index/umi-pages-2026w34-03",
        "20260817",
        StreamKind::Pages,
        Some([3u8; 32]),
    );
    for i in 0..count {
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&(i as u64).to_be_bytes());
        let ulid = segment_text(id);
        day.insert(FileEntry {
            path: format!("data/20260817/{ulid}.parquet"),
            bytes: 134_217_728,
            rows: 21_043,
            blake3: [(i % 251) as u8; 32],
            sha256: [(i % 253) as u8; 32],
            segment_ulid: ulid,
            coordinator: "server3".to_owned(),
            extractor: "umi-extract/0.0.1".to_owned(),
            fetched_at_min_ms: CREATED_MS + i as u64 * 90_000,
            fetched_at_max_ms: CREATED_MS + i as u64 * 90_000 + 90_000,
            verification: Verification {
                local: 18_220,
                quorum: 2_401,
                replayed: 402,
                unverified: 20,
            },
        });
    }
    day
}

fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

fn env(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(fallback)
}
