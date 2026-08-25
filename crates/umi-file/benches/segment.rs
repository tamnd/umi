//! What a segment costs, against doc 10.2's budget and doc 16's gate 1.1.
//!
//! Two of doc 10's numbers are claims rather than measurements, and this is
//! where they get settled.
//!
//! The first is size. Doc 10.2 works out a median row of 3833 bytes and then
//! plans on 6 KB a page, which is what the 342 GB of free disk on server1 is
//! budgeted against. If a page costs 9 KB the disk fills in two thirds of the
//! time doc 08.6's GC rule assumes and the whole cache story has to change, so
//! bytes per page is printed per column, not just as a total, because a total
//! that misses tells you nothing about which column to go and look at.
//!
//! The second is the zstd dictionary. Doc 10.6 says the per shoal dictionary
//! is worth more than the compression level. Doc 10.10 says the compressed
//! markdown frame passes straight through into a Parquet page byte for byte
//! and so costs nothing to convert. Both cannot hold, because a Parquet page
//! has nowhere to put our dictionary, and a segment written with one has to be
//! decompressed and recompressed on the way out. So the dictionary is a knob,
//! `Compression::dictionary`, and part 2 measures it in bytes and in CPU. The
//! first run settled it and the default is now off: the dictionary was 5.6
//! times slower to write and produced a file 3.5 percent larger, so there was
//! never a trade to make. Part 2 stays because the day someone changes the
//! sample rows or the level, that has to be rechecked rather than assumed.
//!
//! Four parts:
//!
//! 1. Write throughput, against gate 1.1's 250 pages a second. Doc 10.8
//!    budgets the encode at about 120 ms of single core work per shoal.
//! 2. The dictionary, on and off, in bytes and in CPU.
//! 3. Bytes per page per column, against doc 10.2.
//! 4. Read throughput, which is doc 12's conversion budget of 30 seconds a
//!    segment, and the projection case doc 15's dashboard needs.
//!
//! Environment:
//!
//! * `UMI_BENCH_ROWS` how many page rows to write, default 20000
//! * `UMI_BENCH_REPEAT` how many times to repeat the timed sections, default 3
//!
//! On a 2 vCPU server the write side is what to watch. Run it pinned so the
//! number means something:
//!
//! ```text
//! cargo build --release -p umi-file --benches
//! taskset -c 1 ./target/release/deps/segment-<hash>
//! ```

use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::Instant;

use arrow::array::RecordBatch;
use umi_file::{Compression, Create, Segment, SegmentWriter, StreamKind, WriterConfig, sample};

/// A made up creation time, because doc 11.1 says nothing in the output path
/// reads a clock and a benchmark that wrote a real one would not be comparable
/// between two runs.
const CREATED_MS: u64 = 1_760_000_000_000;

fn main() {
    let rows = env("UMI_BENCH_ROWS", 20_000);
    let repeat = env("UMI_BENCH_REPEAT", 3);

    println!("umi-file, {rows} page rows, best of {repeat}\n");
    let batch = sample::pages(rows);
    let logical = logical_bytes(&batch);
    println!(
        "arrow in memory: {:.1} MiB, {:.0} B/page\n",
        logical as f64 / (1024.0 * 1024.0),
        logical as f64 / rows as f64
    );

    write_throughput(&batch, repeat);
    dictionary(&batch, repeat);
    per_column(&batch);
    read_throughput(&batch, repeat);
}

/// Part 1. Pages a second on the write side, against gate 1.1.
///
/// Gate 1.1 is 250 pages a second per server and doc 01 gives the box 2 vCPU,
/// with extraction wanting most of one. So the number to look at is not the
/// pages a second, which will be large, it is the share of one core that 250
/// pages a second would take. Doc 10.8 budgets a shoal at about 120 ms of
/// single core work, and a 16384 row shoal at 250 pages a second arrives once
/// a minute, so the budget is roughly 0.2 percent of a core. Anything under a
/// couple of percent is fine and anything over ten is a problem.
fn write_throughput(batch: &RecordBatch, repeat: usize) {
    println!("part 1: write throughput");
    println!(
        "  {:<22} {:>12} {:>12} {:>14}",
        "config", "pages/s", "MB/s", "core at 250/s"
    );

    for (label, config) in [
        ("default, 256 MB", WriterConfig::default()),
        (
            "memory floor, 64 MB",
            WriterConfig::for_memory(WriterConfig::FLOOR_BUDGET),
        ),
        (
            "with dictionary",
            WriterConfig {
                compression: Compression {
                    dictionary: true,
                    ..Compression::default()
                },
                ..WriterConfig::default()
            },
        ),
    ] {
        let mut best = f64::MAX;
        let mut bytes = 0u64;
        for _ in 0..repeat {
            let (elapsed, written) = time_write(batch, config);
            best = best.min(elapsed);
            bytes = written;
        }
        let rows = batch.num_rows() as f64;
        let pages_per_s = rows / best;
        println!(
            "  {label:<22} {pages_per_s:>12.0} {:>12.1} {:>13.2}%",
            bytes as f64 / 1e6 / best,
            250.0 / pages_per_s * 100.0
        );
    }
    println!();
}

/// Part 2. Doc 10.6's dictionary against doc 10.10's pass through.
///
/// The question is not which one compresses better in the abstract. It is
/// whether the bytes the dictionary saves on disk are worth the CPU that doc
/// 12's converter has to spend undoing it, given that the file lives for ten
/// minutes and the Parquet lives forever. Two numbers decide it: the size
/// difference, and the cost of decompressing every text column, which is what
/// a converter that cannot pass the frames through has to pay.
fn dictionary(batch: &RecordBatch, repeat: usize) {
    println!("part 2: the per shoal zstd dictionary");
    println!(
        "  {:<14} {:>12} {:>12} {:>12} {:>14}",
        "dictionary", "bytes", "B/page", "write ms", "decode ms"
    );

    let mut sizes = Vec::new();
    for on in [true, false] {
        let config = WriterConfig {
            compression: Compression {
                dictionary: on,
                ..Compression::default()
            },
            ..WriterConfig::default()
        };
        let mut write = f64::MAX;
        let mut bytes = 0u64;
        for _ in 0..repeat {
            let (elapsed, written) = time_write(batch, config);
            write = write.min(elapsed);
            bytes = written;
        }

        let dir = tempdir();
        let path = dir.path().join("dict.umi");
        write_to(&path, batch, config);
        let mut decode = f64::MAX;
        for _ in 0..repeat {
            decode = decode.min(time_decode_text(&path));
        }

        sizes.push(bytes);
        println!(
            "  {:<14} {bytes:>12} {:>12.0} {:>12.1} {:>14.1}",
            if on { "on" } else { "off" },
            bytes as f64 / batch.num_rows() as f64,
            write * 1e3,
            decode * 1e3
        );
    }

    let (on, off) = (sizes[0] as f64, sizes[1] as f64);
    let saved = (off - on) / off * 100.0;
    if saved > 0.0 {
        println!("  the dictionary saves {saved:.1}% of the segment, which is what doc 10.10's");
        println!("  byte for byte Parquet pass through would be given up to keep.");
    } else {
        println!(
            "  the dictionary costs {:.1}% more bytes and buys nothing, so the",
            -saved
        );
        println!("  default is off and doc 10.10's pass through holds.");
    }
    println!();
}

/// Part 3. Where the bytes actually go, against doc 10.2's 6 KB a page.
///
/// Printed sorted by cost, because the useful reading of this table is the
/// first five lines. Doc 10.2 expects markdown to dominate at roughly 1.4 KB a
/// page compressed and links to come second, and if anything else is above
/// them something is encoded wrong.
fn per_column(batch: &RecordBatch) {
    println!("part 3: bytes per page per column, doc 10.2 budgets 6000");

    let dir = tempdir();
    let path = dir.path().join("columns.umi");
    write_to(&path, batch, WriterConfig::default());
    let segment = Segment::open(&path).expect("open");

    let mut per_column: BTreeMap<String, usize> = BTreeMap::new();
    let mut rows = 0usize;
    for i in 0..segment.shoals() {
        let shoal = segment.shoal(i).expect("shoal");
        rows += shoal.rows();
        for name in leaf_names(&segment) {
            if let Ok(chunk) = shoal.column(&name) {
                *per_column.entry(name).or_default() += chunk.encoded_bytes();
            }
        }
    }

    let mut ranked: Vec<_> = per_column.into_iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let total: usize = ranked.iter().map(|(_, bytes)| *bytes).sum();

    println!(
        "  {:<28} {:>12} {:>10} {:>8}",
        "column", "bytes", "B/page", "share"
    );
    for (name, bytes) in ranked.iter().take(14) {
        println!(
            "  {name:<28} {bytes:>12} {:>10.1} {:>7.1}%",
            *bytes as f64 / rows as f64,
            *bytes as f64 / total as f64 * 100.0
        );
    }
    let rest: usize = ranked.iter().skip(14).map(|(_, bytes)| *bytes).sum();
    println!(
        "  {:<28} {rest:>12} {:>10.1} {:>7.1}%",
        format!("{} more columns", ranked.len().saturating_sub(14)),
        rest as f64 / rows as f64,
        rest as f64 / total as f64 * 100.0
    );

    let file = std::fs::metadata(&path).expect("metadata").len();
    println!(
        "\n  column data {:.0} B/page, whole file {:.0} B/page, doc 10.2 budgets 6000",
        total as f64 / rows as f64,
        file as f64 / rows as f64
    );
    println!(
        "  at 250 pages/s that is {:.1} GB a day against doc 01's 342 GB free\n",
        file as f64 / rows as f64 * 250.0 * 86_400.0 / 1e9
    );
}

/// Part 4. The read side, which is doc 12's converter.
///
/// Doc 12 gives the converter 30 seconds per segment, and a segment is around
/// 128 MB, so a full decode has to run at better than 5 MB/s to leave any room
/// for the Parquet write. The projection line is doc 15's dashboard: counting
/// status codes should not touch the markdown.
fn read_throughput(batch: &RecordBatch, repeat: usize) {
    println!("part 4: read throughput");

    let dir = tempdir();
    let path = dir.path().join("read.umi");
    write_to(&path, batch, WriterConfig::default());
    let bytes = std::fs::metadata(&path).expect("metadata").len();
    let rows = batch.num_rows() as f64;

    /// One way of reading a segment back, so the four of them can go in a
    /// list rather than in four copies of the timing loop.
    type Case = fn(&std::path::Path) -> usize;

    let cases: [(&str, Case); 4] = [
        ("open and read footer", |path| {
            let segment = Segment::open(path).expect("open");
            segment.shoals()
        }),
        ("verify every checksum", |path| {
            let segment = Segment::open(path).expect("open");
            let mut n = 0;
            for i in 0..segment.shoals() {
                let shoal = segment.shoal(i).expect("shoal");
                shoal.verify().expect("checksums");
                n += shoal.rows();
            }
            n
        }),
        ("project url and status", |path| {
            let segment = Segment::open(path).expect("open");
            let mut n = 0;
            for i in 0..segment.shoals() {
                let shoal = segment.shoal(i).expect("shoal");
                n += shoal
                    .to_arrow(&["url", "status"])
                    .expect("to_arrow")
                    .num_rows();
            }
            n
        }),
        ("full decode to arrow", |path| {
            let segment = Segment::open(path).expect("open");
            let mut n = 0;
            for i in 0..segment.shoals() {
                let shoal = segment.shoal(i).expect("shoal");
                n += shoal.to_arrow(&[]).expect("to_arrow").num_rows();
            }
            n
        }),
    ];

    println!(
        "  {:<24} {:>12} {:>12} {:>16}",
        "case", "ms", "pages/s", "s per 128 MB"
    );
    for (label, run) in cases {
        let mut best = f64::MAX;
        for _ in 0..repeat {
            let at = Instant::now();
            black_box(run(&path));
            best = best.min(at.elapsed().as_secs_f64());
        }
        println!(
            "  {label:<24} {:>12.1} {:>12.0} {:>16.2}",
            best * 1e3,
            rows / best,
            best * 128e6 / bytes as f64
        );
    }
    println!();
}

fn time_write(batch: &RecordBatch, config: WriterConfig) -> (f64, u64) {
    let dir = tempdir();
    let path = dir.path().join("bench.umi");
    let at = Instant::now();
    write_to(&path, batch, config);
    let elapsed = at.elapsed().as_secs_f64();
    let bytes = std::fs::metadata(&path).expect("metadata").len();
    (elapsed, bytes)
}

fn write_to(path: &std::path::Path, batch: &RecordBatch, config: WriterConfig) {
    let mut writer = SegmentWriter::create(
        path,
        Create {
            stream: StreamKind::Pages,
            segment_id: [1u8; 16],
            coordinator: [2u8; 32],
            created_ms: CREATED_MS,
            canon_version: 1,
            extractor_version: 1,
            crawl_profile: 0,
        },
        config,
    )
    .expect("create");
    writer.push(batch).expect("push");
    writer.seal().expect("seal");
}

/// What doc 12's converter pays if it cannot pass the compressed frames
/// through: every text column decompressed, which is where the dictionary
/// shows up as CPU rather than as bytes.
fn time_decode_text(path: &std::path::Path) -> f64 {
    let segment = Segment::open(path).expect("open");
    let at = Instant::now();
    for i in 0..segment.shoals() {
        let shoal = segment.shoal(i).expect("shoal");
        for name in ["markdown", "title", "description", "links.item.href"] {
            if let Ok(chunk) = shoal.column(name) {
                black_box(chunk.decode().expect("decode"));
            }
        }
    }
    at.elapsed().as_secs_f64()
}

/// The leaf column names a segment carries, which is the flattened schema
/// rather than the 32 top level fields.
fn leaf_names(segment: &Segment) -> Vec<String> {
    let mut names = Vec::new();
    if let Ok(shoal) = segment.shoal(0) {
        for name in umi_file::column::leaf_names(segment.schema()) {
            if shoal.column(&name.0).is_ok() {
                names.push(name.0);
            }
        }
    }
    names
}

/// Arrow's own idea of what the batch costs in memory, which is the number the
/// compression ratio is against.
fn logical_bytes(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}

fn tempdir() -> tempfile::TempDir {
    tempfile::TempDir::new().expect("tempdir")
}

fn env(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}
