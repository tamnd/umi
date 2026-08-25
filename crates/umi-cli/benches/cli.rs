//! What `umi cat` and `umi ls` cost, on the two file kinds umi produces.
//!
//! This is not a micro benchmark of JSON encoding. It is the number that
//! answers "can somebody actually work with a published slice", because doc
//! 12.4 sizes a repository at 300 GB and `umi cat` is the command a consumer
//! reaches for first. If it runs at a few thousand rows a second then reading a
//! single segment takes minutes and people will write their own reader instead,
//! which defeats the point of publishing a format anybody can read.
//!
//! Three things get measured. Full row output, which is the worst case and the
//! default. A single column projection, which is what most real questions
//! actually need and is the number that says whether the projection is doing
//! anything. And `ls` over a directory, which reads only footers and metadata
//! and therefore should not scale with row count at all, so if it does, the
//! listing is decoding something it has no business decoding.
//!
//! Run it the way every other bench in this tree is run, pinned, because an
//! unpinned run on a loaded box measures the scheduler:
//!
//! ```text
//! cargo bench -p umi-cli
//! taskset -c 7 chrt --fifo 50 ./target/release/deps/cli-<hash>
//! ```

use std::hint::black_box;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use umi_cli::inspect;
use umi_file::sample::T0;
use umi_file::{Create, SegmentWriter, StreamKind, WriterConfig, sample};

/// Rows per segment. Doc 10.2 seals a shoal at 16384 rows, so this is a
/// segment with more than one shoal in it and the per shoal overhead shows up.
const ROWS: usize = 20_000;

/// A sink that counts bytes and throws them away, so the measurement is the
/// reader and the encoder rather than the terminal or the page cache.
struct Sink(u64);

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn main() {
    let dir = tempfile::tempdir().expect("temp dir");
    let segment = write_segment(dir.path(), ROWS);
    let parquet = convert(&segment, &dir.path().join("pages.parquet"));

    let segment_bytes = std::fs::metadata(&segment).unwrap().len();
    let parquet_bytes = std::fs::metadata(&parquet).unwrap().len();

    println!("\numi cat and umi ls, {ROWS} page rows, best of 3");
    println!(
        "segment {:.1} MiB, parquet {:.1} MiB\n",
        segment_bytes as f64 / (1 << 20) as f64,
        parquet_bytes as f64 / (1 << 20) as f64
    );

    println!("part 1: umi cat, every column, newline delimited JSON");
    println!(
        "{:<22} {:>12} {:>12} {:>12}",
        "file", "ms", "rows/s", "MB/s"
    );
    for (label, path) in [
        ("the .umi segment", &segment),
        ("the Parquet file", &parquet),
    ] {
        let (elapsed, bytes) = best(3, || cat(path, None));
        row(label, elapsed, bytes);
    }

    println!("\npart 2: umi cat --columns url, which is the common question");
    println!(
        "{:<22} {:>12} {:>12} {:>12}",
        "file", "ms", "rows/s", "MB/s"
    );
    for (label, path) in [
        ("the .umi segment", &segment),
        ("the Parquet file", &parquet),
    ] {
        let (elapsed, bytes) = best(3, || cat(path, Some(&["url"])));
        row(label, elapsed, bytes);
    }

    println!("\npart 3: umi ls, which reads footers and never a column");
    let target = dir.path().display().to_string();
    // This prints its listing three times. That is the command doing its job
    // and there is no portable way to silence it without unsafe, so it is left
    // above the number rather than hidden.
    let (elapsed, _) = best(3, || (inspect::ls(&target), 0));
    println!("{:<22} {:>12.2}", "2 files", elapsed.as_secs_f64() * 1000.0);
    println!(
        "which is {:.0} us a file, and a 300 GB slice is about 2400 of them",
        elapsed.as_secs_f64() * 1e6 / 2.0
    );
}

fn row(label: &str, elapsed: std::time::Duration, bytes: u64) {
    let seconds = elapsed.as_secs_f64();
    println!(
        "{:<22} {:>12.1} {:>12.0} {:>12.1}",
        label,
        seconds * 1000.0,
        ROWS as f64 / seconds,
        bytes as f64 / seconds / 1e6
    );
}

/// Run a body three times and keep the fastest, which is the usual way to take
/// the scheduler and the page cache back out of a number.
fn best<T>(times: usize, mut body: impl FnMut() -> (T, u64)) -> (std::time::Duration, u64) {
    let mut fastest = std::time::Duration::MAX;
    let mut bytes = 0;
    for _ in 0..times {
        let start = Instant::now();
        let (value, wrote) = body();
        let elapsed = start.elapsed();
        black_box(value);
        if elapsed < fastest {
            fastest = elapsed;
            bytes = wrote;
        }
    }
    (fastest, bytes)
}

fn cat(path: &Path, columns: Option<&[&str]>) -> ((), u64) {
    let mut sink = Sink(0);
    inspect::cat_into(path, columns, u64::MAX, &mut sink).expect("cat");
    ((), sink.0)
}

fn write_segment(dir: &Path, rows: usize) -> PathBuf {
    let path = dir.join("pages.umi");
    let create = Create {
        stream: StreamKind::Pages,
        segment_id: [3u8; 16],
        coordinator: [4u8; 32],
        created_ms: T0,
        canon_version: 1,
        extractor_version: 4,
        crawl_profile: 0,
    };
    let mut writer = SegmentWriter::create(&path, create, WriterConfig::for_memory(256 << 20))
        .expect("create segment");
    writer
        .push(&sample::batch(StreamKind::Pages, rows))
        .expect("push");
    writer.seal().expect("seal");
    path
}

fn convert(segment: &Path, out: &Path) -> PathBuf {
    let opened = umi_file::Segment::open(segment).expect("open segment");
    umi_publish::convert(&opened, out).expect("convert");
    out.to_owned()
}
