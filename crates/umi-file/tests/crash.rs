//! Doc 16's gate 1.3: a hundred kills and not one corrupt segment accepted.
//!
//! The gate asks for the writer to be killed at a uniformly random byte offset,
//! a hundred times, and for every one of those hundred files to come back as
//! the committed prefix with at most the shoal that was in flight missing.
//! Killing between shoals proves nothing, because that is the case doc 10.7
//! designed the commit record around. The kills that are worth anything are the
//! ones inside a column chunk, inside the directory, inside the 32 byte commit
//! record, and in the window between the two `fdatasync` calls, and a uniform
//! offset is how you reach all four without having to name them.
//!
//! There are two suites here doing the main work, and they do different jobs.
//!
//! The first kills a real process. It spawns a child that writes a segment,
//! watches the file grow, and sends it a kill as soon as it passes the chosen
//! offset. That is the real thing: a real signal, a real process that never
//! runs another instruction, real bytes in the page cache. What it cannot do is
//! land on an offset anybody chose, because the kill is delivered while the
//! child is somewhere inside a write. So it checks something more useful than
//! the offset it aimed at: that the bytes left behind are a prefix of the file
//! the same writer produces when nothing kills it.
//!
//! That prefix property is what makes the second suite legitimate. Once a kill
//! is known to leave a prefix and nothing else, cutting a good file at a chosen
//! offset is not a simulation of a crash, it is the same file. So the second
//! suite takes a hundred uniformly random offsets from a fixed seed and checks
//! every one of them exactly, which the first suite cannot do and which is what
//! the gate actually asks for.
//!
//! The rest cover the filesystem that does not keep its promises, because a
//! torn write is not always a short write. One hands back a block with
//! somebody else's bytes in it instead of a short file. One loses part of a
//! shoal that was already committed, which two `fdatasync` calls are supposed
//! to make impossible and which the commit record's digest catches anyway. One
//! changes a column chunk under a shoal whose commit record is perfectly good,
//! which is a layer further down and is what doc 10.4's per chunk digests are
//! for. "No corrupt segment accepted" has to hold in all of them.
//!
//! Nothing here reads a clock or draws from the system random source. The
//! offsets come out of a seeded generator written into this file, so a failure
//! names an offset that reproduces on any machine, which is the reproducibility
//! the gate asks for.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;

use arrow::array::RecordBatch;
use tempfile::TempDir;
use umi_file::layout::{COMMIT_LEN, HEADER_LEN};
use umi_file::{Create, Error, Segment, SegmentWriter, StreamKind, WriterConfig, sample};

/// The seed. Changing it changes which hundred offsets are covered, so change
/// it when you want a different hundred and not otherwise.
const SEED: u64 = 0x5544_4954_5355_4d49;

/// How many kills. The gate says a hundred and means a hundred.
const KILLS: usize = 100;

/// Rows to a shoal, and shoals to the segment.
///
/// Both are as small as they can be and still mean something. A shoal has to
/// be large enough that a uniformly random offset lands inside one rather than
/// between two, which the suites assert rather than assume, and the segment has
/// to have enough shoals that recovering some and losing the rest is a real
/// distinction. Past that, every extra row is a hundred more encodes in the
/// kill suite and a debug build is where this runs.
const SHOAL_ROWS: usize = 256;

/// See [`SHOAL_ROWS`].
const SHOALS: usize = 8;

/// The environment variable that turns a copy of this test binary into the
/// writer that gets killed. See [`the_writer_that_gets_killed`].
const CHILD: &str = "UMI_CRASH_CHILD_SEGMENT";

/// The name the parent passes to `--exact`.
const CHILD_TEST: &str = "the_writer_that_gets_killed";

fn create() -> Create {
    Create {
        stream: StreamKind::Pages,
        segment_id: [7u8; 16],
        coordinator: [9u8; 32],
        created_ms: sample::T0,
        canon_version: 1,
        extractor_version: 4,
        crawl_profile: 0,
    }
}

fn config() -> WriterConfig {
    WriterConfig {
        shoal_rows: SHOAL_ROWS,
        ..WriterConfig::default()
    }
}

/// A fixed sequence, so a failure names an offset somebody else can reproduce.
///
/// splitmix64, written out here rather than pulled in, because the suite does
/// not need a good generator. It needs the same generator on every machine for
/// as long as the seed above stays put, and a crate that improves its algorithm
/// in a minor release would quietly change which offsets are covered without
/// changing a line of this file.
struct Seeded(u64);

impl Seeded {
    fn draw(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `low..high`. The modulo skews the last few values of a 64 bit
    /// draw towards `low` by about one part in 2^50 at these ranges, which is
    /// not a distribution anybody can measure in a hundred samples.
    fn between(&mut self, low: u64, high: u64) -> u64 {
        low + self.draw() % (high - low)
    }
}

/// Where the writer's committed prefix ended after each shoal.
///
/// `SegmentWriter::bytes_written` is one past the shoal's commit record, which
/// is exactly what recovery is supposed to report as `good_bytes` for any cut
/// from here up to the next shoal's commit record. Taking the boundaries from
/// the writer rather than by scanning the file keeps the expected answer
/// independent of the code being tested.
#[derive(Clone, Copy, Debug)]
struct Boundary {
    committed_at: u64,
    rows: u64,
}

/// The segment every suite works against, and what a correct recovery of any
/// prefix of it looks like.
struct Reference {
    /// Every byte, sealed, as the writer produces it when nothing kills it.
    whole: Vec<u8>,
    /// The rows that went in, in order, for comparing what comes back out.
    rows: RecordBatch,
    /// One per shoal, in order.
    boundaries: Vec<Boundary>,
    /// Kept so the files the reference was built from outlive it.
    _dir: TempDir,
}

impl Reference {
    /// How many shoals a file cut at `len` bytes must recover, and where its
    /// good prefix ends.
    fn expected(&self, len: u64) -> (usize, u64, u64) {
        let shoals = self
            .boundaries
            .iter()
            .take_while(|b| b.committed_at <= len)
            .count();
        let committed_at = if shoals == 0 {
            HEADER_LEN as u64
        } else {
            self.boundaries[shoals - 1].committed_at
        };
        let rows = if shoals == 0 {
            0
        } else {
            self.boundaries[shoals - 1].rows
        };
        (shoals, committed_at, rows)
    }

    /// The largest number of bytes one shoal occupies, frame and commit record
    /// included. Recovery may never throw away more than this, because more
    /// than this is more than one shoal.
    fn widest_shoal(&self) -> u64 {
        let mut previous = HEADER_LEN as u64;
        let mut widest = 0;
        for boundary in &self.boundaries {
            widest = widest.max(boundary.committed_at - previous);
            previous = boundary.committed_at;
        }
        widest
    }
}

/// Write the segment, recording where each commit record ended.
fn write(path: &Path, shoals: usize) -> (Vec<Boundary>, RecordBatch) {
    let rows = sample::pages(shoals * SHOAL_ROWS);
    let mut writer = SegmentWriter::create(path, create(), config()).expect("create");
    let mut boundaries = Vec::with_capacity(shoals);
    for shoal in 0..shoals {
        // Exactly one shoal's worth, so `push` seals it on the way out and the
        // byte count below is a committed one.
        writer
            .push(&rows.slice(shoal * SHOAL_ROWS, SHOAL_ROWS))
            .expect("push");
        assert_eq!(writer.shoals(), shoal + 1, "push did not seal a shoal");
        boundaries.push(Boundary {
            committed_at: writer.bytes_written(),
            rows: writer.rows(),
        });
    }
    (boundaries, rows)
}

/// The reference segment, built once for the whole binary.
///
/// Four suites want the same bytes and building them costs a couple of dozen
/// shoal encodes, so it happens once. Nothing here mutates it.
fn reference() -> &'static Reference {
    static ONCE: OnceLock<Reference> = OnceLock::new();
    ONCE.get_or_init(build)
}

fn build() -> Reference {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("reference.umi");
    let (boundaries, rows) = write(&path, SHOALS);
    // Sealing only appends, so the sealed file has every prefix an unsealed one
    // has and a footer as well. That footer is worth covering: a cut inside it
    // is a file whose trailing magic is there but whose digest is not, and doc
    // 10.9 says that falls back to the commit scan rather than being trusted.
    let sealed = dir.path().join("sealed.umi");
    let mut writer = SegmentWriter::create(&sealed, create(), config()).expect("create sealed");
    for shoal in 0..SHOALS {
        writer
            .push(&rows.slice(shoal * SHOAL_ROWS, SHOAL_ROWS))
            .expect("push");
    }
    writer.seal().expect("seal");
    let whole = fs::read(&sealed).expect("read sealed");
    // The kill suite compares what a killed writer left against a prefix of
    // this, which only means anything if two runs of the same writer produce
    // the same bytes. Doc 11.1 says they do. This is where that is checked.
    assert_eq!(
        fs::read(&path).expect("read reference"),
        whole[..boundaries[SHOALS - 1].committed_at as usize],
        "two runs of the same writer disagreed before the footer"
    );
    Reference {
        whole,
        rows,
        boundaries,
        _dir: dir,
    }
}

/// Everything gate 1.3 asks of one recovered file.
///
/// `len` is how long the file is, and the reference turns that into the one
/// answer a correct recovery may give. `what` names the case, because a
/// failure a hundred iterations into a loop needs to say which iteration.
fn check_recovery(reference: &Reference, path: &Path, len: u64, what: &str) {
    let (shoals, good_bytes, rows) = reference.expected(len);

    // Doc 10.9: an unsealed file is refused by the ordinary open rather than
    // half read. The one exception is a cut that lands after the trailing magic
    // is complete, which cannot happen because the magic is the last four bytes
    // of the file.
    match Segment::open(path) {
        Ok(_) => panic!("{what}: a file of {len} bytes opened as if it were sealed"),
        Err(Error::NotSealed) => {}
        Err(other) => panic!("{what}: a file of {len} bytes failed with {other}, wanted NotSealed"),
    }

    let (segment, report) = Segment::open_recover(path).expect("recover");
    assert!(
        !report.sealed,
        "{what}: recovery claimed a torn file was sealed"
    );
    assert_eq!(
        report.shoals as usize, shoals,
        "{what}: {len} bytes recovered {} shoals, {shoals} were committed",
        report.shoals
    );
    assert_eq!(report.rows, rows, "{what}: wrong row count at {len} bytes");
    assert_eq!(
        report.good_bytes, good_bytes,
        "{what}: good prefix at {len} bytes"
    );

    // "At most one shoal lost", in bytes. Everything past the last commit
    // record is thrown away, and if that is ever more than one shoal's worth
    // then a committed shoal went with it.
    let lost = len - good_bytes;
    assert_eq!(report.lost_bytes, lost, "{what}: lost byte count at {len}");
    assert!(
        lost <= reference.widest_shoal(),
        "{what}: {len} bytes lost {lost}, which is more than the widest shoal"
    );

    // Every shoal the recovery claims has to pass its own checksums and hold
    // the rows that went in at that position. A recovery that hands back
    // plausible garbage is worse than one that hands back nothing.
    assert_eq!(
        segment.shoals(),
        shoals,
        "{what}: reader disagrees with report"
    );
    let mut at = 0usize;
    for i in 0..segment.shoals() {
        let shoal = segment.shoal(i).expect("shoal");
        shoal.verify().unwrap_or_else(|e| {
            panic!("{what}: shoal {i} at {len} bytes failed its checksums: {e}")
        });
        let read = shoal
            .to_arrow(&[])
            .unwrap_or_else(|e| panic!("{what}: shoal {i} at {len} bytes did not decode: {e}"));
        assert_eq!(
            read,
            reference.rows.slice(at, shoal.rows()),
            "{what}: shoal {i} at {len} bytes decoded to the wrong rows"
        );
        at += shoal.rows();
    }
    assert_eq!(
        at as u64, rows,
        "{what}: shoals do not add up to the report"
    );
}

#[test]
fn a_hundred_random_offsets_recover_to_the_committed_prefix() {
    let dir = TempDir::new().expect("tempdir");
    let reference = reference();
    let torn = dir.path().join("torn.umi");

    let mut rng = Seeded(SEED);
    let mut inside = 0;
    for _ in 0..KILLS {
        let cut = rng.between(HEADER_LEN as u64, reference.whole.len() as u64);
        let _ = fs::remove_file(&torn);
        fs::write(&torn, &reference.whole[..cut as usize]).expect("write");
        check_recovery(reference, &torn, cut, &format!("cut at {cut}"));
        if cut > reference.expected(cut).1 {
            inside += 1;
        }
    }

    // The gate's own warning, made into an assertion. If a change to the sample
    // rows or the shoal size ever makes the offsets land between shoals, this
    // suite would still pass and would be testing nothing.
    assert!(
        inside * 4 >= KILLS * 3,
        "only {inside} of {KILLS} offsets landed inside a shoal, this suite is not testing much"
    );
    println!("{KILLS} offsets, {inside} of them inside a shoal in flight");
}

#[test]
fn a_torn_tail_of_somebody_elses_bytes_is_not_mistaken_for_a_shoal() {
    // A short file is the common crash, not the only one. A filesystem that
    // allocated the block and died before the data reached it hands back a full
    // length file with whatever was in that block, and doc 10.7's answer is the
    // same either way: the commit record's digest is over the directory, and
    // bytes nobody wrote do not produce one.
    let dir = TempDir::new().expect("tempdir");
    let reference = reference();
    let torn = dir.path().join("garbage-tail.umi");

    let mut rng = Seeded(SEED ^ 0xffff_ffff_ffff_ffff);
    for _ in 0..KILLS {
        let cut = rng.between(HEADER_LEN as u64, reference.whole.len() as u64) as usize;
        let mut bytes = reference.whole[..cut].to_vec();
        // Up to two shoals of noise, so the tail is long enough to reach where
        // a commit record would be and be read as one.
        let tail = rng.between(1, 2 * reference.widest_shoal());
        bytes.extend((0..tail).map(|_| rng.draw() as u8));
        let _ = fs::remove_file(&torn);
        fs::write(&torn, &bytes).expect("write");

        let (shoals, good_bytes, rows) = reference.expected(cut as u64);
        let (segment, report) = Segment::open_recover(&torn).expect("recover");
        assert_eq!(
            report.shoals as usize, shoals,
            "a tail of noise after {cut} bytes was read as {} shoals, {shoals} were committed",
            report.shoals
        );
        assert_eq!(report.rows, rows, "wrong row count after {cut} bytes");
        assert_eq!(
            report.good_bytes, good_bytes,
            "good prefix after {cut} bytes"
        );
        let mut at = 0usize;
        for i in 0..segment.shoals() {
            let shoal = segment.shoal(i).expect("shoal");
            shoal
                .verify()
                .expect("a recovered shoal failed its checksums");
            assert_eq!(
                shoal.to_arrow(&[]).expect("decode"),
                reference.rows.slice(at, shoal.rows()),
                "shoal {i} after {cut} bytes decoded to the wrong rows"
            );
            at += shoal.rows();
        }
    }
}

#[test]
fn recovering_the_good_prefix_twice_finds_nothing_left_to_lose() {
    // "No torn tail surviving." Recovery reports where the intact prefix ends,
    // and doc 10.7 says an operator may truncate there and carry on appending.
    // That is only true if the prefix is itself a whole file, so cutting at
    // `good_bytes` has to leave a file with the same shoals and nothing lost.
    let dir = TempDir::new().expect("tempdir");
    let reference = reference();
    let torn = dir.path().join("twice.umi");

    let mut rng = Seeded(SEED.rotate_left(17));
    for _ in 0..KILLS {
        let cut = rng.between(HEADER_LEN as u64, reference.whole.len() as u64) as usize;
        let _ = fs::remove_file(&torn);
        fs::write(&torn, &reference.whole[..cut]).expect("write");
        let (_, first) = Segment::open_recover(&torn).expect("recover");

        let _ = fs::remove_file(&torn);
        fs::write(&torn, &reference.whole[..first.good_bytes as usize]).expect("write");
        let (_, again) = Segment::open_recover(&torn).expect("recover");
        assert_eq!(
            again.lost_bytes, 0,
            "a recovered prefix still had a torn tail"
        );
        assert_eq!(
            again.shoals, first.shoals,
            "recovery is not idempotent at {cut}"
        );
        assert_eq!(
            again.rows, first.rows,
            "recovery is not idempotent at {cut}"
        );
        assert_eq!(
            again.good_bytes, first.good_bytes,
            "recovery moved at {cut}"
        );
    }
}

#[test]
fn a_kill_before_the_first_row_leaves_a_file_that_says_so() {
    // Doc 10.7 does not promise a header survives, only that a file which
    // cannot be identified is refused rather than guessed at. A cut inside the
    // 4096 byte header is that case, and the only thing to check is that it is
    // an error and not a panic and not an empty segment that reads as valid.
    let dir = TempDir::new().expect("tempdir");
    let reference = reference();
    let torn = dir.path().join("stillborn.umi");

    let mut rng = Seeded(SEED.rotate_right(23));
    for _ in 0..16 {
        let cut = rng.between(0, HEADER_LEN as u64) as usize;
        let _ = fs::remove_file(&torn);
        fs::write(&torn, &reference.whole[..cut]).expect("write");
        assert!(
            Segment::open(&torn).is_err(),
            "a file cut at {cut}, inside the header, opened"
        );
        assert!(
            Segment::open_recover(&torn).is_err(),
            "a file cut at {cut}, inside the header, recovered as if it had a header"
        );
    }

    // The whole header and not one row, which is a crash a second after create
    // and is not an error. It is a segment with nothing in it.
    let _ = fs::remove_file(&torn);
    fs::write(&torn, &reference.whole[..HEADER_LEN]).expect("write");
    let (segment, report) = Segment::open_recover(&torn).expect("an intact header recovers");
    assert_eq!(segment.shoals(), 0);
    assert_eq!(report.rows, 0);
    assert_eq!(report.good_bytes, HEADER_LEN as u64);
    assert_eq!(report.lost_bytes, 0);
}

#[test]
fn a_hundred_real_kills_leave_a_prefix_and_nothing_else() {
    // The one suite that kills a process rather than a file. It is what makes
    // the truncation suites above mean something: they assume a kill leaves a
    // prefix of the bytes a clean run would have written, and this checks that
    // assumption against a real signal a hundred times.
    //
    // `Child::kill` is SIGKILL on unix and TerminateProcess on windows. Neither
    // one gives the child a chance to run another instruction, which is the
    // property that matters, and the writer holds no buffered state in user
    // space anyway.
    let dir = TempDir::new().expect("tempdir");
    let reference = reference();
    let end = reference.boundaries[SHOALS - 1].committed_at;
    let exe = std::env::current_exe().expect("this test binary's own path");

    let mut rng = Seeded(SEED.wrapping_mul(3));
    let mut inside = 0;
    for i in 0..KILLS {
        let path = dir.path().join(format!("kill-{i}.umi"));
        let target = rng.between(HEADER_LEN as u64, end);
        let mut child = Command::new(&exe)
            .args(["--exact", CHILD_TEST, "--ignored", "--nocapture"])
            .env(CHILD, &path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the writer");

        // Watch it grow and kill it as soon as it is past the offset. The kill
        // lands a little further on than that, because the signal is delivered
        // while the child is somewhere inside a write, and that is the point:
        // where it actually lands is not something this test gets to choose.
        loop {
            if fs::metadata(&path).is_ok_and(|m| m.len() >= target) {
                break;
            }
            if child.try_wait().expect("wait").is_some() {
                break;
            }
            // A sleep rather than a spin. The parent and the child are on the
            // same two cores on a runner, and a parent burning one of them is
            // a parent slowing down the writer it is trying to catch. Two
            // hundred microseconds is a fiftieth of the time a shoal takes to
            // encode in a debug build, so the kill still lands inside one.
            std::thread::sleep(std::time::Duration::from_micros(200));
        }
        let _ = child.kill();
        let _ = child.wait();

        let left = fs::read(&path).expect("the killed writer's file reads");
        assert!(
            left.len() <= reference.whole.len() && reference.whole.starts_with(&left),
            "a kill at {target} left {} bytes that are not a prefix of a clean run",
            left.len()
        );
        let len = left.len() as u64;
        check_recovery(reference, &path, len, &format!("killed near {target}"));
        if len > reference.expected(len).1 {
            inside += 1;
        }
        // The temporary directory is one per suite and a hundred segments of a
        // few megabytes is more disk than a test should hold at once.
        let _ = fs::remove_file(&path);
    }

    assert!(
        inside > 0,
        "not one of {KILLS} kills landed inside a shoal, which is the only case worth killing for"
    );
    println!("{KILLS} kills, {inside} of them inside a shoal in flight");
}

#[test]
fn a_shoal_whose_directory_changed_is_not_part_of_the_file() {
    // The commit record carries a digest over the shoal's directory, and this
    // is the case it exists for: the frame is right, the record is right, the
    // shoal is exactly as long as it claims, and one byte of the directory is
    // not what the writer wrote. Doc 10.7's two syncs make that impossible on
    // a filesystem that keeps its promises, which is the reason to check it on
    // one that does not.
    let reference = reference();
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("bent-directory.umi");
    let end = reference.boundaries[SHOALS - 1].committed_at as usize;

    for shoal in 0..SHOALS {
        let mut bytes = reference.whole[..end].to_vec();
        // The directory is the tail of the shoal body, so the byte just before
        // the commit record is the last byte of it.
        let at = reference.boundaries[shoal].committed_at as usize - COMMIT_LEN - 1;
        bytes[at] = bytes[at].wrapping_add(1);
        let _ = fs::remove_file(&path);
        fs::write(&path, &bytes).expect("write");

        let (segment, report) = Segment::open_recover(&path).expect("recover");
        assert_eq!(
            report.shoals as usize, shoal,
            "one bent byte in shoal {shoal}'s directory left {} shoals readable",
            report.shoals
        );
        assert_eq!(segment.shoals(), shoal, "reader disagrees with the report");
        assert_eq!(
            report.good_bytes,
            reference.expected(u64::try_from(at).expect("fits")).1,
            "the good prefix should end before shoal {shoal}"
        );
    }
}

#[test]
fn a_shoal_whose_column_chunk_changed_fails_its_own_checksums() {
    // One layer down. A chunk that changed after the shoal was committed does
    // not touch the directory, so the commit record still verifies and the
    // scan is right to accept the shoal. Doc 10.4 puts a digest on every chunk
    // for exactly this, and `verify` is where a reader asks about it.
    let reference = reference();
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("bent-chunk.umi");
    let end = reference.boundaries[SHOALS - 1].committed_at as usize;

    for shoal in 0..SHOALS {
        let mut bytes = reference.whole[..end].to_vec();
        let (start, stop) = (
            reference
                .expected(reference.boundaries[shoal].committed_at - 1)
                .1 as usize,
            reference.boundaries[shoal].committed_at as usize - COMMIT_LEN,
        );
        // A quarter of the way into the body, which is a column chunk and not
        // the frame at the front or the directory at the back.
        let at = start + (stop - start) / 4;
        bytes[at] = bytes[at].wrapping_add(1);
        let _ = fs::remove_file(&path);
        fs::write(&path, &bytes).expect("write");

        let (segment, report) = Segment::open_recover(&path).expect("recover");
        assert_eq!(
            report.shoals as usize, SHOALS,
            "a bent chunk in shoal {shoal} should not cost the commit record its digest"
        );
        assert!(
            segment.shoal(shoal).expect("shoal").verify().is_err(),
            "shoal {shoal} passed its checksums with a byte changed inside a column chunk"
        );
        for other in (0..SHOALS).filter(|o| *o != shoal) {
            segment
                .shoal(other)
                .expect("shoal")
                .verify()
                .unwrap_or_else(|e| panic!("bending shoal {shoal} broke shoal {other}: {e}"));
        }
    }
}

#[test]
#[ignore = "spawned by a_hundred_real_kills_leave_a_prefix_and_nothing_else, not a test"]
fn the_writer_that_gets_killed() {
    // The child half of the kill suite. Spawning the test binary again with
    // `--exact` is how this gets a separate process without adding a binary
    // target to a library crate that has no business shipping one, and without
    // guessing where cargo put an example.
    //
    // It never seals. A writer that finishes is a writer nobody killed.
    let Some(path) = std::env::var_os(CHILD) else {
        return;
    };
    write(&PathBuf::from(path), SHOALS);
    // Wait to be killed, but not forever. If the parent died before it got to
    // the kill then nothing is ever coming, and a child that waits on that
    // holds the whole test run open.
    std::thread::sleep(std::time::Duration::from_secs(60));
}
