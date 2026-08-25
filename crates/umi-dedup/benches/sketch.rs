//! What the sketch and the chunk tree cost per page.
//!
//! Doc 11.9 budgets 3 to 8 ms per page per core for the whole of extraction on
//! a 150 KB document, and gives the sketch 0.8 to 1.5 ms of that, second only
//! to the HTML parse. That budget is what makes gate 1.1's 250 pages per second
//! reachable at 1.25 cores of extraction, so it is a number with consequences
//! rather than a target of convenience. This bench is where it gets checked.
//!
//! The document sizes are the ones doc 11.9 and doc 04.5 name. 150 KB of HTML
//! extracts to roughly 20 KB of text, so that is the case the 0.8 to 1.5 ms
//! applies to and it is reported first. The 200 KB text case is the long tail:
//! a documentation page, a legal text, a transcript.
//!
//! The chunk tree is measured over 3 MB, which is doc 04.5's own example, and
//! over 128 MB, which is doc 12.2's segment. Both are pure blake3 throughput
//! and both should land within a few percent of what blake3 does on its own,
//! because everything else this crate adds to them is one byte of domain
//! separation per node.
//!
//! Run it pinned, like every other bench in this tree, because an unpinned run
//! on a loaded box measures the scheduler:
//!
//! ```text
//! cargo bench -p umi-dedup
//! taskset -c 7 chrt --fifo 50 ./target/release/deps/sketch-<hash>
//! ```

use std::hint::black_box;
use std::time::{Duration, Instant};

use umi_dedup::{ChunkTree, Content, Sketch, bands, shingle, text_digest};

/// Doc 11.9's document: 150 KB of HTML extracts to about 20 KB of text.
const TYPICAL: usize = 20_000;

/// The long tail: a documentation page or a transcript.
const LARGE: usize = 200_000;

/// Doc 04.5's own example body.
const BODY: usize = 3 << 20;

fn main() {
    let typical = text_of(TYPICAL);
    let large = text_of(LARGE);
    let body: Vec<u8> = (0..BODY)
        .map(|i| (i as u32).wrapping_mul(2_654_435_761) as u8)
        .collect();

    println!("\nthe doc 11.8 sketch and the doc 04.5 chunk tree, best of 5\n");

    println!("part 1: the sketch, which doc 11.9 budgets at 0.8 to 1.5 ms");
    println!(
        "{:<26} {:>10} {:>10} {:>12} {:>10}",
        "text", "KB", "shingles", "ms", "MB/s"
    );
    for (label, text) in [
        ("a typical page, 20 KB", &typical),
        ("a long page, 200 KB", &large),
    ] {
        let shingles = shingle::shingles(text).count();
        let elapsed = best(5, || black_box(Sketch::of(text)));
        line(label, text.len(), shingles, elapsed);
    }

    println!("\npart 2: where the time goes, on the typical page");
    println!(
        "{:<26} {:>10} {:>10} {:>12} {:>10}",
        "stage", "KB", "shingles", "ms", "MB/s"
    );
    let shingles = shingle::shingles(&typical).count();
    let hashing = best(5, || {
        let mut sum = 0u64;
        for h in shingle::shingles(&typical) {
            sum ^= h;
        }
        black_box(sum)
    });
    line("shingle and xxh3 only", typical.len(), shingles, hashing);
    let whole = best(5, || black_box(Sketch::of(&typical)));
    line("plus 64 minhash, simhash", typical.len(), shingles, whole);
    let digest = best(5, || black_box(text_digest(&typical)));
    line("blake3 of the text", typical.len(), 0, digest);
    let content = best(5, || black_box(Content::of(&typical)));
    line("everything doc 11 wants", typical.len(), shingles, content);

    println!("\npart 3: comparing sketches, which is what doc 06 does in bulk");
    let a = Sketch::of(&typical);
    let b = Sketch::of(&large);
    let pairs = 100_000;
    let jaccard = best(5, || {
        let mut sum = 0.0f32;
        for _ in 0..pairs {
            sum += black_box(&a).jaccard(black_box(&b));
        }
        black_box(sum)
    });
    println!(
        "{:<26} {:>10} {:>10} {:>12.2} {:>10.0}",
        "jaccard",
        "",
        pairs,
        jaccard.as_secs_f64() * 1000.0,
        pairs as f64 / jaccard.as_secs_f64()
    );
    let hamming = best(5, || {
        let mut sum = 0u32;
        for _ in 0..pairs {
            sum += black_box(&a).hamming(black_box(&b));
        }
        black_box(sum)
    });
    println!(
        "{:<26} {:>10} {:>10} {:>12.2} {:>10.0}",
        "hamming",
        "",
        pairs,
        hamming.as_secs_f64() * 1000.0,
        pairs as f64 / hamming.as_secs_f64()
    );
    let banding = best(5, || {
        let mut sum = 0u64;
        for _ in 0..pairs {
            sum ^= bands(black_box(&a))[0];
        }
        black_box(sum)
    });
    println!(
        "{:<26} {:>10} {:>10} {:>12.2} {:>10.0}",
        "8 by 8 banding",
        "",
        pairs,
        banding.as_secs_f64() * 1000.0,
        pairs as f64 / banding.as_secs_f64()
    );

    println!("\npart 4: the chunk tree over a body");
    println!(
        "{:<26} {:>10} {:>10} {:>12} {:>10}",
        "body", "KB", "leaves", "ms", "MB/s"
    );
    let tree = best(5, || black_box(ChunkTree::build(&body)));
    let leaves = ChunkTree::build(&body).count() as usize;
    line("3 MB, doc 04.5's example", body.len(), leaves, tree);
    let plain = best(5, || black_box(blake3::hash(&body)));
    line("blake3 on its own", body.len(), 0, plain);

    let built = ChunkTree::build(&body);
    let root = built.root();
    let count = built.count();
    let proofs = 10_000;
    let path = built.proof(47).expect("doc 04.5 asks for chunk 47");
    let chunk = &body[47 * 16 * 1024..48 * 16 * 1024];
    let verify = best(5, || {
        let mut ok = true;
        for _ in 0..proofs {
            ok &= umi_dedup::verify_chunk(&root, 47, count, black_box(chunk), &path);
        }
        black_box(ok)
    });
    println!(
        "\nverifying chunk 47 against the root costs {:.1} us and {} digests,",
        verify.as_secs_f64() * 1e6 / proofs as f64,
        path.len()
    );
    println!("against 3 MB of transfer to check it the other way.");

    let pages_per_core = 1.0 / whole.as_secs_f64();
    println!(
        "\nthe sketch alone runs {pages_per_core:.0} pages a second on one core, \
         and gate 1.1 asks for 250."
    );
}

fn line(label: &str, bytes: usize, shingles: usize, elapsed: Duration) {
    let seconds = elapsed.as_secs_f64();
    println!(
        "{:<26} {:>10.1} {:>10} {:>12.3} {:>10.0}",
        label,
        bytes as f64 / 1000.0,
        shingles,
        seconds * 1000.0,
        bytes as f64 / seconds / 1e6
    );
}

/// Run a body a few times and keep the fastest, which is the usual way to take
/// the scheduler and the page cache back out of a number.
fn best<T>(times: usize, mut body: impl FnMut() -> T) -> Duration {
    let mut fastest = Duration::MAX;
    for _ in 0..times {
        let start = Instant::now();
        let value = body();
        let elapsed = start.elapsed();
        black_box(value);
        fastest = fastest.min(elapsed);
    }
    fastest
}

/// Prose shaped text: words of varying length, sentences of varying length, and
/// a vocabulary large enough that the shingles are mostly distinct.
///
/// Not real English, and it does not need to be. What the sketch costs is a
/// function of how many shingles there are and how long they are, and both of
/// those match. Real English would make the numbers prettier and no truer.
fn text_of(bytes: usize) -> String {
    let mut out = String::with_capacity(bytes + 64);
    let mut seed = 0x9E37_79B9_7F4A_7C15u64;
    let mut word = 0usize;
    while out.len() < bytes {
        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        let length = 3 + (seed >> 60) as usize;
        for i in 0..length {
            let letter = b'a' + ((seed >> (i * 5)) & 0x0F) as u8;
            out.push(letter as char);
        }
        word += 1;
        // A sentence every dozen words or so, which is what puts punctuation
        // inside the shingles the way real text does.
        if word % 13 == 0 {
            out.push('.');
        }
        out.push(' ');
    }
    out
}
