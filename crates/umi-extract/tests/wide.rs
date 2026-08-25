//! The wide golden corpus: ten thousand real pages, from doc 16's gate 1.2.
//!
//! The corpus next to this one is twenty three documents, each chosen because it
//! breaks an extractor in a particular way, and its expected output is checked
//! into the repository as readable markdown. That corpus is good at the failures
//! somebody already thought of. It is no good at all at the ones nobody did.
//!
//! This is the other half of doc 11.10: ten thousand pages nobody picked, off
//! three hundred real hosts, in the encodings and the broken markup the web
//! actually has. Doc 11.1 promises byte identical extraction on every machine
//! forever, and doc 06 makes that promise load bearing, because a fetcher
//! somebody else runs is trusted by comparing its extraction against ours. A one
//! in ten thousand divergence is indistinguishable from a dishonest fetcher, so
//! one in ten thousand is the rate this has to be measured at.
//!
//! # Where the pages are
//!
//! Not here. The corpus is 1.5 GB of HTML, which is not something to put in a
//! git repository, so it is published as a dataset and only the digests are
//! checked in:
//!
//! ```text
//! https://huggingface.co/datasets/open-index/umi-golden
//! ```
//!
//! Download `wide.parquet`, point `UMI_GOLDEN_CORPUS` at it, and run:
//!
//! ```text
//! UMI_GOLDEN_CORPUS=/path/to/wide.parquet \
//!   cargo test -p umi-extract --test wide -- --ignored --nocapture
//! ```
//!
//! The test is `#[ignore]` so that a plain `cargo test` does not fail on a
//! machine that has not downloaded a 28 MB file, and so that the skip is visible
//! in the output as an ignored test rather than as a silent pass. `scripts/
//! build-golden-corpus.sh` is how the file itself was made, out of real crawl
//! output, by a query that produces the same ten thousand pages every time.
//!
//! # Why the digests are truncated
//!
//! Each line holds the first sixteen bytes of three blake3 digests: the input,
//! the markdown and the plain text. Full digests would make this file three
//! megabytes; sixteen bytes makes it one. A hundred and twenty eight bits is far
//! past the point where two of ten thousand documents collide by accident, and
//! accident is the only thing this file is up against. Nobody gains anything by
//! forging an extraction that matches a truncated digest of an extraction they
//! would have to already have.
//!
//! # Blessing
//!
//! ```text
//! UMI_BLESS=1 UMI_GOLDEN_CORPUS=... cargo test -p umi-extract --test wide -- --ignored
//! ```
//!
//! Doc 11.10 has two cases and only two: a deliberate major version bump, with
//! the digests updated in the same commit, or a bug. A blessed diff in a pull
//! request needs a sentence saying which.

use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

use arrow::array::{Array as _, BinaryArray, StringArray};
use arrow::datatypes::DataType;
use umi_extract::extract;
use url::Url;

/// Where the recorded digests live.
fn recorded() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("golden")
        .join("wide.txt")
}

/// The first sixteen bytes of a blake3, as hex. See the module docs.
fn short(bytes: &[u8]) -> String {
    hex::encode(&blake3::hash(bytes).as_bytes()[..16])
}

/// One page: what went in, and the two digests doc 06 compares.
fn line(url: &str, body: &[u8]) -> String {
    // A URL that will not parse is not a reason to stop. The corpus comes off a
    // real crawl and the point of the exercise is to run whatever the web
    // handed over, so an unparseable one becomes a fixed base and the document
    // is still extracted. Nothing about the markdown depends on the base except
    // link resolution, and links off a page with no usable URL are not
    // resolvable anyway.
    let parsed = Url::parse(url).unwrap_or_else(|_| {
        Url::parse("https://corpus.invalid/unparseable").expect("the fallback parses")
    });
    let page = extract(body, &parsed);
    format!(
        "{} md={} text={}\n",
        short(body),
        short(page.markdown.as_bytes()),
        short(page.text().as_bytes()),
    )
}

#[test]
#[ignore = "needs UMI_GOLDEN_CORPUS, see the module documentation"]
fn ten_thousand_real_pages_extract_to_the_recorded_bytes() {
    let Some(path) = std::env::var_os("UMI_GOLDEN_CORPUS") else {
        panic!(
            "UMI_GOLDEN_CORPUS is not set. Download wide.parquet from \
             https://huggingface.co/datasets/open-index/umi-golden and point it at the file."
        );
    };
    let path = PathBuf::from(path);
    let bless = std::env::var_os("UMI_BLESS").is_some();

    // The corpus digest goes in the header and is checked before anything else,
    // because ten thousand mismatched lines against the wrong corpus is a
    // confusing way to be told the wrong file was downloaded.
    //
    // The path is reported when it does not open, because cargo runs an
    // integration test with the package directory as the working directory
    // rather than the workspace root, and a relative path that looks right
    // from a shell resolves somewhere else here. Say which file was tried.
    let corpus_digest = {
        let bytes = fs::read(&path).unwrap_or_else(|error| {
            panic!(
                "UMI_GOLDEN_CORPUS points at {}, which did not read: {error}. \
                 Relative paths resolve against {}, so prefer an absolute one.",
                path.display(),
                std::env::current_dir()
                    .unwrap_or_else(|_| PathBuf::from("."))
                    .display(),
            )
        });
        hex::encode(blake3::hash(&bytes).as_bytes())
    };

    let mut pages = Vec::new();
    for batch in read(&path) {
        let (urls, bodies) = columns(&batch);
        for row in 0..batch.num_rows() {
            pages.push(line(urls.value(row), bodies.value(row)));
        }
    }
    // Sorted, because the row order of a Parquet file is not something to make
    // the digests depend on, and because a sorted file diffs sensibly. The
    // input digest leads every line, so this sorts by document.
    pages.sort();

    let mut out = String::with_capacity(pages.len() * 96 + 256);
    out.push_str(
        "# Written by `UMI_BLESS=1 cargo test -p umi-extract --test wide -- --ignored`.\n\
         # See crates/umi-extract/tests/wide.rs. Three truncated blake3 digests a line:\n\
         # the input html, the markdown, and the plain text doc 11.7 digests.\n",
    );
    writeln!(out, "corpus blake3={corpus_digest}").expect("a string takes writes");
    writeln!(out, "corpus pages={}", pages.len()).expect("a string takes writes");
    for page in &pages {
        out.push_str(page);
    }

    if bless {
        fs::write(recorded(), &out).expect("the digests write");
        eprintln!("blessed {} pages", pages.len());
        return;
    }

    let expected = fs::read_to_string(recorded()).expect("golden/wide.txt reads");
    if expected == out {
        eprintln!("{} pages extracted to the recorded bytes", pages.len());
        return;
    }
    report(&expected, &out);
}

/// Say what diverged, in a form somebody can act on.
///
/// A 10000 line assert_eq is not a failure message, it is a wall. So: the
/// corpus header first, because the wrong corpus explains everything at once,
/// and then the first few documents that differ, by input digest, which is the
/// key to find the page in the Parquet file.
fn report(expected: &str, found: &str) -> ! {
    let head = |text: &str| -> Vec<String> {
        text.lines()
            .filter(|line| line.starts_with("corpus "))
            .map(ToOwned::to_owned)
            .collect()
    };
    assert_eq!(
        head(expected),
        head(found),
        "the corpus is not the one the digests were recorded against"
    );

    let body = |text: &str| -> std::collections::BTreeMap<String, String> {
        text.lines()
            .filter(|line| !line.starts_with('#') && !line.starts_with("corpus "))
            .filter_map(|line| line.split_once(' '))
            .map(|(key, rest)| (key.to_owned(), rest.to_owned()))
            .collect()
    };
    let expected = body(expected);
    let found = body(found);

    let mut diverged = Vec::new();
    for (input, recorded) in &expected {
        match found.get(input) {
            Some(got) if got == recorded => {}
            Some(got) => diverged.push(format!("{input}\n  recorded {recorded}\n  got      {got}")),
            None => diverged.push(format!(
                "{input}\n  recorded {recorded}\n  got      nothing"
            )),
        }
    }
    for input in found.keys() {
        if !expected.contains_key(input) {
            diverged.push(format!(
                "{input}\n  recorded nothing\n  got      a document"
            ));
        }
    }

    let total = diverged.len();
    diverged.truncate(20);
    panic!(
        "{total} of {} documents extracted differently, first {}:\n{}",
        expected.len(),
        diverged.len(),
        diverged.join("\n")
    );
}

/// Every row group of the corpus, as record batches.
fn read(path: &std::path::Path) -> impl Iterator<Item = arrow::record_batch::RecordBatch> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = fs::File::open(path).expect("the corpus opens");
    ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("the corpus is a parquet file")
        .with_batch_size(64)
        .build()
        .expect("the reader builds")
        .map(|batch| batch.expect("a row group reads"))
}

/// The two columns, cast to the one spelling of each this test handles.
///
/// Cast rather than downcast: a Parquet writer is free to hand back `Utf8`,
/// `LargeUtf8` or a view of either, and which one it picks is a detail of the
/// tool that wrote the file rather than anything this test has an opinion on.
fn columns(batch: &arrow::record_batch::RecordBatch) -> (StringArray, BinaryArray) {
    let column = |name: &str, want: &DataType| {
        let index = batch
            .schema()
            .index_of(name)
            .unwrap_or_else(|_| panic!("the corpus has a {name} column"));
        arrow::compute::cast(batch.column(index), want).expect("the column casts")
    };
    let urls = column("url", &DataType::Utf8);
    let bodies = column("body", &DataType::Binary);
    (
        urls.as_any()
            .downcast_ref::<StringArray>()
            .expect("cast to Utf8 produces a StringArray")
            .clone(),
        bodies
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("cast to Binary produces a BinaryArray")
            .clone(),
    )
}
