//! The golden corpus from doc 11.10.
//!
//! Doc 11.1 says extraction produces byte identical output on every machine,
//! forever, and doc 11.10 says the thing that makes that real rather than
//! aspirational is a corpus checked into the repository whose expected output is
//! asserted on every build. This is that test.
//!
//! Every input is a shape that breaks extractors, taken from the list in doc
//! 11.10 and from the ones we have hit since. The expected markdown sits next to
//! each input as a `.md` file, so a change shows up as a readable diff and not
//! as a hash that moved, and `digests.txt` records the blake3 of the markdown
//! and of the plain text because the digests are what doc 06 actually compares
//! between two fetchers.
//!
//! To change the expected output on purpose, run:
//!
//! ```text
//! UMI_BLESS=1 cargo test -p umi-extract --test golden
//! ```
//!
//! and commit the result in the same commit as the change that caused it. Doc
//! 11.10 is explicit that there are only two cases: an intentional major version
//! bump with the golden files updated alongside it, or a bug. There is no third
//! case, so a blessed diff in a pull request needs a sentence saying which one
//! it is.

use std::fs;
use std::path::PathBuf;

use umi_extract::extract;
use url::Url;

fn corpus() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("corpus")
}

/// Every input in the corpus, sorted, because directory order is not stable
/// across filesystems and this test may not depend on it.
fn inputs() -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(corpus())
        .expect("the corpus directory is there")
        .map(|entry| entry.expect("the entry reads").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "html"))
        .map(|path| {
            path.file_stem()
                .expect("an html file has a stem")
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

#[test]
fn every_document_extracts_to_the_recorded_bytes() {
    let bless = std::env::var_os("UMI_BLESS").is_some();
    let names = inputs();
    assert!(!names.is_empty(), "the corpus is empty");

    let mut digests = String::from(
        "# Written by `UMI_BLESS=1 cargo test -p umi-extract --test golden`.\n\
         # md and text are blake3 over the markdown and over the plain text.\n",
    );
    let mut wrong = Vec::new();

    for name in &names {
        let html = fs::read(corpus().join(format!("{name}.html"))).expect("the input reads");
        let url = Url::parse(&format!("https://corpus.example/{name}")).expect("the url parses");
        let page = extract(&html, &url);

        digests.push_str(&format!(
            "{name} md={} text={} declared={} uncertain={} bytes={} density={} share={} raw_share={} dropped={}\n",
            hex::encode(blake3::hash(page.markdown.as_bytes()).as_bytes()),
            hex::encode(page.text_digest()),
            u8::from(page.declared_root),
            u8::from(page.boilerplate_uncertain),
            page.signals.text_bytes,
            page.signals.link_density,
            page.signals.top_node_share,
            page.signals.extracted_share,
            page.signals.dropped_bytes,
        ));

        let expected = corpus().join(format!("{name}.md"));
        if bless {
            fs::write(&expected, &page.markdown).expect("the record writes");
            continue;
        }
        let recorded = fs::read_to_string(&expected).unwrap_or_else(|error| {
            panic!("{name}.md is missing or unreadable ({error}), run with UMI_BLESS=1")
        });
        if recorded != page.markdown {
            wrong.push(name.clone());
            eprintln!("--- {name} expected\n{recorded}\n--- {name} got\n{}\n", page.markdown);
        }
    }

    let recorded_digests = corpus().join("digests.txt");
    if bless {
        fs::write(&recorded_digests, &digests).expect("the digests write");
    } else {
        let recorded = fs::read_to_string(&recorded_digests).expect("digests.txt reads");
        assert_eq!(recorded, digests, "digests.txt does not match");
    }

    assert!(wrong.is_empty(), "extraction changed for {wrong:?}");
}

#[test]
fn extraction_is_the_same_on_a_second_run_in_the_same_process() {
    // The cheap half of doc 16's gate 1.2. Two runs in one process catch
    // anything that depends on allocator addresses or on a map's iteration
    // order. Two runs on two machines is what the gate actually asks for, and
    // that is CI running this test on Linux, macOS and Windows.
    for name in inputs() {
        let html = fs::read(corpus().join(format!("{name}.html"))).expect("the input reads");
        let url = Url::parse(&format!("https://corpus.example/{name}")).expect("the url parses");
        assert_eq!(
            extract(&html, &url),
            extract(&html, &url),
            "{name} extracted differently twice"
        );
    }
}

#[test]
fn plain_text_never_contains_markdown_syntax_we_emitted() {
    // Not a golden assertion, a property. If the serialiser learns a new
    // construct and `plain_text` does not learn to strip it, doc 11.7's content
    // digest starts including markup and every duplicate cluster in the corpus
    // is subtly wrong. This is the test that fails first when that happens.
    for name in inputs() {
        let html = fs::read(corpus().join(format!("{name}.html"))).expect("the input reads");
        let url = Url::parse(&format!("https://corpus.example/{name}")).expect("the url parses");
        let text = extract(&html, &url).text();
        for leftover in ["](http", "```", "| --- |"] {
            assert!(
                !text.contains(leftover),
                "{name} left {leftover:?} in the plain text: {text}"
            );
        }
        assert!(!text.contains('\n'), "{name} left a newline in the plain text");
    }
}
