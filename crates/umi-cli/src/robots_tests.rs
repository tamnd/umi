//! Tests for `umi robots`.
//!
//! Nothing here goes to the network. What is worth testing about this command
//! is which hosts it decides to ask and what it does with the answers, and both
//! of those are decisions it makes before and after the fetch. The fetch itself
//! is `umi-fetch` and has its own suite.

use std::collections::HashSet;

use std::sync::atomic::Ordering;

use super::{Admit, Again, Counts, Options, Source, host_of, hosts_in, progress};

#[test]
fn a_list_written_by_a_person_still_reads_as_hosts() {
    let cases = [
        ("example.com", Some("example.com")),
        ("https://example.com", Some("example.com")),
        ("https://example.com/", Some("example.com")),
        ("http://example.com/robots.txt", Some("example.com")),
        ("https://Example.COM/a/b?c=d#e", Some("example.com")),
        ("example.com.", Some("example.com")),
        ("example.com:8443", Some("example.com")),
        ("https://user@example.com/", Some("example.com")),
        ("  example.com  ", Some("example.com")),
        ("# a comment", None),
        ("", None),
        ("   ", None),
        // A single label is a machine on somebody's network. Asking one would
        // send the run's own resolver looking for a host on the box it is
        // running on.
        ("localhost", None),
        ("not a host", None),
    ];
    for (line, want) in cases {
        assert_eq!(
            host_of(line).as_deref(),
            want,
            "{line:?} did not read the way a list means it",
        );
    }
}

#[test]
fn the_same_site_is_only_asked_once() {
    let mut admit = Admit::new(&Options::default(), HashSet::new());
    assert_eq!(admit.take("example.com").as_deref(), Some("example.com"));
    assert_eq!(
        admit.take("https://EXAMPLE.com/robots.txt"),
        None,
        "the same host in another spelling is the same host",
    );
    assert_eq!(
        admit.take("www.example.com").as_deref(),
        Some("www.example.com"),
        "a different host under the same domain is a different file",
    );
}

#[test]
fn skip_and_limit_carve_the_list_into_runs_that_do_not_overlap() {
    let list = ["a.com", "b.com", "c.com", "d.com", "e.com"];
    let first = Options {
        limit: Some(2),
        ..Options::default()
    };
    let second = Options {
        skip: 2,
        limit: Some(2),
        ..Options::default()
    };

    let taken = |options: &Options| {
        let mut admit = Admit::new(options, HashSet::new());
        let mut got = Vec::new();
        for line in list {
            if let Some(host) = admit.take(line) {
                got.push(host);
            }
            if admit.done() {
                break;
            }
        }
        got
    };

    assert_eq!(taken(&first), ["a.com", "b.com"]);
    assert_eq!(
        taken(&second),
        ["c.com", "d.com"],
        "a second run with the skip of the first run's limit picks up where it stopped",
    );
}

#[test]
fn a_blocked_domain_is_not_asked_and_neither_is_anything_under_it() {
    let blocked = HashSet::from(["blocked.example".to_owned()]);
    let mut admit = Admit::new(&Options::default(), blocked);
    assert_eq!(admit.take("blocked.example"), None);
    assert_eq!(
        admit.take("shop.eu.blocked.example"),
        None,
        "doc 07.7 blocks the site and not one host on it",
    );
    assert_eq!(
        admit.take("notblocked.example").as_deref(),
        Some("notblocked.example"),
        "a domain that only ends the same way is a different domain",
    );
}

#[test]
fn a_source_is_a_path_before_it_is_a_repository() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("hosts.txt");
    std::fs::write(&path, "example.com\n").expect("write");

    let named = |source: &str| {
        Source::parse(&Options {
            source: source.to_owned(),
            ..Options::default()
        })
    };

    assert!(matches!(named("-"), Ok(Source::Lines(None))));
    assert!(matches!(
        named(path.to_str().expect("utf8")),
        Ok(Source::Lines(Some(_)))
    ));
    assert!(matches!(
        named("open-index/ccrawl-domains"),
        Ok(Source::Dataset { .. })
    ));
    // A bare word is a mistake and not an organisation to be guessed at. The
    // alternative is a typo turning into a request for somebody else's
    // dataset.
    assert!(named("ccrawl-domains").is_err());
}

#[test]
fn one_column_comes_out_of_a_list_that_has_several() {
    use std::sync::Arc;

    use arrow::array::{StringArray, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let schema = Arc::new(Schema::new(vec![
        Field::new("rank", DataType::UInt32, false),
        Field::new("domain", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                "example.com",
                "example.org",
                "example.net",
            ])),
        ],
    )
    .expect("batch");

    let mut bytes = Vec::new();
    {
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut bytes, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    assert_eq!(
        hosts_in(bytes.clone(), "domain").expect("read"),
        ["example.com", "example.org", "example.net"],
    );
    assert!(
        hosts_in(bytes, "hostname").is_err(),
        "a column that is not there is an error and not an empty run",
    );
}

#[test]
fn the_four_numbers_say_what_the_web_said() {
    let row = |status: u16, body: Option<&str>| umi_crawl::RobotsRow {
        host: "example.com".to_owned(),
        fetched_at_ms: 1,
        status,
        body: body.map(ToOwned::to_owned),
        groups: 0,
        rules: 0,
        crawl_delay_ms: None,
        allows_us: 1,
        sitemaps: Vec::new(),
        content_usage: None,
    };

    let mut counts = Counts::default();
    counts.add(&row(200, Some("user-agent: *\n")));
    counts.add(&row(404, None));
    counts.add(&row(410, None));
    counts.add(&row(503, None));
    counts.add(&row(429, None));
    counts.add(&row(0, None));

    assert_eq!(counts.rules, 1, "one host published rules");
    assert_eq!(counts.none, 2, "a 404 and a 410 both say there is no file");
    assert_eq!(counts.refused, 2, "a 503 and a 429 are both a refusal");
    assert_eq!(counts.silent, 1, "one host never answered at all");
}

#[test]
fn the_run_says_how_much_the_second_ask_is_recovering() {
    // The second ask is a guess about the network and the progress line is
    // where the guess gets checked. A run whose second asks are answering
    // almost nothing is a run that should stop making them, and there is no way
    // to know that from the silent count on its own, because a host recovered
    // by a second ask is not silent any more and leaves no trace.
    let again = Again::default();
    again.asked.fetch_add(10, Ordering::Relaxed);
    again.answered.fetch_add(3, Ordering::Relaxed);

    let line = progress(
        &crate::crawl::Summary::default(),
        &Counts::default(),
        &again,
        1000,
        2000,
    );
    assert!(
        line.contains("3 of 10 second asks answered"),
        "the line does not say what the retry bought: {line}"
    );

    // A run where nothing has gone silent yet still prints the pair, because a
    // number that appears partway through a run is one an operator has to
    // notice rather than read.
    let quiet = progress(
        &crate::crawl::Summary::default(),
        &Counts::default(),
        &Again::default(),
        1000,
        2000,
    );
    assert!(quiet.contains("0 of 0 second asks answered"), "{quiet}");
}
