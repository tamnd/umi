//! Doc 10.5's row, checked against what a real fetch and a real extraction
//! produce rather than against hand written values.

use std::time::Duration;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{UInt8Type, UInt16Type, UInt32Type, UInt64Type};
use bytes::Bytes;
use umi_extract::extract;
use umi_fetch::outcome::{Failure, Page, Stage, Version};
use umi_fetch::{Media, Outcome};
use umi_types::{FetcherId, OutcomeCode, Revalidator, RowKey, Tier, Verification};

use super::page::{PageBuilder, PageRow, SnippetKind};
use super::{Crawled, extract_digest};

/// The moment every row in this file is dated from, since nothing may read a
/// clock.
const T0: u64 = 1_760_000_000_000;

const URL: &str = "https://example.com/article";

/// A page with the things doc 11 looks for: a title, a description, headings,
/// body links, a nav link outside the content root, and enough prose that the
/// boilerplate scorer picks the article.
fn html(body: &str) -> String {
    format!(
        "<html lang='en-GB'><head><title>The Title</title>\
         <meta name='description' content='What the page is about.'>\
         <link rel='canonical' href='https://example.com/article'>\
         </head><body>\
         <nav><a href='/home'>Home</a></nav>\
         <article><h1>The Heading</h1><p>{body}</p>\
         <h2>A Section</h2><p>{body}</p>\
         <p>See <a href='/next'>the next page</a> and \
         <a href='https://elsewhere.example/x' rel='nofollow'>elsewhere</a>.</p>\
         </article></body></html>"
    )
}

/// Sentences long enough that the extractor keeps the article and short enough
/// that a test reads quickly.
fn prose() -> String {
    let mut out = String::new();
    for n in 0..40 {
        out.push_str(&format!(
            "This is sentence number {n} of an ordinary page about an ordinary \
             subject, written at a length a real article is written at. "
        ));
    }
    out
}

fn page_of(body: &str) -> Page {
    let bytes = Bytes::from(body.as_bytes().to_vec());
    Page {
        final_url: URL.to_owned(),
        status: 200,
        version: Version::Http2,
        redirects: Vec::new(),
        headers_kept: vec![
            ("content-type".to_owned(), "text/html".to_owned()),
            ("etag".to_owned(), "\"abc\"".to_owned()),
        ],
        headers_digest: [7u8; 32],
        content_type: Some("text/html; charset=utf-8".to_owned()),
        media: Media::Html,
        body_digest: *blake3::hash(&bytes).as_bytes(),
        body: bytes,
        revalidate: Revalidator::default(),
        elapsed: Duration::from_millis(120),
    }
}

fn crawled<'a>(outcome: &'a Outcome, extracted: Option<&'a umi_extract::Extracted>) -> Crawled<'a> {
    Crawled {
        url: URL,
        keys: RowKey::for_url(URL, None).expect("canonicalise"),
        host: "example.com",
        fetched_at_ms: T0,
        outcome,
        extracted,
        tier_used: Tier::Plain,
        tier_path: &[Tier::Revalidate, Tier::Plain],
        robots_checked_ms: T0 - 3_600_000,
        content_usage: None,
        fetcher_id: FetcherId::LOCAL,
        verification: Verification::Local,
        crawl_profile: 0,
    }
}

/// A whole row from a body, which is what almost every test here starts with.
fn row_of(body: &str) -> PageRow {
    let url = url::Url::parse(URL).expect("parse");
    let extracted = extract(body.as_bytes(), &url);
    let outcome = Outcome::Ok(Box::new(page_of(body)));
    PageRow::build(&crawled(&outcome, Some(&extracted)))
}

#[test]
fn an_ordinary_page_fills_every_column_doc_10_5_says_is_not_null() {
    let row = row_of(&html(&prose()));

    assert_eq!(row.url, URL);
    assert_eq!(row.final_url, None, "no redirect means null, not a copy");
    assert_eq!(row.host, "example.com");
    assert_eq!(row.status, 200);
    assert_eq!(row.outcome, OutcomeCode::Ok);
    assert_eq!(row.tier_used, Tier::Plain.as_u8());
    assert_eq!(row.tier_path, vec![0, 1]);
    assert_eq!(row.fetched_at_ms, T0);
    assert!(row.content_length > 0);
    assert_eq!(row.lang, Some(*b"en\0"), "en-GB keeps only the primary tag");

    // The three digests, and none of them may be zeroes, which is the whole
    // reason this crate was written before the crawl loop was.
    assert_ne!(row.body_digest, [0u8; 32]);
    assert_ne!(row.chunk_root, [0u8; 32]);
    assert_ne!(row.extract_digest, [0u8; 32]);
    assert_ne!(row.text_digest, [0u8; 32]);
    assert!(row.sketch.shingles > 0, "a real page has shingles");
    assert!(row.text_bytes > 1000);

    assert_eq!(row.title.as_deref(), Some("The Title"));
    assert_eq!(row.description.as_deref(), Some("What the page is about."));
    assert!(row.markdown.is_some());
    assert_eq!(row.headings, ["The Heading", "A Section"]);
    assert!(row.link_count >= 3, "two body links and one nav link");
}

#[test]
fn the_snippets_column_repeats_the_dedicated_columns_on_purpose() {
    let row = row_of(&html(&prose()));
    let kinds: Vec<SnippetKind> = row.snippets.iter().map(|s| s.kind).collect();
    assert_eq!(
        kinds,
        [
            SnippetKind::Title,
            SnippetKind::Description,
            SnippetKind::H1,
            SnippetKind::H2
        ],
        "title, description, then the headings in document order"
    );
    // Same strings, because a consumer reading one column and a consumer
    // reading four must get the same answer.
    assert_eq!(row.snippets[0].text, row.title.clone().unwrap());
    assert_eq!(row.snippets[1].text, row.description.clone().unwrap());
    assert_eq!(row.snippets[2].text, row.headings[0]);
}

#[test]
fn a_noindex_page_keeps_its_links_and_loses_its_prose() {
    // Doc 11.4. This is the one place extraction makes a policy decision and
    // getting it backwards would publish content a publisher asked us not to.
    let body = html(&prose()).replace("<head>", "<head><meta name='robots' content='noindex'>");
    let row = row_of(&body);

    assert_eq!(row.markdown, None);
    assert_eq!(row.title, None);
    assert_eq!(row.description, None);
    assert!(row.headings.is_empty());
    assert!(row.snippets.is_empty());
    assert!(!row.links.is_empty(), "noindex is not nofollow");
    assert!(row.link_count > 0);
    // The digest still covers the extraction, because two fetchers have to
    // agree that the page said noindex.
    assert_ne!(row.extract_digest, [0u8; 32]);
}

#[test]
fn a_304_has_no_body_and_still_has_a_row() {
    let outcome = Outcome::NotModified {
        revalidate: Revalidator {
            etag: Some("\"abc\"".to_owned()),
            last_modified_ms: None,
        },
        headers_kept: vec![("etag".to_owned(), "\"abc\"".to_owned())],
        headers_digest: [9u8; 32],
        elapsed: Duration::from_millis(30),
    };
    let row = PageRow::build(&crawled(&outcome, None));

    assert_eq!(row.status, 304);
    assert_eq!(row.outcome, OutcomeCode::NotModified);
    assert_eq!(row.content_length, 0);
    assert_eq!(row.markdown, None);
    assert!(row.links.is_empty());
    assert_eq!(row.headers_kept.len(), 1);
    // Doc 10.5 says the digest columns are not null, so a row with no body
    // carries the digest of no body.
    assert_eq!(row.chunk_root, empty_chunk_root());
    assert_eq!(row.text_bytes, 0);
    assert_eq!(row.sketch.shingles, 0);
    // The body digest slot holds the header digest on a 304, which is the one
    // deviation in this file and is deliberate.
    assert_eq!(row.body_digest, [9u8; 32]);
}

#[test]
fn a_failure_row_says_what_failed_and_nothing_more() {
    let outcome = Outcome::Failed {
        status: Some(503),
        failure: Failure::ServerError,
    };
    let row = PageRow::build(&crawled(&outcome, None));
    assert_eq!(row.status, 503);
    assert_eq!(row.outcome, OutcomeCode::ServerError);
    assert_eq!(row.content_length, 0);
    assert!(row.markdown.is_none());

    // A timeout has no status at all, and zero is the honest answer rather
    // than a made up 5xx.
    let outcome = Outcome::Failed {
        status: None,
        failure: Failure::Timeout(Stage::Read),
    };
    let row = PageRow::build(&crawled(&outcome, None));
    assert_eq!(row.status, 0);
    assert_eq!(row.outcome, OutcomeCode::Timeout);
}

#[test]
fn an_off_domain_redirect_records_where_it_was_going() {
    let outcome = Outcome::RedirectedOffDomain {
        redirects: Vec::new(),
        target: "https://elsewhere.example/x".to_owned(),
        status: 301,
    };
    let row = PageRow::build(&crawled(&outcome, None));
    assert_eq!(row.outcome, OutcomeCode::RedirectedOffHost);
    assert_eq!(row.status, 301);
    assert_eq!(
        row.final_url.as_deref(),
        Some("https://elsewhere.example/x"),
        "the target is the one thing this row exists to carry"
    );
}

#[test]
fn two_error_rows_agree_with_each_other_because_they_have_the_same_content() {
    // Which is none. This reads oddly and is correct: an empty sketch against
    // an empty sketch is doc 11.8's answer, and it is the reason `jaccard`
    // returns zero on an empty pair rather than one.
    let a = Outcome::Failed {
        status: Some(500),
        failure: Failure::ServerError,
    };
    let b = Outcome::Failed {
        status: Some(502),
        failure: Failure::ServerError,
    };
    let a = PageRow::build(&crawled(&a, None));
    let b = PageRow::build(&crawled(&b, None));
    assert_eq!(a.chunk_root, b.chunk_root);
    assert_eq!(a.text_digest, b.text_digest);
    assert!(
        !a.is_near_duplicate_of(&b, 0.77),
        "two empty sketches are not a near duplicate pair, they are two \
         rows with nothing to compare"
    );
}

#[test]
fn the_same_bytes_produce_the_same_row_every_time() {
    // Doc 11.1, and gate 1.2 checks it across three machines. This checks the
    // easier half: twice on one.
    let body = html(&prose());
    assert_eq!(row_of(&body), row_of(&body));
}

#[test]
fn a_page_that_changed_one_paragraph_is_still_a_near_duplicate() {
    let one = row_of(&html(&prose()));
    let changed = prose().replace("sentence number 3 ", "sentence number three ");
    let two = row_of(&html(&changed));
    assert_ne!(one.text_digest, two.text_digest, "not an exact duplicate");
    assert!(
        one.is_near_duplicate_of(&two, 0.77),
        "one edited sentence in forty should clear doc 11.7's threshold, got {}",
        one.sketch.jaccard(&two.sketch)
    );
}

#[test]
fn two_different_pages_are_not_near_duplicates() {
    let one = row_of(&html(&prose()));
    let other: String = (0..40)
        .map(|n| format!("A completely different remark, the {n}th of them, about weather. "))
        .collect();
    let two = row_of(&html(&other));
    assert!(!one.is_near_duplicate_of(&two, 0.77));
}

#[test]
fn the_extract_digest_moves_when_the_extraction_does_and_not_otherwise() {
    let url = url::Url::parse(URL).expect("parse");
    let body = html(&prose());
    let base = extract(body.as_bytes(), &url);

    // The same bytes twice.
    assert_eq!(
        extract_digest(&base),
        extract_digest(&extract(body.as_bytes(), &url))
    );

    // A different title is a different extraction.
    let retitled = body.replace("The Title", "Another Title");
    assert_ne!(
        extract_digest(&base),
        extract_digest(&extract(retitled.as_bytes(), &url))
    );

    // So is a different link, even with identical prose.
    let relinked = body.replace("/next", "/after");
    assert_ne!(
        extract_digest(&base),
        extract_digest(&extract(relinked.as_bytes(), &url))
    );

    // Whitespace between tags is not, because doc 11.3 normalises it away and
    // two servers minifying differently must still agree.
    let respaced = body.replace("</p><h2>", "</p>\n\n   <h2>");
    assert_eq!(
        extract_digest(&base),
        extract_digest(&extract(respaced.as_bytes(), &url))
    );
}

#[test]
fn the_extract_digest_cannot_be_fooled_by_moving_a_byte_between_fields() {
    // The length prefix trap. Without it a title of "ab" and a description of
    // "c" would hash the same as a title of "a" and a description of "bc",
    // which is a free way to forge agreement between two receipts.
    let url = url::Url::parse(URL).expect("parse");
    let one = html(&prose())
        .replace("The Title", "ab")
        .replace("What the page is about.", "c");
    let two = html(&prose())
        .replace("The Title", "a")
        .replace("What the page is about.", "bc");
    assert_ne!(
        extract_digest(&extract(one.as_bytes(), &url)),
        extract_digest(&extract(two.as_bytes(), &url))
    );
}

#[test]
fn an_absent_field_and_an_empty_one_digest_differently() {
    let url = url::Url::parse(URL).expect("parse");
    let with = html(&prose());
    let without = with.replace(
        "<meta name='description' content='What the page is about.'>",
        "",
    );
    let a = extract(with.as_bytes(), &url);
    let b = extract(without.as_bytes(), &url);
    assert_ne!(extract_digest(&a), extract_digest(&b));
}

#[test]
fn language_tags_that_are_not_a_primary_subtag_are_dropped_rather_than_cut() {
    let url = url::Url::parse(URL).expect("parse");
    let cases = [
        ("en", Some(*b"en\0")),
        ("en-GB", Some(*b"en\0")),
        ("fil", Some(*b"fil")),
        // The script subtag goes, because doc 10.5 asks for the primary
        // subtag and nothing else. This does lose the traditional versus
        // simplified distinction, which is a real one, and doc 11.6's detected
        // language does not recover it either. Recorded here rather than in a
        // comment nobody reads: `lang` answers "roughly what language" and not
        // "which orthography".
        ("zh-Hant", Some(*b"zh\0")),
        // Not a subtag at all, so it is dropped rather than padded into
        // something that looks like one.
        ("x", None),
        ("abcd", None),
        ("12", None),
    ];
    for (tag, want) in cases {
        let body = html(&prose()).replace("lang='en-GB'", &format!("lang='{tag}'"));
        let extracted = extract(body.as_bytes(), &url);
        let outcome = Outcome::Ok(Box::new(page_of(&body)));
        let row = PageRow::build(&crawled(&outcome, Some(&extracted)));
        assert_eq!(row.lang, want, "for lang={tag}");
    }
}

#[test]
fn the_batch_matches_doc_10_5_for_every_shape_of_row() {
    // The point of this one is `RecordBatch::try_new`, which refuses a batch
    // whose columns do not match the schema exactly, names and nullability
    // included. Everything below is one row of each shape a crawl produces.
    let mut builder = PageBuilder::new();
    let ok = row_of(&html(&prose()));
    builder.push(&ok);

    let noindex =
        row_of(&html(&prose()).replace("<head>", "<head><meta name='robots' content='noindex'>"));
    builder.push(&noindex);

    let not_modified = PageRow::build(&crawled(
        &Outcome::NotModified {
            revalidate: Revalidator::default(),
            headers_kept: Vec::new(),
            headers_digest: [1u8; 32],
            elapsed: Duration::from_millis(9),
        },
        None,
    ));
    builder.push(&not_modified);

    builder.push(&PageRow::build(&crawled(&Outcome::Gone, None)));
    builder.push(&PageRow::build(&crawled(
        &Outcome::Failed {
            status: None,
            failure: Failure::Dns,
        },
        None,
    )));

    assert_eq!(builder.rows(), 5);
    let batch = builder.finish();
    assert_eq!(batch.num_rows(), 5);
    assert_eq!(batch.num_columns(), 32, "doc 10.5 has 32 columns");
    assert_eq!(batch.schema(), umi_file::StreamKind::Pages.arrow());
}

#[test]
fn the_columns_hold_what_the_rows_held() {
    // A batch that type checks is not the same as a batch that is correct, so
    // this reads a handful of values back out, including the awkward ones: a
    // null in a nullable column, an empty list next to a full one, and a map.
    let mut builder = PageBuilder::new();
    let ok = row_of(&html(&prose()));
    let failed = PageRow::build(&crawled(
        &Outcome::Failed {
            status: Some(404),
            failure: Failure::NotFound,
        },
        None,
    ));
    builder.push(&ok);
    builder.push(&failed);
    let batch = builder.finish();

    let urls = batch.column(0).as_string::<i32>();
    assert_eq!(urls.value(0), URL);

    let final_urls = batch.column(1).as_string::<i32>();
    assert!(final_urls.is_null(0), "no redirect is null and not empty");

    let status = batch.column(6).as_primitive::<UInt16Type>();
    assert_eq!(status.value(0), 200);
    assert_eq!(status.value(1), 404);

    let outcome = batch.column(7).as_primitive::<UInt8Type>();
    assert_eq!(outcome.value(0), OutcomeCode::Ok.as_u8());
    assert_eq!(outcome.value(1), OutcomeCode::NotFound.as_u8());

    let tier_path = batch.column(9).as_list::<i32>();
    assert_eq!(tier_path.value_length(0), 2);

    let markdown = batch.column(16).as_string::<i32>();
    assert!(!markdown.is_null(0));
    assert!(markdown.is_null(1), "a 404 has no markdown");

    let links = batch.column(21).as_list::<i32>();
    assert!(links.value_length(0) > 0);
    assert_eq!(links.value_length(1), 0, "an empty list is not a null list");
    assert!(!links.is_null(1));

    let headers = batch.column(22).as_map();
    assert_eq!(headers.value_length(0), 2);
    assert_eq!(headers.value_length(1), 0);

    let minhash = batch.column(24).as_fixed_size_binary();
    assert_eq!(minhash.value(0), ok.sketch.to_bytes());

    let simhash = batch.column(25).as_primitive::<UInt64Type>();
    assert_eq!(simhash.value(0), ok.sketch.simhash);

    let text_bytes = batch.column(26).as_primitive::<UInt32Type>();
    assert_eq!(text_bytes.value(0), ok.text_bytes);
    assert_eq!(text_bytes.value(1), 0);

    let robots = batch.column(30).as_primitive::<UInt64Type>();
    assert_eq!(robots.value(0), T0 - 3_600_000);
}

#[test]
fn the_builder_counts_the_bytes_it_is_given() {
    let row = row_of(&html(&prose()));
    let mut builder = PageBuilder::new();
    assert_eq!(builder.bytes(), 0);
    assert!(!builder.is_full());

    builder.push(&row);
    assert_eq!(builder.bytes(), row.variable_bytes());
    // Not a guess about the encoding, just a floor: the markdown alone has to
    // be in there, so a count that forgot the biggest column would fail here.
    let markdown = row.markdown.as_deref().expect("an ok page has markdown");
    assert!(builder.bytes() >= markdown.len());

    builder.push(&row);
    assert_eq!(builder.bytes(), 2 * row.variable_bytes());
}

#[test]
fn the_builder_seals_on_bytes_before_it_reaches_the_row_limit() {
    // The bug this pins: a shoal of large pages overflows Arrow's 32 bit
    // string offsets long before 16384 rows, and Arrow's answer to that is a
    // panic inside `append_value` with no way for a caller to see it coming.
    // Doc 10.4's byte limit is what keeps the builder away from it, so the
    // byte limit has to be the one that fires first on pages this size.
    let row = row_of(&html(&prose().repeat(8)));
    assert!(row.variable_bytes() > 0);

    let mut builder = PageBuilder::new();
    while !builder.is_full() {
        builder.push(&row);
    }

    assert!(builder.bytes() >= PageBuilder::BYTE_LIMIT);
    assert!(
        builder.rows() < PageBuilder::ROW_LIMIT,
        "sealed at {} rows, which means the row limit fired first and the byte \
         limit was doing nothing",
        builder.rows()
    );
    // The count is exactly the one the limit implies, so a builder that
    // stopped early or ran one row over would show up here rather than as a
    // shoal that is quietly the wrong size.
    let rows = builder.rows();
    assert_eq!(rows, PageBuilder::BYTE_LIMIT.div_ceil(row.variable_bytes()));
    assert_eq!(builder.finish().num_rows(), rows);
}

#[test]
fn a_small_row_seals_on_the_row_limit_instead() {
    // The other half of the rule. A crawl of small pages has to stop at 16384
    // rows rather than accumulate until 32 MiB, or a shoal of redirects would
    // hold millions of rows and no reader would want it.
    let row = row_of(&html("<p>Short.</p>"));
    let mut builder = PageBuilder::new();
    while !builder.is_full() {
        builder.push(&row);
    }
    assert_eq!(builder.rows(), PageBuilder::ROW_LIMIT);
    assert!(builder.bytes() < PageBuilder::BYTE_LIMIT);
}

#[test]
fn an_empty_builder_produces_an_empty_batch_and_not_a_panic() {
    // The writer asks for a batch on a seal even when the last tick fetched
    // nothing, and a zero row batch has to be a legal thing to hand it.
    let batch = PageBuilder::new().finish();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 32);
}

#[test]
fn a_row_the_builder_wrote_survives_a_round_trip_through_a_segment() {
    // The end to end shape of the thing: build a row, write it, read it back.
    // Without this the batch could match the schema and still be something the
    // writer will not encode.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("pages.umi");

    let mut builder = PageBuilder::new();
    let row = row_of(&html(&prose()));
    builder.push(&row);
    let batch = builder.finish();

    let mut writer = umi_file::SegmentWriter::create(
        &path,
        umi_file::Create {
            stream: umi_file::StreamKind::Pages,
            segment_id: [1u8; 16],
            coordinator: [0u8; 32],
            created_ms: T0,
            canon_version: 1,
            extractor_version: 1,
            crawl_profile: 0,
        },
        umi_file::WriterConfig::default(),
    )
    .expect("create");
    writer.push(&batch).expect("push");
    let stats = writer.seal().expect("seal");
    assert_eq!(stats.rows, 1);

    let segment = umi_file::Segment::open(&path).expect("open");
    assert_eq!(segment.shoals(), 1);
    let shoal = segment.shoal(0).expect("the only shoal");
    shoal.verify().expect("the checksums hold");
    let back = shoal
        .to_arrow(&["url", "minhash", "simhash"])
        .expect("decode");
    assert_eq!(back.num_rows(), 1);
    assert_eq!(back.column(0).as_string::<i32>().value(0), URL);
    assert_eq!(
        back.column(1).as_fixed_size_binary().value(0),
        row.sketch.to_bytes()
    );
    assert_eq!(
        back.column(2).as_primitive::<UInt64Type>().value(0),
        row.sketch.simhash
    );
}

#[test]
fn the_outcome_codes_round_trip_and_stay_where_they_are() {
    for code in OutcomeCode::ALL {
        assert_eq!(OutcomeCode::from_u8(code.as_u8()), Some(code));
    }
    assert_eq!(OutcomeCode::from_u8(200), None, "unknown stays unknown");
    // The numbers are a published format, so a few of them are pinned here.
    // Changing one of these is changing what every segment already on disk
    // means, and this test is where somebody finds that out.
    assert_eq!(OutcomeCode::Ok.as_u8(), 0);
    assert_eq!(OutcomeCode::NotModified.as_u8(), 1);
    assert_eq!(OutcomeCode::TierExhausted.as_u8(), 14);
    assert_eq!(OutcomeCode::Malformed.as_u8(), 16);
    assert_eq!(OutcomeCode::Ok.wire(), "ok");
    assert_eq!(OutcomeCode::RedirectedOffHost.wire(), "redirected_off_host");
}

#[test]
fn the_snippet_and_verification_codes_stay_where_they_are_too() {
    for kind in SnippetKind::ALL {
        assert_eq!(SnippetKind::from_u8(kind.as_u8()), Some(kind));
    }
    assert_eq!(SnippetKind::Title.as_u8(), 0);
    assert_eq!(SnippetKind::Headline.as_u8(), 5);
    assert_eq!(SnippetKind::from_u8(6), None);
    assert_eq!(SnippetKind::for_heading(4), None, "doc 11.6 stops at h3");

    for level in Verification::ALL {
        assert_eq!(Verification::from_u8(level.as_u8()), Some(level));
    }
    assert_eq!(Verification::Local.as_u8(), 0);
    assert_eq!(Verification::Unverified.as_u8(), 3);
}

/// The chunk root of a body with no bytes in it, which every bodyless row
/// carries.
fn empty_chunk_root() -> [u8; 32] {
    umi_dedup::ChunkTree::build(&[]).root()
}
