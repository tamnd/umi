//! Rows shaped like a crawl, for tests and benchmarks.
//!
//! Doc 10.2's whole budget rests on what a row actually looks like: a median
//! row of 3833 bytes, 50 links a page sharing prefixes, status 200 more than
//! nine times in ten, a timestamp column whose values span at most the 15
//! minutes a segment lives. Generated rows that ignore that shape make the
//! compression numbers meaningless, because a column of zeroes packs to
//! nothing and a column of random bytes packs to nothing either, and the real
//! answer is in between.
//!
//! So these are not the smallest rows that satisfy the schema. They are rows
//! with runs of repeated hosts, markdown with the paragraph structure real
//! markdown has, links that mostly point at the page's own site, a scatter of
//! nulls where doc 10.5 says null is what a row has, and one deliberately
//! incompressible column, because doc 10.5 has one.
//!
//! Everything here is a pure function of the row index. Nothing reads a clock
//! and nothing draws a random number, so a benchmark on server1 and the same
//! benchmark on server3 are working on identical bytes, which is what doc 16's
//! gate 1.2 needs to be checkable at all.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, ListBuilder, MapBuilder, MapFieldNames, RecordBatch,
    StringArray, StringBuilder, StructBuilder, UInt8Array, UInt8Builder, UInt16Array,
    UInt16Builder, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Fields};

use crate::schema::StreamKind;

/// The instant every sample row is dated from, so the timestamp columns hold
/// the small deltas doc 10.6 expects rather than an arbitrary spread.
pub const T0: u64 = 1_760_000_000_000;

/// All three of doc 10.3's streams, for a test or a bench that wants to cover
/// each of them without spelling the list out again.
pub const EVERY_STREAM: [StreamKind; 3] =
    [StreamKind::Pages, StreamKind::Receipts, StreamKind::Robots];

/// A batch of `rows` sample rows for whichever stream is asked for.
#[must_use]
pub fn batch(stream: StreamKind, rows: usize) -> RecordBatch {
    match stream {
        StreamKind::Pages => pages(rows),
        StreamKind::Receipts => receipts(rows),
        StreamKind::Robots => robots(rows),
    }
}

fn host_of(i: usize) -> String {
    format!("site{}.example.com", i % 7)
}

fn url_of(i: usize) -> String {
    format!("https://{}/section{}/page-{i}", host_of(i), i % 13)
}

/// Doc 10.5's page row, filled the way the extractor would fill it.
#[must_use]
pub fn pages(rows: usize) -> RecordBatch {
    let schema = StreamKind::Pages.arrow();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values((0..rows).map(url_of))),
        // Doc 10.5: null when it equals url, which is most rows.
        Arc::new(StringArray::from_iter(
            (0..rows).map(|i| (i % 11 == 0).then(|| format!("{}?utm=1", url_of(i)))),
        )),
        fixed(rows, 10, |i| vec![i as u8; 10]),
        fixed(rows, 8, |i| vec![(i % 7) as u8; 8]),
        Arc::new(StringArray::from_iter_values((0..rows).map(host_of))),
        // Doc 10.6: a shoal spans at most 15 minutes, so these deltas fit in
        // 20 bits and the column should cost about 2.5 bytes a row.
        Arc::new(UInt64Array::from_iter_values(
            (0..rows).map(|i| T0 + (i as u64) * 37),
        )),
        // Doc 10.6: 200 more than nine times in ten in a healthy crawl.
        Arc::new(UInt16Array::from_iter_values((0..rows).map(|i| {
            match i % 20 {
                18 => 304,
                19 => 404,
                _ => 200,
            }
        }))),
        Arc::new(UInt8Array::from_iter_values(
            (0..rows).map(|i| (i % 3) as u8),
        )),
        Arc::new(UInt8Array::from_iter_values(
            (0..rows).map(|i| (i % 2) as u8 + 1),
        )),
        list_u8(rows, |i| vec![1u8, (i % 2) as u8 + 1]),
        Arc::new(StringArray::from_iter(
            (0..rows).map(|i| (i % 20 != 19).then_some("text/html; charset=utf-8")),
        )),
        Arc::new(UInt32Array::from_iter_values(
            (0..rows).map(|i| 40_000 + (i as u32).wrapping_mul(131) % 200_000),
        )),
        fixed_opt(rows, 3, |i| (i % 9 != 8).then(|| b"en\0".to_vec())),
        fixed(rows, 32, |i| vec![(i * 3) as u8; 32]),
        fixed(rows, 32, |i| vec![(i * 5) as u8; 32]),
        fixed(rows, 32, |i| vec![(i * 7) as u8; 32]),
        Arc::new(StringArray::from_iter(
            (0..rows).map(|i| (i % 20 < 18).then(|| markdown(i))),
        )),
        Arc::new(StringArray::from_iter((0..rows).map(|i| {
            (i % 20 < 18).then(|| format!("Page {i} on {}", host_of(i)))
        }))),
        Arc::new(StringArray::from_iter((0..rows).map(|i| {
            (i % 4 == 0).then(|| format!("A description of page {i}, written for a search result."))
        }))),
        list_utf8(rows, |i| {
            vec![
                format!("Heading {i}"),
                "Overview".to_owned(),
                "Details".to_owned(),
            ]
        }),
        snippets(rows),
        links(rows),
        headers_kept(rows),
        Arc::new(StringArray::from_iter(
            (0..rows).map(|i| (i % 50 == 0).then_some("train-ai=n")),
        )),
        // Doc 10.6: incompressible by design, 64 x u32 of minhash.
        fixed(rows, 256, |i| {
            (0..256)
                .map(|b| (i.wrapping_mul(2_654_435_761).wrapping_add(b)) as u8)
                .collect()
        }),
        Arc::new(UInt64Array::from_iter_values(
            (0..rows).map(|i| (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)),
        )),
        Arc::new(UInt32Array::from_iter_values(
            (0..rows).map(|i| 2_000 + (i as u32) % 9_000),
        )),
        Arc::new(UInt32Array::from_iter_values(
            (0..rows).map(|i| 20 + (i as u32) % 60),
        )),
        fixed(rows, 32, |_| vec![0xABu8; 32]),
        Arc::new(UInt8Array::from_iter_values(
            (0..rows).map(|i| (i % 4) as u8),
        )),
        Arc::new(UInt64Array::from_iter_values(
            (0..rows).map(|_| T0 - 3_600_000),
        )),
        Arc::new(UInt32Array::from_iter_values((0..rows).map(|_| 0))),
    ];
    RecordBatch::try_new(schema, columns).expect("the sample pages batch matches doc 10.5")
}

/// Markdown that repeats the way real markdown repeats, which is what doc
/// 10.6's per shoal zstd dictionary is supposed to exploit.
fn markdown(i: usize) -> String {
    let mut out = format!("# Page {i}\n\nThis is the opening paragraph of page {i}. ");
    for section in 0..6 {
        out.push_str(&format!(
            "\n\n## Section {section}\n\nThe body of section {section} on page {i} says \
             something ordinary about the subject, in the way that a page on the web \
             tends to. It links to [another page](/section{section}/page-{i}) and then \
             carries on for a while longer so the paragraph is a realistic length.\n"
        ));
    }
    out
}

/// Doc 04's receipt, flattened the way doc 10.3's receipt stream stores it.
#[must_use]
pub fn receipts(rows: usize) -> RecordBatch {
    let schema = StreamKind::Receipts.arrow();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from_iter_values((0..rows).map(|_| 1))),
        fixed(rows, 16, |i| vec![i as u8; 16]),
        fixed(rows, 16, |i| vec![(i * 2) as u8; 16]),
        fixed(rows, 32, |_| vec![0x11u8; 32]),
        Arc::new(StringArray::from_iter_values(
            (0..rows).map(|i| format!("https://h{}.example/p{i}", i % 5)),
        )),
        Arc::new(StringArray::from_iter((0..rows).map(|_| None::<String>))),
        Arc::new(UInt64Array::from_iter_values(
            (0..rows).map(|i| T0 + i as u64),
        )),
        Arc::new(UInt32Array::from_iter_values(
            (0..rows).map(|i| 100 + (i as u32) % 900),
        )),
        Arc::new(UInt8Array::from_iter_values((0..rows).map(|_| 0))),
        Arc::new(UInt8Array::from_iter_values((0..rows).map(|_| 1))),
        list_u8(rows, |_| vec![1u8]),
        Arc::new(StringArray::from_iter_values((0..rows).map(|_| "GET"))),
        redirects(rows),
        Arc::new(StringArray::from_iter((0..rows).map(|i| {
            (i % 3 == 0).then_some("t13d1516h2_8daaf6152771_b0da82dd1658")
        }))),
        Arc::new(StringArray::from_iter_values((0..rows).map(|_| "2"))),
        Arc::new(UInt16Array::from_iter(
            (0..rows).map(|i| (i % 7 != 6).then_some(200u16)),
        )),
        fixed_opt(rows, 32, |i| (i % 7 != 6).then(|| vec![0x22u8; 32])),
        headers_kept(rows),
        Arc::new(UInt32Array::from_iter(
            (0..rows).map(|i| (i % 7 != 6).then_some(1_000u32 + i as u32)),
        )),
        Arc::new(StringArray::from_iter(
            (0..rows).map(|i| (i % 7 != 6).then_some("text/html")),
        )),
        fixed_opt(rows, 32, |i| (i % 7 != 6).then(|| vec![0x33u8; 32])),
        Arc::new(UInt32Array::from_iter(
            (0..rows).map(|i| (i % 7 != 6).then_some(1_000u32)),
        )),
        fixed_opt(rows, 32, |i| (i % 7 != 6).then(|| vec![0x44u8; 32])),
        Arc::new(UInt32Array::from_iter(
            (0..rows).map(|i| (i % 7 != 6).then_some(1u32)),
        )),
        list_fixed(rows, 32),
        Arc::new(StringArray::from_iter(
            (0..rows).map(|i| Some(format!("h{}.example", i % 5))),
        )),
        Arc::new(StringArray::from_iter((0..rows).map(|_| Some("h2")))),
        Arc::new(UInt64Array::from_iter(
            (0..rows).map(|_| Some(T0 - 86_400_000)),
        )),
        Arc::new(UInt64Array::from_iter(
            (0..rows).map(|_| Some(T0 + 86_400_000)),
        )),
        Arc::new(StringArray::from_iter(
            (0..rows).map(|_| Some("umi-extract/0.4.1")),
        )),
        fixed_opt(rows, 32, |_| Some(vec![0x55u8; 32])),
        Arc::new(UInt8Array::from_iter((0..rows).map(|_| Some(1u8)))),
        Arc::new(UInt32Array::from_iter(
            (0..rows).map(|i| Some(50u32 + (i as u32) % 30)),
        )),
        Arc::new(UInt32Array::from_iter(
            (0..rows).map(|i| Some(3_000u32 + i as u32)),
        )),
        fixed(rows, 64, |i| vec![(i % 251) as u8; 64]),
    ];
    RecordBatch::try_new(schema, columns).expect("the sample receipts batch matches doc 04")
}

/// Doc 10.3's robots stream.
#[must_use]
pub fn robots(rows: usize) -> RecordBatch {
    let schema = StreamKind::Robots.arrow();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            (0..rows).map(|i| format!("h{i}.example.com")),
        )),
        Arc::new(UInt64Array::from_iter_values(
            (0..rows).map(|i| T0 + (i as u64) * 1_000),
        )),
        Arc::new(UInt16Array::from_iter_values(
            (0..rows).map(|i| if i % 10 == 9 { 404 } else { 200 }),
        )),
        Arc::new(StringArray::from_iter((0..rows).map(|i| {
            (i % 10 != 9).then(|| {
                format!(
                    "User-agent: *\nDisallow: /private/\nCrawl-delay: {}\n",
                    i % 5
                )
            })
        }))),
        Arc::new(UInt32Array::from_iter_values((0..rows).map(|_| 1))),
        Arc::new(UInt32Array::from_iter_values((0..rows).map(|_| 2))),
        Arc::new(UInt32Array::from_iter(
            (0..rows).map(|i| (i % 5 != 0).then_some((i % 5) as u32 * 1_000)),
        )),
        Arc::new(UInt8Array::from_iter_values(
            (0..rows).map(|i| u8::from(i % 10 != 9)),
        )),
        list_utf8(rows, |i| {
            vec![format!("https://h{i}.example.com/sitemap.xml")]
        }),
        Arc::new(StringArray::from_iter(
            (0..rows).map(|i| (i % 30 == 0).then_some("train-ai=n")),
        )),
    ];
    RecordBatch::try_new(schema, columns).expect("the sample robots batch matches doc 10.3")
}

fn fixed(rows: usize, width: i32, value: impl Fn(usize) -> Vec<u8>) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::new(width);
    for i in 0..rows {
        builder
            .append_value(value(i))
            .expect("the sample value is the declared width");
    }
    Arc::new(builder.finish())
}

fn fixed_opt(rows: usize, width: i32, value: impl Fn(usize) -> Option<Vec<u8>>) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::new(width);
    for i in 0..rows {
        match value(i) {
            Some(bytes) => builder
                .append_value(bytes)
                .expect("the sample value is the declared width"),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn list_u8(rows: usize, value: impl Fn(usize) -> Vec<u8>) -> ArrayRef {
    let mut builder = ListBuilder::new(UInt8Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::UInt8,
        false,
    )));
    for i in 0..rows {
        for byte in value(i) {
            builder.values().append_value(byte);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn list_utf8(rows: usize, value: impl Fn(usize) -> Vec<String>) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Utf8,
        false,
    )));
    for i in 0..rows {
        for text in value(i) {
            builder.values().append_value(text);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn list_fixed(rows: usize, width: i32) -> ArrayRef {
    let mut builder = ListBuilder::new(FixedSizeBinaryBuilder::new(width)).with_field(Arc::new(
        Field::new("item", DataType::FixedSizeBinary(width), false),
    ));
    for i in 0..rows {
        for one in 0..2u8 {
            builder
                .values()
                .append_value(vec![(i as u8) ^ one; width as usize])
                .expect("the sample value is the declared width");
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn snippets(rows: usize) -> ArrayRef {
    let fields = Fields::from(vec![
        Field::new("kind", DataType::UInt8, false),
        Field::new("text", DataType::Utf8, false),
    ]);
    let builder = StructBuilder::new(
        fields.clone(),
        vec![
            Box::new(UInt8Builder::new()),
            Box::new(StringBuilder::new()),
        ],
    );
    let mut list = ListBuilder::new(builder).with_field(Arc::new(Field::new(
        "item",
        DataType::Struct(fields),
        false,
    )));
    for i in 0..rows {
        for kind in 0..3u8 {
            let values = list.values();
            values
                .field_builder::<UInt8Builder>(0)
                .expect("the struct builder has a kind field")
                .append_value(kind);
            values
                .field_builder::<StringBuilder>(1)
                .expect("the struct builder has a text field")
                .append_value(format!("snippet {kind} for page {i}"));
            values.append(true);
        }
        list.append(true);
    }
    Arc::new(list.finish())
}

/// Doc 10.2 budgets 50 links a page with heavy prefix sharing, because most
/// links on a page point back at the same site.
fn links(rows: usize) -> ArrayRef {
    let fields = Fields::from(vec![
        Field::new("href", DataType::Utf8, false),
        Field::new("anchor", DataType::Utf8, false),
        Field::new("rel", DataType::UInt16, false),
        Field::new("kind", DataType::UInt8, false),
    ]);
    let builder = StructBuilder::new(
        fields.clone(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(StringBuilder::new()),
            Box::new(UInt16Builder::new()),
            Box::new(UInt8Builder::new()),
        ],
    );
    let mut list = ListBuilder::new(builder).with_field(Arc::new(Field::new(
        "item",
        DataType::Struct(fields),
        false,
    )));
    for i in 0..rows {
        for n in 0..50 {
            let values = list.values();
            values
                .field_builder::<StringBuilder>(0)
                .expect("the struct builder has an href field")
                .append_value(format!(
                    "https://{}/section{}/page-{}",
                    host_of(i),
                    n % 13,
                    i + n
                ));
            values
                .field_builder::<StringBuilder>(1)
                .expect("the struct builder has an anchor field")
                .append_value(format!("link {n}"));
            values
                .field_builder::<UInt16Builder>(2)
                .expect("the struct builder has a rel field")
                .append_value((n % 4) as u16);
            values
                .field_builder::<UInt8Builder>(3)
                .expect("the struct builder has a kind field")
                .append_value((n % 3) as u8);
            values.append(true);
        }
        list.append(true);
    }
    Arc::new(list.finish())
}

fn headers_kept(rows: usize) -> ArrayRef {
    // Arrow's defaults are `keys` and `values` with a nullable value, and the
    // schema in doc 10.5 says `key` and `value` with neither nullable, so the
    // names and the nullability both have to be spelled out. A map whose field
    // names differ is a different type as far as `RecordBatch::try_new` is
    // concerned, even though the data is identical.
    let names = MapFieldNames {
        entry: "entries".to_owned(),
        key: "key".to_owned(),
        value: "value".to_owned(),
    };
    let mut builder = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new())
        .with_values_field(Field::new("value", DataType::Utf8, false));
    for i in 0..rows {
        builder.keys().append_value("content-type");
        builder.values().append_value("text/html; charset=utf-8");
        builder.keys().append_value("last-modified");
        builder
            .values()
            .append_value("Wed, 21 Oct 2026 07:28:00 GMT");
        builder.keys().append_value("etag");
        builder.values().append_value(format!("\"{i:x}\""));
        builder
            .append(true)
            .expect("the map row has matching keys and values");
    }
    Arc::new(builder.finish())
}

/// Most fetches redirect nowhere, which is the case a list column has to get
/// right: an empty list is not a null list.
fn redirects(rows: usize) -> ArrayRef {
    let fields = Fields::from(vec![
        Field::new("from", DataType::Utf8, false),
        Field::new("to", DataType::Utf8, false),
        Field::new("status", DataType::UInt16, false),
    ]);
    let builder = StructBuilder::new(
        fields.clone(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(StringBuilder::new()),
            Box::new(UInt16Builder::new()),
        ],
    );
    let mut list = ListBuilder::new(builder).with_field(Arc::new(Field::new(
        "item",
        DataType::Struct(fields),
        false,
    )));
    for i in 0..rows {
        if i % 4 == 0 {
            let values = list.values();
            values
                .field_builder::<StringBuilder>(0)
                .expect("the struct builder has a from field")
                .append_value(format!("http://h{}.example/p{i}", i % 5));
            values
                .field_builder::<StringBuilder>(1)
                .expect("the struct builder has a to field")
                .append_value(format!("https://h{}.example/p{i}", i % 5));
            values
                .field_builder::<UInt16Builder>(2)
                .expect("the struct builder has a status field")
                .append_value(301);
            values.append(true);
        }
        list.append(true);
    }
    Arc::new(list.finish())
}
