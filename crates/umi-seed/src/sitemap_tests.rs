//! Sitemaps, doc 13.6.
//!
//! The cases are grouped the way the problems are: what a correct file gives
//! back, what the real world writes instead, and what a file written to hurt
//! us gets to cost.

use crate::sitemap::{Caps, Sitemap};

const NS: &str = "http://www.sitemaps.org/schemas/sitemap/0.9";

fn urlset(body: &str) -> String {
    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<urlset xmlns=\"{NS}\">{body}</urlset>")
}

fn urls(sitemap: &Sitemap) -> Vec<&str> {
    sitemap.urls.iter().map(|e| e.url.as_str()).collect()
}

/// A body with `count` distinct URLs in it, for the cases about limits.
fn many(count: usize) -> String {
    let mut body = String::new();
    for i in 0..count {
        body.push_str("<url><loc>https://example.com/");
        body.push_str(&i.to_string());
        body.push_str("</loc></url>");
    }
    body
}

// The shape a correct file has.

#[test]
fn a_urlset_gives_its_urls_in_order() {
    let doc = urlset(
        "<url><loc>https://example.com/a</loc></url>\
         <url><loc>https://example.com/b</loc></url>\
         <url><loc>https://example.com/c</loc></url>",
    );
    let out = Sitemap::parse(doc.as_bytes());
    assert_eq!(
        urls(&out),
        [
            "https://example.com/a",
            "https://example.com/b",
            "https://example.com/c"
        ]
    );
    assert!(out.sitemaps.is_empty());
    assert!(!out.truncated && !out.malformed);
}

#[test]
fn lastmod_comes_through_because_it_is_the_point() {
    let doc = urlset(
        "<url><loc>https://example.com/a</loc><lastmod>2026-08-28</lastmod></url>\
         <url><loc>https://example.com/b</loc></url>",
    );
    let out = Sitemap::parse(doc.as_bytes());
    assert_eq!(out.urls[0].lastmod_ms, Some(1_787_875_200_000));
    assert_eq!(
        out.urls[1].lastmod_ms, None,
        "no date is not a date of zero"
    );
}

#[test]
fn an_index_is_kept_apart_from_a_urlset() {
    let doc = format!(
        "<sitemapindex xmlns=\"{NS}\">\
           <sitemap><loc>https://example.com/s1.xml</loc><lastmod>2026-08-28</lastmod></sitemap>\
           <sitemap><loc>https://example.com/s2.xml</loc></sitemap>\
         </sitemapindex>"
    );
    let out = Sitemap::parse(doc.as_bytes());
    assert!(out.urls.is_empty(), "an index has no page urls in it");
    assert_eq!(out.sitemaps.len(), 2);
    assert_eq!(out.sitemaps[0].lastmod_ms, Some(1_787_875_200_000));
}

#[test]
fn the_fields_that_are_not_a_url_are_left_alone() {
    let doc = urlset(
        "<url>\
           <loc>https://example.com/a</loc>\
           <changefreq>daily</changefreq>\
           <priority>0.8</priority>\
           <lastmod>2026-08-28</lastmod>\
         </url>",
    );
    let out = Sitemap::parse(doc.as_bytes());
    assert_eq!(urls(&out), ["https://example.com/a"]);
    assert_eq!(out.urls[0].lastmod_ms, Some(1_787_875_200_000));
}

// What the real world writes instead.

#[test]
fn a_namespace_prefix_does_not_hide_the_elements() {
    // Half the generators on the web bind the sitemap namespace to a prefix,
    // and a reader that matches on the whole tag name gets nothing from them.
    let doc = format!(
        "<sm:urlset xmlns:sm=\"{NS}\">\
           <sm:url><sm:loc>https://example.com/a</sm:loc></sm:url>\
         </sm:urlset>"
    );
    assert_eq!(
        urls(&Sitemap::parse(doc.as_bytes())),
        ["https://example.com/a"]
    );
}

#[test]
fn an_image_extension_does_not_leak_its_urls_into_the_pages() {
    // `<image:loc>` has the same local name as the one we want and sits one
    // level further down. Matching on the name alone would seed the frontier
    // with every JPEG on the site.
    let doc = format!(
        "<urlset xmlns=\"{NS}\" xmlns:image=\"http://www.google.com/schemas/sitemap-image/1.1\">\
           <url>\
             <loc>https://example.com/a</loc>\
             <image:image><image:loc>https://example.com/a.jpg</image:loc></image:image>\
           </url>\
         </urlset>"
    );
    assert_eq!(
        urls(&Sitemap::parse(doc.as_bytes())),
        ["https://example.com/a"]
    );
}

#[test]
fn an_escaped_ampersand_is_put_back_together() {
    // The parser hands this over as three pieces, so a reader that takes the
    // first one gets half a URL and fetches the wrong page.
    let doc = urlset("<url><loc>https://example.com/?a=1&amp;b=2</loc></url>");
    assert_eq!(
        urls(&Sitemap::parse(doc.as_bytes())),
        ["https://example.com/?a=1&b=2"]
    );
}

#[test]
fn a_bare_ampersand_does_not_stop_the_file() {
    // Invalid XML, and the single most common mistake in real sitemaps. A
    // strict reader is entitled to stop here and would throw away every URL
    // after it.
    let doc = urlset(
        "<url><loc>https://example.com/?a=1&b=2</loc></url>\
         <url><loc>https://example.com/c</loc></url>",
    );
    let out = Sitemap::parse(doc.as_bytes());
    assert_eq!(
        urls(&out),
        ["https://example.com/?a=1&b=2", "https://example.com/c"]
    );
}

#[test]
fn cdata_and_whitespace_are_read_the_same_as_text() {
    let doc = urlset(
        "<url><loc><![CDATA[https://example.com/a]]></loc></url>\
         <url><loc>\n   https://example.com/b\n  </loc></url>",
    );
    assert_eq!(
        urls(&Sitemap::parse(doc.as_bytes())),
        ["https://example.com/a", "https://example.com/b"]
    );
}

#[test]
fn an_empty_loc_is_not_an_entry() {
    let doc = urlset(
        "<url><loc></loc></url>\
         <url><lastmod>2026-08-28</lastmod></url>\
         <url><loc>https://example.com/a</loc></url>",
    );
    assert_eq!(
        urls(&Sitemap::parse(doc.as_bytes())),
        ["https://example.com/a"]
    );
}

#[test]
fn the_plain_text_form_is_read_as_urls() {
    // sitemaps.org allows a file of URLs with no markup at all, and a reader
    // that only knows XML sees a document that is malformed from byte one.
    let doc = "https://example.com/a\nhttps://example.com/b\n\n  https://example.com/c  \n";
    let out = Sitemap::parse(doc.as_bytes());
    assert_eq!(
        urls(&out),
        [
            "https://example.com/a",
            "https://example.com/b",
            "https://example.com/c"
        ]
    );
    assert!(!out.malformed, "a text sitemap is a format, not a mistake");
}

#[test]
fn a_byte_order_mark_does_not_decide_the_format() {
    let doc = urlset("<url><loc>https://example.com/a</loc></url>");
    let mut bytes = vec![0xEF, 0xBB, 0xBF];
    bytes.extend_from_slice(doc.as_bytes());
    assert_eq!(urls(&Sitemap::parse(&bytes)), ["https://example.com/a"]);
}

#[test]
fn a_truncated_document_keeps_what_it_had() {
    // A file that was cut off mid element is the shape a fetch that hit the
    // body cap leaves behind, and the URLs before the cut are still good.
    let doc = format!(
        "<urlset xmlns=\"{NS}\">\
           <url><loc>https://example.com/a</loc></url>\
           <url><loc>https://exa"
    );
    let out = Sitemap::parse(doc.as_bytes());
    assert_eq!(urls(&out), ["https://example.com/a"]);
}

// What a hostile file gets to cost.

#[test]
fn the_url_cap_stops_the_parse_and_says_so() {
    let body = many(80);
    let caps = Caps {
        max_urls: 10,
        ..Caps::default()
    };
    let out = Sitemap::parse_with(urlset(&body).as_bytes(), &caps);
    assert_eq!(out.urls.len(), 10);
    assert!(out.truncated, "a short answer has to say it is short");
}

#[test]
fn the_byte_cap_stops_the_read_before_the_memory_goes() {
    let doc = urlset(&many(2000));
    let caps = Caps {
        max_bytes: 1024,
        ..Caps::default()
    };
    let out = Sitemap::parse_with(doc.as_bytes(), &caps);
    assert!(out.truncated);
    assert!(!out.urls.is_empty(), "the prefix is still worth reading");
    assert!(out.urls.len() < 100);
    assert!(
        !out.malformed,
        "cutting the file ourselves is not the file being broken"
    );
}

#[test]
fn a_loc_longer_than_the_cap_is_dropped_rather_than_cut() {
    // Half a URL is a different URL, and fetching it would be a request
    // nobody asked for.
    let long = "x".repeat(20_000);
    let doc = urlset(&format!(
        "<url><loc>https://example.com/{long}</loc></url>\
         <url><loc>https://example.com/a</loc></url>"
    ));
    assert_eq!(
        urls(&Sitemap::parse(doc.as_bytes())),
        ["https://example.com/a"]
    );
}

#[test]
fn a_billion_laughs_is_a_parse_and_not_a_machine() {
    // The classic XML denial of service. quick-xml never resolves DTD
    // entities, so `&lol9;` is an entity nobody defined and contributes
    // nothing, rather than expanding to three gigabytes of `lol`.
    let doc = format!(
        "<!DOCTYPE urlset [\
           <!ENTITY lol \"lol\">\
           <!ENTITY lol1 \"&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;\">\
           <!ENTITY lol2 \"&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;&lol1;\">\
           <!ENTITY lol3 \"&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;&lol2;\">\
         ]>\
         <urlset xmlns=\"{NS}\"><url><loc>https://example.com/&lol3;</loc></url></urlset>"
    );
    let out = Sitemap::parse(doc.as_bytes());
    assert_eq!(urls(&out), ["https://example.com/"]);
}

#[test]
fn deep_nesting_does_not_grow_a_stack() {
    // Fifty thousand open tags and no close tags. Nothing here recurses and
    // the reader is told not to keep the names, so this is a counter going up.
    let doc = format!("<urlset xmlns=\"{NS}\">{}", "<a>".repeat(50_000));
    let out = Sitemap::parse(doc.as_bytes());
    assert!(out.is_empty());
}

/// gzip `body`, which is what a `.xml.gz` sitemap arrives as.
fn gzipped(body: &str) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(body.as_bytes()).expect("write");
    encoder.finish().expect("finish")
}

#[test]
fn a_gzipped_sitemap_is_read_like_any_other() {
    // Most large sites serve `sitemap.xml.gz`, which has a gzip content type
    // rather than a `Content-Encoding`, so no HTTP client unwraps it for us.
    let doc = urlset(
        "<url><loc>https://example.com/a</loc><lastmod>2026-08-28</lastmod></url>\
         <url><loc>https://example.com/b</loc></url>",
    );
    let out = Sitemap::parse(&gzipped(&doc));
    assert_eq!(
        urls(&out),
        ["https://example.com/a", "https://example.com/b"]
    );
    assert_eq!(out.urls[0].lastmod_ms, Some(1_787_875_200_000));
    assert!(!out.truncated && !out.malformed);
}

#[test]
fn the_plain_text_form_survives_being_gzipped_too() {
    let doc = "https://example.com/a\nhttps://example.com/b\n";
    let out = Sitemap::parse(&gzipped(doc));
    assert_eq!(
        urls(&out),
        ["https://example.com/a", "https://example.com/b"]
    );
}

#[test]
fn a_gzip_bomb_costs_the_byte_cap_and_not_the_machine() {
    // Distinct URLs compress about eighteen to one, and a file of repeats goes
    // orders of magnitude past that. The ratio is not the point either way. The
    // defence is that the cap counts what comes out of the decoder rather than
    // what went into it, so a compressed document and a plain one buy the same
    // amount of memory.
    let doc = urlset(&many(200_000));
    let bomb = gzipped(&doc);
    assert!(
        bomb.len() < doc.len() / 10,
        "the test is only meaningful if it compresses"
    );
    let caps = Caps {
        max_bytes: 64 * 1024,
        ..Caps::default()
    };
    let out = Sitemap::parse_with(&bomb, &caps);
    assert!(out.truncated);
    assert!(!out.urls.is_empty(), "the prefix is still worth reading");
    assert!(
        out.urls.len() < 2000,
        "{} urls got past the cap",
        out.urls.len()
    );
    assert!(
        !out.malformed,
        "cutting the stream ourselves is not the file being broken"
    );
}

#[test]
fn a_truncated_gzip_keeps_what_it_managed_to_inflate() {
    // A fetch that hit its body cap leaves exactly this: a valid gzip stream
    // with the end missing. The decoder fails at the cut and the URLs before
    // it are still good.
    let doc = urlset(&many(400));
    let full = gzipped(&doc);
    let cut = &full[..full.len() / 2];
    let out = Sitemap::parse(cut);
    assert!(!out.urls.is_empty());
    assert!(out.urls.len() < 400);
}

#[test]
fn junk_that_is_not_a_sitemap_gives_nothing_rather_than_a_panic() {
    for doc in [
        "<html><body><p>not a sitemap</p></body></html>",
        "<urlset><url><loc>",
        "<<<<<<",
        "<\u{0}\u{1}\u{2}",
        "",
    ] {
        let out = Sitemap::parse(doc.as_bytes());
        assert!(out.urls.is_empty(), "{doc:?} produced urls");
    }
}
