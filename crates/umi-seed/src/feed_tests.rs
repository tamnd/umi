//! Feeds, doc 13.6.
//!
//! Three formats and one reader, so most of what is worth testing is that a
//! file in one of them does not need the caller to have known which one it
//! was.

use crate::feed::Feed;
use crate::sitemap::Caps;

/// 2026-08-28T00:00:00Z, written in each format's own grammar below.
const DAY: u64 = 1_787_875_200_000;

fn links(feed: &Feed) -> Vec<&str> {
    feed.entries.iter().map(|e| e.url.as_str()).collect()
}

// The three formats.

#[test]
fn rss_takes_the_link_text_and_the_pubdate() {
    let doc = "<rss version=\"2.0\"><channel>\
         <title>Example</title>\
         <link>https://example.com/</link>\
         <item>\
           <title>First</title>\
           <link>https://example.com/a</link>\
           <pubDate>Fri, 28 Aug 2026 00:00:00 GMT</pubDate>\
         </item>\
         <item><link>https://example.com/b</link></item>\
       </channel></rss>";
    let out = Feed::parse(doc.as_bytes());
    assert_eq!(
        links(&out),
        ["https://example.com/a", "https://example.com/b"],
        "the channel's own link is not an entry"
    );
    assert_eq!(out.entries[0].lastmod_ms, Some(DAY));
    assert_eq!(out.entries[1].lastmod_ms, None);
}

#[test]
fn atom_takes_the_href_and_the_updated() {
    let doc = "<feed xmlns=\"http://www.w3.org/2005/Atom\">\
         <link rel=\"self\" href=\"https://example.com/feed.xml\"/>\
         <entry>\
           <link rel=\"alternate\" href=\"https://example.com/a\"/>\
           <updated>2026-08-28T00:00:00Z</updated>\
         </entry>\
         <entry><link href=\"https://example.com/b\"/></entry>\
       </feed>";
    let out = Feed::parse(doc.as_bytes());
    assert_eq!(
        links(&out),
        ["https://example.com/a", "https://example.com/b"],
        "a link with no rel is an alternate, which is the page"
    );
    assert_eq!(out.entries[0].lastmod_ms, Some(DAY));
}

#[test]
fn rss_one_is_rdf_and_still_has_items_in_it() {
    let doc = "<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" \
                 xmlns=\"http://purl.org/rss/1.0/\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\">\
         <channel><title>Example</title></channel>\
         <item rdf:about=\"https://example.com/a\">\
           <link>https://example.com/a</link>\
           <dc:date>2026-08-28T00:00:00Z</dc:date>\
         </item>\
       </rdf:RDF>";
    let out = Feed::parse(doc.as_bytes());
    assert_eq!(links(&out), ["https://example.com/a"]);
    assert_eq!(out.entries[0].lastmod_ms, Some(DAY));
}

// The parts that are not the same as the format claims.

#[test]
fn a_link_to_something_that_is_not_the_entry_is_left_alone() {
    // `enclosure` is a podcast episode, `replies` is a comment thread, and
    // seeding either would be crawling something nobody asked for.
    let doc = "<feed xmlns=\"http://www.w3.org/2005/Atom\"><entry>\
         <link rel=\"enclosure\" href=\"https://example.com/a.mp3\"/>\
         <link rel=\"replies\" href=\"https://example.com/a/comments\"/>\
         <link rel=\"alternate\" href=\"https://example.com/a\"/>\
       </entry></feed>";
    assert_eq!(
        links(&Feed::parse(doc.as_bytes())),
        ["https://example.com/a"]
    );
}

#[test]
fn a_guid_is_used_only_when_there_is_no_link() {
    let doc = "<rss version=\"2.0\"><channel>\
         <item><guid>https://example.com/a</guid></item>\
         <item>\
           <guid isPermaLink=\"false\">tag:example.com,2026:1</guid>\
           <link>https://example.com/b</link>\
         </item>\
         <item><guid isPermaLink=\"false\">tag:example.com,2026:2</guid></item>\
       </channel></rss>";
    assert_eq!(
        links(&Feed::parse(doc.as_bytes())),
        ["https://example.com/a", "https://example.com/b"],
        "a guid that says it is not a permalink is not a url"
    );
}

#[test]
fn the_date_that_wins_is_the_one_that_says_when_it_changed() {
    // A post published in 2020 and edited yesterday is a page to refetch, so
    // `updated` beats `published` when a feed carries both.
    let doc = "<feed xmlns=\"http://www.w3.org/2005/Atom\"><entry>\
         <link href=\"https://example.com/a\"/>\
         <published>2020-01-01T00:00:00Z</published>\
         <updated>2026-08-28T00:00:00Z</updated>\
       </entry></feed>";
    assert_eq!(Feed::parse(doc.as_bytes()).entries[0].lastmod_ms, Some(DAY));
}

#[test]
fn either_date_grammar_is_read_wherever_it_turns_up() {
    // An Atom feed with an RSS date in it is a broken feed with a real date in
    // it, and refusing it loses the signal to be right about the format.
    let doc = "<feed xmlns=\"http://www.w3.org/2005/Atom\"><entry>\
         <link href=\"https://example.com/a\"/>\
         <updated>Fri, 28 Aug 2026 00:00:00 GMT</updated>\
       </entry></feed>";
    assert_eq!(Feed::parse(doc.as_bytes()).entries[0].lastmod_ms, Some(DAY));
}

#[test]
fn cdata_and_escapes_reach_the_url_whole() {
    let doc = "<rss version=\"2.0\"><channel>\
         <item><link><![CDATA[https://example.com/a]]></link></item>\
         <item><link>https://example.com/?a=1&amp;b=2</link></item>\
         <item><link href=\"https://example.com/?a=1&amp;b=2\"/></item>\
       </channel></rss>";
    assert_eq!(
        links(&Feed::parse(doc.as_bytes())),
        [
            "https://example.com/a",
            "https://example.com/?a=1&b=2",
            "https://example.com/?a=1&b=2"
        ]
    );
}

#[test]
fn an_entry_with_no_url_at_all_is_not_an_entry() {
    let doc = "<rss version=\"2.0\"><channel>\
         <item><title>No link</title><pubDate>Fri, 28 Aug 2026 00:00:00 GMT</pubDate></item>\
         <item><link>https://example.com/a</link></item>\
       </channel></rss>";
    assert_eq!(
        links(&Feed::parse(doc.as_bytes())),
        ["https://example.com/a"]
    );
}

// The limits, which are the sitemap parser's and are here because a feed is a
// document from a stranger too.

#[test]
fn the_entry_cap_stops_the_parse_and_says_so() {
    let mut body = String::new();
    for i in 0..500 {
        body.push_str("<item><link>https://example.com/");
        body.push_str(&i.to_string());
        body.push_str("</link></item>");
    }
    let doc = format!("<rss version=\"2.0\"><channel>{body}</channel></rss>");
    let caps = Caps {
        max_urls: 25,
        ..Caps::default()
    };
    let out = Feed::parse_with(doc.as_bytes(), &caps);
    assert_eq!(out.entries.len(), 25);
    assert!(out.truncated);
}

#[test]
fn junk_that_is_not_a_feed_gives_nothing_rather_than_a_panic() {
    for doc in [
        "<html><body><p>not a feed</p></body></html>",
        "<rss><channel><item><link>",
        "not xml at all",
        "",
    ] {
        assert!(
            Feed::parse(doc.as_bytes()).is_empty(),
            "{doc:?} gave entries"
        );
    }
}
