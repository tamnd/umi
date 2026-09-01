//! RFC 9309 conformance.
//!
//! Google's `robots_test.cc`, the reference suite for the spec that became
//! RFC 9309, is transpiled wholesale into `tests/conformance.rs`. These are
//! the cases it does not cover: the status code rules from section 2.3.1, the
//! size cap from section 2.5, `Crawl-delay`, `Sitemap`, AIPREF
//! `Content-Usage`, and the four situations issue #6 names. Grouped by RFC
//! section, with those four in their own block.

use super::*;

/// Assert a path's decision under a robots.txt body, for the `umi` agent.
#[track_caller]
fn allowed(body: &str, path: &str) -> bool {
    Robots::parse_str(body).allows(path).is_allowed()
}

// ---------------------------------------------------------------------------
// RFC 9309 2.2.1, group selection
// ---------------------------------------------------------------------------

#[test]
fn a_named_group_beats_the_wildcard_group() {
    let body = "\
User-agent: *
Disallow: /

User-agent: umi
Disallow: /private
";
    assert!(allowed(body, "/public"));
    assert!(!allowed(body, "/private"));
}

#[test]
fn group_order_in_the_file_does_not_matter() {
    // The same rules with the groups swapped must give the same answer, or a
    // site's meaning depends on where in the file it put our name.
    let a = "User-agent: *\nDisallow: /\n\nUser-agent: umi\nDisallow: /x\n";
    let b = "User-agent: umi\nDisallow: /x\n\nUser-agent: *\nDisallow: /\n";
    assert_eq!(allowed(a, "/y"), allowed(b, "/y"));
    assert!(allowed(a, "/y"));
}

#[test]
fn user_agent_matching_is_case_insensitive() {
    let body = "User-agent: UMI\nDisallow: /no\n";
    assert!(!allowed(body, "/no"));
    assert!(allowed(body, "/yes"));
}

#[test]
fn we_do_not_answer_to_variants_of_our_name() {
    // Doc 07.4 is explicit: the token is `umi` and nothing else. A site that
    // writes `umibot` has addressed some other crawler, and quietly matching
    // it would mean obeying rules never meant for us and, worse, ignoring the
    // `*` group that was.
    for variant in ["umibot", "umi/1.0", "umi-crawler", "umicrawler", "um"] {
        let body = format!("User-agent: {variant}\nDisallow: /\n\nUser-agent: *\nAllow: /\n");
        assert!(
            allowed(&body, "/anything"),
            "{variant} should not have matched us"
        );
    }
}

#[test]
fn a_site_that_blocks_the_wildcard_blocks_us() {
    assert!(!allowed("User-agent: *\nDisallow: /\n", "/anything"));
}

#[test]
fn several_user_agent_lines_share_one_group() {
    let body = "\
User-agent: googlebot
User-agent: umi
User-agent: bingbot
Disallow: /shared
";
    assert!(!allowed(body, "/shared"));
}

#[test]
fn two_groups_naming_the_same_agent_are_merged() {
    // RFC 9309 2.2.1. Sites split their rules across repeated blocks all the
    // time, usually by accident, and dropping the second block means crawling
    // paths the site disallowed.
    let body = "\
User-agent: umi
Disallow: /a

User-agent: umi
Disallow: /b
";
    assert!(!allowed(body, "/a"));
    assert!(!allowed(body, "/b"));
    assert!(allowed(body, "/c"));
}

#[test]
fn a_rule_line_ends_the_agent_run() {
    // `User-agent: x / Disallow: /1 / User-agent: y / Disallow: /2` is two
    // groups, not one group with two agents. Getting this wrong applies every
    // site's rules to every crawler.
    let body = "\
User-agent: other
Disallow: /theirs

User-agent: umi
Disallow: /ours
";
    assert!(allowed(body, "/theirs"));
    assert!(!allowed(body, "/ours"));
}

// ---------------------------------------------------------------------------
// RFC 9309 2.2.2, allow and disallow precedence
// ---------------------------------------------------------------------------

#[test]
fn the_longest_matching_rule_wins() {
    let body = "\
User-agent: *
Disallow: /folder
Allow: /folder/subfolder
";
    assert!(!allowed(body, "/folder/other"));
    assert!(allowed(body, "/folder/subfolder/page"));
}

#[test]
fn allow_wins_a_tie_on_length() {
    let body = "User-agent: *\nDisallow: /page\nAllow: /page\n";
    assert!(allowed(body, "/page"));
}

#[test]
fn order_within_a_group_does_not_matter() {
    let a = "User-agent: *\nAllow: /p\nDisallow: /\n";
    let b = "User-agent: *\nDisallow: /\nAllow: /p\n";
    assert_eq!(allowed(a, "/p/x"), allowed(b, "/p/x"));
    assert!(allowed(a, "/p/x"));
    assert!(!allowed(a, "/q"));
}

#[test]
fn nothing_matching_means_allowed() {
    // robots.txt is a deny list. An empty file permits everything, and so does
    // a file whose rules are all about other paths.
    assert!(allowed("", "/anything"));
    assert!(allowed("User-agent: *\nDisallow: /x\n", "/y"));
}

#[test]
fn an_empty_disallow_permits_everything() {
    // RFC 9309 2.2.2, and the conventional way to write a permissive file.
    // Reading it as "disallow the empty prefix", which every path starts with,
    // would block the entire web.
    assert!(allowed("User-agent: *\nDisallow:\n", "/anything"));
    assert!(allowed("User-agent: *\nDisallow: \n", "/"));
}

// ---------------------------------------------------------------------------
// RFC 9309 2.2.3, wildcards
// ---------------------------------------------------------------------------

#[test]
fn star_matches_any_run_including_none() {
    let body = "User-agent: *\nDisallow: /a*b\n";
    assert!(!allowed(body, "/ab"));
    assert!(!allowed(body, "/axxxb"));
    assert!(!allowed(body, "/axxxbyyy"));
    assert!(allowed(body, "/ba"));
}

#[test]
fn dollar_anchors_to_the_end_of_the_path() {
    let body = "User-agent: *\nDisallow: /*.php$\n";
    assert!(!allowed(body, "/index.php"));
    assert!(!allowed(body, "/a/b/c.php"));
    assert!(allowed(body, "/index.php?q=1"));
    assert!(allowed(body, "/index.phps"));
}

#[test]
fn a_bare_dollar_anchors_the_whole_pattern() {
    let body = "User-agent: *\nDisallow: /$\n";
    assert!(!allowed(body, "/"));
    assert!(allowed(body, "/page"));
}

#[test]
fn several_wildcards_in_one_pattern() {
    let body = "User-agent: *\nDisallow: /*/*/private\n";
    assert!(!allowed(body, "/a/b/private"));
    assert!(!allowed(body, "/aa/bb/private/deep"));
    assert!(allowed(body, "/a/private"));
}

#[test]
fn a_pathological_wildcard_pattern_does_not_blow_up() {
    // Patterns like this are on real sites and a backtracking matcher turns
    // them into a denial of service we inflict on ourselves. The matcher is
    // greedy and single pass, so this is linear.
    let body = "User-agent: *\nDisallow: /*/*/*/*/*/*/*/*/*/*/*.php$\n";
    let path = format!("/{}x.php", "a/".repeat(200));
    let start = std::time::Instant::now();
    let decision = Robots::parse_str(body).allows(&path);
    assert!(start.elapsed() < std::time::Duration::from_millis(50));
    assert_eq!(decision, Decision::Disallowed);
}

#[test]
fn matching_is_octet_wise_on_the_encoded_path() {
    // RFC 9309 section 2.2.2 compares the percent-encoded forms, so `/admin`
    // and `/%61dmin` are different strings here and only the first matches.
    // That looks like the classic encode-one-letter bypass and is not one:
    // every path reaching this function has been through canonicalisation,
    // which decodes unreserved escapes, so `/%61dmin` is already `/admin` by
    // the time a decision is asked for. `tests/no_bypass.rs` pins that end to
    // end rather than leaving it as a claim in a comment.
    let body = "User-agent: *\nDisallow: /admin\n";
    assert!(!allowed(body, "/admin"));
    assert!(allowed(body, "/%61dmin"));
    // Paths are case sensitive, so `/Admin` is a different path that this rule
    // does not cover. Canonicalisation never touches the case of a path.
    assert!(allowed(body, "/Admin"));
}

#[test]
fn a_pattern_with_a_non_ascii_character_is_encoded_to_match() {
    // A site writes `Disallow: /café` and a fetcher requests `/caf%C3%A9`,
    // because that is the only thing that goes on the wire. Section 2.2.2
    // says encode the pattern, so the two meet.
    let body = "User-agent: *\nDisallow: /café\n";
    assert!(!allowed(body, "/caf%C3%A9"));
    assert!(allowed(body, "/tea"));
}

#[test]
fn an_encoded_slash_stays_encoded_on_both_sides() {
    // Same reasoning as canonicalisation step 7: decoding `%2F` invents a path
    // boundary the origin does not have. Canonicalisation leaves `%2F` alone
    // and uppercases the hex, so this is the form both sides arrive in.
    let body = "User-agent: *\nDisallow: /a%2Fb\n";
    assert!(!allowed(body, "/a%2Fb"));
    assert!(allowed(body, "/a/b"));
    // A site that typed it in lowercase means the same rule, so the pattern
    // side is normalised on the way in.
    let lower = "User-agent: *\nDisallow: /a%2fb\n";
    assert!(!allowed(lower, "/a%2Fb"));
}

// ---------------------------------------------------------------------------
// RFC 9309 2.2.4, unknown fields, and general lexing
// ---------------------------------------------------------------------------

#[test]
fn unknown_fields_are_ignored() {
    // Including `Host` and `Noindex`, both proposed, neither honoured.
    let body = "\
User-agent: umi
Host: example.com
Noindex: /secret
Request-rate: 1/10
Disallow: /x
";
    assert!(!allowed(body, "/x"));
    assert!(allowed(body, "/secret"));
}

#[test]
fn comments_are_stripped_anywhere_on_a_line() {
    let body = "\
# leading comment
User-agent: umi   # trailing comment
Disallow: /x      # another
";
    assert!(!allowed(body, "/x"));
    assert!(allowed(body, "/y"));
}

#[test]
fn whitespace_around_the_colon_is_tolerated() {
    let body = "User-agent   :   umi  \nDisallow   :   /x  \n";
    assert!(!allowed(body, "/x"));
}

#[test]
fn a_line_with_no_colon_is_skipped() {
    let body = "User-agent: umi\nthis is not a directive\nDisallow: /x\n";
    assert!(!allowed(body, "/x"));
}

#[test]
fn field_names_are_case_insensitive() {
    let body = "USER-AGENT: umi\nDISALLOW: /x\nAlLoW: /x/ok\n";
    assert!(!allowed(body, "/x"));
    assert!(allowed(body, "/x/ok"));
}

#[test]
fn crlf_line_endings_parse() {
    let body = "User-agent: umi\r\nDisallow: /x\r\n";
    assert!(!allowed(body, "/x"));
}

#[test]
fn a_utf8_bom_does_not_eat_the_first_directive() {
    // A BOM makes the first field name `\u{feff}user-agent`, which silently
    // drops the group. Plenty of robots.txt files are saved from Windows
    // editors and carry one.
    let body = "\u{feff}User-agent: umi\nDisallow: /x\n";
    assert!(
        !allowed(body, "/x"),
        "the BOM swallowed the user-agent line"
    );

    // And through the byte path, where the BOM arrives as EF BB BF.
    let mut bytes = vec![0xef, 0xbb, 0xbf];
    bytes.extend_from_slice(b"User-agent: umi\nDisallow: /x\n");
    assert_eq!(Robots::parse(&bytes).allows("/x"), Decision::Disallowed);
}

#[test]
fn invalid_utf8_is_decoded_lossily_rather_than_rejected() {
    // Refusing to parse means treating a site that has rules as a site that
    // has none, which is the worst possible way to fail.
    let mut body = b"User-agent: umi\nDisallow: /caf".to_vec();
    body.push(0xff);
    body.extend_from_slice(b"\nDisallow: /x\n");
    let robots = Robots::parse(&body);
    assert_eq!(robots.allows("/x"), Decision::Disallowed);
}

// ---------------------------------------------------------------------------
// The four edge cases from issue #6, which are the point of the crate
// ---------------------------------------------------------------------------

#[test]
fn a_5xx_means_fully_disallowed() {
    // RFC 9309 2.3.1.4. The naive reading is "no rules came back, so crawl
    // away", which hammers a site hardest exactly when it is least able to
    // serve. That is why the status is an argument to for_status rather than
    // something a caller handles separately and can forget.
    for status in [500, 502, 503, 504, 599] {
        let robots = Robots::for_status(status, b"User-agent: *\nAllow: /\n");
        assert_eq!(
            robots.allows("/anything"),
            Decision::Disallowed,
            "status {status}"
        );
        assert_eq!(robots.provenance(), Provenance::ServerError);
        assert!(robots.is_blanket_disallow());
    }
}

#[test]
fn a_4xx_means_fully_allowed() {
    // RFC 9309 2.3.1.3. Most of the web has no robots.txt and a 404 is the
    // ordinary case, not an error.
    for status in [400, 401, 403, 404, 410, 451] {
        let robots = Robots::for_status(status, b"");
        assert_eq!(
            robots.allows("/anything"),
            Decision::Allowed,
            "status {status}"
        );
        assert_eq!(robots.provenance(), Provenance::NotFound);
    }
}

#[test]
fn a_429_goes_with_the_5xx_and_not_with_the_rest_of_the_4xx() {
    // The one 4xx that is not a site saying it has no rules. A 429 is a site
    // saying we are already asking too often, and reading that as permission to
    // crawl the whole host is the worst available answer to it. Google's
    // implementation carves it out of the 4xx rule for the same reason and doc
    // 07.6 already backs a host off hard on one.
    let robots = Robots::for_status(429, b"");
    assert_eq!(robots.allows("/anything"), Decision::Disallowed);
    assert_eq!(robots.provenance(), Provenance::ServerError);
    assert!(robots.is_blanket_disallow());
}

#[test]
fn anything_unclassifiable_is_disallowed_rather_than_allowed() {
    // A 3xx reaching here means the caller ran out of redirect hops. Unknown
    // is not the same as permitted.
    for status in [0, 100, 301, 302, 307, 308, 999] {
        let robots = Robots::for_status(status, b"");
        assert_eq!(
            robots.allows("/anything"),
            Decision::Disallowed,
            "status {status}"
        );
        assert_eq!(robots.provenance(), Provenance::Unreachable);
    }
}

#[test]
fn a_2xx_is_parsed() {
    let robots = Robots::for_status(200, b"User-agent: *\nDisallow: /x\n");
    assert_eq!(robots.provenance(), Provenance::Parsed);
    assert_eq!(robots.allows("/x"), Decision::Disallowed);
    assert_eq!(robots.allows("/y"), Decision::Allowed);
}

#[test]
fn oversized_files_are_truncated_at_the_documented_limit() {
    // RFC 9309 2.5. Files this big are always generated, and the answer is to
    // parse what fits rather than to fail.
    let mut body = String::from("User-agent: *\nDisallow: /early\n");
    while body.len() < MAX_BYTES {
        body.push_str("Disallow: /filler\n");
    }
    body.push_str("Disallow: /past-the-cap\n");
    assert!(body.len() > MAX_BYTES);

    let robots = Robots::parse(body.as_bytes());
    assert_eq!(robots.allows("/early"), Decision::Disallowed);
    assert_eq!(
        robots.allows("/past-the-cap"),
        Decision::Allowed,
        "a rule past the 500 KiB cap should not have been parsed"
    );
}

#[test]
fn truncation_never_splits_a_multibyte_character() {
    // Cutting at a byte offset can land mid character, and a lossy decode
    // would turn the tail into a replacement character. Harmless here because
    // the cap lands inside filler, but it must not panic.
    // Build a file where the 500 KiB boundary is guaranteed to land inside a
    // three byte character, then check that the rules before it survived and
    // nothing panicked on the way.
    let mut body = String::from("User-agent: *\nDisallow: /早い\n");
    while body.len() < MAX_BYTES + 4096 {
        body.push_str("Disallow: /日本語のパス\n");
    }
    let cut = &body.as_bytes()[..MAX_BYTES];
    assert!(
        std::str::from_utf8(cut).is_err(),
        "this test is only meaningful if the cap lands mid character"
    );

    let robots = Robots::parse(body.as_bytes());
    assert_eq!(robots.allows("/%E6%97%A9%E3%81%84"), Decision::Disallowed);
    assert!(robots.rule_count() > 100);
}

// ---------------------------------------------------------------------------
// Crawl-delay, Sitemap, Content-Usage
// ---------------------------------------------------------------------------

#[test]
fn crawl_delay_is_read_from_the_matching_group() {
    let body = "User-agent: *\nCrawl-delay: 30\n\nUser-agent: umi\nCrawl-delay: 2\n";
    assert_eq!(
        Robots::parse_str(body).crawl_delay(),
        Some(Duration::from_secs(2))
    );
}

#[test]
fn crawl_delay_accepts_a_fraction() {
    let body = "User-agent: *\nCrawl-delay: 0.5\n";
    assert_eq!(
        Robots::parse_str(body).crawl_delay(),
        Some(Duration::from_millis(500))
    );
}

#[test]
fn an_absurd_crawl_delay_is_clamped_and_flagged() {
    // Doc 07.4. `Crawl-delay: 86400` is a soft block dressed as politeness,
    // and honouring it literally means a frontier entry that never drains. The
    // host is deprioritised instead, so the scheduler has to be able to tell a
    // clamped value from a genuine 300.
    let robots = Robots::parse_str("User-agent: *\nCrawl-delay: 86400\n");
    assert_eq!(robots.crawl_delay(), Some(MAX_CRAWL_DELAY));
    assert!(robots.crawl_delay_was_clamped());

    let honest = Robots::parse_str("User-agent: *\nCrawl-delay: 10\n");
    assert!(!honest.crawl_delay_was_clamped());
}

#[test]
fn a_crawl_delay_too_big_for_a_duration_is_clamped_rather_than_a_panic() {
    // A number can be finite and still be more seconds than a `Duration`
    // holds, and the infallible conversion panics on exactly that. Two
    // prefetch runs died here after a million hosts each. Every one of these
    // is a value the parser accepts as an f64.
    for value in ["1e30", "1e300", "99999999999999999999999999", "1.8e19"] {
        let body = format!("User-agent: *\nCrawl-delay: {value}\n");
        let robots = Robots::parse_str(&body);
        assert_eq!(robots.crawl_delay(), Some(MAX_CRAWL_DELAY), "value {value:?}");
        assert!(robots.crawl_delay_was_clamped(), "value {value:?}");
    }
}

#[test]
fn a_tiny_crawl_delay_is_clamped_up() {
    let robots = Robots::parse_str("User-agent: *\nCrawl-delay: 0.001\n");
    assert_eq!(robots.crawl_delay(), Some(MIN_CRAWL_DELAY));
}

#[test]
fn a_nonsense_crawl_delay_is_ignored_rather_than_defaulted() {
    for value in ["abc", "-5", "", "NaN", "1e400"] {
        let body = format!("User-agent: *\nCrawl-delay: {value}\n");
        assert_eq!(
            Robots::parse_str(&body).crawl_delay(),
            None,
            "value {value:?}"
        );
    }
}

#[test]
fn sitemaps_are_collected_regardless_of_group() {
    // Sitemap is file scoped, not group scoped, so a line inside another
    // crawler's block still counts. Doc 07.4 calls this the highest value line
    // in the file and it is routinely thrown away by crawlers.
    let body = "\
Sitemap: https://example.com/sitemap.xml

User-agent: googlebot
Sitemap: https://example.com/news.xml
Disallow: /

User-agent: umi
Disallow: /x
";
    let robots = Robots::parse_str(body);
    assert_eq!(
        robots.sitemaps(),
        [
            "https://example.com/sitemap.xml",
            "https://example.com/news.xml"
        ]
    );
}

#[test]
fn group_count_covers_the_whole_file_and_not_just_our_group() {
    // The published snapshot reports this next to the rule count, and the
    // pair only says something if the group count is the file's rather than
    // ours. Three groups here and we match the last one.
    let body = "\
User-agent: googlebot
Disallow: /a

User-agent: bingbot
User-agent: yandex
Disallow: /b

User-agent: umi
Disallow: /c
";
    let robots = Robots::parse_str(body);
    assert_eq!(robots.group_count(), 3);
    assert_eq!(robots.rule_count(), 1);
}

#[test]
fn group_count_is_zero_without_a_file_behind_it() {
    // A status is not a file, so there is nothing to count. This is the
    // difference a reader needs between a site that published an empty group
    // and a site that was down when we asked.
    assert_eq!(Robots::for_status(503, b"").group_count(), 0);
    assert_eq!(Robots::for_status(404, b"").group_count(), 0);
    // A file with rules but no user agent line has no groups either, and the
    // rules in it apply to nobody.
    assert_eq!(Robots::parse_str("Disallow: /\n").group_count(), 0);
}

#[test]
fn content_usage_is_recorded_and_not_acted_on() {
    // Doc 07.5. AIPREF expresses a preference about AI training, our purpose
    // is index building, and the honest thing is to carry the preference
    // downstream rather than decide on a reader's behalf. So it must appear in
    // the parse and must not change any crawl decision.
    let body = "\
User-agent: *
Content-Usage: train-ai=n
Content-Usage: /private/ search=n
Disallow: /x
";
    let robots = Robots::parse_str(body);
    assert_eq!(robots.content_usage(), ["train-ai=n", "/private/ search=n"]);
    assert_eq!(robots.allows("/private/page"), Decision::Allowed);
    assert_eq!(robots.allows("/x"), Decision::Disallowed);
}

#[test]
fn content_usage_takes_the_value_up_to_a_comment() {
    let robots = Robots::parse_str("Content-Usage:   train-ai=n   # our policy\n");
    assert_eq!(robots.content_usage(), ["train-ai=n"]);
}

// ---------------------------------------------------------------------------
// Shapes real sites produce
// ---------------------------------------------------------------------------

#[test]
fn a_realistic_file_parses_the_way_a_reader_would_expect() {
    let body = "\
# robots.txt for example.com
User-agent: *
Disallow: /cgi-bin/
Disallow: /tmp/
Disallow: /search?
Disallow: /*.pdf$
Allow: /search/about
Crawl-delay: 1

User-agent: BadBot
Disallow: /

Sitemap: https://example.com/sitemap_index.xml
";
    let robots = Robots::parse_str(body);
    assert_eq!(robots.allows("/index.html"), Decision::Allowed);
    assert_eq!(robots.allows("/cgi-bin/thing"), Decision::Disallowed);
    assert_eq!(robots.allows("/search?q=1"), Decision::Disallowed);
    assert_eq!(robots.allows("/search/about"), Decision::Allowed);
    assert_eq!(robots.allows("/doc.pdf"), Decision::Disallowed);
    assert_eq!(robots.allows("/doc.pdf?v=2"), Decision::Allowed);
    assert_eq!(robots.crawl_delay(), Some(Duration::from_secs(1)));
    assert_eq!(robots.sitemaps().len(), 1);
    assert!(!robots.is_blanket_disallow());
}

#[test]
fn a_blanket_disallow_is_detected_so_the_frontier_can_skip_the_host() {
    assert!(Robots::parse_str("User-agent: *\nDisallow: /\n").is_blanket_disallow());
    assert!(!Robots::parse_str("User-agent: *\nDisallow: /x\n").is_blanket_disallow());
    assert!(!Robots::parse_str("User-agent: *\nDisallow: /\nAllow: /ok\n").is_blanket_disallow());
}

#[test]
fn an_html_error_page_served_as_robots_txt_does_not_block_the_site() {
    // Extremely common: a CDN returns a styled 200 error page for
    // /robots.txt. It has no colons that parse as directives, so it becomes an
    // empty rule set, which is allow all. Reading it as a block would take a
    // whole site out of the crawl over a misconfigured CDN.
    let body = "<!DOCTYPE html><html><head><title>404</title></head><body>Not found</body></html>";
    let robots = Robots::parse_str(body);
    assert_eq!(robots.allows("/anything"), Decision::Allowed);
    assert_eq!(robots.rule_count(), 0);
}

#[test]
fn an_html_error_page_served_as_robots_txt_yields_no_rules() {
    // Measured at 24 of 638 real hosts, so a crawl hits this several million
    // times over 100B pages. The site has no robots.txt and its server
    // answers 200 with a styled error page instead of 404.
    //
    // Nothing in it may become a rule. A stray `Disallow` conjured out of an
    // error page would take a whole site out of the frontier on the strength
    // of a misconfigured web server, and the site would have no way to tell
    // that had happened. The protection is that a field name has to be one we
    // know or be followed by a colon, and markup is neither.
    let body = "\
<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <title>404 Not Found</title>
  <style>body { color: #333; background: #fff; font-size: 14px; }</style>
  <script src=\"https://example.com/a.js\"></script>
</head>
<body>
  <p>Disallow all robots from this page</p>
  <a href=\"/sitemap.xml\">Sitemap: our sitemap</a>
</body>
</html>
";
    let robots = Robots::parse(body.as_bytes());
    assert_eq!(robots.rule_count(), 0);
    assert!(robots.allows("/").is_allowed());
    assert!(robots.allows("/anything/at/all").is_allowed());
    assert!(!robots.is_blanket_disallow());
    // The `<a href>` line has a colon in it and the text before that colon is
    // not a field we know, so it does not become a sitemap either.
    assert!(robots.sitemaps().is_empty());
}

// ---------------------------------------------------------------------------
// The two helpers the conformance suite tests directly. Upstream calls these
// `TestGetPathParamsQuery` and `TestMaybeEscapePattern`; they are here rather
// than in `tests/conformance.rs` because they reach past the public decision
// API, and the generator only emits `IsUserAgentAllowed` cases.
// ---------------------------------------------------------------------------

#[test]
fn the_path_of_a_url_is_what_robots_talks_about() {
    // Only testing URLs that are already correctly escaped here.
    assert_eq!(path_of(""), "/");
    assert_eq!(path_of("http://www.example.com"), "/");
    assert_eq!(path_of("http://www.example.com/"), "/");
    assert_eq!(path_of("http://www.example.com/a"), "/a");
    assert_eq!(path_of("http://www.example.com/a/"), "/a/");
    assert_eq!(
        path_of("http://www.example.com/a/b?c=http://d.e/"),
        "/a/b?c=http://d.e/"
    );
    assert_eq!(
        path_of("http://www.example.com/a/b?c=d&e=f#fragment"),
        "/a/b?c=d&e=f"
    );
    assert_eq!(path_of("example.com"), "/");
    assert_eq!(path_of("example.com/"), "/");
    assert_eq!(path_of("example.com/a"), "/a");
    assert_eq!(path_of("example.com/a/"), "/a/");
    assert_eq!(path_of("example.com/a/b?c=d&e=f#fragment"), "/a/b?c=d&e=f");
    assert_eq!(path_of("a"), "/");
    assert_eq!(path_of("a/"), "/");
    assert_eq!(path_of("/a"), "/a");
    assert_eq!(path_of("a/b"), "/b");
    assert_eq!(path_of("example.com?a"), "/?a");
    assert_eq!(path_of("example.com/a;b#c"), "/a;b");
    assert_eq!(path_of("//a/b/c"), "/b/c");
}

#[test]
fn a_pattern_is_encoded_into_the_alphabet_a_url_uses() {
    assert_eq!(
        escape_pattern("http://www.example.com"),
        "http://www.example.com"
    );
    assert_eq!(escape_pattern("/a/b/c"), "/a/b/c");
    assert_eq!(escape_pattern("á"), "%C3%A1");
    assert_eq!(escape_pattern("%aa"), "%AA");
    // A stray `%` that is not an escape is left where it is rather than being
    // encoded into `%25`. Sites write `Disallow: /*%` meaning the character,
    // and rewriting it would stop the rule matching anything.
    assert_eq!(escape_pattern("/50%"), "/50%");
    assert_eq!(escape_pattern("/a%zz"), "/a%zz");
}
