//! The Google robots.txt conformance corpus, transpiled.
//!
//! Every one of these cases comes from `robots_test.cc` in
//! <https://github.com/google/robotstxt>, the reference implementation the
//! RFC 9309 authors wrote alongside the document. The comment above each test
//! is the upstream comment verbatim, so the RFC citation travels with the
//! case. Names beginning `rfc_` are cases the RFC itself specifies; names
//! beginning `google_` are Google extensions that the RFC does not require and
//! that we follow anyway, each explained where it is implemented.
//!
//! Only the `IsUserAgentAllowed` assertions are here, which is 145 of the
//! suite's 146. The remainder exercise Google's parse-callback API, which is a
//! shape of their library rather than a rule about robots.txt, and we have no
//! equivalent to run them against.
//!
//! This file is generated. Do not edit it to make a test pass. If a case here
//! disagrees with the crate, one of the two is wrong about RFC 9309 and the
//! answer is in the RFC, not in this file.

use umi_robots::Robots;

/// The suite's helper: does this robots.txt allow this agent to fetch this URL.
///
/// The agent list is the queried agent then `*`, which is the fallback RFC
/// 9309 section 2.2.1 requires when no group names the agent.
fn allowed(robotstxt: impl AsRef<str>, agent: impl AsRef<str>, url: impl AsRef<str>) -> bool {
    let agent = agent.as_ref();
    Robots::parse_for(robotstxt.as_ref(), &[agent, "*"])
        .allows_url(url.as_ref())
        .is_allowed()
}

/// Google-specific: long lines are ignored after 8 * 2083 bytes. See comment in
/// RobotsTxtParser::Parse().
///
/// Written by hand rather than transpiled, because upstream builds the file
/// with a loop. The arithmetic is copied exactly: the pattern is grown until
/// the line is a few bytes over the cap, so the assertions pin down where the
/// cut lands and not merely that one happened.
#[test]
fn google_line_too_long() {
    const EOL: usize = 1;
    const MAX_LINE: usize = 2083 * 8;

    {
        let disallow = "disallow: ";
        let mut longline = String::from("/x/");
        let max_length = MAX_LINE - longline.len() - disallow.len() + EOL;
        while longline.len() < max_length {
            longline.push('a');
        }
        let robotstxt = format!("user-agent: FooBot\n{disallow}{longline}/qux\n");

        // Matches nothing, so URL is allowed.
        assert!(allowed(&robotstxt, "FooBot", "http://foo.bar/fux"));
        // Matches cut off disallow rule.
        assert!(!allowed(
            &robotstxt,
            "FooBot",
            format!("http://foo.bar{longline}/fux")
        ));
    }

    {
        let allow = "allow: ";
        let mut longline_a = String::from("/x/");
        let mut longline_b = String::from("/x/");
        let max_length = MAX_LINE - longline_a.len() - allow.len() + EOL;
        while longline_a.len() < max_length {
            longline_a.push('a');
            longline_b.push('b');
        }
        let robotstxt = format!(
            "user-agent: FooBot\ndisallow: /\n{allow}{longline_a}/qux\n{allow}{longline_b}/qux\n"
        );

        // URL matches the disallow rule.
        assert!(!allowed(&robotstxt, "FooBot", "http://foo.bar/"));
        // Matches the allow rule exactly.
        assert!(allowed(
            &robotstxt,
            "FooBot",
            format!("http://foo.bar{longline_a}/qux")
        ));
        // Matches cut off allow rule.
        assert!(allowed(
            &robotstxt,
            "FooBot",
            format!("http://foo.bar{longline_b}/fux")
        ));
    }
}

// Google-specific: system test.
#[test]
fn google_system_test() {
    let robotstxt = "user-agent: FooBot\ndisallow: /\n";
    assert!(allowed("", "FooBot", ""));
    assert!(allowed(robotstxt, "", ""));
    assert!(!allowed(robotstxt, "FooBot", ""));
    assert!(allowed("", "", ""));
}

// Rules are colon separated name-value pairs. The following names are
// provisioned:
// user-agent: <value>
// allow: <value>
// disallow: <value>
// See REP RFC section "Protocol Definition".
// https://www.rfc-editor.org/rfc/rfc9309.html#section-2.1
//
// Google specific: webmasters sometimes miss the colon separator, but it's
// obvious what they mean by "disallow /", so we assume the colon if it's
// missing.
#[test]
fn rfc_line_syntax_line() {
    let robotstxt_correct = "user-agent: FooBot\ndisallow: /\n";
    let robotstxt_incorrect = "foo: FooBot\nbar: /\n";
    let robotstxt_incorrect_accepted = "user-agent FooBot\ndisallow /\n";
    let url = "http://foo.bar/x/y";
    assert!(!allowed(robotstxt_correct, "FooBot", url));
    assert!(allowed(robotstxt_incorrect, "FooBot", url));
    assert!(!allowed(robotstxt_incorrect_accepted, "FooBot", url));
}

// A group is one or more user-agent line followed by rules, and terminated
// by a another user-agent line. Rules for same user-agents are combined
// opaquely into one group. Rules outside groups are ignored.
// See REP RFC section "Protocol Definition".
// https://www.rfc-editor.org/rfc/rfc9309.html#section-2.1
#[test]
fn rfc_line_syntax_groups() {
    let robotstxt = "allow: /foo/bar/\n\nuser-agent: FooBot\ndisallow: /\nallow: /x/\nuser-agent: BarBot\ndisallow: /\nallow: /y/\n\n\nallow: /w/\nuser-agent: BazBot\n\nuser-agent: FooBot\nallow: /z/\ndisallow: /\n";
    let url_w = "http://foo.bar/w/a";
    let url_x = "http://foo.bar/x/b";
    let url_y = "http://foo.bar/y/c";
    let url_z = "http://foo.bar/z/d";
    let url_foo = "http://foo.bar/foo/bar/";
    assert!(allowed(robotstxt, "FooBot", url_x));
    assert!(allowed(robotstxt, "FooBot", url_z));
    assert!(!allowed(robotstxt, "FooBot", url_y));
    assert!(allowed(robotstxt, "BarBot", url_y));
    assert!(allowed(robotstxt, "BarBot", url_w));
    assert!(!allowed(robotstxt, "BarBot", url_z));
    assert!(allowed(robotstxt, "BazBot", url_z));
    assert!(!allowed(robotstxt, "FooBot", url_foo));
    assert!(!allowed(robotstxt, "BarBot", url_foo));
    assert!(!allowed(robotstxt, "BazBot", url_foo));
}

// Group must not be closed by rules not explicitly defined in the REP RFC.
// See REP RFC section "Protocol Definition".
// https://www.rfc-editor.org/rfc/rfc9309.html#section-2.1
#[test]
fn rfc_line_syntax_groups_other_rules() {
    let robotstxt =
        "User-agent: BarBot\nSitemap: https://foo.bar/sitemap\nUser-agent: *\nDisallow: /\n";
    let url = "http://foo.bar/";
    assert!(!allowed(robotstxt, "FooBot", url));
    assert!(!allowed(robotstxt, "BarBot", url));
    let robotstxt =
        "User-agent: FooBot\nInvalid-Unknown-Line: unknown\nUser-agent: *\nDisallow: /\n";
    let url = "http://foo.bar/";
    assert!(!allowed(robotstxt, "FooBot", url));
    assert!(!allowed(robotstxt, "BarBot", url));
}

// REP lines are case insensitive. See REP RFC section "Protocol Definition".
// https://www.rfc-editor.org/rfc/rfc9309.html#section-2.1
#[test]
fn rfc_rep_line_names_case_insensitive() {
    let robotstxt_upper = "USER-AGENT: FooBot\nALLOW: /x/\nDISALLOW: /\n";
    let robotstxt_lower = "user-agent: FooBot\nallow: /x/\ndisallow: /\n";
    let robotstxt_camel = "uSeR-aGeNt: FooBot\nAlLoW: /x/\ndIsAlLoW: /\n";
    let url_allowed = "http://foo.bar/x/y";
    let url_disallowed = "http://foo.bar/a/b";
    assert!(allowed(robotstxt_upper, "FooBot", url_allowed));
    assert!(allowed(robotstxt_lower, "FooBot", url_allowed));
    assert!(allowed(robotstxt_camel, "FooBot", url_allowed));
    assert!(!allowed(robotstxt_upper, "FooBot", url_disallowed));
    assert!(!allowed(robotstxt_lower, "FooBot", url_disallowed));
    assert!(!allowed(robotstxt_camel, "FooBot", url_disallowed));
}

// User-agent line values are case insensitive. See REP RFC section "The
// user-agent line".
// https://www.rfc-editor.org/rfc/rfc9309.html#section-2.2.1
#[test]
fn rfc_user_agent_value_case_insensitive() {
    let robotstxt_upper = "User-Agent: FOO BAR\nAllow: /x/\nDisallow: /\n";
    let robotstxt_lower = "User-Agent: foo bar\nAllow: /x/\nDisallow: /\n";
    let robotstxt_camel = "User-Agent: FoO bAr\nAllow: /x/\nDisallow: /\n";
    let url_allowed = "http://foo.bar/x/y";
    let url_disallowed = "http://foo.bar/a/b";
    assert!(allowed(robotstxt_upper, "Foo", url_allowed));
    assert!(allowed(robotstxt_lower, "Foo", url_allowed));
    assert!(allowed(robotstxt_camel, "Foo", url_allowed));
    assert!(!allowed(robotstxt_upper, "Foo", url_disallowed));
    assert!(!allowed(robotstxt_lower, "Foo", url_disallowed));
    assert!(!allowed(robotstxt_camel, "Foo", url_disallowed));
    assert!(allowed(robotstxt_upper, "foo", url_allowed));
    assert!(allowed(robotstxt_lower, "foo", url_allowed));
    assert!(allowed(robotstxt_camel, "foo", url_allowed));
    assert!(!allowed(robotstxt_upper, "foo", url_disallowed));
    assert!(!allowed(robotstxt_lower, "foo", url_disallowed));
    assert!(!allowed(robotstxt_camel, "foo", url_disallowed));
}

// Google specific: accept user-agent value up to the first space. Space is not
// allowed in user-agent values, but that doesn't stop webmasters from using
// them. This is more restrictive than the RFC, since in case of the bad value
// "Googlebot Images" we'd still obey the rules with "Googlebot".
// Extends REP RFC section "The user-agent line"
// https://www.rfc-editor.org/rfc/rfc9309.html#section-2.2.1
#[test]
fn google_accept_user_agent_up_to_first_space() {
    let robotstxt = "User-Agent: *\nDisallow: /\nUser-Agent: Foo Bar\nAllow: /x/\nDisallow: /\n";
    let url = "http://foo.bar/x/y";
    assert!(allowed(robotstxt, "Foo", url));
    assert!(!allowed(robotstxt, "Foo Bar", url));
}

// If no group matches the user-agent, crawlers must obey the first group with a
// user-agent line with a "*" value, if present. If no group satisfies either
// condition, or no groups are present at all, no rules apply.
// See REP RFC section "The user-agent line".
// https://www.rfc-editor.org/rfc/rfc9309.html#section-2.2.1
#[test]
fn rfc_global_groups_secondary() {
    let robotstxt_empty = "";
    let robotstxt_global = "user-agent: *\nallow: /\nuser-agent: FooBot\ndisallow: /\n";
    let robotstxt_only_specific = "user-agent: FooBot\nallow: /\nuser-agent: BarBot\ndisallow: /\nuser-agent: BazBot\ndisallow: /\n";
    let url = "http://foo.bar/x/y";
    assert!(allowed(robotstxt_empty, "FooBot", url));
    assert!(!allowed(robotstxt_global, "FooBot", url));
    assert!(allowed(robotstxt_global, "BarBot", url));
    assert!(allowed(robotstxt_only_specific, "QuxBot", url));
}

// Matching rules against URIs is case sensitive.
// See REP RFC section "The Allow and Disallow lines".
// https://www.rfc-editor.org/rfc/rfc9309.html#section-2.2.2
#[test]
fn rfc_allow_disallow_value_case_sensitive() {
    let robotstxt_lowercase_url = "user-agent: FooBot\ndisallow: /x/\n";
    let robotstxt_uppercase_url = "user-agent: FooBot\ndisallow: /X/\n";
    let url = "http://foo.bar/x/y";
    assert!(!allowed(robotstxt_lowercase_url, "FooBot", url));
    assert!(allowed(robotstxt_uppercase_url, "FooBot", url));
}

// The most specific match found MUST be used. The most specific match is the
// match that has the most octets. In case of multiple rules with the same
// length, the least strict rule must be used.
// See REP RFC section "The Allow and Disallow lines".
// https://www.rfc-editor.org/rfc/rfc9309.html#section-2.2.2
#[test]
fn rfc_longest_match() {
    let url = "http://foo.bar/x/page.html";
    let robotstxt = "user-agent: FooBot\ndisallow: /x/page.html\nallow: /x/\n";
    assert!(!allowed(robotstxt, "FooBot", url));
    let robotstxt = "user-agent: FooBot\nallow: /x/page.html\ndisallow: /x/\n";
    assert!(allowed(robotstxt, "FooBot", url));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/x/"));
    let robotstxt = "user-agent: FooBot\ndisallow: \nallow: \n";
    assert!(allowed(robotstxt, "FooBot", url));
    let robotstxt = "user-agent: FooBot\ndisallow: /\nallow: /\n";
    assert!(allowed(robotstxt, "FooBot", url));
    let url_a = "http://foo.bar/x";
    let url_b = "http://foo.bar/x/";
    let robotstxt = "user-agent: FooBot\ndisallow: /x\nallow: /x/\n";
    assert!(!allowed(robotstxt, "FooBot", url_a));
    assert!(allowed(robotstxt, "FooBot", url_b));
    let robotstxt = "user-agent: FooBot\ndisallow: /x/page.html\nallow: /x/page.html\n";
    assert!(allowed(robotstxt, "FooBot", url));
    let robotstxt = "user-agent: FooBot\nallow: /page\ndisallow: /*.html\n";
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/page.html"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/page"));
    let robotstxt = "user-agent: FooBot\nallow: /x/page.\ndisallow: /*.html\n";
    assert!(allowed(robotstxt, "FooBot", url));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/x/y.html"));
    let robotstxt = "User-agent: *\nDisallow: /x/\nUser-agent: FooBot\nDisallow: /y/\n";
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/x/page"));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/y/page"));
}

// Octets in the URI and robots.txt paths outside the range of the US-ASCII
// coded character set, and those in the reserved range defined by RFC3986,
// MUST be percent-encoded as defined by RFC3986 prior to comparison.
// See REP RFC section "The Allow and Disallow lines".
// https://www.rfc-editor.org/rfc/rfc9309.html#section-2.2.2
//
// NOTE: It's up to the caller to percent encode a URL before passing it to the
// parser. Percent encoding URIs in the rules is unnecessary.
#[test]
fn rfc_encoding() {
    let robotstxt =
        "User-agent: FooBot\nDisallow: /\nAllow: /foo/bar?qux=taz&baz=http://foo.bar?tar&par\n";
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/foo/bar?qux=taz&baz=http://foo.bar?tar&par"
    ));
    let robotstxt = "User-agent: FooBot\nDisallow: /\nAllow: /foo/bar/ツ\n";
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/foo/bar/%E3%83%84"
    ));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/foo/bar/ツ"));
    let robotstxt = "User-agent: FooBot\nDisallow: /\nAllow: /foo/bar/%E3%83%84\n";
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/foo/bar/%E3%83%84"
    ));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/foo/bar/ツ"));
    let robotstxt = "User-agent: FooBot\nDisallow: /\nAllow: /foo/bar/%62%61%7A\n";
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/foo/bar/baz"));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/foo/bar/%62%61%7A"
    ));
}

// The REP RFC defines the following characters that have special meaning in
// robots.txt:
// # - inline comment.
// $ - end of pattern.
// * - any number of characters.
// See REP RFC section "Special Characters".
// https://www.rfc-editor.org/rfc/rfc9309.html#section-2.2.3
#[test]
fn rfc_special_characters() {
    let robotstxt = "User-agent: FooBot\nDisallow: /foo/bar/quz\nAllow: /foo/*/qux\n";
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/foo/bar/quz"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/foo/quz"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/foo//quz"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/foo/bax/quz"));
    let robotstxt = "User-agent: FooBot\nDisallow: /foo/bar$\nAllow: /foo/bar/qux\n";
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/foo/bar"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/foo/bar/qux"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/foo/bar/"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/foo/bar/baz"));
    let robotstxt = "User-agent: FooBot\n# Disallow: /\nDisallow: /foo/quz#qux\nAllow: /\n";
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/foo/bar"));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/foo/quz"));
}

// Google-specific: "index.html" (and only that) at the end of a pattern is
// equivalent to "/".
#[test]
fn google_index_html_is_directory() {
    let robotstxt = "User-Agent: *\nAllow: /allowed-slash/index.html\nDisallow: /\n";
    assert!(allowed(
        robotstxt,
        "foobot",
        "http://foo.com/allowed-slash/"
    ));
    assert!(!allowed(
        robotstxt,
        "foobot",
        "http://foo.com/allowed-slash/index.htm"
    ));
    assert!(allowed(
        robotstxt,
        "foobot",
        "http://foo.com/allowed-slash/index.html"
    ));
    assert!(!allowed(robotstxt, "foobot", "http://foo.com/anyother-url"));
}

#[test]
fn google_documentation_checks() {
    let robotstxt = "user-agent: FooBot\ndisallow: /\nallow: /fish\n";
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/bar"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/fish"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/fish.html"));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/fish/salmon.html"
    ));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/fishheads"));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/fishheads/yummy.html"
    ));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/fish.html?id=anything"
    ));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/Fish.asp"));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/catfish"));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/?id=fish"));
    let robotstxt = "user-agent: FooBot\ndisallow: /\nallow: /fish*\n";
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/bar"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/fish"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/fish.html"));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/fish/salmon.html"
    ));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/fishheads"));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/fishheads/yummy.html"
    ));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/fish.html?id=anything"
    ));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/Fish.bar"));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/catfish"));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/?id=fish"));
    let robotstxt = "user-agent: FooBot\ndisallow: /\nallow: /fish/\n";
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/bar"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/fish/"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/fish/salmon"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/fish/?salmon"));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/fish/salmon.html"
    ));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/fish/?id=anything"
    ));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/fish"));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/fish.html"));
    assert!(!allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/Fish/Salmon.html"
    ));
    let robotstxt = "user-agent: FooBot\ndisallow: /\nallow: /*.php\n";
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/bar"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/filename.php"));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/folder/filename.php"
    ));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/folder/filename.php?parameters"
    ));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar//folder/any.php.file.html"
    ));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/filename.php/"));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/index?f=filename.php/"
    ));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/php/"));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/index?php"));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/windows.PHP"));
    let robotstxt = "user-agent: FooBot\ndisallow: /\nallow: /*.php$\n";
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/bar"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/filename.php"));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/folder/filename.php"
    ));
    assert!(!allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/filename.php?parameters"
    ));
    assert!(!allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/filename.php/"
    ));
    assert!(!allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/filename.php5"
    ));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/php/"));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/filename?php"));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/aaaphpaaa"));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar//windows.PHP"));
    let robotstxt = "user-agent: FooBot\ndisallow: /\nallow: /fish*.php\n";
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/bar"));
    assert!(allowed(robotstxt, "FooBot", "http://foo.bar/fish.php"));
    assert!(allowed(
        robotstxt,
        "FooBot",
        "http://foo.bar/fishheads/catfish.php?parameters"
    ));
    assert!(!allowed(robotstxt, "FooBot", "http://foo.bar/Fish.PHP"));
    let robotstxt = "user-agent: FooBot\nallow: /p\ndisallow: /\n";
    let url = "http://example.com/page";
    assert!(allowed(robotstxt, "FooBot", url));
    let robotstxt = "user-agent: FooBot\nallow: /folder\ndisallow: /folder\n";
    let url = "http://example.com/folder/page";
    assert!(allowed(robotstxt, "FooBot", url));
    let robotstxt = "user-agent: FooBot\nallow: /page\ndisallow: /*.htm\n";
    let url = "http://example.com/page.htm";
    assert!(!allowed(robotstxt, "FooBot", url));
    let robotstxt = "user-agent: FooBot\nallow: /$\ndisallow: /\n";
    let url = "http://example.com/";
    let url_page = "http://example.com/page.html";
    assert!(allowed(robotstxt, "FooBot", url));
    assert!(!allowed(robotstxt, "FooBot", url_page));
}
