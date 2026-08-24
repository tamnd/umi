//! Canonicalisation and robots matching have to agree on one alphabet.
//!
//! RFC 9309 section 2.2.2 compares percent-encoded octets, so
//! `Disallow: /admin` does not match the literal string `/%61dmin`. On its own
//! that reads like the oldest robots bypass there is: encode one letter of the
//! path and walk straight past the rule.
//!
//! It is not one, because nothing hands a raw link to the matcher. Doc 11.2
//! step 6 decodes every unreserved escape during canonicalisation, and the
//! frontier only ever holds canonical URLs, so `%61` is gone before a decision
//! is asked for. That argument spans two crates and neither one's tests can
//! close it, which is what this file is for.

use umi_robots::Robots;
use umi_types::canon::canonicalize;

/// The real path: canonicalise the link, then ask.
#[track_caller]
fn allowed(body: &str, url: &str) -> bool {
    let canonical = canonicalize(url, None).expect("test urls are well formed");
    Robots::parse_str(body).allows_url(&canonical).is_allowed()
}

#[test]
fn encoding_a_letter_does_not_get_past_a_disallow() {
    let body = "User-agent: *\nDisallow: /admin\n";
    assert!(!allowed(body, "https://example.com/admin"));
    assert!(!allowed(body, "https://example.com/%61dmin"));
    assert!(!allowed(body, "https://example.com/adm%69n"));
    assert!(!allowed(body, "https://example.com/%61%64%6D%69%6E"));
    // The case of the hex digits does not change which octet is named.
    assert!(!allowed(body, "https://example.com/admi%6E"));
    assert!(!allowed(body, "https://example.com/admi%6e"));
    // `%41` is `A`, and paths are case sensitive, so `/Admin` really is a
    // different path that this rule does not cover. Decoding the escape is not
    // the same as folding the case, and only the first one happens.
    assert!(allowed(body, "https://example.com/%41dmin"));
}

#[test]
fn a_reserved_character_is_not_decoded_into_a_path_boundary() {
    // `%2F` is reserved, so canonicalisation leaves it encoded and the rule
    // for `/a/b` does not reach `/a%2Fb`. They are different resources and the
    // origin is the one that decides that.
    let body = "User-agent: *\nDisallow: /a/b\n";
    assert!(!allowed(body, "https://example.com/a/b"));
    assert!(allowed(body, "https://example.com/a%2Fb"));
    assert!(allowed(body, "https://example.com/a%2fb"));
}

#[test]
fn a_non_ascii_pattern_meets_the_url_that_goes_on_the_wire() {
    // The site writes the path in UTF-8, the fetcher requests it encoded.
    // Canonicalisation produces the encoded form and the pattern is encoded to
    // match it.
    let body = "User-agent: *\nDisallow: /café/\n";
    assert!(!allowed(body, "https://example.com/café/menu"));
    assert!(!allowed(body, "https://example.com/caf%C3%A9/menu"));
    assert!(allowed(body, "https://example.com/tea/menu"));
}

#[test]
fn a_dot_segment_does_not_get_past_a_disallow() {
    // `/public/../admin` is `/admin`, and canonicalisation resolves it before
    // the matcher ever sees it.
    let body = "User-agent: *\nDisallow: /admin\n";
    assert!(!allowed(body, "https://example.com/public/../admin"));
    assert!(!allowed(body, "https://example.com/./admin"));
    // An empty segment is not a dot segment. RFC 3986 section 5.2.4 removes
    // `.` and `..` and leaves `//` alone, and so does canonicalisation, so
    // `//admin` stays a path this rule does not cover. That is the origin's
    // call: plenty of servers serve something different there, and collapsing
    // it here would have us claim a rule the site did not write.
    assert!(allowed(body, "https://example.com//admin"));
}
