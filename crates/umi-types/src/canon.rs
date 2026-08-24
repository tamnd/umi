//! URL canonicalisation, version `canon/1`.
//!
//! Specified in `docs/spec/11-extraction-and-dedup.md` section 11.2, which
//! calls itself the most load bearing thirty lines in the spec and is not
//! exaggerating. [`UrlKey`](crate::UrlKey) is a fingerprint of the output of
//! this module, so what happens here defines identity for every URL in the
//! system.
//!
//! Wrong in the permissive direction and the frontier fills with the same page
//! forty times. Wrong in the aggressive direction and distinct pages collide
//! and are never crawled at all, silently. Changed later and every key in the
//! system means something different, which is why the version string is a
//! constant that ships in every segment header rather than something implied
//! by the build.
//!
//! Two steps look like bugs and are not. Query parameters are neither sorted
//! nor deduplicated (step 11), and trailing slashes are left exactly as found
//! (step 12). Both are standard canonicalisation moves, and both are wrong
//! often enough on real sites that the breakage outweighs the dedup win.

use std::borrow::Cow;
use std::fmt;

/// Why a URL was rejected rather than canonicalised.
///
/// Rejection is not failure. Most of these fire constantly on real link
/// graphs, because pages link to `mailto:`, `javascript:` and worse, and the
/// admission path treats a rejection as "not a crawl candidate" and moves on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CanonError {
    /// Not `http` or `https`. Step 1.
    NotHttp,
    /// Unparseable as a URL at all, or relative with no base.
    Malformed,
    /// No host, which `http://` allows syntactically and we do not.
    NoHost,
    /// The host failed IDNA validation. Step 4 rejects rather than falling
    /// back to raw bytes, because a host we cannot name is a host we cannot
    /// be polite to.
    BadHost,
    /// Longer than 2048 bytes after canonicalisation. Step 1.
    TooLong,
}

impl fmt::Display for CanonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::NotHttp => "not an http or https url",
            Self::Malformed => "malformed url",
            Self::NoHost => "no host",
            Self::BadHost => "host failed idna validation",
            Self::TooLong => "longer than 2048 bytes canonicalised",
        })
    }
}

impl std::error::Error for CanonError {}

/// The maximum canonical length, from step 1.
pub const MAX_URL_LEN: usize = 2048;

/// The step 10 parameter list, verbatim, as it ships and as it is published.
///
/// The list is data and it ships as data. Baking the text in with
/// `include_str!` keeps the binary self contained, and keeping it in a file
/// means the published copy in `open-index/umi-meta` is byte for byte the
/// thing the crawler actually ran, not a transcription of it. Anyone
/// reproducing a umi URL key from Python can read the file; nobody can read a
/// `const` in a Rust source tree.
pub const TRACKING_PARAMS_FILE: &str = include_str!("../../../data/canon/tracking-params.txt");

/// How a parameter name is matched.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Match {
    /// Exact name.
    Exact,
    /// Any name with this prefix.
    Prefix,
    /// Exact name, but only when the value looks like a session token.
    SessionValued,
}

/// One parsed rule from [`TRACKING_PARAMS_FILE`].
#[derive(Clone, Debug)]
pub struct TrackingRule {
    name: String,
    kind: Match,
}

/// Parse the rule file. Unknown syntax is skipped rather than rejected,
/// because a newer published list read by an older binary should degrade to
/// "removes fewer parameters" and not to "refuses to start".
#[must_use]
pub fn parse_tracking_params(text: &str) -> Vec<TrackingRule> {
    text.lines()
        .map(|line| line.split('#').next().unwrap_or("").trim())
        .filter(|line| !line.is_empty())
        .map(|line| {
            let lower = line.to_ascii_lowercase();
            if let Some(stem) = lower.strip_suffix('*') {
                TrackingRule {
                    name: stem.to_owned(),
                    kind: Match::Prefix,
                }
            } else if let Some(stem) = lower.strip_suffix('?') {
                TrackingRule {
                    name: stem.to_owned(),
                    kind: Match::SessionValued,
                }
            } else {
                TrackingRule {
                    name: lower,
                    kind: Match::Exact,
                }
            }
        })
        .collect()
}

/// The compiled in rule set, parsed once.
fn default_rules() -> &'static [TrackingRule] {
    use std::sync::OnceLock;
    static RULES: OnceLock<Vec<TrackingRule>> = OnceLock::new();
    RULES.get_or_init(|| parse_tracking_params(TRACKING_PARAMS_FILE))
}

/// Is this query parameter one step 10 removes.
///
/// `sid` is deliberately conditional: it is a session token on plenty of sites
/// and a legitimate content identifier on plenty of others, so it is removed
/// only when the value looks like one. Doc 11.2 says the same about `cid`,
/// which additionally needs a per host override list and so is not handled
/// here at all yet.
#[must_use]
pub fn is_tracking_param(name: &str, value: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    default_rules().iter().any(|rule| match rule.kind {
        Match::Exact => lower == rule.name,
        Match::Prefix => lower.starts_with(&rule.name),
        Match::SessionValued => lower == rule.name && looks_like_session_token(value),
    })
}

/// A session token heuristic: long enough, and hex or base64ish throughout.
///
/// Deliberately conservative. Removing a real content parameter merges two
/// distinct pages into one key and the page we drop is never crawled, which is
/// a silent loss. Keeping a session parameter costs one duplicate row, which
/// is noisy but recoverable. So the bar for removal is high.
fn looks_like_session_token(value: &str) -> bool {
    value.len() >= 16
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        && value.bytes().any(|b| b.is_ascii_digit())
}

/// Canonicalise `input`, resolving against `base` when it is relative.
///
/// Returns the canonical absolute form as a string. This is the function
/// [`UrlKey`](crate::UrlKey) is derived from, so its output is the definition
/// of URL identity in umi and every change to it is a version bump.
///
/// # Errors
///
/// Returns [`CanonError`] for anything that is not a crawlable http(s) URL.
/// Callers on the admission path should treat that as "not a candidate" and
/// carry on, not as an error worth reporting.
pub fn canonicalize(input: &str, base: Option<&str>) -> Result<String, CanonError> {
    let raw = input.trim();
    if raw.is_empty() {
        return Err(CanonError::Malformed);
    }

    // Control characters and raw whitespace inside a URL are a parser
    // divergence waiting to happen: browsers strip them, some servers do not,
    // and an attacker choosing which one we behave like is not a position to
    // be in. Strip them before parsing, as WHATWG specifies.
    let cleaned: Cow<'_, str> = if raw.bytes().any(|b| b <= 0x20 || b == 0x7f) {
        Cow::Owned(
            raw.chars()
                .filter(|c| !c.is_control() && *c != ' ')
                .collect(),
        )
    } else {
        Cow::Borrowed(raw)
    };

    let parsed = match base {
        Some(b) => {
            let base_url = url::Url::parse(b).map_err(|_| CanonError::Malformed)?;
            base_url.join(&cleaned).map_err(|_| CanonError::Malformed)?
        }
        None => url::Url::parse(&cleaned).map_err(|_| CanonError::Malformed)?,
    };

    // Step 1: scheme. Checked before anything else so that `mailto:` and
    // `javascript:` cost one comparison, which matters when admission is
    // running at 12500 candidates per second and most links on a page are
    // neither.
    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(CanonError::NotHttp);
    }

    // Steps 2 and 4: the `url` crate has already lowercased the scheme and
    // host and applied UTS-46 during parsing. What is left is to reject the
    // cases it tolerated.
    let host = parsed.host_str().ok_or(CanonError::NoHost)?;
    if host.is_empty() || host == "." || !host.contains(|c: char| c != '.') {
        return Err(CanonError::NoHost);
    }
    if host.starts_with("xn--") && idna::domain_to_unicode(host).1.is_err() {
        return Err(CanonError::BadHost);
    }

    let mut out = String::with_capacity(cleaned.len() + 8);
    out.push_str(scheme);
    out.push_str("://");

    // Step 3: userinfo goes, entirely. A URL carrying credentials is not one
    // we fetch, and keeping the credentials in the key would put them in a
    // published dataset.
    out.push_str(host);

    // Step 5: the port survives only when it is not the scheme default. The
    // `url` crate already elides default ports, so this reads what is left.
    if let Some(port) = parsed.port() {
        out.push(':');
        out.push_str(&port.to_string());
    }

    // Steps 7, 8 and the path parameter half of 10. The `url` crate did 8
    // during parsing. Step 7 is ours, and its subtlety is that `%2F` in a
    // path must survive: decoding it turns one path segment into two and
    // changes which resource is named.
    let path = strip_path_params(parsed.path());
    out.push_str(&normalise_percent_encoding(&path, true));

    // Steps 9, 10 and 11: filter the query without reordering it.
    if let Some(query) = parsed.query() {
        let kept = filter_query(query);
        if !kept.is_empty() {
            out.push('?');
            out.push_str(&kept);
        }
    }

    // Step 6: no fragment, ever. Including the hash bang routing some sites
    // still use, which we accept losing.

    if out.len() > MAX_URL_LEN {
        return Err(CanonError::TooLong);
    }
    Ok(out)
}

/// Step 7. Uppercase hex digits, and decode octets that encode an unreserved
/// character per RFC 3986. Nothing else is decoded.
///
/// `in_path` guards the one case that matters: `%2F` inside a path segment
/// stays encoded, because decoding it invents a segment boundary that the
/// origin does not have.
///
/// Works on bytes and rebuilds a `String` at the end rather than pushing
/// `char`s, so that a non-ASCII byte that somehow reached here is copied
/// through unchanged instead of being reinterpreted as Latin-1. The `url`
/// crate should have percent-encoded those already, but "should have" is not
/// a good enough reason to write a function that corrupts input when it is
/// wrong.
fn normalise_percent_encoding(s: &str, in_path: bool) -> Cow<'_, str> {
    let bytes = s.as_bytes();
    if !bytes.contains(&b'%') {
        return Cow::Borrowed(s);
    }
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            let byte = (hi << 4) | lo;
            if is_unreserved(byte) && !(in_path && byte == b'/') {
                out.push(byte);
            } else {
                out.push(b'%');
                out.push(HEX_UPPER[(byte >> 4) as usize]);
                out.push(HEX_UPPER[(byte & 0xf) as usize]);
            }
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    // Every branch above either copies a byte from a `&str` or writes ASCII,
    // and decoding is limited to unreserved characters, which are all ASCII.
    match String::from_utf8(out) {
        Ok(s) => Cow::Owned(s),
        Err(_) => Cow::Borrowed(s),
    }
}

const HEX_UPPER: &[u8; 16] = b"0123456789ABCDEF";

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Unreserved per RFC 3986 section 2.3.
fn is_unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~')
}

/// Steps 9 through 11. Drop nameless parameters and tracking parameters, keep
/// everything else in the order and multiplicity it arrived in.
fn filter_query(query: &str) -> String {
    let mut out = String::with_capacity(query.len());
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (name, value) = match pair.split_once('=') {
            Some((n, v)) => (n, v),
            None => (pair, ""),
        };
        // Step 9: a parameter with no name carries nothing.
        if name.is_empty() {
            continue;
        }
        if is_tracking_param(name, value) {
            continue;
        }
        if !out.is_empty() {
            out.push('&');
        }
        out.push_str(&normalise_percent_encoding(name, false));
        if pair.contains('=') {
            out.push('=');
            out.push_str(&normalise_percent_encoding(value, false));
        }
    }
    out
}

/// Strip `;jsessionid=` style path parameters, from step 10.
///
/// Java servlet containers still emit these when they cannot set a cookie, and
/// they turn one page into an unbounded number of URLs. Applied to the path
/// before canonicalisation because they are not query parameters and the query
/// filter never sees them.
#[must_use]
pub fn strip_path_params(path: &str) -> Cow<'_, str> {
    if !path.contains(';') {
        return Cow::Borrowed(path);
    }
    let mut out = String::with_capacity(path.len());
    for (i, segment) in path.split('/').enumerate() {
        if i > 0 {
            out.push('/');
        }
        match segment.split_once(';') {
            Some((head, params)) if is_session_path_param(params) => out.push_str(head),
            _ => out.push_str(segment),
        }
    }
    Cow::Owned(out)
}

fn is_session_path_param(params: &str) -> bool {
    let name = params.split('=').next().unwrap_or("");
    matches!(
        name.to_ascii_lowercase().as_str(),
        "jsessionid" | "phpsessid" | "sid" | "sessionid" | "cfid" | "cftoken"
    )
}

/// The pay level domain of a host: the registrable domain, one label below the
/// public suffix.
///
/// This is the unit of partitioning and politeness in doc 03.3, so it decides
/// which coordinator owns a URL and which rate limiter applies to it.
///
/// The implementation here is a placeholder that handles the common multi part
/// suffixes by table. A real public suffix list is milestone 2 work, and until
/// then this over splits on the long tail of country code suffixes, which
/// costs politeness accuracy rather than correctness: two hosts that should
/// share a limiter get separate ones.
#[must_use]
pub fn pay_level_domain(host: &str) -> &str {
    let host = host.strip_suffix('.').unwrap_or(host);
    let labels: Vec<&str> = host.split('.').collect();
    if labels.len() <= 2 {
        return host;
    }
    let last_two = format!("{}.{}", labels[labels.len() - 2], labels[labels.len() - 1]);
    let take = if MULTI_PART_SUFFIXES.contains(&last_two.as_str()) {
        3
    } else {
        2
    };
    if labels.len() <= take {
        return host;
    }
    let skip = labels.len() - take;
    let offset = labels[..skip].iter().map(|l| l.len() + 1).sum::<usize>();
    &host[offset..]
}

/// Two part public suffixes common enough to be worth a table until the real
/// list lands. Ordered by how often they show up in a crawl, not alphabetically.
const MULTI_PART_SUFFIXES: &[&str] = &[
    "co.uk", "org.uk", "ac.uk", "gov.uk", "me.uk", "net.uk", "sch.uk", "com.au", "net.au",
    "org.au", "edu.au", "gov.au", "co.nz", "org.nz", "net.nz", "govt.nz", "co.jp", "or.jp",
    "ne.jp", "ac.jp", "go.jp", "com.br", "net.br", "org.br", "gov.br", "com.cn", "net.cn",
    "org.cn", "gov.cn", "edu.cn", "co.in", "net.in", "org.in", "gov.in", "com.mx", "com.ar",
    "com.tr", "com.tw", "com.hk", "com.sg", "com.my", "co.kr", "or.kr", "co.za", "org.za",
    "com.pl", "com.ua", "co.il", "com.vn", "net.vn", "org.vn", "gov.vn", "edu.vn",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn c(u: &str) -> String {
        canonicalize(u, None).expect("should canonicalise")
    }

    #[test]
    fn step1_rejects_non_http() {
        for u in [
            "mailto:a@b.com",
            "javascript:alert(1)",
            "ftp://example.com/",
            "data:text/html,hi",
            "tel:+15551234",
        ] {
            assert_eq!(canonicalize(u, None), Err(CanonError::NotHttp), "{u}");
        }
    }

    #[test]
    fn step1_rejects_over_2048_bytes() {
        let long = format!("https://example.com/{}", "a".repeat(2100));
        assert_eq!(canonicalize(&long, None), Err(CanonError::TooLong));
    }

    #[test]
    fn ip_address_hosts_normalise_the_way_a_browser_does() {
        // All of these came out of a run over 1.6 million real seed URLs, and
        // every one of them is a case where naive host handling would fetch a
        // different machine than the browser does.
        //
        // A leading zero makes an IPv4 octet octal, so `014` is 12 and not 14.
        assert_eq!(
            c("http://114.207.113.014/bbs/board.php"),
            "http://114.207.113.12/bbs/board.php"
        );
        // A host that is one long number is an IPv4 address, in octal when it
        // starts with a zero and in decimal otherwise.
        assert_eq!(c("http://01754042001/"), "http://15.176.68.1/");
        assert_eq!(c("https://92398226/"), "https://5.129.226.146/");
        assert_eq!(c("http://628379409/"), "http://37.116.79.17/");
        // IPv6 literals get zero compression and lowercase hex.
        assert_eq!(
            c("http://[2001:19f0:8001:0bd7:5400:05ff:fea7:007a]/lg/"),
            "http://[2001:19f0:8001:bd7:5400:5ff:fea7:7a]/lg/"
        );
    }

    #[test]
    fn a_numeric_host_with_no_valid_reading_is_rejected() {
        // `0989992597` starts with a zero so it is read as octal, and 9 and 8
        // are not octal digits, so there is no address here. Chrome refuses
        // these too, and a crawler that invents a reading fetches a machine
        // no user could have reached.
        assert!(canonicalize("http://0989992597/", None).is_err());
        assert!(canonicalize("http://065977632/", None).is_err());
    }

    #[test]
    fn step2_lowercases_scheme_and_host_but_never_path() {
        assert_eq!(
            c("HTTPS://Example.COM/Path/To/Thing"),
            "https://example.com/Path/To/Thing"
        );
    }

    #[test]
    fn step3_strips_userinfo() {
        let out = c("https://user:secret@example.com/x");
        assert_eq!(out, "https://example.com/x");
        // Belt and braces: a credential leaking into a published dataset via
        // the url key would be very hard to walk back.
        assert!(!out.contains("secret"));
        assert!(!out.contains("user"));
    }

    #[test]
    fn step4_converts_idn_to_a_label() {
        assert_eq!(
            c("https://bücher.example/"),
            "https://xn--bcher-kva.example/"
        );
        assert_eq!(c("https://日本.example/"), "https://xn--wgv71a.example/");
    }

    #[test]
    fn step5_strips_default_ports_and_keeps_others() {
        assert_eq!(c("http://example.com:80/"), "http://example.com/");
        assert_eq!(c("https://example.com:443/"), "https://example.com/");
        assert_eq!(c("https://example.com:8443/"), "https://example.com:8443/");
    }

    #[test]
    fn step6_removes_the_fragment() {
        assert_eq!(c("https://example.com/a#section"), "https://example.com/a");
        assert_eq!(c("https://example.com/a#!/route"), "https://example.com/a");
    }

    #[test]
    fn step7_uppercases_hex_and_decodes_only_unreserved() {
        // %7e is a tilde, unreserved, so it decodes.
        assert_eq!(
            c("https://example.com/%7euser"),
            "https://example.com/~user"
        );
        // %20 is a space, reserved, so it stays encoded but gets uppercased.
        assert_eq!(c("https://example.com/a%20b"), "https://example.com/a%20b");
        assert_eq!(c("https://example.com/a%2fb"), "https://example.com/a%2Fb");
    }

    #[test]
    fn step7_never_decodes_an_encoded_slash_in_a_path() {
        // Decoding this would turn one path segment into two and name a
        // different resource than the origin has.
        let out = c("https://example.com/a%2Fb/c");
        assert!(out.contains("%2F"), "{out}");
        assert_ne!(out, "https://example.com/a/b/c");
    }

    #[test]
    fn step8_resolves_dot_segments_and_gives_an_empty_path_a_slash() {
        assert_eq!(c("https://example.com/a/b/../c"), "https://example.com/a/c");
        assert_eq!(c("https://example.com/a/./b"), "https://example.com/a/b");
        assert_eq!(c("https://example.com"), "https://example.com/");
    }

    #[test]
    fn step9_drops_an_empty_query_and_nameless_parameters() {
        assert_eq!(c("https://example.com/a?"), "https://example.com/a");
        assert_eq!(c("https://example.com/a?&&"), "https://example.com/a");
        assert_eq!(
            c("https://example.com/a?=v&b=2"),
            "https://example.com/a?b=2"
        );
    }

    #[test]
    fn step10_removes_tracking_parameters() {
        assert_eq!(
            c("https://example.com/a?utm_source=x&id=7&fbclid=abc"),
            "https://example.com/a?id=7"
        );
        assert_eq!(
            c("https://example.com/a?gclid=x&UTM_Medium=y"),
            "https://example.com/a"
        );
    }

    #[test]
    fn step10_removes_sid_only_when_it_looks_like_a_session() {
        // A short numeric sid is far more likely to be content than a session.
        assert_eq!(
            c("https://example.com/a?sid=42"),
            "https://example.com/a?sid=42"
        );
        assert_eq!(
            c("https://example.com/a?sid=a1b2c3d4e5f6a7b8c9d0"),
            "https://example.com/a"
        );
    }

    #[test]
    fn step11_does_not_sort_or_deduplicate_query_parameters() {
        // Both are standard canonicalisation moves and doc 11.2 rejects both,
        // because order and repetition are semantically live on real sites.
        assert_eq!(
            c("https://example.com/?b=2&a=1"),
            "https://example.com/?b=2&a=1"
        );
        assert_eq!(
            c("https://example.com/?tag=x&tag=y"),
            "https://example.com/?tag=x&tag=y"
        );
    }

    #[test]
    fn step12_leaves_trailing_slashes_exactly_as_found() {
        assert_eq!(c("https://example.com/a"), "https://example.com/a");
        assert_eq!(c("https://example.com/a/"), "https://example.com/a/");
        assert_ne!(c("https://example.com/a"), c("https://example.com/a/"));
    }

    #[test]
    fn canonicalisation_is_idempotent() {
        // If this ever fails, keys are not stable under recanonicalisation and
        // a URL can be admitted twice under two different keys.
        for u in [
            "https://Example.com:443/a/../b?utm_source=x&c=1#frag",
            "http://example.com",
            "https://bücher.example/%7Ea%20b",
            "https://example.com/a%2Fb?tag=x&tag=y",
        ] {
            let once = c(u);
            let twice = c(&once);
            assert_eq!(once, twice, "not idempotent: {u}");
        }
    }

    #[test]
    fn relative_urls_resolve_against_the_base() {
        let base = "https://example.com/dir/page.html";
        assert_eq!(
            canonicalize("../other.html", Some(base)).unwrap(),
            "https://example.com/other.html"
        );
        assert_eq!(
            canonicalize("/root", Some(base)).unwrap(),
            "https://example.com/root"
        );
        assert_eq!(
            canonicalize("sibling", Some(base)).unwrap(),
            "https://example.com/dir/sibling"
        );
    }

    #[test]
    fn control_characters_are_stripped_before_parsing() {
        // Browsers strip these and some servers do not. Letting an attacker
        // choose which of the two we resemble is not a position to be in.
        assert_eq!(
            c("https://example.com/a\u{0009}b"),
            "https://example.com/ab"
        );
        assert_eq!(c("  https://example.com/a\n  "), "https://example.com/a");
    }

    #[test]
    fn path_parameters_are_stripped() {
        assert_eq!(
            strip_path_params("/shop;jsessionid=A1B2C3/item"),
            "/shop/item"
        );
        // A semicolon that is not a session parameter is part of the path.
        assert_eq!(strip_path_params("/a;b=c/d"), "/a;b=c/d");
    }

    #[test]
    fn path_parameters_are_stripped_by_canonicalize_too() {
        // Regression: the stripper existed and canonicalize did not call it,
        // so the servlet half of step 10 was dead code and every session id
        // in a path became a distinct url key.
        assert_eq!(
            c("https://example.com/shop;jsessionid=0A1B2C3D/item"),
            "https://example.com/shop/item"
        );
    }

    #[test]
    fn the_tracking_list_parses_and_covers_the_three_rule_kinds() {
        let rules = parse_tracking_params(TRACKING_PARAMS_FILE);
        assert!(rules.len() > 30, "list looks truncated: {}", rules.len());
        assert!(
            rules
                .iter()
                .any(|r| r.kind == Match::Prefix && r.name == "utm_")
        );
        assert!(
            rules
                .iter()
                .any(|r| r.kind == Match::SessionValued && r.name == "sid")
        );
        assert!(
            rules
                .iter()
                .any(|r| r.kind == Match::Exact && r.name == "gclid")
        );
        // Comments and blank lines must not become rules, or a stray `#` line
        // would start eating parameters whose names begin with a hash.
        assert!(
            rules
                .iter()
                .all(|r| !r.name.is_empty() && !r.name.starts_with('#'))
        );
    }

    #[test]
    fn tracking_rules_apply_from_the_file() {
        assert!(is_tracking_param("utm_campaign", "spring"));
        assert!(is_tracking_param("UTM_Campaign", "spring"));
        assert!(is_tracking_param("aspsessionidabcdefgh", "x"));
        assert!(is_tracking_param("mkt_tok", "x"));
        assert!(!is_tracking_param("q", "rust"));
        assert!(!is_tracking_param("page", "2"));
        // `id` must never be caught by a prefix rule. It is the single most
        // common content parameter on the web and removing it would merge
        // every product page on a site into one key.
        assert!(!is_tracking_param("id", "1234567890abcdef"));
    }

    #[test]
    fn pay_level_domain_finds_the_registrable_domain() {
        assert_eq!(pay_level_domain("www.example.com"), "example.com");
        assert_eq!(pay_level_domain("example.com"), "example.com");
        assert_eq!(pay_level_domain("a.b.c.example.com"), "example.com");
        assert_eq!(pay_level_domain("www.example.co.uk"), "example.co.uk");
        assert_eq!(pay_level_domain("example.co.uk"), "example.co.uk");
        assert_eq!(pay_level_domain("shop.example.com.au"), "example.com.au");
        assert_eq!(pay_level_domain("localhost"), "localhost");
    }

    #[test]
    fn rejects_hosts_that_are_not_hosts() {
        assert!(canonicalize("https://", None).is_err());
        assert!(canonicalize("http://.", None).is_err());
        assert!(canonicalize("not a url", None).is_err());
        assert!(canonicalize("", None).is_err());
    }
}
