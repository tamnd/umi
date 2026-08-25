//! The sixteen headers that leave the fetcher, and the digest over the ones
//! that do not.
//!
//! Doc 11.5 fixes the list and the reasoning behind it. Size is half of it: a
//! full header set on a modern site runs about 1.5 KB, most of it CDN debug
//! fields, and that would be a quarter of doc 10's byte budget per page.
//! Obligation is the other half: `Set-Cookie` routinely carries session
//! identifiers and sometimes carries personal data, and the only defensible
//! way to handle that in a corpus published under an open licence is to never
//! store it. There is no flag that changes the list.
//!
//! Nothing is lost for verification, because [`digest`] covers every header
//! including the ones that are dropped. A digest over a session cookie is one
//! way and the input is unguessable, so it carries none of the value the
//! cookie has, which is what lets doc 04's `headers_digest` be honest about
//! covering the full set.

use http::HeaderMap;

/// The published subset, from `docs/spec/11-extraction-and-dedup.md` section
/// 11.5. Sixteen entries, fixed, no wildcards and no configuration.
pub const KEPT: [&str; 16] = [
    "content-type",
    "content-language",
    "last-modified",
    "etag",
    "cache-control",
    "expires",
    "age",
    "vary",
    "content-encoding",
    "link",
    "x-robots-tag",
    "location",
    "retry-after",
    "content-usage",
    "alt-svc",
    "server",
];

/// Headers that are never stored, under any list, flag or override.
///
/// They are not in [`KEPT`], so this is a second lock on the same door rather
/// than a mechanism. It exists so that the test which asserts it is checking a
/// stated rule rather than an accident of ordering, and so that anyone adding
/// to `KEPT` trips over the reason first.
pub const NEVER_STORED: [&str; 4] = [
    "set-cookie",
    "authorization",
    "proxy-authorization",
    "www-authenticate",
];

/// The published subset of one response's headers, in the order of [`KEPT`].
///
/// A header sent more than once is joined with `, `, which is the RFC 9110
/// rule for every header on the list. `Set-Cookie` is the one header where
/// that rule does not hold, and it is not on the list.
#[must_use]
pub fn kept(headers: &HeaderMap) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for name in KEPT {
        let mut values = headers
            .get_all(name)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .peekable();
        if values.peek().is_some() {
            out.push((name.to_owned(), values.collect::<Vec<_>>().join(", ")));
        }
    }
    out
}

/// Blake3 over a canonical serialisation of every header, kept or not.
///
/// Canonical means: lowercase the name, keep the value bytes exactly as they
/// arrived, write `name\0value\0` for each, and sort those records bytewise
/// before hashing. Sorting is what makes the digest independent of the order
/// the origin or an intermediary chose, which two fetchers of the same page
/// will not agree on. Repeated headers stay as separate records, so a response
/// with two `Set-Cookie` lines does not hash the same as one with the pair
/// joined.
#[must_use]
pub fn digest(headers: &HeaderMap) -> [u8; 32] {
    let mut records: Vec<Vec<u8>> = headers
        .iter()
        .map(|(name, value)| {
            let mut record = Vec::with_capacity(name.as_str().len() + value.len() + 2);
            record.extend_from_slice(name.as_str().to_ascii_lowercase().as_bytes());
            record.push(0);
            record.extend_from_slice(value.as_bytes());
            record.push(0);
            record
        })
        .collect();
    records.sort_unstable();

    let mut hasher = blake3::Hasher::new();
    for record in records {
        hasher.update(&record);
    }
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue};

    use super::{KEPT, NEVER_STORED, digest, kept};

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(
                http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    #[test]
    fn the_list_is_the_sixteen_doc_eleven_names() {
        assert_eq!(KEPT.len(), 16);
        let mut sorted = KEPT.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), 16, "the list has a duplicate in it");
        assert!(
            KEPT.iter().all(|name| *name == name.to_ascii_lowercase()),
            "every name has to be lowercase or the lookup misses"
        );
    }

    #[test]
    fn the_headers_that_are_never_stored_are_not_stored() {
        for name in NEVER_STORED {
            assert!(
                !KEPT.contains(&name),
                "{name} is on the published list, which doc 11.5 forbids"
            );
        }
        // Every spelling an origin might use, including the ones no origin
        // should: mixed case, shouted, and the same forbidden header sent twice.
        // `http` folds a header name to lowercase on the way in, so this cannot
        // regress here, but the rule is a promise about what reaches storage
        // rather than about one library's behaviour, and the day the fetch path
        // grows a second way to build a `HeaderMap` this is the test that
        // notices.
        let map = headers(&[
            ("Set-Cookie", "session=abc; HttpOnly"),
            ("SET-COOKIE", "other=def"),
            ("AUTHORIZATION", "Bearer nope"),
            ("Proxy-Authorization", "Basic nope"),
            ("WWW-Authenticate", "Basic realm=\"x\""),
            ("Content-Type", "text/html"),
        ]);
        assert!(
            map.keys()
                .all(|name| name.as_str() == name.as_str().to_ascii_lowercase()),
            "the map is supposed to arrive folded to lowercase"
        );
        let out = kept(&map);
        assert_eq!(
            out,
            vec![("content-type".to_owned(), "text/html".to_owned())]
        );
        // Not in the names, not in the values, not anywhere.
        let dump = format!("{out:?}");
        for secret in ["session=abc", "other=def", "Bearer nope", "cookie", "auth"] {
            assert!(!dump.contains(secret), "{secret} survived into {dump}");
        }
    }

    #[test]
    fn a_dropped_header_still_moves_the_digest() {
        // The whole point of hashing everything: a response that differs only
        // in a header we do not publish is still a different response, and
        // doc 04's verification depends on that being visible.
        let with = headers(&[("content-type", "text/html"), ("x-cache", "HIT")]);
        let without = headers(&[("content-type", "text/html")]);
        assert_ne!(digest(&with), digest(&without));
        assert_eq!(kept(&with), kept(&without));
    }

    #[test]
    fn the_digest_does_not_depend_on_the_order_they_arrived_in() {
        let one = headers(&[("content-type", "text/html"), ("server", "nginx")]);
        let other = headers(&[("server", "nginx"), ("content-type", "text/html")]);
        assert_eq!(digest(&one), digest(&other));
    }

    #[test]
    fn case_in_the_name_does_not_move_the_digest_but_case_in_the_value_does() {
        let lower = headers(&[("content-type", "text/html")]);
        let upper = headers(&[("Content-Type", "text/html")]);
        assert_eq!(digest(&lower), digest(&upper));

        let shouted = headers(&[("content-type", "TEXT/HTML")]);
        assert_ne!(digest(&lower), digest(&shouted));
    }

    #[test]
    fn a_repeated_header_is_joined_rather_than_dropped() {
        let map = headers(&[("link", "<a>; rel=next"), ("link", "<b>; rel=prev")]);
        assert_eq!(
            kept(&map),
            vec![("link".to_owned(), "<a>; rel=next, <b>; rel=prev".to_owned())]
        );
    }

    #[test]
    fn joining_a_repeat_is_not_the_same_as_having_sent_it_joined() {
        // Two `Link` lines and one `Link` line with a comma in it are the same
        // thing to a parser and different things on the wire. The digest has
        // to see the difference or an intermediary that rewrites one into the
        // other looks like a fabricating fetcher.
        let split = headers(&[("link", "<a>; rel=next"), ("link", "<b>; rel=prev")]);
        let joined = headers(&[("link", "<a>; rel=next, <b>; rel=prev")]);
        assert_eq!(kept(&split), kept(&joined));
        assert_ne!(digest(&split), digest(&joined));
    }

    #[test]
    fn the_kept_pairs_come_back_in_the_order_the_list_names() {
        let map = headers(&[
            ("server", "nginx"),
            ("etag", "\"v1\""),
            ("content-type", "text/html"),
        ]);
        let names: Vec<_> = kept(&map).into_iter().map(|(name, _)| name).collect();
        assert_eq!(names, vec!["content-type", "etag", "server"]);
    }

    #[test]
    fn an_empty_response_has_a_digest_and_no_kept_headers() {
        let map = HeaderMap::new();
        assert!(kept(&map).is_empty());
        assert_eq!(digest(&map), *blake3::hash(b"").as_bytes());
    }
}
