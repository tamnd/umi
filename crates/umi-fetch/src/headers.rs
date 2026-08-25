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

use crate::date;
use crate::outcome::RetryAfter;

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

/// `Retry-After`, in whichever of the two forms the origin used.
///
/// RFC 9110 section 10.2.3 allows delta-seconds or an HTTP-date and origins
/// send both, so both are read. The digits are tried first because that is the
/// overwhelmingly common form and because an HTTP-date never starts with one.
///
/// Anything that is neither is `None`. A `Retry-After: soon` is not a number we
/// can honour and guessing at one would be worse than falling back to the
/// adaptive delay, which is already a considered answer.
#[must_use]
pub fn retry_after(headers: &HeaderMap) -> Option<RetryAfter> {
    let value = headers.get("retry-after")?.to_str().ok()?.trim();
    if let Ok(seconds) = value.parse::<u32>() {
        return Some(RetryAfter::After(seconds));
    }
    date::parse(value).map(RetryAfter::At)
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

    use super::{KEPT, NEVER_STORED, RetryAfter, digest, kept, retry_after};

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
    fn retry_after_reads_both_of_the_forms_rfc_9110_allows() {
        // Delta seconds is what almost everybody sends and the date form is
        // what the rest send, so a limiter that reads only the first honours
        // nothing on the sites most likely to be asking.
        let seconds = headers(&[("retry-after", "120")]);
        assert_eq!(retry_after(&seconds), Some(RetryAfter::After(120)));

        let date = headers(&[("retry-after", "Wed, 21 Oct 2015 07:28:00 GMT")]);
        assert_eq!(retry_after(&date), Some(RetryAfter::At(1_445_412_480_000)));

        // Whitespace around the value is the origin's business and not ours.
        let padded = headers(&[("retry-after", "  30  ")]);
        assert_eq!(retry_after(&padded), Some(RetryAfter::After(30)));
    }

    #[test]
    fn a_retry_after_we_cannot_read_is_nothing_rather_than_a_guess() {
        // Falling back to the adaptive delay is already a considered answer.
        // Inventing a number from a header we did not understand is not.
        for value in ["soon", "", "-5", "1.5", "tomorrow"] {
            let map = headers(&[("retry-after", value)]);
            assert_eq!(retry_after(&map), None, "{value:?}");
        }
        assert_eq!(retry_after(&HeaderMap::new()), None);
    }

    #[test]
    fn a_retry_after_never_parks_a_host_for_longer_than_a_day() {
        // A week is not a lie, it is a site telling us to come back next week,
        // and the frontier already says that by scheduling the url later.
        // Parking a whole host on the strength of one header is a much bigger
        // decision than the header is making.
        let now = 1_700_000_000_000;
        assert_eq!(RetryAfter::After(60).ms_from(now), 60_000);
        assert_eq!(RetryAfter::After(u32::MAX).ms_from(now), RetryAfter::MAX_MS);
        assert_eq!(RetryAfter::At(now + 5000).ms_from(now), 5000);
        assert_eq!(
            RetryAfter::At(now + 30 * u64::from(RetryAfter::MAX_MS)).ms_from(now),
            RetryAfter::MAX_MS
        );
    }

    #[test]
    fn a_retry_after_date_in_the_past_asks_for_nothing() {
        // Clocks disagree by a few seconds all day long. An origin that says
        // "retry at 12:00:03" when we think it is 12:00:05 is asking for
        // nothing, not for a negative wait.
        let now = 1_700_000_000_000;
        assert_eq!(RetryAfter::At(now - 2000).ms_from(now), 0);
        assert_eq!(RetryAfter::At(0).ms_from(now), 0);
    }

    #[test]
    fn an_empty_response_has_a_digest_and_no_kept_headers() {
        let map = HeaderMap::new();
        assert!(kept(&map).is_empty());
        assert_eq!(digest(&map), *blake3::hash(b"").as_bytes());
    }
}
