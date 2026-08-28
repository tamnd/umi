//! The body half of doc 05.8's block signal.
//!
//! The header half lives next to the status classifier and is one lookup: a
//! vendor that sends `cf-mitigated` has told us what it is. The body half is
//! here because it is the harder case and the more common one. A bot manager
//! that is not sure about us serves an interstitial with a 200 on it, and to a
//! crawler that only reads status codes that is a page. Counting it as one is
//! how a corpus fills up with a million copies of "Checking your browser".
//!
//! # What this is not
//!
//! It is not a challenge solver and there is nothing here that tries to pass
//! one. Doc 07.8 is explicit: no CAPTCHA solving, no challenge bypass. This
//! recognises a wall so the crawler can record it, back off and stop spending
//! requests on it, which is the opposite of getting past it.
//!
//! # Cost
//!
//! Every check is bounded to [`SCAN_BYTES`] of the body and runs only on
//! responses that are already suspicious: a 403, 429 or 503, or a 200 that
//! extracted to almost no text. A page that arrived normally and has text on
//! it never reaches this module, so the cost of it at 250 pages a second is
//! nothing.

use umi_types::TierSignal;

/// How much of a body is worth looking at.
///
/// An interstitial is a small page, and the markers below are all in the head
/// or in the first script tag. 16 KiB is well past the whole of every
/// challenge page the vendors serve, and capping it means a 10 MB body cannot
/// turn a cheap check into a scan.
pub const SCAN_BYTES: usize = 16 * 1024;

/// Under this many characters of extracted text, a 200 with a marker in it is
/// a challenge rather than a page that happens to mention one. Doc 05.8.
pub const CHALLENGE_TEXT: usize = 200;

/// Under this many characters of extracted text, with scripts and an app root,
/// a 200 is a client rendered shell. Doc 05.8.
pub const SHELL_TEXT: usize = 500;

/// How many script tags a shell has at least. Doc 05.8.
pub const SHELL_SCRIPTS: usize = 5;

/// The interstitials doc 05.8 calls known, by vendor.
///
/// Written in lowercase because the match is case insensitive, and chosen to
/// be strings that appear in the challenge and not in an ordinary page about
/// the vendor. "cloudflare" on its own would match every status page on the
/// web that says who hosts it.
const INTERSTITIALS: [(&str, &str); 18] = [
    ("cloudflare", "cf-browser-verification"),
    ("cloudflare", "cf_chl_opt"),
    ("cloudflare", "just a moment..."),
    ("cloudflare", "checking your browser before accessing"),
    ("cloudflare", "attention required! | cloudflare"),
    ("cloudflare", "enable javascript and cookies to continue"),
    ("datadome", "captcha-delivery.com"),
    ("datadome", "datadome.co/customers-protection"),
    ("imperva", "_incapsula_resource"),
    ("imperva", "incapsula incident id"),
    ("perimeterx", "px-captcha"),
    ("perimeterx", "/_px/"),
    ("akamai", "akamaighost"),
    ("akamai", "errors.edgesuite.net"),
    ("sucuri", "sucuri website firewall"),
    ("aws waf", "captcha.awswaf.com"),
    ("aws waf", "awswaf-captcha"),
    ("generic", "ddos protection by"),
];

/// The elements that say a body is an application shell rather than a page.
const APP_ROOTS: [&str; 7] = [
    "<noscript",
    "data-reactroot",
    "__next_data__",
    "ng-app",
    "id=\"root\"",
    "id=\"app\"",
    "id='root'",
];

/// Which vendor's wall this body is, if it is one.
///
/// The name is returned rather than a bool because it goes in the log line an
/// operator reads when a domain stops answering, and "blocked" on its own does
/// not tell them whether to look at their own address or at the site.
#[must_use]
pub fn interstitial(body: &[u8]) -> Option<&'static str> {
    let head = &body[..body.len().min(SCAN_BYTES)];
    INTERSTITIALS
        .into_iter()
        .find(|(_, marker)| contains(head, marker.as_bytes()))
        .map(|(vendor, _)| vendor)
}

/// Whether a body is a client rendered shell, given how much text came out of
/// it.
///
/// Three conditions and not one, because each on its own is a normal page. A
/// short page is a redirect notice, a page with six scripts is every news
/// site, and a `<noscript>` is good manners. Together they are a body that
/// contains an application and no content, and doc 05.8 says the answer to
/// that is T3 rather than a better fingerprint.
#[must_use]
pub fn shell(body: &[u8], text_chars: usize) -> bool {
    if text_chars >= SHELL_TEXT {
        return false;
    }
    let head = &body[..body.len().min(SCAN_BYTES)];
    if count(head, b"<script") <= SHELL_SCRIPTS {
        return false;
    }
    APP_ROOTS.iter().any(|root| contains(head, root.as_bytes()))
}

/// What a 200 response says about the tier it came back on.
///
/// `None` is the ordinary case: a page arrived, and it says nothing about the
/// ladder that we did not already know. The two `Some` answers are the two
/// ways a 200 can be a lie.
#[must_use]
pub fn read_ok(body: &[u8], text_chars: usize) -> Option<TierSignal> {
    if text_chars < CHALLENGE_TEXT && interstitial(body).is_some() {
        return Some(TierSignal::Blocked);
    }
    shell(body, text_chars).then_some(TierSignal::Shell)
}

/// ASCII case insensitive substring search, with `needle` already lowercase.
///
/// Naive on purpose. The haystack is capped at [`SCAN_BYTES`], the needles are
/// short and distinctive, and the inner loop stops at the first byte that does
/// not match, so the cost in practice is one comparison per position. A real
/// searcher here would be a dependency and a build minute bought to speed up a
/// path that only runs on responses that already went wrong.
fn contains(hay: &[u8], needle: &[u8]) -> bool {
    find(hay, needle).is_some()
}

/// How many times `needle` appears, counting non overlapping matches.
fn count(hay: &[u8], needle: &[u8]) -> usize {
    let mut at = 0;
    let mut seen = 0;
    while let Some(hit) = find(&hay[at..], needle) {
        seen += 1;
        at += hit + needle.len();
    }
    seen
}

fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    let first = needle[0];
    let last = hay.len() - needle.len();
    (0..=last).find(|&at| {
        hay[at].eq_ignore_ascii_case(&first)
            && hay[at..at + needle.len()]
                .iter()
                .zip(needle)
                .all(|(a, b)| a.eq_ignore_ascii_case(b))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const JUST_A_MOMENT: &str = "<!DOCTYPE html><html><head><title>Just a moment...\
</title></head><body><div id=\"cf-browser-verification\"></div></body></html>";

    #[test]
    fn a_cloudflare_interstitial_is_recognised_whatever_its_case() {
        assert_eq!(interstitial(JUST_A_MOMENT.as_bytes()), Some("cloudflare"));
        let shouted = JUST_A_MOMENT.to_uppercase();
        assert_eq!(interstitial(shouted.as_bytes()), Some("cloudflare"));
    }

    #[test]
    fn an_ordinary_page_is_not_an_interstitial() {
        let page = "<html><body><h1>We moved to Cloudflare last year</h1>\
                    <p>The migration took a weekend and here is what broke.</p>\
                    </body></html>";
        assert_eq!(interstitial(page.as_bytes()), None);
    }

    #[test]
    fn a_marker_past_the_scan_window_is_not_looked_for() {
        let mut body = vec![b' '; SCAN_BYTES];
        body.extend_from_slice(b"cf_chl_opt");
        assert_eq!(interstitial(&body), None);
    }

    #[test]
    fn each_vendor_marker_matches_itself() {
        for (vendor, marker) in INTERSTITIALS {
            assert_eq!(interstitial(marker.as_bytes()), Some(vendor), "{marker}");
        }
    }

    #[test]
    fn a_shell_needs_short_text_and_scripts_and_a_root() {
        let body = "<html><body><div id=\"root\"></div>\
                    <script></script><script></script><script></script>\
                    <script></script><script></script><script></script>\
                    </body></html>";
        assert!(shell(body.as_bytes(), 40));
        // Same body, but the page has content in it.
        assert!(!shell(body.as_bytes(), SHELL_TEXT));
        // Same body, one script short.
        let thin = body.replacen("<script></script>", "", 1);
        assert!(!shell(thin.as_bytes(), 40));
        // Same scripts, no application root.
        let rootless = body.replace("id=\"root\"", "class=\"page\"");
        assert!(!shell(rootless.as_bytes(), 40));
    }

    #[test]
    fn a_two_hundred_carrying_a_wall_is_a_block_and_not_a_page() {
        let body = JUST_A_MOMENT.as_bytes();
        assert_eq!(read_ok(body, 30), Some(TierSignal::Blocked));
        // The same body with real text extracted from it is a page about
        // Cloudflare, which is a thing people write.
        assert_eq!(read_ok(body, 4000), None);
    }

    #[test]
    fn counting_stops_at_the_end_and_does_not_overlap() {
        assert_eq!(count(b"aaaa", b"aa"), 2);
        assert_eq!(count(b"", b"aa"), 0);
        assert_eq!(count(b"a", b"aa"), 0);
    }
}
