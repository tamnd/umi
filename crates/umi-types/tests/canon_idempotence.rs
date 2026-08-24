//! Property test: canonicalisation is idempotent for everything that survives
//! step 1.
//!
//! This is the invariant issue #2 asks a fuzzer to confirm. There is a
//! libFuzzer target in `fuzz/` for long soaks, but it needs nightly and the
//! toolchain is pinned at 1.98 stable, so the version that actually runs on
//! every push is this one: a deterministic generator that builds URL shaped
//! strings out of the pieces canonicalisation cares about and checks that
//! canonicalising twice gives the same answer as canonicalising once.
//!
//! Idempotence is the property that matters because a URL is canonicalised at
//! several points on its way through the system: once when a link is
//! extracted, again when a seed file is read, again when an operator types one
//! into `umi fetch`. If those disagree the same page enters the frontier under
//! two keys, gets fetched twice, and is published twice. Nothing would raise
//! an error. It would just cost money.

use umi_types::canon::canonicalize;

/// xorshift64*, so the corpus is the same on every machine and every run. A
/// property test that fails one time in fifty on CI and never locally is not
/// worth having.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn pick<'a, T>(&mut self, options: &'a [T]) -> &'a T {
        &options[(self.next() % options.len() as u64) as usize]
    }

    fn chance(&mut self, one_in: u64) -> bool {
        self.next().is_multiple_of(one_in)
    }
}

const SCHEMES: &[&str] = &["http", "https", "HTTP", "HTTPS", "ftp", "mailto", ""];
const HOSTS: &[&str] = &[
    "example.com",
    "Example.COM",
    "www.example.co.uk",
    "bücher.example",
    "xn--bcher-kva.example",
    "日本.example",
    "a.b.c.d.example.com",
    "127.0.0.1",
    "[::1]",
    "localhost",
    "",
    ".",
    "..",
    "example.com.",
];
const PORTS: &[&str] = &["", ":80", ":443", ":8080", ":0", ":65535"];
const PATHS: &[&str] = &[
    "",
    "/",
    "/a",
    "/a/",
    "/a/b/../c",
    "/./a",
    "/../../..",
    "/a%2Fb",
    "/a%2fb",
    "/%7Euser",
    "/%7euser",
    "/a%20b",
    "/a b",
    "/shop;jsessionid=0A1B2C3D/item",
    "/a;b=c/d",
    "/%",
    "/%zz",
    "/%e4%b8%ad",
    "/中文",
    "//double//slash//",
];
const QUERIES: &[&str] = &[
    "",
    "?",
    "?a=1",
    "?b=2&a=1",
    "?tag=x&tag=y",
    "?utm_source=x&id=7",
    "?=novalue&a=1",
    "?&&&",
    "?sid=42",
    "?sid=a1b2c3d4e5f6a7b8",
    "?q=a%20b&r=c%2Fd",
    "?flag",
    "?a=1&",
    "?%7e=%7e",
];
const FRAGMENTS: &[&str] = &["", "#", "#top", "#!/route", "#a#b"];

fn build(rng: &mut Rng) -> String {
    let mut s = String::new();
    let scheme = rng.pick(SCHEMES);
    if !scheme.is_empty() {
        s.push_str(scheme);
        s.push_str("://");
    }
    if rng.chance(20) {
        s.push_str("user:pass@");
    }
    s.push_str(rng.pick(HOSTS));
    s.push_str(rng.pick(PORTS));
    s.push_str(rng.pick(PATHS));
    s.push_str(rng.pick(QUERIES));
    s.push_str(rng.pick(FRAGMENTS));
    if rng.chance(30) {
        s.push('\t');
    }
    if rng.chance(40) {
        s.push_str(&"a".repeat(2100));
    }
    s
}

#[test]
fn canonicalisation_is_idempotent_over_a_generated_corpus() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut canonicalised = 0usize;

    for i in 0..200_000 {
        let input = build(&mut rng);
        let Ok(once) = canonicalize(&input, None) else {
            continue;
        };
        canonicalised += 1;

        let twice = canonicalize(&once, None).unwrap_or_else(|e| {
            panic!("iteration {i}: {input:?} canonicalised to {once:?}, which then failed: {e}")
        });
        assert_eq!(twice, once, "iteration {i}: not idempotent, from {input:?}");

        // A canonical URL is also expected to be well formed enough to hand
        // to a fetcher without further work, so assert the shape rather than
        // only the fixed point. A stable but malformed output would satisfy
        // idempotence and still be useless.
        assert!(
            once.starts_with("http://") || once.starts_with("https://"),
            "{once:?}"
        );
        assert!(!once.contains('#'), "fragment survived: {once:?}");
        assert!(!once.contains('@'), "userinfo survived: {once:?}");
        assert!(!once.ends_with('?'), "empty query survived: {once:?}");
        assert!(once.len() <= 2048, "over length: {}", once.len());
    }

    // Guard against the test passing because everything was rejected. If a
    // change to step 1 started throwing out the whole corpus, the loop above
    // would sail through having checked nothing.
    //
    // The corpus is deliberately about half garbage: three of the seven
    // schemes are not http and three of the fourteen hosts are not hosts, so
    // roughly 45 percent surviving is the expected number and anything much
    // under 40 percent means step 1 started rejecting something it should
    // accept.
    assert!(
        canonicalised > 80_000,
        "only {canonicalised} of 200000 inputs canonicalised, corpus or step 1 is wrong"
    );
}

#[test]
fn resolving_against_a_base_is_idempotent_too() {
    // Link extraction resolves against the page URL, and the page URL is
    // itself canonical by then. Resolving an already absolute canonical URL
    // against any base must be a no-op, or a link found on two pages gets two
    // keys depending on which page found it.
    let mut rng = Rng(0xdead_beef_cafe_f00d);
    let bases = [
        "https://example.com/dir/page.html",
        "http://other.example/",
        "https://example.co.uk/a/b/c?q=1",
    ];

    for _ in 0..50_000 {
        let input = build(&mut rng);
        let base = rng.pick(&bases);
        let Ok(once) = canonicalize(&input, Some(base)) else {
            continue;
        };
        assert_eq!(
            canonicalize(&once, Some(base)).as_deref(),
            Ok(once.as_str())
        );
        assert_eq!(canonicalize(&once, None).as_deref(), Ok(once.as_str()));
    }
}

#[test]
fn every_rejection_is_stable() {
    // A URL that is rejected must be rejected every time and for the same
    // reason. Admission treats rejection as "not a candidate" and never
    // revisits it, so a flapping rejection is a URL that enters the frontier
    // on retry and not on first sight.
    let mut rng = Rng(0x0123_4567_89ab_cdef);
    for _ in 0..50_000 {
        let input = build(&mut rng);
        let first = canonicalize(&input, None);
        for _ in 0..3 {
            assert_eq!(canonicalize(&input, None), first, "unstable: {input:?}");
        }
    }
}

#[test]
fn nothing_in_the_corpus_panics_or_hangs() {
    // The real fuzz target's job. Every byte string that reaches
    // canonicalisation comes off the open internet, so it has to return an
    // error rather than unwind, and a nested `%` or a long run of dot
    // segments must not turn into quadratic work.
    let hostile = [
        "%".repeat(5000),
        "https://example.com/".to_owned() + &"%2F".repeat(600),
        "https://example.com/".to_owned() + &"../".repeat(1000),
        "https://example.com/?".to_owned() + &"a=1&".repeat(1000),
        "https://".to_owned() + &"a.".repeat(1000) + "com/",
        "https://example.com/%".to_owned(),
        "https://example.com/%f".to_owned(),
        "\u{0}\u{1}\u{2}https://example.com/".to_owned(),
        "https://exa\u{200b}mple.com/".to_owned(),
        "http://[".to_owned(),
        "http://]".to_owned(),
        ":".repeat(100),
    ];
    for input in &hostile {
        let _ = canonicalize(input, None);
        let _ = canonicalize(input, Some("https://example.com/"));
    }
}
