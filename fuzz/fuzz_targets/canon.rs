//! Fuzz URL canonicalisation for panics and for idempotence.
//!
//! Two properties, and the second is the one worth the CPU time. Panicking on
//! a hostile URL takes a fetcher down, which is loud and gets fixed. Being
//! non idempotent is silent: the page enters the frontier twice under two
//! keys, gets fetched twice, gets published twice, and nothing anywhere
//! reports an error.

#![no_main]

use libfuzzer_sys::fuzz_target;
use umi_types::canon::canonicalize;

fuzz_target!(|data: &[u8]| {
    let Ok(input) = core::str::from_utf8(data) else {
        return;
    };

    // Split the input so one corpus entry exercises both the absolute and the
    // resolve against a base paths, which take different routes through the
    // url crate and have historically disagreed about trailing slashes.
    let (base, target) = match input.split_once('\n') {
        Some((b, t)) => (Some(b), t),
        None => (None, input),
    };

    let Ok(once) = canonicalize(target, base) else {
        return;
    };

    // Canonical output must survive a second pass unchanged, both with the
    // base it came from and without one, because callers downstream have no
    // idea a base was ever involved.
    let twice = canonicalize(&once, base).expect("canonical url failed to recanonicalise");
    assert_eq!(once, twice, "not idempotent with base {base:?}");

    let absolute = canonicalize(&once, None).expect("canonical url failed without a base");
    assert_eq!(once, absolute, "output depends on the base after canonicalisation");

    assert!(once.len() <= umi_types::canon::MAX_URL_LEN);
    assert!(!once.contains('#'));
});
