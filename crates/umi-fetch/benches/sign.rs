//! What a Web Bot Auth signature costs, against gate 1.1's 250 pages a second.
//!
//! Doc 07.2 puts a signature on every outgoing request, so this is work the
//! fetcher does per request rather than per page, and there are more requests
//! than pages once robots.txt fetches and redirects are counted. An Ed25519
//! signature is cheap, but cheap is a claim and this is where it gets checked
//! against the only budget that matters: one server, 250 pages a second, and
//! two cores to do everything else in.
//!
//! Three numbers come out of it. The whole of [`Signer::sign`], which is what
//! the ladder pays. The signature base on its own, which is string formatting
//! and is the part that would grow if the covered component list grew. And
//! [`verify`], which is the origin's cost, not ours, but we are asking origins
//! to run it on requests we send them and it would be rude to ask without
//! knowing the answer.
//!
//! Deliberately not a criterion benchmark, like every other bench in this tree.
//! A small number of large measurements against a fixed target, best of five.
//!
//! Run it pinned, since an unpinned run on a box that is also crawling measures
//! the scheduler:
//!
//! ```text
//! cargo bench -p umi-fetch --no-run
//! taskset -c 7 chrt --fifo 50 ./target/release/deps/sign-<hash>
//! ```

use std::hint::black_box;
use std::time::{Duration, Instant};

use http::{HeaderMap, HeaderName, HeaderValue};
use umi_fetch::webbotauth::{
    ALG, COVERED, Directory, LIFETIME_SECS, Params, Signer, TAG, signature_base, verify,
};
use url::Url;

/// A fixed seed, because a benchmark that generates a key each run measures
/// key generation.
const SEED: [u8; 32] = [42; 32];

/// The nonce seed umi-cli would derive at startup.
const NONCE_SEED: [u8; 16] = [7; 16];

/// A moment in time. Any moment, as long as it is the same one every run.
const T0: u64 = 1_756_400_000;

/// How many requests each measured pass signs.
const BATCH: usize = 20_000;

fn main() {
    let signer =
        Signer::fixed(SEED, "https://umi.dev", NONCE_SEED, T0).expect("the agent is a url");
    let directory = Directory::of(&signer.jwk(None, None).key().expect("the key parses"));
    let covered: Vec<String> = COVERED.iter().map(|c| (*c).to_owned()).collect();

    // Three shapes of URL, because the base is built from the path and a long
    // one costs more to format than a short one.
    let urls: Vec<Url> = [
        "https://example.com/",
        "https://en.example.org/wiki/Special:Search?q=web+bot+auth&ns=0",
        "https://shop.example.net/catalogue/2026/spring/mens/footwear/running/\
         lightweight/model-4417-blue-size-44?ref=nav&page=3&sort=price",
    ]
    .iter()
    .map(|u| Url::parse(u).expect("a url"))
    .collect();

    println!("\ndoc 07.2 request signing, best of 5, {BATCH} requests a pass\n");

    println!("part 1: what the fetcher pays, per request");
    println!(
        "{:<40}{:>10}{:>13}{:>12}",
        "step", "us/req", "req/s", "of a core"
    );

    let whole = best(5, || {
        for i in 0..BATCH {
            black_box(
                signer
                    .sign_at("GET", &urls[i % urls.len()], T0)
                    .expect("the url has a host"),
            );
        }
        BATCH
    });
    line("Signer::sign, base and ed25519 and all", whole);

    let params = Params {
        created: T0,
        expires: T0 + LIFETIME_SECS,
        keyid: signer.keyid().to_owned(),
        alg: ALG.to_owned(),
        nonce: "AAAAAAAAAAAAAAAAAAAAAA".to_owned(),
        tag: TAG.to_owned(),
    };
    let agent = fields(signer.agent());
    let base_only = best(5, || {
        for i in 0..BATCH {
            black_box(
                signature_base("GET", &urls[i % urls.len()], &covered, &params, &agent)
                    .expect("the url has a host"),
            );
        }
        BATCH
    });
    line("the signature base on its own", base_only);

    println!();
    println!("part 2: what the origin pays, per request, on the same box");
    println!(
        "{:<40}{:>10}{:>13}{:>12}",
        "step", "us/req", "req/s", "of a core"
    );

    let signed: Vec<HeaderMap> = urls
        .iter()
        .map(|url| {
            let s = signer.sign_at("GET", url, T0).expect("the url has a host");
            let mut headers = HeaderMap::new();
            for (name, value) in s.headers() {
                headers.insert(
                    HeaderName::from_bytes(name.as_bytes()).expect("a header name"),
                    HeaderValue::from_str(value).expect("a header value"),
                );
            }
            headers
        })
        .collect();

    let checked = best(5, || {
        for i in 0..BATCH {
            let n = i % urls.len();
            black_box(
                verify("GET", &urls[n], &signed[n], &directory, T0 + 1)
                    .expect("it was just signed"),
            );
        }
        BATCH
    });
    line("verify, all the way to the ed25519 check", checked);

    // The same requests an hour later, which is the shape of a replayed one.
    // It is worth its own line because an origin under load is mostly rejecting
    // and the reject path should not be the expensive one.
    let stale = best(5, || {
        for i in 0..BATCH {
            let n = i % urls.len();
            black_box(verify("GET", &urls[n], &signed[n], &directory, T0 + 3600).is_err());
        }
        BATCH
    });
    line("verify, refusing an expired signature", stale);

    println!();
    let per = whole.per_item().as_secs_f64();
    println!(
        "signing costs {:.1} us a request, which is {:.0} requests a second on\n\
         one core. Gate 1.1 wants 250 pages a second on one server, so at one\n\
         request a page signing is {:.3} percent of a core and at four requests\n\
         a page it is {:.3} percent.",
        per * 1e6,
        1.0 / per,
        250.0 * per * 100.0,
        1000.0 * per * 100.0
    );
    let base = base_only.per_item().as_secs_f64();
    println!(
        "the base is {:.1} us of that, so {:.0} percent of the cost is the\n\
         ed25519 signature and the rest is formatting a few hundred bytes.",
        base * 1e6,
        100.0 * (per - base) / per
    );
    let origin = checked.per_item().as_secs_f64();
    println!(
        "an origin verifying us pays {:.1} us a request, or {:.0} requests a\n\
         second on one core, which is the number to quote when somebody asks\n\
         what checking our signature will cost them.",
        origin * 1e6,
        1.0 / origin
    );
}

/// The header fields the signature base reads, which for doc 07.2's covered
/// list is `Signature-Agent` and nothing else.
fn fields(agent: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("signature-agent"),
        HeaderValue::from_str(&format!("\"{agent}\"")).expect("a header value"),
    );
    headers
}

/// One measured pass and how many items it covered.
#[derive(Clone, Copy)]
struct Run {
    elapsed: Duration,
    items: usize,
}

impl Run {
    fn per_item(self) -> Duration {
        self.elapsed / u32::try_from(self.items.max(1)).unwrap_or(u32::MAX)
    }
}

/// Run a body a few times and keep the fastest, which is the usual way to take
/// the scheduler and the page cache back out of a number.
fn best(times: usize, mut body: impl FnMut() -> usize) -> Run {
    let mut fastest = Duration::MAX;
    let mut items = 1;
    for _ in 0..times {
        let start = Instant::now();
        let n = body();
        let elapsed = start.elapsed();
        if elapsed < fastest {
            fastest = elapsed;
            items = n;
        }
    }
    Run {
        elapsed: fastest,
        items,
    }
}

fn line(name: &str, run: Run) {
    let per = run.per_item().as_secs_f64();
    println!(
        "{:<40}{:>10.2}{:>13.0}{:>11.3}%",
        name,
        per * 1e6,
        1.0 / per,
        250.0 * per * 100.0
    );
}
