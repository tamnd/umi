//! `umi doctor`, doc 14.8.
//!
//! Checks the things that actually break, and it matters more than it looks
//! because doc 16's gate 1.1 is measured with it. Two of the lines are load
//! bearing. The `openssl-sys` check is doc 05.5's CI assertion run at runtime,
//! because the BoringSSL symbol prefix conflict produces link failures and
//! segfaults rather than clean errors. The bandwidth measurements are doc 01's
//! milestone 1 gate, and having them here means the number gets measured on
//! every box every time rather than once by whoever set it up.
//!
//! Every check returns a verdict rather than panicking or exiting, so that one
//! broken thing does not hide the other nine.

use std::fmt;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use crate::Error;

/// How a check came out.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Fine.
    Ok,
    /// Works, but something about it will bite later.
    Warn,
    /// Will not work.
    Bad,
    /// Not checked, because the caller asked for `--offline` or the platform
    /// has no way to answer. Never counted against the machine.
    Skip,
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ok => "ok",
            Self::Warn => "warn",
            Self::Bad => "bad",
            Self::Skip => "skip",
        })
    }
}

/// One line of the report.
pub struct Check {
    /// The left column, which is the thing being checked.
    pub name: &'static str,
    /// The middle column, which is what was found.
    pub detail: String,
    /// The right column.
    pub verdict: Verdict,
}

impl Check {
    fn new(name: &'static str, detail: impl Into<String>, verdict: Verdict) -> Self {
        Self {
            name,
            detail: detail.into(),
            verdict,
        }
    }
}

/// What `doctor` was asked to do.
pub struct Options {
    /// Skip everything that touches the network.
    pub offline: bool,
    /// The directory the crawl would write into.
    pub out: std::path::PathBuf,
    /// Run doc 16's gate 1.1 for real, for this many seconds per direction,
    /// rather than the one request sample. `None` is the default because this
    /// moves gigabytes and takes minutes, and `umi doctor` is a thing people
    /// run before a crawl rather than a thing people run once a quarter.
    pub bandwidth: Option<u64>,
}

/// Run every check and print the report.
///
/// # Errors
///
/// Never for a failed check, which is a report line and an exit code. Only when
/// the report itself cannot be written.
pub fn doctor(options: &Options) -> Result<Vec<Check>, Error> {
    let mut checks = vec![
        toolchain(),
        tls_backend(),
        emulation(),
        chromium(),
        dns(&options.offline),
        disk(&options.out),
        memory(),
    ];
    if options.offline {
        checks.push(Check::new("inbound sample", "--offline", Verdict::Skip));
        checks.push(Check::new("clock skew", "--offline", Verdict::Skip));
    } else {
        match options.bandwidth {
            Some(seconds) => checks.extend(sustained(seconds)),
            None => checks.push(bandwidth()),
        }
        checks.push(clock_skew());
    }

    let width = checks.iter().map(|c| c.name.len()).max().unwrap_or(0);
    for check in &checks {
        println!(
            "  {:<width$}  {:<48}  {}",
            check.name, check.detail, check.verdict
        );
    }
    Ok(checks)
}

/// The exit code doc 14.9 wants for a report.
#[must_use]
pub fn worst(checks: &[Check]) -> Verdict {
    if checks.iter().any(|c| c.verdict == Verdict::Bad) {
        Verdict::Bad
    } else if checks.iter().any(|c| c.verdict == Verdict::Warn) {
        Verdict::Warn
    } else {
        Verdict::Ok
    }
}

fn toolchain() -> Check {
    match run("rustc", &["--version"]) {
        Some(version) => {
            let found = version.split_whitespace().nth(1).unwrap_or("").to_owned();
            // Not a hard failure. A fetcher box runs a binary somebody else
            // built and has no reason to have a toolchain on it at all.
            let verdict = if found.is_empty() {
                Verdict::Warn
            } else {
                Verdict::Ok
            };
            Check::new("rust toolchain", found, verdict)
        }
        None => Check::new(
            "rust toolchain",
            "not on PATH, which is fine for a fetcher",
            Verdict::Skip,
        ),
    }
}

/// Doc 05.5's assertion, at runtime.
///
/// The build is rustls only and CI asserts that `openssl-sys` is not in the
/// tree, but a binary can still end up next to a system OpenSSL through a
/// transitive dynamic link, and that is the case that segfaults instead of
/// erroring. On Linux the loaded objects are readable, so read them. Elsewhere
/// say so rather than claiming a check that did not happen.
fn tls_backend() -> Check {
    let maps = Path::new("/proc/self/maps");
    if !maps.exists() {
        return Check::new(
            "tls backend",
            "rustls, no runtime check on this platform",
            Verdict::Skip,
        );
    }
    match std::fs::read_to_string(maps) {
        Ok(body) => {
            let openssl = body
                .lines()
                .any(|line| line.contains("libssl.so") || line.contains("libcrypto.so"));
            if openssl {
                Check::new(
                    "tls backend",
                    "libssl is mapped into this process, see doc 05.5",
                    Verdict::Bad,
                )
            } else {
                Check::new("tls backend", "rustls, no openssl mapped", Verdict::Ok)
            }
        }
        Err(_) => Check::new("tls backend", "cannot read /proc/self/maps", Verdict::Skip),
    }
}

fn emulation() -> Check {
    // Doc 05's T2 browser emulation is milestone 2. Reporting it as absent is
    // the honest answer and it is also the answer that stops somebody
    // wondering why a block signal did not escalate.
    Check::new(
        "emulation feature",
        "not built yet, milestone 2",
        Verdict::Skip,
    )
}

fn chromium() -> Check {
    for binary in ["chromium", "chromium-browser", "google-chrome"] {
        if let Some(path) = which(binary) {
            let version = run(&path, &["--version"]).unwrap_or_default();
            return Check::new(
                "chromium",
                format!("{path} {}", version.trim()),
                Verdict::Ok,
            );
        }
    }
    Check::new(
        "chromium",
        "absent, so tier 3 is unavailable on this box",
        Verdict::Warn,
    )
}

fn dns(offline: &bool) -> Check {
    if *offline {
        return Check::new("dns", "--offline", Verdict::Skip);
    }
    use std::net::ToSocketAddrs as _;
    let start = Instant::now();
    match ("huggingface.co", 443).to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(addr) => Check::new(
                "dns",
                format!("huggingface.co is {addr} in {}", ms(start.elapsed())),
                Verdict::Ok,
            ),
            None => Check::new("dns", "resolved to nothing", Verdict::Bad),
        },
        Err(cause) => Check::new("dns", format!("{cause}"), Verdict::Bad),
    }
}

/// Free space, through `df`, because std has no API for it and the alternative
/// is `libc::statvfs`, which this workspace cannot call.
fn disk(out: &Path) -> Check {
    let target = if out.exists() { out } else { Path::new(".") };
    let Some(output) = run("df", &["-k", &target.display().to_string()]) else {
        return Check::new("disk", "df is not on PATH", Verdict::Skip);
    };
    let Some(line) = output.lines().nth(1) else {
        return Check::new("disk", "df said nothing useful", Verdict::Skip);
    };
    // `df -k` puts available blocks in the fourth column on both Linux and
    // macOS. The field before it is used, and taking it from the end would
    // break on the capacity percentage.
    let Some(free_kb) = line
        .split_whitespace()
        .nth(3)
        .and_then(|f| f.parse::<u64>().ok())
    else {
        return Check::new("disk", "cannot read df output", Verdict::Skip);
    };
    let free_gb = free_kb / 1024 / 1024;
    // Doc 10.2 seals a segment at 128 MB and doc 12.1 gives publishing 10
    // minutes, so eight segments in flight is the number to have room for.
    const SEGMENT_MB: u64 = 128;
    const IN_FLIGHT: u64 = 8;
    let needed_gb = (SEGMENT_MB * IN_FLIGHT).div_ceil(1024);
    let verdict = if free_gb >= 24 {
        Verdict::Ok
    } else if free_gb >= needed_gb {
        Verdict::Warn
    } else {
        Verdict::Bad
    };
    Check::new(
        "disk",
        format!(
            "{} GB free at {}, {} GB needed for 8 segments",
            free_gb,
            target.display(),
            needed_gb
        ),
        verdict,
    )
}

fn memory() -> Check {
    // Doc 03.4 caps umid at 1.5 GB RSS and doc 10.8's writer takes 256 MB of
    // it, so the question is whether there is 1.5 GB to have.
    let budget_mb = 1536u64;
    let Some(available_mb) = available_memory_mb() else {
        return Check::new("memory", "no reading on this platform", Verdict::Skip);
    };
    let verdict = if available_mb >= budget_mb * 2 {
        Verdict::Ok
    } else if available_mb >= budget_mb {
        Verdict::Warn
    } else {
        Verdict::Bad
    };
    Check::new(
        "memory",
        format!("{available_mb} MB available, {budget_mb} MB budgeted"),
        verdict,
    )
}

fn available_memory_mb() -> Option<u64> {
    if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
        // MemAvailable rather than MemFree, because MemFree on a box that has
        // been up for 62 days reads near zero and means nothing.
        for line in meminfo.lines() {
            if let Some(rest) = line.strip_prefix("MemAvailable:") {
                return rest
                    .split_whitespace()
                    .next()
                    .and_then(|kb| kb.parse::<u64>().ok())
                    .map(|kb| kb / 1024);
            }
        }
        return None;
    }
    // macOS. Free plus inactive pages is the closest thing to MemAvailable
    // that `vm_stat` offers.
    let output = run("vm_stat", &[])?;
    let mut page_size = 4096u64;
    let mut pages = 0u64;
    for line in output.lines() {
        if let Some(rest) = line.split("page size of ").nth(1)
            && let Some(size) = rest.split_whitespace().next().and_then(|s| s.parse().ok())
        {
            page_size = size;
        }
        for prefix in ["Pages free:", "Pages inactive:"] {
            if let Some(rest) = line.strip_prefix(prefix) {
                pages += rest
                    .trim()
                    .trim_end_matches('.')
                    .parse::<u64>()
                    .unwrap_or(0);
            }
        }
    }
    Some(pages * page_size / 1024 / 1024)
}

/// Doc 16's gate 1.1 for real: sustained, both directions, named endpoints.
///
/// Two report lines, plus a block on stderr naming what each endpoint
/// delivered. The block is not decoration. Doc 01's first attempt at this gate
/// was ruined by an endpoint that returned one byte, and the only thing that
/// would have caught it at the time was a per endpoint number to look at.
fn sustained(seconds: u64) -> Vec<Check> {
    let options = crate::bandwidth::Options {
        seconds,
        ..crate::bandwidth::Options::default()
    };
    eprintln!(
        "  measuring {seconds} s in each direction over {} streams, this takes about {} minutes",
        options.streams,
        (seconds * 2).div_ceil(60)
    );
    let (inbound, outbound) = match crate::bandwidth::measure(&options) {
        Ok(both) => both,
        Err(cause) => return vec![Check::new("bandwidth", cause, Verdict::Skip)],
    };
    vec![
        report(&inbound, &options, 53_100.0),
        report(&outbound, &options, 6_000.0),
    ]
}

/// One direction, turned into a line and a block.
///
/// `per_page` is what a page costs in that direction: doc 01's measured 53.1 KB
/// of fetched response inbound, doc 10.2's 6 KB of published output outbound.
fn report(
    direction: &crate::bandwidth::Direction,
    options: &crate::bandwidth::Options,
    per_page: f64,
) -> Check {
    let name = if direction.name == "inbound" {
        "inbound sustained"
    } else {
        "outbound sustained"
    };
    eprintln!(
        "  {} {:.1} Mbit/s, {:.2} GB in {:.1} s",
        direction.name,
        direction.mbit(),
        direction.bytes as f64 / 1e9,
        direction.elapsed.as_secs_f64(),
    );
    for (endpoint, bytes) in &direction.per_endpoint {
        // Megabytes rather than gigabytes, because the number that matters
        // most here is the one near zero and `0.00 GB` does not say whether an
        // endpoint was slow or absent.
        eprintln!("      {endpoint}  {:.0} MB", *bytes as f64 / 1e6);
    }
    if let Some(interface) = direction.interface {
        eprintln!(
            "      the interface counters saw {:.2} GB over the same window, \
             which includes whatever else this box is doing",
            interface as f64 / 1e9
        );
    }
    for error in &direction.errors {
        eprintln!("      {error}");
    }

    if !direction.plausible(options) {
        let dead = direction.dead();
        let detail = if dead.is_empty() {
            format!(
                "only {:.2} GB in {:.1} s, not a measurement",
                direction.bytes as f64 / 1e9,
                direction.elapsed.as_secs_f64()
            )
        } else {
            format!(
                "{} of {} endpoints delivered nothing",
                dead.len(),
                direction.per_endpoint.len()
            )
        };
        return Check::new(name, detail, Verdict::Bad);
    }

    let seconds = direction.elapsed.as_secs_f64().max(0.001);
    let pages = direction.bytes as f64 / seconds / per_page;
    // 250 pages a second per host is doc 01's target and doc 16's gate 3.
    // Between 150 and 250 is server1, which is a real box that stays in the
    // fleet doing less fetching rather than a box that fails a check.
    let verdict = if pages >= 250.0 {
        Verdict::Ok
    } else if pages >= 150.0 {
        Verdict::Warn
    } else {
        Verdict::Bad
    };
    // A dead endpoint on a run that otherwise looks fine still belongs in the
    // summary, because it means the rate came off three endpoints rather than
    // four and is a floor rather than the link. server1 reads cachefly at zero
    // every time, and the line should say so without anybody reading the block
    // above it.
    let dead = match direction.dead().len() {
        0 => String::new(),
        1 => ", 1 endpoint dead".to_owned(),
        many => format!(", {many} endpoints dead"),
    };
    Check::new(
        name,
        format!(
            "{:.0} Mbit/s, {pages:.0} pages/s at {:.1} KB{dead}",
            direction.mbit(),
            per_page / 1000.0
        ),
        verdict,
    )
}

/// Doc 01's milestone 1 gate, measured rather than assumed.
///
/// One range request against a large public object on the Hugging Face CDN,
/// timed. It is a download and not an upload, so it measures inbound, and the
/// outbound number a publish needs is reported as unmeasured rather than
/// guessed from it, because asymmetric links are the normal case.
fn bandwidth() -> Check {
    const URL: &str = "https://huggingface.co/api/models?limit=1";
    let start = Instant::now();
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(cause) => return Check::new("inbound sample", format!("{cause}"), Verdict::Skip),
    };
    let result = runtime.block_on(async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent(umi_fetch::USER_AGENT)
            .build()?;
        let body = client.get(URL).send().await?.bytes().await?;
        Ok::<usize, reqwest::Error>(body.len())
    });
    match result {
        Ok(bytes) => {
            let elapsed = start.elapsed().as_secs_f64().max(0.001);
            Check::new(
                "inbound sample",
                format!(
                    "{bytes} B in {}, {:.1} MB/s, too small to be the gate",
                    ms(start.elapsed()),
                    bytes as f64 / elapsed / 1e6
                ),
                Verdict::Warn,
            )
        }
        Err(cause) => Check::new("inbound sample", format!("{cause}"), Verdict::Bad),
    }
}

/// Doc 04's receipts are bound to a lease with a deadline, so a box whose clock
/// is minutes out issues or accepts leases that look expired to everyone else.
fn clock_skew() -> Check {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(cause) => return Check::new("clock skew", format!("{cause}"), Verdict::Skip),
    };
    let result = runtime.block_on(async {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent(umi_fetch::USER_AGENT)
            .build()?;
        let response = client.head("https://huggingface.co/").send().await?;
        let header = |name: http::header::HeaderName| {
            response
                .headers()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        Ok::<(Option<String>, Option<String>), reqwest::Error>((
            header(http::header::DATE),
            header(http::header::AGE),
        ))
    });
    match result {
        Ok((Some(date), age)) => match umi_fetch::date::parse(&date) {
            Some(remote_ms) => {
                // `Age` matters and leaving it out is how this check spent a
                // day reporting a minute of skew on three boxes whose clocks
                // were within a second of each other. huggingface.co is behind
                // CloudFront, a cache hit carries the `Date` the origin wrote
                // when it filled the cache, and `Age` is how long ago that was.
                // RFC 9111 puts the origin's idea of now at `Date` plus `Age`,
                // which is the number to compare a clock against.
                let age_ms = age
                    .as_deref()
                    .and_then(|value| value.trim().parse::<i64>().ok())
                    .unwrap_or(0)
                    .clamp(0, 86_400)
                    * 1000;
                let local_ms = now_ms();
                let skew = i64::try_from(local_ms).unwrap_or(i64::MAX)
                    - i64::try_from(remote_ms).unwrap_or(i64::MAX)
                    - age_ms;
                // A `Date` header has one second resolution, so anything under
                // two seconds is indistinguishable from correct.
                let verdict = if skew.abs() < 2000 {
                    Verdict::Ok
                } else if skew.abs() < 60_000 {
                    Verdict::Warn
                } else {
                    Verdict::Bad
                };
                let cached = if age_ms > 0 {
                    format!(", off a cache {} s old", age_ms / 1000)
                } else {
                    String::new()
                };
                Check::new(
                    "clock skew",
                    format!("{skew:+} ms against huggingface.co{cached}"),
                    verdict,
                )
            }
            None => Check::new(
                "clock skew",
                format!("cannot parse {date:?}"),
                Verdict::Skip,
            ),
        },
        Ok((None, _)) => Check::new("clock skew", "no Date header came back", Verdict::Skip),
        Err(cause) => Check::new("clock skew", format!("{cause}"), Verdict::Bad),
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn which(binary: &str) -> Option<String> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(binary))
        .find(|candidate| candidate.is_file())
        .map(|found| found.display().to_string())
}

fn run(binary: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(binary).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

fn ms(elapsed: Duration) -> String {
    format!("{:.0} ms", elapsed.as_secs_f64() * 1000.0)
}
