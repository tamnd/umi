//! Where doc 15.3's numbers come from.
//!
//! [`crate::backpressure`] is a state machine over a [`Signals`] struct and it
//! reads nothing. This is the other half: free space on the disk the segments
//! land on, and how much memory this process is holding. Both of them are
//! things the standard library has no API for, so both of them are a small
//! piece of platform specific code with a fallback that says it does not know.
//!
//! Not knowing is a supported answer and it is not the same as zero. A crawl
//! on a platform with no reading has to run at full rate rather than sit at
//! rung four forever, so every function here returns an `Option` and the
//! caller leaves the signal at its calm value when it comes back `None`. Doc
//! 15's servers are Linux and the tests run on macOS as well, so the two that
//! matter both have a reading and Windows does not.
//!
//! [`Signals`]: crate::Signals
//!
//! # Why this shells out
//!
//! `statvfs` is a libc call and this workspace denies `unsafe_code`, so a real
//! `statvfs` means either a dependency whose whole job is one syscall or a
//! `build.rs`. `df` is in POSIX, it is on every box that can run a crawler,
//! and `umi doctor` already reads free space the same way.
//!
//! It is not free. `benches/tick.rs` part 6 measures the spawn at 4 to 5 ms on
//! server3, against 37 microseconds for the `/proc` read next to it. Once every
//! ten seconds that is under a twentieth of one percent of the box, which is a
//! rounding error, but it is a few fetches worth of one thread's time and the
//! caller is expected to run it somewhere other than the loop.

use std::path::Path;
use std::process::Command;

/// Free bytes on the filesystem holding `path`, or `None` where there is no
/// reading.
///
/// Free rather than unused. `df` reports the space a normal user may still
/// take, which on ext4 is a few percent under the true figure because of the
/// reserved blocks, and the smaller number is the right one to steer on: the
/// reserve exists so that root can fix a full disk, not so that a crawler can
/// spend it.
#[must_use]
pub fn free_disk_bytes(path: &Path) -> Option<u64> {
    // A path that does not exist yet has no filesystem to ask about, and the
    // parent is the one the directory will be created on.
    let target = if path.exists() {
        path.to_path_buf()
    } else {
        path.parent()?.to_path_buf()
    };
    // `-P` is what makes this parseable. Without it `df` wraps a long device
    // name onto its own line and the fields move to the second one.
    let output = run("df", &["-Pk", &target.to_string_lossy()])?;
    let line = output.lines().nth(1)?;
    let free_kb: u64 = line.split_whitespace().nth(3)?.parse().ok()?;
    Some(free_kb.saturating_mul(1024))
}

/// Resident set size of this process, or `None` where there is no reading.
///
/// Resident and not virtual. Doc 03.4's budget is about pages that are really
/// in memory, and a virtual figure on a process that memory maps a state file
/// counts the whole file whether or not any of it has been touched.
#[must_use]
pub fn rss_bytes() -> Option<u64> {
    if let Some(kb) = proc_status_kb("VmRSS:") {
        return Some(kb.saturating_mul(1024));
    }
    // macOS. `ps` reports RSS in kilobytes and it is the same number Activity
    // Monitor shows.
    let pid = std::process::id().to_string();
    let output = run("ps", &["-o", "rss=", "-p", &pid])?;
    let kb: u64 = output.trim().parse().ok()?;
    Some(kb.saturating_mul(1024))
}

/// One `key: N kB` line out of `/proc/self/status`.
fn proc_status_kb(key: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix(key) {
            return rest.split_whitespace().next()?.parse().ok();
        }
    }
    None
}

/// Run a command and take its stdout, or `None` if it is missing or unhappy.
///
/// Quiet on every failure on purpose. A missing `df` is a platform without a
/// reading, and a crawl is not the place to explain that once every ten
/// seconds.
fn run(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both readings exist on the platforms doc 15 runs on, and neither is
    /// zero. Weak on purpose: the point is that the parse works and the units
    /// are bytes, and a test that asserted a number would fail on whichever
    /// runner happened to be busy.
    #[test]
    #[cfg(unix)]
    fn a_unix_box_has_both_readings() {
        let free = free_disk_bytes(Path::new(".")).expect("df should answer");
        assert!(free > 1 << 20, "{free} bytes free reads like a parse error");
        let rss = rss_bytes().expect("rss should be readable");
        assert!(
            rss > 1 << 20,
            "{rss} bytes resident reads like a parse error"
        );
        assert!(
            rss < 64 << 30,
            "{rss} bytes resident reads like a unit error"
        );
    }

    /// A directory that is not there yet is the normal case on a first run,
    /// and the answer is about the filesystem it would be created on rather
    /// than an error.
    #[test]
    #[cfg(unix)]
    fn a_directory_that_does_not_exist_yet_still_has_a_filesystem() {
        let dir = std::env::temp_dir().join("umi-probe-not-created");
        assert!(free_disk_bytes(&dir).is_some());
    }

    /// Nothing above panics or hangs on a path with no parent, which is what
    /// a bare relative name is.
    #[test]
    fn a_path_with_no_parent_is_not_a_panic() {
        let _ = free_disk_bytes(Path::new(""));
    }
}
