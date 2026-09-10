//! What the box has free, for sizing the pager against.
//!
//! Doc 08.5 says `mmap_size` is set to the smaller of the file size and
//! available memory, and doc 08's ceiling note says admission ends when the
//! seen set index stops fitting in page cache. Both of those are statements
//! about a machine and neither of them can be answered by a constant. The
//! constant that was here read 64 MB of cache and a gigabyte of mapping on
//! every box in the fleet, which is a reasonable guess for a laptop and a
//! twentieth of what server3 can spare.
//!
//! The reading is `MemAvailable` and not `MemFree`, for the same reason
//! `umi doctor` uses it: server3 has been up for seventy eight days and
//! `MemFree` on it reads near nothing, because a kernel that leaves memory
//! unused is a kernel wasting it. `MemAvailable` is the kernel's own estimate
//! of what a new allocation could take without swapping, which is the question
//! being asked.
//!
//! Linux only. Everywhere else this returns nothing and the caller keeps the
//! conservative constant it always had. A crawl runs on Linux, and a macOS
//! reading would mean either shelling out to `vm_stat` from a library or a
//! platform crate, neither of which is worth it to make a development box
//! slightly faster at a benchmark.

/// Memory the kernel thinks a new allocation could have, in bytes.
///
/// `None` when there is no reading to be had, which is every platform that is
/// not Linux and a Linux with no `/proc` mounted.
pub(crate) fn available_bytes() -> Option<u64> {
    read_available(&std::fs::read_to_string("/proc/meminfo").ok()?)
}

/// The parse, split out so a test can hand it a file rather than a machine.
fn read_available(meminfo: &str) -> Option<u64> {
    // MemAvailable is the third line on every kernel that has it, but it is
    // read by name rather than by position, because the line above it is
    // MemFree and picking that one up by accident is a reading that is wrong
    // and plausible at the same time.
    let rest = meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemAvailable:"))?;
    // The unit is in the file and it has always been kB, but reading it is
    // free and assuming it is the kind of thing that is wrong by a factor of
    // 1024 for a year before anyone notices.
    let mut parts = rest.split_whitespace();
    let value: u64 = parts.next()?.parse().ok()?;
    match parts.next() {
        Some("kB") => Some(value * 1024),
        None => Some(value),
        Some(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::read_available;

    #[test]
    fn a_reading_in_kilobytes_comes_back_in_bytes() {
        let meminfo = "MemAvailable:   19626880 kB\nSwapFree:  0 kB\n";
        assert_eq!(read_available(meminfo), Some(19_626_880 * 1024));
    }

    #[test]
    fn a_unit_that_is_not_kilobytes_is_no_reading_at_all() {
        // Rather than a reading that is out by a factor of a thousand, which
        // would size the cache at nothing and be very hard to see.
        assert_eq!(read_available("MemAvailable: 12 MB\n"), None);
    }

    #[test]
    fn a_file_without_the_line_is_no_reading() {
        assert_eq!(read_available("MemTotal: 1 kB\n"), None);
    }

    #[test]
    fn the_lines_above_it_are_not_mistaken_for_it() {
        // The real file opens with MemTotal and MemFree, and both of them
        // would parse. Reading by position rather than by name is how you get
        // a cache sized off MemFree on a box that has been up for months.
        let meminfo = concat!(
            "MemTotal:       24605384 kB\n",
            "MemFree:          312044 kB\n",
            "MemAvailable:   19626880 kB\n",
        );
        assert_eq!(read_available(meminfo), Some(19_626_880 * 1024));
    }
}
