//! A line reader with a ceiling on the line.
//!
//! [`BufRead::read_line`] grows its buffer until it finds a newline, so a
//! seeder that prints a gigabyte without one takes a gigabyte of our memory
//! before we get a chance to reject it. Doc 13.7 lets anyone write a seeder,
//! which means one of them will do this by accident, and the crawler should
//! skip the line rather than fall over.
//!
//! The cap is on what is kept, not on what is read. The bytes still have to
//! stream past to find the next newline, but they go through the reader's own
//! buffer and are dropped, so the memory is constant however long the line is.

use std::io::{self, BufRead, ErrorKind};

/// What one call to [`read_capped`] found.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Line {
    /// A line, in `out`.
    Read,
    /// A line longer than the cap. It has been consumed and `out` is empty.
    TooLong,
    /// End of input, and there was nothing left.
    End,
}

/// Read one newline terminated line into `out`, keeping at most `cap` bytes.
///
/// The trailing newline is consumed and not stored. A carriage return before
/// it is left in place, because trimming is the caller's business and a bare
/// carriage return in the middle of a line is not ours to remove.
pub fn read_capped(input: &mut dyn BufRead, out: &mut Vec<u8>, cap: usize) -> io::Result<Line> {
    out.clear();
    let mut over = false;
    loop {
        let available = match input.fill_buf() {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if available.is_empty() {
            // End of input. A last line with no newline on it is still a line,
            // and an over long one is still over long.
            return Ok(if over {
                Line::TooLong
            } else if out.is_empty() {
                Line::End
            } else {
                Line::Read
            });
        }
        match available.iter().position(|&byte| byte == b'\n') {
            Some(at) => {
                if !over {
                    if out.len() + at > cap {
                        over = true;
                        out.clear();
                    } else {
                        out.extend_from_slice(&available[..at]);
                    }
                }
                input.consume(at + 1);
                return Ok(if over { Line::TooLong } else { Line::Read });
            }
            None => {
                let taken = available.len();
                if !over {
                    if out.len() + taken > cap {
                        over = true;
                        out.clear();
                    } else {
                        out.extend_from_slice(available);
                    }
                }
                input.consume(taken);
            }
        }
    }
}
