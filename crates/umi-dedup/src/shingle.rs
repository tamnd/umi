//! Doc 11.8's 5-gram shingles, hashed with xxh3.
//!
//! A shingle is five consecutive whitespace separated tokens of the normalised
//! text from doc 11.3. Word grams rather than character grams because doc 04.6
//! writes "5-shingles of the text" and because character grams over a language
//! with no spaces would produce a very different sketch for Japanese than for
//! English, and a large part of what we crawl is Japanese.
//!
//! The text is already normalised by the time it gets here: runs of ASCII
//! whitespace have collapsed to one space, each block is trimmed, and the whole
//! thing is NFC. So splitting on ASCII whitespace is the whole tokenizer and
//! there is nothing else to do.
//!
//! Case is left alone, deliberately. Doc 11.3 defines the normalised text and
//! lowercasing is not one of the steps it performs, so folding case here would
//! mean the sketch is computed over a string that is not the one whose digest
//! goes in `text_digest`, and two fetchers reading two different sections of
//! the spec would disagree about which. The cost is that a page republished
//! with a different heading capitalisation scores slightly lower, which is well
//! inside the 0.77 band threshold.

/// How many tokens are in one shingle.
pub const SHINGLE: usize = 5;

/// The 64 bit hashes of a text's shingles, in text order.
///
/// An iterator rather than a `Vec` because a 150 KB document has about 25000
/// shingles and the sketch consumes them once, in order, and never looks back.
/// Materialising them would cost 200 KB of allocation per page to hold data
/// that is dead as soon as it is read, and at 250 pages per second per server
/// that is 50 MB/s of pointless allocator traffic.
///
/// A text with fewer than [`SHINGLE`] tokens yields nothing. That is the right
/// answer rather than a special case: a four word page has no 5-gram, and
/// [`Sketch`](crate::Sketch) turns an empty shingle stream into the empty
/// sketch, which compares as similar to nothing including itself.
#[derive(Clone, Debug)]
pub struct Shingles<'a> {
    text: &'a str,
    /// Byte ranges of the tokens in the window, oldest first. A ring would
    /// avoid the shift but the window is five entries and the shift is five
    /// moves of a `usize` pair, which is cheaper than the index arithmetic a
    /// ring would need.
    window: [(usize, usize); SHINGLE],
    /// How many of `window` are filled, saturating at [`SHINGLE`].
    filled: usize,
    /// Where the tokenizer is, as a byte offset into `text`.
    at: usize,
}

impl<'a> Shingles<'a> {
    /// Start shingling this normalised text.
    #[must_use]
    pub const fn new(text: &'a str) -> Self {
        Self {
            text,
            window: [(0, 0); SHINGLE],
            filled: 0,
            at: 0,
        }
    }

    /// The next whitespace separated token, as a byte range.
    fn next_token(&mut self) -> Option<(usize, usize)> {
        let bytes = self.text.as_bytes();
        while self.at < bytes.len() && bytes[self.at].is_ascii_whitespace() {
            self.at += 1;
        }
        if self.at >= bytes.len() {
            return None;
        }
        let start = self.at;
        while self.at < bytes.len() && !bytes[self.at].is_ascii_whitespace() {
            self.at += 1;
        }
        Some((start, self.at))
    }
}

impl Iterator for Shingles<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        loop {
            let token = self.next_token()?;
            self.window.copy_within(1..SHINGLE, 0);
            self.window[SHINGLE - 1] = token;
            if self.filled < SHINGLE {
                self.filled += 1;
                if self.filled < SHINGLE {
                    continue;
                }
            }
            let from = self.window[0].0;
            let to = self.window[SHINGLE - 1].1;
            // Hashed as one slice of the original text rather than as five
            // tokens joined by a space, which avoids a per shingle allocation
            // that would otherwise dominate this loop. The separators inside
            // the slice are whatever doc 11.3 left there, so a shingle that
            // spans a block boundary carries that boundary's newline and
            // hashes differently from the same five words inside one
            // paragraph. That is one shingle in a document that has thousands
            // and it is the same on every machine, which is what matters.
            return Some(twox_hash::XxHash3_64::oneshot(
                &self.text.as_bytes()[from..to],
            ));
        }
    }
}

/// Hash every 5-gram shingle of a normalised text, in text order.
#[must_use]
pub fn shingles(text: &str) -> Shingles<'_> {
    Shingles::new(text)
}
