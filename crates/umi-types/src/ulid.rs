//! Segment identifiers, from `docs/spec/12-publishing.md` section 12.4.

use core::fmt;

/// Crockford's base32, which is the alphabet ULID is defined over.
///
/// The letters I, L, O and U are missing on purpose. I and L are confusable
/// with 1, O with 0, and U is out so that no accidental four letter word
/// appears in an identifier that ends up in a public file name.
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// A ULID: 48 bits of millisecond timestamp, then 80 bits of randomness.
///
/// Doc 12.4 names every published Parquet file after the segment's ULID and
/// doc 10.4 puts the same 16 bytes in the segment header, so the two are the
/// same value seen through two spellings and this type is the conversion.
///
/// The reason it is a ULID rather than a UUID is the sort order. A day folder
/// holds about 3100 files, and listing it in name order gives them in the order
/// they were sealed, which makes a manifest diff and a reconciliation listing
/// readable without parsing anything.
///
/// Nothing here reads a clock. Doc 11.1 keeps time an argument everywhere in
/// the output path, and a type that could mint an identifier out of nowhere
/// would be the one place that rule leaked.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Ulid([u8; 16]);

impl Ulid {
    /// How many characters the text form takes. Always this many, zero padded,
    /// so file names sort as bytes.
    pub const TEXT_LEN: usize = 26;

    /// Wrap the 16 bytes a segment header carries.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Borrow the 16 bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Build one from a timestamp and 80 bits of entropy.
    ///
    /// The caller supplies both, which is the point. The timestamp is
    /// truncated to 48 bits, which runs out in the year 10889 and is not a
    /// problem worth a `Result`.
    #[must_use]
    pub const fn new(ms: u64, entropy: [u8; 10]) -> Self {
        let t = ms.to_be_bytes();
        Self([
            t[2], t[3], t[4], t[5], t[6], t[7], entropy[0], entropy[1], entropy[2], entropy[3],
            entropy[4], entropy[5], entropy[6], entropy[7], entropy[8], entropy[9],
        ])
    }

    /// The millisecond timestamp in the first 48 bits.
    #[must_use]
    pub const fn timestamp_ms(&self) -> u64 {
        let b = &self.0;
        ((b[0] as u64) << 40)
            | ((b[1] as u64) << 32)
            | ((b[2] as u64) << 24)
            | ((b[3] as u64) << 16)
            | ((b[4] as u64) << 8)
            | (b[5] as u64)
    }

    /// The 26 character text form.
    #[must_use]
    pub fn to_text(self) -> String {
        // 128 bits does not divide into 5 bit groups, so the first character
        // carries only the top 2 bits and the remaining 25 carry 125. Reading
        // the whole thing as one big endian integer and shifting down is
        // clearer than tracking a bit cursor across 16 bytes, and a u128 shift
        // is a single instruction.
        let n = u128::from_be_bytes(self.0);
        let mut out = [0u8; Self::TEXT_LEN];
        for (i, slot) in out.iter_mut().enumerate() {
            let shift = 5 * (Self::TEXT_LEN - 1 - i);
            *slot = ALPHABET[((n >> shift) & 0x1f) as usize];
        }
        // Every byte came out of `ALPHABET`, which is ASCII.
        String::from_utf8(out.to_vec()).unwrap_or_default()
    }

    /// Parse the 26 character text form.
    ///
    /// Case insensitive, because Crockford's alphabet is, and a file name that
    /// came back from an API lowercased should still parse. The four excluded
    /// letters are rejected rather than folded onto their lookalikes: a name
    /// with an `I` in it did not come from us, and quietly reading it as a `1`
    /// would turn a corrupt listing into a wrong lookup.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let bytes = text.as_bytes();
        if bytes.len() != Self::TEXT_LEN {
            return None;
        }
        // The first character carries 2 bits, so anything above 7 would
        // overflow 128 bits and means the text is not a ULID.
        let mut n: u128 = 0;
        for (i, byte) in bytes.iter().enumerate() {
            let upper = byte.to_ascii_uppercase();
            let value = ALPHABET.iter().position(|c| *c == upper)? as u128;
            if i == 0 && value > 7 {
                return None;
            }
            n = (n << 5) | value;
        }
        Some(Self(n.to_be_bytes()))
    }
}

impl fmt::Display for Ulid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_text())
    }
}

impl fmt::Debug for Ulid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Ulid({})", self.to_text())
    }
}

impl From<[u8; 16]> for Ulid {
    fn from(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl From<Ulid> for [u8; 16] {
    fn from(ulid: Ulid) -> Self {
        ulid.0
    }
}

#[cfg(test)]
mod tests {
    use super::Ulid;

    #[test]
    fn the_text_form_round_trips() {
        for seed in 0..256u16 {
            let mut bytes = [0u8; 16];
            for (i, slot) in bytes.iter_mut().enumerate() {
                // Anything that is not all zero and not all one, spread over
                // every byte so no lane of the shift loop is left untested.
                *slot = (seed as u8).wrapping_mul(31).wrapping_add(i as u8 * 7);
            }
            // Keep the top 2 bits inside what 26 characters can hold.
            bytes[0] &= 0x7f;
            let ulid = Ulid::from_bytes(bytes);
            let text = ulid.to_text();
            assert_eq!(text.len(), Ulid::TEXT_LEN);
            assert_eq!(Ulid::parse(&text), Some(ulid), "{text}");
        }
    }

    #[test]
    fn the_timestamp_comes_back_out() {
        let ms = 1_760_000_000_000;
        let ulid = Ulid::new(ms, [0xab; 10]);
        assert_eq!(ulid.timestamp_ms(), ms);
    }

    #[test]
    fn text_order_matches_time_order() {
        // This is the whole reason for using a ULID rather than a UUID, so it
        // gets a test rather than a comment.
        let early = Ulid::new(1_760_000_000_000, [0xff; 10]).to_text();
        let late = Ulid::new(1_760_000_000_001, [0x00; 10]).to_text();
        assert!(early < late, "{early} should sort before {late}");
    }

    #[test]
    fn the_confusable_letters_are_refused_rather_than_folded() {
        let good = Ulid::new(1_760_000_000_000, [1; 10]).to_text();
        for bad in ['I', 'L', 'O', 'U'] {
            let mut text: Vec<char> = good.chars().collect();
            text[10] = bad;
            let text: String = text.into_iter().collect();
            assert_eq!(Ulid::parse(&text), None, "{text}");
        }
    }

    #[test]
    fn a_name_of_the_wrong_length_is_refused() {
        let good = Ulid::new(1_760_000_000_000, [1; 10]).to_text();
        assert_eq!(Ulid::parse(&good[..25]), None);
        assert_eq!(Ulid::parse(&format!("{good}0")), None);
        assert_eq!(Ulid::parse(""), None);
    }

    #[test]
    fn lowercase_parses_because_an_api_may_have_lowercased_it() {
        let ulid = Ulid::new(1_760_000_000_000, [9; 10]);
        assert_eq!(Ulid::parse(&ulid.to_text().to_lowercase()), Some(ulid));
    }

    #[test]
    fn a_text_form_that_would_overflow_128_bits_is_refused() {
        // 'Z' is 31, which needs 5 bits, and the first character has room for
        // 2. Every ULID we mint starts at '0' through '7'.
        assert_eq!(Ulid::parse("Z0000000000000000000000000"), None);
        assert!(Ulid::parse("70000000000000000000000000").is_some());
    }
}
