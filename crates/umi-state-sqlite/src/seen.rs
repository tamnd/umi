//! A bounded exact memory of url keys that are already in the `seen` table.
//!
//! Admission is the largest write in a tick and most of it is wasted. The tick
//! line on a real crawl of server3 says the first big tick admitted 883,976
//! links of 1,689,135 seen, the next admitted 12,081 of 25,916 and the one
//! after admitted 11,227 of 36,696, so the share of links that are new falls
//! from about a half to about a third as the frontier fills, and every one of
//! the others costs a root to leaf descent into a b-tree to be told we already
//! had it. At doc 16's five hundred million rows that tree does not fit in the
//! page cache and the descent is disk.
//!
//! So this sits in front of the table and answers the easy half without asking
//! it. A hit skips a statement. A miss costs a comparison against ten bytes
//! that were already in cache, and then does exactly what the code did before.
//!
//! # Why it is exact and not a filter
//!
//! A bloom filter is the obvious shape and it is the wrong one. A false
//! positive here does not cost a round trip, it drops a url out of the crawl
//! permanently and silently, because the only record that we ever saw it is
//! the answer this gives. There is no second chance and nothing downstream
//! notices. So a slot holds the whole eighty bit key and a hit means the key
//! matched every bit, which cannot be wrong. The direction it is allowed to be
//! wrong in is the harmless one: a miss on a url that is in the table costs one
//! statement, which is the price of not having this at all.
//!
//! # Why it is direct mapped
//!
//! Because the memory has to be bounded and eviction has to be free. There is
//! one slot per key and a write to an occupied slot overwrites it. No chains,
//! no probing, no load factor to watch, no rehash, and the whole table is one
//! allocation that never moves. Losing an entry costs a statement and nothing
//! else, so there is no policy worth the code it would take.
//!
//! The slot comes from the top bits of the key rather than a hash of it. A url
//! key is a blake3 prefix, so its top bits are already uniform and hashing them
//! again would buy nothing. Taking them from the top is what makes the table
//! walk in the same order as the batch: `admit` already sorts its candidates by
//! url key so that the inserts go into the b-tree in order, and a table indexed
//! by the leading bits of that same key is then walked from left to right by
//! the same loop. The probes are sequential memory rather than scattered.

use std::fmt;

use umi_types::UrlKey;

/// Bytes in a url key.
const KEY: usize = 10;

/// Slots in the table.
///
/// Sixteen million and change, which is 160 MB. The number is a compromise and
/// it is worth saying which way it is wrong. Doc 16 caps the resident frontier
/// at a hundred million urls, so a table this size covers a sixth of the worst
/// case and all of anything smaller, and the miss rate rises smoothly rather
/// than falling off a cliff as the frontier passes it. The reason it is not
/// larger is that this is allocated per state backend on every box that opens
/// one, including a laptop running the tests, and 160 MB of untouched zero
/// pages is a size nobody has to think about. It is the first number to raise
/// when a box is doing nothing but crawling.
const SLOTS: usize = 1 << 24;

/// Bits of the key that pick a slot.
const BITS: u32 = SLOTS.trailing_zeros();

/// Url keys known to be in the `seen` table, at most one per slot.
pub struct Seen {
    /// `SLOTS` keys laid end to end, zero meaning empty.
    slots: Box<[u8]>,
}

impl Seen {
    /// An empty table.
    ///
    /// The allocation is zeroed, which on every platform we run on means the
    /// pages are not really there until they are written to, so an open that
    /// never admits anything costs address space and no memory.
    pub fn new() -> Self {
        Self {
            slots: vec![0u8; SLOTS * KEY].into_boxed_slice(),
        }
    }

    /// Whether this key is definitely in the `seen` table.
    ///
    /// False means ask SQLite, and is the answer for a key that was never here
    /// and for one that has been overwritten since.
    pub fn holds(&self, key: &UrlKey) -> bool {
        let key = key.as_bytes();
        // The empty slot is all zeros and so is a defaulted key, so a defaulted
        // key would match every empty slot in the table. It cannot come out of
        // blake3 in this universe, but `UrlKey::default()` is a value this
        // process can construct, and the cost of ruling it out is a comparison
        // against a constant.
        !is_zero(key) && self.slot(key) == key
    }

    /// Record that this key is in the `seen` table.
    ///
    /// Call this after the transaction that put it there has committed. A key
    /// remembered from a transaction that then rolled back would be a key this
    /// answers `true` for and SQLite has never heard of, which is the one way
    /// to lose a url.
    pub fn remember(&mut self, key: &UrlKey) {
        let key = key.as_bytes();
        if is_zero(key) {
            return;
        }
        let at = index(key);
        self.slots[at..at + KEY].copy_from_slice(key);
    }

    /// The bytes currently in this key's slot.
    fn slot(&self, key: &[u8]) -> &[u8] {
        let at = index(key);
        &self.slots[at..at + KEY]
    }
}

impl fmt::Debug for Seen {
    /// Its size and nothing else. `Inner` derives `Debug` and this is 160 MB
    /// of key material, so the derived version would be a way to fill a disk
    /// from a log line.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Seen({SLOTS} slots)")
    }
}

/// Where in `slots` this key's ten bytes start.
fn index(key: &[u8]) -> usize {
    let mut head = [0u8; 8];
    head.copy_from_slice(&key[..8]);
    // Big endian and a right shift, so the slot number rises with the key and
    // a batch sorted by key walks the table forwards.
    let slot = (u64::from_be_bytes(head) >> (64 - BITS)) as usize;
    slot * KEY
}

/// Whether every byte is zero, which is what an empty slot looks like.
fn is_zero(key: &[u8]) -> bool {
    key.iter().all(|byte| *byte == 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: &str) -> UrlKey {
        UrlKey::derive(seed.as_bytes())
    }

    #[test]
    fn a_key_that_was_remembered_is_held_and_one_that_was_not_is_not() {
        let mut seen = Seen::new();
        let a = key("https://example.com/a");
        let b = key("https://example.com/b");
        assert!(!seen.holds(&a));
        seen.remember(&a);
        assert!(seen.holds(&a));
        assert!(!seen.holds(&b));
    }

    #[test]
    fn the_default_key_is_never_held_however_often_it_is_remembered() {
        // An all zero key is what an empty slot looks like, so the one thing
        // this table must never do is answer `true` for it and send a url into
        // a hole. Remembering it has to stay a no-op as well, or it would land
        // in a slot and start swallowing whatever hashes there.
        let mut seen = Seen::new();
        let zero = UrlKey::default();
        assert!(!seen.holds(&zero));
        seen.remember(&zero);
        assert!(!seen.holds(&zero));
        assert!(seen.slots.iter().all(|byte| *byte == 0));
    }

    #[test]
    fn a_slot_holds_the_last_key_written_to_it_and_the_other_one_is_a_miss() {
        // Two keys built to collide, by taking a real key and rewriting the
        // bytes below the ones the index uses. A collision has to read as a
        // miss on the evicted key rather than a hit, because a hit would be
        // the false positive this whole design exists to rule out.
        let first = key("https://example.com/a");
        let mut bytes = *first.as_bytes();
        bytes[9] ^= 0xff;
        let second = UrlKey::from_bytes(bytes);
        assert_eq!(index(first.as_bytes()), index(second.as_bytes()));

        let mut seen = Seen::new();
        seen.remember(&first);
        seen.remember(&second);
        assert!(seen.holds(&second));
        assert!(!seen.holds(&first));
    }

    #[test]
    fn the_slot_rises_with_the_key_so_a_sorted_batch_walks_forwards() {
        // The reason the index comes off the top of the key rather than out of
        // a hash. `admit` sorts by key before it probes, and this is what turns
        // that sort into a left to right walk of the table.
        let mut keys: Vec<UrlKey> = (0..64)
            .map(|n| key(&format!("https://example.com/{n}")))
            .collect();
        keys.sort_unstable();
        let mut last = 0;
        for one in &keys {
            let at = index(one.as_bytes());
            assert!(at >= last, "{at} came after {last}");
            last = at;
        }
    }
}
