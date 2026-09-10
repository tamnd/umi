//! A bounded memory of URLs already in the seen table.
//!
//! The crate doc says where this backend stops, and admission is the wall it
//! stops at. A candidate costs one insert into a table keyed on a hash, which
//! is a fresh root to leaf descent into a b-tree that is gigabytes wide, and
//! the great majority of those descents find the row already there and change
//! nothing. Measured on server3 over six minutes: 2,658,266 links offered,
//! 910,224 admitted, 138 seconds of write. The 1.7 million that were already
//! known paid full price to be told so.
//!
//! Most of them were known recently. A crawl walks a site, and the pages of a
//! site link to the same navigation, the same footer and each other, so the
//! duplicates of a batch are overwhelmingly urls the last few thousand batches
//! also carried. Remembering a few million of them turns the common case from
//! a disk seek into a hash lookup.
//!
//! # Why this is safe
//!
//! Forgetting is free and remembering wrongly is not, so the whole design is
//! about which way it fails.
//!
//! A key that is not here when it should be costs one insert that finds the row
//! already present, which is exactly what the code did before. Nothing is lost
//! and nothing is written twice, so eviction can be as crude as it likes.
//!
//! A key that is here when it should not be is a url dropped for good: the
//! caller believes it is already known and never writes it down. So a key only
//! arrives here after the transaction that put it in the table has committed.
//! A batch that fails or rolls back adds nothing, and the keys it was going to
//! add are simply offered again by whatever retries.

use std::collections::HashSet;
use std::mem;

use umi_types::UrlKey;

/// How many keys a generation holds before it is retired.
///
/// Two generations are live, so the real ceiling is twice this. A `UrlKey` is
/// ten bytes and a hash set spends about twice that on each one, so four
/// million a generation is a few hundred megabytes at the top, against a
/// backend the crate doc already bounds at a hundred million urls. That is the
/// right shape: enough to hold the working set of a broad crawl for minutes,
/// small enough that an operator who never thinks about it does not get a
/// surprise.
const GENERATION: usize = 4_000_000;

/// Keys known to be in the `seen` table, most recent first.
///
/// Two sets and not one, because eviction has to be cheap and it has to keep
/// the recent keys. A set that filled and cleared would throw away everything
/// including the batch that just arrived, and a set with an eviction order
/// would spend more on bookkeeping per key than the lookup saves. Two
/// generations gets the useful half of both: the young set takes every write,
/// and when it fills it becomes the old set and the previous old set is
/// dropped. A key is remembered for at least one generation of traffic after it
/// was last written and at most two, which is all the precision this needs.
#[derive(Debug)]
pub(crate) struct Seen {
    young: HashSet<UrlKey>,
    old: HashSet<UrlKey>,
    generation: usize,
}

impl Default for Seen {
    fn default() -> Self {
        Self::with_generation(GENERATION)
    }
}

impl Seen {
    /// One of these holding `generation` keys a generation.
    ///
    /// A parameter so the tests can fill a generation without allocating eight
    /// million hashes to watch one get retired. Nothing outside them passes
    /// anything but [`GENERATION`].
    pub(crate) fn with_generation(generation: usize) -> Self {
        Self {
            young: HashSet::new(),
            old: HashSet::new(),
            generation: generation.max(1),
        }
    }

    /// Whether this key is known to be in the table already.
    ///
    /// False does not mean it is not. It means this does not know, and the
    /// caller has to ask the table the way it always did.
    pub(crate) fn holds(&self, key: UrlKey) -> bool {
        self.young.contains(&key) || self.old.contains(&key)
    }

    /// Remember a key whose row is committed.
    ///
    /// The caller has to have committed. See the module doc for what happens if
    /// it has not.
    pub(crate) fn keep(&mut self, key: UrlKey) {
        if self.old.contains(&key) {
            // Already remembered, and promoting it would cost a write on the
            // hot path to buy one more generation of memory for a key that is
            // about to be offered again anyway.
            return;
        }
        self.young.insert(key);
        if self.young.len() >= self.generation {
            self.old = mem::take(&mut self.young);
        }
    }

    /// How many keys are held, for the tests and for a dashboard.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.young.len() + self.old.len()
    }
}

#[cfg(test)]
mod tests {
    use super::Seen;
    use umi_types::UrlKey;

    /// Small enough that a test can fill three of them.
    const SPAN: usize = 8;

    fn key(n: u64) -> UrlKey {
        UrlKey::derive(&n.to_le_bytes())
    }

    #[test]
    fn a_key_that_was_kept_is_held_and_one_that_was_not_is_not() {
        let mut seen = Seen::default();
        assert!(!seen.holds(key(1)));
        seen.keep(key(1));
        assert!(seen.holds(key(1)));
        assert!(!seen.holds(key(2)));
    }

    #[test]
    fn keeping_the_same_key_twice_costs_nothing() {
        let mut seen = Seen::default();
        seen.keep(key(1));
        seen.keep(key(1));
        assert_eq!(seen.len(), 1);
    }

    #[test]
    fn a_full_generation_retires_and_the_one_before_it_is_forgotten() {
        // A generation of distinct keys retires the moment it fills, so the
        // first key is in the old set from then on. Half a generation of
        // traffic after that and it is still there, which is the promise: a key
        // survives at least the generation that follows the one it was written
        // in. A second full generation is what finally drops it.
        let mut seen = Seen::with_generation(SPAN);
        let span = SPAN as u64;
        for n in 0..span {
            seen.keep(key(n));
        }
        assert!(
            seen.holds(key(0)),
            "a full generation has only just retired"
        );

        for n in span..(span + span / 2) {
            seen.keep(key(n));
        }
        assert!(
            seen.holds(key(0)),
            "half a generation later it is still held"
        );
        assert!(seen.holds(key(span)));

        for n in (span + span / 2)..(span * 2) {
            seen.keep(key(n));
        }
        assert!(!seen.holds(key(0)), "a second full generation drops it");
        assert!(seen.holds(key(span)), "and keeps the one that replaced it");
        assert!(
            seen.len() <= SPAN * 2,
            "two generations is the ceiling and this held {}",
            seen.len()
        );
    }
}
