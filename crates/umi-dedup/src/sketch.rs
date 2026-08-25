//! Doc 11.8's near duplicate sketch: 64 MinHash values and one simhash.
//!
//! Both are computed in one pass over the shingles from [`crate::shingle`],
//! because the marginal cost of the second sketch once the shingles are hashed
//! is a few percent and the two answer different questions. MinHash estimates
//! Jaccard similarity, which is what doc 04.6 compares two fetches with and
//! what doc 11.8 bands for clustering. Simhash gives a Hamming distance, which
//! is much cheaper for the one question doc 09 asks in bulk: are most of this
//! host's pages the same page, which is what a soft 404 looks like.
//!
//! # The 64 permutations
//!
//! The textbook MinHash construction hashes each shingle 64 times with 64
//! different functions. At about 25000 shingles for a 150 KB document that is
//! 1.6 million xxh3 calls over five word strings, which is tens of milliseconds
//! and roughly forty times doc 11.9's budget for the whole sketch.
//!
//! So the shingle is hashed once and the 64 permutations are affine maps over
//! that one hash: `((h * a_i) ^ b_i) >> 32`. The multiplier is odd, so the map
//! is a bijection on `u64` and the permutation property MinHash needs holds.
//! Taking the high 32 bits rather than the low ones is load bearing: the low
//! bits of a multiply barely mix, and a sketch built from them would collide on
//! documents that differ.
//!
//! The 128 constants are generated at compile time from a fixed splitmix64
//! seed, so they are in the binary rather than in a table someone can typo, and
//! they are identical on every machine, which is what doc 11.1 asks for.

use crate::shingle::Shingles;

/// How many MinHash values are in a sketch. Doc 04.6 and doc 10.5 both fix
/// this at 64, which is 256 bytes on disk.
pub const PERMUTATIONS: usize = 64;

/// How many bytes a sketch takes in doc 10.5's `minhash` column.
pub const SKETCH_BYTES: usize = PERMUTATIONS * 4;

/// Doc 11.8's LSH banding: 8 bands.
pub const BANDS: usize = 8;

/// Doc 11.8's LSH banding: 8 rows per band, which puts the detection threshold
/// at roughly 0.77 Jaccard.
pub const BAND_ROWS: usize = PERMUTATIONS / BANDS;

/// The multiply and xor constants for the 64 permutations.
///
/// A const fn rather than a literal table, so that the seed is visible and the
/// values cannot drift from it. Changing the seed changes every sketch umi has
/// ever written, so it does not change.
const fn permutations() -> [(u64, u64); PERMUTATIONS] {
    let mut out = [(0u64, 0u64); PERMUTATIONS];
    // splitmix64, from a seed chosen once and never again.
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut i = 0;
    while i < PERMUTATIONS {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        let a = z ^ (z >> 31);

        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        let b = z ^ (z >> 31);

        // Odd, so the multiply is a bijection on u64 and the map is a
        // permutation. An even multiplier throws away low bits and would make
        // two different shingles collide by construction.
        out[i] = (a | 1, b);
        i += 1;
    }
    out
}

const PERM: [(u64, u64); PERMUTATIONS] = permutations();

/// The sketch of one document's normalised text.
///
/// Cheap to copy at 264 bytes, which is deliberate: the clustering job in doc
/// 11.8 compares candidates that collided in a band, and a comparison that has
/// to chase a pointer per candidate is a cache miss per candidate.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Sketch {
    /// The 64 minimums, one per permutation.
    pub minhash: [u32; PERMUTATIONS],
    /// The 64 bit simhash over the same shingles.
    pub simhash: u64,
    /// How many shingles went in.
    ///
    /// Not published and not part of doc 10.5's schema. It is here because a
    /// sketch over three shingles is not a meaningful estimate of anything and
    /// [`jaccard`](Self::jaccard) has no way to say so otherwise, and because
    /// the empty document has to be distinguishable from the document whose
    /// every permutation happened to land on `u32::MAX`.
    pub shingles: u32,
}

impl Sketch {
    /// The sketch of a document with no shingles in it.
    ///
    /// Every permutation is `u32::MAX`, which is MinHash's identity for the
    /// empty set: a union with anything leaves the other set's minimum in
    /// place, which is what makes the estimate right.
    pub const EMPTY: Self = Self {
        minhash: [u32::MAX; PERMUTATIONS],
        simhash: 0,
        shingles: 0,
    };

    /// Sketch the normalised plain text from doc 11.3.
    ///
    /// One pass, one xxh3 per shingle, 64 multiply shift and min operations per
    /// shingle for the MinHash and 64 conditional adds for the simhash.
    #[must_use]
    pub fn of(text: &str) -> Self {
        Self::from_hashes(Shingles::new(text))
    }

    /// Sketch a stream of already hashed shingles.
    ///
    /// Public because doc 06's verification path compares two fetchers'
    /// sketches and the test corpus for that is easier to write over hashes
    /// than over prose, and because a future one permutation implementation
    /// wants the same entry point.
    #[must_use]
    pub fn from_hashes(hashes: impl Iterator<Item = u64>) -> Self {
        let mut minhash = [u32::MAX; PERMUTATIONS];
        // i8 counters would overflow on a document with more than 127 shingles,
        // which is most of them, so these are i32 and the sign is read at the
        // end.
        let mut bits = [0i32; 64];
        let mut shingles: u32 = 0;

        for h in hashes {
            shingles = shingles.saturating_add(1);
            for (slot, (a, b)) in minhash.iter_mut().zip(PERM) {
                // The high 32 bits. The low bits of a multiply hardly mix, and
                // a sketch taken from them collides on documents that differ.
                let permuted = ((h.wrapping_mul(a) ^ b) >> 32) as u32;
                if permuted < *slot {
                    *slot = permuted;
                }
            }
            for (bit, count) in bits.iter_mut().enumerate() {
                // Branchless on every compiler worth using: the shift and mask
                // give 0 or 1 and the arithmetic turns that into -1 or +1.
                let set = ((h >> bit) & 1) as i32;
                *count += set * 2 - 1;
            }
        }

        if shingles == 0 {
            return Self::EMPTY;
        }

        let mut simhash = 0u64;
        for (bit, count) in bits.iter().enumerate() {
            // A tie goes to zero. It only happens on an even shingle count with
            // a perfectly balanced bit, and picking one side arbitrarily is
            // fine as long as every machine picks the same side.
            if *count > 0 {
                simhash |= 1 << bit;
            }
        }

        Self {
            minhash,
            simhash,
            shingles,
        }
    }

    /// The estimated Jaccard similarity between two documents, in `0.0..=1.0`.
    ///
    /// The fraction of permutations whose minimums agree. With 64 samples the
    /// standard error is about 0.125, which doc 11.8 says outright is coarse
    /// against doc 04.6's 0.90 threshold, and which is why doc 06 treats this
    /// as one signal of seven rather than as a verdict.
    ///
    /// Two empty sketches return 0.0 rather than 1.0. The Jaccard index of two
    /// empty sets is conventionally 1, but here it would mean "these two pages
    /// with no extractable text are the same page", and they are not, they are
    /// two pages we know nothing about.
    #[must_use]
    pub fn jaccard(&self, other: &Self) -> f32 {
        if self.shingles == 0 || other.shingles == 0 {
            return 0.0;
        }
        let agree = self
            .minhash
            .iter()
            .zip(&other.minhash)
            .filter(|(a, b)| a == b)
            .count();
        agree as f32 / PERMUTATIONS as f32
    }

    /// Hamming distance between two simhashes, in `0..=64`.
    ///
    /// Doc 09's soft 404 detector: a host whose pages are mostly within a few
    /// bits of each other is a host serving one page under many URLs.
    #[must_use]
    pub const fn hamming(&self, other: &Self) -> u32 {
        (self.simhash ^ other.simhash).count_ones()
    }

    /// The 256 bytes doc 10.5's `minhash` column holds, little endian.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; SKETCH_BYTES] {
        let mut out = [0u8; SKETCH_BYTES];
        for (slot, value) in out.chunks_exact_mut(4).zip(self.minhash) {
            slot.copy_from_slice(&value.to_le_bytes());
        }
        out
    }

    /// Read a sketch back out of doc 10.5's columns.
    ///
    /// `shingles` is not stored, so a sketch read back from a published row
    /// carries zero there and [`jaccard`](Self::jaccard) would call it empty.
    /// The count is therefore reconstructed as "not empty unless every
    /// permutation is `u32::MAX`", which is exactly the distinction the field
    /// exists to make and is the only one a reader needs.
    #[must_use]
    pub fn from_bytes(minhash: &[u8; SKETCH_BYTES], simhash: u64) -> Self {
        let mut values = [0u32; PERMUTATIONS];
        for (slot, bytes) in values.iter_mut().zip(minhash.chunks_exact(4)) {
            *slot = u32::from_le_bytes(bytes.try_into().expect("chunks_exact(4) gives 4 bytes"));
        }
        let empty = values.iter().all(|v| *v == u32::MAX);
        Self {
            minhash: values,
            simhash,
            shingles: u32::from(!empty),
        }
    }
}

impl Default for Sketch {
    fn default() -> Self {
        Self::EMPTY
    }
}

/// Doc 11.8's LSH bands: 8 bands of 8 rows, hashed to one `u64` each.
///
/// Two documents are candidates when any band matches. At 8 by 8 the
/// probability of at least one band matching crosses a half at about 0.77
/// Jaccard, which is the detection threshold doc 11.8 quotes.
///
/// The band hash is blake3 over the 32 bytes of the band rather than xxh3,
/// because these keys are the bucket ids for a batch job that runs over the
/// whole corpus, and at that scale a 64 bit key from a non cryptographic hash
/// has enough birthday collisions to matter. Cost is irrelevant here: eight
/// hashes per document against tens of thousands of shingle hashes.
#[must_use]
pub fn bands(sketch: &Sketch) -> [u64; BANDS] {
    let bytes = sketch.to_bytes();
    let mut out = [0u64; BANDS];
    for (slot, band) in out.iter_mut().zip(bytes.chunks_exact(BAND_ROWS * 4)) {
        let digest = blake3::hash(band);
        *slot = u64::from_le_bytes(
            digest.as_bytes()[..8]
                .try_into()
                .expect("a blake3 digest is 32 bytes"),
        );
    }
    out
}
