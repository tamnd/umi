//! Doc 04.5's chunk tree: a blake3 tree over 16 KiB leaves.
//!
//! The receipt carries `chunk_root` and `chunk_count` alongside the plain
//! digest of the body. The plain digest says the fetcher is claiming these
//! exact bytes. The tree says the coordinator can ask for chunk 47 of a 3 MB
//! document, get 16 KiB and a path of at most seven digests back, and know
//! whether that chunk is what was claimed, without transferring the other
//! 2.9 MB. Doc 04.5 calls that out as the reason audits are affordable at
//! scale, and doc 12.2 uses the same tree to verify three random 1 MiB ranges
//! of a published Parquet file instead of re downloading 128 MB of it.
//!
//! # Why not blake3's own tree
//!
//! blake3 is internally a tree over 1 KiB chunks and its `Hasher` will give you
//! exactly this property through its subtree API. It is not used here for two
//! reasons. The leaf size is fixed at 1 KiB, which would make the path for a
//! 3 MB body twelve digests instead of seven and would put 3000 leaves in
//! memory instead of 190. And the subtree API is easy to hold wrong in a way
//! that still verifies, because a caller who gets the chunk boundaries off by
//! one gets a different root and simply reports corruption forever.
//!
//! So the tree here is explicit: leaves are `blake3(0x00 || chunk)`, interior
//! nodes are `blake3(0x01 || left || right)`, and an odd node at any level is
//! carried up unchanged rather than duplicated. The domain separation byte is
//! what stops a second preimage attack that presents an interior node as a
//! leaf, which is the one classic mistake in this construction.

/// Doc 04.5's leaf size, 16 KiB.
pub const CHUNK_BYTES: usize = 16 * 1024;

/// The domain separator for a leaf.
const LEAF: u8 = 0x00;

/// The domain separator for an interior node.
const NODE: u8 = 0x01;

/// The blake3 tree over one body.
///
/// Holds every level, which for a 3 MB body is 190 leaves and 189 interior
/// nodes, or about 12 KB. That is kept rather than recomputed because the
/// coordinator that builds a tree is about to be asked for a proof from it, and
/// rehashing 3 MB to answer is worse than 12 KB of memory for the length of one
/// verification.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ChunkTree {
    /// Level 0 is the leaves and the last level is the single root.
    levels: Vec<Vec<[u8; 32]>>,
    count: u64,
}

impl ChunkTree {
    /// Build the tree over a body.
    ///
    /// An empty body has one leaf, the hash of no bytes. A zero leaf tree would
    /// have no root, and a receipt for a 204 would then have to carry a
    /// sentinel that every verifier has to special case.
    #[must_use]
    pub fn build(body: &[u8]) -> Self {
        let mut level: Vec<[u8; 32]> = if body.is_empty() {
            vec![leaf(&[])]
        } else {
            body.chunks(CHUNK_BYTES).map(leaf).collect()
        };
        let count = level.len() as u64;
        let mut levels = vec![level.clone()];
        while level.len() > 1 {
            let mut up = Vec::with_capacity(level.len().div_ceil(2));
            let (pairs, rest) = level.as_chunks::<2>();
            for two in pairs {
                up.push(node(&two[0], &two[1]));
            }
            // An odd node rides up unchanged. Duplicating it instead, which is
            // what Bitcoin does, makes a tree of n leaves and a tree of n+1
            // leaves where the last is repeated produce the same root, and that
            // is a real forgery and not a theoretical one.
            if let [odd] = rest {
                up.push(*odd);
            }
            levels.push(up.clone());
            level = up;
        }
        Self { levels, count }
    }

    /// The root that goes in the receipt.
    #[must_use]
    pub fn root(&self) -> [u8; 32] {
        self.levels
            .last()
            .and_then(|top| top.first())
            .copied()
            .expect("build always leaves at least one leaf")
    }

    /// How many 16 KiB leaves there are, which is `chunk_count` in the receipt.
    #[must_use]
    pub const fn count(&self) -> u64 {
        self.count
    }

    /// The sibling path proving leaf `index` belongs to [`root`](Self::root).
    ///
    /// At most `ceil(log2(count))` digests, so seven for a 3 MB body and
    /// thirteen for the 128 MB segment doc 12.2 samples. Returns `None` for an
    /// index past the end, which is a caller asking about a chunk that does not
    /// exist and is a bug rather than a verification failure.
    #[must_use]
    pub fn proof(&self, index: u64) -> Option<Vec<[u8; 32]>> {
        if index >= self.count {
            return None;
        }
        let mut at = usize::try_from(index).ok()?;
        let mut path = Vec::with_capacity(self.levels.len());
        for level in &self.levels[..self.levels.len() - 1] {
            let sibling = at ^ 1;
            // No sibling means this node was the odd one out and rode up
            // unchanged, so there is nothing to combine it with at this level
            // and nothing to record.
            if let Some(digest) = level.get(sibling) {
                path.push(*digest);
            }
            at /= 2;
        }
        Some(path)
    }
}

/// Check one chunk against a root without holding the body.
///
/// This is the verifier's side of doc 04.5. It reconstructs the leaf from the
/// bytes it was given, walks the path, and compares. Everything it needs is an
/// argument, so a coordinator auditing a fetcher and a stranger auditing a
/// published segment run the same function.
///
/// `count` is needed and not decorative: the odd node rule means the shape of
/// the tree depends on how many leaves there are, and a verifier that assumed a
/// perfect tree would reject honest proofs for every body whose chunk count is
/// not a power of two.
#[must_use]
pub fn verify_chunk(
    root: &[u8; 32],
    index: u64,
    count: u64,
    chunk: &[u8],
    path: &[[u8; 32]],
) -> bool {
    if index >= count || chunk.len() > CHUNK_BYTES {
        return false;
    }
    let Ok(mut at) = usize::try_from(index) else {
        return false;
    };
    let Ok(mut width) = usize::try_from(count) else {
        return false;
    };
    let mut have = leaf(chunk);
    let mut step = path.iter();
    while width > 1 {
        // The odd node at the end of a level has no sibling and rides up, so
        // the path carries nothing for it and neither does this loop.
        let odd_one_out = at == width - 1 && width % 2 == 1;
        if !odd_one_out {
            let Some(sibling) = step.next() else {
                return false;
            };
            have = if at % 2 == 0 {
                node(&have, sibling)
            } else {
                node(sibling, &have)
            };
        }
        at /= 2;
        width = width.div_ceil(2);
    }
    // A path with digests left over is a proof for a different tree that
    // happened to agree on the prefix, so it is rejected rather than ignored.
    step.next().is_none() && have == *root
}

fn leaf(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[LEAF]);
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

fn node(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[NODE]);
    hasher.update(left);
    hasher.update(right);
    *hasher.finalize().as_bytes()
}
