//! What doc 11.7, doc 11.8 and doc 04.5 actually promise.
//!
//! The MinHash tests use constructed token sets with a known Jaccard rather
//! than prose, because prose gives you a number and no way to say whether the
//! number is right. With constructed sets the true Jaccard is arithmetic and
//! the estimate can be held to the standard error doc 11.8 quotes.

use super::*;

/// A document of `n` distinct five word sentences, so that shingle sets can be
/// intersected by construction.
fn doc(from: usize, to: usize) -> String {
    (from..to)
        .map(|i| format!("alpha{i} beta{i} gamma{i} delta{i} epsilon{i}"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn a_five_word_text_has_one_shingle_and_a_four_word_text_has_none() {
    assert_eq!(shingle::shingles("one two three four five").count(), 1);
    assert_eq!(shingle::shingles("one two three four").count(), 0);
    assert_eq!(shingle::shingles("").count(), 0);
    // Six tokens give two overlapping windows, which is what makes the sketch
    // sensitive to word order rather than to the bag of words.
    assert_eq!(shingle::shingles("one two three four five six").count(), 2);
}

#[test]
fn shingles_slide_by_one_token() {
    let text = "a b c d e f";
    let hashes: Vec<u64> = shingle::shingles(text).collect();
    assert_eq!(hashes.len(), 2);
    assert_eq!(hashes[0], twox_hash::XxHash3_64::oneshot(b"a b c d e"));
    assert_eq!(hashes[1], twox_hash::XxHash3_64::oneshot(b"b c d e f"));
}

#[test]
fn leading_and_repeated_whitespace_does_not_make_extra_shingles() {
    // Doc 11.3 normalises whitespace before this crate ever sees the text, but
    // a fetcher that hands over something less tidy should not produce a
    // different shingle count, because it would then produce a different
    // sketch for the same page.
    assert_eq!(shingle::shingles("  a b   c d e  ").count(), 1);
}

#[test]
fn the_same_text_sketches_the_same_way_every_time() {
    // Doc 11.1's determinism rule, at the smallest scale it can be tested.
    let text = doc(0, 200);
    let a = Sketch::of(&text);
    let b = Sketch::of(&text);
    assert_eq!(a, b);
    assert_eq!(a.jaccard(&b), 1.0);
    assert_eq!(a.hamming(&b), 0);
}

#[test]
fn identical_documents_estimate_one_and_disjoint_ones_estimate_zero() {
    let a = Sketch::of(&doc(0, 400));
    let same = Sketch::of(&doc(0, 400));
    let apart = Sketch::of(&doc(1000, 1400));
    assert_eq!(a.jaccard(&same), 1.0);
    // Not asserted as exactly zero: 64 permutations over two disjoint sets can
    // agree by chance. It has to be near zero.
    assert!(a.jaccard(&apart) < 0.05, "{}", a.jaccard(&apart));
}

#[test]
fn the_jaccard_estimate_is_inside_the_standard_error_doc_11_8_quotes() {
    // Two documents sharing half their sentences. The shingle sets overlap on
    // the shared run and each has its own tail, so the true Jaccard is close
    // to 1/3 and not to 1/2: |A n B| / |A u B| = 200 / 600.
    let a = Sketch::of(&doc(0, 400));
    let b = Sketch::of(&doc(200, 600));
    let estimate = a.jaccard(&b);
    // Doc 11.8 puts the standard error at about 0.125 for 64 permutations, so
    // three sigma is 0.375 and any assertion tighter than that is a test that
    // fails on a different corpus. This one is deliberately loose and it still
    // catches every way of getting the construction wrong, because a broken
    // permutation gives 0.0 or 1.0.
    assert!(
        (estimate - 0.333).abs() < 0.2,
        "estimated {estimate}, true is about 0.333"
    );
}

#[test]
fn jaccard_is_symmetric_and_the_empty_sketch_is_similar_to_nothing() {
    let a = Sketch::of(&doc(0, 50));
    let b = Sketch::of(&doc(25, 75));
    assert_eq!(a.jaccard(&b), b.jaccard(&a));

    let empty = Sketch::of("four words only here");
    assert_eq!(empty, Sketch::EMPTY);
    assert_eq!(empty.shingles, 0);
    // Not 1.0. Two pages we know nothing about are not the same page.
    assert_eq!(empty.jaccard(&Sketch::EMPTY), 0.0);
    assert_eq!(empty.jaccard(&a), 0.0);
    assert_eq!(a.jaccard(&empty), 0.0);
}

#[test]
fn simhash_moves_a_little_for_a_small_edit_and_a_lot_for_a_new_document() {
    let a = Sketch::of(&doc(0, 400));
    let nudged = Sketch::of(&format!("{} one extra sentence here", doc(0, 400)));
    let other = Sketch::of(&doc(1000, 1400));
    assert!(a.hamming(&nudged) < a.hamming(&other));
    // Doc 09 wants "are most of this host's pages the same page" to be a cheap
    // question, and that only works if a near copy stays close. Four extra
    // shingles against two thousand move a counter by at most four, so only
    // the bits that were nearly balanced can flip. Two unrelated documents sit
    // around 32 bits apart, which is what this is really separating from.
    assert!(a.hamming(&nudged) < 10, "{}", a.hamming(&nudged));
}

#[test]
fn every_permutation_is_a_different_map() {
    // The failure this catches is a constant table generated with an even
    // multiplier or with a seed that repeats, which gives 64 correlated
    // permutations and a sketch with the resolution of about one.
    let sketch = Sketch::of(&doc(0, 400));
    let mut seen: Vec<u32> = sketch.minhash.to_vec();
    seen.sort_unstable();
    seen.dedup();
    assert!(seen.len() > 50, "only {} distinct minimums", seen.len());
}

#[test]
fn a_sketch_survives_a_round_trip_through_doc_10_5s_columns() {
    let sketch = Sketch::of(&doc(0, 300));
    let bytes = sketch.to_bytes();
    assert_eq!(bytes.len(), SKETCH_BYTES);
    assert_eq!(bytes.len(), 256);
    let back = Sketch::from_bytes(&bytes, sketch.simhash);
    assert_eq!(back.minhash, sketch.minhash);
    assert_eq!(back.simhash, sketch.simhash);
    assert_eq!(back.jaccard(&sketch), 1.0);

    // The count is not a stored column, so a row read back says only whether
    // there was anything to sketch.
    let empty = Sketch::from_bytes(&Sketch::EMPTY.to_bytes(), 0);
    assert_eq!(empty.shingles, 0);
    assert_eq!(empty.jaccard(&empty), 0.0);
}

#[test]
fn bands_agree_when_the_sketches_do_and_split_when_they_do_not() {
    let a = Sketch::of(&doc(0, 400));
    let same = Sketch::of(&doc(0, 400));
    let apart = Sketch::of(&doc(1000, 1400));
    assert_eq!(bands(&a), bands(&same));
    let hits = bands(&a)
        .iter()
        .zip(bands(&apart))
        .filter(|(x, y)| **x == *y)
        .count();
    assert_eq!(hits, 0);
    assert_eq!(BANDS * BAND_ROWS, PERMUTATIONS);
}

#[test]
fn near_duplicates_collide_in_at_least_one_band() {
    // Doc 11.8 puts the 8 by 8 detection threshold at about 0.77, so a pair
    // well above it has to be a candidate or the clustering job never sees it.
    let a = Sketch::of(&doc(0, 1000));
    let b = Sketch::of(&doc(0, 1000));
    let hits = bands(&a)
        .iter()
        .zip(bands(&b))
        .filter(|(x, y)| **x == *y)
        .count();
    assert!(hits > 0);
}

#[test]
fn the_exact_duplicate_key_is_over_the_text_and_the_ledger_gets_its_first_8_bytes() {
    let text = "The quick brown fox jumps over the lazy dog.";
    let digest = text_digest(text);
    assert_eq!(digest, *blake3::hash(text.as_bytes()).as_bytes());
    assert_eq!(content_hash(text).as_slice(), &digest[..8]);

    let content = Content::of(text);
    assert_eq!(content.digest, digest);
    assert_eq!(content.content_hash(), content_hash(text));
    assert_eq!(content.text_bytes, text.len() as u32);
    assert_eq!(content.sketch, Sketch::of(text));
}

#[test]
fn length_buckets_are_coarse_enough_that_a_paragraph_does_not_move_them() {
    assert_eq!(len_bucket(0), 0);
    assert_eq!(len_bucket(1), 1);
    // Two lengths inside one power of two land together.
    assert_eq!(len_bucket(4200), len_bucket(4500));
    // Two that straddle a boundary do not, and this is why doc 04.6 compares
    // within one bucket rather than for equality: 4000 and 4500 bytes of text
    // are the same page and 4096 sits between them.
    assert_eq!(len_bucket(4500) - len_bucket(4000), 1);
    // A page that shrank by two orders of magnitude is a soft error page
    // wearing the same URL, and that has to be well outside the tolerance.
    assert!(len_bucket(40_000) - len_bucket(400) > 1);
}

#[test]
fn the_link_set_digest_ignores_order_and_repeats_but_not_boundaries() {
    let one = link_set_digest(&["https://a.example/", "https://b.example/"]);
    let other = link_set_digest(&["https://b.example/", "https://a.example/"]);
    assert_eq!(one, other);
    assert_eq!(
        one,
        link_set_digest(&[
            "https://b.example/",
            "https://a.example/",
            "https://a.example/"
        ])
    );
    // Without the separator these two would hash the same, and a hostile
    // fetcher could then swap one link for two that concatenate to it.
    assert_ne!(link_set_digest(&["ab", "c"]), link_set_digest(&["a", "bc"]));
    assert_ne!(one, link_set_digest(&["https://a.example/"]));
}

#[test]
fn a_chunk_tree_over_an_empty_body_still_has_a_root() {
    let tree = ChunkTree::build(&[]);
    assert_eq!(tree.count(), 1);
    let path = tree.proof(0).expect("leaf 0 is there");
    assert!(path.is_empty());
    assert!(verify_chunk(&tree.root(), 0, 1, &[], &path));
}

#[test]
fn every_chunk_of_a_body_verifies_against_its_root() {
    // Deliberately not a power of two leaves, so the odd node rule is on the
    // path for most indexes. 100 KiB and change is seven leaves.
    let body: Vec<u8> = (0..(CHUNK_BYTES * 6 + 913))
        .map(|i| (i as u32).wrapping_mul(2_654_435_761) as u8)
        .collect();
    let tree = ChunkTree::build(&body);
    assert_eq!(tree.count(), 7);
    for (index, chunk) in body.chunks(CHUNK_BYTES).enumerate() {
        let index = index as u64;
        let path = tree.proof(index).expect("in range");
        assert!(
            verify_chunk(&tree.root(), index, tree.count(), chunk, &path),
            "chunk {index} did not verify"
        );
        // Doc 04.5 wants the audit cheap: seven leaves is at most three
        // digests, so a 3 MB body is at most seven and a 128 MB segment at
        // most thirteen.
        assert!(path.len() <= 3, "path was {} long", path.len());
    }
}

#[test]
fn every_leaf_count_from_one_to_thirty_three_verifies() {
    // The odd node rule is the part of this that is easy to get subtly wrong,
    // and it only shows up at particular shapes. So every shape gets tested
    // rather than a few chosen ones.
    for leaves in 1..=33usize {
        let body: Vec<u8> = (0..(CHUNK_BYTES * (leaves - 1) + 1))
            .map(|i| (i % 251) as u8)
            .collect();
        let tree = ChunkTree::build(&body);
        assert_eq!(tree.count(), leaves as u64, "for {leaves} leaves");
        for (index, chunk) in body.chunks(CHUNK_BYTES).enumerate() {
            let path = tree.proof(index as u64).expect("in range");
            assert!(
                verify_chunk(&tree.root(), index as u64, tree.count(), chunk, &path),
                "{leaves} leaves, chunk {index}"
            );
        }
    }
}

#[test]
fn a_tampered_chunk_a_wrong_index_and_a_padded_path_are_all_rejected() {
    let body: Vec<u8> = (0..(CHUNK_BYTES * 5)).map(|i| (i % 251) as u8).collect();
    let tree = ChunkTree::build(&body);
    let root = tree.root();
    let path = tree.proof(2).expect("in range");
    let chunk = &body[CHUNK_BYTES * 2..CHUNK_BYTES * 3];
    assert!(verify_chunk(&root, 2, 5, chunk, &path));

    let mut tampered = chunk.to_vec();
    tampered[0] ^= 1;
    assert!(!verify_chunk(&root, 2, 5, &tampered, &path));

    // The right chunk quoted at the wrong index.
    assert!(!verify_chunk(&root, 3, 5, chunk, &path));
    // An index past the end.
    assert!(!verify_chunk(&root, 5, 5, chunk, &path));
    assert!(tree.proof(5).is_none());
    // A path that verifies but has a digest left over is a proof for a
    // different tree, so it does not count.
    let mut long = path.clone();
    long.push([0u8; 32]);
    assert!(!verify_chunk(&root, 2, 5, chunk, &long));
    // A path that ran out.
    assert!(!verify_chunk(&root, 2, 5, chunk, &path[..1]));
}

#[test]
fn an_interior_node_cannot_be_presented_as_a_leaf() {
    // The domain separation bytes are what stop this. Without them, a body
    // whose single leaf is exactly the 64 bytes of a concatenated node pair
    // would produce a root that also verifies as an interior node of a taller
    // tree, and a fetcher could claim a small body for a large one.
    let one = ChunkTree::build(&[7u8; 100]);
    let two = ChunkTree::build(&[7u8; CHUNK_BYTES + 100]);
    assert_ne!(one.root(), two.root());
    // The root of a two leaf tree is not the leaf hash of its own 64 byte
    // preimage, which is the concrete form of the attack.
    let mut concat = Vec::new();
    concat.extend_from_slice(&ChunkTree::build(&[7u8; CHUNK_BYTES]).root());
    concat.extend_from_slice(&ChunkTree::build(&[7u8; 100]).root());
    assert_ne!(two.root(), ChunkTree::build(&concat).root());
}

#[test]
fn the_last_chunk_is_short_and_is_not_padded() {
    // A verifier that padded the tail to 16 KiB would accept a body with any
    // number of trailing zeros, which is a body it was not given.
    let body = vec![3u8; CHUNK_BYTES + 10];
    let tree = ChunkTree::build(&body);
    let path = tree.proof(1).expect("in range");
    assert!(verify_chunk(&tree.root(), 1, 2, &[3u8; 10], &path));
    let mut padded = vec![3u8; 10];
    padded.extend_from_slice(&[0u8; 6]);
    assert!(!verify_chunk(&tree.root(), 1, 2, &padded, &path));
}
