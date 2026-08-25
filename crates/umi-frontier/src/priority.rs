//! The milestone 1 half of doc 09.2, which is depth and discovery order.
//!
//! Doc 09.2 scores a URL on five terms. Four of them need something we do not
//! have yet: host quality needs the Common Crawl centrality ranks and our own
//! per host observations, link evidence needs a crawl that has already run,
//! freshness urgency needs the change rate estimator in doc 09.4, and the
//! scope bonus needs the focused crawl configuration in doc 13. So milestone 1
//! ships the one term that is available at admission time and leaves the
//! others at zero, and the shape of the sum is not built yet because a sum
//! with one term in it is not a sum.
//!
//! Discovery order is the second half of the score and it does not appear in
//! this file, because it is already free. A URL is admitted with its due time
//! set to the moment it was discovered, and `State::lease` breaks ties by due
//! time ascending. Two URLs at the same depth are therefore offered oldest
//! first without anything here computing it, and adding a discovery term to
//! the score would either duplicate that or quietly disagree with it.
//!
//! Everything here is integer. Doc 09.2 writes the weights as fractions and
//! doc 11.1 rules floats out of anything that decides an outcome, and a
//! scheduling decision is an outcome: two coordinators replaying one crawl
//! have to pick the same URL.

use umi_state::Priority;

/// The link distance at which a URL stops being worth crawling, from doc 09.7.
///
/// It is the first defence against a trap that generates infinite paths, and
/// it is a cap rather than a penalty because a penalty still leaves the URL in
/// the frontier taking up room.
pub const MAX_DEPTH: u8 = 30;

/// The highest score the depth term may reach.
///
/// The band above this is reserved. Doc 09.5 gives feed entries
/// [`Priority::MAX`] so they beat everything in the general crawl, and doc
/// 09.6 puts the whole realtime path above the batch scheduler, so leaving
/// room now means the realtime work in milestone 2 does not have to rescale
/// every score already on disk.
pub const MAX_DEPTH_SCORE: u16 = 60_000;

/// The score for a URL at this link distance from the nearest seed.
///
/// Doc 09.2's `depth_decay` is `1 / (1 + depth)`, which halves at the first
/// hop and then falls away slowly, so a seed heavily outranks its links and
/// depth 20 and depth 25 are nearly the same thing. That is the intended
/// shape: the cheap, effective defence is the cap at [`MAX_DEPTH`], and the
/// decay is there to make the crawl prefer breadth near the seeds.
///
/// Nothing lands on zero, so a deep URL is still crawled when the frontier is
/// quiet rather than being pinned under [`Priority::MIN`] forever.
#[must_use]
pub fn depth_score(depth: u8) -> Priority {
    let decay = u32::from(MAX_DEPTH_SCORE) / (1 + u32::from(depth));
    Priority::from_raw(u16::try_from(decay).unwrap_or(MAX_DEPTH_SCORE))
}

/// Whether a URL at this depth is worth admitting at all.
#[must_use]
pub const fn within_depth(depth: u8, max_depth: u8) -> bool {
    depth <= max_depth
}

#[cfg(test)]
mod tests {
    use umi_state::Priority;

    use super::{MAX_DEPTH, MAX_DEPTH_SCORE, depth_score, within_depth};

    #[test]
    fn a_seed_outranks_its_links_and_the_decay_flattens_out() {
        assert_eq!(depth_score(0).raw(), MAX_DEPTH_SCORE);
        assert_eq!(depth_score(1).raw(), MAX_DEPTH_SCORE / 2);
        assert_eq!(depth_score(2).raw(), MAX_DEPTH_SCORE / 3);
        // The difference between deep and deeper is small on purpose. The cap
        // is what stops a trap, not the score.
        assert!(depth_score(20).raw() - depth_score(25).raw() < 600);
    }

    #[test]
    fn the_score_falls_with_depth_and_never_reaches_the_floor() {
        let mut previous = u16::MAX;
        for depth in 0..=u8::MAX {
            let score = depth_score(depth).raw();
            assert!(score <= previous, "depth {depth} rose");
            assert!(score > Priority::MIN.raw(), "depth {depth} hit the floor");
            previous = score;
        }
    }

    #[test]
    fn the_band_above_the_depth_term_is_left_for_the_realtime_path() {
        for depth in 0..=u8::MAX {
            assert!(depth_score(depth).raw() <= MAX_DEPTH_SCORE);
            assert!(depth_score(depth) < Priority::MAX);
        }
    }

    #[test]
    fn the_depth_cap_is_inclusive() {
        assert!(within_depth(MAX_DEPTH, MAX_DEPTH));
        assert!(!within_depth(MAX_DEPTH + 1, MAX_DEPTH));
        assert!(within_depth(0, 0));
    }
}
