//! AIPREF `Content-Usage`, both forms.
//!
//! The two drafts are `draft-ietf-aipref-vocab`, which defines the categories
//! and the reconciliation rule, and `draft-ietf-aipref-attach`, which defines
//! the robots.txt line and the response header. Every example in these tests
//! that looks like it was copied out of a draft was.

use super::*;

/// The `Content-Usage` value the vocabulary's own examples use.
const BOTH: &str = "train-ai=n, search=y";

// ---------------------------------------------------------------------------
// The value grammar, which is the same in both forms
// ---------------------------------------------------------------------------

#[test]
fn both_categories_parse() {
    let usage = Usage::parse(BOTH);
    assert_eq!(usage.train_ai(), Some(Preference::Disallowed));
    assert_eq!(usage.search(), Some(Preference::Allowed));
    assert!(usage.unreadable().is_empty());
}

#[test]
fn one_category_leaves_the_other_absent() {
    // Absent is a third answer and not a synonym for allowed. A reader
    // filtering on `search` needs to be able to tell a site that said yes from
    // one that said nothing.
    let usage = Usage::parse("train-ai=n");
    assert_eq!(usage.train_ai(), Some(Preference::Disallowed));
    assert_eq!(usage.search(), None);
}

#[test]
fn whitespace_and_case_do_not_change_the_answer() {
    let usage = Usage::parse("  TRAIN-AI = N ,search=Y  ");
    assert_eq!(usage.train_ai(), Some(Preference::Disallowed));
    assert_eq!(usage.search(), Some(Preference::Allowed));
    assert_eq!(usage.render().as_deref(), Some(BOTH));
}

#[test]
fn nothing_stated_is_no_column_value() {
    assert!(Usage::parse("").is_empty());
    assert_eq!(Usage::parse("   ,  , ").render(), None);
}

#[test]
fn the_rendered_order_is_fixed_rather_than_the_order_it_was_written_in() {
    // So that two hosts saying the same thing produce the same string, and one
    // `LIKE` predicate finds both.
    assert_eq!(
        Usage::parse("search=y, train-ai=n").render().as_deref(),
        Usage::parse(BOTH).render().as_deref()
    );
}

// ---------------------------------------------------------------------------
// What the parser cannot read, which is the point of the drafts being drafts
// ---------------------------------------------------------------------------

#[test]
fn an_unknown_category_is_kept_verbatim() {
    // These are drafts and the vocabulary will grow. Dropping a directive we
    // have not heard of would lose the only copy a reader gets on the row, and
    // guessing at its meaning would be worse than losing it.
    let usage = Usage::parse("train-ai=n, ai-input=n");
    assert_eq!(usage.train_ai(), Some(Preference::Disallowed));
    assert_eq!(usage.unreadable(), ["ai-input=n"]);
    assert_eq!(usage.render().as_deref(), Some("train-ai=n, ai-input=n"));
}

#[test]
fn a_known_category_with_a_value_outside_the_vocabulary_is_kept_verbatim() {
    // `train-ai=maybe` is not `y` and it is not `n`, and reading it as either
    // would put a preference in the corpus that nobody expressed.
    let usage = Usage::parse("train-ai=maybe");
    assert_eq!(usage.train_ai(), None);
    assert_eq!(usage.unreadable(), ["train-ai=maybe"]);
}

#[test]
fn an_item_that_is_not_a_pair_at_all_is_kept_verbatim() {
    let usage = Usage::parse("please do not train on this");
    assert_eq!(usage.unreadable(), ["please do not train on this"]);
}

#[test]
fn the_structured_field_boolean_spelling_is_kept_rather_than_guessed_at() {
    // RFC 9651 spells a true boolean `?1`. The vocabulary says the values are
    // `y` and `n`, so a header written the other way is something we record
    // and do not interpret, at least until the drafts settle.
    let usage = Usage::parse("train-ai=?0");
    assert_eq!(usage.train_ai(), None);
    assert_eq!(usage.unreadable(), ["train-ai=?0"]);
}

#[test]
fn unreadable_items_are_capped() {
    // robots.txt is capped at 500 KiB and all of it could be this, and this
    // value is written on every row of the host. The full file is published
    // verbatim in `open-index/umi-robots`, so the cap costs nothing that
    // cannot be recovered.
    let mut usage = Usage::default();
    for n in 0..MAX_UNREADABLE + 4 {
        usage.merge(&Usage::parse(&format!("odd-{n}=n")));
    }
    assert_eq!(usage.unreadable().len(), MAX_UNREADABLE);
    assert_eq!(usage.unreadable()[0], "odd-0=n");
}

#[test]
fn the_same_unreadable_item_from_two_sources_is_kept_once() {
    let mut usage = Usage::parse("ai-input=n");
    usage.merge(&Usage::parse("ai-input=n"));
    assert_eq!(usage.unreadable(), ["ai-input=n"]);
}

// ---------------------------------------------------------------------------
// Reconciliation, vocab draft section 5.1
// ---------------------------------------------------------------------------

#[test]
fn the_most_restrictive_answer_wins() {
    // The one place this differs from the rest of the file. `Allow` beats
    // `Disallow` on a tie in RFC 9309, and here it is the other way round, so
    // a header saying yes cannot undo a robots.txt saying no.
    let cases = [
        ("train-ai=n", "train-ai=y", Some(Preference::Disallowed)),
        ("train-ai=y", "train-ai=n", Some(Preference::Disallowed)),
        ("train-ai=y", "train-ai=y", Some(Preference::Allowed)),
        ("train-ai=n", "train-ai=n", Some(Preference::Disallowed)),
        ("train-ai=y", "", Some(Preference::Allowed)),
        ("", "train-ai=n", Some(Preference::Disallowed)),
        ("", "", None),
    ];
    for (first, second, want) in cases {
        let mut usage = Usage::parse(first);
        usage.merge(&Usage::parse(second));
        assert_eq!(usage.train_ai(), want, "{first:?} then {second:?}");
    }
}

#[test]
fn two_lines_in_one_file_reconcile_the_same_way() {
    let body = "\
Content-Usage: train-ai=y
Content-Usage: train-ai=n
";
    let robots = Robots::parse_str(body);
    assert_eq!(
        robots.usage_for("/").train_ai(),
        Some(Preference::Disallowed)
    );
}

// ---------------------------------------------------------------------------
// The robots.txt form, attach draft section 3
// ---------------------------------------------------------------------------

#[test]
fn a_line_with_no_pattern_applies_to_the_whole_site() {
    let robots = Robots::parse_str("Content-Usage: train-ai=n\n");
    assert_eq!(
        robots.usage_for("/anything").render().as_deref(),
        Some("train-ai=n")
    );
    assert_eq!(robots.usage().render().as_deref(), Some("train-ai=n"));
}

#[test]
fn a_line_with_a_pattern_applies_where_the_pattern_matches() {
    // The draft's own example.
    let body = "\
Content-Usage: train-ai=n
Content-Usage: /ai-ok/ train-ai=y
";
    let robots = Robots::parse_str(body);
    // Outside the pattern only the site wide line counts.
    assert_eq!(
        robots.usage_for("/blog/post").render().as_deref(),
        Some("train-ai=n")
    );
    // Inside it both count, and the restrictive one still wins. The site said
    // no everywhere and yes here, and the reconciliation rule is not about
    // which line is more specific.
    assert_eq!(
        robots.usage_for("/ai-ok/page").render().as_deref(),
        Some("train-ai=n")
    );
}

#[test]
fn a_pattern_uses_the_same_wildcards_as_the_rest_of_the_file() {
    let robots = Robots::parse_str("Content-Usage: /*.pdf$ train-ai=n\n");
    assert_eq!(
        robots.usage_for("/docs/paper.pdf").train_ai(),
        Some(Preference::Disallowed)
    );
    assert_eq!(robots.usage_for("/docs/paper.html").train_ai(), None);
}

#[test]
fn a_non_ascii_pattern_matches_the_encoded_path() {
    // Same rule as RFC 9309 2.2.2, and the same escaping, because a fetcher
    // requests the encoded form and that is the string this is matched
    // against.
    let robots = Robots::parse_str("Content-Usage: /caf\u{e9}/ train-ai=n\n");
    assert_eq!(
        robots.usage_for("/caf%C3%A9/menu").train_ai(),
        Some(Preference::Disallowed)
    );
}

#[test]
fn the_host_wide_value_leaves_out_the_pattern_scoped_lines() {
    // The host record says what the site said about itself. A line about one
    // directory is not that, and copying it onto the host would claim the site
    // meant it everywhere.
    let body = "\
Content-Usage: search=y
Content-Usage: /private/ train-ai=n
";
    let robots = Robots::parse_str(body);
    assert_eq!(robots.usage().render().as_deref(), Some("search=y"));
    assert_eq!(
        robots.usage_for("/private/x").render().as_deref(),
        Some("train-ai=n, search=y")
    );
}

#[test]
fn a_line_that_is_only_a_pattern_is_recorded_rather_than_read_as_site_wide() {
    let robots = Robots::parse_str("Content-Usage: /private/\n");
    assert_eq!(robots.usage_for("/private/x").unreadable(), ["/private/"]);
    assert!(robots.usage_for("/private/x").train_ai().is_none());
}

#[test]
fn usage_for_url_takes_the_path_out_of_the_url() {
    let robots = Robots::parse_str("Content-Usage: /ai-ok/ train-ai=y\n");
    assert_eq!(
        robots
            .usage_for_url("https://example.com/ai-ok/page?q=1")
            .train_ai(),
        Some(Preference::Allowed)
    );
    assert_eq!(
        robots.usage_for_url("https://example.com/other").train_ai(),
        None
    );
}

#[test]
fn a_file_with_no_content_usage_produces_no_column_value() {
    let robots = Robots::parse_str("User-agent: *\nDisallow: /x\n");
    assert_eq!(robots.usage_for("/").render(), None);
    assert_eq!(robots.usage().render(), None);
}

#[test]
fn the_preference_does_not_change_a_crawl_decision() {
    // Doc 07.5, and the reason this file exists at all. Recording it must not
    // turn into acting on it by accident.
    let robots = Robots::parse_str("Content-Usage: train-ai=n, search=n\n");
    assert!(robots.allows("/anything").is_allowed());
    assert!(!robots.is_blanket_disallow());
}
