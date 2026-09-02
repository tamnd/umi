//! The card is the only thing most readers of the corpus will ever read, so
//! the tests here are about it staying true rather than about it staying the
//! same. Nothing checks the prose word for word. What is checked is that every
//! column has a line, that the claims doc 12.9 requires are present, and that
//! the two sentences a reader could be harmed by getting wrong are there.

use crate::repo::{Corpus, Family};

const FAMILIES: [Family; 3] = [Family::Pages, Family::Receipts, Family::Robots];

fn general(family: Family) -> String {
    super::card(&Corpus::new("open-index"), family, "umi-extract/0.0.1")
}

#[test]
fn every_column_in_every_schema_has_a_line_about_it() {
    // The reason the table is generated rather than written. A column added to
    // doc 10.5 without a word about what it holds ships a card that describes
    // a schema the files do not have, and the reader who notices is the one
    // who already built something on the old shape. Failing here is cheap and
    // failing there is not.
    for family in FAMILIES {
        let described = super::described(family);
        for field in family.stream().arrow().fields() {
            let note = described
                .iter()
                .find(|(col, _)| col == field.name())
                .map(|(_, note)| *note);
            assert!(
                note.is_some_and(|note| !note.is_empty()),
                "{family:?} column {} has no description",
                field.name(),
            );
        }
        // And the other direction, because a description for a column that was
        // removed is a line in the table that never renders and a claim nobody
        // can check.
        for (col, _) in described {
            assert!(
                family
                    .stream()
                    .arrow()
                    .fields()
                    .iter()
                    .any(|f| f.name() == col),
                "{family:?} describes {col}, which is not in the schema",
            );
        }
    }
}

#[test]
fn the_card_says_what_doc_12_9_says_it_says() {
    for family in FAMILIES {
        let card = general(family);
        assert!(card.starts_with("---\n"), "{family:?} has no front matter");
        assert!(
            !card.contains("size_categories"),
            "{family:?} guesses at a size on the day the repository is empty",
        );
        assert!(
            card.contains("configs:"),
            "{family:?} has no viewer configuration, so the dataset page is blank",
        );
        assert!(
            card.contains("open-index/umi-meta"),
            "{family:?} does not say where the exclusion list is",
        );
        assert!(
            card.contains(umi_types::CANON_VERSION),
            "{family:?} does not name the canonicalisation version",
        );
        assert!(
            card.contains("umi-extract/0.0.1"),
            "{family:?} does not name the extractor that produced it",
        );
        assert!(
            card.contains("public, openly licensed web index"),
            "{family:?} drops doc 07.3's purpose declaration",
        );
        assert!(
            card.contains("tamnd87@gmail.com"),
            "{family:?} has no takedown address",
        );
    }
}

#[test]
fn the_obligation_to_filter_is_in_the_first_paragraph() {
    // Doc 12.8, in those words: every published dataset card says so in the
    // first paragraph. Further down is where a reader stops reading.
    for family in FAMILIES {
        let card = general(family);
        let body = card.split("---\n").nth(2).expect("the card after the yaml");
        let first = body
            .split("\n\n")
            .nth(1)
            .expect("a paragraph after the title");
        assert!(
            first.contains("exclusion list") && first.contains("filter"),
            "{family:?} buries the exclusion list: {first}",
        );
    }
}

#[test]
fn the_robots_card_does_not_let_anyone_think_it_replaces_asking() {
    // The one claim in this card that could cost a site. A reader who takes a
    // month old snapshot as a standing permission crawls against rules that
    // have changed, and RFC 9309 gives a cached file 24 hours for exactly that
    // reason. So the card says the limit and says what the corpus is good for
    // instead, rather than leaving a reader to infer either.
    let card = general(Family::Robots);
    assert!(card.contains("24 hours"), "the cache limit is not stated");
    assert!(
        card.contains("ask the origin yourself"),
        "the card does not say what to do instead",
    );
}

#[test]
fn the_robots_card_says_what_a_zero_status_is() {
    // A third of the rows carry one, and the obvious reading of it is wrong: a
    // host with no `robots.txt` answers 404, and a zero means the request never
    // came back. A reader who takes zero for "no file" counts several hundred
    // million hosts as having no rules when what we actually have is no answer.
    let card = general(Family::Robots);
    assert!(card.contains("does not mean the host has no"), "{card}");
    assert!(
        card.contains("we did not get an answer"),
        "the card does not say what to read it as instead",
    );
    // And the half of it that is ours rather than the web's. `allows_us` is 0
    // on a silent row because RFC 9309 says so, which is our rule applied to
    // our own failure and not the host refusing anybody.
    assert!(card.contains("not the host refusing you"), "{card}");

    // The other families have no such column and should not carry the section.
    assert!(!general(Family::Pages).contains("What a zero status means"));
}

#[test]
fn only_the_robots_card_names_a_repository_to_query() {
    // The snippet is worth having because the first thing anybody does with a
    // dataset is try to open it. It is only on this card because every other
    // family is split by week and slice, so its repository name carries a date
    // the card generator is not given, and a query naming the wrong repository
    // is worse than no query at all.
    let card = general(Family::Robots);
    assert!(
        card.contains("hf://datasets/open-index/umi-robots/data/**/*.parquet"),
        "{card}"
    );
    for family in [Family::Pages, Family::Receipts, Family::Frontier] {
        assert!(
            !general(family).contains("hf://"),
            "{family:?} names a repository this card cannot know",
        );
    }
}

#[test]
fn the_pages_card_does_not_claim_a_licence_we_do_not_hold() {
    // Doc 12.9's split. A `license: cc0-1.0` tag on a repository full of other
    // people's text would be a claim we cannot make, and it is the kind of
    // claim that is believed because it is machine readable.
    let pages = general(Family::Pages);
    assert!(pages.contains("license: other"), "{pages}");
    assert!(pages.contains("third party material"));
    assert!(!pages.contains("license: cc0-1.0"));

    // The two that are ours, on the other hand, are ours outright.
    assert!(general(Family::Receipts).contains("license: cc0-1.0"));
    assert!(general(Family::Robots).contains("license: cc0-1.0"));
}

#[test]
fn a_focused_crawl_says_it_is_one() {
    // Doc 13.7 sends a focused crawl to its own repository because it is not an
    // unbiased sample, and the card is the only place a reader who found the
    // repository on its own would learn that.
    let card = super::card(
        &Corpus::focused("open-index", "blog.rust-lang.org"),
        Family::Pages,
        "umi-extract/0.0.1",
    );
    assert!(
        card.contains("focused crawl over `blog.rust-lang.org`"),
        "{card}"
    );
    assert!(card.contains("not an unbiased sample"));
    // And it is named after its scope in both places a reader sees a name: the
    // title, and the one Hugging Face puts at the top of the dataset page.
    assert!(card.contains("pretty_name: umi focus blog.rust-lang.org"));
    assert!(card.contains("# umi-focus-blog.rust-lang.org"));

    // And the general corpus does not carry the sentence, since it is not true
    // of it.
    assert!(!general(Family::Pages).contains("focused crawl over"));
}

#[test]
fn the_card_is_written_the_way_the_rest_of_the_project_is() {
    // The house style, checked here because `scripts/check-spec.sh` only reads
    // `docs/spec` and this prose is generated from a Rust file it never sees.
    for family in FAMILIES {
        let card = general(family);
        assert!(
            !card.contains('\u{2014}') && !card.contains('\u{2013}'),
            "{family:?} has a dash nobody typed",
        );
    }
}

#[test]
fn the_robots_card_says_how_many_rows_are_a_second_answer_for_the_same_host() {
    // The reader who counts rows and calls it a host count is off by 23
    // percent, and the reader who takes any row for the current answer gets a
    // stale one. Both are avoidable by saying the number and showing the query,
    // and neither is avoidable by fixing the files, because a published file is
    // never rewritten.
    let card = general(Family::Robots);
    assert!(card.contains("more than once"), "{card}");
    assert!(
        card.contains("do not rewrite a published file"),
        "the card does not say why the duplicates are still there",
    );
    assert!(
        card.contains("QUALIFY row_number() OVER (PARTITION BY host ORDER BY fetched_at_ms DESC)"),
        "the card does not show how to take one row per host",
    );

    // And it no longer explains them as history, which is what they were
    // assumed to be until the corpus was counted. Every duplicate in it was
    // written inside a single day by two runs that overlapped.
    assert!(
        !card.contains("keeps history rather than replacing rows"),
        "{card}"
    );
}
