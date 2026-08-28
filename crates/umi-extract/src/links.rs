//! Links, which is doc 11.4.
//!
//! Links are half the reason the crawler exists, so this gets more care than
//! the body does. Everything here is decided once, on the fetcher, so that the
//! coordinator receives canonical keys it can check against the seen set
//! without parsing anything again.
//!
//! Two rules in doc 11.4 are easy to get backwards and are worth stating in the
//! code as well as in the spec. A link that says `rel="nofollow"` is recorded
//! and is not obeyed: it was designed as a comment spam countermeasure in 2005
//! and has not meant anything about crawlability for a long time, and treating
//! it as a crawl directive would cut out large parts of the web for no reason.
//! A page whose `meta robots` says `nofollow` is a different statement, made by
//! the site about its own page rather than about one destination, and that one
//! is obeyed.

use std::collections::HashSet;

use umi_types::{CanonError, canonicalize};

use crate::dom::{Dom, Kind, Tag};

/// Doc 11.4: a page contributes at most this many links.
///
/// Doc 09's trap section describes pages built to explode a frontier, and the
/// cheapest place to stop one is before it enters the frontier rather than
/// after. The first 5000 in document order are kept and a flag says so.
pub const MAX_LINKS: usize = 5000;

/// Doc 11.4: anchor text is truncated to this many bytes, on a character
/// boundary.
pub const MAX_ANCHOR: usize = 200;

/// The `rel` values doc 11.4 records, as a bitmask.
///
/// A bitmask rather than a set of booleans or a `Vec<String>` because this is a
/// published column on every link of every page of a hundred billion, and
/// sixteen bits is the difference between a number and a storage problem.
/// Values outside this list are dropped rather than kept as strings, for the
/// same reason.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Rel(u16);

impl Rel {
    /// `rel="nofollow"`. Recorded, never obeyed. See the module docs.
    pub const NOFOLLOW: Self = Self(1 << 0);
    /// `rel="ugc"`, user generated content.
    pub const UGC: Self = Self(1 << 1);
    /// `rel="sponsored"`.
    pub const SPONSORED: Self = Self(1 << 2);
    /// `rel="noopener"`.
    pub const NOOPENER: Self = Self(1 << 3);
    /// `rel="canonical"`.
    pub const CANONICAL: Self = Self(1 << 4);
    /// `rel="alternate"`.
    pub const ALTERNATE: Self = Self(1 << 5);
    /// `rel="next"`.
    pub const NEXT: Self = Self(1 << 6);
    /// `rel="prev"`, and `rel="previous"`, which is the same thing spelled the
    /// way people actually spell it.
    pub const PREV: Self = Self(1 << 7);
    /// `rel="me"`, which is how the fediverse verifies a link back.
    pub const ME: Self = Self(1 << 8);
    /// `rel="author"`.
    pub const AUTHOR: Self = Self(1 << 9);

    /// No `rel` at all, which is most links.
    pub const NONE: Self = Self(0);

    /// The raw bits, for the published column.
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Whether every bit in `other` is set here.
    pub const fn has(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    /// Both sets of bits.
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    /// Parse one `rel` attribute, which is a space separated list.
    ///
    /// Unknown tokens are ignored rather than being an error. `rel` is an open
    /// vocabulary and pages carry all sorts of things in it, and a link is not
    /// worth dropping because somebody invented a keyword.
    pub fn parse(value: &str) -> Self {
        let mut bits = Self::NONE;
        for token in value.split_ascii_whitespace() {
            // ASCII case folding, not `to_lowercase`. Doc 11.1 rules out locale
            // dependent folding anywhere a decision hangs on it, and this
            // decides the value of a published column.
            let one = match token.to_ascii_lowercase().as_str() {
                "nofollow" => Self::NOFOLLOW,
                "ugc" => Self::UGC,
                "sponsored" => Self::SPONSORED,
                "noopener" => Self::NOOPENER,
                "canonical" => Self::CANONICAL,
                "alternate" => Self::ALTERNATE,
                "next" => Self::NEXT,
                "prev" | "previous" => Self::PREV,
                "me" => Self::ME,
                "author" => Self::AUTHOR,
                _ => continue,
            };
            bits = bits.union(one);
        }
        bits
    }
}

/// What kind of link this is, from doc 11.4.
///
/// The discriminants are the byte doc 10.5's `links.kind` column stores, so
/// they are a published format. New kinds are appended and an old one is never
/// renumbered, because a segment written today is read by a build from two
/// years from now.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum LinkKind {
    /// An `<a>` inside the extracted content root.
    Body = 0,
    /// An `<a>` outside it, which is nearly always navigation, a sidebar or a
    /// footer.
    Nav = 1,
    /// A `<link>` element in the head.
    Link = 2,
    /// A redirect we followed to get here. Not produced by this crate, because
    /// the redirect chain belongs to the fetch path, which is the only thing
    /// that saw it.
    Redirect = 3,
    /// A sitemap, from `<link rel="sitemap">`.
    Sitemap = 4,
    /// An RSS or Atom feed, from `<link rel="alternate">` with a feed type.
    Feed = 5,
}

impl LinkKind {
    /// Every kind, in code order.
    pub const ALL: [Self; 6] = [
        Self::Body,
        Self::Nav,
        Self::Link,
        Self::Redirect,
        Self::Sitemap,
        Self::Feed,
    ];

    /// The byte doc 10.5's `links.kind` column holds.
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Recover a kind from a stored byte, or `None` for one this build does
    /// not know, which is what reading a newer segment looks like.
    #[must_use]
    pub const fn from_u8(byte: u8) -> Option<Self> {
        if (byte as usize) < Self::ALL.len() {
            Some(Self::ALL[byte as usize])
        } else {
            None
        }
    }
}

/// One link.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Link {
    /// The canonical absolute URL, already through doc 11.2, so the coordinator
    /// can hash it into a key without parsing it again.
    pub url: String,
    /// The anchor text, whitespace collapsed and truncated to `MAX_ANCHOR`
    /// bytes on a character boundary. Empty for a `<link>` element, which has
    /// no text.
    pub anchor: String,
    /// Body, navigation, `<link>`, sitemap or feed.
    pub kind: LinkKind,
    /// The `rel` bitmask.
    pub rel: Rel,
}

impl Link {
    /// Whether this link names a page, as opposed to something the page needs
    /// in order to render itself.
    ///
    /// Doc 10.5's `links` column keeps every link a page carries, including its
    /// stylesheets and its icons, because that column is what the page said and
    /// a crawler's opinion does not change it. The frontier wants a narrower
    /// set, and this is where the two part company.
    ///
    /// An anchor names a page, always. So does a sitemap and so does a feed:
    /// neither is a page itself, but both are lists of pages, and dropping them
    /// would lose the cheapest discovery a site offers. A `<link>` element is
    /// the one that has to be asked, because the head holds the site's own
    /// navigation and its build output in the same element. `canonical`, `next`,
    /// `prev` and `alternate` name pages. `stylesheet`, `icon`, `preload`,
    /// `modulepreload`, `manifest` and `dns-prefetch` name parts, and following
    /// those is how a crawl of five pages fetches two favicons and a bundle.
    #[must_use]
    pub const fn is_page(&self) -> bool {
        match self.kind {
            LinkKind::Body | LinkKind::Nav | LinkKind::Redirect => true,
            LinkKind::Sitemap | LinkKind::Feed => true,
            LinkKind::Link => {
                self.rel.has(Rel::CANONICAL)
                    || self.rel.has(Rel::NEXT)
                    || self.rel.has(Rel::PREV)
                    || self.rel.has(Rel::ALTERNATE)
            }
        }
    }
}

/// Every link on a page, and what happened to the ones that are not here.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Links {
    /// The links, in document order, deduplicated on the whole triple of URL,
    /// anchor and kind.
    pub links: Vec<Link>,
    /// Links dropped because canonicalisation rejected them. Published, because
    /// doc 11.4 wants it watched: a sudden rise in this is a good sign that an
    /// extractor build is broken.
    pub dropped: u32,
    /// Links dropped for having a scheme that is not `http` or `https`.
    ///
    /// Counted apart from `dropped` even though doc 11.4 describes one number,
    /// because a page with a `mailto:` in the footer is not a broken build and
    /// mixing the two makes the signal that doc 11.4 actually wants useless.
    /// Recorded here for a spec edit.
    pub dropped_scheme: u32,
    /// The page had more than `MAX_LINKS` links and the rest were not kept.
    pub truncated: bool,
}

/// What a page's own `meta robots` says about it.
///
/// This is the one place where extraction makes a policy decision, and doc 11.4
/// puts it here rather than in doc 07 because it depends on parsing the body.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Robots {
    /// Do not index the content. Obeyed: the row is still written, with its
    /// URL, status, headers and links, and the markdown, title, description and
    /// snippets withheld. A page we may not index is still a fact about the
    /// frontier and throwing the row away would lose it.
    pub noindex: bool,
    /// Do not follow the links on this page. Obeyed, unlike a link's own
    /// `rel="nofollow"`. See the module docs for why those are different.
    pub nofollow: bool,
}

impl Robots {
    /// Parse one directive list, from a `meta robots` content attribute or an
    /// `X-Robots-Tag` header value.
    ///
    /// `none` is the shorthand for both, which is in the original spec and is
    /// missed often enough to be worth a line of its own.
    ///
    /// Two shapes carry a colon and neither one is a directive we act on. A
    /// keyed directive such as `max-image-preview:none` or `max-snippet:-1`
    /// names a preview limit and says nothing about indexing, and reading the
    /// word after its colon as a directive is how `max-image-preview:none`
    /// turns into "withhold this page". That was not hypothetical: it withheld
    /// a real page out of the two thousand this was measured against. A leading
    /// agent name such as `googlebot: noindex` is addressed to somebody who is
    /// not us, and the whole value goes with it. The reason a directive naming
    /// another crawler is left alone rather than obeyed is written up on this
    /// module's page level reader, which is the other half of the same rule:
    /// telling one crawler's name from an ordinary `meta` name needs a list of
    /// crawler names, and that list is doc 07's job rather than this crate's.
    pub fn parse(value: &str) -> Self {
        let mut robots = Self::default();
        for (index, token) in value.split(',').enumerate() {
            let token = token.trim();
            let Some((key, _)) = token.split_once(':') else {
                match token.to_ascii_lowercase().as_str() {
                    "noindex" => robots.noindex = true,
                    "nofollow" => robots.nofollow = true,
                    "none" => {
                        robots.noindex = true;
                        robots.nofollow = true;
                    }
                    _ => {}
                }
                continue;
            };
            if index == 0 && !is_keyed(key.trim()) {
                return Self::default();
            }
        }
        robots
    }

    /// Both sets of directives, which is how a `meta robots` tag and an
    /// `X-Robots-Tag` header combine: whichever says no, wins.
    pub const fn union(self, other: Self) -> Self {
        Self {
            noindex: self.noindex || other.noindex,
            nofollow: self.nofollow || other.nofollow,
        }
    }
}

/// The robots directives with a colon in them that are real directives rather
/// than an agent name.
///
/// The list exists so that `googlebot: noindex` and `max-snippet:-1` can be told
/// apart, since they have the same shape and mean nothing like the same thing.
/// It is short because the keyed directives are all preview and snippet limits
/// and none of them changes whether a page is indexed.
fn is_keyed(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "max-snippet" | "max-image-preview" | "max-video-preview" | "unavailable_after"
    )
}

/// The `meta robots` directives on a page.
///
/// `name="robots"` is the one every crawler reads. A `name` naming one specific
/// crawler is not ours and is left alone, and the same goes for an `X-Robots-Tag`
/// that opens with an agent name.
///
/// That is a choice and the alternative is defensible, so here is the reason.
/// Obeying somebody else's `noindex` is the more cautious reading, but there is
/// no safe way to do it: telling `<meta name="acmebot" content="noindex">` from
/// `<meta name="keywords" content="noindex, nofollow">` needs a list of crawler
/// names, and a list of crawler names is doc 07's job, not this crate's. Rather
/// than have the two spellings of one statement disagree, both ignore it, and
/// the day doc 07 grows that list both change together.
pub fn robots(dom: &Dom) -> Robots {
    let mut found = Robots::default();
    for id in 0..dom.node_count() {
        if dom.tag(id) != Some(Tag::Meta) {
            continue;
        }
        let Some(element) = dom.element(id) else {
            continue;
        };
        if !element
            .attr("name")
            .is_some_and(|name| name.trim().eq_ignore_ascii_case("robots"))
        {
            continue;
        }
        if let Some(content) = element.attr("content") {
            found = found.union(Robots::parse(content));
        }
    }
    found
}

/// Every link on the page, in document order.
///
/// `root` is the content root doc 11.3 chose, which is what splits a body
/// anchor from a navigation one. That distinction comes free from work already
/// done and it matters more than it looks: navigation links are how you
/// discover a site's structure and body links are how you estimate a page's
/// importance, and doc 09's priority function weights them differently.
///
/// `base` is already resolved: `<base href>` when the page carried a well
/// formed one and the final URL after redirects otherwise, never the requested
/// URL. Getting that backwards is the classic bug that sprays a site's relative
/// links onto somebody else's host.
pub fn collect(dom: &Dom, root: usize, base: &str, page: Robots) -> Links {
    let mut out = Links::default();
    // Exact `(url, anchor, kind)` triples are deduplicated within the page, so
    // a template that repeats its masthead link forty times contributes it once.
    // The set holds indices into `out.links` rather than copies of the triple.
    let mut seen: HashSet<(String, String, LinkKind)> = HashSet::new();
    let inside = in_root(dom, root);

    // `inside` has one entry per node, so this is `0..dom.node_count()` with the
    // content test already in hand. Index order is document order, because the
    // arena is built by a depth first walk that numbers parents before children.
    for (id, &in_content) in inside.iter().enumerate() {
        let Some(element) = dom.element(id) else {
            continue;
        };
        let (href, mut rel, kind) = match element.tag {
            Tag::A => {
                let Some(href) = element.attr("href") else {
                    continue;
                };
                let kind = if in_content {
                    LinkKind::Body
                } else {
                    LinkKind::Nav
                };
                (href, Rel::parse(element.attr("rel").unwrap_or("")), kind)
            }
            Tag::Link => {
                let Some(href) = element.attr("href") else {
                    continue;
                };
                let rel = Rel::parse(element.attr("rel").unwrap_or(""));
                (
                    href,
                    rel,
                    link_kind(element.attr("rel"), element.attr("type")),
                )
            }
            _ => continue,
        };

        // Doc 11.4: a page level `nofollow` applies to every link on the page,
        // so it is recorded on each one rather than left for a consumer to
        // remember to join against the page row.
        if page.nofollow {
            rel = rel.union(Rel::NOFOLLOW);
        }
        let url = match canonicalize(href, Some(base)) {
            Ok(url) => url,
            Err(CanonError::NotHttp) => {
                // `mailto:`, `javascript:`, `tel:`, `data:` and the long tail of
                // application handlers. `mailto:` targets are dropped rather
                // than collected on purpose: harvesting email addresses at web
                // scale creates an obligation we do not want.
                out.dropped_scheme = out.dropped_scheme.saturating_add(1);
                continue;
            }
            Err(_) => {
                out.dropped = out.dropped.saturating_add(1);
                continue;
            }
        };

        let anchor = if element.tag == Tag::A {
            anchor_text(dom, id)
        } else {
            String::new()
        };

        if !seen.insert((url.clone(), anchor.clone(), kind)) {
            continue;
        }
        if out.links.len() == MAX_LINKS {
            out.truncated = true;
            break;
        }
        out.links.push(Link {
            url,
            anchor,
            kind,
            rel,
        });
    }

    out
}

/// Which nodes sit inside the content root, so that a body anchor can be told
/// from a navigation one.
///
/// One downward pass from the root marking its subtree, rather than an ancestor
/// walk per link. A page with 5000 links and a root near the top would otherwise
/// walk most of the tree 5000 times.
///
/// Chrome stops the walk. A `<nav>` inside an `<article>` is still navigation,
/// and it is the one case where the content root and the content disagree.
fn in_root(dom: &Dom, root: usize) -> Vec<bool> {
    let mut inside = vec![false; dom.node_count()];
    let mut stack = vec![root];
    while let Some(id) = stack.pop() {
        if id >= inside.len() || dom.chrome(id) {
            continue;
        }
        inside[id] = true;
        stack.extend(dom.children(id).iter().copied());
    }
    inside
}

/// A `<link>` element's kind, from its `rel` and its `type`.
fn link_kind(rel: Option<&str>, kind: Option<&str>) -> LinkKind {
    let rel = rel.unwrap_or("");
    if rel
        .split_ascii_whitespace()
        .any(|token| token.eq_ignore_ascii_case("sitemap"))
    {
        return LinkKind::Sitemap;
    }
    // A feed is `rel="alternate"` plus a feed media type. The `rel` alone is
    // also how a page declares a translation, and the type alone appears on
    // things that are not alternates, so it takes both.
    let feed = matches!(
        kind.unwrap_or("").trim().to_ascii_lowercase().as_str(),
        "application/rss+xml" | "application/atom+xml" | "application/feed+json"
    );
    if feed {
        return LinkKind::Feed;
    }
    LinkKind::Link
}

/// An anchor's text, collapsed and truncated the way doc 11.4 asks.
///
/// The text of the whole subtree, because an anchor wrapping an `<img>` and a
/// `<span>` is one link with one label as far as a reader is concerned.
fn anchor_text(dom: &Dom, id: usize) -> String {
    let mut text = String::new();
    let mut stack = vec![id];
    let mut order = Vec::new();
    while let Some(node) = stack.pop() {
        order.push(node);
        stack.extend(dom.children(node).iter().rev());
    }
    for node in order {
        if let Kind::Text(raw) = dom.kind(node) {
            for word in raw.split_ascii_whitespace() {
                if !text.is_empty() {
                    text.push(' ');
                }
                text.push_str(word);
                if text.len() >= MAX_ANCHOR {
                    break;
                }
            }
        }
        if text.len() >= MAX_ANCHOR {
            break;
        }
    }
    truncate(text, MAX_ANCHOR)
}

/// Cut a string to at most `max` bytes without splitting a character.
fn truncate(mut text: String, max: usize) -> String {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rel_parses_a_list_and_ignores_what_it_does_not_know() {
        let rel = Rel::parse("  NoFollow   ugc  invented-by-somebody ");
        assert!(rel.has(Rel::NOFOLLOW));
        assert!(rel.has(Rel::UGC));
        assert!(!rel.has(Rel::SPONSORED));
    }

    #[test]
    fn previous_is_spelled_both_ways() {
        assert!(Rel::parse("previous").has(Rel::PREV));
        assert!(Rel::parse("prev").has(Rel::PREV));
    }

    #[test]
    fn an_anchor_is_a_page_whatever_its_rel_says() {
        let link = |kind, rel| Link {
            url: "https://example.com/a".to_owned(),
            anchor: String::new(),
            kind,
            rel: Rel::parse(rel),
        };
        assert!(link(LinkKind::Body, "nofollow ugc sponsored").is_page());
        assert!(link(LinkKind::Nav, "").is_page());
        assert!(link(LinkKind::Sitemap, "sitemap").is_page());
        assert!(link(LinkKind::Feed, "alternate").is_page());
    }

    #[test]
    fn a_head_link_is_a_page_only_when_its_rel_names_one() {
        let link = |rel| Link {
            url: "https://example.com/a".to_owned(),
            anchor: String::new(),
            kind: LinkKind::Link,
            rel: Rel::parse(rel),
        };
        for rel in ["canonical", "next", "prev", "alternate"] {
            assert!(link(rel).is_page(), "{rel}");
        }
        // The four that turned up in a real crawl of excalidraw.com, plus the
        // two that travel with them.
        for rel in [
            "stylesheet",
            "icon",
            "modulepreload",
            "manifest",
            "preload",
            "dns-prefetch",
        ] {
            assert!(!link(rel).is_page(), "{rel}");
        }
    }

    #[test]
    fn robots_none_means_both() {
        let robots = Robots::parse("none");
        assert!(robots.noindex);
        assert!(robots.nofollow);
    }

    #[test]
    fn a_directive_addressed_to_another_crawler_is_not_ours() {
        assert_eq!(Robots::parse("googlebot: noindex"), Robots::default());
        // And the rest of the list goes with it, because the agent name is a
        // prefix on the whole value and not on the first token of it.
        assert_eq!(
            Robots::parse("googlebot: noindex, nofollow"),
            Robots::default()
        );
    }

    #[test]
    fn a_preview_limit_is_not_a_noindex() {
        // The one that got through: `max-image-preview:none` withheld a real
        // page because the word after the colon was read as the `none`
        // shorthand for noindex plus nofollow.
        assert_eq!(
            Robots::parse("max-image-preview:none"),
            Robots::default(),
            "max-image-preview says nothing about indexing"
        );
        assert_eq!(Robots::parse("max-snippet:-1"), Robots::default());
        // A real directive alongside one still counts.
        let both = Robots::parse("max-image-preview:large, noindex");
        assert!(both.noindex);
        assert!(!both.nofollow);
    }

    #[test]
    fn anchor_text_is_cut_on_a_character_boundary() {
        // A three byte character straddling the limit must not come back as
        // half of itself, which would not be UTF-8 at all.
        let long = "\u{3042}".repeat(100);
        let cut = truncate(long, MAX_ANCHOR);
        assert!(cut.len() <= MAX_ANCHOR);
        assert_eq!(cut.len() % 3, 0);
    }
}
