//! The document tree, cleaned down to what the markdown subset can express.
//!
//! html5ever builds an `Rc` tree with interior mutability. That is the right
//! shape for a parser and the wrong shape for everything after it: it cannot
//! move across threads, it holds every attribute of every node alive, and
//! walking it means chasing pointers through `RefCell` twice per node. Doc 11.9
//! gives the whole of extraction 3 to 8 ms per page, and we walk the tree three
//! times, so the parse is converted once into a flat arena and the html5ever
//! tree is dropped before scoring starts.
//!
//! The conversion is also where doc 11.3's drop list is applied. A `<script>`
//! subtree never reaches the arena, so nothing downstream has to remember to
//! skip it.
//!
//! That list has two halves, and they are not the same rule. Most of it is
//! "these bytes are not content and nothing else wants them", and those really
//! do never arrive. But `nav`, `header`, `footer` and `aside` are not content
//! and are still wanted: doc 11.4 asks for a navigation link kind, and says the
//! reason it wants one is that navigation links are how you discover a site's
//! structure. Deleting those four subtrees before the link pass runs would throw
//! away most of the links doc 11.4 is asking for. So they arrive as
//! [`Tag::Chrome`], marked, out of the content and still walkable.

use html5ever::tendril::TendrilSink;
use html5ever::{ParseOpts, parse_document};
use markup5ever_rcdom::{Handle, NodeData};

use crate::sink::Sink;

/// The index of the root node, which every arena has and which is never an
/// element.
pub const ROOT: usize = 0;

/// How deep the arena is allowed to get.
///
/// A page with 50000 nested `<div>` tags is a real thing that real crawlers
/// fall over on, because every tree walk after the parse is recursive and the
/// stack is not infinite. Past this depth the elements are flattened into their
/// nearest ancestor rather than dropped, so the text still comes out and the
/// only thing lost is nesting nobody was rendering anyway.
pub const MAX_DEPTH: u32 = 256;

/// The attributes kept on the arena.
///
/// Doc 11.3 drops every attribute except `href`, `src`, `alt`, `title`, `lang`,
/// `datetime` and the `class` used for code language detection, and that is the
/// rule for the markdown output. It is not the rule for the arena: scoring needs
/// `class` and `id` to match the boilerplate markers, the link pass needs `rel`,
/// and the metadata pass needs the `<meta>` attributes. None of those reach the
/// output. Everything else is dropped here so that a page with 400 inline styles
/// does not pay to carry them.
const KEPT_ATTRS: [&str; 16] = [
    "alt",
    "charset",
    "class",
    "content",
    "datetime",
    "href",
    "http-equiv",
    "id",
    "itemprop",
    "lang",
    "name",
    "property",
    "rel",
    "src",
    "title",
    // `type` on a `<link>`, which is the only thing that tells an RSS feed from
    // a translation: doc 11.4's feed kind needs `rel="alternate"` and a feed
    // media type together, because either one on its own means something else.
    "type",
];

/// A tag we have a rule for.
///
/// Anything not listed is `Other` or `Block`, which are both transparent: their
/// children are serialised and the tag itself contributes nothing. The split
/// between the two is whether the tag breaks a paragraph.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tag {
    /// `<html>`.
    Html,
    /// `<head>`.
    Head,
    /// `<body>`.
    Body,
    /// `<title>`.
    Title,
    /// `<meta>`.
    Meta,
    /// `<link>`.
    Link,
    /// `<base>`.
    Base,
    /// `<article>`.
    Article,
    /// `<main>`.
    Main,
    /// `<section>`.
    Section,
    /// `<div>`.
    Div,
    /// `<span>`.
    Span,
    /// `<h1>` through `<h6>`, carrying the level.
    Heading(u8),
    /// `<p>`.
    P,
    /// `<br>`.
    Br,
    /// `<hr>`.
    Hr,
    /// `<ul>`.
    Ul,
    /// `<ol>`.
    Ol,
    /// `<li>`.
    Li,
    /// `<blockquote>`.
    Blockquote,
    /// `<pre>`.
    Pre,
    /// `<code>`.
    Code,
    /// `<table>`.
    Table,
    /// `<tr>`.
    Tr,
    /// `<th>`.
    Th,
    /// `<td>`.
    Td,
    /// `<a>`.
    A,
    /// `<img>`.
    Img,
    /// `<em>`, `<i>`, `<cite>`, `<dfn>`, `<var>`.
    Em,
    /// `<strong>`, `<b>`.
    Strong,
    /// A block level tag with no markdown of its own, such as `<figure>` or
    /// `<dd>`. Breaks the paragraph, serialises its children, prints nothing.
    Block,
    /// `<nav>`, `<header>`, `<footer>` and `<aside>`: site furniture that doc
    /// 11.3 drops from the content and doc 11.4 still wants the links out of.
    ///
    /// It contributes nothing to the markdown, nothing to any score and nothing
    /// to any text total, exactly as if it had never been parsed. The only pass
    /// that looks inside one is the link pass. See [`Dom::chrome`].
    Chrome,
    /// Any other tag. Transparent and inline, so `<sub>` in the middle of a
    /// sentence does not cut the sentence in three.
    Other,
}

impl Tag {
    /// Whether this tag can win main content detection.
    ///
    /// Doc 11.3 step 2 says block level nodes. Taking that literally would make
    /// a `<p>` a candidate, and the winner would then be the single longest
    /// paragraph on the page rather than the article. Candidates are containers.
    pub fn is_candidate(self) -> bool {
        matches!(
            self,
            Self::Body
                | Self::Article
                | Self::Main
                | Self::Section
                | Self::Div
                | Self::Blockquote
                | Self::Td
        )
    }
}

/// What a node is.
#[derive(Debug)]
pub enum Kind {
    /// The arena root. Its children are whatever html5ever put at the top of
    /// the document, which is usually one `<html>` and sometimes a stray
    /// doctype's worth of nothing.
    Root,
    /// An element.
    Element(Element),
    /// A run of text, exactly as it appeared. Whitespace is collapsed by the
    /// serialiser and not here, because `<pre>` needs the original.
    Text(String),
}

/// An element and the attributes worth keeping.
#[derive(Debug)]
pub struct Element {
    /// The tag.
    pub tag: Tag,
    /// The kept attributes, in document order, names lowercased.
    pub attrs: Vec<(String, String)>,
}

impl Element {
    /// The first value for an attribute name, which is what a browser uses when
    /// a page repeats one.
    pub fn attr(&self, name: &str) -> Option<&str> {
        self.attrs
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// One node in the arena.
///
/// There is no parent link. Every pass over the tree so far walks downwards and
/// a parent index nobody reads is eight bytes per node on a tree with a hundred
/// thousand of them, so it goes in when something needs it.
#[derive(Debug)]
pub struct Node {
    /// What the node is.
    pub kind: Kind,
    /// Children in document order.
    pub children: Vec<usize>,
    /// This node is a [`Tag::Chrome`] element or sits under one.
    pub chrome: bool,
}

/// A parsed document.
#[derive(Debug)]
pub struct Dom {
    nodes: Vec<Node>,
    dropped: u32,
    ld_json: Vec<String>,
    microdata: bool,
    rdfa: bool,
}

/// How many `application/ld+json` blocks are collected from one page.
///
/// Doc 11.6 keeps five fields out of them and explicitly does not keep the blob,
/// because on an e-commerce page the blob is routinely larger than the article.
/// A page with three hundred product blocks has nothing more to say after the
/// first few and this stops the parse carrying them all.
const MAX_LD_JSON: usize = 16;

/// How many bytes of one `application/ld+json` block are collected.
///
/// Same reason. A megabyte of product feed in a `<script>` tag is a real thing
/// and the five fields worth having are never a megabyte in.
const MAX_LD_JSON_BYTES: usize = 256 * 1024;

impl Dom {
    /// Parse a document.
    ///
    /// The bytes are decoded as UTF-8 with invalid sequences replaced, which is
    /// what html5ever's `from_utf8` does and what a browser does. Transcoding
    /// from a declared charset happens in the fetch path before this, because
    /// the charset comes off the Content-Type header as often as it comes off a
    /// `<meta>` tag and only the caller has both.
    pub fn parse(html: &[u8]) -> Self {
        // Script and style are 41 percent of the bytes on a real page and this
        // hands every one of them to html5ever to tokenise, allocate a tendril
        // for and hang off a tree that `absorb` then throws away. Taking them
        // out with a byte scan first was tried and reverted: on a quiet cpu it
        // was worth two percent, and it disagreed with html5ever on five of two
        // thousand real pages. Doc 11.1 wants byte identical output forever, so
        // a scanner that quietly differs from the reference parser is not worth
        // two percent. If this needs to be fast, the answer is to build the
        // arena from html5ever's tokeniser and skip `RcDom`, not to guess ahead
        // of it.
        let parsed = parse_document(Sink::default(), ParseOpts::default())
            .from_utf8()
            .one(html);
        let mut dom = Self {
            nodes: vec![Node {
                kind: Kind::Root,
                children: Vec::new(),
                chrome: false,
            }],
            dropped: 0,
            ld_json: Vec::new(),
            microdata: false,
            rdfa: false,
        };
        dom.absorb(&parsed.document);
        dom
    }

    /// Convert the html5ever tree into the arena.
    ///
    /// Iterative rather than recursive, because the input is hostile by
    /// definition and 50000 nested tags would otherwise be a stack overflow
    /// before `MAX_DEPTH` had a chance to apply. Children are pushed in reverse
    /// so that popping visits them in document order, which is also why a node's
    /// index is greater than every index before it in the document and why every
    /// pass downstream can treat `0..node_count()` as document order.
    fn absorb(&mut self, document: &Handle) {
        let mut stack: Vec<(Handle, usize, u32, bool)> = vec![(document.clone(), ROOT, 0, false)];
        while let Some((handle, parent, depth, chrome)) = stack.pop() {
            let mut chrome = chrome;
            let (kind, keep_children) = match &handle.data {
                NodeData::Document => (None, true),
                NodeData::Text { contents } => {
                    let text = contents.borrow().to_string();
                    // Text inside chrome counts as dropped, because that is what
                    // it was before chrome was kept and this signal should not
                    // move for a change nobody can see in the output.
                    if chrome {
                        self.dropped = self.dropped.saturating_add(text.len() as u32);
                    }
                    (Some(Kind::Text(text)), false)
                }
                NodeData::Element { name, attrs, .. } => {
                    let local = name.local.as_ref();
                    self.note_vocabulary(&attrs.borrow());
                    match classify(local) {
                        None => {
                            // `<script type="application/ld+json">` is the one
                            // dropped subtree with something in it we want. Doc
                            // 11.6 reads five fields out of it, and the script
                            // still does not enter the tree, so no pass
                            // downstream has to learn that some scripts are
                            // different from other scripts.
                            if local == "script" {
                                self.note_ld_json(&handle, &attrs.borrow());
                            }
                            // Already inside chrome, so this subtree's text is
                            // counted by whichever of the two rules gets there
                            // first and never by both.
                            if !chrome {
                                self.dropped += dropped_bytes(&handle);
                            }
                            continue;
                        }
                        Some(tag) => {
                            chrome = chrome || tag == Tag::Chrome;
                            let attrs = attrs
                                .borrow()
                                .iter()
                                .filter(|attr| KEPT_ATTRS.contains(&attr.name.local.as_ref()))
                                .map(|attr| {
                                    (attr.name.local.as_ref().to_owned(), attr.value.to_string())
                                })
                                .collect();
                            (Some(Kind::Element(Element { tag, attrs })), true)
                        }
                    }
                }
                // Comments, doctypes and processing instructions are dropped
                // whole. A doctype has no children and a comment's children are
                // not a thing, so nothing is lost by not descending.
                _ => continue,
            };

            let (me, next_depth) = match kind {
                Some(kind) if depth < MAX_DEPTH => {
                    let me = self.nodes.len();
                    self.nodes.push(Node {
                        kind,
                        children: Vec::new(),
                        chrome,
                    });
                    self.nodes[parent].children.push(me);
                    (me, depth + 1)
                }
                // Past the cap the element is flattened: its children are
                // attached to the last ancestor that fit, still in order.
                Some(Kind::Text(text)) => {
                    let me = self.nodes.len();
                    self.nodes.push(Node {
                        kind: Kind::Text(text),
                        children: Vec::new(),
                        chrome,
                    });
                    self.nodes[parent].children.push(me);
                    (me, depth)
                }
                Some(_) => (parent, depth),
                None => (parent, depth),
            };

            if keep_children {
                for child in handle.children.borrow().iter().rev() {
                    stack.push((child.clone(), me, next_depth, chrome));
                }
            }
        }
    }

    /// Flag a microdata or RDFa vocabulary on an element.
    ///
    /// Doc 11.6 detects both and parses neither, which is a scope cut recorded
    /// in doc 17. Detection is on the attributes that open a vocabulary rather
    /// than on the ones that name a field, because `itemprop` and `property`
    /// both turn up on pages carrying neither vocabulary and would flag most of
    /// the web.
    fn note_vocabulary(&mut self, attrs: &[html5ever::Attribute]) {
        // Both flags set means there is nothing left to look for, and this runs
        // on every element of every page.
        if self.microdata && self.rdfa {
            return;
        }
        for attr in attrs {
            match attr.name.local.as_ref() {
                "itemscope" | "itemtype" => self.microdata = true,
                "typeof" | "vocab" => self.rdfa = true,
                _ => {}
            }
        }
    }

    /// Collect a `<script type="application/ld+json">` body.
    ///
    /// The type test is exact after trimming and ASCII case folding. A `type`
    /// that is missing, or that says `text/javascript`, is a script and not
    /// structured data, and treating a bare `<script>` as JSON-LD would hand the
    /// parser every inline script on the page.
    fn note_ld_json(&mut self, handle: &Handle, attrs: &[html5ever::Attribute]) {
        if self.ld_json.len() >= MAX_LD_JSON {
            return;
        }
        let structured = attrs.iter().any(|attr| {
            attr.name.local.as_ref() == "type"
                && attr
                    .value
                    .trim()
                    .eq_ignore_ascii_case("application/ld+json")
        });
        if !structured {
            return;
        }
        // A script element holds exactly one text child when it holds anything,
        // because the tokeniser runs it in a raw text state.
        let mut body = String::new();
        for child in handle.children.borrow().iter() {
            if let NodeData::Text { contents } = &child.data {
                body.push_str(&contents.borrow());
            }
            if body.len() > MAX_LD_JSON_BYTES {
                return;
            }
        }
        if !body.trim().is_empty() {
            self.ld_json.push(body);
        }
    }

    /// How many nodes the arena holds. Never zero, because the root is always
    /// there, which is why this is not called `len`.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// The `application/ld+json` blocks, in document order.
    pub fn ld_json(&self) -> &[String] {
        &self.ld_json
    }

    /// The page carries microdata: some element had `itemscope` or `itemtype`.
    pub fn microdata(&self) -> bool {
        self.microdata
    }

    /// The page carries RDFa: some element had `typeof` or `vocab`.
    pub fn rdfa(&self) -> bool {
        self.rdfa
    }

    /// What a node is.
    pub fn kind(&self, id: usize) -> &Kind {
        &self.nodes[id].kind
    }

    /// A node's children.
    pub fn children(&self, id: usize) -> &[usize] {
        &self.nodes[id].children
    }

    /// Whether a node is site furniture: a `<nav>`, `<header>`, `<footer>` or
    /// `<aside>`, or anything under one.
    ///
    /// Every pass except the link pass treats a true here as "this node is not
    /// in the document", which is what doc 11.3's drop list says and what the
    /// tree did before chrome was kept.
    pub fn chrome(&self, id: usize) -> bool {
        self.nodes[id].chrome
    }

    /// A node's element, or `None` for the root and for text.
    pub fn element(&self, id: usize) -> Option<&Element> {
        match &self.nodes[id].kind {
            Kind::Element(element) => Some(element),
            _ => None,
        }
    }

    /// A node's tag, or `None` for the root and for text.
    pub fn tag(&self, id: usize) -> Option<Tag> {
        self.element(id).map(|element| element.tag)
    }

    /// The first element with this tag in document order.
    pub fn first(&self, tag: Tag) -> Option<usize> {
        (0..self.nodes.len()).find(|&id| self.tag(id) == Some(tag))
    }

    /// The bytes of text dropped with `<script>` and `<style>`, which is one of
    /// doc 11.6's quality signals and is free to count here.
    ///
    /// Decoded text rather than source bytes, so it reads shorter than the page
    /// wherever the source spelled a character as an entity. It is a rough "how
    /// much of this page was machinery" and not a digest input.
    pub fn dropped_bytes(&self) -> u32 {
        self.dropped
    }
}

/// Map a tag name to a rule, or to `None` for the subtrees doc 11.3 drops.
///
/// `textarea`, `option`, `label` and `legend` are dropped alongside the form
/// controls doc 11.3 names. They are the same class of thing and their text is
/// chrome, not content: the word "Email" next to a box is not something a reader
/// came for. That is a deviation from the letter of the list and it is recorded
/// here rather than being silent.
fn classify(name: &str) -> Option<Tag> {
    Some(match name {
        "script" | "style" | "noscript" | "svg" | "canvas" | "iframe" | "object" | "embed"
        | "input" | "button" | "select" | "textarea" | "option" | "template" | "label"
        | "legend" => return None,

        // Doc 11.3 drops these four and doc 11.4 wants their links, so they are
        // kept out of the content without being deleted. See the module docs.
        // Written up for a spec edit.
        "nav" | "header" | "footer" | "aside" => Tag::Chrome,

        // Doc 11.3 lists `form` with the controls and that is a mistake in the
        // document, not a rule to follow off a cliff. A form is a wrapper, not a
        // control: ASP.NET WebForms puts one `<form runat="server">` around the
        // entire body of every page it serves, and on the two thousand real
        // Common Crawl pages this was measured against, 266 of them, thirteen
        // percent, have a form spanning more than half the document. Dropping
        // the subtree deletes those pages. The controls inside it are still
        // dropped, which is what the rule was reaching for. Written up for a
        // spec edit.
        "form" => Tag::Block,

        "html" => Tag::Html,
        "head" => Tag::Head,
        "body" => Tag::Body,
        "title" => Tag::Title,
        "meta" => Tag::Meta,
        "link" => Tag::Link,
        "base" => Tag::Base,

        "article" => Tag::Article,
        "main" => Tag::Main,
        "section" => Tag::Section,
        "div" => Tag::Div,
        "span" => Tag::Span,

        "h1" => Tag::Heading(1),
        "h2" => Tag::Heading(2),
        "h3" => Tag::Heading(3),
        "h4" => Tag::Heading(4),
        "h5" => Tag::Heading(5),
        "h6" => Tag::Heading(6),

        "p" => Tag::P,
        "br" => Tag::Br,
        "hr" => Tag::Hr,

        "ul" | "menu" => Tag::Ul,
        "ol" => Tag::Ol,
        "li" => Tag::Li,
        "blockquote" => Tag::Blockquote,
        "pre" => Tag::Pre,
        "code" | "kbd" | "samp" | "tt" => Tag::Code,

        "table" => Tag::Table,
        "tr" => Tag::Tr,
        "th" => Tag::Th,
        "td" => Tag::Td,

        "a" => Tag::A,
        "img" => Tag::Img,

        "em" | "i" | "cite" | "dfn" | "var" => Tag::Em,
        "strong" | "b" => Tag::Strong,

        "figure" | "figcaption" | "dl" | "dt" | "dd" | "address" | "details" | "summary"
        | "hgroup" | "center" | "fieldset" | "caption" | "thead" | "tbody" | "tfoot"
        | "colgroup" => Tag::Block,

        _ => Tag::Other,
    })
}

/// The text bytes under a subtree we are about to drop.
///
/// Only used for the dropped byte signal, so it walks the html5ever tree
/// directly rather than paying to convert a subtree we do not want.
fn dropped_bytes(handle: &Handle) -> u32 {
    let mut total = 0u32;
    let mut stack = vec![handle.clone()];
    let mut budget = 100_000u32;
    while let Some(node) = stack.pop() {
        if budget == 0 {
            break;
        }
        budget -= 1;
        if let NodeData::Text { contents } = &node.data {
            total = total.saturating_add(contents.borrow().len() as u32);
        }
        for child in node.children.borrow().iter() {
            stack.push(child.clone());
        }
    }
    total
}
