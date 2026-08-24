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

use html5ever::tendril::TendrilSink;
use html5ever::{ParseOpts, parse_document};
use markup5ever_rcdom::{Handle, NodeData, RcDom};

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
const KEPT_ATTRS: [&str; 15] = [
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
#[derive(Debug)]
pub struct Node {
    /// What the node is.
    pub kind: Kind,
    /// The parent, absent only for the root.
    pub parent: Option<usize>,
    /// Children in document order.
    pub children: Vec<usize>,
}

/// A parsed document.
#[derive(Debug)]
pub struct Dom {
    nodes: Vec<Node>,
    dropped: u32,
}

impl Dom {
    /// Parse a document.
    ///
    /// The bytes are decoded as UTF-8 with invalid sequences replaced, which is
    /// what html5ever's `from_utf8` does and what a browser does. Transcoding
    /// from a declared charset happens in the fetch path before this, because
    /// the charset comes off the Content-Type header as often as it comes off a
    /// `<meta>` tag and only the caller has both.
    pub fn parse(html: &[u8]) -> Self {
        let parsed = parse_document(RcDom::default(), ParseOpts::default())
            .from_utf8()
            .one(html);
        let mut dom = Self {
            nodes: vec![Node {
                kind: Kind::Root,
                parent: None,
                children: Vec::new(),
            }],
            dropped: 0,
        };
        dom.absorb(&parsed.document);
        dom
    }

    /// Convert the html5ever tree into the arena.
    ///
    /// Iterative rather than recursive, because the input is hostile by
    /// definition and 50000 nested tags would otherwise be a stack overflow
    /// before `MAX_DEPTH` had a chance to apply. Children are pushed in reverse
    /// so that popping visits them in document order.
    fn absorb(&mut self, document: &Handle) {
        let mut stack: Vec<(Handle, usize, u32)> = vec![(document.clone(), ROOT, 0)];
        while let Some((handle, parent, depth)) = stack.pop() {
            let (kind, keep_children) = match &handle.data {
                NodeData::Document => (None, true),
                NodeData::Text { contents } => {
                    let text = contents.borrow().to_string();
                    (Some(Kind::Text(text)), false)
                }
                NodeData::Element { name, attrs, .. } => {
                    let local = name.local.as_ref();
                    match classify(local) {
                        None => {
                            self.dropped += dropped_bytes(&handle);
                            continue;
                        }
                        Some(tag) => {
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
                        parent: Some(parent),
                        children: Vec::new(),
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
                        parent: Some(parent),
                        children: Vec::new(),
                    });
                    self.nodes[parent].children.push(me);
                    (me, depth)
                }
                Some(_) => (parent, depth),
                None => (parent, depth),
            };

            if keep_children {
                for child in handle.children.borrow().iter().rev() {
                    stack.push((child.clone(), me, next_depth));
                }
            }
        }
    }

    /// How many nodes the arena holds. Never zero, because the root is always
    /// there, which is why this is not called `len`.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// What a node is.
    pub fn kind(&self, id: usize) -> &Kind {
        &self.nodes[id].kind
    }

    /// A node's children.
    pub fn children(&self, id: usize) -> &[usize] {
        &self.nodes[id].children
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
    pub fn dropped_bytes(&self) -> u32 {
        self.dropped
    }
}

/// Map a tag name to a rule, or to `None` for the subtrees doc 11.3 drops.
///
/// `textarea` and `option` are dropped alongside the form controls doc 11.3
/// names. They are the same class of thing and their text is chrome, not
/// content. That is a deviation from the letter of the list and it is recorded
/// here rather than being silent.
fn classify(name: &str) -> Option<Tag> {
    Some(match name {
        "script" | "style" | "noscript" | "svg" | "canvas" | "iframe" | "object" | "embed"
        | "form" | "input" | "button" | "select" | "nav" | "header" | "footer" | "aside"
        | "textarea" | "option" | "template" => return None,

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
