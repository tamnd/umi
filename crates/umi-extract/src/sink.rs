//! The tree html5ever builds into: a flat arena of nodes rather than a graph of
//! reference counted ones.
//!
//! html5ever hands the tree it builds to whatever implements [`TreeSink`], and
//! until now that was `markup5ever_rcdom`, which gives every node its own
//! allocation, an `Rc` around it, a `RefCell` around its children and a `Weak`
//! back to its parent. `dom.rs` then walks that whole tree once into the flat
//! arena everything downstream reads, and drops the first tree on the floor. So
//! every page paid for two trees, one of them reference counted, and for a
//! recursive drop of the one that was thrown away.
//!
//! A profile of a five minute crawl on server3 put html extraction at 30.75
//! percent of the process, the allocator at 12.15 and `memmove` plus `memcmp` at
//! 9.83, against 0.73 percent for the whole of the crawl loop, the frontier and
//! the state layer together. `markup5ever_rcdom::Node::drop` on its own was 0.87
//! percent, which is more than the crawl loop. The measurement is on issue #250.
//!
//! This is the same tree in a `Vec`. A node is an index, the children are a
//! `Vec<Id>`, the parent is an `Option<Id>`, and the whole thing frees in one
//! deallocation instead of a hundred thousand. `dom.rs` still walks it once,
//! because that walk is what applies doc 11.3's drop list and what guarantees a
//! node's index is greater than every index before it in document order, and
//! that guarantee is what `score.rs` and `markdown.rs` rely on when they sweep
//! `0..node_count()` backwards to accumulate a subtree. A sink that appended
//! straight into the final arena could not promise it, because foster parenting
//! and the adoption agency algorithm both move a node that already exists to a
//! place earlier in the document.
//!
//! # This is rcdom's behaviour, deliberately
//!
//! Doc 11.1 promises extraction output is byte identical forever, so the rules
//! here are rcdom's rules, ported operation for operation rather than rewritten
//! from the spec. `append_before_sibling` computes the insertion index before it
//! detaches the node it is about to insert, which matters when the node is
//! already a child of the same parent, and it does that here too. Text is merged
//! into the previous sibling under exactly rcdom's three conditions.
//! `add_attrs_if_missing` tests incoming names against the attributes that were
//! there when the call started rather than against the list as it grows.
//!
//! Three things are dropped rather than stored, and none of them is readable
//! from the arena, so none of them can change the output. Parse errors are
//! counted by nobody, so they are not collected. The text of a comment, a
//! doctype and a processing instruction is never looked at, so the node is kept
//! as a placeholder, to hold its position among its siblings, and the text is
//! not. The quirks mode is set by the tree builder and read back by nothing.
//!
//! One thing is deliberately not implemented, and it is the reason this file
//! existed before it held a DOM. The tree builder calls
//! `maybe_clone_an_option_into_selectedcontent` once for every `<option>`
//! element it closes, and rcdom's implementation walks the entire subtree of the
//! enclosing `<select>` cloning an `Rc` for every node it passes. A country
//! dropdown has two hundred and fifty options and a product page can have
//! thousands. Profiling a real crawl at concurrency 1024 put those two functions
//! at 17.7 percent of every cycle the process spent, the largest single entry,
//! ahead of allocation and ahead of the tokeniser. None of that work can change
//! the tree: the search loop in rcdom 0.39 matches the local name of the
//! `<select>` it started from rather than the local name of the node it is
//! looking at, so the name it compares against `selectedcontent` is always
//! `select`, the loop never finds anything, and the function it guards is never
//! reached. The trait's own default is empty and that is what we take.

use std::cell::RefCell;

use html5ever::interface::{ElemName, ElementFlags, NodeOrText, QuirksMode, TreeSink};
use html5ever::tendril::StrTendril;
use html5ever::{Attribute, LocalName, Namespace, QualName};

/// A node, by position in the arena.
///
/// `u32` rather than `usize` because doc 05.4 caps a stored body at 512 KiB and
/// a node needs more than one byte of source, so four billion of them is not a
/// number this can reach. Halving the width of every parent pointer and every
/// entry in every child list is worth more than the range.
pub type Id = u32;

/// The document node, which every tree has and which is always first.
pub const DOCUMENT: Id = 0;

/// What a node is.
///
/// Nothing here corresponds to rcdom's `Comment`, `Doctype` or
/// `ProcessingInstruction`. All three are [`Data::Ignored`], because the arena
/// skips them and no pass downstream can ask for their text.
pub enum Data {
    /// The root. Also the contents of a `<template>`, which the tree builder
    /// asks for by handle and which hangs off no parent.
    Document,
    /// A run of text, still in the tendril html5ever handed over. Converted to a
    /// `String` only if it survives the drop list, which is why a `<script>`
    /// body is never copied.
    Text(StrTendril),
    /// An element.
    Element(Element),
    /// A comment, a doctype or a processing instruction.
    ///
    /// Kept as a node rather than skipped so that it still occupies a position
    /// in its parent's child list. `append_before_sibling` inserts at an index
    /// into that list, and a list missing entries would put a foster parented
    /// node in the wrong place.
    Ignored,
}

/// An element, with the attributes exactly as the tokeniser read them.
///
/// Filtering to the attributes doc 11.3 keeps happens in `dom.rs`, on the
/// elements that survive the drop list, rather than here on all of them.
pub struct Element {
    /// The tag name.
    pub name: QualName,
    /// The attributes, in document order.
    pub attrs: Vec<Attribute>,
    /// For a `<template>`, the document node holding its contents.
    template: Option<Id>,
    /// Whether this is a MathML annotation-xml HTML integration point, which the
    /// tree builder sets on the way in and asks about later.
    mathml: bool,
}

/// One node.
pub struct Node {
    /// What the node is.
    pub data: Data,
    /// Children in document order.
    pub children: Vec<Id>,
    /// The parent, or `None` for the document and for a node the tree builder
    /// has created but not yet placed.
    parent: Option<Id>,
}

/// A parsed document, after html5ever has finished with it.
///
/// The same nodes the [`Sink`] built, with the `RefCell` gone, because nothing
/// mutates the tree once the parse is over.
pub struct Tree {
    nodes: Vec<Node>,
}

impl Tree {
    /// A node.
    ///
    /// Panics on an index this tree does not have, which cannot happen: the only
    /// indices in circulation come from this tree's own child lists.
    pub fn node(&self, id: Id) -> &Node {
        &self.nodes[id as usize]
    }

    /// How many nodes the parse produced, counting the document node.
    ///
    /// The arena `dom.rs` builds next holds a subset of these, so this is an
    /// exact upper bound on it and the one number worth sizing that arena from.
    /// Never zero, because the document node is always there, which is why this
    /// is not called `len`.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

/// The name of an element, owned.
///
/// [`TreeSink::elem_name`] hands back a borrow of the name in the tree, and a
/// tree behind a `RefCell` cannot lend one out. The name is two interned atoms
/// and a prefix, so a clone is three word copies and at worst a refcount, and
/// the tree builder asks for it once per tag name comparison.
#[derive(Debug)]
pub struct Name(QualName);

impl ElemName for Name {
    fn ns(&self) -> &Namespace {
        &self.0.ns
    }

    fn local_name(&self) -> &LocalName {
        &self.0.local
    }
}

/// The tree under construction.
pub struct Sink {
    nodes: RefCell<Vec<Node>>,
}

impl Default for Sink {
    fn default() -> Self {
        Self::for_html(0)
    }
}

/// Bytes of html per node, over the ten thousand page wide golden corpus.
///
/// The whole corpus is 1,491,624,180 bytes and parses to 15,510,894 nodes, so
/// 96.17 bytes a node across the lot. Per page the median is 86, the lower
/// quartile 50 and the fifth percentile 33, which is the spread a single ratio
/// has to live with.
///
/// Sizing from the mean rather than from a low percentile is deliberate. What
/// this is buying is fewer doublings, and a doubling curve is forgiving: a page
/// that needs twice the guess pays one reallocation, one that needs four times
/// pays two. Guessing at the fifth percentile would spare those pages a single
/// further doubling each and would triple the reservation on every ordinary
/// page to do it. The mean puts the median page at one reallocation and the
/// lower quartile at one, down from the eleven or so a page takes growing from
/// nothing.
const BYTES_PER_NODE: usize = 96;

impl Sink {
    /// A sink sized for a document of `len` bytes.
    ///
    /// The arena is one contiguous `Vec` that doubles, so a page with a hundred
    /// thousand nodes copies its way there through seventeen reallocations and
    /// moves about eight megabytes doing it. `RcDom` never did that, because it
    /// allocated each node separately and copied nothing, and that is the one
    /// place the flat arena is worse. The input length is known before the parse
    /// starts and predicts the node count well enough to take most of it back.
    pub fn for_html(len: usize) -> Self {
        let mut nodes = Vec::with_capacity(len / BYTES_PER_NODE + 1);
        nodes.push(Node {
            data: Data::Document,
            children: Vec::new(),
            parent: None,
        });
        Self {
            nodes: RefCell::new(nodes),
        }
    }
}

/// Add a node with no parent and hand back its index.
fn make(nodes: &mut Vec<Node>, data: Data) -> Id {
    let id = nodes.len() as Id;
    nodes.push(Node {
        data,
        children: Vec::new(),
        parent: None,
    });
    id
}

/// Make `child` the last child of `parent`.
///
/// The child must not already have one, which is what html5ever promises for
/// every path that reaches here.
fn attach(nodes: &mut [Node], parent: Id, child: Id) {
    let previous = nodes[child as usize].parent.replace(parent);
    assert!(
        previous.is_none(),
        "attaching a node that already has a parent"
    );
    nodes[parent as usize].children.push(child);
}

/// A node's parent and its position in that parent's child list.
fn seat(nodes: &[Node], target: Id) -> Option<(Id, usize)> {
    let parent = nodes[target as usize].parent?;
    let at = nodes[parent as usize]
        .children
        .iter()
        .position(|&child| child == target)
        .expect("a node its parent does not list");
    Some((parent, at))
}

/// Take `target` out of its parent's child list, if it has one.
fn detach(nodes: &mut [Node], target: Id) {
    if let Some((parent, at)) = seat(nodes, target) {
        nodes[parent as usize].children.remove(at);
        nodes[target as usize].parent = None;
    }
}

/// Append to `prev` if it is a text node, and say whether it was.
///
/// html5ever merges adjacent text rather than producing two nodes, and it is the
/// sink's job to do the merging.
fn extend_text(nodes: &mut [Node], prev: Id, text: &StrTendril) -> bool {
    match &mut nodes[prev as usize].data {
        Data::Text(contents) => {
            contents.push_slice(text);
            true
        }
        _ => false,
    }
}

impl TreeSink for Sink {
    type Output = Tree;
    type Handle = Id;
    type ElemName<'a>
        = Name
    where
        Self: 'a;

    fn finish(self) -> Tree {
        Tree {
            nodes: self.nodes.into_inner(),
        }
    }

    /// Dropped. Nothing reads them and a page of broken markup produces
    /// thousands.
    fn parse_error(&self, _msg: std::borrow::Cow<'static, str>) {}

    fn get_document(&self) -> Id {
        DOCUMENT
    }

    fn get_template_contents(&self, target: &Id) -> Id {
        match &self.nodes.borrow()[*target as usize].data {
            Data::Element(element) => element.template.expect("not a template element"),
            _ => panic!("not a template element"),
        }
    }

    /// Dropped. The tree builder tracks its own quirks mode and never asks the
    /// sink for one back.
    fn set_quirks_mode(&self, _mode: QuirksMode) {}

    fn same_node(&self, x: &Id, y: &Id) -> bool {
        x == y
    }

    fn elem_name<'a>(&'a self, target: &'a Id) -> Name {
        match &self.nodes.borrow()[*target as usize].data {
            Data::Element(element) => Name(element.name.clone()),
            _ => panic!("not an element"),
        }
    }

    fn create_element(&self, name: QualName, attrs: Vec<Attribute>, flags: ElementFlags) -> Id {
        let nodes = &mut *self.nodes.borrow_mut();
        // The contents of a `<template>` are a document of their own, reachable
        // only through `get_template_contents`. It is never attached to
        // anything, so the arena walk never sees it.
        let template = flags.template.then(|| make(nodes, Data::Document));
        make(
            nodes,
            Data::Element(Element {
                name,
                attrs,
                template,
                mathml: flags.mathml_annotation_xml_integration_point,
            }),
        )
    }

    fn create_comment(&self, _text: StrTendril) -> Id {
        make(&mut self.nodes.borrow_mut(), Data::Ignored)
    }

    fn create_pi(&self, _target: StrTendril, _data: StrTendril) -> Id {
        make(&mut self.nodes.borrow_mut(), Data::Ignored)
    }

    fn append(&self, parent: &Id, child: NodeOrText<Id>) {
        let nodes = &mut *self.nodes.borrow_mut();
        let child = match child {
            NodeOrText::AppendText(text) => {
                if let Some(&last) = nodes[*parent as usize].children.last()
                    && extend_text(nodes, last, &text)
                {
                    return;
                }
                make(nodes, Data::Text(text))
            }
            NodeOrText::AppendNode(node) => node,
        };
        attach(nodes, *parent, child);
    }

    fn append_before_sibling(&self, sibling: &Id, new_node: NodeOrText<Id>) {
        let nodes = &mut *self.nodes.borrow_mut();
        let (parent, at) =
            seat(nodes, *sibling).expect("append_before_sibling on a node without a parent");

        let child = match new_node {
            NodeOrText::AppendText(text) => {
                // Nothing before the insertion point means nothing to merge
                // into. The tree builder promises the node after it is not text.
                if at > 0 {
                    let prev = nodes[parent as usize].children[at - 1];
                    if extend_text(nodes, prev, &text) {
                        return;
                    }
                }
                make(nodes, Data::Text(text))
            }
            NodeOrText::AppendNode(node) => node,
        };

        // `at` was read before this, and detaching a node that is already a
        // child of the same parent and sits earlier in the list shifts
        // everything after it down by one. rcdom has the same order and so the
        // same result, and matching it is the point.
        detach(nodes, child);
        nodes[child as usize].parent = Some(parent);
        nodes[parent as usize].children.insert(at, child);
    }

    fn append_based_on_parent_node(&self, element: &Id, prev_element: &Id, child: NodeOrText<Id>) {
        let placed = self.nodes.borrow()[*element as usize].parent.is_some();
        if placed {
            self.append_before_sibling(element, child);
        } else {
            self.append(prev_element, child);
        }
    }

    fn append_doctype_to_document(
        &self,
        _name: StrTendril,
        _public_id: StrTendril,
        _system_id: StrTendril,
    ) {
        let nodes = &mut *self.nodes.borrow_mut();
        let doctype = make(nodes, Data::Ignored);
        attach(nodes, DOCUMENT, doctype);
    }

    fn add_attrs_if_missing(&self, target: &Id, attrs: Vec<Attribute>) {
        let nodes = &mut *self.nodes.borrow_mut();
        let Data::Element(element) = &mut nodes[*target as usize].data else {
            panic!("not an element");
        };
        // Against the attributes that were there when the call started, not
        // against the list as it grows, which is rcdom's rule.
        let had = element.attrs.len();
        for attr in attrs {
            if !element.attrs[..had]
                .iter()
                .any(|have| have.name == attr.name)
            {
                element.attrs.push(attr);
            }
        }
    }

    fn remove_from_parent(&self, target: &Id) {
        detach(&mut self.nodes.borrow_mut(), *target);
    }

    fn reparent_children(&self, node: &Id, new_parent: &Id) {
        let nodes = &mut *self.nodes.borrow_mut();
        let moved = std::mem::take(&mut nodes[*node as usize].children);
        for &child in &moved {
            let previous = nodes[child as usize].parent.replace(*new_parent);
            assert_eq!(
                previous,
                Some(*node),
                "reparenting a child of somebody else"
            );
        }
        nodes[*new_parent as usize].children.extend(moved);
    }

    fn is_mathml_annotation_xml_integration_point(&self, target: &Id) -> bool {
        match &self.nodes.borrow()[*target as usize].data {
            Data::Element(element) => element.mathml,
            _ => panic!("not an element"),
        }
    }
}
