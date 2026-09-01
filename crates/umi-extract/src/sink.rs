//! The tree sink html5ever builds into, which is `RcDom` with one method taken
//! back out.
//!
//! `markup5ever_rcdom` implements the whole of `TreeSink`, and one of the
//! methods it implements is `maybe_clone_an_option_into_selectedcontent`. The
//! tree builder calls that once for every `<option>` element it closes, and the
//! implementation walks the entire subtree of the enclosing `<select>` looking
//! for a `<selectedcontent>` child, cloning an `Rc` for every node it passes on
//! the way. A country dropdown has two hundred and fifty options and a product
//! page can have thousands, so the cost is the size of the select subtree times
//! the number of options in it.
//!
//! On server3, profiling a real crawl at concurrency 1024, those two functions
//! were 17.7 percent of every cycle the process spent: 10.25 percent in the
//! `VecDeque` extend that does the cloning and 7.49 percent in the method that
//! drives it. That was the single largest entry in the profile, ahead of
//! allocation, ahead of the tokeniser and ahead of SQLite.
//!
//! None of that work can change the tree. The search loop in rcdom 0.39 matches
//! the local name of the `<select>` it started from rather than the local name
//! of the node it is looking at, so the name it compares against
//! `selectedcontent` is always `select`, the loop never finds anything, and the
//! function it guards is never reached. The walk runs to the end of the subtree
//! and returns `None` every time. Skipping it is not a behaviour change we are
//! choosing to accept, it is the removal of work that has no effect, and doc
//! 11.1's promise that extraction output is byte identical forever holds
//! exactly as it did.
//!
//! The trait's own default for the method is empty, so this is a delegate that
//! forwards every method rcdom implements and leaves that one alone. The
//! alternative is to stop using rcdom at all and build the arena straight off
//! the tokeniser, which is what `dom.rs` says the answer is if the parse ever
//! needs to be fast. It still is the answer. This is the part of it that is
//! worth having today, at eighty lines instead of six hundred, and it does not
//! make that change any harder.

use std::borrow::Cow;

use html5ever::interface::{ElementFlags, NodeOrText, QuirksMode, TreeSink};
use html5ever::tendril::StrTendril;
use html5ever::{Attribute, ExpandedName, QualName};
use markup5ever_rcdom::{Handle, RcDom};

/// An `RcDom` that does not chase `<selectedcontent>`.
#[derive(Default)]
pub struct Sink(RcDom);

impl TreeSink for Sink {
    type Output = RcDom;
    type Handle = Handle;
    type ElemName<'a>
        = ExpandedName<'a>
    where
        Self: 'a;

    fn finish(self) -> RcDom {
        self.0
    }

    fn parse_error(&self, msg: Cow<'static, str>) {
        self.0.parse_error(msg);
    }

    fn get_document(&self) -> Handle {
        self.0.get_document()
    }

    fn get_template_contents(&self, target: &Handle) -> Handle {
        self.0.get_template_contents(target)
    }

    fn set_quirks_mode(&self, mode: QuirksMode) {
        self.0.set_quirks_mode(mode);
    }

    fn same_node(&self, x: &Handle, y: &Handle) -> bool {
        self.0.same_node(x, y)
    }

    fn elem_name<'a>(&'a self, target: &'a Handle) -> ExpandedName<'a> {
        self.0.elem_name(target)
    }

    fn create_element(&self, name: QualName, attrs: Vec<Attribute>, flags: ElementFlags) -> Handle {
        self.0.create_element(name, attrs, flags)
    }

    fn create_comment(&self, text: StrTendril) -> Handle {
        self.0.create_comment(text)
    }

    fn create_pi(&self, target: StrTendril, data: StrTendril) -> Handle {
        self.0.create_pi(target, data)
    }

    fn append(&self, parent: &Handle, child: NodeOrText<Handle>) {
        self.0.append(parent, child);
    }

    fn append_before_sibling(&self, sibling: &Handle, child: NodeOrText<Handle>) {
        self.0.append_before_sibling(sibling, child);
    }

    fn append_based_on_parent_node(
        &self,
        element: &Handle,
        prev_element: &Handle,
        child: NodeOrText<Handle>,
    ) {
        self.0
            .append_based_on_parent_node(element, prev_element, child);
    }

    fn append_doctype_to_document(
        &self,
        name: StrTendril,
        public_id: StrTendril,
        system_id: StrTendril,
    ) {
        self.0
            .append_doctype_to_document(name, public_id, system_id);
    }

    fn add_attrs_if_missing(&self, target: &Handle, attrs: Vec<Attribute>) {
        self.0.add_attrs_if_missing(target, attrs);
    }

    fn remove_from_parent(&self, target: &Handle) {
        self.0.remove_from_parent(target);
    }

    fn reparent_children(&self, node: &Handle, new_parent: &Handle) {
        self.0.reparent_children(node, new_parent);
    }

    fn is_mathml_annotation_xml_integration_point(&self, target: &Handle) -> bool {
        self.0.is_mathml_annotation_xml_integration_point(target)
    }
}
