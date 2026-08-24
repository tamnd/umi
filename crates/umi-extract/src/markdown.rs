//! Serialise a subtree to the fixed CommonMark subset in doc 11.3.
//!
//! Fixed is the important word. This is not "produce nice markdown", it is
//! "produce the same bytes on every machine forever", so every choice that a
//! prettier serialiser would leave to taste is nailed down here: blocks are
//! separated by exactly one blank line, lists always start at 1, emphasis is
//! always `*` and never `_`, and the escape set does not depend on context.
//! Where the pretty answer and the deterministic answer disagree, the
//! deterministic answer wins and the reason is written down next to it.

use url::Url;

use crate::dom::{Dom, Element, Kind, Tag};

/// Characters that get a backslash whenever they appear in text.
///
/// A context sensitive escaper emits fewer backslashes: `_` in the middle of a
/// word is not emphasis, `*` with a space after it is not a bullet. Context
/// sensitive also means the output depends on what came before, which is one
/// more thing that can differ between two implementations of this document. The
/// set is fixed, the cost is some backslashes in snake_case identifiers, and
/// `plain_text` removes them again.
const ESCAPED: [char; 8] = ['\\', '`', '*', '_', '[', ']', '<', '&'];

/// Serialise a subtree.
///
/// `base` is the document base, already resolved from `<base href>` against the
/// final URL by the caller, and every `href` and `src` is resolved against it.
/// Links that do not resolve keep their text and lose their destination, which
/// is better than emitting a relative link nobody downstream can use.
pub fn render(dom: &Dom, root: usize, base: Option<&Url>) -> String {
    let mut out = Writer::new(dom, base);
    out.walk_children(root);
    out.finish()
}

struct Writer<'a> {
    dom: &'a Dom,
    base: Option<&'a Url>,
    blocks: Vec<String>,
    inline: String,
}

impl<'a> Writer<'a> {
    fn new(dom: &'a Dom, base: Option<&'a Url>) -> Self {
        Self {
            dom,
            base,
            blocks: Vec::new(),
            inline: String::new(),
        }
    }

    fn finish(mut self) -> String {
        self.flush();
        self.blocks.join("\n\n")
    }

    /// End the paragraph being built, if there is one.
    fn flush(&mut self) {
        let text = self.inline.trim();
        if text.is_empty() {
            self.inline.clear();
            return;
        }
        let mut block = String::with_capacity(text.len() + 1);
        // A paragraph starting with `#`, `-` or `1.` would parse back as a
        // heading or a list, so the first character is escaped even though the
        // same character mid line is left alone.
        if starts_a_block(text) {
            block.push('\\');
        }
        block.push_str(text);
        self.blocks.push(block);
        self.inline.clear();
    }

    fn walk_children(&mut self, id: usize) {
        // The tree outlives the writer, so copying the reference out of the
        // struct hands the borrow checker a lifetime that does not overlap the
        // `&mut self` the walk needs.
        let dom = self.dom;
        for &child in dom.children(id) {
            self.walk(child);
        }
    }

    fn walk(&mut self, id: usize) {
        let dom = self.dom;
        match dom.kind(id) {
            Kind::Root => self.walk_children(id),
            Kind::Text(raw) => self.text(raw),
            Kind::Element(element) => self.element(id, element),
        }
    }

    fn element(&mut self, id: usize, element: &Element) {
        match element.tag {
            // Head and its contents carry metadata, never body text. `<title>`
            // is read by the metadata pass and must not also land in the
            // markdown as a stray first paragraph.
            Tag::Head | Tag::Title | Tag::Meta | Tag::Link | Tag::Base => {}

            Tag::Span | Tag::Other => self.walk_children(id),
            Tag::Br => self.space(),

            Tag::A => self.link(id, element),
            Tag::Img => self.image(element),
            Tag::Em => self.wrap(id, "*"),
            Tag::Strong => self.wrap(id, "**"),
            Tag::Code => self.code_span(id),

            Tag::Heading(level) => {
                self.flush();
                let text = self.inline_of(id);
                if !text.is_empty() {
                    let hashes = "#".repeat(usize::from(level));
                    self.blocks.push(format!("{hashes} {text}"));
                }
            }
            Tag::Hr => {
                self.flush();
                self.blocks.push("---".to_owned());
            }
            Tag::Pre => self.pre(id),
            Tag::Ul | Tag::Ol => self.list(id, element.tag == Tag::Ol),
            Tag::Blockquote => {
                self.flush();
                let inner = self.render_children(id);
                if !inner.is_empty() {
                    self.blocks.push(quote(&inner));
                }
            }
            Tag::Table => self.table(id),

            // A list item or a row outside its container is malformed HTML that
            // html5ever hands back as is. Treat it as a plain block so the text
            // is not lost.
            Tag::Li | Tag::Tr | Tag::Th | Tag::Td => {
                self.flush();
                self.walk_children(id);
                self.flush();
            }

            Tag::Html | Tag::Body | Tag::Article | Tag::Main | Tag::Section | Tag::Div
            | Tag::P | Tag::Block => {
                self.flush();
                self.walk_children(id);
                self.flush();
            }
        }
    }

    /// Append text with whitespace collapsed and markdown characters escaped.
    ///
    /// The leading and trailing checks are what keeps `<b>one</b> <b>two</b>`
    /// from becoming `**one****two**`. The space between those two elements is
    /// its own text node holding nothing but a space, so a version of this that
    /// only iterated words would emit nothing for it.
    fn text(&mut self, raw: &str) {
        if raw.starts_with(|c: char| c.is_ascii_whitespace()) {
            self.space();
        }
        for (n, word) in raw.split_ascii_whitespace().enumerate() {
            if n > 0 {
                self.space();
            }
            for ch in word.chars() {
                if ESCAPED.contains(&ch) {
                    self.inline.push('\\');
                }
                self.inline.push(ch);
            }
        }
        if raw.ends_with(|c: char| c.is_ascii_whitespace()) {
            self.space();
        }
    }

    /// A separator between two inline runs, collapsed against what is already
    /// there so that `<b>a</b> <i>b</i>` does not become two spaces.
    fn space(&mut self) {
        if !self.inline.is_empty() && !self.inline.ends_with(' ') {
            self.inline.push(' ');
        }
    }

    fn link(&mut self, id: usize, element: &Element) {
        let text = self.inline_of(id);
        if text.is_empty() {
            return;
        }
        match element.attr("href").and_then(|href| self.resolve(href)) {
            Some(url) => self.inline.push_str(&format!("[{text}]({url})")),
            None => self.inline.push_str(&text),
        }
    }

    fn image(&mut self, element: &Element) {
        let alt = collapse(&element.attr("alt").map(escape).unwrap_or_default());
        match element.attr("src").and_then(|src| self.resolve(src)) {
            Some(url) => self.inline.push_str(&format!("[{alt}]({url})")),
            None if !alt.is_empty() => self.inline.push_str(&alt),
            None => {}
        }
    }

    /// Emphasis, which is dropped entirely when it wraps nothing. `<strong></strong>`
    /// is common in templates and `****` is not markdown.
    fn wrap(&mut self, id: usize, marker: &str) {
        let text = self.inline_of(id);
        if text.is_empty() {
            return;
        }
        self.inline.push_str(marker);
        self.inline.push_str(&text);
        self.inline.push_str(marker);
    }

    fn code_span(&mut self, id: usize) {
        let raw = collapse(&self.raw_of(id));
        if raw.is_empty() {
            return;
        }
        let ticks = "`".repeat(longest_run(&raw, '`') + 1);
        // CommonMark strips one leading and one trailing space from a code
        // span, so content that starts or ends with a backtick needs the pad
        // and gets it back.
        let pad = if raw.starts_with('`') || raw.ends_with('`') {
            " "
        } else {
            ""
        };
        self.inline
            .push_str(&format!("{ticks}{pad}{raw}{pad}{ticks}"));
    }

    fn pre(&mut self, id: usize) {
        self.flush();
        let body = self.raw_of(id);
        let body = body.trim_matches('\n');
        if body.trim().is_empty() {
            return;
        }
        let fence = "`".repeat(longest_run(body, '`').max(2) + 1);
        let language = self.language(id).unwrap_or_default();
        self.blocks
            .push(format!("{fence}{language}\n{body}\n{fence}"));
    }

    /// The language for a fenced block, from `class="language-rust"` on the
    /// `<pre>` or on a `<code>` inside it, which is where both Pygments and
    /// highlight.js put it.
    fn language(&self, id: usize) -> Option<String> {
        for node in descendants(self.dom, id) {
            if let Some(element) = self.dom.element(node)
                && let Some(class) = element.attr("class")
            {
                for token in class.split_ascii_whitespace() {
                    let token = token.to_ascii_lowercase();
                    for prefix in ["language-", "lang-", "highlight-source-"] {
                        if let Some(name) = token.strip_prefix(prefix)
                            && !name.is_empty()
                            && name
                                .chars()
                                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-')
                        {
                            return Some(name.to_owned());
                        }
                    }
                }
            }
        }
        None
    }

    fn list(&mut self, id: usize, ordered: bool) {
        self.flush();
        let dom = self.dom;
        let mut items: Vec<String> = Vec::new();
        let mut number = 1usize;
        for &child in dom.children(id) {
            // Anything that is not an item is skipped rather than promoted.
            // Text directly inside a `<ul>` is almost always whitespace, and the
            // rest is template debris.
            match dom.tag(child) {
                Some(Tag::Li) => {}
                Some(Tag::Ul | Tag::Ol) => {
                    // A nested list that is a sibling of the items rather than
                    // inside one. Render it into the previous item, which is
                    // where a browser shows it.
                    let inner = self.render_children(child);
                    if let Some(last) = items.last_mut()
                        && !inner.is_empty()
                    {
                        last.push_str("\n\n");
                        last.push_str(&indent(&inner, "  ", "  "));
                    }
                    continue;
                }
                _ => continue,
            }
            let inner = self.render_children(child);
            if inner.is_empty() {
                // An empty item does not take a number with it, so a list whose
                // second item is a spacer still reads 1, 2, 3.
                continue;
            }
            let marker = if ordered {
                format!("{number}. ")
            } else {
                "- ".to_owned()
            };
            number += 1;
            let hanging = " ".repeat(marker.len());
            items.push(indent(&inner, &marker, &hanging));
        }
        if !items.is_empty() {
            self.blocks.push(items.join("\n"));
        }
    }

    fn table(&mut self, id: usize) {
        self.flush();
        let dom = self.dom;
        let mut rows: Vec<Vec<String>> = Vec::new();
        for node in descendants(dom, id) {
            if dom.tag(node) != Some(Tag::Tr) {
                continue;
            }
            let cells: Vec<String> = dom
                .children(node)
                .iter()
                .filter(|&&cell| matches!(dom.tag(cell), Some(Tag::Th | Tag::Td)))
                .map(|&cell| self.inline_of(cell).replace('|', "\\|"))
                .collect();
            if !cells.is_empty() {
                rows.push(cells);
            }
        }
        if rows.is_empty() {
            return;
        }
        let width = rows.iter().map(Vec::len).max().unwrap_or(0);
        let mut lines = Vec::with_capacity(rows.len() + 1);
        for (n, row) in rows.iter().enumerate() {
            let mut cells = row.clone();
            cells.resize(width, String::new());
            lines.push(format!("| {} |", cells.join(" | ")));
            if n == 0 {
                let rule = vec!["---"; width];
                lines.push(format!("| {} |", rule.join(" | ")));
            }
        }
        self.blocks.push(lines.join("\n"));
    }

    /// The children of a node rendered as blocks, for a list item or a quote.
    fn render_children(&self, id: usize) -> String {
        let mut inner = Writer::new(self.dom, self.base);
        inner.walk_children(id);
        inner.finish()
    }

    /// The children of a node rendered as one line, for a heading, a table cell
    /// or the text of a link. Nested blocks are joined with a space rather than
    /// dropped, because a heading wrapping two paragraphs is still a heading.
    fn inline_of(&self, id: usize) -> String {
        let rendered = self.render_children(id);
        collapse(&rendered.replace('\n', " "))
    }

    /// Every text node under a subtree, verbatim, for `<pre>` and code spans.
    fn raw_of(&self, id: usize) -> String {
        let mut out = String::new();
        for node in descendants(self.dom, id) {
            if let Kind::Text(raw) = self.dom.kind(node) {
                out.push_str(raw);
            }
        }
        out
    }

    /// Resolve a link against the document base.
    ///
    /// `javascript:`, `mailto:` and `data:` are not dropped here. They are not
    /// crawlable and the link pass filters them, but the markdown is a record of
    /// what the page said, and a `mailto:` in the body of a contact page is
    /// content.
    fn resolve(&self, href: &str) -> Option<String> {
        let href = href.trim();
        if href.is_empty() || href.starts_with('#') {
            return None;
        }
        let resolved = match self.base {
            Some(base) => base.join(href).ok()?,
            None => Url::parse(href).ok()?,
        };
        // A parsed URL has no spaces, angle brackets or backslashes left in it,
        // they are all percent encoded by now. Parentheses are legal in a path
        // and would close the link, so they are the only thing to escape.
        let mut out = String::with_capacity(resolved.as_str().len());
        for ch in resolved.as_str().chars() {
            if matches!(ch, '(' | ')') {
                out.push('\\');
            }
            out.push(ch);
        }
        Some(out)
    }
}

/// A subtree in document order, including the node itself.
///
/// Iterative, because the tree is capped at `dom::MAX_DEPTH` but a recursive
/// walk of even that is a stack frame per level on a page built to have them.
fn descendants(dom: &Dom, id: usize) -> Vec<usize> {
    let mut order = Vec::new();
    let mut stack = vec![id];
    while let Some(node) = stack.pop() {
        order.push(node);
        stack.extend(dom.children(node).iter().rev());
    }
    order
}

/// Whether a line would parse as something other than a paragraph.
fn starts_a_block(text: &str) -> bool {
    let bytes = text.as_bytes();
    match bytes.first() {
        Some(b'#' | b'>' | b'-' | b'+' | b'=' | b'|' | b'~' | b':') => true,
        Some(b'0'..=b'9') => {
            let digits = bytes
                .iter()
                .take_while(|byte| byte.is_ascii_digit())
                .count();
            matches!(bytes.get(digits), Some(b'.' | b')'))
        }
        _ => false,
    }
}

/// Prefix a block's first line with `first` and the rest with `rest`.
fn indent(block: &str, first: &str, rest: &str) -> String {
    let mut out = String::with_capacity(block.len() + first.len());
    for (n, line) in block.split('\n').enumerate() {
        if n > 0 {
            out.push('\n');
        }
        if line.is_empty() {
            // A blank line inside a list item stays blank. Padding it with
            // spaces is invisible in a diff and shows up in a digest.
            continue;
        }
        out.push_str(if n == 0 { first } else { rest });
        out.push_str(line);
    }
    out
}

/// Prefix every line of a block with a quote marker.
fn quote(block: &str) -> String {
    let mut out = String::with_capacity(block.len() + block.len() / 8);
    for (n, line) in block.split('\n').enumerate() {
        if n > 0 {
            out.push('\n');
        }
        if line.is_empty() {
            out.push('>');
        } else {
            out.push_str("> ");
            out.push_str(line);
        }
    }
    out
}

/// Collapse runs of ASCII whitespace to one space and trim the ends.
fn collapse(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for (n, word) in text.split_ascii_whitespace().enumerate() {
        if n > 0 {
            out.push(' ');
        }
        out.push_str(word);
    }
    out
}

/// Escape the markdown characters in a string that never went through the
/// serialiser, which is `alt` text.
fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if ESCAPED.contains(&ch) {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// The longest run of one character, for choosing a fence that the content
/// cannot close.
fn longest_run(text: &str, needle: char) -> usize {
    let mut longest = 0usize;
    let mut run = 0usize;
    for ch in text.chars() {
        if ch == needle {
            run += 1;
            longest = longest.max(run);
        } else {
            run = 0;
        }
    }
    longest
}
