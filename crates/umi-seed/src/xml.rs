//! The reader configuration and the text handling both XML seed formats share.
//!
//! Sitemaps and feeds are different documents with the same shape underneath:
//! a stream of elements from a site we have never met, where the only things
//! worth reading are a URL and a date. Both use the same reader settings and
//! both have to put a `<loc>` back together from the pieces the parser hands
//! over, so that lives here instead of twice.

use std::borrow::Cow;

use quick_xml::Reader;
use quick_xml::escape::resolve_predefined_entity;
use quick_xml::events::Event;

/// A reader configured for documents nobody vouched for.
///
/// Three settings, and each of them is about hostile or merely bad input
/// rather than about taste.
///
/// `check_end_names` is off because keeping it on means the reader holds a
/// stack of every open element name, and a 50 MB document of nothing but
/// `<a>` repeated is then 50 MB of input that costs several times that in
/// names. Depth is counted here instead, which costs one integer, and a
/// mismatched close tag in a sitemap is not information anybody was going to
/// act on.
///
/// `allow_dangling_amp` is on because a bare `&` in a query string is the most
/// common mistake in real sitemaps by a wide margin. It is invalid XML and a
/// strict reader is entitled to stop at it, but stopping throws away every URL
/// after the first one that came out of a templating engine with no escaping
/// in it.
///
/// `expand_empty_elements` is on so that `<loc/>` arrives as a start and an end
/// rather than as one event the depth counter has to special case.
pub(crate) fn reader(bytes: &[u8]) -> Reader<&[u8]> {
    let mut reader = Reader::from_reader(bytes);
    let config = reader.config_mut();
    config.check_end_names = false;
    config.allow_dangling_amp = true;
    config.expand_empty_elements = true;
    reader
}

/// The text an event contributes to the element being read, if any.
///
/// Character data arrives in pieces. `https://example.com/?a=1&amp;b=2` inside
/// a `<loc>` is three events, two runs of text with an entity reference
/// between them, so a reader that takes the first text event gets half a URL.
/// Every piece goes through here and the caller joins them.
///
/// An entity nobody has heard of contributes nothing rather than failing the
/// document. The five XML entities and numeric character references are the
/// only ones that can appear without a DTD to define them, and a sitemap with
/// `&nbsp;` in a URL has a bug that is not ours to guess at.
pub(crate) fn chunk<'i>(event: &Event<'i>) -> Option<Cow<'i, str>> {
    match event {
        Event::Text(e) => Some(e.xml10_content()),
        Event::CData(e) => Some(e.xml10_content()),
        Event::GeneralRef(e) => {
            if let Ok(Some(c)) = e.resolve_char_ref() {
                return Some(Cow::Owned(c.to_string()));
            }
            resolve_predefined_entity(e.xml10_content().as_ref()).map(Cow::Borrowed)
        }
        _ => None,
    }
}
