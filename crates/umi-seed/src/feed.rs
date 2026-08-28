//! RSS and Atom feeds, doc 13.6.
//!
//! A feed is the cheapest freshness signal on the web. It is small, it is
//! meant to be polled, and it lists what changed with the date it changed on,
//! which is the same pair a sitemap gives and it gives it far more often. Doc
//! 09 polls feeds for exactly that reason, and this is the parser that half
//! needs.
//!
//! Three formats, one reader. RSS 2.0 puts entries in `<item>` with the URL as
//! the text of `<link>`, Atom puts them in `<entry>` with the URL in the `href`
//! attribute of a `<link>`, and RSS 1.0 is RDF with `<item>` at the top level.
//! They differ in where the URL is written and in which of three date grammars
//! the date is written in, and none of that is worth three parsers, so one
//! reader accepts whichever of them a file turns out to be. Feeds in the wild
//! mix them constantly, and a reader that insists on one gets nothing from a
//! feed whose generator did not.
//!
//! What comes back is [`Entry`] values, the same type a sitemap produces, since
//! from here on a URL with a date on it is a URL with a date on it. The limits
//! are [`Caps`], for the same reason and with the same reasoning.

use crate::sitemap::{Caps, Entry};
use crate::xml::{chunk, reader};
use quick_xml::XmlVersion;
use quick_xml::events::Event;

/// What one feed turned out to contain.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct Feed {
    /// The entries, in the order the feed listed them, which is usually
    /// newest first and is not something to rely on.
    pub entries: Vec<Entry>,
    /// Whether a cap stopped the parse before the end of the document.
    pub truncated: bool,
    /// Whether the document stopped making sense before it ended.
    pub malformed: bool,
}

impl Feed {
    /// Read a feed with the default limits.
    #[must_use]
    pub fn parse(bytes: &[u8]) -> Self {
        Self::parse_with(bytes, &Caps::default())
    }

    /// Read one, with limits the caller chose.
    ///
    /// Gzip is unwrapped first, on the same terms as [`Sitemap::parse_with`].
    /// Feeds are served compressed far less often than sitemaps are, but a
    /// caller that has a document and does not yet know which of the two it is
    /// should not have to care.
    ///
    /// [`Sitemap::parse_with`]: crate::Sitemap::parse_with
    #[must_use]
    pub fn parse_with(bytes: &[u8], caps: &Caps) -> Self {
        if crate::gzip::is_gzip(bytes) {
            let (inflated, truncated) = crate::gzip::inflate(bytes, caps.max_bytes);
            let mut out = Self::read(&inflated, caps);
            out.truncated |= truncated;
            return out;
        }
        Self::read(bytes, caps)
    }

    /// The parse itself, on bytes that are already plain.
    fn read(bytes: &[u8], caps: &Caps) -> Self {
        let mut out = Self::default();
        let bytes = if bytes.len() > caps.max_bytes {
            out.truncated = true;
            &bytes[..caps.max_bytes]
        } else {
            bytes
        };

        let mut reader = reader(bytes);
        let mut depth = 0usize;
        let mut open: Option<usize> = None;
        let mut field: Option<Field> = None;
        let mut buf = String::new();
        let mut item = Item::default();

        loop {
            let event = match reader.read_event() {
                Ok(event) => event,
                Err(_) => {
                    out.malformed = !out.truncated;
                    return out;
                }
            };
            match event {
                Event::Eof => return out,
                Event::Start(e) => {
                    let local = e.local_name();
                    let name: &str = local.as_ref();
                    depth += 1;
                    match open {
                        None => {
                            if name == "item" || name == "entry" {
                                open = Some(depth);
                                item = Item::default();
                            }
                        }
                        Some(at) if depth == at + 1 => {
                            field = match name {
                                // Atom writes the URL in an attribute and
                                // leaves the element empty, RSS writes it as
                                // the text, so the attributes are read here
                                // and the text is read at the end tag. A feed
                                // that does both gets the attribute, because
                                // an Atom link with text in it is decoration.
                                "link" => match href(&e) {
                                    Href::Alternate(url) => {
                                        item.link.get_or_insert(url);
                                        None
                                    }
                                    Href::Elsewhere => None,
                                    Href::InTheText => Some(Field::Link),
                                },
                                // `isPermaLink` defaults to true, which is the
                                // one place RSS makes the useful case the
                                // default.
                                "guid"
                                    if !attr(&e, "isPermaLink").is_some_and(|v| v == "false") =>
                                {
                                    Some(Field::Guid)
                                }
                                "updated" | "pubDate" => Some(Field::Updated),
                                "published" | "date" => Some(Field::Published),
                                _ => None,
                            };
                            buf.clear();
                        }
                        Some(_) => {}
                    }
                }
                Event::End(_) => {
                    match open {
                        Some(at) if depth == at => {
                            open = None;
                            // The link is the URL. A `guid` is a fallback and
                            // not a good one, because half the feeds on the web
                            // put a tag URI in it, so it is used only when
                            // there is no link at all and it is left to
                            // canonicalisation to throw out the ones that are
                            // not URLs.
                            if let Some(url) = item.link.take().or_else(|| item.guid.take()) {
                                out.entries.push(Entry {
                                    url,
                                    lastmod_ms: item.updated.or(item.published),
                                });
                                if out.entries.len() >= caps.max_urls {
                                    out.truncated = true;
                                    return out;
                                }
                            }
                        }
                        Some(at) if depth == at + 1 => {
                            let text = buf.trim();
                            match field.take() {
                                Some(Field::Link)
                                    if !text.is_empty() && text.len() <= caps.max_loc =>
                                {
                                    item.link.get_or_insert_with(|| text.to_owned());
                                }
                                Some(Field::Guid)
                                    if !text.is_empty() && text.len() <= caps.max_loc =>
                                {
                                    item.guid.get_or_insert_with(|| text.to_owned());
                                }
                                Some(Field::Updated) => {
                                    item.updated = item.updated.or_else(|| crate::date::any(text));
                                }
                                Some(Field::Published) => {
                                    item.published =
                                        item.published.or_else(|| crate::date::any(text));
                                }
                                _ => {}
                            }
                            buf.clear();
                        }
                        _ => {}
                    }
                    depth = depth.saturating_sub(1);
                }
                Event::Text(_) | Event::CData(_) | Event::GeneralRef(_) if field.is_some() => {
                    // One byte past the cap is enough to know it is over it,
                    // and stopping there is what keeps a single element from
                    // being a way to spend memory.
                    if let Some(text) = chunk(&event)
                        && buf.len() <= caps.max_loc
                    {
                        buf.push_str(&text);
                    }
                }
                _ => {}
            }
        }
    }

    /// Whether the feed yielded nothing at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// One entry while it is still being read.
#[derive(Default)]
struct Item {
    link: Option<String>,
    guid: Option<String>,
    updated: Option<u64>,
    published: Option<u64>,
}

/// Which child of an entry we are inside.
#[derive(Clone, Copy)]
enum Field {
    Link,
    Guid,
    Updated,
    Published,
}

/// What a `<link>` element turned out to be.
enum Href {
    /// An Atom link to the entry itself.
    Alternate(String),
    /// An Atom link to something else.
    Elsewhere,
    /// No `href` at all, so it is the RSS form and the URL is the text.
    InTheText,
}

/// Which of the three a `<link>` is.
///
/// `rel` is optional in Atom and defaults to `alternate`, so a link with no
/// `rel` is the page. Everything else points at something that is not the
/// entry: `self` is the feed, `enclosure` is a podcast episode, `replies` is a
/// comment thread, and seeding those from here would be crawling things nobody
/// asked for.
fn href(e: &quick_xml::events::BytesStart<'_>) -> Href {
    let Some(url) = attr(e, "href").filter(|href| !href.is_empty()) else {
        return Href::InTheText;
    };
    if attr(e, "rel").is_some_and(|rel| rel != "alternate") {
        return Href::Elsewhere;
    }
    Href::Alternate(url)
}

/// One attribute by local name, with its escapes resolved.
///
/// Normalising is what the XML specification calls this: it turns `&amp;` back
/// into `&` and folds the tabs and newlines an attribute is allowed to be
/// written across into spaces. An `href` with a bare `&` in it is common
/// enough that a failure here is a URL to skip rather than a document to
/// give up on.
fn attr(e: &quick_xml::events::BytesStart<'_>, name: &str) -> Option<String> {
    e.attributes()
        .filter_map(Result::ok)
        .find(|a| a.key.local_name().as_ref() == name)
        .and_then(|a| a.normalized_value(XmlVersion::Implicit1_0).ok())
        .map(|value| value.trim().to_owned())
}
