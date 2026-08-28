//! Sitemaps, doc 13.6 and doc 09.
//!
//! A sitemap is a site telling us which URLs it has and when each one last
//! changed. That is the best seed there is, and it is the only one where the
//! freshness signal comes from the publisher rather than from us guessing at
//! it, so `lastmod` is read here and carried through rather than dropped.
//!
//! Two documents share this parser because they share a shape. A `<urlset>`
//! holds `<url>` elements and a `<sitemapindex>` holds `<sitemap>` elements,
//! and both children carry a `<loc>` and an optional `<lastmod>`. What comes
//! out is [`Sitemap`] with both lists on it, so a file that is somehow both,
//! or that is neither and just happens to contain one of each, needs no
//! decision from the caller. There is also the plain text form sitemaps.org
//! allows, one URL per line, which is detected and read the same way.
//!
//! Nothing here fetches anything and nothing here recurses. A sitemap index
//! points at other files, and following them is the caller's job because the
//! caller is the half that has a fetcher, a robots check and a budget.
//!
//! # Hostile input
//!
//! Every sitemap is a document from a stranger, so the limits are the design
//! rather than an afterthought. The reader is a pull parser, so no document is
//! ever held as a tree. [`Caps::max_bytes`] bounds the input, [`Caps::max_urls`]
//! bounds the output, and [`Caps::max_loc`] bounds any one element, which
//! together mean the memory a parse can use is decided here rather than by
//! whoever wrote the file. The classic XML attack, a DTD whose entity expands
//! to gigabytes, does not apply: quick-xml never resolves DTD entities, so
//! that document parses to text nobody can use rather than to no memory left.

use crate::xml::{chunk, reader};
use quick_xml::events::Event;

/// The largest sitemap to read, in bytes.
///
/// sitemaps.org's own cap, so a file over it is out of spec and is either a
/// mistake or bait. The bytes past this point are not parsed, and the result
/// says so through [`Sitemap::truncated`] rather than being thrown away, since
/// 50 MB of real URLs is still 50 MB of real URLs.
pub const MAX_BYTES: usize = 50 * 1024 * 1024;

/// The most URLs to take from one file, which is also sitemaps.org's cap.
pub const MAX_URLS: usize = 50_000;

/// How deep a chain of sitemap indexes to follow, from doc 09.
///
/// Not used here, because nothing here fetches. It is the number the caller
/// doing the following needs, and it lives next to the parser so that the two
/// halves of the same rule are read together.
pub const MAX_INDEX_DEPTH: u8 = 3;

/// One URL out of a sitemap, with the date the site put on it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Entry {
    /// The URL exactly as the file wrote it, unescaped and trimmed but not yet
    /// canonicalised.
    ///
    /// Canonicalisation belongs to whoever admits this, because the answer
    /// depends on doc 11.2 and on the base URL, and a parser that did it here
    /// would have to know things a parser does not know.
    pub url: String,
    /// `lastmod`, in milliseconds since the epoch, when the file carried one
    /// that could be read.
    pub lastmod_ms: Option<u64>,
}

/// What one sitemap file turned out to contain.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct Sitemap {
    /// URLs from `<url>` elements, in the order the file listed them.
    pub urls: Vec<Entry>,
    /// Sitemaps from `<sitemap>` elements, which are the ones to fetch next.
    pub sitemaps: Vec<Entry>,
    /// Whether a cap stopped the parse before the end of the document.
    ///
    /// The entries above are still good. This says there were more of them.
    pub truncated: bool,
    /// Whether the document stopped making sense before it ended.
    ///
    /// Also not fatal, and for the same reason: a sitemap that is well formed
    /// for forty thousand URLs and then has a stray `<` in it has forty
    /// thousand URLs worth using. It is reported so that an operator looking
    /// at a short crawl can see which of the two happened.
    pub malformed: bool,
}

/// The three numbers that decide what a parse can cost.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Caps {
    /// The most input to read, in bytes.
    pub max_bytes: usize,
    /// The most entries to keep, counting both lists together.
    pub max_urls: usize,
    /// The longest `<loc>` to keep, in bytes.
    ///
    /// Doc 11.2 rejects a URL over 2048 bytes anyway, so this is set well above
    /// that on purpose: a value that merely fails canonicalisation should be
    /// counted as a bad URL with a reason, not silently vanish here.
    pub max_loc: usize,
}

impl Default for Caps {
    fn default() -> Self {
        Self {
            max_bytes: MAX_BYTES,
            max_urls: MAX_URLS,
            max_loc: 4 * umi_types::canon::MAX_URL_LEN,
        }
    }
}

impl Sitemap {
    /// Read a sitemap, a sitemap index or a plain text sitemap.
    #[must_use]
    pub fn parse(bytes: &[u8]) -> Self {
        Self::parse_with(bytes, &Caps::default())
    }

    /// Read one, with limits the caller chose.
    ///
    /// Gzip is unwrapped first when the bytes are a gzip member, because a
    /// `sitemap.xml.gz` is how most large sites serve theirs and no HTTP client
    /// unwraps that for us: the compression is the resource rather than the
    /// transfer encoding. [`Caps::max_bytes`] then bounds what comes out of the
    /// decoder rather than what went into it, which is what makes a document
    /// that inflates to forty gigabytes cost the same as one that does not.
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
        if is_text(bytes) {
            out.read_text(bytes, caps);
        } else {
            out.read_xml(bytes, caps);
        }
        out
    }

    /// Every URL in the file, whichever list it came from.
    ///
    /// For the common case of feeding a frontier, where a URL is a URL and the
    /// caller has already decided it is not following the index.
    pub fn all(&self) -> impl Iterator<Item = &Entry> {
        self.urls.iter().chain(&self.sitemaps)
    }

    /// Whether the file yielded nothing at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.urls.is_empty() && self.sitemaps.is_empty()
    }

    fn len(&self) -> usize {
        self.urls.len() + self.sitemaps.len()
    }

    /// The plain text form: one URL per line, no markup, nothing else allowed.
    ///
    /// There is no `lastmod` in this format, which is most of why sites that
    /// care about freshness do not use it.
    fn read_text(&mut self, bytes: &[u8], caps: &Caps) {
        for line in bytes.split(|b| *b == b'\n') {
            if self.len() >= caps.max_urls {
                self.truncated = true;
                return;
            }
            let Ok(line) = core::str::from_utf8(line) else {
                self.malformed = true;
                continue;
            };
            let line = line.trim();
            if line.is_empty() || line.len() > caps.max_loc {
                continue;
            }
            self.urls.push(Entry {
                url: line.to_owned(),
                lastmod_ms: None,
            });
        }
    }

    fn read_xml(&mut self, bytes: &[u8], caps: &Caps) {
        let mut reader = reader(bytes);
        // Depth is counted here rather than by the reader, and every field is
        // matched against the depth it is supposed to be at. That is what
        // keeps an image sitemap's `<image:loc>`, which sits one level below
        // the `<loc>` we want and has the same local name, out of the page
        // URLs. Namespace prefixes are not resolved, because a sitemap that
        // binds the sitemap namespace to a prefix and then uses a second
        // prefix for images is still telling us which element is a child of
        // which.
        let mut depth = 0usize;
        let mut open: Option<Open> = None;
        let mut field: Option<Field> = None;
        let mut buf = String::new();
        let mut entry = Entry {
            url: String::new(),
            lastmod_ms: None,
        };

        loop {
            let event = match reader.read_event() {
                Ok(event) => event,
                Err(_) => {
                    // A document that was cut off by `max_bytes` ends in the
                    // middle of something by construction, and calling that
                    // malformed would point the operator at the wrong problem.
                    self.malformed = !self.truncated;
                    return;
                }
            };
            match event {
                Event::Eof => return,
                Event::Start(e) => {
                    let local = e.local_name();
                    let name: &str = local.as_ref();
                    depth += 1;
                    match open {
                        None => {
                            open = match name {
                                "url" => Some(Open::Url(depth)),
                                "sitemap" => Some(Open::Sitemap(depth)),
                                _ => None,
                            };
                            if open.is_some() {
                                entry = Entry {
                                    url: String::new(),
                                    lastmod_ms: None,
                                };
                            }
                        }
                        Some(at) if depth == at.depth() + 1 => {
                            field = match name {
                                "loc" => Some(Field::Loc),
                                "lastmod" => Some(Field::Lastmod),
                                _ => None,
                            };
                            buf.clear();
                        }
                        Some(_) => {}
                    }
                }
                Event::End(_) => {
                    match open {
                        Some(at) if depth == at.depth() => {
                            open = None;
                            if !entry.url.is_empty() {
                                let done = Entry {
                                    url: core::mem::take(&mut entry.url),
                                    lastmod_ms: entry.lastmod_ms,
                                };
                                match at {
                                    Open::Url(_) => self.urls.push(done),
                                    Open::Sitemap(_) => self.sitemaps.push(done),
                                }
                                if self.len() >= caps.max_urls {
                                    self.truncated = true;
                                    return;
                                }
                            }
                        }
                        Some(at) if depth == at.depth() + 1 => {
                            let text = buf.trim();
                            match field.take() {
                                // A `<loc>` over the cap is dropped rather
                                // than truncated, because half a URL is a
                                // different URL and fetching it would be a
                                // request nobody asked for.
                                Some(Field::Loc) if text.len() <= caps.max_loc => {
                                    entry.url = text.to_owned();
                                }
                                Some(Field::Lastmod) => {
                                    entry.lastmod_ms = crate::date::any(text);
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
}

/// Which container we are inside, and how deep it was.
#[derive(Clone, Copy)]
enum Open {
    Url(usize),
    Sitemap(usize),
}

impl Open {
    const fn depth(self) -> usize {
        match self {
            Self::Url(depth) | Self::Sitemap(depth) => depth,
        }
    }
}

/// Which child of it we are inside.
#[derive(Clone, Copy)]
enum Field {
    Loc,
    Lastmod,
}

/// Whether this is the plain text form rather than the XML one.
///
/// The first thing that is not whitespace and not a byte order mark decides
/// it. Every XML sitemap starts with a declaration or with an element, and no
/// URL starts with `<`, so this cannot get it wrong on a well formed file of
/// either kind.
fn is_text(bytes: &[u8]) -> bool {
    let bytes = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes);
    match bytes.iter().find(|b| !b.is_ascii_whitespace()) {
        Some(b'<') => false,
        Some(_) => true,
        // An empty file parses to nothing either way, and the XML path is the
        // one that reports it as empty rather than as one bad line.
        None => false,
    }
}
