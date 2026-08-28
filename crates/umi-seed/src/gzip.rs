//! Unwrapping the gzip a sitemap usually arrives as.
//!
//! This is not HTTP compression and it is worth being clear about the
//! difference, because the two look the same from a distance and only one of
//! them is handled for us. A `Content-Encoding: gzip` response is unwrapped by
//! the HTTP client before umi ever sees it. A `sitemap.xml.gz` is a file whose
//! content type is gzip, which is the site telling us the resource itself is a
//! compressed archive, and no client in the world unwraps that. Large sites
//! serve their sitemaps this way as a matter of course, so a reader that only
//! understands XML sees two magic bytes and reports the file as junk.
//!
//! # A gzip bomb is a real document
//!
//! Compression ratios past a thousand to one are easy to build and are the
//! whole point of the format, so a 40 KB file that inflates to 40 GB is
//! something a stranger can serve us on purpose. That is why nothing here
//! inflates into a `Vec` and checks the size afterwards: the check has to be on
//! the way out, one buffer at a time, and it is the caller's `max_bytes` that
//! decides where to stop. The bound is the same one a plain XML sitemap gets,
//! so a hostile site gains nothing by compressing.

use std::io::Read;

use flate2::read::MultiGzDecoder;

/// How much to pull out of the decoder at a time.
const CHUNK: usize = 64 * 1024;

/// Whether these bytes are a gzip member.
///
/// The two byte magic number, which is what the format guarantees and what
/// every other reader keys off. The content type is not used for this: a lot of
/// sites serve `sitemap.xml.gz` as `text/xml`, and a few serve plain XML as
/// `application/gzip`, so the bytes are the only honest answer.
pub(crate) fn is_gzip(bytes: &[u8]) -> bool {
    bytes.starts_with(&[0x1f, 0x8b])
}

/// Inflate, stopping at `max_bytes` of output.
///
/// Returns what came out and whether the cap cut it short.
///
/// A decoder error ends the read and keeps the prefix, which is the same rule
/// the XML reader follows and is right for the same reason: a sitemap whose
/// last member is corrupt still listed forty thousand real URLs before it got
/// there, and throwing those away helps nobody.
///
/// `MultiGzDecoder` rather than `GzDecoder`, because a gzip file is allowed to
/// be several members back to back and some generators produce exactly that by
/// concatenating daily files. The plain decoder reads the first member and
/// stops, which looks from the outside like a sitemap that is mysteriously
/// missing most of its URLs.
pub(crate) fn inflate(bytes: &[u8], max_bytes: usize) -> (Vec<u8>, bool) {
    let mut decoder = MultiGzDecoder::new(bytes);
    // One byte past the cap, so that a document landing exactly on it is not
    // reported as truncated when it was complete.
    let mut out: Vec<u8> = Vec::new();
    let mut chunk = vec![0u8; CHUNK];
    while out.len() <= max_bytes {
        let want = CHUNK.min(max_bytes + 1 - out.len());
        match decoder.read(&mut chunk[..want]) {
            Ok(0) | Err(_) => break,
            Ok(read) => out.extend_from_slice(&chunk[..read]),
        }
    }

    let truncated = out.len() > max_bytes;
    out.truncate(max_bytes);
    (out, truncated)
}
