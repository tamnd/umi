//! The dataset card, from `docs/spec/12-publishing.md` section 12.9.
//!
//! Doc 12.9 fixes what the card says and the order it says it in: what umi is,
//! what the schema is, which spec and extractor version produced it, the
//! canonicalisation version, where the exclusion list lives and the obligation
//! to apply it, the crawl purpose declaration from doc 07.3, what the robots
//! and `Content-Usage` columns mean, and where to send a takedown. Everything
//! below is that list, rendered.
//!
//! Generated rather than committed because there are about 2000 repositories
//! over the corpus's life and doc 12.4 already refuses to name those by hand. A
//! card written once and copied would drift the moment a column moved, so the
//! column table is walked out of the same Arrow schema the writer uses and the
//! prose next to each column lives in one table in this file. A column with no
//! entry in that table is a compile time oversight and a test failure, which is
//! the point: adding a column to doc 10.5 and not saying what it means should
//! not be possible quietly.

use arrow::datatypes::DataType;

use crate::repo::{Corpus, Family, META_REPO};

#[cfg(test)]
#[path = "card_tests.rs"]
mod tests;

/// Where a takedown request goes, from doc 07.3's bot page.
const CONTACT: &str = "tamnd87@gmail.com";

/// The bot page, which is the other half of doc 07.3's declaration.
const BOT_PAGE: &str = "https://umi.dev/bot";

/// The spec, so that a reader can check the card against the document it cites.
const SPEC: &str = "https://github.com/tamnd/umi/tree/main/docs/spec";

/// Render the `README.md` for a repository of this family.
///
/// `extractor` is doc 11.3's extractor version, the same string the manifest
/// carries, so that the card and the manifest cannot disagree about what
/// produced the files underneath them.
#[must_use]
pub fn card(corpus: &Corpus, family: Family, extractor: &str) -> String {
    let mut out = String::with_capacity(8192);
    out.push_str(&front_matter(corpus, family));
    out.push_str(&heading(corpus, family));
    out.push_str(&columns_table(family));
    out.push_str(&provenance(family, extractor));
    out.push_str(PREFERENCES);
    out.push_str(&exclusions());
    out.push_str(&licensing(family));
    out.push_str(&contact());
    out
}

/// The YAML block Hugging Face reads to build the dataset viewer.
///
/// No `size_categories`. The card is written on the commit that creates the
/// repository, which is the moment it holds one file, and the bucket a
/// repository will end up in is not known then. The viewer counts the rows
/// itself, so a guess here would be a claim that is wrong for the whole of the
/// repository's life and nobody would go back and correct it.
fn front_matter(corpus: &Corpus, family: Family) -> String {
    let (pretty, license, tags) = match family {
        Family::Pages => (
            "umi pages",
            // Doc 12.9 will not put a single licence tag on a repository whose
            // rows are third party text. `other` with the split spelled out in
            // the card is the honest form of that, and the link goes to the
            // section that says it rather than to a licence file that would be
            // claiming something we cannot claim.
            "license: other\nlicense_name: umi-corpus-terms\nlicense_link: https://github.com/tamnd/umi/blob/main/docs/spec/12-publishing.md",
            "- web\n- common-crawl\n- web-crawl\n- parquet\n- umi",
        ),
        Family::Receipts => (
            "umi receipts",
            "license: cc0-1.0",
            "- web-crawl\n- provenance\n- audit\n- parquet\n- umi",
        ),
        Family::Robots => (
            "umi robots",
            "license: cc0-1.0",
            "- robots-txt\n- web-crawl\n- crawler-policy\n- parquet\n- umi",
        ),
        Family::Frontier => (
            "umi frontier",
            "license: cc0-1.0",
            "- web-crawl\n- url-list\n- frontier\n- parquet\n- umi",
        ),
    };
    // A focused crawl gets its scope in the name, because a reader browsing the
    // organisation sees the pretty name and nothing else, and three focused
    // repositories all called "umi pages" are three repositories nobody can
    // tell apart.
    let pretty = match (&corpus.focus, family) {
        (Some(name), Family::Pages) => format!("umi focus {name}"),
        _ => pretty.to_owned(),
    };
    format!(
        "---\npretty_name: {pretty}\n{license}\ntags:\n{tags}\nconfigs:\n- config_name: default\n  data_files:\n  - split: train\n    path: data/**/*.parquet\n---\n\n"
    )
}

/// The title, the first paragraph, and what this family holds.
fn heading(corpus: &Corpus, family: Family) -> String {
    let what = match family {
        Family::Pages => {
            "One row per page umi fetched. The extracted text as markdown, the title, the description, the headings, the outlinks with their anchor text, the sixteen response headers doc 11.5 keeps, and the duplicate detection sketches. There is no raw HTML in here and there never will be, because doc 10.2's arithmetic does not survive storing it and doc 07.8 caps how long we may hold it."
        }
        Family::Receipts => {
            "One row per delivery, which is doc 04's receipt flattened. Every row carries the fetcher identity and the Ed25519 signature over what that fetcher claims it saw, so the corpus can be checked against the published fetcher keys by somebody who does not trust us. That is the whole point of publishing it."
        }
        Family::Robots => {
            "One row per `robots.txt` fetch: the host, when we asked, what the origin answered, the raw text if it served one, and the summary our parser read out of it. It is a record of what sites told crawlers, kept over time, and as far as we know nobody else publishes it."
        }
        Family::Frontier => {
            "One row per URL umi knows about and has not fetched yet, with the scheduling state that decides when it would be. This is the crawler's backlog, published rather than kept, because a hundred billion known URLs do not fit on the machines doing the crawling and because a backlog is more useful to everyone else than it is to us. Nothing in here has been fetched, so nothing in here has been checked against the origin's `robots.txt`. Read the rules first, the same as we would."
        }
    };
    let focus = match (&corpus.focus, family) {
        (Some(name), Family::Pages) => format!(
            "\nThis is a focused crawl over `{name}` rather than a slice of the general corpus, which is doc 13.7. It is not an unbiased sample of the web and should not be used as one.\n"
        ),
        _ => String::new(),
    };
    let name = match (&corpus.focus, family) {
        (Some(focus), Family::Pages) => format!("umi-focus-{focus}"),
        _ => family.stem().to_owned(),
    };
    format!(
        "# {name}\n\nPart of [umi](https://github.com/tamnd/umi), an open web crawl published as Parquet. Before you use any of this, read the exclusion list at [`{META_REPO}`](https://huggingface.co/datasets/{META_REPO}) and filter the rows it names. Published files are never rewritten and never deleted, so the exclusion list is how a takedown reaches you, and applying it is a condition of using the data rather than a suggestion.\n{focus}\n{what}\n\n"
    )
}

/// The schema, walked out of the Arrow schema the writer uses.
fn columns_table(family: Family) -> String {
    let described = described(family);
    let mut out = String::from(
        "## Columns\n\nThe schema is doc 10.5's, mapped one to one into Parquet with nothing renamed and nothing flattened.\n\n| column | type | null | what it is |\n| --- | --- | --- | --- |\n",
    );
    for field in family.stream().arrow().fields() {
        let name = field.name();
        let note = described
            .iter()
            .find(|(col, _)| col == name)
            .map_or("", |(_, note)| *note);
        let null = if field.is_nullable() { "yes" } else { "no" };
        out.push_str(&format!(
            "| `{name}` | {} | {null} | {note} |\n",
            render(field.data_type()),
        ));
    }
    out.push('\n');
    out
}

/// The Arrow type as a reader of the table would write it.
fn render(ty: &DataType) -> String {
    match ty {
        DataType::Utf8 => "string".to_owned(),
        DataType::UInt8 => "uint8".to_owned(),
        DataType::UInt16 => "uint16".to_owned(),
        DataType::UInt32 => "uint32".to_owned(),
        DataType::UInt64 => "uint64".to_owned(),
        DataType::FixedSizeBinary(n) => format!("binary({n})"),
        DataType::List(item) => format!("list of {}", render(item.data_type())),
        DataType::Struct(_) => "struct".to_owned(),
        DataType::Map(..) => "map of string to string".to_owned(),
        other => other.to_string(),
    }
}

/// The versions, and the purpose declaration doc 12.9 puts next to them.
fn provenance(family: Family, extractor: &str) -> String {
    // Which column a reader should push a filter down to depends on the order
    // the writer put the rows in, and the frontier is the one family that is
    // sorted by key rather than left in the order the crawl produced.
    let order = match family {
        Family::Frontier => {
            "Rows are written in `(pld_id, host_id, url_key)` order, so a row group's statistics bound the domains inside it and a reader after one site reads one row group rather than the file."
        }
        _ => {
            "Rows are written in fetch completion order, so the `fetched_at_ms` statistics on a row group are the ones worth pushing a filter down to."
        }
    };
    format!(
        "## How this was made\n\nExtractor `{extractor}`, canonicalisation `{canon}`, against the spec at [`docs/spec`]({SPEC}). {order} Files are around 128 MB and a day folder holds one day of them.\n\numi crawls to build a public, openly licensed web index, and that is the whole declaration. We do not train models on the corpus, we do not run an agent that browses for a user, and we do not resell access. The crawler identifies itself as `umi` and its bot page is [{BOT_PAGE}]({BOT_PAGE}). The corpus is open, so what anyone else does with it is out of our hands, which is a fact worth stating plainly rather than eliding.\n\n",
        canon = umi_types::CANON_VERSION,
    )
}

/// What the robots and `Content-Usage` columns mean, which doc 12.9 asks for by
/// name because they are the two a reader is most likely to get wrong.
const PREFERENCES: &str = "## Robots and Content-Usage\n\nEvery page in the corpus was fetched under RFC 9309: the host's `robots.txt` was read first, a `Disallow` that matched was obeyed, and a `robots.txt` we could not read at all disallowed the host rather than allowing it. On the pages schema `robots_checked_ms` says when the file behind that decision was last read.\n\n`content_usage` carries the site's AIPREF `Content-Usage` preference, from the response header or the `robots.txt`, verbatim and unparsed. We record it and we do not act on it, because it is the publisher's statement to whoever reads the corpus and not ours to interpret on their behalf. If you are training on this data, that column is the one to read.\n\nA robots snapshot is only good for a day. RFC 9309 caps a cached `robots.txt` at 24 hours and umi honours that, so the robots corpus is a record of what a host said when we asked, not a standing permission you can crawl against today. Use it to plan, to see which hosts have rules at all, to pick up `Crawl-delay` and `Sitemap` before you queue anything, and then ask the origin yourself.\n\n";

/// The exclusion list, again, with the paths on it.
fn exclusions() -> String {
    format!(
        "## Corrections\n\nWe never rewrite a published file and we never delete one, because both break every checksum anyone recorded and make old work unreproducible. Corrections go on an append only exclusion list in [`{META_REPO}`](https://huggingface.co/datasets/{META_REPO}) under `blocks/`, naming a repository and a file and either a row predicate or a set of `url_key` values, with a reason and a date. Apply it as a filter when you read.\n\nEvery day folder has a manifest under `_manifest/` listing each file with its digests, and a detached Ed25519 signature next to it. The signing keys are in the same meta repository, so a manifest can be checked without asking us for anything.\n\n"
    )
}

/// Doc 12.9's licence split, stated rather than blurred.
fn licensing(family: Family) -> String {
    match family {
        Family::Pages => "## Licence\n\nThe split is real and worth stating plainly. Everything umi created is CC0: the annotations, the quality signals, the duplicate sketches, the link structure, the manifests and the schemas. The extracted page content is third party material that we did not create and cannot license. It is published on the same basis Common Crawl has published on for over a decade, with per row provenance, the publisher's own stated preferences carried in the row next to it, and a takedown process that works. Putting a single licence tag on the repository and hoping nobody looked closely would be the dishonest version.\n\n".to_owned(),
        Family::Receipts | Family::Robots | Family::Frontier => {
            "## Licence\n\nCC0. This is a record umi produced rather than content it collected, so there is nothing here anybody else holds a right in.\n\n".to_owned()
        }
    }
}

/// Where a complaint goes, and what happens to it.
fn contact() -> String {
    format!(
        "## Takedown\n\nMail <{CONTACT}> with the URLs or the host. A request is honoured by adding the rows to the exclusion list, which takes minutes rather than a release cycle, and by adding the host to the crawler's block list so it is not fetched again. You do not need to explain why.\n"
    )
}

/// What each column means, one line each.
///
/// Kept next to the card rather than next to the schema because these are for a
/// reader who has never seen the spec, and the doc comments on the schema are
/// for somebody who has.
const fn described(family: Family) -> &'static [(&'static str, &'static str)] {
    match family {
        Family::Pages => PAGES,
        Family::Receipts => RECEIPTS,
        Family::Robots => ROBOTS,
        Family::Frontier => FRONTIER,
    }
}

/// Doc 10.5's pages schema, in reader's terms.
const PAGES: &[(&str, &str)] = &[
    ("url", "The URL we asked for, canonicalised."),
    (
        "final_url",
        "Where the redirects ended, null when that is the same place.",
    ),
    (
        "url_key",
        "The 10 byte fingerprint of `url`, which is what deduplication and the frontier key on.",
    ),
    ("pld_id", "The 8 byte id of the pay level domain."),
    ("host", "The host as text."),
    (
        "fetched_at_ms",
        "When the fetch finished, Unix milliseconds.",
    ),
    ("status", "The HTTP status, or zero if we never got one."),
    (
        "outcome",
        "How the fetch ended: 0 ok, and the other codes cover not modified, gone, blocked, failed and off domain redirect.",
    ),
    (
        "tier_used",
        "Which rung of the fetch ladder produced this row, 0 through 4.",
    ),
    (
        "tier_path",
        "Every rung tried, cheapest first, so a reader can see what a page cost.",
    ),
    ("content_type", "`Content-Type` as sent."),
    (
        "content_length",
        "Body length in bytes after transfer decoding, which is not always what the header said.",
    ),
    ("lang", "The BCP 47 primary subtag, padded to three bytes."),
    ("body_digest", "blake3 over the body bytes."),
    (
        "chunk_root",
        "blake3 tree root over 16 KiB leaves of the body, so a range can be checked without the whole file.",
    ),
    (
        "extract_digest",
        "blake3 over the extraction, which is what makes an extraction reproducible.",
    ),
    (
        "markdown",
        "The extracted text. Null when there was no body, and null when the page asked not to be indexed.",
    ),
    ("title", "The title, after the usual boilerplate trimming."),
    (
        "description",
        "The meta description or its open graph equivalent.",
    ),
    ("headings", "`h1` through `h3` in document order."),
    (
        "snippets",
        "The same strings the title, description and heading columns hold, tagged by kind, for a reader who wants one column instead of four.",
    ),
    (
        "links",
        "The outlinks: absolute href, anchor text, the `rel` bits packed, and what kind of link it was.",
    ),
    (
        "headers_kept",
        "The sixteen response headers doc 11.5 allows, and nothing else. No cookies and no credentials, ever.",
    ),
    (
        "content_usage",
        "The site's AIPREF `Content-Usage` preference, verbatim. Read this one.",
    ),
    (
        "minhash",
        "64 MinHash values over the text shingles, for near duplicate clustering.",
    ),
    ("simhash", "A 64 bit simhash over the same shingles."),
    (
        "text_bytes",
        "Plain text length, since the plain text itself is not stored.",
    ),
    (
        "link_count",
        "How many outlinks, so a filter does not have to decode the list.",
    ),
    (
        "fetcher_id",
        "Which fetcher delivered it. The matching receipt carries the signature.",
    ),
    (
        "verification",
        "How far doc 06 got: arrived from a known fetcher, or corroborated by a second one.",
    ),
    (
        "robots_checked_ms",
        "When the `robots.txt` behind this fetch was last read.",
    ),
    ("crawl_profile", "Which crawl profile queued the URL."),
];

/// Doc 04's receipt, flattened, in reader's terms.
const RECEIPTS: &[(&str, &str)] = &[
    ("version", "The receipt format version."),
    ("lease_id", "The lease this delivery answered."),
    (
        "nonce",
        "The coordinator's nonce for that lease, which is what stops a receipt being replayed.",
    ),
    ("fetcher_id", "Which fetcher signed it."),
    ("url", "The URL the lease was for."),
    ("final_url", "Where the redirects ended."),
    (
        "fetched_at_ms",
        "When the fetch finished, Unix milliseconds.",
    ),
    ("duration_ms", "How long it took, end to end."),
    (
        "outcome",
        "How the fetch ended, the same codes the pages schema uses.",
    ),
    ("tier_used", "Which rung produced it."),
    ("tier_path", "Every rung tried."),
    (
        "method",
        "The HTTP method, which is `GET` outside a revalidation.",
    ),
    (
        "redirects",
        "Every hop: from, to, and the status that sent us.",
    ),
    (
        "ja4",
        "The TLS client fingerprint the fetcher presented, so a reader can tell which stack made the request.",
    ),
    ("http_version", "The negotiated HTTP version."),
    ("status", "The HTTP status."),
    (
        "headers_digest",
        "blake3 over the full response headers, including the ones not kept.",
    ),
    ("headers_kept", "The sixteen allowed headers."),
    ("content_length", "`Content-Length` as sent."),
    ("content_type", "`Content-Type` as sent."),
    (
        "body_digest",
        "blake3 over the body bytes, which is the value that ties this receipt to a page row.",
    ),
    ("body_length", "Body length in bytes."),
    ("chunk_root", "blake3 tree root over the body."),
    ("chunk_count", "How many 16 KiB leaves that tree has."),
    (
        "tls_chain_digests",
        "blake3 over each certificate in the chain, leaf first.",
    ),
    ("tls_sni", "The name sent in SNI."),
    ("tls_alpn", "The protocol ALPN settled on."),
    (
        "tls_not_before_ms",
        "The leaf certificate's validity start.",
    ),
    ("tls_not_after_ms", "The leaf certificate's validity end."),
    (
        "extractor",
        "Which extractor version ran, when the fetcher ran one.",
    ),
    ("extract_digest", "blake3 over that extraction."),
    (
        "stability",
        "Whether a second extraction of the same bytes agreed.",
    ),
    ("link_count", "How many outlinks the extraction found."),
    ("text_bytes", "How long the extracted text was."),
    (
        "signature",
        "Ed25519 over the receipt, by the key in the fetcher directory.",
    ),
];

/// Doc 07.4's robots snapshot, in reader's terms.
const ROBOTS: &[(&str, &str)] = &[
    ("host", "The host, with no scheme on it."),
    ("fetched_at_ms", "When we asked, Unix milliseconds."),
    (
        "status",
        "What the origin answered. 404 is the common case and it means the site has no file rather than that we failed to reach it. Zero means we never got a response at all, which is the one case where the rules below are ours and not the site's.",
    ),
    (
        "body",
        "The raw file, exactly as served. Null unless the status was a 2xx, because a 404 body is somebody's error page and not a `robots.txt`.",
    ),
    (
        "groups",
        "How many `User-agent` groups the file has, ours and everybody else's. Next to `rules` it says whether a site wrote rules for us or we are reading its wildcard group.",
    ),
    (
        "rules",
        "How many rules were in the group that applied to us.",
    ),
    (
        "crawl_delay_ms",
        "The `Crawl-delay` that applied to us, in milliseconds. Null when the file did not set one.",
    ),
    (
        "allows_us",
        "Whether the root path was allowed to `umi`, 1 or 0. A host that could not be reached is 0, because an unreadable `robots.txt` disallows rather than allows.",
    ),
    ("sitemaps", "Every `Sitemap` line in the file, in order."),
    (
        "content_usage",
        "The AIPREF `Content-Usage` value the file declared, verbatim.",
    ),
];

/// Doc 08.6's frontier shard, in reader's terms.
const FRONTIER: &[(&str, &str)] = &[
    (
        "pld_id",
        "The pay level domain the URL belongs to, as an 8 byte fingerprint. Every host under `example.co.uk` shares one, and it is what the file is sorted on first.",
    ),
    (
        "host_id",
        "The host, as an 8 byte fingerprint. Rate limiting applies per host, so this is the second sort key.",
    ),
    (
        "url_key",
        "The 80 bit fingerprint of the canonical URL, which is what umi deduplicates on.",
    ),
    (
        "url_key_full",
        "The 128 bit fingerprint of the same URL. It exists so that a collision in the short one is detectable rather than silent.",
    ),
    (
        "url",
        "The URL itself, canonicalised. Everything else in the row describes this.",
    ),
    (
        "depth",
        "Link distance from the nearest seed. Zero is a seed, one is a page linked from a seed, and so on.",
    ),
    (
        "priority",
        "The score the scheduler would pick this URL on, higher first. It is umi's own opinion and not a property of the page.",
    ),
    (
        "state",
        "0 pending, 1 fetched, 2 failed, 3 gone, 4 excluded. A spill is overwhelmingly pending, which is the point of it.",
    ),
    (
        "next_due_ms",
        "The earliest time umi would fetch or refetch this, Unix milliseconds.",
    ),
    (
        "last_fetch_ms",
        "When it was last fetched, null when it never was.",
    ),
    (
        "last_change_ms",
        "When the extracted text last actually changed, which is not the same as when it was last fetched. Null until it has changed twice.",
    ),
    ("fetch_count", "How many times it has been fetched."),
    (
        "change_count",
        "How many of those fetches found different content.",
    ),
    (
        "observed_secs",
        "How long the URL has been watched for, in seconds, summed over the intervals we actually served rather than measured from the first fetch. This and `change_count` together are the change rate estimator doc 09 schedules on: two changes in a week and two changes in a year are the same count and want very different refetch intervals.",
    ),
    (
        "content_hash",
        "A truncated digest of the extracted text as of the last fetch, so a refetch can tell a change from a rerun. Null when it has never been fetched.",
    ),
    (
        "etag",
        "The `ETag` the origin last served, verbatim. Null when it served none.",
    ),
    (
        "last_mod_ms",
        "The `Last-Modified` the origin last served, Unix milliseconds. Null when it served none.",
    ),
    (
        "status",
        "The status of the last fetch. Null when it has never been fetched.",
    ),
    (
        "tier_used",
        "Which rung of doc 05's ladder answered last time: 0 revalidate, 1 plain HTTP, 2 emulated, 3 rendered, 4 supervised. Null when it has never been fetched.",
    ),
    (
        "fail_streak",
        "Consecutive failures. It is what retires a URL, and it resets on any success.",
    ),
];
