# 11 Extraction and deduplication

## 11.1 The rule that governs everything here

Extraction runs on machines we do not control. Doc 04 pushes it to the edge because uploading 150 KB of raw HTML per page defeats the whole point of a community fleet, and doc 06 compares extraction digests across independent fetchers to decide whether a delivery is honest. Both of those depend on one property:

**Given the same input bytes and the same extractor version, extraction produces byte identical output on every machine, forever.**

That is a much stronger requirement than "produces good markdown", and it is the constraint that shapes the rest of this document. It rules out hash map iteration order anywhere in the output path, locale dependent case folding, floating point in any scoring threshold that can flip a decision, wall clock or randomness of any kind, and thread count affecting output. It also rules out the usual approach of shelling out to whatever readability library is fashionable, because those libraries change their heuristics on patch releases and every change silently invalidates cross fetcher comparison.

So `umi-extract` pins its own implementation, version stamps every output, and treats a heuristic change as a major version bump that doc 06 handles as version skew rather than as disagreement. The extractor version string in doc 04's receipt is the exact crate version and it is compared exactly.

The second rule, which is smaller but saves a lot of argument: **extraction records, it does not judge**. Quality signals, duplicate clusters and language confidence are computed and published as columns. Nothing is dropped for being low quality, and nothing is dropped for being a duplicate. Doc 02 explains the reasoning: we are producing a corpus, not a training set, and the filtering decisions that make a good training set are exactly the decisions that make a bad index. A consumer who wants RefinedWeb style aggressive filtering can apply it in one pass over our columns. A consumer who wants everything cannot recover what we threw away.

## 11.2 URL canonicalisation

This is the most load bearing 30 lines in the spec. `UrlKey` in doc 08 is a fingerprint of the canonical form, so canonicalisation defines identity for every URL in the system. Get it wrong in the permissive direction and the frontier fills with the same page 40 times. Get it wrong in the aggressive direction and distinct pages collide and are never crawled. Change it later and every key in the system changes.

Therefore it is versioned. The current version is `canon/1`, it is recorded in every segment header and every state checkpoint, and changing it is a migration with a written plan, never a patch release.

Steps, applied in this order:

1. Reject anything that is not `http` or `https`. Reject URLs longer than 2048 bytes after canonicalisation.
2. Lowercase the scheme and the host. Never lowercase the path, query or fragment, because path case is significant on most servers.
3. Strip userinfo entirely. A URL with credentials in it is not something we fetch.
4. Convert the host to its IDNA A-label form under UTS-46 with transitional processing off, which is the current WHATWG rule. Reject hosts that fail IDNA validation rather than falling back to the raw bytes.
5. Strip the port when it is the scheme default. Keep it otherwise.
6. Remove the fragment. Including hash bang fragments, which some sites still use for routing, and which we accept losing.
7. Normalise percent encoding: uppercase the hex digits, and decode any octet that encodes an unreserved character per RFC 3986. Do not decode anything else, and in particular do not decode `%2F` in a path.
8. Resolve dot segments in the path per RFC 3986 section 5.2.4. An empty path becomes `/`.
9. Drop a trailing `?` with an empty query. Drop empty query parameters that have no name.
10. Remove tracking and session parameters by name, case insensitively, from a fixed published list. The list covers `utm_*`, `fbclid`, `gclid`, `dclid`, `msclkid`, `twclid`, `igshid`, `mc_cid`, `mc_eid`, `_ga`, `_gl`, `yclid`, `wickedid`, `at_*`, `ref_src`, `spm`, `sessionid`, `jsessionid`, `phpsessid`, `aspsessionid*`, `sid` when its value looks like a session token, and `cid` only when the host is on a per host override list. Path parameters of the form `;jsessionid=` are stripped too, because Java servlet containers still emit them.
11. Do not sort query parameters and do not deduplicate repeated parameter names. Both are common canonicalisation steps and both are wrong: parameter order and repetition are semantically significant on enough real sites that the breakage outweighs the dedup win.
12. Do not add or remove a trailing slash on the path. `/a` and `/a/` are different resources often enough that normalising them costs more than it saves. The exception is that a bare authority with no path gets `/`, which step 8 already did.

The list in step 10 is data, not code, it ships as a file, and it is published in `open-index/umi-meta` alongside the corpus so that anyone reproducing our keys can. Adding an entry to it is a canonicalisation version bump, which is precisely the point of making it explicit.

Two things canonicalisation deliberately does not do. It does not follow `rel=canonical`, because that is a publisher assertion about content identity and not about URL identity, and doc 11.6 records it as a column so consumers can collapse on it themselves. And it does not consult the network, because canonicalisation runs during admission at 12500 candidates per second and anything requiring I/O is disqualified.

## 11.3 HTML to markdown

Parse with `html5ever` in full spec conformant mode, because the tolerant parsing rules are exactly what makes results reproducible on the broken HTML that dominates the real web. No regex parsing anywhere, no `tl` style non conformant fast parser, since a fast parser that disagrees with a browser on malformed input breaks determinism against the T3 rendered tier.

Then main content detection, in a cascade borrowed in shape from Trafilatura, which remains the strongest published extractor on the standard benchmarks and does it by combining explicit metadata with node scoring rather than by picking one:

1. If the document declares a root, take it. The order is `<main>`, then a node carrying schema.org `articleBody`, then `<article>`, and the order is fixed rather than "whichever we find first" because a page can carry more than one of them and two extractors that disagree about which to prefer produce two different digests for the same bytes. `<main>` goes first because HTML allows at most one visible `<main>` per document, so it is the least ambiguous signal on the page. `<article>` goes last because it is the most ambiguous: a blog index is a page of many `<article>` elements and taking the first would extract one post out of thirty. So `<article>` counts as a declared root only when the document has exactly one, and a document with several falls through to scoring, which is the right answer for an index page.
2. Otherwise score every candidate container by an integer function of its text length, its link density in fixed point hundredths, its paragraph count, and a small penalty table keyed on tag name and on class and id substrings from a fixed published list of boilerplate markers. Take the highest scoring subtree. A candidate container is a `div`, `section`, `article`, `main`, `td`, `blockquote` or `body`, and not every block level node, because scoring a `p` against its own parent is scoring the same text twice and the winner is then decided by the penalty table rather than by the content. Ties go to the container that appears first in document order, for the reason in 11.1.
3. If the winning subtree carries less than 200 bytes of text, or if link density in it exceeds 0.66, fall back to the whole `<body>` and set the `boilerplate_uncertain` flag rather than returning nothing. A directory page or a link farm is still a page we want a row for, and refusing to extract it loses the links, which are often its entire value.

Everything in the scoring function is integer arithmetic on fixed point values. No floats, for the reason in 11.1.

Then serialise to a fixed CommonMark subset: ATX headings for `h1` through `h6`, paragraphs, ordered and unordered lists with nesting, blockquotes, fenced code blocks carrying the language from a `class="language-*"` when present, GitHub flavoured tables, inline links, emphasis and strong emphasis, and horizontal rules. Images become their alt text in square brackets followed by the resolved source URL, because the alt text is content and the URL is a link and both are worth keeping while the image bytes are not.

The drop list is two lists, and an earlier draft of this doc had it as one, which was wrong in both directions.

**Dropped with their subtree**, because nothing inside them is content and nothing inside them is a link worth following: `script`, `style`, `noscript`, `svg`, `canvas`, `iframe`, `object`, `embed`, `input`, `button`, `select`, `textarea`, `option`, and every comment node. `textarea` and `option` are on this list and were not on the original: a `textarea` holds its default value as text and an `option` holds its label, so a parser that walks children without knowing about them silently splices a form's dropdown contents into the middle of the article. This is a real and common corruption on shop and settings pages.

**Dropped from the markdown but still walked for links**: `nav`, `header`, `footer`, and `aside`. These are boilerplate as prose and are the single richest source of site structure there is, and doc 11.4 wants every one of those links. Dropping the subtree would have thrown away exactly the navigation that discovery runs on.

`form` is on neither list, and the original doc had it dropped with its subtree, which was the worst of the mistakes. A `form` is a wrapper, not a kind of content, and a great many sites wrap the entire page body in one so that a search box in the header works. Dropping it took the whole article with it. It collapses to its children like any other wrapper.

`label` and `legend` are also kept, for the mirror image of the reason `textarea` is dropped: they are form associated, so an element name based rule sweeps them up, but their content is ordinary prose written for a human to read.

Also dropped: all attributes except `href`, `src`, `alt`, `title`, `lang`, `datetime`, and the `class` values used for code language detection. Inline `span` and `div` collapse to their children.

Whitespace is normalised: runs of ASCII whitespace collapse to one space, leading and trailing whitespace is trimmed per block, and the whole output is normalised to Unicode NFC. NFC rather than NFKC, because NFKC destroys mathematical notation and full width CJK punctuation, and we crawl a lot of Japanese.

Non HTML content types get their own path. PDF, plain text, XML and JSON each have a small handler, and everything else is stored as a row with metadata and no body. PDF extraction is behind a cargo feature and is off by default until milestone 5, because PDF parsers are the largest untrusted parsing surface anyone can hand a crawler.

**Plain text is not stored.** Doc 10 explains why: it is a pure function of markdown and regenerating it is cheaper than storing it. The function is exactly "strip markup, collapse whitespace, NFC" and it lives in `umi-extract` as a public function so that every consumer produces the same text we hashed.

## 11.4 Links

Links are half the reason the crawler exists, so they get more care than the body does.

Resolution goes against `<base href>` when present and well formed, otherwise against the final URL after redirects, never against the requested URL. Getting that backwards is the classic bug that sprays relative links onto the wrong host.

Every link is canonicalised by 11.2 at extraction time, on the fetcher, so the coordinator receives keys it can check against the seen set without re parsing. Links that fail canonicalisation are counted and dropped, and the count is published, because a sudden rise in it is a good signal that an extractor build is broken.

That is two counts and not one. `dropped` is a link that looked like an `http` or `https` URL and would not canonicalise, and it is the number that says an extractor build is broken. `dropped_scheme` is a link whose scheme we do not crawl, and on a normal page it is every `mailto:` and every `javascript:void(0)` in the navigation, which is a large and completely uninteresting number. Publishing them as one column would have buried the signal under the noise, since a page with forty `javascript:` handlers and one genuinely broken URL would look identical to a page with forty one broken URLs.

Schemes other than `http` and `https` are dropped, which covers `javascript:`, `mailto:`, `tel:`, `data:`, and the long tail of application handlers. `mailto:` targets are dropped rather than collected, deliberately, because collecting email addresses at web scale creates an obligation we do not want.

Each link carries a `rel` bitmask covering `nofollow`, `ugc`, `sponsored`, `noopener`, `canonical`, `alternate`, `next`, `prev`, `me`, and `author`, and a `kind` enum distinguishing a body anchor, a navigation anchor, a `link` element, a redirect target, a sitemap reference, and a feed reference. The distinction between body and navigation anchors comes free from the main content detection in 11.3 and it matters more than it looks: navigation links are how you discover a site's structure and body links are how you estimate a page's importance, and doc 09's priority function weights them differently.

`nofollow` is recorded and is not obeyed as a crawl directive. It was designed as a comment spam countermeasure and has meant nothing about crawlability for over a decade. It is a published column and consumers can weight on it.

`noindex`, in a `meta robots` tag or an `X-Robots-Tag` header, is obeyed. The row is written with its URL, status, headers and link set, and the markdown, title, description and snippets are withheld, with `content_withheld` set to the reason. Links are still extracted and followed unless `nofollow` appears in the same directive, which is what the directive actually means. This is the one place where extraction makes a policy decision, and it belongs here rather than in doc 07 because it depends on parsing the body.

Caps: 5000 links per page, anchor text truncated to 200 bytes on a UTF-8 boundary, and exact `(href, anchor, kind)` triples deduplicated within the page. A page with more than 5000 links keeps the first 5000 in document order and sets a flag. These caps exist because doc 09's trap section describes pages designed to explode a frontier and the cheapest place to stop them is before they enter one.

## 11.5 Headers kept

Doc 04's receipt carries `headers_kept`, and this is the definition. The full header set is hashed into `headers_digest` so that nothing is lost for verification purposes, but only this subset is stored and published:

```
content-type          content-language      last-modified
etag                  cache-control         expires
age                   vary                  content-encoding
link                  x-robots-tag          location
retry-after           content-usage         alt-svc
server
```

Sixteen headers, fixed list, no wildcards, no configuration. Everything else is discarded at extraction time and never leaves the fetcher.

The reasoning is one part size and one part obligation. Size: a full header set on a modern site runs 1.5 KB and would be a quarter of doc 10's byte budget, most of it CDN debug fields nobody will ever read. Obligation: `Set-Cookie` frequently contains session identifiers and occasionally contains things that are unambiguously personal data, and the only defensible way to handle a header like that in a corpus published under an open licence is to never store it. There is no allowlist entry, no debug flag and no operator override that stores `Set-Cookie`, `Authorization`, `Proxy-Authorization`, or `WWW-Authenticate`.

`link` is on the list because it carries `rel=canonical` and `rel=alternate` for non HTML responses, `alt-svc` is there because it is a cheap longitudinal record of HTTP/3 adoption, and `server` is there for the same reason. Those two are the only entries justified by research value rather than by crawler need, and if they turn out to cost more than they are worth they come off.

## 11.6 Metadata and snippets

Extracted from the document head and from structured data, with a fixed precedence so that the result is deterministic:

**Title.** `<title>`, then `og:title`, then the first `h1`. Trimmed to 512 bytes.

**Description.** `meta[name=description]`, then `og:description`, then `twitter:description`, then the first paragraph of the extracted markdown truncated to 300 bytes on a word boundary. The fallback is marked with a flag so consumers can tell an author written description from a derived one.

**Canonical.** `link[rel=canonical]` from the head, or `rel=canonical` from a `Link` header, canonicalised by 11.2. Recorded, never acted on at crawl time.

**Dates.** `article:published_time`, `article:modified_time`, JSON-LD `datePublished` and `dateModified`, and the `Last-Modified` header, kept as separate columns rather than reconciled into one, because reconciling them requires trusting one of them and they disagree constantly.

**Structured data.** JSON-LD blocks are parsed, and we keep the `@type` values, the `datePublished`, `dateModified`, `author.name`, and `headline` fields. We do not keep the full JSON-LD blob, which on e-commerce pages routinely exceeds the size of the page content. Microdata and RDFa are detected and flagged but not parsed, which is a deliberate scope cut recorded in doc 17.

**Headings.** `h1` through `h3` in document order from the extracted subtree, capped at 64 entries.

**Feeds.** `link[rel=alternate]` with an RSS or Atom type, resolved and canonicalised, and handed to doc 09's realtime path. This is the cheapest freshness signal on the web and most crawlers ignore it.

**Language.** Trigram detection over the first 4 KiB of extracted text, storing the BCP 47 primary subtag and a confidence. Below 0.5 confidence the language is stored as `und` rather than guessed, because a wrong language label is worse than an absent one for every downstream use. The `lang` attribute on `<html>` is recorded separately, because publisher declared language and detected language disagree often enough to be interesting.

Four fields in this list had no cap in the first draft, and every one of them is attacker or template controlled, which means "no cap" is really "whatever the page felt like". They are capped now, and the caps are part of the format rather than an implementation detail, because a consumer sizing a column needs to know them.

```
heading text            256 bytes each, 64 entries
author name             256 bytes, 16 entries
JSON-LD @type           64 bytes each, 16 entries
feeds                   16 entries
```

The counts matter more than the byte lengths. A generated page can carry a `h2` every other line and an e-commerce template can list a hundred `@type` values on one product, and without a cap a single page turns into a metadata row larger than its own content. Everything over the cap is truncated rather than dropped, and truncation sets a flag, so a consumer can tell a two author paper from a page that listed two hundred.

**Quality signals**, computed and published, never applied: text byte count, link count, link density in the extracted subtree, the ratio of extracted text to raw document size, the fraction of text in the top boilerplate scoring node, the count of dropped script and style bytes, and a stopword coverage ratio for the detected language that separates natural language prose from generated word salad. Seven integers that let a consumer build their own filter without re parsing 100 billion pages.

## 11.7 Exact duplicates

Exact duplication on the web is enormous and it is cheap to detect. The same article appears on a wire service and 400 syndicating outlets, the same documentation page appears under a dozen version prefixes, the same product page appears with and without a session parameter that step 10 missed.

The digest is blake3-256 over the normalised plain text from 11.3, not over the raw HTML and not over the markdown. Not raw HTML because two byte identical articles differ in their ad slots and CSRF tokens. Not markdown because heading levels shift between templates carrying the same prose. Normalised text is the level at which "the same content" is actually the same.

Detection runs at three ranges, and being honest about the third one matters.

**Within a segment.** Free. The writer already has 21000 rows in hand and a hash set over their digests costs nothing. Catches the printer friendly page, the AMP variant, and the session parameter duplicate, which is the bulk of it.

**Within a PLD.** Doc 08's `LedgerRow` already carries `content_hash`, a truncated 64 bit digest, and the PLD shard is already resident when we complete a fetch. Comparing against it is one lookup in a structure we have already paid for. Catches site internal mirroring, which is the next largest slice.

**Across the whole corpus.** This is the hard one and it does not go on the hot path. A global exact duplicate index at 100 billion pages is a 100 billion entry set with no locality at all, since content digests are uniformly random and cannot be sharded by PLD the way the seen set can. Putting it in front of the writer would mean a random object storage read per page, which at 750 pages per second is not survivable.

So global exact dedup is a batch job that runs behind the crawl, using the DRUM technique from IRLbot applied to content digests instead of URLs: buffer digests in memory, bucket them by digest prefix into 4096 on disk buckets, and when a bucket fills, sort it and merge it against that prefix's shard in object storage in one sequential pass. Amortised cost is a few bytes of sequential I/O per page and zero random reads. Latency is hours, which is completely acceptable, because the output is an annotation and not a filter.

The output is published as its own dataset, `open-index/umi-dedup`, mapping the 32 byte text digest to a cluster id and to the URL and crawl time of the first member seen. A consumer joins it against the pages dataset on `text_digest` and gets exact dedup with one hash join. We do not rewrite published Parquet to add a duplicate flag, because rewriting published data is a class of operation we want to never do.

## 11.8 Near duplicates

The sketch is 64 MinHash values over 5-gram shingles of the normalised text, hashed with xxh3, computed once at extraction time on the fetcher. It serves two different purposes in this spec and it is worth being clear that they are different.

**Doc 04's stability comparison** uses it directly, estimating Jaccard similarity between two independent fetches of the same URL to decide whether two fetchers are describing the same page. That is a direct estimate from 64 samples, so its standard error is about 0.125, which is coarse against doc 04's 0.90 threshold. Doc 06 therefore treats it as one signal among seven rather than as a verdict, and doc 16's milestone 4 calibration is where we find out whether 64 permutations is enough. If it is not, the sketch goes to 128 permutations at 512 bytes per row, which costs about 4 percent of doc 10's byte budget, and that is a price worth paying if the alternative is banning honest fetchers.

**Corpus near duplicate clustering** uses LSH banding over the same sketch, with 8 bands of 8 rows, which puts the detection threshold at roughly 0.77 Jaccard. Candidates that collide in any band are compared exactly on their sketches and clustered by union find. This runs in the same batch job as 11.7's global pass, over the same buckets, and publishes cluster assignments to `open-index/umi-dedup`.

A `simhash` over the same shingles is also stored, 64 bits, because Hamming distance on simhash is much cheaper than the MinHash path for the specific question doc 09 asks about soft 404s, which is "are most of this host's pages the same page". Two sketches for two access patterns, both computed in one pass over the shingles, and the marginal cost of the second is negligible.

Clustering is published, not applied. Doc 02 covers the argument: recent work suggests aggressive global near duplicate removal costs diversity, the right threshold depends entirely on what you are building, and a corpus that has already been deduplicated cannot be un-deduplicated. We publish the cluster ids and the similarity, and the consumer decides.

## 11.9 Cost

Doc 01 budgets 3 to 8 ms per page per core, which at 250 pages per second is 1.25 cores at the midpoint. The breakdown for a 150 KB document:

```
html5ever parse                  1.5 - 3.0 ms
main content scoring             0.3 - 0.6 ms
markdown serialisation           0.3 - 0.7 ms
link resolution and canon        0.1 - 0.3 ms   (50 links)
metadata and JSON-LD             0.1 - 0.4 ms
language detection               0.2 ms         (4 KiB only)
shingle + 64 minhash + simhash   0.8 - 1.5 ms
digests                          0.1 ms         (blake3 is not the problem)
                                 -------------
total                            3.4 - 6.8 ms
```

Parsing dominates and the sketch is second. If the budget is missed, the sketch is the first thing to move, because one permutation hashing can produce a 64 value sketch in a single pass at roughly a third of the cost with a modest accuracy loss on short documents. That is a milestone 3 decision and not a now decision.

Extraction runs on a bounded rayon pool sized to leave a core free for the fetch loop, because a saturated extraction pool that starves the fetcher turns a CPU problem into a politeness problem when leases start expiring.

## 11.10 Versioning and skew

The extractor version is `umi-extract/<semver>` and it appears in the doc 04 receipt, in the doc 10 segment header, and as a column in the published Parquet.

A patch release may fix a crash or a memory issue and must not change output for any input. A minor release may add a column or a metadata field, since adding a column does not change existing digests. A major release changes extraction behaviour and therefore changes digests, and doc 06 handles the transition by comparing digests only between fetchers on the same major version and falling back to pulling raw bytes for central re extraction when versions differ.

The fleet will always be running several versions at once, because we cannot force a volunteer to upgrade. The coordinator publishes the current and minimum supported versions in the doc 04 `HelloAck`, refuses deliveries from below the minimum, and the minimum moves forward slowly, on a published schedule, with at least 30 days of notice.

There is a golden corpus, and it turned out to want to be two of them.

The narrow one is in the repository, at `crates/umi-extract/corpus`. Every document in it was chosen because it breaks extractors in a particular way: single page applications, AMP variants, `<noscript>` heavy pages, RTL scripts, CJK without spaces, HTML emitted by Word, tables used for layout, documents with three `<body>` tags, and 200 KB of nested `<div>`. Its expected output is committed next to it as readable markdown, so a change shows up in review as a diff somebody can read rather than as a hash that moved. It is small enough that every build runs it, and it is good at the failures somebody already thought of.

The wide one is 10000 real pages off 333 real hosts, and it is good at the ones nobody thought of. It cannot be committed, because it is 1.49 GB of HTML, so it is published as a dataset at `open-index/umi-golden` and what the repository holds is `crates/umi-extract/golden/wide.txt`: three truncated blake3 digests per page, for the input, the markdown and the plain text. The corpus file's own digest is the first line, checked before any page is compared, because the wrong corpus should say so once rather than ten thousand times. CI downloads the file, caches it by that digest, and runs the comparison on Linux, macOS and Windows.

Neither corpus is optional and the reason is doc 06. A fetcher somebody else runs is trusted by comparing its extraction of a page against ours, so a one in ten thousand divergence between two honest implementations is indistinguishable from a dishonest one. Ten thousand pages is the rate at which that claim has to be measured, and 23 hand picked pathologies is the rate at which a regression has to be readable.

Any diff in either is an intentional major version bump with the digests updated in the same commit, or a bug. There is no third case, and these tests are what make the determinism rule in 11.1 real rather than aspirational.

Measured, rather than asserted: the wide corpus extracts to the same 10000 markdown digests and the same 10000 text digests on server1, server2 and server3, and in a debug build and a release build of the same source, which are different machines and different compilations of different optimisation levels. The release comparison takes about 75 seconds on eight cores and the debug one takes about 12 minutes, which is why CI runs the release one.
