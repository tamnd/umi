# 10 The .umi file format

## 10.1 What this file is actually for

It is tempting to read "single file columnar format inspired by DuckDB and SQLite" as a request for a small analytical database. It is not, and building one would be a mistake. The `.umi` file has one job: be the landing zone between the writer and the publisher, on a machine with no spare disk, on a crawl that must not lose data when the process dies.

The lifetime of a `.umi` segment is about 90 seconds of filling and under 10 minutes of publishing. After that it is deleted, because doc 01 established that the fleet has 342 GB of free disk against 390 GB of daily output, and doc 12 makes deletion a hard rule rather than a cleanup job. Nothing outside umi ever reads a `.umi` file. Consumers read the Parquet in `open-index/*`.

That reframes the requirements completely.

**What matters.** Crash safety, because the process will be killed and the box will reboot and losing an hour of fetching is losing real money. Write cost, because we have 2 vCPU per host and extraction already wants most of it. Compactness, because disk is the binding constraint and every byte saved is a byte of headroom. Conversion cost to Parquet, because that conversion happens on every segment forever.

**What does not matter.** Query performance, because nothing queries it. Predicate pushdown, because there are no predicates. Random access, because the only reader is a sequential converter and the occasional debugging command. Long term stability of the on disk layout, because no file survives an upgrade. Multi writer support, because there is exactly one writer per file. Updates and deletes, because the file is append only by construction.

So this is a write optimised append only columnar container with a strong crash story and a cheap path to Parquet. It is much closer to a WAL segment that happens to be columnar than to DuckDB. The DuckDB and SQLite influence is real but it is narrow: SQLite's crash discipline of checksums plus a commit record written last, and DuckDB's encoding cascade of cheap lightweight codecs under one general purpose compressor.

## 10.2 The 6 KB per page budget

Docs 01, 04 and 09 all spend a number that this document owes them: 6 KB of stored output per page. Here is where it comes from.

Take a median index worthy HTML page at about 150 KB of raw bytes. Doc 11 extracts it into markdown, plain text derived from that markdown rather than stored separately, an outlink list, snippets, a small subset of headers, scalar metadata, and digests. Encoded into a shoal alongside 5000 of its neighbours, it costs roughly this:

```
column group          logical    encoded   notes
url + final_url          120 B      28 B   FSST, plus prefix elision within a host run
markdown                7000 B    2000 B   zstd-3 with a per shoal trained dictionary
outlink targets         4000 B     900 B   50 links, FSST, heavy prefix sharing
outlink anchors         1000 B     300 B   FSST
snippets                 400 B     150 B   title, description, og, h1..h3
headers kept             300 B      60 B   dictionary, the values repeat hard
scalar metadata          120 B      25 B   delta plus bit packing
digests                  114 B     114 B   body, chunk root, extract, url_key, pld_id
minhash                  256 B     256 B   64 x u32, incompressible by design
                                  ------
median row                        3833 B
```

The median row is under 4 KB. The mean is not, because the distribution has a long right tail and that tail is where the bytes live: a long form article carries 40 KB of markdown, a documentation page carries 200 outlinks, a directory page carries 800. Measured on the ccrawl-cli samples the mean sits around 1.55 times the median for this shape of content, which puts the mean at 5.9 KB. We plan on 6 KB and we round up rather than down, because the failure mode of underestimating storage on these boxes is the crawl stopping.

Doc 04 uses 6 KB for a different quantity, the size of a single delivered extraction on the wire, and it lands in the same place by a different route. A single record compressed on its own cannot amortise a dictionary across neighbours, so markdown costs about 2.6 KB instead of 2.0 and links cost about 1.4 KB instead of 0.9, but a delivery carries no minhash duplication and no per shoal overhead. Median on the wire is about 4.7 KB, mean about 7 KB, and 6 KB is the honest planning figure for both. Both numbers are milestone 3 gates in doc 16 and if the real corpus says 9 KB then doc 01's capacity plan changes, not this paragraph.

Two things are deliberately absent from that table. We do not store plain text alongside markdown, because plain text is a pure function of markdown and regenerating it costs less than storing it. And we do not store raw HTML at all, anywhere, past the 24 hour audit ring buffer in doc 04. Raw HTML at 150 KB per page would be 25 times the storage cost and would blow the disk budget by two orders of magnitude. That is the single decision that makes the whole storage plan work, and it is also the decision that means we can never re extract from history. Doc 17 lists that as a known and accepted loss.

## 10.3 Segments, shoals and the arithmetic between them

A **segment** is one `.umi` file. It seals at 128 MB or 15 minutes, whichever comes first. Doc 01 explains why 128 MB rather than something comfortable in the gigabytes: the publish loop is on the critical path and a segment that takes an hour to fill is an hour of data at risk and an hour of latency added to every consumer.

A **shoal** is a row group. It seals at 16384 rows or 32 MiB of encoded output, whichever comes first. At 6 KB per row the byte cap binds first, at about 5600 rows. The row cap only binds on segments dominated by cheap rows, which in practice means revalidation heavy segments full of 304 responses where a row is a few hundred bytes and 16384 of them fit in 4 MiB.

The arithmetic that follows:

```
shoal      32 MiB / 6 KB          = ~5600 pages
segment    128 MB / 32 MiB        = 4 shoals, ~21000 pages
fill time  21000 / 250 pages/s    = 85 seconds per host
in flight  85 s fill + 10 min publish = ~8 unpublished segments per host
local data footprint              = 8 x 128 MB = ~1 GB per host, steady state
```

One gigabyte of segment data resident per host, against 67 GB free on the tightest box. The disk pressure from doc 01 is entirely about what happens when publishing stalls, not about steady state, and doc 15's backpressure ladder is what covers the stall.

Three stream kinds share the container and differ only in schema: `pages`, `receipts`, and `robots`. The header names the stream and the schema id, and a reader that does not recognise the schema id refuses to open the file rather than guessing. This keeps one writer, one reader and one crash story instead of three.

## 10.4 File layout

```
offset 0
  +--------------------------------------------------+
  | header                              4 KiB fixed  |
  +--------------------------------------------------+
  | shoal 0                                          |
  |   frame header  "SHFR"              64 bytes     |
  |   column chunk 0  (aligned to 64 bytes)          |
  |   column chunk 1                                 |
  |   ...                                            |
  |   shoal directory                                |
  |   commit record "SHOL"              32 bytes     |
  +--------------------------------------------------+
  | shoal 1                                          |
  |   frame header                                   |
  |   ...                                            |
  |   commit record                                  |
  +--------------------------------------------------+
  | ...                                              |
  +--------------------------------------------------+
  | footer                                           |
  |   shoal index                                    |
  |   schema                                         |
  |   segment statistics                             |
  +--------------------------------------------------+
  | footer length                       u32 LE       |
  | footer digest                       16 bytes     |
  | magic  "UMI1"                       4 bytes      |
  +--------------------------------------------------+
EOF
```

The header is fixed at 4 KiB, written once at create, and never rewritten. It carries the magic `UMI1`, the format version, the stream kind, the schema id, a segment ULID, the owning coordinator id, the creation time, the canonicalisation version from doc 11.2, the extractor version, the crawl profile id from doc 13, and a blake3-128 checksum of the preceding bytes. Pinning the canonicalisation and extractor versions in the header rather than per row is what makes a segment self describing without paying for it on every page.

The magic appears at both ends. At the start it identifies the file to `file(1)` and to a human. At the end it is the seal marker: a file whose last 4 bytes are not `UMI1` was not closed cleanly and goes down the recovery path in 10.7.

A column chunk is aligned to 64 bytes so that bit packed and fixed width buffers land on an alignment the decoder is happy with. The padding costs a few hundred bytes per shoal and is not worth optimising away.

The shoal directory sits after the column data rather than before it, because the writer does not know a chunk's encoded length until it has encoded it, and writing the directory first would mean either a seek back or a two pass encode. It lists, per column, the byte offset, the encoded length, the encoding id, the null count, the value count, the min and max for orderable types, and a blake3-128 checksum of the chunk bytes.

Every shoal opens with a 64 byte frame header carrying the magic `SHFR`, the shoal ordinal, the row count, the byte length of the whole shoal including both the frame header and the commit record, and a blake3-128 checksum of the frame header's own first 48 bytes. It exists for one reason: 10.7's recovery scan walks the file forward, and a scan that only has commit records at the end of each shoal has no way to find the first one without already knowing how long the shoal is. The frame header is the thing that tells it. Sealing writes the frame header first with the length field zero, then the column data, then the directory, then rewrites the 8 length bytes in place, then writes the commit record. A frame header whose length is still zero means the process died mid shoal and the scan stops there, which is the same answer it would give for a torn commit record.

The commit record is 32 bytes, which is the constraint the rest of the shoal has to live inside. After the magic, the shoal ordinal and the checksum there is room for a 32 bit offset and a 32 bit length, so every offset in a shoal is relative to the start of that shoal and a single shoal cannot exceed 4 GiB. The writer's 32 MiB seal threshold is three orders of magnitude below that, but a caller who forces a seal on an absurd batch gets a `TooLarge` error rather than a silently wrapped offset.

## 10.5 The row schema

The `pages` schema, which is the one that matters. Types are Arrow types because the reader materialises Arrow and doc 12 converts Arrow to Parquet.

```
url                 utf8            canonical, doc 11.2
final_url           utf8            after same host redirects, null if equal to url
url_key             fixed[10]       80 bit fingerprint, doc 08
pld_id              fixed[8]
host                utf8            dictionary encoded, sorted within a shoal
fetched_at_ms       uint64
status              uint16
outcome             uint8           dictionary of the doc 04 Outcome enum
tier_used           uint8
tier_path           list<uint8>
content_type        utf8            dictionary
content_length      uint32
lang                fixed[3]        BCP 47 primary subtag, dictionary
body_digest         fixed[32]
chunk_root          fixed[32]
extract_digest      fixed[32]
markdown            utf8            the main content, doc 11
title               utf8
description         utf8
headings            list<utf8>
snippets            list<struct{kind:uint8, text:utf8}>
links               list<struct{href:utf8, anchor:utf8, rel:uint16, kind:uint8}>
headers_kept        map<utf8, utf8> the doc 11.5 subset
content_usage       utf8            AIPREF, doc 07.5, null when absent
minhash             fixed[256]      64 x u32 little endian
simhash             uint64
text_bytes          uint32
link_count          uint32
fetcher_id          fixed[32]
verification        uint8           doc 06 outcome: local, quorum, replayed, unverified
robots_checked_ms   uint64
crawl_profile       uint32          doc 13
```

Four of these columns hold a byte whose meaning is fixed for the life of the format, since a segment written today is read by a build from two years from now. `outcome` is the table in doc 04.5. `tier_used` and every entry of `tier_path` are doc 05.2's ladder, 0 through 4. `verification` is 0 local, 1 quorum, 2 replayed, 3 unverified. `links.kind` is 0 body, 1 nav, 2 link, 3 redirect, 4 sitemap, 5 feed, from doc 11.4. `snippets.kind` is 0 title, 1 description, 2 h1, 3 h2, 4 h3, 5 the JSON-LD headline. In every one of them codes are appended, never renumbered, and a retired code stays reserved. A reader that meets a code it does not know keeps the row.

The `snippets` list repeats what `title`, `description` and `headings` already hold, and that is on purpose. It exists so that a consumer building a search result reads one column instead of four, and so that the JSON-LD headline survives, which is frequently the editorial title where `<title>` is the same thing with the site name bolted on.

`body_digest` on a 304 holds the digest of the response headers rather than a body digest, because there is no body. This is the only column in the schema whose meaning depends on another column, and it is worth the exception: without it two 304s from the same origin an hour apart are byte identical rows, and doc 05.3 needs a way to notice a revalidator that changed the cache directives without changing the content. The `outcome` column says which reading applies.

`receipts` is the doc 04 `Receipt` flattened, one row per delivery, including the signature, so that anyone can re verify our published corpus against the fetcher keys without trusting us. `robots` is host, fetch time, status, raw text, and the parsed decision summary from doc 07.4.

Rows arrive in fetch completion order and are written in that order. We do not sort within a shoal, with one exception: the writer holds a small reorder window of 4096 rows and groups by host inside it, because host adjacency is what makes URL prefix elision pay and the window costs 25 MB of buffer for a compression win measured in doc 16's milestone 3 gate. If the win is under 5 percent the window comes out.

## 10.6 Encodings

One cascade, applied per column chunk, chosen by the writer from a fixed set. No sampling based auto selection: the writer knows the column, the column's encoding is fixed by the schema, and removing the choice removes a class of bug and a chunk of CPU. Doc 02 explains why we take the shape of this from BtrBlocks and FastLanes without trying to be Vortex.

**Fixed width integers and timestamps.** Frame of reference against the chunk minimum, then bit packing in the FastLanes 1024 value interleaved layout so that decode is branch free SIMD. Timestamps within a shoal span at most 15 minutes, so `fetched_at_ms` deltas fit in 20 bits and often fewer. This column costs about 2.5 bytes per row rather than 8.

**Small enums.** `outcome`, `tier_used`, `status`, `lang`, `content_type`, `verification` are dictionary encoded with the dictionary in the chunk header and the codes bit packed. `status` in a healthy crawl is 200 more than 90 percent of the time, so the code column is under 2 bits per row.

**Short high cardinality strings.** `url`, `final_url`, `links.href`, `links.anchor`, `title` use FSST with a symbol table trained per chunk on a 16 KiB sample. FSST gives random access at roughly memcpy speed and 2 to 3 times compression on URL shaped data, and it composes with what comes next. URLs additionally get prefix elision against the previous value, which inside a host run reduces most URLs to their differing tail. Together these two are the reason a URL costs 28 bytes and not 120.

**Long text.** `markdown` is the only column where a general purpose compressor earns its keep. Plain zstd level 3, no dictionary. Level 3 rather than higher because doc 01 gives us 2 vCPU and level 3 compresses at around 300 MB/s per core against level 9 at 25 MB/s, and the ratio difference on already extracted markdown is under 8 percent.

This spec originally called for a zstd dictionary trained per shoal over a 2 MiB sample and stored in the chunk. It was implemented and measured, and it lost on both axes: writing a segment went from 2095 ms to 22439 ms, and the output was 4.9 percent larger. A dictionary is worth having when the values are short and repetitive, and pages of extracted markdown are neither, so the sample teaches the dictionary nothing that zstd's own window would not have found within the first few kilobytes of each block. The training cost is paid on every shoal and the ratio loss comes from spending the chunk header on a table nothing hits. The dictionary is gone, and 10.10's byte for byte pass through into Parquet is the reason it can never come back: a dictionary compressed frame is not a frame the Parquet reader can take.

**Digests, minhash and simhash.** Stored raw. They are uniformly random by construction and any attempt to compress them costs CPU to produce a slightly larger output. The writer skips them explicitly rather than discovering this per chunk.

**Lists and maps.** The Arrow layout, and a child column encoded by its own rule. Arrow holds a list's shape as absolute offsets, but what goes on disk is the per row lengths, because the deltas of a monotonic offsets column are exactly those lengths and storing them directly saves the reader a subtraction pass and saves the writer from having to decide what to do about an offsets column that does not start at zero. The reader runs a prefix sum to get Arrow's offsets back. `links` decomposes into four sibling child columns rather than an interleaved struct, which is what lets `links.href` share a FSST symbol table across all links in the shoal.

**Nulls.** A validity bitmap per chunk, omitted entirely when the chunk has no nulls, which is the common case for most columns.

Every chunk header records its encoding id and version. A reader that meets an encoding id it does not know fails loudly. There is no fallback path, because a segment that a reader cannot decode is a bug to fix within the hour, not a compatibility problem to route around.

## 10.7 Crash safety

The writer will be killed. Assume SIGKILL at the worst possible byte offset, assume the box loses power, and design so that the answer is always "we lost at most the shoal in progress".

**The commit record.** After a shoal's column chunks and directory are written, the writer calls `fdatasync`, then appends a 32 byte commit record, then calls `fdatasync` again. The record is a 4 byte tag `SHOL`, the shoal index, the byte offset of the shoal's first chunk, the byte length of the shoal, and a blake3-128 checksum over the shoal directory. Two syncs per shoal, so at 4 shoals per segment and a segment every 85 seconds that is one sync every 10 seconds. That is affordable even on server2's rotational disks, and it is exactly why shoals are 32 MiB and not 4 MiB.

**Opening a file with no footer.** Scan forward from the header. At each step read the 64 byte frame header, take the shoal length from it, and read the commit record that sits at the end of that length. The frame header is what makes the step possible at all, since the commit record is behind data whose size only the frame header knows. Stop at the first frame header or commit record that fails its checksum, is truncated, carries a zero length, or points past EOF. Everything before it is intact and readable. Truncate the file at that point and continue appending, or seal it as a short segment and publish it. Both are safe and the writer picks based on whether the segment is still the active one.

**Torn writes inside a shoal.** Cannot corrupt a committed shoal, because a shoal is only committed after its own bytes are durable. A torn shoal has no valid commit record and is simply not part of the file.

**Torn footer.** The footer is written last, followed by its length, its digest, and the magic, in one `write` and one `fdatasync`. A file whose trailing magic is missing or whose footer digest fails falls back to the commit record scan, which reconstructs everything the footer would have told us at the cost of a linear pass over 128 MB. That pass takes under a second and it happens only after a crash.

**Bit rot.** Every chunk carries a blake3-128 checksum, the directory carries one, and the footer carries one. The publisher in doc 12 verifies every chunk checksum as it converts, so nothing reaches Hugging Face without having been checksummed on the way out. This is not paranoia about disk hardware so much as the cheapest possible guard against a writer bug producing plausible garbage, and it costs about 3 percent of the conversion CPU.

**What we do not do.** No double write, no journal, no torn page protection beyond the commit record, no `O_DIRECT`, no `fsync` on the directory entry past file creation. Append only files plus commit records give the same guarantee for a fraction of the write amplification, and write amplification on server2's disks is the thing to avoid.

**Measured, rather than asserted.** Everything above is a claim about what happens when a process dies, and doc 16's gate 1.3 is where it stops being a claim. The suite lives in `crates/umi-file/tests/crash.rs`. It spawns a writer, kills it a hundred times at offsets drawn from a fixed seed, and checks that what the killed process left behind is a prefix of the bytes a clean run produces and nothing else. That prefix result is what licenses the rest of the suite to work by cutting a good file rather than by killing a process, which is how it can then check a hundred exact offsets, a tail of somebody else's bytes where the short file should have been, a shoal whose directory changed under a valid commit record, and a column chunk that changed under a valid directory. Every one of them has to come back as the committed prefix, with the shoal that was in flight missing and no shoal before it.

The two digest checks are the ones worth naming, because they are the ones a reasonable person would call redundant. Removing either leaves the whole suite passing except for the one case written for it, which is the argument for having written those cases rather than trusting that a torn write always shortens a file.

## 10.8 Writer memory

The writer holds one shoal being filled in unencoded column builders and one shoal being encoded and written. Unencoded markdown dominates: a 32 MiB encoded shoal is roughly 90 MB of builders. Two in flight plus training and slack is where the default comes from.

```
default budget          256 MB
  filling shoal          ~90 MB
  encoding shoal         ~90 MB
  FSST symbol training    ~4 MB
  slack                   ~71 MB
```

Doc 03.4 caps `umid` on server1 at 1.5 GB RSS and doc 01 says server1 has essentially no free memory. So the writer takes a floor of 64 MB, and under the floor the shoal cap drops to 8 MiB and only one shoal is in flight at a time. That costs compression ratio, because symbol tables are trained on a quarter as much data and prefix elision has less to work with. The expected cost is 8 to 12 percent more bytes, and the measured cost is a milestone 3 number. It is the correct trade on a box with zero free RAM and it must be a configuration value, not a rebuild.

Encoding is done on a rayon pool, one column chunk per task, because columns are independent and the encode of a full shoal is around 120 ms of single core work that we would rather not add to the fetch loop's latency.

## 10.9 Reading

The reader opens the file, parses the footer, and hands out shoals. There is no buffer pool, no page cache management and no eviction policy, because the whole file is 128 MB and the only reader is a converter that walks it once.

This spec originally said the reader mmaps the file and exposes uncompressed fixed width columns as slices into the mapping with no copy at all. That is not reachable here. The workspace denies `unsafe_code`, and every safe mmap wrapper is unsound by construction, because another process truncating the file turns a live slice into a SIGBUS with no way for the type system to have stopped it. So the reader does positioned reads instead: one `read_exact_at` per shoal into a reusable buffer, then decode out of that buffer. The read is a single sequential 32 MiB call rather than a page fault storm, the kernel's readahead does the work mmap would have done, and the cost against the mmap plan is one memcpy per shoal, which is under a percent of a decode that has to run zstd and FSST anyway. Digest and minhash columns still cost nothing to decode, they just get sliced out of the shoal buffer rather than out of a mapping. Bit packed columns decode into a caller supplied reusable buffer, one shoal's worth at a time. FSST and zstd columns must be decompressed, and that is unavoidable and is most of the read cost.

The API is deliberately small:

```rust
pub struct Segment { /* file handle + footer */ }

impl Segment {
    pub fn open(path: &Path) -> Result<Segment>;
    pub fn open_recover(path: &Path) -> Result<(Segment, RecoveryReport)>;
    pub fn header(&self) -> &Header;
    pub fn stats(&self) -> &SegmentStats;
    pub fn shoals(&self) -> usize;
    pub fn shoal(&self, i: usize) -> Result<ShoalReader<'_>>;
}

impl ShoalReader<'_> {
    pub fn rows(&self) -> usize;
    pub fn column(&self, name: &str) -> Result<ColumnChunk<'_>>;
    pub fn to_arrow(&self, cols: &[&str]) -> Result<RecordBatch>;
    pub fn verify(&self) -> Result<()>;   // every chunk checksum
}
```

`to_arrow` taking a column subset is the only concession to projection, and it exists because doc 15's DuckDB dashboard occasionally wants counts and status codes out of a local segment without decompressing 100 MB of markdown to get them.

## 10.10 Getting to Parquet

Doc 12 owns the schemas and the upload. What belongs here is why the conversion is cheap, because if it were not the publish budget in doc 01 would not close.

A shoal becomes a Parquet row group, one to one. The logical values never change, so there is no re extraction, no re canonicalisation, and no schema mapping beyond names.

Column by column: dictionary encoded columns map onto Parquet `RLE_DICTIONARY` with the dictionary carried over and the codes re emitted, which is cheap. zstd compressed `markdown` passes through byte for byte, since the Parquet page compression codec is also zstd and the frame is already valid, so the single largest column costs zero CPU to convert. Bit packed integer columns are unpacked and re emitted as Parquet's own bit packing, which is a decode and encode but on cheap data. FSST columns are the real cost, because Parquet has no FSST, so `url`, `links.href` and friends decode from FSST and re encode as plain plus zstd, which loses some ratio and spends most of the conversion's CPU.

Measured expectation is around 40 percent of one core to convert a 128 MB segment in under 30 seconds, which fits inside doc 12's 10 minute seal to deleted budget with the remainder going to upload and verification. If FSST conversion turns out to dominate, the fallback is to store `url` as plain plus zstd in `.umi` too and accept a larger local file, which trades disk we barely have for CPU we barely have. That trade gets made on measurement, not now.

## 10.11 What this format is not

It is not the published format. Nobody outside umi should ever be handed a `.umi` file, and the CLI does not have an export command that produces one.

It is not stable. The format version in the header exists so that a reader can refuse an unknown file, not so that old files keep working. No segment lives long enough for a migration to be needed, and the correct response to a format change is to drain the writers and restart them.

It is not a database. No indexes, no predicates, no transactions across files, no concurrent writers, no readers while writing except the publisher reading committed shoals of the active segment, which is safe precisely because commit records make the committed prefix well defined.

It is not where state lives. Doc 08 owns everything mutable, in its own files with its own lifecycle, and the two never share a directory. A segment can be deleted the moment doc 12 says so, without consulting anything, and that property is worth more than every feature listed above.
