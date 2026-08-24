# 12 Publishing

## 12.1 Why this document is on the critical path

On most crawlers, publishing is the last thing that happens and the first thing that slips. Here it is the thing that keeps the crawl alive. Doc 01 measured 342 GB of free disk across the fleet against roughly 390 GB of output per day, so the local disk holds under a day of crawling even if it holds nothing else. Publishing is not an export step, it is the mechanism by which server1, server2 and server3 stay under their disk limits, and if it stops the crawl stops.

That gives this document a hard budget. **From segment seal to local file deleted, 10 minutes at p99.** Doc 03 quotes 60 minutes at p50 from fetch to published, and the difference between the two is segment fill time, not publish time.

It also gives the design a bias. Everything here prefers a simple operation that always completes over a clever one that is faster on average, because the failure mode of a stalled publisher is a full disk and a stopped crawl, and the failure mode of a slightly slow publisher is nothing at all.

## 12.2 The pipeline

```
segment sealed (128 MB .umi)
   │
   ├─ 1  verify every chunk checksum                     ~1 s
   ├─ 2  convert shoals to Parquet row groups            ~30 s at 0.4 core
   ├─ 3  digest the Parquet file, blake3 and sha256      ~2 s
   ├─ 4  upload to Hugging Face                          ~13 s at 10 MB/s burst
   ├─ 5  verify the remote copy independently            ~3 s sampled, ~15 s full
   ├─ 6  append and sign the manifest entry, push it     ~2 s
   ├─ 7  write remote locations into the state ledger    ~1 s
   └─ 8  GC deletes the local .umi and the local .parquet
                                                        ------
                                              p50 total  ~55 s
```

Steps 1 through 3 are doc 10's business and are already specified there. This document owns 4 through 8.

Bandwidth: each host produces 250 pages per second at 6 KB, so 1.5 MB/s of sustained outbound is the floor, which doc 01 put at 11.8 TB per month fleet wide against a 96 TB allowance. Uploads burst at whatever the link gives, and the budget above assumes 10 MB/s, which makes a 128 MB segment 13 seconds. If a host can only sustain 1.5 MB/s of upload then a segment takes 85 seconds to push and the pipeline is exactly break even with production, with no margin. Measuring real upload throughput to Hugging Face from each box is a milestone 1 gate alongside the inbound measurement in doc 01.

The publisher runs on server3 for the fleet, per doc 03.4, but each coordinator can publish its own segments and does when server3 is unavailable. There is no central publish queue, because a central publish queue is a single point of disk failure for the other two boxes.

## 12.3 Parquet

One `.umi` segment becomes exactly one Parquet file. One shoal becomes exactly one row group. The mapping is deliberate: it means conversion is streaming, it means a corrupted segment affects exactly one file, and it means the Parquet row groups are 32 MiB, which is inside the range every query engine is happy with.

Writer settings, fixed:

```
compression            zstd level 3
row group size         one shoal, ~32 MiB, ~5600 rows
data page size         1 MiB
dictionary encoding    on for host, content_type, lang, outcome, status, tier_used, verification
statistics             on, page level and column chunk level, for all orderable columns
column index           written
offset index           written
bloom filters          url_key, text_digest
sorting columns        none declared
writer version         2.0
```

Bloom filters on `url_key` and `text_digest` and nowhere else. Those two are the columns people do point lookups on, "is this URL in the corpus" and "is this content in the corpus", and a bloom filter turns both into one row group read instead of a full scan. Filters on anything else are bytes we pay for and nobody uses.

Statistics matter more than they look. A consumer filtering on `fetched_at_ms` or on `host` should read three row groups, not the whole file, and that only works if the min and max are written and the writer did not sort rows in a way that makes every row group's range cover everything. Doc 10's 4096 row reorder window groups by host, which incidentally makes the host statistics useful. That is a nice side effect and not a reason to keep the window.

The schema is doc 10.5's, mapped one to one, with names unchanged and types mapped to the obvious Parquet logical types. `fixed[N]` columns become `FIXED_LEN_BYTE_ARRAY`. `map<utf8,utf8>` becomes the standard Parquet map layout. Nothing is flattened and nothing is renamed, so a consumer reading the Parquet is reading exactly the schema in this spec.

## 12.4 Repository layout

Five families of dataset repository under the `open-index` organisation.

**`open-index/umi-pages-<YYYY>w<WW>-<NN>`** is the corpus. One repository covers one ISO week and one 300 GB slice of that week's output, with `NN` allocated on demand starting at `00`. At 750 pages per second the fleet produces about 453 million pages per week, which at 6 KB is 2.7 TB, so a normal week is 9 repositories. Over the 4.2 years doc 01 computes for 100 billion pages that is roughly 2000 repositories, which is the number doc 01 quotes and which is large enough that the naming scheme has to be mechanical rather than curated.

Inside a repository:

```
umi-pages-2026w34-03/
  README.md                       dataset card, generated
  data/
    20260817/
      01K2M8Q0P7R3XN5.parquet     one segment, ULID named
      01K2M8QF2A1C9WZ.parquet
      ...
    20260818/
      ...
  _manifest/
    20260817.json                 every file that day, with digests
    20260817.json.sig             detached ed25519
```

A day folder holds about 3100 files, which is under Hugging Face's 10000 files per folder guidance, and a repository holds about 2300 files across its lifetime, which is well under the 100000 files per repository guidance. Files are 128 MB, which is inside the 200 MB to 2 GB range that downloads in parallel efficiently and nowhere near the per file ceiling.

**`open-index/umi-receipts-<YYYY>w<WW>-<NN>`** is the audit trail. Doc 04's receipt, flattened, one row per delivery, including the fetcher's Ed25519 signature. This is what makes the claim "you can verify our corpus without trusting us" true rather than rhetorical: the fetcher public keys are published, the receipts are signed, and anyone can check that a page we published was delivered by a fetcher that signed for it. At roughly 300 bytes per row after encoding, receipts cost about 30 TB at 100 billion pages, or 5 percent of the corpus, and they are worth every byte.

**`open-index/umi-robots`** is the longitudinal robots.txt corpus described in doc 07.4. Every fetch of every `robots.txt`, raw text plus parsed decision summary. About 50 million hosts at a few hundred compressed bytes each per snapshot, so this is small and it is the single most cited thing we are likely to publish, because nobody else has it.

**`open-index/umi-dedup-<YYYY>w<WW>`** carries doc 11's exact digest to cluster mapping and the near duplicate cluster assignments. Roughly 48 bytes per page, so about 4.8 TB at full scale.

**`open-index/umi-meta`** is one small repository that is the entry point for everything. It holds the registry of every other repository with its week, slice, row count and byte count, the manifest chain heads, the fetcher public key directory, the ban list from doc 06, the block list from doc 07.7, the tracking parameter list from doc 11.2, every schema version ever published, and the canonicalisation version history. A consumer starts here and discovers everything else. It is also the only repository we ever rewrite.

We do not publish a separate link edge list. Fifty outlinks per page at 100 billion pages is five trillion edges, which is larger than the corpus that produced it, and the edges are already in the `links` column. Anyone building a link graph derives it, and Spec 2050 covers what to do with it after that.

## 12.5 Manifests, signatures and the chain

Every day folder in every repository has a manifest, and the manifest is the actual published artifact. The Parquet is just bytes; the manifest is the claim.

```json
{
  "manifest_version": 1,
  "repo": "open-index/umi-pages-2026w34-03",
  "day": "20260817",
  "prev": "blake3:7f2a...",
  "canon_version": "canon/1",
  "schema_id": "umi-pages/1",
  "files": [
    {
      "path": "data/20260817/01K2M8Q0P7R3XN5.parquet",
      "bytes": 134217728,
      "rows": 21043,
      "blake3": "blake3:9c11...",
      "sha256": "sha256:44de...",
      "segment_ulid": "01K2M8Q0P7R3XN5",
      "coordinator": "server3",
      "extractor": "umi-extract/0.4.1",
      "fetched_at_min_ms": 1755388800000,
      "fetched_at_max_ms": 1755389640000,
      "verification": { "local": 18220, "quorum": 2401, "replayed": 402, "unverified": 20 }
    }
  ],
  "digest": "blake3:1b8e..."
}
```

`prev` is the digest of the previous day's manifest in the same repository, and the repository's first manifest points at the manifest chain head recorded in `umi-meta` at the time the repository was created. That makes the whole published corpus a hash chain. Someone who has verified the head has verified everything under it, and quietly rewriting history requires forking the chain visibly.

The detached signature is Ed25519 over the canonical serialisation of the manifest, using the publishing key, which is a different key from the crawl identity key in doc 07.2 and from the coordinator lease signing key in doc 04. Three keys with three purposes, published in `umi-meta`, rotated on different schedules, and none of them able to do another's job.

Two digests per file, blake3 and sha256, because blake3 is what we compute everywhere else and sha256 is what every other tool on Earth can check without installing anything.

The `verification` counts in the manifest are doc 06's outcome distribution for that file. Publishing them means a consumer who only wants pages we fetched ourselves, or only pages that survived cross fetcher quorum, can filter at file granularity before downloading anything. That is a genuinely useful thing to offer and nobody else offers it.

## 12.6 Upload mechanics

Hugging Face dataset repositories over the HTTP API, using the multi file commit endpoint rather than one commit per file. Three thousand commits a day per repository would be abusive and would hit rate limits; we batch into one commit per 32 files or per 5 minutes, whichever comes first, and the manifest for a day is committed last, after every file it references.

That ordering is not incidental. A manifest is only pushed once every file in it is durably present, so a consumer who trusts the manifest is never pointed at a file that does not exist. A crash between file upload and manifest push leaves orphan Parquet files, which are harmless, are detected by the reconciliation pass in 12.8, and are either adopted into the next manifest or deleted.

Hugging Face's Xet backed storage deduplicates at the content chunk level rather than the file level, which is worth understanding because it changes what a re upload costs. Re uploading a Parquet file that differs from a previous one in a few row groups transfers only the changed chunks. We almost never re upload, so the practical benefit is small, but it makes the recovery path in 12.8 cheap enough to use freely.

Retries are exponential with jitter, capped at 6 attempts over roughly 10 minutes, and a segment that fails all 6 goes to a local retry queue and stays on disk. That is the case where doc 15's backpressure ladder starts throttling the crawl, and it is the reason the ladder exists.

The known limits as of August 2026, all of which must be re checked before milestone 5: a soft ceiling around 300 GB per repository with a documented process for asking for more, a recommendation of under 100000 files per repository and under 10000 per folder, and per file sizes that should stay well under 20 GB. Our layout sits comfortably inside all of them by construction. At the point where umi is creating 9 new repositories a week, this stops being a technical question and becomes a conversation with Hugging Face, and doc 16 makes that conversation a milestone 5 deliverable rather than a surprise.

## 12.7 The GC rule

A local file is deleted when, and only when, all four of these are true, in this order:

1. The remote object exists and its size matches.
2. An independent read of the remote object produced a digest equal to the locally computed digest. Independent means a fresh HTTP request that does not reuse the upload's response, and it means the digest was recomputed from the returned bytes rather than taken from a header.
3. The manifest entry referencing it has been committed and its signature verified by reading it back.
4. The state ledger rows for that segment carry the remote repository, path and digest.

If any of the four is false, the file stays. There is no disk pressure override, no operator flag, no `--force`, and no timeout that eventually gives up and deletes. Doc 15's backpressure exists precisely so that this rule never has to be broken: when the disk fills, the crawl slows down, and that is the correct outcome.

Verification in step 2 is sampled by default and full periodically. The default is to fetch three random 1 MiB ranges and check them against doc 10's chunk tree for the corresponding offsets, which costs about 3 seconds, plus one full re download and full digest check every 100 segments, which costs about 15 seconds amortised to 0.15 seconds per segment. Full verification on every segment would cost 128 MB of inbound per segment, or 1.5 MB/s per host, which is real bandwidth on boxes whose inbound may be metered. The sampled check plus the periodic full check is a defensible compromise and the sampling rate is a configuration value.

After deletion, the crawl retains nothing locally except state. If disk allows, the publisher keeps the most recently published segments as an opportunistic read cache for debugging, capped at 5 GB and deletable at any moment by anything that wants the space.

## 12.8 Reconciliation and correction

Once a day, and on every coordinator start, the publisher reconciles. It lists every file in every repository it owns for the current and previous week, compares against the manifests, and against the segment records in state. Three things can be wrong and each has one answer.

**A file exists remotely but is in no manifest.** An orphan from a crash between upload and manifest push. If a matching segment record exists in state, add it to the next manifest. If not, delete it.

**A manifest references a file that does not exist.** This should be impossible given the commit ordering, and if it happens it is a bug worth stopping for. The publisher refuses to advance the chain, alerts, and does not attempt to repair automatically.

**A segment record in state has no remote copy and no local file.** Data loss. The URLs in it are marked for refetch by resetting their ledger rows to due, which is cheap and correct, since the crawl is continuous and refetching 21000 URLs is 85 seconds of work.

Correction of published data is a different problem and it has one mechanism. We never rewrite a published Parquet file and we never delete one. Instead, `umi-meta` carries an exclusion list: repository, file, and either a row predicate or a set of `url_key` values, with a reason and a date. Doc 07.7's block command writes to it and doc 06's ban list feeds it. A consumer honours the exclusion list by applying it as a filter, and every published dataset card says so in the first paragraph.

This is the honest design for an open corpus. Rewriting history breaks every checksum anyone recorded, breaks the manifest chain, and makes reproducibility impossible. An append only exclusion list keeps the chain intact, keeps old work reproducible, and still lets a takedown request be satisfied within the hour. The cost is that a consumer who ignores the exclusion list gets content we have asked them not to use, and that cost is unavoidable in any published dataset, which is why the mechanism has to exist before the first complaint rather than after.

## 12.9 Licensing and the dataset card

The generated dataset card on every repository states, in this order: what umi is, what the schema is, which spec version and which extractor version produced it, the canonicalisation version, the exclusion list location and the obligation to apply it, the crawl purpose declaration from doc 07.3, the robots and `Content-Usage` columns and what they mean, and contact details for takedown.

Licensing splits, and the split is stated plainly rather than blurred. Everything umi creates is CC0: the annotations, the quality signals, the duplicate clusters, the link structure, the receipts, the robots corpus, the manifests, and the schemas. The extracted page content is third party material that we did not create and cannot license, published on the same basis as Common Crawl has published for over a decade, with per row provenance, the publisher's stated preferences carried alongside it, and a takedown process that works. Saying it that way is more honest than putting a licence tag on the repository and hoping nobody looks closely.

## 12.10 What we do not do

We do not publish raw HTML. Doc 10.2 explains the storage arithmetic and doc 07.8 explains the 24 hour retention limit. The consequence, that we can never re extract from history, is accepted and recorded in doc 17.

We do not publish anything that was not verified by doc 06 to at least the `local` level. Unverified deliveries are quarantined and either promoted or discarded, and a row marked `unverified` in the published schema means "verified as arriving from a known fetcher but not corroborated", not "not checked at all".

We do not maintain a query API, a search endpoint, or a hosted index. The corpus is files. Spec 2050 covers what gets built on top and this project ends at the Parquet.

We do not delete published repositories. A week's repository stays forever, including weeks where the crawl was broken and the data is bad, with the badness recorded in `umi-meta` rather than erased. A corpus you can quietly withdraw is a corpus nobody can build on.
