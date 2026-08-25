# 03 Architecture

## 3.1 The five jobs

umi does five things and every component is one of them.

**Schedule.** Decide which URL to fetch next, subject to politeness, priority and freshness. This is the coordinator, and it owns state.

**Fetch.** Turn a URL into bytes, escalating through tiers when the cheap tier fails. This is the fetcher, and it is a protocol so that it can run somewhere else.

**Extract.** Turn bytes into a row: markdown, text, links, snippets, metadata, digests. This is CPU work and it runs wherever the bytes are, which is usually the fetcher.

**Store.** Append rows to a local `.umi` file and update state. This is the writer, and it runs on the coordinator.

**Publish.** Convert sealed `.umi` segments to Parquet, push to Hugging Face, verify, and delete the local copy. This is the publisher, and it is the reason the disk does not fill up.

## 3.2 Process model

Two binaries, one crate workspace.

`umid` is the coordinator daemon. It owns the state store, serves the fetch protocol, runs the writer and the publisher, and hosts the local fetcher pool. On the current fleet exactly one `umid` is the frontier owner for a given pay level domain range, and the three of them peer with each other.

`umi` is the command line, which covers both operator commands against a local or remote `umid` and the standalone fetcher mode. A volunteer runs `umi fetch --coordinator https://umi.dev` and nothing else. That has to be true or doc 04 does not work.

Inside `umid` the pipeline decouples by cost profile, because fetch is I/O bound and extract is CPU bound and they must scale independently.

```
                        ┌──────────────── state (doc 08) ────────────────┐
                        │  seen set · ledger · frontier · host records   │
                        └───▲────────────────────────────────────▲───────┘
                            │ admit(candidates)                  │ complete(outcomes)
                            │                                    │
  seeds ──► admission ──────┘                                    └────── outcome sink
  (doc 13)  · canonicalise                                                    ▲
            · robots gate                                                     │
            · dedup vs seen                                                   │
            · holding pen                                                     │
                                                                              │
            scheduler ──► lease pool ──┬──► local fetch pool ──► extract ──────┤
            (doc 09)     (doc 04)      │    (tiers, doc 05)     (doc 11)       │
                                       │                                       │
                                       └──► umi/1 wire ──► community fetchers ─┤
                                              (doc 04)      (extract at edge)  │
                                                    ▲                          │
                                                    └── verifier (doc 06) ─────┘
                                                            │
                                                            ▼
                                              writer ──► segment.umi (doc 10)
                                                            │  seal
                                                            ▼
                                              publisher ──► Parquet ──► HF (doc 12)
                                                            │  verify
                                                            ▼
                                                           GC (delete local)
```

Two rules keep this honest. Links discovered by an unverified fetcher enter the holding pen, not the frontier, and only graduate once corroborated. And nothing is deleted by GC until the publisher has confirmed the remote copy by digest.

## 3.3 Partitioning across the fleet

Each pay level domain is owned by exactly one coordinator, chosen by rendezvous hashing over the PLD id. All of a site's URLs, its politeness timer, its robots cache and its tier policy live on one node, so no cross node coordination is needed to be polite. Rendezvous hashing rather than a ring because it is simpler to reason about with three nodes and it limits reassignment when one goes away.

Cross partition links are the common case. A page on server1's partition linking to a PLD owned by server3 produces a candidate that is batched and shipped to server3 over the peer channel, in batches of up to 4096 candidates or 200 ms, whichever comes first. Batching matters: at 750 pages/s fleet wide and roughly 50 links per page, admission sees about 37500 candidates per second, two thirds of which cross a partition boundary and almost all of which are already seen. That number, not the page rate, is what the seen set has to survive.

Community fetchers do not partition. They connect to whichever coordinator the load balancer gives them and lease work from that node's partition. A fetcher is stateless with respect to the frontier.

When a coordinator is down its PLDs are unowned and simply are not crawled until it returns. There is no failover and no replication of state. This is a deliberate simplification: state is reconstructable from the published corpus plus the cold state shards, and a few hours of a partition being idle costs nothing at these rates. Doc 15 covers recovery.

## 3.4 What runs where, given the hardware

The three boxes are not interchangeable, so pin roles rather than pretending they are.

**server3** (8 vCPU, 23 GB, 112 GB free) is the primary coordinator. It holds the hot state shards, runs the publisher, and hosts the DuckDB analytics attach point from doc 15. It gets the largest PLD share.

**server2** (6 vCPU, 11 GB, 67 GB free) is the second coordinator and the tier 3 host. Its browser pool is capped at 8 tabs, which is about 2.4 GB of RSS, and it is the only box that runs Chromium.

**server1** (4 vCPU, 6 GB, ~0 free, SSD) is the third coordinator and gets the smallest PLD share. It has the only SSD in the fleet, so it is where write heavy state shards go, but it has no memory headroom at all. Cap `umid` there at 1.5 GB RSS and mean it. If the box cannot hold that, run it as a fetcher only node and give its PLDs to server3.

## 3.5 Crate layout

One Cargo workspace, `tamnd/umi`. Names lean on the sea where the name is genuinely useful and stay plain where it is not.

```
umi/
  crates/
    umi-types/         URLs, canonicalisation, PLD ids, digests, the row schema
    umi-state/         the State trait and shard model (doc 08)
    umi-state-sqlite/  default backend
    umi-nami/          the experimental single file state engine (波, wave)
    umi-state-pg/      Postgres backend
    umi-state-duck/    DuckDB backend, read mostly
    umi-frontier/      scheduling, politeness, change rate model (doc 09)
    umi-robots/        RFC 9309 parser, AIPREF Content-Usage, cache
    umi-fetch/         tier ladder, clients, browser pool (doc 05)
    umi-proto/         the umi/1 wire types, leases, receipts (doc 04)
    umi-verify/        receipt verification, sampling, reputation (doc 06)
    umi-extract/       HTML to markdown, text, links, metadata (doc 11)
    umi-dedup/         content hashing, MinHash, LSH banding
    umi-file/          the .umi container, reader and writer (doc 10)
    umi-publish/       Parquet conversion, HF client, manifests, GC (doc 12)
    umi-seed/          Common Crawl and sitemap seeding (doc 13)
    umi-crawl/         the loop: lease, fetch, extract, sketch, row
    umid/              the coordinator daemon
    umi-cli/           the umi binary (doc 14)
```

`umi-crawl` was not in the first version of this list and its absence was a mistake worth explaining. The loop that turns a lease into a row is the one piece that touches the frontier, the fetcher, robots, extraction, dedup and the writer at once, and there are two programs that need it: `umid` runs it as a service and `umi crawl` runs it once from a terminal. Leaving it in either one would have made the other depend on a binary, and putting it in `umi-file` would have made the file format depend on an HTTP client. So it is a crate, it owns the doc 10.5 row builder, and it is the only place where the order of the pipeline is written down.

`umi-types` is the only crate everything depends on and it must stay free of I/O and free of async. If a URL canonicalisation change requires touching six crates, the boundary is wrong.

The state backends are separate crates behind cargo features so that a fetcher only build does not link SQLite, Postgres and DuckDB. Binary size matters for the volunteer download.

`umi-fetch` has a hard dependency problem worth calling out now. Tier 2 needs BoringSSL through the wreq family of clients, and BoringSSL shares symbol prefixes with openssl-sys, which causes link failures or, worse, segfaults when both end up in the graph. Tier 1 therefore uses rustls and tier 2 is behind the `emulation` feature, off by default, and CI has a job that asserts the default build has no openssl-sys in the dependency tree.

## 3.6 Data flow for one page

Walking one URL end to end, because the diagram hides the interesting parts.

A candidate URL arrives from link extraction, a sitemap, or a seed. Admission canonicalises it (doc 11.2), computes the 80 bit fingerprint, and checks the seen set for its PLD shard. If the shard is cold it is fetched from object storage first, which is the slow path and is why admission works in batches of thousands rather than one at a time. If unseen, admission checks robots for the host, which may itself require a fetch, and inserts a ledger row with a due time.

The scheduler picks it up when its host's politeness timer allows and its priority wins. It becomes a lease: a signed token carrying the URL, a tier hint derived from the host's learned policy, a deadline, a nonce, and the conditional headers to send if we have an ETag from last time.

A fetcher, local or community, takes the lease. It runs the tier ladder from doc 05, starting at the hint. It gets bytes, or a 304, or a block signal, or a failure. It extracts locally, computes the raw body digest and the chunk tree, builds a receipt, signs it, and delivers.

The verifier checks the receipt: self consistency first, which is free, then the sampled and quorum checks from doc 06 which are not. The outcome goes to state, which records the status, the new ETag, the content hash, the tier that worked, and the next due time computed by the change rate model. The row goes to the writer, which appends it to the open `.umi` segment. The extracted links go to admission, or to the holding pen if the fetcher is not yet trusted.

When the segment hits its seal threshold the writer closes it, the publisher converts it to Parquet, uploads to Hugging Face, verifies the remote digest, and GC deletes the local file. Total elapsed time from fetch to published is the segment fill time plus the publish time, which doc 12 budgets at under 60 minutes at p50.

## 3.7 Failure behaviour

The rules, in priority order.

**Never lose the frontier.** State writes are durable before a lease is issued. A crash re issues in flight leases after their deadline, which costs at most a duplicate fetch.

**Never publish unverified data.** A receipt that fails verification quarantines the row and the fetcher. Quarantined rows are not published and are kept for manual review for 7 days.

**Never delete unverified.** GC requires a confirmed remote digest. If Hugging Face is unreachable, segments accumulate and the crawl throttles itself rather than dropping data. Doc 15 defines the backpressure ladder, and on 342 GB of free disk it triggers early.

**Prefer a stalled partition to a rude crawl.** If robots cannot be fetched for a host, that host is not crawled. If a host's error rate crosses the threshold, back off exponentially and record it. There is no configuration that turns this off.
