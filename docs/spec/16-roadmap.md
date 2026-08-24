# 16 Roadmap

## 16.1 How this is meant to be used

Six milestones. Each one has a list of what gets built and a gate that is a number or a yes/no, checkable by running a command, with no room for "close enough".

The rule from doc 00 applies throughout: **if a gate cannot be met on the real hardware, change the spec, not the gate.** A gate that gets relaxed because it was inconvenient is a gate that was never a gate. Several of these numbers are load bearing assumptions from doc 01 that have never been measured, and the entire point of putting them here is that we find out early enough for the answer to change the design rather than late enough for it to sink the project.

The ordering is not arbitrary. Each milestone produces something usable on its own, and each one measures the assumption that the next one depends on. Milestone 1 is useful to a single user on a laptop. Milestone 6 is a public web index. Nothing in between is a phase that produces only internal plumbing.

Durations assume one person, not full time. They are estimates and they are the least reliable numbers in this spec.

## 16.2 Milestone 1: one host, end to end

**Goal.** `umi crawl example.com` on one box produces verified Parquet in a public Hugging Face repository, using the real code paths for everything.

**Built.** `umi-types` with doc 11.2 canonicalisation. `umi-extract` with the doc 11.3 markdown pipeline, links, metadata and the golden corpus test. `umi-robots` with RFC 9309. `umi-fetch` at T1 only, rustls, no emulation, no browser. `umi-state-sqlite` implementing the full doc 08 trait. `umi-frontier` with per host politeness and simple priority, no change rate model yet. `umi-file` with the doc 10 writer, reader, commit records and recovery. `umi-publish` with the doc 12 Parquet conversion, upload, manifest, signature and GC rule. `umi-cli` with `crawl`, `resume`, `get`, `ls`, `cat`, `doctor`, `publish`.

**Gates.**

1. **Bandwidth, measured on all three boxes.** `umi doctor` reports sustained inbound and outbound for at least 60 seconds on server1, server2 and server3, and the results are written into doc 01. This is the first task of the first milestone because doc 01's entire capacity plan rests on inbound not being metered at 32 TB per month. If it is, the sustainable rate is 81 pages per second per host, not 250, and doc 01 changes before anything else is built.
2. **Determinism.** The golden corpus of 10000 documents produces byte identical extraction output across three separate machines and two separate builds. Doc 11.1 is either true or the fetch protocol does not work.
3. **Crash safety.** A 100 iteration test that SIGKILLs the writer at a uniformly random byte offset during a crawl. Every run must recover with at most one shoal lost, no corrupt segment accepted by the reader, and no torn tail surviving.
4. **The GC rule holds.** A test that fakes an upload success with a corrupted remote copy must leave the local file on disk and exit 6.
5. **A real published dataset.** One `open-index/umi-focus-*` repository, with a signed manifest, that `umi verify` passes against from a clean machine.

**If gate 1 fails.** Rewrite doc 01's arithmetic at the measured rate, raise the community fleet target in doc 04 proportionally, and continue. This is a spec change, not a stop.

**Estimate.** 6 to 8 weeks. This is the largest milestone because it is the one that builds the spine.

## 16.3 Milestone 2: the ladder and the clock

**Goal.** The crawler handles the 2026 web rather than the 2010 web, and it knows when to come back.

**Built.** Doc 05's full tier ladder: T0 conditional revalidation, T2 through `wreq` behind the `emulation` feature, T3 through `chromiumoxide` with the subresource policy and the 8 tab cap on server2, T4 gated behind the published per domain allowlist. Per host tier learning and the escalation and de-escalation state machine. Doc 07 in full: Web Bot Auth signing, the key directory, forward confirmable rDNS, the adaptive rate limiter, AIPREF `Content-Usage` parsing, `umi block`, and the bot page at `umi.dev/bot`. Doc 09's change rate estimator, refresh classes, and `umi watch`. Sitemap, feed and `robots.txt` seeding.

**Gates.**

1. **Tier share, measured.** Crawl a stratified sample of 100000 URLs drawn from Common Crawl across the domain rank distribution, and report the fraction of pages that succeed at each tier. Doc 05 assumes roughly 95 percent at T0/T1, 4 percent at T2 and under 1 percent at T3. If T3 is needed for 5 percent, doc 01's capacity plan is wrong and the browser pool becomes the bottleneck for the whole project.
2. **No `openssl-sys` in the default tree.** `cargo tree -e normal` on the default feature set contains no `openssl-sys`, asserted in CI. With `--features emulation`, BoringSSL symbols are prefixed and the resulting binary passes the crash test suite. Doc 05.5 explains why this is a gate and not a footnote.
3. **Politeness under adversarial conditions.** Against a local test origin that returns 429 with `Retry-After`, 503, slow responses and connection resets, the crawler never exceeds the configured rate, never ignores `Retry-After`, and backs off correctly in every case. Verified from the server's logs, not the crawler's.
4. **Robots correctness.** The full Google robots.txt conformance corpus passes, plus our own cases for 5xx, oversized files, redirects and `Crawl-delay` clamping.
5. **Freshness beats Common Crawl on one domain.** Run `umi watch` on a news domain for 14 days. Median staleness at detection must be under 6 hours, against Common Crawl's roughly 30 days for the same URLs. This is doc 00's third headline claim, demonstrated on a small scale before it is claimed at large scale.

**Estimate.** 5 to 7 weeks.

## 16.4 Milestone 3: rate on one box

**Goal.** server3 alone sustains 250 pages per second for 24 hours with a frontier too large for memory.

**Built.** `umi-nami`: the packed fingerprint seen set, the ribbon filter, DRUM style batched merges, the columnar ledger, the per PLD heaps, the redo log and the crash discipline from doc 8.5. Doc 8.6's cold state on object storage with warm, work, evict and forget. Doc 15's backpressure ladder, all three of it. The Prometheus endpoint and the DuckDB checkpoint. Doc 11's within segment and within PLD dedup.

**Gates.**

1. **250 pages per second on server3, sustained over 24 hours**, with a frontier of at least 500 million known URLs, of which no more than 100 million are resident at any moment. Sustained means the 24 hour mean, with no hour below 200.
2. **State compactness.** Under 20 bytes per known URL on disk, including the seen set, the ledger and the filters, measured on the 500 million URL frontier. Doc 08 states this as a measurement rather than a promise, and this is the measurement.
3. **Admission throughput.** 12500 candidates per second sustained through `admit`, which is doc 03.3's number derived from 750 pages per second at 50 links per page. If admission cannot hold that, nothing downstream matters.
4. **Storage compactness.** Mean stored bytes per page under 7 KB across at least 10 million real pages. Doc 10.2 budgets 6 KB and docs 01, 04 and 09 spend it. Over 7 KB and doc 01's disk arithmetic needs redoing.
5. **Shard miss rate under 2 percent** of admitted candidates during steady state crawling. Above that, doc 08's cold tier is thrashing and the crawl is bounded by object storage rather than by the network.
6. **Backpressure works.** Fill the disk artificially and confirm the ladder climbs, the crawl slows, nothing unpublished is deleted, and the ladder descends with hysteresis when space returns.

**If gate 1 fails.** The honest fallback is to drop the per host target and raise the fleet size needed, which pushes weight onto milestone 5. The fallback that is not available is relaxing gate 2, 4 or 6, because those are what keep the boxes alive.

**Estimate.** 8 to 10 weeks. nami is the largest single piece of engineering in the spec.

## 16.5 Milestone 4: three coordinators and the protocol

**Goal.** The fleet runs as a fleet, and untrusted fetchers work over the wire with verification that has been calibrated against real disagreement.

**Built.** Doc 03.3's rendezvous partitioning, peer channels and cross partition candidate batching. `umi-proto` with all five endpoints, leases, receipts, the stability digest and flow control. `umi-verify` with all seven layers from doc 06, the reputation curve, canaries, the holding pen and the quarantine. `umi fetch` as a working standalone fetcher. Doc 15's operational deployment on all three boxes with systemd, sysctls, local DNS and drain.

**Gates.**

1. **750 pages per second fleet wide, sustained over 7 days.** Doc 00's headline number, held for a week rather than a burst, with the publish loop running continuously and the disk backpressure ladder never exceeding level 2.
2. **Honest disagreement, measured.** Fetch the same 100000 URLs from two of our own fetchers on different boxes at controlled time offsets, and report the distribution of stability digest agreement. This produces the two numbers doc 06.7 requires: the rate at which honest, independent fetches of the same URL disagree, and how that rate varies with the delay between them. Doc 04's Jaccard thresholds of 0.90 for text and 0.95 for links are guesses until this runs, and if the measured honest disagreement rate at a 60 second offset exceeds 5 percent, the thresholds move or the sketch goes to 128 permutations.
3. **Verification overhead under 20 percent** of coordinator capacity, per doc 06.6, measured with a simulated fleet of 50 new fetchers all starting at zero reputation, which is the worst case because new fetchers are replayed at 100 percent.
4. **A third party fetcher.** Someone outside the project writes a working fetcher from doc 04 alone, in a language that is not Rust, without asking us a question that is not answered in the document. This is the only gate in the spec that is not a number, and it is the one that decides whether milestone 5 is possible.
5. **Poisoning resistance, demonstrated.** Run a deliberately malicious fetcher that fabricates content, replays receipts, injects frontier spam and fails audits. Every attack in doc 06.1's threat model must be caught, and the transcript of what caught each one gets published.

**Estimate.** 8 to 10 weeks.

## 16.6 Milestone 5: open

**Goal.** Anyone can contribute fetch capacity, and the corpus is published continuously at a scale that requires a real conversation with Hugging Face.

**Built.** The public coordinator at `umi.dev`, the bot page, the published key directories, the fetcher key directory, the ban list and the block list. Static binaries for the common platforms. The volunteer documentation, which is doc 04 rewritten for someone who has never read this spec. Doc 12 at full rate with weekly repository allocation. Doc 11's global exact dedup batch job and the LSH near duplicate clustering, publishing `umi-dedup`. The PDF handler, off by default until here for the reasons in doc 11.3.

**Gates.**

1. **Onboarding works in one command.** A volunteer with a fresh machine runs one command from the bot page and is contributing verified pages within 5 minutes, with no account, no token and no configuration. Measured with at least 10 real volunteers who are not us.
2. **The fleet adds real capacity.** Community fetchers contribute at least 500 pages per second in aggregate, sustained over 7 days, with the coordinator's verification overhead still under 20 percent and no measurable increase in the corpus disagreement rate.
3. **Hugging Face has agreed.** At 9 new repositories a week this stops being a technical question. Doc 12.6 makes this a deliverable: the repository count, the growth rate and the total size are put in front of Hugging Face before we are creating hundreds of repositories, not after. The gate is a written answer, whatever the answer is. If it is no, the fallback is our own object storage with a Hugging Face mirror of a curated subset, and doc 12 changes accordingly.
4. **The corpus is verifiable by a stranger.** Someone outside the project downloads a week of published data, verifies the manifest chain, checks the signatures, re verifies a sample of receipts against the published fetcher keys, and confirms that the exclusion list applies cleanly. The whole "you do not have to trust us" claim is worth nothing until someone who does not trust us has checked it.
5. **10 billion pages published.** A tenth of the target, and roughly 5 times a Common Crawl monthly snapshot, which is the point at which doc 00's size claim stops being a projection.

**Estimate.** 10 to 14 weeks, and this is the estimate least likely to survive contact with reality, because most of it is other people's time.

## 16.7 Milestone 6: the long run

**Goal.** Everything after here is operation rather than construction, and the shape of it is set by two curves.

**The size curve.** Doc 01's arithmetic: our own hardware reaches 100 billion pages in 4.2 years, and only the community fleet changes that. Doc 09 gives discovery 35 percent of the budget, which alone is 12 years, so the discovery share is raised during the land grab and lowered once coverage is respectable. The decision about when to lower it is a judgement call, made against measured coverage of the top 10 million domains rather than against a schedule.

**The freshness curve.** Doc 09.5 shows a fresh core of roughly 631 million URLs at 15 day staleness on our own capacity. That core grows linearly with fleet capacity, and the interesting question is which URLs belong in it. That is a ranking problem and it is where Spec 2050 takes over.

The standing work at this stage is: keeping the tier ladder current as anti bot systems change, which is continuous and never finishes; keeping the fetcher fleet healthy and the reputation curves calibrated; keeping the publish loop ahead of the disk; and responding to site operators within the commitment in doc 07.7.

**Gates**, checked quarterly rather than once:

1. Coverage of the top 1 million domains above 90 percent, and of the top 10 million above 60 percent.
2. Median staleness of the realtime and hourly refresh classes under 6 hours.
3. Corpus growth above 5 billion pages per quarter.
4. Verification disagreement ratio flat or falling.
5. Zero unresolved takedown requests older than one business day.

## 16.8 What could stop this, in order

Doc 17 covers risks properly. The roadmap ordering exists to surface the fatal ones early, and it is worth stating which gates are the real decision points.

**Gate 1.1, bandwidth.** If inbound is metered tightly on all three boxes, the per host rate falls by a factor of three and the whole project depends on the community fleet from day one rather than from milestone 5. Known in week one.

**Gate 2.1, tier share.** If a large fraction of the useful web now requires a browser, the cost model changes by an order of magnitude and the honest response is to narrow the scope to what we can afford. Known by month four.

**Gate 3.1 and 3.2, rate and compactness.** If nami cannot hold the frontier in 20 bytes per URL, the fleet's memory and disk cannot hold a 100 billion URL frontier at all and the target drops. Known by month seven.

**Gate 4.4, a third party fetcher.** If doc 04 is not implementable by someone else from the document alone, there is no community fleet, and without a community fleet the size target is 4.2 years at best. Known by month ten.

**Gate 5.3, Hugging Face.** If the answer is no and there is no affordable alternative, the corpus needs a different home, and the openness claim is the thing at risk rather than the crawling. Known by month fourteen.

Each of those is checkable months before the work that depends on it is finished, which is the entire reason the milestones are ordered this way.
