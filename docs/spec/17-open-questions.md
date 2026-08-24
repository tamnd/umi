# 17 Open questions

## 17.1 Why this document exists

Every specification of this size contains claims that are measured, claims that are reasoned, and claims that are guesses wearing the same font as the other two. This document separates them.

Nothing here is a hedge. The design in docs 01 through 16 is what should be built, and it should be built as written. What follows is the list of places where the design rests on something we have not checked, along with what we would do if the check came back badly. A spec that cannot say which of its numbers it made up is a spec nobody can act on safely.

## 17.2 Things we have not measured

These are doc 16's gates, restated as what they actually are: assumptions.

**Is inbound bandwidth metered?** Doc 01's entire capacity plan assumes it is not, or is metered generously. Crawling is inbound heavy at roughly 25 to 1 against outbound, and VPS allowances are usually written for the opposite shape. If all three boxes are metered at 32 TB per month, the sustainable rate is 81 pages per second per host rather than 250, everything in doc 01 changes by a factor of three, and the community fleet becomes essential from day one instead of from milestone 5. Milestone 1, week one.

**How much of the useful web needs a browser?** Doc 05 assumes under 1 percent of pages require T3 rendering and about 4 percent require T2 emulation. Those numbers come from reading, not from measuring, and the anti bot landscape has moved fast enough that reading from 2025 is already stale. If T3 is 5 percent, the browser pool becomes the bottleneck for the entire project and the cost per page roughly triples. Milestone 2.

**Can nami hold a frontier in 20 bytes per URL?** Doc 08 states it as a measurement rather than a promise, and the reasoning behind it, that sorted 80 bit fingerprints in a dense key space delta encode to 4 to 6 bytes, is sound but untested at scale. If it comes in at 40 bytes, a 100 billion URL frontier is 4 TB rather than 2 TB, cold storage costs double, and the resident working set gets smaller, which raises the shard miss rate, which is the thing that actually bounds throughput. Milestone 3.

**How often do honest fetchers disagree?** Doc 04's Jaccard thresholds of 0.90 for text and 0.95 for links are guesses, and doc 06 builds a seven layer verification scheme on top of them. If independent honest fetches of the same URL 60 seconds apart disagree more than 5 percent of the time, we will be banning people who did nothing wrong, and the fix is either looser thresholds, which weakens verification, or a 128 permutation sketch, which costs storage. Milestone 4, and it must happen before the fleet opens.

**Is 6 KB per page right?** Doc 10.2 derives a median of 3.8 KB and a mean of 5.9 KB from a plausible content distribution, and docs 01, 04, 09, 12 and 15 all spend that number. It is the single most repeated figure in the spec and it comes from arithmetic over an assumed distribution rather than from a corpus. Milestone 3.

**Will anyone actually run a fetcher?** This is the largest unknown in the project and it is not a technical one. Doc 04 exists because our own hardware reaches 100 billion pages in 4.2 years and nothing else closes the gap. The precedents are mixed and mostly discouraging: BOINC and Folding@home built real volunteer fleets around a cause people cared about, YaCy built a peer to peer search engine that nobody used, and Common Crawl has never asked for volunteer fetch capacity at all. We are asking people to donate bandwidth and an IP reputation to a crawler, which is a bigger ask than donating idle CPU. Milestone 4's gate 4 and milestone 5's gate 2 are where we find out, and if the answer is no, the honest response is to accept 4.2 years and say so publicly rather than to keep projecting a fleet that never materialises.

## 17.3 Decisions made without evidence

Places where we picked something defensible and moved on, with what would change our minds.

**80 bit URL keys.** Doc 08's birthday arithmetic gives 0.004 expected collisions at 100 billion URLs, which is fine. What that arithmetic does not cover is that a collision is silent: two distinct URLs map to one key and one of them is never crawled. We would find out only by accident. The mitigation is that `LedgerRow` carries a 128 bit `url_key_full` so a collision is detectable on any row we have actually fetched, but the seen set itself is 80 bits and a collision there is invisible. If milestone 3 shows any full key mismatch at all, the key goes to 96 bits at a cost of 2 bytes per URL.

**Not sorting query parameters.** Doc 11.2 step 11 refuses to sort or deduplicate query parameters, on the grounds that order and repetition are semantically significant often enough that normalising them breaks real sites. The cost is duplicate crawling of URLs that differ only in parameter order. Nobody has published a good measurement of how often either failure occurs. A study on our own corpus after milestone 3 would settle it, and the answer would be a canonicalisation version bump.

**Rendezvous hashing with no failover.** Doc 03.3 accepts that a dead coordinator means its PLDs are simply not crawled. With three nodes that is a third of the crawl stopped, which at a few hours is nothing and at a week is significant. We chose this over replication because replicating mutable state across three boxes with 14 GB of combined free memory is a project of its own. If coordinator outages turn out to be frequent, the cheap fix is not replication but faster reassignment: a peer that has been unreachable for an hour has its PLDs redistributed automatically, accepting the refetch cost.

**The 5 percent realtime cap.** Doc 09.6 caps the realtime path at 5 percent of budget to stop feed driven work from starving everything else. That number was chosen because it sounded right. The correct number depends on how much of the web's genuinely valuable change is announced through feeds and sitemaps, which is measurable and which we have not measured.

**Reputation curve constants.** Doc 06's `rep += (1 - rep) * 0.02` on pass and `rep -= rep * 0.50` on a hard failure produce a curve that feels correct: slow to earn, fast to lose, never quite reaching 1.0. They are not derived from anything. They should be re fitted once there is a real distribution of fetcher behaviour, and until then the risk is that an honest fetcher with an intermittent network takes 50 percent hits it does not deserve.

**Sixteen kept headers.** Doc 11.5's list is a judgement call and two entries, `alt-svc` and `server`, are justified by research value rather than crawler need. If they turn out to cost more than they are worth they come off, and removing a column from a published schema is a schema version bump, which is exactly the kind of small annoyance that argues for being conservative now.

## 17.4 A subtlety that is resolved but worth writing down

Doc 11.1 requires byte identical extraction output across machines, and doc 10.6 compresses the `markdown` column with zstd. zstd output is not stable across zstd versions, or across builds with different window settings, so it would be reasonable to worry that determinism is impossible.

It is not a problem, because nothing is ever digested over compressed bytes. Doc 04's `extract.digest` is blake3 over the canonical CBOR of the logical extraction values, before any compression. Doc 12's file digests are over the Parquet bytes, which are compared against themselves rather than across machines. The compressed representation is free to differ between two fetchers and nothing in the verification scheme notices.

The same argument covers FSST symbol tables, dictionary orderings, and bit packing widths. All of them are representation, none of them are content. The rule to hold onto during implementation is: **digest logical values, never encoded bytes, except when checking that a specific file arrived intact.**

## 17.5 Deliberately deferred

Not rejected, just not now. Each of these has a natural place to land later.

**Topic focused crawling.** Doc 13.9 notes that a scope cannot express "pages about X". Doc 09's priority function already has the term for a classifier driven bonus. This is the natural extension once there is a corpus to train a classifier on, which means after milestone 5, not before.

**Microdata and RDFa.** Doc 11.6 detects and flags them and parses neither. JSON-LD covers most of the structured data that matters in 2026 and the other two are a long tail of parsing surface for a small return.

**Non HTML beyond PDF.** Office documents, ePub, and the rest. PDF is behind a feature flag until milestone 5 because PDF parsers are the largest untrusted parsing surface anyone can hand a crawler, and the others do not justify the surface at all yet.

**Geographically diverse fetching.** Some sites serve different content, or nothing at all, depending on the client's country. Doc 04's `Hello` already carries a self reported country and ASN, and doc 06 compares it against what the coordinator observes, so the mechanism to route work by egress geography exists. Nothing uses it yet. This is one of the genuine advantages a community fleet has over any centrally hosted crawler, and it is worth building once there is a fleet to build it on.

**Parquet compaction.** Doc 12 publishes 128 MB files and never merges them. For most query engines that is a good size, but a consumer doing full corpus scans would prefer 1 GB files. Compaction is straightforward and it is not milestone 1.

**Shell completions, a TUI, a web UI.** All useful, none load bearing.

**State replication and coordinator failover.** Covered in 17.3.

## 17.6 Explicitly rejected

Different from deferred. These will not be built, and if someone proposes them the answer is a pointer to this section.

**Peer to peer anything, and a token.** Doc 01's non goals say it and doc 02 explains why: every fully decentralised web search project has traded away the latency and spam resistance that make search usable, and a token turns a public good into a speculative asset and changes who shows up. The fetch protocol is federated, which is the part of decentralisation that actually works, and the authoritative state stays with coordinators.

**Ignoring robots.txt, ever, for any reason.** Doc 07.8 and doc 14.10. The flag does not exist.

**CAPTCHA solving, residential proxies, IP rotation.** Same sections. A crawler that hides where it comes from cannot claim legitimacy, and legitimacy is the whole strategy.

**Crawling behind authentication.** Including free accounts. If it needs a login it is not the open web.

**Arbitrary pluggable state backends.** Doc 08's trait is narrow on purpose and there are four implementations, not a plugin system. A fifth backend is a pull request against the trait, not an extension point.

**A query API or hosted index.** This project ends at the Parquet. Spec 2050 covers what gets built on top.

## 17.7 Accepted losses

Things the design gives up permanently, written down so that nobody rediscovers them as bugs.

**We can never re extract from history.** Doc 10.2 and doc 07.8: raw HTML is retained for 24 hours and then gone. When the extractor improves, the improvement applies to pages crawled after the change and not to the corpus. The alternative was storing 150 KB per page instead of 6 KB, which is 25 times the storage and would end the project. The partial mitigation is that the crawl is continuous, so a page that matters gets refetched and re extracted within its refresh interval anyway. The pages that never get refetched are the ones nobody was reading.

**Fragments are lost.** Doc 11.2 step 6 removes them, including hash bang routing fragments. A small number of single page applications become one URL instead of many.

**Change history is lost if state is lost.** Doc 15.8: the seen set and an approximate ledger can be rebuilt from the published corpus, but per URL change rate history cannot, and the estimator restarts from its prior. That is a day of degraded scheduling, not data loss.

**The link graph is not published as a graph.** Doc 12.4: five trillion edges is larger than the corpus that produced them. The edges are in the `links` column and consumers derive what they need.

**Published data is never rewritten.** Doc 12.8: corrections happen through an append only exclusion list, so a consumer who ignores the list gets content we have asked them not to use. Every alternative breaks the manifest chain and makes reproducibility impossible, so this is the least bad option rather than a good one.

## 17.8 Risks that could kill the project

In rough order of how likely they are to be the thing that actually does it.

**The web closes faster than we can establish legitimacy.** This is the top risk and most of it is outside our control. Cloudflare fronts roughly a fifth of the web, moved to blocking AI crawlers by default in July 2025, and shipped pay per crawl. Common Crawl's own snapshots have declined by about 29 percent. The whole strategy in doc 07 is to be the kind of crawler that stays allowed: one purpose, honestly declared, signed with Web Bot Auth, robots respecting at every tier, published IP ranges, a contact address that a human reads. That strategy might simply not be enough, because the default is moving toward blocking everything that is not Google, and being a well behaved small crawler is not obviously a category that survives. If a growing fraction of the useful web becomes unreachable, the honest response is to publish exactly what we could and could not reach, which is itself a finding worth having.

**Legal exposure from publishing extracted content.** Common Crawl has done this for over a decade, which is real precedent and not a guarantee. Our position is better than a training corpus in a few specific ways: the purpose is index building, robots is honoured everywhere, `Content-Usage` preferences are propagated rather than stripped, provenance is per row, and there is a takedown path that works within a business day. It is worse in one way, which is that we publish extracted markdown rather than raw archived bytes, and the archival framing that protects the Internet Archive applies less cleanly. Jurisdictions differ, the EU text and data mining opt out regime is its own question, and none of this has been reviewed by anyone qualified. That review should happen before milestone 5, not after.

**Poisoning once we are worth poisoning.** Doc 06 takes this seriously and the seven layers are a real design rather than a gesture. But the threat model in the Carlini line of work is that poisoning a web scale corpus is cheap because a tiny fraction of documents is enough, and our defence rests on sampling rates and quorum sizes that have never faced a motivated attacker. The specific worry is not the obvious fabricator, who gets caught by the TLS chain check on the first audit, but the patient one who is honest for months and then corrupts one domain. Canaries and periodic replay are the answer and their calibration is a guess.

**Bus factor of one.** Doc 15.10 says it: the real cost is attention, and the operational commitment is one person checking a dashboard daily and being reachable. A crawler that stops being watched becomes an impolite crawler within weeks, and an impolite crawler loses the legitimacy that the whole design depends on. There is no technical mitigation for this. The partial one is that every dangerous behaviour is either impossible by construction or requires a flag that does not exist, so an unattended umi degrades into a stopped umi rather than into a bad one.

**Hardware with no redundancy.** Three VPS instances, no replication, no failover, no backups of state beyond the cold shards in object storage. Doc 15.8 makes every failure recoverable, some of them slowly, and the reason that is acceptable is that the valuable output is published continuously and is not on these boxes. The boxes are a cache, which is doc 00's first paragraph, and treating them as one from the beginning is what makes losing one survivable.

**Four years is a long time.** At our own capacity, 100 billion pages takes 4.2 years, and doc 09's discovery budget alone takes 12. Projects lose momentum. The mitigation is the roadmap ordering: milestone 1 is useful to one person on a laptop, milestone 2 produces a fresher corpus for one domain than Common Crawl produces for any domain, and milestone 5 publishes 10 billion pages, which is five times a Common Crawl snapshot. If interest runs out at any point after milestone 2, what exists is still worth having, and that is deliberate.
