# 06 Trust and verification

Opening the fetcher to the public means accepting deliveries from people we cannot identify, cannot audit, and cannot sue. This document is the design that makes that survivable. It does not make it safe, because nobody has solved authenticating data from unidentified sources in a distributed environment, and we should not claim to have.

The goal is narrower and achievable: make poisoning expensive, make it detectable after the fact, and make sure that when it happens the published corpus contains enough provenance for a consumer to identify and excise the affected rows without discarding everything.

## 6.1 Threat model

Who is attacking and what they want.

**The SEO attacker** wants their pages ranked or their competitor's pages absent. They run fetchers that inflate their own content, fabricate inbound links to their domains, or drop pages from a competitor. This is the most likely attacker and the most economically motivated, because an open index that anyone builds on is a target the day it becomes useful.

**The dataset poisoner** wants specific text in the corpus so it ends up in someone's training data. Carlini and co authors showed this is practical against web scale datasets, and the reason it is hard to defend is structural: there is no golden snapshot to diff against, no trusted curator, no bound on how much a page can legitimately change, and no principled trust ranking over domains. Our position is not better than theirs.

**The frontier attacker** does not care about content at all. They want to steer where the crawler goes, either to get their sites crawled heavily or to make us DoS a target by flooding the frontier with URLs on one host. This is the cheapest attack and the one with the worst consequences, because it turns us into the weapon.

**The freeloader** wants credit without work. They return plausible looking fabrications, or replay old content, because generating text is cheaper than fetching pages. This one is easy to catch and is included mostly because it is common.

**The lazy or broken fetcher** is not an attacker but produces the same symptoms: a stale extractor, a broken proxy, a corrupted disk, a clock that is wrong by six hours. Most quarantines will be this, and the system has to distinguish incompetence from malice or it will ban all its volunteers.

Out of scope: an attacker who controls the target website. If example.com serves different content to our fetcher than to a browser, that is example.com's decision and every crawler has the same problem. We record what we saw and move on.

## 6.2 The layers

Seven checks, ordered from free to expensive. Every delivery goes through 1 and 2. The rest are sampled or triggered.

### Layer 1: self consistency, free

Runs on every delivery, costs microseconds, catches broken clients.

The receipt signature verifies against the claimed fetcher key. The nonce matches the lease. The lease id exists, is not expired past its grace window, and was issued to this fetcher. The `url` matches the lease. If a payload is present, `blake3(payload.raw)` equals `body.digest`, the chunk tree root recomputes, `body.length` matches, and `blake3(payload.extract)` equals `extract.digest`. Timestamps are within 5 minutes of coordinator time after accounting for `duration_ms`.

Any failure here is a hard reject. It cannot happen to an honest, correct fetcher.

### Layer 2: plausibility, free

Heuristics that cost nothing and catch the obviously wrong.

Body length against `content_length`. Content type against the extracted structure, so an `text/html` claim with zero HTML elements is suspicious. The link set contains no URL that could not plausibly appear on that page, where the specific check is on the ratio of off site registrable domains to total links against the running distribution for that host. Text language against the host's historical language mix. Text length against the running distribution for that host.

Plausibility failures do not reject. They raise the sampling probability for that delivery to 1.0, which sends it to layer 3.

### Layer 3: replay, sampled

The coordinator refetches the URL itself, from its own egress, at the same tier, and compares. This is the workhorse check and it is what makes fabrication unprofitable.

Comparison uses the stability digest from doc 04.6, not raw byte equality, because live pages carry timestamps, tokens and rotating content. Two fetches agree when the normalised title digests match, the MinHash estimated Jaccard over 5-shingles of the text is at least 0.90, the text length buckets are within one, and the link set Jaccard is at least 0.95.

Sampling rate is a function of reputation and value:

```
p_replay = base(reputation) * value_multiplier(url) * suspicion_multiplier
base:  1.00 at rep 0.0, 0.20 at rep 0.3, 0.05 at rep 0.6, 0.02 at rep 0.9
value: 1.0 normal, 3.0 for high PageRank hosts, 5.0 for seeds and sitemaps
```

A brand new fetcher has every single delivery replayed. That is expensive and it is the point: a new fetcher costs the coordinator exactly as much bandwidth as doing the work itself, and contributes nothing net until it has earned trust. The reputation curve in 6.5 is calibrated so that reaching a rate where the fetcher is net positive takes about 2000 verified deliveries, which at 3 pages/s is a bit under an hour of honest work.

Replay is deliberately delayed by a random 30 to 600 seconds, so a fetcher cannot detect that it is about to be checked by watching for a second request.

### Layer 4: cross fetcher quorum, targeted

For high value URLs the coordinator leases the same URL to k independent fetchers, requiring different self reported ASNs and different observed IPs, and compares their stability digests against each other.

k is 2 for the top 100k hosts by harmonic centrality, 3 for seeds and sitemap indexes, and 1 for everything else. Disagreement between two fetchers triggers a coordinator replay to break the tie. The fetcher on the wrong side of the tie break takes a large reputation hit.

Quorum is expensive, roughly doubling the cost of the URLs it covers, so it is reserved for the URLs whose corruption would matter most. In practice that is under 1 percent of volume.

### Layer 5: canaries, continuous

The coordinator injects leases whose correct answer it already knows.

**Owned canaries.** Pages on domains we control that serve a token derived from `HMAC(secret, url || time_bucket)`. A fetcher that returns anything else did not fetch the page. These are the cheapest and most conclusive check in the system, and they cost one HTTP request each.

**Known stable canaries.** Real URLs that we have fetched repeatedly and observed to be byte stable over days. Standards documents, archived pages, static assets. A fetcher returning something different from a known stable page has either a broken proxy or bad intent, and layer 3 disambiguates.

**Recent replays.** Real URLs we fetched ourselves within the last hour, re leased to a fetcher. Good coverage of realistic content.

Canary rate is 2 percent of a new fetcher's leases, falling to 0.2 percent at high reputation, and canaries are indistinguishable from normal leases.

### Layer 6: TLS chain check, free when present

The receipt carries sha256 of each DER certificate in the chain. The coordinator checks that the chain terminates in a public root, that the leaf covers the requested host, and that the leaf digest matches what it or another fetcher has seen for that host recently.

This catches a fetcher behind a corporate MITM proxy, a fetcher using an interception proxy to modify content, and a fetcher fabricating entirely, since fabricating a plausible cert chain digest for an arbitrary host requires actually connecting to it. Certificate rotation produces false positives, so a mismatch raises sampling rather than rejecting, and a host's leaf set is tracked as a small recent history rather than a single value.

### Layer 7: frontier corroboration, structural

This is the defence against the frontier attacker, and it is a structural rule rather than a check.

Links extracted by a fetcher below reputation 0.6 do not enter the frontier. They enter a holding pen keyed by `(url_key, discovering_fetcher)`. A URL graduates from the holding pen when any of the following is true:

- It was independently discovered by a fetcher above 0.6, or by the coordinator's own fetching
- It was discovered by two fetchers below 0.6 with different observed ASNs
- It appears in a sitemap or feed for its own host
- The discovering fetcher's reputation rises above 0.6 afterwards, which retroactively graduates its pending discoveries

The holding pen is capped per fetcher at 100k entries and per PLD at 10k entries, and it expires after 30 days. The per PLD cap is the specific defence against the DoS-by-frontier attack: no fetcher, and no group of fetchers, can cause more than 10k unverified URLs to be queued against a single site.

## 6.3 What happens when a check fails

Three outcomes, and choosing the right one is what keeps volunteers.

**Reject.** The delivery is discarded, the lease is rescheduled, the fetcher takes a small reputation hit. Used for layer 1 failures and clear layer 3 disagreements. This is recoverable and expected; a fetcher with a flaky connection will hit it sometimes.

**Quarantine.** The delivery is stored in a quarantine table with the full receipt and payload, not published, and flagged for review. The fetcher's sampling rate goes to 1.0 for 24 hours. Used for canary failures, TLS mismatches, and repeated layer 3 disagreement. Quarantined rows are kept 7 days then dropped.

**Ban.** The fetcher key is permanently rejected. Used for exactly two things: an audit response whose bytes do not match a receipt the fetcher already signed, and a canary failure on an owned canary. Both are impossible by accident. Everything else, including sustained incompetence, results in reputation decay to zero rather than a ban, because a fetcher at zero reputation is granted 0.5 pages/s with 100 percent replay, which costs us almost nothing and lets a fixed installation recover.

Bans are published. `open-index/umi-meta` carries a `banned_fetchers` table with the key, the timestamp, and the reason code. That is both a deterrent and a record that a corpus consumer can use.

## 6.4 Provenance in the published data

Every published page row carries `fetcher_id`, `receipt_id`, `verified_by` (a bitmask of which layers ran and passed) and `tier_used`. Receipts themselves are published as a separate table in `open-index/umi-receipts`, keyed by receipt id, containing the full signed structure minus the payload.

This is the part that matters most and it is cheap: roughly 200 bytes per row. If a fetcher is discovered to have been poisoning six months after the fact, anyone holding the corpus can write a single SQL predicate to find and drop every row it touched. Without provenance, the only remedy for one bad actor is to distrust the entire corpus, and that is the failure mode that kills an open dataset.

The receipt signatures are verifiable by anyone, since fetcher public keys are the fetcher ids. A third party can audit our verification rather than trusting it.

## 6.5 Reputation

One scalar per fetcher in [0, 1], updated on every verified event.

```
on pass:  rep += (1 - rep) * 0.02      // asymptotic approach to 1
on soft:  rep -= rep * 0.10            // reject, plausibility flag
on hard:  rep -= rep * 0.50            // quarantine, quorum loss
on decay: rep -= rep * 0.01 per day idle
```

Starting reputation is 0.0, or 0.3 with a valid attestation from a known party, or 0.5 for a fetcher operated by a coordinator operator. The 0.02 gain rate means about 115 consecutive passes to reach 0.9, and with the replay sampling curve in 6.3 that is roughly 2000 deliveries before a fetcher is cheap to trust.

Reputation gates four things: `granted_rate` from doc 04.8, replay sampling probability, whether extracted links skip the holding pen at 0.6, and eligibility for T3 and T4 work at 0.8.

Reputation decays when idle so that a fetcher cannot build trust slowly, go quiet for six months, and return with a clean high score to spend.

Deliberately not included: any transferable reputation, any way to buy reputation, any reputation that survives a key change. A new key is a new fetcher and starts at zero. This means the cost of a Sybil attack is 2000 honest deliveries per identity, which is the entire point.

## 6.6 Cost of the verification system

Worth stating, because a verification design that costs more than it saves is worse than not having a fleet.

At steady state with a mature fleet averaging reputation 0.7, replay sampling is about 3 percent, quorum adds about 1 percent, canaries add 0.3 percent, and audits pull raw bytes for about 0.5 percent of deliveries. Total coordinator overhead is roughly 5 percent of the fleet's page volume in extra fetches, plus the audit bandwidth.

For a fleet contributing 1400 pages/s, that is 70 pages/s of coordinator verification work against 750 pages/s of coordinator capacity, so about 9 percent of our own capacity spent checking other people's. That is a reasonable price. If it drifts above 20 percent the sampling curves need retuning, and doc 15 puts that ratio on the dashboard.

The expensive phase is the beginning. A fleet of entirely new fetchers costs 100 percent replay, meaning the coordinator does all the work twice. Onboarding therefore has to be gradual, and doc 16 gates the community fleet launch on the coordinator having spare capacity to absorb it.

## 6.7 Calibration that has to happen before launch

Two numbers in this document are guesses and both must be measured at milestone 4 before the fleet opens.

**Honest disagreement rate.** Two honest fetchers on different networks fetching the same URL seconds apart will sometimes disagree, because of geo targeting, A/B tests, CDN variation, and rotating content. If the 0.90 MinHash threshold rejects more than 2 percent of honest pairs, the thresholds are wrong and the whole scheme produces noise. The experiment is 100k URLs fetched by two coordinator nodes with different egress, and the output is the distribution of pairwise Jaccard, from which the thresholds get set at a measured false positive rate rather than a guessed one.

**Per host disagreement, not just global.** Some hosts will be inherently unstable, and those hosts should get a per host threshold rather than dragging the global one down. The same experiment produces the per host distribution, and hosts above a variance cutoff get flagged `unstable` and are excluded from quorum and canary duty.

Until both are measured, the community fleet stays closed and all fetching happens on server1, server2 and server3.
