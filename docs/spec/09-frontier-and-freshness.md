# 09 Frontier and freshness

The frontier decides what to fetch next. Freshness decides when to fetch it again. In a continuous crawler these are the same system, because a recrawl is just a URL whose due time came around, which is the one genuinely useful idea in the Google frontier patents: the crawler picks a URL, fetches it, and then either drops it or reschedules it. There is no separate refresh pipeline.

## 9.1 The Mercator structure

Unchanged from the 1999 design, because nothing better has been published and everything that claims to be better is this with extra steps.

Two layers. The front is priority: F queues, and a URL with priority p lands in queue p. The back is politeness: one queue per host, and a host appears in exactly one back queue. A heap keyed by next allowed fetch time picks which host is ready. When a host's queue empties, it is removed from the heap and a new host is pulled from a front queue to replace it.

The invariant that makes it work is that each back queue holds exactly one host, so pulling from the heap can never violate politeness. Priority only decides which hosts get back queues, never the timing.

umi's version differs in three places.

**Back queues are per host but heaps are per PLD.** A domain with 50000 subdomains would otherwise create 50000 back queues. The per PLD cap from doc 07.6 already limits total throughput to that domain, so the structure is a per PLD heap of hosts, and a global heap of PLDs.

**The number of back queues is bounded by resident shards, not by memory.** Doc 08.6 makes a domain's frontier live in its shard, so the working set of back queues is the set of warm domains. Adding a host to the frontier is not a memory allocation, it is a write to a shard.

**Priority is recomputed lazily, not maintained.** F queues in the original are static. Ours are computed at lease time from the ledger row, because priority depends on time and maintaining a sorted structure under time dependent keys is worse than recomputing.

## 9.2 Priority

One `u16` fixed point score per URL, computed at lease time.

```
priority = w_host * host_quality
         + w_depth * depth_decay
         + w_link  * link_evidence
         + w_fresh * freshness_urgency
         + w_scope * scope_bonus
```

`host_quality` starts from Common Crawl's harmonic centrality domain ranks, which `ccrawl-cli` can already fetch, and moves with our own observations: how much unique content the host produces, its error rate, its duplicate rate. This is deliberately the largest term. Host level quality is the single strongest available signal and per page signals are noisy at discovery time, when we have not seen the page yet.

`depth_decay` is `1 / (1 + depth)` where depth is link distance from the nearest seed. Cheap, effective, and the main defence against crawler traps that generate infinite paths.

In fixed point that term needs a stated ceiling, and the first draft did not give one. The score is a `u16` and doc 11.1 forbids floats, so the term is computed as `60000 / (1 + depth)` in integer arithmetic and the depth term's own maximum is 60000, which is depth zero. That leaves 5535 of the `u16` range as headroom so that the five terms can be summed and weighted without the intermediate overflowing, and it makes the term reach zero at depth 60000 rather than asymptotically approaching it in a float. Stating the ceiling is what makes the weights comparable across terms: a weight of 0.2 on depth means a fifth of 60000 and not a fifth of something unspecified.

`link_evidence` is the count of distinct PLDs linking to the URL, log scaled and capped. It arrives late, since we only learn it as we crawl, so it mostly affects recrawl priority rather than first crawl. It is capped hard because it is the term an attacker would target.

`freshness_urgency` is zero for a URL never fetched and rises with overdue-ness for one that has, per 9.4. This is the term that makes revisits compete with discovery on one scale, which is the whole reason there is no separate refresh pipeline.

`scope_bonus` is the focused crawl term from doc 13. In a general crawl it is zero. In a focused crawl it dominates everything else.

The weights are configuration, they ship with defaults, and the defaults are guesses that have to be evaluated at milestone 3 against a held out measure of how much unique, non duplicate, non spam content each policy discovers per thousand fetches.

## 9.3 Politeness scheduling

Per host, the next allowed fetch time is `last_fetch + max(crawl_delay, adaptive_delay)`, with the adaptive term from doc 07.6. Per PLD, a token bucket at 20 requests per second. Both are enforced at lease issue, which is the only place they can be enforced reliably given an open fetcher fleet.

The scheduler loop, per coordinator, once per 100 ms:

```
1. pop PLDs from the global heap whose next_ready <= now, up to lease_batch
2. for each, warm the shard if cold (this is the expensive branch)
3. pop ready hosts from that PLD's heap
4. for each host, take the top priority pending URL from the ledger
5. build a lease, mark in flight, push the host back with next_ready = now + delay
6. push the PLD back with next_ready = min over its hosts
```

Step 2 is where the design either works or does not. A cold shard costs an object GET, which is 50 to 100 ms, and doing that inside the scheduler loop would cap the whole crawl at 10 to 20 domains per second. So warming is asynchronous and speculative: a background task watches the PLD heap for domains coming ready in the next 30 seconds and warms them ahead of time, and the scheduler skips any domain that is not yet resident rather than blocking on it. A domain that is repeatedly skipped gets its priority boosted so it does not starve.

Locality is an explicit scheduling objective for the same reason. Given two domains of equal priority, prefer the resident one. Given a choice of how many URLs to lease for a warm domain, take more of them, because the shard is already paid for. This is why leases are issued in per domain batches rather than one URL at a time, and the batch size is tuned so a warm shard yields at least 100 fetches before eviction becomes attractive.

## 9.4 The change rate model

The classic model is that a page changes as a Poisson process with rate lambda, and that estimating lambda from a sequence of visits where you only observe changed or not changed is a solved problem with a known bias correction for the fact that multiple changes between visits look like one.

The estimator, per URL, updated on every fetch:

```
n = fetch_count since last reset
x = change_count (times content_hash differed from previous)
T = total elapsed observation time

lambda_hat = -log((n - x + 0.5) / (n + 0.5)) / mean_interval
```

The 0.5 terms are the standard smoothing that keeps the estimate finite when a page has never changed or has changed every time. A page observed 10 times with 0 changes gets a small nonzero rate rather than infinity in the revisit interval, and a page that changed every time gets a large but finite one.

Fetches are 8 bytes of counters in the ledger, which is why this is affordable at 100 billion URLs. There is no per URL history, no model, no feature vector.

The revisit interval is not proportional to lambda, and this is the part that gets implemented wrong most often. A page that changes every minute cannot be kept fresh at any budget, so spending on it is waste. The allocation maximises expected freshness per fetch under a bandwidth budget, which produces a non monotonic policy: interval decreases with lambda up to a point and then increases again as the page becomes hopeless.

```
interval = clamp(
    k / lambda_hat                 if lambda_hat < churn_ceiling
    long_interval                  otherwise (give up on tracking, sample it)
    , min_interval, max_interval)
```

`min_interval` is 5 minutes, `max_interval` is 180 days, `churn_ceiling` is one change per 10 minutes. A page above the ceiling drops to a daily sample, because we cannot represent it faithfully and pretending otherwise burns capacity that other pages would use better.

`k` is a half. It is the expected number of changes per visit the interval aims at, so a little under 40 percent of refetches find something new. Larger than that wastes fetches on pages that did not move and smaller leaves the corpus stale.

Two rules bound the estimator on either side of where it has evidence. The interval is never longer than twice the time we have actually watched the page, because two fetches an hour apart with nothing seen to change is not grounds for a six month nap, and the cap falls away on its own once the observation window is longer than the answer. The interval is never shorter than `min_interval` even when every visit found a change, because a page whose extracted text contains a timestamp changes on every fetch by construction.

The estimate is a lower bound rather than a measurement when every observed interval ended in a change, because a page that changes every minute and a page that changes every hour look identical if we only look once an hour. In that case the ceiling only fires when the lower bound alone is enough, which means a page we cannot follow settles an hour or two apart rather than at the daily sample. Sampling faster to tell the two cases apart costs more than the answer is worth. `Last-Modified` is what actually solves it, because it reports the change time rather than the fact of a change.

The arithmetic is fixed point rather than floating point. Doc 08.5 promises a crawl directory can be copied from an x86 machine to an arm one and resumed, `f64::ln` is not required to give the same bits on both, and two coordinators disagreeing about when a page is due is not a difference we are willing to have.

The publisher supplied signals override the estimator when present, and they are always better than it:

`Last-Modified` and `ETag` make revalidation nearly free, so a page with a working revalidator gets a shorter interval than its lambda alone would justify. The cost of being wrong is 500 bytes.

Sitemap `lastmod` is a direct statement of when the page changed. A sitemap that is fetched hourly and has accurate `lastmod` turns the whole change rate problem into a lookup, and for the sites that do it properly it is worth more than every other signal combined.

RSS and Atom feeds are the same thing for new content rather than changed content, and they are the realtime path in 9.6.

## 9.5 Refresh classes and budget

Capacity is allocated to classes rather than left to compete freely, because a pure priority queue lets discovery starve refresh or the reverse, depending on which happens to score higher this week.

| Class | Interval | Membership | Fleet budget |
| --- | --- | --- | --- |
| realtime | under 1 h | feed and sitemap driven, news front pages | 5 percent |
| hourly | 1 to 6 h | high lambda, high quality, working revalidator | 10 percent |
| daily | 6 h to 3 d | active pages on quality hosts | 20 percent |
| weekly | 3 to 30 d | the general web | 25 percent |
| dormant | 30 d and up | never observed to change | 5 percent |
| discovery | n/a | never fetched | 35 percent |

The intervals in that table are the boundaries the code uses and not a description of them, because a class has to be decided the same way by every backend or the budget means something different in each one.

35 percent to discovery is a policy choice with a clear consequence. At 750 pages/s that is 262 pages/s of new URLs, or 8.3 billion a year, so reaching 100 billion pages on our own hardware is 12 years rather than 4.2. The community fleet changes this arithmetic and nothing else does. The split is configurable and doc 16 raises the discovery share during the initial land grab and lowers it once coverage is respectable.

What the refresh budget buys, concretely:

```
refresh budget = 65% of 750 p/s = 487 p/s = 42.1 million refreshes/day
fresh core on a 15 day cycle    = 631 million URLs
fresh core on a 24 hour cycle   = 42 million URLs
realtime class (5%)             = 37.5 p/s = 3.2 million/day
```

So a fresh core of roughly 600 million URLs at 15 day staleness, with about 40 million of them daily, is what the three boxes support. That is the honest version of "fresher than Common Crawl". It is not the whole web kept fresh, it is a large, well chosen core kept fresh while the long tail is revisited on the order of months. Doc 01's target of 100 million in the fresh core has plenty of room.

Class assignment is recomputed on every fetch from lambda, host quality, and revalidator availability. A page moves between classes freely and there is no manual assignment. Concretely the class is read off the interval the 9.4 estimator last produced, so there is no second model to keep in step with the first: a page that starts changing daily is in the daily class the moment the estimator notices, and a page that goes quiet leaves it the same way. A URL nobody has fetched is in discovery by definition.

A share is a floor and not a cap. Each class is offered its share of a lease batch first, and whatever the classes did not want is then offered back to them in turn until the batch is full. A frontier that is nothing but discovery still fills every batch, and the split costs nothing when there is nothing to split. Without that second round the shares would be caps, and a fleet whose realtime class is empty would run at 95 percent of its capacity for no reason.

The class is derived, so storing it is redundant, but the state backends store it anyway and it is the leading column of the index the scheduler leases through. That is not a second source of truth, it is an index key, and the rule is that whatever writes the schedule writes the class in the same statement. What it buys is reachability. A share is only enforceable if a class's due work can be reached without walking past everything ahead of it in priority order, and a lease scan is bounded, so a thousand due hourly URLs sitting below a hundred thousand discovery rows would never be reached and their share would be quietly zero. That is the exact failure this section exists to prevent. With the class in the index the scheduler runs one ordered scan per class, each of which is a prefix of the index, and the bound is on the total rather than on each of the six.

The scans stay open across both rounds. Resuming with an offset instead would be slower than not splitting at all, because an offset in SQLite is not a seek and does the joins again for every row it skips, and the round that spends the leftover would redo a large part of the batch. Measured on server3 against the same benchmark that gates doc 08, the split as described is parity with no split on both lease and complete.

## 9.6 The realtime path

The batch scheduler has a floor of one scheduling tick, and the change rate model has a floor of `min_interval`. Neither gets a news article into the corpus within seconds of publication. That needs a separate, small, push shaped path.

Sources, in order of value:

**Feeds.** Every host with an RSS or Atom feed, discovered from `<link rel=alternate>` or from common paths, gets its feed polled on its own schedule derived from observed post frequency, floored at 60 seconds. A feed entry that is not in the seen set is admitted at maximum priority and bypasses the class budget. A feed is a few KB and yields several new URLs, so this is by far the cheapest discovery in the system.

**Sitemaps.** `Sitemap:` lines from robots.txt, plus `/sitemap.xml`. News sitemaps under the Google News extension are polled at feed frequency. Regular sitemaps are polled at a rate derived from how often their `lastmod` values move. Sitemap index files are followed to a depth of 3 with a cap of 50000 URLs per host per poll, which is the defence against a sitemap that lists ten million URLs.

**WebSub.** If a feed advertises a WebSub hub, subscribe. This turns polling into push and costs one HTTP callback endpoint. Uptake is low but the sites that do it are exactly the high frequency publishers we care about most.

**Hint API.** `POST /v1/hint` on the coordinator accepts a URL and a reason from an authenticated caller. This is how `tamnd/*-cli` tools, partners, and our own tooling push a URL in. Rate limited per caller, and hinted URLs still go through robots, admission and scope checks. Unauthenticated hints are accepted into the holding pen only.

The realtime path is capped at 5 percent of fleet capacity, and the cap is enforced, because an unbounded push path is a DoS vector and because feed polling can quietly grow to dominate a crawl if nobody is watching.

## 9.7 Traps and defences

Crawler traps are the thing that turns a working crawler into a broken one overnight, so the defences are structural rather than heuristic.

**Infinite paths.** Depth cap of 30 from the nearest seed, and a per host cap on distinct path prefixes at each depth. Calendar pages generating `/2027/03/14/` forever hit the depth cap and the per prefix cap first.

**Parameter explosion.** Canonicalisation in doc 11.2 strips known tracking parameters, but faceted search generates real, distinct, useless URLs. The defence is a per host budget: no more than N distinct URLs sharing a path prefix, N starting at 10000 and adjusted by host quality. Exceeding it does not error, it deprioritises the excess to the bottom of the queue where it never gets fetched.

**Session identifiers in paths.** Detected by finding path segments that look high entropy and vary across otherwise identical URLs on the same host. Such segments are treated as wildcards for the purpose of the prefix budget.

**Soft 404s.** A host returning 200 with near identical content for many distinct URLs is the classic sign. Detected by the near duplicate clustering in doc 11 running per host: if more than 60 percent of a host's pages fall into one cluster, the host is flagged and its budget is cut hard.

**Mirror and duplicate hosts.** Detected by content hash overlap across hosts. When two hosts share more than 80 percent of their content hashes, one is picked as canonical by preferring the shorter host name, then the one with more inbound links, and the other is deprioritised rather than blocked. The relationship is recorded and published, since a map of the web's mirrors is independently useful.

**Redirect loops and chains.** Cap of 5 hops, and doc 04 already stops at the first off host redirect and hands it back to admission.

**The unbounded host.** Any single host is capped at 1 percent of the fleet's daily budget by default, so 648000 pages a day. Wikipedia and a handful of others get an explicit higher cap. Without this one host can absorb the entire crawl.

## 9.8 Restart and recovery

The frontier is entirely in state, so restart is not special. A coordinator comes up, opens its state file, replays the redo log, re-issues leases whose deadlines passed, and resumes. The scheduler is stateless in memory: the heaps are rebuilt from the resident shards on demand, which takes about a second per thousand resident domains.

The one thing that is lost is in memory speculative warming, which rebuilds itself within a scheduling tick or two.

Doc 01's target of losing no more than the lease window on restart follows directly, and the mechanism is that a lease is durable before it is handed out.
