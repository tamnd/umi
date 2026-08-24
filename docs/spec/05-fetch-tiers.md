# 05 Fetch tiers

A single HTTP client cannot crawl the 2026 web. A meaningful fraction of pages are client rendered and arrive as an empty shell, and another fraction are behind bot management that scores TLS, HTTP/2 and header signals together and refuses anything that looks like a script. Common Crawl accepts both losses, which is part of why its snapshots have shrunk 29 percent since early 2025. umi does not accept them, but it also does not treat every block as a challenge to be defeated. The tier ladder is how those two positions coexist.

## 5.1 The line this document does not cross

Stated first so the rest reads correctly.

robots.txt is authoritative at every tier. A disallowed URL is never fetched, never leased, and never appears in a lease token. There is no flag that changes this and no tier that bypasses it. Doc 07 owns robots handling.

Tiers 1 through 3 exist so that a crawler which is obeying robots, identifying itself honestly, signing its requests, and respecting rate limits is not misclassified by a heuristic that was written to catch credential stuffers. That is a different thing from working around a decision the site made. The signal that separates them is robots.txt plus any AIPREF `Content-Usage` line: if the site has told us not to, we stop, and no amount of 403 chasing is warranted.

Tier 4 is where the ambiguity lives and it is therefore gated by an explicit per domain allowlist that a named operator has to add by hand, with a reason recorded in the state store. It is off by default, it is off in the public crawl, and it exists for the case where someone has an agreement with a site and needs the crawler to honour it.

## 5.2 The ladder

| Tier | Name | What it is | Cost per page | Share of volume |
| --- | --- | --- | --- | --- |
| T0 | REVALIDATE | Conditional GET with `If-None-Match` / `If-Modified-Since` | ~500 B, ~0.1 ms CPU | 30 to 50 percent at steady state |
| T1 | PLAIN | hyper + rustls, HTTP/2, honest UA, signed | 150 KB, ~5 ms CPU | 90 percent of non revalidate |
| T2 | EMULATED | wreq + BoringSSL browser profile, matched JA4 and H2 SETTINGS | 150 KB, ~6 ms CPU | 5 to 9 percent |
| T3 | RENDERED | Headless Chromium via CDP, subresources filtered | 600 KB to 2 MB, 1 to 3 s, 150 to 300 MB RSS | under 1 percent |
| T4 | SUPERVISED | Full browser, real profile, human in the loop, allowlisted | expensive, manual | negligible |

The share column is the design assumption, not an observation. Measuring it against the real web is a milestone 2 gate in doc 16, and if T3 turns out to be needed for 5 percent of pages rather than 1 percent, the capacity plan in doc 01 has to change rather than the cap.

## 5.3 T0, revalidate

The cheapest fetch is the one that returns 304. At steady state, when the fresh core is large and the discovery rate has flattened, most of what the crawler does is check whether things changed. A 304 is roughly 500 bytes of response headers against 150 KB of body, which is a 300x saving on the constraint that doc 01 identifies as the one to verify.

T0 sends `If-None-Match` with the stored ETag and `If-Modified-Since` with the stored `Last-Modified`, both from the ledger. It sends both when both are known, because servers are inconsistent about which they honour. On 304 the outcome is recorded, the change rate estimator gets a negative observation, and the next due time is pushed out. No extraction runs and no row is written, though the ledger's `last_checked` moves.

Two traps.

Some servers return 200 with identical content even when given a valid ETag. That is detected by the content digest matching the stored one, and it is recorded as a `weak_revalidator` flag on the host so the scheduler can stop paying the full body cost for a site that will never say 304. After three such observations the host's T0 preference drops.

Some servers return 304 while the content has in fact changed, usually because of a reverse proxy misconfiguration. This is caught by forcing an unconditional fetch on a sampled fraction of T0 outcomes, 1 percent by default, and comparing digests. A host that fails this check gets `lying_revalidator` and its T0 is disabled.

## 5.4 T1, plain

The default and the one that should handle the overwhelming majority of the web.

`hyper` over `rustls`, HTTP/2 preferred with HTTP/1.1 fallback, connection pooling keyed by host with a cap of 2 connections per host, gzip, brotli and zstd content encoding, 30 second total timeout, 10 second connect timeout, 512 KB body cap by default.

The identity is honest and fixed. Doc 07 specifies the exact user agent string and the Web Bot Auth signature. The point of T1 is to be recognisable: a site operator who looks at their logs should be able to tell exactly who we are, find our documentation page, and block us in one line if they want to.

T1 does not try to look like a browser and should not. Sending Chrome's header set from a rustls stack produces a mismatch between the TLS fingerprint and the HTTP layer that is more suspicious than being honestly a bot, and it is exactly the inconsistency that JA4 plus JA4H correlation is designed to catch.

## 5.5 T2, emulated

T2 exists for the case where a site's bot management refuses a non browser TLS stack even though robots.txt allows us. That is a real and common configuration, usually a default rule rather than a decision anyone made about us.

The implementation is `wreq` with `wreq-util` profiles, which is the current member of the `reqwest-impersonate` to `rquest` to `wreq` lineage. It links BoringSSL and exposes fine grained control over TLS extensions and HTTP/2 settings rather than trying to reproduce a fingerprint from a string, which is the right design because JA3, JA4 and the Akamai H2 fingerprint cannot be reliably emulated from a hash. Profiles are selected as `Emulation::Chrome136` style presets and the crate maintains a hundred or so of them.

Three engineering facts to build around.

**Consistency across layers is the entire requirement.** For a request that never runs JavaScript, TLS plus HTTP/2 plus headers is the whole detection budget. Getting JA4 right while sending the wrong H2 SETTINGS, or the wrong header order and casing, is worse than not trying. `wreq` handles HTTP/1 header case sensitivity and H2 settings, and the profile has to be used as a unit rather than cherry picked.

**BoringSSL and openssl-sys cannot coexist.** They share symbol prefixes, which causes link failures or, when it links, segfaults. T2 is therefore behind a non default `emulation` cargo feature, and CI asserts that the default build has no `openssl-sys` anywhere in the dependency tree. This is an annoying constraint that will bite whoever adds a dependency that pulls in native TLS, so the CI check is not optional.

**Byte parity is not achievable in process and does not need to be.** `curl-impersonate` reproduces a specific browser build down to the byte by shipping a patched BoringSSL and nghttp2, at the cost of spawning a subprocess per request. That is unacceptable at 250 pages/s. `wreq` gets close with curated presets and that is the right trade. If a specific high value domain genuinely needs byte parity, it belongs in T3 or T4, not in a special case in T2.

Verification of our own fingerprint is a startup self check, not an assumption. On boot, `umid` and the reference fetcher fetch a fingerprint echo endpoint and assert that the observed JA4 matches the profile's expected value. A silent mismatch after a dependency bump would degrade T2 to something worse than T1 without anyone noticing.

## 5.6 T3, rendered

Headless Chromium driven over the Chrome DevTools Protocol, for pages whose content only exists after JavaScript runs.

The Rust options are `chromiumoxide` for async CDP, `headless_chrome` for a blocking API which is wrong for a pooled architecture, and the newer `rustwright`, which puts a Playwright shaped API on a Rust CDP engine with no Node driver subprocess. The driver language does not change the browser's memory profile at all, since the Chromium binary is identical either way. What it saves is the controller process, which is real when running hundreds of sessions but is not the headline. Start with `chromiumoxide` and revisit `rustwright` if the per session controller overhead shows up in a profile.

Subresource policy is where T3 either costs 600 KB or 3 MB per page. The CDP request interception rules are: allow `Document`, `XHR`, `Fetch` and `Script`, block `Image`, `Media`, `Font` and `Stylesheet`, block any request to a third party domain that is on the tracker list, and cap total subresource bytes at 2 MB. Blocking stylesheets occasionally breaks a layout dependent script and that is an acceptable loss.

Completion is `networkIdle0` with a 1500 ms quiet period, a 10 second hard ceiling, and then a `Runtime.evaluate` returning `document.documentElement.outerHTML`. The resulting HTML goes through the same extractor as every other tier, which means a T3 row is indistinguishable from a T1 row downstream except for `tier_used` in the receipt.

Pool management, given the memory numbers in doc 01: one browser process per host machine, one incognito browser context per PLD, tabs recycled after 50 pages or 10 minutes to bound leaks, hard cap of 8 tabs on server2 and zero on server1. server2 is the only fleet box that runs a browser at all.

On detection, the honest position is that T3 is not stealthy and pretending otherwise wastes effort. `chaser-oxide` and `rustwright` both mitigate the obvious tells, avoiding `Runtime.enable` on the default path, using `Page.createIsolatedWorld` instead, normalising the `HeadlessChrome/` user agent token, and cleaning up `navigator.webdriver`. Their own authors say this is not undetectable. umi's position is that T3 is for rendering, not for evasion. If a site blocks headless Chromium specifically, the answer is that the page does not get crawled.

## 5.7 T4, supervised

A real browser with a real profile, driven by or with a human, on an explicit per domain allowlist.

The allowlist entry records the domain, the operator who added it, an ISO timestamp, and a free text reason. It is stored in state, it is included in the published `hosts` table as a boolean plus the reason, and it appears in the operations dashboard. Anyone reading the corpus can see which domains were crawled this way and why.

T4 only ever runs on a fetcher whose operator has opted in with `umi fetch --allow-supervised`. It is never dispatched to a fetcher that has not, and the default is off. Community fetchers are never given T4 work unless attested, opted in, and explicitly enabled by a coordinator operator.

This tier will get very little use and that is correct.

## 5.8 Escalation

Escalation is per host state, not per URL, and it is learned rather than configured.

Every host record carries a `tier_policy`:

```rust
struct TierPolicy {
    preferred: Tier,          // where to start
    max: Tier,                // where to stop
    last_success: Tier,
    consecutive_blocks: u16,
    last_probe_down: Timestamp, // last time we retried a cheaper tier
    render_required: bool,      // T1 body was a shell, T3 was not
    weak_revalidator: bool,
    lying_revalidator: bool,
}
```

A fetch starts at `preferred`, which is `T0` when a revalidator is known and `T1` otherwise. It escalates on a block signal, which is one of:

- HTTP 403, 429, or 503 whose body matches a known interstitial, or which carries `cf-mitigated`, `x-datadome`, `server: AkamaiGHost` with a challenge shaped body, or an equivalent vendor marker
- HTTP 200 whose extracted text is under 200 characters and whose body contains a challenge marker
- A TLS handshake failure that succeeds when retried with a browser profile

It escalates on a render signal, which is distinct and matters: HTTP 200 with a normal looking response but extracted text under 500 characters and a script tag count above 5, against a `<noscript>` or an obvious app root element. That is a client rendered shell, and it means T3, not T2.

Escalation is bounded. `max` starts at T2 for the public crawl and only reaches T3 when `render_required` has been confirmed by an actual T3 fetch producing meaningfully more text than T1 did. It never reaches T4 without an allowlist entry.

De-escalation is the part crawlers usually forget. Every 7 days, or after 1000 fetches at an elevated tier, one fetch is probed at the tier below. If it succeeds, `preferred` drops. Without this a single bad afternoon of Cloudflare tuning pins a domain to browser rendering forever, and browser capacity is the scarcest resource in the fleet.

Backoff on repeated blocks is exponential with a ceiling: 1 minute, 5, 25, 2 hours, 12 hours, then a daily probe. After 30 consecutive days of blocks at `max` the host is marked `refusing` and drops out of the frontier entirely, with a record of why. It is retried monthly at T1 only, which costs one request per host per month.

## 5.9 Budget enforcement

Tier cost is enforced globally, not just per host, because a fleet that decides 20 percent of the web needs rendering will simply stop.

```
render_budget_per_second = min(
    browser_pool_capacity,                  // 8 tabs / 2s = 4 p/s on server2
    fleet_page_rate * max_render_fraction   // 750 * 0.01 = 7.5 p/s
)
```

When T3 demand exceeds the budget, the excess does not fail. It goes into a deferred queue ordered by page priority, and it is preferentially routed to community fetchers that advertise T3 capability. A volunteer with a spare desktop core is worth more to this system than any amount of scheduling cleverness on server2.

The same applies to T2, which is cheap on CPU but does consume the BoringSSL connection pool. The cap there is 15 percent of fleet volume, and crossing it triggers an alert rather than a throttle, because a sudden T2 spike usually means a vendor changed a default rule and someone should look at it.

## 5.10 What gets recorded

Every fetch records `tier_used` and `tier_path` in the receipt, and both are published. That means the corpus itself says which fraction of the web needed which tier, per month, per TLD, per host. Nobody currently publishes that dataset and it is arguably as interesting as the pages.

The host level tier policy is published in the `hosts` table too, so a researcher can ask which parts of the web have become inaccessible to plain HTTP clients over time. Given the direction of Cloudflare's defaults since July 2025, that series is going to be worth having.
