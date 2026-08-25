# 04 The umi/1 fetch protocol

The whole 100 billion page target rests on this document. server1, server2 and server3 supply about 750 pages per second between them and the 18 month target needs 2114. The difference has to come from machines we do not own, which means fetching has to be a wire contract that anyone can implement and that we can check.

## 4.1 Design constraints

**A volunteer must be able to start in one command.** Download a static binary, run `umi fetch --coordinator https://umi.dev`, done. No account, no config file, no Docker, no key ceremony. Registration happens on first contact and the key is generated locally.

**The protocol must be implementable in an afternoon in any language.** HTTPS with CBOR bodies, five endpoints, no gRPC, no custom framing, no streaming requirement. Someone should be able to write a working fetcher in Python in 200 lines. If they can, the fleet grows.

**The coordinator must never trust the fetcher.** Every claim in a delivery is either checkable now, checkable later, or explicitly labelled unverified in the published data. Doc 06 covers the checking. This document defines what has to be in the message for the checking to be possible.

**Bandwidth flows away from the coordinator, not toward it.** A community fetcher that uploads raw HTML consumes coordinator inbound at exactly the rate we would have spent fetching the page ourselves, which defeats the entire purpose of having it. So fetchers extract at the edge and upload the extraction, which is roughly 6 KB against 150 KB of raw, plus digests of the raw bytes. A thousand community fetchers at 3 pages/s each cost the coordinator 18 MB/s of inbound rather than 450 MB/s, and that is the difference between a fleet that scales and one that melts the coordinator.

**Everything is idempotent.** A lease can be delivered twice, a delivery can be retried, a fetcher can crash mid batch. Nothing in the protocol requires exactly once semantics.

## 4.2 Transport and encoding

HTTP/2 over TLS 1.3. Bodies are CBOR (RFC 8949) with a deterministic encoding profile so that digests over message bodies are stable. JSON is accepted on every endpoint for debugging with `Content-Type: application/json` and is never used for anything that gets signed.

Every request carries `umi-protocol: 1` and `umi-agent: <impl>/<version>`. Responses carry `umi-server: umid/<version>`.

Errors are RFC 9457 problem details with a `umi-code` field from a fixed enumeration. Retry behaviour is carried in the response, not guessed by the client: `retry_after_ms` and `retryable: bool`.

## 4.3 Identity

A fetcher identity is an Ed25519 key pair generated on first run and stored at `~/.umi/identity.key`. The public key is the fetcher id, rendered as `umif_` plus base32 of the first 20 bytes of blake3 of the public key.

There is no account, no email, no captcha. Identity is cheap on purpose, because making it expensive does not stop a determined attacker and does stop a casual volunteer. The defence against Sybil identities is not registration friction, it is that a new identity starts with zero reputation and earns work slowly. Doc 06 covers the earning curve.

Optionally a fetcher can present an attestation: a signed statement from a known party vouching for it, a Web Bot Auth key directory URL, or a domain proof via a well known file. Attested fetchers start higher on the reputation curve and can be leased higher value work sooner. Nothing requires attestation.

## 4.4 Endpoints

Five endpoints. That is the whole protocol.

### POST /v1/hello

Announce capabilities, open a session. Called on start and on every reconnect.

```
Hello {
  fetcher_id:      bytes32,          // ed25519 public key
  impl:            text,             // "umi-cli/0.4.1" or "my-python-fetcher/0.1"
  protocol:        uint,             // 1
  tiers:           [uint],           // which of T0..T4 this fetcher can run
  extractor:       text?,            // extractor id+version, null if not extracting
  max_concurrency: uint,             // simultaneous in flight fetches
  target_rate:     float,            // pages/s the operator is willing to sustain
  egress: {
    country:       text?,            // self reported, ISO 3166-1
    asn:           uint?,            // self reported
    ipv6:          bool,
  },
  attestation:     Attestation?,
  signature:       bytes64,          // over the canonical CBOR of everything above
}

HelloAck {
  session:         bytes16,
  fetcher_id:      bytes32,
  reputation:      float,            // 0.0 to 1.0
  policy: {
    max_lease_batch:   uint,         // how many leases per call
    lease_ttl_ms:      uint,
    granted_rate:      float,        // pages/s the coordinator will actually feed
    allowed_tiers:     [uint],
    audit_probability: float,        // fraction of deliveries subject to raw pull
    upload_raw:        bool,         // true for new fetchers, false once trusted
  },
  observed: {
    ip:              text,           // what the coordinator saw, for comparison
    asn:             uint?,
    country:         text?,
  },
  server_time_ms:  uint,
}
```

The `observed` block is not courtesy. A fetcher that self reports an ASN different from the one the coordinator sees is either behind a proxy or lying, and doc 06 weights both.

`granted_rate` is the flow control primitive. The coordinator decides how fast a fetcher is fed based on reputation, on the fetcher's own `target_rate`, and on how much work exists for the PLDs that fetcher is suited to. A new fetcher is granted something small, typically 0.5 pages per second, regardless of what it offers.

### POST /v1/lease

Ask for work. Long polls for up to 20 seconds if nothing is ready.

```
LeaseRequest {
  session:   bytes16,
  want:      uint,                   // capped at policy.max_lease_batch
  tiers:     [uint],                 // subset of allowed, what the fetcher wants now
  hint: {
    prefer_pld: [bytes8]?,           // for focused or affinity fetching
    exclude_pld: [bytes8]?,
  }?,
}

LeaseResponse {
  leases:    [Lease],
  backoff_ms: uint,                  // if leases is empty, wait this long
}

Lease {
  lease_id:      bytes16,
  url:           text,               // already canonical
  url_key:       bytes10,            // 80 bit fingerprint, doc 08
  pld_id:        bytes8,
  tier_hint:     uint,               // start here, escalate per doc 05
  max_tier:      uint,               // do not exceed
  deadline_ms:   uint,               // absolute, coordinator clock
  nonce:         bytes16,            // must appear in the receipt
  conditional: {
    etag:           text?,
    last_modified:  text?,
  }?,
  politeness: {
    min_gap_ms:     uint,            // since the fetcher's last hit on this host
    max_bytes:      uint,            // hard cap, abort past this
    timeout_ms:     uint,
  },
  robots: {
    decision:       "allow",         // coordinator already checked, doc 07
    checked_at_ms:  uint,
    content_usage:  text?,           // AIPREF Content-Usage line if present
  },
  token:         bytes64,            // coordinator ed25519 signature over the lease
}
```

The lease is signed by the coordinator. That matters for a reason that is not obvious: a fetcher can show the lease token to a site operator as proof that it was told to fetch that URL by a specific coordinator, and the coordinator's key is published. When someone complains about traffic, we can answer precisely.

Robots is evaluated by the coordinator, not the fetcher, and the decision is always `allow` because disallowed URLs are never leased. The fetcher does not need a robots parser at all. That is a deliberate reduction of what a third party implementation has to get right, and it means a buggy volunteer fetcher cannot make us impolite.

The exception is redirects. If a fetch redirects off the leased host, the fetcher must not follow it blindly. It records the redirect chain, stops, and returns `redirected_off_host`. The coordinator admits the target as a new candidate and checks robots for it. This costs a round trip and it removes an entire class of politeness bug.

### POST /v1/deliver

Return results. Batched.

```
Delivery {
  session:  bytes16,
  items:    [DeliveryItem],
}

DeliveryItem {
  receipt:  Receipt,
  payload:  Payload?,               // absent for 304, error, or block outcomes
}

Payload {
  extract:  bytes,                  // zstd of canonical CBOR extraction, doc 11
  raw:      bytes?,                 // zstd of raw body, only if policy.upload_raw
}
```

### POST /v1/audit

The coordinator pulls raw bytes for a delivery it wants to check. The fetcher keeps raw bodies in a local ring buffer for `audit_retention` (default 24 hours, 2 GB cap, whichever binds first) exactly so this works.

```
AuditRequest  { session, lease_ids: [bytes16] }
AuditResponse { items: [{ lease_id, raw: bytes?, missing_reason: text? }] }
```

A fetcher that cannot produce raw bytes for an audit within the retention window takes a reputation hit but is not banned, because disks fill and processes restart. A fetcher that produces raw bytes whose digest does not match the receipt it already signed is banned immediately and permanently, because that is not an accident.

### POST /v1/nack

Give work back. Explicit release is much better than waiting for a lease to time out, because the coordinator can reschedule immediately.

```
Nack { session, items: [{ lease_id, reason: NackReason }] }

NackReason = "shutting_down" | "over_capacity" | "tier_unavailable"
           | "host_unreachable" | "operator_refused"
```

`operator_refused` exists so a volunteer can maintain a local blocklist without lying about it. If someone does not want their IP fetching a particular domain, they say so and the coordinator routes it elsewhere. Doc 06 does not penalise this beyond routing.

## 4.5 The receipt

The receipt is the core object of the protocol. It is what makes an untrusted fetch checkable.

```
Receipt {
  version:       uint,               // 1
  lease_id:      bytes16,
  nonce:         bytes16,            // echoed from the lease, anti replay
  fetcher_id:    bytes32,
  url:           text,
  final_url:     text,               // after same host redirects
  fetched_at_ms: uint,               // fetcher clock
  duration_ms:   uint,

  outcome:       Outcome,            // see below
  tier_used:     uint,
  tier_path:     [uint],             // e.g. [1, 2] if T1 was blocked and T2 worked

  request: {
    method:        text,
    redirects:     [{ from: text, to: text, status: uint }],
    ja4:           text?,            // the fingerprint the fetcher believes it sent
    http_version:  text,             // "1.1" | "2" | "3"
  },

  response: {
    status:          uint,
    headers_digest:  bytes32,        // blake3 of canonical header serialisation
    headers_kept:    { text: text }, // the subset we publish, doc 11.5
    content_length:  uint,
    content_type:    text?,
  }?,

  body: {
    digest:      bytes32,            // blake3-256 of the raw body as received
    length:      uint,
    chunk_root:  bytes32,            // blake3 tree over 16 KiB leaves
    chunk_count: uint,
  }?,

  tls: {
    chain_digests: [bytes32],        // sha256 of each DER cert, leaf first
    sni:           text,
    alpn:          text,
    not_before_ms: uint,
    not_after_ms:  uint,
  }?,

  extract: {
    extractor:     text,             // "umi-extract/0.4.1" exact version
    digest:        bytes32,          // blake3 over the extraction, see below
    stability:     Stability,        // see 4.6
    link_count:    uint,
    text_bytes:    uint,
  }?,

  signature:     bytes64,            // ed25519 over canonical CBOR of all above
}

Outcome = "ok" | "not_modified" | "gone" | "not_found"
        | "server_error" | "rate_limited"
        | "blocked" | "challenge" | "timeout" | "dns_failure"
        | "tls_failure" | "too_large" | "redirected_off_host"
        | "robots_changed" | "tier_exhausted"
        | "connect_failure" | "malformed"
```

The receipt carries the string. Doc 10.5's `outcome` column carries a byte, and the mapping is fixed for good, because a segment written today has to be readable by a build from two years from now. Codes are appended and never renumbered, and a code that is withdrawn stays reserved rather than being reused.

```
0  ok                    6  blocked            12  redirected_off_host
1  not_modified          7  challenge          13  robots_changed
2  gone                  8  timeout            14  tier_exhausted
3  not_found             9  dns_failure        15  connect_failure
4  server_error         10  tls_failure        16  malformed
5  rate_limited         11  too_large
```

A reader that meets a code it does not know keeps the row and treats the outcome as unknown. Dropping it would mean an old `umi` silently reporting a smaller crawl than the one on disk, which is the failure mode this whole numbering exists to avoid.

`server_error` and `rate_limited` were missing from the first draft of this list, and their absence forced two very different situations into `blocked`. A 5xx is the origin having a bad minute and the correct response is to back off and retry with the priority unchanged. A 429 is the origin telling us our rate is wrong and the correct response is to widen the host delay in doc 07.3 and retry. `blocked` means the origin has decided about us specifically, and the correct response is doc 05's tier ladder. Collapsing three responses into one outcome would have made the host delay adapt to server load and the tier ladder escalate against a machine that was simply busy, so the enum splits them.

`connect_failure` and `malformed` were added for the same reason once the client was written. A name that does not resolve is a site that has gone, while a connection that is refused is usually a site that is down for the afternoon, and folding the second into `dns_failure` would have retired live hosts. `malformed` means the response was not something HTTP allows, a `Location` that does not parse or a redirect chain with no end, and it has to be separate from `server_error` because a broken intermediary is worth reporting to somebody and a busy origin is not.

Four things about this are load bearing.

**The nonce.** Without it a fetcher could cache a receipt and replay it. With it, every receipt is bound to one lease.

**The extract digest.** This said blake3 over the canonical CBOR of the extraction until the first implementation was written, and then it changed, because canonical CBOR is a serialisation format with choices left in it. RFC 8949 section 4.2 gives two map orderings, leaves float shortening to the encoder, and permits definite or indefinite length for the same value. Each of those is somewhere two honest implementations produce different bytes for the same data, and the failure is not a parse error that somebody notices, it is a community fetcher whose receipts never agree with anyone's and which loses reputation for a bug in a library it did not write.

So there is no encoder. The digest is blake3 over the domain separator `umi-extract-digest/1` followed by the extracted values in a fixed order, each one preceded by a tag byte saying which field it is and prefixed with its length as a little endian uint64. An absent optional field is a length of `0xFFFFFFFFFFFFFFFF`, so an absent description and an empty one differ. A repeated field is a tag, then a count, then the values. Nothing is sorted: headings and links are in document order, which doc 11 already fixes, and sorting them would hide a real disagreement between two extractors that found the same links in a different order.

The result is not a format anybody can decode, which is the point, because it is only ever compared. Writing a second implementation in another language is an afternoon and there is nothing in it to disagree about.

The extractor version is the first field, tagged, and that is deliberate. Doc 11.1 promises that the same input and the same version produce the same output, and promises nothing about two versions, so a digest that ignored the version would assert an agreement the spec does not make. Two fetchers running different extractor versions disagree, which is the honest answer, and doc 06.4 treats it as a disagreement rather than as a lie.

Fetch timing, the response headers and the fetcher id are all outside the digest. Two fetchers on different continents see different headers from the same CDN and take different amounts of time, and folding any of that in would make agreement impossible on purpose.

The frame signatures elsewhere in this document are still over canonical CBOR, and that is not the same problem. A signature is verified against the exact bytes that arrived, by the party that received them, so the sender's encoder choices do not have to match anybody else's. The extract digest is compared between two parties who never exchanged bytes, which is what makes it strict.

**The chunk tree.** The coordinator can ask for chunk 47 of a 3 MB document and verify it against `chunk_root` without transferring the whole body. Cheap partial audits are what make audits affordable at scale.

**The TLS chain digests.** If a fetcher is behind a MITM proxy, or is fabricating content entirely, the certificate chain will not match what the coordinator or another fetcher sees. This is the cheapest fabrication detector in the protocol and it costs the fetcher nothing to include.

**The exact extractor version.** Extraction digests are only comparable between fetchers running the same extractor build. A delivery whose extractor version we do not recognise is accepted, the payload is kept, but the extract digest is not used for cross fetcher comparison and the raw body is pulled for central re extraction. This is how the protocol survives version skew across a fleet we do not control.

## 4.6 The stability digest

Comparing two independent fetches of the same URL by raw digest almost always fails, because live pages carry timestamps, CSRF tokens, session ids, ad slots and rotating banners. A verification scheme built on exact equality would flag every honest fetcher.

So the receipt carries a stability structure designed to be compared across fetches:

```
Stability {
  title_digest:   bytes32,      // blake3 of normalised <title>
  link_set:       bytes32,      // blake3 of sorted, canonicalised outlink set
  minhash:        [uint32; 64], // over 5-shingles of normalised text
  text_len_bucket: uint,        // log2 bucket of text length
  lang:           text?,
}
```

Two fetches of the same URL agree when the title digests match, the estimated Jaccard from the MinHash signatures is at least 0.90, and the text length buckets are within one. The link set is compared separately and more strictly, at 0.95 Jaccard, because links steer the frontier and are the highest value thing to poison.

The thresholds are starting points and doc 06 says how they get tuned against measured disagreement between two honest fetchers on the same URL. That measurement has to happen at milestone 4 before the community fleet opens, because if honest disagreement is routinely worse than 0.90 the whole verification scheme needs different numbers.

## 4.7 The reference fetcher loop

What `umi fetch` actually does, so third party implementations have something to match.

```
hello() -> policy
loop {
  if in_flight < policy.granted_rate * lease_ttl:
     leases = lease(want = min(policy.max_lease_batch, capacity_now()))
  for lease in leases (respecting per host min_gap_ms and local concurrency):
     result = run_tier_ladder(lease)          // doc 05
     extraction = extract(result.body)         // doc 11, skipped on 304/error
     receipt = build_and_sign(lease, result, extraction)
     buffer.push(receipt, extraction, result.raw)
     ring.store(lease.lease_id, result.raw)    // for audits
  if buffer.len >= 64 or buffer.age > 5s:
     deliver(buffer)
  drain_audit_requests()
}
```

Delivery batches of 64 at 5 second flush is a compromise between coordinator round trips and the freshness target. At 3 pages per second a small volunteer flushes on the timer, not the count, so its results reach the corpus within about 5 seconds of being fetched.

## 4.8 Flow control and fairness

The coordinator controls the entire flow through `granted_rate` and `max_lease_batch`, recomputed on every hello and every hour thereafter.

```
granted_rate = min(
    fetcher.target_rate,
    reputation_curve(reputation) ,        // doc 06.5, 0.5 p/s at zero rep
    available_work_for_this_fetcher / active_fetchers_for_that_work
)
```

The last term prevents a large fetcher from starving the fleet when there is a queue of work for a small number of PLDs, which is exactly the situation during a focused crawl.

Backpressure runs the other way too. If the coordinator's disk is filling, or the publisher is behind, `granted_rate` drops fleet wide and the crawl slows. Doc 15 defines the thresholds. On 342 GB of free disk this is not a theoretical mechanism, it will fire.

## 4.9 What a fetcher must never do

Stated as protocol requirements because third party implementations will exist and they need a checklist.

A fetcher must not fetch a URL it was not leased. There is no discovery on the fetcher side.

A fetcher must not follow a redirect to a different registrable domain. Return `redirected_off_host`.

A fetcher must not exceed `politeness.min_gap_ms` against a host, including across concurrent leases for the same host.

A fetcher must not exceed `max_bytes`. Abort the transfer and return `too_large` with the bytes seen.

A fetcher must not run a tier above `max_tier`.

A fetcher must not modify the extraction after computing its digest, and must not sign a receipt whose body digest it did not compute from bytes it actually received.

A fetcher must not present a user agent other than the one the coordinator specifies for its tier. Doc 07 owns that string, and getting it wrong is what turns a legitimate crawler into a reported one.

## 4.10 What the protocol deliberately does not have

No streaming. Long polling is enough at these rates and it works through every proxy.

No push from coordinator to fetcher. Fetchers are behind NAT and always will be.

No payment, no token, no ledger. Doc 06 has reputation, which is enough to allocate work, and adding money would change the failure modes for the worse.

No fetcher to fetcher communication. The coordinator is the only party a fetcher talks to. Quorum comparison in doc 06 happens on the coordinator, and fetchers never learn that they are being compared with each other.

No versioning negotiation beyond the `protocol` integer. When the wire format changes, the integer changes, old fetchers get a clear error telling them to upgrade, and there is no compatibility shim. The fleet is a downloaded binary and upgrading it is easy.
