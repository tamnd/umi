//! Web Bot Auth request signing, from `docs/spec/07-politeness-and-identity.md`
//! section 7.2.
//!
//! A user agent string is a claim anyone can make, and the reason sites treat
//! crawlers badly is that they cannot tell one from a scraper wearing its name.
//! An Ed25519 signature over the request, checkable against a key we publish,
//! turns doc 07's politeness commitments into something an origin can verify
//! instead of something we assert. Cloudflare turned this on at their edge in
//! March 2026 and fronts roughly a fifth of the web, so it is also the
//! difference between being allowed by default on that fifth and being blocked
//! by default on it.
//!
//! The wire format is RFC 9421 HTTP Message Signatures with the profile that
//! `draft-meunier-webbotauth-httpsig-protocol` pins: Ed25519, a `web-bot-auth`
//! tag, a short expiry, a nonce, and a `Signature-Agent` header pointing at the
//! directory where the public key lives.
//!
//! # The three headers
//!
//! ```text
//! Signature-Agent: "https://umi.dev"
//! Signature-Input: sig1=("@authority" "@method" "@path" "signature-agent")\
//!                  ;created=1756400000;expires=1756400060\
//!                  ;keyid="...";alg="ed25519";nonce="...";tag="web-bot-auth"
//! Signature: sig1=:base64:
//! ```
//!
//! `Signature-Agent` is covered by the signature as well as sent, which is the
//! draft's rule and matters: an unsigned pointer to a key directory would let
//! anyone replay our signature while naming a directory they control.
//!
//! # No domain separation prefix here
//!
//! Every other signature in umi is over `context \0 payload`, which is what
//! stops the crawl identity key from being talked into signing a manifest.
//! This one cannot be, because the bytes an origin verifies are fixed by RFC
//! 9421 and a prefix we invented would make every signature we send fail. The
//! separation comes from the key instead: the key that signs requests is a
//! different key from the one that signs manifests, it is loaded from a
//! different place, and neither can be used for the other's job because the
//! public halves differ.
//!
//! # No clock and no random source
//!
//! [`Signer`] takes its clock from the caller, as a closure, for the reason the
//! crate documentation gives: a fetch has to be replayable and a test has to be
//! able to produce the same bytes twice. The nonce is a counter mixed into a
//! seed the caller supplies, so this module reads no entropy either. A nonce
//! only has to be unique and unguessable, and blake3 over a secret seed and a
//! counter is both without needing a random number generator in a crate that
//! deliberately has none.
//!
//! # What the verifier here is for
//!
//! Origins verify us with their own libraries. [`verify`] exists so that our
//! own tests check the bytes rather than checking themselves, so that key
//! rotation can be shown to keep old requests verifiable, and so that an
//! operator can confirm a deployment signs the way the published directory
//! says it does. It reads the profile this module emits plus any covered
//! header, and it is not a general RFC 8941 parser.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use ed25519_dalek::{
    Signature, Signer as _, SigningKey as DalekSigning, Verifier as _,
    VerifyingKey as DalekVerifying,
};
use http::HeaderMap;
use url::Url;

/// The signature tag the draft reserves for this use.
pub const TAG: &str = "web-bot-auth";

/// The only algorithm. The draft allows others and we do not send them.
pub const ALG: &str = "ed25519";

/// The signature label. One signature per request, so one label.
pub const LABEL: &str = "sig1";

/// Where a signature agent serves its keys, relative to the agent URL.
pub const DIRECTORY_PATH: &str = "/.well-known/http-message-signatures-directory";

/// The media type the directory is served with.
pub const DIRECTORY_MEDIA_TYPE: &str = "application/http-message-signatures-directory+json";

/// The signature agent umi presents, which is doc 07.1's host.
pub const AGENT: &str = "https://umi.dev";

/// The components every umi request covers.
///
/// `@authority` is the minimum Cloudflare's guidance asks for, because it is
/// what stops a signature collected on one host from being replayed at
/// another. `@method` and `@path` narrow it to the exact request. There is no
/// `@query` on purpose: a signature that covered the query would still be
/// valid for the same path with the query removed, so covering it buys nothing
/// and costs a base line on every request.
pub const COVERED: [&str; 4] = ["@authority", "@method", "@path", "signature-agent"];

/// How long a signature is good for, in seconds.
///
/// Short, because the only thing this window protects is the gap between us
/// putting a request on the wire and an origin reading it, and a long window is
/// a replay window. Sixty seconds is well inside the draft's ceiling and leaves
/// room for a clock that is half a minute out. A fleet whose clock is further
/// out than that has a bigger problem, and `umi doctor` says so.
pub const LIFETIME_SECS: u64 = 60;

/// What went wrong signing or verifying.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SignatureError {
    /// The seed was not 32 bytes, or the agent was not an http(s) URL.
    #[error("web bot auth: {0}")]
    Setup(&'static str),

    /// A request had no signature on it, or not one of ours.
    #[error("web bot auth: {0}")]
    Missing(&'static str),

    /// A header was there and did not parse the way the draft says it should.
    #[error("web bot auth: malformed {0}")]
    Malformed(&'static str),

    /// The signature named a key the directory does not hold.
    #[error("web bot auth: no key {0} in the directory")]
    UnknownKey(String),

    /// The signature was made before the key was valid, or after it stopped
    /// being. Separate from an expired signature because the operator fix is
    /// different: this one means the directory and the deployment disagree.
    #[error("web bot auth: the key was not in use at that time")]
    KeyWindow,

    /// The signature had expired, or was created in the future.
    #[error("web bot auth: the signature is not valid at this time")]
    Expired,

    /// The bytes did not verify under the key the signature named.
    #[error("web bot auth: the signature did not verify")]
    BadSignature,

    /// The directory was not the JSON the draft describes.
    #[error("web bot auth: directory: {0}")]
    Directory(&'static str),
}

type Result<T> = std::result::Result<T, SignatureError>;

/// One public key, as the directory publishes it.
///
/// This is a JSON Web Key with the members RFC 8037 gives an Ed25519 key, plus
/// the two optional validity fields the draft allows. Nothing here is umi
/// specific, which is the point: a verifier reaches for a JWK library and it
/// works.
#[derive(Clone, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub struct Jwk {
    /// `OKP`, an octet key pair.
    pub kty: String,
    /// `Ed25519`.
    pub crv: String,
    /// The thumbprint, which is what a signature's `keyid` names.
    pub kid: String,
    /// The public key, base64url with no padding.
    pub x: String,
    /// Not valid before, in seconds. Absent means valid from the beginning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nbf: Option<u64>,
    /// Not valid after, in seconds. Absent means still in use.
    ///
    /// A rotated key keeps its entry and gains an `exp`, rather than being
    /// deleted. That is what makes doc 07.2's overlap window work: a request
    /// signed last quarter still names a key a verifier can find, and still
    /// verifies, while a request signed today under the same key does not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exp: Option<u64>,
}

impl Jwk {
    /// Build the entry for a public key.
    #[must_use]
    pub fn new(key: &DalekVerifying, nbf: Option<u64>, exp: Option<u64>) -> Self {
        let x = URL_SAFE_NO_PAD.encode(key.to_bytes());
        Self {
            kid: thumbprint(&x),
            kty: "OKP".to_owned(),
            crv: "Ed25519".to_owned(),
            x,
            nbf,
            exp,
        }
    }

    /// The key itself, or an error when the entry does not hold one.
    ///
    /// # Errors
    ///
    /// [`SignatureError::Directory`] when the type or the curve is not the one
    /// the draft allows, or the key material is not 32 bytes of base64url.
    pub fn key(&self) -> Result<DalekVerifying> {
        if self.kty != "OKP" || self.crv != "Ed25519" {
            return Err(SignatureError::Directory("not an ed25519 key"));
        }
        let raw = URL_SAFE_NO_PAD
            .decode(&self.x)
            .map_err(|_| SignatureError::Directory("the key is not base64url"))?;
        let bytes: [u8; 32] = raw
            .try_into()
            .map_err(|_| SignatureError::Directory("the key is not 32 bytes"))?;
        DalekVerifying::from_bytes(&bytes)
            .map_err(|_| SignatureError::Directory("the key is not on the curve"))
    }

    /// Whether a signature created at `secs` was made inside this key's window.
    #[must_use]
    pub fn in_use_at(&self, secs: u64) -> bool {
        self.nbf.is_none_or(|nbf| secs >= nbf) && self.exp.is_none_or(|exp| secs < exp)
    }
}

/// The RFC 7638 thumbprint of an Ed25519 public key, which is its `kid`.
///
/// The input is the canonical JSON RFC 7638 specifies: the required members
/// only, in lexicographic order, no whitespace. It is spelled out here rather
/// than produced by a serialiser because the point of a thumbprint is that two
/// implementations get the same string, and a serialiser that reorders members
/// or adds a space would quietly break that.
fn thumbprint(x: &str) -> String {
    use sha2::Digest as _;
    let canonical = format!(r#"{{"crv":"Ed25519","kty":"OKP","x":"{x}"}}"#);
    let digest = sha2::Sha256::digest(canonical.as_bytes());
    URL_SAFE_NO_PAD.encode(digest)
}

/// The published key set, as served at [`DIRECTORY_PATH`].
#[derive(Clone, PartialEq, Eq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Directory {
    /// Every key ever used, current and retired.
    pub keys: Vec<Jwk>,
}

impl Directory {
    /// A directory holding one key with no validity window.
    #[must_use]
    pub fn of(key: &DalekVerifying) -> Self {
        Self {
            keys: vec![Jwk::new(key, None, None)],
        }
    }

    /// The entry a `keyid` names.
    #[must_use]
    pub fn find(&self, kid: &str) -> Option<&Jwk> {
        self.keys.iter().find(|entry| entry.kid == kid)
    }

    /// The bytes to serve, pretty printed with a trailing newline.
    ///
    /// Pretty rather than compact because this file is read by people at least
    /// as often as by programs, and it is a few hundred bytes served once a
    /// day per verifier.
    ///
    /// # Errors
    ///
    /// Never in practice. The type is fixed and holds only strings and
    /// numbers, and the error exists because `serde_json` returns one.
    pub fn to_json(&self) -> Result<String> {
        let mut out = serde_json::to_string_pretty(self)
            .map_err(|_| SignatureError::Directory("would not serialise"))?;
        out.push('\n');
        Ok(out)
    }

    /// Read a directory somebody else served.
    ///
    /// # Errors
    ///
    /// [`SignatureError::Directory`] when the bytes are not the JSON the draft
    /// describes.
    pub fn parse(text: &str) -> Result<Self> {
        serde_json::from_str(text).map_err(|_| SignatureError::Directory("is not a key set"))
    }
}

/// The signature parameters, which are both sent and covered.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Params {
    /// When the signature was made, in seconds.
    pub created: u64,
    /// When it stops being valid, in seconds.
    pub expires: u64,
    /// The thumbprint of the key that made it.
    pub keyid: String,
    /// The algorithm, always [`ALG`] on the way out.
    pub alg: String,
    /// Unique per request, so a captured signature cannot be replayed inside
    /// its window.
    pub nonce: String,
    /// [`TAG`], which is how an origin knows what kind of signature this is
    /// without parsing the rest.
    pub tag: String,
}

impl Params {
    /// Serialise the parameter list, which is the tail of `Signature-Input`
    /// and the last line of the signature base.
    ///
    /// Order is fixed. RFC 8941 does not require a particular one, but the
    /// verifier reconstructs the base from what it parsed rather than from
    /// this, so the only thing the order has to be is stable.
    fn to_sf(&self, covered: &[String]) -> String {
        let names = covered
            .iter()
            .map(|name| format!("\"{name}\""))
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "({names});created={};expires={};keyid=\"{}\";alg=\"{}\";nonce=\"{}\";tag=\"{}\"",
            self.created, self.expires, self.keyid, self.alg, self.nonce, self.tag
        )
    }
}

/// The three headers, ready to put on a request.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Signed {
    /// `Signature-Agent`, an RFC 8941 string, quotes included.
    pub agent: String,
    /// `Signature-Input`.
    pub input: String,
    /// `Signature`.
    pub signature: String,
}

impl Signed {
    /// The headers in the order they should be added, name and value.
    #[must_use]
    pub fn headers(&self) -> [(&'static str, &str); 3] {
        [
            ("signature-agent", self.agent.as_str()),
            ("signature-input", self.input.as_str()),
            ("signature", self.signature.as_str()),
        ]
    }
}

/// The private half, and everything needed to use it.
///
/// One per process. Cloning would duplicate the nonce counter, which is the one
/// thing here that must not repeat, so it deliberately does not implement
/// `Clone`. Share it behind an `Arc`.
pub struct Signer {
    key: DalekSigning,
    keyid: String,
    agent: String,
    nonce_seed: [u8; 16],
    counter: AtomicU64,
    now: Box<dyn Fn() -> u64 + Send + Sync>,
}

// By hand, because the clock closure is not `Debug` and because the private key
// must never reach a log line through a derive somebody added later.
impl fmt::Debug for Signer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Signer")
            .field("keyid", &self.keyid)
            .field("agent", &self.agent)
            .finish_non_exhaustive()
    }
}

impl Signer {
    /// Build a signer from a 32 byte seed.
    ///
    /// `agent` is the origin that serves the key directory, without the
    /// well known path on it. `nonce_seed` is 16 bytes the caller produces
    /// once at startup; it never leaves the process and it is not a signing
    /// key, it only has to be unpredictable. `now` returns unix seconds.
    ///
    /// # Errors
    ///
    /// [`SignatureError::Setup`] when the agent is not an absolute http or
    /// https URL.
    pub fn new(
        seed: [u8; 32],
        agent: &str,
        nonce_seed: [u8; 16],
        now: Box<dyn Fn() -> u64 + Send + Sync>,
    ) -> Result<Self> {
        let parsed =
            Url::parse(agent).map_err(|_| SignatureError::Setup("the agent is not a url"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(SignatureError::Setup("the agent is not http or https"));
        }
        let key = DalekSigning::from_bytes(&seed);
        let keyid = Jwk::new(&key.verifying_key(), None, None).kid;
        Ok(Self {
            key,
            keyid,
            agent: agent.trim_end_matches('/').to_owned(),
            nonce_seed,
            counter: AtomicU64::new(0),
            now,
        })
    }

    /// A signer with a clock that does not move, for tests and for producing
    /// known answers.
    ///
    /// # Errors
    ///
    /// The same as [`Signer::new`].
    pub fn fixed(seed: [u8; 32], agent: &str, nonce_seed: [u8; 16], secs: u64) -> Result<Self> {
        Self::new(seed, agent, nonce_seed, Box::new(move || secs))
    }

    /// The thumbprint of the key this signer holds, which is what shows up in
    /// `keyid` and in the directory.
    #[must_use]
    pub fn keyid(&self) -> &str {
        &self.keyid
    }

    /// The origin the `Signature-Agent` header points at.
    #[must_use]
    pub fn agent(&self) -> &str {
        &self.agent
    }

    /// Where the directory for this signer's key is served.
    #[must_use]
    pub fn directory_url(&self) -> String {
        format!("{}{DIRECTORY_PATH}", self.agent)
    }

    /// The directory entry for this signer's key.
    #[must_use]
    pub fn jwk(&self, nbf: Option<u64>, exp: Option<u64>) -> Jwk {
        Jwk::new(&self.key.verifying_key(), nbf, exp)
    }

    /// Sign a request, reading the clock the caller supplied.
    ///
    /// # Errors
    ///
    /// [`SignatureError::Setup`] when the url has no host, which the engine
    /// has already rejected by the time a real fetch gets here.
    pub fn sign(&self, method: &str, url: &Url) -> Result<Signed> {
        self.sign_at(method, url, (self.now)())
    }

    /// Sign a request as if it were being made at `created`.
    ///
    /// The nonce still moves, because two signatures made at the same second
    /// are the normal case at rate and reusing a nonce is the one thing a
    /// replay check looks for.
    ///
    /// # Errors
    ///
    /// The same as [`Signer::sign`].
    pub fn sign_at(&self, method: &str, url: &Url, created: u64) -> Result<Signed> {
        let agent = format!("\"{}\"", self.agent);
        let params = Params {
            created,
            expires: created + LIFETIME_SECS,
            keyid: self.keyid.clone(),
            alg: ALG.to_owned(),
            nonce: self.nonce(),
            tag: TAG.to_owned(),
        };
        let covered: Vec<String> = COVERED.iter().map(|name| (*name).to_owned()).collect();

        let mut fields = HeaderMap::new();
        let value = agent
            .parse()
            .map_err(|_| SignatureError::Setup("the agent is not a header value"))?;
        fields.insert("signature-agent", value);

        let base = signature_base(method, url, &covered, &params, &fields)?;
        let signature = self.key.sign(base.as_bytes());

        Ok(Signed {
            agent,
            input: format!("{LABEL}={}", params.to_sf(&covered)),
            signature: format!("{LABEL}=:{}:", STANDARD.encode(signature.to_bytes())),
        })
    }

    /// The next nonce.
    ///
    /// blake3 over the seed and a counter. The counter makes it unique for the
    /// life of the process and the seed makes it unguessable from outside, and
    /// neither needs a random number generator in a crate that has none. A
    /// restart reuses counter values, which is why the seed is per process
    /// rather than a constant.
    fn nonce(&self) -> String {
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.nonce_seed);
        hasher.update(&n.to_be_bytes());
        URL_SAFE_NO_PAD.encode(&hasher.finalize().as_bytes()[..16])
    }
}

/// The authority of a request target, per RFC 9421 section 2.2.3.
///
/// Host lowercased, port only when it is not the scheme's default. `url` has
/// already done both: it lowercases hosts when it parses and `Url::port`
/// returns `None` for a default port.
fn authority(url: &Url) -> Result<String> {
    let host = url
        .host_str()
        .ok_or(SignatureError::Setup("the url has no host"))?;
    Ok(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    })
}

/// Build the bytes that get signed, per RFC 9421 section 2.5.
///
/// One line per covered component, `"name": value`, separated by newlines, and
/// the parameter list last with no newline after it. A derived component starts
/// with `@` and is computed from the request; anything else is a header read
/// out of `fields`.
///
/// # Errors
///
/// [`SignatureError::Setup`] when the url has no host, and
/// [`SignatureError::Missing`] when a covered header is not on the request,
/// which is a signature that could never verify.
pub fn signature_base(
    method: &str,
    url: &Url,
    covered: &[String],
    params: &Params,
    fields: &HeaderMap,
) -> Result<String> {
    let mut lines = Vec::with_capacity(covered.len() + 1);
    for name in covered {
        let value = match name.as_str() {
            "@authority" => authority(url)?,
            "@method" => method.to_ascii_uppercase(),
            "@path" => {
                let path = url.path();
                if path.is_empty() {
                    "/".to_owned()
                } else {
                    path.to_owned()
                }
            }
            "@query" => match url.query() {
                Some(query) => format!("?{query}"),
                None => "?".to_owned(),
            },
            "@target-uri" => url.as_str().to_owned(),
            derived if derived.starts_with('@') => {
                return Err(SignatureError::Missing("an unsupported derived component"));
            }
            header => {
                let mut values = fields
                    .get_all(header)
                    .iter()
                    .filter_map(|value| value.to_str().ok())
                    .peekable();
                if values.peek().is_none() {
                    return Err(SignatureError::Missing("a covered header is not present"));
                }
                values.map(str::trim).collect::<Vec<_>>().join(", ")
            }
        };
        lines.push(format!("\"{name}\": {value}"));
    }
    lines.push(format!("\"@signature-params\": {}", params.to_sf(covered)));
    Ok(lines.join("\n"))
}

/// What a verified request turned out to be.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Verified {
    /// The thumbprint of the key that signed it.
    pub keyid: String,
    /// The agent URL the request named, without the quotes.
    pub agent: String,
    /// When the signature was made.
    pub created: u64,
    /// The nonce, so a caller keeping a replay window can record it.
    pub nonce: String,
}

/// Check a signed request against a published directory.
///
/// The order of the checks is deliberate. Everything that can be decided from
/// the request alone happens before the key lookup, and the key lookup happens
/// before the Ed25519 verification, so the expensive step only runs on a
/// request that is otherwise well formed. The failure cases are told apart in
/// the error type because an operator debugging a deployment needs to know
/// which one it was, and none of the distinctions help an attacker: a forger
/// cannot produce the signature whatever the error says.
///
/// # Errors
///
/// One of [`SignatureError`]'s variants, naming the check that failed.
pub fn verify(
    method: &str,
    url: &Url,
    headers: &HeaderMap,
    directory: &Directory,
    now: u64,
) -> Result<Verified> {
    let input = header(headers, "signature-input")
        .ok_or(SignatureError::Missing("there is no signature-input"))?;
    let raw =
        header(headers, "signature").ok_or(SignatureError::Missing("there is no signature"))?;

    let (label, rest) = input
        .split_once('=')
        .ok_or(SignatureError::Malformed("signature-input"))?;
    let (covered, params) = parse_input(rest)?;

    if params.tag != TAG {
        return Err(SignatureError::Missing("not a web bot auth signature"));
    }
    if params.alg != ALG {
        return Err(SignatureError::Malformed("signature-input"));
    }
    if now < params.created || now >= params.expires {
        return Err(SignatureError::Expired);
    }

    let entry = directory
        .find(&params.keyid)
        .ok_or_else(|| SignatureError::UnknownKey(params.keyid.clone()))?;
    if !entry.in_use_at(params.created) {
        return Err(SignatureError::KeyWindow);
    }

    let base = signature_base(method, url, &covered, &params, headers)?;
    let bytes = parse_signature(&raw, label)?;
    entry
        .key()?
        .verify(base.as_bytes(), &Signature::from_bytes(&bytes))
        .map_err(|_| SignatureError::BadSignature)?;

    let agent = header(headers, "signature-agent")
        .map(|value| value.trim_matches('"').to_owned())
        .unwrap_or_default();

    Ok(Verified {
        keyid: params.keyid,
        agent,
        created: params.created,
        nonce: params.nonce,
    })
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)?
        .to_str()
        .ok()
        .map(|value| value.trim().to_owned())
}

/// Read the component list and the parameters out of a `Signature-Input`.
///
/// This reads the shape the draft pins rather than all of RFC 8941, which is
/// stated in the module documentation and is the honest description of it.
/// Quoted strings here never contain an escape, because every value that goes
/// in one is a base64url thumbprint, a nonce, a lowercase token or a URL.
fn parse_input(rest: &str) -> Result<(Vec<String>, Params)> {
    let malformed = || SignatureError::Malformed("signature-input");
    let open = rest.find('(').ok_or_else(malformed)?;
    let close = rest.find(')').ok_or_else(malformed)?;
    if close < open {
        return Err(malformed());
    }
    let covered: Vec<String> = rest[open + 1..close]
        .split_whitespace()
        .map(|name| name.trim_matches('"').to_owned())
        .collect();
    if covered.is_empty() {
        return Err(malformed());
    }

    let mut created = None;
    let mut expires = None;
    let mut keyid = None;
    let mut alg = None;
    let mut nonce = None;
    let mut tag = None;
    for part in rest[close + 1..].split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (name, value) = part.split_once('=').ok_or_else(malformed)?;
        let value = value.trim_matches('"');
        match name.trim() {
            "created" => created = value.parse().ok(),
            "expires" => expires = value.parse().ok(),
            "keyid" => keyid = Some(value.to_owned()),
            "alg" => alg = Some(value.to_owned()),
            "nonce" => nonce = Some(value.to_owned()),
            "tag" => tag = Some(value.to_owned()),
            // An unknown parameter is not an error. It is still covered,
            // because the base is rebuilt from the parameters that were parsed
            // and an unknown one would change the bytes, so a request carrying
            // one simply fails to verify rather than being rejected here.
            _ => {}
        }
    }

    Ok((
        covered,
        Params {
            created: created.ok_or_else(malformed)?,
            expires: expires.ok_or_else(malformed)?,
            keyid: keyid.ok_or_else(malformed)?,
            alg: alg.ok_or_else(malformed)?,
            nonce: nonce.ok_or_else(malformed)?,
            tag: tag.ok_or_else(malformed)?,
        },
    ))
}

/// Pull the 64 signature bytes out of `label=:base64:`.
fn parse_signature(raw: &str, label: &str) -> Result<[u8; 64]> {
    let malformed = || SignatureError::Malformed("signature");
    let (found, rest) = raw.split_once('=').ok_or_else(malformed)?;
    if found.trim() != label {
        return Err(SignatureError::Missing("the signature has another label"));
    }
    let encoded = rest
        .trim()
        .strip_prefix(':')
        .and_then(|rest| rest.strip_suffix(':'))
        .ok_or_else(malformed)?;
    STANDARD
        .decode(encoded)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(malformed)
}
