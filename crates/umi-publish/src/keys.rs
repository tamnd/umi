//! The three signing keys, from `docs/spec/12-publishing.md` section 12.5.
//!
//! Doc 12.5 says three keys with three purposes, rotated on different
//! schedules, and none of them able to do another's job. Keeping them in three
//! files is what makes the first half true. The second half needs more than
//! filing, because Ed25519 does not know what it is signing: hand the crawl
//! identity key a manifest and it will sign it perfectly well, and a fetcher
//! that could be talked into signing a manifest shaped receipt would be a hole.
//!
//! So every signature here is over a domain separated message, `context byte
//! string` then a zero byte then the payload. A signature made under one role
//! cannot verify under another, whatever the operator did with the files. This
//! is an addition to doc 12.5 rather than something it asks for, and doc 12.5
//! needs the edit to say so, because the prefix is part of the wire format that
//! a third party verifying our corpus has to reproduce.
//!
//! Keys are loaded from `env:NAME` or `file:PATH` and never from a literal in
//! config, which is the rule doc 15 sets for every secret in the system.

use std::path::Path;

use ed25519_dalek::{Signer as _, SigningKey as DalekSigning, VerifyingKey as DalekVerifying};

use crate::{Error, Result};

/// What a key is allowed to sign.
///
/// The context string is versioned along with the thing it separates, so that
/// a future manifest format can be introduced without a verifier accepting a
/// version 1 signature on a version 2 document.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Role {
    /// Doc 12.5's publishing key, which signs manifests.
    Publishing,
    /// Doc 07.2's crawl identity key, which is what a host proves itself with.
    CrawlIdentity,
    /// Doc 04's coordinator key, which signs leases.
    Lease,
}

impl Role {
    /// The domain separation context. Never reused and never reordered.
    #[must_use]
    pub const fn context(self) -> &'static [u8] {
        match self {
            Self::Publishing => b"umi/publishing/1",
            Self::CrawlIdentity => b"umi/crawl-identity/1",
            Self::Lease => b"umi/lease/1",
        }
    }

    /// What actually gets signed: the context, a zero byte, then the payload.
    ///
    /// The zero byte matters. Without it a context of `umi/lease/1` followed by
    /// a payload starting `23` would be the same bytes as a context of
    /// `umi/lease/12` followed by `3`, and the separation would only hold as
    /// long as no two contexts were prefixes of each other. That is the kind of
    /// invariant that holds until somebody adds a fourth role.
    #[must_use]
    pub fn message(self, payload: &[u8]) -> Vec<u8> {
        let context = self.context();
        let mut out = Vec::with_capacity(context.len() + 1 + payload.len());
        out.extend_from_slice(context);
        out.push(0);
        out.extend_from_slice(payload);
        out
    }
}

/// A private key, bound to one role.
///
/// The role is part of the type rather than an argument to `sign`, so that the
/// place a key is loaded is the place its purpose is decided, and no call site
/// downstream can pick a different one.
pub struct SigningKey {
    role: Role,
    inner: DalekSigning,
}

impl SigningKey {
    /// Wrap 32 bytes of seed.
    #[must_use]
    pub fn from_seed(role: Role, seed: [u8; 32]) -> Self {
        Self {
            role,
            inner: DalekSigning::from_bytes(&seed),
        }
    }

    /// Load a key from `env:NAME` or `file:PATH`.
    ///
    /// Hex or base64 would both work and hex is what this takes, 64 characters,
    /// optionally with trailing whitespace because a file written by `echo` has
    /// a newline on the end and refusing that helps nobody.
    ///
    /// # Errors
    ///
    /// [`Error::Secret`] when the source has no `env:` or `file:` prefix, when
    /// the variable or file is missing, or when the contents are not 32 bytes
    /// of hex. The message never contains the value.
    pub fn load(role: Role, source: &str) -> Result<Self> {
        let text = if let Some(name) = source.strip_prefix("env:") {
            std::env::var(name).map_err(|_| Error::Secret("the environment variable is not set"))?
        } else if let Some(path) = source.strip_prefix("file:") {
            std::fs::read_to_string(Path::new(path))
                .map_err(|_| Error::Secret("the key file could not be read"))?
        } else {
            // Deliberately not "or a literal". Doc 15 says secrets are never
            // literal in config, and the error is where somebody finds that
            // out.
            return Err(Error::Secret("expected env:NAME or file:PATH"));
        };
        let mut seed = [0u8; 32];
        hex::decode_to_slice(text.trim(), &mut seed)
            .map_err(|_| Error::Secret("expected 64 hex characters"))?;
        Ok(Self::from_seed(role, seed))
    }

    /// The public half.
    #[must_use]
    pub fn verifying(&self) -> VerifyingKey {
        VerifyingKey {
            role: self.role,
            inner: self.inner.verifying_key(),
        }
    }

    /// What this key is for.
    #[must_use]
    pub const fn role(&self) -> Role {
        self.role
    }

    /// Sign a payload under this key's role.
    #[must_use]
    pub fn sign(&self, payload: &[u8]) -> [u8; 64] {
        self.inner.sign(&self.role.message(payload)).to_bytes()
    }
}

impl core::fmt::Debug for SigningKey {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // No key material, not even truncated. A debug line ends up in a log
        // and a log ends up somewhere it was not meant to.
        write!(f, "SigningKey({:?})", self.role)
    }
}

/// A public key, bound to one role.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct VerifyingKey {
    role: Role,
    inner: DalekVerifying,
}

impl VerifyingKey {
    /// Wrap 32 bytes of public key.
    ///
    /// # Errors
    ///
    /// [`Error::Key`] when the bytes are not a point on the curve.
    pub fn from_bytes(role: Role, bytes: [u8; 32]) -> Result<Self> {
        let inner = DalekVerifying::from_bytes(&bytes).map_err(|_| Error::Key)?;
        Ok(Self { role, inner })
    }

    /// Parse the 64 hex characters that `umi-meta` publishes.
    ///
    /// # Errors
    ///
    /// [`Error::Key`] for anything that is not 32 bytes of hex on the curve.
    pub fn parse(role: Role, hex_text: &str) -> Result<Self> {
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(hex_text.trim(), &mut bytes).map_err(|_| Error::Key)?;
        Self::from_bytes(role, bytes)
    }

    /// The 32 raw bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; 32] {
        self.inner.to_bytes()
    }

    /// The hex form that goes in `umi-meta`.
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex::encode(self.to_bytes())
    }

    /// What this key is for.
    #[must_use]
    pub const fn role(&self) -> Role {
        self.role
    }

    /// Check a signature over a payload.
    ///
    /// Strict verification, which rejects the small order public keys under
    /// which one signature verifies for more than one key. For a corpus whose
    /// whole claim is "you can check this without trusting us", the permissive
    /// form would leave a way to produce two keys that both validate the same
    /// manifest.
    ///
    /// # Errors
    ///
    /// [`Error::BadSignature`] and nothing else. There is no distinction here
    /// between a malformed signature and a wrong one, because a caller that
    /// treated them differently would be building an oracle.
    pub fn verify(&self, payload: &[u8], signature: &[u8; 64]) -> Result<()> {
        let signature = ed25519_dalek::Signature::from_bytes(signature);
        self.inner
            .verify_strict(&self.role.message(payload), &signature)
            .map_err(|_| Error::BadSignature)
    }
}

#[cfg(test)]
mod tests {
    use super::{Role, SigningKey, VerifyingKey};
    use crate::Error;

    fn key(role: Role) -> SigningKey {
        SigningKey::from_seed(role, [42u8; 32])
    }

    #[test]
    fn a_signature_round_trips() {
        let signing = key(Role::Publishing);
        let payload = b"a manifest, or near enough";
        let sig = signing.sign(payload);
        signing.verifying().verify(payload, &sig).expect("verify");
    }

    #[test]
    fn a_key_cannot_do_another_roles_job() {
        // The point of the whole module. Same seed, same curve point, same
        // payload, and the signature still does not carry across.
        let payload = b"pay the bearer";
        let sig = key(Role::Lease).sign(payload);
        let publishing = key(Role::Publishing).verifying();
        assert!(matches!(
            publishing.verify(payload, &sig),
            Err(Error::BadSignature)
        ));
    }

    #[test]
    fn no_two_contexts_are_a_prefix_of_another_once_the_zero_byte_is_there() {
        // Belt and braces: the zero byte makes this hold for any context, and
        // this checks the contexts we actually have are distinct anyway.
        let roles = [Role::Publishing, Role::CrawlIdentity, Role::Lease];
        for (i, a) in roles.iter().enumerate() {
            for b in &roles[i + 1..] {
                assert_ne!(a.context(), b.context());
                assert_ne!(a.message(b"x"), b.message(b"x"));
            }
        }
    }

    #[test]
    fn a_flipped_bit_anywhere_fails() {
        let signing = key(Role::Publishing);
        let payload = b"the fourth of july";
        let good = signing.sign(payload);
        let verifying = signing.verifying();
        for bit in [0usize, 7, 100, 255, 511] {
            let mut bad = good;
            bad[bit / 8] ^= 1 << (bit % 8);
            assert!(verifying.verify(payload, &bad).is_err(), "bit {bit}");
        }
        let mut other = payload.to_vec();
        other[0] ^= 1;
        assert!(verifying.verify(&other, &good).is_err());
    }

    #[test]
    fn the_public_key_survives_hex() {
        let verifying = key(Role::Publishing).verifying();
        let text = verifying.to_hex();
        assert_eq!(text.len(), 64);
        assert_eq!(
            VerifyingKey::parse(Role::Publishing, &text).expect("parse"),
            verifying
        );
    }

    #[test]
    fn a_key_source_must_say_where_it_came_from() {
        // A literal is the mistake this is here to catch, and the error should
        // say what to do instead rather than just "invalid".
        let err = SigningKey::load(Role::Publishing, &"aa".repeat(32)).expect_err("literal");
        assert!(matches!(err, Error::Secret(msg) if msg.contains("env:")));
        assert!(SigningKey::load(Role::Publishing, "file:/no/such/key").is_err());
        assert!(SigningKey::load(Role::Publishing, "env:UMI_NO_SUCH_KEY_HERE").is_err());
    }

    #[test]
    fn a_key_file_with_a_trailing_newline_still_loads() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("publish.key");
        std::fs::write(&path, format!("{}\n", "2a".repeat(32))).expect("write");
        let loaded =
            SigningKey::load(Role::Publishing, &format!("file:{}", path.display())).expect("load");
        assert_eq!(
            loaded.verifying().to_hex(),
            key(Role::Publishing).verifying().to_hex()
        );
    }

    #[test]
    fn the_debug_form_carries_no_key_material() {
        let signing = key(Role::Publishing);
        let shown = format!("{signing:?}");
        assert!(!shown.contains("2a"), "{shown}");
        assert!(shown.contains("Publishing"), "{shown}");
    }
}
