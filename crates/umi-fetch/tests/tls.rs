//! Gate 2.2's crash suite: both TLS stacks, one process, real handshakes.
//!
//! The failure this exists to catch does not look like a test failure. Two
//! libraries export the same symbol names, the linker picks one without
//! complaining, and the binary works until it segfaults inside TLS on somebody
//! else's machine. Issue #34 calls it the class of bug a volunteer fleet
//! reports as "it crashes sometimes" and nobody can reproduce.
//!
//! It is not hypothetical here. The default build already links `aws-lc-sys`,
//! which rustls uses and which is a BoringSSL fork, and the `emulation` build
//! adds `btls`, which is BoringSSL itself. Both export `SSL_new`, `EVP_*` and
//! several hundred more. `aws-lc-sys` renames its own; `btls` only renames its
//! own when the `prefix-symbols` feature is on, which is why this crate's
//! manifest turns it on for Linux targets. `scripts/check-symbols.sh` is the
//! static half of that and this file is the running half.
//!
//! So this file puts both clients in one process and makes them do full
//! handshakes against a TLS origin on loopback, at the same time, on every
//! platform CI builds for. A handshake is where the collision would land,
//! because that is where both libraries are doing key exchange and X.509
//! parsing at once.
//!
//! The origin's certificate is self signed and neither client trusts it, which
//! is the point rather than a shortcut. The handshake still runs all the way
//! through key exchange and certificate parsing before it is rejected, so the
//! code that would crash has run, and the expected answer is one specific
//! failure rather than "no panic", which is what makes this a test and not a
//! smoke check.

use std::net::SocketAddr;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use umi_fetch::{Failure, FetchConfig, Fetcher, Outcome};

/// Generated once with a hundred year expiry, so this is a fixture and not a
/// thing that starts failing on a Tuesday in 2027. CN and SAN are localhost
/// and 127.0.0.1, which would make it verify if anything trusted it.
const CERT: &[u8] = include_bytes!("tls/cert.der");
const KEY: &[u8] = include_bytes!("tls/key.der");

/// How many fetches run at once. Enough to have several handshakes overlapping
/// in both libraries, which is the state a symbol collision shows up in, and
/// small enough that the whole file runs in well under a second.
const CONCURRENCY: usize = 16;

/// A TLS origin on loopback that will never be trusted.
async fn origin() -> SocketAddr {
    // rustls 0.23 asks the process which crypto provider to use and panics if
    // more than one is compiled in and nothing has chosen. Ours is aws-lc-rs
    // because that is what reqwest pulls in. `ok()` because a second call is a
    // normal thing to happen and not an error.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let key = PrivateKeyDer::try_from(KEY.to_vec()).expect("the fixture key parses");
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![CertificateDer::from(CERT)], key)
        .expect("the fixture certificate and key go together");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("a bound port has an address");
    let acceptor = TlsAcceptor::from(Arc::new(config));

    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            // The server side of the handshake fails too, because the client
            // sends an alert once it has looked at the certificate. That is
            // the expected end of every connection here and there is nothing
            // to report about it.
            tokio::spawn(async move {
                let _ = acceptor.accept(socket).await;
            });
        }
    });

    addr
}

/// What either client should say about an origin it cannot verify.
fn assert_untrusted(outcome: &Outcome, who: &str) {
    match outcome {
        Outcome::Failed {
            failure: Failure::Tls,
            ..
        } => {}
        other => panic!("{who} should have refused an untrusted certificate, said {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn t1_survives_a_full_handshake_against_an_untrusted_origin() {
    let addr = origin().await;
    let url = format!("https://{addr}/");
    let fetcher = Fetcher::with_config(FetchConfig::default()).expect("the client builds");

    let mut work = Vec::new();
    for _ in 0..CONCURRENCY {
        let fetcher = fetcher.clone();
        let url = url.clone();
        work.push(tokio::spawn(async move { fetcher.fetch(&url, None).await }));
    }
    for handle in work {
        let outcome = handle
            .await
            .expect("no task panicked")
            .expect("a url we can crawl");
        assert_untrusted(&outcome, "t1");
    }
}

/// The one that is actually gate 2.2.
///
/// Both clients, interleaved, on the same runtime and the same origin. If the
/// symbols collided this is where the process would die, and it would die
/// rather than fail, so a green run is the whole result.
#[cfg(feature = "emulation")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_tls_stacks_survive_a_full_handshake_in_one_process() {
    use umi_fetch::Emulated;

    let addr = origin().await;
    let url = format!("https://{addr}/");
    let plain = Fetcher::with_config(FetchConfig::default()).expect("the rustls client builds");
    let browser = Emulated::with_config(FetchConfig::default()).expect("boringssl initialises");

    let mut work = Vec::new();
    for index in 0..CONCURRENCY {
        let url = url.clone();
        // Alternating rather than one batch then the other, so the two
        // libraries are inside a handshake at the same moment rather than one
        // after the other.
        if index % 2 == 0 {
            let plain = plain.clone();
            work.push(tokio::spawn(async move {
                ("t1", plain.fetch(&url, None).await)
            }));
        } else {
            let browser = browser.clone();
            work.push(tokio::spawn(async move {
                ("t2", browser.fetch(&url, None).await)
            }));
        }
    }
    for handle in work {
        let (who, result) = handle.await.expect("no task panicked");
        assert_untrusted(&result.expect("a url we can crawl"), who);
    }
}
