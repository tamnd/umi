//! T1's socket: reqwest over rustls.
//!
//! Everything interesting about a response happens in [`crate::engine`]. What
//! is left here is the four things that are genuinely reqwest's: building the
//! client, putting a GET on the wire with our own headers, reading the three
//! fields of the head, and turning a reqwest error into a [`Failure`].

use std::sync::Arc;

use futures_util::StreamExt;
use url::Url;

use crate::engine::{BodyStream, Head, Transport, conditional, failure_from_text};
use crate::outcome::{Failure, Stage, Version};
use crate::webbotauth::Signer;
use crate::{ACCEPT, FetchConfig, FetchError, Result, Revalidator, USER_AGENT};

/// The plain client, honest identity and all.
#[derive(Debug)]
pub(crate) struct Plain {
    client: reqwest::Client,
    signer: Option<Arc<Signer>>,
}

impl Plain {
    /// Build the client doc 05.4 describes.
    ///
    /// # Errors
    ///
    /// [`FetchError::Client`] when the TLS backend will not initialise, which
    /// in practice means the platform has no usable certificate store.
    pub(crate) fn build(config: &FetchConfig, signer: Option<Arc<Signer>>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(USER_AGENT)
            .connect_timeout(config.connect_timeout)
            // Not the reqwest default, which is getaddrinfo on a blocking
            // pool and does not answer faster when more of the fetch window
            // asks at once. See [`crate::resolver`].
            .dns_resolver(crate::resolver::Resolver::shared())
            // Redirects are followed by hand, in the engine, because doc 04.7
            // stops at the first one that leaves the registrable domain and a
            // policy closure cannot report which URL it stopped at.
            .redirect(reqwest::redirect::Policy::none())
            .pool_max_idle_per_host(config.per_host)
            .https_only(false)
            .build()
            .map_err(|e| FetchError::Client(e.to_string()))?;

        Ok(Self { client, signer })
    }
}

impl Transport for Plain {
    type Response = reqwest::Response;

    async fn send(
        &self,
        url: &Url,
        revalidate: Option<&Revalidator>,
    ) -> std::result::Result<Self::Response, Failure> {
        let mut request = self
            .client
            .get(url.clone())
            .header(http::header::ACCEPT, ACCEPT);
        for (name, value) in conditional(revalidate) {
            request = request.header(name, value);
        }
        // A signing failure is not a reason to skip the fetch. The only way it
        // can happen is a url with no host, which the engine has already
        // rejected, and a fetcher that stopped crawling because a signature
        // would not build would be worse than one that crawls unsigned.
        if let Some(signer) = &self.signer
            && let Ok(signed) = signer.sign("GET", url)
        {
            for (name, value) in signed.headers() {
                request = request.header(name, value);
            }
        }
        request.send().await.map_err(transport_failure)
    }

    fn head(response: &Self::Response) -> Head {
        Head {
            status: response.status().as_u16(),
            version: Version::from(response.version()),
            headers: response.headers().clone(),
        }
    }

    fn body(response: Self::Response) -> BodyStream {
        Box::pin(
            response
                .bytes_stream()
                .map(|chunk| chunk.map_err(transport_failure)),
        )
    }
}

/// Turn a reqwest error into the failure class the scheduler acts on.
///
/// The typed questions first, because reqwest can answer those honestly, and
/// only then the source chain walk that has to read English.
fn transport_failure(error: reqwest::Error) -> Failure {
    if error.is_timeout() {
        return Failure::Timeout(if error.is_connect() {
            Stage::Connect
        } else {
            Stage::Read
        });
    }
    if error.is_body() || error.is_decode() {
        return Failure::Malformed;
    }
    failure_from_text(&error)
}
