//! A Hugging Face that runs on a socket, for the tests in this crate.
//!
//! A real hub is not available to a test and a mocked client is not worth
//! testing, so what runs against this is the actual `reqwest` client over an
//! actual TCP connection speaking the actual protocol. That covers the things
//! the client can get wrong on its own: the shape of the ndjson, the order of
//! the requests, the numeric sort on multipart, which statuses are answers
//! rather than failures, and the token going exactly one place.
//!
//! It lives in its own file rather than inside `hub_tests` because the
//! pipeline tests need the same socket, and two copies of a fake hub would
//! drift the first time the protocol moved.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::hub::{Hub, HubConfig, Retry};

/// One request the scripted hub saw.
#[derive(Clone, Debug)]
pub(crate) struct Seen {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) body: Vec<u8>,
}

impl Seen {
    pub(crate) fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).expect("the body is json")
    }

    /// The ndjson commit body, one value a line.
    pub(crate) fn lines(&self) -> Vec<serde_json::Value> {
        String::from_utf8_lossy(&self.body)
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).expect("a commit line is json"))
            .collect()
    }
}

/// What the scripted hub answers with.
pub(crate) struct Say {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Say {
    pub(crate) fn json(status: u16, value: serde_json::Value) -> Self {
        Self {
            status,
            headers: vec![("content-type".into(), "application/json".into())],
            body: value.to_string().into_bytes(),
        }
    }

    pub(crate) fn ok(value: serde_json::Value) -> Self {
        Self::json(200, value)
    }

    pub(crate) fn status(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub(crate) fn bytes(status: u16, body: &[u8]) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.to_vec(),
        }
    }

    pub(crate) fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }
}

/// A hub on localhost that answers from a closure and records everything.
pub(crate) struct Scripted {
    addr: std::net::SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Scripted {
    pub(crate) async fn new<F>(route: F) -> Self
    where
        F: Fn(&Seen) -> Say + Send + Sync + 'static,
    {
        Self::routed(|_| route).await
    }

    /// As [`Scripted::new`], for a hub whose answers name its own address.
    ///
    /// An lfs batch response carries the url the bytes go to, and in a test
    /// that url is this hub. The port is not known until the listener is
    /// bound, so the router is built from the address rather than before it.
    pub(crate) async fn routed<B, F>(build: B) -> Self
    where
        B: FnOnce(std::net::SocketAddr) -> F,
        F: Fn(&Seen) -> Say + Send + Sync + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("a bound port has an address");
        let seen = Arc::new(Mutex::new(Vec::new()));
        let route = Arc::new(build(addr));
        let recorded = Arc::clone(&seen);

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let route = Arc::clone(&route);
                let seen = Arc::clone(&recorded);
                tokio::spawn(async move {
                    // Keep alive, because reqwest pools connections and a
                    // client that had to reconnect per request would be
                    // testing the socket rather than the protocol.
                    while let Some(request) = read_request(&mut socket).await {
                        let say = route(&request);
                        seen.lock().expect("not poisoned").push(request);
                        let mut head = format!("HTTP/1.1 {} X\r\n", say.status);
                        for (name, value) in &say.headers {
                            head.push_str(&format!("{name}: {value}\r\n"));
                        }
                        head.push_str(&format!("content-length: {}\r\n\r\n", say.body.len()));
                        if socket.write_all(head.as_bytes()).await.is_err()
                            || socket.write_all(&say.body).await.is_err()
                        {
                            return;
                        }
                        let _ = socket.flush().await;
                    }
                });
            }
        });

        Self { addr, seen }
    }

    pub(crate) fn config(&self) -> HubConfig {
        HubConfig {
            base: format!("http://{}", self.addr),
            timeout: Duration::from_secs(5),
            // Short enough that a test which exhausts the ladder finishes in
            // well under a second.
            retry: Retry {
                attempts: 3,
                backoff: Duration::from_millis(2),
                seed: 7,
            },
        }
    }

    pub(crate) fn hub(&self) -> Hub {
        Hub::with_config("hf_scripted_token", &self.config()).expect("the client builds")
    }

    pub(crate) fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("not poisoned").clone()
    }

    pub(crate) fn paths(&self) -> Vec<String> {
        self.seen()
            .into_iter()
            .map(|request| format!("{} {}", request.method, request.path))
            .collect()
    }
}

/// One request off the wire, head then body by content length.
pub(crate) async fn read_request(socket: &mut tokio::net::TcpStream) -> Option<Seen> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        match socket.read(&mut byte).await {
            Ok(0) | Err(_) => return None,
            Ok(_) => head.push(byte[0]),
        }
    }
    let text = String::from_utf8_lossy(&head).into_owned();
    let mut lines = text.lines();
    let first = lines.next()?;
    let mut parts = first.split_whitespace();
    let method = parts.next()?.to_owned();
    let path = parts.next()?.to_owned();

    let mut headers = BTreeMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }
    let length: usize = headers
        .get("content-length")
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; length];
    if length > 0 && socket.read_exact(&mut body).await.is_err() {
        return None;
    }
    Some(Seen {
        method,
        path,
        headers,
        body,
    })
}
