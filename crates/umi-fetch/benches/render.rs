//! What one rendered page costs, and whether the pool keeps its promises.
//!
//! Doc 05.6 says T3 is expensive and puts it under one percent of volume. This
//! is the measurement behind that sentence, and it is the number doc 05.9's
//! render budget divides the fleet page rate by.
//!
//! Not a CPU benchmark, and it would be misleading to read it as one. A render
//! is mostly waiting: the quiet period alone is 1500 ms and it is paid on every
//! page, so per page wall time barely moves with the machine. What the pool
//! actually buys is the eight tabs, so the number that matters is pages per
//! second per browser and this prints that as well as the per page time.
//!
//! It needs Chrome or Chromium on the box and it starts a real browser. Run it
//! on server2 or server3, and remember the browser has its own idea about cores
//! so pinning it to one with `taskset` measures something that is not the crawl.
//!
//! ```text
//! cargo bench -p umi-fetch --features render --bench render
//! ```
//!
//! Everything is served from a loopback origin, so the numbers are about the
//! browser and not about somebody else's network. Real pages are slower and the
//! spread between them is wide, which is why the pool exists.

#![allow(clippy::print_stdout, reason = "a bench reports by printing")]

#[cfg(not(feature = "render"))]
fn main() {
    println!("the render feature is off, so there is no T3 to measure");
}

#[cfg(feature = "render")]
fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    runtime.block_on(bench::run());
}

#[cfg(feature = "render")]
mod bench {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use umi_fetch::rendered::QUIET;
    use umi_fetch::{FetchConfig, Outcome, RenderConfig, Renderer};

    /// Pages per round. Two full turns of an eight tab pool, so the pool is the
    /// thing being measured rather than one cold tab.
    const PAGES: usize = 16;

    /// Rounds. Minimums rather than means, the same as every other bench here:
    /// a box that is doing something else produces a slow round and a slow
    /// round is noise, not a measurement.
    const ROUNDS: usize = 3;

    /// The tab cap for the load test. Smaller than doc 05.6's eight so that the
    /// cap actually binds at [`PAGES`] concurrent requests.
    const CAP: usize = 4;

    pub async fn run() {
        let origin = Origin::start().await;
        println!("origin on http://{}", origin.addr);

        settling(&origin).await;
        cost(&origin).await;
        cap_holds(&origin).await;
        never_idle(&origin).await;
        tabs_recycle(&origin).await;
    }

    /// Where the time in one render actually goes.
    ///
    /// One tab, one page at a time, so there is no contention in the number.
    /// The gap between the two rows is doc 05.6's quiet period and the gap
    /// between the fast row and zero is what the browser costs to drive. Both
    /// are worth knowing separately, because only one of them is a knob.
    async fn settling(origin: &Origin) {
        for (label, quiet) in [
            ("load only", Duration::from_millis(100)),
            ("load and quiet", QUIET),
        ] {
            let mut config = RenderConfig::default();
            config.tabs = 1;
            config.quiet = quiet;
            let renderer = launch(config).await;
            // Warm first. The first render on a fresh browser pays for the
            // renderer process and the first connection, and doc 05.6's number
            // is about a pool that has been running for hours.
            let mut best = Duration::MAX;
            for page in 0..4 {
                let url = format!("http://{}/page/{page}", origin.addr);
                let started = Instant::now();
                renderer.fetch(&url, None).await.expect("a real url");
                if page > 0 {
                    best = best.min(started.elapsed());
                }
            }
            println!(
                "one tab, {label:<15} {:>7.0} ms",
                best.as_secs_f64() * 1000.0
            );
            renderer.shutdown().await;
        }
        println!();
    }

    /// Doc 05.6's per page cost, and the pages per second that falls out of it.
    async fn cost(origin: &Origin) {
        let renderer = launch(RenderConfig::default()).await;
        let mut best = Duration::MAX;

        for round in 0..ROUNDS {
            origin.reset();
            let started = Instant::now();
            let mut work = Vec::new();
            for page in 0..PAGES {
                let renderer = renderer.clone();
                let url = format!("http://{}/page/{page}", origin.addr);
                work.push(tokio::spawn(
                    async move { renderer.fetch(&url, None).await },
                ));
            }
            for (page, task) in work.into_iter().enumerate() {
                let outcome = task.await.expect("the task lives").expect("a real url");
                let Outcome::Ok(rendered) = outcome else {
                    panic!("page {page} came back as {outcome:?}");
                };
                // The whole point of T3. This string is not in the HTML the
                // origin sent, it is put there by a script, so a build that
                // returns the shell rather than the page fails here rather
                // than quietly reporting a very fast render.
                let body = String::from_utf8_lossy(&rendered.body);
                assert!(
                    body.contains("rendered by script"),
                    "page {page} was not rendered: {body}"
                );
            }
            let elapsed = started.elapsed();
            println!("round {round}: {PAGES} pages in {elapsed:?}");
            best = best.min(elapsed);
        }

        let counts = renderer.counts();
        let per_page = counts.mean_render();
        let rate = PAGES as f64 / best.as_secs_f64();
        println!();
        println!(
            "per page render     {:>8.0} ms",
            per_page.as_secs_f64() * 1000.0
        );
        println!("pages per second    {rate:>8.2}  at {} tabs", TABS_DEFAULT);
        println!("subresource bytes   {:>8} per page", counts.mean_bytes());
        println!(
            "requests            {:>8} allowed, {} blocked",
            counts.allowed, counts.blocked
        );
        println!(
            "tabs                {:>8} opened, {} recycled, {} reaped",
            counts.opened, counts.recycled, counts.reaped
        );
        // Doc 05.9 divides the fleet page rate by this to size the render
        // budget. One browser at this rate is what a fetcher can promise.
        println!(
            "at 250 pages/s one browser covers {:.2} percent of the fleet",
            rate / 250.0 * 100.0
        );
        println!();

        renderer.shutdown().await;
    }

    /// The tab cap, which is a memory promise and so has to bind on tabs that
    /// exist rather than on renders that have been asked for.
    async fn cap_holds(origin: &Origin) {
        // Field by field rather than a struct literal, because `RenderConfig`
        // is non exhaustive and a bench is another crate.
        let mut config = RenderConfig::default();
        config.tabs = CAP;
        // Short, because this measures overlap and not settling.
        config.quiet = Duration::from_millis(200);
        let renderer = launch(config).await;

        origin.reset();
        let mut work = Vec::new();
        for page in 0..PAGES {
            let renderer = renderer.clone();
            // The slow route holds the document open, which is what makes the
            // overlap visible. Without it a page is served faster than the next
            // one starts and the peak would be one at any cap.
            let url = format!("http://{}/slow/{page}", origin.addr);
            work.push(tokio::spawn(
                async move { renderer.fetch(&url, None).await },
            ));
        }
        for task in work {
            task.await.expect("the task lives").expect("a real url");
        }

        let peak = origin.peak();
        println!("cap: {CAP} tabs, {PAGES} requests, peak {peak} documents in flight");
        assert!(
            peak <= CAP,
            "the pool opened {peak} tabs with a cap of {CAP}"
        );
        // And the other half, which is the one that would pass by accident. A
        // pool that served all sixteen one after another also never exceeds the
        // cap, and it is not the pool doc 05.6 asked for.
        assert!(peak > 1, "the pool served {PAGES} requests one at a time");
        assert!(
            renderer.parked() <= CAP,
            "the pool parked more tabs than it is allowed to have"
        );
        println!();

        renderer.shutdown().await;
    }

    /// A page whose network never goes quiet ends at the ceiling, comes back
    /// with what it has, and gives its tab up rather than holding it.
    async fn never_idle(origin: &Origin) {
        let ceiling = Duration::from_secs(2);
        let mut config = RenderConfig::default();
        config.tabs = 1;
        config.ceiling = ceiling;
        let renderer = launch(config).await;

        // Warm the tab first, so that the number below is the ceiling and not
        // the cost of starting a renderer process.
        let warm = format!("http://{}/page/0", origin.addr);
        renderer.fetch(&warm, None).await.expect("a real url");

        let url = format!("http://{}/hang", origin.addr);
        let started = Instant::now();
        let outcome = renderer.fetch(&url, None).await.expect("a real url");
        let elapsed = started.elapsed();

        assert!(
            matches!(outcome, Outcome::Ok(_)),
            "a page that never settles should still come back: {outcome:?}"
        );
        assert!(
            elapsed >= ceiling && elapsed < ceiling * 2,
            "the ceiling did not stop it: {elapsed:?}"
        );
        println!("ceiling: a page that never goes quiet returned after {elapsed:?}");

        // And the tab is available again, which is the half that would leak.
        let second = renderer.fetch(&url, None).await.expect("a real url");
        assert!(matches!(second, Outcome::Ok(_)));
        let counts = renderer.counts();
        println!(
            "ceiling: {} tabs opened for 3 renders, {} reaped",
            counts.opened, counts.reaped
        );
        assert_eq!(
            counts.opened, 1,
            "a render that hit the ceiling lost its tab"
        );
        println!();

        renderer.shutdown().await;
    }

    /// Tabs are thrown away at the page limit rather than living forever.
    async fn tabs_recycle(origin: &Origin) {
        let mut config = RenderConfig::default();
        config.tabs = 1;
        config.pages_per_tab = 2;
        config.quiet = Duration::from_millis(200);
        let renderer = launch(config).await;

        for page in 0..6 {
            let url = format!("http://{}/page/{page}", origin.addr);
            renderer.fetch(&url, None).await.expect("a real url");
        }
        let counts = renderer.counts();
        println!(
            "recycling: 6 pages at 2 per tab opened {} tabs and recycled {}",
            counts.opened, counts.recycled
        );
        assert_eq!(counts.recycled, 3, "tabs were not recycled at the limit");
        assert!(
            renderer.parked() <= 1,
            "a recycled tab was parked instead of closed"
        );
        println!();

        renderer.shutdown().await;
    }

    const TABS_DEFAULT: usize = 8;

    async fn launch(mut render: RenderConfig) -> Renderer {
        // Chromium will not keep its sandbox as root, and the fleet benchmark
        // boxes are root. This is a bench and not a fetcher, so it says so
        // rather than refusing to run.
        render.sandbox = !running_as_root();
        Renderer::launch(FetchConfig::default(), render, None)
            .await
            .expect("a browser: install chrome or chromium first")
    }

    /// Whether this process is root, read from the filesystem rather than from
    /// `getuid`, because the workspace denies unsafe code and this is not worth
    /// a dependency.
    fn running_as_root() -> bool {
        std::fs::read_to_string("/proc/self/status").is_ok_and(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("Uid:"))
                .and_then(|line| line.split_whitespace().next().map(str::to_owned))
                .as_deref()
                == Some("0")
        })
    }

    /// A loopback origin that serves a page a script has to finish.
    pub struct Origin {
        pub addr: SocketAddr,
        /// The most document requests that were ever open at the same time.
        peak: Arc<AtomicUsize>,
    }

    impl Origin {
        fn reset(&self) {
            self.peak.store(0, Ordering::SeqCst);
        }

        fn peak(&self) -> usize {
            self.peak.load(Ordering::SeqCst)
        }

        async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("a bound port has an address");
            let peak = Arc::new(AtomicUsize::new(0));
            let live = Arc::new(AtomicUsize::new(0));

            let accept_peak = Arc::clone(&peak);
            let accept_live = Arc::clone(&live);
            tokio::spawn(async move {
                loop {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        return;
                    };
                    let peak = Arc::clone(&accept_peak);
                    let live = Arc::clone(&accept_live);
                    tokio::spawn(async move {
                        let mut head = Vec::new();
                        let mut byte = [0u8; 1];
                        while !head.ends_with(b"\r\n\r\n") {
                            match socket.read(&mut byte).await {
                                Ok(0) | Err(_) => return,
                                Ok(_) => head.push(byte[0]),
                            }
                        }
                        let request = String::from_utf8_lossy(&head).into_owned();
                        let path = request
                            .split_whitespace()
                            .nth(1)
                            .unwrap_or_default()
                            .to_owned();

                        // The request that never gets an answer, which is how a
                        // page is made to never go quiet.
                        if path == "/never" {
                            tokio::time::sleep(Duration::from_secs(3600)).await;
                            return;
                        }

                        let document = path.starts_with("/page/")
                            || path.starts_with("/slow/")
                            || path == "/hang";
                        if document {
                            let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(now, Ordering::SeqCst);
                            if path.starts_with("/slow/") {
                                tokio::time::sleep(Duration::from_millis(300)).await;
                            }
                        }

                        let reply = route(&path);
                        let _ = socket.write_all(&reply).await;
                        let _ = socket.flush().await;
                        if document {
                            live.fetch_sub(1, Ordering::SeqCst);
                        }
                    });
                }
            });

            Self { addr, peak }
        }
    }

    /// The whole site, which is four files.
    fn route(path: &str) -> Vec<u8> {
        match path {
            "/app.js" => reply(
                "application/javascript",
                "fetch('/api').then(r => r.text()).then(t => { \
                 document.getElementById('app').textContent = t; });",
            ),
            "/hang.js" => reply(
                "application/javascript",
                "document.getElementById('app').textContent = 'rendered by script'; \
                 fetch('/never');",
            ),
            "/api" => reply("text/plain", "rendered by script"),
            // Refused by the resource filter before they leave the browser, so
            // these bodies only exist to prove the filter is doing it.
            "/style.css" => reply("text/css", "body { color: rebeccapurple }"),
            "/pic.png" => reply("image/png", "not really a png"),
            "/hang" => reply("text/html", &page("/hang.js")),
            _ => reply("text/html", &page("/app.js")),
        }
    }

    /// A page that says nothing until a script has run.
    fn page(script: &str) -> String {
        format!(
            "<!doctype html><html><head><title>bench</title>\
             <link rel=\"stylesheet\" href=\"/style.css\"></head>\
             <body><div id=\"app\">waiting</div>\
             <img src=\"/pic.png\" alt=\"\">\
             <script src=\"{script}\"></script></body></html>"
        )
    }

    fn reply(content_type: &str, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }
}
