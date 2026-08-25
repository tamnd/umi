//! Opening a source, reading it, and holding a seeder to its exit code.

use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::lines::{self, Line};
use crate::{Error, Limits, Rejection, Seed, Seen, Source, Stats, parse};

/// How much of a failing seeder's standard error to quote back.
///
/// A seeder that dies in a stack trace has already had all of it forwarded to
/// our own standard error. This is only the part that goes into the error
/// message, and the last kilobyte is where the reason usually is.
const TAIL_BYTES: usize = 1024;

/// Start reading URLs from `source`.
///
/// # Errors
///
/// Returns [`Error::Spawn`] if the seeder program cannot be started and
/// [`Error::Read`] if a file cannot be opened. Everything that goes wrong
/// after this point arrives through the iterator.
pub fn seed(source: Source, limits: Limits) -> Result<SeedStream, Error> {
    let label = source.label();
    let (input, child, stderr): (Box<dyn BufRead + Send>, _, _) = match &source {
        Source::Stdin => (Box::new(BufReader::new(io::stdin())), None, None),
        Source::File(path) => {
            let file = File::open(path).map_err(|cause| Error::Read {
                source_name: label.clone(),
                cause,
            })?;
            (Box::new(BufReader::new(file)), None, None)
        }
        Source::Command(_) | Source::Shell(_) => {
            let mut child = spawn(&source, &label)?;
            // Both are set to piped below, so neither take can be None, but
            // this crate denies unsafe and would rather not unwrap either.
            let out = child.stdout.take().ok_or_else(|| Error::Spawn {
                program: label.clone(),
                cause: io::Error::other("the seeder has no standard output"),
            })?;
            let errors = child.stderr.take().map(forward);
            (Box::new(BufReader::new(out)), Some(child), errors)
        }
    };

    Ok(SeedStream {
        input,
        child,
        stderr,
        label,
        limits,
        seen: Seen::new(limits.max_seen),
        stats: Stats::default(),
        buffer: Vec::with_capacity(256),
        done: false,
    })
}

/// Build the child process for the two command shaped sources.
fn spawn(source: &Source, label: &str) -> Result<Child, Error> {
    let mut command = match source {
        Source::Command(argv) => {
            let (program, rest) = argv.split_first().ok_or_else(|| Error::Spawn {
                program: label.to_owned(),
                cause: io::Error::other("the seeder command is empty"),
            })?;
            let mut command = Command::new(program);
            command.args(rest);
            command
        }
        Source::Shell(line) => {
            // `sh` and not the operator's login shell. Doc 13.7's examples are
            // pipes and quoting, which every POSIX shell agrees on, and a
            // seeder that only runs under one person's zsh configuration is
            // not reproducible on the next machine.
            let mut command = Command::new("sh");
            command.arg("-c").arg(line);
            command
        }
        Source::Stdin | Source::File(_) => unreachable!("only commands are spawned"),
    };
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|cause| Error::Spawn {
            program: label.to_owned(),
            cause,
        })
}

/// What the reader thread shares with the stream.
type Tail = Arc<Mutex<Vec<u8>>>;

/// Forward the seeder's standard error to ours and keep the tail of it.
///
/// Forwarding matters because a seeder enumerating a large site prints
/// progress there and an operator watching a crawl start should see it. The
/// tail matters because by the time the exit code arrives that output has
/// scrolled away.
fn forward(mut errors: impl Read + Send + 'static) -> (JoinHandle<()>, Tail) {
    let tail: Tail = Arc::new(Mutex::new(Vec::new()));
    let kept = Arc::clone(&tail);
    let handle = std::thread::spawn(move || {
        let mut chunk = [0u8; 8192];
        loop {
            let read = match errors.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            let bytes = &chunk[..read];
            // Ignore a failed write to our own stderr. It means the operator
            // closed it, which is not a reason to stop seeding.
            let _ = io::stderr().write_all(bytes);
            if let Ok(mut kept) = kept.lock() {
                kept.extend_from_slice(bytes);
                if kept.len() > TAIL_BYTES {
                    let from = kept.len() - TAIL_BYTES;
                    kept.drain(..from);
                }
            }
        }
    });
    (handle, tail)
}

/// URLs from one source, canonicalised and deduplicated as they arrive.
///
/// The iterator ends when the source does, except that a seeder which exits
/// non zero produces one final [`Error::Failed`] first. Dropping the stream
/// before the end kills the seeder rather than leaving it writing into a pipe
/// nobody reads.
pub struct SeedStream {
    input: Box<dyn BufRead + Send>,
    child: Option<Child>,
    stderr: Option<(JoinHandle<()>, Tail)>,
    label: String,
    limits: Limits,
    seen: Seen,
    stats: Stats,
    buffer: Vec<u8>,
    done: bool,
}

impl SeedStream {
    /// What the run has done so far, or all of it once the iterator has ended.
    #[must_use]
    pub fn stats(&self) -> Stats {
        let mut stats = self.stats;
        stats.undeduplicated = self.seen.full;
        stats
    }

    /// Reap the seeder and turn a bad exit into an error.
    fn finish(&mut self) -> Option<Error> {
        let mut child = self.child.take()?;
        let status = match child.wait() {
            Ok(status) => status,
            Err(cause) => {
                return Some(Error::Read {
                    source_name: self.label.clone(),
                    cause,
                });
            }
        };
        // Join after the wait, because the thread only ends when the pipe
        // closes and the pipe only closes when the child does.
        let tail = self.stderr.take().map_or_else(Vec::new, |(handle, tail)| {
            let _ = handle.join();
            tail.lock().map(|kept| kept.clone()).unwrap_or_default()
        });
        if status.success() {
            return None;
        }
        let quoted = String::from_utf8_lossy(&tail).trim_end().to_owned();
        Some(Error::Failed {
            program: self.label.clone(),
            status: describe(&status),
            tail: if quoted.is_empty() {
                String::new()
            } else {
                format!("\n{quoted}")
            },
        })
    }
}

/// How the seeder ended, in words rather than in a number nobody remembers.
fn describe(status: &std::process::ExitStatus) -> String {
    match status.code() {
        Some(code) => format!("exited with status {code}"),
        // No code means a signal on Unix. Reading the signal number needs the
        // platform extension trait, and "was killed" is the part the operator
        // acts on either way.
        None => "was killed before it finished".to_owned(),
    }
}

impl Iterator for SeedStream {
    type Item = Result<Seed, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            }
            let capped =
                lines::read_capped(&mut *self.input, &mut self.buffer, self.limits.max_line);
            let line = match capped {
                Ok(line) => line,
                Err(cause) => {
                    self.done = true;
                    return Some(Err(Error::Read {
                        source_name: self.label.clone(),
                        cause,
                    }));
                }
            };
            match line {
                Line::End => {
                    self.done = true;
                    return self.finish().map(Err);
                }
                Line::TooLong => {
                    self.stats.lines += 1;
                    self.stats.too_long += 1;
                }
                Line::Read => {
                    self.stats.lines += 1;
                    match parse(&self.buffer) {
                        Ok(None) => self.stats.skipped += 1,
                        Ok(Some(seed)) => {
                            if self.seen.admit(seed.keys.url) {
                                self.stats.accepted += 1;
                                return Some(Ok(seed));
                            }
                            self.stats.duplicate += 1;
                        }
                        Err(Rejection::NotUtf8) => self.stats.not_utf8 += 1,
                        Err(Rejection::Canon(why)) => {
                            self.stats.rejected += 1;
                            self.stats.why.count(why);
                        }
                    }
                }
            }
        }
    }
}

impl Drop for SeedStream {
    fn drop(&mut self) {
        // A crawl that stops early, because the scope filled or somebody hit
        // ctrl-C, leaves a seeder writing into a pipe with no reader. On Unix
        // that is a SIGPIPE and it usually ends the process, but "usually" is
        // not good enough for something that could be a browser or a database
        // client, so it gets killed and reaped here.
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        // The stderr forwarder is dropped rather than joined, which is the
        // opposite of what [`finish`](SeedStream::finish) does and is
        // deliberate.
        //
        // That thread ends when its read returns zero, and the read returns
        // zero when the last write handle on the pipe closes. Killing the child
        // closes the child's handle and nothing else's, so joining here is a
        // bet that the child was the only holder. On Unix it usually is,
        // because a seeder run through `sh -c` is exec'd into the shell rather
        // than forked from it. On Windows it is not: `sh` comes from Git for
        // Windows, terminating it does not reliably take its descendants with
        // it, and anything it left running keeps the write end open. Joining
        // then blocks forever, which is a hang inside a `Drop` and takes the
        // whole process with it.
        //
        // `finish` can afford the join because it has already waited for the
        // child to exit on its own. `Drop` has not, so it cannot.
        //
        // So the thread is detached. It holds a pipe and a `Vec` of at most a
        // few kilobytes, it exits as soon as the write end goes away, and
        // nobody is going to read the tail it is filling because the stream
        // that owned it is gone.
        drop(self.stderr.take());
    }
}
