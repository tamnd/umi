//! `umi ls` and `umi cat`, from doc 14.6.
//!
//! Both work on the two file kinds umi produces, the `.umi` segment from doc 10
//! and the Parquet file from doc 12, and both print the same fields for either,
//! because "what is in this thing" should not depend on which side of the
//! publish step you are standing on.
//!
//! `umi ls` goes one step further and takes a published repository as well as a
//! directory, which is doc 14.6's second example. The columns are the same
//! because the question is the same, and the answer comes from doc 12.5's day
//! manifests rather than from a file this machine can open. That is the only
//! honest source for a row count over the network: the hub knows how many bytes
//! a file is and has no idea how many rows are in it.
//!
//! # What `ls` does not do
//!
//! It does not check anything. A listing reports what the repository says about
//! itself, and `umi verify` is the command that decides whether to believe it:
//! signatures, the day chain and every digest. So `ls` will happily print a
//! repository that `verify` rejects, and where the two sources it reads
//! disagree it says so in the summary and still exits zero. Splitting it the
//! other way, with a listing that verified as it went, would mean nobody could
//! look at a broken repository to find out how it broke.
//!
//! Progress and headings go to stderr and rows go to stdout, so that
//! `umi cat ... | head` behaves, which doc 14.9 asks for by name.

use std::collections::BTreeMap;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::FileReader as _;
use parquet::file::serialized_reader::SerializedFileReader;
use umi_file::Segment;
use umi_publish::Hub;
use umi_publish::manifest::Manifest;

use crate::Error;

/// What one file turned out to hold.
struct Listing {
    path: PathBuf,
    kind: &'static str,
    rows: u64,
    groups: usize,
    bytes: u64,
    first_ms: u64,
    last_ms: u64,
}

/// Where `umi ls` should look, and what it needs to look there.
#[derive(Clone, Debug)]
pub struct Ls<'a> {
    /// A crawl directory, a single file, or a published repository.
    pub target: &'a str,
    /// A Hugging Face token, for a repository that is not public. Everything
    /// this project publishes is public, so this is almost always `None`.
    pub token: Option<String>,
    /// The organisation to put in front of a bare repository name.
    pub org: &'a str,
}

/// `umi ls`: every segment and every Parquet file under a directory, or every
/// published file in a repository, with row counts, byte counts and time
/// ranges.
///
/// # Errors
///
/// When the directory cannot be read, or a file that looks like a segment is
/// not one. A file that fails to open is reported and does not stop the walk,
/// because the common reason to run `ls` is that something is wrong. For a
/// repository, whatever the hub says, and doc 14.9's exit 6 for a manifest that
/// will not parse.
pub fn ls(options: &Ls<'_>) -> Result<(), Error> {
    match repository(options.target, options.org) {
        Some(repo) => published(&repo, options.token.clone()),
        None => local(options.target),
    }
}

/// Whether a target names a published repository rather than something on this
/// disk, and under which organisation.
///
/// Anything that exists locally is local, so a directory called `open-index`
/// wins over the organisation of the same name. After that the rule is the one
/// doc 12.4 fixes: a published repository is `owner/name`, and a name on its
/// own is one of ours if it is spelled the way doc 12.4 spells them. Everything
/// else stays a path, so that a mistyped directory reports a mistyped directory
/// instead of quietly going to the network to look for it.
fn repository(target: &str, org: &str) -> Option<String> {
    if Path::new(target).exists() || target.starts_with('.') || target.starts_with('/') {
        return None;
    }
    if target.contains('/') {
        return Some(target.to_owned());
    }
    target
        .starts_with("umi-")
        .then(|| format!("{org}/{target}"))
}

fn local(target: &str) -> Result<(), Error> {
    let mut found = Vec::new();
    let mut broken = 0usize;
    for path in walk(Path::new(target))? {
        match list_one(&path) {
            Ok(listing) => found.push(listing),
            Err(cause) => {
                eprintln!("{}: {cause}", path.display());
                broken += 1;
            }
        }
    }
    found.sort_by(|a, b| a.path.cmp(&b.path));

    if found.is_empty() && broken == 0 {
        return Err(Error::Empty);
    }

    println!(
        "{:<44} {:>6} {:>12} {:>7} {:>14} {:>14}",
        "file", "kind", "rows", "groups", "bytes", "span"
    );
    let (mut rows, mut bytes) = (0u64, 0u64);
    for one in &found {
        rows += one.rows;
        bytes += one.bytes;
        println!(
            "{:<44} {:>6} {:>12} {:>7} {:>14} {:>14}",
            trim(&one.path.display().to_string(), 44),
            one.kind,
            one.rows,
            one.groups,
            one.bytes,
            span(one.first_ms, one.last_ms),
        );
    }
    println!(
        "{} files, {rows} rows, {bytes} bytes{}",
        found.len(),
        if broken == 0 {
            String::new()
        } else {
            format!(", {broken} unreadable")
        }
    );
    if broken > 0 {
        return Err(Error::Unreadable(broken));
    }
    Ok(())
}

/// One published file, as the manifest describes it and as the hub holds it.
///
/// Every file from either source gets one of these, including a file only one
/// source knows about, because the rows a listing is most useful for are
/// exactly the ones where the two disagree.
struct Entry {
    path: String,
    day: String,
    /// How many rows the manifest says, or nothing for a file no manifest
    /// names. The hub cannot answer this and neither can a guess.
    rows: Option<u64>,
    /// How many bytes it should be: the manifest's number where there is one
    /// and the hub's where there is not.
    bytes: u64,
    first_ms: u64,
    last_ms: u64,
    /// What the hub says the file is, or `None` when the hub does not have it
    /// at all.
    on_hub: Option<u64>,
}

impl Entry {
    /// The one word in the last column.
    ///
    /// Four states and not two. "The hub has never heard of this file", "the
    /// hub has it and it is a different length" and "the hub has it and no
    /// manifest claims it" are three different things that went wrong at three
    /// different points, and collapsing them into one word would mean reading
    /// the manifests by hand to tell them apart. The first is a publish that
    /// stopped between doc 12.2's step 6 and its step 4 for a later file, the
    /// third is what doc 12.8's reconciliation is for, and the second is not
    /// something that is supposed to be able to happen at all.
    fn state(&self) -> &'static str {
        match (self.rows, self.on_hub) {
            (None, _) => "unnamed",
            (Some(_), Some(bytes)) if bytes == self.bytes => "ok",
            (Some(_), Some(_)) => "size",
            (Some(_), None) => "missing",
        }
    }
}

/// The `YYYYMMDD` folder out of doc 12.4's `data/<day>/<ulid>.parquet`, for a
/// file that has no manifest to say which day it belongs to.
fn day_of(path: &str) -> String {
    path.strip_prefix("data/")
        .and_then(|rest| rest.split('/').next())
        .filter(|day| day.len() == 8 && day.bytes().all(|byte| byte.is_ascii_digit()))
        .unwrap_or("-")
        .to_owned()
}

/// `umi ls open-index/...`: doc 12.5's day manifests, joined against what the
/// hub actually holds.
fn published(repo: &str, token: Option<String>) -> Result<(), Error> {
    let hub = Hub::new(token.unwrap_or_default())?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(Error::Io)?;
    let (manifests, held) = runtime.block_on(read_published(&hub, repo))?;
    let entries = pair(manifests, held);
    if entries.is_empty() {
        // Doc 14.9's exit 3 and not a failure. An empty repository is a
        // truthful answer to "what is published here", and it is the answer for
        // a repository that `ensure_dataset` created and the first segment of
        // the week has not landed in yet.
        return Err(Error::Empty);
    }

    let stdout = std::io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    render(repo, &entries, &mut out).map_err(Error::Io)
}

/// Read every day manifest and every file the repository holds.
///
/// The two halves are returned rather than joined here so that the joining,
/// which is where the interesting decisions are, is a function with no network
/// in it.
async fn read_published(
    hub: &Hub,
    repo: &str,
) -> Result<(Vec<Manifest>, Vec<(String, u64)>), Error> {
    let mut days: Vec<String> = hub
        .list(repo, "_manifest")
        .await?
        .into_iter()
        .filter_map(|found| {
            found
                .path
                .strip_prefix("_manifest/")
                .and_then(|name| name.strip_suffix(".json"))
                .map(ToOwned::to_owned)
        })
        .collect();
    // The names are `YYYYMMDD`, so sorting them as text sorts them as dates.
    days.sort();

    // Read even when there are no manifests, rather than stopping here. A
    // repository with files and no manifest is not an empty repository, it is
    // doc 12.8's reconciliation waiting to happen, and reporting that as
    // "nothing to list" would hide the one thing worth seeing.
    //
    // One listing for the whole repository rather than a `paths-info` call per
    // file. A week of the general corpus is a few hundred files, and doc 12.8
    // needs the same listing, so this is the request that is already paid for.
    let held = hub
        .list(repo, "data")
        .await?
        .into_iter()
        .map(|found| (found.path, found.size))
        .collect();

    let mut manifests = Vec::with_capacity(days.len());
    for day in days {
        let path = format!("_manifest/{day}.json");
        let bytes = hub.read(repo, &path).await?.ok_or(
            // The same message `umi verify` uses for the same thing, and the
            // same exit 6, because a manifest that is in the listing and will
            // not read is the repository contradicting itself.
            umi_publish::Error::Manifest("a manifest in the listing would not read"),
        )?;
        manifests.push(Manifest::parse(&bytes)?);
    }
    Ok((manifests, held))
}

/// Join what the manifests claim against what the hub holds.
///
/// Every file from either source comes out, manifest order first and oldest day
/// first within it, then whatever the hub holds that no manifest named.
fn pair(manifests: Vec<Manifest>, held: Vec<(String, u64)>) -> Vec<Entry> {
    let mut held: BTreeMap<String, u64> = held.into_iter().collect();
    let mut entries = Vec::new();
    for manifest in manifests {
        for file in manifest.files {
            // Removed rather than looked up, so that whatever is left in the
            // map at the end is exactly the set no manifest claimed.
            let on_hub = held.remove(&file.path);
            entries.push(Entry {
                path: file.path,
                day: manifest.day.clone(),
                rows: Some(file.rows),
                bytes: file.bytes,
                first_ms: file.fetched_at_min_ms,
                last_ms: file.fetched_at_max_ms,
                on_hub,
            });
        }
    }
    for (path, size) in held {
        entries.push(Entry {
            day: day_of(&path),
            path,
            rows: None,
            bytes: size,
            first_ms: 0,
            last_ms: 0,
            on_hub: Some(size),
        });
    }
    entries
}

/// The table and the summary under it.
fn render(repo: &str, entries: &[Entry], out: &mut impl std::io::Write) -> std::io::Result<()> {
    writeln!(
        out,
        "{:<44} {:>8} {:>12} {:>14} {:>10} {:>8}",
        "file", "day", "rows", "bytes", "span", "hub"
    )?;
    let (mut rows, mut bytes, mut wrong) = (0u64, 0u64, 0usize);
    let mut days = BTreeMap::new();
    for entry in entries {
        rows += entry.rows.unwrap_or(0);
        bytes += entry.bytes;
        if entry.state() != "ok" {
            wrong += 1;
        }
        *days.entry(entry.day.as_str()).or_insert(0usize) += 1;
        writeln!(
            out,
            "{:<44} {:>8} {:>12} {:>14} {:>10} {:>8}",
            trim(&entry.path, 44),
            entry.day,
            entry
                .rows
                .map_or_else(|| "-".to_owned(), |count| count.to_string()),
            entry.bytes,
            span(entry.first_ms, entry.last_ms),
            entry.state(),
        )?;
    }
    writeln!(
        out,
        "{} file{}, {rows} rows, {bytes} bytes over {} day{} in {repo}",
        entries.len(),
        if entries.len() == 1 { "" } else { "s" },
        days.len(),
        if days.len() == 1 { "" } else { "s" },
    )?;
    // Said only when there is something to say, and said without an opinion
    // about what it means. `ls` reports and `verify` rules.
    if wrong > 0 {
        writeln!(
            out,
            "{wrong} of them do not line up between the manifests and the hub; umi verify checks the digests",
        )?;
    }
    out.flush()
}

/// `umi cat <file>`: rows as newline delimited JSON.
///
/// # Errors
///
/// When the file cannot be opened, a requested column is not in the schema, or
/// stdout closes early with something other than a broken pipe.
pub fn cat(path: &str, limit: Option<u64>, columns: Option<&str>) -> Result<(), Error> {
    let wanted: Option<Vec<&str>> = columns.map(|list| list.split(',').map(str::trim).collect());

    // A big buffer because the alternative is a write syscall per row, and a
    // 20000 row segment is 20000 syscalls for no reason.
    let stdout = std::io::stdout();
    let mut out = BufWriter::with_capacity(256 * 1024, stdout.lock());

    let result = cat_into(
        Path::new(path),
        wanted.as_deref(),
        limit.unwrap_or(u64::MAX),
        &mut out,
    );
    // A closed pipe is what `| head` looks like from here, and doc 14.9 says
    // that has to behave. Exit zero rather than reporting the write failure.
    match result {
        Err(Error::Io(cause)) if cause.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        other => other,
    }
}

/// The half of [`cat`] that does not know what stdout is, so that a test or a
/// bench can read what it wrote without redirecting the whole process.
///
/// # Errors
///
/// The same ones [`cat`] has, except that a broken pipe is reported rather than
/// swallowed, because only the command line knows that a closed stdout is fine.
pub fn cat_into(
    path: &Path,
    wanted: Option<&[&str]>,
    limit: u64,
    out: &mut impl std::io::Write,
) -> Result<(), Error> {
    match kind_of(path) {
        Kind::Umi => cat_umi(path, wanted, limit, out),
        Kind::Parquet => cat_parquet(path, wanted, limit, out),
    }
}

enum Kind {
    Umi,
    Parquet,
}

fn kind_of(path: &Path) -> Kind {
    match path.extension().and_then(|e| e.to_str()) {
        Some("parquet") => Kind::Parquet,
        _ => Kind::Umi,
    }
}

fn cat_umi(
    path: &Path,
    wanted: Option<&[&str]>,
    limit: u64,
    out: &mut impl std::io::Write,
) -> Result<(), Error> {
    let segment = Segment::open(path)?;
    let schema = segment.schema();
    let names: Vec<String> = match wanted {
        Some(list) => {
            // Checked here rather than left to the decoder, because a column
            // that does not exist is a typo and doc 14.9 makes a typo exit 2.
            // Letting it fall through would report it as exit 6, which is the
            // code that means corruption and is never retried.
            for name in list {
                if schema.field_with_name(name).is_err() {
                    return Err(Error::NoColumn((*name).to_owned()));
                }
            }
            list.iter().map(|s| (*s).to_owned()).collect()
        }
        None => schema.fields().iter().map(|f| f.name().clone()).collect(),
    };
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();

    let mut written = 0u64;
    for index in 0..segment.shoals() {
        if written >= limit {
            break;
        }
        let shoal = segment.shoal(index)?;
        let batch = shoal.to_arrow(&refs)?;
        written += write_ndjson(&batch, limit - written, out)?;
    }
    out.flush().map_err(Error::Io)
}

fn cat_parquet(
    path: &Path,
    wanted: Option<&[&str]>,
    limit: u64,
    out: &mut impl std::io::Write,
) -> Result<(), Error> {
    let file = std::fs::File::open(path).map_err(Error::Io)?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    if let Some(list) = wanted {
        let schema = builder.parquet_schema();
        let mut indices = Vec::new();
        for name in list {
            let at = (0..schema.num_columns())
                .find(|&i| leaf_name(schema.column(i).path().string().as_str()) == *name)
                .ok_or_else(|| Error::NoColumn((*name).to_owned()))?;
            indices.push(at);
        }
        let mask = ProjectionMask::leaves(schema, indices);
        builder = builder.with_projection(mask);
    }
    let reader = builder.build()?;

    let mut written = 0u64;
    for batch in reader {
        if written >= limit {
            break;
        }
        written += write_ndjson(&batch?, limit - written, out)?;
    }
    out.flush().map_err(Error::Io)
}

/// Take at most `budget` rows off the front of a batch and write them.
fn write_ndjson(
    batch: &RecordBatch,
    budget: u64,
    out: &mut impl std::io::Write,
) -> Result<u64, Error> {
    let take = usize::try_from(budget)
        .unwrap_or(usize::MAX)
        .min(batch.num_rows());
    if take == 0 {
        return Ok(0);
    }
    let sliced = batch.slice(0, take);
    let mut writer = arrow::json::LineDelimitedWriter::new(&mut *out);
    writer.write(&sliced)?;
    writer.finish()?;
    Ok(take as u64)
}

fn list_one(path: &Path) -> Result<Listing, Error> {
    let bytes = std::fs::metadata(path).map_err(Error::Io)?.len();
    match kind_of(path) {
        Kind::Umi => {
            let segment = Segment::open(path)?;
            let stats = segment.stats();
            Ok(Listing {
                path: path.to_owned(),
                kind: "umi",
                rows: stats.rows,
                groups: segment.shoals(),
                bytes,
                first_ms: stats.first_ms,
                last_ms: stats.last_ms,
            })
        }
        Kind::Parquet => {
            let file = std::fs::File::open(path).map_err(Error::Io)?;
            let reader = SerializedFileReader::new(file)?;
            let meta = reader.metadata();
            let (mut first, mut last) = (u64::MAX, 0u64);
            for group in 0..meta.num_row_groups() {
                let row_group = meta.row_group(group);
                for column in row_group.columns() {
                    if leaf_name(column.column_path().string().as_str()) != "fetched_at_ms" {
                        continue;
                    }
                    // Statistics are what doc 12.3 turns on so a consumer can
                    // skip row groups, and reading them back here is the
                    // cheapest possible check that they are actually there.
                    if let Some(parquet::file::statistics::Statistics::Int64(stat)) =
                        column.statistics()
                        && let (Some(min), Some(max)) = (stat.min_opt(), stat.max_opt())
                    {
                        first = first.min(*min as u64);
                        last = last.max(*max as u64);
                    }
                }
            }
            Ok(Listing {
                path: path.to_owned(),
                kind: "parquet",
                rows: meta.file_metadata().num_rows().try_into().unwrap_or(0),
                groups: meta.num_row_groups(),
                bytes,
                first_ms: if first == u64::MAX { 0 } else { first },
                last_ms: last,
            })
        }
    }
}

/// Everything under `root` that looks like ours, one level of recursion at a
/// time, because doc 12.4's layout is `data/<day>/<ulid>.parquet` and nothing
/// deeper.
fn walk(root: &Path) -> Result<Vec<PathBuf>, Error> {
    if root.is_file() {
        return Ok(vec![root.to_owned()]);
    }
    let mut found = Vec::new();
    let mut stack = vec![root.to_owned()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir).map_err(Error::Io)?;
        for entry in entries {
            let entry = entry.map_err(Error::Io)?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("umi" | "parquet")
            ) {
                found.push(path);
            }
        }
    }
    Ok(found)
}

/// The last component of a dotted Parquet column path. `links.href` is one
/// column to a consumer and three leaves to Parquet.
fn leaf_name(path: &str) -> &str {
    path.rsplit('.').next().unwrap_or(path)
}

fn trim(text: &str, width: usize) -> String {
    if text.len() <= width {
        return text.to_owned();
    }
    // Keep the tail. The interesting part of a path is the file name and the
    // uninteresting part is the directory it is under.
    format!("...{}", &text[text.len() - (width - 3)..])
}

/// A time range in whole seconds, which is the resolution anyone reading a
/// listing cares about, or a dash when the stream has no time column.
fn span(first_ms: u64, last_ms: u64) -> String {
    if first_ms == 0 && last_ms == 0 {
        return "-".to_owned();
    }
    format!("{}s", last_ms.saturating_sub(first_ms) / 1000)
}

#[cfg(test)]
mod tests {
    use umi_file::StreamKind;
    use umi_publish::manifest::{FileEntry, Manifest, Verification};

    use super::{Entry, pair, render, repository};

    #[test]
    fn a_path_that_exists_is_a_path_and_not_a_repository() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().display().to_string();
        assert_eq!(repository(&target, "open-index"), None);
        assert_eq!(repository("./rust-docs", "open-index"), None);
        assert_eq!(repository("/tmp/rust-docs", "open-index"), None);
        // The common typo. A directory that is not there is still a directory,
        // because going to the network to look for `rust-doc` would turn a
        // one line error into a request and a different one line error.
        assert_eq!(repository("rust-doc", "open-index"), None);
    }

    #[test]
    fn a_repository_name_is_taken_as_one() {
        assert_eq!(
            repository("open-index/umi-pages-2026w34-03", "open-index"),
            Some("open-index/umi-pages-2026w34-03".to_owned()),
            "doc 14.6's example"
        );
        assert_eq!(
            repository("umi-focus-blog.rust-lang.org", "open-index"),
            Some("open-index/umi-focus-blog.rust-lang.org".to_owned()),
            "a bare name spelled the way doc 12.4 spells ours gets the org"
        );
        assert_eq!(
            repository("somebody/theirs", "open-index"),
            Some("somebody/theirs".to_owned()),
            "and a name that was spelled out is used as spelled"
        );
    }

    /// A manifest naming one file, with the byte count and the row count a
    /// listing reports.
    fn manifest(day: &str, files: &[(&str, u64, u64)]) -> Manifest {
        let mut manifest = Manifest::new(
            "open-index/umi-pages-2026w34-00",
            day,
            StreamKind::Pages,
            None,
        );
        for (path, rows, bytes) in files {
            manifest.files.push(FileEntry {
                path: (*path).to_owned(),
                bytes: *bytes,
                rows: *rows,
                blake3: [0u8; 32],
                sha256: [0u8; 32],
                segment_ulid: "01K2M8Q0P7R3XN5MTEXAMPLE00".to_owned(),
                coordinator: "test".to_owned(),
                extractor: "umi-extract/0.0.0".to_owned(),
                fetched_at_min_ms: 1_000,
                fetched_at_max_ms: 61_000,
                verification: Verification::default(),
            });
        }
        manifest
    }

    #[test]
    fn a_file_the_hub_holds_at_the_published_size_is_the_only_ok_one() {
        let manifests = vec![manifest(
            "20260817",
            &[
                ("data/20260817/a.parquet", 10, 100),
                ("data/20260817/b.parquet", 20, 200),
                ("data/20260817/c.parquet", 30, 300),
            ],
        )];
        let held = vec![
            ("data/20260817/a.parquet".to_owned(), 100),
            // Short, which is the one thing doc 12.7's first condition is
            // there to catch, and which `ls` reports rather than rules on.
            ("data/20260817/b.parquet".to_owned(), 199),
            // c is not on the hub at all.
            ("data/20260817/d.parquet".to_owned(), 400),
        ];
        let entries = pair(manifests, held);
        let states: Vec<&str> = entries.iter().map(Entry::state).collect();
        assert_eq!(
            states,
            ["ok", "size", "missing", "unnamed"],
            "d is on the hub and in no manifest, and gets a row of its own"
        );
        assert_eq!(entries[3].path, "data/20260817/d.parquet");
        assert_eq!(entries[3].rows, None, "the hub cannot count rows");
        assert_eq!(entries[3].day, "20260817", "and the day comes off the path");
    }

    #[test]
    fn a_repository_with_files_and_no_manifest_still_lists_the_files() {
        // Doc 12.8's case. A publish that got a file up and lost the process
        // before it wrote the manifest leaves exactly this, and "nothing to
        // list" would be the least useful thing to say about it.
        let entries = pair(
            Vec::new(),
            vec![("data/20260817/a.parquet".to_owned(), 100)],
        );
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].state(), "unnamed");
    }

    #[test]
    fn a_file_outside_the_day_layout_has_no_day_to_report() {
        let entries = pair(Vec::new(), vec![("data/loose.parquet".to_owned(), 1)]);
        assert_eq!(entries[0].day, "-");
    }

    #[test]
    fn the_listing_totals_every_day_and_names_what_does_not_line_up() {
        let manifests = vec![
            manifest("20260817", &[("data/20260817/a.parquet", 10, 100)]),
            manifest("20260818", &[("data/20260818/b.parquet", 20, 200)]),
        ];
        let held = vec![("data/20260817/a.parquet".to_owned(), 100)];
        let entries = pair(manifests, held);

        let mut out = Vec::new();
        render("open-index/umi-pages-2026w34-00", &entries, &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();

        assert!(
            text.contains(
                "2 files, 30 rows, 300 bytes over 2 days in open-index/umi-pages-2026w34-00"
            ),
            "{text}"
        );
        assert!(
            text.contains("1 of them do not line up between the manifests and the hub"),
            "{text}"
        );
        assert!(text.contains("60s"), "the span is the fetch range: {text}");
    }

    #[test]
    fn a_listing_that_lines_up_says_nothing_about_what_does_not() {
        let manifests = vec![manifest(
            "20260817",
            &[("data/20260817/a.parquet", 10, 100)],
        )];
        let held = vec![("data/20260817/a.parquet".to_owned(), 100)];
        let entries = pair(manifests, held);

        let mut out = Vec::new();
        render("open-index/umi-focus-example.com", &entries, &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();

        assert!(
            text.contains("1 file, 10 rows, 100 bytes over 1 day in"),
            "{text}"
        );
        assert!(
            !text.contains("umi verify"),
            "the second line is for a repository that does not line up: {text}"
        );
    }
}
