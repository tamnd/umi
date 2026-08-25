//! `umi ls` and `umi cat`, from doc 14.6.
//!
//! Both work on the two file kinds umi produces, the `.umi` segment from doc 10
//! and the Parquet file from doc 12, and both print the same fields for either,
//! because "what is in this thing" should not depend on which side of the
//! publish step you are standing on.
//!
//! Progress and headings go to stderr and rows go to stdout, so that
//! `umi cat ... | head` behaves, which doc 14.9 asks for by name.

use std::io::BufWriter;
use std::path::{Path, PathBuf};

use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::FileReader as _;
use parquet::file::serialized_reader::SerializedFileReader;
use umi_file::Segment;

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

/// `umi ls <dir>`: every segment and every Parquet file under a directory, with
/// row counts, byte counts and time ranges.
///
/// # Errors
///
/// When the directory cannot be read, or a file that looks like a segment is
/// not one. A file that fails to open is reported and does not stop the walk,
/// because the common reason to run `ls` is that something is wrong.
pub fn ls(target: &str) -> Result<(), Error> {
    if !Path::new(target).exists() && target.contains('/') && !target.starts_with('.') {
        // `umi ls open-index/umi-pages-2026w34-03` in doc 14.6. It needs the
        // Hugging Face client, which is the other half of issue #12.
        return Err(Error::NotBuilt(
            "listing a published repository needs the Hugging Face client",
        ));
    }

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
