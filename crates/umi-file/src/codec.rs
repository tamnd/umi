//! The encoding cascade from doc 10.6.
//!
//! One cascade, applied per column chunk, chosen by the writer from a fixed
//! set. Doc 10.6 is explicit that there is no sampling based auto selection:
//! the writer knows the column, the column's encoding is fixed by the schema,
//! and removing the choice removes a class of bug and a chunk of CPU. So
//! [`codec_for`](crate::codec_for) is a lookup and everything here is a pure
//! function of the codec it is handed.
//!
//! Every chunk records its encoding id and version, and a reader that meets an
//! id it does not know fails loudly. Doc 10.6 again: there is no fallback path,
//! because a segment a reader cannot decode is a bug to fix within the hour and
//! not a compatibility problem to route around.
//!
//! # What is not here yet
//!
//! FSST, which doc 10.6 wants for `url`, `final_url`, `links.href`,
//! `links.anchor` and `title`, and which is most of the reason a URL costs 28
//! bytes in doc 10.2's table rather than 120. Those columns take zstd for now,
//! which is the fallback doc 10.10 names for the other direction, and issue 90
//! carries the work with the ratio measured either way.
//!
//! The bit packing here is a plain LSB first layout rather than doc 10.6's
//! FastLanes 1024 value interleaved one. Same bit width, same bytes per value,
//! different order, so it costs decode speed and not size. Doc 10.11 says the
//! format is not stable and no segment survives an upgrade, which is what makes
//! that a safe thing to defer.

use crate::column::{Column, Width};
use crate::layout::Take;
use crate::{Error, Result};

/// Which encoding a chunk carries, written into the shoal directory.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u16)]
pub enum Codec {
    /// Stored as they are. Doc 10.6: digests, minhash and simhash are
    /// uniformly random by construction and any attempt to compress them costs
    /// CPU to produce a slightly larger output.
    Raw = 1,
    /// A dictionary in the chunk header and the codes bit packed. For the small
    /// enums and for `host`, where the values repeat hard.
    Dict = 2,
    /// Frame of reference against the chunk minimum, then bit packing. Doc 10.6
    /// notes that timestamps within a shoal span at most 15 minutes, so
    /// `fetched_at_ms` deltas fit in 20 bits and the column costs about 2.5
    /// bytes a row rather than 8.
    Frame = 3,
    /// zstd level 3 with a dictionary trained per shoal. The only place a
    /// general purpose compressor earns its keep, and for now also the stand in
    /// for FSST.
    Zstd = 4,
}

impl Codec {
    /// Read the id back out of a directory entry.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownCodec`] for anything else, which is doc 10.6's fail
    /// loudly.
    pub const fn from_code(code: u16) -> Result<Self> {
        match code {
            1 => Ok(Self::Raw),
            2 => Ok(Self::Dict),
            3 => Ok(Self::Frame),
            4 => Ok(Self::Zstd),
            other => Err(Error::UnknownCodec(other)),
        }
    }
}

/// How the writer is tuned for compression, from doc 10.6 and doc 10.8.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Compression {
    /// Doc 10.6 says level 3 rather than higher, because doc 01 gives us 2 vCPU
    /// and level 3 compresses at around 300 MB/s per core against level 9 at 25
    /// MB/s, for under 8 percent of ratio on already extracted markdown.
    pub level: i32,
    /// Whether to train a zstd dictionary per chunk. Off, and the bench is why.
    ///
    /// Doc 10.6 says the dictionary is worth more than the level, and doc 10.10
    /// says the compressed `markdown` frame passes through to Parquet byte for
    /// byte and so costs zero CPU to convert. Those two cannot both hold, since
    /// a Parquet page has nowhere to put our dictionary. `benches/segment.rs`
    /// settles it against doc 10.6: over 20000 sample pages the dictionary took
    /// 22131 ms to write against 3929 ms without, and produced a file 3.5
    /// percent *larger*, 983 bytes a page against 949. It loses on both axes.
    /// Our chunks are megabytes of one column, so zstd finds the repetition
    /// inside the frame on its own and the trained dictionary only adds a
    /// header. The training cost alone is 24 percent of a core at 250 pages a
    /// second, which doc 01's 2 vCPU does not have to spare with extraction
    /// wanting most of one.
    pub dictionary: bool,
    /// How much of a chunk to train on. Doc 10.6 says a 2 MiB sample.
    pub sample_bytes: usize,
}

impl Default for Compression {
    fn default() -> Self {
        Self {
            level: 3,
            dictionary: false,
            sample_bytes: 2 * 1024 * 1024,
        }
    }
}

const SHAPE_INTS: u8 = 1;
const SHAPE_BYTES: u8 = 2;
const SHAPE_FIXED: u8 = 3;
const FLAG_VALIDITY: u8 = 1;

/// Not worth training a dictionary on less than this. zstd's trainer wants a
/// spread of samples and gives up on a handful of short ones, and a chunk this
/// small is not where the bytes are anyway.
const MIN_TRAIN_BYTES: usize = 64 * 1024;
const MIN_TRAIN_SAMPLES: usize = 8;

/// Encode one leaf column into the bytes that go on disk.
///
/// # Errors
///
/// [`Error::Unsupported`] when a codec meets a shape it has no rule for, which
/// means [`codec_for`](crate::codec_for) and this file disagree and is a bug
/// rather than a runtime condition.
pub fn encode(column: &Column, codec: Codec, how: Compression) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&(codec as u16).to_le_bytes());
    let shape = match column {
        Column::Ints { .. } => SHAPE_INTS,
        Column::Bytes { .. } => SHAPE_BYTES,
        Column::Fixed { .. } => SHAPE_FIXED,
    };
    out.push(shape);
    let validity = column.validity();
    out.push(if validity.is_some() { FLAG_VALIDITY } else { 0 });
    out.extend_from_slice(
        &u32::try_from(column.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    if let Some(bits) = validity {
        // Doc 10.6: a validity bitmap per chunk, omitted entirely when the
        // chunk has no nulls, which is the common case for most columns.
        let packed = pack_bools(bits);
        out.extend_from_slice(
            &u32::try_from(packed.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        out.extend_from_slice(&packed);
    }

    match column {
        Column::Ints { width, values, .. } => encode_ints(&mut out, *width, values, codec, how),
        Column::Bytes { offsets, data, .. } => encode_bytes(&mut out, offsets, data, codec, how),
        Column::Fixed { size, data, .. } => encode_fixed(&mut out, *size, data, codec, how),
    }?;
    Ok(out)
}

/// Read one leaf column back.
///
/// # Errors
///
/// [`Error::Corrupt`] when the bytes run out, [`Error::UnknownCodec`] for an
/// encoding this build does not have.
pub fn decode(bytes: &[u8]) -> Result<Column> {
    let mut take = Take::new(bytes);
    let codec = Codec::from_code(take.u16()?)?;
    let shape = take.u8()?;
    let flags = take.u8()?;
    let count = take.u32()? as usize;
    let validity = if flags & FLAG_VALIDITY == 0 {
        None
    } else {
        let len = take.u32()? as usize;
        Some(unpack_bools(take.bytes(len)?, count))
    };
    let rest = &bytes[take.rest_from()..];
    match shape {
        SHAPE_INTS => decode_ints(rest, codec, count, validity),
        SHAPE_BYTES => decode_bytes(rest, codec, count, validity),
        SHAPE_FIXED => decode_fixed(rest, codec, count, validity),
        _ => Err(Error::Corrupt("column shape")),
    }
}

fn encode_ints(
    out: &mut Vec<u8>,
    width: Width,
    values: &[u64],
    codec: Codec,
    how: Compression,
) -> Result<()> {
    out.push(width.code());
    match codec {
        Codec::Frame => {
            // Frame of reference against the chunk minimum. A column where
            // every value is the same, which is `version` on receipts and
            // `crawl_profile` on most segments, comes out as the minimum and
            // nothing else.
            let min = values.iter().copied().min().unwrap_or(0);
            let max = values.iter().copied().max().unwrap_or(0);
            let bits = bit_width(max - min);
            out.extend_from_slice(&min.to_le_bytes());
            out.push(bits);
            let shifted: Vec<u64> = values.iter().map(|n| n - min).collect();
            out.extend_from_slice(&pack(&shifted, bits));
            Ok(())
        }
        Codec::Raw => {
            for value in values {
                out.extend_from_slice(&value.to_le_bytes()[..width.code() as usize]);
            }
            Ok(())
        }
        Codec::Dict => {
            let (dict, codes) = dictionary(values);
            out.extend_from_slice(&u32::try_from(dict.len()).unwrap_or(u32::MAX).to_le_bytes());
            for value in &dict {
                out.extend_from_slice(&value.to_le_bytes());
            }
            let bits = bit_width(dict.len().saturating_sub(1) as u64);
            out.push(bits);
            out.extend_from_slice(&pack(&codes, bits));
            Ok(())
        }
        Codec::Zstd => {
            let mut plain = Vec::with_capacity(values.len() * width.code() as usize);
            for value in values {
                plain.extend_from_slice(&value.to_le_bytes()[..width.code() as usize]);
            }
            put_zstd(out, &plain, how, &[]);
            Ok(())
        }
    }
}

fn decode_ints(
    bytes: &[u8],
    codec: Codec,
    count: usize,
    validity: Option<Vec<bool>>,
) -> Result<Column> {
    let mut take = Take::new(bytes);
    let width = Width::from_code(take.u8()?)?;
    let size = width.code() as usize;
    let values = match codec {
        Codec::Frame => {
            let min = take.u64()?;
            let bits = take.u8()?;
            let packed = take.bytes(packed_len(count, bits))?;
            unpack(packed, bits, count)
                .iter()
                .map(|n| n + min)
                .collect()
        }
        Codec::Raw => {
            let raw = take.bytes(count * size)?;
            raw.chunks_exact(size).map(le_u64).collect()
        }
        Codec::Dict => {
            let dict_len = take.u32()? as usize;
            let dict_bytes = take.bytes(dict_len * 8)?;
            let dict: Vec<u64> = dict_bytes
                .as_chunks::<8>()
                .0
                .iter()
                .copied()
                .map(u64::from_le_bytes)
                .collect();
            let bits = take.u8()?;
            let packed = take.bytes(packed_len(count, bits))?;
            unpack(packed, bits, count)
                .iter()
                .map(|code| dict.get(*code as usize).copied().unwrap_or(0))
                .collect()
        }
        Codec::Zstd => {
            let plain = get_zstd(&mut take, &[])?;
            plain.chunks_exact(size).map(le_u64).collect()
        }
    };
    Ok(Column::Ints {
        width,
        values,
        validity,
    })
}

/// Write an offset run as bit packed lengths.
///
/// Lengths and not positions, which is worth spelling out because the obvious
/// thing to write is the offsets themselves and the benchmark says the obvious
/// thing is most of the column. An offset run is monotonic, so its values grow
/// with the size of the whole chunk and the bit width every row pays is the
/// one only the last row needs. On the sample crawl a `links.item.href` chunk
/// holds 55 MB of URLs, which is 26 bits an offset against the 6 bits the
/// longest single URL in it needs, and that came to 162 of the column's 259
/// bytes a page. Lengths make the width a property of the longest value rather
/// than of the chunk, so a column of short strings stays cheap however many
/// rows are in it.
///
/// One length per row and none for the trailing offset, because the trailing
/// offset is the sum of the lengths.
fn put_offsets(out: &mut Vec<u8>, offsets: &[u32]) {
    let lengths: Vec<u64> = offsets
        .windows(2)
        .map(|pair| u64::from(pair[1].saturating_sub(pair[0])))
        .collect();
    let bits = bit_width(lengths.iter().copied().max().unwrap_or(0));
    out.push(bits);
    out.extend_from_slice(&pack(&lengths, bits));
}

/// Read back what [`put_offsets`] wrote, as a prefix sum over the lengths.
fn take_offsets(take: &mut Take<'_>, count: usize) -> Result<Vec<u32>> {
    let bits = take.u8()?;
    let packed = take.bytes(packed_len(count, bits))?;
    let mut offsets = Vec::with_capacity(count + 1);
    let mut at = 0u32;
    offsets.push(at);
    for len in unpack(packed, bits, count) {
        at = at.saturating_add(u32::try_from(len).unwrap_or(u32::MAX));
        offsets.push(at);
    }
    Ok(offsets)
}

fn encode_bytes(
    out: &mut Vec<u8>,
    offsets: &[u32],
    data: &[u8],
    codec: Codec,
    how: Compression,
) -> Result<()> {
    // The offsets take frame of reference plus bit packing whatever the data
    // takes, because they are a monotonic run and nothing else would be
    // sensible. Doc 10.6 puts the same rule on list offsets.
    //
    // The dictionary path does not write them at all. Every row's length is
    // the length of the dictionary entry it points at, so the offsets are a
    // walk over the codes and storing them as well is storing the column
    // twice. On a `host` column of 8192 rows over four distinct values that is
    // 18 KB of offsets against 2 KB of codes, which is the difference between
    // the dictionary earning its keep and not.
    if codec != Codec::Dict {
        put_offsets(out, offsets);
    }

    match codec {
        Codec::Zstd => {
            let dict = train(offsets, data, how);
            out.extend_from_slice(&u32::try_from(dict.len()).unwrap_or(u32::MAX).to_le_bytes());
            out.extend_from_slice(&dict);
            put_zstd(out, data, how, &dict);
            Ok(())
        }
        Codec::Raw => {
            out.extend_from_slice(&u32::try_from(data.len()).unwrap_or(u32::MAX).to_le_bytes());
            out.extend_from_slice(data);
            Ok(())
        }
        Codec::Dict => {
            // The values repeat hard, so the dictionary is most of the column
            // and the codes are a couple of bits a row. `status` in a healthy
            // crawl is 200 more than 90 percent of the time.
            let mut seen: Vec<&[u8]> = Vec::new();
            let mut codes: Vec<u64> = Vec::with_capacity(offsets.len().saturating_sub(1));
            for window in offsets.windows(2) {
                let value = &data[window[0] as usize..window[1] as usize];
                let code = match seen.iter().position(|other| *other == value) {
                    Some(at) => at,
                    None => {
                        seen.push(value);
                        seen.len() - 1
                    }
                };
                codes.push(code as u64);
            }
            out.extend_from_slice(&u32::try_from(seen.len()).unwrap_or(u32::MAX).to_le_bytes());
            let mut at = 0u32;
            for value in &seen {
                at += u32::try_from(value.len()).unwrap_or(0);
                out.extend_from_slice(&at.to_le_bytes());
            }
            for value in &seen {
                out.extend_from_slice(value);
            }
            let bits = bit_width(seen.len().saturating_sub(1) as u64);
            out.push(bits);
            out.extend_from_slice(&pack(&codes, bits));
            Ok(())
        }
        Codec::Frame => Err(Error::Unsupported("frame of reference on bytes")),
    }
}

fn decode_bytes(
    bytes: &[u8],
    codec: Codec,
    count: usize,
    validity: Option<Vec<bool>>,
) -> Result<Column> {
    let mut take = Take::new(bytes);
    // Only the codecs that keep the data laid out row by row carry offsets.
    // The dictionary rebuilds them from its codes below.
    let offsets = if codec == Codec::Dict {
        Vec::new()
    } else {
        take_offsets(&mut take, count)?
    };

    match codec {
        Codec::Zstd => {
            let dict_len = take.u32()? as usize;
            let dict = take.bytes(dict_len)?.to_vec();
            let data = get_zstd(&mut take, &dict)?;
            Ok(Column::Bytes {
                offsets,
                data,
                validity,
            })
        }
        Codec::Raw => {
            let len = take.u32()? as usize;
            let data = take.bytes(len)?.to_vec();
            Ok(Column::Bytes {
                offsets,
                data,
                validity,
            })
        }
        Codec::Dict => {
            let dict_count = take.u32()? as usize;
            let mut ends = Vec::with_capacity(dict_count);
            for _ in 0..dict_count {
                ends.push(take.u32()? as usize);
            }
            let total = ends.last().copied().unwrap_or(0);
            let flat = take.bytes(total)?;
            let bits = take.u8()?;
            let packed = take.bytes(packed_len(count, bits))?;
            let codes = unpack(packed, bits, count);

            let mut data = Vec::new();
            let mut rebuilt = Vec::with_capacity(count + 1);
            rebuilt.push(0u32);
            for code in &codes {
                let at = *code as usize;
                let start = if at == 0 { 0 } else { ends[at - 1] };
                let end = ends.get(at).copied().unwrap_or(start);
                data.extend_from_slice(&flat[start..end]);
                rebuilt.push(u32::try_from(data.len()).unwrap_or(u32::MAX));
            }
            Ok(Column::Bytes {
                offsets: rebuilt,
                data,
                validity,
            })
        }
        Codec::Frame => Err(Error::Unsupported("frame of reference on bytes")),
    }
}

fn encode_fixed(
    out: &mut Vec<u8>,
    size: usize,
    data: &[u8],
    codec: Codec,
    how: Compression,
) -> Result<()> {
    out.extend_from_slice(&u32::try_from(size).unwrap_or(u32::MAX).to_le_bytes());
    match codec {
        Codec::Raw => {
            out.extend_from_slice(data);
            Ok(())
        }
        Codec::Zstd => {
            put_zstd(out, data, how, &[]);
            Ok(())
        }
        Codec::Dict => {
            let mut seen: Vec<&[u8]> = Vec::new();
            let mut codes = Vec::new();
            if size > 0 {
                for value in data.chunks_exact(size) {
                    let code = match seen.iter().position(|other| *other == value) {
                        Some(at) => at,
                        None => {
                            seen.push(value);
                            seen.len() - 1
                        }
                    };
                    codes.push(code as u64);
                }
            }
            out.extend_from_slice(&u32::try_from(seen.len()).unwrap_or(u32::MAX).to_le_bytes());
            for value in &seen {
                out.extend_from_slice(value);
            }
            let bits = bit_width(seen.len().saturating_sub(1) as u64);
            out.push(bits);
            out.extend_from_slice(&pack(&codes, bits));
            Ok(())
        }
        Codec::Frame => Err(Error::Unsupported(
            "frame of reference on fixed width bytes",
        )),
    }
}

fn decode_fixed(
    bytes: &[u8],
    codec: Codec,
    count: usize,
    validity: Option<Vec<bool>>,
) -> Result<Column> {
    let mut take = Take::new(bytes);
    let size = take.u32()? as usize;
    let data = match codec {
        Codec::Raw => take.bytes(count * size)?.to_vec(),
        Codec::Zstd => get_zstd(&mut take, &[])?,
        Codec::Dict => {
            let dict_count = take.u32()? as usize;
            let flat = take.bytes(dict_count * size)?;
            let bits = take.u8()?;
            let packed = take.bytes(packed_len(count, bits))?;
            let mut data = Vec::with_capacity(count * size);
            for code in unpack(packed, bits, count) {
                let at = code as usize * size;
                let value = flat
                    .get(at..at + size)
                    .ok_or(Error::Corrupt("dictionary code"))?;
                data.extend_from_slice(value);
            }
            data
        }
        Codec::Frame => {
            return Err(Error::Unsupported(
                "frame of reference on fixed width bytes",
            ));
        }
    };
    Ok(Column::Fixed {
        size,
        data,
        validity,
    })
}

fn dictionary(values: &[u64]) -> (Vec<u64>, Vec<u64>) {
    let mut dict: Vec<u64> = Vec::new();
    let mut codes: Vec<u64> = Vec::with_capacity(values.len());
    for value in values {
        let code = match dict.iter().position(|other| other == value) {
            Some(at) => at,
            None => {
                dict.push(*value);
                dict.len() - 1
            }
        };
        codes.push(code as u64);
    }
    (dict, codes)
}

/// Train a zstd dictionary over a sample of the chunk's values.
///
/// Returns an empty dictionary rather than an error when there is not enough to
/// train on, because a small chunk is not where the bytes are and a chunk that
/// cannot be dictionary compressed still has to be written.
fn train(offsets: &[u32], data: &[u8], how: Compression) -> Vec<u8> {
    if !how.dictionary || data.len() < MIN_TRAIN_BYTES {
        return Vec::new();
    }
    let mut sizes: Vec<usize> = Vec::new();
    let mut sampled = 0usize;
    for window in offsets.windows(2) {
        let len = (window[1] - window[0]) as usize;
        if len == 0 {
            continue;
        }
        sizes.push(len);
        sampled += len;
        if sampled >= how.sample_bytes {
            break;
        }
    }
    if sizes.len() < MIN_TRAIN_SAMPLES {
        return Vec::new();
    }
    // The samples are the first `sampled` bytes of the data by construction,
    // because the values are stored end to end in offset order.
    zstd::dict::from_continuous(&data[..sampled], &sizes, 110 * 1024).unwrap_or_default()
}

fn put_zstd(out: &mut Vec<u8>, data: &[u8], how: Compression, dict: &[u8]) {
    let compressed = if dict.is_empty() {
        zstd::bulk::compress(data, how.level).unwrap_or_default()
    } else {
        zstd::bulk::Compressor::with_dictionary(how.level, dict)
            .and_then(|mut c| c.compress(data))
            .unwrap_or_default()
    };
    // A compressor that failed leaves nothing behind, and storing the plain
    // bytes is better than losing the shoal. The flag says which happened.
    let stored_plain = compressed.is_empty() && !data.is_empty();
    out.push(u8::from(stored_plain));
    out.extend_from_slice(&u32::try_from(data.len()).unwrap_or(u32::MAX).to_le_bytes());
    let body = if stored_plain { data } else { &compressed };
    out.extend_from_slice(&u32::try_from(body.len()).unwrap_or(u32::MAX).to_le_bytes());
    out.extend_from_slice(body);
}

fn get_zstd(take: &mut Take<'_>, dict: &[u8]) -> Result<Vec<u8>> {
    let stored_plain = take.u8()? == 1;
    let raw_len = take.u32()? as usize;
    let body_len = take.u32()? as usize;
    let body = take.bytes(body_len)?;
    if stored_plain {
        return Ok(body.to_vec());
    }
    let out = if dict.is_empty() {
        zstd::bulk::decompress(body, raw_len)
    } else {
        zstd::bulk::Decompressor::with_dictionary(dict)
            .and_then(|mut d| d.decompress(body, raw_len))
    };
    let out = out.map_err(|_| Error::Corrupt("zstd frame"))?;
    if out.len() != raw_len {
        return Err(Error::Corrupt("decompressed to the wrong length"));
    }
    Ok(out)
}

/// How many bits it takes to hold every value up to and including this one.
#[must_use]
pub const fn bit_width(max: u64) -> u8 {
    if max == 0 {
        0
    } else {
        (64 - max.leading_zeros()) as u8
    }
}

const fn packed_len(count: usize, bits: u8) -> usize {
    (count * bits as usize).div_ceil(8)
}

/// Bit pack, least significant bit first, at an arbitrary width.
///
/// A width of zero writes nothing, which is the right answer for a column where
/// every value equals the frame of reference minimum.
#[must_use]
pub fn pack(values: &[u64], bits: u8) -> Vec<u8> {
    if bits == 0 {
        return Vec::new();
    }
    let mut out = vec![0u8; packed_len(values.len(), bits)];
    let mut at = 0usize;
    for value in values {
        let mut value = *value;
        let mut left = usize::from(bits);
        while left > 0 {
            let byte = at / 8;
            let offset = at % 8;
            let take = (8 - offset).min(left);
            let mask = if take == 8 {
                u8::MAX
            } else {
                (1u8 << take) - 1
            };
            out[byte] |= ((value as u8) & mask) << offset;
            value >>= take;
            left -= take;
            at += take;
        }
    }
    out
}

/// The inverse of [`pack`].
#[must_use]
pub fn unpack(bytes: &[u8], bits: u8, count: usize) -> Vec<u64> {
    if bits == 0 {
        return vec![0u64; count];
    }
    let mut out = Vec::with_capacity(count);
    let mut at = 0usize;
    for _ in 0..count {
        let mut value = 0u64;
        let mut got = 0usize;
        while got < usize::from(bits) {
            let byte = at / 8;
            let offset = at % 8;
            let take = (8 - offset).min(usize::from(bits) - got);
            let mask = if take == 8 {
                u8::MAX
            } else {
                (1u8 << take) - 1
            };
            let piece = u64::from((bytes.get(byte).copied().unwrap_or(0) >> offset) & mask);
            value |= piece << got;
            got += take;
            at += take;
        }
        out.push(value);
    }
    out
}

fn pack_bools(bits: &[bool]) -> Vec<u8> {
    let mut out = vec![0u8; bits.len().div_ceil(8)];
    for (i, set) in bits.iter().enumerate() {
        if *set {
            out[i / 8] |= 1 << (i % 8);
        }
    }
    out
}

fn unpack_bools(bytes: &[u8], count: usize) -> Vec<bool> {
    (0..count)
        .map(|i| bytes.get(i / 8).copied().unwrap_or(0) & (1 << (i % 8)) != 0)
        .collect()
}

fn le_u64(bytes: &[u8]) -> u64 {
    let mut out = [0u8; 8];
    out[..bytes.len().min(8)].copy_from_slice(&bytes[..bytes.len().min(8)]);
    u64::from_le_bytes(out)
}

#[cfg(test)]
mod tests {
    use super::{Codec, Compression, bit_width, decode, encode, pack, unpack};
    use crate::column::{Column, Width};

    fn round_trip(column: &Column, codec: Codec) {
        let bytes = encode(column, codec, Compression::default()).expect("encode");
        let back = decode(&bytes).expect("decode");
        assert_eq!(&back, column, "{codec:?}");
    }

    #[test]
    fn bit_packing_survives_every_width() {
        for bits in 0..=64u8 {
            let cap = if bits == 64 {
                u64::MAX
            } else {
                (1u64 << bits) - 1
            };
            let values: Vec<u64> = (0..37u64)
                .map(|n| n.wrapping_mul(2_654_435_761) & cap)
                .collect();
            let packed = pack(&values, bits);
            assert_eq!(unpack(&packed, bits, values.len()), values, "width {bits}");
        }
    }

    #[test]
    fn a_column_of_one_repeated_value_costs_nothing_to_store() {
        assert_eq!(bit_width(0), 0);
        let column = Column::Ints {
            width: Width::U32,
            values: vec![7; 4096],
            validity: None,
        };
        let bytes = encode(&column, Codec::Frame, Compression::default()).expect("encode");
        // Header, width, minimum, bit width, and no packed body at all.
        assert!(
            bytes.len() < 32,
            "{} bytes for a constant column",
            bytes.len()
        );
        assert_eq!(decode(&bytes).expect("decode"), column);
    }

    #[test]
    fn integers_round_trip_under_every_codec_that_takes_them() {
        let column = Column::Ints {
            width: Width::U64,
            values: vec![
                1_760_000_000_000,
                1_760_000_000_050,
                1_760_000_900_000,
                1_760_000_000_001,
            ],
            validity: None,
        };
        for codec in [Codec::Frame, Codec::Raw, Codec::Dict, Codec::Zstd] {
            round_trip(&column, codec);
        }
    }

    #[test]
    fn strings_round_trip_and_an_empty_value_is_not_a_null() {
        let mut data = Vec::new();
        let mut offsets = vec![0u32];
        for value in ["https://a.example/", "", "https://b.example/x"] {
            data.extend_from_slice(value.as_bytes());
            offsets.push(u32::try_from(data.len()).expect("in range"));
        }
        let column = Column::Bytes {
            offsets,
            data,
            validity: Some(vec![true, true, false]),
        };
        for codec in [Codec::Zstd, Codec::Raw, Codec::Dict] {
            round_trip(&column, codec);
        }
    }

    #[test]
    fn fixed_width_bytes_round_trip() {
        let column = Column::Fixed {
            size: 4,
            data: (0..64u8).collect(),
            validity: None,
        };
        for codec in [Codec::Raw, Codec::Zstd, Codec::Dict] {
            round_trip(&column, codec);
        }
    }

    #[test]
    fn a_zero_row_column_round_trips() {
        // A shoal with no rows in it never gets written, but a projection can
        // ask for a column of a stream that has none of that shape and the
        // encoder should not be the thing that notices.
        for column in [
            Column::Ints {
                width: Width::U8,
                values: Vec::new(),
                validity: None,
            },
            Column::Bytes {
                offsets: vec![0],
                data: Vec::new(),
                validity: None,
            },
            Column::Fixed {
                size: 32,
                data: Vec::new(),
                validity: None,
            },
        ] {
            for codec in [Codec::Raw, Codec::Zstd, Codec::Dict] {
                if matches!(column, Column::Ints { .. }) && codec == Codec::Zstd {
                    continue;
                }
                round_trip(&column, codec);
            }
        }
    }

    #[test]
    fn the_dictionary_earns_its_keep_on_repeating_text() {
        // Doc 10.6's claim about `host`: the values repeat hard, so the
        // dictionary is most of the column and the codes are a couple of bits.
        let mut data = Vec::new();
        let mut offsets = vec![0u32];
        for i in 0..8192 {
            data.extend_from_slice(format!("host{}.example.com", i % 4).as_bytes());
            offsets.push(u32::try_from(data.len()).expect("in range"));
        }
        let logical = data.len();
        let column = Column::Bytes {
            offsets,
            data,
            validity: None,
        };
        let dict = encode(&column, Codec::Dict, Compression::default()).expect("encode");
        assert!(
            dict.len() * 20 < logical,
            "{} bytes against {logical} logical",
            dict.len()
        );
        assert_eq!(decode(&dict).expect("decode"), column);
    }
}
