//! Arrow arrays in, leaf columns out, and back again.
//!
//! Doc 10.6 says lists and maps decompose into an offsets column and child
//! columns encoded by their own rule, and that `links` becomes four sibling
//! child columns rather than an interleaved struct, which is what lets
//! `links.href` share one symbol table across every link in the shoal. So
//! nothing nested reaches the encoder. A `RecordBatch` is taken apart into a
//! flat list of leaves on the way in and put back together on the way out, and
//! the encoder only ever sees three shapes: integers, variable length bytes,
//! and fixed width bytes.
//!
//! The names carry the path, so `links` becomes `links.offsets`, `links.href`,
//! `links.anchor`, `links.rel` and `links.kind`. That is what appears in the
//! shoal directory, and it is why a projection in
//! [`ShoalReader::to_arrow`](crate::ShoalReader::to_arrow) can skip the whole
//! of `markdown` without touching anything else.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, ListArray, MapArray, RecordBatch, StringArray,
    StructArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, FieldRef, SchemaRef};

use crate::{Error, Result};

/// How wide an integer column is, so the reader can narrow it back to the
/// Arrow type it came from.
///
/// Everything is widened to `u64` for encoding, because frame of reference and
/// bit packing do not care how many bytes the value used to occupy and one
/// code path is better than four.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Width {
    /// One byte.
    U8,
    /// Two bytes.
    U16,
    /// Four bytes.
    U32,
    /// Eight bytes.
    U64,
}

impl Width {
    pub(crate) const fn code(self) -> u8 {
        match self {
            Self::U8 => 1,
            Self::U16 => 2,
            Self::U32 => 4,
            Self::U64 => 8,
        }
    }

    pub(crate) const fn from_code(code: u8) -> Result<Self> {
        match code {
            1 => Ok(Self::U8),
            2 => Ok(Self::U16),
            4 => Ok(Self::U32),
            8 => Ok(Self::U64),
            _ => Err(Error::Corrupt("integer width")),
        }
    }
}

/// One leaf column, in the only three shapes the encoder handles.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Column {
    /// Fixed width integers widened to `u64`.
    Ints {
        /// The width to narrow back to on the way out.
        width: Width,
        /// One per row. A null row carries whatever the builder left there,
        /// which the validity bitmap says to ignore.
        values: Vec<u64>,
        /// Present only when something is null, which doc 10.6 says is the
        /// uncommon case for most columns.
        validity: Option<Vec<bool>>,
    },
    /// Variable length bytes: an offsets array of `rows + 1` entries and the
    /// concatenated data.
    Bytes {
        /// `rows + 1` entries, starting at zero.
        offsets: Vec<u32>,
        /// The values, end to end.
        data: Vec<u8>,
        /// See [`Column::Ints::validity`].
        validity: Option<Vec<bool>>,
    },
    /// Fixed width bytes, which is every digest and the minhash.
    Fixed {
        /// How wide each value is.
        size: usize,
        /// `rows * size` bytes.
        data: Vec<u8>,
        /// See [`Column::Ints::validity`].
        validity: Option<Vec<bool>>,
    },
}

impl Column {
    /// How many values there are.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Ints { values, .. } => values.len(),
            Self::Bytes { offsets, .. } => offsets.len().saturating_sub(1),
            Self::Fixed { size, data, .. } => {
                if *size == 0 {
                    0
                } else {
                    data.len() / size
                }
            }
        }
    }

    /// Whether there are no values.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many values are null.
    #[must_use]
    pub fn nulls(&self) -> usize {
        self.validity()
            .map_or(0, |bits| bits.iter().filter(|set| !**set).count())
    }

    /// How many bytes this column held before encoding, for the ratio in
    /// [`SegmentStats`](crate::SegmentStats).
    #[must_use]
    pub fn logical_bytes(&self) -> usize {
        match self {
            Self::Ints { width, values, .. } => values.len() * width.code() as usize,
            Self::Bytes { offsets, data, .. } => data.len() + offsets.len() * 4,
            Self::Fixed { data, .. } => data.len(),
        }
    }

    pub(crate) fn validity(&self) -> Option<&Vec<bool>> {
        match self {
            Self::Ints { validity, .. }
            | Self::Bytes { validity, .. }
            | Self::Fixed { validity, .. } => validity.as_ref(),
        }
    }
}

/// A leaf column with the name it goes into the directory under.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Leaf {
    /// The dotted path, such as `links.href`.
    pub name: String,
    /// The Arrow type it came from, which is what picks the codec.
    pub ty: DataType,
    /// The values.
    pub data: Column,
}

/// Take a batch apart into leaf columns, in schema order.
///
/// # Errors
///
/// [`Error::Unsupported`] for an Arrow type none of the three schemas use. That
/// is a schema change that forgot this file rather than a runtime condition.
pub fn flatten(batch: &RecordBatch) -> Result<Vec<Leaf>> {
    let mut out = Vec::new();
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        push_leaves(field.name(), field, array, &mut out)?;
    }
    Ok(out)
}

/// The leaf names a schema produces, without needing any data.
///
/// The writer puts these in the footer and the reader checks a file against
/// them before it decodes anything, which turns a schema mismatch into a
/// refusal at open time rather than into a wrong answer three columns in.
#[must_use]
pub fn leaf_names(schema: &SchemaRef) -> Vec<(String, DataType)> {
    let mut out = Vec::new();
    for field in schema.fields() {
        push_names(field.name(), field.data_type(), &mut out);
    }
    out
}

fn push_names(name: &str, ty: &DataType, out: &mut Vec<(String, DataType)>) {
    match ty {
        DataType::List(item) => {
            out.push((format!("{name}.offsets"), DataType::UInt32));
            push_names(&format!("{name}.{}", item.name()), item.data_type(), out);
        }
        DataType::Struct(fields) => {
            for field in fields {
                push_names(&format!("{name}.{}", field.name()), field.data_type(), out);
            }
        }
        DataType::Map(entries, _) => {
            out.push((format!("{name}.offsets"), DataType::UInt32));
            push_names(name, entries.data_type(), out);
        }
        other => out.push((name.to_owned(), other.clone())),
    }
}

fn push_leaves(name: &str, field: &FieldRef, array: &ArrayRef, out: &mut Vec<Leaf>) -> Result<()> {
    match field.data_type() {
        DataType::List(item) => {
            let list = downcast::<ListArray>(array, "list")?;
            let (offsets, child) = split_list(list);
            out.push(Leaf {
                name: format!("{name}.offsets"),
                ty: DataType::UInt32,
                data: Column::Ints {
                    width: Width::U32,
                    values: offsets.iter().map(|n| u64::from(*n)).collect(),
                    validity: bits(list.nulls(), list.len()),
                },
            });
            push_leaves(&format!("{name}.{}", item.name()), item, &child, out)
        }
        DataType::Struct(fields) => {
            let structs = downcast::<StructArray>(array, "struct")?;
            for (field, child) in fields.iter().zip(structs.columns()) {
                push_leaves(&format!("{name}.{}", field.name()), field, child, out)?;
            }
            Ok(())
        }
        DataType::Map(entries, _) => {
            let map = downcast::<MapArray>(array, "map")?;
            let (offsets, child) = split_map(map);
            out.push(Leaf {
                name: format!("{name}.offsets"),
                ty: DataType::UInt32,
                data: Column::Ints {
                    width: Width::U32,
                    values: offsets.iter().map(|n| u64::from(*n)).collect(),
                    validity: bits(map.nulls(), map.len()),
                },
            });
            push_leaves(name, entries, &child, out)
        }
        other => {
            out.push(Leaf {
                name: name.to_owned(),
                ty: other.clone(),
                data: leaf_column(array, other)?,
            });
            Ok(())
        }
    }
}

fn leaf_column(array: &ArrayRef, ty: &DataType) -> Result<Column> {
    let validity = bits(array.nulls(), array.len());
    match ty {
        DataType::UInt8 => Ok(ints(
            downcast::<UInt8Array>(array, "u8")?
                .values()
                .iter()
                .map(|n| u64::from(*n)),
            Width::U8,
            validity,
        )),
        DataType::UInt16 => Ok(ints(
            downcast::<UInt16Array>(array, "u16")?
                .values()
                .iter()
                .map(|n| u64::from(*n)),
            Width::U16,
            validity,
        )),
        DataType::UInt32 => Ok(ints(
            downcast::<UInt32Array>(array, "u32")?
                .values()
                .iter()
                .map(|n| u64::from(*n)),
            Width::U32,
            validity,
        )),
        DataType::UInt64 => Ok(ints(
            downcast::<UInt64Array>(array, "u64")?
                .values()
                .iter()
                .copied(),
            Width::U64,
            validity,
        )),
        DataType::Utf8 => {
            let strings = downcast::<StringArray>(array, "utf8")?;
            let mut offsets = Vec::with_capacity(strings.len() + 1);
            let mut data = Vec::new();
            offsets.push(0u32);
            for i in 0..strings.len() {
                // A null carries an empty value here, and the validity bitmap
                // is what says the difference between null and empty string.
                if strings.is_valid(i) {
                    data.extend_from_slice(strings.value(i).as_bytes());
                }
                offsets.push(u32::try_from(data.len()).unwrap_or(u32::MAX));
            }
            Ok(Column::Bytes {
                offsets,
                data,
                validity,
            })
        }
        DataType::FixedSizeBinary(size) => {
            let fixed = downcast::<FixedSizeBinaryArray>(array, "fixed")?;
            let size = usize::try_from(*size).map_err(|_| Error::Unsupported("negative width"))?;
            let mut data = Vec::with_capacity(fixed.len() * size);
            for i in 0..fixed.len() {
                if fixed.is_valid(i) {
                    data.extend_from_slice(fixed.value(i));
                } else {
                    data.resize(data.len() + size, 0);
                }
            }
            Ok(Column::Fixed {
                size,
                data,
                validity,
            })
        }
        other => Err(Error::Unsupported(leak(other))),
    }
}

fn ints(values: impl Iterator<Item = u64>, width: Width, validity: Option<Vec<bool>>) -> Column {
    Column::Ints {
        width,
        values: values.collect(),
        validity,
    }
}

/// Rebase a list's offsets to start at zero and slice its child to match.
///
/// Arrow lets an array be a view into a larger one, so a `ListArray` that came
/// out of a sliced batch has offsets that do not start at zero and a child that
/// is longer than the offsets reach. Writing those out as they are would store
/// whatever the neighbours had, so the rebase is not tidiness.
fn split_list(list: &ListArray) -> (Vec<u32>, ArrayRef) {
    let raw = list.value_offsets();
    let start = raw.first().copied().unwrap_or(0);
    let end = raw.last().copied().unwrap_or(0);
    let offsets: Vec<u32> = raw
        .iter()
        .map(|n| u32::try_from(*n - start).unwrap_or(0))
        .collect();
    let len = usize::try_from(end - start).unwrap_or(0);
    let child = list
        .values()
        .slice(usize::try_from(start).unwrap_or(0), len);
    (offsets, child)
}

fn split_map(map: &MapArray) -> (Vec<u32>, ArrayRef) {
    let raw = map.value_offsets();
    let start = raw.first().copied().unwrap_or(0);
    let end = raw.last().copied().unwrap_or(0);
    let offsets: Vec<u32> = raw
        .iter()
        .map(|n| u32::try_from(*n - start).unwrap_or(0))
        .collect();
    let len = usize::try_from(end - start).unwrap_or(0);
    let entries: ArrayRef = Arc::new(map.entries().clone());
    let child = entries.slice(usize::try_from(start).unwrap_or(0), len);
    (offsets, child)
}

fn bits(nulls: Option<&NullBuffer>, len: usize) -> Option<Vec<bool>> {
    let nulls = nulls?;
    if nulls.null_count() == 0 {
        return None;
    }
    Some((0..len).map(|i| nulls.is_valid(i)).collect())
}

fn downcast<'a, T: 'static>(array: &'a ArrayRef, what: &'static str) -> Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or(Error::Unsupported(what))
}

/// Name a type in an error without allocating a `String` the error type would
/// have to own. The set of types is closed, so anything that reaches here is a
/// schema change and the exact name matters less than that it failed.
fn leak(ty: &DataType) -> &'static str {
    match ty {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "signed integer",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "float",
        DataType::Binary | DataType::LargeBinary => "binary",
        DataType::LargeUtf8 => "large utf8",
        _ => "type",
    }
}

/// Put the leaves back together into a batch, in schema order.
///
/// `wanted` is the top level column subset, or every column when it is empty.
/// A column that was projected away is simply absent from `leaves`, which is
/// how [`ShoalReader::to_arrow`](crate::ShoalReader::to_arrow) avoids paying
/// for `markdown` when all it wanted was a status count.
///
/// # Errors
///
/// [`Error::Corrupt`] when a leaf the schema calls for is missing or has the
/// wrong shape, which after a crash means the directory and the data disagree.
pub fn unflatten(
    schema: &SchemaRef,
    leaves: &mut dyn FnMut(&str) -> Option<Column>,
    wanted: &[&str],
    rows: usize,
) -> Result<RecordBatch> {
    let mut fields: Vec<FieldRef> = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for field in schema.fields() {
        if !wanted.is_empty() && !wanted.contains(&field.name().as_str()) {
            continue;
        }
        let array = build(field.name(), field, leaves, rows)?;
        fields.push(Arc::clone(field));
        arrays.push(array);
    }
    let projected = Arc::new(arrow::datatypes::Schema::new(fields));
    if arrays.is_empty() {
        let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(rows));
        return RecordBatch::try_new_with_options(projected, arrays, &options)
            .map_err(|_| Error::Corrupt("empty projection"));
    }
    RecordBatch::try_new(projected, arrays).map_err(|_| Error::Corrupt("column lengths disagree"))
}

fn build(
    name: &str,
    field: &FieldRef,
    leaves: &mut dyn FnMut(&str) -> Option<Column>,
    rows: usize,
) -> Result<ArrayRef> {
    match field.data_type() {
        DataType::List(item) => {
            let (offsets, nulls) = offsets_of(&format!("{name}.offsets"), leaves)?;
            let child_rows = offsets.last().copied().unwrap_or(0) as usize;
            let child = build(&format!("{name}.{}", item.name()), item, leaves, child_rows)?;
            let offsets = OffsetBuffer::new(ScalarBuffer::from(
                offsets.iter().map(|n| *n as i32).collect::<Vec<i32>>(),
            ));
            Ok(Arc::new(
                ListArray::try_new(Arc::clone(item), offsets, child, nulls)
                    .map_err(|_| Error::Corrupt("list offsets"))?,
            ))
        }
        DataType::Struct(struct_fields) => {
            let mut children: Vec<ArrayRef> = Vec::with_capacity(struct_fields.len());
            for child in struct_fields {
                children.push(build(
                    &format!("{name}.{}", child.name()),
                    child,
                    leaves,
                    rows,
                )?);
            }
            Ok(Arc::new(
                StructArray::try_new(struct_fields.clone(), children, None)
                    .map_err(|_| Error::Corrupt("struct children"))?,
            ))
        }
        DataType::Map(entries, sorted) => {
            let (offsets, nulls) = offsets_of(&format!("{name}.offsets"), leaves)?;
            let child_rows = offsets.last().copied().unwrap_or(0) as usize;
            let child = build(name, entries, leaves, child_rows)?;
            let entries_array = child
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or(Error::Corrupt("map entries"))?
                .clone();
            let offsets = OffsetBuffer::new(ScalarBuffer::from(
                offsets.iter().map(|n| *n as i32).collect::<Vec<i32>>(),
            ));
            Ok(Arc::new(
                MapArray::try_new(Arc::clone(entries), offsets, entries_array, nulls, *sorted)
                    .map_err(|_| Error::Corrupt("map offsets"))?,
            ))
        }
        other => {
            let column = leaves(name).ok_or(Error::Corrupt("missing column"))?;
            leaf_array(other, column, rows)
        }
    }
}

fn offsets_of(
    name: &str,
    leaves: &mut dyn FnMut(&str) -> Option<Column>,
) -> Result<(Vec<u32>, Option<NullBuffer>)> {
    let Some(Column::Ints {
        values, validity, ..
    }) = leaves(name)
    else {
        return Err(Error::Corrupt("missing offsets"));
    };
    let offsets: Vec<u32> = values
        .iter()
        .map(|n| u32::try_from(*n).unwrap_or(u32::MAX))
        .collect();
    // The offsets column carries one more entry than there are lists, so its
    // validity is one short by construction and is dropped rather than
    // reconstructed. Every list in the three schemas is non null.
    let nulls = validity.map(|bits| NullBuffer::from(&bits[..bits.len().saturating_sub(1)]));
    Ok((offsets, nulls))
}

fn leaf_array(ty: &DataType, column: Column, rows: usize) -> Result<ArrayRef> {
    let nulls = column.validity().map(|bits| NullBuffer::from(&bits[..]));
    match (ty, column) {
        (DataType::UInt8, Column::Ints { values, .. }) => {
            let narrowed: Vec<u8> = values.iter().map(|n| *n as u8).collect();
            Ok(Arc::new(UInt8Array::new(
                ScalarBuffer::from(narrowed),
                nulls,
            )))
        }
        (DataType::UInt16, Column::Ints { values, .. }) => {
            let narrowed: Vec<u16> = values.iter().map(|n| *n as u16).collect();
            Ok(Arc::new(UInt16Array::new(
                ScalarBuffer::from(narrowed),
                nulls,
            )))
        }
        (DataType::UInt32, Column::Ints { values, .. }) => {
            let narrowed: Vec<u32> = values.iter().map(|n| *n as u32).collect();
            Ok(Arc::new(UInt32Array::new(
                ScalarBuffer::from(narrowed),
                nulls,
            )))
        }
        (DataType::UInt64, Column::Ints { values, .. }) => Ok(Arc::new(UInt64Array::new(
            ScalarBuffer::from(values),
            nulls,
        ))),
        (DataType::Utf8, Column::Bytes { offsets, data, .. }) => {
            let offsets = OffsetBuffer::new(ScalarBuffer::from(
                offsets.iter().map(|n| *n as i32).collect::<Vec<i32>>(),
            ));
            StringArray::try_new(offsets, Buffer::from_vec(data), nulls)
                .map(|array| Arc::new(array) as ArrayRef)
                .map_err(|_| Error::Corrupt("string offsets"))
        }
        (DataType::FixedSizeBinary(size), Column::Fixed { data, .. }) => {
            FixedSizeBinaryArray::try_new(*size, Buffer::from_vec(data), nulls)
                .map(|array| Arc::new(array) as ArrayRef)
                .map_err(|_| Error::Corrupt("fixed width column"))
        }
        (_, column) => {
            let _ = (rows, column);
            Err(Error::Corrupt("column shape does not match the schema"))
        }
    }
}
