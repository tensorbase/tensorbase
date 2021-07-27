use std::borrow::{Borrow, BorrowMut};
use std::fmt;
use std::io::Write;
use std::mem::{align_of, size_of};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::slice::Iter;
use std::str::from_utf8;

use chrono::{Date, DateTime, Utc};
use tokio::io::{AsyncBufRead, AsyncReadExt};
use uuid::Uuid;

use super::block::{BlockColumn, ServerBlock};
use super::decoder::ValueReader;
#[cfg(feature = "int128")]
use super::value::ValueDecimal128;
use super::value::{
    ValueDate, ValueDateTime, ValueDateTime64, ValueDecimal32, ValueDecimal64, ValueIp4,
    ValueIp6, ValueUuid,
};
use crate::protocol::encoder::Encoder;

use super::{Value, ValueRefEnum};
use crate::errors::{ConversionError, DriverError, Result};
#[cfg(feature = "int128")]
use crate::types::Decimal128;
use crate::types::{Decimal, Decimal32, Decimal64, Field, FieldMeta, SqlType};

use std::cmp::Ordering;
use std::fmt::Formatter;

macro_rules! err {
    ($err: expr) => {
        Err($err.into())
    };
}

pub trait AsOutColumn {
    /// Returns the number of rows
    fn len(&self) -> usize;
    /// Encode column into Clickhouse native format
    fn encode(&self, field: &Field, writer: &mut dyn Write) -> Result<()>;
    /// Check before transmission if the Clickhouse column data
    /// type compatible with provided and can be conceived by the server
    fn is_compatible(&self, field: &Field) -> bool;
}

/// Convert received from Clickhouse server data to rust type
pub trait AsInColumn: Send {
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_>;
    unsafe fn into_bytes(&mut self) -> Vec<u8>;
    fn offset_map(&self) -> Option<Vec<u32>> {
        None
    }
}

/// Default implementation returns `Null` data
impl AsInColumn for () {
    unsafe fn get_at(&self, _: u64) -> ValueRef<'_> {
        ValueRef { inner: None }
    }

    unsafe fn into_bytes(&mut self) -> Vec<u8> {
        vec![]
    }
}

/// Output block column
pub struct ColumnDataAdapter<'b> {
    /// Clickhouse column name
    pub(crate) name: &'b str,
    /// Nullable, Array, LowCardinality
    pub(crate) flag: u8,
    pub(crate) data: Box<dyn AsOutColumn + 'b>,
}

/// Hold reference (or value for discreet data types ) to Clickhouse row values
/// Option::None value indicate null
#[derive(Debug)]
pub struct ValueRef<'a> {
    // TODO: remove one level of indirection. Combine ValueRef and ValueRefEnum into single struct
    inner: Option<ValueRefEnum<'a>>,
}

impl<'a> ValueRef<'a> {
    pub fn into_inner(self) -> Option<ValueRefEnum<'a>> {
        self.inner
    }
}

/// Implement SqlType::DateTime,SqlType::DateTime64-> chrono::DateTime<Utc> data conversion
impl<'a> Value<'a, DateTime<Utc>> for ValueRef<'a> {
    fn get(&'a self, field: &'a Field) -> Result<Option<DateTime<Utc>>> {
        match self.inner {
            Some(ValueRefEnum::DateTime(v)) => Ok(Some(v.to_datetime())),
            Some(ValueRefEnum::DateTime64(v)) => {
                if let SqlType::DateTime64(p, _) = field.sql_type {
                    Ok(Some(v.to_datetime(p)))
                } else {
                    // TODO: Apparently not reachable. Replace it with notreachable!
                    err!(ConversionError::UnsupportedConversion)
                }
            }
            Some(ValueRefEnum::Date(v)) => {
                let d = v.to_date();
                Ok(Some(d.and_hms(0, 0, 0)))
            }
            _ => err!(ConversionError::UnsupportedConversion),
        }
    }
}

/// Implement SqlType::Date -> chrono::Date<Utc> data conversion
impl<'a> Value<'a, Date<Utc>> for ValueRef<'a> {
    fn get(&'a self, field: &'a Field) -> Result<Option<Date<Utc>>> {
        match self.inner {
            Some(ValueRefEnum::DateTime(v)) => Ok(Some(v.to_datetime().date())),
            Some(ValueRefEnum::DateTime64(v)) => {
                if let SqlType::DateTime64(p, _) = field.sql_type {
                    Ok(Some(v.to_datetime(p).date()))
                } else {
                    // TODO: Apparently not reachable. Replace it with notreachable!
                    err!(ConversionError::UnsupportedConversion)
                }
            }
            Some(ValueRefEnum::Date(v)) => {
                let d = v.to_date();
                Ok(Some(d))
            }
            _ => err!(ConversionError::UnsupportedConversion),
        }
    }
}

/// Implement SqlType::Enum(x)|String|FixedSring(size)->&str data conversion
impl<'a> Value<'a, &'a str> for ValueRef<'a> {
    fn get(&'a self, field: &'a Field) -> Result<Option<&'_ str>> {
        match self.inner {
            Some(ValueRefEnum::String(v)) => Ok(Some(from_utf8(v)?)),
            Some(ValueRefEnum::Enum(v)) => {
                // Convert Enum value to Enum title using metadata
                let meta = field.get_meta().expect("corrupted enum index");
                let title = v.transcode(meta);
                Ok(Some(from_utf8(title)?))
            }
            None => Ok(None),
            _ => err!(ConversionError::UnsupportedConversion),
        }
    }
}

/// Implement SqlType::Enum(x)|String|FixedSring(size) - > &[u8]
impl<'a> Value<'a, &'a [u8]> for ValueRef<'a> {
    fn get(&'a self, field: &'a Field) -> Result<Option<&'_ [u8]>> {
        match self.inner {
            Some(ValueRefEnum::String(v)) => Ok(Some(v)),
            Some(ValueRefEnum::Enum(v)) => {
                let meta = field.get_meta().expect("corrupted enum index");
                let title = v.transcode(meta);
                Ok(Some(title))
            }
            Some(ValueRefEnum::Array8(v)) => Ok(Some(v)),
            _ => err!(ConversionError::UnsupportedConversion),
        }
    }
}

impl<'a> Value<'a, i16> for ValueRef<'a> {
    #[allow(clippy::unnested_or_patterns)]
    fn get(&'a self, _: &'a Field) -> Result<Option<i16>> {
        match self.inner {
            Some(ValueRefEnum::Int16(v)) | Some(ValueRefEnum::Enum(v)) => Ok(Some(v)),
            _ => err!(ConversionError::UnsupportedConversion),
        }
    }
}

#[inline]
fn decimal_scale_from_field(field: &Field) -> u8 {
    match field.sql_type {
        SqlType::Decimal(_, s) => s,
        _ => 0, //unreachable
    }
}

macro_rules! impl_value {
    ($f:ty, $vr:path) => {
        impl<'a> Value<'a, $f> for ValueRef<'a> {
            fn get(&'a self, _: &'a Field) -> Result<Option<$f>> {
                match self.inner {
                    Some($vr(v)) => Ok(Some(v.into())),
                    None => Ok(None),
                    _ => err!(ConversionError::UnsupportedConversion),
                }
            }
        }
    };
    ($f:ty, $vr:path, decimal) => {
        impl<'a> Value<'a, $f> for ValueRef<'a> {
            fn get(&'a self, field: &'a Field) -> Result<Option<$f>> {
                match self.inner {
                    Some($vr(v)) => {
                        let scale = decimal_scale_from_field(field);
                        Ok(Some(Decimal::from(v.0, scale)))
                    }
                    None => Ok(None),
                    _ => err!(ConversionError::UnsupportedConversion),
                }
            }
        }
    };
}
// Implement common types data conversion
// SqlType::X -> rust data conversion
impl_value!(f32, ValueRefEnum::Float32);
impl_value!(f64, ValueRefEnum::Float64);
// SqlType::Ipv4 - > Ipv4Addr
impl_value!(Ipv4Addr, ValueRefEnum::Ip4);
// SqlType::Ipv6 - > Ipv6Addr
impl_value!(Ipv6Addr, ValueRefEnum::Ip6);
// SqlType::UUID - > Uuid
impl_value!(Uuid, ValueRefEnum::Uuid);

// SqlType::X t-> X
impl_value!(u64, ValueRefEnum::UInt64);
impl_value!(i64, ValueRefEnum::Int64);
impl_value!(u32, ValueRefEnum::UInt32);
impl_value!(i32, ValueRefEnum::Int32);
impl_value!(u16, ValueRefEnum::UInt16);
// @note! particular case for i16. it can be retrieved from Enum as well
// impl_value!(i16, ValueRefEnum::Int16);
impl_value!(u8, ValueRefEnum::UInt8);
impl_value!(i8, ValueRefEnum::Int8);
// Implement SqlType::Decimal32 -> Decimal<i32> data conversion
impl_value!(Decimal32, ValueRefEnum::Decimal32, decimal);
// Implement SqlType::Decimal64 -> Decimal<i64> data conversion
impl_value!(Decimal64, ValueRefEnum::Decimal64, decimal);
// Implement SqlType::Decimal128 -> Decimal<i128> data conversion
#[cfg(feature = "int128")]
impl_value!(Decimal128, ValueRefEnum::Decimal128, decimal);

// Array values

/// signature to unsignature slice conversion
#[inline]
fn transmute_slice<T, U>(r: &[T]) -> &[U] {
    debug_assert_eq!(std::mem::size_of::<T>(), std::mem::size_of::<U>());
    debug_assert_eq!(std::mem::align_of::<T>(), std::mem::align_of::<U>());
    unsafe { std::slice::from_raw_parts(r.as_ptr() as *const U, r.len()) }
}

macro_rules! impl_array_value_ref {
    ($f:ty, $vr:path) => {
        impl<'a> Value<'a, &'a [$f]> for ValueRef<'a> {
            fn get(&'a self, _: &'a Field) -> Result<Option<&'a [$f]>> {
                match self.inner {
                    Some($vr(v)) => Ok(Some(v)),
                    _ => err!(ConversionError::UnsupportedConversion),
                }
            }
        }
    };
    ($f:ty, $vr:path, transmute) => {
        impl<'a> Value<'a, &'a [$f]> for ValueRef<'a> {
            fn get(&'a self, _: &'a Field) -> Result<Option<&'a [$f]>> {
                match self.inner {
                    Some($vr(v)) => Ok(Some(transmute_slice(v))),
                    _ => err!(ConversionError::UnsupportedConversion),
                }
            }
        }
    };
}
// @note! &[u8] type array clashes with string type column. So
// this type implemented together with  string column
// impl_array_value!(u8,ValueRefEnum::Array8);
impl_array_value_ref!(i8, ValueRefEnum::Array8, transmute);
impl_array_value_ref!(u16, ValueRefEnum::Array16);
impl_array_value_ref!(i16, ValueRefEnum::Array16, transmute);
impl_array_value_ref!(u32, ValueRefEnum::Array32);
impl_array_value_ref!(i32, ValueRefEnum::Array32, transmute);
impl_array_value_ref!(u64, ValueRefEnum::Array64);
impl_array_value_ref!(i64, ValueRefEnum::Array64, transmute);
impl_array_value_ref!(f32, ValueRefEnum::Array32, transmute);
impl_array_value_ref!(f64, ValueRefEnum::Array64, transmute);

// Types that can not be represented as reference to an integral numeric type
// but can be converted to vector.
macro_rules! impl_array_to_vec {
    ($t:ty, $refenum:path,$f:expr) => {
        impl<'a> Value<'a, Vec<$t>> for ValueRef<'a> {
            fn get(&'a self, _: &'a Field) -> Result<Option<Vec<$t>>> {
                match self.inner {
                    Some($refenum(v)) => {
                        let vec = v.iter().map($f).collect();
                        Ok(Some(vec))
                    }
                    _ => err!(ConversionError::UnsupportedConversion),
                }
            }
        }
    };
    ($t:ty, $refenum:path,$f:expr,decimal) => {
        impl<'a> Value<'a, Vec<$t>> for ValueRef<'a> {
            fn get(&'a self, field: &'a Field) -> Result<Option<Vec<$t>>> {
                match self.inner {
                    Some($refenum(v)) => {
                        let scale = decimal_scale_from_field(field);

                        let decimal = v.iter().map(|item| $f(*item, scale)).collect();

                        Ok(Some(decimal))
                    }
                    _ => err!(ConversionError::UnsupportedConversion),
                }
            }
        }
    };
}
// TODO: remove Array128 as redundant.
// Make `Ipv6` and `UUID` data types based on [u8;16] rather u128
impl_array_to_vec!(Date<Utc>, ValueRefEnum::Array16, |item| {
    ValueDate::date_inner(*item as i16)
});
impl_array_to_vec!(DateTime<Utc>, ValueRefEnum::Array32, |item| {
    ValueDateTime::datetime_inner(*item as i32)
});
impl_array_to_vec!(Ipv4Addr, ValueRefEnum::Array32, |item| Ipv4Addr::from(
    *item
));
impl_array_to_vec!(Ipv6Addr, ValueRefEnum::Array128, |item| Ipv6Addr::from(
    (*item).swap_bytes()
));
// The side effect of this design is the ability to interpret any array of u32/i32 as an array
// of Decimal32 with zero fractional part.
// The same is true for u64/i64 and Decimal64
impl_array_to_vec!(
    Decimal32,
    ValueRefEnum::Array32,
    |item, scale| Decimal::from(item as i32, scale),
    decimal
);
impl_array_to_vec!(
    Decimal64,
    ValueRefEnum::Array64,
    |item, scale| Decimal::from(item as i64, scale),
    decimal
);
// TODO: UUID arrays

/// An implementation provides Row-to-Object deserialization interface
/// It's used internally by block iterator
///
/// # Example
/// Some(object) = block.iter()
pub trait Deserialize: Sized {
    fn deserialize(row: Row) -> Result<Self>;
}
/// Input Block data row
#[derive(Debug)]
pub struct Row<'a> {
    /// vector of data references
    col: Vec<ValueRef<'a>>,
    /// data store
    block: &'a ServerBlock,
}

impl<'a> Row<'a> {
    /// # Safety
    /// This function should be called after
    /// `row_index` parameter was checked against row array boundary
    /// Block Iterators check it
    pub unsafe fn create(block: &'a ServerBlock, row_index: u64) -> Row<'a> {
        let col: Vec<_> = block
            .columns
            .iter()
            .map(|c| c.data.get_at(row_index))
            .collect();
        Row { col, block }
    }
    /// Returns the number of columns
    /// This number must correspond to the number of fields in the SELECT statement
    #[inline]
    pub fn len(&self) -> usize {
        self.col.len()
    }
    /// Empty server Data block is the special type of message.
    /// It's used internally and usually cannot be returned to user
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.col.is_empty()
    }

    pub fn iter_columns(&self) -> Iter<BlockColumn> {
        self.block.columns.iter()
    }

    pub fn iter_values(&self) -> Iter<ValueRef<'_>> {
        self.col.iter()
    }
    /// Returns row field value converting underlying Sql type
    /// to rust data type if the specific conversion is available.
    /// Otherwise it returns ConversionError
    /// For nullable Sql types if the field contains null value this method
    /// returns Ok(None)
    pub fn value<T>(&'a self, index: usize) -> Result<Option<T>>
    where
        T: 'a,
        ValueRef<'a>: Value<'a, T>,
    {
        let value_ref = self.col.get(index).ok_or(DriverError::IndexOutOfRange)?;
        let field = self
            .block
            .columns
            .get(index)
            .expect("column index out of range")
            .header
            .field
            .borrow();

        value_ref.get(field)
    }
    /// The same as `value` method but without performing any checking.
    /// # Safety
    /// Calling this method with an out of bound 'index' value is UB.
    /// Panic if this method is called with unsupported data conversion
    /// At the moment the driver provides limited number of data conversions.
    /// This method should be used only if you know that table data structure
    /// will nether change and you know exactly data types every column of the query.
    pub unsafe fn value_unchecked<T>(&'a self, index: usize) -> Option<T>
    where
        T: 'a,
        ValueRef<'a>: Value<'a, T>,
    {
        assert!(self.col.len() > index);
        let value_ref = self.col.get_unchecked(index);
        let field = self
            .block
            .columns
            .get_unchecked(index)
            .header
            .field
            .borrow();

        value_ref.get(field).expect("Conversion error")
    }

    #[inline]
    pub fn column_descr(&self, index: usize) -> Option<&BlockColumn> {
        self.block.columns.get(index)
    }
    /// Returns column index by its name
    pub fn column_index(&self, name: &str) -> usize {
        let item = self
            .block
            .columns
            .iter()
            .enumerate()
            .find(|(_i, c)| c.header.name.eq(name))
            .unwrap();
        item.0
    }
    /// Perform transformation Row to Plain object.
    /// Requires that object type implements Deserialize trait
    pub fn deserialize<D: Deserialize>(self) -> Result<D> {
        <D as Deserialize>::deserialize(self)
    }
}

impl<'a, C: AsInColumn + ?Sized + 'a> AsInColumn for Box<C> {
    #[inline]
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
        self.as_ref().get_at(index)
    }

    #[inline]
    unsafe fn into_bytes(&mut self) -> Vec<u8> {
        self.as_mut().into_bytes()
    }
}
/// String data is stored in Clickhouse as arbitrary byte sequence.
/// It's not always possible safely convert it to utf-8 rust string.
/// Hence we return it as boxed slice.
pub(crate) type BoxString = Box<[u8]>;

/// Enum value, String index pair,
/// 0-T, clickhouse value
/// 1-BoxString, enum string value
#[derive(Clone)]
pub struct EnumIndex<T>(pub T, pub BoxString);

impl<T> EnumIndex<T> {
    #[inline]
    pub(crate) unsafe fn as_str(&self) -> &str {
        std::str::from_utf8_unchecked(self.1.as_ref())
    }
}

impl<T: Ord + Copy> EnumIndex<T> {
    /// Sort by enum value (key)
    #[inline]
    pub(crate) fn fn_sort_val(item1: &EnumIndex<T>) -> T {
        item1.0
    }
    /// Sort by enum name
    #[inline]
    pub(crate) fn fn_sort_str(item1: &EnumIndex<T>, item2: &EnumIndex<T>) -> Ordering {
        Ord::cmp(item1.1.as_ref(), item2.1.as_ref())
    }
}

impl<T: PartialEq> PartialEq for EnumIndex<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T: Copy + fmt::Display> fmt::Display for EnumIndex<T> {
    /// Format Enum index value as a string that represent enum metadata
    #[allow(unused_must_use)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        use std::fmt::Write;
        f.write_str("'")?;
        // SAFETY! all Enum string values are received from Server as a
        // part of Enum metadata. So, all titles  is valid utf-8.
        let s = unsafe { self.as_str() };
        s.escape_default().for_each(|c| {
            f.write_char(c);
        });
        f.write_str("' = ")?;
        f.write_fmt(format_args!("{}", self.0))
    }
}

pub trait EnumTranscode: Copy {
    /// Perform Enum value to Enum title conversion
    fn transcode(self, meta: &FieldMeta) -> &[u8];
}

// interpret it as *T (i8, u8) (i16, u16) in relation to input type
// TODO: return empty slice instead of panic
/// transform enum value to index in title array
impl EnumTranscode for i16 {
    #[inline]
    fn transcode(self, meta: &FieldMeta) -> &[u8] {
        meta.val2str(self)
    }
}

/// Array of Enum8 or Enum16 values
pub(crate) struct EnumColumn<T> {
    data: Vec<T>,
}

/// T can be 'u8'(Enum8) or 'u16'(Enum16)
impl<T: Send> EnumColumn<T> {
    /// Read server stream as a sequence of u8(u16) bytes
    /// and store them in internal buffer
    pub(crate) async fn load_column<R: AsyncBufRead + Unpin>(
        mut reader: R,
        rows: u64,
        field: &Field,
    ) -> Result<EnumColumn<T>> {
        debug_assert!(field.get_meta().is_some());

        let mut data: Vec<T> = Vec::with_capacity(rows as usize);
        unsafe {
            data.set_len(rows as usize);
            reader.read_exact(as_bytes_bufer_mut(&mut data)).await?;
        }
        Ok(EnumColumn { data })
    }
    pub(crate) fn set_nulls(self, _nulls: Option<Vec<u8>>) -> Box<EnumColumn<T>> {
        // TODO: Check nullable enum. If Enum can be nullable how it stores nulls?
        Box::new(self)
    }
}

impl<'a, T: Copy + Send + Into<i16>> AsInColumn for EnumColumn<T> {
    /// Store the enum value  in ValueRef struct
    /// ValueRef performs conversion of Enum value to corresponding reference to title
    /// when a  caller requires it.
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
        assert!((index as usize) < self.data.len());
        let enum_value: i16 = (*self.data.get_unchecked(index as usize)).into();

        ValueRef {
            inner: Some(ValueRefEnum::Enum(enum_value)),
        }
    }
    unsafe fn into_bytes(&mut self) -> Vec<u8> {
        as_bytes_bufer_mut(&mut self.data).to_vec()
    }
}

/// Column of sized types that can be represented
/// in memory as continuous array of fixed-size elements
pub(crate) struct FixedColumn<T: Sized> {
    data: Vec<T>,
}

impl<T: Sized> FixedColumn<T> {
    #[cfg(feature = "is_sorted")]
    pub fn is_sorted(&self) -> bool {
        self.data.is_sorted()
    }
    #[cfg(not(feature = "is_sorted"))]
    #[inline]
    pub fn is_sorted(&self) -> bool {
        // This is used only for debug assert macros.
        // So, just skip this check
        // FIXME: when `is_sorted` will be stabilized
        true
    }
}

/// Column of fixed-size types with `is-null` status indicator
pub(crate) struct FixedNullColumn<T: Sized> {
    inner: FixedColumn<T>,
    // TODO: All values are '1' - null, '0' - no null.
    // For memory usage it might be better  to compress byte array of nulls to bitwise.
    // And yet it can hit performance. Here it is a matter for study
    // AVX2 simd instructions for optimized bit compression:
    // __m256i x0 <- load 32 bytes
    // __m256i x1 = _mm256_set1_epi8 (0x01)
    // __m256i x0 = _mm256_sub_epi8 (x0, x1)
    // int pack = _mm256_movemask_epi8 (x0)
    nulls: Vec<u8>,
}

impl<'a> AsInColumn for FixedColumn<BoxString> {
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
        debug_assert!((index as usize) < self.data.len());
        ValueRef {
            inner: Some(ValueIndex::valueref_at(self.data.as_slice(), index)),
        }
    }

    unsafe fn into_bytes(&mut self) -> Vec<u8> {
        self.data
            .iter_mut()
            .map(|mut val| unsafe {
                let mut buf = Vec::with_capacity(8 + val.len());
                let _ = (val.len() as u64).encode(&mut buf);
                let _ = buf.write(as_bytes_bufer_mut(&mut val));
                buf
            })
            .flatten()
            .collect()
    }

    fn offset_map(&self) -> Option<Vec<u32>> {
        let mut offset = vec![0];
        let mut start = 0;

        offset = self.data.iter().fold(offset, |mut offset, s| {
            start = s.len() + start + 1;
            offset.push(start as u32);
            offset
        });

        Some(offset)
    }
}

/// Borrow mutable reference to an array of
/// arbitrary type and represent it as an array of bytes.
/// It's used internally to load array of integer data types from socket stream.
#[inline]
pub(crate) unsafe fn as_bytes_bufer_mut<T>(v: &mut [T]) -> &mut [u8] {
    std::slice::from_raw_parts_mut(
        v.as_mut_ptr() as *mut u8,
        v.len() * std::mem::size_of::<T>(),
    )
}

#[inline]
pub(crate) unsafe fn as_bytes_bufer<T>(v: &[T]) -> &[u8] {
    std::slice::from_raw_parts(v.as_ptr() as *mut u8, v.len() * std::mem::size_of::<T>())
}

/// Column of String represented as an array of Boxed byte arrays
pub(crate) type StringColumn = FixedColumn<BoxString>;

impl FixedColumn<BoxString> {
    pub(crate) async fn load_string_column<R: AsyncBufRead + Unpin>(
        reader: R,
        rows: u64,
    ) -> Result<StringColumn> {
        let mut data: Vec<BoxString> = Vec::with_capacity(rows as usize);

        let mut rdr = ValueReader::new(reader);
        let mut l: u64;
        for _ in 0..rows {
            l = rdr.read_vint().await?;
            let s: Vec<u8> = rdr.read_string(l).await?;
            data.push(s.into_boxed_slice());
        }

        Ok(StringColumn { data })
    }

    pub(crate) async fn load_fixed_string_column<R: AsyncBufRead + Unpin>(
        mut reader: R,
        rows: u64,
        width: u32,
    ) -> Result<StringColumn> {
        let mut data: Vec<BoxString> = Vec::with_capacity(rows as usize);

        for _ in 0..rows {
            let mut s: Vec<u8> = Vec::with_capacity(width as usize);
            unsafe {
                s.set_len(width as usize);
            }
            reader.read_exact(s.as_mut_slice()).await?;
            data.push(s.into_boxed_slice());
        }

        Ok(StringColumn { data })
    }
}

// impl AsInColumn for FixedColumn<BoxString> {
//     unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
//         assert!((index as usize) < self.data.len());
//         let vr = self.data.get_unchecked(index as usize);
//
//         ValueRef {
//             inner: Some(ValueRefEnum::String(vr)),
//         }
//     }
// }

impl<T: Sized> FixedColumn<T> {
    /// Load Column of integer data types from the socket buffer
    pub(crate) async fn load_column<R: AsyncBufRead + Unpin>(
        mut reader: R,
        rows: u64,
    ) -> Result<FixedColumn<T>> {
        let mut data: Vec<T> = Vec::with_capacity(rows as usize);

        unsafe {
            data.set_len(rows as usize);
            // Big-endian? Never heard
            reader.read_exact(as_bytes_bufer_mut(&mut data)).await?;
        }

        Ok(FixedColumn { data })
    }
    /// Cast Column of one data type to another.
    /// Many types (UUID, Date, DateTime, Ip..) have the same memory representations
    /// as basic integer types (u16, u32, u64,...). Hence we can load data as an array of
    /// integers and cast it to desired type. This is true for little endian platforms.
    /// A bit-endian require byte swap, that can be done in ValueRef getter interface.
    pub(crate) fn cast<U: Sized>(self: FixedColumn<T>) -> FixedColumn<U> {
        assert_eq!(size_of::<T>(), size_of::<U>());
        assert!(align_of::<T>() >= align_of::<U>());

        unsafe {
            let mut clone = std::mem::ManuallyDrop::new(self);
            FixedColumn {
                data: Vec::from_raw_parts(
                    clone.data.as_mut_ptr() as *mut U,
                    clone.data.len(),
                    clone.data.capacity(),
                ),
            }
        }
    }
}

impl<T> FixedColumn<T>
where
    T: Sized + Send + 'static,
    FixedNullColumn<T>: AsInColumn,
    FixedColumn<T>: AsInColumn,
{
    /// Wrap the Column in FixedNullColumn adapter if nulls array is provided.
    /// Return the unchanged Column if nulls is not provided.
    #[inline]
    pub(crate) fn set_nulls(self: Self, nulls: Option<Vec<u8>>) -> Box<dyn AsInColumn> {
        if let Some(nulls) = nulls {
            Box::new(FixedNullColumn { inner: self, nulls })
        } else {
            Box::new(self)
        }
    }
}
/// Row getter interface implementation for nullable column of fixed data types
impl<T: Sized> AsInColumn for FixedNullColumn<T>
where
    FixedColumn<T>: AsInColumn,
{
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
        debug_assert!((index as usize) < self.nulls.len());
        if self.nulls[index as usize] == 1 {
            ValueRef { inner: None }
        } else {
            self.inner.get_at(index)
        }
    }

    unsafe fn into_bytes(&mut self) -> Vec<u8> {
        as_bytes_bufer_mut(&mut self.inner.data).to_vec()
    }
}
/// Any type that implement ValueIndex trait can be loaded into FixedColumn
pub(crate) trait ValueIndex: Sized {
    // TODO: redesign trait method to 'valueref(&self)->ValueRefEnum<'_>
    unsafe fn valueref_at(_: &[Self], index: u64) -> ValueRefEnum<'_>;
}

impl ValueIndex for BoxString {
    #[inline]
    unsafe fn valueref_at(this: &[BoxString], index: u64) -> ValueRefEnum<'_> {
        let vr = this.get_unchecked(index as usize);
        ValueRefEnum::String(vr.as_ref())
    }
}

macro_rules! impl_vre_at {
    ($f:ty,$vr:expr) => {
        impl ValueIndex for $f {
            #[inline]
            unsafe fn valueref_at(this: &[$f], index: u64) -> ValueRefEnum<'_> {
                debug_assert!((index as usize) < this.len());
                let vr = this.get_unchecked(index as usize);
                $vr(*vr)
            }
        }
    };
}

impl_vre_at!(u8, ValueRefEnum::UInt8);
impl_vre_at!(i8, ValueRefEnum::Int8);
impl_vre_at!(u16, ValueRefEnum::UInt16);
impl_vre_at!(i16, ValueRefEnum::Int16);
impl_vre_at!(u32, ValueRefEnum::UInt32);
impl_vre_at!(i32, ValueRefEnum::Int32);
impl_vre_at!(u64, ValueRefEnum::UInt64);
impl_vre_at!(i64, ValueRefEnum::Int64);
#[cfg(feature = "int128")]
impl_vre_at!(u128, ValueRefEnum::UInt128);

impl_vre_at!(f32, ValueRefEnum::Float32);
impl_vre_at!(f64, ValueRefEnum::Float64);

impl_vre_at!(ValueUuid, ValueRefEnum::Uuid);
impl_vre_at!(ValueIp4, ValueRefEnum::Ip4);
impl_vre_at!(ValueIp6, ValueRefEnum::Ip6);

impl_vre_at!(ValueDecimal32, ValueRefEnum::Decimal32);
impl_vre_at!(ValueDecimal64, ValueRefEnum::Decimal64);
#[cfg(feature = "int128")]
impl_vre_at!(ValueDecimal128, ValueRefEnum::Decimal128);

impl_vre_at!(ValueDate, ValueRefEnum::Date);
impl_vre_at!(ValueDateTime, ValueRefEnum::DateTime);
impl_vre_at!(ValueDateTime64, ValueRefEnum::DateTime64);

macro_rules! impl_fixed_column {
    ($f:ty) => {
        impl AsInColumn for FixedColumn<$f> {
            unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
                debug_assert!((index as usize) < self.data.len());
                ValueRef {
                    inner: Some(ValueIndex::valueref_at(self.data.as_slice(), index)),
                }
            }
            unsafe fn into_bytes(&mut self) -> Vec<u8> {
                as_bytes_bufer_mut(&mut self.data).to_vec()
            }
        }
    };
}

impl_fixed_column!(u8);
impl_fixed_column!(i8);
impl_fixed_column!(u16);
impl_fixed_column!(i16);
impl_fixed_column!(u32);
impl_fixed_column!(i32);
impl_fixed_column!(u64);
impl_fixed_column!(i64);
#[cfg(feature = "int128")]
impl_fixed_column!(u128);

impl_fixed_column!(f32);
impl_fixed_column!(f64);

impl_fixed_column!(ValueUuid);
impl_fixed_column!(ValueIp4);
impl_fixed_column!(ValueIp6);

impl_fixed_column!(ValueDecimal32);
impl_fixed_column!(ValueDecimal64);
#[cfg(feature = "int128")]
impl_fixed_column!(ValueDecimal128);

impl_fixed_column!(ValueDate);
impl_fixed_column!(ValueDateTime);
impl_fixed_column!(ValueDateTime64);

/// One dimensional array of integral types
#[allow(dead_code)]
pub(crate) struct FixedArrayColumn<T> {
    /// Array values
    data: Vec<T>,
    /// Array offsets
    index: Vec<u64>,
}

impl<T: Send + IntoArray + 'static> FixedArrayColumn<T> {
    pub(crate) async fn load_column<R>(
        mut reader: R,
        rows: u64,
    ) -> Result<Box<dyn AsInColumn>>
    where
        R: AsyncBufRead + Unpin,
    {
        let index: FixedColumn<u64> =
            FixedColumn::load_column(reader.borrow_mut(), rows).await?;

        debug_assert!(index.is_sorted());

        let rows = *index.data.last().expect("null size array");
        let data: FixedColumn<T> =
            FixedColumn::load_column(reader.borrow_mut(), rows).await?;

        Ok(Box::new(FixedArrayColumn {
            data: data.data,
            index: index.data,
        }))
    }
}

pub trait IntoArray: Sized {
    fn into_array(this: &[Self]) -> ValueRefEnum<'_>;
}

macro_rules! impl_intoarray {
    ($t:ty, $array: expr) => {
        impl IntoArray for $t {
            #[inline]
            fn into_array(this: &[$t]) -> ValueRefEnum<'_> {
                $array(this)
            }
        }
    };
}

impl_intoarray!(u8, ValueRefEnum::Array8);
impl_intoarray!(u16, ValueRefEnum::Array16);
impl_intoarray!(u32, ValueRefEnum::Array32);
impl_intoarray!(u64, ValueRefEnum::Array64);
impl_intoarray!(u128, ValueRefEnum::Array128);

impl<T: Send + IntoArray> AsInColumn for FixedArrayColumn<T> {
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
        let size1 = if index == 0 {
            0_u64
        } else {
            *self.index.get_unchecked((index - 1) as usize)
        };

        let size2 = *self.index.get_unchecked(index as usize);
        debug_assert!(size1 <= size2);
        let size1 = size1 as usize;
        let size2 = size2 as usize;
        debug_assert!(size2 <= self.data.len());

        ValueRef {
            inner: Some(IntoArray::into_array(&self.data[size1..size2])),
        }
    }

    unsafe fn into_bytes(&mut self) -> Vec<u8> {
        let mut data = as_bytes_bufer_mut(&mut self.data).to_vec();
        let mut index = as_bytes_bufer_mut(&mut self.index).to_vec();
        index.append(&mut data);
        index
    }
}

/// LowCardinality data type is used to reduce the storage requirements
/// and significantly improve query performance for String and some other data.
/// Internally it's encoded as a dictionary with numeric keys (u8, u16, u32, or u64 type)
/// T - key type. The current implementation supports only String data type.
pub(crate) struct LowCardinalityColumn<T: Sized + Send> {
    values: Vec<BoxString>,
    data: Vec<T>,
}
// TODO: redesign bearing in mind 32-bit system limitations.
// In 32-bit platforms 64-bit indexes cannot be fit in memory.
// Probably it is not practical to  create and send LowCardinality column
// with more than 2^32 keys. It is worth setting a reasonable limit on
// the number or keys sending in one block.
impl<T> LowCardinalityColumn<T>
where
    T: Sized + Ord + Copy + Send + Into<u64> + 'static,
{
    pub(crate) async fn load_column<R>(
        reader: R,
        rows: u64,
        values: FixedColumn<BoxString>,
    ) -> Result<Box<dyn AsInColumn>>
    where
        R: AsyncBufRead + Unpin,
    {
        debug_assert!(rows > 0);
        let data: FixedColumn<T> = FixedColumn::load_column(reader, rows).await?;

        let m = data
            .data
            .iter()
            .max()
            .expect("corrupted lowcardinality column");

        if (*m).into() >= values.data.len() as u64 {
            return err!(DriverError::IntegrityError);
        }

        Ok(Box::new(LowCardinalityColumn {
            data: data.data,
            values: values.data,
        }))
    }
}

impl<T: Copy + Send + Sized + Into<u64>> AsInColumn for LowCardinalityColumn<T> {
    unsafe fn get_at(&self, index: u64) -> ValueRef<'_> {
        debug_assert!((index as usize) < self.data.len());
        let index = self.data.get_unchecked(index as usize);

        let index: u64 = (*index).into();
        if index == 0 {
            // Supposed the first item in list  is always NULL value
            debug_assert!(self.values[0].len() == 0);
            ValueRef { inner: None }
        } else {
            ValueRef {
                inner: Some(ValueIndex::valueref_at(self.values.as_slice(), index)),
            }
        }
    }

    unsafe fn into_bytes(&mut self) -> Vec<u8> {
        let mut data = as_bytes_bufer_mut(&mut self.data).to_vec();
        let mut values = as_bytes_bufer_mut(&mut self.values).to_vec();
        data.append(&mut values);
        data
    }
}
