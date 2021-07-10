use std::{
    cell::Ref,
    cell::RefMut,
    collections::HashMap,
    mem,
    ops::{Deref, DerefMut},
    slice,
    str::FromStr,
};

use base::bytes_cat;
use base::datetimes::TimeZoneId;
use base::strings::BytesTrim;
use num_traits::PrimInt;

use crate::errs::MetaError;
use crate::errs::MetaResult;

pub type Id = u64;

#[macro_export]
macro_rules! to_qualified_key {
    ($($e:expr),* $(,)*) => {[$($e),*].join(".")}
}

#[derive(PartialEq, Debug, Default, Copy, Clone)]
#[repr(C, packed)]
pub struct ColumnInfo {
    pub data_type: BqlType,
    pub is_primary_key: bool,
    pub is_nullable: bool,
    pub ordinal: u32,
}

//FIXME add ids to Tab/Col model?
#[derive(PartialEq, Debug, Default)]
pub struct Table {
    pub name: String,
    pub dbname: String,
    pub columns: Vec<(String, ColumnInfo)>,
    pub tab_info: TableInfo,
}

impl Table {
    pub fn sort_columns_by_ordinal(&mut self) {
        self.columns.sort_unstable_by_key(|c| c.1.ordinal);
    }

    // pub fn sort_columns_by_names(&mut self, colnames: Vec<String>) {
    //     // self.columns.sort_unstable_by_key(|c|c.1.ordinal);
    // }
}

#[derive(PartialEq, Debug, Default)]
pub struct TableInfo {
    pub create_script: String,
    pub engine: EngineType,
    pub partition_keys_expr: String,
    pub partition_cols: String,
    pub settings: HashMap<String, String>,
}

#[derive(Copy, Clone, Debug, PartialEq, Hash, Eq)]
#[repr(u8)]
pub enum EngineType {
    Default = 0,
    BaseStorage = 1,
}

impl EngineType {
    pub fn from_str(v: &str) -> MetaResult<Self> {
        match v {
            "BaseStorage" => Ok(EngineType::BaseStorage),
            _ => Err(MetaError::UnknownEngineTypeConversionError),
        }
    }
}

//FIXME remove unsafe with match pat
impl From<u8> for EngineType {
    fn from(v: u8) -> Self {
        unsafe { std::mem::transmute(v) }
    }
}

impl Default for EngineType {
    fn default() -> Self {
        EngineType::Default
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Hash, Eq)]
#[repr(C)]
pub enum BqlType {
    UnInit,
    UInt(u8),
    Int(u8),
    Decimal(u8, u8),
    Date,
    DateTime(Option<TimeZoneId>),
    String,
    LowCardinalityString,
    Float(u8),
    FixedString(u8),
    LowCardinalityTinyText,
}

impl Default for BqlType {
    fn default() -> Self {
        BqlType::UnInit
    }
}

// macro_rules! gen_str_to_bql {
//     ( $item:ident, $($typ:ident,)*) => {
//         match $item {
//           $(stringify!($typ) => Ok(BqlType::$typ),)*
//           "LowCardinality(String)" => Ok(BqlType::LowCardinalityString),
//           _ => Err(MetaError::UnknownBqlTypeConversionError)
//         }
//     }
// }

macro_rules! conversion_err {
    ($ITEM:ident) => {
        MetaError::UnknownBqlTypeConversionError(
            unsafe { std::str::from_utf8_unchecked($ITEM) }.to_string(),
        )
    };
}
impl BqlType {
    /// return the data type size in bytes, Err for dynamically sized type
    pub fn size(self) -> MetaResult<u8> {
        match self {
            BqlType::Int(siz) => Ok(siz / 8),
            BqlType::UInt(siz) => Ok(siz / 8),
            BqlType::Float(siz) => Ok(siz / 8),
            BqlType::DateTime(_) => Ok(4),
            BqlType::Date => Ok(2),
            BqlType::Decimal(p, _s) => {
                if p < 10 {
                    Ok(4)
                } else if p <= 18 {
                    Ok(8)
                } else {
                    Err(MetaError::UnsupportedBqlTypeError)
                }
            }
            BqlType::LowCardinalityString => Ok(4),
            BqlType::LowCardinalityTinyText => Ok(1),
            BqlType::FixedString(siz) => Ok(siz),
            _ => Err(MetaError::NoFixedSizeDataTypeError),
        }
    }

    pub fn size_in_usize(self) -> MetaResult<usize> {
        Ok(self.size()? as usize)
    }

    pub fn to_vec(self) -> MetaResult<Vec<u8>> {
        match self {
            BqlType::UnInit => Ok(b"UnInit".to_vec()),
            BqlType::UInt(len) => {
                let mut bi = [0u8; 4];
                let n = itoa::write(&mut bi[..], len)?;
                Ok(bytes_cat!(b"UInt", &bi[..n]))
            }
            BqlType::Int(len) => {
                let mut bi = [0u8; 4];
                let n = itoa::write(&mut bi[..], len)?;
                Ok(bytes_cat!(b"Int", &bi[..n]))
            }
            BqlType::Float(len) => {
                let mut bi = [0u8; 4];
                let n = itoa::write(&mut bi[..], len)?;
                Ok(bytes_cat!(b"Float", &bi[..n]))
            }
            BqlType::Decimal(p, s) => {
                let mut bp = [0u8; 4];
                let np = itoa::write(&mut bp[..], p)?;
                let mut bs = [0u8; 4];
                let ns = itoa::write(&mut bs[..], s)?;
                Ok(bytes_cat!(b"Decimal(", &bp[..np], b",", &bs[..ns], b")"))
            }
            BqlType::DateTime(None) => Ok(b"DateTime".to_vec()),
            BqlType::DateTime(Some(tz)) => {
                Ok(bytes_cat!(b"DateTime('", tz.name().as_bytes(), b"')"))
            }
            BqlType::Date => Ok(b"Date".to_vec()),
            BqlType::String => Ok(b"String".to_vec()),
            BqlType::LowCardinalityString | BqlType::LowCardinalityTinyText => {
                Ok(b"LowCardinality(String)".to_vec())
            }
            BqlType::FixedString(len) => {
                let mut bi = [0u8; 4];
                let n = itoa::write(&mut bi[..], len)?;
                Ok(bytes_cat!(b"FixedString(", &bi[..n], b")"))
            }
        }
    }

    pub fn from_str(item: &str) -> MetaResult<Self> {
        Self::from_bytes(item.as_bytes())
    }

    pub fn from_bytes(item: &[u8]) -> MetaResult<Self> {
        match item {
            b"Int8" => Ok(BqlType::Int(8)),
            b"Int16" => Ok(BqlType::Int(16)),
            b"Int32" => Ok(BqlType::Int(32)),
            b"Int64" => Ok(BqlType::Int(64)),
            b"UInt8" => Ok(BqlType::UInt(8)),
            b"UInt16" => Ok(BqlType::UInt(16)),
            b"UInt32" => Ok(BqlType::UInt(32)),
            b"UInt64" => Ok(BqlType::UInt(64)),
            b"Float16" => Ok(BqlType::Float(16)),
            b"Float32" => Ok(BqlType::Float(32)),
            b"Float64" => Ok(BqlType::Float(64)),
            b"Date" => Ok(BqlType::Date),
            b"String" => Ok(BqlType::String),
            b"LowCardinality(String)" => Ok(BqlType::LowCardinalityString),
            b"LowCardinality(TinyText)" => Ok(BqlType::LowCardinalityTinyText),
            datetime_item if datetime_item.starts_with(b"DateTime") => {
                Self::_datetime_type(datetime_item)
            }
            decimal_item if decimal_item.starts_with(b"Decimal") => {
                Self::_decimal_type(decimal_item)
            }
            fixed_string_item if fixed_string_item.starts_with(b"FixedString") => {
                match &fixed_string_item[b"FixedString".len()..] {
                    [b'(', len @ .., b')'] => Ok(BqlType::FixedString(
                        Self::_parse_num(len).ok_or_else(|| conversion_err!(item))?,
                    )),
                    _ => Err(conversion_err!(item)),
                }
            }
            _ => Err(conversion_err!(item)),
        }
    }

    // SAFETY: Only ASCII content is acceptable.
    fn _datetime_type(datetime_item: &[u8]) -> MetaResult<Self> {
        let tz = match &datetime_item[b"DateTime".len()..] {
            [] => None,
            [b'(', tz @ .., b')'] => match tz.trim() {
                [b'\'', tz @ .., b'\''] => {
                    let tz = unsafe { std::str::from_utf8_unchecked(tz) };
                    Some(TimeZoneId::from_str(tz.trim())?)
                }
                _ => return Err(conversion_err!(datetime_item)),
            },
            _ => return Err(conversion_err!(datetime_item)),
        };
        Ok(Self::DateTime(tz))
    }

    fn _parse_num(bytes: &[u8]) -> Option<u8> {
        btoi::btou(bytes.trim()).ok()
    }

    fn _decimal_type(decimal_item: &[u8]) -> MetaResult<Self> {
        let (precision, scale) = match &decimal_item[b"Decimal".len()..] {
            // Decimal(p, s)
            [b'(', ps @ .., b')'] => {
                let mut ps_iter = ps.split(|v| *v == b',');
                let p = ps_iter
                    .next()
                    .ok_or_else(|| conversion_err!(decimal_item))?;
                let s = ps_iter
                    .next()
                    .ok_or_else(|| conversion_err!(decimal_item))?;
                if ps_iter.next().is_some() {
                    return Err(conversion_err!(decimal_item));
                }
                (
                    Self::_parse_num(p).ok_or_else(|| conversion_err!(decimal_item))?,
                    Self::_parse_num(s).ok_or_else(|| conversion_err!(decimal_item))?,
                )
            }
            // Decimal32(s) => Decimal(9, s)
            [b'3', b'2', b'(', s @ .., b')'] => (
                9,
                Self::_parse_num(s).ok_or_else(|| conversion_err!(decimal_item))?,
            ),
            // Decimal64(s) => Decimal(18, s)
            [b'6', b'4', b'(', s @ .., b')'] => (
                18,
                Self::_parse_num(s).ok_or_else(|| conversion_err!(decimal_item))?,
            ),
            _ => return Err(conversion_err!(decimal_item)),
        };
        // Range of precision and scale:
        // - precision in [1, 76],
        // - scale in [0, precision],
        //     https://clickhouse.tech/docs/en/sql-reference/data-types/decimal/
        if !(1..=76).contains(&precision) || !(0..=precision).contains(&scale) {
            return Err(MetaError::InvalidPrecisionOrScaleOfDecimalError(
                precision, scale,
            ));
        }
        return Ok(BqlType::Decimal(precision, scale));
    }
}

impl AsBytes for usize {}
impl AsBytes for u64 {}
impl AsBytes for u32 {}
impl AsBytes for u16 {}
impl AsBytes for u8 {}
impl AsBytes for i64 {}
impl AsBytes for i32 {}
impl AsBytes for i16 {}
impl AsBytes for i8 {}

impl AsBytes for &str {
    fn as_bytes(&self) -> &[u8] {
        (self as &str).as_bytes()
    }
}

impl AsBytes for &[u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

impl AsBytes for (u64, u64) {}

impl AsBytes for ColumnInfo {}

#[inline]
pub fn to_key_id_order<T: PrimInt>(i: T) -> T {
    i.to_be()
}

pub trait IntoKeyId: IntoRef {
    fn into_key_id(&self) -> Id;
}
impl<'a> IntoKeyId for &'a [u8] {
    fn into_key_id(&self) -> Id {
        u64::from_be(*self.into_ref::<u64>())
    }
}

/// Borrowed from zerocopy, unsafe
/// FIMXE only allow single-layer structs attributed with #[repr(C, packed)]
pub trait AsBytes {
    /// Get the bytes of this value.
    ///
    /// `as_bytes` provides access to the bytes of this value as an immutable
    /// byte slice.
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            // NOTE: This function does not have a Self: Sized bound.
            // size_of_val works for unsized values too.
            let len = mem::size_of_val(self);
            slice::from_raw_parts(self as *const Self as *const u8, len)
        }
    }

    /// Get the bytes of this value mutably.
    ///
    /// `as_bytes_mut` provides access to the bytes of this value as a mutable
    /// byte slice.
    fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            // NOTE: This function does not have a Self: Sized bound.
            // size_of_val works for unsized values too.
            let len = mem::size_of_val(self);
            slice::from_raw_parts_mut(self as *mut Self as *mut u8, len)
        }
    }
}

mod sealed {
    use core::cell::{Ref, RefMut};

    pub trait Sealed {}
    impl<'a> Sealed for &'a [u8] {}
    impl<'a> Sealed for &'a mut [u8] {}
    impl<'a> Sealed for Ref<'a, [u8]> {}
    impl<'a> Sealed for RefMut<'a, [u8]> {}
}

/// A mutable or immutable reference to a byte slice.
///
/// `ByteSlice` abstracts over the mutability of a byte slice reference, and is
/// implemented for various special reference types such as `Ref<[u8]>` and
/// `RefMut<[u8]>`.
pub unsafe trait ByteSlice:
    Deref<Target = [u8]> + Sized + self::sealed::Sealed
{
    fn as_ptr(&self) -> *const u8;
    fn split_at(self, mid: usize) -> (Self, Self);
}

/// A mutable reference to a byte slice.
///
/// `ByteSliceMut` abstracts over various ways of storing a mutable reference to
/// a byte slice, and is implemented for various special reference types such as
/// `RefMut<[u8]>`.
pub unsafe trait ByteSliceMut: ByteSlice + DerefMut {
    fn as_mut_ptr(&mut self) -> *mut u8;
}

unsafe impl<'a> ByteSlice for &'a [u8] {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }
    fn split_at(self, mid: usize) -> (Self, Self) {
        <[u8]>::split_at(self, mid)
    }
}
unsafe impl<'a> ByteSlice for &'a mut [u8] {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }
    fn split_at(self, mid: usize) -> (Self, Self) {
        <[u8]>::split_at_mut(self, mid)
    }
}
unsafe impl<'a> ByteSlice for Ref<'a, [u8]> {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }
    fn split_at(self, mid: usize) -> (Self, Self) {
        Ref::map_split(self, |slice| <[u8]>::split_at(slice, mid))
    }
}
unsafe impl<'a> ByteSlice for RefMut<'a, [u8]> {
    fn as_ptr(&self) -> *const u8 {
        <[u8]>::as_ptr(self)
    }
    fn split_at(self, mid: usize) -> (Self, Self) {
        RefMut::map_split(self, |slice| <[u8]>::split_at_mut(slice, mid))
    }
}

unsafe impl<'a> ByteSliceMut for &'a mut [u8] {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        <[u8]>::as_mut_ptr(self)
    }
}
unsafe impl<'a> ByteSliceMut for RefMut<'a, [u8]> {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        <[u8]>::as_mut_ptr(self)
    }
}
pub trait IntoRef: ByteSlice {
    fn into_ref<T>(&self) -> &T {
        unsafe { &*(self.as_ptr() as *const T) }
    }
}

pub trait IntoMutRef: ByteSliceMut {
    fn into_mut_ref<T>(&mut self) -> &mut T {
        unsafe { &mut *(self.as_mut_ptr() as *mut T) }
    }
}

impl<'a> IntoRef for &'a [u8] {}
impl<'a> IntoMutRef for &'a mut [u8] {}

// === physical storage data type ===

///FIXME data, null_map, lc_dict_data are expensively copied as Vec
pub struct BaseChunk {
    pub btype: BqlType,
    pub size: usize,
    pub data: Vec<u8>,
    pub null_map: Option<Vec<u8>>,
    pub offset_map: Option<Vec<u32>>,
    // pub lc_dict_size: usize,
    pub lc_dict_data: Option<Vec<u8>>,
}

impl std::fmt::Debug for BaseChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaseChunk")
            .field("btype", &self.btype)
            .field("size", &self.size)
            .field("null_map", &self.null_map.is_some())
            .field("offset_map", &self.offset_map.is_some())
            .field("lc_dict_data", &self.lc_dict_data.is_some())
            .field("data[bytes]", &self.data.len())
            .finish()
    }
}

pub trait AsKey: AsBytes {
    fn as_key(self) -> Self;
}

macro_rules! decl_AsKey {
    ($($t:ident,)*) => {
      $(
        impl AsKey for $t {
            fn as_key(self) -> Self {
                self.to_be()
            }
        }
      )*
    }
}

decl_AsKey! { u64,i64,u32,i32,u16,i16,u8,i8, }

impl AsKey for &str {
    fn as_key(self) -> Self {
        self
    }
}

impl AsKey for &[u8] {
    fn as_key(self) -> Self {
        self
    }
}

#[cfg(test)]
mod unit_tests {
    use base::show_option_size;

    use super::*;

    use chrono_tz::Tz;

    #[derive(Copy, Clone, Debug, PartialEq)]
    #[repr(C, packed)]
    pub struct Db {
        pub c1: u64,
        pub c2: u32,
        pub c3: u8,
        pub c4: bool,
        pub c5: i16,
        pub c6: [u8; 3],
    }

    impl AsBytes for Db {}

    #[test]
    fn sanity_checks_for_types() {
        assert_eq!("123.456", to_qualified_key!("123", "456"));

        assert_eq!(
            [0xFF, 0xFF, 0, 0, 0, 0, 0, 0],
            (64u64 * 1024 - 1).as_bytes()
        );

        let bs = &[0xFFu8, 0xFF, 0, 0, 0, 0, 0, 0] as &[u8];
        assert_eq!(*bs.into_ref::<u64>(), (64u64 * 1024 - 1));

        let mut bs = &mut [0xFFu8, 0xFF, 0, 0, 0, 0, 0, 0] as &mut [u8];
        assert_eq!(*bs.into_mut_ref::<u64>(), (64u64 * 1024 - 1));

        let testdb = Db {
            c1: 123456789,
            c2: 2345678,
            c3: 127,
            c4: true,
            c5: -12345,
            c6: [11u8, 22, 33],
        };

        let bs_testdb = testdb.as_bytes();
        assert_eq!(bs_testdb.len(), 8 + 4 + 1 + 1 + 2 + 3);

        assert_eq!(bs_testdb.into_ref::<Db>(), &testdb);
    }

    #[test]
    fn test_bqltype_from_str() -> MetaResult<()> {
        assert_eq!(BqlType::from_str("String")?, BqlType::String);
        assert_eq!(
            BqlType::from_str("FixedString(123)")?,
            BqlType::FixedString(123)
        );
        assert_eq!(
            BqlType::from_str("FixedString( 255 )")?,
            BqlType::FixedString(0xff)
        );
        assert_eq!(
            BqlType::from_str("Decimal(11 , 3)")?,
            BqlType::Decimal(11, 3)
        );
        assert_eq!(BqlType::from_str("Decimal( 9, 2)")?, BqlType::Decimal(9, 2));
        assert_eq!(BqlType::from_str("Decimal32( 4 )")?, BqlType::Decimal(9, 4));
        assert_eq!(BqlType::from_str("Decimal32(  9)")?, BqlType::Decimal(9, 9));
        assert_eq!(
            BqlType::from_str("Decimal64( 4 )")?,
            BqlType::Decimal(18, 4)
        );
        assert_eq!(BqlType::from_str("Decimal64(9)")?, BqlType::Decimal(18, 9));
        assert_eq!(
            BqlType::from_str("Decimal64( 18  )")?,
            BqlType::Decimal(18, 18)
        );
        assert_eq!(BqlType::from_str("Int8")?, BqlType::Int(8));
        assert_eq!(BqlType::from_str("Int32")?, BqlType::Int(32));
        assert_eq!(BqlType::from_str("UInt32")?, BqlType::UInt(32));
        assert_eq!(BqlType::from_str("UInt64")?, BqlType::UInt(64));
        assert_eq!(BqlType::from_str("Float16")?, BqlType::Float(16));
        assert_eq!(BqlType::from_str("Float32")?, BqlType::Float(32));
        assert_eq!(BqlType::from_str("Float64")?, BqlType::Float(64));

        assert_eq!(
            BqlType::from_str("LowCardinality(String)")?,
            BqlType::LowCardinalityString
        );

        assert!(matches!(
            BqlType::from_str("Decimal(0, 0)"), // p < 1
            Err(MetaError::InvalidPrecisionOrScaleOfDecimalError(0, 0))
        ));
        assert!(matches!(
            BqlType::from_str("Decimal(77, 0)"), // p < 76
            Err(MetaError::InvalidPrecisionOrScaleOfDecimalError(77, 0))
        ));
        assert!(matches!(
            BqlType::from_str("Decimal(4, 9)"), // s > p
            Err(MetaError::InvalidPrecisionOrScaleOfDecimalError(4, 9))
        ));
        assert!(matches!(
            BqlType::from_str("Decimal32( 10)"), // s > p for Decimal32
            Err(MetaError::InvalidPrecisionOrScaleOfDecimalError(9, 10))
        ));
        assert!(matches!(
            BqlType::from_str("Decimal64( 19)"), // s > p for Decimal64
            Err(MetaError::InvalidPrecisionOrScaleOfDecimalError(18, 19))
        ));

        assert!(matches!(BqlType::from_str("FixedString(256)"), Err(_)));
        assert!(matches!(BqlType::from_str("FixedString(11 , )"), Err(_)));
        assert!(matches!(BqlType::from_str("FixedString(,)"), Err(_)));
        assert!(matches!(BqlType::from_str("FixedString()"), Err(_)));
        assert!(matches!(BqlType::from_str("FixedString(13, 15)"), Err(_)));
        assert!(matches!(BqlType::from_str("Decimal(11 , )"), Err(_)));
        assert!(matches!(BqlType::from_str("Decimal(11 , 9, )"), Err(_)));
        assert!(matches!(BqlType::from_str("Decimal(11, 9, 3)"), Err(_)));
        assert!(matches!(BqlType::from_str("Decimal(, 11, 9)"), Err(_)));
        assert!(matches!(BqlType::from_str("Decimal32(4 , )"), Err(_)));
        assert!(matches!(BqlType::from_str("Decimal64(, 10 )"), Err(_)));
        assert!(matches!(BqlType::from_str("UInt1234"), Err(_)));

        assert_eq!(BqlType::from_str("DateTime")?, BqlType::DateTime(None));
        assert_eq!(
            BqlType::from_str("DateTime('UTC')")?,
            BqlType::DateTime(Some(TimeZoneId::from(Tz::UTC)))
        );
        assert_eq!(
            BqlType::from_str("DateTime( 'Etc/GMT-8' )")?,
            BqlType::DateTime(Some(TimeZoneId::from(Tz::Etc__GMTMinus8)))
        );
        assert_eq!(
            BqlType::from_str("DateTime( 'America/Los_Angeles')")?,
            BqlType::DateTime(Some(TimeZoneId::from(Tz::America__Los_Angeles)))
        );
        assert!(matches!(
            BqlType::from_str("DateTime('Invalid timezone')"),
            Err(_)
        ));

        Ok(())
    }

    #[test]
    fn test_bqltype_to_vec() -> MetaResult<()> {
        assert_eq!(b"String".to_vec(), BqlType::String.to_vec()?);
        assert_eq!(b"Decimal(11,3)".to_vec(), BqlType::Decimal(11, 3).to_vec()?);
        assert_eq!(b"Int8".to_vec(), BqlType::Int(8).to_vec()?);
        assert_eq!(b"Int32".to_vec(), BqlType::Int(32).to_vec()?);
        assert_eq!(b"UInt32".to_vec(), BqlType::UInt(32).to_vec()?);
        assert_eq!(b"UInt64".to_vec(), BqlType::UInt(64).to_vec()?);
        assert_eq!(b"Float16".to_vec(), BqlType::Float(16).to_vec()?);
        assert_eq!(b"Float32".to_vec(), BqlType::Float(32).to_vec()?);
        assert_eq!(b"Float64".to_vec(), BqlType::Float(64).to_vec()?);
        assert_eq!(
            b"LowCardinality(String)".to_vec(),
            BqlType::LowCardinalityString.to_vec()?
        );
        assert_eq!(b"DateTime".to_vec(), BqlType::DateTime(None).to_vec()?);
        assert_eq!(
            b"DateTime('UTC')".to_vec(),
            BqlType::DateTime(Some(TimeZoneId::from(Tz::UTC))).to_vec()?
        );
        assert_eq!(
            b"DateTime('Etc/GMT-8')".to_vec(),
            BqlType::DateTime(Some(TimeZoneId::from(Tz::Etc__GMTMinus8))).to_vec()?
        );
        assert_eq!(
            b"DateTime('America/Los_Angeles')".to_vec(),
            BqlType::DateTime(Some(TimeZoneId::from(Tz::America__Los_Angeles)))
                .to_vec()?
        );
        assert_eq!(b"Date".to_vec(), BqlType::Date.to_vec()?);
        assert_eq!(
            b"FixedString(193)".to_vec(),
            BqlType::FixedString(193).to_vec()?
        );
        assert_eq!(
            b"FixedString(255)".to_vec(),
            BqlType::FixedString(0xff).to_vec()?
        );

        Ok(())
    }

    #[test]
    fn test_table_sort_columns() {
        let mut tab = Table::default();
        tab.columns.push((
            "a".to_string(),
            ColumnInfo {
                ordinal: 2,
                ..Default::default()
            },
        ));
        tab.columns.push((
            "b".to_string(),
            ColumnInfo {
                ordinal: 3,
                ..Default::default()
            },
        ));
        tab.columns.push((
            "c".to_string(),
            ColumnInfo {
                ordinal: 1,
                ..Default::default()
            },
        ));
        tab.columns.push((
            "d".to_string(),
            ColumnInfo {
                ordinal: 0,
                ..Default::default()
            },
        ));
        tab.sort_columns_by_ordinal();
        println!("columns: {:?}", tab.columns);
    }

    #[test]
    fn test_type_sizes() {
        show_option_size!(BqlType);
    }
}
