use std::{
    cell::Ref,
    cell::RefMut,
    collections::HashMap,
    mem,
    ops::{Deref, DerefMut},
    slice,
};

use base::bytes_cat;
use num_traits::PrimInt;
use std::str;

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
    DateTime,
    String,
    LowCardinalityString,
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
impl BqlType {
    /// return the data type size in bytes, Err for dynamically sized type
    pub fn size(self) -> MetaResult<u8> {
        match self {
            BqlType::Int(siz) => Ok(siz / 8),
            BqlType::UInt(siz) => Ok(siz / 8),
            BqlType::DateTime => Ok(4),
            BqlType::Decimal(p, _s) => {
                if p < 10 { 
                    Ok(4) 
                } else { 
                    Ok(8)
                }
            },
            // BqlType::LowCardinalityString => Ok(128),
            _ => Err(MetaError::NoFixedSizeDataTypeError),
        }
    }

    pub fn to_vec(self) -> MetaResult<Vec<u8>> {
        match self {
            BqlType::UnInit => Ok(b"UnInit".to_vec()),
            BqlType::UInt(len) => {
                let mut bi = [0u8; 4];
                let n = itoa::write(&mut bi[..], len)
                    .map_err(|e| MetaError::WrappingIOError(e))?;
                Ok(bytes_cat!(b"UInt", &bi[..n]))
            }
            BqlType::Int(len) => {
                let mut bi = [0u8; 4];
                let n = itoa::write(&mut bi[..], len)
                    .map_err(|e| MetaError::WrappingIOError(e))?;
                Ok(bytes_cat!(b"Int", &bi[..n]))
            }
            BqlType::Decimal(p, s) => {
                let mut bp = [0u8; 4];
                let np = itoa::write(&mut bp[..], p)
                    .map_err(|e| MetaError::WrappingIOError(e))?;
                let mut bs = [0u8; 4];
                let ns = itoa::write(&mut bs[..], s)
                    .map_err(|e| MetaError::WrappingIOError(e))?;
                Ok(bytes_cat!(b"Decimal(", &bp[..np], b",", &bs[..ns], b")"))
            }
            BqlType::DateTime => Ok(b"DateTime".to_vec()),
            BqlType::String => Ok(b"String".to_vec()),
            BqlType::LowCardinalityString => {
                Ok(b"LowCardinality(String)".to_vec())
            }
        }
    }

    pub fn to_c_typ_str<'a>(self) -> MetaResult<&'a str> {
        match self {
            BqlType::UnInit => Ok("UnInit"),
            BqlType::UInt(len) if len == 8 => Ok("uint8_t"),
            BqlType::UInt(len) if len == 16 => Ok("uint16_t"),
            BqlType::UInt(len) if len == 32 => Ok("uint32_t"),
            BqlType::UInt(len) if len == 64 => Ok("uint64_t"),
            BqlType::Int(len) if len == 8 => Ok("int8_t"),
            BqlType::Int(len) if len == 16 => Ok("int16_t"),
            BqlType::Int(len) if len == 32 => Ok("int32_t"),
            BqlType::Int(len) if len == 64 => Ok("int64_t"),
            BqlType::DateTime | BqlType::LowCardinalityString => Ok("uint32_t"),
            _ => Err(MetaError::UnsupportedBqlTypeError),
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
            b"DateTime" => Ok(BqlType::DateTime),
            b"String" => Ok(BqlType::String),
            b"LowCardinality(String)" => Ok(BqlType::LowCardinalityString),
            [b'D', b'e', b'c', b'i', b'm', b'a', b'l', b'(', ps @ .., b')'] => {
                let mut ps_iter = ps.split(|v| *v == b',');
                let p0 = str::from_utf8(
                    ps_iter
                        .next()
                        .ok_or(MetaError::UnknownBqlTypeConversionError)?,
                )
                .map_err(|_e| MetaError::UnknownBqlTypeConversionError)?;
                let s0 = str::from_utf8(
                    ps_iter
                        .next()
                        .ok_or(MetaError::UnknownBqlTypeConversionError)?,
                )
                .map_err(|_e| MetaError::UnknownBqlTypeConversionError)?;
                let p = p0
                    .trim()
                    .parse::<u8>()
                    .map_err(|_e| MetaError::UnknownBqlTypeConversionError)?;
                let s = s0
                    .trim()
                    .parse::<u8>()
                    .map_err(|_e| MetaError::UnknownBqlTypeConversionError)?;
                //FIXME stronger validations
                Ok(BqlType::Decimal(p, s))
            }
            _ => Err(MetaError::UnknownBqlTypeConversionError),
        }
    }
}

// #[derive(Debug, Copy, Clone, Eq, PartialEq)]
// #[repr(C)]
// pub struct CBqlType(pub u8, pub u16);

// impl Default for CBqlType {
//     fn default() -> Self {
//         CBqlType(CBqlTypeBase::UnInit as u8, 0u16)
//     }
// }

// impl CBqlType {
//     pub fn from_btype(btype: BqlType) -> MetaResult<Self> {
//         match btype {
//             BqlType::UInt(siz) => {
//                 Ok(CBqlType(CBqlTypeBase::UInt as u8, siz as u16))
//             }
//             BqlType::Int(siz) => {
//                 Ok(CBqlType(CBqlTypeBase::Int as u8, siz as u16))
//             }
//             BqlType::DateTime => Ok(CBqlType(CBqlTypeBase::UInt as u8, 32)),
//             _ => Err(MetaError::UnknownCBqlTypeConversionError),
//         }
//     }
// }

// #[repr(C)]
// pub enum CBqlTypeBase {
//     UnInit = 0,
//     UInt = 1,
//     Int = 2,
//     Decimal = 3,
//     DateTime = 4,
//     LowCardinalityString = 5,
//     String = 6,
// }

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

// pub struct NamedBaseChunk {
//     pub name: Vec<u8>,
//     pub data: BaseChunk,
// }

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

#[derive(PartialEq, Debug)]
#[repr(C, packed)]
pub struct Part<T> {
    pub addr: usize,
    pub size: usize,
    pub fd: u32,
    pub min: T,
    pub max: T,
    pub nbits: u16,
}

impl<T> Part<T> {
    /// Get the bytes of this value.
    ///
    /// `as_bytes` provides access to the bytes of this value as an immutable
    /// byte slice.
    #[allow(dead_code)] 
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
    #[allow(dead_code)] 
    fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            // NOTE: This function does not have a Self: Sized bound.
            // size_of_val works for unsized values too.
            let len = mem::size_of_val(self);
            slice::from_raw_parts_mut(self as *mut Self as *mut u8, len)
        }
    }
}

// impl <T> PartialEq for Part<T> {
//     fn eq(&self, other: &Self) -> bool {
//         self.addr == other.addr && self.size == other.size
//     }
// }
// #[derive(PartialEq, Debug)]
// #[repr(C, packed)]
// pub struct PartKeyPrimInt<T: PrimInt> {
//     pub tid_be: Id,
//     pub part_key: T,
// }

// impl<T: PrimInt> PartKeyPrimInt<T> {
//     pub fn new(tid: u64, part_key: T) -> Self {
//         PartKeyPrimInt {
//             tid_be: tid.to_be(),
//             part_key,
//         }
//     }

//     /// Get the bytes of this value.
//     ///
//     /// `as_bytes` provides access to the bytes of this value as an immutable
//     /// byte slice.
//     pub fn as_bytes(&self) -> &[u8] {
//         unsafe {
//             // NOTE: This function does not have a Self: Sized bound.
//             // size_of_val works for unsized values too.
//             let len = mem::size_of_val(self);
//             slice::from_raw_parts(self as *const Self as *const u8, len)
//         }
//     }

//     /// Get the bytes of this value mutably.
//     ///
//     /// `as_bytes_mut` provides access to the bytes of this value as a mutable
//     /// byte slice.
//     pub fn as_bytes_mut(&mut self) -> &mut [u8] {
//         unsafe {
//             // NOTE: This function does not have a Self: Sized bound.
//             // size_of_val works for unsized values too.
//             let len = mem::size_of_val(self);
//             slice::from_raw_parts_mut(self as *mut Self as *mut u8, len)
//         }
//     }
// }

#[cfg(test)]
mod unit_tests {
    use base::show_option_size;

    use super::*;

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
            BqlType::from_str("Decimal(11 , 3)")?,
            BqlType::Decimal(11, 3)
        );
        assert_eq!(
            BqlType::from_str("Decimal( 9, 2)")?,
            BqlType::Decimal(9, 2)
        );
        assert_eq!(BqlType::from_str("Int8")?, BqlType::Int(8));
        assert_eq!(BqlType::from_str("Int32")?, BqlType::Int(32));
        assert_eq!(BqlType::from_str("UInt32")?, BqlType::UInt(32));
        assert_eq!(BqlType::from_str("UInt64")?, BqlType::UInt(64));

        assert_eq!(
            BqlType::from_str("LowCardinality(String)")?,
            BqlType::LowCardinalityString
        );

        assert!(matches!(BqlType::from_str("Decimal(11 , )"), Err(_)));
        assert!(matches!(BqlType::from_str("UInt1234"), Err(_)));

        Ok(())
    }

    #[test]
    fn test_bqltype_to_vec() -> MetaResult<()> {
        assert_eq!(b"String".to_vec(), BqlType::String.to_vec()?);
        assert_eq!(
            b"Decimal(11,3)".to_vec(),
            BqlType::Decimal(11, 3).to_vec()?
        );
        assert_eq!(b"Int8".to_vec(), BqlType::Int(8).to_vec()?);
        assert_eq!(b"Int32".to_vec(), BqlType::Int(32).to_vec()?);
        assert_eq!(b"UInt32".to_vec(), BqlType::UInt(32).to_vec()?);
        assert_eq!(b"UInt64".to_vec(), BqlType::UInt(64).to_vec()?);
        assert_eq!(
            b"LowCardinality(String)".to_vec(),
            BqlType::LowCardinalityString.to_vec()?
        );
        assert_eq!(b"DateTime".to_vec(), BqlType::DateTime.to_vec()?);

        Ok(())
    }

    // #[test]
    // fn test_part_basic() {
    //     let p = Part {
    //         addr: 0x54_32_10_12_34_00,
    //         size: 120000,
    //         min: 1u32,
    //         max: 63u32 * 1024,
    //         nbits: 32,
    //     };

    //     let bsp = p.as_bytes();
    //     assert_eq!(bsp.len(), 8 + 8 + 4 + 4 + 2);

    //     assert_eq!(bsp.into_ref::<Part<u32>>(), &p);

    //     let p = Part {
    //         addr: 0x54_32_10_12_34_00,
    //         size: 120000,
    //         min: 1u64,
    //         max: 63 * 1024,
    //         nbits: 32,
    //     };

    //     let bsp = p.as_bytes();
    //     assert_eq!(bsp.len(), 8 + 8 + 8 + 8 + 2);

    //     assert_eq!(bsp.into_ref::<Part<u64>>(), &p);
    //     assert_eq!(bsp.into_ref::<Part<u64>>().nbits, 32);
    // }

    // #[test]
    // fn test_part_key_basic() {
    //     let ptk = PartKeyPrimInt::new(127, 1u64);

    //     let bs_ptk = ptk.as_bytes();
    //     assert_eq!(bs_ptk.len(), 8 + 8);

    //     assert_eq!(bs_ptk.into_ref::<PartKeyPrimInt<u64>>(), &ptk);
    //     // &ptk.try_into::<[u8;4]>().expect("slice with incorrect length");

    //     let ptk = PartKeyPrimInt::new(127, -123i32);
    //     let bs_ptk = ptk.as_bytes();
    //     assert_eq!(bs_ptk.len(), 8 + 4);

    //     let ptk1 = bs_ptk.into_ref::<PartKeyPrimInt<i32>>();
    //     assert_eq!(*ptk1, ptk);
    //     assert_eq!(ptk1.tid_be, 127.to_be());
    //     assert_eq!(ptk1.part_key, -123i32);
    // }

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
        // show_option_size!(CBqlType);
        // show_option_size!(CBqlTypeBase);
    }
}
