//! # Clickhouse Server protocol
//! Clickhouse uses for communication proprietary  protocol and bespoke binary message formats.
//! The protocol is message-oriented request-response based and uses TCP (TLS) as transport layer.
//! Official documentation doesn't have detailed information about it.
//! This driver implementation is entirely based on source code  of others open-source clickhouse drivers
//! and tcp dump analysis.
//!
//!
//! ##  SERVER_DATA and CLIENT_DATA
//! --------message----------
//! PKID TMPF BLK or CBLK ( 1 or more)
//!   1   0
//! | - | - | ----------- |
//!
//! PKID - Packet ID (1-CLIENT DATA; 2-SERVER DATA), 1 byte
//! TMPF - Temporary Table Name. (0-NULL)
//! BLK  - Raw Block
//! CBLK - Compressed Block
//!
//! --------raw block------------
//!  BI CN RN  COLS
//! |--|--|--|------|
//!
//! BI   - Block Info
//! CN   - Number of Columns (varint)
//! RN   - Number of Rows (varint)
//! COLS - Columns serialized data, data type specific
//!
//! --------block info---------
//!  ? OVF ? BUCK ?
//! |-| - |-|----|-|
//!
//! OVF  - Overflow flag, 0/1
//! BUCK - Bucket number, -1 - not specified (default)
//!
//! --------compressed block----
//!  CTH ALG CSZ DSZ LZ4 BLK
//! |---| - |---|---|-------|
//!
//! CTH - 16 byte CityHash 1.0 Hashsum CityHash128(ALG---LZ4 BLK)
//! ALG - 1 byte Algorithm of compression. 0x82 - LZ4
//! CSZ - Compressed Block size ,  [ALG .. LZ4 BLK]
//! DSZ - Decompressed Block size
//! LZ4 BLK - LZ4 compressed Raw Block
//!
//! ## Block data
//! Block contains series of columns each of them has header and body
//!  -------- block (BLK ) ---------------------
//!        1-column         2-column         next
//! | BCH | BCB      |  BCH  | BCB      | ....
//!  BCH - Block column header
//!  BCB - Block column body
//!
//!  --------- block header (BCH) -------------
//!  | NM  |  TYPE  | CD |
//!  NM - table column name
//!  TYPE - table column type and metadata (i.e Enum type has it dictionary)
//!  CD - column data.
//!
//!  Column data format is type dependant.
//! ## Column values serialization
//! Each data type has it own binary format
//!
//! (U)Int8(16|32|64) and Float32|Float64 data types encoded as the corresponding numeric type in little endian order.
//! String data type  is encoded as sequence of  length + data, there length is varInt encoded
//! FixedString(len) is encoded as sequence of  data with fixed length
//! Date - 2 byte values. Little endian 16 bit signed integer - number of days from epoch (1 Jan 1970 )
//! DateTime - 4 byte value. Little endian 32 bit signed integer - number of seconds from epoch
//! DateTime64 - 8 byte value. Little endian 64 bit signed integer - number of ticks from epoch.
//!   The size in seconds of each tick is defined by scale factor of DateTime64 type metadata.
//!   Scale is 1 byte integer and can take values from 0 to 9.
//!   0 - each second is 1 tick
//!   1 - each second is 10 ticks
//!   n - each second is 10^n ticks
//!  UUID - 16 byte value
//!  IPv4 and IPv6 as corresponding 4 and 16 byte big-endian values (network endian)
//!  Enum8 and Enum16 encoded as array of 1 (or 2 for Enum16) integer values which are keys in
//!   enum dictionary
//!
//! Nullable(BaseType) has additional array of nullable flags. This array has 1-byte values of
//!  0 - (not null ) , 1 - null.  The array has the same length as BaseType data.
//! Null array is stored in column format before Base type.

use crate::client::ServerInfo;
use crate::errors::{ConversionError, Result};
use crate::types::Field;
use std::io::{self, Write};
#[cfg(feature = "int128")]
use value::ValueDecimal128;
use value::{
    ValueDate, ValueDateTime, ValueDateTime64, ValueDecimal32, ValueDecimal64, ValueIp4, ValueIp6,
    ValueUuid,
};

#[cfg(feature = "extra")]
mod simd;

pub mod block;
pub mod code;
pub mod column;
pub mod command;
mod decoder;
pub mod encoder;
pub mod insert;
pub mod packet;
pub mod query;
pub mod value;

/// This trait provides common interface for client request serialization.
/// ServerInfo parameter keeps server specific options (revision, compression method ...)
/// and defines encoded rules, version specific options, and timezone.
pub(crate) trait ServerWriter {
    fn write(&self, cx: &ServerInfo, writer: &mut dyn Write) -> io::Result<()>;
}

/// Read the RIP register (instruction pointer).
/// Used to detect code duplicates
#[cfg(rustc_nightly)]
#[feature(asm)]
#[allow(dead_code)]
#[inline(always)]
pub(crate) fn rip() -> u64 {
    let rip: u64;
    unsafe {
        asm!("lea {}, 0[rip]", out(reg) rip);
    }
    rip
}

/// Gateway from rust to dynamic sql data of Clickhouse Row
#[derive(Debug)]
pub enum ValueRefEnum<'a> {
    String(&'a [u8]),
    Array8(&'a [u8]),
    Array16(&'a [u16]),
    Array32(&'a [u32]),
    Array64(&'a [u64]),
    Array128(&'a [u128]),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    #[cfg(feature = "int128")]
    UInt128(u128),
    Float32(f32),
    Float64(f64),
    Date(ValueDate),
    DateTime(ValueDateTime),
    DateTime64(ValueDateTime64),
    Enum(i16),
    Ip4(ValueIp4),
    Ip6(ValueIp6),
    Uuid(ValueUuid),
    Decimal32(ValueDecimal32),
    Decimal64(ValueDecimal64),
    #[cfg(feature = "int128")]
    Decimal128(ValueDecimal128),
}

/// Row value getter interface
pub trait Value<'a, T: 'a> {
    fn get(&'a self, _: &'a Field) -> Result<Option<T>> {
        Err(ConversionError::UnsupportedConversion.into())
    }
}

#[cfg(test)]
mod test {
    use super::ValueRefEnum;
    use crate::protocol::column::{EnumIndex, ValueRef};
    use crate::types::FieldMeta;
    use std::mem::size_of;

    macro_rules! into_boxed {
        ($s: expr) => {
            $s.to_owned().into_boxed_str().into_boxed_bytes()
        };
    }

    #[test]
    fn test_meta_index() {
        let index = vec![
            EnumIndex(1i16, into_boxed!("yes")),
            EnumIndex(-1i16, into_boxed!("n/a")),
            EnumIndex(0i16, into_boxed!("no")),
            EnumIndex(-2i16, into_boxed!("unknown")),
        ];
        let mut meta = FieldMeta { index };
        //sort by value
        meta.index.sort_unstable_by_key(EnumIndex::fn_sort_val);
        assert_eq!(meta.index[0].0, -2);
        assert_eq!(meta.index[1].0, -1);
        assert_eq!(meta.index[2].0, 0);
        assert_eq!(meta.index[3].0, 1);

        assert_eq!(meta.val2str(-1), b"n/a");
        assert_eq!(meta.val2str(0), b"no");
        assert_eq!(meta.val2str(-2), b"unknown");
        assert_eq!(meta.val2str(1), b"yes");

        meta.index.sort_unstable_by(EnumIndex::fn_sort_str);

        assert_eq!(meta.str2val(b"no").unwrap(), 0i16);
        assert_eq!(meta.str2val(b"yes").unwrap(), 1i16);
        assert_eq!(meta.str2val(b"unknown").unwrap(), -2i16);
        assert_eq!(meta.str2val(b"n/a").unwrap(), -1i16);
        assert!(meta.str2val(b"some other").is_err());
    }

    #[test]
    fn test_valueref_size() {
        let valueref_size = size_of::<ValueRef>();
        let valuerefenum_size = size_of::<ValueRefEnum>();

        assert_eq!(valueref_size, valuerefenum_size);
        assert!(
            valueref_size <= 24,
            "ValueRef should be smaller than 32 bytes. 16 bytes - data + 8 bytes descriptor"
        );
    }
}
