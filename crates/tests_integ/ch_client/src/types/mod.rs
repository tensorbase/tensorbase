#[cfg(feature = "int128")]
pub use crate::protocol::value::ValueDecimal128;
pub use crate::protocol::value::{
    ValueDate, ValueDateTime, ValueDateTime64, ValueDecimal32, ValueDecimal64, ValueIp4, ValueIp6,
    ValueUuid,
};
use chrono_tz::Tz;
#[cfg(feature = "int128")]
pub use decimal::Decimal128;
pub use decimal::{Decimal, Decimal32, Decimal64, DecimalBits};
use std::fmt;
use std::io;
use std::io::Write;

use crate::errors::{ConversionError, Result};
use crate::protocol::column::EnumIndex;
use crate::protocol::encoder::{Encoder, StringEncoderAdapter};
pub use parser::parse_type_field;
use std::fmt::Formatter;

mod decimal;
mod parser;

/// Column field metadata
/// for now it's used for Enum data type `value <-> string` transcode
/// in serialization/deserialization process
pub struct FieldMeta {
    pub(crate) index: Vec<EnumIndex<i16>>,
}

impl FieldMeta {
    /// Searches enum value by string representation
    /// Return i16 value regardless of the data type Enum8 or Enum16
    /// For Enum8 returning value can be safely cast to i8
    ///
    /// Require index to be sorted by str.
    /// `meta.index.sort_unstable_by(EnumIndex::fn_sort_str)`
    #[inline]
    pub(crate) fn str2val(&self, title: &[u8]) -> Result<i16> {
        let index = self
            .index
            .binary_search_by(|item| item.1.as_ref().cmp(title))
            .map_err(|_| ConversionError::EnumMismatch(title.to_vec()))?;

        debug_assert!(index < self.index.len(), "enum index corrupted");
        Ok(unsafe { self.index.get_unchecked(index).0 })
    }
    #[inline]
    pub(crate) fn val2str(&self, val: i16) -> &[u8] {
        let index = self
            .index
            .binary_search_by_key(&val, EnumIndex::fn_sort_val)
            .expect("enum index corrupted");

        debug_assert!(index < self.index.len(), "enum index corrupted");
        unsafe { self.index.get_unchecked(index).1.as_ref() }
    }
}

pub const FIELD_NONE: u8 = 0x00;
pub const FIELD_NULLABLE: u8 = 0x01;
pub const FIELD_LOWCARDINALITY: u8 = 0x02;
pub const FIELD_ARRAY: u8 = 0x04;

pub struct Field {
    pub(crate) sql_type: SqlType,
    pub(crate) flag: u8,
    pub(crate) depth: u8,
    meta: Option<FieldMeta>,
}

impl fmt::Debug for Field {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Field")
            .field("sql_type", &self.sql_type)
            .field("flag", &self.flag)
            .field("depth", &self.depth)
            .finish()
    }
}

impl Clone for Field {
    fn clone(&self) -> Self {
        Field {
            sql_type: self.sql_type,
            flag: self.flag,
            depth: self.depth,
            meta: {
                match &self.meta {
                    None => None,
                    Some(meta) => Some(FieldMeta {
                        index: meta.index.to_vec(),
                    }),
                }
            },
        }
    }
}

impl Field {
    pub fn get_meta(&self) -> Option<&FieldMeta> {
        self.meta.as_ref()
    }
    pub fn get_meta_mut(&mut self) -> Option<&mut FieldMeta> {
        self.meta.as_mut()
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        (self.flag & FIELD_NULLABLE) == FIELD_NULLABLE
    }
    #[inline]
    pub fn is_array(&self) -> bool {
        (self.flag & FIELD_ARRAY) == FIELD_ARRAY
    }
    #[inline]
    pub fn is_lowcardinality(&self) -> bool {
        (self.flag & FIELD_LOWCARDINALITY) == FIELD_LOWCARDINALITY
    }

    fn write_enum(meta: &FieldMeta, writer: &mut dyn Write) -> io::Result<()> {
        let mut iter = meta.index.iter();
        write!(writer, "(")?;
        if let Some(item) = iter.next() {
            //writer.write_fmt( )?;
            write!(writer, "{}", item)?;
        }
        for item in iter {
            write!(writer, ",{}", item)?;
        }

        write!(writer, ")")?;
        Ok(())
    }
}

impl Encoder for Field {
    fn encode(&self, writer: &mut dyn Write) -> io::Result<()> {
        let mut type_adapter = StringEncoderAdapter::new(writer);

        if self.is_nullable() {
            write!(type_adapter, "Nullable(")?;
        };

        write!(type_adapter, "{}", self.sql_type)?;

        if self.sql_type == SqlType::Enum8 || self.sql_type == SqlType::Enum16 {
            Field::write_enum(
                self.meta.as_ref().expect("enum index corrupted"),
                &mut type_adapter,
            )?;
        }

        if self.is_nullable() {
            write!(type_adapter, ")")?;
        };
        Ok(())
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum SqlType {
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    String,
    FixedString(u32),
    Float32,
    Float64,
    Date,
    DateTime,
    DateTime64(u8, Tz),
    Ipv4,
    Ipv6,
    Uuid,
    Decimal(u8, u8),
    Enum8,
    Enum16,
    // type placeholders. don't really instantiate this types
    Array,
    LowCardinality,
}

impl fmt::Display for SqlType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let name = match self {
            SqlType::String => "String",
            SqlType::UInt8 => "UInt8",
            SqlType::UInt16 => "UInt16",
            SqlType::UInt32 => "UInt32",
            SqlType::UInt64 => "UInt64",
            SqlType::Int8 => "Int8",
            SqlType::Int16 => "Int16",
            SqlType::Int32 => "Int32",
            SqlType::Int64 => "Int64",
            SqlType::Uuid => "UUID",
            SqlType::Ipv4 => "IPv4",
            SqlType::Ipv6 => "IPv6",
            SqlType::Float32 => "Float32",
            SqlType::Float64 => "Float64",
            SqlType::Date => "Date",
            SqlType::DateTime => "DateTime",
            SqlType::FixedString(p) => return f.write_fmt(format_args!("FixedString({})", p)),
            SqlType::DateTime64(p, _) => return f.write_fmt(format_args!("DateTime64({})", p)),
            SqlType::Decimal(p, s) => return f.write_fmt(format_args!("Decimal({},{})", p, s)),
            SqlType::Enum8 => "Enum8",
            SqlType::Enum16 => "Enum16",
            _ => unimplemented!(""),
        };

        f.write_str(name)
    }
}

pub(crate) static SCALE: &[i64] = &[
    1,
    10,
    100,
    1000,
    10000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
];

#[cfg(test)]
mod test {
    use super::Field;
    use crate::protocol::column::EnumIndex;
    use crate::protocol::encoder::Encoder;
    use crate::types::{FieldMeta, SqlType, FIELD_NONE, FIELD_NULLABLE};

    macro_rules! into_boxed {
        ($s: expr) => {
            $s.to_owned().into_boxed_str().into_boxed_bytes()
        };
    }
    macro_rules! into_string {
        ($v: ident ) => {
            String::from_utf8_lossy($v.as_slice())
        };
    }

    #[test]
    fn test_field_format_basic() {
        let mut buf = Vec::new();
        let f = Field {
            sql_type: SqlType::String,
            flag: FIELD_NONE,
            depth: 0,
            meta: None,
        };

        f.encode(&mut buf).unwrap();
        assert_eq!(into_string!(buf), "\u{6}String")
    }
    #[test]
    fn test_field_format_nullable() {
        let mut buf = Vec::new();
        let f = Field {
            sql_type: SqlType::String,
            flag: FIELD_NULLABLE,
            depth: 0,
            meta: None,
        };
        f.encode(&mut buf).unwrap();
        assert_eq!(into_string!(buf), "\u{10}Nullable(String)");
    }

    #[test]
    fn test_field_format_param() {
        let mut buf = Vec::new();
        let f = Field {
            sql_type: SqlType::FixedString(20),
            flag: FIELD_NULLABLE,
            depth: 0,
            meta: None,
        };
        f.encode(&mut buf).unwrap();
        assert_eq!(into_string!(buf), "\u{19}Nullable(FixedString(20))");
        buf.clear();
        let f = Field {
            sql_type: SqlType::Decimal(18, 4),
            flag: FIELD_NULLABLE,
            depth: 0,
            meta: None,
        };
        f.encode(&mut buf).unwrap();
        assert_eq!(into_string!(buf), "\u{17}Nullable(Decimal(18,4))");
    }
    #[test]
    fn test_field_format_enum() {
        let mut buf = Vec::new();
        let index = vec![
            EnumIndex(0, into_boxed!("no")),
            EnumIndex(1, into_boxed!("yes")),
            EnumIndex(2, into_boxed!("n'a")),
        ];
        let f = Field {
            sql_type: SqlType::Enum8,
            flag: FIELD_NONE,
            depth: 0,
            meta: Some(FieldMeta { index }),
        };
        f.encode(&mut buf).unwrap();
        assert_eq!(into_string!(buf), "$Enum8('no' = 0,'yes' = 1,'n\\'a' = 2)");
    }
}
