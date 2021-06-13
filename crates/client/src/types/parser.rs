//! Enum type takes  Enum8() or Enum16() forms.
//! Enum8 values range is -128..+127, so can hold up to 255 different values
//! Enum16 values range is -32768..32767 and can hold up to 65536 values
//! In column header  Enums values stored with type definition in metadata block and represented as string
//! "Enum8('title1' = 0, 'title2' = 2, ...)"
//!
use super::{Field, FieldMeta, SqlType};
use crate::errors::{self, ConversionError, DriverError, Result};
use crate::protocol::column::EnumIndex;
use crate::types::{FIELD_ARRAY, FIELD_LOWCARDINALITY, FIELD_NONE, FIELD_NULLABLE};
use chrono_tz::Tz;
use std::fmt::{Debug, Formatter};
use std::str::FromStr;

pub(crate) enum EnumParser<'a> {
    Start,
    Name(usize),
    BeforeEqual(&'a [u8]),
    AfterEqual(&'a [u8]),
    Value(i8, i16, &'a [u8]),
    Separator,
}

impl Debug for EnumParser<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            EnumParser::Start => "Start",
            EnumParser::Name(_) => "Name",
            EnumParser::BeforeEqual(_) => "BeforeEqual",
            EnumParser::AfterEqual(..) => "AfterEqual",
            EnumParser::Value(..) => "Value",
            EnumParser::Separator => "Separator",
        };
        f.write_str(s)
    }
}

macro_rules! field {
    ($sql_type: expr) => {
        Ok(Field {
            sql_type: $sql_type,
            depth: 0,
            flag: FIELD_NONE,
            meta: None,
        })
    };
}

impl<'a> EnumParser<'a> {
    pub(crate) fn parse_str(type_str: &'a str) -> Result<Field> {
        let (sql_type, rest) = if type_str.starts_with("Enum8(") {
            (SqlType::Enum8, &type_str[6..])
        } else if type_str.starts_with("Enum16(") {
            (SqlType::Enum16, &type_str[7..])
        } else {
            return Err(errors::ConversionError::UnknownColumnType(type_str.to_owned()).into());
        };

        let mut state = EnumParser::Start;
        // let mut title: Vec<Box<[u8]>> = Vec::new();
        let mut index: Vec<EnumIndex<i16>> = Vec::new();
        let mut chars = rest.char_indices();
        while let Some((i, c)) = chars.next() {
            match state {
                EnumParser::Start => {
                    if c == '\'' {
                        state = EnumParser::Name(i);
                    }
                }
                EnumParser::Name(ref mut pos) => {
                    if c == '\\' {
                        chars.next();
                    } else if c == '\'' {
                        state = EnumParser::BeforeEqual(&rest[*pos + 1..i].as_bytes());
                    }
                }
                EnumParser::BeforeEqual(s) => {
                    if c == '=' {
                        state = EnumParser::AfterEqual(s);
                    }
                    // ignore other characters
                }
                EnumParser::AfterEqual(s) => {
                    if c.is_whitespace() {
                        // eat whitespaces before digits
                    } else if c == '-' {
                        state = EnumParser::Value(-1, 0, s);
                    } else if c.is_digit(10) {
                        let val = c.to_digit(10).unwrap() as i16;
                        state = EnumParser::Value(1, val, s);
                    } else {
                        return Err(errors::ConversionError::UnknownColumnType(
                            "Enum(..)".to_owned(),
                        )
                        .into());
                    }
                }
                EnumParser::Value(ref sign, ref mut val, s) => {
                    if c.is_whitespace() || c == ',' || c == ')' {
                        if *sign < 0 {
                            *val = -*val;
                        }
                        index.push(EnumIndex(*val, s.to_owned().into_boxed_slice()));

                        state = EnumParser::Start;
                        if c == ')' {
                            break;
                        } else if c != ',' {
                            state = EnumParser::Separator;
                        }
                    } else if c.is_digit(10) {
                        *val *= 10;
                        *val += c.to_digit(10).unwrap() as i16;
                    } else {
                        return Err(errors::ConversionError::UnknownColumnType(
                            "Enum(..)".to_owned(),
                        )
                        .into());
                    }
                }
                EnumParser::Separator => {
                    if c == ',' {
                        state = EnumParser::Start;
                    } else if c == ')' {
                        state = EnumParser::Start;
                        break;
                    }
                }
            }
        }
        if let EnumParser::Start = state {
            index.sort_unstable_by_key(EnumIndex::fn_sort_val);
            Ok(Field {
                sql_type,
                flag: FIELD_NONE,
                depth: 0,
                meta: Some(FieldMeta { index }),
            })
        } else {
            Err(errors::ConversionError::UnknownColumnType("Enum(..)".to_owned()).into())
        }
    }
}

pub enum DecimalParser {
    S(u8),
    P(u8, u8),
}

impl DecimalParser {
    pub(crate) fn parse_str(type_str: &str) -> Result<Field> {
        let (mut state, rest) = if type_str.starts_with("Decimal32(") {
            (DecimalParser::P(9, 0), &type_str[10..])
        } else if type_str.starts_with("Decimal64(") {
            (DecimalParser::P(18, 0), &type_str[10..])
        } else if type_str.starts_with("Decimal(") {
            (DecimalParser::S(0), &type_str[8..])
        } else {
            return Err(errors::ConversionError::UnknownColumnType(type_str.to_owned()).into());
        };

        for c in rest.chars() {
            match state {
                DecimalParser::S(ref mut s) => {
                    if c == ',' {
                        state = DecimalParser::P(*s, 0);
                    } else if c.is_digit(10) {
                        *s = *s * 10 + c.to_digit(10).unwrap() as u8;
                    }
                    //ignore other characters
                }
                DecimalParser::P(ref s, ref mut p) => {
                    if c == ')' {
                        return field!(SqlType::Decimal(*s, *p));
                    } else if c.is_digit(10) {
                        *p = *p * 10 + c.to_digit(10).unwrap() as u8;
                    }
                }
            }
        }
        Err(errors::ConversionError::UnknownColumnType(type_str.to_owned()).into())
    }
}

pub enum DateTimeParser {
    S(u8),
    Tz(u8, u8),
    Fin(u8, chrono_tz::Tz),
}

impl DateTimeParser {
    pub(crate) fn parse_str(type_str: &str) -> Result<Field> {
        let rest = if type_str.eq("Date") {
            return field!(SqlType::Date);
        } else if type_str.eq("DateTime") {
            return field!(SqlType::DateTime);
        } else if type_str.starts_with("DateTime64(") {
            &type_str[11..]
        } else {
            return Err(errors::ConversionError::UnknownColumnType(type_str.to_owned()).into());
        };
        let mut state = DateTimeParser::S(0);
        for (i, c) in rest.char_indices() {
            match state {
                DateTimeParser::S(ref mut s) => {
                    if c == ',' {
                        state = DateTimeParser::Tz(*s, 0);
                    } else if c.is_digit(10) {
                        *s = *s * 10 + c.to_digit(10).unwrap() as u8;
                    } else if c == '\'' {
                        if *s != 0 {
                            break;
                        }
                        state = DateTimeParser::Tz(0, (i + 1) as u8);
                    } else if c == ')' {
                        if *s == 0 {
                            break;
                        }
                        return field!(SqlType::DateTime64(*s, Tz::UTC));
                    }
                }
                DateTimeParser::Tz(ref s, ref mut pos) => {
                    if c == '\'' {
                        if *pos == 0 {
                            *pos = (i + 1) as u8;
                        } else {
                            let tz = rest[(*pos as usize)..i].parse().unwrap_or(Tz::UTC);
                            state = DateTimeParser::Fin(*s, tz);
                        }
                    } else if !c.is_whitespace() && *pos == 0 {
                        break;
                    }
                }
                DateTimeParser::Fin(s, tz) => {
                    if c == ')' {
                        if s > 9 {
                            return Err(
                                DriverError::UnsupportedType(SqlType::DateTime64(s, tz)).into()
                            );
                        }
                        return field!(SqlType::DateTime64(s, tz));
                    }
                }
            }
        }
        Err(errors::ConversionError::UnknownColumnType(type_str.to_owned()).into())
    }
}

#[inline]
fn parse_type_flags(t: &str) -> (u8, u8, &str) {
    let mut flag: u8 = 0;
    let mut depth: u8 = 0;
    // TODO: redesign

    let mut t = if t.starts_with("LowCardinality(") && t.ends_with(')') {
        flag |= FIELD_LOWCARDINALITY;
        &t[15..t.len() - 1]
    } else {
        t
    };
    t = if t.starts_with("Nullable(") && t.ends_with(')') {
        flag |= FIELD_NULLABLE;
        &t[9..t.len() - 1]
    } else {
        t
    };
    while t.len() > 7 && t.starts_with("Array(") && t.ends_with(')') {
        flag |= FIELD_ARRAY;
        depth += 1;
        t = &t[6..t.len() - 1];
    }
    (flag, depth, t)
}

pub fn parse_type_field(t: &str) -> Result<Field> {
    let (flag, depth, t) = parse_type_flags(t);
    macro_rules! field {
        ($sql_type: expr) => {
            Ok(Field {
                sql_type: $sql_type,
                flag,
                depth,
                meta: None,
            })
        };
    }

    if t.eq("String") {
        return field!(SqlType::String);
    } else if t.eq("UInt64") {
        return field!(SqlType::UInt64);
    } else if t.eq("Int64") {
        return field!(SqlType::Int64);
    } else if t.eq("Int32") {
        return field!(SqlType::Int32);
    } else if t.eq("UInt32") {
        return field!(SqlType::UInt32);
    } else if t.eq("Int16") {
        return field!(SqlType::Int16);
    } else if t.eq("UInt16") {
        return field!(SqlType::UInt16);
    } else if t.eq("Int8") {
        return field!(SqlType::Int8);
    } else if t.eq("UInt8") {
        return field!(SqlType::UInt8);
    } else if t.starts_with("Float32") {
        return field!(SqlType::Float32);
    } else if t.starts_with("Float64") {
        return field!(SqlType::Float64);
    } else if t.eq("UUID") {
        return field!(SqlType::Uuid);
    } else if t.eq("IPv4") {
        return field!(SqlType::Ipv4);
    } else if t.eq("IPv6") {
        return field!(SqlType::Ipv6);
    } else if t.starts_with("Decimal") {
        let mut field = DecimalParser::parse_str(t)?;
        field.flag = flag;
        field.depth = depth;
        return Ok(field);
    } else if t.starts_with("Enum") {
        let mut field = EnumParser::parse_str(t)?;
        field.flag = flag;
        field.depth = depth;
        return Ok(field);
    } else if t.starts_with("Date") {
        let mut field = DateTimeParser::parse_str(t)?;
        field.flag = flag;
        field.depth = depth;
        return Ok(field);
    } else if t.starts_with("FixedString(") {
        if let Ok(sz) = u32::from_str(&t[12..t.len() - 1]) {
            return field!(SqlType::FixedString(sz));
        }
    }

    Err(ConversionError::UnknownColumnType(t.to_owned()).into())
}

#[cfg(test)]
mod test {
    use super::{parse_type_field, DateTimeParser, DecimalParser, EnumParser};
    use crate::types::SqlType;
    use crate::types::{FIELD_ARRAY, FIELD_LOWCARDINALITY, FIELD_NONE, FIELD_NULLABLE};
    use chrono_tz::Europe::Moscow;
    use chrono_tz::Tz;

    #[test]
    fn test_parse_basic() {
        let types = [
            "UInt8", "Int8", "UInt16", "Int16", "UInt32", "Int32", "UInt64", "Int64", "Float32",
            "Float64", "UUID", "IPv4", "IPv6", "String",
        ];

        for t in types.iter() {
            let t = parse_type_field(*t).unwrap();
            assert_eq!(FIELD_NONE, t.flag);
        }
    }

    #[test]
    fn test_parse_array() {
        let types = [
            "Array(UInt8)",
            "Array(Int8)",
            "Array(UInt16)",
            "Array(Int16)",
            "Array(UInt32)",
            "Array(Int32)",
            "Array(UInt64)",
            "Array(Int64)",
            "Array(Float32)",
            "Array(Float64)",
            "Array(UUID)",
            "Array(IPv4)",
            "Array(IPv6)",
            "Array(String)",
        ];

        for t in types.iter() {
            let t = parse_type_field(*t).unwrap();
            assert_eq!(FIELD_ARRAY, t.flag);
            assert_eq!(1, t.depth);
        }
        let t = parse_type_field("Array(Array(Decimal(9,3)))").unwrap();
        assert_eq!(FIELD_ARRAY, t.flag);
        assert_eq!(2, t.depth);
        assert_eq!(SqlType::Decimal(9, 3), t.sql_type);
    }

    #[test]
    fn test_parse_enum8() {
        let type_enum_8: &str =
            "Enum8('no' = 0, 'yes' = 1, 'dn' = -1, 'another' = 2, 'the\\'others'= -120 )";
        let field = EnumParser::parse_str(type_enum_8).unwrap();
        assert_eq!(SqlType::Enum8, field.sql_type);
        let index = field.meta.unwrap().index;

        assert_eq!(-120, index[0].0);
        assert_eq!(-1, index[1].0);
        assert_eq!(0, index[2].0);
        assert_eq!(1, index[3].0);
        assert_eq!(2, index[4].0);
        let escaped = unsafe { std::str::from_utf8_unchecked(index[0].1.as_ref()) };
        assert_eq!("the\\'others", escaped);
    }

    #[test]
    fn test_parse_decimal() {
        let type_decimal: &str = "Decimal32(2)";
        assert_eq!(
            SqlType::Decimal(9, 2),
            DecimalParser::parse_str(type_decimal).unwrap().sql_type
        );
        let type_decimal: &str = "Decimal64( 4 )";
        assert_eq!(
            SqlType::Decimal(18, 4),
            DecimalParser::parse_str(type_decimal).unwrap().sql_type
        );
        let type_decimal: &str = "Decimal(10, 2 )";
        assert_eq!(
            SqlType::Decimal(10, 2),
            DecimalParser::parse_str(type_decimal).unwrap().sql_type
        );
        let type_decimal: &str = "Decimal(20, 11)";
        assert_eq!(
            SqlType::Decimal(20, 11),
            DecimalParser::parse_str(type_decimal).unwrap().sql_type
        );
    }

    #[test]
    fn test_parse_datetime() {
        let type_decimal: &str = "Date";
        assert_eq!(
            SqlType::Date,
            DateTimeParser::parse_str(type_decimal).unwrap().sql_type
        );

        let type_decimal: &str = "DateTime";
        assert_eq!(
            SqlType::DateTime,
            DateTimeParser::parse_str(type_decimal).unwrap().sql_type
        );

        let type_decimal: &str = "DateTime64(4,'UTC')";
        assert_eq!(
            SqlType::DateTime64(4, Tz::UTC),
            DateTimeParser::parse_str(type_decimal).unwrap().sql_type
        );

        let type_decimal: &str = "DateTime64(5, 'Europe/Moscow' )";
        assert_eq!(
            SqlType::DateTime64(5, Moscow),
            DateTimeParser::parse_str(type_decimal).unwrap().sql_type
        );

        let type_decimal: &str = "DateTime64( 3 )";
        assert_eq!(
            SqlType::DateTime64(3, Tz::UTC),
            DateTimeParser::parse_str(type_decimal).unwrap().sql_type
        );

        let type_decimal: &str = "DateTime64( 'Europe/Moscow' )";
        assert_eq!(
            SqlType::DateTime64(0, Moscow),
            DateTimeParser::parse_str(type_decimal).unwrap().sql_type
        );

        let type_decimal: &str = "DateTime64()";
        assert!(DateTimeParser::parse_str(type_decimal).is_err());
    }

    #[test]
    fn test_parse_lowcardinality() {
        let type_decimal: &str = "LowCardinality(String)";
        let field = parse_type_field(type_decimal).unwrap();
        assert_eq!(SqlType::String, field.sql_type);
        assert_eq!(FIELD_LOWCARDINALITY, field.flag);
        let type_decimal: &str = "LowCardinality(Nullable(String))";
        let field = parse_type_field(type_decimal).unwrap();
        assert_eq!(SqlType::String, field.sql_type);
        assert_eq!(FIELD_LOWCARDINALITY | FIELD_NULLABLE, field.flag);
    }
}
