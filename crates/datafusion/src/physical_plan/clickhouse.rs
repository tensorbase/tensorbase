//! Implement ClickHouse compatible builtin functions
//!
//! Tests are located at `datafusion_tests`.

use super::{ColumnarValue, PhysicalExpr};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::functions::{ScalarFunctionImplementation, Signature};
use arrow::{
    array::{
        ArrayRef, BooleanArray, Date16Array, GenericStringArray, Int64Array,
        PrimitiveArray, StringOffsetSizeTrait, Timestamp32Array, UInt16Array, UInt8Array,
    },
    datatypes::{ArrowPrimitiveType, DataType, Schema},
};
use chrono::{Datelike, NaiveDate};
use fmt::{Debug, Formatter};
use std::{any::type_name, fmt, lazy::SyncOnceCell, str::FromStr, sync::Arc};

use base::datetimes::{
    days_to_ordinal, days_to_weekday, days_to_year, days_to_ymd, unixtime_to_days,
    unixtime_to_hms, unixtime_to_ordinal, unixtime_to_second, unixtime_to_weekday,
    unixtime_to_year, unixtime_to_ymd, BaseTimeZone,
};

/// The default timezone is specified at the server's startup stage.
pub static DEFAULT_TIMEZONE: SyncOnceCell<BaseTimeZone> = SyncOnceCell::new();

/// Enum of clickhouse built-in scalar functions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuiltinScalarFunction {
    /// toYear
    ToYear,
    /// toQuarter,
    ToQuarter,
    /// toMonth
    ToMonth,
    /// toDayOfYear
    ToDayOfYear,
    /// toDayOfMonth
    ToDayOfMonth,
    /// toDayOfWeek
    ToDayOfWeek,
    /// toDate
    ToDate,
    /// toHour,
    ToHour,
    /// toMinute,
    ToMinute,
    /// toSecond,
    ToSecond,
    /// endsWith,
    EndsWith,
}

impl fmt::Display for BuiltinScalarFunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

impl FromStr for BuiltinScalarFunction {
    type Err = DataFusionError;

    fn from_str(name: &str) -> Result<BuiltinScalarFunction> {
        Ok(match name {
            // date and time functions
            "toYear" | "toYYYY" => BuiltinScalarFunction::ToYear,
            "toQuarter" => BuiltinScalarFunction::ToQuarter,
            "toMonth" => BuiltinScalarFunction::ToMonth,
            "toDayOfYear" => BuiltinScalarFunction::ToDayOfYear,
            "toDayOfMonth" => BuiltinScalarFunction::ToDayOfMonth,
            "toDayOfWeek" => BuiltinScalarFunction::ToDayOfWeek,
            "toDate" => BuiltinScalarFunction::ToDate,
            "toHour" => BuiltinScalarFunction::ToHour,
            "toMinute" => BuiltinScalarFunction::ToMinute,
            "toSecond" => BuiltinScalarFunction::ToSecond,
            "endsWith" => BuiltinScalarFunction::EndsWith,

            _ => {
                return Err(DataFusionError::Plan(format!(
                    "There is no built-in clickhouse function named {}",
                    name
                )))
            }
        })
    }
}

macro_rules! downcast_array_args {
    ($ARG:expr, $TO:ty) => {{
        $ARG.as_any().downcast_ref::<$TO>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast {} to {}",
                $ARG.data_type(),
                type_name::<$TO>()
            ))
        })?
    }};
}

/// wrap function calls from primitive array to primitive array
macro_rules! wrap_datetime_fn {
    ( fn $OP:ident($INPUT_TY:ty $(, $TZ:ident)? ) -> $OUTPUT_TY:ty ) => {
        Arc::new(move |args: &[ColumnarValue]| {
            $( let $TZ = $TZ.clone(); )?
            match &args[0] {
                // tz in the outer $DATA_TYPE does not live long enough,
                // so we have to take it in the inner function.
                ColumnarValue::Array(array) => {
                    let a = downcast_array_args!(array, $INPUT_TY);
                    let res: $OUTPUT_TY = $OP(a $(, $TZ)? )?;
                    Ok(ColumnarValue::Array(Arc::new(res)))
                },
                ColumnarValue::Scalar(scalar) => {
                    let array = scalar.to_array();
                    let a = downcast_array_args!(array, $INPUT_TY);
                    let res: $OUTPUT_TY = $OP(a $(, $TZ)? )?;
                    Ok(ColumnarValue::Array(Arc::new(res)))
                },
            }
        })
    }
}

/// wrap the type error of function `$HAME`
macro_rules! wrap_type_err {
    ($OTHER:ident, $NAME:literal) => {
        return Err(DataFusionError::Internal(format!(
            "Unsupported data type {:?} for function {}",
            $OTHER?, $NAME,
        )))
    };
}

impl BuiltinScalarFunction {
    /// an allowlist of functions to take zero arguments, so that they will get special treatment
    /// while executing.
    pub fn supports_zero_argument(&self) -> bool {
        false
    }

    /// Returns the datatype of the scalar function
    pub fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        match self {
            BuiltinScalarFunction::ToYear => Ok(DataType::UInt16),
            BuiltinScalarFunction::ToMonth => Ok(DataType::UInt8),
            BuiltinScalarFunction::ToDayOfYear => Ok(DataType::UInt16),
            BuiltinScalarFunction::ToDayOfMonth => Ok(DataType::UInt8),
            BuiltinScalarFunction::ToDayOfWeek => Ok(DataType::UInt8),
            BuiltinScalarFunction::ToDate => Ok(DataType::Date16),
            BuiltinScalarFunction::ToQuarter => Ok(DataType::UInt8),
            BuiltinScalarFunction::ToHour => Ok(DataType::UInt8),
            BuiltinScalarFunction::ToMinute => Ok(DataType::UInt8),
            BuiltinScalarFunction::ToSecond => Ok(DataType::UInt8),
            BuiltinScalarFunction::EndsWith => Ok(DataType::Boolean),
        }
    }

    /// Returns the implementation of the scalar function
    pub fn func_impl(
        &self,
        args: &[Arc<dyn PhysicalExpr>],
        schema: &Schema,
    ) -> Result<ScalarFunctionImplementation> {
        let func: ScalarFunctionImplementation = match self {
            BuiltinScalarFunction::ToYear => match args[0].data_type(schema) {
                Ok(DataType::Date16) => {
                    wrap_datetime_fn!(fn date16_to_year(Date16Array) -> UInt16Array)
                }
                Ok(DataType::Timestamp32(tz)) => {
                    wrap_datetime_fn!(fn timestamp32_to_year(Timestamp32Array, tz) -> UInt16Array)
                }
                other => wrap_type_err!(other, "toYear"),
            },
            BuiltinScalarFunction::ToMonth => match args[0].data_type(schema) {
                Ok(DataType::Date16) => {
                    wrap_datetime_fn!(fn date16_to_month(Date16Array) -> UInt8Array)
                }
                Ok(DataType::Timestamp32(tz)) => {
                    wrap_datetime_fn!(fn timestamp32_to_month(Timestamp32Array, tz) -> UInt8Array)
                }
                other => wrap_type_err!(other, "toMonth"),
            },
            BuiltinScalarFunction::ToDayOfYear => match args[0].data_type(schema) {
                Ok(DataType::Date16) => {
                    wrap_datetime_fn!(fn date16_to_day_of_year(Date16Array) -> UInt16Array)
                }
                Ok(DataType::Timestamp32(tz)) => {
                    wrap_datetime_fn!(fn timestamp32_to_day_of_year(Timestamp32Array, tz) -> UInt16Array)
                }
                other => wrap_type_err!(other, "toDayOfYear"),
            },
            BuiltinScalarFunction::ToDayOfMonth => match args[0].data_type(schema) {
                Ok(DataType::Date16) => {
                    wrap_datetime_fn!(fn date16_to_day_of_month(Date16Array) -> UInt8Array)
                }
                Ok(DataType::Timestamp32(tz)) => {
                    wrap_datetime_fn!(fn timestamp32_to_day_of_month(Timestamp32Array, tz) -> UInt8Array)
                }
                other => wrap_type_err!(other, "toDayOfMonth"),
            },
            BuiltinScalarFunction::ToDayOfWeek => match args[0].data_type(schema) {
                Ok(DataType::Date16) => {
                    wrap_datetime_fn!(fn date16_to_day_of_week(Date16Array) -> UInt8Array)
                }
                Ok(DataType::Timestamp32(tz)) => {
                    wrap_datetime_fn!(fn timestamp32_to_day_of_week(Timestamp32Array, tz) -> UInt8Array)
                }
                other => wrap_type_err!(other, "toDayOfWeek"),
            },
            BuiltinScalarFunction::ToDate => match args[0].data_type(schema) {
                Ok(DataType::Utf8) | Ok(DataType::LargeUtf8) => {
                    Arc::new(expr_str_to_date)
                }
                Ok(DataType::Timestamp32(tz)) => {
                    wrap_datetime_fn!(fn timestamp32_to_date(Timestamp32Array, tz) -> Date16Array)
                }
                Ok(DataType::Int64) => {
                    wrap_datetime_fn!(fn int64_to_date(Int64Array) -> Date16Array)
                }
                other => wrap_type_err!(other, "toDate"),
            },
            BuiltinScalarFunction::ToQuarter => match args[0].data_type(schema) {
                Ok(DataType::Date16) => {
                    wrap_datetime_fn!(fn date16_to_quarter(Date16Array) -> UInt8Array)
                }
                Ok(DataType::Timestamp32(tz)) => {
                    wrap_datetime_fn!(fn timestamp32_to_quarter(Timestamp32Array, tz) -> UInt8Array)
                }
                other => wrap_type_err!(other, "toQuarter"),
            },
            BuiltinScalarFunction::ToHour => match args[0].data_type(schema) {
                Ok(DataType::Timestamp32(tz)) => {
                    wrap_datetime_fn!(fn timestamp32_to_hour(Timestamp32Array, tz) -> UInt8Array)
                }
                other => wrap_type_err!(other, "toHour"),
            },
            BuiltinScalarFunction::ToMinute => match args[0].data_type(schema) {
                Ok(DataType::Timestamp32(tz)) => {
                    wrap_datetime_fn!(fn timestamp32_to_minute(Timestamp32Array, tz) -> UInt8Array)
                }
                other => wrap_type_err!(other, "toMinute"),
            },
            BuiltinScalarFunction::ToSecond => match args[0].data_type(schema) {
                Ok(DataType::Timestamp32(tz)) => {
                    wrap_datetime_fn!(fn timestamp32_to_second(Timestamp32Array, tz) -> UInt8Array)
                }
                other => wrap_type_err!(other, "toSecond"),
            },
            BuiltinScalarFunction::EndsWith => Arc::new(expr_ends_with),
        };

        Ok(func)
    }

    /// Returns the signature of the scalar function
    pub fn signature(&self) -> Signature {
        match self {
            BuiltinScalarFunction::ToYear
            | BuiltinScalarFunction::ToMonth
            | BuiltinScalarFunction::ToDayOfYear
            | BuiltinScalarFunction::ToDayOfMonth
            | BuiltinScalarFunction::ToDayOfWeek
            | BuiltinScalarFunction::ToQuarter => {
                Signature::Uniform(1, vec![DataType::Date16, DataType::Timestamp32(None)])
            }
            BuiltinScalarFunction::ToDate => Signature::OneOf(vec![
                Signature::Uniform(1, vec![DataType::Date16, DataType::Int64]),
                Signature::Uniform(1, vec![DataType::Date16, DataType::LargeUtf8]),
                Signature::Uniform(
                    1,
                    vec![DataType::Date16, DataType::Timestamp32(None)],
                ),
            ]),
            BuiltinScalarFunction::ToHour
            | BuiltinScalarFunction::ToMinute
            | BuiltinScalarFunction::ToSecond => {
                Signature::Uniform(1, vec![DataType::Timestamp32(None)])
            }
            BuiltinScalarFunction::EndsWith => Signature::Any(2),
        }
    }
}

fn handle_date_fn<T, U, F>(array: &PrimitiveArray<T>, f: F) -> Result<PrimitiveArray<U>>
where
    T: ArrowPrimitiveType,
    U: ArrowPrimitiveType,
    F: Fn(Option<T::Native>) -> Option<U::Native>,
{
    Ok(array.iter().map(f).collect())
}

fn handle_timestamp_fn<T, U, F>(
    array: &PrimitiveArray<T>,
    tz: Option<BaseTimeZone>,
    f: F,
) -> Result<PrimitiveArray<U>>
where
    T: ArrowPrimitiveType,
    U: ArrowPrimitiveType,
    F: Fn(Option<T::Native>, &BaseTimeZone) -> Option<U::Native>,
{
    let tz = tz
        .as_ref()
        .or(DEFAULT_TIMEZONE.get())
        .ok_or(DataFusionError::Internal(
            "default time zone not initialized".to_string(),
        ))?;
    Ok(array.iter().map(|x| f(x, tz)).collect())
}

macro_rules! def_datetime_fn {
    ( $(
        $(#[$OUTER:meta])*
        fn $OP:ident($ARRAY:ident: $INPUT_TY:ty $(, $TZ:ident )? ) -> $OUTPUT_TY:ty {
            $EXPR:expr
        }
    )* ) => { $( def_datetime_fn!{
        @wrapped $(#[$OUTER])*
        fn $OP($ARRAY: $INPUT_TY $(, $TZ )? ) -> $OUTPUT_TY {
            $EXPR
        }
    } )* };
    // wrapped rule for define date functions
    (
        @wrapped $(#[$OUTER:meta])*
        fn $OP:ident($ARRAY:ident: $INPUT_TY:ty ) -> $OUTPUT_TY:ty {
            $EXPR:expr
        }
    ) => {
        $(#[$OUTER])* pub fn $OP($ARRAY: $INPUT_TY ) -> $OUTPUT_TY {
            handle_date_fn($ARRAY, $EXPR)
        }
    };
    // wrapped rule for define timestamp functions
    (
        @wrapped $(#[$OUTER:meta])*
        fn $OP:ident($ARRAY:ident: $INPUT_TY:ty, $TZ:ident ) -> $OUTPUT_TY:ty {
            $EXPR:expr
        }
    ) => {
        $(#[$OUTER])* pub fn $OP($ARRAY: $INPUT_TY, $TZ: Option<BaseTimeZone> ) -> $OUTPUT_TY {
            handle_timestamp_fn($ARRAY, $TZ, $EXPR)
        }
    };
}

def_datetime_fn! {
    /// Extracts the years from Date16 array
    fn date16_to_year(array: &Date16Array) -> Result<UInt16Array> {
        |x| Some(days_to_year(x? as i32))
    }
    /// Extracts the months from Date16 array
    fn date16_to_month(array: &Date16Array) -> Result<UInt8Array> {
        |x| Some(days_to_ymd(x? as i32).m)
    }
    /// Extracts the days of year from Date16 array
    fn date16_to_day_of_year(array: &Date16Array) -> Result<UInt16Array> {
        |x| Some(days_to_ordinal(x? as i32))
    }
    /// Extracts the days of month from Date16 array
    fn date16_to_day_of_month(array: &Date16Array) -> Result<UInt8Array> {
        |x| Some(days_to_ymd(x? as i32).d)
    }
    /// Extracts the days of week from Date16 array
    fn date16_to_day_of_week(array: &Date16Array) -> Result<UInt8Array> {
        |x| Some(days_to_weekday(x? as i32))
    }
    /// Extracts the months from Date16 array
    fn date16_to_quarter(array: &Date16Array) -> Result<UInt8Array> {
        |x| Some(month_to_quarter(days_to_ymd(x? as i32).m))
    }
    /// Extracts the years from Timestamp32 array
    fn timestamp32_to_year(array: &Timestamp32Array, tz) -> Result<UInt16Array> {
        |x, tz| Some(unixtime_to_year(x? as i32, tz.offset()))
    }
    /// Extracts the months from Timestamp32 array
    fn timestamp32_to_month(array: &Timestamp32Array, tz) -> Result<UInt8Array> {
        |x, tz| Some(unixtime_to_ymd(x? as i32, tz.offset()).m)
    }
    /// Extracts the days of year from Timestamp32 array
    fn timestamp32_to_day_of_year(array: &Timestamp32Array, tz) -> Result<UInt16Array> {
        |x, tz| Some(unixtime_to_ordinal(x? as i32, tz.offset()))
    }
    /// Extracts the days of month from Timestamp32 array
    fn timestamp32_to_day_of_month(array: &Timestamp32Array, tz) -> Result<UInt8Array> {
        |x, tz| Some(unixtime_to_ymd(x? as i32, tz.offset()).d)
    }
    /// Extracts the days of week from Timestamp32 array
    fn timestamp32_to_day_of_week(array: &Timestamp32Array, tz) -> Result<UInt8Array> {
        |x, tz| Some(unixtime_to_weekday(x? as i32, tz.offset()))
    }
    /// Extracts the months from Timestamp32 array
    fn timestamp32_to_quarter(array: &Timestamp32Array, tz) -> Result<UInt8Array> {
        |x, tz| Some(month_to_quarter(unixtime_to_ymd(x? as i32, tz.offset()).m))
    }
    /// Extracts the hours from Timestamp32 array
    fn timestamp32_to_hour(array: &Timestamp32Array, tz) -> Result<UInt8Array> {
        |x, tz| Some(unixtime_to_hms(x? as i32, tz.offset()).h)
    }
    /// Extracts the minutes from Timestamp32 array
    fn timestamp32_to_minute(array: &Timestamp32Array, tz) -> Result<UInt8Array> {
        |x, tz| Some(unixtime_to_hms(x? as i32, tz.offset()).m)
    }
    /// Extracts the seconds from Timestamp32 array
    fn timestamp32_to_second(array: &Timestamp32Array, _tz) -> Result<UInt8Array> {
        |x, _tz| Some(unixtime_to_second(x? as i32))
    }
    /// Extracts the date from Timestamp32Array
    fn timestamp32_to_date(array: &Timestamp32Array, tz) -> Result<Date16Array> {
        |x, tz| Some(unixtime_to_days(x? as i32, tz.offset()) as u16)
    }
    /// Extracts the date from Timestamp32Array
    fn int64_to_date(arr: &Int64Array) -> Result<Date16Array> {
        |x| x.map(|x| if x < 0 { 0 } else { x as u16 })
    }
}

fn month_to_quarter(month: u8) -> u8 {
    (month - 1) / 3 + 1
}

/// Returns true if string ends with suffix for utf-8.
pub fn utf8_ends_with(args: &[ArrayRef]) -> Result<BooleanArray> {
    ends_with::<i32>(args)
}

/// Returns true if string ends with suffix for large utf-8.
pub fn large_utf8_ends_with(args: &[ArrayRef]) -> Result<BooleanArray> {
    ends_with::<i64>(args)
}

/// Returns Date16Array if large utf string is formatted with '%Y-%m-%d' style.
pub fn large_utf8_to_date(args: &[ArrayRef]) -> Result<Date16Array> {
    string_to_date16::<i64>(args)
}

/// Returns Date16Array if utf8 string is formatted with '%Y-%m-%d' style.
pub fn utf8_to_date(args: &[ArrayRef]) -> Result<Date16Array> {
    string_to_date16::<i32>(args)
}

macro_rules! downcast_string_arg {
    ($ARG:expr, $NAME:expr, $T:ident) => {{
        $ARG.as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} to {}",
                    $NAME,
                    type_name::<GenericStringArray<T>>()
                ))
            })?
    }};
}

/// Returns true if string ends with prefix.
/// endsWith('alphabet', 'alph') = 't'
fn ends_with<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<BooleanArray> {
    if args[0].is_null(0) || args[1].is_null(1) {
        return Ok(BooleanArray::from(vec![None]));
    }

    let string_array = downcast_string_arg!(args[0], "string", T);
    let suffix_array = downcast_string_arg!(args[1], "suffix", T);
    let suffix = suffix_array.value(0);

    let result = string_array
        .iter()
        .map(|string| string.map(|string: &str| string.ends_with(suffix)))
        .collect::<BooleanArray>();
    Ok(result)
}

/// Return Date16Array if the string is formatted with '%Y-%m-%d' date style.
fn string_to_date16<T: StringOffsetSizeTrait>(args: &[ArrayRef]) -> Result<Date16Array> {
    if args[0].is_null(0) {
        return Ok(Date16Array::from(vec![None]));
    }

    let string_array = downcast_string_arg!(args[0], "string", T);
    let string_array: Result<Vec<Option<u16>>> = string_array
        .iter()
        .map(|string| string.map(|s: &str| str_to_u16(s)).transpose())
        .collect();
    Ok(string_array?.into())
}

macro_rules! wrap_string_fn {
    ( $(
        $(#[$OUTER:meta])* $NAME:literal => fn $FUNC:ident {
            $( $DATA_TYPE:pat => fn $OP:ident -> $OUTPUT_TY:ty, )*
        }
    )* ) => { $(
        $(#[$OUTER])*
        pub fn $FUNC(args: &[ColumnarValue]) -> $crate::error::Result<ColumnarValue> {
            match args[0].data_type() {
                $(
                _data_type @ $DATA_TYPE => {
                    let len = args
                        .iter()
                        .fold(Option::<usize>::None, |acc, arg| match arg {
                            ColumnarValue::Scalar(_) => acc,
                            ColumnarValue::Array(a) => Some(a.len()),
                        });

                    // to array
                    let args = if let Some(len) = len {
                        args.iter()
                            .map(|arg| arg.clone().into_array(len))
                            .collect::<Vec<ArrayRef>>()
                    } else {
                        args.iter()
                            .map(|arg| arg.clone().into_array(1))
                            .collect::<Vec<ArrayRef>>()
                    };

                    let res = $OP(&args)?;
                    Ok(ColumnarValue::Array(Arc::new(res)))
                },)*
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {:?} for function {}",
                    other, $NAME,
                ))),
            }
        }
    )* }
}

#[inline]
fn str_to_u16(s: &str) -> Result<u16> {
    if s.len() < 8 {
        return Err(DataFusionError::Execution(format!("'{}' too short", s)));
    }

    let s = match s.chars().nth(0).unwrap_or('\u{0}') {
        '1'..='9' => s,
        _ => &s[1..], // skip the length byte
    };

    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        if date.year() > 2148 {
            return Err(DataFusionError::Execution(format!(
                "Date '{}' Error: Year must be lowwer than 2149.",
                s
            )));
        }

        let secs = date.and_hms(0, 0, 0).timestamp();

        let days = (secs / 86_400) as u16;
        return Ok(days);
    }

    Err(DataFusionError::Execution(format!(
        "Error parsing '{}' as date with '%Y-%m-%d' format",
        s
    )))
}

wrap_string_fn! {
    /// wrapping to backend endsWith logics
    "endsWith" => fn expr_ends_with {
        DataType::Utf8 => fn utf8_ends_with -> BooleanArray,
        DataType::LargeUtf8 => fn large_utf8_ends_with -> BooleanArray,
    }
    /// wrapping to backend toDate logics
    "toDate" => fn expr_str_to_date {
        DataType::Utf8 => fn utf8_to_date -> Date16Array,
        DataType::LargeUtf8 => fn large_utf8_to_date -> Date16Array,
    }
}
