//! Implement ClickHouse compatible builtin functions
//!
//! Tests are located at `datafusion_tests`.

use super::{ColumnarValue, PhysicalExpr};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::functions::Signature;
use crate::scalar::ScalarValue;
use arrow::{
    array::{
        Date16Array, Int64Array, LargeStringArray, Timestamp32Array, UInt16Array,
        UInt8Array, UInt16Array, BooleanArray, ArrayRef, GenericStringArray,
	StringOffsetSizeTrait, Array,
    },
    datatypes::DataType,
};
use chrono::prelude::*;
use fmt::{Debug, Formatter};
use std::{fmt, str::FromStr, sync::Arc};
use std::any::type_name;
use log::debug;

use base::datetimes::{
    days_to_ordinal, days_to_weekday, days_to_year, days_to_ymd, unixtime_to_days,
    unixtime_to_hms, unixtime_to_ordinal, unixtime_to_second, unixtime_to_weekday,
    unixtime_to_year, unixtime_to_ymd,
};

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
        _args: &[Arc<dyn PhysicalExpr>],
    ) -> fn(&[ColumnarValue]) -> Result<ColumnarValue> {
        match self {
            BuiltinScalarFunction::ToYear => expr_to_year,
            BuiltinScalarFunction::ToMonth => expr_to_month,
            BuiltinScalarFunction::ToDayOfYear => expr_to_day_of_year,
            BuiltinScalarFunction::ToDayOfMonth => expr_to_day_of_month,
            BuiltinScalarFunction::ToDayOfWeek => expr_to_day_of_week,
            BuiltinScalarFunction::ToDate => expr_to_date,
            BuiltinScalarFunction::ToQuarter => expr_to_quarter,
            BuiltinScalarFunction::ToHour => expr_to_hour,
            BuiltinScalarFunction::ToMinute => expr_to_minute,
            BuiltinScalarFunction::ToSecond => expr_to_second,
            BuiltinScalarFunction::EndsWith => expr_ends_with,
        }
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
                Signature::Uniform(
                    1,
                    vec![DataType::Date16, DataType::Timestamp32(None)],
                ),
                Signature::Uniform(1, vec![DataType::Date16, DataType::LargeUtf8]),
                Signature::Uniform(1, vec![DataType::Date16, DataType::Int64]),
            ]),
            BuiltinScalarFunction::ToHour
            | BuiltinScalarFunction::ToMinute
	    | BuiltinScalarFunction::ToSecond => Signature::Uniform(1, vec![DataType::Timestamp32(None)]),
            BuiltinScalarFunction::EndsWith => Signature::Any(2),
        }
    }
}

/// Extracts the years from Date16 array
#[inline]
fn date16_to_year(x: &u16) -> Result<Option<u16>> {
    Ok(Some(days_to_year(*x as i32)))
}

/// Extracts the months from Date16 array
#[inline]
fn date16_to_month(x: &u16) -> Result<Option<u8>> {
    Ok(Some(days_to_ymd(*x as i32).m))
}

/// Extracts the days of year from Date16 array
#[inline]
fn date16_to_day_of_year(x: &u16) -> Result<Option<u16>> {
    Ok(Some(days_to_ordinal(*x as i32)))
}

/// Extracts the days of month from Date16 array
#[inline]
fn date16_to_day_of_month(x: &u16) -> Result<Option<u8>> {
    Ok(Some(days_to_ymd(*x as i32).d))
}

/// Extracts the days of week from Date16 array
#[inline]
fn date16_to_day_of_week(x: &u16) -> Result<Option<u8>> {
    Ok(Some(days_to_weekday(*x as i32)))
}

#[inline]
fn month_to_quarter(month: u8) -> u8 {
    (month - 1) / 3 + 1
}

/// Extracts the months from i32
#[inline]
pub fn date16_to_quarter(x: &u16) -> Result<Option<u8>> {
    Ok(Some(month_to_quarter(days_to_ymd(*x as i32).m)))
}

/// Extracts the years from i32
#[inline]
fn timestamp32_to_year(x: &i32) -> Result<Option<u16>> {
    Ok(Some(unixtime_to_year(*x)))
}

/// Extracts the months from i32
#[inline]
fn timestamp32_to_month(x: &i32) -> Result<Option<u8>> {
    Ok(Some(unixtime_to_ymd(*x).m))
}

/// Extracts the days of year from i32
#[inline]
fn timestamp32_to_day_of_year(x: &i32) -> Result<Option<u16>> {
    Ok(Some(unixtime_to_ordinal(*x)))
}

/// Extracts the days of month from i32
#[inline]
pub fn timestamp32_to_day_of_month(x: &i32) -> Result<Option<u8>> {
    Ok(Some(unixtime_to_ymd(*x).d))
}

/// Extracts the days of week from i32
fn timestamp32_to_day_of_week(x: &i32) -> Result<Option<u8>> {
    Ok(Some(unixtime_to_weekday(*x)))
}

/// Extracts the months from Timestamp32 array
#[inline]
fn timestamp32_to_quarter(x: &i32) -> Result<Option<u8>> {
    Ok(Some(month_to_quarter(unixtime_to_ymd(*x).m)))
}

/// Extracts the hours from i32
#[inline]
pub fn timestamp32_to_hour(x: &i32) -> Result<Option<u8>> {
    Ok(Some(unixtime_to_hms(*x).h))
}

/// Extracts the minutes from i32
#[inline]
fn timestamp32_to_minute(x: &i32) -> Result<Option<u8>> {
    Ok(Some(unixtime_to_hms(*x).m))
}

/// Extracts the seconds from i32
#[inline]
fn timestamp32_to_second(x: &i32) -> Result<Option<u8>> {
    Ok(Some(unixtime_to_second(*x)))
}

/// Extracts the date from i32
#[inline]
fn timestamp32_to_date(x: &i32) -> Result<Option<u16>> {
    Ok(Some(unixtime_to_days(*x) as u16))
}

/// Extracts the date from string
#[inline]
fn large_string_to_date(s: &str) -> Result<Option<u16>> {
    Ok(Some(string_to_date16(s)?))
}

/// Extracts the date from int64
#[inline]
fn int64_to_date(x: &i64) -> Result<Option<u16>> {
    Ok(Some(unixtime_to_days(*x as i32) as u16))
}

macro_rules! wrap_datetime_fn {
    ( $(
        $(#[$OUTER:meta])* $NAME:literal => fn $FUNC:ident {
            $( ($DATA_TYPE:pat, ScalarValue::$SCALAR:ident) => fn $OP:ident($INPUT_TY:ty) -> $OUTPUT_TY:ty, )*
        }
    )* ) => { $(
        $(#[$OUTER])*
        pub fn $FUNC(args: &[ColumnarValue]) -> $crate::error::Result<ColumnarValue> {
            match &args[0] {
		ColumnarValue::Scalar(scalar) => {
		    match scalar {
			$(
			    ScalarValue::$SCALAR(a) => {
				let res: $OUTPUT_TY = a.iter().map(|x| $OP(&x).unwrap_or(None)).collect();
				Ok(ColumnarValue::Array(Arc::new(res)))
			    },
			)*
			_ => Err(DataFusionError::Internal(format!(
			    "Unsupported data type {:?} for function {}",
			    scalar.get_datatype(), $NAME,
			)))
		    }
		},
		ColumnarValue::Array(array) => {
		    match array.data_type() {
			$(
			    $DATA_TYPE => if let Some(a) = array.as_any().downcast_ref::<$INPUT_TY>() {
				let res: $OUTPUT_TY = a.iter().map(|x| $OP(&x.unwrap_or_default()).unwrap_or(None)).collect();
				Ok(ColumnarValue::Array(Arc::new(res)))
			    } else {
				Err(DataFusionError::Internal(format!(
				    "failed to downcast to {:?}",
				    array.data_type(),
				)))
			    }
			)*
			_ => Err(DataFusionError::Internal(format!(
			    "Unsupported data type {:?} for function {}",
			    array.data_type(), $NAME,
			)))
		    }
		}
	    }
	})*
    }
}

#[inline]
fn string_to_date16(s: &str) -> Result<u16> {
    if s.len() < 8 {
        return Err(DataFusionError::Execution(format!("'{}' too short", s)));
    }
    let s = match s.chars().nth(0).unwrap_or('\u{0}') {
        '1'..='9' => s,
        _ => &s[1..], // remove the length size byte
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

wrap_datetime_fn! {
    /// wrapping to backend to_date logics
    "toDate" => fn expr_to_date {
        (DataType::Timestamp32(_), ScalarValue::Timestamp32) => fn timestamp32_to_date(Timestamp32Array) -> Date16Array,
	(DataType::LargeUtf8, ScalarValue::LargeUtf8) => fn large_string_to_date(LargeStringArray) -> Date16Array,
	(DataType::Int64, ScalarValue::Int64) => fn int64_to_date(Int64Array) -> Date16Array,
    }
    /// wrapping to backend to_year logics
    "toYear" => fn expr_to_year {
        (DataType::Date16, ScalarValue::Date16) => fn date16_to_year(Date16Array) -> UInt16Array,
        (DataType::Timestamp32(_), ScalarValue::Timestamp32) => fn timestamp32_to_year(Timestamp32Array) -> UInt16Array,
    }
    /// wrapping to backend to_quarter logics
    "toQuarter" => fn expr_to_quarter {
        (DataType::Date16, ScalarValue::Date16) => fn date16_to_quarter(Date16Array) -> UInt8Array,
        (DataType::Timestamp32(_), ScalarValue::Timestamp32) => fn timestamp32_to_quarter(Timestamp32Array) -> UInt8Array,
    }
    /// wrapping to backend to_month logics
    "toMonth" => fn expr_to_month {
        (DataType::Date16, ScalarValue::Date16) => fn date16_to_month(Date16Array) -> UInt8Array,
        (DataType::Timestamp32(_), ScalarValue::Timestamp32) => fn timestamp32_to_month(Timestamp32Array) -> UInt8Array,
    }
    /// wrapping to backend to_day_of_year logics
    "toDayOfYear" => fn expr_to_day_of_year {
        (DataType::Date16, ScalarValue::Date16) => fn date16_to_day_of_year(Date16Array) -> UInt16Array,
        (DataType::Timestamp32(_), ScalarValue::Timestamp32) => fn timestamp32_to_day_of_year(Timestamp32Array) -> UInt16Array,
    }
    /// wrapping to backend to_day_of_month logics
    "toDayOfMonth" => fn expr_to_day_of_month {
        (DataType::Date16, ScalarValue::Date16) => fn date16_to_day_of_month(Date16Array) -> UInt8Array,
        (DataType::Timestamp32(_), ScalarValue::Timestamp32) => fn timestamp32_to_day_of_month(Timestamp32Array) -> UInt8Array,
    }
    /// wrapping to backend to_day_of_week logics
    "toDayOfWeek" => fn expr_to_day_of_week {
        (DataType::Date16, ScalarValue::Date16) => fn date16_to_day_of_week(Date16Array) -> UInt8Array,
        (DataType::Timestamp32(_), ScalarValue::Timestamp32) => fn timestamp32_to_day_of_week(Timestamp32Array) -> UInt8Array,
    }
    /// wrapping to backend to_hour logics
    "toHour" => fn expr_to_hour {
        (DataType::Timestamp32(_), ScalarValue::Timestamp32) => fn timestamp32_to_hour(Timestamp32Array) -> UInt8Array,
    }
    /// wrapping to backend to_minute logics
    "toMinute" => fn expr_to_minute {
        (DataType::Timestamp32(_), ScalarValue::Timestamp32) => fn timestamp32_to_minute(Timestamp32Array) -> UInt8Array,
    }
    /// wrapping to backend to_second logics
    "toSecond" => fn expr_to_second {
        (DataType::Timestamp32(_), ScalarValue::Timestamp32) => fn timestamp32_to_second(Timestamp32Array) -> UInt8Array,
    }
}


/// Returns true if string ends with suffix for utf-8.
pub fn utf8_ends_with(args: &[ArrayRef]) -> Result<BooleanArray> {
    ends_with::<i32>(args)
}

/// Returns true if string ends with suffix for large utf-8.
pub fn large_utf8_ends_with(args: &[ArrayRef]) -> Result<BooleanArray> {
    ends_with::<i64>(args)
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

wrap_string_fn! {
    /// wrapping to backend endsWith logics
    "endsWith" => fn expr_ends_with {
        DataType::Utf8 => fn utf8_ends_with -> BooleanArray,
        DataType::LargeUtf8 => fn large_utf8_ends_with -> BooleanArray,
    }
}
