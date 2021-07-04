//! Implement ClickHouse compatible builtin functions
//!
//! Tests are located at `datafusion_tests`.

use super::{ColumnarValue, PhysicalExpr};
use crate::error::{DataFusionError, Result};
use crate::physical_plan::datetime_expressions;
use crate::physical_plan::functions::Signature;
use arrow::{
    array::{
        ArrayRef, BooleanArray, Date16Array, GenericStringArray, PrimitiveArray,
        StringOffsetSizeTrait, Timestamp32Array, UInt16Array, UInt8Array,
    },
    datatypes::{ArrowPrimitiveType, DataType},
};
use fmt::{Debug, Formatter};
use std::{any::type_name, fmt, lazy::SyncOnceCell, str::FromStr, sync::Arc};

use base::datetimes::{
    days_to_ordinal, days_to_weekday, days_to_year, days_to_ymd, unixtime_to_hms,
    unixtime_to_ordinal, unixtime_to_second, unixtime_to_weekday, unixtime_to_year,
    unixtime_to_ymd, BaseTimeZone,
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
            BuiltinScalarFunction::ToDate => datetime_expressions::to_date,
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
            BuiltinScalarFunction::ToDate => Signature::Uniform(1, vec![DataType::Utf8]),
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
    tz: &Option<BaseTimeZone>,
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
        $(#[$OUTER])* pub fn $OP($ARRAY: $INPUT_TY, $TZ: &Option<BaseTimeZone> ) -> $OUTPUT_TY {
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
        |x, tz| Some(unixtime_to_year(x? as i32, tz))
    }
    /// Extracts the months from Timestamp32 array
    fn timestamp32_to_month(array: &Timestamp32Array, tz) -> Result<UInt8Array> {
        |x, tz| Some(unixtime_to_ymd(x? as i32, tz).m)
    }
    /// Extracts the days of year from Timestamp32 array
    fn timestamp32_to_day_of_year(array: &Timestamp32Array, tz) -> Result<UInt16Array> {
        |x, tz| Some(unixtime_to_ordinal(x? as i32, tz))
    }
    /// Extracts the days of month from Timestamp32 array
    fn timestamp32_to_day_of_month(array: &Timestamp32Array, tz) -> Result<UInt8Array> {
        |x, tz| Some(unixtime_to_ymd(x? as i32, tz).d)
    }
    /// Extracts the days of week from Timestamp32 array
    fn timestamp32_to_day_of_week(array: &Timestamp32Array, tz) -> Result<UInt8Array> {
        |x, tz| Some(unixtime_to_weekday(x? as i32, tz))
    }
    /// Extracts the months from Timestamp32 array
    fn timestamp32_to_quarter(array: &Timestamp32Array, tz) -> Result<UInt8Array> {
        |x, tz| Some(month_to_quarter(unixtime_to_ymd(x? as i32, tz).m))
    }
    /// Extracts the hours from Timestamp32 array
    fn timestamp32_to_hour(array: &Timestamp32Array, tz) -> Result<UInt8Array> {
        |x, tz| Some(unixtime_to_hms(x? as i32, tz).h)
    }
    /// Extracts the minutes from Timestamp32 array
    fn timestamp32_to_minute(array: &Timestamp32Array, tz) -> Result<UInt8Array> {
        |x, tz| Some(unixtime_to_hms(x? as i32, tz).m)
    }
    /// Extracts the seconds from Timestamp32 array
    fn timestamp32_to_second(array: &Timestamp32Array, _tz) -> Result<UInt8Array> {
        |x, _tz| Some(unixtime_to_second(x? as i32))
    }
}

fn month_to_quarter(month: u8) -> u8 {
    (month - 1) / 3 + 1
}

macro_rules! wrap_datetime_fn {
    ( $(
        $(#[$OUTER:meta])* $NAME:literal => fn $FUNC:ident {
            $( $DATA_TYPE:pat => fn $OP:ident($INPUT_TY:ty $(, $TZ:ident)? ) -> $OUTPUT_TY:ty, )*
        }
    )* ) => { $(
        $(#[$OUTER])*
        pub fn $FUNC(args: &[ColumnarValue]) -> $crate::error::Result<ColumnarValue> {
            match args[0].data_type() {
                $(
                $DATA_TYPE => if let ColumnarValue::Array(array) = &args[0] {
                    if let Some(a) = array.as_any().downcast_ref::<$INPUT_TY>() {
                        let res: $OUTPUT_TY = $OP(a $(, &$TZ)? )?;
                        Ok(ColumnarValue::Array(Arc::new(res)))
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "failed to downcast to {:?}",
                            args[0].data_type(),
                        )));
                    }
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "failed to downcast to {:?}",
                        args[0].data_type(),
                    )));
                },
                )*
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {:?} for function {}",
                    other, $NAME,
                ))),
            }
        }
    )* }
}

wrap_datetime_fn! {
    /// wrapping to backend to_year logics
    "toYear" => fn expr_to_year {
        DataType::Date16 => fn date16_to_year(Date16Array) -> UInt16Array,
        DataType::Timestamp32(tz) => fn timestamp32_to_year(Timestamp32Array, tz) -> UInt16Array,
    }
    /// wrapping to backend to_quarter logics
    "toQuarter" => fn expr_to_quarter {
        DataType::Date16 => fn date16_to_quarter(Date16Array) -> UInt8Array,
        DataType::Timestamp32(tz) => fn timestamp32_to_quarter(Timestamp32Array, tz) -> UInt8Array,
    }
    /// wrapping to backend to_month logics
    "toMonth" => fn expr_to_month {
        DataType::Date16 => fn date16_to_month(Date16Array) -> UInt8Array,
        DataType::Timestamp32(tz) => fn timestamp32_to_month(Timestamp32Array, tz) -> UInt8Array,
    }
    /// wrapping to backend to_day_of_year logics
    "toDayOfYear" => fn expr_to_day_of_year {
        DataType::Date16 => fn date16_to_day_of_year(Date16Array) -> UInt16Array,
        DataType::Timestamp32(tz) =>
            fn timestamp32_to_day_of_year(Timestamp32Array, tz) -> UInt16Array,
    }
    /// wrapping to backend to_day_of_month logics
    "toDayOfMonth" => fn expr_to_day_of_month {
        DataType::Date16 => fn date16_to_day_of_month(Date16Array) -> UInt8Array,
        DataType::Timestamp32(tz) =>
            fn timestamp32_to_day_of_month(Timestamp32Array, tz) -> UInt8Array,
    }
    /// wrapping to backend to_day_of_week logics
    "toDayOfWeek" => fn expr_to_day_of_week {
        DataType::Date16 => fn date16_to_day_of_week(Date16Array) -> UInt8Array,
        DataType::Timestamp32(tz) =>
            fn timestamp32_to_day_of_week(Timestamp32Array, tz) -> UInt8Array,
    }
    /// wrapping to backend to_hour logics
    "toHour" => fn expr_to_hour {
        DataType::Timestamp32(tz) => fn timestamp32_to_hour(Timestamp32Array, tz) -> UInt8Array,
    }
    /// wrapping to backend to_minute logics
    "toMinute" => fn expr_to_minute {
        DataType::Timestamp32(tz) => fn timestamp32_to_minute(Timestamp32Array, tz) -> UInt8Array,
    }
    /// wrapping to backend to_second logics
    "toSecond" => fn expr_to_second {
        DataType::Timestamp32(tz) => fn timestamp32_to_second(Timestamp32Array, tz) -> UInt8Array,
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
