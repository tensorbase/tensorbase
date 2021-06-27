//! Implement ClickHouse compatible builtin functions
//!
//! Tests are located at `crates/datafusion_tests`.

use super::{
    ColumnarValue, PhysicalExpr,
};
use crate::physical_plan::datetime_expressions;
use crate::physical_plan::functions::Signature;
use crate::{
    error::{DataFusionError, Result},
};
use arrow::{
    array::{
        Date16Array, Timestamp32Array, UInt16Array, UInt8Array,
    },
    datatypes::DataType,
};
use fmt::{Debug, Formatter};
use std::{fmt, str::FromStr, sync::Arc};

use base::datetimes::{
    days_to_year, days_to_ymd, unixtime_to_year, unixtime_to_ymd,
};

/// Enum of clickhouse built-in scalar functions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuiltinScalarFunction {
    /// toYear
    ToYear,
    /// toMonth
    ToMonth,
    /// toDayOfMonth
    ToDayOfMonth,
    /// toDate
    ToDate,
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
            "toMonth" => BuiltinScalarFunction::ToMonth,
            "toDayOfMonth" => BuiltinScalarFunction::ToDayOfMonth,
            "toDate" => BuiltinScalarFunction::ToDate,

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
            BuiltinScalarFunction::ToDayOfMonth => Ok(DataType::UInt8),
            BuiltinScalarFunction::ToDate => Ok(DataType::Date16),
        }
    }

    /// Returns the implementation of the scalar function
    pub fn func_impl(
        &self,
        _args: &[Arc<dyn PhysicalExpr>]
    ) -> fn(&[ColumnarValue]) -> Result<ColumnarValue> {
        match self {
            BuiltinScalarFunction::ToYear => expr_to_year,
            BuiltinScalarFunction::ToMonth => expr_to_month,
            BuiltinScalarFunction::ToDayOfMonth => expr_to_day_of_month,
            BuiltinScalarFunction::ToDate => datetime_expressions::to_date,
        }
    }

    /// Returns the signature of the scalar function
    pub fn signature(&self) -> Signature {
        match self {
            BuiltinScalarFunction::ToYear
            | BuiltinScalarFunction::ToMonth
            | BuiltinScalarFunction::ToDayOfMonth => {
                Signature::Uniform(1, vec![DataType::Date16, DataType::Timestamp32(None)])
            }
            BuiltinScalarFunction::ToDate => Signature::Uniform(1, vec![DataType::Utf8]),
        }
    }
}

/// Extracts the years from Date16 array
pub fn date16_to_year(array: &Date16Array) -> Result<UInt16Array> {
    Ok(array.iter().map(|x| Some(days_to_year(x? as i32))).collect())
}

/// Extracts the months from Date16 array
pub fn date16_to_month(array: &Date16Array) -> Result<UInt8Array> {
    Ok(array.iter().map(|x| Some(days_to_ymd(x? as i32).m)).collect())
}

/// Extracts the days of month from Date16 array
pub fn date16_to_day_of_month(array: &Date16Array) -> Result<UInt8Array> {
    Ok(array.iter().map(|x| Some(days_to_ymd(x? as i32).d)).collect())
}

/// Extracts the years from Timestamp32 array
pub fn timestamp32_to_year(array: &Timestamp32Array) -> Result<UInt16Array> {
    Ok(array.iter().map(|x| Some(unixtime_to_year(x? as i32))).collect())
}

/// Extracts the months from Timestamp32 array
pub fn timestamp32_to_month(array: &Timestamp32Array) -> Result<UInt8Array> {
    Ok(array.iter().map(|x| Some(unixtime_to_ymd(x? as i32).m)).collect())
}

/// Extracts the days of month from Timestamp32 array
pub fn timestamp32_to_day_of_month(array: &Timestamp32Array) -> Result<UInt8Array> {
    Ok(array.iter().map(|x| Some(unixtime_to_ymd(x? as i32).d)).collect())
}

macro_rules! wrap_datetime_fn {
    ( $(
        $(#[$OUTER:meta])* $NAME:literal => fn $FUNC:ident {
            $( $DATA_TYPE:pat => fn $OP:ident($INPUT_TY:ty) -> $OUTPUT_TY:ty, )*
        }
    )* ) => { $(
        $(#[$OUTER])*
        pub fn $FUNC(args: &[ColumnarValue]) -> crate::error::Result<ColumnarValue> {
            match args[0].data_type() {
                $(
                data_type @ $DATA_TYPE => if let ColumnarValue::Array(array) = &args[0] {
                    if let Some(a) = array.as_any().downcast_ref::<$INPUT_TY>() {
                        let res: $OUTPUT_TY = $OP(a)?;
                        Ok(ColumnarValue::Array(Arc::new(res)))
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "failed to downcast to {:?}",
                            data_type,
                        )));
                    }
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "failed to downcast to {:?}",
                        data_type,
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
        DataType::Timestamp32(_) => fn timestamp32_to_year(Timestamp32Array) -> UInt16Array,
    }
    /// wrapping to backend to_month logics
    "toMonth" => fn expr_to_month {
        DataType::Date16 => fn date16_to_month(Date16Array) -> UInt8Array,
        DataType::Timestamp32(_) => fn timestamp32_to_month(Timestamp32Array) -> UInt8Array,
    }
    /// wrapping to backend to_day_of_month logics
    "toDayOfMonth" => fn expr_to_day_of_month {
        DataType::Date16 => fn date16_to_day_of_month(Date16Array) -> UInt8Array,
        DataType::Timestamp32(_) => fn timestamp32_to_day_of_month(Timestamp32Array) -> UInt8Array,
    }
}
