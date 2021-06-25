//! implement CH compatible builtin functions

use std::sync::Arc;

use arrow::{
    array::{
        Array, Date16Array, Timestamp32Array, UInt16Array, UInt16Builder,
        UInt8Array, UInt8Builder,
    },
    datatypes::DataType,
};
use base::datetimes::{
    days_to_year, days_to_ymd, unixtime_to_year, unixtime_to_ymd,
};

use crate::error::DataFusionError;

use super::ColumnarValue;

/// Extracts the years from Date16 array
pub fn date16_to_year(
    array: &Date16Array,
) -> arrow::error::Result<UInt16Array> {
    let mut b = UInt16Builder::new(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            b.append_null()?;
        } else {
            b.append_value(days_to_year(array.value(i) as i32))?;
        }
    }
    Ok(b.finish())
}

/// Extracts the months from Date16 array
pub fn date16_to_month(
    array: &Date16Array,
) -> arrow::error::Result<UInt8Array> {
    let mut b = UInt8Builder::new(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            b.append_null()?;
        } else {
            b.append_value(days_to_ymd(array.value(i) as i32).m)?;
        }
    }
    Ok(b.finish())
}

/// Extracts the days of month from Date16 array
pub fn date16_to_day_of_month(
    array: &Date16Array,
) -> arrow::error::Result<UInt8Array> {
    let mut b = UInt8Builder::new(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            b.append_null()?;
        } else {
            b.append_value(days_to_ymd(array.value(i) as i32).d)?;
        }
    }
    Ok(b.finish())
}

/// Extracts the years from Timestamp32 array
pub fn timestamp32_to_year(
    array: &Timestamp32Array,
) -> arrow::error::Result<UInt16Array> {
    let mut b = UInt16Builder::new(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            b.append_null()?;
        } else {
            b.append_value(unixtime_to_year(array.value(i) as i32))?;
        }
    }
    Ok(b.finish())
}

/// Extracts the months from Timestamp32 array
pub fn timestamp32_to_month(
    array: &Timestamp32Array,
) -> arrow::error::Result<UInt8Array> {
    let mut b = UInt8Builder::new(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            b.append_null()?;
        } else {
            b.append_value(unixtime_to_ymd(array.value(i) as i32).m)?;
        }
    }
    Ok(b.finish())
}

/// Extracts the days of month from Timestamp32 array
pub fn timestamp32_to_day_of_month(
    array: &Timestamp32Array,
) -> arrow::error::Result<UInt8Array> {
    let mut b = UInt8Builder::new(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            b.append_null()?;
        } else {
            b.append_value(unixtime_to_ymd(array.value(i) as i32).d)?;
        }
    }
    Ok(b.finish())
}

macro_rules! wrap_datetime_fn {
    ( $(
        $(#[$OUTER:meta])* $CH_FN:literal => fn $NAME:ident {
            $( $DATA_TYPE:pat => fn $OP:ident($INPUT_TY:ty) -> $OUTPUT_TY:ty, )*
        }
    )* ) => { $(
        $(#[$OUTER])*
        pub fn $NAME(args: &[ColumnarValue]) -> crate::error::Result<ColumnarValue> {
            match args[0].data_type() {
                $(
                $DATA_TYPE => if let ColumnarValue::Array(array) = &args[0] {
                    if let Some(a) = array.as_any().downcast_ref::<$INPUT_TY>() {
                        let res: $OUTPUT_TY = $OP(a)?;
                        Ok(ColumnarValue::Array(Arc::new(res)))
                    } else {
                        return Err(DataFusionError::Internal(
                            format!("{} Date16 error", stringify!($NAME)),
                        ));
                    }
                } else {
                    return Err(DataFusionError::Internal(
                        format!("{} Date16 error", stringify!($NAME)),
                    ));
                },
                )*
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {:?} for function {}",
                    other, $CH_FN,
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
