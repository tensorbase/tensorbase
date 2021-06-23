use std::sync::Arc;

use arrow::{
    array::{Array, Date16Array, Timestamp32Array, UInt16Array, UInt16Builder},
    datatypes::DataType,
};
use base::datetimes::{days_to_year, unixtime_to_year};

use crate::error::DataFusionError;

use super::ColumnarValue;

/// Extracts the years from Date16 array
pub fn date16_to_year(
    array: &Date16Array,
) -> arrow::error::Result<UInt16Array>
where
{
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

/// Extracts the years from Timestamp32 array
pub fn timestamp32_to_year(
    array: &Timestamp32Array,
) -> arrow::error::Result<UInt16Array>
where
{
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

/// wrapping to backend to_year logics
pub fn expr_to_year(
    args: &[ColumnarValue],
) -> crate::error::Result<ColumnarValue> {
    match args[0].data_type() {
        DataType::Date16 => match &args[0] {
            ColumnarValue::Array(array) => {
                let ra = array.as_any().downcast_ref::<Date16Array>();
                match ra {
                    Some(a) => {
                        let res: UInt16Array = date16_to_year(a)?;
                        Ok(ColumnarValue::Array(Arc::new(res)))
                    }
                    _ => {
                        return Err(DataFusionError::Internal(
                            "expr_to_year Date16 error".to_string(),
                        ));
                    }
                }
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "expr_to_year Date16 error".to_string(),
                ));
            }
        },
        DataType::Timestamp32(_) => match &args[0] {
            ColumnarValue::Array(array) => {
                let ra = array.as_any().downcast_ref::<Timestamp32Array>();
                match ra {
                    Some(a) => {
                        let res: UInt16Array = timestamp32_to_year(a)?;
                        Ok(ColumnarValue::Array(Arc::new(res)))
                    }
                    _ => {
                        return Err(DataFusionError::Internal(
                            "expr_to_year Timestamp32 error".to_string(),
                        ));
                    }
                }
            }
            _ => {
                return Err(DataFusionError::Internal(
                    "expr_to_year Timestamp32 error".to_string(),
                ));
            }
        },
        other => Err(DataFusionError::Internal(format!(
            "Unsupported data type {:?} for function toYear",
            other,
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::{array::PrimitiveArray, datatypes::Date16Type};

    #[test]
    fn test_to_year() {
        let a: PrimitiveArray<Date16Type> =
            vec![Some(1), None, Some(366)].into();

        let b = date16_to_year(&a).unwrap();
        assert_eq!(1970, b.value(0));
        assert_eq!(false, b.is_valid(1));
        assert_eq!(1971, b.value(2));
    }

    #[test]
    fn stress_to_year() {
        let v: Vec<u16> = (0..4096).collect();
        let a: PrimitiveArray<Date16Type> = v.into();

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = date16_to_year(&a).unwrap();
            // let b = arrow::compute::kernels::temporal::year(&a).unwrap();
            s += b.len() as usize;
        }
        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }
}
