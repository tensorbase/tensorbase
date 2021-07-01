#[cfg(test)]
mod tests {
    use arrow::array::{UInt16Array, UInt8Array};
    use arrow::{
        array::{Date16Array, GenericStringArray, PrimitiveArray},
        datatypes::{Date16Type, Int64Type, Timestamp32Type},
    };
    use datafusion::physical_plan::{clickhouse::*, ColumnarValue};
    use std::sync::Arc;

    #[test]
    fn test_to_date() {
        let a: GenericStringArray<i64> = vec!["2020-02-01", "2022-10-28"].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_date(&[args]).unwrap();
        match b {
            ColumnarValue::Array(arr) => {
                assert_eq!(
                    arr.as_any()
                        .downcast_ref::<Date16Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(18293), Some(19293)]
                );
            }
            _ => {}
        }

        let a: GenericStringArray<i64> = vec![Some("")].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_date(&[args]);
        assert!(b.is_err());

        let a: GenericStringArray<i64> = vec![Some("2012")].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_date(&[args]);
        assert!(b.is_err());

        let a: PrimitiveArray<Int64Type> =
            vec![Some(1262304000_i64), Some(1298851200)].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_date(&[args]).unwrap();

        match b {
            ColumnarValue::Array(arr) => {
                assert_eq!(
                    arr.as_any()
                        .downcast_ref::<Date16Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(14610), Some(15033)]
                );
            }
            _ => {}
        }

        let a: PrimitiveArray<Timestamp32Type> = vec![1625131000].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_date(&[args]).unwrap();

        match b {
            ColumnarValue::Array(arr) => {
                println!("{:?}", arr);
                assert_eq!(
                    arr.as_any()
                        .downcast_ref::<Date16Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(18809)]
                );
            }
            _ => {}
        }
    }

    #[test]
    fn stress_to_date() {
        let a: GenericStringArray<i64> = std::iter::repeat("2020-2-1")
            .take(4096)
            .collect::<Vec<_>>()
            .into();
        let args = [ColumnarValue::Array(Arc::new(a))];

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = expr_to_date(&args).unwrap();
            s += b.into_array(1).len();
        }

        println!("todate, ts: {:?}, s: {}", ts.elapsed(), s);
    }

    #[test]
    fn test_to_year() {
        let a: PrimitiveArray<Date16Type> = vec![Some(1), Some(366)].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_year(&[args]).unwrap();

        match b {
            ColumnarValue::Array(arr) => {
                assert_eq!(
                    arr.as_any()
                        .downcast_ref::<UInt16Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(1970), Some(1971)]
                );
            }
            _ => {}
        }

        let a: PrimitiveArray<Date16Type> = vec![None].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_year(&[args]);
        assert!(b.is_err());
    }

    #[test]
    fn stress_to_year() {
        let v: Vec<u16> = (0..4096).collect();
        let a: PrimitiveArray<Date16Type> = v.into();
        let args = [ColumnarValue::Array(Arc::new(a))];

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 1..100 {
            let b = expr_to_year(&args).unwrap();
            s += b.into_array(1).len();
        }
        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }

    #[test]
    fn test_to_month() {
        let a: PrimitiveArray<Date16Type> = vec![Some(1), Some(364)].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_month(&[args]).unwrap();

        match b {
            ColumnarValue::Array(arr) => {
                assert_eq!(
                    arr.as_any()
                        .downcast_ref::<UInt8Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(1), Some(12)]
                );
            }
            _ => {}
        }

        let a: PrimitiveArray<Date16Type> = vec![None].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_year(&[args]);
        assert!(b.is_err());
    }

    #[test]
    fn stress_to_month() {
        let v: Vec<u16> = (0..4096).collect();
        let a: PrimitiveArray<Date16Type> = v.into();
        let args = [ColumnarValue::Array(Arc::new(a))];

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = expr_to_month(&args).unwrap();
            s += b.into_array(1).len();
        }

        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }

    #[test]
    fn test_to_day_of_month() {
        let a: PrimitiveArray<Date16Type> = vec![Some(1), Some(58)].into();
        let args = ColumnarValue::Array(Arc::new(a));

        let b = expr_to_day_of_month(&[args]).unwrap();

        match b {
            ColumnarValue::Array(arr) => {
                assert_eq!(
                    arr.as_any()
                        .downcast_ref::<UInt8Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(2), Some(28)]
                );
            }
            _ => {}
        }

        let a: PrimitiveArray<Date16Type> = vec![None].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_year(&[args]);
        assert!(b.is_err());
    }

    #[test]
    fn stress_to_day_of_month() {
        let v: Vec<u16> = (0..4096).collect();
        let a: PrimitiveArray<Date16Type> = v.into();
        let args = [ColumnarValue::Array(Arc::new(a))];

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = expr_to_day_of_month(&args).unwrap();
            s += b.into_array(1).len() as usize;
        }
        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }

    #[test]
    fn test_to_quarter() {
        let a: PrimitiveArray<Date16Type> = vec![Some(1), Some(59), Some(364)].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_quarter(&[args]).unwrap();

        match b {
            ColumnarValue::Array(arr) => {
                assert_eq!(
                    arr.as_any()
                        .downcast_ref::<UInt8Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(1), Some(1), Some(4)]
                );
            }
            _ => {}
        }

        let a: PrimitiveArray<Date16Type> = vec![None].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_year(&[args]);
        assert!(b.is_err());
    }

    #[test]
    fn stress_to_quarter() {
        let v: Vec<u16> = (0..4096).collect();
        let a: PrimitiveArray<Date16Type> = v.into();
        let args = [ColumnarValue::Array(Arc::new(a))];

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = expr_to_quarter(&args).unwrap();
            s += b.into_array(1).len() as usize;
        }
        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }

    #[test]
    fn test_to_hour() {
        let a: PrimitiveArray<Timestamp32Type> =
            vec![Some(0), Some(3601), Some(7202)].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_hour(&[args]).unwrap();

        match b {
            ColumnarValue::Array(arr) => {
                assert_eq!(
                    arr.as_any()
                        .downcast_ref::<UInt8Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(0), Some(1), Some(2)]
                );
            }
            _ => {}
        }

        let a: PrimitiveArray<Date16Type> = vec![None].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_year(&[args]);
        assert!(b.is_err());
    }

    #[test]
    fn stress_to_hour() {
        let v: Vec<i32> = (0..4096).collect();
        let a: PrimitiveArray<Timestamp32Type> = v.into();
        let args = [ColumnarValue::Array(Arc::new(a))];

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = expr_to_hour(&args).unwrap();
            // let b = arrow::compute::kernels::temporal::year(&a).unwrap();
            s += b.into_array(0).len() as usize;
        }
        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }

    #[test]
    fn test_to_minute() {
        let a: PrimitiveArray<Timestamp32Type> =
            vec![Some(0), Some(301), Some(7802)].into();

        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_minute(&[args]).unwrap();

        match b {
            ColumnarValue::Array(arr) => {
                assert_eq!(
                    arr.as_any()
                        .downcast_ref::<UInt8Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(0), Some(5), Some(10)]
                );
            }
            _ => {}
        }

        let a: PrimitiveArray<Date16Type> = vec![None].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_year(&[args]);
        assert!(b.is_err());
    }

    #[test]
    fn stress_to_minute() {
        let v: Vec<i32> = (0..4096).collect();
        let a: PrimitiveArray<Timestamp32Type> = v.into();
        let args = [ColumnarValue::Array(Arc::new(a))];

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = expr_to_minute(&args).unwrap();
            // let b = arrow::compute::kernels::temporal::year(&a).unwrap();
            s += b.into_array(1).len() as usize;
        }
        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }

    #[test]
    fn test_to_second() {
        let a: PrimitiveArray<Timestamp32Type> =
            vec![Some(0), Some(325), Some(7849)].into();

        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_second(&[args]).unwrap();

        match b {
            ColumnarValue::Array(arr) => {
                assert_eq!(
                    arr.as_any()
                        .downcast_ref::<UInt8Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    vec![Some(0), Some(25), Some(49)]
                );
            }
            _ => {}
        }

        let a: PrimitiveArray<Date16Type> = vec![None].into();
        let args = ColumnarValue::Array(Arc::new(a));
        let b = expr_to_year(&[args]);
        assert!(b.is_err());
    }

    #[test]
    fn stress_to_second() {
        let v: Vec<i32> = (0..4096).collect();
        let a: PrimitiveArray<Timestamp32Type> = v.into();
        let args = [ColumnarValue::Array(Arc::new(a))];

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = expr_to_second(&args).unwrap();
            // let b = arrow::compute::kernels::temporal::year(&a).unwrap();
            s += b.into_array(1).len() as usize;
        }
        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }
}
