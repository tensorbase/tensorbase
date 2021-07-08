#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use arrow::array::GenericStringArray;
    use arrow::datatypes::{Int64Type, Timestamp32Type};
    use arrow::{array::PrimitiveArray, datatypes::Date16Type};
    use base::datetimes::BaseTimeZone;
    use datafusion::physical_plan::clickhouse::*;
    use std::sync::Arc;

    #[test]
    fn test_to_date() {
        // test to_date(Timestamp32)
        let a: PrimitiveArray<Timestamp32Type> =
            vec![Some(0), Some(536457600), None, Some(1609459200)].into();
        let b = timestamp32_to_date(&a, Some(BaseTimeZone::default())).unwrap();

        assert_eq!(0, b.value(0));
        assert_eq!(6209, b.value(1)); // 1987-01-01
        assert_eq!(false, b.is_valid(2));
        assert_eq!(18628, b.value(3)); // 2021-01-01

        // test to_date(Int64)
        let a: PrimitiveArray<Int64Type> =
            vec![Some(0), Some(6209), None, Some(18628), Some(-1)].into();
        let b = int64_to_date(&a).unwrap();
        assert_eq!(0, b.value(0));
        assert_eq!(6209, b.value(1)); // 1987-01-01
        assert_eq!(false, b.is_valid(2));
        assert_eq!(18628, b.value(3)); // 2021-01-01
        assert_eq!(0, b.value(4)); // 2021-01-01

        // test to_date(Utf8)
        let a: GenericStringArray<i64> =
            vec![Some("1970-1-1"), Some("1987-01-01"), Some("2021-01-01")].into();
        let a = Arc::new(a);
        let b = large_utf8_to_date(&[a]).unwrap();

        assert_eq!(0, b.value(0));
        assert_eq!(6209, b.value(1));
        assert_eq!(18628, b.value(2));

        let a: GenericStringArray<i64> =
            vec![Some("err"), Some("1987-01-01"), Some("2021-01-01")].into();
        let a = Arc::new(a);
        assert!(large_utf8_to_date(&[a]).is_err());

        let a: GenericStringArray<i64> =
            vec![Some("\u{10}1987-01-01"), Some("\u{10}2021-01-01")].into();
        let a = Arc::new(a);
        let b = large_utf8_to_date(&[a]).unwrap();
        assert_eq!(6209, b.value(0));
        assert_eq!(18628, b.value(1));

        // test to_date(LargeUtf8)
        let a: GenericStringArray<i32> =
            vec![Some("1970-1-1"), Some("1987-01-01"), Some("2021-01-01")].into();
        let a = Arc::new(a);
        let b = utf8_to_date(&[a]).unwrap();

        assert_eq!(0, b.value(0));
        assert_eq!(6209, b.value(1));
        assert_eq!(18628, b.value(2));

        let a: GenericStringArray<i32> =
            vec![Some("err"), Some("1987-01-01"), Some("2021-01-01")].into();
        let a = Arc::new(a);
        assert!(utf8_to_date(&[a]).is_err());
    }

    #[test]
    fn stress_to_date() {
        let v: Vec<i32> = (0..4096).collect();
        let a: PrimitiveArray<Timestamp32Type> = v.into();

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = timestamp32_to_date(&a, Some(BaseTimeZone::default())).unwrap();
            s += b.len() as usize;
        }

        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }

    #[test]
    fn test_to_year() {
        let a: PrimitiveArray<Date16Type> = vec![Some(1), None, Some(366)].into();

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

    #[test]
    fn test_to_month() {
        let a: PrimitiveArray<Date16Type> = vec![Some(1), None, Some(364)].into();

        let b = date16_to_month(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert_eq!(false, b.is_valid(1));
        assert_eq!(12, b.value(2));
    }

    #[test]
    fn stress_to_month() {
        let v: Vec<u16> = (0..4096).collect();
        let a: PrimitiveArray<Date16Type> = v.into();

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = date16_to_month(&a).unwrap();
            // let b = arrow::compute::kernels::temporal::year(&a).unwrap();
            s += b.len() as usize;
        }
        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }

    #[test]
    fn test_to_day_of_month() {
        let a: PrimitiveArray<Date16Type> = vec![Some(1), None, Some(58)].into();

        let b = date16_to_day_of_month(&a).unwrap();
        assert_eq!(2, b.value(0));
        assert_eq!(false, b.is_valid(1));
        assert_eq!(28, b.value(2));
    }

    #[test]
    fn stress_to_day_of_month() {
        let v: Vec<u16> = (0..4096).collect();
        let a: PrimitiveArray<Date16Type> = v.into();

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = date16_to_day_of_month(&a).unwrap();
            // let b = arrow::compute::kernels::temporal::year(&a).unwrap();
            s += b.len() as usize;
        }
        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }

    #[test]
    fn test_to_quarter() {
        let a: PrimitiveArray<Date16Type> =
            vec![Some(1), Some(59), None, Some(364)].into();

        let b = date16_to_quarter(&a).unwrap();
        assert_eq!(1, b.value(0));
        assert_eq!(1, b.value(1));
        assert_eq!(false, b.is_valid(2));
        assert_eq!(4, b.value(3));
    }

    #[test]
    fn stress_to_quarter() {
        let v: Vec<u16> = (0..4096).collect();
        let a: PrimitiveArray<Date16Type> = v.into();

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = date16_to_quarter(&a).unwrap();
            // let b = arrow::compute::kernels::temporal::year(&a).unwrap();
            s += b.len() as usize;
        }
        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }

    #[test]
    fn test_to_hour() {
        let a: PrimitiveArray<Timestamp32Type> =
            vec![Some(0), Some(3601), None, Some(7202)].into();

        let b = timestamp32_to_hour(&a, Some(BaseTimeZone::default())).unwrap();
        assert_eq!(0, b.value(0));
        assert_eq!(1, b.value(1));
        assert_eq!(false, b.is_valid(2));
        assert_eq!(2, b.value(3));
    }

    #[test]
    fn stress_to_hour() {
        let v: Vec<i32> = (0..4096).collect();
        let a: PrimitiveArray<Timestamp32Type> = v.into();

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = timestamp32_to_hour(&a, Some(BaseTimeZone::default())).unwrap();
            // let b = arrow::compute::kernels::temporal::year(&a).unwrap();
            s += b.len() as usize;
        }
        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }

    #[test]
    fn test_to_minute() {
        let a: PrimitiveArray<Timestamp32Type> =
            vec![Some(0), Some(301), None, Some(7802)].into();

        let b = timestamp32_to_minute(&a, Some(BaseTimeZone::default())).unwrap();
        assert_eq!(0, b.value(0));
        assert_eq!(5, b.value(1));
        assert_eq!(false, b.is_valid(2));
        assert_eq!(10, b.value(3));
    }

    #[test]
    fn stress_to_minute() {
        let v: Vec<i32> = (0..4096).collect();
        let a: PrimitiveArray<Timestamp32Type> = v.into();

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = timestamp32_to_minute(&a, Some(BaseTimeZone::default())).unwrap();
            // let b = arrow::compute::kernels::temporal::year(&a).unwrap();
            s += b.len() as usize;
        }
        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }

    #[test]
    fn test_to_second() {
        let a: PrimitiveArray<Timestamp32Type> =
            vec![Some(0), Some(325), None, Some(7849)].into();

        let b = timestamp32_to_second(&a, Some(BaseTimeZone::default())).unwrap();
        assert_eq!(0, b.value(0));
        assert_eq!(25, b.value(1));
        assert_eq!(false, b.is_valid(2));
        assert_eq!(49, b.value(3));
    }

    #[test]
    fn stress_to_second() {
        let v: Vec<i32> = (0..4096).collect();
        let a: PrimitiveArray<Timestamp32Type> = v.into();

        let ts = ::std::time::Instant::now();
        let mut s = 0;
        for _ in 0..100 {
            let b = timestamp32_to_second(&a, Some(BaseTimeZone::default())).unwrap();
            // let b = arrow::compute::kernels::temporal::year(&a).unwrap();
            s += b.len() as usize;
        }
        println!("ts: {:?}, s: {}", ts.elapsed(), s);
    }
}
