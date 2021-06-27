#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use arrow::{array::PrimitiveArray, datatypes::Date16Type};
    use datafusion::physical_plan::clickhouse::*;

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

    #[test]
    fn test_to_month() {
        let a: PrimitiveArray<Date16Type> =
            vec![Some(1), None, Some(364)].into();

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
        let a: PrimitiveArray<Date16Type> =
            vec![Some(1), None, Some(58)].into();

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
        println!("b: {:?}", date16_to_day_of_month(&a).unwrap());
    }
}
