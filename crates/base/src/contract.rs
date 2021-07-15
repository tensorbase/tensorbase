#[macro_export]
macro_rules! contract {
    ($($c:tt)*) => {
        debug_assert!($($c)*);
    };
}

#[cfg(test)]
mod unit_tests {
    fn test_fn_01(x: i32, y: i64) -> i64 {
        contract!(x < 0 && x > -100, "x should be negative integer and larger that -100");
        contract!(y > 100, "y should postive integer");

        let r = x as i64 + y;

        contract!(r > 0);
        r
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic]
    fn basic_check_debug() {
        test_fn_01(1, 99);
    }

    #[test]
    #[cfg(not(debug_assertions))]
    fn basic_check_release() {
        test_fn_01(1, 99);
    }
}
