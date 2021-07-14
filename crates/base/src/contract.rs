#[macro_export]
macro_rules! contract {
    ($x:expr) => {
        assert!($x)
    };
}