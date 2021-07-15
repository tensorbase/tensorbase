use rand::{
    distributions::{
        uniform::{SampleRange, SampleUniform},
        Alphanumeric,
    },
    Rng,
};

pub trait Fuzzable {
    fn fuzz() -> Self;
}

pub trait BoundedFuzzable {
    fn fuzz_bound<T, R>(range: R) -> T
    where
        T: SampleUniform,
        R: SampleRange<T>;
}

macro_rules! impl_fuzzable_for_prims {
    ($($t:ty,)*) => {
        $(
            impl Fuzzable for $t {
                fn fuzz() -> Self {
                  rand::thread_rng().gen()
                }
            }
        )*
    };
}

impl_fuzzable_for_prims! { u8, u16, u32, u64, i8, i16, i32, i64, usize, isize, u128, i128, bool, }

macro_rules! impl_bounded_fuzzable_for_prims {
    ($($t:ty,)*) => {
        $(
            impl BoundedFuzzable for $t {
                fn fuzz_bound<T, R>(range: R) -> T
                where
                    T: SampleUniform,
                    R: SampleRange<T>,
                {
                    rand::thread_rng().gen_range(range)
                }
            }
        )*
    };
}

impl_bounded_fuzzable_for_prims! { u8, u16, u32, u64, i8, i16, i32, i64, usize, isize, u128, i128, bool, }

impl<T: Fuzzable> Fuzzable for Option<T> {
    fn fuzz() -> Self {
        if bool::fuzz() {
            Some(T::fuzz())
        } else {
            None
        }
    }
}

impl Fuzzable for String {
    fn fuzz() -> Self {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(usize::fuzz_bound(0..32))
            .map(char::from)
            .collect()
    }
}

impl<T: Fuzzable> Fuzzable for Vec<T> {
    fn fuzz() -> Self {
        (0..u64::fuzz_bound(0..32)).map(|_| T::fuzz()).collect()
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn basic_check() {
        assert!(u32::fuzz() != u32::fuzz()); //FIXME
        assert!(u64::fuzz_bound(0..100) != u64::fuzz_bound(101..200));
        assert!(Vec::<i64>::fuzz().len() <= 32);
        // println!("{:?}", u64::fuzz_bound(0..100));
        // println!("{:?}", Option::<String>::fuzz());
    }
}
