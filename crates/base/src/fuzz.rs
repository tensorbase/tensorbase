use std::ops::Range;

use rand::{
    distributions::{uniform::SampleUniform, Alphanumeric, Uniform},
    Rng,
};

pub trait Fuzzable {
    fn fuzz() -> Self;
}

pub trait BoundedFuzzable: Sized {
    fn fuzz_bound(range: Range<Self>) -> Self;
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
                fn fuzz_bound(range: Range<Self>) -> Self
                {
                    rand::thread_rng().gen_range(range)
                }
            }
        )*
    };
}

impl_bounded_fuzzable_for_prims! { u8, u16, u32, u64, i8, i16, i32, i64, usize, isize, u128, i128, }

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

pub trait BoundedFuzzableVec<T: BoundedFuzzable> {
    fn fuzz_bound(range: Range<T>) -> Vec<T>;
}

impl<T: BoundedFuzzable + SampleUniform> BoundedFuzzableVec<T> for Vec<T> {
    fn fuzz_bound(range: Range<T>) -> Vec<T> {
        let mut rng = rand::thread_rng();
        let len = usize::fuzz_bound(0..32);
        let r = Uniform::new(range.start, range.end);
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(rng.sample(&r));
        }
        vec
    }
}

macro_rules! impl_fuzzable_for_tuple {
    ($({$($t:tt)*},)*) => {
        $(
        impl<$($t: Fuzzable,)*> Fuzzable for ($($t,)*) {
            fn fuzz() -> Self {
                ($($t::fuzz(),)*)
            }
        }
        )*
    };
}

impl_fuzzable_for_tuple!(
    {A},
    {A B},
    {A B C},
    {A B C D},
    {A B C D E},
    {A B C D E F},
    {A B C D E F G},
    {A B C D E F G H},
    {A B C D E F G H I},
    {A B C D E F G H I J},
    {A B C D E F G H I J K},
    {A B C D E F G H I J K L},
);

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn basic_check() {
        assert!(u32::fuzz() != u32::fuzz()); //FIXME
        assert!(u64::fuzz_bound(0..100) != u64::fuzz_bound(101..200));
        assert!(Vec::<i64>::fuzz().len() < 32);
        let v = Vec::<(u16, String)>::fuzz();
        assert!(v.len() < 32);
        if v.len() > 0 {
            assert!(v[0].1.len() < 32);
        }
        // println!("{:?}", u64::fuzz_bound(0..100));
        // println!("{:?}", Option::<String>::fuzz());
        // println!("{:?}", Vec::<(u16, String)>::fuzz());
    }
}
