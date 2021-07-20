pub trait Hasher {
    fn hash(&self) -> u64;
}

macro_rules! decl_Hasher {
    ($($t:ident,)*) => {
      $(
        impl Hasher for $t {
            #[inline]
            fn hash(&self) -> u64 {
                #[inline]
                unsafe fn _hash(k1: u64, k2: u64) -> u64 {
                    use std::arch::x86_64::*;
                    _mm_crc32_u64(k1, k2)
                }
                unsafe { _hash(0, *self as u64) }
            }
        }
      )*
    }
}

decl_Hasher! { usize,u64,i64,u32,i32,u16,i16,u8,i8, }

impl Hasher for [u8] {
    #[inline]
    fn hash(&self) -> u64 {
        crc32c::crc32c(self) as u64
    }
}

impl Hasher for str {
    #[inline]
    fn hash(&self) -> u64 {
        let bs = self.as_bytes();
        bs.hash()
    }
}

impl Hasher for String {
    #[inline]
    fn hash(&self) -> u64 {
        let bs = self.as_bytes();
        bs.hash()
    }
}

#[cfg(test)]
mod unit_tests {
    use super::Hasher;

    #[test]
    fn hasher_basic_check() {
        assert!(123u64.hash() == 123u16.hash());
        assert!(123u64.hash() == 123u8.hash());
        assert!(u64::MAX.hash() == (-1i64).hash());
        println!("{}", 123u16.hash());

        //
        println!("{}", "../primitives/tests/all_tests.c0".hash());
        assert_eq!("../primitives/tests/all_tests.c0".hash(), 3909039897);
        assert_eq!(b"../primitives/tests/all_tests.c0".hash(), 3909039897);

        let mut ss0 = 0u64;
        for i in 0u64..1000000 {
            let s = format!("../primitives/tests/all_tests.c{}", i);
            ss0 += (s.as_str()).hash();
        }
        println!("ss0: {}", ss0);
        assert_eq!(ss0, 2147478854486776);

        let mut ss1 = 0u64;
        for i in 0u64..1000000 {
            let s = format!("../primitives/tests/all_tests.c{}", i);
            ss1 += s.hash();
        }
        println!("ss1: {}", ss1);
        assert_eq!(ss0, ss1);
    }
}
