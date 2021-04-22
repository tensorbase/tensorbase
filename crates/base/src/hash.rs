use std::convert::TryInto;

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

// inline uint64_t hash_bytes(const void *data, size_t len) {
//     uint64_t rt = 0;
//     // #pragma clang loop unroll_count(8)
//     for (size_t i = 0; i < len; i += 8) {
//       rt ^= _mm_crc32_u64(0, read_uint64((const char*)data + i)); // FIXME or ^?
//     }
//     return rt;
//   }
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

        let mut ss = 0u64;
        for i in 0u64..1000000 {
            let s = format!("../primitives/tests/all_tests.c{}", i);
            ss += s.hash();
        }
        println!("ss: {}", ss);
        assert_eq!(ss, 2147478854486776);
    }
}
