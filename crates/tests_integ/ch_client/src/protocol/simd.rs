#[cfg(target_arch = "x86")]
use core::arch::x86::*;
#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::*;

/// Convert byte array to array of bits
/// 0 byte - bit set
/// 1 byte - bit not set
#[allow(dead_code)]
#[target_feature(enable = "avx2")]
#[allow(clippy::cast_ptr_alignment)]
pub unsafe fn load_packed_flag(flag: &[u8]) -> Box<[u32]> {
    let wcount: usize = flag.len() / 32;
    let rem = 32 - (flag.len() & 0x1f);

    let mut res: Vec<u32> = Vec::with_capacity(if rem < 32 { wcount + 1 } else { wcount });

    res.set_len(res.capacity());

    let one: __m256i = _mm256_set1_epi8(0x01);
    let mut src: *const __m256i = flag.as_ptr() as *const __m256i;
    let mut dst = res.as_mut_ptr();
    for _ in 0..wcount {
        let x = _mm256_lddqu_si256(src);
        let m = _mm256_sub_epi8(x, one);
        *dst = _mm256_movemask_epi8(m) as u32;
        src = src.add(1);
        dst = dst.add(1);
    }
    // last 1 to 32 bytes
    if rem < 32 {
        // read last 32 bytes
        src = flag.as_ptr().add(flag.len() - 32) as *const __m256i;
        let x = _mm256_lddqu_si256(src);
        let m = _mm256_sub_epi8(x, one);
        *dst = (_mm256_movemask_epi8(m) as u32) >> rem;
    }

    res.into_boxed_slice()
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn load_packed_flag_bound() {
        let b1 = [
            0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0,
            1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0,
            0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1,
        ];
        let packed = unsafe { load_packed_flag(&b1[..]) };

        //println!("res {:b} {:b} {:b}",packed[0],packed[1],packed[2]);
        assert_eq!(
            packed.as_ref(),
            &[0x1e0a01e3_u32, 0xffffffff, 0x00, 0xff00ff]
        );
    }

    #[test]
    fn load_packed_flag_unbound() {
        let b1 = [
            0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0,
            1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let packed = unsafe { load_packed_flag(&b1[..]) };
        assert_eq!(packed.as_ref(), &[0x1e0a01e3_u32, 0xff00]);
        // swap 2 bytes of each word
        let b1 = [
            0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 1, 0, 1,
            1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1,
        ];
        let packed = unsafe { load_packed_flag(&b1[..]) };
        assert_eq!(packed.as_ref(), &[0x0a1e01e3_u32, 0xff]);
    }
}
