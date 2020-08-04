/*
 *   Copyright (c) 2020 TensorBase, and its contributors
 *   All rights reserved.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

use ::base::mem::new_aligned64_vec;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};

/**
 ** TODO allow custom rng per call?
 **/

pub fn rand_perm_int_seq(n: u64) -> Vec<u64> {
    rand_perm_vec(0..n)
}

pub fn rand_int_range(from: u64, to: u64) -> std::ops::Range<u64> {
    rand_int_range_with_rng(from, to, &mut thread_rng())
}

pub fn rand_int_range_with_rng(
    from: u64,
    to: u64,
    rng: &mut ThreadRng,
) -> std::ops::Range<u64> {
    let s1 = rng.gen_range(from, to);
    let s2 = rng.gen_range(from, to);
    if s1 < s2 {
        s1..(s2 + 1)
    } else {
        s2..(s1 + 1)
    }
}

pub fn rand_int_range_u32(from: u32, to: u32) -> std::ops::Range<u32> {
    rand_int_range_with_rng_u32(from, to, &mut thread_rng())
}

pub fn rand_int_range_with_rng_u32(
    from: u32,
    to: u32,
    rng: &mut ThreadRng,
) -> std::ops::Range<u32> {
    let s1 = rng.gen_range(from, to);
    let s2 = rng.gen_range(from, to);
    if s1 < s2 {
        s1..(s2 + 1)
    } else {
        s2..(s1 + 1)
    }
}

pub fn rand_int_range_even(from: u64, to: u64) -> std::ops::Range<u64> {
    assert!(to > (from + 1));
    assert!(to > 1);
    let mut rng = thread_rng();
    let s1 = rng.gen_range(from, to);
    let s2 = rng.gen_range(from, to);
    if s1 < s2 {
        s1..(s2 + ((s2 - s1) % 2))
    } else if s1 == s2 {
        if s2 >= to - 2 {
            (to - 2)..to
        } else {
            s2..(s2 + 2)
        }
    } else {
        s2..(s1 + ((s1 - s2) % 2))
    }
}

pub fn rand_perm_vec<T>(r: impl Iterator<Item = T>) -> Vec<T> {
    let mut rng = thread_rng();
    let mut v: Vec<T> = r.collect();
    v.shuffle(&mut rng);
    v
}

pub fn rand_perm_vec_aligned<T: Copy>(r: impl Iterator<Item = T>) -> Vec<T> {
    let mut rng = thread_rng();
    let _t: Vec<T> = r.collect();
    unsafe {
        let mut v: Vec<T> = new_aligned64_vec::<T>(_t.len());
        v.copy_from_slice(&_t[..]);
        v.shuffle(&mut rng);
        v
    }
}

pub fn rand_vec_aligned_u64(r: std::ops::Range<u64>, size: usize) -> Vec<u64> {
    let mut rng = thread_rng();
    unsafe {
        let mut v = new_aligned64_vec::<u64>(size);
        for i in 0..size {
            let e = rng.gen_range(r.start, r.end);
            v[i] = e;
        }
        v
    }
}

pub fn rand_vec_aligned_u32(r: std::ops::Range<u32>, size: usize) -> Vec<u32> {
    let mut rng = thread_rng();
    unsafe {
        let mut v = new_aligned64_vec::<u32>(size);
        for i in 0..size {
            let e = rng.gen_range(r.start, r.end);
            v[i] = e;
        }
        v
    }
}

#[cfg(test)]
mod tests_test_base {
    use super::*;
    use std::time::Instant;

    #[ignore]
    #[test]
    pub fn basic_check() {
        let v = rand_perm_int_seq(2);
        assert!(v == vec![1, 0] || v == vec![0, 1]);

        let v = rand_perm_vec(2..4);
        assert!(v == vec![2, 3] || v == vec![3, 2]);

        let v = rand_perm_vec_aligned(2..4);
        assert!(v == vec![2, 3] || v == vec![3, 2]);

        let v = rand_perm_vec(9..=10);
        assert!(v == vec![9, 10] || v == vec![10, 9]);

        let v = rand_perm_vec_aligned(9..=10);
        assert!(v == vec![9, 10] || v == vec![10, 9]);

        let rng = &mut thread_rng();
        let N = 200_000u64;
        let timer = Instant::now();

        for _ in 0..N {
            let r = rand_int_range_even(0, 101);
            assert!((r.end - r.start) % 2 == 0);
            assert!(r.start >= 0 && r.end <= 101);
            let r = rand_int_range_with_rng(0, 123, rng);
            assert!(r.start >= 0 && r.end <= 123);
        }
        println!(
            "time cost: {:?}, ops: {}",
            timer.elapsed(),
            N * 1000 / (timer.elapsed().as_millis() as u64)
        );
        // dbg!(r);

        for _ in 1..2000 {
            let r = rand_vec_aligned_u64(1..5, 200);
            // dbg!(&r.len());
            assert!(r.len() == 200);
            assert!(
                (*r.iter().max().unwrap() < 5u64)
                    && (*r.iter().min().unwrap() >= 1u64)
            );
            // dbg!(&r);
        }

        for _ in 1..2000 {
            let r = rand_vec_aligned_u32(1..5, 123);
            // dbg!(&r.len());
            assert!(r.len() == 123);
            assert!(
                (*r.iter().max().unwrap() < 5u32)
                    && (*r.iter().min().unwrap() >= 1u32)
            );
            // dbg!(&r);
        }
    }
}
