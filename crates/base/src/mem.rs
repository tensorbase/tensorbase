use std::mem;

use libc::c_void;

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
pub type MemAddr = *mut c_void;

unsafe impl<T> Send for SyncPointer<T> {}
unsafe impl<T> Sync for SyncPointer<T> {}

#[derive(Copy, Clone)]
pub struct SyncPointer<T>(pub *const T);
impl<T> SyncPointer<T> {
    #[inline]
    pub fn as_mut_ptr(&self) -> *mut T {
        self.0 as *mut T
    }

    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.0
    }
}

/// WARN it is your responsiblity to ensure this transmuting is safe
#[inline]
pub fn shape_slice<T>(slice: &[u8]) -> &[T] {
    unsafe {
        std::slice::from_raw_parts(
            slice.as_ptr() as *const T,
            slice.len() / mem::size_of::<T>(),
        )
    }
}

/// WARN it is your responsiblity to ensure this transmuting is safe
#[inline]
pub fn shape_vec<T>(v: Vec<u8>) -> Vec<T> {
    let (ptr, len, cap) = v.into_raw_parts();
    unsafe {
        Vec::from_raw_parts(
            ptr as *mut T,
            len / mem::size_of::<T>(),
            cap / mem::size_of::<T>(),
        )
    }
}


/// WARN it is your responsiblity to ensure this transmuting is safe
#[inline]
pub fn shape_vec_u8<T>(v: Vec<T>) -> Vec<u8> {
    let (ptr, len, cap) = v.into_raw_parts();
    unsafe {
        Vec::from_raw_parts(
            ptr as *mut u8,
            len * mem::size_of::<T>(),
            cap * mem::size_of::<T>(),
        )
    }
}


pub mod aligned {
    use std::alloc::Layout;

    // pub fn alloc(size: usize) -> *mut u8 {
    //     unsafe {
    //         let layout = Layout::from_size_align_unchecked(size, 64);
    //         std::alloc::alloc_zeroed(layout)
    //     }
    // }

    pub fn alloc<T>(size: usize) -> *mut T {
        unsafe {
            let siz = std::mem::size_of::<T>() * size;
            let layout = Layout::from_size_align_unchecked(siz, 64);
            std::alloc::alloc_zeroed(layout) as *mut T
        }
    }

    pub unsafe fn free(p: *mut u8, size: usize) {
        std::alloc::dealloc(p, Layout::from_size_align_unchecked(size, 64));
    }

    pub unsafe fn realloc(
        ptr: *mut u8,
        old_size: usize,
        new_size: usize,
    ) -> *mut u8 {
        let new_ptr = std::alloc::realloc(
            ptr,
            Layout::from_size_align_unchecked(old_size, 64),
            new_size,
        );
        if !new_ptr.is_null() && new_size > old_size {
            new_ptr.add(old_size).write_bytes(0, new_size - old_size);
        }
        new_ptr
    }

    pub fn is_aligned_to<T>(p: *const T, size: usize) -> bool {
        (p as usize % size) == 0
    }

    pub fn is_page_aligned<T>(p: *const T) -> bool {
        is_aligned_to(p, 4096)
    }

    pub fn is_aligned64<T>(p: *const T) -> bool {
        is_aligned_to(p, 64)
    }

    #[cfg(test)]
    mod unit_tests {
        use super::*;

        #[test]
        fn baisc_check() {
            for i in 0..10 {
                let p = alloc(i);
                // make sure this is 64-byte aligned
                assert!(is_aligned64(p));
                unsafe {
                    free(p, i);
                }
            }

            let p = alloc::<u8>(1);
            let ptr = unsafe { p.offset(4) };
            assert_eq!(true, is_aligned_to(ptr, 2));
            assert_eq!(true, is_aligned_to(ptr, 4));
            assert_eq!(false, is_aligned64(ptr));
        }
    }
}

/// WARN created vec does not support to extend
pub unsafe fn new_aligned64_vec<T>(cap: usize) -> Vec<T> {
    let ptr = aligned::alloc::<T>(cap);
    Vec::from_raw_parts(ptr, cap, cap)
}

// ====== Tests ======
#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn baisc_check() {
        let v = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
        let vc = v.clone();
        let v32: Vec<u32> = shape_vec(v);
        assert_eq!(v32.len(), 2);
        assert_eq!(v32[0], 0x04_03_02_01);
        assert_eq!(v32[1], 0x08_07_06_05);
        assert_eq!(shape_vec_u8(v32), vc);
    }
}


// ====== Benchmarks ======
