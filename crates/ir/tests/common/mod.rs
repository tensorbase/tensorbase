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

use std::mem::ManuallyDrop;

pub const unsafe fn transmute<From, To>(from: From) -> To {
    union Transmute<From, To> {
        from: ManuallyDrop<From>,
        to: ManuallyDrop<To>,
    }

    ManuallyDrop::into_inner(
        Transmute {
            from: ManuallyDrop::new(from),
        }
        .to,
    )
}

pub const unsafe fn concat<First, Second, Out>(a: &[u8], b: &[u8]) -> Out
where
    First: Copy,
    Second: Copy,
    Out: Copy,
{
    #[repr(C)]
    #[derive(Copy, Clone)]
    struct Both<A, B>(A, B);

    let arr: Both<First, Second> = Both(
        *transmute::<_, *const First>(a.as_ptr()),
        *transmute::<_, *const Second>(b.as_ptr()),
    );

    transmute(arr)
}

#[macro_export]
macro_rules! const_concat {
    () => {
        ""
    };
    ($a:expr) => {
        $a
    };
    ($a:expr, $b:expr) => {{
        let bytes: &'static [u8] = unsafe {
            &self::concat::<
                [u8; $a.len()],
                [u8; $b.len()],
                [u8; $a.len() + $b.len()],
            >($a.as_bytes(), $b.as_bytes())
        };

        unsafe { self::transmute::<_, &'static str>(bytes) }
    }};
    ($a:expr, $($rest:expr),*) => {{
        const TAIL: &str = const_concat!($($rest),*);
        const_concat!($a, TAIL)
    }};
    ($a:expr, $($rest:expr),*,) => {
        const_concat!($a, $($rest),*)
    };
}

const PRJ_ROOT: &'static str = env!("CARGO_MANIFEST_DIR");
pub const TEST_RES_ROOT: &'static str = const_concat!(PRJ_ROOT, "/tests/res/");
