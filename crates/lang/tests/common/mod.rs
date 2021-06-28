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
#![feature(const_fn_trait_bound)]
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

const PRJ_ROOT: &'static str = env!("CARGO_MANIFEST_DIR");
pub fn get_test_root() -> String {
    [PRJ_ROOT, "/tests/res/"].join("")
}
