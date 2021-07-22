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
#![allow(incomplete_features)]
#![feature(
    backtrace,
    specialization,
    llvm_asm,
    vec_into_raw_parts,
    core_intrinsics,
    once_cell,
    associated_type_defaults
)]
pub mod codec;
pub mod contract;
pub mod datetimes;
pub mod errs;
pub mod fs;
pub mod fuzz;
pub mod hash;
pub mod mem;
pub mod mmap;
pub mod strings;
pub mod utils;
pub mod eval;

pub use base_proc_macro::async_test;
