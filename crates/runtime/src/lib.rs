/*   Copyright (c) 2020 TensorBase, and its contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

#![feature(const_generics)]
#![allow(unused)]

mod jit;

use base::{debug, with_timer, with_timer_print};
use jit::{CompilationUnit, Engine, GobalLibCompilationUnit};
use lazy_static::lazy_static;
use std::{
    ffi::c_void,
    sync::{Arc, Mutex, Once},
};

//TEMP
pub struct ResultSet {
    col_names: Vec<String>,
    col_slices: Vec<Vec<u8>>,
}

static LIB_INIT: Once = Once::new();
// static ENG: &'static Engine = ENG_INIT.call_once(|| &Engine::new());

lazy_static! {
    static ref ENG: Engine = Engine::new();
}

pub fn run_ker(ker: String) -> usize {
    LIB_INIT.call_once(|| {
        GobalLibCompilationUnit::init(
            &ENG,
            include_str!("../ker_lib/ker_lib.c"),
        );
        println!("lib init done.")
    });
    with_timer!(t1,
        let cu_ker = ENG.jitc(&ker);
        let ret = cu_ker.call();
    );
    println!("kernel exec time: {:?}", t1.elapsed());

    // let mut ms = 0 as u64;
    // let p = Box::into_raw(Box::new(&mut ms)) as *mut *mut c_void;
    // debug!(ret);
    ret
}

#[cfg(test)]
mod unit_tests {
    use crate::run_ker;
    use base::debug;
    use ir::codegen;

    #[test]
    fn test_kernel_run() {
        let sql = "select 3 * sum ( 2 *t.c0 -1 ) from t";
        let ker = codegen::gen(sql);
        run_ker(ker.clone());
        // std::thread::sleep(std::time::Duration::from_millis(1000) );
        // run_ker(ker.clone());
        // std::thread::sleep(std::time::Duration::from_millis(1000) );
        // run_ker(ker.clone());
        // println!("{}", &ker);
    }

    #[test]
    fn test_kernel_run_2() {
        let sql = "select sum(c) from t";
        let ker = codegen::gen(sql);
        // println!("{}",&ker);
        let ret = run_ker(ker);
        debug!(ret);
    }
}
