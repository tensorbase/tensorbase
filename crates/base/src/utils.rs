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
#[macro_export]
macro_rules! with_timer {
    ($timer_name:ident, $($s:stmt);+ $(;)?) => {
        let $timer_name = ::std::time::Instant::now();
        $($s)*
    };
}

#[macro_export]
macro_rules! with_timer_print {
    ($timer_name:ident, $($s:stmt);+ $(;)?) => {
        let $timer_name = ::std::time::Instant::now();
        $($s)*
        println!("{:?}", $timer_name.elapsed());
    };
}

#[macro_export]
macro_rules! contract {
    ($x:expr) => {
        assert!($x)
    };
}

#[macro_export]
#[cfg(debug_assertions)]
macro_rules! debug {
    ($x:expr) => {
        dbg!($x)
    };
}

#[macro_export]
#[cfg(not(debug_assertions))]
macro_rules! debug {
    ($x:expr) => {
        std::convert::identity($x)
    };
}

#[cfg(test)]
mod unit_tests {

    #[test]
    fn basic_check() {
        let x = 4;
        debug!(x);
        if debug!(x == 5) {
            println!("x == 5");
        } else {
            println!("x != 5");
        }
    }
}
