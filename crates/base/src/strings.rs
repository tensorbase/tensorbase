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
use std::str;

pub use base_proc_macro::s;

pub trait PutIntoString {
    fn put_into_string(self, sbuf: &mut String);
}

impl<T> PutIntoString for T {
    default fn put_into_string(self, _: &mut String) {
        unimplemented!("unsupported type!");
    }
}

impl<T: ToString> PutIntoString for T {
    default fn put_into_string(self, sbuf: &mut String) {
        sbuf.push_str(&self.to_string());
        // unimplemented!("unsupported type!");
    }
}

impl PutIntoString for String {
    fn put_into_string(self, sbuf: &mut String) {
        sbuf.push_str(&self);
    }
}

impl PutIntoString for &String {
    fn put_into_string(self, sbuf: &mut String) {
        sbuf.push_str(self);
    }
}

impl PutIntoString for &str {
    fn put_into_string(self, sbuf: &mut String) {
        sbuf.push_str(self);
    }
}

macro_rules! def_put_into_string_for_int {
    ($($name:ident,)*) => {$(
        impl PutIntoString for $name {
            fn put_into_string(self, sbuf: &mut String) {
                itoa::fmt(sbuf, self);
            }
        }
    )*
    };
}

//FIXME u128,i128 opt-in
def_put_into_string_for_int! {u8,u16,u32,u64,usize,i8,i16,i32,i64,isize,}

macro_rules! def_put_into_string_for_float {
    ($($name:ident,)*) => {$(
impl PutIntoString for $name {
    fn put_into_string(self, sbuf: &mut String) {
        let mut bytes = [b'\0'; 32];
        match dtoa::write(&mut bytes[..], self) {
            Ok(n) => {
                unsafe {
                    sbuf.push_str(str::from_utf8_unchecked(&bytes[..n]));
                }
            }
            Err(e) => panic!(e)
        }
    }
}
)*
};
}

def_put_into_string_for_float! { f32, f64,}

#[inline]
pub fn remove_whitespace(s: &mut String) {//remove_whitespace
    s.retain(|c| !c.is_whitespace());
}

#[cfg(test)]
mod unit_tests {
    #[test]
    fn basic_check() {
        let mut s = String::from(" a ( b ) \n * \t  c  ");
        super::remove_whitespace(&mut s);
        crate::debug!(&s);
        assert!(&s == "a(b)*c");
    }
}
