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
#[cfg(debug_assertions)]
macro_rules! debug {
    ($x:expr) => {
        dbg!($x)
    };
}

#[macro_export]
macro_rules! bytes_cat {
    ($($arg:expr),*) => {{
        let mut v = Vec::new();
        $(
            v.extend_from_slice($arg);
        )*
        v
    }}
}

#[macro_export]
#[cfg(not(debug_assertions))]
macro_rules! debug {
    ($x:expr) => {
        std::convert::identity($x)
    };
}

#[macro_export]
macro_rules! seq {
    // Sequences
    ($($v:expr,)*) => {
        std::array::IntoIter::new([$($v,)*]).collect()
    };
    ($($v:expr),*) => {
        std::array::IntoIter::new([$($v,)*]).collect()
    };

    // Maps
    ($($k:expr => $v:expr,)*) => {
        std::array::IntoIter::new([$(($k, $v),)*]).collect()
    };
    ($($k:expr => $v:expr),*) => {
        std::array::IntoIter::new([$(($k, $v),)*]).collect()
    };
}

#[macro_export]
macro_rules! show_option_size {
    (header) => {
        println!("{:<22} {:>4}    {}", "Type", "T", "Option<T>");
    };
    ($t:ty) => {
        println!(
            "{:<22} {:4} {:4}",
            stringify!($t),
            std::mem::size_of::<$t>(),
            std::mem::size_of::<Option<$t>>()
        )
    };
}

//REF from dtolnay's https://github.com/dtolnay/reduce
pub trait Reduce<T> {
    fn reduce<F>(self, f: F) -> Option<T>
    where
        Self: Sized,
        F: FnMut(T, T) -> T;
}

impl<T, I> Reduce<T> for I
where
    I: Iterator<Item = T>,
{
    #[inline]
    fn reduce<F>(mut self, f: F) -> Option<T>
    where
        Self: Sized,
        F: FnMut(T, T) -> T,
    {
        self.next().map(|first| self.fold(first, f))
    }
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

    #[test]
    fn test_matches() {
        let foo = 'f';
        assert!(matches!(foo, 'A'..='Z' | 'a'..='z'));

        let bar = Some(4);
        assert!(matches!(bar, Some(x) if x > 2));
    }

    #[test]
    fn test_bytes_cat() {
        assert_eq!(bytes_cat!(b"a", b"B"), b"aB");
        let v = vec![1, 2, 3u8];
        assert_eq!(bytes_cat!(&v, &[4u8, 5, 6]), &[1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_reduce() {
        let v = vec![1usize, 2, 3, 4, 5];
        let sum = v.into_iter().reduce(|a, b| a + b);
        assert_eq!(Some(15), sum);

        // Reduce an empty iterator into None
        let v = Vec::<usize>::new();
        let sum = v.into_iter().reduce(|a, b| a + b);
        assert_eq!(None, sum);
    }

    #[test]
    fn test_next_power_of_two() {
        assert_eq!(0usize.next_power_of_two(), 1);
        assert_eq!(1u64.next_power_of_two(), 1);
        assert_eq!(2u64.next_power_of_two(), 2);
        assert_eq!(3u64.next_power_of_two(), 4);
        assert_eq!(8u64.next_power_of_two(), 8);
        assert_eq!(9u64.next_power_of_two(), 16);
    }
}
