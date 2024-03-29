// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Defines a bitmap, which is used to track which values in an Arrow array are null.
//! This is called a "validity bitmap" in the Arrow documentation.

use crate::buffer::Buffer;
use crate::error::Result;
use crate::util::bit_util;
use std::mem;

use std::ops::{BitAnd, BitOr};

#[derive(Debug, Clone)]
pub struct Bitmap {
    pub(crate) bits: Buffer,
}

impl Bitmap {
    pub fn new(num_bits: usize) -> Self {
        let num_bytes = num_bits / 8 + if num_bits % 8 > 0 { 1 } else { 0 };
        let r = num_bytes % 64;
        let len = if r == 0 {
            num_bytes
        } else {
            num_bytes + 64 - r
        };
        Bitmap {
            bits: Buffer::from(&vec![0xFF; len]),
        }
    }

    pub fn len(&self) -> usize {
        self.bits.len() * 8
    }

    pub fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }

    pub fn is_set(&self, i: usize) -> bool {
        assert!(i < (self.bits.len() << 3));
        unsafe { bit_util::get_bit_raw(self.bits.as_ptr(), i) }
    }

    pub fn buffer_ref(&self) -> &Buffer {
        &self.bits
    }

    pub fn into_buffer(self) -> Buffer {
        self.bits
    }

    /// Returns the total number of bytes of memory occupied by the buffers owned by this [Bitmap].
    pub fn get_buffer_memory_size(&self) -> usize {
        self.bits.capacity()
    }

    /// Returns the total number of bytes of memory occupied physically by this [Bitmap].
    pub fn get_array_memory_size(&self) -> usize {
        self.bits.capacity() + mem::size_of_val(self)
    }
}

impl<'a, 'b> BitAnd<&'b Bitmap> for &'a Bitmap {
    type Output = Result<Bitmap>;

    fn bitand(self, rhs: &'b Bitmap) -> Result<Bitmap> {
        Ok(Bitmap::from((&self.bits & &rhs.bits)?))
    }
}

impl<'a, 'b> BitOr<&'b Bitmap> for &'a Bitmap {
    type Output = Result<Bitmap>;

    fn bitor(self, rhs: &'b Bitmap) -> Result<Bitmap> {
        Ok(Bitmap::from((&self.bits | &rhs.bits)?))
    }
}

impl From<Buffer> for Bitmap {
    fn from(buf: Buffer) -> Self {
        Self { bits: buf }
    }
}

impl PartialEq for Bitmap {
    fn eq(&self, other: &Self) -> bool {
        // buffer equality considers capacity, but here we want to only compare
        // actual data contents
        let self_len = self.bits.len();
        let other_len = other.bits.len();
        if self_len != other_len {
            return false;
        }
        self.bits.as_slice()[..self_len] == other.bits.as_slice()[..self_len]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_length() {
        assert_eq!(512, Bitmap::new(63 * 8).len());
        assert_eq!(512, Bitmap::new(64 * 8).len());
        assert_eq!(1024, Bitmap::new(65 * 8).len());
    }

    #[test]
    fn test_bitwise_and() {
        let bitmap1 = Bitmap::from(Buffer::from([0b01101010]));
        let bitmap2 = Bitmap::from(Buffer::from([0b01001110]));
        assert_eq!(
            Bitmap::from(Buffer::from([0b01001010])),
            (&bitmap1 & &bitmap2).unwrap()
        );
    }

    #[test]
    fn test_bitwise_or() {
        let bitmap1 = Bitmap::from(Buffer::from([0b01101010]));
        let bitmap2 = Bitmap::from(Buffer::from([0b01001110]));
        assert_eq!(
            Bitmap::from(Buffer::from([0b01101110])),
            (&bitmap1 | &bitmap2).unwrap()
        );
    }

    #[test]
    fn test_bitmap_is_set() {
        let bitmap = Bitmap::from(Buffer::from([0b01001010]));
        assert!(!bitmap.is_set(0));
        assert!(bitmap.is_set(1));
        assert!(!bitmap.is_set(2));
        assert!(bitmap.is_set(3));
        assert!(!bitmap.is_set(4));
        assert!(!bitmap.is_set(5));
        assert!(bitmap.is_set(6));
        assert!(!bitmap.is_set(7));
    }
}
