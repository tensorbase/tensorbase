use bytes::{BufMut, BytesMut};

use crate::ch::protocol::ServerCodes;
use crate::errs::{BaseRtError, BaseRtResult};
use crate::types::BaseWriteAware;
use std::{intrinsics::copy_nonoverlapping, slice};

pub trait BytesEncoder {
    fn encode(&self, bs: &mut BytesMut) -> BaseRtResult<()>;
}

pub trait BytesDecoder<R: BytesEncoder> {
    fn decode_column(&mut self, nrows: usize) -> BaseRtResult<R>;
}

//NOTE get_slice may be not useful as thought
///NOTE: the caller's responsibility to ensure the range of slice is valid
pub fn new_mut_slice_at<T>(ptr: &mut T, len: usize) -> &mut [T] {
    unsafe { slice::from_raw_parts_mut(ptr, len) }
}

pub trait BytesExt {
    fn ensure_enough_bytes_to_write(&mut self, additional_space: usize);

    ///NOTE: the caller's responsibility to ensure the range of slice is valid
    fn get_slice_at_write_cursor(&self, len: usize) -> &[u8];

    ///NOTE: the caller's responsibility to ensure the range of slice is valid
    fn get_slice_mut_at_write_cursor(&mut self, len: usize) -> &mut [u8];

    ///NOTE: the caller's responsibility to ensure the advanced cnt is valid
    fn advance_mut_unsafe(&mut self, cnt: usize);

    fn get_ptr_at_write_cursor(&self) -> *const u8;

    fn get_slice_as_read_buf(&mut self) -> &[u8];
}

impl BytesExt for BytesMut {
    #[inline(always)]
    fn get_slice_as_read_buf(&mut self) -> &[u8] {
        let ptr0 = self.as_ptr();
        let len0 = self.len();
        unsafe { std::slice::from_raw_parts(ptr0, len0) }
    }

    #[inline(always)]
    fn ensure_enough_bytes_to_write(&mut self, additional_space: usize) {
        self.reserve(additional_space);
    }

    #[inline(always)]
    fn get_slice_at_write_cursor(&self, len: usize) -> &[u8] {
        unsafe { slice::from_raw_parts(self.get_ptr_at_write_cursor(), len) }
    }

    #[inline(always)]
    fn get_slice_mut_at_write_cursor(&mut self, len: usize) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(self.as_mut_ptr().offset(self.len() as isize), len)
        }
    }

    #[inline(always)]
    fn advance_mut_unsafe(&mut self, cnt: usize) {
        unsafe {
            self.advance_mut(cnt);
        }
    }

    fn get_ptr_at_write_cursor(&self) -> *const u8 {
        unsafe { self.as_ptr().offset(self.len() as isize) }
    }
}

pub trait CHMsgWriteAware {
    // fn write_fixint<T: PrimInt>(&mut self, value: T);
    fn write_str(&mut self, value: &str); //FIXME return BaseRtResult for write?
    fn write_as_exception(&mut self, err: BaseRtError);
    fn write_empty_block(&mut self);
    fn write_end_of_stream(&mut self);
}

impl CHMsgWriteAware for BytesMut {
    #[inline(always)]
    fn write_str(&mut self, value: &str) {
        self.write_varbytes(value.as_bytes());
    }

    #[inline(always)]
    fn write_as_exception(&mut self, err: BaseRtError) {
        self.write_varint(ServerCodes::Exception as u64);
        self.put_u32_le(err.to_err_code());
        self.write_str(&format!("{:?}", err));
        self.write_str(&format!("{}", err));
        self.write_str("");
        self.write_varint(0);
    }

    #[inline(always)]
    fn write_empty_block(&mut self) {
        /// Set of pairs (`FIELD_NUM`, value) in binary form. Then 0.
        // 1  [0]    - ?
        // 0  [1]    - overflow
        // 2  [2]    - ?
        // -1 [3..7] - bucket num as int32
        // 0  [8]    - ?

        const EMPTY_BLK: [u8; 10] = [1u8, 0, 2, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0];
        unsafe {
            copy_nonoverlapping(
                &EMPTY_BLK as *const u8,
                self.as_mut_ptr().add(self.len()),
                10,
            );
            self.advance_mut(10);
        }
    }

    #[inline(always)]
    fn write_end_of_stream(&mut self) {
        self.write_varint(ServerCodes::EndOfStream as u64);
    }

    // #[inline(always)]
    // fn read_fix<T>(&mut self) -> T {
    //     let len = std::mem::size_of::<T>();

    // }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_block_sanity_check() -> BaseRtResult<()> {
        let mut bs = BytesMut::with_capacity(10);

        // let bch = BlockColumnHeader {
        //     name: "some_field".to_string(),
        //     field: Field {
        //         sql_type: SqlType::UInt64,
        //         ..Default::default()
        //     },
        // };

        // bch.encode(&mut bs)?;

        // println!(
        //     "bs.len: {}, bs: {}",
        //     &bs.len(),
        //     String::from_utf8_lossy(&bs)
        // );

        Ok(())
    }
}
