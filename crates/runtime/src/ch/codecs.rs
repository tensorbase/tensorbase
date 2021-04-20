use bytes::{Buf, BufMut, BytesMut};

use crate::ch::protocol::ServerCodes;
use crate::errs::{BaseRtError, BaseRtResult};
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
            slice::from_raw_parts_mut(
                self.as_mut_ptr().offset(self.len() as isize),
                len,
            )
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
    fn write_varint(&mut self, value: u64);
    // fn write_fixint<T: PrimInt>(&mut self, value: T);
    fn write_varbytes(&mut self, value: &[u8]);
    fn write_str(&mut self, value: &str); //FIXME return BaseRtResult for write?
    fn write_as_exception(&mut self, err: BaseRtError);
    fn write_empty_block(&mut self);
    fn write_end_of_stream(&mut self);
}

pub trait CHMsgReadAware {
    fn ensure_enough_bytes_to_read(
        &mut self,
        additional_space: usize,
    ) -> BaseRtResult<()>;
    fn read_varint(&mut self) -> BaseRtResult<u64>;
    fn read_str<'a, 'b>(&mut self) -> BaseRtResult<&'b str>;
    fn read_varbytes<'a, 'b>(&mut self) -> BaseRtResult<&'b [u8]>;
    // fn read_fix<T>(&mut self) -> T;
}

impl CHMsgWriteAware for BytesMut {
    #[inline(always)]
    fn write_varint(&mut self, value: u64) {
        self.reserve(10); //FIXME
        let buf = unsafe {
            slice::from_raw_parts_mut(self.as_mut_ptr().add(self.len()), 10)
        };
        let vi_len = encode_varint64(value, buf);
        unsafe {
            self.advance_mut(vi_len);
        }
    }

    #[inline(always)]
    fn write_varbytes(&mut self, value: &[u8]) {
        let len = value.len();
        self.reserve(10 + len); //FIXME
        self.write_varint(len as u64);
        // value.as_bytes().copy_to_slice()
        unsafe {
            copy_nonoverlapping(
                value.as_ptr(),
                self.as_mut_ptr().add(self.len()),
                len,
            );
            self.advance_mut(len);
        }
    }

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

        const EMPTY_BLK: [u8; 10] =
            [1u8, 0, 2, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0];
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

impl CHMsgReadAware for BytesMut {
    #[inline(always)]
    fn ensure_enough_bytes_to_read(
        &mut self,
        additional_space: usize,
    ) -> BaseRtResult<()> {
        if self.len() >= additional_space {
            Ok(())
        } else {
            Err(BaseRtError::IncompletedWireFormat)
        }
    }

    #[inline(always)]
    fn read_varint(&mut self) -> BaseRtResult<u64> {
        if self.len() == 0 {
            //FIXME unnecssary?
            return Err(BaseRtError::IncompletedWireFormat);
        }
        // most varints are in practice fit in 1 byte
        if self[0] < 0x80 {
            let r = self[0] as u64;
            self.advance(1);
            return Ok(r);
        } else {
            // handle case of two bytes too
            if self.len() <= 1 {
                //FIXME unnecssary?
                return Err(BaseRtError::IncompletedWireFormat);
            }
            if self[1] < 0x80 {
                let r = (self[0] & 0x7f) as u64 | (self[1] as u64) << 7;
                self.advance(2);
                return Ok(r);
            } else {
                return read_raw_varint64_slow_bytesmut(self);
            }
        }
    }

    #[inline(always)]
    fn read_str<'a, 'b>(&mut self) -> BaseRtResult<&'b str> {
        //FIXME one rust driver use arbitrary 128bit bytes as query_id?
        unsafe { Ok(std::str::from_utf8_unchecked(self.read_varbytes()?)) }
    }


    #[inline(always)]
    fn read_varbytes<'a, 'b>(&mut self) -> BaseRtResult<&'b [u8]> {
        let len = self.read_varint()? as usize;
        unsafe {
            let ptr = self.as_ptr();
            self.advance(len);
            Ok(slice::from_raw_parts(ptr, len))
        }
    }
}

impl CHMsgReadAware for &[u8] {
    #[inline(always)]
    fn ensure_enough_bytes_to_read(
        &mut self,
        additional_space: usize,
    ) -> BaseRtResult<()> {
        if self.len() < additional_space {
            Err(BaseRtError::IncompletedWireFormat)
        } else {
            Ok(())
        }
    }

    #[inline(always)]
    fn read_varint(&mut self) -> BaseRtResult<u64> {
        if self.len() == 0 {
            //FIXME unnecssary?
            return Err(BaseRtError::IncompletedWireFormat);
        }
        // most varints are in practice fit in 1 byte
        if self[0] < 0x80 {
            let r = self[0] as u64;
            self.advance(1);
            return Ok(r);
        } else {
            // handle case of two bytes too
            if self.len() <= 1 {
                //FIXME unnecssary?
                return Err(BaseRtError::IncompletedWireFormat);
            }
            if self[1] < 0x80 {
                let r = (self[0] & 0x7f) as u64 | (self[1] as u64) << 7;
                self.advance(2);
                return Ok(r);
            } else {
                return read_raw_varint64_slow(self);
            }
        }
    }

    #[inline(always)]
    fn read_str<'a, 'b>(&mut self) -> BaseRtResult<&'b str> {
        //FIXME one rust driver use arbitrary 128bit bytes as query_id?
        unsafe { Ok(std::str::from_utf8_unchecked(self.read_varbytes()?)) }
    }


    #[inline(always)]
    fn read_varbytes<'a, 'b>(&mut self) -> BaseRtResult<&'b [u8]> {
        let len = self.read_varint()? as usize;
        self.ensure_enough_bytes_to_read(len)?;
        unsafe {
            let ptr = self.as_ptr();
            self.advance(len);
            Ok(slice::from_raw_parts(ptr, len))
        }
    }
}

/// Encode u64 as varint.
/// Panics if buffer length is less than 10.
#[inline(always)]
pub fn encode_varint64(mut value: u64, buf: &mut [u8]) -> usize {
    fn iter(value: &mut u64, byte: &mut u8) -> bool {
        if (*value & !0x7F) > 0 {
            *byte = ((*value & 0x7F) | 0x80) as u8;
            *value >>= 7;
            true
        } else {
            *byte = *value as u8;
            false
        }
    }

    // Explicitly unroll loop to avoid either
    // unsafe code or bound checking when writing to `buf`

    if !iter(&mut value, &mut buf[0]) {
        return 1;
    };
    if !iter(&mut value, &mut buf[1]) {
        return 2;
    };
    if !iter(&mut value, &mut buf[2]) {
        return 3;
    };
    if !iter(&mut value, &mut buf[3]) {
        return 4;
    };
    if !iter(&mut value, &mut buf[4]) {
        return 5;
    };
    if !iter(&mut value, &mut buf[5]) {
        return 6;
    };
    if !iter(&mut value, &mut buf[6]) {
        return 7;
    };
    if !iter(&mut value, &mut buf[7]) {
        return 8;
    };
    if !iter(&mut value, &mut buf[8]) {
        return 9;
    };
    buf[9] = value as u8;
    10
}

/// Encode u32 value as varint.
/// Panics if buffer length is less than 5.
// #[inline(always)]
// pub fn encode_varint32(mut value: u32, buf: &mut [u8]) -> usize {
//     // assert!(buf.len() >= 5);
//
//     fn iter(value: &mut u32, byte: &mut u8) -> bool {
//         if (*value & !0x7F) > 0 {
//             *byte = ((*value & 0x7F) | 0x80) as u8;
//             *value >>= 7;
//             true
//         } else {
//             *byte = *value as u8;
//             false
//         }
//     }
//
//     // Explicitly unroll loop to avoid either
//     // unsafe code or bound checking when writing to `buf`
//
//     if !iter(&mut value, &mut buf[0]) {
//         return 1;
//     };
//     if !iter(&mut value, &mut buf[1]) {
//         return 2;
//     };
//     if !iter(&mut value, &mut buf[2]) {
//         return 3;
//     };
//     if !iter(&mut value, &mut buf[3]) {
//         return 4;
//     };
//     buf[4] = value as u8;
//     5
// }
#[inline(always)]
fn read_raw_varint64_slow_bytesmut(bs: &mut BytesMut) -> BaseRtResult<u64> {
    let mut r: u64 = 0;
    let mut i = 0;
    loop {
        if i == 10 {
            return Err(BaseRtError::InvalidWireFormatInVarInt(r));
        }
        let b = bs[i];
        // TODO: may overflow if i == 9
        r = r | (((b & 0x7f) as u64) << (i * 7));
        i += 1;
        if b < 0x80 {
            bs.advance(i);
            return Ok(r);
        }
    }
}

#[inline(always)]
fn read_raw_varint64_slow(bs: &mut &[u8]) -> BaseRtResult<u64> {
    let mut r: u64 = 0;
    let mut i = 0;
    loop {
        if i == 10 {
            return Err(BaseRtError::InvalidWireFormatInVarInt(r));
        }
        let b = bs[i];
        // TODO: may overflow if i == 9
        r = r | (((b & 0x7f) as u64) << (i * 7));
        i += 1;
        if b < 0x80 {
            bs.advance(i);
            return Ok(r);
        }
    }
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
