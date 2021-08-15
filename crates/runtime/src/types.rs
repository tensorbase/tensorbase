use std::{convert::TryFrom, fmt::Debug, intrinsics::copy_nonoverlapping, slice};

use arrow::{array::LargeStringArray, datatypes::DataType, record_batch::RecordBatch};
use base::codec::encode_varint64;
use bytes::{Buf, BufMut, BytesMut};
use meta::types::{BaseChunk, BqlType};

use crate::errs::{BaseRtError, BaseRtResult};

pub struct BaseColumn {
    pub name: Vec<u8>,
    pub data: BaseChunk,
}

#[derive(Default)]
pub struct BaseDataBlock {
    pub ncols: usize,
    pub nrows: usize,
    pub columns: Vec<BaseColumn>,
}

impl Debug for BaseDataBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BaseDataBlock")
            .field("ncols", &self.ncols)
            .field("nrows", &self.nrows)
            .field("columns", &self.columns)
            .finish()
    }
}

impl BaseDataBlock {
    pub fn reset(&mut self) {
        self.ncols = 0;
        self.nrows = 0;
        self.columns = vec![];
    }
}

fn arrow_type_to_btype(typ: &DataType) -> BaseRtResult<BqlType> {
    log::debug!("arrow_type_to_btype: {}", typ);
    match typ {
        DataType::UInt8 => Ok(BqlType::UInt(8)),
        DataType::UInt16 => Ok(BqlType::UInt(16)),
        DataType::UInt32 => Ok(BqlType::UInt(32)),
        DataType::UInt64 => Ok(BqlType::UInt(64)),
        DataType::Int8 => Ok(BqlType::Int(8)),
        DataType::Int16 => Ok(BqlType::Int(16)),
        DataType::Int32 => Ok(BqlType::Int(32)),
        DataType::Int64 => Ok(BqlType::Int(64)),
        DataType::Float16 => Ok(BqlType::Float(16)),
        DataType::Float32 => Ok(BqlType::Float(32)),
        DataType::Float64 => Ok(BqlType::Float(64)),
        DataType::Timestamp32(None) => Ok(BqlType::DateTime),
        DataType::Timestamp32(Some(tz)) => Ok(BqlType::DateTimeTz(*tz)),
        DataType::Date16 => Ok(BqlType::Date),
        DataType::Decimal(p, s) => Ok(BqlType::Decimal(*p as u8, *s as u8)),
        DataType::LargeUtf8 => Ok(BqlType::String),
        DataType::FixedSizeBinary(len) => Ok(BqlType::FixedString(*len as u8)),
        _ => Err(BaseRtError::UnsupportedConversionToBqlType),
    }
}

impl TryFrom<RecordBatch> for BaseDataBlock {
    type Error = BaseRtError;

    fn try_from(res: RecordBatch) -> Result<Self, Self::Error> {
        let mut blk = BaseDataBlock::default();
        let sch = res.schema();
        let fields = sch.fields();
        let cols = res.columns();
        let ncols = cols.len();
        blk.ncols = ncols;
        for i in 0..ncols {
            let btype = arrow_type_to_btype(fields[i].data_type())?;
            let name = fields[i].name().as_bytes().to_vec();
            let col = &cols[i];
            let cd = col.data();
            // let array = col.as_any().downcast_ref::<array::Int64Array>().unwrap().values();
            let buf = if matches!(btype, BqlType::String) {
                &col.data().buffers()[1]
            } else {
                &col.data().buffers()[0]
            };
            // log::debug!("cd.get_array_memory_size(): {}", cd.get_array_memory_size());
            let (len_in_bytes, offsets) = if matches!(btype, BqlType::String) {
                let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
                let ofs = arr
                    .value_offsets()
                    .last()
                    .copied()
                    .ok_or(BaseRtError::FailToUnwrapOpt)?;

                (
                    ofs as usize,
                    Some(arr.value_offsets().iter().map(|o| *o as u32).collect()),
                )
            } else {
                (btype.size_in_usize()? * col.len(), None)
            };
            let data = unsafe {
                std::slice::from_raw_parts(buf.as_ptr(), len_in_bytes).to_vec()
            };
            blk.nrows = col.len(); //FIXME all rows are in same size

            blk.columns.push(BaseColumn {
                name,
                data: BaseChunk {
                    btype,
                    size: col.len(),
                    // data: Vec::from_raw_parts(qcs.data, qclen_bytes, qclen_bytes),
                    // data: Vec::<u8>::with_capacity(qclen_bytes),
                    data,
                    null_map: None,
                    offset_map: offsets,
                    // pub lc_dict_size: usize,
                    lc_dict_data: None,
                },
            });
        }
        Ok(blk)
    }
}

impl BaseColumn {
    // block header:
    // Initialize header from the index.
    //    for (const auto & column : index_block_it->columns)
    //    {
    //        auto type = DataTypeFactory::instance().get(column.type);
    //        header.insert(ColumnWithTypeAndName{ type, column.name });
    //    }
    ///NOTE insert/select needs this kind info to send to client firstly
    pub fn new_block_header(name: Vec<u8>, typ: BqlType, is_nullable: bool) -> Self {
        BaseColumn {
            name,
            data: BaseChunk {
                btype: typ,
                size: 0,
                data: vec![],
                null_map: if is_nullable { Some(vec![]) } else { None },
                offset_map: None,
                lc_dict_data: None,
            },
        }
    }

    #[inline]
    pub fn get_name<'a>(&'a self) -> &'a str {
        unsafe { std::str::from_utf8_unchecked(&self.name) }
    }
}

pub trait BaseServerConn {
    fn get_query_id(&self) -> &str;
    fn set_query_id(&mut self, query_id: String);
    fn get_db(&self) -> &str;
    fn set_db(&mut self, db: String);
    fn set_compressed(&mut self, is_compressed: bool);
    fn is_compressed(&self) -> bool;
}

pub trait BaseWriteAware {
    fn write_varint(&mut self, value: u64);
    fn write_varbytes(&mut self, value: &[u8]);
}

impl BaseWriteAware for BytesMut {
    #[inline(always)]
    fn write_varint(&mut self, value: u64) {
        self.reserve(10); //FIXME
        let buf =
            unsafe { slice::from_raw_parts_mut(self.as_mut_ptr().add(self.len()), 10) };
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
            copy_nonoverlapping(value.as_ptr(), self.as_mut_ptr().add(self.len()), len);
            self.advance_mut(len);
        }
    }
}

pub trait BaseReadAware {
    fn ensure_enough_bytes_to_read(
        &mut self,
        additional_space: usize,
    ) -> BaseRtResult<()>;
    fn read_varint(&mut self) -> BaseRtResult<u64>;
    fn read_str<'a, 'b>(&mut self) -> BaseRtResult<&'b str>;
    fn read_varbytes<'a, 'b>(&mut self) -> BaseRtResult<&'b [u8]>;
    // fn read_fix<T>(&mut self) -> T;
}

impl BaseReadAware for BytesMut {
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

impl BaseReadAware for &[u8] {
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
