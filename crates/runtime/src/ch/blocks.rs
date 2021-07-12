use std::{convert::TryFrom, slice};

use arrow::{array, datatypes::DataType, record_batch::RecordBatch};
use base::bytes_cat;
use bytes::{Buf, BufMut, BytesMut};
use clickhouse_rs_cityhash_sys::city_hash_128;
use lzzzz::lz4;

use meta::types::{BaseChunk, BqlType};

use crate::ch::codecs::{
    new_mut_slice_at, BytesDecoder, BytesEncoder, BytesExt, CHMsgReadAware,
    CHMsgWriteAware,
};
use crate::ch::protocol::{ServerCodes, LZ4_COMPRESSION_METHOD};
use crate::errs::{BaseRtError, BaseRtResult};

/**
 Clickhouse Server protocol

 Clickhouse uses for communication proprietary  protocol and bespoke binary message formats.
 The protocol is message-oriented request-response based and uses TCP (TLS) as transport layer.
 Official documentation doesn't have detailed information about it.
 This driver implementation is entirely based on source code  of others open-source clickhouse drivers
 and tcp dump analysis.


 ##  SERVER_DATA and CLIENT_DATA
 --------message----------
 PKID TMPF BLK or CBLK ( 1 or more)
   1   0
 | - | - | ----------- |

 PKID - Packet ID 1 byte
 TMPF - Temporary Table Name. (0-NULL)
 BLK  - Raw Block
 CBLK - Compressed Block

 --------raw block------------
  BI CN RN  COLS
 |--|--|--|------|

 BI   - Block Info
 CN   - Number of Columns (varint)
 RN   - Number of Rows (varint)
 COLS - Columns serialized data, data type specific

 --------block info---------
  ? OVF ? BUCK ?
 |-| - |-|----|-|

 OVF  - Overflow flag, 0/1
 BUCK - Bucket number, -1 - not specified (default)

BlockInfo:
 Set of pairs (`FIELD_NUM`, value) in binary form. Then 0.
 is_overflows:
      * After running GROUP BY ... WITH TOTALS with the max_rows_to_group_by and group_by_overflow_mode = 'any' settings,
      *  a row is inserted in the separate block with aggregated values that have not passed max_rows_to_group_by.
      * If it is such a block, then is_overflows is set to true for it.
bucket_num:
      * When using the two-level aggregation method, data with different key groups are scattered across different buckets.
      * In this case, the bucket number is indicated here. It is used to optimize the merge for distributed aggregation.
      * Otherwise -1.
 --------compressed block----
  CTH ALG CSZ DSZ LZ4 BLK
 |---| - |---|---|-------|

 CTH - 16 byte CityHash 1.0 Hashsum CityHash128(ALG---LZ4 BLK)
 ALG - 1 byte Algorithm of compression. 0x82 - LZ4
 CSZ - Compressed Block size ,  [ALG .. LZ4 BLK]
 DSZ - Decompressed Block size
 LZ4 BLK - LZ4 compressed Raw Block

 ## columns data
 Block contains series of columns each of them has header and body
  -------- block (BLK ) ---------------------
        1-column         2-column         next
 | BCH | BCB      |  BCH  | BCB      | ....
  BCH - Block column header
  BCB - Block column body

  --------- block column header (BCH) -------------
  | NM  |  TYPE  | CD |
  NM - table column name
  TYPE - table column type and metadata (i.e Enum type has it dictionary)
  CD - column data.

  Column data format is type dependant.
 ## Column values serialization
 Each data type has it own binary format

 (U)Int8(16|32|64) and Float32|Float64 data types encoded as the corresponding numeric type in little endian order.
 String data type  is encoded as sequence of  length + data, there length is varInt encoded
 FixedString(len) is encoded as sequence of  data with fixed length
 Date - 2 byte values. Little endian 16 bit signed integer - number of days from epoch (1 Jan 1970 )
 DateTime - 4 byte value. Little endian 32 bit signed integer - number of seconds from epoch
 DateTime64 - 8 byte value. Little endian 64 bit signed integer - number of ticks from epoch.
   The size in seconds of each tick is defined by scale factor of DateTime64 type metadata.
   Scale is 1 byte integer and can take values from 0 to 9.
   0 - each second is 1 tick
   1 - each second is 10 ticks
   n - each second is 10^n ticks
  UUID - 16 byte value
  IPv4 and IPv6 as corresponding 4 and 16 byte big-endian values (network endian)
  Enum8 and Enum16 encoded as array of 1 (or 2 for Enum16) integer values which are keys in
   enum dictionary

 Nullable(BaseType) has additional array of nullable flags. This array has 1-byte values of
  0 - (not null ) , 1 - null.  The array has the same length as BaseType data.
 Null array is stored in column format before Base type.

 block header:
 Initialize header from the index.
    for (const auto & column : index_block_it->columns)
    {
        auto type = DataTypeFactory::instance().get(column.type);
        header.insert(ColumnWithTypeAndName{ type, column.name });
    }
*/

const GLOBAL_DICTIONARY: u64 = 0x0100;
const ADDITIONAL_KEY: u64 = 0x0200;

pub struct Block {
    pub name: Vec<u8>,
    pub has_header_decoded: bool,
    pub overflow: bool,
    pub bucket: i32,
    pub ncols: usize,
    pub nrows: usize,
    pub columns: Vec<Column>,
}

impl Default for Block {
    fn default() -> Self {
        Block {
            name: vec![],
            has_header_decoded: false,
            overflow: false,
            bucket: -1,
            ncols: 0,
            nrows: 0,
            columns: vec![],
        }
    }
}

pub(crate) const EMPTY_CLIENT_BLK_BYTES: [u8; 12] = [
    0x02, 0x00, 0x01, 0x00, 0x02, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00,
];

pub(crate) const COMPRESSED_EMPTY_CLIENT_BLK_BYTES: [u8; 38] = [
    0x02, 0x00, 0xa7, 0x83, 0xac, 0x6c, 0xd5, 0x5c, 0x7a, 0x7c, 0xb5, 0xac, 0x46, 0xbd,
    0xdb, 0x86, 0xe2, 0x14, 0x82, 0x14, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00, 0x00, 0xa0,
    0x01, 0x00, 0x02, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00,
];

impl Block {
    #[inline]
    pub fn check_empty_blk_bytes_len(
        rb: &mut &[u8],
        is_compressed: bool,
    ) -> BaseRtResult<()> {
        if is_compressed {
            rb.ensure_enough_bytes_to_read(COMPRESSED_EMPTY_CLIENT_BLK_BYTES.len())
        } else {
            rb.ensure_enough_bytes_to_read(EMPTY_CLIENT_BLK_BYTES.len())
        }
    }

    #[inline(always)]
    pub fn reset(&mut self) {
        self.name = vec![];
        self.has_header_decoded = false;
        self.overflow = false;
        self.bucket = -1;
        self.ncols = 0;
        self.nrows = 0;
        self.columns = vec![];
    }

    #[inline(always)]
    pub fn get_name<'a>(&'a self) -> &'a str {
        unsafe { std::str::from_utf8_unchecked(&self.name) }
    }

    #[inline(always)]
    fn encode_body(&self, bs0: &mut BytesMut) -> BaseRtResult<()> {
        // log::info!("to encode body of block: {:?}", self);
        //BlockInfo:
        // Set of pairs (`FIELD_NUM`, value) in binary form. Then 0.
        bs0.write_varint(1);
        bs0.put_u8(self.overflow as u8);
        bs0.write_varint(2);
        bs0.extend_from_slice(&self.bucket.to_le_bytes());
        bs0.write_varint(0u64);
        //
        bs0.write_varint(self.ncols as u64);
        bs0.write_varint(self.nrows as u64);
        //
        for col in &self.columns {
            col.encode(bs0)?;
        }

        Ok(())
    }

    ///body is code-stripped blk
    ///FIXME strictly this method is not correct, but just nice now
    // pub fn is_empty_block_body(bs: &[u8]) -> bool {
    //     bs == &EMPTY_CLIENT_BLK_BYTES[2..]
    // }

    ///body is code-stripped blk
    ///FIXME strictly this method is not correct, but just nice now
    #[inline(always)]
    pub fn is_empty_block(bs: &[u8]) -> bool {
        if bs.len() == 12 {
            bs == EMPTY_CLIENT_BLK_BYTES
        } else {
            false
        }
    }

    ///body is code-stripped blk
    ///FIXME strictly this method is not correct, but just nice now
    // pub fn is_compressed_empty_block_body(bs: &[u8]) -> bool {
    //     bs == &COMPRESSED_EMPTY_CLIENT_BLK_BYTES[2..]
    // }

    ///body is code-stripped blk
    ///FIXME strictly this method is not correct, but just nice now
    #[inline(always)]
    pub fn is_compressed_empty_block(bs: &[u8]) -> bool {
        if bs.len() == 38 {
            bs == COMPRESSED_EMPTY_CLIENT_BLK_BYTES
        } else {
            false
        }
    }

    pub fn encode_to(
        &self,
        bs: &mut BytesMut,
        _bs: Option<&mut BytesMut>,
    ) -> BaseRtResult<()> {
        bs.write_varint(ServerCodes::Data as u64);
        bs.write_varbytes(&self.name);
        if let Some(bs0) = _bs {
            // --------compressed block----
            //  CTH ALG CSZ DSZ LZ4 BLK
            // |---| - |---|---|-------|
            //
            // CTH - 16 byte CityHash 1.0 Hashsum CityHash128(ALG---LZ4 BLK)
            // ALG - 1 byte Algorithm of compression. 0x82 - LZ4
            // CSZ - Compressed Block size ,  [ALG .. LZ4 BLK]
            // DSZ - Decompressed Block size
            // LZ4 BLK - LZ4 compressed Raw Block
            bs0.clear();
            self.encode_body(bs0)?;
            let skip_len = 16usize + 1 + 4 + 4;
            let raw_len = bs0.len();
            let max_clen = lz4::max_compressed_size(raw_len);
            bs.ensure_enough_bytes_to_write(skip_len + max_clen); //may re-alloc

            let ptr_header = bs.get_ptr_at_write_cursor();
            bs.advance_mut_unsafe(skip_len);

            let n = lz4::compress(
                &bs0[..raw_len],
                &mut bs.get_slice_mut_at_write_cursor(max_clen),
                lz4::ACC_LEVEL_DEFAULT,
            )
            .map_err(|_| BaseRtError::BlockCompressionError)?;
            bs.advance_mut_unsafe(n);
            unsafe {
                *(ptr_header.offset(16) as *mut u8) = LZ4_COMPRESSION_METHOD;

                let p_clen = ptr_header.offset(17) as *mut u32;
                *p_clen = (9 + n) as u32;
                let p_rlen = ptr_header.offset(21) as *mut u32;
                *p_rlen = raw_len as u32;
                let body =
                    new_mut_slice_at(&mut *(ptr_header.offset(16) as *mut u8), 9 + n);
                let cth = city_hash_128(body);
                let cth0 = ptr_header.offset(0) as *mut u64;
                *cth0 = cth.lo;
                let cth1 = ptr_header.offset(8) as *mut u64;
                *cth1 = cth.hi;
            }
            Ok(())
        } else {
            self.encode_body(bs)
        }
    }

    pub fn get_block_header(&self) -> Self {
        let mut blk: Block = Default::default();
        for col in &self.columns {
            blk.columns.push(new_block_header(
                col.name.clone(),
                col.data.btype,
                col.data.null_map.is_some(),
            ));
        }
        blk.ncols = blk.columns.len();
        blk.nrows = 0; //for empty data case
        blk
    }

    fn decode_header_from(&mut self, bs0: &mut &[u8]) -> BaseRtResult<()> {
        bs0.read_varint()?;
        bs0.ensure_enough_bytes_to_read(1)?;
        self.overflow = bs0.get_u8() != 0;
        bs0.read_varint()?;
        bs0.ensure_enough_bytes_to_read(4)?;
        self.bucket = bs0.get_i32_le();
        bs0.read_varint()?;
        //
        self.ncols = bs0.read_varint()? as usize;
        self.nrows = bs0.read_varint()? as usize;

        self.has_header_decoded = true;

        Ok(())
    }
    ///WARN Different to encode_to,
    ///     this metod just decodes the raw body without the msg protocol header
    pub fn decode_from(&mut self, bs: &mut &[u8]) -> BaseRtResult<usize> {
        let ptr0 = (*bs).as_ptr();
        if !self.has_header_decoded {
            let res = self.decode_header_from(bs);
            match res {
                Ok(_) => {}
                Err(BaseRtError::IncompletedWireFormat) => {
                    return Ok(0);
                }
                Err(e) => return Err(e),
            }
        }
        let mut cnt = unsafe { (*bs).as_ptr().offset_from(ptr0) };
        while !self.has_decoded() {
            let res = bs.decode_column(self.nrows);
            match res {
                Ok(col) => {
                    self.columns.push(col);
                    cnt = unsafe { (*bs).as_ptr().offset_from(ptr0) };
                }
                Err(BaseRtError::IncompletedWireFormat) => {
                    return Ok(cnt as usize);
                }
                Err(e) => return Err(e),
            }
        }

        Ok(cnt as usize)
        // if let Some(bs0) = _bs {
        //     //slice out bytesmut bs0 which has guaranteed to be enough for read
        //     let bs0: &mut &[u8] = unsafe {
        //         &mut std::slice::from_raw_parts(bs0.as_ptr(), bs0.len())
        //     };
        //     Block::decode_body(bs0)
        // } else {

        // }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.ncols == 0 && self.nrows == 0
    }

    #[inline(always)]
    pub fn has_decoded(&self) -> bool {
        self.has_header_decoded && (self.columns.len() == self.ncols)
    }
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("name", &String::from_utf8_lossy(&self.name))
            .field("overflow", &self.overflow)
            .field("bucket", &self.bucket)
            .field("ncols", &self.ncols)
            .field("nrows", &self.nrows)
            .field("columns", &self.columns)
            .finish()
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

impl TryFrom<RecordBatch> for Block {
    type Error = BaseRtError;

    fn try_from(res: RecordBatch) -> Result<Self, Self::Error> {
        let mut blk = Block::default();
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
                let arr = col
                    .as_any()
                    .downcast_ref::<array::LargeStringArray>()
                    .unwrap();
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

            blk.columns.push(Column {
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

pub struct Column {
    pub name: Vec<u8>,
    pub data: BaseChunk,
}

impl Column {
    #[inline]
    pub fn get_name<'a>(&'a self) -> &'a str {
        unsafe { std::str::from_utf8_unchecked(&self.name) }
    }
}

impl BytesEncoder for Column {
    fn encode(&self, bs: &mut BytesMut) -> BaseRtResult<()> {
        bs.write_varbytes(&self.name);
        if self.data.null_map.is_none() {
            bs.write_varbytes(&self.data.btype.to_vec()?);
        } else {
            let bs_nlltyp = &self.data.btype.to_vec()?;
            bs.write_varbytes(
                bytes_cat!(b"Nullable(", bs_nlltyp.as_slice(), b")").as_slice(),
            );
        }
        bs.extend_from_slice(&self.data.data);

        Ok(())
    }
}

fn decode_to_column(
    bs: &mut &[u8],
    bt: &[u8],
    name: Vec<u8>,
    nrows: usize,
    is_nullable: bool,
) -> BaseRtResult<Column> {
    let btype = BqlType::from_bytes(bt)?;
    //FIXME assert to not support Nullable(LowCardinality(String))? how about CH?
    let null_map = if is_nullable {
        bs.ensure_enough_bytes_to_read(nrows)?;
        let nm = bs[..nrows].to_vec();
        bs.advance(nrows);
        Some(nm)
    } else {
        None
    };
    if btype == BqlType::String {
        //gen offset_map for strings
        let mut os_map = vec![];
        let oss = bs.as_ptr();
        for i in 0..nrows {
            let os = unsafe { bs.as_ptr().offset_from(oss) } as u32;
            os_map.push(os);
            let slen = bs.read_varint()? as usize;
            bs.ensure_enough_bytes_to_read(slen)?;
            bs.advance(slen);
        }
        let bc_data = unsafe {
            let len = bs.as_ptr().offset_from(oss) as u32;
            os_map.push(len);
            slice::from_raw_parts(oss, len as usize).to_vec()
        };
        Ok(Column {
            name,
            data: BaseChunk {
                btype,
                size: nrows,
                data: bc_data,
                null_map,
                offset_map: Some(os_map),
                lc_dict_data: None,
            },
        })
    } else {
        let len_data = nrows * btype.size_in_usize()?;
        bs.ensure_enough_bytes_to_read(len_data)?;
        let bc_data = bs[..len_data].to_vec();
        bs.advance(len_data);
        Ok(Column {
            name,
            data: BaseChunk {
                btype,
                size: nrows,
                data: bc_data,
                null_map,
                offset_map: None,
                lc_dict_data: None,
            },
        })
    }
}

impl BytesDecoder<Column> for &[u8] {
    fn decode_column(&mut self, nrows: usize) -> BaseRtResult<Column> {
        let name = self.read_varbytes()?.to_vec();
        let btype0 = self.read_varbytes()?;
        match btype0 {
            b"LowCardinality(String)" => {
                //version number
                let ver = self.get_u64_le();
                if ver != 1 {
                    return Err(BaseRtError::UnsupportedLowCardinalityDictVersion);
                }
                //serialization type
                let v = self.get_u64_le();
                if (v & GLOBAL_DICTIONARY == GLOBAL_DICTIONARY)
                    || (v & ADDITIONAL_KEY == 0)
                {
                    return Err(BaseRtError::UnsupportedLowCardinalityDictVersion);
                }
                let btype = match v as u8 & 0x0F {
                    0 => BqlType::UInt(8),
                    1 => BqlType::UInt(16),
                    2 => BqlType::UInt(32),
                    3 => BqlType::UInt(64),
                    _ => return Err(BaseRtError::UnsupportedLowCardinalityDictVersion),
                };
                //number of strings in dict
                let ndict = self.get_u64_le();
                //consume whole dict,a.k.a. gen offset_map
                let mut os_map = vec![];
                let oss = self.as_ptr();
                for i in 0..ndict {
                    let os = unsafe { self.as_ptr().offset_from(oss) } as u32;
                    os_map.push(os);
                    let slen = self.read_varint()? as usize; //NOTE length of bql string in 0..127
                    self.advance(slen);
                }
                let lcd_data = unsafe {
                    let lcd_data_len = self.as_ptr().offset_from(oss) as u32;
                    slice::from_raw_parts(oss, lcd_data_len as usize).to_vec()
                };
                //number of rows in data
                let size = self.get_u64_le() as usize;
                if nrows != size {
                    return Err(BaseRtError::InvalidWireFormatInColumn);
                }
                let len_data = size * btype.size_in_usize()?;
                let data = self[..len_data].to_vec();
                self.advance(len_data);
                Ok(Column {
                    name,
                    data: BaseChunk {
                        btype,
                        size,
                        data,
                        null_map: None,
                        offset_map: Some(os_map),
                        lc_dict_data: Some(lcd_data),
                    },
                })
            }
            [b'N', b'u', b'l', b'l', b'a', b'b', b'l', b'e', b'(', bt @ .., b')'] => {
                decode_to_column(self, bt, name, nrows, true)
            }
            bt => decode_to_column(self, bt, name, nrows, false),
        }
    }
}

impl std::fmt::Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Column")
            .field("name", &String::from_utf8_lossy(&self.name))
            .field("data", &self.data)
            .finish()
    }
}

// block header:
// Initialize header from the index.
//    for (const auto & column : index_block_it->columns)
//    {
//        auto type = DataTypeFactory::instance().get(column.type);
//        header.insert(ColumnWithTypeAndName{ type, column.name });
//    }
///NOTE insert/select needs this kind info to send to client firstly
pub fn new_block_header(name: Vec<u8>, typ: BqlType, is_nullable: bool) -> Column {
    Column {
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

#[cfg(test)]
mod unit_tests {
    use std::cmp::max;

    use crate::ch::messages::process_data_blk;

    use super::*;
    use bytes::BytesMut;
    use meta::types::BqlType;

    #[test]
    fn test_block_encode() -> BaseRtResult<()> {
        let mut bs = BytesMut::with_capacity(4);
        let mut _bs = BytesMut::with_capacity(4);

        let mut blk: Block = Default::default();

        let col: Column = Column {
            name: b"col_1".to_vec(),
            data: BaseChunk {
                btype: BqlType::String,
                size: 1,
                data: b"\x06abcDEF".to_vec(),
                null_map: None,
                offset_map: None,
                lc_dict_data: None,
            },
        };
        blk.ncols = 1;
        blk.nrows = 1;
        blk.columns.push(col);
        // println!("block: {:?}", blk);

        blk.encode_to(&mut bs, Some(&mut _bs))?;
        // println!("encoded block: {:?}", &bs[..]);
        //FIXME assert

        let mut bs = BytesMut::with_capacity(4);
        let mut _bs = BytesMut::with_capacity(4);
        blk.get_block_header().encode_to(&mut bs, Some(&mut _bs))?;
        println!("encoded block_header: {:?}", &bs);
        //FIXME assert
        Ok(())
    }

    #[test]
    fn test_empty_block_encode() -> BaseRtResult<()> {
        //raw empty blk
        let empty_blk: Block = Default::default();
        let mut bs = BytesMut::with_capacity(4);
        empty_blk.encode_to(&mut bs, None)?;
        // println!("encoded block: {:?}", &bs[..]);
        assert_eq!(
            &bs[..],
            &[0x01, 0x00, 0x01, 0x00, 0x02, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00]
        );

        //compressed empty blk
        let compressed_empty_blk_bs = vec![
            0x01, 0x00, 0xa7, 0x83, 0xac, 0x6c, 0xd5, 0x5c, 0x7a, 0x7c, 0xb5, 0xac, 0x46,
            0xbd, 0xdb, 0x86, 0xe2, 0x14, 0x82, 0x14, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x00,
            0x00, 0xa0, 0x01, 0x00, 0x02, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00,
        ];
        bs.clear();
        // println!("compressed_empty_blk_bs.len: {}",compressed_empty_blk_bs.len());
        let mut _bs = BytesMut::with_capacity(4);
        empty_blk.encode_to(&mut bs, Some(&mut _bs))?;
        // println!("encoded block: {:02x?}", &bs[..]);
        assert_eq!(&bs[..], &compressed_empty_blk_bs);

        Ok(())
    }

    #[test]
    fn test_blockinfo_encode() -> BaseRtResult<()> {
        let mut blk: Block = Default::default();
        let headers = vec![
            new_block_header(b"a".to_vec(), BqlType::LowCardinalityString, false),
            new_block_header(b"b".to_vec(), BqlType::UInt(64), true),
        ];
        blk.ncols = headers.len();
        blk.nrows = 0; //for empty data case
        blk.columns.extend(headers);

        let mut bs = BytesMut::with_capacity(1);
        blk.encode_to(&mut bs, None)?;
        let expected_bs: Vec<u8> = vec![
            0x01, 0x00, 0x01, 0x00, 0x02, 0xff, 0xff, 0xff, 0xff, 0x00, 0x02, 0x00, 0x01,
            0x61, 0x16, 0x4c, 0x6f, 0x77, 0x43, 0x61, 0x72, 0x64, 0x69, 0x6e, 0x61, 0x6c,
            0x69, 0x74, 0x79, 0x28, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x29, 0x01, 0x62,
            0x10, 0x4e, 0x75, 0x6c, 0x6c, 0x61, 0x62, 0x6c, 0x65, 0x28, 0x55, 0x49, 0x6e,
            0x74, 0x36, 0x34, 0x29,
        ];
        // println!("{:?}", &bs[..]);
        assert_eq!(&bs[..], &expected_bs);

        let mut blk: Block = Default::default();
        let headers = vec![
            new_block_header(b"trip_id".to_vec(), BqlType::UInt(32), false),
            new_block_header(b"pickup_datetime".to_vec(), BqlType::DateTime, false),
        ];
        blk.ncols = headers.len();
        blk.nrows = 0; //for empty data case
        blk.columns.extend(headers);

        let mut bs = BytesMut::with_capacity(1);
        blk.encode_to(&mut bs, None)?;
        let expected_bs: Vec<u8> = vec![
            0x01, 0x00, 0x01, 0x00, 0x02, 0xff, 0xff, 0xff, 0xff, 0x00, 0x02, 0x00, 0x07,
            0x74, 0x72, 0x69, 0x70, 0x5f, 0x69, 0x64, 0x06, 0x55, 0x49, 0x6e, 0x74, 0x33,
            0x32, 0x0f, 0x70, 0x69, 0x63, 0x6b, 0x75, 0x70, 0x5f, 0x64, 0x61, 0x74, 0x65,
            0x74, 0x69, 0x6d, 0x65, 0x08, 0x44, 0x61, 0x74, 0x65, 0x54, 0x69, 0x6d, 0x65,
        ];
        // println!("{:?}", &bs[..]);
        assert_eq!(&bs[..], &expected_bs);

        Ok(())
    }

    #[test]
    fn test_block_decode() -> BaseRtResult<()> {
        let mut bs = BytesMut::with_capacity(4);
        // let mut _bs = BytesMut::with_capacity(4);

        let mut blk: Block = Default::default();

        let col: Column = Column {
            name: b"col_1".to_vec(),
            data: BaseChunk {
                btype: BqlType::String,
                size: 1,
                data: b"\x06abcDEF".to_vec(),
                null_map: None,
                offset_map: None,
                lc_dict_data: None,
            },
        };
        blk.ncols = 1;
        blk.nrows = 1;
        blk.columns.push(col);
        // println!("block: {:?}", blk);

        // println!("bs.as_ptr: {:p}", bs.as_ptr());
        blk.encode_to(&mut bs, None)?;
        // println!("encoded block: {:?}", &bs[..]);
        // println!("bs.as_ptr: {:p}", bs.as_ptr());

        bs.split_to(2); //skip msg protocol header
        let mut blk2 = Block::default();
        blk2.decode_from(&mut &bs[..])?;
        // println!("block2: {:?}", blk2);

        assert_eq!(blk.columns.len(), blk2.columns.len());
        assert_eq!(blk.columns[0].data.btype, blk2.columns[0].data.btype);
        assert_eq!(blk.columns[0].data.data, blk2.columns[0].data.data);

        Ok(())
    }

    #[test]
    fn test_block_decode_with_compression() -> BaseRtResult<()> {
        let mut bs = BytesMut::with_capacity(4);
        let mut _bs = BytesMut::with_capacity(4);

        let mut blk: Block = Default::default();

        let col: Column = Column {
            name: b"col_1".to_vec(),
            data: BaseChunk {
                btype: BqlType::String,
                size: 3,
                data: b"\x06abcDEF\x03!@#\x01*".to_vec(),
                null_map: None,
                offset_map: None,
                lc_dict_data: None,
            },
        };
        blk.ncols = 1;
        blk.nrows = col.data.size;
        blk.columns.push(col);
        // println!("block: {:?}", blk);

        // println!("bs.as_ptr: {:p}", bs.as_ptr());
        blk.encode_to(&mut bs, Some(&mut _bs))?;
        println!("encoded block: {:?}", &bs[..]);
        // println!("bs.as_ptr: {:p}", bs.as_ptr());

        bs.split_to(2); //skip msg protocol header
                        //decompress
        let nread = process_data_blk(&mut &bs[..], &mut _bs, true)?;
        assert!(nread > 0);
        println!(
            "process_data_blk: nread - {}, _bs.len: {}",
            nread,
            _bs.len()
        );

        let mut blk2 = Block::default();
        blk2.decode_from(&mut &_bs[..])?;
        println!("block2: {:?}", blk2);

        assert_eq!(blk.columns.len(), blk2.columns.len());
        assert_eq!(blk.columns[0].data.btype, blk2.columns[0].data.btype);
        assert_eq!(blk.columns[0].data.data, blk2.columns[0].data.data);
        assert_eq!(blk2.columns[0].data.null_map.is_none(), true);
        assert_eq!(blk2.columns[0].data.btype, BqlType::String);
        assert_eq!(blk2.columns[0].data.offset_map.is_some(), true);
        println!("{:?}", blk2.columns[0].data.offset_map.as_ref().unwrap());

        Ok(())
    }

    #[test]
    fn test_block_decode_with_compression_with_breaking_up() -> BaseRtResult<()> {
        let mut bs = BytesMut::with_capacity(4);
        let mut _bs = BytesMut::with_capacity(4);

        let mut blk: Block = Default::default();

        let col: Column = Column {
            name: b"col_1".to_vec(),
            data: BaseChunk {
                btype: BqlType::String,
                size: 3,
                data: b"\x06abcDEF\x03!@#\x01*".to_vec(),
                null_map: None,
                offset_map: None,
                lc_dict_data: None,
            },
        };
        blk.ncols = 1;
        blk.nrows = col.data.size;
        blk.columns.push(col);
        // println!("block: {:?}", blk);

        // println!("bs.as_ptr: {:p}", bs.as_ptr());
        blk.encode_to(&mut bs, Some(&mut _bs))?;
        println!("encoded block: {:?}", &bs[..]);
        // println!("bs.as_ptr: {:p}", bs.as_ptr());

        bs.split_to(2); //skip msg protocol header
                        //decompress
        let nread = process_data_blk(&mut &bs[..], &mut _bs, true)?;
        assert!(nread > 0);
        println!(
            "process_data_blk: nread - {}, _bs.len: {}",
            nread,
            _bs.len()
        );

        let mut blk2 = Block::default();
        assert_eq!(blk2.has_decoded(), false);

        let mut i = 0;
        while !blk2.has_decoded() {
            i += 4;
            i = max(_bs.len(), i);
            let _bsi = &mut &_bs[..i];
            // let ptr0 = (*_bsi).as_ptr();
            // let len0 = _bsi.len();
            let ndec = blk2.decode_from(_bsi)?;
            _bs.advance(ndec);
            if i < 10 {
                assert_eq!(ndec, 0);
            }
        }

        println!("block2: {:?}", blk2);

        assert_eq!(blk2.has_decoded(), true);
        assert_eq!(blk.columns.len(), blk2.columns.len());
        assert_eq!(blk.columns[0].data.btype, blk2.columns[0].data.btype);
        assert_eq!(blk.columns[0].data.data, blk2.columns[0].data.data);
        assert_eq!(blk2.columns[0].data.null_map.is_none(), true);
        assert_eq!(blk2.columns[0].data.btype, BqlType::String);
        assert_eq!(blk2.columns[0].data.offset_map.is_some(), true);
        println!("{:?}", blk2.columns[0].data.offset_map.as_ref().unwrap());

        Ok(())
    }
}
