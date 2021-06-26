use bytes::{Buf, BufMut, BytesMut};
use lzzzz::lz4;
use std::str;

use crate::mgmt::{BaseCommandKind, BMS, WRITE};

use crate::ch::blocks::{Block, EMPTY_CLIENT_BLK_BYTES};
use crate::ch::codecs::{BytesExt, CHMsgReadAware, CHMsgWriteAware};
use crate::ch::protocol::{
    ClientCodes, ClientInfo, ConnCtx, Interface, QueryKind, ServerCodes,
};
use crate::errs::{BaseRtError, BaseRtResult};

use super::protocol::{StageKind, LZ4_COMPRESSION_METHOD};

const DBMS_NAME: &'static str = "TensorBase";
//FIXME to include from path
const DBMS_VERSION_MAJOR: u64 = 2021;
const DBMS_VERSION_MINOR: u64 = 5;
const REVISION: u64 = 54405; //54441?
const DBMS_VERSION_PATCH: u64 = 0;

/// main entrance for CH related logics
//FIXME BaseRtError: return directly with rb clearing
//      IncompletedWireFormat: retry
//      Ok: may consume partial rb
pub fn response_to(
    rb0: &mut BytesMut,
    wb: &mut BytesMut,
    cctx: &mut ConnCtx,
) -> BaseRtResult<bool> {
    //true: expect to write, false: expect to re-read
    let ptr0 = rb0.as_ptr();
    let len0 = rb0.len();
    let rb: &mut &[u8] = unsafe { &mut std::slice::from_raw_parts(ptr0, len0) };

    //FIXME assume rb0 has at least 1 byte to read?
    match cctx.stage {
        StageKind::DataEODP => {
            Block::check_empty_blk_bytes_len(rb, cctx.is_compressed)?;
            if is_eodp(rb, cctx.is_compressed) {
                // rb0.advance(rb.len());
                //EoS
                rb0.clear();
                wb.write_end_of_stream();
                cctx.stage = StageKind::Default;
                Ok(true)

                // consume_read_buf(rb0, rb, ptr0, len0)?;
                // Ok(false)
            } else {
                Err(BaseRtError::UnexpectedMessage)
            }
        }
        StageKind::DataEODPInsertQuery => {
            Block::check_empty_blk_bytes_len(rb, cctx.is_compressed)?;
            if is_eodp(rb, cctx.is_compressed) {
                rb.advance(rb.len());
                cctx.stage = StageKind::DataPacket;
                consume_read_buf(rb0, rb, ptr0, len0)?;
                Ok(false)
            } else {
                Err(BaseRtError::UnexpectedMessage)
            }
        }
        StageKind::DataPacket => {
            Block::check_empty_blk_bytes_len(rb, cctx.is_compressed)?;
            if is_eodp(rb, cctx.is_compressed) {
                // rb0.advance(rb.len()); //consume for rb
                log::debug!("all data packet received");
                //FIXME clear the asistant rb for next data??? data packet pipelining
                // rb0.clear();
                //EoS
                rb0.clear();
                wb.write_end_of_stream();
                cctx.stage = StageKind::Default;
                Ok(true)
                // Ok(false)
            } else {
                //NOTE now only insert into
                //handle data packets pipelining
                // --------message----------
                // PKID TMPF data buffer blk (STUPID: 1 or more, which belongs to 1 data packat)
                //   1   0
                // | - | - | ----------- |
                if cctx.is_compressed {
                    let intcode = rb.read_varint()? as u8;
                    log::debug!("to process a data packet: {}", intcode);
                    let _ = rb.ensure_enough_bytes_to_read(1)?;
                    let _tmp_tn = rb.read_str()?;
                }
                let rb1 = cctx
                    .raw_blk_req
                    .get_or_insert_with(|| BytesMut::with_capacity(4 * 1024));

                let nread = process_data_blk(rb, rb1, cctx.is_compressed)?;
                consume_read_buf(rb0, rb, ptr0, len0)?;

                // let rb0 = cctx.get_raw_blk_req();
                // log::debug!(
                //     "-- heading 16B of rb0 when decoding block: {:02x?}",
                //     &rb0[..16]
                // );
                let blk = cctx
                    .data_block_opt
                    .get_or_insert_with(|| Box::new(Block::default()));
                blk.reset();

                // log::debug!("rb0 addr: {:p}", rb0.as_ptr());
                // let bs = &mut rb1.get_slice_as_read_buf();
                // let ptr1 = rb1.as_ptr();
                // let len1 = rb1.len();
                // let bs: &mut &[u8] =
                //     unsafe { &mut std::slice::from_raw_parts(ptr1, len1) };
                let bs: &mut &[u8] = &mut rb1.get_slice_as_read_buf();
                let ndec = blk.decode_from(bs)?;
                rb1.advance(ndec);
                if !cctx.is_compressed {
                    rb.advance(ndec);
                    consume_read_buf(rb0, rb, ptr0, len0)?;
                }

                if blk.has_decoded() {
                    log::debug!("_ got block[{:p}]", &blk);
                    let write = WRITE.get().unwrap();
                    write(blk, &cctx.current_tab_ins, cctx.current_tid_ins)?;
                    // log::debug!("blk.columns[0].data.1..100: {:?}", b1);
                } else {
                    cctx.stage = StageKind::DataBlk;
                }
                // consume_read_buf(rb1, bs, ptr1, len1)?;
                Ok(false)
            }
        }
        StageKind::DataBlk => {
            let rb1 = cctx
                .raw_blk_req
                .as_mut()
                .ok_or(BaseRtError::FailToUnwrapOpt)?;
            let nread = process_data_blk(rb, rb1, cctx.is_compressed)?;
            consume_read_buf(rb0, rb, ptr0, len0)?;

            let blk = cctx
                .data_block_opt
                .as_mut()
                .ok_or(BaseRtError::FailToUnwrapOpt)?;
            let bs: &mut &[u8] = &mut rb1.get_slice_as_read_buf();
            let ndec = blk.decode_from(bs)?;
            rb1.advance(ndec);

            if !cctx.is_compressed {
                rb.advance(ndec);
                consume_read_buf(rb0, rb, ptr0, len0)?;
            }
            if blk.has_decoded() {
                log::debug!("got block[{:p}]", &blk);
                let write = WRITE.get().unwrap();
                write(blk, &cctx.current_tab_ins, cctx.current_tid_ins)?;
                cctx.stage = StageKind::DataPacket;
            }

            Ok(false)
        }
        _ => {
            let intcode = rb.read_varint()? as u8;
            let code = ClientCodes::from(intcode);
            // log::debug!("[{}]msg content: {:02x?}", cctx.query_id, &rb[..]);
            match code {
                ClientCodes::Hello => {
                    let _ = response_hello(rb, wb, cctx)?;
                    consume_read_buf(rb0, rb, ptr0, len0)?;
                    Ok(true)
                }
                ClientCodes::Ping => {
                    let _ = response_ping(rb, wb)?;
                    consume_read_buf(rb0, rb, ptr0, len0)?;
                    Ok(true)
                }
                ClientCodes::Query => {
                    response_query(rb, wb, cctx)?;
                    //if has trailing eodp, consume
                    match cctx.stage {
                        StageKind::DataEODPInsertQuery => {
                            consume_read_buf(rb0, rb, ptr0, len0)?;
                            Ok(false)
                        }
                        _ => {
                            rb0.clear();
                            wb.write_end_of_stream();
                            Ok(true)
                        }
                    }
                }
                _ => {
                    log::debug!("unsupported code: {:?}", code);
                    Ok(true)
                }
            }
        }
    }
}

fn consume_read_buf(
    rb0: &mut BytesMut,
    rb: &mut &[u8],
    ptr0: *const u8,
    len0: usize,
) -> BaseRtResult<()> {
    //consume rb0
    let cnt = unsafe { (*rb).as_ptr().offset_from(ptr0) };
    //if cnt throws err
    rb0.advance(cnt as usize);
    if len0 > cnt as usize {
        log::debug!(
            "only partially consume {} in all {} bytes this time",
            cnt,
            len0
        );
        // Ok(false) //has other kinds of packets, to drain
    }
    Ok(())
}

#[inline(always)]
fn is_eodp(rb: &mut &[u8], is_compressed: bool) -> bool {
    if is_compressed {
        Block::is_compressed_empty_block(rb)
    } else {
        Block::is_empty_block(rb)
    }
}

fn response_hello(
    rb: &mut &[u8],
    wb: &mut BytesMut,
    cctx: &mut ConnCtx,
) -> BaseRtResult<bool> {
    let client_name = rb.read_str()?;
    let client_ver_maj = rb.read_varint()?;
    let client_ver_min = rb.read_varint()?;
    let client_revision = rb.read_varint()?;
    let database = rb.read_str()?;
    let username = rb.read_str()?;
    let password = rb.read_str()?;
    log::debug!(
        "[{}]client_revision: {}, database: {}, username: {}, password: {}",
        cctx.query_id,
        client_revision,
        database,
        username,
        password
    );

    if client_revision < REVISION {
        return Err(BaseRtError::UnsupportedClientVersion);
    }

    //resp hello
    wb.write_varint(ServerCodes::Hello as u64);
    wb.write_str(DBMS_NAME);
    wb.write_varint(DBMS_VERSION_MAJOR);
    wb.write_varint(DBMS_VERSION_MINOR);
    wb.write_varint(REVISION);
    wb.write_str(&BMS.timezone_sys);
    wb.write_str(DBMS_NAME);
    wb.write_varint(DBMS_VERSION_PATCH);

    Ok(true)
}

fn response_ping(rb: &mut &[u8], wb: &mut BytesMut) -> BaseRtResult<bool> {
    //resp pong
    wb.write_varint(ServerCodes::Pong as u64);
    Ok(true)
}

fn response_query(
    rb: &mut &[u8],
    wb: &mut BytesMut,
    cctx: &mut ConnCtx,
) -> BaseRtResult<()> {
    //req query
    let query_id = rb.read_str()?.to_string();
    cctx.query_id = query_id;

    let ci = ClientInfo {
        query_kind: QueryKind::from(rb.read_varint()? as u8),
        initial_user: rb.read_str()?.to_string(),
        initial_query_id: rb.read_str()?.to_string(),
        initial_address_string: rb.read_str()?.to_string(),
        interface: Interface::from(rb.read_varint()? as u8),
        os_user: rb.read_str()?.to_string(),
        client_hostname: rb.read_str()?.to_string(),
        client_name: rb.read_str()?.to_string(),
        client_version_major: rb.read_varint()?,
        client_version_minor: rb.read_varint()?,
        client_tcp_protocol_version: rb.read_varint()?,
        quota_key: rb.read_str()?.to_string(),
        client_version_patch: rb.read_varint()?,
    };
    // log::debug!("ci: {:?}", ci);

    //CH
    // Per query settings are also passed via TCP.
    // We need to check them before applying due to they can violate the settings constraints.
    // client_tcp_protocol_version > 54429:
    // SettingsWriteFormat::STRINGS_WITH_FLAGS : SettingsWriteFormat::BINARY
    //FIXME only support STRINGS_WITH_FLAGS? but allow empty?
    //FIXME now just allow empty setting?
    // log::debug!("rb[..3]: {:?}", &rb[..3]);
    //FIXME silly workaround for jdbc
    if rb.len() > 2 && &rb[..3] != [2u8, 1, 38] {
        let setting_str = rb.read_str()?;
        if setting_str.len() != 0 {
            if setting_str != "format_csv_delimiter" {
                log::error!("setting_str: {}", setting_str);
                return Err(BaseRtError::UnsupportedFunctionality2(
                    "TensorBase does not support such settings",
                ));
            } else {
                //FIXME temp workaround for tpch
                let _ = rb.read_str()?;
                let _ = rb.read_str()?;
            }
        }
    }

    let stage = rb.read_varint()?;
    let compression = rb.read_varint()?;
    let query_str = rb.read_str()?.to_string();
    log::debug!(
        "[{}]stage: {}, compression: {}, query_str: {}",
        cctx.query_id,
        stage,
        compression,
        query_str
    );
    cctx.is_compressed = compression == 1;

    let res = BMS.run_commands(query_str, cctx);
    match res {
        Ok(BaseCommandKind::Query(blks)) => {
            for blk in blks {
                if compression == 1 {
                    let _bs = cctx.get_raw_blk_resp();
                    blk.get_block_header().encode_to(wb, Some(_bs))?;
                    blk.encode_to(wb, Some(_bs))?;
                } else {
                    blk.get_block_header().encode_to(wb, None)?;
                    blk.encode_to(wb, None)?;
                }
            }
            // cctx.stage = StageKind::DataEODP;
            Ok(())
        }
        Ok(
            BaseCommandKind::Create
            | BaseCommandKind::Drop
            | BaseCommandKind::Optimize,
        ) => Ok(()),
        Ok(BaseCommandKind::InsertFormatInlineValues(mut blk, qtn, tid)) => {
            let write = WRITE.get().unwrap();
            write(&mut blk, qtn.as_str(), tid)?;
            Ok(())
        }
        Ok(
            BaseCommandKind::InsertFormatInline(header, qtn, tid)
            | BaseCommandKind::InsertFormatCSV(header, qtn, tid),
        ) => {
            //NOTE for insert, server side need to send header for
            // Send block to the client - table structure.
            //sendData(state.io.out->getHeader());
            log::debug!(
                "[{}]header for insert into: {:?}",
                cctx.query_id,
                header
            );
            if compression == 1 {
                let _bs = cctx.get_raw_blk_resp();
                header.encode_to(wb, Some(_bs))?;
            } else {
                header.encode_to(wb, None)?;
            }
            cctx.current_tab_ins = qtn;
            cctx.current_tid_ins = tid;
            cctx.stage = StageKind::DataEODPInsertQuery;
            Ok(())
        }
        Ok(_) => Err(BaseRtError::UnsupportedFunctionality),
        Err(e) => {
            // log::info!("Return Err: {}", e);
            Err(e)
        }
    }
}

const MAX_BLOCK_SIZE_BYTES: usize = 4 * 1024 * 1024; //TODO favor smaller
pub(crate) fn process_data_blk(
    rb: &mut &[u8],
    rb1: &mut BytesMut,
    is_compressed: bool,
) -> BaseRtResult<usize> {
    //FIXME true: eodp, false: no-eodp dp
    // let compressed = cctx.is_compressed;
    // let stage = &cctx.stage;
    // let rb1 = cctx.get_raw_blk_req();
    if is_compressed {
        // log::debug!("{:2x?}", &rb);
        //decompress and move data to rb0
        // --------compressed block----
        //  CTH ALG CSZ DSZ LZ4 BLK
        // |---| - |---|---|-------|
        //
        // CTH - 16 byte CityHash 1.0 Hashsum CityHash128(ALG---LZ4 BLK)
        // ALG - 1 byte Algorithm of compression. 0x82 - LZ4
        // CSZ - Compressed Block size ,  [ALG .. LZ4 BLK]
        // DSZ - Decompressed Block size
        // LZ4 BLK - LZ4 compressed Raw Block
        let _ = rb.ensure_enough_bytes_to_read(16 + 1 + 8)?;
        rb.advance(16);
        let ccode = rb.get_u8();
        if ccode != LZ4_COMPRESSION_METHOD {
            return Err(BaseRtError::UnsupportedBlockCompressionFormat);
        }
        let comp_size = (rb.get_u32_le() - 9) as usize;
        let raw_size = rb.get_u32_le() as usize;
        if comp_size == 0 || comp_size > MAX_BLOCK_SIZE_BYTES
        // || raw_size as usize > MAX_BLOCK_SIZE_BYTES
        {
            return Err(BaseRtError::TooBigBlockSize);
        }
        // unsafe {
        // log::debug!(
        //     "++0 heading 16B of rb0[len={}] after ensure_enough: {:02x?}",
        //     rb0.len(),
        //     std::slice::from_raw_parts(rb0.as_ptr(), 16)
        // );
        // }
        // log::debug!("++0 rb0 ptr: {:p}, cap: {}", rb0.as_ptr(), rb0.capacity());
        rb1.ensure_enough_bytes_to_write(raw_size);
        // log::debug!("++1 rb0 ptr: {:p}, cap: {}", rb0.as_ptr(), rb0.capacity());
        // let dst_bs = unsafe {
        //     std::slice::from_raw_parts_mut(rb0.as_ptr() as *mut u8, raw_size)
        // };
        let dst_bs = rb1.get_slice_mut_at_write_cursor(raw_size);
        // unsafe {
        // log::debug!(
        //     "++1 heading 16B of rb0[len={}] after ensure_enough: {:02x?}",
        //     rb0.len(),
        //     std::slice::from_raw_parts(rb0.as_ptr(), 16)
        // );
        // }

        log::debug!("comp_size: {}, raw_size: {}", comp_size, raw_size);
        if comp_size > rb.len() {
            return Err(BaseRtError::IncompletedWireFormat);
        }

        lz4::decompress(&rb[..comp_size], dst_bs)
            .map_err(|_| BaseRtError::BlockDecompressionError)?;

        unsafe { rb1.advance_mut(raw_size) };
        // log::debug!(
        //     "== heading 16B of rb0[len={}] after decompression: {:02x?}",
        //     rb0.len(),
        //     &rb0[..16]
        // );

        rb.advance(comp_size); //consume for rb
        // if comp_size == 1028304 {
        //     log::debug!("rb[len={}]: {:?}", rb.len(), &rb);
        //     // unsafe {
        //     //     log::debug!(
        //     //         "==0 heading 16B of rb0[len={}] after decompression: {:02x?}",
        //     //         rb0.len(),
        //     //         std::slice::from_raw_parts(rb0.as_ptr(), 16)
        //     //     );
        //     // }
        // }
        Ok(raw_size)
    } else {
        let raw_size = rb.len() - EMPTY_CLIENT_BLK_BYTES.len();
        rb1.resize(raw_size, 0); // make sure the buffer is enough
        rb1.copy_from_slice(&rb[..raw_size]);
        Ok(raw_size)
    }
}

// fn process_raw_data(
//     rb_decomp: &BytesMut,
//     wb: &mut BytesMut,
// ) -> BaseRtResult<bool> {
//     log::info!("{:?}", rb_decomp.bytes());

//     Ok(true)
// }

//insert
//insert is done by query as well.
//insert is done in two steps at client side:
//  1. SendQuery("INSERT INTO " + table_name + " ( " + fields_section.str() + " ) VALUES");
// (wait a packet from server)  server_packet == ServerCodes::Data
//    exchange infos about tables
//  2. SendData(block); SendData(Block());

// case ServerCodes::Data: {
// if (!ReceiveData()) {
// throw std::runtime_error("can't read data packet from input stream");
// }
// return true;
// }
//
// case ServerCodes::Exception: {
// ReceiveException();
// return false;
// }
//
// case ServerCodes::ProfileInfo: {
// Profile profile;
//
// if (!WireFormat::ReadUInt64(&input_, &profile.rows)) {
// return false;
// }
// if (!WireFormat::ReadUInt64(&input_, &profile.blocks)) {
// return false;
// }
// if (!WireFormat::ReadUInt64(&input_, &profile.bytes)) {
// return false;
// }
// if (!WireFormat::ReadFixed(&input_, &profile.applied_limit)) {
// return false;
// }
// if (!WireFormat::ReadUInt64(&input_, &profile.rows_before_limit)) {
// return false;
// }
// if (!WireFormat::ReadFixed(&input_, &profile.calculated_rows_before_limit)) {
// return false;
// }
//
// if (events_) {
// events_->OnProfile(profile);
// }
//
// return true;
// }
//
// case ServerCodes::Progress: {
// Progress info;
//
// if (!WireFormat::ReadUInt64(&input_, &info.rows)) {
// return false;
// }
// if (!WireFormat::ReadUInt64(&input_, &info.bytes)) {
// return false;
// }
// if (REVISION >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS) {
// if (!WireFormat::ReadUInt64(&input_, &info.total_rows)) {
// return false;
// }
// }
//
// if (events_) {
// events_->OnProgress(info);
// }
//
// return true;
// }
//
// case ServerCodes::Pong: {
// return true;
// }
//
// case ServerCodes::EndOfStream: {
// if (events_) {
// events_->OnFinish();
// }
// return false;
// }

// === tests ===
#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::ch::codecs::CHMsgWriteAware;
    use base::show_option_size;
    use bytes::{Buf, BytesMut};
    use meta::types::BqlType;
    use std::time::Instant;
    use zerocopy::AsBytes;

    #[test]
    fn test_to_codes() -> anyhow::Result<()> {
        assert!(matches!(ClientCodes::from(0), ClientCodes::Hello));
        assert!(matches!(ClientCodes::from(1), ClientCodes::Query));
        assert!(matches!(ClientCodes::from(2), ClientCodes::Data));
        assert!(matches!(ClientCodes::from(4), ClientCodes::Ping));
        assert!(matches!(ServerCodes::from(0), ServerCodes::Hello));
        assert!(matches!(ServerCodes::from(1), ServerCodes::Data));
        assert!(matches!(ServerCodes::from(2), ServerCodes::Exception));
        assert!(matches!(ServerCodes::from(4), ServerCodes::Pong));
        assert!(matches!(ServerCodes::from(5), ServerCodes::EndOfStream));
        assert_eq!(ServerCodes::Hello as u8, 0);
        assert_eq!(ServerCodes::Data as u8, 1);
        assert_eq!(ServerCodes::Exception as u8, 2);
        assert_eq!(ServerCodes::Pong as u8, 4);
        assert_eq!(ServerCodes::EndOfStream as u8, 5);
        Ok(())
    }

    #[test]
    fn test_varints_sanitycheck() -> anyhow::Result<()> {
        let mut b = BytesMut::with_capacity(10);
        // b.put_u64(123456789);
        // println!("buf.capacity(): {}", b.capacity());
        // println!("buf.remaining(): {}", b.remaining());
        let v = 1234567u64;
        b.write_varint(v);
        let vi = b.read_varint()?;
        assert_eq!(vi, v);
        assert_eq!(b.len(), 0);
        println!("b.len: {}, varint from b: {}", b.len(), v);

        let mut b = BytesMut::with_capacity(10);
        let v = 0u64;
        b.write_varint(v);
        let vi = b.read_varint()?;
        assert_eq!(vi, v);
        assert_eq!(b.len(), 0);
        println!("b.len: {}, varint from b: {}", b.len(), v);

        let mut b = BytesMut::with_capacity(20);
        let s = "TensorBase Server";
        // let sl = s.len();
        b.write_str(s);
        let ss = b.read_str()?;
        assert_eq!(ss, s);
        assert_eq!(b.len(), 0);
        println!("str read from b: {}", ss);

        let mut b = BytesMut::with_capacity(20);
        let s = "";
        // let sl = s.len();
        b.write_str(s);
        let ss = b.read_str()?;
        assert_eq!(ss, s);
        assert_eq!(b.len(), 0);
        println!("str read from b: {}", ss);

        let mut b = BytesMut::with_capacity(20);
        let s = "127";
        b.write_varint(127);
        b.write_str(s);
        assert_eq!(b.len(), 5);
        assert_eq!(b.as_bytes(), b"\x7f\x03127");
        println!("b: {:?}", b);
        let v = b.read_varint()?;
        let ss = b.read_str()?;
        assert_eq!(v, 127);
        assert_eq!(ss.len(), 3);
        assert_eq!(b.len(), 0);
        println!("str read from b: {}", ss);
        Ok(())
    }

    #[test]
    #[ignore]
    fn test_varints_stress() -> anyhow::Result<()> {
        for v in 0..10_000_000 {
            let mut b = BytesMut::with_capacity(10);
            b.write_varint(v);
            let vi = b.read_varint()?;
            assert_eq!(vi, v);
            assert_eq!(b.len(), 0);
        }

        let mut b = BytesMut::with_capacity(10);
        for v in 0..10_000_000 {
            // println!("b.capacity(): {}",b.capacity());
            b.reserve(10);
            b.write_varint(v);
            let vi = b.read_varint()?;
            assert_eq!(vi, v);
            assert_eq!(b.len(), 0);
        }

        let mut b = BytesMut::with_capacity(10);
        for v in 0u32..10_000_000 {
            // println!("b.capacity(): {}",b.capacity());
            let sv = v.to_string();
            b.reserve(10 + sv.len());
            b.write_str(&sv);
            let vs = b.read_str()?;
            assert_eq!(vs, sv);
            assert_eq!(b.len(), 0);
        }
        Ok(())
    }

    #[test]
    #[ignore]
    fn microbench_varints() -> anyhow::Result<()> {
        let mut b = BytesMut::with_capacity(1024);
        let t = Instant::now();
        for v in 0..100_000_000 {
            // b.ensure_enough_space(10 * 1024 * 1024);
            // b.reserve(10);
            b.write_varint(v);
            let vi = b.read_varint()?;
            assert_eq!(vi, v);
            // if b.len() == 1 {
            //     println!("{}", v);
            // }
            assert_eq!(b.len(), 0);
        }
        println!(
            "b.len: {}, time cost: {} millsecs",
            b.len(),
            t.elapsed().as_millis()
        );
        Ok(())
    }

    #[test]
    fn test_write_bytes() {
        let mut b = BytesMut::with_capacity(64);
        let value = vec![];
        b.write_varbytes(&value);
        assert_eq!(&b, &[0u8][..]);
        // println!("{:?}", b);
    }

    #[test]
    fn size_check() {
        show_option_size!(header);
        show_option_size!(u32);
        show_option_size!(BqlType);
        show_option_size!(Block);
        show_option_size!(StageKind);
        show_option_size!(ConnCtx);
        show_option_size!(BaseCommandKind);
        show_option_size!(Option<u32>);
        show_option_size!(Option<Option<u32>>);
        show_option_size!(Option<ConnCtx>);
        show_option_size!(Box<u32>);
        show_option_size!(BytesMut);
        show_option_size!(BaseRtError);
    }
}
