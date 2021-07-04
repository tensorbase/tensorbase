use crate::myc::constants::StatusFlags;
use crate::myc::io::WriteMysqlExt;
use crate::packet::PacketWriter;
use crate::{Column, ErrorKind};
use byteorder::{LittleEndian, WriteBytesExt};
use myc::constants::{CapabilityFlags, UTF8_GENERAL_CI};
use std::io::{self, Write};

pub(crate) fn write_eof_packet<W: Write>(
    w: &mut PacketWriter<W>,
    s: StatusFlags,
) -> io::Result<()> {
    w.write_all(&[0xFE, 0x00, 0x00])?;
    w.write_u16::<LittleEndian>(s.bits())?;
    w.end_packet()
}

pub(crate) fn write_ok_packet<W: Write>(
    w: &mut PacketWriter<W>,
    rows: u64,
    last_insert_id: u64,
    s: StatusFlags,
) -> io::Result<()> {
    w.write_u8(0x00)?; // OK packet type
    w.write_lenenc_int(rows)?;
    w.write_lenenc_int(last_insert_id)?;
    w.write_u16::<LittleEndian>(s.bits())?;
    w.write_all(&[0x00, 0x00])?; // no warnings
    w.end_packet()
}

pub(crate) fn write_auth_switch_packet<W: Write>(
    w: &mut PacketWriter<W>,
    auth_plugin: &[u8],
    nonce: &[u8],
) -> io::Result<()> {
    w.write_u8(0xfe)?;
    w.write_all(auth_plugin)?;
    w.write_u8(0x00)?;
    w.write_all(nonce)?;
    w.write_u8(0x00)?;
    w.end_packet()
}

pub(crate) fn write_handshake_packet<W: Write>(
    w: &mut PacketWriter<W>,
    connection_id: u32,
    auth_plugin: &[u8],
    nonce: &[u8],
) -> io::Result<()> {
    w.write_all(&[10])?; // protocol 10

    // 5.1.10 because that's what Ruby's ActiveRecord requires
    w.write_all(&b"5.1.10-alpha-msql-proxy\0"[..])?;

    let capabilities = CapabilityFlags::CLIENT_PROTOCOL_41
        | CapabilityFlags::CLIENT_PLUGIN_AUTH
        | CapabilityFlags::CLIENT_SECURE_CONNECTION
        | CapabilityFlags::CLIENT_CONNECT_WITH_DB;

    w.write_u32::<LittleEndian>(connection_id)?;
    w.write_all(&nonce[0..8])?;
    w.write_u8(0)?;
    w.write_u16::<LittleEndian>(capabilities.bits() as u16)?;
    w.write_u8(UTF8_GENERAL_CI as u8)?; // UTF8_GENERAL_CI
    w.write_u16::<LittleEndian>(0)?; // status flags
    w.write_u16::<LittleEndian>((capabilities.bits() >> 16) as u16)?; // extended capabilities
    w.write_u8(nonce.len() as u8 + 1)?; // scramble length
    w.write_all(&[0x00; 6][..])?; // filler
    w.write_all(&[0x00; 4][..])?; // filler
    w.write_all(&nonce[8..])?; // 4.1+ servers must extend salt
    w.write_u8(0)?;
    w.write_all(auth_plugin)?;
    w.write_u8(0)?;
    w.end_packet()
}

pub fn write_err<W: Write>(
    err: ErrorKind,
    msg: &[u8],
    w: &mut PacketWriter<W>,
) -> io::Result<()> {
    w.write_u8(0xFF)?;
    w.write_u16::<LittleEndian>(err as u16)?;
    w.write_u8(b'#')?;
    w.write_all(err.sqlstate())?;
    w.write_all(msg)?;
    w.end_packet()
}

use std::borrow::Borrow;

pub(crate) fn write_prepare_ok<'a, PI, CI, W>(
    id: u32,
    params: PI,
    columns: CI,
    w: &mut PacketWriter<W>,
) -> io::Result<()>
where
    PI: IntoIterator<Item = &'a Column>,
    CI: IntoIterator<Item = &'a Column>,
    <PI as IntoIterator>::IntoIter: ExactSizeIterator,
    <CI as IntoIterator>::IntoIter: ExactSizeIterator,
    W: Write,
{
    let pi = params.into_iter();
    let ci = columns.into_iter();

    // first, write out COM_STMT_PREPARE_OK
    w.write_u8(0x00)?;
    w.write_u32::<LittleEndian>(id)?;
    w.write_u16::<LittleEndian>(ci.len() as u16)?;
    w.write_u16::<LittleEndian>(pi.len() as u16)?;
    w.write_u8(0x00)?;
    w.write_u16::<LittleEndian>(0)?; // number of warnings
    w.end_packet()?;

    write_column_definitions(pi, w, true)?;
    write_column_definitions(ci, w, true)
}

pub(crate) fn write_column_definitions<'a, I, W>(
    i: I,
    w: &mut PacketWriter<W>,
    only_eof_on_nonempty: bool,
) -> io::Result<()>
where
    I: IntoIterator<Item = &'a Column>,
    W: Write,
{
    let mut empty = true;
    for c in i {
        let c = c.borrow();
        w.write_lenenc_str(b"def")?;
        w.write_lenenc_str(b"")?;
        w.write_lenenc_str(c.table.as_bytes())?;
        w.write_lenenc_str(b"")?;
        w.write_lenenc_str(c.column.as_bytes())?;
        w.write_lenenc_str(b"")?;
        w.write_lenenc_int(0xC)?;
        w.write_u16::<LittleEndian>(UTF8_GENERAL_CI)?;
        w.write_u32::<LittleEndian>(1024)?;
        w.write_u8(c.coltype as u8)?;
        w.write_u16::<LittleEndian>(c.colflags.bits())?;
        w.write_all(&[0x00])?; // decimals
        w.write_all(&[0x00, 0x00])?; // unused
        w.end_packet()?;
        empty = false;
    }

    if empty && only_eof_on_nonempty {
        Ok(())
    } else {
        write_eof_packet(w, StatusFlags::empty())
    }
}

pub(crate) fn column_definitions<'a, I, W>(
    i: I,
    w: &mut PacketWriter<W>,
) -> io::Result<()>
where
    I: IntoIterator<Item = &'a Column>,
    <I as IntoIterator>::IntoIter: ExactSizeIterator,
    W: Write,
{
    let i = i.into_iter();
    w.write_lenenc_int(i.len() as u64)?;
    w.end_packet()?;
    write_column_definitions(i, w, false)
}
