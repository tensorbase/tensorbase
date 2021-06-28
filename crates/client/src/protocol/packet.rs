use super::code::*;
use super::encoder::Encoder;
use super::query::Query;
use super::ServerWriter;
use crate::client::ServerInfo;
use crate::pool::Options;
use crate::protocol::block::{EmptyBlock, ServerBlock};
use chrono_tz::Tz;
use std::{io, io::Write};
// OUT

pub(crate) trait Command: ServerWriter {}

pub(crate) struct Hello<'a> {
    pub(crate) opt: &'a Options,
}

impl Command for Hello<'_> {}

impl<'a> ServerWriter for Hello<'a> {
    fn write(&self, _cx: &ServerInfo, writer: &mut dyn Write) -> io::Result<()> {
        CLIENT_HELLO.encode(writer)?;

        crate::CLIENT_NAME.encode(writer)?;
        crate::CLICK_HOUSE_DBMSVERSION_MAJOR.encode(writer)?;
        crate::CLICK_HOUSE_DBMSVERSION_MINOR.encode(writer)?;
        crate::CLICK_HOUSE_REVISION.encode(writer)?;

        self.opt.database.encode(writer)?;
        self.opt.username.encode(writer)?;
        self.opt.password.encode(writer)?;
        Ok(())
    }
}

pub(crate) struct Ping;

impl Command for Ping {}

impl ServerWriter for Ping {
    fn write(&self, _cx: &ServerInfo, writer: &mut dyn Write) -> io::Result<()> {
        CLIENT_PING.encode(writer)
    }
}

pub(crate) struct Cancel;

impl Command for Cancel {}

impl ServerWriter for Cancel {
    fn write(&self, _cx: &ServerInfo, writer: &mut dyn Write) -> io::Result<()> {
        CLIENT_CANCEL.encode(writer)
    }
}

pub(crate) struct Execute {
    pub(crate) query: Query,
}

impl Command for Execute {}

impl ServerWriter for Execute {
    fn write(&self, cx: &ServerInfo, writer: &mut dyn Write) -> io::Result<()> {
        CLIENT_QUERY.encode(writer)?;
        //BaseFix follow to the official client
        let query_id = self
            .query
            .id
            .unwrap_or_default()
            .to_hyphenated()
            .to_string();
        query_id.encode(writer)?;
        // Query string (SELECT, INSERT or DDL )
        self.query.write(cx, writer)?;
        // Write empty block as a marker of the stream end
        EmptyBlock.write(cx, writer)?;
        Ok(())
    }
}

// IN

pub(crate) struct ProfileInfo {
    pub rows: u64,
    pub bytes: u64,
    pub blocks: u64,
    pub applied_limit: u8,
    pub rows_before_limit: u64,
    pub calculated_rows_before_limit: u8,
}

impl std::fmt::Debug for ProfileInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProfileInfo")
            .field("rows", &self.rows)
            .field("bytes", &self.bytes)
            .field("blocks", &self.blocks)
            .field("application limit", &self.applied_limit)
            .field("rows before limit", &self.rows_before_limit)
            .field(
                "calculated rows before limit",
                &self.calculated_rows_before_limit,
            )
            .finish()
    }
}

pub struct Statistics {
    rows: u64,
    bytes: u64,
    total: u64,
}

impl std::fmt::Debug for Statistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Statistics")
            .field("rows", &self.rows)
            .field("bytes", &self.bytes)
            .field("total", &self.total)
            .finish()
    }
}

#[derive(Debug)]
pub(crate) enum Response {
    Pong,
    Data(ServerBlock),
    Hello(String, u64, u64, u64, Tz),
    // Eos,
    // Profile(u64, u64, u64, u8, u8),
    // Totals,
    // Extremes,
}

impl Response {
    pub(crate) fn code(&self) -> u64 {
        match self {
            Response::Pong => SERVER_PONG,
            Response::Data(_) => SERVER_DATA,
            Response::Hello(..) => SERVER_HELLO,
            // Response::Eos => SERVER_END_OF_STREAM,
            // Response::Extremes => SERVER_EXTREMES,
            // Response::Totals => SERVER_TOTALS,
            // Response::Excepion(..) => SERVER_EXCEPTION,
        }
    }
}
