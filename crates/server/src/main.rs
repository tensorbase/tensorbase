#![feature(once_cell)]

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{env, future::Future};

use runtime::ch::protocol::ConnCtx;
use runtime::ch::{
    codecs::{BytesExt, CHMsgWriteAware},
    protocol::StageKind,
};
use runtime::errs::BaseRtError;
use runtime::{ch::messages::response_to, mgmt::BMS};

use actix_codec::{AsyncRead, AsyncWrite};
use actix_rt::net::TcpStream;
use actix_server::Server;
use actix_service::fn_service;
use baselog::{Config, ConfigBuilder, LevelFilter, TermLogger, TerminalMode};
use bytes::{Buf, BufMut, BytesMut};
use log::*;

struct BaseSrvConn {
    io: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
    conn_ctx: ConnCtx,
    // data_packets_processed: bool,
    // use_db: String,
}

// impl BaseSrv {
//     fn handle_request(&mut self, req: Request) {}
// }

const MAX_MSG_SIZE_BYTES: usize = 100 * 1024 * 1024; //TODO favor smaller

impl Future for BaseSrvConn {
    type Output = Result<(), BaseRtError>;

    //FIXME check timeout mech
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut nbytes_rb_last = 0usize;
        loop {
            // log::info!("to read...srvconn: {:p}, rb: {:p}, cctx: {:p}", this, &this.read_buf, &this.conn_ctx);
            if this.read_buf.len() > MAX_MSG_SIZE_BYTES {
                //FIXME do not retur P::R(Err)
                &mut this.read_buf.clear(); //drain all unread
                &mut this
                    .write_buf
                    .write_as_exception(BaseRtError::TooBigMessageSize);
                &mut this.write_buf.write_end_of_stream();
                return Poll::Ready(Err(BaseRtError::TooBigMessageSize));
            }
            this.read_buf
                .ensure_enough_bytes_to_write(this.read_buf.len());
            let read =
                Pin::new(&mut this.io).poll_read_buf(cx, &mut this.read_buf);
            match read {
                Poll::Pending => {
                    if !this.read_buf.is_empty() {
                        // this.write_buf.ensure_enough_bytes_to_write(32 * 1024);

                        //FIXME this case is ill, malicious clients can
                        //      use this to do DoS attacks
                        if nbytes_rb_last == this.read_buf.len() {
                            break; //FIXME DoS mitigation
                        }
                        nbytes_rb_last = this.read_buf.len();
                        match response_to(
                            &mut this.read_buf,
                            &mut this.write_buf,
                            &mut this.conn_ctx,
                        ) {
                            Ok(true) => break,
                            Ok(false) => continue,
                            Err(e) => {
                                log::debug!("Found error: {}", e);
                                match e {
                                    BaseRtError::IncompletedWireFormat => {
                                        continue;
                                    }
                                    e => {
                                        //FIXME now the server does not close the connection,
                                        //      but this is a DoS attack point
                                        //TODO  to add a exception count in ctx
                                        let cctx = &mut this.conn_ctx;
                                        cctx.stage = StageKind::Default;
                                        &mut this.read_buf.clear(); //drain all unread
                                        &mut this
                                            .write_buf
                                            .write_as_exception(e);
                                        &mut this
                                            .write_buf
                                            .write_end_of_stream(); //??? this eos will cause the official client to discon
                                    }
                                }
                                // return Poll::Ready(Err(e));
                            }
                        }
                    }
                    break;
                }
                Poll::Ready(Ok(n)) => {
                    if n == 0 {
                        return Poll::Ready(Ok(()));
                    } else {
                        log::debug!("{} bytes read", n);
                    }
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(BaseRtError::RequestReadError(
                        e.to_string(),
                    )));
                }
            }
        }
        //drain wb
        if !this.write_buf.is_empty() {
            match Pin::new(&mut this.io).poll_write(cx, &this.write_buf) {
                Poll::Ready(Ok(n)) => {
                    this.write_buf.advance(n);
                    if n == 0 {
                        return Poll::Ready(Ok(()));
                    }
                }
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(BaseRtError::ResponseWriteError(
                        e.to_string(),
                    )));
                }
            }
        }
        Poll::Pending
    }
}

#[actix_rt::main]
async fn main() -> io::Result<()> {
    let enable_dbg_log = match env::var("enable_dbg_log") {
        Ok(_v) => true,
        Err(_e) => false,
    };
    #[allow(unused_must_use)]
    {
        TermLogger::init(
            if enable_dbg_log {
                LevelFilter::Debug
            } else {
                LevelFilter::Info
            },
            ConfigBuilder::new()
                .add_filter_ignore_str("sled")
                .add_filter_ignore_str("cranelift")
                .build(),
            TerminalMode::Mixed,
        );
    }

    #[cfg(debug_assertions)]
    info!("[Base] built in debug mode");

    #[cfg(not(debug_assertions))]
    info!("[Base] built in release mode");

    let conf = &BMS.conf;
    let srv_addr = [
        conf.server.ip_addr.as_str(),
        conf.server.port.to_string().as_str(),
    ]
    .join(":");

    // start http server
    Server::build()
        .backlog(1024)
        .bind("base_srv", srv_addr, || {
            fn_service(|io: TcpStream| BaseSrvConn {
                io,
                read_buf: BytesMut::with_capacity(4096),
                write_buf: BytesMut::with_capacity(4096),
                conn_ctx: Default::default(),
                // data_packets_processed: false,
                // use_db: "default".to_string(),
            })
        })?
        .start()
        .await
}
