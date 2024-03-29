use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use runtime::ch::messages::response_to;
use runtime::ch::protocol::ConnCtx;
use runtime::ch::{
    codecs::{BytesExt, CHMsgWriteAware},
    protocol::StageKind,
};
use runtime::errs::BaseRtError;

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::io::poll_read_buf;

pub struct BaseSrvConn<S: AsyncRead + AsyncWrite + Unpin> {
    pub io: S,
    pub read_buf: BytesMut,
    pub write_buf: BytesMut,
    pub conn_ctx: ConnCtx,
}

const MAX_MSG_SIZE_BYTES: usize = 100 * 1024 * 1024; //TODO favor smaller

impl<S: AsyncRead + AsyncWrite + Unpin> Future for BaseSrvConn<S> {
    type Output = Result<(), BaseRtError>;

    //FIXME check timeout mech
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut nbytes_rb_last = 0usize;
        loop {
            // log::info!("to read...srvconn: {:p}, rb: {:p}, cctx: {:p}", this, &this.read_buf, &this.conn_ctx);
            if this.read_buf.len() > MAX_MSG_SIZE_BYTES {
                //FIXME do not retur P::R(Err)
                this.read_buf.clear(); //drain all unread
                this.write_buf
                    .write_as_exception(BaseRtError::TooBigMessageSize);
                this.write_buf.write_end_of_stream();
                return Poll::Ready(Err(BaseRtError::TooBigMessageSize));
            }
            this.read_buf
                .ensure_enough_bytes_to_write(this.read_buf.len());
            match poll_read_buf(Pin::new(&mut this.io), cx, &mut this.read_buf) {
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
                                        this.read_buf.clear(); //drain all unread
                                        this.write_buf.write_as_exception(e);
                                        this.write_buf.write_end_of_stream(); //??? this eos will cause the official client to discon
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
