use std::{
    future::Future,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
};

use futures::stream::Stream;
use tokio::sync::mpsc;

use super::{Inner, InnerConection, POOL_STATUS_STOPPED};

pub(crate) struct Recycler {
    dropped: mpsc::UnboundedReceiver<Option<Box<InnerConection>>>,
    inner: Arc<Inner>,
    eof: bool,
}

impl Recycler {
    pub(crate) fn new(
        inner: Arc<Inner>,
        dropped: mpsc::UnboundedReceiver<Option<Box<InnerConection>>>,
    ) -> Recycler {
        Recycler {
            inner,
            dropped,
            eof: false,
        }
    }
}

impl Future for Recycler {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        //let mut close = self.inner.close.load(Ordering::Acquire);
        loop {
            match Pin::new(&mut self.dropped).poll_next(cx) {
                Poll::Ready(Some(Some(_conn))) => {}
                Poll::Ready(Some(None)) => {
                    break;
                }
                Poll::Ready(None) => {
                    // no more connections are coming -- time to exit!
                    self.inner
                        .close
                        .store(POOL_STATUS_STOPPED, Ordering::Release);
                    self.eof = true;
                }
                Poll::Pending => {
                    if self.eof {
                        let wait = self.inner.wait.load(Ordering::Acquire);
                        if wait == 0 {
                            break;
                        }
                        //awake waiting pool task and repeat
                        self.inner.notifyer.notify();
                    }

                    return Poll::Pending;
                }
            }
        }
        Poll::Ready(())
    }
}
