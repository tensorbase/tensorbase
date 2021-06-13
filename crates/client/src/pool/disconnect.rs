use super::{Inner, Pool, POOL_STATUS_SERVE, POOL_STATUS_STOPPING};
use crate::errors::{DriverError, Result};
use std::future::Future;
use std::pin::Pin;
use std::sync::{atomic::Ordering, Arc};
use std::task::{Context, Poll};

/// DisconnectPool future
pub struct DisconnectPool {
    pool_inner: Arc<Inner>,
}

impl DisconnectPool {
    #[inline(always)]
    pub(super) fn new(pool: Pool) -> DisconnectPool {
        DisconnectPool {
            pool_inner: pool.inner,
        }
    }
}

impl Future for DisconnectPool {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut();

        let stop = this
            .pool_inner
            .close
            .compare_exchange_weak(
                POOL_STATUS_SERVE,
                POOL_STATUS_STOPPING,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_err();

        this.pool_inner.wakers.notify_all();

        // TODO: waiting for connections to close
        if stop {
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(DriverError::PoolDisconnected.into()))
        }
    }
}
