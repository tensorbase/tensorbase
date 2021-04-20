use std::convert::TryInto;
use std::{
    fmt,
    sync::atomic::{self, Ordering},
    sync::Arc,
};

#[cfg(feature = "recycle")]
use tokio::sync;

pub use builder::PoolBuilder;
use crossbeam::queue;
pub use options::CompressionMethod;
pub use options::Options;
use parking_lot::Mutex;
use tokio::time::delay_for;
use util::*;

use crate::{
    client::{disconnect, Connection, InnerConection},
    errors::{Error, Result},
    sync::WakerSet,
};

use self::disconnect::DisconnectPool;
use crate::errors::DriverError;
use futures::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub mod builder;
mod disconnect;
pub mod options;

#[cfg(feature = "recycle")]
mod recycler;
mod util;

/// Normal operation status
const POOL_STATUS_SERVE: i8 = 0;
/// Pool in progress of shutting down.
/// New connection request will be rejected
const POOL_STATUS_STOPPING: i8 = 1;
/// Pool is stopped.
/// All  connections are freed
#[allow(dead_code)]
const POOL_STATUS_STOPPED: i8 = 2;

/// Pool implementation (inner sync structure)
pub(crate) struct Inner {
    new: queue::ArrayQueue<Box<InnerConection>>,
    /// The number of issued connections
    /// This value is in range of 0 .. options.max_pool
    lock: Mutex<usize>,
    /// Pool options
    pub(crate) options: Options,
    /// Used for notification of tasks which wait for available connection
    wakers: WakerSet,
    #[cfg(feature = "recycle")]
    recycler: Option<sync::mpsc::UnboundedReceiver<Option<Box<InnerConection>>>>,
    /// Server host addresses
    hosts: Vec<String>,
    /// The number of active connections that is taken by tasks
    connections_num: atomic::AtomicUsize,
    /// Number of tasks in waiting queue
    //wait: atomic::AtomicUsize,
    /// Pool status flag
    close: atomic::AtomicI8,
}

impl Inner {
    #[cfg(not(feature = "recycle"))]
    #[inline]
    fn spawn_recycler(self: &mut Arc<Inner>) {}

    #[cfg(feature = "recycle")]
    fn spawn_recycler(self: &mut Arc<Inner>) {
        use recycler::Recycler;

        let dropper = if let Some(inner) = Arc::get_mut(self) {
            inner.recycler.take()
        } else {
            None
        };

        //use ttl_check_inerval::TtlCheckInterval;
        if let Some(dropper) = dropper {
            // Spawn the Recycler.
            tokio::spawn(Recycler::new(Arc::clone(self), dropper));
        }
    }

    fn closed(&self) -> bool {
        self.close.load(atomic::Ordering::Relaxed) > POOL_STATUS_SERVE
    }

    /// Take back connection to pool if connection is not deterogated
    /// and pool is not full . Recycle in other case.
    pub(crate) fn return_connection(&self, conn: Box<InnerConection>) {
        // NOTE: It's  safe to call it out of tokio runtime
        let conn = if conn.is_ok()
            && conn.info.flag == 0
            && !self.closed()
            && self.new.len() < self.options.pool_min as usize
        {
            let lock = self.lock.lock();
            match self.new.push(conn) {
                Ok(_) => {
                    self.wakers.notify_one();
                    drop(lock);
                    return;
                }
                Err(econn) => econn.0,
            }
        } else {
            conn
        };
        {
            let mut lock = self.lock.lock();
            *lock -= 1;
            //self.issued.fetch_sub(1, Ordering::AcqRel);
            disconnect(conn);
            // NOTE! Release right before notify
            drop(lock);
        }
        self.wakers.notify_one();
    }
}

struct AwaitConnection<'a, F> {
    key: Option<usize>,
    wakers: &'a WakerSet,
    inner: &'a Inner,
    f: F,
}

impl<F: FnMut(usize, &Inner) -> bool> Future for AwaitConnection<'_, F> {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = unsafe { Pin::get_unchecked_mut(self) };
        let lock = me.inner.lock.lock();
        if let Some(key) = me.key.take() {
            if me.wakers.remove_if_notified(key, cx) {
                me.key = None;
                Poll::Ready((me.f)(*lock, me.inner))
            } else {
                Poll::Pending
            }
        } else if (me.f)(*lock, me.inner) {
            Poll::Ready(true)
        } else {
            let key = me.wakers.insert(cx);
            me.key = Some(key);
            Poll::Pending
        }
    }
}

/// Reference to a asynchronous  Clickhouse connections pool.
/// It can be cloned and shared between threads.
#[derive(Clone)]
pub struct Pool {
    pub(crate) inner: Arc<Inner>,
    #[cfg(feature = "recycle")]
    drop: sync::mpsc::UnboundedSender<Option<Box<InnerConection>>>,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let issued = unsafe { *self.inner.lock.data_ptr() };
        f.debug_struct("Pool")
            .field("inner.min", &self.inner.options.pool_min)
            .field("inner.max", &self.inner.options.pool_max)
            .field("queue len", &self.inner.new.len())
            .field("issued", &issued)
            .finish()
    }
}

impl Pool {
    /// Create pool object from Options object or url string.
    ///
    /// # Example
    /// ```
    /// use clickhouse_driver::prelude::*;
    /// let pool = Pool::create("tcp://username:password@localhost/db?compression=lz4");
    /// ```
    #[inline]
    pub fn create<T, E>(options: T) -> Result<Pool>
    where
        Error: From<E>,
        T: TryInto<Options, Error = E>,
    {
        let options = options.try_into()?;
        PoolBuilder::create(options)
    }

    /// Return pool current status.
    /// `idle` - number of idle connections in pool
    /// `issued` - total number of issued connections
    /// `wait` - number of tasks waiting for a connection
    #[inline(always)]
    pub fn info(&self) -> PoolInfo {
        let inner = &self.inner;

        util::PoolInfo {
            idle: inner.new.len(),
            issued: unsafe { *inner.lock.data_ptr() },
            wait: inner.wakers.len(),
        }
    }

    /// Return a connection from poll or create new if
    /// the poll doesn't have idle connections
    pub async fn connection(&self) -> Result<Connection> {
        let inner = &*self.inner;
        loop {
            let ok = AwaitConnection {
                key: None,
                wakers: &inner.wakers,
                inner: &inner,
                f: |issued: usize, inner: &Inner| {
                    inner.closed()
                        || !inner.new.is_empty()
                        || issued < inner.options.pool_max as usize
                },
            }
            .await;
            if !ok {
                continue;
            }

            if inner.closed() {
                return Err(DriverError::PoolDisconnected.into());
            }

            if let Ok(conn) = self.inner.new.pop() {
                let mut conn = Connection::new(self.clone(), conn);

                if inner.options.ping_before_query {
                    let mut c = inner.options.send_retries;
                    loop {
                        if conn.ping().await.is_ok() {
                            return Ok(conn);
                        }

                        if c <= 1 {
                            break;
                        }

                        delay_for(inner.options.retry_timeout).await;
                        c -= 1;
                    }

                    conn.set_deteriorated();
                    self.return_connection(conn);
                    continue;
                } else {
                    return Ok(conn);
                }
            } else {
                let mut lock = inner.lock.lock();

                if *lock < inner.options.pool_max as usize {
                    // reserve quota...
                    *lock += 1;
                    // ...and goto create new connection releasing lock for other clients
                    break;
                }
            };
        }
        // create new connection
        for addr in self.get_addr_iter() {
            match InnerConection::init(&inner.options, addr).await {
                Ok(conn) => {
                    return Ok(Connection::new(self.clone(), conn));
                }
                Err(err) => {
                    // On timeout repeat connection with another address
                    if !err.is_timeout() {
                        return Err(err);
                    }
                }
            }
        }

        // Release quota if failed to establish new connection
        let mut lock = inner.lock.lock();
        *lock -= 1;
        inner.wakers.notify_any();
        drop(lock);
        Err(crate::errors::DriverError::ConnectionClosed.into())
    }

    /// Take a connection back to pool
    #[inline]
    pub fn return_connection(&self, mut conn: Connection) {
        // NOTE: It's  safe to call it out of tokio runtime
        let pool = conn.pool.take();
        debug_assert_eq!(pool.as_ref().unwrap(), self);

        self.inner.return_connection(conn.take());
    }

    // #[cfg(not(feature = "recycle"))]
    // #[inline]
    // fn send_to_recycler(&self, conn: Box<InnerConection>) {
    //     disconnect(conn);
    // }
    //
    // #[cfg(feature = "recycle")]
    // #[inline]
    // fn send_to_recycler(&self, conn: Box<InnerConection>) {
    //     if let Err(conn) = self.drop.send(Some(conn)) {
    //         let conn = conn.0.unwrap();
    //         // This _probably_ means that the Runtime is shutting down, and that the Recycler was
    //         // dropped rather than allowed to exit cleanly.
    //         if self.inner.close.load(atomic::Ordering::SeqCst) != POOL_STATUS_STOPPED {
    //             // Yup, Recycler was forcibly dropped!
    //             // All we can do here is try the non-pool drop path for Conn.
    //             drop(conn);
    //         } else {
    //             unreachable!("Recycler exited while connections still exist");
    //         }
    //     }
    // }
    /// Close the pool and all issued connections
    pub fn disconnect(self) -> DisconnectPool {
        DisconnectPool::new(self)
    }
    /// Iterate over available host
    fn get_addr_iter(&'_ self) -> AddrIter<'_> {
        let inner = &self.inner;
        let index = if inner.hosts.len() > 1 {
            inner.connections_num.fetch_add(1, Ordering::Relaxed)
        } else {
            inner.connections_num.load(Ordering::Relaxed)
        };
        util::AddrIter::new(inner.hosts.as_slice(), index, self.options().send_retries)
    }
    /// Return Option object used for creation pool
    /// @note! the option does not have hosts
    #[inline]
    pub fn options(&self) -> &Options {
        &self.inner.options
    }
}

impl PartialEq for Pool {
    fn eq(&self, other: &Pool) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

#[cfg(test)]
mod test {
    use super::builder::PoolBuilder;
    use super::Result;

    #[tokio::test]
    async fn test_build_pool() -> Result<()> {
        let pool = PoolBuilder::default()
            .with_database("default")
            .with_username("default")
            .add_addr("www.yandex.ru:9000")
            .build()
            .unwrap();

        assert_eq!(pool.options().username, "default");
        assert_eq!(pool.inner.hosts[0], "www.yandex.ru:9000");

        Ok(())
    }
}
