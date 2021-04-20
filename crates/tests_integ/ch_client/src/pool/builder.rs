use std::{
    convert::{TryFrom, TryInto},
    sync::atomic,
    sync::Arc,
};

use tokio::sync;

use crate::{
    client::InnerConection,
    errors::{Result, UrlError},
    sync::WakerSet,
};

use super::{CompressionMethod, Inner, Options, Pool, POOL_STATUS_SERVE};
use parking_lot::Mutex;

/// Connection pool builder
///
/// # Example
/// `
/// let pool = PoolBuilder::default()
///    .with_database("default")
///    .with_username("default")
///    .with_password("")
///    .add_addr("www.example.com:9000")
///    .build()
///    .unwrap();
/// `
pub struct PoolBuilder {
    addr: Vec<String>,
    username: Option<String>,
    password: Option<String>,
    database: Option<String>,
    pool_min: u16,
    pool_max: u16,
    compression: CompressionMethod,
    ping: bool,
}

impl TryFrom<PoolBuilder> for Options {
    type Error = UrlError;

    fn try_from(mut value: PoolBuilder) -> std::result::Result<Self, UrlError> {
        if value.addr.is_empty() {
            return Err(UrlError::Invalid);
        }

        let mut options = crate::DEF_OPTIONS.clone();
        std::mem::swap(&mut options.addr, &mut value.addr);
        options.compression = value.compression;
        options.ping_before_query = value.ping;
        options.username = value.username.replace(options.username).unwrap_or_default();
        options.password = value.password.replace(options.password).unwrap_or_default();
        options.database = value.database.replace(options.database).unwrap_or_default();

        options.pool_min = value.pool_min;
        options.pool_max = value.pool_max;
        Ok(options)
    }
}

impl PoolBuilder {
    /// Set compression option
    /// This make connection use LZ4 compression for block data transfer
    #[inline]
    pub fn with_compression(mut self) -> Self {
        self.compression = CompressionMethod::LZ4;
        self
    }
    /// If set, this option make connection check server availability after it
    /// is received from pool.
    #[inline]
    pub fn with_ping(mut self) -> Self {
        self.ping = true;
        self
    }
    /// Set connection pool boundaries
    /// min - set the number of idle connection that the pool can keep up to
    /// max - set maximum number of connection that pool can issued
    #[inline]
    pub fn with_pool(mut self, min: u16, max: u16) -> Self {
        self.pool_min = min;
        self.pool_max = max;
        self
    }

    /// Set the username that is used in authentication
    #[inline]
    pub fn with_username(mut self, value: impl ToString) -> Self {
        self.username = Some(value.to_string());
        self
    }
    /// Set the default database that is used in query processing if
    /// the query doesn't explicitly specify another database name
    #[inline]
    pub fn with_database(mut self, value: impl ToString) -> Self {
        self.database = Some(value.to_string());
        self
    }
    /// Set password that is used in authentication
    #[inline]
    pub fn with_password(mut self, value: impl ToString) -> Self {
        self.password = Some(value.to_string());
        self
    }

    /// Set server host address.
    ///
    /// Address must have domain name and port number
    /// # Example
    /// `
    /// PoolBuilder::new()
    ///   .with_addr('example1.com:9000')
    ///   .with_addr('example2.com:9000');
    /// `
    /// Connection pool can have multiple addresses
    /// In this case next connection randomly chooses any
    /// available one if it's reachable
    #[inline]
    pub fn add_addr(mut self, value: impl ToString) -> Self {
        self.addr.push(value.to_string());
        self
    }
    /// Convert the Builder into Pool using specified options.
    /// Note! Created Pool does not have connection.
    /// First connection will be created by executing pool.connection()
    ///
    #[inline]
    pub fn build(self) -> Result<Pool> {
        let options: Options = self.try_into()?;
        PoolBuilder::create(options)
    }
    /// Construct Pool from Option object
    pub(super) fn create(mut options: Options) -> Result<Pool> {
        if options.pool_max < options.pool_min {
            return Err(UrlError::InvalidPoolConstraints {
                min: options.pool_min,
                max: options.pool_max,
            }
            .into());
        }

        #[allow(unused_variables)]
        if cfg!(feature = "recycle") {
            let (tx, rx) = sync::mpsc::unbounded_channel::<Option<Box<InnerConection>>>();
        }

        let hosts = options.take_addr();

        let inner = Arc::new(Inner {
            new: crossbeam::queue::ArrayQueue::new(options.pool_min as usize),
            options,
            wakers: WakerSet::new(),
            lock: Mutex::new(0),
            connections_num: atomic::AtomicUsize::new(0),
            //wait: atomic::AtomicUsize::new(0),
            #[cfg(feature = "recycle")]
            recycler: Some(rx),
            hosts,
            close: POOL_STATUS_SERVE.into(),
        });

        let mut pool = Pool {
            inner,
            #[cfg(feature = "recycle")]
            drop: tx,
        };
        pool.inner.spawn_recycler();
        Ok(pool)
    }
}

impl Default for PoolBuilder {
    fn default() -> Self {
        PoolBuilder {
            addr: Vec::new(),
            username: None,
            password: None,
            database: None,
            pool_min: crate::DEF_OPTIONS.pool_min,
            pool_max: crate::DEF_OPTIONS.pool_max,
            ping: false,
            compression: CompressionMethod::None,
        }
    }
}
