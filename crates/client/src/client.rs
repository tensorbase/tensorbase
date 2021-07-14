use std::fmt;
use std::io;
use std::time::Duration;

use crate::protocol::block::{Block, ServerBlock};
use crate::protocol::command::{CommandSink, ResponseStream};
use crate::protocol::insert::InsertSink;
use crate::protocol::packet::Response;
use crate::protocol::packet::{Execute, Hello, Ping};
use crate::protocol::query::Query;
use crate::protocol::ServerWriter;
use crate::{
    errors::{DriverError, Result},
    pool::{
        options::{CompressionMethod, Options},
        Pool,
    },
};
use chrono_tz::Tz;
use futures::Future;
use log::{debug, info, warn};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

const DEF_OUT_BUF_SIZE: usize = 512;
const DEFAULT_QUERY_BUF_SIZE: usize = 8 * 1024;
/// Connection state flags
const FLAG_DETERIORATED: u8 = 0x01;
const FLAG_PENDING: u8 = 0x02;

pub trait AsyncReadWrite: AsyncWrite + AsyncRead + Send + Unpin {}
impl<T: AsyncWrite + AsyncRead + Send + Unpin> AsyncReadWrite for T {}

#[derive(Debug)]
pub(super) struct ServerInfo {
    pub(crate) revision: u32,
    pub(crate) readonly: u8,
    pub(crate) flag: u8,
    pub(crate) compression: CompressionMethod,
    pub(crate) timezone: Tz,
}

impl ServerInfo {
    #[inline]
    pub(crate) fn set_deteriorated(&mut self) {
        self.flag |= FLAG_DETERIORATED;
    }
    #[inline]
    pub(crate) fn set_pending(&mut self) {
        self.flag |= FLAG_PENDING;
    }
    #[inline]
    pub(crate) fn clear_pending(&mut self) {
        self.flag &= !FLAG_PENDING;
    }

    #[inline]
    pub(crate) fn is_pending(&self) -> bool {
        (self.flag & FLAG_PENDING) == FLAG_PENDING
    }
}
/// Implement established connection (active or idle)
pub(super) struct Inner {
    pub(crate) socket: Option<TcpStream>,
    pub(crate) info: ServerInfo,
}

pub(super) type InnerConnection = Inner;

pub struct QueryResult<'a, R: AsyncRead> {
    pub(crate) inner: ResponseStream<'a, R>,
}

impl<'a, R: AsyncWrite + AsyncRead + Unpin + Send> QueryResult<'a, R> {
    pub fn cancel(&'a mut self) -> impl Future<Output = Result<()>> + 'a {
        self.inner.cancel()
    }
}

impl<'a, R: AsyncRead + Unpin + Send> QueryResult<'a, R> {
    pub async fn next(&mut self) -> Result<Option<ServerBlock>> {
        while let Some(packet) = self.inner.next().await? {
            if let Response::Data(block) = packet {
                return Ok(Some(block));
            } else {
                //println!("packet {:?}", packet);
            }
        }
        Ok(None)
    }

    #[inline]
    pub fn is_pending(&self) -> bool {
        self.inner.is_pending()
    }
}

impl<'a, R: AsyncRead> Drop for QueryResult<'a, R> {
    fn drop(&mut self) {}
}

impl Default for Inner {
    fn default() -> Inner {
        Inner {
            socket: None,
            info: ServerInfo {
                flag: 0,
                revision: crate::CLICK_HOUSE_REVISION as u32,
                timezone: chrono_tz::UTC,
                compression: CompressionMethod::None,
                readonly: 0,
            },
        }
    }
}

impl ServerContext for Inner {
    #[inline]
    fn info(&self) -> &ServerInfo {
        &self.info
    }
}

impl Inner {
    #[inline]
    /// Return true if the connection is not initialized yet
    fn is_null(&self) -> bool {
        self.socket.is_none()
    }
    /// Check if the socket is connected to a peer
    pub(super) fn is_ok(&self) -> bool {
        self.socket
            .as_ref()
            .map_or(false, |socket| socket.peer_addr().is_ok())
    }

    /// Split self into Stream and ServerInfo
    #[inline]
    pub(super) fn split(
        &mut self,
    ) -> Option<(&mut (dyn AsyncReadWrite + '_), &mut ServerInfo)> {
        let info = &mut self.info as *mut ServerInfo;
        // SAFETY: This can be risky if caller use returned values inside Connection
        // or InnerConnection methods. Never do it.
        match self.socket {
            None => None,
            Some(ref mut socket) => unsafe { Some((socket, &mut *info)) },
        }
    }

    /// Establish a connection to a Clickhouse server
    pub(super) async fn init(options: &Options, addr: &str) -> Result<Box<Inner>> {
        let socket = TcpStream::connect(addr).await?;
        Inner::setup_stream(&socket, &options)?;
        info!(
            "connection  established to: {}",
            socket.peer_addr().unwrap()
        );
        // TODO: Tls handshake for secure connection like
        // https://github.com/tokio-rs/tls/blob/master/tokio-native-tls/examples/download-rust-lang.rs
        let conn = Inner::handshake(socket, &options).await?;
        Ok(conn)
    }
    /// Negotiate connection parameters such as timezone, revision, compression
    async fn handshake(mut socket: TcpStream, options: &Options) -> Result<Box<Self>> {
        let mut inner: Box<Inner> = Box::new(Default::default());
        {
            let mut buf = Vec::with_capacity(256);
            {
                Hello { opt: options }.write(&inner.info, &mut buf)?;
            }

            socket.write_all(&buf[..]).await?;
            let stream: &mut dyn AsyncReadWrite = &mut socket;
            drop(buf);
            let mut stream = ResponseStream::with_capacity(
                256,
                stream,
                &mut inner.info,
                options.connection_timeout,
            );

            let (revision, timezone) = match stream.next().await? {
                Some(Response::Hello(_name, _major, _minor, revision, tz)) => {
                    (revision as u32, tz)
                }
                _ => {
                    socket.shutdown().await?;
                    return Err(DriverError::ConnectionTimeout.into());
                }
            };
            drop(stream);
            inner.info.compression = options.compression;
            inner.info.revision = revision;
            inner.info.timezone = timezone;
            inner.info.readonly = options.readonly;
        }
        debug!("handshake complete");
        inner.socket = Some(socket);
        Ok(inner)
    }

    #[inline]
    fn setup_stream(socket: &TcpStream, _options: &Options) -> io::Result<()> {
        // TODO: wait for the support in the std
        // https://github.com/rust-lang/rust/issues/69774
        // https://github.com/tokio-rs/tokio/pull/3146
        // https://github.com/async-rs/async-std/issues/718
        // socket.set_keepalive(options.keepalive)?;
        socket.set_nodelay(true)
    }

    async fn cleanup(&mut self) -> Result<()> {
        // TODO: ensure cancel command indeed interrupt long-running process
        if (self.info.flag & FLAG_PENDING) == FLAG_PENDING {
            //TODO: simplify. There is no reason to call `split` method
            let wrt = if let Some((wrt, _info)) = self.split() {
                wrt
            } else {
                return Err(DriverError::ConnectionClosed.into());
            };
            debug!("cleanup connection");

            CommandSink::new(wrt).cancel().await?;
            info!("sent cancel message");
        };

        Ok(())
    }
}

pub(crate) trait ServerContext {
    fn info(&self) -> &ServerInfo;
}

/// Represent Clickhouse client active connection
pub struct Connection {
    inner: Box<Inner>,
    pub(crate) pool: Option<Pool>,
    out: Vec<u8>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let peer = self.inner.socket.as_ref().map_or("-".to_string(), |s| {
            s.peer_addr().map_or("-".to_string(), |p| format!("{}", p))
        });
        f.debug_struct("Connection")
            .field("inner.pear", &peer)
            .finish()
    }
}

macro_rules! check_pending {
    ($this:ident) => {
        if $this.inner.info.is_pending() {
            return Err(DriverError::OperationInProgress.into());
        }
    };
}

impl Connection {
    /// Assemble connection from socket(Inner) and pool object
    pub(super) fn new(pool: Pool, mut conn: Box<Inner>) -> Connection {
        conn.info.flag = 0;
        Connection {
            pool: Some(pool),
            inner: conn,
            out: Vec::with_capacity(512),
        }
    }

    /// Returns Clickhouse server revision number
    #[inline]
    pub fn revision(&self) -> u32 {
        self.inner.info.revision
    }

    /// Check if a connection has pending status.
    /// Connection get pending status during query call until all data will be fetched
    /// or during insert call until commit
    #[inline]
    pub fn is_pending(&self) -> bool {
        self.inner.info.is_pending()
    }

    /// Returns Clickhouse server timezone
    #[inline]
    pub fn timezone(&self) -> Tz {
        self.inner.info.timezone
    }

    /// Mark connection as drain. This will recycle it on drop instead of retuning to pool
    #[inline]
    pub(super) fn set_deteriorated(&mut self) {
        self.inner.info.flag |= FLAG_DETERIORATED;
    }

    /// Disconnects this connection from server.
    pub(super) async fn disconnect(mut self) -> Result<()> {
        if let Some(mut socket) = self.inner.socket.take() {
            debug!("disconnect method. shutdown connection");
            socket.shutdown().await?;
        }
        Ok(())
    }
    /// Get pool specific options or default ones if the connection  is not established
    fn options(&self) -> &Options {
        self.pool.as_ref().map_or_else(
            || {
                let o: &Options = &*crate::DEF_OPTIONS;
                o
            },
            |p| &p.inner.options,
        )
    }

    /// Perform cleanum  and disconnect from server.
    /// It would be better to drop connection instead of explicitly
    /// call `close`.  Use `close` if you are not going to use the connection
    /// again or you need to interrupt server processes,
    /// associated with the connection
    pub async fn close(mut self) -> Result<()> {
        self.inner.cleanup().await?;
        self.disconnect().await
    }

    /// Ping-pong connection verification
    pub async fn ping(&mut self) -> Result<()> {
        debug!("ping");
        self.out.clear();
        Ping.write(self.inner.info(), &mut self.out)?;

        let mut stream = self.write_command(self.options().ping_timeout).await?;

        while let Some(packet) = &mut stream.next().await? {
            match packet {
                Response::Pong => {
                    info!("ping ok");

                    return Ok(());
                }
                _ => {
                    continue;
                    // return Err(DriverError::PacketOutOfOrder(packet.code()).into());
                }
            }
        }

        warn!("ping failed");
        Err(DriverError::ConnectionTimeout.into())
    }

    /// Execute DDL query statement.
    /// Query should not return query result. Otherwise it will return error.
    pub async fn execute(&mut self, ddl: impl Into<Query>) -> Result<()> {
        check_pending!(self);
        self.out.clear();
        Execute { query: ddl.into() }.write(self.inner.info(), &mut self.out)?;

        let mut stream = self.write_command(self.options().execute_timeout).await?;

        if let Some(packet) = stream.next().await? {
            warn!("execute method returns packet {}", packet.code());
            return Err(DriverError::PacketOutOfOrder(packet.code()).into());
        }

        Ok(())
    }

    /// Execute INSERT query sending Block of data
    /// Returns InsertSink that can be used to streaming next Blocks of data
    pub async fn insert(
        &mut self,
        data: &Block<'_>,
    ) -> Result<InsertSink<'_, &mut dyn AsyncReadWrite>> {
        check_pending!(self);

        let query = Query::from_block(&data);
        self.out.clear();
        Execute { query }.write(self.inner.info(), &mut self.out)?;

        let mut stream = self.write_command(self.options().insert_timeout).await?;

        // We get first block with no rows. We can use it to define table structure.
        // Before call insert we will check input data against server table structure
        stream.skip_empty = false;
        //stream.set_pending();
        let mut stream = if let Some(Response::Data(block)) = stream.next().await? {
            // log::info!("{:?}", &block);
            InsertSink::new(stream, block)
        } else {
            stream.set_deteriorated();
            warn!("insert method. unknown packet received");
            return Err(DriverError::PacketOutOfOrder(0).into());
        };

        stream.next(&data).await?;
        Ok(stream)
    }
    /// Execute SELECT statement returning sequence of ServerBlocks.
    ///
    /// # Example
    /// ```text
    /// use clickhouse_driver::prelude::*;
    ///
    /// let pool = Pool::create("tcp://localhost/");
    /// let mut con = pool.connection().await.unwrap();
    /// let query = con.query("SELECT id,title,message,host,ip FROM log").await.unwrap();
    /// //...
    /// ```
    pub async fn query(
        &mut self,
        sql: impl Into<Query>,
    ) -> Result<QueryResult<'_, &mut dyn AsyncReadWrite>> {
        check_pending!(self);
        self.out.clear();
        Execute { query: sql.into() }.write(self.inner.info(), &mut self.out)?;

        let stream = self.write_command(self.options().query_timeout).await?;

        Ok(QueryResult { inner: stream })
    }

    /// Take inner connection. Drain itself
    #[inline]
    pub(super) fn take(&mut self) -> Box<Inner> {
        //std::mem::replace(&mut self.inner, Box::new(Inner::default()))
        std::mem::take(&mut self.inner)
    }

    async fn write_command(
        &mut self,
        timeout: Duration,
    ) -> Result<ResponseStream<'_, &mut dyn AsyncReadWrite>> {
        let (rw, info) = if let Some((rw, info)) = self.inner.split() {
            (rw, info)
        } else {
            return Err(DriverError::ConnectionClosed.into());
        };

        info.set_pending();

        let mut stream =
            ResponseStream::with_capacity(DEFAULT_QUERY_BUF_SIZE, rw, info, timeout);

        stream.write(self.out.as_slice()).await?;
        self.out.truncate(DEF_OUT_BUF_SIZE);

        Ok(stream)
    }
}

pub(crate) fn disconnect(mut conn: Box<Inner>) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move { conn.cleanup().await });
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }

        let conn = self.take();
        if conn.is_null() {
            return;
        }
        if let Some(pool) = self.pool.take() {
            pool.inner.return_connection(conn);
        } else {
            disconnect(conn);
        }
    }
}
