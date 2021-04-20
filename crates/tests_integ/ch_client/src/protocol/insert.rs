use futures::TryFutureExt;
use std::marker::Unpin;
use tokio::io::{AsyncRead, AsyncWrite};

use super::block::{Block, BlockColumnHeader, EmptyBlock, OutputBlockWrapper, ServerBlock};
use super::command::ResponseStream;
use crate::errors::{ConversionError, DriverError, Result};

use super::ServerWriter;
use crate::protocol::column::{ColumnDataAdapter, EnumIndex};

const DEFAULT_INSERT_BUFFER_SIZE: usize = 8 * 1024;

pub struct InsertSink<'a, R: AsyncRead + AsyncWrite + Unpin + Send> {
    pub(crate) inner: ResponseStream<'a, R>,
    pub(crate) buf: Vec<u8>,
    #[allow(dead_code)]
    pub(crate) columns: Vec<BlockColumnHeader>,
}

impl<'a, R: AsyncRead + AsyncWrite + Unpin + Send> Drop for InsertSink<'a, R> {
    fn drop(&mut self) {
        self.inner.clear_pending()
    }
}

impl<'a, R: AsyncRead + AsyncWrite + Unpin + Send> InsertSink<'a, R> {
    pub(crate) fn new(tcpstream: ResponseStream<'a, R>, block: ServerBlock) -> InsertSink<'a, R> {
        let buf = Vec::with_capacity(DEFAULT_INSERT_BUFFER_SIZE);

        let mut columns = block.into_headers();
        // prepare Enum8 and Enum16  for string->data conversion by sorting index by string
        for column in columns.iter_mut() {
            if let Some(meta) = column.field.get_meta_mut() {
                meta.index.sort_unstable_by(EnumIndex::fn_sort_str);
            }
        }

        InsertSink {
            inner: tcpstream,
            buf,
            columns,
        }
    }
    /// Send another block of data to Clickhouse server.
    /// It's part of streamed insert API.
    /// Streamed insert is the most effective method when you need
    /// to load into server huge data .
    pub async fn next(&mut self, data: &Block<'_>) -> Result<()> {
        self.buf.clear();
        // The number of Columns must be the same!
        // As a precaution check it
        if data.column_count() != self.columns.len() {
            return Err(DriverError::BrokenData.into());
        }
        // TODO: split huge block on chunks less then MAX_BLOCK_SIZE size each
        // Now the caller responsible to split data
        if data.row_count() > crate::MAX_BLOCK_SIZE {
            return Err(DriverError::BrokenData.into());
        }
        let compatible = |(head, col): (&BlockColumnHeader, &ColumnDataAdapter)| {
            head.name.eq(col.name)
                && head.field.flag == col.flag
                && col.data.is_compatible(&head.field)
        };
        // For efficiency we check input data and column data format compatibility only once
        // before serialization. We just check if columns names and nullability flags of
        // table structure  and  provided data block match to each over.
        // Also we make basic check the ability to convert Sql data type to rust data.
        if !self.columns.iter().zip(data.column_iter()).all(compatible) {
            return Err(ConversionError::UnsupportedConversion.into());
        }

        // TODO: get rid of intermediate buffer. Write block right into stream
        OutputBlockWrapper {
            columns: &self.columns,
            inner: data,
        }
        .write(self.inner.info_ref(), &mut self.buf)?;

        self.inner
            .write(self.buf.as_slice())
            .map_err(Into::into)
            .await
    }
    /// Commits  last inserted blocks returning server insert status.
    /// @note. Clickhouse does not support ASID transactions.
    /// There is no way to revert a transaction.
    /// Commit just allow to get status of previously inserted blocks.
    /// If it returns an error, you can send last blocks again.
    /// Clickhouse keeps hashes of last 100 blocks. So you can safely
    /// without duplication commit only every hundredth block.
    ///
    /// # Example
    /// for (i,chunk) in chanks.enumerate(){
    ///     con.insert(Block::new("log")
    ///         .add("id", chunk.vec_of_id)
    ///         .add("message", chunk.vec_of_messages)).await?;
    ///     if i%100 == 0 {
    ///         status = con.commit().await?;
    ///         if status.is_err(){
    ///             todo with error
    ///         }
    ///     }
    /// }
    pub async fn commit(&mut self) -> Result<()> {
        self.buf.clear();
        EmptyBlock.write(self.inner.info_ref(), &mut self.buf)?;
        self.inner.write(self.buf.as_slice()).await?;

        if let Some(packet) = self.inner.next().await? {
            return Err(DriverError::PacketOutOfOrder(packet.code()).into());
        }
        // Disable fuse. it allows us to make intermediate  commits
        // self.inner.set_fuse();
        Ok(())
    }
}
