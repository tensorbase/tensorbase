use core::marker::PhantomData;
use std::{
    fmt,
    io::{self, Write},
    iter::Iterator,
};

use super::code::*;
use super::column::{AsInColumn, ColumnDataAdapter, Deserialize, Row};
use super::encoder::Encoder;
use super::value::IntoColumn;
use super::ServerWriter;
use crate::client::ServerInfo;
use crate::compression::LZ4CompressionWrapper;
use crate::types::{Field, FIELD_NONE, FIELD_NULLABLE};
use chrono_tz::Tz;

pub struct RowIterator<'a> {
    block: &'a ServerBlock,
    id: u64,
}

impl<'a> Iterator for RowIterator<'a> {
    type Item = Row<'a>;
    fn next(&mut self) -> Option<Row<'a>> {
        if self.id >= self.block.rows {
            None
        } else {
            let id = self.id;
            self.id += 1;
            let row = unsafe { Row::create(self.block, id) };
            Some(row)
        }
    }
}

pub struct ItemIterator<'a, D: Deserialize> {
    block: &'a ServerBlock,
    id: u64,
    phantom: PhantomData<&'a D>,
}

impl<'a, D: Deserialize> Iterator for ItemIterator<'a, D> {
    type Item = D;
    fn next(&mut self) -> Option<D> {
        if self.id >= self.block.rows {
            None
        } else {
            let id = self.id;
            self.id += 1;
            let row = unsafe { Row::create(self.block, id) };
            Some(<D as Deserialize>::deserialize(row).expect("unexpected deserialization error"))
        }
    }
}

/// Inserting data column struct
/// Clickhouse serialization format
/// ------column-------------
///  FNM TNM  DATA
/// |---|---|---------|
/// FNM - field name, Clickhouse table column name
/// TNM - type name, Clickhouse sql serialized field type (Int64, String, FixedString(10)...)
/// DATA - serialized data array, has data specific format. Integer data (u)int8|16|32|64|128,
///   float f32|f64, Decimal and other fixed length data
///   (Date, DateTime, UUID, Enum8, Enum16...) are serialized as array of little-endian binary representation.
///   String column is serialized as Variant String - VarInt string length + string byte array
///   FixedString is serialized as array of string data (without length)
///   Nullable data type precedes array of null flags represented as  array of u8 where 0-null, 1-notnull
pub(crate) struct BlockInfo {
    pub(super) cols: u64,
    pub(super) rows: u64,
    pub(super) overflow: bool,
    pub(super) bucket: u32,
}

impl std::fmt::Debug for BlockInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockInfo")
            .field("columns", &self.cols)
            .field("rows", &self.rows)
            .field("overflow", &self.overflow)
            .field("bucket", &self.bucket)
            .finish()
    }
}

impl BlockInfo {
    /// Clickhouse empty block is used as a marker of end of data transfer
    pub(super) fn is_empty(&self) -> bool {
        self.rows == 0 && self.cols == 0
    }
}
/// Output data holder
pub struct Block<'b> {
    columns: Vec<ColumnDataAdapter<'b>>,
    /// Clickhouse overflow flag. @note not used now
    overflow: u8,
    /// The number of rows in each column
    rows: usize,
    /// Clickhouse table name
    pub(crate) table: &'b str,
}

impl<'b> Block<'b> {
    pub fn new(table: &'b str) -> Block<'b> {
        Block {
            overflow: 0,
            columns: Vec::new(),
            rows: 0,
            table,
        }
    }

    /// Returns the number of columns
    #[inline]
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
    /// Returns the number of rows
    #[inline]
    pub fn row_count(&self) -> usize {
        self.rows
    }

    /// Returns whether the block has any columns
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
    /// Iterate over collection of columns.
    /// Each column has it own data, so it's wrapped by adapter that provides common interface
    /// for data encoding and type check
    pub fn column_iter(&self) -> std::slice::Iter<ColumnDataAdapter> {
        self.columns.iter()
    }

    /// Check if the added column has the same number of rows as the others
    fn set_rows(&mut self, rows: usize) {
        if !self.columns.is_empty() {
            if self.rows != rows {
                panic!("block columns must have the same length")
            }
        } else {
            self.rows = rows;
        };
    }
    /// Add new column to the block
    /// NOTE! columns should be added in order of INSERT query
    pub fn add<T: 'b>(mut self, name: &'b str, data: Vec<T>) -> Self
    where
        T: IntoColumn<'b>,
    {
        self.set_rows(data.len());

        self.columns.push(ColumnDataAdapter {
            name,
            flag: FIELD_NONE,
            data: IntoColumn::to_column(data),
        });
        self
    }
    /// Add new column to block.
    /// In contrast to `add` method, here we add column for Nullable data types.
    /// Option None value is used  as null data.
    pub fn add_nullable<T: 'b>(mut self, name: &'b str, data: Vec<Option<T>>) -> Self
    where
        Option<T>: IntoColumn<'b>,
        T: Default,
    {
        self.set_rows(data.len());

        self.columns.push(ColumnDataAdapter {
            name,
            flag: FIELD_NULLABLE,
            data: IntoColumn::to_column(data),
        });
        self
    }
}
/// Block column name and type.
///
///
/// BlockColumnHeader is a part of column set of ServerBlock and used to
/// retrieve raw server data and convert it to rust data type for SELECT statements;
/// or to validate output data for INSERT statements.
/// To prevent superfluous server load by provided bunch of incorrect data
/// driver should validate the Block against server table structure
/// before it been sent to server.
pub struct BlockColumnHeader {
    pub(crate) field: Field,
    pub(crate) name: String,
}

impl fmt::Debug for BlockColumnHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("name", &self.name)
            .field("field.type", &self.field.sql_type)
            .finish()
    }
}

/// input Block column data.
pub struct BlockColumn {
    pub(crate) header: BlockColumnHeader,
    pub(crate) data: Box<dyn AsInColumn>,
}

impl BlockColumn {
    pub(crate) fn into_header(self) -> BlockColumnHeader {
        self.header
    }
}

impl std::fmt::Debug for BlockColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockColumn")
            .field("sql_type", &self.header.field.sql_type)
            .field("name", &self.header.name)
            .finish()
    }
}

#[derive(Debug)]
pub struct ServerBlock {
    pub(crate) columns: Vec<BlockColumn>,
    pub(crate) rows: u64,
    pub(crate) timezone: Tz,
}

impl ServerBlock {
    #[inline]
    pub(crate) fn into_columns(self) -> Vec<BlockColumn> {
        self.columns
    }

    #[inline]
    pub(crate) fn into_headers(self) -> Vec<BlockColumnHeader> {
        self.into_columns()
            .into_iter()
            .map(|c| c.into_header())
            .collect()
    }

    pub fn iter_rows(&self) -> RowIterator {
        RowIterator { block: self, id: 0 }
    }
    pub fn iter<D: Deserialize>(&self) -> ItemIterator<D> {
        ItemIterator {
            block: self,
            id: 0,
            phantom: PhantomData,
        }
    }

    #[inline]
    pub fn column_count(&self) -> u64 {
        self.columns.len() as u64
    }
    #[inline]
    pub fn row_count(&self) -> u64 {
        self.rows
    }
}

/// Provide specific implementation of ServerWriter trait for Empty and Data blocks
pub(crate) trait AsBlock {
    fn dump(&self, cx: &ServerInfo, writer: &mut dyn Write) -> std::io::Result<()>;
}

impl<B: AsBlock> ServerWriter for B {
    /// Serialize Block according to  server capabilities and client settings.
    /// Compress content by LZ4 if corresponding option is set
    fn write(&self, cx: &ServerInfo, writer: &mut dyn Write) -> std::io::Result<()> {
        CLIENT_DATA.encode(writer)?;

        // Temporary table
        if cx.revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES {
            ().encode(writer)?;
        }

        if !cx.compression.is_none() {
            let mut compress = LZ4CompressionWrapper::new(writer);
            self.dump(cx, &mut compress)?;
            compress.flush()
        } else {
            self.dump(cx, writer)?;
            writer.flush()
        }
    }
}
/// Empty block message is used as specific SERVER_DATA message
/// and indicate the end of data stream
/// In response to this message Clickhouse returns success of failure status
pub struct EmptyBlock;

/// Optimized byte sequence for empty block
impl AsBlock for EmptyBlock {
    /// Write block content to output stream
    #[inline]
    fn dump(&self, cx: &ServerInfo, writer: &mut dyn Write) -> std::io::Result<()> {
        let revision = cx.revision;
        if revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
            // 1  [0]    - ?
            // 0  [1]    - overflow
            // 2  [2]    - ?
            // -1 [3..7] - bucket num as int32
            // 0  [8]    - ?

            [1u8, 0, 2, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0]
                .as_ref()
                .encode(writer)
        } else {
            // columns, rows
            [0u8, 0u8].as_ref().encode(writer)
        }
    }
}
/// OutputBlockWrapper is adapter that combine provided by caller
/// columns data (Block) and
/// Clickhouse server table metadata (   BlockColumnHeader[] )
pub(super) struct OutputBlockWrapper<'b> {
    pub(super) inner: &'b Block<'b>,
    pub(super) columns: &'b Vec<BlockColumnHeader>,
}

impl OutputBlockWrapper<'_> {
    fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

impl<'b> AsBlock for OutputBlockWrapper<'b> {
    /// Write block content to output stream
    fn dump(&self, cx: &ServerInfo, writer: &mut dyn Write) -> io::Result<()> {
        if self.is_empty() {
            return EmptyBlock.dump(cx, writer);
        }

        let revision = cx.revision;
        if revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO {
            [1u8, self.inner.overflow, 2, 0xFF, 0xFF, 0xFF, 0xFF, 0]
                .as_ref()
                .encode(writer)?;
        };

        (self.columns.len() as u64).encode(writer)?;
        (self.inner.rows as u64).encode(writer)?;

        for (head, col) in self.columns.iter().zip(self.inner.columns.iter()) {
            head.name.encode(writer)?;
            head.field.encode(writer)?;
            col.data.encode(&head.field, writer)?;
        }

        Ok(())
    }
}
