extern crate server_mysql;

use async_trait::async_trait;
use std::io;
use tokio::io::AsyncWrite;

use arrow::{array, datatypes::DataType, record_batch::RecordBatch};
use log;
use server_mysql::{
    AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter, ParamParser,
    QueryResultWriter, StatementMetaWriter,
};

use crate::{
    errs::{BaseRtError, BaseRtResult},
    mgmt::{BaseCommandKind, BMS},
    types::BaseServerConn,
};

#[derive(Default, Debug)]
pub struct MysqlConn {
    query_id: String,
    current_db: String,
    is_compressed: bool,
}

impl BaseServerConn for MysqlConn {
    fn get_query_id(&self) -> &str {
        &self.query_id
    }

    fn set_query_id(&mut self, query_id: String) {
        self.query_id = query_id;
    }

    fn get_db(&self) -> &str {
        &self.current_db
    }

    fn set_db(&mut self, db: String) {
        self.current_db = db;
    }

    fn set_compressed(&mut self, is_compressed: bool) {
        self.is_compressed = is_compressed;
    }

    fn is_compressed(&self) -> bool {
        self.is_compressed
    }
}

#[async_trait]
impl<W: io::Write + AsyncWrite + Send> AsyncMysqlShim<W> for MysqlConn {
    type Error = BaseRtError;

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn on_close<'a>(&'a mut self, stmt: u32)
    where
        W: 'async_trait,
    {
    }

    async fn on_init<'a>(
        &'a mut self,
        schema: &'a str,
        writer: InitWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        let query = format!("USE {}", schema);
        match BMS.run_commands(query, self) {
            Ok(_) => Ok(writer.ok()?),
            Err(e) => {
                // TODO: Add access denied error
                writer.error(
                    ErrorKind::ER_BAD_DB_ERROR,
                    format!("Can't access database {}", schema).as_bytes(),
                )?;
                Err(e)
            }
        }
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<(), Self::Error> {
        let res = BMS.run_commands(query.to_string(), self);
        match res {
            Ok(BaseCommandKind::Query(blks)) => write_blks(results, blks),
            Ok(
                BaseCommandKind::Create
                | BaseCommandKind::Drop
                | BaseCommandKind::Optimize
                | BaseCommandKind::InsertFormatInlineValues
                | BaseCommandKind::InsertFormatSelectValue,
            ) => {
                log::debug!("Query Success:1");
                results.completed(0, 0)?;
                Ok(())
            }
            Ok(
                BaseCommandKind::InsertFormatInline(header, qtn, tid)
                | BaseCommandKind::InsertFormatCSV(header, qtn, tid),
            ) => {
                todo!()
            }
            Ok(_) => Err(BaseRtError::UnsupportedFunctionality),
            Err(e) => {
                // log::info!("Return Err: {}", e);
                Err(e)
            }
        }
    }
}

fn write_blks<W: io::Write>(
    results: QueryResultWriter<W>,
    blks: Vec<RecordBatch>,
) -> BaseRtResult<()> {
    if blks.len() > 1 {
        return Err(BaseRtError::UnexpectedMessage);
    }
    let blk = &blks[0];
    let columns: Vec<Column> = blk
        .schema()
        .fields()
        .iter()
        .map(|c| Column {
            table: "".to_string(),
            column: c.name().to_owned(),
            coltype: arrow_type_to_mysql_type(c.data_type()).unwrap(),
            colflags: if matches!(
                c.data_type(),
                DataType::UInt16 | DataType::UInt32 | DataType::UInt64 | DataType::UInt8
            ) {
                ColumnFlags::UNSIGNED_FLAG
            } else {
                ColumnFlags::empty()
            },
        })
        .collect();

    let mut writer = results.start(&columns)?;
    for row in 0..blk.num_rows() {
        for col in blk.columns() {
            match col.data_type() {
                DataType::Int8 => {
                    let val = col
                        .as_any()
                        .downcast_ref::<array::Int8Array>()
                        .unwrap()
                        .value(row);
                    log::debug!("Write result on row {}: {:?}", row, val);
                    writer.write_col(val)?;
                }
                DataType::Int16 => {
                    let val = col
                        .as_any()
                        .downcast_ref::<array::Int16Array>()
                        .unwrap()
                        .value(row);
                    log::debug!("Write result on row {}: {:?}", row, val);
                    writer.write_col(val)?;
                }
                DataType::Int32 => {
                    let val = col
                        .as_any()
                        .downcast_ref::<array::Int32Array>()
                        .unwrap()
                        .value(row);
                    log::debug!("Write result on row {}: {:?}", row, val);
                    writer.write_col(val)?;
                }
                DataType::Int64 => {
                    let val = col
                        .as_any()
                        .downcast_ref::<array::Int64Array>()
                        .unwrap()
                        .value(row);
                    log::debug!("Write result on row {}: {:?}", row, val);
                    writer.write_col(val)?;
                }
                DataType::UInt8 => {
                    let val = col
                        .as_any()
                        .downcast_ref::<array::UInt8Array>()
                        .unwrap()
                        .value(row);
                    log::debug!("Write result on row {}: {:?}", row, val);
                    writer.write_col(val)?;
                }
                DataType::UInt16 => {
                    let val = col
                        .as_any()
                        .downcast_ref::<array::UInt16Array>()
                        .unwrap()
                        .value(row);
                    log::debug!("Write result on row {}: {:?}", row, val);
                    writer.write_col(val)?;
                }
                DataType::UInt32 => {
                    let val = col
                        .as_any()
                        .downcast_ref::<array::UInt32Array>()
                        .unwrap()
                        .value(row);
                    log::debug!("Write result on row {}: {:?}", row, val);
                    writer.write_col(val)?;
                }
                DataType::UInt64 => {
                    let val = col
                        .as_any()
                        .downcast_ref::<array::UInt64Array>()
                        .unwrap()
                        .value(row);
                    log::debug!("Write result on row {}: {:?}", row, val);
                    writer.write_col(val)?;
                }
                DataType::Float32 => {
                    let val = col
                        .as_any()
                        .downcast_ref::<array::Float32Array>()
                        .unwrap()
                        .value(row);
                    log::debug!("Write result on row {}: {:?}", row, val);
                    writer.write_col(val)?;
                }
                DataType::Float64 => {
                    let val = col
                        .as_any()
                        .downcast_ref::<array::Float64Array>()
                        .unwrap()
                        .value(row);
                    log::debug!("Write result on row {}: {:?}", row, val);
                    writer.write_col(val)?;
                }
                DataType::Timestamp32(_) => {
                    todo!();
                }
                DataType::Date16 => {
                    todo!();
                }
                DataType::Decimal(_, _) => {
                    todo!();
                }
                DataType::LargeUtf8 => {
                    let val = col
                        .as_any()
                        .downcast_ref::<array::LargeStringArray>()
                        .unwrap()
                        .value(row);
                    log::debug!("Write result on row {}: {:?}", row, val);
                    writer.write_col(val)?;
                }
                DataType::FixedSizeBinary(_) => {
                    let val = col
                        .as_any()
                        .downcast_ref::<array::FixedSizeBinaryArray>()
                        .unwrap()
                        .value(row);
                    log::debug!("Write result on row {}: {:?}", row, val);
                    writer.write_col(val)?;
                }
                _ => return Err(BaseRtError::UnsupportedValueConversion),
            }
        }
        writer.end_row()?;
    }

    Ok(writer.finish()?)
}

fn arrow_type_to_mysql_type(typ: &DataType) -> BaseRtResult<ColumnType> {
    log::debug!("arrow_type_to_btype: {}", typ);
    match typ {
        DataType::Int8 | DataType::UInt8 => Ok(ColumnType::MYSQL_TYPE_TINY),
        DataType::Int16 | DataType::UInt16 => Ok(ColumnType::MYSQL_TYPE_SHORT),
        DataType::Int32 | DataType::UInt32 => Ok(ColumnType::MYSQL_TYPE_LONG),
        DataType::Int64 | DataType::UInt64 => Ok(ColumnType::MYSQL_TYPE_LONGLONG),
        DataType::Float32 => Ok(ColumnType::MYSQL_TYPE_FLOAT),
        DataType::Float64 => Ok(ColumnType::MYSQL_TYPE_DOUBLE),
        DataType::Timestamp32(_) => todo!(),
        DataType::Date16 => Ok(ColumnType::MYSQL_TYPE_DATE),
        DataType::Decimal(_, _) => Ok(ColumnType::MYSQL_TYPE_DECIMAL),
        DataType::LargeUtf8 => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
        DataType::FixedSizeBinary(len) => Ok(ColumnType::MYSQL_TYPE_TINY),
        _ => Err(BaseRtError::UnsupportedConversionToBqlType),
    }
}
