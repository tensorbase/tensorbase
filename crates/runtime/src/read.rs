use crate::{
    errs::{BaseRtError, BaseRtResult},
    mgmt::BMS,
    types::{BaseColumn, BaseDataBlock},
};
use arrow::{
    array::ArrayData, buffer::Buffer, datatypes::DataType, record_batch::RecordBatch,
};
use arrow::{array::*, datatypes::Field, datatypes::Schema};
use base::datetimes::TimeZoneId;
use client::{
    prelude::{errors::Error as ClientError, PoolBuilder, ServerBlock},
    types::SqlType,
};
use engine::{remote, types::QueryState};
use lang::parse::{Pair, RemoteAddr, RemoteDbType, RemoteTableInfo, Rule};
use meta::{
    store::{parts::PartStore, sys::MetaStore},
    types::btype_to_arrow_type,
};
use mysql::{OptsBuilder, Pool as MyPool};
use std::{sync::Arc, time::Instant};

pub fn query(
    ms: &MetaStore,
    ps: &PartStore,
    query_id: &str,
    current_db: &str,
    p: Pair<Rule>,
) -> BaseRtResult<Vec<RecordBatch>> {
    let timer = Instant::now();
    let query_id = query_id.replace("-", "_");
    let raw_query = p.as_str().to_string();
    let mut qs = QueryState::default();

    // debug_assert!(ret == 0);
    let res = engine::run(
        ms,
        ps,
        current_db,
        p,
        raw_query.as_str(),
        query_id.as_str(),
        &mut qs,
    )?;

    log::debug!("query run time cost {:?}", timer.elapsed());

    Ok(res)
}

const DEFAULT_REMOTE_PORT: u16 = 9528;
const DEFAULT_REMOTE_COMPRESSION: &str = "lz4";
const DEFAULT_REMOTE_POOL_MIN_SIZE: u16 = 1;
const DEFAULT_REMOTE_POOL_MAX_SIZE: u16 = 4;

fn update_remote_db_pools(remote_tb_info: &RemoteTableInfo) -> BaseRtResult<()> {
    let ps = &BMS.remote_db_pool;

    let RemoteTableInfo {
        addrs,
        username,
        password,
        database_name,
        ..
    } = remote_tb_info;
    for remote_addr in addrs {
        if let Some(pool) = ps.get(&remote_addr) {
            continue;
        }

        let RemoteAddr {
            ip_addr,
            host_name,
            port,
        } = remote_addr.clone();
        let mut builder = PoolBuilder::default()
            .with_compression()
            .with_pool(DEFAULT_REMOTE_POOL_MIN_SIZE, DEFAULT_REMOTE_POOL_MAX_SIZE);

        if let Some(username) = &username {
            builder = builder.with_username(username);
        }

        if let Some(password) = &password {
            builder = builder.with_password(password);
        }

        if let Some(database) = &database_name {
            builder = builder.with_database(database);
        }

        let addr = ip_addr
            .map(|i| i.to_string())
            .or(host_name)
            .map(|s| format!("{}:{}", s, port.unwrap_or(DEFAULT_REMOTE_PORT)))
            .unwrap_or("".into());

        builder = builder.add_addr(addr);
        let pool = builder.build()?;
        ps.insert(remote_addr.clone(), pool);
    }

    Ok(())
}

pub fn update_mysql_db_pools(remote_tb_info: &RemoteTableInfo) -> BaseRtResult<()> {
    let ps = &BMS.remote_mysql_pool;
    let RemoteTableInfo {
        addrs,
        username,
        password,
        database_name,
        ..
    } = remote_tb_info;
    for remote_addr in addrs {
        if let Some(pool) = ps.get(&remote_addr) {
            continue;
        }

        let RemoteAddr {
            ip_addr,
            host_name,
            port,
        } = remote_addr.clone();
        let mut opt = OptsBuilder::new();

        opt = opt.ip_or_hostname(ip_addr.map(|ip| ip.to_string()).or(host_name));

        opt = opt.tcp_port(port.unwrap_or(3306));

        if let Some(username) = username.as_ref() {
            opt = opt.user(username.into());
        }

        if let Some(password) = password.as_ref() {
            opt = opt.pass(password.into());
        }

        if let Some(db) = database_name.as_ref() {
            opt = opt.db_name(db.into());
        }

        let pool = MyPool::new(opt)?;
        ps.insert(remote_addr.clone(), pool);
    }

    Ok(())
}

pub fn remote_query(
    remote_tb_info: RemoteTableInfo,
    raw_query: &str,
    is_local: bool,
) -> BaseRtResult<Vec<RecordBatch>> {
    match remote_tb_info.database_type {
        RemoteDbType::ClickHouse | RemoteDbType::TensorBase => {
            update_remote_db_pools(&remote_tb_info)?;
        }
        RemoteDbType::Mysql => {
            update_mysql_db_pools(&remote_tb_info)?;
        }
    }

    let sql = if !is_local {
        remote_tb_info
            .to_local_query_str(raw_query)
            .ok_or(ClientError::Other("missing table info.".into()))?
    } else {
        raw_query.to_owned()
    };

    match remote_tb_info.database_type {
        RemoteDbType::ClickHouse | RemoteDbType::TensorBase => {
            let ps = &BMS.remote_db_pool;

            let blks = remote_tb_info
                .addrs
                .into_iter()
                .map(|addr| {
                    let pool = &*ps.get(&addr).unwrap();
                    match remote::run(pool, &sql) {
                        Ok(blks) => Ok(blks
                            .into_iter()
                            .map(|b| serverblock_to_recordbatch(b).unwrap())),
                        Err(err) => Err(BaseRtError::WrappingEngineError(err)),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect();

            Ok(blks)
        }
        RemoteDbType::Mysql => {
            let ps = &BMS.remote_mysql_pool;
            let blks = remote_tb_info
                .addrs
                .into_iter()
                .map(|addr| {
                    let pool = &*ps.get(&addr).unwrap();
                    let block = match remote::mysql_run(pool, &sql) {
                        Ok((ncols, nrows, cols)) => {
                            let mut blk = BaseDataBlock {
                                ncols,
                                nrows,
                                columns: vec![],
                            };
                            cols.into_iter().for_each(|col| {
                                let col = BaseColumn {
                                    name: col.col_name,
                                    data: col.data,
                                };
                                blk.columns.push(col);
                            });
                            Ok(blk)
                        }
                        Err(err) => Err(err),
                    };
                    basedatablock_to_recordbatch(block?)
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(blks)
        }
    }
}

// TODO FIXME: The best approach is to unify the server and client block definitions
// to avoid conversions
#[inline]
fn sqltype_to_arrowtype(sqltype: SqlType) -> DataType {
    match sqltype {
        SqlType::UInt8 => DataType::UInt8,
        SqlType::UInt16 => DataType::UInt16,
        SqlType::UInt32 => DataType::UInt32,
        SqlType::UInt64 => DataType::UInt64,
        SqlType::Int8 => DataType::Int8,
        SqlType::Int16 => DataType::Int16,
        SqlType::Int32 => DataType::Int32,
        SqlType::Int64 => DataType::Int64,
        SqlType::String => DataType::LargeUtf8,
        SqlType::FixedString(i) => DataType::LargeBinary,
        SqlType::Float32 => DataType::Float32,
        SqlType::Float64 => DataType::Float64,
        SqlType::Date => DataType::Date16,
        SqlType::DateTime(tz) => match tz {
            Some(tz) => {
                DataType::Timestamp32(Some(TimeZoneId(TimeZoneId::calc_offset_of_tz(tz))))
            }
            None => DataType::Timestamp32(None),
        },
        SqlType::DateTime64(id, tz) => {
            DataType::Timestamp32(Some(TimeZoneId(TimeZoneId::calc_offset_of_tz(tz))))
        }
        SqlType::Decimal(x, y) => DataType::Decimal(x.into(), y.into()),
        SqlType::LowCardinality => DataType::UInt8,
        SqlType::Ipv4
        | SqlType::Ipv6
        | SqlType::Uuid
        | SqlType::Enum8
        | SqlType::Enum16
        | SqlType::Array => unimplemented!(),
    }
}

// TODO FIXME: The best approach is to unify the server and client block definitions
// to avoid conversions
fn serverblock_to_recordbatch(b: ServerBlock) -> BaseRtResult<RecordBatch> {
    let mut cols: Vec<Arc<dyn Array>> = Vec::new();
    let mut fields = Vec::new();
    let nrows = b.rows as usize;

    for mut c in b.columns {
        let arrow_type = sqltype_to_arrowtype(c.header.field.get_sqltype());

        fields.push(Field::new(
            &c.header.name,
            arrow_type.clone(),
            c.header.field.is_nullable(),
        ));

        let data = unsafe { c.data.into_bytes() };

        let buf = Buffer::from_slice_ref(&data);

        let data = if matches!(arrow_type, DataType::LargeUtf8) {
            let offset_map = c.data.offset_map().unwrap(); // Must have offset map
            let offset_map = offset_map
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<i64>>();
            let buf_om = Buffer::from_slice_ref(&offset_map);

            ArrayData::builder(arrow_type.clone())
                .len(nrows)
                .add_buffer(buf_om)
                .add_buffer(buf)
                .build()
        } else {
            ArrayData::builder(arrow_type.clone())
                .len(nrows)
                .add_buffer(buf)
                .build()
        };
        match arrow_type {
            DataType::Int8 => {
                cols.push(Arc::new(Int8Array::from(data)));
            }
            DataType::Int16 => {
                cols.push(Arc::new(Int16Array::from(data)));
            }
            DataType::Int32 => {
                cols.push(Arc::new(Int32Array::from(data)));
            }
            DataType::Int64 => {
                cols.push(Arc::new(Int64Array::from(data)));
            }
            DataType::UInt8 => {
                cols.push(Arc::new(UInt8Array::from(data)));
            }
            DataType::UInt16 => {
                cols.push(Arc::new(UInt16Array::from(data)));
            }
            DataType::UInt32 => {
                cols.push(Arc::new(UInt32Array::from(data)));
            }
            DataType::UInt64 => {
                cols.push(Arc::new(UInt64Array::from(data)));
            }
            DataType::Float32 => {
                cols.push(Arc::new(Float32Array::from(data)));
            }
            DataType::Float64 => {
                cols.push(Arc::new(Float64Array::from(data)));
            }
            DataType::Timestamp32(_) => {
                cols.push(Arc::new(Timestamp32Array::from(data)));
            }
            DataType::Date16 => {
                cols.push(Arc::new(Date16Array::from(data)));
            }
            DataType::Decimal(_, _) => {
                cols.push(Arc::new(DecimalArray::from(data)));
            }
            DataType::LargeUtf8 => {
                cols.push(Arc::new(GenericStringArray::<i64>::from(data)));
            }
            DataType::FixedSizeBinary(_) => {
                cols.push(Arc::new(FixedSizeBinaryArray::from(data)));
            }
            // TODO!!!
            _ => return Err(BaseRtError::FailToUnwrapOpt),
        }
    }

    let schema = Schema::new(fields);

    Ok(RecordBatch::try_new(Arc::new(schema), cols)?)
}

fn basedatablock_to_recordbatch(b: BaseDataBlock) -> BaseRtResult<RecordBatch> {
    let mut cols: Vec<Arc<dyn Array>> = Vec::new();
    let mut fields = Vec::new();
    let nrows = b.nrows as usize;

    for c in b.columns {
        let arrow_type = btype_to_arrow_type(c.data.btype)?;

        fields.push(Field::new(c.get_name(), arrow_type.clone(), true));

        let buf = Buffer::from_slice_ref(&c.data.data);

        let data = if matches!(arrow_type, DataType::LargeUtf8) {
            if let Some(offset_map) = c.data.offset_map {
                let buf_om = Buffer::from_slice_ref(&offset_map);

                ArrayData::builder(arrow_type.clone())
                    .len(nrows)
                    .add_buffer(buf_om)
                    .add_buffer(buf)
                    .build()
            } else {
                // Must give an offset map
                todo!();
            }
        } else {
            ArrayData::builder(arrow_type.clone())
                .len(nrows)
                .add_buffer(buf)
                .build()
        };
        match arrow_type {
            DataType::Int8 => {
                cols.push(Arc::new(Int8Array::from(data)));
            }
            DataType::Int16 => {
                cols.push(Arc::new(Int16Array::from(data)));
            }
            DataType::Int32 => {
                cols.push(Arc::new(Int32Array::from(data)));
            }
            DataType::Int64 => {
                cols.push(Arc::new(Int64Array::from(data)));
            }
            DataType::UInt8 => {
                cols.push(Arc::new(UInt8Array::from(data)));
            }
            DataType::UInt16 => {
                cols.push(Arc::new(UInt16Array::from(data)));
            }
            DataType::UInt32 => {
                cols.push(Arc::new(UInt32Array::from(data)));
            }
            DataType::UInt64 => {
                cols.push(Arc::new(UInt64Array::from(data)));
            }
            DataType::Float32 => {
                cols.push(Arc::new(Float32Array::from(data)));
            }
            DataType::Float64 => {
                cols.push(Arc::new(Float64Array::from(data)));
            }
            DataType::Timestamp32(_) => {
                cols.push(Arc::new(Timestamp32Array::from(data)));
            }
            DataType::Date16 => {
                cols.push(Arc::new(Date16Array::from(data)));
            }
            DataType::Decimal(_, _) => {
                cols.push(Arc::new(DecimalArray::from(data)));
            }
            DataType::LargeUtf8 => {
                cols.push(Arc::new(GenericStringArray::<i64>::from(data)));
            }
            DataType::FixedSizeBinary(_) => {
                cols.push(Arc::new(FixedSizeBinaryArray::from(data)));
            }
            // TODO!!!
            _ => return Err(BaseRtError::FailToUnwrapOpt),
        }
    }

    let schema = Schema::new(fields);

    Ok(RecordBatch::try_new(Arc::new(schema), cols)?)
}
