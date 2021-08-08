use crate::{
    ch::blocks::{Block, Column},
    errs::{BaseRtError, BaseRtResult},
    mgmt::BMS,
};
use client::prelude::{errors::Error as ClientError, PoolBuilder};
use engine::remote;
use engine::types::QueryState;
use lang::parse::{Pair, RemoteAddr, RemoteDbType, RemoteTableInfo, Rule};
use meta::store::{parts::PartStore, sys::MetaStore};
use mysql::{OptsBuilder, Pool as MyPool};
use std::{convert::TryFrom, time::Instant};

pub fn query(
    ms: &MetaStore,
    ps: &PartStore,
    query_id: &str,
    current_db: &str,
    p: Pair<Rule>,
) -> BaseRtResult<Vec<Block>> {
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

    let mut blks = Vec::with_capacity(res.len());
    for rb in res {
        let blk = Block::try_from(rb)?;

        blks.push(blk);
    }

    Ok(blks)
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
) -> BaseRtResult<Vec<Block>> {
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
                        Ok(blks) => Ok(blks.into_iter().map(|b| b.into())),
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
                    match remote::mysql_run(pool, &sql) {
                        Ok((ncols, nrows, cols)) => {
                            let mut blk = Block {
                                name: "".into(),
                                has_header_decoded: true,
                                overflow: false,
                                bucket: -1,
                                ncols,
                                nrows,
                                columns: vec![],
                            };
                            cols.into_iter().for_each(|col| {
                                let col = Column {
                                    name: col.col_name,
                                    data: col.data,
                                };
                                blk.columns.push(col);
                            });
                            Ok(blk)
                        }
                        Err(err) => Err(err),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(blks)
        }
    }
}
