use crate::errs::EngineResult;
use crate::mysql::{col_to_bql_type, get_val_bytes_from_row};
use client::prelude::{Pool, ServerBlock};
use meta::types::BaseChunk;
use mysql::{consts::ColumnType, prelude::Queryable, Pool as MyPool};
use tokio::runtime::Handle;

pub fn run(pool: &Pool, sql: &str) -> EngineResult<Vec<ServerBlock>> {
    let res: EngineResult<_> = tokio::task::block_in_place(|| {
        Handle::current().block_on(async move {
            let mut conn = pool.connection().await?;
            let mut query_result = conn.query(sql).await?;
            let mut blocks = vec![];

            while let Some(block) = query_result.next().await? {
                blocks.push(block);
            }

            Ok(blocks)
        })
    });
    Ok(res?)
}

fn query_decimal_precision_scale(
    pool: &MyPool,
    tab_name: &str,
    col_name: &str,
) -> EngineResult<Option<(u8, u8)>> {
    let sql = format!(
        "select NUMERIC_PRECISION, NUMERIC_SCALE \
       from information_schema.columns \
       where table_name='{}' and column_name='{}' limit 1;",
        tab_name, col_name
    );

    let mut conn = pool.get_conn()?;
    let res: Option<(u8, u8)> = conn.query_first(&sql)?;

    Ok(res)
}

pub struct MySqlCol {
    pub col_name: Vec<u8>,
    pub data: BaseChunk,
    pub prec_scale: Option<(u8, u8)>,
}

pub fn mysql_run(
    pool: &MyPool,
    sql: &str,
) -> EngineResult<(usize, usize, Vec<MySqlCol>)> {
    let res: EngineResult<_> = tokio::task::block_in_place(|| {
        Handle::current().block_on(async move {
            let mut conn = pool.get_conn()?;
            let res = conn.query_iter(&sql)?;
            let ncols = res.columns().as_ref().len();
            let mut cols: Vec<MySqlCol> = vec![];
            let mut nrows = 0;

            for c in res.columns().as_ref() {
                let prec_scale = match c.column_type() {
                    ColumnType::MYSQL_TYPE_DECIMAL
                    | ColumnType::MYSQL_TYPE_NEWDECIMAL => query_decimal_precision_scale(
                        pool,
                        c.table_str().as_ref(),
                        c.name_str().as_ref(),
                    )?,
                    _ => None,
                };
                let btype = col_to_bql_type(&c, &prec_scale)?;

                cols.push(MySqlCol {
                    col_name: c.name_str().as_bytes().to_vec(),
                    data: BaseChunk {
                        btype,
                        size: 0,
                        data: vec![],
                        null_map: Some(vec![]),
                        offset_map: None,
                        lc_dict_data: None,
                    },
                    prec_scale,
                });
            }

            for row in res {
                let r = row?;
                for c in cols.iter_mut() {
                    let data = get_val_bytes_from_row(
                        &r,
                        &mut c.data.offset_map,
                        &mut c.data.null_map.as_mut().unwrap(),
                        &mut c.data.size,
                    )?;
                    c.data.data.extend(data);
                }
                nrows += 1;
            }

            Ok((ncols, nrows, cols))
        })
    });

    Ok(res?)
}
