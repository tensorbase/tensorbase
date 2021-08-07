use crate::errs::EngineResult;
use crate::mysql::{col_to_bql_type, get_val_bytes_from_row};
use client::prelude::{Pool, ServerBlock};
use meta::types::BaseChunk;
use mysql::{prelude::Queryable, Pool as MyPool};
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

pub fn mysql_run(
    pool: &MyPool,
    sql: &str,
) -> EngineResult<(usize, usize, Vec<(Vec<u8>, BaseChunk)>)> {
    let res: EngineResult<_> = tokio::task::block_in_place(|| {
        Handle::current().block_on(async move {
            let mut conn = pool.get_conn()?;
            let res = conn.query_iter(&sql)?;
            let ncols = res.columns().as_ref().len();
            let mut cols: Vec<(Vec<u8>, BaseChunk)> = vec![];
            let mut nrows = 0;

            for c in res.columns().as_ref() {
                let btype = col_to_bql_type(&c)?;
                cols.push((
                    c.name_str().as_bytes().to_vec(),
                    BaseChunk {
                        btype,
                        size: 0,
                        data: vec![],
                        null_map: None,
                        offset_map: None,
                        lc_dict_data: None,
                    },
                ));
            }

            for row in res {
                let r = row?;
                for c in cols.iter_mut() {
                    c.1.data.extend(get_val_bytes_from_row(
                        &r,
                        &mut c.1.offset_map,
                        &mut c.1.size,
                    )?);
                }
                nrows += 1;
            }

            Ok((ncols, nrows, cols))
        })
    });

    Ok(res?)
}
