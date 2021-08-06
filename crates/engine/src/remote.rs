use crate::errs::EngineResult;
use client::prelude::{Pool, ServerBlock};
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
