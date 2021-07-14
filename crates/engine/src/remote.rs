use crate::errs::EngineResult;
use client::prelude::{Pool, ServerBlock};
use std::lazy::SyncLazy;
use tokio::runtime::{self, Runtime};

static TOKIO_RT: SyncLazy<Runtime> = SyncLazy::new(|| {
    runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
});

pub fn run(pool: &Pool, sql: &str) -> EngineResult<Vec<ServerBlock>> {
    TOKIO_RT.block_on(async move {
        let mut conn = pool.connection().await?;
        let mut query_result = conn.query(sql).await?;
        let mut blocks = vec![];

        while let Some(block) = query_result.next().await? {
            blocks.push(block);
        }

        Ok(blocks)
    })
}
