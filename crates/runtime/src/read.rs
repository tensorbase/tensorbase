use engine::types::QueryState;
use lang::parse::{Pair, Rule};
use meta::store::{parts::PartStore, sys::MetaStore};
use std::{convert::TryFrom, time::Instant};

use crate::{ch::blocks::Block, errs::BaseRtResult};

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

    // log::debug!("res: {:?}", res);
    // arrow::util::pretty::print_batches(&res)?;

    let mut blks = Vec::with_capacity(res.len());
    for rb in res {
        let blk = Block::try_from(rb)?;
        log::debug!("blk: {:?}", blk);
        blks.push(blk);
    }

    Ok(blks)
}
