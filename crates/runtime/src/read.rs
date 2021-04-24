use dashmap::DashMap;
use engine::{run, types::QueryState};
use lang::parse::{Pair, Rule};
use libc::c_void;
use meta::{
    store::{parts::PartStore, sys::MetaStore},
    types::{BaseChunk, BqlType},
};
use std::{convert::TryFrom, ffi::CString, ptr, time::Instant};

use crate::{
    ch::blocks::{Block, Column},
    errs::{BaseRtError, BaseRtResult},
};

pub(crate) fn query(
    ms: &MetaStore,
    ps: &PartStore,
    query_id: &str,
    current_db: &str,
    p: Pair<Rule>,
    tz_offset: i32,
) -> BaseRtResult<Vec<Block>> {
    let timer = Instant::now();
    let query_id = query_id.replace("-", "_");
    let raw_query = p.as_str().to_string();
    let mut qs = QueryState::default();
    qs.tz_offset = tz_offset;

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

    log::debug!("res: {:?}", res);
    // arrow::util::pretty::print_batches(&res)?;

    let mut blks = Vec::with_capacity(res.len());
    for rb in res {
        let blk = Block::try_from(rb)?;
        log::debug!("blk: {:?}", blk);
        blks.push(blk);
    }

    Ok(blks)
}