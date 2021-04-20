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
    mgmt::BuildKersHasher,
};

pub(crate) fn query(
    ms: &MetaStore,
    ps: &PartStore,
    kers_reg: &DashMap<String, (CString, QueryState), BuildKersHasher>,
    query_id: &str,
    current_db: &str,
    p: Pair<Rule>,
    tz_offset: i32,
) -> BaseRtResult<Vec<Block>> {
    // if let Some(ker_info) = kers_reg.get(p.as_str()) {
    //     let ker_name = &ker_info.0;
    //     let qs0 = &ker_info.1;//FIXME this will be thrown?
    //     let qs = QueryState::default();

    //     let timer = Instant::now();
    //     let ret = rerun_ker(&ker_name);
    //     log::debug!("rerun ker in time cost {:?}", timer.elapsed());
    //     debug_assert!(ret == 0);

    //     unsafe { gen_results_blk(&qs) }
    // } else {
    let timer = Instant::now();
    let query_id = query_id.replace("-", "_");
    let ker_name =
        CString::new("_ker".to_string() + query_id.as_str()).unwrap();
    let raw_query = p.as_str().to_string();
    let mut qs = QueryState::default();
    qs.tz_offset = tz_offset;
    // let ker = gen_ker(ms, ps, current_db, p, query_id.as_str(), &mut qs)
    //     .map_err(|e| BaseRtError::WrappingEngineError(e))?;
    // // log::debug!("ker: {}\n", ker.clone().to_str().unwrap());
    //
    // let ret = run_ker(&ker_name, ker);

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
    log::info!("query run time cost {:?}", timer.elapsed());

    log::debug!("res: {:?}", res);
    // arrow::util::pretty::print_batches(&res)?;

    let mut blks = Vec::with_capacity(res.len());
    for rb in res {
        let blk = Block::try_from(rb)?;
        log::debug!("blk: {:?}", blk);
        blks.push(blk);
    }

    //FIXME check ret to requeue and drop the old qs
    // let _old_ker_info = kers_reg.insert(raw_query, (ker_name, qs));

    Ok(blks)
    // }
}