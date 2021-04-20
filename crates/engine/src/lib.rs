#![feature(once_cell)]

use arrow::record_batch::RecordBatch;
use errs::EngineResult;
use lang::parse::{self, Pair, Rule};
use meta::store::{parts::PartStore, sys::MetaStore};
use types::QueryState;

pub mod datafusions;
pub mod errs;
pub mod types;

pub fn run(
    ms: &MetaStore,
    ps: &PartStore,
    current_db: &str,
    p: Pair<Rule>,
    raw_query: &str,
    query_id: &str,
    qs: &mut QueryState,
) -> EngineResult<Vec<RecordBatch>> {
    let (tabs, cols) = parse::parse_tables(p)?;
    log::debug!("projections - tabs: {:?}, cols: {:?}", tabs, cols);
    datafusions::run(ms, ps, current_db, raw_query, query_id, tabs, cols, qs)
}
