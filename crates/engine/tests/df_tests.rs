use std::{
    collections::HashMap,
    ffi::{c_void, CString},
    fs,
    path::Path,
    ptr,
    time::Instant,
};

use base::{debug, mmap::mm_file_ro};
use engine::errs::EngineError;
use engine::errs::EngineResult;
use meta::{
    store::parts::{open_file_as_fd, CoPaInfo, PartStore},
    types::{BqlType, ColumnInfo, Id},
};

use arrow::{
    array::{ArrayData, Int32Array},
    datatypes::{Int32Type, SchemaRef},
};
use arrow::{
    array::{ArrayRef, UInt32Array},
    ffi::FFI_ArrowArray,
    record_batch::RecordBatch,
    util::pretty,
};
use arrow::{
    buffer::Buffer,
    datatypes::{DataType, Field, Schema},
};
use datafusion::{
    datasource::MemTable,
    physical_plan::{
        coalesce_batches::CoalesceBatchesExec, memory::MemoryExec,
        repartition::RepartitionExec, ExecutionPlan, Partitioning,
    },
    prelude::{ExecutionConfig, ExecutionContext},
};
use datafusion::{datasource::TableProvider, physical_plan::collect};
use futures::StreamExt;
use std::sync::Arc;

//FIXME a dedicate nyc taxi test suite needed
const TID: Id = 4;
const CID0: Id = TID + 1;
const CID1: Id = TID + 2;

#[tokio::test]
#[ignore]
async fn basic_query() -> EngineResult<()> {
    let cis = vec![(CID0, BqlType::UInt(32))];
    let mut query_id = 1u64;
    let sql = "select sum(trip_id) from trips_lite";
    // let sql = "select sum(123*trip_id+456) from trips_lite where trip_id>666";
    // let sql = "select sum(trip_id) from trips_lite where trip_id>666";
    let exprs = vec!["sum(trip_id)"];
    let res_chk = vec![vec![1072792700412018895]];
    query_id += 1;
    run_ker_test_arrow(
        query_id.to_string().as_str(),
        cis,
        exprs,
        res_chk,
        None,
        sql,
    )
    .await?;

    Ok(())
}

fn init_copas2(cis: Vec<(Id, BqlType)>) -> Vec<Vec<CoPaInfo>> {
    let parts_dir = "/jin/tmp/tb_schema";
    let data_dirs = vec!["/jin/tmp/tb_data".to_string()];
    let dd = &data_dirs;
    let ps = PartStore::new(&[parts_dir], dd).unwrap();
    // ps.pretty_print().unwrap();
    let mut copas = Vec::new();
    ps.fill_copainfos_int_by_ptk_range(&mut copas, TID, &cis, 0, u64::MAX)
        .unwrap();
    copas
}

async fn run_ker_test_arrow(
    query_id: &str,
    cis: Vec<(Id, BqlType)>,
    exprs: Vec<&str>,
    expected_results_rows_wised: Vec<Vec<i64>>,
    filters_expr: Option<&str>,
    sql: &str,
) -> EngineResult<()> {
    // let sql = "select numbers from system.numbers limit 10";
    let mut qs = engine::types::QueryState::default();
    qs.tz_offset = 8 * 3600;

    let timer = Instant::now();

    let copass = init_copas2(cis.clone());
    println!("copass.len: {}", copass.len());
    // qs.copass.extend(copass);
    // println!("Q#{}: init time cost: {:?}", query_id, timer.elapsed());
    let conf = ExecutionConfig::new();
    let mut ctx = ExecutionContext::with_config(conf);



    // let start = Instant::now();

    // let elapsed = start.elapsed().as_micros();
    // println!("Query {} iteration {} create_logical_plan took {} ms", opt.query, i, elapsed);
    // result = execute_query(&mut ctx, &plan, opt.debug).await?;

    let plan = ctx.create_logical_plan(sql)?;
    let plan = ctx.optimize(&plan)?;

    let tabname = "trips_lite";
    let schema = Arc::new(Schema::new(vec![Field::new(
        tabname,
        DataType::UInt32,
        false,
    )]));
    ctx.register_table(
        tabname,
        Arc::new(MemTable::try_new(schema.clone(), vec![])?),
    )?;

    println!("Optimized logical plan:\n{:?}", plan);
    let ep = ctx.create_physical_plan(&plan)?;
    // println!(
    //     "from gen BaseTable to create_physical_plan took {} micros",
    //     start.elapsed().as_micros(),
    // );
    let result = collect(ep).await?;
    println!("Q#{}: run time cost: {:?}", query_id, timer.elapsed());
    pretty::print_batches(&result)?;

    let nc = copass.len();
    let np = copass[0].len();
    println!("nc: {}, np: {}", nc, np);
    let mut batches = Vec::with_capacity(np);
    for i in 0..np {
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(nc);
        for j in 0..nc {
            let copa = &copass[j];
            let cpi = &copa[i];
            let dummy = Arc::new(FFI_ArrowArray::empty());
            let buf = unsafe {
                let ptr = std::ptr::NonNull::new(cpi.addr as *mut u8)
                    .ok_or(EngineError::UnwrapOptionError)?;
                Buffer::from_unowned(ptr, cpi.len_in_bytes, dummy)
            };
            let data = ArrayData::builder(DataType::UInt32)
                .len(cpi.size)
                .add_buffer(buf)
                .build();
            cols.push(Arc::new(UInt32Array::from(data)));
        }
        let batch = RecordBatch::try_new(schema.clone(), cols)?;
        batches.push(batch);
    }
    println!("batches.len: {}", batches.len());
    let conf = ExecutionConfig::new();
    let mut ctx = ExecutionContext::with_config(conf);

    let tabname = "trips_lite";
    let schema = Arc::new(Schema::new(vec![Field::new(
        "trip_id",
        DataType::UInt32,
        false,
    )]));
    ctx.register_table(
        tabname,
        Arc::new(MemTable::try_new(schema.clone(), vec![batches])?),
    )?;
    // println!("batches[0].num_columns: {}", batches[0].num_columns());

    // let output_partitions =
    //     coalesce_batches(&schema, vec![batches], 16 * 1024).await?;
    // let BaseTable = BaseTable::try_new(schema.clone(), output_partitions)?;


    // let start = Instant::now();
    // println!(
    //     "Loaded table '{}' into memory in {} ms",
    //     table,
    //     start.elapsed().as_millis()
    // );

    println!("round-2 query...");
    let timer = Instant::now();

    let plan = ctx.create_logical_plan(sql)?;
    let plan = ctx.optimize(&plan)?;
    let ep = ctx.create_physical_plan(&plan)?;
    let result = collect(ep).await?;

    println!(
        "round-2 Q#{}: run time cost: {:?}",
        query_id,
        timer.elapsed()
    );

    pretty::print_batches(&result)?;

    Ok(())
}
