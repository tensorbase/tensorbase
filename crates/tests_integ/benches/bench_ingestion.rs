use ch_client::prelude::*;
use ch_client::prelude::{Options, Pool};
use rand::{prelude::SmallRng, RngCore, SeedableRng};
use std::{convert::TryInto, time::Instant};

pub fn get_pool_ch() -> Pool {
    let database_url = "tcp://127.0.0.1:9000?execute_timeout=15s&query_timeout=20s&pool_min=12&pool_max=12&compression=lz4".to_string();
    let opts: Options = database_url.try_into().unwrap();
    Pool::create(opts).unwrap()
}

pub fn get_pool_tb() -> Pool {
    let database_url = "tcp://127.0.0.1:9528?execute_timeout=15s&query_timeout=20s&pool_min=12&pool_max=12&compression=lz4".to_string();
    let opts: Options = database_url.try_into().unwrap();
    Pool::create(opts).unwrap()
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // run("clickhouse").await
    run("tensorbase").await
}

async fn run(db_kind: &str) -> std::io::Result<()> {
    println!("--- to run against {}", db_kind);

    let (pool, create_table_sql) = if db_kind == "clickhouse" {
        (
            get_pool_ch(),
            "CREATE TABLE test_tab(a UInt64, b UInt64)
            ENGINE = MergeTree
            PARTITION BY a%100
            ORDER BY a",
        )
    } else {
        (
            get_pool_tb(),
            "CREATE TABLE test_tab(a UInt64, b UInt64)
            ENGINE = BaseStorage
            PARTITION BY rem(a,100)",
        )
    };

    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_tab"))
        .await?;
    conn.execute(create_table_sql).await?;

    // let mut rng = SmallRng::from_entropy();

    let nrows_blk = 256u64 * 1024;
    // let mut data_a0: Vec<u8> = Vec::with_capacity(8 * nrows_blk);
    // rng.fill_bytes(&mut data_a0);
    let data_a: Vec<u64> = (0u64..nrows_blk).collect::<Vec<_>>();
    let sum_res_a = data_a.iter().sum::<u64>();

    // let mut data_b0: Vec<u8> = Vec::with_capacity(8 * nrows_blk);
    // rng.fill_bytes(&mut data_b0);
    let data_b: Vec<u64> = (0u64..nrows_blk).map(|e| e * 2).collect::<Vec<_>>();
    let sum_res_b = data_b.iter().sum::<u64>();
    println!("sum_res_a: {}, sum_res_b: {}", sum_res_a, sum_res_b);

    let block = { Block::new("test_tab").add("a", data_a).add("b", data_b) };

    let t = Instant::now();
    let num_ins = 1u64 * 512;
    for _i in 0..num_ins {
        let mut insert = conn.insert(&block).await?;
        insert.commit().await?;
    }

    // drop(insert);

    {
        let sql = "select sum(a), sum(b) from test_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let agg_res_a: u64 = row.value(0)?.unwrap();
                let agg_res_b: u64 = row.value(1)?.unwrap();
                println!("agg_res_a: {}, agg_res_b: {}", agg_res_a, agg_res_b);
                assert_eq!(agg_res_a, sum_res_a * num_ins);
                assert_eq!(agg_res_b, sum_res_b * num_ins);
            }
        }
    }

    println!("time cost: {:?}", t.elapsed());

    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}
