use ch_client::prelude::errors;
use ch_client::prelude::*;
mod common;
use common::get_pool;

// macro_rules! get {
//     ($row:ident, $idx: expr, $msg: expr) => {
//         $row.value($idx)?.expect($msg)
//     };
//     ($row:ident, $idx: expr) => {
//         get!($row, $idx, "unexpected error")
//     };
// }

#[tokio::test]
async fn stress_test_ddl() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    for i in 0..100 {
        let tn = format!("test_tab_{}", i);
        conn.execute(format!("DROP TABLE IF EXISTS {}", tn)).await?;
        conn.execute(format!("CREATE TABLE {}(x Int64)", tn))
            .await?;
    }

    conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
async fn basic_test_insert() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_tab"))
        .await?;
    conn.execute(format!("CREATE TABLE test_tab(a UInt64)"))
        .await?;

    let data_a = vec![1u64, 3, 5, 7, 9, 11];
    let count_res = data_a.len() as i64;
    let sum_res = data_a.iter().sum::<u64>() as i64;
    let block = { Block::new("test_tab").add("a", data_a) };

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);

    let sql = "select count(a) from test_tab";
    {
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let agg_res: i64 = row.value(0)?.unwrap();
                assert_eq!(agg_res, count_res);
            }
        }
    }

    {
        let sql = "select sum(a) from test_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let agg_res: i64 = row.value(0)?.unwrap();
                // println!("{}", agg_res);
                assert_eq!(agg_res, sum_res);
            }
        }
    }

    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
#[ignore = "Rust driver now does not support LCS..."]
async fn basic_insert_lcstring() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_tab"))
        .await?;
    conn.execute(format!("CREATE TABLE test_tab(s LowCardinality(String))"))
        .await?;

    let data_s = vec!["a", "b", "c", "a", "b", "a"];
    let count_res = data_s.len() as i64;
    let block = { Block::new("test_tab").add("s", data_s) };

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);
    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}

// #[tokio::test]
// async fn test_insert_large_block() -> errors::Result<()> {
//     let pool = get_pool();
//     let mut conn = pool.connection().await?;

//     // conn.execute("create database if not exists test_db")
//     //     .await?;
//     // conn.execute("use test_db").await?;

//     // conn.execute(format!("DROP TABLE IF EXISTS test_tab"))
//     //     .await?;
//     // conn.execute(format!("CREATE TABLE test_tab(a UInt64)"))
//     //     .await?;
//     let siz_data = 500_000; //331775u32;
//     let mut data_t: Vec<u32> = Vec::new();
//     for i in 0u32..siz_data {
//         data_t.push(735020648u32 + i);
//     }

//     let now = chrono::offset::Utc::now();
//     let data_p = vec![now; siz_data as usize];

//     let block = {
//         Block::new("trips_lite_n10")
//             .add("trip_id", data_t)
//             .add("pickup_datetime", data_p)
//     };

//     let mut insert = conn.insert(&block).await?;
//     insert.commit().await?;

//     drop(insert);

//     // conn.execute("drop database if exists test_db").await?;
//     Ok(())
// }

#[tokio::test]
async fn basic_test_query_system_numbers() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    let sql = "select sum(number) from system.numbers(1000000000000)";
    let mut query_result = conn.query(sql).await?;

    while let Some(block) = query_result.next().await? {
        for row in block.iter_rows() {
            let agg_res: i64 = row.value(0)?.unwrap();
            // println!("{}", agg_res);
            assert_eq!(agg_res, 1001881602603448320);
        }
    }

    drop(query_result);

    Ok(())
}

#[tokio::test]
async fn stress_test_query_system_numbers() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("use system").await?;

    let sql = "select sum(number) from numbers(1000000000000)";

    for i in 0..100 {
        //FIXME -> 1024
        println!("to run q#{}", i);
        let mut query_result = conn.query(sql).await?;
        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let agg_res: i64 = row.value(0)?.unwrap();
                // println!("{}", agg_res);
                assert_eq!(agg_res, 1001881602603448320);
            }
        }
        drop(query_result);
    }

    let sql = "select sum(123*number+456) from numbers(1000000000000)";

    for i in 0..100 {
        //FIXME -> 1024
        println!("to run q#{}", i);
        let mut query_result = conn.query(sql).await?;
        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let agg_res: i64 = row.value(0)?.unwrap();
                // println!("{}", agg_res);
                assert_eq!(agg_res, -5895315395742717952);
            }
        }
        drop(query_result);
    }

    conn.execute("use default").await?;

    Ok(())
}

// #[tokio::test]
// async fn test_query_compress() -> errors::Result<()> {
//     let config = get_config();

//     let pool = Pool::create(config.set_compression(CompressionMethod::LZ4)).unwrap();
//     {
//         let mut conn = pool.connection().await?;

//         let mut qr = conn.query("SELECT lcs FROM main LIMIT 1000").await?;
//         while let Some(_block) = qr.next().await? {}
//         assert_eq!(qr.is_pending(), false);
//     }

//     drop(pool);
//     let config = get_config();

//     let pool = Pool::create(config.set_compression(CompressionMethod::None)).unwrap();
//     let mut conn = pool.connection().await?;

//     let mut qr = conn.query("SELECT lcs FROM main LIMIT 1000").await?;
//     while let Some(block) = qr.next().await? {
//         for row in block.iter_rows() {
//             let _lcs: &str = row.value(0)?.unwrap();
//             //println!("{}",lcs);
//         }
//     }
//     assert_eq!(qr.is_pending(), false);

//     drop(pool);
//     let pool = get_pool();
//     {
//         let mut conn = pool.connection().await?;

//         let mut qr = conn.query("SELECT lcs FROM main LIMIT 1000").await?;
//         while let Some(block) = qr.next().await? {
//             for row in block.iter_rows() {
//                 let _lcs: &str = row.value(0)?.unwrap();
//                 //println!("{}", lcs);
//             }
//         }
//         assert_eq!(qr.is_pending(), false);
//     }
//     Ok(())
// }

// #[tokio::test]
// async fn test_query_pending() -> errors::Result<()> {
//     let pool = get_pool();
//     let mut conn = pool.connection().await?;

//     let mut query_result = conn.query("SELECT  i64 FROM main").await?;

//     let mut i: u32 = 0;
//     while let Some(_block) = query_result.next().await? {
//         i += 1;
//         if i == 1 {
//             assert_eq!(query_result.is_pending(), true);
//         }
//     }

//     assert_eq!(query_result.is_pending(), false);
//     drop(query_result);
//     Ok(())
// }

// #[tokio::test]
// async fn test_query_string() -> errors::Result<()> {
//     let pool = get_pool();
//     let mut conn = pool.connection().await?;

//     let mut query_result = conn.query("SELECT title FROM main").await?;

//     while let Some(block) = query_result.next().await? {
//         for (j, row) in block.iter_rows().enumerate() {
//             let s: &str = get!(row, 0);
//             println!("{:4}:{}", j, s);
//         }
//     }

//     Ok(())
// }

// #[tokio::test]
// async fn test_query_enum() -> errors::Result<()> {
//     let pool = get_pool();
//     let mut conn = pool.connection().await?;

//     let mut query_result = conn.query("SELECT e8,e16 FROM main").await?;

//     while let Some(block) = query_result.next().await? {
//         for row in block.iter_rows() {
//             let e8: &str = get!(row, 0);
//             let e16: &str = get!(row, 1);
//             println!("'{}'='{}'", e8, e16);
//         }
//     }

//     Ok(())
// }

// #[tokio::test]
// async fn test_query_nullable() -> errors::Result<()> {
//     let pool = get_pool();
//     let mut conn = pool.connection().await?;

//     let mut query_result = conn.query("SELECT n FROM main WHERE n=NULL").await?;

//     while let Some(block) = query_result.next().await? {
//         for row in block.iter_rows() {
//             let n: Option<u16> = row.value(0)?;
//             assert!(n.is_none());
//         }
//     }
//     drop(query_result);

//     let mut query_result = conn.query("SELECT n FROM main WHERE n=1").await?;

//     while let Some(block) = query_result.next().await? {
//         for row in block.iter_rows() {
//             let n: Option<u16> = row.value(0)?;
//             assert!(n.is_some());
//             assert_eq!(n.unwrap(), 1u16);
//         }
//     }
//     Ok(())
// }

// #[tokio::test]
// async fn test_query_lowcardinality() -> errors::Result<()> {
//     let pool = get_pool();
//     let mut conn = pool.connection().await?;

//     let mut query_result = conn
//         .query("SELECT lcs FROM mainx WHERE lcs='May' LIMIT 1000")
//         .await?;

//     while let Some(block) = query_result.next().await? {
//         for row in block.iter_rows() {
//             let lcs: &str = row.value(0)?.unwrap();
//             assert_eq!(lcs, "May");
//         }
//     }
//     drop(query_result);
//     let mut query_result = conn
//         .query("SELECT lcs FROM mainx WHERE lcs IS NULL LIMIT 1000")
//         .await?;

//     while let Some(block) = query_result.next().await? {
//         for row in block.iter_rows() {
//             let lcs: Option<&str> = row.value(0)?;
//             assert!(lcs.is_none());
//         }
//     }

//     Ok(())
// }
