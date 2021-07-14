use client::prelude::*;
use client::{prelude::errors, types::SqlType};
mod common;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use client::prelude::types::Decimal;
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
async fn tests_integ_stress_test_ddl() -> errors::Result<()> {
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
        conn.execute(format!("TRUNCATE TABLE {}", tn)).await?;
    }

    conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
async fn tests_integ_basic_test_insert() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_tab"))
        .await?;
    conn.execute(format!("CREATE TABLE test_tab(a Int64)"))
        .await?;

    let data_a = vec![1i64, 3, 5, 7, 9, 11];
    let count_res = data_a.len() as i64;
    let sum_res = data_a.iter().sum::<i64>() as i64;
    let block = { Block::new("test_tab").add("a", data_a) };

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);

    {
        let sql = "select count(a) from test_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                assert_results(row, count_res)?;
            }
        }
    }

    {
        let sql = "select sum(a) from test_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let agg_res: i64 = row.value::<i64>(0)?.unwrap() as i64;
                // println!("{}", agg_res);
                assert_eq!(agg_res, sum_res);
            }
        }
    }

    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
async fn tests_integ_basic_test_insert_select() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;
    conn.execute("create database if not exists test_insert_select_db")
        .await?;
    conn.execute("use test_insert_select_db").await?;
    conn.execute("drop table if exists test_t1").await?;
    conn.execute("drop table if exists test_t2").await?;
    conn.execute("create table test_t1(a UInt64, b UInt8)")
        .await?;
    conn.execute("create table test_t2(a UInt64, b UInt8)")
        .await?;
    conn.execute("insert into test_t1 values(1, 3), (2, 4), (3, 5)")
        .await?;
    conn.execute("insert into test_t2 select * from test_t1")
        .await?;
    let mut query_result = conn.query("select * from test_t2 order by a").await?;

    while let Some(block) = query_result.next().await? {
        let mut i = 1;
        for row in block.iter_rows() {
            assert_eq!(row.value::<u64>(0)?.unwrap(), i);
            i += 1;
        }
    }

    let mut conn = pool.connection().await?;
    conn.execute("use test_insert_select_db").await?;
    conn.execute("drop table if exists test_t3").await?;
    conn.execute("drop table if exists test_t4").await?;
    conn.execute("create table test_t3(a String)").await?;
    conn.execute("create table test_t4(a String)").await?;
    let block = {
        Block::new("test_t3")
            .add("a", vec!["aelvbs a1 233 🀄️", "b^&#*-['&**%%%", "c;;;;\n\t"])
    };
    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    let mut conn = pool.connection().await?;
    conn.execute("use test_insert_select_db").await?;
    conn.execute("insert into test_t4 select * from test_t3")
        .await?;
    conn.execute("insert into test_t4(a) select a from test_t3 order by a limit 1")
        .await?;
    let mut query_result = conn.query("select count(*) from test_t4").await?;

    while let Some(block) = query_result.next().await? {
        for row in block.iter_rows() {
            assert_eq!(row.value::<u64>(0)?.unwrap(), 4);
        }
    }

    let mut conn = pool.connection().await?;
    conn.execute("use test_insert_select_db").await?;
    let mut query_result = conn
        .query("select * from test_t4 order by a limit 1")
        .await?;

    while let Some(block) = query_result.next().await? {
        for row in block.iter_rows() {
            assert_eq!(row.value::<&str>(0)?.unwrap(), "c;;;;\n\t");
        }
    }

    Ok(())
}

fn assert_results(row: Row, count_res: i64) -> errors::Result<()> {
    let rd = row.column_descr(0).unwrap();
    let styp = rd.sqltype();
    let agg_res: i64 = match styp {
        SqlType::UInt64 => row.value::<u64>(0)?.unwrap() as i64,
        _ => row.value::<i64>(0)?.unwrap(),
    };
    assert_eq!(agg_res, count_res);
    Ok(())
}

#[tokio::test]
async fn tests_integ_basic_insert_float() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_tab"))
        .await?;
    conn.execute(format!("CREATE TABLE test_tab(a Float64)"))
        .await?;

    let data_a = vec![1.1, 1.2, 1.3];
    let count_res = data_a.len() as i64;
    let sum_res = data_a.iter().sum::<f64>() as f64;
    let block = { Block::new("test_tab").add("a", data_a) };

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);

    {
        let sql = "select count(a) from test_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let agg_res: i64 = row.value::<u64>(0)?.unwrap() as i64;
                assert_eq!(agg_res, count_res);
            }
        }
    }

    {
        let sql = "select sum(a) from test_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let agg_res: f64 = row.value::<f64>(0)?.unwrap() as f64;
                // println!("{}", agg_res);
                assert_eq!(agg_res, sum_res);
            }
        }
    }

    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
async fn tests_integ_basic_insert_decimal32() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_tab_dec"))
        .await?;
    conn.execute(format!("CREATE TABLE test_tab_dec(a Decimal(9,2))"))
        .await?;

    let data_a = vec![Decimal::from(12300_i32, 2), Decimal::from(1002_i32, 2)];
    let checks = vec!["123.00", "10.02"];
    let block = Block::new("test_tab_dec").add("a", data_a);

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);
    {
        let sql = "select a from test_tab_dec";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            let mut i = 0;

            for row in block.iter_rows() {
                let res = row.value::<Decimal<i32>>(0)?.unwrap();
                assert_eq!(res.to_string(), checks[i]);
                i += 1;
            }
        }
    }

    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
async fn tests_integ_basic_insert_decimal64() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_tab_dec"))
        .await?;
    conn.execute(format!("CREATE TABLE test_tab_dec(a Decimal(12,2))"))
        .await?;

    let data_a = vec![
        Decimal::from(12300_i64, 2),
        Decimal::from(100_200_300_120_i64, 2),
    ];
    let checks = vec!["123.00", "1002003001.20"];
    let block = Block::new("test_tab_dec").add("a", data_a);

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);
    {
        let sql = "select a from test_tab_dec";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            let mut i = 0;

            for row in block.iter_rows() {
                let res = row.value::<Decimal<i64>>(0)?.unwrap();
                assert_eq!(res.to_string(), checks[i]);
                i += 1;
            }
        }
    }

    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
async fn tests_integ_basic_insert_date() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_tab_date"))
        .await?;
    conn.execute(format!("CREATE TABLE test_tab_date(a Date)"))
        .await?;

    let data_a = vec![Utc.ymd(2010, 10, 20), Utc.ymd(2020, 1, 7)];
    let checks = vec!["2010-10-20", "2020-01-07"];
    let block = { Block::new("test_tab_date").add("a", data_a) };

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);
    {
        let sql = "select a from test_tab_date";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            let mut i = 0;

            for row in block.iter_rows() {
                println!("{:?}", row);
                let res: DateTime<Utc> = row.value(0)?.unwrap();
                assert_eq!(res.date().naive_utc().to_string(), checks[i]);
                i += 1;
            }
        }
    }

    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
#[ignore = "Rust driver now does not support LCS..."]
async fn tests_integ_basic_insert_lcstring() -> errors::Result<()> {
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
    let _count_res = data_s.len() as i64;
    let block = Block::new("test_tab").add("s", data_s);
    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);
    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
async fn tests_integ_basic_insert_string() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_tab"))
        .await?;
    conn.execute(format!("CREATE TABLE test_tab(s String)"))
        .await?;

    let data_s = vec!["a", "ab", "abc"];
    // let count_res = data_s.len() as i64;
    let block = Block::new("test_tab").add("s", data_s.clone());

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);
    {
        let sql = "select s from test_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            let mut i = 0;

            for row in block.iter_rows() {
                let res: &str = row.value(0)?.unwrap();
                assert_eq!(res.to_string(), data_s[i]);
                i += 1;
            }
        }
    }

    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
async fn tests_integ_basic_insert_fixed_string() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_tab"))
        .await?;
    conn.execute(format!("CREATE TABLE test_tab(s FixedString(3))"))
        .await?;

    let data_s = vec!["a  ", "ab ", "abc"];
    let count_res = data_s.len() as i64;
    let block = Block::new("test_tab").add("s", data_s.clone());

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);
    {
        let sql = "select s from test_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for (i, row) in block.iter_rows().enumerate() {
                let res: &str = row.value(0)?.unwrap();
                assert_eq!(res, data_s[i]);
                println!("{}", res.to_string());
            }
        }
    }
    {
        let sql = "select count(s) from test_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let agg_res = row.value::<u64>(0)?.unwrap() as i64;
                assert_eq!(agg_res, count_res);
            }
        }
    }

    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
async fn tests_integ_truncate_table() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("drop table if exists test1_tab"))
        .await?;
    conn.execute(format!("drop table if exists test1_tab"))
        .await?;
    conn.execute(format!("create table test1_tab(a UInt32)"))
        .await?;
    conn.execute(format!("create table test2_tab(a UInt32)"))
        .await?;
    conn.execute(format!("insert into test1_tab values(0),(1)"))
        .await?;
    conn.execute(format!("insert into test2_tab values(0),(1)"))
        .await?;

    conn.execute(format!("truncate table test1_tab")).await?;
    {
        let sql = "select count(a) from test1_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let agg_res = row.value::<u64>(0)?.unwrap() as i64;
                assert_eq!(agg_res, 0);
            }
        }
    }

    {
        let sql = "select count(a) from test2_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let agg_res = row.value::<u64>(0)?.unwrap() as i64;
                assert_eq!(agg_res, 2);
            }
        }
    }

    conn.execute(format!("insert into test1_tab values(0),(1)"))
        .await?;

    {
        let sql = "select count(a) from test1_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let agg_res = row.value::<u64>(0)?.unwrap() as i64;
                assert_eq!(agg_res, 2);
            }
        }
    }

    conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
async fn tests_integ_desc_table() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("drop table if exists test_tab"))
        .await?;
    conn.execute(format!("create table test(a UInt64, b String)"))
        .await?;

    {
        let sql = "desc test";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            let mut block = block.iter_rows();
            let row = block.next().unwrap();
            assert_eq!(row.value::<&str>(0)?.unwrap(), "a");
            assert_eq!(row.value::<&str>(1)?.unwrap(), "UInt64");
            let row = block.next().unwrap();
            assert_eq!(row.value::<&str>(0)?.unwrap(), "b");
            assert_eq!(row.value::<&str>(1)?.unwrap(), "String");
        }
    }
    {
        let sql = "describe test";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            let mut block = block.iter_rows();
            let row = block.next().unwrap();
            assert_eq!(row.value::<&str>(0)?.unwrap(), "a");
            assert_eq!(row.value::<&str>(1)?.unwrap(), "UInt64");
            let row = block.next().unwrap();
            assert_eq!(row.value::<&str>(0)?.unwrap(), "b");
            assert_eq!(row.value::<&str>(1)?.unwrap(), "String");
        }
    }
    {
        let sql = "desc table test";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            let mut block = block.iter_rows();
            let row = block.next().unwrap();
            assert_eq!(row.value::<&str>(0)?.unwrap(), "a");
            assert_eq!(row.value::<&str>(1)?.unwrap(), "UInt64");
            let row = block.next().unwrap();
            assert_eq!(row.value::<&str>(0)?.unwrap(), "b");
            assert_eq!(row.value::<&str>(1)?.unwrap(), "String");
        }
    }

    Ok(())
}

#[allow(non_snake_case)]
#[tokio::test]
async fn tests_integ_cast_LargeUtf8_to_Utf8() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_tab"))
        .await?;
    conn.execute(format!("CREATE TABLE test_tab(s String)"))
        .await?;

    let data_s = vec!["a", "ab", "abc"];
    let count_res = data_s.len() as i64;
    let block = Block::new("test_tab").add("s", data_s.clone());

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);
    {
        let sql = "select count(s) from test_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let agg_res = row.value::<u64>(0)?.unwrap() as i64;
                assert_eq!(agg_res, count_res);
            }
        }
    }

    {
        let sql = "select count(s) from test_tab where s = 'ab'";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for (_, row) in block.iter_rows().enumerate() {
                let res: u64 = row.value(0)?.unwrap();
                println!("{}", res);
                assert_eq!(res, 1);
            }
        }
    }

    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[allow(non_snake_case)]
#[tokio::test]
async fn tests_integ_cast_simple_datatype() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    {
        let sql = "select cast(2147483647 as Int64)";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let res = row.value::<i64>(0)?.unwrap() as i64;
                assert_eq!(res, std::i32::MAX as i64);
            }
        }
    }

    {
        let sql = "select cast(-2147483648 as Int64)";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let res = row.value::<i64>(0)?.unwrap() as i64;
                assert_eq!(res, std::i32::MIN as i64);
            }
        }
    }

    {
        let sql = "select cast(0.000001 as Float64)";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let res = row.value::<f64>(0)?.unwrap() as f64;
                assert_eq!(res, 0.000001);
            }
        }
    }

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;
    conn.execute(format!("DROP TABLE IF EXISTS test_tab"))
        .await?;
    conn.execute(format!("CREATE TABLE test_tab(a Int32)"))
        .await?;

    let data_s = (1..100000).into_iter().collect::<Vec<i32>>();
    let sum_res: i64 = data_s.iter().map(|i| *i as i64).sum();
    let block = Block::new("test_tab").add("a", data_s.clone());

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    let mut conn = pool.connection().await?;
    conn.execute("use test_db").await?;
    {
        let sql = "select sum(cast(a as Int64)) from test_tab";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                let res = row.value::<i64>(0)?.unwrap() as i64;
                assert_eq!(res, sum_res);
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn tests_integ_date_cast() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_tab_date"))
        .await?;
    conn.execute(format!("CREATE TABLE test_tab_date(a Date)"))
        .await?;

    let data_a = vec![Utc.ymd(2010, 10, 20), Utc.ymd(2020, 1, 7)];
    let checks = vec!["2010-10-20", "2020-01-07"];
    let block = { Block::new("test_tab_date").add("a", data_a) };

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);
    {
        let sql = "select a from test_tab_date";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            let mut i = 0;

            for row in block.iter_rows() {
                let res: DateTime<Utc> = row.value(0)?.unwrap();
                assert_eq!(res.date().naive_utc().to_string(), checks[i]);
                i += 1;
            }
        }
    }

    {
        let sql = "select count(1) from test_tab_date where a < '2011-11-11' ";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for row in block.iter_rows() {
                println!("{:?}", row);
                let res: u64 = row.value(0)?.unwrap();
                assert_eq!(res, 1);
            }
        }
    }

    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
async fn tests_integ_select_remote_function() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_remote_func"))
        .await?;
    conn.execute(format!(
        "CREATE TABLE test_remote_func( \
            a UInt8, \
            b UInt16, \
            c UInt32, \
            d UInt64, \
            e Int8, \
            f Int16, \
            g Int32, \
            h Int64, \
            i String, \
            j DateTime \
        )"
    ))
    .await?;

    let data_a = vec![1u8, 2, 3];
    let data_b = vec![1u16, 2, 3];
    let data_c = vec![1u32, 2, 3];
    let data_d = vec![1u64, 2, 3];
    let data_i = vec!["abc", "efg", "hello world"];
    let data_naive = vec![
        NaiveDate::from_ymd(2010, 1, 1).and_hms(1, 1, 1),
        NaiveDate::from_ymd(2011, 2, 28).and_hms(2, 5, 6),
        NaiveDate::from_ymd(2012, 2, 29).and_hms(23, 59, 59),
    ];
    let data_j = apply_offset(&data_naive, FixedOffset::west(11 * 3600 + 45 * 60));

    let block = {
        Block::new("test_remote_func")
            .add("a", data_a)
            .add("b", data_b)
            .add("c", data_c)
            .add("d", data_d)
            .add("i", data_i)
            .add("j", data_j)
    };

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);
    let dates = [
        Utc.ymd(2010, 1, 1).and_hms(0, 0, 0),
        Utc.ymd(2011, 2, 28).and_hms(0, 0, 0),
        Utc.ymd(2012, 2, 29).and_hms(0, 0, 0),
    ];
    let data = vec![1, 2, 3];
    let data_i = vec!["abc", "efg", "hello world"];

    {
        let sql =
            "select a,b,c,d,e,f,h,i,j from remote('127.0.0.1:9528', test_remote_func)";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for (k, row) in block.iter_rows().enumerate() {
                println!("k={:?} row={:?}", k, row);
                let mut iter = 0..;
                let a: u8 = row.value(iter.next().unwrap())?.unwrap();
                let b: u16 = row.value(iter.next().unwrap())?.unwrap();
                let c: u32 = row.value(iter.next().unwrap())?.unwrap();
                let d: u64 = row.value(iter.next().unwrap())?.unwrap();
                let i: &str = row.value(iter.next().unwrap())?.unwrap();
                let j: DateTime<Utc> = row.value(iter.next().unwrap())?.unwrap();
                assert_eq!(a, data[k] as u8);
                assert_eq!(b, data[k] as u16);
                assert_eq!(c, data[k] as u32);
                assert_eq!(d, data[k] as u64);
                assert_eq!(i, data_i[k]);
                assert_eq!(j, dates[k]);
            }
        }
    }

    Ok(())
}

fn apply_offset(
    data_naive: &Vec<NaiveDateTime>,
    tz: impl TimeZone,
) -> Vec<DateTime<Utc>> {
    data_naive
        .iter()
        .map(|b| Utc.from_utc_datetime(&tz.from_local_datetime(b).unwrap().naive_utc()))
        .collect()
}

#[tokio::test]
async fn tests_integ_date_time_functions() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("DROP TABLE IF EXISTS test_tab_date"))
        .await?;
    conn.execute(format!(
        "CREATE TABLE test_tab_date( \
            a Date, \
            b DateTime, \
            c String, \
            d Int64, \
            e DateTime('Etc/GMT+5'), \
            f DateTime('-11:45') \
        )"
    ))
    .await?;

    let data_a = vec![
        Utc.ymd(2010, 1, 1),
        Utc.ymd(2011, 2, 28),
        Utc.ymd(2012, 2, 29),
        Utc.ymd(2012, 3, 4),
        Utc.ymd(2021, 8, 31),
        Utc.ymd(2021, 6, 27),
    ];

    let data_naive = vec![
        NaiveDate::from_ymd(2010, 1, 1).and_hms(1, 1, 1),
        NaiveDate::from_ymd(2011, 2, 28).and_hms(2, 5, 6),
        NaiveDate::from_ymd(2012, 2, 29).and_hms(23, 59, 59),
        NaiveDate::from_ymd(2012, 3, 4).and_hms(5, 6, 7),
        NaiveDate::from_ymd(2021, 8, 31).and_hms(14, 32, 3),
        NaiveDate::from_ymd(2021, 6, 27).and_hms(17, 44, 32),
    ];

    let data_b = apply_offset(&data_naive, Tz::Etc__GMTMinus8);
    let data_e = apply_offset(&data_naive, Tz::Etc__GMTPlus5);
    let data_f = apply_offset(&data_naive, FixedOffset::west(11 * 3600 + 45 * 60));

    let data_c = vec![
        "2010-1-1",
        "2011-2-28",
        "2012-02-29",
        "2012-03-4",
        "2021-8-31",
        "2021-6-27",
    ];

    let data_d = vec![14610i64, 15033, 15399, 15403, 18870, 18805];

    let years = vec![2010, 2011, 2012, 2012, 2021, 2021];
    let months = vec![1, 2, 2, 3, 8, 6];
    let quarters = vec![1, 1, 1, 1, 3, 2];
    let day_of_years = vec![1, 59, 60, 64, 243, 178];
    let day_of_months = vec![1, 28, 29, 4, 31, 27];
    let day_of_weeks = vec![5, 1, 3, 7, 2, 7];
    let hours = vec![1, 2, 23, 5, 14, 17];
    let minutes = vec![1, 5, 59, 6, 32, 44];
    let seconds = vec![1, 6, 59, 7, 3, 32];
    let dates = [
        Utc.ymd(2010, 1, 1).and_hms(0, 0, 0),
        Utc.ymd(2011, 2, 28).and_hms(0, 0, 0),
        Utc.ymd(2012, 2, 29).and_hms(0, 0, 0),
        Utc.ymd(2012, 3, 4).and_hms(0, 0, 0),
        Utc.ymd(2021, 8, 31).and_hms(0, 0, 0),
        Utc.ymd(2021, 6, 27).and_hms(0, 0, 0),
    ];

    let block = {
        Block::new("test_tab_date")
            .add("a", data_a)
            .add("b", data_b)
            .add("c", data_c)
            .add("d", data_d)
            .add("e", data_e)
            .add("f", data_f)
    };

    let mut insert = conn.insert(&block).await?;
    insert.commit().await?;

    drop(insert);
    {
        let sql = "select \
            toYear(a), toYear(b), toYear(e), toYear(f), \
            toMonth(a), toMonth(b), toMonth(e), toMonth(f), \
            toDayOfYear(a), toDayOfYear(b), toDayOfYear(e), toDayOfYear(f), \
            toDayOfMonth(a), toDayOfMonth(b), toDayOfMonth(e), toDayOfMonth(f), \
            toDayOfWeek(a), toDayOfWeek(b), toDayOfWeek(e), toDayOfWeek(f), \
            toQuarter(a), toQuarter(b), toQuarter(e), toQuarter(f), \
            toHour(b), toMinute(b), toSecond(b), \
            toHour(e), toMinute(e), toSecond(e), \
            toHour(f), toMinute(f), toSecond(f), \
            toDate(b), toDate(c), toDate(d), toDate(e), toDate(f) \
        from test_tab_date";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            for (i, row) in block.iter_rows().enumerate() {
                println!("{:?}", row);
                let mut iter = 0..;
                let year_a: u16 = row.value(iter.next().unwrap())?.unwrap();
                let year_b: u16 = row.value(iter.next().unwrap())?.unwrap();
                let year_e: u16 = row.value(iter.next().unwrap())?.unwrap();
                let year_f: u16 = row.value(iter.next().unwrap())?.unwrap();
                let month_a: u8 = row.value(iter.next().unwrap())?.unwrap();
                let month_b: u8 = row.value(iter.next().unwrap())?.unwrap();
                let month_e: u8 = row.value(iter.next().unwrap())?.unwrap();
                let month_f: u8 = row.value(iter.next().unwrap())?.unwrap();
                let day_of_year_a: u16 = row.value(iter.next().unwrap())?.unwrap();
                let day_of_year_b: u16 = row.value(iter.next().unwrap())?.unwrap();
                let day_of_year_e: u16 = row.value(iter.next().unwrap())?.unwrap();
                let day_of_year_f: u16 = row.value(iter.next().unwrap())?.unwrap();
                let day_of_month_a: u8 = row.value(iter.next().unwrap())?.unwrap();
                let day_of_month_b: u8 = row.value(iter.next().unwrap())?.unwrap();
                let day_of_month_e: u8 = row.value(iter.next().unwrap())?.unwrap();
                let day_of_month_f: u8 = row.value(iter.next().unwrap())?.unwrap();
                let day_of_week_a: u8 = row.value(iter.next().unwrap())?.unwrap();
                let day_of_week_b: u8 = row.value(iter.next().unwrap())?.unwrap();
                let day_of_week_e: u8 = row.value(iter.next().unwrap())?.unwrap();
                let day_of_week_f: u8 = row.value(iter.next().unwrap())?.unwrap();
                let quarter_a: u8 = row.value(iter.next().unwrap())?.unwrap();
                let quarter_b: u8 = row.value(iter.next().unwrap())?.unwrap();
                let quarter_e: u8 = row.value(iter.next().unwrap())?.unwrap();
                let quarter_f: u8 = row.value(iter.next().unwrap())?.unwrap();
                let hour_b: u8 = row.value(iter.next().unwrap())?.unwrap();
                let minute_b: u8 = row.value(iter.next().unwrap())?.unwrap();
                let second_b: u8 = row.value(iter.next().unwrap())?.unwrap();
                let hour_e: u8 = row.value(iter.next().unwrap())?.unwrap();
                let minute_e: u8 = row.value(iter.next().unwrap())?.unwrap();
                let second_e: u8 = row.value(iter.next().unwrap())?.unwrap();
                let hour_f: u8 = row.value(iter.next().unwrap())?.unwrap();
                let minute_f: u8 = row.value(iter.next().unwrap())?.unwrap();
                let second_f: u8 = row.value(iter.next().unwrap())?.unwrap();
                let date_b: DateTime<Utc> = row.value(iter.next().unwrap())?.unwrap();
                let date_c: DateTime<Utc> = row.value(iter.next().unwrap())?.unwrap();
                let date_d: DateTime<Utc> = row.value(iter.next().unwrap())?.unwrap();
                let date_e: DateTime<Utc> = row.value(iter.next().unwrap())?.unwrap();
                let date_f: DateTime<Utc> = row.value(iter.next().unwrap())?.unwrap();
                assert_eq!(year_a, years[i]);
                assert_eq!(year_b, years[i]);
                assert_eq!(year_e, years[i]);
                assert_eq!(year_f, years[i]);
                assert_eq!(month_a, months[i]);
                assert_eq!(month_b, months[i]);
                assert_eq!(month_e, months[i]);
                assert_eq!(month_f, months[i]);
                assert_eq!(day_of_year_a, day_of_years[i]);
                assert_eq!(day_of_year_b, day_of_years[i]);
                assert_eq!(day_of_year_e, day_of_years[i]);
                assert_eq!(day_of_year_f, day_of_years[i]);
                assert_eq!(day_of_month_a, day_of_months[i]);
                assert_eq!(day_of_month_b, day_of_months[i]);
                assert_eq!(day_of_month_e, day_of_months[i]);
                assert_eq!(day_of_month_f, day_of_months[i]);
                assert_eq!(day_of_week_a, day_of_weeks[i]);
                assert_eq!(day_of_week_b, day_of_weeks[i]);
                assert_eq!(day_of_week_e, day_of_weeks[i]);
                assert_eq!(day_of_week_f, day_of_weeks[i]);
                assert_eq!(quarter_a, quarters[i]);
                assert_eq!(quarter_b, quarters[i]);
                assert_eq!(quarter_e, quarters[i]);
                assert_eq!(quarter_f, quarters[i]);
                assert_eq!(hour_b, hours[i]);
                assert_eq!(minute_b, minutes[i]);
                assert_eq!(second_b, seconds[i]);
                assert_eq!(hour_e, hours[i]);
                assert_eq!(minute_e, minutes[i]);
                assert_eq!(second_e, seconds[i]);
                assert_eq!(hour_f, hours[i]);
                assert_eq!(minute_f, minutes[i]);
                assert_eq!(second_f, seconds[i]);
                assert_eq!(date_b, dates[i]);
                assert_eq!(date_c, dates[i]);
                assert_eq!(date_d, dates[i]);
                assert_eq!(date_e, dates[i]);
                assert_eq!(date_f, dates[i]);
            }
        }
    }

    // conn.execute("drop database if exists test_db").await?;
    Ok(())
}

#[tokio::test]
async fn tests_integ_select_all() -> errors::Result<()> {
    let pool = get_pool();
    let mut conn = pool.connection().await?;

    conn.execute("create database if not exists test_db")
        .await?;
    conn.execute("use test_db").await?;

    conn.execute(format!("drop table if exists test1_tab"))
        .await?;
    conn.execute(format!("create table test1_tab(a UInt64, b UInt64)"))
        .await?;
    conn.execute(format!("insert into test1_tab values(1,1),(2,2)"))
        .await?;

    {
        let sql = "select * from test1_tab where b = 1";
        let mut query_result = conn.query(sql).await?;

        while let Some(block) = query_result.next().await? {
            let len = block.column_count();
            assert_eq!(len, 2);
        }
    }

    conn.execute("drop database if exists test_db").await?;
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

// The system number table is supported in CH but not datafusion
// #[tokio::test]
// async fn basic_test_query_system_numbers() -> errors::Result<()> {
//     let pool = get_pool();
//     let mut conn = pool.connection().await?;
//
//     let sql = "select sum(number) from system.numbers(1000000000000)";
//     let mut query_result = conn.query(sql).await?;
//
//     while let Some(block) = query_result.next().await? {
//         for row in block.iter_rows() {
//             let agg_res: i64 = row.value(0)?.unwrap();
//             // println!("{}", agg_res);
//             assert_eq!(agg_res, 1001881602603448320);
//         }
//     }
//
//     drop(query_result);
//
//     Ok(())
// }
//
// #[tokio::test]
// async fn stress_test_query_system_numbers() -> errors::Result<()> {
//     let pool = get_pool();
//     let mut conn = pool.connection().await?;
//
//     conn.execute("use system").await?;
//
//     let sql = "select sum(number) from numbers(1000000000000)";
//
//     for i in 0..100 {
//         //FIXME -> 1024
//         println!("to run q#{}", i);
//         let mut query_result = conn.query(sql).await?;
//         while let Some(block) = query_result.next().await? {
//             for row in block.iter_rows() {
//                 let agg_res: i64 = row.value(0)?.unwrap();
//                 // println!("{}", agg_res);
//                 assert_eq!(agg_res, 1001881602603448320);
//             }
//         }
//         drop(query_result);
//     }
//
//     let sql = "select sum(123*number+456) from numbers(1000000000000)";
//
//     for i in 0..100 {
//         //FIXME -> 1024
//         println!("to run q#{}", i);
//         let mut query_result = conn.query(sql).await?;
//         while let Some(block) = query_result.next().await? {
//             for row in block.iter_rows() {
//                 let agg_res: i64 = row.value(0)?.unwrap();
//                 // println!("{}", agg_res);
//                 assert_eq!(agg_res, -5895315395742717952);
//             }
//         }
//         drop(query_result);
//     }
//
//     conn.execute("use default").await?;
//
//     Ok(())
// }

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
