mod common;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use common::get_tb_mysql_pool;
use mysql::prelude::*;
use mysql_common::bigdecimal::BigDecimal;

#[tokio::test]
async fn tests_mysql_integ_stress_test_ddl() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    for i in 0..100 {
        let tn = format!("test_tab_{}", i);
        conn.query_drop(format!("DROP TABLE IF EXISTS {}", tn))
            .unwrap();
        conn.query_drop(format!("CREATE TABLE {}(x Int64)", tn))
            .unwrap();
        conn.query_drop(format!("TRUNCATE TABLE {}", tn)).unwrap();
    }

    conn.query_drop("drop database if exists test_db").unwrap();
}

#[tokio::test]
async fn tests_mysql_integ_basic_test_insert() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    conn.query_drop(format!("DROP TABLE IF EXISTS test_tab"))
        .unwrap();
    conn.query_drop(format!("CREATE TABLE test_tab(a Int64)"))
        .unwrap();

    let data_a = vec![1i64, 3, 5, 7, 9, 11];
    for a in &data_a {
        conn.query_drop(format!("INSERT INTO test_tab values ({})", a))
            .unwrap();
    }

    {
        let sql = "select count(a) from test_tab";
        let res: Vec<usize> = conn.query(sql).unwrap();
        assert_eq!(res.len(), 1);
        assert_eq!(res[0], 6);
    }

    {
        let sql = "select sum(a) from test_tab";
        let sum_res: i64 = data_a.iter().sum();
        let _ = conn.query_map(sql, |res: i64| assert_eq!(res, sum_res));
    }

    conn.query_drop("drop database if exists test_db").unwrap();
}

#[tokio::test]
async fn tests_mysql_integ_basic_test_insert_select() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();
    conn.query_drop("create database if not exists test_insert_select_db")
        .unwrap();
    conn.query_drop("use test_insert_select_db").unwrap();
    conn.query_drop("drop table if exists test_t1").unwrap();
    conn.query_drop("drop table if exists test_t2").unwrap();
    conn.query_drop("create table test_t1(a UInt64, b UInt8)")
        .unwrap();
    conn.query_drop("create table test_t2(a UInt64, b UInt8)")
        .unwrap();
    conn.query_drop("insert into test_t1 values(1, 3), (2, 4), (3, 5)")
        .unwrap();
    conn.query_drop("insert into test_t2 select * from test_t1")
        .unwrap();
    let query_result = conn.query_iter("select * from test_t2 order by a").unwrap();

    for (i, block) in query_result.enumerate() {
        let row = block.unwrap();
        assert_eq!(row.get::<u64, _>(0).unwrap(), (i + 1) as u64);
        assert_eq!(row.get::<u64, _>(1).unwrap(), (i + 3) as u64);
    }

    let mut conn = pool.get_conn().unwrap();
    conn.query_drop("use test_insert_select_db").unwrap();
    conn.query_drop("drop table if exists test_t3").unwrap();
    conn.query_drop("drop table if exists test_t4").unwrap();
    conn.query_drop("create table test_t3(a String)").unwrap();
    conn.query_drop("create table test_t4(a String)").unwrap();

    let data_a = vec!["aelvbs a1 233 🀄️", "b^&#*-['&**%%%", "c;;;;\n\t"];

    let data_escaped = data_a.iter().map(|s| {
        s.chars()
            .map(|c| {
                if c == '\'' {
                    "''".to_string()
                } else {
                    c.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join("")
    });

    for a in data_escaped {
        conn.query_drop(&format!("insert into test_t3 values ('{}')", a))
            .unwrap();
    }

    let mut conn = pool.get_conn().unwrap();
    conn.query_drop("use test_insert_select_db").unwrap();
    conn.query_drop("insert into test_t4 select * from test_t3")
        .unwrap();
    conn.query_drop("insert into test_t4(a) select a from test_t3 order by a limit 1")
        .unwrap();
    let query_result = conn.query_iter("select count(*) from test_t4").unwrap();

    for block in query_result {
        let row = block.unwrap();
        assert_eq!(row.get::<u64, _>(0).unwrap(), 4);
    }

    let mut conn = pool.get_conn().unwrap();
    conn.query_drop("use test_insert_select_db").unwrap();
    let query_result = conn
        .query_iter("select * from test_t4 order by a limit 1")
        .unwrap();

    for block in query_result {
        let row = block.unwrap();
        assert_eq!(row.get::<String, _>(0).unwrap(), "c;;;;\n\t");
    }
}

#[tokio::test]
async fn tests_mysql_integ_basic_insert_float() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    conn.query_drop(format!("DROP TABLE IF EXISTS test_tab"))
        .unwrap();
    conn.query_drop(format!("CREATE TABLE test_tab(a Float64)"))
        .unwrap();

    let data_a = vec![1.1, 1.2, 1.3];
    let count_res: i64 = data_a.len() as i64;
    let sum_res: f64 = data_a.iter().sum();

    for a in data_a {
        conn.query_drop(&format!("insert into test_tab values ({})", a))
            .unwrap();
    }

    {
        let sql = "select count(a) from test_tab";
        let query_result = conn.query_iter(sql).unwrap();

        for block in query_result {
            let row = block.unwrap();
            let agg_res: i64 = row.get::<u64, _>(0).unwrap() as i64;
            assert_eq!(agg_res, count_res);
        }
    }

    {
        let sql = "select sum(a) from test_tab";
        let query_result = conn.query_iter(sql).unwrap();

        for block in query_result {
            let row = block.unwrap();
            let agg_res: f64 = row.get::<f64, _>(0).unwrap() as f64;
            // println!("{}", agg_res);
            assert_eq!(agg_res, sum_res);
        }
    }
}

#[tokio::test]
async fn tests_mysql_integ_basic_insert_decimal32() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    conn.query_drop(format!("DROP TABLE IF EXISTS test_tab_dec"))
        .unwrap();
    conn.query_drop(format!("CREATE TABLE test_tab_dec(a Decimal(9,2))"))
        .unwrap();

    let data_a = vec!["123.00", "10.02"];
    for &a in &data_a {
        conn.query_drop(&format!("insert into test_tab_dec values ({})", a))
            .unwrap();
    }

    {
        let sql = "select a from test_tab_dec";
        let mut query_result = conn.query_iter(sql).unwrap();

        let mut i = 0;
        while let Some(block) = query_result.next() {
            let row = block.unwrap();
            let res: BigDecimal = row.get::<BigDecimal, _>(0).unwrap();
            assert_eq!(res.to_string(), data_a[i]);
            i += 1;
        }
    }
}

#[tokio::test]
async fn tests_mysql_integ_basic_insert_decimal64() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    conn.query_drop(format!("DROP TABLE IF EXISTS test_tab_dec"))
        .unwrap();
    conn.query_drop(format!("CREATE TABLE test_tab_dec(a Decimal(12,2))"))
        .unwrap();

    let data_a = vec!["123.00", "1002003001.20"];
    for &a in &data_a {
        conn.query_drop(&format!("insert into test_tab_dec values ({})", a))
            .unwrap();
    }

    {
        let sql = "select a from test_tab_dec";
        let mut query_result = conn.query_iter(sql).unwrap();

        let mut i = 0;
        while let Some(block) = query_result.next() {
            let row = block.unwrap();
            let res: BigDecimal = row.get::<BigDecimal, _>(0).unwrap();
            assert_eq!(res.to_string(), data_a[i]);
            i += 1;
        }
    }
}

#[tokio::test]
async fn tests_mysql_integ_basic_insert_date() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    conn.query_drop(format!("DROP TABLE IF EXISTS test_tab_date"))
        .unwrap();
    conn.query_drop(format!("CREATE TABLE test_tab_date(a Date)"))
        .unwrap();

    let data_a = vec![Utc.ymd(2010, 10, 20), Utc.ymd(2020, 1, 7)];
    let checks = vec!["2010-10-20", "2020-01-07"];
    for &a in &data_a {
        conn.query_drop(format!(
            "insert into test_tab_date values ('{}')",
            a.naive_utc()
        ))
        .unwrap();
    }

    {
        let sql = "select a from test_tab_date";
        let mut query_result = conn.query_iter(sql).unwrap();

        let mut i = 0;
        while let Some(block) = query_result.next() {
            let row = block.unwrap();

            let res: NaiveDate = row.get(0).unwrap();
            assert_eq!(res.to_string(), checks[i]);
            i += 1;
        }
    }
}

#[tokio::test]
async fn tests_mysql_integ_basic_insert_string() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    conn.query_drop(format!("DROP TABLE IF EXISTS test_tab"))
        .unwrap();
    conn.query_drop(format!("CREATE TABLE test_tab(s String)"))
        .unwrap();

    let data_s = vec!["a", "ab", "abc"];

    for a in &data_s {
        conn.query_drop(&format!("insert into test_tab values ('{}')", a))
            .unwrap();
    }

    {
        let sql = "select s from test_tab";
        let query_result = conn.query_iter(sql).unwrap();

        for (i, block) in query_result.enumerate() {
            let row = block.unwrap();
            let res: String = row.get(0).unwrap();
            assert_eq!(res, data_s[i]);
        }
    }
}

#[tokio::test]
async fn tests_mysql_integ_basic_insert_fixed_string() {
    let pool = get_tb_mysql_pool();
    println!("Created pool");
    let mut conn = pool.get_conn().unwrap();
    println!("Got connection");

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    conn.query_drop(format!("DROP TABLE IF EXISTS test_tab"))
        .unwrap();
    conn.query_drop(format!("CREATE TABLE test_tab(s FixedString(3))"))
        .unwrap();

    let data_s = vec!["a  ", "ab ", "abc"];
    let count_res = data_s.len() as i64;

    for a in &data_s {
        conn.query_drop(&format!("insert into test_tab values ('{}')", a))
            .unwrap();
    }

    {
        let sql = "select s from test_tab";
        let query_result = conn.query_iter(sql).unwrap();

        for (i, block) in query_result.enumerate() {
            let row = block.unwrap();
            let res: String = row.get(0).unwrap();
            assert_eq!(res, data_s[i]);
            println!("{}", res.to_string());
        }
    }
    {
        let sql = "select count(s) from test_tab";
        let query_result = conn.query_iter(sql).unwrap();

        for block in query_result {
            let row = block.unwrap();
            let agg_res = row.get::<u64, _>(0).unwrap() as i64;
            assert_eq!(agg_res, count_res);
        }
    }
}

#[tokio::test]
async fn tests_mysql_integ_desc_table() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    conn.query_drop(format!("drop table if exists test_tab"))
        .unwrap();
    conn.query_drop(format!("create table test(a UInt64, b String)"))
        .unwrap();

    let fields = vec!["a", "b"];
    let types = vec!["UInt64", "String"];

    {
        let sql = "desc test";
        let query_result = conn.query_iter(sql).unwrap();

        for (i, block) in query_result.enumerate() {
            let row = block.unwrap();
            assert_eq!(row.get::<String, _>(0).unwrap(), fields[i]);
            assert_eq!(row.get::<String, _>(1).unwrap(), types[i]);
        }
    }
    {
        let sql = "describe test";
        let query_result = conn.query_iter(sql).unwrap();

        for (i, block) in query_result.enumerate() {
            let row = block.unwrap();
            assert_eq!(row.get::<String, _>(0).unwrap(), fields[i]);
            assert_eq!(row.get::<String, _>(1).unwrap(), types[i]);
        }
    }
    {
        let sql = "desc table test";
        let query_result = conn.query_iter(sql).unwrap();

        for (i, block) in query_result.enumerate() {
            let row = block.unwrap();
            assert_eq!(row.get::<String, _>(0).unwrap(), fields[i]);
            assert_eq!(row.get::<String, _>(1).unwrap(), types[i]);
        }
    }
}

#[allow(non_snake_case)]
#[tokio::test]
async fn tests_mysql_integ_cast_LargeUtf8_to_Utf8() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    conn.query_drop(format!("DROP TABLE IF EXISTS test_tab"))
        .unwrap();
    conn.query_drop(format!("CREATE TABLE test_tab(s String)"))
        .unwrap();

    let data_s = vec!["a", "ab", "abc"];
    let count_res = data_s.len() as i64;

    for a in data_s {
        conn.query_drop(&format!("insert into test_tab values ('{}')", a))
            .unwrap();
    }

    {
        let sql = "select count(s) from test_tab";
        let query_result = conn.query_iter(sql).unwrap();

        for block in query_result {
            let row = block.unwrap();
            let agg_res = row.get::<u64, _>(0).unwrap() as i64;
            assert_eq!(agg_res, count_res);
        }
    }

    {
        let sql = "select count(s) from test_tab where s = 'ab'";
        let query_result = conn.query_iter(sql).unwrap();

        for block in query_result {
            let row = block.unwrap();
            let res: u64 = row.get(0).unwrap();
            println!("{}", res);
            assert_eq!(res, 1);
        }
    }
}

#[allow(non_snake_case)]
#[tokio::test]
async fn tests_mysql_integ_cast_simple_datatype() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    {
        let sql = "select cast(2147483647 as Int64)";
        let query_result = conn.query_iter(sql).unwrap();

        for block in query_result {
            let row = block.unwrap();
            let res = row.get::<i64, _>(0).unwrap() as i64;
            assert_eq!(res, std::i32::MAX as i64);
        }
    }

    {
        let sql = "select cast(-2147483648 as Int64)";
        let query_result = conn.query_iter(sql).unwrap();

        for block in query_result {
            let row = block.unwrap();
            let res = row.get::<i64, _>(0).unwrap() as i64;
            assert_eq!(res, std::i32::MIN as i64);
        }
    }

    {
        let sql = "select cast(0.000001 as Float64)";
        let query_result = conn.query_iter(sql).unwrap();

        for block in query_result {
            let row = block.unwrap();
            let res = row.get::<f64, _>(0).unwrap() as f64;
            assert_eq!(res, 0.000001);
        }
    }

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();
    conn.query_drop(format!("DROP TABLE IF EXISTS test_tab"))
        .unwrap();
    conn.query_drop(format!("CREATE TABLE test_tab(a Int32)"))
        .unwrap();

    let data_a = ((1e9 as i32)..(1e9 as i32 + 100))
        .into_iter()
        .collect::<Vec<i32>>();
    let sum_res: i64 = data_a.iter().map(|i| *i as i64).sum();

    for a in data_a {
        conn.query_drop(&format!("insert into test_tab values ({})", a))
            .unwrap();
    }

    let mut conn = pool.get_conn().unwrap();
    conn.query_drop("use test_db").unwrap();
    {
        let sql = "select sum(cast(a as Int64)) from test_tab";
        let query_result = conn.query_iter(sql).unwrap();

        for block in query_result {
            let row = block.unwrap();
            let res = row.get::<i64, _>(0).unwrap() as i64;
            assert_eq!(res, sum_res);
        }
    }
}

#[tokio::test]
async fn tests_mysql_integ_date_cast() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    conn.query_drop(format!("DROP TABLE IF EXISTS test_tab_date"))
        .unwrap();
    conn.query_drop(format!("CREATE TABLE test_tab_date(a Date)"))
        .unwrap();

    let data_a = vec![Utc.ymd(2010, 10, 20), Utc.ymd(2020, 1, 7)];
    let checks = vec!["2010-10-20", "2020-01-07"];
    for &a in &data_a {
        conn.query_drop(format!(
            "insert into test_tab_date values ('{}')",
            a.naive_utc()
        ))
        .unwrap();
    }

    {
        let sql = "select a from test_tab_date";
        let mut query_result = conn.query_iter(sql).unwrap();

        let mut i = 0;
        while let Some(block) = query_result.next() {
            let row = block.unwrap();

            println!("{:?}", row);
            let res: NaiveDate = row.get(0).unwrap();
            assert_eq!(res.to_string(), checks[i]);
            i += 1;
        }
    }

    {
        let sql = "select count(1) from test_tab_date where a < '2011-11-11' ";
        let mut query_result = conn.query_iter(sql).unwrap();

        while let Some(block) = query_result.next() {
            let row = block.unwrap();
            println!("{:?}", row);
            let res: u64 = row.get(0).unwrap();
            assert_eq!(res, 1);
        }
    }
}

#[tokio::test]
#[ignore = "MySQL server currently does not support remote functions"]
async fn tests_mysql_integ_select_remote_function() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop(format!("DROP TABLE IF EXISTS test_remote_func"))
        .unwrap();
    conn.query_drop(format!(
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
        )"
    ))
    .unwrap();

    let data_a = vec![1u8, 2, 3];
    let data_b = vec![1u16, 2, 3];
    let data_c = vec![1u32, 2, 3];
    let data_d = vec![1u64, 2, 3];
    let data_i = vec!["abc", "efg", "hello world"];
    // @fandahao17 TODO: Add date types
    // let data_naive = vec![
    //     NaiveDate::from_ymd(2010, 1, 1).and_hms(1, 1, 1),
    //     NaiveDate::from_ymd(2011, 2, 28).and_hms(2, 5, 6),
    //     NaiveDate::from_ymd(2012, 2, 29).and_hms(23, 59, 59),
    // ];
    // let data_j = apply_offset(&data_naive, FixedOffset::west(11 * 3600 + 45 * 60));

    for i in 0..data_a.len() {
        conn.query_drop(&format!(
            "insert into test_t3 values ({}, {}, {}, {}, '{}')",
            data_a[i], data_b[i], data_c[i], data_d[i], data_i[i]
        ))
        .unwrap();
    }

    // @fandahao17 TODO: Add date types
    // let dates = vec![
    //     NaiveDate::from_ymd(2010, 1, 1).and_hms(1, 1, 1),
    //     NaiveDate::from_ymd(2011, 2, 28).and_hms(2, 5, 6),
    //     NaiveDate::from_ymd(2012, 2, 29).and_hms(23, 59, 59),
    // ];
    // let dates = apply_offset(&dates, FixedOffset::west(11 * 3600 + 45 * 60));
    let data = vec![1, 2, 3];
    let data_i = vec!["abc", "efg", "hello world"];

    {
        let sql = "select a,b,c,d,i,j from remote('127.0.0.1:9528', test_remote_func)";
        let query_result = conn.query_iter(sql).unwrap();

        for (k, block) in query_result.enumerate() {
            let row = block.unwrap();
            println!("k={:?} row={:?}", k, row);
            let a: u8 = row.get(0).unwrap();
            let b: u16 = row.get(1).unwrap();
            let c: u32 = row.get(2).unwrap();
            let d: u64 = row.get(3).unwrap();
            let i: String = row.get(4).unwrap();
            // let j: DateTime<Utc> = row.get(iter.next()).unwrap();
            assert_eq!(a, data[k] as u8);
            assert_eq!(b, data[k] as u16);
            assert_eq!(c, data[k] as u32);
            assert_eq!(d, data[k] as u64);
            assert_eq!(i, data_i[k]);
            // assert_eq!(j, dates[k]);
        }
    }
}

#[tokio::test]
#[ignore = "MySQL server currently does not support remote functions"]
async fn tests_mysql_integ_insert_into_remote_function() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop(format!("DROP TABLE IF EXISTS test_remote_func"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE test_remote_func( \
            a UInt8, \
            b UInt16, \
            c UInt32, \
            d UInt64, \
            i String, \
        )"
    ))
    .unwrap();

    let data_a = vec![1u8, 2, 3];
    let data_b = vec![1u16, 2, 3];
    let data_c = vec![1u32, 2, 3];
    let data_d = vec![1u64, 2, 3];
    let data_i = vec!["abc", "efg", "hello world"];
    // @fandahao17 TODO: Add date types
    // let data_naive = vec![
    //     NaiveDate::from_ymd(2010, 1, 1).and_hms(1, 1, 1),
    //     NaiveDate::from_ymd(2011, 2, 28).and_hms(2, 5, 6),
    //     NaiveDate::from_ymd(2012, 2, 29).and_hms(23, 59, 59),
    // ];
    // let data_j = apply_offset(&data_naive, FixedOffset::west(11 * 3600 + 45 * 60));

    for i in 0..data_a.len() {
        conn.query_drop(&format!(
            "insert into test_t3 values ({}, {}, {}, {}, '{}')",
            data_a[i], data_b[i], data_c[i], data_d[i], data_i[i]
        ))
        .unwrap();
    }

    // @fandahao17 TODO: Add date types
    // let dates = vec![
    //     NaiveDate::from_ymd(2010, 1, 1).and_hms(1, 1, 1),
    //     NaiveDate::from_ymd(2011, 2, 28).and_hms(2, 5, 6),
    //     NaiveDate::from_ymd(2012, 2, 29).and_hms(23, 59, 59),
    // ];
    // let dates = apply_offset(&dates, FixedOffset::west(11 * 3600 + 45 * 60));

    {
        let sql = "insert into function remote('127.0.0.1:9528', test_remote_func) select * from test_remote_func";
        let _query_result = conn.query_iter(sql).unwrap();
    }
    {
        let sql = "select count(a), count(b), count(c), count(d), count(i), count(j) from test_remote_func";
        let query_result = conn.query_iter(sql).unwrap();

        for block in query_result {
            let row = block.unwrap();
            let agg_res = row.get::<u64, _>(0).unwrap() as i64;
            assert_eq!(agg_res, 6);
            let agg_res = row.get::<u64, _>(1).unwrap() as i64;
            assert_eq!(agg_res, 6);
            let agg_res = row.get::<u64, _>(2).unwrap() as i64;
            assert_eq!(agg_res, 6);
            let agg_res = row.get::<u64, _>(3).unwrap() as i64;
            assert_eq!(agg_res, 6);
            let agg_res = row.get::<u64, _>(4).unwrap() as i64;
            assert_eq!(agg_res, 6);
            let agg_res = row.get::<u64, _>(5).unwrap() as i64;
            assert_eq!(agg_res, 6);
        }
    }
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
async fn tests_mysql_integ_date_time_functions() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    conn.query_drop(format!("DROP TABLE IF EXISTS test_tab_date"))
        .unwrap();
    conn.query_drop(format!(
        "CREATE TABLE test_tab_date( \
            a Date, \
            b DateTime, \
            c String, \
            ct String, \
            d Int64, \
            dt Int64, \
            e DateTime('Etc/GMT+5'), \
            f DateTime('-11:45') \
        )"
    ))
    .unwrap();

    let data_a = vec![
        Utc.ymd(2010, 1, 1),
        Utc.ymd(2011, 2, 28),
        Utc.ymd(2012, 2, 29),
        Utc.ymd(2012, 3, 4),
        Utc.ymd(2021, 8, 31),
        Utc.ymd(2021, 6, 27),
    ];

    let data_a_datetime: Vec<_> = data_a
        .iter()
        .map(|date| {
            Tz::Etc__GMTMinus8
                .from_local_date(&date.naive_utc())
                .unwrap()
                .and_hms(0, 0, 0)
        })
        .collect();

    let data_naive = vec![
        NaiveDate::from_ymd(2010, 1, 1).and_hms(1, 1, 1),
        NaiveDate::from_ymd(2011, 2, 28).and_hms(2, 5, 6),
        NaiveDate::from_ymd(2012, 2, 29).and_hms(23, 59, 59),
        NaiveDate::from_ymd(2012, 3, 4).and_hms(5, 6, 7),
        NaiveDate::from_ymd(2021, 8, 31).and_hms(14, 32, 3),
        NaiveDate::from_ymd(2021, 6, 27).and_hms(17, 44, 32),
    ];

    let data_b = data_naive
        .iter()
        .map(|t| Utc.from_utc_datetime(t))
        .collect::<Vec<_>>();
    let data_e = data_b.clone();
    let data_f = data_b.clone();

    let data_b_check = apply_offset(&data_naive, Tz::Etc__GMTMinus8);
    let data_e_check = apply_offset(&data_naive, Tz::Etc__GMTPlus5);
    let data_f_check = apply_offset(&data_naive, FixedOffset::west(11 * 3600 + 45 * 60));

    let data_c = vec![
        "2010-1-1",
        "2011-2-28",
        "2012-02-29",
        "2012-03-4",
        "2021-8-31",
        "2021-6-27",
    ];

    let data_ct = vec![
        "2010-01-01 01:01:01",
        "2011-02-28 02:05:06",
        "2012-02-29 23:59:59",
        "2012-03-04 05:06:07",
        "2021-08-31 14:32:03",
        "2021-06-27 17:44:32",
    ];

    let data_d = vec![14610_i64, 15033, 15399, 15403, 18870, 18805];
    let data_dt: Vec<_> = data_b.iter().map(DateTime::timestamp).collect();

    let years = vec![2010, 2011, 2012, 2012, 2021, 2021];
    let months = vec![1, 2, 2, 3, 8, 6];
    let quarters = vec![1, 1, 1, 1, 3, 2];
    let day_of_years = vec![1, 59, 60, 64, 243, 178];
    let day_of_months = vec![1, 28, 29, 4, 31, 27];
    let day_of_weeks = vec![5, 1, 3, 7, 2, 7];
    let hours = vec![1, 2, 23, 5, 14, 17];
    let minutes = vec![1, 5, 59, 6, 32, 44];
    let seconds = vec![1, 6, 59, 7, 3, 32];

    for i in 0..6 {
        conn.query_drop(format!(
            "insert into test_tab_date values ('{}', '{}', '{}', '{}', {}, {}, '{}', '{}')",
            data_a[i].naive_local(),
            data_b[i].naive_local(),
            data_c[i],
            data_ct[i],
            data_d[i],
            data_dt[i],
            data_e[i].naive_local(),
            data_f[i].naive_local()
        ))
        .unwrap();
    }
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
            toDate(a), toDate(b), toDate(c), toDate(d), toDate(e), toDate(f), \
            toDateTime(a), toDateTime(b), toDateTime(ct), \
            toDateTime(dt), toDateTime(e), toDateTime(f) \
        from test_tab_date";
        let mut query_result = conn.query_iter(sql).unwrap();

        let mut i = 0;
        while let Some(block) = query_result.next() {
            let row = block.unwrap();
            println!("{:?}", row);
            let mut iter = 0..;
            let year_a: u16 = row.get(iter.next().unwrap()).unwrap();
            let year_b: u16 = row.get(iter.next().unwrap()).unwrap();
            let year_e: u16 = row.get(iter.next().unwrap()).unwrap();
            let year_f: u16 = row.get(iter.next().unwrap()).unwrap();
            let month_a: u8 = row.get(iter.next().unwrap()).unwrap();
            let month_b: u8 = row.get(iter.next().unwrap()).unwrap();
            let month_e: u8 = row.get(iter.next().unwrap()).unwrap();
            let month_f: u8 = row.get(iter.next().unwrap()).unwrap();
            let day_of_year_a: u16 = row.get(iter.next().unwrap()).unwrap();
            let day_of_year_b: u16 = row.get(iter.next().unwrap()).unwrap();
            let day_of_year_e: u16 = row.get(iter.next().unwrap()).unwrap();
            let day_of_year_f: u16 = row.get(iter.next().unwrap()).unwrap();
            let day_of_month_a: u8 = row.get(iter.next().unwrap()).unwrap();
            let day_of_month_b: u8 = row.get(iter.next().unwrap()).unwrap();
            let day_of_month_e: u8 = row.get(iter.next().unwrap()).unwrap();
            let day_of_month_f: u8 = row.get(iter.next().unwrap()).unwrap();
            let day_of_week_a: u8 = row.get(iter.next().unwrap()).unwrap();
            let day_of_week_b: u8 = row.get(iter.next().unwrap()).unwrap();
            let day_of_week_e: u8 = row.get(iter.next().unwrap()).unwrap();
            let day_of_week_f: u8 = row.get(iter.next().unwrap()).unwrap();
            let quarter_a: u8 = row.get(iter.next().unwrap()).unwrap();
            let quarter_b: u8 = row.get(iter.next().unwrap()).unwrap();
            let quarter_e: u8 = row.get(iter.next().unwrap()).unwrap();
            let quarter_f: u8 = row.get(iter.next().unwrap()).unwrap();
            let hour_b: u8 = row.get(iter.next().unwrap()).unwrap();
            let minute_b: u8 = row.get(iter.next().unwrap()).unwrap();
            let second_b: u8 = row.get(iter.next().unwrap()).unwrap();
            let hour_e: u8 = row.get(iter.next().unwrap()).unwrap();
            let minute_e: u8 = row.get(iter.next().unwrap()).unwrap();
            let second_e: u8 = row.get(iter.next().unwrap()).unwrap();
            let hour_f: u8 = row.get(iter.next().unwrap()).unwrap();
            let minute_f: u8 = row.get(iter.next().unwrap()).unwrap();
            let second_f: u8 = row.get(iter.next().unwrap()).unwrap();
            let to_date_a: NaiveDate = row.get(iter.next().unwrap()).unwrap();
            let to_date_b: NaiveDate = row.get(iter.next().unwrap()).unwrap();
            let to_date_c: NaiveDate = row.get(iter.next().unwrap()).unwrap();
            let to_date_d: NaiveDate = row.get(iter.next().unwrap()).unwrap();
            let to_date_e: NaiveDate = row.get(iter.next().unwrap()).unwrap();
            let to_date_f: NaiveDate = row.get(iter.next().unwrap()).unwrap();
            let datetime_a: NaiveDateTime = row.get(iter.next().unwrap()).unwrap();
            let datetime_b: NaiveDateTime = row.get(iter.next().unwrap()).unwrap();
            let datetime_c: NaiveDateTime = row.get(iter.next().unwrap()).unwrap();
            let datetime_d: NaiveDateTime = row.get(iter.next().unwrap()).unwrap();
            let datetime_e: NaiveDateTime = row.get(iter.next().unwrap()).unwrap();
            let datetime_f: NaiveDateTime = row.get(iter.next().unwrap()).unwrap();
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
            assert_eq!(to_date_a, data_a[i].naive_utc());
            assert_eq!(to_date_b, data_a[i].naive_utc());
            assert_eq!(to_date_c, data_a[i].naive_utc());
            assert_eq!(to_date_d, data_a[i].naive_utc());
            assert_eq!(to_date_e, data_a[i].naive_utc());
            assert_eq!(to_date_f, data_a[i].naive_utc());
            assert_eq!(datetime_a, data_a_datetime[i].naive_utc());
            assert_eq!(datetime_b, data_b_check[i].naive_utc());
            assert_eq!(datetime_c, data_b_check[i].naive_utc());
            assert_eq!(datetime_d, data_b[i].naive_utc());
            assert_eq!(datetime_e, data_e_check[i].naive_utc());
            assert_eq!(datetime_f, data_f_check[i].naive_utc());
            i += 1;
        }
    }
}

#[tokio::test]
#[ignore = "MySQL server currently does not support uuid types"]
async fn tests_mysql_integ_uuid() {}

#[tokio::test]
async fn tests_mysql_integ_select_all() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    conn.query_drop(format!("drop table if exists test1_tab"))
        .unwrap();
    conn.query_drop(format!("create table test1_tab(a UInt64, b UInt64)"))
        .unwrap();
    conn.query_drop(format!("insert into test1_tab values(1,1),(2,2)"))
        .unwrap();

    {
        let sql = "select * from test1_tab where b = 1";
        let query_result = conn.query_iter(sql).unwrap();

        for block in query_result {
            let row = block.unwrap();
            let len = row.columns_ref().len();
            assert_eq!(len, 2);
        }
    }

    conn.query_drop("drop database if exists test_db").unwrap();
}

#[tokio::test]
async fn tests_mysql_integ_partition_prune() {
    let pool = get_tb_mysql_pool();
    let mut conn = pool.get_conn().unwrap();

    conn.query_drop("create database if not exists test_db")
        .unwrap();
    conn.query_drop("use test_db").unwrap();

    conn.query_drop(format!("drop table if exists test1_tab"))
        .unwrap();
    conn.query_drop(format!(
        "create table test1_tab(a UInt64, b UInt64) engine=BaseStorage partition by a"
    ))
    .unwrap();
    conn.query_drop(format!("insert into test1_tab values(1,1),(2,2)"))
        .unwrap();

    {
        let sql = "select a from test1_tab where b = 1";
        let query_result = conn.query_iter(sql).unwrap();
        assert_eq!(query_result.count(), 1);
    }

    conn.query_drop(format!("drop table if exists test2_tab"))
        .unwrap();
    conn.query_drop(format!(
        "create table test2_tab (a UInt32, b UInt32) engine=BaseStorage partition by a"
    ))
    .unwrap();

    conn.query_drop(format!("insert into test2_tab values(1,1),(2,2)"))
        .unwrap();
    {
        let sql = "select * from test2_tab where a<>1";
        let query_result = conn.query_iter(sql).unwrap();
        assert_eq!(query_result.count(), 1);
    }

    conn.query_drop("drop database if exists test_db").unwrap();
}
