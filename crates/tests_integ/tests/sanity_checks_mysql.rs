mod common;
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

    // @fandahao17 TODO: Add support for String types
    // let mut conn = pool.get_conn().unwrap();
    // conn.query_drop("use test_insert_select_db").unwrap();
    // conn.query_drop("drop table if exists test_t3").unwrap();
    // conn.query_drop("drop table if exists test_t4").unwrap();
    // conn.query_drop("create table test_t3(a String)").unwrap();
    // conn.query_drop("create table test_t4(a String)").unwrap();

    // let data_a = vec!["aelvbs a1 233 🀄️", "b^&#*-['&**%%%", "c;;;;\n\t"];

    // for a in data_a {
    //     conn.query_drop(&format!("insert into test_t3 values ({})", a))
    //         .unwrap();
    // }

    // let mut conn = pool.get_conn().unwrap();
    // conn.query_drop("use test_insert_select_db").unwrap();
    // conn.query_drop("insert into test_t4 select * from test_t3")
    //     .unwrap();
    // conn.query_drop("insert into test_t4(a) select a from test_t3 order by a limit 1")
    //     .unwrap();
    // let query_result = conn.query_iter("select count(*) from test_t4").unwrap();

    // for block in query_result {
    //     let row = block.unwrap();
    //     assert_eq!(row.get::<u64, _>(0).unwrap(), 4);
    // }

    // let mut conn = pool.get_conn().unwrap();
    // conn.query_drop("use test_insert_select_db").unwrap();
    // let query_result = conn
    //     .query_iter("select * from test_t4 order by a limit 1")
    //     .unwrap();

    // for block in query_result {
    //     let row = block.unwrap();
    //     assert_eq!(row.get::<String, _>(0).unwrap(), "c;;;;\n\t");
    // }
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
#[ignore = "MySQL server currently does not support date types"]
async fn tests_mysql_integ_basic_insert_date() {}

#[tokio::test]
#[ignore = "MySQL server currently does not support string types"]
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
#[ignore = "MySQL server currently does not support string types"]
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
#[ignore = "MySQL server currently does not support string types"]
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
#[ignore = "MySQL server currently does not support string types"]
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
        conn.query_drop(&format!("insert into test_t3 values ('{}')", a))
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
#[ignore = "MySQL server currently does not support date types"]
async fn tests_mysql_integ_date_cast() {}

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

#[tokio::test]
#[ignore = "MySQL server currently does not support date types"]
async fn tests_mysql_integ_date_time_functions() {}

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
