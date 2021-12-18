use std::io::prelude::*;
use std::io::BufReader;
use std::{fs::File, path::Path};

use client::prelude::{Options, Pool};
use mysql::prelude::*;
use mysql::{Opts as MyOpts, Pool as MyPool};
use std::convert::TryInto;
use std::env;
use url::Url;

// use tokio::prelude::*;

pub fn db_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| {
        "tcp://localhost:9528?execute_timeout=5s&query_timeout=20s&pool_max=4&compression=lz4".into()
    })
}

pub fn mysql_url() -> String {
    env::var("MYSQL_DATABASE_URL")
        .unwrap_or_else(|_| "mysql://test:test@localhost:3306/test".into())
}

pub fn get_config() -> Options {
    let database_url = db_url();
    database_url.try_into().expect("Pool::create err?")
}

/// Replace connection parameters in
/// the environment with specified in `url` argument
/// Preserve host,port,database,user info
pub fn get_pool_extend(url: &str) -> Pool {
    let mut config_url = Url::parse(db_url().as_str()).expect("Pool::create err?");
    let url = Url::parse(url).expect("Pool::create err?");

    config_url.set_query(url.query());
    Pool::create(config_url).expect("Pool::create err?")
}

pub fn get_pool() -> Pool {
    let url = db_url();
    Pool::create(url).expect("Pool::create err?")
}

pub fn get_mysql_pool() -> MyPool {
    MyPool::new(MyOpts::from_url(mysql_url().as_str()).expect("MySQL URL"))
        .expect("MySQL database url")
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let tn_opt = Some("basic_checks".to_string());

    let tests_root = env!("CARGO_MANIFEST_DIR");
    println!("tests_root: {}", tests_root);

    let pool = get_pool();
    let mut conn = pool.connection().await?;

    if let Some(tn) = tn_opt {
        let tst_path = Path::new(tests_root)
            .join("sql_test_scripts")
            .join([&tn, ".sql"].join(""));
        println!("tst_path: {:?}", tst_path);
        let file = File::open(tst_path)?;
        let mut buf_reader = BufReader::new(file);
        let mut contents = String::new();
        buf_reader.read_to_string(&mut contents)?;
        let mut sql = contents.as_str();
        let mut ct = 0;
        while let Some(inl) = sql.find("\n\n") {
            let stat = sql[..inl].trim();
            if inl == 0 {
                break;
            }
            println!("run statement#{}: {}\n", ct, stat);
            conn.execute(stat).await?;

            sql = &sql[(inl + 2)..];
            ct += 1;
        }
    } else {
        todo!()
    }

    let pool = get_mysql_pool();

    let mut conn = pool.get_conn().expect("MySQL pool connection");

    let r: Option<i32> = conn.query_first("select 1").expect("get one");
    assert_eq!(r, Some(1));

    Ok(())
}
