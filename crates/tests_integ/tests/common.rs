#![allow(dead_code)]
use client::prelude::{Options, Pool};
use std::convert::TryInto;
use std::env;
use url::Url;
use mysql::prelude::*;
use mysql::*;
use mysql::{Opts as MyOpts, Pool as MyPool};

pub fn db_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| {
        "tcp://localhost:9528?execute_timeout=5s&query_timeout=20s&pool_max=4&compression=lz4".into()
    })
}
pub fn ch_url() -> String {
    env::var("DATABASE_URL").unwrap_or_else(|_| {
        "tcp://default:@localhost:9000?execute_timeout=5s&query_timeout=20s&pool_max=4&compression=lz4".into()
    })
}

pub fn get_mysql_pool() -> MyPool {
    let mut opt = OptsBuilder::new();
    opt = opt.ip_or_hostname("127.0.0.1".into());
    opt = opt.tcp_port(3306);
    opt = opt.user("root".into());
    opt = opt.pass("test".into());
    MyPool::new(opt).expect("mysql pool")
}

pub fn get_config() -> Options {
    let database_url = db_url();
    database_url.try_into().unwrap()
}

pub fn get_ch_config() -> Options {
    let database_url = ch_url();
    database_url.try_into().unwrap()
}

/// Replace connection parameters in
/// the environment with specified in `url` argument
/// Preserve host,port,database,user info
pub fn get_pool_extend(url: &str) -> Pool {
    let mut config_url = Url::parse(db_url().as_str()).unwrap();
    let url = Url::parse(url).unwrap();

    config_url.set_query(url.query());
    Pool::create(config_url).unwrap()
}

pub fn get_pool() -> Pool {
    // let url = db_url();
    let opts = get_config();
    Pool::create(opts).unwrap()
}

pub fn get_ch_pool() -> Pool {
    // let url = db_url();
    let opts = get_ch_config();
    Pool::create(opts).unwrap()
}
