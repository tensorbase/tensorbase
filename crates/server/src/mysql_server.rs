#![feature(once_cell)]

use baselog::{ConfigBuilder, LevelFilter, TermLogger, TerminalMode};
use server_mysql::*;
use tokio::net::TcpListener;

use runtime::{
    mgmt::{BMS, READ, REMOTE_READ, WRITE},
    mysql::MysqlConn,
    read::{query, remote_query},
    write::write_block,
};
use std::env;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() {
    let enable_dbg_log = match env::var("enable_dbg_log") {
        Ok(_v) => true,
        Err(_e) => false,
    };
    #[allow(unused_must_use)]
    {
        TermLogger::init(
            if enable_dbg_log {
                LevelFilter::Debug
            } else {
                LevelFilter::Info
            },
            ConfigBuilder::new()
                .add_filter_ignore_str("sled")
                .add_filter_ignore_str("cranelift")
                .build(),
            TerminalMode::Mixed,
        );
    }

    //init
    READ.get_or_init(|| query);
    WRITE.get_or_init(|| write_block);
    REMOTE_READ.get_or_init(|| remote_query);

    let conf = &BMS.conf;
    let tcp_server_conf = conf.server.tcp.as_ref().unwrap();
    let srv_addr = [
        tcp_server_conf.ip_addr.as_str(),
        tcp_server_conf.port.to_string().as_str(),
    ]
    .join(":");

    let listener = TcpListener::bind(srv_addr.clone()).await.unwrap();
    // let port = listener.local_addr().unwrap().port();

    log::info!("Server listening on {:?}", srv_addr);

    while let Ok((s, addr)) = listener.accept().await {
        log::info!("Got connection from {:?}", addr);
        tokio::spawn(async move {
            AsyncMysqlIntermediary::run_on(MysqlConn::default(), s)
                .await
                .unwrap_or_else(|err| log::error!("{:?}", err))
        });
    }
}
