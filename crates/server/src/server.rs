#![feature(once_cell)]

use std::{env, io};

use actix_rt::net::TcpStream;
use actix_server::Server;
use actix_service::fn_service;
use baselog::{ConfigBuilder, LevelFilter, TermLogger, TerminalMode};
use bytes::BytesMut;
use log::info;
use runtime::{
    mgmt::{BMS, READ, REMOTE_READ, WRITE},
    read::{query, remote_query},
    write::write_block,
};
use server::BaseSrvConn;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[actix_rt::main]
async fn main() -> io::Result<()> {
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

    #[cfg(debug_assertions)]
    info!("[Base] built in debug mode");

    #[cfg(not(debug_assertions))]
    info!("[Base] built in release mode");

    let conf = &BMS.conf;
    let srv_addr = [
        conf.server.ip_addr.as_str(),
        conf.server.port.to_string().as_str(),
    ]
    .join(":");

    //init
    READ.get_or_init(|| query);
    WRITE.get_or_init(|| write_block);
    REMOTE_READ.get_or_init(|| remote_query);

    // start http server
    Server::build()
        .backlog(1024)
        .bind("base_srv", srv_addr, || {
            fn_service(|io: TcpStream| BaseSrvConn {
                io,
                read_buf: BytesMut::with_capacity(4096),
                write_buf: BytesMut::with_capacity(4096),
                conn_ctx: Default::default(),
                // data_packets_processed: false,
                // use_db: "default".to_string(),
            })
        })?
        .start()
        .await
}
