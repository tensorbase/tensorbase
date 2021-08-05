#![feature(once_cell)]

use std::{
    env,
    fs::File,
    io::{self, BufReader},
};

use baselog::{ConfigBuilder, LevelFilter, TermLogger, TerminalMode};
use bytes::BytesMut;
use futures::future;
use log::info;
use runtime::{
    mgmt::{BMS, READ, REMOTE_READ, WRITE},
    read::{query, remote_query},
    write::write_block,
};
use server::BaseSrvConn;
use tokio::net::TcpListener;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
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
    info!("{:?}", conf);

    //init
    READ.get_or_init(|| query);
    WRITE.get_or_init(|| write_block);
    REMOTE_READ.get_or_init(|| remote_query);

    // start servers
    if let Some(tcp_srv) = &conf.server.tcp {
        log::info!("server: {:?}", tcp_srv);
        let srv_addr =
            [tcp_srv.ip_addr.as_str(), tcp_srv.port.to_string().as_str()].join(":");
        let mut listener = TcpListener::bind(srv_addr).await?;

        loop {
            let (io, _) = listener.accept().await?;
            tokio::spawn(BaseSrvConn {
                io,
                read_buf: BytesMut::with_capacity(4096),
                write_buf: BytesMut::with_capacity(4096),
                conn_ctx: Default::default(),
            });
        }
    }

    Ok(())
}
