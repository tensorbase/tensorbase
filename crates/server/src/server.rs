#![feature(once_cell)]

use std::{
    env,
    fs::File,
    io::{self, BufReader},
};

use actix_rt::net::TcpStream;
use actix_server::Server;
use actix_service::fn_service;
use baselog::{ConfigBuilder, LevelFilter, TermLogger, TerminalMode};
use bytes::BytesMut;
use futures::future;
use log::info;
use runtime::{
    mgmt::{BMS, READ, REMOTE_READ, WRITE},
    read::{query, remote_query},
    write::write_block,
};
use rustls::{
    internal::pemfile::certs, internal::pemfile::rsa_private_keys, NoClientAuth,
    ServerConfig,
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
    info!("{:?}", conf);

    //init
    READ.get_or_init(|| query);
    WRITE.get_or_init(|| write_block);
    REMOTE_READ.get_or_init(|| remote_query);

    // start servers
    let mut servers = vec![];
    if let Some(tcp_srv) = &conf.server.tcp {
        log::info!("server: {:?}", tcp_srv);
        let srv_addr =
            [tcp_srv.ip_addr.as_str(), tcp_srv.port.to_string().as_str()].join(":");
        servers.push(
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
                })
                .unwrap()
                .start(),
        );
    }
    if let Some(tls_srv) = &conf.server.tls {
        log::info!("server: {:?}", tls_srv);

        let mut tls_config = ServerConfig::new(NoClientAuth::new());
        // Load TLS key and cert files
        let cert_file = &mut BufReader::new(
            File::open(
                "/jin/tensorbase/ws/tensorbase/crates/server/tests/confs/certs/cert.pem",
            )
            .unwrap(),
        );
        let key_file = &mut BufReader::new(
            File::open(
                "/jin/tensorbase/ws/tensorbase/crates/server/tests/confs/certs/key.pem",
            )
            .unwrap(),
        );

        let cert_chain = certs(cert_file).unwrap();
        let mut keys = rsa_private_keys(key_file).unwrap();
        tls_config
            .set_single_cert(cert_chain, keys.remove(0))
            .unwrap();

        let _srv_addr =
            [tls_srv.ip_addr.as_str(), tls_srv.port.to_string().as_str()].join(":");
        todo!()
    }
    if servers.is_empty() {
        panic!("can not find a valid server setup in conf!");
    }
    future::try_join_all(servers).await?;

    Ok(())
}
