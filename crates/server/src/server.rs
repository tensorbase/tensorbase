#![feature(once_cell)]

use std::{
    env,
    fs::File,
    io::{self, BufReader},
    path::Path,
    sync::Arc,
};

use baselog::{ConfigBuilder, LevelFilter, TermLogger, TerminalMode};
use bytes::BytesMut;
use futures::{future, FutureExt};
use log::info;
use meta::confs::{Tcp, Tls};
use runtime::{
use runtime::mgmt::BMS;
use server::BaseSrvConn;
use tokio::net::TcpListener;
use tokio_rustls::rustls::internal::pemfile::{certs, rsa_private_keys};
use tokio_rustls::rustls::{Certificate, NoClientAuth, PrivateKey, ServerConfig};
use tokio_rustls::TlsAcceptor;

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
    let mut servers = vec![];
    if let Some(tcp) = &conf.server.tcp {
        log::info!("server: {:?}", tcp);
        servers.push(fut_tcp_srv(tcp).boxed());
    }
    if let Some(tls) = &conf.server.tls {
        log::info!("server: {:?}", tls);
        servers.push(fut_tls_srv(tls).boxed());
    }
    if servers.is_empty() {
        panic!("can not find a valid server setup in conf!");
    }
    future::try_join_all(servers).await?;

    Ok(())
}

fn load_certs(path: &Path) -> io::Result<Vec<Certificate>> {
    certs(&mut BufReader::new(File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid cert"))
}

fn load_keys(path: &Path) -> io::Result<Vec<PrivateKey>> {
    rsa_private_keys(&mut BufReader::new(File::open(path)?))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid key"))
}

async fn fut_tcp_srv(tcp: &Tcp) -> io::Result<()> {
    let srv_addr = [tcp.ip_addr.as_str(), tcp.port.to_string().as_str()].join(":");
    let listener = TcpListener::bind(srv_addr).await?;

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

async fn fut_tls_srv(tls: &Tls) -> io::Result<()> {
    let srv_addr = [tls.ip_addr.as_str(), tls.port.to_string().as_str()].join(":");
    let path_cert = Path::new(&tls.certificate_file);
    let path_pkey = Path::new(&tls.private_key_file);
    let certs = load_certs(&path_cert)?;
    let mut keys = load_keys(&path_pkey)?;
    let mut config = ServerConfig::new(NoClientAuth::new());
    config
        .set_single_cert(certs, keys.remove(0))
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind(srv_addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let tls = acceptor.accept(stream).await?;
        tokio::spawn(BaseSrvConn {
            io: tls,
            read_buf: BytesMut::with_capacity(4096),
            write_buf: BytesMut::with_capacity(4096),
            conn_ctx: Default::default(),
        });
    }
}
