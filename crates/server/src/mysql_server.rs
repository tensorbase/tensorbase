use baselog::{ConfigBuilder, LevelFilter, TermLogger, TerminalMode};
use runtime::mgmt::BMS;
use runtime::mysql::MysqlConn;
use server_mysql::*;
use tokio::net::TcpListener;

use std::env;

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

    let conf = &BMS.conf;
    let tcp_server_conf = conf.server.tcp.as_ref().unwrap();
    let srv_addr = [
        tcp_server_conf.ip_addr.as_str(),
        tcp_server_conf.port.to_string().as_str(),
    ]
    .join(":");

    let listener = TcpListener::bind(srv_addr.clone()).await.unwrap();
    // let port = listener.local_addr().unwrap().port();

    if let Ok((s, _)) = listener.accept().await {
        AsyncMysqlIntermediary::run_on(MysqlConn::default(), s)
            .await
            .unwrap();
    }
}
