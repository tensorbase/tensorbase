use baselog::{ConfigBuilder, LevelFilter, TermLogger, TerminalMode};
use runtime::mgmt::BMS;
use runtime::mysql::MysqlConn;
use server_mysql::*;

use std::env;
use std::net;

fn main() {
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
    let srv_addr = [
        conf.server.ip_addr.as_str(),
        conf.server.port.to_string().as_str(),
    ]
    .join(":");

    let listener = net::TcpListener::bind(srv_addr.clone()).unwrap();
    // let port = listener.local_addr().unwrap().port();

    if let Ok((s, _)) = listener.accept() {
        MysqlIntermediary::run_on_tcp(MysqlConn::default(), s).unwrap();
    }
}
