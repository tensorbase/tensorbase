use baselog::{ConfigBuilder, LevelFilter, TermLogger, TerminalMode};
use log;
use mysql::prelude::*;
use mysql::*;

fn main() {
    // we connect using MySQL bindings, but no MySQL server is running!
    let srv_addr = "localhost:9528";
    let mut db =
        mysql::Conn::new(Opts::from_url(&format!("mysql://{}", srv_addr)).unwrap())
            .unwrap();

    #[allow(unused_must_use)]
    {
        TermLogger::init(
            LevelFilter::Info,
            ConfigBuilder::new().build(),
            TerminalMode::Mixed,
        );
    }

    assert_eq!(db.ping(), true);
    {
        let cmd = "CREATE DATABASE IF NOT EXISTS test";
        db.query_drop(cmd).unwrap();
        log::info!("Command '{}' success", cmd);
    }
    {
        let cmd = "USE test";
        db.query_drop(cmd).unwrap();
        log::info!("Command '{}' success", cmd);
    }
    {
        let cmd = "DROP TABLE IF EXISTS test_tab";
        db.query_drop(cmd).unwrap();
        log::info!("Command '{}' success", cmd);
    }
    {
        let cmd = "CREATE TABLE test_tab(foo UInt64)";
        db.query_drop(cmd).unwrap();
        log::info!("Command '{}' success", cmd);
    }
    {
        let cmd = "INSERT INTO test_tab VALUES (1), (2), (3)";
        db.query_drop(cmd).unwrap();
        log::info!("Command '{}' success", cmd);
        // TODO: Check how many rows are affected?
    }
    {
        let cmd = "select foo from test_tab";
        let mut results = db.query_iter(cmd).unwrap();
        {
            let cols = results.columns();
            let cols = cols.as_ref();
            assert_eq!(cols.len(), 1);
            assert_eq!(cols[0].name_str(), "foo");

            let one = results.next();
            assert!(one.is_some());
            if let Some(row) = one {
                let row = row.unwrap();
                assert_eq!(row.len(), 1);
                let one = row.get::<u64, _>(0).unwrap();
                assert_eq!(one, 1);
            }

            let one = results.next();
            assert!(one.is_some());
            if let Some(row) = one {
                let row = row.unwrap();
                assert_eq!(row.len(), 1);
                let one = row.get::<u64, _>(0).unwrap();
                assert_eq!(one, 2);
            }

            let one = results.next();
            assert!(one.is_some());
            if let Some(row) = one {
                let row = row.unwrap();
                assert_eq!(row.len(), 1);
                let one = row.get::<u64, _>(0).unwrap();
                assert_eq!(one, 3);
            }
        }
        log::info!("Command '{}' success", cmd);
    }
    {
        let cmd = "select sum(foo) from test_tab";
        let mut results = db.query_iter(cmd).unwrap();
        let one = results.next();
        assert!(one.is_some());
        if let Some(row) = one {
            let row = row.unwrap();
            assert_eq!(row.len(), 1);
            let one = row.get::<u64, _>(0).unwrap();
            assert_eq!(one, 6);
        }
        assert_eq!(results.count(), 0);
        log::info!("Command '{}' success", cmd);
    }
    drop(db);
}
