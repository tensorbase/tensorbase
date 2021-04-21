use baselog::{Config, LevelFilter, TermLogger, TerminalMode};
use meta::{confs::Conf, types::BqlType};
use runtime::{
    errs::{BaseRtError, BaseRtResult},
    mgmt::{BaseCommandKind, BaseMgmtSys},
};
use test_utils::prepare_empty_tmp_dir;

fn prepare_bms<'a>() -> BaseRtResult<BaseMgmtSys<'a>> {
    #[allow(unused_must_use)]
    {
        TermLogger::init(
            LevelFilter::Info,
            Config::default(),
            TerminalMode::Mixed,
        );
    }

    let dir_test = prepare_empty_tmp_dir(Some("/jin/tmp/system_test"));
    let conf_test: Conf = toml::from_str(&format!(
        r#"[system]
        meta_dirs = ["{}/test_sys"]
        data_dirs = ["{}/test_data"]
        [server]
        ip_addr = "127.0.0.1"
        # port = 8080
    "#,
        &dir_test, &dir_test
    ))
    .unwrap();
    let conf = Box::new(conf_test);
    BaseMgmtSys::from_conf(Box::leak(conf))
}

#[test]
fn test_run_commands() -> BaseRtResult<()> {
    let bms = prepare_bms()?;
    let mut cctx = Default::default();

    let res = bms.run_commands("".to_string(), &mut cctx);
    assert!(matches!(res, Err(_)));

    let res = bms.run_commands("show something".to_string(), &mut cctx);
    assert!(matches!(
        res,
        Err(BaseRtError::WrappingLangError(
            lang::errs::LangError::ASTError
        ))
    ));

    let dbname = "xxx_123";
    let res = bms.run_commands(
        format!("create database if not exists {}", dbname),
        &mut cctx,
    );
    assert!(matches!(res, Ok(_))); //println!("{:?}", res);

    let res = bms.run_commands(
        format!("create database if not exists {}", dbname),
        &mut cctx,
    );
    assert!(matches!(res, Ok(_))); //println!("{:?}", res);

    let res =
        bms.run_commands(format!("create database {}", dbname), &mut cctx);
    assert!(matches!(
        res,
        Err(BaseRtError::WrappingMetaError(
            meta::errs::MetaError::EntityExistedError(_)
        ))
    ));

    // println!("{:?}", res);

    let res = bms.run_commands("show databases".to_string(), &mut cctx)?;
    assert!(matches!(res, BaseCommandKind::Query(_)));
    if let BaseCommandKind::Query(vbc) = res {
        assert!(vbc.len() == 1);
        assert!(vbc[0].ncols == 1);
        // assert!(vbc[0].nrows == 1);
        assert!(vbc[0].columns[0].data.btype == BqlType::String);
        assert!(vbc[0].columns[0].data.data.len() > 0);
    }

    let res = bms.run_commands(
        r#"CREATE TABLE IF NOT EXISTS payment11
    (
        `a` LowCardinality(String),
        `b` Nullable(UInt64)
    )
    ENGINE = BaseStorage"#.to_string(),
        &mut cctx,
    )?;

    assert!(matches!(res, BaseCommandKind::Create));

    assert_eq!("default", cctx.current_db);
    let res = bms.run_commands("use xxx_123".to_string(), &mut cctx)?;
    assert_eq!("xxx_123", cctx.current_db);

    let res = bms.run_commands("insert into default.payment11 values".to_string(), &mut cctx)?;
    assert!(matches!(res, BaseCommandKind::InsertFormatInline(_, _, _)));

    let res = bms.run_commands("insert into default.payment11 values ('a', 1), ('b', 2)".to_string(), &mut cctx)?;
    match res {
        BaseCommandKind::InsertFormatInlineValues(rows, _name, _id) => {
            assert_eq!(rows.ncols, 2);
            assert_eq!(rows.nrows, 2);
        }
        _ => panic!("res should match InsertFormatInlineValues")
    }

    Ok(())
}
