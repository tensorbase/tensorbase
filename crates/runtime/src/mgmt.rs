use arrow::{
    array::{LargeStringArray, LargeStringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use base::bytes_cat;
use base::{
    codec::encode_ascii_bytes_vec_short,
    datetimes::{parse_to_epoch, TimeZoneId},
    mem::SyncPointer,
    strings::s,
};
use basejit::jit;
use bytes::BytesMut;
use clap::{App, Arg};
use client::prelude::Pool;
use client::prelude::PoolBuilder;
use dashmap::DashMap;
use lang::parse::RemoteAddr;
use lang::parse::{
    parse_command, parse_create_database, parse_create_table, parse_desc_table,
    parse_drop_database, parse_drop_table, parse_insert_into, parse_optimize_table,
    parse_show_create_table, parse_table_place, seek_to_sub_cmd, Pair, Rule,
    TablePlaceKind, TablePlaceKindContext,
};
use meta::{
    confs::Conf,
    errs::MetaError,
    store::{parts::PartStore, sys::MetaStore},
    toml,
    types::{BaseChunk, BqlType, Id},
};
use mysql::{Compression, OptsBuilder, Pool as MyPool, SslOpts};
use std::net::IpAddr;
use std::time::Duration;
use std::{
    convert::TryInto,
    env,
    fs::remove_dir_all,
    lazy::SyncLazy,
    panic::panic_any,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{
    errs::{BaseRtError, BaseRtResult},
    read::{query, remote_query},
    types::{BaseColumn, BaseDataBlock, BaseServerConn, BaseWriteAware},
    write::write_block,
};
use datafusion::physical_plan::clickhouse::DEFAULT_TIMEZONE;

pub struct BaseHasher {
    state: u64,
}

impl std::hash::Hasher for BaseHasher {
    fn write(&mut self, bytes: &[u8]) {
        use base::hash::Hasher;
        self.state = bytes.hash();
    }

    fn finish(&self) -> u64 {
        self.state
    }
}
#[derive(Clone)]
pub struct BuildPtkExprsHasher;
impl std::hash::BuildHasher for BuildPtkExprsHasher {
    type Hasher = BaseHasher;
    fn build_hasher(&self) -> BaseHasher {
        BaseHasher { state: 0 }
    }
}

pub static BMS: SyncLazy<BaseMgmtSys> = SyncLazy::new(|| {
    let args: Vec<String> = match env::var("BASE_DBG_CONF_OVERRIDE") {
        Ok(conf_path) => {
            vec![
                "target/debug/server".to_string(),
                "-c".to_string(),
                conf_path,
            ]
        }
        _ => env::args().collect(),
    };
    // log::debug!("args: {:?}", args);

    let matches = App::new("TensorBase Server")
    .version("1.0")
    .author("TensorBase")
    .about("TensorBase Server")
    .arg(
        Arg::new("conf")
            .short('c')
            .about("Specify the conf file to use")
            .takes_value(true)
    )
    .arg(
        Arg::new("conf_string")
            .short('s')
            .about("Specify the string as conf to use, which makes pure-shell users' lifes more easier.")
            // .required_unless_present("conf")
            .takes_value(true)
    )
    .get_matches_from(args);
    let mut conf_opt: Option<Conf>;
    if let Some(conf_path) = matches.value_of("conf") {
        conf_opt = Conf::load(Some(conf_path));
        if conf_opt == None {
            panic_any(format!(
                "The conf file pointed as the path [{}] does not exist!",
                conf_path
            ));
        }
    } else {
        conf_opt = Conf::load(None);
    }
    if let Some(conf_string) = matches.value_of("conf_string") {
        let conf0: Conf =
            toml::from_str(conf_string).expect("Can not parse your provided conf string");
        conf_opt = Some(conf0);
    }
    let conf: Conf = if let Some(c) = conf_opt {
        c
    } else {
        panic!(
            "Can not load configurations via default location (conf/base.conf) or command-line arguments"
        )
    };
    //check existence
    for dd in &conf.system.data_dirs {
        if !Path::new(dd).exists() {
            panic_any(format!("data_dir [{}] does not existed", dd))
        }
    }

    log::info!("confs to use:");
    log::info!("system.meta_dirs: {:?}", &conf.system.meta_dirs);
    log::info!("system.data_dirs: {:?}", &conf.system.data_dirs);

    let conf = Box::new(conf);
    let mut bms = BaseMgmtSys::from_conf(Box::leak(conf)).unwrap();
    bms.build_remote_db_pool();
    bms.build_mysql_db_pool();
    bms
});

pub static EXPR_JIT: SyncLazy<Mutex<jit::JIT>> =
    SyncLazy::new(|| Mutex::new(jit::JIT::default()));

//FIXME
#[derive(Debug)]
pub enum BaseCommandKind {
    Default,
    Create,
    Drop,
    Query(Vec<RecordBatch>), //FIXME need iterator for big return
    InsertFormatInline(BaseDataBlock, String, Id),
    InsertFormatInlineValues,
    InsertFormatCSV(BaseDataBlock, String, Id),
    InsertFormatSelectValue,
    Optimize,
}

impl Default for BaseCommandKind {
    fn default() -> Self {
        BaseCommandKind::Default
    }
}

// #[derive(Debug, Default)]
// pub struct RunCommandCtx {
//     pub query_id: String
// }
#[allow(non_snake_case)]
fn ZERO_PART_KEY_EXPR_FN() -> u64 {
    0
}

pub struct BaseMgmtSys<'a> {
    pub conf: Pin<&'a Conf>,
    pub meta_store: MetaStore,
    pub part_store: PartStore<'a>,
    pub remote_db_pool: DashMap<RemoteAddr, Pool>,
    pub remote_mysql_pool: DashMap<RemoteAddr, MyPool>,
    pub ptk_exprs_reg: DashMap<Id, SyncPointer<u8>, BuildPtkExprsHasher>,
    pub timezone: TimeZoneId,
    pub timezone_name: String,
}

impl<'a> BaseMgmtSys<'a> {
    pub fn from_conf(conf: &'a Conf) -> BaseRtResult<Self> {
        let ms_path = conf.system.meta_dirs.as_slice();
        let meta_store =
            MetaStore::new(ms_path).map_err(|e| BaseRtError::WrappingMetaError(e))?;
        let part_store = PartStore::new(ms_path, &conf.system.data_dirs)?;
        let timezone = match &conf.system.timezone {
            Some(tz_name) => TimeZoneId::from_str(&tz_name)?,
            _ => TimeZoneId::from_local().unwrap_or_default(),
        };
        DEFAULT_TIMEZONE.get_or_init(|| timezone);
        let timezone_name = timezone.name_ch();
        log::info!("current timezone sets to {}", timezone_name);

        //prepare two system level databases
        let res = meta_store.new_db("system");
        match res {
            Ok(tid) => {
                //numbers
                let cmd = "CREATE TABLE system.numbers
                (
                    number Int64
                )
                ENGINE = BaseStorage
                ";
                let p = BaseMgmtSys::parse_cmd_as_pair(cmd)?;
                let (mut t, _fallible) = parse_create_table(p)
                    .map_err(|e| BaseRtError::WrappingLangError(e))?;
                t.tab_info.create_script = cmd.to_string();
                let res = meta_store.create_table(&t);
                match res {
                    Err(e) => {
                        return Err(BaseRtError::WrappingMetaError(e));
                    }
                    _ => log::info!("database `system` created"), //[dbid={}]
                }
            }
            Err(MetaError::EntityExistedError(tid)) => {
                log::info!("database `system` existed") //[dbid={}]
            }
            Err(e) => return Err(BaseRtError::WrappingMetaError(e)),
        }
        let res = meta_store.new_db("default");
        match res {
            Ok(tid) => log::info!("database `default` created"), //[dbid={}]
            Err(MetaError::EntityExistedError(tid)) => {
                log::info!("database `default` existed") //[dbid={}]
            }
            Err(e) => return Err(BaseRtError::WrappingMetaError(e)),
        }
        let jit = jit::JIT::default();
        let ptk_exprs_reg =
            DashMap::<Id, SyncPointer<u8>, BuildPtkExprsHasher>::with_hasher(
                BuildPtkExprsHasher,
            );
        //
        Ok(BaseMgmtSys {
            conf: Pin::new(conf),
            meta_store,
            part_store,
            remote_db_pool: DashMap::new(),
            remote_mysql_pool: DashMap::new(),
            ptk_exprs_reg,
            timezone,
            timezone_name,
        })
    }

    pub fn build_mysql_db_pool(&mut self) {
        if let Some(remote_tables) = self.conf.remote_tables.as_ref() {
            if let Some(my) = remote_tables.mysql.as_ref() {
                for conf in &my.members {
                    let mut opt = OptsBuilder::new();

                    opt =
                        opt.ip_or_hostname(conf.ip_addr.as_ref().or(conf.host.as_ref()));

                    opt = opt.tcp_port(conf.port);

                    if let Some(username) = conf.username.as_ref() {
                        opt = opt.user(username.into());
                    }

                    if let Some(password) = conf.password.as_ref() {
                        opt = opt.pass(password.into());
                    }

                    if let Some(db) = conf.database.as_ref() {
                        opt = opt.db_name(db.into());
                    }

                    opt = opt.compress(conf.compress.map(|c| Compression::new(c)));

                    opt = opt.secure_auth(conf.secure_auth);

                    opt = opt.read_timeout(
                        conf.read_timeout.map(|n| Duration::from_millis(n)),
                    );

                    opt = opt.tcp_keepalive_time_ms(conf.tcp_keepalive);
                    opt = opt.tcp_nodelay(conf.tcp_nodelay);

                    match (
                        conf.ssl_pkcs12_path.as_ref(),
                        conf.ssl_root_ca_path.as_ref(),
                        conf.ssl_password.as_ref(),
                    ) {
                        (Some(pk), Some(ca), pass) => {
                            let mut ssl_opt = SslOpts::default();
                            let mut pk_path = PathBuf::new();
                            pk_path.push(pk);
                            let mut ca_path = PathBuf::new();
                            ca_path.push(ca);
                            ssl_opt = ssl_opt.with_pkcs12_path(Some(pk_path));
                            ssl_opt = ssl_opt.with_root_cert_path(Some(ca_path));
                            ssl_opt = ssl_opt.with_password(pass.map(|s| s.to_string()));
                            opt = opt.ssl_opts(ssl_opt);
                        }
                        _ => {}
                    };

                    let remote_addr = RemoteAddr {
                        ip_addr: conf.ip_addr.as_ref().map(|s| {
                            s.parse::<IpAddr>().expect("correct ipv4 or ipv6 address")
                        }),
                        host_name: conf.host.clone(),
                        port: Some(conf.port),
                    };

                    let pool =
                        MyPool::new(opt).expect("connect the remote mysql database");
                    log::info!("connect remote mysql database: {:?}", remote_addr);

                    self.remote_mysql_pool.insert(remote_addr, pool);
                }
            }
        }
    }

    pub fn build_remote_db_pool(&mut self) {
        if let Some(remote_tables) = self.conf.remote_tables.as_ref() {
            if let Some(ch) = remote_tables.clickhouse.as_ref() {
                for conf in &ch.members {
                    let mut builder = PoolBuilder::default();

                    if conf.ping {
                        builder = builder.with_ping();
                    }

                    if let Some(comp) = &conf.compression {
                        builder = builder.with_compression();
                    }

                    builder = builder.with_pool(conf.pool_min_size, conf.pool_max_size);

                    if let Some(username) = &conf.username {
                        builder = builder.with_username(username);
                    }

                    if let Some(password) = &conf.password {
                        builder = builder.with_password(password);
                    }

                    let addr = conf
                        .ip_addr
                        .as_ref()
                        .map(|i| i.to_string())
                        .or(conf.host.as_ref().map(|h| h.to_string()))
                        .map(|s| format!("{}:{}", s, conf.port))
                        .unwrap_or("".into());

                    builder = builder.add_addr(addr);
                    let pool = builder
                        .build()
                        .expect("initial remote connection pool failed");
                    let remote_addr = RemoteAddr {
                        ip_addr: conf.ip_addr.as_ref().map(|s| {
                            s.parse::<IpAddr>().expect("correct ipv4 or ipv6 address")
                        }),
                        host_name: conf.host.clone(),
                        port: Some(conf.port),
                    };
                    log::info!("connect remote ch/tb database: {:?}", remote_addr);
                    self.remote_db_pool.insert(remote_addr, pool);
                }
            }
        }
    }

    //===
    pub fn get_ptk_exps_fn_ptr(
        &self,
        qtn: &str,
        tid: Id,
        col_ptk_typ: BqlType,
    ) -> BaseRtResult<*const u8> {
        let fp_opt = self.ptk_exprs_reg.get(&tid);
        match fp_opt {
            Some(fp) => {
                let p = *fp;
                Ok(p.as_ptr())
            }
            None => {
                let rt = match self.meta_store.get_table_info_partition_keys_expr(tid)? {
                    Some(iv) => {
                        let ptk_expr = unsafe { std::str::from_utf8_unchecked(&*iv) };
                        // log::debug!("ptk_expr: {}", ptk_expr);
                        let ptk_expr = match col_ptk_typ {
                            BqlType::Date => ["date", ptk_expr].join("_"),
                            _ => ptk_expr.to_string(),
                        };

                        let mut ptc = String::new();
                        match self.meta_store.get_table_info_partition_cols(tid)? {
                            Some(iv) => ptc
                                .push_str(unsafe { std::str::from_utf8_unchecked(&*iv) }),
                            None => {}
                        };
                        //FIXME validate the expr and cols
                        //trim trailing comma if have
                        let ptc = if ptc.ends_with(",") {
                            &ptc[..ptc.len() - 1]
                        } else {
                            &ptc
                        };
                        let expr_name = qtn.replace(".", "_");
                        EXPR_JIT
                            .lock()
                            .map_err(|_| BaseRtError::LightJitCompilationError)?
                            .ensure_fn_redef(expr_name.as_str());

                        let mut fn_code = s!(
                        fn $expr_name$($ptc$) -> (r) {
                            r = $ptk_expr$
                        });
                        fn_code.push('\n');
                        log::debug!("To jit compile expr: {}", fn_code);

                        let fn_code_ptr = EXPR_JIT
                            .lock()
                            .map_err(|_| BaseRtError::LightJitCompilationError)?
                            .compile(fn_code.as_str())
                            .map_err(|_| BaseRtError::LightJitCompilationError)?; //FIXME possible memory leak
                        fn_code_ptr
                    }
                    None => ZERO_PART_KEY_EXPR_FN as *const u8,
                };
                self.ptk_exprs_reg.insert(tid, SyncPointer(rt));
                Ok(rt)
            }
        }
    }

    //=== commands ===

    pub fn command_create_database(&self, p: Pair<Rule>) -> BaseRtResult<()> {
        let di =
            parse_create_database(p).map_err(|e| BaseRtError::WrappingLangError(e))?;
        let ms = &self.meta_store;
        let dbid_opt = ms.new_db(di.dbname.as_str());
        match dbid_opt {
            Err(e) if di.fallible => Err(BaseRtError::WrappingMetaError(e)),
            _ => Ok(()),
        }
    }

    pub fn command_show_databases(&self) -> BaseRtResult<RecordBatch> {
        let ms = &self.meta_store;
        let bc = ms
            .get_all_databases()
            .map_err(|e| BaseRtError::WrappingMetaError(e))?;

        let schema = Schema::new(vec![Field::new("name", DataType::LargeUtf8, false)]);
        Ok(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(bc)])?)
    }

    pub fn command_show_tables(&self, dbname: &str) -> BaseRtResult<RecordBatch> {
        let ms = &self.meta_store;
        let bc = ms
            .get_tables(dbname)
            .map_err(|e| BaseRtError::WrappingMetaError(e))?;

        let schema = Schema::new(vec![Field::new("name", DataType::LargeUtf8, false)]);
        Ok(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(bc)])?)
    }

    pub fn command_show_create_table(
        &self,
        p: Pair<Rule>,
        current_db: &str,
    ) -> BaseRtResult<RecordBatch> {
        let (dbn_opt, tn) = parse_show_create_table(p)?;
        let qtname = if dbn_opt.is_some() {
            [dbn_opt.ok_or(BaseRtError::SchemaInfoShouldExistButNot)?, tn].join(".")
        } else {
            [current_db, tn.as_str()].join(".")
        };
        let ms = &self.meta_store;
        let tid = ms.tid_by_qname(qtname).ok_or(BaseRtError::TableNotExist)?;
        let iv_script = ms
            .get_table_info_create_script(tid)
            .map_err(|e| BaseRtError::WrappingMetaError(e))?
            .ok_or(BaseRtError::TableNotExist)?;
        // log::debug!("create_script: {:?}", iv_script);
        let mut bs = BytesMut::with_capacity(64);
        bs.write_varbytes(&*iv_script);

        let bc = LargeStringArray::from(vec![String::from_utf8(bs.to_vec()).unwrap()]);

        let schema =
            Schema::new(vec![Field::new("statement", DataType::LargeUtf8, false)]);
        Ok(RecordBatch::try_new(Arc::new(schema), vec![Arc::new(bc)])?)
    }

    pub fn command_desc_table(
        &self,
        p: Pair<Rule>,
        current_db: &str,
    ) -> BaseRtResult<RecordBatch> {
        let (dbn_opt, tn) = parse_desc_table(p)?;
        let ms = &self.meta_store;
        let col_infos = ms.get_columns(
            dbn_opt.as_ref().map(|s| s.as_str()).unwrap_or(current_db),
            &tn,
        )?;
        let len = col_infos.len();

        let mut builder_name = LargeStringBuilder::new(len);
        let mut builder_type = LargeStringBuilder::new(len);
        for (name0, _, col_info) in col_infos.into_iter() {
            let mut name = Vec::with_capacity(len);
            let mut dtype = Vec::with_capacity(len * 3);
            let mut data = col_info.data_type.to_vec()?;
            data = if col_info.is_nullable {
                bytes_cat!(b"Nullable(", &data, b")")
            } else {
                data
            };
            encode_ascii_bytes_vec_short(name0.as_bytes(), &mut name)?;
            encode_ascii_bytes_vec_short(&data, &mut dtype)?;
            builder_name.append_value(String::from_utf8(name).unwrap())?;
            builder_type.append_value(String::from_utf8(dtype).unwrap())?;
        }

        let schema = Schema::new(vec![
            Field::new("name", DataType::LargeUtf8, false),
            Field::new("type", DataType::LargeUtf8, false),
        ]);
        Ok(RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(builder_name.finish()),
                Arc::new(builder_type.finish()),
            ],
        )?)
        // blk.columns.push(Column {
        //     name: b"default_type".to_vec(),
        //     data: BaseChunk {
        //         btype: BqlType::String,
        //         size: len,
        //         data: vec![0; len],
        //         null_map: None,
        //         offset_map: None,
        //         lc_dict_data: None,
        //     },
        // });
        // blk.columns.push(Column {
        //     name: b"default_expression".to_vec(),
        //     data: BaseChunk {
        //         btype: BqlType::String,
        //         size: len,
        //         data: vec![0; len],
        //         null_map: None,
        //         offset_map: None,
        //         lc_dict_data: None,
        //     },
        // });
        // blk.columns.push(Column {
        //     name: b"comment".to_vec(),
        //     data: BaseChunk {
        //         btype: BqlType::String,
        //         size: len,
        //         data: vec![0; len],
        //         null_map: None,
        //         offset_map: None,
        //         lc_dict_data: None,
        //     },
        // });
        // blk.columns.push(Column {
        //     name: b"codec_expression".to_vec(),
        //     data: BaseChunk {
        //         btype: BqlType::String,
        //         size: len,
        //         data: vec![0; len],
        //         null_map: None,
        //         offset_map: None,
        //         lc_dict_data: None,
        //     },
        // });
        // blk.columns.push(Column {
        //     name: b"ttl_expression".to_vec(),
        //     data: BaseChunk {
        //         btype: BqlType::String,
        //         size: len,
        //         data: vec![0; len],
        //         null_map: None,
        //         offset_map: None,
        //         lc_dict_data: None,
        //     },
        // });

        // blk.ncols = blk.columns.len();

        // Ok(blk)
    }

    fn has_mulit_cols_in_partkey(pc: &str) -> bool {
        let pcl = pc.len();
        if pcl == 0 {
            false
        } else {
            match pc.find(',') {
                Some(idx) => {
                    if idx == pcl - 1 {
                        false
                    } else {
                        true
                    }
                }
                None => false,
            }
        }
    }

    pub fn command_create_table(
        &self,
        p: Pair<Rule>,
        current_db: &str,
        create_script: &str,
    ) -> BaseRtResult<()> {
        let (mut t, fallible) =
            parse_create_table(p).map_err(|e| BaseRtError::WrappingLangError(e))?;
        if t.dbname.is_empty() {
            t.dbname.push_str(current_db);
        }
        t.tab_info.create_script = create_script.to_string();
        //FIXME to validate t.tab_info.partition_key
        // if t.tab_info.partition_cols.len() == 0 {
        //     return Err(BaseRtError::NoPartitionKeySettingFound);
        // }
        //FIXME only support single partition key columns
        if BaseMgmtSys::has_mulit_cols_in_partkey(t.tab_info.partition_cols.as_str()) {
            return Err(BaseRtError::MultiplePartitionKeyNotSupported);
        }

        let ms = &self.meta_store;
        let res = ms.create_table(&t);
        match res {
            Err(e) if fallible => Err(BaseRtError::WrappingMetaError(e)),
            _ => Ok(()),
        }
    }

    pub fn command_insert_into<T>(
        &self,
        p: Pair<Rule>,
        cctx: &mut T,
    ) -> BaseRtResult<BaseCommandKind>
    where
        T: BaseServerConn,
    {
        let current_db = cctx.get_db();
        let insert_info =
            parse_insert_into(p).map_err(|e| BaseRtError::WrappingLangError(e))?;
        let tab = insert_info.tab;
        let ms = &self.meta_store;
        let dbn = tab.dbname.as_str();
        let dbn = if dbn.is_empty() { current_db } else { dbn };
        ms.dbid_by_name(dbn).ok_or(BaseRtError::DatabaseNotExist)?;
        let tn = tab.name.as_str();
        let mut qtn = [dbn, tn].join(".");

        if let lang::parse::InsertFormat::Remote(ref ctx, _) = insert_info.format {
            if let lang::parse::TablePlaceKind::Remote(tab_info) = &ctx.place_kind {
                let db_name = tab_info
                    .database_name
                    .as_ref()
                    .unwrap_or(&dbn.to_owned())
                    .to_string();
                let tab_name = &tab_info.table_name;
                qtn = [db_name, tab_name.to_string()].join(".");
            }
        }

        let tid = ms.tid_by_qname(&qtn).ok_or(BaseRtError::TableNotExist)?;
        let mut blk = Default::default();

        match insert_info.values {
            None => {
                command_insert_into_gen_header(&tab, &qtn, ms, &mut blk, dbn, tn)?;
            }
            Some(vt) => {
                command_insert_into_gen_block(&tab, &qtn, ms, &mut blk, dbn, tn, vt)?;
            }
        }

        match insert_info.format {
            lang::parse::InsertFormat::Inline => {
                Ok(BaseCommandKind::InsertFormatInline(blk, qtn, tid))
            }
            lang::parse::InsertFormat::CSV => {
                Ok(BaseCommandKind::InsertFormatCSV(blk, qtn, tid))
            }
            lang::parse::InsertFormat::InlineValues => {
                //FIXME
                write_block(&mut blk, qtn.as_str(), tid)?;
                Ok(BaseCommandKind::InsertFormatInlineValues)
            }
            lang::parse::InsertFormat::Select(ref select_stmt) => {
                self.command_insert_into_select(cctx, select_stmt, qtn, tid)
            }
            lang::parse::InsertFormat::Remote(ctx, ref query) => {
                self.command_insert_into_remote(ctx, blk, query, qtn, tid)
            }
        }
    }

    fn command_insert_into_remote(
        &self,
        ctx: TablePlaceKindContext,
        blk: BaseDataBlock,
        query: &str,
        qtn: String,
        tid: Id,
    ) -> BaseRtResult<BaseCommandKind> {
        if let TablePlaceKind::Remote(remote_tb_info) = ctx.place_kind {
            if query.contains("values") {
                return Ok(BaseCommandKind::InsertFormatInline(blk, qtn, tid));
            }
            let blks = remote_query(remote_tb_info, query, true)?;

            log::debug!("subquery blks {:?}", blks);
            for blk in blks {
                write_block(&mut blk.try_into()?, qtn.as_str(), tid)?;
            }

            return Ok(BaseCommandKind::InsertFormatSelectValue);
        }

        Err(BaseRtError::WrappingLangError(
            lang::errs::LangError::ASTError("missing remote table info.".to_owned()),
        ))
    }

    fn command_insert_into_select<T>(
        &self,
        cctx: &mut T,
        select_stmt: &str,
        qtn: String,
        tid: Id,
    ) -> BaseRtResult<BaseCommandKind>
    where
        T: BaseServerConn,
    {
        let query_id = &cctx.get_query_id();
        let timer = Instant::now();
        let p = BaseMgmtSys::parse_cmd_as_pair(select_stmt)?;

        if let BaseCommandKind::Query(blks) =
            self.command_query(p, &cctx.get_db(), query_id)?
        {
            log::debug!("process subquery: {} in {:?}", query_id, timer.elapsed());
            log::debug!("subquery blks {:?}", blks);
            for blk in blks {
                write_block(&mut blk.try_into()?, qtn.as_str(), tid)?;
            }
            return Ok(BaseCommandKind::InsertFormatSelectValue);
        } else {
            unreachable!()
        }
    }

    pub fn command_drop_database(&self, p: Pair<Rule>) -> BaseRtResult<BaseCommandKind> {
        let di = parse_drop_database(p).map_err(|e| BaseRtError::WrappingLangError(e))?;
        let ms = &self.meta_store;
        let res = ms.remove_database(di.dbname.as_str());
        match res {
            Err(e) if di.fallible => {
                return Err(BaseRtError::WrappingMetaError(e));
            }
            _ => Ok(BaseCommandKind::Drop),
        }
    }

    pub fn command_drop_table(
        &self,
        p: Pair<Rule>,
        current_db: &str,
    ) -> BaseRtResult<BaseCommandKind> {
        //drop all column and table metas
        let (dbn, tn, fallible) = parse_drop_table(p)?;
        let dbname = if dbn.is_some() {
            dbn.ok_or(BaseRtError::SchemaInfoShouldExistButNot)?
        } else {
            current_db.to_string()
        };
        let ms = &self.meta_store;
        let res = ms.remove_table(dbname.as_str(), tn.as_str());
        match res {
            Err(e) if fallible => {
                return Err(BaseRtError::WrappingMetaError(e));
            }
            Err(_) => return Ok(BaseCommandKind::Drop), //non-fallible
            Ok((tid, cids)) => {
                //remove all data
                let dd = &self.conf.system.data_dirs;
                for dir in dd {
                    let res = remove_dir_all(format!("{}/{}", dir, tid));
                    // .map_err(|e| )?;
                    if let Err(e) = res {
                        if e.kind() != std::io::ErrorKind::NotFound {
                            return Err(BaseRtError::WrappingIoError(e));
                        }
                    }
                    log::debug!("Data of table {}, removed", &tn);
                }
                //uncache part files
                //FIXME need more tests in that no lock here
                let ps = &self.part_store;
                ps.uncache_for_table(tid, &cids)?;

                Ok(BaseCommandKind::Drop)
            }
        }
    }

    pub fn command_truncate_table(
        &self,
        p: Pair<Rule>,
        current_db: &str,
    ) -> BaseRtResult<BaseCommandKind> {
        //NOTE truncate is implemented as "drop table" without meta touching
        let (dbn, tn, fallible) = parse_drop_table(p)?;
        let dbname = if dbn.is_some() {
            if fallible {
                dbn.ok_or(BaseRtError::SchemaInfoShouldExistButNot)?
            } else {
                return Ok(BaseCommandKind::Drop); //non-fallible
            }
        } else {
            current_db.to_string()
        };
        let ms = &self.meta_store;
        let ps = &self.part_store;
        let qtn = [dbname.as_str(), tn.as_str()].join(".");
        let tid_opt = ms.tid_by_qname(qtn.as_str());
        match tid_opt {
            Some(tid) => {
                //remove PartStore data
                let cids = ms.get_column_ids(qtn.as_str())?;
                ps.acquire_lock(tid)?;
                ps.clear(tid, &cids)?;
                ps.release_lock(tid)?;
                //remove all data
                let dd = &self.conf.system.data_dirs;
                for dir in dd {
                    let res = remove_dir_all(format!("{}/{}", dir, tid));
                    if let Err(e) = res {
                        if e.kind() != std::io::ErrorKind::NotFound {
                            return Err(BaseRtError::WrappingIoError(e));
                        }
                    }
                    log::debug!("Data of table {}, truncated", &tn);
                }
                //uncache part files
                //FIXME need more tests in that no lock here
                ps.uncache_for_table(tid, &cids)?;

                Ok(BaseCommandKind::Drop)
            }
            None if fallible => {
                return Err(BaseRtError::TableNotExist);
            }
            None => return Ok(BaseCommandKind::Drop), //non-fallible
        }
    }

    pub fn command_use_db(&self, p: Pair<Rule>) -> BaseRtResult<String> {
        //use_db > database_name > id
        let p = p
            .into_inner()
            .next()
            .ok_or(BaseRtError::CommandParsingError)?;
        let dbname = p.as_str().trim().to_owned();
        let ms = &self.meta_store;
        let id_opt = ms.dbid_by_name(&dbname);
        match id_opt {
            Some(dbid) => Ok(dbname),
            _ => Err(BaseRtError::WrappingMetaError(MetaError::DbNotExistedError)),
        }
    }

    //WARN debug only
    pub fn command_optimize_table(
        &self,
        p: Pair<Rule>,
        current_db: &str,
    ) -> BaseRtResult<BaseCommandKind> {
        let (dbn_opt, tn) =
            parse_optimize_table(p).map_err(|e| BaseRtError::WrappingLangError(e))?;
        let _qtn = if let Some(dbn) = dbn_opt {
            [dbn.as_str(), tn.as_str()].join(".")
        } else {
            [current_db, tn.as_str()].join(".")
        };
        let ms = &self.meta_store;
        let ps = &self.part_store;
        //FIXME _qtn is not used...
        ps.flush()?;

        Ok(BaseCommandKind::Optimize)
    }

    pub fn command_query(
        &self,
        p: Pair<Rule>,
        current_db: &str,
        query_id: &str,
        // raw_query: String,
    ) -> BaseRtResult<BaseCommandKind> {
        let ctx = parse_table_place(p.clone())?;
        match ctx.place_kind {
            TablePlaceKind::Local => {
                let blks =
                    query(&self.meta_store, &self.part_store, query_id, current_db, p)?;
                Ok(BaseCommandKind::Query(blks))
            }
            TablePlaceKind::Remote(remote_tb_info) => {
                log::debug!("successfully parsed remote query to {:?} ", remote_tb_info);
                let blks = remote_query(remote_tb_info, p.as_str(), false)?;

                Ok(BaseCommandKind::Query(blks))
            }
        }
    }

    fn parse_cmd_as_pair(cmds: &str) -> BaseRtResult<Pair<Rule>> {
        let ps = parse_command(cmds).map_err(|e| BaseRtError::WrappingLangError(e))?;
        let mut ps: Vec<_> = ps.into_iter().collect();
        if ps.len() != 1 {
            return Err(BaseRtError::MultiqueryNotSupportedError);
        }
        let p = ps.remove(0);
        log::debug!("successfully parsed command: {} ", cmds);
        let p = seek_to_sub_cmd(p.into_inner())
            .map_err(|e| BaseRtError::WrappingLangError(e))?;
        Ok(p)
    }

    /*
    commands should have an auth mech
    */
    pub fn run_commands<T>(
        &self,
        cmds: String,
        cctx: &mut T,
    ) -> BaseRtResult<BaseCommandKind>
    where
        T: BaseServerConn,
    {
        // let ps = parse_command(cmds)
        //     .map_err(|e| BaseRtError::WrappingLangError(e))?;
        // let mut ps: Vec<_> = ps.into_iter().collect();
        // if ps.len() != 1 {
        //     return Err(BaseRtError::MultiqueryNotSupportedError);
        // }
        // let p = ps.remove(0);
        // log::debug!("successfully parsed command: {} ", cmds);
        // let p = seek_to_sub_cmd(&mut p.into_inner())
        //     .map_err(|e| BaseRtError::WrappingLangError(e))?;
        let p = BaseMgmtSys::parse_cmd_as_pair(&cmds)?;
        // log::debug!("cctx: {:p}, cctx.current_db: {:p},{}",
        //                cctx, &cctx.current_db, &cctx.current_db);
        match p.as_rule() {
            Rule::show_databases => {
                let blk = self.command_show_databases()?;
                Ok(BaseCommandKind::Query(vec![blk]))
            }
            Rule::show_tables => {
                let blk = self.command_show_tables(cctx.get_db())?;
                Ok(BaseCommandKind::Query(vec![blk]))
            }
            Rule::show_create_table => {
                let blk = self.command_show_create_table(p, cctx.get_db())?;
                Ok(BaseCommandKind::Query(vec![blk]))
            }
            Rule::desc_table => {
                let blk = self.command_desc_table(p, cctx.get_db())?;
                Ok(BaseCommandKind::Query(vec![blk]))
            }
            Rule::create_database => self
                .command_create_database(p)
                .map(|e| BaseCommandKind::Create),
            Rule::create_table => self
                .command_create_table(p, cctx.get_db(), cmds.as_str())
                .map(|e| BaseCommandKind::Create),
            Rule::use_db => {
                let dbn = self.command_use_db(p)?;
                cctx.set_db(dbn);
                return Ok(BaseCommandKind::Create); //FIXME Create like but not Create semantic
            }
            Rule::drop_database => {
                return self.command_drop_database(p);
            }
            Rule::drop_table => {
                return self.command_drop_table(p, cctx.get_db());
            }
            Rule::truncate_table => {
                return self.command_truncate_table(p, cctx.get_db());
            }
            Rule::optimize_table => {
                return self.command_optimize_table(p, cctx.get_db());
            }
            Rule::insert_into => {
                return self.command_insert_into(p, cctx);
            }
            Rule::query => {
                let query_id = cctx.get_query_id();
                let timer = Instant::now();
                let rt = self.command_query(p, cctx.get_db(), query_id);
                log::debug!("process query: {} in {:?}", query_id, timer.elapsed());
                return rt;
            }
            _ => return Err(BaseRtError::UnsupportedCommand),
        }
    }
}

fn command_insert_into_gen_header(
    tab: &meta::types::Table,
    qtn: &String,
    ms: &MetaStore,
    header: &mut BaseDataBlock,
    dbn: &str,
    tn: &str,
) -> Result<(), BaseRtError> {
    if tab.columns.len() != 0 {
        //insert into some columns
        for (cn, _) in &tab.columns {
            let qcn = [qtn.as_str(), &cn].join(".");
            let cid = ms.cid_by_qname(&qcn).ok_or(BaseRtError::ColumnNotExist)?;
            let ci = ms
                .get_column_info(cid)?
                .ok_or(BaseRtError::SchemaInfoShouldExistButNot)?;
            header.columns.push(BaseColumn::new_block_header(
                cn.as_bytes().to_vec(),
                ci.data_type,
                ci.is_nullable,
            ));
        }
    } else {
        //insert into all columns
        let mut col_infos = ms.get_columns(dbn, tn)?;
        //NOTE ch client relays on the order of cols to match that being inserted into
        col_infos.sort_unstable_by_key(|c| c.2.ordinal);
        for (cn, _, ci) in col_infos {
            header.columns.push(BaseColumn::new_block_header(
                cn.as_bytes().to_vec(),
                ci.data_type,
                ci.is_nullable,
            ));
        }
    }
    header.ncols = header.columns.len();
    header.nrows = 0; //for empty data case
    Ok(())
}

fn parse_literal_as_bytes(lit: &str, btyp: BqlType) -> BaseRtResult<Vec<u8>> {
    let mut rt = Vec::new();
    match btyp {
        BqlType::UInt(bits) => match bits {
            64 => {
                let v = lit
                    .parse::<u64>()
                    .map_err(|_e| BaseRtError::InsertIntoValueParsingError)?
                    .to_le_bytes();
                rt.extend(&v);
            }
            32 => {
                let v = lit
                    .parse::<u32>()
                    .map_err(|_e| BaseRtError::InsertIntoValueParsingError)?
                    .to_le_bytes();
                rt.extend(&v);
            }
            16 => {
                let v = lit
                    .parse::<u16>()
                    .map_err(|_e| BaseRtError::InsertIntoValueParsingError)?
                    .to_le_bytes();
                rt.extend(&v);
            }
            8 => {
                let v = lit
                    .parse::<u8>()
                    .map_err(|_e| BaseRtError::InsertIntoValueParsingError)?
                    .to_le_bytes();
                rt.extend(&v);
            }
            _ => return Err(BaseRtError::UnsupportedValueConversion),
        },
        BqlType::Int(bits) => match bits {
            64 => {
                let v = lit
                    .parse::<i64>()
                    .map_err(|_e| BaseRtError::InsertIntoValueParsingError)?
                    .to_le_bytes();
                rt.extend(&v);
            }
            32 => {
                let v = lit
                    .parse::<i32>()
                    .map_err(|_e| BaseRtError::InsertIntoValueParsingError)?
                    .to_le_bytes();
                rt.extend(&v);
            }
            16 => {
                let v = lit
                    .parse::<i16>()
                    .map_err(|_e| BaseRtError::InsertIntoValueParsingError)?
                    .to_le_bytes();
                rt.extend(&v);
            }
            8 => {
                let v = lit
                    .parse::<i8>()
                    .map_err(|_e| BaseRtError::InsertIntoValueParsingError)?
                    .to_le_bytes();
                rt.extend(&v);
            }
            _ => return Err(BaseRtError::UnsupportedValueConversion),
        },
        BqlType::DateTimeTz(tz) => {
            let tz_offset = tz.offset();
            let ut = parse_to_epoch(lit, tz_offset)?;
            let v = ut.to_le_bytes();
            rt.extend(&v);
        }
        BqlType::DateTime => {
            let tz_offset = BMS.timezone.offset();
            let ut = parse_to_epoch(lit, tz_offset)?;
            let v = ut.to_le_bytes();
            rt.extend(&v);
        }
        BqlType::String => {
            todo!()
        }
        // LowCardinalityString,
        _ => todo!(),
    }

    Ok(rt)
}

fn command_insert_into_gen_block(
    tab: &meta::types::Table,
    qtn: &String,
    ms: &MetaStore,
    blk: &mut BaseDataBlock,
    dbn: &str,
    tn: &str,
    rows: Vec<Vec<String>>,
) -> BaseRtResult<()> {
    let nr = rows.len();
    if tab.columns.len() != 0 {
        return Err(BaseRtError::UnsupportedFunctionality2(
            "insert with partial values now is not supported",
        ));
        //insert into some columns
        // let mut ic = 0;
        // for (cn, _) in &tab.columns {
        //     let qcn = [qtn.as_str(), &cn].join(".");
        //     let cid =
        //         ms.cid_by_qname(&qcn).ok_or(BaseRtError::ColumnNotExist)?;
        //     let ci = ms
        //         .get_column_info(cid)?
        //         .ok_or(BaseRtError::SchemaInfoShouldExistButNot)?;
        //     let name = cn.as_bytes().to_vec();
        //     let btype = ci.data_type;
        //     //rows to cols
        //     let nr = rows.len();
        //     if nc != rows[0].len() {
        //         return Err(BaseRtError::InvalidFormatForInsertIntoValueList);
        //     }
        //     let mut data: Vec<u8> = Vec::new();
        //     for i in 0..nr {
        //         let lit = &rows[i][ic];
        //         let bs = parse_literal_as_bytes(lit, btype)?;
        //         data.extend(bs);
        //     }
        //     blk.columns.push(Column {
        //         name,
        //         data: BaseChunk {
        //             btype,
        //             size: 0,
        //             data,
        //             null_map: if ci.is_nullable { Some(vec![]) } else { None }, //FIXME
        //             offset_map: None,
        //             lc_dict_data: None,
        //         },
        //     });
        //     ic += 1;
        // }
    } else {
        //insert into all columns
        let mut col_infos = ms.get_columns(dbn, tn)?;
        //NOTE ch client relays on the order of cols to match that being inserted into
        col_infos.sort_unstable_by_key(|c| c.2.ordinal);
        let mut ic = 0;
        let nc = col_infos.len();
        for (cn, _, ci) in col_infos {
            let name = cn.as_bytes().to_vec();
            let btype = ci.data_type;
            //rows to cols
            if nc != rows[0].len() {
                return Err(BaseRtError::InvalidFormatForInsertIntoValueList);
            }
            let mut data: Vec<u8> = Vec::new();
            for i in 0..nr {
                let lit = &rows[i][ic];
                let bs = parse_literal_as_bytes(lit, btype)?;
                data.extend(bs);
            }
            blk.columns.push(BaseColumn {
                name,
                data: BaseChunk {
                    btype,
                    size: 0,
                    data,
                    null_map: if ci.is_nullable { Some(vec![]) } else { None }, //FIXME
                    offset_map: None,
                    lc_dict_data: None,
                },
            });
            ic += 1;
        }
    }
    blk.ncols = blk.columns.len();
    blk.nrows = nr;

    Ok(())
}

#[cfg(test)]
mod unit_tests {
    use super::BaseMgmtSys;

    #[test]
    fn test_has_mulit_cols_in_partkey() {
        assert_eq!(BaseMgmtSys::has_mulit_cols_in_partkey(""), false);
        assert_eq!(BaseMgmtSys::has_mulit_cols_in_partkey(","), false);
        assert_eq!(BaseMgmtSys::has_mulit_cols_in_partkey("cols,"), false);
        assert_eq!(BaseMgmtSys::has_mulit_cols_in_partkey("cols1,cols2,"), true);
        assert_eq!(
            BaseMgmtSys::has_mulit_cols_in_partkey("cols_xxx_1,cols_2,cols_3,"),
            true
        );
    }
}
