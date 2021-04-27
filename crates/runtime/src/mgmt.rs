use base::{datetimes::parse_to_epoch, mem::SyncPointer, strings::s};
use bytes::BytesMut;
use chrono::{Local, Offset, TimeZone};
use chrono_tz::{OffsetComponents, OffsetName, TZ_VARIANTS};
use dashmap::DashMap;
use engine::types::QueryState;
use lang::parse::{
    parse_command, parse_create_database, parse_create_table,
    parse_drop_database, parse_drop_table, parse_insert_into,
    parse_optimize_table, parse_show_create_table, seek_to_sub_cmd, Pair, Rule,
};
use lightjit::jit;
use meta::{
    confs::Conf,
    errs::MetaError,
    store::{parts::PartStore, sys::MetaStore},
    toml,
    types::{BaseChunk, BqlType, Id},
};
use std::{
    env, ffi::CString, fs::remove_dir_all, lazy::SyncLazy, panic::panic_any,
    path::Path, pin::Pin, sync::Mutex, time::Instant,
};

use clap::{App, Arg};

use crate::{
    ch::{
        blocks::{new_block_header, Block, Column},
        codecs::CHMsgWriteAware,
        protocol::ConnCtx,
    },
    errs::{BaseRtError, BaseRtResult},
};

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
        Ok(conf_path) => vec![
            "target/debug/server".to_string(),
            "-c".to_string(),
            conf_path,
        ],
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
    let mut conf_opt: Option<Conf> = None;
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
        let conf0: Conf = toml::from_str(conf_string)
            .expect("Can not parse your provided conf string");
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
    log::info!("server.ip_addr: {:?}", &conf.server.ip_addr);
    log::info!("server.port: {:?}", &conf.server.port);

    let conf = Box::new(conf);
    BaseMgmtSys::from_conf(Box::leak(conf)).unwrap()
});

pub static EXPR_JIT: SyncLazy<Mutex<jit::JIT>> =
    SyncLazy::new(|| Mutex::new(jit::JIT::default()));

// pub fn bms<'a>() -> BaseRtResult<&'a BaseMgmtSys<'a>> {
//     Ok(BMS.get().ok_or(BaseRtError::BMSNotAvailableError)?)
// }

// pub fn bms_conf() -> BaseRtResult<&'static Conf> {
//     Ok(&bms()?.conf)
// }

// pub fn bms_tz_sys() -> BaseRtResult<&'static str> {
//     Ok(&bms()?.timezone_sys)
// }
//FIXME
#[derive(Debug)]
pub enum BaseCommandKind {
    Default,
    Create,
    Drop,
    Query(Vec<Block>), //FIXME need iterator for big return
    InsertFormatInline(Block, String, Id),
    InsertFormatInlineValues(Block, String, Id),
    InsertFormatCSV(Block, String, Id),
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
    pub(crate) meta_store: MetaStore,
    pub(crate) part_store: PartStore<'a>,
    pub ptk_exprs_reg: DashMap<Id, SyncPointer<u8>, BuildPtkExprsHasher>,
    pub timezone_sys: String,
    pub timezone_sys_offset: i32,
}

impl<'a> BaseMgmtSys<'a> {
    pub fn from_conf(conf: &'a Conf) -> BaseRtResult<Self> {
        let ms_path = conf.system.meta_dirs.as_slice();
        let meta_store = MetaStore::new(ms_path)
            .map_err(|e| BaseRtError::WrappingMetaError(e))?;
        let part_store = PartStore::new(ms_path, &conf.system.data_dirs)?;
        let (timezone_sys, timezone_sys_offset) = {
            let mut ret = ("GMT".to_string(), 0);
            let ctz = Local::now().offset().fix();
            for tz in TZ_VARIANTS.iter() {
                let some_time = tz.ymd(1, 1, 1).and_hms(0, 0, 0);
                let stz = some_time.offset().fix();
                if stz == ctz {
                    let tz_sys = some_time.offset().tz_id();
                    let tz_sys_offset =
                        some_time.offset().base_utc_offset().num_seconds()
                            as i32;
                    log::info!(
                        "current timezone sets to {}",
                        tz_sys,
                        // tz_sys_offset
                    );
                    ret = (tz_sys.to_string(), tz_sys_offset)
                }
            }
            ret
        };
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
            ptk_exprs_reg,
            timezone_sys,
            timezone_sys_offset,
        })
    }

    //===
    pub fn get_ptk_exps_fn_ptr(
        &self,
        qtn: &str,
        tid: Id,
    ) -> BaseRtResult<*const u8> {
        let fp_opt = self.ptk_exprs_reg.get(&tid);
        match fp_opt {
            Some(fp) => {
                let p = *fp;
                Ok(p.as_ptr())
            }
            None => {
                let rt = match self
                    .meta_store
                    .get_table_info_partition_keys_expr(tid)?
                {
                    Some(iv) => {
                        let ptk_expr =
                            unsafe { std::str::from_utf8_unchecked(&*iv) };
                        let mut ptc = String::new();
                        match self
                            .meta_store
                            .get_table_info_partition_cols(tid)?
                        {
                            Some(iv) => ptc.push_str(unsafe {
                                std::str::from_utf8_unchecked(&*iv)
                            }),
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
                            .map_err(|_| {
                                BaseRtError::LightJitCompilationError
                            })?; //FIXME possible memory leak
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
        let di = parse_create_database(p)
            .map_err(|e| BaseRtError::WrappingLangError(e))?;
        let ms = &self.meta_store;
        let dbid_opt = ms.new_db(di.dbname.as_str());
        match dbid_opt {
            Err(e) if di.fallible => Err(BaseRtError::WrappingMetaError(e)),
            _ => Ok(()),
        }
    }

    pub fn command_show_databases(&self) -> BaseRtResult<Block> {
        let ms = &self.meta_store;
        let bc = ms
            .get_all_databases()
            .map_err(|e| BaseRtError::WrappingMetaError(e))?;
        let mut blk = Block::default();
        blk.nrows = bc.size;
        blk.columns.push(Column {
            name: b"name".to_vec(),
            data: bc,
        });
        blk.ncols = blk.columns.len();

        Ok(blk)
    }

    pub fn command_show_tables(&self, dbname: &str) -> BaseRtResult<Block> {
        let ms = &self.meta_store;
        let bc = ms
            .get_tables(dbname)
            .map_err(|e| BaseRtError::WrappingMetaError(e))?;
        let mut blk = Block::default();
        blk.nrows = bc.size;
        blk.columns.push(Column {
            name: b"name".to_vec(),
            data: bc,
        });
        blk.ncols = blk.columns.len();

        Ok(blk)
    }

    pub fn command_show_create_table(
        &self,
        p: Pair<Rule>,
        current_db: &str,
    ) -> BaseRtResult<Block> {
        let (dbn_opt, tn) = parse_show_create_table(p)?;
        let qtname = if dbn_opt.is_some() {
            [dbn_opt.ok_or(BaseRtError::SchemaInfoShouldExistButNot)?, tn]
                .join(".")
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
        let mut blk = Block::default();
        blk.nrows = 1;
        blk.columns.push(Column {
            name: b"statement".to_vec(),
            data: BaseChunk {
                btype: BqlType::String,
                size: 1,
                data: bs.to_vec(),
                null_map: None,
                offset_map: None,
                lc_dict_data: None,
            },
        });
        blk.ncols = blk.columns.len();

        Ok(blk)
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
        let (mut t, fallible) = parse_create_table(p)
            .map_err(|e| BaseRtError::WrappingLangError(e))?;
        if t.dbname.is_empty() {
            t.dbname.push_str(current_db);
        }
        t.tab_info.create_script = create_script.to_string();
        //FIXME to validate t.tab_info.partition_key
        // if t.tab_info.partition_cols.len() == 0 {
        //     return Err(BaseRtError::NoPartitionKeySettingFound);
        // }
        //FIXME only support single partition key columns
        if BaseMgmtSys::has_mulit_cols_in_partkey(
            t.tab_info.partition_cols.as_str(),
        ) {
            return Err(BaseRtError::MultiplePartitionKeyNotSupported);
        }

        let ms = &self.meta_store;
        let res = ms.create_table(&t);
        match res {
            Err(e) if fallible => Err(BaseRtError::WrappingMetaError(e)),
            _ => Ok(()),
        }
    }

    pub fn command_insert_into(
        &self,
        p: Pair<Rule>,
        current_db: &str,
    ) -> BaseRtResult<BaseCommandKind> {
        let insert_info = parse_insert_into(p)
            .map_err(|e| BaseRtError::WrappingLangError(e))?;
        let tab = insert_info.tab;
        let ms = &self.meta_store;
        let dbn = tab.dbname.as_str();
        let dbn = if dbn.is_empty() { current_db } else { dbn };
        ms.dbid_by_name(dbn).ok_or(BaseRtError::DatabaseNotExist)?;
        let tn = tab.name.as_str();
        let qtn = [dbn, tn].join(".");
        let tid = ms.tid_by_qname(&qtn).ok_or(BaseRtError::TableNotExist)?;

        let mut blk: Block = Default::default();
        match insert_info.values {
            None => {
                command_insert_into_gen_header(
                    &tab, &qtn, ms, &mut blk, dbn, tn,
                )?;
            }
            Some(vt) => {
                // println!("vt: {:?}", vt);
                command_insert_into_gen_block(
                    &tab,
                    &qtn,
                    ms,
                    &mut blk,
                    dbn,
                    tn,
                    vt,
                    self.timezone_sys_offset,
                )?;
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
                Ok(BaseCommandKind::InsertFormatInlineValues(blk, qtn, tid))
            }
        }
    }

    pub fn command_drop_database(
        &self,
        p: Pair<Rule>,
    ) -> BaseRtResult<BaseCommandKind> {
        let di = parse_drop_database(p)
            .map_err(|e| BaseRtError::WrappingLangError(e))?;
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
        let qtn = [dbname.as_str(), tn.as_str()].join(".");
        let tid_opt = ms.tid_by_qname(qtn.as_str());
        match tid_opt {
            Some(tid) => {
                //remove all data
                let dd = &self.conf.system.data_dirs;
                for dir in dd {
                    remove_dir_all(format!("{}/{}", dir, tid))
                        .map_err(|e| BaseRtError::WrappingIoError(e))?;
                    log::debug!("Data of table {}, removed", &tn);
                }
                let cids = ms.get_column_ids(qtn.as_str())?;
                //uncache part files
                //FIXME need more tests in that no lock here
                let ps = &self.part_store;
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
            _ => Err(BaseRtError::WrappingMetaError(
                MetaError::DbNotExistedError,
            )),
        }
    }

    //WARN debug only
    pub fn command_optimize_table(
        &self,
        p: Pair<Rule>,
        current_db: &str,
    ) -> BaseRtResult<BaseCommandKind> {
        let (dbn_opt, tn) = parse_optimize_table(p)
            .map_err(|e| BaseRtError::WrappingLangError(e))?;
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
        let blks = crate::read::query(
            &self.meta_store,
            &self.part_store,
            query_id,
            current_db,
            p,
            self.timezone_sys_offset,
        )?;
        Ok(BaseCommandKind::Query(blks))
    }

    fn parse_cmd_as_pair(cmds: &str) -> BaseRtResult<Pair<Rule>> {
        let ps = parse_command(cmds)
            .map_err(|e| BaseRtError::WrappingLangError(e))?;
        let mut ps: Vec<_> = ps.into_iter().collect();
        if ps.len() != 1 {
            return Err(BaseRtError::MultiqueryNotSupportedError);
        }
        let p = ps.remove(0);
        log::debug!("successfully parsed command: {} ", cmds);
        let p = seek_to_sub_cmd(&mut p.into_inner())
            .map_err(|e| BaseRtError::WrappingLangError(e))?;
        Ok(p)
    }

    /*
    commands should have an auth mech
    */
    pub fn run_commands(
        &self,
        cmds: String,
        cctx: &mut ConnCtx,
    ) -> BaseRtResult<BaseCommandKind> {
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
                let blk = self.command_show_tables(&cctx.current_db)?;
                Ok(BaseCommandKind::Query(vec![blk]))
            }
            Rule::show_create_table => {
                let blk =
                    self.command_show_create_table(p, &cctx.current_db)?;
                Ok(BaseCommandKind::Query(vec![blk]))
            }
            Rule::create_database => self
                .command_create_database(p)
                .map(|e| BaseCommandKind::Create),
            Rule::create_table => self
                .command_create_table(p, &cctx.current_db, cmds.as_str())
                .map(|e| BaseCommandKind::Create),
            Rule::use_db => {
                let dbn = self.command_use_db(p)?;
                cctx.current_db = dbn;
                return Ok(BaseCommandKind::Create); //FIXME Create like but not Create semantic
            }
            Rule::drop_database => {
                return self.command_drop_database(p);
            }
            Rule::drop_table => {
                return self.command_drop_table(p, &cctx.current_db);
            }
            Rule::truncate_table => {
                return self.command_truncate_table(p, &cctx.current_db);
            }
            Rule::optimize_table => {
                return self.command_optimize_table(p, &cctx.current_db);
            }
            Rule::insert_into => {
                return self.command_insert_into(p, &cctx.current_db);
            }
            Rule::query => {
                let query_id = &cctx.query_id;
                let timer = Instant::now();
                let rt = self.command_query(p, &cctx.current_db, query_id);
                log::debug!(
                    "process query: {} in {:?}",
                    query_id,
                    timer.elapsed()
                );
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
    header: &mut Block,
    dbn: &str,
    tn: &str,
) -> Result<(), BaseRtError> {
    if tab.columns.len() != 0 {
        //insert into some columns
        for (cn, _) in &tab.columns {
            let qcn = [qtn.as_str(), &cn].join(".");
            let cid =
                ms.cid_by_qname(&qcn).ok_or(BaseRtError::ColumnNotExist)?;
            let ci = ms
                .get_column_info(cid)?
                .ok_or(BaseRtError::SchemaInfoShouldExistButNot)?;
            header.columns.push(new_block_header(
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
            header.columns.push(new_block_header(
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

fn parse_literal_as_bytes(
    lit: &str,
    btyp: BqlType,
    tz_offset: i32,
) -> BaseRtResult<Vec<u8>> {
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
        BqlType::DateTime => {
            let ut = (parse_to_epoch(lit)
                .map_err(|_e| BaseRtError::InsertIntoValueParsingError)?
                as i64
                - (tz_offset as i64)) as u32;
            let v = ut.to_le_bytes();
            rt.extend(&v);
        }
        // String =>
        // LowCardinalityString,
        _ => todo!(),
    }

    Ok(rt)
}

fn command_insert_into_gen_block(
    tab: &meta::types::Table,
    qtn: &String,
    ms: &MetaStore,
    blk: &mut Block,
    dbn: &str,
    tn: &str,
    rows: Vec<Vec<String>>,
    tz_offset: i32,
) -> BaseRtResult<()> {
    let nr = rows.len();
    if tab.columns.len() != 0 {
        return Err(BaseRtError::UnsupportedFunctionality2("insert with partial values now is not supported"));
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
        //         let bs = parse_literal_as_bytes(lit, btype, tz_offset)?;
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
                let bs = parse_literal_as_bytes(lit, btype, tz_offset)?;
                data.extend(bs);
            }
            blk.columns.push(Column {
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
        }
        ic += 1;
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
        assert_eq!(
            BaseMgmtSys::has_mulit_cols_in_partkey("cols1,cols2,"),
            true
        );
        assert_eq!(
            BaseMgmtSys::has_mulit_cols_in_partkey("cols_xxx_1,cols_2,cols_3,"),
            true
        );
    }
}
