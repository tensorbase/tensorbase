/**
sys tab design:

external reprs: all infos from sql/bql
internal reprs: ids...


db_tab as tree

all ids are globally auto-incr

"0" tree:
db - did
db.tab - tid
db.tab.col - cid

"1" tree:
did - db
tid - did:tab
cid - did:tid:col

(unused:
kv semantic tables:

tid - pk_cid or type(1B?):value as pk?
cid:pk_value - type(1B?):value as key
index:
cid:value:pk_vaule []
)

tree_cols:
----------
cid - column_info

tree_tabs:
----------

tid, "cr" - create_script (string)
tid, "en" - engine (enum)
tid, "pa" - partition_keys_expr
tid, "pc" - partition_cols
tid, "se", settings.k - settings.v



-----------

CREATE TABLE nyc_taxi (
    trip_id INT32 PRIMARY KEY,
    pickup_datetime UNIX_DATETIME NOT NULL,
    passenger_count UINT8 NOT NULL
)

*/
use crate::to_qualified_key;

use crate::errs::{MetaError, MetaResult};
use crate::types::*;

use num_traits::PrimInt;
pub use sled::IVec;

pub struct MetaStore {
    // paths : Vec<String>,
    mdb: sled::Db,
    // mdb1: sled::Db,
    tree0: sled::Tree,
    tree1: sled::Tree,
    tree_tabs: sled::Tree,
    tree_cols: sled::Tree,
}

const KEY_SYS_IDX_DBS: &'static str = "system.__idx_dbs_";
const KEY_SYS_IDX_TABS: &'static str = "system.__idx_tabs_";

/**
 *
 */
impl MetaStore {
    pub fn new<T: AsRef<str>>(dirs: &[T]) -> MetaResult<Self> {
        assert!(!dirs.is_empty());

        let p0 = [dirs[0].as_ref(), "m0"].join("/");
        let mdb = sled::Config::default()
            .path(p0)
            .cache_capacity(64 * 1024 * 1024)
            .open()
            .map_err(|_e| MetaError::OpenError)?;
        let tree0 = mdb.open_tree(b"0").map_err(|_e| MetaError::OpenError)?;
        let tree1 = mdb.open_tree(b"1").map_err(|_e| MetaError::OpenError)?;
        let tree_tabs =
            mdb.open_tree(b"ts").map_err(|_e| MetaError::OpenError)?;
        let tree_cols =
            mdb.open_tree(b"cs").map_err(|_e| MetaError::OpenError)?;
        Ok(MetaStore {
            mdb,
            tree0,
            tree1,
            tree_tabs,
            tree_cols,
        })

        // match paths.len() {
        //     1 => {
        //         let p0 = [&paths[0], "m0"].join("/");
        //         let p1 = [&paths[0], "m1"].join("/");
        //         let mdb = sled::Config::default()
        //             .path(p0)
        //             .cache_capacity(128 * 1024 * 1024)
        //             .open()
        //             .map_err(|e| MetaError::MetaStoreOpenError)?;
        //         let mdb1 = sled::Config::default()
        //             .path(p1)
        //             .cache_capacity(128 * 1024 * 1024)
        //             .open()
        //             .map_err(|e| MetaError::MetaStoreOpenError)?;
        //         let tree0 = mdb
        //             .open_tree(b"0")
        //             .map_err(|e| MetaError::MetaStoreOpenError)?;
        //         let tree1 = mdb
        //             .open_tree(b"1")
        //             .map_err(|e| MetaError::MetaStoreOpenError)?;
        //         Ok(MetaStore { mdb, mdb1, tree0, tree1 })
        //     }
        //     _ => {
        //         let p0 = [&paths[0], "m0"].join("/");
        //         let p1 = [&paths[1], "m1"].join("/");
        //         let mdb = sled::Config::default()
        //             .path(p0)
        //             .cache_capacity(128 * 1024 * 1024)
        //             .open()
        //             .map_err(|e| MetaError::MetaStoreOpenError)?;
        //         let mdb1 = sled::Config::default()
        //             .path(p1)
        //             .cache_capacity(128 * 1024 * 1024)
        //             .open()
        //             .map_err(|e| MetaError::MetaStoreOpenError)?;
        //         let tree0 = mdb
        //             .open_tree(b"0")
        //             .map_err(|e| MetaError::MetaStoreOpenError)?;
        //         let tree1 = mdb
        //             .open_tree(b"1")
        //             .map_err(|e| MetaError::MetaStoreOpenError)?;
        //         Ok(MetaStore { mdb, mdb1, tree0, tree1 })
        //     }
        // }
    }

    //FIXME assumed the idgen can be recovered, but we still need a
    //      manual recovery mech which could be done in housekeeping
    fn gen_id(&self) -> MetaResult<Id> {
        self.mdb.generate_id().map_err(|_e| MetaError::IdGenError)
    }

    ///WARN this api assumes the name are qualified name and validated before passing
    #[inline]
    fn id<T: AsRef<str>>(&self, qname: T) -> Option<Id> {
        self.tree0
            .get(qname.as_ref())
            .unwrap_or(None)
            .map(|bs| *(&*bs).into_ref::<u64>())
    }

    #[inline]
    fn _name(&self, id: Id) -> Option<IVec> {
        self.tree1.get(id.to_be_bytes()).unwrap_or(None)
    }

    pub fn dbname(&self, id: Id) -> Option<IVec> {
        self._name(id)
    }

    pub fn full_tabname(&self, id: Id) -> Option<IVec> {
        self._name(id)
    }

    pub fn full_colname<'a>(&self, id: Id) -> Option<IVec> {
        self._name(id)
    }

    // pub fn get_value<PKT, R>(&self, cid: Id, pk: PKT) -> R {
    //     todo!()
    // }

    #[inline(always)]
    fn _new(&self, ks: &str) -> MetaResult<Id> {
        let idopt = self.id(&ks);
        match idopt {
            Some(id) => Err(MetaError::EntityExistedError(id)),
            None => {
                let id = self.gen_id()?;
                //tree0
                let _old = self
                    .tree0
                    .insert(ks, id.as_bytes())
                    .map_err(|_e| MetaError::InsertError)?;
                debug_assert!(_old.is_none());
                //tree1
                let _old = self
                    .tree1
                    .insert(id.to_be_bytes(), ks)
                    .map_err(|_e| MetaError::InsertError)?;
                debug_assert!(_old.is_none());
                // if let Some(old_value) = r {
                //     let bs = &*old_value;
                //     log::trace!("find old dbname.tabname: {}", unsafe {
                //         std::str::from_utf8_unchecked(bs)
                //     });
                //     return Err(MetaError::MetaStoreTabExistedError(id));
                // }

                Ok(id)
            }
        }
    }

    //FIXME move all key_sys into dedicated tree_sys?
    pub fn new_db(&self, dbname: &str) -> MetaResult<Id> {
        let rt = self._new(dbname)?;
        let mut key_sd = vec![];
        key_sd.extend_from_slice(KEY_SYS_IDX_DBS.as_bytes());
        key_sd.extend_from_slice(&rt.to_be_bytes());
        let _old = self
            .tree0
            .insert(key_sd, dbname)
            .map_err(|_e| MetaError::InsertError)?;
        debug_assert!(_old.is_none());
        Ok(rt)
    }

    pub fn get_all_databases(&self) -> MetaResult<BaseChunk> {
        let dbn_iter = self.tree0.scan_prefix(KEY_SYS_IDX_DBS);
        let mut rt = vec![];
        let mut size = 0usize;
        for kv in dbn_iter {
            let (_, v) = kv.map_err(|_| MetaError::GetError)?;
            let bs = &*v;
            let bs_len = bs.len();
            if bs_len > 127 {
                return Err(MetaError::TooLongLengthForStringError);
            }
            // log::info!("v: {}", unsafe{std::str::from_utf8_unchecked(bs)});
            rt.push(bs_len as u8);
            rt.extend_from_slice(bs);
            size += 1;
        }
        Ok(BaseChunk {
            btype: BqlType::String,
            size,
            data: rt,
            null_map: None,
            offset_map: None,
            lc_dict_data: None,
        })
    }

    pub fn get_table_names(&self, dbname: &str) -> MetaResult<Vec<String>> {
        let tbn_iter =
            self.tree0.scan_prefix([KEY_SYS_IDX_TABS, dbname].join(""));
        let mut rt = vec![];
        for kv in tbn_iter {
            let (_, v) = kv.map_err(|_| MetaError::GetError)?;
            let bs = &*v;
            let bs_len = bs.len();
            if bs_len > 127 {
                return Err(MetaError::TooLongLengthForStringError);
            }
            // log::info!("v: {}", unsafe{std::str::from_utf8_unchecked(bs)});
            rt.push(unsafe { std::str::from_utf8_unchecked(bs) }.to_string());
        }
        Ok(rt)
    }

    //deprecated, get_tables -> get_table_names
    pub fn get_tables(&self, dbname: &str) -> MetaResult<BaseChunk> {
        let tbn_iter =
            self.tree0.scan_prefix([KEY_SYS_IDX_TABS, dbname].join(""));
        let mut rt = vec![];
        let mut size = 0usize;
        for kv in tbn_iter {
            let (_, v) = kv.map_err(|_| MetaError::GetError)?;
            let bs = &*v;
            let bs_len = bs.len();
            if bs_len > 127 {
                return Err(MetaError::TooLongLengthForStringError);
            }
            // log::info!("v: {}", unsafe{std::str::from_utf8_unchecked(bs)});
            rt.push(bs_len as u8);
            rt.extend_from_slice(bs);
            size += 1;
        }
        Ok(BaseChunk {
            btype: BqlType::String,
            size,
            data: rt,
            null_map: None,
            offset_map: None,
            lc_dict_data: None,
        })
    }

    pub fn get_columns(
        &self,
        dbname: &str,
        tname: &str,
    ) -> MetaResult<Vec<(String, u64, ColumnInfo)>> {
        let cnp = to_qualified_key!(dbname, tname, "");
        self._get_columns(&cnp)
    }

    pub fn get_columns_by_qtn(
        &self,
        qtn: &String,
    ) -> MetaResult<Vec<(String, u64, ColumnInfo)>> {
        let cnp = to_qualified_key!(qtn.as_str(), "");
        self._get_columns(&cnp)
    }

    #[inline(always)]
    fn _get_columns(
        &self,
        cnp: &String,
    ) -> MetaResult<Vec<(String, u64, ColumnInfo)>> {
        let ci_iter = self.tree0.scan_prefix(cnp);
        let mut rt = vec![];
        for kv in ci_iter {
            let (bs_qcn, bs_cid) = kv.map_err(|_| MetaError::GetError)?;
            let cn = unsafe {
                std::str::from_utf8_unchecked(&(&*bs_qcn)[cnp.len()..])
            }
            .to_string();
            let iv_cid = &*bs_cid;
            let cid = *iv_cid.into_ref::<u64>();
            // log::info!("v: {}", unsafe{std::str::from_utf8_unchecked(bs)});
            rt.push((
                cn,
                cid,
                self.get_column_info(cid)?
                    .ok_or(MetaError::EntityShouldExistButNot)?,
            ));
        }
        Ok(rt)
    }

    pub fn get_column_ids(
        &self,
        qtn: &str,
    ) -> MetaResult<Vec<Id>> {
        // let cnp = to_qualified_key!(dbname, tname, "");
        let ci_iter = self.tree0.scan_prefix(qtn);
        let mut rt = vec![];
        for kv in ci_iter {
            let (_bs_qcn, bs_cid) = kv.map_err(|_| MetaError::GetError)?;
            let iv_cid = &*bs_cid;
            let cid = *iv_cid.into_ref::<u64>();
            // log::info!("v: {}", unsafe{std::str::from_utf8_unchecked(bs)});
            rt.push(cid);
        }
        Ok(rt)
    }

    // #[inline]
    // fn is_system_protected_entity(dbname: &str) -> bool {}

    pub fn remove_database(&self, dbname: &str) -> MetaResult<()> {
        if dbname == "system" || dbname == "default" {
            return Err(MetaError::SystemLevelEntitiesCanNotRemoved);
        }

        if let Ok(_dbid) = self._del(dbname) {
            //remove all tables in this database
            let tabs = self.get_table_names(dbname)?;
            for t in tabs {
                self.remove_table(dbname, t.as_str())?;
            }
            Ok(())
        } else {
            Err(MetaError::EntityDelError)
        }
    }

    //drop all column and table metas
    pub fn remove_table(
        &self,
        dbname: &str,
        tabname: &str,
    ) -> MetaResult<(Id, Vec<Id>)> {
        if dbname == "system" {
            return Err(MetaError::SystemLevelEntitiesCanNotRemoved);
        }

        let qtn = to_qualified_key!(dbname, tabname);
        let tid = self._del(qtn.as_str())?;
        let cols = self.get_columns(dbname, tabname)?;
        let mut cids = vec![];
        for (cn, _, _) in cols {
            let qcn = to_qualified_key!(qtn.as_str(), cn.as_str());
            let cid = self._del(qcn.as_str())?;
            cids.push(cid);
        }
        Ok((tid, cids))

        // let res: TransactionResult<(), MetaError> =
        //     (&self.tree0, &self.tree1, &self.tree_tabs, &self.tree_cols)
        //         .transaction(|(tx0, tx1, txts, txcs)| {
        //             //FIXME if we use
        //             let qtn = to_qualified_key!(dbname, tabname);
        //             if let Ok(_tid) = self._del(qtn.as_str()) {
        //                 let cols = if let Ok(cols) =
        //                     self.get_columns(dbname, tabname)
        //                 {
        //                     cols
        //                 } else {
        //                     abort(MetaError::EntityDelError)?
        //                 };
        //                 for (qcn, _, _) in cols {
        //                     if let Ok(_cid) = self._del(qcn.as_str()) {
        //                     } else {
        //                         return abort(MetaError::EntityDelError)?;
        //                     }
        //                 }
        //                 Ok(())
        //             } else {
        //                 abort(MetaError::EntityDelError)?
        //             }
        //         });
        // //FIXME map_err does work for TransactionResult?
        // match res {
        //     Err(_) => return Err(MetaError::EntityDelError),
        //     _ => Ok(()),
        // }
    }

    fn new_tab(&self, dbname: &str, tabname: &str) -> MetaResult<Id> {
        let ks = to_qualified_key!(dbname, tabname);
        let rt = self._new(&ks)?;
        let mut key_sd = vec![];
        key_sd.extend_from_slice(KEY_SYS_IDX_TABS.as_bytes());
        key_sd.extend_from_slice(dbname.as_bytes());
        key_sd.extend_from_slice(&rt.to_be_bytes());
        let _old = self
            .tree0
            .insert(key_sd, tabname)
            .map_err(|_| MetaError::InsertError)?;
        debug_assert!(_old.is_none());
        Ok(rt)
    }

    // pub fn create_table(&self, ) -> MetaResult<()> {}

    fn new_col(
        &self,
        dbname: &str,
        tabname: &str,
        colname: &str,
    ) -> MetaResult<Id> {
        let ks = to_qualified_key!(dbname, tabname, colname);
        self._new(&ks)
    }

    //this call just removes a name and all its direct entries in four trees
    fn _del(&self, qname: &str) -> MetaResult<Id> {
        let id_opt = self
            .tree0
            .remove(qname)
            .map_err(|_| MetaError::InsertError)
            .unwrap_or(None)
            .map(|bs| *(&*bs).into_ref::<u64>());
        if let Some(id) = id_opt {
            self.tree1
                .remove(id.to_be_bytes())
                .map_err(|_| MetaError::InsertError)?;
            //remove in tree_tabs if matched
            //FIXME batch may be better
            let ti_iter = self.tree_tabs.scan_prefix(id.to_be_bytes());
            for kv in ti_iter {
                let (bs_k, _) = kv.map_err(|_| MetaError::EntityDelError)?;
                self.tree_tabs
                    .remove(bs_k)
                    .map_err(|_| MetaError::EntityDelError)?;
            }
            //remove in tree_cols if matched
            let ci_iter = self.tree_cols.scan_prefix(id.to_be_bytes());
            for kv in ci_iter {
                let (bs_k, _) = kv.map_err(|_| MetaError::EntityDelError)?;
                self.tree_cols
                    .remove(bs_k)
                    .map_err(|_| MetaError::EntityDelError)?;
            }
            //remove in tree0 for KEY_SYS_IDX_TABS if matched
            let qtn: Vec<&str> = qname.split(".").collect();
            if qtn.len() == 2 {
                //just for qualified tn
                let mut key_sd = vec![];
                key_sd.extend_from_slice(KEY_SYS_IDX_TABS.as_bytes());
                key_sd.extend_from_slice(qtn[0].as_bytes());
                key_sd.extend_from_slice(&id.to_be_bytes());
                self.tree0
                    .remove(&key_sd[..])
                    .map_err(|_| MetaError::EntityDelError)?;
            }
            //remove in tree0 for KEY_SYS_IDX_DBS if matched
            let mut key_sd = vec![];
            key_sd.extend_from_slice(KEY_SYS_IDX_DBS.as_bytes());
            key_sd.extend_from_slice(&id.to_be_bytes());
            self.tree0
                .remove(key_sd)
                .map_err(|_| MetaError::EntityDelError)?;
            Ok(id)
        } else {
            Err(MetaError::EntityDelError)
        }
    }

    fn new_table_info(&self, tid: Id, ti: &TableInfo) -> MetaResult<()> {
        self.insert_table_info_kv(tid, "cr", ti.create_script.as_str())?;
        self.insert_table_info_kv(tid, "en", ti.engine as u8)?;
        self.insert_table_info_kv(tid, "pa", ti.partition_keys_expr.as_str())?;
        self.insert_table_info_kv(tid, "pc", ti.partition_cols.as_str())?;
        for setting in ti.settings.iter() {
            let k = ["se", &setting.0].join("");
            self.insert_table_info_kv(tid, k.as_str(), setting.1.as_str())?;
        }
        Ok(())
    }

    pub fn get_table_info_create_script(
        &self,
        tid: Id,
    ) -> MetaResult<Option<IVec>> {
        self._get_table_info(tid, "cr")
    }
    pub fn get_table_info_partition_keys_expr(
        &self,
        tid: Id,
    ) -> MetaResult<Option<IVec>> {
        self._get_table_info(tid, "pa")
    }
    pub fn get_table_info_partition_cols(
        &self,
        tid: Id,
    ) -> MetaResult<Option<IVec>> {
        self._get_table_info(tid, "pc")
    }
    pub fn get_table_info_setting(
        &self,
        tid: Id,
        setting_key: &str,
    ) -> MetaResult<Option<IVec>> {
        let k = ["se", setting_key].join("");
        self._get_table_info(tid, k.as_str())
    }

    fn _get_table_info(&self, tid: Id, k: &str) -> MetaResult<Option<IVec>> {
        let mut key: Vec<u8> = Vec::with_capacity(16);
        key.extend_from_slice(to_key_id_order(tid).as_bytes());
        key.extend_from_slice(k.as_bytes());
        self.tree_tabs.get(key).map_err(|_e| MetaError::GetError)
    }

    #[allow(dead_code)] 
    fn get_table_info_engine(&self, tid: Id) -> MetaResult<EngineType> {
        self._get_table_info_prim_int::<u8>(tid, "en")
            .map(|e| EngineType::from(e))
    }

    fn _get_table_info_prim_int<T: PrimInt>(
        &self,
        tid: Id,
        k: &str,
    ) -> MetaResult<T> {
        let mut key: Vec<u8> = Vec::with_capacity(16);
        key.extend_from_slice(to_key_id_order(tid).as_bytes());
        key.extend_from_slice(k.as_bytes());
        let r = self.tree_tabs.get(key).map_err(|_e| MetaError::GetError)?;
        if let Some(iv) = r {
            let bs = &*iv;
            if bs.len() == std::mem::size_of::<T>() {
                let ci = bs.into_ref::<T>();
                Ok(*ci)
            } else {
                //FIXME handle old version
                Err(MetaError::StoreGotTypeNotExpectedError)
            }
        } else {
            Err(MetaError::StoreGotTypeNotExpectedError)
        }
    }

    // tree_tabs:
    fn insert_table_info_kv<T: AsBytes>(
        &self,
        tid: Id,
        k: &str,
        v: T,
    ) -> MetaResult<()> {
        let mut key: Vec<u8> = Vec::with_capacity(16);
        key.extend_from_slice(to_key_id_order(tid).as_bytes());
        key.extend_from_slice(k.as_bytes());
        let r = self
            .tree_tabs
            .insert(key, v.as_bytes())
            .map_err(|_e| MetaError::InsertError)?;
        if r.is_some() {
            log::info!("{:?}", r.as_ref().unwrap());
        }
        debug_assert!(r.is_none());
        Ok(())
    }

    #[allow(dead_code)] 
    fn insert_cell<T: AsBytes, U: AsBytes>(
        &self,
        cid: Id,
        pk: U,
        value: T,
        is_index_needed: bool,
    ) -> MetaResult<()> {
        let mut k: Vec<u8> = Vec::new();
        k.extend_from_slice(&cid.to_be_bytes());
        k.extend_from_slice(pk.as_bytes());
        let mut v: Vec<u8> = Vec::new();
        v.extend_from_slice(value.as_bytes());

        let r = self
            .tree_cols
            .insert(k.as_slice(), v.as_slice())
            .map_err(|_e| MetaError::InsertError)?;
        debug_assert!(r.is_none());

        if is_index_needed {
            let mut k: Vec<u8> = Vec::new();
            k.extend_from_slice(&cid.to_be_bytes());
            k.extend_from_slice(value.as_bytes()); //FIXME when value is int and at key...
            k.extend_from_slice(pk.as_bytes()); //unused, as id for cell.k
            let r = self
                .tree_cols
                .insert(k.as_slice(), &[])
                .map_err(|_e| MetaError::InsertError)?;
            debug_assert!(r.is_none());
        }
        Ok(())
    }

    // pub fn _get_cell_value_ivec<T: AsBytes>(
    //     &self,
    //     cid: Id,
    //     pk: T,
    // ) -> MetaResult<Option<IVec>> {
    //     let mut k: Vec<u8> = Vec::new();
    //     k.extend_from_slice(&cid.to_be_bytes());
    //     k.extend_from_slice(pk.as_bytes());

    //     self.tree_cols
    //         .get(k.as_slice())
    //         .map_err(|e| MetaError::GetError)
    // }

    //
    // name          :string; pk?
    // ordinal       :u32;
    // data_type     :ColType;
    // tab           :Id;
    // db            :Id;
    // is_pk         :u8
    // pub fn col(name: &str, ordin: u32, data_type: BqlType, dbid: Id, tid: Id, is_pk: u8) {}
    // dbn,tn,cn(name/ordin/data_type)
    //      => cid
    //      => ordin
    //      => type
    pub fn create_table(&self, tab: &Table) -> MetaResult<Id> {
        let dn = &tab.dbname;
        if let Some(_) = self.id(&dn) {
            let tn = &tab.name;
            let tid = self.new_tab(dn, tn)?;
            self.new_table_info(tid, &tab.tab_info)?;
            for (colname, col_info) in &tab.columns {
                let cid = self.new_col(dn, tn, colname)?;
                let r = self
                    .tree_cols
                    .insert(&cid.to_be_bytes(), col_info.as_bytes())
                    .map_err(|_| MetaError::InsertError)?;
                debug_assert!(r.is_none());
            }
            Ok(tid)
        } else {
            Err(MetaError::DbNotExistedError)
        }
    }

    pub fn get_column_info(&self, cid: Id) -> MetaResult<Option<ColumnInfo>> {
        let r = self
            .tree_cols
            .get(&cid.to_be_bytes())
            .map_err(|_e| MetaError::InsertError)?;
        if let Some(iv) = r {
            let bs = &*iv;
            if std::mem::size_of::<ColumnInfo>() == bs.len() {
                let ci = bs.into_ref::<ColumnInfo>();
                Ok(Some(*ci))
            } else {
                //FIXME handle old version
                Err(MetaError::GenericError)
            }
        } else {
            Ok(None)
        }
    }


    pub fn dbid_by_name<T: AsRef<str>>(&self, name: T) -> Option<Id> {
        self.id(name)
    }

    pub fn tid_by_qname<T: AsRef<str>>(&self, qname: T) -> Option<Id> {
        self.id(qname)
    }

    pub fn cid_by_qname<T: AsRef<str>>(&self, qname: T) -> Option<Id> {
        self.id(qname)
    }

    #[allow(dead_code)] 
    fn pretty_print(&self) -> MetaResult<()> {
        let name = &*self.mdb.name();
        println!("mdb: {}", unsafe { std::str::from_utf8_unchecked(&*name) });

        println!("====== dump tree0 ======");
        for r in self.tree0.iter() {
            let (k, v) = r.map_err(|_| MetaError::InsertError)?;
            println!(
                "k: {:?}, v: {:?}",
                String::from_utf8_lossy(&*k),
                // &*k,
                &*v
            );
        }

        println!("====== dump tree1 ======");
        for r in self.tree1.iter() {
            let (k, v) = r.map_err(|_| MetaError::InsertError)?;
            println!(
                "k: {:?}, v: {:?}",
                // unsafe { std::str::from_utf8_unchecked(&*k) },
                &*k,
                &*v
            );
        }

        println!("====== dump tree_tabs ======");
        for r in self.tree_tabs.iter() {
            let (k, v) = r.map_err(|_| MetaError::InsertError)?;
            println!(
                "k: {:?}, v: {:?}",
                // unsafe { std::str::from_utf8_unchecked(&*k) },
                &*k,
                &*v
            );
        }

        println!("====== dump tree_cols ======");
        for r in self.tree_cols.iter() {
            let (k, v) = r.map_err(|_| MetaError::InsertError)?;
            println!(
                "k: {:?}, v: {:?}",
                // unsafe { std::str::from_utf8_unchecked(&*k) },
                &*k,
                &*v
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod unit_tests {
    use base::seq;
    use baselog::{Config, LevelFilter, TermLogger, TerminalMode};

    use super::*;
    use crate::errs::MetaResult;
    use std::env::temp_dir;
    use std::fs::remove_dir_all;
    use std::path::Path;

    fn prepare_db_dir() -> MetaResult<String> {
        // sled::Config::default().temporary(true).open().unwrap()
        let t = temp_dir();
        let tmp_dir = t.to_str().unwrap();
        println!("tmp_dir: {}", tmp_dir);
        let tmp_mdb = [tmp_dir, "m0"].join("/");
        let tmp_mdb_path = Path::new(&tmp_mdb);
        if tmp_mdb_path.exists() {
            remove_dir_all(tmp_mdb_path).unwrap();
            println!("to remove the existed tmp_mdb_path: {} ", tmp_mdb);
        }

        Ok(tmp_dir.to_string())
    }

    #[test]
    fn sanity_checks() -> MetaResult<()> {
        let mdb_dir = prepare_db_dir()?;

        let ms = MetaStore::new(&[mdb_dir])?;
        //add db
        let dbname = "test_db_01";
        let dbid = ms.new_db(dbname)?;
        assert!(dbid == 0);
        println!("dbid: {}", dbid);
        assert_eq!(dbid, ms.id(dbname).unwrap());
        assert_eq!(dbname.as_bytes(), &*(ms.dbname(dbid).unwrap()));

        //add tab
        let tabname = "test_2020-12-21";
        let tid = ms.new_tab(dbname, tabname)?;
        assert!(tid == 1);
        let full_tabname = to_qualified_key!(dbname, tabname);
        assert_eq!(tid, ms.id(&full_tabname).unwrap());
        assert_eq!(full_tabname.as_bytes(), &*(ms.full_tabname(tid).unwrap()));
        //add col
        let cname_prefix = "c_as_df_gh";
        let pkvalue_prefix = "pk_adsd_adsa_";
        for i in 0u64..5 {
            let cname = [cname_prefix, &i.to_string()].join("");
            // let full_colname = to_key_bytes!(dbname, tabname, &cname);
            let cid = ms.new_col(dbname, tabname, &cname)?;
            assert_eq!((i + 2), cid);
            let is_index = if cid == 5 { true } else { false };
            //add rows a.k.a. col_tuple
            ms.insert_cell(
                cid,
                (pkvalue_prefix.to_string() + &i.to_string()).as_str(),
                i,
                is_index,
            )?;
        }
        let res = ms.tree0.scan_prefix(&full_tabname);
        // println!("res.count: {}", res.by_ref());
        for r in res {
            let (k, v) = r.map_err(|_| MetaError::InsertError)?;
            println!(
                "k: {}, v: {:?}",
                unsafe { std::str::from_utf8_unchecked(&*k) },
                &*v
            );
        }

        println!("to iter all cols...");
        let res = ms.tree_cols.iter();
        // .scan_prefix(to_key_id_order(3).as_bytes());
        // println!("res.count: {}", res.by_ref());
        for r in res {
            let (k, v) = r.map_err(|_| MetaError::InsertError)?;
            println!(
                "k: {:?}, v: {:?}",
                // unsafe { std::str::from_utf8_unchecked(&*k) },
                &*k,
                &*v
            );
        }

        let cid_scan = 3u64;
        println!("to scan for cid={} ...", cid_scan);
        let res = ms
            .tree_cols
            .scan_prefix(to_key_id_order(cid_scan).as_bytes());
        for r in res {
            let (k, v) = r.map_err(|_| MetaError::InsertError)?;
            println!(
                "k: {:?}, v: {:?}",
                // unsafe { std::str::from_utf8_unchecked(&*k) },
                &*k,
                &*v
            );
        }

        //clean in reversed way
        println!("clean in reversed way...");
        let cname_prefix = "c_as_df_gh";
        for i in 0u64..5 {
            let cname = [cname_prefix, &i.to_string()].join("");
            let full_colname = to_qualified_key!(dbname, tabname, &cname);
            let cid = ms.cid_by_qname(&full_colname).unwrap();
            assert!(cid > 1);
            ms._del(&full_colname)?;
            assert_eq!(ms.get_column_info(cid)?, None);
        }

        ms._del(&full_tabname)?;
        assert_eq!(ms.id(&full_tabname), None);
        assert_eq!(ms.full_tabname(tid), None);
        ms._del(dbname)?;
        assert_eq!(ms.id(dbname), None);
        assert_eq!(ms.dbname(dbid), None);

        Ok(())
    }

    #[test]
    fn sanity_checks_v2() -> MetaResult<()> {
        #[allow(unused_must_use)]
        {
            TermLogger::init(
                LevelFilter::Info,
                Config::default(),
                TerminalMode::Mixed,
            );
        }

        let mut t = Table {
            name: "t_asdaskd_01".to_string(),
            dbname: "db_dasjhdjsa_01".to_string(),
            columns: vec![],
            tab_info: TableInfo {
                create_script: "create_123456\n".to_string(),
                engine: EngineType::BaseStorage,
                partition_keys_expr: "toYYYYMM(ds)".to_string(),
                partition_cols: "ds".to_string(),
                settings: seq![
                    "a".to_string() => "1".to_string(),
                    "b".to_string() => "1".to_string(),
                    "b".to_string() => "2".to_string(),
                ],
            },
        };
        for i in 1..=5 {
            t.columns.push((
                (String::from("col") + &i.to_string()),
                ColumnInfo {
                    data_type: BqlType::Decimal(9, 3),
                    is_primary_key: false,
                    is_nullable: true,
                    ordinal: i - 1,
                },
            ));
        }
        //
        let mdb_dir = prepare_db_dir()?;

        let ms = MetaStore::new(&[mdb_dir])?;
        //add db
        let dbname = &t.dbname;
        let dbid = ms.new_db(dbname)?;
        assert!(dbid == 0);
        println!("dbid: {}", dbid);
        let tid = ms.create_table(&t)?;

        println!("to iter all cols...");
        let res = ms.tree_cols.iter();
        // .scan_prefix(to_key_id_order(3).as_bytes());
        // println!("res.count: {}", res.by_ref());
        for r in res {
            let (k, _v) = r.map_err(|_| MetaError::InsertError)?;
            // println!(
            //     "k: {:?}, v: {:?}",
            //     // unsafe { std::str::from_utf8_unchecked(&*k) },
            //     &*k,
            //     &*v
            // );
            let cid = (&*k).into_key_id();
            let ci = ms.get_column_info(cid)?.unwrap();
            println!("v: {:?}", ci);
            assert!((cid - 2) as u32 == ci.ordinal);
        }


        println!("to iter all TabInfos...");
        let res = ms.tree_tabs.iter();
        // .scan_prefix(to_key_id_order(3).as_bytes());
        // println!("res.count: {}", res.by_ref());
        for r in res {
            let (k, v) = r.map_err(|_| MetaError::InsertError)?;
            println!(
                "k: {:?}, v: {:?}",
                // unsafe { std::str::from_utf8_unchecked(&*k) },
                &*k,
                &*v
            );
        }

        let cs = ms.get_table_info_create_script(tid)?.unwrap();
        assert_eq!(&*cs, b"create_123456\n");
        let eng = ms.get_table_info_engine(tid)?;
        assert_eq!(eng, EngineType::BaseStorage);
        let pakey = ms.get_table_info_partition_keys_expr(tid)?.unwrap();
        assert_eq!(&*pakey, b"toYYYYMM(ds)");

        let sa = ms.get_table_info_setting(tid, "a")?.unwrap();
        assert_eq!(&*sa, b"1");
        let sa = ms.get_table_info_setting(tid, "b")?.unwrap();
        assert_eq!(&*sa, b"2");
        let sa = ms.get_table_info_setting(tid, "c")?;
        assert!(sa.is_none());

        //
        let bc = ms.get_all_databases()?;
        println!("{:?}", bc);

        //
        let cis = ms.get_columns(dbname, &t.name)?;
        for ci in cis {
            println!("ci: {:?}", ci);
        }

        //
        println!("==========================");
        println!(
            "all tables in db={}: {:?}",
            dbname,
            ms.get_table_names(dbname)
        );
        ms.pretty_print()?;

        println!("----- to remove database {}", dbname);
        ms.remove_database(dbname)?;
        println!("----- database {} removed", dbname);
        ms.pretty_print()?;

        Ok(())
    }

    #[test]
    #[ignore]
    fn dump() -> MetaResult<()> {
        let mdb_dir = "/jin/tmp/tb_schema";
        let ms = MetaStore::new(&[mdb_dir])?;
        ms.pretty_print()?;
        Ok(())
    }
}
