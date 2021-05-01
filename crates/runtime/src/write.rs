use std::{
    collections::HashMap,
    intrinsics::{copy_nonoverlapping, unlikely},
    mem, slice,
};

use base::mem::shape_slice;
use libc::{c_void, close};
use meta::{
    store::{
        parts::{
            ensure_table_path_existed, gen_ompath_from_part_path,
            get_part_path, open_file_as_fd, PartStore,
        },
        sys::MetaStore,
    },
    types::{BqlType, Id},
};

use crate::{
    ch::blocks::Block,
    errs::{BaseRtError, BaseRtResult},
    mgmt::BMS,
};
/*
*
*/
// pub fn write_part<T>(part_key: T) -> BaseRtResult<()> {
//     //get the part file

//     //if none, create and save it back

//     //append the block to the end of part

// }

// #[thread_local]
// static WB_BUF: Vec<u8> = Vec::<u8>::new();

// pub struct MemPart {
//     pub btype: BqlType,
//     pub data: Vec<u8>,
//     pub null_map: Option<Vec<u8>>,
//     pub offset_map: Option<Vec<u32>>,
//     // pub lc_dict_size: usize,
//     pub lc_dict_data: Option<Vec<u8>>,
// }

// struct MemColumn<T> {
//     cid: Id,
//     parts: DashMap<T, MemPart>, //TODO configurable
// }

// struct MemTable<T> {
//     tid: Id,
//     pid: T,
//     cols: Vec<MemColumn<T>>,
//     has_flushed: bool,
// }

pub fn write_block(
    blk: &Block,
    tab_ins: &str,
    tid_ins: Id,
) -> BaseRtResult<()> {
    debug_assert!(tab_ins.len() > 0);
    debug_assert!(tid_ins > 0);

    let ms = &BMS.meta_store;
    let ps = &BMS.part_store;
    log::debug!("tab_ins: {}, insert block: {:?}", tab_ins, blk);

    //scan for batching
    let ptks = ms
        .get_table_info_partition_cols(tid_ins)?
        .ok_or(BaseRtError::SchemaInfoShouldExistButNot)?;
    let parts: HashMap<u64, Vec<(u32, u32)>, BuildBaseHasher> =
        if ptks.len() == 0 {
            //no ptk cols
            let mut parts =
                HashMap::<u64, Vec<(u32, u32)>, BuildBaseHasher>::with_hasher(
                    BuildBaseHasher,
                ); //assumed blk dix < 4G
            parts.insert(0, vec![(0, (blk.nrows - 1) as u32)]);
            parts
        } else {
            gen_parts_by_ptk_names(ptks, blk, tab_ins, tid_ins)?
        };

    //write parts
    if parts.len() >= 1000 {
        //FIXME configurable
        log::debug!("parts: {:?}", parts.keys());
        return Err(BaseRtError::TooManyPartsPerBlock);
    }

    for (ptk, idxs) in parts {
        if has_blob_type_column(blk) {
            write_part_locked(blk, ptk, idxs, ms, ps, tab_ins, tid_ins)?;
        } else {
            write_part(blk, ptk, idxs, ms, ps, tab_ins, tid_ins)?;
        }
    }

    Ok(())
}

fn gen_parts_by_ptk_names(
    ptks: meta::store::sys::IVec,
    blk: &Block,
    tab_ins: &str,
    tid_ins: u64,
) -> Result<HashMap<u64, Vec<(u32, u32)>, BuildBaseHasher>, BaseRtError> {
    let cname_ptk = if ptks.len() == 0 {
        // vec![]
        ""
    } else {
        let bs_ptks = &*ptks;
        // bs_ptks[..bs_ptks.len() - 1].to_vec()
        unsafe { std::str::from_utf8_unchecked(&bs_ptks[..bs_ptks.len() - 1]) }
    };
    let mut ptk_idx = usize::MAX;
    for i in 0..blk.ncols {
        if &blk.columns[i].name == cname_ptk.as_bytes() {
            ptk_idx = i;
            break;
        }
    }
    if ptk_idx == usize::MAX {
        return Err(BaseRtError::NoPartitionKeyColumnFoundWhenInserting);
    }
    let col_ptk = &blk.columns[ptk_idx];
    let ptk_expr_fn_ptr = BMS.get_ptk_exps_fn_ptr(tab_ins, tid_ins)?;
    let ctyp_ptk = &col_ptk.data.btype;
    let cdata_ptk = &col_ptk.data.data;
    let nr = blk.nrows;
    if unlikely(nr >= u32::MAX as usize) {
        return Err(BaseRtError::TooBigBlockSize);
    }
    let parts = match ctyp_ptk {
        meta::types::BqlType::UInt(bits) => match bits {
            8 => gen_part_idxs(
                ptk_expr_fn_ptr as *const u8,
                ctyp_ptk,
                cdata_ptk,
                nr,
            )?,
            16 => {
                let cdata_ptk = shape_slice::<u16>(cdata_ptk);
                gen_part_idxs(
                    ptk_expr_fn_ptr as *const u8,
                    ctyp_ptk,
                    cdata_ptk,
                    nr,
                )?
            }
            32 => {
                let cdata_ptk = shape_slice::<u32>(cdata_ptk);
                gen_part_idxs(
                    ptk_expr_fn_ptr as *const u8,
                    ctyp_ptk,
                    cdata_ptk,
                    nr,
                )?
            }
            64 => {
                let cdata_ptk = shape_slice::<u64>(cdata_ptk);
                gen_part_idxs(
                    ptk_expr_fn_ptr as *const u8,
                    ctyp_ptk,
                    cdata_ptk,
                    nr,
                )?
            }
            _ => {
                return Err(BaseRtError::UnsupportedPartitionKeyType);
            }
        },
        meta::types::BqlType::DateTime => {
            let cdata_ptk = shape_slice::<u32>(cdata_ptk);
            gen_part_idxs(
                ptk_expr_fn_ptr as *const u8,
                ctyp_ptk,
                cdata_ptk,
                nr,
            )?
        }
        // meta::types::BqlType::Int(_) => {}
        // meta::types::BqlType::Decimal(_, _) => {}
        _ => {
            return Err(BaseRtError::UnsupportedPartitionKeyType);
        }
    };
    Ok(parts)
}

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

pub struct BuildBaseHasher;

impl std::hash::BuildHasher for BuildBaseHasher {
    type Hasher = BaseHasher;
    fn build_hasher(&self) -> BaseHasher {
        BaseHasher { state: 0 }
    }
}

#[inline]
pub unsafe fn transmute<A, B>(a: A) -> B {
    let b = ::core::ptr::read(&a as *const A as *const B);
    ::core::mem::forget(a);
    b
}

#[inline(always)]
fn gen_part_idxs<T: 'static + Sized + Copy>(
    ptk_expr_fn_ptr: *const u8,
    ctyp_ptk: &BqlType,
    cdata_ptk: &[T],
    nr: usize,
) -> BaseRtResult<HashMap<u64, Vec<(u32, u32)>, BuildBaseHasher>> {
    let mut parts =
        HashMap::<u64, Vec<(u32, u32)>, BuildBaseHasher>::with_hasher(
            BuildBaseHasher,
        ); //assumed blk dix < 4G
    // let siz_typ_ptk = mem::size_of::<T>();
    let ptk_expr_fn =
        unsafe { mem::transmute::<_, fn(T) -> u64>(ptk_expr_fn_ptr) };
    let is_datetime_typ = matches!(ctyp_ptk, BqlType::DateTime);
    let tz_ofs = BMS.timezone_sys_offset as i64;
    for j in 0..nr {
        //FIXME
        let ptk = if is_datetime_typ {
            let cd_ptk: i64 = unsafe { transmute(cdata_ptk[j]) };
            ptk_expr_fn(unsafe { transmute(cd_ptk + tz_ofs) })
        } else {
            ptk_expr_fn(cdata_ptk[j])
        };
        // parts.merge(ptk, j as u32);
        let jj = j as u32;
        let e = parts.entry(ptk).or_insert_with(Vec::new);
        let ropt = e.last_mut();
        match ropt {
            Some(r) if (r.1 + 1) == jj => {
                r.1 = jj;
            }
            _ => {
                e.push((jj, jj));
            }
        }
    }
    Ok(parts)
}

#[inline(always)]
fn count_len(idxs: &Vec<(u32, u32)>) -> usize {
    let mut blen = 0usize;
    for r in idxs {
        blen += (r.1 - r.0 + 1) as usize;
    }
    blen
}

#[inline(always)]
fn gather_into_buf(
    idxs: &Vec<(u32, u32)>,
    pt_len_in_bytes: usize,
    ptr_cdata: *const u8,
    ctyp_siz: usize,
) -> Vec<u8> {
    let mut bb = Vec::<u8>::with_capacity(pt_len_in_bytes);
    let mut i = 0usize;
    unsafe {
        for r in idxs {
            let rlen = r.1 - r.0 + 1;
            let sp = ptr_cdata.offset(r.0 as isize * ctyp_siz as isize);
            let dp = bb.as_mut_ptr().offset(i as isize * ctyp_siz as isize);
            copy_nonoverlapping(sp, dp, rlen as usize * ctyp_siz);
            i += rlen as usize;
        }
        bb.set_len(pt_len_in_bytes);
    }
    bb
}

#[inline(always)]
fn gather_into_blob_buf(
    idxs: &Vec<(u32, u32)>,
    ptr_cdata: *const u8,
    omdata: &Vec<u32>,
    siz_part: usize,
) -> (Vec<u8>, Vec<usize>) {
    debug_assert!(idxs.len() > 0);
    let mut bb = Vec::<u8>::new();
    let mut om = vec![0usize];
    for r in idxs {
        let oss = omdata[r.0 as usize];
        let len_last = omdata[(r.1 + 1) as usize] - omdata[r.1 as usize];
        let rlen_in_bytes = omdata[r.1 as usize] - oss + len_last;
        let v = unsafe {
            let ptr = ptr_cdata.offset(oss as isize);
            slice::from_raw_parts(ptr, rlen_in_bytes as usize)
        };
        bb.extend_from_slice(v);
        let oml = om.pop().unwrap();
        let mut os = 0;
        for i in r.0..=r.1 {
            os = oml + (omdata[i as usize] - oss) as usize;
            om.push(os);
        }
        om.push(os + len_last as usize);
    }
    for i in 0..om.len() {
        om[i] += siz_part;
    }
    let len_blob = bb.len();
    // unsafe {
    //     libc::pwrite(fd as i32, bb.as_ptr() as *const c_void, len_blob, 0);
    //     libc::pwrite(
    //         fd_om as i32,
    //         om.as_ptr() as *const c_void,
    //         om.len() * size_of::<u64>(),
    //         0,
    //     );
    //     close(fd as i32);
    //     close(fd_om as i32);
    // }
    (bb, om)
}

#[inline(always)]
fn has_blob_type_column(blk: &Block) -> bool {
    for i in 0..blk.ncols {
        let col = &blk.columns[i];
        let cchk = &col.data;
        let ctyp = cchk.btype;
        if matches!(ctyp, BqlType::String) {
            return true;
        }
    }
    return false;
}

#[inline(always)]
fn write_part(
    blk: &Block,
    ptk: u64,
    idxs: Vec<(u32, u32)>,
    ms: &MetaStore,
    ps: &PartStore,
    tab_ins: &str,
    tid: Id,
) -> BaseRtResult<()> {
    let dp = ps.get_part_dir(ptk);
    ensure_table_path_existed(tid, dp)?;

    let pt_len = count_len(&idxs);
    let prid = ps.get_prid_int_ptk(tid, ptk, pt_len)?;
    for i in 0..blk.ncols {
        let col = &blk.columns[i];
        let cname = unsafe { std::str::from_utf8_unchecked(&col.name) }; //FIXME
        let qcn = [tab_ins, cname].join(".");
        let cid = ms.cid_by_qname(qcn).ok_or(BaseRtError::ColumnNotExist)?;
        let fpath = get_part_path(tid, cid, ptk, dp)?;
        let fd = open_file_as_fd(&fpath)?;

        let cchk = &col.data;
        let ctyp = cchk.btype;
        let cdata = &cchk.data;

        //FIXME nm, om
        //write
        let ctyp_siz = ctyp.size()? as usize;
        let pt_len_in_bytes = pt_len * ctyp_siz;
        let offset_in_bytes = prid * ctyp_siz;
        //FIXME gather into bb
        let bb =
            gather_into_buf(&idxs, pt_len_in_bytes, cdata.as_ptr(), ctyp_siz);
        dump_buf(
            fd,
            offset_in_bytes,
            pt_len_in_bytes,
            bb.as_ptr() as *const c_void,
        );

        ps.insert_copa_int_ptk(cid, ptk, offset_in_bytes + pt_len_in_bytes)?;
    }
    ps.set_copa_size_int_ptk(tid, ptk, prid + pt_len)?;

    Ok(())
}

fn dump_buf(
    fd: u32,
    offset_in_bytes: usize,
    pt_len_in_bytes: usize,
    buf: *const c_void,
) {
    unsafe {
        libc::fallocate(
            fd as i32,
            0,
            offset_in_bytes as i64,
            pt_len_in_bytes as i64,
        );
        libc::pwrite(fd as i32, buf, pt_len_in_bytes, offset_in_bytes as i64);
        close(fd as i32);
    }
}

//TODO slow path, to boost the performance when we want
// #[inline(always)]
fn write_part_locked(
    blk: &Block,
    ptk: u64,
    idxs: Vec<(u32, u32)>,
    ms: &MetaStore,
    ps: &PartStore,
    tab_ins: &str,
    tid: Id,
) -> BaseRtResult<()> {
    let dp = ps.get_part_dir(ptk);
    ensure_table_path_existed(tid, dp)?;

    let pt_len = count_len(&idxs);

    ps.acquire_lock(tid)?;
    let prid = ps.get_prid_int_ptk(tid, ptk, pt_len)?;
    for i in 0..blk.ncols {
        let col = &blk.columns[i];
        let cname = unsafe { std::str::from_utf8_unchecked(&col.name) }; //FIXME
        let qcn = [tab_ins, cname].join(".");
        let cid = ms.cid_by_qname(qcn).ok_or(BaseRtError::ColumnNotExist)?;

        let cchk = &col.data;
        let ctyp = cchk.btype;

        match ctyp {
            BqlType::String => {
                let cdata = &cchk.data;
                let omdata = cchk.offset_map.as_ref().unwrap();
                let siz_in_bytes = ps
                    .get_copa_siz_in_bytes_int_ptk(cid, ptk)?
                    .unwrap_or_default();
                let (bb, om) = gather_into_blob_buf(
                    &idxs,
                    cdata.as_ptr(),
                    omdata,
                    siz_in_bytes,
                );

                let fpath = get_part_path(tid, cid, ptk, dp)?;
                let fd = open_file_as_fd(&fpath)?;
                let ompath = gen_ompath_from_part_path(&fpath)?;
                let fd_om = open_file_as_fd(&ompath)?;
                dump_buf(
                    fd,
                    siz_in_bytes,
                    bb.len(),
                    bb.as_ptr() as *const c_void,
                );
                dump_buf(
                    fd_om,
                    prid * mem::size_of::<u64>(),
                    om.len() * mem::size_of::<u64>(),
                    om.as_ptr() as *const c_void,
                );

                ps.insert_copa_int_ptk(cid, ptk, siz_in_bytes + bb.len())?;
            }
            _ => {
                let cdata = &cchk.data;
                let ctyp_siz = ctyp.size()? as usize;
                let pt_len_in_bytes = pt_len * ctyp_siz;
                let offset_in_bytes = prid * ctyp_siz;
                let bb = gather_into_buf(
                    &idxs,
                    pt_len_in_bytes,
                    cdata.as_ptr(),
                    ctyp_siz,
                );

                let fpath = get_part_path(tid, cid, ptk, dp)?;
                let fd = open_file_as_fd(&fpath)?;
                dump_buf(
                    fd,
                    offset_in_bytes,
                    pt_len_in_bytes,
                    bb.as_ptr() as *const c_void,
                );

                ps.insert_copa_int_ptk(
                    cid,
                    ptk,
                    offset_in_bytes + pt_len_in_bytes,
                )?;
            }
        }
    }
    ps.release_lock(tid)?;

    ps.set_copa_size_int_ptk(tid, ptk, prid + pt_len)?;

    Ok(())
}

#[cfg(test)]
mod unit_tests {
    use base::{
        mem::{shape_slice, shape_vec_u8},
        with_timer_print,
    };
    use baselog::{
        Config, ConfigBuilder, LevelFilter, TermLogger, TerminalMode,
    };
    use lightjit::builtins::to_fn1;
    use meta::{
        confs::Conf,
        types::{BaseChunk, BqlType, ColumnInfo, EngineType, Table, TableInfo},
    };
    use rand::prelude::*;
    use std::{
        env::{self},
        fs::{self, remove_dir_all, File},
        path::Path,
    };
    use walkdir::WalkDir;

    use crate::{ch::blocks::Column, errs::BaseRtResult, mgmt::BMS};

    use super::*;

    fn prepare_bms(tmp_dir: &str) -> BaseRtResult<()> {
        #[allow(unused_must_use)]
        {
            TermLogger::init(
                LevelFilter::Debug,
                ConfigBuilder::new()
                    .add_filter_ignore_str("sled")
                    .add_filter_ignore_str("cranelift")
                    .build(),
                TerminalMode::Mixed,
            );
        }

        // let t = temp_dir();
        // let tmp_dir = t.to_str().unwrap();
        println!("tmp_dir: {}", tmp_dir);
        let tmp_dir_path = Path::new(&tmp_dir);
        if tmp_dir_path.exists() {
            remove_dir_all(tmp_dir_path).unwrap();
            println!("to remove the existed tmp_dir_path: {} ", tmp_dir);
        }
        fs::create_dir(tmp_dir_path).unwrap();
        let tmp_conf_path = [tmp_dir, "test.conf"].join("/");
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_conf_path)
            .unwrap();
        let meta_dir = [tmp_dir, "meta"].join("/");
        let data_dir = [tmp_dir, "data"].join("/");
        fs::create_dir(&meta_dir).unwrap();
        fs::create_dir(&data_dir).unwrap();

        let conf0: Conf = toml::from_str(&format!(
            r#"[system]
            meta_dirs = ["{}"]
            data_dirs = ["{}"]
            
            [server]
            ip_addr = "127.0.0.1"
            # port = 8080
        "#,
            meta_dir, data_dir,
        ))
        .unwrap();
        // println!("{}", toml::to_string_pretty(&conf0)?);
        Conf::save(&conf0, Some(tmp_conf_path.as_str())).unwrap();

        env::set_var("BASE_DBG_CONF_OVERRIDE", &tmp_conf_path);

        assert_eq!(BMS.conf.system.meta_dirs[0], meta_dir);
        assert_eq!(BMS.conf.system.data_dirs[0], data_dir);

        let t = Table {
            name: "test_tab".to_string(),
            dbname: "test_db".to_string(),
            columns: vec![
                (
                    "col1".to_string(),
                    ColumnInfo {
                        data_type: BqlType::UInt(32),
                        is_primary_key: true,
                        is_nullable: false,
                        ordinal: 0,
                    },
                ),
                (
                    "col2".to_string(),
                    ColumnInfo {
                        data_type: BqlType::DateTime,
                        is_primary_key: false,
                        is_nullable: false,
                        ordinal: 0,
                    },
                ),
            ],
            tab_info: TableInfo {
                create_script: "not_important_here\n".to_string(),
                engine: EngineType::BaseStorage,
                partition_keys_expr: "toYYYYMMDD(col2)".to_string(),
                partition_cols: "col2,".to_string(),
                settings: Default::default(),
            },
        };

        let ms = &BMS.meta_store;
        let dbname = &t.dbname;
        let dbid = ms.new_db(dbname)?;
        assert_eq!(dbid, 4);
        println!("dbid: {}", dbid);
        let tid = ms.create_table(&t)?;

        Ok(())
    }

    #[test]
    fn test_get_ptk_exps_fn_ptr() -> BaseRtResult<()> {
        let bms_path = "/jin/tmp/xxx";
        let qtn = "test_db.test_tab";
        prepare_bms(bms_path)?;

        let tid = BMS
            .meta_store
            .tid_by_qname(qtn)
            .ok_or(BaseRtError::TableNotExist)?;
        let fn_ptr = BMS.get_ptk_exps_fn_ptr(qtn, tid)?;

        let toYYYYMMDD = to_fn1(fn_ptr);

        // println!("toYYYYMMDD(0): {}", toYYYYMMDD(0));
        assert_eq!(toYYYYMMDD(0), 19700101);

        let fn_ptr2 = BMS.get_ptk_exps_fn_ptr(qtn, tid)?;
        let toYYYYMMDD2 = to_fn1(fn_ptr2);
        assert_eq!(toYYYYMMDD2(1095379200), 20040917);

        assert_eq!(BMS.ptk_exprs_reg.len(), 1);

        Ok(())
    }

    #[ignore]
    #[test]
    fn stress_test_get_ptk_exps_fn_ptr() -> BaseRtResult<()> {
        let bms_path = "/jin/tmp/xxx";
        let qtn = "test_db.test_tab";
        prepare_bms(bms_path)?;

        let tid = BMS
            .meta_store
            .tid_by_qname("test_db.test_tab")
            .ok_or(BaseRtError::TableNotExist)?;
        let fn_ptr = BMS.get_ptk_exps_fn_ptr(qtn, tid)?;

        let toYYYYMMDD = to_fn1(fn_ptr);

        // println!("toYYYYMMDD(0): {}", toYYYYMMDD(0));
        assert_eq!(toYYYYMMDD(0), 19700101);

        let mut sum = 0;
        with_timer_print! {t0,
            for i in 0..40*1024*1024 {
                // fn_ptr = BMS.get_ptk_exps_fn_ptr(tid)?;
                let fn_ptr2 = BMS.get_ptk_exps_fn_ptr(qtn, tid)?;
                let toYYYYMMDD2 = to_fn1(fn_ptr2);
                sum += toYYYYMMDD2(i);
            }
        }
        println!("sum: {}", sum);
        // println!("1095379200's YYYYMMDD: {}", to_fn1(fn_ptr)(1095379200));
        Ok(())
    }

    #[test]
    fn test_write_block_integ() -> BaseRtResult<()> {
        let bms_path = "/jin/tmp/xxx";
        let qtn = "test_db.test_tab";
        prepare_bms(bms_path)?;

        //gen blk
        // let rng = rand::thread_rng();
        let col_len = 1024 * 1024usize;
        let cdata1: Vec<u32> = (0..col_len as u32).collect();
        let cdata2: Vec<u32> =
            cdata1.iter().map(|e| 2 * (*e) + 1354291200).collect();

        let mut blk = Block::default();
        let col1: Column = Column {
            name: b"col1".to_vec(),
            data: BaseChunk {
                btype: BqlType::UInt(32),
                size: col_len,
                data: shape_vec_u8(cdata1),
                null_map: None,
                offset_map: None,
                lc_dict_data: None,
            },
        };
        let col2: Column = Column {
            name: b"col2".to_vec(),
            data: BaseChunk {
                btype: BqlType::UInt(32),
                size: col_len,
                data: shape_vec_u8(cdata2),
                null_map: None,
                offset_map: None,
                lc_dict_data: None,
            },
        };
        blk.columns.push(col1);
        blk.columns.push(col2);
        blk.ncols = blk.columns.len();
        blk.nrows = col_len;

        let qtn = "test_db.test_tab";
        let tid = BMS
            .meta_store
            .tid_by_qname(qtn)
            .ok_or(BaseRtError::TableNotExist)?;

        write_block(&blk, qtn, tid)?;

        let mut siz_count = 0;
        let mut num_files_count = 0;
        for entry in WalkDir::new([bms_path, "data"].join("/")).into_iter()
        // .skip(3)
        {
            let e = entry.unwrap();
            let fp = e.path();
            if fp.is_file() {
                num_files_count += 1;
                let f = File::open(fp).unwrap();
                let flen = f.metadata().unwrap().len();
                assert!(flen > 0);
                siz_count += flen;
            }
        }
        assert_eq!(num_files_count, 50);
        assert_eq!(siz_count, 8 * 1024 * 1024);

        // println!("siz_count: {}", siz_count);
        Ok(())
    }

    #[test]
    fn test_count_len() -> BaseRtResult<()> {
        let idxps = vec![
            (0u32, 0u32),
            (2u32, 2u32),
            (5u32, 10u32),
            (12u32, 14u32),
            (15u32, 15u32),
        ];
        let len = count_len(&idxps);
        // println!("{}", len);
        assert_eq!(len, 12);
        Ok(())
    }

    #[test]
    fn test_gen_part_idxs() -> BaseRtResult<()> {
        let bms_path = "/jin/tmp/xxx";
        prepare_bms(bms_path)?;

        let mut rng = rand::thread_rng();
        let mut nums: Vec<u32> = (1354291200..1356969599).step_by(2).collect();
        nums.shuffle(&mut rng);
        let v = nums[0..1024 * 1024].to_vec();
        fn ptk_expr_fn_ptr(x: u64) -> u64 {
            (x - 1354291200) / 86400
        }

        let ctyp = BqlType::Int(32);
        println!("start to rand part ...");
        with_timer_print! {t1,
            let parts =
            gen_part_idxs(ptk_expr_fn_ptr as *const u8, &ctyp, &v, v.len())?;
          assert_eq!(parts.len(), 31);
          for (p, idxs) in parts {
              assert!(idxs.len() > 31000);
              // println!("parts: {}, len: {}", p, idxs.len());
              let pt_len = count_len(&idxs);
              let pt_len_in_bytes = pt_len * 4;
              let bb = gather_into_buf(
                  &idxs,
                  pt_len_in_bytes,
                  v.as_ptr() as *const u8,
                  4,
              );
              // println!("parts: {}, len: {}", p, bb.len());
              let bb = shape_slice::<u32>(&bb);
              for b in bb {
                  assert_eq!(ptk_expr_fn_ptr(*b as u64), p);
              }
          }
        }


        println!("start to seq part ...");

        let nums: Vec<u32> = (1354291200..1356969599).step_by(2).collect();
        let v = nums[0..1024 * 1024].to_vec();
        with_timer_print! {t1,
            let parts =
              gen_part_idxs(ptk_expr_fn_ptr as *const u8, &ctyp, &v, v.len())?;
            assert_eq!(parts.len(), 25);
            for (p, idxs) in parts {
                assert_eq!(idxs.len(), 1);
                // println!("parts: {}, len: {}", p, idxs.len());
                let pt_len = count_len(&idxs);
                let pt_len_in_bytes = pt_len * 4;
                let bb = gather_into_buf(
                    &idxs,
                    pt_len_in_bytes,
                    v.as_ptr() as *const u8,
                    4,
                );
                // println!("parts: {}, len: {}", p, bb.len());
                let bb = shape_slice::<u32>(&bb);
                for b in bb {
                    assert_eq!(ptk_expr_fn_ptr(*b as u64), p);
                }
            }
        }

        Ok(())
    }
}
