use std::{
    ffi::CStr,
    io::{Error, ErrorKind},
    thread::park_timeout,
    time::Duration,
};

use base::{bytes_cat, mem::MemAddr, mmap::mm_file_ro};
use libc::close;

use crate::{
    errs::{MetaError, MetaResult},
    types::{AsBytes, BqlType, Id, IntoRef},
};

///
///## Basic designs and concepts about Parts:
///
///* Partition Tree:
///  store and manage the meta infos to partition data horizontally in TB.
///  All tables should have implicit or explicit partition infos
///  from their creations.
///
///* Part Store:
///  a implementation for the partition tree.
///  currently to use sled is not performant and could be changed in the future
///
///* Copa(Column Partition):
///  TB is columnar, so the partition is columnar, which is called as CoPa(is
///  short for Column Partition). Data in the CoPa are append-only and unordered.
///
///* Partition Key:
///  All tables should specify the partition keys implicitly or explicitly from
///  their creations.
///

//FIXME move to types mod?
#[derive(Debug)]
#[repr(C)]
pub struct CoPaInfo {
    pub addr: MemAddr,
    pub addr_om: MemAddr,
    pub size: usize, //WARN size is not the len of bytes, it is the size of that copa
    pub len_in_bytes: usize,
}

impl CoPaInfo {
    #[inline(always)]
    pub fn len_in_bytes(
        size: usize,
        col_typ: BqlType,
        cid: Id,
        ptk: u64,
        ps: &PartStore,
    ) -> MetaResult<usize> {
        match col_typ {
            BqlType::String => Ok(ps
                .get_copa_siz_in_bytes_int_ptk(cid, ptk)?
                .unwrap_or_default()),
            _ => Ok(size * (col_typ.size()? as usize)),
        }
    }

    #[inline(always)]
    pub fn len_in_bytes_om(size: usize) -> usize {
        size * std::mem::size_of::<u64>()
    }
}

#[inline(always)]
pub fn open_file_as_fd(fpath: &Vec<u8>) -> MetaResult<u32> {
    let part_file = CStr::from_bytes_with_nul(fpath)
        .map_err(|_| MetaError::GetPartInfoError)?;
    open(part_file)
}

// #[inline(always)]
// pub fn open_file_as_fd_append(fpath: &Vec<u8>) -> MetaResult<u32> {
//     let part_file = CStr::from_bytes_with_nul(fpath)
//         .map_err(|_| MetaError::GetPartInfoError)?;
//     open_append(part_file)
// }

#[inline]
pub fn ensure_table_path_existed(tid: Id, dp: &str) -> MetaResult<()> {
    //mkdir
    let mut bs_dn = [0u8; 12];
    let mut n = itoa::write(&mut bs_dn[..], tid)?;
    bs_dn[n] = b'/';
    n += 1;
    let dir_path = bytes_cat!(dp.as_bytes(), b"/", &bs_dn[..=n]);
    let dir = CStr::from_bytes_with_nul(&dir_path)
        .map_err(|_e| MetaError::GetPartInfoError)?;
    mkdir(dir)?;
    // log::debug!("mkdir : {:?} done!", &dir);
    Ok(())
}

#[inline]
pub fn get_part_path(
    tid: Id,
    cid: Id,
    ptk: u64,
    dp: &str,
) -> MetaResult<Vec<u8>> {
    //open file
    let mut bs_fn = [0u8; 56];
    let mut n = itoa::write(&mut bs_fn[..], tid)?;
    bs_fn[n] = b'/';
    n += 1;
    n += itoa::write(&mut bs_fn[n..], cid)?;
    bs_fn[n] = b'_';
    n += 1;
    n += itoa::write(&mut bs_fn[n..], ptk)?;

    // let fpath = bytes_cat!(dp.as_bytes(), b"/", &bs_fn[..=n]);
    let mut fpath = Vec::with_capacity(dp.len() + 1 + n + 1);
    fpath.extend_from_slice(dp.as_bytes());
    fpath.push(b'/');
    fpath.extend_from_slice(&bs_fn[..=n]);
    Ok(fpath)
}

#[inline]
pub fn gen_ompath_from_part_path(part_path: &Vec<u8>) -> MetaResult<Vec<u8>> {
    let mut ompath = Vec::with_capacity(part_path.len() + 2);
    ompath.extend_from_slice(part_path);
    let idx = ompath
        .iter()
        .position(|&e| e == 0)
        .ok_or(MetaError::OptionIsNoneButShouldNot)?;
    ompath.insert(idx, b'm');
    ompath.insert(idx, b'o');
    Ok(ompath)
}

pub struct PartStore<'a> {
    data_dirs: &'a Vec<String>,
    #[allow(dead_code)]
    mdb: sled::Db,
    tree_parts: sled::Tree,
    tree_prids: sled::Tree,
    tree_part_size: sled::Tree,
    tree_locks: sled::Tree,
}

impl<'a> PartStore<'a> {
    pub fn new<T: AsRef<str>>(
        meta_dirs: &[T],
        data_dirs: &'a Vec<String>,
    ) -> MetaResult<Self> {
        assert!(meta_dirs.len() > 0);
        assert!(data_dirs.len() > 0);

        let p0 = [meta_dirs[0].as_ref(), "p0"].join("/");
        let mdb = sled::Config::default()
            .path(p0)
            .cache_capacity(128 * 1024 * 1024) //FIXME configurable
            .open()
            .map_err(|_| MetaError::OpenError)?;
        let tree_part_size =
            mdb.open_tree(b"ps").map_err(|_| MetaError::OpenError)?;
        let tree_parts =
            mdb.open_tree(b"pt").map_err(|_| MetaError::OpenError)?;
        let tree_prids =
            mdb.open_tree(b"pr").map_err(|_| MetaError::OpenError)?;
        let tree_locks =
            mdb.open_tree(b"l").map_err(|_e| MetaError::OpenError)?;
        tree_locks.clear().map_err(|_e| MetaError::OpenError)?;

        Ok(PartStore {
            mdb,
            data_dirs,
            tree_parts,
            tree_prids,
            tree_part_size,
            tree_locks,
        })
    }

    //NOTE a table lock is implemented by acquire_lock, release_lock, is_locked
    pub fn acquire_lock(&self, tid: Id) -> MetaResult<()> {
        let kbs = tid.to_be_bytes();
        loop {
            if self.is_locked(tid)? {
                park_timeout(Duration::from_micros(1)); //FIXME
            }
            //try to lock
            let res_cas = self
                .tree_locks
                .compare_and_swap(kbs, Some(&[0]), Some(&[1]))
                .map_err(|_| MetaError::FailToLockTable)?;
            match res_cas {
                Ok(_) => break,
                Err(_) => continue,
            }
        }
        Ok(())
    }

    pub fn release_lock(&self, tid: Id) -> MetaResult<()> {
        self.tree_locks
            .insert(tid.to_be_bytes(), &[0])
            .map_err(|_| MetaError::GetPartInfoError)?;

        Ok(())
    }

    #[inline]
    fn is_locked(&self, tid: Id) -> MetaResult<bool> {
        let kbs = tid.to_be_bytes();
        let res = self
            .tree_locks
            .get(kbs)
            .map_err(|_| MetaError::FailToLockTable)?;
        match res {
            Some(iv) => {
                let v = *(&*iv).into_ref::<u8>();
                return Ok(v == 1);
            }
            None => {
                let res_cas = self
                    .tree_locks
                    .compare_and_swap(kbs, None as Option<&[u8]>, Some(&[0]))
                    .map_err(|_| MetaError::FailToLockTable)?;
                match res_cas {
                    Ok(_) => return Ok(true),
                    Err(_) => {
                        if let Some(iv) = self
                            .tree_locks
                            .get(kbs)
                            .map_err(|_| MetaError::ShouldHasLockButNot)?
                        {
                            let v = *(&*iv).into_ref::<u8>();
                            return Ok(v == 1);
                        } else {
                            return Err(MetaError::ShouldHasLockButNot);
                        }
                    }
                }
            }
        }
    }

    #[inline]
    pub fn get_prid_int_ptk(
        &self,
        tid: Id,
        ptk: u64,
        reserved_len: usize,
    ) -> MetaResult<usize> {
        let k = (tid.to_be(), ptk.to_be());
        let kbs = k.as_bytes();
        if let Ok(Some(v)) = self.tree_part_size.fetch_and_update(kbs, |old| {
            let old_num = match old {
                Some(bytes) => *bytes.into_ref::<usize>(),
                None => 0,
            };
            Some((old_num + reserved_len).as_bytes().to_vec())
        }) {
            Ok(*(&*v).into_ref::<usize>())
        } else {
            Ok(0)
        }
    }

    #[inline]
    pub fn set_copa_size_int_ptk(
        &self,
        tid: Id,
        ptk: u64,
        part_size: usize,
    ) -> MetaResult<()> {
        let k = (tid.to_be(), ptk.to_be());
        let kbs = k.as_bytes();
        self.tree_prids
            .insert(kbs, part_size.as_bytes())
            .map_err(|_| MetaError::InsertPartInfoError)?;
        Ok(())
    }

    //FIXME possibly add more metadata
    #[inline]
    pub fn insert_copa_int_ptk(
        &self,
        cid: Id,
        ptk: u64,
        siz_in_bytes: usize,
    ) -> MetaResult<()> {
        let k = (cid.to_be(), ptk.to_be());
        let kbs = k.as_bytes();
        self.tree_parts
            .insert(kbs, siz_in_bytes.as_bytes())
            .map_err(|_| MetaError::InsertPartInfoError)?;
        Ok(())
    }

    #[inline]
    pub fn get_copa_siz_in_bytes_int_ptk(
        &self,
        cid: Id,
        ptk: u64,
    ) -> MetaResult<Option<usize>> {
        let k = (cid.to_be(), ptk.to_be());
        let kbs = k.as_bytes();
        let rt = self
            .tree_parts
            .get(kbs)
            .map_err(|_| MetaError::GetPartInfoError)?
            .map(|iv| *(&*iv).into_ref::<usize>());
        Ok(rt)
    }

    pub fn get_part_dir(&self, ptk: u64) -> &String {
        let dd = self.data_dirs;
        use base::hash::Hasher;
        let idx_dd = ptk.hash() as usize % dd.len();
        &dd[idx_dd]
    }

    pub fn fill_copainfos_int_by_ptk_range(
        &self,
        copass_ret: &mut Vec<Vec<CoPaInfo>>,
        tid: Id,
        cis: &Vec<(Id, BqlType)>,
        ptk_s: u64,
        ptk_e: u64,
    ) -> MetaResult<()> {
        let parts_tree = &self.tree_parts;

        for (cid, col_typ) in cis {
            let k = (cid.to_be(), ptk_s.to_be());
            let kbs0 = k.as_bytes();
            let k = (cid.to_be(), ptk_e.to_be());
            let kbs1 = k.as_bytes();

            let mut cps = Vec::new();
            let it = parts_tree.range(kbs0..=kbs1);
            for res in it {
                if let Ok((kbs, _v)) = res {
                    let (_, ptk_be) = *(&*kbs).into_ref::<(u64, u64)>();
                    //
                    let k = (tid.to_be(), ptk_be);
                    let kbs = k.as_bytes();
                    let ptk = ptk_be.to_be();
                    let iv_part_siz = self
                        .tree_prids
                        .get(kbs)
                        .map_err(|_| MetaError::GetPartInfoError)?
                        .ok_or(MetaError::CanNotFindPartError)?;
                    let size = *(&*iv_part_siz).into_ref::<usize>();
                    let dp = self.get_part_dir(ptk);
                    let fpath = get_part_path(tid, *cid, ptk, dp)?;
                    // println!("fpath: {}", std::str::from_utf8(&fpath).unwrap());
                    let pfd = open_file_as_fd(&fpath)?;
                    let len_in_bytes =
                        CoPaInfo::len_in_bytes(size, *col_typ, *cid, ptk, self)?;
                    // log::debug!("copar size: {}, len: {}", size, len);
                    let addr = mm_file_ro(pfd, len_in_bytes)?;
                    //issue#22 add om
                    let addr_om = if matches!(*col_typ, BqlType::String) {
                        let ompath = gen_ompath_from_part_path(&fpath)?;
                        let fd_om = open_file_as_fd(&ompath)?;
                        mm_file_ro(fd_om, CoPaInfo::len_in_bytes_om(size))?
                    } else {
                        0 as MemAddr
                    };
                    unsafe {
                        close(pfd as i32);
                    }
                    cps.push(CoPaInfo {
                        addr,
                        addr_om,
                        size,
                        len_in_bytes,
                    })
                } else {
                    return Err(MetaError::GetPartInfoError);
                }
            }
            copass_ret.push(cps);
        }

        Ok(())
    }

    //FIXME to rework
    //TODO
    pub fn uncache_for_table(&self, _tid: Id, _cids: &[Id]) -> MetaResult<()> {
        Ok(())
    }

    //FIXME to rework
    //TODO
    pub fn flush(&self) -> MetaResult<()> {
        Ok(())
    }

    #[allow(dead_code)]
    fn pretty_print(&self) -> MetaResult<()> {
        let name = &*self.mdb.name();
        println!("psdb: {}", unsafe { std::str::from_utf8_unchecked(&*name) });

        println!("====== dump tree_prids ======");
        let mut ss = 0;
        for r in self.tree_prids.iter() {
            let (k, v) = r.map_err(|_| MetaError::InsertError)?;
            let kbs = &*k;
            let (k0, k1) = kbs.into_ref::<(usize, usize)>();
            let prid = *(&*v).into_ref::<u64>();
            println!(
                "(tid, ptk): {:?}, prid: {}",
                (k0.to_be(), k1.to_be()),
                prid
            );
            ss += prid;
        }
        println!("total num of rows: {}", ss);

        println!("====== dump tree_part_size ======");
        for r in self.tree_part_size.iter() {
            let (k, v) = r.map_err(|_| MetaError::InsertError)?;
            let kbs = &*k;
            let (k0, k1) = kbs.into_ref::<(usize, usize)>();
            // let k0 = u64::from_be_bytes(
            //     kbs[..8].try_into().map_err(|_| MetaError::InsertError)?,
            // );
            // let k1 = u64::from_be_bytes(
            //     kbs[8..].try_into().map_err(|_| MetaError::InsertError)?,
            // );
            println!(
                "(tid, ptk): {:?}, size: {}",
                (k0.to_be(), k1.to_be()),
                (&*v).into_ref::<u64>()
            );
        }

        println!("====== dump tree_parts ======");
        for r in self.tree_parts.iter() {
            let (k, v) = r.map_err(|_| MetaError::InsertError)?;
            let kbs = &*k;
            let (k0, k1) = kbs.into_ref::<(usize, usize)>();
            // let k0 = u64::from_be_bytes(
            //     kbs[..8].try_into().map_err(|_| MetaError::InsertError)?,
            // );
            // let k1 = u64::from_be_bytes(
            //     kbs[8..].try_into().map_err(|_| MetaError::InsertError)?,
            // );
            println!(
                "(cid, ptk): {:?}, value: {}",
                (k0.to_be(), k1.to_be()),
                (&*v).into_ref::<u64>()
            );
        }

        // println!("====== dump tree_part_fd ======");
        // for r in self.cache_part_fds.iter() {
        //     let (k, v) = r.map_err(|_| MetaError::InsertError)?;
        //     let kbs = &*k;
        //     let k0 = u64::from_be_bytes(
        //         kbs[..8].try_into().map_err(|_| MetaError::InsertError)?,
        //     );
        //     let k1 = u64::from_be_bytes(
        //         kbs[8..].try_into().map_err(|_| MetaError::InsertError)?,
        //     );
        //     println!(
        //         "(cid, ptk): {:?}, fd: {}",
        //         (k0, k1),
        //         (&*v).into_ref::<u64>()
        //     );
        // }

        // println!("====== dump tree_part_fd_nm ======");
        // for r in self.tree_part_fd_nm.iter() {
        //     let (k, v) = r.map_err(|_| MetaError::InsertError)?;
        //     println!("(cid, ptk): {:?}, fd_nm: {:?}", &*k, &*v);
        // }

        // println!("====== dump tree_part_fd_om ======");
        // for r in self.tree_part_fd_om.iter() {
        //     let (k, v) = r.map_err(|_| MetaError::InsertError)?;
        //     println!(
        //         "(cid, ptk): {:?}, fd_om: {:?}",
        //         // unsafe { std::str::from_utf8_unchecked(&*k) },
        //         &*k,
        //         &*v
        //     );
        // }

        Ok(())
    }
}

fn mkdir(path: &CStr) -> MetaResult<()> {
    unsafe {
        let mode =
            libc::S_IRWXU | libc::S_IRWXG | libc::S_IROTH | libc::S_IXOTH;
        let res = libc::mkdir(path.as_ptr(), mode);
        if res < 0
            && !matches!(
                Error::last_os_error().kind(),
                ErrorKind::AlreadyExists
            )
        {
            Err(MetaError::WrappingIOError(Error::last_os_error()))
        } else {
            Ok(())
        }
    }
}

#[inline(always)]
fn open(path: &CStr) -> MetaResult<u32> {
    unsafe {
        let mode =
            libc::S_IRUSR | libc::S_IWUSR | libc::S_IRGRP | libc::S_IROTH;
        let fd = libc::open(
            path.as_ptr(),
            libc::O_CREAT | libc::O_RDWR | libc::O_NOATIME,
            mode,
        );
        if fd < 0 {
            Err(MetaError::WrappingIOError(Error::last_os_error()))
        } else {
            Ok(fd as u32)
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use baselog::{ConfigBuilder, LevelFilter, TermLogger, TerminalMode};
    use walkdir::WalkDir;

    use super::*;
    use crate::errs::MetaResult;
    use std::fs::create_dir_all;
    use std::path::Path;
    use std::{fs::remove_dir_all, time::Instant};

    fn prepare_dirs(tmp_dir: &str) -> MetaResult<(String, String)> {
        if Path::new(&tmp_dir).exists() {
            remove_dir_all(tmp_dir).unwrap();
            println!("to remove the existed tmp_dir: {} ", &tmp_dir);
        }
        create_dir_all(Path::new(&tmp_dir))?;

        let meta_dir = format!("{}/meta", tmp_dir);
        let data_dir = format!("{}/data", tmp_dir);
        println!("tmp_dir: {}", tmp_dir);
        if Path::new(&meta_dir).exists() {
            remove_dir_all(&meta_dir).unwrap();
            println!("to remove the existed meta_dir: {} ", &meta_dir);
        }
        if Path::new(&data_dir).exists() {
            remove_dir_all(&data_dir).unwrap();
            println!("to remove the existed data_dir: {} ", &data_dir);
        }
        create_dir_all(Path::new(&meta_dir))?;
        create_dir_all(Path::new(&data_dir))?;

        Ok((meta_dir, data_dir))
    }

    #[test]
    fn basic_checks_get_offset_int_ptk() -> MetaResult<()> {
        let (parts_dir, data_dir) = prepare_dirs("/jin/tmp/parts_test")?;
        let data_dirs = vec![data_dir];
        let ps = PartStore::new(&[parts_dir], &data_dirs)?;

        let tid = 123;
        let ptk = 20200202;
        for i in 0usize..100_000 {
            let ofs = ps.get_prid_int_ptk(tid, ptk, 20_000)?;
            assert_eq!(ofs, 20_000 * i);
        }

        Ok(())
    }

    #[test]
    fn basic_checks_get_fd_int() -> MetaResult<()> {
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

        let (parts_dir, data_dir) = prepare_dirs("/jin/tmp/parts_test")?;
        let data_dirs = vec![data_dir];
        let ps = PartStore::new(&[parts_dir], &data_dirs)?;

        for i in 1..1000 {
            let tid = 0;
            let cid = i;
            let ptk = 20200202;
            let dp = ps.get_part_dir(ptk);
            ensure_table_path_existed(tid, dp)?;
            let fpath = get_part_path(tid, cid, ptk, dp)?;
            let fd = open_file_as_fd(&fpath)?;
            assert!(fd > 0);
        }
        for i in 1001..2000 {
            let tid = 1000;
            let cid = i;
            let ptk = 20200202;
            let dp = ps.get_part_dir(ptk);
            ensure_table_path_existed(tid, dp)?;
            let fpath = get_part_path(tid, cid, ptk, dp)?;
            let fd = open_file_as_fd(&fpath)?;
            assert!(fd > 0);
        }
        let mut num_files_count = 0;
        for entry in WalkDir::new(&data_dirs[0]).max_depth(2) {
            let e = entry.unwrap();
            let fp = e.path();
            if fp.is_file() {
                num_files_count += 1;
            }
        }
        assert_eq!(num_files_count, 999 * 2);

        // ps.pretty_print()?;
        // assert_eq!(ps.cache_part_fds.len(), 999 * 2);

        // let tid = 0;
        // let cids: Vec<u64> = (1u64..1000).collect();
        // ps.uncache_for_table(tid, &cids)?;
        // assert_eq!(ps.cache_part_fds.len(), 999);

        // let tid = 1000;
        // let cids: Vec<u64> = (1001u64..2000).collect();
        // ps.uncache_for_table(tid, &cids)?;
        // assert_eq!(ps.cache_part_fds.len(), 0);

        // ps.pretty_print()?;

        Ok(())
    }

    #[test]
    fn test_create_part_path() -> MetaResult<()> {
        let (_parts_dir, data_dir) = prepare_dirs("/jin/tmp/parts_test")?;
        let mut bh = 0;
        let t = Instant::now();
        for tid in 0..10 {
            for cid in 0..100 {
                for ptk in 0..1000 {
                    let pp = get_part_path(tid, cid, ptk, data_dir.as_str())?;
                    let omp = gen_ompath_from_part_path(&pp)?;
                    let omp_len = omp.len();
                    assert_eq!(&omp[omp_len - 3..omp_len], b"om\0");
                    bh += pp.len();
                }
            }
        }
        assert_eq!(bh, 33790000);
        println!("bh: {} in {:?}", bh, t.elapsed());
        Ok(())
    }

    #[test]
    fn test_get_copainfos_int_by_ptk_range() -> MetaResult<()> {
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

        let (parts_dir, data_dir) = prepare_dirs("/jin/tmp/parts_test")?;
        let data_dirs = vec![data_dir];
        let ps = PartStore::new(&[parts_dir], &data_dirs)?;

        //prepare test data
        let tid = 0; //faked
        let cids = vec![(1u64, BqlType::UInt(32))]; //faked
        for (cid, _col_typ) in cids.iter() {
            for i in 0..10 {
                let ptk = 20200101 + i;
                let dp = ps.get_part_dir(ptk);
                ensure_table_path_existed(tid, dp)?;
                let fpath = get_part_path(tid, *cid, ptk, dp)?;
                let fd = open_file_as_fd(&fpath)?;
                assert!(fd > 0);
                ps.set_copa_size_int_ptk(tid, ptk, ptk as usize)?;
                ps.insert_copa_int_ptk(*cid, ptk, 0)?;
            }
        }


        let mut cpss = Vec::new();
        ps.fill_copainfos_int_by_ptk_range(&mut cpss, tid, &cids, 0, 20200105)?;
        let mut ct_part = 0;
        for cps in cpss {
            // println!("copa: {:?}", cpi);
            for cp in cps {
                assert!(cp.addr as u64 > 0);
                assert!(cp.size >= 20200101);
                // assert_eq!(cpi.cbtype, cbtype);
                ct_part += 1;
            }
        }
        assert_eq!(ct_part, 5);

        let cids = vec![(1u64, BqlType::UInt(32))]; //faked
        let mut cpss = Vec::new();
        ps.fill_copainfos_int_by_ptk_range(
            &mut cpss, tid, &cids, 20200102, 20200103,
        )?;
        let mut ct_part = 0;
        for cps in cpss {
            // println!("copa: {:?}", cpi);
            for cp in cps {
                assert!(cp.addr as u64 > 0);
                assert!(cp.size == 20200102 || cp.size == 20200103);
                // assert_eq!(cpi.cbtype, cbtype);
                ct_part += 1;
            }
        }
        assert_eq!(ct_part, 2);

        Ok(())
    }

    #[test]
    #[ignore]
    fn dump() -> MetaResult<()> {
        let parts_dir = "/jin/tmp/tb_schema";
        let data_dirs = vec!["/jin/tmp/tb_data".to_string()];
        let ps = PartStore::new(&[parts_dir], &data_dirs)?;
        ps.pretty_print()?;
        Ok(())
    }
}
