/*
* need dedicated store for mm_part?
* FIXME add a size cache for tid+ptk?
* design: 
* assumed: only parts of store files are written in a time period
* (addr_mm_part, size_addr_mm_part), (size_part)
*
*/

use std::{
    lazy::SyncLazy,
    mem,
    sync::atomic::{fence, Ordering},
};

use crate::errs::{MetaError, MetaResult};
use base::{
    mem::MemAddr,
    mmap::{mm_anon, mm_file_ro, mm_mremap},
};
use libc::c_void;

unsafe impl Send for SyncPartAddrsCache {}
unsafe impl Sync for SyncPartAddrsCache {}

pub struct SyncPartAddrsCache(pub PartAddrsCache);
impl SyncPartAddrsCache {
    #[inline]
    pub fn pim(&self) -> &PartAddrsCache {
        &self.0
    }
}

// pub static PAC: SyncLazy<SyncPartAddrsCache> =
//     SyncLazy::new(|| SyncPartAddrsCache(PartAddrsCache::new().unwrap()));
/*
* addr_parts just for query
*/
pub struct PartAddrsCache {
    addr_parts: MemAddr, //NOTE in fact, MemAddr of MemAddr
    size_addr_parts: usize,
}

#[inline]
fn ensure_size_enough(
    addr: &mut *mut c_void,
    current_size: usize,
    allowed_size: usize,
    expectd_size: usize,
) -> MetaResult<()> {
    // let allowed_size = current_size.next_power_of_two();
    if expectd_size > allowed_size {
        let oa = *addr;
        let na = mm_mremap(oa, allowed_size, allowed_size * 2)?;
        *addr = na;
    }
    Ok(())
}

impl PartAddrsCache {
    pub fn new() -> MetaResult<PartAddrsCache> {
        let size_addr_parts = 1024 * 1024 * mem::size_of::<MemAddr>(); //FIXME as configurable
        let addr_parts = mm_anon(size_addr_parts)
            .map_err(|e| MetaError::WrappingBaseError(e))?;
        let pim = PartAddrsCache {
            addr_parts,
            size_addr_parts,
        };
        Ok(pim)
    }

    //=== part addrs
    #[inline(always)]
    pub fn get_addr_part_fd(&self, fd: u32) -> MetaResult<MemAddr> {
        self.ensure_size_enough(fd)?;
        unsafe {
            let addr = self.addr_parts.offset(fd as isize) as *mut MemAddr;
            Ok(*addr)
        }
    }

    //FIXME
    // #[inline(always)]
    // pub fn update_addr_part_fd(
    //     &self,
    //     fd: u32,
    //     new_addr: MemAddr,
    // ) -> MetaResult<()> {
    //     self.ensure_size_enough(fd)?;
    //     unsafe {
    //         let addr = self.addr_parts.offset(fd as isize) as *mut MemAddr;
    //         *addr = new_addr;
    //         fence(Ordering::SeqCst);
    //     }
    //     Ok(())
    // }

    // #[inline(always)]
    // pub fn reset_addr_part_fd(&self, fd: u32) -> MetaResult<()> {
    //     if fd as usize >= self.size_addr_parts {
    //         unsafe {
    //             let addr = self.addr_parts.offset(fd as isize) as *mut MemAddr;
    //             *addr = 0 as MemAddr;
    //             fence(Ordering::Release);
    //         }
    //     }
    //     Ok(())
    // }

    #[inline(always)]
    pub fn ensure_size_enough(&self, fd: u32) -> MetaResult<()> {
        if fd as usize >= self.size_addr_parts {
            Err(MetaError::NoEnoughCacheSpace)
            //FIXME need a lock guard
            // let old_siz = self.size_addr_parts;
            // let new_siz = old_siz * 2;
            // unsafe {
            //     let this = self as *const PartAddrsCache as *mut PartAddrsCache;
            //     (*this).addr_parts =
            //         mm_mremap(self.addr_parts, old_siz, new_siz)
            //             .map_err(|e| MetaError::WrappingBaseError(e))?;
            //     (*this).size_addr_parts = new_siz;
            // }
        } else {
            Ok(())
        }
    }
}
