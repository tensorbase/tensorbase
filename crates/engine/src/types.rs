use std::time::Instant;

use base::mmap::mm_unmap;
use libc::c_void;
use meta::{
    store::parts::{CoPaInfo, PartStore},
    types::{BqlType, Id},
};

use crate::errs::{EngineError, EngineResult};

//FIXME experimental
// #[repr(C)]
// pub struct NativeColumn {
//     pub size: usize,
//     pub data: *mut u8,
//     pub null_map_size: *mut u8,
//     pub null_map: *mut u8,
//     pub offset_map_size: u64,
//     pub offset_map: *mut u32,
//     pub lc_dict_size: u64,
//     pub lc_dict: *mut u8,
//     pub ordinal: u32,
// }
pub trait IQueryState {
}

pub struct QueryState {
    pub copasss: Vec<Vec<Vec<CoPaInfo>>>,
    pub tz_offset: i32,
    pub tid: Id,
    pub cis: Vec<(Id, BqlType)>,
}

unsafe impl Send for QueryState {}
unsafe impl Sync for QueryState {}

impl QueryState {
    // #[inline]
    // pub fn get_results_ptr(&mut self) -> *mut *mut NativeColumn {
    //     (&mut self.ptr_results) as *mut *mut NativeColumn
    // }

    // #[inline]
    // pub fn get_copas(&mut self) -> &mut Vec<CoPaInfo> {
    //     &mut self.copas
    // }
    pub fn pretty_print(&self) {
    }

    // pub fn update_copass_from_partstore(
    //     &mut self,
    //     ps: &PartStore,
    //     ptk_s: u64,
    //     ptk_e: u64,
    // ) -> EngineResult<()> {
    //     debug_assert!(self.cis.len() > 0);
    //     //FIXME first drain out self.copass
    //     ps.fill_copainfos_int_by_ptk_range(
    //         &mut self.copass,
    //         self.tid,
    //         &self.cis,
    //         ptk_s,
    //         ptk_e,
    //     )?;
    //     if self.copass.len() == 0 {
    //         return Err(EngineError::UnexpectedDataLoadingError);
    //     }
    //     log::debug!(
    //         "got {} copas, with {} copa per copas",
    //         self.copass.len(),
    //         self.copass[0].len(),
    //     );
    //     Ok(())
    // }
}

impl Default for QueryState {
    fn default() -> Self {
        QueryState {
            copasss: Vec::new(),
            tz_offset: 0,
            tid: 0,
            cis: Vec::new(),
        }
    }
}

// impl Drop for QueryState {
//     fn drop(&mut self) {
//     }
// }

#[cfg(test)]
mod unit_tests {
    use super::*;
    use base::show_option_size;

    #[test]
    fn size_check() {
        show_option_size!(header);
        show_option_size!(QueryState);
    }
}
