use meta::{
    store::parts::CoPaInfo,
    types::{BqlType, Id},
};


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
    pub fn pretty_print(&self) {
    }
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
