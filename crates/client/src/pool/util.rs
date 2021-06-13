/// Server host addresses iterator
///
/// New establishing connection chooses random host address from
/// provided list. This implementation use round-robin strategy for
/// selecting. A host may be temporary unavailable, so we repeatedly
/// check it again up to 'repeat' count times .
pub(crate) struct AddrIter<'a> {
    hosts: &'a [String],
    start: u16,
    i: u16,
    max: u16,
}

impl<'a> AddrIter<'a> {
    #[inline]
    pub(super) fn new(hosts: &'a [String], start: usize, repeat: u8) -> AddrIter<'a> {
        AddrIter {
            hosts,
            i: 0,
            start: (start % hosts.len()) as u16,
            max: (hosts.len() * repeat as usize) as u16,
        }
    }
}

impl<'a> Iterator for AddrIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i < self.max {
            let r = &self.hosts[(self.i + self.start) as usize % (self.hosts.len())];
            self.i += 1;
            Some(r)
        } else {
            None
        }
    }
}

/// Connection pool status
/// - The number of idle connections in pool
/// - The total number of issued connections including idle and active
/// - The number of tasks that are waiting for available connection
#[derive(Debug)]
pub struct PoolInfo {
    pub idle: usize,
    pub issued: usize,
    pub wait: usize,
}
