pub mod heapam;
pub mod heapam_visibility;
pub mod heapam_xlog;
pub mod heaptoast;
pub mod pruneheap;
pub mod vacuumlazy;
pub mod visibilitymap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeapWalPolicy {
    Wal,
    NoWal,
}

impl HeapWalPolicy {
    pub fn from_relpersistence(relpersistence: char) -> Self {
        if relpersistence == 'p' {
            Self::Wal
        } else {
            Self::NoWal
        }
    }
}
