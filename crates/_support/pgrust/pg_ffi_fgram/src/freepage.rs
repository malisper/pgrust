use core::marker::PhantomData;

use crate::types::Size;

pub const FPM_PAGE_SIZE: Size = 4096;
pub const FPM_NUM_FREELISTS: usize = 129;

pub const FREE_PAGE_SPAN_LEADER_MAGIC: i32 = 0xea40_20f0_u32 as i32;

#[repr(C)]
pub union Relptr<T> {
    pub relptr_type: *mut T,
    pub relptr_off: Size,
    _marker: PhantomData<T>,
}

impl<T> Clone for Relptr<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Relptr<T> {}

impl<T> Default for Relptr<T> {
    fn default() -> Self {
        Self { relptr_off: 0 }
    }
}

impl<T> core::fmt::Debug for Relptr<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Relptr")
            .field("relptr_off", &self.offset())
            .finish()
    }
}

impl<T> Relptr<T> {
    pub const fn null() -> Self {
        Self { relptr_off: 0 }
    }

    pub fn offset(self) -> Size {
        unsafe { self.relptr_off }
    }

    pub fn is_null(self) -> bool {
        self.offset() == 0
    }
}

pub enum FreePageBtree {}

pub type RelptrFreePageBtree = Relptr<FreePageBtree>;
pub type RelptrFreePageManager = Relptr<FreePageManager>;
pub type RelptrFreePageSpanLeader = Relptr<FreePageSpanLeader>;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct FreePageSpanLeader {
    pub magic: i32,
    pub npages: Size,
    pub prev: RelptrFreePageSpanLeader,
    pub next: RelptrFreePageSpanLeader,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FreePageManager {
    pub self_: RelptrFreePageManager,
    pub btree_root: RelptrFreePageBtree,
    pub btree_recycle: RelptrFreePageSpanLeader,
    pub btree_depth: u32,
    pub btree_recycle_count: u32,
    pub singleton_first_page: Size,
    pub singleton_npages: Size,
    pub contiguous_pages: Size,
    pub contiguous_pages_dirty: bool,
    pub freelist: [RelptrFreePageSpanLeader; FPM_NUM_FREELISTS],
}

impl Default for FreePageManager {
    fn default() -> Self {
        Self {
            self_: Relptr::null(),
            btree_root: Relptr::null(),
            btree_recycle: Relptr::null(),
            btree_depth: 0,
            btree_recycle_count: 0,
            singleton_first_page: 0,
            singleton_npages: 0,
            contiguous_pages: 0,
            contiguous_pages_dirty: true,
            freelist: [Relptr::null(); FPM_NUM_FREELISTS],
        }
    }
}

impl FreePageManager {
    pub fn largest_contiguous_pages(&self) -> Size {
        self.contiguous_pages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_page_manager_layout_matches_c_header_order() {
        assert_eq!(core::mem::size_of::<RelptrFreePageManager>(), 8);
        assert_eq!(core::mem::offset_of!(FreePageManager, self_), 0);
        assert_eq!(core::mem::offset_of!(FreePageManager, btree_root), 8);
        assert_eq!(core::mem::offset_of!(FreePageManager, btree_recycle), 16);
        assert_eq!(core::mem::offset_of!(FreePageManager, btree_depth), 24);
        assert_eq!(
            core::mem::offset_of!(FreePageManager, btree_recycle_count),
            28
        );
        assert_eq!(
            core::mem::offset_of!(FreePageManager, singleton_first_page),
            32
        );
        assert_eq!(core::mem::offset_of!(FreePageManager, singleton_npages), 40);
        assert_eq!(core::mem::offset_of!(FreePageManager, contiguous_pages), 48);
        assert_eq!(
            core::mem::offset_of!(FreePageManager, contiguous_pages_dirty),
            56
        );
        assert_eq!(core::mem::offset_of!(FreePageManager, freelist), 64);
        assert_eq!(core::mem::size_of::<FreePageManager>(), 1096);
    }

    #[test]
    fn free_page_span_leader_uses_relative_links() {
        assert_eq!(core::mem::offset_of!(FreePageSpanLeader, magic), 0);
        assert_eq!(core::mem::offset_of!(FreePageSpanLeader, npages), 8);
        assert_eq!(core::mem::offset_of!(FreePageSpanLeader, prev), 16);
        assert_eq!(core::mem::offset_of!(FreePageSpanLeader, next), 24);
        assert_eq!(core::mem::size_of::<FreePageSpanLeader>(), 32);
        assert_eq!(core::mem::size_of::<RelptrFreePageSpanLeader>(), 8);
    }
}
