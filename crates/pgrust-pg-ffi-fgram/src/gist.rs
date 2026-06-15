use core::ffi::c_void;

pub use crate::storage::{FirstOffsetNumber, InvalidOffsetNumber};
use crate::{uint16, BlockNumber, Datum, OffsetNumber, PageXLogRecPtr};

/// `GistNSN` -- a GiST node-sequence-number, an `XLogRecPtr` (gist.h).
pub type GistNSN = crate::XLogRecPtr;

/// `PageGistNSN` -- the unaligned (split) on-page representation of a `GistNSN`
/// (gist.h: `typedef PageXLogRecPtr PageGistNSN;`).
pub type PageGistNSN = PageXLogRecPtr;

/// `GIST_PAGE_ID` -- the page id stored in `GISTPageOpaqueData::gist_page_id`.
pub const GIST_PAGE_ID: uint16 = 0xFF81;

/// `GISTPageOpaqueData` -- the special area of every GiST page (gist.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GISTPageOpaqueData {
    /// this value must change on page split.
    pub nsn: PageGistNSN,
    /// next page if any.
    pub rightlink: BlockNumber,
    /// see bit definitions in `backend-access-gist`.
    pub flags: uint16,
    /// for identification of GiST indexes.
    pub gist_page_id: uint16,
}

const _: () = {
    // GISTPageOpaqueData: PageGistNSN (2*uint32 = 8) + BlockNumber (4)
    // + 2*uint16 (4) = 16.
    assert!(core::mem::size_of::<GISTPageOpaqueData>() == 16);
    assert!(core::mem::align_of::<GISTPageOpaqueData>() == 4);
};

pub type StrategyNumber = u16;
pub type Relation = *mut c_void;
pub type Page = *mut c_void;

pub const InvalidStrategy: StrategyNumber = 0;
pub const BTLessStrategyNumber: StrategyNumber = 1;
pub const BTLessEqualStrategyNumber: StrategyNumber = 2;
pub const BTEqualStrategyNumber: StrategyNumber = 3;
pub const BTGreaterEqualStrategyNumber: StrategyNumber = 4;
pub const BTGreaterStrategyNumber: StrategyNumber = 5;
pub const BtreeGistNotEqualStrategyNumber: StrategyNumber = 6;

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct GISTENTRY {
    pub key: Datum,
    pub rel: Relation,
    pub page: Page,
    pub offset: OffsetNumber,
    pub leafkey: bool,
}

impl GISTENTRY {
    pub const fn new(
        key: Datum,
        rel: Relation,
        page: Page,
        offset: OffsetNumber,
        leafkey: bool,
    ) -> Self {
        Self {
            key,
            rel,
            page,
            offset,
            leafkey,
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct GIST_SPLITVEC {
    pub spl_left: *mut OffsetNumber,
    pub spl_nleft: i32,
    pub spl_ldatum: Datum,
    pub spl_ldatum_exists: bool,
    pub spl_right: *mut OffsetNumber,
    pub spl_nright: i32,
    pub spl_rdatum: Datum,
    pub spl_rdatum_exists: bool,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct GistEntryVector {
    pub n: i32,
    pub vector: [GISTENTRY; 0],
}

/// `GISTNodeBufferPage` -- the on-temp-file page format used by the GiST
/// buffering build to spill node buffers to disk (gist_private.h).
///
/// `prev` links to the previous block of the same node buffer, `freespace` is
/// the remaining free space on the page, and the (flexibly-sized) tuple data
/// follows at `BUFFER_PAGE_DATA_OFFSET`.  A whole page is exactly `BLCKSZ`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct GISTNodeBufferPage {
    /// previous block of this node buffer, or `InvalidBlockNumber`.
    pub prev: BlockNumber,
    /// free space remaining on this page.
    pub freespace: u32,
    /// `char tupledata[FLEXIBLE_ARRAY_MEMBER];` -- the tuples start here.
    pub tupledata: [u8; 0],
}

const _: () = {
    // GISTNodeBufferPage header: BlockNumber (4) + uint32 (4) = 8 bytes.
    assert!(core::mem::size_of::<GISTNodeBufferPage>() == 8);
    assert!(core::mem::align_of::<GISTNodeBufferPage>() == 4);
};
