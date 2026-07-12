//! GiST vocabulary shared by the gist AM, gist_xlog redo, and relscan
//! (access/gist.h, access/gist_private.h, access/gistxlog.h).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use ::datum::Datum;
use ::types_core::xact::FullTransactionId;
use ::types_core::{
    uint16, BlockNumber, OffsetNumber, Oid, TransactionId, XLogRecPtr, BLCKSZ,
};
use ::types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};

pub mod pairingheap;
pub mod state;

pub const GIST_CONSISTENT_PROC: u16 = 1;
pub const GIST_UNION_PROC: u16 = 2;
pub const GIST_COMPRESS_PROC: u16 = 3;
pub const GIST_DECOMPRESS_PROC: u16 = 4;
pub const GIST_PENALTY_PROC: u16 = 5;
pub const GIST_PICKSPLIT_PROC: u16 = 6;
pub const GIST_EQUAL_PROC: u16 = 7;
pub const GIST_DISTANCE_PROC: u16 = 8;
pub const GIST_FETCH_PROC: u16 = 9;
pub const GIST_OPTIONS_PROC: u16 = 10;
pub const GIST_SORTSUPPORT_PROC: u16 = 11;
pub const GIST_TRANSLATE_CMPTYPE_PROC: u16 = 12;
pub const GISTNProcs: usize = 12;

pub const F_LEAF: uint16 = 1 << 0;
pub const F_DELETED: uint16 = 1 << 1;
pub const F_TUPLES_DELETED: uint16 = 1 << 2;
pub const F_FOLLOW_RIGHT: uint16 = 1 << 3;
pub const F_HAS_GARBAGE: uint16 = 1 << 4;

pub type GistNSN = XLogRecPtr;

pub const GistBuildLSN: XLogRecPtr = 1;
pub const GIST_PAGE_ID: uint16 = 0xFF81;
pub const GIST_ROOT_BLKNO: BlockNumber = 0;

pub const TUPLE_IS_VALID: u16 = 0xffff;
pub const TUPLE_IS_INVALID: u16 = 0xfffe;

pub const GIST_MAX_SPLIT_PAGES: usize = 75;
pub const GIST_MIN_FILLFACTOR: i32 = 10;
pub const GIST_DEFAULT_FILLFACTOR: i32 = 90;

pub const GIST_UNLOCK: i32 = 0;
pub const GIST_SHARE: i32 = 1;
pub const GIST_EXCLUSIVE: i32 = 2;

// Decoded view; on disk nsn is PageGistNSN {xlogid HI u32, xrecoff LO u32}
// (pre-9.3 compat), NOT a native u64 — the codec below swaps halves.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GISTPageOpaqueData {
    pub nsn: GistNSN,
    pub rightlink: BlockNumber,
    pub flags: uint16,
    pub gist_page_id: uint16,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct GistPageOpaqueDisk {
    nsn_xlogid: u32,
    nsn_xrecoff: u32,
    rightlink: BlockNumber,
    flags: uint16,
    gist_page_id: uint16,
}

const _: () = assert!(core::mem::size_of::<GistPageOpaqueDisk>() == 16);

pub const GiSTPageSize: usize =
    BLCKSZ - SizeOfPageHeaderData - core::mem::size_of::<GistPageOpaqueDisk>();
pub const SizeOfGistPageOpaque: usize = core::mem::size_of::<GistPageOpaqueDisk>();

pub const GISTMaxIndexTupleSize: usize = (GiSTPageSize / 4 - 4) & !7;
pub const GISTMaxIndexKeySize: usize = GISTMaxIndexTupleSize - 8;

const PD_SPECIAL_OFF: usize = 16;

#[inline]
fn special_off(bytes: *const u8) -> usize {
    // SAFETY: pd_special is at a 2-aligned in-page offset (page contract).
    let off = unsafe { bytes.add(PD_SPECIAL_OFF).cast::<u16>().read() } as usize;
    debug_assert!(
        off >= SizeOfPageHeaderData && off <= BLCKSZ,
        "corrupt pd_special"
    );
    off.min(BLCKSZ - core::mem::size_of::<GistPageOpaqueDisk>())
}

#[inline]
pub fn page_opaque(page: &PageRef<'_>) -> GISTPageOpaqueData {
    // SAFETY: in-bounds (special_off clamps); unaligned read tolerates any
    // pd_special the clamp admits.
    let d = unsafe {
        page.as_ptr()
            .add(special_off(page.as_ptr()))
            .cast::<GistPageOpaqueDisk>()
            .read_unaligned()
    };
    GISTPageOpaqueData {
        nsn: ((d.nsn_xlogid as u64) << 32) | d.nsn_xrecoff as u64,
        rightlink: d.rightlink,
        flags: d.flags,
        gist_page_id: d.gist_page_id,
    }
}

#[inline]
pub fn page_opaque_set(page: &mut PageMut<'_>, opaque: GISTPageOpaqueData) {
    let off = special_off(page.as_ref().as_ptr());
    let d = GistPageOpaqueDisk {
        nsn_xlogid: (opaque.nsn >> 32) as u32,
        nsn_xrecoff: opaque.nsn as u32,
        rightlink: opaque.rightlink,
        flags: opaque.flags,
        gist_page_id: opaque.gist_page_id,
    };
    // SAFETY: in-bounds write to this page's special area.
    unsafe {
        page.as_mut_ptr()
            .add(off)
            .cast::<GistPageOpaqueDisk>()
            .write_unaligned(d);
    }
}

#[inline]
pub fn page_opaque_update(page: &mut PageMut<'_>, f: impl FnOnce(&mut GISTPageOpaqueData)) {
    let mut op = page_opaque(&page.as_ref());
    f(&mut op);
    page_opaque_set(page, op);
}

#[inline]
pub fn GistPageIsLeaf(page: &PageRef<'_>) -> bool {
    page_opaque(page).flags & F_LEAF != 0
}
#[inline]
pub fn GistPageIsDeleted(page: &PageRef<'_>) -> bool {
    page_opaque(page).flags & F_DELETED != 0
}
#[inline]
pub fn GistTuplesDeleted(page: &PageRef<'_>) -> bool {
    page_opaque(page).flags & F_TUPLES_DELETED != 0
}
#[inline]
pub fn GistPageHasGarbage(page: &PageRef<'_>) -> bool {
    page_opaque(page).flags & F_HAS_GARBAGE != 0
}
#[inline]
pub fn GistFollowRight(page: &PageRef<'_>) -> bool {
    page_opaque(page).flags & F_FOLLOW_RIGHT != 0
}
#[inline]
pub fn GistPageGetNSN(page: &PageRef<'_>) -> GistNSN {
    page_opaque(page).nsn
}
#[inline]
pub fn GistPageSetNSN(page: &mut PageMut<'_>, val: GistNSN) {
    page_opaque_update(page, |op| op.nsn = val);
}
#[inline]
pub fn GistPageGetRightLink(page: &PageRef<'_>) -> BlockNumber {
    page_opaque(page).rightlink
}
#[inline]
pub fn GistPageSetRightLink(page: &mut PageMut<'_>, blkno: BlockNumber) {
    page_opaque_update(page, |op| op.rightlink = blkno);
}

pub const GistDeletedPageContentsSize: usize = 8;

pub fn GistPageSetDeleted(page: &mut PageMut<'_>, deletexid: FullTransactionId) {
    page_opaque_update(page, |op| op.flags |= F_DELETED);
    let lower = SizeOfPageHeaderData + GistDeletedPageContentsSize;
    page.set_pd_lower(lower as u16);
    // SAFETY: 8-byte deleteXid at the start of page contents, in-bounds.
    unsafe {
        page.as_mut_ptr()
            .add(SizeOfPageHeaderData)
            .cast::<u64>()
            .write_unaligned(deletexid.to_u64());
    }
}

pub fn GistPageGetDeleteXid(page: &PageRef<'_>) -> FullTransactionId {
    debug_assert!(GistPageIsDeleted(page));
    if (page.pd_lower() as usize) >= SizeOfPageHeaderData + GistDeletedPageContentsSize {
        // SAFETY: pd_lower attests the deleteXid field is present.
        let v = unsafe {
            page.as_ptr()
                .add(SizeOfPageHeaderData)
                .cast::<u64>()
                .read_unaligned()
        };
        FullTransactionId::from_u64(v)
    } else {
        ::types_core::xact::FirstNormalFullTransactionId
    }
}

pub const XLOG_GIST_PAGE_UPDATE: u8 = 0x00;
pub const XLOG_GIST_DELETE: u8 = 0x10;
pub const XLOG_GIST_PAGE_REUSE: u8 = 0x20;
pub const XLOG_GIST_PAGE_SPLIT: u8 = 0x30;
pub const XLOG_GIST_PAGE_DELETE: u8 = 0x60;
pub const XLOG_GIST_ASSIGN_LSN: u8 = 0x70;

pub const SizeOfGistxlogPageUpdate: usize = 4;

#[derive(Clone, Copy, Debug, Default)]
pub struct GistxlogPageUpdate {
    pub ntodelete: u16,
    pub ntoinsert: u16,
}

impl GistxlogPageUpdate {
    #[inline]
    pub fn encode(&self) -> [u8; 4] {
        let mut b = [0u8; 4];
        b[0..2].copy_from_slice(&self.ntodelete.to_ne_bytes());
        b[2..4].copy_from_slice(&self.ntoinsert.to_ne_bytes());
        b
    }
    #[inline]
    pub fn decode(b: &[u8]) -> Self {
        Self {
            ntodelete: u16::from_ne_bytes([b[0], b[1]]),
            ntoinsert: u16::from_ne_bytes([b[2], b[3]]),
        }
    }
}

pub const SizeOfGistxlogDelete: usize = 8;

#[derive(Clone, Copy, Debug, Default)]
pub struct GistxlogDelete {
    pub snapshotConflictHorizon: TransactionId,
    pub ntodelete: u16,
    pub isCatalogRel: bool,
}

impl GistxlogDelete {
    #[inline]
    pub fn encode(&self) -> [u8; 8] {
        let mut b = [0u8; 8];
        b[0..4].copy_from_slice(&self.snapshotConflictHorizon.to_ne_bytes());
        b[4..6].copy_from_slice(&self.ntodelete.to_ne_bytes());
        b[6] = self.isCatalogRel as u8;
        b
    }
    #[inline]
    pub fn decode(b: &[u8]) -> Self {
        Self {
            snapshotConflictHorizon: TransactionId::from_ne_bytes([b[0], b[1], b[2], b[3]]),
            ntodelete: u16::from_ne_bytes([b[4], b[5]]),
            isCatalogRel: b[6] != 0,
        }
    }
}

pub const SizeOfGistxlogPageSplit: usize = 24;

#[derive(Clone, Copy, Debug, Default)]
pub struct GistxlogPageSplit {
    pub origrlink: BlockNumber,
    pub orignsn: GistNSN,
    pub origleaf: bool,
    pub npage: u16,
    pub markfollowright: bool,
}

impl GistxlogPageSplit {
    #[inline]
    pub fn encode(&self) -> [u8; 24] {
        let mut b = [0u8; 24];
        b[0..4].copy_from_slice(&self.origrlink.to_ne_bytes());
        b[8..16].copy_from_slice(&self.orignsn.to_ne_bytes());
        b[16] = self.origleaf as u8;
        b[18..20].copy_from_slice(&self.npage.to_ne_bytes());
        b[20] = self.markfollowright as u8;
        b
    }
    #[inline]
    pub fn decode(b: &[u8]) -> Self {
        Self {
            origrlink: BlockNumber::from_ne_bytes([b[0], b[1], b[2], b[3]]),
            orignsn: GistNSN::from_ne_bytes([
                b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
            ]),
            origleaf: b[16] != 0,
            npage: u16::from_ne_bytes([b[18], b[19]]),
            markfollowright: b[20] != 0,
        }
    }
}

pub const SizeOfGistxlogPageDelete: usize = 10;

#[derive(Clone, Copy, Debug, Default)]
pub struct GistxlogPageDelete {
    pub deleteXid: FullTransactionId,
    pub downlinkOffset: OffsetNumber,
}

impl GistxlogPageDelete {
    #[inline]
    pub fn encode(&self) -> [u8; 10] {
        let mut b = [0u8; 10];
        b[0..8].copy_from_slice(&self.deleteXid.to_u64().to_ne_bytes());
        b[8..10].copy_from_slice(&self.downlinkOffset.to_ne_bytes());
        b
    }
    #[inline]
    pub fn decode(b: &[u8]) -> Self {
        Self {
            deleteXid: FullTransactionId::from_u64(u64::from_ne_bytes([
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
            ])),
            downlinkOffset: OffsetNumber::from_ne_bytes([b[8], b[9]]),
        }
    }
}

pub const SizeOfGistxlogPageReuse: usize = 25;

pub const SizeOfGistxlogPage: usize = 8;

#[derive(Clone, Copy, Debug, Default)]
pub struct gistxlogPage {
    pub blkno: BlockNumber,
    pub num: i32,
}

impl gistxlogPage {
    #[inline]
    pub fn encode(&self) -> [u8; 8] {
        let mut b = [0u8; 8];
        b[0..4].copy_from_slice(&self.blkno.to_ne_bytes());
        b[4..8].copy_from_slice(&self.num.to_ne_bytes());
        b
    }
    #[inline]
    pub fn decode(b: &[u8]) -> Self {
        Self {
            blkno: BlockNumber::from_ne_bytes([b[0], b[1], b[2], b[3]]),
            num: i32::from_ne_bytes([b[4], b[5], b[6], b[7]]),
        }
    }
}

// The closed opclass set (gistproc) never dereferences C's rel/page members;
// leafness of the source page is threaded instead. rel_natts stands in for
// C's entry->rel->rd_att->natts (btree_gist penalty scaling): 0 everywhere
// except the copies call_penalty stamps.
#[derive(Clone, Copy, Debug)]
pub struct GISTENTRY {
    pub key: Datum,
    pub offset: OffsetNumber,
    pub leafkey: bool,
    pub page_is_leaf: bool,
    pub rel_natts: u16,
}

impl GISTENTRY {
    #[inline]
    pub fn init(key: Datum, offset: OffsetNumber, leafkey: bool, page_is_leaf: bool) -> Self {
        GISTENTRY {
            key,
            offset,
            leafkey,
            page_is_leaf,
            rel_natts: 0,
        }
    }
}

impl Default for GISTENTRY {
    fn default() -> Self {
        GISTENTRY {
            key: Datum::null(),
            offset: 0,
            leafkey: false,
            page_is_leaf: false,
            rel_natts: 0,
        }
    }
}

// PrepareSortSupportFromGistIndexRel handshake for non-builtin opclasses: the
// sortsupport proc receives a pointer to this shim (its "SortSupport") and
// installs a leaf-key comparator; tuplesort applies it with the column's
// collation on its mcx-threaded lane.
pub type GistSsupCmp =
    fn(Datum, Datum, Oid, ::mcx::Mcx<'_>) -> ::types_error::PgResult<i32>;

pub struct GistSortSupportShim {
    pub ssup_collation: Oid,
    pub comparator: Option<GistSsupCmp>,
}

pub struct GistEntryVector {
    pub n: i32,
    pub vector: Vec<GISTENTRY>,
}

impl GistEntryVector {
    pub fn with_len(n: usize) -> Self {
        GistEntryVector {
            n: n as i32,
            vector: vec![GISTENTRY::default(); n],
        }
    }
}

#[derive(Default)]
pub struct GistSplitVec {
    pub spl_left: Vec<OffsetNumber>,
    pub spl_ldatum: Datum,
    pub spl_ldatum_exists: bool,
    pub spl_right: Vec<OffsetNumber>,
    pub spl_rdatum: Datum,
    pub spl_rdatum_exists: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xlog_record_sizes_match_c() {
        assert_eq!(SizeOfGistxlogPageUpdate, 4);
        assert_eq!(SizeOfGistxlogDelete, 8);
        assert_eq!(SizeOfGistxlogPageSplit, 24);
        assert_eq!(SizeOfGistxlogPageDelete, 10);
        assert_eq!(SizeOfGistxlogPageReuse, 25);
        assert_eq!(SizeOfGistxlogPage, 8);
    }

    #[test]
    fn page_size_constants_match_c() {
        assert_eq!(GiSTPageSize, 8192 - 24 - 16);
        assert_eq!(GISTMaxIndexTupleSize, 2032);
        assert_eq!(GISTMaxIndexKeySize, 2024);
    }
}
