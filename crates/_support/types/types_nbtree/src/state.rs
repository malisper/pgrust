use core::mem::MaybeUninit;

use ::datum::Datum;
use ::mcx::{Allocator, Mcx, PgBox, PgVec};
use ::types_core::{
    BlockNumber, Buffer, BufferIsValid, InvalidBlockNumber, InvalidBuffer, OffsetNumber, Size,
    XLogRecPtr,
};
use ::types_error::PgResult;
use ::types_fmgr::FmgrInfo;
use ::types_scan::scankey::ScanKeyData;
use ::types_scan::sdir::{NoMovementScanDirection, ScanDirection};
use ::types_storage::storage::LocationIndex;
use ::types_tuple::itemptr::ItemPointerData;

use crate::page::MaxTIDsPerBTreePage;

pub struct BTStackData<'mcx> {
    pub bts_blkno: BlockNumber,
    pub bts_offset: OffsetNumber,
    pub bts_parent: Option<&'mcx mut BTStackData<'mcx>>,
}

pub type BTStack<'mcx> = Option<&'mcx mut BTStackData<'mcx>>;

const _: () = assert!(!core::mem::needs_drop::<BTStackData>());

// C sizes scankeys[INDEX_MAX_KEYS] as a keysz-sized flexible array member; keysz = scankeys.len().
pub struct BTScanInsertData<'mcx> {
    pub heapkeyspace: bool,
    pub allequalimage: bool,
    pub anynullkeys: bool,
    pub nextkey: bool,
    pub backward: bool,
    pub scantid: Option<ItemPointerData>,
    pub scankeys: PgVec<'mcx, ScanKeyData>,
}

impl BTScanInsertData<'_> {
    #[inline]
    pub fn keysz(&self) -> i32 {
        self.scankeys.len() as i32
    }
}

pub struct BTInsertStateData<'mcx> {
    // C's IndexTuple pointer: the caller-built on-image index tuple bytes.
    pub itup: &'mcx [u8],
    pub itemsz: Size,
    pub itup_key: BTScanInsertData<'mcx>,
    pub buf: Buffer,
    pub bounds_valid: bool,
    pub low: OffsetNumber,
    pub stricthigh: OffsetNumber,
    pub postingoff: i32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BTScanPosItem {
    pub heapTid: ItemPointerData,
    pub indexOffset: OffsetNumber,
    pub tupleOffset: LocationIndex,
}

const _: () = assert!(core::mem::size_of::<BTScanPosItem>() == 10);

// items[] is MaybeUninit for C palloc parity: btbeginscan never zero-fills the
// two 13.6KB arrays; only [firstItem, lastItem] is ever written then read.
pub struct BTScanPosData {
    pub buf: Buffer,
    pub currPage: BlockNumber,
    pub prevPage: BlockNumber,
    pub nextPage: BlockNumber,
    pub lsn: XLogRecPtr,
    pub dir: ScanDirection,
    pub nextTupleOffset: i32,
    pub moreLeft: bool,
    pub moreRight: bool,
    pub firstItem: i32,
    pub lastItem: i32,
    pub itemIndex: i32,
    pub items: [MaybeUninit<BTScanPosItem>; MaxTIDsPerBTreePage],
}

const _: () = assert!(!core::mem::needs_drop::<BTScanPosData>());

impl BTScanPosData {
    /// # Safety
    /// `p` must be valid for writes of `Self`; `items` is left uninit.
    pub unsafe fn init_header(p: *mut Self) {
        (&raw mut (*p).buf).write(InvalidBuffer);
        (&raw mut (*p).currPage).write(InvalidBlockNumber);
        (&raw mut (*p).prevPage).write(InvalidBlockNumber);
        (&raw mut (*p).nextPage).write(InvalidBlockNumber);
        (&raw mut (*p).lsn).write(0);
        (&raw mut (*p).dir).write(NoMovementScanDirection);
        (&raw mut (*p).nextTupleOffset).write(0);
        (&raw mut (*p).moreLeft).write(false);
        (&raw mut (*p).moreRight).write(false);
        (&raw mut (*p).firstItem).write(0);
        (&raw mut (*p).lastItem).write(0);
        (&raw mut (*p).itemIndex).write(0);
    }

    /// # Safety
    /// `items[i]` must have been written since the position was loaded
    /// (readers stay within [firstItem, lastItem]).
    #[inline]
    pub unsafe fn item(&self, i: usize) -> BTScanPosItem {
        debug_assert!(i >= self.firstItem as usize && i <= self.lastItem as usize);
        self.items[i].assume_init()
    }

    #[inline]
    pub fn set_item(&mut self, i: usize, item: BTScanPosItem) {
        self.items[i].write(item);
    }
}

#[inline]
pub fn BTScanPosIsPinned(scanpos: &BTScanPosData) -> bool {
    debug_assert!(scanpos.currPage != InvalidBlockNumber || !BufferIsValid(scanpos.buf));
    BufferIsValid(scanpos.buf)
}

#[inline]
pub fn BTScanPosIsValid(scanpos: &BTScanPosData) -> bool {
    debug_assert!(scanpos.currPage != InvalidBlockNumber || !BufferIsValid(scanpos.buf));
    scanpos.currPage != InvalidBlockNumber
}

#[inline]
pub fn BTScanPosInvalidate(scanpos: &mut BTScanPosData) {
    scanpos.buf = InvalidBuffer;
    scanpos.currPage = InvalidBlockNumber;
}

// C divergence: callbacks drop C's unused Relation arg; by-ref skip support
// (uuid) is unimplemented, so no allocator is threaded either.
pub type SkipSupportIncDec = fn(Datum, &mut bool) -> Datum;

#[derive(Clone, Copy)]
pub struct SkipSupportData {
    pub low_elem: Datum,
    pub high_elem: Datum,
    pub decrement: SkipSupportIncDec,
    pub increment: SkipSupportIncDec,
}

// C divergence: low_compare/high_compare are owned copies of the arrayKeyData keys, not
// pointers into it (same mutability/lifetime). num_elems == -1 = skip array (elem_values empty).
pub struct BTArrayKeyInfo<'mcx> {
    pub scan_key: i32,
    pub num_elems: i32,
    pub elem_values: PgVec<'mcx, Datum>,
    pub cur_elem: i32,
    pub attlen: i16,
    pub attbyval: bool,
    pub null_elem: bool,
    pub sksup: Option<SkipSupportData>,
    pub low_compare: Option<ScanKeyData>,
    pub high_compare: Option<ScanKeyData>,
}


// C divergence: arrayContext is dropped — the PgVec allocations' 'mcx IS the scan-lifespan context.
pub struct BTScanOpaqueData<'mcx> {
    pub qual_ok: bool,
    pub numberOfKeys: i32,
    pub keyData: PgVec<'mcx, ScanKeyData>,

    pub numArrayKeys: i32,
    pub skipScan: bool,
    pub needPrimScan: bool,
    pub scanBehind: bool,
    pub oppositeDirCheck: bool,
    pub arrayKeys: PgVec<'mcx, BTArrayKeyInfo<'mcx>>,
    pub orderProcs: PgVec<'mcx, FmgrInfo>,

    pub killedItems: PgVec<'mcx, i32>,
    pub numKilled: i32,
    pub dropPin: bool,

    pub currTuples: Option<PgVec<'mcx, u8>>,
    pub markTuples: Option<PgVec<'mcx, u8>>,

    pub markItemIndex: i32,

    pub currPos: BTScanPosData,
    pub markPos: BTScanPosData,
}

impl<'mcx> BTScanOpaqueData<'mcx> {
    // btbeginscan's palloc + header assignment, in place (by-value Self = ~27KB stack memcpy).
    pub fn alloc_in(mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, Self>> {
        let layout = core::alloc::Layout::new::<Self>();
        let ptr = Allocator::allocate(&mcx, layout).map_err(|_| mcx.oom(layout.size()))?;
        let p = ptr.as_ptr() as *mut Self;
        // SAFETY: fresh allocation of `layout`; every non-MaybeUninit field is
        // written exactly once before the box is formed.
        unsafe {
            (&raw mut (*p).qual_ok).write(false);
            (&raw mut (*p).numberOfKeys).write(0);
            (&raw mut (*p).keyData).write(PgVec::new_in(mcx));
            (&raw mut (*p).numArrayKeys).write(0);
            (&raw mut (*p).skipScan).write(false);
            (&raw mut (*p).needPrimScan).write(false);
            (&raw mut (*p).scanBehind).write(false);
            (&raw mut (*p).oppositeDirCheck).write(false);
            (&raw mut (*p).arrayKeys).write(PgVec::new_in(mcx));
            (&raw mut (*p).orderProcs).write(PgVec::new_in(mcx));
            (&raw mut (*p).killedItems).write(PgVec::new_in(mcx));
            (&raw mut (*p).numKilled).write(0);
            (&raw mut (*p).dropPin).write(false);
            (&raw mut (*p).currTuples).write(None);
            (&raw mut (*p).markTuples).write(None);
            (&raw mut (*p).markItemIndex).write(-1);
            BTScanPosData::init_header(&raw mut (*p).currPos);
            BTScanPosData::init_header(&raw mut (*p).markPos);
            Ok(PgBox::from_raw_in(p, mcx))
        }
    }
}

pub struct BTReadPageState<'p> {
    pub minoff: OffsetNumber,
    pub maxoff: OffsetNumber,
    // High key (forward) / first non-pivot tuple (backward) bytes, borrowed from the
    // pinned leaf page like C's IndexTuple pointer; None on the rightmost/leftmost page.
    pub finaltup: Option<&'p [u8]>,
    pub page: &'p [u8],
    pub firstpage: bool,
    pub forcenonrequired: bool,
    pub startikey: i32,

    pub offnum: OffsetNumber,

    pub skip: OffsetNumber,
    pub continuescan: bool,

    pub rechecks: i16,
    pub targetdistance: i16,
    pub nskipadvances: i16,
}
