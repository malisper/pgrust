//! BRIN vocabulary shared by brin_tuple/brin_pageops/brin_minmax/brin/brin_xlog
//! and relscan (BrinOpaque), split out so the AM crates avoid cycles.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use core::cell::{Cell, RefCell};
use std::rc::Rc;

use ::datum::Datum;
use ::fmgr::FmgrInfo;
use ::mcx::MemoryContext;
use ::types_core::{BlockNumber, Buffer, InvalidBlockNumber, Oid, BLCKSZ};
use ::types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use ::types_tuple::itemptr::ItemPointerData;
use ::types_tuple::TupleDescData;

pub const BRIN_PAGETYPE_META: u16 = 0xF091;
pub const BRIN_PAGETYPE_REVMAP: u16 = 0xF092;
pub const BRIN_PAGETYPE_REGULAR: u16 = 0xF093;

pub const BRIN_EVACUATE_PAGE: u16 = 1 << 0;

pub const BRIN_CURRENT_VERSION: u16 = 1;
pub const BRIN_META_MAGIC: u32 = 0xA8109CFA;
pub const BRIN_METAPAGE_BLKNO: BlockNumber = 0;
pub const BRIN_DEFAULT_PAGES_PER_RANGE: BlockNumber = 128;
pub const BRIN_ALL_BLOCKRANGES: BlockNumber = InvalidBlockNumber;

pub const SizeOfBrinTuple: usize = 5;
pub const BRIN_OFFSET_MASK: u8 = 0x1F;
pub const BRIN_EMPTY_RANGE_MASK: u8 = 0x20;
pub const BRIN_PLACEHOLDER_MASK: u8 = 0x40;
pub const BRIN_NULLS_MASK: u8 = 0x80;

pub const BRIN_PROCNUM_OPCINFO: u16 = 1;
pub const BRIN_PROCNUM_ADDVALUE: u16 = 2;
pub const BRIN_PROCNUM_CONSISTENT: u16 = 3;
pub const BRIN_PROCNUM_UNION: u16 = 4;

// MAXALIGN(sizeof(BrinSpecialSpace)) == 8: type is the last u16 on the page,
// flags the u16 before it.
pub const BrinSpecialSpaceSize: usize = 8;

const TYPE_OFF: usize = BLCKSZ - 2;
const FLAGS_OFF: usize = BLCKSZ - 4;

const REVMAP_CONTENTS_OFF: usize = SizeOfPageHeaderData;
pub const REVMAP_CONTENT_SIZE: usize = BLCKSZ - REVMAP_CONTENTS_OFF - BrinSpecialSpaceSize;
pub const REVMAP_PAGE_MAXITEMS: usize =
    REVMAP_CONTENT_SIZE / core::mem::size_of::<ItemPointerData>();

pub const fn HEAPBLK_TO_REVMAP_BLK(
    pagesPerRange: BlockNumber,
    heapBlk: BlockNumber,
) -> BlockNumber {
    (heapBlk / pagesPerRange) / REVMAP_PAGE_MAXITEMS as BlockNumber
}

pub const fn HEAPBLK_TO_REVMAP_INDEX(pagesPerRange: BlockNumber, heapBlk: BlockNumber) -> usize {
    ((heapBlk / pagesPerRange) % REVMAP_PAGE_MAXITEMS as BlockNumber) as usize
}

#[inline]
fn page_read_u16(page: &PageRef<'_>, off: usize) -> u16 {
    // SAFETY: off < BLCKSZ, 2-aligned (all callers pass aligned constants).
    unsafe { page.as_ptr().add(off).cast::<u16>().read() }
}

#[inline]
fn page_write_u16(page: &mut PageMut<'_>, off: usize, v: u16) {
    // SAFETY: off < BLCKSZ, 2-aligned; caller holds exclusive page access.
    unsafe { page.as_ref().as_ptr().cast_mut().add(off).cast::<u16>().write(v) }
}

#[inline]
pub fn BrinPageType(page: &PageRef<'_>) -> u16 {
    page_read_u16(page, TYPE_OFF)
}

#[inline]
pub fn BrinSetPageType(page: &mut PageMut<'_>, ty: u16) {
    page_write_u16(page, TYPE_OFF, ty)
}

#[inline]
pub fn BrinPageFlags(page: &PageRef<'_>) -> u16 {
    page_read_u16(page, FLAGS_OFF)
}

#[inline]
pub fn BrinSetPageFlags(page: &mut PageMut<'_>, flags: u16) {
    page_write_u16(page, FLAGS_OFF, flags)
}

#[inline]
pub fn BRIN_IS_META_PAGE(page: &PageRef<'_>) -> bool {
    BrinPageType(page) == BRIN_PAGETYPE_META
}

#[inline]
pub fn BRIN_IS_REVMAP_PAGE(page: &PageRef<'_>) -> bool {
    BrinPageType(page) == BRIN_PAGETYPE_REVMAP
}

#[inline]
pub fn BRIN_IS_REGULAR_PAGE(page: &PageRef<'_>) -> bool {
    BrinPageType(page) == BRIN_PAGETYPE_REGULAR
}

#[derive(Clone, Copy, Debug)]
pub struct BrinMetaPageData {
    pub brinMagic: u32,
    pub brinVersion: u32,
    pub pagesPerRange: BlockNumber,
    pub lastRevmapPage: BlockNumber,
}

const META_OFF: usize = SizeOfPageHeaderData;
pub const SizeOfBrinMetaPageData: usize = 16;

#[inline]
pub fn brin_meta_read(page: &PageRef<'_>) -> BrinMetaPageData {
    // SAFETY: metapage contents at +24, 16B in-bounds, 4-aligned.
    unsafe {
        let p = page.as_ptr().add(META_OFF).cast::<u32>();
        BrinMetaPageData {
            brinMagic: p.read(),
            brinVersion: p.add(1).read(),
            pagesPerRange: p.add(2).read(),
            lastRevmapPage: p.add(3).read(),
        }
    }
}

#[inline]
pub fn brin_meta_write(page: &mut PageMut<'_>, meta: &BrinMetaPageData) {
    // SAFETY: as brin_meta_read; caller holds exclusive page access.
    unsafe {
        let p = page.as_ref().as_ptr().cast_mut().add(META_OFF).cast::<u32>();
        p.write(meta.brinMagic);
        p.add(1).write(meta.brinVersion);
        p.add(2).write(meta.pagesPerRange);
        p.add(3).write(meta.lastRevmapPage);
    }
}

const ITEMPTR_SIZE: usize = 6;

#[inline]
pub fn revmap_get_tid(page: &PageRef<'_>, index: usize) -> ItemPointerData {
    debug_assert!(index < REVMAP_PAGE_MAXITEMS);
    // SAFETY: rm_tids array bounds asserted; unaligned 6-byte read.
    unsafe {
        core::ptr::read_unaligned(
            page.as_ptr()
                .add(REVMAP_CONTENTS_OFF + index * ITEMPTR_SIZE)
                .cast::<ItemPointerData>(),
        )
    }
}

#[inline]
pub fn revmap_set_tid(page: &mut PageMut<'_>, index: usize, tid: ItemPointerData) {
    debug_assert!(index < REVMAP_PAGE_MAXITEMS);
    // SAFETY: as revmap_get_tid; caller holds exclusive page access.
    unsafe {
        core::ptr::write_unaligned(
            page.as_ref()
                .as_ptr()
                .cast_mut()
                .add(REVMAP_CONTENTS_OFF + index * ITEMPTR_SIZE)
                .cast::<ItemPointerData>(),
            tid,
        )
    }
}

#[inline]
pub fn brin_tuple_blkno(tup: &[u8]) -> BlockNumber {
    BlockNumber::from_ne_bytes(tup[0..4].try_into().unwrap())
}

#[inline]
pub fn brin_tuple_set_blkno(tup: &mut [u8], blkno: BlockNumber) {
    tup[0..4].copy_from_slice(&blkno.to_ne_bytes());
}

#[inline]
pub fn brin_tuple_info(tup: &[u8]) -> u8 {
    tup[4]
}

#[inline]
pub fn BrinTupleDataOffset(tup: &[u8]) -> usize {
    (brin_tuple_info(tup) & BRIN_OFFSET_MASK) as usize
}

#[inline]
pub fn BrinTupleHasNulls(tup: &[u8]) -> bool {
    brin_tuple_info(tup) & BRIN_NULLS_MASK != 0
}

#[inline]
pub fn BrinTupleIsPlaceholder(tup: &[u8]) -> bool {
    brin_tuple_info(tup) & BRIN_PLACEHOLDER_MASK != 0
}

#[inline]
pub fn BrinTupleIsEmptyRange(tup: &[u8]) -> bool {
    brin_tuple_info(tup) & BRIN_EMPTY_RANGE_MASK != 0
}

pub const XLOG_BRIN_CREATE_INDEX: u8 = 0x00;
pub const XLOG_BRIN_INSERT: u8 = 0x10;
pub const XLOG_BRIN_UPDATE: u8 = 0x20;
pub const XLOG_BRIN_SAMEPAGE_UPDATE: u8 = 0x30;
pub const XLOG_BRIN_REVMAP_EXTEND: u8 = 0x40;
pub const XLOG_BRIN_DESUMMARIZE: u8 = 0x50;
pub const XLOG_BRIN_OPMASK: u8 = 0x70;
pub const XLOG_BRIN_INIT_PAGE: u8 = 0x80;

pub const SizeOfBrinCreateIdx: usize = 6;
pub const SizeOfBrinInsert: usize = 10;
pub const SizeOfBrinUpdate: usize = 14;
pub const SizeOfBrinSamepageUpdate: usize = 2;
pub const SizeOfBrinRevmapExtend: usize = 4;
pub const SizeOfBrinDesummarize: usize = 10;

pub fn xl_brin_createidx(pagesPerRange: BlockNumber, version: u16) -> [u8; SizeOfBrinCreateIdx] {
    let mut b = [0u8; SizeOfBrinCreateIdx];
    b[0..4].copy_from_slice(&pagesPerRange.to_ne_bytes());
    b[4..6].copy_from_slice(&version.to_ne_bytes());
    b
}

pub fn xl_brin_insert(
    heapBlk: BlockNumber,
    pagesPerRange: BlockNumber,
    offnum: u16,
) -> [u8; SizeOfBrinInsert] {
    let mut b = [0u8; SizeOfBrinInsert];
    b[0..4].copy_from_slice(&heapBlk.to_ne_bytes());
    b[4..8].copy_from_slice(&pagesPerRange.to_ne_bytes());
    b[8..10].copy_from_slice(&offnum.to_ne_bytes());
    b
}

// oldOffnum at 0; the embedded xl_brin_insert member sits at offset 4
// (u32 member alignment); the 2-byte hole is zeroed.
pub fn xl_brin_update(
    oldOffnum: u16,
    heapBlk: BlockNumber,
    pagesPerRange: BlockNumber,
    offnum: u16,
) -> [u8; SizeOfBrinUpdate] {
    let mut b = [0u8; SizeOfBrinUpdate];
    b[0..2].copy_from_slice(&oldOffnum.to_ne_bytes());
    b[4..14].copy_from_slice(&xl_brin_insert(heapBlk, pagesPerRange, offnum));
    b
}

pub fn xl_brin_samepage_update(offnum: u16) -> [u8; SizeOfBrinSamepageUpdate] {
    offnum.to_ne_bytes()
}

pub fn xl_brin_revmap_extend(targetBlk: BlockNumber) -> [u8; SizeOfBrinRevmapExtend] {
    targetBlk.to_ne_bytes()
}

pub fn xl_brin_desummarize(
    pagesPerRange: BlockNumber,
    heapBlk: BlockNumber,
    regOffset: u16,
) -> [u8; SizeOfBrinDesummarize] {
    let mut b = [0u8; SizeOfBrinDesummarize];
    b[0..4].copy_from_slice(&pagesPerRange.to_ne_bytes());
    b[4..8].copy_from_slice(&heapBlk.to_ne_bytes());
    b[8..10].copy_from_slice(&regOffset.to_ne_bytes());
    b
}

pub struct XlBrinCreateIdx {
    pub pagesPerRange: BlockNumber,
    pub version: u16,
}

pub fn decode_createidx(d: &[u8]) -> XlBrinCreateIdx {
    XlBrinCreateIdx {
        pagesPerRange: u32::from_ne_bytes(d[0..4].try_into().unwrap()),
        version: u16::from_ne_bytes(d[4..6].try_into().unwrap()),
    }
}

pub struct XlBrinInsert {
    pub heapBlk: BlockNumber,
    pub pagesPerRange: BlockNumber,
    pub offnum: u16,
}

pub fn decode_insert(d: &[u8]) -> XlBrinInsert {
    XlBrinInsert {
        heapBlk: u32::from_ne_bytes(d[0..4].try_into().unwrap()),
        pagesPerRange: u32::from_ne_bytes(d[4..8].try_into().unwrap()),
        offnum: u16::from_ne_bytes(d[8..10].try_into().unwrap()),
    }
}

pub struct XlBrinUpdate {
    pub oldOffnum: u16,
    pub insert: XlBrinInsert,
}

pub fn decode_update(d: &[u8]) -> XlBrinUpdate {
    XlBrinUpdate {
        oldOffnum: u16::from_ne_bytes(d[0..2].try_into().unwrap()),
        insert: decode_insert(&d[4..14]),
    }
}

pub fn decode_samepage_update(d: &[u8]) -> u16 {
    u16::from_ne_bytes(d[0..2].try_into().unwrap())
}

pub fn decode_revmap_extend(d: &[u8]) -> BlockNumber {
    u32::from_ne_bytes(d[0..4].try_into().unwrap())
}

pub struct XlBrinDesummarize {
    pub pagesPerRange: BlockNumber,
    pub heapBlk: BlockNumber,
    pub regOffset: u16,
}

pub fn decode_desummarize(d: &[u8]) -> XlBrinDesummarize {
    XlBrinDesummarize {
        pagesPerRange: u32::from_ne_bytes(d[0..4].try_into().unwrap()),
        heapBlk: u32::from_ne_bytes(d[4..8].try_into().unwrap()),
        regOffset: u16::from_ne_bytes(d[8..10].try_into().unwrap()),
    }
}

// The closed opclass set (rule 4); the OPCINFO pg_amproc OID selects the arm.
pub const F_BRIN_MINMAX_OPCINFO: Oid = 3383;
pub const F_BRIN_MINMAX_MULTI_OPCINFO: Oid = 4616;
pub const F_BRIN_INCLUSION_OPCINFO: Oid = 4105;
pub const F_BRIN_BLOOM_OPCINFO: Oid = 4591;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BrinOpcKind {
    MinMax,
    MinMaxMulti,
    Bloom,
    Inclusion,
}

pub const BRIN_MAX_NSTORED: usize = 3;

pub const PG_BRIN_MINMAX_MULTI_SUMMARYOID: Oid = 4601;
pub const PG_BRIN_BLOOM_SUMMARYOID: Oid = 4600;

pub struct MinmaxOpaque {
    pub cached_subtype: Cell<Oid>,
    pub strategy_procinfos: RefCell<[Option<FmgrInfo>; 5]>,
}

impl Default for MinmaxOpaque {
    fn default() -> Self {
        MinmaxOpaque {
            cached_subtype: Cell::new(0),
            strategy_procinfos: RefCell::new([const { None }; 5]),
        }
    }
}

// BloomOpaque/InclusionOpaque: FmgrInfos resolve lazily on first use as in
// C, so a broken opclass errors at the same point C does (rule-5 caches).
#[derive(Default)]
pub struct BloomOpaque {
    pub hash_procinfo: RefCell<Option<FmgrInfo>>,
}

pub const INCLUSION_MAX_PROCNUMS: usize = 4;
pub const RT_MAX_STRATEGY: usize = 30;

pub struct InclusionOpaque {
    pub extra_procinfos: RefCell<[Option<FmgrInfo>; INCLUSION_MAX_PROCNUMS]>,
    pub extra_proc_missing: Cell<[bool; INCLUSION_MAX_PROCNUMS]>,
    pub cached_subtype: Cell<Oid>,
    pub strategy_procinfos: RefCell<[Option<FmgrInfo>; RT_MAX_STRATEGY]>,
}

impl Default for InclusionOpaque {
    fn default() -> Self {
        InclusionOpaque {
            extra_procinfos: RefCell::new([const { None }; INCLUSION_MAX_PROCNUMS]),
            extra_proc_missing: Cell::new([false; INCLUSION_MAX_PROCNUMS]),
            cached_subtype: Cell::new(0),
            strategy_procinfos: RefCell::new([const { None }; RT_MAX_STRATEGY]),
        }
    }
}

pub struct BrinColInfo {
    pub oi_nstored: u16,
    // Parsed opclass options image for this column (C: each support fn's
    // flinfo carries it; the direct-dispatch port carries it once here).
    pub oi_opclass_options: Option<Box<[u8]>>,
    pub oi_regular_nulls: bool,
    pub kind: BrinOpcKind,
    pub oi_typids: [Oid; BRIN_MAX_NSTORED],
    pub minmax: MinmaxOpaque,
    pub distance_procinfo: RefCell<Option<FmgrInfo>>,
    pub bloom: Option<Box<BloomOpaque>>,
    pub inclusion: Option<Box<InclusionOpaque>>,
}

pub struct MinMaxMultiRanges {
    pub typid: Oid,
    pub colloid: Oid,
    pub attno: u16,
    pub cmp: FmgrInfo,
    pub nranges: i32,
    pub nsorted: i32,
    pub nvalues: i32,
    pub maxvalues: i32,
    pub target_maxvalues: i32,
    pub values: Vec<Datum>,
}

// BrinDesc (brin_internal.h). Owner structure holding droppy caches (Rc,
// RefCell) — std collections justified per the rd_supportinfo precedent.
pub struct BrinDesc<'mcx> {
    pub bd_tupdesc: Rc<TupleDescData<'mcx>>,
    pub bd_disktdesc: TupleDescData<'mcx>,
    pub bd_totalstored: usize,
    // Decode-once copies of bd_index fields the opclasses read (rule 6);
    // BrinDesc carries no Relation handle.
    pub bd_opfamily: Vec<Oid>,
    pub bd_opcintype: Vec<Oid>,
    pub bd_indcollation: Vec<Oid>,
    pub bd_pages_per_range: BlockNumber,
    pub bd_info: Vec<BrinColInfo>,
}

impl BrinDesc<'_> {
    #[inline]
    pub fn natts(&self) -> usize {
        self.bd_tupdesc.natts as usize
    }
}

pub struct BrinValues {
    pub bv_attno: u16,
    pub bv_hasnulls: bool,
    pub bv_allnulls: bool,
    pub bv_values: [Datum; BRIN_MAX_NSTORED],
    pub bv_mem_value: Option<Box<MinMaxMultiRanges>>,
}

// BrinMemTuple; bt_context owns the datumCopy'd by-ref values and is reset by
// brin_memtuple_initialize exactly as C resets the dtuple context.
pub struct BrinMemTuple {
    pub bt_placeholder: bool,
    pub bt_empty_range: bool,
    pub bt_blkno: BlockNumber,
    pub bt_context: MemoryContext,
    // Deform output scratch with retained capacity (bt_values/bt_allnulls/
    // bt_hasnulls in C).
    pub bt_values: Vec<Datum>,
    pub bt_allnulls: Vec<bool>,
    pub bt_hasnulls: Vec<bool>,
    pub bt_columns: Vec<BrinValues>,
}

// BrinRevmap (brin_revmap.c); rm_irel is threaded as a parameter instead.
// Buffers are raw pinned ids released by brinRevmapTerminate (resowner
// releases them on abort, as in C).
pub struct BrinRevmap {
    pub rm_pagesPerRange: BlockNumber,
    pub rm_lastRevmapPage: Cell<BlockNumber>,
    pub rm_metaBuf: Buffer,
    pub rm_currBuf: Cell<Buffer>,
}

pub struct BrinOpaque<'mcx> {
    pub bo_pagesPerRange: BlockNumber,
    pub bo_rmAccess: BrinRevmap,
    pub bo_bdesc: BrinDesc<'mcx>,
}

pub struct BrinInsertState<'mcx> {
    pub bis_rmAccess: BrinRevmap,
    pub bis_desc: BrinDesc<'mcx>,
    pub bis_pages_per_range: BlockNumber,
}
