//! SP-GiST vocabulary (spgist.h + spgist_private.h + spgxlog.h): on-disk
//! tuple/page codecs, state carriers, opclass call frames, xlog records.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod state;
pub mod xlog;

use ::types_core::{BlockNumber, OffsetNumber, TransactionId, BLCKSZ};
use ::types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use ::types_tuple::itemptr::ItemPointerData;

pub const SPGIST_CONFIG_PROC: u16 = 1;
pub const SPGIST_CHOOSE_PROC: u16 = 2;
pub const SPGIST_PICKSPLIT_PROC: u16 = 3;
pub const SPGIST_INNER_CONSISTENT_PROC: u16 = 4;
pub const SPGIST_LEAF_CONSISTENT_PROC: u16 = 5;
pub const SPGIST_COMPRESS_PROC: u16 = 6;
pub const SPGIST_OPTIONS_PROC: u16 = 7;
pub const SPGISTNRequiredProc: usize = 5;
pub const SPGISTNProc: usize = 7;

pub const spgKeyColumn: usize = 0;
pub const spgFirstIncludeColumn: usize = 1;

pub const SPGIST_METAPAGE_BLKNO: BlockNumber = 0;
pub const SPGIST_ROOT_BLKNO: BlockNumber = 1;
pub const SPGIST_NULL_BLKNO: BlockNumber = 2;
pub const SPGIST_LAST_FIXED_BLKNO: BlockNumber = SPGIST_NULL_BLKNO;

#[inline]
pub const fn SpGistBlockIsRoot(blkno: BlockNumber) -> bool {
    blkno == SPGIST_ROOT_BLKNO || blkno == SPGIST_NULL_BLKNO
}

#[inline]
pub const fn SpGistBlockIsFixed(blkno: BlockNumber) -> bool {
    blkno <= SPGIST_LAST_FIXED_BLKNO
}

pub const SPGIST_META: u16 = 1 << 0;
pub const SPGIST_DELETED: u16 = 1 << 1;
pub const SPGIST_LEAF: u16 = 1 << 2;
pub const SPGIST_NULLS: u16 = 1 << 3;

pub const SPGIST_PAGE_ID: u16 = 0xFF82;
pub const SPGIST_MAGIC_NUMBER: u32 = 0xBA0BABEE;
pub const SPGIST_CACHED_PAGES: usize = 8;

pub const SPGIST_LIVE: u8 = 0;
pub const SPGIST_REDIRECT: u8 = 1;
pub const SPGIST_DEAD: u8 = 2;
pub const SPGIST_PLACEHOLDER: u8 = 3;

pub const SGITMAXNNODES: u32 = 0x1FFF;
pub const SGITMAXPREFIXSIZE: u32 = 0xFFFF;
pub const SGITMAXSIZE: u32 = 0xFFFF;

pub const GBUF_LEAF: i32 = 0x03;
pub const GBUF_NULLS: i32 = 0x04;
pub const GBUF_PARITY_MASK: i32 = 0x03;

#[inline]
pub const fn GBUF_INNER_PARITY(x: BlockNumber) -> i32 {
    (x % 3) as i32
}

#[inline]
pub const fn GBUF_REQ_LEAF(flags: i32) -> bool {
    (flags & GBUF_PARITY_MASK) == GBUF_LEAF
}

#[inline]
pub const fn GBUF_REQ_NULLS(flags: i32) -> bool {
    (flags & GBUF_NULLS) != 0
}

pub const SPGIST_MIN_FILLFACTOR: i32 = 10;
pub const SPGIST_DEFAULT_FILLFACTOR: i32 = 80;

#[inline]
pub const fn MAXALIGN(x: usize) -> usize {
    (x + 7) & !7
}

#[inline]
pub const fn MAXALIGN_DOWN(x: usize) -> usize {
    x & !7
}

pub const SIZEOF_SPGIST_PAGE_OPAQUE_DATA: usize = 8;
pub const SGITHDRSZ: usize = 8;
pub const SGNTHDRSZ: usize = 8;
pub const SGDTSIZE: usize = 16;
pub const SIZEOF_SPGIST_LEAF_TUPLE_DATA: usize = 12;
pub const SIZEOF_ITEM_ID_DATA: usize = 4;
pub const SIZEOF_DATUM: usize = 8;
pub const SIZEOF_INDEX_ATTRIBUTE_BITMAP_DATA: usize = 4;

#[inline]
pub const fn SGLTHDRSZ(hasnulls: bool) -> usize {
    if hasnulls {
        MAXALIGN(SIZEOF_SPGIST_LEAF_TUPLE_DATA + SIZEOF_INDEX_ATTRIBUTE_BITMAP_DATA)
    } else {
        MAXALIGN(SIZEOF_SPGIST_LEAF_TUPLE_DATA)
    }
}

pub const SPGIST_PAGE_CAPACITY: usize =
    MAXALIGN_DOWN(BLCKSZ - SizeOfPageHeaderData - MAXALIGN(SIZEOF_SPGIST_PAGE_OPAQUE_DATA));

// itup.h masks (node tuples reuse the IndexTupleData header).
pub const INDEX_SIZE_MASK: u16 = 0x1FFF;
pub const INDEX_VAR_MASK: u16 = 0x4000;
pub const INDEX_NULL_MASK: u16 = 0x8000;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
#[repr(C)]
pub struct SpGistPageOpaqueData {
    pub flags: u16,
    pub nRedirection: u16,
    pub nPlaceholder: u16,
    pub spgist_page_id: u16,
}

const _: () = assert!(core::mem::size_of::<SpGistPageOpaqueData>() == 8);

const PD_SPECIAL_OFF: usize = 16;

#[inline]
fn special_off(bytes: *const u8) -> usize {
    // SAFETY: pd_special is at a 2-aligned in-page offset (page contract).
    let off = unsafe { bytes.add(PD_SPECIAL_OFF).cast::<u16>().read() } as usize;
    debug_assert!(
        off >= SizeOfPageHeaderData && off <= BLCKSZ,
        "corrupt pd_special"
    );
    off.min(BLCKSZ - core::mem::size_of::<SpGistPageOpaqueData>())
}

#[inline]
pub fn page_opaque(page: &PageRef<'_>) -> SpGistPageOpaqueData {
    // SAFETY: in-bounds (special_off clamps); unaligned read.
    unsafe {
        page.as_ptr()
            .add(special_off(page.as_ptr()))
            .cast::<SpGistPageOpaqueData>()
            .read_unaligned()
    }
}

#[inline]
pub fn page_opaque_set(page: &mut PageMut<'_>, op: SpGistPageOpaqueData) {
    let off = special_off(page.as_ref().as_ptr());
    // SAFETY: in-bounds write to this page's special area.
    unsafe {
        page.as_mut_ptr()
            .add(off)
            .cast::<SpGistPageOpaqueData>()
            .write_unaligned(op)
    }
}

#[inline]
pub fn page_opaque_update(page: &mut PageMut<'_>, f: impl FnOnce(&mut SpGistPageOpaqueData)) {
    let mut op = page_opaque(&page.as_ref());
    f(&mut op);
    page_opaque_set(page, op);
}

#[inline]
pub fn SpGistPageIsMeta(page: &PageRef<'_>) -> bool {
    page_opaque(page).flags & SPGIST_META != 0
}

#[inline]
pub fn SpGistPageIsDeleted(page: &PageRef<'_>) -> bool {
    page_opaque(page).flags & SPGIST_DELETED != 0
}

#[inline]
pub fn SpGistPageIsLeaf(page: &PageRef<'_>) -> bool {
    page_opaque(page).flags & SPGIST_LEAF != 0
}

#[inline]
pub fn SpGistPageStoresNulls(page: &PageRef<'_>) -> bool {
    page_opaque(page).flags & SPGIST_NULLS != 0
}

/// SpGistPageGetFreeSpace(p, n).
#[inline]
pub fn SpGistPageGetFreeSpace(page: &PageRef<'_>, n: usize) -> usize {
    page.exact_free_space()
        + (page_opaque(page).nPlaceholder as usize).min(n) * (SGDTSIZE + SIZEOF_ITEM_ID_DATA)
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct SpGistLastUsedPage {
    pub blkno: BlockNumber,
    pub freeSpace: i32,
}

impl Default for SpGistLastUsedPage {
    fn default() -> Self {
        SpGistLastUsedPage {
            blkno: ::types_core::InvalidBlockNumber,
            freeSpace: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct SpGistLUPCache {
    pub cachedPage: [SpGistLastUsedPage; SPGIST_CACHED_PAGES],
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct SpGistMetaPageData {
    pub magicNumber: u32,
    pub lastUsedPages: SpGistLUPCache,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SpGistTypeDesc {
    pub type_: ::types_core::Oid,
    pub attlen: i16,
    pub attbyval: bool,
    pub attalign: i8,
    pub attstorage: i8,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct spgConfigIn {
    pub attType: ::types_core::Oid,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct spgConfigOut {
    pub prefixType: ::types_core::Oid,
    pub labelType: ::types_core::Oid,
    pub leafType: ::types_core::Oid,
    pub canReturnData: bool,
    pub longValuesOK: bool,
}

// rd_amcache payload (C SpGistCache); POD, held in a Cell on the relcache entry.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpGistCache {
    pub config: spgConfigOut,
    pub attType: SpGistTypeDesc,
    pub attLeafType: SpGistTypeDesc,
    pub attPrefixType: SpGistTypeDesc,
    pub attLabelType: SpGistTypeDesc,
    pub lastUsedPages: SpGistLUPCache,
}

const _: () = assert!(!core::mem::needs_drop::<SpGistCache>());

// ---------------------------------------------------------------------------
// On-disk tuple codecs. Bitfield layouts are fixed by the C ABI (LSB-first).
// ---------------------------------------------------------------------------

/// Inner tuple header: u32 word (tupstate:2 | allTheSame:1 | nNodes:13 |
/// prefixSize:16), u16 size, 2 pad bytes; SGITHDRSZ == 8.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpGistInnerTupleHeader {
    pub tupstate: u8,
    pub allTheSame: bool,
    pub nNodes: u16,
    pub prefixSize: u16,
    pub size: u16,
}

impl SpGistInnerTupleHeader {
    #[inline]
    pub fn decode(b: &[u8]) -> Self {
        let w = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
        SpGistInnerTupleHeader {
            tupstate: (w & 0x3) as u8,
            allTheSame: (w >> 2) & 0x1 != 0,
            nNodes: ((w >> 3) & 0x1FFF) as u16,
            prefixSize: (w >> 16) as u16,
            size: u16::from_ne_bytes([b[4], b[5]]),
        }
    }

    #[inline]
    pub fn encode(&self, b: &mut [u8]) {
        let w = (self.tupstate as u32 & 0x3)
            | ((self.allTheSame as u32) << 2)
            | (((self.nNodes as u32) & 0x1FFF) << 3)
            | ((self.prefixSize as u32) << 16);
        b[0..4].copy_from_slice(&w.to_ne_bytes());
        b[4..6].copy_from_slice(&self.size.to_ne_bytes());
    }
}

/// Leaf tuple header: u32 word (tupstate:2 | size:30), u16 t_info
/// (nextOffset:14 | free:1 | hasNullMask:1), 6-byte heapPtr; 12 bytes.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpGistLeafTupleHeader {
    pub tupstate: u8,
    pub size: u32,
    pub t_info: u16,
    pub heapPtr: ItemPointerData,
}

impl SpGistLeafTupleHeader {
    #[inline]
    pub fn decode(b: &[u8]) -> Self {
        let w = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
        SpGistLeafTupleHeader {
            tupstate: (w & 0x3) as u8,
            size: w >> 2,
            t_info: u16::from_ne_bytes([b[4], b[5]]),
            heapPtr: read_item_pointer(&b[6..12]),
        }
    }

    #[inline]
    pub fn encode(&self, b: &mut [u8]) {
        let w = (self.tupstate as u32 & 0x3) | (self.size << 2);
        b[0..4].copy_from_slice(&w.to_ne_bytes());
        b[4..6].copy_from_slice(&self.t_info.to_ne_bytes());
        write_item_pointer(&mut b[6..12], &self.heapPtr);
    }

    #[inline]
    pub fn nextOffset(&self) -> OffsetNumber {
        self.t_info & 0x3FFF
    }

    #[inline]
    pub fn hasNullMask(&self) -> bool {
        self.t_info & 0x8000 != 0
    }

    #[inline]
    pub fn set_nextOffset(&mut self, off: OffsetNumber) {
        self.t_info = (self.t_info & 0xC000) | (off & 0x3FFF);
    }

    #[inline]
    pub fn set_hasNullMask(&mut self, hasnulls: bool) {
        self.t_info = (self.t_info & 0x7FFF) | if hasnulls { 0x8000 } else { 0 };
    }
}

/// SGLT_GET_NEXTOFFSET over a raw leaf-tuple image.
#[inline]
pub fn leaf_next_offset(b: &[u8]) -> OffsetNumber {
    u16::from_ne_bytes([b[4], b[5]]) & 0x3FFF
}

/// SGLT_SET_NEXTOFFSET over a raw leaf-tuple image.
#[inline]
pub fn leaf_set_next_offset(b: &mut [u8], off: OffsetNumber) {
    let t = (u16::from_ne_bytes([b[4], b[5]]) & 0xC000) | (off & 0x3FFF);
    b[4..6].copy_from_slice(&t.to_ne_bytes());
}

/// Leaf-tuple size field over a raw image.
#[inline]
pub fn leaf_size(b: &[u8]) -> usize {
    (u32::from_ne_bytes([b[0], b[1], b[2], b[3]]) >> 2) as usize
}

/// Dead tuple: u32 word (tupstate:2 | size:30), u16 t_info, 6-byte pointer,
/// u32 xid; 16 bytes total (== SGDTSIZE).
#[derive(Clone, Copy, Debug, Default)]
pub struct SpGistDeadTupleHeader {
    pub tupstate: u8,
    pub size: u32,
    pub t_info: u16,
    pub pointer: ItemPointerData,
    pub xid: TransactionId,
}

impl SpGistDeadTupleHeader {
    #[inline]
    pub fn decode(b: &[u8]) -> Self {
        let w = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
        SpGistDeadTupleHeader {
            tupstate: (w & 0x3) as u8,
            size: w >> 2,
            t_info: u16::from_ne_bytes([b[4], b[5]]),
            pointer: read_item_pointer(&b[6..12]),
            xid: TransactionId::from_ne_bytes([b[12], b[13], b[14], b[15]]),
        }
    }

    #[inline]
    pub fn encode(&self, b: &mut [u8]) {
        let w = (self.tupstate as u32 & 0x3) | (self.size << 2);
        b[0..4].copy_from_slice(&w.to_ne_bytes());
        b[4..6].copy_from_slice(&self.t_info.to_ne_bytes());
        write_item_pointer(&mut b[6..12], &self.pointer);
        b[12..16].copy_from_slice(&self.xid.to_ne_bytes());
    }
}

/// tupstate of any SP-GiST tuple image (low 2 bits of the first word).
#[inline]
pub fn tuple_state(b: &[u8]) -> u8 {
    b[0] & 0x3
}

/// Node tuple (IndexTupleData): 6-byte t_tid, u16 t_info.
#[inline]
pub fn node_tuple_size(node: &[u8]) -> usize {
    (u16::from_ne_bytes([node[6], node[7]]) & INDEX_SIZE_MASK) as usize
}

#[inline]
pub fn node_tuple_has_nulls(node: &[u8]) -> bool {
    u16::from_ne_bytes([node[6], node[7]]) & INDEX_NULL_MASK != 0
}

#[inline]
pub fn node_tuple_tid(node: &[u8]) -> ItemPointerData {
    read_item_pointer(&node[0..6])
}

#[inline]
pub fn node_tuple_set_tid(node: &mut [u8], tid: &ItemPointerData) {
    write_item_pointer(&mut node[0..6], tid);
}

/// Iterate the node tuples of an inner-tuple image (SGITITERATE): yields
/// (node index, byte offset of the node within the image).
pub fn inner_tuple_nodes(inner: &[u8]) -> InnerNodeIter<'_> {
    let hdr = SpGistInnerTupleHeader::decode(inner);
    InnerNodeIter {
        inner,
        off: SGITHDRSZ + hdr.prefixSize as usize,
        i: 0,
        n: hdr.nNodes as usize,
    }
}

pub struct InnerNodeIter<'a> {
    inner: &'a [u8],
    off: usize,
    i: usize,
    n: usize,
}

impl Iterator for InnerNodeIter<'_> {
    type Item = (usize, usize);

    #[inline]
    fn next(&mut self) -> Option<(usize, usize)> {
        if self.i >= self.n {
            return None;
        }
        let r = (self.i, self.off);
        self.off += node_tuple_size(&self.inner[self.off..]);
        self.i += 1;
        Some(r)
    }
}

/// ItemPointerData disk codec ({bi_hi, bi_lo, ip_posid} u16 triple).
#[inline]
pub fn read_item_pointer(b: &[u8]) -> ItemPointerData {
    debug_assert!(b.len() >= 6);
    // SAFETY: 6 bytes checked; ItemPointerData is a repr(C) 6-byte POD.
    unsafe { b.as_ptr().cast::<ItemPointerData>().read_unaligned() }
}

#[inline]
pub fn write_item_pointer(b: &mut [u8], ip: &ItemPointerData) {
    debug_assert!(b.len() >= 6);
    // SAFETY: 6 bytes checked; unaligned write of a 6-byte POD.
    unsafe { b.as_mut_ptr().cast::<ItemPointerData>().write_unaligned(*ip) }
}

// ---------------------------------------------------------------------------
// Redo-shared page/tuple operations (spgxlog.c uses these without a Relation).
// ---------------------------------------------------------------------------

/// SpGistInitPage.
pub fn SpGistInitPage(pm: &mut PageMut<'_>, f: u16) {
    pm.init(SIZEOF_SPGIST_PAGE_OPAQUE_DATA);
    page_opaque_set(
        pm,
        SpGistPageOpaqueData {
            flags: f,
            nRedirection: 0,
            nPlaceholder: 0,
            spgist_page_id: SPGIST_PAGE_ID,
        },
    );
}

/// spgFormDeadTuple (pure image builder).
pub fn spgFormDeadTuple(
    redirect_xid: TransactionId,
    tupstate: u8,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) -> [u8; SGDTSIZE] {
    let mut hdr = SpGistDeadTupleHeader {
        tupstate,
        size: SGDTSIZE as u32,
        t_info: 0,
        pointer: ItemPointerData::invalid(),
        xid: 0,
    };
    if tupstate == SPGIST_REDIRECT {
        hdr.pointer = ItemPointerData::new(blkno, offnum);
        hdr.xid = redirect_xid;
    }
    let mut storage = [0u8; SGDTSIZE];
    hdr.encode(&mut storage);
    storage
}

/// spgUpdateNodeLink over a mutable inner-tuple image.
pub fn spgUpdateNodeLink(inner: &mut [u8], nodeN: i32, blkno: BlockNumber, offset: OffsetNumber) {
    let hdr = SpGistInnerTupleHeader::decode(inner);
    let mut off = SGITHDRSZ + hdr.prefixSize as usize;
    for i in 0..hdr.nNodes as i32 {
        if i == nodeN {
            let tid = ItemPointerData::new(blkno, offset);
            node_tuple_set_tid(&mut inner[off..], &tid);
            return;
        }
        off += node_tuple_size(&inner[off..]);
    }
    panic!("failed to find requested node {nodeN} in SPGiST inner tuple");
}

/// spgPageIndexMultiDelete: replace `itemnos` with dead tuples, preserving
/// offsets. Shared by the insert path and WAL redo.
pub fn spgPageIndexMultiDelete(
    redirect_xid: TransactionId,
    pm: &mut PageMut<'_>,
    itemnos: &[OffsetNumber],
    firststate: u8,
    reststate: u8,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) {
    let nitems = itemnos.len();
    if nitems == 0 {
        return;
    }
    debug_assert!(nitems <= ::types_storage::bufpage::MaxIndexTuplesPerPage);

    let mut sortednos = [0 as OffsetNumber; ::types_storage::bufpage::MaxIndexTuplesPerPage];
    sortednos[..nitems].copy_from_slice(itemnos);
    sortednos[..nitems].sort_unstable();

    pm.index_multi_delete(&sortednos[..nitems]);

    let first_item = itemnos[0];
    let mut tuple: Option<([u8; SGDTSIZE], u8)> = None;

    for &itemno in &sortednos[..nitems] {
        let tupstate = if itemno == first_item { firststate } else { reststate };
        let img = match tuple {
            Some((img, st)) if st == tupstate => img,
            _ => {
                let img = spgFormDeadTuple(redirect_xid, tupstate, blkno, offnum);
                tuple = Some((img, tupstate));
                img
            }
        };
        if pm.add_item(&img, itemno, 0) != Some(itemno) {
            panic!("failed to add item of size {SGDTSIZE} to SPGiST index page");
        }
        if tupstate == SPGIST_REDIRECT {
            page_opaque_update(pm, |op| op.nRedirection += 1);
        } else if tupstate == SPGIST_PLACEHOLDER {
            page_opaque_update(pm, |op| op.nPlaceholder += 1);
        }
    }
}
