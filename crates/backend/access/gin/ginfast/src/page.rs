//! GIN page-byte accessors for the pending-list pages and the metapage, mirror
//! the C `GinPageGetOpaque` / `GinPageGetMeta` macros (ginblock.h). The byte
//! offsets match `ginutil`'s on-disk image (verified against the C struct
//! layouts there).

use page::{PageGetContents, PageGetSpecialPointer, PageRef};
use utils_error::PgResult;
use types_core::primitive::BlockNumber;
use types_core::primitive::OffsetNumber;
use gin::GinMetaPageData;

/// `GinPageOpaqueData` field layout in the page special area:
/// `rightlink: BlockNumber (u32) | maxoff: OffsetNumber (u16) | flags: u16`.
#[derive(Clone, Copy, Debug, Default)]
pub struct GinOpaque {
    pub rightlink: BlockNumber,
    pub maxoff: OffsetNumber,
    pub flags: u16,
}

/// `GinPageGetOpaque(page)` — read the opaque header out of a page byte image.
pub fn gin_opaque_from_page(page: &[u8]) -> PgResult<GinOpaque> {
    let pr = PageRef::new(page)?;
    let special = PageGetSpecialPointer(&pr)?;
    let rightlink = u32::from_ne_bytes([special[0], special[1], special[2], special[3]]);
    let maxoff = u16::from_ne_bytes([special[4], special[5]]);
    let flags = u16::from_ne_bytes([special[6], special[7]]);
    Ok(GinOpaque {
        rightlink,
        maxoff,
        flags,
    })
}

/// Byte offset of the page special area (`pd_special`, a u16 at offset 16 of the
/// `PageHeaderData`).
fn special_offset(page: &[u8]) -> usize {
    u16::from_ne_bytes([page[16], page[17]]) as usize
}

/// `GinPageGetOpaque(page)->rightlink = blkno`.
pub fn set_rightlink(page: &mut [u8], blkno: BlockNumber) {
    let s = special_offset(page);
    page[s..s + 4].copy_from_slice(&blkno.to_ne_bytes());
}

/// `GinPageGetOpaque(page)->maxoff = n`.
pub fn set_maxoff(page: &mut [u8], n: OffsetNumber) {
    let s = special_offset(page);
    page[s + 4..s + 6].copy_from_slice(&n.to_ne_bytes());
}

/// `GinPageGetOpaque(page)->flags = f` (plain assignment — used by `shiftList`
/// which sets `flags = GIN_DELETED`, clearing all other bits).
pub fn set_flags(page: &mut [u8], f: u16) {
    let s = special_offset(page);
    page[s + 6..s + 8].copy_from_slice(&f.to_ne_bytes());
}

/// `GinPageGetOpaque(page)->flags |= f`.
pub fn or_flags(page: &mut [u8], f: u16) {
    let s = special_offset(page);
    let cur = u16::from_ne_bytes([page[s + 6], page[s + 7]]);
    page[s + 6..s + 8].copy_from_slice(&(cur | f).to_ne_bytes());
}

// ---------------------------------------------------------------------------
// Metapage (`GinPageGetMeta` == PageGetContents).
// ---------------------------------------------------------------------------

/// `MAXALIGN(SizeOfPageHeaderData)` — `PageGetContents` offset (= 24).
fn meta_offset() -> usize {
    24
}

const OFF_GIN_HEAD: usize = 0;
const OFF_GIN_TAIL: usize = 4;
const OFF_GIN_TAILFREESIZE: usize = 8;
const OFF_GIN_NPENDINGPAGES: usize = 12;
const OFF_GIN_NPENDINGHEAPTUPLES: usize = 16;
const OFF_GIN_NTOTALPAGES: usize = 24;
const OFF_GIN_NENTRYPAGES: usize = 28;
const OFF_GIN_NDATAPAGES: usize = 32;
const OFF_GIN_NENTRIES: usize = 40;
const OFF_GIN_VERSION: usize = 48;
/// `sizeof(GinMetaPageData)` on disk (MAXALIGN(offsetof(ginVersion)+4) = 56).
pub const SIZE_OF_GIN_META_PAGE_DATA: usize = 56;

/// `GinPageGetMeta(page)` — read the metadata struct from a page byte image.
pub fn read_meta(page: &[u8]) -> PgResult<GinMetaPageData> {
    let pr = PageRef::new(page)?;
    let c = PageGetContents(&pr)?;
    let g32 = |o: usize| u32::from_ne_bytes([c[o], c[o + 1], c[o + 2], c[o + 3]]);
    let g64 = |o: usize| {
        i64::from_ne_bytes([
            c[o], c[o + 1], c[o + 2], c[o + 3], c[o + 4], c[o + 5], c[o + 6], c[o + 7],
        ])
    };
    Ok(GinMetaPageData {
        head: g32(OFF_GIN_HEAD),
        tail: g32(OFF_GIN_TAIL),
        tailFreeSize: g32(OFF_GIN_TAILFREESIZE),
        nPendingPages: g32(OFF_GIN_NPENDINGPAGES),
        nPendingHeapTuples: g64(OFF_GIN_NPENDINGHEAPTUPLES),
        nTotalPages: g32(OFF_GIN_NTOTALPAGES),
        nEntryPages: g32(OFF_GIN_NENTRYPAGES),
        nDataPages: g32(OFF_GIN_NDATAPAGES),
        nEntries: g64(OFF_GIN_NENTRIES),
        ginVersion: i32::from_ne_bytes([
            c[OFF_GIN_VERSION],
            c[OFF_GIN_VERSION + 1],
            c[OFF_GIN_VERSION + 2],
            c[OFF_GIN_VERSION + 3],
        ]),
    })
}

/// Write the metadata struct into a page byte image and set `pd_lower` past the
/// metadata (so xlog page compression won't lose it), mirroring the
/// `((PageHeader) metapage)->pd_lower = ...` idiom GIN uses after every
/// metapage update.
pub fn write_meta(page: &mut [u8], meta: &GinMetaPageData) {
    let off = meta_offset();
    let put32 = |page: &mut [u8], fo: usize, v: u32| {
        let p = off + fo;
        page[p..p + 4].copy_from_slice(&v.to_ne_bytes());
    };
    let put64 = |page: &mut [u8], fo: usize, v: i64| {
        let p = off + fo;
        page[p..p + 8].copy_from_slice(&v.to_ne_bytes());
    };
    put32(page, OFF_GIN_HEAD, meta.head);
    put32(page, OFF_GIN_TAIL, meta.tail);
    put32(page, OFF_GIN_TAILFREESIZE, meta.tailFreeSize);
    put32(page, OFF_GIN_NPENDINGPAGES, meta.nPendingPages);
    put64(page, OFF_GIN_NPENDINGHEAPTUPLES, meta.nPendingHeapTuples);
    put32(page, OFF_GIN_NTOTALPAGES, meta.nTotalPages);
    put32(page, OFF_GIN_NENTRYPAGES, meta.nEntryPages);
    put32(page, OFF_GIN_NDATAPAGES, meta.nDataPages);
    put64(page, OFF_GIN_NENTRIES, meta.nEntries);
    {
        let p = off + OFF_GIN_VERSION;
        page[p..p + 4].copy_from_slice(&meta.ginVersion.to_ne_bytes());
    }
    // pd_lower = (metadata + sizeof(GinMetaPageData)) - page.
    let pd_lower = (off + SIZE_OF_GIN_META_PAGE_DATA) as u16;
    page[12..14].copy_from_slice(&pd_lower.to_ne_bytes());
}

/// Serialize a [`GinMetaPageData`] to its on-disk byte image (for the WAL
/// records that copy `metadata` into the record body).
pub fn meta_to_bytes(meta: &GinMetaPageData) -> [u8; SIZE_OF_GIN_META_PAGE_DATA] {
    let mut buf = [0u8; SIZE_OF_GIN_META_PAGE_DATA];
    buf[OFF_GIN_HEAD..OFF_GIN_HEAD + 4].copy_from_slice(&meta.head.to_ne_bytes());
    buf[OFF_GIN_TAIL..OFF_GIN_TAIL + 4].copy_from_slice(&meta.tail.to_ne_bytes());
    buf[OFF_GIN_TAILFREESIZE..OFF_GIN_TAILFREESIZE + 4]
        .copy_from_slice(&meta.tailFreeSize.to_ne_bytes());
    buf[OFF_GIN_NPENDINGPAGES..OFF_GIN_NPENDINGPAGES + 4]
        .copy_from_slice(&meta.nPendingPages.to_ne_bytes());
    buf[OFF_GIN_NPENDINGHEAPTUPLES..OFF_GIN_NPENDINGHEAPTUPLES + 8]
        .copy_from_slice(&meta.nPendingHeapTuples.to_ne_bytes());
    buf[OFF_GIN_NTOTALPAGES..OFF_GIN_NTOTALPAGES + 4]
        .copy_from_slice(&meta.nTotalPages.to_ne_bytes());
    buf[OFF_GIN_NENTRYPAGES..OFF_GIN_NENTRYPAGES + 4]
        .copy_from_slice(&meta.nEntryPages.to_ne_bytes());
    buf[OFF_GIN_NDATAPAGES..OFF_GIN_NDATAPAGES + 4]
        .copy_from_slice(&meta.nDataPages.to_ne_bytes());
    buf[OFF_GIN_NENTRIES..OFF_GIN_NENTRIES + 8].copy_from_slice(&meta.nEntries.to_ne_bytes());
    buf[OFF_GIN_VERSION..OFF_GIN_VERSION + 4].copy_from_slice(&meta.ginVersion.to_ne_bytes());
    buf
}

/// `IndexTupleSize(itup)` over a raw byte image: `t_info & INDEX_SIZE_MASK`
/// (`t_info` is the u16 at bytes 6..8 of the `IndexTupleData` header).
pub fn index_tuple_size(tuple: &[u8]) -> usize {
    const INDEX_SIZE_MASK: u16 = 0x1FFF;
    let t_info = u16::from_ne_bytes([tuple[6], tuple[7]]);
    (t_info & INDEX_SIZE_MASK) as usize
}
