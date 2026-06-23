//! BRIN on-disk page layout (`access/brin_page.h`) and the page-init
//! primitives (`brin_page_init` / `brin_metapage_init`, `brin_pageops.c`),
//! grounded against the `BLCKSZ` page bytes 1:1 with the C macros.
//!
//! These are transcribed identically to the inlined copies in
//! `backend-access-brin-xlog`; both move to this crate's ownership once the
//! redo crate is re-pointed.

use page::{PageInit, PageMut};
use utils_error::PgResult;
use types_core::primitive::{BlockNumber, BLCKSZ};
use types_storage::bufpage::SizeOfPageHeaderData;

// ===========================================================================
// brin_page.h constants.
// ===========================================================================

/// `BRIN_PAGETYPE_META` (brin_page.h).
pub const PAGETYPE_META: u16 = 0xF091;
/// `BRIN_PAGETYPE_REVMAP` (brin_page.h).
pub const PAGETYPE_REVMAP: u16 = 0xF092;
/// `BRIN_PAGETYPE_REGULAR` (brin_page.h).
pub const PAGETYPE_REGULAR: u16 = 0xF093;

/// `BRIN_EVACUATE_PAGE` (brin_page.h) — page-flags bit (not WAL-logged).
pub const BRIN_EVACUATE_PAGE: u16 = 1 << 0;

/// `BRIN_META_MAGIC` (brin_page.h).
pub const BRIN_META_MAGIC: u32 = 0xA8109CFA;

/// `BRIN_METAPAGE_BLKNO` (brin_page.h): the metapage is always block 0.
pub const BRIN_METAPAGE_BLKNO: BlockNumber = 0;

// ===========================================================================
// MAXALIGN / page-content offsets.
// ===========================================================================

/// `MAXALIGN(x)` (c.h): round up to `MAXIMUM_ALIGNOF` (8).
pub const fn maxalign(x: usize) -> usize {
    (x + 7) & !7
}

/// `PageGetContents(page)` offset (bufpage.h): the area after the MAXALIGN'd
/// page header. `BrinMetaPageData` / `RevmapContents.rm_tids` start here.
pub const CONTENTS_OFFSET: usize = maxalign(SizeOfPageHeaderData as usize);

/// `sizeof(BrinSpecialSpace)` (brin_page.h) = `MAXALIGN(1)` = 8 bytes; it
/// always occupies the last MAXALIGN-sized element of the page.
pub const SIZEOF_BRIN_SPECIAL_SPACE: usize = maxalign(1);

/// `sizeof(ItemPointerData)` on the on-disk ABI: 6 bytes.
pub const SIZEOF_ITEM_POINTER_DATA: usize = 6;

/// Byte offset of `pd_lower` within `PageHeaderData` (the uint16 at offset 12).
const OFF_PD_LOWER: usize = 12;

/// `REVMAP_CONTENT_SIZE` (brin_page.h): bytes available for the revmap's
/// `rm_tids` array (offsetof(RevmapContents, rm_tids) == 0).
pub const REVMAP_CONTENT_SIZE: usize =
    BLCKSZ - maxalign(SizeOfPageHeaderData as usize) - 0 - maxalign(SIZEOF_BRIN_SPECIAL_SPACE);

/// `REVMAP_PAGE_MAXITEMS` (brin_page.h): max revmap entries per page.
pub const REVMAP_PAGE_MAXITEMS: usize = REVMAP_CONTENT_SIZE / SIZEOF_ITEM_POINTER_DATA;

/// `BrinMaxItemSize` (brin_pageops.c): the largest item allowed on a regular
/// page. BRIN tolerates a single item per page, so this is the whole page
/// minus header, one line pointer, and the special space.
pub const BRIN_MAX_ITEM_SIZE: usize = {
    // MAXALIGN_DOWN(x) = x & ~7
    let inner = maxalign(SizeOfPageHeaderData as usize + 4 /* sizeof(ItemIdData) */)
        + maxalign(SIZEOF_BRIN_SPECIAL_SPACE);
    (BLCKSZ - inner) & !7
};

/// `sizeof(BrinMetaPageData)` = 16 bytes (four 4-byte fields, no padding).
pub const SIZEOF_BRIN_META_PAGE_DATA: usize = 16;

const META_OFF_MAGIC: usize = CONTENTS_OFFSET; // brinMagic
const META_OFF_VERSION: usize = CONTENTS_OFFSET + 4; // brinVersion
const META_OFF_PAGES_PER_RANGE: usize = CONTENTS_OFFSET + 8; // pagesPerRange
const META_OFF_LAST_REVMAP_PAGE: usize = CONTENTS_OFFSET + 12; // lastRevmapPage

// ===========================================================================
// Page-type / page-flags special-area accessors (brin_page.h).
//
//   struct BrinSpecialSpace { uint16 vector[MAXALIGN(1) / sizeof(uint16)]; };
//   #define BrinPageType(page)  (vector[idx - 1])
//   #define BrinPageFlags(page) (vector[idx - 2])
// — i.e. the page type is the last half-word, the flags the second-to-last.
// ===========================================================================

/// `BrinPageType(page)` (brin_page.h).
pub fn brin_page_type(page: &[u8]) -> u16 {
    let off = BLCKSZ - 2;
    u16::from_ne_bytes([page[off], page[off + 1]])
}

/// `BrinPageType(page) = type`.
pub fn set_brin_page_type(page: &mut [u8], ty: u16) {
    let off = BLCKSZ - 2;
    page[off..off + 2].copy_from_slice(&ty.to_ne_bytes());
}

/// `BrinPageFlags(page)` (brin_page.h).
pub fn brin_page_flags(page: &[u8]) -> u16 {
    let off = BLCKSZ - 4;
    u16::from_ne_bytes([page[off], page[off + 1]])
}

/// `BrinPageFlags(page) |= flags`.
pub fn or_brin_page_flags(page: &mut [u8], flags: u16) {
    let cur = brin_page_flags(page);
    let off = BLCKSZ - 4;
    page[off..off + 2].copy_from_slice(&(cur | flags).to_ne_bytes());
}

/// `BRIN_IS_META_PAGE(page)` (brin_page.h).
pub fn brin_is_meta_page(page: &[u8]) -> bool {
    brin_page_type(page) == PAGETYPE_META
}

/// `BRIN_IS_REVMAP_PAGE(page)` (brin_page.h).
pub fn brin_is_revmap_page(page: &[u8]) -> bool {
    brin_page_type(page) == PAGETYPE_REVMAP
}

/// `BRIN_IS_REGULAR_PAGE(page)` (brin_page.h).
pub fn brin_is_regular_page(page: &[u8]) -> bool {
    brin_page_type(page) == PAGETYPE_REGULAR
}

// ===========================================================================
// pd_lower / metapage field accessors.
// ===========================================================================

/// `((PageHeader) page)->pd_lower = value`.
fn set_pd_lower(page: &mut [u8], value: u16) {
    page[OFF_PD_LOWER..OFF_PD_LOWER + 2].copy_from_slice(&value.to_ne_bytes());
}

/// `metadata->pagesPerRange`.
pub fn meta_pages_per_range(page: &[u8]) -> BlockNumber {
    BlockNumber::from_ne_bytes([
        page[META_OFF_PAGES_PER_RANGE],
        page[META_OFF_PAGES_PER_RANGE + 1],
        page[META_OFF_PAGES_PER_RANGE + 2],
        page[META_OFF_PAGES_PER_RANGE + 3],
    ])
}

/// `metadata->lastRevmapPage`.
pub fn meta_last_revmap_page(page: &[u8]) -> BlockNumber {
    BlockNumber::from_ne_bytes([
        page[META_OFF_LAST_REVMAP_PAGE],
        page[META_OFF_LAST_REVMAP_PAGE + 1],
        page[META_OFF_LAST_REVMAP_PAGE + 2],
        page[META_OFF_LAST_REVMAP_PAGE + 3],
    ])
}

/// `metadata->lastRevmapPage = blk`.
pub fn set_meta_last_revmap_page(page: &mut [u8], blk: BlockNumber) {
    page[META_OFF_LAST_REVMAP_PAGE..META_OFF_LAST_REVMAP_PAGE + 4]
        .copy_from_slice(&blk.to_ne_bytes());
}

/// `((PageHeader) metapg)->pd_lower = ((char *) metadata +
/// sizeof(BrinMetaPageData)) - (char *) metapg`: set pd_lower just past the
/// metadata.
pub fn set_meta_pd_lower(page: &mut [u8]) {
    set_pd_lower(page, (CONTENTS_OFFSET + SIZEOF_BRIN_META_PAGE_DATA) as u16);
}

// ===========================================================================
// brin_page_init / brin_metapage_init (brin_pageops.c).
// ===========================================================================

/// `brin_page_init(page, type)` (brin_pageops.c:474): `PageInit` the page and
/// stamp its special-area page type. Caller is responsible for marking dirty.
pub fn brin_page_init(page: &mut [u8], page_type: u16) -> PgResult<()> {
    PageInit(page, BLCKSZ, SIZEOF_BRIN_SPECIAL_SPACE)?;
    set_brin_page_type(page, page_type);
    Ok(())
}

/// `brin_metapage_init(page, pagesPerRange, version)` (brin_pageops.c:485):
/// initialize a new BRIN index's metapage.
pub fn brin_metapage_init(
    page: &mut [u8],
    pages_per_range: BlockNumber,
    version: u16,
) -> PgResult<()> {
    brin_page_init(page, PAGETYPE_META)?;

    // metadata->brinMagic = BRIN_META_MAGIC;
    page[META_OFF_MAGIC..META_OFF_MAGIC + 4].copy_from_slice(&BRIN_META_MAGIC.to_ne_bytes());
    // metadata->brinVersion = version;
    page[META_OFF_VERSION..META_OFF_VERSION + 4].copy_from_slice(&(version as u32).to_ne_bytes());
    // metadata->pagesPerRange = pagesPerRange;
    page[META_OFF_PAGES_PER_RANGE..META_OFF_PAGES_PER_RANGE + 4]
        .copy_from_slice(&pages_per_range.to_ne_bytes());

    // Note we cheat here: 0 is not a valid revmap block (it's the metapage),
    // but doing this lets the first revmap page be created with the index.
    set_meta_last_revmap_page(page, 0);

    // Set pd_lower just past the metadata — essential, else metadata is lost
    // if xlog.c compresses the page.
    set_meta_pd_lower(page);
    Ok(())
}

// ===========================================================================
// PageSetLSN against page bytes (used by both files' XLOG legs).
// ===========================================================================

/// `PageSetLSN(page, recptr)` against the page bytes.
pub fn page_set_lsn(page: &mut [u8], recptr: types_core::XLogRecPtr) -> PgResult<()> {
    let mut pmut = PageMut::new(page)?;
    ::page::PageSetLSN(&mut pmut, recptr);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_init_sets_type_and_flags() {
        let mut page = alloc::vec![0u8; BLCKSZ];
        brin_page_init(&mut page, PAGETYPE_REGULAR).unwrap();
        assert_eq!(brin_page_type(&page), PAGETYPE_REGULAR);
        assert!(brin_is_regular_page(&page));
        assert!(!brin_is_meta_page(&page));
        assert!(!brin_is_revmap_page(&page));
        assert_eq!(brin_page_flags(&page), 0);
        or_brin_page_flags(&mut page, BRIN_EVACUATE_PAGE);
        assert_eq!(brin_page_flags(&page) & BRIN_EVACUATE_PAGE, BRIN_EVACUATE_PAGE);
        // Setting the flag must not corrupt the adjacent page-type half-word.
        assert_eq!(brin_page_type(&page), PAGETYPE_REGULAR);
    }

    #[test]
    fn metapage_init_writes_metadata() {
        let mut page = alloc::vec![0u8; BLCKSZ];
        brin_metapage_init(&mut page, 128, 2).unwrap();
        assert_eq!(brin_page_type(&page), PAGETYPE_META);
        assert!(brin_is_meta_page(&page));
        assert_eq!(meta_pages_per_range(&page), 128);
        assert_eq!(meta_last_revmap_page(&page), 0);
    }
}
