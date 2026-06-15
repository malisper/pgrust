//! GiST on-disk page layout (`access/gist.h`) and the page-byte primitives from
//! `access/gist/gistutil.c` (`gistinitpage` / `GISTInitBuffer` / `gistcheckpage`
//! / `gistfillbuffer`) plus the NSN special-area accessors (`GistPageGetNSN` /
//! `GistPageSetNSN`), grounded against the `BLCKSZ` page bytes 1:1 with the C
//! macros — exactly the model used by `backend-access-brin-pageops`.
//!
//! The GiST special area is `GISTPageOpaqueData` (gist.h):
//!
//! ```c
//! typedef struct GISTPageOpaqueData
//! {
//!     PageXLogRecPtr nsn;          /* 8 bytes */
//!     BlockNumber    rightlink;    /* 4 bytes */
//!     uint16         flags;        /* 2 bytes */
//!     uint16         gist_page_id; /* 2 bytes */
//! } GISTPageOpaqueData;            /* 16 bytes total */
//! ```
//!
//! `PageGistNSN`/`PageXLogRecPtr` is the split `{ uint32 xlogid; uint32 xrecoff; }`
//! on-disk form of an `XLogRecPtr`, handled here exactly like `PageXLogRecPtrGet`
//! / `PageXLogRecPtrSet` in bufpage.h.

use backend_storage_page::{
    PageAddItemExtended, PageGetContents, PageGetMaxOffsetNumber, PageGetSpecialPointer,
    PageGetSpecialSize, PageInit, PageIsEmpty, PageIsNew, PageMut, PageRef,
};
use types_storage::bufpage::SizeOfPageHeaderData;
use alloc::format;
use alloc::vec::Vec;
use backend_storage_buffer_bufmgr_seams::{buffer_get_block_number, with_buffer_page};
use backend_utils_error::{ereport, PgResult};
use types_error::error::ERROR;
use types_core::primitive::{BlockNumber, OffsetNumber, Size, XLogRecPtr, BLCKSZ};
use types_core::xact::{FirstNormalTransactionId, FullTransactionId};
use types_error::error::ERRCODE_INDEX_CORRUPTED;
use types_gist::{GistNSN, GIST_PAGE_ID};
use types_storage::Buffer;
use types_tuple::heaptuple::{FIRST_OFFSET_NUMBER, INVALID_OFFSET_NUMBER};

/// `MAXALIGN(x)` (c.h): round up to `MAXIMUM_ALIGNOF` (8).
pub const fn maxalign(x: usize) -> usize {
    (x + 7) & !7
}

/// `sizeof(GISTPageOpaqueData)` (gist.h) — 16 bytes, see module docs.
pub const SIZEOF_GIST_PAGE_OPAQUE_DATA: usize = 16;

/// `sizeof(FullTransactionId)` (access/transam.h) — a 64-bit value.
const SIZEOF_FULL_TRANSACTION_ID: usize = 8;

/// `GiSTPageSize` (gist_private.h:476): the usable bytes on a GiST page,
/// `BLCKSZ - SizeOfPageHeaderData - MAXALIGN(sizeof(GISTPageOpaqueData))`.
pub const GiSTPageSize: usize =
    BLCKSZ as usize - SizeOfPageHeaderData - maxalign(SIZEOF_GIST_PAGE_OPAQUE_DATA);

// Byte offsets of the special-area fields, relative to the start of the
// MAXALIGN'd special area (the area is exactly 16 bytes so no MAXALIGN slack).
const OPQ_OFF_NSN: usize = 0; // PageXLogRecPtr (xlogid, xrecoff)
const OPQ_OFF_RIGHTLINK: usize = 8; // BlockNumber
const OPQ_OFF_FLAGS: usize = 12; // uint16
const OPQ_OFF_GIST_PAGE_ID: usize = 14; // uint16

/// `GistPageGetOpaque(page)` start offset within the page bytes: the special
/// area begins at `pd_special`. We resolve it via `PageGetSpecialPointer`, which
/// returns the special-area slice; callers index into it with the `OPQ_OFF_*`
/// constants.

// ===========================================================================
// gistinitpage / GISTInitBuffer (gistutil.c).
// ===========================================================================

/// `gistinitpage(page, f)` (gistutil.c:757): `PageInit` the page with a
/// `GISTPageOpaqueData` special area and stamp `rightlink`/`flags`/page-id.
pub fn gistinitpage(page: &mut [u8], f: u16) -> PgResult<()> {
    // PageInit(page, BLCKSZ, sizeof(GISTPageOpaqueData));
    PageInit(page, BLCKSZ as Size, SIZEOF_GIST_PAGE_OPAQUE_DATA as Size)?;

    // opaque = GistPageGetOpaque(page);
    // opaque->rightlink = InvalidBlockNumber;
    set_gist_page_rightlink(page, BlockNumber::MAX)?;
    // opaque->flags = f;
    set_gist_page_flags(page, f)?;
    // opaque->gist_page_id = GIST_PAGE_ID;
    set_gist_page_id(page, GIST_PAGE_ID)?;
    Ok(())
}

/// `GISTInitBuffer(b, f)` (gistutil.c:772): initialize a freshly-extended index
/// buffer's page. The page bytes are reached through the bufmgr seam
/// (`with_buffer_page` — the owned-model stand-in for `BufferGetPage`); the
/// caller is responsible for the surrounding critical section + dirty mark.
pub fn GISTInitBuffer(b: Buffer, f: u16) -> PgResult<()> {
    with_buffer_page::call(b, &mut |page: &mut [u8]| gistinitpage(page, f))
}

// ===========================================================================
// gistcheckpage (gistutil.c).
// ===========================================================================

/// `gistcheckpage(rel, buf)` (gistutil.c:784): verify that a freshly-read page
/// looks sane (not all-zero, and a `GISTPageOpaqueData`-sized special area).
/// `rel_name` / the block number are supplied for the error messages (C reads
/// them from the `Relation` and `BufferGetBlockNumber`).
pub fn gistcheckpage(rel_name: &str, buf: Buffer) -> PgResult<()> {
    let blkno = buffer_get_block_number::call(buf);
    with_buffer_page::call(buf, &mut |bytes: &mut [u8]| {
        let page = PageRef::new(bytes)?;

        // if (PageIsNew(page)) ereport(ERROR, ... "unexpected zero page" ...)
        if PageIsNew(&page) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INDEX_CORRUPTED)
                .errmsg(format!(
                    "index \"{rel_name}\" contains unexpected zero page at block {blkno}"
                ))
                .errhint("Please REINDEX it.")
                .into_error());
        }

        // if (PageGetSpecialSize(page) != MAXALIGN(sizeof(GISTPageOpaqueData)))
        //     ereport(ERROR, ... "corrupted page" ...)
        if PageGetSpecialSize(&page) as usize != maxalign(SIZEOF_GIST_PAGE_OPAQUE_DATA) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INDEX_CORRUPTED)
                .errmsg(format!(
                    "index \"{rel_name}\" contains corrupted page at block {blkno}"
                ))
                .errhint("Please REINDEX it.")
                .into_error());
        }
        Ok(())
    })
}

// ===========================================================================
// gistfillbuffer (gistutil.c) — "gistpageaddtup".
// ===========================================================================

/// `gistfillbuffer(page, itup, len, off)` (gistutil.c:34): add the on-disk
/// index tuples `itups` to `page` starting at offset `off` (or appended when
/// `off == InvalidOffsetNumber`). Each `itups[i]` is the contiguous on-disk
/// index-tuple byte image (`index_form_tuple` output), so its size is just its
/// byte length (`IndexTupleSize`). `elog(ERROR)` if an item won't fit.
pub fn gistfillbuffer(page: &mut [u8], itups: &[Vec<u8>], off: OffsetNumber) -> PgResult<()> {
    let mut pmut = PageMut::new(page)?;

    // if (off == InvalidOffsetNumber)
    //     off = PageIsEmpty(page) ? FirstOffsetNumber
    //                             : OffsetNumberNext(PageGetMaxOffsetNumber(page));
    let mut off = if off == INVALID_OFFSET_NUMBER {
        if PageIsEmpty(&pmut.as_ref()) {
            FIRST_OFFSET_NUMBER
        } else {
            PageGetMaxOffsetNumber(&pmut.as_ref()) + 1
        }
    } else {
        off
    };

    for (i, itup) in itups.iter().enumerate() {
        // Size sz = IndexTupleSize(itup[i]);
        let sz = itup.len();
        // l = PageAddItem(page, (Item) itup[i], sz, off, false, false);
        // PAI flags: overwrite=false, is_heap=false -> 0.
        let l = PageAddItemExtended(&mut pmut, itup, off, 0)?;
        if l == INVALID_OFFSET_NUMBER {
            return Err(ereport(ERROR)
                .errmsg_internal(format!(
                    "failed to add item to GiST index page, item {i} out of {}, size {sz} bytes",
                    itups.len()
                ))
                .into_error());
        }
        off += 1;
    }
    Ok(())
}

// ===========================================================================
// GISTPageOpaqueData special-area accessors (gist.h).
// ===========================================================================

/// `GistPageGetNSN(page)` (gist.h:189): `PageXLogRecPtrGet(opaque->nsn)`.
pub fn gist_page_get_nsn(page: &[u8]) -> PgResult<GistNSN> {
    let pref = PageRef::new(page)?;
    let opaque = PageGetSpecialPointer(&pref)?;
    Ok(read_xlogrecptr(&opaque[OPQ_OFF_NSN..]))
}

/// `GistPageSetNSN(page, val)` (gist.h:190): `PageXLogRecPtrSet(opaque->nsn, val)`.
pub fn gist_page_set_nsn(page: &mut [u8], val: GistNSN) -> PgResult<()> {
    write_special(page, OPQ_OFF_NSN, &split_xlogrecptr(val))
}

/// `GistPageGetOpaque(page)->rightlink`.
pub fn gist_page_rightlink(page: &[u8]) -> PgResult<BlockNumber> {
    let pref = PageRef::new(page)?;
    let opaque = PageGetSpecialPointer(&pref)?;
    Ok(BlockNumber::from_ne_bytes([
        opaque[OPQ_OFF_RIGHTLINK],
        opaque[OPQ_OFF_RIGHTLINK + 1],
        opaque[OPQ_OFF_RIGHTLINK + 2],
        opaque[OPQ_OFF_RIGHTLINK + 3],
    ]))
}

/// `GistPageGetOpaque(page)->rightlink = blk`.
pub fn set_gist_page_rightlink(page: &mut [u8], blk: BlockNumber) -> PgResult<()> {
    write_special(page, OPQ_OFF_RIGHTLINK, &blk.to_ne_bytes())
}

/// `GistPageGetOpaque(page)->flags`.
pub fn gist_page_flags(page: &[u8]) -> PgResult<u16> {
    let pref = PageRef::new(page)?;
    let opaque = PageGetSpecialPointer(&pref)?;
    Ok(u16::from_ne_bytes([
        opaque[OPQ_OFF_FLAGS],
        opaque[OPQ_OFF_FLAGS + 1],
    ]))
}

/// `GistPageGetOpaque(page)->flags = flags`.
pub fn set_gist_page_flags(page: &mut [u8], flags: u16) -> PgResult<()> {
    write_special(page, OPQ_OFF_FLAGS, &flags.to_ne_bytes())
}

/// `GistPageGetOpaque(page)->gist_page_id = id`.
pub fn set_gist_page_id(page: &mut [u8], id: u16) -> PgResult<()> {
    write_special(page, OPQ_OFF_GIST_PAGE_ID, &id.to_ne_bytes())
}

// ===========================================================================
// gist.h page-flag predicates and the follow-right / deleted accessors.
// ===========================================================================

/// `GistPageIsLeaf(page)` (gist.h:172): `GistPageGetOpaque(page)->flags & F_LEAF`.
pub fn GistPageIsLeaf(page: &[u8]) -> PgResult<bool> {
    Ok((gist_page_flags(page)? & types_gist::F_LEAF) != 0)
}

/// `GistPageIsDeleted(page)` (gist.h:175): `flags & F_DELETED`.
pub fn GistPageIsDeleted(page: &[u8]) -> PgResult<bool> {
    Ok((gist_page_flags(page)? & types_gist::F_DELETED) != 0)
}

/// `GistFollowRight(page)` (gist.h:185): `flags & F_FOLLOW_RIGHT`.
pub fn GistFollowRight(page: &[u8]) -> PgResult<bool> {
    Ok((gist_page_flags(page)? & types_gist::F_FOLLOW_RIGHT) != 0)
}

/// `GistMarkFollowRight(page)` (gist.h:186): `flags |= F_FOLLOW_RIGHT`.
pub fn GistMarkFollowRight(page: &mut [u8]) -> PgResult<()> {
    let f = gist_page_flags(page)?;
    set_gist_page_flags(page, f | types_gist::F_FOLLOW_RIGHT)
}

/// `GistClearFollowRight(page)` (gist.h:187): `flags &= ~F_FOLLOW_RIGHT`.
pub fn GistClearFollowRight(page: &mut [u8]) -> PgResult<()> {
    let f = gist_page_flags(page)?;
    set_gist_page_flags(page, f & !types_gist::F_FOLLOW_RIGHT)
}

/// `GistPageGetDeleteXid(page)` (gist.h:217): the `deleteXid` stored in the
/// `GISTDeletedPageContents` after the page header (only valid when the page is
/// `F_DELETED`). When the field isn't present (an old-format deleted page, with
/// `pd_lower` short of the field) the historical fallback
/// `FullTransactionIdFromEpochAndXid(0, FirstNormalTransactionId)` is returned.
pub fn GistPageGetDeleteXid(page: &[u8]) -> PgResult<FullTransactionId> {
    let pref = PageRef::new(page)?;
    // if (pd_lower >= MAXALIGN(SizeOfPageHeaderData)
    //                 + offsetof(GISTDeletedPageContents, deleteXid)
    //                 + sizeof(FullTransactionId))   [deleteXid is at offset 0]
    let pd_lower = pref.pd_lower() as usize;
    let need = maxalign(SizeOfPageHeaderData) + SIZEOF_FULL_TRANSACTION_ID;
    if pd_lower >= need {
        // ((GISTDeletedPageContents *) PageGetContents(page))->deleteXid
        let contents = PageGetContents(&pref)?;
        let value = u64::from_ne_bytes([
            contents[0], contents[1], contents[2], contents[3], contents[4], contents[5],
            contents[6], contents[7],
        ]);
        Ok(FullTransactionId::from_u64(value))
    } else {
        Ok(FullTransactionId::from_epoch_and_xid(
            0,
            FirstNormalTransactionId,
        ))
    }
}

/// `GistPageHasGarbage(page)` (gist.h:183): `flags & F_HAS_GARBAGE`.
pub fn GistPageHasGarbage(page: &[u8]) -> PgResult<bool> {
    Ok((gist_page_flags(page)? & types_gist::F_HAS_GARBAGE) != 0)
}

/// `GistClearPageHasGarbage(page)` (gist.h:184): `flags &= ~F_HAS_GARBAGE`.
pub fn GistClearPageHasGarbage(page: &mut [u8]) -> PgResult<()> {
    let f = gist_page_flags(page)?;
    set_gist_page_flags(page, f & !types_gist::F_HAS_GARBAGE)
}

/// `GistMarkPageHasGarbage(page)` (gist.h:182): `flags |= F_HAS_GARBAGE`.
pub fn GistMarkPageHasGarbage(page: &mut [u8]) -> PgResult<()> {
    let f = gist_page_flags(page)?;
    set_gist_page_flags(page, f | types_gist::F_HAS_GARBAGE)
}

// ===========================================================================
// PageXLogRecPtr split-word codec (bufpage.h PageXLogRecPtrGet/Set).
// ===========================================================================

/// `PageXLogRecPtrGet(val)` (bufpage.h:262): combine the split `{ xlogid,
/// xrecoff }` half-words into a 64-bit `XLogRecPtr`.
fn read_xlogrecptr(bytes: &[u8]) -> XLogRecPtr {
    let xlogid = u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let xrecoff = u32::from_ne_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    ((xlogid as u64) << 32) | (xrecoff as u64)
}

/// `PageXLogRecPtrSet(ptr, lsn)` (bufpage.h:266): split a 64-bit `XLogRecPtr`
/// into the on-disk `{ xlogid, xrecoff }` half-word pair.
fn split_xlogrecptr(lsn: XLogRecPtr) -> [u8; 8] {
    let xlogid = (lsn >> 32) as u32;
    let xrecoff = (lsn & 0xFFFF_FFFF) as u32;
    let mut out = [0u8; 8];
    out[0..4].copy_from_slice(&xlogid.to_ne_bytes());
    out[4..8].copy_from_slice(&xrecoff.to_ne_bytes());
    out
}

/// Write `data` into the page's special area at byte offset `off`. The special
/// area's start offset within the page (`pd_special`) is read from the page
/// header, exactly like `GistPageGetOpaque`.
fn write_special(page: &mut [u8], off: usize, data: &[u8]) -> PgResult<()> {
    let special_off = {
        let pref = PageRef::new(page)?;
        // pd_special is the uint16 at offset 16 of PageHeaderData.
        let sp = PageGetSpecialPointer(&pref)?;
        page.len() - sp.len()
    };
    page[special_off + off..special_off + off + data.len()].copy_from_slice(data);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initpage_sets_opaque_fields() {
        let mut page = alloc::vec![0u8; BLCKSZ as usize];
        gistinitpage(&mut page, types_gist::F_LEAF).unwrap();
        assert_eq!(gist_page_flags(&page).unwrap(), types_gist::F_LEAF);
        assert_eq!(gist_page_rightlink(&page).unwrap(), BlockNumber::MAX);
        // PageGetSpecialSize == MAXALIGN(sizeof(GISTPageOpaqueData)) so checkpage
        // would accept it.
        let pref = PageRef::new(&page).unwrap();
        assert_eq!(
            PageGetSpecialSize(&pref) as usize,
            maxalign(SIZEOF_GIST_PAGE_OPAQUE_DATA)
        );
        assert!(!PageIsNew(&pref));
    }

    #[test]
    fn nsn_roundtrip() {
        let mut page = alloc::vec![0u8; BLCKSZ as usize];
        gistinitpage(&mut page, 0).unwrap();
        assert_eq!(gist_page_get_nsn(&page).unwrap(), 0);
        gist_page_set_nsn(&mut page, 0x0001_2345_6789_ABCD).unwrap();
        assert_eq!(gist_page_get_nsn(&page).unwrap(), 0x0001_2345_6789_ABCD);
        // Setting NSN must not disturb the adjacent rightlink/flags.
        assert_eq!(gist_page_rightlink(&page).unwrap(), BlockNumber::MAX);
    }
}
