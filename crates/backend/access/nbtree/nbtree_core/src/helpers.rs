//! Page-format / tuple-format inline helpers (`access/nbtree.h` and
//! `storage/bufpage.h` inlines) that the `nbtree.c` AM handler and amcheck
//! reach through `backend-access-nbtree-core-seams`. These are the
//! `PageIsNew` / `BTPageGetOpaque` / `PageGetMaxOffsetNumber` / `PageGetItem`
//! reads and the `BTreeTuple*` accessors, decoded field-by-field from the page
//! / tuple byte image exactly as in C (mirroring the `nbtdedup` idiom — never a
//! raw struct cast).

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::primitive::{BlockNumber, OffsetNumber};
use ::types_error::PgResult;
use types_nbtree::{
    BTCycleId, BT_IS_POSTING, BT_OFFSET_MASK, BT_PIVOT_HEAP_TID_ATTR, INDEX_ALT_TID_MASK,
};
use ::types_storage::storage::Buffer;
use ::types_tuple::heaptuple::{
    BlockIdData, IndexTupleData, IndexTupleSize, ItemPointerData,
};

use page::{
    ItemPointerGetOffsetNumberNoCheck, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageGetSpecialPointer, PageIsNew, PageRef,
};
use bufmgr_seams as bufmgr;

/// `sizeof(ItemPointerData)`.
const SIZEOF_IPD: usize = ::core::mem::size_of::<ItemPointerData>();

// ---------------------------------------------------------------------------
// Byte decoders (shared with nbtdedup; reproduced here so the seam crate has no
// dependency on dedup internals).
// ---------------------------------------------------------------------------

/// Read an [`ItemPointerData`] (6 `#[repr(C)]` bytes) from the start of `bytes`.
fn read_ipd(bytes: &[u8]) -> ItemPointerData {
    debug_assert!(bytes.len() >= 6);
    ItemPointerData {
        ip_blkid: BlockIdData {
            bi_hi: u16::from_ne_bytes([bytes[0], bytes[1]]),
            bi_lo: u16::from_ne_bytes([bytes[2], bytes[3]]),
        },
        ip_posid: u16::from_ne_bytes([bytes[4], bytes[5]]),
    }
}

/// Interpret the leading 8 bytes of a page item as an [`IndexTupleData`] header.
fn index_tuple_header(tuple: &[u8]) -> IndexTupleData {
    debug_assert!(tuple.len() >= 8);
    let t_tid = read_ipd(&tuple[0..6]);
    let t_info = u16::from_ne_bytes([tuple[6], tuple[7]]);
    IndexTupleData { t_tid, t_info }
}

/// `BTreeTupleIsPivot(itup)` (nbtree.h).
fn is_pivot(hdr: &IndexTupleData) -> bool {
    if (hdr.t_info & INDEX_ALT_TID_MASK) == 0 {
        return false;
    }
    (ItemPointerGetOffsetNumberNoCheck(&hdr.t_tid) & BT_IS_POSTING) == 0
}

/// `BTreeTupleIsPosting(itup)` (nbtree.h).
fn is_posting(hdr: &IndexTupleData) -> bool {
    if (hdr.t_info & INDEX_ALT_TID_MASK) == 0 {
        return false;
    }
    (ItemPointerGetOffsetNumberNoCheck(&hdr.t_tid) & BT_IS_POSTING) != 0
}

/// `BTreeTupleGetNPosting(posting)` (nbtree.h).
fn n_posting(hdr: &IndexTupleData) -> u16 {
    debug_assert!(is_posting(hdr));
    ItemPointerGetOffsetNumberNoCheck(&hdr.t_tid) & BT_OFFSET_MASK
}

/// `BTreeTupleGetPostingOffset(posting)` (nbtree.h) —
/// `BlockIdGetBlockNumber(&posting->t_tid.ip_blkid)`.
fn posting_offset(hdr: &IndexTupleData) -> u32 {
    debug_assert!(is_posting(hdr));
    ((hdr.t_tid.ip_blkid.bi_hi as u32) << 16) | (hdr.t_tid.ip_blkid.bi_lo as u32)
}

/// `BTreeTupleGetPostingN(posting, n)` — the `n`-th heap TID of a posting list.
fn posting_list_n(tuple: &[u8], n: usize) -> ItemPointerData {
    let hdr = index_tuple_header(tuple);
    let off = posting_offset(&hdr) as usize;
    read_ipd(&tuple[off + n * SIZEOF_IPD..])
}

// ---------------------------------------------------------------------------
// Byte offset of the page's special area (`pd_special`, a `u16` at offset 16).
// ---------------------------------------------------------------------------

/// `BTPageGetOpaque(page)` flag/cycleid/next decode used by [`page_opaque`].
/// Layout of the 16-byte `BTPageOpaqueData`: btpo_prev(0) btpo_next(4)
/// btpo_level(8) btpo_flags(12) btpo_cycleid(14).
fn special_fields(page: &PageRef<'_>) -> PgResult<(u16, BTCycleId, BlockNumber, u32)> {
    let special = PageGetSpecialPointer(page)?;
    let rd_u32 = |off: usize| -> u32 {
        u32::from_ne_bytes([
            special[off],
            special[off + 1],
            special[off + 2],
            special[off + 3],
        ])
    };
    let rd_u16 = |off: usize| -> u16 { u16::from_ne_bytes([special[off], special[off + 1]]) };
    let btpo_next = rd_u32(4);
    let btpo_level = rd_u32(8);
    let btpo_flags = rd_u16(12);
    let btpo_cycleid = rd_u16(14);
    Ok((btpo_flags, btpo_cycleid, btpo_next, btpo_level))
}

// ===========================================================================
// Installable seams (page-format / tuple-format inline reads).
// ===========================================================================

/// `PageIsNew(page)` (bufpage.h): is the page all-zero (never initialized)?
pub fn page_is_new(page: &[u8]) -> bool {
    match PageRef::new(page) {
        Ok(p) => PageIsNew(&p),
        // A page too short to parse is treated as not-new (mirrors the C
        // pd_upper==0 read which only inspects the header).
        Err(_) => false,
    }
}

/// `BTPageGetOpaque(page)` (nbtree.h): `(btpo_flags, btpo_cycleid, btpo_next)`.
pub fn page_opaque(page: &[u8]) -> (u16, BTCycleId, BlockNumber) {
    let p = PageRef::new(page).expect("page_opaque: malformed page");
    let (flags, cycleid, next, _level) =
        special_fields(&p).expect("page_opaque: malformed special area");
    (flags, cycleid, next)
}

/// `BTPageGetOpaque(page)->btpo_level` (nbtree.h).
pub fn page_btpo_level(page: &[u8]) -> u32 {
    let p = PageRef::new(page).expect("page_btpo_level: malformed page");
    let (_flags, _cycleid, _next, level) =
        special_fields(&p).expect("page_btpo_level: malformed special area");
    level
}

/// `opaque->btpo_cycleid = 0` written into the page in the shared buffer.
pub fn page_clear_cycleid(buf: Buffer) {
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        // pd_special offset is a u16 at byte offset 16 of the page header;
        // btpo_cycleid is at offset 14 within the 16-byte BTPageOpaqueData.
        let special_off = u16::from_ne_bytes([page[16], page[17]]) as usize;
        let off = special_off + 14;
        page[off] = 0;
        page[off + 1] = 0;
        Ok(())
    })
    .expect("page_clear_cycleid: buffer write failed");
}

/// `PageGetMaxOffsetNumber(page)` (bufpage.h).
pub fn page_get_max_offset_number(page: &[u8]) -> OffsetNumber {
    let p = PageRef::new(page).expect("page_get_max_offset_number: malformed page");
    PageGetMaxOffsetNumber(&p)
}

/// `PageGetItem(page, PageGetItemId(page, offnum))` (bufpage.h): the index tuple
/// at `offnum`, returned as owned bytes in `mcx`.
pub fn page_get_item<'mcx>(
    mcx: Mcx<'mcx>,
    page: &[u8],
    offnum: OffsetNumber,
) -> PgResult<PgVec<'mcx, u8>> {
    let p = PageRef::new(page)?;
    let itemid = PageGetItemId(&p, offnum)?;
    let item = PageGetItem(&p, &itemid)?;
    let mut v = vec_with_capacity_in(mcx, item.len())?;
    v.extend_from_slice(item);
    Ok(v)
}

/// `BTreeTupleIsPivot(itup)` (nbtree.h).
pub fn tuple_is_pivot(itup: &[u8]) -> bool {
    is_pivot(&index_tuple_header(itup))
}

/// `BTreeTupleIsPosting(itup)` (nbtree.h).
pub fn tuple_is_posting(itup: &[u8]) -> bool {
    is_posting(&index_tuple_header(itup))
}

/// `itup->t_tid` — the heap TID of a regular (non-posting) index tuple, or the
/// lowest heap TID for a posting tuple / the pivot tiebreak TID for a pivot.
/// Mirrors `BTreeTupleGetHeapTID(itup)` for the common non-pivot case the seam
/// callers use, falling back to the pivot heap-TID attribute when present.
pub fn tuple_heap_tid(itup: &[u8]) -> ItemPointerData {
    let hdr = index_tuple_header(itup);
    if is_pivot(&hdr) {
        if (ItemPointerGetOffsetNumberNoCheck(&hdr.t_tid) & BT_PIVOT_HEAP_TID_ATTR) != 0 {
            let sz = IndexTupleSize(&hdr);
            let off = sz - SIZEOF_IPD;
            return read_ipd(&itup[off..]);
        }
        // Heap-TID attribute was truncated: return an invalid pointer
        // (bi_hi/bi_lo/ip_posid all zero), matching the C NULL-ish result.
        ItemPointerData::default()
    } else if is_posting(&hdr) {
        posting_list_n(itup, 0)
    } else {
        hdr.t_tid
    }
}

/// `BTreeTupleGetNPosting(itup)` (nbtree.h): the number of TIDs in a posting
/// tuple.
pub fn tuple_n_posting(itup: &[u8]) -> i32 {
    n_posting(&index_tuple_header(itup)) as i32
}

/// `BTreeTupleGetPostingN(itup, n)` (nbtree.h): the `n`th heap TID in a posting
/// tuple.
pub fn tuple_posting_tid(itup: &[u8], n: i32) -> ItemPointerData {
    posting_list_n(itup, n as usize)
}

/// `_bt_form_posting(base, htids, nhtids)` (nbtdedup.c): build a posting-list
/// index tuple from `base` and the heap-TID array, returned as owned bytes.
/// Installed from this crate as a thin wrapper over `nbtdedup`'s pure builder
/// (which carries the explicit `nhtids` C parameter); `nhtids == htids.len()`.
pub fn bt_form_posting<'mcx>(
    mcx: Mcx<'mcx>,
    base: &[u8],
    htids: &[ItemPointerData],
) -> PgResult<PgVec<'mcx, u8>> {
    dedup::_bt_form_posting(mcx, base, htids, htids.len() as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal regular (non-pivot, non-posting) index-tuple byte image:
    /// 6-byte t_tid + 2-byte t_info, then `extra` payload bytes.
    fn make_regular(blk: u32, posid: u16, t_info: u16, extra: usize) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&((blk >> 16) as u16).to_ne_bytes()); // bi_hi
        v.extend_from_slice(&(blk as u16).to_ne_bytes()); // bi_lo
        v.extend_from_slice(&posid.to_ne_bytes()); // ip_posid
        v.extend_from_slice(&t_info.to_ne_bytes()); // t_info
        v.resize(8 + extra, 0);
        v
    }

    #[test]
    fn regular_tuple_is_neither_pivot_nor_posting() {
        // t_info with no INDEX_ALT_TID_MASK -> plain non-pivot tuple.
        let t = make_regular(42, 7, 16 /* size */, 8);
        assert!(!tuple_is_pivot(&t));
        assert!(!tuple_is_posting(&t));
        // heap TID round-trips through the byte codec.
        let tid = tuple_heap_tid(&t);
        assert_eq!(tid.ip_blkid.bi_hi, 0);
        assert_eq!(tid.ip_blkid.bi_lo, 42);
        assert_eq!(tid.ip_posid, 7);
    }

    #[test]
    fn posting_flag_detection() {
        // INDEX_ALT_TID_MASK set + BT_IS_POSTING bit in offset => posting tuple.
        let mut t = make_regular(0, BT_IS_POSTING, INDEX_ALT_TID_MASK, 0);
        // posting offset (BlockIdGetBlockNumber of t_tid) must point past header;
        // set bi_hi/bi_lo to a small MAXALIGN'd offset of 8.
        t[0..2].copy_from_slice(&0u16.to_ne_bytes()); // bi_hi
        t[2..4].copy_from_slice(&8u16.to_ne_bytes()); // bi_lo == offset 8
        // offset field carries (nposting | BT_IS_POSTING); set nposting = 2.
        let offset = 2u16 | BT_IS_POSTING;
        t[4..6].copy_from_slice(&offset.to_ne_bytes());
        // append 2 heap TIDs starting at offset 8.
        t.resize(8 + 2 * SIZEOF_IPD, 0);
        // TID0 = (blk 100, pos 1)
        t[8..10].copy_from_slice(&0u16.to_ne_bytes());
        t[10..12].copy_from_slice(&100u16.to_ne_bytes());
        t[12..14].copy_from_slice(&1u16.to_ne_bytes());
        // TID1 = (blk 200, pos 2)
        t[14..16].copy_from_slice(&0u16.to_ne_bytes());
        t[16..18].copy_from_slice(&200u16.to_ne_bytes());
        t[18..20].copy_from_slice(&2u16.to_ne_bytes());

        assert!(tuple_is_posting(&t));
        assert!(!tuple_is_pivot(&t));
        assert_eq!(tuple_n_posting(&t), 2);
        let t0 = tuple_posting_tid(&t, 0);
        assert_eq!(t0.ip_blkid.bi_lo, 100);
        assert_eq!(t0.ip_posid, 1);
        let t1 = tuple_posting_tid(&t, 1);
        assert_eq!(t1.ip_blkid.bi_lo, 200);
        assert_eq!(t1.ip_posid, 2);
        // tuple_heap_tid of a posting tuple is the lowest (first) TID.
        let lo = tuple_heap_tid(&t);
        assert_eq!(lo.ip_blkid.bi_lo, 100);
        assert_eq!(lo.ip_posid, 1);
    }

    #[test]
    fn pivot_flag_detection() {
        // INDEX_ALT_TID_MASK set, BT_IS_POSTING NOT set => pivot tuple.
        let t = make_regular(0, 0 /* offset 0, no posting bit */, INDEX_ALT_TID_MASK, 0);
        assert!(tuple_is_pivot(&t));
        assert!(!tuple_is_posting(&t));
    }

    #[test]
    fn read_ipd_roundtrip() {
        let mut b = [0u8; 6];
        b[0..2].copy_from_slice(&5u16.to_ne_bytes());
        b[2..4].copy_from_slice(&9u16.to_ne_bytes());
        b[4..6].copy_from_slice(&3u16.to_ne_bytes());
        let tid = read_ipd(&b);
        assert_eq!(tid.ip_blkid.bi_hi, 5);
        assert_eq!(tid.ip_blkid.bi_lo, 9);
        assert_eq!(tid.ip_posid, 3);
    }
}
