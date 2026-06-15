//! Port of `src/backend/access/spgist/spgdoinsert.c` (PostgreSQL 18.3): the
//! SP-GiST insert engine.
//!
//! `spgdoinsert` walks the SP-GiST tree from the root, dispatching the opclass
//! `choose` support procedure (via the typed [`spg_choose`] seam) at each inner
//! tuple to decide whether to descend, add a node, or split the inner tuple, and
//! finally placing a leaf tuple — possibly via `addLeafTuple`, `moveLeafs`, or a
//! full `doPickSplit` (which calls the opclass `picksplit` support procedure).
//!
//! ## Memory / page model
//!
//! The C `SPPageDesc` carries a live `Page` pointer plus its `Buffer`. In this
//! repo a page is never a long-lived borrow: [`SPPageDesc`] holds only the
//! `Buffer` (plus block/offset/node), and page bytes are read with
//! [`buffer_get_page`](bufmgr::buffer_get_page) (a snapshot) or mutated in place
//! through [`with_buffer_page`](bufmgr::with_buffer_page). The C invariant "if
//! `buffer` is valid we hold pin + exclusive lock, and `page` is valid exactly
//! then" is preserved: `page` is implicit, derived from `buffer`.
//!
//! On-disk tuples (`SpGistLeafTuple` / `SpGistInnerTuple` / `SpGistNodeTuple` /
//! `SpGistDeadTuple`) are owned byte images ([`PgVec`]`<u8>`), exactly as the F0
//! builders produce them.
//!
//! WAL is emitted inline (the nbtree/BRIN idiom): a critical section wraps the
//! mutate-and-log region, the record header is serialized by the
//! `types_xlog_records::spgxlog` write-side encoders, and `XLogRegisterBuffer`
//! registers the live buffers by block id. During an index build
//! (`state.isBuild`) page changes are made but no WAL record is written.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use mcx::{Mcx, PgVec};

use backend_storage_page::{
    ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerIsValid, ItemPointerSet,
    PageAddItemExtended, PageGetExactFreeSpace, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageIndexTupleDelete, PageMut, PageRef,
};
use backend_utils_error::ereport;
use types_error::error::{ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use types_error::{PgError, PgResult};

use types_core::primitive::{
    BlockNumber, InvalidBlockNumber, OffsetNumber, Oid, RegProcedure, Size,
};
use types_rel::Relation;
use types_storage::buf::{Buffer, InvalidBuffer, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{
    ItemPointerData, TupleDescData, FIRST_OFFSET_NUMBER as FirstOffsetNumber,
    INVALID_OFFSET_NUMBER as InvalidOffsetNumber,
};

use types_spgist::{
    spgChooseIn, spgChooseOut, spgChooseOutResult, spgChooseResultType, spgPickSplitIn,
    spgPickSplitOut, SpGistState, SpGistTypeDesc, GBUF_INNER_PARITY, GBUF_LEAF, GBUF_NULLS,
    SPGIST_COMPRESS_PROC, SPGIST_DEAD, SPGIST_LEAF, SPGIST_LIVE, SPGIST_NULLS, SPGIST_PLACEHOLDER,
    SPGIST_REDIRECT, SpGistBlockIsRoot, SGITMAXNNODES, spgFirstIncludeColumn, spgKeyColumn,
    SPGIST_NULL_BLKNO, SPGIST_ROOT_BLKNO, SPGIST_METAPAGE_BLKNO,
};
use types_spgist::{SPGIST_CHOOSE_PROC, SPGIST_PICKSPLIT_PROC};

use types_xlog_records::spgxlog::{
    spgxlogAddLeaf, spgxlogAddNode, spgxlogMoveLeafs, spgxlogPickSplit, spgxlogSplitTuple,
    spgxlogState,
};
use types_wal::xloginsert::{REGBUF_STANDARD, REGBUF_WILL_INIT};

use backend_access_transam_xloginsert_seams as xloginsert;
use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_init_miscinit_seams as miscinit;

use crate::{
    node_tuple_has_nulls, node_tuple_size, read_inner_datum, spgFormDeadTuple, spgFormInnerTuple,
    spgFormLeafTuple, spgFormNodeTuple, spgDeformLeafTuple, spgExtractNodeLabels, write_item_pointer,
    SpGistGetBuffer, SpGistGetLeafTupleSize, SpGistInitBuffer, SpGistPageAddNewItem,
    SpGistPageIsLeaf, SpGistPageStoresNulls, SpGistSetLastUsedPage, SGDTSIZE, SGITHDRSZ, SGLTHDRSZ,
    SIZEOF_ITEM_ID_DATA, SPGIST_PAGE_CAPACITY,
};

/// `RM_SPGIST_ID` (rmgrlist.h entry 16) — the SP-GiST resource-manager id.
/// Defined locally until rmgr ids migrate to `types-wal` (mirrors BRIN's
/// `RM_BRIN_ID`).
const RM_SPGIST_ID: types_core::RmgrId = 16;

// XLOG record types for SPGiST (spgxlog.h).
const XLOG_SPGIST_ADD_LEAF: u8 = 0x10;
const XLOG_SPGIST_MOVE_LEAFS: u8 = 0x20;
const XLOG_SPGIST_ADD_NODE: u8 = 0x30;
const XLOG_SPGIST_SPLIT_TUPLE: u8 = 0x40;
const XLOG_SPGIST_PICKSPLIT: u8 = 0x50;

// ===========================================================================
// SPPageDesc and small private mirrors of spgist_private.h macros.
// ===========================================================================

/// `SPPageDesc` (spgdoinsert.c:36) — coordinates for one page in the descent.
///
/// `page` (a live `Page` pointer in C) is implicit here: page bytes are reached
/// through `buffer`. When `buffer != InvalidBuffer` we hold the pin + exclusive
/// lock and the page is available.
#[derive(Clone, Copy, Debug)]
struct SPPageDesc {
    /// block number, or `InvalidBlockNumber`.
    blkno: BlockNumber,
    /// page's buffer number, or `InvalidBuffer`.
    buffer: Buffer,
    /// offset of tuple, or `InvalidOffsetNumber`.
    offnum: OffsetNumber,
    /// node number within inner tuple, or -1.
    node: i32,
}

impl SPPageDesc {
    /// All-invalid descriptor (the C zero-init / explicit invalidation).
    fn invalid() -> Self {
        SPPageDesc {
            blkno: InvalidBlockNumber,
            buffer: InvalidBuffer,
            offnum: InvalidOffsetNumber,
            node: -1,
        }
    }
}

/// `BufferIsValid(buf)`.
#[inline]
fn buffer_is_valid(buf: Buffer) -> bool {
    buf != InvalidBuffer
}

/// `OidIsValid(oid)`.
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != types_core::primitive::InvalidOid
}

/// `RelationGetRelationName(index)` for error messages.
fn rel_name(index: &Relation<'_>) -> alloc::string::String {
    index.name().into()
}

/// `elog(ERROR, "...")` (internal message, no SQLSTATE).
fn elog_error(msg: alloc::string::String) -> PgError {
    ereport(ERROR).errmsg_internal(msg).into_error()
}

// --- page-opaque counters used here that are not exposed by lib.rs ---
//
// The SP-GiST special area lives in the last MAXALIGN element of the page:
// flags (u16 @0), nRedirection (u16 @2), nPlaceholder (u16 @4), page_id (@6).
const OPAQUE_OFFSET: usize =
    types_core::primitive::BLCKSZ as usize - crate::MAXALIGN(core::mem::size_of::<
        types_spgist::SpGistPageOpaqueData,
    >());

#[inline]
fn opaque_n_redirection(page: &[u8]) -> u16 {
    u16::from_ne_bytes([page[OPAQUE_OFFSET + 2], page[OPAQUE_OFFSET + 3]])
}
#[inline]
fn set_opaque_n_redirection(page: &mut [u8], v: u16) {
    page[OPAQUE_OFFSET + 2..OPAQUE_OFFSET + 4].copy_from_slice(&v.to_ne_bytes());
}
#[inline]
fn opaque_n_placeholder(page: &[u8]) -> u16 {
    u16::from_ne_bytes([page[OPAQUE_OFFSET + 4], page[OPAQUE_OFFSET + 5]])
}
#[inline]
fn set_opaque_n_placeholder(page: &mut [u8], v: u16) {
    page[OPAQUE_OFFSET + 4..OPAQUE_OFFSET + 6].copy_from_slice(&v.to_ne_bytes());
}

/// `SpGistPageGetFreeSpace(p, n)` (spgist_private.h):
/// `PageGetExactFreeSpace(p) + Min(nPlaceholder, n) * (SGDTSIZE + sizeof(ItemIdData))`.
fn SpGistPageGetFreeSpace(page: &[u8], n: usize) -> PgResult<usize> {
    let free = PageGetExactFreeSpace(&PageRef::new(page)?);
    let nph = opaque_n_placeholder(page) as usize;
    Ok(free + core::cmp::min(nph, n) * (SGDTSIZE + SIZEOF_ITEM_ID_DATA))
}

// --- leaf-tuple header field accessors against on-disk bytes ---

/// `SGLT_GET_NEXTOFFSET(tup)` — low 14 bits of `t_info` (the uint16 @4).
#[inline]
pub(crate) fn lt_get_next_offset(tup: &[u8]) -> OffsetNumber {
    u16::from_ne_bytes([tup[4], tup[5]]) & 0x3FFF
}
/// `SGLT_GET_HASNULLMASK(tup)` — bit 0x8000 of `t_info`.
#[inline]
pub(crate) fn lt_get_has_null_mask(tup: &[u8]) -> bool {
    u16::from_ne_bytes([tup[4], tup[5]]) & 0x8000 != 0
}
/// `SGLT_SET_NEXTOFFSET(tup, off)` — preserve the two high flag bits of `t_info`.
#[inline]
fn lt_set_next_offset(tup: &mut [u8], off: OffsetNumber) {
    let cur = u16::from_ne_bytes([tup[4], tup[5]]);
    let v = (cur & 0xC000) | (off & 0x3FFF);
    tup[4..6].copy_from_slice(&v.to_ne_bytes());
}
/// A leaf/dead tuple's `tupstate` (low 2 bits of the packed `bits` word @0).
#[inline]
pub(crate) fn lt_tupstate(tup: &[u8]) -> u32 {
    u32::from_ne_bytes([tup[0], tup[1], tup[2], tup[3]]) & 0x3
}
/// A leaf/dead/inner tuple's `size`. For leaf/dead: `bits >> 2`. For an inner
/// tuple the size is the separate uint16 @4; callers pick the right accessor.
#[inline]
fn lt_size(tup: &[u8]) -> usize {
    (u32::from_ne_bytes([tup[0], tup[1], tup[2], tup[3]]) >> 2) as usize
}

// --- inner-tuple header field accessors against on-disk bytes ---

/// An inner tuple's `size` (the uint16 @4).
#[inline]
fn it_size(tup: &[u8]) -> usize {
    u16::from_ne_bytes([tup[4], tup[5]]) as usize
}
/// An inner tuple's `nNodes:13`.
#[inline]
pub(crate) fn it_n_nodes(tup: &[u8]) -> u32 {
    (u32::from_ne_bytes([tup[0], tup[1], tup[2], tup[3]]) >> 3) & 0x1FFF
}
/// An inner tuple's `prefixSize:16`.
#[inline]
pub(crate) fn it_prefix_size(tup: &[u8]) -> usize {
    ((u32::from_ne_bytes([tup[0], tup[1], tup[2], tup[3]]) >> 16) & 0xFFFF) as usize
}
/// An inner tuple's `allTheSame:1`.
#[inline]
pub(crate) fn it_all_the_same(tup: &[u8]) -> bool {
    (u32::from_ne_bytes([tup[0], tup[1], tup[2], tup[3]]) >> 2) & 0x1 != 0
}
/// Set an inner tuple's `allTheSame:1` bit (on the on-disk image).
#[inline]
fn it_set_all_the_same(tup: &mut [u8], v: bool) {
    let mut w = u32::from_ne_bytes([tup[0], tup[1], tup[2], tup[3]]);
    w = (w & !(0x1 << 2)) | ((v as u32) << 2);
    tup[0..4].copy_from_slice(&w.to_ne_bytes());
}

/// `SGITNODEPTR(x)` offset: where the node array starts (after header + prefix).
#[inline]
fn it_node_ptr_off(tup: &[u8]) -> usize {
    SGITHDRSZ + it_prefix_size(tup)
}

/// `SGITDATUM(x, s)` (spgist_private.h): the prefix datum of an inner tuple, or
/// `(Datum) 0` if it has no prefix.
pub(crate) fn it_datum<'mcx>(
    mcx: Mcx<'mcx>,
    state: &SpGistState<'mcx>,
    tup: &[u8],
) -> PgResult<Datum<'mcx>> {
    if it_prefix_size(tup) > 0 {
        read_inner_datum(mcx, &state.attPrefixType, &tup[SGITHDRSZ..])
    } else {
        Ok(Datum::null())
    }
}

/// `SGLTDATUM(x, s)` (spgist_private.h): the (single) key datum of a leaf tuple,
/// `fetch_att(SGLTDATAPTR, attLeafType.attbyval, attLeafType.attlen)`.
pub(crate) fn lt_datum<'mcx>(
    mcx: Mcx<'mcx>,
    state: &SpGistState<'mcx>,
    tup: &[u8],
) -> PgResult<Datum<'mcx>> {
    let dataptr = SGLTHDRSZ(lt_get_has_null_mask(tup));
    fetch_att(mcx, &state.attLeafType, &tup[dataptr..])
}

/// `fetch_att(T, attbyval, attlen)` (tupmacs.h): read a stored attribute as a
/// `Datum`. By-value reads the attlen-sized scalar word; by-reference returns
/// the verbatim bytes (attlen / varsize_any).
fn fetch_att<'mcx>(
    mcx: Mcx<'mcx>,
    att: &SpGistTypeDesc,
    data: &[u8],
) -> PgResult<Datum<'mcx>> {
    if att.attbyval {
        let word = match att.attlen {
            1 => data[0] as usize,
            2 => u16::from_ne_bytes([data[0], data[1]]) as usize,
            4 => u32::from_ne_bytes([data[0], data[1], data[2], data[3]]) as usize,
            _ => {
                let mut w = [0u8; 8];
                w.copy_from_slice(&data[..8]);
                usize::from_ne_bytes(w)
            }
        };
        Ok(Datum::ByVal(word))
    } else {
        // By-reference: reuse the inner-datum decoder (attlen / varsize_any).
        read_inner_datum(mcx, att, data)
    }
}

// ===========================================================================
// IndexTupleSize / node iteration helpers.
// ===========================================================================

/// `SGITITERATE` — collect the byte offset of each node tuple inside `inner`.
pub(crate) fn node_offsets(inner: &[u8]) -> Vec<usize> {
    let n = it_n_nodes(inner) as usize;
    let mut offs = Vec::with_capacity(n);
    let mut off = it_node_ptr_off(inner);
    for _ in 0..n {
        offs.push(off);
        off += node_tuple_size(&inner[off..]);
    }
    offs
}

/// Read a node tuple's `t_tid` (the 6-byte ItemPointerData @0).
pub(crate) fn node_t_tid(node: &[u8]) -> ItemPointerData {
    ItemPointerData {
        ip_blkid: types_tuple::heaptuple::BlockIdData {
            bi_hi: u16::from_ne_bytes([node[0], node[1]]),
            bi_lo: u16::from_ne_bytes([node[2], node[3]]),
        },
        ip_posid: u16::from_ne_bytes([node[4], node[5]]),
    }
}

/// Write a node tuple's `t_tid` in place (block/offset), preserving `t_info`.
fn node_set_t_tid(node: &mut [u8], blkno: BlockNumber, offnum: OffsetNumber) {
    let mut tid = ItemPointerData::default();
    ItemPointerSet(&mut tid, blkno, offnum);
    write_item_pointer(&mut node[0..6], &tid);
}

// ===========================================================================
// spgUpdateNodeLink (exported)
// ===========================================================================

/// `spgUpdateNodeLink(tup, nodeN, blkno, offset)` (spgdoinsert.c:51) — set the
/// downlink (`t_tid`) of node `nodeN` in inner tuple `tup` to `(blkno, offset)`.
pub fn spgUpdateNodeLink(
    tup: &mut [u8],
    node_n: i32,
    blkno: BlockNumber,
    offset: OffsetNumber,
) -> PgResult<()> {
    let offs = node_offsets(tup);
    for (i, &noff) in offs.iter().enumerate() {
        if i as i32 == node_n {
            node_set_t_tid(&mut tup[noff..], blkno, offset);
            return Ok(());
        }
    }
    Err(elog_error(format!(
        "failed to find requested node {node_n} in SPGiST inner tuple"
    )))
}

// ===========================================================================
// addNode (static)
// ===========================================================================

/// `addNode(state, tuple, label, offset)` (spgdoinsert.c:79) — return a new
/// inner-tuple image identical to `tuple` but with an additional node carrying
/// `label` inserted at position `offset`. The new node's downlink is invalid.
fn addNode<'mcx>(
    mcx: Mcx<'mcx>,
    state: &SpGistState<'mcx>,
    tuple: &[u8],
    label: &Datum<'_>,
    offset: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let n_nodes = it_n_nodes(tuple) as i32;
    let offset = if offset < 0 {
        // Generic SP-GiST programming error.
        n_nodes
    } else if offset > n_nodes {
        return Err(elog_error(format!(
            "invalid offset {offset} for SPGiST node, max {n_nodes}"
        )));
    } else {
        offset
    };

    let node_offs = node_offsets(tuple);
    let mut nodes: Vec<PgVec<'mcx, u8>> = Vec::with_capacity((n_nodes + 1) as usize);

    // Copy existing nodes, shifting those at/after `offset` up by one and
    // inserting the new node at `offset`.
    for i in 0..n_nodes {
        if i == offset {
            nodes.push(spgFormNodeTuple(mcx, state, label, false)?);
        }
        // Copy node i verbatim.
        let noff = node_offs[i as usize];
        let nsz = node_tuple_size(&tuple[noff..]);
        let mut buf = mcx::vec_with_capacity_in(mcx, nsz)?;
        buf.extend_from_slice(&tuple[noff..noff + nsz]);
        nodes.push(buf);
    }
    if offset == n_nodes {
        nodes.push(spgFormNodeTuple(mcx, state, label, false)?);
    }

    let prefix = it_datum(mcx, state, tuple)?;
    spgFormInnerTuple(mcx, state, it_prefix_size(tuple) > 0, &prefix, &nodes)
}

// ===========================================================================
// spgPageIndexMultiDelete (exported)
// ===========================================================================

/// `spgPageIndexMultiDelete(state, page, itemnos, firststate, reststate, blkno,
/// offnum)` (spgdoinsert.c:130) — delete the given items from `page`, replacing
/// each with a dead tuple of the given state. The *first* of `itemnos`
/// (unsorted) gets `firststate`, the rest get `reststate`. Updates the page's
/// redirection/placeholder counters. Must do no pallocs (called in critical
/// sections); `state.deadTupleStorage` provides the workspace.
pub fn spgPageIndexMultiDelete(
    state: &mut SpGistState<'_>,
    page: &mut [u8],
    itemnos: &[OffsetNumber],
    firststate: u32,
    reststate: u32,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) -> PgResult<()> {
    let nitems = itemnos.len();
    if nitems == 0 {
        return Ok(());
    }

    // Sort a copy (PageIndexMultiDelete needs ascending offsets); the original
    // order is kept to identify the first item.
    let mut sortednos: Vec<OffsetNumber> = itemnos.to_vec();
    if nitems > 1 {
        sortednos.sort_unstable();
    }
    page_index_multi_delete(page, &sortednos)?;

    let first_item = itemnos[0];

    // Cache: only rebuild the dead tuple when its state changes.
    let mut cached_state: Option<u32> = None;
    for &itemno in &sortednos {
        let tupstate = if itemno == first_item {
            firststate
        } else {
            reststate
        };
        if cached_state != Some(tupstate) {
            spgFormDeadTuple(state, tupstate, blkno, offnum);
            cached_state = Some(tupstate);
        }
        let dead = state
            .deadTupleStorage
            .as_ref()
            .expect("spgPageIndexMultiDelete: deadTupleStorage is NULL")
            .clone();
        let added = {
            let mut pm = PageMut::new(page)?;
            // PageAddItem(page, tuple, size, itemno, false, false) — overwrite
            // false; the slot was just freed by PageIndexMultiDelete.
            PageAddItemExtended(&mut pm, &dead, itemno, 0)?
        };
        if added != itemno {
            return Err(elog_error(format!(
                "failed to add item of size {} to SPGiST index page",
                dead.len()
            )));
        }

        if tupstate == SPGIST_REDIRECT {
            set_opaque_n_redirection(page, opaque_n_redirection(page) + 1);
        } else if tupstate == SPGIST_PLACEHOLDER {
            set_opaque_n_placeholder(page, opaque_n_placeholder(page) + 1);
        }
    }
    Ok(())
}

/// `PageIndexMultiDelete(page, sortednos)` over the page-byte primitive: delete
/// the given ascending offsets. The repo exposes single-offset deletion; deleting
/// from high to low keeps the remaining offsets stable.
fn page_index_multi_delete(page: &mut [u8], sortednos: &[OffsetNumber]) -> PgResult<()> {
    for &off in sortednos.iter().rev() {
        let mut pm = PageMut::new(page)?;
        PageIndexTupleDelete(&mut pm, off)?;
    }
    Ok(())
}

// ===========================================================================
// saveNodeLink (static) — needs the parent's buffer/page.
// ===========================================================================

/// `saveNodeLink(index, parent, blkno, offnum)` (spgdoinsert.c:185) — update the
/// parent inner tuple's `parent.node` downlink to `(blkno, offnum)` and mark the
/// parent buffer dirty. Must be the last change to the parent page in a WAL
/// action.
fn saveNodeLink(
    parent: &SPPageDesc,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) -> PgResult<()> {
    let node_n = parent.node;
    let parent_off = parent.offnum;
    bufmgr::with_buffer_page::call(parent.buffer, &mut |pg: &mut [u8]| {
        let item_off = {
            let pr = PageRef::new(pg)?;
            let iid = PageGetItemId(&pr, parent_off)?;
            iid.lp_off() as usize
        };
        let inner = &mut pg[item_off..];
        spgUpdateNodeLink(inner, node_n, blkno, offnum)
    })?;
    bufmgr::mark_buffer_dirty::call(parent.buffer);
    Ok(())
}

// ===========================================================================
// setRedirectionTuple (static)
// ===========================================================================

/// `setRedirectionTuple(current, position, blkno, offnum)` (spgdoinsert.c:567) —
/// point an existing REDIRECT dead tuple at `(blkno, offnum)`. Operates on the
/// page bytes of `current.buffer`.
fn setRedirectionTuple(
    current: &SPPageDesc,
    position: OffsetNumber,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) -> PgResult<()> {
    bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
        let item_off = {
            let pr = PageRef::new(pg)?;
            let iid = PageGetItemId(&pr, position)?;
            iid.lp_off() as usize
        };
        // SpGistDeadTupleData.pointer is the ItemPointerData @6.
        let mut tid = ItemPointerData::default();
        ItemPointerSet(&mut tid, blkno, offnum);
        write_item_pointer(&mut pg[item_off + 6..item_off + 12], &tid);
        Ok(())
    })
}

// ===========================================================================
// checkSplitConditions (static)
// ===========================================================================

/// `checkSplitConditions(index, state, current, &n_to_split)` (spgdoinsert.c:332)
/// — examine the leaf chain headed by `current.offnum` and report its live-tuple
/// count and total size, to decide whether a `moveLeafs` is worthwhile. Returns
/// `(totalSize, nToSplit)`.
fn checkSplitConditions(
    state: &SpGistState<'_>,
    page: &[u8],
    current: &SPPageDesc,
) -> PgResult<(usize, i32)> {
    if SpGistBlockIsRoot(current.blkno) {
        // Impossible values to force the doPickSplit path.
        return Ok((types_core::primitive::BLCKSZ as usize, types_core::primitive::BLCKSZ as i32));
    }

    let mut n: i32 = 0;
    let mut total_size: usize = 0;
    let mut i = current.offnum;
    while i != InvalidOffsetNumber {
        let it_off = {
            let pr = PageRef::new(page)?;
            let iid = PageGetItemId(&pr, i)?;
            iid.lp_off() as usize
        };
        let it = &page[it_off..];
        let st = lt_tupstate(it);
        if st == SPGIST_LIVE {
            debug_assert!(SpGistPageIsLeaf(page));
            n += 1;
            total_size += lt_size(it) + SIZEOF_ITEM_ID_DATA;
        } else if st == SPGIST_DEAD {
            // The only non-live state allowed here, and only as the first/only
            // chain item.
            debug_assert_eq!(lt_get_next_offset(it), InvalidOffsetNumber);
        } else {
            return Err(elog_error("unexpected SPGiST tuple state".into()));
        }
        i = lt_get_next_offset(it);
    }
    let _ = state;
    Ok((total_size, n))
}

// ===========================================================================
// addLeafTuple (static)
// ===========================================================================

/// `addLeafTuple(index, state, leafTuple, current, parent, isNulls, isNew)`
/// (spgdoinsert.c:202) — add a leaf tuple to the page in `current`, either as a
/// fresh item (when not part of a chain / on a root page) or chained onto the
/// existing head tuple.
fn addLeafTuple<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    state: &SpGistState<'mcx>,
    leaf_tuple: &mut PgVec<'mcx, u8>,
    current: &mut SPPageDesc,
    parent: &SPPageDesc,
    is_nulls: bool,
    is_new: bool,
) -> PgResult<()> {
    let leaf_size = leaf_tuple.len();

    let mut xlrec = spgxlogAddLeaf {
        newPage: is_new,
        storesNulls: is_nulls,
        offnumLeaf: InvalidOffsetNumber,
        offnumHeadLeaf: InvalidOffsetNumber,
        offnumParent: InvalidOffsetNumber,
        nodeI: 0,
    };

    miscinit::start_crit_section::call();

    if current.offnum == InvalidOffsetNumber || SpGistBlockIsRoot(current.blkno) {
        // Tuple is not part of a chain.
        lt_set_next_offset(leaf_tuple, InvalidOffsetNumber);
        let mut new_off = InvalidOffsetNumber;
        bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
            new_off = SpGistPageAddNewItem(state, pg, leaf_tuple, leaf_size as Size, None, false)?;
            Ok(())
        })?;
        current.offnum = new_off;

        xlrec.offnumLeaf = current.offnum;

        // Must update parent downlink if any.
        if buffer_is_valid(parent.buffer) {
            xlrec.offnumParent = parent.offnum;
            xlrec.nodeI = parent.node as u16;
            saveNodeLink(parent, current.blkno, current.offnum)?;
        }
    } else {
        // Tuple must be inserted into existing chain.
        let head_off = current.offnum;
        let head_state = {
            let pg = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
            let pr = PageRef::new(&pg)?;
            let iid = PageGetItemId(&pr, head_off)?;
            let head = PageGetItem(&pr, &iid)?;
            lt_tupstate(head)
        };

        if head_state == SPGIST_LIVE {
            let head_next = {
                let pg = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
                let pr = PageRef::new(&pg)?;
                let iid = PageGetItemId(&pr, head_off)?;
                lt_get_next_offset(PageGetItem(&pr, &iid)?)
            };
            lt_set_next_offset(leaf_tuple, head_next);
            let mut off = InvalidOffsetNumber;
            bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
                off = SpGistPageAddNewItem(state, pg, leaf_tuple, leaf_size as Size, None, false)?;
                Ok(())
            })?;
            // Re-get head (page may have moved it) and point it at the new tuple.
            bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
                let item_off = {
                    let pr = PageRef::new(pg)?;
                    let iid = PageGetItemId(&pr, head_off)?;
                    iid.lp_off() as usize
                };
                lt_set_next_offset(&mut pg[item_off..], off);
                Ok(())
            })?;

            xlrec.offnumLeaf = off;
            xlrec.offnumHeadLeaf = head_off;
            current.offnum = off;
        } else if head_state == SPGIST_DEAD {
            // Replace a DEAD tuple in place.
            lt_set_next_offset(leaf_tuple, InvalidOffsetNumber);
            bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
                let mut pm = PageMut::new(pg)?;
                PageIndexTupleDelete(&mut pm, head_off)?;
                let added = {
                    let mut pm2 = PageMut::new(pg)?;
                    PageAddItemExtended(&mut pm2, leaf_tuple, head_off, 0)?
                };
                if added != head_off {
                    return Err(elog_error(format!(
                        "failed to add item of size {leaf_size} to SPGiST index page"
                    )));
                }
                Ok(())
            })?;
            // WAL replay distinguishes this case by equal offnums.
            xlrec.offnumLeaf = head_off;
            xlrec.offnumHeadLeaf = head_off;
        } else {
            return Err(elog_error("unexpected SPGiST tuple state".into()));
        }
    }

    bufmgr::mark_buffer_dirty::call(current.buffer);

    if relation_needs_wal(index) && !state.isBuild {
        xloginsert::xlog_begin_insert::call()?;
        xloginsert::xlog_register_data::call(&xlrec.to_bytes())?;
        xloginsert::xlog_register_data::call(&leaf_tuple[..leaf_size])?;

        let mut flags = REGBUF_STANDARD;
        if xlrec.newPage {
            flags |= REGBUF_WILL_INIT;
        }
        xloginsert::xlog_register_buffer::call(0, current.buffer, flags)?;
        if xlrec.offnumParent != InvalidOffsetNumber {
            xloginsert::xlog_register_buffer::call(1, parent.buffer, REGBUF_STANDARD)?;
        }

        let recptr = xloginsert::xlog_insert_record::call(RM_SPGIST_ID, XLOG_SPGIST_ADD_LEAF)?;
        bufmgr::page_set_lsn::call(current.buffer, recptr)?;
        if xlrec.offnumParent != InvalidOffsetNumber {
            bufmgr::page_set_lsn::call(parent.buffer, recptr)?;
        }
    }

    miscinit::end_crit_section::call();
    Ok(())
}

/// `RelationNeedsWAL(index)`.
fn relation_needs_wal(index: &Relation<'_>) -> bool {
    relcache::relation_needs_wal::call(index)
}

/// Build a `spgxlogState` from `STORE_STATE(state, dest)`.
fn store_state(state: &SpGistState<'_>) -> spgxlogState {
    spgxlogState {
        redirectXid: state.redirectXid,
        isBuild: state.isBuild,
    }
}

// ===========================================================================
// moveLeafs (static)
// ===========================================================================

/// `moveLeafs(index, state, current, parent, newLeafTuple, isNulls)`
/// (spgdoinsert.c:386) — move all live tuples of the chain headed by
/// `current.offnum` (plus the new one) to a fresh leaf page, leaving a redirect
/// behind, and re-point the parent downlink.
fn moveLeafs<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    state: &mut SpGistState<'mcx>,
    current: &mut SPPageDesc,
    parent: &SPPageDesc,
    new_leaf_tuple: &mut PgVec<'mcx, u8>,
    is_nulls: bool,
) -> PgResult<()> {
    debug_assert!(buffer_is_valid(parent.buffer) && parent.buffer != current.buffer);

    let cur_page = bufmgr::buffer_get_page::call(mcx, current.buffer)?;
    let max = PageGetMaxOffsetNumber(&PageRef::new(&cur_page)?);

    let new_leaf_size = new_leaf_tuple.len();
    let mut size = new_leaf_size + SIZEOF_ITEM_ID_DATA;

    // Walk the chain, recording which to delete and accumulating size.
    let mut to_delete: Vec<OffsetNumber> = Vec::with_capacity(max as usize);
    let mut replace_dead = false;
    let mut i = current.offnum;
    while i != InvalidOffsetNumber {
        let it_off = {
            let pr = PageRef::new(&cur_page)?;
            let iid = PageGetItemId(&pr, i)?;
            iid.lp_off() as usize
        };
        let it = &cur_page[it_off..];
        let st = lt_tupstate(it);
        if st == SPGIST_LIVE {
            debug_assert!(SpGistPageIsLeaf(&cur_page));
            to_delete.push(i);
            size += lt_size(it) + SIZEOF_ITEM_ID_DATA;
        } else if st == SPGIST_DEAD {
            debug_assert_eq!(lt_get_next_offset(it), InvalidOffsetNumber);
            to_delete.push(i);
            replace_dead = true;
        } else {
            return Err(elog_error("unexpected SPGiST tuple state".into()));
        }
        i = lt_get_next_offset(it);
    }
    let n_delete = to_delete.len();

    // Find a leaf page that will hold them.
    let mut new_page_init = false;
    let flags = GBUF_LEAF | if is_nulls { GBUF_NULLS } else { 0 };
    let nbuf = SpGistGetBuffer(mcx, index, flags, size as i32, &mut new_page_init)?;
    let nblkno = bufmgr::buffer_get_block_number::call(nbuf);
    debug_assert_ne!(nblkno, current.blkno);

    // Build the WAL leaf-image payload + the inserted-offset list as we go.
    let mut leafdata: Vec<u8> = Vec::with_capacity(size);
    let mut to_insert: Vec<OffsetNumber> = Vec::with_capacity(n_delete + 1);
    let mut r: OffsetNumber = InvalidOffsetNumber;
    // A single `startOffset` hint threaded through every SpGistPageAddNewItem on
    // the new page, exactly as the C does.
    let mut start_offset = InvalidOffsetNumber;

    miscinit::start_crit_section::call();

    if !replace_dead {
        for &doff in &to_delete {
            // Re-get `it`, chain it onto `r`, add to the new page.
            let it_off = {
                let pr = PageRef::new(&cur_page)?;
                let iid = PageGetItemId(&pr, doff)?;
                iid.lp_off() as usize
            };
            let it_size_bytes = lt_size(&cur_page[it_off..]);
            let mut it_img: Vec<u8> = cur_page[it_off..it_off + it_size_bytes].to_vec();
            lt_set_next_offset(&mut it_img, r);
            bufmgr::with_buffer_page::call(nbuf, &mut |pg: &mut [u8]| {
                r = SpGistPageAddNewItem(
                    state,
                    pg,
                    &it_img,
                    it_size_bytes as Size,
                    Some(&mut start_offset),
                    false,
                )?;
                Ok(())
            })?;
            to_insert.push(r);
            leafdata.extend_from_slice(&it_img);
        }
    }

    // Add the new tuple.
    {
        lt_set_next_offset(new_leaf_tuple, r);
        bufmgr::with_buffer_page::call(nbuf, &mut |pg: &mut [u8]| {
            r = SpGistPageAddNewItem(
                state,
                pg,
                new_leaf_tuple,
                new_leaf_size as Size,
                Some(&mut start_offset),
                false,
            )?;
            Ok(())
        })?;
        to_insert.push(r);
        leafdata.extend_from_slice(&new_leaf_tuple[..new_leaf_size]);
    }

    // Now delete the old tuples, leaving a redirect (or placeholder in build).
    let firststate = if state.isBuild {
        SPGIST_PLACEHOLDER
    } else {
        SPGIST_REDIRECT
    };
    bufmgr::with_buffer_page::call(current.buffer, &mut |pg: &mut [u8]| {
        spgPageIndexMultiDelete(
            state,
            pg,
            &to_delete,
            firststate,
            SPGIST_PLACEHOLDER,
            nblkno,
            r,
        )
    })?;

    // Update parent downlink.
    saveNodeLink(parent, nblkno, r)?;

    bufmgr::mark_buffer_dirty::call(current.buffer);
    bufmgr::mark_buffer_dirty::call(nbuf);

    if relation_needs_wal(index) && !state.isBuild {
        let xlrec = spgxlogMoveLeafs {
            nMoves: n_delete as u16,
            newPage: new_page_init,
            replaceDead: replace_dead,
            storesNulls: is_nulls,
            offnumParent: parent.offnum,
            nodeI: parent.node as u16,
        };
        let state_src = store_state(state);

        xloginsert::xlog_begin_insert::call()?;
        xloginsert::xlog_register_data::call(&xlrec.to_bytes(&state_src))?;
        xloginsert::xlog_register_data::call(&offsets_to_bytes(&to_delete))?;
        xloginsert::xlog_register_data::call(&offsets_to_bytes(&to_insert))?;
        xloginsert::xlog_register_data::call(&leafdata)?;

        xloginsert::xlog_register_buffer::call(0, current.buffer, REGBUF_STANDARD)?;
        let mut nflags = REGBUF_STANDARD;
        if new_page_init {
            nflags |= REGBUF_WILL_INIT;
        }
        xloginsert::xlog_register_buffer::call(1, nbuf, nflags)?;
        xloginsert::xlog_register_buffer::call(2, parent.buffer, REGBUF_STANDARD)?;

        let recptr = xloginsert::xlog_insert_record::call(RM_SPGIST_ID, XLOG_SPGIST_MOVE_LEAFS)?;
        bufmgr::page_set_lsn::call(current.buffer, recptr)?;
        bufmgr::page_set_lsn::call(nbuf, recptr)?;
        bufmgr::page_set_lsn::call(parent.buffer, recptr)?;
    }

    miscinit::end_crit_section::call();

    // The new leaf page (nbuf) is fully written and released here; `current`
    // keeps pointing at the old page (the caller breaks out of the loop). The
    // `current` parameter is unused for output, mirroring the C.
    let _ = current;

    SpGistSetLastUsedPage(mcx, index, nbuf)?;
    bufmgr::unlock_release_buffer::call(nbuf);

    Ok(())
}

/// Serialize a slice of `OffsetNumber` (uint16) for `XLogRegisterData`.
fn offsets_to_bytes(offs: &[OffsetNumber]) -> Vec<u8> {
    let mut v = Vec::with_capacity(offs.len() * 2);
    for &o in offs {
        v.extend_from_slice(&o.to_ne_bytes());
    }
    v
}

// The remaining big actions (doPickSplit / spgAddNodeAction / spgSplitNodeAction
// / spgMatchNodeAction / checkAllTheSame) and the spgdoinsert driver follow.
include!("spgdoinsert_actions.rs");
