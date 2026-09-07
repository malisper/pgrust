//! spgdoinsert.c: the insert state machine (choose/picksplit protocol,
//! moveLeafs, addNode, splitTuple).

use ::bufmgr_seams::{self as bufmgr};
use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::nbtree::itup::ItupBuf;
use ::types_core::{
    BlockNumber, Buffer, InvalidBlockNumber, OffsetNumber, BLCKSZ,
};
use ::types_core::fmgr::INDEX_MAX_KEYS;
use ::types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use ::types_rel::Relation;
use ::types_spgist::state::{spgChooseIn, spgChooseOut, spgPickSplitIn, spgPickSplitOut, SpGistState};
use ::types_spgist::xlog::*;
use ::types_spgist::*;
use ::types_storage::bufpage::PageMut;
use ::types_tuple::itemptr::{ItemPointerData, ItemPointerIsValid};
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};
use init_small::globals::{EndCriticalSection, StartCriticalSection};

use crate::utils::*;
pub use ::types_spgist::{spgPageIndexMultiDelete, spgUpdateNodeLink};

const K: usize = INDEX_MAX_KEYS as usize;
pub const RM_SPGIST_ID: u8 = ::types_core::RmgrIds::RM_SPGIST_ID as u8;

#[derive(Clone, Copy)]
pub(crate) struct SPPageDesc {
    pub(crate) blkno: BlockNumber,
    pub(crate) buffer: Buffer,
    pub(crate) offnum: OffsetNumber,
    pub(crate) node: i32,
}

#[inline]
fn page(desc: &SPPageDesc) -> PageMut<'static> {
    buf_page_mut(desc.buffer)
}

fn saveNodeLink(
    parent: &SPPageDesc,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) -> PgResult<()> {
    let mut pm = page(parent);
    let inner = item_slice_mut(&mut pm, parent.offnum);
    spgUpdateNodeLink(inner, parent.node, blkno, offnum)?;
    bufmgr::mark_buffer_dirty::call(parent.buffer)?;
    Ok(())
}

pub(crate) fn addLeafTuple(
    index: &Relation<'_>,
    state: &mut SpGistState<'_>,
    leaf_tuple: &mut [u8],
    current: &mut SPPageDesc,
    parent: &SPPageDesc,
    is_nulls: bool,
    is_new: bool,
) -> PgResult<()> {
    let mut xlrec = spgxlogAddLeaf {
        newPage: is_new,
        storesNulls: is_nulls,
        offnumLeaf: InvalidOffsetNumber,
        offnumHeadLeaf: InvalidOffsetNumber,
        offnumParent: InvalidOffsetNumber,
        nodeI: 0,
    };

    // spgdoinsert.c:217 START_CRIT_SECTION: an error from here to the WAL
    // record leaves a torn, unlogged page — C promotes it to PANIC.
    StartCriticalSection();

    if current.offnum == InvalidOffsetNumber || SpGistBlockIsRoot(current.blkno) {
        leaf_set_next_offset(leaf_tuple, InvalidOffsetNumber);
        let mut pm = page(current);
        current.offnum = SpGistPageAddNewItem(&mut pm, leaf_tuple, None, false)?;
        xlrec.offnumLeaf = current.offnum;

        if parent.buffer != InvalidBuffer {
            xlrec.offnumParent = parent.offnum;
            xlrec.nodeI = parent.node as u16;
            saveNodeLink(parent, current.blkno, current.offnum)?;
        }
    } else {
        let mut pm = page(current);
        let head_state = tuple_state(item_slice(&pm.as_ref(), current.offnum));
        if head_state == SPGIST_LIVE {
            let head_next = leaf_next_offset(item_slice(&pm.as_ref(), current.offnum));
            leaf_set_next_offset(leaf_tuple, head_next);
            let offnum = SpGistPageAddNewItem(&mut pm, leaf_tuple, None, false)?;
            // re-get head: it may have moved on the page
            let head = item_slice_mut(&mut pm, current.offnum);
            leaf_set_next_offset(head, offnum);
            xlrec.offnumLeaf = offnum;
            xlrec.offnumHeadLeaf = current.offnum;
        } else if head_state == SPGIST_DEAD {
            leaf_set_next_offset(leaf_tuple, InvalidOffsetNumber);
            pm.index_tuple_delete(current.offnum);
            if pm.add_item(leaf_tuple, current.offnum, 0) != Some(current.offnum) {
                return Err(add_item_failed(leaf_tuple.len()));
            }
            // WAL replay distinguishes this case by equal offnums
            xlrec.offnumLeaf = current.offnum;
            xlrec.offnumHeadLeaf = current.offnum;
        } else {
            return Err(tuple_state_error(head_state));
        }
    }

    bufmgr::mark_buffer_dirty::call(current.buffer)?;

    if relation_needs_wal(index) && !state.isBuild {
        let xl = xlrec.encode();
        let mut flags = REGBUF_STANDARD;
        if xlrec.newPage {
            flags |= REGBUF_WILL_INIT;
        }
        let b0 = XLogRegBuf {
            block_id: 0,
            buffer: current.buffer,
            flags,
            bufdata: &[],
        };
        let b1 = XLogRegBuf {
            block_id: 1,
            buffer: parent.buffer,
            flags: REGBUF_STANDARD,
            bufdata: &[],
        };
        let bufs: &[XLogRegBuf<'_>] = if xlrec.offnumParent != InvalidOffsetNumber {
            &[b0, b1]
        } else {
            &[b0]
        };
        let recptr = xloginsert_seams::xlog_insert_record::call(
            RM_SPGIST_ID,
            XLOG_SPGIST_ADD_LEAF,
            0,
            &[&xl, leaf_tuple],
            bufs,
        )?;
        page(current).set_lsn(recptr);
        if xlrec.offnumParent != InvalidOffsetNumber {
            page(parent).set_lsn(recptr);
        }
    }

    // spgdoinsert.c:319
    EndCriticalSection();
    Ok(())
}

fn checkSplitConditions(
    index: &Relation<'_>,
    current: &SPPageDesc,
    n_to_split: &mut i32,
) -> PgResult<usize> {
    if SpGistBlockIsRoot(current.blkno) {
        *n_to_split = BLCKSZ as i32;
        return Ok(BLCKSZ);
    }

    let pm = page(current);
    let pr = pm.as_ref();
    let mut guard = LeafChainGuard::new(pr.max_offset_number());
    let mut n = 0;
    let mut total_size = 0usize;
    let mut i = current.offnum;
    while i != InvalidOffsetNumber {
        crate::check_for_interrupts()?;
        guard.visit(i, index, current.blkno)?;
        let it = item_slice(&pr, i);
        let st = tuple_state(it);
        if st == SPGIST_LIVE {
            n += 1;
            total_size += leaf_size(it) + SIZEOF_ITEM_ID_DATA;
        } else if st == SPGIST_DEAD {
            debug_assert!(i == current.offnum);
        } else {
            return Err(tuple_state_error(st));
        }
        i = leaf_next_offset(it);
    }
    *n_to_split = n;
    Ok(total_size)
}

fn moveLeafs<'m>(
    mcx: Mcx<'m>,
    index: &Relation<'_>,
    state: &mut SpGistState<'_>,
    current: &SPPageDesc,
    parent: &SPPageDesc,
    new_leaf_tuple: &mut [u8],
    is_nulls: bool,
) -> PgResult<()> {
    debug_assert!(parent.buffer != InvalidBuffer);
    debug_assert!(parent.buffer != current.buffer);

    let mut xlrec = spgxlogMoveLeafs::default();

    let mut to_delete: PgVec<'m, OffsetNumber> = PgVec::new_in(mcx);
    let mut to_insert: PgVec<'m, OffsetNumber> = PgVec::new_in(mcx);
    let mut size = new_leaf_tuple.len() + SIZEOF_ITEM_ID_DATA;
    let mut replace_dead = false;

    {
        let pm = page(current);
        let pr = pm.as_ref();
        let mut guard = LeafChainGuard::new(pr.max_offset_number());
        let mut i = current.offnum;
        while i != InvalidOffsetNumber {
            crate::check_for_interrupts()?;
            guard.visit(i, index, current.blkno)?;
            let it = item_slice(&pr, i);
            let st = tuple_state(it);
            if st == SPGIST_LIVE {
                to_delete.push(i);
                size += leaf_size(it) + SIZEOF_ITEM_ID_DATA;
            } else if st == SPGIST_DEAD {
                debug_assert!(i == current.offnum);
                to_delete.push(i);
                replace_dead = true;
            } else {
                return Err(tuple_state_error(st));
            }
            i = leaf_next_offset(it);
        }
    }
    let n_delete = to_delete.len();

    let mut new_page = false;
    let nbuf = SpGistGetBuffer(
        index,
        GBUF_LEAF | if is_nulls { GBUF_NULLS } else { 0 },
        size as i32,
        &mut new_page,
    )?;
    xlrec.newPage = new_page;
    let nblkno = bufmgr::buffer_get_block_number::call(nbuf);
    debug_assert!(nblkno != current.blkno);

    let mut leafdata: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, size)?;

    // spgdoinsert.c:460 START_CRIT_SECTION
    StartCriticalSection();

    let mut r = InvalidOffsetNumber;
    let mut start_offset = InvalidOffsetNumber;

    if !replace_dead {
        for i in 0..n_delete {
            // chain order gets reversed; the source tuple is about to die,
            // so updating its link in place is fine (C does the same)
            let mut cpm = page(current);
            let it = item_slice_mut(&mut cpm, to_delete[i]);
            debug_assert!(tuple_state(it) == SPGIST_LIVE);
            leaf_set_next_offset(it, r);
            let img: PgVec<'m, u8> = {
                let mut v = ::mcx::vec_with_capacity_in(mcx, it.len())?;
                v.extend_from_slice(it);
                v
            };
            let mut npm = buf_page_mut(nbuf);
            r = SpGistPageAddNewItem(&mut npm, &img, Some(&mut start_offset), false)?;
            to_insert.push(r);
            leafdata.extend_from_slice(&img);
        }
    }

    leaf_set_next_offset(new_leaf_tuple, r);
    {
        let mut npm = buf_page_mut(nbuf);
        r = SpGistPageAddNewItem(&mut npm, new_leaf_tuple, Some(&mut start_offset), false)?;
    }
    to_insert.push(r);
    leafdata.extend_from_slice(new_leaf_tuple);
    let n_insert = to_insert.len();

    {
        let mut cpm = page(current);
        spgPageIndexMultiDelete(
            state.redirectXid,
            &mut cpm,
            &to_delete,
            if state.isBuild { SPGIST_PLACEHOLDER } else { SPGIST_REDIRECT },
            SPGIST_PLACEHOLDER,
            nblkno,
            r,
        );
    }

    saveNodeLink(parent, nblkno, r)?;

    bufmgr::mark_buffer_dirty::call(current.buffer)?;
    bufmgr::mark_buffer_dirty::call(nbuf)?;

    if relation_needs_wal(index) && !state.isBuild {
        xlrec.stateSrc = spgxlogState {
            redirectXid: state.redirectXid,
            isBuild: state.isBuild,
        };
        xlrec.nMoves = n_delete as u16;
        xlrec.replaceDead = replace_dead;
        xlrec.storesNulls = is_nulls;
        xlrec.offnumParent = parent.offnum;
        xlrec.nodeI = parent.node as u16;

        let xl = xlrec.encode();
        let del_bytes = offnum_bytes(&to_delete);
        let ins_bytes = offnum_bytes(&to_insert);
        let _ = n_insert;

        let recptr = xloginsert_seams::xlog_insert_record::call(
            RM_SPGIST_ID,
            XLOG_SPGIST_MOVE_LEAFS,
            0,
            &[&xl, del_bytes, ins_bytes, &leafdata],
            &[
                XLogRegBuf {
                    block_id: 0,
                    buffer: current.buffer,
                    flags: REGBUF_STANDARD,
                    bufdata: &[],
                },
                XLogRegBuf {
                    block_id: 1,
                    buffer: nbuf,
                    flags: REGBUF_STANDARD
                        | if xlrec.newPage { REGBUF_WILL_INIT } else { 0 },
                    bufdata: &[],
                },
                XLogRegBuf {
                    block_id: 2,
                    buffer: parent.buffer,
                    flags: REGBUF_STANDARD,
                    bufdata: &[],
                },
            ],
        )?;

        page(current).set_lsn(recptr);
        buf_page_mut(nbuf).set_lsn(recptr);
        page(parent).set_lsn(recptr);
    }

    // spgdoinsert.c:553
    EndCriticalSection();

    SpGistSetLastUsedPage(index, nbuf)?;
    unlock_release(nbuf)?;
    Ok(())
}

#[inline]
fn offnum_bytes(v: &[OffsetNumber]) -> &[u8] {
    // SAFETY: OffsetNumber (u16) reinterpreted as ne bytes.
    unsafe { core::slice::from_raw_parts(v.as_ptr().cast::<u8>(), v.len() * 2) }
}

fn setRedirectionTuple(
    current: &SPPageDesc,
    position: OffsetNumber,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) {
    let mut pm = page(current);
    let dt = item_slice_mut(&mut pm, position);
    debug_assert!(tuple_state(dt) == SPGIST_REDIRECT);
    write_item_pointer(&mut dt[6..12], &ItemPointerData::new(blkno, offnum));
}

// Owned picksplit output (the opclass out-arrays copied out of the fmgr call).
struct PickSplitResult<'m> {
    has_prefix: bool,
    prefix_datum: Datum,
    n_nodes: usize,
    node_labels: Option<PgVec<'m, Datum>>,
    map_tuples_to_nodes: PgVec<'m, i32>,
    leaf_tuple_datums: PgVec<'m, Datum>,
}

fn checkAllTheSame(
    n_tuples: usize,
    out: &mut PickSplitResult<'_>,
    too_big: bool,
    include_new: &mut bool,
) -> PgResult<bool> {
    *include_new = true;
    if n_tuples <= 1 {
        return Ok(false);
    }
    let limit = if too_big { n_tuples - 1 } else { n_tuples };

    let the_node = out.map_tuples_to_nodes[0];
    for i in 1..limit {
        if out.map_tuples_to_nodes[i] != the_node {
            return Ok(false);
        }
    }

    if too_big && out.map_tuples_to_nodes[n_tuples - 1] != the_node {
        *include_new = false;
    }

    out.n_nodes = 8;
    for i in 0..n_tuples {
        out.map_tuples_to_nodes[i] = (i % out.n_nodes) as i32;
    }

    if let Some(labels) = out.node_labels.as_mut() {
        let the_label = labels[the_node as usize];
        labels.clear();
        labels.resize(out.n_nodes, the_label);
    }

    Ok(true)
}

#[allow(clippy::too_many_arguments)]
fn doPickSplit<'m>(
    mcx: Mcx<'m>,
    index: &Relation<'_>,
    state: &mut SpGistState<'_>,
    current: &mut SPPageDesc,
    parent: &mut SPPageDesc,
    new_leaf_tuple: &[u8],
    level: i32,
    is_nulls: bool,
    is_new: bool,
) -> PgResult<bool> {
    let mut inserted_new = false;
    let mut xlrec = spgxlogPickSplit::default();

    let max = page(current).as_ref().max_offset_number();
    let n = max as usize + 1;

    let mut in_datums: PgVec<'m, Datum> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut to_delete: PgVec<'m, OffsetNumber> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut to_insert: PgVec<'m, OffsetNumber> = ::mcx::vec_with_capacity_in(mcx, n)?;
    // borrowed old leaf images: (page item slices stay valid under our locks)
    let mut old_leaf_offs: PgVec<'m, OffsetNumber> = ::mcx::vec_with_capacity_in(mcx, n)?;
    // droppy elements (ItupBuf wraps PgVec): std Vec, not the no-drop arena
    let mut new_leafs: Vec<ItupBuf<'m>> = Vec::with_capacity(n);
    let mut leaf_page_select: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, n)?;

    xlrec.stateSrc = spgxlogState {
        redirectXid: state.redirectXid,
        isBuild: state.isBuild,
    };

    let mut space_to_delete = 0usize;
    {
        let pm = page(current);
        let pr = pm.as_ref();
        if SpGistBlockIsRoot(current.blkno) {
            for i in FirstOffsetNumber..=max {
                let it = item_slice(&pr, i);
                let st = tuple_state(it);
                if st == SPGIST_LIVE {
                    in_datums.push(if is_nulls { Datum::null() } else { leaf_datum(it, state)? });
                    old_leaf_offs.push(i);
                    to_delete.push(i);
                    space_to_delete += leaf_size(it) + SIZEOF_ITEM_ID_DATA;
                } else {
                    return Err(tuple_state_error(st));
                }
            }
        } else {
            let mut guard = LeafChainGuard::new(max);
            let mut i = current.offnum;
            while i != InvalidOffsetNumber {
                crate::check_for_interrupts()?;
                guard.visit(i, index, current.blkno)?;
                let it = item_slice(&pr, i);
                let st = tuple_state(it);
                if st == SPGIST_LIVE {
                    in_datums.push(if is_nulls { Datum::null() } else { leaf_datum(it, state)? });
                    old_leaf_offs.push(i);
                    to_delete.push(i);
                    debug_assert!(leaf_size(it) >= SGDTSIZE);
                    space_to_delete += leaf_size(it) - SGDTSIZE;
                } else if st == SPGIST_DEAD {
                    debug_assert!(i == current.offnum);
                    to_delete.push(i);
                } else {
                    return Err(tuple_state_error(st));
                }
                i = leaf_next_offset(it);
            }
        }
    }

    // the incoming tuple is always part of the picksplit input
    in_datums.push(if is_nulls { Datum::null() } else { leaf_datum(new_leaf_tuple, state)? });
    let n_tuples = in_datums.len();
    let n_to_delete = to_delete.len();

    let mut out = PickSplitResult {
        has_prefix: false,
        prefix_datum: Datum::null(),
        n_nodes: 0,
        node_labels: None,
        map_tuples_to_nodes: PgVec::new_in(mcx),
        leaf_tuple_datums: PgVec::new_in(mcx),
    };

    let leaf_desc = state.leafTupDesc.clone();
    let natts = leaf_desc.natts as usize;
    let mut leaf_datums = [Datum::null(); K];
    let mut leaf_isnulls = [false; K];
    let mut total_leaf_sizes = 0usize;

    if !is_nulls {
        let psin = spgPickSplitIn {
            nTuples: n_tuples as i32,
            datums: in_datums.as_ptr(),
            level,
        };
        let mut psout = spgPickSplitOut::default();
        state.call_picksplit(mcx, &psin, &mut psout)?;

        out.has_prefix = psout.hasPrefix;
        out.prefix_datum = psout.prefixDatum;
        out.n_nodes = psout.nNodes as usize;
        if !psout.nodeLabels.is_null() {
            let mut v: PgVec<'m, Datum> = ::mcx::vec_with_capacity_in(mcx, out.n_nodes)?;
            // SAFETY: opclass contract — nNodes labels in the armed mcx.
            v.extend_from_slice(unsafe {
                core::slice::from_raw_parts(psout.nodeLabels, out.n_nodes)
            });
            out.node_labels = Some(v);
        }
        // SAFETY: opclass contract — nTuples entries each.
        unsafe {
            out.map_tuples_to_nodes
                .extend_from_slice(core::slice::from_raw_parts(
                    psout.mapTuplesToNodes,
                    n_tuples,
                ));
            out.leaf_tuple_datums
                .extend_from_slice(core::slice::from_raw_parts(
                    psout.leafTupleDatums,
                    n_tuples,
                ));
        }

        for i in 0..n_tuples {
            if natts > 1 {
                let pm = page(current);
                let pr = pm.as_ref();
                let src = if i + 1 == n_tuples {
                    new_leaf_tuple
                } else {
                    item_slice(&pr, old_leaf_offs[i])
                };
                spgDeformLeafTuple(src, &leaf_desc, &mut leaf_datums, &mut leaf_isnulls, is_nulls);
            }
            leaf_datums[spgKeyColumn] = out.leaf_tuple_datums[i];
            leaf_isnulls[spgKeyColumn] = false;

            let heap_ptr = source_heap_ptr(current, &old_leaf_offs, new_leaf_tuple, i, n_tuples);
            let lt = spgFormLeafTuple(mcx, state, &heap_ptr, &leaf_datums[..natts.max(1)], &leaf_isnulls[..natts.max(1)])?;
            total_leaf_sizes += lt.size() + SIZEOF_ITEM_ID_DATA;
            new_leafs.push(lt);
        }
    } else {
        out.has_prefix = false;
        out.n_nodes = 1;
        out.node_labels = None;
        out.map_tuples_to_nodes.resize(n_tuples, 0);

        for i in 0..n_tuples {
            if natts > 1 {
                let pm = page(current);
                let pr = pm.as_ref();
                let src = if i + 1 == n_tuples {
                    new_leaf_tuple
                } else {
                    item_slice(&pr, old_leaf_offs[i])
                };
                spgDeformLeafTuple(src, &leaf_desc, &mut leaf_datums, &mut leaf_isnulls, is_nulls);
            }
            leaf_datums[spgKeyColumn] = Datum::null();
            leaf_isnulls[spgKeyColumn] = true;

            let heap_ptr = source_heap_ptr(current, &old_leaf_offs, new_leaf_tuple, i, n_tuples);
            let lt = spgFormLeafTuple(mcx, state, &heap_ptr, &leaf_datums[..natts.max(1)], &leaf_isnulls[..natts.max(1)])?;
            total_leaf_sizes += lt.size() + SIZEOF_ITEM_ID_DATA;
            new_leafs.push(lt);
        }
    }

    let mut include_new = true;
    let all_the_same = checkAllTheSame(
        n_tuples,
        &mut out,
        total_leaf_sizes > SPGIST_PAGE_CAPACITY,
        &mut include_new,
    )?;

    let max_to_include = if include_new {
        n_tuples
    } else {
        total_leaf_sizes -= new_leafs[n_tuples - 1].size() + SIZEOF_ITEM_ID_DATA;
        n_tuples - 1
    };

    // form nodes + inner tuple
    let mut node_imgs: Vec<ItupBuf<'m>> = Vec::with_capacity(out.n_nodes);
    for i in 0..out.n_nodes {
        let (label, labelisnull) = match out.node_labels.as_ref() {
            Some(l) => (l[i], false),
            None => (Datum::null(), true),
        };
        node_imgs.push(spgFormNodeTuple(mcx, state, label, labelisnull)?);
    }
    let node_slices: PgVec<'m, &[u8]> = {
        let mut v = ::mcx::vec_with_capacity_in(mcx, out.n_nodes)?;
        for nimg in node_imgs.iter() {
            v.push(nimg.as_slice());
        }
        v
    };
    let mut inner_tuple = spgFormInnerTuple(
        mcx,
        state,
        out.has_prefix,
        out.prefix_datum,
        &node_slices,
    )?;
    set_all_the_same(inner_tuple.as_mut_slice(), all_the_same);
    let inner_size = inner_tuple_size(inner_tuple.as_slice());

    // per-node space accounting
    let mut leaf_sizes: PgVec<'m, i32> = PgVec::new_in(mcx);
    leaf_sizes.resize(out.n_nodes, 0);
    for i in 0..max_to_include {
        let nn = picksplit_node_index(out.map_tuples_to_nodes[i], out.n_nodes)?;
        leaf_sizes[nn] += (new_leafs[i].size() + SIZEOF_ITEM_ID_DATA) as i32;
    }

    // choose the inner-tuple page
    xlrec.initInner = false;
    let new_inner_buffer: Buffer;
    if parent.buffer != InvalidBuffer
        && !SpGistBlockIsRoot(parent.blkno)
        && SpGistPageGetFreeSpace(&page(parent).as_ref(), 1)
            >= inner_size + SIZEOF_ITEM_ID_DATA
    {
        new_inner_buffer = parent.buffer;
    } else if parent.buffer != InvalidBuffer {
        let mut init_inner = false;
        new_inner_buffer = SpGistGetBuffer(
            index,
            GBUF_INNER_PARITY(parent.blkno + 1) | if is_nulls { GBUF_NULLS } else { 0 },
            (inner_size + SIZEOF_ITEM_ID_DATA) as i32,
            &mut init_inner,
        )?;
        xlrec.initInner = init_inner;
    } else {
        new_inner_buffer = InvalidBuffer;
    }

    let current_free_space = if !SpGistBlockIsRoot(current.blkno) {
        page(current).as_ref().exact_free_space() + space_to_delete
    } else {
        0
    };

    xlrec.initDest = false;
    let mut new_leaf_buffer = InvalidBuffer;
    let mut n_to_insert;

    if total_leaf_sizes <= current_free_space {
        n_to_insert = n_tuples - 1;
        if include_new {
            n_to_insert += 1;
            inserted_new = true;
        }
        // BUG-GUARD: nToInsert semantics — C counts live old tuples
        // (n_tuples-1) plus optionally the new one
        leaf_page_select.resize(n_to_insert, 0);
    } else if n_tuples == 1 && total_leaf_sizes > SPGIST_PAGE_CAPACITY {
        new_leaf_buffer = InvalidBuffer;
        debug_assert!(include_new);
        n_to_insert = 0;
    } else {
        let mut init_dest = false;
        new_leaf_buffer = SpGistGetBuffer(
            index,
            GBUF_LEAF | if is_nulls { GBUF_NULLS } else { 0 },
            total_leaf_sizes.min(SPGIST_PAGE_CAPACITY) as i32,
            &mut init_dest,
        )?;
        xlrec.initDest = init_dest;

        let mut node_page_select: PgVec<'m, u8> = PgVec::new_in(mcx);
        node_page_select.resize(out.n_nodes, 0);

        let mut curspace = current_free_space as i64;
        let mut newspace = buf_page_mut(new_leaf_buffer).as_ref().exact_free_space() as i64;
        for i in 0..out.n_nodes {
            if (leaf_sizes[i] as i64) <= curspace {
                node_page_select[i] = 0;
                curspace -= leaf_sizes[i] as i64;
            } else {
                node_page_select[i] = 1;
                newspace -= leaf_sizes[i] as i64;
            }
        }
        n_to_insert = n_tuples - 1;
        if curspace >= 0 && newspace >= 0 {
            if include_new {
                n_to_insert += 1;
                inserted_new = true;
            }
        } else if include_new {
            let node_of_new = out.map_tuples_to_nodes[n_tuples - 1] as usize;
            leaf_sizes[node_of_new] -=
                (new_leafs[n_tuples - 1].size() + SIZEOF_ITEM_ID_DATA) as i32;
            divide_leaf_groups(
                &leaf_sizes,
                current_free_space as i64,
                buf_page_mut(new_leaf_buffer).as_ref().exact_free_space() as i64,
                &mut node_page_select,
            )?;
        } else {
            // C (spgdoinsert.c:1115-1118): we already excluded the new tuple.
            return Err(failed_to_divide_leaf_groups());
        }
        leaf_page_select.clear();
        for i in 0..n_to_insert {
            let nn = out.map_tuples_to_nodes[i] as usize;
            leaf_page_select.push(node_page_select[nn]);
        }
    }
    xlrec.nDelete = 0;
    xlrec.initSrc = is_new;
    xlrec.storesNulls = is_nulls;
    xlrec.isRootSplit = SpGistBlockIsRoot(current.blkno);

    let mut leafdata: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, total_leaf_sizes)?;

    // ---- begin page modifications ----
    // spgdoinsert.c:1136 START_CRIT_SECTION
    StartCriticalSection();

    if !SpGistBlockIsRoot(current.blkno) {
        let n_placeholder = page_opaque(&page(current).as_ref()).nPlaceholder as usize;
        if state.isBuild
            && n_to_delete + n_placeholder == page(current).as_ref().max_offset_number() as usize
        {
            SpGistInitBuffer(
                current.buffer,
                SPGIST_LEAF | if is_nulls { SPGIST_NULLS } else { 0 },
            );
            xlrec.initSrc = true;
        } else if is_new {
            debug_assert!(n_to_delete == 0);
        } else {
            xlrec.nDelete = n_to_delete as u16;
            let mut cpm = page(current);
            if !state.isBuild {
                spgPageIndexMultiDelete(
                    state.redirectXid,
                    &mut cpm,
                    &to_delete,
                    SPGIST_REDIRECT,
                    SPGIST_PLACEHOLDER,
                    SPGIST_METAPAGE_BLKNO,
                    FirstOffsetNumber,
                );
            } else {
                spgPageIndexMultiDelete(
                    state.redirectXid,
                    &mut cpm,
                    &to_delete,
                    SPGIST_PLACEHOLDER,
                    SPGIST_PLACEHOLDER,
                    InvalidBlockNumber,
                    InvalidOffsetNumber,
                );
            }
        }
    }
    let redirect_tuple_pos = if !SpGistBlockIsRoot(current.blkno)
        && !state.isBuild
        && !is_new
        && xlrec.nDelete > 0
    {
        to_delete[0]
    } else {
        InvalidOffsetNumber
    };

    // place leaf tuples, updating node downlinks in the inner-tuple image
    let mut start_offsets = [InvalidOffsetNumber; 2];
    let node_offs: PgVec<'m, usize> = {
        let mut v = ::mcx::vec_with_capacity_in(mcx, out.n_nodes)?;
        for (_, off) in inner_tuple_nodes(inner_tuple.as_slice()) {
            v.push(off);
        }
        v
    };

    for i in 0..n_to_insert {
        let sel = leaf_page_select[i];
        let leaf_buffer = if sel != 0 { new_leaf_buffer } else { current.buffer };
        let leaf_block = bufmgr::buffer_get_block_number::call(leaf_buffer);

        let nn = out.map_tuples_to_nodes[i] as usize;
        let inner_img = inner_tuple.as_mut_slice();
        let node = &mut inner_img[node_offs[nn]..];
        let tid = node_tuple_tid(node);
        {
            let it = new_leafs[i].as_mut_slice();
            if ItemPointerIsValid(&tid) {
                debug_assert!(
                    ::types_tuple::itemptr::ItemPointerGetBlockNumber(&tid) == leaf_block
                );
                leaf_set_next_offset(it, tid.ip_posid);
            } else {
                leaf_set_next_offset(it, InvalidOffsetNumber);
            }
        }

        let newoffset = {
            let mut lpm = buf_page_mut(leaf_buffer);
            SpGistPageAddNewItem(
                &mut lpm,
                new_leafs[i].as_slice(),
                Some(&mut start_offsets[sel as usize]),
                false,
            )?
        };
        to_insert.push(newoffset);

        let node = &mut inner_tuple.as_mut_slice()[node_offs[nn]..];
        node_tuple_set_tid(node, &ItemPointerData::new(leaf_block, newoffset));

        leafdata.extend_from_slice(new_leafs[i].as_slice());
    }

    if new_leaf_buffer != InvalidBuffer {
        bufmgr::mark_buffer_dirty::call(new_leaf_buffer)?;
    }

    let save_current = *current;

    if new_inner_buffer == parent.buffer && new_inner_buffer != InvalidBuffer {
        debug_assert!(current.buffer != parent.buffer);
        current.blkno = parent.blkno;
        current.buffer = parent.buffer;
        current.offnum = {
            let mut pm = page(current);
            SpGistPageAddNewItem(&mut pm, inner_tuple.as_slice(), None, false)?
        };
        xlrec.offnumInner = current.offnum;
        xlrec.innerIsParent = true;
        xlrec.offnumParent = parent.offnum;
        xlrec.nodeI = parent.node as u16;
        saveNodeLink(parent, current.blkno, current.offnum)?;

        if redirect_tuple_pos != InvalidOffsetNumber {
            setRedirectionTuple(&save_current, redirect_tuple_pos, current.blkno, current.offnum);
        }
        bufmgr::mark_buffer_dirty::call(save_current.buffer)?;
    } else if parent.buffer != InvalidBuffer {
        debug_assert!(new_inner_buffer != InvalidBuffer);
        current.buffer = new_inner_buffer;
        current.blkno = bufmgr::buffer_get_block_number::call(current.buffer);
        current.offnum = {
            let mut pm = page(current);
            SpGistPageAddNewItem(&mut pm, inner_tuple.as_slice(), None, false)?
        };
        xlrec.offnumInner = current.offnum;
        bufmgr::mark_buffer_dirty::call(current.buffer)?;

        xlrec.innerIsParent = parent.buffer == current.buffer;
        xlrec.offnumParent = parent.offnum;
        xlrec.nodeI = parent.node as u16;
        saveNodeLink(parent, current.blkno, current.offnum)?;

        if redirect_tuple_pos != InvalidOffsetNumber {
            setRedirectionTuple(&save_current, redirect_tuple_pos, current.blkno, current.offnum);
        }
        bufmgr::mark_buffer_dirty::call(save_current.buffer)?;
    } else {
        debug_assert!(SpGistBlockIsRoot(current.blkno));
        debug_assert!(redirect_tuple_pos == InvalidOffsetNumber);

        SpGistInitBuffer(current.buffer, if is_nulls { SPGIST_NULLS } else { 0 });
        xlrec.initInner = true;
        xlrec.innerIsParent = false;

        current.offnum = {
            let mut pm = page(current);
            pm.add_item(inner_tuple.as_slice(), InvalidOffsetNumber, 0)
                .unwrap_or(InvalidOffsetNumber)
        };
        if current.offnum != FirstOffsetNumber {
            return Err(add_item_failed(inner_size));
        }
        xlrec.offnumInner = current.offnum;
        xlrec.offnumParent = InvalidOffsetNumber;
        xlrec.nodeI = 0;

        bufmgr::mark_buffer_dirty::call(current.buffer)?;
    }
    let save_current_valid = save_current.buffer != current.buffer && save_current.buffer != InvalidBuffer && !(new_inner_buffer == InvalidBuffer);

    if relation_needs_wal(index) && !state.isBuild {
        xlrec.nInsert = n_to_insert as u16;
        let xl = xlrec.encode();
        let del_bytes = offnum_bytes(&to_delete[..xlrec.nDelete as usize]);
        let ins_bytes = offnum_bytes(&to_insert);
        let sel_bytes: &[u8] = &leaf_page_select[..n_to_insert];

        let mut bufs: Vec<XLogRegBuf<'_>> = Vec::with_capacity(4);
        if save_current_valid {
            let mut flags = REGBUF_STANDARD;
            if xlrec.initSrc {
                flags |= REGBUF_WILL_INIT;
            }
            bufs.push(XLogRegBuf {
                block_id: 0,
                buffer: save_current.buffer,
                flags,
                bufdata: &[],
            });
        }
        if new_leaf_buffer != InvalidBuffer {
            let mut flags = REGBUF_STANDARD;
            if xlrec.initDest {
                flags |= REGBUF_WILL_INIT;
            }
            bufs.push(XLogRegBuf {
                block_id: 1,
                buffer: new_leaf_buffer,
                flags,
                bufdata: &[],
            });
        }
        let mut flags = REGBUF_STANDARD;
        if xlrec.initInner {
            flags |= REGBUF_WILL_INIT;
        }
        bufs.push(XLogRegBuf {
            block_id: 2,
            buffer: current.buffer,
            flags,
            bufdata: &[],
        });
        if parent.buffer != InvalidBuffer && parent.buffer != current.buffer {
            bufs.push(XLogRegBuf {
                block_id: 3,
                buffer: parent.buffer,
                flags: REGBUF_STANDARD,
                bufdata: &[],
            });
        }

        let recptr = xloginsert_seams::xlog_insert_record::call(
            RM_SPGIST_ID,
            XLOG_SPGIST_PICKSPLIT,
            0,
            &[
                &xl,
                del_bytes,
                ins_bytes,
                sel_bytes,
                &inner_tuple.as_slice()[..inner_size],
                &leafdata,
            ],
            &bufs,
        )?;

        if new_leaf_buffer != InvalidBuffer {
            buf_page_mut(new_leaf_buffer).set_lsn(recptr);
        }
        if save_current_valid {
            buf_page_mut(save_current.buffer).set_lsn(recptr);
        }
        page(current).set_lsn(recptr);
        if parent.buffer != InvalidBuffer {
            page(parent).set_lsn(recptr);
        }
    }

    // spgdoinsert.c:1438
    EndCriticalSection();

    if new_leaf_buffer != InvalidBuffer {
        SpGistSetLastUsedPage(index, new_leaf_buffer)?;
        unlock_release(new_leaf_buffer)?;
    }
    if save_current_valid {
        SpGistSetLastUsedPage(index, save_current.buffer)?;
        unlock_release(save_current.buffer)?;
    }

    Ok(inserted_new)
}

fn source_heap_ptr(
    current: &SPPageDesc,
    old_leaf_offs: &[OffsetNumber],
    new_leaf_tuple: &[u8],
    i: usize,
    n_tuples: usize,
) -> ItemPointerData {
    if i + 1 == n_tuples {
        SpGistLeafTupleHeader::decode(new_leaf_tuple).heapPtr
    } else {
        let pm = page(current);
        let pr = pm.as_ref();
        SpGistLeafTupleHeader::decode(item_slice(&pr, old_leaf_offs[i])).heapPtr
    }
}

#[inline]
fn inner_tuple_size(inner: &[u8]) -> usize {
    SpGistInnerTupleHeader::decode(inner).size as usize
}

#[inline]
fn set_all_the_same(inner: &mut [u8], v: bool) {
    let mut hdr = SpGistInnerTupleHeader::decode(inner);
    hdr.allTheSame = v;
    hdr.encode(inner);
}

fn spgMatchNodeAction(
    index: &Relation<'_>,
    current: &mut SPPageDesc,
    parent: &mut SPPageDesc,
    nodeN: i32,
) -> PgResult<()> {
    if parent.buffer != InvalidBuffer && parent.buffer != current.buffer {
        SpGistSetLastUsedPage(index, parent.buffer)?;
        unlock_release(parent.buffer)?;
    }

    parent.blkno = current.blkno;
    parent.buffer = current.buffer;
    parent.offnum = current.offnum;
    parent.node = nodeN;

    let pm = page(current);
    let pr = pm.as_ref();
    let inner = item_slice(&pr, current.offnum);
    let tid = locate_node(inner, nodeN)?;

    if ItemPointerIsValid(&tid) {
        current.blkno = ::types_tuple::itemptr::ItemPointerGetBlockNumber(&tid);
        current.offnum = tid.ip_posid;
    } else {
        current.blkno = InvalidBlockNumber;
        current.offnum = InvalidOffsetNumber;
    }
    current.buffer = InvalidBuffer;
    Ok(())
}

pub(crate) fn addNode<'m>(
    mcx: Mcx<'m>,
    state: &mut SpGistState<'_>,
    inner: &[u8],
    label: Datum,
    offset: i32,
) -> PgResult<ItupBuf<'m>> {
    let hdr = SpGistInnerTupleHeader::decode(inner);
    let n = hdr.nNodes as i32;
    let offset = if offset < 0 {
        n
    } else if offset > n {
        // spgdoinsert.c:90 elog(ERROR): catchable XX000.
        return Err(Box::new(PgError::error(
            "invalid offset for adding node to SPGiST inner tuple",
        )));
    } else {
        offset
    };

    let new_node = spgFormNodeTuple(mcx, state, label, false)?;

    let mut nodes: PgVec<'m, &[u8]> = ::mcx::vec_with_capacity_in(mcx, n as usize + 1)?;
    nodes.resize(n as usize + 1, &[][..]);
    for (i, off) in inner_tuple_nodes(inner) {
        let node = &inner[off..off + node_tuple_size(&inner[off..])];
        if (i as i32) < offset {
            nodes[i] = node;
        } else {
            nodes[i + 1] = node;
        }
    }
    nodes[offset as usize] = new_node.as_slice();

    spgFormInnerTuple(
        mcx,
        state,
        hdr.prefixSize > 0,
        inner_prefix_datum(inner, state)?,
        &nodes,
    )
}

#[allow(clippy::too_many_arguments)]
fn spgAddNodeAction<'m>(
    mcx: Mcx<'m>,
    index: &Relation<'_>,
    state: &mut SpGistState<'_>,
    current: &mut SPPageDesc,
    parent: &SPPageDesc,
    nodeN: i32,
    node_label: Datum,
) -> PgResult<()> {
    debug_assert!(!SpGistPageStoresNulls(&page(current).as_ref()));

    let (new_inner_tuple, old_inner_size) = {
        let pm = page(current);
        let pr = pm.as_ref();
        let inner = item_slice(&pr, current.offnum);
        let old_size = inner_tuple_size(inner);
        (addNode(mcx, state, inner, node_label, nodeN)?, old_size)
    };
    let new_size = inner_tuple_size(new_inner_tuple.as_slice());

    let mut xlrec = spgxlogAddNode {
        offnum: current.offnum,
        offnumNew: InvalidOffsetNumber,
        newPage: false,
        parentBlk: -1,
        offnumParent: InvalidOffsetNumber,
        nodeI: 0,
        stateSrc: spgxlogState {
            redirectXid: state.redirectXid,
            isBuild: state.isBuild,
        },
    };

    if page(current).as_ref().exact_free_space() + old_inner_size >= new_size {
        // replace in place (freespace check in C is vs the size delta)
        // spgdoinsert.c:1546 START_CRIT_SECTION
        StartCriticalSection();
        {
            let mut pm = page(current);
            pm.index_tuple_delete(current.offnum);
            if pm.add_item(&new_inner_tuple.as_slice()[..new_size], current.offnum, 0)
                != Some(current.offnum)
            {
                return Err(add_item_failed(new_size));
            }
        }
        bufmgr::mark_buffer_dirty::call(current.buffer)?;

        if relation_needs_wal(index) && !state.isBuild {
            let xl = xlrec.encode();
            let recptr = xloginsert_seams::xlog_insert_record::call(
                RM_SPGIST_ID,
                XLOG_SPGIST_ADD_NODE,
                0,
                &[&xl, &new_inner_tuple.as_slice()[..new_size]],
                &[XLogRegBuf {
                    block_id: 0,
                    buffer: current.buffer,
                    flags: REGBUF_STANDARD,
                    bufdata: &[],
                }],
            )?;
            page(current).set_lsn(recptr);
        }

        // spgdoinsert.c:1572
        EndCriticalSection();
    } else {
        // move to another page and redirect
        if SpGistBlockIsRoot(current.blkno) {
            panic!("cannot enlarge root tuple any more");
        }
        debug_assert!(parent.buffer != InvalidBuffer);

        let save_current = *current;

        xlrec.offnumParent = parent.offnum;
        xlrec.nodeI = parent.node as u16;

        let mut new_page = false;
        current.buffer = SpGistGetBuffer(
            index,
            GBUF_INNER_PARITY(current.blkno),
            (new_size + SIZEOF_ITEM_ID_DATA) as i32,
            &mut new_page,
        )?;
        xlrec.newPage = new_page;
        current.blkno = bufmgr::buffer_get_block_number::call(current.buffer);

        if current.blkno == save_current.blkno {
            panic!("SPGiST new buffer shouldn't be same as old buffer");
        }

        xlrec.parentBlk = if parent.buffer == save_current.buffer {
            0
        } else if parent.buffer == current.buffer {
            1
        } else {
            2
        };

        // spgdoinsert.c:1629 START_CRIT_SECTION
        StartCriticalSection();

        current.offnum = {
            let mut pm = page(current);
            SpGistPageAddNewItem(&mut pm, &new_inner_tuple.as_slice()[..new_size], None, false)?
        };
        xlrec.offnumNew = current.offnum;
        bufmgr::mark_buffer_dirty::call(current.buffer)?;

        saveNodeLink(parent, current.blkno, current.offnum)?;

        let dt = if state.isBuild {
            spgFormDeadTuple(state.redirectXid, SPGIST_PLACEHOLDER, InvalidBlockNumber, InvalidOffsetNumber)
        } else {
            spgFormDeadTuple(state.redirectXid, SPGIST_REDIRECT, current.blkno, current.offnum)
        };

        {
            let mut spm = buf_page_mut(save_current.buffer);
            spm.index_tuple_delete(save_current.offnum);
            if spm.add_item(&dt, save_current.offnum, 0) != Some(save_current.offnum) {
                return Err(add_item_failed(SGDTSIZE));
            }
            if state.isBuild {
                page_opaque_update(&mut spm, |op| op.nPlaceholder += 1);
            } else {
                page_opaque_update(&mut spm, |op| op.nRedirection += 1);
            }
        }
        bufmgr::mark_buffer_dirty::call(save_current.buffer)?;

        if relation_needs_wal(index) && !state.isBuild {
            let xl = xlrec.encode();
            let mut bufs: Vec<XLogRegBuf<'_>> = Vec::with_capacity(4);
            bufs.push(XLogRegBuf {
                block_id: 0,
                buffer: save_current.buffer,
                flags: REGBUF_STANDARD,
                bufdata: &[],
            });
            let mut flags = REGBUF_STANDARD;
            if xlrec.newPage {
                flags |= REGBUF_WILL_INIT;
            }
            bufs.push(XLogRegBuf {
                block_id: 1,
                buffer: current.buffer,
                flags,
                bufdata: &[],
            });
            if xlrec.parentBlk == 2 {
                bufs.push(XLogRegBuf {
                    block_id: 2,
                    buffer: parent.buffer,
                    flags: REGBUF_STANDARD,
                    bufdata: &[],
                });
            }
            let recptr = xloginsert_seams::xlog_insert_record::call(
                RM_SPGIST_ID,
                XLOG_SPGIST_ADD_NODE,
                0,
                &[&xl, &new_inner_tuple.as_slice()[..new_size]],
                &bufs,
            )?;
            page(current).set_lsn(recptr);
            page(parent).set_lsn(recptr);
            buf_page_mut(save_current.buffer).set_lsn(recptr);
        }

        // spgdoinsert.c:1699
        EndCriticalSection();

        if save_current.buffer != current.buffer && save_current.buffer != parent.buffer {
            SpGistSetLastUsedPage(index, save_current.buffer)?;
            unlock_release(save_current.buffer)?;
        }
    }
    Ok(())
}

/// spgdoinsert.c:1768-1769: the prefix tuple of an inner-tuple split "must
/// fit in the space that innerTuple now occupies".
fn split_prefix_fits(prefix_size: usize, inner_size: usize) -> PgResult<()> {
    if prefix_size > inner_size {
        // elog(ERROR): a catchable XX000, not a panic.
        return Err(Box::new(PgError::error(
            "SPGiST inner-tuple split must not produce longer prefix",
        )));
    }
    Ok(())
}

/// spgdoinsert.c:2293-2295: "AddNode is not sensible if nodes don't have
/// labels".
fn add_node_requires_labels(in_choose: &spgChooseIn) -> PgResult<()> {
    if in_choose.nodeLabels.is_null() {
        // elog(ERROR): a catchable XX000, not a panic.
        return Err(Box::new(PgError::error(
            "cannot add a node to an inner tuple without node labels",
        )));
    }
    Ok(())
}

fn spgSplitNodeAction<'m>(
    mcx: Mcx<'m>,
    index: &Relation<'_>,
    state: &mut SpGistState<'_>,
    current: &mut SPPageDesc,
    out: &spgChooseOut,
) -> PgResult<()> {
    debug_assert!(!SpGistPageStoresNulls(&page(current).as_ref()));

    let spgChooseOut::SplitTuple {
        prefixHasPrefix,
        prefixPrefixDatum,
        prefixNNodes,
        prefixNodeLabels,
        childNodeN,
        postfixHasPrefix,
        postfixPrefixDatum,
    } = *out
    else {
        unreachable!("spgSplitNodeAction on a non-splitTuple result")
    };

    check_split_tuple_output(prefixNNodes, childNodeN)?;

    let (mut prefix_tuple, postfix_tuple, old_inner_size, old_all_the_same) = {
        let pm = page(current);
        let pr = pm.as_ref();
        let inner = item_slice(&pr, current.offnum);
        let inner_hdr = SpGistInnerTupleHeader::decode(inner);

        let mut prefix_nodes: Vec<ItupBuf<'m>> = Vec::with_capacity(prefixNNodes as usize);
        for i in 0..prefixNNodes as usize {
            let (label, labelisnull) = if prefixNodeLabels.is_null() {
                (Datum::null(), true)
            } else {
                // SAFETY: opclass contract — prefixNNodes labels.
                (unsafe { *prefixNodeLabels.add(i) }, false)
            };
            prefix_nodes.push(spgFormNodeTuple(mcx, state, label, labelisnull)?);
        }
        let prefix_slices: PgVec<'m, &[u8]> = {
            let mut v = ::mcx::vec_with_capacity_in(mcx, prefix_nodes.len())?;
            for nimg in prefix_nodes.iter() {
                v.push(nimg.as_slice());
            }
            v
        };
        let prefix_tuple =
            spgFormInnerTuple(mcx, state, prefixHasPrefix, prefixPrefixDatum, &prefix_slices)?;

        split_prefix_fits(inner_tuple_size(prefix_tuple.as_slice()), inner_hdr.size as usize)?;

        let old_nodes: PgVec<'m, &[u8]> = {
            let mut v = ::mcx::vec_with_capacity_in(mcx, inner_hdr.nNodes as usize)?;
            for (_, off) in inner_tuple_nodes(inner) {
                v.push(&inner[off..off + node_tuple_size(&inner[off..])]);
            }
            v
        };
        let mut postfix_tuple =
            spgFormInnerTuple(mcx, state, postfixHasPrefix, postfixPrefixDatum, &old_nodes)?;
        set_all_the_same(postfix_tuple.as_mut_slice(), inner_hdr.allTheSame);

        (
            prefix_tuple,
            postfix_tuple,
            inner_hdr.size as usize,
            inner_hdr.allTheSame,
        )
    };
    let _ = old_all_the_same;
    let prefix_size = inner_tuple_size(prefix_tuple.as_slice());
    let postfix_size = inner_tuple_size(postfix_tuple.as_slice());

    let mut xlrec = spgxlogSplitTuple::default();

    let mut new_buffer = InvalidBuffer;
    if SpGistBlockIsRoot(current.blkno)
        || SpGistPageGetFreeSpace(&page(current).as_ref(), 1) + old_inner_size
            < prefix_size + postfix_size + SIZEOF_ITEM_ID_DATA
    {
        let mut new_page = false;
        new_buffer = SpGistGetBuffer(
            index,
            GBUF_INNER_PARITY(current.blkno + 1),
            (postfix_size + SIZEOF_ITEM_ID_DATA) as i32,
            &mut new_page,
        )?;
        xlrec.newPage = new_page;
    }

    // spgdoinsert.c:1814 START_CRIT_SECTION
    StartCriticalSection();

    {
        let mut pm = page(current);
        pm.index_tuple_delete(current.offnum);
        match pm.add_item(&prefix_tuple.as_slice()[..prefix_size], current.offnum, 0) {
            Some(o) if o == current.offnum => {}
            _ => return Err(add_item_failed(prefix_size)),
        }
    }
    xlrec.offnumPrefix = current.offnum;

    let postfix_blkno;
    let postfix_offset;
    if new_buffer == InvalidBuffer {
        postfix_blkno = current.blkno;
        let mut pm = page(current);
        postfix_offset =
            SpGistPageAddNewItem(&mut pm, &postfix_tuple.as_slice()[..postfix_size], None, false)?;
        xlrec.postfixBlkSame = true;
    } else {
        postfix_blkno = bufmgr::buffer_get_block_number::call(new_buffer);
        let mut pm = buf_page_mut(new_buffer);
        postfix_offset =
            SpGistPageAddNewItem(&mut pm, &postfix_tuple.as_slice()[..postfix_size], None, false)?;
        bufmgr::mark_buffer_dirty::call(new_buffer)?;
        xlrec.postfixBlkSame = false;
    }
    xlrec.offnumPostfix = postfix_offset;

    // set the downlink in both the WAL image and the on-page copy
    spgUpdateNodeLink(prefix_tuple.as_mut_slice(), childNodeN, postfix_blkno, postfix_offset)?;
    {
        let mut pm = page(current);
        let on_page = item_slice_mut(&mut pm, current.offnum);
        spgUpdateNodeLink(on_page, childNodeN, postfix_blkno, postfix_offset)?;
    }

    bufmgr::mark_buffer_dirty::call(current.buffer)?;

    if relation_needs_wal(index) && !state.isBuild {
        let xl = xlrec.encode();
        let mut bufs: Vec<XLogRegBuf<'_>> = Vec::with_capacity(4);
        bufs.push(XLogRegBuf {
            block_id: 0,
            buffer: current.buffer,
            flags: REGBUF_STANDARD,
            bufdata: &[],
        });
        if new_buffer != InvalidBuffer {
            let mut flags = REGBUF_STANDARD;
            if xlrec.newPage {
                flags |= REGBUF_WILL_INIT;
            }
            bufs.push(XLogRegBuf {
                block_id: 1,
                buffer: new_buffer,
                flags,
                bufdata: &[],
            });
        }
        let recptr = xloginsert_seams::xlog_insert_record::call(
            RM_SPGIST_ID,
            XLOG_SPGIST_SPLIT_TUPLE,
            0,
            &[
                &xl,
                &prefix_tuple.as_slice()[..prefix_size],
                &postfix_tuple.as_slice()[..postfix_size],
            ],
            &bufs,
        )?;
        page(current).set_lsn(recptr);
        if new_buffer != InvalidBuffer {
            buf_page_mut(new_buffer).set_lsn(recptr);
        }
    }

    // spgdoinsert.c:1896
    EndCriticalSection();

    if new_buffer != InvalidBuffer {
        SpGistSetLastUsedPage(index, new_buffer)?;
        unlock_release(new_buffer)?;
    }
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn index_row_too_large(index: &Relation<'_>, leaf_size: usize) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "index row size {} exceeds maximum {} for index \"{}\"",
            leaf_size - SIZEOF_ITEM_ID_DATA,
            SPGIST_PAGE_CAPACITY - SIZEOF_ITEM_ID_DATA,
            index.name()
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .with_hint("Values larger than a buffer page cannot be indexed."),
    )
}

/// spgdoinsert. `mcx` is the per-tuple temp context (reset by caller).
pub fn spgdoinsert<'m>(
    mcx: Mcx<'m>,
    index: &Relation<'_>,
    state: &mut SpGistState<'_>,
    heap_ptr: &ItemPointerData,
    datums: &[Datum],
    isnulls: &[bool],
) -> PgResult<bool> {
    let leaf_desc = state.leafTupDesc.clone();
    let natts = leaf_desc.natts as usize;
    let isnull = isnulls[spgKeyColumn];
    let mut level: i32 = 0;
    let mut leaf_datums = [Datum::null(); K];

    if !isnull {
        if state.has_compress() {
            leaf_datums[spgKeyColumn] = state.call_compress(mcx, datums[spgKeyColumn])?;
        } else {
            debug_assert!(state.attLeafType.type_ == state.attType.type_);
            leaf_datums[spgKeyColumn] = if state.attType.attlen == -1 {
                maybe_detoast(mcx, datums[spgKeyColumn])?
            } else {
                datums[spgKeyColumn]
            };
        }
    }

    for i in spgFirstIncludeColumn..natts {
        if !isnulls[i] {
            leaf_datums[i] = if leaf_desc.compact_attr(i).attlen == -1 {
                maybe_detoast(mcx, datums[i])?
            } else {
                datums[i]
            };
        }
    }

    let mut leaf_size =
        SpGistGetLeafTupleSize(&leaf_desc, &leaf_datums[..natts], &isnulls[..natts])
            + SIZEOF_ITEM_ID_DATA;

    if leaf_size > SPGIST_PAGE_CAPACITY && (isnull || !state.config.longValuesOK) {
        return Err(index_row_too_large(index, leaf_size));
    }
    let mut best_leaf_size = leaf_size;
    let mut num_no_progress_cycles = 0;

    let mut current = SPPageDesc {
        blkno: if isnull { SPGIST_NULL_BLKNO } else { SPGIST_ROOT_BLKNO },
        buffer: InvalidBuffer,
        offnum: FirstOffsetNumber,
        node: -1,
    };
    let mut parent = SPPageDesc {
        blkno: InvalidBlockNumber,
        buffer: InvalidBuffer,
        offnum: InvalidOffsetNumber,
        node: -1,
    };

    crate::check_for_interrupts()?;

    let result = 'outer: loop {
        // C (spgdoinsert.c:2044-2048): INTERRUPTS_PENDING_CONDITION() —
        // non-destructive. We hold buffer lock(s) after the first iteration,
        // so ProcessInterrupts couldn't throw a cancel here; break out with
        // result = false (the caller restarts the insertion unless the
        // post-release CHECK_FOR_INTERRUPTS below throws).
        if init_small::globals::InterruptPending() {
            break 'outer false;
        }

        let mut is_new = false;
        if current.blkno == InvalidBlockNumber {
            current.buffer = SpGistGetBuffer(
                index,
                GBUF_LEAF | if isnull { GBUF_NULLS } else { 0 },
                leaf_size.min(SPGIST_PAGE_CAPACITY) as i32,
                &mut is_new,
            )?;
            current.blkno = bufmgr::buffer_get_block_number::call(current.buffer);
        } else if parent.buffer == InvalidBuffer {
            current.buffer = bufmgr::read_buffer::call(index, current.blkno)?;
            bufmgr::lock_buffer::call(current.buffer, bufmgr::BUFFER_LOCK_EXCLUSIVE)?;
        } else if current.blkno != parent.blkno {
            current.buffer = bufmgr::read_buffer::call(index, current.blkno)?;
            if !bufmgr::conditional_lock_buffer::call(current.buffer)? {
                // Release both buffers before propagating either failure, so
                // an error on one doesn't leak the other's pin.
                let released_current = bufmgr::release_buffer::call(current.buffer);
                unlock_release(parent.buffer)?;
                released_current?;
                return Ok(false);
            }
        } else {
            current.buffer = parent.buffer;
        }

        {
            let pm = page(&current);
            let stores_nulls = SpGistPageStoresNulls(&pm.as_ref());
            if isnull != stores_nulls {
                // C (spgdoinsert.c:2104): elog(ERROR) — catchable XX000.
                return Err(Box::new(PgError::error(format!(
                    "SPGiST index page {} has wrong nulls flag",
                    current.blkno
                ))));
            }
        }

        let process_inner;

        if SpGistPageIsLeaf(&page(&current).as_ref()) {
            let mut leaf_tuple = spgFormLeafTuple(
                mcx,
                state,
                heap_ptr,
                &leaf_datums[..natts],
                &isnulls[..natts],
            )?;
            let lt_size = leaf_tuple.size();

            let fits = lt_size + SIZEOF_ITEM_ID_DATA
                <= SpGistPageGetFreeSpace(&page(&current).as_ref(), 1);
            if fits {
                addLeafTuple(
                    index,
                    state,
                    &mut leaf_tuple.as_mut_slice()[..lt_size],
                    &mut current,
                    &parent,
                    isnull,
                    is_new,
                )?;
                break 'outer true;
            }
            let mut n_to_split = 0;
            let size_to_split = checkSplitConditions(index, &current, &mut n_to_split)?;
            if size_to_split < SPGIST_PAGE_CAPACITY / 2
                && n_to_split < 64
                && lt_size + SIZEOF_ITEM_ID_DATA + size_to_split <= SPGIST_PAGE_CAPACITY
            {
                debug_assert!(!is_new);
                moveLeafs(
                    mcx,
                    index,
                    state,
                    &current,
                    &parent,
                    &mut leaf_tuple.as_mut_slice()[..lt_size],
                    isnull,
                )?;
                break 'outer true;
            }
            if doPickSplit(
                mcx,
                index,
                state,
                &mut current,
                &mut parent,
                &leaf_tuple.as_slice()[..lt_size],
                level,
                isnull,
                is_new,
            )? {
                break 'outer true;
            }
            debug_assert!(!SpGistPageIsLeaf(&page(&current).as_ref()));
            process_inner = true;
        } else {
            process_inner = true;
        }

        if process_inner {
            // process_inner_tuple
            loop {
                // C (spgdoinsert.c:2170-2174): same non-destructive check, so
                // a broken choose function looping on add/split requests
                // still yields to a cancel.
                if init_small::globals::InterruptPending() {
                    break 'outer false;
                }

                let (in_choose, n_nodes, all_the_same) = {
                    let pm = page(&current);
                    let pr = pm.as_ref();
                    let inner = item_slice(&pr, current.offnum);
                    let hdr = SpGistInnerTupleHeader::decode(inner);

                    let mut labels_vec: Vec<Datum> = Vec::new();
                    let has_labels = spgExtractNodeLabels(state, inner, &mut labels_vec)?;
                    // move labels into mcx so pointers stay valid after this block
                    let labels_ptr = if has_labels {
                        let mut lv: PgVec<'m, Datum> =
                            ::mcx::vec_with_capacity_in(mcx, labels_vec.len())?;
                        lv.extend_from_slice(&labels_vec);
                        let p = lv.as_ptr();
                        core::mem::forget(lv);
                        p
                    } else {
                        core::ptr::null()
                    };

                    (
                        spgChooseIn {
                            datum: datums[spgKeyColumn],
                            leafDatum: leaf_datums[spgKeyColumn],
                            level,
                            allTheSame: hdr.allTheSame,
                            hasPrefix: hdr.prefixSize > 0,
                            prefixDatum: inner_prefix_datum(inner, state)?,
                            nNodes: hdr.nNodes as i32,
                            nodeLabels: labels_ptr,
                        },
                        hdr.nNodes as i32,
                        hdr.allTheSame,
                    )
                };

                let mut out = spgChooseOut::None;
                if !isnull {
                    state.call_choose(mcx, &in_choose, &mut out)?;
                } else {
                    out = spgChooseOut::MatchNode {
                        nodeN: 0,
                        levelAdd: 0,
                        restDatum: Datum::null(),
                    };
                }

                if all_the_same {
                    resolve_all_the_same_choice(&mut out, n_nodes)?;
                }

                match out {
                    spgChooseOut::MatchNode {
                        nodeN,
                        levelAdd,
                        restDatum,
                    } => {
                        spgMatchNodeAction(index, &mut current, &mut parent, nodeN)?;
                        level += levelAdd;
                        if !isnull {
                            leaf_datums[spgKeyColumn] = restDatum;
                            leaf_size = SpGistGetLeafTupleSize(
                                &leaf_desc,
                                &leaf_datums[..natts],
                                &isnulls[..natts],
                            ) + SIZEOF_ITEM_ID_DATA;
                        }

                        if leaf_size > SPGIST_PAGE_CAPACITY {
                            let mut ok = false;
                            if state.config.longValuesOK && !isnull {
                                if leaf_size < best_leaf_size {
                                    ok = true;
                                    best_leaf_size = leaf_size;
                                    num_no_progress_cycles = 0;
                                } else {
                                    num_no_progress_cycles += 1;
                                    if num_no_progress_cycles < 10 {
                                        ok = true;
                                    }
                                }
                            }
                            if !ok {
                                return Err(index_row_too_large(index, leaf_size));
                            }
                        }
                        continue 'outer;
                    }
                    spgChooseOut::AddNode { nodeLabel, nodeN } => {
                        add_node_requires_labels(&in_choose)?;
                        spgAddNodeAction(
                            mcx, index, state, &mut current, &parent, nodeN, nodeLabel,
                        )?;
                        continue;
                    }
                    spgChooseOut::SplitTuple { .. } => {
                        spgSplitNodeAction(mcx, index, state, &mut current, &out)?;
                        continue;
                    }
                    spgChooseOut::None => {
                        panic!("unrecognized SPGiST choose result")
                    }
                }
            }
        }
    };

    if current.buffer != InvalidBuffer {
        SpGistSetLastUsedPage(index, current.buffer)?;
        unlock_release(current.buffer)?;
    }
    if parent.buffer != InvalidBuffer && parent.buffer != current.buffer {
        SpGistSetLastUsedPage(index, parent.buffer)?;
        unlock_release(parent.buffer)?;
    }

    crate::check_for_interrupts()?;
    Ok(result)
}

fn maybe_detoast<'m>(mcx: Mcx<'m>, datum: Datum) -> PgResult<Datum> {
    let p = datum.as_usize() as *const u8;
    // SAFETY: non-null varlena datum carries a live pointer (caller contract).
    let extended = unsafe {
        ::types_tuple::varatt::varatt_is_1b(p)
            || ::types_tuple::varatt::varatt_is_1b_e(p)
            || detoast_is_compressed(p)
    };
    if !extended {
        return Ok(datum);
    }
    // SAFETY: live varlena image; detoast copies into mcx.
    let img = unsafe {
        let len = ::types_tuple::varatt::varsize_any(p);
        core::slice::from_raw_parts(p, len)
    };
    let out = ::detoast::detoast_attr(mcx, img)?;
    let ptr = out.as_ptr() as usize;
    core::mem::forget(out);
    Ok(Datum::from_usize(ptr))
}

// VARATT_IS_COMPRESSED on a 4B header.
#[inline]
unsafe fn detoast_is_compressed(p: *const u8) -> bool {
    let hdr = p.cast::<u32>().read_unaligned();
    if cfg!(target_endian = "little") {
        (hdr & 0x03) == 0x02
    } else {
        (hdr & 0xC000_0000) == 0x4000_0000
    }
}

// ---------------------------------------------------------------------------
// Opclass-output guards: the spgdoinsert.c elog(ERROR) sites that police what
// a choose/picksplit support function handed back.
// ---------------------------------------------------------------------------

/// spgdoinsert.c:955-957 — one picksplit `mapTuplesToNodes[]` entry as an
/// index into the node array.
fn picksplit_node_index(n: i32, n_nodes: usize) -> PgResult<usize> {
    if n < 0 || n as usize >= n_nodes {
        // C (spgdoinsert.c:957): elog(ERROR) — catchable XX000.
        return Err(Box::new(PgError::error(
            "inconsistent result of SPGiST picksplit function",
        )));
    }
    Ok(n as usize)
}

/// spgdoinsert.c:1100-1113 — assign each node's leaf group to the current
/// page (0) or the new leaf page (1); both space budgets must stay >= 0.
fn divide_leaf_groups(
    leaf_sizes: &[i32],
    mut curspace: i64,
    mut newspace: i64,
    node_page_select: &mut [u8],
) -> PgResult<()> {
    for (i, &size) in leaf_sizes.iter().enumerate() {
        if (size as i64) <= curspace {
            node_page_select[i] = 0;
            curspace -= size as i64;
        } else {
            node_page_select[i] = 1;
            newspace -= size as i64;
        }
    }
    if curspace < 0 || newspace < 0 {
        return Err(failed_to_divide_leaf_groups());
    }
    Ok(())
}

/// spgdoinsert.c:1112 and :1117.
fn failed_to_divide_leaf_groups() -> Box<PgError> {
    // elog(ERROR) — catchable XX000.
    Box::new(PgError::error("failed to divide leaf tuple groups across pages"))
}

/// spgdoinsert.c:1481-1489 — SGITITERATE to node `nodeN` and return its
/// downlink.
fn locate_node(inner: &[u8], nodeN: i32) -> PgResult<ItemPointerData> {
    let hdr = SpGistInnerTupleHeader::decode(inner);
    if nodeN < 0 || nodeN >= hdr.nNodes as i32 {
        // C (spgdoinsert.c:1489): elog(ERROR) — catchable XX000.
        return Err(Box::new(PgError::error(format!(
            "failed to find requested node {nodeN} in SPGiST inner tuple"
        ))));
    }
    let mut node_off = SGITHDRSZ + hdr.prefixSize as usize;
    for _ in 0..nodeN {
        node_off += node_tuple_size(&inner[node_off..]);
    }
    Ok(node_tuple_tid(&inner[node_off..]))
}

/// spgdoinsert.c:1733-1741 — sanity of a spgSplitTuple choose output.
fn check_split_tuple_output(prefixNNodes: i32, childNodeN: i32) -> PgResult<()> {
    // C (spgdoinsert.c:1735, :1740): elog(ERROR) — catchable XX000.
    if prefixNNodes <= 0 || prefixNNodes > SGITMAXNNODES as i32 {
        return Err(Box::new(PgError::error(format!(
            "invalid number of prefix nodes: {prefixNNodes}"
        ))));
    }
    if childNodeN < 0 || childNodeN >= prefixNNodes {
        return Err(Box::new(PgError::error(format!(
            "invalid child node number: {childNodeN}"
        ))));
    }
    Ok(())
}

/// spgdoinsert.c:2205-2218 — an allTheSame inner tuple admits no AddNode;
/// a MatchNode descends into a random one of its nodes.
fn resolve_all_the_same_choice(out: &mut spgChooseOut, n_nodes: i32) -> PgResult<()> {
    match out {
        spgChooseOut::AddNode { .. } => {
            // C (spgdoinsert.c:2212): elog(ERROR) — catchable XX000.
            Err(Box::new(PgError::error(
                "cannot add a node to an allTheSame inner tuple",
            )))
        }
        spgChooseOut::MatchNode { nodeN, .. } => {
            *nodeN = pg_prng::global_prng(|p| p.u64_range(0, (n_nodes - 1) as u64)) as i32;
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod opclass_output_guard_tests {
    //! Witnesses for the spgdoinsert.c elog(ERROR) guards on opclass output.
    //! C raises a catchable ERROR (XX000, the elog default) with these exact
    //! message bytes; a Rust `panic!` at the same site is the divergence.
    use super::*;
    use ::types_error::ERRCODE_INTERNAL_ERROR;
    use ::types_tuple::itemptr::ItemPointerGetBlockNumber;

    fn assert_c_error<T: core::fmt::Debug>(r: PgResult<T>, msg: &str) {
        match r {
            Err(e) => {
                assert_eq!(e.message(), msg, "message bytes differ from C");
                assert_eq!(e.sqlstate(), ERRCODE_INTERNAL_ERROR, "elog(ERROR) is XX000");
            }
            Ok(v) => panic!("expected error {msg:?}, got Ok({v:?})"),
        }
    }

    // spgdoinsert.c:955-957
    #[test]
    fn picksplit_mapping_out_of_range_is_c_error() {
        assert_eq!(picksplit_node_index(2, 3).unwrap(), 2);
        assert_c_error(
            picksplit_node_index(3, 3),
            "inconsistent result of SPGiST picksplit function",
        );
        assert_c_error(
            picksplit_node_index(-1, 3),
            "inconsistent result of SPGiST picksplit function",
        );
    }

    // spgdoinsert.c:1100-1113
    #[test]
    fn leaf_groups_overflowing_both_pages_is_c_error() {
        let mut sel = [9u8; 2];
        divide_leaf_groups(&[40, 40], 50, 50, &mut sel).unwrap();
        assert_eq!(sel, [0, 1], "first group on the current page, second on the new page");
        let mut sel = [9u8; 2];
        assert_c_error(
            divide_leaf_groups(&[100, 100], 50, 50, &mut sel),
            "failed to divide leaf tuple groups across pages",
        );
    }

    // spgdoinsert.c:1115-1118 (the "already excluded the new tuple" arm)
    #[test]
    fn excluded_new_tuple_arm_is_c_error() {
        let r: PgResult<()> = Err(failed_to_divide_leaf_groups());
        assert_c_error(r, "failed to divide leaf tuple groups across pages");
    }

    // A two-node inner tuple with no prefix: header, then two 8-byte node
    // tuples (t_tid at 0..6, t_info = size at 6..8).
    fn two_node_inner_tuple() -> Vec<u8> {
        let mut img = vec![0u8; SGITHDRSZ + 2 * SGNTHDRSZ];
        SpGistInnerTupleHeader {
            tupstate: SPGIST_LIVE,
            allTheSame: false,
            nNodes: 2,
            prefixSize: 0,
            size: img.len() as u16,
        }
        .encode(&mut img);
        for (i, tid) in [ItemPointerData::new(7, 3), ItemPointerData::new(9, 5)].iter().enumerate() {
            let off = SGITHDRSZ + i * SGNTHDRSZ;
            node_tuple_set_tid(&mut img[off..], tid);
            img[off + 6..off + 8].copy_from_slice(&(SGNTHDRSZ as u16).to_ne_bytes());
        }
        img
    }

    // spgdoinsert.c:1481-1489
    #[test]
    fn match_node_out_of_range_is_c_error() {
        let img = two_node_inner_tuple();
        let tid = locate_node(&img, 1).unwrap();
        assert_eq!((ItemPointerGetBlockNumber(&tid), tid.ip_posid), (9, 5));
        assert_c_error(locate_node(&img, 2), "failed to find requested node 2 in SPGiST inner tuple");
        assert_c_error(locate_node(&img, -1), "failed to find requested node -1 in SPGiST inner tuple");
    }

    // spgdoinsert.c:1733-1741
    #[test]
    fn split_tuple_bad_prefix_or_child_is_c_error() {
        check_split_tuple_output(2, 1).unwrap();
        assert_c_error(check_split_tuple_output(0, 0), "invalid number of prefix nodes: 0");
        assert_c_error(
            check_split_tuple_output(SGITMAXNNODES as i32 + 1, 0),
            "invalid number of prefix nodes: 8192",
        );
        assert_c_error(check_split_tuple_output(2, 2), "invalid child node number: 2");
        assert_c_error(check_split_tuple_output(2, -1), "invalid child node number: -1");
    }

    // spgdoinsert.c:2211-2212
    #[test]
    fn all_the_same_add_node_is_c_error() {
        let mut out = spgChooseOut::SplitTuple {
            prefixHasPrefix: false,
            prefixPrefixDatum: Datum::null(),
            prefixNNodes: 1,
            prefixNodeLabels: core::ptr::null(),
            childNodeN: 0,
            postfixHasPrefix: false,
            postfixPrefixDatum: Datum::null(),
        };
        resolve_all_the_same_choice(&mut out, 4).unwrap();
        let mut out = spgChooseOut::AddNode { nodeLabel: Datum::null(), nodeN: 0 };
        assert_c_error(
            resolve_all_the_same_choice(&mut out, 4),
            "cannot add a node to an allTheSame inner tuple",
        );
    }
}

#[cfg(test)]
mod choose_result_tests {
    use super::*;

    // spgdoinsert.c:1769 elog(ERROR): an opclass choose method returning
    // spgSplitTuple with a prefix tuple larger than the inner tuple it
    // replaces is a catchable XX000 error, never a panic (audit row
    // spgdoinsert-884020c4).
    #[test]
    fn split_longer_prefix_is_catchable_error() {
        assert!(split_prefix_fits(32, 32).is_ok());
        let err = split_prefix_fits(33, 32).expect_err("longer prefix must be rejected");
        assert_eq!(err.message(), "SPGiST inner-tuple split must not produce longer prefix");
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INTERNAL_ERROR);
        assert_eq!(err.level(), ::types_error::ERROR);
    }

    // spgdoinsert.c:2295 elog(ERROR): spgAddNode against an inner tuple whose
    // nodes carry no labels is a catchable XX000 error, never a panic (audit
    // row spgdoinsert-f79b01f2).
    #[test]
    fn add_node_without_labels_is_catchable_error() {
        let labels = [Datum::from_i32(1), Datum::from_i32(2)];
        let mut input = spgChooseIn {
            datum: Datum::null(),
            leafDatum: Datum::null(),
            level: 0,
            allTheSame: false,
            hasPrefix: false,
            prefixDatum: Datum::null(),
            nNodes: 2,
            nodeLabels: labels.as_ptr(),
        };
        assert!(add_node_requires_labels(&input).is_ok());
        input.nodeLabels = core::ptr::null();
        let err =
            add_node_requires_labels(&input).expect_err("label-less AddNode must be rejected");
        assert_eq!(err.message(), "cannot add a node to an inner tuple without node labels");
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INTERNAL_ERROR);
        assert_eq!(err.level(), ::types_error::ERROR);
    }
}
