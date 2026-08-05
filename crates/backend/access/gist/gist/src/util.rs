//! gistutil.c: page init/check, tuple (de)compression, unions, penalties,
//! choose, new-buffer allocation, fake LSNs.

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::nbtree::itup::{index_form_tuple, index_getattr, maxalign, ItupBuf};
use ::types_core::{
    AttrNumber, BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, OffsetNumber, Oid,
    XLogRecPtr, BLCKSZ, RELPERSISTENCE_TEMP,
};
use ::types_error::{PgError, PgResult, ERRCODE_INDEX_CORRUPTED};
use ::types_gist::{
    page_opaque, page_opaque_update, GISTPageOpaqueData, GistEntryVector, GistPageIsDeleted,
    GISTENTRY, GIST_PAGE_ID, GiSTPageSize, TUPLE_IS_INVALID, TUPLE_IS_VALID,
};
use ::types_rel::Relation;
use ::types_storage::bufpage::{PageMut, PageRef};
use ::types_tuple::itemptr::ItemPointerData;

use crate::state::GistState;

pub(crate) const FirstOffsetNumber: OffsetNumber = 1;
pub(crate) const InvalidOffsetNumber: OffsetNumber = 0;
const InvalidOid: Oid = 0;
const SIZEOF_ITEM_ID_DATA: usize = 4;

pub type ITup = *const u8;

#[inline]
pub unsafe fn index_tuple_size(itup: ITup) -> usize {
    ::nbtree::itup::index_tuple_size(itup)
}

#[inline]
pub unsafe fn itup_slice<'a>(itup: ITup) -> &'a [u8] {
    core::slice::from_raw_parts(itup, index_tuple_size(itup))
}

// PageGetItem for offnum with the per-page max_offset check done by caller.
#[inline]
pub fn page_item(page: &PageRef<'_>, offnum: OffsetNumber) -> ITup {
    let id = page.item_id(offnum);
    page.item_raw(id).0
}

#[inline]
pub(crate) unsafe fn itup_get_tid(itup: ITup) -> ItemPointerData {
    itup.cast::<ItemPointerData>().read_unaligned()
}

#[inline]
pub unsafe fn itup_block_number(itup: ITup) -> BlockNumber {
    let hi = itup.cast::<u16>().read_unaligned() as u32;
    let lo = itup.add(2).cast::<u16>().read_unaligned() as u32;
    (hi << 16) | lo
}

#[inline]
pub(crate) unsafe fn itup_posid(itup: ITup) -> u16 {
    itup.add(4).cast::<u16>().read_unaligned()
}

#[inline]
pub(crate) unsafe fn gist_tuple_is_invalid(itup: ITup) -> bool {
    itup_posid(itup) == TUPLE_IS_INVALID
}

#[inline]
pub fn itup_set_block_number(itup: &mut [u8], blkno: BlockNumber) {
    itup[0..2].copy_from_slice(&((blkno >> 16) as u16).to_ne_bytes());
    itup[2..4].copy_from_slice(&((blkno & 0xffff) as u16).to_ne_bytes());
}

#[inline]
pub(crate) fn gist_tuple_set_valid(itup: &mut [u8]) {
    itup[4..6].copy_from_slice(&TUPLE_IS_VALID.to_ne_bytes());
}

pub(crate) fn copy_itup<'mcx>(mcx: Mcx<'mcx>, itup: ITup) -> PgResult<ItupBuf<'mcx>> {
    // SAFETY: caller holds the pin/lock keeping itup live.
    let sz = unsafe { index_tuple_size(itup) };
    let mut buf = ItupBuf::with_size(mcx, maxalign(sz))?;
    // SAFETY: src live for sz (caller contract); dst freshly sized.
    unsafe { core::ptr::copy_nonoverlapping(itup, buf.as_mut_ptr(), sz) };
    Ok(buf)
}

#[track_caller]
#[cold]
#[inline(never)]
fn index_corrupted(msg: std::string::String) -> Box<PgError> {
    Box::new(
        PgError::error(msg)
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_hint("Please REINDEX it."),
    )
}

/// gistinitpage.
pub fn gistinitpage(page: &mut PageMut<'_>, f: u16) {
    page.init(::types_gist::SizeOfGistPageOpaque);
    ::types_gist::page_opaque_set(
        page,
        GISTPageOpaqueData {
            nsn: 0,
            rightlink: InvalidBlockNumber,
            flags: f,
            gist_page_id: GIST_PAGE_ID,
        },
    );
}

/// GISTInitBuffer.
pub fn gist_init_buffer(pin: &BufferPin, f: u16) {
    let mut page = crate::buf_page_mut(pin.buffer());
    gistinitpage(&mut page, f);
}

/// gistcheckpage.
pub fn gistcheckpage(rel: &Relation<'_>, pin: &BufferPin) -> PgResult<()> {
    let page = pin.page();
    if page.is_new() {
        return Err(index_corrupted(format!(
            "index \"{}\" contains unexpected zero page at block {}",
            rel.name(),
            pin.block_number()
        )));
    }
    if BLCKSZ - page.pd_special() as usize != ::types_gist::SizeOfGistPageOpaque {
        return Err(index_corrupted(format!(
            "index \"{}\" contains corrupted page at block {}",
            rel.name(),
            pin.block_number()
        )));
    }
    Ok(())
}

/// gistfillbuffer: add tuples at `off` (or append when InvalidOffsetNumber).
pub fn gistfillbuffer(
    rel_name: &str,
    page: &mut PageMut<'_>,
    itup: &[&[u8]],
    off: OffsetNumber,
) -> PgResult<()> {
    let mut off = if off == InvalidOffsetNumber {
        if page.as_ref().pd_lower() as usize
            <= ::types_storage::bufpage::SizeOfPageHeaderData
        {
            FirstOffsetNumber
        } else {
            page.as_ref().max_offset_number() + 1
        }
    } else {
        off
    };
    for tup in itup {
        let l = page.add_item(tup, off, 0);
        if l.is_none() {
            panic!("failed to add item to index page in \"{rel_name}\"");
        }
        off += 1;
    }
    Ok(())
}

/// gistnospace.
pub fn gistnospace(
    page: &PageRef<'_>,
    itvec: &[&[u8]],
    todelete: OffsetNumber,
    freespace: usize,
) -> bool {
    let mut size = freespace;
    let mut deleted = 0usize;
    for it in itvec {
        size += it.len() + SIZEOF_ITEM_ID_DATA;
    }
    if todelete != InvalidOffsetNumber {
        let itup = page_item(page, todelete);
        // SAFETY: page item under the caller's lock.
        deleted = unsafe { index_tuple_size(itup) } + SIZEOF_ITEM_ID_DATA;
    }
    page.free_space() + deleted < size
}

/// gistfitpage.
pub fn gistfitpage(itvec: &[&[u8]]) -> bool {
    let mut size = 0usize;
    for it in itvec {
        size += it.len() + SIZEOF_ITEM_ID_DATA;
    }
    size <= GiSTPageSize
}

/// gistextractpage: copy every tuple off the page.
pub fn gistextractpage<'mcx>(
    mcx: Mcx<'mcx>,
    page: &PageRef<'_>,
) -> PgResult<Vec<ItupBuf<'mcx>>> {
    let maxoff = page.max_offset_number();
    let mut itvec = Vec::with_capacity(maxoff as usize);
    for i in FirstOffsetNumber..=maxoff {
        itvec.push(copy_itup(mcx, page_item(page, i))?);
    }
    Ok(itvec)
}

/// gistfillitupvec: flatten tuple images into one contiguous buffer.
pub fn gistfillitupvec<'mcx>(mcx: Mcx<'mcx>, vec: &[&[u8]]) -> PgResult<PgVec<'mcx, u8>> {
    let memlen: usize = vec.iter().map(|it| it.len()).sum();
    let mut out: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, memlen)?;
    for it in vec {
        out.extend_from_slice(it);
    }
    Ok(out)
}

/// gistdentryinit.
pub fn gistdentryinit(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    nkey: usize,
    k: Datum,
    o: OffsetNumber,
    l: bool,
    page_is_leaf: bool,
    is_null: bool,
) -> PgResult<GISTENTRY> {
    if !is_null {
        let e = GISTENTRY::init(k, o, l, page_is_leaf);
        if !giststate.has_decompress(nkey) {
            return Ok(e);
        }
        giststate.call_decompress(mcx, nkey, &e)
    } else {
        Ok(GISTENTRY::init(Datum::null(), o, l, page_is_leaf))
    }
}

/// gistCompressValues.
pub fn gistCompressValues(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    r: &Relation<'_>,
    attdata: &[Datum],
    isnull: &[bool],
    isleaf: bool,
    compatt: &mut [Datum],
) -> PgResult<()> {
    let nkeyatts = r.indnkeyatts() as usize;
    for i in 0..nkeyatts {
        if isnull[i] {
            compatt[i] = Datum::null();
        } else {
            let centry = GISTENTRY::init(attdata[i], 0, isleaf, false);
            if giststate.has_compress(i) {
                let cep = giststate.call_compress(mcx, i, &centry)?;
                compatt[i] = cep.key;
            } else {
                compatt[i] = centry.key;
            }
        }
    }
    let natts = r.rd_att.natts as usize;
    if isleaf {
        compatt[nkeyatts..natts].copy_from_slice(&attdata[nkeyatts..natts]);
    }
    Ok(())
}

/// gistFormTuple; t_tid offset is set to the 0xffff sentinel.
pub fn gistFormTuple<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &mut GistState<'_>,
    r: &Relation<'_>,
    attdata: &[Datum],
    isnull: &[bool],
    isleaf: bool,
) -> PgResult<ItupBuf<'mcx>> {
    let natts = r.rd_att.natts as usize;
    let mut compatt = [Datum::null(); ::types_core::fmgr::INDEX_MAX_KEYS as usize];
    gistCompressValues(mcx, giststate, r, attdata, isnull, isleaf, &mut compatt[..natts])?;

    // Non-leaf tuples form over the truncated descriptor: INCLUDE attrs are
    // dropped (gistutil.c:583-585).
    let tupdesc = if isleaf {
        giststate.leafTupdesc.clone()
    } else {
        giststate.nonLeafTupdesc.clone()
    };
    let n = tupdesc.natts as usize;
    let mut res = index_form_tuple(mcx, &tupdesc, &compatt[..n], &isnull[..n])?;
    // SAFETY: fresh owned image, in-bounds 2-byte store at t_tid.ip_posid.
    unsafe {
        res.as_mut_ptr()
            .add(4)
            .cast::<u16>()
            .write_unaligned(TUPLE_IS_VALID);
    }
    Ok(res)
}

pub fn gist_index_getattr(
    itup: ITup,
    attno_1based: usize,
    giststate: &GistState<'_>,
) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: itup live (caller's pin/copy), aligned per itup contract.
    let d = unsafe {
        index_getattr(
            itup,
            attno_1based as AttrNumber,
            &giststate.leafTupdesc,
            &mut isnull,
        )
    };
    (d, isnull)
}

/// gistMakeUnionItVec.
pub fn gistMakeUnionItVec(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    itvec: &[ITup],
    attr: &mut [Datum],
    isnull: &mut [bool],
) -> PgResult<()> {
    let ncols = giststate.nonLeafTupdesc.natts as usize;
    for i in 0..ncols {
        // union's evec is DENSE 0-based (unlike picksplit's 1-based vector).
        let mut evec = GistEntryVector {
            n: 0,
            vector: Vec::with_capacity(itvec.len() + 2),
        };
        for &it in itvec {
            let (datum, is_null) = gist_index_getattr(it, i + 1, giststate);
            if is_null {
                continue;
            }
            let entry = gistdentryinit(mcx, giststate, i, datum, 0, false, false, false)?;
            evec.vector.push(entry);
        }
        evec.n = evec.vector.len() as i32;

        if evec.n == 0 {
            attr[i] = Datum::null();
            isnull[i] = true;
        } else {
            if evec.n == 1 {
                // unionFn may expect at least two inputs
                let dup = evec.vector[0];
                evec.vector.push(dup);
                evec.n = 2;
            }
            attr[i] = giststate.call_union(mcx, i, &evec)?;
            isnull[i] = false;
        }
    }
    Ok(())
}

/// gistunion.
pub fn gistunion<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'_>,
    itvec: &[ITup],
    giststate: &mut GistState<'_>,
) -> PgResult<ItupBuf<'mcx>> {
    let n = giststate.nonLeafTupdesc.natts as usize;
    let mut attr = [Datum::null(); ::types_core::fmgr::INDEX_MAX_KEYS as usize];
    let mut isnull = [false; ::types_core::fmgr::INDEX_MAX_KEYS as usize];
    gistMakeUnionItVec(mcx, giststate, itvec, &mut attr[..n], &mut isnull[..n])?;
    gistFormTuple(mcx, giststate, r, &attr[..n], &isnull[..n], false)
}

/// gistMakeUnionKey.
pub fn gistMakeUnionKey(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    attno: usize,
    entry1: &GISTENTRY,
    isnull1: bool,
    entry2: &GISTENTRY,
    isnull2: bool,
) -> PgResult<(Datum, bool)> {
    if isnull1 && isnull2 {
        return Ok((Datum::null(), true));
    }
    let (a, b) = if !isnull1 && !isnull2 {
        (*entry1, *entry2)
    } else if !isnull1 {
        (*entry1, *entry1)
    } else {
        (*entry2, *entry2)
    };
    let evec = GistEntryVector {
        n: 2,
        vector: vec![a, b],
    };
    let dst = giststate.call_union(mcx, attno, &evec)?;
    Ok((dst, false))
}

/// gistKeyIsEQ.
pub fn gistKeyIsEQ(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    attno: usize,
    a: Datum,
    b: Datum,
) -> PgResult<bool> {
    giststate.call_same(mcx, attno, a, b)
}

/// gistDeCompressAtt.
pub fn gistDeCompressAtt(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    r: &Relation<'_>,
    tuple: ITup,
    o: OffsetNumber,
    attdata: &mut [GISTENTRY],
    isnull: &mut [bool],
) -> PgResult<()> {
    let nkeyatts = r.indnkeyatts() as usize;
    for i in 0..nkeyatts {
        let (datum, is_null) = gist_index_getattr(tuple, i + 1, giststate);
        isnull[i] = is_null;
        attdata[i] = gistdentryinit(mcx, giststate, i, datum, o, false, false, is_null)?;
    }
    Ok(())
}

/// gistgetadjusted: None when no key update is needed.
pub fn gistgetadjusted<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'_>,
    oldtup: ITup,
    addtup: ITup,
    giststate: &mut GistState<'_>,
) -> PgResult<Option<ItupBuf<'mcx>>> {
    const K: usize = ::types_core::fmgr::INDEX_MAX_KEYS as usize;
    let nkeyatts = r.indnkeyatts() as usize;

    let mut oldentries = [GISTENTRY::default(); K];
    let mut oldisnull = [false; K];
    let mut addentries = [GISTENTRY::default(); K];
    let mut addisnull = [false; K];
    gistDeCompressAtt(mcx, giststate, r, oldtup, 0, &mut oldentries, &mut oldisnull)?;
    gistDeCompressAtt(mcx, giststate, r, addtup, 0, &mut addentries, &mut addisnull)?;

    let mut attr = [Datum::null(); K];
    let mut isnull = [false; K];
    let mut neednew = false;

    for i in 0..nkeyatts {
        let (un, un_isnull) = gistMakeUnionKey(
            mcx,
            giststate,
            i,
            &oldentries[i],
            oldisnull[i],
            &addentries[i],
            addisnull[i],
        )?;
        attr[i] = un;
        isnull[i] = un_isnull;

        if neednew {
            continue;
        }
        if isnull[i] {
            continue;
        }
        if !addisnull[i]
            && (oldisnull[i] || !gistKeyIsEQ(mcx, giststate, i, oldentries[i].key, attr[i])?)
        {
            neednew = true;
        }
    }

    if neednew {
        let mut newtup = gistFormTuple(mcx, giststate, r, &attr, &isnull, false)?;
        // newtup->t_tid = oldtup->t_tid
        // SAFETY: both live images; 6-byte t_tid copy.
        unsafe {
            core::ptr::copy_nonoverlapping(oldtup, newtup.as_mut_ptr(), 6);
        }
        Ok(Some(newtup))
    } else {
        Ok(None)
    }
}

/// gistpenalty.
pub fn gistpenalty(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    attno: usize,
    orig: &GISTENTRY,
    is_null_orig: bool,
    add: &GISTENTRY,
    is_null_add: bool,
) -> PgResult<f32> {
    if !giststate.penaltyFn[attno].fn_strict || (!is_null_orig && !is_null_add) {
        let p = giststate.call_penalty(mcx, attno, orig, add)?;
        Ok(if p.is_nan() || p < 0.0 { 0.0 } else { p })
    } else if is_null_orig && is_null_add {
        Ok(0.0)
    } else {
        Ok(f32::INFINITY)
    }
}

/// gistchoose.
pub fn gistchoose(
    mcx: Mcx<'_>,
    r: &Relation<'_>,
    p: &PageRef<'_>,
    it: ITup,
    giststate: &mut GistState<'_>,
) -> PgResult<OffsetNumber> {
    const K: usize = ::types_core::fmgr::INDEX_MAX_KEYS as usize;
    debug_assert!(!::types_gist::GistPageIsLeaf(p));
    let nkeyatts = r.indnkeyatts() as usize;

    let mut identry = [GISTENTRY::default(); K];
    let mut isnull = [false; K];
    gistDeCompressAtt(mcx, giststate, r, it, 0, &mut identry, &mut isnull)?;

    let mut result = FirstOffsetNumber;
    let mut best_penalty = [-1.0f32; K];
    let mut keep_current_best: i32 = -1;
    let maxoff = p.max_offset_number();

    let mut i = FirstOffsetNumber;
    while i <= maxoff {
        let itup = page_item(p, i);
        let mut zero_penalty = true;

        let mut j = 0usize;
        while j < nkeyatts {
            let (datum, is_null) = gist_index_getattr(itup, j + 1, giststate);
            let entry = gistdentryinit(mcx, giststate, j, datum, i, false, false, is_null)?;
            let usize_ = gistpenalty(mcx, giststate, j, &entry, is_null, &identry[j], isnull[j])?;
            if usize_ > 0.0 {
                zero_penalty = false;
            }

            if best_penalty[j] < 0.0 || usize_ < best_penalty[j] {
                result = i;
                best_penalty[j] = usize_;
                if j < nkeyatts - 1 {
                    best_penalty[j + 1] = -1.0;
                }
                keep_current_best = -1;
                j += 1;
            } else if best_penalty[j] == usize_ {
                j += 1;
            } else {
                zero_penalty = false;
                break;
            }
        }

        if j == nkeyatts && result != i {
            if keep_current_best == -1 {
                keep_current_best = pg_prng::global_prng(pg_prng::PgPrng::next_bool) as i32;
            }
            if keep_current_best == 0 {
                result = i;
                keep_current_best = -1;
            }
        }

        if zero_penalty {
            if keep_current_best == -1 {
                keep_current_best = pg_prng::global_prng(pg_prng::PgPrng::next_bool) as i32;
            }
            if keep_current_best == 1 {
                break;
            }
        }

        i += 1;
    }

    Ok(result)
}

/// gistFetchAtt.
fn gistFetchAtt(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    nkey: usize,
    k: Datum,
) -> PgResult<Datum> {
    let fentry = GISTENTRY::init(k, 0, false, false);
    let fep = giststate.call_fetch(mcx, nkey, &fentry)?;
    Ok(fep.key)
}

/// gistFetchTuple, rendered as fetched (Datum, isnull) columns; the scan layer
/// forms an index tuple over fetchTupdesc from them (C forms a heap tuple —
/// observationally identical through StoreIndexTuple's deform).
pub fn gistFetchTupleValues(
    mcx: Mcx<'_>,
    giststate: &mut GistState<'_>,
    r: &Relation<'_>,
    tuple: ITup,
    fetchatt: &mut [Datum],
    isnull: &mut [bool],
) -> PgResult<()> {
    let natts = r.rd_att.natts as usize;
    let nkeyatts = r.indnkeyatts() as usize;

    for i in 0..nkeyatts {
        let (datum, is_null) = gist_index_getattr(tuple, i + 1, giststate);
        isnull[i] = is_null;
        if giststate.has_fetch(i) {
            fetchatt[i] = if !is_null {
                gistFetchAtt(mcx, giststate, i, datum)?
            } else {
                Datum::null()
            };
        } else if !giststate.has_compress(i) {
            fetchatt[i] = if !is_null { datum } else { Datum::null() };
        } else {
            isnull[i] = true;
            fetchatt[i] = Datum::null();
        }
    }
    for i in nkeyatts..natts {
        let (datum, is_null) = gist_index_getattr(tuple, i + 1, giststate);
        fetchatt[i] = datum;
        isnull[i] = is_null;
    }
    Ok(())
}

/// gistNewBuffer: pinned + exclusively locked; caller initializes the page.
pub fn gistNewBuffer<'mcx>(
    r: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
) -> PgResult<BufferPin> {
    loop {
        let blkno = freespace::GetFreeIndexPage(r)?;
        if blkno == InvalidBlockNumber {
            break;
        }

        let pin = BufferPin::adopt(bufmgr::read_buffer::call(r, blkno)?)
            .expect("ReadBuffer returned InvalidBuffer");
        if bufmgr::conditional_lock_buffer::call(pin.buffer())? {
            let page = pin.page();
            if page.is_new() {
                return Ok(pin);
            }
            gistcheckpage(r, &pin)?;
            if gistPageRecyclable(heaprel, &page)? {
                if transam_xlog_seams::xlog_standby_info_active::call()
                    && crate::relation_needs_wal(r)
                {
                    let delete_xid = ::types_gist::GistPageGetDeleteXid(&page);
                    crate::wal::gistXLogPageReuse(r, heaprel, blkno, delete_xid)?;
                }
                return Ok(pin);
            }
            bufmgr::lock_buffer::call(pin.buffer(), bufmgr::BUFFER_LOCK_UNLOCK)?;
        }
        drop(pin);
    }

    let (buf, _extended_by) = bufmgr::extend_buffered_rel_by::call(
        r,
        ForkNumber::MAIN_FORKNUM,
        None,
        bufmgr::EB_LOCK_FIRST,
        1,
    )?;
    Ok(BufferPin::adopt(buf).expect("ExtendBufferedRelBy returned InvalidBuffer"))
}

/// gistPageRecyclable. C passes NULL rel to GlobalVisCheckRemovableFullXid;
/// the seam takes the heap relation, whose horizon is what the deleteXid
/// stamp guards.
pub fn gistPageRecyclable(
    heaprel: &::types_rel::RelationData<'_>,
    page: &PageRef<'_>,
) -> PgResult<bool> {
    if page.is_new() {
        return Ok(true);
    }
    if GistPageIsDeleted(page) {
        let deletexid_full = ::types_gist::GistPageGetDeleteXid(page);
        return procarray_seams::global_vis_check_removable_full_xid::call(
            heaprel,
            deletexid_full,
        );
    }
    Ok(false)
}

// gistGetFakeLSN's static counters (backend-local, matching C's statics).
thread_local! {
    static FAKE_LSN_TEMP_COUNTER: core::cell::Cell<XLogRecPtr> =
        const { core::cell::Cell::new(FirstNormalUnloggedLSN) };
    static FAKE_LSN_LASTLSN: core::cell::Cell<XLogRecPtr> =
        const { core::cell::Cell::new(0) };
}

const FirstNormalUnloggedLSN: XLogRecPtr = 1000;

/// gistGetFakeLSN.
pub fn gistGetFakeLSN(rel: &Relation<'_>) -> PgResult<XLogRecPtr> {
    if rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP {
        return Ok(FAKE_LSN_TEMP_COUNTER.with(|c| {
            let v = c.get();
            c.set(v + 1);
            v
        }));
    }
    if rel.is_permanent() {
        debug_assert!(!crate::relation_needs_wal(rel));
        let mut currlsn = ::transam_xlog::GetXLogInsertRecPtr();
        let lastlsn = FAKE_LSN_LASTLSN.with(|c| c.get());
        if lastlsn != 0 && lastlsn == currlsn {
            currlsn = crate::wal::gistXLogAssignLSN()?;
        }
        FAKE_LSN_LASTLSN.with(|c| c.set(currlsn));
        Ok(currlsn)
    } else {
        // Unlogged relations are visible to other backends and survive clean
        // restarts: the shared counter (GetFakeLSNForUnloggedRel) handles it.
        Ok(::transam_xlog::ctl::GetFakeLSNForUnloggedRel())
    }
}
