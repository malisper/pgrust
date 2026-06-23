//! GiST opclass-state init and tuple (de)compression from `access/gist/gist.c`
//! (`initGISTstate`) and `access/gist/gistutil.c` (`gistFormTuple` /
//! `gistCompressValues` / `gistDeCompressAtt`).
//!
//! `GISTSTATE` ([`gist::GISTSTATE`]) stores, per index column, a resolved
//! [`FmgrInfo`] for each opclass support procedure — exactly the C struct. In
//! the owned typed-seam dispatch model the AM does not call the function pointer
//! itself; instead the opclass installs its typed body into the
//! `backend-access-gist-dispatch-seams` seam keyed on the procedure OID, and the
//! AM dispatches by reading `giststate.<proc>Fn[col].fn_oid` and handing it to
//! the seam. `FmgrInfo` thus serves here as the per-column OID carrier (its
//! `fn_oid` is `InvalidOid` exactly when the opclass omits the optional
//! procedure, matching C's `fmgr_info_copy` vs `fn_oid = InvalidOid` legs).

use alloc::vec::Vec;
use heaptuple::{heap_form_tuple, FormedTuple};
use indextuple_seams::{index_form_tuple_desc, nocache_index_getattr};
use tupdesc::CreateTupleDescTruncatedCopy;
use dispatch_seams::{
    gist_compress, gist_decompress, gist_fetch, gist_penalty, gist_same, gist_union,
};
use indexam_seams::{index_getprocid, index_getprocinfo};
use bufmgr_seams::{
    buffer_get_page, conditional_lock_buffer, extend_buffered_rel, lock_buffer, read_buffer,
    release_buffer,
};
use page::{
    PageGetFreeSpace, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageIsNew, PageRef,
};
use utils_error::{ereport, PgResult};
use mcx::{alloc_in, Mcx, PgVec};
use types_error::error::ERROR;
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{
    AttrNumber, BlockNumber, ForkNumber, InvalidBlockNumber, InvalidOid, OffsetNumber, Oid,
};
use gist::{
    GistEntryVector, GISTENTRY, GISTSTATE, GIST_COMPRESS_PROC, GIST_CONSISTENT_PROC,
    GIST_DECOMPRESS_PROC, GIST_DISTANCE_PROC, GIST_EQUAL_PROC, GIST_FETCH_PROC, GIST_PENALTY_PROC,
    GIST_PICKSPLIT_PROC, GIST_UNION_PROC,
};
use rel::Relation;
use types_storage::Buffer;
use types_tuple::heaptuple::Datum;
use types_tuple::heaptuple::{DEFAULT_COLLATION_OID, FIRST_OFFSET_NUMBER, INVALID_OFFSET_NUMBER};

use crate::gist_page::{
    gistcheckpage, GistPageGetDeleteXid, GistPageIsDeleted, GiSTPageSize,
};

/// `BUFFER_LOCK_UNLOCK` (bufmgr.h) — the `LockBuffer` "release" mode, which
/// `GIST_UNLOCK` (gist_private.h) aliases.
const BUFFER_LOCK_UNLOCK: i32 = 0;

/// `INDEX_MAX_KEYS` (pg_config_manual.h).
const INDEX_MAX_KEYS: i32 = 32;

/// `OidIsValid(oid)` (postgres_ext.h).
#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `initGISTstate(index)` (gist.c:1537): build the per-column opclass support
/// dispatch state (resolved support-proc OIDs / FmgrInfos) and the leaf /
/// truncated-non-leaf tuple descriptors for a GiST index.
///
/// In C this allocates a `GiST scan context` and `palloc`s the `GISTSTATE`
/// inside it; in the owned model the whole `GISTSTATE` lives in `mcx`
/// (`scanCxt == tempCxt == mcx`, the C "caller must change tempCxt if needed"
/// invariant preserved by `tempCxt` aliasing `scanCxt`).
pub fn initGISTstate<'mcx>(mcx: Mcx<'mcx>, index: &Relation<'mcx>) -> PgResult<GISTSTATE<'mcx>> {
    // safety check to protect fixed-size arrays in GISTSTATE
    let natts = index.rd_att.natts;
    if natts > INDEX_MAX_KEYS {
        return Err(ereport(ERROR)
            .errmsg_internal(alloc::format!(
                "numberOfAttributes {natts} > {INDEX_MAX_KEYS}"
            ))
            .into_error());
    }

    // giststate->leafTupdesc = index->rd_att;
    let leaf_tupdesc = Some(alloc_in(mcx, index.rd_att.clone_in(mcx)?)?);
    // giststate->nonLeafTupdesc = CreateTupleDescTruncatedCopy(index->rd_att,
    //     IndexRelationGetNumberOfKeyAttributes(index));
    let nkeyatts = index.indnkeyatts();
    let non_leaf_tupdesc = Some(alloc_in(
        mcx,
        CreateTupleDescTruncatedCopy(mcx, index.rd_att.as_ref(), nkeyatts)?,
    )?);
    // fetchTupdesc is set later (in gistbeginscan); C leaves it NULL here. We
    // leave the field None so it is well-formed and the scan layer fills it.
    let fetch_tupdesc: types_tuple::heaptuple::TupleDesc<'mcx> = None;

    let n = natts as usize;
    let mut consistent_fn = Vec::with_capacity(n);
    let mut union_fn = Vec::with_capacity(n);
    let mut compress_fn = Vec::with_capacity(n);
    let mut decompress_fn = Vec::with_capacity(n);
    let mut penalty_fn = Vec::with_capacity(n);
    let mut picksplit_fn = Vec::with_capacity(n);
    let mut equal_fn = Vec::with_capacity(n);
    let mut distance_fn = Vec::with_capacity(n);
    let mut fetch_fn = Vec::with_capacity(n);
    let mut support_collation = Vec::with_capacity(n);

    let nkeys = nkeyatts as usize;
    for i in 0..n {
        if i < nkeys {
            let attno = (i + 1) as AttrNumber;
            // consistentFn / unionFn / penaltyFn / picksplitFn / equalFn are
            // mandatory — index_getprocinfo complains on a missing one.
            consistent_fn.push(index_getprocinfo::call(
                index,
                attno,
                GIST_CONSISTENT_PROC as u16,
            )?);
            union_fn.push(index_getprocinfo::call(index, attno, GIST_UNION_PROC as u16)?);

            // opclasses are not required to provide a Compress method.
            compress_fn.push(optional_proc(index, attno, GIST_COMPRESS_PROC)?);
            // opclasses are not required to provide a Decompress method.
            decompress_fn.push(optional_proc(index, attno, GIST_DECOMPRESS_PROC)?);

            penalty_fn.push(index_getprocinfo::call(index, attno, GIST_PENALTY_PROC as u16)?);
            picksplit_fn.push(index_getprocinfo::call(
                index,
                attno,
                GIST_PICKSPLIT_PROC as u16,
            )?);
            equal_fn.push(index_getprocinfo::call(index, attno, GIST_EQUAL_PROC as u16)?);

            // opclasses are not required to provide a Distance method.
            distance_fn.push(optional_proc(index, attno, GIST_DISTANCE_PROC)?);
            // opclasses are not required to provide a Fetch method.
            fetch_fn.push(optional_proc(index, attno, GIST_FETCH_PROC)?);

            // If the index column has a specified collation honor it, else the
            // default collation (harmless if the support fn ignores collation).
            let collation = index.rd_indcollation.get(i).copied().unwrap_or(InvalidOid);
            support_collation.push(if OidIsValid(collation) {
                collation
            } else {
                DEFAULT_COLLATION_OID
            });
        } else {
            // No opclass information for INCLUDE attributes.
            consistent_fn.push(FmgrInfo::empty());
            union_fn.push(FmgrInfo::empty());
            compress_fn.push(FmgrInfo::empty());
            decompress_fn.push(FmgrInfo::empty());
            penalty_fn.push(FmgrInfo::empty());
            picksplit_fn.push(FmgrInfo::empty());
            equal_fn.push(FmgrInfo::empty());
            distance_fn.push(FmgrInfo::empty());
            fetch_fn.push(FmgrInfo::empty());
            support_collation.push(InvalidOid);
        }
    }

    Ok(GISTSTATE {
        scanCxt: mcx,
        tempCxt: mcx,
        leafTupdesc: leaf_tupdesc,
        nonLeafTupdesc: non_leaf_tupdesc,
        fetchTupdesc: fetch_tupdesc,
        consistentFn: consistent_fn,
        unionFn: union_fn,
        compressFn: compress_fn,
        decompressFn: decompress_fn,
        penaltyFn: penalty_fn,
        picksplitFn: picksplit_fn,
        equalFn: equal_fn,
        distanceFn: distance_fn,
        fetchFn: fetch_fn,
        supportCollation: support_collation,
    })
}

/// The C
/// ```c
/// if (OidIsValid(index_getprocid(index, attno, proc)))
///     fmgr_info_copy(&giststate->fooFn[i], index_getprocinfo(...), scanCxt);
/// else
///     giststate->fooFn[i].fn_oid = InvalidOid;
/// ```
/// leg for an optional support procedure.
fn optional_proc<'mcx>(
    index: &Relation<'mcx>,
    attno: AttrNumber,
    proc: i32,
) -> PgResult<FmgrInfo> {
    if OidIsValid(index_getprocid::call(index, attno, proc as u16)?) {
        index_getprocinfo::call(index, attno, proc as u16)
    } else {
        Ok(FmgrInfo::empty())
    }
}

/// `gistFormTuple(giststate, r, attdata, isnull, isleaf)` (gistutil.c:574):
/// compress each attribute and form an index tuple over the leaf or truncated
/// non-leaf descriptor. The returned on-disk bytes have the offset of `t_tid`
/// set to `0xffff` (the historical "unused on internal pages" sentinel).
pub fn gistFormTuple<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    r: &Relation<'mcx>,
    attdata: &[Datum<'mcx>],
    isnull: &[bool],
    isleaf: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let compatt = gistCompressValues(mcx, giststate, r, attdata, isnull, isleaf)?;

    // res = index_form_tuple(isleaf ? leafTupdesc : nonLeafTupdesc, compatt,
    //                        isnull); both descriptors are populated by
    // initGISTstate, so the Option is always Some here.
    let tupdesc_box = if isleaf {
        giststate.leafTupdesc.as_ref()
    } else {
        giststate.nonLeafTupdesc.as_ref()
    };
    let tupdesc = tupdesc_box
        .expect("gistFormTuple: GISTSTATE tuple descriptor not initialized")
        .as_ref();
    let mut res = index_form_tuple_desc::call(mcx, tupdesc, &compatt, isnull)?;

    // The offset number on tuples on internal pages is unused. For historical
    // reasons, it is set to 0xffff. `ItemPointerSetOffsetNumber(&res->t_tid,
    // 0xffff)`: t_tid is the leading `ItemPointerData` of the on-disk image
    // (`BlockIdData ip_blkid` [4 bytes] then `OffsetNumber ip_posid` [2 bytes]),
    // so the offset number is the u16 at byte offset 4.
    res[4..6].copy_from_slice(&0xffffu16.to_ne_bytes());
    Ok(res)
}

/// `gistCompressValues(giststate, r, attdata, isnull, isleaf, compatt)`
/// (gistutil.c:595): call the compress method on each key attribute, producing
/// the compressed [`Datum`] for each column (NULLs pass through as `(Datum) 0`).
/// INCLUDE attributes are emplaced verbatim.
pub fn gistCompressValues<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    r: &Relation<'mcx>,
    attdata: &[Datum<'mcx>],
    isnull: &[bool],
    isleaf: bool,
) -> PgResult<Vec<Datum<'mcx>>> {
    let natts = r.rd_att.natts as usize;
    let nkeyatts = r.indnkeyatts() as usize;
    let mut compatt: Vec<Datum<'mcx>> = Vec::with_capacity(natts);

    // Call the compress method on each key attribute.
    for i in 0..nkeyatts {
        if isnull[i] {
            // compatt[i] = (Datum) 0;
            compatt.push(Datum::ByVal(0));
        } else {
            // GISTENTRY centry; gistentryinit(centry, attdata[i], r, NULL, 0, isleaf);
            let centry = GISTENTRY {
                key: attdata[i].clone_in(mcx)?,
                rel: r.rd_id,
                page: InvalidBlockNumber,
                offset: 0,
                leafkey: isleaf,
            };
            // there may not be a compress function in opclass
            let proc_oid = giststate.compressFn[i].fn_oid;
            if OidIsValid(proc_oid) {
                let cep = gist_compress::call(
                    mcx,
                    proc_oid,
                    giststate.supportCollation[i],
                    &centry,
                )?;
                compatt.push(cep.key.clone_in(mcx)?);
            } else {
                // cep = &centry; compatt[i] = cep->key;
                compatt.push(centry.key);
            }
        }
    }

    // Emplace each included attribute if any.
    if isleaf {
        for i in nkeyatts..natts {
            compatt.push(attdata[i].clone_in(mcx)?);
        }
    } else {
        // Internal tuples are formed over nonLeafTupdesc, which has only the
        // key attributes; no INCLUDE columns to emplace.
    }

    Ok(compatt)
}

/// `gistDeCompressAtt(giststate, r, tuple, p, o, attdata, isnull)`
/// (gistutil.c:296), per-attribute body (`gistdentryinit`, gistutil.c:547):
/// initialize a [`GISTENTRY`] from a stored key, running the opclass decompress
/// method when present.
///
/// The full C `gistDeCompressAtt` deforms a stored `IndexTuple` against the
/// index descriptor; that deform belongs to the scan/insert layer (the on-disk
/// `IndexTuple` is reached there). This function provides the per-key
/// `gistdentryinit` core — given a key `Datum` (or NULL) it returns the
/// decompressed entry — which the scan/insert layer drives once per attribute.
pub fn gistdentryinit<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    nkey: usize,
    k: Datum<'mcx>,
    rel: Oid,
    page: types_core::primitive::BlockNumber,
    o: u16,
    leaf: bool,
    is_null: bool,
) -> PgResult<GISTENTRY<'mcx>> {
    if !is_null {
        // gistentryinit(*e, k, r, pg, o, l);
        let e = GISTENTRY {
            key: k,
            rel,
            page,
            offset: o,
            leafkey: leaf,
        };
        // there may not be a decompress function in opclass
        let proc_oid = giststate.decompressFn[nkey].fn_oid;
        if !OidIsValid(proc_oid) {
            return Ok(e);
        }
        // dep = decompressFn(e); if (dep != e) gistentryinit(*e, dep->...);
        let dep = gist_decompress::call(mcx, proc_oid, giststate.supportCollation[nkey], &e)?;
        Ok(GISTENTRY {
            key: dep.key.clone_in(mcx)?,
            rel: dep.rel,
            page: dep.page,
            offset: dep.offset,
            leafkey: dep.leafkey,
        })
    } else {
        // gistentryinit(*e, (Datum) 0, r, pg, o, l);
        Ok(GISTENTRY {
            key: Datum::ByVal(0),
            rel,
            page,
            offset: o,
            leafkey: leaf,
        })
    }
}

// ===========================================================================
// Insertion-helper layer (gistutil.c). The gist.c insert spine drives these
// over on-disk index tuples it reads from pages via PageGetItem; in the owned
// model an on-disk `IndexTuple` is its contiguous byte image (`&[u8]`), exactly
// what `index_form_tuple` produces and `nocache_index_getattr` consumes.
// ===========================================================================

/// `sizeof(ItemIdData)` (storage/itemid.h) — a line-pointer is 4 bytes.
const SIZEOF_ITEM_ID_DATA: usize = 4;

/// `IndexTupleSize(itup)` (access/itup.h) over an on-disk byte image: the size
/// is the low [`INDEX_SIZE_MASK`] bits of `t_info`, the `u16` at byte offset 6.
#[inline]
pub(crate) fn index_tuple_size(itup: &[u8]) -> usize {
    const INDEX_SIZE_MASK: u16 = 0x1fff;
    let t_info = u16::from_ne_bytes([itup[6], itup[7]]);
    (t_info & INDEX_SIZE_MASK) as usize
}

/// `gistjoinvector(itvec, len, additvec, addlen)` (gistutil.c:113): append the
/// index-tuple byte images in `additvec` to `itvec`. In C this `repalloc`s the
/// pointer array; in the owned model the vector grows in place, so this is a
/// plain extend (each appended tuple is the caller's byte image).
pub fn gistjoinvector<'mcx>(
    itvec: &mut Vec<PgVec<'mcx, u8>>,
    additvec: &[PgVec<'mcx, u8>],
) -> PgResult<()> {
    for it in additvec {
        let mcx = *it.allocator();
        itvec.push(mcx::slice_in(mcx, it)?);
    }
    Ok(())
}

/// `gistfillitupvec(vec, veclen, &memlen)` (gistutil.c:126): flatten an array of
/// on-disk index tuples into a single contiguous byte buffer (their concatenated
/// images). Returns the buffer; its length is the C `*memlen`.
pub fn gistfillitupvec<'mcx>(mcx: Mcx<'mcx>, vec: &[&[u8]]) -> PgResult<PgVec<'mcx, u8>> {
    let mut memlen = 0usize;
    for it in vec {
        memlen += index_tuple_size(it);
    }
    let mut out = mcx::vec_with_capacity_in(mcx, memlen)?;
    for it in vec {
        let sz = index_tuple_size(it);
        out.extend_from_slice(&it[..sz]);
    }
    Ok(out)
}

/// `GistTupleSetValid(itup)` (gist_private.h): clear the `GIST_TRUNCATED` /
/// invalid marking — set the offset of `t_tid` to `TUPLE_IS_VALID` (`0xffff`).
/// `t_tid`'s `ip_posid` is the `u16` at byte offset 4 of the on-disk image.
#[inline]
pub(crate) fn gist_tuple_set_valid(itup: &mut [u8]) {
    itup[4..6].copy_from_slice(&gist::TUPLE_IS_VALID.to_ne_bytes());
}

/// `GistTupleIsInvalid(itup)` (gist_private.h): is the tuple's `t_tid` offset the
/// invalid sentinel (`TUPLE_IS_INVALID`)?
#[inline]
pub(crate) fn gist_tuple_is_invalid(itup: &[u8]) -> bool {
    u16::from_ne_bytes([itup[4], itup[5]]) == gist::TUPLE_IS_INVALID
}

/// `ItemPointerGetBlockNumber(&itup->t_tid)` over an on-disk image: `t_tid` is
/// the leading `ItemPointerData` (`BlockIdData ip_blkid` [4 bytes] then
/// `OffsetNumber ip_posid` [2 bytes]); the block number is the `BlockIdData`,
/// stored as two big-to-low `u16` halves (`bi_hi`, `bi_lo`) at bytes 0..4.
#[inline]
pub(crate) fn itup_block_number(itup: &[u8]) -> BlockNumber {
    let bi_hi = u16::from_ne_bytes([itup[0], itup[1]]) as u32;
    let bi_lo = u16::from_ne_bytes([itup[2], itup[3]]) as u32;
    (bi_hi << 16) | bi_lo
}

/// `itup->t_tid` over an on-disk image: the full leading `ItemPointerData`
/// (`BlockIdData ip_blkid` [bytes 0..4] then `OffsetNumber ip_posid`
/// [bytes 4..6]). Used by the scan layer to report a matching heap TID.
#[inline]
pub(crate) fn itup_heap_ptr(itup: &[u8]) -> types_tuple::heaptuple::ItemPointerData {
    let blkno = itup_block_number(itup);
    let ip_posid = u16::from_ne_bytes([itup[4], itup[5]]);
    types_tuple::heaptuple::ItemPointerData::new(blkno, ip_posid)
}

/// `ItemPointerSetBlockNumber(&itup->t_tid, blkno)` over an on-disk image: store
/// the block number into the leading `BlockIdData` (`bi_hi`, `bi_lo` halves).
#[inline]
pub(crate) fn itup_set_block_number(itup: &mut [u8], blkno: BlockNumber) {
    let bi_hi = (blkno >> 16) as u16;
    let bi_lo = (blkno & 0xffff) as u16;
    itup[0..2].copy_from_slice(&bi_hi.to_ne_bytes());
    itup[2..4].copy_from_slice(&bi_lo.to_ne_bytes());
}

/// `gistnospace(page, itvec, len, todelete, freespace)` (gistutil.c:58): does the
/// vector of index tuples `itvec` (each an on-disk byte image), plus `freespace`
/// reserved bytes, not fit on `page` after deleting the tuple at `todelete`?
pub fn gistnospace(
    page: &[u8],
    itvec: &[&[u8]],
    todelete: OffsetNumber,
    freespace: usize,
) -> PgResult<bool> {
    let mut size = freespace;
    let mut deleted = 0usize;

    for it in itvec {
        size += index_tuple_size(it) + SIZEOF_ITEM_ID_DATA;
    }

    let pref = PageRef::new(page)?;
    if todelete != INVALID_OFFSET_NUMBER {
        // itup = PageGetItem(page, PageGetItemId(page, todelete));
        let id = PageGetItemId(&pref, todelete)?;
        let itup = PageGetItem(&pref, &id)?;
        deleted = index_tuple_size(itup) + SIZEOF_ITEM_ID_DATA;
    }

    Ok(PageGetFreeSpace(&pref) + deleted < size)
}

/// `gistfitpage(itvec, len)` (gistutil.c:78): does the vector of index tuples
/// `itvec` fit on a single empty GiST page?
pub fn gistfitpage(itvec: &[&[u8]]) -> bool {
    let mut size = 0usize;
    for it in itvec {
        size += index_tuple_size(it) + SIZEOF_ITEM_ID_DATA;
    }
    // TODO (as in C): Consider fillfactor.
    size <= GiSTPageSize
}

/// `gistextractpage(page, &len)` (gistutil.c:94): read every index tuple off
/// `page` into a vector. Each returned element is the on-disk byte image of the
/// tuple at its offset (a copy, since the caller outlives the borrowed page).
pub fn gistextractpage<'mcx>(mcx: Mcx<'mcx>, page: &[u8]) -> PgResult<Vec<PgVec<'mcx, u8>>> {
    let pref = PageRef::new(page)?;
    let maxoff = PageGetMaxOffsetNumber(&pref);
    let mut itvec = Vec::with_capacity(maxoff as usize);
    let mut i = FIRST_OFFSET_NUMBER;
    while i <= maxoff {
        let id = PageGetItemId(&pref, i)?;
        let it = PageGetItem(&pref, &id)?;
        itvec.push(mcx::slice_in(mcx, it)?);
        i += 1;
    }
    Ok(itvec)
}

/// `index_getattr(itup, attnum, leafTupdesc, &isnull)` over an on-disk byte
/// image: extract the 1-based key attribute `attno_1based` from the index tuple
/// `itup`. Returns the canonical `(value, isnull)`.
fn index_getattr<'mcx>(
    mcx: Mcx<'mcx>,
    itup: &[u8],
    attno_1based: i32,
    giststate: &GISTSTATE<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    let leaf = giststate
        .leafTupdesc
        .as_ref()
        .expect("gistutil: GISTSTATE leafTupdesc not initialized")
        .as_ref();
    nocache_index_getattr::call(mcx, itup, attno_1based, leaf)
}

/// Public re-export of [`index_getattr`] for the split layer (gistsplit.c uses
/// `index_getattr(itup, attno, leafTupdesc, &isnull)` directly).
pub fn index_getattr_pub<'mcx>(
    mcx: Mcx<'mcx>,
    itup: &[u8],
    attno_1based: i32,
    giststate: &GISTSTATE<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    index_getattr(mcx, itup, attno_1based, giststate)
}

/// `gistMakeUnionItVec(giststate, itvec, len, attr, isnull)` (gistutil.c:154):
/// make the per-column union of the keys in the index-tuple vector `itvec`. The
/// union `Datum`s and their null flags are returned (one per non-leaf column).
pub fn gistMakeUnionItVec<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    itvec: &[&[u8]],
) -> PgResult<(Vec<Datum<'mcx>>, Vec<bool>)> {
    let non_leaf = giststate
        .nonLeafTupdesc
        .as_ref()
        .expect("gistMakeUnionItVec: GISTSTATE nonLeafTupdesc not initialized")
        .as_ref();
    let ncols = non_leaf.natts as usize;
    let mut attr: Vec<Datum<'mcx>> = Vec::with_capacity(ncols);
    let mut isnull: Vec<bool> = Vec::with_capacity(ncols);

    for i in 0..ncols {
        // Collect non-null datums for this column into an entry vector.
        let mut evec = GistEntryVector {
            n: 0,
            vector: Vec::with_capacity(itvec.len() + 2),
        };
        for it in itvec {
            let (datum, is_null) = index_getattr(mcx, it, (i + 1) as i32, giststate)?;
            if is_null {
                continue;
            }
            // gistdentryinit(giststate, i, evec->vector + evec->n, datum,
            //                NULL, NULL, 0, false, IsNull[=false]);
            let entry = gistdentryinit(
                mcx,
                giststate,
                i,
                datum,
                InvalidOid,
                InvalidBlockNumber,
                0,
                false,
                false,
            )?;
            evec.vector.push(entry);
            evec.n += 1;
        }

        if evec.n == 0 {
            // If this column was all NULLs, the union is NULL.
            attr.push(Datum::ByVal(0));
            isnull.push(true);
        } else {
            if evec.n == 1 {
                // unionFn may expect at least two inputs.
                evec.n = 2;
                let dup = evec.vector[0].clone();
                evec.vector.push(dup);
            }
            // attr[i] = unionFn(evec, &attrsize);
            let proc_oid = giststate.unionFn[i].fn_oid;
            let key = gist_union::call(mcx, proc_oid, giststate.supportCollation[i], &evec)?;
            attr.push(key);
            isnull.push(false);
        }
    }

    Ok((attr, isnull))
}

/// `gistunion(r, itvec, len, giststate)` (gistutil.c:218): the on-disk index
/// tuple (byte image) holding the union of every key in `itvec`.
pub fn gistunion<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    itvec: &[&[u8]],
    giststate: &GISTSTATE<'mcx>,
) -> PgResult<PgVec<'mcx, u8>> {
    let (attr, isnull) = gistMakeUnionItVec(mcx, giststate, itvec)?;
    gistFormTuple(mcx, giststate, r, &attr, &isnull, false)
}

/// `gistMakeUnionKey(giststate, attno, entry1, isnull1, entry2, isnull2, dst,
/// dstisnull)` (gistutil.c:232): the union of two single keys. Returns
/// `(union_datum, isnull)`.
pub fn gistMakeUnionKey<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    attno: usize,
    entry1: &GISTENTRY<'mcx>,
    isnull1: bool,
    entry2: &GISTENTRY<'mcx>,
    isnull2: bool,
) -> PgResult<(Datum<'mcx>, bool)> {
    if isnull1 && isnull2 {
        return Ok((Datum::ByVal(0), true));
    }

    // evec->n = 2; fill vector[0]/vector[1] from the non-null entries.
    let (a, b) = if !isnull1 && !isnull2 {
        (entry1.clone(), entry2.clone())
    } else if !isnull1 {
        (entry1.clone(), entry1.clone())
    } else {
        (entry2.clone(), entry2.clone())
    };
    let evec = GistEntryVector {
        n: 2,
        vector: alloc::vec![a, b],
    };

    let proc_oid = giststate.unionFn[attno].fn_oid;
    let dst = gist_union::call(mcx, proc_oid, giststate.supportCollation[attno], &evec)?;
    Ok((dst, false))
}

/// `gistKeyIsEQ(giststate, attno, a, b)` (gistutil.c:280): whether two index
/// keys for column `attno` are exactly equal (the opclass `same` method).
pub fn gistKeyIsEQ<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    attno: usize,
    a: &Datum<'mcx>,
    b: &Datum<'mcx>,
) -> PgResult<bool> {
    let proc_oid = giststate.equalFn[attno].fn_oid;
    gist_same::call(mcx, proc_oid, giststate.supportCollation[attno], a, b)
}

/// `gistDeCompressAtt(giststate, r, tuple, p, o, attdata, isnull)`
/// (gistutil.c:295): decompress every key attribute of the on-disk index tuple
/// `tuple`, returning the per-column entries and their null flags. `page`/`o`
/// identify the entry's location (`InvalidBlockNumber`/`0` when none).
pub fn gistDeCompressAtt<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    r: &Relation<'mcx>,
    tuple: &[u8],
    page: BlockNumber,
    o: OffsetNumber,
) -> PgResult<(Vec<GISTENTRY<'mcx>>, Vec<bool>)> {
    let nkeyatts = r.indnkeyatts() as usize;
    let mut attdata = Vec::with_capacity(nkeyatts);
    let mut isnull = Vec::with_capacity(nkeyatts);
    for i in 0..nkeyatts {
        let (datum, is_null) = index_getattr(mcx, tuple, (i + 1) as i32, giststate)?;
        isnull.push(is_null);
        let entry = gistdentryinit(mcx, giststate, i, datum, r.rd_id, page, o, false, is_null)?;
        attdata.push(entry);
    }
    Ok((attdata, isnull))
}

/// `gistgetadjusted(r, oldtup, addtup, giststate)` (gistutil.c:315): the union
/// of `oldtup` and `addtup`; `None` (C: `NULL`) when the union equals `oldtup`
/// (no key update needed). On a needed update the returned on-disk tuple carries
/// `oldtup`'s `t_tid`.
pub fn gistgetadjusted<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    oldtup: &[u8],
    addtup: &[u8],
    giststate: &GISTSTATE<'mcx>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let nkeyatts = r.indnkeyatts() as usize;

    let (oldentries, oldisnull) =
        gistDeCompressAtt(mcx, giststate, r, oldtup, InvalidBlockNumber, 0)?;
    let (addentries, addisnull) =
        gistDeCompressAtt(mcx, giststate, r, addtup, InvalidBlockNumber, 0)?;

    let mut attr: Vec<Datum<'mcx>> = Vec::with_capacity(nkeyatts);
    let mut isnull: Vec<bool> = Vec::with_capacity(nkeyatts);
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
        attr.push(un);
        isnull.push(un_isnull);

        if neednew {
            // we already need a new key, so we can skip the check
            continue;
        }
        if isnull[i] {
            // union of key may be NULL iff both keys are NULL
            continue;
        }
        if !addisnull[i]
            && (oldisnull[i]
                || !gistKeyIsEQ(mcx, giststate, i, &oldentries[i].key, &attr[i])?)
        {
            neednew = true;
        }
    }

    if neednew {
        // need to update key: newtup->t_tid = oldtup->t_tid.
        let mut newtup = gistFormTuple(mcx, giststate, r, &attr, &isnull, false)?;
        // t_tid is the leading 6-byte ItemPointerData of the on-disk image.
        newtup[0..6].copy_from_slice(&oldtup[0..6]);
        Ok(Some(newtup))
    } else {
        Ok(None)
    }
}

/// `gistpenalty(giststate, attno, orig, isNullOrig, add, isNullAdd)`
/// (gistutil.c:723): the cost of inserting `add`'s key under `orig`'s key.
/// Negative / NaN penalties are clamped to 0, and a strict penalty method
/// mixing a null with a non-null returns `+infinity`.
pub fn gistpenalty<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    attno: usize,
    orig: &GISTENTRY<'mcx>,
    is_null_orig: bool,
    add: &GISTENTRY<'mcx>,
    is_null_add: bool,
) -> PgResult<f32> {
    let penalty;
    if !giststate.penaltyFn[attno].fn_strict || (!is_null_orig && !is_null_add) {
        let proc_oid = giststate.penaltyFn[attno].fn_oid;
        let p = gist_penalty::call(
            mcx,
            proc_oid,
            giststate.supportCollation[attno],
            orig,
            add,
        )?;
        // disallow negative or NaN penalty
        penalty = if p.is_nan() || p < 0.0 { 0.0 } else { p };
    } else if is_null_orig && is_null_add {
        penalty = 0.0;
    } else {
        // try to prevent mixing null and non-null values
        penalty = f32::INFINITY;
    }
    Ok(penalty)
}

/// `gistchoose(r, p, it, giststate)` (gistutil.c:373): search the upper index
/// page `p` for the entry offset with the lowest penalty for inserting the
/// compressed key in the index tuple `it`.
pub fn gistchoose<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    p: &[u8],
    it: &[u8],
    giststate: &GISTSTATE<'mcx>,
) -> PgResult<OffsetNumber> {
    let nkeyatts = r.indnkeyatts() as usize;

    // gistDeCompressAtt(giststate, r, it, NULL, 0, identry, isnull);
    let (identry, isnull) = gistDeCompressAtt(mcx, giststate, r, it, InvalidBlockNumber, 0)?;

    // we'll return FirstOffsetNumber if page is empty (shouldn't happen)
    let mut result = FIRST_OFFSET_NUMBER;

    // best_penalty[j] is the best penalty for column j, or -1 (unexamined).
    let mut best_penalty = alloc::vec![-1.0f32; nkeyatts.max(1)];
    best_penalty[0] = -1.0;

    // keep_current_best is -1 (no choice yet), 1 (keep), 0 (replace).
    let mut keep_current_best: i32 = -1;

    let pref = PageRef::new(p)?;
    let maxoff = PageGetMaxOffsetNumber(&pref);

    let mut i = FIRST_OFFSET_NUMBER;
    while i <= maxoff {
        let id = PageGetItemId(&pref, i)?;
        let itup = PageGetItem(&pref, &id)?;
        let mut zero_penalty = true;

        let mut j = 0usize;
        while j < nkeyatts {
            // Compute penalty for this column.
            let (datum, is_null) = index_getattr(mcx, itup, (j + 1) as i32, giststate)?;
            let entry = gistdentryinit(mcx, giststate, j, datum, r.rd_id, InvalidBlockNumber, i, false, is_null)?;
            let usize_ = gistpenalty(mcx, giststate, j, &entry, is_null, &identry[j], isnull[j])?;
            if usize_ > 0.0 {
                zero_penalty = false;
            }

            if best_penalty[j] < 0.0 || usize_ < best_penalty[j] {
                // New best penalty for this column.
                result = i;
                best_penalty[j] = usize_;
                if j < nkeyatts - 1 {
                    best_penalty[j + 1] = -1.0;
                }
                // new best, so reset keep-it decision
                keep_current_best = -1;
                j += 1;
            } else if best_penalty[j] == usize_ {
                // Exactly as good for this column; compare the next column.
                j += 1;
            } else {
                // Worse for this column; skip remaining columns, next tuple.
                zero_penalty = false; // so outer loop won't exit
                break;
            }
        }

        // If we looped past the last column and didn't update result, this
        // tuple is exactly as good as the prior best tuple.
        if j == nkeyatts && result != i {
            if keep_current_best == -1 {
                keep_current_best = if prng::global_prng(|s| s.next_bool()) { 1 } else { 0 };
            }
            if keep_current_best == 0 {
                result = i;
                keep_current_best = -1;
            }
        }

        // Zero penalty for all columns + decided not to keep searching => done.
        if zero_penalty {
            if keep_current_best == -1 {
                keep_current_best = if prng::global_prng(|s| s.next_bool()) { 1 } else { 0 };
            }
            if keep_current_best == 1 {
                break;
            }
        }

        i += 1;
    }

    Ok(result)
}

/// `gistFetchAtt(giststate, nkey, k, r)` (gistutil.c:645): reconstruct a single
/// indexed value via the opclass fetch method.
fn gistFetchAtt<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    nkey: usize,
    k: Datum<'mcx>,
    r: &Relation<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // gistentryinit(fentry, k, r, NULL, 0, false);
    let fentry = GISTENTRY {
        key: k,
        rel: r.rd_id,
        page: InvalidBlockNumber,
        offset: 0,
        leafkey: false,
    };
    let proc_oid = giststate.fetchFn[nkey].fn_oid;
    let fep = gist_fetch::call(mcx, proc_oid, giststate.supportCollation[nkey], &fentry)?;
    // fetchFn set 'key', return it to the caller.
    fep.key.clone_in(mcx)
}

/// `gistFetchTuple(giststate, r, tuple)` (gistutil.c:666): reconstruct a heap
/// tuple of the originally-indexed data from the on-disk index tuple `tuple`
/// (index-only scans). The `fetchTupdesc` must have been set (the scan layer
/// fills it in `gistbeginscan`).
pub fn gistFetchTuple<'mcx>(
    mcx: Mcx<'mcx>,
    giststate: &GISTSTATE<'mcx>,
    r: &Relation<'mcx>,
    tuple: &[u8],
) -> PgResult<FormedTuple<'mcx>> {
    let natts = r.rd_att.natts as usize;
    let nkeyatts = r.indnkeyatts() as usize;
    let mut fetchatt: Vec<Datum<'mcx>> = Vec::with_capacity(natts);
    let mut isnull: Vec<bool> = Vec::with_capacity(natts);

    for i in 0..nkeyatts {
        let (datum, is_null) = index_getattr(mcx, tuple, (i + 1) as i32, giststate)?;
        isnull.push(is_null);
        if giststate.fetchFn[i].fn_oid != InvalidOid {
            if !is_null {
                fetchatt.push(gistFetchAtt(mcx, giststate, i, datum, r)?);
            } else {
                fetchatt.push(Datum::ByVal(0));
            }
        } else if giststate.compressFn[i].fn_oid == InvalidOid {
            // No compress method that could change the original value, so the
            // attribute is necessarily stored in original form.
            if !is_null {
                fetchatt.push(datum);
            } else {
                fetchatt.push(Datum::ByVal(0));
            }
        } else {
            // Index-only scans not supported for this column; the planner isn't
            // interested in it, so replace with NULL.
            isnull[i] = true;
            fetchatt.push(Datum::ByVal(0));
        }
    }

    // Get each included attribute, if any.
    for i in nkeyatts..natts {
        let (datum, is_null) = index_getattr(mcx, tuple, (i + 1) as i32, giststate)?;
        fetchatt.push(datum);
        isnull.push(is_null);
    }

    let fetch_tupdesc = giststate
        .fetchTupdesc
        .as_ref()
        .expect("gistFetchTuple: GISTSTATE fetchTupdesc not initialized")
        .as_ref();
    Ok(heap_form_tuple(mcx, fetch_tupdesc, &fetchatt, &isnull)?)
}

/// `gistNewBuffer(r, heaprel)` (gistutil.c:823): allocate a new page (by
/// recycling via the FSM, or by extending the index file). The returned buffer
/// is pinned and exclusive-locked; the caller initializes the page via
/// `GISTInitBuffer`.
pub fn gistNewBuffer<'mcx>(
    mcx: Mcx<'mcx>,
    r: &Relation<'mcx>,
    heaprel: &Relation<'mcx>,
) -> PgResult<Buffer> {
    // First, try to get a page from the FSM.
    loop {
        let blkno = freespace_seams::get_free_index_page::call(r)?;
        if blkno == InvalidBlockNumber {
            break; // nothing left in FSM
        }

        let buffer = read_buffer::call(r, blkno)?;

        // Guard against someone else having recycled the page; it may be locked.
        if conditional_lock_buffer::call(buffer)? {
            let recyclable = {
                let page = buffer_get_page::call(mcx, buffer)?;
                let pref = PageRef::new(&page)?;
                if PageIsNew(&pref) {
                    // Never initialized: OK to use.
                    return Ok(buffer);
                }
                // Check the page looks sane before reading its special area.
                gistcheckpage(r.name(), buffer)?;
                gist_page_recyclable(&page)?
            };

            if recyclable {
                // Recycle a deleted, sufficiently-old page. If WAL is generated
                // for Hot Standby create a conflict record.
                if transam_xlog_seams::xlog_standby_info_active::call()
                    && relcache_seams::relation_needs_wal::call(r)
                {
                    let page = buffer_get_page::call(mcx, buffer)?;
                    let delete_xid = GistPageGetDeleteXid(&page)?;
                    crate::gistxlog::gist_xlog_page_reuse(
                        r,
                        heaprel,
                        bufmgr_seams::buffer_get_block_number::call(buffer),
                        delete_xid,
                    )?;
                }
                return Ok(buffer);
            }

            lock_buffer::call(buffer, BUFFER_LOCK_UNLOCK)?;
        }

        // Can't use it, so release buffer and try again.
        release_buffer::call(buffer);
    }

    // Must extend the file.
    let buffer = extend_buffered_rel::call(r, ForkNumber::MAIN_FORKNUM)?;
    Ok(buffer)
}

/// `gistPageRecyclable(page)` (gistutil.c:887): can this page be recycled yet?
/// A new page always can; a deleted page can once its deletion XID is no longer
/// visible to any in-progress scan.
pub fn gist_page_recyclable(page: &[u8]) -> PgResult<bool> {
    let pref = PageRef::new(page)?;
    if PageIsNew(&pref) {
        return Ok(true);
    }
    if GistPageIsDeleted(page)? {
        // The page was deleted; keep it as a tombstone as long as its deletion
        // XID could still be visible to anyone (a scan might have seen the
        // downlink). GlobalVisCheckRemovableFullXid composes from procarray.
        let deletexid_full = GistPageGetDeleteXid(page)?;
        let state =
            procarray_seams::global_vis_test_for::call(InvalidOid)?;
        return Ok(
            procarray_seams::global_vis_test_is_removable_fullxid::call(
                state,
                deletexid_full,
            ),
        );
    }
    Ok(false)
}

// ===========================================================================
// gistoptions / gistproperty / gisttranslatecmptype (gistutil.c)
// ===========================================================================

/// `gistoptions(reloptions, validate)` (gistutil.c:912) — reloptions processing
/// for GiST. The options are `fillfactor` (int) and `buffering` (enum);
/// delegates to the reloptions owner's `build_reloptions_gist` seam, which
/// carries the `RELOPT_KIND_GIST` parse table.
pub fn gistoptions(
    reloptions: Option<&[u8]>,
    validate: bool,
) -> PgResult<Option<Vec<u8>>> {
    reloptions_seams::build_reloptions_gist::call(reloptions, validate)
}

/// `IndexAMProperty` (amapi.h) — the boolean/text property inquiry kinds. GiST
/// overrides the core property code for `AMPROP_DISTANCE_ORDERABLE` and
/// `AMPROP_RETURNABLE`; every other value is the C `default:` that returns
/// false. Mirrors `backend-access-spgist-core`'s local enum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexAMProperty {
    /// `AMPROP_DISTANCE_ORDERABLE`.
    DistanceOrderable,
    /// `AMPROP_RETURNABLE`.
    Returnable,
    /// Any other `IndexAMProperty` value (the C `default:` case).
    Other,
}

/// `gistproperty(index_oid, attno, prop, propname, res, isnull)`
/// (gistutil.c:933) — check boolean properties of indexes. GiST overrides the
/// core property code for `AMPROP_DISTANCE_ORDERABLE` (which the core does not
/// support) and handles `AMPROP_RETURNABLE` here to save opening the rel to
/// call `gistcanreturn`.
///
/// Returns `(handled, res, isnull)`: `handled` is the C boolean return (whether
/// this routine answered the inquiry), `res`/`isnull` the out-params. The
/// `propname` argument is unused by the C body (only `prop` is consulted), so
/// it is omitted.
pub fn gistproperty(
    index_oid: Oid,
    attno: i32,
    prop: IndexAMProperty,
) -> PgResult<(bool, bool, bool)> {
    // Only answer column-level inquiries.
    if attno == 0 {
        return Ok((false, false, false));
    }

    // Currently, GiST distance-ordered scans require that there be a distance
    // function in the opclass with the default types (i.e. the one loaded into
    // the relcache entry, see initGISTstate). So we assume that if such a
    // function exists, then there's a reason for it. Essentially the same code
    // can test whether we support returning the column data, since that's true
    // if the opclass provides a fetch proc.
    let procno = match prop {
        IndexAMProperty::DistanceOrderable => GIST_DISTANCE_PROC,
        IndexAMProperty::Returnable => GIST_FETCH_PROC,
        IndexAMProperty::Other => return Ok((false, false, false)),
    };

    // First we need to know the column's opclass.
    let opclass = lsyscache_seams::get_index_column_opclass::call(
        index_oid, attno,
    )?;
    if !OidIsValid(opclass) {
        // isnull = true; return true.
        return Ok((true, false, true));
    }

    // Now look up the opclass family and input datatype.
    let (opfamily, opcintype) =
        match lsyscache_seams::get_opclass_opfamily_and_input_type::call(
            opclass,
        )? {
            Some(pair) => pair,
            None => return Ok((true, false, true)),
        };

    // And now we can check whether the function is provided. The C uses
    // `SearchSysCacheExists4(AMPROCNUM, opfamily, opcintype, opcintype, procno)`;
    // `get_opfamily_proc` keys the identical `AMPROCNUM` syscache row and is
    // valid exactly when the support function is registered.
    let mut res = OidIsValid(lsyscache_seams::get_opfamily_proc::call(
        opfamily,
        opcintype,
        opcintype,
        procno as i16,
    )?);

    // Special case: even without a fetch function, AMPROP_RETURNABLE is true if
    // the opclass has no compress function.
    if prop == IndexAMProperty::Returnable && !res {
        res = !OidIsValid(lsyscache_seams::get_opfamily_proc::call(
            opfamily,
            opcintype,
            opcintype,
            GIST_COMPRESS_PROC as i16,
        )?);
    }

    // isnull = false; return true.
    Ok((true, res, false))
}

/// `RTEqualStrategyNumber` (stratnum.h).
const RT_EQUAL_STRATEGY_NUMBER: u16 = 18;
/// `RTLessStrategyNumber` (stratnum.h).
const RT_LESS_STRATEGY_NUMBER: u16 = 20;
/// `RTLessEqualStrategyNumber` (stratnum.h).
const RT_LESS_EQUAL_STRATEGY_NUMBER: u16 = 21;
/// `RTGreaterStrategyNumber` (stratnum.h).
const RT_GREATER_STRATEGY_NUMBER: u16 = 22;
/// `RTGreaterEqualStrategyNumber` (stratnum.h).
const RT_GREATER_EQUAL_STRATEGY_NUMBER: u16 = 23;
/// `RTOverlapStrategyNumber` (stratnum.h).
const RT_OVERLAP_STRATEGY_NUMBER: u16 = 3;
/// `RTContainedByStrategyNumber` (stratnum.h).
const RT_CONTAINED_BY_STRATEGY_NUMBER: u16 = 8;
/// `InvalidStrategy` (stratnum.h).
const INVALID_STRATEGY: u16 = 0;

/// `GIST_TRANSLATE_CMPTYPE_PROC` (gist.h) — support function 12.
const GIST_TRANSLATE_CMPTYPE_PROC: i16 = 12;
/// `ANYOID` (pg_type.h).
const ANYOID: Oid = 2276;

/// `gist_translate_cmptype_common(cmptype)` (gistutil.c:1064) — the built-in
/// stratnum translation support function for GiST opclasses that use the
/// `RT*StrategyNumber` constants. Maps a [`CompareType`] to its R-tree strategy
/// number, or `InvalidStrategy` for anything outside the recognized set. C
/// returns the result as a `uint16` Datum.
pub fn gist_translate_cmptype_common(cmptype: i32) -> u16 {
    use types_tableam::amapi::CompareType;
    if cmptype == CompareType::COMPARE_EQ as i32 {
        RT_EQUAL_STRATEGY_NUMBER
    } else if cmptype == CompareType::COMPARE_LT as i32 {
        RT_LESS_STRATEGY_NUMBER
    } else if cmptype == CompareType::COMPARE_LE as i32 {
        RT_LESS_EQUAL_STRATEGY_NUMBER
    } else if cmptype == CompareType::COMPARE_GT as i32 {
        RT_GREATER_STRATEGY_NUMBER
    } else if cmptype == CompareType::COMPARE_GE as i32 {
        RT_GREATER_EQUAL_STRATEGY_NUMBER
    } else if cmptype == CompareType::COMPARE_OVERLAP as i32 {
        RT_OVERLAP_STRATEGY_NUMBER
    } else if cmptype == CompareType::COMPARE_CONTAINED_BY as i32 {
        RT_CONTAINED_BY_STRATEGY_NUMBER
    } else {
        INVALID_STRATEGY
    }
}

/// `gisttranslatecmptype(cmptype, opfamily)` (gistutil.c:1097) — return the
/// opclass's private stratnum used for the given compare type, by calling the
/// opclass's `GIST_TRANSLATE_CMPTYPE_PROC` support function (if any). Returns
/// `InvalidStrategy` if the function is not defined. The fmgr invocation needs
/// the current context for any allocations and can `ereport`, so this carries
/// `Mcx`/`PgResult` (the `amtranslatecmptype` vtable slot, infallible+context-free,
/// cannot host it; GiST is dispatched by name).
pub fn gisttranslatecmptype<'mcx>(
    mcx: Mcx<'mcx>,
    cmptype: i32,
    opfamily: Oid,
) -> PgResult<u16> {
    // Check whether the function is provided.
    let funcid = lsyscache_seams::get_opfamily_proc::call(
        opfamily,
        ANYOID,
        ANYOID,
        GIST_TRANSLATE_CMPTYPE_PROC,
    )?;
    if !OidIsValid(funcid) {
        return Ok(INVALID_STRATEGY);
    }

    // Ask the translation function:
    // OidFunctionCall1Coll(funcid, InvalidOid, Int32GetDatum(cmptype)).
    let result = fmgr_seams::function_call1_coll_datum::call(
        mcx,
        funcid,
        InvalidOid,
        Datum::from_i32(cmptype),
    )?;
    Ok(result.as_u16())
}
