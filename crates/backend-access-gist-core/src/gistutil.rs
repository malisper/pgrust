//! GiST opclass-state init and tuple (de)compression from `access/gist/gist.c`
//! (`initGISTstate`) and `access/gist/gistutil.c` (`gistFormTuple` /
//! `gistCompressValues` / `gistDeCompressAtt`).
//!
//! `GISTSTATE` ([`types_gist::GISTSTATE`]) stores, per index column, a resolved
//! [`FmgrInfo`] for each opclass support procedure — exactly the C struct. In
//! the owned typed-seam dispatch model the AM does not call the function pointer
//! itself; instead the opclass installs its typed body into the
//! `backend-access-gist-dispatch-seams` seam keyed on the procedure OID, and the
//! AM dispatches by reading `giststate.<proc>Fn[col].fn_oid` and handing it to
//! the seam. `FmgrInfo` thus serves here as the per-column OID carrier (its
//! `fn_oid` is `InvalidOid` exactly when the opclass omits the optional
//! procedure, matching C's `fmgr_info_copy` vs `fn_oid = InvalidOid` legs).

use alloc::vec::Vec;
use backend_access_common_indextuple_seams::index_form_tuple_desc;
use backend_access_common_tupdesc::CreateTupleDescTruncatedCopy;
use backend_access_gist_dispatch_seams::{gist_compress, gist_decompress};
use backend_access_index_indexam_seams::{index_getprocid, index_getprocinfo};
use backend_utils_error::{ereport, PgResult};
use mcx::{alloc_in, Mcx, PgVec};
use types_error::error::ERROR;
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, InvalidBlockNumber, InvalidOid, Oid};
use types_gist::{
    GISTENTRY, GISTSTATE, GIST_COMPRESS_PROC, GIST_CONSISTENT_PROC, GIST_DECOMPRESS_PROC,
    GIST_DISTANCE_PROC, GIST_EQUAL_PROC, GIST_FETCH_PROC, GIST_PENALTY_PROC, GIST_PICKSPLIT_PROC,
    GIST_UNION_PROC,
};
use types_rel::Relation;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::DEFAULT_COLLATION_OID;

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
