use core::cell::RefCell;
use core::mem::ManuallyDrop;

use cache_syscache::{
    ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheGetAttrNotNull, SysCacheKey,
    AMOID, INDEXRELID,
};
use datum::Datum;
use mcx::{Mcx, MemoryContext, PgHashMap, PgVec};
use relcache::schemapg::{
    ACCESS_METHOD_PROCEDURE_INDEX_ID, ACCESS_METHOD_PROCEDURE_RELATION_ID, OPCLASS_OID_INDEX_ID,
    OPERATOR_CLASS_RELATION_ID,
};
use relcache_build_seams::{IndexAccessInfo, PgIndexListShape};
use types_core::{AttrNumber, InvalidOid, Oid, INDEX_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERRCODE_INTERNAL_ERROR};
use types_tuple::HeapTupleData;
use types_rel::{AccessShareLock, FormData_pg_class, FormData_pg_index};

use crate::{int2vector_values, oid_key, oidvector_values, req};

const INDEX_INDRELID_INDEX_ID: Oid = 2678;
const OID_BTREE_OPS_OID: Oid = 1981;
const INT2_BTREE_OPS_OID: Oid = 1979;
const BRIN_AM_OID: Oid = 3580;
const SPGIST_AM_OID: Oid = 4000;
const GIST_AM_OID: Oid = 783;
const BTNProcs: usize = 6;
const GISTNProcs: usize = 12;
const SPGISTNProcs: usize = 7;
// BRIN_LAST_OPTIONAL_PROCNUM (brin_internal.h).
const BRINNProcs: usize = 15;
const MAX_AM_PROCS: usize = BRINNProcs;
// BTORDER_PROC == HASHSTANDARD_PROC == 1: slot 0 is the preloaded proc for
// btree and hash.
const BTORDER_PROC: usize = 1;
const HASHNProcs: usize = 3;
const GIN_AM_OID: Oid = 2742;
const GINNProcs: usize = 7;

const Anum_pg_amproc_amprocfamily: i32 = 2;
const Anum_pg_amproc_amproclefttype: i32 = 3;
const Anum_pg_amproc_amprocrighttype: i32 = 4;
const Anum_pg_amproc_amprocnum: i32 = 5;
const Anum_pg_amproc_amproc: i32 = 6;

const Anum_pg_am_amhandler: i32 = 3;

const Anum_pg_index_indexrelid: i32 = 1;
const Anum_pg_index_indrelid: i32 = 2;
const Anum_pg_index_indnatts: i32 = 3;
const Anum_pg_index_indnkeyatts: i32 = 4;
const Anum_pg_index_indisunique: i32 = 5;
const Anum_pg_index_indnullsnotdistinct: i32 = 6;
const Anum_pg_index_indisprimary: i32 = 7;
const Anum_pg_index_indisexclusion: i32 = 8;
const Anum_pg_index_indimmediate: i32 = 9;
const Anum_pg_index_indisvalid: i32 = 11;
const Anum_pg_index_indcheckxmin: i32 = 12;
const Anum_pg_index_indisready: i32 = 13;
const Anum_pg_index_indislive: i32 = 14;
const Anum_pg_index_indisreplident: i32 = 15;
const Anum_pg_index_indkey: i32 = 16;
const Anum_pg_index_indcollation: i32 = 17;
const Anum_pg_index_indclass: i32 = 18;
const Anum_pg_index_indoption: i32 = 19;
const Anum_pg_index_indexprs: i32 = 20;
const Anum_pg_index_indpred: i32 = 21;

const Anum_pg_opclass_opcfamily: i32 = 6;
const Anum_pg_opclass_opcintype: i32 = 7;

// RelationInitIndexAccessInfo (relcache.c). C resolves rd_amhandler from the
// pg_am row (relcache.c:1484-1487) and rd_indam through it
// (InitIndexAmRoutine, relcache.c:1432 GetIndexAmRoutine) — done the same way
// below, so a relam with no pg_am row or a handler that is not an index AM
// is C's catchable XX000, never a panic. The rd_support array is preloaded
// here (rd_supportinfo resolves lazily per key column), so the pg_amproc
// scan is structurally subsumed.
pub(crate) fn relation_init_index_access_info(
    mcx: Mcx<'static>,
    relid: Oid,
    form: &FormData_pg_class,
    relnatts: i16,
) -> PgResult<IndexAccessInfo> {
    let Some(tup) = SearchSysCache1(INDEXRELID, SysCacheKey::Value(Datum::from_oid(relid)))?
    else {
        return Err(cache_lookup_failed(relid));
    };
    // relcache.c:1483-1487: the index's pg_am row must exist -- a relam
    // with no pg_am row is elog(ERROR, "cache lookup failed for access
    // method %u"), never a closed-set panic -- and rd_amhandler is the row's
    // amhandler. The AM is then dispatched on that handler
    // (InitIndexAmRoutine, relcache.c:1432 GetIndexAmRoutine), never on the
    // relam OID, so a pg_am row pointing at a non-handler proc errors like C.
    let am_kind = match SearchSysCache1(AMOID, SysCacheKey::Value(Datum::from_oid(form.relam)))? {
        Some(amtup) => {
            let amhandler = SysCacheGetAttrNotNull(AMOID, &amtup, Anum_pg_am_amhandler);
            ReleaseSysCache(amtup);
            amhandler.map(|d| d.as_oid()).and_then(amapi::GetIndexAmRoutine)
        }
        None => Err(access_method_lookup_failed(form.relam)),
    };
    let am_kind = match am_kind {
        Ok(k) => k,
        Err(e) => {
            ReleaseSysCache(tup);
            return Err(e);
        }
    };
    let get = |attno: i32| -> PgResult<Datum> {
        let (d, isnull) = SysCacheGetAttr(INDEXRELID, &tup, attno)?;
        if isnull {
            return Err(unexpected_null_pg_index(relid, attno));
        }
        Ok(d)
    };

    let indnatts = get(Anum_pg_index_indnatts)?.as_i16();
    // relcache.c:1492-1495: the pg_class row's relnatts must agree with
    // pg_index.indnatts before any vector is sliced by it.
    if relnatts != indnatts {
        ReleaseSysCache(tup);
        return Err(relnatts_disagrees(relid));
    }
    let indnkeyatts = get(Anum_pg_index_indnkeyatts)?.as_i16();
    let nkey = indnkeyatts as usize;

    let mut indkey: PgVec<'static, AttrNumber> =
        mcx::vec_with_capacity_in(mcx, indnatts as usize)?;
    // SAFETY: not-null plain-storage vector columns of the held syscache tuple.
    let (keyvals, collvals, classvals, optvals) = unsafe {
        (
            int2vector_values(get(Anum_pg_index_indkey)?),
            oidvector_values(get(Anum_pg_index_indcollation)?),
            oidvector_values(get(Anum_pg_index_indclass)?),
            int2vector_values(get(Anum_pg_index_indoption)?),
        )
    };
    indkey.extend_from_slice(keyvals);
    // C reads indclass/indcollation/indoption[0..indnkeyatts) straight off the
    // fixed-width vectors; a pg_index row whose vectors are shorter than its
    // indnkeyatts is a bogus row (C would read past them), never a slice panic.
    if nkey > classvals.len() || nkey > collvals.len() || nkey > optvals.len() {
        ReleaseSysCache(tup);
        return Err(bogus_pg_index());
    }

    // amroutine->amsupport per handler.
    let amsupport = match am_kind {
        types_relscan::IndexAmKind::Btree => BTNProcs,
        types_relscan::IndexAmKind::Hash => HASHNProcs,
        types_relscan::IndexAmKind::Gin => GINNProcs,
        types_relscan::IndexAmKind::Gist => GISTNProcs,
        types_relscan::IndexAmKind::Spgist => SPGISTNProcs,
        types_relscan::IndexAmKind::Brin => BRINNProcs,
        // pgvector hnsw: distance, norm, type-info.
        types_relscan::IndexAmKind::Hnsw => 3,
        // contrib/bloom: hash proc + options proc (BLOOM_NPROC).
        types_relscan::IndexAmKind::Bloom => 2,
        #[allow(unreachable_patterns)]
        other => panic!("relcache_build: index AM kind {other:?} for index {relid} unported"),
    };
    let mut opfamily: PgVec<'static, Oid> = mcx::vec_with_capacity_in(mcx, nkey)?;
    let mut opcintype: PgVec<'static, Oid> = mcx::vec_with_capacity_in(mcx, nkey)?;
    let mut supportinfo: Vec<Option<types_fmgr::FmgrInfo>> = Vec::with_capacity(nkey);
    // C rd_support: nkey x amsupport proc OIDs, row-major.
    let mut support: PgVec<'static, Oid> = mcx::vec_with_capacity_in(mcx, nkey * amsupport)?;
    for &opc in &classvals[..nkey] {
        if opc == InvalidOid {
            return Err(bogus_pg_index());
        }
        let ent = lookup_opclass_info(opc, amsupport)?;
        opfamily.push(ent.opcfamily);
        opcintype.push(ent.opcintype);
        support.extend_from_slice(&ent.support[..amsupport]);
        // slot-0 FmgrInfo preload: BTORDER_PROC == HASHSTANDARD_PROC == 1; gin
        // dispatches its support procs by OID (rule-4 closed set); gist
        // resolves its procs in initGISTstate; brin via lsyscache at use.
        let proc = if form.relam == GIN_AM_OID
            || form.relam == GIST_AM_OID
            || form.relam == SPGIST_AM_OID
            || form.relam == BRIN_AM_OID
            || am_kind == types_relscan::IndexAmKind::Hnsw
            || am_kind == types_relscan::IndexAmKind::Bloom
        {
            0
        } else {
            ent.support[BTORDER_PROC - 1]
        };
        supportinfo.push(if proc != 0 {
            Some(fmgr_seams::fmgr_info::call(proc)?)
        } else {
            None
        });
    }
    let mut indcollation: PgVec<'static, Oid> = mcx::vec_with_capacity_in(mcx, nkey)?;
    indcollation.extend_from_slice(&collvals[..nkey]);
    let mut indoption: PgVec<'static, i16> = mcx::vec_with_capacity_in(mcx, nkey)?;
    indoption.extend_from_slice(&optvals[..nkey]);

    let index = FormData_pg_index {
        indexrelid: get(Anum_pg_index_indexrelid)?.as_oid(),
        indrelid: get(Anum_pg_index_indrelid)?.as_oid(),
        indnatts,
        indnkeyatts,
        indisunique: get(Anum_pg_index_indisunique)?.as_bool(),
        indnullsnotdistinct: get(Anum_pg_index_indnullsnotdistinct)?.as_bool(),
        indisprimary: get(Anum_pg_index_indisprimary)?.as_bool(),
        indisexclusion: get(Anum_pg_index_indisexclusion)?.as_bool(),
        indimmediate: get(Anum_pg_index_indimmediate)?.as_bool(),
        indisvalid: get(Anum_pg_index_indisvalid)?.as_bool(),
        indisready: get(Anum_pg_index_indisready)?.as_bool(),
        indcheckxmin: get(Anum_pg_index_indcheckxmin)?.as_bool(),
        // relcache.c:1475: rd_indextuple is the syscache tuple; its xmin is
        // the indcheckxmin horizon (plancat.c:281).
        indxmin: tup.tuple().t_data().xmin(),
        indkey,
        has_indpred: !SysCacheGetAttr(INDEXRELID, &tup, Anum_pg_index_indpred)?.1,
        indexprs_src: {
            let (d, isnull) = SysCacheGetAttr(INDEXRELID, &tup, Anum_pg_index_indexprs)?;
            if isnull {
                None
            } else {
                // Bound the pg_node_tree varlena against the cached tuple's
                // extent before text_str dereferences its (untrusted) header.
                checked_varlena_size(&tup.tuple(), d)?;
                Some(crate::attrs::text_str(mcx, mcx, d)?)
            }
        },
        indpred_src: {
            let (d, isnull) = SysCacheGetAttr(INDEXRELID, &tup, Anum_pg_index_indpred)?;
            if isnull {
                None
            } else {
                checked_varlena_size(&tup.tuple(), d)?;
                Some(crate::attrs::text_str(mcx, mcx, d)?)
            }
        },
    };
    ReleaseSysCache(tup);

    Ok(IndexAccessInfo { index, opcintype, opfamily, indoption, indcollation, supportinfo, support })
}

#[derive(Clone, Copy)]
struct OpClassEnt {
    opcfamily: Oid,
    opcintype: Oid,
    support: [types_core::primitive::RegProcedure; MAX_AM_PROCS],
}

thread_local! {
    // C's OpClassCache: filled once per opclass, never flushed.
    static OPCLASS_CACHE: RefCell<Option<ManuallyDrop<PgHashMap<'static, Oid, OpClassEnt>>>> =
        const { RefCell::new(None) };
}

fn lookup_opclass_info(opc: Oid, amsupport: usize) -> PgResult<OpClassEnt> {
    let hit = OPCLASS_CACHE.with(|c| c.borrow().as_ref().and_then(|m| m.get(&opc).copied()));
    if let Some(ent) = hit {
        return Ok(ent);
    }
    let ent = load_opclass(opc, amsupport)?;
    OPCLASS_CACHE.with(|c| {
        let mut slot = c.borrow_mut();
        let m = slot.get_or_insert_with(|| {
            let mcx = ::mcx::session_root("OpClassCache").mcx();
            ManuallyDrop::new(PgHashMap::with_capacity_in(64, mcx))
        });
        m.insert(opc, ent);
    });
    Ok(ent)
}

fn load_opclass(opc: Oid, amsupport: usize) -> PgResult<OpClassEnt> {
    debug_assert!(amsupport <= MAX_AM_PROCS);
    let cx = MemoryContext::new("LookupOpclassInfo");
    let mcx = cx.mcx();
    let rel = table::table_open(mcx, OPERATOR_CLASS_RELATION_ID, AccessShareLock)?;
    let keys = [oid_key(1, opc)];
    // Heap-fallback for the opclasses the critical indexes themselves use,
    // or the load would recurse into the index it is building (C's guard).
    let index_ok = relcache::criticalRelcachesBuilt()
        || (opc != OID_BTREE_OPS_OID && opc != INT2_BTREE_OPS_OID);
    let mut scan =
        genam::systable_beginscan(mcx, &rel, OPCLASS_OID_INDEX_ID, index_ok, None, &keys)?;
    let mut ent = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => OpClassEnt {
            opcfamily: req(rel.descr(), tup, Anum_pg_opclass_opcfamily)?.as_oid(),
            opcintype: req(rel.descr(), tup, Anum_pg_opclass_opcintype)?.as_oid(),
            support: [0; MAX_AM_PROCS],
        },
        None => return Err(opclass_not_found(opc)),
    };
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;

    // C fetches only the default support procs (lefttype = righttype = opcintype).
    let rel = table::table_open(mcx, ACCESS_METHOD_PROCEDURE_RELATION_ID, AccessShareLock)?;
    let keys = [
        oid_key(Anum_pg_amproc_amprocfamily, ent.opcfamily),
        oid_key(Anum_pg_amproc_amproclefttype, ent.opcintype),
        oid_key(Anum_pg_amproc_amprocrighttype, ent.opcintype),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        ACCESS_METHOD_PROCEDURE_INDEX_ID,
        index_ok,
        None,
        &keys,
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let num = req(rel.descr(), tup, Anum_pg_amproc_amprocnum)?.as_i16();
        if num <= 0 || num as usize > amsupport {
            return Err(invalid_amproc(num, opc));
        }
        ent.support[num as usize - 1] = req(rel.descr(), tup, Anum_pg_amproc_amproc)?.as_oid();
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(ent)
}

// The pg_index half of RelationGetIndexList (relcache.c:4836).
pub(crate) fn scan_pg_index_shapes<'mcx>(
    mcx: Mcx<'mcx>,
    indrelid: Oid,
) -> PgResult<PgVec<'mcx, PgIndexListShape>> {
    let cx = MemoryContext::new("RelationGetIndexList");
    let smcx = cx.mcx();
    let rel = table::table_open(smcx, INDEX_RELATION_ID, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_index_indrelid, indrelid)];
    let mut scan =
        genam::systable_beginscan(smcx, &rel, INDEX_INDRELID_INDEX_ID, true, None, &keys)?;
    let mut out: PgVec<'mcx, PgIndexListShape> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(smcx, &mut scan)? {
        let td = rel.descr();
        out.push(PgIndexListShape {
            indexrelid: req(td, tup, Anum_pg_index_indexrelid)?.as_oid(),
            indislive: req(td, tup, Anum_pg_index_indislive)?.as_bool(),
            indisunique: req(td, tup, Anum_pg_index_indisunique)?.as_bool(),
            indisprimary: req(td, tup, Anum_pg_index_indisprimary)?.as_bool(),
            indimmediate: req(td, tup, Anum_pg_index_indimmediate)?.as_bool(),
            indisvalid: req(td, tup, Anum_pg_index_indisvalid)?.as_bool(),
            indisreplident: req(td, tup, Anum_pg_index_indisreplident)?.as_bool(),
            has_indpred: !crate::getattr(td, tup, Anum_pg_index_indpred).1,
        });
    }
    genam::systable_endscan(smcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(out)
}

const STATISTIC_EXT_RELATION_ID: Oid = 3381;
const STATISTIC_EXT_RELID_INDEX_ID: Oid = 3379;
const Anum_pg_statistic_ext_oid: i32 = 1;
const Anum_pg_statistic_ext_stxrelid: i32 = 2;

pub(crate) fn scan_pg_statistic_ext_oids<'mcx>(
    mcx: Mcx<'mcx>,
    stxrelid: Oid,
) -> PgResult<PgVec<'mcx, Oid>> {
    let cx = MemoryContext::new("RelationGetStatExtList");
    let smcx = cx.mcx();
    let rel = table::table_open(smcx, STATISTIC_EXT_RELATION_ID, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_statistic_ext_stxrelid, stxrelid)];
    let mut scan =
        genam::systable_beginscan(smcx, &rel, STATISTIC_EXT_RELID_INDEX_ID, true, None, &keys)?;
    let mut out: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(smcx, &mut scan)? {
        out.push(req(rel.descr(), tup, Anum_pg_statistic_ext_oid)?.as_oid());
    }
    genam::systable_endscan(smcx, scan)?;
    rel.close(AccessShareLock)?;
    out.sort_unstable();
    Ok(out)
}

#[track_caller]
#[cold]
#[inline(never)]
fn cache_lookup_failed(relid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cache lookup failed for index {relid}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

// relcache.c:1486 elog(ERROR, "cache lookup failed for access method %u").
#[track_caller]
#[cold]
#[inline(never)]
fn access_method_lookup_failed(relam: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cache lookup failed for access method {relam}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn unexpected_null_pg_index(relid: Oid, attno: i32) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "unexpected null in pg_index column {attno} for index {relid}"
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

// relcache.c:1494 elog(ERROR, "relnatts disagrees with indnatts for index %u").
#[track_caller]
#[cold]
#[inline(never)]
fn relnatts_disagrees(relid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("relnatts disagrees with indnatts for index {relid}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
// relcache.c:1632 elog(ERROR, "bogus pg_index tuple") -- no OID in the text.
fn bogus_pg_index() -> Box<PgError> {
    Box::new(
        PgError::error("bogus pg_index tuple")
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_amproc(num: i16, opc: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("invalid amproc number {num} for opclass {opc}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn opclass_not_found(opc: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("could not find tuple for opclass {opc}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

// Validate an untrusted catalog varlena datum against the extent of the tuple
// image that backs it, returning its header-declared byte size. `d` is a
// not-null by-ref datum fetched from `tuple`, so its pointer lands inside the
// tuple image; a forged 4-byte header can otherwise declare up to ~1 GiB and
// drive an out-of-bounds slice. Mirrors the detoast/bounds check committed for
// the pg_trigger varlenas: a length that runs past the tuple's end becomes a
// catchable corruption error rather than a read past the live allocation.
fn checked_varlena_size(tuple: &HeapTupleData<'_>, d: Datum) -> PgResult<usize> {
    let p = d.as_usize() as *const u8;
    let start = tuple.header_ptr();
    let end = start.wrapping_add(tuple.t_len as usize);
    // The by-ref datum must point within the tuple image that holds it.
    if p < start || p > end {
        return Err(corrupt_varlena());
    }
    let avail = (end as usize) - (p as usize);
    // SAFETY: p lies within the live [start, end) image, so at least `avail`
    // bytes are readable; varsize_bounded reads no header byte past `avail`.
    unsafe { types_tuple::varatt::varsize_bounded(p, avail) }.ok_or_else(corrupt_varlena)
}

#[track_caller]
#[cold]
#[inline(never)]
fn corrupt_varlena() -> Box<PgError> {
    Box::new(
        PgError::error("catalog varlena length exceeds tuple extent".to_string())
            .with_sqlstate(ERRCODE_DATA_CORRUPTED),
    )
}

const CONSTRAINT_RELATION_ID: Oid = 2606;
const CONSTRAINT_RELID_TYPID_NAME_INDEX_ID: Oid = 2665;
const Anum_pg_constraint_contype: i32 = 4;
const Anum_pg_constraint_conrelid: i32 = 9;
const Anum_pg_constraint_conindid: i32 = 11;
const Anum_pg_constraint_conperiod: i32 = 20;
const Anum_pg_constraint_conexclop: i32 = 27;
const CONSTRAINT_EXCLUSION: i8 = b'x' as i8;
const CONSTRAINT_PRIMARY: i8 = b'p' as i8;
const CONSTRAINT_UNIQUE: i8 = b'u' as i8;
const OIDOID: Oid = 26;

// RelationGetExclusionInfo's pg_constraint scan (relcache.c:5640-5773). Its
// consistency checks are elog(ERROR) naming RelationGetRelationName(index):
// duplicate record (:5724), null conexclop (:5733), conexclop not a 1-D Oid
// array of indnkeyatts (:5742), record missing (:5751) -- catchable XX000s.
pub(crate) fn scan_exclusion_ops<'mcx>(
    mcx: Mcx<'mcx>,
    conrelid: Oid,
    index_relid: Oid,
    index_relname: &str,
    indnkeyatts: i16,
) -> PgResult<PgVec<'mcx, Oid>> {
    let cx = MemoryContext::new("RelationGetExclusionInfo");
    let smcx = cx.mcx();
    let rel = table::table_open(smcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_constraint_conrelid, conrelid)];
    let mut scan = genam::systable_beginscan(
        smcx,
        &rel,
        CONSTRAINT_RELID_TYPID_NAME_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let mut out: Option<PgVec<'mcx, Oid>> = None;
    while let Some(tup) = genam::systable_getnext(smcx, &mut scan)? {
        let td = rel.descr();
        let contype = req(td, tup, Anum_pg_constraint_contype)?.as_i8();
        let conperiod = req(td, tup, Anum_pg_constraint_conperiod)?.as_bool();
        if contype != CONSTRAINT_EXCLUSION
            && !(conperiod && (contype == CONSTRAINT_PRIMARY || contype == CONSTRAINT_UNIQUE))
        {
            continue;
        }
        if req(td, tup, Anum_pg_constraint_conindid)?.as_oid() != index_relid {
            continue;
        }
        if out.is_some() {
            return Err(exclusion_info_error(format!(
                "unexpected exclusion constraint record found for rel {index_relname}"
            )));
        }
        let (d, isnull) = crate::getattr(td, tup, Anum_pg_constraint_conexclop);
        if isnull {
            return Err(exclusion_info_error(format!("null conexclop for rel {index_relname}")));
        }
        // The oid[] varlena header comes off the (possibly crafted) catalog
        // tuple; bound its declared length against the tuple extent so a forged
        // header raises a catchable error instead of reading past the image.
        let size = checked_varlena_size(tup, d)?;
        let p = d.as_usize() as *const u8;
        // SAFETY: checked_varlena_size confirmed `size` bytes are inside the
        // live tuple image backing this by-ref datum.
        let image = unsafe { core::slice::from_raw_parts(p, size) };
        let payload = varlena::open_image(smcx, image)?;
        let body = payload.as_bytes();
        let total = body.len() + 4;
        let mut full: PgVec<'_, u8> = mcx::vec_with_capacity_in(smcx, total)?;
        mcx::vec_append_bytes(&mut full, &(((total as u32) << 2).to_ne_bytes()))?;
        mcx::vec_append_bytes(&mut full, body)?;
        // relcache.c:5738-5744: ARR_NDIM == 1, dims[0] == indnkeyatts, no
        // nulls, OIDOID elements -- else "conexclop is not a 1-D Oid array".
        let rd = |off: usize| -> i32 {
            full.get(off..off + 4).map_or(0, |b| i32::from_ne_bytes(b.try_into().unwrap()))
        };
        if rd(4) != 1 || rd(16) != indnkeyatts as i32 || rd(8) != 0 || rd(12) as Oid != OIDOID {
            return Err(exclusion_info_error("conexclop is not a 1-D Oid array".to_string()));
        }
        let elems = datum::array_build::deconstruct_array_image(smcx, &full, 4, true, b'i')?;
        let mut ops: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, elems.len())?;
        for e in elems.iter() {
            ops.push(e.as_oid());
        }
        out = Some(ops);
    }
    genam::systable_endscan(smcx, scan)?;
    rel.close(AccessShareLock)?;
    out.ok_or_else(|| {
        exclusion_info_error(format!(
            "exclusion constraint record missing for rel {index_relname}"
        ))
    })
}

#[track_caller]
#[cold]
#[inline(never)]
fn exclusion_info_error(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INTERNAL_ERROR))
}

#[cfg(test)]
mod index_tests {
    use super::*;
    use types_tuple::ItemPointerData;

    // A forged 4-byte varlena header on a catalog column must be rejected
    // against the tuple extent instead of driving an out-of-bounds read.
    #[test]
    fn forged_varlena_length_is_caught() {
        let mut image = [0u8; 32];
        // 4-byte header at offset 24 declaring the ~1 GiB max varlena length.
        let forged = types_tuple::varatt::set_varsize_4b_word(0x3FFF_FFFF).to_ne_bytes();
        image[24..28].copy_from_slice(&forged);
        let base = image.as_ptr();
        // SAFETY: 32-byte live image; checked_varlena_size only reads
        // header_ptr()/t_len, never the tuple header struct itself.
        let tuple = unsafe {
            HeapTupleData::from_raw_parts(
                base,
                image.len() as u32,
                ItemPointerData::default(),
                InvalidOid,
            )
        };
        // SAFETY: offset 24 is within the 32-byte image.
        let d = Datum::from_usize(unsafe { base.add(24) } as usize);
        let err = checked_varlena_size(&tuple, d).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    }

    // A well-formed short (1-byte header) varlena within the tuple is accepted.
    #[test]
    fn inbounds_varlena_length_is_accepted() {
        let mut image = [0u8; 32];
        // 1-byte header at offset 24: 4-byte total (header + 3 payload bytes).
        let off = 24usize;
        // SAFETY: offset 24 is within the 32-byte image.
        unsafe { types_tuple::varatt::set_varsize_short(image.as_mut_ptr().add(off), 4) };
        let base = image.as_ptr();
        let tuple = unsafe {
            HeapTupleData::from_raw_parts(
                base,
                image.len() as u32,
                ItemPointerData::default(),
                InvalidOid,
            )
        };
        let d = Datum::from_usize(unsafe { base.add(off) } as usize);
        assert_eq!(checked_varlena_size(&tuple, d).ok().unwrap(), 4);
    }
}
