use core::cell::RefCell;
use core::mem::ManuallyDrop;

use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheKey, INDEXRELID};
use datum::Datum;
use mcx::{Mcx, MemoryContext, PgHashMap, PgVec};
use relcache::schemapg::{
    ACCESS_METHOD_PROCEDURE_INDEX_ID, ACCESS_METHOD_PROCEDURE_RELATION_ID, OPCLASS_OID_INDEX_ID,
    OPERATOR_CLASS_RELATION_ID,
};
use relcache_build_seams::{IndexAccessInfo, PgIndexListShape};
use types_core::{AttrNumber, InvalidOid, Oid, INDEX_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_rel::{AccessShareLock, FormData_pg_class, FormData_pg_index};

use crate::{int2vector_values, oid_key, oidvector_values, req};

const INDEX_INDRELID_INDEX_ID: Oid = 2678;
const OID_BTREE_OPS_OID: Oid = 1981;
const INT2_BTREE_OPS_OID: Oid = 1979;
const BTREE_AM_OID: Oid = 403;
const HASH_AM_OID: Oid = 405;
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

// RelationInitIndexAccessInfo (relcache.c). C also resolves rd_amhandler/
// rd_indam (amapi dispatches on rd_rel.relam here) and preloads the
// rd_support array (rd_supportinfo resolves lazily per key column), so the
// pg_am probe and the pg_amproc scan are structurally subsumed.
pub(crate) fn relation_init_index_access_info(
    mcx: Mcx<'static>,
    relid: Oid,
    form: &FormData_pg_class,
) -> PgResult<IndexAccessInfo> {
    let Some(tup) = SearchSysCache1(INDEXRELID, SysCacheKey::Value(Datum::from_oid(relid)))?
    else {
        return Err(cache_lookup_failed(relid));
    };
    let get = |attno: i32| -> PgResult<Datum> {
        let (d, isnull) = SysCacheGetAttr(INDEXRELID, &tup, attno)?;
        if isnull {
            return Err(unexpected_null_pg_index(relid, attno));
        }
        Ok(d)
    };

    let indnatts = get(Anum_pg_index_indnatts)?.as_i16();
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

    let amsupport = match form.relam {
        BTREE_AM_OID => BTNProcs,
        HASH_AM_OID => HASHNProcs,
        GIN_AM_OID => GINNProcs,
        GIST_AM_OID => GISTNProcs,
        SPGIST_AM_OID => SPGISTNProcs,
        BRIN_AM_OID => BRINNProcs,
        other => panic!(
            "relcache_build: index AM {other} for index {relid} unported \
             (amapi closed set is btree+hash+gin+gist+spgist+brin)"
        ),
    };
    let mut opfamily: PgVec<'static, Oid> = mcx::vec_with_capacity_in(mcx, nkey)?;
    let mut opcintype: PgVec<'static, Oid> = mcx::vec_with_capacity_in(mcx, nkey)?;
    let mut supportinfo: Vec<Option<types_fmgr::FmgrInfo>> = Vec::with_capacity(nkey);
    // C rd_support: nkey x amsupport proc OIDs, row-major.
    let mut support: PgVec<'static, Oid> = mcx::vec_with_capacity_in(mcx, nkey * amsupport)?;
    for &opc in &classvals[..nkey] {
        if opc == InvalidOid {
            return Err(bogus_pg_index(relid));
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
        indkey,
        has_indpred: !SysCacheGetAttr(INDEXRELID, &tup, Anum_pg_index_indpred)?.1,
        indexprs_src: {
            let (d, isnull) = SysCacheGetAttr(INDEXRELID, &tup, Anum_pg_index_indexprs)?;
            if isnull { None } else { Some(crate::attrs::text_str(mcx, mcx, d)?) }
        },
        indpred_src: {
            let (d, isnull) = SysCacheGetAttr(INDEXRELID, &tup, Anum_pg_index_indpred)?;
            if isnull { None } else { Some(crate::attrs::text_str(mcx, mcx, d)?) }
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
            let mcx = Box::leak(Box::new(MemoryContext::new("OpClassCache"))).mcx();
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

#[cold]
#[inline(never)]
fn cache_lookup_failed(relid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cache lookup failed for index {relid}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

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

#[cold]
#[inline(never)]
fn bogus_pg_index(relid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("bogus pg_index tuple for index {relid}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[cold]
#[inline(never)]
fn invalid_amproc(num: i16, opc: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("invalid amproc number {num} for opclass {opc}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

#[cold]
#[inline(never)]
fn opclass_not_found(opc: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("could not find tuple for opclass {opc}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}
