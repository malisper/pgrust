#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

#[cfg(test)]
mod tests;

mod define;
pub use define::DefineIndex;

mod reindex;
pub use reindex::ExecReindex;

use datum::Datum;
use mcx::MemoryContext;
use types_core::catalog::C_COLLATION_OID;
use types_core::fmgr::F_OIDEQ;
use types_core::{AttrNumber, InvalidOid, Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INTERNAL_ERROR, ERROR};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{HeapTupleData, TupleDescData};

const OperatorClassRelationId: Oid = 2616;
const OpclassAmNameNspIndexId: Oid = 2686;
const Anum_pg_opclass_oid: i32 = 1;
const Anum_pg_opclass_opcmethod: i32 = 2;
const Anum_pg_opclass_opcintype: i32 = 7;
const Anum_pg_opclass_opcdefault: i32 = 8;
const TYPCATEGORY_INVALID: i8 = 0;

pub fn init_seams() {
    indexcmds_seams::get_default_opclass::set(GetDefaultOpClass);
    indexcmds_seams::wait_for_older_snapshots::set(WaitForOlderSnapshots);
    indexcmds_seams::define_index_for_alter::set(define::define_index_for_alter);
    indexcmds_seams::define_index::set(define::DefineIndex);
    indexcmds_seams::index_set_parent_index::set(define::IndexSetParentIndex);
    indexcmds_seams::check_index_compatible::set(define::CheckIndexCompatible);
    indexcmds_seams::resolve_opclass::set(define::ResolveOpClass);
    indexcmds_seams::choose_relation_name::set(define::ChooseRelationName);
}

// WaitForOlderSnapshots (indexcmds.c:431): wait out transactions that might
// still see catalog state older than limit_xmin; progress reporting unported.
pub fn WaitForOlderSnapshots(limit_xmin: types_core::TransactionId) -> PgResult<()> {
    use types_core::InvalidLocalTransactionId;
    use types_storage::storage::{PROC_IN_SAFE_IC, PROC_IN_VACUUM, PROC_IS_AUTOVACUUM};
    let scratch = MemoryContext::new("WaitForOlderSnapshots");
    let mcx = scratch.mcx();
    let exclude = PROC_IS_AUTOVACUUM | PROC_IN_VACUUM | PROC_IN_SAFE_IC;
    let mut old_snapshots =
        procarray::GetCurrentVirtualXIDs(mcx, limit_xmin, true, false, exclude)?;
    for i in 0..old_snapshots.len() {
        if old_snapshots[i].localTransactionId == InvalidLocalTransactionId {
            continue;
        }
        if i > 0 {
            let newer =
                procarray::GetCurrentVirtualXIDs(mcx, limit_xmin, true, false, exclude)?;
            for j in i..old_snapshots.len() {
                if old_snapshots[j].localTransactionId == InvalidLocalTransactionId {
                    continue;
                }
                if !newer.iter().any(|n| *n == old_snapshots[j]) {
                    old_snapshots[j].localTransactionId = InvalidLocalTransactionId;
                }
            }
        }
        if old_snapshots[i].localTransactionId != InvalidLocalTransactionId {
            lock::VirtualXactLock(old_snapshots[i], true)?;
        }
    }
    Ok(())
}

fn oid_key(attno: i32, oid: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info({F_OIDEQ}) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

fn req(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> PgResult<Datum> {
    let mut isnull = false;
    // SAFETY: pg_opclass row read under its relation's descriptor; attno is a
    // declared non-null column of that catalog.
    let d = unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) };
    if isnull {
        return Err(Box::new(
            PgError::error(format!("unexpected null in pg_opclass column {attno}"))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
        ));
    }
    Ok(d)
}

// C IsPreferredType (parse_coerce.c).
fn IsPreferredType(category: i8, type_id: Oid) -> PgResult<bool> {
    let (typcategory, typispreferred) = lsyscache::get_type_category_preferred(type_id)?;
    Ok((category == typcategory || category == TYPCATEGORY_INVALID) && typispreferred)
}

/// GetDefaultOpClass (indexcmds.c).
pub fn GetDefaultOpClass(type_id: Oid, am_id: Oid) -> PgResult<Oid> {
    let type_id = lsyscache::getBaseType(type_id)?;
    let (tcategory, _) = lsyscache::get_type_category_preferred(type_id)?;

    let mut result = InvalidOid;
    let mut nexact = 0;
    let mut ncompatible = 0;
    let mut ncompatiblepreferred = 0;

    let cx = MemoryContext::new("GetDefaultOpClass");
    let mcx = cx.mcx();
    let rel = table::table_open(mcx, OperatorClassRelationId, types_rel::AccessShareLock)?;
    let keys = [oid_key(Anum_pg_opclass_opcmethod, am_id)];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, OpclassAmNameNspIndexId, true, None, &keys)?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        if !req(rel.descr(), tup, Anum_pg_opclass_opcdefault)?.as_bool() {
            continue;
        }
        let opcintype = req(rel.descr(), tup, Anum_pg_opclass_opcintype)?.as_oid();
        let oid = req(rel.descr(), tup, Anum_pg_opclass_oid)?.as_oid();
        if opcintype == type_id {
            nexact += 1;
            result = oid;
        } else if nexact == 0 && coerce::IsBinaryCoercible(type_id, opcintype)? {
            if IsPreferredType(tcategory, opcintype)? {
                ncompatiblepreferred += 1;
                result = oid;
            } else if ncompatiblepreferred == 0 {
                ncompatible += 1;
                result = oid;
            }
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;

    if nexact > 1 {
        return Err(multiple_default_opclasses(type_id));
    }

    if nexact == 1 || ncompatiblepreferred == 1 || (ncompatiblepreferred == 0 && ncompatible == 1)
    {
        debug_assert!(OidIsValid(result));
        return Ok(result);
    }

    Ok(InvalidOid)
}

#[track_caller]
#[cold]
#[inline(never)]
fn multiple_default_opclasses(type_id: Oid) -> Box<PgError> {
    let name = match format_type::format_type_be(type_id) {
        Ok(n) => n,
        Err(e) => return e,
    };
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("there are multiple default operator classes for data type {name}"))
            .into_error()
            .with_error_location(types_error::ErrorLocation::new(
                "indexcmds.c",
                0,
                "GetDefaultOpClass",
            )),
    )
}
