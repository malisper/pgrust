//! ginfast.c: gin_clean_pending_list, the SQL-callable pending-list cleanup
//! entry point. Split into its own crate (mirrors brin/brin_funcs) so the
//! `gin` AM crate — depended on by indexam for the ambulkdelete/amvacuumcleanup
//! dispatch — never depends back on indexam/aclchk.
#![allow(non_snake_case)]

use ::datum::Datum;
use ::types_core::{GIN_AM_OID, RELATION_RELATION_ID};
use ::types_error::{
    PgError, PgResult, DEBUG1, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_WRONG_OBJECT_TYPE,
};
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use ::types_nodes::parsenodes::ObjectType;
use ::types_rel::{Relation, RowExclusiveLock, RELKIND_INDEX};

#[track_caller]
#[cold]
#[inline(never)]
fn recovery_in_progress_error() -> Box<PgError> {
    Box::new(
        PgError::error("recovery is in progress")
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint("GIN pending list cannot be cleaned up during recovery."),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn not_a_gin_index(indexRel: &Relation<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!("\"{}\" is not a GIN index", indexRel.name()))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn other_temp_index() -> Box<PgError> {
    Box::new(
        PgError::error("cannot access temporary indexes of other sessions")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

fn is_gin_index(indexRel: &Relation<'_>) -> bool {
    indexRel.rd_rel.relkind == RELKIND_INDEX && indexRel.rd_rel.relam == GIN_AM_OID
}

/// gin_clean_pending_list (ginfast.c:1030).
pub fn fc_gin_clean_pending_list(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let indexoid = fcinfo.arg(0).as_oid();
    let mcx = fcinfo.result_mcx();

    if transam_xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_error());
    }

    let indexRel = indexam_seams::index_open::call(mcx, indexoid, RowExclusiveLock)?;

    if !is_gin_index(&indexRel) {
        return Err(not_a_gin_index(&indexRel));
    }

    // ginfast.c:1056 — we have no visibility into the owning session's local
    // buffers, so reading its pending list would return wrong data.
    if indexRel.is_other_temp() {
        return Err(other_temp_index());
    }

    if !aclchk::object_ownercheck(RELATION_RELATION_ID, indexoid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            ObjectType::OBJECT_INDEX,
            indexRel.name(),
        )?;
    }

    let is_valid = indexRel.rd_index.as_ref().map_or(false, |i| i.indisvalid);

    let pages_deleted: i64 = if is_valid {
        let mut stats = ::types_nbtree::IndexBulkDeleteResult::default();
        let state = gin::build::initGinState(&indexRel)?;
        gin::ginInsertCleanup(mcx, &indexRel, &state, true, true, true, Some(&mut stats))?;
        stats.pages_deleted as i64
    } else {
        let _ = elog::elog(
            DEBUG1,
            format!("index \"{}\" is not valid", indexRel.name()),
        );
        0
    };

    indexRel.close(RowExclusiveLock)?;

    Ok(Datum::from_i64(pages_deleted))
}

pub static GIN_FUNCS_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: 3789,
    name: "gin_clean_pending_list",
    nargs: 1,
    strict: true,
    retset: false,
    func: fc_gin_clean_pending_list as PGFunction,
}];

pub fn init_seams() {
    ::fmgr_core::register_late_builtins(GIN_FUNCS_BUILTINS);
}

#[cfg(test)]
mod tests {
    use super::GIN_FUNCS_BUILTINS;

    #[test]
    fn builtins_match_pg_proc_dat() {
        let expect = [(3789u32, "gin_clean_pending_list", 1i16)];
        assert_eq!(GIN_FUNCS_BUILTINS.len(), expect.len());
        for (b, (foid, name, nargs)) in GIN_FUNCS_BUILTINS.iter().zip(expect) {
            assert_eq!(b.foid, foid);
            assert_eq!(b.name, name);
            assert_eq!(b.nargs, nargs);
            assert!(b.strict);
            assert!(!b.retset);
        }
    }
}
