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

#[cold]
#[inline(never)]
fn recovery_in_progress_error() -> Box<PgError> {
    Box::new(
        PgError::error("recovery is in progress")
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint("GIN pending list cannot be cleaned up during recovery."),
    )
}

#[cold]
#[inline(never)]
fn not_a_gin_index(indexRel: &Relation<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!("\"{}\" is not a GIN index", indexRel.name()))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
    )
}

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

/// gin_clean_pending_list (ginfast.c). Returns pages_deleted, computed as the
/// drop in the metapage's nPendingPages across the cleanup (RELATION_IS_OTHER_TEMP
/// is const-false in this single-backend model, so that C check is a no-op here).
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

    // RELATION_IS_OTHER_TEMP(indexRel): const-false single-backend; the
    // ERRCODE_FEATURE_NOT_SUPPORTED arm above stays wired for the day that
    // changes (cross-backend temp relation visibility) but never fires now.
    let _ = other_temp_index;

    if !aclchk::object_ownercheck(RELATION_RELATION_ID, indexoid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            ObjectType::OBJECT_INDEX,
            indexRel.name(),
        )?;
    }

    let is_valid = indexRel.rd_index.as_ref().map_or(false, |i| i.indisvalid);

    let pages_deleted: i64 = if is_valid {
        let before = gin::ginGetStats(&indexRel)?.nPendingPages as i64;
        let state = gin::build::initGinState(&indexRel)?;
        gin::ginInsertCleanup(mcx, &indexRel, &state, true, true, true)?;
        let after = gin::ginGetStats(&indexRel)?.nPendingPages as i64;
        (before - after).max(0)
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
        let expect = [(3789u32, "gin_clean_pending_list", 1usize)];
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
