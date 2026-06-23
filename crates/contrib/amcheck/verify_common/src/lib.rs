#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

//! `contrib-amcheck-verify-common` — port of
//! `contrib/amcheck/verify_common.c`.
//!
//! Utility functions common to every per-AM amcheck verifier.
//! [`amcheck_lock_relation_and_check`] is the workhorse every verifier
//! (`verify_nbtree.c`, `verify_gin.c`) calls: given an index OID it locks the
//! heap (before the index, to avoid deadlocks), opens both relations, switches
//! to the table owner's userid under a security-restricted operation, runs
//! [`index_checkable`], invokes the AM's check callback, then unwinds the GUC
//! nest level / userid and releases the locks.

use ::types_core::init::SECURITY_RESTRICTED_OPERATION;
use ::types_core::primitive::{InvalidOid, Oid, OidIsValid};
use ::types_core::catalog::{RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED};
use ::types_tuple::access::RELKIND_INDEX;
use ::types_error::error::{
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_READ_ONLY_SQL_TRANSACTION,
    ERRCODE_UNDEFINED_TABLE,
};
use types_error::{NOTICE, PgError, PgResult};
use ::rel::Relation;
use ::types_storage::lock::{LOCKMODE, ShareLock};

use verify_common_seams::{BTCallbackState, IndexDoCheckCallback};

use indexam_seams as indexam;
use table_seams as table;
use transam_xlog_seams as xlog;
use index_seams as catalog_index;
use relcache_seams as relcache;
use syscache_seams as syscache;
use elog_seams as elog;
use miscinit_seams as miscinit;
use guc_seams as guc;

/// `amcheck_index_mainfork_expected(rel)` (verify_common.c) — whether the index
/// relation should have a main-fork file. Verification skips unlogged indexes
/// in hot standby, where there is nothing to verify.
///
/// NB: Caller should call [`index_checkable`] before calling here.
fn amcheck_index_mainfork_expected(rel: &Relation<'_>) -> PgResult<bool> {
    if rel.rd_rel.relpersistence != RELPERSISTENCE_UNLOGGED
        || !xlog::recovery_in_progress::call()
    {
        return Ok(true);
    }

    // ereport(NOTICE, errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION), ...)
    let _ = ERRCODE_READ_ONLY_SQL_TRANSACTION;
    elog::ereport_msg::call(
        NOTICE,
        format!(
            "cannot verify unlogged index \"{}\" during recovery, skipping",
            rel.name()
        ),
        None,
    )?;

    Ok(false)
}

/// `index_checkable(rel, am_id)` (verify_common.c) — basic suitability checks
/// for verifying `rel` as an index of access method `am_id`.
///
/// NB: Intentionally not checking permissions, the function is normally not
/// callable by non-superusers. If granted, it's useful to be able to check a
/// whole cluster.
pub fn index_checkable<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    rel: &Relation<'mcx>,
    am_id: Oid,
) -> PgResult<bool> {
    if rel.rd_rel.relkind != RELKIND_INDEX || rel.rd_rel.relam != am_id {
        // SearchSysCache1(AMOID, am_id) -> amname for the requested AM and the
        // relation's actual AM, for the error message.
        let want = syscache::search_am_name::call(mcx, am_id)?
            .map(|s| s.to_string())
            .unwrap_or_default();
        let have = syscache::search_am_name::call(mcx, rel.rd_rel.relam)?
            .map(|s| s.to_string())
            .unwrap_or_default();
        return Err(PgError::error(format!(
            "expected \"{want}\" index as targets for verification"
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_detail(format!("Relation \"{}\" is a {have} index.", rel.name())));
    }

    if relation_is_other_temp(rel)? {
        return Err(PgError::error(
            "cannot access temporary tables of other sessions",
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_detail(format!(
            "Index \"{}\" is associated with temporary relation.",
            rel.name()
        )));
    }

    // !rel->rd_index->indisvalid
    let indisvalid = rel
        .rd_index
        .as_ref()
        .map(|ix| ix.indisvalid)
        .unwrap_or(false);
    if !indisvalid {
        return Err(PgError::error(format!(
            "cannot check index \"{}\"",
            rel.name()
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .with_detail("Index is not valid."));
    }

    amcheck_index_mainfork_expected(rel)
}

/// `RELATION_IS_OTHER_TEMP(relation)` (`utils/rel.h`):
/// `relpersistence == RELPERSISTENCE_TEMP && !rd_islocaltemp`. `rd_islocaltemp`
/// is relcache-owned (set per `relpersistence`/temp-namespace ownership at
/// build time); read it through the relcache seam.
fn relation_is_other_temp(rel: &Relation<'_>) -> PgResult<bool> {
    if rel.rd_rel.relpersistence != RELPERSISTENCE_TEMP {
        return Ok(false);
    }
    Ok(!relcache::rd_islocaltemp::call(rel)?)
}

/// `amcheck_lock_relation_and_check(indrelid, am_id, check, lockmode, state)`
/// (verify_common.c).
///
/// Given an index relation OID, lock the table (before the index, to avoid
/// deadlocks), then: (1) ensure the index can be checked, (2) switch to the
/// table owner's userid under a security-restricted operation, (3) keep GUC
/// changes local to this command, (4) run the callback, and finally unwind.
pub fn amcheck_lock_relation_and_check(
    indrelid: Oid,
    am_id: Oid,
    check: IndexDoCheckCallback,
    lockmode: LOCKMODE,
    state: BTCallbackState,
) -> PgResult<()> {
    let arena = mcx::MemoryContext::new("amcheck lock and check");
    let mcx = arena.mcx();

    // We must lock table before index to avoid deadlocks. If indrelid isn't an
    // index, IndexGetRelation() fails; postpone complaining and let the
    // is-it-an-index test below fail with a friendlier message.
    let heapid = catalog_index::index_get_relation::call(indrelid, true)?;

    let heaprel: Option<Relation<'_>>;
    let save_userid;
    let save_sec_context;
    let save_nestlevel;

    if OidIsValid(heapid) {
        let hr = table::table_open::call(mcx, heapid, lockmode)?;

        // Switch to the table owner's userid so index functions run as that
        // user; lock down security-restricted operations and make GUC changes
        // local to this command.
        let (uid, sec) = miscinit::get_user_id_and_sec_context::call();
        save_userid = uid;
        save_sec_context = sec;
        miscinit::set_user_id_and_sec_context::call(
            hr.rd_rel.relowner,
            save_sec_context | SECURITY_RESTRICTED_OPERATION,
        );
        save_nestlevel = guc::new_guc_nest_level::call();
        heaprel = Some(hr);
    } else {
        heaprel = None;
        save_userid = InvalidOid;
        save_sec_context = -1;
        save_nestlevel = -1;
    }

    // Open the index (heap already locked first to prevent deadlocking). No
    // indcheckxmin usability test is needed here.
    let indrel = indexam::index_open::call(mcx, indrelid, lockmode)?;

    // The IndexGetRelation call above took no lock, so a race against an index
    // drop/recreation could have netted us the wrong table.
    if heaprel.is_none()
        || heapid != catalog_index::index_get_relation::call(indrelid, false)?
    {
        return Err(PgError::error(format!(
            "could not open parent table of index \"{}\"",
            indrel.name()
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_TABLE));
    }
    let heaprel = heaprel.expect("checked is_none above");

    // Check that the relation is suitable, then run the callback. As in C, an
    // error from index_checkable or the callback ereports(ERROR) and unwinds
    // (the inline cleanup below is reached only on the success path; the
    // transaction abort releases the GUC nest level / userid / locks otherwise).
    if index_checkable(mcx, &indrel, am_id)? {
        check(&indrel, &heaprel, &state, lockmode == ShareLock)?;
    }

    // Roll back any GUC changes executed by index functions.
    guc::at_eoxact_guc::call(false, save_nestlevel)?;

    // Restore userid and security context.
    miscinit::set_user_id_and_sec_context::call(save_userid, save_sec_context);

    // Release locks early: nothing in the called routines triggers shared cache
    // invalidations, so we can relax the usual commit-time release. `close`
    // consumes the carrier and runs its armed closer (relcache close + lock
    // release for this lockmode).
    indrel.close(lockmode)?;
    heaprel.close(lockmode)?;

    Ok(())
}

/// Install every seam this crate owns.
pub fn init_seams() {
    ::verify_common_seams::amcheck_lock_relation_and_check::set(
        amcheck_lock_relation_and_check,
    );
}
