#![allow(non_snake_case)]

use std::rc::Rc;

use mcx::Mcx;
use rel_vocab::RangeVar;
use types_core::{Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_rel::{AccessShareLock, MaxLockMode, NoLock, Relation, RelationData, LOCKMODE};

#[cfg(test)]
mod tests;

pub fn init_seams() {
    relation_seams::relation_open::set(relation_open);
    relation_seams::try_relation_open::set(try_relation_open);
    relation_seams::relation_openrv::set(relation_openrv);
    relation_seams::relation_openrv_extended::set(relation_openrv_extended);
}

// relation_close, lock half only. The relcache half (C's RelationClose
// decrement) is the Drop of the handle's Rc<RelationData> — rd_refcnt is the
// strong count, so no by-OID relcache call exists. Drop-without-close is C's
// abort path: NoLock, the lock stays until xact cleanup.
fn relation_closer(relid: Oid, lockmode: LOCKMODE) -> PgResult<()> {
    if lockmode != NoLock {
        lmgr_seams::unlock_relation_oid::call(relid, lockmode)?;
    }
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn could_not_open(relationId: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("could not open relation with OID {relationId}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

fn finish_open<'mcx>(
    data: Rc<RelationData<'mcx>>,
    lockmode: LOCKMODE,
    check_bootstrap: bool,
) -> Relation<'mcx> {
    let r = Relation::open_rc(data, Some(relation_closer));

    debug_assert!(
        lockmode != NoLock
            || (check_bootstrap && miscinit_seams::is_bootstrap_processing_mode::call())
            || lmgr_seams::check_relation_locked_by_me::call(r.rd_id, AccessShareLock, true)
    );

    if r.uses_local_buffers() {
        xact_seams::set_xact_accessed_temp_namespace::call();
    }

    r.pgstat_enabled
        .set(pgstat_seams::pgstat_init_relation::call(r.rd_id, r.rd_rel.relkind));

    r
}

pub fn relation_open<'mcx>(
    _mcx: Mcx<'mcx>,
    relationId: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    debug_assert!(lockmode >= NoLock && lockmode <= MaxLockMode);

    if lockmode != NoLock {
        lmgr_seams::lock_relation_oid::call(relationId, lockmode)?;
    }

    let Some(data) = relcache_seams::relation_id_get_relation::call(relationId)? else {
        return Err(could_not_open(relationId));
    };

    Ok(finish_open(data, lockmode, true))
}

pub fn try_relation_open<'mcx>(
    _mcx: Mcx<'mcx>,
    relationId: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Option<Relation<'mcx>>> {
    debug_assert!(lockmode >= NoLock && lockmode <= MaxLockMode);

    if lockmode != NoLock {
        lmgr_seams::lock_relation_oid::call(relationId, lockmode)?;
    }

    if !syscache_seams::search_syscache_exists_reloid::call(relationId)? {
        if lockmode != NoLock {
            lmgr_seams::unlock_relation_oid::call(relationId, lockmode)?;
        }
        return Ok(None);
    }

    let Some(data) = relcache_seams::relation_id_get_relation::call(relationId)? else {
        return Err(could_not_open(relationId));
    };

    Ok(Some(finish_open(data, lockmode, false)))
}

pub fn relation_openrv<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RangeVar,
    lockmode: LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    // Needed even under an existing lock: GRANT/REVOKE run without locking the
    // target, and current ACL info must be visible.
    if lockmode != NoLock {
        inval_seams::accept_invalidation_messages::call()?;
    }

    let relOid = namespace_seams::range_var_get_relid::call(mcx, relation, lockmode, false)?;

    relation_open(mcx, relOid, NoLock)
}

pub fn relation_openrv_extended<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RangeVar,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<Option<Relation<'mcx>>> {
    if lockmode != NoLock {
        inval_seams::accept_invalidation_messages::call()?;
    }

    let relOid = namespace_seams::range_var_get_relid::call(mcx, relation, lockmode, missing_ok)?;

    if !OidIsValid(relOid) {
        return Ok(None);
    }

    Ok(Some(relation_open(mcx, relOid, NoLock)?))
}
