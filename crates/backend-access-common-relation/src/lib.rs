//! Port of `src/backend/access/common/relation.c` — generic relation routines
//! that implement access to relations (tables, indexes, etc).
//!
//! `relation.c` is pure orchestration: every operation is a call into a
//! genuinely-external subsystem — the relcache
//! (`RelationIdGetRelation`/`RelationClose`), the lock manager
//! (`LockRelationOid`/`UnlockRelationOid`/`CheckRelationLockedByMe`), the
//! syscache (`SearchSysCacheExists1`), namespace search (`RangeVarGetRelid`),
//! shared-cache invalidation (`AcceptInvalidationMessages`),
//! `pgstat_init_relation`, the `IsBootstrapProcessingMode` probe, and the
//! temp-namespace flag (`RelationUsesLocalBuffers` / `MyXactFlags`). Each is
//! routed through that owner's per-owner seam crate. The C control flow between
//! those calls stays in this crate.
//!
//! The C `Relation` (`struct RelationData *`) crosses as a
//! [`types_rel::Relation`] handle: the relcache owner copies the consumed slice
//! of its entry into the caller's `mcx`, and this unit arms the handle with the
//! close path (`relation_close`: relcache `RelationClose` then the lock
//! release). `Drop` on the handle is the abort path
//! (`relation_close(rel, NoLock)`).

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::primitive::{Oid, OidIsValid};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_rel::{Relation, RelationData};
use types_storage::lock::{AccessShareLock, NoLock, LOCKMODE, MAX_LOCKMODES};

/// Install this unit's seam implementations (the `relation_open` family
/// declared in `backend-access-common-relation-seams`).
pub fn init_seams() {
    backend_access_common_relation_seams::relation_open::set(relation_open);
    backend_access_common_relation_seams::try_relation_open::set(try_relation_open);
    backend_access_common_relation_seams::relation_openrv::set(relation_openrv);
    backend_access_common_relation_seams::relation_openrv_extended::set(relation_openrv_extended);
}

/// The close path armed onto every relation handle this unit opens:
/// `relation_close(relation, lockmode)` — `RelationClose(relation)` then, if
/// `lockmode` is not `NoLock`, the lock release. C captures
/// `relation->rd_lockInfo.lockRelId` and calls `UnlockRelationId(&relid,
/// lockmode)`; the lmgr `unlock_relation_oid` wrapper re-derives the same lock
/// tag from the relation OID (`SetLocktagRelationOid`), so the OID-keyed handle
/// closer is faithful.
fn relation_closer(relid: Oid, lockmode: LOCKMODE) -> PgResult<()> {
    // The relcache does the real work...
    backend_utils_cache_relcache_seams::relation_close::call(relid)?;

    if lockmode != NoLock {
        backend_storage_lmgr_lmgr_seams::unlock_relation_oid::call(relid, lockmode)?;
    }

    Ok(())
}

/// Fetch a CLONE of the relcache's shared cell for `relationId` (the
/// dual-carry handle's pin). This is called right after
/// `relation_id_get_relation` (the copy) succeeds, so the entry is already
/// built and pinned in the cache; the shared lookup against the same cache must
/// find it. A `None` here would mean the entry vanished between the two
/// lookups, which cannot happen within a single open — treated as the same
/// `could not open relation` error C raises for the copy path.
fn relation_id_get_relation_shared(relationId: Oid) -> PgResult<types_rel::RelcacheCell> {
    match backend_utils_cache_relcache_seams::relation_id_get_relation_shared::call(relationId)? {
        // Erase the concrete `Rc<RefCell<RelationData>>` to the handle's
        // type-erased `Rc<dyn Any>` pin (lossless `Rc` unsizing coercion). The
        // `strong_count` pin rides along; the concrete cell is recovered by
        // downcast at the typed accessors.
        Some(cell) => Ok(cell as types_rel::RelcacheCell),
        None => Err(
            PgError::error(format!("could not open relation with OID {relationId}"))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
        ),
    }
}

/// Wrap a freshly built relcache descriptor in a [`Relation`] handle armed with
/// this unit's close path, and run the post-open bookkeeping shared by
/// `relation_open` / `try_relation_open`: the lock-held-by-me self-check, the
/// temp-namespace flag, and `pgstat_init_relation`.
fn finish_open<'mcx>(
    cell: types_rel::RelcacheCell,
    data: RelationData<'mcx>,
    lockmode: LOCKMODE,
    check_bootstrap: bool,
) -> PgResult<Relation<'mcx>> {
    // DUAL-CARRY (F1): the handle holds a CLONE of the relcache's shared cell
    // alongside the trimmed projected copy. The clone makes
    // `Rc::strong_count > 1` for as long as this relation is open, so the
    // relcache's `strong_count == 1` eviction now gates on open handles
    // (`relation_close`/`Drop` frees the cell). The `Deref` target stays the
    // trimmed copy, so consumers are untouched.
    let r = Relation::open_with_cell(cell, data, Some(relation_closer));

    // If we didn't get the lock ourselves, assert that caller holds one
    // (except, for relation_open, in bootstrap mode where no locks are used).
    //
    // Assert(lockmode != NoLock ||
    //        [IsBootstrapProcessingMode() ||]
    //        CheckRelationLockedByMe(r, AccessShareLock, true));
    debug_assert!(
        lockmode != NoLock
            || (check_bootstrap
                && backend_utils_init_miscinit_seams::is_bootstrap_processing_mode::call())
            || backend_storage_lmgr_lmgr_seams::check_relation_locked_by_me::call(
                r.rd_id,
                AccessShareLock,
                true,
            )
    );

    // Make note that we've accessed a temporary relation.
    // if (RelationUsesLocalBuffers(r))
    //     MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE;
    if r.uses_local_buffers() {
        backend_access_transam_xact_seams::set_xact_accessed_temp_namespace::call();
    }

    backend_utils_activity_pgstat_seams::pgstat_init_relation::call(r.rd_id)?;

    Ok(r)
}

/// `relation_open(Oid relationId, LOCKMODE lockmode)`.
///
/// Open any relation by relation OID. If `lockmode` is not `NoLock`, the
/// specified kind of lock is obtained on the relation. An error is raised if the
/// relation does not exist.
///
/// NB: a "relation" is anything with a `pg_class` entry. The caller is expected
/// to check whether the relkind is something it can handle.
pub fn relation_open<'mcx>(
    mcx: Mcx<'mcx>,
    relationId: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    debug_assert!(lockmode >= NoLock && (lockmode as usize) < MAX_LOCKMODES);

    // Get the lock before trying to open the relcache entry. lmgr locks are
    // transaction-scoped (released by relation_close or, on error, by xact
    // abort), so the guard is kept rather than dropped at end of scope.
    if lockmode != NoLock {
        backend_storage_lmgr_lmgr_seams::lock_relation_oid::call(relationId, lockmode)?.keep();
    }

    // The relcache does all the real work...
    let r = backend_utils_cache_relcache_seams::relation_id_get_relation::call(mcx, relationId)?;

    let Some(data) = r else {
        // elog(ERROR, "could not open relation with OID %u", relationId)
        return Err(
            PgError::error(format!("could not open relation with OID {relationId}"))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
        );
    };

    // Also obtain a CLONE of the cache's shared cell (C's live `RelationData
    // *`) for the handle to pin (dual-carry, F1). Same lookup against the same
    // cache the copy came from; the entry is already built/pinned above.
    let cell = relation_id_get_relation_shared(relationId)?;

    finish_open(cell, data, lockmode, true)
}

/// `try_relation_open(Oid relationId, LOCKMODE lockmode)`.
///
/// Same as [`relation_open`], except return `None` (the C NULL) instead of
/// failing if the relation does not exist. Probes the syscache after locking so
/// a useless lock can be released.
pub fn try_relation_open<'mcx>(
    mcx: Mcx<'mcx>,
    relationId: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Option<Relation<'mcx>>> {
    debug_assert!(lockmode >= NoLock && (lockmode as usize) < MAX_LOCKMODES);

    // Get the lock first. Transaction-scoped; keep the guard (see relation_open).
    if lockmode != NoLock {
        backend_storage_lmgr_lmgr_seams::lock_relation_oid::call(relationId, lockmode)?.keep();
    }

    // Now that we have the lock, probe to see if the relation really exists or
    // not.  SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relationId)).
    if !backend_utils_cache_syscache_seams::search_syscache_exists_reloid::call(relationId)? {
        // Release useless lock.
        if lockmode != NoLock {
            backend_storage_lmgr_lmgr_seams::unlock_relation_oid::call(relationId, lockmode)?;
        }

        return Ok(None);
    }

    // Should be safe to do a relcache load.
    let r = backend_utils_cache_relcache_seams::relation_id_get_relation::call(mcx, relationId)?;

    let Some(data) = r else {
        // elog(ERROR, "could not open relation with OID %u", relationId)
        return Err(
            PgError::error(format!("could not open relation with OID {relationId}"))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
        );
    };

    // Also obtain a CLONE of the cache's shared cell to pin (dual-carry, F1).
    let cell = relation_id_get_relation_shared(relationId)?;

    // No bootstrap short-circuit in try_relation_open's C Assert.
    Ok(Some(finish_open(cell, data, lockmode, false)?))
}

/// `relation_openrv(const RangeVar *relation, LOCKMODE lockmode)`.
///
/// Same as [`relation_open`], but the relation is specified by a `RangeVar`.
pub fn relation_openrv<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &types_tuple::access::RangeVar,
    lockmode: LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    // Check for shared-cache-inval messages before trying to open the relation.
    // This is needed even if we already hold a lock on the relation, because
    // GRANT/REVOKE are executed without taking any lock on the target relation,
    // and we want to be sure we see current ACL information. We can skip this if
    // asked for NoLock, on the assumption that such a call is not the first one
    // in the current command, and so we should be reasonably up-to-date already.
    if lockmode != NoLock {
        backend_utils_cache_inval_seams::accept_invalidation_messages::call()?;
    }

    // Look up and lock the appropriate relation using namespace search.
    let relOid =
        backend_catalog_namespace_seams::range_var_get_relid::call(mcx, relation, lockmode, false)?;

    // Let relation_open do the rest. RangeVarGetRelid already took the lock, so
    // open with NoLock.
    relation_open(mcx, relOid, NoLock)
}

/// `relation_openrv_extended(const RangeVar *relation, LOCKMODE lockmode, bool
/// missing_ok)`.
///
/// Same as [`relation_openrv`], but with an additional `missing_ok` argument
/// allowing a `None` return rather than an error if the relation is not found.
/// (Note that some other causes, such as permissions problems, will still result
/// in an `ereport`.)
pub fn relation_openrv_extended<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &types_tuple::access::RangeVar,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<Option<Relation<'mcx>>> {
    // Check for shared-cache-inval messages before trying to open the relation.
    // See comments in relation_openrv().
    if lockmode != NoLock {
        backend_utils_cache_inval_seams::accept_invalidation_messages::call()?;
    }

    // Look up and lock the appropriate relation using namespace search.
    let relOid = backend_catalog_namespace_seams::range_var_get_relid::call(
        mcx, relation, lockmode, missing_ok,
    )?;

    // Return None on not-found.
    if !OidIsValid(relOid) {
        return Ok(None);
    }

    // Let relation_open do the rest.
    Ok(Some(relation_open(mcx, relOid, NoLock)?))
}

#[cfg(test)]
mod tests;
