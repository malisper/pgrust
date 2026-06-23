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
//! [`::rel::Relation`] handle: the relcache owner copies the consumed slice
//! of its entry into the caller's `mcx`, and this unit arms the handle with the
//! close path (`relation_close`: relcache `RelationClose` then the lock
//! release). `Drop` on the handle is the abort path
//! (`relation_close(rel, NoLock)`).

#![allow(non_snake_case)]

use ::mcx::Mcx;
use ::types_core::primitive::{Oid, OidIsValid};
use ::types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use ::rel::{Relation, RelationData};
use ::types_storage::lock::{AccessShareLock, NoLock, LOCKMODE, MAX_LOCKMODES};

/// Install this unit's seam implementations (the `relation_open` family
/// declared in `backend-access-common-relation-seams`).
pub fn init_seams() {
    common_relation_seams::relation_open::set(relation_open);
    common_relation_seams::try_relation_open::set(try_relation_open);
    common_relation_seams::relation_openrv::set(relation_openrv);
    common_relation_seams::relation_openrv_extended::set(relation_openrv_extended);
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
    relcache_seams::relation_close::call(relid)?;

    if lockmode != NoLock {
        lmgr_seams::unlock_relation_oid::call(relid, lockmode)?;
    }

    Ok(())
}

/// Fetch a CLONE of the relcache's shared cell for `relationId` (the
/// dual-carry handle's pin). This is called right after
/// `relation_id_get_relation` (the copy) succeeds, so the entry is already
/// built and pinned in the cache; this PIN-FREE cell fetch finds the same
/// cell WITHOUT taking a second `rd_refcnt` pin (the copy path already took
/// the single pin this open's close releases — taking another here would
/// double-count the open, so `CheckTableNotInUse` would later see an inflated
/// refcount). A `None` here would mean the entry vanished between the two
/// lookups, which cannot happen within a single open — treated as the same
/// `could not open relation` error C raises for the copy path.
fn relation_id_get_relation_cell(relationId: Oid) -> PgResult<::rel::RelcacheCell> {
    match relcache_seams::relation_id_get_relation_cell::call(relationId)? {
        // Erase the concrete `Rc<RefCell<RelationData>>` to the handle's
        // type-erased `Rc<dyn Any>` pin (lossless `Rc` unsizing coercion). The
        // `strong_count` pin rides along; the concrete cell is recovered by
        // downcast at the typed accessors.
        Some(cell) => Ok(cell as ::rel::RelcacheCell),
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
    cell: ::rel::RelcacheCell,
    data: RelationData<'mcx>,
    lockmode: LOCKMODE,
    check_bootstrap: bool,
) -> PgResult<Relation<'mcx>> {
    finish_open_inner(cell, data, lockmode, check_bootstrap, false)
}

/// `finish_open` with control over the lock-held-by-me self-check. `prebuilt`
/// suppresses that check for a just-built local relation
/// ([`relation_open_prebuilt`]): C never re-opens the freshly built
/// `new_rel_desc` through `relation_open` (it uses the build's return directly
/// and locks it *afterwards* via `LockRelation` in `index_create`), so the
/// "caller must already hold a lock" assertion — which guards opens of
/// *existing* relations — does not apply.
fn finish_open_inner<'mcx>(
    cell: ::rel::RelcacheCell,
    data: RelationData<'mcx>,
    lockmode: LOCKMODE,
    check_bootstrap: bool,
    prebuilt: bool,
) -> PgResult<Relation<'mcx>> {
    // DUAL-CARRY (F1): the handle holds a CLONE of the relcache's shared cell
    // alongside the trimmed projected copy. The clone makes
    // `Rc::strong_count > 1` for as long as this relation is open, so the
    // relcache's `strong_count == 1` eviction now gates on open handles
    // (`relation_close`/`Drop` frees the cell). The `Deref` target stays the
    // trimmed copy, so consumers are untouched.
    let mut r = Relation::open_with_cell(cell, data, Some(relation_closer));

    // If we didn't get the lock ourselves, assert that caller holds one
    // (except, for relation_open, in bootstrap mode where no locks are used).
    //
    // Assert(lockmode != NoLock ||
    //        [IsBootstrapProcessingMode() ||]
    //        CheckRelationLockedByMe(r, AccessShareLock, true));
    debug_assert!(
        prebuilt
            || lockmode != NoLock
            || (check_bootstrap
                && miscinit_seams::is_bootstrap_processing_mode::call())
            || lmgr_seams::check_relation_locked_by_me::call(
                r.rd_id,
                AccessShareLock,
                true,
            )
    );

    // Make note that we've accessed a temporary relation.
    // if (RelationUsesLocalBuffers(r))
    //     MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE;
    if r.uses_local_buffers() {
        transam_xact_seams::set_xact_accessed_temp_namespace::call();
    }

    // pgstat_init_relation(r) (pgstat_relation.c): decide whether this
    // relation's cumulative stats should be counted and store the bit onto the
    // open relation, mirroring C's `rel->pgstat_enabled = ...`. The pgstat
    // owner reads `rel->rd_rel->relkind` and the `pgstat_track_counts` GUC; we
    // pass the relkind off the freshly built descriptor and store the returned
    // bit so every later `pgstat_count_*` gate (which passes
    // `relation.pgstat_enabled`) sees the right value. C runs this last in
    // `relation_open`, after the temp-namespace flag.
    let pgstat_enabled = pgstat_seams::pgstat_init_relation::call(
        r.rd_id,
        r.rd_rel.relkind,
    );
    r.set_pgstat_enabled(pgstat_enabled);

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
        lmgr_seams::lock_relation_oid::call(relationId, lockmode)?.keep();
    }

    // The relcache does all the real work...
    let r = relcache_seams::relation_id_get_relation::call(mcx, relationId)?;

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
    let cell = relation_id_get_relation_cell(relationId)?;

    finish_open(cell, data, lockmode, true)
}

/// Open a just-built local relation (`RelationBuildLocalRelation`'s result)
/// WITHOUT taking a fresh `rd_refcnt` pin: the build already took the single
/// `RelationIncrementReferenceCount` pin, so a normal [`relation_open`] would
/// over-pin the entry (leaving a stuck reference that later makes
/// `CheckTableNotInUse` on DROP/TRUNCATE report the relation as still in use).
/// The returned handle is armed with the close path, so dropping/closing it
/// releases that single build pin — exactly mirroring C's
/// `heap_create` (returns the pinned `new_rel_desc`) followed by
/// `table_close(new_rel_desc, NoLock)` in `heap_create_with_catalog`.
///
/// Takes `NoLock` (the caller already holds `AccessExclusiveLock` for the
/// cataloged path, or holds its own for the uncataloged callers), so no lock
/// is acquired or released here.
pub fn relation_open_prebuilt<'mcx>(
    mcx: Mcx<'mcx>,
    relationId: Oid,
) -> PgResult<Relation<'mcx>> {
    // Pin-free projection of the already-pinned entry.
    let data = relcache_seams::relation_project_existing::call(
        mcx, relationId,
    )?
    .ok_or_else(|| {
        PgError::error(format!("could not open relation with OID {relationId}"))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR)
    })?;

    // Pin-free shared-cell clone (the dual-carry strong_count pin only).
    let cell = relation_id_get_relation_cell(relationId)?;

    // prebuilt => suppress the lock-held-by-me self-check: this is a just-built
    // local relation nobody else can see yet (C locks it AFTER the build, e.g.
    // `LockRelation` in `index_create`), so the assertion guarding opens of
    // existing relations does not apply.
    finish_open_inner(cell, data, NoLock, true, true)
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
        lmgr_seams::lock_relation_oid::call(relationId, lockmode)?.keep();
    }

    // Now that we have the lock, probe to see if the relation really exists or
    // not.  SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relationId)).
    if !syscache_seams::search_syscache_exists_reloid::call(relationId)? {
        // Release useless lock.
        if lockmode != NoLock {
            lmgr_seams::unlock_relation_oid::call(relationId, lockmode)?;
        }

        return Ok(None);
    }

    // Should be safe to do a relcache load.
    let r = relcache_seams::relation_id_get_relation::call(mcx, relationId)?;

    let Some(data) = r else {
        // elog(ERROR, "could not open relation with OID %u", relationId)
        return Err(
            PgError::error(format!("could not open relation with OID {relationId}"))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
        );
    };

    // Also obtain a CLONE of the cache's shared cell to pin (dual-carry, F1).
    let cell = relation_id_get_relation_cell(relationId)?;

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
        inval_seams::accept_invalidation_messages::call()?;
    }

    // Look up and lock the appropriate relation using namespace search.
    let relOid =
        namespace_seams::range_var_get_relid::call(mcx, relation, lockmode, false)?;

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
        inval_seams::accept_invalidation_messages::call()?;
    }

    // Look up and lock the appropriate relation using namespace search.
    let relOid = namespace_seams::range_var_get_relid::call(
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
