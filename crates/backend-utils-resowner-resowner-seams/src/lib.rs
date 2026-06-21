//! Seam declarations for the resource-owner unit
//! (`utils/resowner/resowner.c`).
//!
//! Consumers:
//!
//! 1. portalcmds models the C `saveResourceOwner = CurrentResourceOwner;
//!    CurrentResourceOwner = portal->resowner; ...; CurrentResourceOwner =
//!    saveResourceOwner;` save/run/restore idiom as a single scoped callback
//!    (`with_current_resource_owner`). `CurrentResourceOwner` is *not* exposed
//!    as a save/restore global pair to portalcmds — that is the ambient-state
//!    anti-pattern the lifecycle model forbids (docs/query-lifecycle-raii.md;
//!    resowner dissolves into RAII/scoped capability).
//!
//! 2. logical decoding's slot-advance helper needs the raw get/set on
//!    `CurrentResourceOwner` (it saves/restores the executor's resource owner
//!    across decoding), so the bare global accessors are also exposed.
//!
//! 3. the PREPARE/EXECUTE EXPLAIN driver threads the current resource owner
//!    handle into `GetCachedPlan`/`ReleaseCachedPlan`, so a plain read of the
//!    current owner (as the parsestmt opaque handle) is exposed too.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_error::PgResult;
use types_logical::ResourceOwnerHandle;
use types_nodes::parsestmt::ResourceOwnerHandle as ParsestmtResourceOwnerHandle;
use types_portal::ResourceOwner;

seam_core::seam!(
    /// Run `f` with `owner` installed as the current resource owner, restoring
    /// the previous current owner afterwards (and on error). When `owner`
    /// is the C NULL (`ResourceOwner::is_null`), the current owner is left
    /// unchanged for the duration (mirrors `if (portal->resowner)
    /// CurrentResourceOwner = portal->resowner;`). `f`'s error propagates.
    pub fn with_current_resource_owner(
        owner: ResourceOwner,
        f: &mut dyn FnMut() -> PgResult<()>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// Read `CurrentResourceOwner`.
    pub fn CurrentResourceOwner() -> ResourceOwnerHandle
);

seam_core::seam!(
    /// `CurrentResourceOwner = value`.
    pub fn set_CurrentResourceOwner(value: ResourceOwnerHandle)
);

seam_core::seam!(
    /// `CurrentResourceOwner` (resowner.c global) — the backend's current
    /// resource owner, as the parsestmt opaque handle the PREPARE/EXECUTE
    /// driver threads into the plan-cache calls. Pure read of backend-local
    /// state.
    pub fn current_resource_owner() -> PgResult<ParsestmtResourceOwnerHandle>
);

seam_core::seam!(
    /// `CreateAuxProcessResourceOwner()` (resowner.c): create the aux-process
    /// resource owner and set `CurrentResourceOwner` to it, registering a
    /// cleanup callback. `Err` carries its `ereport` surface.
    pub fn create_aux_process_resource_owner() -> PgResult<()>
);

seam_core::seam!(
    /// `ReleaseAuxProcessResources(isCommit)` (resowner.c): release everything
    /// held by the aux-process resource owner (warning about leaked buffer
    /// pins). `Err` carries its `ereport` surface.
    pub fn release_aux_process_resources(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `CurrentResourceOwner = NULL` (resowner.c global): reset the current
    /// resource owner to nothing.
    pub fn reset_current_resource_owner()
);

seam_core::seam!(
    /// `CurrentResourceOwner = owner` (resowner.c global): restore the current
    /// resource owner. snapbuild.c's SnapBuildClearExportedSnapshot restores
    /// the owner saved before StartTransactionCommand (NULL handle == C NULL).
    pub fn set_current_resource_owner(owner: ParsestmtResourceOwnerHandle)
);

seam_core::seam!(
    /// `AtStart_ResourceOwner()` (xact.c:1330): create the toplevel
    /// "TopTransaction" resource owner for the transaction and publish it to
    /// the `TopTransactionResourceOwner`/`CurTransactionResourceOwner`/
    /// `CurrentResourceOwner` globals. Consumers (the buffer manager's
    /// `ResourceOwnerEnlarge(CurrentResourceOwner)` pin path) require a live
    /// current owner. `Err` carries the `ResourceOwnerCreate` ereport surface.
    pub fn at_start_resource_owner() -> PgResult<()>
);

seam_core::seam!(
    /// `ResourceOwnerRelease(TopTransactionResourceOwner,
    /// RESOURCE_RELEASE_BEFORE_LOCKS, true, is_commit)` — the first
    /// transaction-end resource-owner release leg of `CommitTransaction`/
    /// `AbortTransaction`/`PrepareTransaction` (xact.c), issued before the
    /// `AtEOXact_Buffers`/relcache/inval calls. Commit/Abort additionally do
    /// `CurrentResourceOwner = NULL` immediately before this via
    /// `reset_current_resource_owner` (Prepare does not). `Err` carries the
    /// release ereport surface.
    pub fn release_transaction_owner_before_locks(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `ResourceOwnerRelease(TopTransactionResourceOwner,
    /// RESOURCE_RELEASE_LOCKS, true, is_commit)` then
    /// `ResourceOwnerRelease(..., RESOURCE_RELEASE_AFTER_LOCKS, ...)` — the
    /// second transaction-end resource-owner release legs of
    /// `CommitTransaction`/`AbortTransaction` (xact.c), issued after
    /// `AtEOXact_Inval`/`AtEOXact_MultiXact`. `Err` carries the release surface.
    pub fn release_transaction_owner_locks(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `ResourceOwnerDelete(TopTransactionResourceOwner);
    /// CurTransactionResourceOwner = NULL; TopTransactionResourceOwner = NULL`
    /// — the final transaction-end resource-owner delete/clear leg of
    /// `CommitTransaction`/`AbortTransaction`/`CleanupTransaction` (xact.c).
    /// `Err` carries the delete ereport surface.
    pub fn delete_transaction_owner() -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubStart_ResourceOwner()` (xact.c:1283): create the subtransaction's
    /// `"SubTransaction"` resource owner as a child of the immediate parent's
    /// (`= the current CurTransactionResourceOwner`), then publish it to the
    /// `CurTransactionResourceOwner`/`CurrentResourceOwner` globals. Pins taken
    /// during the subtransaction are tracked on this owner so a subtransaction
    /// abort can release them independently of the parent. `Err` carries the
    /// `ResourceOwnerCreate` ereport surface.
    pub fn at_substart_resource_owner() -> PgResult<()>
);

seam_core::seam!(
    /// `ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_BEFORE_LOCKS,
    /// false, is_commit)` — the first subtransaction-end resource-owner release
    /// leg of `CommitSubTransaction`/`AbortSubTransaction` (xact.c), issued
    /// before the per-subxact relcache/inval calls. This is the leg that
    /// releases buffer pins held by the subtransaction. `Err` carries the
    /// release ereport surface.
    pub fn release_subxact_owner_before_locks(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `ResourceOwnerRelease(s->curTransactionOwner, RESOURCE_RELEASE_LOCKS,
    /// false, is_commit)` then `ResourceOwnerRelease(..., AFTER_LOCKS, ...)` —
    /// the second subtransaction-end resource-owner release legs of
    /// `CommitSubTransaction`/`AbortSubTransaction` (xact.c). `Err` carries the
    /// release surface.
    pub fn release_subxact_owner_locks(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `CurrentResourceOwner = s->curTransactionOwner` (xact.c:1316,
    /// `AtSubAbort_ResourceOwner`): re-establish a valid `CurrentResourceOwner`
    /// (the subtransaction owner, = the live `CurTransactionResourceOwner`)
    /// before the abort cleanup that may consult `CurrentResourceOwner`.
    pub fn set_current_to_cur_transaction()
);

seam_core::seam!(
    /// `CurrentResourceOwner = s->parent->curTransactionOwner;
    /// CurTransactionResourceOwner = s->parent->curTransactionOwner;
    /// ResourceOwnerDelete(s->curTransactionOwner); s->curTransactionOwner = NULL`
    /// — the final subtransaction-end delete/restore leg of
    /// `CommitSubTransaction`/`AbortSubTransaction`/`CleanupSubTransaction`
    /// (xact.c): restore the parent's owner to the Cur/Current globals and free
    /// the subtransaction owner. `Err` carries the delete ereport surface.
    pub fn cleanup_subxact_owner() -> PgResult<()>
);
