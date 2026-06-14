//! Seam declarations for the shared `amcheck` driver in
//! `contrib/amcheck/verify_common.c` (unit `contrib-amcheck-verify-common`,
//! still `todo`).
//!
//! `amcheck_lock_relation_and_check` is the common entry point every per-AM
//! amcheck verifier (`verify_nbtree.c`, `verify_gin.c`) calls: it opens and
//! locks the index + its heap, runs `index_checkable`, then invokes the AM's
//! `IndexDoCheckCallback` with the relations and a `readonly` flag, and
//! finally drops the locks.
//!
//! The owning unit installs this from its `init_seams()` when it lands; until
//! then a call panics loudly. It is consumed by both `bt_index_check` and
//! `bt_index_parent_check` in `contrib-amcheck-verify-nbtree`.

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use types_error::PgResult;
use types_rel::Relation;
use types_storage::lock::LOCKMODE;

/// `BTCallbackState` (verify_nbtree.c) — the per-check flags the nbtree
/// verifier threads through `amcheck_lock_relation_and_check`'s opaque
/// `void *state`. In C this is an arbitrary `void *`; the only scaffolded
/// consumer is the nbtree verifier, so the shared seam carries the concrete
/// argument record (the `void *` is resolved to its real type per
/// opacity-inherited-never-introduced).
#[derive(Clone, Copy, Debug, Default)]
pub struct BTCallbackState {
    /// `parentcheck` — running `bt_index_parent_check` (ShareLock, full
    /// parent/child verification) rather than `bt_index_check`.
    pub parentcheck: bool,
    /// `heapallindexed` — also verify the heap has no unindexed tuples.
    pub heapallindexed: bool,
    /// `rootdescend` — also re-find every non-pivot tuple via a fresh search.
    pub rootdescend: bool,
    /// `checkunique` — also check the uniqueness constraint if the index is
    /// unique.
    pub checkunique: bool,
}

/// `IndexDoCheckCallback` (verify_common.h) — the per-AM checker the common
/// driver invokes once it holds the locks: `(rel, heaprel, state, readonly)`.
/// The C `void *state` is resolved to the concrete [`BTCallbackState`] (the
/// only scaffolded consumer). Returns `PgResult` so the checker's `ereport`s
/// propagate.
pub type IndexDoCheckCallback =
    for<'mcx> fn(&Relation<'mcx>, &Relation<'mcx>, &BTCallbackState, bool) -> PgResult<()>;

seam_core::seam!(
    /// `amcheck_lock_relation_and_check(indrelid, am_id, check, lockmode,
    /// state)` (verify_common.c): open and lock the index `indrelid` (and its
    /// heap) at `lockmode`, run `index_checkable(rel, am_id)`, invoke `check`
    /// with the relations and the `readonly` flag implied by `lockmode`, then
    /// release the locks. `Err` carries the open/lock/checkable ereports and
    /// any error the callback raises.
    pub fn amcheck_lock_relation_and_check(
        indrelid: Oid,
        am_id: Oid,
        check: IndexDoCheckCallback,
        lockmode: LOCKMODE,
        state: BTCallbackState,
    ) -> PgResult<()>
);
