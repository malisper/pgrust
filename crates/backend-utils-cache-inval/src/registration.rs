//! Registration / (sub)transaction-state bookkeeping (inval.c
//! `InvalidationInfo` / `TransInvalidationInfo`, `Register*Invalidation`,
//! `Prepare[Inplace]InvalidationState`, and the public `Register*` /
//! `PrepareInvalidationState` wrappers).

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;

use crate::msgs::InvalidationMsgsGroup;
use crate::InvalState;

/* fields common to both transactional and inplace invalidation */
#[derive(Clone, Copy, Default)]
pub(crate) struct InvalidationInfo {
    /// Events emitted by current command (`CurrentCmdInvalidMsgs`).
    pub(crate) current_cmd_invalid_msgs: InvalidationMsgsGroup,
    /// init file must be invalidated? (`RelcacheInitFileInval`).
    pub(crate) relcache_init_file_inval: bool,
}

/* subclass adding fields specific to transactional invalidation */
#[derive(Clone, Copy, Default)]
pub(crate) struct TransInvalidationInfo {
    /// Base class (`ii`).
    pub(crate) ii: InvalidationInfo,
    /// Events emitted by previous commands of this (sub)transaction.
    pub(crate) prior_cmd_invalid_msgs: InvalidationMsgsGroup,
    /// Subtransaction nesting depth (`my_level`).
    pub(crate) my_level: i32,
}

/// Selector for which `InvalidationInfo` a register call targets (C passes an
/// `InvalidationInfo *` from one of the two `Prepare*` routines).
#[derive(Clone, Copy)]
pub(crate) enum InfoRef {
    /// `transInvalInfo` (top of stack) viewed as `InvalidationInfo`.
    Trans,
    /// `inplaceInvalInfo`.
    Inplace,
}

impl InfoRef {
    pub(crate) fn current_cmd_group_mut<'a, 'mcx>(
        &self,
        _state: &'a mut InvalState<'mcx>,
    ) -> (
        &'a mut [crate::msgs::InvalMessageArray<'mcx>; 2],
        &'a mut InvalidationMsgsGroup,
    ) {
        todo!("InfoRef::current_cmd_group_mut: split-borrow arrays + current_cmd group")
    }

    pub(crate) fn set_relcache_init_file_inval(&self, _state: &mut InvalState<'_>) {
        todo!("InfoRef::set_relcache_init_file_inval")
    }
}

/// `RegisterCatcacheInvalidation` (static callback replayed from the
/// `PrepareToInvalidateCacheTuple` requests).
pub(crate) fn register_catcache_invalidation<'mcx>(
    _mcx: Mcx<'mcx>,
    _state: &mut InvalState<'mcx>,
    _info: InfoRef,
    _cache_id: i32,
    _hash_value: u32,
    _db_id: Oid,
) -> PgResult<()> {
    todo!("RegisterCatcacheInvalidation")
}

/// `RegisterCatalogInvalidation`.
pub(crate) fn register_catalog_invalidation<'mcx>(
    _mcx: Mcx<'mcx>,
    _state: &mut InvalState<'mcx>,
    _info: InfoRef,
    _db_id: Oid,
    _cat_id: Oid,
) -> PgResult<()> {
    todo!("RegisterCatalogInvalidation")
}

/// `RegisterRelcacheInvalidation` (also drives `GetCurrentCommandId(true)` and
/// the init-file flag via `RelationIdIsInInitFile`).
pub(crate) fn register_relcache_invalidation<'mcx>(
    _mcx: Mcx<'mcx>,
    _state: &mut InvalState<'mcx>,
    _info: InfoRef,
    _db_id: Oid,
    _rel_id: Oid,
) -> PgResult<()> {
    todo!("RegisterRelcacheInvalidation")
}

/// `RegisterRelsyncInvalidation`.
pub(crate) fn register_relsync_invalidation<'mcx>(
    _mcx: Mcx<'mcx>,
    _state: &mut InvalState<'mcx>,
    _info: InfoRef,
    _db_id: Oid,
    _rel_id: Oid,
) -> PgResult<()> {
    todo!("RegisterRelsyncInvalidation")
}

/// `RegisterSnapshotInvalidation`.
pub(crate) fn register_snapshot_invalidation<'mcx>(
    _mcx: Mcx<'mcx>,
    _state: &mut InvalState<'mcx>,
    _info: InfoRef,
    _db_id: Oid,
    _rel_id: Oid,
) -> PgResult<()> {
    todo!("RegisterSnapshotInvalidation")
}

/// `PrepareInvalidationState` — initialize inval data for the current
/// (sub)transaction, returning a handle to the top transactional info.
pub(crate) fn prepare_invalidation_state<'mcx>(
    _mcx: Mcx<'mcx>,
    _state: &mut InvalState<'mcx>,
) -> PgResult<InfoRef> {
    todo!("PrepareInvalidationState")
}

/// `PrepareInplaceInvalidationState` — initialize inval data for an inplace
/// update.
pub(crate) fn prepare_inplace_invalidation_state<'mcx>(
    _mcx: Mcx<'mcx>,
    _state: &mut InvalState<'mcx>,
) -> InfoRef {
    todo!("PrepareInplaceInvalidationState")
}

/* ----------------------------------------------------------------
 *  Public re-exports of the internal register primitives
 * ---------------------------------------------------------------- */

/// `RegisterCatcacheInvalidation(cacheId, hashValue, dbId)`.
pub fn RegisterCatcacheInvalidation(_cacheId: i32, _hashValue: u32, _dbId: Oid) -> PgResult<()> {
    todo!("RegisterCatcacheInvalidation (public wrapper)")
}

/// `RegisterRelcacheInvalidation(dbId, relId)`.
pub fn RegisterRelcacheInvalidation(_dbId: Oid, _relId: Oid) -> PgResult<()> {
    todo!("RegisterRelcacheInvalidation (public wrapper)")
}

/// `RegisterSnapshotInvalidation(dbId, relId)`.
pub fn RegisterSnapshotInvalidation(_dbId: Oid, _relId: Oid) -> PgResult<()> {
    todo!("RegisterSnapshotInvalidation (public wrapper)")
}

/// `PrepareInvalidationState()`.
pub fn PrepareInvalidationState() -> PgResult<()> {
    todo!("PrepareInvalidationState (public wrapper)")
}
