//! Registration / (sub)transaction-state bookkeeping (inval.c
//! `InvalidationInfo` / `TransInvalidationInfo`, `Register*Invalidation`,
//! `Prepare[Inplace]InvalidationState`).

use ::mcx::Mcx;
use types_core::{primitive::OidIsValid, Oid};
use types_error::{PgError, PgResult};

use transam_xact_seams as xact_seams;
use relcache_seams as relcache_seams;

use crate::msgs::{self, InvalMessageArray, InvalidationMsgsGroup};
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
    /// Split-borrow the dense message arrays together with the
    /// `CurrentCmdInvalidMsgs` group of the selected `InvalidationInfo`.
    ///
    /// The C `Register*` routines append into `info->CurrentCmdInvalidMsgs`
    /// using the file-scope `InvalMessageArrays[]`; the two live in distinct
    /// fields of [`InvalState`], so this hands back independent `&mut`s to both.
    pub(crate) fn current_cmd_group_mut<'a, 'mcx>(
        &self,
        state: &'a mut InvalState<'mcx>,
    ) -> (
        &'a mut [InvalMessageArray<'mcx>; 2],
        &'a mut InvalidationMsgsGroup,
    ) {
        let arrays = &mut state.message_arrays;
        let group = match self {
            InfoRef::Trans => {
                let top = state
                    .trans_inval_stack
                    .last_mut()
                    .expect("transInvalInfo must be set by PrepareInvalidationState");
                &mut top.ii.current_cmd_invalid_msgs
            }
            InfoRef::Inplace => {
                let info = state
                    .inplace_inval_info
                    .as_mut()
                    .expect("inplaceInvalInfo must be set by PrepareInplaceInvalidationState");
                &mut info.current_cmd_invalid_msgs
            }
        };
        (arrays, group)
    }

    /// `info->RelcacheInitFileInval = true`.
    pub(crate) fn set_relcache_init_file_inval(&self, state: &mut InvalState<'_>) {
        match self {
            InfoRef::Trans => {
                state
                    .trans_inval_stack
                    .last_mut()
                    .expect("transInvalInfo must be set by PrepareInvalidationState")
                    .ii
                    .relcache_init_file_inval = true;
            }
            InfoRef::Inplace => {
                state
                    .inplace_inval_info
                    .as_mut()
                    .expect("inplaceInvalInfo must be set by PrepareInplaceInvalidationState")
                    .relcache_init_file_inval = true;
            }
        }
    }
}

/// `RegisterCatcacheInvalidation` — register an invalidation event for a
/// catcache tuple entry. (Static callback replayed from the
/// `PrepareToInvalidateCacheTuple` requests.)
pub(crate) fn register_catcache_invalidation<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut InvalState<'mcx>,
    info: InfoRef,
    cache_id: i32,
    hash_value: u32,
    db_id: Oid,
) -> PgResult<()> {
    let (arrays, group) = info.current_cmd_group_mut(state);
    msgs::add_catcache_invalidation_message(mcx, arrays, group, cache_id, hash_value, db_id)
}

/// `RegisterCatalogInvalidation` — register an invalidation event for all
/// catcache entries from a catalog.
pub(crate) fn register_catalog_invalidation<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut InvalState<'mcx>,
    info: InfoRef,
    db_id: Oid,
    cat_id: Oid,
) -> PgResult<()> {
    let (arrays, group) = info.current_cmd_group_mut(state);
    msgs::add_catalog_invalidation_message(mcx, arrays, group, db_id, cat_id)
}

/// `RegisterRelcacheInvalidation` — register a relcache invalidation event
/// (also drives `GetCurrentCommandId(true)` and the init-file flag via
/// `RelationIdIsInInitFile`).
pub(crate) fn register_relcache_invalidation<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut InvalState<'mcx>,
    info: InfoRef,
    db_id: Oid,
    rel_id: Oid,
) -> PgResult<()> {
    {
        let (arrays, group) = info.current_cmd_group_mut(state);
        msgs::add_relcache_invalidation_message(mcx, arrays, group, db_id, rel_id)?;
    }

    /*
     * Most of the time, relcache invalidation is associated with system
     * catalog updates, but there are a few cases where it isn't.  Quick hack
     * to ensure that the next CommandCounterIncrement() will think that we
     * need to do CommandEndInvalidationMessages().
     */
    let _ = xact_seams::get_current_command_id::call(true)?;

    /*
     * If the relation being invalidated is one of those cached in a relcache
     * init file, mark that we need to zap that file at commit. For simplicity
     * invalidations for a specific database always invalidate the shared file
     * as well.  Also zap when we are invalidating whole relcache.
     */
    if !OidIsValid(rel_id) || relcache_seams::relation_id_is_in_init_file::call(rel_id) {
        info.set_relcache_init_file_inval(state);
    }

    Ok(())
}

/// `RegisterRelsyncInvalidation` — register a relsynccache invalidation event.
pub(crate) fn register_relsync_invalidation<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut InvalState<'mcx>,
    info: InfoRef,
    db_id: Oid,
    rel_id: Oid,
) -> PgResult<()> {
    let (arrays, group) = info.current_cmd_group_mut(state);
    msgs::add_relsync_invalidation_message(mcx, arrays, group, db_id, rel_id)
}

/// `RegisterSnapshotInvalidation` — register an invalidation event for MVCC
/// scans against a given catalog. Only needed for catalogs without catcaches.
pub(crate) fn register_snapshot_invalidation<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut InvalState<'mcx>,
    info: InfoRef,
    db_id: Oid,
    rel_id: Oid,
) -> PgResult<()> {
    let (arrays, group) = info.current_cmd_group_mut(state);
    msgs::add_snapshot_invalidation_message(mcx, arrays, group, db_id, rel_id)
}

/// `PrepareInvalidationState` — initialize inval data for the current
/// (sub)transaction, returning a handle to the top transactional info.
pub(crate) fn prepare_invalidation_state<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut InvalState<'mcx>,
) -> PgResult<InfoRef> {
    // PrepareToInvalidateCacheTuple() needs relcache: AssertCouldGetRelation().
    // Can't queue transactional message while collecting inplace messages.
    debug_assert!(state.inplace_inval_info.is_none());

    let nest_level = xact_seams::get_current_transaction_nest_level::call();

    if let Some(top) = state.trans_inval_stack.last() {
        if top.my_level == nest_level {
            return Ok(InfoRef::Trans);
        }
    }

    // MemoryContextAllocZero(TopTransactionContext, sizeof(TransInvalidationInfo)).
    let mut my_info = TransInvalidationInfo {
        ii: InvalidationInfo::default(),
        prior_cmd_invalid_msgs: InvalidationMsgsGroup::default(),
        // myInfo->parent = transInvalInfo (modelled by the stack ordering).
        my_level: nest_level,
    };

    /* Now, do we have a previous stack entry? */
    if let Some(parent) = state.trans_inval_stack.last() {
        /* Yes; this one should be for a deeper nesting level. */
        debug_assert!(my_info.my_level > parent.my_level);

        /*
         * The parent (sub)transaction must not have any current (i.e.,
         * not-yet-locally-processed) messages.
         */
        if parent.ii.current_cmd_invalid_msgs.num_messages_in_group() != 0 {
            return Err(PgError::error(
                "cannot start a subtransaction when there are unprocessed inval messages",
            ));
        }

        /*
         * MemoryContextAllocZero set firstmsg = nextmsg = 0 in each group,
         * which is fine for the first (sub)transaction, but otherwise we need
         * to update them to follow whatever is already in the arrays.
         */
        let parent_current = parent.ii.current_cmd_invalid_msgs;
        my_info
            .prior_cmd_invalid_msgs
            .set_group_to_follow(&parent_current);
        let prior = my_info.prior_cmd_invalid_msgs;
        my_info.ii.current_cmd_invalid_msgs.set_group_to_follow(&prior);
    } else {
        /*
         * Here, we need only clear any array pointers left over from a prior
         * transaction.
         */
        state.message_arrays[crate::CAT_CACHE_MSGS] = InvalMessageArray::new(mcx);
        state.message_arrays[crate::REL_CACHE_MSGS] = InvalMessageArray::new(mcx);
    }

    // transInvalInfo = myInfo (push onto the stack).
    state.trans_inval_stack.push(my_info);
    Ok(InfoRef::Trans)
}

/// `PrepareInplaceInvalidationState` — initialize inval data for an inplace
/// update.
pub(crate) fn prepare_inplace_invalidation_state<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut InvalState<'mcx>,
) -> InfoRef {
    // AssertCouldGetRelation(); limit of one inplace update under assembly.
    debug_assert!(state.inplace_inval_info.is_none());

    // gone after WAL insertion CritSection ends, so use current context:
    // myInfo = palloc0(sizeof(InvalidationInfo)).
    let mut my_info = InvalidationInfo::default();

    /* Stash our messages past end of the transactional messages, if any. */
    if let Some(top) = state.trans_inval_stack.last() {
        let top_current = top.ii.current_cmd_invalid_msgs;
        my_info
            .current_cmd_invalid_msgs
            .set_group_to_follow(&top_current);
    } else {
        state.message_arrays[crate::CAT_CACHE_MSGS] = InvalMessageArray::new(mcx);
        state.message_arrays[crate::REL_CACHE_MSGS] = InvalMessageArray::new(mcx);
    }

    state.inplace_inval_info = Some(my_info);
    InfoRef::Inplace
}
