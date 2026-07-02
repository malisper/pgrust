use mcx::Mcx;
use types_core::primitive::OidIsValid;
use types_core::Oid;
use types_error::{PgError, PgResult};

use crate::msgs::{self, InvalidationMsgsGroup, MsgArrays};
use crate::InvalState;

#[derive(Clone, Copy, Default)]
pub(crate) struct InvalidationInfo {
    pub(crate) current_cmd_invalid_msgs: InvalidationMsgsGroup,
    pub(crate) relcache_init_file_inval: bool,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct TransInvalidationInfo {
    pub(crate) ii: InvalidationInfo,
    pub(crate) prior_cmd_invalid_msgs: InvalidationMsgsGroup,
    pub(crate) my_level: i32,
}

// The `InvalidationInfo *` a Register* call targets; a selector, not a &mut,
// so register paths split-borrow the dense arrays alongside the group.
#[derive(Clone, Copy)]
pub(crate) enum InfoRef {
    Trans,
    Inplace,
}

impl InfoRef {
    pub(crate) fn current_cmd_group_mut<'a, 'mcx>(
        &self,
        state: &'a mut InvalState<'mcx>,
    ) -> (&'a mut MsgArrays<'mcx>, &'a mut InvalidationMsgsGroup) {
        let arrays = &mut state.msg_arrays;
        let group = match self {
            InfoRef::Trans => {
                let top = state
                    .trans_stack
                    .last_mut()
                    .expect("transInvalInfo set by PrepareInvalidationState");
                &mut top.ii.current_cmd_invalid_msgs
            }
            InfoRef::Inplace => {
                let info = state
                    .inplace_info
                    .as_mut()
                    .expect("inplaceInvalInfo set by PrepareInplaceInvalidationState");
                &mut info.current_cmd_invalid_msgs
            }
        };
        (arrays, group)
    }

    pub(crate) fn set_relcache_init_file_inval(&self, state: &mut InvalState<'_>) {
        match self {
            InfoRef::Trans => {
                state
                    .trans_stack
                    .last_mut()
                    .expect("transInvalInfo set by PrepareInvalidationState")
                    .ii
                    .relcache_init_file_inval = true;
            }
            InfoRef::Inplace => {
                state
                    .inplace_info
                    .as_mut()
                    .expect("inplaceInvalInfo set by PrepareInplaceInvalidationState")
                    .relcache_init_file_inval = true;
            }
        }
    }
}

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

    // Ensures the next CommandCounterIncrement processes invalidations.
    let _ = xact_seams::get_current_command_id::call(true)?;

    if !OidIsValid(rel_id) || relcache_seams::relation_id_is_in_init_file::call(rel_id) {
        info.set_relcache_init_file_inval(state);
    }

    Ok(())
}

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

pub(crate) fn prepare_invalidation_state(state: &mut InvalState<'_>) -> PgResult<InfoRef> {
    debug_assert!(state.inplace_info.is_none());

    let nest_level = xact_seams::get_current_transaction_nest_level::call();

    if let Some(top) = state.trans_stack.last() {
        if top.my_level == nest_level {
            return Ok(InfoRef::Trans);
        }
    }

    let mut my_info = TransInvalidationInfo {
        my_level: nest_level,
        ..Default::default()
    };

    if let Some(parent) = state.trans_stack.last() {
        debug_assert!(my_info.my_level > parent.my_level);

        if parent.ii.current_cmd_invalid_msgs.num_in_group() != 0 {
            return Err(PgError::error(
                "cannot start a subtransaction when there are unprocessed inval messages",
            )
            .into());
        }

        let parent_current = parent.ii.current_cmd_invalid_msgs;
        my_info.prior_cmd_invalid_msgs.set_group_to_follow(&parent_current);
        let prior = my_info.prior_cmd_invalid_msgs;
        my_info.ii.current_cmd_invalid_msgs.set_group_to_follow(&prior);
    } else {
        // C nulls pointers freed with TopTransactionContext; retained-capacity
        // buffers just clear.
        state.msg_arrays[crate::CAT_CACHE_MSGS].clear();
        state.msg_arrays[crate::REL_CACHE_MSGS].clear();
    }

    state
        .trans_stack
        .try_reserve(1)
        .map_err(|_| state.mcx.oom(size_of::<TransInvalidationInfo>()))?;
    state.trans_stack.push(my_info);
    Ok(InfoRef::Trans)
}

pub(crate) fn prepare_inplace_invalidation_state(state: &mut InvalState<'_>) -> InfoRef {
    debug_assert!(state.inplace_info.is_none());

    let mut my_info = InvalidationInfo::default();

    if let Some(top) = state.trans_stack.last() {
        let top_current = top.ii.current_cmd_invalid_msgs;
        my_info.current_cmd_invalid_msgs.set_group_to_follow(&top_current);
    } else {
        state.msg_arrays[crate::CAT_CACHE_MSGS].clear();
        state.msg_arrays[crate::REL_CACHE_MSGS].clear();
    }

    state.inplace_info = Some(my_info);
    InfoRef::Inplace
}
