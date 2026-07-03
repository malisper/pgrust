use mcx::{Mcx, PgVec};
use types_core::primitive::OidIsValid;
use types_core::Oid;
use types_error::PgResult;
use types_storage::SharedInvalidationMessage;

use crate::local::LocalExecuteInvalidationMessage;
use crate::msgs::{append_invalidation_messages, subgroup_slice, InvalidationMsgsGroup};
use crate::{with_state, InvalState, CAT_CACHE_MSGS, REL_CACHE_MSGS};

// rmgrlist.h / xact.h.
pub(crate) const RM_XACT_ID: u8 = 1;
pub(crate) const XLOG_XACT_INVALIDATIONS: u8 = 0x60;

pub(crate) const REPLAY_CHUNK: usize = 32;

// Borrow released around each execute (it re-enters inval): one short borrow
// + one memcpy per 32-message chunk into stack scratch, dispatch borrow-free.
// The walk bound is fixed at entry (C's ProcessMessageSubGroup `_endmsg`) and
// executes never mutate registered messages (they only append), so the chunk
// image cannot go stale: mid-walk registrations land behind the fixed bound
// on both sides.
#[inline]
pub(crate) fn process_group_with(
    group: &InvalidationMsgsGroup,
    func: impl FnMut(&SharedInvalidationMessage) -> PgResult<()>,
) -> PgResult<()> {
    // Inline guard: the empty group (every no-DDL CommandEnd/abort) must not
    // pay the outlined walk's frame + chunk-scratch setup.
    if group.num_in_group() == 0 {
        return Ok(());
    }
    process_group_slow(group, func)
}

fn process_group_slow(
    group: &InvalidationMsgsGroup,
    mut func: impl FnMut(&SharedInvalidationMessage) -> PgResult<()>,
) -> PgResult<()> {
    use std::mem::MaybeUninit;
    for subgroup in [CAT_CACHE_MSGS, REL_CACHE_MSGS] {
        let end = group.num_in_sub_group(subgroup);
        let mut off = 0usize;
        while off < end {
            // MaybeUninit + one memcpy: a dummy-initialized array is a
            // per-element store loop (the enum stride defeats memset).
            let mut chunk: [MaybeUninit<SharedInvalidationMessage>; REPLAY_CHUNK] =
                [const { MaybeUninit::uninit() }; REPLAY_CHUNK];
            let n = (end - off).min(REPLAY_CHUNK);
            with_state(|state| {
                let msgs = &subgroup_slice(&state.msg_arrays, group, subgroup)[off..off + n];
                // SAFETY: n <= REPLAY_CHUNK; src and dst are distinct objects.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        msgs.as_ptr(),
                        chunk.as_mut_ptr().cast::<SharedInvalidationMessage>(),
                        n,
                    );
                }
            });
            // SAFETY: chunk[..n] fully written under the borrow above.
            let msgs = unsafe {
                std::slice::from_raw_parts(chunk.as_ptr().cast::<SharedInvalidationMessage>(), n)
            };
            for msg in msgs {
                func(msg)?;
            }
            off += n;
        }
    }
    Ok(())
}

pub(crate) fn process_group_locally(
    select: impl Fn(&InvalState<'_>) -> Option<InvalidationMsgsGroup>,
) -> PgResult<()> {
    let Some(group) = with_state(|state| select(state)) else {
        return Ok(());
    };
    process_group_with(&group, LocalExecuteInvalidationMessage)
}

// sinval send never re-enters inval: dense subgroup slices go straight to the
// seam under the live borrow — alloc-free, like C.
fn send_group(state: &InvalState<'_>, group: &InvalidationMsgsGroup) -> PgResult<()> {
    for subgroup in [CAT_CACHE_MSGS, REL_CACHE_MSGS] {
        let msgs = subgroup_slice(&state.msg_arrays, group, subgroup);
        if !msgs.is_empty() {
            sinval_seams::send_shared_invalid_messages::call(msgs)?;
        }
    }
    Ok(())
}

pub fn CommandEndInvalidationMessages() -> PgResult<()> {
    let Some(group) =
        with_state(|state| Some(state.trans_stack.last()?.ii.current_cmd_invalid_msgs))
    else {
        return Ok(());
    };
    process_group_with(&group, LocalExecuteInvalidationMessage)?;

    if transam_xlog_seams::xlog_logical_info_active::call() {
        LogLogicalInvalidations()?;
    }

    with_state(|state| {
        let info = state.trans_stack.last_mut().expect("checked non-empty");
        append_invalidation_messages(
            &mut info.prior_cmd_invalid_msgs,
            &mut info.ii.current_cmd_invalid_msgs,
        );
    });

    Ok(())
}

pub fn AtEOXact_Inval(isCommit: bool) -> PgResult<()> {
    enum Eox {
        Empty,
        Commit(bool),
        Abort(InvalidationMsgsGroup),
    }

    let action = with_state(|state| {
        state.inplace_info = None;
        match state.trans_stack.first() {
            None => Eox::Empty,
            Some(info) => {
                /* Must be at top of stack */
                debug_assert!(state.trans_stack.len() == 1 && info.my_level == 1);
                if isCommit {
                    Eox::Commit(info.ii.relcache_init_file_inval)
                } else {
                    Eox::Abort(info.prior_cmd_invalid_msgs)
                }
            }
        }
    });

    match action {
        Eox::Empty => return Ok(()),
        Eox::Commit(relcache_init_file_inval) => {
            if relcache_init_file_inval {
                relcache_seams::relation_cache_init_file_pre_invalidate::call()?;
            }

            with_state(|state| {
                let info = &mut state.trans_stack[0];
                append_invalidation_messages(
                    &mut info.prior_cmd_invalid_msgs,
                    &mut info.ii.current_cmd_invalid_msgs,
                );
                let group = info.prior_cmd_invalid_msgs;
                send_group(state, &group)
            })?;

            if relcache_init_file_inval {
                relcache_seams::relation_cache_init_file_post_invalidate::call()?;
            }
        }
        Eox::Abort(group) => {
            process_group_with(&group, LocalExecuteInvalidationMessage)?;
        }
    }

    // C frees the arrays with TopTransactionContext; capacity retained here
    // (alloc-free steady state per commit).
    with_state(|state| {
        state.trans_stack.clear();
        for arr in &mut state.msg_arrays {
            arr.clear();
        }
    });

    Ok(())
}

pub fn AtEOSubXact_Inval(isCommit: bool) -> PgResult<()> {
    let info_level = match with_state(|state| {
        if isCommit {
            debug_assert!(state.inplace_info.is_none());
        } else {
            state.inplace_info = None;
        }
        state.trans_stack.last().map(|top| top.my_level)
    }) {
        Some(level) => level,
        None => return Ok(()),
    };

    let my_level = xact_seams::get_current_transaction_nest_level::call();

    if info_level != my_level {
        debug_assert!(info_level < my_level);
        return Ok(());
    }

    if isCommit {
        /* If CurrentCmdInvalidMsgs still has anything, fix it */
        CommandEndInvalidationMessages()?;

        // Entries are lazy: no parent at the adjacent level, just re-level.
        let parent_is_adjacent = with_state(|state| {
            let len = state.trans_stack.len();
            len >= 2 && state.trans_stack[len - 2].my_level >= my_level - 1
        });

        if !parent_is_adjacent {
            with_state(|state| {
                state.trans_stack.last_mut().expect("checked non-empty").my_level -= 1;
            });
            return Ok(());
        }

        with_state(|state| {
            let len = state.trans_stack.len();
            let (head, tail) = state.trans_stack.split_at_mut(len - 1);
            let parent = &mut head[len - 2];
            let myinfo = &mut tail[0];

            append_invalidation_messages(
                &mut parent.prior_cmd_invalid_msgs,
                &mut myinfo.prior_cmd_invalid_msgs,
            );

            parent
                .ii
                .current_cmd_invalid_msgs
                .set_group_to_follow(&parent.prior_cmd_invalid_msgs);

            if myinfo.ii.relcache_init_file_inval {
                parent.ii.relcache_init_file_inval = true;
            }

            state.trans_stack.pop();
        });
    } else {
        process_group_locally(|state| Some(state.trans_stack.last()?.prior_cmd_invalid_msgs))?;

        with_state(|state| {
            state.trans_stack.pop();
        });
    }

    Ok(())
}

pub fn PreInplace_Inval() -> PgResult<()> {
    let pre = with_state(|state| {
        state
            .inplace_info
            .as_ref()
            .is_some_and(|info| info.relcache_init_file_inval)
    });
    if pre {
        relcache_seams::relation_cache_init_file_pre_invalidate::call()?;
    }
    Ok(())
}

pub fn AtInplace_Inval() -> PgResult<()> {
    let relcache_init_file_inval = match with_state(|state| {
        state.inplace_info.as_ref().map(|info| info.relcache_init_file_inval)
    }) {
        Some(flag) => flag,
        None => return Ok(()),
    };

    with_state(|state| {
        let group = state
            .inplace_info
            .as_ref()
            .expect("inplace_info checked above")
            .current_cmd_invalid_msgs;
        send_group(state, &group)
    })?;

    if relcache_init_file_inval {
        relcache_seams::relation_cache_init_file_post_invalidate::call()?;
    }

    with_state(forget_inplace_invalidation_state);
    Ok(())
}

pub fn ForgetInplace_Inval() {
    with_state(forget_inplace_invalidation_state);
}

// C only nulls inplaceInvalInfo; the owned arrays physically hold the stash,
// so roll them back to the stash start. An aborted subtransaction leaves dead
// slots past the live cursor (cursor-write overwrites them, C shape), so len
// may exceed nextmsg; the stash still ends the live region, making the
// firstmsg rollback sound.
pub(crate) fn forget_inplace_invalidation_state(state: &mut InvalState<'_>) {
    if let Some(info) = state.inplace_info.take() {
        let group = info.current_cmd_invalid_msgs;
        for subgroup in [CAT_CACHE_MSGS, REL_CACHE_MSGS] {
            debug_assert!(state.msg_arrays[subgroup].len() >= group.nextmsg[subgroup]);
            state.msg_arrays[subgroup].truncate(group.firstmsg[subgroup]);
        }
    }
}

pub fn PostPrepare_Inval() -> PgResult<()> {
    AtEOXact_Inval(false)
}

// AtEOXact_Inval processing order (Prior:Cat, Current:Cat, Prior:Rel,
// Current:Rel); C allocates in CurTransactionContext — the caller's mcx.
pub fn xactGetCommittedInvalidationMessages<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<(PgVec<'mcx, SharedInvalidationMessage>, bool)> {
    with_state(|state| {
        let info = match state.trans_stack.first() {
            Some(info) => info,
            None => return Ok((PgVec::new_in(mcx), false)),
        };
        debug_assert!(state.trans_stack.len() == 1 && info.my_level == 1);

        let relcache_init_file_inval = info.ii.relcache_init_file_inval;
        let prior = info.prior_cmd_invalid_msgs;
        let current = info.ii.current_cmd_invalid_msgs;
        let nummsgs = prior.num_in_group() + current.num_in_group();

        let mut msgarray: PgVec<'mcx, SharedInvalidationMessage> = PgVec::new_in(mcx);
        msgarray
            .try_reserve_exact(nummsgs)
            .map_err(|_| mcx.oom(nummsgs * size_of::<SharedInvalidationMessage>()))?;
        for (group, subgroup) in [
            (&prior, CAT_CACHE_MSGS),
            (&current, CAT_CACHE_MSGS),
            (&prior, REL_CACHE_MSGS),
            (&current, REL_CACHE_MSGS),
        ] {
            msgarray.extend_from_slice(subgroup_slice(&state.msg_arrays, group, subgroup));
        }
        debug_assert_eq!(msgarray.len(), nummsgs);
        Ok((msgarray, relcache_init_file_inval))
    })
}

pub fn inplaceGetInvalidationMessages<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<(PgVec<'mcx, SharedInvalidationMessage>, bool)> {
    with_state(|state| {
        let info = match state.inplace_info.as_ref() {
            Some(info) => info,
            None => return Ok((PgVec::new_in(mcx), false)),
        };
        let relcache_init_file_inval = info.relcache_init_file_inval;
        let group = info.current_cmd_invalid_msgs;
        let nummsgs = group.num_in_group();

        let mut msgarray: PgVec<'mcx, SharedInvalidationMessage> = PgVec::new_in(mcx);
        msgarray
            .try_reserve_exact(nummsgs)
            .map_err(|_| mcx.oom(nummsgs * size_of::<SharedInvalidationMessage>()))?;
        for subgroup in [CAT_CACHE_MSGS, REL_CACHE_MSGS] {
            msgarray.extend_from_slice(subgroup_slice(&state.msg_arrays, &group, subgroup));
        }
        debug_assert_eq!(msgarray.len(), nummsgs);
        Ok((msgarray, relcache_init_file_inval))
    })
}

pub fn ProcessCommittedInvalidationMessages(
    msgs: &[SharedInvalidationMessage],
    relcache_init_file_inval: bool,
    dbid: Oid,
    tsid: Oid,
) -> PgResult<()> {
    if msgs.is_empty() {
        return Ok(());
    }

    if relcache_init_file_inval {
        // C pokes DatabasePath directly (SetDatabasePath is once-per-backend);
        // recovery-cold, so a per-call context for the path is fine.
        if OidIsValid(dbid) {
            let ctx = mcx::MemoryContext::new("ProcessCommittedInvalidationMessages");
            let path = relpath_seams::get_database_path::call(ctx.mcx(), dbid, tsid)?;
            miscinit_seams::set_database_path::call(&path);
        }

        relcache_seams::relation_cache_init_file_pre_invalidate::call()?;

        if OidIsValid(dbid) {
            miscinit_seams::clear_database_path::call();
        }
    }

    sinval_seams::send_shared_invalid_messages::call(msgs)?;

    if relcache_init_file_inval {
        relcache_seams::relation_cache_init_file_post_invalidate::call()?;
    }

    Ok(())
}

pub fn LogLogicalInvalidations() -> PgResult<()> {
    with_state(|state| {
        let group = match state.trans_stack.last() {
            Some(top) => top.ii.current_cmd_invalid_msgs,
            None => return Ok(()),
        };
        let nmsgs = group.num_in_group();
        if nmsgs == 0 {
            return Ok(());
        }

        // Reused scratch wire images; xlog_insert never re-enters inval, so
        // the borrow stays held.
        let InvalState { msg_arrays, wal_scratch, .. } = state;
        for subgroup in [CAT_CACHE_MSGS, REL_CACHE_MSGS] {
            let buf = &mut wal_scratch[subgroup];
            buf.clear();
            for msg in subgroup_slice(msg_arrays, &group, subgroup) {
                mcx::vec_append_bytes(buf, &msg.to_wire_bytes())?;
            }
        }

        let header = (nmsgs as i32).to_ne_bytes();
        let mut fragments: [&[u8]; 3] = [&header, &[], &[]];
        let mut n = 1;
        for subgroup in [CAT_CACHE_MSGS, REL_CACHE_MSGS] {
            if !wal_scratch[subgroup].is_empty() {
                fragments[n] = &wal_scratch[subgroup];
                n += 1;
            }
        }
        xloginsert_seams::xlog_insert::call(RM_XACT_ID, XLOG_XACT_INVALIDATIONS, &fragments[..n])?;
        Ok(())
    })
}
