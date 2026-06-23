//! End-of-(sub)transaction, inplace, and 2PC/recovery processing (inval.c
//! `AtEOXact_Inval`, `AtEOSubXact_Inval`, `CommandEndInvalidationMessages`,
//! the inplace `PreInplace_Inval` / `AtInplace_Inval` / `ForgetInplace_Inval`,
//! `PostPrepare_Inval`, `xactGetCommittedInvalidationMessages`,
//! `inplaceGetInvalidationMessages`, `ProcessCommittedInvalidationMessages`,
//! and `LogLogicalInvalidations`).

use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_storage::SharedInvalidationMessage;
use ::wal::wal::RM_XACT_ID;
use ::wal::xact::XLOG_XACT_INVALIDATIONS;

use transam_xact_seams as xact_seams;
use transam_xlog_seams as xlog_seams;
use xloginsert_seams as xloginsert_seams;
use sinval_seams as sinval_seams;
use common_relpath_seams as relpath_seams;
use relcache_seams as relcache_seams;
use miscinit_seams as miscinit_seams;

use crate::msgs::{
    append_invalidation_messages, num_messages_in_subgroup_slice,
};
use crate::{with_state, CAT_CACHE_MSGS, REL_CACHE_MSGS};

/// `OidIsValid(InvalidOid)` is false; `InvalidOid == 0`.
const INVALID_OID: Oid = 0;

/// OOM building a contiguous SI message array (C: the `palloc`/`MemoryContextAlloc`
/// failure that `ereport(ERROR)`s).
fn oom() -> PgError {
    PgError::error("out of memory collecting invalidation messages")
}

/// Discard the inplace invalidation info and physically drop its messages from
/// the dense arrays (re-establishing the `nextmsg == len` invariant).
pub(crate) fn forget_inplace_invalidation_state(state: &mut crate::InvalState<'_>) {
    // C just sets inplaceInvalInfo = NULL and relies on the surrounding memory
    // context being reset.  In the owned model the inplace info's messages were
    // appended to the dense arrays, so dropping the info must also roll the
    // dense arrays back so the `nextmsg == len` invariant holds.
    if let Some(info) = state.inplace_inval_info.take() {
        let group = &info.current_cmd_invalid_msgs;
        for subgroup in [CAT_CACHE_MSGS, REL_CACHE_MSGS] {
            let n = group.num_messages_in_sub_group(subgroup);
            let arr = &mut state.message_arrays[subgroup].msgs;
            debug_assert_eq!(arr.len(), group.nextmsg[subgroup]);
            arr.truncate(group.firstmsg[subgroup]);
            let _ = n;
        }
    }
}

/// `CommandEndInvalidationMessages` — make the just-completed command's catalog
/// changes visible locally.
pub fn CommandEndInvalidationMessages() -> PgResult<()> {
    // You might think this shouldn't be called outside any transaction, but
    // bootstrap does it, and also ABORT issued when not in a transaction. So
    // just quietly return if no state to work on.
    if with_state(|state| state.trans_inval_stack.is_empty()) {
        return Ok(());
    }

    // ProcessInvalidationMessages(&CurrentCmdInvalidMsgs,
    //                             LocalExecuteInvalidationMessage)
    let cur_msgs = with_state(|state| {
        let top = state.trans_inval_stack.len() - 1;
        let group = state.trans_inval_stack[top].ii.current_cmd_invalid_msgs;
        crate::local_list::collect_group_messages(state.mcx, &state.message_arrays, &group)
    })?;
    for msg in &cur_msgs {
        crate::local_list::LocalExecuteInvalidationMessage(msg)?;
    }

    // WAL Log per-command invalidation messages for wal_level=logical
    if xlog_seams::xlog_logical_info_active::call() {
        LogLogicalInvalidations()?;
    }

    // AppendInvalidationMessages(&PriorCmdInvalidMsgs, &CurrentCmdInvalidMsgs)
    with_state(|state| {
        let top = state.trans_inval_stack.len() - 1;
        let info = &mut state.trans_inval_stack[top];
        append_invalidation_messages(
            &mut info.prior_cmd_invalid_msgs,
            &mut info.ii.current_cmd_invalid_msgs,
        );
    });

    Ok(())
}

/// `AtEOXact_Inval` — process queued invalidation messages at end of main
/// transaction.
pub fn AtEOXact_Inval(isCommit: bool) -> PgResult<()> {
    // inplaceInvalInfo = NULL
    with_state(|state| {
        state.inplace_inval_info = None;
    });

    // Quick exit if no transactional messages
    if with_state(|state| state.trans_inval_stack.is_empty()) {
        return Ok(());
    }

    // Must be at top of stack: my_level == 1 && parent == NULL
    debug_assert!(with_state(|state| {
        state.trans_inval_stack.len() == 1 && state.trans_inval_stack[0].my_level == 1
    }));

    if isCommit {
        // Relcache init file invalidation requires processing both before and
        // after we send the SI messages.  However, we need not do anything
        // unless we committed.
        let relcache_init_file_inval =
            with_state(|state| state.trans_inval_stack[0].ii.relcache_init_file_inval);

        if relcache_init_file_inval {
            relcache_seams::relation_cache_init_file_pre_invalidate::call()?;
        }

        // AppendInvalidationMessages(&PriorCmdInvalidMsgs, &CurrentCmdInvalidMsgs)
        with_state(|state| {
            let info = &mut state.trans_inval_stack[0];
            append_invalidation_messages(
                &mut info.prior_cmd_invalid_msgs,
                &mut info.ii.current_cmd_invalid_msgs,
            );
        });

        // ProcessInvalidationMessagesMulti(&PriorCmdInvalidMsgs,
        //                                  SendSharedInvalidMessages)
        let batches = with_state(|state| {
            let group = state.trans_inval_stack[0].prior_cmd_invalid_msgs;
            crate::local_list::collect_group_messages_multi(
                state.mcx,
                &state.message_arrays,
                &group,
            )
        })?;
        for batch in &batches {
            sinval_seams::send_shared_invalid_messages::call(batch)?;
        }

        if relcache_init_file_inval {
            relcache_seams::relation_cache_init_file_post_invalidate::call()?;
        }
    } else {
        // ProcessInvalidationMessages(&PriorCmdInvalidMsgs,
        //                             LocalExecuteInvalidationMessage)
        let prior_msgs = with_state(|state| {
            let group = state.trans_inval_stack[0].prior_cmd_invalid_msgs;
            crate::local_list::collect_group_messages(state.mcx, &state.message_arrays, &group)
        })?;
        for msg in &prior_msgs {
            crate::local_list::LocalExecuteInvalidationMessage(msg)?;
        }
    }

    // transInvalInfo = NULL — pop the whole stack (and reset the dense arrays,
    // since C relies on TopTransactionContext being emptied).
    with_state(|state| {
        state.trans_inval_stack.clear();
        for arr in &mut state.message_arrays {
            arr.msgs.clear();
        }
    });

    Ok(())
}

/// `AtEOSubXact_Inval` — process queued invalidation messages at subtransaction
/// end.
pub fn AtEOSubXact_Inval(isCommit: bool) -> PgResult<()> {
    // Successful inplace update must clear this, but we clear it on abort.
    if isCommit {
        debug_assert!(with_state(|state| state.inplace_inval_info.is_none()));
    } else {
        with_state(|state| {
            state.inplace_inval_info = None;
        });
    }

    // Quick exit if no transactional messages.
    if with_state(|state| state.trans_inval_stack.is_empty()) {
        return Ok(());
    }

    let my_level = xact_seams::get_current_transaction_nest_level::call();

    // Also bail out quickly if messages are not for this level.
    let info_level = with_state(|state| {
        let top = state.trans_inval_stack.len() - 1;
        state.trans_inval_stack[top].my_level
    });
    if info_level != my_level {
        debug_assert!(info_level < my_level);
        return Ok(());
    }

    if isCommit {
        // If CurrentCmdInvalidMsgs still has anything, fix it
        CommandEndInvalidationMessages()?;

        // We create invalidation stack entries lazily, so the parent might not
        // have one.  Instead of creating one, moving all the data over, and
        // then freeing our own, we can just adjust the level of our own entry.
        //
        // In the stack model, the parent is the element below the top; "parent
        // has one and at the right level" means the stack has >= 2 entries and
        // the entry below the top has my_level == my_level - 1.
        let parent_is_adjacent = with_state(|state| {
            let len = state.trans_inval_stack.len();
            len >= 2 && state.trans_inval_stack[len - 2].my_level >= my_level - 1
        });

        if !parent_is_adjacent {
            with_state(|state| {
                let top = state.trans_inval_stack.len() - 1;
                state.trans_inval_stack[top].my_level -= 1;
            });
            return Ok(());
        }

        with_state(|state| {
            let len = state.trans_inval_stack.len();
            // Pass up my inval messages to parent.  Notice that we stick them
            // in PriorCmdInvalidMsgs, not CurrentCmdInvalidMsgs, since they've
            // already been locally processed.
            //
            // We split the parent and my entries out of the stack so we can
            // borrow both mutably at once.
            let (head, tail) = state.trans_inval_stack.as_mut_slice().split_at_mut(len - 1);
            let parent = &mut head[len - 2];
            let myinfo = &mut tail[0];

            append_invalidation_messages(
                &mut parent.prior_cmd_invalid_msgs,
                &mut myinfo.prior_cmd_invalid_msgs,
            );

            // Must readjust parent's CurrentCmdInvalidMsgs indexes now
            parent
                .ii
                .current_cmd_invalid_msgs
                .set_group_to_follow(&parent.prior_cmd_invalid_msgs);

            // Pending relcache inval becomes parent's problem too
            if myinfo.ii.relcache_init_file_inval {
                parent.ii.relcache_init_file_inval = true;
            }

            // Pop the transaction state stack
            state.trans_inval_stack.pop();
        });
    } else {
        // ProcessInvalidationMessages(&PriorCmdInvalidMsgs,
        //                             LocalExecuteInvalidationMessage)
        let prior_msgs = with_state(|state| {
            let top = state.trans_inval_stack.len() - 1;
            let group = state.trans_inval_stack[top].prior_cmd_invalid_msgs;
            crate::local_list::collect_group_messages(state.mcx, &state.message_arrays, &group)
        })?;
        for msg in &prior_msgs {
            crate::local_list::LocalExecuteInvalidationMessage(msg)?;
        }

        // Pop the transaction state stack
        with_state(|state| {
            state.trans_inval_stack.pop();
        });
    }

    Ok(())
}

/// `PreInplace_Inval`.
pub fn PreInplace_Inval() -> PgResult<()> {
    // Assert(CritSectionCount == 0) — elided (no CritSectionCount accessor here).
    let pre = with_state(|state| {
        state
            .inplace_inval_info
            .as_ref()
            .is_some_and(|info| info.relcache_init_file_inval)
    });
    if pre {
        relcache_seams::relation_cache_init_file_pre_invalidate::call()?;
    }
    Ok(())
}

/// `AtInplace_Inval`.
pub fn AtInplace_Inval() -> PgResult<()> {
    // Assert(CritSectionCount > 0) — elided.

    if with_state(|state| state.inplace_inval_info.is_none()) {
        return Ok(());
    }

    // ProcessInvalidationMessagesMulti(&CurrentCmdInvalidMsgs,
    //                                  SendSharedInvalidMessages)
    let batches = with_state(|state| {
        let group = state
            .inplace_inval_info
            .as_ref()
            .expect("inplace_inval_info is Some")
            .current_cmd_invalid_msgs;
        crate::local_list::collect_group_messages_multi(state.mcx, &state.message_arrays, &group)
    })?;
    for batch in &batches {
        sinval_seams::send_shared_invalid_messages::call(batch)?;
    }

    let relcache_init_file_inval = with_state(|state| {
        state
            .inplace_inval_info
            .as_ref()
            .expect("inplace_inval_info is Some")
            .relcache_init_file_inval
    });
    if relcache_init_file_inval {
        relcache_seams::relation_cache_init_file_post_invalidate::call()?;
    }

    // inplaceInvalInfo = NULL
    with_state(|state| {
        state.inplace_inval_info = None;
    });

    Ok(())
}

/// `ForgetInplace_Inval`.
pub fn ForgetInplace_Inval() {
    // inplaceInvalInfo = NULL — in the owned model dropping the inplace info
    // also rolls its messages off the dense arrays.
    with_state(forget_inplace_invalidation_state);
}

/// `PostPrepare_Inval`.
///
/// Here, we want to act as though the transaction aborted, so that we will undo
/// any syscache changes it made, thereby bringing us into sync with the outside
/// world, which doesn't believe the transaction committed yet.
pub fn PostPrepare_Inval() -> PgResult<()> {
    AtEOXact_Inval(false)
}

/// `xactGetCommittedInvalidationMessages` — collect all pending messages into a
/// single contiguous array (in `AtEOXact_Inval` processing order) for the
/// commit WAL record; returns the messages and the `RelcacheInitFileInval` flag.
pub fn xactGetCommittedInvalidationMessages() -> PgResult<(Vec<SharedInvalidationMessage>, bool)> {
    // Quick exit if we haven't done anything with invalidation messages.
    if with_state(|state| state.trans_inval_stack.is_empty()) {
        return Ok((Vec::new(), false));
    }

    // Must be at top of stack: my_level == 1 && parent == NULL
    debug_assert!(with_state(|state| {
        state.trans_inval_stack.len() == 1 && state.trans_inval_stack[0].my_level == 1
    }));

    with_state(|state| {
        let info = &state.trans_inval_stack[0];
        let relcache_init_file_inval = info.ii.relcache_init_file_inval;

        let prior = &info.prior_cmd_invalid_msgs;
        let current = &info.ii.current_cmd_invalid_msgs;
        let arrays = &state.message_arrays;

        // Collect all the pending messages into a single contiguous array,
        // maintaining the order they would be processed in by AtEOXact_Inval():
        //   Prior:CatCache, Current:CatCache, Prior:RelCache, Current:RelCache.
        let nummsgs = prior.num_messages_in_group() + current.num_messages_in_group();

        let mut msgarray: Vec<SharedInvalidationMessage> = Vec::new();
        msgarray
            .try_reserve(nummsgs)
            .map_err(|_| oom())?;

        for (group, subgroup) in [
            (prior, CAT_CACHE_MSGS),
            (current, CAT_CACHE_MSGS),
            (prior, REL_CACHE_MSGS),
            (current, REL_CACHE_MSGS),
        ] {
            let slice = num_messages_in_subgroup_slice(arrays, group, subgroup);
            msgarray.extend_from_slice(slice);
        }

        debug_assert_eq!(msgarray.len(), nummsgs);
        Ok((msgarray, relcache_init_file_inval))
    })
}

/// `inplaceGetInvalidationMessages` — collect the inplace update's pending
/// messages for its WAL record.
pub fn inplaceGetInvalidationMessages() -> PgResult<(Vec<SharedInvalidationMessage>, bool)> {
    // Quick exit if we haven't done anything with invalidation messages.
    if with_state(|state| state.inplace_inval_info.is_none()) {
        return Ok((Vec::new(), false));
    }

    with_state(|state| {
        let info = state
            .inplace_inval_info
            .as_ref()
            .expect("inplace_inval_info is Some");
        let relcache_init_file_inval = info.relcache_init_file_inval;
        let group = &info.current_cmd_invalid_msgs;
        let arrays = &state.message_arrays;

        let nummsgs = group.num_messages_in_group();
        let mut msgarray: Vec<SharedInvalidationMessage> = Vec::new();
        msgarray
            .try_reserve(nummsgs)
            .map_err(|_| oom())?;

        for subgroup in [CAT_CACHE_MSGS, REL_CACHE_MSGS] {
            let slice = num_messages_in_subgroup_slice(arrays, group, subgroup);
            msgarray.extend_from_slice(slice);
        }

        debug_assert_eq!(msgarray.len(), nummsgs);
        Ok((msgarray, relcache_init_file_inval))
    })
}

/// `ProcessCommittedInvalidationMessages` — replay invalidation messages during
/// recovery (`xact_redo_commit` / `standby_redo`).
pub fn ProcessCommittedInvalidationMessages(
    msgs: &[SharedInvalidationMessage],
    nmsgs: i32,
    relcache_init_file_inval: bool,
    dbid: Oid,
    tsid: Oid,
) -> PgResult<()> {
    if nmsgs <= 0 {
        return Ok(());
    }

    // elog(DEBUG4, "replaying commit with %d messages%s", ...) — omitted (no
    // debug logging surface here); behaviour is unaffected.

    if relcache_init_file_inval {
        // elog(DEBUG4, "removing relcache init files for database %u", dbid)

        // RelationCacheInitFilePreInvalidate, when the invalidation message is
        // for a specific database, requires DatabasePath to be set, but we
        // should not use SetDatabasePath during recovery, since it is intended
        // to be used only once by normal backends.  Hence, a quick hack: set
        // DatabasePath directly then unset after use.
        if dbid != INVALID_OID {
            // GetDatabasePath builds the path string; we hold it for the
            // duration of the set/use/clear dance, then drop it (C:
            // pfree(DatabasePath) below).
            let path = relpath_seams::get_database_path::call(dbid, tsid);
            miscinit_seams::set_database_path::call(path.as_str());
        }

        relcache_seams::relation_cache_init_file_pre_invalidate::call()?;

        if dbid != INVALID_OID {
            miscinit_seams::clear_database_path::call();
        }
    }

    sinval_seams::send_shared_invalid_messages::call(&msgs[..nmsgs as usize])?;

    if relcache_init_file_inval {
        relcache_seams::relation_cache_init_file_post_invalidate::call()?;
    }

    Ok(())
}

/// `LogLogicalInvalidations` — emit WAL for invalidations of the current command.
pub fn LogLogicalInvalidations() -> PgResult<()> {
    // Quick exit if we haven't done anything with invalidation messages.
    if with_state(|state| state.trans_inval_stack.is_empty()) {
        return Ok(());
    }

    // group = &transInvalInfo->ii.CurrentCmdInvalidMsgs
    let (nmsgs, cat_bytes, rel_bytes) = with_state(|state| -> PgResult<_> {
        let top = state.trans_inval_stack.len() - 1;
        let group = state.trans_inval_stack[top].ii.current_cmd_invalid_msgs;
        let nmsgs = group.num_messages_in_group();
        if nmsgs == 0 {
            return Ok((0usize, Vec::new(), Vec::new()));
        }

        let arrays = &state.message_arrays;
        let cat = num_messages_in_subgroup_slice(arrays, &group, CAT_CACHE_MSGS);
        let rel = num_messages_in_subgroup_slice(arrays, &group, REL_CACHE_MSGS);
        Ok((nmsgs, si_msgs_bytes(cat)?, si_msgs_bytes(rel)?))
    })?;

    if nmsgs > 0 {
        // xl_xact_invals { int nmsgs; SharedInvalidationMessage msgs[]; }
        // MinSizeOfXactInvals == offsetof(xl_xact_invals, msgs) == sizeof(int).
        let header = (nmsgs as i32).to_ne_bytes();

        // XLogBeginInsert(); XLogRegisterData(header); XLogRegisterData(cat
        // subgroup if any); XLogRegisterData(rel subgroup if any);
        // XLogInsert(RM_XACT_ID, XLOG_XACT_INVALIDATIONS).
        let mut fragments: Vec<&[u8]> = Vec::with_capacity(3);
        fragments.push(&header);
        if !cat_bytes.is_empty() {
            fragments.push(&cat_bytes);
        }
        if !rel_bytes.is_empty() {
            fragments.push(&rel_bytes);
        }

        xloginsert_seams::xlog_insert::call(
            RM_XACT_ID,
            XLOG_XACT_INVALIDATIONS,
            0,
            &fragments,
        )?;
    }

    Ok(())
}

/// Serialize a slice of SI messages to their on-disk wire image (the bytes
/// `XLogRegisterData` would copy from the dense array).
fn si_msgs_bytes(msgs: &[SharedInvalidationMessage]) -> PgResult<Vec<u8>> {
    let mut buf = Vec::new();
    buf.try_reserve(msgs.len() * ::types_storage::sinval::SHARED_INVALIDATION_MESSAGE_SIZE)
        .map_err(|_| oom())?;
    for msg in msgs {
        buf.extend_from_slice(&msg.to_wire_bytes());
    }
    Ok(buf)
}
