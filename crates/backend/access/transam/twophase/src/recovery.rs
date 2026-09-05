use elog::ereport;
use lwlock::LW_EXCLUSIVE;
use types_core::{
    RepOriginId, TransactionId, TransactionIdFollows, TransactionIdFollowsOrEquals,
    TransactionIdPrecedes, XLogRecPtr, INVALID_PROC_NUMBER,
};
use types_error::{ErrorLevel, PgResult, ERRCODE_DATA_CORRUPTED, ERROR, LOG, WARNING};

use crate::codec::{BufferLayout, TwoPhaseFileHeader};
use crate::core::{
    buffer_layout, corrupt_guard, gxact_load_subxact_data, mark_as_preparing_guts, mark_as_prepared,
    max_prepared_error, process_records, remove_gxact, xlog_read_twophase_data, PostPrepare_Twophase,
};
use crate::files;
use crate::here;
use crate::state::{lock_twophase_state, unlock_twophase_state, TwoPhaseState, NO_GXACT};

fn orig_next_xid() -> TransactionId {
    types_core::FullTransactionId::from_u64(
        varsup::TransamVariables()
            .nextXid
            .load(std::sync::atomic::Ordering::Relaxed),
    )
    .xid()
}

fn decode_gid(buf: &[u8], layout: &BufferLayout, gidlen: u16) -> String {
    let g = &buf[layout.gid..layout.gid + gidlen as usize];
    let end = g.iter().position(|&b| b == 0).unwrap_or(g.len());
    String::from_utf8_lossy(&g[..end]).into_owned()
}

fn decode_children(buf: &[u8], layout: &BufferLayout, n: usize) -> Vec<TransactionId> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let o = layout.children + i * 4;
        v.push(TransactionId::from_ne_bytes(buf[o..o + 4].try_into().unwrap()));
    }
    v
}

/// `PrepareRedoAdd`: register a gxact from a prepare-record body during redo.
/// Unlike C (whose callers hold TwoPhaseStateLock), the installed entry takes
/// the lock itself; `locked` distinguishes the restoreTwoPhaseData caller.
pub(crate) fn prepare_redo_add_locked(
    buf: &[u8],
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    origin_id: RepOriginId,
) -> PgResult<()> {
    debug_assert!(transam_xlog::RecoveryInProgress());
    let hdr = corrupt_guard(TwoPhaseFileHeader::from_bytes(buf), "PrepareRedoAdd")?;
    let layout = buffer_layout(&hdr, buf, "PrepareRedoAdd")?;
    let gid = decode_gid(buf, &layout, hdr.gidlen);

    // 2PC data that already reached disk was restored by restoreTwoPhaseData;
    // skip the WAL copy to avoid duplicates.
    if start_lsn != 0 && files::twophase_file_exists(hdr.xid)? {
        let reached = xlogrecovery_seams::reached_consistency::call();
        let level: ErrorLevel = if reached { ERROR } else { WARNING };
        let (h, l) = ((start_lsn >> 32) as u32, start_lsn as u32);
        ereport(level)
            .errmsg(format!(
                "could not recover two-phase state file for transaction {}",
                hdr.xid
            ))
            .errdetail(format!(
                "Two-phase state file has been found in WAL record {h:X}/{l:X}, but this transaction has already been restored from disk."
            ))
            .finish(here("PrepareRedoAdd"))?;
        return Ok(());
    }

    let st = TwoPhaseState();
    let Some(idx) = st.pop_free() else {
        return Err(max_prepared_error("PrepareRedoAdd"));
    };
    let g = st.gxact(idx);
    unsafe { g.prepared_at.set(hdr.prepared_at) };
    unsafe { g.prepare_start_lsn.set(start_lsn) };
    unsafe { g.prepare_end_lsn.set(end_lsn) };
    unsafe { g.xid.set(hdr.xid) };
    unsafe { g.owner.set(hdr.owner) };
    unsafe { g.locking_backend.set(INVALID_PROC_NUMBER) };
    unsafe { g.valid.set(false) };
    unsafe { g.ondisk.set(start_lsn == 0) };
    unsafe { g.inredo.set(true) };
    let mut gidbuf = unsafe { g.gid.get() };
    gidbuf.set(&gid);
    unsafe { g.gid.set(gidbuf) };
    st.push_active(idx);

    if origin_id != 0 {
        origin_seams::replorigin_advance::call(origin_id, hdr.origin_lsn, end_lsn, false, false)?;
    }
    Ok(())
}

/// `PrepareRedoRemove`. Caller-locked variant (TwoPhaseStateLock exclusive).
pub(crate) fn prepare_redo_remove_locked(xid: TransactionId, give_warning: bool) -> PgResult<()> {
    let st = TwoPhaseState();
    let mut found = NO_GXACT;
    for i in 0..unsafe { st.num_prep_xacts.get() } {
        let idx = st.prep_xact(i);
        if unsafe { st.gxact(idx).xid.get() } == xid {
            debug_assert!(unsafe { st.gxact(idx).inredo.get() });
            found = idx;
            break;
        }
    }
    if found == NO_GXACT {
        return Ok(());
    }
    if unsafe { st.gxact(found).ondisk.get() } {
        files::remove_two_phase_file(xid, give_warning)?;
    }
    remove_gxact(found);
    Ok(())
}

/// `ProcessTwoPhaseBuffer`. Caller holds TwoPhaseStateLock exclusive.
fn process_two_phase_buffer(
    xid: TransactionId,
    prepare_start_lsn: XLogRecPtr,
    fromdisk: bool,
    set_parent: bool,
    set_next_xid: bool,
) -> PgResult<Option<Vec<u8>>> {
    if !fromdisk {
        debug_assert!(prepare_start_lsn != 0);
    }

    // Already processed?
    if transam::TransactionIdDidCommit(xid)? || transam::TransactionIdDidAbort(xid)? {
        if fromdisk {
            ereport(WARNING)
                .errmsg(format!(
                    "removing stale two-phase state file for transaction {xid}"
                ))
                .finish(here("ProcessTwoPhaseBuffer"))?;
            files::remove_two_phase_file(xid, true)?;
        } else {
            ereport(WARNING)
                .errmsg(format!(
                    "removing stale two-phase state from memory for transaction {xid}"
                ))
                .finish(here("ProcessTwoPhaseBuffer"))?;
            prepare_redo_remove_locked(xid, true)?;
        }
        return Ok(None);
    }

    // Reject XID if too new.
    if TransactionIdFollowsOrEquals(xid, orig_next_xid()) {
        if fromdisk {
            ereport(WARNING)
                .errmsg(format!(
                    "removing future two-phase state file for transaction {xid}"
                ))
                .finish(here("ProcessTwoPhaseBuffer"))?;
            files::remove_two_phase_file(xid, true)?;
        } else {
            ereport(WARNING)
                .errmsg(format!(
                    "removing future two-phase state from memory for transaction {xid}"
                ))
                .finish(here("ProcessTwoPhaseBuffer"))?;
            prepare_redo_remove_locked(xid, true)?;
        }
        return Ok(None);
    }

    let buf = if fromdisk {
        files::read_twophase_file(xid, false)?
            .expect("two-phase state file disappeared")
    } else {
        xlog_read_twophase_data(prepare_start_lsn)?
    };

    let hdr = corrupt_guard(TwoPhaseFileHeader::from_bytes(&buf), "ProcessTwoPhaseBuffer")?;
    if hdr.xid != xid {
        let msg = if fromdisk {
            format!("corrupted two-phase state file for transaction {xid}")
        } else {
            format!("corrupted two-phase state in memory for transaction {xid}")
        };
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(msg)
            .finish(here("ProcessTwoPhaseBuffer"))
            .unwrap_err());
    }

    let layout = buffer_layout(&hdr, &buf, "ProcessTwoPhaseBuffer")?;
    let subxids = decode_children(&buf, &layout, hdr.nsubxacts as usize);
    for &subxid in &subxids {
        debug_assert!(TransactionIdFollows(subxid, xid));
        if set_next_xid {
            varsup::AdvanceNextFullTransactionIdPastXid(subxid)?;
        }
        if set_parent {
            subtrans::SubTransSetParent(subxid, xid)?;
        }
    }

    Ok(Some(buf))
}

/// `restoreTwoPhaseData`: scan pg_twophase at the start of recovery.
pub fn restoreTwoPhaseData() -> PgResult<()> {
    lock_twophase_state(LW_EXCLUSIVE);
    let result = (|| -> PgResult<()> {
        let mut names = files::scan_twophase_dir()?;
        names.sort_unstable();
        for fxid in names {
            let xid = fxid as u32;
            let buf = process_two_phase_buffer(xid, 0, true, false, false)?;
            if let Some(buf) = buf {
                prepare_redo_add_locked(&buf, 0, 0, 0)?;
            }
        }
        Ok(())
    })();
    unlock_twophase_state();
    result
}

/// `PrescanPreparedTransactions(NULL, NULL)`: the oldest valid prepared XID
/// (or nextXid when none).
pub fn PrescanPreparedTransactions() -> PgResult<TransactionId> {
    Ok(prescan_prepared_transactions_impl()?.0)
}

/// `PrescanPreparedTransactions(&xids, &nxids)`: also collects the valid
/// prepared-xact XIDs for the hot-standby fake running-xacts snapshot.
pub fn PrescanPreparedTransactionsXids() -> PgResult<(TransactionId, Vec<TransactionId>)> {
    prescan_prepared_transactions_impl()
}

fn prescan_prepared_transactions_impl() -> PgResult<(TransactionId, Vec<TransactionId>)> {
    let orig_next = orig_next_xid();
    let mut result = orig_next;
    let mut xids: Vec<TransactionId> = Vec::new();

    let st = TwoPhaseState();
    lock_twophase_state(LW_EXCLUSIVE);
    let inner = (|| -> PgResult<()> {
        // C's plain `i++` loop: a swap-removing ProcessTwoPhaseBuffer leaves
        // the entry moved into slot i unscanned this pass — reproduced.
        let mut i = 0;
        while i < unsafe { st.num_prep_xacts.get() } {
            let idx = st.prep_xact(i);
            let g = st.gxact(idx);
            debug_assert!(unsafe { g.inredo.get() });
            let xid = unsafe { g.xid.get() };
            let buf = process_two_phase_buffer(
                xid,
                unsafe { g.prepare_start_lsn.get() },
                unsafe { g.ondisk.get() },
                false,
                true,
            )?;
            if buf.is_some() {
                if TransactionIdPrecedes(xid, result) {
                    result = xid;
                }
                xids.push(xid);
            }
            i += 1;
        }
        Ok(())
    })();
    unlock_twophase_state();
    inner?;
    Ok((result, xids))
}

/// `TwoPhaseGetXidByVirtualXID(vxid, &have_more)` (twophase.c:852).
pub fn TwoPhaseGetXidByVirtualXID(
    proc_number: ::types_core::ProcNumber,
    lxid: u32,
) -> PgResult<(TransactionId, bool)> {
    use std::sync::atomic::Ordering::Relaxed;
    let mut result = ::types_core::InvalidTransactionId;
    let mut have_more = false;

    let st = TwoPhaseState();
    lock_twophase_state(lwlock::LW_SHARED);
    for i in 0..unsafe { st.num_prep_xacts.get() } {
        let g = st.gxact(st.prep_xact(i));
        if !unsafe { g.valid.get() } {
            continue;
        }
        let proc = lmgr_proc::GetPGProcByNumber(unsafe { g.pgprocno.get() });
        // Startup process sets proc->vxid.procNumber to INVALID_PROC_NUMBER,
        // so redo-restored gxacts never match (C asserts !inredo on match).
        if proc.vxid.procNumber.load(Relaxed) == proc_number
            && proc.vxid.lxid.load(Relaxed) == lxid
        {
            debug_assert!(!unsafe { g.inredo.get() });
            if result != ::types_core::InvalidTransactionId {
                have_more = true;
                break;
            }
            result = unsafe { g.xid.get() };
        }
    }
    unlock_twophase_state();
    Ok((result, have_more))
}

/// `StandbyRecoverPreparedTransactions`: pg_subtrans setup during hot standby.
pub fn StandbyRecoverPreparedTransactions() -> PgResult<()> {
    lock_twophase_state(LW_EXCLUSIVE);
    let st = TwoPhaseState();
    let inner = (|| -> PgResult<()> {
        let mut i = 0;
        while i < unsafe { st.num_prep_xacts.get() } {
            let idx = st.prep_xact(i);
            let g = st.gxact(idx);
            debug_assert!(unsafe { g.inredo.get() });
            let _ = process_two_phase_buffer(
                unsafe { g.xid.get() },
                unsafe { g.prepare_start_lsn.get() },
                unsafe { g.ondisk.get() },
                true,
                false,
            )?;
            i += 1;
        }
        Ok(())
    })();
    unlock_twophase_state();
    inner
}

/// `RecoverPreparedTransactions`: reload full state (locks etc.) at the end of
/// recovery.
pub fn RecoverPreparedTransactions() -> PgResult<()> {
    let st = TwoPhaseState();
    lock_twophase_state(LW_EXCLUSIVE);
    let inner = (|| -> PgResult<()> {
        let mut i = 0;
        while i < unsafe { st.num_prep_xacts.get() } {
            let idx = st.prep_xact(i);
            let g = st.gxact(idx);
            let xid = unsafe { g.xid.get() };

            let Some(buf) = process_two_phase_buffer(
                xid,
                unsafe { g.prepare_start_lsn.get() },
                unsafe { g.ondisk.get() },
                true,
                false,
            )?
            else {
                i += 1;
                continue;
            };

            ereport(LOG)
                .errmsg(format!(
                    "recovering prepared transaction {xid} from shared memory"
                ))
                .finish(here("RecoverPreparedTransactions"))?;

            let hdr =
                corrupt_guard(TwoPhaseFileHeader::from_bytes(&buf), "RecoverPreparedTransactions")?;
            debug_assert_eq!(hdr.xid, xid);
            let layout = buffer_layout(&hdr, &buf, "RecoverPreparedTransactions")?;
            let gid = decode_gid(&buf, &layout, hdr.gidlen);
            let subxids = decode_children(&buf, &layout, hdr.nsubxacts as usize);

            mark_as_preparing_guts(idx, xid, &gid, hdr.prepared_at, hdr.owner, hdr.database);
            unsafe { g.inredo.set(false) };

            gxact_load_subxact_data(idx, &subxids);
            mark_as_prepared(idx, true)?;

            unlock_twophase_state();

            // Recover other state (notably locks) via the rmgr callbacks. The
            // lock is released across these steps, so on error the closure must
            // re-acquire it before propagating: the sole tail release below is
            // otherwise fired on an unheld lock, and TwoPhaseStateLock's release
            // helper `.expect()`s success (state.rs), so the recovering startup
            // process aborts (SIGABRT) instead of exiting cleanly with the
            // original error. C relies on LWLockReleaseAll tolerating the
            // not-held lock during the FATAL unwind (twophase.c:2136-2159); we
            // make the re-acquire explicit to keep the release balanced.
            let step = process_records(
                &buf,
                layout.records,
                xid,
                &twophase_rmgr::twophase_recover_callbacks,
            )
            .and_then(|()| {
                if xlogutils::InHotStandby() {
                    standby_seams::standby_release_lock_tree::call(xid, &subxids)?;
                }
                Ok(())
            });
            if let Err(e) = step {
                lock_twophase_state(LW_EXCLUSIVE);
                return Err(e);
            }

            PostPrepare_Twophase();

            lock_twophase_state(LW_EXCLUSIVE);
            i += 1;
        }
        Ok(())
    })();
    unlock_twophase_state();
    inner
}

/// `CheckPointTwoPhase(redo_horizon)`: move to-disk any gxact whose PREPARE is
/// at or before the horizon.
pub fn CheckPointTwoPhase(redo_horizon: XLogRecPtr) -> PgResult<()> {
    if twophase_config::max_prepared_xacts() <= 0 {
        return Ok(());
    }

    let st = TwoPhaseState();
    let mut serialized_xacts = 0u32;
    lock_twophase_state(lwlock::LW_SHARED);
    let inner = (|| -> PgResult<()> {
        for i in 0..unsafe { st.num_prep_xacts.get() } {
            let g = st.gxact(st.prep_xact(i));
            if (unsafe { g.valid.get() } || unsafe { g.inredo.get() })
                && !unsafe { g.ondisk.get() }
                && unsafe { g.prepare_end_lsn.get() } <= redo_horizon
            {
                let buf = xlog_read_twophase_data(unsafe { g.prepare_start_lsn.get() })?;
                files::recreate_two_phase_file(unsafe { g.xid.get() }, &buf)?;
                unsafe { g.ondisk.set(true) };
                unsafe { g.prepare_start_lsn.set(0) };
                unsafe { g.prepare_end_lsn.set(0) };
                serialized_xacts += 1;
            }
        }
        Ok(())
    })();
    unlock_twophase_state();
    inner?;

    files::fsync_twophase_dir()?;

    if guc_tables::vars::log_checkpoints.read() && serialized_xacts > 0 {
        let plural = if serialized_xacts == 1 { "" } else { "s" };
        ereport(LOG)
            .errmsg(format!(
                "{serialized_xacts} two-phase state file{plural} {} written for {} prepared transaction{plural}",
                if serialized_xacts == 1 { "was" } else { "were" },
                if serialized_xacts == 1 { "a long-running" } else { "long-running" },
            ))
            .finish(here("CheckPointTwoPhase"))?;
    }
    Ok(())
}
