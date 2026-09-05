// Conformance witnesses for the audit-18.6 remediation batch
// b023-backend-replication-walsender-1 (docs/conformance/audit-18.6/
// remediation/lanes/b023-backend-replication-walsender-1.md). Each test
// asserts the C 18.6 walsender.c contract at the cited line; the fake wire
// and slot helpers are the UPLOAD_MANIFEST tests' (lib.rs `tests`).

use super::*;
use crate::tests::{slot_lock, upload_setup, INPUT, WIRE};

fn conformance_setup() {
    upload_setup();
    if !timestamp_seams::get_current_timestamp::is_installed() {
        timestamp_seams::get_current_timestamp::set(|| 0);
    }
    // The streaming helpers read these GUCs through their slots; the
    // walsender's own init_seams does not run in a unit test.
    guc_tables::vars::wal_sender_timeout.install_if_absent(guc_tables::GucVarAccessors {
        get: || 60 * 1000,
        set: |_| {},
    });
    guc_tables::vars::synchronous_commit.install_if_absent(guc_tables::GucVarAccessors {
        get: || guc_tables::consts::SYNCHRONOUS_COMMIT_REMOTE_FLUSH,
        set: |_| {},
    });
    // proc_exit is a seam; the tests below reach it on purpose and catch
    // the marker panic instead of exiting the test process.
    if !ipc_seams::proc_exit::is_installed() {
        ipc_seams::proc_exit::set(proc_exit_marker);
    }
    set_walsender_flags(false);
}

fn proc_exit_marker(code: i32, _pid: i32) -> ! {
    panic!("proc_exit({code}) reached");
}

fn wire() -> Vec<u8> {
    WIRE.with(|w| w.borrow().clone())
}

fn contains(hay: &[u8], needle: &[u8]) -> bool {
    hay.windows(needle.len()).any(|w| w == needle)
}

// walsender.c:346 WalSndErrorCleanup begins with LWLockReleaseAll(): a
// replication command that ERRORs while holding an LWLock (slot.c:412's
// duplicate-slot ERROR under ReplicationSlotAllocationLock) runs no
// transaction abort, so the walsender's own cleanup is the only release
// point. Verified finding fp-replication-slot-B-c1: the lock stayed held for
// the walsender's life and every slot create/drop and the checkpointer's
// CheckPointReplicationSlots blocked behind it.
#[test]
fn error_cleanup_releases_lwlocks_held_at_the_error() {
    let _g = slot_lock();
    conformance_setup();
    assert_eq!(MY_WAL_SND.get(), -1);
    InitWalSenderSlot();
    if resowner::AuxProcessResourceOwner().is_null() {
        resowner::CreateAuxProcessResourceOwner().expect("aux resource owner");
    }

    static LOCK: lwlock::LWLockPadded = lwlock::LWLockPadded::new_unlocked(0);
    lwlock::LWLockAcquire(&LOCK.lock, lwlock::LW_EXCLUSIVE, 0).expect("acquire");
    assert!(lwlock::LWLockHeldByMe(&LOCK.lock));

    WalSndErrorCleanup().expect("WalSndErrorCleanup");

    assert!(
        !lwlock::LWLockHeldByMe(&LOCK.lock),
        "WalSndErrorCleanup must release every LWLock held at the error (walsender.c:346 LWLockReleaseAll)"
    );
    assert_eq!(my_walsnd_state(), WalSndState::Startup);
    WalSndKill(0, 0);
}

// walsender.c:2461/2646: standby reply bodies are read with pq_getmsgint64 /
// pq_getmsgint / pq_getmsgbyte, which raise 08P01 "insufficient data left in
// message" (pqformat.c) on a short CopyData. A truncated 'r' or 'h' must be
// that ERROR, never an out-of-bounds panic.
#[test]
fn truncated_standby_reply_is_a_protocol_violation_error() {
    let cases: [(&str, &[u8], &str); 4] = [
        ("h + 3 bytes", b"h\x00\x00\x00", "insufficient data left in message"),
        ("r + 11 bytes", b"r\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", "insufficient data left in message"),
        // Four complete int64s, then the replyRequested byte is missing.
        ("r + 32 bytes", &[b'r'; 33], "no data left in message"),
        // reply_time + 3 int32s complete, the last epoch is cut short.
        ("h + 20 bytes", &[b'h'; 21], "insufficient data left in message"),
    ];
    for (what, body, expected) in cases {
        let err = replies::ProcessStandbyMessage(body)
            .err()
            .unwrap_or_else(|| panic!("{what}: truncated reply must ERROR"));
        assert_eq!(err.sqlstate(), types_error::ERRCODE_PROTOCOL_VIOLATION, "{what}");
        assert_eq!(err.message(), expected, "{what}");
    }
    // An empty CopyData has no message-type byte at all (pq_getmsgbyte).
    let err = replies::ProcessStandbyMessage(b"").err().expect("empty reply must ERROR");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_PROTOCOL_VIOLATION);
    assert_eq!(err.message(), "no data left in message");
}

// walsender.c:2312 ProcessRepliesIfAny: an unknown message type byte on the
// CopyBoth stream is ereport(FATAL) with errcode(ERRCODE_PROTOCOL_VIOLATION).
// FATAL never returns: errfinish's recovery action is proc_exit(1)
// (elog.c:660), which the harness's proc_exit seam turns into the marker
// panic caught below; the report itself is witnessed through emit_log_hook,
// which errfinish runs before exiting.
static FATAL_REPORTS: std::sync::Mutex<Vec<(types_error::ErrorLevel, types_error::SqlState, String)>> =
    std::sync::Mutex::new(Vec::new());

fn fatal_capture_hook(e: &types_error::PgError, _output_to_server: &mut bool) {
    FATAL_REPORTS
        .lock()
        .expect("fatal reports mutex")
        .push((e.level(), e.sqlstate(), e.message().to_string()));
}

// The unwind policy exempts cfg(test) items; its checker scans per file and
// this module's #[cfg(test)] sits on the `mod` line in lib.rs, so the
// exemption is restated on the item itself.
#[cfg(test)]
#[test]
fn invalid_standby_message_type_is_fatal_protocol_violation() {
    let _g = slot_lock();
    conformance_setup();
    STREAMING_DONE_RECEIVING.with(|c| c.set(false));
    INPUT.with(|q| q.borrow_mut().push_back(b"z".to_vec()));

    FATAL_REPORTS.lock().expect("fatal reports mutex").clear();
    // emit_log_hook is thread-local, so only this test's reports land here.
    let prev = elog::set_emit_log_hook(Some(fatal_capture_hook));
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        replies::ProcessRepliesIfAny()
    }));
    elog::set_emit_log_hook(prev);

    let payload = r.expect_err("'z' must be FATAL, which never returns");
    let exit = payload.downcast_ref::<String>().cloned().unwrap_or_default();
    assert_eq!(exit, "proc_exit(1) reached", "FATAL exits through proc_exit(1) (elog.c errfinish)");

    let reports = FATAL_REPORTS.lock().expect("fatal reports mutex").clone();
    let (level, sqlstate, message) = reports
        .iter()
        .find(|(level, _, _)| *level == types_error::FATAL)
        .cloned()
        .expect("the FATAL report reaches emit_log_hook before proc_exit");
    assert_eq!(level, types_error::FATAL);
    assert_eq!(sqlstate, types_error::ERRCODE_PROTOCOL_VIOLATION);
    assert_eq!(message, "invalid standby message type \"z\"");
}

// walsender.c:3565 WalSndDone: once everything sent has been replicated, the
// walsender ends the COPY with CommandComplete "COPY 0" (SetQueryCompletion
// CMDTAG_COPY + EndCommand + pq_flush) BEFORE proc_exit(0) — a standby or
// pg_receivewal sees a controlled end of stream, not a bare socket close.
// The catch_unwind below is a test harness catching the proc_exit marker
// panic; the unwind policy exempts cfg(test) items, but its checker scans per
// file and this module's #[cfg(test)] sits on the `mod` line in lib.rs, so
// the exemption is restated on the item itself.
#[cfg(test)]
#[test]
fn walsnd_done_sends_copy_command_complete_before_exit() {
    let _g = slot_lock();
    conformance_setup();
    assert_eq!(MY_WAL_SND.get(), -1);
    InitWalSenderSlot();

    SENT_PTR.with(|c| c.set(0x1000));
    my_walsnd().lock().expect("walsnd mutex").flush = 0x1000;
    WAL_SND_CAUGHT_UP.with(|c| c.set(true));
    WAITING_FOR_PING_RESPONSE.with(|c| c.set(false));

    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        streaming::WalSndDone(&mut |()| Ok(()))
    }));
    assert!(r.is_err(), "WalSndDone must proc_exit(0) once caught up and replicated");

    let w = wire();
    let expect = [b'C', 0, 0, 0, 11, b'C', b'O', b'P', b'Y', b' ', b'0', 0];
    assert!(
        contains(&w, &expect),
        "CommandComplete \"COPY 0\" must precede the exit (wire = {w:?})"
    );
    WalSndKill(0, 0);
}

// walsender.c:1717 WalSndUpdateProgress: an empty (skipped) transaction under
// synchronous replication with a sync standby defined sends
// WalSndKeepalive(false, lsn) at once, so the skipped transaction's commit
// does not wait for the next timeout-driven keepalive.
#[test]
fn update_progress_skipped_xact_sends_keepalive_under_syncrep() {
    let _g = slot_lock();
    conformance_setup();
    assert_eq!(MY_WAL_SND.get(), -1);
    InitWalSenderSlot();
    WalSndCtl()
        .sync_standbys_status
        .store(SYNC_STANDBY_INIT | SYNC_STANDBY_DEFINED, std::sync::atomic::Ordering::Relaxed);
    LAST_REPLY_TIMESTAMP.with(|c| c.set(0));
    WAITING_FOR_PING_RESPONSE.with(|c| c.set(false));
    SENT_PTR.with(|c| c.set(0x5000));
    WIRE.with(|w| w.borrow_mut().clear());

    let lsn: XLogRecPtr = 0x1234;
    logical_stream::wal_snd_update_progress(true, lsn, true).expect("update progress");
    pqcomm::pq_flush().expect("flush");

    let w = wire();
    // CopyData('k' walEnd=lsn sendtime requestReply=0), walEnd in network order.
    let mut expect = vec![b'd', 0, 0, 0, 22, b'k'];
    expect.extend_from_slice(&lsn.to_be_bytes());
    assert!(
        contains(&w, &expect),
        "skipped xact under syncrep must send keepalive at lsn {lsn:#x} (wire = {w:?})"
    );
    assert!(
        !WAITING_FOR_PING_RESPONSE.with(|c| c.get()),
        "the keepalive must not request a reply"
    );
    WalSndCtl().sync_standbys_status.store(0, std::sync::atomic::Ordering::Relaxed);
    WalSndKill(0, 0);
}

// Control for the test above: nothing is sent when no sync standby is defined
// (the keepalive is gated on SYNC_STANDBY_DEFINED, exactly as C).
#[test]
fn update_progress_skipped_xact_is_silent_without_sync_standbys() {
    let _g = slot_lock();
    conformance_setup();
    assert_eq!(MY_WAL_SND.get(), -1);
    InitWalSenderSlot();
    WalSndCtl().sync_standbys_status.store(SYNC_STANDBY_INIT, std::sync::atomic::Ordering::Relaxed);
    LAST_REPLY_TIMESTAMP.with(|c| c.set(0));
    WAITING_FOR_PING_RESPONSE.with(|c| c.set(false));
    WIRE.with(|w| w.borrow_mut().clear());

    logical_stream::wal_snd_update_progress(true, 0x1234, true).expect("update progress");
    pqcomm::pq_flush().expect("flush");
    assert!(wire().is_empty(), "no keepalive without a sync standby (wire = {:?})", wire());
    WalSndCtl().sync_standbys_status.store(0, std::sync::atomic::Ordering::Relaxed);
    WalSndKill(0, 0);
}
