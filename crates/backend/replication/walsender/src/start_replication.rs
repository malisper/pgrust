//! START_REPLICATION (physical + logical) and the standby flush-position helper.
//!
//! `GetStandbyFlushRecPtr` is genuine in-crate LSN arithmetic and is ported
//! here.  `StartReplication` / `StartLogicalReplication` set up the xlogreader,
//! acquire/validate the replication slot, install the decoding context, and
//! drive `WalSndLoop`; their bodies are dominated by unported subsystems
//! (xlogreader, slots, logical decoding, timeline history, libpq) and panic
//! precisely until those land.

#![allow(non_snake_case)]

use crate::core::{proc_get, StartReplicationCmd, TimeLineID, XLogRecPtr};
use crate::core::WalSndSendDataCallback;
use crate::{dest, slotsync, walrcvfuncs, xlog, xlogrecovery};

/// `static XLogRecPtr GetStandbyFlushRecPtr(TimeLineID *tli)`.
pub fn GetStandbyFlushRecPtr(tli: &mut TimeLineID) -> XLogRecPtr {
    debug_assert!(
        proc_get(|p| p.am_cascading_walsender)
            || slotsync::is_syncing_replication_slots::call()
    );

    // We can safely send what's already been replayed.  If walreceiver streams
    // WAL from the same timeline, we can also send what it has streamed but not
    // yet replayed.
    let (_written, receivePtr, receiveTLI) = walrcvfuncs::get_wal_rcv_flush_rec_ptr_full::call();
    let (replayPtr, replayTLI) = xlogrecovery::get_xlog_replay_rec_ptr_tli::call();

    *tli = replayTLI;

    let mut result = replayPtr;
    if receiveTLI == replayTLI && receivePtr > replayPtr {
        result = receivePtr;
    }
    result
}

/// `void WaitForStandbyConfirmation(XLogRecPtr moveto)` — wait for the standby
/// slots to confirm `moveto` (the logical walsender's failover-slot wait).
pub fn WaitForStandbyConfirmation(moveto: XLogRecPtr) -> types_error::PgResult<()> {
    crate::slot::wait_for_standby_confirmation::call(moveto)
}

/// `static void StartReplication(StartReplicationCmd *cmd)`.
pub fn StartReplication(cmd: &StartReplicationCmd) {
    use crate::core::{
        with_proc, InvalidXLogRecPtr, WalSndState, INT8OID, TEXTOID,
    };

    // create xlogreader for physical replication
    //   xlogreader = XLogReaderAllocate(...);
    // In the owned model there is no walsender-owned xlogreader allocation; the
    // WAL slice read is performed through the `wal_read` seam in
    // XLogSendPhysical.  (Nothing to allocate / no out-of-memory path here.)

    // We assume here that we're logging enough information in the WAL for
    // log-shipping, since this is checked in PostmasterMain().

    if let Some(slotname) = cmd.slotname.as_deref() {
        // ReplicationSlotAcquire(cmd->slotname, true, true);
        crate::slot::replication_slot_acquire::call(slotname, true, true)
            .expect("ReplicationSlotAcquire");
        // if (SlotIsLogical(MyReplicationSlot)) ereport(ERROR, ...);
        if !crate::slot::slot_is_physical::call() {
            utils_error::ereport(types_error::ERROR)
                .errcode(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(alloc::string::String::from(
                    "cannot use a logical replication slot for physical replication",
                ))
                .finish(types_error::ErrorLocation::new(
                    "walsender.c",
                    0,
                    "StartReplication",
                ))
                .expect("ereport(ERROR) logical-slot-for-physical");
        }

        // We don't need to verify the slot's restart_lsn here; instead we rely
        // on the caller requesting the starting point to use.
    }

    // Select the timeline.  If it was given explicitly by the client, use that.
    // Otherwise use the timeline of the last replayed record.
    //   am_cascading_walsender = RecoveryInProgress();
    let am_cascading = xlog::recovery_in_progress::call();
    with_proc(|p| p.am_cascading_walsender = am_cascading);

    let mut flush_tli: TimeLineID = 0;
    let flush_ptr: XLogRecPtr = if am_cascading {
        GetStandbyFlushRecPtr(&mut flush_tli)
    } else {
        let (ptr, tli) = xlog::get_flush_rec_ptr::call();
        flush_tli = tli;
        ptr
    };

    if cmd.timeline != 0 {
        // sendTimeLine = cmd->timeline;
        with_proc(|p| p.sendTimeLine = cmd.timeline);
        if cmd.timeline == flush_tli {
            with_proc(|p| {
                p.sendTimeLineIsHistoric = false;
                p.sendTimeLineValidUpto = InvalidXLogRecPtr;
            });
        } else {
            with_proc(|p| p.sendTimeLineIsHistoric = true);

            // Check that the timeline the client requested exists, and the
            // requested start location is on that timeline.
            //   timeLineHistory = readTimeLineHistory(FlushTLI);
            //   switchpoint = tliSwitchPoint(cmd->timeline, timeLineHistory,
            //                                &sendTimeLineNextTLI);
            //   list_free_deep(timeLineHistory);
            let (switchpoint, next_tli) =
                timeline_switch_point_history(flush_tli, cmd.timeline);
            with_proc(|p| p.sendTimeLineNextTLI = next_tli);

            // This is quite loose on purpose: only check we didn't fork off the
            // requested timeline before the switchpoint.
            if switchpoint != InvalidXLogRecPtr && switchpoint < cmd.startpoint {
                utils_error::ereport(types_error::ERROR)
                    .errmsg(alloc::format!(
                        "requested starting point {:X}/{:X} on timeline {} is not in this server's history",
                        (cmd.startpoint >> 32) as u32,
                        cmd.startpoint as u32,
                        cmd.timeline,
                    ))
                    .finish(types_error::ErrorLocation::new(
                        "walsender.c",
                        0,
                        "StartReplication",
                    ))
                    .expect("ereport(ERROR) startpoint-not-in-history");
            }
            with_proc(|p| p.sendTimeLineValidUpto = switchpoint);
        }
    } else {
        with_proc(|p| {
            p.sendTimeLine = flush_tli;
            p.sendTimeLineValidUpto = InvalidXLogRecPtr;
            p.sendTimeLineIsHistoric = false;
        });
    }

    // streamingDoneSending = streamingDoneReceiving = false;
    with_proc(|p| {
        p.streamingDoneSending = false;
        p.streamingDoneReceiving = false;
    });

    // If there is nothing to stream, don't even enter COPY mode.
    let (is_historic, valid_upto) =
        proc_get(|p| (p.sendTimeLineIsHistoric, p.sendTimeLineValidUpto));
    if !is_historic || cmd.startpoint < valid_upto {
        // Clear state for the initial catchup mode so monitoring can trigger
        // actions on later streaming-state changes.
        //   WalSndSetState(WALSNDSTATE_CATCHUP);
        crate::init::WalSndSetState(WalSndState::WALSNDSTATE_CATCHUP);

        // Send a CopyBothResponse message, and start streaming.
        //   pq_beginmessage(&buf, PqMsg_CopyBothResponse);
        //   pq_sendbyte(&buf, 0);          /* overall copy format = textual */
        //   pq_sendint16(&buf, 0);         /* natts = 0 */
        //   pq_endmessage(&buf);
        //   pq_flush();
        send_copy_both_response();
        crate::pq_flush();

        // Don't allow a request to stream from a future point in WAL that
        // hasn't been flushed to disk in this server yet.
        if flush_ptr < cmd.startpoint {
            utils_error::ereport(types_error::ERROR)
                .errmsg(alloc::format!(
                    "requested starting point {:X}/{:X} is ahead of the WAL flush position of this server {:X}/{:X}",
                    (cmd.startpoint >> 32) as u32,
                    cmd.startpoint as u32,
                    (flush_ptr >> 32) as u32,
                    flush_ptr as u32,
                ))
                .finish(types_error::ErrorLocation::new(
                    "walsender.c",
                    0,
                    "StartReplication",
                ))
                .expect("ereport(ERROR) startpoint-ahead-of-flush");
        }

        // Start streaming from the requested point.
        //   sentPtr = cmd->startpoint;
        with_proc(|p| p.sentPtr = cmd.startpoint);

        // Initialize shared memory status, too.
        crate::shmem_array::my_set_sentptr(cmd.startpoint);

        // SyncRepInitConfig();
        crate::sync_rep_init_config();

        // Main loop of walsender.
        //   replication_active = true;
        with_proc(|p| p.replication_active = 1);

        crate::mainloop::WalSndLoop(crate::physical::XLogSendPhysical as WalSndSendDataCallback);

        //   replication_active = false;
        with_proc(|p| p.replication_active = 0);
        if proc_get(|p| p.got_STOPPING) != 0 {
            crate::proc_exit(0);
        }
        crate::init::WalSndSetState(WalSndState::WALSNDSTATE_STARTUP);

        debug_assert!(proc_get(|p| p.streamingDoneSending && p.streamingDoneReceiving));
    }

    // if (cmd->slotname) ReplicationSlotRelease();
    if cmd.slotname.is_some() {
        crate::slot::replication_slot_release::call()
            .expect("ReplicationSlotRelease");
    }

    // Copy is finished now. Send a single-row result set indicating the next
    // timeline.
    if proc_get(|p| p.sendTimeLineIsHistoric) {
        let (valid_upto, next_tli) =
            proc_get(|p| (p.sendTimeLineValidUpto, p.sendTimeLineNextTLI));

        // snprintf(startpos_str, ..., "%X/%X", LSN_FORMAT_ARGS(sendTimeLineValidUpto));
        let startpos_str =
            alloc::format!("{:X}/{:X}", (valid_upto >> 32) as u32, valid_upto as u32);

        // The repo has no ambient memory context for the walsender command path,
        // so own one for the duration of this result set (as IdentifySystem
        // does).
        let ctx = mcx::MemoryContext::new("START_REPLICATION");
        let mcx = ctx.mcx();

        // dest = CreateDestReceiver(DestRemoteSimple);
        let dest =
            dest::create_dest_receiver::call(types_dest::CommandDest::RemoteSimple);

        // tupdesc = CreateTemplateTupleDesc(2);  int8 next_tli + text next_tli_startpos
        let mut tupdesc = tupdesc::CreateTemplateTupleDesc(mcx, 2)
            .expect("CreateTemplateTupleDesc(2)");
        tupdesc::TupleDescInitBuiltinEntry(
            &mut tupdesc, 1, "next_tli", INT8OID, -1, 0,
        )
        .expect("TupleDescInitBuiltinEntry(next_tli)");
        tupdesc::TupleDescInitBuiltinEntry(
            &mut tupdesc, 2, "next_tli_startpos", TEXTOID, -1, 0,
        )
        .expect("TupleDescInitBuiltinEntry(next_tli_startpos)");
        let tupdesc = Some(mcx::alloc_in(mcx, tupdesc).expect("alloc tupdesc"));

        // tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);
        let mut tstate = execTuples_seams::begin_tup_output_tupdesc::call(
            mcx,
            dest,
            tupdesc,
            nodes::TupleSlotKind::Virtual,
        )
        .expect("begin_tup_output_tupdesc");

        // values[0] = Int64GetDatum((int64) sendTimeLineNextTLI);
        // values[1] = CStringGetTextDatum(startpos_str);
        let v0 = types_tuple::Datum::from_i64(next_tli as i64);
        let v1 = varlena_seams::cstring_to_text_v::call(mcx, &startpos_str)
            .expect("cstring_to_text(next_tli_startpos)");
        let values = [v0, v1];
        let nulls = [false, false];

        // do_tup_output(tstate, values, nulls);
        execTuples_seams::do_tup_output::call(mcx, &mut tstate, &values, &nulls)
            .expect("do_tup_output");
        // end_tup_output(tstate);
        execTuples_seams::end_tup_output::call(mcx, tstate)
            .expect("end_tup_output");
    }

    // Send CommandComplete message.
    //   EndReplicationCommand("START_STREAMING");
    crate::command::end_replication_command_pub("START_STREAMING");
}

/// `pq_beginmessage(&buf, PqMsg_CopyBothResponse); pq_sendbyte(&buf, 0);
/// pq_sendint16(&buf, 0); pq_endmessage(&buf);` — the 3-byte CopyBothResponse
/// body `[overall_format=0, natts_hi=0, natts_lo=0]`.
fn send_copy_both_response() {
    // PqMsg_CopyBothResponse == 'W'.
    crate::pq_putmessage_copyboth_response();
}

/// `readTimeLineHistory(FlushTLI)` + `tliSwitchPoint(cmd->timeline, history,
/// &sendTimeLineNextTLI)` for the explicit-client-timeline path of
/// `StartReplication`.  The timeline-history read + switchpoint lookup are owned
/// by the unported mcx-threaded `readTimeLineHistory`/`tliSwitchPoint`; reached
/// only when a cascading client explicitly requests a historic timeline.
fn timeline_switch_point_history(
    _flush_tli: TimeLineID,
    _rqst_tli: TimeLineID,
) -> (XLogRecPtr, TimeLineID) {
    utils_error::ereport(types_error::ERROR)
        .errmsg(alloc::string::String::from(
            "START_REPLICATION ... TIMELINE <historic>: timeline-history \
             readTimeLineHistory/tliSwitchPoint is not yet ported",
        ))
        .finish(types_error::ErrorLocation::new(
            "walsender.c",
            0,
            "StartReplication",
        ))
        .expect("ereport(ERROR) historic-timeline-unported");
    unreachable!()
}

/// `static void StartLogicalReplication(StartReplicationCmd *cmd)`.
pub fn StartLogicalReplication(_cmd: &StartReplicationCmd) {
    panic!(
        "StartLogicalReplication: depends on unported logical decoding context \
         creation (CreateDecodingContext) + output plugin + WalSndLoop(XLogSendLogical)"
    );
}

/// `static void WalSndSegmentOpen(...)` — xlogreader segment-open callback.
pub fn WalSndSegmentOpen() {
    panic!("WalSndSegmentOpen: depends on unported xlogreader segment open");
}

/// `static int logical_read_xlog_page(...)` — xlogreader page-read callback.
pub fn logical_read_xlog_page() -> i32 {
    panic!("logical_read_xlog_page: depends on unported xlogreader page read");
}

/// `if (xlogreader) XLogReaderFree(xlogreader)` in WalSndErrorCleanup — close the
/// walsender's open xlogreader if any.  The xlogreader is owned elsewhere.
pub fn xlogreader_close_if_open() {
    // The walsender's `XLogReaderState *xlogreader` is owned by the xlogreader
    // subsystem; there is nothing for this crate to free in the owned model
    // until that vertical lands.  (No-op, matching the C `if (xlogreader ==
    // NULL) return;` early-out in the common error path.)
}
