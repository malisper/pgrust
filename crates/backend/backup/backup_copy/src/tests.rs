//! Tests for the COPY-stream base-backup sink.
//!
//! The tests drive the real sink callbacks with fixture implementations of the
//! outward seams: the libpq transport (`pq_putmessage` / `pq_flush_if_writable`
//! capture), the byte-stream encoding-conversion seam used by `pq_send*` /
//! `pq_puttextmessage` (a no-op passthrough), the wall clock, and the
//! `DestRemoteSimple` result-set path (`create_dest_remote_simple` /
//! `begin/do/end_tup_output` capture). The fixtures verify the exact in-band
//! `CopyData` framing and the result-set column layout / value selection.

use super::*;

use std::cell::{Cell, RefCell};
use std::sync::Once;
use std::thread_local;
use std::vec;
use std::vec::Vec;

use sink::{BbsinkState, TablespaceInfo};

/// Captured libpq events, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    /// A `pq_putmessage(msgtype, body)`.
    Msg(u8, Vec<u8>),
    /// A `pq_flush_if_writable()`.
    Flush,
    /// `CreateDestReceiver(DestRemoteSimple)`.
    NewDest,
    /// `begin_tup_output_tupdesc(dest, columns)`.
    Begin(Vec<ResultColumn>),
    /// `do_tup_output(tstate, values)`.
    Row(Vec<Option<ResultValue>>),
    /// `end_tup_output(tstate)`.
    End,
}

thread_local! {
    static EVENTS: RefCell<Vec<Event>> = const { RefCell::new(Vec::new()) };
    static NOW: Cell<TimestampTz> = const { Cell::new(0) };
}

fn install_fixtures() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        pqcomm_seams::pq_putmessage::set(|msgtype, body| {
            EVENTS.with(|e| e.borrow_mut().push(Event::Msg(msgtype, body.to_vec())));
            Ok(0)
        });
        pqcomm_seams::pq_flush_if_writable::set(|| {
            EVENTS.with(|e| e.borrow_mut().push(Event::Flush));
            0
        });
        // No-op passthrough conversion (pq_send* / pq_puttextmessage path).
        mbutils_seams::pg_server_to_client::set(|_mcx, _s| Ok(None));
        timestamp_seams::get_current_timestamp::set(|| NOW.with(|n| n.get()));
        timestamp_seams::timestamp_difference_milliseconds::set(
            |start, stop| (stop - start) / 1000,
        );
        backup_copy_seams::create_dest_remote_simple::set(|| {
            EVENTS.with(|e| e.borrow_mut().push(Event::NewDest));
            nodes::parsestmt::DestReceiverHandle(1)
        });
        backup_copy_seams::begin_tup_output_tupdesc::set(|dest, columns| {
            EVENTS.with(|e| e.borrow_mut().push(Event::Begin(columns)));
            backup_copy_seams::TupOutputState { dest }
        });
        backup_copy_seams::do_tup_output::set(|_tstate, values| {
            EVENTS.with(|e| e.borrow_mut().push(Event::Row(values)));
        });
        backup_copy_seams::end_tup_output::set(|_tstate| {
            EVENTS.with(|e| e.borrow_mut().push(Event::End));
        });
    });
}

fn drain() -> Vec<Event> {
    EVENTS.with(|e| core::mem::take(&mut *e.borrow_mut()))
}

fn setup() -> mcx::MemoryContext {
    install_fixtures();
    NOW.with(|n| n.set(0));
    drain();
    mcx::MemoryContext::new("copy-test")
}

#[test]
fn format_lsn_matches_c() {
    // psprintf("%X/%X", LSN_FORMAT_ARGS): high/low 32 bits, uppercase, no pad.
    assert_eq!(format_lsn(0), "0/0");
    assert_eq!(format_lsn(0x0000_0001_ABCD_EF12), "1/ABCDEF12");
    assert_eq!(format_lsn(0xDEAD_BEEF_0000_0010), "DEADBEEF/10");
}

#[test]
fn begin_backup_emits_two_resultsets_command_complete_and_copyout() {
    let ctx = setup();
    let mcx = ctx.mcx();

    let mut sink = bbsink_copystream_new(mcx, true);
    let mut state = BbsinkState {
        tablespaces: vec![
            TablespaceInfo { oid: 1663, path: Some("/data".into()), rpath: None, size: Some(4096) },
            TablespaceInfo { oid: 0, path: None, rpath: None, size: Some(-1) },
        ],
        tablespace_num: 0,
        bytes_done: 0,
        bytes_total: 0,
        bytes_total_is_valid: false,
        startptr: 0x0000_0001_0000_00A0,
        starttli: 7,
    };

    // bbsink_begin_backup sets buffer_length then invokes begin_backup.
    sink::bbsink_begin_backup(&mut sink, &mut state, 8192).unwrap();

    let ev = drain();
    // First result set: SendXlogRecPtrResult (recptr text, tli int8).
    assert_eq!(ev[0], Event::NewDest);
    assert_eq!(
        ev[1],
        Event::Begin(vec![
            ResultColumn { name: "recptr".into(), typ: ResultColumnType::Text },
            ResultColumn { name: "tli".into(), typ: ResultColumnType::Int8 },
        ])
    );
    assert_eq!(
        ev[2],
        Event::Row(vec![
            Some(ResultValue::Text("1/A0".into())),
            Some(ResultValue::Int8(7)),
        ])
    );
    assert_eq!(ev[3], Event::End);
    // SendXlogRecPtrResult ends with its own CommandComplete "SELECT".
    assert_eq!(ev[4], Event::Msg(PQ_MSG_COMMAND_COMPLETE, b"SELECT\0".to_vec()));

    // Second result set: SendTablespaceList (spcoid oid, spclocation text, size int8).
    assert_eq!(ev[5], Event::NewDest);
    assert_eq!(
        ev[6],
        Event::Begin(vec![
            ResultColumn { name: "spcoid".into(), typ: ResultColumnType::Oid },
            ResultColumn { name: "spclocation".into(), typ: ResultColumnType::Text },
            ResultColumn { name: "size".into(), typ: ResultColumnType::Int8 },
        ])
    );
    // Row 1: oid 1663, "/data", 4096/1024 = 4.
    assert_eq!(
        ev[7],
        Event::Row(vec![
            Some(ResultValue::Oid(1663)),
            Some(ResultValue::Text("/data".into())),
            Some(ResultValue::Int8(4)),
        ])
    );
    // Row 2: NULL path -> spcoid/spclocation NULL; size -1 -> NULL.
    assert_eq!(ev[8], Event::Row(vec![None, None, None]));
    assert_eq!(ev[9], Event::End);

    // begin_backup's own CommandComplete "SELECT" then CopyOutResponse.
    assert_eq!(ev[10], Event::Msg(PQ_MSG_COMMAND_COMPLETE, b"SELECT\0".to_vec()));
    // CopyOutResponse body: format byte 0, natts int16 0.
    assert_eq!(ev[11], Event::Msg(PQ_MSG_COPY_OUT_RESPONSE, vec![0, 0, 0]));
    assert_eq!(ev.len(), 12);
}

#[test]
fn begin_archive_frames_new_archive_with_path() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let mut sink = bbsink_copystream_new(mcx, true);
    let mut state = BbsinkState {
        tablespaces: vec![TablespaceInfo {
            oid: 0,
            path: Some("/some/where".into()),
            rpath: None,
            size: None,
        }],
        tablespace_num: 0,
        ..Default::default()
    };

    sink::bbsink_begin_archive(&mut sink, &mut state, "base.tar").unwrap();

    let ev = drain();
    // CopyData: 'n', "base.tar\0", "/some/where\0".
    let mut body = vec![b'n'];
    body.extend_from_slice(b"base.tar\0");
    body.extend_from_slice(b"/some/where\0");
    assert_eq!(ev, vec![Event::Msg(PQ_MSG_COPY_DATA, body)]);
}

#[test]
fn begin_archive_null_path_sends_empty_string() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let mut sink = bbsink_copystream_new(mcx, true);
    let mut state = BbsinkState {
        tablespaces: vec![TablespaceInfo { oid: 0, path: None, rpath: None, size: None }],
        tablespace_num: 0,
        ..Default::default()
    };

    sink::bbsink_begin_archive(&mut sink, &mut state, "x").unwrap();

    let ev = drain();
    let mut body = vec![b'n'];
    body.extend_from_slice(b"x\0");
    body.extend_from_slice(b"\0"); // empty path
    assert_eq!(ev, vec![Event::Msg(PQ_MSG_COPY_DATA, body)]);
}

#[test]
fn archive_contents_ships_data_with_leading_type_byte() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let mut sink = bbsink_copystream_new(mcx, true);
    let mut state = BbsinkState { startptr: 0, starttli: 1, ..Default::default() };

    // Allocate the buffer (begin_backup), then drain its setup events.
    sink::bbsink_begin_backup(&mut sink, &mut state, 8192).unwrap();
    drain();

    // Fill some bytes into the buffer and ship 4 of them.
    sink.buffer_mut().unwrap()[..4].copy_from_slice(b"ABCD");
    state.bytes_done = 4;
    sink::bbsink_archive_contents(&mut sink, &mut state, 4).unwrap();

    let ev = drain();
    // CopyData body = in-band 'd' + the 4 data bytes. No progress report yet
    // (bytes_done < PROGRESS_REPORT_BYTE_INTERVAL).
    assert_eq!(ev, vec![Event::Msg(PQ_MSG_COPY_DATA, b"dABCD".to_vec())]);
}

#[test]
fn archive_contents_not_to_client_sends_nothing_but_may_report() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let mut sink = bbsink_copystream_new(mcx, false);
    let mut state = BbsinkState { startptr: 0, starttli: 1, ..Default::default() };
    sink::bbsink_begin_backup(&mut sink, &mut state, 8192).unwrap();
    drain();

    // Cross the byte interval and advance the clock past the threshold so a
    // progress report fires even though no data is sent to the client.
    state.bytes_done = PROGRESS_REPORT_BYTE_INTERVAL + 10;
    NOW.with(|n| n.set(2_000_000)); // 2s in microseconds -> ms diff 2000 >= 1000
    sink::bbsink_archive_contents(&mut sink, &mut state, 1).unwrap();

    let ev = drain();
    // No 'd' data message; a 'p' progress CopyData then a flush.
    let mut prog = vec![b'p'];
    prog.extend_from_slice(&state.bytes_done.to_be_bytes());
    assert_eq!(ev, vec![Event::Msg(PQ_MSG_COPY_DATA, prog), Event::Flush]);
}

#[test]
fn archive_contents_no_report_when_time_threshold_not_met() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let mut sink = bbsink_copystream_new(mcx, false);
    let mut state = BbsinkState { startptr: 0, starttli: 1, ..Default::default() };
    sink::bbsink_begin_backup(&mut sink, &mut state, 8192).unwrap();
    drain();

    // Cross the byte interval but keep the clock under the threshold (and not
    // before the last report), so no progress report is sent.
    state.bytes_done = PROGRESS_REPORT_BYTE_INTERVAL + 10;
    NOW.with(|n| n.set(500_000)); // 0.5s -> 500ms < 1000
    sink::bbsink_archive_contents(&mut sink, &mut state, 1).unwrap();

    assert_eq!(drain(), vec![]);
}

#[test]
fn end_archive_forces_progress_report() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let mut sink = bbsink_copystream_new(mcx, true);
    let mut state = BbsinkState { bytes_done: 12345, ..Default::default() };

    sink::bbsink_end_archive(&mut sink, &mut state).unwrap();

    let ev = drain();
    let mut prog = vec![b'p'];
    prog.extend_from_slice(&12345u64.to_be_bytes());
    assert_eq!(ev, vec![Event::Msg(PQ_MSG_COPY_DATA, prog), Event::Flush]);
}

#[test]
fn begin_manifest_sends_m_marker() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let mut sink = bbsink_copystream_new(mcx, true);
    let mut state = BbsinkState::default();

    sink::bbsink_begin_manifest(&mut sink, &mut state).unwrap();

    assert_eq!(drain(), vec![Event::Msg(PQ_MSG_COPY_DATA, vec![b'm'])]);
}

#[test]
fn manifest_contents_ships_data_with_leading_type_byte() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let mut sink = bbsink_copystream_new(mcx, true);
    let mut state = BbsinkState { startptr: 0, starttli: 1, ..Default::default() };
    sink::bbsink_begin_backup(&mut sink, &mut state, 8192).unwrap();
    drain();

    sink.buffer_mut().unwrap()[..3].copy_from_slice(b"xyz");
    sink::bbsink_manifest_contents(&mut sink, &mut state, 3).unwrap();

    assert_eq!(drain(), vec![Event::Msg(PQ_MSG_COPY_DATA, b"dxyz".to_vec())]);
}

#[test]
fn end_manifest_does_nothing() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let mut sink = bbsink_copystream_new(mcx, true);
    let mut state = BbsinkState::default();

    sink::bbsink_end_manifest(&mut sink, &mut state).unwrap();

    assert_eq!(drain(), vec![]);
}

#[test]
fn end_backup_sends_copydone_then_xlogpos_result() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let mut sink = bbsink_copystream_new(mcx, true);
    // end_backup asserts tablespace_num == tablespaces.len(); both 0 here.
    let mut state = BbsinkState::default();

    sink::bbsink_end_backup(&mut sink, &mut state, 0x0000_0002_0000_0040, 9)
        .unwrap();

    let ev = drain();
    // CopyDone (empty body) first.
    assert_eq!(ev[0], Event::Msg(PQ_MSG_COPY_DONE, vec![]));
    // Then SendXlogRecPtrResult for endptr/endtli.
    assert_eq!(ev[1], Event::NewDest);
    assert_eq!(
        ev[2],
        Event::Begin(vec![
            ResultColumn { name: "recptr".into(), typ: ResultColumnType::Text },
            ResultColumn { name: "tli".into(), typ: ResultColumnType::Int8 },
        ])
    );
    assert_eq!(
        ev[3],
        Event::Row(vec![
            Some(ResultValue::Text("2/40".into())),
            Some(ResultValue::Int8(9)),
        ])
    );
    assert_eq!(ev[4], Event::End);
    assert_eq!(ev[5], Event::Msg(PQ_MSG_COMMAND_COMPLETE, b"SELECT\0".to_vec()));
    assert_eq!(ev.len(), 6);
}

#[test]
fn cleanup_does_nothing() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let mut sink = bbsink_copystream_new(mcx, true);
    let mut state = BbsinkState::default();

    sink::bbsink_cleanup(&mut sink, &mut state).unwrap();

    assert_eq!(drain(), vec![]);
}
