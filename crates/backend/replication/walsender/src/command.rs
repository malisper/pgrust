//! `exec_replication_command` and the replication-command handlers.
//!
//! 1:1 port of the command-dispatch section of `walsender.c`.  The
//! stopping-mode guard, the command-state transition, the aborted-transaction
//! guard, and the parse-result dispatch (the C `switch (cmd_node->type)`) are
//! ported here.  The replication scanner/parser is reached through its owner
//! seams; the per-command bodies (slot create/drop/alter, base backup, timeline
//! history, SHOW, manifest upload, IDENTIFY_SYSTEM, READ_REPLICATION_SLOT) are
//! deep cross-subsystem logic and are reached through their owner seams.

#![allow(non_snake_case)]

use alloc::string::ToString;

use crate::core::{proc_get, ReplCommand, ReplicationKind, WalSndState};
use crate::{basebackup, dest, snapbuild, tcop};

/// `bool exec_replication_command(const char *cmd_string)` — parse and execute a
/// replication command; returns false if it was not a WalSender command.
pub fn exec_replication_command(cmd_string: &str) -> bool {
    // If WAL sender was told that shutdown is getting close, switch its status so
    // the next replication commands are handled correctly.
    if proc_get(|p| p.got_STOPPING) != 0 {
        crate::init::WalSndSetState(WalSndState::WALSNDSTATE_STOPPING);
    }

    // Throw if in stopping mode: prevent commands that could generate WAL while
    // the shutdown checkpoint is being written.  Prohibit all new commands.
    if crate::shmem_array::my_state() == WalSndState::WALSNDSTATE_STOPPING {
        error_stopping_mode();
    }

    // CREATE_REPLICATION_SLOT ... LOGICAL exports a snapshot until the next
    // command; clean up the old stuff if there's anything.
    snapbuild::snap_build_clear_exported_snapshot::call()
        .expect("SnapBuildClearExportedSnapshot");

    tcop::check_for_interrupts::call().expect("CHECK_FOR_INTERRUPTS");

    // Parse the command with the real replication scanner/parser.  `None` is the
    // C "not a WalSender command" path (the SQL path takes over): return false.
    let cmd = match parse_replication_command(cmd_string) {
        Some(cmd) => cmd,
        None => return false,
    };

    // Report query to monitoring, log it per log_replication_commands (DEBUG1
    // otherwise), and allocate the per-command output/reply/tmp buffers.
    begin_replication_command(cmd_string);

    // Disallow replication commands in aborted transaction blocks.
    if crate::xact::is_aborted_transaction_block_state::call() {
        error_aborted_transaction();
    }

    tcop::check_for_interrupts::call().expect("CHECK_FOR_INTERRUPTS");

    // switch (cmd_node->type)
    match cmd {
        ReplCommand::IdentifySystem => {
            let cmdtag = "IDENTIFY_SYSTEM";
            set_ps_display(cmdtag);
            IdentifySystem();
            end_replication_command(cmdtag);
        }
        ReplCommand::ReadReplicationSlot(c) => {
            let cmdtag = "READ_REPLICATION_SLOT";
            set_ps_display(cmdtag);
            ReadReplicationSlot(c).expect("ReadReplicationSlot");
            end_replication_command(cmdtag);
        }
        ReplCommand::BaseBackup(c) => {
            let cmdtag = "BASE_BACKUP";
            set_ps_display(cmdtag);
            crate::xact::prevent_in_transaction_block::call(true, cmdtag)
                .expect("PreventInTransactionBlock(BASE_BACKUP)");
            basebackup::send_base_backup::call(c).expect("SendBaseBackup");
            end_replication_command(cmdtag);
        }
        ReplCommand::CreateReplicationSlot(c) => {
            let cmdtag = "CREATE_REPLICATION_SLOT";
            set_ps_display(cmdtag);
            CreateReplicationSlot(c);
            end_replication_command(cmdtag);
        }
        ReplCommand::DropReplicationSlot(c) => {
            let cmdtag = "DROP_REPLICATION_SLOT";
            set_ps_display(cmdtag);
            DropReplicationSlot(c);
            end_replication_command(cmdtag);
        }
        ReplCommand::AlterReplicationSlot(c) => {
            let cmdtag = "ALTER_REPLICATION_SLOT";
            set_ps_display(cmdtag);
            AlterReplicationSlot(c);
            end_replication_command(cmdtag);
        }
        ReplCommand::StartReplication(c) => {
            let cmdtag = "START_REPLICATION";
            set_ps_display(cmdtag);
            crate::xact::prevent_in_transaction_block::call(true, cmdtag)
                .expect("PreventInTransactionBlock(START_REPLICATION)");

            if c.kind == ReplicationKind::REPLICATION_KIND_PHYSICAL {
                crate::start_replication::StartReplication(&c);
            } else {
                crate::start_replication::StartLogicalReplication(&c);
            }

            // Dupe, but necessary per libpqrcv_endstreaming.
            end_replication_command(cmdtag);
        }
        ReplCommand::TimeLineHistory(c) => {
            let cmdtag = "TIMELINE_HISTORY";
            set_ps_display(cmdtag);
            crate::xact::prevent_in_transaction_block::call(true, cmdtag)
                .expect("PreventInTransactionBlock(TIMELINE_HISTORY)");
            SendTimeLineHistory(c).expect("SendTimeLineHistory");
            end_replication_command(cmdtag);
        }
        ReplCommand::VariableShow(n) => {
            let cmdtag = "SHOW";
            set_ps_display(cmdtag);
            cmd_variable_show(n);
            end_replication_command(cmdtag);
        }
        ReplCommand::UploadManifest => {
            let cmdtag = "UPLOAD_MANIFEST";
            set_ps_display(cmdtag);
            crate::xact::prevent_in_transaction_block::call(true, cmdtag)
                .expect("PreventInTransactionBlock(UPLOAD_MANIFEST)");
            UploadManifest();
            end_replication_command(cmdtag);
        }
    }

    true
}

// ---------------------------------------------------------------------------
// exec_replication_command sub-steps.
// ---------------------------------------------------------------------------

/// The parse leg of `exec_replication_command`: scan + parse `cmd_string` with
/// the real ported replication scanner/parser.  `None` = the C `return false`
/// path (not a walsender command — the SQL path takes over), after the
/// physical-replication-SQL restriction check.
fn parse_replication_command(cmd_string: &str) -> Option<ReplCommand> {
    let is_repl = repl_gram::is_replication_command(cmd_string)
        .expect("replication_scanner_is_replication_command");
    if !is_repl {
        // C: ereport(ERROR, "cannot execute SQL commands in WAL sender for
        // physical replication") when am_walsender && !am_db_walsender.
        let physical_only = proc_get(|p| p.am_walsender && !p.am_db_walsender);
        if physical_only {
            panic!(
                "cannot execute SQL commands in WAL sender for physical replication \
                 (ERRCODE_FEATURE_NOT_SUPPORTED)"
            );
        }
        return None;
    }

    // Parse via the real ported replication grammar (direct dependency).
    Some(repl_gram::replication_parse(cmd_string).expect("replication_parse"))
}

/// `set_ps_display(cmdtag)`.
fn set_ps_display(cmdtag: &str) {
    crate::ps_status::set_ps_display::call(cmdtag.to_string());
}

/// The per-command "begin": `pgstat_report_activity` + the command log +
/// per-command StringInfo buffers.  The libpq buffer init lives in the libpq
/// owner; report + log are reached through the tcop/pgstat owners.
fn begin_replication_command(cmd_string: &str) {
    // `ereport(log_replication_commands ? LOG : DEBUG1, errmsg("received
    // replication command: %s", cmd_string))`.
    let level = if proc_get(|p| p.log_replication_commands) {
        types_error::LOG
    } else {
        types_error::DEBUG1
    };
    utils_error::ereport(level)
        .errmsg(alloc::format!("received replication command: {cmd_string}"))
        .finish(types_error::ErrorLocation::new(
            "walsender.c",
            0,
            "exec_replication_command",
        ))
        .ok();

    // The per-command output/reply/tmp StringInfo buffers are reset here; the
    // `output_message` buffer is owned by this crate.
    crate::core::with_output_message(|b| b.clear());
}

/// `EndReplicationCommand(cmdtag)` + `debug_query_string = NULL`.
fn end_replication_command(cmdtag: &str) {
    dest::end_replication_command::call(cmdtag.to_string())
        .expect("EndReplicationCommand");
    tcop::reset_debug_query_string::call();
}

/// `ereport(ERROR, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, "cannot execute new
/// commands while WAL sender is in stopping mode")`.
fn error_stopping_mode() -> ! {
    panic!("cannot execute new commands while WAL sender is in stopping mode");
}

/// `ereport(ERROR, ERRCODE_ACTIVE_SQL_TRANSACTION, "current transaction is
/// aborted, commands ignored until end of transaction block")`.
fn error_aborted_transaction() -> ! {
    panic!(
        "current transaction is aborted, commands ignored until end of transaction block"
    );
}

// ---------------------------------------------------------------------------
// Per-command handlers.  These bodies are deep cross-subsystem logic
// (syscache / libpq tuple framing / slots / basebackup / decoding); each is
// reached through its owner seam, panicking until that owner lands.
// ---------------------------------------------------------------------------

/// `static void IdentifySystem(void)`.
///
/// 1:1 port of walsender.c's `IdentifySystem`: reply with a one-row, four-column
/// result set — system identifier (text), timeline (int8), current xlog
/// position (text), and the database name (text, NULL for a physical
/// walsender). The tuple is sent through the `DestRemoteSimple` receiver via the
/// `begin/do/end_tup_output` owner seams.
pub fn IdentifySystem() {
    use crate::core::{INT8OID, TEXTOID};

    // The repo has no ambient memory context for the walsender command path, so
    // own one for the duration of this command (as SendBaseBackup does).
    let ctx = mcx::MemoryContext::new("IDENTIFY_SYSTEM");
    let mcx = ctx.mcx();

    // snprintf(sysid, sizeof(sysid), UINT64_FORMAT, GetSystemIdentifier());
    let sysid = alloc::format!("{}", crate::xlog::get_system_identifier::call());

    // am_cascading_walsender = RecoveryInProgress();
    let am_cascading = crate::xlog::recovery_in_progress::call();
    crate::core::with_proc(|p| p.am_cascading_walsender = am_cascading);

    // if (am_cascading_walsender) logptr = GetStandbyFlushRecPtr(&currTLI);
    // else                        logptr = GetFlushRecPtr(&currTLI);
    let mut curr_tli: crate::core::TimeLineID = 0;
    let logptr = if am_cascading {
        crate::start_replication::GetStandbyFlushRecPtr(&mut curr_tli)
    } else {
        let (ptr, tli) = crate::xlog::get_flush_rec_ptr::call();
        curr_tli = tli;
        ptr
    };

    // snprintf(xloc, sizeof(xloc), "%X/%X", LSN_FORMAT_ARGS(logptr));
    let xloc = alloc::format!("{:X}/{:X}", (logptr >> 32) as u32, logptr as u32);

    // if (MyDatabaseId != InvalidOid) { StartTransactionCommand();
    //   dbname = get_database_name(MyDatabaseId); ... CommitTransactionCommand(); }
    let dbname: Option<alloc::string::String> = {
        let dbid = crate::miscinit::my_database_id::call();
        if dbid != crate::core::InvalidOid {
            // syscache access needs a transaction env.
            crate::xact::start_transaction_command::call()
                .expect("StartTransactionCommand(IDENTIFY_SYSTEM)");
            let name = dbcommands_seams::get_database_name::call(mcx, dbid)
                .expect("get_database_name")
                .map(|s| s.as_str().to_string());
            crate::xact::commit_transaction_command::call()
                .expect("CommitTransactionCommand(IDENTIFY_SYSTEM)");
            name
        } else {
            None
        }
    };

    // dest = CreateDestReceiver(DestRemoteSimple);
    let dest = dest::create_dest_receiver::call(types_dest::CommandDest::RemoteSimple);

    // need a tuple descriptor representing four columns
    // tupdesc = CreateTemplateTupleDesc(4);
    let mut tupdesc = tupdesc::CreateTemplateTupleDesc(mcx, 4)
        .expect("CreateTemplateTupleDesc(4)");
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 1, "systemid", TEXTOID, -1, 0)
        .expect("TupleDescInitBuiltinEntry(systemid)");
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 2, "timeline", INT8OID, -1, 0)
        .expect("TupleDescInitBuiltinEntry(timeline)");
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 3, "xlogpos", TEXTOID, -1, 0)
        .expect("TupleDescInitBuiltinEntry(xlogpos)");
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 4, "dbname", TEXTOID, -1, 0)
        .expect("TupleDescInitBuiltinEntry(dbname)");
    let tupdesc = Some(mcx::alloc_in(mcx, tupdesc).expect("alloc tupdesc"));

    // prepare for projection of tuples
    // tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);
    let mut tstate = execTuples_seams::begin_tup_output_tupdesc::call(
        mcx,
        dest,
        tupdesc,
        nodes::TupleSlotKind::Virtual,
    )
    .expect("begin_tup_output_tupdesc");

    // column 1: system identifier (text)
    // column 2: timeline (int8)
    // column 3: wal location (text)
    // column 4: database name, or NULL if none (text)
    let v0 = varlena_seams::cstring_to_text_v::call(mcx, &sysid)
        .expect("cstring_to_text(systemid)");
    let v1 = types_tuple::Datum::from_i64(curr_tli as i64);
    let v2 = varlena_seams::cstring_to_text_v::call(mcx, &xloc)
        .expect("cstring_to_text(xlogpos)");
    let (v3, null3) = match &dbname {
        Some(name) => (
            varlena_seams::cstring_to_text_v::call(mcx, name)
                .expect("cstring_to_text(dbname)"),
            false,
        ),
        None => (types_tuple::Datum::null(), true),
    };

    let values = [v0, v1, v2, v3];
    let nulls = [false, false, false, null3];

    // do_tup_output(tstate, values, nulls);
    execTuples_seams::do_tup_output::call(mcx, &mut tstate, &values, &nulls)
        .expect("do_tup_output");

    // end_tup_output(tstate);
    execTuples_seams::end_tup_output::call(mcx, tstate).expect("end_tup_output");
}

/// `static void ReadReplicationSlot(ReadReplicationSlotCmd *cmd)`.
///
/// 1:1 port of walsender.c's `ReadReplicationSlot`: reply with a one-row,
/// three-column result set describing the named *physical* replication slot —
/// slot type (text, always "physical"), restart LSN (text), and the timeline
/// that LSN was produced on (int8). When the slot does not exist (or is not
/// in_use) every column is NULL. A logical slot is rejected. The slot contents
/// are snapshotted under the per-slot spinlock while `ReplicationSlotControlLock`
/// is held shared, exactly as in C.
pub fn ReadReplicationSlot(cmd: crate::core::ReadReplicationSlotCmd) -> types_error::PgResult<()> {
    use crate::core::{INT8OID, InvalidOid, InvalidXLogRecPtr, TEXTOID};
    use ::types_storage::storage::REPLICATION_SLOT_CONTROL_LOCK;
    use ::types_storage::LWLockMode;

    const READ_REPLICATION_SLOT_COLS: usize = 3;

    // The repo has no ambient memory context for the walsender command path, so
    // own one for the duration of this command (as IdentifySystem does).
    let ctx = mcx::MemoryContext::new("READ_REPLICATION_SLOT");
    let mcx = ctx.mcx();

    // tupdesc = CreateTemplateTupleDesc(READ_REPLICATION_SLOT_COLS);
    let mut tupdesc = tupdesc::CreateTemplateTupleDesc(mcx, READ_REPLICATION_SLOT_COLS as i32)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 1, "slot_type", TEXTOID, -1, 0)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 2, "restart_lsn", TEXTOID, -1, 0)?;
    // TimeLineID is unsigned, so int4 is not wide enough.
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 3, "restart_tli", INT8OID, -1, 0)?;
    let tupdesc = Some(mcx::alloc_in(mcx, tupdesc)?);

    // Datum values[READ_REPLICATION_SLOT_COLS] = {0}; memset(nulls, true, ...);
    let mut values: [types_tuple::Datum; READ_REPLICATION_SLOT_COLS] =
        core::array::from_fn(|_| types_tuple::Datum::null());
    let mut nulls = [true; READ_REPLICATION_SLOT_COLS];

    // LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    let control_lock = lwlock_seams::lwlock_acquire_main::call(
        REPLICATION_SLOT_CONTROL_LOCK,
        LWLockMode::LW_SHARED,
    )?;

    // slot = SearchNamedReplicationSlot(cmd->slotname, false);
    let slotname = cmd.slotname.as_deref().unwrap_or("");
    let slot = crate::slot::search_named_replication_slot::call(slotname, false)?;

    if slot.is_none() || !crate::slot::slot_in_use::call(slot) {
        // LWLockRelease(ReplicationSlotControlLock);
        control_lock.release()?;
    } else {
        // Copy slot contents while holding spinlock.
        // SpinLockAcquire(&slot->mutex); slot_contents = *slot; SpinLockRelease(...)
        crate::slot::slot_spin_acquire::call(slot);
        let slot_database = crate::slot::slot_data_database::call(slot);
        let slot_restart_lsn = crate::slot::slot_data_restart_lsn::call(slot);
        crate::slot::slot_spin_release::call(slot);

        // LWLockRelease(ReplicationSlotControlLock);
        control_lock.release()?;

        // if (OidIsValid(slot_contents.data.database)) ereport(ERROR, ...)
        if slot_database != InvalidOid {
            return utils_error::ereport(types_error::ERROR)
                .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot use READ_REPLICATION_SLOT with a logical replication slot".to_string())
                .finish(types_error::ErrorLocation::new("walsender.c", 0, "ReadReplicationSlot"));
        }

        // slot type
        values[0] = varlena_seams::cstring_to_text_v::call(mcx, "physical")?;
        nulls[0] = false;

        // start LSN
        if slot_restart_lsn != InvalidXLogRecPtr {
            let xloc = alloc::format!(
                "{:X}/{:X}",
                (slot_restart_lsn >> 32) as u32,
                slot_restart_lsn as u32
            );
            values[1] = varlena_seams::cstring_to_text_v::call(mcx, &xloc)?;
            nulls[1] = false;
        }

        // timeline this WAL was produced on
        if slot_restart_lsn != InvalidXLogRecPtr {
            // While in recovery, use as timeline the currently-replaying one to
            // get the LSN position's history.
            let current_timeline = if crate::xlog::recovery_in_progress::call() {
                let (_lsn, tli) = crate::xlogrecovery::get_xlog_replay_rec_ptr_tli::call();
                tli
            } else {
                // GetWALInsertionTimeLine(): on a primary (recovery Done) the
                // insert TLI is always set, so the IfSet form returns the same
                // value GetWALInsertionTimeLine() would (this branch is taken
                // only when !RecoveryInProgress()).
                crate::xlog::get_wal_insertion_timeline_if_set::call()
            };

            let timeline_history = crate::timeline::read_timeline_history::call(mcx, current_timeline)?;
            let slots_position_timeline =
                crate::timeline::tli_of_point_in_history::call(slot_restart_lsn, &timeline_history)?;
            values[2] = types_tuple::Datum::from_i64(slots_position_timeline as i64);
            nulls[2] = false;
        }
    }

    // dest = CreateDestReceiver(DestRemoteSimple);
    let dest = dest::create_dest_receiver::call(types_dest::CommandDest::RemoteSimple);

    // tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);
    let mut tstate = execTuples_seams::begin_tup_output_tupdesc::call(
        mcx,
        dest,
        tupdesc,
        nodes::TupleSlotKind::Virtual,
    )?;

    // do_tup_output(tstate, values, nulls);
    execTuples_seams::do_tup_output::call(mcx, &mut tstate, &values, &nulls)?;

    // end_tup_output(tstate);
    execTuples_seams::end_tup_output::call(mcx, tstate)?;
    Ok(())
}

/// `static void SendTimeLineHistory(TimeLineHistoryCmd *cmd)`.
///
/// 1:1 port of walsender.c's `SendTimeLineHistory`: reply with a one-row,
/// two-column result set — the timeline-history file name (text) and its raw
/// contents (text). The RowDescription is sent through the `DestRemoteSimple`
/// receiver's `rStartup`; the single DataRow is framed by hand (column count +
/// per-column length + bytes, big-endian, exactly as `pq_sendint16`/
/// `pq_sendint32`/`pq_sendbytes` do) and sent via `pq_putmessage(PqMsg_DataRow)`.
pub fn SendTimeLineHistory(cmd: crate::core::TimeLineHistoryCmd) -> types_error::PgResult<()> {
    use crate::core::TEXTOID;

    // O_RDONLY | PG_BINARY (PG_BINARY == 0 on non-Windows).
    const O_RDONLY: i32 = 0;
    const PG_BINARY: i32 = 0;
    // <unistd.h> lseek whence constants.
    const SEEK_SET: i32 = 0;
    const SEEK_END: i32 = 2;
    // PqMsg_DataRow ('D'); the per-message-length is added by pq_putmessage.
    const PQMSG_DATA_ROW: u8 = b'D';
    // PGAlignedBlock is BLCKSZ (8192) bytes; mirror the C read chunk size.
    const BLCKSZ: usize = 8192;
    // WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ (utils/wait_event_types.h):
    // PG_WAIT_IO (0x0A000000) | 67, index 67 of the WaitEventIO section of
    // wait_event_names.txt.
    const WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ: u32 = 0x0A000000 | 67;

    // The repo has no ambient memory context for the walsender command path, so
    // own one for the duration of this command (as IdentifySystem does).
    let ctx = mcx::MemoryContext::new("TIMELINE_HISTORY");
    let mcx = ctx.mcx();

    // dest = CreateDestReceiver(DestRemoteSimple);
    let dest = dest::create_dest_receiver::call(types_dest::CommandDest::RemoteSimple);

    // Reply with a result set with one row, and two columns. The first col is
    // the name of the history file, 2nd is the contents.
    // tupdesc = CreateTemplateTupleDesc(2);
    let mut tupdesc = tupdesc::CreateTemplateTupleDesc(mcx, 2)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 1, "filename", TEXTOID, -1, 0)?;
    tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 2, "content", TEXTOID, -1, 0)?;
    let tupdesc = mcx::alloc_in(mcx, tupdesc)?;

    // TLHistoryFileName(histfname, cmd->timeline);
    // TLHistoryFilePath(path, cmd->timeline);
    let histfname = crate::timeline::tl_history_file_name::call(cmd.timeline);
    let path = crate::timeline::tl_history_file_path::call(cmd.timeline);

    // Send a RowDescription message: dest->rStartup(dest, CMD_SELECT, tupdesc);
    dest::dest_rstartup::call(mcx, dest, nodes::nodes::CmdType::CMD_SELECT, &*tupdesc)?;

    // Send a DataRow message: build the body by hand.
    // pq_beginmessage(&buf, PqMsg_DataRow); pq_sendint16(&buf, 2);
    let mut buf: alloc::vec::Vec<u8> = alloc::vec::Vec::new();
    buf.extend_from_slice(&2i16.to_be_bytes()); // # of columns

    // col1: the history file name.
    let fname_bytes = histfname.as_bytes();
    buf.extend_from_slice(&(fname_bytes.len() as i32).to_be_bytes()); // col1 len
    buf.extend_from_slice(fname_bytes);

    // fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    let fd = fd_seams::open_transient_file::call(&path, O_RDONLY | PG_BINARY);
    if fd < 0 {
        return utils_error::ereport(types_error::ERROR)
            .errcode_for_file_access()
            .errmsg(alloc::format!("could not open file \"{path}\""))
            .finish(types_error::ErrorLocation::new("walsender.c", 0, "SendTimeLineHistory"));
    }

    // Determine file length and send it to client.
    // histfilelen = lseek(fd, 0, SEEK_END);
    let histfilelen = fd_seams::transient_lseek::call(fd, 0, SEEK_END);
    if histfilelen < 0 {
        return utils_error::ereport(types_error::ERROR)
            .errcode_for_file_access()
            .errmsg(alloc::format!("could not seek to end of file \"{path}\""))
            .finish(types_error::ErrorLocation::new("walsender.c", 0, "SendTimeLineHistory"));
    }
    // if (lseek(fd, 0, SEEK_SET) != 0) ereport(ERROR, ...)
    if fd_seams::transient_lseek::call(fd, 0, SEEK_SET) != 0 {
        return utils_error::ereport(types_error::ERROR)
            .errcode_for_file_access()
            .errmsg(alloc::format!("could not seek to beginning of file \"{path}\""))
            .finish(types_error::ErrorLocation::new("walsender.c", 0, "SendTimeLineHistory"));
    }

    // pq_sendint32(&buf, histfilelen); /* col2 len */
    buf.extend_from_slice(&(histfilelen as i32).to_be_bytes());

    // bytesleft = histfilelen;
    let mut bytesleft = histfilelen;
    while bytesleft > 0 {
        let mut rbuf = [0u8; BLCKSZ];

        // pgstat_report_wait_start(WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ);
        waitevent_seams::pgstat_report_wait_start::call(
            WAIT_EVENT_WALSENDER_TIMELINE_HISTORY_READ,
        );
        // nread = read(fd, rbuf.data, sizeof(rbuf));
        let nread = fd_seams::transient_read::call(fd, &mut rbuf);
        waitevent_seams::pgstat_report_wait_end::call();

        if nread < 0 {
            return utils_error::ereport(types_error::ERROR)
                .errcode_for_file_access()
                .errmsg(alloc::format!("could not read file \"{path}\""))
                .finish(types_error::ErrorLocation::new("walsender.c", 0, "SendTimeLineHistory"));
        } else if nread == 0 {
            return utils_error::ereport(types_error::ERROR)
                .errcode(types_error::ERRCODE_DATA_CORRUPTED)
                .errmsg(alloc::format!(
                    "could not read file \"{path}\": read {nread} of {bytesleft}"
                ))
                .finish(types_error::ErrorLocation::new("walsender.c", 0, "SendTimeLineHistory"));
        }

        // pq_sendbytes(&buf, rbuf.data, nread);
        buf.extend_from_slice(&rbuf[..nread as usize]);
        bytesleft -= nread as i64;
    }

    // if (CloseTransientFile(fd) != 0) ereport(ERROR, ...)
    if fd_seams::close_transient_file::call(fd) != 0 {
        return utils_error::ereport(types_error::ERROR)
            .errcode_for_file_access()
            .errmsg(alloc::format!("could not close file \"{path}\""))
            .finish(types_error::ErrorLocation::new("walsender.c", 0, "SendTimeLineHistory"));
    }

    // pq_endmessage(&buf); — frame and send the assembled DataRow.
    crate::pq::pq_putmessage::call(PQMSG_DATA_ROW, &buf)?;
    Ok(())
}

/// `static void UploadManifest(void)`.
pub fn UploadManifest() {
    basebackup::upload_manifest::call().expect("UploadManifest");
}

/// `static void CreateReplicationSlot(CreateReplicationSlotCmd *cmd)`.
pub fn CreateReplicationSlot(_cmd: crate::core::CreateReplicationSlotCmd) {
    panic!(
        "CreateReplicationSlot: depends on unported slot creation + logical \
         decoding context setup + libpq result framing"
    );
}

/// `static void DropReplicationSlot(DropReplicationSlotCmd *cmd)`.
pub fn DropReplicationSlot(cmd: crate::core::DropReplicationSlotCmd) {
    // ReplicationSlotDrop(cmd->slotname, !cmd->wait); EndReplicationCommand.
    let name = cmd.slotname.as_deref().unwrap_or("");
    crate::slot::replication_slot_drop::call(name, !cmd.wait).expect("ReplicationSlotDrop");
}

/// `static void AlterReplicationSlot(AlterReplicationSlotCmd *cmd)`.
pub fn AlterReplicationSlot(_cmd: crate::core::AlterReplicationSlotCmd) {
    panic!(
        "AlterReplicationSlot: depends on unported failover-flag slot alter \
         (ReplicationSlotAlter)"
    );
}

/// `GetPGVariable(name)` wrapped in Start/CommitTransactionCommand (SHOW).
///
/// 1:1 port of the `case T_VariableShowStmt:` arm of `exec_replication_command`
/// (walsender.c): create a `DestRemoteSimple` receiver, run the SHOW inside a
/// transaction command (syscache access needs a transaction environment), and
/// emit the single-row result through the `GetPGVariable` owner seam.
fn cmd_variable_show(n: crate::core::VariableShowStmt) {
    // dest = CreateDestReceiver(DestRemoteSimple);
    let dest = dest::create_dest_receiver::call(types_dest::CommandDest::RemoteSimple);

    // The repo has no ambient memory context for the walsender command path, so
    // own one for the duration of this command (mirroring SendBaseBackup's
    // inward seam entry, which owns a `MemoryContext` for its run).
    let ctx = mcx::MemoryContext::new("SHOW");

    // syscache access needs a transaction environment
    // StartTransactionCommand();
    crate::xact::start_transaction_command::call().expect("StartTransactionCommand(SHOW)");

    // GetPGVariable(n->name, dest);
    utility_out_seams::get_pg_variable::call(ctx.mcx(), Some(&n.name), dest)
        .expect("GetPGVariable");

    // CommitTransactionCommand();
    crate::xact::commit_transaction_command::call().expect("CommitTransactionCommand(SHOW)");
}
