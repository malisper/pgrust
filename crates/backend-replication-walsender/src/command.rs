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
            ReadReplicationSlot(c);
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
            SendTimeLineHistory(c);
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
    let is_repl = backend_replication_repl_gram::is_replication_command(cmd_string)
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
    Some(backend_replication_repl_gram::replication_parse(cmd_string).expect("replication_parse"))
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
    backend_utils_error::ereport(level)
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
            let name = backend_commands_dbcommands_seams::get_database_name::call(mcx, dbid)
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
    let mut tupdesc = backend_access_common_tupdesc::CreateTemplateTupleDesc(mcx, 4)
        .expect("CreateTemplateTupleDesc(4)");
    backend_access_common_tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 1, "systemid", TEXTOID, -1, 0)
        .expect("TupleDescInitBuiltinEntry(systemid)");
    backend_access_common_tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 2, "timeline", INT8OID, -1, 0)
        .expect("TupleDescInitBuiltinEntry(timeline)");
    backend_access_common_tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 3, "xlogpos", TEXTOID, -1, 0)
        .expect("TupleDescInitBuiltinEntry(xlogpos)");
    backend_access_common_tupdesc::TupleDescInitBuiltinEntry(&mut tupdesc, 4, "dbname", TEXTOID, -1, 0)
        .expect("TupleDescInitBuiltinEntry(dbname)");
    let tupdesc = Some(mcx::alloc_in(mcx, tupdesc).expect("alloc tupdesc"));

    // prepare for projection of tuples
    // tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);
    let mut tstate = backend_executor_execTuples_seams::begin_tup_output_tupdesc::call(
        mcx,
        dest,
        tupdesc,
        types_nodes::TupleSlotKind::Virtual,
    )
    .expect("begin_tup_output_tupdesc");

    // column 1: system identifier (text)
    // column 2: timeline (int8)
    // column 3: wal location (text)
    // column 4: database name, or NULL if none (text)
    let v0 = backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, &sysid)
        .expect("cstring_to_text(systemid)");
    let v1 = types_tuple::Datum::from_i64(curr_tli as i64);
    let v2 = backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, &xloc)
        .expect("cstring_to_text(xlogpos)");
    let (v3, null3) = match &dbname {
        Some(name) => (
            backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, name)
                .expect("cstring_to_text(dbname)"),
            false,
        ),
        None => (types_tuple::Datum::null(), true),
    };

    let values = [v0, v1, v2, v3];
    let nulls = [false, false, false, null3];

    // do_tup_output(tstate, values, nulls);
    backend_executor_execTuples_seams::do_tup_output::call(mcx, &mut tstate, &values, &nulls)
        .expect("do_tup_output");

    // end_tup_output(tstate);
    backend_executor_execTuples_seams::end_tup_output::call(mcx, tstate).expect("end_tup_output");
}

/// `static void ReadReplicationSlot(ReadReplicationSlotCmd *cmd)`.
pub fn ReadReplicationSlot(_cmd: crate::core::ReadReplicationSlotCmd) {
    panic!(
        "ReadReplicationSlot: depends on unported slot-by-name lookup + libpq \
         3-column result framing"
    );
}

/// `static void SendTimeLineHistory(TimeLineHistoryCmd *cmd)`.
pub fn SendTimeLineHistory(_cmd: crate::core::TimeLineHistoryCmd) {
    panic!(
        "SendTimeLineHistory: depends on unported timeline-history file read + \
         libpq CopyOut/result framing"
    );
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
    backend_tcop_utility_out_seams::get_pg_variable::call(ctx.mcx(), Some(&n.name), dest)
        .expect("GetPGVariable");

    // CommitTransactionCommand();
    crate::xact::commit_transaction_command::call().expect("CommitTransactionCommand(SHOW)");
}
