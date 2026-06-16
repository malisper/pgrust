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
pub fn IdentifySystem() {
    // SELECT system identifier, current TLI, and current/last-flushed LSN, and
    // (for a db-connected walsender) the database name; emit a single-row
    // result via the libpq tuple-description framing.
    panic!(
        "IdentifySystem: depends on unported libpq single-row result framing \
         (DestRemoteSimple) + GetSystemIdentifier/GetFlushRecPtr tuple assembly"
    );
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
fn cmd_variable_show(_n: crate::core::VariableShowStmt) {
    panic!(
        "SHOW: depends on unported GetPGVariable + transaction-wrapped libpq \
         result framing"
    );
}
