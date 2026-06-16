//! The utility-command **dispatch** — `ProcessUtility` /
//! `standard_ProcessUtility` / `ProcessUtilityForAlterTable` / `ExecDropStmt`
//! (utility.c:521-1090, 1593-1660), PostgreSQL 18.3.
//!
//! This is the giant `switch (nodeTag(parsetree))` that hands every utility
//! command to its owning handler. The dispatch's *control flow* is ported here
//! 1:1 over the owned [`types_nodes::nodes::Node`] tree:
//!
//!   * the `readOnlyTree` deep-copy ([`PlannedStmt::clone_in`] into the
//!     per-utility `mcx`), the recursion guard, and the read-only / parallel-
//!     mode / recovery prohibition (utility.c:556-602);
//!   * the full `nodeTag(parsetree)` switch (utility.c:614-1074): the
//!     transaction-control verbs (BEGIN/COMMIT/PREPARE/SAVEPOINT/…), the portal
//!     verbs (DECLARE/CLOSE/FETCH), DO, the tablespace/database/role globals
//!     (each fenced by `PreventInTransactionBlock`), COPY, PREPARE/EXECUTE/
//!     DEALLOCATE, NOTIFY/LISTEN/UNLISTEN (incl. the background-process reject),
//!     LOAD, CALL, CLUSTER, VACUUM, EXPLAIN, ALTER SYSTEM, SET/SHOW, DISCARD,
//!     event triggers, CHECKPOINT (with its `pg_checkpoint` privilege check),
//!     LOCK TABLE, SET CONSTRAINTS, and the GRANT/DROP/RENAME/ALTER…/COMMENT/
//!     SECURITY LABEL "fast path" arms that consult
//!     `EventTriggerSupportsObjectType` and otherwise fall through to the
//!     event-trigger-fenced [`process_utility_slow`] seam;
//!   * the trailing `free_parsestate` (the parsestate is `mcx`-owned and dropped
//!     with the context) + `CommandCounterIncrement`;
//!   * `ExecDropStmt`'s `removeType` switch (utility.c:1958-1990) and
//!     `ProcessUtilityForAlterTable`'s event-trigger fence (utility.c:1593-1610).
//!
//! Every *command body* the switch routes to lives in another subsystem and is
//! reached through a thin forwarding seam in
//! [`backend_tcop_utility_out_seams`] (xact-control, portal verbs, every
//! `commands` handler, the event-trigger machinery, the checkpointer, …). A seam
//! carries no dispatch logic — it forwards the already-classified parse tree plus
//! the runtime context the handler needs. CREATE TABLE → `DefineRelation` and
//! CREATE INDEX → `DefineIndex` (and every other event-trigger-supporting DDL)
//! are reached via the `_ =>` arm's [`process_utility_slow`] seam, exactly as in
//! C. Each seam defaults to a loud panic until its owning subsystem installs the
//! real handler at single-threaded startup — never a silent stub.
//!
//! The read-only / parallel / recovery / security predicates and the
//! tag-classifiers the dispatch consults (`ClassifyUtilityCommandAsReadOnly`,
//! `CreateCommandTag`, `PreventCommandIf*`, `CheckRestrictedOperation`) are
//! grounded in-crate (see [`crate::classify`]).

use backend_tcop_cmdtag::get_command_tag_name;
use backend_utils_error::ereport;
use mcx::Mcx;
use types_core::cmdtag::CommandTag;
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE, ERROR,
};
use types_core::init::BackendType;
use types_nodes::ddlnodes::TransactionStmtKind;
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::Node;
use types_nodes::parsenodes::{
    OBJECT_FOREIGN_TABLE, OBJECT_INDEX, OBJECT_MATVIEW, OBJECT_SEQUENCE, OBJECT_TABLE, OBJECT_VIEW,
};
use types_nodes::parsestmt::{
    DestReceiverHandle, ParseState, ProcessUtilityContext, PROCESS_UTILITY_QUERY_NONATOMIC,
    PROCESS_UTILITY_TOPLEVEL,
};
use types_nodes::portalcmds::ParamListInfo;
use types_portal::QueryCompletion;

use backend_tcop_utility_out_seams as rt;

use crate::classify::{
    CheckRestrictedOperation, ClassifyUtilityCommandAsReadOnly, PreventCommandDuringRecovery,
    PreventCommandIfParallelMode, PreventCommandIfReadOnly,
};
use crate::commandtag::CreateCommandTag;
use crate::consts::{
    CMDTAG_COPY, CMDTAG_ROLLBACK, COMMAND_IS_STRICTLY_READ_ONLY, COMMAND_OK_IN_PARALLEL_MODE,
    COMMAND_OK_IN_READ_ONLY_TXN, COMMAND_OK_IN_RECOVERY, ROLE_PG_CHECKPOINT,
};

/// `SetQueryCompletion(qc, commandTag, nprocessed)` (cmdtag.h, inline). `qc` is
/// `Option` because the C parameter is a nullable pointer; the `qc == NULL`
/// guard becomes `if let Some(qc) = qc`. `CommandTag` is the
/// `types_core::cmdtag::CommandTag` newtype; `QueryCompletion.commandTag` is the
/// `types_portal` `i32` alias, so the value crosses through its `.0`.
#[inline]
fn set_query_completion(qc: &mut Option<&mut QueryCompletion>, command_tag: CommandTag, nprocessed: u64) {
    if let Some(qc) = qc.as_mut() {
        qc.commandTag = command_tag.0;
        qc.nprocessed = nprocessed;
    }
}

/// `ProcessUtility` (utility.c:521-545) — the general utility-command invoker.
///
/// In C, `ProcessUtility_hook` may interpose; it is not modeled, so this always
/// runs [`standard_process_utility`].
#[allow(clippy::too_many_arguments)]
pub fn ProcessUtility<'mcx>(
    mcx: Mcx<'mcx>,
    pstmt: &PlannedStmt<'mcx>,
    query_string: &str,
    read_only_tree: bool,
    context: ProcessUtilityContext,
    params: ParamListInfo,
    dest: DestReceiverHandle,
    qc: &mut QueryCompletion,
) -> PgResult<()> {
    // Assert(IsA(pstmt, PlannedStmt));
    // Assert(pstmt->commandType == CMD_UTILITY);
    // Assert(qc == NULL || qc->commandTag == CMDTAG_UNKNOWN);
    standard_ProcessUtility(mcx, pstmt, query_string, read_only_tree, context, params, dest, qc)
}

/// `standard_ProcessUtility` (utility.c:548-1090) — the utility-command dispatch
/// switch; commands needing event-trigger support fall through to the
/// [`rt::process_utility_slow`] seam.
///
/// `mcx` is the per-utility working context. C does `pstmt = copyObject(pstmt)`
/// (when `readOnlyTree`) and `make_parsestate(NULL)` in `CurrentMemoryContext`
/// (the per-message context, reset after the command); here both allocate in
/// `mcx`, which the caller owns and drops on return — the owned analogue of the
/// per-message reset / `free_parsestate`. `qc` is filled in place.
#[allow(clippy::too_many_arguments)]
pub fn standard_ProcessUtility<'mcx>(
    mcx: Mcx<'mcx>,
    pstmt: &PlannedStmt<'mcx>,
    query_string: &str,
    read_only_tree: bool,
    context: ProcessUtilityContext,
    params: ParamListInfo,
    dest: DestReceiverHandle,
    qc: &mut QueryCompletion,
) -> PgResult<()> {
    let is_top_level = context == PROCESS_UTILITY_TOPLEVEL;
    let is_atomic_context = !(context == PROCESS_UTILITY_TOPLEVEL
        || context == PROCESS_UTILITY_QUERY_NONATOMIC)
        || rt::is_transaction_block::call();

    // This can recurse, so check for excessive recursion.
    rt::check_stack_depth::call()?;

    // If the given node tree is read-only, make a copy so subsequent
    // parse-analysis doesn't damage the original tree. The copy lives in `mcx`.
    let owned_copy: Option<PlannedStmt<'mcx>> = if read_only_tree {
        Some(pstmt.clone_in(mcx)?)
    } else {
        None
    };
    let pstmt: &PlannedStmt<'mcx> = owned_copy.as_ref().unwrap_or(pstmt);

    // `parsetree = pstmt->utilityStmt` — utility.c requires it non-NULL.
    let parsetree: &Node<'mcx> = pstmt
        .utilityStmt
        .as_deref()
        .expect("standard_ProcessUtility: PlannedStmt.utilityStmt is NULL");

    // Prohibit read/write commands in read-only states.
    let readonly_flags = ClassifyUtilityCommandAsReadOnly(parsetree)?;
    if readonly_flags != COMMAND_IS_STRICTLY_READ_ONLY
        && (rt::xact_read_only::call() || rt::is_in_parallel_mode::call())
    {
        let commandtag = CreateCommandTag(parsetree)?;
        let tag_name = get_command_tag_name(commandtag.0);

        if (readonly_flags & COMMAND_OK_IN_READ_ONLY_TXN) == 0 {
            PreventCommandIfReadOnly(tag_name)?;
        }
        if (readonly_flags & COMMAND_OK_IN_PARALLEL_MODE) == 0 {
            PreventCommandIfParallelMode(tag_name)?;
        }
        if (readonly_flags & COMMAND_OK_IN_RECOVERY) == 0 {
            PreventCommandDuringRecovery(tag_name)?;
        }
    }

    // The C function builds a `make_parsestate(NULL)` and assigns
    // `pstate->p_sourcetext = queryString` and `pstate->p_queryEnv = queryEnv`.
    // The owned parsestate lives in `mcx`; dropping `mcx` is `free_parsestate`.
    let mut pstate = backend_parser_analyze_seams::make_parsestate::call(mcx, None)?;
    pstate.p_sourcetext = Some(mcx::PgString::from_str_in(query_string, mcx)?);

    let stmt_location = pstmt.stmt_location;
    let stmt_len = pstmt.stmt_len;

    let result = dispatch_switch(
        mcx,
        pstmt,
        parsetree,
        query_string,
        context,
        params,
        dest,
        stmt_location,
        stmt_len,
        &mut pstate,
        qc,
        is_top_level,
        is_atomic_context,
    );

    // C has no PG_TRY here: on ereport(ERROR) the longjmp unwinds and the
    // aborting memory context reclaims the parsestate. On the success path C runs
    // `free_parsestate(pstate)` then `CommandCounterIncrement()`. The `?` below is
    // the unwind; the parsestate is `mcx`-owned (its drop is `free_parsestate`).
    result?;

    // Make effects of commands visible (see bug #15631).
    rt::command_counter_increment::call()?;
    Ok(())
}

/// The `switch (nodeTag(parsetree))` body of `standard_ProcessUtility`
/// (utility.c:614-1074), factored out so the trailing `CommandCounterIncrement`
/// always runs on the success path.
#[allow(clippy::too_many_arguments)]
fn dispatch_switch<'mcx>(
    mcx: Mcx<'mcx>,
    pstmt: &PlannedStmt<'mcx>,
    parsetree: &Node<'mcx>,
    query_string: &str,
    context: ProcessUtilityContext,
    params: ParamListInfo,
    dest: DestReceiverHandle,
    stmt_location: i32,
    stmt_len: i32,
    pstate: &mut ParseState<'mcx>,
    qc: &mut QueryCompletion,
    is_top_level: bool,
    is_atomic_context: bool,
) -> PgResult<()> {
    // The C `qc` is a nullable pointer the dispatch threads to a few handlers as
    // `Option<&mut QueryCompletion>`; pquery always passes a real `qc`, so the
    // local `qc_opt` wraps it. (Distinct sub-statements never need to pass NULL
    // through here — the multi-statement NULL handling lives in pquery.)
    let mut qc_opt: Option<&mut QueryCompletion> = Some(qc);

    match parsetree {
        // ******************** transactions ********************
        Node::TransactionStmt(stmt) => {
            match stmt.kind {
                // START TRANSACTION (SQL99) is identical to BEGIN.
                TransactionStmtKind::TRANS_STMT_BEGIN | TransactionStmtKind::TRANS_STMT_START => {
                    rt::begin_transaction_block::call()?;
                    for cell in stmt.options.iter() {
                        let item = match &**cell {
                            Node::DefElem(d) => d,
                            _ => continue,
                        };
                        let defname = item.defname.as_deref();
                        // C: SetPGVariable(name, list_make1(item->arg), true).
                        let arg: &Node = item
                            .arg
                            .as_deref()
                            .expect("transaction option DefElem has NULL arg");
                        if let Some(name @ ("transaction_isolation"
                        | "transaction_read_only"
                        | "transaction_deferrable")) = defname
                        {
                            rt::set_pg_variable::call(name, arg, true)?;
                        }
                    }
                }

                TransactionStmtKind::TRANS_STMT_COMMIT => {
                    if !rt::end_transaction_block::call(stmt.chain)? {
                        // report unsuccessful commit in qc
                        set_query_completion(&mut qc_opt, CMDTAG_ROLLBACK, 0);
                    }
                }

                TransactionStmtKind::TRANS_STMT_PREPARE => {
                    if !rt::prepare_transaction_block::call(stmt.gid.as_deref())? {
                        // report unsuccessful commit in qc
                        set_query_completion(&mut qc_opt, CMDTAG_ROLLBACK, 0);
                    }
                }

                TransactionStmtKind::TRANS_STMT_COMMIT_PREPARED => {
                    rt::prevent_in_transaction_block::call(is_top_level, "COMMIT PREPARED")?;
                    rt::finish_prepared_transaction::call(stmt.gid.as_deref(), true)?;
                }

                TransactionStmtKind::TRANS_STMT_ROLLBACK_PREPARED => {
                    rt::prevent_in_transaction_block::call(is_top_level, "ROLLBACK PREPARED")?;
                    rt::finish_prepared_transaction::call(stmt.gid.as_deref(), false)?;
                }

                TransactionStmtKind::TRANS_STMT_ROLLBACK => {
                    rt::user_abort_transaction_block::call(stmt.chain)?;
                }

                TransactionStmtKind::TRANS_STMT_SAVEPOINT => {
                    rt::require_transaction_block::call(is_top_level, "SAVEPOINT")?;
                    rt::define_savepoint::call(stmt.savepoint_name.as_deref())?;
                }

                TransactionStmtKind::TRANS_STMT_RELEASE => {
                    rt::require_transaction_block::call(is_top_level, "RELEASE SAVEPOINT")?;
                    rt::release_savepoint::call(stmt.savepoint_name.as_deref())?;
                }

                TransactionStmtKind::TRANS_STMT_ROLLBACK_TO => {
                    rt::require_transaction_block::call(is_top_level, "ROLLBACK TO SAVEPOINT")?;
                    rt::rollback_to_savepoint::call(stmt.savepoint_name.as_deref())?;
                    // CommitTransactionCommand re-defines the savepoint again.
                }
            }
        }

        // Portal (cursor) manipulation
        Node::DeclareCursorStmt(_) => {
            rt::perform_cursor_open::call(mcx, pstate, parsetree, params, is_top_level)?;
        }

        Node::ClosePortalStmt(stmt) => {
            CheckRestrictedOperation("CLOSE")?;
            rt::perform_portal_close::call(stmt.portalname.as_deref())?;
        }

        Node::FetchStmt(_) => {
            rt::perform_portal_fetch::call(mcx, parsetree, dest, qc_opt.take())?;
        }

        Node::DoStmt(_) => {
            rt::execute_do_stmt::call(mcx, pstate, parsetree, is_atomic_context)?;
        }

        Node::CreateTableSpaceStmt(_) => {
            // no event triggers for global objects
            rt::prevent_in_transaction_block::call(is_top_level, "CREATE TABLESPACE")?;
            rt::create_table_space::call(mcx, parsetree)?;
        }

        Node::DropTableSpaceStmt(_) => {
            // no event triggers for global objects
            rt::prevent_in_transaction_block::call(is_top_level, "DROP TABLESPACE")?;
            rt::drop_table_space::call(mcx, parsetree)?;
        }

        Node::AlterTableSpaceOptionsStmt(_) => {
            // no event triggers for global objects
            rt::alter_table_space_options::call(mcx, parsetree)?;
        }

        Node::TruncateStmt(_) => {
            rt::execute_truncate::call(mcx, parsetree)?;
        }

        Node::CopyStmt(_) => {
            let processed = rt::do_copy::call(mcx, pstate, parsetree, stmt_location, stmt_len)?;
            set_query_completion(&mut qc_opt, CMDTAG_COPY, processed);
        }

        Node::PrepareStmt(_) => {
            CheckRestrictedOperation("PREPARE")?;
            rt::prepare_query::call(mcx, pstate, parsetree, stmt_location, stmt_len)?;
        }

        Node::ExecuteStmt(_) => {
            rt::execute_query::call(mcx, pstate, parsetree, params, dest, qc_opt.take())?;
        }

        Node::DeallocateStmt(_) => {
            CheckRestrictedOperation("DEALLOCATE")?;
            rt::deallocate_query::call(parsetree)?;
        }

        Node::GrantRoleStmt(_) => {
            // no event triggers for global objects
            rt::grant_role::call(mcx, pstate, parsetree)?;
        }

        Node::CreatedbStmt(_) => {
            // no event triggers for global objects
            rt::prevent_in_transaction_block::call(is_top_level, "CREATE DATABASE")?;
            rt::createdb::call(mcx, pstate, parsetree)?;
        }

        Node::AlterDatabaseStmt(_) => {
            // no event triggers for global objects
            rt::alter_database::call(mcx, pstate, parsetree, is_top_level)?;
        }

        Node::AlterDatabaseRefreshCollStmt(_) => {
            // no event triggers for global objects
            rt::alter_database_refresh_coll::call(mcx, parsetree)?;
        }

        Node::AlterDatabaseSetStmt(_) => {
            // no event triggers for global objects
            rt::alter_database_set::call(mcx, parsetree)?;
        }

        Node::DropdbStmt(_) => {
            // no event triggers for global objects
            rt::prevent_in_transaction_block::call(is_top_level, "DROP DATABASE")?;
            rt::drop_database::call(mcx, pstate, parsetree)?;
        }

        // Query-level asynchronous notification
        Node::NotifyStmt(stmt) => {
            rt::async_notify::call(stmt.conditionname.as_deref(), stmt.payload.as_deref())?;
        }

        Node::ListenStmt(stmt) => {
            CheckRestrictedOperation("LISTEN")?;

            // LISTEN is not allowed in background processes.
            if rt::my_backend_type::call() != BackendType::Backend {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    // translator: %s is name of a SQL command, eg LISTEN
                    .errmsg(format!(
                        "cannot execute {} within a background process",
                        "LISTEN"
                    ))
                    .into_error());
            }

            let name = stmt
                .conditionname
                .as_deref()
                .expect("LISTEN: conditionname is NULL");
            rt::async_listen::call(name)?;
        }

        Node::UnlistenStmt(stmt) => {
            CheckRestrictedOperation("UNLISTEN")?;
            match stmt.conditionname.as_deref() {
                Some(name) => rt::async_unlisten::call(name)?,
                None => rt::async_unlisten_all::call()?,
            }
        }

        Node::LoadStmt(stmt) => {
            rt::close_all_vfds::call(); // probably not necessary...
                                        // Allowed names are restricted if you're not superuser
            rt::load_file::call(stmt.filename.as_deref(), !rt::superuser::call())?;
        }

        Node::CallStmt(_) => {
            rt::execute_call_stmt::call(mcx, parsetree, params, is_atomic_context, dest)?;
        }

        Node::ClusterStmt(_) => {
            rt::cluster::call(mcx, pstate, parsetree, is_top_level)?;
        }

        Node::VacuumStmt(_) => {
            rt::exec_vacuum::call(mcx, pstate, parsetree, is_top_level)?;
        }

        Node::ExplainStmt(_) => {
            rt::explain_query::call(mcx, pstate, parsetree, params, dest)?;
        }

        Node::AlterSystemStmt(_) => {
            rt::prevent_in_transaction_block::call(is_top_level, "ALTER SYSTEM")?;
            rt::alter_system_set_config_file::call(parsetree)?;
        }

        Node::VariableSetStmt(_) => {
            rt::exec_set_variable_stmt::call(parsetree, is_top_level)?;
        }

        Node::VariableShowStmt(n) => {
            rt::get_pg_variable::call(mcx, n.name.as_deref(), dest)?;
        }

        Node::DiscardStmt(_) => {
            // should we allow DISCARD PLANS?
            CheckRestrictedOperation("DISCARD")?;
            rt::discard_command::call(parsetree, is_top_level)?;
        }

        Node::CreateEventTrigStmt(_) => {
            // no event triggers on event triggers
            rt::create_event_trigger::call(mcx, parsetree)?;
        }

        Node::AlterEventTrigStmt(_) => {
            // no event triggers on event triggers
            rt::alter_event_trigger::call(parsetree)?;
        }

        // ******************************** ROLE statements ****
        Node::CreateRoleStmt(_) => {
            rt::create_role::call(mcx, pstate, parsetree)?;
        }

        Node::AlterRoleStmt(_) => {
            rt::alter_role::call(mcx, pstate, parsetree)?;
        }

        Node::AlterRoleSetStmt(_) => {
            rt::alter_role_set::call(mcx, parsetree)?;
        }

        Node::DropRoleStmt(_) => {
            rt::drop_role::call(parsetree)?;
        }

        Node::ReassignOwnedStmt(_) => {
            rt::reassign_owned_objects::call(parsetree)?;
        }

        Node::LockStmt(_) => {
            // LOCK TABLE outside a transaction block is user error.
            rt::require_transaction_block::call(is_top_level, "LOCK TABLE")?;
            rt::lock_table_command::call(parsetree)?;
        }

        Node::ConstraintsSetStmt(_) => {
            rt::warn_no_transaction_block::call(is_top_level, "SET CONSTRAINTS")?;
            rt::after_trigger_set_state::call(parsetree)?;
        }

        Node::CheckPointStmt(_) => {
            if !rt::has_privs_of_role::call(rt::get_user_id::call(), ROLE_PG_CHECKPOINT) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    // translator: %s is name of a SQL command, eg CHECKPOINT
                    .errmsg(format!(
                        "permission denied to execute {} command",
                        "CHECKPOINT"
                    ))
                    .errdetail(format!(
                        "Only roles with privileges of the \"{}\" role may execute this command.",
                        "pg_checkpoint"
                    ))
                    .into_error());
            }

            rt::request_checkpoint::call(
                types_wal::xlog_consts::CHECKPOINT_IMMEDIATE
                    | types_wal::xlog_consts::CHECKPOINT_WAIT
                    | (if rt::checkpoint_recovery_in_progress::call() {
                        0
                    } else {
                        types_wal::xlog_consts::CHECKPOINT_FORCE
                    }),
            )?;
        }

        // The following statements have event-trigger support only in some
        // cases, so we "fast path" them in the other cases.
        Node::GrantStmt(stmt) => {
            if rt::event_trigger_supports_object_type::call(stmt.objtype) {
                rt::process_utility_slow::call(
                    mcx, pstate, pstmt, query_string, context, params, dest, is_top_level,
                    qc_opt.take(),
                )?;
            } else {
                rt::execute_grant_stmt::call(mcx, parsetree)?;
            }
        }

        Node::DropStmt(stmt) => {
            if rt::event_trigger_supports_object_type::call(stmt.removeType) {
                rt::process_utility_slow::call(
                    mcx, pstate, pstmt, query_string, context, params, dest, is_top_level,
                    qc_opt.take(),
                )?;
            } else {
                ExecDropStmt(mcx, parsetree, is_top_level)?;
            }
        }

        Node::RenameStmt(stmt) => {
            if rt::event_trigger_supports_object_type::call(stmt.renameType) {
                rt::process_utility_slow::call(
                    mcx, pstate, pstmt, query_string, context, params, dest, is_top_level,
                    qc_opt.take(),
                )?;
            } else {
                rt::exec_rename_stmt::call(mcx, parsetree)?;
            }
        }

        Node::AlterObjectDependsStmt(stmt) => {
            if rt::event_trigger_supports_object_type::call(stmt.objectType) {
                rt::process_utility_slow::call(
                    mcx, pstate, pstmt, query_string, context, params, dest, is_top_level,
                    qc_opt.take(),
                )?;
            } else {
                rt::exec_alter_object_depends_stmt::call(mcx, parsetree)?;
            }
        }

        Node::AlterObjectSchemaStmt(stmt) => {
            if rt::event_trigger_supports_object_type::call(stmt.objectType) {
                rt::process_utility_slow::call(
                    mcx, pstate, pstmt, query_string, context, params, dest, is_top_level,
                    qc_opt.take(),
                )?;
            } else {
                rt::exec_alter_object_schema_stmt::call(mcx, parsetree)?;
            }
        }

        Node::AlterOwnerStmt(stmt) => {
            if rt::event_trigger_supports_object_type::call(stmt.objectType) {
                rt::process_utility_slow::call(
                    mcx, pstate, pstmt, query_string, context, params, dest, is_top_level,
                    qc_opt.take(),
                )?;
            } else {
                rt::exec_alter_owner_stmt::call(mcx, parsetree)?;
            }
        }

        Node::CommentStmt(stmt) => {
            if rt::event_trigger_supports_object_type::call(stmt.objtype) {
                rt::process_utility_slow::call(
                    mcx, pstate, pstmt, query_string, context, params, dest, is_top_level,
                    qc_opt.take(),
                )?;
            } else {
                rt::comment_object::call(mcx, parsetree)?;
            }
        }

        Node::SecLabelStmt(stmt) => {
            if rt::event_trigger_supports_object_type::call(stmt.objtype) {
                rt::process_utility_slow::call(
                    mcx, pstate, pstmt, query_string, context, params, dest, is_top_level,
                    qc_opt.take(),
                )?;
            } else {
                rt::exec_sec_label_stmt::call(mcx, parsetree)?;
            }
        }

        _ => {
            // All other statement types have event trigger support.
            rt::process_utility_slow::call(
                mcx, pstate, pstmt, query_string, context, params, dest, is_top_level,
                qc_opt.take(),
            )?;
        }
    }
    Ok(())
}

/// `ProcessUtilityForAlterTable` (utility.c:1593-1610) — recursive entry from
/// ALTER TABLE for subcommands such as CREATE INDEX.
///
/// For event triggers, it "closes" the current complex-command set and starts a
/// new one afterwards, to keep command-event ordering consistent, then builds a
/// wrapper subcommand `PlannedStmt` and re-enters `ProcessUtility` (encapsulated
/// in the [`rt::process_utility_wrapper`] seam).
pub fn ProcessUtilityForAlterTable<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &Node<'mcx>,
    outer_pstmt: &PlannedStmt<'mcx>,
    query_string: &str,
    relid: types_core::primitive::Oid,
) -> PgResult<()> {
    rt::event_trigger_alter_table_end::call();

    rt::process_utility_wrapper::call(
        mcx,
        stmt,
        query_string,
        outer_pstmt.stmt_location,
        outer_pstmt.stmt_len,
    )?;

    let outer_stmt: &Node = outer_pstmt
        .utilityStmt
        .as_deref()
        .expect("ProcessUtilityForAlterTable: outer PlannedStmt.utilityStmt is NULL");
    rt::event_trigger_alter_table_start::call(outer_stmt);
    rt::event_trigger_alter_table_relid::call(relid);
    Ok(())
}

/// `ExecDropStmt` (utility.c:1958-1990, static) — dispatch a `DropStmt` to the
/// relation-removal (`RemoveRelations`) or general object-removal
/// (`RemoveObjects`) executor.
pub fn ExecDropStmt<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>, is_top_level: bool) -> PgResult<()> {
    let dstmt = match stmt {
        Node::DropStmt(d) => d,
        _ => panic!("ExecDropStmt: not a DropStmt"),
    };
    match dstmt.removeType {
        OBJECT_INDEX => {
            if dstmt.concurrent {
                rt::prevent_in_transaction_block::call(is_top_level, "DROP INDEX CONCURRENTLY")?;
            }
            // fall through
            rt::remove_relations::call(mcx, stmt)?;
        }
        OBJECT_TABLE | OBJECT_SEQUENCE | OBJECT_VIEW | OBJECT_MATVIEW | OBJECT_FOREIGN_TABLE => {
            rt::remove_relations::call(mcx, stmt)?;
        }
        _ => {
            rt::remove_objects::call(mcx, stmt)?;
        }
    }
    Ok(())
}
