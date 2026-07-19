use mcx::Mcx;
use pg_depend::ObjectAddress;
use tcop_dest::DestReceiver;
use types_error::PgResult;
use types_nodes::node_tree::Node;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::ExplainStmt;
use types_nodes::parsenodes::TransactionStmtKind::*;
use types_nodes::plannodes::PlannedStmt;
use types_nodes::NodeTag;
use types_portal::{ParamListHandle, QueryCompletion, QueryEnvHandle};
use utility_seams::{
    ProcessUtilityContext, PROCESS_UTILITY_QUERY_NONATOMIC, PROCESS_UTILITY_SUBCOMMAND,
    PROCESS_UTILITY_TOPLEVEL,
};

use crate::classify::{
    CheckRestrictedOperation, ClassifyUtilityCommandAsReadOnly, PreventCommandDuringRecovery,
};
use crate::commandtag::CreateCommandTag;
use crate::consts::{
    CMDTAG_ROLLBACK, COMMAND_IS_STRICTLY_READ_ONLY, COMMAND_OK_IN_PARALLEL_MODE,
    COMMAND_OK_IN_READ_ONLY_TXN, COMMAND_OK_IN_RECOVERY,
};
use crate::{handler_gap, handler_unsupported};

// pg_authid.dat oid 4544.
const ROLE_PG_CHECKPOINT: ::types_core::Oid = 4544;

const INVALID_OBJECT_ADDRESS: ObjectAddress =
    ObjectAddress::set(types_core::InvalidOid, types_core::InvalidOid);

#[inline]
fn set_query_completion(qc: &mut Option<&mut QueryCompletion>, tag: types_core::CommandTag) {
    if let Some(qc) = qc.as_mut() {
        qc.commandTag = tag;
        qc.nprocessed = 0;
    }
}

// Uncollected command types stay loud instead of silently missing from
// pg_event_trigger_ddl_commands.
fn collect_gap(what: &str) {
    if event_trigger::EventTriggerCollectionActive() {
        panic!("unported: {what} command collection (active ddl_command_end/sql_drop state)");
    }
}

// ProcessUtility_hook (hook-surface.md section 2): enter/leave pair so a
// consumer (pg_stat_statements) can wrap the call with timing, zero the
// pstmt queryId, and track nesting. The leave fires on the error path too
// (C PG_FINALLY parity), and must not touch pstmt (C: the tree may already
// be freed by a ROLLBACK). Empty by default (S1 ships no consumer).
seam_core::tap!(pub fn tap_process_utility_enter<'a, 'b>(pstmt: &'a PlannedStmt<'b>));
seam_core::tap!(
    pub fn tap_process_utility_leave<'a>(
        ok: bool,
        source_text: &'a str,
        qc: Option<&'a QueryCompletion>,
    )
);

// C's hookable entry; no plugin surface exists, so this IS standard_ProcessUtility.
// Whole utility path is cold: per-statement DDL dispatch, never per-tuple —
// keeps the ~100 dispatch arms out of the query-path text (icache/iTLB).
#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
pub fn ProcessUtility<'p, 'a, 's, 'd, 'q, 'mcx>(
    mcx: Mcx<'mcx>,
    pstmt: &'p PlannedStmt<'a>,
    source_text: &'s str,
    read_only_tree: bool,
    context: ProcessUtilityContext,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    dest: &'d mut DestReceiver<'mcx>,
    qc: Option<&'q mut QueryCompletion>,
) -> PgResult<()> {
    tap_process_utility_enter::call_if(|f| f(pstmt));
    debug_assert!(pstmt.commandType == CmdType::CMD_UTILITY);
    debug_assert!(qc
        .as_ref()
        .is_none_or(|qc| qc.commandTag == types_portal::CMDTAG_UNKNOWN));
    let mut qc = qc;
    let r = standard_ProcessUtility(
        mcx,
        pstmt,
        source_text,
        read_only_tree,
        context,
        params,
        query_env,
        dest,
        qc.as_deref_mut(),
    );
    tap_process_utility_leave::call_if(|f| f(r.is_ok(), source_text, qc.as_deref()));
    r
}

#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
pub fn standard_ProcessUtility<'p, 'a, 's, 'd, 'q, 'mcx>(
    mcx: Mcx<'mcx>,
    pstmt: &'p PlannedStmt<'a>,
    source_text: &'s str,
    read_only_tree: bool,
    context: ProcessUtilityContext,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    dest: &'d mut DestReceiver<'mcx>,
    qc: Option<&'q mut QueryCompletion>,
) -> PgResult<()> {
    let is_top_level = context == PROCESS_UTILITY_TOPLEVEL;
    let is_atomic_context = !(context == PROCESS_UTILITY_TOPLEVEL
        || context == PROCESS_UTILITY_QUERY_NONATOMIC)
        || xact::IsTransactionBlock();

    // C: check_stack_depth() — recursion guard unported repo-wide (stack lane).

    // C: pstmt = copyObject(pstmt) — consumers scribble on the tree, so a
    // plancache-held tree is never executed directly.
    let pstmt: &'p PlannedStmt<'a> = if read_only_tree {
        let copy = copyfuncs::copy_utility_planned_stmt(mcx, pstmt)?;
        // Retention contract (unify_stmt_lifetime): the copy lives in the
        // portal-context mcx, which outlives the utility call.
        unsafe { core::mem::transmute::<&PlannedStmt<'_>, &'p PlannedStmt<'a>>(copy) }
    } else {
        pstmt
    };

    let parsetree: Node<'a> = pstmt
        .utilityStmt
        .expect("standard_ProcessUtility: PlannedStmt.utilityStmt is NULL");

    let readonly_flags = ClassifyUtilityCommandAsReadOnly(parsetree)?;
    if readonly_flags != COMMAND_IS_STRICTLY_READ_ONLY
        && (xact::XactReadOnly() || xact::IsInParallelMode())
    {
        let commandtag = CreateCommandTag(parsetree);
        let tag_name = cmdtag::GetCommandTagName(commandtag);

        if (readonly_flags & COMMAND_OK_IN_READ_ONLY_TXN) == 0 {
            xact::PreventCommandIfReadOnly(tag_name)?;
        }
        if (readonly_flags & COMMAND_OK_IN_PARALLEL_MODE) == 0 {
            xact::PreventCommandIfParallelMode(tag_name)?;
        }
        if (readonly_flags & COMMAND_OK_IN_RECOVERY) == 0 {
            PreventCommandDuringRecovery(tag_name)?;
        }
    }

    // C: pstate = make_parsestate(NULL); the two consumers a live arm needs
    // (p_sourcetext, p_queryEnv) are threaded as parameters instead.

    let mut qc = qc;
    dispatch_switch(
        mcx,
        parsetree,
        pstmt,
        source_text,
        context,
        params,
        query_env,
        dest,
        &mut qc,
    )?;

    xact::CommandCounterIncrement()?;
    Ok(())
}

// Retention contract (execmain::shorten_pstmt precedent): the statement arena
// and the portal context both outlive the utility call, and nothing derived
// from the unified handles escapes it — dest receives copied bytes only.
unsafe fn unify_stmt_lifetime<'u>(s: &ExplainStmt<'_>) -> &'u ExplainStmt<'u> {
    unsafe { core::mem::transmute::<&ExplainStmt<'_>, &'u ExplainStmt<'u>>(s) }
}

// Same retention contract: EvaluateParams transforms the raw param exprs in
// the statement arena, which outlives the utility call.
unsafe fn unify_execute_lifetime<'u>(
    s: &types_nodes::parsenodes::ExecuteStmt<'_>,
) -> &'u types_nodes::parsenodes::ExecuteStmt<'u> {
    unsafe {
        core::mem::transmute::<
            &types_nodes::parsenodes::ExecuteStmt<'_>,
            &'u types_nodes::parsenodes::ExecuteStmt<'u>,
        >(s)
    }
}

// Same retention contract: the CALL statement arena and the portal context
// outlive the utility call; dest receives copied bytes only.
unsafe fn unify_call_lifetime<'u>(
    s: &types_nodes::rawnodes::CallStmt<'_>,
) -> &'u types_nodes::rawnodes::CallStmt<'u> {
    unsafe {
        core::mem::transmute::<
            &types_nodes::rawnodes::CallStmt<'_>,
            &'u types_nodes::rawnodes::CallStmt<'u>,
        >(s)
    }
}

#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn dispatch_switch<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: Node<'_>,
    pstmt: &PlannedStmt<'_>,
    source_text: &str,
    context: ProcessUtilityContext,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    dest: &mut DestReceiver<'mcx>,
    qc: &mut Option<&mut QueryCompletion>,
) -> PgResult<()> {
    let is_top_level = context == PROCESS_UTILITY_TOPLEVEL;
    let is_atomic_context = !(context == PROCESS_UTILITY_TOPLEVEL
        || context == PROCESS_UTILITY_QUERY_NONATOMIC)
        || xact::IsTransactionBlock();
    use NodeTag::*;
    match parsetree.node_tag() {
        T_TransactionStmt => {
            let stmt = parsetree.as_transaction_stmt().unwrap();
            match stmt.kind {
                TRANS_STMT_BEGIN | TRANS_STMT_START => {
                    xact::BeginTransactionBlock()?;
                    for item in stmt.options.iter() {
                        let item = item.as_def_elem().expect("BEGIN options: DefElem list");
                        match item.defname.unwrap_or("") {
                            name @ ("transaction_isolation" | "transaction_read_only"
                            | "transaction_deferrable") => {
                                guc_funcs::SetPGVariable(name, item.arg, true)?;
                            }
                            other => panic!("unexpected BEGIN option: {other}"),
                        }
                    }
                }

                TRANS_STMT_COMMIT => {
                    if !xact::EndTransactionBlock(stmt.chain)? {
                        set_query_completion(qc, CMDTAG_ROLLBACK);
                    }
                }

                TRANS_STMT_PREPARE => {
                    let gid = stmt.gid.expect("PREPARE TRANSACTION: gid is NULL");
                    if !xact::PrepareTransactionBlock(gid)? {
                        set_query_completion(qc, CMDTAG_ROLLBACK);
                    }
                }

                TRANS_STMT_COMMIT_PREPARED => {
                    xact::PreventInTransactionBlock(is_top_level, "COMMIT PREPARED")?;
                    let gid = stmt.gid.expect("COMMIT PREPARED: gid is NULL");
                    twophase_seams::finish_prepared_transaction::call(gid, true)?;
                }

                TRANS_STMT_ROLLBACK_PREPARED => {
                    xact::PreventInTransactionBlock(is_top_level, "ROLLBACK PREPARED")?;
                    let gid = stmt.gid.expect("ROLLBACK PREPARED: gid is NULL");
                    twophase_seams::finish_prepared_transaction::call(gid, false)?;
                }

                TRANS_STMT_ROLLBACK => {
                    xact::UserAbortTransactionBlock(stmt.chain)?;
                }

                TRANS_STMT_SAVEPOINT => {
                    xact::RequireTransactionBlock(is_top_level, "SAVEPOINT")?;
                    xact::DefineSavepoint(stmt.savepoint_name)?;
                }

                TRANS_STMT_RELEASE => {
                    xact::RequireTransactionBlock(is_top_level, "RELEASE SAVEPOINT")?;
                    xact::ReleaseSavepoint(
                        stmt.savepoint_name.expect("RELEASE SAVEPOINT: name is NULL"),
                    )?;
                }

                TRANS_STMT_ROLLBACK_TO => {
                    xact::RequireTransactionBlock(is_top_level, "ROLLBACK TO SAVEPOINT")?;
                    xact::RollbackToSavepoint(
                        stmt.savepoint_name
                            .expect("ROLLBACK TO SAVEPOINT: name is NULL"),
                    )?;
                    // CommitTransactionCommand re-defines the savepoint.
                }
            }
        }

        T_DeclareCursorStmt => {
            let stmt = parsetree.as_declare_cursor_stmt().unwrap();
            // This DECLARE's own slice of the (possibly multi-statement)
            // source text; PerformCursorOpen re-derives its plan from it.
            let loc = pstmt.stmt_location.max(0) as usize;
            let stmt_text = if pstmt.stmt_len > 0 {
                &source_text[loc..loc + pstmt.stmt_len as usize]
            } else {
                &source_text[loc..]
            };
            portalcmds::PerformCursorOpen(mcx, stmt, stmt_text, source_text, params, is_top_level)?;
        }
        T_ClosePortalStmt => {
            let stmt = parsetree.as_close_portal_stmt().unwrap();
            CheckRestrictedOperation("CLOSE")?;
            portalcmds::PerformPortalClose(stmt.portalname)?;
        }
        T_FetchStmt => {
            let stmt = parsetree.as_fetch_stmt().unwrap();
            portalcmds::PerformPortalFetch(stmt, dest, qc.as_deref_mut())?;
        }

        T_DoStmt => {
            let stmt = parsetree.as_do_stmt().unwrap();
            functioncmds::ExecuteDoStmt(stmt, is_atomic_context)?;
        }

        T_CreateTableSpaceStmt => {
            xact::PreventInTransactionBlock(is_top_level, "CREATE TABLESPACE")?;
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::CreateTableSpaceStmt>()
                .expect("CreateTableSpaceStmt");
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::CreateTableSpaceStmt<'_>,
                    &types_nodes::parsenodes::CreateTableSpaceStmt<'mcx>,
                >(stmt)
            };
            commands_tablespace::CreateTableSpace(mcx, stmt)?;
        }
        T_DropTableSpaceStmt => {
            xact::PreventInTransactionBlock(is_top_level, "DROP TABLESPACE")?;
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::DropTableSpaceStmt>()
                .expect("DropTableSpaceStmt");
            commands_tablespace::DropTableSpace(mcx, stmt)?;
        }
        T_AlterTableSpaceOptionsStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterTableSpaceOptionsStmt>()
                .expect("AlterTableSpaceOptionsStmt");
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::AlterTableSpaceOptionsStmt<'_>,
                    &types_nodes::parsenodes::AlterTableSpaceOptionsStmt<'mcx>,
                >(stmt)
            };
            commands_tablespace::AlterTableSpaceOptions(mcx, stmt)?;
        }

        T_TruncateStmt => {
            let stmt = parsetree.as_truncate_stmt().unwrap();
            // Retention contract as unify_stmt_lifetime: nothing derived from
            // the statement arena escapes the utility call.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::TruncateStmt<'_>,
                    &types_nodes::parsenodes::TruncateStmt<'mcx>,
                >(stmt)
            };
            tablecmds::ExecuteTruncate(mcx, stmt)?;
        }
        T_CopyStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node.as_copy_stmt().unwrap();
            let processed =
                copy_cmd::DoCopy(mcx, stmt, source_text, pstmt.stmt_location, pstmt.stmt_len)?;
            if let Some(qc) = qc.as_mut() {
                qc.commandTag = crate::consts::CMDTAG_COPY;
                qc.nprocessed = processed;
            }
        }

        T_PrepareStmt => {
            CheckRestrictedOperation("PREPARE")?;
            let stmt = parsetree.as_prepare_stmt().unwrap();
            prepare::PrepareQuery(source_text, stmt, pstmt.stmt_location, pstmt.stmt_len)?;
        }
        T_ExecuteStmt => {
            let stmt = parsetree.as_execute_stmt().unwrap();
            // SAFETY: see unify_execute_lifetime.
            let stmt = unsafe { unify_execute_lifetime(stmt) };
            prepare::ExecuteQuery(mcx, stmt, source_text, params, None, dest, qc.as_deref_mut())?;
        }
        T_DeallocateStmt => {
            CheckRestrictedOperation("DEALLOCATE")?;
            prepare::DeallocateQuery(parsetree.as_deallocate_stmt().unwrap())?;
        }

        T_GrantStmt => {
            let stmt = parsetree.as_grant_stmt().unwrap();
            if event_trigger::EventTriggerSupportsObjectType(stmt.objtype) {
                process_utility_slow(
                    mcx, parsetree, pstmt, source_text, context, params, query_env, qc,
                )?;
            } else {
                aclchk::ExecuteGrantStmt(mcx, stmt)?;
            }
        }
        T_GrantRoleStmt => {
            let stmt = parsetree.as_grant_role_stmt().unwrap();
            user::GrantRole(mcx, stmt)?;
        }

        T_CreatedbStmt => {
            xact::PreventInTransactionBlock(is_top_level, "CREATE DATABASE")?;
            let stmt = parsetree.as_createdb_stmt().unwrap();
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::CreatedbStmt<'_>,
                    &types_nodes::parsenodes::CreatedbStmt<'mcx>,
                >(stmt)
            };
            dbcommands::createdb(mcx, stmt)?;
        }
        T_AlterDatabaseStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterDatabaseStmt>()
                .expect("AlterDatabaseStmt");
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::AlterDatabaseStmt<'_>,
                    &types_nodes::parsenodes::AlterDatabaseStmt<'mcx>,
                >(stmt)
            };
            dbcommands::AlterDatabase(mcx, stmt, is_top_level)?;
        }
        T_AlterDatabaseRefreshCollStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterDatabaseRefreshCollStmt>()
                .expect("AlterDatabaseRefreshCollStmt");
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::AlterDatabaseRefreshCollStmt<'_>,
                    &types_nodes::parsenodes::AlterDatabaseRefreshCollStmt<'mcx>,
                >(stmt)
            };
            dbcommands::AlterDatabaseRefreshColl(mcx, stmt)?;
        }
        T_AlterDatabaseSetStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterDatabaseSetStmt>()
                .expect("AlterDatabaseSetStmt");
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::AlterDatabaseSetStmt<'_>,
                    &types_nodes::parsenodes::AlterDatabaseSetStmt<'mcx>,
                >(stmt)
            };
            dbcommands::AlterDatabaseSet(mcx, stmt)?;
        }
        T_DropdbStmt => {
            xact::PreventInTransactionBlock(is_top_level, "DROP DATABASE")?;
            let stmt = parsetree.as_dropdb_stmt().unwrap();
            let mut force = false;
            for opt in stmt.options.iter() {
                let d = opt.as_def_elem().expect("dropdb options are DefElems");
                match d.defname.unwrap_or("") {
                    "force" => force = true,
                    other => {
                        return Err(elog::ereport(types_error::ERROR)
                            .errcode(types_error::ERRCODE_SYNTAX_ERROR)
                            .errmsg(format!("unrecognized DROP DATABASE option \"{other}\""))
                            .errposition(d.location + 1)
                            .into_error()
                            .into())
                    }
                }
            }
            dbcommands::dropdb(mcx, stmt.dbname.unwrap_or(""), stmt.missing_ok, force)?;
        }

        T_NotifyStmt => {
            let stmt = parsetree.as_notify_stmt().unwrap();
            commands_async::Async_Notify(stmt.conditionname.unwrap_or(""), stmt.payload)?;
        }
        T_ListenStmt => {
            let stmt = parsetree.as_listen_stmt().unwrap();
            CheckRestrictedOperation("LISTEN")?;
            // Background processes have no way to drain NOTIFY messages and
            // would block async SLRU cleanout indefinitely (utility.c:811).
            if miscinit::GetMyBackendType() != types_core::BackendType::Backend {
                return Err(elog::ereport(types_error::ERROR)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("cannot execute LISTEN within a background process")
                    .into_error()
                    .into());
            }
            commands_async::Async_Listen(stmt.conditionname.unwrap_or(""))?;
        }
        T_UnlistenStmt => {
            let stmt = parsetree.as_unlisten_stmt().unwrap();
            CheckRestrictedOperation("UNLISTEN")?;
            match stmt.conditionname {
                Some(name) => commands_async::Async_Unlisten(name)?,
                None => commands_async::Async_UnlistenAll()?,
            }
        }

        // load_file over dfmgr's builtin registry: no dlopen exists, so an
        // unregistered filename is C's file-access error. C's !superuser()
        // path restriction is skipped (no filesystem paths to restrict).
        T_LoadStmt => {
            let stmt = parsetree.as_load_stmt().expect("LoadStmt");
            dfmgr::load_file(stmt.filename)?;
        }
        T_CallStmt => {
            let stmt = parsetree.as_call_stmt().expect("CallStmt");
            functioncmds::ExecuteCallStmt(
                mcx,
                unsafe { unify_call_lifetime(stmt) },
                params,
                is_atomic_context,
                dest,
            )?;
        }
        T_ClusterStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::ClusterStmt>()
                .expect("ClusterStmt");
            commands_cluster::cluster(mcx, stmt, is_top_level)?;
        }
        T_VacuumStmt => {
            // ExecVacuum's VACUUM half lives in commands_vacuum, the ANALYZE
            // half in commands_analyze (each panics on the other's lane).
            let stmt = parsetree.as_vacuum_stmt().unwrap();
            if stmt.is_vacuumcmd {
                commands_vacuum::ExecVacuum(mcx, stmt, source_text, is_top_level)?;
            } else {
                commands_analyze::ExecVacuum(mcx, stmt, source_text, is_top_level)?;
            }
        }
        T_ExplainStmt => {
            let stmt = parsetree.as_explain_stmt().unwrap();
            // SAFETY: see unify_stmt_lifetime.
            let stmt = unsafe { unify_stmt_lifetime(stmt) };
            explain::ExplainQuery(mcx, stmt, source_text, params, query_env, dest)?;
        }
        T_AlterSystemStmt => {
            xact::PreventInTransactionBlock(is_top_level, "ALTER SYSTEM")?;
            guc_funcs::AlterSystemSetConfigFile(parsetree.as_alter_system_stmt().unwrap())?;
        }
        T_VariableSetStmt => {
            let stmt = parsetree.as_variable_set_stmt().unwrap();
            guc_funcs::ExecSetVariableStmt(stmt, is_top_level)?;
        }
        T_VariableShowStmt => {
            let n = parsetree.as_variable_show_stmt().unwrap();
            guc_funcs::GetPGVariable(mcx, n.name.unwrap_or(""), dest)?;
        }
        T_DiscardStmt => {
            CheckRestrictedOperation("DISCARD")?;
            discard::DiscardCommand(parsetree.as_discard_stmt().unwrap(), is_top_level)?;
        }

        // No event triggers on event triggers.
        T_CreateEventTrigStmt => {
            let stmt = parsetree.as_create_event_trig_stmt().unwrap();
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::CreateEventTrigStmt<'_>,
                    &types_nodes::parsenodes::CreateEventTrigStmt<'mcx>,
                >(stmt)
            };
            event_trigger::CreateEventTrigger(mcx, stmt)?;
        }
        T_AlterEventTrigStmt => {
            let stmt = parsetree.as_alter_event_trig_stmt().unwrap();
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::AlterEventTrigStmt<'_>,
                    &types_nodes::parsenodes::AlterEventTrigStmt<'mcx>,
                >(stmt)
            };
            event_trigger::AlterEventTrigger(mcx, stmt)?;
        }

        T_CreatePublicationStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::CreatePublicationStmt>()
                .expect("CreatePublicationStmt");
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::CreatePublicationStmt<'_>,
                    &types_nodes::parsenodes::CreatePublicationStmt<'mcx>,
                >(stmt)
            };
            commands_publicationcmds::CreatePublication(mcx, stmt, source_text)?;
        }
        T_AlterPublicationStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterPublicationStmt>()
                .expect("AlterPublicationStmt");
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::AlterPublicationStmt<'_>,
                    &types_nodes::parsenodes::AlterPublicationStmt<'mcx>,
                >(stmt)
            };
            commands_publicationcmds::AlterPublication(mcx, stmt, source_text)?;
        }

        T_CreateSubscriptionStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::CreateSubscriptionStmt>()
                .expect("CreateSubscriptionStmt");
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::CreateSubscriptionStmt<'_>,
                    &types_nodes::parsenodes::CreateSubscriptionStmt<'mcx>,
                >(stmt)
            };
            subscriptioncmds::CreateSubscription(mcx, stmt, is_top_level)?;
        }
        T_AlterSubscriptionStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterSubscriptionStmt>()
                .expect("AlterSubscriptionStmt");
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::AlterSubscriptionStmt<'_>,
                    &types_nodes::parsenodes::AlterSubscriptionStmt<'mcx>,
                >(stmt)
            };
            subscriptioncmds::AlterSubscription(mcx, stmt, is_top_level)?;
        }
        T_DropSubscriptionStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::DropSubscriptionStmt>()
                .expect("DropSubscriptionStmt");
            subscriptioncmds::DropSubscription(mcx, stmt, is_top_level)?;
        }

        T_CreateRoleStmt => {
            let stmt = parsetree.as_create_role_stmt().unwrap();
            user::CreateRole(mcx, stmt)?;
        }
        T_AlterRoleStmt => {
            let stmt = parsetree.as_alter_role_stmt().unwrap();
            user::AlterRole(mcx, stmt)?;
        }
        T_AlterRoleSetStmt => {
            let stmt = parsetree.as_alter_role_set_stmt().unwrap();
            user::AlterRoleSet(mcx, stmt)?;
        }
        T_DropRoleStmt => {
            let stmt = parsetree.as_drop_role_stmt().unwrap();
            user::DropRole(mcx, stmt)?;
        }
        T_ReassignOwnedStmt => {
            let stmt = parsetree.as_reassign_owned_stmt().unwrap();
            user::ReassignOwnedObjects(mcx, stmt)?;
        }

        T_LockStmt => {
            xact::RequireTransactionBlock(is_top_level, "LOCK TABLE")?;
            let stmt = parsetree.as_lock_stmt().unwrap();
            lockcmds::LockTableCommand(mcx, stmt)?;
        }
        T_ConstraintsSetStmt => {
            xact::WarnNoTransactionBlock(is_top_level, "SET CONSTRAINTS")?;
            let stmt = parsetree.as_constraints_set_stmt().unwrap();
            trigger::AfterTriggerSetState(mcx, stmt)?;
        }
        T_CheckPointStmt => {
            if !acl_seams::has_privs_of_role::call(
                miscinit::GetUserId(),
                ROLE_PG_CHECKPOINT,
            )? {
                return Err(::elog::ereport(types_error::ERROR)
                    .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg("permission denied to execute CHECKPOINT command")
                    .errdetail(
                        "Only roles with privileges of the \"pg_checkpoint\" role may \
                         execute this command."
                            .to_string(),
                    )
                    .into_error()
                    .into());
            }
            let force =
                if transam_xlog::RecoveryInProgress() { 0 } else { transam_xlog::CHECKPOINT_FORCE };
            checkpointer_seams::request_checkpoint::call(
                transam_xlog::CHECKPOINT_IMMEDIATE | transam_xlog::CHECKPOINT_WAIT | force,
            )?;
        }

        T_DropStmt => {
            let stmt = parsetree.as_drop_stmt().unwrap();
            if event_trigger::EventTriggerSupportsObjectType(stmt.removeType) {
                process_utility_slow(
                    mcx, parsetree, pstmt, source_text, context, params, query_env, qc,
                )?;
            } else {
                exec_drop_stmt(mcx, parsetree, is_top_level)?;
            }
        }

        T_CommentStmt => {
            let stmt = parsetree.as_comment_stmt().unwrap();
            if event_trigger::EventTriggerSupportsObjectType(stmt.objtype) {
                process_utility_slow(
                    mcx, parsetree, pstmt, source_text, context, params, query_env, qc,
                )?;
            } else {
                exec_comment_stmt(mcx, parsetree)?;
            }
        }

        T_SecLabelStmt => {
            let stmt = parsetree.as_sec_label_stmt().unwrap();
            if event_trigger::EventTriggerSupportsObjectType(stmt.objtype) {
                process_utility_slow(
                    mcx, parsetree, pstmt, source_text, context, params, query_env, qc,
                )?;
            } else {
                exec_seclabel_stmt(mcx, parsetree)?;
            }
        }

        T_RenameStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::RenameStmt>()
                .expect("RenameStmt");
            if event_trigger::EventTriggerSupportsObjectType(stmt.renameType) {
                process_utility_slow(
                    mcx, parsetree, pstmt, source_text, context, params, query_env, qc,
                )?;
            } else {
                exec_rename_stmt(mcx, parsetree)?;
            }
        }

        T_AlterOwnerStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterOwnerStmt>()
                .expect("AlterOwnerStmt");
            if event_trigger::EventTriggerSupportsObjectType(stmt.objectType) {
                process_utility_slow(
                    mcx, parsetree, pstmt, source_text, context, params, query_env, qc,
                )?;
            } else {
                exec_alter_owner_non_et(mcx, stmt)?;
            }
        }

        // All other statement types have event trigger support.
        _ => process_utility_slow(
            mcx, parsetree, pstmt, source_text, context, params, query_env, qc,
        )?,
    }
    Ok(())
}

struct EventTriggerCleanup(bool);
impl Drop for EventTriggerCleanup {
    fn drop(&mut self) {
        if self.0 {
            event_trigger::EventTriggerEndCompleteQuery();
        }
    }
}

// ProcessUtilitySlow (utility.c): the event-trigger-fenced DDL fan-out.
#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn process_utility_slow<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: Node<'_>,
    pstmt: &PlannedStmt<'_>,
    source_text: &str,
    context: ProcessUtilityContext,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    qc: &mut Option<&mut QueryCompletion>,
) -> PgResult<()> {
    let is_top_level = context == PROCESS_UTILITY_TOPLEVEL;
    let is_complete_query = context != PROCESS_UTILITY_SUBCOMMAND;
    let tag = CreateCommandTag(parsetree);

    let need_cleanup = is_complete_query && event_trigger::EventTriggerBeginCompleteQuery(mcx)?;
    // Drop-guard = C's PG_FINALLY around EventTriggerEndCompleteQuery.
    let _cleanup = EventTriggerCleanup(need_cleanup);

    if is_complete_query {
        event_trigger::EventTriggerDDLCommandStart(mcx, tag)?;
    }

    let address = slow_switch(
        mcx, parsetree, pstmt, source_text, context, is_top_level, params, query_env, qc,
    )?;

    if let Some(address) = address {
        event_trigger::EventTriggerCollectSimpleCommand(address, INVALID_OBJECT_ADDRESS, tag);
    }

    if is_complete_query {
        event_trigger::EventTriggerSQLDrop(mcx, tag)?;
        event_trigger::EventTriggerDDLCommandEnd(mcx, tag)?;
    }
    Ok(())
}

// Ok(Some(address)) feeds the shared EventTriggerCollectSimpleCommand tail;
// Ok(None) = C's `commandCollected = true` arms.
#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn slow_switch<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: Node<'_>,
    pstmt: &PlannedStmt<'_>,
    source_text: &str,
    _context: ProcessUtilityContext,
    is_top_level: bool,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    qc: &mut Option<&mut QueryCompletion>,
) -> PgResult<Option<ObjectAddress>> {
    use NodeTag::*;
    match parsetree.node_tag() {
        T_CreateSchemaStmt => {
            let stmt = parsetree.as_create_schema_stmt().unwrap();
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::CreateSchemaStmt<'_>,
                    &types_nodes::parsenodes::CreateSchemaStmt<'mcx>,
                >(stmt)
            };
            let tag = CreateCommandTag(parsetree);
            let (stmt_location, stmt_len) = (pstmt.stmt_location, pstmt.stmt_len);
            // C runs this block inside CreateSchemaCommand: collect the
            // schema for event triggers ahead of the element subcommands,
            // then hand each element straight to ProcessUtility (the grammar
            // guarantees they are utility statements).
            let mut exec_elements = |nsp_oid: types_core::Oid,
                                     elts: &types_nodes::NodeList<'mcx>,
                                     schema_name: &str|
             -> PgResult<()> {
                event_trigger::EventTriggerCollectSimpleCommand(
                    ObjectAddress::set(NAMESPACE_RELATION_ID, nsp_oid),
                    INVALID_OBJECT_ADDRESS,
                    tag,
                );
                let elements =
                    parse_utilcmd::transformCreateSchemaStmtElements(mcx, elts, schema_name)?;
                for element in elements.iter() {
                    let wrapper = PlannedStmt {
                        commandType: CmdType::CMD_UTILITY,
                        canSetTag: false,
                        utilityStmt: Some(element),
                        stmt_location,
                        stmt_len,
                        ..PlannedStmt::default()
                    };
                    let mut dest = DestReceiver::DoNothing;
                    ProcessUtility(
                        mcx,
                        &wrapper,
                        source_text,
                        false,
                        PROCESS_UTILITY_SUBCOMMAND,
                        types_portal::ParamListHandle::NULL,
                        types_portal::QueryEnvHandle::NULL,
                        &mut dest,
                        None,
                    )?;
                    xact::CommandCounterIncrement()?;
                }
                Ok(())
            };
            schemacmds::CreateSchemaCommand(mcx, stmt, &mut exec_elements)?;
            Ok(None)
        }

        T_CreateStmt | T_CreateForeignTableStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let mut stmts = parse_utilcmd::transformCreateStmt(mcx, stmt_node, source_text)?;
            let mut table_rv: Option<&'mcx types_nodes::primnodes::RangeVar<'mcx>> = None;
            let mut i = 0;
            while i < stmts.len() {
                let stmt = stmts.nth(i);
                i += 1;
                match stmt.node_tag() {
                    T_CreateStmt => {
                        let cstmt = stmt
                            .as_variant::<types_nodes::rawnodes::CreateStmt>()
                            .expect("CreateStmt");
                        table_rv = cstmt.relation;
                        let relid = tablecmds::DefineRelation(
                            mcx,
                            cstmt,
                            types_rel::RELKIND_RELATION,
                            types_core::InvalidOid,
                            source_text,
                        )?;
                        event_trigger::EventTriggerCollectSimpleCommand(
                            ObjectAddress::set(types_core::RELATION_RELATION_ID, relid),
                            INVALID_OBJECT_ADDRESS,
                            CreateCommandTag(stmt),
                        );
                        xact::CommandCounterIncrement()?;
                        let toast_options = reloptions::transformRelOptions(
                            mcx,
                            None,
                            &cstmt.options,
                            Some("toast"),
                            reloptions::HEAP_RELOPT_NAMESPACES,
                            true,
                            false,
                        )?;
                        reloptions::heap_reloptions(
                            mcx,
                            types_rel::RELKIND_TOASTVALUE,
                            toast_options.as_deref(),
                            true,
                        )?;
                        catalog_toasting::NewRelationCreateToastTable(
                            mcx,
                            relid,
                            toast_options.as_deref(),
                        )?;
                    }
                    T_CreateForeignTableStmt => {
                        let cstmt = stmt
                            .as_variant::<types_nodes::rawnodes::CreateForeignTableStmt>()
                            .expect("CreateForeignTableStmt");
                        table_rv = cstmt.base.relation;
                        let relid = tablecmds::DefineRelation(
                            mcx,
                            &cstmt.base,
                            types_rel::RELKIND_FOREIGN_TABLE,
                            types_core::InvalidOid,
                            source_text,
                        )?;
                        foreigncmds::CreateForeignTable(mcx, cstmt, relid)?;
                        event_trigger::EventTriggerCollectSimpleCommand(
                            ObjectAddress::set(types_core::RELATION_RELATION_ID, relid),
                            INVALID_OBJECT_ADDRESS,
                            CreateCommandTag(stmt),
                        );
                    }
                    T_TableLikeClause => {
                        // Delayed LIKE expansion: sub-statements run before
                        // any remaining actions (C list_concat(morestmts, stmts)).
                        let tlc = stmt
                            .as_variant::<types_nodes::rawnodes::TableLikeClause>()
                            .expect("TableLikeClause");
                        let rv = table_rv.expect("LIKE expansion before CreateStmt");
                        let morestmts = parse_utilcmd::expandTableLikeClause(mcx, rv, tlc)?;
                        for (j, m) in morestmts.iter().enumerate() {
                            stmts.insert_nth(mcx, i + j, m)?;
                        }
                    }
                    T_AlterTableStmt => {
                        let atstmt = stmt
                            .as_variant::<types_nodes::parsenodes::AlterTableStmt>()
                            .expect("AlterTableStmt");
                        exec_alter_table_stmt(mcx, atstmt, stmt, source_text, is_top_level)?;
                    }
                    T_IndexStmt => exec_index_stmt(mcx, stmt, source_text, is_top_level)?,
                    T_CommentStmt => {
                        // C recurses through ProcessUtility; the inner
                        // ProcessUtilitySlow collects address = CommentObject.
                        let cstmt = stmt
                            .as_variant::<types_nodes::parsenodes::CommentStmt>()
                            .expect("CommentStmt");
                        let addr = commands_comment::CommentObject(mcx, cstmt)?;
                        event_trigger::EventTriggerCollectSimpleCommand(
                            ObjectAddress {
                                classId: addr.classId,
                                objectId: addr.objectId,
                                objectSubId: addr.objectSubId,
                            },
                            INVALID_OBJECT_ADDRESS,
                            crate::consts::CMDTAG_COMMENT,
                        );
                    }
                    // C recurses through ProcessUtility for the serial
                    // blist/alist statements; the wrapper adds nothing here.
                    T_CreateSeqStmt => {
                        let seqstmt = stmt
                            .as_variant::<types_nodes::rawnodes::CreateSeqStmt>()
                            .expect("CreateSeqStmt");
                        let mut pstate = parser_small1::make_parsestate(mcx, None);
                        {
                            let mut v: mcx::PgVec<'mcx, u8> = mcx::PgVec::new_in(mcx);
                            mcx::vec_append_bytes(&mut v, source_text.as_bytes())?;
                            pstate.p_sourcetext = Some(v.leak());
                        }
                        let seqoid = sequence::DefineSequence(mcx, Some(&pstate), seqstmt)?;
                        parser_small1::free_parsestate(pstate)?;
                        event_trigger::EventTriggerCollectSimpleCommand(
                            ObjectAddress::set(types_core::RELATION_RELATION_ID, seqoid),
                            INVALID_OBJECT_ADDRESS,
                            CreateCommandTag(stmt),
                        );
                    }
                    T_AlterSeqStmt => {
                        let altstmt = stmt
                            .as_variant::<types_nodes::AlterSeqStmt>()
                            .expect("AlterSeqStmt");
                        let seqoid = sequence::AlterSequence(mcx, altstmt)?;
                        event_trigger::EventTriggerCollectSimpleCommand(
                            ObjectAddress::set(types_core::RELATION_RELATION_ID, seqoid),
                            INVALID_OBJECT_ADDRESS,
                            CreateCommandTag(stmt),
                        );
                    }
                    T_CreateStatsStmt => {
                        // C recurses through ProcessUtility; the inner
                        // ProcessUtilitySlow collects address = CreateStatistics.
                        let address = exec_create_stats_stmt(mcx, stmt, source_text)?;
                        event_trigger::EventTriggerCollectSimpleCommand(
                            address,
                            INVALID_OBJECT_ADDRESS,
                            CreateCommandTag(stmt),
                        );
                    }
                    // unported: analysis-generated substatement kinds whose
                    // dispatch lane isn't wired yet — clean 0A000.
                    other => {
                        return Err(handler_unsupported(&format!(
                            "a substatement of this DDL command ({other:?})"
                        )))
                    }
                }
                if i < stmts.len() {
                    xact::CommandCounterIncrement()?;
                }
            }
            // The multiple commands generated here are stashed individually.
            Ok(None)
        }

        T_AlterTableStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterTableStmt>()
                .expect("AlterTableStmt");
            // Retention contract as unify_stmt_lifetime: nothing derived from
            // the statement arena escapes the utility call.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::AlterTableStmt<'_>,
                    &types_nodes::parsenodes::AlterTableStmt<'mcx>,
                >(stmt)
            };
            exec_alter_table_stmt(mcx, stmt, parsetree, source_text, is_top_level)?;
            // ALTER TABLE stashes commands internally.
            Ok(None)
        }

        T_AlterTableMoveAllStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterTableMoveAllStmt>()
                .expect("AlterTableMoveAllStmt");
            // Retention contract as unify_stmt_lifetime: nothing derived from
            // the statement arena escapes the utility call.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::AlterTableMoveAllStmt<'_>,
                    &types_nodes::parsenodes::AlterTableMoveAllStmt<'mcx>,
                >(stmt)
            };
            tablecmds::AlterTableMoveAll(mcx, stmt)?;
            // Commands are stashed in AlterTableMoveAll (per relation).
            Ok(None)
        }

        T_IndexStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            exec_index_stmt(mcx, stmt_node, source_text, is_top_level)?;
            // CREATE INDEX collects itself ahead of any ALTER TABLE subcmds.
            Ok(None)
        }

        T_CreateTrigStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::CreateTrigStmt>()
                .expect("CreateTrigStmt");
            let trigoid = trigger::CreateTrigger(mcx, stmt, source_text)?;
            Ok(Some(ObjectAddress::set(types_core::TRIGGER_RELATION_ID, trigoid)))
        }

        T_ReindexStmt => {
            // REINDEX collects itself, per-index, inside reindex_index/
            // reindex_relation/ReindexRelationConcurrently (index.c,
            // indexcmds.c: EventTriggerCollectSimpleCommand).
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::ReindexStmt>()
                .expect("ReindexStmt");
            indexcmds::ExecReindex(mcx, stmt, is_top_level)?;
            Ok(None)
        }

        T_CreateFunctionStmt => {
            let stmt = parsetree.as_create_function_stmt().unwrap();
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::CreateFunctionStmt<'_>,
                    &types_nodes::parsenodes::CreateFunctionStmt<'mcx>,
                >(stmt)
            };
            let mut pstate = parser_small1::make_parsestate(mcx, None);
            {
                let mut v: mcx::PgVec<'mcx, u8> = mcx::PgVec::new_in(mcx);
                mcx::vec_append_bytes(&mut v, source_text.as_bytes())?;
                pstate.p_sourcetext = Some(v.leak());
            }
            let address = functioncmds::CreateFunction(mcx, &mut pstate, stmt, source_text)?;
            parser_small1::free_parsestate(pstate)?;
            Ok(Some(address))
        }

        T_CreateStatsStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let address = exec_create_stats_stmt(mcx, stmt_node, source_text)?;
            Ok(Some(address))
        }

        T_AlterCollationStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterCollationStmt>()
                .expect("AlterCollationStmt");
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::AlterCollationStmt<'_>,
                    &types_nodes::parsenodes::AlterCollationStmt<'mcx>,
                >(stmt)
            };
            // C: address = AlterCollation; no address surface yet.
            collect_gap("ALTER COLLATION");
            collationcmds::AlterCollation(mcx, stmt)?;
            Ok(None)
        }

        T_AlterStatsStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::rawnodes::AlterStatsStmt>()
                .expect("AlterStatsStmt");
            collect_gap("ALTER STATISTICS");
            statscmds::AlterStatistics(mcx, stmt)?;
            Ok(None)
        }

        T_DropOwnedStmt => {
            // C: ProcessUtilitySlow (utility.c:1817) — the sql_drop fences must
            // be armed so shdepDropOwned's deletions are collected; no
            // commands stashed for DROP.
            let stmt = parsetree.as_drop_owned_stmt().unwrap();
            user::DropOwnedObjects(mcx, stmt)?;
            Ok(None)
        }

        T_AlterDefaultPrivilegesStmt => {
            // C: ExecAlterDefaultPrivilegesStmt + EventTriggerCollectAlterDefPrivs
            // (utility.c:1823); commandCollected = true.
            let stmt = parsetree.as_alter_default_privileges_stmt().unwrap();
            aclchk::ExecAlterDefaultPrivilegesStmt(mcx, stmt)?;
            event_trigger::EventTriggerCollectAlterDefPrivs(
                CreateCommandTag(parsetree),
                stmt.action.expect("AlterDefaultPrivilegesStmt.action").objtype,
            );
            Ok(None)
        }

        T_RenameStmt => {
            // C: address = ExecRenameStmt (alter.c); arms without a ported
            // address surface stay loud under active collection.
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::RenameStmt>()
                .expect("RenameStmt");
            match exec_rename_stmt_inner(mcx, stmt)? {
                Some(address) => Ok(Some(address)),
                None => {
                    collect_gap("RENAME");
                    Ok(None)
                }
            }
        }

        T_DropStmt => {
            exec_drop_stmt(mcx, parsetree, is_top_level)?;
            // No commands stashed for DROP.
            Ok(None)
        }

        T_CreateFdwStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::CreateFdwStmt>()
                .expect("CreateFdwStmt");
            let fdwoid = foreigncmds::CreateForeignDataWrapper(mcx, stmt, source_text)?;
            Ok(Some(ObjectAddress::set(
                types_core::FOREIGN_DATA_WRAPPER_RELATION_ID,
                fdwoid,
            )))
        }
        T_AlterFdwStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::AlterFdwStmt>()
                .expect("AlterFdwStmt");
            collect_gap("ALTER FOREIGN DATA WRAPPER");
            foreigncmds::AlterForeignDataWrapper(mcx, stmt, source_text)?;
            Ok(None)
        }
        T_CreateForeignServerStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::CreateForeignServerStmt>()
                .expect("CreateForeignServerStmt");
            let srvoid = foreigncmds::CreateForeignServer(mcx, stmt)?;
            Ok(Some(ObjectAddress::set(types_core::FOREIGN_SERVER_RELATION_ID, srvoid)))
        }
        T_AlterForeignServerStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::AlterForeignServerStmt>()
                .expect("AlterForeignServerStmt");
            collect_gap("ALTER SERVER");
            foreigncmds::AlterForeignServer(mcx, stmt)?;
            Ok(None)
        }
        T_CreateUserMappingStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::CreateUserMappingStmt>()
                .expect("CreateUserMappingStmt");
            // C: address = CreateUserMapping (foreigncmds.c).
            let address = foreigncmds::CreateUserMapping(mcx, stmt)?;
            Ok(Some(address))
        }
        T_AlterUserMappingStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::AlterUserMappingStmt>()
                .expect("AlterUserMappingStmt");
            collect_gap("ALTER USER MAPPING");
            foreigncmds::AlterUserMapping(mcx, stmt)?;
            Ok(None)
        }
        T_DropUserMappingStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::DropUserMappingStmt>()
                .expect("DropUserMappingStmt");
            collect_gap("DROP USER MAPPING");
            foreigncmds::RemoveUserMapping(mcx, stmt)?;
            Ok(None)
        }
        T_ImportForeignSchemaStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::ImportForeignSchemaStmt>()
                .expect("ImportForeignSchemaStmt");
            collect_gap("IMPORT FOREIGN SCHEMA");
            foreigncmds::ImportForeignSchema(mcx, stmt)?;
            Ok(None)
        }

        T_CommentStmt => {
            // C: address = CommentObject (comment.c).
            let address = exec_comment_stmt(mcx, parsetree)?;
            Ok(Some(address))
        }

        T_SecLabelStmt => {
            // C: address = ExecSecLabelStmt; the address is not collected yet.
            collect_gap("SECURITY LABEL");
            exec_seclabel_stmt(mcx, parsetree)?;
            Ok(None)
        }

        T_GrantStmt => {
            // EventTriggerCollectGrant fires inside ExecGrantStmt_oids (C too).
            let stmt = parsetree.as_grant_stmt().unwrap();
            aclchk::ExecuteGrantStmt(mcx, stmt)?;
            Ok(None)
        }

        T_CreateTableAsStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::CreateTableAsStmt>()
                .expect("CreateTableAsStmt");
            let relid = commands_createas::ExecCreateTableAs(
                mcx,
                stmt,
                source_text,
                params,
                query_env,
                qc.as_deref_mut(),
            )?;
            // C collects InvalidObjectAddress on the if-not-exists skip.
            Ok(Some(ObjectAddress::set(types_core::RELATION_RELATION_ID, relid)))
        }
        T_RefreshMatViewStmt => {
            // REFRESH CONCURRENTLY executes DDL internally; inhibit command
            // collection around it exactly as C.
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::RefreshMatViewStmt>()
                .expect("RefreshMatViewStmt");
            event_trigger::EventTriggerInhibitCommandCollection();
            let res = matview_seams::exec_refresh_mat_view::call(
                mcx,
                stmt,
                source_text,
                qc.as_deref_mut(),
            );
            event_trigger::EventTriggerUndoInhibitCommandCollection();
            let matview_oid = res?;
            Ok(Some(ObjectAddress::set(types_core::RELATION_RELATION_ID, matview_oid)))
        }
        T_CreateSeqStmt => {
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let seqstmt = stmt_node
                .as_variant::<types_nodes::rawnodes::CreateSeqStmt>()
                .expect("CreateSeqStmt");
            let mut pstate = parser_small1::make_parsestate(mcx, None);
            {
                let mut v: mcx::PgVec<'mcx, u8> = mcx::PgVec::new_in(mcx);
                mcx::vec_append_bytes(&mut v, source_text.as_bytes())?;
                pstate.p_sourcetext = Some(v.leak());
            }
            let seqoid = sequence::DefineSequence(mcx, Some(&pstate), seqstmt)?;
            parser_small1::free_parsestate(pstate)?;
            Ok(Some(ObjectAddress::set(types_core::RELATION_RELATION_ID, seqoid)))
        }
        T_AlterSeqStmt => {
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let altstmt =
                stmt_node.as_variant::<types_nodes::AlterSeqStmt>().expect("AlterSeqStmt");
            let seqoid = sequence::AlterSequence(mcx, altstmt)?;
            Ok(Some(ObjectAddress::set(types_core::RELATION_RELATION_ID, seqoid)))
        }
        T_CreateDomainStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_create_domain_stmt()
                .expect("CreateDomainStmt");
            let mut pstate = parser_small1::make_parsestate(mcx, None);
            {
                let mut v: mcx::PgVec<'mcx, u8> = mcx::PgVec::new_in(mcx);
                mcx::vec_append_bytes(&mut v, source_text.as_bytes())?;
                pstate.p_sourcetext = Some(v.leak());
            }
            let address = typecmds::DefineDomain(mcx, &mut pstate, stmt)?;
            parser_small1::free_parsestate(pstate)?;
            Ok(Some(address))
        }
        T_DefineStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::DefineStmt>()
                .expect("DefineStmt");
            match stmt.kind {
                types_nodes::parsenodes::ObjectType::OBJECT_COLLATION => {
                    let mut pstate = parser_small1::make_parsestate(mcx, None);
                    {
                        let mut v: mcx::PgVec<'mcx, u8> = mcx::PgVec::new_in(mcx);
                        mcx::vec_append_bytes(&mut v, source_text.as_bytes())?;
                        pstate.p_sourcetext = Some(v.leak());
                    }
                    // C: address = DefineCollation; the ported form returns no address.
                    collect_gap("CREATE COLLATION");
                    collationcmds::DefineCollation(mcx, &mut pstate, stmt)?;
                    parser_small1::free_parsestate(pstate)?;
                }
                types_nodes::parsenodes::ObjectType::OBJECT_OPERATOR => {
                    debug_assert!(!stmt.oldstyle);
                    // C: address = DefineOperator; the ported form returns no address.
                    collect_gap("CREATE OPERATOR");
                    operatorcmds::DefineOperator(mcx, &stmt.defnames, &stmt.definition)?;
                }
                types_nodes::parsenodes::ObjectType::OBJECT_TYPE => {
                    debug_assert!(!stmt.oldstyle);
                    let mut pstate = parser_small1::make_parsestate(mcx, None);
                    {
                        let mut v: mcx::PgVec<'mcx, u8> = mcx::PgVec::new_in(mcx);
                        mcx::vec_append_bytes(&mut v, source_text.as_bytes())?;
                        pstate.p_sourcetext = Some(v.leak());
                    }
                    // C: address = DefineType; the ported form returns no address.
                    collect_gap("CREATE TYPE");
                    typecmds::DefineType(mcx, &mut pstate, &stmt.defnames, &stmt.definition)?;
                    parser_small1::free_parsestate(pstate)?;
                }
                types_nodes::parsenodes::ObjectType::OBJECT_AGGREGATE => {
                    let mut pstate = parser_small1::make_parsestate(mcx, None);
                    {
                        let mut v: mcx::PgVec<'mcx, u8> = mcx::PgVec::new_in(mcx);
                        mcx::vec_append_bytes(&mut v, source_text.as_bytes())?;
                        pstate.p_sourcetext = Some(v.leak());
                    }
                    // C: address = DefineAggregate; the ported form returns no address.
                    collect_gap("CREATE AGGREGATE");
                    aggregatecmds::DefineAggregate(
                        mcx,
                        &mut pstate,
                        &stmt.defnames,
                        &stmt.args,
                        stmt.oldstyle,
                        &stmt.definition,
                        stmt.replace,
                    )?;
                    parser_small1::free_parsestate(pstate)?;
                }
                types_nodes::parsenodes::ObjectType::OBJECT_TSPARSER => {
                    // C: address = DefineTSParser; the ported form returns no address.
                    collect_gap("CREATE TEXT SEARCH PARSER");
                    tsearchcmds::DefineTSParser(mcx, stmt)?;
                }
                types_nodes::parsenodes::ObjectType::OBJECT_TSTEMPLATE => {
                    // C: address = DefineTSTemplate; the ported form returns no address.
                    collect_gap("CREATE TEXT SEARCH TEMPLATE");
                    tsearchcmds::DefineTSTemplate(mcx, stmt)?;
                }
                types_nodes::parsenodes::ObjectType::OBJECT_TSDICTIONARY => {
                    // C: address = DefineTSDictionary; the ported form returns no address.
                    collect_gap("CREATE TEXT SEARCH DICTIONARY");
                    tsearchcmds::DefineTSDictionary(mcx, stmt)?;
                }
                types_nodes::parsenodes::ObjectType::OBJECT_TSCONFIGURATION => {
                    // C: address = DefineTSConfiguration; the ported form returns no address.
                    collect_gap("CREATE TEXT SEARCH CONFIGURATION");
                    tsearchcmds::DefineTSConfiguration(mcx, stmt)?;
                }
                // unported: DefineStmt kinds without a define lane — 0A000.
                other => {
                    return Err(handler_unsupported(&format!(
                        "CREATE for this object type ({other:?})"
                    )))
                }
            }
            Ok(None)
        }
        T_CreateConversionStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::CreateConversionStmt>()
                .expect("CreateConversionStmt");
            let address = conversioncmds::CreateConversionCommand(mcx, stmt)?;
            Ok(Some(address))
        }
        T_CreatePLangStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::CreatePLangStmt>()
                .expect("CreatePLangStmt");
            let address = proclang::CreateProceduralLanguage(mcx, stmt)?;
            Ok(Some(address))
        }
        T_AlterTSDictionaryStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::AlterTSDictionaryStmt>()
                .expect("AlterTSDictionaryStmt");
            // C: address = AlterTSDictionary; the ported form returns no address.
            collect_gap("ALTER TEXT SEARCH DICTIONARY");
            tsearchcmds::AlterTSDictionary(mcx, stmt)?;
            Ok(None)
        }
        T_AlterTSConfigurationStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::AlterTSConfigurationStmt>()
                .expect("AlterTSConfigurationStmt");
            // C: address = AlterTSConfiguration; the ported form returns no address.
            collect_gap("ALTER TEXT SEARCH CONFIGURATION");
            tsearchcmds::AlterTSConfiguration(mcx, stmt)?;
            Ok(None)
        }
        T_CompositeTypeStmt => {
            // Retention contract as unify_stmt_lifetime.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::CompositeTypeStmt>()
                .expect("CompositeTypeStmt");
            typecmds::DefineCompositeType(
                mcx,
                stmt.typevar.expect("CompositeTypeStmt.typevar"),
                stmt.coldeflist.clone_in(mcx)?,
                source_text,
            )?;
            Ok(None)
        }
        T_CreateEnumStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node.as_create_enum_stmt().expect("CreateEnumStmt");
            let address = typecmds::DefineEnum(mcx, stmt)?;
            Ok(Some(address))
        }
        T_CreateRangeStmt => {
            // Retention contract as unify_stmt_lifetime.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node.as_create_range_stmt().expect("CreateRangeStmt");
            let mut pstate = parser_small1::make_parsestate(mcx, None);
            {
                let mut v: mcx::PgVec<'mcx, u8> = mcx::PgVec::new_in(mcx);
                mcx::vec_append_bytes(&mut v, source_text.as_bytes())?;
                pstate.p_sourcetext = Some(v.leak());
            }
            let address = typecmds::DefineRange(mcx, &mut pstate, stmt)?;
            parser_small1::free_parsestate(pstate)?;
            Ok(Some(address))
        }
        T_AlterEnumStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node.as_alter_enum_stmt().expect("AlterEnumStmt");
            let address = typecmds::AlterEnum(mcx, stmt)?;
            Ok(Some(address))
        }
        T_AlterTypeStmt => {
            // Retention contract as unify_stmt_lifetime.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node.as_alter_type_stmt().expect("AlterTypeStmt");
            let address = typecmds::AlterType(mcx, stmt)?;
            Ok(Some(address))
        }
        T_AlterDomainStmt => {
            // Retention contract as unify_stmt_lifetime.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::AlterDomainStmt>()
                .expect("AlterDomainStmt");
            typecmds::AlterDomain(mcx, stmt)?;
            Ok(None)
        }
        T_AlterObjectSchemaStmt => {
            // Retention contract as unify_stmt_lifetime.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::AlterObjectSchemaStmt>()
                .expect("AlterObjectSchemaStmt");
            match stmt.objectType {
                types_nodes::parsenodes::ObjectType::OBJECT_DOMAIN
                | types_nodes::parsenodes::ObjectType::OBJECT_TYPE => {
                    let names = stmt
                        .object
                        .expect("AlterObjectSchemaStmt.object")
                        .as_list()
                        .expect("name list");
                    typecmds::AlterTypeNamespace(
                        mcx,
                        names,
                        stmt.newschema.expect("newschema"),
                        stmt.objectType,
                    )?;
                }
                types_nodes::parsenodes::ObjectType::OBJECT_AGGREGATE
                | types_nodes::parsenodes::ObjectType::OBJECT_COLLATION
                | types_nodes::parsenodes::ObjectType::OBJECT_CONVERSION
                | types_nodes::parsenodes::ObjectType::OBJECT_FUNCTION
                | types_nodes::parsenodes::ObjectType::OBJECT_OPERATOR
                | types_nodes::parsenodes::ObjectType::OBJECT_OPCLASS
                | types_nodes::parsenodes::ObjectType::OBJECT_OPFAMILY
                | types_nodes::parsenodes::ObjectType::OBJECT_PROCEDURE
                | types_nodes::parsenodes::ObjectType::OBJECT_ROUTINE
                | types_nodes::parsenodes::ObjectType::OBJECT_STATISTIC_EXT
                | types_nodes::parsenodes::ObjectType::OBJECT_TSCONFIGURATION
                | types_nodes::parsenodes::ObjectType::OBJECT_TSDICTIONARY
                | types_nodes::parsenodes::ObjectType::OBJECT_TSPARSER
                | types_nodes::parsenodes::ObjectType::OBJECT_TSTEMPLATE => {
                    collect_gap("ALTER SET SCHEMA");
                    commands_alter::ExecAlterObjectSchemaStmt_generic(mcx, stmt)?;
                }
                // ExecAlterObjectSchemaStmt (alter.c): relations route through
                // AlterTableNamespace.
                types_nodes::parsenodes::ObjectType::OBJECT_TABLE
                | types_nodes::parsenodes::ObjectType::OBJECT_SEQUENCE
                | types_nodes::parsenodes::ObjectType::OBJECT_VIEW
                | types_nodes::parsenodes::ObjectType::OBJECT_MATVIEW
                | types_nodes::parsenodes::ObjectType::OBJECT_FOREIGN_TABLE => {
                    collect_gap("ALTER SET SCHEMA");
                    tablecmds::AlterTableNamespace(mcx, stmt)?;
                }
                // ExecAlterObjectSchemaStmt (alter.c) OBJECT_EXTENSION arm:
                // AlterExtensionNamespace moves the member objects too.
                types_nodes::parsenodes::ObjectType::OBJECT_EXTENSION => {
                    collect_gap("ALTER SET SCHEMA");
                    let name = stmt
                        .object
                        .expect("AlterObjectSchemaStmt.object")
                        .as_string()
                        .expect("extension name String")
                        .sval;
                    extension::AlterExtensionNamespace(
                        mcx,
                        name,
                        stmt.newschema.expect("newschema"),
                    )?;
                }
                // unported: ExecAlterObjectSchemaStmt object types without a
                // ported lane — 0A000.
                other => {
                    return Err(handler_unsupported(&format!(
                        "ALTER ... SET SCHEMA for this object type ({other:?})"
                    )))
                }
            }
            Ok(None)
        }
        T_RuleStmt => {
            // Retention contract as unify_stmt_lifetime.
            let stmt = parsetree
                .as_variant::<types_nodes::rawnodes::RuleStmt>()
                .expect("RuleStmt");
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::rawnodes::RuleStmt<'_>,
                    &types_nodes::rawnodes::RuleStmt<'mcx>,
                >(stmt)
            };
            let address = rewrite_define::DefineRule(mcx, stmt, source_text)?;
            Ok(Some(address))
        }
        T_ViewStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt = parsetree
                .as_variant::<types_nodes::rawnodes::ViewStmt>()
                .expect("ViewStmt");
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::rawnodes::ViewStmt<'_>,
                    &types_nodes::rawnodes::ViewStmt<'mcx>,
                >(stmt)
            };
            event_trigger::EventTriggerAlterTableStart(CreateCommandTag(parsetree));
            let view_oid = commands_view::DefineView(
                mcx,
                stmt,
                source_text,
                pstmt.stmt_location,
                pstmt.stmt_len,
            )?;
            event_trigger::EventTriggerCollectSimpleCommand(
                ObjectAddress::set(types_core::RELATION_RELATION_ID, view_oid),
                INVALID_OBJECT_ADDRESS,
                CreateCommandTag(parsetree),
            );
            event_trigger::EventTriggerAlterTableEnd();
            Ok(None)
        }
        T_CreatePolicyStmt => {
            let stmt = parsetree.as_create_policy_stmt().unwrap();
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::CreatePolicyStmt<'_>,
                    &types_nodes::parsenodes::CreatePolicyStmt<'mcx>,
                >(stmt)
            };
            // C: address = CreatePolicy (policy.c).
            let address = commands_policy::CreatePolicy(mcx, stmt)?;
            Ok(Some(address))
        }
        T_AlterPolicyStmt => {
            let stmt = parsetree.as_alter_policy_stmt().unwrap();
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::AlterPolicyStmt<'_>,
                    &types_nodes::parsenodes::AlterPolicyStmt<'mcx>,
                >(stmt)
            };
            // C: address = AlterPolicy (policy.c).
            let address = commands_policy::AlterPolicy(mcx, stmt)?;
            Ok(Some(address))
        }

        T_CreateOpClassStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::CreateOpClassStmt>()
                .expect("CreateOpClassStmt");
            // C: command is stashed in DefineOpClass (EventTriggerCollect-
            // CreateOpClass; the implicit family collects in CreateOpFamily).
            opclasscmds::DefineOpClass(mcx, stmt)?;
            Ok(None)
        }

        T_CreateOpFamilyStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::CreateOpFamilyStmt>()
                .expect("CreateOpFamilyStmt");
            // C: command is stashed in DefineOpFamily (via CreateOpFamily's
            // EventTriggerCollectSimpleCommand).
            opclasscmds::DefineOpFamily(mcx, stmt)?;
            Ok(None)
        }

        T_AlterOpFamilyStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::AlterOpFamilyStmt>()
                .expect("AlterOpFamilyStmt");
            collect_gap("ALTER OPERATOR FAMILY");
            opclasscmds::AlterOpFamily(mcx, stmt)?;
            Ok(None)
        }

        T_AlterOperatorStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::AlterOperatorStmt>()
                .expect("AlterOperatorStmt");
            collect_gap("ALTER OPERATOR");
            operatorcmds::AlterOperator(mcx, stmt)?;
            Ok(None)
        }

        T_CreateCastStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::CreateCastStmt>()
                .expect("CreateCastStmt");
            // C: address = CreateCast; the ported form's address is uncollected.
            collect_gap("CREATE CAST");
            functioncmds::CreateCast(mcx, stmt)?;
            Ok(None)
        }

        T_CreateTransformStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::CreateTransformStmt>()
                .expect("CreateTransformStmt");
            // C: address = CreateTransform; the ported form's address is uncollected.
            collect_gap("CREATE TRANSFORM");
            functioncmds::CreateTransform(mcx, stmt)?;
            Ok(None)
        }

        T_CreateAmStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::CreateAmStmt>()
                .expect("CreateAmStmt");
            // C: address = CreateAccessMethod; the ported form's address is uncollected.
            collect_gap("CREATE ACCESS METHOD");
            commands_amcmds::CreateAccessMethod(mcx, stmt)?;
            Ok(None)
        }

        T_CreateExtensionStmt => {
            // Retention contract as unify_stmt_lifetime.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::CreateExtensionStmt>()
                .expect("CreateExtensionStmt");
            // C: address = CreateExtension; the ported form returns no address.
            collect_gap("CREATE EXTENSION");
            extension::CreateExtension(mcx, stmt)?;
            Ok(None)
        }
        T_AlterExtensionStmt => {
            // Retention contract as unify_stmt_lifetime.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::AlterExtensionStmt>()
                .expect("AlterExtensionStmt");
            collect_gap("ALTER EXTENSION");
            extension::ExecAlterExtensionStmt(mcx, stmt)?;
            Ok(None)
        }
        T_AlterExtensionContentsStmt => {
            // Retention contract as unify_stmt_lifetime.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::AlterExtensionContentsStmt>()
                .expect("AlterExtensionContentsStmt");
            collect_gap("ALTER EXTENSION ... ADD/DROP");
            extension::ExecAlterExtensionContentsStmt(mcx, stmt)?;
            Ok(None)
        }

        T_AlterFunctionStmt => {
            // Retention contract as unify_stmt_lifetime.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::AlterFunctionStmt>()
                .expect("AlterFunctionStmt");
            let address = functioncmds::AlterFunction(mcx, stmt, source_text)?;
            Ok(Some(address))
        }
        T_AlterOwnerStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::AlterOwnerStmt>()
                .expect("AlterOwnerStmt");
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::AlterOwnerStmt<'_>,
                    &types_nodes::parsenodes::AlterOwnerStmt<'mcx>,
                >(stmt)
            };
            match stmt.objectType {
                types_nodes::parsenodes::ObjectType::OBJECT_DOMAIN
                | types_nodes::parsenodes::ObjectType::OBJECT_TYPE => {
                    let names = stmt
                        .object
                        .expect("AlterOwnerStmt.object")
                        .as_list()
                        .expect("name list");
                    let newowner = aclchk::get_rolespec_oid(
                        stmt.newowner.expect("AlterOwnerStmt.newowner"),
                        false,
                    )?;
                    // C: address = ExecAlterOwnerStmt; no address surface yet.
                    collect_gap("ALTER OWNER");
                    typecmds::AlterTypeOwner(mcx, names, newowner, stmt.objectType)?;
                    Ok(None)
                }
                types_nodes::parsenodes::ObjectType::OBJECT_PUBLICATION
                | types_nodes::parsenodes::ObjectType::OBJECT_SUBSCRIPTION
                | types_nodes::parsenodes::ObjectType::OBJECT_DATABASE
                | types_nodes::parsenodes::ObjectType::OBJECT_AGGREGATE
                | types_nodes::parsenodes::ObjectType::OBJECT_COLLATION
                | types_nodes::parsenodes::ObjectType::OBJECT_CONVERSION
                | types_nodes::parsenodes::ObjectType::OBJECT_FUNCTION
                | types_nodes::parsenodes::ObjectType::OBJECT_LANGUAGE
                | types_nodes::parsenodes::ObjectType::OBJECT_LARGEOBJECT
                | types_nodes::parsenodes::ObjectType::OBJECT_OPERATOR
                | types_nodes::parsenodes::ObjectType::OBJECT_OPCLASS
                | types_nodes::parsenodes::ObjectType::OBJECT_OPFAMILY
                | types_nodes::parsenodes::ObjectType::OBJECT_PROCEDURE
                | types_nodes::parsenodes::ObjectType::OBJECT_ROUTINE
                | types_nodes::parsenodes::ObjectType::OBJECT_STATISTIC_EXT
                | types_nodes::parsenodes::ObjectType::OBJECT_TABLESPACE
                | types_nodes::parsenodes::ObjectType::OBJECT_TSDICTIONARY
                | types_nodes::parsenodes::ObjectType::OBJECT_TSCONFIGURATION
                | types_nodes::parsenodes::ObjectType::OBJECT_FDW
                | types_nodes::parsenodes::ObjectType::OBJECT_FOREIGN_SERVER
                | types_nodes::parsenodes::ObjectType::OBJECT_SCHEMA => {
                    // C: address = ExecAlterOwnerStmt; no address collected yet.
                    collect_gap("ALTER OWNER");
                    commands_alter::ExecAlterOwnerStmt(mcx, stmt)?;
                    Ok(None)
                }
                // unported: ExecAlterOwnerStmt object types without a ported
                // lane — 0A000.
                other => Err(handler_unsupported(&format!(
                    "ALTER ... OWNER TO for this object type ({other:?})"
                ))),
            }
        }

        // unported: utility statement tags with no ProcessUtilitySlow lane
        // yet — clean 0A000 (was a user-reachable panic).
        other => Err(handler_unsupported(&format!(
            "this utility statement ({other:?})"
        ))),
    }
}

const NAMESPACE_RELATION_ID: types_core::Oid = 2615;

fn exec_drop_stmt<'mcx>(mcx: Mcx<'mcx>, parsetree: Node<'_>, is_top_level: bool) -> PgResult<()> {
    use types_nodes::parsenodes::ObjectType::*;
    let stmt = parsetree.as_drop_stmt().unwrap();
    // Retention contract as unify_stmt_lifetime: nothing derived from
    // the statement arena escapes the utility call.
    let stmt = unsafe {
        core::mem::transmute::<
            &types_nodes::parsenodes::DropStmt<'_>,
            &types_nodes::parsenodes::DropStmt<'mcx>,
        >(stmt)
    };
    match stmt.removeType {
        OBJECT_INDEX if stmt.concurrent => {
            xact::PreventInTransactionBlock(is_top_level, "DROP INDEX CONCURRENTLY")?;
            tablecmds::RemoveRelations(mcx, stmt)?;
        }
        OBJECT_INDEX | OBJECT_TABLE | OBJECT_SEQUENCE | OBJECT_VIEW | OBJECT_MATVIEW
        | OBJECT_FOREIGN_TABLE => tablecmds::RemoveRelations(mcx, stmt)?,
        // DROP POLICY stays specialized: dropcmds' get_object_address
        // has no OBJECT_POLICY arm yet (C routes it through RemoveObjects).
        OBJECT_POLICY => commands_policy::RemovePolicyObjects(mcx, stmt)?,
        // DROP TEXT SEARCH objects stay specialized for the same reason.
        _ => commands_dropcmds::RemoveObjects(mcx, stmt)?,
    }
    Ok(())
}

fn exec_comment_stmt<'mcx>(mcx: Mcx<'mcx>, parsetree: Node<'_>) -> PgResult<ObjectAddress> {
    let stmt = parsetree.as_comment_stmt().unwrap();
    // Retention contract as unify_stmt_lifetime.
    let stmt = unsafe {
        core::mem::transmute::<
            &types_nodes::parsenodes::CommentStmt<'_>,
            &types_nodes::parsenodes::CommentStmt<'mcx>,
        >(stmt)
    };
    let addr = commands_comment::CommentObject(mcx, stmt)?;
    Ok(ObjectAddress { classId: addr.classId, objectId: addr.objectId, objectSubId: addr.objectSubId })
}

fn exec_seclabel_stmt<'mcx>(mcx: Mcx<'mcx>, parsetree: Node<'_>) -> PgResult<()> {
    let stmt = parsetree.as_sec_label_stmt().unwrap();
    // Retention contract as unify_stmt_lifetime.
    let stmt = unsafe {
        core::mem::transmute::<
            &types_nodes::parsenodes::SecLabelStmt<'_>,
            &types_nodes::parsenodes::SecLabelStmt<'mcx>,
        >(stmt)
    };
    seclabel::ExecSecLabelStmt(mcx, stmt)?;
    Ok(())
}

// ExecAlterOwnerStmt (alter.c) for the object types without event-trigger
// support.
fn exec_alter_owner_non_et<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::parsenodes::AlterOwnerStmt<'_>,
) -> PgResult<()> {
    match stmt.objectType {
        types_nodes::parsenodes::ObjectType::OBJECT_DATABASE => {
            // C: ExecAlterOwnerStmt case OBJECT_DATABASE (alter.c) ->
            // AlterDatabaseOwner (dbcommands.c). Address is dropped: databases
            // are not event-trigger-supported objects, so this path never
            // collects.
            let name = stmt
                .object
                .expect("AlterOwnerStmt.object")
                .as_string()
                .expect("database name String")
                .sval;
            let newowner =
                aclchk::get_rolespec_oid(stmt.newowner.expect("AlterOwnerStmt.newowner"), false)?;
            dbcommands::AlterDatabaseOwner(mcx, name, newowner).map(|_| ())
        }
        types_nodes::parsenodes::ObjectType::OBJECT_TABLESPACE => {
            let name = stmt
                .object
                .expect("AlterOwnerStmt.object")
                .as_string()
                .expect("tablespace name String")
                .sval;
            let newowner =
                aclchk::get_rolespec_oid(stmt.newowner.expect("AlterOwnerStmt.newowner"), false)?;
            commands_tablespace::AlterTableSpaceOwner(mcx, name, newowner)
        }
        types_nodes::parsenodes::ObjectType::OBJECT_DATABASE => {
            let name = stmt
                .object
                .expect("AlterOwnerStmt.object")
                .as_string()
                .expect("database name String")
                .sval;
            let newowner =
                aclchk::get_rolespec_oid(stmt.newowner.expect("AlterOwnerStmt.newowner"), false)?;
            dbcommands::AlterDatabaseOwner(mcx, name, newowner).map(|_| ())
        }
        types_nodes::parsenodes::ObjectType::OBJECT_EVENT_TRIGGER => {
            // C: ExecAlterOwnerStmt case OBJECT_EVENT_TRIGGER (alter.c) ->
            // AlterEventTriggerOwner (event_trigger.c). Address is dropped:
            // event triggers are not event-trigger-supported objects, so this
            // path never collects.
            let name = stmt
                .object
                .expect("AlterOwnerStmt.object")
                .as_string()
                .expect("event trigger name String")
                .sval;
            let newowner =
                aclchk::get_rolespec_oid(stmt.newowner.expect("AlterOwnerStmt.newowner"), false)?;
            event_trigger::AlterEventTriggerOwner(mcx, name, newowner).map(|_| ())
        }
        // unported: non-event-trigger owner lanes without a ported handler
        // (defensive — DATABASE/TABLESPACE/EVENT TRIGGER are the grammar's
        // non-ET forms and all three are handled above) — clean 0A000.
        other => Err(handler_unsupported(&format!(
            "ALTER ... OWNER TO for this object type ({other:?})"
        ))),
    }
}

fn exec_rename_stmt<'mcx>(mcx: Mcx<'mcx>, parsetree: Node<'_>) -> PgResult<()> {
    let stmt = parsetree
        .as_variant::<types_nodes::parsenodes::RenameStmt>()
        .expect("RenameStmt");
    exec_rename_stmt_inner(mcx, stmt).map(|_| ())
}

// C ExecRenameStmt returns the renamed object's address for the collection
// tail; arms whose ports do not surface an address yet return None (the
// T_RenameStmt dispatch arm stays loud for those under active collection).
fn exec_rename_stmt_inner<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::parsenodes::RenameStmt<'_>,
) -> PgResult<Option<ObjectAddress>> {
    match stmt.renameType {
        types_nodes::parsenodes::ObjectType::OBJECT_DATABASE => {
            dbcommands::RenameDatabase(
                mcx,
                stmt.subname.expect("RENAME DATABASE subname"),
                stmt.newname.expect("RENAME DATABASE newname"),
            )?;
        }
        types_nodes::parsenodes::ObjectType::OBJECT_TABLE => {
            tablecmds::RenameRelation(mcx, stmt)?;
        }
        types_nodes::parsenodes::ObjectType::OBJECT_COLUMN => {
            tablecmds::renameatt(mcx, stmt)?;
        }
        types_nodes::parsenodes::ObjectType::OBJECT_POLICY => {
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::RenameStmt<'_>,
                    &types_nodes::parsenodes::RenameStmt<'mcx>,
                >(stmt)
            };
            return Ok(Some(commands_policy::rename_policy(mcx, stmt)?));
        }
        types_nodes::parsenodes::ObjectType::OBJECT_TRIGGER => {
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::RenameStmt<'_>,
                    &types_nodes::parsenodes::RenameStmt<'mcx>,
                >(stmt)
            };
            trigger::renametrig(mcx, stmt)?;
        }
        types_nodes::parsenodes::ObjectType::OBJECT_TABCONSTRAINT => {
            tablecmds::RenameConstraint(mcx, stmt)?;
        }
        types_nodes::parsenodes::ObjectType::OBJECT_RULE => {
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::RenameStmt<'_>,
                    &types_nodes::parsenodes::RenameStmt<'mcx>,
                >(stmt)
            };
            let rvn = stmt.relation.expect("RENAME RULE has a relation");
            let rv = rel_vocab::RangeVar {
                catalogname: rvn.catalogname,
                schemaname: rvn.schemaname,
                relname: rvn.relname.expect("RangeVar.relname"),
                inh: rvn.inh,
                relpersistence: rvn.relpersistence,
                location: rvn.location,
            };
            rewrite_define::RenameRewriteRule(
                mcx,
                &rv,
                stmt.subname.expect("RenameStmt.subname"),
                stmt.newname.expect("RenameStmt.newname"),
            )?;
        }
        types_nodes::parsenodes::ObjectType::OBJECT_AGGREGATE
        | types_nodes::parsenodes::ObjectType::OBJECT_COLLATION
        | types_nodes::parsenodes::ObjectType::OBJECT_CONVERSION
        | types_nodes::parsenodes::ObjectType::OBJECT_EVENT_TRIGGER
        | types_nodes::parsenodes::ObjectType::OBJECT_FDW
        | types_nodes::parsenodes::ObjectType::OBJECT_FOREIGN_SERVER
        | types_nodes::parsenodes::ObjectType::OBJECT_FUNCTION
        | types_nodes::parsenodes::ObjectType::OBJECT_OPCLASS
        | types_nodes::parsenodes::ObjectType::OBJECT_OPFAMILY
        | types_nodes::parsenodes::ObjectType::OBJECT_LANGUAGE
        | types_nodes::parsenodes::ObjectType::OBJECT_PROCEDURE
        | types_nodes::parsenodes::ObjectType::OBJECT_ROUTINE
        | types_nodes::parsenodes::ObjectType::OBJECT_STATISTIC_EXT
        | types_nodes::parsenodes::ObjectType::OBJECT_TSCONFIGURATION
        | types_nodes::parsenodes::ObjectType::OBJECT_TSDICTIONARY
        | types_nodes::parsenodes::ObjectType::OBJECT_TSPARSER
        | types_nodes::parsenodes::ObjectType::OBJECT_TSTEMPLATE
        | types_nodes::parsenodes::ObjectType::OBJECT_PUBLICATION
        | types_nodes::parsenodes::ObjectType::OBJECT_SUBSCRIPTION => {
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::RenameStmt<'_>,
                    &types_nodes::parsenodes::RenameStmt<'mcx>,
                >(stmt)
            };
            return Ok(Some(commands_alter::ExecRenameStmt_generic(mcx, stmt)?));
        }
        types_nodes::parsenodes::ObjectType::OBJECT_INDEX
        | types_nodes::parsenodes::ObjectType::OBJECT_SEQUENCE
        | types_nodes::parsenodes::ObjectType::OBJECT_VIEW
        | types_nodes::parsenodes::ObjectType::OBJECT_MATVIEW
        | types_nodes::parsenodes::ObjectType::OBJECT_FOREIGN_TABLE => {
            tablecmds::RenameRelation(mcx, stmt)?;
        }
        types_nodes::parsenodes::ObjectType::OBJECT_ATTRIBUTE => {
            tablecmds::renameatt(mcx, stmt)?;
        }
        types_nodes::parsenodes::ObjectType::OBJECT_DOMAIN
        | types_nodes::parsenodes::ObjectType::OBJECT_TYPE => {
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::RenameStmt<'_>,
                    &types_nodes::parsenodes::RenameStmt<'mcx>,
                >(stmt)
            };
            typecmds::RenameType(mcx, stmt)?;
        }
        types_nodes::parsenodes::ObjectType::OBJECT_DOMCONSTRAINT => {
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::RenameStmt<'_>,
                    &types_nodes::parsenodes::RenameStmt<'mcx>,
                >(stmt)
            };
            typecmds::RenameDomainConstraint(mcx, stmt)?;
        }
        types_nodes::parsenodes::ObjectType::OBJECT_TABLESPACE => {
            commands_tablespace::RenameTableSpace(
                mcx,
                stmt.subname.expect("RenameStmt.subname"),
                stmt.newname.expect("RenameStmt.newname"),
            )?;
        }
        types_nodes::parsenodes::ObjectType::OBJECT_SCHEMA => {
            schemacmds::RenameSchema(
                mcx,
                stmt.subname.expect("RenameStmt.subname"),
                stmt.newname.expect("RenameStmt.newname"),
            )?;
        }
        types_nodes::parsenodes::ObjectType::OBJECT_ROLE => {
            let roleid = user::RenameRole(
                mcx,
                stmt.subname.expect("RenameStmt.subname"),
                stmt.newname.expect("RenameStmt.newname"),
            )?;
            return Ok(Some(ObjectAddress::set(catalog::AuthIdRelationId, roleid)));
        }
        other => panic!("unported: ExecRenameStmt {other:?}"),
    }
    Ok(None)
}

fn exec_create_stats_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt_node: Node<'mcx>,
    source_text: &str,
) -> PgResult<ObjectAddress> {
    let stmt = stmt_node
        .as_variant::<types_nodes::rawnodes::CreateStatsStmt>()
        .expect("CreateStatsStmt");
    if let Some(first) = stmt.relations.iter().next() {
        let Some(rv_node) = first.as_range_var() else {
            return Err(Box::new(
                types_error::PgError::new(
                    types_error::ERROR,
                    "CREATE STATISTICS only supports relation names in the FROM clause"
                        .to_string(),
                )
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        };
        let rv = rel_vocab::RangeVar {
            catalogname: rv_node.catalogname,
            schemaname: rv_node.schemaname,
            relname: rv_node.relname.expect("CreateStatsStmt relation without relname"),
            inh: rv_node.inh,
            relpersistence: rv_node.relpersistence,
            location: rv_node.location,
        };
        let relid = catalog_namespace::RangeVarGetRelidExtended(
            &rv,
            types_rel::ShareUpdateExclusiveLock,
            0,
            None,
        )?;
        // Cloned LIKE statistics arrive pre-transformed (C utility.c:1901).
        if !stmt.transformed {
            parse_clause::transformStatsStmt(mcx, relid, stmt_node, source_text)?;
        }
    }
    let stmt = stmt_node
        .as_variant::<types_nodes::rawnodes::CreateStatsStmt>()
        .expect("CreateStatsStmt");
    // C: address = CreateStatistics(); collected by the shared slow-path tail.
    statscmds::CreateStatistics(mcx, stmt, true)
}

fn exec_index_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt_node: Node<'mcx>,
    source_text: &str,
    is_top_level: bool,
) -> PgResult<()> {
    let stmt = stmt_node
        .as_variant::<types_nodes::rawnodes::IndexStmt>()
        .expect("IndexStmt");
    if stmt.concurrent {
        xact::PreventInTransactionBlock(is_top_level, "CREATE INDEX CONCURRENTLY")?;
    }
    let lockmode = if stmt.concurrent {
        types_rel::ShareUpdateExclusiveLock
    } else {
        types_rel::ShareLock
    };
    let rv_node = stmt.relation.expect("IndexStmt without relation");
    let rv = rel_vocab::RangeVar {
        catalogname: rv_node.catalogname,
        schemaname: rv_node.schemaname,
        relname: rv_node.relname.expect("IndexStmt relation without relname"),
        inh: rv_node.inh,
        relpersistence: rv_node.relpersistence,
        location: rv_node.location,
    };
    let mut cb = |rv2: &rel_vocab::RangeVar<'_>,
                  rel_id: types_core::Oid,
                  old_rel_id: types_core::Oid|
     -> PgResult<()> { range_var_callback_owns_relation(mcx, rv2, rel_id, old_rel_id) };
    let relid = catalog_namespace::RangeVarGetRelidExtended(&rv, lockmode, 0, Some(&mut cb))?;
    // Partitioned recursion locks every partition up front (deadlock
    // avoidance) and pre-checks partition relkinds.
    if rv.inh
        && lsyscache::get_rel_relkind(relid)? as u8 == types_rel::RELKIND_PARTITIONED_TABLE
    {
        let inheritors = pg_inherits::find_all_inheritors(mcx, relid, lockmode)?;
        for &partrelid in inheritors.iter() {
            let relkind = lsyscache::get_rel_relkind(partrelid)? as u8;
            if relkind != types_rel::RELKIND_RELATION
                && relkind != types_rel::RELKIND_MATVIEW
                && relkind != types_rel::RELKIND_PARTITIONED_TABLE
                && relkind != types_rel::RELKIND_FOREIGN_TABLE
            {
                panic!(
                    "unexpected relkind \"{}\" on partition \"{}\"",
                    relkind as char, rv.relname
                );
            }
            if relkind == types_rel::RELKIND_FOREIGN_TABLE && (stmt.unique || stmt.primary) {
                return Err(Box::new(
                    types_error::PgError::new(
                        types_error::ERROR,
                        format!(
                            "cannot create unique index on partitioned table \"{}\"",
                            rv.relname
                        ),
                    )
                    .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                    .with_detail(format!(
                        "Table \"{}\" contains partitions that are foreign tables.",
                        rv.relname
                    )),
                ));
            }
        }
    }
    let is_alter_table = stmt.transformed;
    parse_clause::transformIndexStmt(mcx, relid, stmt_node, source_text)?;
    // Re-acquire: transformIndexStmt mutated the stmt node in place.
    let stmt = stmt_node
        .as_variant::<types_nodes::rawnodes::IndexStmt>()
        .expect("IndexStmt");
    let tag = CreateCommandTag(stmt_node);
    event_trigger::EventTriggerAlterTableStart(tag);
    let index_relid = indexcmds::DefineIndex(
        mcx,
        relid,
        stmt,
        types_core::InvalidOid,
        types_core::InvalidOid,
        types_core::InvalidOid,
        is_alter_table,
        true,
        true,
        false,
        false,
    )?;
    // Stash CREATE INDEX itself first; any ALTER TABLE-stashed commands must
    // appear after it.
    event_trigger::EventTriggerCollectSimpleCommand(
        ObjectAddress::set(types_core::RELATION_RELATION_ID, index_relid),
        INVALID_OBJECT_ADDRESS,
        tag,
    );
    event_trigger::EventTriggerAlterTableEnd();
    // idxcomment is applied inside DefineIndex (indexcmds.c:1288).
    Ok(())
}

fn exec_alter_table_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::parsenodes::AlterTableStmt<'mcx>,
    parsetree: Node<'_>,
    source_text: &str,
    is_top_level: bool,
) -> PgResult<()> {
    for cnode in stmt.cmds.iter() {
        let cmd = cnode
            .as_variant::<types_nodes::parsenodes::AlterTableCmd>()
            .expect("AlterTableCmd");
        if cmd.subtype == types_nodes::parsenodes::AlterTableType::AT_DetachPartition
            && cmd
                .def
                .and_then(|d| d.as_variant::<types_nodes::rawnodes::PartitionCmd>())
                .is_some_and(|p| p.concurrent)
        {
            xact::PreventInTransactionBlock(
                is_top_level,
                "ALTER TABLE ... DETACH CONCURRENTLY",
            )?;
        }
    }
    let lockmode = tablecmds::AlterTableGetLockLevel(&stmt.cmds);
    let relid = tablecmds::AlterTableLookupRelation(mcx, stmt, lockmode)?;
    if relid != types_core::InvalidOid {
        let tag = CreateCommandTag(parsetree);
        event_trigger::EventTriggerAlterTableStart(tag);
        event_trigger::EventTriggerAlterTableRelid(relid);
        let res = tablecmds::AlterTable(mcx, relid, lockmode, stmt, source_text, tag);
        event_trigger::EventTriggerAlterTableEnd();
        res?;
    } else {
        elog_seams::ereport_msg::call(
            types_error::NOTICE,
            format!(
                "relation \"{}\" does not exist, skipping",
                stmt.relation.and_then(|r| r.relname).unwrap_or("")
            ),
            None,
        )?;
    }
    Ok(())
}

// RangeVarCallbackOwnsRelation (tablecmds.c).
fn range_var_callback_owns_relation(
    _mcx: Mcx<'_>,
    rv: &rel_vocab::RangeVar<'_>,
    rel_id: types_core::Oid,
    _old_rel_id: types_core::Oid,
) -> PgResult<()> {
    if rel_id == types_core::InvalidOid {
        return Ok(());
    }
    if !aclchk::object_ownercheck(
        types_core::RELATION_RELATION_ID,
        rel_id,
        miscinit::GetUserId(),
    )? {
        let relkind = lsyscache::get_rel_relkind(rel_id)? as u8;
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            get_relkind_objtype(relkind),
            rv.relname,
        )?;
    }
    let relnamespace = lsyscache::get_rel_namespace(rel_id)?;
    let is_system =
        catalog::IsCatalogRelationOid(rel_id) || catalog::IsToastNamespace(relnamespace);
    if is_system && !init_small::globals::allowSystemTableMods() {
        return Err(Box::new(
            types_error::PgError::new(
                types_error::ERROR,
                format!("permission denied: \"{}\" is a system catalog", rv.relname),
            )
            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

// get_relkind_objtype (objectaddress.c)
fn get_relkind_objtype(relkind: u8) -> types_nodes::parsenodes::ObjectType {
    use types_nodes::parsenodes::ObjectType::*;
    match relkind {
        types_rel::RELKIND_RELATION | types_rel::RELKIND_PARTITIONED_TABLE => OBJECT_TABLE,
        types_rel::RELKIND_INDEX | types_rel::RELKIND_PARTITIONED_INDEX => OBJECT_INDEX,
        types_rel::RELKIND_SEQUENCE => OBJECT_SEQUENCE,
        types_rel::RELKIND_VIEW => OBJECT_VIEW,
        types_rel::RELKIND_MATVIEW => OBJECT_MATVIEW,
        types_rel::RELKIND_FOREIGN_TABLE => OBJECT_FOREIGN_TABLE,
        _ => OBJECT_TABLE,
    }
}
