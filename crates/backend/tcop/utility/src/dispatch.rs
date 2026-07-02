use tcop_dest::DestReceiver;
use types_error::PgResult;
use types_nodes::node_tree::Node;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::TransactionStmtKind::*;
use types_nodes::plannodes::PlannedStmt;
use types_nodes::NodeTag;
use types_portal::{ParamListHandle, QueryCompletion, QueryEnvHandle};
use utility_seams::{
    ProcessUtilityContext, PROCESS_UTILITY_QUERY_NONATOMIC, PROCESS_UTILITY_TOPLEVEL,
};

use crate::classify::{
    CheckRestrictedOperation, ClassifyUtilityCommandAsReadOnly, PreventCommandDuringRecovery,
};
use crate::commandtag::CreateCommandTag;
use crate::consts::{
    CMDTAG_ROLLBACK, COMMAND_IS_STRICTLY_READ_ONLY, COMMAND_OK_IN_PARALLEL_MODE,
    COMMAND_OK_IN_READ_ONLY_TXN, COMMAND_OK_IN_RECOVERY,
};
use crate::handler_gap;

#[inline]
fn set_query_completion(qc: &mut Option<&mut QueryCompletion>, tag: types_core::CommandTag) {
    if let Some(qc) = qc.as_mut() {
        qc.commandTag = tag;
        qc.nprocessed = 0;
    }
}

// C's hookable entry; no plugin surface exists, so this IS standard_ProcessUtility.
#[allow(clippy::too_many_arguments)]
pub fn ProcessUtility<'p, 'a, 's, 'd, 'q, 'mcx>(
    pstmt: &'p PlannedStmt<'a>,
    source_text: &'s str,
    read_only_tree: bool,
    context: ProcessUtilityContext,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    dest: &'d mut DestReceiver<'mcx>,
    qc: Option<&'q mut QueryCompletion>,
) -> PgResult<()> {
    debug_assert!(pstmt.commandType == CmdType::CMD_UTILITY);
    debug_assert!(qc
        .as_ref()
        .is_none_or(|qc| qc.commandTag == types_portal::CMDTAG_UNKNOWN));
    standard_ProcessUtility(
        pstmt,
        source_text,
        read_only_tree,
        context,
        params,
        query_env,
        dest,
        qc,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn standard_ProcessUtility<'p, 'a, 's, 'd, 'q, 'mcx>(
    pstmt: &'p PlannedStmt<'a>,
    _source_text: &'s str,
    read_only_tree: bool,
    context: ProcessUtilityContext,
    _params: ParamListHandle,
    _query_env: QueryEnvHandle,
    dest: &'d mut DestReceiver<'mcx>,
    qc: Option<&'q mut QueryCompletion>,
) -> PgResult<()> {
    let is_top_level = context == PROCESS_UTILITY_TOPLEVEL;
    let _is_atomic_context = !(context == PROCESS_UTILITY_TOPLEVEL
        || context == PROCESS_UTILITY_QUERY_NONATOMIC)
        || xact::IsTransactionBlock();

    // C: check_stack_depth() — recursion guard unported repo-wide (stack lane).

    if read_only_tree {
        // C: pstmt = copyObject(pstmt). Reachable only from plancache-held
        // trees; node deep-copy lands with the plancache lane.
        panic!(
            "standard_ProcessUtility (utility.c:613): readOnlyTree copyObject \
             needs node clone_in (plancache lane)"
        );
    }

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

    // C: pstate = make_parsestate(NULL) here. Every arm that consumes it is
    // still loud, so its construction rides the parser/analyze lane.

    let mut qc = qc;
    dispatch_switch(parsetree, is_top_level, dest, &mut qc)?;

    xact::CommandCounterIncrement()?;
    Ok(())
}

fn dispatch_switch(
    parsetree: Node<'_>,
    is_top_level: bool,
    dest: &mut DestReceiver<'_>,
    qc: &mut Option<&mut QueryCompletion>,
) -> PgResult<()> {
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

        T_DeclareCursorStmt => handler_gap("PerformCursorOpen (portalcmds lane)"),
        T_ClosePortalStmt => {
            CheckRestrictedOperation("CLOSE")?;
            handler_gap("PerformPortalClose (portalcmds lane)")
        }
        T_FetchStmt => handler_gap("PerformPortalFetch (portalcmds lane)"),

        T_DoStmt => handler_gap("ExecuteDoStmt (functioncmds lane)"),

        T_CreateTableSpaceStmt => {
            xact::PreventInTransactionBlock(is_top_level, "CREATE TABLESPACE")?;
            handler_gap("CreateTableSpace (tablespace lane)")
        }
        T_DropTableSpaceStmt => {
            xact::PreventInTransactionBlock(is_top_level, "DROP TABLESPACE")?;
            handler_gap("DropTableSpace (tablespace lane)")
        }
        T_AlterTableSpaceOptionsStmt => handler_gap("AlterTableSpaceOptions (tablespace lane)"),

        T_TruncateStmt => handler_gap("ExecuteTruncate (tablecmds lane)"),
        T_CopyStmt => handler_gap("DoCopy (copy lane)"),

        T_PrepareStmt => {
            CheckRestrictedOperation("PREPARE")?;
            handler_gap("PrepareQuery (prepare lane)")
        }
        T_ExecuteStmt => handler_gap("ExecuteQuery (prepare lane)"),
        T_DeallocateStmt => {
            CheckRestrictedOperation("DEALLOCATE")?;
            handler_gap("DeallocateQuery (prepare lane)")
        }

        T_GrantRoleStmt => handler_gap("GrantRole (user lane)"),

        T_CreatedbStmt => {
            xact::PreventInTransactionBlock(is_top_level, "CREATE DATABASE")?;
            handler_gap("createdb (dbcommands lane)")
        }
        T_AlterDatabaseStmt => handler_gap("AlterDatabase (dbcommands lane)"),
        T_AlterDatabaseRefreshCollStmt => {
            handler_gap("AlterDatabaseRefreshColl (dbcommands lane)")
        }
        T_AlterDatabaseSetStmt => handler_gap("AlterDatabaseSet (dbcommands lane)"),
        T_DropdbStmt => {
            xact::PreventInTransactionBlock(is_top_level, "DROP DATABASE")?;
            handler_gap("dropdb (dbcommands lane)")
        }

        T_NotifyStmt => handler_gap("Async_Notify (async lane)"),
        T_ListenStmt => {
            CheckRestrictedOperation("LISTEN")?;
            handler_gap("Async_Listen (async lane)")
        }
        T_UnlistenStmt => {
            CheckRestrictedOperation("UNLISTEN")?;
            handler_gap("Async_Unlisten (async lane)")
        }

        T_LoadStmt => handler_gap("load_file (dfmgr lane)"),
        T_CallStmt => handler_gap("ExecuteCallStmt (functioncmds lane)"),
        T_ClusterStmt => handler_gap("cluster (cluster lane)"),
        T_VacuumStmt => handler_gap("ExecVacuum (vacuum lane)"),
        T_ExplainStmt => handler_gap("ExplainQuery (explain lane)"),
        T_AlterSystemStmt => {
            xact::PreventInTransactionBlock(is_top_level, "ALTER SYSTEM")?;
            handler_gap("AlterSystemSetConfigFile (guc lane)")
        }
        T_VariableSetStmt => {
            let stmt = parsetree.as_variable_set_stmt().unwrap();
            guc_funcs::ExecSetVariableStmt(stmt, is_top_level)?;
        }
        T_VariableShowStmt => {
            let n = parsetree.as_variable_show_stmt().unwrap();
            guc_funcs::GetPGVariable(n.name.unwrap_or(""), dest)?;
        }
        T_DiscardStmt => {
            CheckRestrictedOperation("DISCARD")?;
            handler_gap("DiscardCommand (discard lane)")
        }

        T_CreateEventTrigStmt => handler_gap("CreateEventTrigger (event_trigger lane)"),
        T_AlterEventTrigStmt => handler_gap("AlterEventTrigger (event_trigger lane)"),

        T_CreateRoleStmt => handler_gap("CreateRole (user lane)"),
        T_AlterRoleStmt => handler_gap("AlterRole (user lane)"),
        T_AlterRoleSetStmt => handler_gap("AlterRoleSet (user lane)"),
        T_DropRoleStmt => handler_gap("DropRole (user lane)"),
        T_ReassignOwnedStmt => handler_gap("ReassignOwnedObjects (user lane)"),

        T_LockStmt => {
            xact::RequireTransactionBlock(is_top_level, "LOCK TABLE")?;
            handler_gap("LockTableCommand (lockcmds lane)")
        }
        T_ConstraintsSetStmt => {
            xact::WarnNoTransactionBlock(is_top_level, "SET CONSTRAINTS")?;
            handler_gap("AfterTriggerSetState (trigger lane)")
        }
        T_CheckPointStmt => handler_gap("RequestCheckpoint (checkpointer lane)"),

        // Everything else — the GRANT/DROP/RENAME/ALTER.../COMMENT/SECURITY
        // LABEL fast paths and the event-trigger-fenced DDL fan-out.
        _ => handler_gap("ProcessUtilitySlow DDL fan-out (utility slow lane)"),
    }
    Ok(())
}
