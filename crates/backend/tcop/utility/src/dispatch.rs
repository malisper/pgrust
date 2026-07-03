use mcx::Mcx;
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

// pg_authid.dat oid 4544.
const ROLE_PG_CHECKPOINT: ::types_core::Oid = 4544;

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
    debug_assert!(pstmt.commandType == CmdType::CMD_UTILITY);
    debug_assert!(qc
        .as_ref()
        .is_none_or(|qc| qc.commandTag == types_portal::CMDTAG_UNKNOWN));
    standard_ProcessUtility(
        mcx,
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

    // C: pstate = make_parsestate(NULL); the two consumers a live arm needs
    // (p_sourcetext, p_queryEnv) are threaded as parameters instead.

    let mut qc = qc;
    dispatch_switch(
        mcx,
        parsetree,
        pstmt,
        source_text,
        is_top_level,
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

#[allow(clippy::too_many_arguments)]
fn dispatch_switch<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: Node<'_>,
    pstmt: &PlannedStmt<'_>,
    source_text: &str,
    is_top_level: bool,
    params: ParamListHandle,
    query_env: QueryEnvHandle,
    dest: &mut DestReceiver<'mcx>,
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
            let stmt = parsetree.as_copy_stmt().unwrap();
            let processed = copy_cmd::DoCopy(mcx, stmt)?;
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
            prepare::ExecuteQuery(mcx, stmt, source_text, params, dest, qc.as_deref_mut())?;
        }
        T_DeallocateStmt => {
            CheckRestrictedOperation("DEALLOCATE")?;
            prepare::DeallocateQuery(parsetree.as_deallocate_stmt().unwrap())?;
        }

        T_GrantStmt => {
            let stmt = parsetree.as_grant_stmt().unwrap();
            aclchk::ExecuteGrantStmt(mcx, stmt)?;
        }
        T_GrantRoleStmt => handler_gap("GrantRole (user lane)"),

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
        T_AlterDatabaseStmt => handler_gap("AlterDatabase (dbcommands lane)"),
        T_AlterDatabaseRefreshCollStmt => {
            handler_gap("AlterDatabaseRefreshColl (dbcommands lane)")
        }
        T_AlterDatabaseSetStmt => handler_gap("AlterDatabaseSet (dbcommands lane)"),
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

        // load_file: no dynamic loader exists; every linked library is
        // "already loaded", which C treats as silent success. A filename that
        // C would dlopen diverges (notes/divergences).
        T_LoadStmt => {
            let _ = parsetree.as_load_stmt().expect("LoadStmt");
        }
        T_CallStmt => handler_gap("ExecuteCallStmt (functioncmds lane)"),
        T_ClusterStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::ClusterStmt>()
                .expect("ClusterStmt");
            commands_cluster::cluster(mcx, stmt, is_top_level)?;
        }
        T_ReindexStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::ReindexStmt>()
                .expect("ReindexStmt");
            indexcmds::ExecReindex(mcx, stmt, is_top_level)?;
        }
        T_VacuumStmt => {
            // ExecVacuum's VACUUM half lives in commands_vacuum, the ANALYZE
            // half in commands_analyze (each panics on the other's lane).
            let stmt = parsetree.as_vacuum_stmt().unwrap();
            if stmt.is_vacuumcmd {
                commands_vacuum::ExecVacuum(mcx, stmt, is_top_level)?;
            } else {
                commands_analyze::ExecVacuum(mcx, stmt, is_top_level)?;
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
            handler_gap("AlterSystemSetConfigFile (guc lane)")
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

        T_CreateEventTrigStmt => handler_gap("CreateEventTrigger (event_trigger lane)"),
        T_AlterEventTrigStmt => handler_gap("AlterEventTrigger (event_trigger lane)"),

        T_CreateRoleStmt => handler_gap("CreateRole (user lane)"),
        T_AlterRoleStmt => handler_gap("AlterRole (user lane)"),
        T_AlterRoleSetStmt => handler_gap("AlterRoleSet (user lane)"),
        T_DropRoleStmt => handler_gap("DropRole (user lane)"),
        T_ReassignOwnedStmt => handler_gap("ReassignOwnedObjects (user lane)"),

        T_LockStmt => {
            xact::RequireTransactionBlock(is_top_level, "LOCK TABLE")?;
            let stmt = parsetree.as_lock_stmt().unwrap();
            lockcmds::LockTableCommand(mcx, stmt)?;
        }
        T_ConstraintsSetStmt => {
            xact::WarnNoTransactionBlock(is_top_level, "SET CONSTRAINTS")?;
            handler_gap("AfterTriggerSetState (trigger lane)")
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
                OBJECT_RULE => dropcmds::RemoveObjects(mcx, stmt)?,
                _ => handler_gap("RemoveObjects (dropcmds lane)"),
            }
        }

        T_CommentStmt => {
            let stmt = parsetree.as_comment_stmt().unwrap();
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::CommentStmt<'_>,
                    &types_nodes::parsenodes::CommentStmt<'mcx>,
                >(stmt)
            };
            commands_comment::CommentObject(mcx, stmt)?;
        }

        T_CreateSchemaStmt => {
            let stmt = parsetree.as_create_schema_stmt().unwrap();
            // Retention contract as unify_stmt_lifetime.
            let stmt = unsafe {
                core::mem::transmute::<
                    &types_nodes::parsenodes::CreateSchemaStmt<'_>,
                    &types_nodes::parsenodes::CreateSchemaStmt<'mcx>,
                >(stmt)
            };
            schemacmds::CreateSchemaCommand(mcx, stmt)?;
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
            functioncmds::CreateFunction(mcx, stmt)?;
        }

        T_CreateStatsStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
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
                catalog_namespace::RangeVarGetRelidExtended(
                    &rv,
                    types_rel::ShareUpdateExclusiveLock,
                    0,
                    None,
                )?;
            }
            // transformStatsStmt is a no-op for plain column references; the
            // expression lane panics inside CreateStatistics.
            statscmds::CreateStatistics(mcx, stmt)?;
        }

        T_IndexStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            exec_index_stmt(mcx, stmt_node, source_text, is_top_level)?;
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
            exec_alter_table_stmt(mcx, stmt, source_text)?;
        }

        T_RenameStmt => {
            let stmt = parsetree
                .as_variant::<types_nodes::parsenodes::RenameStmt>()
                .expect("RenameStmt");
            match stmt.renameType {
                types_nodes::parsenodes::ObjectType::OBJECT_TABLE => {
                    tablecmds::RenameRelation(mcx, stmt)?;
                }
                types_nodes::parsenodes::ObjectType::OBJECT_COLUMN => {
                    tablecmds::renameatt(mcx, stmt)?;
                }
                other => panic!("unported: ExecRenameStmt {other:?}"),
            }
        }

        // Everything else — the GRANT/DROP/RENAME/ALTER.../COMMENT/SECURITY
        // LABEL fast paths and the event-trigger-fenced DDL fan-out.
        T_CreateStmt => {
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
                        xact::CommandCounterIncrement()?;
                        // toast_options: stmt.options is nil (gated in
                        // DefineRelation), so transformRelOptions yields 0.
                        catalog_toasting::NewRelationCreateToastTable(mcx, relid)?;
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
                        exec_alter_table_stmt(mcx, atstmt, source_text)?;
                    }
                    T_IndexStmt => exec_index_stmt(mcx, stmt, source_text, is_top_level)?,
                    T_CommentStmt => {
                        let cstmt = stmt
                            .as_variant::<types_nodes::parsenodes::CommentStmt>()
                            .expect("CommentStmt");
                        commands_comment::CommentObject(mcx, cstmt)?;
                    }
                    // C recurses through ProcessUtility for the serial
                    // blist/alist statements; the wrapper adds nothing here.
                    T_CreateSeqStmt => {
                        let seqstmt = stmt
                            .as_variant::<types_nodes::rawnodes::CreateSeqStmt>()
                            .expect("CreateSeqStmt");
                        sequence::DefineSequence(mcx, seqstmt)?;
                    }
                    T_AlterSeqStmt => {
                        let altstmt = stmt
                            .as_variant::<types_nodes::AlterSeqStmt>()
                            .expect("AlterSeqStmt");
                        sequence::AlterSequence(mcx, altstmt)?;
                    }
                    _ => handler_gap("ProcessUtilitySlow side statements (blist/alist)"),
                }
                if i < stmts.len() {
                    xact::CommandCounterIncrement()?;
                }
            }
        }
        T_DefineStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::DefineStmt>()
                .expect("DefineStmt");
            match stmt.kind {
                types_nodes::parsenodes::ObjectType::OBJECT_OPERATOR => {
                    debug_assert!(!stmt.oldstyle);
                    operatorcmds::DefineOperator(mcx, &stmt.defnames, &stmt.definition)?;
                }
                other => handler_gap(&format!("DefineStmt kind {other:?} (define lanes)")),
            }
        }

        T_CreateOpClassStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::CreateOpClassStmt>()
                .expect("CreateOpClassStmt");
            opclasscmds::DefineOpClass(mcx, stmt)?;
        }

        T_CreateOpFamilyStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::CreateOpFamilyStmt>()
                .expect("CreateOpFamilyStmt");
            opclasscmds::DefineOpFamily(mcx, stmt)?;
        }

        T_AlterOpFamilyStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::AlterOpFamilyStmt>()
                .expect("AlterOpFamilyStmt");
            opclasscmds::AlterOpFamily(mcx, stmt)?;
        }

        T_AlterOperatorStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::parsenodes::AlterOperatorStmt>()
                .expect("AlterOperatorStmt");
            operatorcmds::AlterOperator(mcx, stmt)?;
        }

        T_CreateTableAsStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::CreateTableAsStmt>()
                .expect("CreateTableAsStmt");
            commands_createas::ExecCreateTableAs(
                mcx,
                stmt,
                source_text,
                params,
                query_env,
                qc.as_deref_mut(),
            )?;
        }
        T_RefreshMatViewStmt => {
            // C wraps this in EventTriggerInhibitCommandCollection; no event
            // trigger surface exists.
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node
                .as_variant::<types_nodes::rawnodes::RefreshMatViewStmt>()
                .expect("RefreshMatViewStmt");
            commands_matview::ExecRefreshMatView(mcx, stmt, source_text, qc.as_deref_mut())?;
        }
        T_CreateSeqStmt => {
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let seqstmt = stmt_node
                .as_variant::<types_nodes::rawnodes::CreateSeqStmt>()
                .expect("CreateSeqStmt");
            sequence::DefineSequence(mcx, seqstmt)?;
        }
        T_AlterSeqStmt => {
            let stmt_node =
                unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let altstmt =
                stmt_node.as_variant::<types_nodes::AlterSeqStmt>().expect("AlterSeqStmt");
            sequence::AlterSequence(mcx, altstmt)?;
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
            typecmds::DefineDomain(mcx, &mut pstate, stmt)?;
            parser_small1::free_parsestate(pstate)?;
        }
        T_CreateEnumStmt => {
            // Retention contract as unify_stmt_lifetime: the statement arena
            // outlives the utility call; nothing derived escapes it.
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node.as_create_enum_stmt().expect("CreateEnumStmt");
            typecmds::DefineEnum(mcx, stmt)?;
        }
        T_AlterEnumStmt => {
            let stmt_node = unsafe { core::mem::transmute::<Node<'_>, Node<'mcx>>(parsetree) };
            let stmt = stmt_node.as_alter_enum_stmt().expect("AlterEnumStmt");
            typecmds::AlterEnum(mcx, stmt)?;
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
            rewrite_define::DefineRule(mcx, stmt, source_text)?;
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
            commands_view::DefineView(
                mcx,
                stmt,
                source_text,
                pstmt.stmt_location,
                pstmt.stmt_len,
            )?;
        }
        _ => handler_gap("ProcessUtilitySlow DDL fan-out (utility slow lane)"),
    }
    Ok(())
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
    if rv.inh
        && lsyscache::get_rel_relkind(relid)? as u8 == types_rel::RELKIND_PARTITIONED_TABLE
    {
        handler_gap("CREATE INDEX partitioned-table recursion");
    }
    let is_alter_table = stmt.transformed;
    parse_clause::transformIndexStmt(mcx, relid, stmt_node, source_text)?;
    // Re-acquire: transformIndexStmt mutated the stmt node in place.
    let stmt = stmt_node
        .as_variant::<types_nodes::rawnodes::IndexStmt>()
        .expect("IndexStmt");
    let index_relid = indexcmds::DefineIndex(
        mcx,
        relid,
        stmt,
        types_core::InvalidOid,
        is_alter_table,
        true,
        true,
        false,
        false,
    )?;
    if let Some(comment) = stmt.idxcomment {
        commands_comment::CreateComments(
            mcx,
            index_relid,
            types_core::RELATION_RELATION_ID,
            0,
            Some(comment),
        )?;
    }
    Ok(())
}

fn exec_alter_table_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::parsenodes::AlterTableStmt<'mcx>,
    source_text: &str,
) -> PgResult<()> {
    // DETACH CONCURRENTLY transaction-block fence: unported subtypes
    // are loud inside AlterTableGetLockLevel.
    let lockmode = tablecmds::AlterTableGetLockLevel(&stmt.cmds);
    let relid = tablecmds::AlterTableLookupRelation(mcx, stmt, lockmode)?;
    if relid != types_core::InvalidOid {
        // Event triggers absent by design (EventTriggerAlterTable*).
        tablecmds::AlterTable(mcx, relid, lockmode, stmt, source_text)?;
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

// RangeVarCallbackOwnsRelation (tablecmds.c): object_ownercheck superuser
// fastpath (role-ACL walk loud, drop.rs precedent) + IsSystemClass guard.
fn range_var_callback_owns_relation(
    _mcx: Mcx<'_>,
    rv: &rel_vocab::RangeVar<'_>,
    rel_id: types_core::Oid,
    _old_rel_id: types_core::Oid,
) -> PgResult<()> {
    if rel_id == types_core::InvalidOid {
        return Ok(());
    }
    if !superuser::superuser_arg(miscinit::GetUserId())? {
        handler_gap("RangeVarCallbackOwnsRelation object_ownercheck for non-superusers");
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
