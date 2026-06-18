//! `ProcessUtilitySlow` (utility.c:1085-1581, PostgreSQL 18.3) — the
//! event-trigger-fenced half of the utility dispatch.
//!
//! The "Slow" variant only ever receives statements supported by the event
//! triggers facility; the trigger support calls are therefore always performed
//! when the context allows (`isCompleteQuery`). This ports the
//! `switch (nodeTag(parsetree))` 1:1 over the owned [`Node`] tree, with the same
//! `needCleanup` / `commandCollected` / `address` / `secondaryObject` tracking,
//! the same `CommandCounterIncrement` placement, and the same event-trigger
//! fence ordering. The C `PG_TRY` / `PG_FINALLY` that guarantees
//! `EventTriggerEndCompleteQuery` runs becomes an explicit
//! run-body-then-finally over the `?` error path here (the `?` is the longjmp).
//!
//! Every command *body* lives in another subsystem and is reached through a thin
//! forwarding seam in [`backend_tcop_utility_out_seams`] (`rt`). The reachable
//! CREATE TABLE spine — `transform_create_stmt` → `define_relation` →
//! `CommandCounterIncrement` → `create_toast_for_relation` — is installed by
//! backend-parser-parse-utilcmd and backend-commands-tablecmds. Every other arm
//! (and the event-trigger fences, whose owner event_trigger.c is unported) calls
//! an uninstalled seam, which is the project's loud documented panic — never a
//! silent stub.

use mcx::Mcx;
use types_catalog::catalog_dependency::{InvalidObjectAddress, ObjectAddress};
use types_error::PgResult;
use types_nodes::nodeindexscan::PlannedStmt;
use types_nodes::nodes::Node;
use types_nodes::parsestmt::{ParseState, ProcessUtilityContext, PROCESS_UTILITY_SUBCOMMAND};
use types_nodes::portalcmds::ParamListInfo;
use types_portal::QueryCompletion;
use types_storage::lock::{ShareLock, ShareUpdateExclusiveLock};
use types_tuple::access::{RELKIND_FOREIGN_TABLE, RELKIND_RELATION};

use backend_tcop_utility_out_seams as rt;

/// `ProcessUtilitySlow(pstate, pstmt, queryString, context, params, queryEnv,
/// dest, qc)` (utility.c:1085-1581). `queryEnv` is folded into `pstate`
/// (`pstate->p_queryEnv`) by the dispatch, so it is not a separate parameter
/// here. `dest`/`qc` are threaded for the CTAS / REFRESH arms.
#[allow(clippy::too_many_arguments)]
pub fn process_utility_slow<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    pstmt: &PlannedStmt<'mcx>,
    query_string: &str,
    context: ProcessUtilityContext,
    params: ParamListInfo,
    _dest: types_nodes::parsestmt::DestReceiverHandle,
    is_top_level: bool,
    mut qc: Option<&mut QueryCompletion>,
) -> PgResult<()> {
    // `parsetree = pstmt->utilityStmt` — guaranteed non-NULL by the dispatch.
    let parsetree: &Node<'mcx> = pstmt
        .utilityStmt
        .as_deref()
        .expect("ProcessUtilitySlow: PlannedStmt.utilityStmt is NULL");

    let is_complete_query = context != PROCESS_UTILITY_SUBCOMMAND;

    // All event trigger calls are done only when isCompleteQuery is true.
    let need_cleanup =
        is_complete_query && rt::event_trigger_begin_complete_query::call()?;

    // The C `PG_TRY { body } PG_FINALLY { if (needCleanup) EventTriggerEndCompleteQuery(); }`.
    // We run the body, capture its result, run the finally, then propagate.
    let result = process_utility_slow_body(
        mcx,
        pstate,
        pstmt,
        parsetree,
        query_string,
        context,
        params,
        is_top_level,
        is_complete_query,
        &mut qc,
    );

    if need_cleanup {
        rt::event_trigger_end_complete_query::call();
    }

    result
}

/// The `PG_TRY` body of `ProcessUtilitySlow`: the `EventTriggerDDLCommandStart`,
/// the `nodeTag(parsetree)` switch, and the trailing collect / SQLDrop /
/// DDLCommandEnd fences.
#[allow(clippy::too_many_arguments)]
fn process_utility_slow_body<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    pstmt: &PlannedStmt<'mcx>,
    parsetree: &Node<'mcx>,
    query_string: &str,
    _context: ProcessUtilityContext,
    params: ParamListInfo,
    is_top_level: bool,
    is_complete_query: bool,
    qc: &mut Option<&mut QueryCompletion>,
) -> PgResult<()> {
    if is_complete_query {
        rt::event_trigger_ddl_command_start::call(parsetree)?;
    }

    // ObjectAddress address; ObjectAddress secondaryObject = InvalidObjectAddress;
    let mut address: ObjectAddress = InvalidObjectAddress;
    let secondary_object: ObjectAddress = InvalidObjectAddress;
    let mut command_collected = false;

    match parsetree {
        // ----- relation and attribute manipulation -----
        Node::CreateSchemaStmt(_) => {
            rt::create_schema_command::call(
                mcx,
                parsetree,
                query_string,
                pstmt.stmt_location,
                pstmt.stmt_len,
            )?;
            // EventTriggerCollectSimpleCommand called by CreateSchemaCommand.
            command_collected = true;
        }

        Node::CreateStmt(_) | Node::CreateForeignTableStmt(_) => {
            // Run parse analysis ...
            let parsetree_ptr =
                mcx::alloc_in(mcx, parsetree.clone_in(mcx)?)?;
            let mut stmts = rt::transform_create_stmt::call(mcx, parsetree_ptr, query_string)?;

            // ... and do it.  We can't use foreach() because we may modify the
            // list midway through, so pick off the elements one at a time, the
            // hard way: front-delete from `stmts`, possibly prepend more.
            let mut table_rv: Option<types_nodes::nodes::NodePtr<'mcx>> = None;
            while !stmts.is_empty() {
                // list_delete_first: take the head, keeping ownership.
                let stmt: types_nodes::nodes::NodePtr<'mcx> = stmts.remove(0);

                match &*stmt {
                    Node::CreateStmt(cstmt) => {
                        // Remember transformed RangeVar for LIKE.
                        table_rv = match &cstmt.relation {
                            Some(rv) => Some(mcx::alloc_in(mcx, rv.clone_in(mcx)?)?),
                            None => None,
                        };

                        // Create the table itself.
                        address = rt::define_relation::call(
                            mcx,
                            cstmt.clone_in(mcx)?,
                            RELKIND_RELATION,
                            types_core::primitive::InvalidOid,
                            Some(query_string),
                        )?;
                        rt::event_trigger_collect_simple_command::call(
                            address,
                            secondary_object,
                            &stmt,
                        )?;

                        // Let NewRelationCreateToastTable decide if this one
                        // needs a secondary relation too.
                        rt::command_counter_increment::call()?;

                        // parse + validate toast reloptions and create the
                        // toast table (transformRelOptions("toast") +
                        // heap_reloptions(RELKIND_TOASTVALUE) +
                        // NewRelationCreateToastTable), sharing the new relation OID.
                        rt::create_toast_for_relation::call(mcx, address.objectId, &cstmt.options)?;
                    }
                    Node::CreateForeignTableStmt(cstmt) => {
                        // Remember transformed RangeVar for LIKE.
                        table_rv = match &cstmt.base.relation {
                            Some(rv) => Some(mcx::alloc_in(mcx, rv.clone_in(mcx)?)?),
                            None => None,
                        };

                        address = rt::define_relation::call(
                            mcx,
                            cstmt.base.clone_in(mcx)?,
                            RELKIND_FOREIGN_TABLE,
                            types_core::primitive::InvalidOid,
                            Some(query_string),
                        )?;
                        rt::create_foreign_table::call(mcx, &stmt, address.objectId)?;
                        rt::event_trigger_collect_simple_command::call(
                            address,
                            secondary_object,
                            &stmt,
                        )?;
                    }
                    Node::TableLikeClause(_) => {
                        // Delayed processing of LIKE options; prepend the
                        // resulting sub-statements to `stmts`.
                        let heap_rv_src = table_rv
                            .as_ref()
                            .expect("expandTableLikeClause: table_rv is NULL");
                        let heap_rv = mcx::alloc_in(mcx, heap_rv_src.clone_in(mcx)?)?;
                        let morestmts =
                            rt::expand_table_like_clause::call(mcx, heap_rv, stmt)?;
                        // list_concat(morestmts, stmts): morestmts first.
                        let mut combined = morestmts;
                        for s in stmts.drain(..) {
                            combined.push(s);
                        }
                        stmts = combined;
                    }
                    _ => {
                        // Recurse for anything else. The recursive call will
                        // stash the objects so created into our event-trigger
                        // context. (wrapper PlannedStmt + None receiver.)
                        rt::process_utility_wrapper::call(
                            mcx,
                            &stmt,
                            query_string,
                            pstmt.stmt_location,
                            pstmt.stmt_len,
                        )?;
                    }
                }

                // Need CCI between commands.
                if !stmts.is_empty() {
                    rt::command_counter_increment::call()?;
                }
            }

            // The multiple commands generated here are stashed individually, so
            // disable collection below.
            command_collected = true;
        }

        Node::AlterTableStmt(_) => {
            // The whole DETACH-CONCURRENTLY guard + lock-level + relation lookup
            // + EventTrigger fence + AlterTable (+ the "does not exist, skipping"
            // NOTICE) is one tablecmds-owned step (atcontext is internal there).
            rt::alter_table_slow::call(
                mcx,
                pstmt,
                parsetree,
                query_string,
                params,
                is_top_level,
            )?;
            // ALTER TABLE stashes commands internally.
            command_collected = true;
        }

        Node::AlterDomainStmt(_) => {
            // The 'T'/'N'/'O'/'C'/'X'/'V' subtype switch (typecmds.c).
            address = rt::alter_domain::call(mcx, parsetree)?;
        }

        // ----- object creation / destruction -----
        Node::DefineStmt(_) => {
            // The `kind` switch (aggregate / operator / type / TS* / collation).
            address = rt::define_stmt::call(mcx, pstate, parsetree)?;
        }

        Node::IndexStmt(stmt) => {
            // CREATE INDEX.
            if stmt.concurrent {
                rt::prevent_in_transaction_block::call(is_top_level, "CREATE INDEX CONCURRENTLY")?;
            }

            // Look up the relation OID just once, taking the strongest lock that
            // will eventually be needed (matching DefineIndex).
            let lockmode = if stmt.concurrent {
                ShareUpdateExclusiveLock
            } else {
                ShareLock
            };
            let relation_src = stmt
                .relation
                .as_ref()
                .expect("CREATE INDEX: IndexStmt.relation is NULL");
            let relation = mcx::alloc_in(mcx, relation_src.clone_in(mcx)?)?;
            let relid = rt::range_var_get_relid_owns_relation::call(mcx, relation, lockmode)?;

            // CREATE INDEX on partitioned tables (but not regular inherited
            // tables) recurses to partitions; lock them early, validate
            // relkinds, and count the partitions so DefineIndex needn't redo
            // the find_all_inheritors search. The owner reads stmt->relation->inh
            // and stmt->unique/primary internally and returns -1 when the target
            // is not a partitioned table.
            let stmt_for_count = mcx::alloc_in(mcx, parsetree.clone_in(mcx)?)?;
            let nparts: i32 =
                rt::create_index_count_partitions::call(mcx, relid, stmt_for_count, lockmode)?;

            // An already-transformed IndexStmt came from generateClonedIndexStmt
            // (expandTableLikeClause) — treat it like ALTER TABLE ADD INDEX.
            let is_alter_table = stmt.transformed;

            // Run parse analysis ...
            let stmt_ptr = mcx::alloc_in(mcx, parsetree.clone_in(mcx)?)?;
            let stmt2 = rt::transform_index_stmt::call(mcx, relid, stmt_ptr, query_string)?;

            // ... and do it.
            rt::event_trigger_alter_table_start::call(parsetree);
            address = rt::define_index::call(mcx, relid, stmt2, nparts, is_alter_table)?;

            // Add the CREATE INDEX node itself to the stash right away; commands
            // stashed in the ALTER TABLE code must appear after this one.
            rt::event_trigger_collect_simple_command::call(address, secondary_object, parsetree)?;
            command_collected = true;
            rt::event_trigger_alter_table_end::call();
        }

        Node::ReindexStmt(_) => {
            rt::exec_reindex::call(mcx, pstate, parsetree, is_top_level)?;
            // EventTriggerCollectSimpleCommand is called directly.
            command_collected = true;
        }

        Node::CompositeTypeStmt(_) => {
            // CREATE TYPE (composite).
            address = rt::define_composite_type::call(mcx, parsetree)?;
        }

        Node::CreateEnumStmt(_) => {
            address = rt::define_enum::call(mcx, parsetree)?;
        }

        Node::CreateRangeStmt(_) => {
            address = rt::define_range::call(mcx, pstate, parsetree)?;
        }

        Node::AlterEnumStmt(_) => {
            address = rt::alter_enum::call(mcx, parsetree)?;
        }

        Node::ViewStmt(_) => {
            // CREATE VIEW.
            rt::event_trigger_alter_table_start::call(parsetree);
            address = rt::define_view::call(
                mcx,
                parsetree,
                query_string,
                pstmt.stmt_location,
                pstmt.stmt_len,
            )?;
            rt::event_trigger_collect_simple_command::call(address, secondary_object, parsetree)?;
            // stashed internally
            command_collected = true;
            rt::event_trigger_alter_table_end::call();
        }

        Node::CreateFunctionStmt(_) => {
            address = rt::create_function::call(mcx, pstate, parsetree)?;
        }

        Node::AlterFunctionStmt(_) => {
            address = rt::alter_function::call(mcx, pstate, parsetree)?;
        }

        Node::RuleStmt(_) => {
            address = rt::define_rule::call(mcx, parsetree, query_string)?;
        }

        Node::CreateSeqStmt(_) => {
            address = rt::define_sequence::call(mcx, pstate, parsetree)?;
        }

        Node::AlterSeqStmt(_) => {
            address = rt::alter_sequence::call(mcx, pstate, parsetree)?;
        }

        Node::CreateTableAsStmt(_) => {
            address = rt::exec_create_table_as::call(mcx, pstate, parsetree, params, qc.take())?;
        }

        Node::RefreshMatViewStmt(_) => {
            // REFRESH CONCURRENTLY runs DDL internally; inhibit collection.
            rt::event_trigger_inhibit_command_collection::call();
            let r = rt::exec_refresh_mat_view::call(mcx, parsetree, query_string, qc.take());
            rt::event_trigger_undo_inhibit_command_collection::call();
            address = r?;
        }

        Node::CreateTrigStmt(_) => {
            address = rt::create_trigger::call(mcx, parsetree, query_string)?;
        }

        Node::CommentStmt(_) => {
            address = rt::comment_object_slow::call(mcx, parsetree)?;
        }

        Node::GrantStmt(_) => {
            rt::execute_grant_stmt_slow::call(mcx, parsetree)?;
            // commands are stashed in ExecGrantStmt_oids
            command_collected = true;
        }

        Node::AlterDefaultPrivilegesStmt(_) => {
            rt::exec_alter_default_privileges_stmt::call(mcx, pstate, parsetree)?;
            rt::event_trigger_collect_alter_def_privs::call(parsetree);
            command_collected = true;
        }

        Node::CreatePolicyStmt(_) => {
            address = rt::create_policy::call(mcx, parsetree)?;
        }

        Node::AlterPolicyStmt(_) => {
            address = rt::alter_policy::call(mcx, parsetree)?;
        }

        Node::SecLabelStmt(_) => {
            address = rt::exec_sec_label_stmt_slow::call(mcx, parsetree)?;
        }

        Node::DropStmt(_) => {
            crate::dispatch::ExecDropStmt(mcx, parsetree, is_top_level)?;
            // no commands stashed for DROP
            command_collected = true;
        }

        Node::RenameStmt(_) => {
            address = rt::exec_rename_stmt_slow::call(mcx, parsetree)?;
        }

        Node::AlterObjectDependsStmt(_) => {
            address = rt::exec_alter_object_depends_stmt_slow::call(mcx, parsetree)?;
        }

        Node::AlterObjectSchemaStmt(_) => {
            address = rt::exec_alter_object_schema_stmt_slow::call(mcx, parsetree)?;
        }

        Node::AlterOwnerStmt(_) => {
            address = rt::exec_alter_owner_stmt_slow::call(mcx, parsetree)?;
        }

        Node::AlterOperatorStmt(_) => {
            address = rt::alter_operator::call(mcx, parsetree)?;
        }

        Node::AlterTypeStmt(_) => {
            address = rt::alter_type::call(mcx, parsetree)?;
        }

        Node::AlterCollationStmt(_) => {
            address = rt::alter_collation::call(mcx, parsetree)?;
        }

        Node::DropOwnedStmt(_) => {
            rt::drop_owned_objects::call(mcx, parsetree)?;
            // no commands stashed for DROP
            command_collected = true;
        }

        Node::CreateStatsStmt(stmt) => {
            // CREATE STATISTICS supports only relation names in FROM.
            let rel_src = stmt
                .relations
                .first()
                .expect("CREATE STATISTICS: empty relations list");
            if !matches!(&**rel_src, Node::RangeVar(_)) {
                return Err(backend_utils_error::ereport(types_error::ERROR)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(
                        "CREATE STATISTICS only supports relation names in the FROM clause"
                            .to_string(),
                    )
                    .into_error());
            }
            // ShareUpdateExclusiveLock: conflicts with ANALYZE / other DDL that
            // sets statistics, but not with normal queries.
            let rel = mcx::alloc_in(mcx, rel_src.clone_in(mcx)?)?;
            let relid = rt::range_var_get_relid_share_update::call(mcx, rel)?;

            // Run parse analysis ...
            let stmt_ptr = mcx::alloc_in(mcx, parsetree.clone_in(mcx)?)?;
            let stmt2 = rt::transform_stats_stmt::call(mcx, relid, stmt_ptr, query_string)?;
            address = rt::create_statistics::call(mcx, stmt2)?;
        }

        Node::AlterStatsStmt(_) => {
            address = rt::alter_statistics::call(mcx, parsetree)?;
        }

        // The extension / FDW / AM / publication / subscription / transform /
        // cast / conversion / language / op-class / op-family / user-mapping /
        // import-foreign-schema DDL arms (utility.c:1395-1581). Their owners are
        // not yet ported; route them to one documented seam-panic so the
        // unported set is a single loud panic rather than ~30 panicking arms.
        _ => {
            address = rt::process_utility_slow_unported::call(mcx, pstate, parsetree, is_top_level)?;
        }
    }

    // Remember the object so that ddl_command_end event triggers can reach it.
    if !command_collected {
        rt::event_trigger_collect_simple_command::call(address, secondary_object, parsetree)?;
    }

    if is_complete_query {
        rt::event_trigger_sql_drop::call(parsetree)?;
        rt::event_trigger_ddl_command_end::call(parsetree)?;
    }

    Ok(())
}
