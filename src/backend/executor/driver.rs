use super::{
    Catalog, ExecError, ExecutorContext, ParseError, Plan, PlannedStmt, QueryDesc, Statement,
    StatementResult, TransactionId, TupleSlot, Value, bind_delete, bind_insert, bind_update,
    check_planned_stmt_select_for_update_privileges, check_planned_stmt_select_privileges,
    clear_subquery_eval_cache, create_query_desc, ensure_no_deferred_column_errors, eval_expr,
    execute_analyze, execute_create_index, execute_create_table, execute_delete,
    execute_drop_table, execute_explain, execute_insert, execute_merge, execute_truncate_table,
    execute_update, execute_vacuum, executor_start, parse_statement, pg_plan_query,
    pg_plan_values_query,
};
use crate::backend::parser::{
    CatalogLookup, CreateStatisticsStatement, SelectStatement, pg_plan_query_with_config,
    pg_plan_values_query_with_config, plan_merge,
};
use crate::backend::utils::cache::{
    catcache::CatCache, relcache::RelCache, visible_catalog::VisibleCatalog,
};
use crate::include::nodes::pathnodes::PlannerConfig;
use crate::pl::plpgsql::execute_do_with_context;
use pgrust_executor::{
    ReadonlyCreateStatisticsError, RestrictedRelationInfo, RestrictedViewCatalog,
    RestrictedViewError, UnsupportedStatementExecError, queue_pending_notification,
    reject_restricted_views_in_planned_stmt as reject_restricted_views_in_planned_stmt_impl,
    reject_restricted_views_in_select as reject_restricted_views_in_select_impl,
    restrict_nonsystem_view_enabled as restrict_nonsystem_view_guc_enabled,
    unsupported_statement_error as unsupported_statement_error_impl,
    validate_readonly_create_statistics as validate_readonly_create_statistics_impl,
};

fn unsupported_statement_error(stmt: &pgrust_nodes::parsenodes::UnsupportedStatement) -> ExecError {
    match unsupported_statement_error_impl(stmt) {
        UnsupportedStatementExecError::SecurityLabel { sql } => {
            ExecError::Parse(crate::backend::parser::security_label_provider_error(&sql))
        }
        UnsupportedStatementExecError::AlterTableWithOids => {
            ExecError::Parse(ParseError::UnexpectedToken {
                expected: "valid ALTER TABLE form",
                actual: "syntax error at or near \"WITH\"".into(),
            })
        }
        UnsupportedStatementExecError::FeatureNotSupported { feature, sql } => {
            ExecError::Parse(ParseError::FeatureNotSupported(format!("{feature}: {sql}")))
        }
    }
}

fn restrict_nonsystem_view_enabled(ctx: &ExecutorContext) -> bool {
    restrict_nonsystem_view_guc_enabled(
        ctx.gucs
            .get("restrict_nonsystem_relation_kind")
            .map(String::as_str),
    )
}

fn visible_catalog_for_planning(catalog: &Catalog) -> VisibleCatalog {
    VisibleCatalog::new(
        RelCache::from_catalog(catalog),
        Some(CatCache::from_catalog(catalog)),
    )
}

struct RestrictedViewCatalogAdapter<'a>(&'a dyn CatalogLookup);

impl RestrictedViewCatalog for RestrictedViewCatalogAdapter<'_> {
    fn lookup_relation_by_name(&self, name: &str) -> Option<RestrictedRelationInfo> {
        let entry = self.0.lookup_any_relation(name)?;
        let relation_name = self
            .0
            .class_row_by_oid(entry.relation_oid)
            .map(|row| row.relname)
            .unwrap_or_default();
        Some(RestrictedRelationInfo {
            relation_oid: entry.relation_oid,
            relation_name,
            namespace_oid: entry.namespace_oid,
            relkind: entry.relkind,
        })
    }

    fn relation_info_by_oid(&self, relation_oid: u32) -> Option<RestrictedRelationInfo> {
        let row = self.0.class_row_by_oid(relation_oid)?;
        Some(RestrictedRelationInfo {
            relation_oid,
            relation_name: row.relname,
            namespace_oid: row.relnamespace,
            relkind: row.relkind,
        })
    }
}

fn restricted_view_error(err: RestrictedViewError) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "access to non-system view \"{}\" is restricted",
            err.relation_name
        ),
        detail: None,
        hint: None,
        sqlstate: "55000",
    }
}

fn reject_restricted_views_in_planned_stmt(
    planned_stmt: &PlannedStmt,
    catalog: &dyn CatalogLookup,
    ctx: &ExecutorContext,
) -> Result<(), ExecError> {
    if !restrict_nonsystem_view_enabled(ctx) {
        return Ok(());
    }
    reject_restricted_views_in_planned_stmt_impl(
        planned_stmt,
        &RestrictedViewCatalogAdapter(catalog),
    )
    .map_err(restricted_view_error)
}

fn reject_restricted_views_in_select(
    select: &SelectStatement,
    catalog: &dyn CatalogLookup,
) -> Result<(), ExecError> {
    reject_restricted_views_in_select_impl(select, &RestrictedViewCatalogAdapter(catalog))
        .map_err(restricted_view_error)
}

pub fn execute_planned_stmt(
    planned_stmt: PlannedStmt,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    execute_query_desc(create_query_desc(planned_stmt, None), ctx)
}

pub fn execute_query_desc(
    query_desc: QueryDesc,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    clear_subquery_eval_cache();
    let columns = query_desc.columns();
    let column_names = query_desc.column_names();
    let planned_stmt = query_desc.planned_stmt;
    let saved_subplans = std::mem::replace(&mut ctx.subplans, planned_stmt.subplans);
    let saved_scalar_function_cache = std::mem::take(&mut ctx.scalar_function_cache);
    let saved_proc_execute_acl_cache = std::mem::take(&mut ctx.proc_execute_acl_cache);
    let saved_initplan_values = std::mem::take(&mut ctx.expr_bindings.initplan_values);
    let result = (|| {
        let saved_exec_params = if planned_stmt.ext_params.is_empty() {
            Vec::new()
        } else {
            let mut param_slot = ctx
                .expr_bindings
                .outer_tuple
                .clone()
                .map(TupleSlot::virtual_row)
                .unwrap_or_else(|| TupleSlot::empty(0));
            let mut saved = Vec::with_capacity(planned_stmt.ext_params.len());
            for param in &planned_stmt.ext_params {
                let value = eval_expr(&param.expr, &mut param_slot, ctx)?;
                let old = ctx.expr_bindings.exec_params.insert(param.paramid, value);
                saved.push((param.paramid, old));
            }
            saved
        };
        ctx.cte_tables.clear();
        ctx.cte_tables.extend(
            ctx.pinned_cte_tables
                .iter()
                .map(|(cte_id, table)| (*cte_id, table.clone())),
        );
        ctx.cte_producers.clear();
        ctx.recursive_worktables.clear();
        let result = (|| {
            let mut state = executor_start(planned_stmt.plan_tree);
            let mut rows = Vec::new();
            while let Some(slot) = state.exec_proc_node(ctx)? {
                let mut values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                ensure_no_deferred_column_errors(&values)?;
                Value::materialize_all(&mut values);
                rows.push(values);
            }
            Ok(StatementResult::Query {
                columns,
                column_names,
                rows,
            })
        })();
        ctx.cte_tables.clear();
        ctx.cte_producers.clear();
        ctx.recursive_worktables.clear();
        for (paramid, old) in saved_exec_params {
            if let Some(value) = old {
                ctx.expr_bindings.exec_params.insert(paramid, value);
            } else {
                ctx.expr_bindings.exec_params.remove(&paramid);
            }
        }
        result
    })();
    ctx.expr_bindings.initplan_values = saved_initplan_values;
    ctx.proc_execute_acl_cache = saved_proc_execute_acl_cache;
    ctx.scalar_function_cache = saved_scalar_function_cache;
    ctx.subplans = saved_subplans;
    clear_subquery_eval_cache();
    result
}

pub fn execute_plan(plan: Plan, ctx: &mut ExecutorContext) -> Result<StatementResult, ExecError> {
    execute_query_desc(
        create_query_desc(
            PlannedStmt {
                command_type: crate::include::executor::execdesc::CommandType::Select,
                depends_on_row_security: false,
                relation_privileges: Vec::new(),
                plan_tree: plan,
                subplans: Vec::new(),
                ext_params: Vec::new(),
            },
            None,
        ),
        ctx,
    )
}

pub fn execute_sql(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    let stmt = parse_statement(sql)?;
    execute_statement_with_source(stmt, Some(sql), catalog, ctx, xid)
}

pub fn execute_statement(
    stmt: Statement,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    execute_statement_with_source(stmt, None, catalog, ctx, xid)
}

fn execute_statement_with_source(
    stmt: Statement,
    source_text: Option<&str>,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    let cid = ctx.next_command_id;
    ctx.snapshot = ctx.txns.read().snapshot_for_command(xid, cid)?;
    let saved_scalar_function_cache = std::mem::take(&mut ctx.scalar_function_cache);
    let saved_proc_execute_acl_cache = std::mem::take(&mut ctx.proc_execute_acl_cache);
    let saved_initplan_values = std::mem::take(&mut ctx.expr_bindings.initplan_values);
    let result = (|| match stmt {
        Statement::Do(stmt) => execute_do_with_context(&stmt, catalog, ctx),
        Statement::Explain(stmt) => execute_explain(stmt, catalog, ctx, PlannerConfig::default()),
        Statement::Select(stmt) => {
            let requires_update = stmt.locking_clause.is_some();
            let planning_catalog = visible_catalog_for_planning(catalog);
            if restrict_nonsystem_view_enabled(ctx) {
                reject_restricted_views_in_select(&stmt, &planning_catalog)?;
            }
            let planned = crate::backend::rewrite::with_restrict_nonsystem_view_expansion(
                restrict_nonsystem_view_enabled(ctx),
                || pg_plan_query(&stmt, &planning_catalog),
            )?;
            if requires_update {
                check_planned_stmt_select_for_update_privileges(&planned, ctx)?;
            } else {
                reject_restricted_views_in_planned_stmt(&planned, &planning_catalog, ctx)?;
                check_planned_stmt_select_privileges(&planned, ctx)?;
            }
            execute_query_desc(
                create_query_desc(planned, source_text.map(str::to_string)),
                ctx,
            )
        }
        Statement::Values(stmt) => execute_query_desc(
            create_query_desc(
                pg_plan_values_query(&stmt, catalog)?,
                source_text.map(str::to_string),
            ),
            ctx,
        ),
        Statement::Analyze(stmt) => execute_analyze(stmt, catalog),
        Statement::Notify(stmt) => {
            queue_pending_notification(
                &mut ctx.pending_async_notifications,
                &stmt.channel,
                stmt.payload.as_deref().unwrap_or(""),
            )?;
            Ok(StatementResult::AffectedRows(0))
        }
        Statement::Listen(stmt) => {
            if let Some(runtime) = ctx.async_notify_runtime.as_ref() {
                runtime.listen(ctx.client_id, &stmt.channel);
            }
            Ok(StatementResult::AffectedRows(0))
        }
        Statement::Unlisten(stmt) => {
            if let Some(runtime) = ctx.async_notify_runtime.as_ref() {
                runtime.unlisten(ctx.client_id, stmt.channel.as_deref());
            }
            Ok(StatementResult::AffectedRows(0))
        }
        Statement::Load(_) | Statement::Discard(_) => Ok(StatementResult::AffectedRows(0)),
        Statement::DeclareCursor(_)
        | Statement::Fetch(_)
        | Statement::Move(_)
        | Statement::ClosePortal(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "portal command handled by session layer",
            actual: "portal command".into(),
        })),
        Statement::Show(_)
        | Statement::Checkpoint(_)
        | Statement::Set(_)
        | Statement::SetTransaction(_)
        | Statement::SetConstraints(_)
        | Statement::Reset(_)
        | Statement::Prepare(_)
        | Statement::Execute(_)
        | Statement::Deallocate(_)
        | Statement::SetRole(_)
        | Statement::ResetRole(_)
        | Statement::AlterTableAlterConstraint(_)
        | Statement::AlterTableAlterColumnCompression(_)
        | Statement::AlterTableAlterColumnOptions(_)
        | Statement::AlterTableAlterColumnStatistics(_)
        | Statement::AlterTableAlterColumnStorage(_)
        | Statement::AlterTableAlterColumnDefault(_)
        | Statement::AlterTableAlterColumnExpression(_)
        | Statement::AlterTableAlterColumnIdentity(_)
        // :HACK: ALTER TABLE ... SET (...) is accepted narrowly for numeric.sql and ignored
        // until table reloptions are modeled for real.
        | Statement::AlterTableSet(_)
        // :HACK: ALTER INDEX ... SET (...) is accepted narrowly for hash_index.sql and ignored
        // until mutable index reloptions are modeled for real.
        | Statement::AlterIndexSet(_)
        | Statement::AlterTableReset(_)
        | Statement::AlterTableSetRowSecurity(_)
        | Statement::CreateStatistics(_)
        | Statement::AlterStatistics(_)
        | Statement::CreatePolicy(_)
        | Statement::AlterPolicy(_)
        | Statement::DropPolicy(_)
        | Statement::CommentOnStatistics(_)
        | Statement::DropStatistics(_) => Ok(StatementResult::AffectedRows(0)),
        Statement::CopyFrom(_) | Statement::CopyTo(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COPY handled by session layer",
            actual: "COPY".into(),
        })),
        Statement::AlterPublication(_)
        | Statement::CommentOnPublication(_)
        | Statement::CreatePublication(_)
        | Statement::DropPublication(_)
        | Statement::AlterSubscription(_)
        | Statement::CommentOnSubscription(_)
        | Statement::CreateSubscription(_)
        | Statement::DropSubscription(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "PUBLICATION/SUBSCRIPTION handled by database/session layer",
            actual: "PUBLICATION/SUBSCRIPTION".into(),
        })),
        Statement::CreateTextSearchDictionary(_)
        | Statement::AlterTextSearchDictionary(_)
        | Statement::CreateTextSearchConfiguration(_)
        | Statement::AlterTextSearchConfiguration(_)
        | Statement::DropTextSearchConfiguration(_)
        | Statement::DropTextSearch(_) => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "TEXT SEARCH handled by database/session layer",
                actual: "TEXT SEARCH".into(),
            }))
        }
        Statement::CreateTrigger(_)
        | Statement::CreateEventTrigger(_)
        | Statement::DropTrigger(_)
        | Statement::DropEventTrigger(_)
        | Statement::AlterTableTriggerState(_)
        | Statement::AlterTriggerRename(_)
        | Statement::AlterEventTrigger(_)
        | Statement::AlterEventTriggerOwner(_)
        | Statement::AlterEventTriggerRename(_) => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "TRIGGER handled by database/session layer",
                actual: "TRIGGER".into(),
            }))
        }
        Statement::AlterTableRename(_)
        | Statement::AlterTableSetSchema(_)
        | Statement::AlterTableSetTablespace(_)
        | Statement::AlterIndexSetTablespace(_)
        | Statement::AlterMoveAllTablespace(_)
        | Statement::AlterTableSetPersistence(_)
        | Statement::AlterTableSetWithoutCluster(_)
        | Statement::AlterIndexRename(_)
        | Statement::AlterIndexAttachPartition(_)
        | Statement::AlterViewRename(_)
        | Statement::AlterViewRenameColumn(_)
        | Statement::AlterViewSetSchema(_)
        | Statement::AlterMaterializedViewSetSchema(_)
        | Statement::AlterMaterializedViewSetAccessMethod(_)
        | Statement::AlterIndexAlterColumnStatistics(_)
        | Statement::AlterIndexAlterColumnOptions(_) => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "ALTER TABLE/INDEX/VIEW handled by database/session layer",
                actual: "ALTER TABLE/INDEX/VIEW".into(),
            }))
        }
        Statement::AlterTableOwner(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER TABLE OWNER handled by database/session layer",
            actual: "ALTER TABLE OWNER".into(),
        })),
        Statement::AlterLargeObjectOwner(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER LARGE OBJECT handled by database/session layer",
            actual: "ALTER LARGE OBJECT".into(),
        })),
        Statement::AlterTableRenameColumn(_) => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "ALTER TABLE RENAME COLUMN handled by database/session layer",
                actual: "ALTER TABLE RENAME COLUMN".into(),
            }))
        }
        Statement::AlterTableAddColumn(_) | Statement::AlterTableAddColumns(_) => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER TABLE ADD COLUMN handled by database/session layer",
            actual: "ALTER TABLE ADD COLUMN".into(),
        }))
        }
        Statement::AlterTableAddConstraint(_)
        | Statement::AlterTableDropConstraint(_)
        | Statement::AlterTableRenameConstraint(_)
        | Statement::AlterTableSetNotNull(_)
        | Statement::AlterTableDropNotNull(_)
        | Statement::AlterTableValidateConstraint(_)
        | Statement::AlterTableInherit(_)
        | Statement::AlterTableNoInherit(_)
        | Statement::AlterTableOf(_)
        | Statement::AlterTableNotOf(_)
        | Statement::AlterTableAttachPartition(_)
        | Statement::AlterTableDetachPartition(_) => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "ALTER TABLE constraint operations handled by database/session layer",
                actual: "ALTER TABLE constraint operation".into(),
            }))
        }
        Statement::AlterTableDropColumn(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER TABLE DROP COLUMN handled by database/session layer",
            actual: "ALTER TABLE DROP COLUMN".into(),
        })),
        Statement::AlterTableAlterColumnType(_) => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "ALTER TABLE ALTER COLUMN TYPE handled by database/session layer",
                actual: "ALTER TABLE ALTER COLUMN TYPE".into(),
            }))
        }
        Statement::AlterViewOwner(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER VIEW OWNER handled by database/session layer",
            actual: "ALTER VIEW OWNER".into(),
        })),
        Statement::AlterSchemaOwner(_) | Statement::AlterSchemaRename(_) => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "ALTER SCHEMA handled by database/session layer",
                actual: "ALTER SCHEMA".into(),
            }))
        }
        Statement::CommentOnDatabase(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON DATABASE handled by database/session layer",
            actual: "COMMENT ON DATABASE".into(),
        })),
        Statement::CommentOnTable(_) | Statement::CommentOnColumn(_) | Statement::CommentOnSequence(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON TABLE handled by database/session layer",
            actual: "COMMENT ON TABLE".into(),
        })),
        Statement::CommentOnView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON VIEW handled by database/session layer",
            actual: "COMMENT ON VIEW".into(),
        })),
        Statement::CommentOnIndex(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON INDEX handled by database/session layer",
            actual: "COMMENT ON INDEX".into(),
        })),
        Statement::CommentOnAggregate(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON AGGREGATE handled by database/session layer",
            actual: "COMMENT ON AGGREGATE".into(),
        })),
        Statement::CommentOnFunction(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON FUNCTION handled by database/session layer",
            actual: "COMMENT ON FUNCTION".into(),
        })),
        Statement::CommentOnOperator(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON OPERATOR handled by database/session layer",
            actual: "COMMENT ON OPERATOR".into(),
        })),
        Statement::CommentOnLargeObject(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON LARGE OBJECT handled by database/session layer",
            actual: "COMMENT ON LARGE OBJECT".into(),
        })),
        Statement::CommentOnType(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON TYPE handled by database/session layer",
            actual: "COMMENT ON TYPE".into(),
        })),
        Statement::CommentOnConstraint(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON CONSTRAINT handled by database/session layer",
            actual: "COMMENT ON CONSTRAINT".into(),
        })),
        Statement::CommentOnRule(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON RULE handled by database/session layer",
            actual: "COMMENT ON RULE".into(),
        })),
        Statement::CommentOnTrigger(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON TRIGGER handled by database/session layer",
            actual: "COMMENT ON TRIGGER".into(),
        })),
        Statement::CommentOnEventTrigger(_) => Err(ExecError::Parse(
            ParseError::UnexpectedToken {
                expected: "COMMENT ON EVENT TRIGGER handled by database/session layer",
                actual: "COMMENT ON EVENT TRIGGER".into(),
            },
        )),
        Statement::CommentOnDomain(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON DOMAIN handled by database/session layer",
            actual: "COMMENT ON DOMAIN".into(),
        })),
        Statement::CommentOnConversion(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON CONVERSION handled by database/session layer",
            actual: "COMMENT ON CONVERSION".into(),
        })),
        Statement::CommentOnForeignDataWrapper(_)
        | Statement::CommentOnForeignServer(_)
        | Statement::CreateForeignDataWrapper(_)
        | Statement::CreateForeignServer(_)
        | Statement::CreateLanguage(_)
        | Statement::AlterLanguage(_)
        | Statement::DropLanguage(_)
        | Statement::CreateForeignTable(_)
        | Statement::ImportForeignSchema(_)
        | Statement::CreateUserMapping(_)
        | Statement::AlterForeignDataWrapper(_)
        | Statement::AlterForeignDataWrapperOwner(_)
        | Statement::AlterForeignDataWrapperRename(_)
        | Statement::AlterForeignServer(_)
        | Statement::AlterForeignServerOwner(_)
        | Statement::AlterForeignServerRename(_)
        | Statement::AlterForeignTableOptions(_)
        | Statement::AlterUserMapping(_)
        | Statement::DropForeignServer(_)
        | Statement::DropUserMapping(_)
        | Statement::DropForeignDataWrapper(_) => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "FOREIGN DATA WRAPPER handled by database/session layer",
                actual: "FOREIGN DATA WRAPPER".into(),
            }))
        }
        Statement::CommentOnRole(_)
        | Statement::CreateRole(_)
        | Statement::AlterRole(_)
        | Statement::DropRole(_)
        | Statement::GrantObject(_)
        | Statement::RevokeObject(_)
        | Statement::AlterDefaultPrivileges(_)
        | Statement::GrantRoleMembership(_)
        | Statement::RevokeRoleMembership(_)
        | Statement::SetSessionAuthorization(_)
        | Statement::ResetSessionAuthorization(_)
        | Statement::DropOwned(_)
        | Statement::ReassignOwned(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "role management".into(),
        ))),
        Statement::ReindexIndex(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "REINDEX handled by database/session layer",
            actual: "REINDEX".into(),
        })),
        Statement::CreateIndex(stmt) => execute_create_index(stmt, catalog, ctx),
        Statement::Call(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "CALL execution".into(),
        ))),
        Statement::CreateFunction(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE FUNCTION handled by database/session layer",
            actual: "CREATE FUNCTION".into(),
        })),
        Statement::CreateProcedure(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE PROCEDURE handled by database/session layer",
            actual: "CREATE PROCEDURE".into(),
        })),
        Statement::CreateAggregate(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE AGGREGATE handled by database/session layer",
            actual: "CREATE AGGREGATE".into(),
        })),
        Statement::AlterAggregateRename(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER AGGREGATE handled by database/session layer",
            actual: "ALTER AGGREGATE".into(),
        })),
        Statement::CreateCast(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE CAST handled by database/session layer",
            actual: "CREATE CAST".into(),
        })),
        Statement::CreateOperator(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE OPERATOR handled by database/session layer",
            actual: "CREATE OPERATOR".into(),
        })),
        Statement::DropFunction(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP FUNCTION handled by database/session layer",
            actual: "DROP FUNCTION".into(),
        })),
        Statement::DropProcedure(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP PROCEDURE handled by database/session layer",
            actual: "DROP PROCEDURE".into(),
        })),
        Statement::DropRoutine(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP ROUTINE handled by database/session layer",
            actual: "DROP ROUTINE".into(),
        })),
        Statement::DropAggregate(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP AGGREGATE handled by database/session layer",
            actual: "DROP AGGREGATE".into(),
        })),
        Statement::DropCast(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP CAST handled by database/session layer",
            actual: "DROP CAST".into(),
        })),
        Statement::DropOperator(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP OPERATOR handled by database/session layer",
            actual: "DROP OPERATOR".into(),
        })),
        Statement::CreateOperatorClass(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE OPERATOR CLASS handled by database/session layer",
            actual: "CREATE OPERATOR CLASS".into(),
        })),
        Statement::CreateOperatorFamily(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE OPERATOR FAMILY handled by database/session layer",
            actual: "CREATE OPERATOR FAMILY".into(),
        })),
        Statement::AlterOperatorFamily(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER OPERATOR FAMILY handled by database/session layer",
            actual: "ALTER OPERATOR FAMILY".into(),
        })),
        Statement::AlterOperatorClass(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER OPERATOR CLASS handled by database/session layer",
            actual: "ALTER OPERATOR CLASS".into(),
        })),
        Statement::DropOperatorFamily(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP OPERATOR FAMILY handled by database/session layer",
            actual: "DROP OPERATOR FAMILY".into(),
        })),
        Statement::DropOperatorClass(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP OPERATOR CLASS handled by database/session layer",
            actual: "DROP OPERATOR CLASS".into(),
        })),
        Statement::CreateTextSearch(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE TEXT SEARCH handled by database/session layer",
            actual: "CREATE TEXT SEARCH".into(),
        })),
        Statement::AlterTextSearch(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER TEXT SEARCH handled by database/session layer",
            actual: "ALTER TEXT SEARCH".into(),
        })),
        Statement::AlterOperator(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER OPERATOR handled by database/session layer",
            actual: "ALTER OPERATOR".into(),
        })),
        Statement::AlterConversion(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER CONVERSION handled by database/session layer",
            actual: "ALTER CONVERSION".into(),
        })),
        Statement::AlterProcedure(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "ALTER PROCEDURE".into(),
        ))),
        Statement::AlterRoutine(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER ROUTINE handled by database/session layer",
            actual: "ALTER ROUTINE".into(),
        })),
        Statement::CreateDatabase(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE DATABASE handled by database/session layer",
            actual: "CREATE DATABASE".into(),
        })),
        Statement::AlterDatabase(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER DATABASE handled by database/session layer",
            actual: "ALTER DATABASE".into(),
        })),
        Statement::CreateSchema(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE SCHEMA handled by database/session layer",
            actual: "CREATE SCHEMA".into(),
        })),
        Statement::CreateTablespace(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE TABLESPACE handled by database/session layer",
            actual: "CREATE TABLESPACE".into(),
        })),
        Statement::DropTablespace(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP TABLESPACE handled by database/session layer",
            actual: "DROP TABLESPACE".into(),
        })),
        Statement::AlterTablespace(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER TABLESPACE handled by database/session layer",
            actual: "ALTER TABLESPACE".into(),
        })),
        Statement::CreateDomain(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE DOMAIN handled by database/session layer",
            actual: "CREATE DOMAIN".into(),
        })),
        Statement::AlterDomain(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER DOMAIN handled by database/session layer",
            actual: "ALTER DOMAIN".into(),
        })),
        Statement::CreateConversion(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE CONVERSION handled by database/session layer",
            actual: "CREATE CONVERSION".into(),
        })),
        Statement::CreateCollation(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE COLLATION handled by database/session layer",
            actual: "CREATE COLLATION".into(),
        })),
        Statement::CreateType(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE TYPE handled by database/session layer",
            actual: "CREATE TYPE".into(),
        })),
        Statement::AlterType(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER TYPE handled by database/session layer",
            actual: "ALTER TYPE".into(),
        })),
        Statement::AlterTypeOwner(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER TYPE OWNER handled by database/session layer",
            actual: "ALTER TYPE OWNER".into(),
        })),
        Statement::CreateSequence(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE SEQUENCE handled by database/session layer",
            actual: "CREATE SEQUENCE".into(),
        })),
        Statement::CreateTable(stmt) => execute_create_table(stmt, catalog),
        Statement::CreateTableAs(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "create table handled by database/session layer",
            actual: "CREATE TABLE AS".into(),
        })),
        Statement::CreateView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE VIEW handled by database/session layer",
            actual: "CREATE VIEW".into(),
        })),
        Statement::CreateRule(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE RULE handled by database/session layer",
            actual: "CREATE RULE".into(),
        })),
        Statement::AlterRuleRename(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER RULE handled by database/session layer",
            actual: "ALTER RULE".into(),
        })),
        Statement::AlterTableRuleState(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER TABLE handled by database/session layer",
            actual: "ALTER TABLE".into(),
        })),
        Statement::DropTable(stmt) => execute_drop_table(stmt, catalog, ctx),
        Statement::DropIndex(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP INDEX handled by database/session layer",
            actual: "DROP INDEX".into(),
        })),
        Statement::DropDomain(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP DOMAIN handled by database/session layer",
            actual: "DROP DOMAIN".into(),
        })),
        Statement::DropConversion(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP CONVERSION handled by database/session layer",
            actual: "DROP CONVERSION".into(),
        })),
        Statement::DropCollation(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP COLLATION handled by database/session layer",
            actual: "DROP COLLATION".into(),
        })),
        Statement::DropType(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP TYPE handled by database/session layer",
            actual: "DROP TYPE".into(),
        })),
        Statement::DropSequence(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP SEQUENCE handled by database/session layer",
            actual: "DROP SEQUENCE".into(),
        })),
        Statement::DropDatabase(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP DATABASE handled by database/session layer",
            actual: "DROP DATABASE".into(),
        })),
        Statement::DropExtension(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP EXTENSION handled by database/session layer",
            actual: "DROP EXTENSION".into(),
        })),
        Statement::DropAccessMethod(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP ACCESS METHOD handled by database/session layer",
            actual: "DROP ACCESS METHOD".into(),
        })),
        Statement::DropView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP VIEW handled by database/session layer",
            actual: "DROP VIEW".into(),
        })),
        Statement::DropRule(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP RULE handled by database/session layer",
            actual: "DROP RULE".into(),
        })),
        Statement::DropSchema(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP SCHEMA handled by database/session layer",
            actual: "DROP SCHEMA".into(),
        })),
        Statement::AlterSequence(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER SEQUENCE handled by database/session layer",
            actual: "ALTER SEQUENCE".into(),
        })),
        Statement::AlterSequenceOwner(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER SEQUENCE OWNER handled by database/session layer",
            actual: "ALTER SEQUENCE OWNER".into(),
        })),
        Statement::AlterSequenceRename(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER SEQUENCE RENAME handled by database/session layer",
            actual: "ALTER SEQUENCE RENAME".into(),
        })),
        Statement::AlterSequenceSetSchema(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER SEQUENCE SET SCHEMA handled by database/session layer",
            actual: "ALTER SEQUENCE SET SCHEMA".into(),
        })),
        Statement::RefreshMaterializedView(_) => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "REFRESH MATERIALIZED VIEW handled by database/session layer",
                actual: "REFRESH MATERIALIZED VIEW".into(),
            }))
        }
        Statement::Cluster(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CLUSTER handled by database/session layer",
            actual: "CLUSTER".into(),
        })),
        Statement::DropMaterializedView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP MATERIALIZED VIEW handled by database/session layer",
            actual: "DROP MATERIALIZED VIEW".into(),
        })),
        Statement::LockTable(_) => Err(ExecError::DetailedError {
            message: "LOCK TABLE can only be used in transaction blocks".into(),
            detail: None,
            hint: None,
            sqlstate: "25P01",
        }),
        Statement::TruncateTable(stmt) => execute_truncate_table(stmt, catalog, ctx, xid),
        Statement::Vacuum(stmt) => execute_vacuum(stmt, catalog, ctx),
        Statement::Insert(stmt) => {
            execute_insert(bind_insert(&stmt, catalog)?, catalog, ctx, xid, cid)
        }
        Statement::Merge(stmt) => execute_merge(plan_merge(&stmt, catalog)?, catalog, ctx, xid, cid),
        Statement::Update(stmt) => {
            execute_update(bind_update(&stmt, catalog)?, catalog, ctx, xid, cid)
        }
        Statement::Delete(stmt) => execute_delete(bind_delete(&stmt, catalog)?, catalog, ctx, xid),
        Statement::Unsupported(stmt) if stmt.feature == "ALTER DEFAULT PRIVILEGES" => {
            // :HACK: pgrust does not track default ACLs yet; accept this DDL
            // form so regression scripts that set up ownership can proceed.
            Ok(StatementResult::AffectedRows(0))
        }
        Statement::Unsupported(stmt)
            if stmt.feature == "ALTER TABLE form"
                && stmt.sql.to_ascii_lowercase().contains(" set without oids") =>
        {
            Ok(StatementResult::AffectedRows(0))
        }
        Statement::AlterTableCompound(_) => Err(ExecError::Parse(
            ParseError::FeatureNotSupported("ALTER TABLE compound execution".into()),
        )),
        Statement::Unsupported(stmt) => Err(unsupported_statement_error(&stmt)),
        Statement::AlterTableMulti(_) | Statement::AlterTableReplicaIdentity(_) => {
            Err(ExecError::Parse(ParseError::FeatureNotSupported(
                "utility statement in executor".into(),
            )))
        }
        Statement::Begin(_)
        | Statement::Commit(_)
        | Statement::Rollback(_)
        | Statement::PrepareTransaction(_)
        | Statement::CommitPrepared(_)
        | Statement::RollbackPrepared(_)
        | Statement::Savepoint(_)
        | Statement::ReleaseSavepoint(_)
        | Statement::RollbackTo(_) => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "non-transaction-control statement",
                actual: "transaction control".into(),
            }))
        }
    })();
    ctx.expr_bindings.initplan_values = saved_initplan_values;
    ctx.proc_execute_acl_cache = saved_proc_execute_acl_cache;
    ctx.scalar_function_cache = saved_scalar_function_cache;
    ctx.next_command_id = ctx.next_command_id.saturating_add(1);
    result
}

pub fn execute_readonly_statement(
    stmt: Statement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    execute_readonly_statement_with_config(stmt, catalog, ctx, PlannerConfig::default())
}

pub fn execute_readonly_statement_with_config(
    stmt: Statement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
    planner_config: PlannerConfig,
) -> Result<StatementResult, ExecError> {
    match stmt {
        Statement::Do(stmt) => execute_do_with_context(&stmt, catalog, ctx),
        Statement::Explain(stmt) => execute_explain(stmt, catalog, ctx, planner_config),
        Statement::Select(stmt) => {
            if let Some(locking_clause) = stmt.locking_clause {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "{} is not allowed in a read-only execution context",
                        locking_clause.sql()
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "25006",
                });
            }
            if restrict_nonsystem_view_enabled(ctx) {
                reject_restricted_views_in_select(&stmt, catalog)?;
            }
            let planned = crate::backend::rewrite::with_restrict_nonsystem_view_expansion(
                restrict_nonsystem_view_enabled(ctx),
                || pg_plan_query_with_config(&stmt, catalog, planner_config),
            )?;
            reject_restricted_views_in_planned_stmt(&planned, catalog, ctx)?;
            check_planned_stmt_select_privileges(&planned, ctx)?;
            execute_planned_stmt(planned, ctx)
        }
        Statement::Values(stmt) => execute_planned_stmt(
            pg_plan_values_query_with_config(&stmt, catalog, planner_config)?,
            ctx,
        ),
        Statement::Analyze(stmt) => execute_analyze(stmt, catalog),
        Statement::CreateStatistics(stmt) => validate_readonly_create_statistics(&stmt, catalog),
        Statement::Show(_)
        | Statement::Set(_)
        | Statement::Reset(_)
        | Statement::Prepare(_)
        | Statement::Execute(_)
        | Statement::Deallocate(_)
        | Statement::AlterTableSet(_)
        | Statement::AlterTableReset(_)
        | Statement::AlterTableSetSchema(_)
        | Statement::AlterTableSetTablespace(_)
        | Statement::AlterIndexSetTablespace(_)
        | Statement::AlterMoveAllTablespace(_)
        | Statement::AlterTableRenameColumn(_)
        | Statement::AlterViewRenameColumn(_)
        | Statement::AlterViewSetSchema(_)
        | Statement::AlterMaterializedViewSetSchema(_)
        | Statement::AlterMaterializedViewSetAccessMethod(_)
        | Statement::AlterTableAddColumn(_)
        | Statement::AlterTableAddColumns(_)
        | Statement::AlterTableDropColumn(_)
        | Statement::AlterTableAlterColumnType(_)
        | Statement::AlterTableAlterColumnCompression(_)
        | Statement::AlterTableAlterColumnStorage(_)
        | Statement::AlterTableAlterColumnDefault(_)
        | Statement::AlterTableAlterColumnExpression(_)
        | Statement::AlterTableOf(_)
        | Statement::AlterTableNotOf(_)
        | Statement::AlterLargeObjectOwner(_)
        | Statement::AlterTableAlterColumnIdentity(_)
        | Statement::AlterTableAttachPartition(_)
        | Statement::AlterTableDetachPartition(_) => Ok(StatementResult::AffectedRows(0)),
        Statement::AlterTableRename(_) | Statement::AlterViewRename(_) => {
            Ok(StatementResult::AffectedRows(0))
        }
        Statement::Merge(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "MERGE".into(),
        ))),
        Statement::Unsupported(stmt) if stmt.feature == "ALTER DEFAULT PRIVILEGES" => {
            // :HACK: see readonly path above.
            Ok(StatementResult::AffectedRows(0))
        }
        Statement::Unsupported(stmt)
            if stmt.feature == "ALTER TABLE form"
                && stmt.sql.to_ascii_lowercase().contains(" set without oids") =>
        {
            Ok(StatementResult::AffectedRows(0))
        }
        Statement::Unsupported(stmt) => Err(unsupported_statement_error(&stmt)),
        Statement::CommentOnTable(_)
        | Statement::CommentOnColumn(_)
        | Statement::CommentOnSequence(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON TABLE".into(),
        })),
        Statement::CommentOnView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON VIEW".into(),
        })),
        Statement::CommentOnIndex(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON INDEX".into(),
        })),
        Statement::CommentOnAggregate(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON AGGREGATE".into(),
        })),
        Statement::CommentOnFunction(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON FUNCTION".into(),
        })),
        Statement::CommentOnOperator(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON OPERATOR".into(),
        })),
        Statement::CommentOnLargeObject(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON LARGE OBJECT".into(),
        })),
        Statement::CommentOnType(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON TYPE".into(),
        })),
        Statement::CommentOnConstraint(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON CONSTRAINT".into(),
        })),
        Statement::CommentOnRule(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON RULE".into(),
        })),
        Statement::CommentOnTrigger(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON TRIGGER".into(),
        })),
        Statement::CommentOnEventTrigger(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON EVENT TRIGGER".into(),
        })),
        Statement::CommentOnDomain(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON DOMAIN".into(),
        })),
        Statement::CommentOnConversion(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON CONVERSION".into(),
        })),
        Statement::CommentOnDatabase(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON DATABASE".into(),
        })),
        Statement::CommentOnRole(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON ROLE".into(),
        })),
        Statement::CreateIndex(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE INDEX".into(),
        })),
        Statement::ReindexIndex(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "REINDEX".into(),
        })),
        Statement::Call(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "CALL execution".into(),
        ))),
        Statement::CreateFunction(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE FUNCTION".into(),
        })),
        Statement::CreateProcedure(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE PROCEDURE".into(),
        })),
        Statement::CreateAggregate(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE AGGREGATE".into(),
        })),
        Statement::AlterAggregateRename(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER AGGREGATE".into(),
        })),
        Statement::CreateCast(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE CAST".into(),
        })),
        Statement::CreateOperator(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE OPERATOR".into(),
        })),
        Statement::DropFunction(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP FUNCTION".into(),
        })),
        Statement::DropProcedure(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP PROCEDURE".into(),
        })),
        Statement::DropRoutine(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP ROUTINE".into(),
        })),
        Statement::DropAggregate(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP AGGREGATE".into(),
        })),
        Statement::DropCast(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP CAST".into(),
        })),
        Statement::DropOperator(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP OPERATOR".into(),
        })),
        Statement::CreateOperatorClass(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE OPERATOR CLASS".into(),
        })),
        Statement::CreateOperatorFamily(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE OPERATOR FAMILY".into(),
        })),
        Statement::AlterOperatorFamily(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER OPERATOR FAMILY".into(),
        })),
        Statement::AlterOperatorClass(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER OPERATOR CLASS".into(),
        })),
        Statement::DropOperatorFamily(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP OPERATOR FAMILY".into(),
        })),
        Statement::DropOperatorClass(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP OPERATOR CLASS".into(),
        })),
        Statement::CreateTextSearch(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE TEXT SEARCH".into(),
        })),
        Statement::AlterTextSearch(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER TEXT SEARCH".into(),
        })),
        Statement::AlterOperator(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER OPERATOR".into(),
        })),
        Statement::AlterConversion(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER CONVERSION".into(),
        })),
        Statement::AlterProcedure(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "ALTER PROCEDURE".into(),
        ))),
        Statement::AlterRoutine(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER ROUTINE".into(),
        })),
        Statement::CreateDatabase(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE DATABASE".into(),
        })),
        Statement::AlterDatabase(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER DATABASE".into(),
        })),
        Statement::CreateSchema(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE SCHEMA".into(),
        })),
        Statement::CreateConversion(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE CONVERSION".into(),
        })),
        Statement::CreateCollation(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE COLLATION".into(),
        })),
        Statement::CreateTrigger(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE TRIGGER".into(),
        })),
        Statement::CreateEventTrigger(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE EVENT TRIGGER".into(),
        })),
        Statement::DropTrigger(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP TRIGGER".into(),
        })),
        Statement::DropEventTrigger(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP EVENT TRIGGER".into(),
        })),
        Statement::CreateTablespace(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE TABLESPACE".into(),
        })),
        Statement::DropTablespace(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP TABLESPACE".into(),
        })),
        Statement::AlterTablespace(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER TABLESPACE".into(),
        })),
        Statement::AlterSchemaOwner(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER SCHEMA OWNER".into(),
        })),
        Statement::AlterSchemaRename(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER SCHEMA RENAME".into(),
        })),
        Statement::CreateDomain(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE DOMAIN".into(),
        })),
        Statement::AlterDomain(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER DOMAIN".into(),
        })),
        Statement::CreateType(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE TYPE".into(),
        })),
        Statement::AlterType(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER TYPE".into(),
        })),
        Statement::AlterTypeOwner(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER TYPE OWNER".into(),
        })),
        Statement::CreateView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE VIEW".into(),
        })),
        Statement::CreateRule(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE RULE".into(),
        })),
        Statement::AlterRuleRename(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER RULE".into(),
        })),
        Statement::AlterTableRuleState(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER TABLE".into(),
        })),
        Statement::Cluster(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CLUSTER".into(),
        })),
        Statement::Vacuum(stmt) => execute_vacuum(stmt, catalog, ctx),
        Statement::DropView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP VIEW".into(),
        })),
        Statement::DropRule(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP RULE".into(),
        })),
        Statement::DropDomain(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP DOMAIN".into(),
        })),
        Statement::DropConversion(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP CONVERSION".into(),
        })),
        Statement::DropCollation(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP COLLATION".into(),
        })),
        Statement::DropType(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP TYPE".into(),
        })),
        Statement::DropDatabase(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP DATABASE".into(),
        })),
        Statement::DropExtension(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP EXTENSION".into(),
        })),
        Statement::DropAccessMethod(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP ACCESS METHOD".into(),
        })),
        other => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: format!("{other:?}"),
        })),
    }
}

fn validate_readonly_create_statistics(
    stmt: &CreateStatisticsStatement,
    catalog: &dyn CatalogLookup,
) -> Result<StatementResult, ExecError> {
    validate_readonly_create_statistics_impl(
        &stmt.from_clause,
        &RestrictedViewCatalogAdapter(catalog),
    )
    .map(|()| StatementResult::AffectedRows(0))
    .map_err(|err| match err {
        ReadonlyCreateStatisticsError::UnexpectedEof => ExecError::Parse(ParseError::UnexpectedEof),
        ReadonlyCreateStatisticsError::UnsupportedFromClause => ExecError::DetailedError {
            message: "CREATE STATISTICS only supports relation names in the FROM clause".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        },
        ReadonlyCreateStatisticsError::UnknownTable(relation_name) => {
            ExecError::Parse(ParseError::UnknownTable(relation_name))
        }
        ReadonlyCreateStatisticsError::UnsupportedRelation {
            relation_name,
            relkind,
        } => unsupported_readonly_statistics_relation_error(&relation_name, relkind),
    })
}

fn unsupported_readonly_statistics_relation_error(relation_name: &str, relkind: char) -> ExecError {
    let base_name = relation_name
        .rsplit_once('.')
        .map(|(_, name)| name)
        .unwrap_or(relation_name)
        .trim_matches('"');
    let detail_kind = match relkind {
        'c' => "composite types",
        'f' => "foreign tables",
        'i' | 'I' => "indexes",
        'S' => "sequences",
        't' => "TOAST tables",
        'v' => "views",
        _ => "relations of this kind",
    };
    ExecError::DetailedError {
        message: format!("cannot define statistics for relation \"{base_name}\""),
        detail: Some(format!(
            "This operation is not supported for {detail_kind}."
        )),
        hint: None,
        sqlstate: "42809",
    }
}

pub fn exec_next<'a>(
    state: &'a mut super::PlanState,
    ctx: &mut ExecutorContext,
) -> Result<Option<&'a mut super::TupleSlot>, ExecError> {
    ctx.check_for_interrupts()?;
    state.exec_proc_node(ctx)
}
