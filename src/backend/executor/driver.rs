use super::{
    Catalog, ExecError, ExecutorContext, ParseError, Plan, PlannedStmt, QueryDesc, Statement,
    StatementResult, TransactionId, TupleSlot, Value, bind_delete, bind_insert, bind_update,
    create_query_desc, eval_expr, execute_analyze, execute_create_index, execute_create_table,
    execute_delete, execute_drop_table, execute_explain, execute_insert, execute_merge,
    execute_truncate_table, execute_update, execute_vacuum, executor_start, parse_statement,
    pg_plan_query, pg_plan_values_query,
};
use crate::backend::parser::{CatalogLookup, UnsupportedStatement, plan_merge};
use crate::pl::plpgsql::execute_do;

fn unsupported_statement_error(stmt: &UnsupportedStatement) -> ExecError {
    ExecError::Parse(ParseError::FeatureNotSupported(format!(
        "{}: {}",
        stmt.feature, stmt.sql
    )))
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
    let columns = query_desc.columns();
    let column_names = query_desc.column_names();
    let planned_stmt = query_desc.planned_stmt;
    let saved_subplans = std::mem::replace(&mut ctx.subplans, planned_stmt.subplans);
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
    ctx.cte_producers.clear();
    ctx.recursive_worktables.clear();
    let result = (|| {
        let mut state = executor_start(planned_stmt.plan_tree);
        let mut rows = Vec::new();
        while let Some(slot) = state.exec_proc_node(ctx)? {
            let mut values = slot.values()?.iter().cloned().collect::<Vec<_>>();
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
    ctx.subplans = saved_subplans;
    result
}

pub fn execute_plan(plan: Plan, ctx: &mut ExecutorContext) -> Result<StatementResult, ExecError> {
    execute_query_desc(
        create_query_desc(
            PlannedStmt {
                command_type: crate::include::executor::execdesc::CommandType::Select,
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
    let result = match stmt {
        Statement::Do(stmt) => execute_do(&stmt),
        Statement::Explain(stmt) => execute_explain(stmt, catalog, ctx),
        Statement::Select(stmt) => execute_query_desc(
            create_query_desc(pg_plan_query(&stmt, catalog)?, source_text.map(str::to_string)),
            ctx,
        ),
        Statement::Values(stmt) => execute_query_desc(
            create_query_desc(
                pg_plan_values_query(&stmt, catalog)?,
                source_text.map(str::to_string),
            ),
            ctx,
        ),
        Statement::Analyze(stmt) => execute_analyze(stmt, catalog),
        Statement::Show(_)
        | Statement::Checkpoint(_)
        | Statement::Set(_)
        | Statement::Reset(_)
        | Statement::SetRole(_)
        | Statement::ResetRole(_)
        | Statement::AlterTableAlterConstraint(_)
        | Statement::AlterTableAlterColumnDefault(_)
        // :HACK: ALTER TABLE ... SET (...) is accepted narrowly for numeric.sql and ignored
        // until table reloptions are modeled for real.
        | Statement::AlterTableSet(_)
        | Statement::AlterTableSetRowSecurity(_) => Ok(StatementResult::AffectedRows(0)),
        Statement::CopyFrom(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COPY handled by session layer",
            actual: "COPY".into(),
        })),
        Statement::CreateTrigger(_) | Statement::DropTrigger(_) => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "TRIGGER handled by database/session layer",
                actual: "TRIGGER".into(),
            }))
        }
        Statement::AlterTableRename(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER TABLE RENAME handled by database/session layer",
            actual: "ALTER TABLE RENAME".into(),
        })),
        Statement::AlterTableOwner(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER TABLE OWNER handled by database/session layer",
            actual: "ALTER TABLE OWNER".into(),
        })),
        Statement::AlterTableRenameColumn(_) => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "ALTER TABLE RENAME COLUMN handled by database/session layer",
                actual: "ALTER TABLE RENAME COLUMN".into(),
            }))
        }
        Statement::AlterTableAddColumn(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER TABLE ADD COLUMN handled by database/session layer",
            actual: "ALTER TABLE ADD COLUMN".into(),
        })),
        Statement::AlterTableAddConstraint(_)
        | Statement::AlterTableDropConstraint(_)
        | Statement::AlterTableRenameConstraint(_)
        | Statement::AlterTableSetNotNull(_)
        | Statement::AlterTableDropNotNull(_)
        | Statement::AlterTableValidateConstraint(_) => {
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
        Statement::AlterSchemaOwner(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER SCHEMA OWNER handled by database/session layer",
            actual: "ALTER SCHEMA OWNER".into(),
        })),
        Statement::CommentOnTable(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON TABLE handled by database/session layer",
            actual: "COMMENT ON TABLE".into(),
        })),
        Statement::CommentOnRule(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON RULE handled by database/session layer",
            actual: "COMMENT ON RULE".into(),
        })),
        Statement::CommentOnDomain(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON DOMAIN handled by database/session layer",
            actual: "COMMENT ON DOMAIN".into(),
        })),
        Statement::CommentOnConversion(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON CONVERSION handled by database/session layer",
            actual: "COMMENT ON CONVERSION".into(),
        })),
        Statement::CommentOnRole(_)
        | Statement::CreateRole(_)
        | Statement::AlterRole(_)
        | Statement::DropRole(_)
        | Statement::GrantObject(_)
        | Statement::RevokeObject(_)
        | Statement::GrantRoleMembership(_)
        | Statement::RevokeRoleMembership(_)
        | Statement::SetSessionAuthorization(_)
        | Statement::ResetSessionAuthorization(_)
        | Statement::ReassignOwned(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "role management".into(),
        ))),
        Statement::CreateIndex(stmt) => execute_create_index(stmt, catalog, ctx),
        Statement::CreateFunction(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE FUNCTION handled by database/session layer",
            actual: "CREATE FUNCTION".into(),
        })),
        Statement::CreateOperatorClass(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE OPERATOR CLASS handled by database/session layer",
            actual: "CREATE OPERATOR CLASS".into(),
        })),
        Statement::CreateDatabase(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE DATABASE handled by database/session layer",
            actual: "CREATE DATABASE".into(),
        })),
        Statement::CreateSchema(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE SCHEMA handled by database/session layer",
            actual: "CREATE SCHEMA".into(),
        })),
        Statement::CreateTablespace(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE TABLESPACE handled by database/session layer",
            actual: "CREATE TABLESPACE".into(),
        })),
        Statement::CreateDomain(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE DOMAIN handled by database/session layer",
            actual: "CREATE DOMAIN".into(),
        })),
        Statement::CreateConversion(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE CONVERSION handled by database/session layer",
            actual: "CREATE CONVERSION".into(),
        })),
        Statement::CreateType(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE TYPE handled by database/session layer",
            actual: "CREATE TYPE".into(),
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
        Statement::Unsupported(stmt) => Err(unsupported_statement_error(&stmt)),
        Statement::Begin | Statement::Commit | Statement::Rollback => {
            Err(ExecError::Parse(ParseError::UnexpectedToken {
                expected: "non-transaction-control statement",
                actual: "BEGIN/COMMIT/ROLLBACK".into(),
            }))
        }
    };
    ctx.next_command_id = ctx.next_command_id.saturating_add(1);
    result
}

pub fn execute_readonly_statement(
    stmt: Statement,
    catalog: &dyn CatalogLookup,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    match stmt {
        Statement::Do(stmt) => execute_do(&stmt),
        Statement::Explain(stmt) => execute_explain(stmt, catalog, ctx),
        Statement::Select(stmt) => execute_planned_stmt(pg_plan_query(&stmt, catalog)?, ctx),
        Statement::Values(stmt) => execute_planned_stmt(pg_plan_values_query(&stmt, catalog)?, ctx),
        Statement::Analyze(stmt) => execute_analyze(stmt, catalog),
        Statement::Show(_)
        | Statement::Set(_)
        | Statement::Reset(_)
        | Statement::AlterTableSet(_)
        | Statement::AlterTableRenameColumn(_)
        | Statement::AlterTableAddColumn(_)
        | Statement::AlterTableDropColumn(_)
        | Statement::AlterTableAlterColumnType(_)
        | Statement::AlterTableAlterColumnDefault(_) => Ok(StatementResult::AffectedRows(0)),
        Statement::AlterTableRename(_) => Ok(StatementResult::AffectedRows(0)),
        Statement::Merge(_) => Err(ExecError::Parse(ParseError::FeatureNotSupported(
            "MERGE".into(),
        ))),
        Statement::Unsupported(stmt) => Err(unsupported_statement_error(&stmt)),
        Statement::CommentOnTable(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON TABLE".into(),
        })),
        Statement::CommentOnRule(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON RULE".into(),
        })),
        Statement::CommentOnDomain(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON DOMAIN".into(),
        })),
        Statement::CommentOnConversion(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON CONVERSION".into(),
        })),
        Statement::CommentOnRole(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON ROLE".into(),
        })),
        Statement::CreateIndex(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE INDEX".into(),
        })),
        Statement::CreateFunction(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE FUNCTION".into(),
        })),
        Statement::CreateOperatorClass(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE OPERATOR CLASS".into(),
        })),
        Statement::CreateDatabase(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE DATABASE".into(),
        })),
        Statement::CreateSchema(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE SCHEMA".into(),
        })),
        Statement::CreateConversion(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE CONVERSION".into(),
        })),
        Statement::CreateTrigger(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE TRIGGER".into(),
        })),
        Statement::DropTrigger(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP TRIGGER".into(),
        })),
        Statement::CreateTablespace(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE TABLESPACE".into(),
        })),
        Statement::AlterSchemaOwner(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "ALTER SCHEMA OWNER".into(),
        })),
        Statement::CreateDomain(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE DOMAIN".into(),
        })),
        Statement::CreateType(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE TYPE".into(),
        })),
        Statement::CreateView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE VIEW".into(),
        })),
        Statement::CreateRule(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE RULE".into(),
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
        Statement::DropType(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP TYPE".into(),
        })),
        Statement::DropDatabase(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP DATABASE".into(),
        })),
        other => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: format!("{other:?}"),
        })),
    }
}

pub fn exec_next<'a>(
    state: &'a mut super::PlanState,
    ctx: &mut ExecutorContext,
) -> Result<Option<&'a mut super::TupleSlot>, ExecError> {
    ctx.check_for_interrupts()?;
    state.exec_proc_node(ctx)
}
