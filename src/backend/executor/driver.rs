use super::{
    Catalog, ExecError, ExecutorContext, ParseError, Plan, PlannedStmt, QueryDesc, Statement,
    StatementResult, TransactionId, Value, bind_delete, bind_insert, bind_update,
    create_query_desc, execute_analyze, execute_create_index, execute_create_table, execute_delete,
    execute_drop_table, execute_explain, execute_insert, execute_truncate_table, execute_update,
    execute_vacuum, executor_start, parse_statement, pg_plan_query, pg_plan_values_query,
};
use crate::backend::parser::CatalogLookup;
use crate::backend::parser::UnsupportedStatement;
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
    let saved_subplans = std::mem::replace(&mut ctx.subplans, query_desc.planned_stmt.subplans);
    let result = (|| {
        let mut state = executor_start(query_desc.planned_stmt.plan_tree);
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
        | Statement::Set(_)
        | Statement::Reset(_)
        // :HACK: ALTER TABLE ... SET (...) is accepted narrowly for numeric.sql and ignored
        // until table reloptions are modeled for real.
        | Statement::AlterTableSet(_) => Ok(StatementResult::AffectedRows(0)),
        Statement::CopyFrom(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COPY handled by session layer",
            actual: "COPY".into(),
        })),
        Statement::AlterTableRename(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER TABLE RENAME handled by database/session layer",
            actual: "ALTER TABLE RENAME".into(),
        })),
        Statement::AlterTableAddColumn(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "ALTER TABLE ADD COLUMN handled by database/session layer",
            actual: "ALTER TABLE ADD COLUMN".into(),
        })),
        Statement::CommentOnTable(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "COMMENT ON TABLE handled by database/session layer",
            actual: "COMMENT ON TABLE".into(),
        })),
        Statement::CreateIndex(stmt) => execute_create_index(stmt, catalog, ctx),
        Statement::CreateTable(stmt) => execute_create_table(stmt, catalog),
        Statement::CreateTableAs(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "create table handled by database/session layer",
            actual: "CREATE TABLE AS".into(),
        })),
        Statement::CreateView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "CREATE VIEW handled by database/session layer",
            actual: "CREATE VIEW".into(),
        })),
        Statement::DropTable(stmt) => execute_drop_table(stmt, catalog, ctx),
        Statement::DropView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "DROP VIEW handled by database/session layer",
            actual: "DROP VIEW".into(),
        })),
        Statement::TruncateTable(stmt) => execute_truncate_table(stmt, catalog, ctx, xid),
        Statement::Vacuum(stmt) => execute_vacuum(stmt, catalog),
        Statement::Insert(stmt) => {
            execute_insert(bind_insert(&stmt, catalog)?, catalog, ctx, xid, cid)
        }
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
        | Statement::AlterTableAddColumn(_) => Ok(StatementResult::AffectedRows(0)),
        Statement::AlterTableRename(_) => Ok(StatementResult::AffectedRows(0)),
        Statement::Unsupported(stmt) => Err(unsupported_statement_error(&stmt)),
        Statement::CommentOnTable(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "COMMENT ON TABLE".into(),
        })),
        Statement::CreateIndex(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE INDEX".into(),
        })),
        Statement::CreateView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "CREATE VIEW".into(),
        })),
        Statement::Vacuum(stmt) => execute_vacuum(stmt, catalog),
        Statement::DropView(_) => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: "DROP VIEW".into(),
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
    state.exec_proc_node(ctx)
}
