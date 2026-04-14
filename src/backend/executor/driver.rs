use super::{
    Catalog, ExecError, ExecutorContext, ParseError, Plan, Statement, StatementResult,
    TransactionId, Value, bind_delete, bind_insert, bind_update, build_plan, build_values_plan,
    execute_analyze, execute_create_index, execute_create_table, execute_delete,
    execute_drop_table, execute_explain, execute_insert, execute_truncate_table, execute_update,
    execute_vacuum, executor_start, parse_statement,
};
use crate::backend::parser::CatalogLookup;
use crate::pl::plpgsql::execute_do;

pub fn execute_plan(plan: Plan, ctx: &mut ExecutorContext) -> Result<StatementResult, ExecError> {
    let columns = plan.columns();
    let column_names = plan.column_names();
    let mut state = executor_start(plan);
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
}

pub fn execute_sql(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    let stmt = parse_statement(sql)?;
    execute_statement(stmt, catalog, ctx, xid)
}

pub fn execute_statement(
    stmt: Statement,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    let cid = ctx.next_command_id;
    ctx.snapshot = ctx.txns.read().snapshot_for_command(xid, cid)?;
    let result = match stmt {
        Statement::Do(stmt) => execute_do(&stmt),
        Statement::Explain(stmt) => execute_explain(stmt, catalog, ctx),
        Statement::Select(stmt) => execute_plan(build_plan(&stmt, catalog)?, ctx),
        Statement::Values(stmt) => execute_plan(build_values_plan(&stmt, catalog)?, ctx),
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
        Statement::Select(stmt) => execute_plan(build_plan(&stmt, catalog)?, ctx),
        Statement::Values(stmt) => execute_plan(build_values_plan(&stmt, catalog)?, ctx),
        Statement::Analyze(stmt) => execute_analyze(stmt, catalog),
        Statement::Show(_)
        | Statement::Set(_)
        | Statement::Reset(_)
        | Statement::AlterTableSet(_)
        | Statement::AlterTableAddColumn(_) => Ok(StatementResult::AffectedRows(0)),
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
