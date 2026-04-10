pub mod nodes;
pub mod expr;
pub(crate) mod tuple_decoder;
mod explain;
pub mod commands;

pub use nodes::*;
pub use expr::eval_expr;

use crate::access::heap::am::HeapError;
use crate::access::heap::mvcc::{CommandId, MvccError, Snapshot, TransactionId, TransactionManager};
use crate::access::heap::tuple::TupleError;
use crate::catalog::Catalog;
use crate::parser::{
    ParseError, Statement, bind_delete, bind_insert, bind_update, build_plan, parse_statement,
};
use crate::{BufferPool, ClientId, SmgrStorageBackend};

use std::cmp::Ordering;
use std::rc::Rc;

use expr::compare_order_values;
use commands::*;

pub struct ExecutorContext {
    pub pool: std::sync::Arc<BufferPool<SmgrStorageBackend>>,
    pub txns: std::sync::Arc<parking_lot::RwLock<TransactionManager>>,
    pub snapshot: Snapshot,
    pub client_id: ClientId,
    pub next_command_id: CommandId,
    pub outer_rows: Vec<Vec<Value>>,
    /// When true, each node records per-node timing stats (for EXPLAIN ANALYZE).
    pub timed: bool,
}

#[derive(Debug)]
pub enum ExecError {
    Heap(HeapError),
    Tuple(TupleError),
    Parse(ParseError),
    InvalidColumn(usize),
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    NonBoolQual(Value),
    UnsupportedStorageType {
        column: String,
        ty: ScalarType,
        attlen: i16,
    },
    InvalidStorageValue {
        column: String,
        details: String,
    },
    StringDataRightTruncation {
        ty: String,
    },
    CardinalityViolation(String),
    UnboundOuterColumn {
        depth: usize,
        index: usize,
    },
    MissingRequiredColumn(String),
    InvalidRegex(String),
}

impl From<HeapError> for ExecError {
    fn from(value: HeapError) -> Self {
        Self::Heap(value)
    }
}

impl From<TupleError> for ExecError {
    fn from(value: TupleError) -> Self {
        Self::Tuple(value)
    }
}

impl From<ParseError> for ExecError {
    fn from(value: ParseError) -> Self {
        Self::Parse(value)
    }
}

impl From<MvccError> for ExecError {
    fn from(value: MvccError) -> Self {
        Self::Heap(HeapError::Mvcc(value))
    }
}

use std::collections::HashSet;

#[derive(Debug, Clone)]
pub(crate) enum AccumState {
    Count { count: i64 },
    CountDistinct { seen: HashSet<Value> },
    Sum { sum: Option<i64> },
    Avg { sum: Option<i64>, count: i64 },
    Min { min: Option<Value> },
    Max { max: Option<Value> },
}

impl AccumState {
    pub(crate) fn new(func: AggFunc, distinct: bool) -> Self {
        match (func, distinct) {
            (AggFunc::Count, true) => AccumState::CountDistinct { seen: HashSet::new() },
            (AggFunc::Count, false) => AccumState::Count { count: 0 },
            (AggFunc::Sum, _) => AccumState::Sum { sum: None },
            (AggFunc::Avg, _) => AccumState::Avg { sum: None, count: 0 },
            (AggFunc::Min, _) => AccumState::Min { min: None },
            (AggFunc::Max, _) => AccumState::Max { max: None },
        }
    }

/// Return a compiled transition function resolved at plan time, like PG's
    /// aggregate transition functions (e.g. int8inc_any for count(*)). Avoids
    /// per-tuple enum match dispatch in the hot loop.
    pub(crate) fn transition_fn(func: AggFunc, has_arg: bool, distinct: bool) -> fn(&mut AccumState, &Value) {
        match (func, has_arg, distinct) {
            (AggFunc::Count, _, true) => |state, value| {
                if let AccumState::CountDistinct { seen } = state {
                    if !matches!(value, Value::Null) {
                        seen.insert(value.to_owned_value());
                    }
                }
            },
            (AggFunc::Count, false, false) => |state, _value| {
                // count(*): unconditional increment, like PG's int8inc_any
                if let AccumState::Count { count } = state { *count += 1; }
            },
            (AggFunc::Count, true, false) => |state, value| {
                if let AccumState::Count { count } = state {
                    if !matches!(value, Value::Null) { *count += 1; }
                }
            },
            (AggFunc::Sum, _, _) => |state, value| {
                if let AccumState::Sum { sum } = state {
                    if let Value::Int32(v) = value {
                        *sum = Some(sum.unwrap_or(0) + *v as i64);
                    }
                }
            },
            (AggFunc::Avg, _, _) => |state, value| {
                if let AccumState::Avg { sum, count } = state {
                    if let Value::Int32(v) = value {
                        *sum = Some(sum.unwrap_or(0) + *v as i64);
                        *count += 1;
                    }
                }
            },
            (AggFunc::Min, _, _) => |state, value| {
                if let AccumState::Min { min } = state {
                    if !matches!(value, Value::Null) {
                        *min = Some(match min.take() {
                            None => value.clone(),
                            Some(current) => {
                                if compare_order_values(value, &current, None, false) == Ordering::Less {
                                    value.clone()
                                } else {
                                    current
                                }
                            }
                        });
                    }
                }
            },
            (AggFunc::Max, _, _) => |state, value| {
                if let AccumState::Max { max } = state {
                    if !matches!(value, Value::Null) {
                        *max = Some(match max.take() {
                            None => value.clone(),
                            Some(current) => {
                                if compare_order_values(value, &current, None, false) == Ordering::Greater {
                                    value.clone()
                                } else {
                                    current
                                }
                            }
                        });
                    }
                }
            },
        }
    }

    pub(crate) fn finalize(&self) -> Value {
        match self {
            AccumState::Count { count } => Value::Int32(*count as i32),
            AccumState::CountDistinct { seen } => Value::Int32(seen.len() as i32),
            AccumState::Sum { sum } => match sum {
                Some(v) => Value::Int32(*v as i32),
                None => Value::Null,
            },
            AccumState::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    match sum {
                        Some(v) => Value::Int32((*v / *count) as i32),
                        None => Value::Null,
                    }
                }
            }
            AccumState::Min { min } => min.clone().unwrap_or(Value::Null),
            AccumState::Max { max } => max.clone().unwrap_or(Value::Null),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AggGroup {
    pub(crate) key_values: Vec<Value>,
    pub(crate) accum_states: Vec<AccumState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementResult {
    Query {
        columns: Vec<QueryColumn>,
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    AffectedRows(usize),
}

pub fn executor_start(plan: Plan) -> PlanState {
    match plan {
        Plan::Result => Box::new(ResultState {
            emitted: false,
            slot: TupleSlot::empty(0),
            stats: NodeExecStats::default(),
        }),
        Plan::SeqScan { rel, desc } => {
            let column_names: Vec<String> = desc.columns.iter().map(|c| c.name.clone()).collect();
            let attr_descs = desc.attribute_descs();
            let decoder = Rc::new(tuple_decoder::CompiledTupleDecoder::compile(&desc, &attr_descs));
            let ncols = desc.columns.len();
            let mut slot = TupleSlot::empty(ncols);
            slot.decoder = Some(decoder);
            Box::new(SeqScanState {
                rel,
                column_names,
                scan: None,
                slot,
                qual: None,
                stats: NodeExecStats::default(),
            })
        }
        Plan::NestedLoopJoin { left, right, on } => {
            let combined_names: Vec<String> = left.column_names().into_iter()
                .chain(right.column_names())
                .collect();
            let ncols = combined_names.len();
            Box::new(NestedLoopJoinState {
                left: executor_start(*left),
                right: executor_start(*right),
                on,
                combined_names,
                right_rows: None,
                current_left: None,
                right_index: 0,
                slot: TupleSlot::empty(ncols),
                stats: NodeExecStats::default(),
            })
        }
        // Like PG's ExecSeqScanWithQual: push the qual into the scan node
        // so tuples that fail never leave the scan (no vtable dispatch).
        Plan::Filter { input, predicate } if matches!(&*input, Plan::SeqScan { .. }) => {
            let Plan::SeqScan { rel, desc } = *input else { unreachable!() };
            let column_names: Vec<String> = desc.columns.iter().map(|c| c.name.clone()).collect();
            let attr_descs = desc.attribute_descs();
            let decoder = Rc::new(tuple_decoder::CompiledTupleDecoder::compile(&desc, &attr_descs));
            let qual = expr::compile_predicate_with_decoder(&predicate, &decoder);
            let ncols = desc.columns.len();
            let mut slot = TupleSlot::empty(ncols);
            slot.decoder = Some(decoder);
            Box::new(SeqScanState {
                rel,
                column_names,
                scan: None,
                slot,
                qual: Some(qual),
                stats: NodeExecStats::default(),
            })
        }
        Plan::Filter { input, predicate } => {
            let compiled_predicate = expr::compile_predicate(&predicate);
            Box::new(FilterState {
                input: executor_start(*input),
                predicate,
                compiled_predicate,
                stats: NodeExecStats::default(),
            })
        }
        Plan::OrderBy { input, items } => Box::new(OrderByState {
            input: executor_start(*input),
            items,
            rows: None,
            next_index: 0,
            stats: NodeExecStats::default(),
        }),
        Plan::Limit {
            input,
            limit,
            offset,
        } => Box::new(LimitState {
            input: executor_start(*input),
            limit,
            offset,
            skipped: 0,
            returned: 0,
            stats: NodeExecStats::default(),
        }),
        Plan::Projection { input, targets } => {
            let column_names: Vec<String> = targets.iter().map(|t| t.name.clone()).collect();
            let ncols = column_names.len();
            Box::new(ProjectionState {
                input: executor_start(*input),
                targets,
                column_names,
                slot: TupleSlot::empty(ncols),
                stats: NodeExecStats::default(),
            })
        }
        Plan::Aggregate {
            input,
            group_by,
            accumulators,
            having,
            output_columns,
        } => {
            let output_column_names = output_columns.iter().map(|c| c.name.clone()).collect();
            let key_buffer = Vec::with_capacity(group_by.len());
            let trans_fns: Vec<fn(&mut AccumState, &Value)> = accumulators
                .iter()
                .map(|a| AccumState::transition_fn(a.func, a.arg.is_some(), a.distinct))
                .collect();
            Box::new(AggregateState {
                input: executor_start(*input),
                group_by,
                accumulators,
                having,
                output_columns: output_column_names,
                result_rows: None,
                next_index: 0,
                key_buffer,
                trans_fns,
                stats: NodeExecStats::default(),
            })
        }
        Plan::GenerateSeries { start, stop, step, output } => {
            Box::new(GenerateSeriesState {
                start,
                stop,
                step,
                current: 0,
                end: 0,
                step_val: 0,
                initialized: false,
                slot: TupleSlot::empty(1),
                column_names: vec![output.name],
                stats: NodeExecStats::default(),
            })
        }
        Plan::Unnest { args, output_columns } => {
            let column_names = output_columns.into_iter().map(|c| c.name).collect();
            Box::new(UnnestState {
                args,
                output_columns: column_names,
                rows: None,
                next_index: 0,
                stats: NodeExecStats::default(),
            })
        }
    }
}

pub fn execute_plan(
    plan: Plan,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    let columns = plan.columns();
    let column_names = plan.column_names();
    let mut state = executor_start(plan);
    let mut rows = Vec::new();
    while let Some(slot) = state.exec_proc_node(ctx)? {
        rows.push(slot.values()?.iter().map(|v| v.to_owned_value()).collect());
    }
    Ok(StatementResult::Query { columns, column_names, rows })
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
        Statement::Explain(stmt) => execute_explain(stmt, catalog, ctx),
        Statement::Select(stmt) => execute_plan(build_plan(&stmt, catalog)?, ctx),
        Statement::ShowTables => execute_show_tables(catalog),
        Statement::CreateTable(stmt) => execute_create_table(stmt, catalog),
        Statement::DropTable(stmt) => execute_drop_table(stmt, catalog, ctx),
        Statement::TruncateTable(stmt) => execute_truncate_table(stmt, catalog, ctx),
        Statement::Vacuum(_) => Ok(StatementResult::AffectedRows(0)),
        Statement::Insert(stmt) => execute_insert(bind_insert(&stmt, catalog)?, ctx, xid, cid),
        Statement::Update(stmt) => execute_update(bind_update(&stmt, catalog)?, ctx, xid, cid),
        Statement::Delete(stmt) => execute_delete(bind_delete(&stmt, catalog)?, ctx, xid),
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

/// Execute a read-only statement (SELECT, EXPLAIN, SHOW) with only a shared catalog reference.
pub fn execute_readonly_statement(
    stmt: Statement,
    catalog: &Catalog,
    ctx: &mut ExecutorContext,
) -> Result<StatementResult, ExecError> {
    match stmt {
        Statement::Explain(stmt) => execute_explain(stmt, catalog, ctx),
        Statement::Select(stmt) => execute_plan(build_plan(&stmt, catalog)?, ctx),
        Statement::ShowTables => execute_show_tables(catalog),
        other => Err(ExecError::Parse(ParseError::UnexpectedToken {
            expected: "read-only statement",
            actual: format!("{other:?}"),
        })),
    }
}

pub fn exec_next<'a>(
    state: &'a mut PlanState,
    ctx: &mut ExecutorContext,
) -> Result<Option<&'a mut TupleSlot>, ExecError> {
    state.exec_proc_node(ctx)
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::heap::am::{heap_flush, heap_insert_mvcc, heap_update};
    use crate::access::heap::mvcc::INVALID_TRANSACTION_ID;
    use crate::access::heap::tuple::TupleValue;
    use crate::parser::{Catalog, CatalogEntry};
    use crate::storage::smgr::{ForkNumber, MdStorageManager, StorageManager};
    use crate::access::heap::tuple::{AttributeDesc, HeapTuple};
    use crate::RelFileLocator;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_executor_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn rel() -> RelFileLocator {
        RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: 14000,
        }
    }

    fn pets_rel() -> RelFileLocator {
        RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: 14001,
        }
    }

    fn relation_desc() -> RelationDesc {
        RelationDesc {
            columns: vec![
                crate::catalog::column_desc("id", crate::parser::SqlType::new(crate::parser::SqlTypeKind::Int4), false),
                crate::catalog::column_desc("name", crate::parser::SqlType::new(crate::parser::SqlTypeKind::Text), false),
                crate::catalog::column_desc("note", crate::parser::SqlType::new(crate::parser::SqlTypeKind::Text), true),
            ],
        }
    }

    fn catalog() -> Catalog {
        let mut catalog = Catalog::default();
        catalog.insert(
            "people",
            CatalogEntry {
                rel: rel(),
                desc: relation_desc(),
            },
        );
        catalog
    }

    fn pets_relation_desc() -> RelationDesc {
        RelationDesc {
            columns: vec![
                crate::catalog::column_desc("id", crate::parser::SqlType::new(crate::parser::SqlTypeKind::Int4), false),
                crate::catalog::column_desc("name", crate::parser::SqlType::new(crate::parser::SqlTypeKind::Text), false),
                crate::catalog::column_desc("owner_id", crate::parser::SqlType::new(crate::parser::SqlTypeKind::Int4), true),
            ],
        }
    }

    fn catalog_with_pets() -> Catalog {
        let mut catalog = catalog();
        catalog.insert(
            "pets",
            CatalogEntry {
                rel: pets_rel(),
                desc: pets_relation_desc(),
            },
        );
        catalog
    }

    fn varchar_catalog(name: &str, len: i32) -> Catalog {
        let mut catalog = Catalog::default();
        catalog.insert(
            name,
            CatalogEntry {
                rel: crate::RelFileLocator {
                    spc_oid: 0,
                    db_oid: 1,
                    rel_number: 15002,
                },
                desc: RelationDesc {
                    columns: vec![crate::catalog::column_desc(
                        "name",
                        crate::parser::SqlType::with_char_len(crate::parser::SqlTypeKind::Varchar, len),
                        false,
                    )],
                },
            },
        );
        catalog
    }

    fn shipments_rel() -> RelFileLocator {
        RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: 15003,
        }
    }

    fn shipments_relation_desc() -> RelationDesc {
        use crate::parser::{SqlType, SqlTypeKind};

        RelationDesc {
            columns: vec![
                crate::catalog::column_desc("id", SqlType::new(SqlTypeKind::Int4), false),
                crate::catalog::column_desc("company_id", SqlType::new(SqlTypeKind::Text), false),
                crate::catalog::column_desc("year", SqlType::new(SqlTypeKind::Text), false),
                crate::catalog::column_desc("container_numbers", SqlType::array_of(SqlType::new(SqlTypeKind::Varchar)), true),
                crate::catalog::column_desc("container_types_categories", SqlType::array_of(SqlType::new(SqlTypeKind::Varchar)), true),
                crate::catalog::column_desc("container_size_categories", SqlType::array_of(SqlType::new(SqlTypeKind::Varchar)), true),
            ],
        }
    }

    fn shipments_catalog() -> Catalog {
        let mut catalog = Catalog::default();
        catalog.insert(
            "om_shipments",
            CatalogEntry {
                rel: shipments_rel(),
                desc: shipments_relation_desc(),
            },
        );
        catalog
    }

    fn tuple(id: i32, name: &str, note: Option<&str>) -> HeapTuple {
        let desc = relation_desc().attribute_descs();
        HeapTuple::from_values(
            &desc,
            &[
                TupleValue::Bytes(id.to_le_bytes().to_vec()),
                TupleValue::Bytes(name.as_bytes().to_vec()),
                match note {
                    Some(note) => TupleValue::Bytes(note.as_bytes().to_vec()),
                    None => TupleValue::Null,
                },
            ],
        )
        .unwrap()
    }

    fn pet_tuple(id: i32, name: &str, owner_id: i32) -> HeapTuple {
        let desc = pets_relation_desc().attribute_descs();
        HeapTuple::from_values(
            &desc,
            &[
                TupleValue::Bytes(id.to_le_bytes().to_vec()),
                TupleValue::Bytes(name.as_bytes().to_vec()),
                TupleValue::Bytes(owner_id.to_le_bytes().to_vec()),
            ],
        )
        .unwrap()
    }

    /// Test-only: create the storage fork for a relation.
    fn create_fork(pool: &BufferPool<SmgrStorageBackend>, rel: RelFileLocator) {
        pool.with_storage_mut(|s| {
            s.smgr.open(rel).unwrap();
            match s.smgr.create(rel, ForkNumber::Main, false) {
                Ok(()) => {}
                Err(crate::storage::smgr::SmgrError::AlreadyExists { .. }) => {}
                Err(e) => panic!("create_fork failed: {e:?}"),
            }
        });
    }

    /// Test-only: create a buffer pool with the "people" table fork ready.
    fn test_pool(base: &PathBuf) -> std::sync::Arc<BufferPool<SmgrStorageBackend>> {
        let smgr = MdStorageManager::new(base);
        let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
        create_fork(&*pool, rel());
        pool
    }

    /// Test-only: create a buffer pool with both "people" and "pets" forks ready.
    fn test_pool_with_pets(base: &PathBuf) -> std::sync::Arc<BufferPool<SmgrStorageBackend>> {
        let pool = test_pool(base);
        create_fork(&*pool, pets_rel());
        pool
    }

    fn empty_executor_context(base: &PathBuf) -> ExecutorContext {
        let txns = TransactionManager::new_durable(base).unwrap();
        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        ExecutorContext {
            pool: test_pool(base),
            txns: std::sync::Arc::new(parking_lot::RwLock::new(txns)),
            snapshot,
            client_id: 1,
            next_command_id: 0,
            outer_rows: Vec::new(),
            timed: false,
        }
    }

    fn run_plan(
        base: &PathBuf,
        txns: &TransactionManager,
        plan: Plan,
    ) -> Result<Vec<(Vec<String>, Vec<Value>)>, ExecError> {
        let pool = test_pool(base);
        let txns_arc = std::sync::Arc::new(parking_lot::RwLock::new(txns.clone()));
        let mut state = executor_start(plan);
        let mut ctx = ExecutorContext {
            pool,
            txns: txns_arc,
            snapshot: txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
            client_id: 42,
            next_command_id: 0,
            outer_rows: Vec::new(),
            timed: false,
        };

        let names = state.column_names().to_vec();
        let mut rows = Vec::new();
        while let Some(slot) = exec_next(&mut state, &mut ctx)? {
            rows.push((names.clone(), slot.values()?.iter().map(|v| v.to_owned_value()).collect()));
        }
        Ok(rows)
    }

    fn run_sql(
        base: &PathBuf,
        txns: &TransactionManager,
        xid: TransactionId,
        sql: &str,
    ) -> Result<StatementResult, ExecError> {
        run_sql_with_catalog(base, txns, xid, sql, catalog())
    }

    fn run_sql_with_catalog(
        base: &PathBuf,
        txns: &TransactionManager,
        xid: TransactionId,
        sql: &str,
        mut catalog: Catalog,
    ) -> Result<StatementResult, ExecError> {
        let smgr = MdStorageManager::new(base);
        let pool = std::sync::Arc::new(BufferPool::new(SmgrStorageBackend::new(smgr), 8));
        for name in catalog.table_names().collect::<Vec<_>>() {
            if let Some(entry) = catalog.get(&name) {
                create_fork(&*pool, entry.rel);
            }
        }
        let txns_arc = std::sync::Arc::new(parking_lot::RwLock::new(txns.clone()));
        let mut ctx = ExecutorContext {
            pool,
            txns: txns_arc,
            snapshot: txns.snapshot(xid).unwrap(),
            client_id: 77,
            next_command_id: 0,
            outer_rows: Vec::new(),
            timed: false,
        };
        execute_sql(sql, &mut catalog, &mut ctx, xid)
    }

    fn assert_query_rows(result: StatementResult, expected: Vec<Vec<Value>>) {
        match result {
            StatementResult::Query { rows, .. } => assert_eq!(rows, expected),
            other => panic!("expected query result, got {:?}", other),
        }
    }

    fn seed_people_and_pets(base: &PathBuf, txns: &mut TransactionManager) {
        let xid = txns.begin();
        run_sql_with_catalog(
            base,
            txns,
            xid,
            "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'b'), (3, 'carol', null)",
            catalog_with_pets(),
        )
        .unwrap();
        run_sql_with_catalog(
            base,
            txns,
            xid,
            "insert into pets (id, name, owner_id) values (10, 'mocha', 1), (11, 'pixel', 1), (12, 'otis', 2), (13, 'stray', null)",
            catalog_with_pets(),
        )
        .unwrap();
        txns.commit(xid).unwrap();
    }

    #[test]
    fn expr_eval_obeys_null_semantics() {
        let base = temp_dir("expr_eval_obeys_null_semantics");
        let mut ctx = empty_executor_context(&base);
        let mut slot = TupleSlot::virtual_row(
            vec![Value::Int32(7), Value::Text("alice".into()), Value::Null],
        );
        assert_eq!(eval_expr(&Expr::Eq(Box::new(Expr::Column(0)), Box::new(Expr::Const(Value::Int32(7)))), &mut slot, &mut ctx).unwrap(), Value::Bool(true));
        assert_eq!(eval_expr(&Expr::Eq(Box::new(Expr::Column(2)), Box::new(Expr::Const(Value::Text("x".into())))), &mut slot, &mut ctx).unwrap(), Value::Null);
        assert_eq!(eval_expr(&Expr::And(Box::new(Expr::Const(Value::Bool(true))), Box::new(Expr::Const(Value::Null))), &mut slot, &mut ctx).unwrap(), Value::Null);
        assert_eq!(eval_expr(&Expr::IsNull(Box::new(Expr::Column(2))), &mut slot, &mut ctx).unwrap(), Value::Bool(true));
        assert_eq!(eval_expr(&Expr::IsNotNull(Box::new(Expr::Column(2))), &mut slot, &mut ctx).unwrap(), Value::Bool(false));
        assert_eq!(eval_expr(&Expr::IsDistinctFrom(Box::new(Expr::Column(2)), Box::new(Expr::Const(Value::Null))), &mut slot, &mut ctx).unwrap(), Value::Bool(false));
        assert_eq!(eval_expr(&Expr::IsDistinctFrom(Box::new(Expr::Column(1)), Box::new(Expr::Const(Value::Null))), &mut slot, &mut ctx).unwrap(), Value::Bool(true));
    }

    #[test]
    fn physical_slot_lazily_deforms_heap_tuple() {
        use crate::access::heap::tuple::ItemPointerData;
        let desc = Rc::new(relation_desc());
        let attr_descs: Rc<[AttributeDesc]> = desc.attribute_descs().into();
        let mut slot = TupleSlot::from_heap_tuple(
            desc,
            attr_descs,
            ItemPointerData { block_number: 0, offset_number: 1 },
            tuple(1, "alice", None),
        );
        assert_eq!(slot.values().unwrap(), &[Value::Int32(1), Value::Text("alice".into()), Value::Null]);
        assert_eq!(slot.tid(), Some(ItemPointerData { block_number: 0, offset_number: 1 }));
    }

    #[test]
    fn seqscan_filter_projection_returns_expected_rows() {
        let base = temp_dir("scan_filter_project");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let pool = test_pool(&base);
        let xid = txns.begin();
        let rows = [tuple(1, "alice", Some("alpha")), tuple(2, "bob", None), tuple(3, "carol", Some("gamma"))];
        let mut blocks = Vec::new();
        for row in rows { let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap(); blocks.push(tid.block_number); }
        txns.commit(xid).unwrap();
        blocks.sort(); blocks.dedup();
        for block in blocks { heap_flush(&*pool, 1, rel(), block).unwrap(); }
        drop(pool);
        let plan = Plan::Projection {
            input: Box::new(Plan::Filter {
                input: Box::new(Plan::SeqScan { rel: rel(), desc: relation_desc() }),
                predicate: Expr::Gt(Box::new(Expr::Column(0)), Box::new(Expr::Const(Value::Int32(1)))),
            }),
            targets: vec![
                TargetEntry { name: "name".into(), expr: Expr::Column(1), sql_type: crate::parser::SqlType::new(crate::parser::SqlTypeKind::Text) },
                TargetEntry { name: "note_is_null".into(), expr: Expr::IsNull(Box::new(Expr::Column(2))), sql_type: crate::parser::SqlType::new(crate::parser::SqlTypeKind::Bool) },
            ],
        };
        let rows = run_plan(&base, &txns, plan).unwrap();
        assert_eq!(rows, vec![
            (vec!["name".into(), "note_is_null".into()], vec![Value::Text("bob".into()), Value::Bool(true)]),
            (vec!["name".into(), "note_is_null".into()], vec![Value::Text("carol".into()), Value::Bool(false)]),
        ]);
    }

    #[test]
    fn seqscan_skips_superseded_versions() {
        let base = temp_dir("visible_versions");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let pool = test_pool(&base);
        let insert_xid = txns.begin();
        let old_tid = heap_insert_mvcc(&*pool, 1, rel(), insert_xid, &tuple(1, "alice", Some("old"))).unwrap();
        txns.commit(insert_xid).unwrap();
        let update_xid = txns.begin();
        let new_tid = heap_update(&*pool, 1, rel(), &txns, update_xid, old_tid, &tuple(1, "alice", Some("new"))).unwrap();
        txns.commit(update_xid).unwrap();
        heap_flush(&*pool, 1, rel(), old_tid.block_number).unwrap();
        if new_tid.block_number != old_tid.block_number { heap_flush(&*pool, 1, rel(), new_tid.block_number).unwrap(); }
        drop(pool);
        let plan = Plan::SeqScan { rel: rel(), desc: relation_desc() };
        let rows = run_plan(&base, &txns, plan).unwrap();
        assert_eq!(rows, vec![(vec!["id".into(), "name".into(), "note".into()], vec![Value::Int32(1), Value::Text("alice".into()), Value::Text("new".into())])]);
    }

    #[test] fn insert_sql_inserts_row() { let base = temp_dir("insert_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); assert_eq!(run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha')").unwrap(), StatementResult::AffectedRows(1)); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select name, note from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("alice".into()), Value::Text("alpha".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn select_sql_with_table_alias() { let base = temp_dir("select_sql_table_alias"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select p.name from people p where p.id = 1").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["name"]); assert_eq!(rows, vec![vec![Value::Text("alice".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn select_sql_text_cast() { let base = temp_dir("select_sql_text_cast"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select (id)::text from people").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["id"]); assert_eq!(rows, vec![vec![Value::Text("1".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn select_sql_varchar_cast_truncates() { let base = temp_dir("select_sql_varchar_cast"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'abcdef'::varchar(3)").unwrap() { StatementResult::Query { columns, rows, .. } => { assert_eq!(columns[0].sql_type, crate::parser::SqlType::with_char_len(crate::parser::SqlTypeKind::Varchar, 3)); assert_eq!(rows, vec![vec![Value::Text("abc".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn select_sql_plain_varchar_cast_preserves_text() { let base = temp_dir("select_sql_plain_varchar_cast"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'abcdef'::varchar").unwrap() { StatementResult::Query { columns, rows, .. } => { assert_eq!(columns[0].sql_type, crate::parser::SqlType::new(crate::parser::SqlTypeKind::Varchar)); assert_eq!(rows, vec![vec![Value::Text("abcdef".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn select_sql_type_cast_with_alias() { let base = temp_dir("select_sql_type_cast_alias"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select (p.name)::text as w from people p").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["w"]); assert_eq!(rows, vec![vec![Value::Text("alice".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn select_star_sql_with_table_alias() { let base = temp_dir("select_star_sql_table_alias"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select * from people p").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["id", "name", "note"]); assert_eq!(rows, vec![vec![Value::Int32(1), Value::Text("alice".into()), Value::Text("alpha".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn select_sql_explicit_alias_overrides_column_name() { let base = temp_dir("select_sql_explicit_alias"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select p.name as w from people p").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["w"]); assert_eq!(rows, vec![vec![Value::Text("alice".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn select_sql_explicit_alias_preserved_for_empty_result() { let base = temp_dir("select_sql_explicit_alias_empty"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select p.name as w from people p").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["w"]); assert!(rows.is_empty()); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn insert_sql_inserts_multiple_rows() { let base = temp_dir("insert_multi_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); assert_eq!(run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha'), (2, 'bob', null)").unwrap(), StatementResult::AffectedRows(2)); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id, name, note from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(1), Value::Text("alice".into()), Value::Text("alpha".into())], vec![Value::Int32(2), Value::Text("bob".into()), Value::Null]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn update_sql_updates_matching_rows() { let base = temp_dir("update_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let insert_xid = txns.begin(); run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (1, 'alice', 'old')").unwrap(); txns.commit(insert_xid).unwrap(); let update_xid = txns.begin(); assert_eq!(run_sql(&base, &txns, update_xid, "update people set note = 'new' where id = 1").unwrap(), StatementResult::AffectedRows(1)); txns.commit(update_xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select note from people where id = 1").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("new".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn delete_sql_deletes_matching_rows() { let base = temp_dir("delete_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let insert_xid = txns.begin(); run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (1, 'alice', null)").unwrap(); run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (2, 'bob', 'keep')").unwrap(); txns.commit(insert_xid).unwrap(); let delete_xid = txns.begin(); assert_eq!(run_sql(&base, &txns, delete_xid, "delete from people where note is null").unwrap(), StatementResult::AffectedRows(1)); txns.commit(delete_xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select name from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("bob".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn order_by_limit_offset_returns_expected_rows() { let base = temp_dir("order_by_limit_offset"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let insert_xid = txns.begin(); run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (3, 'carol', 'c'), (2, 'bob', null)").unwrap(); txns.commit(insert_xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id, name from people order by id desc limit 2 offset 1").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(2), Value::Text("bob".into())], vec![Value::Int32(1), Value::Text("alice".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn explain_mentions_sort_and_limit_nodes() { let base = temp_dir("explain_sort_limit"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "explain select name from people order by id desc limit 1 offset 2").unwrap() { StatementResult::Query { rows, .. } => { let rendered = rows.into_iter().map(|row| match &row[0] { Value::Text(text) => text.clone(), other => panic!("expected explain text row, got {:?}", other), }).collect::<Vec<_>>(); assert!(rendered.iter().any(|line| line.contains("Projection"))); assert!(rendered.iter().any(|line| line.contains("Limit"))); assert!(rendered.iter().any(|line| line.contains("Sort"))); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn order_by_nulls_first_and_last_work() { let base = temp_dir("order_by_nulls"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let insert_xid = txns.begin(); run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')").unwrap(); txns.commit(insert_xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from people order by note asc nulls first").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(2)], vec![Value::Int32(1)], vec![Value::Int32(3)]]); } other => panic!("expected query result, got {:?}", other), } match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from people order by note desc nulls last").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(3)], vec![Value::Int32(1)], vec![Value::Int32(2)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn null_predicates_work_in_where_clause() { let base = temp_dir("null_predicates"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let insert_xid = txns.begin(); run_sql(&base, &txns, insert_xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')").unwrap(); txns.commit(insert_xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from people where note is null").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(2)]]); } other => panic!("expected query result, got {:?}", other), } match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from people where note is not null order by id").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(3)]]); } other => panic!("expected query result, got {:?}", other), } match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from people where note is distinct from null order by id").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(3)]]); } other => panic!("expected query result, got {:?}", other), } match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from people where note is not distinct from null").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(2)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn show_tables_lists_catalog_tables() { let base = temp_dir("show_tables"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "show tables").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["table_name".to_string()]); assert_eq!(rows, vec![vec![Value::Text("people".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn explain_returns_plan_lines() { let base = temp_dir("explain_sql"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "explain select name from people").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["QUERY PLAN".to_string()]); let rendered = rows.into_iter().map(|row| match &row[0] { Value::Text(text) => text.clone(), other => panic!("expected text explain line, got {:?}", other), }).collect::<Vec<_>>(); assert!(rendered.iter().any(|line| line.contains("Projection"))); assert!(rendered.iter().any(|line| line.contains("Seq Scan"))); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn select_without_from_returns_constant_row() { let base = temp_dir("select_without_from"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 1").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["expr1".to_string()]); assert_eq!(rows, vec![vec![Value::Int32(1)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn select_from_people_returns_zero_column_rows() { let base = temp_dir("select_from_people"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha'), (2, 'bob', null)").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select from people").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert!(column_names.is_empty()); assert_eq!(rows, vec![vec![], vec![]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn explain_analyze_buffers_reports_runtime_and_buffers() { let base = temp_dir("explain_analyze_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "explain (analyze, buffers) select name from people").unwrap() { StatementResult::Query { rows, .. } => { let rendered = rows.into_iter().map(|row| match &row[0] { Value::Text(text) => text.clone(), other => panic!("expected text explain line, got {:?}", other), }).collect::<Vec<_>>(); assert!(rendered.iter().any(|line| line.contains("actual rows="))); assert!(rendered.iter().any(|line| line.contains("Execution Time:"))); assert!(rendered.iter().any(|line| line.contains("Buffers: shared"))); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn inner_join_returns_matching_rows() { let base = temp_dir("join_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let pool = test_pool_with_pets(&base); let xid = txns.begin(); for row in [tuple(1, "alice", Some("alpha")), tuple(2, "bob", None), tuple(3, "carol", Some("storage"))] { let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap(); heap_flush(&*pool, 1, rel(), tid.block_number).unwrap(); } for row in [pet_tuple(10, "Kitchen", 2), pet_tuple(11, "Mocha", 3)] { let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &row).unwrap(); heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap(); } txns.commit(xid).unwrap(); match run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "select people.name, pets.name from people join pets on people.id = pets.owner_id", catalog_with_pets()).unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("bob".into()), Value::Text("Kitchen".into())], vec![Value::Text("carol".into()), Value::Text("Mocha".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn cross_join_returns_cartesian_product() { let base = temp_dir("cross_join_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let pool = test_pool_with_pets(&base); let xid = txns.begin(); for row in [tuple(1, "alice", Some("alpha")), tuple(2, "bob", None)] { let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap(); heap_flush(&*pool, 1, rel(), tid.block_number).unwrap(); } for row in [pet_tuple(10, "Kitchen", 2), pet_tuple(11, "Mocha", 3)] { let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &row).unwrap(); heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap(); } txns.commit(xid).unwrap(); match run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "select people.name, pets.name from people, pets", catalog_with_pets()).unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("alice".into()), Value::Text("Kitchen".into())], vec![Value::Text("alice".into()), Value::Text("Mocha".into())], vec![Value::Text("bob".into()), Value::Text("Kitchen".into())], vec![Value::Text("bob".into()), Value::Text("Mocha".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn cross_join_where_clause_can_use_addition() { let base = temp_dir("cross_join_addition_sql"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let pool = test_pool_with_pets(&base); let xid = txns.begin(); for row in [tuple(1, "alice", Some("alpha")), tuple(2, "bob", None)] { let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap(); heap_flush(&*pool, 1, rel(), tid.block_number).unwrap(); } for row in [pet_tuple(10, "Kitchen", 1), pet_tuple(11, "Mocha", 2)] { let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &row).unwrap(); heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap(); } txns.commit(xid).unwrap(); match run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "select people.name, pets.name from people, pets where pets.owner_id + 1 = people.id", catalog_with_pets()).unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("bob".into()), Value::Text("Kitchen".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn count_star_without_group_by() { let base = temp_dir("count_star"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select count(*) from people").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["count"]); assert_eq!(rows, vec![vec![Value::Int32(3)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn count_star_on_empty_table() { let base = temp_dir("count_star_empty"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select count(*) from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(0)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn group_by_with_count() { let base = temp_dir("group_by_count"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'a'), (3, 'carol', 'b')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select note, count(*) from people group by note order by note").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["note", "count"]); assert_eq!(rows, vec![vec![Value::Text("a".into()), Value::Int32(2)], vec![Value::Text("b".into()), Value::Int32(1)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn sum_avg_min_max_aggregates() { let base = temp_dir("sum_avg_min_max"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (10, 'alice', 'a'), (20, 'bob', 'b'), (30, 'carol', 'c')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select sum(id), avg(id), min(id), max(id) from people").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["sum", "avg", "min", "max"]); assert_eq!(rows, vec![vec![Value::Int32(60), Value::Int32(20), Value::Int32(10), Value::Int32(30)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn having_filters_groups() { let base = temp_dir("having_filter"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'a'), (3, 'carol', 'b')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select note, count(*) from people group by note having count(*) > 1").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("a".into()), Value::Int32(2)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn count_expr_skips_nulls() { let base = temp_dir("count_expr_nulls"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'c')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select count(note) from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(2)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn sum_of_all_nulls_returns_null() { let base = temp_dir("sum_all_nulls"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', null), (2, 'bob', null)").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select min(note), max(note) from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Null, Value::Null]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn null_group_by_keys_are_grouped_together() { let base = temp_dir("null_group_keys"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', null), (2, 'bob', 'a'), (3, 'carol', null), (4, 'dave', 'a')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select note, count(*) from people group by note order by note").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows.len(), 2); assert_eq!(rows[0], vec![Value::Text("a".into()), Value::Int32(2)]); assert_eq!(rows[1], vec![Value::Null, Value::Int32(2)]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn sum_and_avg_skip_nulls() { let base = temp_dir("sum_avg_skip_nulls"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (10, 'alice', 'a'), (20, 'bob', null), (30, 'carol', 'c')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select count(*), count(note), sum(id), avg(id), min(id), max(id) from people").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["count", "count", "sum", "avg", "min", "max"]); assert_eq!(rows, vec![vec![Value::Int32(3), Value::Int32(2), Value::Int32(60), Value::Int32(20), Value::Int32(10), Value::Int32(30)]]); } other => panic!("expected query result, got {:?}", other), } }

    // regex (~) operator tests
    #[test] fn count_distinct_counts_unique_values() { let base = temp_dir("count_distinct"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'a'), (3, 'carol', 'b'), (4, 'dave', 'b'), (5, 'eve', 'c')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select count(distinct note) from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(3)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn count_distinct_skips_nulls() { let base = temp_dir("count_distinct_nulls"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', null), (3, 'carol', 'a'), (4, 'dave', null)").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select count(distinct note) from people").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(1)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn count_distinct_with_group_by() { let base = temp_dir("count_distinct_group"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'x'), (2, 'alice', 'x'), (3, 'alice', 'y'), (4, 'bob', 'x'), (5, 'bob', 'x')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select name, count(distinct note) from people group by name order by name").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("alice".into()), Value::Int32(2)], vec![Value::Text("bob".into()), Value::Int32(1)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn generate_series_basic() { let base = temp_dir("gen_series_basic"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select * from generate_series(1, 5)").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["generate_series"]); assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(2)], vec![Value::Int32(3)], vec![Value::Int32(4)], vec![Value::Int32(5)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn generate_series_with_step() { let base = temp_dir("gen_series_step"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select * from generate_series(0, 10, 3)").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(0)], vec![Value::Int32(3)], vec![Value::Int32(6)], vec![Value::Int32(9)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn generate_series_negative_step() { let base = temp_dir("gen_series_neg"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select * from generate_series(5, 1, -1)").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(5)], vec![Value::Int32(4)], vec![Value::Int32(3)], vec![Value::Int32(2)], vec![Value::Int32(1)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn generate_series_empty() { let base = temp_dir("gen_series_empty"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select * from generate_series(1, 0)").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, Vec::<Vec<Value>>::new()); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn generate_series_with_where() { let base = temp_dir("gen_series_where"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select * from generate_series(1, 10) where generate_series > 8").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(9)], vec![Value::Int32(10)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn select_array_literal_round_trips() { let base = temp_dir("array_literal_round_trip"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select ARRAY['a', 'b']::varchar[]").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Array(vec![Value::Text("a".into()), Value::Text("b".into())])]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn any_array_truth_table_and_overlap_work() { let base = temp_dir("array_any_overlap"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'b' = any(ARRAY['a', 'b']::varchar[]), 'z' = any(ARRAY['a', null]::varchar[]), ARRAY['a']::varchar[] && ARRAY['b', 'a']::varchar[]").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true), Value::Null, Value::Bool(true)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn unnest_single_and_multi_arg_work() { let base = temp_dir("unnest_multi"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select * from unnest(ARRAY[1, 2], ARRAY['x']::varchar[]) as u(a, b)").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["a", "b"]); assert_eq!(rows, vec![vec![Value::Int32(1), Value::Text("x".into())], vec![Value::Int32(2), Value::Null]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn shipment_shaped_array_query_runs() { let base = temp_dir("shipment_arrays"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql_with_catalog(&base, &txns, xid, "insert into om_shipments (id, company_id, year, container_numbers, container_types_categories, container_size_categories) values (1, 'acme', '2024', ARRAY['c1', 'c2']::varchar[], ARRAY['dry', 'dry']::varchar[], ARRAY['40_high_cube', '20_standard']::varchar[]), (2, 'acme', '2024', ARRAY['c3']::varchar[], ARRAY['dry']::varchar[], ARRAY['40_high_cube']::varchar[]), (3, 'beta', '2024', ARRAY['c4']::varchar[], ARRAY['dry']::varchar[], ARRAY['20_standard']::varchar[])", shipments_catalog()).unwrap(); txns.commit(xid).unwrap(); match run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "select om_shipments.company_id, count(distinct om_shipments.id) as shipments_filtered, sum((select count(*) from unnest(om_shipments.container_numbers, om_shipments.container_types_categories, om_shipments.container_size_categories) as c(num, type_cat, size_cat) where (c.size_cat)::text = any(ARRAY['40_high_cube']::varchar[]))) as containers_filtered from om_shipments where om_shipments.year = '2024' and om_shipments.container_size_categories && ARRAY['40_high_cube']::varchar[] group by om_shipments.company_id order by om_shipments.company_id", shipments_catalog()).unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("acme".into()), Value::Int32(2), Value::Int32(2)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn all_array_semantics_match_empty_false_and_null_cases() { let base = temp_dir("all_array_semantics"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 1 < all(ARRAY[2, 3]), 1 < all(ARRAY[]::int4[]), 3 < all(ARRAY[2, null]::int4[]), 1 < all(ARRAY[2, null]::int4[])").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true), Value::Bool(true), Value::Bool(false), Value::Null]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn any_array_empty_and_null_array_cases() { let base = temp_dir("any_array_empty_null"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 1 = any(ARRAY[]::int4[]), 1 = any((null)::int4[]), (null)::int4 = any(ARRAY[1]::int4[])").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(false), Value::Null, Value::Null]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn array_overlap_false_and_null_cases() { let base = temp_dir("array_overlap_false_null"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select ARRAY['a']::varchar[] && ARRAY['b']::varchar[], ARRAY['a', null]::varchar[] && ARRAY['b', null]::varchar[], ARRAY['a']::varchar[] && (null)::varchar[]").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(false), Value::Bool(false), Value::Null]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn typed_empty_array_selects_as_empty_value() { let base = temp_dir("typed_empty_array"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select ARRAY[]::varchar[]").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Array(vec![])]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn unnest_null_and_empty_arrays_return_no_rows() { let base = temp_dir("unnest_null_empty"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select * from unnest((null)::int4[])").unwrap() { StatementResult::Query { rows, .. } => assert!(rows.is_empty()), other => panic!("expected query result, got {:?}", other), } match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select * from unnest(ARRAY[]::int4[])").unwrap() { StatementResult::Query { rows, .. } => assert!(rows.is_empty()), other => panic!("expected query result, got {:?}", other), } }
    #[test] fn unnest_null_array_zips_with_longer_input() { let base = temp_dir("unnest_null_zip"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select * from unnest((null)::int4[], ARRAY['x', 'y']::varchar[]) as u(a, b)").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Null, Value::Text("x".into())], vec![Value::Null, Value::Text("y".into())]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn array_columns_round_trip_through_storage() { let base = temp_dir("array_storage_roundtrip"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); assert_eq!(run_sql_with_catalog(&base, &txns, xid, "insert into om_shipments (id, company_id, year, container_numbers, container_types_categories, container_size_categories) values (1, 'acme', '2024', ARRAY['n1', null]::varchar[], ARRAY['dry']::varchar[], ARRAY['40_high_cube']::varchar[])", shipments_catalog()).unwrap(), StatementResult::AffectedRows(1)); txns.commit(xid).unwrap(); match run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "select container_numbers from om_shipments", shipments_catalog()).unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Array(vec![Value::Text("n1".into()), Value::Null])]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn generate_series_column_alias() {
        let base = temp_dir("gen_series_alias");
        let txns = TransactionManager::new_durable(&base).unwrap();
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select val from generate_series(1, 3) as g(val)").unwrap() {
            StatementResult::Query { column_names, rows, .. } => {
                assert_eq!(column_names, vec!["val"]);
                assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(2)], vec![Value::Int32(3)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn generate_series_column_alias_in_where() {
        let base = temp_dir("gen_series_alias_where");
        let txns = TransactionManager::new_durable(&base).unwrap();
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select val from generate_series(1, 5) as g(val) where val > 3").unwrap() {
            StatementResult::Query { column_names, rows, .. } => {
                assert_eq!(column_names, vec!["val"]);
                assert_eq!(rows, vec![vec![Value::Int32(4)], vec![Value::Int32(5)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn generate_series_table_alias_only() {
        let base = temp_dir("gen_series_table_alias");
        let txns = TransactionManager::new_durable(&base).unwrap();
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select * from generate_series(1, 3) as g").unwrap() {
            StatementResult::Query { column_names, rows, .. } => {
                assert_eq!(column_names, vec!["generate_series"]);
                assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(2)], vec![Value::Int32(3)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn generate_series_alias_without_as_keyword() {
        let base = temp_dir("gen_series_no_as");
        let txns = TransactionManager::new_durable(&base).unwrap();
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select i from generate_series(1, 3) g(i)").unwrap() {
            StatementResult::Query { column_names, rows, .. } => {
                assert_eq!(column_names, vec!["i"]);
                assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(2)], vec![Value::Int32(3)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn generate_series_table_alias_qualifies_column() {
        let base = temp_dir("gen_series_qualify");
        let txns = TransactionManager::new_durable(&base).unwrap();
        // Use the table alias to qualify the column reference: g.val
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select g.val from generate_series(1, 3) as g(val)").unwrap() {
            StatementResult::Query { column_names, rows, .. } => {
                assert_eq!(column_names, vec!["val"]);
                assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(2)], vec![Value::Int32(3)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn select_from_derived_table() {
        let base = temp_dir("derived_table_basic");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let xid = txns.begin();
        run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha'), (2, 'bob', null)").unwrap();
        txns.commit(xid).unwrap();
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select p.id from (select id from people) p order by p.id").unwrap() {
            StatementResult::Query { column_names, rows, .. } => {
                assert_eq!(column_names, vec!["id"]);
                assert_eq!(rows, vec![vec![Value::Int32(1)], vec![Value::Int32(2)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn select_from_aliasless_derived_table() {
        let base = temp_dir("derived_table_no_alias");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let xid = txns.begin();
        run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha')").unwrap();
        txns.commit(xid).unwrap();
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from (select id from people)").unwrap() {
            StatementResult::Query { column_names, rows, .. } => {
                assert_eq!(column_names, vec!["id"]);
                assert_eq!(rows, vec![vec![Value::Int32(1)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn derived_table_column_aliases_rename_output() {
        let base = temp_dir("derived_table_alias_cols");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let xid = txns.begin();
        run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha')").unwrap();
        txns.commit(xid).unwrap();
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select p.x from (select id from people) p(x)").unwrap() {
            StatementResult::Query { column_names, rows, .. } => {
                assert_eq!(column_names, vec!["x"]);
                assert_eq!(rows, vec![vec![Value::Int32(1)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn derived_table_partial_column_aliases_preserve_remaining_names() {
        let base = temp_dir("derived_table_alias_partial");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let xid = txns.begin();
        run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha')").unwrap();
        txns.commit(xid).unwrap();
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select p.x, p.name from (select id, name from people) p(x) order by p.x").unwrap() {
            StatementResult::Query { column_names, rows, .. } => {
                assert_eq!(column_names, vec!["x", "name"]);
                assert_eq!(rows, vec![vec![Value::Int32(1), Value::Text("alice".into())]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn join_against_derived_table_returns_matching_rows() {
        let base = temp_dir("join_derived_table");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let pool = test_pool_with_pets(&base);
        let xid = txns.begin();
        for row in [tuple(1, "alice", Some("alpha")), tuple(2, "bob", None), tuple(3, "carol", Some("storage"))] {
            let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &row).unwrap();
            heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
        }
        for row in [pet_tuple(10, "Kitchen", 2), pet_tuple(11, "Mocha", 3)] {
            let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &row).unwrap();
            heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap();
        }
        txns.commit(xid).unwrap();
        match run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "select p.name, q.owner_id from people p join (select owner_id from pets) q on p.id = q.owner_id order by q.owner_id", catalog_with_pets()).unwrap() {
            StatementResult::Query { column_names, rows, .. } => {
                assert_eq!(column_names, vec!["name", "owner_id"]);
                assert_eq!(rows, vec![
                    vec![Value::Text("bob".into()), Value::Int32(2)],
                    vec![Value::Text("carol".into()), Value::Int32(3)],
                ]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn derived_table_can_cross_join_with_generate_series() {
        let base = temp_dir("derived_table_cross_srf");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let xid = txns.begin();
        run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'alpha'), (2, 'bob', null)").unwrap();
        txns.commit(xid).unwrap();
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select p.id, g.generate_series from (select id from people) p, generate_series(1, 2) g order by p.id, g.generate_series").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![
                    vec![Value::Int32(1), Value::Int32(1)],
                    vec![Value::Int32(1), Value::Int32(2)],
                    vec![Value::Int32(2), Value::Int32(1)],
                    vec![Value::Int32(2), Value::Int32(2)],
                ]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn generate_series_sources_can_cross_join_each_other() {
        let base = temp_dir("srf_cross_join");
        let txns = TransactionManager::new_durable(&base).unwrap();
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select g.generate_series, h.generate_series from generate_series(1, 2) g, generate_series(5, 6) h order by g.generate_series, h.generate_series").unwrap() {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![
                    vec![Value::Int32(1), Value::Int32(5)],
                    vec![Value::Int32(1), Value::Int32(6)],
                    vec![Value::Int32(2), Value::Int32(5)],
                    vec![Value::Int32(2), Value::Int32(6)],
                ]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn join_alias_hides_inner_relation_names() {
        let base = temp_dir("join_alias_hides_inner");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let pool = test_pool_with_pets(&base);
        let xid = txns.begin();
        let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &tuple(1, "alice", Some("alpha"))).unwrap();
        heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
        let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &pet_tuple(10, "Kitchen", 1)).unwrap();
        heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap();
        txns.commit(xid).unwrap();
        let err = run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "select p.name from (people p join pets q on p.id = q.owner_id) j", catalog_with_pets()).unwrap_err();
        assert!(matches!(err, ExecError::Parse(ParseError::UnknownColumn(name)) if name == "p.name"));
    }
    #[test] fn non_lateral_derived_table_rejects_outer_refs() {
        let base = temp_dir("derived_table_outer_ref");
        let txns = TransactionManager::new_durable(&base).unwrap();
        let err = run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select * from people p, (select p.id from people) q").unwrap_err();
        assert!(matches!(err, ExecError::Parse(ParseError::UnknownColumn(name)) if name == "p.id"));
    }
    #[test] fn derived_table_alias_preserved_for_empty_result() {
        let base = temp_dir("derived_table_empty_alias");
        let txns = TransactionManager::new_durable(&base).unwrap();
        match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select p.x from (select id from people where id > 10) p(x)").unwrap() {
            StatementResult::Query { column_names, rows, .. } => {
                assert_eq!(column_names, vec!["x"]);
                assert!(rows.is_empty());
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn parenthesized_join_alias_can_be_selected_from() {
        let base = temp_dir("parenthesized_join_alias");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let pool = test_pool_with_pets(&base);
        let xid = txns.begin();
        let tid = heap_insert_mvcc(&*pool, 1, rel(), xid, &tuple(1, "alice", Some("alpha"))).unwrap();
        heap_flush(&*pool, 1, rel(), tid.block_number).unwrap();
        let tid = heap_insert_mvcc(&*pool, 1, pets_rel(), xid, &pet_tuple(10, "Kitchen", 1)).unwrap();
        heap_flush(&*pool, 1, pets_rel(), tid.block_number).unwrap();
        txns.commit(xid).unwrap();
        match run_sql_with_catalog(&base, &txns, INVALID_TRANSACTION_ID, "select j.note, j.owner_id from (people p join pets q on p.id = q.owner_id) j", catalog_with_pets()).unwrap() {
            StatementResult::Query { column_names, rows, .. } => {
                assert_eq!(column_names, vec!["note", "owner_id"]);
                assert_eq!(rows, vec![vec![Value::Text("alpha".into()), Value::Int32(1)]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
    #[test] fn regex_basic_match() { let base = temp_dir("regex_basic_match"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'foobar' ~ 'foo'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_basic_no_match() { let base = temp_dir("regex_basic_no_match"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'foobar' ~ 'baz'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(false)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_start_anchor_match() { let base = temp_dir("regex_start_anchor_match"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'foobar' ~ '^foo'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_start_anchor_no_match() { let base = temp_dir("regex_start_anchor_no_match"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'foobar' ~ '^bar'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(false)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_end_anchor_match() { let base = temp_dir("regex_end_anchor_match"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'foobar' ~ 'bar$'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_end_anchor_no_match() { let base = temp_dir("regex_end_anchor_no_match"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'foobar' ~ 'foo$'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(false)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_full_anchor_match() { let base = temp_dir("regex_full_anchor"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'foobar' ~ '^foobar$'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_dot_matches_any() { let base = temp_dir("regex_dot"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'foobar' ~ 'f.obar'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_plus_quantifier() { let base = temp_dir("regex_plus"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'fooooo' ~ 'fo+'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_star_quantifier() { let base = temp_dir("regex_star"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'f' ~ 'fo*'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_digit_class_match() { let base = temp_dir("regex_digit_match"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'abc123' ~ '[0-9]+'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_digit_class_no_match() { let base = temp_dir("regex_digit_no_match"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'abc' ~ '[0-9]+'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(false)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_alternation_first_branch() { let base = temp_dir("regex_alt_first"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'foo' ~ '(foo|bar)'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_alternation_second_branch() { let base = temp_dir("regex_alt_second"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'bar' ~ '(foo|bar)'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(true)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_is_case_sensitive() { let base = temp_dir("regex_case_sensitive"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select 'FOO' ~ 'foo'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Bool(false)]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_null_text_returns_null() { let base = temp_dir("regex_null_text_returns_null"); let mut ctx = empty_executor_context(&base); let mut slot = TupleSlot::virtual_row(vec![Value::Null]); assert_eq!(eval_expr(&Expr::RegexMatch(Box::new(Expr::Column(0)), Box::new(Expr::Const(Value::Text("foo".into())))), &mut slot, &mut ctx).unwrap(), Value::Null); }
    #[test] fn regex_null_pattern_returns_null() { let base = temp_dir("regex_null_pattern_returns_null"); let mut ctx = empty_executor_context(&base); let mut slot = TupleSlot::virtual_row(vec![Value::Text("foobar".into())]); assert_eq!(eval_expr(&Expr::RegexMatch(Box::new(Expr::Column(0)), Box::new(Expr::Const(Value::Null))), &mut slot, &mut ctx).unwrap(), Value::Null); }
    #[test] fn regex_filters_rows_in_where_clause() { let base = temp_dir("regex_filter_where"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'bob', 'b'), (3, 'charlie', 'c')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select name from people where name ~ '^a'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("alice".into())]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_filter_matches_multiple_rows() { let base = temp_dir("regex_filter_multi"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'arnold', 'b'), (3, 'bob', 'c')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select name from people where name ~ '^a' order by name").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("alice".into())], vec![Value::Text("arnold".into())]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_combined_with_and() { let base = temp_dir("regex_and"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'a'), (2, 'albert', 'b'), (3, 'bob', 'c')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select name from people where name ~ '^al' and id > 1").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("albert".into())]]); } other => panic!("{:?}", other), } }
    #[test] fn regex_null_column_excluded_from_results() { let base = temp_dir("regex_null_col"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'keep'), (2, 'bob', null)").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select id from people where note ~ 'keep'").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Int32(1)]]); } other => panic!("{:?}", other), } }
    #[test] fn ungrouped_column_is_rejected() { let base = temp_dir("ungrouped_column"); let txns = TransactionManager::new_durable(&base).unwrap(); let result = run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select name, count(*) from people"); assert!(result.is_err()); }
    #[test] fn aggregate_in_where_is_rejected() { let base = temp_dir("agg_in_where"); let txns = TransactionManager::new_durable(&base).unwrap(); let result = run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select name from people where count(*) > 1"); assert!(result.is_err()); }
    #[test] fn explain_shows_aggregate_node() { let base = temp_dir("explain_agg"); let txns = TransactionManager::new_durable(&base).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "explain select note, count(*) from people group by note").unwrap() { StatementResult::Query { rows, .. } => { let rendered = rows.into_iter().map(|row| match &row[0] { Value::Text(text) => text.clone(), other => panic!("expected text, got {:?}", other), }).collect::<Vec<_>>(); assert!(rendered.iter().any(|line| line.contains("Aggregate"))); assert!(rendered.iter().any(|line| line.contains("Seq Scan"))); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn group_by_with_order_by_and_limit() { let base = temp_dir("group_by_order_limit"); let mut txns = TransactionManager::new_durable(&base).unwrap(); let xid = txns.begin(); run_sql(&base, &txns, xid, "insert into people (id, name, note) values (1, 'alice', 'x'), (2, 'bob', 'y'), (3, 'carol', 'x'), (4, 'dave', 'y'), (5, 'eve', 'z')").unwrap(); txns.commit(xid).unwrap(); match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select note, count(*) from people group by note order by count(*) desc limit 2").unwrap() { StatementResult::Query { rows, .. } => { assert_eq!(rows, vec![vec![Value::Text("x".into()), Value::Int32(2)], vec![Value::Text("y".into()), Value::Int32(2)]]); } other => panic!("expected query result, got {:?}", other), } }
    #[test] fn random_returns_float_in_range() { let base = temp_dir("random_func"); let txns = TransactionManager::new_durable(&base).unwrap(); for _ in 0..10 { match run_sql(&base, &txns, INVALID_TRANSACTION_ID, "select random()").unwrap() { StatementResult::Query { column_names, rows, .. } => { assert_eq!(column_names, vec!["random".to_string()]); assert_eq!(rows.len(), 1); match &rows[0][0] { Value::Float64(v) => assert!(*v >= 0.0 && *v < 1.0, "random() must be in [0,1), got {v}"), other => panic!("expected Float64, got {:?}", other), } } other => panic!("expected query result, got {:?}", other), } } }

    #[test]
    fn insert_sql_varchar_rejects_non_space_overflow() {
        let base = temp_dir("insert_varchar_overflow");
        let txns = TransactionManager::new_durable(&base).unwrap();
        let err = run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "insert into t (name) values ('cd')",
            varchar_catalog("t", 1),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ExecError::StringDataRightTruncation { ref ty } if ty == "character varying(1)"
        ));
    }

    #[test]
    fn insert_sql_varchar_trims_trailing_spaces() {
        let base = temp_dir("insert_varchar_trailing_spaces");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let xid = txns.begin();
        run_sql_with_catalog(
            &base,
            &txns,
            xid,
            "insert into t (name) values ('c     ')",
            varchar_catalog("t", 1),
        )
        .unwrap();
        txns.commit(xid).unwrap();

        match run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select name from t",
            varchar_catalog("t", 1),
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Text("c".into())]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    #[test]
    fn insert_sql_varchar_counts_characters_not_bytes() {
        let base = temp_dir("insert_varchar_multibyte");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let xid = txns.begin();
        run_sql_with_catalog(
            &base,
            &txns,
            xid,
            "insert into t (name) values ('éé')",
            varchar_catalog("t", 2),
        )
        .unwrap();
        txns.commit(xid).unwrap();

        let err = run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "insert into t (name) values ('ééé')",
            varchar_catalog("t", 2),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            ExecError::StringDataRightTruncation { ref ty } if ty == "character varying(2)"
        ));
    }

    #[test]
    fn scalar_subquery_target_list_returns_per_row_counts() {
        let base = temp_dir("scalar_subquery_target_list");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.name, (select count(*) from pets q where q.owner_id = p.id) from people p order by p.id",
                catalog_with_pets(),
            )
            .unwrap(),
            vec![
                vec![Value::Text("alice".into()), Value::Int32(2)],
                vec![Value::Text("bob".into()), Value::Int32(1)],
                vec![Value::Text("carol".into()), Value::Int32(0)],
            ],
        );
    }

    #[test]
    fn scalar_subquery_zero_rows_yields_null() {
        let base = temp_dir("scalar_subquery_zero_rows");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.name, (select q.name from pets q where q.owner_id = p.id and q.id = 999) from people p order by p.id",
                catalog_with_pets(),
            )
            .unwrap(),
            vec![
                vec![Value::Text("alice".into()), Value::Null],
                vec![Value::Text("bob".into()), Value::Null],
                vec![Value::Text("carol".into()), Value::Null],
            ],
        );
    }

    #[test]
    fn scalar_subquery_multiple_rows_errors() {
        let base = temp_dir("scalar_subquery_multiple_rows");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        let err = run_sql_with_catalog(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select (select q.name from pets q where q.owner_id = p.id) from people p where p.id = 1",
            catalog_with_pets(),
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("more than one row returned by a subquery used as an expression"));
    }

    #[test]
    fn exists_and_not_exists_are_correlated_per_row() {
        let base = temp_dir("exists_correlated_per_row");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.id from people p where exists (select 1 from pets q where q.owner_id = p.id) order by p.id",
                catalog_with_pets(),
            )
            .unwrap(),
            vec![vec![Value::Int32(1)], vec![Value::Int32(2)]],
        );
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.id from people p where not exists (select 1 from pets q where q.owner_id = p.id) order by p.id",
                catalog_with_pets(),
            )
            .unwrap(),
            vec![vec![Value::Int32(3)]],
        );
    }

    #[test]
    fn in_subquery_truth_table_cases() {
        let base = temp_dir("in_subquery_truth_table");
        let txns = TransactionManager::new_durable(&base).unwrap();
        assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select 1 in (select 1), 1 in (select 2), 1 in (select 1 where false)",
            )
            .unwrap(),
            vec![vec![Value::Bool(true), Value::Bool(false), Value::Bool(false)]],
        );
    }

    #[test]
    fn not_in_subquery_truth_table_cases() {
        let base = temp_dir("not_in_subquery_truth_table");
        let txns = TransactionManager::new_durable(&base).unwrap();
        assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select 1 not in (select 1), 1 not in (select 2), 1 not in (select 1 where false)",
            )
            .unwrap(),
            vec![vec![Value::Bool(false), Value::Bool(true), Value::Bool(true)]],
        );
    }

    #[test]
    fn in_and_not_in_propagate_nulls_like_postgres() {
        let base = temp_dir("in_not_in_nulls");
        let txns = TransactionManager::new_durable(&base).unwrap();
        assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select 1 in (select null), 1 not in (select null), null in (select 1), null not in (select 1)",
            )
            .unwrap(),
            vec![vec![Value::Null, Value::Null, Value::Null, Value::Null]],
        );
    }

    #[test]
    fn any_and_all_subquery_match_postgres_empty_set_semantics() {
        let base = temp_dir("any_all_empty_set");
        let txns = TransactionManager::new_durable(&base).unwrap();
        assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select 1 = any (select 1 where false), 1 < all (select 1 where false)",
            )
            .unwrap(),
            vec![vec![Value::Bool(false), Value::Bool(true)]],
        );
    }

    #[test]
    fn any_and_all_subquery_propagate_nulls() {
        let base = temp_dir("any_all_nulls");
        let txns = TransactionManager::new_durable(&base).unwrap();
        assert_query_rows(
            run_sql(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select 1 = any (select null), 1 < all (select null)",
            )
            .unwrap(),
            vec![vec![Value::Null, Value::Null]],
        );
    }

    #[test]
    fn correlated_any_subquery_filters_rows() {
        let base = temp_dir("correlated_any_subquery");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.id from people p where p.id = any (select q.owner_id from pets q where q.owner_id is not null) order by p.id",
                catalog_with_pets(),
            )
            .unwrap(),
            vec![vec![Value::Int32(1)], vec![Value::Int32(2)]],
        );
    }

    #[test]
    fn grouped_query_having_can_use_correlated_exists() {
        let base = temp_dir("grouped_having_correlated_exists");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.id, count(*) from people p group by p.id having exists (select 1 from pets q where q.owner_id = p.id) order by p.id",
                catalog_with_pets(),
            )
            .unwrap(),
            vec![
                vec![Value::Int32(1), Value::Int32(1)],
                vec![Value::Int32(2), Value::Int32(1)],
            ],
        );
    }

    #[test]
    fn nested_outer_correlation_uses_the_correct_row() {
        let base = temp_dir("nested_outer_correlation");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.id from people p where exists (select 1 from pets q where q.owner_id = p.id and exists (select 1 from people r where r.id = p.id and r.name = p.name)) order by p.id",
                catalog_with_pets(),
            )
            .unwrap(),
            vec![vec![Value::Int32(1)], vec![Value::Int32(2)]],
        );
    }

    #[test]
    fn scalar_subquery_can_be_used_in_order_by() {
        let base = temp_dir("scalar_subquery_order_by");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        seed_people_and_pets(&base, &mut txns);
        assert_query_rows(
            run_sql_with_catalog(
                &base,
                &txns,
                INVALID_TRANSACTION_ID,
                "select p.name from people p order by (select count(*) from pets q where q.owner_id = p.id) desc, p.id",
                catalog_with_pets(),
            )
            .unwrap(),
            vec![
                vec![Value::Text("alice".into())],
                vec![Value::Text("bob".into())],
                vec![Value::Text("carol".into())],
            ],
        );
    }
}
