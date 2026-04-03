use crate::access::heap::am::{
    HeapError, VisibleHeapScan, heap_delete, heap_flush, heap_insert_mvcc_with_cid,
    heap_scan_begin_visible, heap_scan_next_visible, heap_update_with_cid,
};
use crate::access::heap::mvcc::{CommandId, MvccError, Snapshot, TransactionId, TransactionManager};
use crate::catalog::Catalog;
use crate::access::heap::tuple::{
    AttributeDesc, HeapTuple, ItemPointerData, TupleError, TupleValue,
};
use crate::parser::{
    BoundDeleteStatement, BoundInsertStatement, BoundUpdateStatement, DropTableStatement,
    ParseError, Statement, bind_create_table, bind_delete, bind_insert, bind_update, build_plan,
    parse_statement,
};
use crate::storage::smgr::StorageManager;
use crate::{BufferPool, ClientId, RelFileLocator, SmgrStorageBackend};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    Int32,
    Text,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDesc {
    pub name: String,
    pub storage: AttributeDesc,
    pub ty: ScalarType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationDesc {
    pub columns: Vec<ColumnDesc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int32(i32),
    Text(String),
    Bool(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetEntry {
    pub name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(usize),
    Const(Value),
    Eq(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    SeqScan {
        rel: RelFileLocator,
        desc: RelationDesc,
    },
    Filter {
        input: Box<Plan>,
        predicate: Expr,
    },
    Projection {
        input: Box<Plan>,
        targets: Vec<TargetEntry>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleSlot {
    column_names: Vec<String>,
    source: SlotSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SlotSource {
    Physical {
        desc: RelationDesc,
        tid: ItemPointerData,
        tuple: HeapTuple,
        materialized: Option<Vec<Value>>,
    },
    Virtual {
        values: Vec<Value>,
    },
}

#[derive(Debug)]
pub enum PlanState {
    SeqScan(SeqScanState),
    Filter(FilterState),
    Projection(ProjectionState),
}

#[derive(Debug)]
pub struct SeqScanState {
    rel: RelFileLocator,
    desc: RelationDesc,
    scan: Option<VisibleHeapScan>,
}

#[derive(Debug)]
pub struct FilterState {
    input: Box<PlanState>,
    predicate: Expr,
}

#[derive(Debug)]
pub struct ProjectionState {
    input: Box<PlanState>,
    targets: Vec<TargetEntry>,
}

pub struct ExecutorContext<'a> {
    pub pool: &'a mut BufferPool<SmgrStorageBackend>,
    pub txns: &'a TransactionManager,
    pub snapshot: Snapshot,
    pub client_id: ClientId,
    pub next_command_id: CommandId,
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
    MissingRequiredColumn(String),
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

impl RelationDesc {
    pub fn attribute_descs(&self) -> Vec<AttributeDesc> {
        self.columns.iter().map(|c| c.storage.clone()).collect()
    }
}

impl TupleSlot {
    pub fn from_heap_tuple(desc: RelationDesc, tid: ItemPointerData, tuple: HeapTuple) -> Self {
        Self {
            column_names: desc.columns.iter().map(|c| c.name.clone()).collect(),
            source: SlotSource::Physical {
                desc,
                tid,
                tuple,
                materialized: None,
            },
        }
    }

    pub fn virtual_row(column_names: Vec<String>, values: Vec<Value>) -> Self {
        Self {
            column_names,
            source: SlotSource::Virtual { values },
        }
    }

    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    pub fn tid(&self) -> Option<ItemPointerData> {
        match &self.source {
            SlotSource::Physical { tid, .. } => Some(*tid),
            SlotSource::Virtual { .. } => None,
        }
    }

    pub fn values(&mut self) -> Result<&[Value], ExecError> {
        match &mut self.source {
            SlotSource::Virtual { values } => Ok(values.as_slice()),
            SlotSource::Physical {
                desc,
                tuple,
                materialized,
                ..
            } => {
                if materialized.is_none() {
                    let attr_descs = desc.attribute_descs();
                    let raw = tuple.deform(&attr_descs)?;
                    let mut values = Vec::with_capacity(desc.columns.len());
                    for (column, datum) in desc.columns.iter().zip(raw.into_iter()) {
                        values.push(decode_value(column, datum)?);
                    }
                    *materialized = Some(values);
                }
                Ok(materialized.as_ref().unwrap().as_slice())
            }
        }
    }

    pub fn into_values(mut self) -> Result<Vec<Value>, ExecError> {
        Ok(self.values()?.to_vec())
    }
}

pub fn executor_start(plan: Plan) -> PlanState {
    match plan {
        Plan::SeqScan { rel, desc } => PlanState::SeqScan(SeqScanState {
            rel,
            desc,
            scan: None,
        }),
        Plan::Filter { input, predicate } => PlanState::Filter(FilterState {
            input: Box::new(executor_start(*input)),
            predicate,
        }),
        Plan::Projection { input, targets } => PlanState::Projection(ProjectionState {
            input: Box::new(executor_start(*input)),
            targets,
        }),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementResult {
    Query {
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    AffectedRows(usize),
}

pub fn execute_plan(
    plan: Plan,
    ctx: &mut ExecutorContext<'_>,
) -> Result<StatementResult, ExecError> {
    let mut state = executor_start(plan);
    let mut rows = Vec::new();
    let mut column_names = None;
    while let Some(slot) = exec_next(&mut state, ctx)? {
        if column_names.is_none() {
            column_names = Some(slot.column_names().to_vec());
        }
        rows.push(slot.into_values()?);
    }
    Ok(StatementResult::Query {
        column_names: column_names.unwrap_or_default(),
        rows,
    })
}

pub fn execute_sql(
    sql: &str,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    let stmt = parse_statement(sql)?;
    execute_statement(stmt, catalog, ctx, xid)
}

pub fn execute_statement(
    stmt: Statement,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    let cid = ctx.next_command_id;
    ctx.snapshot = ctx.txns.snapshot_for_command(xid, cid)?;
    let result = match stmt {
        Statement::Select(stmt) => execute_plan(build_plan(&stmt, catalog)?, ctx),
        Statement::CreateTable(stmt) => execute_create_table(stmt, catalog),
        Statement::DropTable(stmt) => execute_drop_table(stmt, catalog, ctx),
        Statement::Insert(stmt) => execute_insert(bind_insert(&stmt, catalog)?, ctx, xid, cid),
        Statement::Update(stmt) => execute_update(bind_update(&stmt, catalog)?, ctx, xid, cid),
        Statement::Delete(stmt) => execute_delete(bind_delete(&stmt, catalog)?, ctx, xid),
    };
    ctx.next_command_id = ctx.next_command_id.saturating_add(1);
    result
}

fn execute_create_table(
    stmt: crate::parser::CreateTableStatement,
    catalog: &mut Catalog,
) -> Result<StatementResult, ExecError> {
    let _entry = bind_create_table(&stmt, catalog)?;
    Ok(StatementResult::AffectedRows(0))
}

fn execute_drop_table(
    stmt: DropTableStatement,
    catalog: &mut Catalog,
    ctx: &mut ExecutorContext<'_>,
) -> Result<StatementResult, ExecError> {
    let entry = catalog
        .drop_table(&stmt.table_name)
        .map_err(|err| match err {
            crate::catalog::CatalogError::UnknownTable(name) => ExecError::Parse(ParseError::TableDoesNotExist(name)),
            other => ExecError::Parse(ParseError::UnexpectedToken {
                expected: "droppable table",
                actual: format!("{other:?}"),
            }),
        })?;

    let _ = ctx.pool.invalidate_relation(entry.rel);
    ctx.pool.storage_mut().smgr.unlink(entry.rel, None, false);
    Ok(StatementResult::AffectedRows(0))
}

pub fn exec_next(
    state: &mut PlanState,
    ctx: &mut ExecutorContext<'_>,
) -> Result<Option<TupleSlot>, ExecError> {
    match state {
        PlanState::SeqScan(scan) => exec_seq_scan(scan, ctx),
        PlanState::Filter(filter) => exec_filter(filter, ctx),
        PlanState::Projection(projection) => exec_projection(projection, ctx),
    }
}

fn exec_seq_scan(
    state: &mut SeqScanState,
    ctx: &mut ExecutorContext<'_>,
) -> Result<Option<TupleSlot>, ExecError> {
    if state.scan.is_none() {
        state.scan = Some(heap_scan_begin_visible(
            ctx.pool,
            state.rel,
            ctx.snapshot.clone(),
        )?);
    }

    let scan = state.scan.as_mut().unwrap();
    if let Some((tid, tuple)) = heap_scan_next_visible(ctx.pool, ctx.client_id, ctx.txns, scan)? {
        Ok(Some(TupleSlot::from_heap_tuple(
            state.desc.clone(),
            tid,
            tuple,
        )))
    } else {
        Ok(None)
    }
}

fn exec_filter(
    state: &mut FilterState,
    ctx: &mut ExecutorContext<'_>,
) -> Result<Option<TupleSlot>, ExecError> {
    loop {
        let Some(mut slot) = exec_next(&mut state.input, ctx)? else {
            return Ok(None);
        };

        match eval_expr(&state.predicate, &mut slot)? {
            Value::Bool(true) => return Ok(Some(slot)),
            Value::Bool(false) | Value::Null => continue,
            other => return Err(ExecError::NonBoolQual(other)),
        }
    }
}

fn exec_projection(
    state: &mut ProjectionState,
    ctx: &mut ExecutorContext<'_>,
) -> Result<Option<TupleSlot>, ExecError> {
    let Some(mut input) = exec_next(&mut state.input, ctx)? else {
        return Ok(None);
    };

    let mut values = Vec::with_capacity(state.targets.len());
    let mut names = Vec::with_capacity(state.targets.len());
    for target in &state.targets {
        values.push(eval_expr(&target.expr, &mut input)?);
        names.push(target.name.clone());
    }

    Ok(Some(TupleSlot::virtual_row(names, values)))
}

pub fn eval_expr(expr: &Expr, slot: &mut TupleSlot) -> Result<Value, ExecError> {
    match expr {
        Expr::Column(index) => slot
            .values()?
            .get(*index)
            .cloned()
            .ok_or(ExecError::InvalidColumn(*index)),
        Expr::Const(value) => Ok(value.clone()),
        Expr::Eq(left, right) => {
            compare_values("=", eval_expr(left, slot)?, eval_expr(right, slot)?)
        }
        Expr::Lt(left, right) => order_values(
            "<",
            eval_expr(left, slot)?,
            eval_expr(right, slot)?,
            |a, b| a < b,
        ),
        Expr::Gt(left, right) => order_values(
            ">",
            eval_expr(left, slot)?,
            eval_expr(right, slot)?,
            |a, b| a > b,
        ),
        Expr::And(left, right) => eval_and(eval_expr(left, slot)?, eval_expr(right, slot)?),
        Expr::Or(left, right) => eval_or(eval_expr(left, slot)?, eval_expr(right, slot)?),
        Expr::Not(inner) => match eval_expr(inner, slot)? {
            Value::Bool(value) => Ok(Value::Bool(!value)),
            Value::Null => Ok(Value::Null),
            other => Err(ExecError::NonBoolQual(other)),
        },
        Expr::IsNull(inner) => Ok(Value::Bool(matches!(eval_expr(inner, slot)?, Value::Null))),
    }
}

fn execute_insert(
    stmt: BoundInsertStatement,
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    let column_names: Vec<String> = stmt.desc.columns.iter().map(|c| c.name.clone()).collect();
    let mut touched_blocks = std::collections::BTreeSet::new();

    for row in &stmt.values {
        let mut slot =
            TupleSlot::virtual_row(column_names.clone(), vec![Value::Null; stmt.desc.columns.len()]);
        let mut values = vec![Value::Null; stmt.desc.columns.len()];
        for (column_index, expr) in stmt.target_indexes.iter().zip(row.iter()) {
            values[*column_index] = eval_expr(expr, &mut slot)?;
        }

        let tuple = tuple_from_values(&stmt.desc, &values)?;
        let tid = heap_insert_mvcc_with_cid(ctx.pool, ctx.client_id, stmt.rel, xid, cid, &tuple)?;
        touched_blocks.insert(tid.block_number);
    }

    for block_number in touched_blocks {
        heap_flush(ctx.pool, ctx.client_id, stmt.rel, block_number)?;
    }

    Ok(StatementResult::AffectedRows(stmt.values.len()))
}

fn execute_update(
    stmt: BoundUpdateStatement,
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
    cid: CommandId,
) -> Result<StatementResult, ExecError> {
    let mut scan = heap_scan_begin_visible(ctx.pool, stmt.rel, ctx.snapshot.clone())?;
    let mut affected_rows = 0;

    while let Some((tid, tuple)) =
        heap_scan_next_visible(ctx.pool, ctx.client_id, ctx.txns, &mut scan)?
    {
        let mut slot = TupleSlot::from_heap_tuple(stmt.desc.clone(), tid, tuple);
        if !predicate_matches(stmt.predicate.as_ref(), &mut slot)? {
            continue;
        }
        let original_values = slot.into_values()?;
        let mut eval_slot = TupleSlot::virtual_row(
            stmt.desc.columns.iter().map(|c| c.name.clone()).collect(),
            original_values.clone(),
        );
        let mut values = original_values;
        for assignment in &stmt.assignments {
            values[assignment.column_index] = eval_expr(&assignment.expr, &mut eval_slot)?;
        }

        let replacement = tuple_from_values(&stmt.desc, &values)?;
        let new_tid = heap_update_with_cid(
            ctx.pool,
            ctx.client_id,
            stmt.rel,
            ctx.txns,
            xid,
            cid,
            tid,
            &replacement,
        )?;
        heap_flush(ctx.pool, ctx.client_id, stmt.rel, tid.block_number)?;
        if new_tid.block_number != tid.block_number {
            heap_flush(ctx.pool, ctx.client_id, stmt.rel, new_tid.block_number)?;
        }
        affected_rows += 1;
    }

    Ok(StatementResult::AffectedRows(affected_rows))
}

fn execute_delete(
    stmt: BoundDeleteStatement,
    ctx: &mut ExecutorContext<'_>,
    xid: TransactionId,
) -> Result<StatementResult, ExecError> {
    let mut scan = heap_scan_begin_visible(ctx.pool, stmt.rel, ctx.snapshot.clone())?;
    let mut targets = Vec::new();

    while let Some((tid, tuple)) =
        heap_scan_next_visible(ctx.pool, ctx.client_id, ctx.txns, &mut scan)?
    {
        let mut slot = TupleSlot::from_heap_tuple(stmt.desc.clone(), tid, tuple);
        if !predicate_matches(stmt.predicate.as_ref(), &mut slot)? {
            continue;
        }
        targets.push(tid);
    }

    for tid in &targets {
        heap_delete(ctx.pool, ctx.client_id, stmt.rel, ctx.txns, xid, *tid)?;
        heap_flush(ctx.pool, ctx.client_id, stmt.rel, tid.block_number)?;
    }

    Ok(StatementResult::AffectedRows(targets.len()))
}

fn predicate_matches(predicate: Option<&Expr>, slot: &mut TupleSlot) -> Result<bool, ExecError> {
    let Some(predicate) = predicate else {
        return Ok(true);
    };
    match eval_expr(predicate, slot)? {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other)),
    }
}

fn tuple_from_values(desc: &RelationDesc, values: &[Value]) -> Result<HeapTuple, ExecError> {
    let tuple_values = desc
        .columns
        .iter()
        .zip(values.iter())
        .map(|(column, value)| encode_value(column, value))
        .collect::<Result<Vec<_>, _>>()?;
    HeapTuple::from_values(&desc.attribute_descs(), &tuple_values).map_err(ExecError::from)
}

fn encode_value(column: &ColumnDesc, value: &Value) -> Result<TupleValue, ExecError> {
    match (column.ty, value) {
        (_, Value::Null) => {
            if !column.storage.nullable {
                Err(ExecError::MissingRequiredColumn(column.name.clone()))
            } else {
                Ok(TupleValue::Null)
            }
        }
        (ScalarType::Int32, Value::Int32(v)) => Ok(TupleValue::Bytes(v.to_le_bytes().to_vec())),
        (ScalarType::Text, Value::Text(v)) => Ok(TupleValue::Bytes(v.as_bytes().to_vec())),
        (ScalarType::Bool, Value::Bool(v)) => Ok(TupleValue::Bytes(vec![u8::from(*v)])),
        (_, other) => Err(ExecError::TypeMismatch {
            op: "assignment",
            left: Value::Null,
            right: other.clone(),
        }),
    }
}

fn eval_and(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(true), Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(true), Value::Null)
        | (Value::Null, Value::Bool(true))
        | (Value::Null, Value::Null) => Ok(Value::Null),
        (left, right) => Err(ExecError::TypeMismatch {
            op: "AND",
            left,
            right,
        }),
    }
}

fn eval_or(left: Value, right: Value) -> Result<Value, ExecError> {
    match (left, right) {
        (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
        (Value::Bool(false), Value::Bool(false)) => Ok(Value::Bool(false)),
        (Value::Bool(false), Value::Null)
        | (Value::Null, Value::Bool(false))
        | (Value::Null, Value::Null) => Ok(Value::Null),
        (left, right) => Err(ExecError::TypeMismatch {
            op: "OR",
            left,
            right,
        }),
    }
}

fn compare_values(op: &'static str, left: Value, right: Value) -> Result<Value, ExecError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Bool(l == r)),
        (Value::Text(l), Value::Text(r)) => Ok(Value::Bool(l == r)),
        (Value::Bool(l), Value::Bool(r)) => Ok(Value::Bool(l == r)),
        _ => Err(ExecError::TypeMismatch { op, left, right }),
    }
}

fn order_values<F>(op: &'static str, left: Value, right: Value, cmp: F) -> Result<Value, ExecError>
where
    F: FnOnce(i32, i32) -> bool + Copy,
{
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (&left, &right) {
        (Value::Int32(l), Value::Int32(r)) => Ok(Value::Bool(cmp(*l, *r))),
        (Value::Text(l), Value::Text(r)) => Ok(Value::Bool(match op {
            "<" => l < r,
            ">" => l > r,
            _ => unreachable!(),
        })),
        _ => Err(ExecError::TypeMismatch { op, left, right }),
    }
}

fn decode_value(column: &ColumnDesc, bytes: Option<Vec<u8>>) -> Result<Value, ExecError> {
    let Some(bytes) = bytes else {
        return Ok(Value::Null);
    };

    match column.ty {
        ScalarType::Int32 => {
            if column.storage.attlen != 4 || bytes.len() != 4 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty,
                    attlen: column.storage.attlen,
                });
            }
            Ok(Value::Int32(i32::from_le_bytes(
                bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| ExecError::InvalidStorageValue {
                        column: column.name.clone(),
                        details: "int4 must be exactly 4 bytes".into(),
                    })?,
            )))
        }
        ScalarType::Text => {
            if column.storage.attlen != -1 && column.storage.attlen != -2 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty,
                    attlen: column.storage.attlen,
                });
            }
            String::from_utf8(bytes)
                .map(Value::Text)
                .map_err(|e| ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: e.to_string(),
                })
        }
        ScalarType::Bool => {
            if column.storage.attlen != 1 || bytes.len() != 1 {
                return Err(ExecError::UnsupportedStorageType {
                    column: column.name.clone(),
                    ty: column.ty,
                    attlen: column.storage.attlen,
                });
            }
            match bytes[0] {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                other => Err(ExecError::InvalidStorageValue {
                    column: column.name.clone(),
                    details: format!("invalid bool byte {}", other),
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::heap::am::{heap_flush, heap_insert_mvcc, heap_update};
    use crate::access::heap::mvcc::INVALID_TRANSACTION_ID;
    use crate::access::heap::tuple::{AttributeAlign, TupleValue};
    use crate::parser::{Catalog, CatalogEntry};
    use crate::storage::smgr::MdStorageManager;
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

    fn relation_desc() -> RelationDesc {
        RelationDesc {
            columns: vec![
                ColumnDesc {
                    name: "id".into(),
                    storage: AttributeDesc {
                        name: "id".into(),
                        attlen: 4,
                        attalign: AttributeAlign::Int,
                        nullable: false,
                    },
                    ty: ScalarType::Int32,
                },
                ColumnDesc {
                    name: "name".into(),
                    storage: AttributeDesc {
                        name: "name".into(),
                        attlen: -1,
                        attalign: AttributeAlign::Int,
                        nullable: false,
                    },
                    ty: ScalarType::Text,
                },
                ColumnDesc {
                    name: "note".into(),
                    storage: AttributeDesc {
                        name: "note".into(),
                        attlen: -1,
                        attalign: AttributeAlign::Int,
                        nullable: true,
                    },
                    ty: ScalarType::Text,
                },
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

    fn run_plan(
        base: &PathBuf,
        txns: &TransactionManager,
        plan: Plan,
    ) -> Result<Vec<(Vec<String>, Vec<Value>)>, ExecError> {
        let smgr = MdStorageManager::new(base);
        let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let mut state = executor_start(plan);
        let mut ctx = ExecutorContext {
            pool: &mut pool,
            txns,
            snapshot: txns.snapshot(INVALID_TRANSACTION_ID).unwrap(),
            client_id: 42,
            next_command_id: 0,
        };

        let mut rows = Vec::new();
        while let Some(slot) = exec_next(&mut state, &mut ctx)? {
            rows.push((slot.column_names().to_vec(), slot.into_values()?));
        }
        Ok(rows)
    }

    fn run_sql(
        base: &PathBuf,
        txns: &TransactionManager,
        xid: TransactionId,
        sql: &str,
    ) -> Result<StatementResult, ExecError> {
        let smgr = MdStorageManager::new(base);
        let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let mut ctx = ExecutorContext {
            pool: &mut pool,
            txns,
            snapshot: txns.snapshot(xid).unwrap(),
            client_id: 77,
            next_command_id: 0,
        };
        let mut catalog = catalog();
        execute_sql(sql, &mut catalog, &mut ctx, xid)
    }

    #[test]
    fn expr_eval_obeys_null_semantics() {
        let desc = relation_desc();
        let mut slot = TupleSlot::virtual_row(
            desc.columns.iter().map(|c| c.name.clone()).collect(),
            vec![Value::Int32(7), Value::Text("alice".into()), Value::Null],
        );

        assert_eq!(
            eval_expr(
                &Expr::Eq(
                    Box::new(Expr::Column(0)),
                    Box::new(Expr::Const(Value::Int32(7)))
                ),
                &mut slot
            )
            .unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            eval_expr(
                &Expr::Eq(
                    Box::new(Expr::Column(2)),
                    Box::new(Expr::Const(Value::Text("x".into())))
                ),
                &mut slot
            )
            .unwrap(),
            Value::Null
        );
        assert_eq!(
            eval_expr(
                &Expr::And(
                    Box::new(Expr::Const(Value::Bool(true))),
                    Box::new(Expr::Const(Value::Null))
                ),
                &mut slot
            )
            .unwrap(),
            Value::Null
        );
    }

    #[test]
    fn physical_slot_lazily_deforms_heap_tuple() {
        let mut slot = TupleSlot::from_heap_tuple(
            relation_desc(),
            ItemPointerData {
                block_number: 0,
                offset_number: 1,
            },
            tuple(1, "alice", None),
        );

        assert_eq!(
            slot.values().unwrap(),
            &[Value::Int32(1), Value::Text("alice".into()), Value::Null,]
        );
        assert_eq!(
            slot.tid(),
            Some(ItemPointerData {
                block_number: 0,
                offset_number: 1,
            })
        );
    }

    #[test]
    fn seqscan_filter_projection_returns_expected_rows() {
        let base = temp_dir("scan_filter_project");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let smgr = MdStorageManager::new(&base);
        let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);

        let xid = txns.begin();
        let rows = [
            tuple(1, "alice", Some("alpha")),
            tuple(2, "bob", None),
            tuple(3, "carol", Some("gamma")),
        ];
        let mut blocks = Vec::new();
        for row in rows {
            let tid = heap_insert_mvcc(&mut pool, 1, rel(), xid, &row).unwrap();
            blocks.push(tid.block_number);
        }
        txns.commit(xid).unwrap();
        blocks.sort();
        blocks.dedup();
        for block in blocks {
            heap_flush(&mut pool, 1, rel(), block).unwrap();
        }
        drop(pool);

        let plan = Plan::Projection {
            input: Box::new(Plan::Filter {
                input: Box::new(Plan::SeqScan {
                    rel: rel(),
                    desc: relation_desc(),
                }),
                predicate: Expr::Gt(
                    Box::new(Expr::Column(0)),
                    Box::new(Expr::Const(Value::Int32(1))),
                ),
            }),
            targets: vec![
                TargetEntry {
                    name: "name".into(),
                    expr: Expr::Column(1),
                },
                TargetEntry {
                    name: "note_is_null".into(),
                    expr: Expr::IsNull(Box::new(Expr::Column(2))),
                },
            ],
        };

        let rows = run_plan(&base, &txns, plan).unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    vec!["name".into(), "note_is_null".into()],
                    vec![Value::Text("bob".into()), Value::Bool(true)]
                ),
                (
                    vec!["name".into(), "note_is_null".into()],
                    vec![Value::Text("carol".into()), Value::Bool(false)]
                )
            ]
        );
    }

    #[test]
    fn seqscan_skips_superseded_versions() {
        let base = temp_dir("visible_versions");
        let mut txns = TransactionManager::new_durable(&base).unwrap();
        let smgr = MdStorageManager::new(&base);
        let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);

        let insert_xid = txns.begin();
        let old_tid = heap_insert_mvcc(
            &mut pool,
            1,
            rel(),
            insert_xid,
            &tuple(1, "alice", Some("old")),
        )
        .unwrap();
        txns.commit(insert_xid).unwrap();

        let update_xid = txns.begin();
        let new_tid = heap_update(
            &mut pool,
            1,
            rel(),
            &txns,
            update_xid,
            old_tid,
            &tuple(1, "alice", Some("new")),
        )
        .unwrap();
        txns.commit(update_xid).unwrap();
        heap_flush(&mut pool, 1, rel(), old_tid.block_number).unwrap();
        if new_tid.block_number != old_tid.block_number {
            heap_flush(&mut pool, 1, rel(), new_tid.block_number).unwrap();
        }
        drop(pool);

        let plan = Plan::SeqScan {
            rel: rel(),
            desc: relation_desc(),
        };
        let rows = run_plan(&base, &txns, plan).unwrap();
        assert_eq!(
            rows,
            vec![(
                vec!["id".into(), "name".into(), "note".into()],
                vec![
                    Value::Int32(1),
                    Value::Text("alice".into()),
                    Value::Text("new".into())
                ]
            )]
        );
    }

    #[test]
    fn insert_sql_inserts_row() {
        let base = temp_dir("insert_sql");
        let mut txns = TransactionManager::new_durable(&base).unwrap();

        let xid = txns.begin();
        assert_eq!(
            run_sql(
                &base,
                &txns,
                xid,
                "insert into people (id, name, note) values (1, 'alice', 'alpha')",
            )
            .unwrap(),
            StatementResult::AffectedRows(1)
        );
        txns.commit(xid).unwrap();

        match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select name, note from people",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Text("alice".into()),
                        Value::Text("alpha".into())
                    ]]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    #[test]
    fn insert_sql_inserts_multiple_rows() {
        let base = temp_dir("insert_multi_sql");
        let mut txns = TransactionManager::new_durable(&base).unwrap();

        let xid = txns.begin();
        assert_eq!(
            run_sql(
                &base,
                &txns,
                xid,
                "insert into people (id, name, note) values (1, 'alice', 'alpha'), (2, 'bob', null)",
            )
            .unwrap(),
            StatementResult::AffectedRows(2)
        );
        txns.commit(xid).unwrap();

        match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select id, name, note from people",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(
                    rows,
                    vec![
                        vec![
                            Value::Int32(1),
                            Value::Text("alice".into()),
                            Value::Text("alpha".into())
                        ],
                        vec![Value::Int32(2), Value::Text("bob".into()), Value::Null]
                    ]
                );
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    #[test]
    fn update_sql_updates_matching_rows() {
        let base = temp_dir("update_sql");
        let mut txns = TransactionManager::new_durable(&base).unwrap();

        let insert_xid = txns.begin();
        run_sql(
            &base,
            &txns,
            insert_xid,
            "insert into people (id, name, note) values (1, 'alice', 'old')",
        )
        .unwrap();
        txns.commit(insert_xid).unwrap();

        let update_xid = txns.begin();
        assert_eq!(
            run_sql(
                &base,
                &txns,
                update_xid,
                "update people set note = 'new' where id = 1",
            )
            .unwrap(),
            StatementResult::AffectedRows(1)
        );
        txns.commit(update_xid).unwrap();

        match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select note from people where id = 1",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Text("new".into())]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }

    #[test]
    fn delete_sql_deletes_matching_rows() {
        let base = temp_dir("delete_sql");
        let mut txns = TransactionManager::new_durable(&base).unwrap();

        let insert_xid = txns.begin();
        run_sql(
            &base,
            &txns,
            insert_xid,
            "insert into people (id, name, note) values (1, 'alice', null)",
        )
        .unwrap();
        run_sql(
            &base,
            &txns,
            insert_xid,
            "insert into people (id, name, note) values (2, 'bob', 'keep')",
        )
        .unwrap();
        txns.commit(insert_xid).unwrap();

        let delete_xid = txns.begin();
        assert_eq!(
            run_sql(
                &base,
                &txns,
                delete_xid,
                "delete from people where note is null",
            )
            .unwrap(),
            StatementResult::AffectedRows(1)
        );
        txns.commit(delete_xid).unwrap();

        match run_sql(
            &base,
            &txns,
            INVALID_TRANSACTION_ID,
            "select name from people",
        )
        .unwrap()
        {
            StatementResult::Query { rows, .. } => {
                assert_eq!(rows, vec![vec![Value::Text("bob".into())]]);
            }
            other => panic!("expected query result, got {:?}", other),
        }
    }
}
