use crate::access::heap::am::VisibleHeapScan;
use crate::access::heap::tuple::{AttributeDesc, HeapTuple, ItemPointerData};
use crate::compact_string::CompactString;
use crate::RelFileLocator;
use std::rc::Rc;
use std::time::Duration;

use super::expr::decode_value;

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

impl RelationDesc {
    pub fn attribute_descs(&self) -> Vec<AttributeDesc> {
        self.columns.iter().map(|c| c.storage.clone()).collect()
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Int32(i32),
    Float64(f64),
    Text(CompactString),
    Bool(bool),
    Null,
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::Text(a), Value::Text(b)) => a.as_str() == b.as_str(),
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl Eq for Value {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetEntry {
    pub name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByEntry {
    pub expr: Expr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFunc {
    pub fn name(&self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggAccum {
    pub func: AggFunc,
    pub arg: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(usize),
    Const(Value),
    Add(Box<Expr>, Box<Expr>),
    Negate(Box<Expr>),
    Eq(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    IsDistinctFrom(Box<Expr>, Box<Expr>),
    IsNotDistinctFrom(Box<Expr>, Box<Expr>),
    Random,
    CurrentTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    Result,
    SeqScan {
        rel: RelFileLocator,
        desc: RelationDesc,
    },
    NestedLoopJoin {
        left: Box<Plan>,
        right: Box<Plan>,
        on: Expr,
    },
    Filter {
        input: Box<Plan>,
        predicate: Expr,
    },
    OrderBy {
        input: Box<Plan>,
        items: Vec<OrderByEntry>,
    },
    Limit {
        input: Box<Plan>,
        limit: Option<usize>,
        offset: usize,
    },
    Projection {
        input: Box<Plan>,
        targets: Vec<TargetEntry>,
    },
    Aggregate {
        input: Box<Plan>,
        group_by: Vec<Expr>,
        accumulators: Vec<AggAccum>,
        having: Option<Expr>,
        output_columns: Vec<String>,
    },
}

impl Plan {
    /// Extract output column names from the plan tree.
    pub fn column_names(&self) -> Vec<String> {
        match self {
            Plan::Result => vec![],
            Plan::SeqScan { desc, .. } => desc.columns.iter().map(|c| c.name.clone()).collect(),
            Plan::Filter { input, .. } | Plan::OrderBy { input, .. } | Plan::Limit { input, .. } => {
                input.column_names()
            }
            Plan::Projection { targets, .. } => targets.iter().map(|t| t.name.clone()).collect(),
            Plan::Aggregate { output_columns, .. } => output_columns.clone(),
            Plan::NestedLoopJoin { left, right, .. } => {
                let mut names = left.column_names();
                names.extend(right.column_names());
                names
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleSlot {
    pub(crate) column_names: Rc<[String]>,
    pub(crate) source: SlotSource,
}

pub(crate) enum SlotSource {
    Physical {
        desc: Rc<RelationDesc>,
        attr_descs: Rc<[AttributeDesc]>,
        tid: ItemPointerData,
        tuple: HeapTuple,
        materialized: Option<Vec<Value>>,
    },
    Virtual {
        values: Vec<Value>,
    },
    /// Zero-copy slot: holds a raw pointer to tuple bytes on a pinned buffer
    /// page. User data on the page is immutable (heap_page_replace_tuple only
    /// writes headers), and the pin prevents eviction.
    ///
    /// SAFETY: the pointer is valid as long as the buffer is pinned. The caller
    /// must ensure the slot is consumed (or materialized) before the pin is
    /// released.
    BufferHeap {
        tuple_ptr: *const u8,
        tuple_len: usize,
        buffer_id: usize,
        decoder: Rc<super::tuple_decoder::CompiledTupleDecoder>,
        materialized: Option<Vec<Value>>,
    },
}

impl std::fmt::Debug for SlotSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SlotSource::Physical { desc, tid, materialized, .. } => f
                .debug_struct("Physical")
                .field("desc_cols", &desc.columns.len())
                .field("tid", tid)
                .field("materialized", &materialized.is_some())
                .finish(),
            SlotSource::Virtual { values } => f
                .debug_struct("Virtual")
                .field("len", &values.len())
                .finish(),
            SlotSource::BufferHeap { tuple_len, buffer_id, materialized, .. } => f
                .debug_struct("BufferHeap")
                .field("tuple_len", tuple_len)
                .field("buffer_id", buffer_id)
                .field("materialized", &materialized.is_some())
                .finish(),
        }
    }
}

impl Clone for SlotSource {
    fn clone(&self) -> Self {
        match self {
            SlotSource::Physical { desc, attr_descs, tid, tuple, materialized } => {
                SlotSource::Physical {
                    desc: Rc::clone(desc),
                    attr_descs: Rc::clone(attr_descs),
                    tid: *tid,
                    tuple: tuple.clone(),
                    materialized: materialized.clone(),
                }
            }
            SlotSource::Virtual { values } => SlotSource::Virtual { values: values.clone() },
            SlotSource::BufferHeap { materialized: Some(values), .. } => {
                // Cloning a BufferHeap forces materialization — the clone becomes
                // a self-contained Virtual slot so it doesn't depend on the pin.
                SlotSource::Virtual { values: values.clone() }
            }
            SlotSource::BufferHeap { .. } => {
                panic!("cannot clone unmaterialized BufferHeap slot — call materialize() first")
            }
        }
    }
}

impl PartialEq for SlotSource {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SlotSource::Virtual { values: a }, SlotSource::Virtual { values: b }) => a == b,
            _ => false,
        }
    }
}

impl Eq for SlotSource {}

#[derive(Debug, Clone, Default)]
pub struct NodeExecStats {
    pub loops: u64,
    pub rows: u64,
    pub total_time: Duration,
}

/// The inner enum holding node-specific state.
#[derive(Debug)]
pub enum PlanStateKind {
    Result(ResultState),
    SeqScan(SeqScanState),
    NestedLoopJoin(NestedLoopJoinState),
    Filter(FilterState),
    OrderBy(OrderByState),
    Limit(LimitState),
    Projection(ProjectionState),
    Aggregate(AggregateState),
}

/// Executor node state. The function pointer avoids per-tuple match dispatch,
/// like PostgreSQL's `ExecProcNode`.
pub struct PlanState {
    pub kind: PlanStateKind,
    /// Direct function pointer for this node type — set once at plan init.
    pub(crate) exec_proc_node: fn(&mut PlanState, &mut super::ExecutorContext) -> Result<Option<TupleSlot>, super::ExecError>,
}

impl std::fmt::Debug for PlanState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.kind.fmt(f)
    }
}

#[derive(Debug)]
pub struct ResultState {
    pub(crate) emitted: bool,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct SeqScanState {
    pub(crate) rel: RelFileLocator,
    pub(crate) column_names: Rc<[String]>,
    pub(crate) scan: Option<VisibleHeapScan>,
    pub(crate) decoder: Rc<super::tuple_decoder::CompiledTupleDecoder>,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct FilterState {
    pub(crate) input: Box<PlanState>,
    pub(crate) predicate: Expr,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct NestedLoopJoinState {
    pub(crate) left: Box<PlanState>,
    pub(crate) right: Box<PlanState>,
    pub(crate) on: Expr,
    pub(crate) right_rows: Option<Vec<TupleSlot>>,
    pub(crate) current_left: Option<TupleSlot>,
    pub(crate) right_index: usize,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct ProjectionState {
    pub(crate) input: Box<PlanState>,
    pub(crate) targets: Vec<TargetEntry>,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct OrderByState {
    pub(crate) input: Box<PlanState>,
    pub(crate) items: Vec<OrderByEntry>,
    pub(crate) rows: Option<Vec<TupleSlot>>,
    pub(crate) next_index: usize,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct LimitState {
    pub(crate) input: Box<PlanState>,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: usize,
    pub(crate) skipped: usize,
    pub(crate) returned: usize,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct AggregateState {
    pub(crate) input: Box<PlanState>,
    pub(crate) group_by: Vec<Expr>,
    pub(crate) accumulators: Vec<AggAccum>,
    pub(crate) having: Option<Expr>,
    pub(crate) output_columns: Vec<String>,
    pub(crate) result_rows: Option<Vec<TupleSlot>>,
    pub(crate) next_index: usize,
    pub(crate) stats: NodeExecStats,
}

impl TupleSlot {
    pub fn from_heap_tuple(
        desc: Rc<RelationDesc>,
        attr_descs: Rc<[AttributeDesc]>,
        column_names: Rc<[String]>,
        tid: ItemPointerData,
        tuple: HeapTuple,
    ) -> Self {
        Self {
            column_names,
            source: SlotSource::Physical {
                desc,
                attr_descs,
                tid,
                tuple,
                materialized: None,
            },
        }
    }

    pub fn virtual_row(column_names: Rc<[String]>, values: Vec<Value>) -> Self {
        Self {
            column_names,
            source: SlotSource::Virtual { values },
        }
    }

    pub fn column_names(&self) -> &Rc<[String]> {
        &self.column_names
    }

    /// Create a zero-copy slot that points directly at on-page tuple bytes.
    ///
    /// SAFETY: `tuple_ptr` must point to valid tuple bytes on a pinned buffer
    /// page. The slot must be consumed or materialized before the pin is released.
    pub(crate) unsafe fn from_buffer_heap(
        column_names: Rc<[String]>,
        tuple_ptr: *const u8,
        tuple_len: usize,
        buffer_id: usize,
        decoder: Rc<super::tuple_decoder::CompiledTupleDecoder>,
    ) -> Self {
        Self {
            column_names,
            source: SlotSource::BufferHeap {
                tuple_ptr,
                tuple_len,
                buffer_id,
                decoder,
                materialized: None,
            },
        }
    }

    /// Convert to a self-contained Virtual slot, decoding from the raw pointer
    /// if needed. Must be called while the buffer is still pinned.
    pub fn materialize(mut self) -> Result<Self, super::ExecError> {
        match &mut self.source {
            SlotSource::Virtual { .. } | SlotSource::Physical { .. } => Ok(self),
            SlotSource::BufferHeap { materialized: Some(_), .. } => {
                // Already decoded — extract into Virtual.
                let SlotSource::BufferHeap { materialized, .. } = self.source else { unreachable!() };
                Ok(Self {
                    column_names: self.column_names,
                    source: SlotSource::Virtual { values: materialized.unwrap() },
                })
            }
            SlotSource::BufferHeap { tuple_ptr, tuple_len, decoder, materialized, .. } => {
                let bytes = unsafe { std::slice::from_raw_parts(*tuple_ptr, *tuple_len) };
                let mut values = Vec::new();
                decoder.decode_into(bytes, &mut values)?;
                *materialized = Some(values);
                let SlotSource::BufferHeap { materialized, .. } = self.source else { unreachable!() };
                Ok(Self {
                    column_names: self.column_names,
                    source: SlotSource::Virtual { values: materialized.unwrap() },
                })
            }
        }
    }

    pub fn tid(&self) -> Option<ItemPointerData> {
        match &self.source {
            SlotSource::Physical { tid, .. } => Some(*tid),
            SlotSource::Virtual { .. } | SlotSource::BufferHeap { .. } => None,
        }
    }

    pub fn values(&mut self) -> Result<&[Value], super::ExecError> {
        match &mut self.source {
            SlotSource::Virtual { values } => Ok(values.as_slice()),
            SlotSource::Physical {
                desc,
                attr_descs,
                tuple,
                materialized,
                ..
            } => {
                if materialized.is_none() {
                    let raw = tuple.deform(attr_descs)?;
                    let mut values = Vec::with_capacity(desc.columns.len());
                    for (column, datum) in desc.columns.iter().zip(raw.into_iter()) {
                        values.push(decode_value(column, datum)?);
                    }
                    *materialized = Some(values);
                }
                Ok(materialized.as_ref().unwrap().as_slice())
            }
            SlotSource::BufferHeap { tuple_ptr, tuple_len, decoder, materialized, .. } => {
                if materialized.is_none() {
                    // SAFETY: pointer is valid because the buffer is pinned
                    // and user data is immutable on the page.
                    let bytes = unsafe { std::slice::from_raw_parts(*tuple_ptr, *tuple_len) };
                    let mut values = Vec::new();
                    decoder.decode_into(bytes, &mut values)?;
                    *materialized = Some(values);
                }
                Ok(materialized.as_ref().unwrap().as_slice())
            }
        }
    }

    pub fn into_values(mut self) -> Result<Vec<Value>, super::ExecError> {
        self.values()?;
        match self.source {
            SlotSource::Virtual { values } => Ok(values),
            SlotSource::Physical { materialized: Some(values), .. } => Ok(values),
            SlotSource::Physical { materialized: None, .. } => unreachable!("values() just materialized"),
            SlotSource::BufferHeap { materialized: Some(values), .. } => Ok(values),
            SlotSource::BufferHeap { materialized: None, .. } => unreachable!("values() just materialized"),
        }
    }
}
