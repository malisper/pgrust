use crate::access::heap::am::VisibleHeapScan;
use crate::access::heap::tuple::{AttributeDesc, HeapTuple, ItemPointerData};
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
    Text(String),
    Bool(bool),
    Null,
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::Text(a), Value::Text(b)) => a == b,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleSlot {
    pub(crate) column_names: Rc<[String]>,
    pub(crate) source: SlotSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
}

#[derive(Debug, Clone, Default)]
pub struct NodeExecStats {
    pub loops: u64,
    pub rows: u64,
    pub total_time: Duration,
}

#[derive(Debug)]
pub enum PlanState {
    Result(ResultState),
    SeqScan(SeqScanState),
    NestedLoopJoin(NestedLoopJoinState),
    Filter(FilterState),
    OrderBy(OrderByState),
    Limit(LimitState),
    Projection(ProjectionState),
    Aggregate(AggregateState),
}

#[derive(Debug)]
pub struct ResultState {
    pub(crate) emitted: bool,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct SeqScanState {
    pub(crate) rel: RelFileLocator,
    pub(crate) desc: Rc<RelationDesc>,
    pub(crate) attr_descs: Rc<[AttributeDesc]>,
    pub(crate) column_names: Rc<[String]>,
    pub(crate) scan: Option<VisibleHeapScan>,
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

    pub fn tid(&self) -> Option<ItemPointerData> {
        match &self.source {
            SlotSource::Physical { tid, .. } => Some(*tid),
            SlotSource::Virtual { .. } => None,
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
        }
    }

    pub fn into_values(mut self) -> Result<Vec<Value>, super::ExecError> {
        // Materialize if needed
        self.values()?;
        match self.source {
            SlotSource::Virtual { values } => Ok(values),
            SlotSource::Physical { materialized: Some(values), .. } => Ok(values),
            SlotSource::Physical { materialized: None, .. } => unreachable!("values() just materialized"),
        }
    }
}
