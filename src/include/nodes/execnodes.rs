use crate::backend::access::heap::heapam::VisibleHeapScan;
use crate::backend::access::transam::xact::{Snapshot, TransactionManager};
use crate::backend::executor::hashjoin::{HashJoinPhase, HashJoinTable};
use crate::backend::utils::cache::relcache::IndexRelCacheEntry;
use crate::include::storage::buf_internals::BufferUsageStats;
use crate::include::access::htup::{AttributeDesc, HeapTuple, ItemPointerData};
use crate::include::access::relscan::IndexScanDesc;
use crate::include::access::relscan::ScanDirection;
use crate::include::access::scankey::ScanKeyData;
use crate::include::nodes::plannodes::PlanEstimate;
use crate::{BufferPool, ClientId, OwnedBufferPin, RelFileLocator, SmgrStorageBackend};
use parking_lot::RwLock;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crate::backend::executor::{AccumState, ExecError, ExecutorContext};
pub use crate::include::nodes::datum::{NumericValue, Value};
pub use crate::include::nodes::parsenodes::SqlType;
pub use crate::include::nodes::plannodes::Plan;
pub use crate::include::nodes::primnodes::{
    AggAccum, AggFunc, BuiltinScalarFunction, ColumnDesc, Expr, JoinType, JsonTableFunction,
    OrderByEntry, ProjectSetTarget, QueryColumn, RelationDesc, ScalarType, SetReturningCall,
    TargetEntry, ToastRelationRef,
};

pub struct TupleSlot {
    pub(crate) kind: SlotKind,
    /// Decoded column values, like PG's tts_values[]. Reused across tuples
    /// to avoid per-tuple allocation.
    pub(crate) tts_values: Vec<Value>,
    /// Number of columns decoded so far (0..ncols). Like PG's tts_nvalid.
    pub(crate) tts_nvalid: usize,
    /// Byte offset in the tuple data area after the last decoded column,
    /// used to resume incremental decode for variable-width columns.
    pub(crate) decode_offset: usize,
    /// Compiled tuple decoder, like PG's tts_tupleDescriptor. Set once when
    /// the slot is created; shared via Rc so future scan types can share it.
    pub(crate) decoder: Option<Rc<crate::backend::executor::exec_tuples::CompiledTupleDecoder>>,
    pub(crate) toast: Option<ToastFetchContext>,
}

#[derive(Clone)]
pub(crate) struct ToastFetchContext {
    pub(crate) relation: ToastRelationRef,
    pub(crate) pool: Arc<BufferPool<SmgrStorageBackend>>,
    pub(crate) txns: Arc<RwLock<TransactionManager>>,
    pub(crate) snapshot: Snapshot,
    pub(crate) client_id: ClientId,
}

/// Describes how the slot's underlying tuple data is stored.
/// Like PG's TTS_FLAG_* / BufferHeapTupleTableSlot vs VirtualTupleTableSlot.
pub(crate) enum SlotKind {
    /// No tuple stored. Initial state before first scan tuple.
    Empty,
    /// tts_values is authoritative (no backing tuple to decode from).
    Virtual,
    /// Owned heap tuple from a heap fetch (used by UPDATE/DELETE path).
    HeapTuple {
        desc: Rc<RelationDesc>,
        attr_descs: Rc<[AttributeDesc]>,
        tid: ItemPointerData,
        tuple: HeapTuple,
    },
    /// Zero-copy reference to tuple bytes on a pinned buffer page.
    /// Decoded lazily into tts_values via the slot's `decoder` field.
    BufferHeapTuple {
        desc: Rc<RelationDesc>,
        attr_descs: Rc<[AttributeDesc]>,
        tuple_ptr: *const u8,
        tuple_len: usize,
        pin: Rc<OwnedBufferPin<SmgrStorageBackend>>,
    },
}

impl std::fmt::Debug for SlotKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SlotKind::Empty => write!(f, "Empty"),
            SlotKind::Virtual => write!(f, "Virtual"),
            SlotKind::HeapTuple { tid, .. } => {
                f.debug_struct("HeapTuple").field("tid", tid).finish()
            }
            SlotKind::BufferHeapTuple { tuple_len, pin, .. } => f
                .debug_struct("BufferHeapTuple")
                .field("tuple_len", tuple_len)
                .field("buffer_id", &pin.buffer_id())
                .finish(),
        }
    }
}

impl Clone for SlotKind {
    fn clone(&self) -> Self {
        match self {
            SlotKind::Empty => SlotKind::Empty,
            SlotKind::Virtual => SlotKind::Virtual,
            SlotKind::HeapTuple {
                desc,
                attr_descs,
                tid,
                tuple,
            } => SlotKind::HeapTuple {
                desc: Rc::clone(desc),
                attr_descs: Rc::clone(attr_descs),
                tid: *tid,
                tuple: tuple.clone(),
            },
            SlotKind::BufferHeapTuple { .. } => {
                panic!("cannot clone BufferHeapTuple — call materialize() first")
            }
        }
    }
}

impl PartialEq for SlotKind {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SlotKind::Empty, SlotKind::Empty) => true,
            (SlotKind::Virtual, SlotKind::Virtual) => true,
            _ => false,
        }
    }
}

impl Eq for SlotKind {}

impl std::fmt::Debug for TupleSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TupleSlot")
            .field("kind", &self.kind)
            .field("tts_nvalid", &self.tts_nvalid)
            .field("ncols", &self.ncols())
            .finish()
    }
}

impl Clone for TupleSlot {
    fn clone(&self) -> Self {
        Self {
            kind: match &self.kind {
                SlotKind::BufferHeapTuple { .. } => SlotKind::Virtual,
                other => other.clone(),
            },
            tts_values: self.tts_values.iter().cloned().collect::<Vec<_>>(),
            tts_nvalid: self.tts_nvalid,
            decode_offset: 0,
            decoder: None,
            toast: self.toast.clone(),
        }
    }
}

impl PartialEq for TupleSlot {
    fn eq(&self, other: &Self) -> bool {
        self.tts_values == other.tts_values
    }
}

impl Eq for TupleSlot {}

#[derive(Debug, Clone, Default)]
pub struct NodeExecStats {
    pub loops: u64,
    pub rows: u64,
    pub total_time: Duration,
    pub first_tuple_time: Option<Duration>,
    pub rows_removed_by_filter: u64,
    pub buffer_usage: BufferUsageStats,
    pub buffer_usage_start: Option<BufferUsageStats>,
}

/// Trait for executor plan nodes, like PostgreSQL's ExecProcNode vtable.
/// Each node type implements this trait, and dispatch is via trait object.
///
/// `exec_proc_node` returns a borrowed `&mut TupleSlot` owned by the node.
/// Like PG's ExecProcNode, the caller must consume the slot before the next
/// call (the borrow checker enforces this).
pub trait PlanNode: std::fmt::Debug {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError>;

    /// Re-borrow the slot from the last exec_proc_node call.
    /// Used by filter to return a reference to the child's slot
    /// after evaluating the predicate.
    fn current_slot(&mut self) -> Option<&mut TupleSlot>;

    /// Output column names for this node. Fixed for the lifetime of the query.
    fn column_names(&self) -> &[String];

    fn node_stats(&self) -> &NodeExecStats;
    fn node_stats_mut(&mut self) -> &mut NodeExecStats;
    fn plan_info(&self) -> PlanEstimate;
    fn node_label(&self) -> String;
    fn explain_details(&self, _indent: usize, _analyze: bool, _lines: &mut Vec<String>) {}

    /// Format children for EXPLAIN output. The node itself is formatted by
    /// the caller; this method handles child nodes.
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>);
}

/// Executor plan state — a trait object for dynamic dispatch.
pub type PlanState = Box<dyn PlanNode>;

#[derive(Debug)]
pub struct ResultState {
    pub(crate) emitted: bool,
    pub(crate) slot: TupleSlot,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct AppendState {
    pub(crate) children: Vec<PlanState>,
    pub(crate) current_child: usize,
    pub(crate) column_names: Vec<String>,
    pub(crate) slot: TupleSlot,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

pub struct SeqScanState {
    pub(crate) rel: RelFileLocator,
    pub(crate) relation_name: String,
    pub(crate) toast_relation: Option<ToastRelationRef>,
    pub(crate) column_names: Vec<String>,
    pub(crate) desc: Rc<RelationDesc>,
    pub(crate) attr_descs: Rc<[AttributeDesc]>,
    pub(crate) scan: Option<VisibleHeapScan>,
    /// Reusable slot, like PG's ss_ScanTupleSlot. Holds BufferHeapTuple
    /// with lazy decode into tts_values. The slot's `decoder` field holds
    /// the compiled tuple decoder (set once at plan start).
    pub(crate) slot: TupleSlot,
    /// Pushed-down qual, like PG's ExecSeqScanWithQual. When set, the scan
    /// evaluates the predicate inline and only returns qualifying tuples.
    /// Avoids a separate FilterState and its per-tuple vtable dispatch.
    pub(crate) qual: Option<crate::backend::executor::expr::CompiledPredicate>,
    pub(crate) qual_expr: Option<Expr>,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

impl std::fmt::Debug for SeqScanState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeqScanState")
            .field("rel", &self.rel)
            .field("relation_name", &self.relation_name)
            .field("has_qual", &self.qual.is_some())
            .finish()
    }
}

pub struct IndexScanState {
    pub(crate) rel: RelFileLocator,
    pub(crate) toast_relation: Option<ToastRelationRef>,
    pub(crate) index_rel: RelFileLocator,
    pub(crate) am_oid: u32,
    pub(crate) column_names: Vec<String>,
    pub(crate) desc: Rc<RelationDesc>,
    pub(crate) attr_descs: Rc<[AttributeDesc]>,
    pub(crate) index_meta: IndexRelCacheEntry,
    pub(crate) keys: Vec<ScanKeyData>,
    pub(crate) direction: ScanDirection,
    pub(crate) scan: Option<IndexScanDesc>,
    pub(crate) slot: TupleSlot,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

impl std::fmt::Debug for IndexScanState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexScanState")
            .field("rel", &self.rel)
            .field("index_rel", &self.index_rel)
            .field("am_oid", &self.am_oid)
            .finish()
    }
}

pub struct FilterState {
    pub(crate) input: PlanState,
    pub(crate) predicate: Expr,
    pub(crate) compiled_predicate: crate::backend::executor::expr::CompiledPredicate,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

impl std::fmt::Debug for FilterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterState")
            .field("predicate", &self.predicate)
            .finish()
    }
}

#[derive(Debug)]
pub struct NestedLoopJoinState {
    pub(crate) left: PlanState,
    pub(crate) right: PlanState,
    pub(crate) right_plan: Option<Plan>,
    pub(crate) kind: JoinType,
    pub(crate) cross_right_outer: bool,
    pub(crate) join_qual: Vec<Expr>,
    pub(crate) qual: Vec<Expr>,
    pub(crate) combined_names: Vec<String>,
    pub(crate) left_rows: Option<Vec<TupleSlot>>,
    pub(crate) right_rows: Option<Vec<TupleSlot>>,
    pub(crate) right_matched: Option<Vec<bool>>,
    pub(crate) current_left: Option<TupleSlot>,
    pub(crate) current_right: Option<TupleSlot>,
    pub(crate) current_left_matched: bool,
    pub(crate) left_index: usize,
    pub(crate) right_index: usize,
    pub(crate) left_width: usize,
    pub(crate) right_width: usize,
    pub(crate) unmatched_right_index: usize,
    pub(crate) slot: TupleSlot,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct HashState {
    pub(crate) input: PlanState,
    pub(crate) hash_keys: Vec<Expr>,
    pub(crate) column_names: Vec<String>,
    pub(crate) table: Option<HashJoinTable>,
    pub(crate) built: bool,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct HashJoinState {
    pub(crate) left: PlanState,
    pub(crate) right: Box<HashState>,
    pub(crate) kind: JoinType,
    pub(crate) hash_clauses: Vec<Expr>,
    pub(crate) hash_keys: Vec<Expr>,
    pub(crate) join_qual: Vec<Expr>,
    pub(crate) qual: Vec<Expr>,
    pub(crate) combined_names: Vec<String>,
    pub(crate) left_width: usize,
    pub(crate) right_width: usize,
    pub(crate) phase: HashJoinPhase,
    pub(crate) current_outer: Option<TupleSlot>,
    pub(crate) current_bucket_entries: Vec<usize>,
    pub(crate) current_bucket_index: usize,
    pub(crate) matched_outer: bool,
    pub(crate) unmatched_inner_index: usize,
    pub(crate) slot: TupleSlot,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct ProjectionState {
    pub(crate) input: PlanState,
    pub(crate) targets: Vec<TargetEntry>,
    pub(crate) column_names: Vec<String>,
    pub(crate) slot: TupleSlot,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct OrderByState {
    pub(crate) input: PlanState,
    pub(crate) items: Vec<OrderByEntry>,
    pub(crate) rows: Option<Vec<TupleSlot>>,
    pub(crate) next_index: usize,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct LimitState {
    pub(crate) input: PlanState,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: usize,
    pub(crate) skipped: usize,
    pub(crate) returned: usize,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct AggregateState {
    pub(crate) input: PlanState,
    pub(crate) group_by: Vec<Expr>,
    pub(crate) accumulators: Vec<AggAccum>,
    pub(crate) having: Option<Expr>,
    pub(crate) output_columns: Vec<String>,
    pub(crate) result_rows: Option<Vec<TupleSlot>>,
    pub(crate) next_index: usize,
    /// Reusable buffer for group-by key evaluation, allocated once at plan start.
    pub(crate) key_buffer: Vec<Value>,
    /// Compiled transition functions resolved at plan time, like PG's
    /// aggregate transfn pointers. Avoids per-tuple enum dispatch.
    pub(crate) trans_fns: Vec<fn(&mut AccumState, &[Value])>,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct ValuesState {
    pub(crate) rows: Vec<Vec<Expr>>,
    pub(crate) output_columns: Vec<String>,
    pub(crate) result_rows: Option<Vec<TupleSlot>>,
    pub(crate) next_index: usize,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct FunctionScanState {
    pub(crate) call: SetReturningCall,
    pub(crate) output_columns: Vec<String>,
    pub(crate) rows: Option<Vec<TupleSlot>>,
    pub(crate) next_index: usize,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct ProjectSetState {
    pub(crate) input: PlanState,
    pub(crate) targets: Vec<ProjectSetTarget>,
    pub(crate) output_columns: Vec<String>,
    pub(crate) current_input: Option<TupleSlot>,
    pub(crate) current_srf_rows: Vec<Vec<Value>>,
    pub(crate) current_row_count: usize,
    pub(crate) next_index: usize,
    pub(crate) slot: TupleSlot,
    pub(crate) plan_info: PlanEstimate,
    pub(crate) stats: NodeExecStats,
}

impl TupleSlot {
    pub fn from_heap_tuple(
        desc: Rc<RelationDesc>,
        attr_descs: Rc<[AttributeDesc]>,
        tid: ItemPointerData,
        tuple: HeapTuple,
    ) -> Self {
        let ncols = desc.columns.len();
        Self {
            kind: SlotKind::HeapTuple {
                desc,
                attr_descs,
                tid,
                tuple,
            },
            tts_values: Vec::with_capacity(ncols),
            tts_nvalid: 0,
            decode_offset: 0,
            decoder: None,
            toast: None,
        }
    }

    pub fn virtual_row(values: Vec<Value>) -> Self {
        let nvalid = values.len();
        Self {
            kind: SlotKind::Virtual,
            tts_values: values,
            tts_nvalid: nvalid,
            decode_offset: 0,
            decoder: None,
            toast: None,
        }
    }

    pub(crate) fn empty(ncols: usize) -> Self {
        Self {
            kind: SlotKind::Empty,
            tts_values: Vec::with_capacity(ncols),
            tts_nvalid: 0,
            decode_offset: 0,
            decoder: None,
            toast: None,
        }
    }

    /// Read a fixed-offset int32 directly from raw tuple bytes, like PG's
    /// heap_getattr fast path. Bypasses the full decode machinery. Returns
    /// None if the slot is not a BufferHeapTuple.
    #[inline]
    pub(crate) fn get_fixed_int32(&self, data_offset: usize) -> Option<i32> {
        if let SlotKind::BufferHeapTuple {
            tuple_ptr,
            tuple_len,
            ..
        } = &self.kind
        {
            let bytes = unsafe { std::slice::from_raw_parts(*tuple_ptr, *tuple_len) };
            let hoff = bytes[22] as usize;
            let start = hoff + data_offset;
            if start + 4 <= bytes.len() {
                return Some(i32::from_le_bytes([
                    bytes[start],
                    bytes[start + 1],
                    bytes[start + 2],
                    bytes[start + 3],
                ]));
            }
        }
        None
    }

    /// Number of columns in this slot.
    pub(crate) fn ncols(&self) -> usize {
        match &self.kind {
            SlotKind::HeapTuple { desc, .. } => desc.columns.len(),
            SlotKind::BufferHeapTuple { .. } => self
                .decoder
                .as_ref()
                .expect("BufferHeapTuple requires decoder")
                .ncols(),
            SlotKind::Virtual | SlotKind::Empty => self.tts_values.len(),
        }
    }

    /// Convert to a self-contained Virtual slot, decoding all columns and
    /// materializing TextRef → owned Text. Releases the buffer pin.
    pub fn materialize(mut self) -> Result<Self, ExecError> {
        self.values()?;
        Value::materialize_all(&mut self.tts_values);
        Ok(Self {
            kind: SlotKind::Virtual,
            tts_values: self.tts_values,
            tts_nvalid: self.tts_nvalid,
            decode_offset: 0,
            decoder: None,
            toast: self.toast,
        })
    }

    pub fn tid(&self) -> Option<ItemPointerData> {
        match &self.kind {
            SlotKind::HeapTuple { tid, .. } => Some(*tid),
            _ => None,
        }
    }
}
