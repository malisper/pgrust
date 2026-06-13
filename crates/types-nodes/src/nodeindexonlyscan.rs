//! Index-only-scan node vocabulary (`nodes/plannodes.h` `Scan`/`IndexOnlyScan`,
//! `executor/execnodes.h` `IndexOnlyScanState`/`IndexRuntimeKeyInfo`,
//! `access/genam.h` `IndexScanInstrumentation`/`SharedIndexScanInstrumentation`).
//!
//! The embedded `ScanState`/`PlanState` head reuses [`ScanStateData`]; the
//! leading `Plan` base reuses [`crate::nodeindexscan::Plan`]; executor-pool
//! aliases follow the owned model ([`SlotId`] for `TupleTableSlot *`,
//! [`EcxtId`] for `ExprContext *`). `ioss_RelationDesc` is the open index
//! relation handle; `ioss_ScanDesc` is the index-AM scan descriptor
//! ([`types_tableam::relscan::IndexScanDesc`]).

use alloc::vec::Vec;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::primitive::{AttrNumber, Oid};
use types_error::PgResult;
use types_rel::Relation;
use types_scan::scankey::ScanKeyData;
use types_scan::sdir::ScanDirection;
use types_tuple::heaptuple::{HeapTuple, IndexTuple, ItemPointerData, TupleDescData};

use crate::execexpr::ExprState;
use crate::execnodes::{EcxtId, ScanStateData, SlotId};
use crate::nodeindexscan::Plan;
use crate::primnodes::{Expr, TargetEntry};

pub use crate::execstate_tags::T_IndexOnlyScanState;
pub use crate::nodes::T_IndexOnlyScan;
pub use types_storage::{Buffer, InvalidBuffer};

/// `IndexScanInstrumentation` (access/genam.h) — per-scan instrumentation kept
/// by the index AMs (the search count incremented by `pgstat_count_index_scan`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IndexScanInstrumentation {
    /// `uint64 nsearches` — index search count.
    pub nsearches: u64,
}

/// `SharedIndexScanInstrumentation` (access/genam.h) — the DSM-resident copy of
/// the per-worker instrumentation. C's `winstrument[FLEXIBLE_ARRAY_MEMBER]`
/// becomes an owned `Vec` of `num_workers` entries.
#[derive(Clone, Debug, Default)]
pub struct SharedIndexScanInstrumentation {
    /// `int num_workers`.
    pub num_workers: i32,
    /// `IndexScanInstrumentation winstrument[FLEXIBLE_ARRAY_MEMBER]`.
    pub winstrument: Vec<IndexScanInstrumentation>,
}

/// `ParallelIndexScanDescData` (access/relscan.h) — the generic parallel
/// index-scan descriptor that lives in DSM. Trimmed to the offsets the
/// index-only-scan node and `index_beginscan_parallel` consume; the
/// `ps_snapshot_data[FLEXIBLE_ARRAY_MEMBER]` serialized snapshot tail is owned
/// by the parallel-scan setup and not consumed here.
#[derive(Clone, Debug, Default)]
pub struct ParallelIndexScanDescData {
    /// `Size ps_offset_ins` — offset to `SharedIndexScanInstrumentation`.
    pub ps_offset_ins: usize,
    /// `Size ps_offset_am` — offset to the am-specific structure.
    pub ps_offset_am: usize,
}

/// `ParallelIndexScanDesc` — `ParallelIndexScanDescData *`.
pub type ParallelIndexScanDesc<'mcx> = PgBox<'mcx, ParallelIndexScanDescData>;

/// `IndexScanDescData` (access/relscan.h) — the index-AM scan descriptor,
/// palloc'd by `index_beginscan` and filled by the AM's `amgettuple`. Trimmed
/// to the fields the index-only-scan node reads. The AM-private scan state
/// (`opaque`) and the per-fetch heap state (`xs_heapfetch`) are owned by the
/// index AM / table AM and ride opaquely.
#[derive(Debug)]
pub struct IndexScanDescData<'mcx> {
    /// `Relation heapRelation` — heap relation descriptor, or `None`.
    pub heapRelation: Option<Relation<'mcx>>,
    /// `Relation indexRelation` — index relation descriptor.
    pub indexRelation: Relation<'mcx>,
    /// `int numberOfKeys` — number of index qualifier conditions.
    pub numberOfKeys: i32,
    /// `int numberOfOrderBys` — number of ordering operators.
    pub numberOfOrderBys: i32,
    /// `struct ScanKeyData *keyData` — array of index qualifier descriptors.
    pub keyData: PgVec<'mcx, ScanKeyData>,
    /// `struct ScanKeyData *orderByData` — array of ordering-op descriptors.
    pub orderByData: PgVec<'mcx, ScanKeyData>,
    /// `bool xs_want_itup` — caller requests index tuples.
    pub xs_want_itup: bool,
    /// `bool kill_prior_tuple` — last-returned tuple is dead.
    pub kill_prior_tuple: bool,
    /// `struct IndexScanInstrumentation *instrument` — counters maintained by
    /// the AM; `None` is the C `NULL`.
    pub instrument: Option<IndexScanInstrumentation>,
    /// `IndexTuple xs_itup` — index tuple returned by the AM.
    pub xs_itup: IndexTuple<'mcx>,
    /// `struct TupleDescData *xs_itupdesc` — rowtype descriptor of `xs_itup`.
    pub xs_itupdesc: Option<PgBox<'mcx, TupleDescData<'mcx>>>,
    /// `HeapTuple xs_hitup` — index data returned by the AM, as a HeapTuple.
    pub xs_hitup: HeapTuple<'mcx>,
    /// `struct TupleDescData *xs_hitupdesc` — rowtype descriptor of `xs_hitup`.
    pub xs_hitupdesc: Option<PgBox<'mcx, TupleDescData<'mcx>>>,
    /// `ItemPointerData xs_heaptid` — result TID.
    pub xs_heaptid: ItemPointerData,
    /// `bool xs_heap_continue` — must keep walking, potential further results.
    pub xs_heap_continue: bool,
    /// `bool xs_recheck` — scan keys must be rechecked.
    pub xs_recheck: bool,
    /// `bool xs_recheckorderby` — ORDER BY values need recheck.
    pub xs_recheckorderby: bool,
    /// `struct ParallelIndexScanDescData *parallel_scan` — parallel index scan
    /// information, in shared memory; `None` is the C `NULL`.
    pub parallel_scan: Option<ParallelIndexScanDesc<'mcx>>,
}

/// `IndexScanDesc` — `IndexScanDescData *`.
pub type IndexScanDesc<'mcx> = PgBox<'mcx, IndexScanDescData<'mcx>>;

/// `IndexRuntimeKeyInfo` (execnodes.h) — info about a scankey whose value must
/// be evaluated at runtime. The `scan_key` slot the value is written into is
/// addressed by its index in the owning node's scankey array; `key_expr` is
/// the compiled expression to evaluate.
#[derive(Debug)]
pub struct IndexRuntimeKeyInfo<'mcx> {
    /// `struct ScanKeyData *scan_key` — index of the scankey to fill, in the
    /// owning node's `ioss_ScanKeys`/`ioss_OrderByKeys` array.
    pub scan_key: usize,
    /// `ExprState *key_expr` — expr to evaluate to get the value.
    pub key_expr: Option<PgBox<'mcx, ExprState>>,
    /// `bool key_toastable` — is the expr's result a toastable datatype?
    pub key_toastable: bool,
}

/// `Scan` plan base (plannodes.h):
///
/// ```c
/// typedef struct Scan { Plan plan; Index scanrelid; } Scan;
/// ```
#[derive(Debug, Default)]
pub struct Scan<'mcx> {
    /// `Plan plan` — its first field starts with the `NodeTag`.
    pub plan: Plan<'mcx>,
    /// `Index scanrelid` — relid is index into the range table.
    pub scanrelid: u32,
}

impl Scan<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Scan<'b>> {
        Ok(Scan {
            plan: self.plan.clone_in(mcx)?,
            scanrelid: self.scanrelid,
        })
    }
}

/// `IndexOnlyScan` plan node (plannodes.h):
///
/// ```c
/// typedef struct IndexOnlyScan
/// {
///     Scan          scan;
///     Oid           indexid;
///     List         *indexqual;
///     List         *recheckqual;
///     List         *indexorderby;
///     List         *indextlist;
///     ScanDirection indexorderdir;
/// } IndexOnlyScan;
/// ```
#[derive(Debug)]
pub struct IndexOnlyScan<'mcx> {
    /// `Scan scan` — its first field (`plan`) starts with the `NodeTag`.
    pub scan: Scan<'mcx>,
    /// `Oid indexid` — OID of index to scan.
    pub indexid: Oid,
    /// `List *indexqual` — list of index quals (usually OpExprs).
    pub indexqual: Option<PgVec<'mcx, Expr>>,
    /// `List *recheckqual` — index quals in recheckable form.
    pub recheckqual: Option<PgVec<'mcx, Expr>>,
    /// `List *indexorderby` — list of index ORDER BY exprs.
    pub indexorderby: Option<PgVec<'mcx, Expr>>,
    /// `List *indextlist` — TargetEntry list describing index's cols.
    pub indextlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    /// `ScanDirection indexorderdir` — forward or backward or don't care.
    pub indexorderdir: ScanDirection,
}

impl IndexOnlyScan<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<IndexOnlyScan<'b>> {
        let clone_exprs = |src: &Option<PgVec<'_, Expr>>| -> PgResult<Option<PgVec<'b, Expr>>> {
            match src {
                Some(list) => {
                    let mut out = vec_with_capacity_in(mcx, list.len())?;
                    for e in list.iter() {
                        out.push(e.clone());
                    }
                    Ok(Some(out))
                }
                None => Ok(None),
            }
        };
        let indextlist = match &self.indextlist {
            Some(list) => {
                let mut out = vec_with_capacity_in(mcx, list.len())?;
                for tle in list.iter() {
                    out.push(tle.clone_in(mcx)?);
                }
                Some(out)
            }
            None => None,
        };
        Ok(IndexOnlyScan {
            scan: self.scan.clone_in(mcx)?,
            indexid: self.indexid,
            indexqual: clone_exprs(&self.indexqual)?,
            recheckqual: clone_exprs(&self.recheckqual)?,
            indexorderby: clone_exprs(&self.indexorderby)?,
            indextlist,
            indexorderdir: self.indexorderdir,
        })
    }
}

/// `IndexOnlyScanState` (execnodes.h), trimmed to consumed fields.
#[derive(Debug)]
pub struct IndexOnlyScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `ExprState *recheckqual` — execution state for recheckqual expressions.
    pub recheckqual: Option<PgBox<'mcx, ExprState>>,
    /// `struct ScanKeyData *ioss_ScanKeys` — Skey structures for index quals.
    pub ioss_ScanKeys: PgVec<'mcx, ScanKeyData>,
    /// `int ioss_NumScanKeys`.
    pub ioss_NumScanKeys: i32,
    /// `struct ScanKeyData *ioss_OrderByKeys`.
    pub ioss_OrderByKeys: PgVec<'mcx, ScanKeyData>,
    /// `int ioss_NumOrderByKeys`.
    pub ioss_NumOrderByKeys: i32,
    /// `IndexRuntimeKeyInfo *ioss_RuntimeKeys`.
    pub ioss_RuntimeKeys: PgVec<'mcx, IndexRuntimeKeyInfo<'mcx>>,
    /// `int ioss_NumRuntimeKeys`.
    pub ioss_NumRuntimeKeys: i32,
    /// `bool ioss_RuntimeKeysReady`.
    pub ioss_RuntimeKeysReady: bool,
    /// `ExprContext *ioss_RuntimeContext` — context for evaling runtime Skeys.
    pub ioss_RuntimeContext: Option<EcxtId>,
    /// `Relation ioss_RelationDesc` — index relation descriptor; `None` until
    /// `index_open` (no-op close in EXPLAIN-only).
    pub ioss_RelationDesc: Option<Relation<'mcx>>,
    /// `struct IndexScanDescData *ioss_ScanDesc` — index scan descriptor.
    pub ioss_ScanDesc: Option<IndexScanDesc<'mcx>>,
    /// `IndexScanInstrumentation ioss_Instrument` — local instrumentation.
    pub ioss_Instrument: IndexScanInstrumentation,
    /// `SharedIndexScanInstrumentation *ioss_SharedInfo` — parallel worker
    /// instrumentation (no leader entry).
    pub ioss_SharedInfo: Option<PgBox<'mcx, SharedIndexScanInstrumentation>>,
    /// `TupleTableSlot *ioss_TableSlot` — slot for tuples fetched from the
    /// table (id into the EState slot pool).
    pub ioss_TableSlot: Option<SlotId>,
    /// `Buffer ioss_VMBuffer` — buffer in use for visibility-map testing.
    pub ioss_VMBuffer: Buffer,
    /// `Size ioss_PscanLen` — size of the parallel index-only scan descriptor.
    pub ioss_PscanLen: usize,
    /// `AttrNumber *ioss_NameCStringAttNums` — attnums of name-typed columns to
    /// pad to NAMEDATALEN.
    pub ioss_NameCStringAttNums: PgVec<'mcx, AttrNumber>,
    /// `int ioss_NameCStringCount`.
    pub ioss_NameCStringCount: i32,
}

impl<'mcx> IndexOnlyScanState<'mcx> {
    /// `makeNode(IndexOnlyScanState)` — palloc0'd state with every field zeroed
    /// (the C `makeNode` zero-init), allocated in `mcx`.
    pub fn make_in(mcx: Mcx<'mcx>) -> Self {
        IndexOnlyScanState {
            ss: ScanStateData::default(),
            recheckqual: None,
            ioss_ScanKeys: PgVec::new_in(mcx),
            ioss_NumScanKeys: 0,
            ioss_OrderByKeys: PgVec::new_in(mcx),
            ioss_NumOrderByKeys: 0,
            ioss_RuntimeKeys: PgVec::new_in(mcx),
            ioss_NumRuntimeKeys: 0,
            ioss_RuntimeKeysReady: false,
            ioss_RuntimeContext: None,
            ioss_RelationDesc: None,
            ioss_ScanDesc: None,
            ioss_Instrument: IndexScanInstrumentation::default(),
            ioss_SharedInfo: None,
            ioss_TableSlot: None,
            ioss_VMBuffer: InvalidBuffer,
            ioss_PscanLen: 0,
            ioss_NameCStringAttNums: PgVec::new_in(mcx),
            ioss_NameCStringCount: 0,
        }
    }

    /// `makeNode(IndexOnlyScanState)` allocated as a `PgBox` (C: `makeNode`
    /// returns the pointer).
    pub fn make_boxed_in(mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, Self>> {
        alloc_in(mcx, Self::make_in(mcx))
    }
}
