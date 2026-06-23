//! Index-only-scan node vocabulary (`nodes/plannodes.h` `Scan`/`IndexOnlyScan`,
//! `executor/execnodes.h` `IndexOnlyScanState`/`IndexRuntimeKeyInfo`,
//! `access/genam.h` `IndexScanInstrumentation`/`SharedIndexScanInstrumentation`).
//!
//! The embedded `ScanState`/`PlanState` head reuses [`ScanStateData`]; the
//! leading `Plan` base reuses [`crate::nodeindexscan::Plan`]; executor-pool
//! aliases follow the owned model ([`SlotId`] for `TupleTableSlot *`,
//! [`EcxtId`] for `ExprContext *`). `ioss_RelationDesc` is the open index
//! relation handle; `ioss_ScanDesc` is the index-AM scan descriptor
//! ([`::types_tableam::relscan::IndexScanDesc`]).

use alloc::vec::Vec;

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use ::types_core::primitive::{AttrNumber, Oid};
use ::types_error::PgResult;
use ::rel::Relation;
use ::types_scan::scankey::ScanKeyData;
use ::types_scan::sdir::ScanDirection;
use ::types_sortsupport::SortSupportData;
use types_tuple::heaptuple::Datum;

use crate::execexpr::ExprState;
use crate::execnodes::{EcxtId, ScanStateData, SlotId};
use crate::nodeindexscan::Scan;
use crate::primnodes::{Expr, TargetEntry};

pub use crate::execstate_tags::T_IndexOnlyScanState;
pub use crate::nodes::T_IndexOnlyScan;
pub use types_storage::{Buffer, InvalidBuffer};

// The index-scan descriptor family (`IndexScanDescData`/`IndexScanDesc`,
// `ParallelIndexScanDescData`/`ParallelIndexScanDesc`, and the
// `IndexScanInstrumentation`/`SharedIndexScanInstrumentation` counters) is the
// canonical one defined in `types_tableam` (F1 of the index-AM tower). The
// trimmed copies that used to live here are deleted; these re-exports keep the
// `nodes::nodeindexonlyscan::…` paths the executor nodes use. `types-nodes`
// already depends on `types-tableam`, so there is no dependency cycle.
pub use ::types_tableam::genam::{IndexScanInstrumentation, SharedIndexScanInstrumentation};
pub use ::types_tableam::relscan::{
    IndexScanDesc, IndexScanDescData, ParallelIndexScanDescData, ParallelIndexScanDescHandle,
};

/// `ParallelIndexScanDesc` — C's `ParallelIndexScanDescData *`, the `Copy`
/// in-DSM pointer handle the executor threads through (the canonical
/// `types_tableam` [`ParallelIndexScanDescHandle`]). It carries NO Rust
/// lifetime — like the C bare pointer — but the alias keeps a `<'mcx>` slot so
/// the seam signatures that historically used `ParallelIndexScanDesc<'mcx>`
/// continue to parse; the lifetime is simply unused.
pub type ParallelIndexScanDesc<'mcx> = ParallelIndexScanDescHandle;

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
    pub key_expr: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `bool key_toastable` — is the expr's result a toastable datatype?
    pub key_toastable: bool,
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
    pub indexqual: Option<PgVec<'mcx, Expr<'mcx>>>,
    /// `List *recheckqual` — index quals in recheckable form.
    pub recheckqual: Option<PgVec<'mcx, Expr<'mcx>>>,
    /// `List *indexorderby` — list of index ORDER BY exprs.
    pub indexorderby: Option<PgVec<'mcx, Expr<'mcx>>>,
    /// `List *indextlist` — TargetEntry list describing index's cols.
    pub indextlist: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    /// `ScanDirection indexorderdir` — forward or backward or don't care.
    pub indexorderdir: ScanDirection,
}

impl IndexOnlyScan<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<IndexOnlyScan<'b>> {
        let clone_exprs = |src: &Option<PgVec<'_, Expr<'_>>>| -> PgResult<Option<PgVec<'b, Expr<'b>>>> {
            match src {
                Some(list) => {
                    let mut out = vec_with_capacity_in(mcx, list.len())?;
                    for e in list.iter() {
                        // Deep-copy via `clone_in`, not the derived `Expr::clone`
                        // (which panics on a `SubPlan` arm).
                        out.push(e.clone_in(mcx)?);
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
    pub recheckqual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `struct ScanKeyData *ioss_ScanKeys` — Skey structures for index quals.
    pub ioss_ScanKeys: PgVec<'mcx, ScanKeyData<'mcx>>,
    /// `int ioss_NumScanKeys`.
    pub ioss_NumScanKeys: i32,
    /// `struct ScanKeyData *ioss_OrderByKeys`.
    pub ioss_OrderByKeys: PgVec<'mcx, ScanKeyData<'mcx>>,
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

pub use crate::execstate_tags::T_IndexScanState;
pub use crate::nodes::T_IndexScan;

/// `ReorderTuple` (nodeIndexscan.c) — a buffered ORDER-BY-recheck tuple plus its
/// recomputed distances, held in the index scan's reorder queue. C palloc's
/// these in the per-query context and links them into a `pairingheap`; the
/// owned model carries the heap-tuple copy as a [`FormedTuple`] and the distance
/// arrays as owned `Vec`s.
#[derive(Debug)]
pub struct ReorderTuple<'mcx> {
    /// `HeapTuple htup` — the palloc'd copy of the scan tuple.
    pub tuple: types_tuple::heaptuple::FormedTuple<'mcx>,
    /// `Datum *orderbyvals`.
    pub orderbyvals: Vec<Datum<'mcx>>,
    /// `bool *orderbynulls`.
    pub orderbynulls: Vec<bool>,
}

/// `IndexScanState` (executor/execnodes.h) — runtime state of a plain index
/// scan.
///
/// The reorder queue (`iss_ReorderQueue`) backs the `IndexNextWithReorder`
/// ORDER-BY-recheck path. C uses a `pairingheap` keyed on `reorderqueue_cmp`
/// (which calls the per-key `SortSupport->comparator`); in the owned model the
/// comparison crosses a fallible seam and so cannot be a pure value-comparator
/// closure inside the leaf pairing heap. The reorder queue is therefore a
/// `PgVec` of `ReorderTuple`s with the same min-extraction discipline applied
/// at the add/pop sites via `cmp_orderbyvals` — behaviourally identical KNN
/// ordering, container only differs. `ReorderTuple` lives in the owning crate
/// (above the types layer); the queue is carried as an opaque erased element
/// type the owner downcasts. The `iss_*` recheck arrays below feed it.
#[derive(Debug)]
pub struct IndexScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `ExprState *indexqualorig` — execution state for the original-form index
    /// quals (used for lossy rechecks and EvalPlanQual).
    pub indexqualorig: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `List *indexorderbyorig` — execution states for the ORDER BY exprs in
    /// original form (used to recompute distances on a lossy index).
    pub indexorderbyorig: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
    /// `struct ScanKeyData *iss_ScanKeys` — Skey structures for index quals.
    pub iss_ScanKeys: PgVec<'mcx, ScanKeyData<'mcx>>,
    /// `int iss_NumScanKeys`.
    pub iss_NumScanKeys: i32,
    /// `struct ScanKeyData *iss_OrderByKeys`.
    pub iss_OrderByKeys: PgVec<'mcx, ScanKeyData<'mcx>>,
    /// `int iss_NumOrderByKeys`.
    pub iss_NumOrderByKeys: i32,
    /// `IndexRuntimeKeyInfo *iss_RuntimeKeys`.
    pub iss_RuntimeKeys: PgVec<'mcx, IndexRuntimeKeyInfo<'mcx>>,
    /// `int iss_NumRuntimeKeys`.
    pub iss_NumRuntimeKeys: i32,
    /// `bool iss_RuntimeKeysReady`.
    pub iss_RuntimeKeysReady: bool,
    /// `ExprContext *iss_RuntimeContext` — context for evaling runtime Skeys.
    pub iss_RuntimeContext: Option<EcxtId>,
    /// `Relation iss_RelationDesc` — index relation descriptor; `None` until
    /// `index_open` (no-op close in EXPLAIN-only).
    pub iss_RelationDesc: Option<Relation<'mcx>>,
    /// `struct IndexScanDescData *iss_ScanDesc` — index scan descriptor.
    pub iss_ScanDesc: Option<IndexScanDesc<'mcx>>,
    /// `IndexScanInstrumentation iss_Instrument` — local instrumentation.
    pub iss_Instrument: IndexScanInstrumentation,
    /// `SharedIndexScanInstrumentation *iss_SharedInfo` — parallel worker
    /// instrumentation (no leader entry).
    pub iss_SharedInfo: Option<PgBox<'mcx, SharedIndexScanInstrumentation>>,
    /// `bool iss_ReachedEnd` — the index scan has returned its last tuple (the
    /// reorder queue may still hold buffered tuples).
    pub iss_ReachedEnd: bool,
    /// `Datum *iss_OrderByValues` — re-computed ORDER BY distances for the
    /// current tuple (lossy-recheck path).
    pub iss_OrderByValues: PgVec<'mcx, Datum<'mcx>>,
    /// `bool *iss_OrderByNulls` — is-null flags for `iss_OrderByValues`.
    pub iss_OrderByNulls: PgVec<'mcx, bool>,
    /// `SortSupport iss_SortSupport` — per-ORDER-BY-key sort support used by
    /// `cmp_orderbyvals`.
    pub iss_SortSupport: PgVec<'mcx, SortSupportData<'mcx>>,
    /// `bool *iss_OrderByTypByVals` — per-ORDER-BY-key typbyval (for
    /// `datumCopy`).
    pub iss_OrderByTypByVals: PgVec<'mcx, bool>,
    /// `int16 *iss_OrderByTypLens` — per-ORDER-BY-key typlen (for `datumCopy`).
    pub iss_OrderByTypLens: PgVec<'mcx, i16>,
    /// `pairingheap *iss_ReorderQueue` — the reorder queue (a `PgVec` of
    /// `ReorderTuple`s; see the struct doc). `None` when the scan has no ORDER BY
    /// recheck (the C `iss_ReorderQueue == NULL`).
    pub iss_ReorderQueue: Option<PgVec<'mcx, ReorderTuple<'mcx>>>,
    /// `Size iss_PscanLen` — size of the parallel index-scan descriptor.
    pub iss_PscanLen: usize,
}

impl<'mcx> IndexScanState<'mcx> {
    /// `makeNode(IndexScanState)` — palloc0'd state with every field zeroed
    /// (the C `makeNode` zero-init), allocated in `mcx`.
    pub fn make_in(mcx: Mcx<'mcx>) -> Self {
        IndexScanState {
            ss: ScanStateData::default(),
            indexqualorig: None,
            indexorderbyorig: PgVec::new_in(mcx),
            iss_ScanKeys: PgVec::new_in(mcx),
            iss_NumScanKeys: 0,
            iss_OrderByKeys: PgVec::new_in(mcx),
            iss_NumOrderByKeys: 0,
            iss_RuntimeKeys: PgVec::new_in(mcx),
            iss_NumRuntimeKeys: 0,
            iss_RuntimeKeysReady: false,
            iss_RuntimeContext: None,
            iss_RelationDesc: None,
            iss_ScanDesc: None,
            iss_Instrument: IndexScanInstrumentation::default(),
            iss_SharedInfo: None,
            iss_ReachedEnd: false,
            iss_OrderByValues: PgVec::new_in(mcx),
            iss_OrderByNulls: PgVec::new_in(mcx),
            iss_SortSupport: PgVec::new_in(mcx),
            iss_OrderByTypByVals: PgVec::new_in(mcx),
            iss_OrderByTypLens: PgVec::new_in(mcx),
            iss_ReorderQueue: None,
            iss_PscanLen: 0,
        }
    }

    /// `makeNode(IndexScanState)` allocated as a `PgBox`.
    pub fn make_boxed_in(mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, Self>> {
        alloc_in(mcx, Self::make_in(mcx))
    }
}
