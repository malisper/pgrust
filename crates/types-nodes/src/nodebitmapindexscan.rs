//! Bitmap-index-scan node vocabulary (`nodes/plannodes.h` `BitmapIndexScan`,
//! `executor/execnodes.h` `BitmapIndexScanState`/`IndexArrayKeyInfo`).
//!
//! The embedded `ScanState`/`PlanState` head reuses [`ScanStateData`]; the
//! leading `Scan`/`Plan` base reuses [`crate::nodeindexonlyscan::Scan`] /
//! [`crate::nodeindexscan::Plan`]. The scan-key / runtime-key / instrumentation
//! vocabulary (`ScanKeyData`, `IndexRuntimeKeyInfo`, `IndexScanDescData`,
//! `IndexScanInstrumentation`, `SharedIndexScanInstrumentation`) is shared with
//! the index-only-scan node and reused from [`crate::nodeindexonlyscan`].

use mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use types_core::primitive::Oid;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::PgResult;
use types_rel::Relation;
use types_scan::scankey::ScanKeyData;

use crate::execexpr::ExprState;
use crate::execnodes::{EcxtId, ScanStateData};
use crate::nodeindexonlyscan::{
    IndexRuntimeKeyInfo, IndexScanDesc, IndexScanInstrumentation,
    SharedIndexScanInstrumentation,
};
use crate::nodeindexscan::Scan;
use crate::primnodes::Expr;

pub use crate::execstate_tags::T_BitmapIndexScanState;

/// `T_BitmapIndexScan` (nodes/nodetags.h, PG 18.3 generated order).
pub const T_BitmapIndexScan: crate::nodes::NodeTag = crate::nodes::NodeTag(343);

/// `IndexArrayKeyInfo` (execnodes.h) — info about a ScalarArrayOpExpr scankey
/// whose value is an array evaluated at runtime; the scan is iterated once per
/// array element. `scan_key` is the index of the scankey to fill in the owning
/// node's scankey array (the C `struct ScanKeyData *`); `array_expr` is the
/// compiled expression yielding the array value.
#[derive(Debug)]
pub struct IndexArrayKeyInfo<'mcx> {
    /// `struct ScanKeyData *scan_key` — index of the scankey to put the value
    /// into, in the owning node's `biss_ScanKeys` array.
    pub scan_key: usize,
    /// `ExprState *array_expr` — expr to evaluate to get the array value.
    pub array_expr: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `int next_elem` — next array element to use.
    pub next_elem: i32,
    /// `int num_elems` — number of elems in current array value.
    pub num_elems: i32,
    /// `Datum *elem_values` — array of `num_elems` Datums.
    pub elem_values: PgVec<'mcx, Datum<'mcx>>,
    /// `bool *elem_nulls` — array of `num_elems` is-null flags.
    pub elem_nulls: PgVec<'mcx, bool>,
}

/// `BitmapIndexScan` plan node (plannodes.h):
///
/// ```c
/// typedef struct BitmapIndexScan
/// {
///     Scan    scan;
///     Oid     indexid;
///     bool    isshared;
///     List   *indexqual;
///     List   *indexqualorig;
/// } BitmapIndexScan;
/// ```
#[derive(Debug)]
pub struct BitmapIndexScan<'mcx> {
    /// `Scan scan` — its first field (`plan`) starts with the `NodeTag`.
    pub scan: Scan<'mcx>,
    /// `Oid indexid` — OID of index to scan.
    pub indexid: Oid,
    /// `bool isshared` — create shared bitmap if set.
    pub isshared: bool,
    /// `List *indexqual` — list of index quals (OpExprs).
    pub indexqual: Option<PgVec<'mcx, Expr<'mcx>>>,
    /// `List *indexqualorig` — the same in original form.
    pub indexqualorig: Option<PgVec<'mcx, Expr<'mcx>>>,
}

impl BitmapIndexScan<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<BitmapIndexScan<'b>> {
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
        Ok(BitmapIndexScan {
            scan: self.scan.clone_in(mcx)?,
            indexid: self.indexid,
            isshared: self.isshared,
            indexqual: clone_exprs(&self.indexqual)?,
            indexqualorig: clone_exprs(&self.indexqualorig)?,
        })
    }
}

/// `BitmapIndexScanState` (execnodes.h):
///
/// ```c
/// typedef struct BitmapIndexScanState
/// {
///     ScanState                 ss;
///     TIDBitmap                *biss_result;
///     struct ScanKeyData       *biss_ScanKeys;
///     int                       biss_NumScanKeys;
///     IndexRuntimeKeyInfo      *biss_RuntimeKeys;
///     int                       biss_NumRuntimeKeys;
///     IndexArrayKeyInfo        *biss_ArrayKeys;
///     int                       biss_NumArrayKeys;
///     bool                      biss_RuntimeKeysReady;
///     ExprContext              *biss_RuntimeContext;
///     Relation                  biss_RelationDesc;
///     struct IndexScanDescData *biss_ScanDesc;
///     IndexScanInstrumentation  biss_Instrument;
///     SharedIndexScanInstrumentation *biss_SharedInfo;
/// } BitmapIndexScanState;
/// ```
///
/// `biss_result` (the C `TIDBitmap *` pre-made-bitmap handoff a parent
/// `BitmapOr` stores) is an opaque owned bitmap; in the owned tree the running
/// `MultiExecProcNode` bitmap is threaded through the `execProcnode`
/// `multi_exec_proc_node` return rather than smuggled into this field, so the
/// field is present for struct fidelity but stays `None` here.
#[derive(Debug)]
pub struct BitmapIndexScanState<'mcx> {
    /// `ScanState ss` — its first field is `NodeTag`.
    pub ss: ScanStateData<'mcx>,
    /// `TIDBitmap *biss_result` — pre-made bitmap from a parent BitmapOr, or
    /// `None`. The owned bitmap type carries the dynahash registry handle, so
    /// the handle rides as a `PgBox` over the `TIDBitmap` carrier supplied by
    /// the bitmap owner.
    pub biss_result: Option<PgBox<'mcx, types_tidbitmap::TIDBitmap>>,
    /// `struct ScanKeyData *biss_ScanKeys` — Skey structures for index quals.
    pub biss_ScanKeys: PgVec<'mcx, ScanKeyData<'mcx>>,
    /// `int biss_NumScanKeys`.
    pub biss_NumScanKeys: i32,
    /// `IndexRuntimeKeyInfo *biss_RuntimeKeys`.
    pub biss_RuntimeKeys: PgVec<'mcx, IndexRuntimeKeyInfo<'mcx>>,
    /// `int biss_NumRuntimeKeys`.
    pub biss_NumRuntimeKeys: i32,
    /// `IndexArrayKeyInfo *biss_ArrayKeys`.
    pub biss_ArrayKeys: PgVec<'mcx, IndexArrayKeyInfo<'mcx>>,
    /// `int biss_NumArrayKeys`.
    pub biss_NumArrayKeys: i32,
    /// `bool biss_RuntimeKeysReady`.
    pub biss_RuntimeKeysReady: bool,
    /// `ExprContext *biss_RuntimeContext` — context for evaling runtime Skeys
    /// (id into the EState exprcontext pool).
    pub biss_RuntimeContext: Option<EcxtId>,
    /// `Relation biss_RelationDesc` — index relation descriptor; `None` until
    /// `index_open` (and in EXPLAIN-only).
    pub biss_RelationDesc: Option<Relation<'mcx>>,
    /// `struct IndexScanDescData *biss_ScanDesc` — index scan descriptor.
    pub biss_ScanDesc: Option<IndexScanDesc<'mcx>>,
    /// `IndexScanInstrumentation biss_Instrument` — local instrumentation.
    pub biss_Instrument: IndexScanInstrumentation,
    /// `SharedIndexScanInstrumentation *biss_SharedInfo` — parallel worker
    /// instrumentation (no leader entry).
    pub biss_SharedInfo: Option<PgBox<'mcx, SharedIndexScanInstrumentation>>,
}

impl<'mcx> BitmapIndexScanState<'mcx> {
    /// `makeNode(BitmapIndexScanState)` — palloc0'd state with every field
    /// zeroed (the C `makeNode` zero-init), allocated in `mcx`.
    pub fn make_in(mcx: Mcx<'mcx>) -> Self {
        BitmapIndexScanState {
            ss: ScanStateData::default(),
            biss_result: None,
            biss_ScanKeys: PgVec::new_in(mcx),
            biss_NumScanKeys: 0,
            biss_RuntimeKeys: PgVec::new_in(mcx),
            biss_NumRuntimeKeys: 0,
            biss_ArrayKeys: PgVec::new_in(mcx),
            biss_NumArrayKeys: 0,
            biss_RuntimeKeysReady: false,
            biss_RuntimeContext: None,
            biss_RelationDesc: None,
            biss_ScanDesc: None,
            biss_Instrument: IndexScanInstrumentation::default(),
            biss_SharedInfo: None,
        }
    }

    /// `makeNode(BitmapIndexScanState)` allocated as a `PgBox` (C: `makeNode`
    /// returns the pointer).
    pub fn make_boxed_in(mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, Self>> {
        alloc_in(mcx, Self::make_in(mcx))
    }
}
