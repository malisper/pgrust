#![allow(non_snake_case)]

use ::executils::EStateData;
use ::indexam::{
    index_beginscan_bitmap, index_close, index_endscan, index_getbitmap, index_rescan,
    IndexScanDescData,
};
use ::mcx::{Mcx, PgBox, PgVec};
use ::nodeindexscan::{
    exec_index_advance_array_keys, exec_index_build_scan_keys, exec_index_eval_array_keys,
    exec_index_eval_runtime_keys, IndexArrayKeyInfo, RuntimeKeysState,
};
use ::tidbitmap::TIDBitmap;
use ::types_error::PgResult;
use ::types_nodes::plannodes::BitmapIndexScan;
use ::types_rel::{NoLock, Relation};
use ::types_scan::scankey::ScanKeyData;
use ::types_slot::EXEC_FLAG_EXPLAIN_ONLY;

pub fn init_seams() {}

pub struct BitmapIndexScanState<'mcx> {
    pub biss_ScanDesc: Option<PgBox<'mcx, IndexScanDescData<'mcx>>>,
    pub biss_RelationDesc: Option<Relation<'mcx>>,
    pub biss_ScanKeys: PgVec<'mcx, ScanKeyData>,
    // C treats array keys as runtime keys: biss_Runtime is Some whenever
    // either kind exists, and its `ready` covers both (false = an empty or
    // null array, so no scan at all).
    pub biss_Runtime: Option<PgBox<'mcx, RuntimeKeysState<'mcx>>>,
    pub biss_ArrayKeys: PgVec<'mcx, IndexArrayKeyInfo<'mcx>>,
    /// C `ss.ps.plan->plan_node_id`: the key under which this node's
    /// `biss_Instrument.nsearches` is republished to the estate (the
    /// thread-native stand-in for `biss_SharedInfo`'s DSM slot).
    pub biss_PlanNodeId: i32,
}

pub fn exec_init_bitmap_index_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &BitmapIndexScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<BitmapIndexScanState<'mcx>> {
    // C nodeBitmapIndexscan.c:272: plain EXPLAIN stops here — the index is
    // neither opened nor locked and no scan keys are built ("This allows an
    // index-advisor plugin to EXPLAIN a plan containing references to
    // nonexistent indexes"; an EXPLAIN of a cached generic plan takes no
    // index lock, AcquireExecutorLocks covers tables only). The plan never
    // runs; exec_end_bitmap_index_scan closes nothing (C: "no-op if we
    // didn't open it").
    if eflags & EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return Ok(BitmapIndexScanState {
            biss_ScanDesc: None,
            biss_RelationDesc: None,
            biss_ScanKeys: PgVec::new_in(mcx),
            biss_Runtime: None,
            biss_ArrayKeys: PgVec::new_in(mcx),
            biss_PlanNodeId: node.scan.plan.plan_node_id,
        });
    }
    // C nodeBitmapIndexscan.c:276: rellockmode unconditionally — a reused
    // generic plan gets no planner locks and AcquireExecutorLocks covers
    // tables only.
    let index_rel = indexam::index_open(
        mcx,
        node.indexid,
        ::nodeindexscan::index_lockmode(estate, node.scan.scanrelid),
    )?;
    exec_init_bitmap_index_scan_rel(mcx, node, estate, eflags, index_rel)
}

pub fn exec_init_bitmap_index_scan_rel<'mcx>(
    mcx: Mcx<'mcx>,
    node: &BitmapIndexScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    _eflags: i32,
    index_rel: Relation<'mcx>,
) -> PgResult<BitmapIndexScanState<'mcx>> {
    // C divergence: isshared only picks the dsa allocator for biss_result;
    // thread-native builds a plain arena bitmap and freezes it at
    // tbm_prepare_shared_iterate, so no arm is needed here.
    let mut runtime_keys = ::mcx::PgVec::new_in(mcx);
    let mut array_keys = ::mcx::PgVec::new_in(mcx);
    let params = estate.param_bind();
    let biss_ScanKeys = ::executils::with_subplan_compile_env(estate, |env| {
        exec_index_build_scan_keys(
            mcx,
            &index_rel,
            &node.indexqual,
            params,
            false,
            &mut runtime_keys,
            Some(&mut array_keys),
            env,
        )
    })?;
    let biss_Runtime = if runtime_keys.is_empty() && array_keys.is_empty() {
        None
    } else {
        Some(::mcx::alloc_in(
            mcx,
            RuntimeKeysState {
                keys: runtime_keys,
                ready: false,
                ecxt: estate.exec_assign_expr_context(),
            },
        )?)
    };
    // nodeBitmapIndexscan.c:323: the scan begins at init (this is where an AM
    // without ampredlocks takes its relation predicate lock), and the keys go
    // straight to the AM when none of them is a runtime or array key.
    let snapshot = estate.es_snapshot.clone().expect("bitmap index scan requires es_snapshot");
    let mut scandesc =
        index_beginscan_bitmap(mcx, &index_rel, snapshot, biss_ScanKeys.len() as i32)?;
    if biss_Runtime.is_none() {
        index_rescan(&mut scandesc, Some(&biss_ScanKeys), None)?;
    }
    Ok(BitmapIndexScanState {
        biss_ScanDesc: Some(::mcx::alloc_in(mcx, scandesc)?),
        biss_RelationDesc: Some(index_rel),
        biss_ScanKeys,
        biss_Runtime,
        biss_ArrayKeys: array_keys,
        biss_PlanNodeId: node.scan.plan.plan_node_id,
    })
}

/// C `biss_Instrument.nsearches` (execnodes.h:1805; the AM bumps it through
/// `scan->instrument`): republish the scan's running search count to the
/// estate under this node's plan_node_id. In a parallel worker this is the
/// per-worker `winstrument` slot the leader sums in show_indexsearches_info
/// (explain.c:3879-3887); ANALYZE-only, like C's `instrument` pointer.
#[inline]
fn republish_nsearches(
    estate: &mut EStateData<'_>,
    plan_node_id: i32,
    scandesc: &IndexScanDescData<'_>,
) {
    if estate.es_instrument != 0 {
        estate.instr_set_index_nsearches(plan_node_id, scandesc.xs_nsearches);
    }
}

/// C's biss_result hand-off from BitmapOr; returns ntuples added.
pub fn multi_exec_bitmap_index_scan_into<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tbm: &mut TIDBitmap<'_>,
) -> PgResult<f64> {
    let mcx = estate.es_query_cxt;
    if node.biss_Runtime.as_deref().is_some_and(|r| !r.ready) {
        exec_rescan_bitmap_index_scan(node, estate)?;
    }
    if node.biss_ScanDesc.is_none() {
        let snapshot = estate
            .es_snapshot
            .clone()
            .expect("bitmap index scan requires es_snapshot");
        let mut scandesc = index_beginscan_bitmap(
            mcx,
            node.biss_RelationDesc
                .as_ref()
                .expect("index relation open"),
            snapshot,
            node.biss_ScanKeys.len() as i32,
        )?;
        if node.biss_Runtime.as_deref().is_none_or(|r| r.ready) {
            index_rescan(&mut scandesc, Some(&node.biss_ScanKeys), None)?;
        }
        node.biss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
    }

    // C MultiExecBitmapIndexScan: doscan=false means the rescan found a null
    // or empty array key, so no scan is possible; otherwise loop the scan
    // over every array-key element combination.
    let mut doscan = node.biss_Runtime.as_deref().is_none_or(|r| r.ready);
    let mut n_tuples = 0.0;
    while doscan {
        let scandesc = node
            .biss_ScanDesc
            .as_deref_mut()
            .expect("scan desc initialized above");
        n_tuples += index_getbitmap(scandesc, tbm)? as f64;
        republish_nsearches(estate, node.biss_PlanNodeId, scandesc);
        check_for_interrupts()?;
        doscan = exec_index_advance_array_keys(&mut node.biss_ArrayKeys, &mut node.biss_ScanKeys);
        if doscan {
            let scandesc = node
                .biss_ScanDesc
                .as_deref_mut()
                .expect("scan desc initialized above");
            index_rescan(scandesc, Some(&node.biss_ScanKeys), None)?;
        }
    }
    Ok(n_tuples)
}

pub fn multi_exec_bitmap_index_scan<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<TIDBitmap<'mcx>> {
    let mut tbm = TIDBitmap::new(
        estate.es_query_cxt,
        init_small::globals::work_mem() as usize * 1024,
    );
    multi_exec_bitmap_index_scan_into(node, estate, &mut tbm)?;
    Ok(tbm)
}

// ---------------------------------------------------------------------------
// bitmap-morsels mode C (parallel bitmap BUILD): clamped-range claims.
//
// The build phase partitions a const btree range qual [lo, hi] on the leading
// index column into subrange granules; each worker claim re-runs ITS OWN
// BitmapIndexScan with the >=/<= keys' arguments overwritten to the claim's
// bounds (the planner-resolved comparison procs are reused verbatim — no
// opfamily lookups, no new keys) and adds the results into a claim-local
// bitmap. Union of the claims over a cover of [lo, hi] equals the unclamped
// scan: bitmaps are sets, so even boundary sloppiness would only cost
// duplicates, which union dedups — but the granule cover is exact anyway.
// Everything else fails closed at admission (`clampable_range` = None).
// ---------------------------------------------------------------------------

/// Datum width of a clampable int-family key argument.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ClampWidth {
    I16,
    I32,
    I64,
}

/// One clampable key: its position in `biss_ScanKeys` + its argument width.
#[derive(Clone, Copy, Debug)]
pub struct ClampKey {
    pub idx: usize,
    pub width: ClampWidth,
}

/// The mode C clampable shape: the `>=` key and the `<=` key of a const
/// btree range on the leading index attribute.
#[derive(Clone, Copy, Debug)]
pub struct BitmapBuildClamp {
    pub ge: ClampKey,
    pub le: ClampKey,
}

fn clamp_width_for(effective_type: ::types_core::Oid) -> Option<ClampWidth> {
    match effective_type {
        ::types_core::catalog::INT2OID => Some(ClampWidth::I16),
        ::types_core::catalog::INT4OID => Some(ClampWidth::I32),
        ::types_core::catalog::INT8OID => Some(ClampWidth::I64),
        _ => None,
    }
}

fn read_arg(key: &ScanKeyData, width: ClampWidth) -> i64 {
    match width {
        ClampWidth::I16 => key.sk_argument.as_i16() as i64,
        ClampWidth::I32 => key.sk_argument.as_i32() as i64,
        ClampWidth::I64 => key.sk_argument.as_i64(),
    }
}

fn write_arg(key: &mut ScanKeyData, width: ClampWidth, v: i64) {
    // The clamp bounds always lie inside the original [lo, hi] (the granule
    // cover is a partition of it), so the narrowing casts cannot truncate.
    key.sk_argument = match width {
        ClampWidth::I16 => ::datum::Datum::from_i16(v as i16),
        ClampWidth::I32 => ::datum::Datum::from_i32(v as i32),
        ClampWidth::I64 => ::datum::Datum::from_i64(v),
    };
}

/// Fail-closed shape probe: `Some((clamp, lo, hi))` iff the built scankeys
/// are EXACTLY a two-key const range on the leading index attribute —
/// strategies {`>=`, `<=`}, zero `SK_*` flags, int2/int4/int8 arguments
/// (`sk_subtype` = the operator's right-hand type, set unconditionally by
/// `exec_index_build_scan_keys` on ordinary keys). Anything else — strict
/// bounds, extra keys, arrays, row keys, NULL tests, non-int types —
/// returns None and mode C falls back to the serial build.
pub fn clampable_range(keys: &[ScanKeyData]) -> Option<(BitmapBuildClamp, i64, i64)> {
    use ::types_scan::scankey::{BTGreaterEqualStrategyNumber, BTLessEqualStrategyNumber};
    if keys.len() != 2 {
        return None;
    }
    let mut ge: Option<ClampKey> = None;
    let mut le: Option<ClampKey> = None;
    for (idx, key) in keys.iter().enumerate() {
        if key.sk_flags != 0 || key.sk_attno != 1 {
            return None;
        }
        let width = clamp_width_for(key.sk_subtype)?;
        match key.sk_strategy {
            s if s == BTGreaterEqualStrategyNumber && ge.is_none() => {
                ge = Some(ClampKey { idx, width });
            }
            s if s == BTLessEqualStrategyNumber && le.is_none() => {
                le = Some(ClampKey { idx, width });
            }
            _ => return None,
        }
    }
    let (ge, le) = (ge?, le?);
    let lo = read_arg(&keys[ge.idx], ge.width);
    let hi = read_arg(&keys[le.idx], le.width);
    if lo > hi {
        return None; // empty range: nothing to parallelize
    }
    Some((BitmapBuildClamp { ge, le }, lo, hi))
}

/// mode C build claim: run this node's index scan with the range keys
/// temporarily clamped to `[lo, hi]` and add the results into `tbm`.
/// The caller proved the shape with `clampable_range` on the identical plan
/// (leader-side admission; a diverged worker shape is the caller's ERROR).
/// The scan keys stay clamped after return — every claim rewrites both
/// arguments, and the build phase is this node's only driver.
pub fn multi_exec_bitmap_index_scan_clamped_into<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    clamp: &BitmapBuildClamp,
    lo: i64,
    hi: i64,
    tbm: &mut TIDBitmap<'_>,
) -> PgResult<f64> {
    debug_assert!(node.biss_Runtime.is_none(), "mode C admission excludes runtime keys");
    write_arg(&mut node.biss_ScanKeys[clamp.ge.idx], clamp.ge.width, lo);
    write_arg(&mut node.biss_ScanKeys[clamp.le.idx], clamp.le.width, hi);
    let mcx = estate.es_query_cxt;
    if node.biss_ScanDesc.is_none() {
        let snapshot = estate
            .es_snapshot
            .clone()
            .expect("bitmap index scan requires es_snapshot");
        let scandesc = index_beginscan_bitmap(
            mcx,
            node.biss_RelationDesc
                .as_ref()
                .expect("index relation open"),
            snapshot,
            node.biss_ScanKeys.len() as i32,
        )?;
        node.biss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
    }
    let scandesc = node
        .biss_ScanDesc
        .as_deref_mut()
        .expect("scan desc initialized above");
    index_rescan(scandesc, Some(&node.biss_ScanKeys), None)?;
    let n_tuples = index_getbitmap(scandesc, tbm)? as f64;
    republish_nsearches(estate, node.biss_PlanNodeId, scandesc);
    check_for_interrupts()?;
    Ok(n_tuples)
}

pub fn exec_end_bitmap_index_scan(node: &mut BitmapIndexScanState<'_>) -> PgResult<()> {
    if let Some(scandesc) = node.biss_ScanDesc.take() {
        index_endscan(PgBox::into_inner(scandesc))?;
    }
    if let Some(index_rel) = node.biss_RelationDesc.take() {
        index_close(index_rel, NoLock)?;
    }
    node.biss_ScanKeys.clear();
    node.biss_Runtime = None;
    node.biss_ArrayKeys.clear();
    Ok(())
}

/// `ExecReScanBitmapIndexScan`: recompute runtime keys, re-expand array keys
/// (ready = false when an array is null/empty), then rescan if possible.
pub fn exec_rescan_bitmap_index_scan<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    if let Some(rt) = node.biss_Runtime.as_deref_mut() {
        estate.reset_expr_context(rt.ecxt);
        exec_index_eval_runtime_keys(estate, rt.ecxt, &mut rt.keys, &mut node.biss_ScanKeys, &mut [])?;
        rt.ready = if node.biss_ArrayKeys.is_empty() {
            true
        } else {
            exec_index_eval_array_keys(
                mcx,
                estate,
                rt.ecxt,
                &mut node.biss_ArrayKeys,
                &mut node.biss_ScanKeys,
            )?
        };
    }
    if node.biss_Runtime.as_deref().is_none_or(|r| r.ready) {
        if let Some(scandesc) = node.biss_ScanDesc.as_deref_mut() {
            index_rescan(scandesc, Some(&node.biss_ScanKeys), None)?;
        }
    }
    Ok(())
}

#[inline(always)]
fn check_for_interrupts() -> types_error::PgResult<()> {
    if init_small::globals::InterruptPending() {
        postgres_seams::check_for_interrupts::call()?;
    }
    Ok(())
}

#[cfg(test)]
mod clamp_tests {
    use super::*;
    use ::types_scan::scankey::{
        BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber, BTLessEqualStrategyNumber,
        SK_SEARCHARRAY,
    };

    fn key(attno: i16, strategy: u16, subtype: ::types_core::Oid, arg: i64) -> ScanKeyData {
        let mut k = ScanKeyData::empty();
        k.sk_attno = attno;
        k.sk_strategy = strategy;
        k.sk_subtype = subtype;
        k.sk_argument = match subtype {
            ::types_core::catalog::INT2OID => ::datum::Datum::from_i16(arg as i16),
            ::types_core::catalog::INT4OID => ::datum::Datum::from_i32(arg as i32),
            _ => ::datum::Datum::from_i64(arg),
        };
        k
    }

    #[test]
    fn accepts_the_between_shape_and_reads_bounds() {
        for t in [
            ::types_core::catalog::INT2OID,
            ::types_core::catalog::INT4OID,
            ::types_core::catalog::INT8OID,
        ] {
            let keys = [
                key(1, BTGreaterEqualStrategyNumber, t, 100),
                key(1, BTLessEqualStrategyNumber, t, 2000),
            ];
            let (clamp, lo, hi) = clampable_range(&keys).expect("clampable");
            assert_eq!((lo, hi), (100, 2000));
            assert_eq!(clamp.ge.idx, 0);
            assert_eq!(clamp.le.idx, 1);
            // key order flipped
            let keys = [
                key(1, BTLessEqualStrategyNumber, t, 2000),
                key(1, BTGreaterEqualStrategyNumber, t, 100),
            ];
            let (clamp, lo, hi) = clampable_range(&keys).expect("clampable flipped");
            assert_eq!((lo, hi), (100, 2000));
            assert_eq!(clamp.ge.idx, 1);
            assert_eq!(clamp.le.idx, 0);
        }
    }

    #[test]
    fn refuses_every_off_shape() {
        let t = ::types_core::catalog::INT4OID;
        let ge = key(1, BTGreaterEqualStrategyNumber, t, 100);
        let le = key(1, BTLessEqualStrategyNumber, t, 2000);
        // one key only
        assert!(clampable_range(&[ge.clone()]).is_none());
        // three keys
        assert!(clampable_range(&[ge.clone(), le.clone(), ge.clone()]).is_none());
        // strict bound
        let strict = key(1, BTGreaterStrategyNumber, t, 100);
        assert!(clampable_range(&[strict, le.clone()]).is_none());
        // two >= keys
        assert!(clampable_range(&[ge.clone(), ge.clone()]).is_none());
        // non-leading attribute
        let att2 = key(2, BTGreaterEqualStrategyNumber, t, 100);
        assert!(clampable_range(&[att2, le.clone()]).is_none());
        // SK_ flags (array key)
        let mut arr = ge.clone();
        arr.sk_flags = SK_SEARCHARRAY;
        assert!(clampable_range(&[arr, le.clone()]).is_none());
        // non-int subtype (numeric = 1700)
        let numeric = key(1, BTGreaterEqualStrategyNumber, 1700, 100);
        assert!(clampable_range(&[numeric, le.clone()]).is_none());
        // subtype 0 (never set = not an ordinary key)
        let zero = key(1, BTGreaterEqualStrategyNumber, 0, 100);
        assert!(clampable_range(&[zero, le.clone()]).is_none());
        // empty range
        let ge_high = key(1, BTGreaterEqualStrategyNumber, t, 5000);
        assert!(clampable_range(&[ge_high, le.clone()]).is_none());
    }

    #[test]
    fn clamp_write_read_roundtrip_by_width() {
        let t = ::types_core::catalog::INT8OID;
        let mut k = key(1, BTGreaterEqualStrategyNumber, t, 0);
        write_arg(&mut k, ClampWidth::I64, -123456789012345);
        assert_eq!(read_arg(&k, ClampWidth::I64), -123456789012345);
        let mut k = key(1, BTGreaterEqualStrategyNumber, ::types_core::catalog::INT2OID, 0);
        write_arg(&mut k, ClampWidth::I16, -32000);
        assert_eq!(read_arg(&k, ClampWidth::I16), -32000);
        let mut k = key(1, BTGreaterEqualStrategyNumber, ::types_core::catalog::INT4OID, 0);
        write_arg(&mut k, ClampWidth::I32, 2_000_000_000);
        assert_eq!(read_arg(&k, ClampWidth::I32), 2_000_000_000);
    }
}

// Exempt (droppy owners released in exec_end_bitmap_index_scan); the
// destructure keeps the census exhaustive.
unsafe impl mcx::ForgetSafe for BitmapIndexScanState<'_> {}
const _: fn(&BitmapIndexScanState<'_>) = |v| {
    let BitmapIndexScanState {
        biss_ScanDesc: _,
        biss_RelationDesc: _,
        biss_ScanKeys: _,
        biss_Runtime: _,
        biss_ArrayKeys: _,
        biss_PlanNodeId: _,
    } = v;
};
