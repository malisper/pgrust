// Array keys loud-panic in the shared scankey builder; runtime keys are live.
#![allow(non_snake_case)]

use ::executils::EStateData;
use ::indexam::{
    index_beginscan_bitmap, index_close, index_endscan, index_getbitmap, index_rescan,
    IndexScanDescData,
};
use ::mcx::{Mcx, PgBox, PgVec};
use ::nodeindexscan::{exec_index_build_scan_keys, exec_index_eval_runtime_keys, RuntimeKeysState};
use ::tidbitmap::TIDBitmap;
use ::types_error::PgResult;
use ::types_nodes::plannodes::BitmapIndexScan;
use ::types_rel::{NoLock, Relation};
use ::types_scan::scankey::ScanKeyData;

pub fn init_seams() {}

pub struct BitmapIndexScanState<'mcx> {
    pub biss_ScanDesc: Option<PgBox<'mcx, IndexScanDescData<'mcx>>>,
    pub biss_RelationDesc: Option<Relation<'mcx>>,
    pub biss_ScanKeys: PgVec<'mcx, ScanKeyData>,
    pub biss_Runtime: Option<PgBox<'mcx, RuntimeKeysState<'mcx>>>,
}

pub fn exec_init_bitmap_index_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &BitmapIndexScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<BitmapIndexScanState<'mcx>> {
    let index_rel = indexam::index_open(mcx, node.indexid, NoLock)?;
    exec_init_bitmap_index_scan_rel(mcx, node, estate, eflags, index_rel)
}

pub fn exec_init_bitmap_index_scan_rel<'mcx>(
    mcx: Mcx<'mcx>,
    node: &BitmapIndexScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    _eflags: i32,
    index_rel: Relation<'mcx>,
) -> PgResult<BitmapIndexScanState<'mcx>> {
    if node.isshared {
        panic!("nodebitmapindexscan: isshared (parallel bitmap scan lane) not ported");
    }
    let (biss_ScanKeys, runtime_keys) =
        exec_index_build_scan_keys(mcx, &index_rel, &node.indexqual, estate.param_bind())?;
    let biss_Runtime = if runtime_keys.is_empty() {
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
    Ok(BitmapIndexScanState {
        biss_ScanDesc: None,
        biss_RelationDesc: Some(index_rel),
        biss_ScanKeys,
        biss_Runtime,
    })
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

    let scandesc = node
        .biss_ScanDesc
        .as_deref_mut()
        .expect("scan desc initialized above");
    let n_tuples = index_getbitmap(scandesc, tbm)? as f64;
    check_for_interrupts()?;
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

pub fn exec_end_bitmap_index_scan(node: &mut BitmapIndexScanState<'_>) -> PgResult<()> {
    if let Some(scandesc) = node.biss_ScanDesc.take() {
        index_endscan(PgBox::into_inner(scandesc))?;
    }
    if let Some(index_rel) = node.biss_RelationDesc.take() {
        index_close(index_rel, NoLock)?;
    }
    node.biss_ScanKeys.clear();
    node.biss_Runtime = None;
    Ok(())
}

/// `ExecReScanBitmapIndexScan`; array keys stay loud in the shared builder.
pub fn exec_rescan_bitmap_index_scan<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(rt) = node.biss_Runtime.as_deref_mut() {
        estate.reset_expr_context(rt.ecxt);
        exec_index_eval_runtime_keys(estate, rt.ecxt, &mut rt.keys, &mut node.biss_ScanKeys)?;
        rt.ready = true;
    }
    if let Some(scandesc) = node.biss_ScanDesc.as_deref_mut() {
        index_rescan(scandesc, Some(&node.biss_ScanKeys), None)?;
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

// Exempt (droppy owners released in exec_end_bitmap_index_scan); the
// destructure keeps the census exhaustive.
unsafe impl mcx::ForgetSafe for BitmapIndexScanState<'_> {}
const _: fn(&BitmapIndexScanState<'_>) = |v| {
    let BitmapIndexScanState {
        biss_ScanDesc: _,
        biss_RelationDesc: _,
        biss_ScanKeys: _,
        biss_Runtime: _,
    } = v;
};
