// Runtime/array keys loud-panic in the shared scankey builder.
#![allow(non_snake_case)]

use ::executils::EStateData;
use ::indexam::{
    index_beginscan_bitmap, index_close, index_endscan, index_getbitmap, index_rescan,
    IndexScanDescData,
};
use ::mcx::{Mcx, PgBox, PgVec};
use ::nodeindexscan::exec_index_build_scan_keys;
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
    _estate: &mut EStateData<'mcx>,
    _eflags: i32,
    index_rel: Relation<'mcx>,
) -> PgResult<BitmapIndexScanState<'mcx>> {
    if node.isshared {
        panic!("nodebitmapindexscan: isshared (parallel bitmap scan lane) not ported");
    }
    let biss_ScanKeys = exec_index_build_scan_keys(mcx, &index_rel, &node.indexqual)?;
    Ok(BitmapIndexScanState {
        biss_ScanDesc: None,
        biss_RelationDesc: Some(index_rel),
        biss_ScanKeys,
    })
}

/// C's biss_result hand-off from BitmapOr; returns ntuples added.
pub fn multi_exec_bitmap_index_scan_into<'mcx>(
    node: &mut BitmapIndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    tbm: &mut TIDBitmap<'_>,
) -> PgResult<f64> {
    let mcx = estate.es_query_cxt;
    if node.biss_ScanDesc.is_none() {
        let snapshot = estate
            .es_snapshot
            .clone()
            .expect("bitmap index scan requires es_snapshot");
        let mut scandesc = index_beginscan_bitmap(
            mcx,
            node.biss_RelationDesc.as_ref().expect("index relation open"),
            snapshot,
            node.biss_ScanKeys.len() as i32,
        )?;
        index_rescan(&mut scandesc, Some(&node.biss_ScanKeys), None)?;
        node.biss_ScanDesc = Some(::mcx::alloc_in(mcx, scandesc)?);
    }

    let scandesc = node.biss_ScanDesc.as_deref_mut().expect("scan desc initialized above");
    let n_tuples = index_getbitmap(scandesc, tbm)? as f64;
    check_for_interrupts();
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
    Ok(())
}

/// Runtime/array key arms unreachable (init loud-panics on non-Const quals).
pub fn exec_rescan_bitmap_index_scan(node: &mut BitmapIndexScanState<'_>) -> PgResult<()> {
    if let Some(scandesc) = node.biss_ScanDesc.as_deref_mut() {
        index_rescan(scandesc, Some(&node.biss_ScanKeys), None)?;
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn interrupt_unported() -> ! {
    panic!("nodebitmapindexscan: ProcessInterrupts (tcop/postgres.c) unported")
}

#[inline(always)]
fn check_for_interrupts() {
    if init_small::globals::InterruptPending() {
        interrupt_unported();
    }
}

// Exempt (every field): droppy owners, all released in
// exec_end_bitmap_index_scan; the destructure keeps the census exhaustive.
unsafe impl mcx::ForgetSafe for BitmapIndexScanState<'_> {}
const _: fn(&BitmapIndexScanState<'_>) = |v| {
    let BitmapIndexScanState { biss_ScanDesc: _, biss_RelationDesc: _, biss_ScanKeys: _ } = v;
};
