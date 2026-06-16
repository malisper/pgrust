//! `spghandler` (spgutils.c:43) — the SP-GiST index access method handler:
//! return the one unified [`IndexAmRoutine`] with its property flags and
//! callback function pointers.
//!
//! Convention A (leading-`mcx` HRTB on the allocating scan/insert/vacuum
//! callbacks); the thin adapters below translate between the unified
//! `IndexScanDescData` / vacuum descriptor and SP-GiST's own callback shapes.
//! `amvalidate` / `amadjustmembers` / `amoptions` / `amproperty` /
//! `amcostestimate` are reached by name (they need an `Mcx`, are fallible, or
//! live below this layer) and so are not raw fn-ptr vtable slots — matching the
//! bt/hash/gin/brin/gist handlers in this repo.

use mcx::Mcx;

use types_amapi::{
    AmCostEstimate, IndexAMProperty as AmIndexAMProperty, IndexAmRoutine, IndexBuildResult,
    IndexPath, OpFamilyMember, PlannerInfo, T_IndexAmRoutine,
};
use types_core::primitive::{InvalidOid, Oid};
use types_rel::Relation;
use types_scan::scankey::ScanKeyData;
use types_scan::sdir::ScanDirection;
use types_spgist::{SPGISTNProc, SPGIST_OPTIONS_PROC};
use types_tableam::amapi::{IndexUniqueCheck, TIDBitmap};
use types_tableam::index_info_carrier::IndexInfoCarrier;
use types_tableam::genam::{IndexBulkDeleteResult, IndexVacuumInfo};
use types_tableam::relscan::{IndexScanDesc, IndexScanDescData};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;
use types_error::PgResult;

use crate::spgscan::so;
use crate::{
    spgbeginscan, spgbuildempty, spgbulkdelete, spgcanreturn, spgendscan, spggetbitmap,
    spggettuple, spginsert, spgproperty, spgrescan, spgvacuumcleanup, IndexAMProperty,
};

/// `VACUUM_OPTION_PARALLEL_BULKDEL` (`commands/vacuum.h`): `1 << 0`.
const VACUUM_OPTION_PARALLEL_BULKDEL: u8 = 1 << 0;
/// `VACUUM_OPTION_PARALLEL_COND_CLEANUP` (`commands/vacuum.h`): `1 << 1`.
const VACUUM_OPTION_PARALLEL_COND_CLEANUP: u8 = 1 << 1;

/// `spghandler(PG_FUNCTION_ARGS)` (spgutils.c:43).
pub fn spghandler() -> IndexAmRoutine {
    IndexAmRoutine {
        type_: T_IndexAmRoutine,
        amstrategies: 0,
        amsupport: SPGISTNProc as u16,
        amoptsprocnum: SPGIST_OPTIONS_PROC as u16,
        amcanorder: false,
        amcanorderbyop: true,
        amcanhash: false,
        amconsistentequality: false,
        amconsistentordering: false,
        amcanbackward: false,
        amcanunique: false,
        amcanmulticol: false,
        amoptionalkey: true,
        amsearcharray: false,
        amsearchnulls: true,
        amstorage: true,
        amclusterable: false,
        ampredlocks: false,
        amcanparallel: false,
        amcanbuildparallel: false,
        amcaninclude: true,
        amusemaintenanceworkmem: false,
        amsummarizing: false,
        amparallelvacuumoptions: VACUUM_OPTION_PARALLEL_BULKDEL
            | VACUUM_OPTION_PARALLEL_COND_CLEANUP,
        amkeytype: InvalidOid,

        // spgvalidate / spgtranslate* need an `Mcx` and/or return soft errors,
        // so they cannot ride the raw fn-ptr slots; reached by name through
        // amapi (backend-access-spg-validate).
        amvalidate: None,
        amtranslatestrategy: None,
        amtranslatecmptype: None,

        // Build / options / plan-time callbacks (#340). `spgbuildempty` and
        // `spgproperty` are this crate's own fns (wired directly; `spgproperty`
        // builds a transient cache context like `spgcanreturn_am`). `spgbuild`
        // needs the real `IndexInfo` (erased carrier can't supply it),
        // `spgoptions` needs the reloptions `Datum` detoast the #341 dispatch
        // does, `spgcostestimate` (selfuncs.c) and `spgadjustmembers`
        // (spg-validate) are not reachable from here — sanctioned panic legs
        // reached via #341. SP-GiST has no gettreeheight/buildphasename (NULL).
        ambuild: spgbuild_am,
        ambuildempty: spgbuildempty_am,
        amcostestimate: spgcostestimate_am,
        amgettreeheight: None,
        amoptions: spgoptions_am,
        amproperty: Some(spgproperty_am),
        ambuildphasename: None,
        amadjustmembers: Some(spgadjustmembers_am),

        // Insert / vacuum callbacks (F1/F4).
        aminsert: spginsert_am,
        aminsertcleanup: None,
        ambulkdelete: spgbulkdelete_am,
        amvacuumcleanup: spgvacuumcleanup_am,

        // Scan callbacks (F2).
        ambeginscan: spgbeginscan_am,
        amrescan: spgrescan_am,
        amendscan: spgendscan_am,
        amcanreturn: Some(spgcanreturn_am),
        amgettuple: Some(spggettuple_am),
        amgetbitmap: Some(spggetbitmap_am),
        ammarkpos: None,
        amrestrpos: None,

        // No parallel index scan (amcanparallel = false).
        amestimateparallelscan: None,
        aminitparallelscan: None,
        amparallelrescan: None,
    }
}

// ===========================================================================
// Build / options / plan-time vtable adapters (#340). See the doc comment in
// `spghandler` for why the build / cross-crate slots are sanctioned panic legs
// (reached via the #341 index.c dispatch).
// ===========================================================================

/// `ambuild` adapter — `spgbuild` needs the real `IndexInfo`, which the erased
/// `IndexInfo` carrier cannot supply; reached via the #341 dispatch.
fn spgbuild_am<'mcx>(
    _mcx: Mcx<'mcx>,
    _heap_relation: &Relation<'mcx>,
    _index_relation: &Relation<'mcx>,
    _index_info: &mut IndexInfoCarrier<'_, 'mcx>,
) -> PgResult<IndexBuildResult> {
    panic!(
        "spgbuild: index.c build dispatch (#341) not yet ported — \
         needs the real types_nodes::execnodes::IndexInfo"
    )
}

/// `ambuildempty` adapter — wires this crate's `spgbuildempty`.
fn spgbuildempty_am<'mcx>(mcx: Mcx<'mcx>, index_relation: &Relation<'mcx>) -> PgResult<()> {
    spgbuildempty(mcx, index_relation)
}

/// `amcostestimate` adapter — `spgcostestimate` (selfuncs.c) not reachable;
/// reached via the #341 dispatch.
fn spgcostestimate_am<'mcx>(
    _mcx: Mcx<'mcx>,
    _root: &mut PlannerInfo,
    _path: &mut IndexPath,
    _loop_count: f64,
) -> PgResult<AmCostEstimate> {
    panic!("spgcostestimate: index cost estimation (selfuncs.c) not yet reachable from spgist (#341)")
}

/// `amoptions` adapter — `spgoptions` takes the parsed reloptions byte image,
/// which needs the reloptions `Datum` detoast the #341 dispatch performs.
fn spgoptions_am<'mcx>(
    _mcx: Mcx<'mcx>,
    _reloptions: Datum<'mcx>,
    _validate: bool,
) -> PgResult<Option<alloc::vec::Vec<u8>>> {
    panic!("spgoptions: needs the reloptions Datum detoast done by the index.c dispatch (#341)")
}

/// `amproperty` adapter — wires this crate's `spgproperty`, mapping the
/// canonical `IndexAMProperty` to SP-GiST's local enum. `spgproperty` needs an
/// `Mcx` for the opclass-cache lookup; build a transient context (mirrors
/// `spgcanreturn_am`).
fn spgproperty_am(
    index_oid: Oid,
    attno: i32,
    prop: AmIndexAMProperty,
    _propname: &str,
    res: &mut bool,
    isnull: &mut bool,
) -> PgResult<bool> {
    let sprop = match prop {
        AmIndexAMProperty::AMPROP_DISTANCE_ORDERABLE => IndexAMProperty::DistanceOrderable,
        _ => IndexAMProperty::Other,
    };
    let cx = mcx::MemoryContext::new("SP-GiST amproperty temporary context");
    let (handled, r, n) = spgproperty(cx.mcx(), index_oid, attno, sprop)?;
    *res = r;
    *isnull = n;
    Ok(handled)
}

/// `amadjustmembers` adapter — `spgadjustmembers` (spg-validate) not reachable
/// from this crate; reached via the #341 dispatch.
fn spgadjustmembers_am<'mcx>(
    _mcx: Mcx<'mcx>,
    _opfamilyoid: Oid,
    _opclassoid: Oid,
    _operators: &mut alloc::vec::Vec<OpFamilyMember>,
    _functions: &mut alloc::vec::Vec<OpFamilyMember>,
) -> PgResult<()> {
    panic!("spgadjustmembers: opclass member adjust (spg-validate) not yet reachable from spgist-core (#341)")
}

// ===========================================================================
// AM-vtable adapters.
// ===========================================================================

#[allow(clippy::too_many_arguments)]
fn spginsert_am<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    heap_tid: &ItemPointerData,
    heap_relation: &Relation<'mcx>,
    check_unique: IndexUniqueCheck,
    index_unchanged: bool,
    index_info: &mut IndexInfoCarrier<'_, 'mcx>,
) -> PgResult<bool> {
    spginsert(
        mcx,
        index_relation,
        values,
        isnull,
        heap_tid,
        heap_relation,
        check_unique,
        index_unchanged,
        index_info,
    )
}

/// `ambulkdelete` adapter — wrap the VACUUM driver's `callback_state` handle
/// into SP-GiST's `IndexBulkDeleteCallback` closure (the membership test rides
/// the `vacuum_tid_is_dead` seam). `None` callback_state is the cleanup-only
/// NULL callback; in that case there is nothing to delete and `spgbulkdelete`
/// is still run with an always-`false` callback (matching VACUUM's contract of
/// a non-NULL callback whenever bulkdelete is invoked).
fn spgbulkdelete_am<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    callback_state: Option<u64>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let mut cb = move |tid: &ItemPointerData| -> PgResult<bool> {
        match callback_state {
            Some(h) => Ok(backend_commands_vacuum_seams::vacuum_tid_is_dead::call(*tid, h)),
            None => Ok(false),
        }
    };
    let res = spgbulkdelete(mcx, info, stats, &mut cb)?;
    Ok(Some(res))
}

fn spgvacuumcleanup_am<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    spgvacuumcleanup(mcx, info, stats)
}

fn spgbeginscan_am<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    nkeys: i32,
    norderbys: i32,
) -> PgResult<IndexScanDesc<'mcx>> {
    // spgbeginscan builds the full descriptor (opaque erased) itself.
    spgbeginscan(mcx, index_relation, nkeys, norderbys)
}

fn spgrescan_am<'mcx>(
    _mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    keys: &[ScanKeyData<'mcx>],
    orderbys: &[ScanKeyData<'mcx>],
) -> PgResult<()> {
    spgrescan(scan, keys, orderbys)
}

fn spgendscan_am<'mcx>(_mcx: Mcx<'mcx>, scan: &mut IndexScanDescData<'mcx>) -> PgResult<()> {
    spgendscan(so(scan))
}

fn spgcanreturn_am(index: &Relation<'_>, attno: i32) -> PgResult<bool> {
    // INCLUDE attributes are always fetchable without consulting the cache, so
    // short-circuit before allocating a context (matches spgcanreturn's own
    // `attno > 1` fast path).
    if attno > 1 {
        return Ok(true);
    }
    // The key-column answer reads the opclass config via `spgGetCache`, which
    // wants an `Mcx`. The `amcanreturn` vtable slot is context-free; build a
    // transient context for the cache lookup (the cached value is interned in
    // the relation's `rd_amcache`, not in this scratch arena).
    let cx = mcx::MemoryContext::new("SP-GiST amcanreturn temporary context");
    spgcanreturn(cx.mcx(), index, attno)
}

fn spggettuple_am<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    direction: ScanDirection,
) -> PgResult<bool> {
    spggettuple(mcx, scan, direction)
}

fn spggetbitmap_am<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    tbm: &mut TIDBitmap,
) -> PgResult<i64> {
    spggetbitmap(mcx, scan, tbm)
}
