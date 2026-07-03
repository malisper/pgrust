//! nodeSubplan.c initplan slice: ExecInitSubPlan + ExecSetParamPlan for
//! uncorrelated EXISTS/EXPR subplans; every other sublink shape is loud.

use ::executils::{EStateData, SubplanStateCell};
use ::mcx::{Mcx, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_CARDINALITY_VIOLATION};
use ::types_nodes::primnodes::{SubLinkType, SubPlan};
use ::types_scan::sdir::ScanDirection;
use ::datum as Datum_crate;
use Datum_crate::Datum;

use crate::procnode::{exec_proc_node, PlanStateNode};

pub(crate) struct SubPlanState<'mcx> {
    sub_link_type: SubLinkType,
    set_param: PgVec<'mcx, i32>,
    /// The subplan's PlanState (es_subplanstates cell); taken out for the
    /// duration of a run so same-plan re-entry is a loud panic, not aliasing.
    ps_cell: core::ptr::NonNull<Option<PlanStateNode<'mcx>>>,
}

/// `ExecInitSubPlan` (nodeSubplan.c), initPlan arm: parks the SubPlanState on
/// every setParam so the first param read runs the subplan.
pub(crate) fn exec_init_sub_plan<'mcx>(
    subplan: &SubPlan<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    if !subplan.parParam.is_nil() || subplan.useHashTable || subplan.testexpr.is_some() {
        panic!(
            "ExecInitSubPlan (nodeSubplan.c): correlated/hashed/testexpr SubPlan \
             \"{}\" — only uncorrelated initplans are ported",
            subplan.plan_name.unwrap_or("?")
        );
    }
    if !matches!(
        subplan.subLinkType,
        SubLinkType::EXISTS_SUBLINK | SubLinkType::EXPR_SUBLINK
    ) {
        panic!(
            "ExecInitSubPlan (nodeSubplan.c): {:?} initplan not ported",
            subplan.subLinkType
        );
    }
    let cell = estate
        .es_subplanstates
        .get((subplan.plan_id - 1) as usize)
        .unwrap_or_else(|| panic!("subplan \"{}\" was not initialized", subplan.plan_name.unwrap_or("?")))
        .0;

    let mut set_param: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    for id in subplan.setParam.iter() {
        set_param.push(id);
    }
    debug_assert_eq!(set_param.len(), 1);

    let mut boxed = ::mcx::alloc_in(
        mcx,
        SubPlanState {
            sub_link_type: subplan.subLinkType,
            set_param,
            ps_cell: cell.cast(),
        },
    )?;
    let raw: *mut SubPlanState<'mcx> = &mut *boxed;
    // Forget-on-reset: the arena reclaims the bytes at es_query_cxt reset; the
    // skipped drop is only a PgVec header whose buffer is the same arena.
    core::mem::forget(boxed);
    // SAFETY: raw comes from a live arena allocation.
    let erased = SubplanStateCell(unsafe { core::ptr::NonNull::new_unchecked(raw) }.cast());
    // SAFETY: same allocation, shared read after the forget.
    let sstate: &SubPlanState<'mcx> = unsafe { &*raw };

    for id in sstate.set_param.iter() {
        let pid = *id as usize;
        estate.es_param_exec_vals[pid].exec_plan = true;
        estate.es_param_subplans[pid] = Some(erased);
    }
    Ok(())
}

/// The [`executils::SubplanHook`] impl; installed once per query in InitPlan.
///
/// # Safety
/// `p` is an es_query_cxt-lifetime SubPlanState installed by
/// [`exec_init_sub_plan`] on the same estate.
pub(crate) unsafe fn subplan_hook(
    p: core::ptr::NonNull<()>,
    estate: &mut EStateData<'_>,
) -> PgResult<()> {
    // SAFETY: caller contract; the 'mcx erased here is the estate's own.
    let sstate = unsafe { &*p.cast::<SubPlanState<'_>>().as_ptr() };
    exec_set_param_plan(sstate, estate)
}

// ExecSetParamPlan (nodeSubplan.c), EXISTS/EXPR arms. Divergence: C copies the
// whole result tuple (curTuple, freed per re-run); with one setParam column a
// datumCopy of that column into es_query_cxt is the same boundary, and re-runs
// (chgParam rescans) don't exist on this lane so nothing accumulates.
fn exec_set_param_plan<'mcx>(
    sstate: &SubPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let paramid = sstate.set_param[0] as usize;
    // SAFETY: es_query_cxt-lifetime cell; exclusive by the take-out protocol
    // (a nested take of the same cell panics below).
    let cell = unsafe { &mut *sstate.ps_cell.as_ptr() };
    let mut ps = cell
        .take()
        .unwrap_or_else(|| panic!("recursive initplan execution (nodeSubplan.c)"));

    let saved_dir = estate.es_direction;
    estate.es_direction = ScanDirection::ForwardScanDirection;

    let result = run_subplan(sstate, &mut ps, estate, paramid);

    estate.es_direction = saved_dir;
    *cell = Some(ps);
    result
}

fn run_subplan<'mcx>(
    sstate: &SubPlanState<'mcx>,
    ps: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    paramid: usize,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let mut found = false;
    let mut value = Datum::null();
    let mut isnull = true;

    while let Some(slot_id) = exec_proc_node(ps, estate)? {
        match sstate.sub_link_type {
            SubLinkType::EXISTS_SUBLINK => {
                found = true;
                break;
            }
            SubLinkType::EXPR_SUBLINK => {
                if found {
                    return Err(too_many_rows());
                }
                found = true;
                let slot = estate.slot_mut(slot_id);
                let (attlen, attbyval) = {
                    let desc = slot
                        .base()
                        .tts_tupleDescriptor
                        .as_ref()
                        .expect("subplan result slot has a descriptor");
                    (desc.attrs[0].attlen, desc.attrs[0].attbyval)
                };
                let mut vnull = false;
                let v = exectuples::slot_getattr(slot, 1, &mut vnull);
                isnull = vnull;
                value = if vnull || attbyval { v } else { datum_copy_in(mcx, v, attlen)? };
            }
            other => unreachable!("{other:?} initplan is loud at init"),
        }
    }

    let prm = &mut estate.es_param_exec_vals[paramid];
    prm.exec_plan = false;
    match sstate.sub_link_type {
        SubLinkType::EXISTS_SUBLINK => {
            prm.value = Datum::from_bool(found);
            prm.isnull = false;
        }
        SubLinkType::EXPR_SUBLINK => {
            if found {
                prm.value = value;
                prm.isnull = isnull;
            } else {
                prm.value = Datum::null();
                prm.isnull = true;
            }
        }
        other => unreachable!("{other:?} initplan is loud at init"),
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn too_many_rows() -> Box<PgError> {
    Box::new(
        PgError::error("more than one row returned by a subquery used as an expression".to_string())
            .with_sqlstate(ERRCODE_CARDINALITY_VIOLATION),
    )
}

// datumCopy (datum.c) into es_query_cxt (fold.rs precedent); heap-sourced
// varlenas can carry short/toast headers — loud until the detoast lane.
fn datum_copy_in<'mcx>(mcx: Mcx<'mcx>, value: Datum_crate::Datum, attlen: i16) -> PgResult<Datum_crate::Datum> {
    let p = value.as_usize() as *const u8;
    if p.is_null() {
        return Ok(Datum::null());
    }
    let size = match attlen {
        -1 => {
            // SAFETY: non-null by-ref varlena datum; header byte readable.
            let tag = unsafe { *p };
            assert!(
                tag & 0x03 == 0,
                "ExecSetParamPlan (nodeSubplan.c): short/toasted varlena initplan \
                 result — detoast lane not ported"
            );
            // SAFETY: 4B-header form asserted above.
            unsafe { Datum_crate::VarlenaRef::from_ptr(p).varsize() }
        }
        -2 => {
            let mut n = 0usize;
            // SAFETY: non-null NUL-terminated cstring datum.
            while unsafe { *p.add(n) } != 0 {
                n += 1;
            }
            n + 1
        }
        l => {
            debug_assert!(l > 0);
            l as usize
        }
    };
    // SAFETY: `size` bytes readable per the arms above.
    let src = unsafe { core::slice::from_raw_parts(p, size) };
    let out = ::mcx::slice_in(mcx, src)?;
    Ok(Datum::from_usize(out.leak().as_ptr() as usize))
}
