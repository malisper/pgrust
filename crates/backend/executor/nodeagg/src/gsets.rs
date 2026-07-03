// nodeAgg.c grouping-sets machinery: phases (top Agg + chain), projected_set
// rollup emission, grouped_cols projection nulling, inter-phase tuplesorts.
// Sorted/plain strategies only — the planner gates hashed/AGG_MIXED grouping
// sets loud. Divergence: C resets aggcontexts[0..numReset] per set boundary;
// one bump arena serves every set here and reclaims at query end.
use std::ptr::NonNull;
use std::rc::Rc;

use ::execexpr::{
    exec_build_agg_trans_gsets, exec_eval_expr, exec_project, exec_qual, AggPerGroup,
    AggTransSpec, EvalSlots, ExprState, GroupedColsCell,
};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{vec_with_capacity_in, Allocator, Mcx, PgBox, PgVec};
use ::tuplesort::{Tuplesort, TUPLESORT_NONE};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::FmNodePtr;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::{Agg, Sort};
use ::types_pathnodes::{AGG_PLAIN, AGG_SORTED};
use ::types_portal::params::ParamBind;
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::TupleDescData;

use crate::AggStateData;

// Droppy element types (PgVec/PgBox members): the state box owns the drops
// (AggStateData peragg precedent), so reserve without the no-drop ctor.
fn droppy_vec<'mcx, T>(mcx: Mcx<'mcx>, cap: usize) -> PgResult<PgVec<'mcx, T>> {
    let mut v: PgVec<'mcx, T> = PgVec::new_in(mcx);
    v.try_reserve(cap).map_err(|_| mcx.oom(cap * core::mem::size_of::<T>()))?;
    Ok(v)
}


// C AggStatePerPhaseData; phases[0] here is C's phase 1 (the dummy hash
// phase 0 does not exist in the sorted-only port).
pub(crate) struct PerPhaseData<'mcx> {
    aggstrategy: u32,
    num_cols: usize,
    numsets: usize,
    gset_lengths: PgVec<'mcx, usize>,
    grouped_cols: PgVec<'mcx, PgVec<'mcx, i16>>,
    // Indexed by (compare length - 1), C phase->eqfunctions.
    eqfunctions: PgVec<'mcx, Option<PgBox<'mcx, ExprState<'mcx>>>>,
    evaltrans: PgBox<'mcx, ExprState<'mcx>>,
    sortnode: Option<&'mcx Sort<'mcx>>,
}

pub(crate) struct GroupingSetsState<'mcx> {
    phases: PgVec<'mcx, PerPhaseData<'mcx>>,
    current_phase: usize,
    projected_set: i32,
    input_done: bool,
    all_grouped_cols_desc: PgVec<'mcx, i16>,
    _pergroups: PgVec<'mcx, PgVec<'mcx, AggPerGroup>>,
    pergroup_bases: PgVec<'mcx, NonNull<AggPerGroup>>,
    first_slot: SlotData<'mcx>,
    first_stored: bool,
    pending_slot: SlotData<'mcx>,
    have_pending: bool,
    sort_in: Option<Tuplesort>,
    sort_out: Option<Tuplesort>,
    sort_slot: SlotData<'mcx>,
    sort_desc: Option<Rc<TupleDescData<'static>>>,
    grouped_cols_cell: NonNull<GroupedColsCell>,
}

impl GroupingSetsState<'_> {
    pub(crate) fn grouping_cell(&self) -> NonNull<GroupedColsCell> {
        self.grouped_cols_cell
    }
}

fn phase_aggnode<'mcx>(node: &'mcx Agg<'mcx>, phaseidx: usize) -> &'mcx Agg<'mcx> {
    if phaseidx == 0 {
        node
    } else {
        node.chain
            .nth(phaseidx - 1)
            .as_agg()
            .unwrap_or_else(|| panic!("ExecInitAgg (nodeAgg.c): Agg.chain cell is not an Agg"))
    }
}

pub(crate) fn init_grouping_sets<'mcx>(
    node: &'mcx Agg<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_desc_static: Option<Rc<TupleDescData<'static>>>,
    specs: &[AggTransSpec<'_, 'mcx>],
    numtrans: usize,
    fm_agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
    tmpcontext: ::executils::EcxtId,
) -> PgResult<PgBox<'mcx, GroupingSetsState<'mcx>>> {
    let mcx = estate.es_query_cxt;
    let per_tuple = estate.ecxt(tmpcontext).per_tuple_mcx();
    let numphases = 1 + node.chain.len();

    let mut maxsets = 1usize;
    for phaseidx in 0..numphases {
        let aggnode = phase_aggnode(node, phaseidx);
        if aggnode.aggstrategy != AGG_SORTED && aggnode.aggstrategy != AGG_PLAIN {
            panic!(
                "ExecInitAgg (nodeAgg.c): grouping-sets strategy {} (hashed/AGG_MIXED) \
                 not ported — grouping-sets lane",
                aggnode.aggstrategy
            );
        }
        maxsets = maxsets.max(aggnode.groupingSets.len());
    }

    let mut pergroups: PgVec<'mcx, PgVec<'mcx, AggPerGroup>> = droppy_vec(mcx, maxsets)?;
    let mut pergroup_bases: PgVec<'mcx, NonNull<AggPerGroup>> =
        vec_with_capacity_in(mcx, maxsets)?;
    for _ in 0..maxsets {
        let mut pg: PgVec<'mcx, AggPerGroup> = vec_with_capacity_in(mcx, numtrans.max(1))?;
        pg.resize(
            numtrans,
            AggPerGroup {
                trans_value: ::datum::Datum::null(),
                trans_value_is_null: true,
                no_trans_value: true,
            },
        );
        pergroup_bases.push(NonNull::new(pg.as_mut_ptr()).unwrap());
        pergroups.push(pg);
    }

    let outer_plan = node
        .plan
        .lefttree
        .and_then(Node::as_plan)
        .unwrap_or_else(|| panic!("ExecInitAgg (nodeAgg.c): Agg without an outer plan"));
    let scan_desc = execscan::exec_type_from_tl(mcx, &outer_plan.targetlist)?;

    let mut all_grouped: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    let mut phases: PgVec<'mcx, PerPhaseData<'mcx>> = droppy_vec(mcx, numphases)?;
    for phaseidx in 0..numphases {
        let aggnode = phase_aggnode(node, phaseidx);
        let sortnode = if phaseidx > 0 {
            let s = aggnode.plan.lefttree.and_then(Node::as_sort);
            assert!(s.is_some(), "ExecInitAgg (nodeAgg.c): chain Agg without a Sort");
            s
        } else {
            None
        };
        let num_cols = aggnode.numCols as usize;
        let numsets = aggnode.groupingSets.len();
        let mut gset_lengths: PgVec<'mcx, usize> = vec_with_capacity_in(mcx, numsets)?;
        let mut grouped_cols: PgVec<'mcx, PgVec<'mcx, i16>> = droppy_vec(mcx, numsets)?;
        for set in aggnode.groupingSets.iter() {
            let len = set
                .as_int_list()
                .unwrap_or_else(|| {
                    panic!("ExecInitAgg (nodeAgg.c): Agg.groupingSets cell is not an int list")
                })
                .len();
            // C: each set's columns are a grpColIdx prefix (planner contract).
            let mut cols: PgVec<'mcx, i16> = vec_with_capacity_in(mcx, len)?;
            cols.extend_from_slice(&aggnode.grpColIdx[..len]);
            cols.sort_unstable();
            for &c in cols.iter() {
                if !all_grouped.contains(&c) {
                    all_grouped.push(c);
                }
            }
            gset_lengths.push(len);
            grouped_cols.push(cols);
        }

        let mut eqfunctions: PgVec<'mcx, Option<PgBox<'mcx, ExprState<'mcx>>>> =
            droppy_vec(mcx, num_cols)?;
        for _ in 0..num_cols {
            eqfunctions.push(None);
        }
        if aggnode.aggstrategy == AGG_SORTED {
            debug_assert!(num_cols > 0);
            for k in 0..numsets {
                let length = gset_lengths[k];
                if length == 0 || eqfunctions[length - 1].is_some() {
                    continue;
                }
                eqfunctions[length - 1] =
                    Some(build_grouping_equal_prefix(mcx, &scan_desc, aggnode, length)?);
            }
            if eqfunctions[num_cols - 1].is_none() {
                eqfunctions[num_cols - 1] =
                    Some(build_grouping_equal_prefix(mcx, &scan_desc, aggnode, num_cols)?);
            }
        }

        let nsets_eff = numsets.max(1);
        let mut evaltrans = exec_build_agg_trans_gsets(
            mcx,
            specs,
            &pergroup_bases[..nsets_eff],
            fm_agg_node,
            params,
        )?;
        // By-ref transfn results ride the armed per-tuple mcx (lib.rs note).
        // SAFETY: the tmpcontext ExprContext outlives every phase program.
        unsafe { evaltrans.arm_result_mcx_raw(per_tuple) };

        phases.push(PerPhaseData {
            aggstrategy: aggnode.aggstrategy,
            num_cols,
            numsets,
            gset_lengths,
            grouped_cols,
            eqfunctions,
            evaltrans,
            sortnode,
        });
    }
    all_grouped.sort_unstable_by(|a, b| b.cmp(a));

    let cell_layout = core::alloc::Layout::new::<GroupedColsCell>();
    let raw = mcx.allocate(cell_layout).map_err(|_| mcx.oom(cell_layout.size()))?;
    let grouped_cols_cell: NonNull<GroupedColsCell> = raw.cast();
    // SAFETY: fresh allocation; repointed in prepare_projection_slot before
    // any projection reads it.
    unsafe { grouped_cols_cell.write(GroupedColsCell { ptr: core::ptr::null(), len: 0 }) };

    let first_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(scan_desc.clone()));
    let pending_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(scan_desc.clone()));
    let sort_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(scan_desc));
    let sort_desc = if numphases > 1 {
        Some(outer_desc_static.unwrap_or_else(|| {
            panic!("ExecInitAgg (nodeAgg.c): chained grouping sets need the outer result type")
        }))
    } else {
        None
    };

    let mut gs = ::mcx::alloc_in(
        mcx,
        GroupingSetsState {
            phases,
            current_phase: 0,
            projected_set: -1,
            input_done: false,
            all_grouped_cols_desc: all_grouped,
            _pergroups: pergroups,
            pergroup_bases,
            first_slot,
            first_stored: false,
            pending_slot,
            have_pending: false,
            sort_in: None,
            sort_out: None,
            sort_slot,
            sort_desc,
            grouped_cols_cell,
        },
    )?;
    initialize_phase(&mut gs, 0)?;
    Ok(gs)
}

fn build_grouping_equal_prefix<'mcx>(
    mcx: Mcx<'mcx>,
    desc: &Rc<TupleDescData<'mcx>>,
    aggnode: &Agg<'_>,
    length: usize,
) -> PgResult<PgBox<'mcx, ExprState<'mcx>>> {
    let mut eqfuncoids: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, length)?;
    for &op in &aggnode.grpOperators[..length] {
        eqfuncoids.push(lsyscache::get_opcode(op)?);
    }
    ::execexpr::exec_build_grouping_equal(
        mcx,
        desc,
        desc,
        &aggnode.grpColIdx[..length],
        &eqfuncoids,
        &aggnode.grpCollations[..length],
    )
}

// C initialize_phase: swap sort_out into sort_in (performing the sort) and
// open the next phase's tuplesort when one follows.
fn initialize_phase(gs: &mut GroupingSetsState<'_>, newphase: usize) -> PgResult<()> {
    debug_assert!(newphase == 0 || newphase == gs.current_phase + 1);
    gs.sort_in = None;
    if newphase == 0 {
        gs.sort_out = None;
    } else {
        let mut ts = gs.sort_out.take().expect("previous phase fed the next phase's sort");
        ts.performsort()?;
        gs.sort_in = Some(ts);
    }
    if newphase + 1 < gs.phases.len() {
        let sortnode =
            gs.phases[newphase + 1].sortnode.expect("chain Agg without a Sort (init checked)");
        let work_mem = init_small::globals::work_mem();
        gs.sort_out = Some(Tuplesort::begin_heap(
            gs.sort_desc.clone().expect("chained grouping sets carry the outer result type"),
            sortnode.sortColIdx,
            sortnode.sortOperators,
            sortnode.collations,
            sortnode.nullsFirst,
            work_mem,
            TUPLESORT_NONE,
        )?);
    }
    gs.current_phase = newphase;
    Ok(())
}

enum Fetched {
    Outer(ExecSlotId),
    Sorted,
}

fn fetch_input_tuple<'mcx, F>(
    gs: &mut GroupingSetsState<'mcx>,
    estate: &mut EStateData<'mcx>,
    fetch_outer: &mut F,
) -> PgResult<Option<Fetched>>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    let mcx = estate.es_query_cxt;
    if gs.sort_in.is_some() {
        let GroupingSetsState { sort_in, sort_out, sort_slot, .. } = gs;
        if !sort_in.as_mut().unwrap().gettupleslot(true, false, sort_slot, mcx)? {
            return Ok(None);
        }
        if let Some(out) = sort_out.as_mut() {
            out.puttupleslot(sort_slot, mcx)?;
        }
        Ok(Some(Fetched::Sorted))
    } else {
        match fetch_outer(estate)? {
            None => Ok(None),
            Some(id) => {
                if gs.sort_out.is_some() {
                    let slot = estate.slot_mut(id);
                    gs.sort_out.as_mut().unwrap().puttupleslot(slot, mcx)?;
                }
                Ok(Some(Fetched::Outer(id)))
            }
        }
    }
}

// C prepare_projection_slot: publish the set's grouped_cols (read by
// EEOP_GROUPING_FUNC) and null out ungrouped columns of the representative.
fn prepare_projection_slot<'mcx>(
    gs: &mut GroupingSetsState<'mcx>,
    current_set: usize,
    mcx: Mcx<'mcx>,
) {
    let phase = &gs.phases[gs.current_phase];
    let grouped = &phase.grouped_cols[current_set];
    // SAFETY: once-allocated cell; the per-set column vecs are stable after
    // init (never resized).
    unsafe {
        gs.grouped_cols_cell
            .write(GroupedColsCell { ptr: grouped.as_ptr(), len: grouped.len() })
    };
    if !gs.first_stored {
        exectuples::exec_store_all_null_tuple(&mut gs.first_slot, mcx);
        return;
    }
    if let Some(&max_col) = gs.all_grouped_cols_desc.first() {
        exectuples::slot_getsomeattrs(&mut gs.first_slot, max_col as i32);
        let base = gs.first_slot.base_mut();
        for &attnum in gs.all_grouped_cols_desc.iter() {
            if !grouped.contains(&attnum) {
                base.tts_isnull[(attnum - 1) as usize] = true;
            }
        }
    }
}

fn initialize_aggregates_sets(node: &mut AggStateData<'_>, num_reset: usize) {
    let gs = node.gsets.as_mut().expect("grouping-sets retrieve");
    for setno in 0..num_reset {
        let base = gs.pergroup_bases[setno];
        for (transno, init) in node.trans_init.iter().enumerate() {
            // SAFETY: transno < numtrans slots of the once-allocated per-set
            // array; base pointers are the sole access path.
            unsafe {
                base.as_ptr().add(transno).write(AggPerGroup {
                    trans_value: init.value,
                    trans_value_is_null: init.isnull,
                    no_trans_value: init.isnull,
                });
            }
        }
    }
}

// C agg_retrieve_direct, grouping-sets form. projected_set is -1 initially or
// the just-completed index into gset_lengths (C invariant).
pub(crate) fn agg_retrieve_grouping_sets<'mcx, F>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    fetch_outer: &mut F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    let mcx = estate.es_query_cxt;
    'outer: while !node.agg_done {
        estate.reset_expr_context(node.ps_ExprContext);

        let (num_grouping_sets, num_reset) = {
            let gs = node.gsets.as_ref().unwrap();
            let n = gs.phases[gs.current_phase].numsets.max(1);
            let r = if gs.projected_set >= 0 && (gs.projected_set as usize) < n {
                gs.projected_set as usize + 1
            } else {
                n
            };
            (n, r)
        };

        {
            let gs = node.gsets.as_mut().unwrap();
            if gs.input_done && gs.projected_set >= num_grouping_sets as i32 - 1 {
                if gs.current_phase < gs.phases.len() - 1 {
                    let next = gs.current_phase + 1;
                    initialize_phase(gs, next)?;
                    gs.input_done = false;
                    gs.projected_set = -1;
                    gs.first_stored = false;
                    continue 'outer;
                }
                node.agg_done = true;
                break;
            }
        }

        let boundary = {
            let gs = node.gsets.as_mut().unwrap();
            let phase = &gs.phases[gs.current_phase];
            let next_set_size = if gs.projected_set >= 0
                && (gs.projected_set as usize) < num_grouping_sets - 1
            {
                phase.gset_lengths[gs.projected_set as usize + 1]
            } else {
                0
            };
            if gs.input_done {
                true
            } else if phase.aggstrategy != AGG_PLAIN
                && gs.projected_set != -1
                && (gs.projected_set as usize) < num_grouping_sets - 1
                && next_set_size > 0
            {
                debug_assert!(gs.have_pending);
                let GroupingSetsState { phases, first_slot, pending_slot, current_phase, .. } =
                    &mut **gs;
                let eq = phases[*current_phase].eqfunctions[next_set_size - 1]
                    .as_mut()
                    .expect("eqfunctions built for every set length");
                let mut slots = EvalSlots {
                    scan: None,
                    inner: Some(first_slot),
                    outer: Some(pending_slot),
                };
                !exec_qual(Some(eq), &mut slots)?
            } else {
                false
            }
        };

        if boundary {
            let gs = node.gsets.as_mut().unwrap();
            gs.projected_set += 1;
            debug_assert!((gs.projected_set as usize) < num_grouping_sets);
        } else {
            {
                let gs = node.gsets.as_mut().unwrap();
                gs.projected_set = 0;
            }
            let mut have_group = true;
            let pending = node.gsets.as_ref().unwrap().have_pending;
            if pending {
                let gs = node.gsets.as_mut().unwrap();
                let GroupingSetsState { first_slot, pending_slot, .. } = &mut **gs;
                core::mem::swap(first_slot, pending_slot);
                gs.have_pending = false;
                gs.first_stored = true;
            } else {
                match fetch_input_tuple(node.gsets.as_mut().unwrap(), estate, fetch_outer)? {
                    Some(fetched) => {
                        copy_into_first(node, estate, fetched)?;
                    }
                    None => {
                        // Empty input: project only the zero-size sets.
                        let gs = node.gsets.as_mut().unwrap();
                        gs.input_done = true;
                        gs.first_stored = false;
                        let mut proj = gs.projected_set as usize;
                        let lengths = &gs.phases[gs.current_phase].gset_lengths;
                        while lengths[proj] > 0 {
                            proj += 1;
                            if proj >= num_grouping_sets {
                                break;
                            }
                        }
                        gs.projected_set = proj as i32;
                        if proj >= num_grouping_sets {
                            continue 'outer;
                        }
                        have_group = false;
                    }
                }
            }
            initialize_aggregates_sets(node, num_reset);
            if have_group {
                drain_group(node, estate, fetch_outer)?;
            }
        }

        let current_set = node.gsets.as_ref().unwrap().projected_set as usize;
        prepare_projection_slot(node.gsets.as_mut().unwrap(), current_set, mcx);
        let pergroup = node.gsets.as_ref().unwrap().pergroup_bases[current_set];
        crate::finalize_aggregates(node, estate, pergroup)?;

        {
            let AggStateData { gsets, qual, .. } = node;
            let gs = gsets.as_mut().unwrap();
            let mut slots =
                EvalSlots { scan: None, inner: None, outer: Some(&mut gs.first_slot) };
            if !exec_qual(qual.as_deref_mut(), &mut slots)? {
                continue;
            }
        }
        let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
        let gs = node.gsets.as_mut().unwrap();
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(&mut gs.first_slot) };
        exec_project(&mut node.proj, &mut slots, result_slot, mcx)?;
        return Ok(Some(node.ps_ResultTupleSlot));
    }
    Ok(None)
}

fn copy_into_first<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    fetched: Fetched,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    match fetched {
        Fetched::Outer(id) => {
            let slot = estate.slot_mut(id);
            let gs = node.gsets.as_mut().unwrap();
            exectuples::exec_copy_slot(&mut gs.first_slot, slot, mcx, mcx)?;
        }
        Fetched::Sorted => {
            let gs = node.gsets.as_mut().unwrap();
            let GroupingSetsState { first_slot, sort_slot, .. } = &mut **gs;
            exectuples::exec_copy_slot(first_slot, sort_slot, mcx, mcx)?;
        }
    }
    node.gsets.as_mut().unwrap().first_stored = true;
    Ok(())
}

// The inner advance loop: accumulate the group starting at first_slot until
// the outer input ends or the full-width grouping columns change.
fn drain_group<'mcx, F>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    fetch_outer: &mut F,
) -> PgResult<()>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    let mcx = estate.es_query_cxt;
    {
        let gs = node.gsets.as_mut().unwrap();
        let GroupingSetsState { phases, first_slot, current_phase, .. } = &mut **gs;
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(first_slot) };
        exec_eval_expr(&mut phases[*current_phase].evaltrans, &mut slots)?;
    }
    estate.reset_expr_context(node.tmpcontext);
    loop {
        let fetched = match fetch_input_tuple(node.gsets.as_mut().unwrap(), estate, fetch_outer)?
        {
            Some(f) => f,
            None => {
                node.gsets.as_mut().unwrap().input_done = true;
                return Ok(());
            }
        };
        let crossed = {
            let gs = node.gsets.as_mut().unwrap();
            let phase = &gs.phases[gs.current_phase];
            if phase.aggstrategy != AGG_PLAIN && phase.num_cols > 0 {
                let num_cols = phase.num_cols;
                match fetched {
                    Fetched::Outer(id) => {
                        let outer_slot = estate.slot_mut(id);
                        let GroupingSetsState { phases, first_slot, current_phase, .. } =
                            &mut **gs;
                        let eq = phases[*current_phase].eqfunctions[num_cols - 1]
                            .as_mut()
                            .expect("full-width eqfunction built");
                        let mut slots = EvalSlots {
                            scan: None,
                            inner: Some(first_slot),
                            outer: Some(&mut *outer_slot),
                        };
                        if !exec_qual(Some(eq), &mut slots)? {
                            let GroupingSetsState { pending_slot, .. } = &mut **gs;
                            exectuples::exec_copy_slot(pending_slot, outer_slot, mcx, mcx)?;
                            gs.have_pending = true;
                            return Ok(());
                        }
                        false
                    }
                    Fetched::Sorted => {
                        let GroupingSetsState {
                            phases,
                            first_slot,
                            pending_slot,
                            sort_slot,
                            current_phase,
                            ..
                        } = &mut **gs;
                        let eq = phases[*current_phase].eqfunctions[num_cols - 1]
                            .as_mut()
                            .expect("full-width eqfunction built");
                        let mut slots = EvalSlots {
                            scan: None,
                            inner: Some(first_slot),
                            outer: Some(&mut *sort_slot),
                        };
                        if !exec_qual(Some(eq), &mut slots)? {
                            exectuples::exec_copy_slot(pending_slot, sort_slot, mcx, mcx)?;
                            gs.have_pending = true;
                            return Ok(());
                        }
                        false
                    }
                }
            } else {
                false
            }
        };
        debug_assert!(!crossed);
        {
            let gs = node.gsets.as_mut().unwrap();
            match fetched {
                Fetched::Outer(id) => {
                    let outer_slot = estate.slot_mut(id);
                    let GroupingSetsState { phases, current_phase, .. } = &mut **gs;
                    let mut slots =
                        EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
                    exec_eval_expr(&mut phases[*current_phase].evaltrans, &mut slots)?;
                }
                Fetched::Sorted => {
                    let GroupingSetsState { phases, sort_slot, current_phase, .. } = &mut **gs;
                    let mut slots =
                        EvalSlots { scan: None, inner: None, outer: Some(sort_slot) };
                    exec_eval_expr(&mut phases[*current_phase].evaltrans, &mut slots)?;
                }
            }
        }
        estate.reset_expr_context(node.tmpcontext);
    }
}

pub(crate) fn rescan_grouping_sets(gs: &mut GroupingSetsState<'_>) -> PgResult<()> {
    gs.input_done = false;
    gs.projected_set = -1;
    gs.have_pending = false;
    gs.first_stored = false;
    gs.sort_in = None;
    gs.sort_out = None;
    initialize_phase(gs, 0)
}
