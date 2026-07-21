// nodeAgg.c grouping-sets machinery: phases (top Agg + chain), projected_set
// rollup emission, grouped_cols projection nulling, inter-phase tuplesorts,
// hashed/AGG_MIXED grouping sets (C's phase 0). hashagg spill: C spills
// per-set (lookup_hash_entries, hashagg_spill_tuple, nodeAgg.c:2180-3233) —
// hash_ngroups_current/hash_mem_limit/spill_mode/tapeset are node-shared
// (C's AggState fields), but each set gets its own HashAggSpill/partitions
// and its own refill trans program (indirect through its cell), since a
// refill batch processes exactly one grouping set at a time. Divergence: C
// resets aggcontexts[0..numReset] per set boundary; one bump arena serves
// every set here and reclaims at query end.
use core::alloc::Layout;
use std::ptr::NonNull;
use std::rc::Rc;

use ::execexpr::{
    exec_build_agg_trans_gsets, exec_build_agg_trans_hashed, exec_eval_expr, exec_project,
    exec_qual, AggPerGroup, AggTransSpec, EvalSlots, ExprState, GroupedColsCell,
};
use ::execgrouping::TupleHashTable;
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{vec_with_capacity_in, Allocator, Mcx, MemoryContext, PgBox, PgVec};
use ::sort_storage::{LogicalTapeSet, TapeIdx};
use ::tuplesort::{Tuplesort, TUPLESORT_NONE};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::FmNodePtr;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::{Agg, Sort};
use ::types_pathnodes::{AGG_HASHED, AGG_MIXED, AGG_PLAIN, AGG_SORTED};
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


// C AggStatePerPhaseData for the SORTED phases only: phases[i] here is C's
// phase i+1; C's phase 0 is HashSetsState.
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

// C AggStatePerHashData, one hashed grouping set.
pub(crate) struct PerHashSetData<'mcx> {
    hashtable: TupleHashTable<'mcx>,
    hashslot: SlotData<'mcx>,
    retrieve_slot: SlotData<'mcx>,
    hash_grp_col_idx_input: PgVec<'mcx, i16>,
    largest_grp_col_idx: i32,
    grouped_cols: PgVec<'mcx, i16>,
    cell: NonNull<NonNull<AggPerGroup>>,
    // u64: in hashtable mode this is execgrouping's packed (start,visited)
    // cursor, whose high-32 packing must survive 32-bit wasm; in compact
    // mode it is a plain row index (cast at use).
    hashiter: u64,
    // hash_agg_set_limits' input_groups for this set's hashagg_spill_init
    // calls (C: perhash->aggnode->numGroups).
    num_groups: f64,
    // C hash_spills[setno]: this set's spill partitions, lazily created on
    // its first spilled tuple (spill->partitions == NULL check).
    spill: Option<crate::HashAggSpill<'mcx>>,
    // Indirect trans program for exactly this set (exec_build_agg_trans_hashed),
    // used by agg_refill_hash_table_gs: a refill batch advances one set at a
    // time, unlike the combined mixed/gsets program which expects every
    // set's cell live for the row.
    refill_trans: PgBox<'mcx, ExprState<'mcx>>,
}

// C's phase 0: every AGG_HASHED/AGG_MIXED aggnode of node+chain is one set.
pub(crate) struct HashSetsState<'mcx> {
    perhash: PgVec<'mcx, PerHashSetData<'mcx>>,
    hash_first_slot: SlotData<'mcx>,
    // hash_ngroups_current/hash_mem_limit/hashentrysize/spill_mode/
    // ever_spilled/tapeset are C AggState fields shared across every hash
    // set (hash_tablecxt/hashcontext are one context for all sets too; here
    // that's agg_node.aggcontext(), read where needed).
    hash_ngroups_limit: u64,
    hash_ngroups_current: u64,
    hash_mem_limit: usize,
    hashentrysize: f64,
    spill_mode: bool,
    ever_spilled: bool,
    tapeset: Option<LogicalTapeSet<'mcx>>,
    // C hash_batches: a stack, top at the end; each batch also carries setno
    // (C's HashAggBatch.setno) since sets aren't distinguishable by tape.
    hash_batches: PgVec<'mcx, HashAggBatchGs>,
    all_cols_needed: bool,
    max_colno_needed: i32,
    colnos_needed: PgVec<'mcx, bool>,
    rslot: SlotData<'mcx>,
    wslot: SlotData<'mcx>,
    read_buf: PgVec<'mcx, u64>,
    tmp_ctx: MemoryContext,
    table_filled: bool,
    current_set: usize,
}

// C HashAggBatch, grouping-sets form (setno-tagged).
struct HashAggBatchGs {
    setno: usize,
    input_tape: TapeIdx,
    used_bits: u32,
    input_card: f64,
}

impl GroupingSetsState<'_> {
    pub(crate) fn collect_param_deps(&self, deps: &mut Vec<u32>) {
        for ph in self.phases.iter() {
            deps.extend_from_slice(ph.evaltrans.param_exec_deps());
        }
        if let Some(h) = self.hash.as_ref() {
            for ph in h.perhash.iter() {
                deps.extend_from_slice(ph.refill_trans.param_exec_deps());
            }
        }
    }
}

pub(crate) struct GroupingSetsState<'mcx> {
    phases: PgVec<'mcx, PerPhaseData<'mcx>>,
    current_phase: usize,
    projected_set: i32,
    input_done: bool,
    all_grouped_cols_desc: PgVec<'mcx, i16>,
    _pergroups: PgVec<'mcx, PgVec<'mcx, AggPerGroup>>,
    pergroup_bases: PgVec<'mcx, NonNull<AggPerGroup>>,
    pub(crate) first_slot: SlotData<'mcx>,
    first_stored: bool,
    pending_slot: SlotData<'mcx>,
    have_pending: bool,
    sort_in: Option<Tuplesort>,
    sort_out: Option<Tuplesort>,
    sort_slot: SlotData<'mcx>,
    sort_desc: Option<Rc<TupleDescData<'static>>>,
    grouped_cols_cell: NonNull<GroupedColsCell>,
    hash: Option<HashSetsState<'mcx>>,
    mixed: bool,
    in_hash_mode: bool,
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
    let numnodes = 1 + node.chain.len();

    // C's phase split: AGG_HASHED/AGG_MIXED aggnodes are phase-0 hash sets,
    // the rest are sorted phases in chain order.
    let mut sorted_nodes: PgVec<'mcx, &'mcx Agg<'mcx>> = vec_with_capacity_in(mcx, numnodes)?;
    let mut hashed_nodes: PgVec<'mcx, &'mcx Agg<'mcx>> = vec_with_capacity_in(mcx, numnodes)?;
    let mut maxsets = 1usize;
    for phaseidx in 0..numnodes {
        let aggnode = phase_aggnode(node, phaseidx);
        match aggnode.aggstrategy {
            AGG_HASHED | AGG_MIXED => hashed_nodes.push(aggnode),
            AGG_SORTED | AGG_PLAIN => sorted_nodes.push(aggnode),
            s => panic!("ExecInitAgg (nodeAgg.c): grouping-sets strategy {s} cannot happen"),
        }
        maxsets = maxsets.max(aggnode.groupingSets.len());
    }
    let mixed = node.aggstrategy == AGG_MIXED;
    debug_assert!(hashed_nodes.is_empty() || node.aggstrategy == AGG_HASHED || mixed);
    let numphases = sorted_nodes.len();

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

    // The gset column lists are grpColIdx prefixes, so each node's grouped
    // union is grpColIdx[..numCols].
    let mut all_grouped: PgVec<'mcx, i16> = PgVec::new_in(mcx);
    for phaseidx in 0..numnodes {
        let aggnode = phase_aggnode(node, phaseidx);
        for &c in &aggnode.grpColIdx[..aggnode.numCols as usize] {
            if !all_grouped.contains(&c) {
                all_grouped.push(c);
            }
        }
    }

    let mut hash = if hashed_nodes.is_empty() {
        None
    } else {
        Some(init_hash_sets(
            node,
            estate,
            &hashed_nodes,
            &all_grouped,
            &scan_desc,
            numtrans,
            specs,
            fm_agg_node,
            params,
        )?)
    };
    let per_tuple = estate.ecxt(tmpcontext).per_tuple_mcx();
    if let Some(h) = hash.as_mut() {
        for ph in h.perhash.iter_mut() {
            // SAFETY: the tmpcontext ExprContext outlives every phase program.
            unsafe { ph.refill_trans.arm_result_mcx_raw(per_tuple) };
            // C BuildTupleHashTable tempcxt (lib.rs init note): probe-time
            // detoasts of compressed by-ref keys must live in the per-row
            // reset context, not query memory — bounded, spill-accountable.
            // SAFETY: same outlives argument as the arming above.
            unsafe { ph.hashtable.set_temp_ctx_raw(per_tuple) };
        }
    }
    let mut phases: PgVec<'mcx, PerPhaseData<'mcx>> = droppy_vec(mcx, numphases)?;
    for (phaseidx, &aggnode) in sorted_nodes.iter().enumerate() {
        let sortnode = if core::ptr::eq(aggnode, node) {
            None
        } else {
            aggnode.plan.lefttree.and_then(Node::as_sort)
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
            for eq in eqfunctions.iter_mut().flatten() {
                // Boundary eqs detoast compressed by-ref keys through the
                // frame's result mcx; C runs them in tmpcontext per-tuple
                // memory, reset per input row.
                // SAFETY: the tmpcontext ExprContext outlives every phase
                // program (same estate).
                unsafe { eq.arm_result_mcx_raw(per_tuple) };
            }
        }

        let nsets_eff = numsets.max(1);
        // C: phase one, and only phase one, of a mixed agg also advances the
        // hash-set transitions (dosort + dohash), null-checking pergroup per
        // set. Our compiled indirect trans steps don't null-check, so the
        // hash side runs through each set's own program from
        // lookup_hash_entries instead; this program stays sorted-only even
        // for phase 0 of a mixed agg.
        let mut evaltrans =
            exec_build_agg_trans_gsets(mcx, specs, &pergroup_bases[..nsets_eff], fm_agg_node, params)?;
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

    let hash = hash;
    let pure_hashed = numphases == 0;

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
            hash,
            mixed,
            in_hash_mode: pure_hashed,
        },
    )?;
    if !pure_hashed {
        initialize_phase(&mut gs, 0)?;
    }
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

// find_hash_columns + build_hash_tables (nodeAgg.c), grouping-sets form.
#[allow(clippy::too_many_arguments)]
fn init_hash_sets<'mcx>(
    node: &'mcx Agg<'mcx>,
    estate: &mut EStateData<'mcx>,
    hashed_nodes: &[&'mcx Agg<'mcx>],
    all_grouped: &[i16],
    scan_desc: &Rc<TupleDescData<'mcx>>,
    numtrans: usize,
    specs: &[AggTransSpec<'_, 'mcx>],
    fm_agg_node: FmNodePtr,
    params: ParamBind<'mcx>,
) -> PgResult<HashSetsState<'mcx>> {
    let mcx = estate.es_query_cxt;
    let outer_plan = node
        .plan
        .lefttree
        .and_then(Node::as_plan)
        .unwrap_or_else(|| panic!("ExecInitAgg (nodeAgg.c): Agg without an outer plan"));
    let outer_tlist = &outer_plan.targetlist;
    let outer_natts = outer_tlist.len();
    let num_hashes = hashed_nodes.len();

    let mut base_cols: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, outer_natts)?;
    base_cols.resize(outer_natts, false);
    for tle in node.plan.targetlist.iter() {
        crate::collect_base_var_cols(tle, &mut base_cols);
    }
    for q in node.plan.qual.iter() {
        crate::collect_base_var_cols(q, &mut base_cols);
    }

    let hashentrysize = crate::hash_agg_entry_size(
        numtrans,
        outer_plan.plan_width.max(0) as usize,
        node.transitionSpace as usize,
    );
    let total_groups: f64 = hashed_nodes.iter().map(|a| a.numGroups as f64).sum();
    let (mem_limit, hash_ngroups_limit, planned_partitions) =
        crate::hash_agg_set_limits(hashentrysize, total_groups, 0);
    estate.es_agg_instrumentation.push((
        node.plan.plan_node_id,
        ::types_core::instrument::AggregateInstrumentation {
            hash_batches_used: 1,
            hash_planned_partitions: planned_partitions as i32,
            ..Default::default()
        },
    ));

    let mut perhash: PgVec<'mcx, PerHashSetData<'mcx>> = droppy_vec(mcx, num_hashes)?;
    for &aggnode in hashed_nodes {
        let num_cols = aggnode.numCols as usize;
        assert!(num_cols > 0 && aggnode.grpColIdx.len() == num_cols);
        assert!(aggnode.numGroups > 0, "Agg.numGroups unset (planner must estimate it)");

        let mut grouped_cols: PgVec<'mcx, i16> = vec_with_capacity_in(mcx, num_cols)?;
        grouped_cols.extend_from_slice(&aggnode.grpColIdx[..num_cols]);
        grouped_cols.sort_unstable();

        // Vars nulled by prepare_projection_slot for this set (grouped
        // elsewhere only) are not worth storing (C find_hash_columns).
        let mut colnos: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, outer_natts)?;
        colnos.extend_from_slice(&base_cols);
        for &attnum in all_grouped {
            if !grouped_cols.contains(&attnum) {
                colnos[(attnum - 1) as usize] = false;
            }
        }
        let mut hash_grp_col_idx_input: PgVec<'mcx, i16> =
            vec_with_capacity_in(mcx, outer_natts + num_cols)?;
        for &attno in &aggnode.grpColIdx[..num_cols] {
            hash_grp_col_idx_input.push(attno);
            colnos[(attno - 1) as usize] = false;
        }
        for (i, &needed) in colnos.iter().enumerate() {
            if needed {
                hash_grp_col_idx_input.push((i + 1) as i16);
            }
        }
        let largest_grp_col_idx =
            hash_grp_col_idx_input.iter().map(|&a| a as i32).max().unwrap_or(0);

        let mut hash_tlist = types_nodes::list::NodeList::nil();
        for &attno in hash_grp_col_idx_input.iter() {
            hash_tlist.lappend(mcx, outer_tlist.nth((attno - 1) as usize))?;
        }
        let hash_desc = execscan::exec_type_from_tl(mcx, &hash_tlist)?;

        let (eqfuncoids, hashfunctions) =
            ::execgrouping::exec_tuples_hash_prepare(mcx, aggnode.grpOperators)?;
        let nbuckets = crate::hash_choose_num_buckets(
            hashentrysize,
            aggnode.numGroups,
            mem_limit / num_hashes,
        );
        let mut key_col_idx: PgVec<'mcx, i16> = vec_with_capacity_in(mcx, num_cols)?;
        for i in 0..num_cols {
            key_col_idx.push((i + 1) as i16);
        }
        let additionalsize = numtrans * core::mem::size_of::<AggPerGroup>();
        let hashtable = ::execgrouping::build_tuple_hash_table(
            mcx,
            &hash_desc,
            &key_col_idx,
            &eqfuncoids,
            &hashfunctions,
            aggnode.grpCollations,
            nbuckets,
            additionalsize,
            false,
        )?;
        let hashslot = exectuples::make_tuple_table_slot(
            mcx,
            TupleSlotKind::Virtual,
            Some(hash_desc.clone()),
        );
        let retrieve_slot =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(hash_desc));

        let cell_layout = Layout::new::<NonNull<AggPerGroup>>();
        let raw = mcx.allocate(cell_layout).map_err(|_| mcx.oom(cell_layout.size()))?;
        let cell: NonNull<NonNull<AggPerGroup>> = raw.cast();
        // SAFETY: fresh allocation; repointed before every trans-program run
        // (lookup_hash_entries).
        unsafe { cell.write(NonNull::dangling()) };

        // A refill batch (agg_refill_hash_table_gs) advances exactly one
        // set's cell per row; the combined mixed/gsets program assumes every
        // set's cell is live, so this set gets its own indirect program
        // (exec_build_agg_trans_hashed, C's per-set hashagg_recompile
        // shape) built once at init. Armed by the caller once per_tuple is
        // available (arming here would tie *estate's borrow to 'mcx).
        let refill_trans = exec_build_agg_trans_hashed(mcx, specs, cell, fm_agg_node, params)?;

        perhash.push(PerHashSetData {
            hashtable,
            hashslot,
            retrieve_slot,
            hash_grp_col_idx_input,
            largest_grp_col_idx,
            grouped_cols,
            cell,
            hashiter: 0,
            num_groups: aggnode.numGroups as f64,
            spill: None,
            refill_trans,
        });
    }

    // C colnos_needed/all_cols_needed/max_colno_needed (find_cols): the
    // spilled tuple shape must carry every grouping column of every set
    // (all_grouped is the union across the whole node, sorted+hashed) AND
    // every aggregated input column — spill_tuple_gs NULLs everything else,
    // so a missing aggregate arg column silently NULLs that agg's result
    // for every group refilled from tape.
    let mut colnos_needed = base_cols;
    for &attnum in all_grouped {
        colnos_needed[(attnum - 1) as usize] = true;
    }
    {
        let mut aggrefs: PgVec<'mcx, (Node<'mcx>, &'mcx types_nodes::primnodes::Aggref<'mcx>)> =
            PgVec::new_in(mcx);
        for tle in node.plan.targetlist.iter() {
            crate::collect_aggrefs(tle, &mut aggrefs);
        }
        for q in node.plan.qual.iter() {
            crate::collect_aggrefs(q, &mut aggrefs);
        }
        for &(_, aggref) in aggrefs.iter() {
            for a in aggref.args.iter() {
                crate::collect_base_var_cols(a, &mut colnos_needed);
            }
            for a in aggref.aggdirectargs.iter() {
                crate::collect_base_var_cols(a, &mut colnos_needed);
            }
            if let Some(f) = aggref.aggfilter {
                crate::collect_base_var_cols(f, &mut colnos_needed);
            }
        }
    }
    let mut max_colno_needed = 0i32;
    let mut all_cols_needed = true;
    for (i, &n) in colnos_needed.iter().enumerate() {
        if n {
            max_colno_needed = (i + 1) as i32;
        } else {
            all_cols_needed = false;
        }
    }
    let outer_desc = execscan::exec_type_from_tl(mcx, outer_tlist)?;
    let rslot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(outer_desc.clone()));
    let wslot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(outer_desc));
    let tmp_ctx = mcx.context().new_child_bump("HashAgg spill tuple");

    let hash_first_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(scan_desc.clone()));
    Ok(HashSetsState {
        perhash,
        hash_first_slot,
        hash_ngroups_limit,
        hash_ngroups_current: 0,
        hash_mem_limit: mem_limit,
        hashentrysize,
        spill_mode: false,
        ever_spilled: false,
        tapeset: None,
        hash_batches: PgVec::new_in(mcx),
        all_cols_needed,
        max_colno_needed,
        colnos_needed,
        rslot,
        wslot,
        read_buf: PgVec::new_in(mcx),
        tmp_ctx,
        table_filled: false,
        current_set: 0,
    })
}

// hash_agg_check_limits (nodeAgg.c): total_mem/ngroups are node-shared
// (every set feeds the same aggcontext and the same group counter).
fn hash_agg_check_limits_gs<'mcx>(
    h: &mut HashSetsState<'mcx>,
    aggctx: Mcx<'_>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let ngroups = h.hash_ngroups_current;
    let meta_mem: usize = h.perhash.iter().map(|ph| ph.hashtable.meta_mem()).sum();
    let total_mem = meta_mem + aggctx.context().subtree_used();
    if ngroups > 0 && (total_mem > h.hash_mem_limit || ngroups > h.hash_ngroups_limit) {
        h.spill_mode = true;
        if !h.ever_spilled {
            h.ever_spilled = true;
            h.tapeset = Some(LogicalTapeSet::create(mcx, true)?);
        }
        crate::hashagg_release_retained("enter_spill");
    }
    Ok(())
}

// hashagg_spill_tuple (nodeAgg.c), grouping-sets form: `input` None reads
// from `h.rslot` (the refill batch's just-read tuple); each set gets its own
// HashAggSpill (C hash_spills[setno]), lazily created on its first miss.
fn spill_tuple_gs<'mcx>(
    h: &mut HashSetsState<'mcx>,
    setno: usize,
    used_bits: u32,
    input: Option<&mut SlotData<'mcx>>,
    hash: u32,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let HashSetsState {
        perhash,
        tapeset,
        wslot,
        rslot,
        all_cols_needed,
        max_colno_needed,
        colnos_needed,
        tmp_ctx,
        hashentrysize,
        ..
    } = h;
    let tapeset = tapeset.as_mut().expect("spill mode has a tapeset");
    let ph = &mut perhash[setno];
    if ph.spill.is_none() {
        ph.spill =
            Some(crate::hashagg_spill_init(mcx, tapeset, used_bits, ph.num_groups, *hashentrysize)?);
    }
    let spill = ph.spill.as_mut().unwrap();
    let input = match input {
        Some(s) => s,
        None => rslot,
    };
    let slot = if !*all_cols_needed {
        exectuples::slot_getsomeattrs(input, *max_colno_needed);
        exectuples::exec_clear_tuple(wslot, mcx);
        {
            let src = input.base();
            let dst = wslot.base_mut();
            for (i, &needed) in colnos_needed.iter().enumerate() {
                if needed {
                    dst.tts_values[i] = src.tts_values[i];
                    dst.tts_isnull[i] = src.tts_isnull[i];
                } else {
                    dst.tts_isnull[i] = true;
                }
            }
        }
        exectuples::exec_store_virtual_tuple(wslot);
        wslot
    } else {
        input
    };
    {
        let fetched = exectuples::exec_fetch_slot_minimal_tuple(slot, mcx, tmp_ctx.mcx())?;
        let (ptr, len): (*const u8, usize) = match &fetched {
            // SAFETY: live image led by t_len.
            exectuples::FetchedMinimalTuple::Slot(p, _) => {
                (p.as_ptr().cast(), unsafe { (*p.as_ptr()).t_len } as usize)
            }
            exectuples::FetchedMinimalTuple::Copied(t) => (t.as_ptr(), t.t_len() as usize),
        };
        let partition =
            if spill.shift < 32 { ((hash & spill.mask) >> spill.shift) as usize } else { 0 };
        spill.ntuples[partition] += 1;
        // Hash the hash: partition-shared bits skew the HLL otherwise.
        spill.hll_card[partition].add(::hashfn::hash_bytes_uint32(hash));
        let tape = spill.partitions[partition];
        tapeset.write(tape, &hash.to_ne_bytes())?;
        // SAFETY: len readable bytes per the fetch above.
        tapeset.write(tape, unsafe { core::slice::from_raw_parts(ptr, len) })?;
    }
    tmp_ctx.reset();
    Ok(())
}

// hashagg_spill_finish (nodeAgg.c): one HashAggBatchGs per non-empty
// partition, tagged with the set it belongs to.
fn spill_finish_gs<'mcx>(
    h: &mut HashSetsState<'mcx>,
    setno: usize,
    spill: crate::HashAggSpill<'mcx>,
    batches_used: &mut i32,
) -> PgResult<()> {
    let used_bits = (32 - spill.shift) as u32;
    let tapeset = h.tapeset.as_mut().expect("spill has a tapeset");
    for i in 0..spill.npartitions {
        if spill.ntuples[i] == 0 {
            continue;
        }
        let cardinality = spill.hll_card[i].estimate();
        tapeset.rewind_for_read(spill.partitions[i], crate::HASHAGG_READ_BUFFER_SIZE as usize)?;
        h.hash_batches.push(HashAggBatchGs {
            setno,
            input_tape: spill.partitions[i],
            used_bits,
            input_card: cardinality,
        });
        *batches_used += 1;
    }
    Ok(())
}

// hashagg_finish_initial_spills (nodeAgg.c): turn every set's initial-pass
// spill partitions into batches once the outer plan is exhausted.
fn hashagg_finish_initial_spills_gs<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let id = node.plan.plan.plan_node_id;
    let gs = node.gsets.as_mut().unwrap();
    let h = gs.hash.as_mut().unwrap();
    let mut total_npartitions = 0usize;
    let numsets = h.perhash.len();
    let mut batches_used = 0i32;
    for setno in 0..numsets {
        if let Some(spill) = h.perhash[setno].spill.take() {
            total_npartitions += spill.npartitions;
            spill_finish_gs(h, setno, spill, &mut batches_used)?;
        }
    }
    if batches_used > 0 {
        let ai = crate::agg_instrumentation(estate, id);
        ai.hash_batches_used += batches_used;
    }
    update_hash_metrics(node, estate, false, total_npartitions);
    node.gsets.as_mut().unwrap().hash.as_mut().unwrap().spill_mode = false;
    Ok(())
}

// initialize_hash_entry (nodeAgg.c), grouping-sets form: count the new group
// node-wide, maybe enter spill mode, then seed the entry's pergroup array.
fn initialize_hash_entry_gs<'mcx>(
    h: &mut HashSetsState<'mcx>,
    setno: usize,
    ix: u32,
    trans_init: &[::datum::NullableDatum],
    trans_typ: &[crate::TransTyp],
    table_mcx: Mcx<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    h.hash_ngroups_current += 1;
    hash_agg_check_limits_gs(h, table_mcx, mcx)?;
    if trans_init.is_empty() {
        return Ok(());
    }
    let ph = &mut h.perhash[setno];
    let pergroup = ph
        .hashtable
        .entry_additional(ix)
        .expect("numtrans > 0 tables carry additional space")
        .cast::<AggPerGroup>();
    for (transno, init) in trans_init.iter().enumerate() {
        let typ = trans_typ[transno];
        let value = if !init.isnull && !typ.byval {
            // SAFETY: node-lifetime initval datum copied into the table
            // context (C's hashcontext datumCopy).
            unsafe { ::execexpr::agg_datum_copy(table_mcx, init.value, typ.len)? }
        } else {
            init.value
        };
        // SAFETY: the entry's additional block holds numtrans AggPerGroup
        // slots (execgrouping contract).
        unsafe {
            pergroup.as_ptr().add(transno).write(AggPerGroup {
                trans_value: value,
                trans_value_is_null: init.isnull,
                no_trans_value: init.isnull,
            });
        }
    }
    // SAFETY: once-allocated cell the trans steps read.
    unsafe { ph.cell.write(pergroup) };
    Ok(())
}

// lookup_hash_entries (nodeAgg.c): one create-or-find per hash set. A miss
// (spill mode already active, or this set just crossed its limit) spills the
// tuple to that set's tape instead of creating a group (C: pergroup[setno] =
// NULL). Divergence: C's combined evaltrans null-checks pergroup per set
// inline; our compiled indirect trans steps don't, so each set advances
// through its own dedicated program (`ph.refill_trans`) immediately here,
// run only for sets that got a live entry this row.
fn lookup_hash_entries<'mcx>(
    hash: &mut HashSetsState<'mcx>,
    trans_init: &[::datum::NullableDatum],
    trans_typ: &[crate::TransTyp],
    agg_node: NonNull<::types_fmgr::AggStateNode>,
    input_slot: &mut SlotData<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    // SAFETY: read of the once-allocated node; no &mut is live to it.
    let table_mcx = unsafe { agg_node.as_ref() }.aggcontext();
    let numsets = hash.perhash.len();
    for setno in 0..numsets {
        let ph = &mut hash.perhash[setno];
        exectuples::slot_getsomeattrs(input_slot, ph.largest_grp_col_idx);
        exectuples::exec_clear_tuple(&mut ph.hashslot, mcx);
        {
            let src = input_slot.base();
            let dst = ph.hashslot.base_mut();
            for (i, &attno) in ph.hash_grp_col_idx_input.iter().enumerate() {
                let v = (attno - 1) as usize;
                dst.tts_values[i] = src.tts_values[v];
                dst.tts_isnull[i] = src.tts_isnull[v];
            }
        }
        exectuples::exec_store_virtual_tuple(&mut ph.hashslot);

        let hashval = ph.hashtable.hash_slot(&mut ph.hashslot)?;
        let use_table = !hash.spill_mode;
        let ph = &mut hash.perhash[setno];
        let (ix, isnew) =
            ph.hashtable.lookup(&mut ph.hashslot, hashval, use_table.then_some(table_mcx), mcx)?;
        let Some(ix) = ix else {
            spill_tuple_gs(hash, setno, 0, Some(input_slot), hashval, mcx)?;
            continue;
        };
        if isnew {
            initialize_hash_entry_gs(hash, setno, ix, trans_init, trans_typ, table_mcx, mcx)?;
        } else if !trans_init.is_empty() {
            let ph = &mut hash.perhash[setno];
            let pergroup = ph
                .hashtable
                .entry_additional(ix)
                .expect("numtrans > 0 tables carry additional space")
                .cast::<AggPerGroup>();
            // SAFETY: once-allocated cell the trans steps read.
            unsafe { ph.cell.write(pergroup) };
        }
        let ph = &mut hash.perhash[setno];
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(input_slot) };
        exec_eval_expr(&mut ph.refill_trans, &mut slots)?;
    }
    Ok(())
}

// agg_refill_hash_table (nodeAgg.c), grouping-sets form: false = no more
// spilled batches anywhere. A batch only ever touches one set's table/cell;
// the batch's own indirect program (`ph.refill_trans`) advances it row by
// row, so no other set's cell needs to be valid during this pass.
fn agg_refill_hash_table_gs<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let batch = {
        let gs = node.gsets.as_mut().unwrap();
        let h = gs.hash.as_mut().unwrap();
        let Some(batch) = h.hash_batches.pop() else {
            return Ok(false);
        };
        let (mem_limit, ngroups_limit, _) =
            crate::hash_agg_set_limits(h.hashentrysize, batch.input_card, batch.used_bits);
        h.hash_mem_limit = mem_limit;
        h.hash_ngroups_limit = ngroups_limit;
        for ph in h.perhash.iter_mut() {
            ph.hashtable.reset();
            // Every set's table (not just this batch's) is wiped here (C:
            // ResetTupleHashTable for setno in 0..num_hashes) — a stale
            // cursor from a prior fully-drained pass must not survive it.
            ph.hashiter = 0;
        }
        h.hash_ngroups_current = 0;
        debug_assert!(h.perhash[batch.setno].spill.is_none());
        batch
    };
    // SAFETY: sole access path to the node during the reset (C's
    // ReScanExprContext(hashcontext)+MemoryContextReset(hash_tablecxt), one
    // shared aggcontext here).
    unsafe { node.agg_node.as_mut() }.reset();
    crate::hashagg_release_retained("refill_batch");

    let setno = batch.setno;
    loop {
        let advance = {
            let AggStateData { gsets, trans_init, trans_typ, agg_node, .. } = node;
            let gs = &mut **gsets.as_mut().unwrap();
            let h = &mut *gs.hash.as_mut().unwrap();
            let HashSetsState { perhash, tapeset, rslot, read_buf, spill_mode, .. } = h;
            let tapeset = tapeset.as_mut().expect("batches imply a tapeset");
            let Some(hash) = crate::hashagg_batch_read(tapeset, batch.input_tape, read_buf)? else {
                break;
            };
            let tup = NonNull::new(read_buf.as_mut_ptr().cast::<::types_tuple::htup::MinimalTupleData>())
                .expect("read_buf is non-null");
            // SAFETY: the image stays live in read_buf until the next read.
            unsafe { exectuples::exec_store_minimal_tuple_ptr(rslot, mcx, tup) };
            let ph = &mut perhash[setno];
            exectuples::slot_getsomeattrs(rslot, ph.largest_grp_col_idx);
            exectuples::exec_clear_tuple(&mut ph.hashslot, mcx);
            {
                let src = rslot.base();
                let dst = ph.hashslot.base_mut();
                for (i, &attno) in ph.hash_grp_col_idx_input.iter().enumerate() {
                    let v = (attno - 1) as usize;
                    dst.tts_values[i] = src.tts_values[v];
                    dst.tts_isnull[i] = src.tts_isnull[v];
                }
            }
            exectuples::exec_store_virtual_tuple(&mut ph.hashslot);
            // SAFETY: read of the once-allocated node; no &mut is live to it.
            let table_mcx = unsafe { agg_node.as_ref() }.aggcontext();
            let use_table = !*spill_mode;
            let (ix, isnew) = ph.hashtable.lookup(
                &mut ph.hashslot,
                hash,
                use_table.then_some(table_mcx),
                mcx,
            )?;
            match ix {
                Some(ix) => {
                    if isnew {
                        initialize_hash_entry_gs(h, setno, ix, trans_init, trans_typ, table_mcx, mcx)?;
                    } else if !trans_init.is_empty() {
                        let ph = &mut h.perhash[setno];
                        let pergroup = ph
                            .hashtable
                            .entry_additional(ix)
                            .expect("numtrans > 0 tables carry additional space")
                            .cast::<AggPerGroup>();
                        // SAFETY: once-allocated cell the trans steps read.
                        unsafe { ph.cell.write(pergroup) };
                    }
                    true
                }
                None => {
                    spill_tuple_gs(h, setno, batch.used_bits, None, hash, mcx)?;
                    false
                }
            }
        };
        if advance {
            let AggStateData { gsets, .. } = node;
            let gs = &mut **gsets.as_mut().unwrap();
            let h = &mut *gs.hash.as_mut().unwrap();
            let HashSetsState { perhash, rslot, .. } = h;
            let ph = &mut perhash[setno];
            let mut slots = EvalSlots { scan: None, inner: None, outer: Some(rslot) };
            exec_eval_expr(&mut ph.refill_trans, &mut slots)?;
        }
        estate.reset_expr_context(node.tmpcontext);
    }

    let id = node.plan.plan.plan_node_id;
    let gs = node.gsets.as_mut().unwrap();
    let h = gs.hash.as_mut().unwrap();
    h.tapeset.as_mut().unwrap().close_tape(batch.input_tape);
    let spilled = h.perhash[setno].spill.take();
    let npartitions = spilled.as_ref().map_or(0, |s| s.npartitions);
    if let Some(spill) = spilled {
        let mut batches_used = 0i32;
        spill_finish_gs(h, setno, spill, &mut batches_used)?;
        if batches_used > 0 {
            let ai = crate::agg_instrumentation(estate, id);
            ai.hash_batches_used += batches_used;
        }
    }
    update_hash_metrics(node, estate, true, npartitions);
    let gs = node.gsets.as_mut().unwrap();
    let h = gs.hash.as_mut().unwrap();
    h.spill_mode = false;
    h.current_set = setno;
    h.perhash[setno].hashiter = 0;
    Ok(true)
}

// agg_fill_hash_table (nodeAgg.c), grouping-sets AGG_HASHED form.
pub(crate) fn agg_fill_hash_table<'mcx, F>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    fetch_outer: &mut F,
) -> PgResult<()>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    let mcx = estate.es_query_cxt;
    while let Some(outer_id) = fetch_outer(estate)? {
        estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
        {
            let AggStateData { gsets, trans_init, trans_typ, agg_node, .. } = node;
            let gs = gsets.as_mut().unwrap();
            let h = gs.hash.as_mut().expect("hashed grouping sets");
            let outer_slot = estate.slot_mut(outer_id);
            // lookup_hash_entries runs each hit set's own trans program
            // inline; spilled sets simply don't advance this row.
            lookup_hash_entries(h, trans_init, trans_typ, *agg_node, outer_slot, mcx)?;
        }
        estate.reset_expr_context(node.tmpcontext);
    }
    hashagg_finish_initial_spills_gs(node, estate)?;
    let gs = node.gsets.as_mut().unwrap();
    let h = gs.hash.as_mut().unwrap();
    h.table_filled = true;
    h.current_set = 0;
    for ph in h.perhash.iter_mut() {
        ph.hashiter = 0;
    }
    Ok(())
}

// hash_agg_update_metrics (nodeAgg.c): buffer_mem accounts for the shared
// tapeset's read/write buffers (npartitions is the just-finished pass's
// count, from_tape marks a refill-batch pass).
fn update_hash_metrics(
    node: &mut AggStateData<'_>,
    estate: &mut EStateData<'_>,
    from_tape: bool,
    npartitions: usize,
) {
    let gs = node.gsets.as_mut().unwrap();
    let h = gs.hash.as_mut().unwrap();
    // SAFETY: read of the once-allocated node; no &mut is live to it.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let meta: usize = h.perhash.iter().map(|ph| ph.hashtable.meta_mem()).sum();
    let hashkey_mem = aggctx.context().subtree_used();
    let buffer_mem = npartitions * crate::HASHAGG_WRITE_BUFFER_SIZE as usize
        + if from_tape { crate::HASHAGG_READ_BUFFER_SIZE as usize } else { 0 };
    let total = (meta + hashkey_mem + buffer_mem) as u64;
    let id = node.plan.plan.plan_node_id;
    let ai = crate::agg_instrumentation(estate, id);
    ai.hash_mem_peak = ai.hash_mem_peak.max(total);
    if let Some(ts) = node.gsets.as_ref().unwrap().hash.as_ref().unwrap().tapeset.as_ref() {
        let disk_used = ts.blocks() as u64 * 8;
        ai.hash_disk_used = ai.hash_disk_used.max(disk_used);
    }
    let h = node.gsets.as_mut().unwrap().hash.as_mut().unwrap();
    if h.hash_ngroups_current > 0 {
        h.hashentrysize = 16.0 + hashkey_mem as f64 / h.hash_ngroups_current as f64;
    }
}

// agg_retrieve_hash_table(_in_memory) (nodeAgg.c), multi-set form: walk each
// set's table in turn; the representative tuple rebuilt into an outer-format
// slot with the unstored columns NULL.
pub(crate) fn agg_retrieve_hash_table<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    loop {
        estate.reset_expr_context(node.ps_ExprContext);
        // After exhausting the in-memory tables, try refilling from a
        // spilled batch (agg_retrieve_hash_table's outer retry in C); only
        // agg_done when both the tables and the spill batches are drained.
        let (current_set, ix) = 'find: loop {
            {
                let gs = &mut **node.gsets.as_mut().unwrap();
                let h = gs.hash.as_mut().expect("hashed grouping sets");
                let HashSetsState { perhash, current_set, .. } = h;
                loop {
                    let ph = &mut perhash[*current_set];
                    if let Some(ix) = ph.hashtable.iterate(&mut ph.hashiter) {
                        break 'find (*current_set, ix);
                    }
                    if *current_set + 1 < perhash.len() {
                        *current_set += 1;
                        perhash[*current_set].hashiter = 0;
                    } else {
                        break;
                    }
                }
            }
            if !agg_refill_hash_table_gs(node, estate)? {
                node.agg_done = true;
                return Ok(None);
            }
        };
        let pergroup = {
            let AggStateData { gsets, .. } = node;
            let gs = &mut **gsets.as_mut().unwrap();
            let GroupingSetsState { hash, grouped_cols_cell, .. } = gs;
            let h = hash.as_mut().expect("hashed grouping sets");
            let HashSetsState { perhash, hash_first_slot, .. } = h;
            let ph = &mut perhash[current_set];
            let tup = ph.hashtable.entry_tuple(ix);
            // SAFETY: entry images live in the node's table context for the
            // table's lifetime.
            unsafe { exectuples::exec_store_minimal_tuple_ptr(&mut ph.retrieve_slot, mcx, tup) };
            exectuples::slot_getallattrs(&mut ph.retrieve_slot);

            exectuples::exec_store_all_null_tuple(hash_first_slot, mcx);
            {
                let src = ph.retrieve_slot.base();
                let dst = hash_first_slot.base_mut();
                for (i, &attno) in ph.hash_grp_col_idx_input.iter().enumerate() {
                    let v = (attno - 1) as usize;
                    dst.tts_values[v] = src.tts_values[i];
                    dst.tts_isnull[v] = src.tts_isnull[i];
                }
            }
            // Publish the set's grouped_cols for EEOP_GROUPING_FUNC; the
            // projection nulling is vacuous here (unstored cols are NULL).
            // SAFETY: once-allocated cell; grouped_cols is stable after init.
            unsafe {
                grouped_cols_cell.write(GroupedColsCell {
                    ptr: ph.grouped_cols.as_ptr(),
                    len: ph.grouped_cols.len(),
                })
            };
            ph.hashtable.entry_additional(ix).map_or(NonNull::dangling(), |p| p.cast())
        };
        crate::finalize_aggregates(node, estate, pergroup)?;

        // Qual/tlist SubPlans need the suspension drivers (node-ecxt), same
        // as the plain and sorted retrieval paths.
        if node.proj.has_subplan() || node.qual.as_deref().is_some_and(|q| q.has_subplan()) {
            let ecxt = node.ps_ExprContext;
            let result = node.ps_ResultTupleSlot;
            let instr_idx = node.instr_idx;
            let AggStateData { gsets, qual, proj, .. } = node;
            let h = gsets.as_mut().unwrap().hash.as_mut().unwrap();
            if !::executils::exec_qual_with_subplans_outer(
                qual.as_deref_mut(),
                &mut h.hash_first_slot,
                estate,
                ecxt,
            )? {
                estate.instr_count_filtered1(instr_idx);
                continue;
            }
            ::executils::exec_project_with_subplans_outer(
                proj,
                &mut h.hash_first_slot,
                estate,
                ecxt,
                result,
            )?;
            return Ok(Some(result));
        }
        {
            let AggStateData { gsets, qual, .. } = node;
            let h = gsets.as_mut().unwrap().hash.as_mut().unwrap();
            let mut slots =
                EvalSlots { scan: None, inner: None, outer: Some(&mut h.hash_first_slot) };
            if !exec_qual(qual.as_deref_mut(), &mut slots)? {
                estate.instr_count_filtered1(node.instr_idx);
                continue;
            }
        }
        let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
        let h = node.gsets.as_mut().unwrap().hash.as_mut().unwrap();
        let mut slots =
            EvalSlots { scan: None, inner: None, outer: Some(&mut h.hash_first_slot) };
        exec_project(&mut node.proj, &mut slots, result_slot, mcx)?;
        return Ok(Some(node.ps_ResultTupleSlot));
    }
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
    for setno in 0..num_reset {
        // Grouping sets never admit set-mode (init gates on
        // !has_grouping_sets), so force is always false here.
        crate::restart_pertrans_sortstates(&mut node.pertrans_sort, setno, false)
            .expect("sortstate restart");
    }
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

        let mut switch_to_hash = false;
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
                if gs.mixed {
                    // Sorted phases done, hash tables full: output those.
                    gs.in_hash_mode = true;
                    gs.sort_in = None;
                    gs.sort_out = None;
                    let h = gs.hash.as_mut().expect("mixed grouping sets");
                    h.table_filled = true;
                    h.current_set = 0;
                    for ph in h.perhash.iter_mut() {
                        ph.hashiter = 0;
                    }
                    switch_to_hash = true;
                } else {
                    node.agg_done = true;
                    break;
                }
            }
        }
        if switch_to_hash {
            update_hash_metrics(node, estate, false, 0);
            return agg_retrieve_hash_table(node, estate);
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
        crate::process_ordered_aggregates_set(node, estate, current_set, pergroup)?;
        crate::finalize_aggregates(node, estate, pergroup)?;

        // Qual/tlist SubPlans need the suspension drivers (node-ecxt), same
        // as the plain and sorted retrieval paths.
        if node.proj.has_subplan() || node.qual.as_deref().is_some_and(|q| q.has_subplan()) {
            let ecxt = node.ps_ExprContext;
            let result = node.ps_ResultTupleSlot;
            let instr_idx = node.instr_idx;
            let AggStateData { gsets, qual, proj, .. } = node;
            let gs = gsets.as_mut().unwrap();
            if !::executils::exec_qual_with_subplans_outer(
                qual.as_deref_mut(),
                &mut gs.first_slot,
                estate,
                ecxt,
            )? {
                estate.instr_count_filtered1(instr_idx);
                continue;
            }
            ::executils::exec_project_with_subplans_outer(
                proj,
                &mut gs.first_slot,
                estate,
                ecxt,
                result,
            )?;
            return Ok(Some(result));
        }
        {
            let AggStateData { gsets, qual, .. } = node;
            let gs = gsets.as_mut().unwrap();
            let mut slots =
                EvalSlots { scan: None, inner: None, outer: Some(&mut gs.first_slot) };
            if !exec_qual(qual.as_deref_mut(), &mut slots)? {
                estate.instr_count_filtered1(node.instr_idx);
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
        let AggStateData { gsets, trans_init, trans_typ, agg_node, .. } = node;
        let gs = &mut **gsets.as_mut().unwrap();
        let GroupingSetsState { phases, first_slot, current_phase, hash, mixed, .. } = gs;
        // C: phase 1 (and only phase 1) of a mixed agg updates the hash
        // tables in the same advance.
        if *mixed && *current_phase == 0 {
            let h = hash.as_mut().expect("mixed grouping sets");
            lookup_hash_entries(h, trans_init, trans_typ, *agg_node, first_slot, mcx)?;
        }
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(first_slot) };
        exec_eval_expr(&mut phases[*current_phase].evaltrans, &mut slots)?;
    }
    if !node.pertrans_sort.is_empty() {
        let gs = node.gsets.as_ref().unwrap();
        let nsets = gs.phases[gs.current_phase].numsets.max(1);
        crate::collect_ordered_input(node, estate, nsets)?;
    }
    estate.reset_expr_context(node.tmpcontext);
    loop {
        let fetched = match fetch_input_tuple(node.gsets.as_mut().unwrap(), estate, fetch_outer)?
        {
            Some(f) => f,
            None => {
                let finish_spills = {
                    let gs = node.gsets.as_ref().unwrap();
                    gs.mixed && gs.current_phase == 0
                };
                // C agg_retrieve_direct (nodeAgg.c:2551): outer plan
                // exhausted during the hashing phase of a mixed agg — the
                // per-set initial spills must become batches HERE, or every
                // spilled tuple is silently dropped.
                if finish_spills {
                    hashagg_finish_initial_spills_gs(node, estate)?;
                }
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
            let AggStateData { gsets, trans_init, trans_typ, agg_node, .. } = node;
            let gs = &mut **gsets.as_mut().unwrap();
            match fetched {
                Fetched::Outer(id) => {
                    let outer_slot = estate.slot_mut(id);
                    let GroupingSetsState { phases, current_phase, hash, mixed, .. } = gs;
                    if *mixed && *current_phase == 0 {
                        let h = hash.as_mut().expect("mixed grouping sets");
                        lookup_hash_entries(h, trans_init, trans_typ, *agg_node, outer_slot, mcx)?;
                    }
                    let mut slots =
                        EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
                    exec_eval_expr(&mut phases[*current_phase].evaltrans, &mut slots)?;
                }
                Fetched::Sorted => {
                    let GroupingSetsState { phases, sort_slot, current_phase, mixed, .. } = gs;
                    // Phase 1 reads the outer plan directly; sorted input
                    // only feeds later, non-hashing phases.
                    debug_assert!(!*mixed || *current_phase > 0);
                    let mut slots =
                        EvalSlots { scan: None, inner: None, outer: Some(sort_slot) };
                    exec_eval_expr(&mut phases[*current_phase].evaltrans, &mut slots)?;
                }
            }
        }
        if !node.pertrans_sort.is_empty() {
            let gs = node.gsets.as_ref().unwrap();
            let nsets = gs.phases[gs.current_phase].numsets.max(1);
            crate::collect_ordered_input(node, estate, nsets)?;
        }
        estate.reset_expr_context(node.tmpcontext);
    }
}

// ExecAgg (nodeAgg.c) dispatch, grouping-sets form: hashed output mode is
// C's phase 0 (pure AGG_HASHED from the start, AGG_MIXED after the sorted
// phases hand over).
pub(crate) fn exec_agg_gsets<'mcx, F>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    fetch_outer: &mut F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    let (in_hash, filled) = {
        let gs = node.gsets.as_ref().unwrap();
        (gs.in_hash_mode, gs.hash.as_ref().is_some_and(|h| h.table_filled))
    };
    if in_hash {
        if !filled {
            agg_fill_hash_table(node, estate, fetch_outer)?;
        }
        return agg_retrieve_hash_table(node, estate);
    }
    agg_retrieve_grouping_sets(node, estate, fetch_outer)
}

// ExecReScanAgg's pure-hashed reuse arm: filled tables and unchanged input
// only need their iterators reset. Returns false when a full reset is due.
pub(crate) fn rescan_hash_reuse(gs: &mut GroupingSetsState<'_>) -> bool {
    if !gs.phases.is_empty() {
        return false;
    }
    let Some(h) = gs.hash.as_mut() else { return false };
    // C ExecReScanAgg: the no-reset fast path requires the hash side never
    // spilled (a spilled tapeset/batches state doesn't survive a rescan).
    if h.ever_spilled {
        return false;
    }
    if h.table_filled {
        h.current_set = 0;
        for ph in h.perhash.iter_mut() {
            ph.hashiter = 0;
        }
    }
    true
}

pub(crate) fn rescan_grouping_sets(gs: &mut GroupingSetsState<'_>) -> PgResult<()> {
    gs.input_done = false;
    gs.projected_set = -1;
    gs.have_pending = false;
    gs.first_stored = false;
    gs.sort_in = None;
    gs.sort_out = None;
    if let Some(h) = gs.hash.as_mut() {
        for ph in h.perhash.iter_mut() {
            ph.hashtable.reset();
            ph.hashiter = 0;
            ph.spill = None;
        }
        h.hash_ngroups_current = 0;
        h.table_filled = false;
        h.current_set = 0;
        h.spill_mode = false;
        h.hash_batches.clear();
        // hashagg_reset_spill_state (nodeAgg.c): a spilled tapeset doesn't
        // survive a rescan; the next spill (if any) opens a fresh one.
        if let Some(ts) = h.tapeset.take() {
            ts.close()?;
        }
        h.ever_spilled = false;
    }
    gs.in_hash_mode = gs.phases.is_empty();
    if gs.phases.is_empty() {
        return Ok(());
    }
    initialize_phase(gs, 0)
}

// hashagg_reset_spill_state (nodeAgg.c), grouping-sets form: called once at
// ExecEndAgg to close the shared tapeset (rescan already tears down spill
// state on chgParam; this covers the no-rescan end-of-query path).
pub(crate) fn end_grouping_sets(gs: &mut GroupingSetsState<'_>) {
    let Some(h) = gs.hash.as_mut() else { return };
    for ph in h.perhash.iter_mut() {
        ph.spill = None;
    }
    h.hash_batches.clear();
    if let Some(ts) = h.tapeset.take() {
        ts.close().expect("hashagg tapeset close");
    }
}
