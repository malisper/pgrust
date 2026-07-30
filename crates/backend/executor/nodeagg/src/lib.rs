// nodeAgg.c, AGG_PLAIN/AGG_SORTED/AGG_HASHED single-grouping-set slice: byval
// and by-ref transtypes (INTERNAL is a byval pointer datum; its state lives in
// the AggStateNode aggcontext the transfn reaches via fcinfo->context; by-ref
// transvalues copy into that aggcontext at C's datumCopy points), finalfn
// via resolve-once peragg carriers; transitions compile into one execexpr
// program (C's evaltrans). AGG_HASHED spills to LogicalTapeSet batches at the
// hash_mem/ngroups limits (single set; the gsets.rs hash path stays a loud
// panic). Grouping sets (all strategies) live in gsets.rs. aggsplit variants
// are loud panics.
#![allow(non_snake_case)]

use core::alloc::Layout;
use std::ptr::NonNull;
use std::rc::Rc;

use ::datum::{Datum, NullableDatum};
use ::types_fmgr::{AggStateNode, FmNodePtr, FmgrInfo, LocalFcinfo};
use ::execexpr::{
    exec_build_agg_projection_info_subplans, exec_build_agg_qual_subplans, exec_build_agg_trans,
    exec_build_agg_trans_hashed, exec_eval_expr, exec_project, exec_qual, AggBind,
    AggOrderedSpec, AggPerGroup, AggTransSpec, EvalSlots, ExprState,
};
use ::tuplesort::{Tuplesort, TUPLESORT_NONE};
use ::execgrouping::TupleHashTable;
use ::hyperloglog::HyperLogLog32;
use ::sort_storage::{LogicalTapeSet, TapeIdx};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{vec_with_capacity_in, Allocator, MemoryContext, PgBox, PgVec};
use ::types_core::catalog::PROCEDURE_RELATION_ID;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Agg;
use ::types_nodes::primnodes::{Aggref, AGGKIND_NORMAL};
use ::types_nodes::NodeTag;
use ::types_pathnodes::{
    AGGSPLITOP_COMBINE, AGGSPLITOP_DESERIALIZE, AGGSPLITOP_SERIALIZE, AGGSPLITOP_SKIPFINAL,
    AGGSPLIT_FINAL_DESERIAL, AGGSPLIT_INITIAL_SERIAL, AGGSPLIT_SIMPLE, AGG_HASHED, AGG_MIXED,
    AGG_PLAIN, AGG_SORTED,
};
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::htup::MinimalTupleData;
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

mod codedgroup;
mod compact;
mod distinctset;
mod gsets;
mod hashgrouped;
pub mod merge;
pub mod pardistinct;
pub mod plainpd;
pub mod sink;
pub mod runtime_partial;
pub mod sortedsink;
pub mod spankey;

pub use compact::{
    agg_hash_compact_armed, agg_hash_compact_backstop, agg_hash_compact_batch,
    compact_batch_install_enabled,
    agg_emit_mark_drained, batch_emit_row, batch_emit_scan_block, batch_emit_set_block,
    topk_finalize_select, BATCH_EMIT_BLOCK,
    agg_hash_compact_batch_mk1, agg_hash_compact_batch_mk2, agg_hash_compact_disarm,
    agg_hash_compact_intern, agg_hash_compact_mk_admit, agg_hash_compact_mk_admit1,
    agg_hash_compact_over_limits, agg_hash_compact_probe_coded,
    agg_hash_compact_probe_text_direct, agg_hash_compact_text_direct, text_direct_enabled,
    agg_hash_compact_mk_admit_multi, agg_hash_compact_ngroups, agg_hash_compact_try_arm_mk_multi,
    agg_hash_compact_mk_shape, agg_hash_compact_reduced_admissible,
    agg_hash_compact_sink_admissible, agg_hash_compact_sink_would_refuse,
    agg_hash_compact_try_arm, agg_hash_compact_try_arm_mk, agg_hash_compact_try_arm_mk1,
    agg_hash_compact_try_arm_reduced,
    agg_hash_spill_unlikely, mk_keys2_lane, mk_numeric_datum_bits, mk_numeric_i64_bits,
    mk_numeric_key_bits, mk_numeric_mant_abs_max, CompactArm, MkComp, MkCompKind, MkShape,
    RedDerived, RedOp, RedShape,
};
pub use codedgroup::{
    agg_codedgroup_accept_batch, agg_codedgroup_admissible, agg_codedgroup_begin,
    agg_codedgroup_economical, agg_codedgroup_emit_next, agg_codedgroup_emitting,
    agg_codedgroup_finish_build, agg_codedgroup_key_arg_atts, agg_codedgroup_mode_global,
    agg_codedgroup_next_replay, agg_codedgroup_reset, CgAccept,
};
pub use hashgrouped::{
    agg_hashgroup_accept, agg_hashgroup_accept_batch_row, agg_hashgroup_accept_batch_span,
    agg_hashgroup_admissible, agg_hashgroup_adopt_merged, agg_hashgroup_arm_fold,
    agg_hashgroup_batch_shape, agg_hashgroup_begin, agg_hashgroup_economical,
    agg_hashgroup_economical_sink, agg_hashgroup_emit_next, agg_hashgroup_emitting,
    agg_hashgroup_finish_build, agg_hashgroup_next_rep, agg_hashgroup_reset,
    agg_hashgroup_residual_active, agg_hashgroup_set_residual, agg_hashgroup_state_active,
    agg_hashgroup_text_key_count, HashGroupOrderKey, HgBatchRow, HgBatchShape, HgSpanStop,
};
pub use ::execgrouping::GroupKeyKind;

const ACL_EXECUTE: u64 = 1 << 7;
const ACLCHECK_OK: i32 = 0;

pub struct AggStateData<'mcx> {
    pub plan: &'mcx Agg<'mcx>,
    pub ps_ExprContext: EcxtId,
    pub tmpcontext: EcxtId,
    // C's curaggcontext, in the FmNode the transfn fcinfos carry; raw arena
    // cell so the pointer survives self moving (drop: make_agg_state_node).
    agg_node: NonNull<AggStateNode>,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    proj: PgBox<'mcx, ExprState<'mcx>>,
    evaltrans: Option<PgBox<'mcx, ExprState<'mcx>>>,
    peragg: PgVec<'mcx, PerAggData<'mcx>>,
    trans_init: PgVec<'mcx, NullableDatum>,
    trans_typ: PgVec<'mcx, TransTyp>,
    // Owners of once-allocated arrays; all element access goes through the
    // *_base pointers so the step-held pointers stay valid (steps.rs note).
    _pergroup: PgVec<'mcx, AggPerGroup>,
    pergroup_base: NonNull<AggPerGroup>,
    agg_values_base: NonNull<Datum>,
    agg_nulls_base: NonNull<bool>,
    agg_done: bool,
    skip_final: bool,
    numtrans: usize,
    // avgpack: bit per transno of the AvgInt8 class (`_int8` {count,sum}
    // transarray — avg(int2/int4)), 0 when the kill switch is off or any
    // such transno is >= 64. Computed once at node build; a SINK worker
    // build's compact arm adopts it as the table's packed-representation
    // mask (compact.rs `CompactHash::avgpack_mask`), and the leader's
    // combine/emit resolution reads the same value — one deterministic
    // predicate on both sides (the F1 leader/worker-verdict law).
    pub(crate) avgpack_shape_mask: u64,
    perhash: Option<PerHashData<'mcx>>,
    merge: Option<merge::FinalizeMerge<'mcx>>,
    persort: Option<PerSortData<'mcx>>,
    gsets: Option<PgBox<'mcx, gsets::GroupingSetsState<'mcx>>>,
    pertrans_sort: PgVec<'mcx, PerTransSortData<'mcx>>,
    // Lane-v2 skip-sort arming (`agg_force_distinct_set`): when true,
    // set-capable PRESORTED entries also run set-mode (collect inserts,
    // finalize replays) — the lane feeds UNSORTED input, so the adjacent-
    // dedup contract is gone and the exact set replaces it. Sticky once
    // armed: value-safe on any later per-tuple fallback too (set dedup over
    // sorted input yields the same distinct multiset; admitted transitions
    // are replay-order-insensitive). False = presorted entries keep C's
    // per-row dedup bit-exactly.
    force_distinct_set: bool,
    // Every grouping-key equality operator is REPRESENTATIONAL (equal keys
    // are byte-equal: int2/4/8/bool eq, texteq under a deterministic
    // collation — recorded at init, false when never probed). The narrow-
    // sort arm's second admission leg: with a narrowed sort a DIFFERENT row
    // becomes each group's first tuple, and the projected group-key
    // representative must not be able to differ (numeric's 1.0/1.00 class
    // equality would leak the choice).
    group_eq_representational: bool,
    // Every transition function of this node is order-insensitive-EXACT
    // (`order_insensitive_exact_transfn` whitelist, recorded at init): same
    // input multiset in ANY order produces byte-identical transvalues. The
    // grouped narrow-sort arm's admission leg (nothing in the node observes
    // intra-group row order once presorted dedup is replaced by exact sets).
    trans_order_insensitive: bool,
    qual: Option<PgBox<'mcx, ExprState<'mcx>>>,
    // Lane-v2 hash-agg breaker fold state (None = lane off / nothing admits).
    lanefold: Option<LaneFold<'mcx>>,
    // Lane-v2 metadata-answer plan (pgrcolumnar footer answers): Some iff EVERY
    // transition is footer-answerable (lanefold::classify_meta) on an
    // AGG_PLAIN node the lane fold gate admitted. Consumed by the execmain
    // metaagg arm via agg_meta_plan/exec_agg_meta.
    meta_aggs: Option<PgVec<'mcx, ::lanefold::MetaTrans>>,
    // InstrCountFiltered1 target (HAVING rejections); set by instrument_node.
    pub instr_idx: Option<u32>,
    // Sink::combine already ran for this build (spills finished, handoff
    // installed): agg_hash_build_finish must not repeat either (a double
    // handoff install would double-count the worker's groups). Cleared by
    // finish for the next build (rescan).
    hash_build_combined: bool,
    // Lane-v2 hash-grouped exact-DISTINCT arm (hashgrouped.rs): Some while
    // the arm holds group state (building / emitting / degraded residual).
    // While it exists, group-boundary aggcontext resets are SKIPPED — the
    // table's by-ref transvalues live in aggcontext (module doc).
    hashgroup: Option<Box<hashgrouped::HashGroupedState<'mcx>>>,
    // Lane-v2 dict-code batched exact-DISTINCT grouping (codedgroup.rs, the
    // near-unique text-key shape class): Some while the arm holds state
    // (building or emitting). Plain Rust memory only — no aggcontext
    // residue, no interplay with the group-boundary resets (module doc).
    codedgroup: Option<Box<codedgroup::CodedGroupState<'mcx>>>,
    // M2 aggregation sink (sink.rs): the LEADER's adopted parallel emit
    // state — published per-bucket identity-projected rows drained one per
    // call. Plain Rust memory; no aggcontext residue.
    sink_emit: Option<Box<sink::SinkEmitState>>,
    // Runtime distinct sink PAREMIT (pardistinct.rs section doc): the
    // LEADER's adopted per-partition ordered emit buckets, drained one row
    // per call through the cross-bucket merge. Plain Rust memory; no
    // aggcontext residue, no interplay with the group-boundary resets.
    pdemit: Option<Box<pardistinct::PdParemitState>>,
    // sorted-arm lane: the ordered-grouped runtime sink's adopted emit state
    // (sortedsink.rs) — stitched ordered segments drained one row per call.
    // Plain Rust memory; no aggcontext residue.
    sorted_sink_emit: Option<Box<sortedsink::SortedSinkEmitState>>,
}

// Lane-v2 fold state for the execmain lanev2 hash-agg breaker: the lanefold
// plan classified over this node's transition specs at init, plus the
// residual per-row transition program (the transitions classify refused,
// compiled with their ORIGINAL transnos so it runs beside the batched fold).
struct LaneFold<'mcx> {
    plan: ::lanefold::LanePlan<'mcx>,
    resid: Option<PgBox<'mcx, ExprState<'mcx>>>,
}

// Mirrors execmain `lanev2::enabled()` (the pgrust.lane_executor GUC's
// session backing cell); duplicated because nodeagg cannot depend on
// execmain (crate cycle) — both read the same guc_tables backing.
fn lane_v2_enabled() -> bool {
    ::guc_tables::backing::pgrust_lane_executor()
}

/// SE-GROUPONLY (night/subquery-admission): `PGRUST_LANE_V2_GROUPONLY`,
/// **DEFAULT ON** since t36 flips2 (`=0|off` is the kill switch — the
/// flipped-kill idiom; every other spelling stays ON). FLIP EVIDENCE
/// (GL-GROUPONLY-1 FLIP-RECOMMENDED, 2026-07-21, A-B-A discriminator job
/// pgrust-fast-tests-1d6836056f-1784619706-15e1, arena-strings 10M/2M
/// median-of-5): official damped geomean ON/OFF 0.720 (1.39x win on the
/// admitted grouped-subquery/DISTINCT wrapper shapes; the middle ON boot
/// beat BOTH flanking OFF boots), ordered string_agg md5 parity every leg
/// at both scales, refusal witness 20 -> 0. Letter caveats: the earlier
/// 7.2x-cliff claim restates as ~1.4x on this fixture class (the refusal
/// lands on legacy serial hash agg, not a cliff); serial-lane admission
/// only (the rig pinned pgrust.parallel_engine=legacy). Admits
/// ZERO-transition hashed aggregation
/// (grouping-only builds: bare `GROUP BY` emit under a parent consumer,
/// `SELECT DISTINCT`, the grouped-subquery inner) into the lane's staged
/// feeds via the vacuous fold plan (`lanefold::empty_plan`) — the
/// arena-strings profile's 7.2x admission cliff (`SELECT count(*) FROM
/// (SELECT url FROM t GROUP BY url) s` classify-refused at 1982ms vs the
/// aggregated twin's 274ms, SAME inner HashAggregate). OFF keeps today's
/// refusal byte-for-byte ("no lanefold plan (classify refused)" — the
/// legacy row-at-a-time TupleHashTable build). This is the serial-lane
/// half of the named "plain SELECT DISTINCT hash-shape gap" (m5-coverage
/// CbDistinctIntKeys row note); the parallel SINK stays a fail-closed
/// refusal (`agg_sink_plan_shape_ok` — no partial states to export).
fn lane_v2_grouponly_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        grouponly_spelling_on(std::env::var("PGRUST_LANE_V2_GROUPONLY").as_deref().ok())
    })
}

/// The default-ON kill spelling rule (t36 flips2), factored pure for unit
/// tests: OFF iff exactly `0` or `off`; unset and every other spelling ON.
fn grouponly_spelling_on(v: Option<&str>) -> bool {
    !matches!(v, Some("0") | Some("off"))
}

const MAX_ORDERED_TRANS_ARGS: usize = 8;

// C AggStatePerTransData's non-presorted DISTINCT/ORDER BY slice
// (build_pertrans_for_aggref): the evaltrans program parks each row's args in
// `scratch` and raises `flag`; collect_ordered_input feeds the tuplesort and
// process_ordered_aggregate_{single,multi} replay the transfn at the group
// boundary.
struct PerTransSortData<'mcx> {
    transno: usize,
    num_inputs: usize,
    num_trans_inputs: usize,
    num_distinct_cols: usize,
    // C aggpresorted DISTINCT (ExecEvalPreOrderedDistinctSingle/Multi): no
    // sortstate; each parked row is dedup-checked against the last-seen value
    // and replayed through the transfn immediately, in input order.
    presorted: bool,
    haslast: bool,
    // Single-column comparand; by-ref values retained in last_buf (C
    // datumCopy into the group aggcontext, pfree'd per replacement).
    last_single: NullableDatum,
    last_buf: PgVec<'mcx, u8>,
    input_byval: bool,
    input_typlen: i16,
    sortdesc: Rc<TupleDescData<'mcx>>,
    sort_col_idx: PgVec<'mcx, i16>,
    sort_ops: PgVec<'mcx, Oid>,
    sort_collations: PgVec<'mcx, Oid>,
    sort_nulls_first: PgVec<'mcx, bool>,
    equalfn_one: Option<FmgrInfo>,
    equalfn_multi: Option<PgBox<'mcx, ExprState<'mcx>>>,
    transfn: FmgrInfo,
    agg_collation: Oid,
    scratch: NonNull<NullableDatum>,
    flag: NonNull<bool>,
    // Lane-v2 exact-DISTINCT set hosting (distinctset.rs, pgrcolumnar-v2 plan
    // §2.3). `Some` = this entry is SET-MODE: admitted at ExecInitAgg
    // (`distinct_set_kind`, only under PGRUST_LANE_V2 and never with grouping
    // sets / combine / presorted / agg-level ORDER BY), the per-group
    // tuplesort is replaced by `dset` (insert on collect, transfn replay at
    // the group boundary), and lane drives may host the node (the value-
    // identity argument lives on `distinct_set_kind`). `dset_degraded` is a
    // PER-GROUP runtime fallback: past the work_mem budget the group's set is
    // dumped into the very tuplesort it displaced (`degrade_distinct_set`)
    // and the C sort path finishes the group — C-shaped spill conservatism
    // (charter §4) instead of a bespoke set spill.
    set_kind: Option<distinctset::DistinctKeyKind>,
    dset: Option<distinctset::DistinctSet<'mcx>>,
    dset_degraded: bool,
    // COUNT(DISTINCT x) finalize shortcut eligibility: this set-capable
    // entry's transition is exactly int8inc_any (count(x), strict, initcond
    // '0'). Replaying the deduped set through int8inc_any is one increment
    // per non-null distinct value with the at-most-one NULL strict-skipped,
    // so the set-mode finalize collapses to `transvalue += |set|` — the set
    // is the counter (`process_ordered_aggregates_set`). False keeps the
    // per-element transfn replay.
    set_count_transfn: bool,
    // The aggregate's single argument is exactly OUTER column `att`
    // (0-based) with no FILTER (recorded at init from the Aggref): the
    // per-row transition program's whole effect for this entry is "park
    // outer col att + flag", so a lane drive may feed the staged scan lane
    // directly (`agg_plain_distinct_insert_batch` requires att 0; the
    // codedgroup batch feed reads any recorded att) instead of running the
    // program. None = the arg is not a bare OUTER Var (or FILTER exists).
    direct_att: Option<u16>,
    // One sortstate per grouping set (C sortstates[maxsets]); [0] otherwise.
    sortstates: Vec<Option<Tuplesort>>,
    insert_slot: Option<SlotData<'mcx>>,
    slot1: Option<SlotData<'mcx>>,
    slot2: Option<SlotData<'mcx>>,
}

impl PerTransSortData<'_> {
    /// Whether this entry runs SET-MODE right now: a set-capable
    /// non-presorted entry always does; a set-capable presorted entry only
    /// under the lane's skip-sort arming (`force_distinct_set` — the input
    /// is unsorted then, so C's adjacent-dedup contract is unavailable).
    #[inline]
    fn set_active(&self, force: bool) -> bool {
        self.set_kind.is_some() && (force || !self.presorted)
    }
}

fn init_pertrans_sort<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    aggref: &'mcx Aggref<'mcx>,
    transno: usize,
    transfn_oid: Oid,
    agg_collation: Oid,
    presorted: bool,
    set_candidate: bool,
) -> PgResult<(PerTransSortData<'mcx>, AggOrderedSpec)> {
    let num_inputs = aggref.args.len();
    let num_trans_inputs = aggref.aggargtypes.len();
    assert!(
        num_trans_inputs + 1 <= MAX_ORDERED_TRANS_ARGS,
        "build_pertrans_for_aggref (nodeAgg.c): {num_trans_inputs} ordered trans inputs \
         exceed the replay fcinfo"
    );
    // By construction aggorder is a prefix of aggdistinct
    // (transformDistinctClause).
    let sortlist =
        if !aggref.aggdistinct.is_nil() { &aggref.aggdistinct } else { &aggref.aggorder };
    let num_sort_cols = sortlist.len();
    let num_distinct_cols = aggref.aggdistinct.len();
    debug_assert!(num_sort_cols > 0);
    debug_assert!(num_sort_cols >= aggref.aggorder.len());
    let sortdesc = execscan::exec_type_from_tl(mcx, &aggref.args)?;

    let mut sort_col_idx: PgVec<'mcx, i16> = vec_with_capacity_in(mcx, num_sort_cols)?;
    let mut sort_ops: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, num_sort_cols)?;
    let mut sort_collations: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, num_sort_cols)?;
    let mut sort_nulls_first: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, num_sort_cols)?;
    for sc_node in sortlist {
        let scl = sc_node.as_sort_group_clause().expect("agg sortlist cell");
        let tle = aggref
            .args
            .iter()
            .find_map(|n| {
                let t = n.as_target_entry().expect("Aggref.args cell");
                (t.ressortgroupref == scl.tleSortGroupRef).then_some(t)
            })
            .expect("agg ORDER BY/DISTINCT expression not found in Aggref.args");
        assert!(scl.sortop != 0, "sortless SortGroupClause survived the parser");
        sort_col_idx.push(tle.resno);
        sort_ops.push(scl.sortop);
        sort_collations.push(execscan::expr_collation(tle.expr));
        sort_nulls_first.push(scl.nulls_first);
    }

    let mut equalfn_one = None;
    let mut equalfn_multi = None;
    let mut eq_proc: Oid = 0;
    if num_distinct_cols > 0 {
        let mut eqfuncoids: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, num_distinct_cols)?;
        for sc_node in &aggref.aggdistinct {
            let scl = sc_node.as_sort_group_clause().expect("aggdistinct cell");
            eqfuncoids.push(lsyscache::get_opcode(scl.eqop)?);
        }
        if num_distinct_cols == 1 {
            eq_proc = eqfuncoids[0];
            equalfn_one = Some(fmgr_core::fmgr_info(eqfuncoids[0])?);
        } else {
            equalfn_multi = Some(::execexpr::exec_build_grouping_equal(
                mcx,
                &sortdesc,
                &sortdesc,
                &sort_col_idx[..num_distinct_cols],
                &eqfuncoids,
                &sort_collations[..num_distinct_cols],
            )?);
        }
    }

    let mut transfn = fmgr_core::fmgr_info(transfn_oid)?;
    let mut fnexpr_types: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, num_trans_inputs + 1)?;
    fnexpr_types.push(aggref.aggtranstype);
    for t in aggref.aggargtypes.iter() {
        fnexpr_types.push(t);
    }
    // SAFETY: leaked into the query arena; the replay flinfo dies with the
    // plan it serves — from_node_ref's contract (same carrier as
    // build_agg_trans's AggFnArgTypes).
    let fnexpr_types: &'static [Oid] = unsafe { core::mem::transmute(fnexpr_types.leak()) };
    // C build_aggregate_transfn_expr: the fake FuncExpr returns the
    // transition type (carrier slot 0).
    let carrier = ::mcx::alloc_leak_in(
        mcx,
        ::types_core::fmgr::AggFnArgTypes { rettype: aggref.aggtranstype, argtypes: fnexpr_types },
    )?;
    // SAFETY: carrier is arena-backed for the query, see above.
    transfn.fn_expr = Some(unsafe { ::types_core::fmgr::FnExprErased::from_node_ref(carrier) });

    let scratch_layout = Layout::array::<NullableDatum>(num_inputs.max(1))
        .expect("ordered scratch layout");
    let scratch: NonNull<NullableDatum> = ::mcx::Allocator::allocate(&mcx, scratch_layout)
        .map_err(|_| mcx.oom(scratch_layout.size()))?
        .cast();
    // SAFETY: fresh allocation of num_inputs slots.
    unsafe {
        for i in 0..num_inputs {
            scratch.as_ptr().add(i).write(NullableDatum::null());
        }
    }
    let flag_layout = Layout::new::<bool>();
    let flag: NonNull<bool> = ::mcx::Allocator::allocate(&mcx, flag_layout)
        .map_err(|_| mcx.oom(flag_layout.size()))?
        .cast();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { flag.write(false) };

    let (insert_slot, slot1, slot2) = if num_inputs > 1 {
        (
            Some(exectuples::make_tuple_table_slot(
                mcx,
                TupleSlotKind::Virtual,
                Some(sortdesc.clone()),
            )),
            Some(exectuples::make_tuple_table_slot(
                mcx,
                TupleSlotKind::MinimalTuple,
                Some(sortdesc.clone()),
            )),
            (num_distinct_cols > 0).then(|| {
                exectuples::make_tuple_table_slot(
                    mcx,
                    TupleSlotKind::MinimalTuple,
                    Some(sortdesc.clone()),
                )
            }),
        )
    } else {
        (None, None, None)
    };

    let ospec = AggOrderedSpec {
        scratch,
        num_trans_inputs: num_trans_inputs as u16,
        flag,
    };
    debug_assert!(!presorted || num_distinct_cols > 0);
    let (input_byval, input_typlen) = {
        let a = sortdesc.attr(0);
        (a.attbyval, a.attlen)
    };
    // Lane-v2 exact-DISTINCT set admission (distinctset.rs; pgrcolumnar-v2 plan
    // §2.3). Structural half here: single-column DISTINCT (which by
    // transformDistinctClause construction also means no agg-level ORDER BY
    // beyond that column — refuse any explicit aggorder), single input,
    // single transition input; the caller's `set_candidate` carries the
    // node-level half (lane enabled, no grouping sets, no combine).
    // Presorted entries get a kind too — DORMANT (C's per-row dedup runs)
    // unless the lane's skip-sort drive arms `force_distinct_set`. Everything
    // refused keeps the C paths bit-exactly.
    let set_kind = if set_candidate
        && aggref.aggorder.is_nil()
        && num_distinct_cols == 1
        && num_inputs == 1
        && num_trans_inputs == 1
    {
        distinct_set_kind(
            transfn_oid,
            sortdesc.attr(0).atttypid,
            eq_proc,
            sort_collations[0],
        )?
    } else {
        None
    };
    // Direct staged feed shape (`direct_att` field doc): single input whose
    // expression is exactly a bare Var(OUTER, attno) and no FILTER.
    let direct_att = (num_inputs == 1 && aggref.aggfilter.is_none())
        .then(|| {
            aggref.args.iter().next().and_then(|n| {
                let tle = n.as_target_entry().expect("Aggref.args cell");
                tle.expr.as_var().and_then(|v| {
                    (v.varno == ::execexpr::OUTER_VAR
                        && v.varlevelsup == 0
                        && v.varattno >= 1)
                        .then(|| (v.varattno - 1) as u16)
                })
            })
        })
        .flatten();
    Ok((
        PerTransSortData {
            transno,
            num_inputs,
            num_trans_inputs,
            num_distinct_cols,
            presorted,
            haslast: false,
            last_single: NullableDatum::null(),
            last_buf: PgVec::new_in(mcx),
            input_byval,
            input_typlen,
            sortdesc,
            sort_col_idx,
            sort_ops,
            sort_collations,
            sort_nulls_first,
            equalfn_one,
            equalfn_multi,
            transfn,
            agg_collation,
            scratch,
            flag,
            set_kind,
            dset: None,
            dset_degraded: false,
            // 2804 = int8inc_any, count(x)'s transition (`distinct_set_kind`'s
            // F_INT8INC_ANY admission row).
            set_count_transfn: set_kind.is_some() && transfn_oid == 2804,
            direct_att,
            sortstates: Vec::new(),
            insert_slot,
            slot1,
            slot2,
        },
        ospec,
    ))
}

/// The lane-v2 exact-DISTINCT set's type/equality/transition admission
/// matrix (pgrcolumnar-v2 plan §2.3). `Some(kind)` requires ALL of:
///
///   * the transition is one of count(any) / sum(int2|int4) /
///     avg(int2|int4) / sum-or-avg(int8) — the order-insensitive allowlist
///     (each transfn's result over a distinct-value multiset is independent
///     of replay order: pure counting or exact integer/Int128 accumulation);
///   * the DISTINCT equality operator's proc is the type's own equality and
///     that equality is REPRESENTATIONAL on the stored key —
///       int2eq/int4eq/int8eq over int2/int4/int8 (sign-extended-word value
///       equality; `DistinctSet` keys the sign-extended i64), or
///       texteq over text/varchar under a DETERMINISTIC collation (texteq's
///       deterministic arm is length+memcmp of detoasted content bytes —
///       exactly `DistinctSet`'s byte key). Nondeterministic collations
///       refuse: equal-under-collation but byte-different values would
///       violate equal-values-must-hash-equal for a byte hash.
///
/// Everything else (multi-arg DISTINCT, ORDER BY within the aggregate,
/// other types/operators — numeric's class equality included, bpchar's
/// space-stripping bpchareq included) returns None and keeps the C
/// sort-based path.
fn distinct_set_kind(
    transfn_oid: Oid,
    atttypid: Oid,
    eq_proc: Oid,
    collation: Oid,
) -> PgResult<Option<distinctset::DistinctKeyKind>> {
    // pg_proc transition functions (fmgr_core canonical.rs oids).
    const F_INT8INC_ANY: Oid = 2804; // count(x)
    const F_INT2_SUM: Oid = 1840; // sum(int2)
    const F_INT4_SUM: Oid = 1841; // sum(int4)
    const F_INT2_AVG_ACCUM: Oid = 1962; // avg(int2)
    const F_INT4_AVG_ACCUM: Oid = 1963; // avg(int4)
    const F_INT8_AVG_ACCUM: Oid = 2746; // sum(int8) / avg(int8)
    if !matches!(
        transfn_oid,
        F_INT8INC_ANY
            | F_INT2_SUM
            | F_INT4_SUM
            | F_INT2_AVG_ACCUM
            | F_INT4_AVG_ACCUM
            | F_INT8_AVG_ACCUM
    ) {
        return Ok(None);
    }
    // pg_proc equality procs (types_core::fmgr + int8/builtins.rs).
    const F_INT2EQ: Oid = 63;
    const F_INT4EQ: Oid = 65;
    const F_TEXTEQ: Oid = 67;
    const F_INT8EQ: Oid = 467;
    // GL-LOWDIST-3 datetime widening (adt_date/adt_timestamp builtins).
    const F_DATE_EQ: Oid = 1086;
    const F_TIMESTAMPTZ_EQ: Oid = 1152;
    const F_TIMESTAMP_EQ: Oid = 2052;
    const INT2OID: Oid = 21;
    const INT4OID: Oid = 23;
    const INT8OID: Oid = 20;
    const TEXTOID: Oid = 25;
    const VARCHAROID: Oid = 1043;
    const DATEOID: Oid = 1082;
    const TIMESTAMPOID: Oid = 1114;
    const TIMESTAMPTZOID: Oid = 1184;
    Ok(match (eq_proc, atttypid) {
        (F_INT2EQ, INT2OID) => Some(distinctset::DistinctKeyKind::Int16),
        (F_INT4EQ, INT4OID) => Some(distinctset::DistinctKeyKind::Int32),
        (F_INT8EQ, INT8OID) => Some(distinctset::DistinctKeyKind::Int64),
        // GL-LOWDIST-3 (knob-gated, default OFF): the datetime family's
        // same-type equality is REPRESENTATIONAL word equality on the
        // stored key exactly like the int family — date_eq is `==` on the
        // i32 day count, timestamp_eq/timestamptz_eq are `==` on the i64
        // microsecond count (adt_date/adt_timestamp cmp-op macros; the
        // infinity sentinels are ordinary word values) — so the sets ride
        // the existing Int32/Int64 lanes byte-identically. Cross-type
        // equalities (date-vs-timestamp) never appear as a same-type
        // DISTINCT arg's operator and stay refused.
        (F_DATE_EQ, DATEOID) if distinct_datetime_enabled() => {
            Some(distinctset::DistinctKeyKind::Int32)
        }
        (F_TIMESTAMP_EQ, TIMESTAMPOID) | (F_TIMESTAMPTZ_EQ, TIMESTAMPTZOID)
            if distinct_datetime_enabled() =>
        {
            Some(distinctset::DistinctKeyKind::Int64)
        }
        (F_TEXTEQ, TEXTOID | VARCHAROID)
            if collation != 0 && lsyscache::get_collation_isdeterministic(collation)? =>
        {
            Some(distinctset::DistinctKeyKind::Bytes)
        }
        _ => None,
    })
}

/// GL-LOWDIST-3 datetime-distinct widening — **DEFAULT ON** since the
/// GL-LOWDIST-3 flip (letter scratchpad/night/GL-LOWDIST-3-letter.md;
/// widen A/B @ 96c075c0a, dop {1,4,16} x {2.5M,10M}: serial set-mode
/// 14-23x over the C sort path, sink/GM 0.015-0.165 at every cell, all
/// oracle legs byte-equal). Kill spellings exactly
/// `PGRUST_LANE_V2_DISTINCT_DATETIME=0|off` (the t35 flipped-kill idiom) —
/// same spelling in the planner probe (m5_suppress::
/// distinct_datetime_enabled), the GROUPSINK coherence rule: admission
/// (set kinds here + sink spec derivation) and routing (probe suppression)
/// kill together. The pardistinct HYBRIDS never read this knob: their
/// `pd_derive_spec` calls pass `admit_datetime: false` and refuse datetime
/// sets cleanly (the hybrids are on the D1 deletion list — widening only
/// sink+serial keeps the displacement direction).
pub fn distinct_datetime_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_LANE_V2_DISTINCT_DATETIME").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

// C AggStatePerTransData's transtypeLen/transtypeByVal pair, indexed by
// transno (drives the initval datumCopy at group init).
#[derive(Clone, Copy)]
pub(crate) struct TransTyp {
    pub(crate) len: i16,
    pub(crate) byval: bool,
}

// AGG_SORTED state: firstSlot/grp_firstTuple as two swappable minimal slots
// (the pending slot holds C's grp_firstTuple copy), the grouping-boundary
// program is C's phase->eqfunctions[numCols-1].
struct PerSortData<'mcx> {
    first_slot: SlotData<'mcx>,
    pending_slot: SlotData<'mcx>,
    // None when numCols == 0 (all keys constant): no boundary, one group.
    eq: Option<PgBox<'mcx, ExprState<'mcx>>>,
    have_pending: bool,
}

// C AggStatePerHashData, single grouping set (find_hash_columns order:
// grouping cols first, then other needed input cols).
struct PerHashData<'mcx> {
    hashtable: TupleHashTable<'mcx>,
    // C's one minimal-tuple hashslot split in two (same allocation shape):
    // the virtual slot feeds lookups, the minimal one deforms at retrieve.
    hashslot: SlotData<'mcx>,
    retrieve_slot: SlotData<'mcx>,
    first_slot: SlotData<'mcx>,
    num_cols: usize,
    hash_grp_col_idx_input: PgVec<'mcx, i16>,
    largest_grp_col_idx: i32,
    outer_natts: usize,
    // The steps' pergroup indirection cell (exec_build_agg_trans_hashed).
    pergroup_cell: NonNull<NonNull<AggPerGroup>>,
    hash_ngroups_limit: u64,
    hash_ngroups_current: u64,
    hash_mem_limit: usize,
    table_filled: bool,
    // u64: hashtable mode holds execgrouping's packed (start,visited)
    // cursor (high-32 packing must survive 32-bit wasm); compact mode a
    // plain row index (cast at use).
    hashiter: u64,
    // C hash_tablecxt: entries + pergroups (transvalues stay in aggcontext).
    table_ctx: MemoryContext,
    spill: HashSpillState<'mcx>,
    // Lane-v2 compact-row table (Stage 2.2) when armed for this build; the
    // C tuplehash above stays the fallback + oracle (compact.rs module doc).
    compact: Option<compact::CompactHash>,
    // Stage-4 §4.4 radix exchange (merge.rs): the worker-side bounded-table
    // state, lazily resolved off the handoff registry on the first probe.
    exchange: merge::ExchangeState,
    // M2 aggregation sink (sink.rs): Some(cap) on a SINK WORKER build — the
    // compact arms size/gate by the cap (bounded Local discipline) and the
    // backstop must never migrate into the C table (the sink cannot export
    // it); the sink drain flushes at the cap instead.
    sink_cap: Option<u32>,
    // M3.5 spill-armed admission (mt16-cliffs, the ~10M-group @100M hmm=2 cliff):
    // true on a sink build whose engagement carries a live spill arm. The
    // compact admission gates skip the ESTIMATE-based SpillRisk refusal
    // then (word-keyed shapes only — a budget crossing degrades to spill
    // epochs, not an error), keeping the cap-bounded sizing discipline.
    // Meaningful only while `sink_cap` is Some; canonical bytes-keyed
    // shapes never see it set (their runs are not spillable — the C2
    // record-format gap keeps their phase-1 refusal).
    sink_spill_ok: bool,
    // GL-Q2829-FIX-1's per-thread FREEING byref-state child lived here
    // until the t45 revert adjudication (GL-DICTDRAIN-3): the drain's
    // Local-owned table migrates across pool threads, so a per-(thread,
    // query) context home broke the replace-free's allocator-exactness.
    // The by-ref str state store now travels WITH the table —
    // `compact::CompactHash::str_arena` (armed by
    // `sink::agg_sink_arm_str_state`).
}

// The AggState spill slice (nodeAgg.c), single set: `spill` doubles as C's
// hash_spills[0] and the refill loop's local spill; (input_card, used_bits)
// are the lazy hashagg_spill_init parameters for the current pass.
struct HashSpillState<'mcx> {
    mode: bool,
    ever_spilled: bool,
    tapeset: Option<LogicalTapeSet<'mcx>>,
    spill: Option<HashAggSpill<'mcx>>,
    // C stack: top at the end.
    batches: PgVec<'mcx, HashAggBatch>,
    all_cols_needed: bool,
    max_colno_needed: i32,
    colnos_needed: PgVec<'mcx, bool>,
    rslot: SlotData<'mcx>,
    wslot: SlotData<'mcx>,
    // hashagg_batch_read scratch: one maxaligned minimal-tuple image.
    read_buf: PgVec<'mcx, u64>,
    // hashagg_spill_tuple's transient tuple copy; reset after every write.
    tmp_ctx: MemoryContext,
    input_card: f64,
    used_bits: u32,
    hashentrysize: f64,
}

// C HashAggSpill.
struct HashAggSpill<'mcx> {
    npartitions: usize,
    partitions: PgVec<'mcx, TapeIdx>,
    ntuples: PgVec<'mcx, i64>,
    hll_card: PgVec<'mcx, HyperLogLog32>,
    mask: u32,
    shift: i32,
}

// C HashAggBatch, setno-free (single set).
struct HashAggBatch {
    input_tape: TapeIdx,
    used_bits: u32,
    input_card: f64,
}

// C AggStatePerAggData finalize slice; result copy discipline rides the armed
// result mcx instead of MemoryContextContains.
struct PerAggData<'mcx> {
    transno: u32,
    aggref: &'mcx Aggref<'mcx>,
    trans_shared: bool,
    finalfn: Option<FmgrInfo>,
    // C AggStatePerTransData.serialfn, hosted per-agg (resolved once; shared
    // transnos duplicate the resolved carrier, not the resolution).
    serialfn: Option<FmgrInfo>,
    num_final_args: u16,
    agg_collation: Oid,
    resulttype_len: i16,
    direct_args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
}

fn make_agg_state_node<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    ctx: MemoryContext,
) -> PgResult<NonNull<AggStateNode>> {
    let layout = Layout::new::<AggStateNode>();
    let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
    let p: NonNull<AggStateNode> = raw.cast();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(AggStateNode::new(ctx)) };
    // The node's MemoryContext is droppy inside a no-drop arena: the query
    // context's reset callback is its destructor (docs/no-drop.md guard rule).
    // SAFETY: fires exactly once, before the arena bytes are reclaimed.
    mcx.context()
        .register_reset_callback(move || unsafe { core::ptr::drop_in_place(p.as_ptr()) });
    Ok(p)
}

#[track_caller]
#[cold]
#[inline(never)]
fn agg_lookup_failed(aggfnoid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for aggregate {aggfnoid}")))
}

#[track_caller]
#[cold]
#[inline(never)]
fn agg_permission_denied(aggfnoid: Oid) -> Box<PgError> {
    let msg = match syscache_seams::pg_proc_proname::call(aggfnoid) {
        Ok(Some(name)) => format!(
            "permission denied for aggregate {}",
            core::str::from_utf8(name.name_str()).expect("catalog NameData is valid UTF-8")
        ),
        _ => format!("permission denied for aggregate {aggfnoid}"),
    };
    Box::new(PgError::error(msg).with_sqlstate(::types_error::ERRCODE_INSUFFICIENT_PRIVILEGE))
}

// unported: node families this walker does not know raise a clean
// ERRCODE_FEATURE_NOT_SUPPORTED error at ExecInitAgg time (C uses the
// generic expression_tree_walker, which cannot miss a family).
#[track_caller]
#[cold]
#[inline(never)]
fn agg_tlist_unported(tag: ::types_nodes::NodeTag) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "aggregate target list over {tag:?} expressions is not yet implemented"
        ))
        .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

fn collect_aggrefs<'mcx>(
    node: Node<'mcx>,
    out: &mut PgVec<'mcx, (Node<'mcx>, &'mcx Aggref<'mcx>)>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Aggref => out.push((node, node.as_aggref().unwrap())),
        // GroupingFunc args are never evaluated (EEOP_GROUPING_FUNC reads
        // grouped_cols only).
        NodeTag::T_GroupingFunc => {}
        NodeTag::T_TargetEntry => collect_aggrefs(node.as_target_entry().unwrap().expr, out)?,
        NodeTag::T_Var | NodeTag::T_Const => {}
        NodeTag::T_FuncExpr => {
            for a in node.as_func_expr().unwrap().args.iter() {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_OpExpr => {
            for a in node.as_op_expr().unwrap().args.iter() {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_Param
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_NextValueExpr
        | NodeTag::T_CoerceToDomainValue => {}
        NodeTag::T_RelabelType => collect_aggrefs(node.as_relabel_type().unwrap().arg, out)?,
        NodeTag::T_CoerceViaIO => collect_aggrefs(node.as_coerce_via_io().unwrap().arg, out)?,
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            collect_aggrefs(a.arg, out)?;
            if let Some(e) = a.elemexpr {
                collect_aggrefs(e, out)?;
            }
        }
        NodeTag::T_ConvertRowtypeExpr => {
            collect_aggrefs(node.as_convert_rowtype_expr().unwrap().arg, out)?
        }
        NodeTag::T_CoerceToDomain => {
            collect_aggrefs(node.as_coerce_to_domain().unwrap().arg, out)?
        }
        NodeTag::T_BoolExpr => {
            for a in node.as_bool_expr().unwrap().args.iter() {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_NullTest => {
            if let Some(a) = node.as_null_test().unwrap().arg {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_BooleanTest => {
            if let Some(a) = node.as_boolean_test().unwrap().arg {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_DistinctExpr => {
            for a in node.as_distinct_expr().unwrap().args.iter() {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for a in node.as_scalar_array_op_expr().unwrap().args.iter() {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_ArrayExpr => {
            for e in node.as_array_expr().unwrap().elements.iter() {
                collect_aggrefs(e, out)?;
            }
        }
        NodeTag::T_RowExpr => {
            for a in node.as_row_expr().unwrap().args.iter() {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            for a in rc.largs.iter().chain(rc.rargs.iter()) {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(a) = c.arg {
                collect_aggrefs(a, out)?;
            }
            for w in c.args.iter() {
                let cw = w.as_case_when().expect("CaseWhen");
                collect_aggrefs(cw.expr.expect("CaseWhen.expr"), out)?;
                collect_aggrefs(cw.result.expect("CaseWhen.result"), out)?;
            }
            if let Some(d) = c.defresult {
                collect_aggrefs(d, out)?;
            }
        }
        NodeTag::T_CoalesceExpr => {
            for a in node.as_coalesce_expr().unwrap().args.iter() {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_MinMaxExpr => {
            for a in node.as_min_max_expr().unwrap().args.iter() {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                collect_aggrefs(e, out)?;
            }
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for a in c.args.iter() {
                collect_aggrefs(a, out)?;
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                collect_aggrefs(e, out)?;
            }
        }
        NodeTag::T_JsonIsPredicate => {
            if let Some(e) = node.as_json_is_predicate().unwrap().expr {
                collect_aggrefs(e, out)?;
            }
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            if let Some(te) = sp.testexpr {
                collect_aggrefs(te, out)?;
            }
            for a in sp.args.iter() {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            for a in x.named_args.iter().chain(x.args.iter()) {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_SubscriptingRef => {
            let sref = node.as_subscripting_ref().unwrap();
            for a in sref.refupperindexpr.iter().flatten() {
                collect_aggrefs(a, out)?;
            }
            for a in sref.reflowerindexpr.iter().flatten() {
                collect_aggrefs(a, out)?;
            }
            if let Some(e) = sref.refexpr {
                collect_aggrefs(e, out)?;
            }
            if let Some(e) = sref.refassgnexpr {
                collect_aggrefs(e, out)?;
            }
        }
        // C expression_tree_walker recursion for the OpExpr-shaped and
        // field-access families (primnodes.h).
        NodeTag::T_NullIfExpr => {
            for a in node.as_null_if_expr().unwrap().args.iter() {
                collect_aggrefs(a, out)?;
            }
        }
        NodeTag::T_FieldSelect => {
            collect_aggrefs(node.as_field_select().unwrap().arg, out)?
        }
        NodeTag::T_FieldStore => {
            let fs = node.as_field_store().unwrap();
            collect_aggrefs(fs.arg, out)?;
            for v in fs.newvals.iter() {
                collect_aggrefs(v, out)?;
            }
        }
        NodeTag::T_NamedArgExpr => {
            if let Some(a) = node.as_named_arg_expr().unwrap().arg {
                collect_aggrefs(a, out)?;
            }
        }
        // unported: any family this walker does not know.
        tag => return Err(agg_tlist_unported(tag)),
    }
    Ok(())
}

// GetAggInitVal (nodeAgg.c): initval text through the transtype's typinput.
// In-function by-ref results ride the resolved carrier's scratch (dead once
// flinfo drops); C's palloc'd result is modeled by the datumCopy into mcx.
fn get_agg_init_val(mcx: ::mcx::Mcx<'_>, text: &str, transtype: Oid) -> PgResult<Datum> {
    let (typinput, typioparam) = lsyscache::getTypeInputInfo(transtype)?;
    let mut flinfo = fmgr_core::fmgr_info(typinput)?;
    let cstr = std::ffi::CString::new(text)
        .expect("agginitval text contains an interior NUL");
    let d = ::types_fmgr::input_function_call(&mut flinfo, Some(&cstr), typioparam, -1, mcx)?;
    let (typlen, typbyval) = lsyscache::get_typlenbyval(transtype)?;
    if typbyval {
        Ok(d)
    } else {
        // SAFETY: non-null by-ref in-function result, live until flinfo drops.
        unsafe { ::execexpr::agg_datum_copy(mcx, d, typlen) }
    }
}

/// `ExecInitAgg` (nodeAgg.c). The caller (execProcnode's T_Agg arm) inits the
/// outer child and passes this node's result type.
pub fn exec_init_agg<'mcx>(
    node: &'mcx Agg<'mcx>,
    estate: &mut EStateData<'mcx>,
    _eflags: i32,
    result_desc: Rc<TupleDescData<'static>>,
    outer_desc: Option<Rc<TupleDescData<'static>>>,
) -> PgResult<AggStateData<'mcx>> {
    let mcx = estate.es_query_cxt;
    let has_grouping_sets = !node.groupingSets.is_nil() || !node.chain.is_nil();
    if node.aggstrategy != AGG_PLAIN
        && node.aggstrategy != AGG_HASHED
        && node.aggstrategy != AGG_SORTED
        && node.aggstrategy != AGG_MIXED
    {
        panic!("ExecInitAgg (nodeAgg.c): aggstrategy {} cannot happen", node.aggstrategy);
    }
    assert!(
        node.aggstrategy != AGG_MIXED || has_grouping_sets,
        "ExecInitAgg (nodeAgg.c): AGG_MIXED outside grouping sets cannot happen"
    );
    let do_combine = node.aggsplit & AGGSPLITOP_COMBINE != 0;
    let skip_final = node.aggsplit & AGGSPLITOP_SKIPFINAL != 0;
    let do_serialize = node.aggsplit & AGGSPLITOP_SERIALIZE != 0;
    let do_deserialize = node.aggsplit & AGGSPLITOP_DESERIALIZE != 0;
    assert!(
        node.aggsplit == AGGSPLIT_SIMPLE
            || node.aggsplit == AGGSPLIT_INITIAL_SERIAL
            || node.aggsplit == AGGSPLIT_FINAL_DESERIAL,
        "ExecInitAgg (nodeAgg.c): aggsplit {} cannot happen",
        node.aggsplit
    );
    assert!(
        node.aggsplit == AGGSPLIT_SIMPLE || !has_grouping_sets,
        "ExecInitAgg (nodeAgg.c): partial aggregation under grouping sets cannot happen"
    );
    if node.aggstrategy == AGG_PLAIN && node.numCols != 0 {
        panic!("ExecInitAgg (nodeAgg.c): AGG_PLAIN with grouping columns cannot happen");
    }
    // AGG_SORTED with numCols == 0 is legal: every grouping key was proved
    // constant (or the grouping set is empty), so the whole input is one
    // group; C's boundary check is guarded by numCols > 0.

    // Hashed: the node context IS the table context (C hands
    // BuildTupleHashTable the same hashcontext memory).
    let agg_ctx_name =
        if node.aggstrategy == AGG_HASHED { "HashAgg hash table" } else { "AggContext" };
    let agg_node = make_agg_state_node(mcx, mcx.context().new_child_bump(agg_ctx_name))?;
    let fm_agg_node: FmNodePtr = Some(agg_node.cast());
    let tmpcontext = estate.create_expr_context();
    let ps_ExprContext = estate.exec_assign_expr_context();
    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::Virtual);

    let mut aggrefs: PgVec<'mcx, (Node<'mcx>, &'mcx Aggref<'mcx>)> = PgVec::new_in(mcx);
    for tle in node.plan.targetlist.iter() {
        collect_aggrefs(tle, &mut aggrefs)?;
    }
    for q in node.plan.qual.iter() {
        collect_aggrefs(q, &mut aggrefs)?;
    }
    // tlist and qual Aggrefs can share aggnos (find_compatible_agg);
    // numaggs == 0 is C's hashed-DISTINCT shape.
    // C: numaggs == 0 is not an error for any strategy — grouping-only Agg
    // (hash-based grouping, or every Aggref lives in an outer level and rides
    // in as a SubPlan arg / was optimized away).
    let numaggs = aggrefs.iter().map(|(_, a)| a.aggno + 1).max().unwrap_or(0) as usize;

    let mut by_aggno: PgVec<'mcx, Option<(Node<'mcx>, &'mcx Aggref<'mcx>)>> =
        vec_with_capacity_in(mcx, numaggs)?;
    by_aggno.resize(numaggs, None);
    let mut numtrans = 0usize;
    for &(anode, aggref) in aggrefs.iter() {
        let (aggno, transno) = (aggref.aggno, aggref.aggtransno);
        assert!(aggno >= 0 && transno >= 0, "Aggref without planner aggno/aggtransno");
        assert!((aggno as usize) < numaggs, "Aggref.aggno out of range");
        if let Some((_, prev)) = by_aggno[aggno as usize] {
            assert!(
                prev.aggfnoid == aggref.aggfnoid && prev.aggtransno == transno,
                "shared aggno with diverging Aggrefs"
            );
        }
        by_aggno[aggno as usize] = Some((anode, aggref));
        numtrans = numtrans.max(transno as usize + 1);
    }

    let userid = miscinit_seams::get_user_id::call();
    // Droppy FmgrInfo carriers: AggStateData's box owns the drops
    // (ExprState.frames precedent), hence no no-drop ctor.
    let mut peragg: PgVec<'mcx, PerAggData<'mcx>> = PgVec::new_in(mcx);
    peragg
        .try_reserve(numaggs)
        .map_err(|_| mcx.oom(numaggs * core::mem::size_of::<PerAggData<'_>>()))?;
    let mut trans_init: PgVec<'mcx, NullableDatum> = vec_with_capacity_in(mcx, numtrans)?;
    trans_init.resize(numtrans, NullableDatum::null());
    let mut trans_aggref: PgVec<'mcx, Option<(Node<'mcx>, &'mcx Aggref<'mcx>)>> =
        vec_with_capacity_in(mcx, numtrans)?;
    trans_aggref.resize(numtrans, None);
    let mut trans_fnoid: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, numtrans)?;
    trans_fnoid.resize(numtrans, 0);
    let mut trans_deserialfn: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, numtrans)?;
    trans_deserialfn.resize(numtrans, 0);
    let mut trans_typ: PgVec<'mcx, TransTyp> = vec_with_capacity_in(mcx, numtrans)?;
    trans_typ.resize(numtrans, TransTyp { len: 0, byval: true });

    let mut pertrans_sort: PgVec<'mcx, PerTransSortData<'mcx>> = PgVec::new_in(mcx);
    let mut ordered_specs: PgVec<'mcx, Option<AggOrderedSpec>> =
        vec_with_capacity_in(mcx, numtrans)?;
    ordered_specs.resize(numtrans, None);
    let mut trans_shared: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, numtrans)?;
    trans_shared.resize(numtrans, false);
    let params = estate.param_bind();
    for aggno in 0..numaggs {
        let (aggref_node, aggref) = by_aggno[aggno].expect("planner aggno numbering has gaps");
        let aclresult = aclchk_seams::object_aclcheck::call(
            PROCEDURE_RELATION_ID,
            aggref.aggfnoid,
            userid,
            ACL_EXECUTE,
        )?;
        if aclresult != ACLCHECK_OK {
            return Err(agg_permission_denied(aggref.aggfnoid));
        }
        let shape = syscache_seams::lookup_pg_aggregate_shape::call(aggref.aggfnoid)?
            .ok_or_else(|| agg_lookup_failed(aggref.aggfnoid))?;
        let is_ordered_set = shape.aggkind != AGGKIND_NORMAL;
        debug_assert!(shape.aggkind == aggref.aggkind);
        if (!aggref.aggorder.is_nil() || !aggref.aggdistinct.is_nil())
            && node.aggstrategy == AGG_HASHED
        {
            panic!("ExecInitAgg (nodeAgg.c): DISTINCT/ORDER BY under AGG_HASHED cannot happen");
        }
        let transtype = aggref.aggtranstype;
        assert!(transtype != 0, "Aggref.aggtranstype unset (planner must resolve it)");
        let (translen, transbyval) = lsyscache::get_typlenbyval(transtype)?;

        const INTERNALOID: Oid = 2281;
        let mut serialfn_oid: Oid = 0;
        let mut deserialfn_oid: Oid = 0;
        if transtype == INTERNALOID {
            if do_serialize {
                assert!(skip_final, "serialization only valid when not running finalfn");
                if shape.aggserialfn == 0 {
                    return Err(Box::new(PgError::error(
                        "serialfunc not provided for serialization aggregation".to_string(),
                    )));
                }
                serialfn_oid = shape.aggserialfn;
            }
            if do_deserialize {
                assert!(do_combine, "deserialization only valid when combining states");
                if shape.aggdeserialfn == 0 {
                    return Err(Box::new(PgError::error(
                        "deserialfunc not provided for deserialization aggregation".to_string(),
                    )));
                }
                deserialfn_oid = shape.aggdeserialfn;
            }
        }
        let serialfn = if serialfn_oid != 0 { Some(fmgr_core::fmgr_info(serialfn_oid)?) } else { None };

        let num_direct_args = aggref.aggdirectargs.len();
        let num_final_args = if shape.aggfinalextra {
            aggref.aggargtypes.len() as u16 + 1
        } else {
            num_direct_args as u16 + 1
        };
        let finalfn = if !skip_final && shape.aggfinalfn != 0 {
            // Divergence: C aclchecks as the aggregate owner; differs only
            // under SET ROLE.
            let aclresult = aclchk_seams::object_aclcheck::call(
                PROCEDURE_RELATION_ID,
                shape.aggfinalfn,
                userid,
                ACL_EXECUTE,
            )?;
            if aclresult != ACLCHECK_OK {
                return Err(agg_permission_denied(shape.aggfinalfn));
            }
            let mut flinfo = fmgr_core::fmgr_info(shape.aggfinalfn)?;
            // build_aggregate_finalfn_expr's [transtype, input types..].
            let mut fnexpr_types: PgVec<'mcx, Oid> =
                vec_with_capacity_in(mcx, num_final_args as usize)?;
            fnexpr_types.push(aggref.aggtranstype);
            for t in aggref.aggargtypes.iter().take(num_final_args as usize - 1) {
                fnexpr_types.push(t);
            }
            // SAFETY: leaked into the query arena; the flinfo dies with the
            // plan (init_pertrans_sort's carrier precedent).
            let fnexpr_types: &'static [Oid] =
                unsafe { core::mem::transmute(fnexpr_types.leak()) };
            // C build_aggregate_finalfn_expr: the fake FuncExpr returns the
            // aggregate result type.
            let carrier = ::mcx::alloc_leak_in(
                mcx,
                ::types_core::fmgr::AggFnArgTypes {
                    rettype: aggref.aggtype,
                    argtypes: fnexpr_types,
                },
            )?;
            // SAFETY: carrier is arena-backed for the query, see above.
            flinfo.fn_expr =
                Some(unsafe { ::types_core::fmgr::FnExprErased::from_node_ref(carrier) });
            Some(flinfo)
        } else {
            None
        };
        let (resulttype_len, _resulttype_byval) = lsyscache::get_typlenbyval(aggref.aggtype)?;

        let mut direct_args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>> = PgVec::new_in(mcx);
        for d in aggref.aggdirectargs.iter() {
            let mut es = ::execexpr::exec_init_expr(mcx, Some(d), params)?
                .expect("aggdirectargs cell is a non-NULL expression");
            // SAFETY: the ps_ExprContext outlives the program (same estate);
            // C evaluates direct args in its per-tuple memory.
            unsafe { es.arm_result_mcx_raw(estate.ecxt(ps_ExprContext).per_tuple_mcx()) };
            direct_args.push(es);
        }

        let transno = aggref.aggtransno as usize;
        peragg.push(PerAggData {
            transno: transno as u32,
            aggref,
            trans_shared: false,
            finalfn,
            serialfn,
            num_final_args,
            agg_collation: aggref.inputcollid,
            resulttype_len,
            direct_args,
        });
        let transfn_oid = if do_combine {
            if shape.aggcombinefn == 0 {
                return Err(Box::new(PgError::error(
                    "combinefn not set for aggregate function".to_string(),
                )));
            }
            shape.aggcombinefn
        } else {
            shape.aggtransfn
        };
        match trans_aggref[transno] {
            // find_compatible_trans keys sharing on the transition state.
            Some((_, prev)) => {
                assert!(
                    trans_fnoid[transno] == transfn_oid
                        && prev.aggtranstype == aggref.aggtranstype,
                    "shared transno with diverging transition state"
                );
                trans_shared[transno] = true;
            }
            None => {
                trans_aggref[transno] = Some((aggref_node, aggref));
                trans_fnoid[transno] = transfn_oid;
                trans_deserialfn[transno] = deserialfn_oid;
                trans_typ[transno] = TransTyp { len: translen, byval: transbyval };
                // C build_pertrans_for_aggref: aggpresorted ORDER BY (no
                // DISTINCT) runs as a plain aggregate; aggpresorted DISTINCT
                // keeps a pertrans for the consecutive-duplicate check.
                if !is_ordered_set
                    && (!aggref.aggorder.is_nil() || !aggref.aggdistinct.is_nil())
                    && !(aggref.aggpresorted && aggref.aggdistinct.is_nil())
                {
                    let (mut ps, ospec) = init_pertrans_sort(
                        mcx,
                        aggref,
                        transno,
                        shape.aggtransfn,
                        aggref.inputcollid,
                        aggref.aggpresorted,
                        // Node-level half of the exact-DISTINCT set admission
                        // (distinct_set_kind doc): lane-v2 only (lane-OFF is
                        // bit-untouched), never under grouping sets (per-set
                        // sortstates — the set is single-set) or a combine
                        // phase.
                        lane_v2_enabled() && !has_grouping_sets && !do_combine,
                    )?;
                    if let Some(eq) = ps.equalfn_multi.as_mut() {
                        // The DISTINCT dedup eq detoasts compressed by-ref
                        // args through the frame's result mcx; the drain
                        // resets tmpcontext per row (C: tmpcontext memory).
                        // SAFETY: the tmpcontext ExprContext outlives the
                        // program (same estate).
                        unsafe {
                            eq.arm_result_mcx_raw(estate.ecxt(tmpcontext).per_tuple_mcx())
                        };
                    }
                    pertrans_sort.push(ps);
                    ordered_specs[transno] = Some(ospec);
                }
                let initval = syscache_seams::pg_aggregate_agginitval::call(mcx, aggref.aggfnoid)?
                    .ok_or_else(|| agg_lookup_failed(aggref.aggfnoid))?;
                trans_init[transno] = match initval {
                    None => NullableDatum::null(),
                    Some(text) => NullableDatum {
                        value: get_agg_init_val(mcx, &text, transtype)?,
                        isnull: false,
                    },
                };
                if do_combine {
                    if fmgr_core::fmgr_info(transfn_oid)?.fn_strict && transtype == INTERNALOID {
                        return Err(Box::new(
                            PgError::error(
                                "combine function with transition type internal must not be \
                                 declared STRICT"
                                    .to_string(),
                            )
                            .with_sqlstate(::types_error::ERRCODE_INVALID_FUNCTION_DEFINITION),
                        ));
                    }
                } else if trans_init[transno].isnull
                    && fmgr_core::fmgr_info(transfn_oid)?.fn_strict
                {
                    // C checks the FIRST aggregated input (nodeAgg.c
                    // IsBinaryCoercible gate) — the strict first-value path
                    // copies args[1]; exact-match covers every live agg.
                    let input_type = aggref.aggargtypes.first();
                    if input_type != Some(transtype) {
                        panic!(
                            "ExecInitAgg (nodeAgg.c): strict transfn with NULL initval and \
                             input type {input_type:?} != transtype {transtype} \
                             (IsBinaryCoercible not ported)"
                        );
                    }
                }
            }
        }
    }

    for pa in peragg.iter_mut() {
        pa.trans_shared = trans_shared[pa.transno as usize];
    }

    let mut pergroup: PgVec<'mcx, AggPerGroup> = vec_with_capacity_in(mcx, numtrans)?;
    pergroup.resize(
        numtrans,
        AggPerGroup { trans_value: Datum::null(), trans_value_is_null: true, no_trans_value: true },
    );
    let pergroup_base = NonNull::new(pergroup.as_mut_ptr()).unwrap();

    let (agg_values_base, agg_nulls_base) = {
        let ecxt = estate.ecxt_mut(ps_ExprContext);
        ecxt.ecxt_aggvalues.resize(numaggs, Datum::null());
        ecxt.ecxt_aggnulls.resize(numaggs, true);
        (
            NonNull::new(ecxt.ecxt_aggvalues.as_mut_ptr()).unwrap(),
            NonNull::new(ecxt.ecxt_aggnulls.as_mut_ptr()).unwrap(),
        )
    };

    let mut specs: PgVec<'mcx, AggTransSpec<'mcx, 'mcx>> = vec_with_capacity_in(mcx, numtrans)?;
    for transno in 0..numtrans {
        let (_, aggref) =
            trans_aggref[transno].expect("planner aggtransno numbering has gaps");
        // SAFETY: transno < numtrans elements of the once-allocated pergroup.
        let pg = unsafe { NonNull::new_unchecked(pergroup_base.as_ptr().add(transno)) };
        let is_ordered_set = aggref.aggkind != AGGKIND_NORMAL;
        let num_direct_args = if is_ordered_set { aggref.aggdirectargs.len() } else { 0 };
        let mut arg_types: PgVec<'mcx, Oid>;
        if do_combine {
            // aggcombinefn always has two arguments of aggtranstype.
            assert!(
                aggref.args.len() == 1 && ordered_specs[transno].is_none(),
                "combining Aggref has one arg and no DISTINCT/ORDER BY"
            );
            arg_types = vec_with_capacity_in(mcx, 2)?;
            arg_types.push(aggref.aggtranstype);
            arg_types.push(aggref.aggtranstype);
        } else {
            arg_types =
                vec_with_capacity_in(mcx, aggref.aggargtypes.len() - num_direct_args + 1)?;
            arg_types.push(aggref.aggtranstype);
            for t in aggref.aggargtypes.iter().skip(num_direct_args) {
                arg_types.push(t);
            }
        }
        let cur_agg =
            is_ordered_set.then(|| (NonNull::from(aggref).cast::<()>(), trans_shared[transno]));
        specs.push(AggTransSpec {
            transfn_oid: trans_fnoid[transno],
            deserialfn_oid: trans_deserialfn[transno],
            combine: do_combine,
            inputcollid: aggref.inputcollid,
            init_value_is_null: trans_init[transno].isnull,
            arg_types: arg_types.leak(),
            args: &aggref.args,
            aggfilter: aggref.aggfilter,
            pergroup: pg,
            transtype_byval: trans_typ[transno].byval,
            transtype_len: trans_typ[transno].len,
            ordered: ordered_specs[transno],
            cur_agg,
        });
    }
    let merge_outer_desc = if !has_grouping_sets && node.aggstrategy == AGG_HASHED {
        outer_desc.clone()
    } else {
        None
    };
    let (mut evaltrans, perhash, persort, gs) = if has_grouping_sets {
        let gs = gsets::init_grouping_sets(
            node, estate, outer_desc, &specs, numtrans, fm_agg_node, params, tmpcontext,
        )?;
        (None, None, None, Some(gs))
    } else if node.aggstrategy == AGG_HASHED {
        let mut ph = init_perhash(node, estate, numtrans)?;
        // Hash/match evaluation allocates in tmpcontext per-tuple memory,
        // reset per input row by every drive loop — C BuildTupleHashTable's
        // tempcxt (nodeAgg.c init_hash_table). Above all: probing a
        // compressed/external by-ref grouping key detoasts per PROBE; a
        // query-lifetime context there is memory ∝ input rows, invisible to
        // hash_agg_check_limits — high-NDV text-key hash aggregation must
        // stay bounded by entering spill mode, not grow with the scan.
        // SAFETY: the tmpcontext ExprContext outlives the table (same
        // estate, arena-boxed).
        unsafe { ph.hashtable.set_temp_ctx_raw(estate.ecxt(tmpcontext).per_tuple_mcx()) };
        let evaltrans = ::executils::with_subplan_compile_env(estate, |env| {
            ::execexpr::exec_build_agg_trans_hashed_subplans(
                mcx,
                &specs,
                ph.pergroup_cell,
                fm_agg_node,
                params,
                env,
            )
        })?;
        (Some(evaltrans), Some(ph), None, None)
    } else {
        let mut persort = if node.aggstrategy == AGG_SORTED {
            Some(init_persort(node, estate)?)
        } else {
            None
        };
        if let Some(ps) = persort.as_mut() {
            // The boundary eq detoasts compressed by-ref keys through the
            // frame's result mcx; C runs it in tmpcontext per-tuple memory
            // (ExecQualAndReset), which agg_retrieve_sorted resets per row.
            // SAFETY: the tmpcontext ExprContext outlives the program (same
            // estate).
            if let Some(eq) = ps.eq.as_mut() {
                unsafe { eq.arm_result_mcx_raw(estate.ecxt(tmpcontext).per_tuple_mcx()) };
            }
        }
        let evaltrans = ::executils::with_subplan_compile_env(estate, |env| {
            ::execexpr::exec_build_agg_trans_subplans(mcx, &specs, fm_agg_node, params, env)
        })?;
        (Some(evaltrans), None, persort, None)
    };
    // C invokes transfns in the tmpcontext per-tuple memory; by-ref call
    // results ride the armed result mcx there, reset per tuple (phase
    // programs are armed inside init_grouping_sets).
    if let Some(et) = evaltrans.as_mut() {
        // SAFETY: the tmpcontext ExprContext outlives the program (same estate).
        unsafe { et.arm_result_mcx_raw(estate.ecxt(tmpcontext).per_tuple_mcx()) };
    }
    let bind = AggBind {
        values: agg_values_base,
        nulls: agg_nulls_base,
        naggs: numaggs as u16,
        grouping: gs.as_ref().map(|g| g.grouping_cell()),
    };
    let (proj, qual) = ::executils::with_subplan_compile_env(estate, |env| -> PgResult<_> {
        let env = env.map(|mut e| {
            e.agg = Some(bind);
            e
        });
        let proj = exec_build_agg_projection_info_subplans(
            mcx,
            &node.plan.targetlist,
            None,
            bind,
            params,
            env,
        )?;
        let qual = exec_build_agg_qual_subplans(mcx, &node.plan.qual, bind, params, env)?;
        Ok((proj, qual))
    })?;
    let merge = match (&perhash, &evaltrans, &merge_outer_desc) {
        (Some(ph), Some(et), Some(od)) => {
            let has_subplan = et.has_subplan()
                || proj.has_subplan()
                || qual.as_deref().is_some_and(|q| q.has_subplan());
            merge::init_finalize_merge(
                node,
                estate,
                &trans_fnoid,
                &trans_typ,
                &trans_aggref,
                pertrans_sort.is_empty(),
                has_subplan,
                ph,
                Some(od),
            )?
        }
        _ => None,
    };
    let mut qual = qual;
    if let Some(q) = qual.as_mut() {
        // HAVING callees allocate by-ref results through the frame's result
        // mcx; C evaluates the qual in the output ExprContext's per-tuple
        // memory, reset per group.
        // SAFETY: the ps_ExprContext outlives the program (same estate).
        unsafe { q.arm_result_mcx_raw(estate.ecxt(ps_ExprContext).per_tuple_mcx()) };
    }

    // Lane-v2 agg-breaker fold plan (lanefold crate), classified once at
    // init — only when the lane can ever engage this node (env-gated; hashed,
    // plain, or sorted, single set, no sorted transitions, subplan- and
    // param-free transition program — the lane's own admission gate re-checks
    // the rest per call). AGG_SORTED joins for the sorted-fold arm
    // (lanev2 `try_own_sorted_agg_over_seq_scan`): its per-group-run fold
    // targets the same fixed `pergroup_base` the plain fold does.
    let lanefold = if lane_v2_enabled()
        && lane_v2_grouponly_enabled()
        && gs.is_none()
        && node.aggstrategy == AGG_HASHED
        && numtrans == 0
    {
        // SE-GROUPONLY: a grouping-only hashed build (zero transitions) —
        // ordered FIRST because a zero-transition evaltrans still exists as
        // an (empty) program, which would send the shape into the classify
        // arm below and out through its empty-spec refusal. The "fold" is
        // vacuous (`lanefold::empty_plan` — no trans, no lane columns); the
        // lane's value is the staged/batched GROUP PROBE (K2 / dict-group /
        // compact single-text / Mk feeds) replacing the per-row
        // TupleHashTable lookup, and the retrieve emits reconstructed keys
        // with an empty finalize loop. Zero-trans discipline downstream:
        // pergroup pointers are DANGLING sentinels (never dereferenced —
        // the empty plan folds nothing, peragg is empty at finalize), the
        // compact tables carry 0-byte state rows, and the compact→C
        // backstop migrate skips its state copy.
        Some(LaneFold { plan: ::lanefold::empty_plan(mcx), resid: None })
    } else if lane_v2_enabled()
        && gs.is_none()
        && (node.aggstrategy == AGG_HASHED
            || node.aggstrategy == AGG_PLAIN
            || node.aggstrategy == AGG_SORTED)
        && pertrans_sort.is_empty()
        && evaltrans
            .as_deref()
            .is_some_and(|et| !et.has_subplan() && et.param_exec_deps().is_empty())
    {
        match ::lanefold::classify(mcx, &specs) {
            Some(plan) => {
                let resid = if plan.resid.is_empty() {
                    None
                } else {
                    let mut keep: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, numtrans)?;
                    keep.resize(numtrans, false);
                    for &r in plan.resid.iter() {
                        keep[r] = true;
                    }
                    let mut prog = if node.aggstrategy == AGG_HASHED {
                        let base =
                            perhash.as_ref().expect("hashed Agg has perhash").pergroup_cell;
                        ::executils::with_subplan_compile_env(estate, |env| {
                            ::execexpr::exec_build_agg_trans_hashed_masked(
                                mcx, &specs, &keep, base, fm_agg_node, params, env,
                            )
                        })?
                    } else {
                        // AGG_PLAIN / AGG_SORTED: fixed pergroup targets
                        // (spec.pergroup = pergroup_base + transno), same as
                        // the full evaltrans both strategies build.
                        ::executils::with_subplan_compile_env(estate, |env| {
                            ::execexpr::exec_build_agg_trans_plain_masked(
                                mcx, &specs, &keep, fm_agg_node, params, env,
                            )
                        })?
                    };
                    // Same result-mcx discipline as the full evaltrans.
                    // SAFETY: the tmpcontext ExprContext outlives the program.
                    unsafe {
                        prog.arm_result_mcx_raw(estate.ecxt(tmpcontext).per_tuple_mcx())
                    };
                    Some(prog)
                };
                Some(LaneFold { plan, resid })
            }
            None => None,
        }
    } else {
        None
    };

    // Metadata-answer plan (lane-v2 metaagg arm, pgrcolumnar footer answers):
    // classified only under the SAME node-shape gate the lanefold plan
    // passed (lane on, single set, no sorted transitions, subplan- and
    // param-free program — `lanefold.is_some()` implies all of it), plus
    // AGG_PLAIN and a finalizing phase. classify_meta admits a subset of
    // classify's transitions and requires ALL of them, so a Some here
    // implies the lanefold plan is Some with an empty resid. skip_final
    // (partial-agg phase) is refused: exec_agg_meta writes finalize-ready
    // end states through the normal plain_finish tail, and partial plain
    // aggs only arise under Gather, whose parallel scans the meta scan
    // refuses anyway.
    let meta_aggs = if lanefold.is_some() && node.aggstrategy == AGG_PLAIN && !skip_final {
        ::lanefold::classify_meta(mcx, &specs)
    } else {
        None
    };

    // Narrow-sort + sorted-fold admission leg (field doc): probed wherever a
    // sorted-agg lane arm can engage (lane on, AGG_SORTED, real grouping
    // keys) — the narrow-sort arm needs it for internal-sort entries, the
    // sorted-fold arm (lanev2 sorted-agg over pgrcolumnar SeqScan) for its
    // raw-datum group-boundary compare.
    let group_eq_representational = if lane_v2_enabled()
        && node.aggstrategy == AGG_SORTED
        && node.numCols > 0
    {
        let mut ok = true;
        for (i, &op) in node.grpOperators.iter().enumerate() {
            const F_BOOLEQ: Oid = 60;
            const F_INT2EQ: Oid = 63;
            const F_INT4EQ: Oid = 65;
            const F_TEXTEQ: Oid = 67;
            const F_INT8EQ: Oid = 467;
            ok &= match lsyscache::get_opcode(op)? {
                F_BOOLEQ | F_INT2EQ | F_INT4EQ | F_INT8EQ => true,
                // Text keys serve only the narrow-sort arm (the sorted-fold
                // arm's raw compare is by-value-width only), so probe the
                // collation only where that arm can engage — and unit
                // harnesses without the collation syscache seam never build
                // internal-sort entries, so they never reach the lookup.
                F_TEXTEQ if !pertrans_sort.is_empty() => {
                    let coll = node.grpCollations[i];
                    coll != 0 && lsyscache::get_collation_isdeterministic(coll)?
                }
                _ => false,
            };
            if !ok {
                break;
            }
        }
        ok
    } else {
        false
    };

    let avgpack_shape_mask = sink::sink_avgpack_shape_mask(&peragg);
    Ok(AggStateData {
        plan: node,
        ps_ExprContext,
        tmpcontext,
        agg_node,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        proj,
        evaltrans,
        peragg,
        trans_init,
        trans_typ,
        _pergroup: pergroup,
        pergroup_base,
        agg_values_base,
        agg_nulls_base,
        agg_done: false,
        skip_final,
        numtrans,
        avgpack_shape_mask,
        perhash,
        merge,
        persort,
        gsets: gs,
        pertrans_sort,
        force_distinct_set: false,
        group_eq_representational,
        trans_order_insensitive: (0..numtrans)
            .all(|t| order_insensitive_exact_transfn(trans_fnoid[t])),
        qual,
        lanefold,
        meta_aggs,
        instr_idx: None,
        hash_build_combined: false,
        hashgroup: None,
        codedgroup: None,
        sink_emit: None,
        pdemit: None,
        sorted_sink_emit: None,
    })
}

/// Transition functions whose result is byte-identical for ANY input order —
/// pure counting and exact integer / Int128 accumulation (the same family
/// `distinct_set_kind` admits, plus `int8inc` = count(*)). No floats (fp
/// addition is order-sensitive), no min/max (a collation-equal tie could
/// return a byte-different representative), no by-ref accumulators outside
/// the exact-integer family.
fn order_insensitive_exact_transfn(transfn_oid: Oid) -> bool {
    const F_INT8INC: Oid = 1219; // count(*)
    const F_INT8INC_ANY: Oid = 2804; // count(x)
    const F_INT2_SUM: Oid = 1840;
    const F_INT4_SUM: Oid = 1841;
    const F_INT2_AVG_ACCUM: Oid = 1962;
    const F_INT4_AVG_ACCUM: Oid = 1963;
    const F_INT8_AVG_ACCUM: Oid = 2746; // sum(int8) / avg(int8)
    matches!(
        transfn_oid,
        F_INT8INC
            | F_INT8INC_ANY
            | F_INT2_SUM
            | F_INT4_SUM
            | F_INT2_AVG_ACCUM
            | F_INT4_AVG_ACCUM
            | F_INT8_AVG_ACCUM
    )
}

// The AGG_SORTED half of ExecInitAgg: outer-format slots + the grouping-
// boundary program (execTuplesMatchPrepare -> ExecBuildGroupingEqual).
fn init_persort<'mcx>(
    node: &'mcx Agg<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PerSortData<'mcx>> {
    let mcx = estate.es_query_cxt;
    let outer_plan = node
        .plan
        .lefttree
        .and_then(Node::as_plan)
        .unwrap_or_else(|| panic!("ExecInitAgg (nodeAgg.c): Agg without an outer plan"));
    let outer_desc = execscan::exec_type_from_tl(mcx, &outer_plan.targetlist)?;

    let num_cols = node.numCols as usize;
    debug_assert!(node.grpColIdx.len() == num_cols && node.grpOperators.len() == num_cols);
    let eq = if num_cols > 0 {
        let mut eqfuncoids: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, num_cols)?;
        for &op in node.grpOperators {
            eqfuncoids.push(lsyscache::get_opcode(op)?);
        }
        Some(::execexpr::exec_build_grouping_equal(
            mcx,
            &outer_desc,
            &outer_desc,
            node.grpColIdx,
            &eqfuncoids,
            node.grpCollations,
        )?)
    } else {
        None
    };
    let first_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(outer_desc.clone()));
    let pending_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(outer_desc));
    Ok(PerSortData { first_slot, pending_slot, eq, have_pending: false })
}

// find_cols (nodeAgg.c): outer columns referenced outside aggregate args.
fn collect_base_var_cols(node: Node<'_>, out: &mut PgVec<'_, bool>) {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().unwrap();
            assert!(v.varattno >= 1 && (v.varattno as usize) <= out.len());
            out[(v.varattno - 1) as usize] = true;
        }
        NodeTag::T_Const | NodeTag::T_Aggref | NodeTag::T_GroupingFunc => {}
        NodeTag::T_TargetEntry => {
            collect_base_var_cols(node.as_target_entry().unwrap().expr, out)
        }
        NodeTag::T_FuncExpr => {
            for a in node.as_func_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_OpExpr => {
            for a in node.as_op_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_NullIfExpr => {
            for a in node.as_null_if_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_Param
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_NextValueExpr
        | NodeTag::T_CoerceToDomainValue => {}
        NodeTag::T_RelabelType => collect_base_var_cols(node.as_relabel_type().unwrap().arg, out),
        NodeTag::T_CoerceViaIO => collect_base_var_cols(node.as_coerce_via_io().unwrap().arg, out),
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            collect_base_var_cols(a.arg, out);
            if let Some(e) = a.elemexpr {
                collect_base_var_cols(e, out);
            }
        }
        NodeTag::T_ConvertRowtypeExpr => {
            collect_base_var_cols(node.as_convert_rowtype_expr().unwrap().arg, out)
        }
        NodeTag::T_CoerceToDomain => {
            collect_base_var_cols(node.as_coerce_to_domain().unwrap().arg, out)
        }
        NodeTag::T_BoolExpr => {
            for a in node.as_bool_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_NullTest => {
            if let Some(a) = node.as_null_test().unwrap().arg {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_BooleanTest => {
            if let Some(a) = node.as_boolean_test().unwrap().arg {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_DistinctExpr => {
            for a in node.as_distinct_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for a in node.as_scalar_array_op_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_ArrayExpr => {
            for e in node.as_array_expr().unwrap().elements.iter() {
                collect_base_var_cols(e, out);
            }
        }
        NodeTag::T_RowExpr => {
            for a in node.as_row_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            for a in rc.largs.iter().chain(rc.rargs.iter()) {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(a) = c.arg {
                collect_base_var_cols(a, out);
            }
            for w in c.args.iter() {
                let cw = w.as_case_when().expect("CaseWhen");
                collect_base_var_cols(cw.expr.expect("CaseWhen.expr"), out);
                collect_base_var_cols(cw.result.expect("CaseWhen.result"), out);
            }
            if let Some(d) = c.defresult {
                collect_base_var_cols(d, out);
            }
        }
        NodeTag::T_CoalesceExpr => {
            for a in node.as_coalesce_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_MinMaxExpr => {
            for a in node.as_min_max_expr().unwrap().args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                collect_base_var_cols(e, out);
            }
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for a in c.args.iter() {
                collect_base_var_cols(a, out);
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                collect_base_var_cols(e, out);
            }
        }
        NodeTag::T_JsonIsPredicate => {
            if let Some(e) = node.as_json_is_predicate().unwrap().expr {
                collect_base_var_cols(e, out);
            }
        }
        // C expression_tree_walker: SubPlan walks testexpr + args (args carry
        // the per-row correlated exprs, e.g. an outer-level agg's Aggref).
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            if let Some(te) = sp.testexpr {
                collect_base_var_cols(te, out);
            }
            for a in sp.args.iter() {
                collect_base_var_cols(a, out);
            }
        }
        NodeTag::T_AlternativeSubPlan => {
            for sp in node.as_alternative_sub_plan().unwrap().subplans.iter() {
                collect_base_var_cols(sp, out);
            }
        }
        tag => panic!("find_cols (nodeAgg.c): node family {tag:?} not ported"),
    }
}

// find_hash_columns + build_hash_tables (nodeAgg.c), single grouping set.
fn init_perhash<'mcx>(
    node: &'mcx Agg<'mcx>,
    estate: &mut EStateData<'mcx>,
    numtrans: usize,
) -> PgResult<PerHashData<'mcx>> {
    let mcx = estate.es_query_cxt;
    let outer_plan = node
        .plan
        .lefttree
        .and_then(Node::as_plan)
        .unwrap_or_else(|| panic!("ExecInitAgg (nodeAgg.c): Agg without an outer plan"));
    let outer_tlist = &outer_plan.targetlist;
    let outer_natts = outer_tlist.len();
    let num_cols = node.numCols as usize;
    assert!(
        num_cols > 0 && node.grpColIdx.len() == num_cols,
        "init_perhash: numCols {} grpColIdx.len {} strategy {} gsets {}",
        num_cols,
        node.grpColIdx.len(),
        node.aggstrategy,
        node.groupingSets.len()
    );
    assert!(node.numGroups > 0, "Agg.numGroups unset (planner must estimate it)");

    let mut base_cols: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, outer_natts)?;
    base_cols.resize(outer_natts, false);
    for tle in node.plan.targetlist.iter() {
        collect_base_var_cols(tle, &mut base_cols);
    }
    for q in node.plan.qual.iter() {
        collect_base_var_cols(q, &mut base_cols);
    }

    // find_cols' colnos_needed: unaggregated + aggregated input columns.
    let mut colnos_needed: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, outer_natts)?;
    colnos_needed.resize(outer_natts, false);
    colnos_needed.copy_from_slice(&base_cols);
    for &attno in node.grpColIdx {
        colnos_needed[(attno - 1) as usize] = true;
    }
    {
        let mut aggrefs: PgVec<'mcx, (Node<'mcx>, &'mcx Aggref<'mcx>)> = PgVec::new_in(mcx);
        for tle in node.plan.targetlist.iter() {
            collect_aggrefs(tle, &mut aggrefs)?;
        }
        for q in node.plan.qual.iter() {
            collect_aggrefs(q, &mut aggrefs)?;
        }
        for &(_, aggref) in aggrefs.iter() {
            for a in aggref.args.iter() {
                collect_base_var_cols(a, &mut colnos_needed);
            }
            for a in aggref.aggdirectargs.iter() {
                collect_base_var_cols(a, &mut colnos_needed);
            }
            if let Some(f) = aggref.aggfilter {
                collect_base_var_cols(f, &mut colnos_needed);
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

    let mut hash_grp_col_idx_input: PgVec<'mcx, i16> =
        vec_with_capacity_in(mcx, outer_natts)?;
    for &attno in node.grpColIdx {
        hash_grp_col_idx_input.push(attno);
        base_cols[(attno - 1) as usize] = false;
    }
    for (i, &needed) in base_cols.iter().enumerate() {
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
    let outer_desc = execscan::exec_type_from_tl(mcx, outer_tlist)?;

    let (eqfuncoids, hashfunctions) =
        ::execgrouping::exec_tuples_hash_prepare(mcx, node.grpOperators)?;

    let additionalsize = numtrans * core::mem::size_of::<AggPerGroup>();
    let hashentrysize = hash_agg_entry_size(
        numtrans,
        outer_plan.plan_width.max(0) as usize,
        node.transitionSpace as usize,
    );
    let (mem_limit, hash_ngroups_limit, planned_partitions) =
        hash_agg_set_limits(hashentrysize, node.numGroups as f64, 0);
    estate.es_agg_instrumentation.push((
        node.plan.plan_node_id,
        ::types_core::instrument::AggregateInstrumentation {
            hash_batches_used: 1,
            hash_planned_partitions: planned_partitions as i32,
            ..Default::default()
        },
    ));
    let nbuckets = hash_choose_num_buckets(hashentrysize, node.numGroups, mem_limit);

    let mut key_col_idx: PgVec<'mcx, i16> = vec_with_capacity_in(mcx, num_cols)?;
    for i in 0..num_cols {
        key_col_idx.push((i + 1) as i16);
    }

    let hashtable = ::execgrouping::build_tuple_hash_table(
        mcx,
        &hash_desc,
        &key_col_idx,
        &eqfuncoids,
        &hashfunctions,
        node.grpCollations,
        nbuckets,
        additionalsize,
        // C build_hash_table: DO_AGGSPLIT_SKIPFINAL(aggsplit) — partial aggs
        // (each parallel participant, leader included) get a per-worker hash
        // IV so their bucket-order EMISSION doesn't feed the finalize's
        // identically-mapped table in hash order (finalize-corr lane: that
        // correlation cost 104e9 probes / ~500s on a decision-support big-group finalize).
        node.aggsplit & AGGSPLITOP_SKIPFINAL != 0,
    )?;
    let hashslot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(hash_desc.clone()));
    let retrieve_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(hash_desc));
    let first_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(outer_desc.clone()));
    let rslot = exectuples::make_tuple_table_slot(
        mcx,
        TupleSlotKind::MinimalTuple,
        Some(outer_desc.clone()),
    );
    let wslot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(outer_desc));
    let table_ctx = mcx.context().new_child_bump("HashAgg table context");
    let tmp_ctx = mcx.context().new_child_bump("HashAgg spill tuple");

    let cell_layout = Layout::new::<NonNull<AggPerGroup>>();
    let raw = mcx.allocate(cell_layout).map_err(|_| mcx.oom(cell_layout.size()))?;
    let pergroup_cell: NonNull<NonNull<AggPerGroup>> = raw.cast();
    // SAFETY: fresh allocation of the cell's exact layout; repointed before
    // every evaltrans run (lookup_hash_entry).
    unsafe { pergroup_cell.write(NonNull::dangling()) };

    Ok(PerHashData {
        hashtable,
        hashslot,
        retrieve_slot,
        first_slot,
        num_cols,
        hash_grp_col_idx_input,
        largest_grp_col_idx,
        outer_natts,
        pergroup_cell,
        hash_ngroups_limit,
        hash_ngroups_current: 0,
        hash_mem_limit: mem_limit,
        table_filled: false,
        compact: None,
        exchange: merge::ExchangeState::Unresolved,
        sink_cap: None,
        sink_spill_ok: false,
        hashiter: 0,
        table_ctx,
        spill: HashSpillState {
            mode: false,
            ever_spilled: false,
            tapeset: None,
            spill: None,
            batches: PgVec::new_in(mcx),
            all_cols_needed,
            max_colno_needed,
            colnos_needed,
            rslot,
            wslot,
            read_buf: PgVec::new_in(mcx),
            tmp_ctx,
            input_card: node.numGroups as f64,
            used_bits: 0,
            hashentrysize,
        },
    })
}

const SIZEOF_MINIMAL_TUPLE_HEADER: usize = 15;
// C: sizeof(MemoryChunk) = 8 in production builds (memutils_memorychunk.h).
const CHUNKHDRSZ: usize = 8;

const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

/// C `hash_agg_entry_size` (nodeAgg.c).
pub fn hash_agg_entry_size(num_trans: usize, tuple_width: usize, transition_space: usize) -> f64 {
    let tuple_size = maxalign(SIZEOF_MINIMAL_TUPLE_HEADER) + tuple_width;
    let tuple_chunk_size = maxalign(tuple_size);
    let pergroup_chunk_size = num_trans * core::mem::size_of::<AggPerGroup>();
    let transition_chunk_size = if transition_space > 0 {
        CHUNKHDRSZ + transition_space.next_power_of_two()
    } else {
        0
    };
    (16 + tuple_chunk_size + pergroup_chunk_size + transition_chunk_size) as f64
}

const HASHAGG_PARTITION_FACTOR: f64 = 1.50;
const HASHAGG_MIN_PARTITIONS: f64 = 4.0;
const HASHAGG_MAX_PARTITIONS: f64 = 1024.0;
const HASHAGG_READ_BUFFER_SIZE: f64 = 8192.0;
const HASHAGG_WRITE_BUFFER_SIZE: f64 = 8192.0;

// C my_log2 (dynahash.c): ceil(log2(num)).
fn my_log2(num: i64) -> u32 {
    if num <= 1 {
        return 0;
    }
    64 - ((num - 1) as u64).leading_zeros()
}

/// C `hash_choose_num_partitions` (nodeAgg.c) -> (npartitions,
/// partition_bits).
fn hash_choose_num_partitions(
    input_groups: f64,
    hashentrysize: f64,
    used_bits: u32,
) -> (usize, u32) {
    let hash_mem_limit = ::execgrouping::get_hash_memory_limit() as f64;
    let partition_limit =
        (hash_mem_limit * 0.25 - HASHAGG_READ_BUFFER_SIZE) / HASHAGG_WRITE_BUFFER_SIZE;
    let mem_wanted = HASHAGG_PARTITION_FACTOR * input_groups * hashentrysize;
    let mut dpartitions = 1.0 + (mem_wanted / hash_mem_limit);
    if dpartitions > partition_limit {
        dpartitions = partition_limit;
    }
    dpartitions = dpartitions.clamp(HASHAGG_MIN_PARTITIONS, HASHAGG_MAX_PARTITIONS);
    let mut partition_bits = my_log2(dpartitions as i64);
    if partition_bits + used_bits >= 32 {
        partition_bits = 32 - used_bits;
    }
    (1usize << partition_bits, partition_bits)
}

/// C `hash_choose_num_buckets` (nodeAgg.c).
fn hash_choose_num_buckets(hashentrysize: f64, ngroups: i64, memory: usize) -> usize {
    let max_nbuckets = ((memory as f64 / hashentrysize) as usize) >> 1;
    (ngroups.max(0) as usize).min(max_nbuckets).max(1)
}

/// C `hash_agg_set_limits` (nodeAgg.c) -> (mem_limit, ngroups_limit,
/// num_partitions).
pub fn hash_agg_set_limits(
    hashentrysize: f64,
    input_groups: f64,
    used_bits: u32,
) -> (usize, u64, usize) {
    let hash_mem_limit = ::execgrouping::get_hash_memory_limit();
    if input_groups * hashentrysize <= hash_mem_limit as f64 {
        return (hash_mem_limit, (hash_mem_limit as f64 / hashentrysize) as u64, 0);
    }
    let (npartitions, _) = hash_choose_num_partitions(input_groups, hashentrysize, used_bits);
    let partition_mem =
        (HASHAGG_READ_BUFFER_SIZE + HASHAGG_WRITE_BUFFER_SIZE * npartitions as f64) as usize;
    let mem_limit = if hash_mem_limit > 4 * partition_mem {
        hash_mem_limit - partition_mem
    } else {
        (hash_mem_limit as f64 * 0.75) as usize
    };
    let ngroups_limit =
        if mem_limit as f64 > hashentrysize { (mem_limit as f64 / hashentrysize) as u64 } else { 1 };
    (mem_limit, ngroups_limit, npartitions)
}

const HASHAGG_HLL_BIT_WIDTH: u8 = 5;

// PGRUST_HASHAGG_MEMDEBUG diagnostics: accounted components vs kernel RSS at
// spill-mode entry and every batch boundary. Off (one cached env probe) on
// production paths.
fn hashagg_memdebug_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("PGRUST_HASHAGG_MEMDEBUG").is_some())
}

// (vmrss, anon, shmem, hwm) in kB from /proc/self/status; zeros off-Linux.
fn hashagg_vm_kb() -> (u64, u64, u64, u64) {
    let mut rss = 0u64;
    let mut hwm = 0u64;
    let mut anon = 0u64;
    let mut shmem = 0u64;
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for l in s.lines() {
            let kb = |v: &str| v.trim().trim_end_matches("kB").trim().parse().unwrap_or(0);
            if let Some(v) = l.strip_prefix("VmRSS:") {
                rss = kb(v);
            } else if let Some(v) = l.strip_prefix("VmHWM:") {
                hwm = kb(v);
            } else if let Some(v) = l.strip_prefix("RssAnon:") {
                anon = kb(v);
            } else if let Some(v) = l.strip_prefix("RssShmem:") {
                shmem = kb(v);
            }
        }
    }
    (rss, anon, shmem, hwm)
}

// release_retained + proof-of-execution prints under PGRUST_HASHAGG_MEMDEBUG:
// installed?, and anon RSS before/after the collect.
fn hashagg_release_retained(tag: &str) {
    if !hashagg_memdebug_enabled() {
        ::mcx::release_retained();
        return;
    }
    let (rb, ab, ..) = hashagg_vm_kb();
    let installed = ::mcx::release_retained();
    let (ra, aa, ..) = hashagg_vm_kb();
    eprintln!(
        "HASHAGG_MEMDEBUG release_retained {tag}: installed={installed} rss_kb {rb}->{ra} anon_kb {ab}->{aa}"
    );
}

#[cold]
#[inline(never)]
fn hashagg_memdebug(tag: &str, ph: &PerHashData<'_>, tval_mem: usize, buffer_mem: usize) {
    let (rss, anon, shmem, hwm) = hashagg_vm_kb();
    let meta = ph.hashtable.meta_mem();
    let entry = ph.table_ctx.subtree_used();
    eprintln!(
        "HASHAGG_MEMDEBUG {tag}: ngroups={} meta_kb={} table_ctx_kb={} aggctx_kb={} bufs_kb={} accounted_kb={} vmrss_kb={rss} anon_kb={anon} shmem_kb={shmem} vmhwm_kb={hwm} nbatches_pending={} limit_kb={}",
        ph.hash_ngroups_current,
        meta / 1024,
        entry / 1024,
        tval_mem / 1024,
        buffer_mem / 1024,
        (meta + entry + tval_mem + buffer_mem) / 1024,
        ph.spill.batches.len(),
        ph.hash_mem_limit / 1024,
    );
    static NCALL: core::sync::atomic::AtomicU32 = core::sync::atomic::AtomicU32::new(0);
    let n = NCALL.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    if n < 4 || n % 16 == 0 {
        let mut total = 0usize;
        for t in ::mcxt_stats::backend_context_forest() {
            hashagg_memdebug_tree(&t, 1, &mut total);
        }
        eprintln!("HASHAGG_MEMDEBUG forest_total_foot_kb={}", total / 1024);
    }
}

fn hashagg_memdebug_tree(t: &::mcx::TreeStats, level: usize, total: &mut usize) {
    *total += t.arena_footprint;
    if t.subtree_used >= 256 * 1024 || t.arena_footprint >= 256 * 1024 {
        eprintln!(
            "HASHAGG_MEMDEBUG ctx l{level} {}{} [{}] used_kb={} foot_kb={} subtree_used_kb={} nblocks={}",
            t.name,
            t.ident.as_deref().map(|i| format!(": {i}")).unwrap_or_default(),
            t.kind,
            t.used / 1024,
            t.arena_footprint / 1024,
            t.subtree_used / 1024,
            t.nblocks,
        );
    }
    for c in &t.children {
        hashagg_memdebug_tree(c, level + 1, total);
    }
}

// hash_agg_check_limits + hash_agg_enter_spill_mode (nodeAgg.c). Divergence:
// no nullcheck recompile — on a spill-mode miss the caller skips the whole
// transition program for the row (single-set equivalent). C's eager spill
// init in enter_spill_mode is lazy here (first spilled tuple), same inputs.
fn hash_agg_check_limits<'mcx>(
    ph: &mut PerHashData<'mcx>,
    aggctx: ::mcx::Mcx<'_>,
    mcx: ::mcx::Mcx<'mcx>,
) -> PgResult<()> {
    let ngroups = ph.hash_ngroups_current;
    let meta_mem = ph.hashtable.meta_mem();
    let entry_mem = ph.table_ctx.subtree_used();
    let tval_mem = aggctx.context().subtree_used();
    let total_mem = meta_mem + entry_mem + tval_mem;
    if ngroups > 0 && (total_mem > ph.hash_mem_limit || ngroups > ph.hash_ngroups_limit) {
        ph.spill.mode = true;
        if !ph.spill.ever_spilled {
            ph.spill.ever_spilled = true;
            ph.spill.tapeset = Some(LogicalTapeSet::create(mcx, true)?);
        }
        // Allocator hygiene, not a C step: mimalloc retains freed segments,
        // and the spill pass's grow/free churn would otherwise hold
        // batch-sized RSS to query end. The pass is disk-bound; the collect
        // cost hides.
        hashagg_release_retained("enter_spill");
        if hashagg_memdebug_enabled() {
            hashagg_memdebug("enter_spill_mode", ph, tval_mem, 0);
        }
    }
    Ok(())
}

// initialize_hash_entry (nodeAgg.c): count the group, maybe enter spill
// mode, then seed the entry's pergroup array. Per new group, off the per-row
// path — outlined to keep lookup_hash_entry's fill loop lean.
#[inline(never)]
fn initialize_hash_entry<'mcx>(
    ph: &mut PerHashData<'mcx>,
    trans_init: &[NullableDatum],
    trans_typ: &[TransTyp],
    agg_node: NonNull<AggStateNode>,
    ix: u32,
    mcx: ::mcx::Mcx<'mcx>,
) -> PgResult<()> {
    ph.hash_ngroups_current += 1;
    // SAFETY: read of the once-allocated node; no &mut is live to it.
    let aggctx = unsafe { agg_node.as_ref() }.aggcontext();
    hash_agg_check_limits(ph, aggctx, mcx)?;
    if trans_init.is_empty() {
        return Ok(());
    }
    let pergroup = ph
        .hashtable
        .entry_additional(ix)
        .expect("numtrans > 0 tables carry additional space")
        .cast::<AggPerGroup>();
    for (transno, init) in trans_init.iter().enumerate() {
        let typ = trans_typ[transno];
        let value = if !init.isnull && !typ.byval {
            // SAFETY: node-lifetime initval datum copied into the aggcontext
            // (C initialize_aggregate's datumCopy in curaggcontext memory).
            unsafe { ::execexpr::agg_datum_copy(aggctx, init.value, typ.len)? }
        } else {
            init.value
        };
        // SAFETY: the entry's additional block holds numtrans AggPerGroup
        // slots, zeroed at creation (execgrouping contract).
        unsafe {
            pergroup.as_ptr().add(transno).write(AggPerGroup {
                trans_value: value,
                trans_value_is_null: init.isnull,
                no_trans_value: init.isnull,
            });
        }
    }
    // SAFETY: the cell is a once-allocated live slot the trans steps read.
    unsafe { ph.pergroup_cell.write(pergroup) };
    Ok(())
}

// hashagg_spill_init (nodeAgg.c).
#[cold]
#[inline(never)]
fn hashagg_spill_init<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    tapeset: &mut LogicalTapeSet<'mcx>,
    used_bits: u32,
    input_groups: f64,
    hashentrysize: f64,
) -> PgResult<HashAggSpill<'mcx>> {
    let (npartitions, partition_bits) =
        hash_choose_num_partitions(input_groups, hashentrysize, used_bits);
    let mut partitions: PgVec<'mcx, TapeIdx> = vec_with_capacity_in(mcx, npartitions)?;
    for _ in 0..npartitions {
        partitions.push(tapeset.create_tape());
    }
    let mut ntuples: PgVec<'mcx, i64> = vec_with_capacity_in(mcx, npartitions)?;
    ntuples.resize(npartitions, 0);
    let mut hll_card: PgVec<'mcx, HyperLogLog32> = PgVec::new_in(mcx);
    hll_card
        .try_reserve(npartitions)
        .map_err(|_| mcx.oom(npartitions * core::mem::size_of::<HyperLogLog32>()))?;
    for _ in 0..npartitions {
        hll_card.push(HyperLogLog32::new(HASHAGG_HLL_BIT_WIDTH));
    }
    let shift = 32 - used_bits as i32 - partition_bits as i32;
    let mask = if shift < 32 { ((npartitions - 1) as u32) << shift } else { 0 };
    Ok(HashAggSpill { npartitions, partitions, ntuples, hll_card, mask, shift })
}

// hashagg_spill_tuple (nodeAgg.c); `input` None = the batch rslot (refill).
// Cold from the in-memory fill's view; the spill passes are IO-bound.
#[cold]
#[inline(never)]
fn hashagg_spill_tuple<'mcx>(
    ss: &mut HashSpillState<'mcx>,
    input: Option<&mut SlotData<'mcx>>,
    hash: u32,
    mcx: ::mcx::Mcx<'mcx>,
) -> PgResult<()> {
    let HashSpillState {
        spill,
        tapeset,
        wslot,
        rslot,
        all_cols_needed,
        max_colno_needed,
        colnos_needed,
        tmp_ctx,
        input_card,
        used_bits,
        hashentrysize,
        ..
    } = ss;
    let tapeset = tapeset.as_mut().expect("spill mode has a tapeset");
    if spill.is_none() {
        *spill = Some(hashagg_spill_init(mcx, tapeset, *used_bits, *input_card, *hashentrysize)?);
    }
    let spill = spill.as_mut().unwrap();
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

#[track_caller]
#[cold]
#[inline(never)]
fn tape_eof_error(requested: usize, got: usize) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "unexpected EOF for hashagg batch tape: requested {requested} bytes, read {got} bytes"
    )))
}

// hashagg_batch_read (nodeAgg.c): None = tape exhausted.
fn hashagg_batch_read(
    tapeset: &mut LogicalTapeSet<'_>,
    tape: TapeIdx,
    read_buf: &mut PgVec<'_, u64>,
) -> PgResult<Option<u32>> {
    let mut word = [0u8; 4];
    let n = tapeset.read(tape, &mut word)?;
    if n == 0 {
        return Ok(None);
    }
    if n != 4 {
        return Err(tape_eof_error(4, n));
    }
    let hash = u32::from_ne_bytes(word);
    let n = tapeset.read(tape, &mut word)?;
    if n != 4 {
        return Err(tape_eof_error(4, n));
    }
    let t_len = u32::from_ne_bytes(word) as usize;
    assert!(t_len >= 4, "hashagg batch tuple shorter than its length word");
    read_buf.clear();
    read_buf.resize(t_len.div_ceil(8), 0);
    // SAFETY: t_len <= the freshly-sized buffer's bytes.
    let bytes =
        unsafe { core::slice::from_raw_parts_mut(read_buf.as_mut_ptr().cast::<u8>(), t_len) };
    bytes[..4].copy_from_slice(&(t_len as u32).to_ne_bytes());
    let n = tapeset.read(tape, &mut bytes[4..])?;
    if n != t_len - 4 {
        return Err(tape_eof_error(t_len - 4, n));
    }
    Ok(Some(hash))
}

// hashagg_spill_finish (nodeAgg.c).
fn hashagg_spill_finish<'mcx>(
    ss: &mut HashSpillState<'mcx>,
    spill: HashAggSpill<'mcx>,
    batches_used: &mut i32,
) -> PgResult<()> {
    let used_bits = (32 - spill.shift) as u32;
    let tapeset = ss.tapeset.as_mut().expect("spill has a tapeset");
    for i in 0..spill.npartitions {
        if spill.ntuples[i] == 0 {
            continue;
        }
        let cardinality = spill.hll_card[i].estimate();
        tapeset.rewind_for_read(spill.partitions[i], HASHAGG_READ_BUFFER_SIZE as usize)?;
        ss.batches.push(HashAggBatch {
            input_tape: spill.partitions[i],
            used_bits,
            input_card: cardinality,
        });
        *batches_used += 1;
    }
    Ok(())
}

// hashagg_finish_initial_spills (nodeAgg.c).
fn hashagg_finish_initial_spills<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let id = node.plan.plan.plan_node_id;
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    let mut total_npartitions = 0usize;
    if let Some(spill) = ph.spill.spill.take() {
        total_npartitions = spill.npartitions;
        let ai = agg_instrumentation(estate, id);
        hashagg_spill_finish(&mut ph.spill, spill, &mut ai.hash_batches_used)?;
    }
    hash_agg_update_metrics(node, estate, false, total_npartitions);
    node.perhash.as_mut().unwrap().spill.mode = false;
    Ok(())
}

// hashagg_reset_spill_state (nodeAgg.c); the lazy-init parameters go back
// to the initial pass's (C passes them fresh at each spill site).
fn hashagg_reset_spill_state(ph: &mut PerHashData<'_>, input_card: f64) {
    let ss = &mut ph.spill;
    ss.spill = None;
    ss.batches.clear();
    if let Some(ts) = ss.tapeset.take() {
        ts.close().expect("hashagg tapeset close");
    }
    ss.input_card = input_card;
    ss.used_bits = 0;
    if ph.spill.ever_spilled {
        // A finished spill pass leaves batch-sized freed segments retained
        // by mimalloc; release them so post-query RSS returns to baseline.
        hashagg_release_retained("spill_teardown");
    }
}

fn agg_instrumentation<'a>(
    estate: &'a mut EStateData<'_>,
    id: i32,
) -> &'a mut ::types_core::instrument::AggregateInstrumentation {
    estate
        .es_agg_instrumentation
        .iter_mut()
        .find_map(|(i, ai)| (*i == id).then_some(ai))
        .expect("init_perhash published this node's metrics")
}

// agg_refill_hash_table (nodeAgg.c): false = input exhausted.
fn agg_refill_hash_table<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let batch = {
        let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
        let Some(batch) = ph.spill.batches.pop() else {
            return Ok(false);
        };
        let (mem_limit, ngroups_limit, _) =
            hash_agg_set_limits(ph.spill.hashentrysize, batch.input_card, batch.used_bits);
        ph.hash_mem_limit = mem_limit;
        ph.hash_ngroups_limit = ngroups_limit;
        ph.hashtable.reset();
        ph.table_ctx.reset();
        ph.hash_ngroups_current = 0;
        ph.spill.input_card = batch.input_card;
        ph.spill.used_bits = batch.used_bits;
        debug_assert!(ph.spill.spill.is_none());
        batch
    };
    // SAFETY: sole access path to the node during the reset (C's
    // ReScanExprContext(hashcontext)).
    unsafe { node.agg_node.as_mut() }.reset();
    // Batch boundary just freed up to a full hash_mem of table memory;
    // release mimalloc's retained segments before the next fill (disk-bound
    // here, so the collect cost hides).
    hashagg_release_retained("refill_batch");

    loop {
        // C's loop-top CHECK_FOR_INTERRUPTS (agg_refill_hash_table has no
        // child node to supply cancel points).
        if init_small::globals::InterruptPending() {
            postgres_seams::check_for_interrupts::call()?;
        }
        let advance = {
            let AggStateData { perhash, trans_init, trans_typ, agg_node, .. } = node;
            let ph = perhash.as_mut().unwrap();
            let got = hashagg_batch_read(
                ph.spill.tapeset.as_mut().expect("batches imply a tapeset"),
                batch.input_tape,
                &mut ph.spill.read_buf,
            )?;
            let Some(hash) = got else {
                break;
            };
            let tup = NonNull::new(ph.spill.read_buf.as_mut_ptr().cast::<MinimalTupleData>())
                .expect("read_buf is non-null");
            // SAFETY: the image stays live in read_buf until the next read.
            unsafe { exectuples::exec_store_minimal_tuple_ptr(&mut ph.spill.rslot, mcx, tup) };
            {
                let PerHashData {
                    hashslot, hash_grp_col_idx_input, largest_grp_col_idx, spill, ..
                } = &mut *ph;
                prepare_hash_slot(
                    hashslot,
                    hash_grp_col_idx_input,
                    *largest_grp_col_idx,
                    &mut spill.rslot,
                    mcx,
                );
            }
            let table_mcx = ph.table_ctx.mcx();
            let use_table = !ph.spill.mode;
            let (ix, isnew) = ph.hashtable.lookup(
                &mut ph.hashslot,
                hash,
                use_table.then_some(table_mcx),
                mcx,
            )?;
            match ix {
                Some(ix) => {
                    if isnew {
                        initialize_hash_entry(ph, trans_init, trans_typ, *agg_node, ix, mcx)?;
                    } else if !trans_init.is_empty() {
                        let pergroup = ph
                            .hashtable
                            .entry_additional(ix)
                            .expect("numtrans > 0 tables carry additional space")
                            .cast::<AggPerGroup>();
                        // SAFETY: once-allocated live cell the trans steps read.
                        unsafe { ph.pergroup_cell.write(pergroup) };
                    }
                    true
                }
                None => {
                    hashagg_spill_tuple(&mut ph.spill, None, hash, mcx)?;
                    false
                }
            }
        };
        if advance {
            let tmpcontext = node.tmpcontext;
            let AggStateData { perhash, evaltrans, .. } = node;
            let ph = perhash.as_mut().unwrap();
            let et = evaltrans.as_mut().unwrap();
            if et.has_subplan() {
                ::executils::exec_eval_expr_with_subplans_outer(
                    et,
                    &mut ph.spill.rslot,
                    estate,
                    tmpcontext,
                )?;
            } else {
                let mut slots =
                    EvalSlots { scan: None, inner: None, outer: Some(&mut ph.spill.rslot) };
                exec_eval_expr(et, &mut slots)?;
            }
        }
        estate.reset_expr_context(node.tmpcontext);
    }

    let id = node.plan.plan.plan_node_id;
    let ph = node.perhash.as_mut().unwrap();
    ph.spill.tapeset.as_mut().unwrap().close_tape(batch.input_tape);
    let spilled = ph.spill.spill.take();
    let npartitions = spilled.as_ref().map_or(0, |s| s.npartitions);
    if let Some(spill) = spilled {
        let ai = agg_instrumentation(estate, id);
        hashagg_spill_finish(&mut ph.spill, spill, &mut ai.hash_batches_used)?;
    }
    hash_agg_update_metrics(node, estate, true, npartitions);
    let ph = node.perhash.as_mut().unwrap();
    ph.spill.mode = false;
    ph.hashiter = 0;
    Ok(true)
}

// initialize_aggregate (nodeAgg.c) sortstate restart, one grouping set.
// `force` = the node's `force_distinct_set` (grouping-sets callers pass
// false — set-mode is never admitted there).
pub(crate) fn restart_pertrans_sortstates(
    pertrans_sort: &mut [PerTransSortData<'_>],
    setno: usize,
    force: bool,
) -> PgResult<()> {
    for ps in pertrans_sort.iter_mut() {
        if ps.set_active(force) {
            // Set-mode entry (distinctset.rs): the group boundary clears the
            // set (allocation kept for the next group) instead of restarting
            // a tuplesort. A degraded group's leftover sortstate (rescan
            // before the group finalized) ends here, exactly as the sort
            // path's restart would.
            debug_assert_eq!(setno, 0, "set-mode refuses grouping sets");
            if let Some(old) = ps.sortstates.get_mut(setno).and_then(|s| s.take()) {
                old.end();
            }
            if let Some(d) = ps.dset.as_mut() {
                d.clear();
            }
            ps.dset_degraded = false;
            continue;
        }
        if ps.presorted {
            continue;
        }
        if ps.sortstates.len() <= setno {
            ps.sortstates.resize_with(setno + 1, || None);
        }
        if let Some(old) = ps.sortstates[setno].take() {
            old.end();
        }
        let work_mem = init_small::globals::work_mem();
        ps.sortstates[setno] = Some(if ps.num_inputs == 1 {
            Tuplesort::begin_datum(
                ps.sortdesc.attr(0).atttypid,
                ps.sort_ops[0],
                ps.sort_collations[0],
                ps.sort_nulls_first[0],
                work_mem,
                TUPLESORT_NONE,
            )?
        } else {
            // SAFETY: lifetime erasure for the tuplesort API only; the sort
            // ends before the query context resets (group boundary, end,
            // rescan), so the desc outlives every access.
            let desc: Rc<TupleDescData<'static>> =
                unsafe { core::mem::transmute(ps.sortdesc.clone()) };
            Tuplesort::begin_heap(
                desc,
                &ps.sort_col_idx,
                &ps.sort_ops,
                &ps.sort_collations,
                &ps.sort_nulls_first,
                work_mem,
                TUPLESORT_NONE,
            )?
        });
    }
    Ok(())
}

// initialize_aggregates (nodeAgg.c); by-ref initvals datumCopy into the
// aggcontext. `estate` serves only the hash-grouped residual hook's text-key
// detoast (per-tuple memory).
fn initialize_aggregates<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &EStateData<'mcx>,
) -> PgResult<()> {
    restart_pertrans_sortstates(&mut node.pertrans_sort, 0, node.force_distinct_set)?;
    for (transno, init) in node.trans_init.iter().enumerate() {
        let typ = node.trans_typ[transno];
        let value = if !init.isnull && !typ.byval {
            // SAFETY: node-lifetime initval datum; agg_node is live, no &mut.
            unsafe {
                ::execexpr::agg_datum_copy(
                    node.agg_node.as_ref().aggcontext(),
                    init.value,
                    typ.len,
                )?
            }
        } else {
            init.value
        };
        // SAFETY: transno < the pergroup array's once-allocated length; the
        // base pointer is the sole access path (struct invariant).
        unsafe {
            node.pergroup_base.as_ptr().add(transno).write(AggPerGroup {
                trans_value: value,
                trans_value_is_null: init.isnull,
                no_trans_value: init.isnull,
            });
        }
    }
    // Hash-grouped-arm degrade residue (hashgrouped.rs): a beginning group
    // with saved partial state gets it installed OVER the fresh init — this
    // seam is shared by the lane emit chain and the C pull-loop fallback, so
    // both resume the degraded node identically. No-op unless the arm
    // degraded on this node.
    if node.hashgroup.is_some() {
        hashgrouped::residual_preload(node, estate)?;
    }
    Ok(())
}

// The tuplesort feed half of the ordered-trans steps: rows the program
// marked live park their args in scratch until here. Runs before the
// tmpcontext reset — by-ref scratch datums live in per-tuple memory.
pub(crate) fn collect_ordered_input<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    nsets: usize,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let tmp = node.tmpcontext;
    let AggStateData {
        pertrans_sort, trans_typ, agg_node, pergroup_base, force_distinct_set, ..
    } = node;
    for ps in pertrans_sort.iter_mut() {
        // SAFETY: once-allocated cells the trans program writes (steps.rs).
        if !unsafe { ps.flag.read() } {
            continue;
        }
        // SAFETY: as above.
        unsafe { ps.flag.write(false) };
        if ps.set_active(*force_distinct_set) {
            if !ps.dset_degraded {
                // Set-mode entry (distinctset.rs): the parked row inserts
                // into the group's exact-distinct set instead of the
                // tuplesort (or, when the skip-sort drive forced a presorted
                // entry, instead of the adjacent-dedup); a budget overflow
                // degrades the GROUP back to the tuplesort (subsequent rows
                // then take the sort feed below).
                debug_assert_eq!(nsets, 1, "set-mode refuses grouping sets");
                collect_distinct_set(ps, estate, tmp)?;
                continue;
            }
            // Degraded group: fall through to the tuplesort feed
            // (sortstates[0] begun at the degrade point).
        } else if ps.presorted {
            advance_presorted_distinct(
                ps,
                trans_typ[ps.transno],
                *agg_node,
                *pergroup_base,
                estate,
                tmp,
                mcx,
            )?;
            continue;
        }
        for setno in 0..nsets {
            let sort = ps.sortstates[setno].as_mut().expect("ordered pertrans sort begun");
            if ps.num_inputs == 1 {
                // SAFETY: scratch slot 0 written by the program this row.
                let nd = unsafe { ps.scratch.read() };
                sort.putdatum(nd.value, nd.isnull)?;
            } else {
                let slot =
                    ps.insert_slot.as_mut().expect("multi-input ordered agg has a slot");
                exectuples::exec_clear_tuple(slot, mcx);
                {
                    let base = slot.base_mut();
                    for i in 0..ps.num_inputs {
                        // SAFETY: i < num_inputs scratch slots.
                        let nd = unsafe { ps.scratch.as_ptr().add(i).read() };
                        base.tts_values[i] = nd.value;
                        base.tts_isnull[i] = nd.isnull;
                    }
                }
                exectuples::exec_store_virtual_tuple(slot);
                sort.puttupleslot(slot, mcx)?;
            }
        }
    }
    Ok(())
}

/// Memory budget for one exact-distinct set: the same work_mem allowance the
/// displaced tuplesort would get before spilling. Crossing the budget spills
/// the set to hash-partitioned tapes (distinctset.rs `SpillState` — v2) or,
/// below `SPILL_MIN_BUDGET`, degrades the group to that tuplesort
/// (`degrade_distinct_set` — the v1 path, kept for whatever spill refuses)
/// so total memory behavior stays work_mem-bounded either way. Capped so the
/// text blob's u32 offsets can never overflow under absurd work_mem
/// settings.
fn distinct_set_budget() -> usize {
    let kb = init_small::globals::work_mem().max(64) as usize;
    (kb * 1024).min(1 << 31)
}

/// `PGRUST_LANE_V2_DISTINCTFIN` kill switch (default ON): the
/// COUNT(DISTINCT) finalize shortcut (`set_count_transfn` field doc) — the
/// set-mode replay of a bare int8inc_any transition collapses to
/// `transvalue += |set|`. `0`/`off` keeps the per-element transfn replay
/// (the A/B off arm; results are byte-identical either way).
fn distinctfin_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_DISTINCTFIN").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Add a materialized COUNT(DISTINCT) contribution `n` to a count pergroup
/// state — the distinctfin shortcut's arithmetic, shared with the codedgroup
/// emit fastpath. Overflow parity: C's int8inc errors at the crossing
/// increment (int8.c "bigint out of range") — unreachable in practice (the n
/// distinct values were materialized by this backend), kept for the exact
/// error surface.
///
/// SAFETY: `pg` must point at a live pergroup slot holding a NON-NULL by-val
/// i64 transition state (`set_count_transfn` + the callers' null guards).
pub(crate) unsafe fn count_distinct_apply(pg: *mut AggPerGroup, n: i64) -> PgResult<()> {
    let cur = unsafe { (*pg).trans_value.as_i64() };
    let Some(newv) = cur.checked_add(n) else {
        return Err(Box::new(PgError::error("bigint out of range").with_sqlstate(
            ::types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
        )));
    };
    unsafe {
        (*pg).trans_value = Datum::from_i64(newv);
    }
    Ok(())
}

// Budget crossing (collect-time): first crossing picks the group's overflow
// path once — the v2 set spill when the budget can absorb the tape write
// buffers (`SPILL_MIN_BUDGET`), else the v1 degrade-to-tuplesort — and later
// crossings of a spilled set keep flushing epochs to the tapes.
#[cold]
#[inline(never)]
fn distinct_set_overflow<'mcx>(
    ps: &mut PerTransSortData<'mcx>,
    mcx: ::mcx::Mcx<'mcx>,
    budget: usize,
) -> PgResult<()> {
    let kind = ps.set_kind.expect("set-mode pertrans");
    let dset = ps.dset.as_mut().expect("overflow fires on insert");
    if dset.spilled() || budget >= distinctset::SPILL_MIN_BUDGET {
        dset.spill_flush(kind, budget, mcx)
    } else {
        degrade_distinct_set(ps)
    }
}

// The set-mode half of the ordered-trans collect: the parked scratch datum
// inserts into the group's exact-distinct set. NULLs collapse to `seen_null`
// (the C sort path's DISTINCT dedup makes at most one NULL reach the
// transfn; the replay passes exactly one, through the identical
// advance_transition_function). Runs before the tmpcontext reset — by-ref
// scratch datums live in per-tuple memory, and any detoast copy lands there
// too (the set retains its own canonical image).
fn collect_distinct_set<'mcx>(
    ps: &mut PerTransSortData<'mcx>,
    estate: &mut EStateData<'mcx>,
    tmp: EcxtId,
) -> PgResult<()> {
    let kind = ps.set_kind.expect("set-mode pertrans");
    // SAFETY: scratch slot 0 written by the program this row.
    let nd = unsafe { ps.scratch.read() };
    let dset = ps.dset.get_or_insert_with(distinctset::DistinctSet::new);
    if nd.isnull {
        dset.seen_null = true;
        return Ok(());
    }
    match kind {
        distinctset::DistinctKeyKind::Int16 => dset.insert_i64(nd.value.as_i16() as i64),
        distinctset::DistinctKeyKind::Int32 => dset.insert_i64(nd.value.as_i32() as i64),
        distinctset::DistinctKeyKind::Int64 => dset.insert_i64(nd.value.as_i64()),
        distinctset::DistinctKeyKind::Bytes => {
            // SAFETY: non-null live text/varchar varlena — the admission
            // proved the argument type; detoast copies land in per-tuple
            // memory (reset per row).
            let v = unsafe {
                ::types_fmgr::datum_varlena_packed(nd.value, estate.ecxt(tmp).per_tuple_mcx())
            }?;
            dset.insert_bytes(v.data());
        }
    }
    let budget = distinct_set_budget();
    if dset.over_budget(budget) {
        distinct_set_overflow(ps, estate.es_query_cxt, budget)?;
    }
    Ok(())
}

// Budget overflow: dump the group's distinct values into the tuplesort the
// set displaced and finish the group on the C sort path (its own work_mem
// spill included). Value-identical: the sort re-orders whatever feed order
// it gets, its drain re-dedups (set values are already unique; later rows
// may duplicate them), and the admitted transfns are replay-order-
// insensitive. `dset_degraded` routes this group's remaining collects to the
// sort feed; the group boundary (restart / process) re-arms the set.
#[cold]
#[inline(never)]
fn degrade_distinct_set(ps: &mut PerTransSortData<'_>) -> PgResult<()> {
    let kind = ps.set_kind.expect("set-mode pertrans");
    debug_assert!(!ps.dset_degraded);
    let work_mem = init_small::globals::work_mem();
    let mut sort = Tuplesort::begin_datum(
        ps.sortdesc.attr(0).atttypid,
        ps.sort_ops[0],
        ps.sort_collations[0],
        ps.sort_nulls_first[0],
        work_mem,
        TUPLESORT_NONE,
    )?;
    let dset = ps.dset.as_mut().expect("degrade fires on insert");
    match kind {
        distinctset::DistinctKeyKind::Int16 => {
            for &k in dset.ints() {
                sort.putdatum(Datum::from_i16(k as i16), false)?;
            }
        }
        distinctset::DistinctKeyKind::Int32 => {
            for &k in dset.ints() {
                sort.putdatum(Datum::from_i32(k as i32), false)?;
            }
        }
        distinctset::DistinctKeyKind::Int64 => {
            for &k in dset.ints() {
                sort.putdatum(Datum::from_i64(k), false)?;
            }
        }
        distinctset::DistinctKeyKind::Bytes => {
            for i in 0..dset.n_bytes() {
                sort.putdatum(dset.bytes_datum(i), false)?;
            }
        }
    }
    if dset.seen_null {
        sort.putdatum(Datum::null(), true)?;
    }
    dset.clear_shrink();
    if ps.sortstates.is_empty() {
        ps.sortstates.resize_with(1, || None);
    }
    debug_assert!(ps.sortstates[0].is_none());
    ps.sortstates[0] = Some(sort);
    ps.dset_degraded = true;
    Ok(())
}

// v2 spilled-set replay (distinctset.rs `SpillState` doc): flush the
// residual epoch, then load-dedup-replay each hash partition in turn.
// Partitions are DISJOINT, so replays never repeat a value across
// partitions; within a partition the set re-dedups whatever the flush
// epochs wrote twice. A partition whose distinct values alone exceed the
// budget finishes on a work_mem-bounded datum tuplesort instead: the
// partial set plus the tape's remaining raw values feed the sort, whose
// adjacent-dedup drain (the C sort path's own discipline, `equalfn_one`
// included) replays each distinct value exactly once. Value identity is the
// v1 argument unchanged: same distinct multiset, different replay order,
// order-insensitive transfns.
#[allow(clippy::too_many_arguments)]
fn replay_spilled_distinct_set<'mcx, F>(
    dset: &mut distinctset::DistinctSet<'mcx>,
    ps: &mut PerTransSortData<'mcx>,
    kind: distinctset::DistinctKeyKind,
    estate: &mut EStateData<'mcx>,
    tmp: EcxtId,
    replay: &mut F,
) -> PgResult<()>
where
    F: FnMut(&mut PerTransSortData<'mcx>, &mut EStateData<'mcx>, NullableDatum) -> PgResult<()>,
{
    use distinctset::DistinctKeyKind as K;
    let budget = distinct_set_budget();
    let mcx = estate.es_query_cxt;
    let datum_of = |kind: K, k: i64| match kind {
        K::Int16 => Datum::from_i16(k as i16),
        K::Int32 => Datum::from_i32(k as i32),
        K::Int64 => Datum::from_i64(k),
        K::Bytes => unreachable!("bytes values replay from images"),
    };
    dset.spill_finish_writes(kind, budget, mcx)?;
    for p in 0..dset.spill_nparts() {
        if dset.spill_load_partition(kind, p, budget)? {
            match kind {
                K::Int16 | K::Int32 | K::Int64 => {
                    for i in 0..dset.ints().len() {
                        let d = datum_of(kind, dset.ints()[i]);
                        replay(ps, estate, NullableDatum { value: d, isnull: false })?;
                    }
                }
                K::Bytes => {
                    for i in 0..dset.n_bytes() {
                        let d = dset.bytes_datum(i);
                        replay(ps, estate, NullableDatum { value: d, isnull: false })?;
                    }
                }
            }
            continue;
        }
        // Oversize partition: bounded finish on the C sort path (the
        // degrade dump's exact shape, scoped to this partition).
        let work_mem = init_small::globals::work_mem();
        let mut sort = Tuplesort::begin_datum(
            ps.sortdesc.attr(0).atttypid,
            ps.sort_ops[0],
            ps.sort_collations[0],
            ps.sort_nulls_first[0],
            work_mem,
            TUPLESORT_NONE,
        )?;
        match kind {
            K::Int16 | K::Int32 | K::Int64 => {
                for i in 0..dset.ints().len() {
                    sort.putdatum(datum_of(kind, dset.ints()[i]), false)?;
                }
                let mut vals: Vec<i64> = Vec::new();
                loop {
                    vals.clear();
                    if !dset.spill_read_ints(p, &mut vals)? {
                        break;
                    }
                    for &k in &vals {
                        sort.putdatum(datum_of(kind, k), false)?;
                    }
                }
            }
            K::Bytes => {
                for i in 0..dset.n_bytes() {
                    sort.putdatum(dset.bytes_datum(i), false)?;
                }
                // Transient canonical image per record (putdatum copies
                // by-ref datums into the sort immediately — the degrade
                // dump relies on the same contract).
                let mut rec: Vec<u8> = Vec::new();
                let mut img: Vec<u32> = Vec::new();
                loop {
                    if !dset.spill_read_bytes(p, &mut rec)? {
                        break;
                    }
                    let d = distinctset::varlena_image(&rec, &mut img);
                    sort.putdatum(d, false)?;
                }
            }
        }
        sort.performsort()?;
        // Adjacent-dedup drain-replay — the sort path's own discipline
        // (process_ordered_aggregates_set's single-input arm, sans NULLs:
        // partition tapes never carry them).
        let sort_spilled = sort.spilled();
        let byref_typlen = if sort_spilled { sort.datum_byref_typlen() } else { 0 };
        let mut old_buf: PgVec<'mcx, u8> = PgVec::new_in(mcx);
        let mut old: Option<NullableDatum> = None;
        while let Some(nd) = sort.getdatum(true)? {
            debug_assert!(!nd.isnull, "partition tapes carry no NULLs");
            if let Some(o) = old {
                let eq = ps.equalfn_one.as_mut().expect("single-col DISTINCT eqfn");
                let mut fc2 = LocalFcinfo::<2>::fresh(ps.agg_collation);
                // SAFETY: the per-tuple context outlives the call (resets
                // recycle the same context object).
                unsafe { fc2.set_result_mcx(estate.ecxt(tmp).per_tuple_mcx()) };
                fc2.args[0] = NullableDatum { value: o.value, isnull: false };
                fc2.args[1] = NullableDatum { value: nd.value, isnull: false };
                if eq.invoke(&mut fc2)?.as_bool() {
                    continue;
                }
            }
            replay(ps, estate, nd)?;
            old = Some(if byref_typlen != 0 {
                NullableDatum {
                    value: copy_scratch_datum(&mut old_buf, nd.value, byref_typlen)?,
                    isnull: false,
                }
            } else {
                nd
            });
        }
        sort.end();
    }
    dset.spill_end()?;
    Ok(())
}

// ExecEvalPreOrderedDistinctSingle/Multi (execExprInterp.c) + the transfn
// call: presorted DISTINCT rows skip the transfn when equal to the last-seen
// value; distinct rows become the new comparand and advance the transition.
fn advance_presorted_distinct<'mcx>(
    ps: &mut PerTransSortData<'mcx>,
    typ: TransTyp,
    agg_node: NonNull<AggStateNode>,
    pergroup_base: NonNull<AggPerGroup>,
    estate: &mut EStateData<'mcx>,
    tmp: EcxtId,
    mcx: ::mcx::Mcx<'mcx>,
) -> PgResult<()> {
    if ps.num_inputs == 1 {
        // SAFETY: scratch slot 0 written by the program this row.
        let nd = unsafe { ps.scratch.read() };
        let isdistinct = if !ps.haslast || ps.last_single.isnull != nd.isnull {
            true
        } else if nd.isnull {
            false
        } else {
            let eq = ps.equalfn_one.as_mut().expect("single-col DISTINCT eqfn");
            let mut fc2 = LocalFcinfo::<2>::fresh(ps.agg_collation);
            // SAFETY: the per-tuple context outlives the call (resets recycle
            // the same context object).
            unsafe { fc2.set_result_mcx(estate.ecxt(tmp).per_tuple_mcx()) };
            fc2.args[0] = NullableDatum { value: ps.last_single.value, isnull: false };
            fc2.args[1] = NullableDatum { value: nd.value, isnull: false };
            !eq.invoke(&mut fc2)?.as_bool()
        };
        if !isdistinct {
            return Ok(());
        }
        ps.haslast = true;
        ps.last_single = if !nd.isnull && !ps.input_byval {
            // scratch datums live in per-tuple memory: retain a copy.
            NullableDatum {
                value: copy_scratch_datum(&mut ps.last_buf, nd.value, ps.input_typlen)?,
                isnull: false,
            }
        } else {
            nd
        };
    } else {
        {
            let slot = ps.insert_slot.as_mut().expect("multi-input ordered agg has a slot");
            exectuples::exec_clear_tuple(slot, mcx);
            {
                let base = slot.base_mut();
                for i in 0..ps.num_inputs {
                    // SAFETY: i < num_inputs scratch slots.
                    let nd = unsafe { ps.scratch.as_ptr().add(i).read() };
                    base.tts_values[i] = nd.value;
                    base.tts_isnull[i] = nd.isnull;
                }
            }
            exectuples::exec_store_virtual_tuple(slot);
        }
        let matched = if ps.haslast {
            let (cur, uniq) = (&mut ps.insert_slot, &mut ps.slot2);
            let mut slots = EvalSlots {
                scan: None,
                inner: uniq.as_mut().map(|s| &mut *s),
                outer: cur.as_mut().map(|s| &mut *s),
            };
            exec_qual(ps.equalfn_multi.as_deref_mut(), &mut slots)?
        } else {
            false
        };
        if matched {
            return Ok(());
        }
        ps.haslast = true;
        let (cur, uniq) = (&mut ps.insert_slot, &mut ps.slot2);
        exectuples::exec_copy_slot(
            uniq.as_mut().expect("presorted multi-col DISTINCT has a uniq slot"),
            cur.as_mut().expect("multi-input ordered agg has a slot"),
            mcx,
            mcx,
        )?;
    }

    // SAFETY: transno < numtrans of the once-allocated pergroup array.
    let pg = unsafe { pergroup_base.as_ptr().add(ps.transno) };
    let mut fcinfo = LocalFcinfo::<MAX_ORDERED_TRANS_ARGS>::fresh(ps.agg_collation);
    fcinfo.nargs = (ps.num_trans_inputs + 1) as i16;
    fcinfo.context = Some(agg_node.cast());
    // SAFETY: as the equalfn arming above.
    unsafe { fcinfo.set_result_mcx(estate.ecxt(tmp).per_tuple_mcx()) };
    for i in 0..ps.num_trans_inputs {
        // SAFETY: i < num_inputs scratch slots (num_trans_inputs <= num_inputs).
        fcinfo.args[i + 1] = unsafe { ps.scratch.as_ptr().add(i).read() };
    }
    advance_transition_function(
        &mut fcinfo,
        &mut ps.transfn,
        typ,
        ps.num_trans_inputs,
        agg_node,
        pg,
    )
}

// C advance_transition_function (nodeAgg.c): the sorted-input replay of the
// transfn; by-ref result discipline mirrors execexpr's agg_plain_trans_byref.
fn advance_transition_function(
    fcinfo: &mut LocalFcinfo<MAX_ORDERED_TRANS_ARGS>,
    transfn: &mut FmgrInfo,
    typ: TransTyp,
    num_trans_inputs: usize,
    agg_node: NonNull<AggStateNode>,
    pg: *mut AggPerGroup,
) -> PgResult<()> {
    // SAFETY: pg is the once-allocated pergroup slot, sole live pointer here;
    // agg_node outlives the call (query-lifetime cell).
    unsafe {
        if transfn.fn_strict {
            for i in 1..=num_trans_inputs {
                if fcinfo.args[i].isnull {
                    return Ok(());
                }
            }
            if (*pg).no_trans_value {
                // C ExecAggInitGroup: the first value becomes the transvalue.
                let v = fcinfo.args[1];
                let value = if !typ.byval {
                    ::execexpr::agg_datum_copy(agg_node.as_ref().aggcontext(), v.value, typ.len)?
                } else {
                    v.value
                };
                (*pg) = AggPerGroup {
                    trans_value: value,
                    trans_value_is_null: false,
                    no_trans_value: false,
                };
                return Ok(());
            }
            if (*pg).trans_value_is_null {
                return Ok(());
            }
        }
        fcinfo.args[0] =
            NullableDatum { value: (*pg).trans_value, isnull: (*pg).trans_value_is_null };
        fcinfo.isnull = false;
        let result = transfn.invoke(fcinfo)?;
        let isnull = fcinfo.isnull;
        let new_val = if !typ.byval && result.as_usize() != (*pg).trans_value.as_usize() {
            if !isnull {
                ::execexpr::agg_datum_copy(agg_node.as_ref().aggcontext(), result, typ.len)?
            } else {
                Datum::null()
            }
        } else {
            result
        };
        (*pg).trans_value = new_val;
        (*pg).trans_value_is_null = isnull;
    }
    Ok(())
}

// process_ordered_aggregate_single/multi (nodeAgg.c): drain each pertrans
// sort through the transfn with DISTINCT dedup. Datums/tuples read without
// copy — the in-memory sort images stay live until tuplesort_end.
fn process_ordered_aggregates<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let base = node.pergroup_base;
    process_ordered_aggregates_set(node, estate, 0, base)
}

// process_ordered_aggregate_{single,multi} (nodeAgg.c) for one grouping set.
pub(crate) fn process_ordered_aggregates_set<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    setno: usize,
    set_pergroup_base: NonNull<AggPerGroup>,
) -> PgResult<()> {
    if node.pertrans_sort.is_empty() {
        return Ok(());
    }
    let mcx = estate.es_query_cxt;
    let tmp = node.tmpcontext;
    let AggStateData {
        pertrans_sort, trans_typ, agg_node, force_distinct_set, ..
    } = node;
    let pergroup_base = &set_pergroup_base;
    for ps in pertrans_sort.iter_mut() {
        let set_active = ps.set_active(*force_distinct_set);
        // Presorted DISTINCT already advanced per row (unless set-mode took
        // the entry over): drop the group's comparand (C
        // finalize_aggregates' haslast reset).
        if !set_active && ps.presorted {
            if ps.haslast {
                ps.haslast = false;
                ps.last_single = NullableDatum::null();
                if let Some(s2) = ps.slot2.as_mut() {
                    exectuples::exec_clear_tuple(s2, mcx);
                }
            }
            continue;
        }
        // SAFETY: transno < numtrans of the once-allocated pergroup array.
        let pg = unsafe { pergroup_base.as_ptr().add(ps.transno) };
        let typ = trans_typ[ps.transno];
        let mut fcinfo = LocalFcinfo::<MAX_ORDERED_TRANS_ARGS>::fresh(ps.agg_collation);
        fcinfo.nargs = (ps.num_trans_inputs + 1) as i16;
        fcinfo.context = Some(agg_node.cast());
        // SAFETY: the per-tuple context outlives every call below (resets
        // recycle the same context object).
        unsafe { fcinfo.set_result_mcx(estate.ecxt(tmp).per_tuple_mcx()) };
        if set_active {
            if !ps.dset_degraded {
                // Set-mode replay (distinctset.rs): each distinct value once
                // through the SAME advance_transition_function the sort
                // drain uses; the at-most-one NULL replays last (order is
                // transfn-invisible — the admitted transitions are
                // order-insensitive, which is the set's admission ticket).
                debug_assert_eq!(setno, 0, "set-mode refuses grouping sets");
                let Some(mut dset) = ps.dset.take() else {
                    continue;
                };
                let kind = ps.set_kind.expect("set-mode pertrans");
                // COUNT(DISTINCT x) finalize shortcut (`set_count_transfn`
                // field doc): the deduped set IS the count. n int8inc_any
                // replays from the current state add exactly n (strict fn,
                // by-val i64 state, no finalfn side channel); the at-most-one
                // NULL replay strict-skips, contributing 0. The state guards
                // are structural for count (initcond '0' → non-null i64
                // before any replay) and fall back to the literal replay if
                // ever violated.
                if ps.set_count_transfn
                    && distinctfin_enabled()
                    // SAFETY: pg is the once-allocated pergroup slot (loop
                    // invariant above).
                    && unsafe { !(*pg).no_trans_value && !(*pg).trans_value_is_null }
                {
                    let n: i64 = if dset.spilled() {
                        // The spilled load-dedup machinery runs unchanged
                        // (partition load, oversize-partition tuplesort with
                        // adjacent dedup); only the per-element transfn call
                        // becomes a counter bump.
                        let mut n = 0i64;
                        let mut count = |_ps: &mut PerTransSortData<'mcx>,
                                         estate: &mut EStateData<'mcx>,
                                         nd: NullableDatum|
                         -> PgResult<()> {
                            debug_assert!(!nd.isnull, "partition tapes carry no NULLs");
                            estate.reset_expr_context(tmp);
                            n += 1;
                            Ok(())
                        };
                        replay_spilled_distinct_set(
                            &mut dset,
                            ps,
                            kind,
                            estate,
                            tmp,
                            &mut count,
                        )?;
                        n
                    } else {
                        match kind {
                            distinctset::DistinctKeyKind::Bytes => dset.n_bytes() as i64,
                            _ => dset.ints().len() as i64,
                        }
                    };
                    // SAFETY: pg is the once-allocated pergroup slot with a
                    // non-null by-val i64 state (guard read above).
                    unsafe { count_distinct_apply(pg, n)? };
                    dset.clear();
                    ps.dset = Some(dset);
                    continue;
                }
                let mut replay = |ps: &mut PerTransSortData<'mcx>,
                                  estate: &mut EStateData<'mcx>,
                                  nd: NullableDatum|
                 -> PgResult<()> {
                    estate.reset_expr_context(tmp);
                    fcinfo.args[1] = nd;
                    advance_transition_function(
                        &mut fcinfo,
                        &mut ps.transfn,
                        typ,
                        ps.num_trans_inputs,
                        *agg_node,
                        pg,
                    )
                };
                if dset.spilled() {
                    // v2 spilled group: per-partition load-dedup-replay
                    // (oversize partitions finish on a bounded tuplesort).
                    replay_spilled_distinct_set(
                        &mut dset,
                        ps,
                        kind,
                        estate,
                        tmp,
                        &mut replay,
                    )?;
                    if dset.seen_null {
                        replay(ps, estate, NullableDatum::null())?;
                    }
                    dset.clear();
                    ps.dset = Some(dset);
                    continue;
                }
                match kind {
                    distinctset::DistinctKeyKind::Int16 => {
                        for i in 0..dset.ints().len() {
                            let k = dset.ints()[i];
                            replay(
                                ps,
                                estate,
                                NullableDatum { value: Datum::from_i16(k as i16), isnull: false },
                            )?;
                        }
                    }
                    distinctset::DistinctKeyKind::Int32 => {
                        for i in 0..dset.ints().len() {
                            let k = dset.ints()[i];
                            replay(
                                ps,
                                estate,
                                NullableDatum { value: Datum::from_i32(k as i32), isnull: false },
                            )?;
                        }
                    }
                    distinctset::DistinctKeyKind::Int64 => {
                        for i in 0..dset.ints().len() {
                            let k = dset.ints()[i];
                            replay(
                                ps,
                                estate,
                                NullableDatum { value: Datum::from_i64(k), isnull: false },
                            )?;
                        }
                    }
                    distinctset::DistinctKeyKind::Bytes => {
                        for i in 0..dset.n_bytes() {
                            let d = dset.bytes_datum(i);
                            replay(ps, estate, NullableDatum { value: d, isnull: false })?;
                        }
                    }
                }
                if dset.seen_null {
                    replay(ps, estate, NullableDatum::null())?;
                }
                dset.clear();
                ps.dset = Some(dset);
                continue;
            }
            // Degraded group: the dumped tuplesort drains through the sort
            // path below (its DISTINCT dedup included); re-arm the set for
            // the next group.
            ps.dset_degraded = false;
        }
        let mut sort = ps.sortstates[setno].take().expect("ordered pertrans sort begun");
        sort.performsort()?;
        // Spilled by-ref values live in recycled slab slots (valid until the
        // next fetch): the held DISTINCT comparand needs C's datumCopy shape.
        // The in-memory lever (images live until end, no copy) stays.
        let spilled = sort.spilled();
        if ps.num_inputs == 1 {
            let byref_typlen = if spilled { sort.datum_byref_typlen() } else { 0 };
            let mut old_buf: PgVec<'mcx, u8> = PgVec::new_in(mcx);
            let mut old: Option<NullableDatum> = None;
            while let Some(nd) = sort.getdatum(true)? {
                estate.reset_expr_context(tmp);
                if ps.num_distinct_cols > 0 {
                    if let Some(o) = old {
                        let equal = if o.isnull && nd.isnull {
                            true
                        } else if o.isnull != nd.isnull {
                            false
                        } else {
                            let eq = ps.equalfn_one.as_mut().expect("single-col DISTINCT eqfn");
                            let mut fc2 = LocalFcinfo::<2>::fresh(ps.agg_collation);
                            // SAFETY: as the transfn arming above.
                            unsafe { fc2.set_result_mcx(estate.ecxt(tmp).per_tuple_mcx()) };
                            fc2.args[0] = NullableDatum { value: o.value, isnull: false };
                            fc2.args[1] = NullableDatum { value: nd.value, isnull: false };
                            eq.invoke(&mut fc2)?.as_bool()
                        };
                        if equal {
                            continue;
                        }
                    }
                }
                fcinfo.args[1] = nd;
                advance_transition_function(
                    &mut fcinfo,
                    &mut ps.transfn,
                    typ,
                    ps.num_trans_inputs,
                    *agg_node,
                    pg,
                )?;
                old = Some(if byref_typlen != 0 && !nd.isnull {
                    NullableDatum {
                        value: copy_scratch_datum(&mut old_buf, nd.value, byref_typlen)?,
                        isnull: false,
                    }
                } else {
                    nd
                });
            }
            sort.end();
        } else {
            let mut have_old = false;
            loop {
                let got =
                    sort.gettupleslot(true, spilled, ps.slot1.as_mut().expect("sortslot"), mcx)?;
                if !got {
                    break;
                }
                let matched = if ps.num_distinct_cols > 0 && have_old {
                    let (s1, s2) = (
                        // Two disjoint options; split borrows via as_mut.
                        &mut ps.slot1,
                        &mut ps.slot2,
                    );
                    let mut slots = EvalSlots {
                        scan: None,
                        inner: s2.as_mut().map(|s| &mut *s),
                        outer: s1.as_mut().map(|s| &mut *s),
                    };
                    exec_qual(ps.equalfn_multi.as_deref_mut(), &mut slots)?
                } else {
                    false
                };
                if !matched {
                    {
                        let s1 = ps.slot1.as_mut().unwrap();
                        exectuples::slot_getsomeattrs(s1, ps.num_trans_inputs as i32);
                        let base = s1.base();
                        for i in 0..ps.num_trans_inputs {
                            fcinfo.args[i + 1] = NullableDatum {
                                value: base.tts_values[i],
                                isnull: base.tts_isnull[i],
                            };
                        }
                    }
                    advance_transition_function(
                        &mut fcinfo,
                        &mut ps.transfn,
                        typ,
                        ps.num_trans_inputs,
                        *agg_node,
                        pg,
                    )?;
                    if ps.num_distinct_cols > 0 {
                        core::mem::swap(&mut ps.slot1, &mut ps.slot2);
                        have_old = true;
                    }
                }
                estate.reset_expr_context(tmp);
                exectuples::exec_clear_tuple(ps.slot1.as_mut().unwrap(), mcx);
            }
            exectuples::exec_clear_tuple(ps.slot1.as_mut().unwrap(), mcx);
            if let Some(s2) = ps.slot2.as_mut() {
                exectuples::exec_clear_tuple(s2, mcx);
            }
            sort.end();
        }
    }
    Ok(())
}

/// The held-comparand copy for spilled by-ref datum sorts (C datumCopy +
/// pfree per replaced value; retained scratch here).
fn copy_scratch_datum<'m>(
    buf: &mut PgVec<'m, u8>,
    val: Datum,
    typlen: i16,
) -> PgResult<Datum> {
    let src = val.as_usize() as *const u8;
    // SAFETY: non-null by-ref datum readable for its full size.
    let size = unsafe {
        if typlen == -1 {
            ::types_tuple::varatt::varsize_any(src)
        } else {
            typlen as usize
        }
    };
    buf.clear();
    buf.reserve(size);
    // SAFETY: reserved size bytes; src readable per above; regions disjoint.
    unsafe {
        core::ptr::copy_nonoverlapping(src, buf.as_mut_ptr(), size);
        buf.set_len(size);
    }
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

/// `ExecAgg` -> `agg_retrieve_direct` (nodeAgg.c), single-group arm: drain the
/// outer child through the transition program, then finalize and project the
// C resolves an initplan's PARAM_EXEC lazily inside ExecEvalParamExec; this
// executor hoists instead: any pending initplan a program depends on runs
// before the drive evaluates it (noderesult.c pattern).
fn hoist_pending_initplans<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut deps: Vec<u32> = Vec::new();
    if let Some(et) = node.evaltrans.as_deref() {
        deps.extend_from_slice(et.param_exec_deps());
    }
    deps.extend_from_slice(node.proj.param_exec_deps());
    if let Some(q) = node.qual.as_deref() {
        deps.extend_from_slice(q.param_exec_deps());
    }
    if let Some(gs) = node.gsets.as_deref() {
        gs.collect_param_deps(&mut deps);
    }
    if !deps.is_empty() {
        ::executils::exec_eval_param_exec_params(estate, &deps)?;
    }
    Ok(())
}

/// one result row. Zero input rows still produce a row (C contract).
pub fn exec_agg<'mcx, F>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut fetch_outer: F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    if node.agg_done {
        return Ok(None);
    }
    hoist_pending_initplans(node, estate)?;
    if node.gsets.is_some() {
        return gsets::exec_agg_gsets(node, estate, &mut fetch_outer);
    }
    if node.plan.aggstrategy == AGG_HASHED {
        if !node.perhash.as_ref().expect("hashed Agg has perhash").table_filled {
            agg_fill_hash_table(node, estate, &mut fetch_outer)?;
        }
        if node.merge.as_ref().is_some_and(|m| m.has_run()) {
            return merge::agg_retrieve_merged(node, estate);
        }
        return agg_retrieve_hash_table(node, estate, None);
    }
    if node.plan.aggstrategy == AGG_SORTED {
        return agg_retrieve_sorted(node, estate, &mut fetch_outer);
    }
    initialize_aggregates(node, estate)?;

    while let Some(outer_id) = fetch_outer(estate)? {
        estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
        let et = node.evaltrans.as_mut().unwrap();
        if et.has_subplan() {
            ::executils::exec_eval_expr_with_subplans(et, estate, node.tmpcontext)?;
        } else {
            let outer_slot = estate.slot_mut(outer_id);
            let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
            exec_eval_expr(et, &mut slots)?;
        }
        if !node.pertrans_sort.is_empty() {
            collect_ordered_input(node, estate, 1)?;
        }
        estate.reset_expr_context(node.tmpcontext);
    }
    plain_finish(node, estate)
}

// exec_agg's post-drain tail (finalize + HAVING + project), shared with the
// batched drive.
fn plain_finish<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    process_ordered_aggregates(node, estate)?;
    estate.reset_expr_context(node.ps_ExprContext);
    finalize_aggregates(node, estate, node.pergroup_base)?;
    node.agg_done = true;

    // project_aggregates: the HAVING qual (var-free here) gates the one row.
    if node.proj.has_subplan() || node.qual.as_deref().is_some_and(|q| q.has_subplan()) {
        let ecxt = node.ps_ExprContext;
        if !::executils::exec_qual_with_subplans(node.qual.as_deref_mut(), estate, ecxt)? {
            estate.instr_count_filtered1(node.instr_idx);
            return Ok(None);
        }
        ::executils::exec_project_with_subplans(
            &mut node.proj,
            estate,
            ecxt,
            node.ps_ResultTupleSlot,
        )?;
        return Ok(Some(node.ps_ResultTupleSlot));
    }
    let mut slots = EvalSlots { scan: None, inner: None, outer: None };
    if !exec_qual(node.qual.as_deref_mut(), &mut slots)? {
        estate.instr_count_filtered1(node.instr_idx);
        return Ok(None);
    }
    let mcx = estate.es_query_cxt;
    let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
    let mut slots = EvalSlots { scan: None, inner: None, outer: None };
    exec_project(&mut node.proj, &mut slots, result_slot, mcx)?;
    Ok(Some(node.ps_ResultTupleSlot))
}

/// Page-batch feed for the fused agg-over-scan drive (upstream batch
/// executor design, CF 6176); implemented over the concrete scan node by the
/// dispatcher, which owns both sides.
///
/// Promoted to the shared `executils::BatchSource` seam (lane-executor-v2
/// design §Architecture 1) so the execmain lane driver can consume the same
/// batch source. Kept re-exported under the historical `AggBatchSource` name
/// so the fused-agg path (and its `SeqScan`/index/bitmap source impls in
/// execmain) is unchanged.
pub use ::executils::BatchSource as AggBatchSource;

/// Shapes `exec_agg_batched` handles; the dispatcher falls back to the
/// per-tuple drive otherwise.
pub fn agg_batch_drainable(node: &AggStateData<'_>) -> bool {
    node.gsets.is_none()
        && node.merge.is_none()
        && node.pertrans_sort.is_empty()
        && (node.plan.aggstrategy == AGG_PLAIN || node.plan.aggstrategy == AGG_HASHED)
        && node.evaltrans.as_deref().is_some_and(|et| !et.has_subplan())
}

/// Outer-slot deform prefix the batched drive reads per row (evaltrans
/// FETCHSOME bound + hashed grouping columns); None = shape unknown, the
/// SoA batch deform stays disarmed.
pub fn agg_batch_outer_prefix(node: &AggStateData<'_>) -> Option<i32> {
    debug_assert!(agg_batch_drainable(node));
    let mut p = node
        .evaltrans
        .as_deref()
        .expect("drainable Agg has evaltrans")
        .max_fetch(::execexpr::SlotSrc::Outer)?;
    if node.plan.aggstrategy == AGG_HASHED {
        p = p.max(node.perhash.as_ref().expect("hashed Agg has perhash").largest_grp_col_idx);
    }
    Some(p)
}

/// `exec_agg` over a page-batch source: identical per-row transition order,
/// minus the per-tuple node recursion (and minus the slot store for
/// input-free transition kernels).
pub fn exec_agg_batched<'mcx, S: AggBatchSource<'mcx>>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut src: S,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert!(agg_batch_drainable(node));
    if node.agg_done {
        return Ok(None);
    }
    if node.plan.aggstrategy == AGG_HASHED {
        if !node.perhash.as_ref().expect("hashed Agg has perhash").table_filled {
            agg_fill_hash_table_batched(node, estate, &mut src)?;
        }
        return agg_retrieve_hash_table(node, estate, None);
    }
    initialize_aggregates(node, estate)?;

    let storeless = src.storeless_ok()
        && matches!(
            node.evaltrans.as_deref().unwrap().kernel(),
            ::execexpr::Kernel::AggTransByVal { .. } | ::execexpr::Kernel::AggTransByValThin { .. }
        );
    // count(*) advances once per page batch; a refused advance re-runs the
    // batch through the per-row kernel so overflow ereports at exactly C's
    // row. The per-row resets are no-ops here (the transition and the kernel
    // qual allocate nothing), so one reset per batch is state-identical.
    let count_star = node.evaltrans.as_deref().unwrap().agg_count_star();
    loop {
        let n = src.next_batch(estate)?;
        if n == 0 {
            break;
        }
        if let Some((pergroup, strict)) = count_star {
            // Qual'd count(*): the source's bitmap census replaces the
            // per-row fetch+transition walk; a None census or refused
            // advance falls to the per-row drain below.
            let c = if storeless { Some(n) } else { src.qualifying_count(estate, n)? };
            if let Some(c) = c {
                if ::execexpr::agg_count_star_advance(pergroup, strict, c) {
                    estate.reset_expr_context(node.tmpcontext);
                    continue;
                }
            }
        }
        if storeless {
            for _ in 0..n {
                let mut slots = EvalSlots::default();
                exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)?;
                estate.reset_expr_context(node.tmpcontext);
            }
        } else {
            // Fetch-dead word skip (`skip_words`): a cleared bit is a row
            // `fetch_tuple` rejects with no observable effect — same
            // surviving rows, same order, same transitions.
            let skip = src.skip_words();
            exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), 0, n, |i| -> PgResult<()> {
                if !src.fetch_tuple(i, estate)? {
                    return Ok(());
                }
                let outer_id = src.outer_slot();
                estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
                let outer_slot = estate.slot_mut(outer_id);
                let mut slots =
                    EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
                exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)?;
                estate.reset_expr_context(node.tmpcontext);
                Ok(())
            })?;
        }
    }
    plain_finish(node, estate)
}

fn agg_fill_hash_table_batched<'mcx, S: AggBatchSource<'mcx>>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    src: &mut S,
) -> PgResult<()> {
    loop {
        let n = src.next_batch(estate)?;
        if n == 0 {
            break;
        }
        // Fetch-dead word skip (`skip_words`) — see `exec_agg_batched`'s
        // per-row drain: same surviving rows, same order, same entries.
        let skip = src.skip_words();
        exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), 0, n, |i| -> PgResult<()> {
            if !src.fetch_tuple(i, estate)? {
                return Ok(());
            }
            let outer_id = src.outer_slot();
            estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
            if lookup_hash_entry(node, estate, outer_id)? {
                let outer_slot = estate.slot_mut(outer_id);
                let mut slots =
                    EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
                exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)?;
            }
            estate.reset_expr_context(node.tmpcontext);
            Ok(())
        })?;
    }
    hashagg_finish_initial_spills(node, estate)?;
    merge::maybe_install_handoff(node, estate)?;
    let ph = node.perhash.as_mut().unwrap();
    ph.table_filled = true;
    ph.hashiter = 0;
    Ok(())
}

// ===========================================================================
// Lane-executor-v2 hash-agg breaker delegation seam (design §Architecture 1,
// §8). The lane's pipeline-breaker node implements push `Sink` + `Source` in
// `execmain/src/lanev2.rs`; these thin entry points delegate every substantive
// step to the SAME row-path machinery the fused batched drive uses — the
// per-row transition path (`lookup_hash_entry` + evaltrans), the hashagg
// spill, and the canonical `agg_retrieve_hash_table` read-back (same table,
// same iteration → same output order as C).
// ===========================================================================

/// Agg-side admission for the lane-v2 hash-agg breaker: batch-drainable,
/// AGG_HASHED, and initplan-param-free (the lane drive, like
/// `exec_agg_batched`, does not hoist pending initplans).
pub fn agg_hash_breaker_admissible(node: &AggStateData<'_>) -> bool {
    agg_batch_drainable(node)
        && node.plan.aggstrategy == AGG_HASHED
        && node
            .evaltrans
            .as_deref()
            .is_none_or(|et| et.param_exec_deps().is_empty())
        && node.proj.param_exec_deps().is_empty()
        && node.qual.as_deref().is_none_or(|q| q.param_exec_deps().is_empty())
}

/// `agg_done` read for the lane driver (exec_agg's top-of-call guard).
pub fn agg_is_done(node: &AggStateData<'_>) -> bool {
    node.agg_done
}

/// Strategy read for dispatchers outside this crate (SE-AGGJOIN: the runtime
/// hash-join arm's grouped/plain divert).
pub fn agg_is_hashed(node: &AggStateData<'_>) -> bool {
    node.plan.aggstrategy == AGG_HASHED
}

/// Build→Probe phase flag for the breaker: the hash table's `table_filled`
/// IS the phase (exactly C's cross-call state; no new field).
pub fn agg_hash_table_filled(node: &AggStateData<'_>) -> bool {
    node.perhash.as_ref().is_some_and(|ph| ph.table_filled)
}

/// Breaker `Sink::accept`: one outer row through prepare/lookup + the
/// transition program — `agg_fill_hash_table_batched`'s per-row body verbatim
/// (spill-mode misses spill the tuple and skip the transition, identically).
pub fn agg_hash_build_accept<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
) -> PgResult<()> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_HASHED);
    estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
    if lookup_hash_entry(node, estate, outer_id)? {
        let outer_slot = estate.slot_mut(outer_id);
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
        exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)?;
    }
    estate.reset_expr_context(node.tmpcontext);
    Ok(())
}

/// Breaker `Sink::combine` (the Stage-4 seam, reserved since Phase 2 and
/// landed with the pool): the partial build's cross-worker contribution —
/// finish initial spills, then hand the whole table to the leader by pointer
/// (merge handoff) when a finalize registered one. The leader's finalize
/// combines the handed tables partition-parallel with the ported combinefn
/// machinery (merge.rs), so this IS the combine half of combine-before-
/// finish; `finish` then only flips the breaker to its Source face.
/// Idempotence: guarded by `hash_build_combined` — a double install would
/// double-count this worker's groups.
pub fn agg_hash_build_combine<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if node.hash_build_combined {
        return Ok(());
    }
    hashagg_finish_initial_spills(node, estate)?;
    merge::maybe_install_handoff(node, estate)?;
    node.hash_build_combined = true;
    Ok(())
}

/// Breaker `Sink::finish` (= Finalize): `agg_fill_hash_table_batched`'s
/// post-drain tail — combine (spill finish + handoff install) unless a
/// `Sink::combine` call already ran it, flip the phase flag, park the
/// iterator at the table head. The combined flag resets here so a rescan's
/// fresh build combines again.
pub fn agg_hash_build_finish<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    agg_hash_build_combine(node, estate)?;
    node.hash_build_combined = false;
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    ph.table_filled = true;
    ph.hashiter = 0;
    Ok(())
}

/// Breaker `Source::produce`: the canonical read-back —
/// `agg_retrieve_hash_table` (one qual-passing group per call, C's iteration
/// order, spill refill included).
pub fn agg_hash_retrieve<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    agg_retrieve_hash_table(node, estate, None)
}

// ===========================================================================
// SE-AGGJOIN (band 87001) — the GROUPED runtime seam: per-worker hashed
// builds (the breaker's own `agg_hash_build_accept` per row — C's checked
// transition program, spill-mode-free by refusal) exported into
// SELF-CONTAINED grouped partials (runtime_partial::GroupedRuntimePartial),
// combined order-insensitive-exactly across workers, and absorbed into the
// LEADER's hash table entry-by-entry so the canonical retrieve (finalize +
// HAVING + projection, C's iteration) emits them. The runtime hash-join
// multibuild walk is the one caller.
// ===========================================================================

/// Byval word-equality group-key types the grouped export/absorb admits:
/// bool, "char", int2/4/8, oid, date — types whose grouping equality IS
/// datum-word equality at the attribute width (NULLs group together, matching
/// the (word, isnull) key representation).
fn grouped_key_type_exportable(att: &::types_tuple::FormData_pg_attribute) -> bool {
    att.attbyval
        && matches!(att.attlen, 1 | 2 | 4 | 8)
        && matches!(att.atttypid, 16 | 18 | 20 | 21 | 23 | 26 | 1082)
}

/// SE-CBKEYS (the GL-CBKEYS-1 lane): a grouping column's key EXPORT kind.
/// `Word` = the byval vocabulary above (the bootstrap row, byte-untouched).
/// `Bytes` = canonical-bytes text/varchar under a DETERMINISTIC collation
/// (default 100 / C 950) — byte equality of the detoasted content IS the
/// grouping operator's verdict (texteq; the scan-side C3/distinct
/// machinery's `group_eq_representational` law).
///
/// SE-BPCHAR (the GL-BPCHAR-1 lane) — the TIE LAW of record: BPCHAR (1042)
/// columns with a REAL typmod (`char(n)`, atttypmod >= 5) additionally
/// admit as `Bytes` when the caller passes `admit_bpchar` (the
/// PGRUST_LANE_V2_CBKEYS_BPCHAR gate). The ruling, proven against the
/// vendored functions (varchar::bpchar_clip / bpchareq, tie-law unit
/// corpus in the varchar crate): every stored `char(n)` value carries
/// EXACTLY n characters (bpchar_input/recv and the length-coercion cast
/// pad, or truncate trailing spaces only), and `bpchareq` is texteq over
/// `bc_trim` (trailing-0x20-BYTE strip — multibyte-safe because every
/// legal SERVER encoding keeps non-first bytes of multibyte characters
/// high-bit-set, so 0x20 is always a real space; C's bcTruelen relies on
/// the same law). Hence for two values of ONE column (same typmod, the
/// bare-Var probe discipline): equal-under-bpchareq <=> byte-identical
/// stored images — the canonical bytes ARE the stored bytes, and the
/// group representative is unique (no trailing-blank tie exists between
/// equal keys). Typmod-less bpchar (unpadded storage) stays refused; the
/// absorb-side `!isnew` check remains the DEFENSE (a non-canonical image
/// — corruption, a rogue writer — refuses to the serial rerun), never the
/// argument.
enum GroupedKeyKind {
    Word,
    Bytes,
}

fn grouped_key_kind(
    att: &::types_tuple::FormData_pg_attribute,
    admit_bpchar: bool,
) -> Option<GroupedKeyKind> {
    if grouped_key_type_exportable(att) {
        return Some(GroupedKeyKind::Word);
    }
    if att.attlen != -1 || !matches!(att.attcollation, 100 | 950) {
        return None;
    }
    if matches!(att.atttypid, 25 | 1043) {
        return Some(GroupedKeyKind::Bytes);
    }
    // char(n) under the tie law: typmod = n + VARHDRSZ(4), n >= 1.
    (admit_bpchar && att.atttypid == 1042 && att.atttypmod >= 5)
        .then_some(GroupedKeyKind::Bytes)
}

/// Fail-closed admission for the grouped runtime sink: a serial simple-split
/// hashed Agg (single set, param-free — the breaker gate), untouched by any
/// lane arm (no compact/sink/merge state), whose fold plan covers EVERY
/// transition with order-insensitive-exact kinds (the runtime_partial
/// whitelist — AvgAccum/Int128 numeric-family states included) and whose
/// grouping keys are all byval int-family word-equality types.
pub fn agg_grouped_runtime_admissible(node: &AggStateData<'_>) -> bool {
    agg_grouped_runtime_shell_admissible(node)
        && runtime_partial::agg_runtime_partial_admissible(node)
}

/// SE-NUMJOIN (the GL-NUMJOIN-1 lane): the grouped admission's POLY
/// twin — the identical structural shell, with the SE-AGGPOLY manifest
/// (>=1 numeric_avg_accum NumericAvg transition, remainder exportable lane
/// kinds; arg expressions free) in place of the full-fold-plan requirement.
/// The caller (the runtime hash-join grouped sink) gates it behind the
/// PGRUST_LANE_V2_AGGJOIN_NUMERIC knob and tries the plan-based admission
/// FIRST — plan-covered shapes never reach this.
pub fn agg_grouped_poly_runtime_admissible(node: &AggStateData<'_>) -> bool {
    agg_grouped_runtime_shell_admissible(node)
        && runtime_partial::agg_poly_partial_admissible(node)
}

/// SE-CBKEYS: the BYTES-key admission pair — the identical structural
/// core with the canonical-bytes key census (every key column Word- or
/// Bytes-exportable, at least one Bytes) in place of the word-only census.
/// The caller (the runtime hash-join grouped sink) gates these behind the
/// PGRUST_LANE_V2_CBKEYS knob and tries the word-key admissions FIRST —
/// word-keyed shapes never reach them.
pub fn agg_grouped_bytes_runtime_admissible(node: &AggStateData<'_>, admit_bpchar: bool) -> bool {
    agg_grouped_runtime_shell_core(node)
        && grouped_keys_bytes_admissible(node, admit_bpchar)
        && runtime_partial::agg_runtime_partial_admissible(node)
}

pub fn agg_grouped_bytes_poly_runtime_admissible(
    node: &AggStateData<'_>,
    admit_bpchar: bool,
) -> bool {
    agg_grouped_runtime_shell_core(node)
        && grouped_keys_bytes_admissible(node, admit_bpchar)
        && runtime_partial::agg_poly_partial_admissible(node)
}

/// The grouped admission's structural shell (shared by the plan-based and
/// poly rows): the core below + byval int-family word-equality grouping
/// keys (the bootstrap vocabulary, byte-untouched).
fn agg_grouped_runtime_shell_admissible(node: &AggStateData<'_>) -> bool {
    if !agg_grouped_runtime_shell_core(node) {
        return false;
    }
    let ph = node.perhash.as_ref().expect("core verified perhash");
    let base = ph.hashslot.base();
    let Some(desc) = base.tts_tupleDescriptor.as_ref() else { return false };
    let nkeys = ph.hash_grp_col_idx_input.len();
    if desc.attrs.len() < nkeys || nkeys == 0 {
        return false;
    }
    desc.attrs[..nkeys].iter().all(grouped_key_type_exportable)
}

/// SE-CBKEYS: the mixed word/bytes key census (>=1 canonical-bytes text
/// column; bpchar and non-deterministic collations refuse via
/// `grouped_key_kind`).
fn grouped_keys_bytes_admissible(node: &AggStateData<'_>, admit_bpchar: bool) -> bool {
    let Some(ph) = node.perhash.as_ref() else { return false };
    let base = ph.hashslot.base();
    let Some(desc) = base.tts_tupleDescriptor.as_ref() else { return false };
    let nkeys = ph.hash_grp_col_idx_input.len();
    if desc.attrs.len() < nkeys || nkeys == 0 {
        return false;
    }
    let mut n_bytes = 0usize;
    for att in &desc.attrs[..nkeys] {
        match grouped_key_kind(att, admit_bpchar) {
            Some(GroupedKeyKind::Bytes) => n_bytes += 1,
            Some(GroupedKeyKind::Word) => {}
            None => return false,
        }
    }
    n_bytes > 0
}

/// The structural core (no key-vocabulary check): a serial simple-split
/// hashed Agg (single set, param-free — the breaker gate), untouched by
/// any lane arm (no compact/sink/merge state).
fn agg_grouped_runtime_shell_core(node: &AggStateData<'_>) -> bool {
    if node.plan.aggstrategy != AGG_HASHED
        || node.plan.aggsplit != AGGSPLIT_SIMPLE
        || !agg_hash_breaker_admissible(node)
        || node.gsets.is_some()
        || node.merge.is_some()
        || node.persort.is_some()
    {
        return false;
    }
    let Some(ph) = node.perhash.as_ref() else { return false };
    !(ph.compact.is_some() || ph.sink_cap.is_some() || node.sink_emit.is_some())
}

/// Width-normalized key word of one stored key datum (canonical value form:
/// two equal group keys always normalize to the same word; NULL = 0).
fn grouped_key_word(att: &::types_tuple::FormData_pg_attribute, d: Datum, isnull: bool) -> i64 {
    if isnull {
        return 0;
    }
    match att.attlen {
        1 => (d.as_usize() as u8) as i64,
        2 => d.as_i16() as i64,
        4 => d.as_i32() as i64,
        _ => d.as_i64(),
    }
}

/// The inverse: rebuild the canonical datum from a normalized key word.
fn grouped_key_datum(att: &::types_tuple::FormData_pg_attribute, w: i64) -> Datum {
    match att.attlen {
        1 => Datum::from_usize((w as u8) as usize),
        2 => Datum::from_i16(w as i16),
        4 => Datum::from_i32(w as i32),
        _ => Datum::from_i64(w),
    }
}

/// SE-CBKEYS: a text/varchar key datum's canonical CONTENT bytes —
/// short/inline images read in place; compressed/external images detoast
/// (the hash table materializes input datums verbatim; a heap scan can hand
/// back toasted attributes). Byte equality of this content is the grouping
/// verdict under the admitted deterministic collations.
fn grouped_text_key_bytes(mcx: ::mcx::Mcx<'_>, d: Datum) -> PgResult<Box<[u8]>> {
    use ::types_tuple::varatt;
    let p = d.as_usize() as *const u8;
    if p.is_null() {
        return Err(Box::new(PgError::error(
            "grouped bytes key: null pointer datum".to_string(),
        )));
    }
    // SAFETY: a non-null text datum points at a live varlena image readable
    // through its header (the retrieve slot's materialized entry tuple).
    unsafe {
        if varatt::varatt_is_1b(p) && !varatt::varatt_is_1b_e(p) {
            let n = varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT;
            return Ok(core::slice::from_raw_parts(p.add(varatt::VARHDRSZ_SHORT), n).into());
        }
        if varatt::varatt_is_4b_u(p) {
            let n = varatt::varsize_4b(p) - varatt::VARHDRSZ;
            return Ok(core::slice::from_raw_parts(p.add(varatt::VARHDRSZ), n).into());
        }
        // Compressed or external: flatten, then take the 4B payload.
        let image = core::slice::from_raw_parts(p, varatt::varsize_any(p));
        let flat = ::detoast::detoast_attr(mcx, image)?;
        let n = varatt::varsize_4b(flat.as_ptr()) - varatt::VARHDRSZ;
        Ok(core::slice::from_raw_parts(flat.as_ptr().add(varatt::VARHDRSZ), n).into())
    }
}

/// The inverse: rebuild a canonical 4B-header varlena datum from content
/// bytes (the absorb side; the hash table copies the tuple into its own
/// context on insert, so `mcx` = the query context is life-time-sufficient).
fn grouped_text_key_datum(mcx: ::mcx::Mcx<'_>, content: &[u8]) -> PgResult<Datum> {
    use ::types_tuple::varatt;
    let total = content.len() + varatt::VARHDRSZ;
    let layout = core::alloc::Layout::from_size_align(total, 8)
        .map_err(|_| PgError::error("grouped bytes key: oversized image".to_string()))?;
    let raw = ::mcx::Allocator::allocate(&mcx, layout).map_err(|_| mcx.oom(total))?;
    let ptr = raw.cast::<u8>().as_ptr();
    let word = varatt::set_varsize_4b_word(total as u32);
    // SAFETY: fresh allocation of `total` bytes.
    unsafe {
        core::ptr::copy_nonoverlapping(word.to_ne_bytes().as_ptr(), ptr, varatt::VARHDRSZ);
        core::ptr::copy_nonoverlapping(content.as_ptr(), ptr.add(varatt::VARHDRSZ), content.len());
    }
    Ok(Datum::from_usize(ptr as usize))
}

/// WORKER side (per morsel, cumulative-overwrite discipline — the M1 partial
/// export's grouped twin): export the node's whole hash table into `out`.
/// `Ok(false)` = the build left the exportable envelope (spill mode entered /
/// ever spilled, or more than `max_groups` groups) — the caller refuses the
/// engagement to the serial arm (fail-closed; no wrong results, the table is
/// simply not exportable).
pub fn agg_hash_export_grouped_into<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    max_groups: usize,
    out: &mut runtime_partial::GroupedRuntimePartial,
) -> PgResult<bool> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_HASHED);
    let mcx = estate.es_query_cxt;
    // SE-NUMJOIN (CAR 2): the export schema, once per call (plan-based for
    // every pre-existing engagement; the poly manifest only for shapes the
    // knob-gated poly admission let in).
    let schema = runtime_partial::trans_schema(node)?;
    out.groups.clear();
    out.scratch_ptrs.clear();
    {
        let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
        if ph.spill.mode || ph.spill.ever_spilled || ph.compact.is_some() {
            return Ok(false);
        }
        if ph.hash_ngroups_current > max_groups as u64 {
            return Ok(false);
        }
        let nkeys = ph.hash_grp_col_idx_input.len();
        let mut it = 0u64;
        while let Some(ix) = ph.hashtable.iterate(&mut it) {
            let tup = ph.hashtable.entry_tuple(ix);
            // SAFETY: entry images live in the node's table context for the
            // table's lifetime (the retrieve path's identical store).
            unsafe {
                exectuples::exec_store_minimal_tuple_ptr(&mut ph.retrieve_slot, mcx, tup)
            };
            exectuples::slot_getallattrs(&mut ph.retrieve_slot);
            let base = ph.retrieve_slot.base();
            let desc = base
                .tts_tupleDescriptor
                .as_ref()
                .expect("hash retrieve slot has a descriptor");
            let mut key: runtime_partial::GroupKeyWords = Vec::with_capacity(nkeys);
            for i in 0..nkeys {
                let isnull = base.tts_isnull[i];
                let part = if isnull {
                    runtime_partial::GroupKeyPart::Word(0)
                } else {
                    match grouped_key_kind(&desc.attrs[i], true) {
                        Some(GroupedKeyKind::Word) => runtime_partial::GroupKeyPart::Word(
                            grouped_key_word(&desc.attrs[i], base.tts_values[i], isnull),
                        ),
                        // SE-CBKEYS: canonical content bytes (admission
                        // guaranteed the kind; reaching None is a walk bug).
                        Some(GroupedKeyKind::Bytes) => runtime_partial::GroupKeyPart::Bytes(
                            grouped_text_key_bytes(mcx, base.tts_values[i])?,
                        ),
                        None => {
                            return Err(Box::new(PgError::error(
                                "grouped export: key column outside the admitted vocabulary"
                                    .to_string(),
                            )))
                        }
                    }
                };
                key.push((part, isnull));
            }
            let pg = ph
                .hashtable
                .entry_additional(ix)
                .expect("numtrans > 0 tables carry additional space");
            out.groups.push((key, runtime_partial::RuntimePartial::default()));
            out.scratch_ptrs.push(pg.as_ptr() as usize);
        }
    }
    let runtime_partial::GroupedRuntimePartial { groups, scratch_ptrs } = out;
    for (i, (_key, partial)) in groups.iter_mut().enumerate() {
        let base = NonNull::new(scratch_ptrs[i] as *mut AggPerGroup)
            .expect("entry pergroup pointer is non-null");
        runtime_partial::export_partial_with(node, &schema, base, partial)?;
    }
    Ok(true)
}

/// Failure-path reset for a half-absorbed leader table (the rescan reset's
/// perhash arm): the caller refuses to the serial arm, which must find the
/// node exactly as ExecInitAgg left it.
fn grouped_absorb_reset(node: &mut AggStateData<'_>) {
    let numgroups = node.plan.numGroups as f64;
    if let Some(ph) = node.perhash.as_mut() {
        ph.table_filled = false;
        ph.hashiter = 0;
        ph.hash_ngroups_current = 0;
        hashagg_reset_spill_state(ph, numgroups);
        ph.spill.ever_spilled = false;
        ph.spill.mode = false;
        ph.hashtable.reset();
        ph.table_ctx.reset();
    }
    // SAFETY: sole access path to the node during the reset (the rescan
    // reset's own discipline); frees the entries' aggcontext initval copies.
    unsafe { node.agg_node.as_mut() }.reset();
}

/// LEADER side: absorb the combined grouped partial into the node's OWN hash
/// table — one entry per group (key datums rebuilt canonically, pergroup
/// states written byte-for-byte via the runtime_partial absorb) — then flip
/// the table to its filled phase so the canonical retrieve emits it.
/// `Ok(false)` = refused fail-closed (touched table, group count at the
/// spill limit, or a limit crossing mid-absorb); the table is reset and the
/// serial arm proceeds as if nothing happened.
pub fn exec_agg_grouped_runtime_partials<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    combined: &[(runtime_partial::GroupKeyWords, runtime_partial::RuntimePartial)],
) -> PgResult<bool> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_HASHED);
    let mcx = estate.es_query_cxt;
    // SE-NUMJOIN (CAR 2): the absorb schema, once per call (see the
    // export's twin note).
    let schema = runtime_partial::trans_schema(node)?;
    {
        let Some(ph) = node.perhash.as_ref() else { return Ok(false) };
        if ph.table_filled
            || ph.hash_ngroups_current != 0
            || ph.spill.mode
            || ph.spill.ever_spilled
            || ph.compact.is_some()
        {
            return Ok(false);
        }
        // Group count must sit clear of the spill threshold: an absorb that
        // enters spill mode would drop groups (fail-closed pre-guard).
        if combined.len() as u64 >= ph.hash_ngroups_limit.max(1) {
            return Ok(false);
        }
    }
    for (key, partial) in combined {
        let pergroup = {
            let AggStateData { perhash, trans_init, trans_typ, agg_node, .. } = &mut *node;
            let ph = perhash.as_mut().expect("hashed Agg has perhash");
            exectuples::exec_clear_tuple(&mut ph.hashslot, mcx);
            {
                let base = ph.hashslot.base_mut();
                let desc = base
                    .tts_tupleDescriptor
                    .as_ref()
                    .expect("hashslot has a descriptor")
                    .clone();
                if key.len() > base.tts_values.len() {
                    None
                } else {
                    let mut ok = true;
                    for (i, (part, isnull)) in key.iter().enumerate() {
                        base.tts_isnull[i] = *isnull;
                        base.tts_values[i] = if *isnull {
                            Datum::null()
                        } else {
                            match part {
                                runtime_partial::GroupKeyPart::Word(w) => {
                                    grouped_key_datum(&desc.attrs[i], *w)
                                }
                                // SE-CBKEYS: rebuild the canonical
                                // varlena; an allocation failure refuses to
                                // the serial rerun (fail-closed, correct
                                // results — the grouped_absorb_reset path).
                                runtime_partial::GroupKeyPart::Bytes(b) => {
                                    match grouped_text_key_datum(mcx, b) {
                                        Ok(d) => d,
                                        Err(_) => {
                                            ok = false;
                                            break;
                                        }
                                    }
                                }
                            }
                        };
                    }
                    ok.then_some(())
                }
            }
            .and_then(|()| {
                exectuples::exec_store_virtual_tuple(&mut ph.hashslot);
                let hash = ph.hashtable.hash_slot(&mut ph.hashslot).ok()?;
                let table_mcx = ph.table_ctx.mcx();
                let (ix, isnew) = ph
                    .hashtable
                    .lookup(&mut ph.hashslot, hash, Some(table_mcx), mcx)
                    .ok()?;
                let ix = ix?;
                // Combined keys are deduplicated; a non-new entry means the
                // key round-trip diverged — refuse, never miscombine.
                if !isnew {
                    return None;
                }
                initialize_hash_entry(ph, trans_init, trans_typ, *agg_node, ix, mcx).ok()?;
                if ph.spill.mode {
                    return None;
                }
                Some(
                    ph.hashtable
                        .entry_additional(ix)
                        .expect("numtrans > 0 tables carry additional space")
                        .cast::<AggPerGroup>(),
                )
            })
        };
        match pergroup {
            Some(pg) => runtime_partial::absorb_partial_states_with(node, &schema, pg, partial)?,
            None => {
                grouped_absorb_reset(node);
                return Ok(false);
            }
        }
    }
    agg_hash_build_finish(node, estate)?;
    Ok(true)
}

/// Emit-side top-N boundary spec (lane-v2 topnemit): the resolved pergroup
/// index of the sort's leading-key aggregate, whose RAW int8 transvalue IS
/// its finalized output datum (`topn_emit_resolve` proved finalfn-none +
/// int8 + a bare-Aggref tlist entry), and the keep direction of the sort's
/// leading order operator.
#[derive(Clone, Copy)]
pub struct TopnEmitSpec {
    /// Pergroup index (the planner transno) of the ORDER BY leading-key agg.
    pub transno: u32,
    /// true = descending leading key (keep transvalues >= boundary);
    /// false = ascending (keep <= boundary).
    pub desc: bool,
}

/// One retrieve call's live boundary cut (lane-v2 topnemit): skip groups
/// whose leading-key transvalue is STRICTLY worse than `bound` — exactly the
/// groups the downstream bounded tuplesort would compare-and-discard with no
/// state change, hoisted in front of key reconstruction, finalize,
/// qual/projection and the sort put. See the invariant block at the lane's
/// `sort_feed_agg_topn`.
pub struct TopnEmitCut<'a> {
    pub spec: TopnEmitSpec,
    /// The tuplesort's current k-th boundary leading-key datum (non-null,
    /// int8; read from the FULL bounded heap's root).
    pub bound: i64,
    /// Cumulative count of boundary-skipped groups (stats evidence).
    pub skipped: &'a mut u64,
}

impl TopnEmitCut<'_> {
    /// Skip verdict for one group's leading-key pergroup state: `true` iff
    /// the transvalue is present, non-null, and STRICTLY worse than the
    /// boundary. NULL / pending transvalues always pass (their rank depends
    /// on NULLS placement; the tuplesort's comparator stays the authority).
    #[inline]
    fn skips(&self, pg: &AggPerGroup) -> bool {
        if pg.trans_value_is_null || pg.no_trans_value {
            return false;
        }
        let v = pg.trans_value.as_i64();
        if self.spec.desc {
            v < self.bound
        } else {
            v > self.bound
        }
    }
}

/// Finalfns whose evaluation is skippable for a boundary-rejected group:
/// pure arithmetic finalizations (no side effects, no reachable error paths,
/// no direct args) over int8/Int128/NumericAggState transition states. A
/// skipped group elides exactly these calls plus the projection of bare
/// Var/Const/Aggref tlist entries — nothing C could observably do.
///   1964 int8_avg (avg int2/int4)   3389 numeric_poly_avg (avg int8)
///   3388 numeric_poly_sum (sum int8) 1837 numeric_avg   3178 numeric_sum
///   3572 int2int4_sum (sum int2/int4 window final)
const TOPN_SKIPPABLE_FINALFNS: [Oid; 6] = [1837, 1964, 3178, 3388, 3389, 3572];

/// Admission for the lane's emit-side top-N boundary cut: resolve the sort's
/// leading input column `resno` (1-based over this Agg's output tlist) to the
/// pergroup transno it finalizes from, iff skipping a boundary-rejected
/// group's whole emit body is observation-free. Requires:
///   * final (not partial) single-set hashed agg, no HAVING qual (a skipped
///     qual evaluation — including its possible error — must not be elided);
///   * the resno's tlist entry is a BARE Aggref with NO finalfn whose result
///     and transition type are both int8-byval (count(*)/count(x)/sum-int
///     family): the emitted datum is the raw transvalue word, so the
///     pre-finalize compare equals the sort comparator's post-finalize one;
///   * every peragg's finalfn is absent or in `TOPN_SKIPPABLE_FINALFNS`,
///     with no direct args and no DISTINCT/ORDER BY qualifiers;
///   * every other tlist entry is a bare Var or Const (projection of a
///     skipped group evaluates nothing that could error).
pub fn topn_emit_resolve(node: &AggStateData<'_>, resno: i16) -> Option<u32> {
    if node.skip_final || node.gsets.is_some() || node.qual.is_some() {
        return None;
    }
    if node.plan.aggstrategy != AGG_HASHED {
        return None;
    }
    let mut key_transno: Option<u32> = None;
    for te_node in &node.plan.plan.targetlist {
        let te = te_node.as_target_entry()?;
        match te.expr.node_tag() {
            NodeTag::T_Var | NodeTag::T_Const => {
                if te.resno == resno {
                    // The sort key is a grouping column, not an aggregate.
                    return None;
                }
            }
            NodeTag::T_Aggref => {
                let aggref = te.expr.as_aggref().expect("tag-checked Aggref");
                if te.resno == resno {
                    let aggno = aggref.aggno;
                    if aggno < 0 || aggno as usize >= node.peragg.len() {
                        return None;
                    }
                    let pa = &node.peragg[aggno as usize];
                    let tt = &node.trans_typ[pa.transno as usize];
                    // Raw-transvalue-is-the-output family only: finalfn-none,
                    // int8 result over an int8 byval transition word.
                    if pa.finalfn.is_some()
                        || aggref.aggtype != ::types_core::catalog::INT8OID
                        || !tt.byval
                        || tt.len != 8
                    {
                        return None;
                    }
                    key_transno = Some(pa.transno);
                }
            }
            _ => return None,
        }
    }
    // Whole-emit observation-freedom: every aggregate this node finalizes
    // must be skippable (the tlist walk above already covers every OUTPUT
    // expr; peragg covers qual/tlist aggs uniformly).
    for pa in node.peragg.iter() {
        if !pa.direct_args.is_empty()
            || !pa.aggref.aggorder.is_nil()
            || !pa.aggref.aggdistinct.is_nil()
        {
            return None;
        }
        if let Some(f) = pa.finalfn.as_ref() {
            if !TOPN_SKIPPABLE_FINALFNS.contains(&f.fn_oid) {
                return None;
            }
        }
    }
    key_transno
}

/// Breaker `Source::produce` with the lane's armed top-N boundary cut:
/// `agg_retrieve_hash_table` skipping groups strictly worse than the
/// downstream bounded sort's current k-th boundary (lane-v2 topnemit).
pub fn agg_hash_retrieve_topn<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    cut: Option<TopnEmitCut<'_>>,
) -> PgResult<Option<ExecSlotId>> {
    agg_retrieve_hash_table(node, estate, cut)
}

// ===========================================================================
// Lane-v2 batchemit: batched finalize+emit straight off the compact agg
// table (the datekey lane's "batch-emit-from-compact" charter). The armed
// agg→sort feed walks the compact table in blocks and, for every surviving
// group, builds the OUTPUT ROW directly — no per-group fmgr finalize
// round-trip, no transarray re-parse through a fresh fcinfo, no first_slot
// scatter + projection interpretation, no per-group ExprContext reset (the
// reset is block-granular; every finalized image is copied by the sort put
// before the next block's reset). Admission is byte-identity-by-construction:
//   * finalfn-none byval aggs emit the raw transvalue word (identical datum);
//   * `int8_avg` / `numeric_poly_avg` / `numeric_poly_sum` route through the
//     SAME test-pinned cores the fmgr finalfns call (`ops::int64_avg_div` /
//     `int128_avg_div`, `aggregates::numeric_poly_*`) after the SAME
//     transition-state validation — the NUMERIC images are byte-identical;
//   * every other tlist entry is a bare grouping-key Var or a Const.
// Anything else refuses (`batch_emit_resolve` → None) and the feed takes the
// per-row retrieve path unchanged. Kill switch (lane side):
// PGRUST_LANE_V2_BATCHEMIT=0.
// ===========================================================================

/// One output column of the batched compact-table emit, in tlist (resno)
/// order.
pub enum BatchEmitCol {
    /// Grouping key component `j` (compact key order — the tlist Var's
    /// input attno resolved against `hash_grp_col_idx_input`).
    Key(u16),
    /// Bare tlist Const (plan-lifetime datum; the sort put copies).
    Const { value: Datum, isnull: bool },
    /// Finalfn-none byval aggregate: the raw transvalue word IS the result
    /// (count(*)/count(x)/sum(int2/int4)/min/max int families) — exactly the
    /// per-row finalize's no-finalfn arm.
    Trans(u32),
    /// `avg(int2/int4)` (finalfn `int8_avg`, oid 1964): {count,sum} int8[2]
    /// transarray → `ops::int64_avg_div` (the finalfn's exact core).
    AvgInt8(u32),
    /// `avg(int8)` (finalfn `numeric_poly_avg`, oid 3389): Int128AggState →
    /// `aggregates::numeric_poly_avg` (the finalfn's exact core).
    AvgInt128(u32),
    /// `sum(int8)` (finalfn `numeric_poly_sum`, oid 3388): Int128AggState →
    /// `aggregates::numeric_poly_sum` (the finalfn's exact core).
    SumInt128(u32),
}

/// Resolved batched-emit program + block scratch (owned by the lane's feed
/// driver; `idx` holds the current block's surviving compact row indices).
pub struct BatchEmitPlan {
    pub(crate) cols: Vec<BatchEmitCol>,
    pub(crate) idx: Vec<u32>,
    pub(crate) keyvals: Vec<(Datum, bool)>,
    pub(crate) vals: Vec<(Datum, bool)>,
}

/// The finalfns whose batched kernels are byte-identical by construction:
/// 1964 int8_avg, 3388 numeric_poly_sum, 3389 numeric_poly_avg.
const BATCH_EMIT_FINALFNS: [Oid; 3] = [1964, 3388, 3389];
/// `_int8` (int8 array) — int8_avg's declared transition type.
const INT8ARRAYOID: Oid = 1016;
/// INTERNAL — the pointer-datum transition type of the poly agg family.
const INTERNALOID: Oid = 2281;

/// Admission for the batched compact-table finalize+emit (invariant block
/// above). `None` = not admitted; the feed runs the per-row retrieve path
/// unchanged (never a lane refusal). Requires the compact table (so it runs
/// strictly AFTER the agg build), a final single-set hashed agg with no
/// HAVING qual / grouping sets / DISTINCT-ORDER transitions / subplans, and
/// every tlist entry classifiable as a [`BatchEmitCol`].
pub fn batch_emit_resolve(node: &AggStateData<'_>) -> Option<BatchEmitPlan> {
    if node.skip_final || node.gsets.is_some() || node.qual.is_some() {
        return None;
    }
    if !node.pertrans_sort.is_empty() || node.plan.aggstrategy != AGG_HASHED {
        return None;
    }
    if node.proj.has_subplan() {
        return None;
    }
    let ph = node.perhash.as_ref()?;
    ph.compact.as_ref()?;
    let mut cols: Vec<BatchEmitCol> = Vec::with_capacity(node.plan.plan.targetlist.len());
    for te_node in &node.plan.plan.targetlist {
        let te = te_node.as_target_entry()?;
        // tlists are resno-ordered; anything else refuses (the slot write
        // indexes by position).
        if te.resno as usize != cols.len() + 1 {
            return None;
        }
        match te.expr.node_tag() {
            NodeTag::T_Var => {
                let v = te.expr.as_var()?;
                if v.varno != ::execexpr::OUTER_VAR || v.varlevelsup != 0 {
                    return None;
                }
                let j = ph
                    .hash_grp_col_idx_input
                    .iter()
                    .position(|&a| i32::from(a) == i32::from(v.varattno))?;
                // Only the grouping-key components are reconstructable from
                // the compact table; a stored EXTRA column (a functionally-
                // dependent tlist Var beyond the key, fdgroup-wr) has no
                // compact read-back — refuse (the arming gates already
                // refuse such shapes; this keeps the resolve honest).
                if j >= ph.num_cols {
                    return None;
                }
                cols.push(BatchEmitCol::Key(j as u16));
            }
            NodeTag::T_Const => {
                let c = te.expr.as_const()?;
                cols.push(BatchEmitCol::Const { value: c.constvalue, isnull: c.constisnull });
            }
            NodeTag::T_Aggref => {
                let aggref = te.expr.as_aggref().expect("tag-checked Aggref");
                let aggno = aggref.aggno;
                if aggno < 0 || aggno as usize >= node.peragg.len() {
                    return None;
                }
                let pa = &node.peragg[aggno as usize];
                let col = match pa.finalfn.as_ref() {
                    None => {
                        // Raw-transvalue emission requires a byval word (the
                        // per-row arm's read-only marking never runs on
                        // byval transtypes, so the datum is identical).
                        let tt = &node.trans_typ[pa.transno as usize];
                        if !tt.byval || pa.aggref.aggtranstype == INTERNALOID {
                            return None;
                        }
                        BatchEmitCol::Trans(pa.transno)
                    }
                    Some(f) => match f.fn_oid {
                        1964 if pa.aggref.aggtranstype == INT8ARRAYOID => {
                            BatchEmitCol::AvgInt8(pa.transno)
                        }
                        3389 if pa.aggref.aggtranstype == INTERNALOID => {
                            BatchEmitCol::AvgInt128(pa.transno)
                        }
                        3388 if pa.aggref.aggtranstype == INTERNALOID => {
                            BatchEmitCol::SumInt128(pa.transno)
                        }
                        _ => return None,
                    },
                };
                cols.push(col);
            }
            _ => return None,
        }
    }
    // Whole-emit equivalence: every aggregate this node would finalize must
    // be in the batched vocabulary (with no qual, peragg ⊆ tlist aggs — this
    // sweep is the belt-and-braces mirror of topn_emit_resolve's).
    for pa in node.peragg.iter() {
        if !pa.direct_args.is_empty()
            || !pa.aggref.aggorder.is_nil()
            || !pa.aggref.aggdistinct.is_nil()
        {
            return None;
        }
        match pa.finalfn.as_ref() {
            None => {
                if !node.trans_typ[pa.transno as usize].byval {
                    return None;
                }
            }
            Some(f) => {
                if !BATCH_EMIT_FINALFNS.contains(&f.fn_oid) {
                    return None;
                }
            }
        }
    }
    Some(BatchEmitPlan { cols, idx: Vec::new(), keyvals: Vec::new(), vals: Vec::new() })
}

/// SE-AGGPOLY (band 101001): every aggregate's ARGUMENT expressions (and
/// FILTER, belt-and-braces — the poly manifest refuses filters anyway) —
/// the helper-side evaluation surface of a poly runtime engagement: the
/// per-row transition programs run on helpers, while finalize + HAVING +
/// projection run on the leader. `None` = a non-TargetEntry argument form
/// (refuse fail-closed). The caller applies its parallel-safety walker to
/// every returned node.
pub fn agg_poly_arg_exprs<'mcx>(
    node: &AggStateData<'mcx>,
) -> Option<Vec<::types_nodes::node_tree::Node<'mcx>>> {
    let mut out = Vec::new();
    for pa in node.peragg.iter() {
        for a in pa.aggref.args.iter() {
            let tle = a.as_target_entry()?;
            out.push(tle.expr);
        }
        if let Some(f) = pa.aggref.aggfilter {
            out.push(f);
        }
    }
    Some(out)
}

/// Lane-v2 fold plan classified at init; None = the lane is off, the shape
/// can never engage the breaker, or no transition admits (`lanefold::classify`
/// returned None).
pub fn agg_lanefold_plan<'a, 'mcx>(
    node: &'a AggStateData<'mcx>,
) -> Option<&'a ::lanefold::LanePlan<'mcx>> {
    node.lanefold.as_ref().map(|lf| &lf.plan)
}

/// The node's aggcontext arena — C's curaggcontext, the context transfns
/// reach via fcinfo->context (`AggCheckCallContext`) and where by-ref
/// transvalues are datumCopy'd (execexpr's `agg_datum_copy` target). The lane
/// fold allocates INTERNAL transition states (lanefold `Int128AvgAccum`) here
/// so fold-fed and per-row/demoted batches accumulate into one shared state,
/// and str-kind transvalue copies land exactly where the per-row program's
/// would.
pub fn agg_aggcontext<'a>(node: &'a AggStateData<'_>) -> ::mcx::Mcx<'a> {
    // SAFETY: agg_node is the node's own arena-boxed AggStateNode, live for
    // the node's lifetime; no &mut to it is formed during this borrow.
    unsafe { node.agg_node.as_ref() }.aggcontext()
}

/// Lane-v2 staged join-feed admission inputs: the outer columns the hashed
/// build reads per row — C find_cols' `colnos_needed` (grouping + hashed +
/// unaggregated + aggregated input columns; exactly the spill projection's
/// column set) — plus the deform bound (`max_colno_needed`). A staged replay
/// slot carrying exactly these columns (others NULL) is observation-identical
/// to the original input slot for the whole build: the probe hashes grouping
/// columns, the transition programs read the aggregated inputs, and a spilled
/// tuple materializes exactly the needed columns (the spill projection nulls
/// the unneeded ones anyway — `hashagg_spill_tuple`'s wslot arm).
pub fn agg_hash_needed_cols<'a>(node: &'a AggStateData<'_>) -> (&'a [bool], i32) {
    let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
    (&ph.spill.colnos_needed, ph.spill.max_colno_needed)
}
/// Lane-v2 fold-feed probe: `agg_hash_build_accept` with the transition
/// program split — prepare/lookup per row (spill-mode misses spill the tuple
/// identically), then only the RESIDUAL transitions (the transnos classify
/// refused) run per-row; the admitted transitions are folded per batch by the
/// caller (`lanefold::fold_rows_grouped`) over the returned pergroup
/// snapshot. Transition-major reordering across independent pergroup cells is
/// bit-invisible (the fold kernels are commutative and non-erroring on
/// admitted/guard-proven data; residual transitions still run in row order).
/// None = spill-mode miss: no transition runs, exactly as the per-row build.
pub fn agg_hash_build_probe_resid<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
) -> PgResult<Option<NonNull<AggPerGroup>>> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_HASHED);
    estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
    let mut pg = None;
    if lookup_hash_entry(node, estate, outer_id)? {
        let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
        // SE-GROUPONLY: zero-transition builds have no pergroup array —
        // lookup_hash_entry never writes the cell (trans_init is empty on
        // both its arms). Hand back a DANGLING sentinel: the caller only
        // forwards it to the fold, and the empty plan folds nothing (the
        // resid arm below is None too), so it is never dereferenced.
        pg = Some(if node.trans_init.is_empty() {
            NonNull::dangling()
        } else {
            // SAFETY: lookup_hash_entry installed the entry's live pergroup
            // in the cell (numtrans > 0 on this arm).
            unsafe { ph.pergroup_cell.as_ptr().read() }
        });
        if let Some(resid) =
            node.lanefold.as_mut().and_then(|lf| lf.resid.as_mut())
        {
            let outer_slot = estate.slot_mut(outer_id);
            let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
            exec_eval_expr(resid, &mut slots)?;
        }
    }
    estate.reset_expr_context(node.tmpcontext);
    Ok(pg)
}

/// Expr-key feed resid leg: run only the RESIDUAL transitions for a row whose
/// group the caller already resolved (the per-epoch code→pergroup cache) —
/// `agg_hash_build_probe_resid` with the lookup replaced by installing the
/// cached pergroup in the cell the resid program reads. Byte-identical to the
/// probe leg for found-existing groups: `lookup_hash_entry`'s only effect on
/// a hit is that same cell write.
pub fn agg_hash_build_resid_group<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
    pg: NonNull<AggPerGroup>,
) -> PgResult<()> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_HASHED);
    estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
    {
        let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
        // SAFETY: the once-allocated cell every probe leg writes; the resid
        // program reads it through the same pointer.
        unsafe { ph.pergroup_cell.as_ptr().write(pg) };
    }
    if let Some(resid) = node.lanefold.as_mut().and_then(|lf| lf.resid.as_mut()) {
        let outer_slot = estate.slot_mut(outer_id);
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
        exec_eval_expr(resid, &mut slots)?;
    }
    estate.reset_expr_context(node.tmpcontext);
    Ok(())
}

// ===========================================================================
// Lane-v2 plain-agg (AGG_PLAIN, ungrouped) fold-drive delegation seam. The
// lane's fold drive (execmain/src/lanev2.rs) owns the batched feed; these
// thin entry points delegate every substantive step to the SAME row-path
// machinery `exec_agg` / `exec_agg_batched` use — initialize_aggregates, the
// per-row transition program, and the canonical `plain_finish`
// finalize/HAVING/project tail (one result row; zero-row input included).
// ===========================================================================

/// Every DISTINCT/ORDER-BY-within-aggregate internal-sort entry is a
/// lane-hosted exact-DISTINCT set (distinctset module; admission matrix on
/// `distinct_set_kind`), and there is at least one. Mixed nodes — any
/// tuplesort-mode or presorted entry alongside — refuse wholesale: the lane
/// hosts only the all-set shape (allowlist discipline; the C paths keep
/// everything else).
pub fn agg_pertrans_all_distinct_set(node: &AggStateData<'_>) -> bool {
    // `force_distinct_set` counts armed presorted entries as set-mode: once a
    // narrow/skip-sort drive armed the node, every later admission must see
    // the entries the way the collect/replay run them (sticky, value-safe —
    // the force doc).
    !node.pertrans_sort.is_empty()
        && node.pertrans_sort.iter().all(|ps| ps.set_active(node.force_distinct_set))
}

/// Skip-sort admission (lanev2 `try_own_plain_distinct_agg_over_sort`):
/// EVERY transition of this AGG_PLAIN node is replayed from an
/// exact-DISTINCT set — each transno has a pertrans_sort entry and each
/// entry is set-capable (presorted included: the drive arms
/// `force_distinct_set`, converting the plan-Sort-served adjacent-dedup to
/// the exact set). That total coverage is what makes SKIPPING the plan's
/// Sort legal beyond row order: no transition ever observes input order
/// (the per-row program only parks; all aggregation happens at the replay,
/// whose transfns are the order-insensitive allowlist), so same rows in ⇒
/// same values out — the order-relaxation charter's exact grant.
pub fn agg_plain_distinct_set_only(node: &AggStateData<'_>) -> bool {
    node.plan.aggstrategy == AGG_PLAIN
        && node.gsets.is_none()
        && node.merge.is_none()
        && node.pertrans_sort.len() == node.numtrans
        && node.pertrans_sort.iter().all(|ps| ps.set_kind.is_some())
        && node.evaltrans.as_deref().is_some_and(|et| !et.has_subplan())
        && node
            .evaltrans
            .as_deref()
            .is_none_or(|et| et.param_exec_deps().is_empty())
        && node.proj.param_exec_deps().is_empty()
        && node.qual.as_deref().is_none_or(|q| q.param_exec_deps().is_empty())
}

/// Arm skip-sort set-mode (see `force_distinct_set` field doc): the lane's
/// skip-sort drive calls this before the build so presorted entries collect
/// into sets. Sticky by design — value-safe on later per-tuple fallbacks.
pub fn agg_force_distinct_set(node: &mut AggStateData<'_>) {
    debug_assert!(agg_plain_distinct_set_only(node));
    node.force_distinct_set = true;
}

/// Agg-side admission for the lane-v2 plain-agg exact-DISTINCT drive
/// (execmain lanev2.rs `try_own_plain_distinct_agg_over_seq_scan`):
/// AGG_PLAIN whose every internal-sort entry is a set-mode exact-DISTINCT
/// (`agg_pertrans_all_distinct_set` — such nodes are NOT batch-drainable, so
/// the fold drive and the legacy fused arm never see them), single grouping
/// set, no merge phase, subplan-free transitions, initplan-param-free
/// everywhere (the lane drive does not hoist pending initplans). The drive
/// itself is `agg_plain_build_begin` + per-row `agg_plain_build_accept`
/// (whose collect feeds the sets) + `agg_plain_finish` (whose
/// process_ordered_aggregates replays them).
pub fn agg_plain_distinct_set_admissible(node: &AggStateData<'_>) -> bool {
    node.plan.aggstrategy == AGG_PLAIN
        && node.gsets.is_none()
        && node.merge.is_none()
        && agg_pertrans_all_distinct_set(node)
        && node.evaltrans.as_deref().is_some_and(|et| !et.has_subplan())
        && node
            .evaltrans
            .as_deref()
            .is_none_or(|et| et.param_exec_deps().is_empty())
        && node.proj.param_exec_deps().is_empty()
        && node.qual.as_deref().is_none_or(|q| q.param_exec_deps().is_empty())
}

/// Agg-side admission for the lane-v2 plain-agg fold drive: batch-drainable,
/// AGG_PLAIN, a classified fold plan, and initplan-param-free (the lane
/// drive, like `exec_agg_batched`, does not hoist pending initplans).
pub fn agg_plain_fold_admissible(node: &AggStateData<'_>) -> bool {
    agg_batch_drainable(node)
        && node.plan.aggstrategy == AGG_PLAIN
        && node.lanefold.is_some()
        && node
            .evaltrans
            .as_deref()
            .is_none_or(|et| et.param_exec_deps().is_empty())
        && node.proj.param_exec_deps().is_empty()
        && node.qual.as_deref().is_none_or(|q| q.param_exec_deps().is_empty())
}

/// Agg-side admission for the lane-v2 plain-agg PER-ROW drain feed (the
/// pgrcolumnar no-qual-feed tranche): `agg_plain_fold_admissible` minus the
/// classified-fold-plan requirement — the per-row feed runs the FULL per-row
/// transition program (`agg_plain_build_accept`) over batch-decoded staged
/// windows, so arbitrary transition expressions are hosted. Same
/// batch-drainable + initplan-param-free gates as the fold drive.
pub fn agg_plain_perrow_admissible(node: &AggStateData<'_>) -> bool {
    agg_batch_drainable(node)
        && node.plan.aggstrategy == AGG_PLAIN
        && node
            .evaltrans
            .as_deref()
            .is_none_or(|et| et.param_exec_deps().is_empty())
        && node.proj.param_exec_deps().is_empty()
        && node.qual.as_deref().is_none_or(|q| q.param_exec_deps().is_empty())
}

/// Feed-phase begin: `exec_agg`'s `initialize_aggregates` (fresh initval
/// pergroups — a rescan re-enters here with `agg_done` cleared).
pub fn agg_plain_build_begin<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_PLAIN);
    initialize_aggregates(node, estate)
}

/// One outer row through the FULL per-row transition program — `exec_agg`'s
/// single-group loop body verbatim, ordered-input collection included (a
/// no-op for the fold drive's shapes, whose admission requires
/// `pertrans_sort` empty; the exact-DISTINCT drive's set entries collect
/// here). Demoted/fallback rows and scalar-qual feeds run here.
pub fn agg_plain_build_accept<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
) -> PgResult<()> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_PLAIN);
    estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
    {
        let outer_slot = estate.slot_mut(outer_id);
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
        exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)?;
    }
    if !node.pertrans_sort.is_empty() {
        collect_ordered_input(node, estate, 1)?;
    }
    estate.reset_expr_context(node.tmpcontext);
    Ok(())
}

/// Direct staged-key feed admission (the lane distinct drives' batched arm):
/// the node's ONE transition is a set-mode exact-DISTINCT whose argument is
/// exactly outer column 0 with no FILTER (`direct_att == Some(0)`). For that shape the
/// per-row transition program's entire effect is "park outer column 0 +
/// flag" and the collect inserts it into the set — so feeding the staged
/// scan key lane straight into the set (`agg_plain_distinct_insert_batch`
/// for integer keys, `agg_plain_distinct_insert_bytes_batch` /
/// `agg_plain_distinct_insert_dict_batch` for text keys over the varlena
/// key staging) reproduces the per-row feed value-for-value (order within
/// the set is replay-invisible; admission).
pub fn agg_plain_distinct_direct_shape(node: &AggStateData<'_>) -> bool {
    node.numtrans == 1 && node.pertrans_sort.len() == 1 && {
        let ps = &node.pertrans_sort[0];
        ps.set_active(node.force_distinct_set)
            && ps.num_inputs == 1
            && ps.direct_att == Some(0)
            && ps.set_kind.is_some()
    }
}

/// Whether the direct-shape node's single set-mode key is text/varchar
/// (`DistinctKeyKind::Bytes`) — the lane drives' dispatch between the
/// fixed-width staged key feed and the varlena/dict-code key feed.
pub fn agg_plain_distinct_key_is_bytes(node: &AggStateData<'_>) -> bool {
    debug_assert!(agg_plain_distinct_direct_shape(node));
    node.pertrans_sort[0].set_kind == Some(distinctset::DistinctKeyKind::Bytes)
}

/// One staged batch of the direct-feed drive: `keys` are the batch's
/// NON-NULL key datums in row order (`saw_null` folds the batch's NULLs —
/// the set collapses every NULL to one `seen_null` anyway), `hashes`/`ints`
/// are caller-owned scratch. Equivalent to the per-row program+collect over
/// the same rows (`agg_plain_distinct_direct_shape` is the caller's
/// obligation). The budget check runs once per batch, so the set may
/// overshoot by at most one staged page batch before spilling/degrading.
/// A group already degraded to its tuplesort keeps feeding it here (values
/// and NULLs alike are sort inputs whose drain re-dedups — one NULL stands
/// for the batch's many, which dedup to one either way).
pub fn agg_plain_distinct_insert_batch<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    keys: &[Datum],
    saw_null: bool,
    ints: &mut Vec<i64>,
    hashes: &mut Vec<u64>,
) -> PgResult<()> {
    debug_assert!(agg_plain_distinct_direct_shape(node));
    let ps = &mut node.pertrans_sort[0];
    let kind = ps.set_kind.expect("set-mode pertrans");
    if ps.dset_degraded {
        let sort = ps.sortstates[0].as_mut().expect("degraded group has a sortstate");
        for &d in keys {
            sort.putdatum(d, false)?;
        }
        if saw_null {
            sort.putdatum(Datum::null(), true)?;
        }
        return Ok(());
    }
    ints.clear();
    match kind {
        distinctset::DistinctKeyKind::Int16 => {
            ints.extend(keys.iter().map(|d| d.as_i16() as i64));
        }
        distinctset::DistinctKeyKind::Int32 => {
            ints.extend(keys.iter().map(|d| d.as_i32() as i64));
        }
        distinctset::DistinctKeyKind::Int64 => {
            ints.extend(keys.iter().map(|d| d.as_i64()));
        }
        distinctset::DistinctKeyKind::Bytes => {
            unreachable!("bytes keys take agg_plain_distinct_insert_bytes_batch")
        }
    }
    let dset = ps.dset.get_or_insert_with(distinctset::DistinctSet::new);
    dset.insert_i64_batch(ints, hashes);
    if saw_null {
        dset.seen_null = true;
    }
    let budget = distinct_set_budget();
    if dset.over_budget(budget) {
        distinct_set_overflow(ps, estate.es_query_cxt, budget)?;
    }
    Ok(())
}

/// One staged window of the direct-feed drive consumed as the scan's KEY
/// LANE (hot-gap lever C2, the int-key count(DISTINCT) class): `vals`/`isnull` are the staged key
/// column's value and null lanes for the window's rows, in row order.
/// Equivalent to `agg_plain_distinct_insert_batch` over the same rows —
/// NULLs elided value-for-value and folded into the set's one `seen_null` —
/// with the null scan hoisted to once per window (the all-non-null arm is
/// one straight datum→i64 pass; pgrcolumnar lanes are null-free by the format).
/// Same batch-granular budget-check/overflow contract; a degraded group
/// keeps feeding its tuplesort (non-null values in row order + one NULL —
/// one stands for the window's many, which dedup to one either way).
pub fn agg_plain_distinct_insert_lane_batch<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    vals: &[Datum],
    isnull: &[bool],
    ints: &mut Vec<i64>,
    hashes: &mut Vec<u64>,
) -> PgResult<()> {
    debug_assert!(agg_plain_distinct_direct_shape(node));
    debug_assert_eq!(vals.len(), isnull.len());
    let saw_null = isnull.contains(&true);
    let ps = &mut node.pertrans_sort[0];
    let kind = ps.set_kind.expect("set-mode pertrans");
    if ps.dset_degraded {
        let sort = ps.sortstates[0].as_mut().expect("degraded group has a sortstate");
        for (&d, &nl) in vals.iter().zip(isnull) {
            if !nl {
                sort.putdatum(d, false)?;
            }
        }
        if saw_null {
            sort.putdatum(Datum::null(), true)?;
        }
        return Ok(());
    }
    ints.clear();
    match kind {
        distinctset::DistinctKeyKind::Int16 => {
            if saw_null {
                ints.extend(
                    vals.iter().zip(isnull).filter(|&(_, &nl)| !nl).map(|(d, _)| d.as_i16() as i64),
                );
            } else {
                ints.extend(vals.iter().map(|d| d.as_i16() as i64));
            }
        }
        distinctset::DistinctKeyKind::Int32 => {
            if saw_null {
                ints.extend(
                    vals.iter().zip(isnull).filter(|&(_, &nl)| !nl).map(|(d, _)| d.as_i32() as i64),
                );
            } else {
                ints.extend(vals.iter().map(|d| d.as_i32() as i64));
            }
        }
        distinctset::DistinctKeyKind::Int64 => {
            if saw_null {
                ints.extend(vals.iter().zip(isnull).filter(|&(_, &nl)| !nl).map(|(d, _)| d.as_i64()));
            } else {
                ints.extend(vals.iter().map(|d| d.as_i64()));
            }
        }
        distinctset::DistinctKeyKind::Bytes => {
            unreachable!("bytes keys take agg_plain_distinct_insert_bytes_batch")
        }
    }
    let dset = ps.dset.get_or_insert_with(distinctset::DistinctSet::new);
    dset.insert_i64_batch(ints, hashes);
    if saw_null {
        dset.seen_null = true;
    }
    let budget = distinct_set_budget();
    if dset.over_budget(budget) {
        distinct_set_overflow(ps, estate.es_query_cxt, budget)?;
    }
    Ok(())
}

/// One staged batch of the direct-feed drive, TEXT keys (the varlena key
/// staging): `keys` are the batch's NON-NULL key datums in row order — live
/// text/varchar varlena pointers (in-page on heap, decoded images on
/// pgrcolumnar). Each detoasts exactly as the per-row collect does
/// (`datum_varlena_packed` into per-tuple memory — reset once per batch here
/// instead of per row: a lifetime-only difference, the set retains its own
/// canonical image) and inserts its content bytes. Same batch-granular
/// budget-check/overflow contract as the integer feed; a degraded group
/// keeps feeding its tuplesort (raw datums — the sort copies, its drain
/// re-dedups).
pub fn agg_plain_distinct_insert_bytes_batch<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    keys: &[Datum],
    saw_null: bool,
) -> PgResult<()> {
    debug_assert!(agg_plain_distinct_direct_shape(node));
    let tmp = node.tmpcontext;
    let ps = &mut node.pertrans_sort[0];
    debug_assert_eq!(ps.set_kind, Some(distinctset::DistinctKeyKind::Bytes));
    if ps.dset_degraded {
        let sort = ps.sortstates[0].as_mut().expect("degraded group has a sortstate");
        for &d in keys {
            sort.putdatum(d, false)?;
        }
        if saw_null {
            sort.putdatum(Datum::null(), true)?;
        }
        return Ok(());
    }
    let dset = ps.dset.get_or_insert_with(distinctset::DistinctSet::new);
    for &d in keys {
        // SAFETY: non-null live text/varchar varlena — the admission proved
        // the argument type; detoast copies land in per-tuple memory.
        let v = unsafe {
            ::types_fmgr::datum_varlena_packed(d, estate.ecxt(tmp).per_tuple_mcx())
        }?;
        dset.insert_bytes(v.data());
    }
    if saw_null {
        dset.seen_null = true;
    }
    let budget = distinct_set_budget();
    if dset.over_budget(budget) {
        distinct_set_overflow(ps, estate.es_query_cxt, budget)?;
    }
    estate.reset_expr_context(tmp);
    Ok(())
}

/// One staged batch of the direct-feed drive, DICT-CODED text keys (the
/// pgrcolumnar zero-decode dict lane): `codes` are the batch's per-row u32 codes
/// into `dict` (the row group's decoded-Datum dictionary; NULL-free by the
/// dict-lane contract), and `memo` is the caller's IDENTITY-SCOPED per-code
/// dedup bitmap (cleared by the caller whenever the memo identity changes).
/// Without a stitch the identity is the dict epoch and the memo is indexed
/// by local code (≥ dict.len() bits). With `stitch` (`Some`, the v7
/// part-global dictionary): the identity is the scan-stable gepoch, the
/// memo is indexed by PART-GLOBAL code (`stitch[local]`, ≥ gndv bits) and
/// never clears at epoch rolls — each distinct string is detoasted +
/// hashed + inserted once per part instead of once per row group. A memo
/// hit means this code's value was already FED this identity — to the
/// in-memory set, a spill tape, or the degraded tuplesort — all of which
/// dedup exactly, so skipping the repeat insert is value-invisible: the
/// distinct-value multiset each consumer sees is unchanged (this holds
/// across a mid-identity degrade too: every memo-marked value was fed to a
/// structure the degrade replays). The memo NEVER substitutes codes for set
/// elements — the set stores the full content bytes (codes, global or
/// local, are scan-scoped identities, so ids-as-elements would break
/// exactness; ids only serve as an insert filter).
pub fn agg_plain_distinct_insert_dict_batch<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    codes: &[u32],
    dict: &[Datum],
    stitch: Option<&[u32]>,
    memo: &mut [u64],
) -> PgResult<()> {
    debug_assert!(agg_plain_distinct_direct_shape(node));
    debug_assert!(stitch.is_none_or(|s| s.len() == dict.len()));
    debug_assert!(stitch.is_some() || memo.len() * 64 >= dict.len());
    // Memo bit index for a local code: part-global when stitched (bitmap
    // indexed 0..gndv, never reset across row groups), local otherwise.
    let bit = |c: u32| -> usize {
        match stitch {
            Some(s) => s[c as usize] as usize,
            None => c as usize,
        }
    };
    let tmp = node.tmpcontext;
    let ps = &mut node.pertrans_sort[0];
    debug_assert_eq!(ps.set_kind, Some(distinctset::DistinctKeyKind::Bytes));
    if ps.dset_degraded {
        // Degraded group: feed each identity-new value once (the sort's
        // drain re-dedups; feeding one representative per value is the same
        // distinct multiset the per-row feed produces).
        let sort = ps.sortstates[0].as_mut().expect("degraded group has a sortstate");
        for &c in codes {
            let i = bit(c);
            let (w, b) = (i / 64, i % 64);
            if memo[w] >> b & 1 == 0 {
                memo[w] |= 1 << b;
                sort.putdatum(dict[c as usize], false)?;
            }
        }
        return Ok(());
    }
    let dset = ps.dset.get_or_insert_with(distinctset::DistinctSet::new);
    for &c in codes {
        let i = bit(c);
        let (w, b) = (i / 64, i % 64);
        if memo[w] >> b & 1 == 0 {
            memo[w] |= 1 << b;
            // SAFETY: dict entries are live decoded text varlena images
            // (never external/compressed — the decode produced them); the
            // packed read is a no-copy header decode.
            let v = unsafe {
                ::types_fmgr::datum_varlena_packed(
                    dict[c as usize],
                    estate.ecxt(tmp).per_tuple_mcx(),
                )
            }?;
            dset.insert_bytes(v.data());
        }
    }
    let budget = distinct_set_budget();
    if dset.over_budget(budget) {
        distinct_set_overflow(ps, estate.es_query_cxt, budget)?;
    }
    estate.reset_expr_context(tmp);
    Ok(())
}

/// One outer row through only the RESIDUAL transitions (the transnos
/// classify refused); the admitted transitions are folded per batch by the
/// caller (`lanefold::fold_batch`) over `agg_plain_pergroup_base`. No-op when
/// the plan admitted every transition.
pub fn agg_plain_build_accept_resid<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
) -> PgResult<()> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_PLAIN);
    let Some(resid) = node.lanefold.as_mut().and_then(|lf| lf.resid.as_mut()) else {
        return Ok(());
    };
    estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
    let outer_slot = estate.slot_mut(outer_id);
    let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
    exec_eval_expr(resid, &mut slots)?;
    estate.reset_expr_context(node.tmpcontext);
    Ok(())
}

/// The single group's once-allocated pergroup array (the fold target).
pub fn agg_plain_pergroup_base(node: &AggStateData<'_>) -> NonNull<AggPerGroup> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_PLAIN);
    node.pergroup_base
}

/// Bare-count storeless shape probe: the node's WHOLE transition program is
/// one `int8inc(transvalue)` — count(*) reading no input columns — compiled
/// to a byval kernel (`ExprState::agg_count_star`'s contract). The shape
/// `exec_agg_batched`'s storeless arm advances once per page batch; the
/// runtime direct morsel drive admits exactly it (bare heap count(*)).
/// FILTER, count(expr), or any second transition compiles to
/// `Kernel::Program` and probes false.
pub fn agg_plain_count_star_shape(node: &AggStateData<'_>) -> bool {
    node.plan.aggstrategy == AGG_PLAIN
        && node.evaltrans.as_deref().is_some_and(|et| et.agg_count_star().is_some())
}

/// One page batch of `n` VISIBLE rows through the bare-count storeless
/// advance — `exec_agg_batched`'s storeless count(*) arm verbatim: one
/// checked add per batch, one tmpcontext reset per batch (the per-row
/// resets are no-ops for this shape: the transition allocates nothing). A
/// refused advance (int8 overflow / null transvalue under a non-strict
/// call) re-runs the batch through the per-row kernel so the ereport rises
/// at exactly C's row. Caller contract: `agg_plain_count_star_shape` holds
/// and `n` counts visible qual-free rows (`BatchSource::storeless_ok`).
pub fn agg_plain_count_star_accept_batch<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    n: u32,
) -> PgResult<()> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_PLAIN);
    if let Some((pergroup, strict)) =
        node.evaltrans.as_deref().and_then(|et| et.agg_count_star())
    {
        if ::execexpr::agg_count_star_advance(pergroup, strict, n) {
            estate.reset_expr_context(node.tmpcontext);
            return Ok(());
        }
    }
    // Refused advance (or a diverged shape): the per-row storeless kernel —
    // input-free transitions store no slot (exec_agg_batched's per-row
    // storeless loop, ereport-at-C's-row parity).
    for _ in 0..n {
        let mut slots = EvalSlots::default();
        exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)?;
        estate.reset_expr_context(node.tmpcontext);
    }
    Ok(())
}

/// Retrieve: `exec_agg`'s post-drain tail (finalize + HAVING + project, sets
/// `agg_done`) — the one result row, C's zero-row contract included.
pub fn agg_plain_finish<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_PLAIN);
    plain_finish(node, estate)
}

// ===========================================================================
// Lane-v2 parallel exact-DISTINCT partials (pardistinct.rs) — the leader-
// side seams: spec derivation from the initialized aggregate slices, and
// the plain-shape merged adoption. The grouped adoption lives in
// hashgrouped.rs (it reuses that arm's Emit machinery wholesale).
// ===========================================================================

/// Derive the parallel-DISTINCT build recipe from this node's initialized
/// aggregate slices. `desc` is the OUTER tuple descriptor (the row shape
/// both the workers' scans and the GatherMerge stream produce). `None`
/// refuses: any transition outside the exact-integer vocabulary
/// (`pardistinct::vocab_kind` — `order_insensitive_exact_transfn` minus the
/// Int128 family), any non-Var / FILTERed argument, or a group key type
/// outside int2/int4/int8 (+ text/varchar iff `admit_text_keys` — the
/// distinct-bytes car; see the `key_kind` contract note in the body).
/// Derivation treats presorted entries as set-mode (the arm always
/// arms `force_distinct_set` before engaging — but only AFTER every refusal
/// point, so a refusal leaves the classic path's adjacent-dedup untouched).
/// Env-gated derive-refusal diagnosis (PGRUST_LANE_V2_TRACE — the lane's
/// trace channel; pd_derive_spec is a pure Option chain, so the refusal
/// POINT is otherwise invisible to the arm's traces).
#[cold]
fn pd_derive_trace(msg: &str) {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_LANE_V2_TRACE").as_deref(), Ok("1") | Ok("on"))
    }) {
        eprintln!("[lane-v2] pd_derive_spec refused: {msg}");
    }
}

pub fn pd_derive_spec(
    node: &AggStateData<'_>,
    desc: &TupleDescData<'_>,
    admit_text_keys: bool,
    admit_datetime: bool,
) -> Option<std::sync::Arc<pardistinct::PdSpec>> {
    use pardistinct::{PdInt, PdKeyKind, PdSetSpec, PdSpec, PdVocab};
    const INT2OID: Oid = 21;
    const INT4OID: Oid = 23;
    const INT8OID: Oid = 20;
    const TEXTOID: Oid = 25;
    const VARCHAROID: Oid = 1043;
    const DATEOID: Oid = 1082;
    const TIMESTAMPOID: Oid = 1114;
    const TIMESTAMPTZOID: Oid = 1184;
    let int_kind = |t: Oid| match t {
        INT2OID => Some(PdInt::I16),
        INT4OID => Some(PdInt::I32),
        INT8OID => Some(PdInt::I64),
        _ => None,
    };
    // GL-LOWDIST-3: the SET-ARG width vocabulary — int family always;
    // datetime (i32 date / i64 timestamp+timestamptz word equality, the
    // distinct_set_kind argument) only under the caller's contract. The
    // runtime distinct SINK passes `distinct_datetime_enabled()`; the
    // Gather-era pardistinct HYBRIDS pass false and refuse datetime sets
    // cleanly (D1 deletion list — sink+serial only keeps the displacement
    // direction). Vocab args and group keys stay int/text (unchanged).
    let set_arg_kind = |t: Oid| -> Option<PdInt> {
        if let Some(k) = int_kind(t) {
            return Some(k);
        }
        if !admit_datetime {
            return None;
        }
        match t {
            DATEOID => Some(PdInt::I32),
            TIMESTAMPOID | TIMESTAMPTZOID => Some(PdInt::I64),
            _ => None,
        }
    };
    // Group-key component kind. `admit_text_keys` is the caller's CONTRACT
    // that byte equality is the grouping operator's verdict for text
    // columns (the runtime distinct sink passes it only after
    // `agg_hashgroup_admissible` proved `group_eq_representational` texteq
    // under a deterministic collation — bpchar and nondeterministic
    // collations never pass that admission). The Gather-era arms pass
    // false: their merge/emit surfaces stay integer-key-only.
    let key_kind = |t: Oid| -> Option<PdKeyKind> {
        if let Some(k) = int_kind(t) {
            return Some(PdKeyKind::Int(k));
        }
        (admit_text_keys && matches!(t, TEXTOID | VARCHAROID)).then_some(PdKeyKind::Bytes)
    };
    // The aggregate's single plain-Var argument (0-based outer attno).
    let arg_att = |ar: &::types_nodes::primnodes::Aggref<'_>| -> Option<u16> {
        if ar.aggfilter.is_some() || ar.args.len() != 1 {
            return None;
        }
        let tle = ar.args.iter().next()?.as_target_entry()?;
        let v = tle.expr.as_var()?;
        (v.varno == ::execexpr::OUTER_VAR
            && v.varlevelsup == 0
            && v.varattno >= 1
            && (v.varattno as i32) <= desc.natts)
            .then(|| (v.varattno - 1) as u16)
    };
    let mut max_att = 0i32;
    let mut key_atts = Vec::with_capacity(node.plan.grpColIdx.len());
    let mut key_kinds = Vec::with_capacity(node.plan.grpColIdx.len());
    for &col in node.plan.grpColIdx {
        if col < 1 || (col as i32) > desc.natts {
            return None;
        }
        key_atts.push((col - 1) as u16);
        let Some(kind) = key_kind(desc.attr((col - 1) as usize).atttypid) else {
            pd_derive_trace("group key column outside the int/text vocabulary");
            return None;
        };
        key_kinds.push(kind);
        max_att = max_att.max(col as i32);
    }
    if key_atts.len() > 32 {
        return None;
    }
    // DISTINCT transitions, in pertrans_sort order (the emit re-installs
    // merged sets into those slots).
    let mut sets = Vec::with_capacity(node.pertrans_sort.len());
    let mut set_transnos: Vec<usize> = Vec::with_capacity(node.pertrans_sort.len());
    for ps in node.pertrans_sort.iter() {
        let Some(kind) = ps.set_kind else {
            pd_derive_trace("set transition without set_kind");
            return None;
        };
        if !ps.set_active(true) || ps.num_inputs != 1 {
            pd_derive_trace("set transition inactive or multi-input");
            return None;
        }
        let pa = node.peragg.iter().find(|pa| pa.transno as usize == ps.transno)?;
        if !pa.aggref.aggorder.is_nil() || pa.aggref.aggdistinct.is_nil() {
            pd_derive_trace("set aggref has aggorder / lacks aggdistinct");
            return None;
        }
        let Some(att) = arg_att(pa.aggref) else {
            pd_derive_trace("set argument not a plain outer Var");
            return None;
        };
        // The set kind was established from this very argument at init; the
        // width re-check keeps the extraction honest (set_arg_kind — the
        // GL-LOWDIST-3 datetime widening rides here, caller-gated).
        match kind {
            distinctset::DistinctKeyKind::Int16
            | distinctset::DistinctKeyKind::Int32
            | distinctset::DistinctKeyKind::Int64 => {
                if set_arg_kind(desc.attr(att as usize).atttypid).is_none() {
                    pd_derive_trace("set argument outside the caller's width vocabulary");
                    return None;
                }
            }
            distinctset::DistinctKeyKind::Bytes => {}
        }
        max_att = max_att.max(att as i32 + 1);
        sets.push(PdSetSpec { att, kind });
        set_transnos.push(ps.transno);
    }
    // Every remaining transno must be a vocabulary transition.
    let mut vocab: Vec<PdVocab> = Vec::new();
    let mut seen: Vec<bool> = vec![false; node.numtrans];
    for t in &set_transnos {
        seen[*t] = true;
    }
    for pa in node.peragg.iter() {
        let transno = pa.transno as usize;
        if seen[transno] {
            continue;
        }
        seen[transno] = true;
        let ar = pa.aggref;
        if !ar.aggdistinct.is_nil() || !ar.aggorder.is_nil() {
            pd_derive_trace("vocab aggref carries aggdistinct/aggorder");
            return None;
        }
        let att = if ar.args.is_nil() {
            None
        } else {
            let Some(a) = arg_att(ar) else {
                pd_derive_trace("vocab argument not a plain outer Var");
                return None;
            };
            max_att = max_att.max(a as i32 + 1);
            let Some(k) = int_kind(desc.attr(a as usize).atttypid) else {
                pd_derive_trace("vocab argument not an int2/int4/int8 column");
                return None;
            };
            Some((a, k))
        };
        if ar.aggfilter.is_some() {
            pd_derive_trace("vocab aggref has FILTER");
            return None;
        }
        let Some(kind) = pardistinct::vocab_kind(ar.aggfnoid, att) else {
            pd_derive_trace("vocab transfn outside the exact-integer whitelist");
            return None;
        };
        vocab.push(PdVocab { transno: transno as u32, kind });
    }
    if !seen.iter().all(|&s| s) {
        pd_derive_trace("uncovered transition (neither set nor vocab)");
        return None;
    }
    Some(std::sync::Arc::new(PdSpec {
        key_atts,
        key_kinds,
        vocab,
        sets,
        max_att,
        worker_budget: distinct_set_budget() / 2,
        // dedupsub I3: unknown here — the runtime sink overrides at engage
        // (Gather-era arms keep the projection inert).
        expected_worker_rows: 0,
    }))
}

/// Vocab aggfnoids map through the transfn whitelist; re-exported checks
/// the runtime distinct sink pairs with `pd_derive_spec`'s admission story.
/// (The GM-hybrid-only surface — the handoff registry, the export/adopt
/// snapshot, and the leader-side parallel-merge drivers — was DELETED at
/// Phase-5 D1.)
pub use pardistinct::{
    pd_batch_insert_enabled, pd_bucket_precount, pd_concat_buckets, pd_emit_bucket,
    pd_empty_grouped_table, pd_merge_bucket, pd_merge_bucket_refs, pd_paremit_recipe,
    pd_paremit_state, pd_route_value_records, pd_spill_bytes_mode,
    pd_spill_min_record_width, pd_spill_record_width, pd_table_from_spill, pd_vec_plan,
    PdBucketMerger, PdBuilder, PdEmitBucket, PdEmitRecipe, PdFeed, PdHandedTable, PdInt,
    PdKeyKind, PdMerged, PdParemitCol, PdParemitState, PdSinkLocal, PdSinkMerged, PdSpec,
    PdTopnCand, PdTopnKey, PdTopnSpec, PdVecPlan, PdVecScratch, PD_SINK_GROUP_PARTS,
};

/// PAREMIT shape probe (runtime distinct sink, emission-in-combine fast
/// path — pardistinct.rs section doc): `Some(cols)` iff every output
/// column is a pure shuffle of group keys and identity-finalized aggregate
/// results the combine workers can materialize from the merged partials —
/// the merge.rs `build_emit_plan` admission, extended to the distinct
/// sink's vocabulary. Anything else (HAVING, expressions over aggregates
/// or keys, non-count DISTINCT aggs, avg's finalfn shape, non-key Vars)
/// returns `None` and the engagement keeps the ADOPT tail (never a serial
/// refusal — adopt handles the general shapes byte-identically).
///
/// Spec-independent by design: the economics tier prices the paremit
/// shape BEFORE `pd_derive_spec` runs; [`pd_paremit_recipe`] resolves
/// these columns against the derived spec.
pub fn pd_paremit_cols(node: &AggStateData<'_>) -> Option<Vec<pardistinct::PdParemitCol>> {
    use pardistinct::PdParemitCol;
    // pg_proc count(*) / count(any) / sum(int2) / sum(int4) — the
    // identity-finalize vocabulary (avg carries a finalfn: refused).
    const AGG_COUNT_STAR: Oid = 2803;
    const AGG_COUNT_ANY: Oid = 2147;
    const AGG_SUM_INT2: Oid = 2109;
    const AGG_SUM_INT4: Oid = 2108;
    // HAVING re-checks per group and expression projections need the
    // interpreter — both keep the adopt tail (m2-sinks §6: emission moves
    // into combine only where the shape admits it).
    if node.qual.is_some() || node.skip_final {
        return None;
    }
    let group_cols = node.plan.grpColIdx;
    let mut cols = Vec::with_capacity(node.plan.plan.targetlist.len());
    for n in node.plan.plan.targetlist.iter() {
        let te = n.as_target_entry()?;
        if let Some(v) = te.expr.as_var() {
            // The projection evaluates over the outer tuple; only the
            // grouping columns are materialized in the merged result.
            if v.varno != ::execexpr::OUTER_VAR || v.varlevelsup != 0 {
                return None;
            }
            let i = group_cols.iter().position(|&c| c == v.varattno)?;
            cols.push(PdParemitCol::Key(i));
            continue;
        }
        if let Some(ar) = te.expr.as_aggref() {
            if ar.aggno < 0 || ar.aggno as usize >= node.peragg.len() {
                return None;
            }
            let pa = &node.peragg[ar.aggno as usize];
            // Identity finalize only (merge.rs discipline): no finalfn
            // (the result IS the trans value), no direct args, byval
            // transtype — count/sum-int shapes all qualify.
            if pa.finalfn.is_some()
                || !pa.direct_args.is_empty()
                || !node.trans_typ[pa.transno as usize].byval
            {
                return None;
            }
            if !pa.aggref.aggdistinct.is_nil() {
                // count(DISTINCT x) only: `set_count_transfn` proves the
                // transition is exactly int8inc_any, so the merged set's
                // value count IS the replay result (distinctset.rs
                // `value_count` doc). Other set aggs keep the adopt
                // replay.
                let si = node
                    .pertrans_sort
                    .iter()
                    .position(|ps| ps.transno == pa.transno as usize)?;
                if !node.pertrans_sort[si].set_count_transfn {
                    return None;
                }
                cols.push(PdParemitCol::SetCount(si));
                continue;
            }
            let sum = match ar.aggfnoid {
                AGG_COUNT_STAR | AGG_COUNT_ANY => false,
                AGG_SUM_INT2 | AGG_SUM_INT4 => true,
                _ => return None,
            };
            cols.push(PdParemitCol::Vocab { transno: pa.transno, sum });
            continue;
        }
        // Consts / expressions keep the projection interpreter (adopt).
        return None;
    }
    Some(cols)
}

/// Whether the runtime distinct sink's PAREMIT emit state is installed
/// (the drive routes straight to [`agg_pdemit_emit_next`]; the plan's
/// Sort was bypassed and must never be fed).
pub fn agg_pdemit_emitting(node: &AggStateData<'_>) -> bool {
    node.pdemit.is_some()
}

/// Install the adopted paremit emit state (runtime distinct sink,
/// Completed leader path). The caller returns rows via
/// [`agg_pdemit_emit_next`] from here on.
pub fn agg_pdemit_install(node: &mut AggStateData<'_>, st: pardistinct::PdParemitState) {
    debug_assert!(node.pdemit.is_none());
    debug_assert!(node.hashgroup.is_none());
    node.pdemit = Some(Box::new(st));
}

/// Emit the next merged paremit row into the node's result slot — the
/// `agg_retrieve_emitted` discipline: a datum memcpy per row, no
/// finalize, no projection interpreter, no per-row expr-context reset
/// (nothing on this path allocates per tuple; text datums point into the
/// published buckets' arenas, which outlive every pull). `Ok(None)` =
/// stream end (`agg_done` set, state dropped). No HAVING on admitted
/// shapes — every pull is one group row.
pub fn agg_pdemit_emit_next<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    let result = node.ps_ResultTupleSlot;
    let st = node.pdemit.as_deref_mut().expect("pdemit emit without state");
    let Some((bucket, row)) = pardistinct::pd_paremit_next(st)? else {
        // Stream end: clear the slot's borrowed arena datums BEFORE the
        // buckets drop (the hashgrouped end arm's discipline).
        let slot = estate.slot_mut(result);
        exectuples::exec_clear_tuple(slot, mcx);
        node.agg_done = true;
        agg_pdemit_reset(node);
        return Ok(None);
    };
    let natts = st.natts;
    let (values, nulls) = st.row(bucket, row);
    let slot = estate.slot_mut(result);
    exectuples::exec_clear_tuple(slot, mcx);
    {
        let sb = slot.base_mut();
        sb.tts_values[..natts].copy_from_slice(values);
        sb.tts_isnull[..natts].copy_from_slice(nulls);
    }
    exectuples::exec_store_virtual_tuple(slot);
    Ok(Some(result))
}

/// Rescan/teardown: drop the paremit emit state. Engagement-sized results
/// ride the same allocator-release discipline as the sink and hashgroup
/// teardowns (69b97573f): the buckets were built by helper threads that
/// have exited — purge the freed-but-retained segments so a repeat
/// execution rebuilds inside the same RSS envelope.
pub fn agg_pdemit_reset(node: &mut AggStateData<'_>) {
    if let Some(st) = node.pdemit.take() {
        let bytes = st.mem_bytes();
        drop(st);
        if bytes >= SINK_RELEASE_MIN_BYTES {
            hashagg_release_retained("pdemit_teardown");
        }
    }
}

// (agg_plain_adopt_empty / agg_plain_adopt_merged — the GM-hybrid plain
// leader drive's adoption tail — were DELETED at Phase-5 D1 with the
// lane-v2 pardistinct drives. The runtime plain-distinct sink installs its
// merged sets through plainpd::agg_plain_install_merged_set instead.)

/// Metadata-answerable transitions (lane-v2 metaagg arm); None = not
/// answerable (the fold/per-row drives own the node).
pub fn agg_meta_plan<'a>(node: &'a AggStateData<'_>) -> Option<&'a [::lanefold::MetaTrans]> {
    node.meta_aggs.as_deref()
}

/// Metadata-answered plain agg: per-transition end states written from AM
/// metadata (footer row counts + zone maps + footer sums), finalized through
/// the normal plain path — the STANDARD for metadata-answered aggregates:
/// end states only, the real finalfns do the finalize (parity by
/// construction; notes/q4-avg-quarantine-resolution.md proved the Sum128 →
/// avg finalize exact). `minmax` maps scan column -> (min, max) over visible
/// rows, exact by the zone-map contract; `sums` maps scan column -> exact
/// i128 sum over visible rows; `rows` = visible row count. rows == 0 leaves
/// every transition at its init state (count 0, sum/min/max NULL) — the
/// empty-input scan result.
pub fn exec_agg_meta<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    rows: u64,
    minmax: &[(u16, i64, i64)],
    sums: &[(u16, i128)],
) -> PgResult<Option<ExecSlotId>> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_PLAIN);
    if node.agg_done {
        return Ok(None);
    }
    initialize_aggregates(node, estate)?;
    if rows > 0 {
        let metas = node.meta_aggs.as_deref().expect("meta arm requires a meta plan");
        for t in metas {
            // Affine derivation over the exact footer sum: mod-2^64 equal to
            // the per-row wrapped fold (SumBase legality argument); i128
            // cannot overflow — |S| < 2^96 by the RG_FLAG_SUMS bound and
            // |coeff| <= 2^31.
            let sum = |col: u16| -> i128 {
                let s = sums
                    .iter()
                    .find(|e| e.0 == col)
                    .expect("meta arm supplies every sum column")
                    .1;
                t.mulk as i128 * s + t.addend as i128 * rows as i128
            };
            // SAFETY: transno indexes the node's once-allocated pergroup
            // array (classify_meta transnos come from its spec list).
            let pg = unsafe { &mut *node.pergroup_base.as_ptr().add(t.transno as usize) };
            let v = match t.kind {
                ::lanefold::MetaKind::Count => rows as i64,
                ::lanefold::MetaKind::Min | ::lanefold::MetaKind::Max => {
                    let &(_, mn, mx) = minmax
                        .iter()
                        .find(|e| e.0 == t.col)
                        .expect("meta arm supplies every min/max column");
                    if t.kind == ::lanefold::MetaKind::Min { mn } else { mx }
                }
                // i128 narrows wrapping — the lane fold's i64 wrapping-add
                // contract (C -fwrapv parity for int2/int4_sum).
                ::lanefold::MetaKind::Sum => sum(t.col) as i64,
                ::lanefold::MetaKind::AvgAccum => {
                    // End state of N int2/int4_avg_accum calls over the
                    // aggcontext initval copy ('{0,0}' int8[2]).
                    assert!(!pg.trans_value_is_null, "avg transarray is never NULL");
                    let arr = pg.trans_value.as_usize() as *mut u8;
                    // SAFETY: aggcontext-lived initval copy, shape validated.
                    unsafe {
                        assert!(
                            ::types_tuple::varatt::varatt_is_4b_u(arr)
                                && ::types_tuple::varatt::varsize_4b(arr)
                                    == ::lanefold::INT8_TRANSARRAY_SIZE
                                && arr.add(8).cast::<i32>().read() == 0,
                            "expected 2-element int8 array"
                        );
                        let td =
                            arr.add(::lanefold::ARR_OVERHEAD_NONULLS_1).cast::<i64>();
                        *td = rows as i64;
                        *td.add(1) = sum(t.col) as i64;
                    }
                    continue;
                }
                ::lanefold::MetaKind::Sum128 => {
                    // End state of N int8_avg_accum calls: a fresh
                    // aggcontext Int128AggState (the transfn's own
                    // first-call allocation shape).
                    use ::adt_numeric::aggregates::Int128AggState;
                    // SAFETY: agg_node is live for the node's lifetime.
                    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
                    let layout = core::alloc::Layout::new::<Int128AggState>();
                    let raw = ::mcx::Allocator::allocate(&aggctx, layout)
                        .map_err(|_| aggctx.oom(layout.size()))?;
                    let p = raw.cast::<Int128AggState>().as_ptr();
                    // SAFETY: fresh allocation of the exact layout.
                    unsafe {
                        p.write(Int128AggState {
                            calc_sum_x2: false,
                            n: rows as i64,
                            sum_x: sum(t.col),
                            sum_x2: 0,
                        });
                    }
                    pg.trans_value = Datum::from_usize(p as usize);
                    pg.trans_value_is_null = false;
                    pg.no_trans_value = false;
                    continue;
                }
            };
            pg.trans_value = Datum::from_i64(v);
            pg.trans_value_is_null = false;
            pg.no_trans_value = false;
        }
    }
    plain_finish(node, estate)
}

// ===========================================================================
// Lane-v2 sorted-agg (AGG_SORTED) streaming-operator delegation seam. The
// lane hosts AGG_SORTED as a mid-pipeline operator (execmain/src/lanev2.rs
// `SortedAggOp`): rows are PUSHED at it in the child's sorted order and it
// emits one finalized group row per boundary — the control-flow inverse of
// `agg_retrieve_sorted`'s pull loop. These thin entry points delegate every
// substantive step to the SAME row-path machinery — the persort state
// (first/pending slots + `have_pending`), the ported grouping-equality
// ExprState (`ps.eq`), `initialize_aggregates`, the per-row transition
// program, and the finalize/HAVING/project tail — split at
// `agg_retrieve_sorted`'s own seams, so the same rows run the same
// comparisons, transitions and finalizations in the same order
// (byte-identical), and the node state is call-boundary-compatible with the
// pull loop in BOTH directions (each returns with the current group closed
// and at most a pending boundary tuple saved), making a per-call fallback to
// `exec_agg` byte-safe.
// ===========================================================================

/// Agg-side admission for the lane-v2 sorted-agg streaming operator:
/// AGG_SORTED single grouping set, no merge phase, no
/// DISTINCT/ORDER-BY-within-aggregate internal sorts (`pertrans_sort`) —
/// UNLESS every such entry is a lane-hosted exact-DISTINCT set (distinctset
/// module; `agg_sorted_accept`/`agg_sorted_group_begin` drive the collect and
/// the emit tail's `process_ordered_aggregates` replays the sets) — plus
/// subplan-free transitions, and initplan-param-free everywhere (the
/// lane drive, like `exec_agg_batched`, does not hoist pending initplans).
/// Subplan-bearing HAVING/projection ARE admitted: the emit tail delegates to
/// the same subplan-aware qual/project arms `agg_retrieve_sorted` uses.
pub fn agg_sorted_lane_admissible(node: &AggStateData<'_>) -> bool {
    node.plan.aggstrategy == AGG_SORTED
        && node.gsets.is_none()
        && node.merge.is_none()
        && (node.pertrans_sort.is_empty() || agg_pertrans_all_distinct_set(node))
        && node.evaltrans.as_deref().is_some_and(|et| !et.has_subplan())
        && node
            .evaltrans
            .as_deref()
            .is_none_or(|et| et.param_exec_deps().is_empty())
        && node.proj.param_exec_deps().is_empty()
        && node.qual.as_deref().is_none_or(|q| q.param_exec_deps().is_empty())
}

/// Grouped narrow-sort admission (lanev2 `try_own_sorted_agg_over_sort`'s
/// order-relaxation arm — the sorted grouped exact-DISTINCT shape): AGG_SORTED whose
/// every internal-sort entry is a set-CAPABLE exact-DISTINCT (presorted
/// entries included — the drive arms `force_distinct_set`, replacing the
/// plan's sort-suffix adjacent-dedup with the exact set) and whose EVERY
/// transition function is order-insensitive-exact. Under those two facts the
/// plan Sort's distinct-arg suffix keys have NO observable effect beyond
/// intra-group row order — which nothing in the node observes — so the lane
/// may sort by the group-key prefix alone (the sort-side structural check is
/// the drive's: prefix == group columns) and still produce byte-identical
/// output: same groups in the same order, same exact aggregate values.
pub fn agg_sorted_distinct_narrow_admissible(node: &AggStateData<'_>) -> bool {
    node.plan.aggstrategy == AGG_SORTED
        && node.gsets.is_none()
        && node.merge.is_none()
        && !node.pertrans_sort.is_empty()
        && node.pertrans_sort.iter().all(|ps| ps.set_kind.is_some())
        && node.trans_order_insensitive
        && node.group_eq_representational
        && node.evaltrans.as_deref().is_some_and(|et| !et.has_subplan())
        && node
            .evaltrans
            .as_deref()
            .is_none_or(|et| et.param_exec_deps().is_empty())
        && node.proj.param_exec_deps().is_empty()
        && node.qual.as_deref().is_none_or(|q| q.param_exec_deps().is_empty())
}

/// Arm set-mode for the grouped narrow-sort drive (see `force_distinct_set`
/// field doc — sticky, value-safe on later per-tuple fallbacks: the set over
/// ANY input order yields the same distinct multiset, and the admitted
/// transitions are replay-order-insensitive).
pub fn agg_sorted_force_distinct_set(node: &mut AggStateData<'_>) {
    debug_assert!(agg_sorted_distinct_narrow_admissible(node));
    node.force_distinct_set = true;
}

/// The plan's grouping columns (1-based attnos into the outer slot) — the
/// narrow-sort drive's prefix check.
pub fn agg_plan_group_cols<'a>(node: &AggStateData<'a>) -> &'a [i16] {
    node.plan.grpColIdx
}

/// Whether a drive already armed `force_distinct_set` on this node (the
/// narrow-sort drive re-narrows a rescan-rebuilt sort iff so — the plain
/// admission is force-satisfied then and would otherwise skip the probe).
pub fn agg_distinct_set_forced(node: &AggStateData<'_>) -> bool {
    node.force_distinct_set
}

/// A boundary tuple is saved and the next group has not started — the lane
/// operator's cross-call resume flag (C's own `have_pending` state).
pub fn agg_sorted_have_pending(node: &AggStateData<'_>) -> bool {
    node.persort.as_ref().is_some_and(|ps| ps.have_pending)
}

/// Start a new group — `agg_retrieve_sorted`'s per-group prologue verbatim:
/// reset the per-output context + aggcontext, install the group's first tuple
/// (`Some(id)` = copy the pushed row; `None` = swap in the saved pending
/// boundary tuple), `initialize_aggregates`, and run the transition program
/// on the first tuple.
pub fn agg_sorted_group_begin<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    first: Option<ExecSlotId>,
) -> PgResult<()> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_SORTED);
    let mcx = estate.es_query_cxt;
    estate.reset_expr_context(node.ps_ExprContext);
    // SAFETY: sole access path to the node during the reset (the frames'
    // copies are raw and dormant between evaluations). SKIPPED while the
    // hash-grouped arm's degrade residue exists: other groups' by-ref
    // transvalues live in aggcontext until the residue drains
    // (hashgrouped.rs module doc; memory stays bounded by the residue).
    if !hashgrouped::agg_hashgroup_state_active(node) {
        unsafe { node.agg_node.as_mut() }.reset();
    }
    {
        let AggStateData { persort, .. } = node;
        let ps = persort.as_mut().expect("sorted Agg has persort");
        match first {
            None => {
                debug_assert!(ps.have_pending);
                core::mem::swap(&mut ps.first_slot, &mut ps.pending_slot);
                ps.have_pending = false;
            }
            Some(outer_id) => {
                let outer_slot = estate.slot_mut(outer_id);
                exectuples::exec_copy_slot(&mut ps.first_slot, outer_slot, mcx, mcx)?;
            }
        }
    }
    initialize_aggregates(node, estate)?;
    {
        let AggStateData { persort, evaltrans, .. } = node;
        let ps = persort.as_mut().expect("sorted Agg has persort");
        let et = evaltrans.as_mut().unwrap();
        let mut slots =
            EvalSlots { scan: None, inner: None, outer: Some(&mut ps.first_slot) };
        exec_eval_expr(et, &mut slots)?;
    }
    // Ordered-input collection (the pull loop's interleave) — a no-op unless
    // the admission's exact-DISTINCT set entries parked this row.
    if !node.pertrans_sort.is_empty() {
        collect_ordered_input(node, estate, 1)?;
    }
    estate.reset_expr_context(node.tmpcontext);
    Ok(())
}

/// The group-boundary comparison — the ported grouping-equality ExprState
/// (`ps.eq`, C's ExecBuildGroupingEqual product; NULL grouping keys compare
/// as same-group through it) over {inner: group first tuple, outer: pushed
/// row}, exactly as `agg_retrieve_sorted`'s loop. `eq` None (numCols == 0,
/// planner-proved-redundant keys) = never a boundary, as C's numCols > 0
/// guard.
pub fn agg_sorted_same_group<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
) -> PgResult<bool> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_SORTED);
    let AggStateData { persort, .. } = node;
    let ps = persort.as_mut().expect("sorted Agg has persort");
    let outer_slot = estate.slot_mut(outer_id);
    let mut slots = EvalSlots {
        scan: None,
        inner: Some(&mut ps.first_slot),
        outer: Some(&mut *outer_slot),
    };
    match ps.eq.as_mut() {
        Some(eq) => exec_qual(Some(eq), &mut slots),
        None => Ok(true),
    }
}

/// One same-group row through the FULL per-row transition program —
/// `agg_retrieve_sorted`'s loop body verbatim, ordered-input collection
/// included (no subplan arms: the lane admission refuses those shapes; the
/// only admitted `pertrans_sort` entries are exact-DISTINCT sets, which
/// collect here exactly as the pull loop interleaves).
pub fn agg_sorted_accept<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
) -> PgResult<()> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_SORTED);
    {
        let outer_slot = estate.slot_mut(outer_id);
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
        exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)?;
    }
    if !node.pertrans_sort.is_empty() {
        collect_ordered_input(node, estate, 1)?;
    }
    estate.reset_expr_context(node.tmpcontext);
    Ok(())
}

/// Save the boundary row as the next group's first tuple (query-context copy
/// into the pending slot — `agg_retrieve_sorted`'s boundary arm verbatim).
pub fn agg_sorted_save_pending<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
) -> PgResult<()> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_SORTED);
    let mcx = estate.es_query_cxt;
    let AggStateData { persort, .. } = node;
    let ps = persort.as_mut().expect("sorted Agg has persort");
    let outer_slot = estate.slot_mut(outer_id);
    exectuples::exec_copy_slot(&mut ps.pending_slot, outer_slot, mcx, mcx)?;
    ps.have_pending = true;
    Ok(())
}

/// Finalize + HAVING + project the completed group —
/// `agg_retrieve_sorted`'s post-loop tail verbatim (subplan-aware qual/proj
/// arms included; the group's representative tuple rides in
/// `persort.first_slot`). `None` = the HAVING qual rejected the group (the
/// caller starts the next group / ends the stream, exactly as the pull
/// loop's `continue`).
pub fn agg_sorted_emit<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_SORTED);
    let mcx = estate.es_query_cxt;
    process_ordered_aggregates(node, estate)?;
    finalize_aggregates(node, estate, node.pergroup_base)?;

    if node.proj.has_subplan() || node.qual.as_deref().is_some_and(|q| q.has_subplan()) {
        let ecxt = node.ps_ExprContext;
        let result = node.ps_ResultTupleSlot;
        let instr_idx = node.instr_idx;
        let AggStateData { persort, qual, proj, .. } = node;
        let ps = persort.as_mut().expect("sorted Agg has persort");
        if !::executils::exec_qual_with_subplans_outer(
            qual.as_deref_mut(),
            &mut ps.first_slot,
            estate,
            ecxt,
        )? {
            estate.instr_count_filtered1(instr_idx);
            return Ok(None);
        }
        ::executils::exec_project_with_subplans_outer(
            proj,
            &mut ps.first_slot,
            estate,
            ecxt,
            result,
        )?;
        return Ok(Some(result));
    }
    {
        let AggStateData { persort, qual, .. } = node;
        let ps = persort.as_mut().expect("sorted Agg has persort");
        let mut slots =
            EvalSlots { scan: None, inner: None, outer: Some(&mut ps.first_slot) };
        if !exec_qual(qual.as_deref_mut(), &mut slots)? {
            estate.instr_count_filtered1(node.instr_idx);
            return Ok(None);
        }
    }
    let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
    let ps = node.persort.as_mut().unwrap();
    let mut slots = EvalSlots { scan: None, inner: None, outer: Some(&mut ps.first_slot) };
    exec_project(&mut node.proj, &mut slots, result_slot, mcx)?;
    Ok(Some(node.ps_ResultTupleSlot))
}

/// Input exhausted: `agg_retrieve_sorted`'s end-of-stream arm (sets
/// `agg_done` BEFORE the last group finalizes, exactly as the pull loop's
/// `fetch None` arms do).
pub fn agg_sorted_input_done(node: &mut AggStateData<'_>) {
    debug_assert_eq!(node.plan.aggstrategy, AGG_SORTED);
    node.agg_done = true;
}

/// Sorted-FOLD admission (lanev2 `try_own_sorted_agg_over_seq_scan`'s
/// vectorized arm): the plain sorted admission PLUS a classified lanefold
/// plan, no internal sorts at all (the fold cannot interleave ordered-input
/// collection), real grouping keys, and representational grouping equality —
/// the grant under which the lane's raw-datum boundary compare over the
/// staged key lanes returns exactly the ported grouping-equality program's
/// verdict (NULL keys grouping together handled by the lane's null-pair
/// compare).
pub fn agg_sorted_fold_admissible(node: &AggStateData<'_>) -> bool {
    agg_sorted_lane_admissible(node)
        && node.pertrans_sort.is_empty()
        && node.lanefold.is_some()
        && node.plan.numCols > 0
        && node.group_eq_representational
}

/// The current group's pergroup array — the sorted fold target: the same
/// once-allocated base `initialize_aggregates` re-initializes at every
/// `agg_sorted_group_begin`.
pub fn agg_sorted_pergroup_base(node: &AggStateData<'_>) -> NonNull<AggPerGroup> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_SORTED);
    node.pergroup_base
}

/// One same-group row through only the RESIDUAL transitions (the transnos
/// classify refused) — the fold-feed discipline: the admitted transitions
/// fold per group run over `agg_sorted_pergroup_base`. No-op when the plan
/// admitted every transition. Mirrors `agg_plain_build_accept_resid`.
pub fn agg_sorted_accept_resid<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
) -> PgResult<()> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_SORTED);
    let Some(resid) = node.lanefold.as_mut().and_then(|lf| lf.resid.as_mut()) else {
        return Ok(());
    };
    estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
    let outer_slot = estate.slot_mut(outer_id);
    let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
    exec_eval_expr(resid, &mut slots)?;
    estate.reset_expr_context(node.tmpcontext);
    Ok(())
}

/// The OPEN group's grouping-key datums, read from the group's first tuple
/// (`persort.first_slot` — installed at `agg_sorted_group_begin`, live until
/// the group emits). `out` receives one `(value, isnull)` per grouping
/// column in `grpColIdx` order. The sorted-fold arm re-derives its boundary
/// comparand through this after every group begin (fresh, from-pending, and
/// cross-call resumes alike) — by-value key columns only per that arm's
/// admission, so the returned Datums are self-contained.
pub fn agg_sorted_group_key(node: &mut AggStateData<'_>, out: &mut [(Datum, bool)]) {
    debug_assert_eq!(node.plan.aggstrategy, AGG_SORTED);
    let cols = node.plan.grpColIdx;
    debug_assert_eq!(out.len(), cols.len());
    let ps = node.persort.as_mut().expect("sorted Agg has persort");
    for (o, &attno) in out.iter_mut().zip(cols.iter()) {
        let mut isnull = false;
        let d = exectuples::slot_getattr(&mut ps.first_slot, attno as i32, &mut isnull);
        *o = (d, isnull);
    }
}

// ===========================================================================
// Lane-v2 K2 staged group probe (design §3a): the fold feed's batched
// hash+probe pre-pass. The lane stages the (single) grouping key per batch,
// hashes the whole lane in one tight kernel loop (`agg_hash_hash_staged`),
// then probes each row IN ROW ORDER through the same C-ported tuplehash
// lookup the per-row path uses (`agg_hash_probe_staged`) — same hash values
// (bit-identical by the kernel contract), same insertion order, same entry
// initialization, same spill decisions → same table layout, same iteration
// order, same output bytes. Only the per-row expr-program walk + slot
// prepare + per-row context churn are replaced.
// ===========================================================================

/// K2 admission: a single grouping-key column whose tuplehash probe kernel is
/// batch-hostable (Int4/Int8/Text — `staged_probe_supported`), and NO extra
/// stored columns beyond the key (`hash_grp_col_idx_input` len 1). Extra
/// columns exist exactly when a functionally-dependent output column rides
/// the hash entry (the planner reduced GROUP BY to the PK,
/// remove_useless_groupby_columns): the group's representative row must then
/// carry the dependent values, which the key-only staged probe cannot
/// present at insert (fdgroup-wr, compat-matrix B4) — those shapes keep the
/// per-row arrival probe (`prepare_hash_slot` presents the full image).
/// Returns the key's 0-based column number in the agg's OUTER (input)
/// tuple. `None` = keep the per-row arrival probe.
pub fn agg_hash_staged_probe_col(node: &AggStateData<'_>) -> Option<u16> {
    let ph = node.perhash.as_ref()?;
    if ph.num_cols == 1
        && ph.hash_grp_col_idx_input.len() == 1
        && ph.hashtable.staged_probe_supported()
    {
        Some((ph.hash_grp_col_idx_input[0] - 1) as u16)
    } else {
        None
    }
}

/// Whether the single staged grouping key probes through the TEXT kernel
/// (deterministic collation proved at kernel selection) — the M2 sink's
/// single-text admission input (raw key bytes are canonical across
/// workers for exactly this kernel). Meaningful only alongside a `Some`
/// [`agg_hash_staged_probe_col`].
pub fn agg_hash_staged_probe_is_text(node: &AggStateData<'_>) -> bool {
    node.perhash
        .as_ref()
        .is_some_and(|ph| ph.num_cols == 1 && ph.hashtable.staged_probe_is_text())
}

/// Multi-key admission input (multikey spike §2.4): the grouping key
/// columns' (0-based INPUT colno, packing classification) pairs, in key
/// order. Empty = not a hashed agg.
pub fn agg_hash_key_cols(
    node: &AggStateData<'_>,
) -> Vec<(u16, ::execgrouping::GroupKeyKind)> {
    match node.perhash.as_ref() {
        Some(ph) => ph
            .hashtable
            .key_cols()
            .iter()
            .enumerate()
            .map(|(j, kc)| ((ph.hash_grp_col_idx_input[j] - 1) as u16, kc.kind))
            .collect(),
        None => Vec::new(),
    }
}

/// Whether the lanefold plan carries residual (classify-refused) transitions.
/// The K2 deferred probe hosts only fully-admitted plans: residuals need the
/// live input row at probe time, which a deferred flush no longer has.
pub fn agg_lanefold_has_resid(node: &AggStateData<'_>) -> bool {
    node.lanefold.as_ref().is_some_and(|lf| lf.resid.is_some())
}

/// K2 batched hashing over the staged key lane — delegates to the tuplehash
/// probe kernel, bit-identical per element to the per-row `hash_slot`.
///
/// Contract (like `hash_staged`): non-null staged datums are live values of
/// the grouping key's type for the whole call.
pub fn agg_hash_hash_staged(
    node: &AggStateData<'_>,
    keys: &[Datum],
    isnull: &[bool],
    out: &mut Vec<u32>,
) -> PgResult<()> {
    let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
    ph.hashtable.hash_staged(keys, isnull, out)
}

/// K2 staged probe: `lookup_hash_entry` with the grouping key presented
/// directly (the caller staged it — self-contained for the batch) and the
/// hash precomputed by [`agg_hash_hash_staged`]. Same C-ported lookup, same
/// first-arrival insertion, same `initialize_hash_entry`, same spill-mode
/// gate as the per-row path. `None` = spill-mode miss: no transition runs
/// (exactly as per-row) and the caller replays + spills the row via
/// [`agg_hash_spill_staged`].
pub fn agg_hash_probe_staged<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    key: Datum,
    isnull: bool,
    hash: u32,
) -> PgResult<Option<NonNull<AggPerGroup>>> {
    debug_assert_eq!(node.plan.aggstrategy, AGG_HASHED);
    let mcx = estate.es_query_cxt;
    let AggStateData { perhash, trans_init, trans_typ, agg_node, .. } = node;
    let ph = perhash.as_mut().expect("hashed Agg has perhash");
    debug_assert_eq!(ph.num_cols, 1);
    // The miss leg below presents ONLY the key in the hashslot; a stored
    // extra column (a functionally-dependent tlist Var — hash entries carry
    // it as the group's representative value) would be inserted NULL, a
    // silent wrong result. `agg_hash_staged_probe_col` refuses those shapes.
    debug_assert_eq!(
        ph.hash_grp_col_idx_input.len(),
        1,
        "staged probe requires key-only hash rows (extra dependent columns must refuse K2)"
    );
    // Fast path — the overwhelmingly common found-existing-group case: the
    // slot-free kernel find (no hashslot presentation, no slot deform).
    let ix = match ph.hashtable.find_staged(key, isnull, hash)? {
        Some(ix) => ix,
        None => {
            // Miss: present the key in the hashslot (prepare_hash_slot's
            // tail with the key already in hand) and run the full C-ported
            // lookup — the insert leg (entry copy + robin-hood placement) or
            // the spill-mode miss, exactly as the per-row path.
            exectuples::exec_clear_tuple(&mut ph.hashslot, mcx);
            {
                let base = ph.hashslot.base_mut();
                base.tts_values[0] = key;
                base.tts_isnull[0] = isnull;
            }
            exectuples::exec_store_virtual_tuple(&mut ph.hashslot);
            #[cfg(debug_assertions)]
            {
                // The staged hash must equal what the per-row path computes.
                let h = ph.hashtable.hash_slot(&mut ph.hashslot)?;
                debug_assert_eq!(h, hash);
            }
            let table_mcx = ph.table_ctx.mcx();
            let use_table = !ph.spill.mode;
            let (ix, isnew) = ph.hashtable.lookup(
                &mut ph.hashslot,
                hash,
                use_table.then_some(table_mcx),
                mcx,
            )?;
            let Some(ix) = ix else {
                return Ok(None);
            };
            debug_assert!(isnew, "find_staged missed an existing entry");
            if isnew {
                initialize_hash_entry(ph, trans_init, trans_typ, *agg_node, ix, mcx)?;
            }
            ix
        }
    };
    // SE-GROUPONLY: zero-transition builds carry no additional space —
    // the dangling sentinel is never dereferenced (the empty fold plan
    // folds nothing; the dict-code per-epoch caches only store and forward
    // these pointers to the same fold).
    Ok(Some(
        ph.hashtable
            .entry_additional(ix)
            .map_or(NonNull::dangling(), |p| p.cast::<AggPerGroup>()),
    ))
}

/// K2 spill leg for a staged spill-mode miss: `hashagg_spill_tuple` over the
/// caller's replayed row (needed columns populated, unneeded NULL — the spill
/// projection's own treatment), byte-identical to spilling the original
/// input row.
pub fn agg_hash_spill_staged<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot: ExecSlotId,
    hash: u32,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    let s = estate.slot_mut(slot);
    hashagg_spill_tuple(&mut ph.spill, Some(s), hash, mcx)
}

const MAX_FINAL_ARGS: usize = 8;

// finalize_aggregate(s) (nodeAgg.c): finalfn results land in ps_ExprContext's
// per-tuple memory via the armed result mcx (C's MemoryContextContains +
// datumCopy discipline); no finalfn = the byval transvalue itself.
pub(crate) fn finalize_aggregates<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &EStateData<'mcx>,
    pergroup: NonNull<AggPerGroup>,
) -> PgResult<()> {
    let per_tuple = estate.ecxt(node.ps_ExprContext).per_tuple_mcx();
    let skip_final = node.skip_final;
    let AggStateData {
        peragg, trans_typ, agg_node, agg_values_base, agg_nulls_base, persort, gsets, ..
    } = node;
    for (aggno, pa) in peragg.iter_mut().enumerate() {
        // SAFETY: transno < the once-allocated pergroup array length; base
        // pointers are the sole access paths (struct invariants).
        let pg = unsafe { &*pergroup.as_ptr().add(pa.transno as usize) };
        // C MakeExpandedObjectReadOnly on the transvalue (both arms).
        // SAFETY: a non-null by-ref transvalue points at a live image.
        let trans_value = if !pg.trans_value_is_null && trans_typ[pa.transno as usize].len == -1
        {
            unsafe {
                datum::expandeddatum::make_expanded_object_read_only_internal(pg.trans_value)
            }
        } else {
            pg.trans_value
        };
        // finalize_partialaggregate (nodeAgg.c): serialfn or raw transvalue.
        if skip_final {
            let (value, isnull) = match pa.serialfn.as_mut() {
                None => (trans_value, pg.trans_value_is_null),
                Some(flinfo) => {
                    if flinfo.fn_strict && pg.trans_value_is_null {
                        (Datum::null(), true)
                    } else {
                        let mut fcinfo = LocalFcinfo::<MAX_FINAL_ARGS>::fresh(0);
                        fcinfo.nargs = 1;
                        fcinfo.context = Some(agg_node.cast());
                        // SAFETY: the per-tuple context outlives this stack
                        // frame's single call.
                        unsafe { fcinfo.set_result_mcx(per_tuple) };
                        fcinfo.args[0] = NullableDatum {
                            value: trans_value,
                            isnull: pg.trans_value_is_null,
                        };
                        let result = flinfo.invoke(&mut fcinfo)?;
                        let isnull = fcinfo.isnull;
                        // SAFETY: a non-null varlena result points at a live
                        // image (C MakeExpandedObjectReadOnly on the result).
                        let value = if !isnull && pa.resulttype_len == -1 {
                            unsafe {
                                datum::expandeddatum::make_expanded_object_read_only_internal(
                                    result,
                                )
                            }
                        } else {
                            result
                        };
                        (value, isnull)
                    }
                }
            };
            // SAFETY: aggno < the once-allocated result array lengths.
            unsafe {
                agg_values_base.as_ptr().add(aggno).write(value);
                agg_nulls_base.as_ptr().add(aggno).write(isnull);
            }
            continue;
        }
        let mut direct: [NullableDatum; MAX_FINAL_ARGS] =
            [NullableDatum::null(); MAX_FINAL_ARGS];
        let mut anynull = false;
        assert!(
            pa.direct_args.len() < MAX_FINAL_ARGS,
            "finalize_aggregate (nodeAgg.c): {} direct args not supported",
            pa.direct_args.len()
        );
        for (i, es) in pa.direct_args.iter_mut().enumerate() {
            // The current group's representative tuple: AGG_SORTED holds it
            // in persort; grouping sets hold it in the gsets projection slot.
            let outer = match persort.as_mut() {
                Some(ps) => Some(&mut ps.first_slot),
                None => gsets.as_mut().map(|gs| &mut gs.first_slot),
            };
            let mut slots = EvalSlots { scan: None, inner: None, outer };
            let nd = exec_eval_expr(es, &mut slots)?;
            direct[i] = nd;
            anynull |= nd.isnull;
        }
        let (value, isnull) = match pa.finalfn.as_mut() {
            None => (trans_value, pg.trans_value_is_null),
            Some(flinfo) => {
                assert!(
                    (pa.num_final_args as usize) <= MAX_FINAL_ARGS,
                    "finalize_aggregate (nodeAgg.c): {} finalfn args not supported",
                    pa.num_final_args
                );
                let mut fcinfo = LocalFcinfo::<MAX_FINAL_ARGS>::fresh(pa.agg_collation);
                fcinfo.nargs = pa.num_final_args as i16;
                fcinfo.context = Some(agg_node.cast());
                // SAFETY: the per-tuple context outlives this stack frame's
                // single call.
                unsafe { fcinfo.set_result_mcx(per_tuple) };
                fcinfo.args[0] =
                    NullableDatum { value: trans_value, isnull: pg.trans_value_is_null };
                for i in 0..pa.direct_args.len() {
                    fcinfo.args[i + 1] = direct[i];
                }
                anynull |= pg.trans_value_is_null
                    || pa.num_final_args as usize > pa.direct_args.len() + 1;
                // SAFETY: query-lifetime node; no &mut lives across the call.
                let agg = unsafe { agg_node.as_ref() };
                agg.set_current_agg(NonNull::from(pa.aggref).cast(), pa.trans_shared);
                let out = if flinfo.fn_strict && anynull {
                    (Datum::null(), true)
                } else {
                    let result = flinfo.invoke(&mut fcinfo)?;
                    let isnull = fcinfo.isnull;
                    // C MakeExpandedObjectReadOnly on the result.
                    // SAFETY: a non-null varlena result points at a live image.
                    let value = if !isnull && pa.resulttype_len == -1 {
                        unsafe {
                            datum::expandeddatum::make_expanded_object_read_only_internal(result)
                        }
                    } else {
                        result
                    };
                    (value, isnull)
                };
                agg.clear_current_agg();
                out
            }
        };
        // SAFETY: aggno < the once-allocated result array lengths.
        unsafe {
            agg_values_base.as_ptr().add(aggno).write(value);
            agg_nulls_base.as_ptr().add(aggno).write(isnull);
        }
    }
    Ok(())
}

// agg_retrieve_direct (nodeAgg.c), AGG_SORTED single-set arm: one group per
// pass; the boundary tuple is copied into the pending slot and swapped in as
// the next group's first tuple. Group copies live in the query context
// (C pfrees each; bump arenas reclaim at query end).
fn agg_retrieve_sorted<'mcx, F>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    fetch_outer: &mut F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    let mcx = estate.es_query_cxt;
    while !node.agg_done {
        estate.reset_expr_context(node.ps_ExprContext);
        // SAFETY: sole access path to the node during the reset (the frames'
        // copies are raw and dormant between evaluations). SKIPPED while the
        // hash-grouped arm's degrade residue exists (see
        // agg_sorted_group_begin — the same guard, fallback side).
        if !hashgrouped::agg_hashgroup_state_active(node) {
            unsafe { node.agg_node.as_mut() }.reset();
        }

        {
            let AggStateData { persort, .. } = node;
            let ps = persort.as_mut().expect("sorted Agg has persort");
            if ps.have_pending {
                core::mem::swap(&mut ps.first_slot, &mut ps.pending_slot);
                ps.have_pending = false;
            } else {
                match fetch_outer(estate)? {
                    Some(outer_id) => {
                        let outer_slot = estate.slot_mut(outer_id);
                        exectuples::exec_copy_slot(&mut ps.first_slot, outer_slot, mcx, mcx)?;
                    }
                    None => {
                        node.agg_done = true;
                        return Ok(None);
                    }
                }
            }
        }
        initialize_aggregates(node, estate)?;
        {
            let tmpcontext = node.tmpcontext;
            let AggStateData { persort, evaltrans, .. } = node;
            let ps = persort.as_mut().expect("sorted Agg has persort");
            let et = evaltrans.as_mut().unwrap();
            if et.has_subplan() {
                ::executils::exec_eval_expr_with_subplans_outer(
                    et,
                    &mut ps.first_slot,
                    estate,
                    tmpcontext,
                )?;
            } else {
                let mut slots =
                    EvalSlots { scan: None, inner: None, outer: Some(&mut ps.first_slot) };
                exec_eval_expr(et, &mut slots)?;
            }
        }
        if !node.pertrans_sort.is_empty() {
            collect_ordered_input(node, estate, 1)?;
        }
        estate.reset_expr_context(node.tmpcontext);
        loop {
            let Some(outer_id) = fetch_outer(estate)? else {
                node.agg_done = true;
                break;
            };
            let tmpcontext = node.tmpcontext;
            let AggStateData { persort, evaltrans, .. } = node;
            let ps = persort.as_mut().expect("sorted Agg has persort");
            let outer_slot = estate.slot_mut(outer_id);
            let mut slots = EvalSlots {
                scan: None,
                inner: Some(&mut ps.first_slot),
                outer: Some(&mut *outer_slot),
            };
            let same_group = match ps.eq.as_mut() {
                Some(eq) => exec_qual(Some(eq), &mut slots)?,
                // numCols == 0: no group boundary, as C's numCols > 0 guard.
                None => true,
            };
            if !same_group {
                exectuples::exec_copy_slot(&mut ps.pending_slot, outer_slot, mcx, mcx)?;
                ps.have_pending = true;
                break;
            }
            let et = evaltrans.as_mut().unwrap();
            if et.has_subplan() {
                estate.ecxt_mut(tmpcontext).ecxt_outertuple = Some(outer_id);
                ::executils::exec_eval_expr_with_subplans(et, estate, tmpcontext)?;
            } else {
                let mut slots =
                    EvalSlots { scan: None, inner: None, outer: Some(&mut *outer_slot) };
                exec_eval_expr(et, &mut slots)?;
            }
            if !node.pertrans_sort.is_empty() {
                collect_ordered_input(node, estate, 1)?;
            }
            estate.reset_expr_context(node.tmpcontext);
        }
        process_ordered_aggregates(node, estate)?;
        finalize_aggregates(node, estate, node.pergroup_base)?;

        if node.proj.has_subplan() || node.qual.as_deref().is_some_and(|q| q.has_subplan()) {
            let ecxt = node.ps_ExprContext;
            let result = node.ps_ResultTupleSlot;
            let instr_idx = node.instr_idx;
            let AggStateData { persort, qual, proj, .. } = node;
            let ps = persort.as_mut().expect("sorted Agg has persort");
            if !::executils::exec_qual_with_subplans_outer(
                qual.as_deref_mut(),
                &mut ps.first_slot,
                estate,
                ecxt,
            )? {
                estate.instr_count_filtered1(instr_idx);
                continue;
            }
            ::executils::exec_project_with_subplans_outer(
                proj,
                &mut ps.first_slot,
                estate,
                ecxt,
                result,
            )?;
            return Ok(Some(result));
        }
        {
            let AggStateData { persort, qual, .. } = node;
            let ps = persort.as_mut().expect("sorted Agg has persort");
            let mut slots =
                EvalSlots { scan: None, inner: None, outer: Some(&mut ps.first_slot) };
            if !exec_qual(qual.as_deref_mut(), &mut slots)? {
                estate.instr_count_filtered1(node.instr_idx);
                continue;
            }
        }
        let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
        let ps = node.persort.as_mut().unwrap();
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(&mut ps.first_slot) };
        exec_project(&mut node.proj, &mut slots, result_slot, mcx)?;
        return Ok(Some(node.ps_ResultTupleSlot));
    }
    Ok(None)
}

// agg_fill_hash_table (nodeAgg.c): drain the child through the hash lookup +
// transition program; spill-mode misses skip the program for the row.
fn agg_fill_hash_table<'mcx, F>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    fetch_outer: &mut F,
) -> PgResult<()>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    while let Some(outer_id) = fetch_outer(estate)? {
        estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(outer_id);
        if lookup_hash_entry(node, estate, outer_id)? {
            let outer_slot = estate.slot_mut(outer_id);
            let mut slots = EvalSlots { scan: None, inner: None, outer: Some(outer_slot) };
            exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)?;
        }
        estate.reset_expr_context(node.tmpcontext);
    }
    merge::consume_handoff(node, estate)?;
    hashagg_finish_initial_spills(node, estate)?;
    merge::maybe_install_handoff(node, estate)?;
    let ph = node.perhash.as_mut().unwrap();
    ph.table_filled = true;
    ph.hashiter = 0;
    Ok(())
}

// hash_agg_update_metrics (nodeAgg.c); hashkey mem = the aggcontext
// subtree (byref transvalues; C's hashcontext per-tuple memory).
fn hash_agg_update_metrics(
    node: &mut AggStateData<'_>,
    estate: &mut EStateData<'_>,
    from_tape: bool,
    npartitions: usize,
) {
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    // SAFETY: read of the once-allocated node; no &mut is live to it.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let meta_mem = ph.hashtable.meta_mem() as u64;
    let entry_mem = ph.table_ctx.subtree_used() as u64;
    let hashkey_mem = aggctx.context().subtree_used() as u64;
    let buffer_mem = npartitions as u64 * HASHAGG_WRITE_BUFFER_SIZE as u64
        + if from_tape { HASHAGG_READ_BUFFER_SIZE as u64 } else { 0 };
    let total = meta_mem + entry_mem + hashkey_mem + buffer_mem;
    let id = node.plan.plan.plan_node_id;
    let ai = agg_instrumentation(estate, id);
    ai.hash_mem_peak = ai.hash_mem_peak.max(total);
    if let Some(ts) = ph.spill.tapeset.as_ref() {
        // BLCKSZ / 1024.
        let disk_used = ts.blocks() as u64 * 8;
        ai.hash_disk_used = ai.hash_disk_used.max(disk_used);
    }
    if ph.hash_ngroups_current > 0 {
        // 16 = C TupleHashEntrySize().
        ph.spill.hashentrysize = 16.0 + hashkey_mem as f64 / ph.hash_ngroups_current as f64;
    }
    if hashagg_memdebug_enabled() {
        let tag = if from_tape { "batch_done" } else { "initial_fill_done" };
        hashagg_memdebug(tag, ph, hashkey_mem as usize, buffer_mem as usize);
    }
}

// prepare_hash_slot (nodeAgg.c).
#[inline(always)]
fn prepare_hash_slot<'mcx>(
    hashslot: &mut SlotData<'mcx>,
    hash_grp_col_idx_input: &[i16],
    largest_grp_col_idx: i32,
    input: &mut SlotData<'mcx>,
    mcx: ::mcx::Mcx<'mcx>,
) {
    exectuples::slot_getsomeattrs(input, largest_grp_col_idx);
    exectuples::exec_clear_tuple(hashslot, mcx);
    {
        let src = input.base();
        let dst = hashslot.base_mut();
        for (i, &attno) in hash_grp_col_idx_input.iter().enumerate() {
            let v = (attno - 1) as usize;
            dst.tts_values[i] = src.tts_values[v];
            dst.tts_isnull[i] = src.tts_isnull[v];
        }
    }
    exectuples::exec_store_virtual_tuple(hashslot);
}

// prepare_hash_slot + lookup_hash_entries (nodeAgg.c), single set: false =
// spill-mode miss, tuple spilled, the caller skips the transition program.
fn lookup_hash_entry<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_id: ExecSlotId,
) -> PgResult<bool> {
    let mcx = estate.es_query_cxt;
    let AggStateData { perhash, trans_init, trans_typ, agg_node, .. } = node;
    let ph = perhash.as_mut().expect("hashed Agg has perhash");

    let outer_slot = estate.slot_mut(outer_id);
    {
        let PerHashData { hashslot, hash_grp_col_idx_input, largest_grp_col_idx, .. } =
            &mut *ph;
        prepare_hash_slot(
            hashslot,
            hash_grp_col_idx_input,
            *largest_grp_col_idx,
            outer_slot,
            mcx,
        );
    }

    let hash = ph.hashtable.hash_slot(&mut ph.hashslot)?;
    let table_mcx = ph.table_ctx.mcx();
    let use_table = !ph.spill.mode;
    let (ix, isnew) =
        ph.hashtable.lookup(&mut ph.hashslot, hash, use_table.then_some(table_mcx), mcx)?;
    let Some(ix) = ix else {
        hashagg_spill_tuple(&mut ph.spill, Some(outer_slot), hash, mcx)?;
        return Ok(false);
    };
    if isnew {
        initialize_hash_entry(ph, trans_init, trans_typ, *agg_node, ix, mcx)?;
    } else if !trans_init.is_empty() {
        let pergroup = ph
            .hashtable
            .entry_additional(ix)
            .expect("numtrans > 0 tables carry additional space")
            .cast::<AggPerGroup>();
        // SAFETY: the cell is a once-allocated live slot the trans steps read.
        unsafe { ph.pergroup_cell.write(pergroup) };
    }
    Ok(true)
}

// agg_retrieve_hash_table(_in_memory) (nodeAgg.c): one qual-passing group per
// call, the representative tuple rebuilt into the outer-format first_slot.
// `cut`: the lane's armed emit-side top-N boundary (lane-v2 topnemit) — skip
// groups strictly worse than the downstream bounded sort's k-th boundary
// BEFORE key reconstruction / finalize / projection (admission proved the
// skipped body observation-free; `None` = C's retrieve verbatim).
fn agg_retrieve_hash_table<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut cut: Option<TopnEmitCut<'_>>,
) -> PgResult<Option<ExecSlotId>> {
    // M2 sink (sink.rs): an adopted parallel result is the node's source —
    // finished, fully-projected rows (no pergroups; the topn cut is a skip
    // optimization over transvalues and simply does not apply).
    if node.sink_emit.is_some() {
        return sink::agg_sink_emit_next(node, estate);
    }
    let mcx = estate.es_query_cxt;
    loop {
        estate.reset_expr_context(node.ps_ExprContext);

        // Lane-v2 compact-table read-back (compact.rs): row order —
        // order-relaxed vs the C bucket iterate; no spill refill (compact
        // builds never spill). first_slot gets the reconstructed grouping
        // key, exactly the copy the C arm below performs from the stored
        // tuple; the shared finalize/qual/project tail runs unchanged.
        let pergroup = if node.perhash.as_ref().is_some_and(|ph| ph.compact.is_some()) {
            match compact::compact_retrieve_next(node, estate, cut.as_mut())? {
                Some(pg) => pg,
                None => {
                    node.agg_done = true;
                    return Ok(None);
                }
            }
        } else {
        let next = {
            let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
            ph.hashtable.iterate(&mut ph.hashiter)
        };
        let Some(ix) = next else {
            if !agg_refill_hash_table(node, estate)? {
                node.agg_done = true;
                return Ok(None);
            }
            continue;
        };
        // Top-N boundary cut, hoisted in front of the entry-tuple store and
        // the grouping-key copy: the group's pergroup state is reachable
        // without either.
        if let Some(c) = cut.as_mut() {
            let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
            if let Some(p) = ph.hashtable.entry_additional(ix) {
                // SAFETY: transno < the entry's once-allocated pergroup
                // array length (resolve checked it against this node).
                let pg =
                    unsafe { &*p.cast::<AggPerGroup>().as_ptr().add(c.spec.transno as usize) };
                if c.skips(pg) {
                    *c.skipped += 1;
                    // The elided sort put's per-row cadence.
                    postgres_seams::check_for_interrupts::call()?;
                    continue;
                }
            }
        }
        {
            let ph = node.perhash.as_mut().expect("hashed Agg has perhash");

            let tup = ph.hashtable.entry_tuple(ix);
            // SAFETY: entry images live in the node's table context for the
            // table's lifetime.
            unsafe { exectuples::exec_store_minimal_tuple_ptr(&mut ph.retrieve_slot, mcx, tup) };
            exectuples::slot_getallattrs(&mut ph.retrieve_slot);

            exectuples::exec_store_all_null_tuple(&mut ph.first_slot, mcx);
            {
                let PerHashData {
                    retrieve_slot: hashslot, first_slot, hash_grp_col_idx_input, ..
                } = &mut *ph;
                let src = hashslot.base();
                let dst = first_slot.base_mut();
                for (i, &attno) in hash_grp_col_idx_input.iter().enumerate() {
                    let v = (attno - 1) as usize;
                    dst.tts_values[v] = src.tts_values[i];
                    dst.tts_isnull[v] = src.tts_isnull[i];
                }
            }
            ph.hashtable.entry_additional(ix).map_or(NonNull::dangling(), |p| p.cast())
        }
        };
        // Written by lookup_hash_entry; unread (and dangling) when peragg is
        // empty.
        finalize_aggregates(node, estate, pergroup)?;

        if node.proj.has_subplan() || node.qual.as_deref().is_some_and(|q| q.has_subplan()) {
            let ecxt = node.ps_ExprContext;
            let result = node.ps_ResultTupleSlot;
            let instr_idx = node.instr_idx;
            let AggStateData { perhash, qual, proj, .. } = node;
            let ph = perhash.as_mut().unwrap();
            if !::executils::exec_qual_with_subplans_outer(
                qual.as_deref_mut(),
                &mut ph.first_slot,
                estate,
                ecxt,
            )? {
                estate.instr_count_filtered1(instr_idx);
                continue;
            }
            ::executils::exec_project_with_subplans_outer(
                proj,
                &mut ph.first_slot,
                estate,
                ecxt,
                result,
            )?;
            return Ok(Some(result));
        }
        {
            let AggStateData { perhash, qual, .. } = node;
            let ph = perhash.as_mut().unwrap();
            let mut slots =
                EvalSlots { scan: None, inner: None, outer: Some(&mut ph.first_slot) };
            if !exec_qual(qual.as_deref_mut(), &mut slots)? {
                estate.instr_count_filtered1(node.instr_idx);
                continue;
            }
        }
        let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
        let ph = node.perhash.as_mut().unwrap();
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(&mut ph.first_slot) };
        exec_project(&mut node.proj, &mut slots, result_slot, mcx)?;
        return Ok(Some(node.ps_ResultTupleSlot));
    }
}

/// M2 sink teardown floor: an adopted parallel emit result at or above this
/// content size triggers an allocator release after the drop. The engagement
/// that produced it churned a working set several times larger (per-worker
/// tables + radix runs + merge tables, most of it in helper threads that have
/// already exited), all freed-but-RETAINED by mimalloc — at high-cardinality @100M-class
/// shapes (~10^8 groups) that ratchet is multi-GB per execution and a repeat
/// run of the same query crosses the pod cgroup ceiling: the kernel
/// OOM-kills the whole (single-process) server silently (m2-coverage
/// forensics class; notes/q33-try2-kill.md). Same discipline as the hashagg
/// spill path's `hashagg_release_retained` sites. Small emits skip: the
/// collect is not free and sub-64MB engagements cannot meaningfully ratchet.
const SINK_RELEASE_MIN_BYTES: usize = 64 << 20;

/// `ExecEndAgg` node-local half; the caller ends the outer child (contexts
/// are freed with the EState).
pub fn exec_end_agg(node: &mut AggStateData<'_>) {
    // M2 sink: drop the adopted parallel emit state NOW (its bufs are std
    // allocations, not EState arena) and, for engagement-sized results,
    // release the allocator's freed-but-retained segments so the NEXT
    // execution of this shape rebuilds inside the same RSS envelope instead
    // of stacking a second working set on top (see SINK_RELEASE_MIN_BYTES).
    if let Some(st) = node.sink_emit.take() {
        let bytes: usize = st.retained_bytes();
        drop(st);
        if bytes >= SINK_RELEASE_MIN_BYTES {
            hashagg_release_retained("sink_teardown");
        }
    }
    // sorted-arm lane: same discipline for the ordered sink's segments.
    sortedsink::agg_sorted_sink_reset(node);
    node.qual = None;
    node.merge = None;
    hashgrouped::agg_hashgroup_reset(node);
    codedgroup::agg_codedgroup_reset(node);
    agg_pdemit_reset(node);
    if let Some(ph) = node.perhash.as_mut() {
        hashagg_reset_spill_state(ph, node.plan.numGroups as f64);
    }
    if let Some(gs) = node.gsets.as_mut() {
        gsets::end_grouping_sets(gs);
    }
    node.perhash = None;
    node.persort = None;
    node.gsets = None;
    node.pertrans_sort.clear();
    for pa in node.peragg.iter_mut() {
        pa.finalfn = None;
    }
    node.proj.release_frames();
    if let Some(et) = node.evaltrans.as_mut() {
        et.release_frames();
    }
    if let Some(r) = node.lanefold.as_mut().and_then(|lf| lf.resid.as_mut()) {
        r.release_frames();
    }
    node.ps_ResultTupleDesc = None;
}

/// `ExecReScanAgg` (nodeAgg.c) AGG_PLAIN arm; the caller rescans the outer
/// child (chgParam is always NULL until the Param lanes land).
/// ExecReScanAgg (nodeAgg.c), chgParam-nonnull arm: input changed, so hashed
/// results are rebuilt (C reuses only when no params changed in the subtree).
pub fn exec_rescan_agg_chg<'mcx>(node: &mut AggStateData<'mcx>, _estate: &mut EStateData<'mcx>) {
    let numgroups = node.plan.numGroups as f64;
    node.agg_done = false;
    // Hash-grouped-arm state (any phase) rebuilds from scratch: drop it so
    // the aggcontext reset below can free its by-ref transvalues safely.
    hashgrouped::agg_hashgroup_reset(node);
    codedgroup::agg_codedgroup_reset(node);
    // Runtime distinct sink paremit: spent on rescan, same law as the sink.
    agg_pdemit_reset(node);
    merge::reset_merge_for_rescan(node);
    // M2 sink: a rescan re-engages (or falls back) from scratch; any adopted
    // parallel emit state is spent.
    sink::agg_sink_reset_emit(node);
    sortedsink::agg_sorted_sink_reset(node);
    for ps in node.pertrans_sort.iter_mut() {
        for st in ps.sortstates.iter_mut() {
            if let Some(sort) = st.take() {
                sort.end();
            }
        }
        // A rescan can cut a group short of finalize: drop the presorted
        // DISTINCT comparand (C leaves haslast set over reset memory here).
        ps.haslast = false;
        ps.last_single = NullableDatum::null();
    }
    if let Some(gs) = node.gsets.as_mut() {
        gsets::rescan_grouping_sets(gs).expect("grouping-sets rescan");
        return;
    }
    if let Some(ph) = node.perhash.as_mut() {
        ph.table_filled = false;
        ph.hashiter = 0;
        ph.hash_ngroups_current = 0;
        hashagg_reset_spill_state(ph, numgroups);
        ph.spill.ever_spilled = false;
        ph.spill.mode = false;
        ph.hashtable.reset();
        ph.table_ctx.reset();
        compact::compact_reset(ph);
        // Fresh build: the exchange re-resolves (a spill-disabled Off must
        // not leak into the rebuilt table's run).
        ph.exchange = merge::ExchangeState::Unresolved;
    }
    if let Some(ps) = node.persort.as_mut() {
        ps.have_pending = false;
    }
    // SAFETY: sole access path to the node during the reset; frees hashed
    // byref transvalues too (they live in aggcontext).
    unsafe { node.agg_node.as_mut() }.reset();
}

pub fn exec_rescan_agg<'mcx>(node: &mut AggStateData<'mcx>, _estate: &mut EStateData<'mcx>) {
    let numgroups = node.plan.numGroups as f64;
    node.agg_done = false;
    // Hash-grouped-arm state (any phase) rebuilds from scratch (see
    // exec_rescan_agg_chg).
    hashgrouped::agg_hashgroup_reset(node);
    codedgroup::agg_codedgroup_reset(node);
    // Runtime distinct sink paremit: spent on rescan, same law as the sink.
    agg_pdemit_reset(node);
    // Merged results combine into the handed buffers in place, so a rescan
    // rebuilds from a fresh worker run instead of reusing the filled table.
    let merged = merge::reset_merge_for_rescan(node);
    // M2 sink: spent on rescan (see exec_rescan_agg_chg).
    sink::agg_sink_reset_emit(node);
    sortedsink::agg_sorted_sink_reset(node);
    for ps in node.pertrans_sort.iter_mut() {
        for st in ps.sortstates.iter_mut() {
            if let Some(sort) = st.take() {
                sort.end();
            }
        }
        // A rescan can cut a group short of finalize: drop the presorted
        // DISTINCT comparand (C leaves haslast set over reset memory here).
        ps.haslast = false;
        ps.last_single = NullableDatum::null();
    }
    if let Some(gs) = node.gsets.as_mut() {
        // C's no-chgParam AGG_HASHED arm: filled tables are reused, only the
        // iterators reset.
        if !gsets::rescan_hash_reuse(gs) {
            gsets::rescan_grouping_sets(gs).expect("grouping-sets rescan");
        }
        return;
    }
    if let Some(ph) = node.perhash.as_mut() {
        if !ph.spill.ever_spilled && !merged {
            // C's no-chgParam arm: the filled table is reused, only the
            // iterator resets (the caller's child rescan is then redundant
            // but harmless).
            ph.hashiter = 0;
            return;
        }
        // Spilled tables were consumed batchwise; rebuild (C falls through).
        ph.table_filled = false;
        ph.hashiter = 0;
        ph.hash_ngroups_current = 0;
        hashagg_reset_spill_state(ph, numgroups);
        ph.spill.ever_spilled = false;
        ph.spill.mode = false;
        ph.hashtable.reset();
        ph.table_ctx.reset();
        compact::compact_reset(ph);
        // Fresh build: the exchange re-resolves (a spill-disabled Off must
        // not leak into the rebuilt table's run).
        ph.exchange = merge::ExchangeState::Unresolved;
        // SAFETY: sole access path to the node during the reset.
        unsafe { node.agg_node.as_mut() }.reset();
        return;
    }
    if let Some(ps) = node.persort.as_mut() {
        ps.have_pending = false;
    }
    // SAFETY: sole access path to the node during the reset.
    unsafe { node.agg_node.as_mut() }.reset();
}

/// C `AggGetAggref` (nodeAgg.c).
///
/// # Safety
/// `fcinfo.context`, if set, points at a live node outliving `'a`; the
/// cur-agg slot only ever holds `&'query Aggref` pointers.
pub unsafe fn agg_get_aggref<'a>(
    fcinfo: &::types_fmgr::FunctionCallInfoBaseData,
) -> Option<&'a Aggref<'a>> {
    // SAFETY: caller contract.
    let node = unsafe { fcinfo.agg_state_node() }?;
    let (p, _) = node.current_agg()?;
    // SAFETY: writer invariant above.
    Some(unsafe { p.cast::<Aggref<'a>>().as_ref() })
}

/// C `AggStateIsShared` (nodeAgg.c); true (conservative) outside an agg call.
///
/// # Safety
/// As [`agg_get_aggref`].
pub unsafe fn agg_state_is_shared(fcinfo: &::types_fmgr::FunctionCallInfoBaseData) -> bool {
    // SAFETY: caller contract.
    match unsafe { fcinfo.agg_state_node() } {
        Some(node) => node.current_agg().map_or(true, |(_, shared)| shared),
        None => true,
    }
}

/// C `AggRegisterCallback` (nodeAgg.c).
///
/// # Safety
/// As [`agg_get_aggref`], plus `AggStateNode::register_shutdown_callback`'s
/// contract on `func`/`arg`.
pub unsafe fn agg_register_callback(
    fcinfo: &::types_fmgr::FunctionCallInfoBaseData,
    func: unsafe fn(*mut ()),
    arg: *mut (),
) -> PgResult<()> {
    // SAFETY: caller contract.
    match unsafe { fcinfo.agg_state_node() } {
        Some(node) => {
            // SAFETY: caller contract.
            unsafe { node.register_shutdown_callback(func, arg) };
            Ok(())
        }
        None => Err(Box::new(PgError::error(
            "aggregate function cannot register a callback in this context",
        ))),
    }
}

mcx::forget_safe_nodrop!(TransTyp, HashAggBatch);

// Exempt: all released in exec_end_agg (proj/evaltrans via release_frames;
// the spill tapeset via hashagg_reset_spill_state; the table/tmp contexts
// die with the struct's normal drop).
mcx::forget_safe_struct!(
    PerAggData<'_> { transno, aggref, trans_shared, num_final_args,
        agg_collation, resulttype_len;
        finalfn, serialfn, direct_args },
    PerSortData<'_> { have_pending; first_slot, pending_slot, eq },
    HashSpillState<'_> { mode, ever_spilled, batches, all_cols_needed,
        max_colno_needed, colnos_needed, read_buf, input_card, used_bits,
        hashentrysize;
        spill, tapeset, rslot, wslot, tmp_ctx },
    PerHashData<'_> { num_cols, hash_grp_col_idx_input, largest_grp_col_idx,
        outer_natts, pergroup_cell, hash_ngroups_limit, hash_ngroups_current,
        hash_mem_limit, table_filled, hashiter, spill, sink_cap, sink_spill_ok;
        hashtable, hashslot, retrieve_slot, first_slot, table_ctx, compact,
        exchange },
    AggStateData<'_> { plan, ps_ExprContext, tmpcontext, agg_node,
        ps_ResultTupleSlot, peragg, trans_init, trans_typ, _pergroup,
        pergroup_base, agg_values_base, agg_nulls_base, agg_done, skip_final, numtrans,
        avgpack_shape_mask,
        force_distinct_set, group_eq_representational, trans_order_insensitive,
        instr_idx, hash_build_combined;
        ps_ResultTupleDesc, proj, evaltrans, perhash, merge, persort, gsets,
        pertrans_sort, qual, lanefold, meta_aggs, hashgroup, codedgroup,
        sink_emit, pdemit, sorted_sink_emit },
    // resid released in exec_end_agg (evaltrans discipline); the plan holds
    // only arena PgVecs.
    LaneFold<'_> { plan; resid },
);
