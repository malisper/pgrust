//! Runtime scan-pipeline plain-agg partial states (M1 scan pipelines,
//! docs/design/parallelism-redesign-2026-07.md §2.2/§5-M1).
//!
//! A pinned runtime pipeline runs the plain-agg FOLD feed per worker over
//! disjoint morsel claims; each worker's end state is its node's pergroup
//! transvalues. This module gives those states a SELF-CONTAINED, memory-
//! context-free representation (`RuntimePartial` — plain Rust integers) so
//! they can cross to the leader after the worker's executor and transaction
//! are gone, plus the exact leader-side absorption (`exec_agg_runtime_
//! partials` mirrors `exec_agg_meta`'s pergroup construction: the absorbed
//! state is byte-for-byte the end state N transfn calls would have left).
//!
//! REASSOCIATION LEGALITY (the byval-combine / order-insensitive-exact
//! discipline): only LaneKinds whose cross-worker combine is exact and
//! order-insensitive admit —
//!   * CountStar/CountAny — i64 add;
//!   * Sum (int2/int4_sum: i64 wrapping accumulate) — wrapping add is the
//!     mod-2^64 ring, commutative/associative, so any grouping of row terms
//!     bit-equals C's serial accumulation;
//!   * AvgAccum (int8[2] {count,sum}) / Int128AvgAccum (Int128AggState) —
//!     component-wise adds, same argument;
//!   * Min/Max (strict byval, signed total order) — integer ties are
//!     IDENTICAL datum words, so the survivor is bit-identical under any
//!     combine order;
//!   * BitAnd/BitOr, BoolAnd/BoolOr — bitwise/boolean lattice ops.
//! REFUSED: FMin/FMax (NaN payload / -0.0 tie survivor depends on row
//! order), StrMin/StrMax, BpMin/BpMax (by-ref, tie rules keep a specific
//! ROW's datum), and any plan with residual per-row transitions (arbitrary
//! transfns). The refusal is the engagement gate's job (`agg_runtime_
//! partial_admissible`) — fail-closed, before any worker launches.

use ::datum::Datum;
use ::lanefold::{LaneKind, LaneTrans};
use ::types_error::{PgError, PgResult};

use ::executils::{EStateData, ExecSlotId};

use crate::AggStateData;

/// One transno's self-contained partial state.
#[derive(Clone, Copy, Debug)]
pub enum RuntimePartialTrans {
    /// CountStar/CountAny running count (transvalue is never NULL).
    Count(i64),
    /// int2/int4_sum accumulator; `present` = transvalue not NULL (NULL
    /// init, first non-null row adopts).
    Sum { v: i64, present: bool },
    /// Strict byval Min/Max/BitAnd/BitOr/BoolAnd/BoolOr fold word,
    /// sign-extended from the lane's result width.
    Fold { v: i64, present: bool },
    /// int2/int4_avg_accum int8[2] {count, sum} transarray state.
    AvgAccum { n: i64, sum: i64 },
    /// int8_avg_accum Int128AggState {n, sum_x}; `present` = state
    /// allocated (the transfn ran at least once).
    Int128 { n: i64, sum: i128, present: bool },
}

/// A worker's (or the combined) plain-agg partial: one entry per admitted
/// transno, in plan `trans` order.
#[derive(Clone, Debug, Default)]
pub struct RuntimePartial {
    pub trans: Vec<(u16, RuntimePartialTrans)>,
}

fn kind_admits(kind: LaneKind) -> bool {
    matches!(
        kind,
        LaneKind::CountStar
            | LaneKind::CountAny
            | LaneKind::Sum
            | LaneKind::AvgAccum
            | LaneKind::Int128AvgAccum
            | LaneKind::Min
            | LaneKind::Max
            | LaneKind::BitAnd
            | LaneKind::BitOr
            | LaneKind::BoolAnd
            | LaneKind::BoolOr
    )
}

/// Fail-closed shape admission for the runtime pipeline: a classified fold
/// plan covering EVERY transition (no residuals), every kind combinable
/// exactly under reassociation. The caller separately gates strategy
/// (AGG_PLAIN), scan shape, and session/binder policy.
pub fn agg_runtime_partial_admissible(node: &AggStateData<'_>) -> bool {
    match crate::agg_lanefold_plan(node) {
        Some(plan) => {
            plan.resid.is_empty() && plan.trans.iter().all(|t| kind_admits(t.kind))
        }
        None => false,
    }
}

// Sign-extended fold word at the lane's RESULT width (the transvalue store
// width) — exact under either datum-construction convention because every
// admitted store is itself width-faithful.
fn fold_word(t: &LaneTrans, d: Datum) -> i64 {
    use ::lanefold::LaneWidth;
    match t.res_width {
        LaneWidth::I16 => t_i64(d.as_i16() as i64),
        LaneWidth::I32 => t_i64(d.as_i32() as i64),
        LaneWidth::Bool => d.as_bool() as i64,
        _ => d.as_i64(),
    }
}

#[inline]
fn t_i64(v: i64) -> i64 {
    v
}

// int8[2] transarray element pointer (the AvgAccum state layout
// exec_agg_meta pins: 4B-uncompressed varlena of INT8_TRANSARRAY_SIZE with
// no nulls bitmap). Returns (count, sum) reading, or writes via the mut arm.
unsafe fn int8_transarray_elems(datum: Datum) -> PgResult<*mut i64> {
    let arr = datum.as_usize() as *mut u8;
    if !::types_tuple::varatt::varatt_is_4b_u(arr)
        || ::types_tuple::varatt::varsize_4b(arr) != ::lanefold::INT8_TRANSARRAY_SIZE
        || arr.add(8).cast::<i32>().read() != 0
    {
        return Err(Box::new(PgError::error(
            "runtime partial: unexpected avg transarray shape".to_string(),
        )));
    }
    Ok(arr.add(::lanefold::ARR_OVERHEAD_NONULLS_1).cast::<i64>())
}

/// WORKER side helper for the export below. Must run before the worker's
/// executor is torn down (the by-ref states live in its aggcontext).
/// Export the node's plain pergroups into a caller-retained partial
/// (capacity reused across morsels — the export runs once per morsel per
/// worker, and a fresh Vec each time was a malloc+free pair on the engaged
/// data path; m2-integration std-collections audit, AGENTS.md rule 7).
///
/// ERROR-PATH INVARIANT (leader side must uphold): the partial is cleared
/// BEFORE the fallible fill, so on Err the slot holds a TRUNCATED partial.
/// This is safe because every worker error marks the drive self-errored and
/// the leader combines partials only on a clean Completed outcome
/// (runtime_scan's take_error discipline) — never adopt a slot from an
/// errored engagement.
pub fn agg_runtime_export_partial_into(
    node: &AggStateData<'_>,
    partial: &mut RuntimePartial,
) -> PgResult<()> {
    export_partial_from(node, crate::agg_plain_pergroup_base(node), partial)
}

/// SORTED-arm twin (q28-sorted-arm): export the OPEN group's pergroup states
/// (the sorted drive's single current-group array) — the per-claim boundary
/// partial of the ordered-grouped runtime arm. Same admission, same
/// self-contained representation, same error-path invariant as the plain
/// export above.
pub fn agg_sorted_export_partial_into(
    node: &AggStateData<'_>,
    partial: &mut RuntimePartial,
) -> PgResult<()> {
    debug_assert_eq!(node.plan.aggstrategy, crate::AGG_SORTED);
    export_partial_from(node, crate::agg_sorted_pergroup_base(node), partial)
}

fn export_partial_from(
    node: &AggStateData<'_>,
    base: core::ptr::NonNull<::execexpr::AggPerGroup>,
    partial: &mut RuntimePartial,
) -> PgResult<()> {
    let plan = crate::agg_lanefold_plan(node)
        .ok_or_else(|| PgError::error("runtime partial export without a fold plan".to_string()))?;
    let out = &mut partial.trans;
    out.clear();
    out.reserve(plan.trans.len());
    for t in plan.trans.iter() {
        // SAFETY: transno indexes the node's once-allocated pergroup array
        // (fold plan transnos come from its spec list).
        let pg = unsafe { &*base.as_ptr().add(t.transno as usize) };
        let p = match t.kind {
            LaneKind::CountStar | LaneKind::CountAny => {
                RuntimePartialTrans::Count(pg.trans_value.as_i64())
            }
            LaneKind::Sum => RuntimePartialTrans::Sum {
                v: if pg.trans_value_is_null { 0 } else { pg.trans_value.as_i64() },
                present: !pg.trans_value_is_null,
            },
            LaneKind::Min
            | LaneKind::Max
            | LaneKind::BitAnd
            | LaneKind::BitOr
            | LaneKind::BoolAnd
            | LaneKind::BoolOr => RuntimePartialTrans::Fold {
                v: if pg.trans_value_is_null { 0 } else { fold_word(t, pg.trans_value) },
                present: !pg.trans_value_is_null,
            },
            LaneKind::AvgAccum => {
                // Initval copy is never NULL (agg_plain_build_begin).
                if pg.trans_value_is_null {
                    return Err(Box::new(PgError::error(
                        "runtime partial: NULL avg transarray".to_string(),
                    )));
                }
                // SAFETY: aggcontext-lived initval copy, shape validated in
                // the helper.
                let td = unsafe { int8_transarray_elems(pg.trans_value)? };
                RuntimePartialTrans::AvgAccum {
                    n: unsafe { td.read() },
                    sum: unsafe { td.add(1).read() },
                }
            }
            LaneKind::Int128AvgAccum => {
                if pg.trans_value_is_null {
                    RuntimePartialTrans::Int128 { n: 0, sum: 0, present: false }
                } else {
                    use ::adt_numeric::aggregates::Int128AggState;
                    // SAFETY: non-null Int128AvgAccum transvalues point at
                    // the aggcontext state the fold/transfn chain installed.
                    let st =
                        unsafe { &*(pg.trans_value.as_usize() as *const Int128AggState) };
                    if st.calc_sum_x2 {
                        return Err(Box::new(PgError::error(
                            "runtime partial: unexpected sum_x2 state".to_string(),
                        )));
                    }
                    RuntimePartialTrans::Int128 { n: st.n, sum: st.sum_x, present: true }
                }
            }
            _ => {
                return Err(Box::new(PgError::error(
                    "runtime partial: inadmissible lane kind".to_string(),
                )))
            }
        };
        out.push((t.transno, p));
    }
    Ok(())
}

fn combine_two(kind: LaneKind, a: RuntimePartialTrans, b: RuntimePartialTrans) -> RuntimePartialTrans {
    use RuntimePartialTrans as P;
    match (a, b) {
        (P::Count(x), P::Count(y)) => P::Count(x.wrapping_add(y)),
        (P::Sum { v: x, present: px }, P::Sum { v: y, present: py }) => P::Sum {
            v: x.wrapping_add(y),
            present: px || py,
        },
        (P::Fold { v: x, present: px }, P::Fold { v: y, present: py }) => {
            let v = match (px, py) {
                (true, true) => match kind {
                    LaneKind::Min => x.min(y),
                    LaneKind::Max => x.max(y),
                    LaneKind::BitAnd => x & y,
                    LaneKind::BitOr => x | y,
                    LaneKind::BoolAnd => ((x != 0) && (y != 0)) as i64,
                    LaneKind::BoolOr => ((x != 0) || (y != 0)) as i64,
                    _ => unreachable!("fold combine over a non-fold kind"),
                },
                (true, false) => x,
                (false, _) => y,
            };
            P::Fold { v, present: px || py }
        }
        (P::AvgAccum { n: nx, sum: sx }, P::AvgAccum { n: ny, sum: sy }) => P::AvgAccum {
            n: nx.wrapping_add(ny),
            sum: sx.wrapping_add(sy),
        },
        (P::Int128 { n: nx, sum: sx, present: px }, P::Int128 { n: ny, sum: sy, present: py }) => {
            P::Int128 { n: nx + ny, sum: sx + sy, present: px || py }
        }
        _ => unreachable!("mismatched runtime partial shapes for one transno"),
    }
}

/// Pairwise combine (the ordered-grouped arm's boundary stitch): fold `src`
/// into `dst` in place. Left-to-right stitch order; every admitted kind is
/// order-insensitive-exact so the association cannot be observed.
pub fn agg_runtime_combine_into(
    node: &AggStateData<'_>,
    dst: &mut RuntimePartial,
    src: &RuntimePartial,
) -> PgResult<()> {
    let plan = crate::agg_lanefold_plan(node)
        .ok_or_else(|| PgError::error("runtime partial combine without a fold plan".to_string()))?;
    for p in [&*dst, src] {
        if p.trans.len() != plan.trans.len()
            || p.trans
                .iter()
                .zip(plan.trans.iter())
                .any(|(&(no, _), t)| no != t.transno)
        {
            return Err(Box::new(PgError::error(
                "runtime partial: transno layout mismatch".to_string(),
            )));
        }
    }
    for (i, t) in plan.trans.iter().enumerate() {
        dst.trans[i].1 = combine_two(t.kind, dst.trans[i].1, src.trans[i].1);
    }
    Ok(())
}

/// Combine worker partials (order-insensitive-exact for every admitted
/// kind; install order is immaterial by construction).
pub fn agg_runtime_combine(
    node: &AggStateData<'_>,
    parts: &[RuntimePartial],
) -> PgResult<RuntimePartial> {
    let plan = crate::agg_lanefold_plan(node)
        .ok_or_else(|| PgError::error("runtime partial combine without a fold plan".to_string()))?;
    let mut acc: Option<RuntimePartial> = None;
    for p in parts {
        if p.trans.len() != plan.trans.len()
            || p.trans
                .iter()
                .zip(plan.trans.iter())
                .any(|(&(no, _), t)| no != t.transno)
        {
            return Err(Box::new(PgError::error(
                "runtime partial: transno layout mismatch".to_string(),
            )));
        }
        acc = Some(match acc {
            None => p.clone(),
            Some(mut a) => {
                for (i, t) in plan.trans.iter().enumerate() {
                    a.trans[i].1 = combine_two(t.kind, a.trans[i].1, p.trans[i].1);
                }
                a
            }
        });
    }
    Ok(acc.unwrap_or_default())
}

/// LEADER side: initialize the plain pergroups, absorb the combined partial
/// (byte-for-byte the end state the serial transfn chain leaves), then run
/// the ordinary retrieve (finalize + HAVING + project). Mirrors
/// `exec_agg_meta`'s construction discipline exactly.
pub fn exec_agg_runtime_partials<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    combined: &RuntimePartial,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert_eq!(node.plan.aggstrategy, crate::AGG_PLAIN);
    if node.agg_done {
        return Ok(None);
    }
    crate::agg_plain_build_begin(node, estate)?;
    absorb_partial_states(node, combined)?;
    crate::agg_plain_finish(node, estate)
}

/// SORTED-arm twin (q28-sorted-arm): absorb a combined boundary partial into
/// the CURRENT group's pergroup states. The caller ran
/// [`crate::agg_sorted_stitch_begin`] (initval copies installed) and follows
/// with `agg_sorted_emit` — the plain arm's begin/absorb/finish discipline,
/// sorted flavor.
pub fn agg_sorted_absorb_partial(
    node: &mut AggStateData<'_>,
    combined: &RuntimePartial,
) -> PgResult<()> {
    debug_assert_eq!(node.plan.aggstrategy, crate::AGG_SORTED);
    absorb_partial_states(node, combined)
}

/// The shared absorb loop: write each transno's combined partial into the
/// node's once-allocated pergroup array (plain and sorted share
/// `pergroup_base` — one current-group array either way), byte-for-byte the
/// serial transfn chain's end state.
fn absorb_partial_states(
    node: &mut AggStateData<'_>,
    combined: &RuntimePartial,
) -> PgResult<()> {
    {
        let plan = crate::agg_lanefold_plan(node)
            .ok_or_else(|| PgError::error("runtime partial absorb without a fold plan".to_string()))?;
        if combined.trans.len() != plan.trans.len() {
            return Err(Box::new(PgError::error(
                "runtime partial: combined layout mismatch".to_string(),
            )));
        }
        let base = node.pergroup_base;
        let mut int128_fixups: Vec<(u16, i64, i128)> = Vec::new();
        for (t, &(transno, p)) in plan.trans.iter().zip(combined.trans.iter()) {
            debug_assert_eq!(t.transno, transno);
            // SAFETY: transno indexes the node's once-allocated pergroup
            // array; initialize_aggregates just rewrote it.
            let pg = unsafe { &mut *base.as_ptr().add(t.transno as usize) };
            match p {
                RuntimePartialTrans::Count(n) => {
                    pg.trans_value = Datum::from_i64(n);
                    pg.trans_value_is_null = false;
                    pg.no_trans_value = false;
                }
                RuntimePartialTrans::Sum { v, present } => {
                    if present {
                        pg.trans_value = Datum::from_i64(v);
                        pg.trans_value_is_null = false;
                        pg.no_trans_value = false;
                    }
                }
                RuntimePartialTrans::Fold { v, present } => {
                    if present {
                        pg.trans_value = Datum::from_i64(v);
                        pg.trans_value_is_null = false;
                        pg.no_trans_value = false;
                    }
                }
                RuntimePartialTrans::AvgAccum { n, sum } => {
                    if pg.trans_value_is_null {
                        return Err(Box::new(PgError::error(
                            "runtime partial: NULL avg transarray at absorb".to_string(),
                        )));
                    }
                    // SAFETY: aggcontext initval copy, shape validated.
                    unsafe {
                        let td = int8_transarray_elems(pg.trans_value)?;
                        td.write(n);
                        td.add(1).write(sum);
                    }
                    pg.no_trans_value = false;
                }
                RuntimePartialTrans::Int128 { n, sum, present } => {
                    if present {
                        int128_fixups.push((t.transno, n, sum));
                    }
                }
            }
        }
        // Int128 states allocate in the aggcontext (borrow of `node` above
        // ends first): the transfn's own first-call allocation shape.
        for (transno, n, sum) in int128_fixups {
            use ::adt_numeric::aggregates::Int128AggState;
            let aggcx = crate::agg_aggcontext(node);
            let layout = core::alloc::Layout::new::<Int128AggState>();
            let raw = ::mcx::Allocator::allocate(&aggcx, layout)
                .map_err(|_| aggcx.oom(layout.size()))?;
            let ptr = raw.cast::<Int128AggState>().as_ptr();
            // SAFETY: fresh allocation of the exact layout.
            unsafe {
                ptr.write(Int128AggState { calc_sum_x2: false, n, sum_x: sum, sum_x2: 0 });
            }
            let base = node.pergroup_base;
            // SAFETY: transno bound as above.
            let pg = unsafe { &mut *base.as_ptr().add(transno as usize) };
            pg.trans_value = Datum::from_usize(ptr as usize);
            pg.trans_value_is_null = false;
            pg.no_trans_value = false;
        }
    }
    Ok(())
}
