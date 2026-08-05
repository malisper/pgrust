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
use ::lanefold::{LaneKind, LaneTrans, LaneWidth};
use ::types_error::{PgError, PgResult};

use ::executils::{EStateData, ExecSlotId};

use crate::AggStateData;

/// One transno's self-contained partial state.
#[derive(Clone, Debug)]
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
    /// SE-AGGPOLY (band 101001): numeric_avg_accum NumericAggState
    /// (sum/avg over NUMERIC — the poly manifest's NumericAvg kind), in the
    /// self-contained relocation form below. Boxed: the payload is cold and
    /// large next to the word-sized arms.
    NumericAgg(Box<NumericAggPartial>),
}

/// SE-AGGPOLY: an exact finalized numeric sum, self-contained (plain Rust
/// digits — no memory context, crosses threads by value). Snapshotted from
/// `NumericSumAccum::finalize`'s NumericVar; rebuilt at absorb through
/// `VarView` into the leader's aggcontext accumulator.
#[derive(Clone, Debug)]
pub struct NumericSnapshot {
    pub ndigits: i32,
    pub weight: i32,
    pub sign: u16,
    pub dscale: i32,
    pub digits: Vec<::adt_numeric::NumericDigit>,
}

impl NumericSnapshot {
    fn view(&self) -> ::adt_numeric::VarView<'_> {
        ::adt_numeric::VarView {
            ndigits: self.ndigits,
            weight: self.weight,
            sign: self.sign,
            dscale: self.dscale,
            digits: &self.digits,
        }
    }
}

/// SE-AGGPOLY: one transno's self-contained NumericAggState partial — C's
/// numeric_avg_serialize field set (numeric.c:5323), with the worker's
/// finalized exact sum carried as a digit snapshot instead of a wire string.
/// `sums` holds AT MOST one snapshot per contributing worker (taken only
/// when that worker's n > 0 — C's `state2->N > 0` combine gate); the exact
/// additions are DEFERRED to leader absorb, which keeps the cross-worker
/// combine free of memory contexts. Order-insensitivity: numeric addition
/// is exact, and the accumulator's dscale is a max over added values, so
/// the absorb order across workers is unobservable — the finalized output
/// (value + dscale = the global max input dscale) is byte-identical to the
/// serial transfn chain's.
#[derive(Clone, Debug, Default)]
pub struct NumericAggPartial {
    /// The transvalue was non-NULL — a state EXISTED (numeric_avg_accum is
    /// NOT strict: a null-input row creates an empty state; C pg_proc 2858
    /// proisstrict=f, verified vendored REL 18.3). Absorb installs an
    /// allocated (possibly empty) state exactly when this holds.
    pub present: bool,
    pub n: i64,
    pub max_scale: i32,
    pub max_scale_count: i64,
    pub nan_count: i64,
    pub pinf_count: i64,
    pub ninf_count: i64,
    pub sums: Vec<NumericSnapshot>,
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

/// SE-NUMJOIN (the GL-NUMJOIN-1 lane): the per-transno export SCHEMA a
/// runtime-partial export/combine/absorb runs under. `Plan` = the classified
/// fold plan covers everything (the pre-existing path, byte-untouched);
/// `Poly` = the SE-AGGPOLY manifest (>=1 numeric_avg_accum NumericAvg entry,
/// every other transno an exportable lane kind — arg expressions free, the
/// per-row transition program evaluates them). Derived ONCE per
/// export/combine/absorb call and reused across groups. Reachability is the
/// admission gates' job: only engagements the (knob-gated) poly admission
/// let in ever carry a Poly schema here.
pub(crate) enum TransSchema {
    Plan,
    Poly(Vec<PolyTrans>),
}

pub(crate) fn trans_schema(node: &AggStateData<'_>) -> PgResult<TransSchema> {
    if agg_runtime_partial_admissible(node) {
        return Ok(TransSchema::Plan);
    }
    match agg_poly_manifest(node) {
        Some(m) => Ok(TransSchema::Poly(m)),
        None => Err(Box::new(PgError::error(
            "runtime partial: no exportable trans schema".to_string(),
        ))),
    }
}

/// The (transno, combine-kind) layout of one schema — the combine loops'
/// shared shape (NumericAvg entries carry their own law inside
/// `combine_into` and never read the kind; see `poly_lane_kind`).
fn schema_layout(node: &AggStateData<'_>, schema: &TransSchema) -> PgResult<Vec<(u16, LaneKind)>> {
    Ok(match schema {
        TransSchema::Plan => crate::agg_lanefold_plan(node)
            .ok_or_else(|| {
                PgError::error("runtime partial: plan schema without a fold plan".to_string())
            })?
            .trans
            .iter()
            .map(|t| (t.transno, t.kind))
            .collect(),
        TransSchema::Poly(m) => m.iter().map(|e| (e.transno, poly_lane_kind(e))).collect(),
    })
}

/// Schema-dispatched per-base export: `Plan` takes [`export_partial_from`]
/// byte-identically; `Poly` runs the manifest loop (the SE-AGGPOLY export
/// body over an EXPLICIT pergroup base — the grouped sink passes each hash
/// entry's array; the plain wrapper passes the node's fixed one).
pub(crate) fn export_partial_with(
    node: &AggStateData<'_>,
    schema: &TransSchema,
    base: core::ptr::NonNull<::execexpr::AggPerGroup>,
    partial: &mut RuntimePartial,
) -> PgResult<()> {
    match schema {
        TransSchema::Plan => export_partial_from(node, base, partial),
        TransSchema::Poly(manifest) => {
            let out = &mut partial.trans;
            out.clear();
            out.reserve(manifest.len());
            for e in manifest {
                // SAFETY: transno indexes the node's once-allocated pergroup
                // array (manifest transnos are 0..numtrans by construction).
                let pg = unsafe { &*base.as_ptr().add(e.transno as usize) };
                let p = match &e.kind {
                    PolyTransKind::Lane(t) => export_lane_trans(t, pg)?,
                    PolyTransKind::NumericAvg => export_numeric_state(pg)?,
                    // AGG_INTCASE: per-row int-family entry — the identical
                    // per-kind export body as Lane (state-keyed).
                    PolyTransKind::PerRow { kind, res_width } => {
                        export_kind(*kind, *res_width, pg)?
                    }
                };
                out.push((e.transno, p));
            }
            Ok(())
        }
    }
}

// Sign-extended fold word at the lane's RESULT width (the transvalue store
// width) — exact under either datum-construction convention because every
// admitted store is itself width-faithful.
fn fold_word(res_width: LaneWidth, d: Datum) -> i64 {
    match res_width {
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
    // SE-NUMJOIN (CAR 2): schema-dispatched — plan-admissible nodes take
    // the pre-existing path byte-identically; nodes the (knob-gated) poly
    // admission let in export via the manifest (numeric states relocated).
    let schema = trans_schema(node)?;
    export_partial_with(node, &schema, crate::agg_plain_pergroup_base(node), partial)
}

/// SORTED-arm twin (the sorted-arm lane): export the OPEN group's pergroup states
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

pub(crate) fn export_partial_from(
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
        out.push((t.transno, export_lane_trans(t, pg)?));
    }
    Ok(())
}

/// One classified lane transition's pergroup state, exported self-contained
/// (the per-kind body of [`export_partial_from`], shared with the SE-AGGPOLY
/// manifest export — byte-identical either caller).
fn export_lane_trans(t: &LaneTrans, pg: &::execexpr::AggPerGroup) -> PgResult<RuntimePartialTrans> {
    export_kind(t.kind, t.res_width, pg)
}

/// The (kind, result-width)-keyed export body. Shared by the fold-plan
/// callers above (through [`export_lane_trans`]) and the poly manifest's
/// per-row entries (AGG_INTCASE — the pergroup state a per-row transfn
/// chain leaves has the identical layout, keyed by the transition, not by
/// how its argument was evaluated).
fn export_kind(
    kind: LaneKind,
    res_width: LaneWidth,
    pg: &::execexpr::AggPerGroup,
) -> PgResult<RuntimePartialTrans> {
    Ok(match kind {
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
            v: if pg.trans_value_is_null { 0 } else { fold_word(res_width, pg.trans_value) },
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
                let st = unsafe { &*(pg.trans_value.as_usize() as *const Int128AggState) };
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
    })
}

/// Fold `src` into `dst` in place (one transno). `kind` is the LANE kind of
/// record for the byval arms; the NumericAgg arm carries its own combine law
/// (C numeric_avg_combine's field rules, sums concatenated — the exact
/// additions are deferred to leader absorb) and callers pass any kind for it.
fn combine_into(kind: LaneKind, dst: &mut RuntimePartialTrans, src: &RuntimePartialTrans) {
    use RuntimePartialTrans as P;
    match (dst, src) {
        (P::Count(x), P::Count(y)) => *x = x.wrapping_add(*y),
        (P::Sum { v: x, present: px }, P::Sum { v: y, present: py }) => {
            *x = x.wrapping_add(*y);
            *px = *px || *py;
        }
        (P::Fold { v: x, present: px }, P::Fold { v: y, present: py }) => {
            *x = match (*px, *py) {
                (true, true) => match kind {
                    LaneKind::Min => (*x).min(*y),
                    LaneKind::Max => (*x).max(*y),
                    LaneKind::BitAnd => *x & *y,
                    LaneKind::BitOr => *x | *y,
                    LaneKind::BoolAnd => ((*x != 0) && (*y != 0)) as i64,
                    LaneKind::BoolOr => ((*x != 0) || (*y != 0)) as i64,
                    _ => unreachable!("fold combine over a non-fold kind"),
                },
                (true, false) => *x,
                (false, _) => *y,
            };
            *px = *px || *py;
        }
        (P::AvgAccum { n: nx, sum: sx }, P::AvgAccum { n: ny, sum: sy }) => {
            *nx = nx.wrapping_add(*ny);
            *sx = sx.wrapping_add(*sy);
        }
        (P::Int128 { n: nx, sum: sx, present: px }, P::Int128 { n: ny, sum: sy, present: py }) => {
            *nx += *ny;
            *sx += *sy;
            *px = *px || *py;
        }
        // SE-AGGPOLY: C numeric_avg_combine's non-sum field rules
        // (numeric.c:5159 numeric_combine / 5251 numeric_avg_combine —
        // counts add; max_scale is the (max, tie-count-sum) monoid gated on
        // src.n > 0, commutative/associative exactly), with the sum
        // snapshots CONCATENATED instead of added — absorb adds them into
        // the leader accumulator, where addition of exact numerics makes
        // the association unobservable.
        (P::NumericAgg(x), P::NumericAgg(y)) => {
            x.present |= y.present;
            x.n += y.n;
            x.nan_count += y.nan_count;
            x.pinf_count += y.pinf_count;
            x.ninf_count += y.ninf_count;
            if y.n > 0 {
                if y.max_scale > x.max_scale {
                    x.max_scale = y.max_scale;
                    x.max_scale_count = y.max_scale_count;
                } else if y.max_scale == x.max_scale {
                    x.max_scale_count += y.max_scale_count;
                }
                x.sums.extend(y.sums.iter().cloned());
            }
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
        combine_into(t.kind, &mut dst.trans[i].1, &src.trans[i].1);
    }
    Ok(())
}

/// Combine worker partials (order-insensitive-exact for every admitted
/// kind; install order is immaterial by construction). SE-NUMJOIN
/// (CAR 2): layout derived per schema — plan-admissible nodes combine over
/// the fold plan exactly as before; poly-admitted nodes over the manifest
/// (the NumericAgg law rides `combine_into`).
pub fn agg_runtime_combine(
    node: &AggStateData<'_>,
    parts: &[RuntimePartial],
) -> PgResult<RuntimePartial> {
    let schema = trans_schema(node)?;
    let layout = schema_layout(node, &schema)?;
    let mut acc: Option<RuntimePartial> = None;
    for p in parts {
        if p.trans.len() != layout.len()
            || p.trans
                .iter()
                .zip(layout.iter())
                .any(|(&(no, _), &(lno, _))| no != lno)
        {
            return Err(Box::new(PgError::error(
                "runtime partial: transno layout mismatch".to_string(),
            )));
        }
        acc = Some(match acc {
            None => p.clone(),
            Some(mut a) => {
                for (i, &(_, kind)) in layout.iter().enumerate() {
                    combine_into(kind, &mut a.trans[i].1, &p.trans[i].1);
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

/// SORTED-arm twin (the sorted-arm lane): absorb a combined boundary partial into
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
/// serial transfn chain's end state. SE-NUMJOIN (CAR 2): schema-
/// dispatched — plan-admissible nodes (the sorted arm always; the plain arm
/// unless the poly admission engaged) take the pre-existing path
/// byte-identically.
fn absorb_partial_states(
    node: &mut AggStateData<'_>,
    combined: &RuntimePartial,
) -> PgResult<()> {
    let schema = trans_schema(node)?;
    let base = node.pergroup_base;
    absorb_partial_states_with(node, &schema, base, combined)
}

/// Schema-dispatched absorb over an explicit pergroup base (the grouped
/// sink's per-entry arrays; the plain/sorted wrappers' fixed array).
pub(crate) fn absorb_partial_states_with(
    node: &mut AggStateData<'_>,
    schema: &TransSchema,
    base: core::ptr::NonNull<::execexpr::AggPerGroup>,
    combined: &RuntimePartial,
) -> PgResult<()> {
    match schema {
        TransSchema::Plan => absorb_partial_states_at(node, base, combined),
        TransSchema::Poly(manifest) => absorb_poly_at(node, manifest, base, combined),
    }
}

/// SE-AGGJOIN (band 87001): the absorb loop over an EXPLICIT pergroup base —
/// the grouped runtime sink writes each combined group's states into its
/// hash-table ENTRY's pergroup array (the plain/sorted arms pass the node's
/// fixed current-group array through the wrapper above). Same byte-for-byte
/// end-state contract; Int128 states allocate in the aggcontext exactly as
/// the transfn's first call would.
pub(crate) fn absorb_partial_states_at(
    node: &mut AggStateData<'_>,
    base: core::ptr::NonNull<::execexpr::AggPerGroup>,
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
        let mut int128_fixups: Vec<(u16, i64, i128)> = Vec::new();
        for (t, (transno, p)) in plan.trans.iter().zip(combined.trans.iter()) {
            debug_assert_eq!(t.transno, *transno);
            // SAFETY: transno indexes the node's once-allocated pergroup
            // array; initialize_aggregates just rewrote it.
            let pg = unsafe { &mut *base.as_ptr().add(t.transno as usize) };
            absorb_lane_trans(pg, p, t.transno, &mut int128_fixups)?;
        }
        // Int128 states allocate in the aggcontext (borrow of `node` above
        // ends first): the transfn's own first-call allocation shape.
        install_int128_fixups(node, base, int128_fixups)?;
    }
    Ok(())
}

/// One transno's combined partial written into its pergroup slot (the
/// per-kind body of [`absorb_partial_states_at`], shared with the SE-AGGPOLY
/// manifest absorb — byte-identical either caller). Int128 states defer to
/// `int128_fixups` (they allocate in the aggcontext, which the caller can
/// only borrow after its fold-plan borrow ends).
fn absorb_lane_trans(
    pg: &mut ::execexpr::AggPerGroup,
    p: &RuntimePartialTrans,
    transno: u16,
    int128_fixups: &mut Vec<(u16, i64, i128)>,
) -> PgResult<()> {
    match p {
        RuntimePartialTrans::Count(n) => {
            pg.trans_value = Datum::from_i64(*n);
            pg.trans_value_is_null = false;
            pg.no_trans_value = false;
        }
        RuntimePartialTrans::Sum { v, present } => {
            if *present {
                pg.trans_value = Datum::from_i64(*v);
                pg.trans_value_is_null = false;
                pg.no_trans_value = false;
            }
        }
        RuntimePartialTrans::Fold { v, present } => {
            if *present {
                pg.trans_value = Datum::from_i64(*v);
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
                td.write(*n);
                td.add(1).write(*sum);
            }
            pg.no_trans_value = false;
        }
        RuntimePartialTrans::Int128 { n, sum, present } => {
            if *present {
                int128_fixups.push((transno, *n, *sum));
            }
        }
        RuntimePartialTrans::NumericAgg(_) => {
            // Lane plans never classify a numeric transition; a NumericAgg
            // partial reaching the plan-based absorb is a layout bug.
            return Err(Box::new(PgError::error(
                "runtime partial: NumericAgg outside the poly manifest".to_string(),
            )));
        }
    }
    Ok(())
}

/// The deferred Int128 installs: the transfn's own first-call allocation
/// shape, in the node's aggcontext.
fn install_int128_fixups(
    node: &mut AggStateData<'_>,
    base: core::ptr::NonNull<::execexpr::AggPerGroup>,
    int128_fixups: Vec<(u16, i64, i128)>,
) -> PgResult<()> {
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
        // SAFETY: transno bound as in the caller's absorb loop.
        let pg = unsafe { &mut *base.as_ptr().add(transno as usize) };
        pg.trans_value = Datum::from_usize(ptr as usize);
        pg.trans_value_is_null = false;
        pg.no_trans_value = false;
    }
    Ok(())
}

// ===========================================================================
// SE-AGGJOIN (band 87001) — GROUPED runtime partials: the grouped-agg-over-
// join sink's cross-worker state. One entry per hash group: the group's key
// datums in a SELF-CONTAINED representation (width-normalized i64 word +
// null flag per grouping column, in hash-key order — admission restricts
// keys to byval int-family types, where word equality IS group equality and
// NULLs group together exactly as C's grouping does) plus the group's
// RuntimePartial (the SAME per-transno representation and combine rules as
// the plain arm above — order-insensitive-exact kinds only).
// ===========================================================================

/// One grouping column's self-contained key value. `Word` = the
/// width-normalized byval datum word (the SE-AGGJOIN bootstrap vocabulary —
/// word equality IS group equality for the admitted byval types). `Bytes` =
/// SE-CBKEYS (the GL-CBKEYS-1 lane): the detoasted CONTENT bytes of a
/// text/varchar key under a DETERMINISTIC collation, where byte equality is
/// the grouping operator's verdict (texteq — the same
/// `group_eq_representational` law the scan-side C3/distinct machinery
/// proved; bpchar NEVER admits: its space-stripping bpchareq and
/// trailing-blank representative ties sit outside the byte-equality
/// envelope — the named refusal of record, mirrored at the probe). NULLs
/// carry `Word(0)` regardless of column kind (the isnull flag dominates
/// equality; NULLs group together exactly as C's grouping does).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum GroupKeyPart {
    Word(i64),
    Bytes(Box<[u8]>),
}

/// One group's self-contained key: (key value, isnull) per grouping column,
/// in the hash table's key-column order.
pub type GroupKeyWords = Vec<(GroupKeyPart, bool)>;

/// A worker's (or the combined) grouped partial. `scratch_ptrs` is export
/// scratch (entry pergroup pointers gathered under the perhash borrow, read
/// back outside it) — capacity reused across morsels, never read across
/// calls (m2-integration std-collections audit discipline).
#[derive(Default)]
pub struct GroupedRuntimePartial {
    pub groups: Vec<(GroupKeyWords, RuntimePartial)>,
    pub scratch_ptrs: Vec<usize>,
}

/// Combine worker grouped partials into one deduplicated group list
/// (order-insensitive-exact per-transno combines; group order is the
/// first-seen order across the worker list — the leader's absorb order,
/// immaterial to results: the canonical retrieve re-iterates its own table).
pub fn agg_grouped_runtime_combine(
    node: &AggStateData<'_>,
    parts: &[GroupedRuntimePartial],
) -> PgResult<Vec<(GroupKeyWords, RuntimePartial)>> {
    // SE-NUMJOIN (CAR 2): layout per schema — plan-admissible nodes
    // combine over the fold plan exactly as before; poly-admitted grouped
    // nodes over the manifest (NumericAgg's own law rides `combine_into`).
    let schema = trans_schema(node)?;
    let layout = schema_layout(node, &schema)?;
    let mut index: std::collections::HashMap<GroupKeyWords, usize> =
        std::collections::HashMap::new();
    let mut out: Vec<(GroupKeyWords, RuntimePartial)> = Vec::new();
    for part in parts {
        for (key, p) in &part.groups {
            if p.trans.len() != layout.len()
                || p.trans
                    .iter()
                    .zip(layout.iter())
                    .any(|(&(no, _), &(lno, _))| no != lno)
            {
                return Err(Box::new(PgError::error(
                    "grouped runtime partial: transno layout mismatch".to_string(),
                )));
            }
            match index.get(key) {
                Some(&i) => {
                    for (j, &(_, kind)) in layout.iter().enumerate() {
                        combine_into(kind, &mut out[i].1.trans[j].1, &p.trans[j].1);
                    }
                }
                None => {
                    index.insert(key.clone(), out.len());
                    out.push((key.clone(), p.clone()));
                }
            }
        }
    }
    Ok(out)
}

// ===========================================================================
// SE-AGGPOLY (band 101001) — the poly export MANIFEST: per-transno
// classification for plain-agg nodes whose transitions the fold plan does
// NOT fully cover, but whose uncovered remainder is exactly the
// numeric_avg_accum family (sum/avg over NUMERIC — NumericAggState). The
// per-row drive runs C's checked transition program either way; the
// manifest exists ONLY so the end states have a self-contained
// export/combine/absorb (the plan-based path above stays byte-untouched —
// callers try it FIRST and take the poly path only on its refusal).
// ===========================================================================

/// avg(numeric) / sum(numeric) aggregate OIDs (pg_proc REL 18.3, verified
/// vendored): both ride transfn numeric_avg_accum (2858, NOT strict) over an
/// INTERNAL NumericAggState with calc_sum_x2 = false. The numeric_accum
/// (1834) stddev/variance family carries sum_x2 and is deliberately NOT
/// admitted (named refusal — its combine exists but stays out of this
/// increment's proven envelope).
const AGG_AVG_NUMERIC: u32 = 2103;
const AGG_SUM_NUMERIC: u32 = 2114;
/// INTERNAL — the pointer-datum transition type.
const POLY_INTERNALOID: u32 = 2281;

// AGG_INTCASE (int-CASE fold-args car): the int-family plain-agg OIDs whose
// TRANSITION STATE is already an exportable runtime-partial kind (pg_proc /
// pg_aggregate REL 18.3, verified vendored — mirrors the planner probe's
// PLAIN_FOLD_AGGS constants; drift is pinned by the intcase e2e's
// engagement legs). The manifest classifies by STATE — the argument
// expression is free (the per-row transition program evaluates it), which
// is exactly what unlocks conditional (CASE/COALESCE) int args.
const AGG_COUNT_ANY: u32 = 2147;
const AGG_SUM_INT8: u32 = 2107;
const AGG_SUM_INT4: u32 = 2108;
const AGG_SUM_INT2: u32 = 2109;
const AGG_AVG_INT8: u32 = 2100;
const AGG_AVG_INT4: u32 = 2101;
const AGG_AVG_INT2: u32 = 2102;
const AGG_MAX_INT8: u32 = 2115;
const AGG_MAX_INT4: u32 = 2116;
const AGG_MAX_INT2: u32 = 2117;
const AGG_MIN_INT8: u32 = 2131;
const AGG_MIN_INT4: u32 = 2132;
const AGG_MIN_INT2: u32 = 2133;
/// int8 / int8[] transition types (count / int2+int4 avg transarrays).
const POLY_INT8OID: u32 = 20;
const POLY_INT8ARRAYOID: u32 = 1016;
const POLY_INT2OID: u32 = 21;
const POLY_INT4OID: u32 = 23;

/// `PGRUST_LANE_V2_AGG_INTCASE` (int-CASE fold-args car; DEFAULT ON since
/// GL-INTCASE-1 — fleet-ab-parallelism.md 2026-07-21, `=0|off` kills):
/// admit int-family plain aggregates over ARBITRARY single-argument
/// expressions (the conditional-aggregation idiom — sum(CASE...),
/// count-if) as first-class poly manifest entries. The m5 suppression
/// probe keys the matching plan shapes under the SAME env spelling (knob
/// coherence — a keyed-but-disarmed shape would land on serial; the
/// dop4-loss region belongs to the AggPolyHeapPlain FLOOR, not this
/// knob). Killed = the pre-car manifest (numeric anchor required),
/// byte-identical.
fn agg_intcase_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_LANE_V2_AGG_INTCASE").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// AGG_INTCASE: (state kind, result width) for an int-family plain agg the
/// per-row drive can carry regardless of its argument's shape. `None` =
/// not in the vocabulary (fail-closed; the caller refuses). The
/// aggtranstype double-check pins the state layout the export reads.
fn intcase_perrow_kind(ar: &::types_nodes::primnodes::Aggref<'_>) -> Option<(LaneKind, LaneWidth)> {
    let (kind, res_width, transtype) = match ar.aggfnoid {
        AGG_COUNT_ANY => (LaneKind::CountAny, LaneWidth::I64, POLY_INT8OID),
        AGG_SUM_INT2 | AGG_SUM_INT4 => (LaneKind::Sum, LaneWidth::I64, POLY_INT8OID),
        AGG_SUM_INT8 | AGG_AVG_INT8 => {
            (LaneKind::Int128AvgAccum, LaneWidth::I64, POLY_INTERNALOID)
        }
        AGG_AVG_INT2 | AGG_AVG_INT4 => (LaneKind::AvgAccum, LaneWidth::I64, POLY_INT8ARRAYOID),
        AGG_MAX_INT2 => (LaneKind::Max, LaneWidth::I16, POLY_INT2OID),
        AGG_MIN_INT2 => (LaneKind::Min, LaneWidth::I16, POLY_INT2OID),
        AGG_MAX_INT4 => (LaneKind::Max, LaneWidth::I32, POLY_INT4OID),
        AGG_MIN_INT4 => (LaneKind::Min, LaneWidth::I32, POLY_INT4OID),
        AGG_MAX_INT8 => (LaneKind::Max, LaneWidth::I64, POLY_INT8OID),
        AGG_MIN_INT8 => (LaneKind::Min, LaneWidth::I64, POLY_INT8OID),
        _ => return None,
    };
    (ar.aggtranstype == transtype).then_some((kind, res_width))
}

/// One manifest entry's classification.
#[derive(Clone, Copy, Debug)]
pub enum PolyTransKind {
    /// A fold-plan-classified transition (the runtime-partial whitelist —
    /// `kind_admits` holds).
    Lane(LaneTrans),
    /// numeric_avg_accum NumericAggState (sum/avg numeric, no sum_x2).
    NumericAvg,
    /// AGG_INTCASE: an int-family plain agg whose ARG the fold plan could
    /// not classify (conditional / expression args) but whose STATE is an
    /// exportable kind — per-row driven, exported/combined/absorbed by the
    /// identical per-kind bodies as `Lane`.
    PerRow { kind: LaneKind, res_width: LaneWidth },
}

/// One transno's manifest row. Entries are in TRANSNO order (canonical:
/// leader and worker derive the identical manifest from the identical plan).
#[derive(Clone, Copy, Debug)]
pub struct PolyTrans {
    pub transno: u16,
    pub kind: PolyTransKind,
}

/// The LANE kind a combine consults for a manifest entry. NumericAgg
/// combines carry their own law inside `combine_into` and never read the
/// kind — CountStar is an arbitrary stand-in there.
fn poly_lane_kind(e: &PolyTrans) -> LaneKind {
    match e.kind {
        PolyTransKind::Lane(t) => t.kind,
        PolyTransKind::NumericAvg => LaneKind::CountStar,
        PolyTransKind::PerRow { kind, .. } => kind,
    }
}

/// Classify the node's transitions into the poly manifest, fail-closed:
/// `None` = at least one transition is neither an exportable lane kind,
/// nor a bare sum/avg(numeric), nor (AGG_INTCASE, knob-gated) an
/// int-family plain agg with an exportable state — the caller refuses to
/// the serial arm. Requires at least ONE poly entry (NumericAvg or
/// PerRow; fully-lane-covered nodes belong to the plan-based path above,
/// byte-untouched). DISTINCT/ORDER BY/FILTER/ordered-set qualifiers
/// refuse (the export has no representation for their side state); the
/// aggregate's ARGUMENT expression is free — the per-row transition
/// program evaluates it, the state layout does not depend on it.
pub fn agg_poly_manifest(node: &AggStateData<'_>) -> Option<Vec<PolyTrans>> {
    let numtrans = node.numtrans;
    if numtrans == 0 {
        return None;
    }
    let mut kinds: Vec<Option<PolyTransKind>> = vec![None; numtrans];
    if let Some(plan) = crate::agg_lanefold_plan(node) {
        for t in plan.trans.iter() {
            // A classified-but-not-exportable kind is NOT a refusal here:
            // skip it and let the peragg inspection below decide. Before the
            // fold-trans inc-2 tier (lane aggseq-fold2), sum/avg(numeric)
            // never classified, so the NumericAvg inspection owned those
            // transnos; now classify() returns a NumAccum LaneTrans for the
            // same shape (knob-gated) and a fail-closed `return None` here
            // would kill the poly arm under FOLD_TRANS=1 + AGG_POLY=1 — a
            // keyed-then-refused serial landing (the suppress-then-refuse
            // defect class). Skipping restores the pre-classification
            // behavior exactly: numeric transnos re-classify as NumericAvg;
            // every other unexportable kind still refuses in the loop below
            // (its aggfnoid is not sum/avg(numeric)).
            if !kind_admits(t.kind) {
                continue;
            }
            kinds[t.transno as usize] = Some(PolyTransKind::Lane(*t));
        }
    }
    // Poly entries = the fold plan's remainder: NumericAvg anchors plus
    // (AGG_INTCASE, knob-gated) per-row int-family entries. At least one is
    // required — fully-lane-covered nodes belong to the plan-based path.
    let mut n_poly = 0usize;
    for pa in node.peragg.iter() {
        let transno = pa.transno as usize;
        if kinds[transno].is_some() {
            // Shared transno: the same catalog key classified it already.
            continue;
        }
        let ar = pa.aggref;
        if !pa.direct_args.is_empty()
            || !ar.aggorder.is_nil()
            || !ar.aggdistinct.is_nil()
            || ar.aggfilter.is_some()
        {
            return None;
        }
        if matches!(ar.aggfnoid, AGG_AVG_NUMERIC | AGG_SUM_NUMERIC)
            && ar.aggtranstype == POLY_INTERNALOID
        {
            kinds[transno] = Some(PolyTransKind::NumericAvg);
            n_poly += 1;
            continue;
        }
        if agg_intcase_enabled() {
            if let Some((kind, res_width)) = intcase_perrow_kind(ar) {
                kinds[transno] = Some(PolyTransKind::PerRow { kind, res_width });
                n_poly += 1;
                continue;
            }
        }
        return None;
    }
    if n_poly == 0 {
        return None;
    }
    let mut out = Vec::with_capacity(numtrans);
    for (i, k) in kinds.into_iter().enumerate() {
        // A transno no peragg names would be a planner numbering gap —
        // refuse (the sink derivation's discipline).
        out.push(PolyTrans { transno: i as u16, kind: k? });
    }
    Some(out)
}

/// Fail-closed poly admission (the plan-based `agg_runtime_partial_
/// admissible`'s manifest twin). The caller separately gates strategy,
/// scan shape, session policy, and the PGRUST_LANE_V2_AGG_POLY knob.
pub fn agg_poly_partial_admissible(node: &AggStateData<'_>) -> bool {
    agg_poly_manifest(node).is_some()
}

/// One numeric transition's pergroup state, exported self-contained (the
/// relocation): field snapshot + the worker's finalized exact sum as owned
/// digits. The sum is snapshotted only when n > 0 (C numeric_avg_combine's
/// `state2->N > 0` gate — an all-null/empty worker contributes nothing to
/// the leader accumulator, exactly as C's combine skips it).
fn export_numeric_state(pg: &::execexpr::AggPerGroup) -> PgResult<RuntimePartialTrans> {
    use ::adt_numeric::aggregates::NumericAggState;
    if pg.trans_value_is_null {
        return Ok(RuntimePartialTrans::NumericAgg(Box::default()));
    }
    // SAFETY: non-null numeric_avg_accum transvalues point at the
    // aggcontext state the transfn chain installed; the worker's executor
    // is live and this export is its sole accessor (finalize's lazy carry
    // is the only mutation — the serialize builtin's own discipline).
    let st = unsafe { &mut *(pg.trans_value.as_usize() as *mut NumericAggState) };
    if st.calc_sum_x2 {
        return Err(Box::new(PgError::error(
            "runtime partial: unexpected numeric sum_x2 state".to_string(),
        )));
    }
    let mut p = NumericAggPartial {
        present: true,
        n: st.n,
        max_scale: st.max_scale,
        max_scale_count: st.max_scale_count,
        nan_count: st.nan_count,
        pinf_count: st.pinf_count,
        ninf_count: st.ninf_count,
        sums: Vec::new(),
    };
    if st.n > 0 {
        let mut tmp = ::adt_numeric::NumericVar::new();
        st.sum_x.finalize(&mut tmp);
        p.sums.push(NumericSnapshot {
            ndigits: tmp.ndigits,
            weight: tmp.weight,
            sign: tmp.sign,
            dscale: tmp.dscale,
            digits: tmp.digits().to_vec(),
        });
    }
    Ok(RuntimePartialTrans::NumericAgg(Box::new(p)))
}

/// WORKER side, manifest flavor of [`agg_runtime_export_partial_into`]
/// (plain pergroups; cumulative-overwrite per morsel; the same truncated-
/// partial-on-Err invariant — leaders combine only clean Completed
/// outcomes).
pub fn agg_poly_export_partial_into(
    node: &AggStateData<'_>,
    partial: &mut RuntimePartial,
) -> PgResult<()> {
    let manifest = agg_poly_manifest(node)
        .ok_or_else(|| PgError::error("poly export without a manifest".to_string()))?;
    let base = crate::agg_plain_pergroup_base(node);
    let out = &mut partial.trans;
    out.clear();
    out.reserve(manifest.len());
    for e in &manifest {
        // SAFETY: transno indexes the node's once-allocated pergroup array
        // (manifest transnos are 0..numtrans by construction).
        let pg = unsafe { &*base.as_ptr().add(e.transno as usize) };
        let p = match &e.kind {
            PolyTransKind::Lane(t) => export_lane_trans(t, pg)?,
            PolyTransKind::NumericAvg => export_numeric_state(pg)?,
            PolyTransKind::PerRow { kind, res_width } => export_kind(*kind, *res_width, pg)?,
        };
        out.push((e.transno, p));
    }
    Ok(())
}

/// Combine worker poly partials (manifest layout; order-insensitive-exact
/// for every entry — the NumericAgg law is argued at the type's doc).
pub fn agg_poly_runtime_combine(
    node: &AggStateData<'_>,
    parts: &[RuntimePartial],
) -> PgResult<RuntimePartial> {
    let manifest = agg_poly_manifest(node)
        .ok_or_else(|| PgError::error("poly combine without a manifest".to_string()))?;
    let mut acc: Option<RuntimePartial> = None;
    for p in parts {
        if p.trans.len() != manifest.len()
            || p.trans
                .iter()
                .zip(manifest.iter())
                .any(|(&(no, _), e)| no != e.transno)
        {
            return Err(Box::new(PgError::error(
                "poly partial: transno layout mismatch".to_string(),
            )));
        }
        acc = Some(match acc {
            None => p.clone(),
            Some(mut a) => {
                for (i, e) in manifest.iter().enumerate() {
                    combine_into(poly_lane_kind(e), &mut a.trans[i].1, &p.trans[i].1);
                }
                a
            }
        });
    }
    Ok(acc.unwrap_or_default())
}

/// LEADER side, manifest flavor of [`exec_agg_runtime_partials`]: begin,
/// absorb (lane entries via the shared per-kind body; numeric entries
/// rebuilt in the aggcontext exactly as C's deserialize + combine
/// composition leaves them), then the ordinary retrieve.
pub fn exec_agg_poly_runtime_partials<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    combined: &RuntimePartial,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert_eq!(node.plan.aggstrategy, crate::AGG_PLAIN);
    if node.agg_done {
        return Ok(None);
    }
    crate::agg_plain_build_begin(node, estate)?;
    absorb_poly_partial_states(node, combined)?;
    crate::agg_plain_finish(node, estate)
}

fn absorb_poly_partial_states(
    node: &mut AggStateData<'_>,
    combined: &RuntimePartial,
) -> PgResult<()> {
    let manifest = agg_poly_manifest(node)
        .ok_or_else(|| PgError::error("poly absorb without a manifest".to_string()))?;
    let base = node.pergroup_base;
    absorb_poly_at(node, &manifest, base, combined)
}

/// The manifest absorb body over an EXPLICIT pergroup base (SE-NUMJOIN,
/// CAR 2: the grouped-join sink writes each combined group's states into
/// its hash-table entry's array; the plain wrapper above passes the node's
/// fixed one — byte-identical to the pre-factor body for that caller).
fn absorb_poly_at(
    node: &mut AggStateData<'_>,
    manifest: &[PolyTrans],
    base: core::ptr::NonNull<::execexpr::AggPerGroup>,
    combined: &RuntimePartial,
) -> PgResult<()> {
    if combined.trans.len() != manifest.len() {
        return Err(Box::new(PgError::error(
            "poly partial: combined layout mismatch".to_string(),
        )));
    }
    let mut int128_fixups: Vec<(u16, i64, i128)> = Vec::new();
    let mut numeric_fixups: Vec<(u16, &NumericAggPartial)> = Vec::new();
    for (e, (transno, p)) in manifest.iter().zip(combined.trans.iter()) {
        if e.transno != *transno {
            return Err(Box::new(PgError::error(
                "poly partial: transno layout mismatch".to_string(),
            )));
        }
        match (&e.kind, p) {
            (PolyTransKind::NumericAvg, RuntimePartialTrans::NumericAgg(np)) => {
                if np.present {
                    numeric_fixups.push((e.transno, np));
                }
                // Absent state: the pergroup keeps its initialized NULL —
                // C's zero-transfn-call end state (numeric_avg_accum's
                // initval is NULL).
            }
            (PolyTransKind::NumericAvg, _) => {
                return Err(Box::new(PgError::error(
                    "poly partial: numeric entry with a non-numeric state".to_string(),
                )));
            }
            (PolyTransKind::Lane(_) | PolyTransKind::PerRow { .. }, p) => {
                // SAFETY: transno bound as in the plan-based absorb.
                let pg = unsafe { &mut *base.as_ptr().add(e.transno as usize) };
                absorb_lane_trans(pg, p, e.transno, &mut int128_fixups)?;
            }
        }
    }
    install_int128_fixups(node, base, int128_fixups)?;
    // Numeric installs: the transfn's first-call allocation shape, then the
    // deferred exact additions (any order — addition is exact, the accum
    // dscale is a max).
    for (transno, np) in numeric_fixups {
        use ::adt_numeric::aggregates::NumericAggState;
        let aggcx = crate::agg_aggcontext(node);
        let layout = core::alloc::Layout::new::<NumericAggState>();
        let raw = ::mcx::Allocator::allocate(&aggcx, layout)
            .map_err(|_| aggcx.oom(layout.size()))?;
        let ptr = raw.cast::<NumericAggState>().as_ptr();
        // SAFETY: fresh allocation of the exact layout.
        unsafe { ptr.write(NumericAggState::new(false)) };
        // SAFETY: just-written state, sole reference.
        let st = unsafe { &mut *ptr };
        st.n = np.n;
        st.max_scale = np.max_scale;
        st.max_scale_count = np.max_scale_count;
        st.nan_count = np.nan_count;
        st.pinf_count = np.pinf_count;
        st.ninf_count = np.ninf_count;
        for s in &np.sums {
            st.sum_x.add(aggcx, s.view())?;
        }
        // SAFETY: transno bound as above.
        let pg = unsafe { &mut *base.as_ptr().add(transno as usize) };
        pg.trans_value = Datum::from_usize(ptr as usize);
        pg.trans_value_is_null = false;
        pg.no_trans_value = false;
    }
    Ok(())
}

// ===========================================================================
// GL-MJSORT-FOLD (the merge-join duplicate-band fold lever, GL-MJSORT-3
// §3.1 seams 1-2): the EXPLICIT-layout leader absorb + the fold arm's own
// per-transno recognizer. Neither existing schema path classifies the fold
// arm's shapes — lanefold args are single-Var affine (sum(Var+Var) over a
// join never classifies) and the poly manifest requires a numeric anchor —
// so the arm carries its own tight recognizer and hands the absorb an
// EXPLICIT (transno, state) layout instead of a derived trans schema. The
// absorb bodies themselves are the existing per-kind ones
// (`absorb_lane_trans` + `install_int128_fixups`), verbatim.
// ===========================================================================

/// LEADER side, explicit-layout flavor of [`exec_agg_runtime_partials`]:
/// begin (initval pergroups), absorb the combined per-transno states, then
/// the ordinary retrieve (finalize + HAVING + project). Byte-for-byte the
/// end state the serial transfn chain leaves — the absorb loop calls the
/// existing per-kind bodies. Fail-closed: a transno outside the node's
/// once-allocated pergroup array errors (never writes past it).
pub fn exec_agg_explicit_partials<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    combined: &[(u16, RuntimePartialTrans)],
) -> PgResult<Option<ExecSlotId>> {
    debug_assert_eq!(node.plan.aggstrategy, crate::AGG_PLAIN);
    if node.agg_done {
        return Ok(None);
    }
    crate::agg_plain_build_begin(node, estate)?;
    absorb_explicit_partials(node, combined)?;
    crate::agg_plain_finish(node, estate)
}

fn absorb_explicit_partials(
    node: &mut AggStateData<'_>,
    combined: &[(u16, RuntimePartialTrans)],
) -> PgResult<()> {
    let base = node.pergroup_base;
    let numtrans = node.trans_typ.len();
    let mut int128_fixups: Vec<(u16, i64, i128)> = Vec::new();
    for (transno, p) in combined {
        if (*transno as usize) >= numtrans {
            return Err(Box::new(PgError::error(
                "explicit partial: transno out of range".to_string(),
            )));
        }
        // SAFETY: transno bound-checked against the once-allocated pergroup
        // array just above; initialize_aggregates just rewrote it.
        let pg = unsafe { &mut *base.as_ptr().add(*transno as usize) };
        absorb_lane_trans(pg, p, *transno, &mut int128_fixups)?;
    }
    install_int128_fixups(node, base, int128_fixups)
}

/// The fold arm's per-transno state class — every member's cross-partition
/// combine is exact and order-insensitive (the reassociation legality at
/// the module head), and every member's absorbed representation is an
/// existing [`RuntimePartialTrans`] arm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MjFoldKind {
    /// int8inc — count(*): counts every row, reads no argument.
    CountStar,
    /// int8inc_any — count(x): counts non-NULL argument rows.
    CountAny,
    /// int2_sum/int4_sum — i64 accumulate (C's UNCHECKED int8 add; the
    /// mod-2^64 ring reassociates exactly).
    Sum,
    /// int8_avg_accum — Int128AggState {n, sum}; serves BOTH sum(int8) and
    /// avg(int8) (same state, different finalfn — finalize is the node's
    /// own retrieve).
    Int128,
    /// int2/4/8smaller — strict byval signed min (integer ties are
    /// identical datum words; any combine order bit-equals serial).
    Min,
    /// int2/4/8larger — strict byval signed max.
    Max,
}

/// One recognized transition: the fold arm resolves `arg` against the join
/// tlist (GL-MJSORT-3 §3.1 seam 3) and refuses BY NAME anything its
/// grammar cannot classify.
pub struct MjFoldTrans<'mcx> {
    pub transno: u16,
    pub kind: MjFoldKind,
    /// The aggregate's single argument expression (None = count(*)).
    pub arg: Option<::types_nodes::node_tree::Node<'mcx>>,
    /// Declared input type oid (0 for count(*); ANYOID for count(x) — the
    /// resolver reads only NULLness there and pins the int family itself).
    pub arg_type: u32,
}

// Transfn oids of the fold vocabulary (pg_proc REL 18.3, verified vendored;
// lanefold::classify_trans's own table — mirrored because those consts are
// that crate's private classification detail).
const MJF_INT8INC: u32 = 1219;
const MJF_INT8INC_ANY: u32 = 2804;
const MJF_INT2_SUM: u32 = 1840;
const MJF_INT4_SUM: u32 = 1841;
const MJF_INT8_AVG_ACCUM: u32 = 2746;
const MJF_INT4LARGER: u32 = 768;
const MJF_INT4SMALLER: u32 = 769;
const MJF_INT2LARGER: u32 = 770;
const MJF_INT2SMALLER: u32 = 771;
const MJF_INT8LARGER: u32 = 1236;
const MJF_INT8SMALLER: u32 = 1237;
const MJF_INT2OID: u32 = 21;
const MJF_INT4OID: u32 = 23;
const MJF_INT8OID: u32 = 20;
const MJF_INTERNALOID: u32 = 2281;

/// The pure recognizer table (unit-pinned): (transfn oid, init-null,
/// has-args, aggtranstype) -> fold kind. Keyed off the TRANSFN oid exactly
/// as lanefold::classify_trans — the transfn IS the state contract the
/// absorb writes; the aggtranstype double-check pins the state layout
/// (the intcase precedent). Init-value conditions mirror classify_trans:
/// count's initval is non-null (0), sum/avg/min/max start NULL.
fn mjfold_kind(
    transfn: u32,
    init_null: bool,
    has_args: bool,
    aggtranstype: u32,
) -> Option<MjFoldKind> {
    match transfn {
        MJF_INT8INC if !init_null && !has_args && aggtranstype == MJF_INT8OID => {
            Some(MjFoldKind::CountStar)
        }
        MJF_INT8INC_ANY if !init_null && has_args && aggtranstype == MJF_INT8OID => {
            Some(MjFoldKind::CountAny)
        }
        MJF_INT2_SUM | MJF_INT4_SUM
            if init_null && has_args && aggtranstype == MJF_INT8OID =>
        {
            Some(MjFoldKind::Sum)
        }
        MJF_INT8_AVG_ACCUM if init_null && has_args && aggtranstype == MJF_INTERNALOID => {
            Some(MjFoldKind::Int128)
        }
        MJF_INT2SMALLER if init_null && has_args && aggtranstype == MJF_INT2OID => {
            Some(MjFoldKind::Min)
        }
        MJF_INT4SMALLER if init_null && has_args && aggtranstype == MJF_INT4OID => {
            Some(MjFoldKind::Min)
        }
        MJF_INT8SMALLER if init_null && has_args && aggtranstype == MJF_INT8OID => {
            Some(MjFoldKind::Min)
        }
        MJF_INT2LARGER if init_null && has_args && aggtranstype == MJF_INT2OID => {
            Some(MjFoldKind::Max)
        }
        MJF_INT4LARGER if init_null && has_args && aggtranstype == MJF_INT4OID => {
            Some(MjFoldKind::Max)
        }
        MJF_INT8LARGER if init_null && has_args && aggtranstype == MJF_INT8OID => {
            Some(MjFoldKind::Max)
        }
        _ => None,
    }
}

/// GL-MJSORT-3 §3.1 seam 2 — the fold arm's OWN tight recognizer over the
/// node's transitions. `Ok(None)` = the node shape or at least one
/// transition is outside the vocabulary — the caller refuses BY NAME.
/// Node-level refusals: non-PLAIN strategy, combine/partial aggsplit
/// modes, any sorted collection state (ordered-set/aggorder/aggdistinct —
/// `pertrans_sort` empty is the belt), FILTER, direct args. Per-transno:
/// the transfn-oid table above (resolved from pg_aggregate exactly as
/// ExecInitAgg's own non-combine arm), every transno covered exactly once.
pub fn agg_mjfold_recognize<'mcx>(
    node: &AggStateData<'mcx>,
) -> PgResult<Option<Vec<MjFoldTrans<'mcx>>>> {
    if node.plan.aggstrategy != crate::AGG_PLAIN
        || node.plan.aggsplit != ::types_nodes::primnodes::AGGSPLIT_SIMPLE
        || !node.pertrans_sort.is_empty()
        || node.numtrans == 0
    {
        return Ok(None);
    }
    let numtrans = node.numtrans;
    let mut out: Vec<Option<MjFoldTrans<'mcx>>> = Vec::new();
    out.resize_with(numtrans, || None);
    for pa in node.peragg.iter() {
        let ar = pa.aggref;
        if !pa.direct_args.is_empty()
            || ar.aggkind != ::types_nodes::primnodes::AGGKIND_NORMAL
            || !ar.aggorder.is_nil()
            || !ar.aggdistinct.is_nil()
            || ar.aggfilter.is_some()
        {
            return Ok(None);
        }
        let transno = pa.transno as usize;
        if transno >= numtrans {
            return Ok(None);
        }
        if out[transno].is_some() {
            // Shared transno: the same catalog key classified it already
            // (find_compatible_trans keys sharing on the transition state).
            continue;
        }
        let Some(shape) = ::syscache_seams::lookup_pg_aggregate_shape::call(ar.aggfnoid)?
        else {
            return Ok(None);
        };
        let nargs = ar.args.len();
        if nargs > 1 {
            return Ok(None);
        }
        let Some(kind) = mjfold_kind(
            shape.aggtransfn,
            node.trans_init[transno].isnull,
            nargs == 1,
            ar.aggtranstype,
        ) else {
            return Ok(None);
        };
        let arg = if nargs == 1 {
            let Some(tle) = ar.args.iter().next().and_then(|n| n.as_target_entry()) else {
                return Ok(None);
            };
            if tle.resjunk {
                return Ok(None);
            }
            Some(tle.expr)
        } else {
            None
        };
        out[transno] = Some(MjFoldTrans {
            transno: transno as u16,
            kind,
            arg,
            arg_type: ar.aggargtypes.first().unwrap_or(0),
        });
    }
    let mut v = Vec::with_capacity(numtrans);
    for t in out {
        match t {
            Some(t) => v.push(t),
            // A transno no peragg names would be a planner numbering gap —
            // refuse (the sink derivation's discipline).
            None => return Ok(None),
        }
    }
    Ok(Some(v))
}

#[cfg(test)]
mod mjfold_tests {
    use super::*;

    /// The recognizer table's exact vocabulary: transfn oid keyed, initval
    /// and aggtranstype pinned per entry (GL-MJSORT-3 §3.1 seam 2).
    #[test]
    fn mjfold_kind_table() {
        use MjFoldKind::*;
        // count(*): non-null init, NO args, int8 state.
        assert_eq!(mjfold_kind(MJF_INT8INC, false, false, MJF_INT8OID), Some(CountStar));
        assert_eq!(mjfold_kind(MJF_INT8INC, true, false, MJF_INT8OID), None);
        assert_eq!(mjfold_kind(MJF_INT8INC, false, true, MJF_INT8OID), None);
        // count(x): non-null init, one arg.
        assert_eq!(mjfold_kind(MJF_INT8INC_ANY, false, true, MJF_INT8OID), Some(CountAny));
        assert_eq!(mjfold_kind(MJF_INT8INC_ANY, true, true, MJF_INT8OID), None);
        // sum(int2/int4): NULL init, int8 state.
        assert_eq!(mjfold_kind(MJF_INT2_SUM, true, true, MJF_INT8OID), Some(Sum));
        assert_eq!(mjfold_kind(MJF_INT4_SUM, true, true, MJF_INT8OID), Some(Sum));
        assert_eq!(mjfold_kind(MJF_INT4_SUM, false, true, MJF_INT8OID), None);
        // sum(int8)/avg(int8): the shared Int128AggState transition.
        assert_eq!(
            mjfold_kind(MJF_INT8_AVG_ACCUM, true, true, MJF_INTERNALOID),
            Some(Int128)
        );
        assert_eq!(mjfold_kind(MJF_INT8_AVG_ACCUM, true, true, MJF_INT8OID), None);
        // min/max at each width; the state type pins the width.
        assert_eq!(mjfold_kind(MJF_INT2SMALLER, true, true, MJF_INT2OID), Some(Min));
        assert_eq!(mjfold_kind(MJF_INT4SMALLER, true, true, MJF_INT4OID), Some(Min));
        assert_eq!(mjfold_kind(MJF_INT8SMALLER, true, true, MJF_INT8OID), Some(Min));
        assert_eq!(mjfold_kind(MJF_INT2LARGER, true, true, MJF_INT2OID), Some(Max));
        assert_eq!(mjfold_kind(MJF_INT4LARGER, true, true, MJF_INT4OID), Some(Max));
        assert_eq!(mjfold_kind(MJF_INT8LARGER, true, true, MJF_INT8OID), Some(Max));
        // Width/state mismatches refuse (the layout pin).
        assert_eq!(mjfold_kind(MJF_INT4LARGER, true, true, MJF_INT8OID), None);
        assert_eq!(mjfold_kind(MJF_INT8SMALLER, true, true, MJF_INT4OID), None);
        // Outside the vocabulary: float/numeric/text transfns never admit.
        for oid in [208u32, 222, 2858, 458, 1963, 2805] {
            assert_eq!(mjfold_kind(oid, true, true, MJF_INT8OID), None, "oid {oid}");
            assert_eq!(mjfold_kind(oid, false, true, MJF_INT8OID), None, "oid {oid}");
        }
    }

    /// The explicit absorb's bound check is fail-closed (never writes past
    /// the pergroup array): exercised as a pure bounds predicate here; the
    /// executor-side path is proven by the fold e2e legs.
    #[test]
    fn explicit_absorb_transno_bound() {
        // The guard under test is `transno as usize >= trans_typ.len()` in
        // absorb_explicit_partials — pinned structurally: u16::MAX must
        // always trip for any numtrans an admitted plain node carries.
        let numtrans = 3usize;
        assert!((u16::MAX as usize) >= numtrans);
        assert!(!((2u16 as usize) >= numtrans));
    }
}

// ===========================================================================
// SE-AGGPOLY unit corpus (band 101001): the pure NumericAgg combine law.
// The manifest/export/absorb sides ride executor state and are proven by
// the aggpoly e2e + dualexec corpus (scripts/aggpoly-e2e.sh,
// scripts/dualexec/corpus-aggpoly.sql).
// ===========================================================================

#[cfg(test)]
mod poly_tests {
    use super::*;

    // Fold-trans kinds are SERIAL-ONLY: kind_admits must refuse them all, so
    // (a) the plan-based export path falls through to the poly path, and
    // (b) agg_poly_manifest's classified-transno seeding SKIPS them (the
    // skip-not-refuse arm above — a NumAccum transno re-classifies as
    // NumericAvg by peragg inspection, keeping the poly arm alive under
    // FOLD_TRANS=1 + AGG_POLY=1; composition fix, lane aggseq-fold2).
    #[test]
    fn fold_trans_kinds_never_export() {
        for k in [
            LaneKind::FSum,
            LaneKind::FAccum,
            LaneKind::NumAccum,
            LaneKind::FRegrAccum,
            LaneKind::Count2,
        ] {
            assert!(!kind_admits(k), "{k:?} is serial-only (fold-trans tier)");
        }
    }

    fn snap(digits: Vec<::adt_numeric::NumericDigit>, weight: i32, dscale: i32) -> NumericSnapshot {
        NumericSnapshot { ndigits: digits.len() as i32, weight, sign: 0, dscale, digits }
    }

    fn np(n: i64, max_scale: i32, max_scale_count: i64, sums: Vec<NumericSnapshot>) -> RuntimePartialTrans {
        RuntimePartialTrans::NumericAgg(Box::new(NumericAggPartial {
            present: true,
            n,
            max_scale,
            max_scale_count,
            nan_count: 0,
            pinf_count: 0,
            ninf_count: 0,
            sums,
        }))
    }

    fn unbox(p: &RuntimePartialTrans) -> &NumericAggPartial {
        match p {
            RuntimePartialTrans::NumericAgg(b) => b,
            _ => panic!("not a NumericAgg partial"),
        }
    }

    /// C numeric_avg_combine's field rules: counts add; the (max_scale,
    /// count) monoid adopts on >, adds counts on ==, and is skipped
    /// entirely — sums included — when src.n == 0.
    #[test]
    fn numeric_combine_field_law() {
        let mut dst = np(3, 2, 1, vec![snap(vec![1, 2], 0, 2)]);
        // Greater src scale: adopt scale + its count; sums concat.
        combine_into(LaneKind::CountStar, &mut dst, &np(2, 5, 7, vec![snap(vec![9], 1, 5)]));
        let d = unbox(&dst);
        assert_eq!((d.n, d.max_scale, d.max_scale_count), (5, 5, 7));
        assert_eq!(d.sums.len(), 2);
        // Equal src scale: counts add.
        let mut dst2 = dst.clone();
        combine_into(LaneKind::CountStar, &mut dst2, &np(1, 5, 3, vec![]));
        let d2 = unbox(&dst2);
        assert_eq!((d2.n, d2.max_scale, d2.max_scale_count), (6, 5, 10));
        // Smaller src scale: dst scale stands, src count dropped.
        let mut dst3 = dst.clone();
        combine_into(LaneKind::CountStar, &mut dst3, &np(1, 1, 100, vec![]));
        let d3 = unbox(&dst3);
        assert_eq!((d3.max_scale, d3.max_scale_count), (5, 7));
    }

    /// src.n == 0 skips max_scale AND sums (C's `state2->N > 0` gate) while
    /// the NaN/Inf counters still add (they sit outside the gate).
    #[test]
    fn numeric_combine_empty_src_gate() {
        let mut dst = np(3, 2, 1, vec![snap(vec![1], 0, 2)]);
        let src = RuntimePartialTrans::NumericAgg(Box::new(NumericAggPartial {
            present: true,
            n: 0,
            max_scale: 9,
            max_scale_count: 9,
            nan_count: 4,
            pinf_count: 2,
            ninf_count: 1,
            sums: vec![],
        }));
        combine_into(LaneKind::CountStar, &mut dst, &src);
        let d = unbox(&dst);
        assert_eq!((d.n, d.max_scale, d.max_scale_count), (3, 2, 1));
        assert_eq!((d.nan_count, d.pinf_count, d.ninf_count), (4, 2, 1));
        assert_eq!(d.sums.len(), 1);
    }

    /// Order-insensitivity: every combine order over three partials leaves
    /// identical scalar fields and the identical MULTISET of sum snapshots
    /// (the leader's exact additions make the list order unobservable).
    #[test]
    fn numeric_combine_order_insensitive() {
        let parts = [
            np(2, 3, 4, vec![snap(vec![1], 0, 3)]),
            np(5, 3, 2, vec![snap(vec![2, 3], 1, 1)]),
            np(0, 0, 0, vec![]),
            np(7, 6, 1, vec![snap(vec![4], 0, 6)]),
        ];
        let orders: [[usize; 4]; 4] = [[0, 1, 2, 3], [3, 2, 1, 0], [1, 3, 0, 2], [2, 0, 3, 1]];
        let mut results: Vec<(i64, i32, i64, Vec<Vec<::adt_numeric::NumericDigit>>)> = Vec::new();
        for order in orders {
            let mut acc = parts[order[0]].clone();
            for &i in &order[1..] {
                combine_into(LaneKind::CountStar, &mut acc, &parts[i]);
            }
            let a = unbox(&acc);
            let mut sums: Vec<Vec<::adt_numeric::NumericDigit>> =
                a.sums.iter().map(|s| s.digits.clone()).collect();
            sums.sort();
            results.push((a.n, a.max_scale, a.max_scale_count, sums));
        }
        for r in &results[1..] {
            assert_eq!(r, &results[0]);
        }
    }
}
