//! Expression group keys (expr-key tranche): host `Agg(hashed) → SeqScan`
//! builds whose scan PROJECTS the (single) grouping key as a computed
//! expression — the shape `decide_agg_lane`'s `ps_ProjInfo.is_none()` gate
//! refused to the per-row breaker feed until now.
//!
//! Two admission classes, one feed:
//!
//! * **Int-expression keys** (the ts-extract grouped-agg class): the key is int2/4/8
//!   arithmetic over scan Vars/Consts — the stitcher census
//!   (`ScanProjCols`, the projstitch vocabulary). The key lane is computed
//!   per staged batch by the lanestitch REFERENCE INTERPRETER
//!   (`eval_project` — the parity oracle projstitch replays on) into a
//!   scratch lane, then fed to the existing K2/compact single-key probe.
//!   Trap discipline is projstitch's refuse-and-replay verbatim: an erroring
//!   key (overflow / division by zero) discards the batch's computed lane
//!   before ANY probe/transition ran and replays the WHOLE batch through the
//!   per-row emit path — the C-ported `exec_project` raises C's exact error
//!   on C's row — then refuses STICKY (all later batches per-row).
//!
//! * **Dict-expression keys** (the regexp-over-long-text class): the key is a strict fmgr chain
//!   over ONE dict-coded pgrcolumnar text column (`ScanProjExprKey` census →
//!   `laneexec::dicteval`, IMMUTABLE internal-language builtins only —
//!   volatile/stable/SQL-language functions refuse there). The dict-memo
//!   principle applied to the KEY: the chain runs through the REAL fmgr once
//!   per distinct dictionary code per epoch (k calls, not n), and grouping
//!   rides the dictgroup pattern — a per-epoch code→pergroup map resolves
//!   each unseen code once through the same staged-probe leg the K2 path
//!   uses (first-arrival insertion order, entry init, spill decisions all
//!   identical). Raw (non-dict) windows evaluate per selected row through
//!   the same fmgr — the per-row path's exact call count. Errors raise from
//!   the lazy memo fill at exactly the first selected row of the erroring
//!   code — the row the per-row projection would have raised on.
//!
//! Coordinate change: with a projection, the agg's input space is the
//! PROJECTED tlist, not the scan tuple. Admission therefore requires every
//! transition/spill column to be a bare-Var tlist entry (mapped to its base
//! scan lane — `MapCols`); the computed key column is fed from the derived
//! lane. Residual (classify-refused) transitions ARE hosted — unlike K2 —
//! because the projected row can be rebuilt from the staged lanes plus the
//! derived key (`fill_stage_slot`), so the resid program never needs the
//! per-row projection: this is what lets that class's `avg(length(Referer))` leg
//! ride along while the regexp key stays once-per-code.
//!
//! Everything outside the vocabulary refuses (`RefuseReason::ExprKeyShape`)
//! to the per-row breaker feed, byte-identically. Kill switch:
//! `PGRUST_LANE_V2_EXPRKEY=0|off`.

use std::sync::OnceLock;

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

use super::{
    agg_fold_staged, agg_fold_staged_mm, collect_mm_codes, mm_str_cols, stats, trace_feed,
    CodesCols, RefuseReason, ShapeClass,
};

/// Kill switch (default ON inside the lane; `PGRUST_LANE_V2` still gates
/// every caller).
fn exprkey_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_EXPRKEY").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Opt-in switch for the coded-group arm (q29coded lane): the Dict key
/// class groups by the INTERN ID of the memo's output value on the compact
/// mk1 single-Intern table instead of probing the staged C tuplehash with
/// the output TEXT. Default OFF — MEASURED REGRESSION at the regexp-key 100M
/// lpp15 face (5.11s vs 3.00s staged; byte gates green, par16/serial
/// controls flat, zero budget teardowns): the partial's OUTPUT CONTRACT is
/// still materialized text rows through the Gather tuple queues, so the
/// coded table defers every group's key materialization from INSERT time
/// (parallel, overlapped with the scan) to EMIT time (the pipeline-
/// critical leader/queue face) and pays an intern re-image on top — +70%
/// wall at +6.6% cycles (stall, not compute; notes/q29coded-lane.md). The
/// arm becomes profitable only when the handoff itself is id-keyed (the
/// merge-face increments); until then it is the opt-in substrate.
/// `PGRUST_LANE_V2_CODEDKEY=1|on` engages.
fn codedkey_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        matches!(std::env::var("PGRUST_LANE_V2_CODEDKEY").as_deref(), Ok("1") | Ok("on"))
    })
}

/// GL-DICTDRAIN-1 (the Dict-class parallel sink drain): the Dict key class
/// becomes SINK-admissible through a 1-Intern compact spec — the M2 sink
/// single-text (C2) shape with the packed component fed from the dicteval
/// memo's output value instead of a staged scan column. The serial coded
/// arm's measured loss (`codedkey_enabled` doc: the partial's output
/// contract re-materialized every key through the Gather tuple queues) is
/// exactly what the sink handoff deletes — worker tables flush
/// canonical-byte runs and the leader merges on bytes, so the id-keyed
/// handoff the q29coded note named as the profitability condition IS the
/// sink. DEFAULT ON since the A-on-top-of-B ruling (`=0|off` kills); SAME
/// spelling as the m5 probe recognizer (knob-coherence law — a probe that suppressed a
/// dict-key shape this gate refuses would land it on the serial rerun).
pub(super) fn dictkey_sink_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        // DEFAULT ON since the A-on-top-of-B ruling (Michael, 2026-07-22;
        // flipped-kill idiom, same spelling as the m5 half — knob-coherence
        // law): OFF iff exactly `0`/`off`.
        !matches!(std::env::var("PGRUST_LANE_V2_AGG_DICTKEY").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Kill switch for the redundant-key (reduced grouping) tranche —
/// independent of the single-computed-key arms above.
fn redkey_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_REDKEY").as_deref(), Ok("0") | Ok("off"))
    })
}

const TEXTOID: ::types_core::Oid = 25;
const VARCHAROID: ::types_core::Oid = 1043;
const TIMESTAMPOID: ::types_core::Oid = 1114;

/// pg_proc oid of `timestamp_trunc(text, timestamp)` — `date_trunc` over
/// plain (tz-less) timestamp. The timestamptz variants (1217/1284) are NOT
/// admitted: their truncation is timezone-dependent.
const F_TIMESTAMP_TRUNC: ::types_core::Oid = 2020;

/// The ts-trunc class recognizer: `date_trunc(<const unit>, ts_col)` over a
/// plain TIMESTAMP column, for units whose C truncation is uniform
/// microsecond floor arithmetic (`timestamp2tm` → zero the sub-unit fields →
/// `tm2timestamp` ≡ `t - t.rem_euclid(unit_usecs)`: second/minute/hour
/// boundaries are uniformly spaced on PG's tz-less, leap-second-free
/// timeline, and day boundaries are 86400e6-aligned to the 2000-01-01 epoch).
/// Returns the unit's microseconds. `None` = not this shape (byte-identical
/// per-row fallback — including every unit alias/error C would raise, which
/// the per-row fmgr path preserves verbatim).
///
/// Unit strings: exact lowercase matches of the canonical names + plurals
/// only, compared after the same ASCII downcasing `downcase_ident` applies.
/// Abbreviations ("min", "hr", …) deliberately refuse — the per-row path
/// computes them correctly; admitting them here would duplicate C's
/// deltatktbl alias table for no measured shape.
fn ts_trunc_unit_usecs(
    xk: &::execexpr::ScanProjExprKey,
    keytype: ::types_core::Oid,
    mcx: ::mcx::Mcx<'_>,
) -> Option<i64> {
    if xk.input_type != TIMESTAMPOID || keytype != TIMESTAMPOID || xk.ncalls != 1 {
        return None;
    }
    let c = &xk.calls[0];
    if c.fn_oid != F_TIMESTAMP_TRUNC || c.nargs != 2 || c.var_argno != 1 {
        return None;
    }
    let unit = c.args[0];
    if unit.isnull {
        return None;
    }
    // SAFETY: a compile-time non-null text Const datum (census contract) is
    // a live varlena for the statement's lifetime.
    let packed = unsafe { ::types_fmgr::datum_varlena_packed(unit.value, mcx) }.ok()?;
    let mut low = [0u8; 16];
    let data = packed.data();
    if data.is_empty() || data.len() > low.len() {
        return None;
    }
    for (d, s) in low.iter_mut().zip(data) {
        *d = s.to_ascii_lowercase();
    }
    Some(match &low[..data.len()] {
        b"second" | b"seconds" => 1_000_000,
        b"minute" | b"minutes" => 60_000_000,
        b"hour" | b"hours" => 3_600_000_000,
        b"day" | b"days" => 86_400_000_000,
        _ => return None,
    })
}

/// The ts-trunc kernel: C `timestamp_trunc` for the admitted units over a
/// finite timestamp — microsecond floor to the unit boundary. ±infinity
/// (`DT_NOBEGIN`/`DT_NOEND` = i64::MIN/MAX) passes through unchanged
/// (C's `TIMESTAMP_NOT_FINITE` arm). Total for every storable timestamp:
/// the minimum valid timestamp is day-aligned, so flooring never leaves the
/// valid range (C's `tm2timestamp` range error is unreachable here).
#[inline(always)]
fn ts_trunc_apply(t: i64, unit: i64) -> i64 {
    if t == i64::MIN || t == i64::MAX {
        t
    } else {
        t - t.rem_euclid(unit)
    }
}

const NUMERICOID: ::types_core::Oid = 1700;

/// pg_proc oid of `extract(text, timestamp)` — the NUMERIC-returning
/// EXTRACT over plain (tz-less) timestamp. `date_part` (2021, float8
/// result — a different output surface) and the timestamptz variant
/// (tz-dependent) are NOT admitted.
const F_EXTRACT_TIMESTAMP: ::types_core::Oid = 6202;

/// extract()-class fields the fast multi-key kernel hosts (ts-extract class).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TsPartField {
    Minute,
    Hour,
}

/// The ts-extract recognizer: `extract(<const field> FROM ts_col)` over a
/// plain TIMESTAMP column producing a NUMERIC grouping key, for fields whose
/// C value (`timestamp2tm` → `tm.tm_min`/`tm.tm_hour`) is uniform
/// microsecond arithmetic on PG's tz-less, leap-second-free timeline:
/// minute-of-hour and hour-of-day boundaries are uniformly spaced, so the
/// field derives from the µs remainder without the tm decomposition (oracle
/// sweep vs `timestamp_part_common` in the unit tests), and the NUMERIC
/// result is `int64_to_numeric(field)` — dscale 0, constructible as packed
/// key bits directly (`nodeagg::mk_numeric_i64_bits`). `None` = not this
/// shape; the production fmgr chain keeps the key, byte-identically (that
/// covers calendar-dependent fields — day, month, dow, … — and `second`,
/// whose dscale-6 result is unpackable and demotes the packed feed anyway).
fn ts_extract_field(
    xk: &::execexpr::ScanProjExprKey,
    keytype: ::types_core::Oid,
    mcx: ::mcx::Mcx<'_>,
) -> Option<TsPartField> {
    if xk.input_type != TIMESTAMPOID || keytype != NUMERICOID || xk.ncalls != 1 {
        return None;
    }
    let c = &xk.calls[0];
    if c.fn_oid != F_EXTRACT_TIMESTAMP || c.nargs != 2 || c.var_argno != 1 {
        return None;
    }
    let unit = c.args[0];
    if unit.isnull {
        return None;
    }
    // SAFETY: a compile-time non-null text Const datum (census contract) is
    // a live varlena for the statement's lifetime.
    let packed = unsafe { ::types_fmgr::datum_varlena_packed(unit.value, mcx) }.ok()?;
    let mut low = [0u8; 16];
    let data = packed.data();
    if data.is_empty() || data.len() > low.len() {
        return None;
    }
    for (d, s) in low.iter_mut().zip(data) {
        *d = s.to_ascii_lowercase();
    }
    Some(match &low[..data.len()] {
        b"minute" | b"minutes" => TsPartField::Minute,
        b"hour" | b"hours" => TsPartField::Hour,
        _ => return None,
    })
}

/// C `timestamp_part_common` for the admitted fields over a FINITE tz-less
/// timestamp — total and non-erroring for every storable value (the tm
/// decomposition of a storable timestamp cannot fail). ±infinity never
/// reaches here: C returns NULL for oscillating fields of a non-finite
/// input, which is the caller's NULL-key demote arm.
#[inline(always)]
fn ts_extract_apply(t: i64, f: TsPartField) -> i64 {
    match f {
        TsPartField::Minute => t.rem_euclid(3_600_000_000) / 60_000_000,
        TsPartField::Hour => t.rem_euclid(86_400_000_000) / 3_600_000_000,
    }
}

/// Census→stitcher arith mapping (nodeseqscan's projstitch arm keeps its own
/// private copy — the enums are 1:1 by construction).
fn proj_arith(op: ::execexpr::ProjArithOp) -> ::lanestitch::ArithOp {
    use ::execexpr::ProjArithOp as E;
    use ::lanestitch::ArithOp as S;
    match op {
        E::Add2 => S::Add2,
        E::Sub2 => S::Sub2,
        E::Mul2 => S::Mul2,
        E::Div2 => S::Div2,
        E::Add4 => S::Add4,
        E::Sub4 => S::Sub4,
        E::Mul4 => S::Mul4,
        E::Div4 => S::Div4,
        E::Add8 => S::Add8,
        E::Sub8 => S::Sub8,
        E::Mul8 => S::Mul8,
        E::Div8 => S::Div8,
    }
}

/// Canonicalize an arith const to the lanestitch canonical-datum contract
/// (sign-extended image at the op's own width — same-width families only).
fn proj_arith_konst(op: ::execexpr::ProjArithOp, konst: ::datum::Datum) -> ::datum::Datum {
    use ::execexpr::ProjArithOp as E;
    match op {
        E::Add2 | E::Sub2 | E::Mul2 | E::Div2 => ::datum::Datum::from_i16(konst.as_i16()),
        E::Add4 | E::Sub4 | E::Mul4 | E::Div4 => ::datum::Datum::from_i32(konst.as_i32()),
        E::Add8 | E::Sub8 | E::Mul8 | E::Div8 => ::datum::Datum::from_i64(konst.as_i64()),
    }
}

/// Multi-key packed state (the ts-extract multi-key arm): a projected scan whose tlist
/// is bare Vars plus EXACTLY ONE computed column — a strict fmgr chain over
/// one base scan column (`ScanProjExprKey` census) — where the agg groups by
/// 2..N keys including the computed one. The computed key's grouping kind
/// must be NUMERIC (`extract(minute FROM ts)`-class): its values derive per
/// surviving row through the production fmgr and pack via the canonical
/// numeric key form (`nodeagg::mk_numeric_datum_bits`); every other key is a
/// bare-Var component packed from its base lane (Int/Numeric) or the
/// dict/intern lane (TextRaw, pgrcolumnar). Unpackable numeric values (range /
/// non-minimal display scale) DEMOTE: the compact table migrates to the C
/// tuplehash and the batch replays per-row — never a lossy pack.
pub(super) struct MultiKeyChain {
    /// The computed key's chain (production fmgr entry points). `None` ONLY
    /// for the CaseDict class (`case_dict` is `Some` then): the computed
    /// component is a conditional text select, not a numeric chain.
    pub(super) chain: Option<::laneexec::ValueChain>,
    /// CaseDict computed key (band-2a computed-text-key class): `CASE WHEN <AND of
    /// int-eq-const preds> THEN <text Var> ELSE <text Const> END` as a
    /// grouping key — evaluated per survivor as a bitmask of int compares
    /// selecting between the THEN column's per-(epoch, code) intern id and
    /// the ELSE const's memoized intern id. The packed component is Intern
    /// (4-byte id) at the computed key's position.
    pub(super) case_dict: Option<CaseDictSpec>,
    /// Recognized ts-extract fast kernel (see [`ts_extract_field`]): the
    /// derived key computes as int64 field arithmetic per survivor and packs
    /// via `mk_numeric_i64_bits` — no per-row fmgr, no NUMERIC datum. `None`
    /// = the production chain derives (byte-identical, colder). Non-finite
    /// inputs take the chain's exact NULL-key demote (C extracts NULL from
    /// ±infinity for these fields).
    pub(super) fast: Option<TsPartField>,
    /// Base scan colno feeding the chain.
    pub(super) input_base: u16,
    /// The TextRaw component's tlist attno (agg-input space), when one
    /// exists — `agg_hash_compact_try_arm_mk`'s dict_att.
    pub(super) dict_input_att: Option<u16>,
    /// Its base scan colno (the dict-lane registration target).
    pub(super) dict_base: Option<u16>,
    /// Pack scratch (the scan feed's shape, reused per batch).
    pub(super) mks: super::MkScratch,
}

/// The recognized CaseDict computed key (see [`MultiKeyChain::case_dict`]).
/// Recognition is v1-strict: exactly one WHEN arm, no CASE arg, the
/// condition an AND of `<int Var> = <int Const>` builtin equalities (any
/// int2/4/8 cross-width pair), THEN a bare text Var of the scan, ELSE a
/// non-NULL text Const. Evaluation is effect-free and non-erroring (int
/// compares + verbatim text select), so reading the THEN column on ELSE
/// rows is sound (the pack pre-passes' "per-value, effect-free" rule).
pub(super) struct CaseDictSpec {
    /// AND predicates: (base scan colno, canonical i64 const, Var width).
    pub(super) preds: Vec<(u16, i64, u8)>,
    /// THEN: the text Var's base scan colno (dict-lane registered).
    pub(super) then_base: u16,
    /// ELSE: the const's raw text payload bytes.
    pub(super) else_bytes: Vec<u8>,
    /// Per-(epoch, code) intern-id cache for the THEN column (the mk
    /// Intern arm's cache discipline, its own identity roll). Entry
    /// encoding: 0 = unset, `id + 1` otherwise (`reset_code_id_cache` —
    /// the zero-page allocation is the CaseDict vecstate fix).
    pub(super) cd_epoch: Option<(bool, u64)>,
    pub(super) cd_code_ids: Vec<u32>,
    /// Memoized ELSE intern id (cleared with the intern cache).
    pub(super) else_id: Option<u32>,
    /// Per-batch condition scratch (indexed by staged row).
    pub(super) cond: Vec<bool>,
}

/// How the key lane is computed per batch.
pub enum ExprKeyKind {
    /// Stitcher-vocabulary int arithmetic: a single-output lanestitch
    /// program over the staged base lanes (interpreter tier — the parity
    /// oracle; error identity by refuse-and-replay).
    Arith { prog: ::lanestitch::Program, ncols: usize },
    /// Strict fmgr chain over one dict-coded text column, evaluated once
    /// per (epoch, code) by the dicteval memo (per selected row on Raw
    /// windows). `gather_input` = some transition/spill column reads the
    /// SAME base column, so each dict-answered window gathers it to Raw
    /// AFTER key derivation.
    Dict {
        input_col: u16,
        prog: Box<::laneexec::DictEvalProg>,
        gather_input: bool,
    },
    /// `date_trunc(<const unit>, ts)` over a plain TIMESTAMP column for
    /// uniform-microsecond units (see [`ts_trunc_unit_usecs`]): the key lane
    /// derives by the non-erroring floor kernel [`ts_trunc_apply`] —
    /// bit-identical to the fmgr `timestamp_trunc` for every storable input,
    /// so there is no trap/replay leg at all.
    TsTrunc { input_col: u16, unit: i64 },
    /// Packed multi-key over a projected scan (see [`MultiKeyChain`]).
    Multi(Box<MultiKeyChain>),
    /// Redundant grouping-key elimination (reduced-expr-key class): 2..N int grouping
    /// keys where every non-representative key is `Var ± Const` over the
    /// ONE bare-Var key (deterministic — grouping by the representative
    /// alone is the same partition). The build probes the compact table on
    /// the representative lane only; the redundant keys are reconstructed
    /// at group read-back (compact `RedShape`). No per-batch key
    /// derivation at all — instead a per-batch RANGE GUARD proves every
    /// selected representative value inside the overflow-free domain of
    /// every derived expression; a violating batch refuse-and-replays
    /// per-row STICKY (the C-ported `exec_project` raises C's exact
    /// overflow error on C's row), exactly the arith-trap discipline.
    Reduced {
        /// The armed compact emit spec (key order; cloned to re-arm).
        shape: ::nodeagg::RedShape,
        /// Base scan lane of the representative key.
        rep_att: u16,
        /// Overflow-free canonical domain of the representative: a batch
        /// whose selected values leave `[lo, hi]` demotes per-row.
        lo: i64,
        hi: i64,
    },
}

/// Per-node expr-key state, memoized on `AggPlanState` next to the lane
/// choice (the census runs once; scratch is reused across batches/builds).
pub struct ExprKeyState {
    /// tlist arity == the agg's input natts (result-slot descriptor len).
    natts: usize,
    /// Per tlist column: `Some(base scan col)` for bare Vars; `None` for
    /// the computed key column.
    map: Vec<Option<u16>>,
    /// The computed key's tlist column (== `agg_hash_staged_probe_col`).
    key_out: u16,
    /// Base-column staging prefix (key inputs + every mapped column + the
    /// scan qual's fetch bound).
    prefix: i32,
    kind: ExprKeyKind,
    /// Sticky refuse-and-replay flag (arith trap): all later batches take
    /// the per-row emit path.
    refused: bool,
    /// M2 sink build (set by the sink drain adapter, never on serial
    /// builds): demote exits REFUSE (sticky `refused`, no C-table
    /// migration, no per-row replay — the RG abort discards the build and
    /// the serial whole-attempt rerun re-derives everything) and the coded
    /// arm's classic budget peek is skipped (the sink cap + flush law
    /// bounds the table; `agg_hash_compact_over_limits` is a classic-build
    /// accounting).
    sink_build: bool,
    // Reusable per-build scratch.
    rows: Vec<u32>,
    keys: Vec<::datum::Datum>,
    knull: Vec<bool>,
    hashes: Vec<u32>,
    hash1: Vec<u32>,
    key_vals: Vec<::datum::Datum>,
    key_null: Vec<bool>,
    /// Per-epoch code→pergroup map (dictgroup pattern).
    dg_epoch: Option<u64>,
    dg_slots: Vec<Option<core::ptr::NonNull<::execexpr::AggPerGroup>>>,
    /// M2 sink str MIN/MAX dict-code memo (GL-DICTDRAIN-1): the serial
    /// feed's tie-copy collapse, PER-WORKER-BUILD persistent — without it
    /// every tied row of a dict window datumCopies the transvalue into the
    /// bump aggcontext (text's last-tied-wins law), which measured as a
    /// runaway context (~224MiB at a 10M/1720-group cell) tripping the
    /// residual budget refusal. Lives inside the state (not the adapter)
    /// so it survives batches; `take_sink_mm`/`put_sink_mm` move it around
    /// the drain call; `invalidate_group_caches` bumps its generation on
    /// every flush (pergroup-pointer keyed — the 830320fed law).
    sink_mm: MmState,
    sink_mm_armed: bool,
}

impl ExprKeyState {
    /// Heap backing-store bytes for the process estate ledger
    /// (GL-CONCMEM-1): the batch scratch lanes plus the per-epoch
    /// code→pergroup map (`dg_slots` — gndv-sized at dict shapes, the
    /// family's whale lane). Capacity-based; settled by the drive at claim
    /// boundaries, never per row.
    pub(super) fn estate_bytes(&self) -> usize {
        use super::vec_estate_bytes as vb;
        vb(&self.map)
            + vb(&self.rows)
            + vb(&self.keys)
            + vb(&self.knull)
            + vb(&self.hashes)
            + vb(&self.hash1)
            + vb(&self.key_vals)
            + vb(&self.key_null)
            + vb(&self.dg_slots)
    }
}

/// `LaneCols` remap for projected-scan folds: plan/fold columns are tlist
/// attnos; each admitted one maps to a base scan lane. The computed key
/// column is never in `plan.cols` (admission: it has no base lane).
struct MapCols<'a, 'mcx> {
    soa: &'a ::exectuples::SoaBatch<'mcx>,
    map: &'a [Option<u16>],
}

impl ::lanefold::LaneCols for MapCols<'_, '_> {
    fn col_values(&self, c: usize) -> &[::datum::Datum] {
        let base = self.map[c].expect("fold column admitted as a bare Var") as usize;
        self.soa.col_values(base)
    }

    fn col_isnull(&self, c: usize) -> &[bool] {
        let base = self.map[c].expect("fold column admitted as a bare Var") as usize;
        self.soa.col_isnull(base)
    }
}

/// The decide-phase census + staging arm. `Some` = the fold feed can host
/// this projected build through the expr-key path (staging armed, state
/// ready); `None` = refused (reason ticked) — the caller keeps the per-row
/// breaker feed, byte-identically.
pub(super) fn decide_exprkey<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> Option<Box<ExprKeyState>> {
    if !exprkey_enabled() {
        return None;
    }
    let refused = || {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::ExprKeyShape);
        None
    };
    // Plan-level half: classified unguarded-or-guarded fold plan (guarded
    // plans re-prove per batch like the main feed), single kernel-hostable
    // grouping key. Residual transitions are admitted (module doc).
    let plan = ::nodeagg::agg_lanefold_plan(agg)?;
    let Some(key_out) = ::nodeagg::agg_hash_staged_probe_col(agg) else {
        // 2..N grouping keys: the redundant-key (reduced grouping) tranche
        // first (reduced-key class — every non-representative key a Var ± Const
        // function of the one bare-Var key; its own refuse accounting ticks
        // inside), then the packed multi-key arm (ts-extract class). Its refusals
        // tick the multikey taxonomy inside.
        if let Some(xk) = decide_reduced(agg, ss, estate) {
            return Some(xk);
        }
        if ::nodeagg::agg_hash_key_cols(agg).len() >= 2 {
            if let Some(xk) = decide_exprkey_mk(agg, ss, estate) {
                return Some(xk);
            }
            // CaseDict computed-text-key class (band-2a): the census
            // above has no CASE vocabulary — a refusal falls through to the
            // plan-tlist recognizer.
            return decide_exprkey_mk_case(agg, ss, estate);
        }
        return refused();
    };
    let proj = ss.ss.ps_ProjInfo.as_ref()?;
    let result_slot = proj.pi_result_slot;
    let natts = estate
        .slot(result_slot)
        .base()
        .tts_tupleDescriptor
        .as_ref()?
        .attrs
        .len();
    // Census the projection: the arith class first (its census also matches
    // single-Var arith chains the dict walker would), then the dict class.
    let mut map: Vec<Option<u16>> = Vec::with_capacity(natts);
    let kind = if let Some(cols) = proj
        .pi_state
        .scan_proj_cols()
        .filter(|c| c.n as usize == natts && c.any_arith())
    {
        // Exactly one computed column, and it must be the grouping key.
        let mut prog = ::lanestitch::Program::new();
        let mut computed = None;
        let mut ncols = 0usize;
        for (j, col) in cols.cols[..cols.n as usize].iter().enumerate() {
            match *col {
                ::execexpr::ScanProjCol::Var { attnum } => {
                    map.push(Some(attnum));
                }
                ::execexpr::ScanProjCol::ArithVV { op, a, b } => {
                    if computed.is_some() || a.max(b) as usize >= ::lanestitch::MAX_COLS {
                        return refused();
                    }
                    computed = Some(j as u16);
                    map.push(None);
                    ncols = ncols.max(a.max(b) as usize + 1);
                    prog.steps.push(::lanestitch::Step::LoadLane { col: a, out: 0 });
                    prog.steps.push(::lanestitch::Step::LoadLane { col: b, out: 1 });
                    prog.steps.push(::lanestitch::Step::Arith {
                        op: proj_arith(op),
                        a: 0,
                        b: 1,
                        out: 2,
                    });
                    prog.steps.push(::lanestitch::Step::StoreOut { a: 2, out: 0 });
                }
                ::execexpr::ScanProjCol::ArithVK { op, attnum, konst, var_is_arg0 } => {
                    if computed.is_some() || attnum as usize >= ::lanestitch::MAX_COLS {
                        return refused();
                    }
                    computed = Some(j as u16);
                    map.push(None);
                    ncols = ncols.max(attnum as usize + 1);
                    let k = proj_arith_konst(op, konst);
                    let kix =
                        prog.push_const(::datum::NullableDatum { value: k, isnull: false });
                    prog.steps.push(::lanestitch::Step::LoadLane { col: attnum, out: 0 });
                    prog.steps.push(::lanestitch::Step::LoadConst { k: kix, out: 1 });
                    let (a, b) = if var_is_arg0 { (0u8, 1u8) } else { (1u8, 0u8) };
                    prog.steps.push(::lanestitch::Step::Arith {
                        op: proj_arith(op),
                        a,
                        b,
                        out: 2,
                    });
                    prog.steps.push(::lanestitch::Step::StoreOut { a: 2, out: 0 });
                }
            }
        }
        if computed != Some(key_out) {
            return refused();
        }
        ExprKeyKind::Arith { prog, ncols }
    } else if let Some(xk) = proj.pi_state.scan_proj_expr_key() {
        if xk.n as usize != natts || xk.key_out != key_out {
            return refused();
        }
        // The computed chain's result type must be the grouping key's
        // column type (defense in depth — the tupledesc is plan authority).
        let keytype = estate.slot(result_slot).base().tts_tupleDescriptor.as_ref()?.attrs
            [key_out as usize]
            .atttypid;
        // Ts-trunc class first (date_trunc-keyed class): `date_trunc(const, ts)` for
        // uniform-microsecond units — a non-erroring arithmetic key lane,
        // no fmgr, no dict requirement (any staged store).
        if let Some(unit) = ts_trunc_unit_usecs(&xk, keytype, estate.es_query_cxt) {
            for j in 0..natts {
                map.push(xk.cols[j]);
            }
            ExprKeyKind::TsTrunc { input_col: xk.input_col, unit }
        } else {
            // Dict class: pgrcolumnar text column, IMMUTABLE internal builtins
            // (dicteval's fail-closed compile owns the catalog gate).
            if !::nodeseqscan::seq_scan_is_pgrcolumnar(ss)
                || !matches!(xk.input_type, TEXTOID | VARCHAROID)
            {
                return refused();
            }
            let mut calls = Vec::with_capacity(xk.ncalls as usize);
            for c in &xk.calls[..xk.ncalls as usize] {
                let Some(rettype) = ::laneexec::func_catalog_rettype(c.fn_oid) else {
                    return refused();
                };
                calls.push(::laneexec::DictCallSpec {
                    fn_oid: c.fn_oid,
                    collation: c.collation,
                    var_argno: c.var_argno as u16,
                    args: c.args[..c.nargs as usize].to_vec(),
                    rettype,
                });
            }
            if calls.last().is_some_and(|c| c.rettype != keytype) {
                return refused();
            }
            let spec = ::laneexec::DictExprSpec { col: xk.input_col, calls };
            let prog = match ::laneexec::dicteval_compile_value(&spec) {
                Ok(p) => p,
                Err(reason) => {
                    ::laneexec::log_dicteval_refused(reason);
                    return refused();
                }
            };
            for j in 0..natts {
                map.push(xk.cols[j]);
            }
            ExprKeyKind::Dict { input_col: xk.input_col, prog, gather_input: false }
        }
    } else {
        return refused();
    };
    // Coordinate map: every fold column and every spill-needed column except
    // the key must be a bare-Var tlist entry.
    if plan.cols.iter().any(|&c| map.get(c as usize).is_none_or(|m| m.is_none())) {
        return refused();
    }
    let (colnos_needed, _max) = ::nodeagg::agg_hash_needed_cols(agg);
    if colnos_needed.len() != natts {
        return refused();
    }
    let mut prefix = 0i32;
    for (c, &need) in colnos_needed.iter().enumerate() {
        if !need {
            continue;
        }
        if c == key_out as usize {
            continue;
        }
        match map[c] {
            Some(base) => prefix = prefix.max(base as i32 + 1),
            None => return refused(),
        }
    }
    let mut gather_input = false;
    match &kind {
        ExprKeyKind::Multi(_) => unreachable!("multi-key shapes decide in decide_exprkey_mk"),
        ExprKeyKind::Reduced { .. } => {
            unreachable!("the reduced kind decides in decide_reduced")
        }
        ExprKeyKind::Arith { ncols, .. } => prefix = prefix.max(*ncols as i32),
        ExprKeyKind::TsTrunc { input_col, .. } => prefix = prefix.max(*input_col as i32 + 1),
        ExprKeyKind::Dict { input_col, .. } => {
            prefix = prefix.max(*input_col as i32 + 1);
            // Transitions/spill reading the key's own base column: each
            // dict window gathers it to Raw after key derivation.
            gather_input = colnos_needed
                .iter()
                .enumerate()
                .any(|(c, &need)| need && c != key_out as usize && map[c] == Some(*input_col));
        }
    }
    if let Some(q) = ss.ss.qual.as_deref() {
        // Cover the qual's fetch bound so kernel/PREWHERE arms can host it;
        // an unknowable bound refuses (subplan/param quals never reach here
        // — seq_scan_fusible hosts them per-row, and this feed's batched
        // route requires whole-qual bitmap verdicts anyway).
        match q.max_fetch(::execexpr::SlotSrc::Scan) {
            Some(b) => prefix = prefix.max(b),
            None => return refused(),
        }
    }
    if prefix <= 0 {
        return refused();
    }
    // Staging arm (decide-phase probe, like `probe_arm_fold_prefix`): the
    // PREWHERE lane first on qual'd pgrcolumnar scans, then the columnar /
    // fixed-width-prefix deform. A refusing arm fails open to per-row.
    let armed = if ::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        if ss.ss.qual.is_some() {
            match ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, prefix) {
                Ok(true) => {}
                Ok(false) | Err(_) => {}
            }
        }
        let dict_key = match &kind {
            ExprKeyKind::Dict { input_col, .. } => Some(*input_col),
            ExprKeyKind::Arith { .. }
            | ExprKeyKind::TsTrunc { .. }
            | ExprKeyKind::Multi(_)
            | ExprKeyKind::Reduced { .. } => None,
        };
        ::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, prefix, dict_key)
    } else {
        ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, prefix, false, true, true);
        ::nodeseqscan::seq_scan_batch_soa(ss).is_some()
    };
    if !armed {
        return refused();
    }
    let kind = match kind {
        ExprKeyKind::Dict { input_col, prog, .. } => {
            ExprKeyKind::Dict { input_col, prog, gather_input }
        }
        k => k,
    };
    trace_feed(match &kind {
        ExprKeyKind::Arith { .. } => "agg-over-seqscan: expr-key feed armed (arith key)",
        ExprKeyKind::TsTrunc { .. } => "agg-over-seqscan: expr-key feed armed (ts-trunc key)",
        ExprKeyKind::Dict { .. } => "agg-over-seqscan: expr-key feed armed (dict key)",
        ExprKeyKind::Multi(_) => unreachable!("multi-key shapes decide in decide_exprkey_mk"),
        ExprKeyKind::Reduced { .. } => {
            unreachable!("the reduced kind decides in decide_reduced")
        }
    });
    Some(Box::new(ExprKeyState {
        natts,
        map,
        key_out,
        prefix,
        kind,
        refused: false,
        sink_build: false,
        sink_mm: MmState {
            cols: Vec::new(),
            codes: Vec::new(),
            scratch: ::lanefold::StrMmScratch::default(),
        },
        sink_mm_armed: false,
        rows: Vec::new(),
        keys: Vec::new(),
        knull: Vec::new(),
        hashes: Vec::new(),
        hash1: Vec::new(),
        key_vals: Vec::new(),
        key_null: Vec::new(),
        dg_epoch: None,
        dg_slots: Vec::new(),
    }))
}

/// The multi-key packed decide (ts-extract class; see [`MultiKeyChain`]): mirrors
/// `scan_mk_shape`'s admission over the PROJECTED coordinate space, WITHOUT
/// arming the compact table (the decide phase holds `&AggStateData`; the
/// build feed arms per build, exactly like the scan feed re-deciding per
/// build). `None` = refused (multikey taxonomy ticked) — the caller keeps
/// the per-row breaker feed, byte-identically.
fn decide_exprkey_mk<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> Option<Box<ExprKeyState>> {
    if !super::multikey_enabled() {
        return None;
    }
    let refused = || {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::MultiKeyShape);
        None
    };
    // v1: pgrcolumnar only — text key components need dict lanes and the
    // offset-free columnar arm stages every base component as decoded
    // datums (a heap fixed-width prefix cannot stage varlena keys).
    if !::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        return refused();
    }
    let plan = ::nodeagg::agg_lanefold_plan(agg)?;
    // The packed fold has no per-row leg: unguarded, no varlena guards, no
    // residual transitions (the scan multi-key feed's exact gates).
    if plan.guarded || !plan.vguards.is_empty() || ::nodeagg::agg_lanefold_has_resid(agg) {
        return refused();
    }
    let proj = ss.ss.ps_ProjInfo.as_ref()?;
    let result_slot = proj.pi_result_slot;
    let natts = estate
        .slot(result_slot)
        .base()
        .tts_tupleDescriptor
        .as_ref()?
        .attrs
        .len();
    let Some(xk) = proj.pi_state.scan_proj_expr_key() else { return refused() };
    if xk.n as usize != natts {
        return refused();
    }
    let key_out = xk.key_out;
    // Component classification over agg-input (tlist) coordinates: the
    // computed column must be one of the grouping keys and NUMERIC (the
    // extract()-class census result type); every other key is a bare Var of
    // a packable kind, at most one raw-bytes text key (dict/intern lane).
    let key_cols = ::nodeagg::agg_hash_key_cols(agg);
    let mut computed_is_key = false;
    let mut dict_input_att: Option<u16> = None;
    let mut fixed_total = 0usize;
    let mut n_numeric = 0usize;
    for &(att, kind) in &key_cols {
        if att == key_out {
            computed_is_key = true;
            if kind != ::nodeagg::GroupKeyKind::Numeric {
                return refused();
            }
            n_numeric += 1;
            fixed_total += 8;
            continue;
        }
        if xk.cols.get(att as usize).copied().flatten().is_none() {
            return refused();
        }
        match kind {
            ::nodeagg::GroupKeyKind::Int { width } => fixed_total += width as usize,
            ::nodeagg::GroupKeyKind::Numeric => {
                n_numeric += 1;
                fixed_total += 8;
            }
            ::nodeagg::GroupKeyKind::TextRaw => {
                if dict_input_att.is_some() {
                    return refused();
                }
                // The fold must not read the dict component's SoA cells
                // (stale under a dict-answered window — the dictgroup rule).
                if plan.cols.iter().any(|&c| c == att) {
                    return refused();
                }
                dict_input_att = Some(att);
                fixed_total += 4;
            }
            _ => return refused(),
        }
    }
    if !computed_is_key {
        return refused();
    }
    // Width-negotiation preview (the build-time arm decides
    // authoritatively): numeric components shrink 8 → 4 bytes when the
    // image exceeds 16; a shape that cannot fit either way refuses now so
    // the per-row breaker feed keeps the build.
    if fixed_total > 16 && (n_numeric == 0 || fixed_total - n_numeric * 4 > 16) {
        return refused();
    }
    // The computed key's chain: same census→spec mapping as the dict class;
    // compile_value_chain owns the catalog gates (IMMUTABLE internal-
    // language strict builtins, concrete types).
    let mut calls = Vec::with_capacity(xk.ncalls as usize);
    for c in &xk.calls[..xk.ncalls as usize] {
        let Some(rettype) = ::laneexec::func_catalog_rettype(c.fn_oid) else {
            return refused();
        };
        calls.push(::laneexec::DictCallSpec {
            fn_oid: c.fn_oid,
            collation: c.collation,
            var_argno: c.var_argno as u16,
            args: c.args[..c.nargs as usize].to_vec(),
            rettype,
        });
    }
    // The chain's result type must be the grouping key column's type
    // (defense in depth — the tupledesc is plan authority).
    let keytype = estate.slot(result_slot).base().tts_tupleDescriptor.as_ref()?.attrs
        [key_out as usize]
        .atttypid;
    if calls.last().is_none_or(|c| c.rettype != keytype) {
        return refused();
    }
    let chain = match ::laneexec::compile_value_chain(&calls) {
        Ok(c) => c,
        Err(_) => return refused(),
    };
    // Fast ts-extract kernel (the eponymous class): recognized ON TOP of the compiled
    // chain — a non-recognized shape keeps the chain, byte-identically.
    let fast = ts_extract_field(&xk, keytype, estate.es_query_cxt);
    // Coordinate map + the fold/spill bare-Var rules (the single-key
    // decide's exact checks).
    let mut map: Vec<Option<u16>> = Vec::with_capacity(natts);
    for j in 0..natts {
        map.push(xk.cols[j]);
    }
    if plan.cols.iter().any(|&c| map.get(c as usize).is_none_or(|m| m.is_none())) {
        return refused();
    }
    let (colnos_needed, _max) = ::nodeagg::agg_hash_needed_cols(agg);
    if colnos_needed.len() != natts {
        return refused();
    }
    let mut prefix = xk.input_col as i32 + 1;
    for (c, &need) in colnos_needed.iter().enumerate() {
        if !need || c == key_out as usize {
            continue;
        }
        match map[c] {
            Some(base) => prefix = prefix.max(base as i32 + 1),
            None => return refused(),
        }
    }
    if let Some(q) = ss.ss.qual.as_deref() {
        match q.max_fetch(::execexpr::SlotSrc::Scan) {
            Some(b) => prefix = prefix.max(b),
            None => return refused(),
        }
    }
    if prefix <= 0 {
        return refused();
    }
    let dict_base =
        dict_input_att.map(|att| map[att as usize].expect("TextRaw keys are bare Vars"));
    // Staging arm: PREWHERE first on qual'd scans, then the offset-free
    // columnar arm (dict registration on the text component's base column).
    if ss.ss.qual.is_some() {
        match ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, prefix) {
            Ok(true) => {}
            Ok(false) | Err(_) => {}
        }
    }
    if !::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, prefix, dict_base) {
        return refused();
    }
    trace_feed(if fast.is_some() {
        "agg-over-seqscan: expr-key feed armed (multi-key packed, ts-extract kernel)"
    } else {
        "agg-over-seqscan: expr-key feed armed (multi-key packed)"
    });
    Some(Box::new(ExprKeyState {
        natts,
        map,
        key_out,
        prefix,
        kind: ExprKeyKind::Multi(Box::new(MultiKeyChain {
            chain: Some(chain),
            case_dict: None,
            fast,
            input_base: xk.input_col,
            dict_input_att,
            dict_base,
            mks: super::MkScratch::default(),
        })),
        refused: false,
        sink_build: false,
        sink_mm: MmState {
            cols: Vec::new(),
            codes: Vec::new(),
            scratch: ::lanefold::StrMmScratch::default(),
        },
        sink_mm_armed: false,
        rows: Vec::new(),
        keys: Vec::new(),
        knull: Vec::new(),
        hashes: Vec::new(),
        hash1: Vec::new(),
        key_vals: Vec::new(),
        key_null: Vec::new(),
        dg_epoch: None,
        dg_slots: Vec::new(),
    }))
}

/// The reduced-grouping (redundant-key) admission: `Agg(hashed) → SeqScan`
/// with 2..N int grouping keys over a projected scan where EXACTLY ONE key
/// is a bare Var (the representative) and every other key is same-width
/// `Var ± Const` int arithmetic over that Var — grouping by the reduced set
/// {representative} produces the identical partition, so the build probes
/// one int lane and reconstructs the redundant keys once per GROUP at
/// read-back (the compact table's `RedShape` emit spec) instead of packing
/// or evaluating them per row. The canonical instance exactly:
/// `GROUP BY ClientIP, ClientIP-1, ClientIP-2, ClientIP-3`.
///
/// The general functional-dependency case (multiple bare-Var keys,
/// expression-over-expression, mul/div, cross-Var arithmetic) refuses to
/// the per-row breaker feed, byte-identically. Kill switch:
/// `PGRUST_LANE_V2_REDKEY=0|off`.
fn decide_reduced<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> Option<Box<ExprKeyState>> {
    use ::execexpr::ProjArithOp as E;
    use ::nodeagg::{RedDerived, RedOp};
    if !redkey_enabled() {
        return None;
    }
    let refused = || {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::RedKeyShape);
        None
    };
    // Fold-admitted plan, no residuals (the compact table is the ONLY host
    // for the reduced key set — the C table's arrival probe needs all key
    // columns — and the compact feed has no per-row resid leg).
    let plan = ::nodeagg::agg_lanefold_plan(agg)?;
    if ::nodeagg::agg_lanefold_has_resid(agg) {
        return refused();
    }
    // 2..N grouping keys, all canonical-int class at ONE width.
    let key_cols = ::nodeagg::agg_hash_key_cols(agg);
    if key_cols.len() < 2 {
        return refused();
    }
    let mut width = 0u8;
    for &(_, kind) in &key_cols {
        let ::execgrouping::GroupKeyKind::Int { width: w } = kind else {
            return refused();
        };
        if width == 0 {
            width = w;
        } else if width != w {
            return refused();
        }
    }
    let proj = ss.ss.ps_ProjInfo.as_ref()?;
    let result_slot = proj.pi_result_slot;
    let natts = estate
        .slot(result_slot)
        .base()
        .tts_tupleDescriptor
        .as_ref()?
        .attrs
        .len();
    let Some(cols) = proj
        .pi_state
        .scan_proj_cols()
        .filter(|c| c.n as usize == natts && c.any_arith())
    else {
        return refused();
    };
    // Key-order index of a tlist (agg input) column, when it is a key.
    let key_ord = |c: u16| key_cols.iter().position(|&(a, _)| a == c);
    // Pass 1: the representative — EXACTLY ONE key column that is a bare
    // Var (>1 is the general functional-dependency case: refused).
    let mut rep: Option<(u16, u16)> = None;
    for (j, col) in cols.cols[..natts].iter().enumerate() {
        if key_ord(j as u16).is_some() {
            if let ::execexpr::ScanProjCol::Var { attnum } = *col {
                if rep.is_some() {
                    return refused();
                }
                rep = Some((j as u16, attnum));
            }
        }
    }
    let Some((key_out, rep_att)) = rep else {
        return refused();
    };
    // Pass 2: classify every tlist column — bare Vars map to base lanes;
    // every OTHER key column must be same-width Add/Sub over the
    // representative's Var with a non-null Const (census contract); any
    // other computed column refuses.
    let mut map: Vec<Option<u16>> = Vec::with_capacity(natts);
    let mut red_keys: Vec<Option<RedDerived>> = vec![None; key_cols.len()];
    for (j, col) in cols.cols[..natts].iter().enumerate() {
        match *col {
            ::execexpr::ScanProjCol::Var { attnum } => map.push(Some(attnum)),
            ::execexpr::ScanProjCol::ArithVK { op, attnum, konst, var_is_arg0 } => {
                let Some(k) = key_ord(j as u16) else {
                    return refused();
                };
                if attnum != rep_att {
                    return refused();
                }
                let (rop, w) = match op {
                    E::Add2 => (RedOp::Add, 2),
                    E::Sub2 => (RedOp::Sub, 2),
                    E::Add4 => (RedOp::Add, 4),
                    E::Sub4 => (RedOp::Sub, 4),
                    E::Add8 => (RedOp::Add, 8),
                    E::Sub8 => (RedOp::Sub, 8),
                    // Mul/Div: deterministic too, but out of the v1
                    // boundary (Var ± Const only).
                    _ => return refused(),
                };
                if w != width {
                    return refused();
                }
                let k64 = match width {
                    2 => proj_arith_konst(op, konst).as_i16() as i64,
                    4 => proj_arith_konst(op, konst).as_i32() as i64,
                    _ => proj_arith_konst(op, konst).as_i64(),
                };
                red_keys[k] = Some(RedDerived { op: rop, konst: k64, var_is_arg0 });
                map.push(None);
            }
            ::execexpr::ScanProjCol::ArithVV { .. } => return refused(),
        }
    }
    // Exactly the representative's key-order slot stays underived.
    if red_keys.iter().filter(|d| d.is_none()).count() != 1
        || key_ord(key_out).is_none_or(|k| red_keys[k].is_some())
    {
        return refused();
    }
    // Every fold column a mapped bare Var (count(*) plans read none).
    if plan.cols.iter().any(|&c| map.get(c as usize).is_none_or(|m| m.is_none())) {
        return refused();
    }
    // Needed (spill-replay) columns: mapped bare Vars, or key columns —
    // the reduced feed never spills (compact-only; the backstop migrates
    // whole tables and demoted batches replay per-row with the full
    // projection), so derived key columns need no staged lane.
    let (colnos_needed, _max) = ::nodeagg::agg_hash_needed_cols(agg);
    if colnos_needed.len() != natts {
        return refused();
    }
    let mut prefix = rep_att as i32 + 1;
    for (c, &need) in colnos_needed.iter().enumerate() {
        if !need || key_ord(c as u16).is_some() {
            continue;
        }
        match map[c] {
            Some(base) => prefix = prefix.max(base as i32 + 1),
            None => return refused(),
        }
    }
    if let Some(q) = ss.ss.qual.as_deref() {
        match q.max_fetch(::execexpr::SlotSrc::Scan) {
            Some(b) => prefix = prefix.max(b),
            None => return refused(),
        }
    }
    // Overflow-free canonical domain of the representative: intersect each
    // derived expression's non-erroring input range at the key width (C
    // int2/4/8 pl/mi semantics — anything outside errors per-row). An empty
    // domain means EVERY non-null row errors: refuse (per-row raises it).
    let (tmin, tmax) = match width {
        2 => (i16::MIN as i128, i16::MAX as i128),
        4 => (i32::MIN as i128, i32::MAX as i128),
        _ => (i64::MIN as i128, i64::MAX as i128),
    };
    let (mut lo, mut hi) = (tmin, tmax);
    for d in red_keys.iter().flatten() {
        let c = d.konst as i128;
        let (l, h) = match (d.op, d.var_is_arg0) {
            (RedOp::Add, _) => (tmin - c, tmax - c),
            (RedOp::Sub, true) => (tmin + c, tmax + c),
            (RedOp::Sub, false) => (c - tmax, c - tmin),
        };
        lo = lo.max(l);
        hi = hi.min(h);
    }
    if lo > hi {
        return refused();
    }
    let (lo, hi) = (lo.max(i64::MIN as i128) as i64, hi.min(i64::MAX as i128) as i64);
    // Compact-table admissibility (read-only precheck; the feed installs
    // the real table per build — same gates, same verdict).
    match ::nodeagg::agg_hash_compact_reduced_admissible(agg) {
        ::nodeagg::CompactArm::Armed => {}
        ::nodeagg::CompactArm::KeyKind => {
            stats::tick_refused(ShapeClass::AggBuild, RefuseReason::CompactKeyKind);
            return None;
        }
        ::nodeagg::CompactArm::SpillRisk => {
            stats::tick_refused(ShapeClass::AggBuild, RefuseReason::CompactSpillRisk);
            return None;
        }
        ::nodeagg::CompactArm::Off => return None,
    }
    if !arm_stage(ss, estate, prefix, None) {
        return refused();
    }
    trace_feed("agg-over-seqscan: expr-key feed armed (reduced key)");
    Some(Box::new(ExprKeyState {
        natts,
        map,
        key_out,
        prefix,
        kind: ExprKeyKind::Reduced {
            shape: ::nodeagg::RedShape { width, keys: red_keys },
            rep_att,
            lo,
            hi,
        },
        refused: false,
        sink_build: false,
        sink_mm: MmState {
            cols: Vec::new(),
            codes: Vec::new(),
            scratch: ::lanefold::StrMmScratch::default(),
        },
        sink_mm_armed: false,
        rows: Vec::new(),
        keys: Vec::new(),
        knull: Vec::new(),
        hashes: Vec::new(),
        hash1: Vec::new(),
        key_vals: Vec::new(),
        key_null: Vec::new(),
        dg_epoch: None,
        dg_slots: Vec::new(),
    }))
}

/// The shared staging arm (decide-phase probe + per-build re-arm): the
/// PREWHERE lane first on qual'd pgrcolumnar scans, then the columnar /
/// fixed-width-prefix deform. A refusing arm fails open to per-row.
fn arm_stage<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    prefix: i32,
    dict_key: Option<u16>,
) -> bool {
    if ::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        if ss.ss.qual.is_some() {
            let _ = ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, prefix);
        }
        ::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, prefix, dict_key)
    } else {
        ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, prefix, false, true, true);
        ::nodeseqscan::seq_scan_batch_soa(ss).is_some()
    }
}

/// Re-arm the staging for a build (idempotent — the decide-phase probe armed
/// the identical shape; rescans re-enter here).
/// Builtin int-equality pg_proc oids (all int2/4/8 same- and cross-width
/// pairs): int2eq, int4eq, int8eq, int24eq, int42eq, int48eq, int84eq,
/// int28eq, int82eq. Pure value equality — a canonical-i64 compare is
/// exact for every pair.
const INT_EQ_FNS: [u32; 9] = [63, 65, 467, 158, 159, 852, 474, 1850, 1856];

const INT2OID: ::types_core::Oid = ::types_core::catalog::INT2OID;
const INT4OID: ::types_core::Oid = ::types_core::catalog::INT4OID;
const INT8OID: ::types_core::Oid = ::types_core::catalog::INT8OID;

/// One recognized CaseDict predicate operand pair: (base colno, canonical
/// const, Var width). `None` = not the `<int Var> = <int Const>` shape.
fn case_dict_pred(op: &::types_nodes::primnodes::OpExpr<'_>) -> Option<(u16, i64, u8)> {
    if !INT_EQ_FNS.contains(&op.opfuncid) || op.args.len() != 2 {
        return None;
    }
    let mut it = op.args.iter();
    let (a, b) = (it.next()?, it.next()?);
    let (v, c) = match (a.as_var(), b.as_const()) {
        (Some(v), Some(c)) => (v, c),
        _ => match (b.as_var(), a.as_const()) {
            (Some(v), Some(c)) => (v, c),
            _ => return None,
        },
    };
    if v.varlevelsup != 0 || v.varattno <= 0 || c.constisnull {
        return None;
    }
    let width = match v.vartype {
        INT2OID => 2u8,
        INT4OID => 4,
        INT8OID => 8,
        _ => return None,
    };
    let cv = match c.consttype {
        INT2OID => c.constvalue.as_i16() as i64,
        INT4OID => c.constvalue.as_i32() as i64,
        INT8OID => c.constvalue.as_i64(),
        _ => return None,
    };
    Some(((v.varattno - 1) as u16, cv, width))
}

/// Recognize the CaseDict tlist entry (see [`CaseDictSpec`]): `CASE WHEN
/// <AND of int-eq-const preds> THEN <text Var> ELSE <text Const> END`.
/// Returns (preds, then_base, else_bytes).
fn case_dict_recognize(
    expr: ::types_nodes::Node<'_>,
    mcx: ::mcx::Mcx<'_>,
) -> Option<(Vec<(u16, i64, u8)>, u16, Vec<u8>)> {
    let ce = expr.as_case_expr()?;
    if ce.arg.is_some() || !matches!(ce.casetype, TEXTOID) || ce.args.len() != 1 {
        return None;
    }
    let when = ce.args.iter().next()?.as_variant::<::types_nodes::primnodes::CaseWhen>()?;
    // Condition: one int-eq OpExpr, or an AND of them.
    let cond = when.expr?;
    let mut preds: Vec<(u16, i64, u8)> = Vec::new();
    if let Some(op) = cond.as_op_expr() {
        preds.push(case_dict_pred(op)?);
    } else {
        let be = cond.as_bool_expr()?;
        if be.boolop != ::types_nodes::primnodes::BoolExprType::AND_EXPR || be.args.len() == 0 {
            return None;
        }
        for a in be.args.iter() {
            preds.push(case_dict_pred(a.as_op_expr()?)?);
        }
    }
    // THEN: a bare text Var of the scan.
    let tv = when.result?.as_var()?;
    if tv.varlevelsup != 0 || tv.varattno <= 0 || !matches!(tv.vartype, TEXTOID) {
        return None;
    }
    // ELSE: a non-NULL text Const (a missing/NULL default derives NULL
    // keys, which the packed image cannot carry — refuse).
    let dc = ce.defresult?.as_const()?;
    if dc.constisnull || !matches!(dc.consttype, TEXTOID) {
        return None;
    }
    // SAFETY: non-null text Const datum in plan memory (parse authority);
    // detoast/借 copies through mcx as every Const consumer does.
    let bytes = unsafe { ::types_fmgr::datum_varlena_packed(dc.constvalue, mcx) }
        .ok()?
        .data()
        .to_vec();
    Some((preds, (tv.varattno - 1) as u16, bytes))
}

/// [`decide_exprkey_mk`]'s CaseDict twin (band-2a computed-text-key class):
/// the projected scan computes ONE grouping key as a CaseDict text select
/// (recognized off the PLAN tlist — the compiled census has no CASE
/// vocabulary); every other tlist column is a bare Var. The packed image
/// carries the computed key as a second Intern component (the shared
/// intern pool disambiguates by bytes), so the shape is SERIAL-ONLY: the
/// M2 sink's canonical-bytes machinery caps Intern components at one and
/// refuses upstream.
fn decide_exprkey_mk_case<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> Option<Box<ExprKeyState>> {
    if !super::multikey_enabled() || !case_dict_enabled() {
        return None;
    }
    let refused = || {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::MultiKeyShape);
        None
    };
    if !::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        return refused();
    }
    let Some(plan) = ::nodeagg::agg_lanefold_plan(agg) else {
        trace_feed("expr-key case-dict: refused (no fold plan)");
        return None;
    };
    if plan.guarded || !plan.vguards.is_empty() || ::nodeagg::agg_lanefold_has_resid(agg) {
        trace_feed("expr-key case-dict: refused (guarded/vguards/residual plan)");
        return refused();
    }
    let Some(proj) = ss.ss.ps_ProjInfo.as_ref() else {
        trace_feed("expr-key case-dict: refused (unprojected scan)");
        return None;
    };
    let result_slot = proj.pi_result_slot;
    let natts = estate
        .slot(result_slot)
        .base()
        .tts_tupleDescriptor
        .as_ref()?
        .attrs
        .len();
    // The scan PLAN tlist: bare Vars everywhere except ONE CaseDict entry.
    let Some(scan_plan) = agg.plan.plan.lefttree.and_then(::types_nodes::Node::as_seq_scan)
    else {
        trace_feed("expr-key case-dict: refused (agg child is not a SeqScan node)");
        return None;
    };
    let tlist = &scan_plan.scan.plan.targetlist;
    if tlist.len() != natts {
        trace_feed("expr-key case-dict: refused (tlist arity)");
        return refused();
    }
    let mcx = estate.es_query_cxt;
    let mut map: Vec<Option<u16>> = vec![None; natts];
    let mut case_col: Option<(u16, Vec<(u16, i64, u8)>, u16, Vec<u8>)> = None;
    for (j, n) in tlist.iter().enumerate() {
        let te = n.as_target_entry()?;
        let expr = te.expr;
        if let Some(v) = expr.as_var() {
            if v.varlevelsup != 0 || v.varattno <= 0 {
                return refused();
            }
            map[j] = Some((v.varattno - 1) as u16);
            continue;
        }
        if case_col.is_some() {
            trace_feed("expr-key case-dict: refused (two computed entries)");
            return refused();
        }
        let (preds, then_base, else_bytes) = match case_dict_recognize(expr, mcx) {
            Some(r) => r,
            None => {
                trace_feed("expr-key case-dict: refused (CASE shape not recognized)");
                return refused();
            }
        };
        case_col = Some((j as u16, preds, then_base, else_bytes));
    }
    let Some((key_out, preds, then_base, else_bytes)) = case_col else {
        trace_feed("expr-key case-dict: refused (no CASE entry in the scan tlist)");
        return refused();
    };
    let refused_at = |why: &str| {
        trace_feed(&format!("expr-key case-dict: refused ({why})"));
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::MultiKeyShape);
        None
    };
    // Grouping-key classification (agg-input coordinates): the computed
    // column must be a TextRaw grouping key; every other key a bare Var of
    // a packable kind, at most one OTHER raw-bytes text key. Numeric keys
    // refuse (v1 — keeps the CaseDict pack loop demote-free).
    let key_cols = ::nodeagg::agg_hash_key_cols(agg);
    let mut computed_is_key = false;
    let mut dict_input_att: Option<u16> = None;
    let mut fixed_total = 0usize;
    for &(att, kind) in &key_cols {
        if att == key_out {
            if kind != ::nodeagg::GroupKeyKind::TextRaw {
                return refused_at("computed key kind != TextRaw");
            }
            computed_is_key = true;
            fixed_total += 4;
            continue;
        }
        if map.get(att as usize).copied().flatten().is_none() {
            return refused_at("key column is not a bare Var");
        }
        match kind {
            ::nodeagg::GroupKeyKind::Int { width } => fixed_total += width as usize,
            ::nodeagg::GroupKeyKind::TextRaw => {
                if dict_input_att.is_some() {
                    return refused_at("a third text key");
                }
                if plan.cols.iter().any(|&c| c == att) {
                    return refused_at("fold reads the text key");
                }
                dict_input_att = Some(att);
                fixed_total += 4;
            }
            _ => return refused_at("unpackable key kind"),
        }
    }
    if !computed_is_key || fixed_total > 16 {
        return refused_at("computed col not a key, or image > 16B");
    }
    // Dict-answered windows leave value lanes stale: the fold must not
    // read the THEN column (its cells ride the extra dict registration).
    if plan.cols.iter().any(|&c| map.get(c as usize).copied().flatten() == Some(then_base)) {
        return refused_at("fold reads the THEN column");
    }
    // The fold/spill coordinate rules (decide_exprkey_mk's exact checks).
    if plan.cols.iter().any(|&c| map.get(c as usize).is_none_or(|m| m.is_none())) {
        return refused_at("fold column unmapped");
    }
    let (colnos_needed, _max) = ::nodeagg::agg_hash_needed_cols(agg);
    if colnos_needed.len() != natts {
        return refused_at("needed-cols arity");
    }
    let mut prefix = then_base as i32 + 1;
    for &(base, _, _) in &preds {
        prefix = prefix.max(base as i32 + 1);
    }
    for (c, &need) in colnos_needed.iter().enumerate() {
        if !need || c == key_out as usize {
            continue;
        }
        match map[c] {
            Some(base) => prefix = prefix.max(base as i32 + 1),
            None => return refused_at("needed col unmapped"),
        }
    }
    if let Some(q) = ss.ss.qual.as_deref() {
        match q.max_fetch(::execexpr::SlotSrc::Scan) {
            Some(b) => prefix = prefix.max(b),
            None => return refused_at("qual fetch bound"),
        }
    }
    if prefix <= 0 {
        return refused_at("empty prefix");
    }
    let dict_base =
        dict_input_att.map(|att| map[att as usize].expect("TextRaw keys are bare Vars"));
    // Staging: PREWHERE first on qual'd scans, then the columnar arm with
    // the primary dict registration + the THEN column's extra registration.
    if ss.ss.qual.is_some() {
        match ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, prefix) {
            Ok(true) => {}
            Ok(false) | Err(_) => {}
        }
    }
    if !::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, prefix, dict_base) {
        return refused_at("columnar arm");
    }
    if !::nodeseqscan::seq_scan_cb_dict_want_extra(ss, then_base) {
        return refused_at("extra dict registration");
    }
    trace_feed("agg-over-seqscan: expr-key feed armed (multi-key packed, case-dict key)");
    Some(Box::new(ExprKeyState {
        natts,
        map,
        key_out,
        prefix,
        kind: ExprKeyKind::Multi(Box::new(MultiKeyChain {
            chain: None,
            case_dict: Some(CaseDictSpec {
                preds,
                then_base,
                else_bytes,
                cd_epoch: None,
                cd_code_ids: Vec::new(),
                else_id: None,
                cond: Vec::new(),
            }),
            fast: None,
            input_base: then_base,
            dict_input_att,
            dict_base,
            mks: super::MkScratch::default(),
        })),
        refused: false,
        sink_build: false,
        sink_mm: MmState {
            cols: Vec::new(),
            codes: Vec::new(),
            scratch: ::lanefold::StrMmScratch::default(),
        },
        sink_mm_armed: false,
        rows: Vec::new(),
        keys: Vec::new(),
        knull: Vec::new(),
        hashes: Vec::new(),
        hash1: Vec::new(),
        key_vals: Vec::new(),
        key_null: Vec::new(),
        dg_epoch: None,
        dg_slots: Vec::new(),
    }))
}

/// `PGRUST_LANE_V2_CASEDICT` kill switch (default ON): the CaseDict
/// computed-text-key class. Off, those shapes keep the per-row breaker
/// feed exactly as before the car.
fn case_dict_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_CASEDICT").as_deref(), Ok("0") | Ok("off"))
    })
}

pub(super) fn exprkey_rearm<'mcx>(
    xk: &ExprKeyState,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    if ::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        if ss.ss.qual.is_some() {
            let _ = ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, xk.prefix);
        }
        let dict_key = match &xk.kind {
            ExprKeyKind::Dict { input_col, .. } => Some(*input_col),
            ExprKeyKind::Multi(m) => m.dict_base,
            ExprKeyKind::Arith { .. }
            | ExprKeyKind::TsTrunc { .. }
            | ExprKeyKind::Reduced { .. } => None,
        };
        if !::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, xk.prefix, dict_key) {
            return false;
        }
        // CaseDict THEN column: its dict lanes ride an EXTRA registration
        // on the armed batch (band-2a CaseDict).
        if let ExprKeyKind::Multi(m) = &xk.kind {
            if let Some(cd) = &m.case_dict {
                if !::nodeseqscan::seq_scan_cb_dict_want_extra(ss, cd.then_base) {
                    return false;
                }
            }
        }
        true
    } else {
        ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, xk.prefix, false, true, true);
        ::nodeseqscan::seq_scan_batch_soa(ss).is_some()
    }
}

/// The expr-key build feed: `agg_hash_build_fold_feed`'s structure with the
/// key lane computed instead of read, tlist→base column remap on every lane
/// consumer, and the per-row emit path as the universal fallback (fallback
/// rows, bitmap-less batches, guard demotes, dicteval demotes, and the
/// arith refuse-and-replay all route the WHOLE batch through it — never
/// mixing a partial batched fold with per-row transitions inside one batch).
pub(super) fn exprkey_build_fold_feed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    xk: &mut ExprKeyState,
    stage_slot: &mut Option<ExecSlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let has_resid = ::nodeagg::agg_lanefold_has_resid(agg);
    // Stage-2.2 compact table: int-arith keys with fully-admitted
    // transitions (no resid — the compact fold has no per-row leg), same
    // arming gates as the K2 feed (aggsplit, spill estimate, key width).
    // The REDUCED kind arms its own compact mode and REQUIRES it (the C
    // table's arrival probe needs every key column, so there is no staged-
    // probe fallback): an unarmable table routes the whole build per-row.
    let tick_arm = |arm: ::nodeagg::CompactArm| -> bool {
        match arm {
            ::nodeagg::CompactArm::Armed => true,
            ::nodeagg::CompactArm::KeyKind => {
                stats::tick_refused(ShapeClass::AggBuild, RefuseReason::CompactKeyKind);
                false
            }
            ::nodeagg::CompactArm::SpillRisk => {
                stats::tick_refused(ShapeClass::AggBuild, RefuseReason::CompactSpillRisk);
                false
            }
            ::nodeagg::CompactArm::Off => false,
        }
    };
    let compact = match &xk.kind {
        ExprKeyKind::Arith { .. } | ExprKeyKind::TsTrunc { .. } if !has_resid => {
            tick_arm(::nodeagg::agg_hash_compact_try_arm(agg))
        }
        ExprKeyKind::Reduced { shape, .. } if !has_resid => {
            let armed =
                tick_arm(::nodeagg::agg_hash_compact_try_arm_reduced(agg, shape.clone()));
            if !armed {
                xk.refused = true;
            }
            armed
        }
        _ => false,
    };
    // Coded-group arm (q29coded lane): the Dict key class groups by the
    // INTERN ID of the memo's output value on the compact mk1 single-Intern
    // table (`agg_hash_compact_try_arm_mk1` — the M2 sink single-text arm's
    // shape, armed here on the classic build) — the staged C-tuplehash text
    // probe and its per-group minimal-tuple key materialization never run
    // while armed. Fail-closed: any refusal (spill estimate, key kind,
    // aggsplit divisor, kill switch) keeps the staged leg byte-identically.
    // The dg/ch machinery composes unchanged — pergroup pointers simply
    // point into compact rows, with the teardown ordering law on every
    // migration path (see `coded_drop_caches`).
    let mut coded = match &xk.kind {
        ExprKeyKind::Dict { .. } if !has_resid && codedkey_enabled() => {
            let armed = tick_arm(::nodeagg::agg_hash_compact_try_arm_mk1(agg, Some(xk.key_out)));
            if armed {
                trace_feed("expr-key coded-group feed engaged (intern key)");
            }
            armed
        }
        _ => false,
    };
    // Multi-key arm: the packed compact table arms per build (mirrors the
    // scan feed's scan_mk_shape sequence, which also re-decides per build).
    // A non-armed build (spill risk under the current limits) runs whole
    // batches per-row — the arrival machinery, byte-identical.
    let mk_shape: Option<::nodeagg::MkShape> = if let ExprKeyKind::Multi(m) = &xk.kind {
        // CaseDict shapes carry TWO Intern components: the bare text Var
        // (dict_input_att) and the computed CASE key itself (key_out) —
        // both pack through the shared intern pool.
        let mut atts_buf = [0u16; 2];
        let mut n_atts = 0usize;
        if let Some(a) = m.dict_input_att {
            atts_buf[n_atts] = a;
            n_atts += 1;
        }
        if m.case_dict.is_some() {
            atts_buf[n_atts] = xk.key_out;
            n_atts += 1;
        }
        match ::nodeagg::agg_hash_compact_try_arm_mk_multi(agg, false, &atts_buf[..n_atts]) {
            ::nodeagg::CompactArm::Armed => {
                Some(::nodeagg::agg_hash_compact_mk_shape(agg).expect("armed multi-key table"))
            }
            ::nodeagg::CompactArm::KeyKind => {
                stats::tick_refused(ShapeClass::AggBuild, RefuseReason::MultiKeyShape);
                None
            }
            ::nodeagg::CompactArm::SpillRisk => {
                stats::tick_refused(ShapeClass::AggBuild, RefuseReason::CompactSpillRisk);
                None
            }
            ::nodeagg::CompactArm::Off => None,
        }
    } else {
        None
    };
    trace_feed(if mk_shape.is_some() {
        "agg-over-seqscan: expr-key fold feed engaged (multi-key packed)"
    } else if compact && matches!(xk.kind, ExprKeyKind::Reduced { .. }) {
        "agg-over-seqscan: expr-key fold feed engaged (reduced key, compact table)"
    } else if compact {
        "agg-over-seqscan: expr-key fold feed engaged (compact table)"
    } else {
        "agg-over-seqscan: expr-key fold feed engaged"
    });
    let mut idxs: Vec<u32> = Vec::new();
    let mut groups: Vec<core::ptr::NonNull<::execexpr::AggPerGroup>> = Vec::new();
    // Str MIN/MAX dict-code memo (lane-v2-dictminmax): plan columns are
    // tlist attnos; the mm map resolves them to base scan columns (bare-Var
    // admission — the computed key column never carries a str transition).
    let mut mm = MmState {
        cols: {
            let plan = ::nodeagg::agg_lanefold_plan(agg).expect("expr-key feed without a plan");
            mm_str_cols(plan, |c| xk.map.get(c as usize).copied().flatten())
        },
        codes: Vec::new(),
        scratch: ::lanefold::StrMmScratch::default(),
    };
    if !mm.cols.is_empty() {
        trace_feed("fold str min/max dict-code memo armed (expr-key)");
    }
    // Code-histogram build arming (lane-v2-codehist): the Dict key class
    // where ONE dict column feeds the key AND every admitted transition —
    // selected rows count per (epoch, code) and each (group, code) advances
    // ONCE with multiplicity. Str-kind plans additionally require the
    // no-spill estimate (their per-row tie-copies collapse; see
    // agg_hash_spill_unlikely). Non-armed shapes keep the per-row dg leg,
    // byte-identically.
    let mut ch: Option<CodeHistState> = if codehist_enabled() {
        match &xk.kind {
            ExprKeyKind::Dict { input_col, .. } if !has_resid => {
                let plan =
                    ::nodeagg::agg_lanefold_plan(agg).expect("expr-key feed without a plan");
                let icol = *input_col;
                let ntrans = plan.trans.len();
                let has_str = plan.trans.iter().any(|t| {
                    matches!(
                        t.kind,
                        ::lanefold::LaneKind::StrMin | ::lanefold::LaneKind::StrMax
                    )
                });
                let hostable = ::lanefold::plan_code_hostable(plan)
                    .is_some_and(|pc| xk.map.get(pc as usize).copied().flatten() == Some(icol));
                if hostable && (!has_str || ::nodeagg::agg_hash_spill_unlikely(agg)) {
                    trace_feed("expr-key code-histogram build engaged");
                    Some(CodeHistState::new(ntrans, has_str))
                } else {
                    None
                }
            }
            _ => None,
        }
    } else {
        None
    };
    // Fresh per-build epoch map (rescans must not reuse stale pergroups).
    xk.dg_epoch = None;
    xk.dg_slots.clear();
    // Same for the multi-key intern cache: rescans rebuild the compact +
    // intern tables, so cached code -> intern-id entries are stale.
    if let ExprKeyKind::Multi(m) = &mut xk.kind {
        m.mks.epoch = None;
        m.mks.code_ids.clear();
    }
    // K1 inc-1 source selection (the hashed feed's exact pattern, incl. the
    // grouped small-N floor): knob-ON heap scans over the floor ride
    // HeapBatchSource; everything else — and the whole knob-OFF world —
    // constructs SeqScanSource (same monomorphized drain, same machine
    // code). Knob-ON the serial scan is ONE claim: end_claim settles after
    // the drain on success AND error (zero-pins-at-settle; the drain error
    // wins the report), before the histogram flush / phase flip. Strict
    // on-error pin release is the HeapBatchSource arm's; the below-floor
    // SeqScanSource arm clears the slot only, matching base knob-OFF (see
    // SeqScanSource::end_claim).
    use super::batch_source::BatchGranuleSource as _;
    if super::batch_source::heapfeed_v2_enabled() {
        if super::batch_source::heap_gagg_admits(ss) {
            // K1 inc-2 (wave-9 WS-AH): the expr-key twin engages late
            // materialization on this arm only — HEAPFEED ∧ K1_LATEMAT ∧
            // gagg-admits; the per-build shape admission (statically-known
            // key input cols only) runs inside the drain.
            let latemat = super::batch_source::k1_latemat_enabled();
            let mut src = super::batch_source::HeapBatchSource::new(ss);
            let drove = exprkey_fold_drain(
                agg, &mut src, xk, stage_slot, compact, &mut coded, mk_shape.as_ref(),
                &mut idxs, &mut groups, &mut mm, &mut ch, latemat, estate,
            );
            let settled = src.end_claim(estate);
            drove?;
            settled?;
        } else {
            let mut src = super::batch_source::SeqScanSource::new(ss);
            let drove = exprkey_fold_drain(
                agg, &mut src, xk, stage_slot, compact, &mut coded, mk_shape.as_ref(),
                &mut idxs, &mut groups, &mut mm, &mut ch, false, estate,
            );
            let settled = src.end_claim(estate);
            drove?;
            settled?;
        }
    } else {
        exprkey_fold_drain(
            agg,
            &mut super::batch_source::SeqScanSource::new(ss),
            xk,
            stage_slot,
            compact,
            &mut coded,
            mk_shape.as_ref(),
            &mut idxs,
            &mut groups,
            &mut mm,
            &mut ch,
            false,
            estate,
        )?;
    }
    // Pending histogram counts flush at feed end (before the phase flip).
    ch_flush(agg, xk, &mut ch, &mut mm.scratch)?;
    ::nodeagg::agg_hash_build_finish(agg, estate)
}

/// The expr-key feed's drain half (K1 inc-1; the hashed drain's exact
/// pattern): generic over the storage seam's batch source — the staging
/// loop only; the per-batch machinery (`exprkey_batch`) reaches the hosted
/// scan through the transitional `seq_scan_bridge` (WS-A inc-2 deletes
/// it). Both instantiations monomorphize to #[inline] delegation — the
/// SeqScanSource instantiation is the pre-split machine code.
#[allow(clippy::too_many_arguments)]
fn exprkey_fold_drain<'mcx, S: super::batch_source::BatchGranuleSource<'mcx>>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    src: &mut S,
    xk: &mut ExprKeyState,
    stage_slot: &mut Option<ExecSlotId>,
    compact: bool,
    coded: &mut bool,
    mk_shape: Option<&::nodeagg::MkShape>,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    mm: &mut MmState,
    ch: &mut Option<CodeHistState>,
    latemat: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // End-of-scan clear ownership is process-static (trait-doc single-owner
    // rules): knob-OFF the drain clears inline exactly as before; knob-ON
    // the feed wrapper's end_claim owns it.
    let clear_inline = !super::batch_source::heapfeed_v2_enabled();
    // K1 inc-2 late-materialization arm (wave-9 WS-AH), per build: staging
    // narrows to {qual clause cols ∪ the key's statically-known input cols};
    // the deferred prefix columns complete per batch for qual survivors
    // only. Only key kinds whose input set is statically known admit
    // (TsTrunc input / the Arith program's lane width); Multi/Reduced/Dict
    // refuse NAMED `k1-latemat-exprkey-shape` (their per-batch machinery
    // reads whole-batch or code-derived cells this seam cannot narrow).
    let latemat_cols: Option<Vec<u16>> = if latemat {
        exprkey_k1_latemat_arm(super::batch_source::require_bridge(src)?, agg, xk)
    } else {
        None
    };
    loop {
        let n = src.next_batch(estate)?;
        if n == 0 {
            if clear_inline {
                let ss = super::batch_source::require_bridge(src)?;
                let mcx = estate.es_query_cxt;
                ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            }
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        // K1 inc-2 completion (pass B): the hashed drain's exact treatment —
        // fill the deferred columns for the qual survivors BEFORE any
        // consumer (the key derivation, folds, spill replays, and the
        // per-row exits' emit publish all read completed cells; the sticky
        // per-row exit path emits selected rows only, whose cells are
        // completed here). Fallback bits OR'd into the bitmap are harmless
        // (kind-0 rows only fill).
        if let Some(cols) = &latemat_cols {
            // WS-AH review F3 hardening: pin the arm invariant (an armed
            // drive recomputes the whole-qual bitmap on every staged batch
            // — qual_armed + nwords > 0) against future feed re-plumbing;
            // a stale bitmap here would silently complete the wrong rows.
            #[cfg(debug_assertions)]
            {
                let ss = super::batch_source::require_bridge(src)?;
                debug_assert!(
                    ::nodeseqscan::seq_scan_batch_qual_bitmap_ready(ss),
                    "k1-latemat completion without THIS batch's whole-qual bitmap"
                );
            }
            let nwords = (n as usize).div_ceil(64);
            let mut sel = [0u64; ::exectuples::SOA_BM_WORDS];
            match src.qual_sel() {
                Some(s) => sel[..nwords].copy_from_slice(&s[..nwords]),
                // Belt: no staged verdict ⇒ complete every row (never a
                // stale cell).
                None => sel[..nwords].fill(u64::MAX),
            }
            src.complete_deform(estate, cols, &sel[..nwords])?;
        }
        exprkey_batch(
            agg,
            super::batch_source::require_bridge(src)?,
            xk,
            stage_slot,
            compact,
            coded,
            mk_shape,
            idxs,
            groups,
            mm,
            ch,
            n,
            estate,
        )?;
    }
    Ok(())
}

/// K1 inc-2 late-materialization admission for the expr-key twin (wave-9
/// WS-AH, contract §2 scope 3): the hashed drain's `scan_k1_latemat_arm`
/// pattern with the key set = the kind's statically-known INPUT columns —
/// TsTrunc's one input column, or the Arith program's whole lane width
/// (`Lane { values: soa.col_values(c) }` for c in 0..ncols at derivation).
/// Multi/Reduced/Dict refuse NAMED `k1-latemat-exprkey-shape`: their
/// per-batch machinery (packability pre-checks, representative-domain
/// guards, dict-window code paths) reads cells this seam cannot narrow
/// soundly. Guarded/vguard plans refuse NAMED (rail G).
fn exprkey_k1_latemat_arm<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    agg: &::nodeagg::AggStateData<'mcx>,
    xk: &ExprKeyState,
) -> Option<Vec<u16>> {
    // Per-build re-decision: never inherit a previous build's narrowing.
    ::nodeseqscan::seq_scan_k1_latemat_disarm(ss);
    let plan = ::nodeagg::agg_lanefold_plan(agg)?;
    if plan.guarded || !plan.vguards.is_empty() {
        ::laneexec::log_refused("k1-latemat-guard-cols");
        return None;
    }
    let keys: Vec<u16> = match &xk.kind {
        ExprKeyKind::TsTrunc { input_col, .. } => vec![*input_col],
        ExprKeyKind::Arith { ncols, .. } => (0..*ncols as u16).collect(),
        ExprKeyKind::Multi(_) | ExprKeyKind::Reduced { .. } | ExprKeyKind::Dict { .. } => {
            ::laneexec::log_refused("k1-latemat-exprkey-shape");
            return None;
        }
    };
    // K1-F2 selectivity gate (SE9-GATES item 2): the hashed drain's exact
    // admission — late-mat only inside the plan-time low-selectivity win
    // envelope; one estimate per BUILD.
    if let Err(reason) = super::batch_source::k1_latemat_sel_admits(ss) {
        ::laneexec::log_refused(reason);
        return None;
    }
    match ::nodeseqscan::seq_scan_k1_latemat_arm(ss, &keys) {
        Ok(cols) => {
            trace_feed("k1 late-mat staging engaged (expr-key twin)");
            Some(cols)
        }
        Err(reason) => {
            ::laneexec::log_refused(reason);
            None
        }
    }
}

/// `PGRUST_LANE_V2_CODEHIST=0|off` kill switch (default ON inside the lane).
fn codehist_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_LANE_V2_CODEHIST").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Per-build code-histogram state (lane-v2-codehist). Per-epoch (row-group)
/// arrays are keyed by dict code; per-code caches fill at FIRST TOUCH while
/// the window's dict datum is valid and are pointer-free afterwards (int
/// values + a varlena IMAGE copy for str advances), so the flush never
/// dereferences a dict pointer — dict lifetimes stay window-scoped as
/// documented. Flushing is ALWAYS sound at any point: splitting a code's
/// count into several advances is byte-invisible (wrapping sums split;
/// min/max re-advance of an equal value keeps equal bytes; only the
/// ALLOCATION count changes, which the str/no-spill gate already covers) —
/// so the feed flushes liberally: epoch rollover, Raw windows, any per-row
/// route, feed end.
struct CodeHistState {
    ntrans: usize,
    has_str: bool,
    epoch: Option<u64>,
    /// Per-code selected-row counts (this epoch, since the last flush).
    hist: Vec<u32>,
    /// Codes with hist > 0, first-occurrence order.
    touched: Vec<u32>,
    /// 0 unknown / 1 proven / 2 failed (`datum_code_guards_ok`).
    guard: Vec<u8>,
    /// Per-code transition values (`code_trans_vals`), ntrans stride.
    valsflat: Vec<i64>,
    /// Per-code (offset, len) into `simg` (str plans only).
    simg_off: Vec<(u32, u32)>,
    /// Concatenated varlena images (byte-identical to the dict entries).
    simg: Vec<u8>,
    vals_scratch: Vec<i64>,
    rowcodes: Vec<u32>,
    /// Sticky spill-mode disarm: later batches keep the per-row dg leg.
    disarmed: bool,
}

enum ChVerdict {
    /// Batch counted into the histogram — no per-row probe/fold/resid runs.
    Counted,
    /// A touched code failed the per-code data proof: route the WHOLE batch
    /// through the per-row program (identical to a row-domain check_guards
    /// demote — the failing value IS selected in this batch).
    Demote,
    /// Spill-mode probe miss: disarm sticky; the existing per-row dg leg
    /// runs this batch (re-probes hit; the missing code spills per row).
    Disarm,
}

/// Opt-in for the zero-page (fresh alloc_zeroed) code→intern-id cache
/// allocation (`PGRUST_EXPRKEY_ZEROPAGE=1`). Default OFF — MEASURED
/// (board-or-hold wave, notes/vecstate-lane.md): the fresh-mmap-per-
/// execution strategy showed a consistent official-channel hot try-2
/// spike (+6-14% on the CaseDict shape; allocator page-churn class) while the eager
/// u32-memset refill below carries the SAME -27% diagnostic win (the win
/// is the u32 id+1 representation — memset-optimizable at half the bytes
/// vs the old `Vec<Option<u32>>` ptr::write fill — not the lazy paging).
fn zeropage_cache_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    crate::once_val(&ON, || {
        matches!(
            std::env::var("PGRUST_EXPRKEY_ZEROPAGE").as_deref(),
            Ok("1") | Ok("on") | Ok("true")
        )
    })
}

/// Reset a code → intern-id cache for a new (epoch) identity. Entry
/// encoding: 0 = unset, `id + 1` otherwise.
///
/// vecstate CaseDict fix (notes/vecstate-lane.md wave-3): under a v7 stitch
/// `size` is the part-GLOBAL dict NDV (URL @100M ≈ 18.3M) and the
/// historical `clear()+resize(size, None)` eagerly `ptr::write`-filled the
/// whole gndv-sized array per worker per execution — 38% of that shape's total
/// cycles. The fresh `vec![0; size]` goes through `alloc_zeroed` (kernel
/// zero pages for large sizes): untouched codes never cost a write and the
/// resident set is O(touched pages), so the per-query working memory
/// stays bounded by what the query actually references (no-large-caches
/// law: the cache still dies with the scan — only the FILL became lazy).
pub(super) fn reset_code_id_cache(cache: &mut Vec<u32>, size: usize) {
    if zeropage_cache_enabled() {
        *cache = vec![0u32; size];
    } else {
        // Kill-switch arm: the historical buffer-retaining eager refill.
        cache.clear();
        cache.resize(size, 0);
    }
}

impl CodeHistState {
    fn new(ntrans: usize, has_str: bool) -> CodeHistState {
        CodeHistState {
            ntrans,
            has_str,
            epoch: None,
            hist: Vec::new(),
            touched: Vec::new(),
            guard: Vec::new(),
            valsflat: Vec::new(),
            simg_off: Vec::new(),
            simg: Vec::new(),
            vals_scratch: Vec::new(),
            rowcodes: Vec::new(),
            disarmed: false,
        }
    }

    /// Reset the per-epoch arrays for a new dictionary (caller flushed).
    fn begin_epoch(&mut self, epoch: u64, ndict: usize) {
        self.epoch = Some(epoch);
        self.hist.clear();
        self.hist.resize(ndict, 0);
        self.touched.clear();
        self.guard.clear();
        self.guard.resize(ndict, 0);
        self.valsflat.clear();
        self.valsflat.resize(ndict * self.ntrans, 0);
        if self.has_str {
            self.simg_off.clear();
            self.simg_off.resize(ndict, (0, 0));
            self.simg.clear();
        }
    }
}

/// Flush pending histogram counts: one `fold_code_group` per touched code,
/// first-occurrence order, off the pointer-free per-code caches. Clears the
/// counts (per-code caches stay — the epoch is still live) and invalidates
/// the str MIN/MAX memo (these advances bypass it). No-op when unarmed or
/// empty.
fn ch_flush<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    xk: &ExprKeyState,
    ch: &mut Option<CodeHistState>,
    mm_scratch: &mut ::lanefold::StrMmScratch,
) -> PgResult<()> {
    let Some(ch) = ch.as_mut() else { return Ok(()) };
    if ch.touched.is_empty() {
        return Ok(());
    }
    let plan = ::nodeagg::agg_lanefold_plan(agg).expect("expr-key feed without a plan");
    let aggcx = ::nodeagg::agg_aggcontext(agg);
    // avgpack: packed inline AvgAccum slots (sink worker builds only).
    let avgpack_mask = ::nodeagg::sink::agg_sink_avgpack_mask(agg);
    for &code in &ch.touched {
        let c = code as usize;
        let n = ch.hist[c] as i64;
        debug_assert!(n >= 1);
        ch.hist[c] = 0;
        let pg = xk.dg_slots[c].expect("counted codes were resolved at first touch");
        let vals = &ch.valsflat[c * ch.ntrans..(c + 1) * ch.ntrans];
        let strd = if ch.has_str {
            let (off, _len) = ch.simg_off[c];
            ::datum::Datum::from_usize(ch.simg[off as usize..].as_ptr() as usize)
        } else {
            ::datum::Datum::null()
        };
        // SAFETY: pergroup arrays cover every transno (probe contract);
        // aggcx is the node's agg context; strd is a live inline varlena
        // image copy for str plans (begin_epoch/simg discipline); guards
        // proven per code at first touch.
        unsafe { ::lanefold::fold_code_group(plan, vals, strd, n, pg, aggcx, avgpack_mask)? };
    }
    ch.touched.clear();
    // The advances above bypassed the per-group str memo.
    mm_scratch.invalidate();
    Ok(())
}

/// One dict-window batch through the code histogram: prove + cache each NEW
/// touched code (guards, transition values, str image) while the window's
/// dict datum is valid, resolve unresolved groups through the SAME staged
/// probe leg at the first surviving row (identical first-arrival insertion
/// order), then count every survivor into the per-epoch histogram. Counting
/// happens ONLY after the whole batch validated (a Demote/Disarm exit
/// leaves the histogram untouched — the per-row route replays the batch
/// cleanly).
fn ch_batch<'mcx>(
    ch: &mut CodeHistState,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    xk: &mut ExprKeyState,
    lane: &::exectuples::SoaDictLane,
    coded: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ChVerdict> {
    debug_assert_eq!(ch.epoch, Some(lane.table.epoch));
    let ndict = lane.table.ndict as usize;
    // Pass 1 (plan borrowed, agg immutable): per-code proofs + caches at
    // first touch; collect the batch's code-per-row sequence.
    {
        let plan = ::nodeagg::agg_lanefold_plan(agg).expect("expr-key feed without a plan");
        ch.rowcodes.clear();
        for k in 0..xk.rows.len() {
            let i = xk.rows[k] as usize;
            let code = lane.code(i);
            let c = code as usize;
            debug_assert!(c < ndict, "filler contract: code < ndict");
            match ch.guard[c] {
                1 => {}
                2 => return Ok(ChVerdict::Demote),
                _ => {
                    let d = lane.table.datum(code);
                    // SAFETY: dict entries are live inline varlena images
                    // for the staged window (decode contract).
                    if !unsafe { ::lanefold::datum_code_guards_ok(plan, d) } {
                        ch.guard[c] = 2;
                        return Ok(ChVerdict::Demote);
                    }
                    // SAFETY: guards just proven for d.
                    unsafe { ::lanefold::code_trans_vals(plan, d, &mut ch.vals_scratch) };
                    ch.valsflat[c * ch.ntrans..(c + 1) * ch.ntrans]
                        .copy_from_slice(&ch.vals_scratch);
                    if ch.has_str {
                        // Pointer-free image copy (4-aligned so the 4B
                        // varlena header reads stay aligned), byte-identical
                        // to the dict entry — the flush advance datumCopies
                        // exactly these bytes.
                        while ch.simg.len() % 4 != 0 {
                            ch.simg.push(0);
                        }
                        let off = ch.simg.len() as u32;
                        // SAFETY: inline varlena (vguard above) — the image
                        // spans varsize_any bytes from the header.
                        let img = unsafe {
                            let ptr = d.as_usize() as *const u8;
                            let len = ::types_tuple::varatt::varsize_any(ptr);
                            core::slice::from_raw_parts(ptr, len)
                        };
                        ch.simg.extend_from_slice(img);
                        ch.simg_off[c] = (off, img.len() as u32);
                    }
                    ch.guard[c] = 1;
                }
            }
            ch.rowcodes.push(code);
        }
    }
    // Pass 2 (agg mutable): resolve unresolved groups in first-occurrence
    // row order — the dg leg's exact probe sequence (coded arm: the same
    // intern+compact resolve, which never misses — no Disarm leg).
    for (k, &code) in ch.rowcodes.iter().enumerate() {
        let c = code as usize;
        if xk.dg_slots[c].is_none() {
            if coded {
                let key = xk.keys[k];
                debug_assert!(!xk.knull[k], "coded batches were null-checked above");
                // SAFETY: memo outputs are live non-null text varlenas for
                // the staged window (dicteval arena contract).
                let v =
                    unsafe { ::types_fmgr::datum_varlena_packed(key, estate.es_query_cxt) }?;
                xk.dg_slots[c] = Some(::nodeagg::agg_hash_compact_probe_coded(agg, v.data())?);
                continue;
            }
            let (key, isnull) = (xk.keys[k], xk.knull[k]);
            ::nodeagg::agg_hash_hash_staged(agg, &[key], &[isnull], &mut xk.hash1)?;
            match ::nodeagg::agg_hash_probe_staged(agg, estate, key, isnull, xk.hash1[0])? {
                Some(pg) => xk.dg_slots[c] = Some(pg),
                None => return Ok(ChVerdict::Disarm),
            }
        }
    }
    // Pass 3: count (validated batch only).
    for &code in &ch.rowcodes {
        let c = code as usize;
        if ch.hist[c] == 0 {
            ch.touched.push(code);
        }
        ch.hist[c] += 1;
    }
    Ok(ChVerdict::Counted)
}

/// Per-build str MIN/MAX dict-code memo state (see `StrMmScratch`).
struct MmState {
    /// (plan col, base scan col) pairs for the plan's StrMin/StrMax lanes.
    cols: Vec<(u16, u16)>,
    /// Per-batch collected code views.
    codes: Vec<(u16, ::exectuples::SoaDictLane)>,
    scratch: ::lanefold::StrMmScratch,
}

/// One staged batch. See `exprkey_build_fold_feed` for the routing rules.
#[allow(clippy::too_many_arguments)]
/// M2 sink drain adapter (runtime_agg.rs): one staged page batch through
/// the expr-key feed under SINK constraints — compact table REQUIRED, no
/// dict/code-histogram state; the str-mm memo is the STATE's own
/// (`take_sink_mm` — build-persistent tie-copy collapse; empty for every
/// kind but Dict); `mk_shape` = the armed packed shape for the Multi kind
/// (ts-extract class), `None` otherwise. `Ok(false)` = the batch routed
/// anywhere the compact table cannot host (sticky range-guard refusal, a
/// numeric pack demote's compact disarm): the sink cannot export the C
/// tuplehash, so the caller REFUSES (RG abort → serial whole-attempt rerun
/// — a data-borne error then surfaces from the serial replay with C's
/// exact error identity).
pub(super) fn exprkey_sink_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    xk: &mut ExprKeyState,
    mk_shape: Option<&::nodeagg::MkShape>,
    stage_slot: &mut Option<ExecSlotId>,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    n: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let mut mm = xk.take_sink_mm(agg);
    let mut ch: Option<CodeHistState> = None;
    debug_assert!(xk.sink_build, "the sink drain adapter requires the armed sink decide");
    // GL-DICTDRAIN-1: the Dict kind drains through the CODED arm — the
    // dicteval memo derives once per (epoch, code) and the resolve probes
    // the 1-Intern compact table by the output value's canonical bytes.
    // `compact=false` keeps the single-datum compact probe (int-key
    // vocabulary) off this kind; every other kind keeps its flags verbatim.
    let dict = matches!(xk.kind, ExprKeyKind::Dict { .. });
    let mut coded = dict;
    let drove = exprkey_batch(
        agg, ss, xk, stage_slot, !dict, &mut coded, mk_shape, idxs, groups, &mut mm, &mut ch, n,
        estate,
    );
    xk.put_sink_mm(mm);
    drove?;
    Ok(!xk.refused && ::nodeagg::agg_hash_compact_armed(agg))
}

/// The sink-admissible key kind of an expr-key decide (runtime_agg's
/// admission input).
pub(super) enum SinkXkKind {
    /// Arith/TsTrunc: single staged int key (compact Single).
    Single,
    /// Redundant-key elimination (compact Reduced).
    Reduced(::nodeagg::RedShape),
    /// Packed multi-key over the projected scan (ts-extract class) — the compact
    /// Multi arm packs it; `dict_input_att` names the TextRaw component's
    /// tlist attno when one exists (the intern/canonical-bytes lane).
    Multi { dict_input_att: Option<u16> },
    /// GL-DICTDRAIN-1: the Dict key class through the 1-Intern compact
    /// spec (the C2 single-text shape) — the dicteval memo derives the key
    /// once per (epoch, code), the coded resolve interns the OUTPUT VALUE's
    /// canonical bytes and probes the mk1 table (intern-armed or DIRECT).
    /// Knob-gated (`dictkey_sink_enabled`), DEFAULT OFF.
    DictCoded,
}

impl ExprKeyState {
    /// The sink-admissible key kind of this decide: `Single` for Arith/
    /// TsTrunc, `Reduced` for the redundant-key kind, `Multi` for the packed
    /// multi-key kind (int/numeric/one-text components — the compact mk
    /// admission owns the component gates); `DictCoded` for the Dict kind
    /// UNDER THE KNOB (GL-DICTDRAIN-1 — the 1-Intern compact spec; its
    /// per-epoch code→pergroup map stays worker-LOCAL, but the compact mk1
    /// table it points into exports canonical bytes at flush); `None` for
    /// the knob-off Dict kind (the historical per-worker C-table refusal).
    pub(super) fn sink_key_kind(&self) -> Option<SinkXkKind> {
        match &self.kind {
            ExprKeyKind::Arith { .. } | ExprKeyKind::TsTrunc { .. } => Some(SinkXkKind::Single),
            ExprKeyKind::Reduced { shape, .. } => Some(SinkXkKind::Reduced(shape.clone())),
            ExprKeyKind::Multi(m) => {
                // CaseDict shapes (TWO Intern components) are sink-admissible
                // since canon-sink car 1: the canonical image length-prefixes
                // multi-text tails, so the two tails decode unambiguously.
                // The leader's `mk_shape_sink_ok` still owns the component
                // gates (and the two-text kill switch).
                Some(SinkXkKind::Multi { dict_input_att: m.dict_input_att })
            }
            ExprKeyKind::Dict { .. } if dictkey_sink_enabled() => Some(SinkXkKind::DictCoded),
            ExprKeyKind::Dict { .. } => None,
        }
    }

    /// The Multi kind's Intern (text) component input atts, in the packed
    /// arm's order — the `agg_hash_compact_{mk_admit,try_arm_mk}_multi`
    /// argument (mirrors `exprkey_build_fold_feed`'s own arm sequence:
    /// the bare text Var first, then the CaseDict computed key). Empty for
    /// intern-free Multi shapes. The Dict kind (GL-DICTDRAIN-1) carries
    /// exactly ONE Intern att — the computed key column (`key_out`), the
    /// serial coded arm's `try_arm_mk1` argument verbatim. `None` for the
    /// remaining kinds.
    pub(super) fn sink_mk_intern_atts(&self) -> Option<([u16; 2], usize)> {
        match &self.kind {
            ExprKeyKind::Multi(m) => {
                let mut atts = [0u16; 2];
                let mut n = 0usize;
                if let Some(a) = m.dict_input_att {
                    atts[n] = a;
                    n += 1;
                }
                if m.case_dict.is_some() {
                    atts[n] = self.key_out;
                    n += 1;
                }
                Some((atts, n))
            }
            ExprKeyKind::Dict { .. } => Some(([self.key_out, 0], 1)),
            _ => None,
        }
    }

    /// M2 sink arm marker (GL-DICTDRAIN-1): the drain adapter's demote
    /// exits refuse instead of migrating (field doc on `sink_build`). Set
    /// once by `arm_sink_build` on the WORKER's own decide — the leader's
    /// admission-probe state never drains batches.
    pub(super) fn set_sink_build(&mut self) {
        self.sink_build = true;
    }

    /// Drop the per-epoch code→pergroup cache (dictgroup pattern) AND the
    /// sink str-mm memo generation. The M2 sink drive calls this after
    /// EVERY flush — the flush RESET the compact table, so every cached
    /// pointer (dg slot or mm memo key) is a dangling table row (the
    /// 830320fed law; the intern-id caches ride the separate
    /// `invalidate_mk_intern_cache` channel, which only intern-reset
    /// flushes raise). No-op for kinds that never fill the caches.
    pub(super) fn invalidate_group_caches(&mut self) {
        self.dg_epoch = None;
        self.dg_slots.clear();
        self.sink_mm.scratch.invalidate();
    }

    /// Move the sink str-mm memo out for one drain call (borrow split —
    /// `exprkey_batch` takes `&mut self` AND `&mut MmState`). First take
    /// arms the cols for the Dict kind (the serial feed's `mm_str_cols`
    /// over the plan's StrMin/StrMax lanes through the base-column map);
    /// every other kind keeps an empty memo (their sink plans carry no str
    /// transitions — the vguard belts).
    pub(super) fn take_sink_mm(&mut self, agg: &::nodeagg::AggStateData<'_>) -> MmState {
        if !self.sink_mm_armed {
            self.sink_mm_armed = true;
            if matches!(self.kind, ExprKeyKind::Dict { .. }) {
                if let Some(plan) = ::nodeagg::agg_lanefold_plan(agg) {
                    let map = &self.map;
                    self.sink_mm.cols =
                        mm_str_cols(plan, |c| map.get(c as usize).copied().flatten());
                    if !self.sink_mm.cols.is_empty() {
                        trace_feed("fold str min/max dict-code memo armed (dict sink)");
                    }
                }
            }
        }
        core::mem::replace(
            &mut self.sink_mm,
            MmState {
                cols: Vec::new(),
                codes: Vec::new(),
                scratch: ::lanefold::StrMmScratch::default(),
            },
        )
    }

    /// Return the memo after the drain call (see [`Self::take_sink_mm`]).
    pub(super) fn put_sink_mm(&mut self, mm: MmState) {
        self.sink_mm = mm;
    }

    /// Sticky per-build refusal flag (arith trap / range guard).
    pub(super) fn sink_refused(&self) -> bool {
        self.refused
    }

    /// Invalidate the Multi kind's per-epoch code→intern-id cache. The M2
    /// sink drain calls this after a flush that RESET the worker's intern
    /// table (wide-vocabulary bounding) — a cached id would materialize
    /// the WRONG bytes. No-op for the other kinds.
    pub(super) fn invalidate_mk_intern_cache(&mut self) {
        if let ExprKeyKind::Multi(m) = &mut self.kind {
            m.mks.epoch = None;
            m.mks.code_ids.clear();
            if let Some(cd) = m.case_dict.as_mut() {
                cd.cd_epoch = None;
                cd.cd_code_ids.clear();
                cd.else_id = None;
            }
        }
    }
}

/// Drop the coded-group arm's compact-row pointer caches, STICKY (q29coded
/// lane). Callers run this strictly BEFORE `agg_hash_compact_disarm` — the
/// migration frees the compact rows the dg/ch caches point into — and
/// strictly AFTER `ch_flush` (pending per-code counts fold into those same
/// still-live rows). The build keeps the staged C-table leg from here: the
/// migration re-homed every group there, so later batches re-resolve codes
/// against the same groups, first-arrival order preserved.
fn coded_drop_caches(xk: &mut ExprKeyState, coded: &mut bool) {
    xk.dg_slots.clear();
    xk.dg_epoch = None;
    *coded = false;
    trace_feed("expr-key coded-group teardown (staged leg resumes)");
}

/// The universal whole-batch per-row exit: flush pending per-code histogram
/// counts, tear down the coded arm's caches when engaged (ORDER LAW: flush →
/// drop caches → disarm; the disarm migration frees the compact rows), then
/// migrate any armed compact table, drop the str MIN/MAX memo (per-row str
/// advances bypass it), and route the whole batch through the per-row
/// program — byte-identical (the permuted advance order is byte-invisible
/// on transvalues).
#[allow(clippy::too_many_arguments)]
fn per_row_exit<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    xk: &mut ExprKeyState,
    ch: &mut Option<CodeHistState>,
    mm: &mut MmState,
    coded: &mut bool,
    n: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // M2 sink builds have NO per-row leg (no C-table export, and
    // `compact_migrate` is a classic-build move — sink tables carry
    // avgpack/canon state it must never touch): REFUSE, sticky. Every
    // per-row-exit site runs BEFORE this batch's probes/transitions, and
    // the RG abort discards the whole worker build — the serial
    // whole-attempt rerun re-derives everything (a data-borne error then
    // surfaces there with C's exact identity).
    if xk.sink_build {
        xk.refused = true;
        trace_feed("expr-key sink demote: refusing to serial (sticky)");
        return Ok(());
    }
    ch_flush(agg, xk, ch, &mut mm.scratch)?;
    if *coded {
        coded_drop_caches(xk, coded);
    }
    ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
    mm.scratch.invalidate();
    per_row_batch(agg, ss, n, estate)
}

#[allow(clippy::too_many_arguments)]
fn exprkey_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    xk: &mut ExprKeyState,
    stage_slot: &mut Option<ExecSlotId>,
    compact: bool,
    coded: &mut bool,
    mk_shape: Option<&::nodeagg::MkShape>,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    mm: &mut MmState,
    ch: &mut Option<CodeHistState>,
    n: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let nwords = (n as usize).div_ceil(64);
    // Survivor collection: whole-qual bitmap verdicts minus fallback rows.
    // Anything less (sticky refusal, a fallback-bearing batch, a qual with
    // no staged verdicts, a batch staged before arming) routes the WHOLE
    // batch per-row — both modes probe in row order, so the per-batch
    // choice preserves the global first-arrival insertion sequence.
    let mut sel = [0u64; ::exectuples::SOA_BM_WORDS];
    let batched = !xk.refused && {
        match ::nodeseqscan::seq_scan_batch_soa(ss) {
            None => false,
            Some(soa) => {
                let all_lane = soa.fallback_words().iter().all(|&w| w == 0);
                if !all_lane {
                    false
                } else if let Some(qsel) = ::nodeseqscan::seq_scan_batch_qual_sel(ss)
                    .filter(|_| ::nodeseqscan::seq_scan_batch_qual_bitmap_ready(ss))
                {
                    sel[..nwords].copy_from_slice(&qsel[..nwords]);
                    // Belt: the staged drive ORs fallback bits into sel for
                    // the fetch contract — clear them (none staged here).
                    for (s, fb) in sel[..nwords].iter_mut().zip(soa.fallback_words()) {
                        *s &= !fb;
                    }
                    true
                } else if ss.ss.qual.is_none() {
                    sel[..nwords].fill(!0u64);
                    if n % 64 != 0 {
                        sel[nwords - 1] = (1u64 << (n % 64)) - 1;
                    }
                    true
                } else {
                    false
                }
            }
        }
    };
    if !batched {
        return per_row_exit(agg, ss, xk, ch, mm, coded, n, estate);
    }
    xk.rows.clear();
    for (w, &word) in sel[..nwords].iter().enumerate() {
        let mut bits = word;
        while bits != 0 {
            let i = (w as u32) * 64 + bits.trailing_zeros();
            bits &= bits - 1;
            xk.rows.push(i);
        }
    }
    // ZERO-SURVIVOR staged window: skip BEFORE any lane demand (the near-unique-text-key
    // codedgroup precedent — condcache-census lane). A condition-cache HIT
    // whose cached verdicts are all-fail legitimately skips the survivor
    // deform and dict-lane gather (nodeseqscan's cond_hit arm; multi-clause
    // all-fail miss windows and zone AllFail folds behave the same), so the
    // window carries NO live lanes. The Dict leg's dicteval prepare would
    // see ColView::Missing and demote, DISARMING the compact table for the
    // whole remaining build — a one-shot degrade triggered by a window that
    // has nothing to probe, fold, or transition on ANY path (the batched
    // route and the per-row replay are both no-ops over zero survivors).
    // The bitmap here is the WHOLE qual's verdict (the `batched` admission
    // above), so empty selection == truly zero survivors.
    if xk.rows.is_empty() {
        return Ok(());
    }
    // Multi-key packed batches own everything from here (derive → pack →
    // packed probe → fold); the single-key legs below never see them.
    if matches!(xk.kind, ExprKeyKind::Multi(_)) {
        return exprkey_mk_batch(agg, ss, xk, mk_shape, idxs, groups, n, estate);
    }
    // Reduced grouping (redundant keys): no key derivation at all — the
    // representative lane probes the compact table directly.
    if matches!(xk.kind, ExprKeyKind::Reduced { .. }) {
        return reduced_batch(agg, ss, xk, &sel, nwords, n, idxs, groups, estate);
    }
    // Key-lane derivation.
    let mut dict_lane: Option<::exectuples::SoaDictLane> = None;
    match &mut xk.kind {
        ExprKeyKind::Multi(_) => unreachable!("multi-key batches returned above"),
        ExprKeyKind::Reduced { .. } => {
            unreachable!("reduced batches routed through reduced_batch above")
        }
        ExprKeyKind::Arith { prog, ncols } => {
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("expr-key batched route requires the armed SoA");
            let mut lanes: Vec<::lanestitch::Lane<'_>> = Vec::with_capacity(*ncols);
            for c in 0..*ncols {
                lanes.push(::lanestitch::Lane {
                    values: soa.col_values(c),
                    isnull: soa.col_isnull(c),
                });
            }
            let batch = ::lanestitch::Batch { nrows: n, lanes };
            // Word-level SelVec build: `all(n)` masks bits at/past n, so
            // ANDing the whole-qual words is exactly the per-row
            // clear-on-cleared-bit walk (wordskip lane: no per-row test).
            let mut sv = ::lanestitch::SelVec::all(n);
            for (w, s) in sv.words[..nwords].iter_mut().zip(sel[..nwords].iter()) {
                *w &= *s;
            }
            xk.key_vals.clear();
            xk.key_vals.resize(n as usize, ::datum::Datum::null());
            xk.key_null.clear();
            xk.key_null.resize(n as usize, true);
            let mut outs = [::lanestitch::OutLane {
                values: &mut xk.key_vals[..],
                isnull: &mut xk.key_null[..],
            }];
            // SAFETY-free interpreter tier; an Err is an arith trap
            // (overflow / zero divisor) on some selected row. Refuse-and-
            // replay (module doc): discard the computed lane — NO probe or
            // transition has run for this batch — and replay the whole
            // batch per-row; `exec_project` raises C's exact error on C's
            // row. Sticky thereafter.
            if ::lanestitch::eval_project(prog, &batch, &sv, &mut outs).is_err() {
                xk.refused = true;
                trace_feed("expr-key arith trap: replaying batch per-row (sticky)");
                return per_row_exit(agg, ss, xk, ch, mm, coded, n, estate);
            }
        }
        ExprKeyKind::TsTrunc { input_col, unit } => {
            let (col, unit) = (*input_col as usize, *unit);
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("expr-key batched route requires the armed SoA");
            let vals = soa.col_values(col);
            let nulls = soa.col_isnull(col);
            xk.key_vals.clear();
            xk.key_vals.resize(n as usize, ::datum::Datum::null());
            xk.key_null.clear();
            xk.key_null.resize(n as usize, true);
            // Non-erroring floor kernel (strict: NULL in -> NULL out); no
            // trap/replay leg — bit-identical to the fmgr for every input.
            for &i in &xk.rows {
                let i = i as usize;
                if !nulls[i] {
                    xk.key_vals[i] =
                        ::datum::Datum::from_i64(ts_trunc_apply(vals[i].as_i64(), unit));
                    xk.key_null[i] = false;
                }
            }
        }
        ExprKeyKind::Dict { input_col, prog, gather_input } => {
            let col = *input_col as usize;
            {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("expr-key batched route requires the armed SoA");
                dict_lane = soa.dict_lane(col);
                // Once per (epoch, code) on dict windows; per selected row
                // on Raw windows — errors raise at the per-row path's row.
                match ::laneexec::dicteval_prepare_batch(
                    core::slice::from_mut(prog),
                    soa,
                    &sel[..nwords],
                    n,
                )? {
                    ::laneexec::DictEvalPrepared::Ready => {}
                    ::laneexec::DictEvalPrepared::Demote(reason) => {
                        ::laneexec::log_dicteval_demoted(reason);
                        return per_row_exit(agg, ss, xk, ch, mm, coded, n, estate);
                    }
                }
                let (vals, nulls) = prog.scratch();
                xk.key_vals.clear();
                xk.key_vals.extend_from_slice(vals);
                xk.key_null.clear();
                xk.key_null.extend_from_slice(nulls);
            }
            // Fold/resid/spill consumers read the key's base column: gather
            // the dict window to Raw AFTER derivation (the captured lane
            // pointers stay valid for the staged window).
            if *gather_input {
                ::nodeseqscan::seq_scan_batch_gather_dict(ss, col);
            }
        }
    }
    // Guarded plans (int2-Var OpExpr admissions): prove the survivors
    // before any fold — the main feed's discipline over the remapped lanes.
    // Code-histogram dict batches skip the row-domain walk: the ch path
    // proves per TOUCHED CODE instead (values of selected rows ⊆ touched
    // dict entries — `datum_code_guards_ok`), and its Demote/Disarm exits
    // route the whole batch per-row, which re-proves row-domain.
    let ch_owns_batch = dict_lane.is_some() && ch.as_ref().is_some_and(|c| !c.disarmed);
    if !ch_owns_batch {
        let plan = ::nodeagg::agg_lanefold_plan(agg).expect("expr-key feed without a plan");
        if plan.guarded {
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("expr-key batched route requires the armed SoA");
            // SAFETY: selected rows are staged non-fallback rows with live
            // lane values for every mapped plan column.
            let demote = unsafe {
                ::lanefold::check_guards(
                    plan,
                    &MapCols { soa, map: &xk.map },
                    &sel[..nwords],
                    |_| None,
                )
            } == ::lanefold::GuardCheck::Demote;
            if demote {
                return per_row_exit(agg, ss, xk, ch, mm, coded, n, estate);
            }
        }
    }
    // Survivor-aligned key arrays.
    xk.keys.clear();
    xk.knull.clear();
    for &i in &xk.rows {
        xk.keys.push(xk.key_vals[i as usize]);
        xk.knull.push(xk.key_null[i as usize]);
    }
    // Coded-group arm (q29coded): pre-batch gates, BEFORE any probe.
    // (a) Budget peek — the classic backstop migrates as a side effect,
    // which would free the compact rows the dg/ch caches point into; the
    // coded teardown runs the same migration in cache-safe order (flush →
    // drop caches → disarm) and the batch continues on the staged leg
    // below, which finds every group in the C table. (b) A NULL derived
    // key cannot pack into the null-bitmap-free mk1 image — same teardown;
    // the staged leg's probe handles NULL keys natively (byte-identical).
    //
    // M2 sink builds (GL-DICTDRAIN-1): the budget peek is SKIPPED — the
    // sink cap + flush law bounds the table between batches, and
    // `agg_hash_compact_over_limits` is classic-build accounting (its own
    // doc). A NULL derived key REFUSES (no C-table leg; the strict-chain +
    // cbstore no-NULLs admission makes this unreachable at defaults — the
    // belt covers slot-stream windows and future widenings).
    if *coded && xk.sink_build {
        if xk.knull.iter().any(|&nl| nl) {
            xk.refused = true;
            trace_feed("expr-key sink demote: NULL derived key — refusing to serial (sticky)");
            return Ok(());
        }
    } else if *coded
        && (::nodeagg::agg_hash_compact_over_limits(agg) || xk.knull.iter().any(|&nl| nl))
    {
        ch_flush(agg, xk, ch, &mut mm.scratch)?;
        coded_drop_caches(xk, coded);
        ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
    }
    // Probe. Compact first (int keys, no resid), then the dictgroup-style
    // per-epoch code map (dict windows), then the batched staged probe.
    if compact && ::nodeagg::agg_hash_compact_armed(agg) {
        let ExprKeyState { keys, knull, rows, .. } = &mut *xk;
        if ::nodeagg::agg_hash_compact_batch(agg, estate, keys, knull, groups)? {
            idxs.clear();
            idxs.extend_from_slice(rows);
            // SAFETY: every probed row is non-fallback with valid lane
            // values for every mapped plan column; each pergroup was
            // installed by the compact probe within this batch; the rest is
            // agg_fold_staged's contract; dict-code views satisfy the
            // col_codes contract (`seq_scan_batch_dict_codes` through the
            // base-column map).
            collect_mm_codes(ss, &mm.cols, &mut mm.codes);
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("expr-key batched route requires the armed SoA");
            return unsafe {
                agg_fold_staged_mm(
                    agg,
                    &CodesCols { inner: &MapCols { soa, map: &xk.map }, codes: &mm.codes },
                    idxs,
                    groups,
                    Some(&mut mm.scratch),
                )
            };
        }
        // Runtime backstop migrated to the C table BEFORE this batch: fall
        // through to the staged probe (same rows, same order).
    }
    idxs.clear();
    groups.clear();
    if let Some(lane) = dict_lane {
        // Dictgroup pattern: per-epoch direct-indexed code→pergroup map;
        // unseen codes resolve once through the staged-probe leg at exactly
        // the first surviving row (first-arrival order, spill decisions
        // identical to the per-row path).
        let ndict = lane.table.ndict as usize;
        // Code-histogram epoch rollover: pending counts flush BEFORE the
        // code→pergroup map resets (the flush reads it).
        if let Some(chs) = ch.as_mut() {
            if !chs.disarmed && chs.epoch != Some(lane.table.epoch) {
                ch_flush(agg, xk, ch, &mut mm.scratch)?;
                let chs = ch.as_mut().expect("just matched Some");
                chs.begin_epoch(lane.table.epoch, ndict);
            }
        }
        if xk.dg_epoch != Some(lane.table.epoch) {
            xk.dg_epoch = Some(lane.table.epoch);
            xk.dg_slots.clear();
            xk.dg_slots.resize(ndict, None);
        }
        // Code-histogram batch (lane-v2-codehist): count survivors per code
        // instead of probing + folding per row. Demote/Disarm verdicts fall
        // back byte-identically (ChVerdict doc).
        if ch.as_ref().is_some_and(|c| !c.disarmed) {
            let chs = ch.as_mut().expect("just checked Some");
            match ch_batch(chs, agg, xk, &lane, *coded, estate)? {
                ChVerdict::Counted => return Ok(()),
                ChVerdict::Demote => {
                    // Whole-batch per-row route: flush + memo drop as at
                    // every other per-row return (coded teardown inside).
                    return per_row_exit(agg, ss, xk, ch, mm, coded, n, estate);
                }
                ChVerdict::Disarm => {
                    let chs = ch.as_mut().expect("just checked Some");
                    chs.disarmed = true;
                    trace_feed("expr-key code-histogram disarmed (spill mode)");
                    // The batch's row-domain guard proof was skipped for
                    // the ch path, so it must not reach the dg fold leg:
                    // the universal per-row route runs it instead
                    // (byte-identical; spill rows take C's row path).
                    return per_row_exit(agg, ss, xk, ch, mm, coded, n, estate);
                }
            }
        }
        for k in 0..xk.rows.len() {
            let i = xk.rows[k];
            let code = lane.code(i as usize) as usize;
            debug_assert!(code < ndict, "filler contract: code < ndict");
            let pg = match xk.dg_slots[code] {
                Some(pg) => pg,
                None if *coded => {
                    // Coded-group resolve (q29coded): intern the memo's
                    // OUTPUT VALUE once per (epoch, code) — cross-epoch
                    // group identity is the intern table — and probe the
                    // compact mk1 table by the u32 id; never a text probe
                    // of the C tuplehash, never a spill leg (the budget
                    // peek above bounds the table). First-arrival order is
                    // the staged leg's exactly (same rows, same sequence).
                    // M2 sink DIRECT tables (arena-strings inc-3) probe on
                    // the canonical image instead — same bytes identity,
                    // same seed loop; the sink drive's flush invalidation
                    // (`invalidate_group_caches`) keeps this cache honest.
                    let key = xk.keys[k];
                    debug_assert!(!xk.knull[k], "coded batches were null-checked above");
                    // SAFETY: memo outputs are live non-null text varlenas
                    // for the staged window (dicteval arena contract).
                    let v =
                        unsafe { ::types_fmgr::datum_varlena_packed(key, estate.es_query_cxt) }?;
                    let pg = if ::nodeagg::agg_hash_compact_text_direct(agg) {
                        ::nodeagg::agg_hash_compact_probe_text_direct(agg, v.data())?
                    } else {
                        ::nodeagg::agg_hash_compact_probe_coded(agg, v.data())?
                    };
                    xk.dg_slots[code] = Some(pg);
                    pg
                }
                None => {
                    let (key, isnull) = (xk.keys[k], xk.knull[k]);
                    ::nodeagg::agg_hash_hash_staged(agg, &[key], &[isnull], &mut xk.hash1)?;
                    let hash = xk.hash1[0];
                    match ::nodeagg::agg_hash_probe_staged(agg, estate, key, isnull, hash)? {
                        Some(pg) => {
                            xk.dg_slots[code] = Some(pg);
                            pg
                        }
                        None => {
                            // Spill-mode miss: replay the projected row off
                            // the staged lanes + derived key and spill it;
                            // no transition runs. Deliberately NOT cached:
                            // every later row of the code must also spill.
                            spill_row(agg, ss, xk, stage_slot, i, key, isnull, hash, estate)?;
                            continue;
                        }
                    }
                }
            };
            idxs.push(i);
            groups.push(pg);
        }
    } else if *coded {
        // Raw window under the coded arm: per selected row through the same
        // intern+probe (raw windows are the rare non-dict chunks; the memo
        // scratch already carries per-row derived values). The epoch ended —
        // flush pending histogram counts first, the staged leg's discipline.
        ch_flush(agg, xk, ch, &mut mm.scratch)?;
        let direct = ::nodeagg::agg_hash_compact_text_direct(agg);
        for k in 0..xk.rows.len() {
            let i = xk.rows[k];
            let key = xk.keys[k];
            debug_assert!(!xk.knull[k], "coded batches were null-checked above");
            // SAFETY: per-row derived results are live non-null text
            // varlenas (the memo's Raw-window scratch fill).
            let v = unsafe { ::types_fmgr::datum_varlena_packed(key, estate.es_query_cxt) }?;
            let pg = if direct {
                ::nodeagg::agg_hash_compact_probe_text_direct(agg, v.data())?
            } else {
                ::nodeagg::agg_hash_compact_probe_coded(agg, v.data())?
            };
            idxs.push(i);
            groups.push(pg);
        }
    } else {
        // Raw window / arith key: batched hash pre-pass + in-order probe
        // (the K2 leg exactly, with the derived key lane). A Raw window for
        // the dict input column means its epoch ended — flush pending
        // histogram counts (always sound; see CodeHistState).
        ch_flush(agg, xk, ch, &mut mm.scratch)?;
        {
            let ExprKeyState { keys, knull, hashes, .. } = &mut *xk;
            ::nodeagg::agg_hash_hash_staged(agg, keys, knull, hashes)?;
        }
        for k in 0..xk.rows.len() {
            let i = xk.rows[k];
            let (key, isnull, hash) = (xk.keys[k], xk.knull[k], xk.hashes[k]);
            match ::nodeagg::agg_hash_probe_staged(agg, estate, key, isnull, hash)? {
                Some(pg) => {
                    idxs.push(i);
                    groups.push(pg);
                }
                None => {
                    spill_row(agg, ss, xk, stage_slot, i, key, isnull, hash, estate)?;
                }
            }
        }
    }
    // Residual transitions per probed row, in row order, over the projected
    // row rebuilt from the staged lanes + derived key (never the per-row
    // projection — that is the whole point for the dict class).
    if ::nodeagg::agg_lanefold_has_resid(agg) && !idxs.is_empty() {
        for k in 0..idxs.len() {
            let i = idxs[k];
            let slot_id = fill_stage_slot(
                agg,
                ss,
                xk,
                stage_slot,
                i,
                xk.key_vals[i as usize],
                xk.key_null[i as usize],
                estate,
            )?;
            ::nodeagg::agg_hash_build_resid_group(agg, estate, slot_id, groups[k])?;
        }
    }
    collect_mm_codes(ss, &mm.cols, &mut mm.codes);
    let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
        .expect("expr-key batched route requires the armed SoA");
    // SAFETY: as the compact arm above — non-fallback staged rows, valid
    // lane values for every mapped plan column, pergroups installed by this
    // batch's probes, guarded plans proven above, dict-code views per the
    // col_codes contract.
    unsafe {
        agg_fold_staged_mm(
            agg,
            &CodesCols { inner: &MapCols { soa, map: &xk.map }, codes: &mm.codes },
            idxs,
            groups,
            Some(&mut mm.scratch),
        )
    }
}

/// One multi-key packed batch (see [`MultiKeyChain`]): backstop check, the
/// computed key derived per survivor through the production fmgr chain,
/// the pack pre-pass over the survivors' component lanes (Int/Numeric from
/// base lanes, the derived numeric from the chain lane, text through the
/// per-epoch intern resolve), the packed compact-table probe, then the
/// whole-batch fold over the remapped lanes.
///
/// Demote discipline: EVERY demotion here happens BEFORE any probe or
/// transition ran for this batch — chain errors (refuse-and-replay: the
/// per-row replay raises C's exact error at C's exact row), NULL derived
/// keys, and unpackable numeric values (range / non-minimal display scale)
/// all disarm the compact table (migrating its groups to the C tuplehash)
/// and replay the WHOLE batch per-row, sticky thereafter.
#[allow(clippy::too_many_arguments)]
fn exprkey_mk_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    xk: &mut ExprKeyState,
    mk_shape: Option<&::nodeagg::MkShape>,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    n: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Not armed this build, or the runtime backstop migrated (before ANY
    // per-batch work — a migration never splits a batch): whole batch (and
    // every later one — the table stays disarmed) through the per-row leg.
    let armed = mk_shape.is_some() && ::nodeagg::agg_hash_compact_backstop(agg, estate)?;
    let Some(shape) = mk_shape.filter(|_| armed) else {
        return per_row_batch(agg, ss, n, estate);
    };
    debug_assert!(!shape.nullable, "the expr-key multi-key arm is cbstore-only (no null byte)");
    // Derive the computed key over the survivors. Errors: refuse-and-replay.
    let mut derive_err = false;
    let mut null_key = false;
    // CaseDict shapes skip the derive entirely: their computed component
    // evaluates inside the pack pre-pass (an intern id, not a datum lane).
    if !matches!(&xk.kind, ExprKeyKind::Multi(m) if m.case_dict.is_some()) {
        let ExprKeyState { kind, rows, key_vals, key_null, .. } = &mut *xk;
        let ExprKeyKind::Multi(m) = kind else {
            unreachable!("mk batch requires the Multi kind")
        };
        let chain = m.chain.as_mut().expect("non-CaseDict Multi shapes carry the chain");
        chain.reset();
        key_vals.clear();
        key_vals.resize(n as usize, ::datum::Datum::null());
        key_null.clear();
        key_null.resize(n as usize, true);
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
            .expect("expr-key batched route requires the armed SoA");
        let col = m.input_base as usize;
        let (values, isnull) = (soa.col_values(col), soa.col_isnull(col));
        if let Some(field) = m.fast {
            // Fast ts-extract kernel: non-erroring int64 field arithmetic
            // per survivor — `key_vals` carries RAW i64 field values (the
            // pack arm below reads them through `mk_numeric_i64_bits`, and
            // a demoted batch never reads them at all). Strict NULL and the
            // ±infinity sentinels take the chain's exact NULL-key arm (C
            // extracts NULL from a non-finite timestamp for these fields).
            for &i in rows.iter() {
                let i = i as usize;
                let t = values[i].as_i64();
                if isnull[i] || t == i64::MIN || t == i64::MAX {
                    null_key = true;
                } else {
                    key_vals[i] = ::datum::Datum::from_i64(ts_extract_apply(t, field));
                    key_null[i] = false;
                }
            }
        } else {
            for &i in rows.iter() {
                let i = i as usize;
                let input = ::datum::NullableDatum { value: values[i], isnull: isnull[i] };
                match chain.eval(input) {
                    Ok(nd) => {
                        key_vals[i] = nd.value;
                        key_null[i] = nd.isnull;
                        null_key |= nd.isnull;
                    }
                    Err(_) => {
                        // Discard the error: NO probe or transition ran; the
                        // per-row replay's exec_project raises C's exact
                        // error on C's exact row.
                        derive_err = true;
                        break;
                    }
                }
            }
        }
    }
    if derive_err || null_key {
        // NULL derived keys cannot pack without a null-bitmap byte
        // (pgrcolumnar shapes carry none): same demote as an error, minus the
        // replayed raise.
        xk.refused = true;
        trace_feed("expr-key multi-key demote: replaying batch per-row (sticky)");
        ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
        return per_row_batch(agg, ss, n, estate);
    }
    // Pack pre-pass, component-major over the survivors (scan_mk_batch's
    // shape, remapped: components address tlist attnos; base lanes come
    // through `map`, the computed component from the derived lane).
    let mut unpackable = false;
    {
        let ExprKeyState { kind, rows, key_vals, map, key_out, .. } = &mut *xk;
        let ExprKeyKind::Multi(m) = kind else {
            unreachable!("mk batch requires the Multi kind")
        };
        let fast_kernel = m.fast.is_some();
        let MultiKeyChain { case_dict, mks, .. } = &mut **m;
        let super::MkScratch { packbuf, keys1, epoch, code_ids, .. } = mks;
        packbuf.clear();
        packbuf.resize(rows.len(), 0u128);
        'comps: for comp in shape.comps.iter() {
            let att = comp.att;
            let off_bits = comp.off as u32 * 8;
            match comp.kind {
                ::nodeagg::MkCompKind::Numeric { width } if att == *key_out => {
                    // The derived key lane. Unpackable values demote —
                    // never a lossy pack (read-back byte-identity). Fast
                    // ts-extract batches carry RAW i64 field values: the
                    // integer pack produces the datum path's exact bits
                    // (`mk_numeric_i64_bits` ≡ pack of `int64_to_numeric`).
                    let fast = fast_kernel;
                    for (k, &i) in rows.iter().enumerate() {
                        let bits = if fast {
                            ::nodeagg::mk_numeric_i64_bits(key_vals[i as usize].as_i64(), width)
                        } else {
                            ::nodeagg::mk_numeric_datum_bits(key_vals[i as usize], width)
                        };
                        match bits {
                            Some(bits) => packbuf[k] |= (bits as u128) << off_bits,
                            None => {
                                unpackable = true;
                                break 'comps;
                            }
                        }
                    }
                }
                ::nodeagg::MkCompKind::Numeric { width } => {
                    // A bare-Var numeric key column from its base lane.
                    let base = map[att as usize].expect("Var keys map to base lanes") as usize;
                    let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                        .expect("expr-key batched route requires the armed SoA");
                    let (values, isnull) = (soa.col_values(base), soa.col_isnull(base));
                    for (k, &i) in rows.iter().enumerate() {
                        let i = i as usize;
                        debug_assert!(
                            !isnull[i],
                            "cbstore no-NULLs proof violated in a multi-key window"
                        );
                        match ::nodeagg::mk_numeric_datum_bits(values[i], width) {
                            Some(bits) => packbuf[k] |= (bits as u128) << off_bits,
                            None => {
                                unpackable = true;
                                break 'comps;
                            }
                        }
                    }
                }
                ::nodeagg::MkCompKind::Int { width } => {
                    let base = map[att as usize].expect("Var keys map to base lanes") as usize;
                    let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                        .expect("expr-key batched route requires the armed SoA");
                    let (values, isnull) = (soa.col_values(base), soa.col_isnull(base));
                    let mask = if width == 8 { u64::MAX } else { (1u64 << (width * 8)) - 1 };
                    for (k, &i) in rows.iter().enumerate() {
                        let i = i as usize;
                        debug_assert!(
                            !isnull[i],
                            "cbstore no-NULLs proof violated in a multi-key window"
                        );
                        let v = match width {
                            2 => values[i].as_i16() as i64,
                            4 => values[i].as_i32() as i64,
                            _ => values[i].as_i64(),
                        };
                        packbuf[k] |= (((v as u64) & mask) as u128) << off_bits;
                    }
                }
                ::nodeagg::MkCompKind::Intern if att == *key_out && case_dict.is_some() => {
                    // CaseDict computed key (band-2a): per-survivor int
                    // predicate mask, then select between the THEN column's
                    // per-(epoch, code) intern id and the memoized ELSE id.
                    let cd = case_dict.as_mut().expect("checked Some");
                    let mcx = estate.es_query_cxt;
                    let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                        .expect("expr-key batched route requires the armed SoA");
                    // Predicate pass (AND of int equalities; a NULL operand
                    // makes the WHEN not-true -> ELSE, C's CASE semantics).
                    cd.cond.clear();
                    cd.cond.resize(n as usize, true);
                    for &(base, cv, width) in &cd.preds {
                        let (values, isnull) =
                            (soa.col_values(base as usize), soa.col_isnull(base as usize));
                        for &i in rows.iter() {
                            let i = i as usize;
                            if !cd.cond[i] {
                                continue;
                            }
                            let v = match width {
                                2 => values[i].as_i16() as i64,
                                4 => values[i].as_i32() as i64,
                                _ => values[i].as_i64(),
                            };
                            cd.cond[i] = !isnull[i] && v == cv;
                        }
                    }
                    // ELSE id, memoized (cleared with the intern caches).
                    let else_id = match cd.else_id {
                        Some(id) => id,
                        None => {
                            let id =
                                ::nodeagg::agg_hash_compact_intern(agg, &cd.else_bytes);
                            cd.else_id = Some(id);
                            id
                        }
                    };
                    let lane = soa.dict_lane(cd.then_base as usize);
                    match lane {
                        Some(lane) => {
                            let ndict = lane.table.ndict as usize;
                            let global = lane.table.has_stitch();
                            let (ident, size) = if global {
                                ((true, lane.table.gepoch), lane.table.gndv as usize)
                            } else {
                                ((false, lane.table.epoch), ndict)
                            };
                            if cd.cd_epoch != Some(ident) {
                                cd.cd_epoch = Some(ident);
                                reset_code_id_cache(&mut cd.cd_code_ids, size);
                            }
                            for (k, &i) in rows.iter().enumerate() {
                                let id = if cd.cond[i as usize] {
                                    let local = lane.code(i as usize);
                                    debug_assert!(
                                        (local as usize) < ndict,
                                        "filler contract: code < ndict"
                                    );
                                    let code = if global {
                                        lane.table.global_code(local) as usize
                                    } else {
                                        local as usize
                                    };
                                    match cd.cd_code_ids[code] {
                                        c if c != 0 => c - 1,
                                        _ => {
                                            let d = lane.table.datum(local);
                                            // SAFETY: dict entries are live
                                            // non-null text varlenas for the
                                            // staged window (dict lane
                                            // contract).
                                            let v = unsafe {
                                                ::types_fmgr::datum_varlena_packed(d, mcx)
                                            }?;
                                            let id = ::nodeagg::agg_hash_compact_intern(
                                                agg,
                                                v.data(),
                                            );
                                            debug_assert!(id != u32::MAX, "id+1 encoding");
                                            cd.cd_code_ids[code] = id + 1;
                                            id
                                        }
                                    }
                                } else {
                                    else_id
                                };
                                packbuf[k] |= (id as u128) << off_bits;
                            }
                        }
                        None => {
                            // Raw-answered window: per-row intern of the
                            // THEN column on cond-true rows (correct,
                            // colder — the Intern arm's fallback rule).
                            let values = soa.col_values(cd.then_base as usize);
                            for (k, &i) in rows.iter().enumerate() {
                                let id = if cd.cond[i as usize] {
                                    let d = values[i as usize];
                                    // SAFETY: staged non-null live text
                                    // varlena (columnar fill; admission
                                    // proved the column type).
                                    let v = unsafe {
                                        ::types_fmgr::datum_varlena_packed(d, mcx)
                                    }?;
                                    ::nodeagg::agg_hash_compact_intern(agg, v.data())
                                } else {
                                    else_id
                                };
                                packbuf[k] |= (id as u128) << off_bits;
                            }
                        }
                    }
                }
                ::nodeagg::MkCompKind::Intern => {
                    let base = map[att as usize].expect("Var keys map to base lanes") as usize;
                    let mcx = estate.es_query_cxt;
                    let lane = ::nodeseqscan::seq_scan_batch_soa(ss)
                        .and_then(|soa| soa.dict_lane(base));
                    match lane {
                        Some(lane) => {
                            // Code → intern-id resolve (the scan feed's
                            // exact cache): per-epoch (RG-rolled), or under
                            // a v7 stitch keyed on part-global codes and
                            // the scan-stable gepoch (never re-rolled).
                            let ndict = lane.table.ndict as usize;
                            let global = lane.table.has_stitch();
                            let (ident, size) = if global {
                                ((true, lane.table.gepoch), lane.table.gndv as usize)
                            } else {
                                ((false, lane.table.epoch), ndict)
                            };
                            if *epoch != Some(ident) {
                                *epoch = Some(ident);
                                reset_code_id_cache(code_ids, size);
                            }
                            debug_assert!(code_ids.len() >= size);
                            for (k, &i) in rows.iter().enumerate() {
                                let local = lane.code(i as usize);
                                debug_assert!(
                                    (local as usize) < ndict,
                                    "filler contract: code < ndict"
                                );
                                let code = if global {
                                    lane.table.global_code(local) as usize
                                } else {
                                    local as usize
                                };
                                debug_assert!(code < size, "stitch contract: code < gndv");
                                let id = match code_ids[code] {
                                    c if c != 0 => c - 1,
                                    _ => {
                                        let d = lane.table.datum(local);
                                        // SAFETY: dict entries are live
                                        // non-null text varlenas for the
                                        // staged window (dict lane
                                        // contract; kernel selection proved
                                        // the column type).
                                        let v = unsafe {
                                            ::types_fmgr::datum_varlena_packed(d, mcx)
                                        }?;
                                        let id = ::nodeagg::agg_hash_compact_intern(
                                            agg,
                                            v.data(),
                                        );
                                        debug_assert!(id != u32::MAX, "id+1 encoding");
                                        code_ids[code] = id + 1;
                                        id
                                    }
                                };
                                packbuf[k] |= (id as u128) << off_bits;
                            }
                        }
                        None => {
                            // Raw-answered window: per-row intern (correct,
                            // colder — the scan feed's fallback rule).
                            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                                .expect("expr-key batched route requires the armed SoA");
                            let values = soa.col_values(base);
                            debug_assert!(
                                rows.iter().all(|&i| !soa.col_isnull(base)[i as usize]),
                                "cbstore no-NULLs proof violated in a multi-key window"
                            );
                            for (k, &i) in rows.iter().enumerate() {
                                let d = values[i as usize];
                                // SAFETY: staged non-null live text varlena
                                // (columnar fill stages decoded datums;
                                // kernel selection proved the column type).
                                let v =
                                    unsafe { ::types_fmgr::datum_varlena_packed(d, mcx) }?;
                                let id = ::nodeagg::agg_hash_compact_intern(agg, v.data());
                                packbuf[k] |= (id as u128) << off_bits;
                            }
                        }
                    }
                }
            }
        }
        if !unpackable && !shape.two_words {
            // One-word shapes narrow u128 -> i64 (a real stride change).
            // Two-word shapes probe the accumulator in place at the probe
            // block below (mk_keys2_lane — mkaccept inc-1).
            keys1.clear();
            keys1.extend(packbuf.iter().map(|&w| w as u64 as i64));
        }
    }
    if unpackable {
        xk.refused = true;
        trace_feed("expr-key multi-key demote: numeric key unpackable (sticky)");
        ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
        return per_row_batch(agg, ss, n, estate);
    }
    // Packed probe + whole-batch fold over the remapped lanes.
    {
        let ExprKeyState { kind, rows, .. } = &mut *xk;
        let ExprKeyKind::Multi(m) = kind else {
            unreachable!("mk batch requires the Multi kind")
        };
        if shape.two_words {
            let super::MkScratch { packbuf, keys2, .. } = &mut m.mks;
            let lane = ::nodeagg::mk_keys2_lane(packbuf, keys2);
            ::nodeagg::agg_hash_compact_batch_mk2(agg, lane, groups)?;
        } else {
            ::nodeagg::agg_hash_compact_batch_mk1(agg, &m.mks.keys1, groups)?;
        }
        idxs.clear();
        idxs.extend_from_slice(rows);
    }
    let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
        .expect("expr-key batched route requires the armed SoA");
    // SAFETY: every probed row is a non-fallback staged row with valid lane
    // values for every mapped plan column (a dict component is never in
    // `plan.cols` — admission); the plan is unguarded (admission); each
    // pergroup was installed by the packed compact probe within this batch;
    // the rest is agg_fold_staged's contract.
    unsafe { agg_fold_staged(agg, &MapCols { soa, map: &xk.map }, idxs, groups) }
}

/// One staged batch of the REDUCED (redundant-key) route: range-guard the
/// representative lane, prove any plan guards, probe the compact table on
/// the representative alone, and fold whole-batch. Every demote (range
/// trap, guard demote, backstop migration) replays the WHOLE batch through
/// the per-row emit path — the C-ported `exec_project` computes (and, for
/// out-of-range keys, ERRORS on) every derived key at exactly the per-row
/// path's row — and the range trap and migration are STICKY (the compact
/// table is the only reduced host; once it is gone the C table needs all
/// key columns per arrival).
#[allow(clippy::too_many_arguments)]
fn reduced_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    xk: &mut ExprKeyState,
    sel: &[u64],
    nwords: usize,
    n: u32,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let ExprKeyKind::Reduced { ref shape, rep_att, lo, hi } = xk.kind else {
        unreachable!("reduced_batch requires the Reduced kind")
    };
    let width = shape.width;
    // Survivor-aligned representative keys + the overflow range guard: a
    // selected value outside [lo, hi] means some derived key errors on this
    // batch — refuse-and-replay per-row, sticky (arith-trap discipline).
    {
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
            .expect("reduced batched route requires the armed SoA");
        let vals = soa.col_values(rep_att as usize);
        let nulls = soa.col_isnull(rep_att as usize);
        xk.keys.clear();
        xk.knull.clear();
        let (mut mn, mut mx) = (i64::MAX, i64::MIN);
        for &i in &xk.rows {
            let isnull = nulls[i as usize];
            let d = vals[i as usize];
            if !isnull {
                let v = match width {
                    2 => d.as_i16() as i64,
                    4 => d.as_i32() as i64,
                    _ => d.as_i64(),
                };
                mn = mn.min(v);
                mx = mx.max(v);
            }
            xk.keys.push(d);
            xk.knull.push(isnull);
        }
        if mn <= mx && (mn < lo || mx > hi) {
            xk.refused = true;
            trace_feed("reduced-key range trap: replaying batch per-row (sticky)");
            ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
            return per_row_batch(agg, ss, n, estate);
        }
    }
    // Guarded plans: prove the survivors before any fold (main-feed
    // discipline over the remapped lanes).
    {
        let plan = ::nodeagg::agg_lanefold_plan(agg).expect("reduced feed without a plan");
        if plan.guarded {
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("reduced batched route requires the armed SoA");
            // SAFETY: selected rows are staged non-fallback rows with live
            // lane values for every mapped plan column.
            let demote = unsafe {
                ::lanefold::check_guards(
                    plan,
                    &MapCols { soa, map: &xk.map },
                    &sel[..nwords],
                    |_| None,
                )
            } == ::lanefold::GuardCheck::Demote;
            if demote {
                ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
                return per_row_batch(agg, ss, n, estate);
            }
        }
    }
    // Probe the compact table on the representative lane. `false` = the
    // runtime backstop migrated to the C table BEFORE this batch (or a
    // prior one did): sticky per-row from here on.
    {
        let ExprKeyState { keys, knull, .. } = &mut *xk;
        if !::nodeagg::agg_hash_compact_batch(agg, estate, keys, knull, groups)? {
            xk.refused = true;
            trace_feed("reduced-key backstop migration: per-row from here (sticky)");
            return per_row_batch(agg, ss, n, estate);
        }
    }
    idxs.clear();
    idxs.extend_from_slice(&xk.rows);
    let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
        .expect("reduced batched route requires the armed SoA");
    // SAFETY: every probed row is non-fallback with valid lane values for
    // every mapped plan column; each pergroup was installed by the compact
    // probe within this batch; the rest is agg_fold_staged's contract.
    unsafe { agg_fold_staged(agg, &MapCols { soa, map: &xk.map }, idxs, groups) }
}

/// Whole-batch per-row route: the arrival loop over `seq_scan_batch_emit`
/// (per-tuple context reset, store, qual, per-row `exec_project` — C's exact
/// error at C's exact row), every row through the FULL per-row transition
/// program (`agg_hash_build_accept`). The demote discipline verbatim: never
/// mix a partial batched fold with per-row transitions inside one batch, and
/// never fold a guarded plan this route did not prove.
fn per_row_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    n: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Emit-dead word skip over the staged qual bitmap: a cleared skip-sel
    // bit is a row the emit rejects with no observable effect (definitive
    // even under requal) — same accepted rows, same order, same errors.
    let skip = {
        let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
        ::nodeseqscan::seq_scan_batch_skip_sel(ss).map(|s| {
            w[..s.len()].copy_from_slice(s);
            w
        })
    };
    ::exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), 0, n, |i| -> PgResult<()> {
        if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
            ::nodeagg::agg_hash_build_accept(agg, estate, slot)?;
        }
        Ok(())
    })
}

/// Rebuild the projected row in the memoized stage slot: needed columns from
/// their base lanes, the key column from the derived lane, everything else
/// NULL (the spill projection's own treatment). Descriptor = the projection
/// RESULT slot's (the agg's input space).
#[allow(clippy::too_many_arguments)]
fn fill_stage_slot<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    xk: &ExprKeyState,
    stage_slot: &mut Option<ExecSlotId>,
    i: u32,
    key: ::datum::Datum,
    key_isnull: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ExecSlotId> {
    let slot_id = match *stage_slot {
        Some(s) => s,
        None => {
            let desc = estate
                .slot(
                    ss.ss
                        .ps_ProjInfo
                        .as_ref()
                        .expect("expr-key feed requires a projected scan")
                        .pi_result_slot,
                )
                .base()
                .tts_tupleDescriptor
                .clone();
            let s = estate.exec_init_extra_tuple_slot(desc, ::types_slot::TupleSlotKind::Virtual);
            *stage_slot = Some(s);
            s
        }
    };
    let (colnos_needed, _) = ::nodeagg::agg_hash_needed_cols(agg);
    let mcx = estate.es_query_cxt;
    let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
        .expect("expr-key batched route requires the armed SoA");
    let slot = estate.slot_mut(slot_id);
    ::exectuples::exec_clear_tuple(slot, mcx);
    let base = slot.base_mut();
    for c in 0..xk.natts {
        base.tts_values[c] = ::datum::Datum::null();
        base.tts_isnull[c] = true;
    }
    for (c, &need) in colnos_needed.iter().enumerate() {
        if !need {
            continue;
        }
        if c == xk.key_out as usize {
            base.tts_values[c] = key;
            base.tts_isnull[c] = key_isnull;
        } else {
            let b = xk.map[c].expect("needed columns admitted as bare Vars") as usize;
            base.tts_values[c] = soa.col_values(b)[i as usize];
            base.tts_isnull[c] = soa.col_isnull(b)[i as usize];
        }
    }
    ::exectuples::exec_store_virtual_tuple(slot);
    Ok(slot_id)
}

/// Spill-mode miss: replay the projected row and spill it byte-identically
/// (`hashagg_spill_tuple` materializes the slot, so derived-key datums with
/// epoch/batch lifetime are long enough by construction).
#[cold]
#[allow(clippy::too_many_arguments)]
fn spill_row<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    xk: &ExprKeyState,
    stage_slot: &mut Option<ExecSlotId>,
    i: u32,
    key: ::datum::Datum,
    key_isnull: bool,
    hash: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let slot_id = fill_stage_slot(agg, ss, xk, stage_slot, i, key, key_isnull, estate)?;
    ::nodeagg::agg_hash_spill_staged(agg, estate, slot_id, hash)
}

#[cfg(test)]
mod ts_trunc_tests {
    use super::ts_trunc_apply;

    /// The ts-trunc kernel vs the C-ported `timestamp_trunc` oracle, over
    /// every admitted unit: boundary cases (±infinity sentinels, the unit
    /// boundaries around 0 = 2000-01-01, pre-2000 negatives) plus a
    /// deterministic LCG sweep across the storable range. Any storable
    /// value the oracle truncates, the kernel must match bit-for-bit; the
    /// oracle erroring (out-of-range decode) on a value proves that value
    /// is not storable, so the kernel's answer there is unreachable.
    #[test]
    fn ts_trunc_matches_timestamp_trunc() {
        let units: [(&[u8], i64); 4] = [
            (b"second", 1_000_000),
            (b"minute", 60_000_000),
            (b"hour", 3_600_000_000),
            (b"day", 86_400_000_000),
        ];
        let mut cases: Vec<i64> = vec![
            0,
            1,
            -1,
            59_999_999,
            60_000_000,
            60_000_001,
            -59_999_999,
            -60_000_000,
            -60_000_001,
            86_399_999_999,
            86_400_000_000,
            -86_400_000_000,
            -86_400_000_001,
            // 2013-era timestamp values in the analytics bank (µs since 2000-01-01).
            426_038_400_000_000 + 12 * 3_600_000_000 + 34 * 60_000_000 + 56_789_012,
            i64::MIN, // DT_NOBEGIN: passes through
            i64::MAX, // DT_NOEND: passes through
        ];
        // Deterministic LCG sweep over the full i64 space; out-of-range
        // values are skipped when the oracle refuses them (not storable).
        let mut x: u64 = 0x9e3779b97f4a7c15;
        for _ in 0..20_000 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            cases.push(x as i64);
            // Bias half the sweep into the valid timestamp band (±~9e15 µs
            // covers 1715-2285 AD) so most samples exercise the kernel.
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            cases.push((x as i64) % 9_000_000_000_000_000);
        }
        for &(name, usecs) in &units {
            for &t in &cases {
                match ::adt_timestamp::timestamp_trunc(name, t) {
                    Ok(oracle) => {
                        assert_eq!(
                            ts_trunc_apply(t, usecs),
                            oracle,
                            "unit={} t={}",
                            String::from_utf8_lossy(name),
                            t
                        );
                    }
                    Err(_) => {
                        // Out-of-range decode: not a storable timestamp.
                        assert!(
                            t != 0 && t != i64::MIN && t != i64::MAX,
                            "oracle refused an in-range probe t={t}"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod ts_extract_tests {
    use super::{ts_extract_apply, TsPartField};

    /// The ts-extract kernel vs the C-ported `timestamp_part_common` oracle
    /// (retnumeric = the EXTRACT surface), over both admitted fields:
    /// boundary cases around 0 = 2000-01-01 (pre-2000 negatives exercise the
    /// euclidean remainder), the ±infinity sentinels (oracle: NULL — the
    /// feed's NULL-key demote arm, kernel never called), plus a
    /// deterministic LCG sweep. The oracle's NUMERIC must equal
    /// `int64_to_numeric(kernel(t))` BYTE-identically — that is exactly the
    /// datum the read-back leg reconstructs from the packed bits
    /// (`mk_numeric_i64_bits` ≡ datum-pack of `int64_to_numeric`, proven in
    /// nodeagg's compact tests).
    #[test]
    fn ts_extract_matches_timestamp_part() {
        let fields: [(&[u8], TsPartField); 2] =
            [(b"minute", TsPartField::Minute), (b"hour", TsPartField::Hour)];
        let mut cases: Vec<i64> = vec![
            0,
            1,
            -1,
            59_999_999,
            60_000_000,
            60_000_001,
            -59_999_999,
            -60_000_000,
            -60_000_001,
            3_599_999_999,
            3_600_000_000,
            -3_600_000_000,
            -3_600_000_001,
            86_399_999_999,
            86_400_000_000,
            -86_400_000_000,
            -86_400_000_001,
            // 2013-era timestamp values in the analytics bank (µs since 2000-01-01).
            426_038_400_000_000 + 12 * 3_600_000_000 + 34 * 60_000_000 + 56_789_012,
        ];
        let mut x: u64 = 0x9e3779b97f4a7c15;
        for _ in 0..20_000 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            cases.push(x as i64);
            // Bias half the sweep into the valid timestamp band.
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            cases.push((x as i64) % 9_000_000_000_000_000);
        }
        for &(name, field) in &fields {
            for &t in &cases {
                match ::adt_timestamp::timestamp_part_common(name, t, true) {
                    Ok(::adt_timestamp::PartValue::Numeric(img)) => {
                        let ours = ::adt_numeric::int64_to_numeric(ts_extract_apply(t, field));
                        assert_eq!(
                            ours.as_bytes(),
                            img.as_bytes(),
                            "field={} t={}",
                            String::from_utf8_lossy(name),
                            t
                        );
                    }
                    Ok(::adt_timestamp::PartValue::Null) => {
                        // C's non-finite arm: the feed demotes NULL keys
                        // per-row BEFORE the kernel runs; assert the arm is
                        // exactly the sentinels the derive loop tests for.
                        assert!(
                            t == i64::MIN || t == i64::MAX,
                            "oracle returned NULL for a finite probe t={t}"
                        );
                    }
                    Ok(::adt_timestamp::PartValue::Float(_)) => {
                        panic!("retnumeric oracle returned a float (t={t})")
                    }
                    Err(_) => {
                        // Out-of-range decode: not a storable timestamp, the
                        // kernel's answer there is unreachable.
                        assert!(
                            t != 0 && t != i64::MIN && t != i64::MAX,
                            "oracle refused an in-range probe t={t}"
                        );
                    }
                }
            }
        }
        // Sentinels are the NULL arm.
        for t in [i64::MIN, i64::MAX] {
            for &(name, _) in &fields {
                assert!(matches!(
                    ::adt_timestamp::timestamp_part_common(name, t, true),
                    Ok(::adt_timestamp::PartValue::Null)
                ));
            }
        }
    }
}
