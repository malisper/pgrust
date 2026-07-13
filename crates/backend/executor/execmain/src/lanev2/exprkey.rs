//! Expression group keys (expr-key tranche): host `Agg(hashed) → SeqScan`
//! builds whose scan PROJECTS the (single) grouping key as a computed
//! expression — the shape `decide_agg_lane`'s `ps_ProjInfo.is_none()` gate
//! refused to the per-row breaker feed until now.
//!
//! Two admission classes, one feed:
//!
//! * **Int-expression keys** (ClickBench Q19-class): the key is int2/4/8
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
//! * **Dict-expression keys** (Q29-class): the key is a strict fmgr chain
//!   over ONE dict-coded cbstore text column (`ScanProjExprKey` census →
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
//! per-row projection: this is what lets Q29's `avg(length(Referer))` leg
//! ride along while the regexp key stays once-per-code.
//!
//! Everything outside the vocabulary refuses (`RefuseReason::ExprKeyShape`)
//! to the per-row breaker feed, byte-identically. Kill switch:
//! `PGRUST_LANE_V2_EXPRKEY=0|off`.

use std::sync::OnceLock;

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

use super::{
    agg_fold_staged_mm, collect_mm_codes, mm_str_cols, stats, trace_feed, CodesCols,
    RefuseReason, ShapeClass,
};

/// Kill switch (default ON inside the lane; `PGRUST_LANE_V2` still gates
/// every caller).
fn exprkey_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_EXPRKEY").as_deref(), Ok("0") | Ok("off"))
    })
}

const TEXTOID: ::types_core::Oid = 25;
const VARCHAROID: ::types_core::Oid = 1043;

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
        // Dict class: cbstore text column, IMMUTABLE internal builtins
        // (dicteval's fail-closed compile owns the catalog gate).
        if xk.n as usize != natts
            || xk.key_out != key_out
            || !::nodeseqscan::seq_scan_is_cbstore(ss)
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
        // The chain's result type must be the grouping key's column type
        // (defense in depth — the tupledesc is plan authority).
        let keytype = estate.slot(result_slot).base().tts_tupleDescriptor.as_ref()?.attrs
            [key_out as usize]
            .atttypid;
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
        ExprKeyKind::Arith { ncols, .. } => prefix = prefix.max(*ncols as i32),
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
    // PREWHERE lane first on qual'd cbstore scans, then the columnar /
    // fixed-width-prefix deform. A refusing arm fails open to per-row.
    let armed = if ::nodeseqscan::seq_scan_is_cbstore(ss) {
        if ss.ss.qual.is_some() {
            match ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, prefix) {
                Ok(true) => {}
                Ok(false) | Err(_) => {}
            }
        }
        let dict_key = match &kind {
            ExprKeyKind::Dict { input_col, .. } => Some(*input_col),
            ExprKeyKind::Arith { .. } => None,
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
        ExprKeyKind::Dict { .. } => "agg-over-seqscan: expr-key feed armed (dict key)",
    });
    Some(Box::new(ExprKeyState {
        natts,
        map,
        key_out,
        prefix,
        kind,
        refused: false,
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

/// Re-arm the staging for a build (idempotent — the decide-phase probe armed
/// the identical shape; rescans re-enter here).
pub(super) fn exprkey_rearm<'mcx>(
    xk: &ExprKeyState,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    if ::nodeseqscan::seq_scan_is_cbstore(ss) {
        if ss.ss.qual.is_some() {
            let _ = ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, xk.prefix);
        }
        let dict_key = match &xk.kind {
            ExprKeyKind::Dict { input_col, .. } => Some(*input_col),
            ExprKeyKind::Arith { .. } => None,
        };
        ::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, xk.prefix, dict_key)
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
    let compact = !has_resid
        && matches!(xk.kind, ExprKeyKind::Arith { .. })
        && match ::nodeagg::agg_hash_compact_try_arm(agg) {
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
        };
    trace_feed(if compact {
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
    loop {
        let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
        if n == 0 {
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        exprkey_batch(
            agg, ss, xk, stage_slot, compact, &mut idxs, &mut groups, &mut mm, &mut ch, n,
            estate,
        )?;
    }
    // Pending histogram counts flush at feed end (before the phase flip).
    ch_flush(agg, xk, &mut ch, &mut mm.scratch)?;
    ::nodeagg::agg_hash_build_finish(agg, estate)
}

/// `PGRUST_LANE_V2_CODEHIST=0|off` kill switch (default ON inside the lane).
fn codehist_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
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
        unsafe { ::lanefold::fold_code_group(plan, vals, strd, n, pg, aggcx)? };
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
    // row order — the dg leg's exact probe sequence.
    for (k, &code) in ch.rowcodes.iter().enumerate() {
        let c = code as usize;
        if xk.dg_slots[c].is_none() {
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
fn exprkey_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    xk: &mut ExprKeyState,
    stage_slot: &mut Option<ExecSlotId>,
    compact: bool,
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
        ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
        return {
            // Whole-batch per-row route: str advances bypass the memo, and
            // pending histogram counts flush first (always sound; the
            // permuted advance order is byte-invisible on transvalues).
            ch_flush(agg, xk, ch, &mut mm.scratch)?;
            mm.scratch.invalidate();
            per_row_batch(agg, ss, n, estate)
        };
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
    // Key-lane derivation.
    let mut dict_lane: Option<::exectuples::SoaDictLane> = None;
    match &mut xk.kind {
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
            let mut sv = ::lanestitch::SelVec::all(n);
            for i in 0..n {
                if sel[(i / 64) as usize] & (1u64 << (i % 64)) == 0 {
                    sv.clear(i);
                }
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
                ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
                return {
            // Whole-batch per-row route: str advances bypass the memo, and
            // pending histogram counts flush first (always sound; the
            // permuted advance order is byte-invisible on transvalues).
            ch_flush(agg, xk, ch, &mut mm.scratch)?;
            mm.scratch.invalidate();
            per_row_batch(agg, ss, n, estate)
        };
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
                        ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
                        return {
            // Whole-batch per-row route: str advances bypass the memo, and
            // pending histogram counts flush first (always sound; the
            // permuted advance order is byte-invisible on transvalues).
            ch_flush(agg, xk, ch, &mut mm.scratch)?;
            mm.scratch.invalidate();
            per_row_batch(agg, ss, n, estate)
        };
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
                ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
                return {
            // Whole-batch per-row route: str advances bypass the memo, and
            // pending histogram counts flush first (always sound; the
            // permuted advance order is byte-invisible on transvalues).
            ch_flush(agg, xk, ch, &mut mm.scratch)?;
            mm.scratch.invalidate();
            per_row_batch(agg, ss, n, estate)
        };
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
            match ch_batch(chs, agg, xk, &lane, estate)? {
                ChVerdict::Counted => return Ok(()),
                ChVerdict::Demote => {
                    ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
                    return {
                        // Whole-batch per-row route: flush + memo drop as at
                        // every other per-row return.
                        ch_flush(agg, xk, ch, &mut mm.scratch)?;
                        mm.scratch.invalidate();
                        per_row_batch(agg, ss, n, estate)
                    };
                }
                ChVerdict::Disarm => {
                    let chs = ch.as_mut().expect("just checked Some");
                    chs.disarmed = true;
                    trace_feed("expr-key code-histogram disarmed (spill mode)");
                    ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
                    return {
                        // The batch's row-domain guard proof was skipped for
                        // the ch path, so it must not reach the dg fold leg:
                        // the universal per-row route runs it instead
                        // (byte-identical; spill rows take C's row path).
                        ch_flush(agg, xk, ch, &mut mm.scratch)?;
                        mm.scratch.invalidate();
                        per_row_batch(agg, ss, n, estate)
                    };
                }
            }
        }
        for k in 0..xk.rows.len() {
            let i = xk.rows[k];
            let code = lane.code(i as usize) as usize;
            debug_assert!(code < ndict, "filler contract: code < ndict");
            let pg = match xk.dg_slots[code] {
                Some(pg) => pg,
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
    for i in 0..n {
        if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
            ::nodeagg::agg_hash_build_accept(agg, estate, slot)?;
        }
    }
    Ok(())
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
