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

use super::{agg_fold_staged, stats, trace_feed, RefuseReason, ShapeClass};

/// Kill switch (default ON inside the lane; `PGRUST_LANE_V2` still gates
/// every caller).
fn exprkey_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_EXPRKEY").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Kill switch for the redundant-key (reduced grouping) tranche —
/// independent of the single-computed-key arms above.
fn redkey_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_REDKEY").as_deref(), Ok("0") | Ok("off"))
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

/// Multi-key packed state (the Q19-class arm): a projected scan whose tlist
/// is bare Vars plus EXACTLY ONE computed column — a strict fmgr chain over
/// one base scan column (`ScanProjExprKey` census) — where the agg groups by
/// 2..N keys including the computed one. The computed key's grouping kind
/// must be NUMERIC (`extract(minute FROM ts)`-class): its values derive per
/// surviving row through the production fmgr and pack via the canonical
/// numeric key form (`nodeagg::mk_numeric_datum_bits`); every other key is a
/// bare-Var component packed from its base lane (Int/Numeric) or the
/// dict/intern lane (TextRaw, cbstore). Unpackable numeric values (range /
/// non-minimal display scale) DEMOTE: the compact table migrates to the C
/// tuplehash and the batch replays per-row — never a lossy pack.
pub(super) struct MultiKeyChain {
    /// The computed key's chain (production fmgr entry points).
    pub(super) chain: ::laneexec::ValueChain,
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
    /// Packed multi-key over a projected scan (see [`MultiKeyChain`]).
    Multi(Box<MultiKeyChain>),
    /// Redundant grouping-key elimination (Q36 class): 2..N int grouping
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
        // 2..N grouping keys: the redundant-key (reduced grouping) tranche
        // first (Q36 class — every non-representative key a Var ± Const
        // function of the one bare-Var key; its own refuse accounting ticks
        // inside), then the packed multi-key arm (Q19-class). Its refusals
        // tick the multikey taxonomy inside.
        if let Some(xk) = decide_reduced(agg, ss, estate) {
            return Some(xk);
        }
        if ::nodeagg::agg_hash_key_cols(agg).len() >= 2 {
            return decide_exprkey_mk(agg, ss, estate);
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
        ExprKeyKind::Multi(_) => unreachable!("multi-key shapes decide in decide_exprkey_mk"),
        ExprKeyKind::Reduced { .. } => {
            unreachable!("the reduced kind decides in decide_reduced")
        }
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
            ExprKeyKind::Arith { .. } | ExprKeyKind::Multi(_) | ExprKeyKind::Reduced { .. } => {
                None
            }
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

/// The multi-key packed decide (Q19-class; see [`MultiKeyChain`]): mirrors
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
    // v1: cbstore only — text key components need dict lanes and the
    // offset-free columnar arm stages every base component as decoded
    // datums (a heap fixed-width prefix cannot stage varlena keys).
    if !::nodeseqscan::seq_scan_is_cbstore(ss) {
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
    trace_feed("agg-over-seqscan: expr-key feed armed (multi-key packed)");
    Some(Box::new(ExprKeyState {
        natts,
        map,
        key_out,
        prefix,
        kind: ExprKeyKind::Multi(Box::new(MultiKeyChain {
            chain,
            input_base: xk.input_col,
            dict_input_att,
            dict_base,
            mks: super::MkScratch::default(),
        })),
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

/// The reduced-grouping (redundant-key) admission: `Agg(hashed) → SeqScan`
/// with 2..N int grouping keys over a projected scan where EXACTLY ONE key
/// is a bare Var (the representative) and every other key is same-width
/// `Var ± Const` int arithmetic over that Var — grouping by the reduced set
/// {representative} produces the identical partition, so the build probes
/// one int lane and reconstructs the redundant keys once per GROUP at
/// read-back (the compact table's `RedShape` emit spec) instead of packing
/// or evaluating them per row. ClickBench Q36 exactly:
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
/// PREWHERE lane first on qual'd cbstore scans, then the columnar /
/// fixed-width-prefix deform. A refusing arm fails open to per-row.
fn arm_stage<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    prefix: i32,
    dict_key: Option<u16>,
) -> bool {
    if ::nodeseqscan::seq_scan_is_cbstore(ss) {
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
            ExprKeyKind::Multi(m) => m.dict_base,
            ExprKeyKind::Arith { .. } | ExprKeyKind::Reduced { .. } => None,
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
        ExprKeyKind::Arith { .. } if !has_resid => {
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
    // Multi-key arm: the packed compact table arms per build (mirrors the
    // scan feed's scan_mk_shape sequence, which also re-decides per build).
    // A non-armed build (spill risk under the current limits) runs whole
    // batches per-row — the arrival machinery, byte-identical.
    let mk_shape: Option<::nodeagg::MkShape> = if let ExprKeyKind::Multi(m) = &xk.kind {
        match ::nodeagg::agg_hash_compact_try_arm_mk(agg, false, m.dict_input_att) {
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
    // Fresh per-build epoch map (rescans must not reuse stale pergroups).
    xk.dg_epoch = None;
    xk.dg_slots.clear();
    // Same for the multi-key intern cache: rescans rebuild the compact +
    // intern tables, so cached code -> intern-id entries are stale.
    if let ExprKeyKind::Multi(m) = &mut xk.kind {
        m.mks.epoch = None;
        m.mks.code_ids.clear();
    }
    loop {
        let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
        if n == 0 {
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        exprkey_batch(
            agg,
            ss,
            xk,
            stage_slot,
            compact,
            mk_shape.as_ref(),
            &mut idxs,
            &mut groups,
            n,
            estate,
        )?;
    }
    ::nodeagg::agg_hash_build_finish(agg, estate)
}

/// One staged batch. See `exprkey_build_fold_feed` for the routing rules.
#[allow(clippy::too_many_arguments)]
fn exprkey_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    xk: &mut ExprKeyState,
    stage_slot: &mut Option<ExecSlotId>,
    compact: bool,
    mk_shape: Option<&::nodeagg::MkShape>,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
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
        return per_row_batch(agg, ss, n, estate);
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
                return per_row_batch(agg, ss, n, estate);
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
                        return per_row_batch(agg, ss, n, estate);
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
    {
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
                return per_row_batch(agg, ss, n, estate);
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
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("expr-key batched route requires the armed SoA");
            // SAFETY: every probed row is non-fallback with valid lane
            // values for every mapped plan column; each pergroup was
            // installed by the compact probe within this batch; the rest is
            // agg_fold_staged's contract.
            return unsafe { agg_fold_staged(agg, &MapCols { soa, map: &xk.map }, idxs, groups) };
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
        if xk.dg_epoch != Some(lane.table.epoch) {
            xk.dg_epoch = Some(lane.table.epoch);
            xk.dg_slots.clear();
            xk.dg_slots.resize(ndict, None);
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
        // (the K2 leg exactly, with the derived key lane).
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
    let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
        .expect("expr-key batched route requires the armed SoA");
    // SAFETY: as the compact arm above — non-fallback staged rows, valid
    // lane values for every mapped plan column, pergroups installed by this
    // batch's probes, guarded plans proven above.
    unsafe { agg_fold_staged(agg, &MapCols { soa, map: &xk.map }, idxs, groups) }
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
    {
        let ExprKeyState { kind, rows, key_vals, key_null, .. } = &mut *xk;
        let ExprKeyKind::Multi(m) = kind else {
            unreachable!("mk batch requires the Multi kind")
        };
        m.chain.reset();
        key_vals.clear();
        key_vals.resize(n as usize, ::datum::Datum::null());
        key_null.clear();
        key_null.resize(n as usize, true);
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
            .expect("expr-key batched route requires the armed SoA");
        let col = m.input_base as usize;
        let (values, isnull) = (soa.col_values(col), soa.col_isnull(col));
        for &i in rows.iter() {
            let i = i as usize;
            let input = ::datum::NullableDatum { value: values[i], isnull: isnull[i] };
            match m.chain.eval(input) {
                Ok(nd) => {
                    key_vals[i] = nd.value;
                    key_null[i] = nd.isnull;
                    null_key |= nd.isnull;
                }
                Err(_) => {
                    // Discard the error: NO probe or transition ran; the
                    // per-row replay's exec_project raises C's exact error
                    // on C's exact row.
                    derive_err = true;
                    break;
                }
            }
        }
    }
    if derive_err || null_key {
        // NULL derived keys cannot pack without a null-bitmap byte
        // (cbstore shapes carry none): same demote as an error, minus the
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
        let super::MkScratch { packbuf, keys1, keys2, epoch, code_ids, .. } = &mut m.mks;
        packbuf.clear();
        packbuf.resize(rows.len(), 0u128);
        'comps: for comp in shape.comps.iter() {
            let att = comp.att;
            let off_bits = comp.off as u32 * 8;
            match comp.kind {
                ::nodeagg::MkCompKind::Numeric { width } if att == *key_out => {
                    // The derived key lane. Unpackable values demote —
                    // never a lossy pack (read-back byte-identity).
                    for (k, &i) in rows.iter().enumerate() {
                        match ::nodeagg::mk_numeric_datum_bits(key_vals[i as usize], width) {
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
                                code_ids.clear();
                                code_ids.resize(size, None);
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
                                    Some(id) => id,
                                    None => {
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
                                        code_ids[code] = Some(id);
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
        if !unpackable {
            if shape.two_words {
                keys2.clear();
                keys2.extend(packbuf.iter().map(|&w| [w as u64, (w >> 64) as u64]));
            } else {
                keys1.clear();
                keys1.extend(packbuf.iter().map(|&w| w as u64 as i64));
            }
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
            ::nodeagg::agg_hash_compact_batch_mk2(agg, &m.mks.keys2, groups)?;
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
