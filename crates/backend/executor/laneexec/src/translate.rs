// Fail-closed translation of scan quals into lane programs (harvested from
// the old lane executor; backend retargeted from the batchexec POC to this
// crate's own interpreter — the JIT/stitched and deform-fusion direct tiers
// did NOT come along: stitched bodies are rebuilt on lanestitch in a later
// tranche, and the direct tier was a heap-deform lever with no pgrcolumnar
// counterpart).
use crate::interp::{
    compile_qual, eval_qual, BStep, Batch, CmpOp as LaneCmp, LaneRef, Program, QualPlan, SelVec,
    MAX_ROWS,
};
use crate::shape::{LaneClause, LaneCmpRhs, LaneQualShape, LaneSuffix};
use datum::NullableDatum;
use exectuples::{SoaBatch, SOA_BM_WORDS, SOA_MAX_ROWS};
use types_error::PgResult;

// Staged windows must fit the lane engine's fixed selection width.
const _: () = assert!(SOA_MAX_ROWS <= MAX_ROWS);

/// One reorderable unit of the vectorizable prefix (prewhere staged eval):
/// its own compiled program (int/null/bool clauses) or a dict-tier clause.
/// Reordering is legal ONLY inside the prefix — every prefix clause is
/// non-erroring and non-volatile by the fail-closed admission (the
/// whitelist IS the semantic contract); anything volatile or error-capable
/// lives in the requal tail, which re-runs the FULL qual in original order
/// per surviving row at fetch.
struct StagedClause {
    cols: Vec<u16>,
    kind: StagedKind,
    // Static cost class (ascending = cheaper): 0 null/bool tests, 1 int eq,
    // 2 int range/ne, 3 IN-list, 4 dict eq/ne, 5 dict LIKE.
    class: u8,
    // pg_statistic refinement (fraction of rows passing); None = unknown.
    sel: Option<f64>,
    orig: u16,
    // Single-column `Var CMP Const` int clause: the driver folds this against
    // the staged pgrcolumnar granule's zone metadata (compressed-domain skip).
    zone: Option<StagedZoneSrc>,
}

/// Raw operands of a foldable `Var CMP Const` staged clause. The scan driver
/// re-derives the zone qual through the SAME extraction that built the
/// pruning zone quals (op/width/flip), so the fold is byte-identical.
#[derive(Clone, Copy)]
pub struct StagedZoneSrc {
    pub col: u16,
    pub fn_oid: ::types_core::Oid,
    pub commuted: bool,
    pub konst: ::datum::Datum,
}

enum StagedKind {
    Int { prog: Program, plan: QualPlan, max_col: u16 },
    Dict(usize),
}

pub struct LaneQualProg {
    prog: Program,
    plan: QualPlan,
    staged: Vec<StagedClause>,
    pub nclauses: u32,
    pub max_attnum: u16,
    /// Hybrid split: the program is a conservative pre-filter (the qual's
    /// vectorizable clause prefix); every selected row must re-run the FULL
    /// original qual per row at fetch (error identity/order, LIMIT
    /// truncation, and volatile-call counts are the per-row drive's by
    /// construction).
    pub requal: bool,
    /// Dict-memoizable text clauses of the prefix (pgrcolumnar scans only):
    /// evaluated by the dict tier after the int program — legal in any order
    /// because every prefix clause is non-erroring by admission.
    dict: Vec<crate::dict::DictClause>,
    staged_logged: bool,
    /// Canonical 128-bit fingerprint of the vectorizable PREFIX (original
    /// clause order; operator fn oid, commutation, collation, column, and
    /// constant VALUE BYTES all included) — the condition-cache key
    /// component. None = the prefix is not cacheable: a clause whose
    /// evaluation is not provably deterministic-and-non-erroring per row
    /// (the dict regex tier preserves error identity by evaluating exactly
    /// the per-row drive's row set, so serving its bits from a cache could
    /// skip an error a miss run raises). The requal tail never contributes:
    /// cached bits are prefix verdicts only and the tail re-runs per
    /// surviving row at fetch either way.
    pub fingerprint: Option<u128>,
}

impl LaneQualProg {
    #[inline]
    pub fn ndict(&self) -> u32 {
        self.dict.len() as u32
    }

    /// Columns the dict tier reads as codes+dict (SoaBatch dict_want arming).
    pub fn dict_cols(&self) -> impl Iterator<Item = u16> + '_ {
        self.dict.iter().map(|c| c.col)
    }

    /// Every column the lane program reads from the SoA batch (int clause
    /// lanes + dict clause lanes incl. their Raw-window fallback) — the
    /// lane-read mask for the pgrcolumnar fill skip.
    pub fn read_cols(&self) -> impl Iterator<Item = u16> + '_ {
        self.prog
            .steps
            .iter()
            .filter_map(|s| match *s {
                BStep::LoadLane { col, .. } | BStep::InListAnyConst { col, .. } => Some(col),
                _ => None,
            })
            .chain(self.dict.iter().map(|c| c.col))
    }

    /// True when the lane program reads column `c`'s Raw Datum cells as live
    /// datums for its verdicts (the int clause lanes: the whole-prefix
    /// program's lane loads and the staged Int clauses). Dict-tier clauses
    /// are deliberately excluded — their Raw-window fallback reads datums
    /// only BEFORE the post-qual gather, so a post-qual length-lane
    /// conversion of the column is safe for them.
    pub fn reads_col_raw(&self, c: u16) -> bool {
        self.prog.steps.iter().any(|s| match *s {
            BStep::LoadLane { col, .. } | BStep::InListAnyConst { col, .. } => col == c,
            _ => false,
        }) || self
            .staged
            .iter()
            .any(|sc| matches!(sc.kind, StagedKind::Int { .. }) && sc.cols.contains(&c))
    }

    /// The WHOLE qual is one `col <> ''` text clause: Some(col). No requal
    /// tail, no other clauses, no int-lane reads — the selection bitmap is
    /// then a pure length predicate (see DictClause::is_ne_empty), so a
    /// granule length-stats consumer can derive it from footer empty-string
    /// counts without staging the granule.
    pub fn ne_empty_single_col(&self) -> Option<u16> {
        if self.requal || self.nclauses != 1 || self.dict.len() != 1 {
            return None;
        }
        let no_int_reads = !self
            .prog
            .steps
            .iter()
            .any(|s| matches!(*s, BStep::LoadLane { .. } | BStep::InListAnyConst { .. }));
        (no_int_reads && self.dict[0].is_ne_empty()).then(|| self.dict[0].col)
    }

    /// Prewhere staged evaluation: number of independently evaluable prefix
    /// clauses, in ascending estimated-cost order.
    pub fn nstaged(&self) -> usize {
        self.staged.len()
    }

    /// Columns clause `k` reads (deform these before eval_staged(k)).
    pub fn staged_cols(&self, k: usize) -> &[u16] {
        &self.staged[k].cols
    }

    /// Zone-fold source for clause `k`, if it is a single-column
    /// `Var CMP Const` int comparator (compressed-domain granule skip).
    pub fn staged_zone_src(&self, k: usize) -> Option<StagedZoneSrc> {
        self.staged[k].zone
    }

    /// Refine the static cost order with live selectivity estimates.
    /// `sel_of(col, class)` returns the estimated fraction of rows passing
    /// the clause (pg_statistic-backed); None keeps the static class order.
    pub fn order_staged(&mut self, sel_of: &dyn Fn(u16, u8) -> Option<f64>) {
        for sc in &mut self.staged {
            if let Some(&c) = sc.cols.first() {
                sc.sel = sel_of(c, sc.class).filter(|s| s.is_finite());
            }
        }
        sort_staged(&mut self.staged);
    }

    /// pg_statistic-backed refinement: eq clauses get (1-nullfrac)/ndistinct;
    /// everything else (and stats-free relations) keeps the static class
    /// order (equality < range < LIKE).
    pub fn order_staged_with_stats(&mut self, mcx: ::mcx::Mcx<'_>, relid: ::types_core::Oid) {
        if !syscache_seams::lookup_pg_statistic_bundle::is_installed() {
            return;
        }
        self.order_staged(&|col, class| {
            if class != 1 {
                return None;
            }
            let b = syscache_seams::lookup_pg_statistic_bundle::call(
                mcx,
                relid,
                col as i16 + 1,
                false,
            )
            .ok()
            .flatten()?;
            let d = b.stadistinct as f64;
            if d > 0.0 {
                Some(((1.0 - b.stanullfrac as f64) / d).clamp(0.0, 1.0))
            } else if d < 0.0 {
                // Fraction-distinct (near-unique): maximally selective.
                Some(0.0)
            } else {
                None
            }
        });
    }

    /// Once-per-plan engagement marker for the staged drive.
    pub fn log_staged_once(&mut self) {
        if !self.staged_logged {
            self.staged_logged = true;
            crate::log_staged_qual(self.staged.len());
        }
    }

    /// Evaluate staged clause `k` over the staged batch, ANDing into `sel`
    /// (bits for rows 0..nrows must be set on entry for rows still live).
    pub fn eval_staged(
        &mut self,
        k: usize,
        soa: &SoaBatch<'_>,
        nrows: u32,
        sel: &mut [u64; SOA_BM_WORDS],
    ) -> PgResult<()> {
        let nwords = (nrows as usize).div_ceil(64);
        let mut sv = SelVec::all(nrows);
        sv.words[..nwords].copy_from_slice(&sel[..nwords]);
        match &mut self.staged[k].kind {
            StagedKind::Int { prog, plan, max_col } => {
                let ncols = *max_col as usize + 1;
                debug_assert!(ncols <= soa.ncols() as usize && nrows == soa.nrows());
                let mut lanes = Vec::with_capacity(ncols);
                for c in 0..ncols {
                    lanes.push(LaneRef::Raw {
                        values: soa.col_values(c),
                        isnull: soa.col_isnull(c),
                    });
                }
                let batch = Batch { nrows, lanes };
                eval_qual(plan, prog, &batch, &mut sv)?;
            }
            StagedKind::Dict(i) => {
                let i = *i;
                crate::dict::eval_dict_clauses(&mut self.dict[i..=i], soa, nrows, &mut sv)?;
            }
        }
        sel[..nwords].copy_from_slice(&sv.words[..nwords]);
        Ok(())
    }
}

/// Fail-closed translation of a decoded scan-qual shape into a lane program.
/// Vocabulary: implicitly-ANDed strict int comparison clauses (int2/int4/
/// int8 + every cross-width family incl. int24/int42, Var-vs-non-null-Const
/// and Var-vs-Var) and NULL tests; a qual with a leading run of such clauses
/// and an arbitrary (subplan-free, exec-param-free — the walker's contract)
/// tail engages as a HYBRID: the prefix vectorizes, the tail runs per row on
/// prefix survivors via the original evaluator. Engagement gate for hybrids
/// (profitability, not correctness — a weak 1-clause pre-filter under a
/// volatile tail buys nothing): prefix >= 2 clauses, or prefix >= 1 with a
/// provably non-volatile tail (unknown volatility counts as volatile).
/// `dict_lanes` (pgrcolumnar scans): dict-memoizable text clauses — LIKE/NOT
/// LIKE with a proven-non-erroring constant pattern, texteq/textne against a
/// constant, deterministic collation — join the vectorizable prefix instead
/// of ending it; they evaluate on the dict-memo tier (or per row over the
/// Raw text lane when a window is not dict-coded). Heap scans must pass
/// false: text columns have no SoA lane there, and admitting one would only
/// widen the prefix past the fixed-width plan.
pub fn translate_scan_qual(
    shape: &LaneQualShape,
    dict_lanes: bool,
) -> Result<Box<LaneQualProg>, &'static str> {
    // A structurally-parsed clause whose fn oid the whitelist refuses (e.g.
    // textlike) ends the prefix like a structural miss: it and everything
    // after it become the per-row requal tail — unless the dict tier admits
    // it (fail-closed classification above).
    let mut dict = Vec::new();
    let mut split = shape.clauses.len();
    for (k, cl) in shape.clauses.iter().enumerate() {
        // Union (dict x vocab): the vocab clause vocabulary decides
        // admission; a refused Cmp clause gets one dict-tier chance (pgrcolumnar
        // text lanes) before ending the prefix.
        let ok = match cl {
            LaneClause::Cmp(c) => {
                if lane_cmp_for_fn_oid(c.fn_oid, c.commuted).is_some() {
                    true
                } else if dict_lanes {
                    if let Some(dc) = crate::dict::dict_clause_for(c) {
                        dict.push(dc);
                        continue;
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            // SAOP scalars always feed arg0, so the element op is never
            // commuted.
            LaneClause::InList { fn_oid, .. } => lane_cmp_for_fn_oid(*fn_oid, false).is_some(),
            LaneClause::NullTest { .. } | LaneClause::BoolVar { .. }
            | LaneClause::BoolTest { .. } => true,
        };
        if !ok {
            split = k;
            break;
        }
    }
    let has_suffix = split < shape.clauses.len() || !matches!(shape.suffix, LaneSuffix::None);
    if split == 0 {
        return Err("not an in-core int comparison fn");
    }
    let requal = if has_suffix {
        let tail_nonvolatile = shape.clauses[split..].iter().all(|cl| match cl {
            LaneClause::Cmp(c) => fn_nonvolatile(c.fn_oid),
            LaneClause::InList { fn_oid, .. } => fn_nonvolatile(*fn_oid),
            LaneClause::NullTest { .. } | LaneClause::BoolVar { .. }
            | LaneClause::BoolTest { .. } => true,
        }) && match &shape.suffix {
            LaneSuffix::None => true,
            LaneSuffix::Calls(oids) => oids.iter().all(|&oid| fn_nonvolatile(oid)),
            LaneSuffix::Opaque => false,
        };
        if !(split >= 2 || tail_nonvolatile) {
            return Err("hybrid prefix below the engagement gate");
        }
        true
    } else {
        false
    };
    let prefix = &shape.clauses[..split];
    let mut prog = Program::new();
    let mut max_attnum = 0u16;
    let mut staged: Vec<StagedClause> = Vec::new();
    let mut dict_i = 0usize;
    for (orig, cl) in prefix.iter().enumerate() {
        let class = emit_clause(&mut prog, cl, &mut max_attnum);
        if class == CLASS_DICT_ADMITTED {
            let dcol = dict[dict_i].col;
            staged.push(StagedClause {
                cols: vec![dcol],
                kind: StagedKind::Dict(dict_i),
                class: dict[dict_i].cost_class(),
                sel: None,
                orig: orig as u16,
                zone: None,
            });
            dict_i += 1;
            continue;
        }
        let mut cprog = Program::new();
        let mut cmax = 0u16;
        emit_clause(&mut cprog, cl, &mut cmax);
        let cols = clause_cols(&cprog);
        let cplan = compile_qual(&cprog);
        let zone = staged_zone_of(cl, &cols);
        staged.push(StagedClause {
            cols,
            kind: StagedKind::Int { prog: cprog, plan: cplan, max_col: cmax },
            class,
            sel: None,
            orig: orig as u16,
            zone,
        });
    }
    debug_assert_eq!(dict_i, dict.len());
    sort_staged(&mut staged);
    let plan = compile_qual(&prog);
    let fingerprint = fingerprint_prefix(prefix, &dict);
    Ok(Box::new(LaneQualProg {
        prog,
        plan,
        staged,
        nclauses: split as u32,
        max_attnum,
        requal,
        dict,
        staged_logged: false,
        fingerprint,
    }))
}

// ===========================================================================
// Condition-cache fingerprint (pgrust.condition_cache): canonical hash of
// the vectorizable prefix in ORIGINAL clause order. The cached value is the
// prefix's survivor bitmap, so the fingerprint must pin everything that
// determines it: clause kind, column identity, comparator fn oid,
// commutation, collation, and the constant's VALUE (word bits for the byval
// int/float/date/ts/oid families; payload bytes for text consts). Staged
// evaluation ORDER deliberately excluded — every prefix clause is
// non-erroring and the bitmap is an AND, so order cannot change the bits.
// None = refuse caching (fail closed): a prefix clause whose per-row
// evaluation is not provably deterministic-and-non-erroring (dict regex
// tier), or a const the hasher cannot canonicalize.
// ===========================================================================

struct Fp {
    a: std::collections::hash_map::DefaultHasher,
    b: std::collections::hash_map::DefaultHasher,
}

impl Fp {
    fn new() -> Fp {
        use std::hash::Hasher;
        let mut a = std::collections::hash_map::DefaultHasher::new();
        let mut b = std::collections::hash_map::DefaultHasher::new();
        // Distinct stream salts (incl. a format version to invalidate every
        // key if the hashed vocabulary ever changes shape).
        a.write_u64(0x7067_636f_6e64_0001);
        b.write_u64(0xcc5a_17ab_9e3d_0001);
        Fp { a, b }
    }
    fn u64(&mut self, v: u64) {
        use std::hash::Hasher;
        self.a.write_u64(v);
        self.b.write_u64(v);
    }
    fn bytes(&mut self, v: &[u8]) {
        use std::hash::Hasher;
        self.a.write(v);
        self.b.write(v);
        // Length is hashed explicitly so concatenations can't collide.
        self.u64(v.len() as u64);
    }
    fn finish(self) -> u128 {
        use std::hash::Hasher;
        (self.a.finish() as u128) << 64 | self.b.finish() as u128
    }
}

fn fingerprint_prefix(
    prefix: &[LaneClause],
    dict: &[crate::dict::DictClause],
) -> Option<u128> {
    let mut h = Fp::new();
    let mut dict_i = 0usize;
    h.u64(prefix.len() as u64);
    for cl in prefix {
        match cl {
            LaneClause::Cmp(c) => {
                if lane_cmp_for_fn_oid(c.fn_oid, c.commuted).is_some() {
                    h.u64(1);
                    h.u64(c.col as u64);
                    h.u64(c.fn_oid as u64);
                    h.u64(c.commuted as u64);
                    h.u64(c.collation as u64);
                    match c.rhs {
                        LaneCmpRhs::Const(k) => {
                            h.u64(10);
                            // Byval word: the whitelist families are all
                            // word-carried (int/date/ts sign-extended, oid
                            // zero-extended, float bit patterns) — the word
                            // IS the value.
                            h.u64(k.as_i64() as u64);
                        }
                        LaneCmpRhs::Col(c2) => {
                            h.u64(11);
                            h.u64(c2 as u64);
                        }
                    }
                } else {
                    // Dict-admitted text clause (translate kept prefix/dict
                    // in lockstep). Regex ops refuse: their lazy tri-memo
                    // evaluates exactly the per-row drive's row set to keep
                    // error identity — a cache hit would skip those rows.
                    let dc = dict.get(dict_i)?;
                    dict_i += 1;
                    if !dc.cache_safe() {
                        return None;
                    }
                    let LaneCmpRhs::Const(k) = c.rhs else { return None };
                    h.u64(2);
                    h.u64(c.col as u64);
                    h.u64(c.fn_oid as u64);
                    h.u64(c.commuted as u64);
                    h.u64(c.collation as u64);
                    h.bytes(crate::dict::inline_varlena_payload(k)?);
                }
            }
            LaneClause::NullTest { col, want_null } => {
                h.u64(3);
                h.u64(*col as u64);
                h.u64(*want_null as u64);
            }
            LaneClause::BoolVar { col } => {
                h.u64(4);
                h.u64(*col as u64);
            }
            LaneClause::BoolTest { col, kind } => {
                h.u64(5);
                h.u64(*col as u64);
                h.u64(match kind {
                    crate::shape::LaneBoolTest::IsTrue => 0,
                    crate::shape::LaneBoolTest::IsNotTrue => 1,
                    crate::shape::LaneBoolTest::IsFalse => 2,
                    crate::shape::LaneBoolTest::IsNotFalse => 3,
                });
            }
            LaneClause::InList { col, fn_oid, elems } => {
                h.u64(6);
                h.u64(*col as u64);
                h.u64(*fn_oid as u64);
                h.u64(elems.len() as u64);
                for e in elems {
                    h.u64(e.isnull as u64);
                    h.u64(if e.isnull { 0 } else { e.value.as_i64() as u64 });
                }
            }
        }
    }
    Some(h.finish())
}

// Sentinel class: a Cmp clause the dict tier admitted (no lane op).
const CLASS_DICT_ADMITTED: u8 = u8::MAX;

fn is_eq_op(op: LaneCmp) -> bool {
    matches!(
        op,
        LaneCmp::Int4Eq
            | LaneCmp::Int8Eq
            | LaneCmp::Int2Eq
            | LaneCmp::Int84Eq
            | LaneCmp::Int48Eq
            | LaneCmp::Int24Eq
            | LaneCmp::Int42Eq
            | LaneCmp::OidEq
            | LaneCmp::Float4Eq
            | LaneCmp::Float8Eq
            | LaneCmp::Float48Eq
            | LaneCmp::Float84Eq
    )
}

// Foldable iff a lone single-column `Var CMP Const` int comparator. The
// driver decides zone-eligibility (op/width) — anything else stays None.
fn staged_zone_of(cl: &LaneClause, cols: &[u16]) -> Option<StagedZoneSrc> {
    let LaneClause::Cmp(c) = cl else { return None };
    let LaneCmpRhs::Const(konst) = &c.rhs else { return None };
    if cols.len() != 1 || cols[0] != c.col {
        return None;
    }
    Some(StagedZoneSrc { col: c.col, fn_oid: c.fn_oid, commuted: c.commuted, konst: *konst })
}

fn clause_cols(prog: &Program) -> Vec<u16> {
    let mut cols: Vec<u16> = prog
        .steps
        .iter()
        .filter_map(|s| match *s {
            BStep::LoadLane { col, .. } | BStep::InListAnyConst { col, .. } => Some(col),
            _ => None,
        })
        .collect();
    cols.sort_unstable();
    cols.dedup();
    cols
}

// (class, selectivity, original position) ascending; stable across equal
// keys so the no-stats order is deterministic.
fn sort_staged(staged: &mut [StagedClause]) {
    staged.sort_by(|a, b| {
        (a.class, a.sel.unwrap_or(1.0), a.orig)
            .partial_cmp(&(b.class, b.sel.unwrap_or(1.0), b.orig))
            .expect("selectivities are finite")
    });
}

fn emit_clause(prog: &mut Program, cl: &LaneClause, max_attnum: &mut u16) -> u8 {
    match cl {
        LaneClause::Cmp(cl) => {
            let Some(op) = lane_cmp_for_fn_oid(cl.fn_oid, cl.commuted) else {
                // Dict-admitted clause: evaluated by the dict tier, but
                // its column still widens the deform prefix.
                *max_attnum = (*max_attnum).max(cl.col);
                return CLASS_DICT_ADMITTED;
            };
            prog.steps.push(BStep::LoadLane { col: cl.col, out: 0 });
            *max_attnum = (*max_attnum).max(cl.col);
            match cl.rhs {
                LaneCmpRhs::Const(k) => {
                    // Mixed-extension fact (oid): Consts arrive
                    // ObjectIdGetDatum-ZERO-extended while deformed
                    // lanes SIGN-extend 4-byte attrs (C carries the
                    // same mix and never reads the upper word).
                    // Canonicalize oid konsts to the lane
                    // sign-extension contract so a width-blind 2x64
                    // unsigned SIMD arm stays exact; the interpreter
                    // truncates either way.
                    let k = if oid_family(op) {
                        datum::Datum::from_i32(k.as_u32() as i32)
                    } else {
                        k
                    };
                    let kix = prog.push_const(NullableDatum { value: k, isnull: false });
                    prog.steps.push(BStep::LoadConst { k: kix, out: 1 });
                }
                LaneCmpRhs::Col(c) => {
                    prog.steps.push(BStep::LoadLane { col: c, out: 1 });
                    *max_attnum = (*max_attnum).max(c);
                }
            }
            prog.steps.push(BStep::Cmp { op, a: 0, b: 1, out: 2 });
            prog.steps.push(BStep::Qual { a: 2 });
            if is_eq_op(op) { 1 } else { 2 }
        }
        LaneClause::NullTest { col, want_null } => {
            prog.steps.push(BStep::LoadLane { col: *col, out: 0 });
            *max_attnum = (*max_attnum).max(*col);
            prog.steps.push(if *want_null {
                BStep::IsNull { a: 0, out: 1 }
            } else {
                BStep::IsNotNull { a: 0, out: 1 }
            });
            prog.steps.push(BStep::Qual { a: 1 });
            0
        }
        LaneClause::BoolVar { col } => {
            // LoadLane + Qual is exactly bare-bool-Var semantics: NULL
            // or false fails the row on both drives.
            prog.steps.push(BStep::LoadLane { col: *col, out: 0 });
            *max_attnum = (*max_attnum).max(*col);
            prog.steps.push(BStep::Qual { a: 0 });
            0
        }
        LaneClause::BoolTest { col, kind } => {
            let (null_result, negate) = match kind {
                crate::shape::LaneBoolTest::IsTrue => (false, false),
                crate::shape::LaneBoolTest::IsNotTrue => (true, true),
                crate::shape::LaneBoolTest::IsFalse => (false, true),
                crate::shape::LaneBoolTest::IsNotFalse => (true, false),
            };
            prog.steps.push(BStep::LoadLane { col: *col, out: 0 });
            *max_attnum = (*max_attnum).max(*col);
            prog.steps.push(BStep::BoolTest { null_result, negate, a: 0, out: 1 });
            prog.steps.push(BStep::Qual { a: 1 });
            0
        }
        LaneClause::InList { col, fn_oid, elems } => {
            let op = lane_cmp_for_fn_oid(*fn_oid, false)
                .expect("prefix oids whitelisted by the split scan");
            let k = prog.consts.len() as u16;
            for e in elems {
                // Belt: array elements are sign-extended post the
                // arrayfuncs fetch_att fix; re-canonicalize anyway so
                // oid IN-list exactness never depends on the element
                // source (same contract as the Cmp konst above).
                let e = if oid_family(op) && !e.isnull {
                    NullableDatum {
                        value: datum::Datum::from_i32(e.value.as_u32() as i32),
                        isnull: false,
                    }
                } else {
                    *e
                };
                prog.push_const(e);
            }
            *max_attnum = (*max_attnum).max(*col);
            prog.steps.push(BStep::InListAnyConst {
                col: *col,
                op,
                k,
                n: elems.len() as u16,
                out: 0,
            });
            prog.steps.push(BStep::Qual { a: 0 });
            3
        }
    }
}

// Unknown/unresolvable volatility counts as volatile (fail-closed gate);
// the seam guard covers unit tests / early boot without a syscache.
fn fn_nonvolatile(oid: u32) -> bool {
    syscache_seams::lookup_pg_proc_shape::is_installed()
        && matches!(lsyscache::func_volatile(oid), Ok(v) if v != b'v' as i8)
}

/// Evaluate the lane qual over the staged SoA batch into `sel`. Same contract
/// as qual_bitmap_cmp_const: bits are written for rows 0..nrows; the caller
/// ORs the fallback-row bits so skipped rows take the per-row re-check path.
pub fn eval_lane_qual(
    lq: &mut LaneQualProg,
    soa: &SoaBatch<'_>,
    nrows: u32,
    sel: &mut [u64; SOA_BM_WORDS],
) -> PgResult<()> {
    let ncols = lq.max_attnum as usize + 1;
    debug_assert!(ncols <= soa.ncols() as usize && nrows == soa.nrows());
    let mut lanes = Vec::with_capacity(ncols);
    for c in 0..ncols {
        lanes.push(LaneRef::Raw { values: soa.col_values(c), isnull: soa.col_isnull(c) });
    }
    let batch = Batch { nrows, lanes };
    let mut sv = SelVec::all(nrows);
    eval_qual(&mut lq.plan, &lq.prog, &batch, &mut sv)?;
    if !lq.dict.is_empty() {
        crate::dict::eval_dict_clauses(&mut lq.dict, soa, nrows, &mut sv)?;
    }
    let nwords = (nrows as usize).div_ceil(64);
    sel[..nwords].copy_from_slice(&sv.words[..nwords]);
    Ok(())
}

/// Fail-closed comparator whitelist: pg_proc oid of an in-core non-erroring
/// strict int comparator -> lane op, with the Var normalized to the left
/// operand. `commuted` = the clause's Var fed arg1, so the emitted op is the
/// mirror (operand swap flips the relation AND crosses width families:
/// commuted int42gt = Int24Lt). pub for the classification-oracle tests.
pub fn lane_cmp_for_fn_oid(fn_oid: u32, commuted: bool) -> Option<LaneCmp> {
    let base = match fn_oid {
        65 => LaneCmp::Int4Eq,
        144 => LaneCmp::Int4Ne,
        66 => LaneCmp::Int4Lt,
        149 => LaneCmp::Int4Le,
        147 => LaneCmp::Int4Gt,
        150 => LaneCmp::Int4Ge,
        467 => LaneCmp::Int8Eq,
        468 => LaneCmp::Int8Ne,
        469 => LaneCmp::Int8Lt,
        471 => LaneCmp::Int8Le,
        470 => LaneCmp::Int8Gt,
        472 => LaneCmp::Int8Ge,
        63 => LaneCmp::Int2Eq,
        145 => LaneCmp::Int2Ne,
        64 => LaneCmp::Int2Lt,
        148 => LaneCmp::Int2Le,
        146 => LaneCmp::Int2Gt,
        151 => LaneCmp::Int2Ge,
        474 => LaneCmp::Int84Eq,
        475 => LaneCmp::Int84Ne,
        476 => LaneCmp::Int84Lt,
        478 => LaneCmp::Int84Le,
        477 => LaneCmp::Int84Gt,
        479 => LaneCmp::Int84Ge,
        852 => LaneCmp::Int48Eq,
        853 => LaneCmp::Int48Ne,
        854 => LaneCmp::Int48Lt,
        856 => LaneCmp::Int48Le,
        855 => LaneCmp::Int48Gt,
        857 => LaneCmp::Int48Ge,
        158 => LaneCmp::Int24Eq,
        164 => LaneCmp::Int24Ne,
        160 => LaneCmp::Int24Lt,
        166 => LaneCmp::Int24Le,
        162 => LaneCmp::Int24Gt,
        168 => LaneCmp::Int24Ge,
        159 => LaneCmp::Int42Eq,
        165 => LaneCmp::Int42Ne,
        161 => LaneCmp::Int42Lt,
        167 => LaneCmp::Int42Le,
        163 => LaneCmp::Int42Gt,
        169 => LaneCmp::Int42Ge,
        // date (int32 days; DATE_NOINT/infinity sentinels order correctly,
        // so date_cmp is a plain int compare — date.c parity).
        1086 => LaneCmp::Int4Eq,
        1091 => LaneCmp::Int4Ne,
        1087 => LaneCmp::Int4Lt,
        1088 => LaneCmp::Int4Le,
        1089 => LaneCmp::Int4Gt,
        1090 => LaneCmp::Int4Ge,
        // timestamp / timestamptz (int64 microseconds; infinity sentinels
        // are i64::MIN/MAX — timestamp_cmp_internal is a plain int compare).
        2052 | 1152 => LaneCmp::Int8Eq,
        2053 | 1153 => LaneCmp::Int8Ne,
        2054 | 1154 => LaneCmp::Int8Lt,
        2055 | 1155 => LaneCmp::Int8Le,
        2057 | 1157 => LaneCmp::Int8Gt,
        2056 | 1156 => LaneCmp::Int8Ge,
        // oid: unsigned 32-bit (oid_ops in scalar builtins compare as_oid).
        184 => LaneCmp::OidEq,
        185 => LaneCmp::OidNe,
        716 => LaneCmp::OidLt,
        717 => LaneCmp::OidLe,
        1638 => LaneCmp::OidGt,
        1639 => LaneCmp::OidGe,
        // float families: non-erroring with float.h NaN ordering (NaN = NaN,
        // NaN > any non-NaN) — the lane kernels reproduce it exactly.
        287 => LaneCmp::Float4Eq,
        288 => LaneCmp::Float4Ne,
        289 => LaneCmp::Float4Lt,
        290 => LaneCmp::Float4Le,
        291 => LaneCmp::Float4Gt,
        292 => LaneCmp::Float4Ge,
        293 => LaneCmp::Float8Eq,
        294 => LaneCmp::Float8Ne,
        295 => LaneCmp::Float8Lt,
        296 => LaneCmp::Float8Le,
        297 => LaneCmp::Float8Gt,
        298 => LaneCmp::Float8Ge,
        299 => LaneCmp::Float48Eq,
        300 => LaneCmp::Float48Ne,
        301 => LaneCmp::Float48Lt,
        302 => LaneCmp::Float48Le,
        303 => LaneCmp::Float48Gt,
        304 => LaneCmp::Float48Ge,
        305 => LaneCmp::Float84Eq,
        306 => LaneCmp::Float84Ne,
        307 => LaneCmp::Float84Lt,
        308 => LaneCmp::Float84Le,
        309 => LaneCmp::Float84Gt,
        310 => LaneCmp::Float84Ge,
        _ => return None,
    };
    Some(if commuted { swap_operands(base) } else { base })
}

fn oid_family(op: LaneCmp) -> bool {
    matches!(
        op,
        LaneCmp::OidEq
            | LaneCmp::OidNe
            | LaneCmp::OidLt
            | LaneCmp::OidLe
            | LaneCmp::OidGt
            | LaneCmp::OidGe
    )
}

// op'(b, a) == op(a, b): mirror the relation, mirror the width family.
fn swap_operands(op: LaneCmp) -> LaneCmp {
    match op {
        LaneCmp::Int4Eq => LaneCmp::Int4Eq,
        LaneCmp::Int4Ne => LaneCmp::Int4Ne,
        LaneCmp::Int4Lt => LaneCmp::Int4Gt,
        LaneCmp::Int4Le => LaneCmp::Int4Ge,
        LaneCmp::Int4Gt => LaneCmp::Int4Lt,
        LaneCmp::Int4Ge => LaneCmp::Int4Le,
        LaneCmp::Int8Eq => LaneCmp::Int8Eq,
        LaneCmp::Int8Ne => LaneCmp::Int8Ne,
        LaneCmp::Int8Lt => LaneCmp::Int8Gt,
        LaneCmp::Int8Le => LaneCmp::Int8Ge,
        LaneCmp::Int8Gt => LaneCmp::Int8Lt,
        LaneCmp::Int8Ge => LaneCmp::Int8Le,
        LaneCmp::Int2Eq => LaneCmp::Int2Eq,
        LaneCmp::Int2Ne => LaneCmp::Int2Ne,
        LaneCmp::Int2Lt => LaneCmp::Int2Gt,
        LaneCmp::Int2Le => LaneCmp::Int2Ge,
        LaneCmp::Int2Gt => LaneCmp::Int2Lt,
        LaneCmp::Int2Ge => LaneCmp::Int2Le,
        LaneCmp::Int84Eq => LaneCmp::Int48Eq,
        LaneCmp::Int84Ne => LaneCmp::Int48Ne,
        LaneCmp::Int84Lt => LaneCmp::Int48Gt,
        LaneCmp::Int84Le => LaneCmp::Int48Ge,
        LaneCmp::Int84Gt => LaneCmp::Int48Lt,
        LaneCmp::Int84Ge => LaneCmp::Int48Le,
        LaneCmp::Int48Eq => LaneCmp::Int84Eq,
        LaneCmp::Int48Ne => LaneCmp::Int84Ne,
        LaneCmp::Int48Lt => LaneCmp::Int84Gt,
        LaneCmp::Int48Le => LaneCmp::Int84Ge,
        LaneCmp::Int48Gt => LaneCmp::Int84Lt,
        LaneCmp::Int48Ge => LaneCmp::Int84Le,
        LaneCmp::Int24Eq => LaneCmp::Int42Eq,
        LaneCmp::Int24Ne => LaneCmp::Int42Ne,
        LaneCmp::Int24Lt => LaneCmp::Int42Gt,
        LaneCmp::Int24Le => LaneCmp::Int42Ge,
        LaneCmp::Int24Gt => LaneCmp::Int42Lt,
        LaneCmp::Int24Ge => LaneCmp::Int42Le,
        LaneCmp::Int42Eq => LaneCmp::Int24Eq,
        LaneCmp::Int42Ne => LaneCmp::Int24Ne,
        LaneCmp::Int42Lt => LaneCmp::Int24Gt,
        LaneCmp::Int42Le => LaneCmp::Int24Ge,
        LaneCmp::Int42Gt => LaneCmp::Int24Lt,
        LaneCmp::Int42Ge => LaneCmp::Int24Le,
        LaneCmp::OidEq => LaneCmp::OidEq,
        LaneCmp::OidNe => LaneCmp::OidNe,
        LaneCmp::OidLt => LaneCmp::OidGt,
        LaneCmp::OidLe => LaneCmp::OidGe,
        LaneCmp::OidGt => LaneCmp::OidLt,
        LaneCmp::OidGe => LaneCmp::OidLe,
        LaneCmp::Float4Eq => LaneCmp::Float4Eq,
        LaneCmp::Float4Ne => LaneCmp::Float4Ne,
        LaneCmp::Float4Lt => LaneCmp::Float4Gt,
        LaneCmp::Float4Le => LaneCmp::Float4Ge,
        LaneCmp::Float4Gt => LaneCmp::Float4Lt,
        LaneCmp::Float4Ge => LaneCmp::Float4Le,
        LaneCmp::Float8Eq => LaneCmp::Float8Eq,
        LaneCmp::Float8Ne => LaneCmp::Float8Ne,
        LaneCmp::Float8Lt => LaneCmp::Float8Gt,
        LaneCmp::Float8Le => LaneCmp::Float8Ge,
        LaneCmp::Float8Gt => LaneCmp::Float8Lt,
        LaneCmp::Float8Ge => LaneCmp::Float8Le,
        LaneCmp::Float48Eq => LaneCmp::Float84Eq,
        LaneCmp::Float48Ne => LaneCmp::Float84Ne,
        LaneCmp::Float48Lt => LaneCmp::Float84Gt,
        LaneCmp::Float48Le => LaneCmp::Float84Ge,
        LaneCmp::Float48Gt => LaneCmp::Float84Lt,
        LaneCmp::Float48Ge => LaneCmp::Float84Le,
        LaneCmp::Float84Eq => LaneCmp::Float48Eq,
        LaneCmp::Float84Ne => LaneCmp::Float48Ne,
        LaneCmp::Float84Lt => LaneCmp::Float48Gt,
        LaneCmp::Float84Le => LaneCmp::Float48Ge,
        LaneCmp::Float84Gt => LaneCmp::Float48Lt,
        LaneCmp::Float84Ge => LaneCmp::Float48Le,
    }
}
