// The stitcher: compiles one qual-segment program into ONE fused AArch64
// body (batch in, selection bits out). The row loop is INSIDE the body
// (prior-art doctrine: hot loops inside a single stitched region); per-step
// snippets are straight-line stencils.
//
// Cross-stencil value passing: NO register allocation across stencils
// (CPython copy-and-patch doctrine). The interpreter's virtual register file
// lives in the body's stack frame at fixed offsets ([sp + r*16] value,
// [sp + r*16 + 8] isnull byte); every generic stencil reads/writes those
// slots. Fixed callee-saved conventions between stencils: x19 = params
// block, x20 = row index, x21 = nrows, x23 = sel words, x25-x28 = hoisted
// (values, isnull) base pointers of the first two used columns
// (loop-invariant — hoisting is not register allocation). x0-x17 are
// per-stencil scratch.
//
// Clause fusion: the planner recognizes whole non-erroring clause shapes
// ([lane, const, cmp, qual] and [lane, lane, cmp, qual]) and emits one fused
// stencil with no register-file round trip — legal only when the fused
// window's output registers are dead downstream (regs_dead_after).
//
// Semantics: steps execute in program order per row, ascending row order —
// clause order, short-circuit, and error positions are identical to the
// loop-inside interpreter by construction. The erroring (Arith) stencils
// carry the refuse-and-replay discipline (the design-doc §3a /
// emit_inline_strict2 pattern adapted to a batch body): an overflow or
// zero divisor branches to the body's refuse exit instead of constructing
// an error in stitched code — the driver replays the batch on the
// interpreter, which raises C's exact error on C's row. No Rust helpers are
// reachable from a stitched body at all (nothing semantics-bearing is
// open-coded; anything that would need a helper refuses instead).
//
// Fail-closed classification: `plan_clauses` is exhaustive over Step and
// CmpOp with no wildcard admission — a new variant fails to compile
// (refuse -> the caller stays on the interpreter tier) until classified
// here. Float comparators are whitelisted ONLY in the lane-vs-non-NaN-const
// fused shape (scalar fcmp conds and the NEON ordered-compare arm are exact
// there — see float_cond); float var-var and NaN consts refuse.
//
// SIMD tier (classify_simd): when the whole program is provably
// non-erroring, the body runs 64-row blocks — NEON stencils build a 64-bit
// pass word (8 Datums per iteration, 2x64 per q-register), non-vector
// clauses run per-row over the word's surviving bits in ascending order.
// Vector-pass shapes: CmpConst / FCmpConst / CmpVar / FCmpVar (the exact
// NaN-mask formulation, see emit_fcmpvar_quad_2d), NullTestQ / BoolTestQ
// (mask ops over the already-loaded isnull bytes / value words), and SaopQ
// (one compare block per non-NULL element ORed into the pass word — SIMD
// beats the scalar loop at every admitted length, see vector_pass_clause).
// Legality is the vectorized interpreter's
// argument, spelled out: the qual is an AND of clauses; with every clause
// in the plan pure and non-erroring (classify_simd refuses any Arith),
// each clause's per-row outcome is a pure function of the row's lane
// bytes, so evaluating clauses in any order — across rows and across
// clauses — produces the identical AND. A row failing a pure clause never
// reaches any later clause in EITHER order, so short-circuit is
// unobservable; there are no errors to reorder by construction. Any
// erroring (Arith) clause refuses the SIMD shape and the program falls
// back to the scalar row-loop body, which keeps exact per-row
// refuse-and-replay order.

use datum::Datum;

use crate::emit::{Cond, Emitter, Label};
use crate::spec::{
    is_float_cmp, ArithOp, BoolTestKind, CmpOp, NullTestKind, Program, Step, MAX_COLS, MAX_REGS,
    MAX_ROWS,
};

// Params-block field offsets, provided by lib.rs from offset_of (asserted
// against the repr(C) layout there).
pub(crate) struct ParamsLayout {
    pub lane_stride: u32,
    pub lane_p0: u32,
    pub lane_isnull: u32,
    pub sel: u32,
    pub nrows: u32,
    /// Base offset of the output-lane array (projection bodies only; qual
    /// layouts pass 0 — no StoreOut ever emits there by classification).
    pub outs_base: u32,
}

/// Upper bound on SaopAny array length (the stencil unrolls one compare
/// block per element; larger arrays refuse and stay on the interpreter).
pub(crate) const MAX_SAOP_ELEMS: usize = 128;

/// Body exit codes (x0): 0 = batch fully consumed; RC_REFUSE = an erroring
/// stencil tripped (overflow / zero divisor) — the driver must replay the
/// batch on the interpreter (refuse-and-replay).
pub(crate) const RC_OK: i64 = 0;
pub(crate) const RC_REFUSE: i64 = -1;

/// Float relation of a whitelisted float comparator.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum FRel {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// One classified clause. Fused shapes carry everything the stencil bakes;
/// Generic runs steps[lo..hi] through the virtual register file.
pub(crate) enum ClauseShape {
    /// !isnull(lane) && int_cmp(lane, konst) — one fused stencil.
    CmpConst { col: u16, op: CmpOp, konst: Datum },
    /// !isnull(lane) && pgf_rel(lane_as_f64, konst_f64) with konst non-NaN.
    FCmpConst { col: u16, rel: FRel, konst_bits: u64, lane_f32: bool },
    /// !isnull(a) && !isnull(b) && int_cmp(a, b).
    CmpVar { a_col: u16, b_col: u16, op: CmpOp },
    /// !isnull(a) && !isnull(b) && pgf_rel(a, b) — float var-var. Exact on
    /// both tiers via explicit NaN masks (emit arms carry the proofs); both
    /// sides evaluate at f64 (exact promotion).
    FCmpVar { a_col: u16, b_col: u16, rel: FRel, a_f32: bool, b_f32: bool },
    /// lane IS [NOT] NULL fused with its Qual: a pure predicate of the
    /// row's isnull byte (NullTest never yields NULL).
    NullTestQ { col: u16, kind: NullTestKind },
    /// lane IS [NOT] TRUE/FALSE fused with its Qual: a pure predicate of
    /// (isnull byte, value word != 0) (BooleanTest never yields NULL).
    BoolTestQ { col: u16, kind: BoolTestKind },
    /// lane <op> ANY (const array) fused with its Qual. The three-valued
    /// SAOP surface collapses under a qual: the row passes iff the scalar
    /// is non-NULL and some non-NULL element matches (a NULL result fails
    /// the qual exactly like false), so NULL elements only matter by NOT
    /// matching. Whitelisted non-erroring comparators only (no floats).
    SaopQ { col: u16, op: CmpOp, arr: u16 },
    Generic { lo: usize, hi: usize },
}

pub(crate) struct Plan {
    pub clauses: Vec<ClauseShape>,
    pub used_cols: Vec<u16>,
    pub has_arith: bool,
}

fn reg_bad(r: u8) -> bool {
    r as usize >= MAX_REGS
}

/// (reads, write) register sets of one step — the fusion liveness check.
fn step_io(s: &Step) -> ([Option<u8>; 2], Option<u8>) {
    match *s {
        Step::LoadLane { out, .. } | Step::LoadConst { out, .. } => ([None, None], Some(out)),
        Step::Cmp { a, b, out, .. } | Step::Arith { a, b, out, .. } => {
            ([Some(a), Some(b)], Some(out))
        }
        Step::NullTest { a, out, .. }
        | Step::BoolTest { a, out, .. }
        | Step::SaopAny { a, out, .. } => ([Some(a), None], Some(out)),
        Step::Qual { a } => ([Some(a), None], None),
        Step::StoreOut { a, .. } => ([Some(a), None], None),
        // WS-AA wave-7 RowOp steps: no register-file traffic (they never
        // reach a qual/projection plan — refused below — but liveness must
        // stay total over the vocabulary).
        Step::NextRow | Step::ProtocolCall { .. } => ([None, None], None),
    }
}

/// Fusion legality: a fused clause skips its register-file writes, so every
/// downstream read of the window's registers must be preceded by a rewrite.
fn regs_dead_after(steps: &[Step], from: usize, regs: &[u8]) -> bool {
    let mut pending: Vec<u8> = regs.to_vec();
    for s in &steps[from..] {
        let (reads, write) = step_io(s);
        for r in reads.into_iter().flatten() {
            if pending.contains(&r) {
                return false;
            }
        }
        if let Some(w) = write {
            pending.retain(|&r| r != w);
            if pending.is_empty() {
                return true;
            }
        }
    }
    true
}

/// Lane/konst widths of a whitelisted float family: (lane is f32, konst is
/// f32). Both sides evaluate at f64 (exact promotion).
fn float_family(op: CmpOp) -> (bool, bool) {
    use CmpOp::*;
    match op {
        Float4Eq | Float4Ne | Float4Lt | Float4Le | Float4Gt | Float4Ge => (true, true),
        Float8Eq | Float8Ne | Float8Lt | Float8Le | Float8Gt | Float8Ge => (false, false),
        Float48Eq | Float48Ne | Float48Lt | Float48Le | Float48Gt | Float48Ge => (true, false),
        Float84Eq | Float84Ne | Float84Lt | Float84Le | Float84Gt | Float84Ge => (false, true),
        _ => unreachable!("float_family on a non-float comparator"),
    }
}

fn float_rel(op: CmpOp) -> FRel {
    use CmpOp::*;
    match op {
        Float4Eq | Float8Eq | Float48Eq | Float84Eq => FRel::Eq,
        Float4Ne | Float8Ne | Float48Ne | Float84Ne => FRel::Ne,
        Float4Lt | Float8Lt | Float48Lt | Float84Lt => FRel::Lt,
        Float4Le | Float8Le | Float48Le | Float84Le => FRel::Le,
        Float4Gt | Float8Gt | Float48Gt | Float84Gt => FRel::Gt,
        Float4Ge | Float8Ge | Float48Ge | Float84Ge => FRel::Ge,
        _ => unreachable!("float_rel on a non-float comparator"),
    }
}

/// Scalar float pass-cond after `fcmp lane, konst` with konst non-NaN.
/// Unordered (= NaN lane) sets NZCV=0011: HI/HS read true (PG float.h — a
/// NaN passes gt/ge vs any non-NaN), MI/LS/EQ read false, NE reads true —
/// exactly the pgf_* truth table restricted to a non-NaN rhs.
fn float_cond(rel: FRel) -> Cond {
    match rel {
        FRel::Eq => Cond::Eq,
        FRel::Ne => Cond::Ne,
        FRel::Lt => Cond::Mi,
        FRel::Le => Cond::Ls,
        FRel::Gt => Cond::Hi,
        FRel::Ge => Cond::Hs,
    }
}

// Canonical sign-extension (the deform/from_iN contract — see spec.rs)
// makes the interpreter's truncate-then-widen cross-width semantics equal
// to one signed compare at any width covering both sides: w for the
// int2/int4 mixes, x for every int8 family. Oid scalar compares stay at w
// width with unsigned conds — extension-blind and exact for u32 whatever
// the upper words. The 2x64 SIMD arm additionally requires BOTH operands
// canonical-sign-extended (spec.rs contract): sign-extension is injective
// and monotone under UNSIGNED 64-bit compare, so CMHI/CMHS/CMEQ on
// sign-extended words are exact u32 semantics.
fn cmp_cond(op: CmpOp) -> (bool, Cond) {
    use CmpOp::*;
    let cond = match op {
        Int4Eq | Int8Eq | Int2Eq | Int84Eq | Int48Eq | Int24Eq | Int42Eq | OidEq => Cond::Eq,
        Int4Ne | Int8Ne | Int2Ne | Int84Ne | Int48Ne | Int24Ne | Int42Ne | OidNe => Cond::Ne,
        Int4Lt | Int8Lt | Int2Lt | Int84Lt | Int48Lt | Int24Lt | Int42Lt => Cond::Lt,
        Int4Le | Int8Le | Int2Le | Int84Le | Int48Le | Int24Le | Int42Le => Cond::Le,
        Int4Gt | Int8Gt | Int2Gt | Int84Gt | Int48Gt | Int24Gt | Int42Gt => Cond::Gt,
        Int4Ge | Int8Ge | Int2Ge | Int84Ge | Int48Ge | Int24Ge | Int42Ge => Cond::Ge,
        OidLt => Cond::Lo,
        OidLe => Cond::Ls,
        OidGt => Cond::Hi,
        OidGe => Cond::Hs,
        op => unreachable!("cmp_cond on a float comparator (float={})", is_float_cmp(op)),
    };
    let wide = matches!(
        op,
        Int8Eq | Int8Ne | Int8Lt | Int8Le | Int8Gt | Int8Ge
            | Int84Eq | Int84Ne | Int84Lt | Int84Le | Int84Gt | Int84Ge
            | Int48Eq | Int48Ne | Int48Lt | Int48Le | Int48Gt | Int48Ge
    );
    (wide, cond)
}

/// Fail-closed classification: the SINGLE gate deciding whether a program
/// stitches and how each clause renders. None = refuse (the caller stays on
/// the interpreter tier). Exhaustive over Step and CmpOp — no wildcard
/// admission arm anywhere in this function.
pub(crate) fn plan_clauses(prog: &Program, ncols: usize) -> Option<Plan> {
    if prog.volatile || prog.steps.is_empty() {
        return None;
    }
    let ncols = ncols.min(MAX_COLS);
    let steps = &prog.steps;
    let mut clauses = Vec::new();
    let mut used_cols: Vec<u16> = Vec::new();
    let mut has_arith = false;
    let use_col = |used: &mut Vec<u16>, col: u16| {
        if !used.contains(&col) {
            used.push(col);
        }
    };
    let col_ok = |col: u16| (col as usize) < ncols;
    let mut lo = 0usize;
    for (ix, s) in steps.iter().enumerate() {
        let Step::Qual { .. } = s else { continue };
        let hi = ix + 1;
        let shape = match &steps[lo..hi] {
            [Step::LoadLane { col, out: r0 }, Step::LoadConst { k, out: r1 }, Step::Cmp { op, a, b, out }, Step::Qual { a: q }]
                if a == r0
                    && b == r1
                    && q == out
                    && r0 != r1
                    && !reg_bad(*r0)
                    && !reg_bad(*r1)
                    && !reg_bad(*out)
                    && col_ok(*col)
                    && (*k as usize) < prog.consts.len()
                    && !prog.consts[*k as usize].isnull
                    && regs_dead_after(steps, hi, &[*r0, *r1, *out]) =>
            {
                use_col(&mut used_cols, *col);
                let konst = prog.consts[*k as usize].value;
                if is_float_cmp(*op) {
                    let (lane_f32, konst_f32) = float_family(*op);
                    let kf = if konst_f32 { konst.as_f32() as f64 } else { konst.as_f64() };
                    if kf.is_nan() {
                        // The fcmp conds and the NEON ordered compares are
                        // exact only for a non-NaN rhs; a NaN konst refuses
                        // (fail closed) rather than emit an unproven body.
                        return None;
                    }
                    ClauseShape::FCmpConst {
                        col: *col,
                        rel: float_rel(*op),
                        konst_bits: kf.to_bits(),
                        lane_f32,
                    }
                } else {
                    ClauseShape::CmpConst { col: *col, op: *op, konst }
                }
            }
            [Step::LoadLane { col: ca, out: r0 }, Step::LoadLane { col: cb, out: r1 }, Step::Cmp { op, a, b, out }, Step::Qual { a: q }]
                if a == r0
                    && b == r1
                    && q == out
                    && r0 != r1
                    && !reg_bad(*r0)
                    && !reg_bad(*r1)
                    && !reg_bad(*out)
                    && col_ok(*ca)
                    && col_ok(*cb)
                    && regs_dead_after(steps, hi, &[*r0, *r1, *out]) =>
            {
                use_col(&mut used_cols, *ca);
                use_col(&mut used_cols, *cb);
                if is_float_cmp(*op) {
                    // Float var-var admits ONLY as this fused shape: the
                    // stencils carry the exact pgf_* NaN-mask formulation
                    // (a NaN rhs is possible per row, unlike the const
                    // shape's baked non-NaN konst). The generic
                    // register-file Cmp stencil still refuses floats.
                    let (a_f32, b_f32) = float_family(*op);
                    ClauseShape::FCmpVar {
                        a_col: *ca,
                        b_col: *cb,
                        rel: float_rel(*op),
                        a_f32,
                        b_f32,
                    }
                } else {
                    ClauseShape::CmpVar { a_col: *ca, b_col: *cb, op: *op }
                }
            }
            [Step::LoadLane { col, out: r0 }, Step::NullTest { a, out: r1, kind }, Step::Qual { a: q }]
                if a == r0
                    && q == r1
                    && !reg_bad(*r0)
                    && !reg_bad(*r1)
                    && col_ok(*col)
                    && regs_dead_after(steps, hi, &[*r0, *r1]) =>
            {
                use_col(&mut used_cols, *col);
                ClauseShape::NullTestQ { col: *col, kind: *kind }
            }
            [Step::LoadLane { col, out: r0 }, Step::BoolTest { a, out: r1, kind }, Step::Qual { a: q }]
                if a == r0
                    && q == r1
                    && !reg_bad(*r0)
                    && !reg_bad(*r1)
                    && col_ok(*col)
                    && regs_dead_after(steps, hi, &[*r0, *r1]) =>
            {
                use_col(&mut used_cols, *col);
                ClauseShape::BoolTestQ { col: *col, kind: *kind }
            }
            [Step::LoadLane { col, out: r0 }, Step::SaopAny { a, out: r1, op, arr }, Step::Qual { a: q }]
                if a == r0
                    && q == r1
                    && !reg_bad(*r0)
                    && !reg_bad(*r1)
                    && col_ok(*col)
                    && !is_float_cmp(*op)
                    && (*arr as usize) < prog.arrays.len()
                    && prog.arrays[*arr as usize].len() <= MAX_SAOP_ELEMS
                    && regs_dead_after(steps, hi, &[*r0, *r1]) =>
            {
                use_col(&mut used_cols, *col);
                ClauseShape::SaopQ { col: *col, op: *op, arr: *arr }
            }
            window => {
                // Generic clause: every step individually classifiable
                // (exhaustive match, no wildcard — fail-closed doctrine).
                // The clause must also be register-SELF-CONTAINED (every
                // read preceded by a write within the clause): the SIMD
                // tier runs each clause as its own row loop, so a register
                // flowing across a clause boundary would carry the wrong
                // row's value. Cross-clause register flow refuses.
                let mut written: Vec<u8> = Vec::new();
                let reads_ok = |written: &Vec<u8>, r: u8| written.contains(&r);
                for step in window {
                    match *step {
                        Step::LoadLane { col, out } => {
                            if reg_bad(out) || !col_ok(col) {
                                return None;
                            }
                            use_col(&mut used_cols, col);
                            written.push(out);
                        }
                        Step::LoadConst { k, out } => {
                            if reg_bad(out) || k as usize >= prog.consts.len() {
                                return None;
                            }
                            written.push(out);
                        }
                        Step::Cmp { op, a, b, out } => {
                            // Float compares are whitelisted ONLY in the
                            // fused const shape: the generic register-file
                            // stencil has no NaN-exact cond for var-var.
                            if reg_bad(a) || reg_bad(b) || reg_bad(out) || is_float_cmp(op) {
                                return None;
                            }
                            if !reads_ok(&written, a) || !reads_ok(&written, b) {
                                return None;
                            }
                            written.push(out);
                        }
                        Step::Arith { op: _, a, b, out } => {
                            if reg_bad(a) || reg_bad(b) || reg_bad(out) {
                                return None;
                            }
                            if !reads_ok(&written, a) || !reads_ok(&written, b) {
                                return None;
                            }
                            has_arith = true;
                            written.push(out);
                        }
                        Step::NullTest { a, out, .. } | Step::BoolTest { a, out, .. } => {
                            if reg_bad(a) || reg_bad(out) || !reads_ok(&written, a) {
                                return None;
                            }
                            written.push(out);
                        }
                        Step::SaopAny { a, out, op, arr } => {
                            // Old-lane admission surface: strict-OR const-array
                            // ANY over a fixed-width by-value comparator. Float
                            // element compares have no NaN-exact scalar cond —
                            // refuse (fail closed). Non-existent array refuses.
                            if reg_bad(a) || reg_bad(out) || !reads_ok(&written, a) {
                                return None;
                            }
                            if is_float_cmp(op) || (arr as usize) >= prog.arrays.len() {
                                return None;
                            }
                            // Code-size bound: an unrolled per-element stencil,
                            // so cap the array (fail closed above it).
                            if prog.arrays[arr as usize].len() > MAX_SAOP_ELEMS {
                                return None;
                            }
                            written.push(out);
                        }
                        Step::Qual { a } => {
                            if reg_bad(a) || !reads_ok(&written, a) {
                                return None;
                            }
                        }
                        // Projection-only step: a qual program never stores
                        // output lanes (fail closed — the qual body has no
                        // outs in its params block).
                        Step::StoreOut { .. } => return None,
                        // WS-AA wave-7: RowOp chain steps never stitch into a
                        // qual body (fail closed — the row-chain tier hosts on
                        // the interp walker; its compiled body deleted at RB-R1).
                        Step::NextRow | Step::ProtocolCall { .. } => return None,
                    }
                }
                ClauseShape::Generic { lo, hi }
            }
        };
        clauses.push(shape);
        lo = hi;
    }
    // Trailing non-Qual steps have no observable effect on a qual segment;
    // refuse rather than guess (fail closed).
    if lo != steps.len() || clauses.is_empty() || used_cols.is_empty() {
        return None;
    }
    Some(Plan { clauses, used_cols, has_arith })
}

// Virtual register file offsets in the stack frame, plus the SIMD spill
// slots (block pass word / block base row / bit-iteration cursor).
const REGFILE_BYTES: u32 = (MAX_REGS as u32) * 16;
const SPILL_MASK: u32 = REGFILE_BYTES;
const SPILL_BASE: u32 = REGFILE_BYTES + 8;
const SPILL_BITS: u32 = REGFILE_BYTES + 16;
const _: () = assert!(REGFILE_BYTES == 256, "frame offsets assume the 256-byte register file");
const _: () = assert!(MAX_ROWS % 64 == 0);

// ---- SVE2 tier frame extension (survivor-index buffer) --------------------
//
// The SVE survivor-extraction path (emit_sve_extract) stores one block's
// surviving row indices densely: 64 u32 slots plus MAX-VL ST1W overstore
// slack (the compact'd index vector stores a full vector under an all-true
// governing predicate; only the CNTP-counted prefix is meaningful). Tier
// detection refuses VL > SVE_MAX_VL_BYTES so the slack bound holds.
const SURV_BUF: u32 = REGFILE_BYTES + 32;
pub(crate) const SVE_MAX_VL_BYTES: usize = 64;
const SURV_BUF_BYTES: u32 = 64 * 4 + SVE_MAX_VL_BYTES as u32;
const SURV_CNT: u32 = SURV_BUF + SURV_BUF_BYTES;
const NEON_FRAME: u32 = REGFILE_BYTES + 32;
const SVE_FRAME: u32 = (SURV_CNT + 8).div_ceil(16) * 16;
const _: () = assert!(NEON_FRAME == 288 && SVE_FRAME == 624);

/// Adaptive crossover: the SVE COMPACT survivor-extraction path engages for
/// a block when the vector pass leaves at least this many survivors of 64.
/// The spike measured the extraction-only crossover band at ~10-13 of 64
/// (notes/sve2-spike-2026-07-14.md, K1: NEON pass-word + rbit/clz wins
/// below ~15% selectivity; SVE COMPACT selectivity-invariant). The
/// PRODUCTION stencils re-measured on c8gd (sve_ab, fleet job
/// pgrust-fast-tests-7414f7a5b4-1783871662) put the whole-pipeline
/// break-even nearer 20% — the per-survivor clause body costs the same on
/// both arms, diluting the extraction margin — with the dense path -1.4%
/// at 30%, -7.8% at 50%, -13.8% at 90%. 16/64 (25%) keeps the marginal
/// band on NEON and takes the wins above it.
pub(crate) const SVE_SURVIVOR_CROSSOVER: u32 = 16;

/// Per-clause cap on SVE2 MATCH IN-list candidates (8 u16 candidates per
/// 128-bit MATCH segment; the spike measured 1.8x at k=4 up to 8.8x at
/// k=64 over the NEON per-candidate CMEQ+ORR stencil).
pub(crate) const MAX_MATCH_ELEMS: usize = 64;

// Bit i -> byte-lane weight 1<<i, the movemask currency.
const MOVEMASK_WEIGHTS: u64 = 0x8040_2010_0804_0201;

fn rv(r: u8) -> u32 {
    r as u32 * 16
}

fn rn(r: u8) -> u32 {
    r as u32 * 16 + 8
}

const PARAMS: u32 = 19;
const ROW: u32 = 20;
const NROWS: u32 = 21;
const SEL: u32 = 23;
// Hoist register pairs (values ptr, isnull ptr) for used_cols[0..2].
const HOIST_PAIRS: [(u32, u32); 2] = [(25, 26), (27, 28)];

/// Kill switch for measurement: PGRUST_LANESTITCH_SIMD=0|off pins the
/// scalar row-loop bodies (read per compile — compiles are rare).
fn simd_enabled() -> bool {
    !matches!(std::env::var("PGRUST_LANESTITCH_SIMD").as_deref(), Ok("0") | Ok("off"))
}

// ---- SVE2 stencil-tier selection ------------------------------------------
//
// Boot-time HWCAP gate (spike §"HWCAP detection"): the SVE2 tier engages
// only when the kernel reports both SVE (AT_HWCAP) and SVE2 (AT_HWCAP2) —
// std's runtime detection reads exactly those getauxval bits on
// linux/aarch64 — and the configured vector length fits the survivor
// buffer's overstore slack. Apple Silicon and non-SVE Graviton read false
// and keep the NEON tier untouched. The emitted SVE stencils are
// VL-agnostic (ptrue-pattern governance, no hardcoded VL strides), so a
// wider future part only needs the slack bound (SVE_MAX_VL_BYTES) raised.

#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
fn sve_vl_bytes() -> usize {
    // prctl(PR_SVE_GET_VL): low 16 bits = current vector length in bytes.
    extern "C" {
        fn prctl(option: i32, ...) -> i32;
    }
    const PR_SVE_GET_VL: i32 = 51;
    let r = unsafe { prctl(PR_SVE_GET_VL) };
    if r < 0 {
        0
    } else {
        (r as usize) & 0xFFFF
    }
}

fn sve2_hw() -> bool {
    #[cfg(all(target_arch = "aarch64", target_os = "linux"))]
    {
        static HW: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *HW.get_or_init(|| {
            std::arch::is_aarch64_feature_detected!("sve")
                && std::arch::is_aarch64_feature_detected!("sve2")
                && (1..=SVE_MAX_VL_BYTES).contains(&sve_vl_bytes())
        })
    }
    #[cfg(not(all(target_arch = "aarch64", target_os = "linux")))]
    false
}

/// The active SIMD stencil tier, read per compile like `simd_enabled`.
/// PGRUST_LANESTITCH_SVE2=0|off pins NEON on SVE2 hardware (A/B and the
/// fleet parity matrix); =force drops the adaptive survivor crossover to 0
/// so every generic-clause block takes the SVE extraction path (parity
/// soak coverage of the SVE stencils at any selectivity). Both are inert
/// off-SVE2-hardware — the tier never engages where it cannot execute.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SimdTier {
    Neon,
    Sve2 { force: bool },
}

pub(crate) fn simd_tier() -> SimdTier {
    if !sve2_hw() {
        return SimdTier::Neon;
    }
    match std::env::var("PGRUST_LANESTITCH_SVE2").as_deref() {
        Ok("0") | Ok("off") => SimdTier::Neon,
        Ok("force") => SimdTier::Sve2 { force: true },
        _ => SimdTier::Sve2 { force: false },
    }
}

/// The u16-domain candidate list of a SaopQ clause the SVE2 MATCH stencil
/// admits, or None (fail closed to the NEON per-candidate stencil).
/// Admission: an Eq-relation comparator, 1..=MAX_MATCH_ELEMS non-NULL
/// elements, every element in [0, 0xFFFF] as its canonical 64-bit Datum
/// image. Exactness without lane-domain metadata: for elem in [0, 0xFFFF],
/// datum == elem (64-bit canonical compare, any admitted int/oid width)
/// iff low16(datum) == low16(elem) AND the upper 48 datum bits are zero —
/// the stencil ANDs the MATCH mask with that in-domain mask, so
/// out-of-domain lane values (including sign-extended negatives) never
/// false-match.
fn saop_match_elems(prog: &Program, op: CmpOp, arr: u16) -> Option<Vec<u16>> {
    if vcmp(op) != VCmp::Eq {
        return None;
    }
    let elems = saop_vector_elems(prog, arr);
    if elems.is_empty() || elems.len() > MAX_MATCH_ELEMS {
        return None;
    }
    elems
        .iter()
        .map(|d| {
            let v = d.as_u64();
            if v <= 0xFFFF {
                Some(v as u16)
            } else {
                None
            }
        })
        .collect()
}

/// z-registers available for block-resident MATCH candidate vectors:
/// v18-v23 always (untouched by the NEON group stencils), plus whatever
/// remains of the v24-v28 constant bank after CmpConst/FCmpConst dups.
/// 8 candidates per register (two packed u64s ZIP1'd into segment 0).
fn match_free_regs(plan: &Plan) -> Vec<u32> {
    let nconst = plan
        .clauses
        .iter()
        .filter(|c| matches!(c, ClauseShape::CmpConst { .. } | ClauseShape::FCmpConst { .. }))
        .count() as u32;
    (18..24u32).chain(24 + nconst..29).collect()
}

/// Per-clause MATCH assignment: candidate u16 lists for the SaopQ clauses
/// the register budget admits, in clause order (greedy; the rest keep the
/// NEON stencil). Deterministic — emit and telemetry share it.
fn assign_match_clauses(prog: &Program, plan: &Plan, tier: SimdTier) -> Vec<Option<Vec<u16>>> {
    let mut out: Vec<Option<Vec<u16>>> = Vec::with_capacity(plan.clauses.len());
    if !matches!(tier, SimdTier::Sve2 { .. }) {
        out.resize_with(plan.clauses.len(), || None);
        return out;
    }
    let mut budget = match_free_regs(plan).len();
    for c in &plan.clauses {
        let assigned = match *c {
            ClauseShape::SaopQ { op, arr, .. } => match saop_match_elems(prog, op, arr) {
                Some(elems) if elems.len().div_ceil(8) <= budget => {
                    budget -= elems.len().div_ceil(8);
                    Some(elems)
                }
                _ => None,
            },
            _ => None,
        };
        out.push(assigned);
    }
    out
}

struct Ctx<'a> {
    lay: &'a ParamsLayout,
    hoist: Vec<(u16, u32, u32)>,
    row_fail: Label,
    row_next: Label,
    refuse: Label,
}

impl<'a> Ctx<'a> {
    /// Same binding context, different per-row fail/next targets (the SIMD
    /// bit-iteration loops re-aim the shared step stencils).
    fn with_row_labels(&self, row_fail: Label, row_next: Label) -> Ctx<'a> {
        Ctx { lay: self.lay, hoist: self.hoist.clone(), row_fail, row_next, refuse: self.refuse }
    }
}

fn lane_p0(ctx: &Ctx<'_>, col: u16) -> u32 {
    col as u32 * ctx.lay.lane_stride + ctx.lay.lane_p0
}

fn lane_isnull(ctx: &Ctx<'_>, col: u16) -> u32 {
    col as u32 * ctx.lay.lane_stride + ctx.lay.lane_isnull
}

fn out_p0(ctx: &Ctx<'_>, out: u16) -> u32 {
    ctx.lay.outs_base + out as u32 * ctx.lay.lane_stride + ctx.lay.lane_p0
}

fn out_isnull(ctx: &Ctx<'_>, out: u16) -> u32 {
    ctx.lay.outs_base + out as u32 * ctx.lay.lane_stride + ctx.lay.lane_isnull
}

/// The lane's values base pointer: a hoisted callee-saved register, or a
/// params load into `scratch`.
fn lane_p0_reg(e: &mut Emitter, ctx: &Ctx<'_>, col: u16, scratch: u32) -> u32 {
    match ctx.hoist.iter().find(|&&(c, _, _)| c == col) {
        Some(&(_, p0, _)) => p0,
        None => {
            e.ldr_x(scratch, PARAMS, lane_p0(ctx, col));
            scratch
        }
    }
}

fn lane_isnull_reg(e: &mut Emitter, ctx: &Ctx<'_>, col: u16, scratch: u32) -> u32 {
    match ctx.hoist.iter().find(|&&(c, _, _)| c == col) {
        Some(&(_, _, nul)) => nul,
        None => {
            e.ldr_x(scratch, PARAMS, lane_isnull(ctx, col));
            scratch
        }
    }
}

/// Emits the fused body for a classified plan. Always emit-sel (the qual
/// bitmap is the segment's output currency).
pub(crate) fn emit_pipeline(prog: &Program, plan: &Plan, lay: &ParamsLayout) -> Vec<u32> {
    let mut e = Emitter::new();

    let simd = classify_simd(plan);
    let tier = simd_tier();
    // The SVE survivor-extraction path exists only when a SIMD body has
    // non-vector clauses to run per surviving row; only then does the frame
    // grow the survivor-index buffer.
    let sve_survivors = simd
        && matches!(tier, SimdTier::Sve2 { .. })
        && plan.clauses.iter().any(|c| !vector_pass_clause(c));
    let frame = if sve_survivors { SVE_FRAME } else { NEON_FRAME };

    // Prologue: fp/lr chain kept (unwind guidance), x19-x28 saved, 256-byte
    // virtual register file + 32 spill bytes (+ the SVE survivor buffer)
    // below.
    e.raw(0xA9BA_7BFD); // stp x29, x30, [sp, #-0x60]!
    e.raw(0x9100_03FD); // mov x29, sp
    e.raw(0xA901_53F3); // stp x19, x20, [sp, #0x10]
    e.raw(0xA902_5BF5); // stp x21, x22, [sp, #0x20]
    e.raw(0xA903_63F7); // stp x23, x24, [sp, #0x30]
    e.raw(0xA904_6BF9); // stp x25, x26, [sp, #0x40]
    e.raw(0xA905_73FB); // stp x27, x28, [sp, #0x50]
    e.sub_sp_imm(frame);
    e.mov_x(PARAMS, 0);
    e.ldr_x(NROWS, PARAMS, lay.nrows);
    e.ldr_x(SEL, PARAMS, lay.sel);

    // Loop-invariant lane base pointers for the first two used columns.
    let hoist: Vec<(u16, u32, u32)> = plan
        .used_cols
        .iter()
        .take(HOIST_PAIRS.len())
        .zip(HOIST_PAIRS)
        .map(|(&col, (p0, nul))| (col, p0, nul))
        .collect();
    let mut ctx = Ctx { lay, hoist, row_fail: Label(0), row_next: Label(0), refuse: Label(0) };
    for &(col, p0, nul) in &ctx.hoist {
        e.ldr_x(p0, PARAMS, lane_p0(&ctx, col));
        e.ldr_x(nul, PARAMS, lane_isnull(&ctx, col));
    }
    e.mov_x(ROW, 31); // i = 0

    let loop_head = e.new_label();
    ctx.row_next = e.new_label();
    ctx.row_fail = e.new_label();
    let exit_ok = e.new_label();
    ctx.refuse = e.new_label();
    let (row_next, row_fail) = (ctx.row_next, ctx.row_fail);
    let refuse = ctx.refuse;
    let ctx = ctx;

    // SIMD tier: 64-row blocks while a full block remains; leaves ROW at the
    // block-aligned frontier so the scalar row loop finishes the n % 64 tail
    // (and everything, when the shape refuses SIMD).
    if simd {
        emit_simd_blocks(&mut e, &ctx, prog, plan, tier);
    }

    e.bind(loop_head);
    e.cmp_x_x(ROW, NROWS);
    e.b_cond(Cond::Ge, exit_ok);

    for shape in &plan.clauses {
        emit_clause(&mut e, &ctx, prog, shape);
    }

    e.bind(row_next);
    e.add_x_imm(ROW, ROW, 1);
    e.b(loop_head);

    e.bind(row_fail);
    // Input sel is all-ones (driver contract): only failures write.
    e.lsr_x_6(8, ROW);
    e.ldr_x_idx3(9, SEL, 8);
    e.and_x_63(10, ROW);
    e.movz_x(11, 1);
    e.lslv_x(11, 11, 10);
    e.bic_x(9, 9, 11);
    e.str_x_idx3(9, SEL, 8);
    e.b(row_next);

    e.bind(exit_ok);
    e.movz_x(0, 0); // RC_OK
    let epilogue = e.new_label();
    e.b(epilogue);
    e.bind(refuse);
    e.movn_x(0, 0); // RC_REFUSE = -1
    e.bind(epilogue);
    e.add_sp_imm(frame);
    e.raw(0xA941_53F3); // ldp x19, x20, [sp, #0x10]
    e.raw(0xA942_5BF5); // ldp x21, x22, [sp, #0x20]
    e.raw(0xA943_63F7); // ldp x23, x24, [sp, #0x30]
    e.raw(0xA944_6BF9); // ldp x25, x26, [sp, #0x40]
    e.raw(0xA945_73FB); // ldp x27, x28, [sp, #0x50]
    e.raw(0xA8C6_7BFD); // ldp x29, x30, [sp], #0x60
    e.ret();

    e.finish()
}

/// One clause of the scalar row-loop (or bit-iteration) section.
fn emit_clause(e: &mut Emitter, ctx: &Ctx<'_>, prog: &Program, shape: &ClauseShape) {
    match *shape {
        ClauseShape::CmpConst { col, op, konst } => {
            let nul = lane_isnull_reg(e, ctx, col, 8);
            e.ldrb_idx(10, nul, ROW);
            e.cbnz_w(10, ctx.row_fail);
            let p0 = lane_p0_reg(e, ctx, col, 8);
            e.ldr_x_idx3(11, p0, ROW);
            emit_cmp_konst_tail(e, ctx, op, konst);
        }
        ClauseShape::FCmpConst { col, rel, konst_bits, lane_f32 } => {
            let nul = lane_isnull_reg(e, ctx, col, 8);
            e.ldrb_idx(10, nul, ROW);
            e.cbnz_w(10, ctx.row_fail);
            let p0 = lane_p0_reg(e, ctx, col, 8);
            e.ldr_x_idx3(11, p0, ROW);
            if lane_f32 {
                e.fmov_s_w(0, 11);
                e.fcvt_d_s(0, 0);
            } else {
                e.fmov_d_x(0, 11);
            }
            e.ldr_lit(12, konst_bits);
            e.fmov_d_x(1, 12);
            e.fcmp_d(0, 1);
            e.b_cond(float_cond(rel).inv(), ctx.row_fail);
        }
        ClauseShape::CmpVar { a_col, b_col, op } => {
            let nul_a = lane_isnull_reg(e, ctx, a_col, 8);
            e.ldrb_idx(10, nul_a, ROW);
            let nul_b = lane_isnull_reg(e, ctx, b_col, 8);
            e.ldrb_idx(13, nul_b, ROW);
            e.orr_w(10, 10, 13);
            e.cbnz_w(10, ctx.row_fail);
            let p0_a = lane_p0_reg(e, ctx, a_col, 8);
            e.ldr_x_idx3(11, p0_a, ROW);
            let p0_b = lane_p0_reg(e, ctx, b_col, 8);
            e.ldr_x_idx3(12, p0_b, ROW);
            let (wide, cond) = cmp_cond(op);
            if wide {
                e.cmp_x_x(11, 12);
            } else {
                e.cmp_w_w(11, 12);
            }
            e.b_cond(cond.inv(), ctx.row_fail);
        }
        ClauseShape::FCmpVar { a_col, b_col, rel, a_f32, b_f32 } => {
            let nul_a = lane_isnull_reg(e, ctx, a_col, 8);
            e.ldrb_idx(10, nul_a, ROW);
            let nul_b = lane_isnull_reg(e, ctx, b_col, 8);
            e.ldrb_idx(13, nul_b, ROW);
            e.orr_w(10, 10, 13);
            e.cbnz_w(10, ctx.row_fail);
            let p0_a = lane_p0_reg(e, ctx, a_col, 8);
            e.ldr_x_idx3(11, p0_a, ROW);
            let p0_b = lane_p0_reg(e, ctx, b_col, 8);
            e.ldr_x_idx3(12, p0_b, ROW);
            if a_f32 {
                e.fmov_s_w(0, 11);
                e.fcvt_d_s(0, 0);
            } else {
                e.fmov_d_x(0, 11);
            }
            if b_f32 {
                e.fmov_s_w(1, 12);
                e.fcvt_d_s(1, 1);
            } else {
                e.fmov_d_x(1, 12);
            }
            // Exact pgf_* var-var relations from at most three fcmp probes
            // (fcmp x,x sets V iff x is NaN; a mixed-NaN pair is unordered).
            //   pgf_eq(a,b) = ordered(a==b) | (nan_a & nan_b)
            //   pgf_lt(a,b) = ordered(a<b)  | (nan_b & !nan_a)
            //   pgf_le(a,b) = nan_b | ordered(a<=b)
            //   pgf_gt(a,b) = ordered(a>b)  | (nan_a & !nan_b)
            //   pgf_ge(a,b) = nan_a | ordered(a>=b)
            //   pgf_ne      = !pgf_eq
            // After `fcmp d0, d1` the ordered relations read exactly as
            // their integer conds (unordered NZCV=0011 reads false on
            // Eq/Mi/Ls/Gt/Ge), so each relation is one ordered probe plus
            // the NaN arm spelled out below.
            let lpass = e.new_label();
            match rel {
                FRel::Eq => {
                    e.fcmp_d(0, 1);
                    e.b_cond(Cond::Eq, lpass); // ordered equal
                    e.fcmp_d(0, 0);
                    e.b_cond(Cond::Vc, ctx.row_fail); // a non-NaN: fail
                    e.fcmp_d(1, 1);
                    e.b_cond(Cond::Vc, ctx.row_fail); // b non-NaN: fail
                }
                FRel::Ne => {
                    e.fcmp_d(0, 1);
                    e.b_cond(Cond::Eq, ctx.row_fail); // ordered equal: fail
                    e.fcmp_d(0, 0);
                    e.b_cond(Cond::Vc, lpass); // a non-NaN: can't be both-NaN
                    e.fcmp_d(1, 1);
                    e.b_cond(Cond::Vs, ctx.row_fail); // both NaN: equal: fail
                }
                FRel::Lt => {
                    e.fcmp_d(0, 1);
                    e.b_cond(Cond::Mi, lpass); // ordered a < b
                    e.fcmp_d(1, 1);
                    e.b_cond(Cond::Vc, ctx.row_fail); // b non-NaN: fail
                    e.fcmp_d(0, 0);
                    e.b_cond(Cond::Vs, ctx.row_fail); // a NaN too: fail
                }
                FRel::Le => {
                    e.fcmp_d(1, 1);
                    e.b_cond(Cond::Vs, lpass); // b NaN: everything <= NaN
                    e.fcmp_d(0, 1);
                    e.b_cond(Cond::Hi, ctx.row_fail); // !(ordered a <= b)
                }
                FRel::Gt => {
                    e.fcmp_d(0, 1);
                    e.b_cond(Cond::Gt, lpass); // ordered a > b
                    e.fcmp_d(0, 0);
                    e.b_cond(Cond::Vc, ctx.row_fail); // a non-NaN: fail
                    e.fcmp_d(1, 1);
                    e.b_cond(Cond::Vs, ctx.row_fail); // b NaN too: fail
                }
                FRel::Ge => {
                    e.fcmp_d(0, 0);
                    e.b_cond(Cond::Vs, lpass); // a NaN: NaN >= everything
                    e.fcmp_d(0, 1);
                    e.b_cond(Cond::Lt, ctx.row_fail); // !(ordered a >= b)
                }
            }
            e.bind(lpass);
        }
        ClauseShape::NullTestQ { col, kind } => {
            let nul = lane_isnull_reg(e, ctx, col, 8);
            e.ldrb_idx(10, nul, ROW);
            match kind {
                NullTestKind::IsNull => e.cbz_w(10, ctx.row_fail),
                NullTestKind::IsNotNull => e.cbnz_w(10, ctx.row_fail),
            }
        }
        ClauseShape::BoolTestQ { col, kind } => {
            use BoolTestKind::*;
            let nul = lane_isnull_reg(e, ctx, col, 8);
            e.ldrb_idx(10, nul, ROW);
            let lpass = e.new_label();
            // The NOT kinds pass outright on a NULL row; the plain kinds
            // fail outright. Then truthiness (value word != 0, DatumGetBool)
            // decides: IsTrue/IsNotFalse need it set, IsFalse/IsNotTrue
            // need it clear.
            match kind {
                IsTrue | IsFalse => e.cbnz_w(10, ctx.row_fail),
                IsNotTrue | IsNotFalse => e.cbnz_w(10, lpass),
            }
            let p0 = lane_p0_reg(e, ctx, col, 8);
            e.ldr_x_idx3(11, p0, ROW);
            match kind {
                IsTrue | IsNotFalse => e.cbz_x(11, ctx.row_fail),
                IsFalse | IsNotTrue => e.cbnz_x(11, ctx.row_fail),
            }
            e.bind(lpass);
        }
        ClauseShape::SaopQ { col, op, arr } => {
            // Fused qual form: pass iff the scalar is non-NULL and some
            // non-NULL element matches (NULL SAOP results fail a qual like
            // false, so NULL elements need no code at all here).
            let nul = lane_isnull_reg(e, ctx, col, 8);
            e.ldrb_idx(10, nul, ROW);
            e.cbnz_w(10, ctx.row_fail);
            let p0 = lane_p0_reg(e, ctx, col, 8);
            e.ldr_x_idx3(11, p0, ROW);
            let (wide, cond) = cmp_cond(op);
            let lpass = e.new_label();
            for elem in prog.arrays[arr as usize].iter().filter(|e| !e.isnull) {
                e.ldr_lit(12, elem.value.as_usize() as u64);
                if wide {
                    e.cmp_x_x(11, 12);
                } else {
                    e.cmp_w_w(11, 12);
                }
                e.b_cond(cond, lpass);
            }
            e.b(ctx.row_fail); // no element matched (or none exist)
            e.bind(lpass);
        }
        ClauseShape::Generic { lo, hi } => {
            for step in &prog.steps[lo..hi] {
                emit_step(e, ctx, prog, step);
            }
        }
    }
}

/// Compare x11 against the baked constant; fail -> ctx.row_fail.
fn emit_cmp_konst_tail(e: &mut Emitter, ctx: &Ctx<'_>, op: CmpOp, konst: Datum) {
    let (wide, cond) = cmp_cond(op);
    if wide {
        e.ldr_lit(12, konst.as_usize() as u64);
        e.cmp_x_x(11, 12);
    } else {
        let v = konst.as_i32();
        if (0..=4095).contains(&v) {
            e.cmp_w_imm(11, v as u32);
        } else {
            e.ldr_lit(12, konst.as_usize() as u64);
            e.cmp_w_w(11, 12);
        }
    }
    e.b_cond(cond.inv(), ctx.row_fail);
}

/// The strict-OR ScalarArrayOpExpr stencil: unrolls one compare per element
/// into a running match word (res) and a running null word (resnull), then
/// collapses to the register-file result (value = res; isnull = resnull &&
/// !res). Non-erroring — the whitelisted comparators never trap. Reads x9-x16
/// scratch only.
fn emit_saop(e: &mut Emitter, _ctx: &Ctx<'_>, prog: &Program, a: u8, out: u8, op: CmpOp, arr: u16) {
    let elems = &prog.arrays[arr as usize];
    let (wide, cond) = cmp_cond(op);
    let lscalarnull = e.new_label();
    let lcombine = e.new_label();
    e.movz_x(14, 0); // res = false
    e.movz_x(15, 0); // resnull = false
    e.ldrb(10, 31, rn(a)); // scalar isnull byte
    e.cbnz_w(10, lscalarnull);
    e.ldr_x(12, 31, rv(a)); // scalar value
    for elem in elems {
        if elem.isnull {
            e.movz_x(15, 1); // a NULL element can only contribute NULL
        } else {
            e.ldr_lit(9, elem.value.as_usize() as u64);
            if wide {
                e.cmp_x_x(12, 9);
            } else {
                e.cmp_w_w(12, 9);
            }
            e.cset_x(16, cond);
            e.orr_x(14, 14, 16); // res |= (scalar op elem)
        }
    }
    e.b(lcombine);
    e.bind(lscalarnull);
    // Scalar NULL: every strict compare is NULL — the array being non-empty
    // makes the whole result NULL (else it stays false).
    if !elems.is_empty() {
        e.movz_x(15, 1);
    }
    e.bind(lcombine);
    e.str_x(14, 31, rv(out));
    e.bic_x(9, 15, 14); // isnull = resnull & !res
    e.strb(9, 31, rn(out));
}

/// Generic register-file stencils: exhaustive over the vocabulary.
fn emit_step(e: &mut Emitter, ctx: &Ctx<'_>, prog: &Program, step: &Step) {
    match *step {
        Step::LoadLane { col, out } => {
            let p0 = lane_p0_reg(e, ctx, col, 8);
            e.ldr_x_idx3(9, p0, ROW);
            e.str_x(9, 31, rv(out));
            let nul = lane_isnull_reg(e, ctx, col, 8);
            e.ldrb_idx(10, nul, ROW);
            e.strb(10, 31, rn(out));
        }
        Step::LoadConst { k, out } => {
            let c = prog.consts[k as usize];
            if c.value.as_usize() == 0 {
                e.str_x(31, 31, rv(out));
            } else {
                e.ldr_lit(9, c.value.as_usize() as u64);
                e.str_x(9, 31, rv(out));
            }
            if c.isnull {
                e.movz_w(10, 1);
                e.strb(10, 31, rn(out));
            } else {
                e.strb(31, 31, rn(out));
            }
        }
        Step::Cmp { op, a, b, out } => {
            let lnull = e.new_label();
            let ldone = e.new_label();
            e.ldrb(10, 31, rn(a));
            e.ldrb(11, 31, rn(b));
            e.orr_w(10, 10, 11);
            e.cbnz_w(10, lnull);
            e.ldr_x(12, 31, rv(a));
            e.ldr_x(13, 31, rv(b));
            let (wide, cond) = cmp_cond(op);
            if wide {
                e.cmp_x_x(12, 13);
            } else {
                e.cmp_w_w(12, 13);
            }
            e.cset_x(14, cond);
            e.str_x(14, 31, rv(out));
            e.strb(31, 31, rn(out));
            e.b(ldone);
            e.bind(lnull);
            e.str_x(31, 31, rv(out));
            e.movz_w(14, 1);
            e.strb(14, 31, rn(out));
            e.bind(ldone);
        }
        Step::Arith { op, a, b, out } => {
            // Refuse-and-replay: any trap condition exits the body with
            // RC_REFUSE — no stitched error construction (the interpreter
            // replay owns error identity and position). Width-dispatched:
            // int2 range-checks with sxth, int4 with sxtw, int8 with the
            // V flag (add/sub) or smulh (mul); every div refuses on the
            // zero and MIN/-1 traps and lets the replay pick the message.
            use ArithOp::*;
            let lnull = e.new_label();
            let ldone = e.new_label();
            e.ldrb(10, 31, rn(a));
            e.ldrb(11, 31, rn(b));
            e.orr_w(10, 10, 11);
            e.cbnz_w(10, lnull);
            e.ldr_x(12, 31, rv(a));
            e.ldr_x(13, 31, rv(b));
            let width = op.width();
            match op {
                // ---- int2 (smallint): compute in w-regs, range-check i16.
                Add2 | Sub2 | Mul2 => {
                    match op {
                        Add2 => e.adds_w(14, 12, 13),
                        Sub2 => e.subs_w(14, 12, 13),
                        _ => e.mul_w(14, 12, 13),
                    }
                    // Two i16 operands never overflow i32 under add/sub/mul,
                    // so the i16 range is the only trap: sxth != value.
                    e.sxth_w(15, 14);
                    e.cmp_w_w(14, 15);
                    e.b_cond(Cond::Ne, ctx.refuse);
                }
                Div2 => {
                    e.cbz_w(13, ctx.refuse);
                    let ldiv = e.new_label();
                    e.cmn_w_imm(13, 1); // b == -1?
                    e.b_cond(Cond::Ne, ldiv);
                    e.movn_w(15, 0x7FFF); // i16::MIN
                    e.cmp_w_w(12, 15);
                    e.b_cond(Cond::Eq, ctx.refuse);
                    e.bind(ldiv);
                    e.sdiv_w(14, 12, 13);
                }
                // ---- int4 (integer).
                Add4 => {
                    e.adds_w(14, 12, 13);
                    e.b_cond(Cond::Vs, ctx.refuse);
                }
                Sub4 => {
                    e.subs_w(14, 12, 13);
                    e.b_cond(Cond::Vs, ctx.refuse);
                }
                Mul4 => {
                    e.smull(14, 12, 13);
                    e.cmp_x_w_sxtw(14, 14);
                    e.b_cond(Cond::Ne, ctx.refuse);
                }
                Div4 => {
                    e.cbz_w(13, ctx.refuse);
                    let ldiv = e.new_label();
                    e.cmn_w_imm(13, 1);
                    e.b_cond(Cond::Ne, ldiv);
                    e.movz_w_hw1(15, 0x8000); // i32::MIN
                    e.cmp_w_w(12, 15);
                    e.b_cond(Cond::Eq, ctx.refuse);
                    e.bind(ldiv);
                    e.sdiv_w(14, 12, 13);
                }
                // ---- int8 (bigint): full-width x-reg ops.
                Add8 => {
                    e.adds_x(14, 12, 13);
                    e.b_cond(Cond::Vs, ctx.refuse);
                }
                Sub8 => {
                    e.subs_x(14, 12, 13);
                    e.b_cond(Cond::Vs, ctx.refuse);
                }
                Mul8 => {
                    // Overflow iff the signed high half != the sign-replicate
                    // of the low half (the standard smulh check).
                    e.mul_x(14, 12, 13);
                    e.smulh(15, 12, 13);
                    e.asr_x_63(16, 14);
                    e.cmp_x_x(15, 16);
                    e.b_cond(Cond::Ne, ctx.refuse);
                }
                Div8 => {
                    e.cbz_x(13, ctx.refuse);
                    let ldiv = e.new_label();
                    e.cmn_x_imm(13, 1); // b == -1?
                    e.b_cond(Cond::Ne, ldiv);
                    // a == i64::MIN? negating it overflows (cmp xzr, a sets V).
                    e.cmp_x_x(31, 12);
                    e.b_cond(Cond::Vs, ctx.refuse);
                    e.bind(ldiv);
                    e.sdiv_x(14, 12, 13);
                }
            }
            // int2/int4 results canonicalize to the full word via sxtw
            // (from_iN parity); int8 already fills the word.
            if width != 8 {
                e.sxtw(14, 14);
            }
            e.str_x(14, 31, rv(out));
            e.strb(31, 31, rn(out));
            e.b(ldone);
            e.bind(lnull);
            e.str_x(31, 31, rv(out));
            e.movz_w(14, 1);
            e.strb(14, 31, rn(out));
            e.bind(ldone);
        }
        Step::NullTest { a, out, kind } => {
            // Non-erroring, non-NULL: out.value = (isnull ? / !isnull ?), a
            // pure function of the operand's null byte.
            e.ldrb(10, 31, rn(a));
            e.cmp_x_imm(10, 0);
            let cond = match kind {
                crate::spec::NullTestKind::IsNull => Cond::Ne,
                crate::spec::NullTestKind::IsNotNull => Cond::Eq,
            };
            e.cset_x(14, cond);
            e.str_x(14, 31, rv(out));
            e.strb(31, 31, rn(out));
        }
        Step::BoolTest { a, out, kind } => {
            // Non-erroring, non-NULL BooleanTest collapse. nn = !isnull,
            // truthy = (value != 0) (DatumGetBool). Each kind is one AND/OR
            // of nn/notnull with truthy/falsy.
            use crate::spec::BoolTestKind::*;
            e.ldrb(10, 31, rn(a)); // isnull byte (0/1)
            e.ldr_x(11, 31, rv(a)); // value word
            e.cmp_x_imm(10, 0);
            // nn/notnull selector.
            let null_cond = match kind {
                IsTrue | IsFalse => Cond::Eq, // nn = isnull == 0
                IsNotTrue | IsNotFalse => Cond::Ne, // notnull = isnull != 0
            };
            e.cset_x(12, null_cond);
            e.cmp_x_imm(11, 0);
            // truthy/falsy selector.
            let val_cond = match kind {
                IsTrue | IsNotFalse => Cond::Ne, // value != 0 (truthy)
                IsFalse | IsNotTrue => Cond::Eq,  // value == 0 (falsy)
            };
            e.cset_x(13, val_cond);
            match kind {
                IsTrue | IsFalse => e.and_x(14, 12, 13),
                IsNotTrue | IsNotFalse => e.orr_x(14, 12, 13),
            }
            e.str_x(14, 31, rv(out));
            e.strb(31, 31, rn(out));
        }
        Step::SaopAny { a, out, op, arr } => {
            emit_saop(e, ctx, prog, a, out, op, arr);
        }
        Step::Qual { a } => {
            e.ldrb(10, 31, rn(a));
            e.cbnz_w(10, ctx.row_fail);
            e.ldr_x(11, 31, rv(a));
            e.cbz_x(11, ctx.row_fail);
        }
        Step::StoreOut { a, out } => {
            // Projection bodies only (plan_clauses refuses StoreOut in qual
            // programs, so a qual body never reaches this arm). Out lane base
            // pointers load from the params block per store — projections
            // hoist input lanes, not outputs.
            e.ldr_x(8, PARAMS, out_p0(ctx, out));
            e.ldr_x(9, 31, rv(a));
            e.str_x_idx3(9, 8, ROW);
            e.ldr_x(8, PARAMS, out_isnull(ctx, out));
            e.ldrb(10, 31, rn(a));
            e.strb_idx(10, 8, ROW);
        }
        // WS-AA wave-7: both planners refuse RowOp steps, so no qual or
        // projection body ever reaches these arms (the row-chain tier hosts
        // on the interp walker; its compiled body deleted at RB-R1).
        Step::NextRow | Step::ProtocolCall { .. } => {
            unreachable!("RowOp step in a qual/projection body (planner refuses)")
        }
    }
}

// ---- SIMD tier ----------------------------------------------------------
//
// Vector register convention inside the block loop (v8-v15 are callee-saved
// and never touched): v0-v7 lane data, v16-v17 null/nan/mask scratch,
// v24-v28 dup'd clause constants, v29 clause-AND byte mask, v30 weights.
// GPR convention: x9 pass word, x10 row cursor, x11 shift, x12-x16 scratch;
// ROW (x20) holds the block base except inside bit-iteration bodies, which
// set it to the surviving row and reload the base from the spill afterwards.

#[derive(Clone, Copy, PartialEq, Eq)]
enum VCmp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    // Unsigned forms (oid): exact only under the both-operands-
    // sign-extended contract stated at cmp_cond.
    Hi,
    Hs,
    Lo,
    Ls,
}

/// Width-blind: canonical sign-extension makes the 2x64 signed compare
/// exact for the narrow families too, and the unsigned Hi/Hs/Lo/Ls forms
/// exact for oid. Reachable only for non-float ops (plan construction).
fn vcmp(op: CmpOp) -> VCmp {
    use CmpOp::*;
    match op {
        Int4Eq | Int8Eq | Int2Eq | Int84Eq | Int48Eq | Int24Eq | Int42Eq | OidEq => VCmp::Eq,
        Int4Ne | Int8Ne | Int2Ne | Int84Ne | Int48Ne | Int24Ne | Int42Ne | OidNe => VCmp::Ne,
        Int4Gt | Int8Gt | Int2Gt | Int84Gt | Int48Gt | Int24Gt | Int42Gt => VCmp::Gt,
        Int4Ge | Int8Ge | Int2Ge | Int84Ge | Int48Ge | Int24Ge | Int42Ge => VCmp::Ge,
        Int4Lt | Int8Lt | Int2Lt | Int84Lt | Int48Lt | Int24Lt | Int42Lt => VCmp::Lt,
        Int4Le | Int8Le | Int2Le | Int84Le | Int48Le | Int24Le | Int42Le => VCmp::Le,
        OidGt => VCmp::Hi,
        OidGe => VCmp::Hs,
        OidLt => VCmp::Lo,
        OidLe => VCmp::Ls,
        op => unreachable!("vcmp on a float comparator (float={})", is_float_cmp(op)),
    }
}

/// True = this clause has a NEON vector-pass arm in emit_simd_blocks; false
/// = inside a SIMD body it runs per-row over the pass word's surviving bits
/// (Generic clauses). SaopQ is vector-pass at EVERY admitted length: the
/// admission-economics bench (tests/simd_econ.rs, M-series, 1024-row
/// batches) has the SIMD form beating the scalar unrolled loop at every k
/// up to MAX_SAOP_ELEMS (8.8x at k=1, 1.4x at k=16, 1.22x at k=128), so no
/// separate SIMD cap exists — narrowing it later means adding (and testing)
/// a fused-clause bits path here.
fn vector_pass_clause(c: &ClauseShape) -> bool {
    !matches!(c, ClauseShape::Generic { .. })
}

/// The elements a SaopQ vector arm compares: NULL elements contribute
/// nothing to a qual (a NULL SAOP result fails exactly like false), so only
/// non-NULL elements count against the vector-tier cap or emit code.
fn saop_vector_elems(prog: &Program, arr: u16) -> Vec<Datum> {
    prog.arrays[arr as usize].iter().filter(|e| !e.isnull).map(|e| e.value).collect()
}

/// SIMD legality for a whole plan: every clause pure and non-erroring
/// (no Arith anywhere — the header's reordering argument), at least one
/// vector-pass clause, and at most 5 distinct hoisted compare constants
/// (v24-v28; SaopQ and FCmpVar dup their operands per group, not here).
fn classify_simd(plan: &Plan) -> bool {
    if !simd_enabled() || plan.has_arith {
        return false;
    }
    let mut nconst = 0u32;
    let mut any_simd = false;
    for c in &plan.clauses {
        if let ClauseShape::CmpConst { .. } | ClauseShape::FCmpConst { .. } = c {
            nconst += 1;
        }
        any_simd |= vector_pass_clause(c);
    }
    any_simd && nconst <= 5
}

/// 8 Datums compared in v0-v3 against `rhs(i)`; Lt/Le swap operands.
fn emit_cmp_quad_2d(e: &mut Emitter, v: VCmp, rhs: impl Fn(u32) -> u32) {
    for va in 0..4u32 {
        let vb = rhs(va);
        match v {
            VCmp::Eq | VCmp::Ne => e.cmeq_2d(va, va, vb),
            VCmp::Gt => e.cmgt_2d(va, va, vb),
            VCmp::Ge => e.cmge_2d(va, va, vb),
            VCmp::Lt => e.cmgt_2d(va, vb, va),
            VCmp::Le => e.cmge_2d(va, vb, va),
            VCmp::Hi => e.cmhi_2d(va, va, vb),
            VCmp::Hs => e.cmhs_2d(va, va, vb),
            VCmp::Lo => e.cmhi_2d(va, vb, va),
            VCmp::Ls => e.cmhs_2d(va, vb, va),
        }
    }
}

/// 8 float Datums in v0-v3 (already f64 lanes) against the dup'd konst in
/// `kreg` (non-NaN by classification). The pgf_* truth table restricted to
/// a non-NaN rhs: gt/ge OR in the lane's isnan mask; lt/le/eq are
/// ordered-only (NaN lane reads false); ne complements eq.
fn emit_fcmp_quad_2d(e: &mut Emitter, rel: FRel, kreg: u32) {
    for va in 0..4u32 {
        match rel {
            FRel::Eq => e.fcmeq_2d(va, va, kreg),
            FRel::Ne => {
                e.fcmeq_2d(va, va, kreg);
                e.not_16b(va, va);
            }
            FRel::Gt | FRel::Ge => {
                // v16 = isnan(lane): !(lane == lane).
                e.fcmeq_2d(16, va, va);
                e.not_16b(16, 16);
                if rel == FRel::Gt {
                    e.fcmgt_2d(va, va, kreg);
                } else {
                    e.fcmge_2d(va, va, kreg);
                }
                e.orr_16b(va, va, 16);
            }
            FRel::Lt => e.fcmgt_2d(va, kreg, va),
            FRel::Le => e.fcmge_2d(va, kreg, va),
        }
    }
}

/// 8 float var-var comparisons: a-lanes in v0-v3 vs b-lanes in v4-v7 (all
/// already f64), results into v0-v3. Exact pgf_* NaN-ordering semantics
/// from ordered NEON compares plus explicit NaN masks (fcmeq x,x is the
/// nonnan mask: false exactly on NaN lanes):
///   pgf_eq(a,b) = ordered(a==b) | (nan_a & nan_b)
///                 [nan_a & nan_b = ~(nonnan_a | nonnan_b), De Morgan]
///   pgf_ne      = ~pgf_eq
///   pgf_lt(a,b) = ordered(b>a) | (nonnan_a & ~nonnan_b)   [!nan_a & nan_b]
///   pgf_le(a,b) = ordered(b>=a) | ~nonnan_b               [nan_b]
///   pgf_gt(a,b) = ordered(a>b) | (nonnan_b & ~nonnan_a)   [nan_a & !nan_b]
///   pgf_ge(a,b) = ordered(a>=b) | ~nonnan_a               [nan_a]
/// Each ordered compare is false on any NaN lane, so the union with the
/// NaN arm is disjoint-exact (utils/float.h truth table, both operands
/// unrestricted). Scratch: v16, v17; clobbers the b-lane registers.
fn emit_fcmpvar_quad_2d(e: &mut Emitter, rel: FRel) {
    for i in 0..4u32 {
        let (a, b) = (i, 4 + i);
        match rel {
            FRel::Eq | FRel::Ne => {
                e.fcmeq_2d(16, a, a); // nonnan_a
                e.fcmeq_2d(17, b, b); // nonnan_b
                e.orr_16b(16, 16, 17);
                e.not_16b(16, 16); // nan_a & nan_b
                e.fcmeq_2d(a, a, b); // ordered a == b
                e.orr_16b(a, a, 16);
                if rel == FRel::Ne {
                    e.not_16b(a, a);
                }
            }
            FRel::Lt => {
                e.fcmeq_2d(16, a, a); // nonnan_a
                e.fcmeq_2d(17, b, b); // nonnan_b
                e.bic_16b(16, 16, 17); // !nan_a & nan_b
                e.fcmgt_2d(a, b, a); // ordered a < b
                e.orr_16b(a, a, 16);
            }
            FRel::Le => {
                e.fcmeq_2d(17, b, b);
                e.not_16b(17, 17); // nan_b
                e.fcmge_2d(a, b, a); // ordered a <= b
                e.orr_16b(a, a, 17);
            }
            FRel::Gt => {
                e.fcmeq_2d(16, b, b); // nonnan_b
                e.fcmeq_2d(17, a, a); // nonnan_a
                e.bic_16b(16, 16, 17); // nan_a & !nan_b
                e.fcmgt_2d(a, a, b); // ordered a > b
                e.orr_16b(a, a, 16);
            }
            FRel::Ge => {
                e.fcmeq_2d(17, a, a);
                e.not_16b(17, 17); // nan_a
                e.fcmge_2d(a, a, b); // ordered a >= b
                e.orr_16b(a, a, 17);
            }
        }
    }
}

/// Narrows the four 2x64 compare masks in v0-v3 to 8 byte-masks in v0.8b
/// (lane order = row order; UZP1 keeps the low halves, which for
/// all-ones/zeros masks are the masks themselves). `neg` complements the
/// narrowed mask (the int Ne arm compares with CMEQ; the float Ne arm
/// already complemented at 2d width).
fn emit_narrow_masks_8b(e: &mut Emitter, neg: bool) {
    e.uzp1_4s(0, 0, 1);
    e.uzp1_4s(1, 2, 3);
    e.uzp1_8h(0, 0, 1);
    e.xtn_8b(0, 0);
    if neg {
        e.not_8b(0, 0);
    }
}

/// Loads 8 Datums at rows [x10, x10+8) of `col` into v(base)..v(base+3).
fn emit_load_group(e: &mut Emitter, ctx: &Ctx<'_>, col: u16, vbase: u32) {
    let p0 = lane_p0_reg(e, ctx, col, 15);
    e.add_x_lsl3(16, p0, 10);
    e.ldp_q(vbase, vbase + 1, 16, 0);
    e.ldp_q(vbase + 2, vbase + 3, 16, 32);
}

/// Non-null byte mask of `col` rows [x10, x10+8) into v(dst).
fn emit_notnull_8b(e: &mut Emitter, ctx: &Ctx<'_>, col: u16, dst: u32) {
    let nul = lane_isnull_reg(e, ctx, col, 15);
    e.ldr_d_idx(dst, nul, 10);
    e.cmeq0_8b(dst, dst);
}

/// The 64-row block section. On entry ROW = 0; on exit ROW = nrows & !63.
fn emit_simd_blocks(e: &mut Emitter, ctx: &Ctx<'_>, prog: &Program, plan: &Plan, tier: SimdTier) {
    let match_of = assign_match_clauses(prog, plan, tier);
    let block_loop = e.new_label();
    let simd_done = e.new_label();
    e.bind(block_loop);
    e.sub_x(12, NROWS, ROW);
    e.cmp_x_imm(12, 64);
    e.b_cond(Cond::Lt, simd_done);

    // Block-invariant vectors, re-materialized per block.
    e.ldr_lit(13, MOVEMASK_WEIGHTS);
    e.fmov_d_x(30, 13);
    let mut kreg = 24u32;
    let mut kreg_of = Vec::with_capacity(plan.clauses.len());
    for c in &plan.clauses {
        match c {
            ClauseShape::CmpConst { konst, .. } => {
                e.ldr_lit(12, konst.as_usize() as u64);
                e.dup_2d_x(kreg, 12);
                kreg_of.push(Some(kreg));
                kreg += 1;
            }
            ClauseShape::FCmpConst { konst_bits, .. } => {
                e.ldr_lit(12, *konst_bits);
                e.dup_2d_x(kreg, 12);
                kreg_of.push(Some(kreg));
                kreg += 1;
            }
            _ => kreg_of.push(None),
        }
    }
    debug_assert!(kreg <= 29);

    // SVE2 MATCH candidate vectors, block-resident like the const bank.
    // Each register carries 8 u16 candidates in MATCH segment 0 (two packed
    // u64s ZIP1'd; NEON writes zero the upper z bits, so higher segments —
    // never governed anyway — hold zeros at any VL). Partial chunks pad by
    // repeating the first candidate (idempotent under OR-of-equalities).
    let mut match_regs_of: Vec<Vec<u32>> = Vec::with_capacity(plan.clauses.len());
    {
        let mut free = match_free_regs(plan).into_iter();
        let mut any_match = false;
        for m in &match_of {
            let mut regs = Vec::new();
            if let Some(elems) = m {
                let pack4 = |c: &[u16], pad: u16| -> u64 {
                    let mut w = 0u64;
                    for i in 0..4 {
                        w |= (*c.get(i).unwrap_or(&pad) as u64) << (16 * i);
                    }
                    w
                };
                for chunk in elems.chunks(8) {
                    let reg = free.next().expect("assign_match_clauses bounded the budget");
                    let lo = pack4(chunk, elems[0]);
                    let hi = pack4(if chunk.len() > 4 { &chunk[4..] } else { &[] }, elems[0]);
                    e.ldr_lit(12, lo);
                    e.dup_2d_x(reg, 12);
                    e.ldr_lit(12, hi);
                    e.dup_2d_x(16, 12);
                    e.zip1_2d(reg, reg, 16);
                    regs.push(reg);
                    any_match = true;
                }
            }
            match_regs_of.push(regs);
        }
        if any_match {
            // Governing predicate: exactly the 8-row group at any VL.
            e.ptrue_h_vl8(1);
        }
    }

    // Qual vector pass: 8 groups of 8 rows -> 64-bit pass word in x9.
    e.movz_x(9, 0);
    e.mov_x(10, ROW);
    e.movz_x(14, 0);
    let group_loop = e.new_label();
    e.bind(group_loop);
    let mut first = true;
    for (ix, (c, kr)) in plan.clauses.iter().zip(&kreg_of).enumerate() {
        match *c {
            ClauseShape::CmpConst { col, op, .. } => {
                emit_load_group(e, ctx, col, 0);
                emit_cmp_quad_2d(e, vcmp(op), |_| kr.unwrap());
                emit_narrow_masks_8b(e, vcmp(op) == VCmp::Ne);
                emit_notnull_8b(e, ctx, col, 17);
                e.and_8b(0, 0, 17);
            }
            ClauseShape::FCmpConst { col, rel, lane_f32, .. } => {
                emit_load_group(e, ctx, col, 0);
                if lane_f32 {
                    // Promote 8 raw-f32 datums to f64 lanes (exact).
                    for va in 0..4u32 {
                        e.xtn_2s_2d(va, va);
                        e.fcvtl_2d_2s(va, va);
                    }
                }
                emit_fcmp_quad_2d(e, rel, kr.unwrap());
                emit_narrow_masks_8b(e, false);
                emit_notnull_8b(e, ctx, col, 17);
                e.and_8b(0, 0, 17);
            }
            ClauseShape::CmpVar { a_col, b_col, op } => {
                emit_load_group(e, ctx, a_col, 0);
                emit_load_group(e, ctx, b_col, 4);
                emit_cmp_quad_2d(e, vcmp(op), |i| 4 + i);
                emit_narrow_masks_8b(e, vcmp(op) == VCmp::Ne);
                emit_notnull_8b(e, ctx, a_col, 17);
                emit_notnull_8b(e, ctx, b_col, 16);
                e.and_8b(17, 17, 16);
                e.and_8b(0, 0, 17);
            }
            ClauseShape::FCmpVar { a_col, b_col, rel, a_f32, b_f32 } => {
                emit_load_group(e, ctx, a_col, 0);
                emit_load_group(e, ctx, b_col, 4);
                if a_f32 {
                    for va in 0..4u32 {
                        e.xtn_2s_2d(va, va);
                        e.fcvtl_2d_2s(va, va);
                    }
                }
                if b_f32 {
                    for vb in 4..8u32 {
                        e.xtn_2s_2d(vb, vb);
                        e.fcvtl_2d_2s(vb, vb);
                    }
                }
                emit_fcmpvar_quad_2d(e, rel);
                emit_narrow_masks_8b(e, false);
                emit_notnull_8b(e, ctx, a_col, 17);
                emit_notnull_8b(e, ctx, b_col, 16);
                e.and_8b(17, 17, 16);
                e.and_8b(0, 0, 17);
            }
            ClauseShape::NullTestQ { col, kind } => {
                // The clause mask IS the (inverted) notnull byte mask —
                // NullTest never yields NULL and its value is a pure
                // function of the isnull byte.
                emit_notnull_8b(e, ctx, col, 0);
                if kind == NullTestKind::IsNull {
                    e.not_8b(0, 0);
                }
            }
            ClauseShape::BoolTestQ { col, kind } => {
                use BoolTestKind::*;
                emit_load_group(e, ctx, col, 0);
                // Falsy 2d masks (value word == 0); the narrow either keeps
                // them (IsFalse/IsNotTrue want falsy) or complements to
                // truthy (IsTrue/IsNotFalse).
                for va in 0..4u32 {
                    e.cmeq0_2d(va, va);
                }
                emit_narrow_masks_8b(e, matches!(kind, IsTrue | IsNotFalse));
                emit_notnull_8b(e, ctx, col, 17);
                match kind {
                    // is_true = truthy & !null; is_false = falsy & !null.
                    IsTrue | IsFalse => e.and_8b(0, 0, 17),
                    // !is_true = falsy | null; !is_false = truthy | null.
                    IsNotTrue | IsNotFalse => {
                        e.not_8b(17, 17);
                        e.orr_8b(0, 0, 17);
                    }
                }
            }
            ClauseShape::SaopQ { col, op, arr } => {
                if !match_regs_of[ix].is_empty() {
                    // SVE2 MATCH stencil (spike K3): 8 u16 candidates per
                    // MATCH vs the NEON per-candidate CMEQ+ORR below.
                    // Exactness: elements are in [0, 0xFFFF], so a datum
                    // equals an element iff its low 16 bits MATCH and its
                    // upper 48 bits are zero (the in-domain mask) — see
                    // saop_match_elems.
                    emit_load_group(e, ctx, col, 0);
                    // Row low16s, row-ordered, into v16.8h (UZP1 keeps the
                    // even (low) halves at each width).
                    e.uzp1_4s(16, 0, 1);
                    e.uzp1_4s(17, 2, 3);
                    e.uzp1_8h(16, 16, 17);
                    // In-domain byte mask: (datum & !0xFFFF) == 0.
                    e.ldr_lit(12, 0xFFFF);
                    e.dup_2d_x(17, 12);
                    for i in 0..4u32 {
                        e.bic_16b(i, i, 17);
                        e.cmeq0_2d(i, i);
                    }
                    emit_narrow_masks_8b(e, false);
                    // MATCH per candidate register, OR-accumulated in p3.
                    for (j, &reg) in match_regs_of[ix].iter().enumerate() {
                        if j == 0 {
                            e.match_h(3, 1, 16, reg);
                        } else {
                            e.match_h(4, 1, 16, reg);
                            e.orr_p(3, 1, 3, 4);
                        }
                    }
                    // Predicate -> byte mask, AND into the clause mask.
                    e.cpy_z_h_neg1(17, 3);
                    e.xtn_8b(17, 17);
                    e.and_8b(0, 0, 17);
                    emit_notnull_8b(e, ctx, col, 17);
                    e.and_8b(0, 0, 17);
                } else {
                    emit_saop_neon_group(e, ctx, prog, col, op, arr);
                }
            }
            ClauseShape::Generic { .. } => continue,
        }
        if first {
            e.orr_8b(29, 0, 0);
            first = false;
        } else {
            e.and_8b(29, 29, 0);
        }
    }
    e.and_8b(29, 29, 30);
    e.addv_b_8b(29, 29);
    e.umov_w_b0(12, 29);
    e.lslv_x(12, 12, 14);
    e.orr_x(9, 9, 12);
    e.add_x_imm(10, 10, 8);
    e.add_x_imm(14, 14, 8);
    e.cmp_x_imm(14, 64);
    e.b_cond(Cond::Lt, group_loop);
    e.str_x(9, 31, SPILL_MASK);
    e.str_x(ROW, 31, SPILL_BASE);

    // Non-vector clauses per surviving row, ascending. Running them after
    // every vector-pass clause is exact: all clauses are pure and
    // non-erroring (classify_simd refused arith), so the implicit AND
    // commutes — a row failing a pure clause never reaches an erroring
    // clause in either order because no erroring clause exists here at all.
    let generics: Vec<&ClauseShape> =
        plan.clauses.iter().filter(|c| !vector_pass_clause(c)).collect();
    if !generics.is_empty() {
        if let SimdTier::Sve2 { force } = tier {
            // Adaptive survivor-emit tier (spike K1): when this block keeps
            // at least SVE_SURVIVOR_CROSSOVER of its 64 rows, extract the
            // survivor indices densely via COMPACT/CNTP and iterate the
            // list; below the crossover the NEON rbit/clz bit-iteration
            // stays cheaper. `force` drops the threshold to 0 (parity
            // soak coverage).
            let lneon = e.new_label();
            let lmerge = e.new_label();
            e.fmov_d_x(0, 9);
            e.cnt_8b(0, 0);
            e.addv_b_8b(0, 0);
            e.umov_w_b0(12, 0);
            e.cmp_w_imm(12, if force { 0 } else { SVE_SURVIVOR_CROSSOVER });
            e.b_cond(Cond::Lo, lneon);
            emit_sve_extract(e);
            for (gi, c) in generics.iter().enumerate() {
                emit_dense_clause(e, ctx, prog, c, gi > 0);
            }
            e.b(lmerge);
            e.bind(lneon);
            for c in &generics {
                emit_bits_clause(e, ctx, prog, c);
            }
            e.bind(lmerge);
        } else {
            for c in &generics {
                emit_bits_clause(e, ctx, prog, c);
            }
        }
    }
    e.ldr_x(ROW, 31, SPILL_BASE);

    // Full block: input word is all-ones (driver contract), so storing the
    // computed pass word is exactly "failures cleared".
    e.ldr_x(9, 31, SPILL_MASK);
    e.lsr_x_6(10, ROW);
    e.str_x_idx3(9, SEL, 10);

    e.add_x_imm(ROW, ROW, 64);
    e.b(block_loop);
    e.bind(simd_done);
}

/// The NEON per-candidate SaopQ group arm (every relation; the SVE2 MATCH
/// stencil above replaces it only for admitted Eq/u16-domain clauses).
fn emit_saop_neon_group(
    e: &mut Emitter,
    ctx: &Ctx<'_>,
    prog: &Program,
    col: u16,
    op: CmpOp,
    arr: u16,
) {
    let elems = saop_vector_elems(prog, arr);
    emit_load_group(e, ctx, col, 0);
    let v = vcmp(op);
    let ne = v == VCmp::Ne;
    // Accumulators v4-v7. For every relation but Ne the row's
    // match word is OR over elements of cmp(scalar, elem) —
    // OR-accumulate from zeros. Ne needs OR of complements,
    // which is NOT(AND of CMEQ) (De Morgan): AND-accumulate
    // from ones, complement once in the narrow.
    for i in 0..4u32 {
        if ne {
            e.cmeq_2d(4 + i, i, i); // x == x: all-ones
        } else {
            e.eor_16b(4 + i, 4 + i, 4 + i); // zeros
        }
    }
    for elem in &elems {
        e.ldr_lit(12, elem.as_usize() as u64);
        e.dup_2d_x(16, 12);
        for i in 0..4u32 {
            match v {
                VCmp::Eq | VCmp::Ne => e.cmeq_2d(17, i, 16),
                VCmp::Gt => e.cmgt_2d(17, i, 16),
                VCmp::Ge => e.cmge_2d(17, i, 16),
                VCmp::Lt => e.cmgt_2d(17, 16, i),
                VCmp::Le => e.cmge_2d(17, 16, i),
                VCmp::Hi => e.cmhi_2d(17, i, 16),
                VCmp::Hs => e.cmhs_2d(17, i, 16),
                VCmp::Lo => e.cmhi_2d(17, 16, i),
                VCmp::Ls => e.cmhs_2d(17, 16, i),
            }
            if ne {
                e.and_16b(4 + i, 4 + i, 17);
            } else {
                e.orr_16b(4 + i, 4 + i, 17);
            }
        }
    }
    // Empty element set falls out naturally: zeros (or
    // NOT(ones) for Ne) — every row fails, matching the
    // interpreter's false-or-NULL qual failure.
    for i in 0..4u32 {
        e.orr_16b(i, 4 + i, 4 + i);
    }
    emit_narrow_masks_8b(e, ne);
    emit_notnull_8b(e, ctx, col, 17);
    e.and_8b(0, 0, 17);
}

/// SVE COMPACT survivor extraction (spike K1): densifies the block's pass
/// word (x9) into block-relative row indices at [sp + SURV_BUF], count at
/// [sp + SURV_CNT]. Selectivity-invariant; VL-agnostic (ptrue-pattern
/// governance; ST1W overstores at most one vector of slack — see
/// SURV_BUF_BYTES). Clobbers z0/z4/z6/z7, p0/p2-p6, x11-x13; reads v30
/// (the block-invariant movemask weights, whose upper z bits NEON zeroed).
fn emit_sve_extract(e: &mut Emitter) {
    e.ptrue_s_all(0);
    e.ptrue_b_vl8(2);
    // Block-relative row-id vectors for the two COMPACT halves of a group.
    e.index_s_imm(6, 0, 1);
    e.index_s_imm(7, 4, 1);
    e.add_x_imm(11, 31, SURV_BUF); // cursor
    for g in 0..8u32 {
        // Pass-word byte g -> per-row byte mask (bit i of the byte selects
        // weight lane i) -> .b predicate over the 8 rows.
        e.ubfx_x_byte(12, 9, 8 * g);
        e.dup_z_b_w(0, 12);
        e.and_z(0, 0, 30);
        e.cmpne_b_imm0(3, 2, 0);
        // Widen to .s lanes: rows 0..3 in p5, rows 4..7 in p6 (at VL=128;
        // wider VLs shift rows into p5 and leave p6 empty — both halves
        // stay exact because the row-id vectors carry absolute positions).
        e.punpklo(4, 3);
        e.punpklo(5, 4);
        e.punpkhi(6, 4);
        e.compact_s(4, 5, 6);
        e.st1w_s(4, 0, 11);
        e.cntp_s(12, 0, 5);
        e.add_x_lsl2(11, 11, 12);
        e.compact_s(4, 6, 7);
        e.st1w_s(4, 0, 11);
        e.cntp_s(12, 0, 6);
        e.add_x_lsl2(11, 11, 12);
        if g < 7 {
            e.add_z_s_imm(6, 8);
            e.add_z_s_imm(7, 8);
        }
    }
    e.add_x_imm(12, 31, SURV_BUF);
    e.sub_x(13, 11, 12);
    e.lsr_x_imm(13, 13, 2);
    e.str_x(13, 31, SURV_CNT);
}

/// One non-vector clause over the extracted survivor list: the dense-index
/// twin of emit_bits_clause (identical fail path; ascending row order is
/// COMPACT's — the same order the bit iteration walks). Loop state lives in
/// the otherwise-unused callee-saved x22 (cursor) / x24 (count) — clause
/// stencils only clobber x8-x16, so no per-survivor spill traffic. Only
/// clauses AFTER the first re-check the pass-word bit (a prior dense clause
/// may have cleared it; the first clause sees the extraction's exact list).
fn emit_dense_clause(
    e: &mut Emitter,
    ctx: &Ctx<'_>,
    prog: &Program,
    shape: &ClauseShape,
    recheck: bool,
) {
    e.movz_x(22, 0); // cursor
    e.ldr_x(24, 31, SURV_CNT);
    let head = e.new_label();
    let done = e.new_label();
    let fail = e.new_label();
    e.bind(head);
    e.cmp_x_x(22, 24);
    e.b_cond(Cond::Ge, done);
    e.add_x_imm(12, 31, SURV_BUF);
    e.ldr_w_idx2(13, 12, 22); // block-relative survivor index
    e.add_x_imm(22, 22, 1);
    if recheck {
        e.ldr_x(14, 31, SPILL_MASK);
        e.lsrv_x(14, 14, 13);
        e.and_x_1(14, 14);
        e.cbz_x(14, head); // failed by an earlier dense clause
    }
    e.ldr_x(15, 31, SPILL_BASE);
    e.add_x(ROW, 15, 13);
    let cctx = ctx.with_row_labels(fail, head);
    emit_clause(e, &cctx, prog, shape);
    e.b(head);
    e.bind(fail);
    e.ldr_x(13, 31, SPILL_BASE);
    e.sub_x(12, ROW, 13);
    e.movz_x(14, 1);
    e.lslv_x(14, 14, 12);
    e.ldr_x(15, 31, SPILL_MASK);
    e.bic_x(15, 15, 14);
    e.str_x(15, 31, SPILL_MASK);
    e.b(head);
    e.bind(done);
}

/// Bit-iteration prelude: copies the pass word to the cursor slot, then per
/// iteration extracts the lowest set row into ROW. Loop state lives in the
/// spill slots.
fn emit_bits_head(e: &mut Emitter) -> (Label, Label) {
    e.ldr_x(10, 31, SPILL_MASK);
    e.str_x(10, 31, SPILL_BITS);
    let head = e.new_label();
    let done = e.new_label();
    e.bind(head);
    e.ldr_x(10, 31, SPILL_BITS);
    e.cbz_x(10, done);
    e.rbit_x(11, 10);
    e.clz_x(11, 11);
    e.sub_x_imm(12, 10, 1);
    e.and_x(12, 10, 12);
    e.str_x(12, 31, SPILL_BITS);
    e.ldr_x(13, 31, SPILL_BASE);
    e.add_x(ROW, 13, 11);
    (head, done)
}

/// One non-vector clause over the block's surviving rows (its scalar
/// stencil with the fail/next labels re-aimed); a failing row clears its
/// bit in the spilled pass word.
fn emit_bits_clause(e: &mut Emitter, ctx: &Ctx<'_>, prog: &Program, shape: &ClauseShape) {
    let (head, done) = emit_bits_head(e);
    let fail = e.new_label();
    let cctx = ctx.with_row_labels(fail, head);
    emit_clause(e, &cctx, prog, shape);
    e.b(head);
    e.bind(fail);
    e.ldr_x(13, 31, SPILL_BASE);
    e.sub_x(12, ROW, 13);
    e.movz_x(14, 1);
    e.lslv_x(14, 14, 12);
    e.ldr_x(15, 31, SPILL_MASK);
    e.bic_x(15, 15, 14);
    e.str_x(15, 31, SPILL_MASK);
    e.b(head);
    e.bind(done);
}

/// Compile-time introspection for tests/telemetry: whether the plan takes
/// the 64-row NEON block tier (the scalar loop still owns the tail).
pub(crate) fn plan_is_simd(plan: &Plan) -> bool {
    classify_simd(plan)
}

/// Compile-time introspection for tests/telemetry: (the SVE COMPACT
/// survivor path was emitted, number of clauses on the SVE2 MATCH stencil).
/// Both zero on non-SVE2 hardware or with the tier pinned off.
pub(crate) fn plan_sve2_info(prog: &Program, plan: &Plan) -> (bool, usize) {
    if !classify_simd(plan) {
        return (false, 0);
    }
    let tier = simd_tier();
    let survivors = matches!(tier, SimdTier::Sve2 { .. })
        && plan.clauses.iter().any(|c| !vector_pass_clause(c));
    let nmatch = assign_match_clauses(prog, plan, tier).iter().flatten().count();
    (survivors, nmatch)
}

// ---- Projection tier ------------------------------------------------------
//
// A projection program computes output lanes for the SELECTED rows of a
// staged batch (the qual segment's selection bitmap is the input currency):
// straight-line steps ending in StoreOut stores, NO Qual steps. The body is
// one scalar row loop — test the row's sel bit, skip clear rows, run the
// register-file stencils, store the outputs. Arith traps take the same
// refuse-and-replay exit as the qual body: RC_REFUSE with no error
// constructed; the driver replays the batch through the C-ported per-row
// projection, which raises C's exact error on C's row (outputs written
// before the trap are discarded — the consumer contract only covers a batch
// whose body exited RC_OK).

/// One classified projection program: a single generic window over the whole
/// step list (no clause structure to fuse).
pub(crate) struct ProjPlan {
    pub used_cols: Vec<u16>,
}

/// Fail-closed classification for projection programs: exhaustive over Step
/// with no wildcard admission. Register-self-contained by construction (one
/// window = the whole program). Refuses: any Qual step (projection segments
/// carry no filter), float compares (no NaN-exact var-var cond — the fused
/// const shapes are qual-only), missing StoreOut (nothing observable), and
/// every bound violation.
pub(crate) fn plan_project(prog: &Program, ncols: usize, nouts: usize) -> Option<ProjPlan> {
    if prog.volatile || prog.steps.is_empty() || nouts == 0 || nouts > crate::spec::MAX_OUTS {
        return None;
    }
    let ncols = ncols.min(MAX_COLS);
    let mut used_cols: Vec<u16> = Vec::new();
    let mut written: Vec<u8> = Vec::new();
    let mut any_store = false;
    let use_col = |used: &mut Vec<u16>, col: u16| {
        if !used.contains(&col) {
            used.push(col);
        }
    };
    let col_ok = |col: u16| (col as usize) < ncols;
    let reads_ok = |written: &Vec<u8>, r: u8| written.contains(&r);
    for step in &prog.steps {
        match *step {
            Step::LoadLane { col, out } => {
                if reg_bad(out) || !col_ok(col) {
                    return None;
                }
                use_col(&mut used_cols, col);
                written.push(out);
            }
            Step::LoadConst { k, out } => {
                if reg_bad(out) || k as usize >= prog.consts.len() {
                    return None;
                }
                written.push(out);
            }
            Step::Cmp { op, a, b, out } => {
                if reg_bad(a) || reg_bad(b) || reg_bad(out) || is_float_cmp(op) {
                    return None;
                }
                if !reads_ok(&written, a) || !reads_ok(&written, b) {
                    return None;
                }
                written.push(out);
            }
            Step::Arith { op: _, a, b, out } => {
                if reg_bad(a) || reg_bad(b) || reg_bad(out) {
                    return None;
                }
                if !reads_ok(&written, a) || !reads_ok(&written, b) {
                    return None;
                }
                written.push(out);
            }
            Step::NullTest { a, out, .. } | Step::BoolTest { a, out, .. } => {
                if reg_bad(a) || reg_bad(out) || !reads_ok(&written, a) {
                    return None;
                }
                written.push(out);
            }
            Step::SaopAny { a, out, op, arr } => {
                if reg_bad(a) || reg_bad(out) || !reads_ok(&written, a) {
                    return None;
                }
                if is_float_cmp(op) || (arr as usize) >= prog.arrays.len() {
                    return None;
                }
                if prog.arrays[arr as usize].len() > MAX_SAOP_ELEMS {
                    return None;
                }
                written.push(out);
            }
            Step::StoreOut { a, out } => {
                if reg_bad(a) || !reads_ok(&written, a) || out as usize >= nouts {
                    return None;
                }
                any_store = true;
            }
            // Projection segments carry no filter clauses (fail closed).
            Step::Qual { .. } => return None,
            // WS-AA wave-7: RowOp chain steps never stitch into a projection
            // body (fail closed — the row-chain tier hosts on the interp
            // walker; its compiled body deleted at RB-R1).
            Step::NextRow | Step::ProtocolCall { .. } => return None,
        }
    }
    if !any_store || used_cols.is_empty() {
        return None;
    }
    Some(ProjPlan { used_cols })
}

/// Emits the projection body: one scalar row loop over the staged batch,
/// each SELECTED row (its bit set in the caller's sel words) running the
/// generic register-file stencils; clear rows skip in a handful of
/// instructions. Same prologue/epilogue and refuse exit as `emit_pipeline`.
pub(crate) fn emit_project_pipeline(
    prog: &Program,
    plan: &ProjPlan,
    lay: &ParamsLayout,
) -> Vec<u32> {
    let mut e = Emitter::new();

    e.raw(0xA9BA_7BFD); // stp x29, x30, [sp, #-0x60]!
    e.raw(0x9100_03FD); // mov x29, sp
    e.raw(0xA901_53F3); // stp x19, x20, [sp, #0x10]
    e.raw(0xA902_5BF5); // stp x21, x22, [sp, #0x20]
    e.raw(0xA903_63F7); // stp x23, x24, [sp, #0x30]
    e.raw(0xA904_6BF9); // stp x25, x26, [sp, #0x40]
    e.raw(0xA905_73FB); // stp x27, x28, [sp, #0x50]
    e.raw(0xD104_83FF); // sub sp, sp, #288
    e.mov_x(PARAMS, 0);
    e.ldr_x(NROWS, PARAMS, lay.nrows);
    e.ldr_x(SEL, PARAMS, lay.sel);

    let hoist: Vec<(u16, u32, u32)> = plan
        .used_cols
        .iter()
        .take(HOIST_PAIRS.len())
        .zip(HOIST_PAIRS)
        .map(|(&col, (p0, nul))| (col, p0, nul))
        .collect();
    let mut ctx = Ctx { lay, hoist, row_fail: Label(0), row_next: Label(0), refuse: Label(0) };
    for &(col, p0, nul) in &ctx.hoist {
        e.ldr_x(p0, PARAMS, lane_p0(&ctx, col));
        e.ldr_x(nul, PARAMS, lane_isnull(&ctx, col));
    }
    e.mov_x(ROW, 31); // i = 0

    let loop_head = e.new_label();
    ctx.row_next = e.new_label();
    // No Qual steps exist in a classified projection program (plan_project
    // refuses them), so row_fail is unreachable; alias it to row_next.
    ctx.row_fail = ctx.row_next;
    let exit_ok = e.new_label();
    ctx.refuse = e.new_label();
    let row_next = ctx.row_next;
    let refuse = ctx.refuse;
    let ctx = ctx;

    e.bind(loop_head);
    e.cmp_x_x(ROW, NROWS);
    e.b_cond(Cond::Ge, exit_ok);

    // Selection test: skip rows whose bit is clear.
    e.lsr_x_6(8, ROW);
    e.ldr_x_idx3(9, SEL, 8);
    e.and_x_63(10, ROW);
    e.movz_x(11, 1);
    e.lslv_x(11, 11, 10);
    e.and_x(9, 9, 11);
    e.cbz_x(9, row_next);

    for step in &prog.steps {
        emit_step(&mut e, &ctx, prog, step);
    }

    e.bind(row_next);
    e.add_x_imm(ROW, ROW, 1);
    e.b(loop_head);

    e.bind(exit_ok);
    e.movz_x(0, 0); // RC_OK
    let epilogue = e.new_label();
    e.b(epilogue);
    e.bind(refuse);
    e.movn_x(0, 0); // RC_REFUSE = -1
    e.bind(epilogue);
    e.raw(0x9104_83FF); // add sp, sp, #288
    e.raw(0xA941_53F3); // ldp x19, x20, [sp, #0x10]
    e.raw(0xA942_5BF5); // ldp x21, x22, [sp, #0x20]
    e.raw(0xA943_63F7); // ldp x23, x24, [sp, #0x30]
    e.raw(0xA944_6BF9); // ldp x25, x26, [sp, #0x40]
    e.raw(0xA945_73FB); // ldp x27, x28, [sp, #0x50]
    e.raw(0xA8C6_7BFD); // ldp x29, x30, [sp], #0x60
    e.ret();

    e.finish()
}
