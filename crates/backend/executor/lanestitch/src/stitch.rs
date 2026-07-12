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
// non-erroring, the body runs 64-row blocks — NEON compare stencils build a
// 64-bit pass word (8 Datums per iteration, 2x64 per q-register), non-SIMD
// clauses run per-row over the word's surviving bits in ascending order.
// Legality is the vectorized interpreter's argument: every clause pure and
// non-erroring makes cross-row/cross-clause reordering unobservable
// (qual = AND). Any erroring (Arith) clause refuses the SIMD shape and the
// program falls back to the scalar row-loop body, which keeps exact
// per-row refuse-and-replay order.

use datum::Datum;

use crate::emit::{Cond, Emitter, Label};
use crate::spec::{is_float_cmp, ArithOp, CmpOp, Program, Step, MAX_COLS, MAX_REGS, MAX_ROWS};

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
                    && !is_float_cmp(*op)
                    && regs_dead_after(steps, hi, &[*r0, *r1, *out]) =>
            {
                use_col(&mut used_cols, *ca);
                use_col(&mut used_cols, *cb);
                ClauseShape::CmpVar { a_col: *ca, b_col: *cb, op: *op }
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
const _: () = assert!(REGFILE_BYTES == 256, "prologue sub sp is hardcoded to 288 = 256 + 32 spill");
const _: () = assert!(MAX_ROWS % 64 == 0);

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

    // Prologue: fp/lr chain kept (unwind guidance), x19-x28 saved, 256-byte
    // virtual register file + 32 spill bytes below.
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
    if classify_simd(plan) {
        emit_simd_blocks(&mut e, &ctx, prog, plan);
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

/// SIMD legality for a whole plan: every clause pure and non-erroring
/// (no Arith anywhere), at least one NEON-shaped clause, and at most 5
/// distinct compare constants (v24-v28).
fn classify_simd(plan: &Plan) -> bool {
    if !simd_enabled() || plan.has_arith {
        return false;
    }
    let mut nconst = 0u32;
    let mut any_simd = false;
    for c in &plan.clauses {
        match c {
            ClauseShape::CmpConst { .. } | ClauseShape::FCmpConst { .. } => {
                nconst += 1;
                any_simd = true;
            }
            ClauseShape::CmpVar { .. } => any_simd = true,
            ClauseShape::Generic { .. } => {}
        }
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
fn emit_simd_blocks(e: &mut Emitter, ctx: &Ctx<'_>, prog: &Program, plan: &Plan) {
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

    // Qual vector pass: 8 groups of 8 rows -> 64-bit pass word in x9.
    e.movz_x(9, 0);
    e.mov_x(10, ROW);
    e.movz_x(14, 0);
    let group_loop = e.new_label();
    e.bind(group_loop);
    let mut first = true;
    for (c, kr) in plan.clauses.iter().zip(&kreg_of) {
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

    // Non-SIMD clauses per surviving row, ascending. Running them after
    // every SIMD clause is exact: all clauses are pure and non-erroring
    // (classify_simd refused arith), so the implicit AND commutes.
    for c in &plan.clauses {
        if let ClauseShape::Generic { lo, hi } = *c {
            emit_bits_clause(e, ctx, prog, &prog.steps[lo..hi]);
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

/// One non-SIMD clause over the block's surviving rows; a failing Qual
/// clears the row's bit in the spilled pass word.
fn emit_bits_clause(e: &mut Emitter, ctx: &Ctx<'_>, prog: &Program, steps: &[Step]) {
    let (head, done) = emit_bits_head(e);
    let fail = e.new_label();
    let cctx = ctx.with_row_labels(fail, head);
    for step in steps {
        emit_step(e, &cctx, prog, step);
    }
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
