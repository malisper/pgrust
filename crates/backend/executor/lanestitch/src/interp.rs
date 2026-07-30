// The loop-inside reference interpreter: the semantic SPECIFICATION every
// stitched body is parity-tested against, the fail-open floor at runtime,
// and the refuse-and-replay engine (an arith trap in a stitched body replays
// the batch here so the error fires on C's row with C's message).

use datum::{Datum, NullableDatum};
use types_error::{
    PgError, PgResult, ERRCODE_DIVISION_BY_ZERO, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

use crate::spec::{
    ArithOp, Batch, BoolTestKind, ChainCursor, ChainOutcome, ChainVerdict, NullTestKind, OutLane,
    Program, RowChainHost, SelVec, Step, MAX_REGS, MAX_ROWS,
};

#[cold]
#[inline(never)]
pub(crate) fn division_by_zero() -> Box<PgError> {
    Box::new(PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO))
}

// Width-specific out-of-range messages, byte-identical to C int.c / int8.c
// (int2 -> "smallint", int4 -> "integer", int8 -> "bigint"); the stitched
// tier never fabricates these — refuse-and-replay routes through here so the
// replay raises C's exact message on C's row.
#[track_caller]
#[cold]
#[inline(never)]
fn out_of_range(width: u8) -> Box<PgError> {
    let msg = match width {
        2 => "smallint out of range",
        8 => "bigint out of range",
        _ => "integer out of range",
    };
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE))
}

// Loud fail-closed error for a RowOp step reaching a non-chain walker (and
// for chain-shape violations): the interpreter is the spec, so an illegal
// program errors rather than silently misbehaving. (The compiled stitched
// chain body that shared these error strings was DELETED at RB-R1/SE18 —
// this walker is now the row-chain vocabulary's only engine.)
#[cold]
#[inline(never)]
pub(crate) fn rowop_misuse(what: &str) -> Box<PgError> {
    Box::new(PgError::error(format!("lanestitch row-chain contract violation: {what}")))
}

/// int.c / int8.c parity: checked add/sub/mul (width-exact overflow) and the
/// div traps (zero divisor + MIN/-1). Reads and writes at the op's width; the
/// Datum image stays canonically sign-extended (from_iN).
#[inline(always)]
fn arith_eval(op: ArithOp, a: Datum, b: Datum) -> PgResult<Datum> {
    use ArithOp::*;
    let w = op.width();
    let oor = || out_of_range(w);
    match op {
        Add2 => (a.as_i16().checked_add(b.as_i16())).map(Datum::from_i16).ok_or_else(oor),
        Sub2 => (a.as_i16().checked_sub(b.as_i16())).map(Datum::from_i16).ok_or_else(oor),
        Mul2 => (a.as_i16().checked_mul(b.as_i16())).map(Datum::from_i16).ok_or_else(oor),
        Div2 => {
            let (x, y) = (a.as_i16(), b.as_i16());
            if y == 0 {
                return Err(division_by_zero());
            }
            x.checked_div(y).map(Datum::from_i16).ok_or_else(oor)
        }
        Add4 => (a.as_i32().checked_add(b.as_i32())).map(Datum::from_i32).ok_or_else(oor),
        Sub4 => (a.as_i32().checked_sub(b.as_i32())).map(Datum::from_i32).ok_or_else(oor),
        Mul4 => (a.as_i32().checked_mul(b.as_i32())).map(Datum::from_i32).ok_or_else(oor),
        Div4 => {
            let (x, y) = (a.as_i32(), b.as_i32());
            if y == 0 {
                return Err(division_by_zero());
            }
            x.checked_div(y).map(Datum::from_i32).ok_or_else(oor)
        }
        Add8 => (a.as_i64().checked_add(b.as_i64())).map(Datum::from_i64).ok_or_else(oor),
        Sub8 => (a.as_i64().checked_sub(b.as_i64())).map(Datum::from_i64).ok_or_else(oor),
        Mul8 => (a.as_i64().checked_mul(b.as_i64())).map(Datum::from_i64).ok_or_else(oor),
        Div8 => {
            let (x, y) = (a.as_i64(), b.as_i64());
            if y == 0 {
                return Err(division_by_zero());
            }
            x.checked_div(y).map(Datum::from_i64).ok_or_else(oor)
        }
    }
}

/// Flow verdict of one PURE step (the shared per-row walk of the qual /
/// projection / row-chain tiers).
#[derive(Clone, Copy, PartialEq, Eq)]
enum PureFlow {
    Cont,
    /// A `Qual` step read NULL/false: the row fails (qual programs) / is
    /// skipped back to the loop top (row-chain programs).
    QualFail,
}

/// One PURE step at row `i` over the shared register file — the single copy
/// of the per-row step semantics all three walkers share. RowOp steps
/// (`NextRow`/`ProtocolCall`) are NOT pure and error loudly here — the
/// row-chain walker (`eval_row_chain`) owns them and never routes them
/// through this helper.
#[inline(always)]
fn eval_pure_step(
    prog: &Program,
    batch: &Batch<'_>,
    i: u32,
    step: &Step,
    regs: &mut [NullableDatum; MAX_REGS],
    outs: &mut [OutLane<'_>],
) -> PgResult<PureFlow> {
    match *step {
        Step::LoadLane { col, out } => {
            let lane = &batch.lanes[col as usize];
            regs[out as usize] = NullableDatum {
                value: lane.values[i as usize],
                isnull: lane.isnull[i as usize],
            };
        }
        Step::LoadConst { k, out } => {
            regs[out as usize] = prog.consts[k as usize];
        }
        Step::Cmp { op, a, b, out } => {
            let (a, b) = (regs[a as usize], regs[b as usize]);
            regs[out as usize] = if a.isnull || b.isnull {
                NullableDatum::null()
            } else {
                NullableDatum {
                    value: Datum::from_bool(op.eval(a.value, b.value)),
                    isnull: false,
                }
            };
        }
        Step::Arith { op, a, b, out } => {
            let (a, b) = (regs[a as usize], regs[b as usize]);
            regs[out as usize] = if a.isnull || b.isnull {
                NullableDatum::null()
            } else {
                NullableDatum { value: arith_eval(op, a.value, b.value)?, isnull: false }
            };
        }
        Step::NullTest { a, out, kind } => {
            let r = regs[a as usize];
            let v = match kind {
                NullTestKind::IsNull => r.isnull,
                NullTestKind::IsNotNull => !r.isnull,
            };
            regs[out as usize] = NullableDatum { value: Datum::from_bool(v), isnull: false };
        }
        Step::BoolTest { a, out, kind } => {
            let r = regs[a as usize];
            // Truthy = non-NULL bool datum reading true (DatumGetBool).
            let is_true = !r.isnull && r.value.as_bool();
            let is_false = !r.isnull && !r.value.as_bool();
            let v = match kind {
                BoolTestKind::IsTrue => is_true,
                BoolTestKind::IsNotTrue => !is_true,
                BoolTestKind::IsFalse => is_false,
                BoolTestKind::IsNotFalse => !is_false,
            };
            regs[out as usize] = NullableDatum { value: Datum::from_bool(v), isnull: false };
        }
        Step::SaopAny { a, out, op, arr } => {
            // Strict-OR ScalarArrayOpExpr three-valued result: scan for a
            // non-NULL matching element (short-circuits true); a NULL
            // scalar or a NULL element with no match yields NULL.
            let scalar = regs[a as usize];
            let elems = &prog.arrays[arr as usize];
            let mut res = false;
            let mut resnull = false;
            for e in elems {
                if scalar.isnull || e.isnull {
                    resnull = true;
                    continue;
                }
                if op.eval(scalar.value, e.value) {
                    res = true;
                    break;
                }
            }
            regs[out as usize] = if res {
                NullableDatum { value: Datum::from_bool(true), isnull: false }
            } else if resnull {
                NullableDatum::null()
            } else {
                NullableDatum { value: Datum::from_bool(false), isnull: false }
            };
        }
        Step::Qual { a } => {
            let r = regs[a as usize];
            if r.isnull || !r.value.as_bool() {
                return Ok(PureFlow::QualFail);
            }
        }
        Step::StoreOut { a, out } => {
            let r = regs[a as usize];
            let lane = &mut outs[out as usize];
            lane.values[i as usize] = r.value;
            lane.isnull[i as usize] = r.isnull;
        }
        // WS-AA wave-7: RowOp steps never reach the pure walkers (a qual or
        // projection program carrying one refuses at classification; this is
        // the spec-level fail-closed backstop, loud in every profile so the
        // pin tests can assert it).
        Step::NextRow | Step::ProtocolCall { .. } => {
            return Err(rowop_misuse("RowOp step in a qual/projection program"));
        }
    }
    Ok(PureFlow::Cont)
}

/// The whole program for row `i`, steps in order, first failing Qual step
/// exits the row. Errors propagate with rows 0..i-1 fully consumed.
#[inline(always)]
pub fn eval_row(prog: &Program, batch: &Batch<'_>, i: u32) -> PgResult<bool> {
    eval_row_outs(prog, batch, i, &mut [])
}

/// [`eval_row`] with projection output lanes: `StoreOut` steps write
/// `outs[out]` at row `i`. Qual programs pass `&mut []` (a StoreOut in a
/// qual program is a caller bug — debug-asserted).
#[inline(always)]
pub fn eval_row_outs(
    prog: &Program,
    batch: &Batch<'_>,
    i: u32,
    outs: &mut [OutLane<'_>],
) -> PgResult<bool> {
    let mut regs = [NullableDatum::null(); MAX_REGS];
    for step in &prog.steps {
        if eval_pure_step(prog, batch, i, step, &mut regs, outs)? == PureFlow::QualFail {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The reference projection tier over one staged batch: for every SELECTED
/// row (ascending), run the whole program, `StoreOut` steps writing the
/// output lanes. Non-selected rows are untouched (their output cells hold
/// garbage — the consumer contract only covers selected rows). On error,
/// outputs for rows before the erroring row are final; the caller discards
/// them (refuse-and-replay routes the batch through the C-ported per-row
/// path, which re-raises the error on C's row with C's message).
pub fn eval_project(
    prog: &Program,
    batch: &Batch<'_>,
    sel: &SelVec,
    outs: &mut [OutLane<'_>],
) -> PgResult<()> {
    debug_assert!(batch.nrows as usize <= MAX_ROWS);
    for i in 0..batch.nrows {
        if sel.contains(i) {
            let ok = eval_row_outs(prog, batch, i, outs)?;
            debug_assert!(ok, "projection programs carry no Qual steps");
        }
    }
    Ok(())
}

/// The reference tier over one staged batch: ascending rows, failing rows'
/// sel bits cleared. On error, bits for rows before the erroring row are
/// final (interpreter parity contract for the stitched tier).
pub fn eval_qual(prog: &Program, batch: &Batch<'_>, sel: &mut SelVec) -> PgResult<()> {
    debug_assert!(batch.nrows as usize <= MAX_ROWS);
    for i in 0..batch.nrows {
        if sel.contains(i) && !eval_row(prog, batch, i)? {
            sel.clear(i);
        }
    }
    Ok(())
}

// ===== WS-AA wave-7 RowOp chain walker (fusion inc-0) =======================

/// Chain-shape validation shared by the twin and the stitched planner:
/// exactly one `NextRow`, and every step BEFORE it (the loop-top segment,
/// the loop_top_owed LAW's structural placement) is a `ProtocolCall` —
/// pure steps have no staged row to read at the loop top. Returns the
/// `NextRow` position, or None for an ill-formed chain (this walker
/// errors loudly).
/// The loop-top law's violation text (formerly shared with the stitched
/// chain body, deleted at RB-R1/SE18).
pub(crate) const LOOP_TOP_MISUSE: &str =
    "loop-top protocol call answered a row verdict with no current row";

/// The D2 cursor/lane bound's violation text (wave-9 WS-AG rung 1, the
/// wave-7 D2 charter rider): a pure step needs a staged lane row at the
/// cursor index, so a chain whose host pulls past the staged batch (or
/// whose batch exceeds the MAX_ROWS lane contract) errors loudly instead
/// of indexing out of bounds. ONE string, shared with the stitched tier
/// when pure steps join production chains (rung 4) — error identity by
/// construction, the LOOP_TOP_MISUSE precedent.
pub(crate) const CURSOR_PAST_BATCH: &str =
    "chain cursor past the staged batch (pure step with no staged lane row)";

pub(crate) fn chain_next_pos(prog: &Program) -> Option<usize> {
    let mut pos = None;
    for (i, s) in prog.steps.iter().enumerate() {
        if let Step::NextRow = s {
            if pos.is_some() {
                return None;
            }
            pos = Some(i);
        }
    }
    let pos = pos?;
    if prog.steps[..pos].iter().all(|s| matches!(s, Step::ProtocolCall { .. })) {
        Some(pos)
    } else {
        None
    }
}

/// The loop-inside row-chain reference walker — the semantic SPECIFICATION
/// of the RowOp vocabulary (fusion inc-0; docs/design/rowmode-endgame.md
/// §2.2), exactly as `eval_qual` specifies the qual tier. One call = one
/// chain drive at the capacity-one boundary:
///
/// * Steps before the (single) `NextRow` are the loop-top segment and run
///   before EVERY source pull — including the final, exhausted one (the
///   mt_step `P -> pull(None)` ordering). Loop-top protocol calls must
///   answer `Continue`.
/// * `NextRow` stages row `cursor.row` (then increments the cursor); host
///   exhaustion exits with [`ChainOutcome::Done`].
/// * Per-row steps run in program order: pure steps over the staged batch
///   lanes at the staged row's index with per-row registers (the `eval_row`
///   walk verbatim — one shared implementation); a failing `Qual` skips the
///   row back to the loop top; `ProtocolCall` verdicts: `Continue` runs the
///   next step, `SkipRow` returns to the loop top, `EmitPause` exits with
///   [`ChainOutcome::Paused`] (re-entry restarts at the loop top).
/// * Errors (pure or protocol) propagate immediately with prior rows fully
///   consumed — protocol targets are the node's own helpers, so their
///   PgError unwind is byte-identical by construction (never replayed).
pub fn eval_row_chain(
    prog: &Program,
    batch: &Batch<'_>,
    cursor: &mut ChainCursor,
    host: &mut dyn RowChainHost,
    outs: &mut [OutLane<'_>],
) -> PgResult<ChainOutcome> {
    let Some(next_pos) = chain_next_pos(prog) else {
        return Err(rowop_misuse(
            "chain program needs exactly one NextRow with a protocol-only loop top",
        ));
    };
    // D2 precondition (wave-9 WS-AG rung 1): pure steps index the staged
    // batch lanes at the cursor row, so their rows must stay inside the
    // batch (and the batch inside the MAX_ROWS lane contract). Computed
    // once per drive; protocol-only chains stage no lane rows and stay
    // unbounded — the DML chain's cursor counts pulls, not lane rows.
    let has_pure = prog.steps[next_pos + 1..]
        .iter()
        .any(|s| !matches!(s, Step::ProtocolCall { .. }));
    'chain: loop {
        // Loop-top segment (validated ProtocolCall-only).
        for step in &prog.steps[..next_pos] {
            let Step::ProtocolCall { call } = *step else { unreachable!("validated above") };
            match host.protocol_call(call)? {
                ChainVerdict::Continue => {}
                ChainVerdict::SkipRow | ChainVerdict::EmitPause => {
                    return Err(rowop_misuse(LOOP_TOP_MISUSE));
                }
            }
        }
        // The pull.
        if !host.next_row()? {
            return Ok(ChainOutcome::Done);
        }
        let row = cursor.row;
        cursor.row += 1;
        // The D2 cursor/lane bound (loud, never an OOB index): only chains
        // with pure steps read lanes, and they may not outrun the batch.
        if has_pure && (row >= batch.nrows || batch.nrows as usize > MAX_ROWS) {
            return Err(rowop_misuse(CURSOR_PAST_BATCH));
        }
        // Per-row segment.
        let mut regs = [NullableDatum::null(); MAX_REGS];
        for step in &prog.steps[next_pos + 1..] {
            match *step {
                Step::NextRow => unreachable!("chain_next_pos admits exactly one NextRow"),
                Step::ProtocolCall { call } => match host.protocol_call(call)? {
                    ChainVerdict::Continue => {}
                    ChainVerdict::SkipRow => continue 'chain,
                    ChainVerdict::EmitPause => return Ok(ChainOutcome::Paused),
                },
                ref pure => {
                    if eval_pure_step(prog, batch, row, pure, &mut regs, outs)?
                        == PureFlow::QualFail
                    {
                        continue 'chain;
                    }
                }
            }
        }
    }
}
