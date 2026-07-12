// The loop-inside reference interpreter: the semantic SPECIFICATION every
// stitched body is parity-tested against, the fail-open floor at runtime,
// and the refuse-and-replay engine (an arith trap in a stitched body replays
// the batch here so the error fires on C's row with C's message).

use datum::{Datum, NullableDatum};
use types_error::{
    PgError, PgResult, ERRCODE_DIVISION_BY_ZERO, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

use crate::spec::{
    ArithOp, Batch, BoolTestKind, NullTestKind, OutLane, Program, SelVec, Step, MAX_REGS, MAX_ROWS,
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
                    return Ok(false);
                }
            }
            Step::StoreOut { a, out } => {
                let r = regs[a as usize];
                let lane = &mut outs[out as usize];
                lane.values[i as usize] = r.value;
                lane.isnull[i as usize] = r.isnull;
            }
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
