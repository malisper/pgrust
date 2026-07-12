// The loop-inside reference interpreter: the semantic SPECIFICATION every
// stitched body is parity-tested against, the fail-open floor at runtime,
// and the refuse-and-replay engine (an arith trap in a stitched body replays
// the batch here so the error fires on C's row with C's message).

use datum::{Datum, NullableDatum};
use types_error::{
    PgError, PgResult, ERRCODE_DIVISION_BY_ZERO, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

use crate::spec::{ArithOp, Batch, Program, SelVec, Step, MAX_REGS, MAX_ROWS};

#[cold]
#[inline(never)]
pub(crate) fn division_by_zero() -> Box<PgError> {
    Box::new(PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO))
}

#[cold]
#[inline(never)]
pub(crate) fn int_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("integer out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

#[inline(always)]
fn arith_eval(op: ArithOp, a: i32, b: i32) -> PgResult<i32> {
    // int.c parity: checked ops, INT_MIN/-1 division overflow included.
    match op {
        ArithOp::Add4 => a.checked_add(b).ok_or_else(int_out_of_range),
        ArithOp::Sub4 => a.checked_sub(b).ok_or_else(int_out_of_range),
        ArithOp::Mul4 => a.checked_mul(b).ok_or_else(int_out_of_range),
        ArithOp::Div4 => {
            if b == 0 {
                return Err(division_by_zero());
            }
            a.checked_div(b).ok_or_else(int_out_of_range)
        }
    }
}

/// The whole program for row `i`, steps in order, first failing Qual step
/// exits the row. Errors propagate with rows 0..i-1 fully consumed.
#[inline(always)]
pub fn eval_row(prog: &Program, batch: &Batch<'_>, i: u32) -> PgResult<bool> {
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
                    NullableDatum {
                        value: Datum::from_i32(arith_eval(op, a.value.as_i32(), b.value.as_i32())?),
                        isnull: false,
                    }
                };
            }
            Step::Qual { a } => {
                let r = regs[a as usize];
                if r.isnull || !r.value.as_bool() {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
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
