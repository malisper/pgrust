//! `tablefunc.c` `normal_rand` — a value-per-call SRF over the Box-Muller
//! polar method, drawing from the house global PRNG seam.

use datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

struct NormalRandFctx {
    mean: f64,
    stddev: f64,
    carry_val: f64,
    use_carry: bool,
}

// get_normal_pair: Algorithm P (polar method), mean 0, stddev 1.
fn get_normal_pair() -> (f64, f64) {
    loop {
        let (u1, u2) = pg_prng::global_prng(|p| (p.next_f64(), p.next_f64()));
        let v1 = 2.0 * u1 - 1.0;
        let v2 = 2.0 * u2 - 1.0;
        let s = v1 * v1 + v2 * v2;
        if s >= 1.0 {
            continue;
        }
        if s == 0.0 {
            return (0.0, 0.0);
        }
        let s = ((-2.0 * s.ln()) / s).sqrt();
        return (v1 * s, v2 * s);
    }
}

#[cold]
#[inline(never)]
fn negative_rows() -> PgError {
    PgError::error("number of rows cannot be negative".to_string())
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

pub(crate) fn fc_normal_rand(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("normal_rand: NULL flinfo");

    if !flinfo.has_fn_extra() {
        let num_tuples = fcinfo.arg_i32(0);
        if num_tuples < 0 {
            return Err(negative_rows().into());
        }
        let fctx = NormalRandFctx {
            mean: fcinfo.arg_f64(1),
            stddev: fcinfo.arg_f64(2),
            carry_val: 0.0,
            use_carry: false,
        };
        let call = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        call.max_calls = num_tuples as u64;
        call.user_fctx = Some(Box::new(fctx));
    }

    let call = funcapi::per_MultiFuncCall(flinfo);
    if call.call_cntr < call.max_calls {
        let fctx = call
            .user_fctx
            .as_mut()
            .expect("normal_rand: user_fctx set at first call")
            .downcast_mut::<NormalRandFctx>()
            .expect("normal_rand: user_fctx is NormalRandFctx");
        let result = if fctx.use_carry {
            fctx.use_carry = false;
            fctx.carry_val
        } else {
            let (n1, n2) = get_normal_pair();
            fctx.carry_val = fctx.mean + fctx.stddev * n2;
            fctx.use_carry = true;
            fctx.mean + fctx.stddev * n1
        };
        Ok(funcapi::srf_return_next(flinfo, fcinfo, Datum::from_f64(result)))
    } else {
        Ok(funcapi::srf_return_done(flinfo, fcinfo))
    }
}
