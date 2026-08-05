use datum::{Datum, NullableDatum};
use types_fmgr::LocalFcinfo;

fn call1(
    fc: fn(
        Option<&mut types_fmgr::FmgrInfo>,
        &mut types_fmgr::FunctionCallInfoBaseData,
    ) -> types_error::PgResult<Datum>,
    a: Datum,
) -> types_error::PgResult<Datum> {
    let mut f = LocalFcinfo::<1>::new(0);
    f.args[0] = NullableDatum::value(a);
    fc(None, &mut f)
}

/// Native canonical-NaN replay for the rounding wave (NAN-shim screening
/// companion, prove-target MANDATORY NAN rule): none of the vendored C
/// sections reach the NAN macro / get_float8_nan — NaN only PROPAGATES.
/// Pin that real silicon propagates the canonical quiet NaN
/// 0x7ff8000000000000 through the shipped wrappers, so the vendored C
/// (whose rint/ceil/floor propagate the same input bits) needs no shim
/// and any CBMC NaN-payload counterexample here would be a tool artifact.
#[test]
fn rounding_nan_propagation_canonical() {
    let nan = f64::from_bits(0x7ff8000000000000);
    for fc in [
        adt_float::builtins::fc_dround,
        adt_float::builtins::fc_dceil,
        adt_float::builtins::fc_dfloor,
        adt_float::builtins::fc_dtrunc,
    ] {
        let r = call1(fc, Datum::from_f64(nan)).expect("infallible");
        assert_eq!(
            r.as_f64().to_bits(),
            0x7ff8000000000000,
            "must match native C canonical NaN"
        );
    }
    // dsign(NaN): the else branch — exactly 0.0 (C comment claim).
    let r = call1(adt_float::builtins::fc_dsign, Datum::from_f64(nan)).expect("infallible");
    assert_eq!(r.as_f64().to_bits(), 0.0f64.to_bits());
    // dsqrt(NaN): NaN < 0 is false, sqrt(NaN) propagates canonically.
    let r = call1(adt_float::builtins::fc_dsqrt, Datum::from_f64(nan)).expect("NaN is not < 0");
    assert_eq!(r.as_f64().to_bits(), 0x7ff8000000000000);
}

#[test]
fn dsqrt_negative_arg_semantics() {
    match call1(adt_float::builtins::fc_dsqrt, Datum::from_f64(-1.0)) {
        Ok(d) => panic!("expected Err, got Ok({})", d.as_f64()),
        Err(e) => {
            assert_eq!(e.level, types_error::ERROR);
            assert_eq!(
                e.sqlstate,
                types_error::ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION
            );
        }
    }
}

#[test]
fn in_range_nan_offset_semantics() {
    let mut f = LocalFcinfo::<5>::new(0);
    f.args[0] = NullableDatum::value(Datum::from_f64(1.0));
    f.args[1] = NullableDatum::value(Datum::from_f64(1.0));
    f.args[2] = NullableDatum::value(Datum::from_f64(f64::NAN));
    f.args[3] = NullableDatum::value(Datum::from_bool(false));
    f.args[4] = NullableDatum::value(Datum::from_bool(false));
    match adt_float::builtins::fc_in_range_float8_float8(None, &mut f) {
        Ok(d) => panic!("expected Err, got Ok({})", d.as_bool()),
        Err(e) => {
            assert_eq!(e.level, types_error::ERROR);
            assert_eq!(
                e.sqlstate,
                types_error::ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE
            );
        }
    }
}

/// dtrunc truncates toward zero (control_dtrunc_vs_c_round's ground
/// truth: rint(1.5) = 2.0 differs).
#[test]
fn dtrunc_toward_zero_semantics() {
    for (arg, want) in [(1.5f64, 1.0f64), (-1.5, -1.0), (0.5, 0.0), (-0.5, -0.0)] {
        let r = call1(adt_float::builtins::fc_dtrunc, Datum::from_f64(arg)).expect("infallible");
        assert_eq!(r.as_f64().to_bits(), want.to_bits(), "dtrunc({arg})");
    }
}

#[test]
fn f84div_zero_divide_semantics() {
    let mut f = LocalFcinfo::<2>::new(0);
    f.args[0] = NullableDatum::value(Datum::from_f64(2.0));
    f.args[1] = NullableDatum::value(Datum::from_f32(-0.0));
    match adt_float::builtins::fc_float84div(None, &mut f) {
        Ok(d) => panic!("expected Err, got Ok({})", d.as_f64()),
        Err(e) => {
            println!("level={:?} sqlstate={:?}", e.level, e.sqlstate);
            assert_eq!(e.level, types_error::ERROR);
            assert_eq!(e.sqlstate, types_error::ERRCODE_DIVISION_BY_ZERO);
        }
    }
}
