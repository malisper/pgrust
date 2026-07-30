// Native ground-truth for the spots_dsqrt in-model FAILED (grid NaN rows):
// real silicon C (clang sqrt) and shipped Rust must agree bit-exactly.
use datum::{Datum, NullableDatum};

fn call1_local(a: Datum) -> types_error::PgResult<Datum> {
    let mut f = types_fmgr::LocalFcinfo::<1>::new(0);
    f.args[0] = NullableDatum::value(a);
    adt_float::builtins::fc_dsqrt(None, &mut f)
}

#[test]
fn dsqrt_grid_matches_native_c() {
    // (input bits, expected native-C output bits or None for error)
    let cases: [(u64, Option<u64>); 8] = [
        (0x0000000000000000, Some(0x0000000000000000)),
        (0x8000000000000000, Some(0x8000000000000000)),
        (0x7FF0000000000000, Some(0x7FF0000000000000)),
        (0xFFF0000000000000, None),
        (0x7FF8000000000000, Some(0x7FF8000000000000)),
        (0xFFF8000000000001, Some(0xFFF8000000000001)),
        (0x7FF0000000000001, Some(0x7FF8000000000001)),
        (0x0000000000000001, Some(0x1e60000000000000)),
    ];
    for (inb, expect) in cases {
        let r = call1_local(Datum::from_f64(f64::from_bits(inb)));
        match (r, expect) {
            (Ok(d), Some(e)) => assert_eq!(d.as_f64().to_bits(), e, "in={inb:016x}"),
            (Err(_), None) => {}
            (r, e) => panic!("in={inb:016x} rust={r:?} native-C-expect={e:?}"),
        }
    }
}
