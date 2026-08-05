//! STANDING REGRESSION WITNESS — corruption plane (array header ndim outside
//! 0..=MAXDIM). Not a divergence record any more: the divergence it used to
//! witness was FIXED, and this file's job is to keep it fixed.
//!
//! What it used to say: for a malformed flat array image whose ndim field is
//! outside 0..=MAXDIM, C's header-read builtins return SQL NULL from their
//! `AARR_NDIM(v) <= 0 || AARR_NDIM(v) > MAXDIM` sanity check, while shipped
//! read_dims_lbounds looped `0..ndim as usize` BEFORE that check and panicked —
//! dims[6] on a [i32; 6] for ndim > MAXDIM, a ~2^64 range for ndim < 0. It
//! asserted `catch_unwind(...).is_err()`.
//!
//! What happened: the panic was FIXED, not ratified (arrayfuncs: sanity-check
//! ndim BEFORE the dims read). read_dims_lbounds now returns the RAW ndim with
//! zeroed dims/lbounds when `ndim as u32 > MAXDIM`, each wrapper's own sanity
//! check then produces C's verdict, and arrayutils::array_get_n_items_safe
//! rejects `ndim > dims.len()` with C's own message and sqlstate.
//!
//! So this witness now asserts the CORRECT behavior, dual-executed against the
//! same vendored REL_18 C the Kani harnesses use (c/pg_arrayhdr.c). The Kani
//! fence that cited this divergence is gone too — see the *_ndim_corrupt
//! harnesses in src/lib.rs, which prove the same plane symbolically. This file
//! stays because it is cheap, it runs on every `cargo test`, and it is the
//! artifact that fails first if anyone reintroduces a pre-check dims read.
//!
//! C's behavior on this plane, established by EXECUTING the vendored bodies
//! (never re-derived from memory):
//!
//!   ndim               ndims / lower / upper / length / dims   cardinality
//!   -1, i32::MIN       NULL                                    VALUE 0
//!   0                  NULL                                    VALUE 0
//!   1..=MAXDIM         values                                  dims product
//!   7, 1000, i32::MAX  NULL                                    no answer (*)
//!
//! (*) array_cardinality has NO sanity check: it calls ArrayGetNItems with the
//!     raw ndim and a bare `const int *dims`, so above MAXDIM C reads dim words
//!     past the dims area and past the datum. Byte-dependent garbage, not a
//!     specification — pgrust raises a DEFINED error there instead (C's own
//!     "number of array dimensions (%d) exceeds the maximum allowed (%d)",
//!     sqlstate 54000). That choice is deliberate; this file asserts it, and
//!     deliberately does NOT call C on that cell (it would be an out-of-bounds
//!     read in this process).

use proof_arrayfuncs_hdr::{mk_corrupt_image, MAXDIM};
use std::os::raw::c_int;

extern "C" {
    fn pg_array_ndims(v: *const u8, isnull: *mut c_int) -> i32;
    fn pg_array_lower(v: *const u8, reqdim: i32, isnull: *mut c_int) -> i32;
    fn pg_array_upper(v: *const u8, reqdim: i32, isnull: *mut c_int) -> i32;
    fn pg_array_length(v: *const u8, reqdim: i32, isnull: *mut c_int) -> i32;
    fn pg_array_cardinality(v: *const u8, err: *mut c_int) -> i32;
    fn pg_array_dims(v: *const u8, isnull: *mut c_int, out: *mut u8) -> i32;
}

fn install_detoast_once() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        detoast_seams::detoast_attr::set(|mcx, image| {
            let mut v = mcx::vec_with_capacity_in(mcx, image.len())?;
            mcx::vec_append_bytes(&mut v, image)?;
            Ok(v)
        });
    });
}

type Wrapper = fn(
    Option<&mut types_fmgr::FmgrInfo>,
    &mut types_fmgr::FunctionCallInfoBaseData,
) -> types_error::PgResult<datum::Datum>;

fn call2(
    fc: Wrapper,
    a: datum::Datum,
    b: datum::Datum,
    ctx: &mcx::MemoryContext,
) -> types_error::PgResult<(bool, datum::Datum)> {
    let mut f = types_fmgr::LocalFcinfo::<2>::new(0);
    f.args[0] = datum::NullableDatum::value(a);
    f.args[1] = datum::NullableDatum::value(b);
    unsafe { f.set_result_mcx(ctx.mcx()) };
    let r = fc(None, &mut f)?;
    Ok((f.isnull, r))
}

/// The five sanity-checked members, dual-executed: both sides must say NULL,
/// and the Rust side must get there without panicking. reqdim is swept across
/// both of C's arms so the witness does not depend on one request.
#[test]
fn out_of_range_ndim_nulls_like_c_and_never_panics() {
    install_detoast_once();
    let ctx = mcx::MemoryContext::new_bump("corruption-plane");
    let dims = [2i32, 3, 5, 7, 11, 13];
    let lbs = [1i32, -4, 0, 9, i32::MIN, i32::MAX];
    for ndim in [-1i32, i32::MIN, 7, 8, 1000, i32::MAX] {
        let img = mk_corrupt_image(ndim, &dims, &lbs);
        let d = datum::Datum::from_usize(img.as_ptr() as usize);

        // array_ndims (1-arg; reqdim ignored)
        let mut c_null: c_int = 0;
        let c = unsafe { pg_array_ndims(img.as_ptr(), &mut c_null) };
        assert_eq!((c_null, c), (1, 0), "C array_ndims ndim={ndim}");
        let (rnull, _) = call2(
            arrayfuncs::ops::fc_array_ndims,
            d,
            datum::Datum::from_i32(0),
            &ctx,
        )
        .expect("fc_array_ndims must not error");
        assert!(rnull, "array_ndims ndim={ndim} must be NULL like C");

        // array_dims (1-arg): C's null arm, so no text is rendered either side
        let mut c_null: c_int = 0;
        let mut c_out = [0u8; MAXDIM * 33 + 1];
        let c = unsafe { pg_array_dims(img.as_ptr(), &mut c_null, c_out.as_mut_ptr()) };
        assert_eq!((c_null, c), (1, 0), "C array_dims ndim={ndim}");
        let (rnull, _) = call2(
            arrayfuncs::ops::fc_array_dims,
            d,
            datum::Datum::from_i32(0),
            &ctx,
        )
        .expect("fc_array_dims must not error");
        assert!(rnull, "array_dims ndim={ndim} must be NULL like C");

        // the 2-arg members, over a reqdim sweep spanning both C arms
        let cases: [(
            unsafe extern "C" fn(*const u8, i32, *mut c_int) -> i32,
            Wrapper,
            &str,
        ); 3] = [
            (
                pg_array_lower,
                arrayfuncs::ops::fc_array_lower,
                "array_lower",
            ),
            (
                pg_array_upper,
                arrayfuncs::ops::fc_array_upper,
                "array_upper",
            ),
            (
                pg_array_length,
                arrayfuncs::builtins::fc_array_length,
                "array_length",
            ),
        ];
        for reqdim in [i32::MIN, -1, 0, 1, 2, 6, 7, i32::MAX] {
            for (cfn, rfn, name) in cases {
                let mut c_null: c_int = 0;
                let c = unsafe { cfn(img.as_ptr(), reqdim, &mut c_null) };
                assert_eq!((c_null, c), (1, 0), "C {name} ndim={ndim} reqdim={reqdim}");
                let (rnull, _) = call2(rfn, d, datum::Datum::from_i32(reqdim), &ctx)
                    .unwrap_or_else(|_| panic!("{name} must not error"));
                assert!(
                    rnull,
                    "{name} ndim={ndim} reqdim={reqdim} must be NULL like C"
                );
            }
        }
    }
}

/// array_cardinality, non-positive ndim: C's ArrayGetNItems takes its own
/// `ndim <= 0 -> return 0` arm, so the answer is the VALUE 0, not a NULL.
/// Dual-executed (C reads no dim words on that arm).
#[test]
fn nonpositive_ndim_cardinality_is_value_zero_like_c() {
    install_detoast_once();
    let ctx = mcx::MemoryContext::new_bump("corruption-plane");
    let dims = [2i32, 3, 5, 7, 11, 13];
    let lbs = [1i32; MAXDIM];
    for ndim in [0i32, -1, -2, i32::MIN] {
        let img = mk_corrupt_image(ndim, &dims, &lbs);
        let d = datum::Datum::from_usize(img.as_ptr() as usize);
        let mut c_err: c_int = 0;
        let c = unsafe { pg_array_cardinality(img.as_ptr(), &mut c_err) };
        assert_eq!((c_err, c), (0, 0), "C array_cardinality ndim={ndim}");
        let (rnull, rv) = call2(
            arrayfuncs::ops::fc_array_cardinality,
            d,
            datum::Datum::from_i32(0),
            &ctx,
        )
        .expect("fc_array_cardinality must not error on non-positive ndim");
        assert!(!rnull, "array_cardinality never returns NULL");
        assert_eq!(rv.as_i32(), c, "array_cardinality ndim={ndim}");
    }
}

/// array_cardinality above MAXDIM: no C answer to match (module doc), so this
/// asserts pgrust's DEFINED replacement — a catchable error carrying C's own
/// dimension-count text and sqlstate. Never a panic, never a value. C is
/// intentionally not called on this cell.
#[test]
fn over_maxdim_cardinality_is_a_defined_error_not_a_panic() {
    install_detoast_once();
    let ctx = mcx::MemoryContext::new_bump("corruption-plane");
    let dims = [2i32, 3, 5, 7, 11, 13];
    let lbs = [1i32; MAXDIM];
    for ndim in [7i32, 8, 1000, i32::MAX] {
        let img = mk_corrupt_image(ndim, &dims, &lbs);
        let d = datum::Datum::from_usize(img.as_ptr() as usize);
        let e = call2(
            arrayfuncs::ops::fc_array_cardinality,
            d,
            datum::Datum::from_i32(0),
            &ctx,
        )
        .err()
        .unwrap_or_else(|| panic!("over-MAXDIM ndim={ndim} must not yield a value"));
        assert_eq!(
            e.message(),
            format!("number of array dimensions ({ndim}) exceeds the maximum allowed ({MAXDIM})")
        );
        assert_eq!(e.sqlstate, types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        assert_eq!(e.level, types_error::ERROR);
    }
}

/// The shipped reader itself, at the exact frame that used to panic: raw ndim
/// out, dims/lbounds zeroed, no unwind. This is the property the fix added and
/// the one a regression would break first — hence the catch_unwind is kept, with
/// its verdict inverted.
#[test]
fn read_dims_lbounds_returns_raw_ndim_without_unwinding() {
    let dims = [2i32, 3, 5, 7, 11, 13];
    let lbs = [1i32, -4, 0, 9, i32::MIN, i32::MAX];
    for ndim in [-1i32, i32::MIN, 7, 8, 1000, i32::MAX] {
        let img = mk_corrupt_image(ndim, &dims, &lbs);
        let r = std::panic::catch_unwind(|| arrayfuncs::foundation::read_dims_lbounds(&img));
        let (got, got_dims, got_lbs) =
            r.unwrap_or_else(|_| panic!("read_dims_lbounds panicked on ndim={ndim}"));
        assert_eq!(got, ndim, "ndim must come back RAW, never clamped");
        assert_eq!(got_dims, [0i32; MAXDIM], "out-of-range ndim fills nothing");
        assert_eq!(got_lbs, [0i32; MAXDIM]);
    }
    // Valid boundary, same call: mk_corrupt_image lays the body out exactly as
    // a 6-dim array would, so ndim=MAXDIM must read it all back. Guards against
    // the fix over-tightening the accepted boundary.
    let img = mk_corrupt_image(MAXDIM as i32, &dims, &lbs);
    assert_eq!(
        arrayfuncs::foundation::read_dims_lbounds(&img),
        (MAXDIM as i32, dims, lbs),
        "the fix must not over-tighten the accepted boundary"
    );
}
