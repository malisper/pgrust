//! WITNESS (divergence CANDIDATE, corruption plane — no ruling here):
//! for a malformed flat array image with ndim outside 0..=MAXDIM, the C
//! builtins (array_lower et al.) return SQL NULL from the sanity check,
//! while shipped read_dims_lbounds (arrayfuncs/src/foundation.rs) loops
//! `0..ndim as usize` BEFORE the wrapper's sanity check and panics on the
//! stack-array/slice index (ndim > 6) or on a huge range (ndim < 0).
//! Such images are unreachable from any constructed array (array_in /
//! ArrayCheckBounds cap ndim at MAXDIM) — this only matters for corrupt
//! on-disk data, where C Postgres itself reads garbage for large ndim in
//! other paths. Candidate + witness per wave-2 rules; adjudication is the
//! panic-fatality doctrine's call, not this lane's.

use proof_arrayfuncs_hdr::{mk_image, MAXDIM};

#[test]
fn witness_ndim7_panics_where_c_returns_null() {
    // C side: sanity check fires, NULL verdict (exercised in native_diff's
    // plane; here we witness the Rust side).
    let img = mk_image(7, &[1; MAXDIM], &[1; MAXDIM]);
    let r = std::panic::catch_unwind(|| {
        // read_dims_lbounds is the first thing every dims-reading wrapper
        // does with the image; ndim=7 indexes dims[6] on a [i32; 6].
        arrayfuncs::foundation::read_dims_lbounds(&img)
    });
    assert!(
        r.is_err(),
        "expected index-out-of-bounds panic on ndim=7 (C returns NULL)"
    );
}
