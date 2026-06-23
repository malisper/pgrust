//! Unit tests for the `hashfunc.c` port.
//!
//! The bit mixers are a direct dependency (`common-hashfn`), so the tests
//! assert against the genuine PostgreSQL Jenkins values. The collation /
//! oidvector seams are owned by not-yet-ported crates, so the tests install
//! stand-ins via `::set` (idempotent, process-global; tests run
//! single-threaded).

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;

/// `OIDOID` (pg_type.dat) — the element type `check_valid_oidvector` insists on.
const OIDOID: Oid = 26;

/// Seam slots are process-global `OnceLock`s (a second `set` panics), so the
/// collation-determinism stand-in reads this flag instead of being re-installed.
static DETERMINISTIC: AtomicBool = AtomicBool::new(true);
static INSTALL: Once = Once::new();

/// Install the outward seams this crate calls once, with test stand-ins
/// matching the C contract.
fn install_seams() {
    INSTALL.call_once(|| {
        // check_valid_oidvector: enforce the same ndim/dataoffset/elemtype rule
        // as utils/adt/oid.c.
        check_valid_oidvector::set(|ndim, dataoffset, elemtype| {
            if ndim != 1 || dataoffset != 0 || elemtype != OIDOID {
                return Err(ereport(ERROR)
                    .errmsg("array is not a valid oidvector")
                    .into_error());
            }
            Ok(())
        });
        // collation_is_deterministic: read the test flag.
        collation_is_deterministic::set(|_collid| Ok(DETERMINISTIC.load(Ordering::SeqCst)));
        // pg_strxfrm: an identity transform stand-in (the trailing NUL is
        // appended by the body, not the seam — matching pg_strnxfrm's `bsize`
        // significant bytes contract).
        pg_strxfrm::set(|mcx, _collid, src| mcx::slice_in(mcx, src));
    });
    DETERMINISTIC.store(true, Ordering::SeqCst);
}

// ---------------------------------------------------------------------------
// Integer family
// ---------------------------------------------------------------------------

#[test]
fn int_family_matches_uint32_mixer() {
    for n in [-7i32, -1, 0, 1, 42, 1_000_000, i32::MIN, i32::MAX] {
        assert_eq!(hashint4(n), hashfn::hash_bytes_uint32(n as u32));
    }
    assert_eq!(hashint2(-5), hashfn::hash_bytes_uint32((-5i32) as u32));
    assert_eq!(hashchar(-1), hashfn::hash_bytes_uint32((-1i32) as u32));
    assert_eq!(hashchar(127), hashfn::hash_bytes_uint32(127u32));
    assert_eq!(hashoid(12345), hashfn::hash_bytes_uint32(12345));
    assert_eq!(hashenum(12345), hashfn::hash_bytes_uint32(12345));
}

#[test]
fn int8_cross_type_equality() {
    for n in [0i32, 1, -1, 7, -7, 100000, -100000, i32::MAX, i32::MIN] {
        assert_eq!(hashint8(n as i64), hashint4(n), "mismatch for {n}");
    }
    for n in [0i16, 1, -1, 99, -99] {
        assert_eq!(hashint8(n as i64), hashint2(n), "mismatch for {n}");
    }
}

#[test]
fn int8_fold_is_correct() {
    let val: i64 = 0x0000_0001_0000_0002;
    assert_eq!(hashint8_fold(val), 0x0000_0002 ^ 0x0000_0001);
    let val: i64 = -1;
    assert_eq!(hashint8_fold(val), 0xFFFF_FFFFu32 ^ !0xFFFF_FFFFu32);
}

#[test]
fn extended_seed_threads_through() {
    let seed = 0xDEAD_BEEF_CAFE_F00D;
    assert_eq!(
        hashint4extended(42, seed),
        hashfn::hash_bytes_uint32_extended(42u32, seed)
    );
    assert_eq!(
        hashoidextended(99, seed),
        hashfn::hash_bytes_uint32_extended(99u32, seed)
    );
    assert_eq!(hashint8extended(42, seed), hashint4extended(42, seed));
}

// ---------------------------------------------------------------------------
// Float family
// ---------------------------------------------------------------------------

#[test]
fn float_zero_collapses() {
    assert_eq!(hashfloat4(0.0), 0);
    assert_eq!(hashfloat4(-0.0), 0);
    assert_eq!(hashfloat8(0.0), 0);
    assert_eq!(hashfloat8(-0.0), 0);
    let seed = 0x1234_5678_9ABC_DEF0;
    assert_eq!(hashfloat4extended(0.0, seed), seed);
    assert_eq!(hashfloat4extended(-0.0, seed), seed);
    assert_eq!(hashfloat8extended(0.0, seed), seed);
    assert_eq!(hashfloat8extended(-0.0, seed), seed);
}

#[test]
fn float_nan_canonicalized() {
    let nan_a = f64::from_bits(0x7FF8_0000_0000_0001);
    let nan_b = f64::from_bits(0xFFF8_0000_0000_0007);
    assert!(nan_a.is_nan() && nan_b.is_nan());
    let expected = hashfn::hash_bytes(&get_float8_nan().to_ne_bytes());
    assert_eq!(hashfloat8(nan_a), expected);
    assert_eq!(hashfloat8(nan_b), expected);
    assert_eq!(hashfloat4(f32::NAN), expected);
}

#[test]
fn float4_float8_cross_type_equality() {
    for x in [1.0f32, 2.5, -3.25, 1e10, -1e-10, 12345.678] {
        assert_eq!(hashfloat4(x), hashfloat8(x as f64), "mismatch for {x}");
    }
}

// ---------------------------------------------------------------------------
// oidvector
// ---------------------------------------------------------------------------

/// Build an oidvector varlena image: the 24-byte header (vl_len_, ndim,
/// dataoffset, elemtype, dim1, lbound1) followed by the `values` Oid array.
fn make_oidvector_image(ndim: i32, values: &[Oid]) -> Vec<u8> {
    let mut img = Vec::new();
    img.extend_from_slice(&0i32.to_ne_bytes()); // vl_len_
    img.extend_from_slice(&ndim.to_ne_bytes()); // ndim
    img.extend_from_slice(&0i32.to_ne_bytes()); // dataoffset
    img.extend_from_slice(&(OIDOID as i32).to_ne_bytes()); // elemtype
    img.extend_from_slice(&(values.len() as i32).to_ne_bytes()); // dim1
    img.extend_from_slice(&0i32.to_ne_bytes()); // lbound1
    for v in values {
        img.extend_from_slice(&v.to_ne_bytes());
    }
    img
}

#[test]
fn oidvector_hashes_value_image() {
    install_seams();
    let image = make_oidvector_image(1, &[1, 2, 3]);
    let mut values_image: Vec<u8> = Vec::new();
    for x in [1u32, 2, 3] {
        values_image.extend_from_slice(&x.to_ne_bytes());
    }
    assert_eq!(
        hashoidvector(&image).unwrap(),
        hashfn::hash_bytes(&values_image)
    );
    let seed = 7;
    assert_eq!(
        hashoidvectorextended(&image, seed).unwrap(),
        hashfn::hash_bytes_extended(&values_image, seed)
    );
}

#[test]
fn oidvector_rejects_invalid() {
    install_seams();
    let image = make_oidvector_image(2, &[1]); // violate ndim == 1
    assert!(hashoidvector(&image).is_err());
}

// ---------------------------------------------------------------------------
// name / varlena / bytea
// ---------------------------------------------------------------------------

#[test]
fn name_varlena_bytea_hash_their_image() {
    let bytes = b"hello world";
    assert_eq!(hashname(bytes), hashfn::hash_bytes(bytes));
    assert_eq!(hashvarlena(bytes), hashfn::hash_bytes(bytes));
    assert_eq!(hashbytea(bytes), hashvarlena(bytes));
    let seed = 0xABCD;
    assert_eq!(hashbyteaextended(bytes, seed), hashvarlenaextended(bytes, seed));
    assert_eq!(
        hashnameextended(bytes, seed),
        hashfn::hash_bytes_extended(bytes, seed)
    );
}

// ---------------------------------------------------------------------------
// text (collation-aware)
// ---------------------------------------------------------------------------

#[test]
fn text_zero_collation_errors() {
    install_seams();
    let err = hashtext(b"abc", 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INDETERMINATE_COLLATION);
    assert!(hashtextextended(b"abc", 0, 1).is_err());
}

#[test]
fn text_deterministic_hashes_image() {
    install_seams();
    DETERMINISTIC.store(true, Ordering::SeqCst);
    let key = b"deterministic";
    assert_eq!(hashtext(key, 100).unwrap(), hashfn::hash_bytes(key));
    let seed = 55;
    assert_eq!(
        hashtextextended(key, 100, seed).unwrap(),
        hashfn::hash_bytes_extended(key, seed)
    );
}

#[test]
fn text_nondeterministic_uses_strxfrm() {
    install_seams();
    DETERMINISTIC.store(false, Ordering::SeqCst);
    let key = b"nd";
    // The body appends the trailing NUL to the (identity) transform.
    let mut transformed: Vec<u8> = key.to_vec();
    transformed.push(0);
    assert_eq!(
        hashtext(key, 100).unwrap(),
        hashfn::hash_bytes(&transformed)
    );
    DETERMINISTIC.store(true, Ordering::SeqCst);
}
