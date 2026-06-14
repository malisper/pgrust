//! Tests for the datum.c port: the value-model (`Datum` enum) lane (byte model
//! + the migration-target `*_v` seam shape) and the residual bare-word
//! (`ScalarWord`) raw-pointer ABI edge.

use super::*;
use mcx::MemoryContext;

// Build a plain 4-byte-header varlena image (uncompressed inline) carrying
// `payload`. Mirrors `SET_VARSIZE` over a fresh buffer.
fn varlena_4b(payload: &[u8]) -> alloc::vec::Vec<u8> {
    let total = (VARHDRSZ + payload.len()) as u32;
    let mut v = alloc::vec::Vec::with_capacity(VARHDRSZ + payload.len());
    let hdr = (total << 2).to_ne_bytes(); // little-endian SET_VARSIZE_4B
    v.extend_from_slice(&hdr);
    v.extend_from_slice(payload);
    v
}

// ---- datumGetSize (byte model) --------------------------------------------

#[test]
fn get_size_byval_and_fixed_byref() {
    // by-value: typLen
    assert_eq!(datum_get_size_bytes(None, true, 8).unwrap(), 8);
    // fixed by-ref: typLen
    let img = [0u8; 16];
    assert_eq!(datum_get_size_bytes(Some(&img), false, 16).unwrap(), 16);
}

#[test]
fn get_size_varlena_and_cstring() {
    let vl = varlena_4b(b"hello");
    assert_eq!(datum_get_size_bytes(Some(&vl), false, -1).unwrap(), vl.len());

    let cstr = b"abc\0extra";
    assert_eq!(datum_get_size_bytes(Some(cstr), false, -2).unwrap(), 4);
}

#[test]
fn get_size_invalid_typlen_errors() {
    assert!(datum_get_size_bytes(Some(&[0u8]), false, -7).is_err());
}

// ---- datumCopy (byte model / value enum) ----------------------------------

#[test]
fn copy_byval_verbatim() {
    let ctx = MemoryContext::new("test");
    let v = Datum::from_i64(12345);
    let out = datum_copy(ctx.mcx(), &v, true, 8).unwrap();
    assert_eq!(out, Datum::from_i64(12345));
}

#[test]
fn copy_varlena_verbatim() {
    let ctx = MemoryContext::new("test");
    let vl = varlena_4b(b"world!");
    let v = Datum::ByRef(slice_in(ctx.mcx(), &vl).unwrap());
    let out = datum_copy(ctx.mcx(), &v, false, -1).unwrap();
    assert_eq!(out.as_ref_bytes(), &vl[..]);
}

#[test]
fn copy_fixed_byref() {
    let ctx = MemoryContext::new("test");
    let img = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    let v = Datum::ByRef(slice_in(ctx.mcx(), &img).unwrap());
    let out = datum_copy(ctx.mcx(), &v, false, 12).unwrap();
    assert_eq!(out.as_ref_bytes(), &img[..]);
}

// ---- datumIsEqual / image_eq / image_hash (byte model) --------------------

#[test]
fn is_equal_byval_and_byref() {
    let a = Datum::from_i64(7);
    let b = Datum::from_i64(7);
    let c = Datum::from_i64(8);
    assert!(datum_is_equal(&a, &b, true, 8).unwrap());
    assert!(!datum_is_equal(&a, &c, true, 8).unwrap());

    let ctx = MemoryContext::new("test");
    let v1 = Datum::ByRef(slice_in(ctx.mcx(), &varlena_4b(b"xy")).unwrap());
    let v2 = Datum::ByRef(slice_in(ctx.mcx(), &varlena_4b(b"xy")).unwrap());
    let v3 = Datum::ByRef(slice_in(ctx.mcx(), &varlena_4b(b"xz")).unwrap());
    assert!(datum_is_equal(&v1, &v2, false, -1).unwrap());
    assert!(!datum_is_equal(&v1, &v3, false, -1).unwrap());
}

#[test]
fn image_eq_byte_model() {
    let ctx = MemoryContext::new("test");
    let v1 = Datum::ByRef(slice_in(ctx.mcx(), &varlena_4b(b"abc")).unwrap());
    let v2 = Datum::ByRef(slice_in(ctx.mcx(), &varlena_4b(b"abc")).unwrap());
    let v3 = Datum::ByRef(slice_in(ctx.mcx(), &varlena_4b(b"abcd")).unwrap());
    assert!(datum_image_eq_bytes(&v1, &v2, false, -1).unwrap());
    assert!(!datum_image_eq_bytes(&v1, &v3, false, -1).unwrap());
    // `*_v` seam shape is the same logic.
    assert!(datum_image_eq_v(&v1, &v2, false, -1).unwrap());

    // cstring
    let c1 = Datum::ByRef(slice_in(ctx.mcx(), b"hi\0").unwrap());
    let c2 = Datum::ByRef(slice_in(ctx.mcx(), b"hi\0").unwrap());
    assert!(datum_image_eq_bytes(&c1, &c2, false, -2).unwrap());
}

#[test]
fn image_hash_byte_model_matches_payload() {
    let ctx = MemoryContext::new("test");
    let vl = varlena_4b(b"hashme");
    let v = Datum::ByRef(slice_in(ctx.mcx(), &vl).unwrap());
    // Logical payload is "hashme".
    assert_eq!(
        datum_image_hash_bytes(&v, false, -1).unwrap(),
        common_hashfn::hash_bytes(b"hashme")
    );
    assert_eq!(
        datum_image_hash_v(&v, false, -1).unwrap(),
        common_hashfn::hash_bytes(b"hashme")
    );
}

#[test]
fn btequalimage_is_true() {
    assert!(btequalimage(25));
}

// ---- Value-enum estimate / serialize round-trip (`*_v` lane) --------------

#[test]
fn estimate_serialize_v_null() {
    let need = datum_estimate_space_v(&Datum::null(), true, true, 8);
    assert_eq!(need, core::mem::size_of::<i32>());
    let mut buf = alloc::vec![0u8; need];
    let end = datum_serialize_v(&Datum::null(), true, true, 8, buf.as_mut_ptr());
    assert_eq!(end as usize - buf.as_ptr() as usize, need);
}

#[test]
fn estimate_serialize_v_byval() {
    let d = Datum::from_i64(0x0123_4567_89AB_CDEF);
    let need = datum_estimate_space_v(&d, false, true, 8);
    assert_eq!(need, core::mem::size_of::<i32>() + core::mem::size_of::<usize>());
    let mut buf = alloc::vec![0u8; need];
    let end = datum_serialize_v(&d, false, true, 8, buf.as_mut_ptr());
    assert_eq!(end as usize - buf.as_ptr() as usize, need);
    // Restore via the bare-word cursor lane: round-trips the word.
    let (val, isnull, _adv) = datum_restore(buf.as_mut_ptr());
    assert!(!isnull);
    assert_eq!(val.as_i64(), 0x0123_4567_89AB_CDEF);
}

#[test]
fn estimate_serialize_v_varlena() {
    let ctx = MemoryContext::new("test");
    let vl = varlena_4b(b"roundtrip");
    let d = Datum::ByRef(slice_in(ctx.mcx(), &vl).unwrap());
    let need = datum_estimate_space_v(&d, false, false, -1);
    assert_eq!(need, core::mem::size_of::<i32>() + vl.len());
    let mut buf = alloc::vec![0u8; need];
    let end = datum_serialize_v(&d, false, false, -1, buf.as_mut_ptr());
    assert_eq!(end as usize - buf.as_ptr() as usize, need);

    let (val, isnull, _adv) = datum_restore(buf.as_mut_ptr());
    assert!(!isnull);
    let restored = unsafe { core::slice::from_raw_parts(val.as_usize() as *const u8, vl.len()) };
    assert_eq!(restored, &vl[..]);
}

// ---- Residual bare-word ABI edge (ScalarWord) -----------------------------

// The bare-word `datum_copy_word` / `datum_transfer` deep-copy tests were
// removed with those functions, and the `word_image_eq_*` tests were removed
// with `datum_image_eq_word`: their by-reference legs forged / dereferenced a
// pointer word (`leak_bytes_as_datum` / `datum_ptr_slice`). The canonical
// by-reference copy and image-equality are covered by `copy_varlena_verbatim` /
// `copy_fixed_byref` and the `image_eq_*` tests above, which exercise the
// byte-model `datum_copy` / `datum_image_eq_bytes` over `Datum::ByRef`.

// ---- Serialize / restore round-trip (bare-word cursor lane) ---------------

#[test]
fn serialize_restore_null() {
    let mut buf = alloc::vec![0u8; core::mem::size_of::<i32>()];
    let end = datum_serialize(ScalarWord::null(), true, true, 8, buf.as_mut_ptr());
    assert_eq!(end as usize - buf.as_ptr() as usize, core::mem::size_of::<i32>());
    let (val, isnull, _adv) = datum_restore(buf.as_mut_ptr());
    assert!(isnull);
    assert_eq!(val, ScalarWord::null());
}

#[test]
fn serialize_restore_byval() {
    let d = ScalarWord::from_i64(0x0123_4567_89AB_CDEF);
    let need = core::mem::size_of::<i32>() + core::mem::size_of::<usize>();
    let mut buf = alloc::vec![0u8; need];
    let end = datum_serialize(d, false, true, 8, buf.as_mut_ptr());
    assert_eq!(end as usize - buf.as_ptr() as usize, need);
    let (val, isnull, _adv) = datum_restore(buf.as_mut_ptr());
    assert!(!isnull);
    assert_eq!(val, d);
}

#[test]
fn serialize_restore_varlena() {
    let vl = varlena_4b(b"roundtrip");
    let d = ScalarWord::from_usize(vl.as_ptr() as usize);
    let need = core::mem::size_of::<i32>() + vl.len();
    let mut buf = alloc::vec![0u8; need];
    let end = datum_serialize(d, false, false, -1, buf.as_mut_ptr());
    assert_eq!(end as usize - buf.as_ptr() as usize, need);

    let (val, isnull, _adv) = datum_restore(buf.as_mut_ptr());
    assert!(!isnull);
    let restored = unsafe { core::slice::from_raw_parts(val.as_usize() as *const u8, vl.len()) };
    assert_eq!(restored, &vl[..]);
}
