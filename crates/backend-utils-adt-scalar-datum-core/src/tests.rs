//! Tests for the datum.c port: both the byte-model (`TupleValue`) lane and the
//! bare-`Datum` raw-pointer lane.

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

// ---- datumCopy (byte model) -----------------------------------------------

#[test]
fn copy_byval_verbatim() {
    let ctx = MemoryContext::new("test");
    let v = TupleValue::ByVal(Datum::from_i64(12345));
    let out = datum_copy(ctx.mcx(), &v, true, 8).unwrap();
    assert_eq!(out, TupleValue::ByVal(Datum::from_i64(12345)));
}

#[test]
fn copy_varlena_verbatim() {
    let ctx = MemoryContext::new("test");
    let vl = varlena_4b(b"world!");
    let v = TupleValue::ByRef(slice_in(ctx.mcx(), &vl).unwrap());
    let out = datum_copy(ctx.mcx(), &v, false, -1).unwrap();
    assert_eq!(out.as_ref_bytes(), &vl[..]);
}

#[test]
fn copy_fixed_byref() {
    let ctx = MemoryContext::new("test");
    let img = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
    let v = TupleValue::ByRef(slice_in(ctx.mcx(), &img).unwrap());
    let out = datum_copy(ctx.mcx(), &v, false, 12).unwrap();
    assert_eq!(out.as_ref_bytes(), &img[..]);
}

// ---- datumIsEqual / image_eq / image_hash (byte model) --------------------

#[test]
fn is_equal_byval_and_byref() {
    let a = TupleValue::ByVal(Datum::from_i64(7));
    let b = TupleValue::ByVal(Datum::from_i64(7));
    let c = TupleValue::ByVal(Datum::from_i64(8));
    assert!(datum_is_equal(&a, &b, true, 8).unwrap());
    assert!(!datum_is_equal(&a, &c, true, 8).unwrap());

    let ctx = MemoryContext::new("test");
    let v1 = TupleValue::ByRef(slice_in(ctx.mcx(), &varlena_4b(b"xy")).unwrap());
    let v2 = TupleValue::ByRef(slice_in(ctx.mcx(), &varlena_4b(b"xy")).unwrap());
    let v3 = TupleValue::ByRef(slice_in(ctx.mcx(), &varlena_4b(b"xz")).unwrap());
    assert!(datum_is_equal(&v1, &v2, false, -1).unwrap());
    assert!(!datum_is_equal(&v1, &v3, false, -1).unwrap());
}

#[test]
fn image_eq_byte_model() {
    let ctx = MemoryContext::new("test");
    let v1 = TupleValue::ByRef(slice_in(ctx.mcx(), &varlena_4b(b"abc")).unwrap());
    let v2 = TupleValue::ByRef(slice_in(ctx.mcx(), &varlena_4b(b"abc")).unwrap());
    let v3 = TupleValue::ByRef(slice_in(ctx.mcx(), &varlena_4b(b"abcd")).unwrap());
    assert!(datum_image_eq_bytes(&v1, &v2, false, -1).unwrap());
    assert!(!datum_image_eq_bytes(&v1, &v3, false, -1).unwrap());

    // cstring
    let c1 = TupleValue::ByRef(slice_in(ctx.mcx(), b"hi\0").unwrap());
    let c2 = TupleValue::ByRef(slice_in(ctx.mcx(), b"hi\0").unwrap());
    assert!(datum_image_eq_bytes(&c1, &c2, false, -2).unwrap());
}

#[test]
fn image_hash_byte_model_matches_payload() {
    let ctx = MemoryContext::new("test");
    let vl = varlena_4b(b"hashme");
    let v = TupleValue::ByRef(slice_in(ctx.mcx(), &vl).unwrap());
    // Logical payload is "hashme".
    assert_eq!(
        datum_image_hash_bytes(&v, false, -1).unwrap(),
        common_hashfn::hash_bytes(b"hashme")
    );
}

#[test]
fn btequalimage_is_true() {
    assert!(btequalimage(25));
}

// ---- Bare-Datum lane ------------------------------------------------------

#[test]
fn word_transfer_byval_is_verbatim() {
    // !typByVal is false -> else leg -> datumCopy returns value verbatim.
    let d = Datum::from_i64(42);
    assert_eq!(datum_transfer(d, true, 8), d);
}

#[test]
fn word_transfer_varlena_non_expanded_deep_copies() {
    // typLen == -1 but not an external-expanded-RW pointer -> else leg ->
    // datumCopy deep-copies the varlena verbatim.
    let vl = varlena_4b(b"transfer");
    let src = Datum::from_usize(vl.as_ptr() as usize);
    let out = datum_transfer(src, false, -1);
    assert_ne!(out.as_usize(), src.as_usize());
    let copied = unsafe { core::slice::from_raw_parts(out.as_usize() as *const u8, vl.len()) };
    assert_eq!(copied, &vl[..]);
}

#[test]
fn word_copy_byval() {
    let d = Datum::from_i64(99);
    assert_eq!(datum_copy_word(d, true, 8), d);
}

#[test]
fn word_copy_varlena_deep() {
    let vl = varlena_4b(b"deepcopy");
    let src = Datum::from_usize(vl.as_ptr() as usize);
    let out = datum_copy_word(src, false, -1);
    // The copy is a distinct allocation with identical bytes.
    assert_ne!(out.as_usize(), src.as_usize());
    let copied = unsafe { core::slice::from_raw_parts(out.as_usize() as *const u8, vl.len()) };
    assert_eq!(copied, &vl[..]);
}

#[test]
fn word_image_eq_varlena() {
    let a = varlena_4b(b"same");
    let b = varlena_4b(b"same");
    let c = varlena_4b(b"diff");
    let da = Datum::from_usize(a.as_ptr() as usize);
    let db = Datum::from_usize(b.as_ptr() as usize);
    let dc = Datum::from_usize(c.as_ptr() as usize);
    assert!(datum_image_eq_word(da, db, false, -1).unwrap());
    assert!(!datum_image_eq_word(da, dc, false, -1).unwrap());
}

#[test]
fn word_image_hash_varlena_matches_byte_lane() {
    let vl = varlena_4b(b"payload");
    let d = Datum::from_usize(vl.as_ptr() as usize);
    assert_eq!(
        datum_image_hash_word(d, false, -1).unwrap(),
        common_hashfn::hash_bytes(b"payload")
    );
}

#[test]
fn word_image_eq_byval() {
    assert!(datum_image_eq_word(Datum::from_i64(5), Datum::from_i64(5), true, 8).unwrap());
    assert!(!datum_image_eq_word(Datum::from_i64(5), Datum::from_i64(6), true, 8).unwrap());
}

// ---- Estimate / serialize / restore round-trip (bare-Datum lane) ----------

#[test]
fn serialize_restore_null() {
    let need = datum_estimate_space(Datum::null(), true, true, 8);
    assert_eq!(need, core::mem::size_of::<i32>());
    let mut buf = alloc::vec![0u8; need];
    let end = datum_serialize(Datum::null(), true, true, 8, buf.as_mut_ptr());
    assert_eq!(end as usize - buf.as_ptr() as usize, need);
    let (val, isnull, _adv) = datum_restore(buf.as_mut_ptr());
    assert!(isnull);
    assert_eq!(val, Datum::null());
}

#[test]
fn serialize_restore_byval() {
    let d = Datum::from_i64(0x0123_4567_89AB_CDEF);
    let need = datum_estimate_space(d, false, true, 8);
    assert_eq!(need, core::mem::size_of::<i32>() + core::mem::size_of::<Datum>());
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
    let d = Datum::from_usize(vl.as_ptr() as usize);
    let need = datum_estimate_space(d, false, false, -1);
    assert_eq!(need, core::mem::size_of::<i32>() + vl.len());
    let mut buf = alloc::vec![0u8; need];
    let end = datum_serialize(d, false, false, -1, buf.as_mut_ptr());
    assert_eq!(end as usize - buf.as_ptr() as usize, need);

    let (val, isnull, _adv) = datum_restore(buf.as_mut_ptr());
    assert!(!isnull);
    let restored = unsafe { core::slice::from_raw_parts(val.as_usize() as *const u8, vl.len()) };
    assert_eq!(restored, &vl[..]);
}
