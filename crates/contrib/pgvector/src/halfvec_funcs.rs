//! pgvector 0.8.5 halfvec.c fmgr entry points: I/O + typmod cast + casts /
//! array conversions (array_to_halfvec, halfvec_to_float4, vector_to_halfvec;
//! halfvec_to_vector is registered here too though its C home is vector.c).
use datum::Datum;
use mcx::PgVec;
use types_core::{Oid, FLOAT4OID, FLOAT8OID, INT4OID, NUMERICOID};
use types_error::{
    PgError, PgResult, ERRCODE_DATA_EXCEPTION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NULL_VALUE_NOT_ALLOWED,
};
use types_fmgr::{cstring_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_tuple::varatt;

use crate::funcs::{detoasted_image, image_datum};
use crate::half::*;
use crate::halfutils::{float4_to_half_unchecked, half_is_inf, half_is_zero};
use crate::vec::{
    check_dim as vec_check_dim, check_expected_dim as vec_check_expected_dim, VecBuilder, VecView,
};

// SAFETY contract of callers: arg i is a non-null halfvec varlena (strict fns).
unsafe fn arg_halfvec<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<HalfVecView<'a>> {
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    HalfVecView::from_payload(v.data())
}

// SAFETY contract of callers: arg i is a non-null vector varlena (strict fns).
unsafe fn arg_vector<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<VecView<'a>> {
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    VecView::from_payload(v.data())
}

fn binary_2arg<'a>(fcinfo: &'a Fcinfo) -> PgResult<(HalfVecView<'a>, HalfVecView<'a>)> {
    // SAFETY: strict fns — both args are halfvec varlenas.
    Ok(unsafe { (arg_halfvec(fcinfo, 0)?, arg_halfvec(fcinfo, 1)?) })
}

pub fn fc_halfvec_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict input fn — arg0 cstring, arg2 typmod.
    let lit = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let typmod = fcinfo.arg_i32(2);
    let mut x = [0u16; HALFVEC_MAX_DIM];
    let dim = parse_halfvec(lit, typmod, &mut x)?;
    let mut b = HalfVecBuilder::new(fcinfo.result_mcx(), dim)?;
    for (i, h) in x[..dim].iter().enumerate() {
        b.set_raw(i, *h);
    }
    Ok(image_datum(b.image()))
}

pub fn fc_halfvec_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 halfvec.
    let v = unsafe { arg_halfvec(fcinfo, 0)? };
    let dim = v.dim();
    let mcx = fcinfo.result_mcx();
    let mut out: PgVec<'_, u8> =
        mcx::vec_with_capacity_in(mcx, ryu::FLOAT_SHORTEST_DECIMAL_LEN * dim + 3)?;
    let mut scratch = [0u8; ryu::FLOAT_SHORTEST_DECIMAL_LEN];
    mcx::vec_append_bytes(&mut out, b"[")?;
    for i in 0..dim {
        if i > 0 {
            mcx::vec_append_bytes(&mut out, b",")?;
        }
        let n = ryu::float_to_shortest_decimal_bufn(v.x(i), &mut scratch);
        mcx::vec_append_bytes(&mut out, &scratch[..n])?;
    }
    mcx::vec_append_bytes(&mut out, b"]\0")?;
    Ok(cstring_result(out))
}

pub fn fc_halfvec_typmod_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: strict fn — arg0 cstring[].
    let arr = unsafe { detoasted_image(mcx, fcinfo.arg(0))? };
    let tl = arrayfuncs::array_get_integer_typmods(mcx, arr)?;
    if tl.len() != 1 {
        return Err(PgError::error("invalid type modifier")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .into());
    }
    if tl[0] < 1 {
        return Err(
            PgError::error("dimensions for type halfvec must be at least 1")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .into(),
        );
    }
    if tl[0] as usize > HALFVEC_MAX_DIM {
        return Err(PgError::error(format!(
            "dimensions for type halfvec cannot exceed {HALFVEC_MAX_DIM}"
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .into());
    }
    Ok(Datum::from_i32(tl[0]))
}

pub fn fc_halfvec_recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let typmod = fcinfo.arg_i32(2);
    let dim = pqformat::pq_getmsgint(buf, 2)? as u16 as i16;
    let unused = pqformat::pq_getmsgint(buf, 2)? as u16 as i16;
    if dim < 1 {
        check_dim(0)?;
    }
    check_dim(dim as usize)?;
    check_expected_dim(typmod, dim as usize)?;
    if unused != 0 {
        return Err(
            PgError::error(format!("expected unused to be 0, not {unused}"))
                .with_sqlstate(ERRCODE_DATA_EXCEPTION)
                .into(),
        );
    }
    let mut b = HalfVecBuilder::new(fcinfo.result_mcx(), dim as usize)?;
    for i in 0..dim as usize {
        let h = pqformat::pq_getmsgint(buf, 2)? as u16;
        check_element(h)?;
        b.set_raw(i, h);
    }
    Ok(image_datum(b.image()))
}

pub fn fc_halfvec_send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 halfvec.
    let v = unsafe { arg_halfvec(fcinfo, 0)? };
    let mut buf = pqformat::pq_begintypsend(fcinfo.result_mcx())?;
    pqformat::pq_sendint(&mut buf, v.dim() as u32, 2)?;
    pqformat::pq_sendint(&mut buf, 0, 2)?;
    for i in 0..v.dim() {
        pqformat::pq_sendint(&mut buf, v.raw(i) as u32, 2)?;
    }
    Ok(types_fmgr::varlena_result(pqformat::pq_endtypsend(buf)))
}

pub fn fc_halfvec(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 halfvec, arg1 typmod.
    let v = unsafe { arg_halfvec(fcinfo, 0)? };
    let typmod = fcinfo.arg_i32(1);
    check_expected_dim(typmod, v.dim())?;
    Ok(fcinfo.arg(0))
}

// C: halfvec.c array_to_halfvec. Mirrors funcs::fc_array_to_vector's
// structure (ndim/nulls checks, deconstruct_array_builtin / numeric path,
// CheckDim/CheckExpectedDim), but per-element conversion goes through
// float4_to_half (C: Float4ToHalf), which raises the "is out of range for
// type halfvec" error inline on finite-input overflow; NaN/already-infinite
// inputs pass through that conversion and are only caught by the final
// CheckElement sweep below (matching C's two-phase structure exactly).
pub fn fc_array_to_halfvec(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: strict fn — arg0 array, arg1 typmod.
    let arr = unsafe { detoasted_image(mcx, fcinfo.arg(0))? };
    let typmod = fcinfo.arg_i32(1);

    if arrayfuncs::arr_ndim(arr) > 1 {
        return Err(PgError::error("array must be 1-D")
            .with_sqlstate(ERRCODE_DATA_EXCEPTION)
            .into());
    }
    if arrayfuncs::arr_hasnull(arr) && arrayfuncs::array_contains_nulls(arr) {
        return Err(PgError::error("array must not contain nulls")
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
            .into());
    }

    let elemtype: Oid = arrayfuncs::arr_elemtype(arr);
    // numeric is not in builtin_meta: varlena, int-aligned.
    let (elems, _nulls) = if elemtype == NUMERICOID {
        arrayfuncs::deconstruct_array(mcx, arr, -1, false, b'i', true)?
    } else {
        arrayfuncs::deconstruct_array_builtin(mcx, arr, elemtype, true)?
    };
    let n = elems.len();
    check_dim(n)?;
    check_expected_dim(typmod, n)?;

    let mut b = HalfVecBuilder::new(mcx, n)?;
    match elemtype {
        INT4OID => {
            for (i, d) in elems.iter().enumerate() {
                b.set(i, d.as_i32() as f32)?;
            }
        }
        // C: Float4ToHalf(DatumGetFloat8(...)) — the double implicitly
        // narrows to float at the call site (overflow rounds to the
        // matching infinity, same as Rust's `as f32`); Float4ToHalf then
        // sees that already-infinite float and does not treat it as an
        // overflow of a finite value.
        FLOAT8OID => {
            for (i, d) in elems.iter().enumerate() {
                b.set(i, d.as_f64() as f32)?;
            }
        }
        FLOAT4OID => {
            for (i, d) in elems.iter().enumerate() {
                b.set(i, d.as_f32())?;
            }
        }
        NUMERICOID => {
            for (i, d) in elems.iter().enumerate() {
                let p = d.as_usize() as *const u8;
                // SAFETY: non-null numeric element datum inside the array image.
                let payload = unsafe {
                    let total = varatt::varsize_any(p);
                    let hdr = if varatt::varatt_is_1b(p) { 1 } else { 4 };
                    core::slice::from_raw_parts(p.add(hdr), total - hdr)
                };
                let f4 = adt_numeric::ops::numeric_float4(adt_numeric::Num::from_payload(payload))?;
                b.set(i, f4)?;
            }
        }
        _ => {
            return Err(PgError::error("unsupported array type")
                .with_sqlstate(ERRCODE_DATA_EXCEPTION)
                .into())
        }
    }

    // C: final "Check elements" loop — catches NaN and already-infinite
    // inputs that float4_to_half's inline overflow check does not reject.
    for i in 0..n {
        check_element(b.get_raw(i))?;
    }
    Ok(image_datum(b.image()))
}

// C: halfvec.c halfvec_to_float4. Same real[] construction helper as
// funcs::fc_vector_to_float4.
pub fn fc_halfvec_to_float4(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 halfvec.
    let v = unsafe { arg_halfvec(fcinfo, 0)? };
    let mcx = fcinfo.result_mcx();
    let mut datums: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, v.dim())?;
    for i in 0..v.dim() {
        datums.push(Datum::from_f32(v.x(i)));
    }
    let img = arrayfuncs::construct_array(mcx, &datums, FLOAT4OID, 4, true, b'i')?;
    Ok(image_datum(img))
}

// C: halfvec.c vector_to_halfvec. CheckDim/CheckExpectedDim here are
// halfvec.c's own (crate::half's, imported above) — matching C exactly, even
// though HALFVEC_MAX_DIM == VECTOR_MAX_DIM numerically.
pub fn fc_vector_to_halfvec(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 vector, arg1 typmod.
    let v = unsafe { arg_vector(fcinfo, 0)? };
    let typmod = fcinfo.arg_i32(1);
    check_dim(v.dim())?;
    check_expected_dim(typmod, v.dim())?;

    let mcx = fcinfo.result_mcx();
    let mut b = HalfVecBuilder::new(mcx, v.dim())?;
    for i in 0..v.dim() {
        b.set(i, v.x(i))?;
    }
    Ok(image_datum(b.image()))
}

// C: vector.c halfvec_to_vector (registered from this module per the task
// brief). CheckDim/CheckExpectedDim here are vector.c's own (crate::vec's,
// aliased above). HalfToFloat4 never overflows f32, so no fallible set/final
// check is needed, unlike the vector_to_halfvec direction.
pub fn fc_halfvec_to_vector(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 halfvec, arg1 typmod.
    let v = unsafe { arg_halfvec(fcinfo, 0)? };
    let typmod = fcinfo.arg_i32(1);
    vec_check_dim(v.dim())?;
    vec_check_expected_dim(typmod, v.dim())?;

    let mcx = fcinfo.result_mcx();
    let mut b = VecBuilder::new(mcx, v.dim())?;
    for i in 0..v.dim() {
        b.set(i, v.x(i));
    }
    Ok(image_datum(b.image()))
}

// ---- Task 5: distances, norm, arithmetic, quantize, subvector, concat ----
// Mirrors funcs.rs's vector equivalents (fc_l2_distance .. fc_subvector),
// swapping VecView/VecBuilder for HalfVecView/HalfVecBuilder and the vec.rs
// kernels for half.rs's (l2_squared_distance, inner_product,
// cosine_similarity, l1_distance, norm), per $S/src/halfvec.c ~555-1000.

pub fn fc_halfvec_l2_distance(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = binary_2arg(fcinfo)?;
    check_dims(&a, &b)?;
    Ok(Datum::from_f64((l2_squared_distance(&a, &b) as f64).sqrt()))
}

pub fn fc_halfvec_l2_squared_distance(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (a, b) = binary_2arg(fcinfo)?;
    check_dims(&a, &b)?;
    Ok(Datum::from_f64(l2_squared_distance(&a, &b) as f64))
}

pub fn fc_halfvec_inner_product(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = binary_2arg(fcinfo)?;
    check_dims(&a, &b)?;
    Ok(Datum::from_f64(inner_product(&a, &b) as f64))
}

pub fn fc_halfvec_negative_inner_product(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (a, b) = binary_2arg(fcinfo)?;
    check_dims(&a, &b)?;
    Ok(Datum::from_f64(-(inner_product(&a, &b) as f64)))
}

pub fn fc_halfvec_cosine_distance(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = binary_2arg(fcinfo)?;
    check_dims(&a, &b)?;
    let mut similarity = cosine_similarity(&a, &b);
    // C: "Keep in range"
    if similarity > 1.0 {
        similarity = 1.0;
    } else if similarity < -1.0 {
        similarity = -1.0;
    }
    Ok(Datum::from_f64(1.0 - similarity))
}

pub fn fc_halfvec_spherical_distance(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (a, b) = binary_2arg(fcinfo)?;
    check_dims(&a, &b)?;
    let mut distance = inner_product(&a, &b) as f64;
    // C: "Prevent NaN with acos with loss of precision"
    if distance > 1.0 {
        distance = 1.0;
    } else if distance < -1.0 {
        distance = -1.0;
    }
    Ok(Datum::from_f64(distance.acos() / core::f64::consts::PI))
}

pub fn fc_halfvec_l1_distance(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = binary_2arg(fcinfo)?;
    check_dims(&a, &b)?;
    Ok(Datum::from_f64(l1_distance(&a, &b) as f64))
}

pub fn fc_halfvec_vector_dims(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 halfvec.
    let a = unsafe { arg_halfvec(fcinfo, 0)? };
    Ok(Datum::from_i32(a.dim() as i32))
}

pub fn fc_halfvec_l2_norm(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 halfvec.
    let a = unsafe { arg_halfvec(fcinfo, 0)? };
    Ok(Datum::from_f64(norm(&a)))
}

// C: halfvec_l2_normalize builds its own result HalfVector directly; we
// instead reuse halfvec_l2_normalize_image (half.rs), which is also the
// HNSW normalize callback, so it needs a full varlena image (4B header +
// payload). Rebuild that image from the arg view rather than duplicating
// the norm/divide/overflow-check logic here.
pub fn fc_halfvec_l2_normalize(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 halfvec.
    let v = unsafe { arg_halfvec(fcinfo, 0)? };
    let mcx = fcinfo.result_mcx();
    let mut b = HalfVecBuilder::new(mcx, v.dim())?;
    for i in 0..v.dim() {
        b.set_raw(i, v.raw(i));
    }
    Ok(image_datum(halfvec_l2_normalize_image(mcx, &b.image())?))
}

// C halfvec_add/sub/mul: compute in f32 (HalfToFloat4'd operands), narrow via
// Float4ToHalfUnchecked (no per-set range check — the two checks below run
// once, over the whole result, exactly like C's two post-loop sweeps), then:
//   - overflow: HalfIsInf(rx[i]) -> float_overflow_error() ("value out of
//     range: overflow", ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE) for all three ops.
//   - underflow (mul only): HalfIsZero(rx[i]) && !(HalfIsZero(ax[i]) ||
//     HalfIsZero(bx[i])) -> float_underflow_error() ("value out of range:
//     underflow").
fn elementwise(
    fcinfo: &mut Fcinfo,
    op: impl Fn(f32, f32) -> f32,
    check_underflow: bool,
) -> PgResult<Datum> {
    let (a, b) = binary_2arg(fcinfo)?;
    check_dims(&a, &b)?;
    let mut r = HalfVecBuilder::new(fcinfo.result_mcx(), a.dim())?;
    for i in 0..a.dim() {
        r.set_raw(i, float4_to_half_unchecked(op(a.x(i), b.x(i))));
    }
    for i in 0..a.dim() {
        let h = r.get_raw(i);
        if half_is_inf(h) {
            return Err(Box::new(adt_float::float_overflow_error()));
        }
        if check_underflow && half_is_zero(h) && !(half_is_zero(a.raw(i)) || half_is_zero(b.raw(i))) {
            return Err(Box::new(adt_float::float_underflow_error()));
        }
    }
    Ok(image_datum(r.image()))
}

pub fn fc_halfvec_add(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    elementwise(fcinfo, |x, y| x + y, false)
}

pub fn fc_halfvec_sub(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    elementwise(fcinfo, |x, y| x - y, false)
}

pub fn fc_halfvec_mul(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    elementwise(fcinfo, |x, y| x * y, true)
}

pub fn fc_halfvec_concat(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (a, b) = binary_2arg(fcinfo)?;
    let dim = a.dim() + b.dim();
    check_dim(dim)?;
    let mut r = HalfVecBuilder::new(fcinfo.result_mcx(), dim)?;
    for i in 0..a.dim() {
        r.set_raw(i, a.raw(i));
    }
    for i in 0..b.dim() {
        r.set_raw(a.dim() + i, b.raw(i));
    }
    Ok(image_datum(r.image()))
}

// VarBit image: 4B varlena header, i32 bit length, zero-padded data bytes.
// Mirrors funcs::fc_binary_quantize exactly (same layout), reading from a
// HalfVecView instead of a VecView.
pub fn fc_halfvec_binary_quantize(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 halfvec.
    let a = unsafe { arg_halfvec(fcinfo, 0)? };
    let dim = a.dim();
    let nbytes = dim.div_ceil(8);
    let total = 4 + 4 + nbytes;
    let mut img: PgVec<'_, u8> = mcx::vec_with_capacity_in(fcinfo.result_mcx(), total)?;
    img.resize(total, 0);
    img[..4].copy_from_slice(&((total as u32) << 2).to_ne_bytes());
    img[4..8].copy_from_slice(&(dim as i32).to_ne_bytes());
    for i in 0..dim {
        if a.x(i) > 0.0 {
            img[8 + i / 8] |= 1 << (7 - (i % 8));
        }
    }
    Ok(image_datum(img))
}

// Mirrors funcs::fc_subvector exactly (same CheckDim call sites and error
// texts — half.rs's check_dim already renders the halfvec-specific message),
// reading/writing HalfVecView/HalfVecBuilder raw halves instead of f32s.
pub fn fc_halfvec_subvector(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 halfvec, args 1-2 int4.
    let a = unsafe { arg_halfvec(fcinfo, 0)? };
    let mut start = fcinfo.arg_i32(1);
    let count = fcinfo.arg_i32(2);

    if count < 1 {
        check_dim(0)?;
    }
    let adim = a.dim() as i32;
    // start + count without i32 overflow (both checked positive / bounded).
    let end = if start > adim - count { adim + 1 } else { start + count };
    if start < 1 {
        start = 1;
    } else if start > adim {
        check_dim(0)?;
    }
    // C's CheckDim takes a signed dim: a negative (end - start) must raise
    // the 22000 "at least 1 dimension" error, not wrap through usize into
    // the 54000 max-dimension branch.
    let dim = end - start;
    if dim < 1 {
        check_dim(0)?;
    }
    let dim = dim as usize;
    check_dim(dim)?;
    let mut r = HalfVecBuilder::new(fcinfo.result_mcx(), dim)?;
    for i in 0..dim {
        r.set_raw(i, a.raw((start - 1) as usize + i));
    }
    Ok(image_datum(r.image()))
}
