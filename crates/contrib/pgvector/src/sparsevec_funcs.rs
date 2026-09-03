//! pgvector 0.8.5 sparsevec.c I/O fmgr entry points — mirrors funcs.rs's
//! vector counterparts. DIVERGENCES (recorded): none.
use datum::Datum;
use mcx::PgVec;
use types_core::{Oid, FLOAT4OID, FLOAT8OID, INT4OID, NUMERICOID};
use types_error::{
    PgError, PgResult, ERRCODE_DATA_EXCEPTION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NULL_VALUE_NOT_ALLOWED,
};
use types_fmgr::{cstring_result, varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_tuple::varatt;

use crate::funcs::{detoasted_image, image_datum};
use crate::sparse::{
    check_dim, check_element, check_expected_dim, check_index, check_nnz, parse_sparsevec,
    SparseInputElement, SparseVecBuilder, SparseVecView, SPARSEVEC_MAX_DIM,
};
use crate::vec::{VecBuilder, VecView};

// SAFETY contract of callers: arg i is a non-null sparsevec varlena (strict fns).
unsafe fn arg_sparsevec<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<SparseVecView<'a>> {
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    SparseVecView::from_payload(v.data())
}

// SAFETY contract of callers: arg i is a non-null vector varlena (strict fns).
unsafe fn arg_vector<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<VecView<'a>> {
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    VecView::from_payload(v.data())
}

pub fn fc_sparsevec_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict input fn — arg0 cstring, arg2 typmod.
    let lit = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let typmod = fcinfo.arg_i32(2);
    let mut elems: Vec<SparseInputElement> = Vec::new();
    let dim = parse_sparsevec(lit, typmod, &mut elems)?;
    let mut b = SparseVecBuilder::new(fcinfo.result_mcx(), dim, elems.len())?;
    for (i, e) in elems.iter().enumerate() {
        b.set(i, e.index, e.value);
    }
    Ok(image_datum(b.image()))
}

// C: sparsevec_out (sparsevec.c ~418-466). Buffer sized
// (11 + FLOAT_SHORTEST_DECIMAL_LEN) * nnz + 13: nnz*10 for indices, nnz for
// ':', nnz*(FLOAT_SHORTEST_DECIMAL_LEN - 1) for values, nnz-1 for ',', 10 for
// dim, 4 for '{', '}', '/', '\0'.
pub fn fc_sparsevec_out(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 sparsevec.
    let v = unsafe { arg_sparsevec(fcinfo, 0)? };
    let nnz = v.nnz();
    let cap = (11 + ryu::FLOAT_SHORTEST_DECIMAL_LEN) * nnz + 13;
    let mcx = fcinfo.result_mcx();
    let mut out: PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, cap)?;
    let mut int_scratch = [0u8; 11];
    let mut float_scratch = [0u8; ryu::FLOAT_SHORTEST_DECIMAL_LEN];

    mcx::vec_append_bytes(&mut out, b"{")?;
    for i in 0..nnz {
        if i > 0 {
            mcx::vec_append_bytes(&mut out, b",")?;
        }
        // Convert 0-based numbering (C) to 1-based (SQL).
        let n = numutils::pg_ltoa(v.index(i) + 1, &mut int_scratch);
        mcx::vec_append_bytes(&mut out, &int_scratch[..n])?;
        mcx::vec_append_bytes(&mut out, b":")?;
        let n = ryu::float_to_shortest_decimal_bufn(v.value(i), &mut float_scratch);
        mcx::vec_append_bytes(&mut out, &float_scratch[..n])?;
    }
    mcx::vec_append_bytes(&mut out, b"}/")?;
    let n = numutils::pg_ltoa(v.dim(), &mut int_scratch);
    mcx::vec_append_bytes(&mut out, &int_scratch[..n])?;
    mcx::vec_append_bytes(&mut out, b"\0")?;
    Ok(cstring_result(out))
}

pub fn fc_sparsevec_typmod_in(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
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
            PgError::error("dimensions for type sparsevec must be at least 1")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .into(),
        );
    }
    if tl[0] > SPARSEVEC_MAX_DIM {
        return Err(PgError::error(format!(
            "dimensions for type sparsevec cannot exceed {SPARSEVEC_MAX_DIM}"
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .into());
    }
    Ok(Datum::from_i32(tl[0]))
}

pub fn fc_sparsevec_recv(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let typmod = fcinfo.arg_i32(2);
    let dim = pqformat::pq_getmsgint(buf, 4)? as i32;
    let nnz = pqformat::pq_getmsgint(buf, 4)? as i32;
    let unused = pqformat::pq_getmsgint(buf, 4)? as i32;

    check_dim(dim)?;
    check_nnz(nnz, dim)?;
    check_expected_dim(typmod, dim)?;

    if unused != 0 {
        return Err(PgError::error(format!("expected unused to be 0, not {unused}"))
            .with_sqlstate(ERRCODE_DATA_EXCEPTION)
            .into());
    }

    let mut b = SparseVecBuilder::new(fcinfo.result_mcx(), dim, nnz as usize)?;

    // Binary representation uses zero-based numbering for indices. C checks
    // each index immediately after reading it (CheckIndex(indices, i, dim));
    // re-running check_index over the growing prefix each time reproduces
    // the same incremental bounds/order/duplicate checks and error at the
    // same element.
    let mut indices: Vec<i32> = Vec::with_capacity(nnz as usize);
    for _ in 0..nnz {
        let idx = pqformat::pq_getmsgint(buf, 4)? as i32;
        indices.push(idx);
        check_index(indices.iter().copied(), dim)?;
    }

    for i in 0..nnz as usize {
        let val = pqformat::pq_getmsgfloat4(buf)?;
        check_element(val)?;
        if val == 0.0 {
            return Err(PgError::error(
                "binary representation of sparsevec cannot contain zero values",
            )
            .with_sqlstate(ERRCODE_DATA_EXCEPTION)
            .into());
        }
        b.set(i, indices[i], val);
    }

    Ok(image_datum(b.image()))
}

pub fn fc_sparsevec_send(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 sparsevec.
    let v = unsafe { arg_sparsevec(fcinfo, 0)? };
    let mut buf = pqformat::pq_begintypsend(fcinfo.result_mcx())?;
    pqformat::pq_sendint(&mut buf, v.dim() as u32, 4)?;
    pqformat::pq_sendint(&mut buf, v.nnz() as u32, 4)?;
    pqformat::pq_sendint(&mut buf, 0, 4)?;

    // Binary representation uses zero-based numbering for indices.
    for i in 0..v.nnz() {
        pqformat::pq_sendint(&mut buf, v.index(i) as u32, 4)?;
    }
    for i in 0..v.nnz() {
        pqformat::pq_sendfloat4(&mut buf, v.value(i))?;
    }
    Ok(varlena_result(pqformat::pq_endtypsend(buf)))
}

// C: sparsevec (sparsevec.c ~581-591) — convert sparsevec to sparsevec, used
// to check the type modifier.
pub fn fc_sparsevec(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 sparsevec, arg1 typmod.
    let v = unsafe { arg_sparsevec(fcinfo, 0)? };
    let typmod = fcinfo.arg_i32(1);
    check_expected_dim(typmod, v.dim())?;
    Ok(fcinfo.arg(0))
}

// C: vector_to_sparsevec (sparsevec.c ~596-638). CheckDim/CheckExpectedDim
// here are sparsevec.c's own (this function is defined in sparsevec.c), not
// vector.c's — matches the already-imported crate::sparse checks. C does not
// call CheckElement on the source values: a stored vector never contains
// NaN/Inf, so there is nothing to recheck.
pub fn fc_vector_to_sparsevec(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 vector, arg1 typmod.
    let v = unsafe { arg_vector(fcinfo, 0)? };
    let typmod = fcinfo.arg_i32(1);
    let dim = v.dim();

    check_dim(dim as i32)?;
    check_expected_dim(typmod, dim as i32)?;

    let mut nnz = 0usize;
    for i in 0..dim {
        if v.x(i) != 0.0 {
            nnz += 1;
        }
    }

    let mut b = SparseVecBuilder::new(fcinfo.result_mcx(), dim as i32, nnz)?;
    let mut j = 0usize;
    for i in 0..dim {
        let x = v.x(i);
        if x != 0.0 {
            b.set(j, i as i32, x);
            j += 1;
        }
    }
    Ok(image_datum(b.image()))
}

// C: array_to_sparsevec (sparsevec.c ~679-806). Array handling mirrors
// funcs::fc_array_to_vector (1-D check, null check, numeric vs. builtin
// deconstruct). CheckDim/CheckExpectedDim are sparsevec.c's own. Unlike
// fc_array_to_vector (which calls vec::check_element once per element right
// after conversion), C's array_to_sparsevec only runs CheckElement over the
// *collected non-zero* values, in ascending original-array-index order —
// same order we fill `b`, so checking each value at fill time (rather than
// in a separate pass afterward) reproduces the same error at the same
// element.
pub fn fc_array_to_sparsevec(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
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
    check_dim(n as i32)?;
    check_expected_dim(typmod, n as i32)?;

    // Convert every element to f32 up front. C runs this same per-type
    // conversion twice (once to count nnz, once to fill); since the
    // conversion is pure (same value or same error for the same input
    // element every time), converting once and reusing the result is
    // observably identical, including the position of a numeric_float4
    // conversion error relative to CheckDim/CheckExpectedDim.
    let mut values: Vec<f32> = Vec::with_capacity(n);
    match elemtype {
        INT4OID => {
            for d in elems.iter() {
                values.push(d.as_i32() as f32);
            }
        }
        FLOAT8OID => {
            for d in elems.iter() {
                values.push(d.as_f64() as f32);
            }
        }
        FLOAT4OID => {
            for d in elems.iter() {
                values.push(d.as_f32());
            }
        }
        NUMERICOID => {
            for d in elems.iter() {
                let p = d.as_usize() as *const u8;
                // SAFETY: non-null numeric element datum inside the array image.
                let payload = unsafe {
                    let total = varatt::varsize_any(p);
                    let hdr = if varatt::varatt_is_1b(p) { 1 } else { 4 };
                    core::slice::from_raw_parts(p.add(hdr), total - hdr)
                };
                values.push(adt_numeric::ops::numeric_float4(
                    adt_numeric::Num::from_payload(payload),
                )?);
            }
        }
        _ => {
            return Err(PgError::error("unsupported array type")
                .with_sqlstate(ERRCODE_DATA_EXCEPTION)
                .into())
        }
    }

    let nnz = values.iter().filter(|&&v| v != 0.0).count();
    let mut b = SparseVecBuilder::new(mcx, n as i32, nnz)?;
    let mut j = 0usize;
    for (i, &v) in values.iter().enumerate() {
        if v != 0.0 {
            check_element(v)?;
            b.set(j, i as i32, v);
            j += 1;
        }
    }
    Ok(image_datum(b.image()))
}

// C: sparsevec_to_vector (vector.c ~1308-1326). Defined in vector.c, so
// CheckDim/CheckExpectedDim here are vector.c's own (VECTOR_MAX_DIM-based),
// not sparsevec.c's — hence the fully-qualified crate::vec paths rather than
// the crate::sparse names already imported above.
pub fn fc_sparsevec_to_vector(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg0 sparsevec, arg1 typmod.
    let v = unsafe { arg_sparsevec(fcinfo, 0)? };
    let typmod = fcinfo.arg_i32(1);
    let dim = v.dim();

    crate::vec::check_dim(dim as usize)?;
    crate::vec::check_expected_dim(typmod, dim as usize)?;

    let mut b = VecBuilder::new(fcinfo.result_mcx(), dim as usize)?;
    for i in 0..v.nnz() {
        b.set(v.index(i) as usize, v.value(i));
    }
    Ok(image_datum(b.image()))
}
