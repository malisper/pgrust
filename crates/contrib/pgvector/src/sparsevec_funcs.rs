//! pgvector 0.8.5 sparsevec.c I/O fmgr entry points — mirrors funcs.rs's
//! vector counterparts. DIVERGENCES (recorded): none.
use datum::Datum;
use mcx::PgVec;
use types_error::{PgError, PgResult, ERRCODE_DATA_EXCEPTION, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{cstring_result, varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::funcs::{detoasted_image, image_datum};
use crate::sparse::{
    check_dim, check_element, check_expected_dim, check_index, check_nnz, parse_sparsevec,
    SparseInputElement, SparseVecBuilder, SparseVecView, SPARSEVEC_MAX_DIM,
};

// SAFETY contract of callers: arg i is a non-null sparsevec varlena (strict fns).
unsafe fn arg_sparsevec<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<SparseVecView<'a>> {
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    SparseVecView::from_payload(v.data())
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
