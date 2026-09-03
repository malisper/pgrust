//! pgvector 0.8.5 halfvec.c fmgr entry points: I/O + typmod cast.
use datum::Datum;
use mcx::PgVec;
use types_error::{PgError, PgResult, ERRCODE_DATA_EXCEPTION, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{cstring_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::funcs::{detoasted_image, image_datum};
use crate::half::*;

// SAFETY contract of callers: arg i is a non-null halfvec varlena (strict fns).
unsafe fn arg_halfvec<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<HalfVecView<'a>> {
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    HalfVecView::from_payload(v.data())
}

// Not yet called from this file: kept for Task 5's halfvec distance/
// comparison functions, which take two halfvec args each.
#[allow(dead_code)]
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
