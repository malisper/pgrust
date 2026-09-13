use alloc::boxed::Box;
use alloc::format;

use ::datum::varlena::{set_varsize_4b, VARHDRSZ};
use ::datum::Datum;
use ::mcx::{vec_append_bytes, Mcx, PgVec};
use ::stringinfo::StringInfo;
use ::types_core::{primitive::InvalidOid, Oid};
use ::types_error::{PgError, PgResult};

use crate::fcinfo::{function_call1_coll_in, FmgrInfo, LocalFcinfo};
use crate::getarg::datum_varlena_packed;

// Binary-wire fmgr frame; extends the by-ref result convention
// (notes/fc-wire-convention.md).

#[track_caller]
#[cold]
#[inline(never)]
fn receive_returned_non_null(fn_oid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "receive function {fn_oid} returned non-NULL"
    )))
}

#[track_caller]
#[cold]
#[inline(never)]
fn receive_returned_null(fn_oid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "receive function {fn_oid} returned NULL"
    )))
}

/// C `ReceiveFunctionCall`; `buf: None` reads a NULL.
pub fn receive_function_call(
    flinfo: &mut FmgrInfo,
    buf: Option<&mut StringInfo<'_>>,
    typioparam: Oid,
    typmod: i32,
    mcx: Mcx<'_>,
) -> PgResult<Datum> {
    let buf_is_null = buf.is_none();
    if buf_is_null && flinfo.fn_strict {
        return Ok(Datum::null());
    }

    let mut fcinfo = LocalFcinfo::<3>::fresh(InvalidOid);
    // SAFETY: `mcx` outlives this stack frame's single call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    // isnull stays false on all args even for a NULL buf, as in C.
    fcinfo.set_arg(
        0,
        match buf {
            Some(b) => Datum::from_usize(core::ptr::from_mut(b) as usize),
            None => Datum::null(),
        },
    );
    fcinfo.set_arg(1, Datum::from_oid(typioparam));
    fcinfo.set_arg(2, Datum::from_i32(typmod));

    let result = flinfo.invoke(&mut fcinfo)?;
    if buf_is_null {
        if !fcinfo.isnull {
            return Err(receive_returned_non_null(flinfo.fn_oid));
        }
    } else if fcinfo.isnull {
        return Err(receive_returned_null(flinfo.fn_oid));
    }
    Ok(result)
}

/// C `SendFunctionCall`: `DatumGetByteaP` over the result, so a toasted or
/// short-header bytea (a SQL-language send function returning a stored
/// column) comes back as a 4B-uncompressed image in `mcx`.
pub fn send_function_call(flinfo: &mut FmgrInfo, val: Datum, mcx: Mcx<'_>) -> PgResult<Datum> {
    let out = function_call1_coll_in(flinfo, InvalidOid, mcx, val)?;
    // SAFETY: a send function's result is a live non-null bytea datum.
    let packed = unsafe { datum_varlena_packed(out, mcx)? };
    if !packed.is_short() {
        return Ok(Datum::from_usize(packed.as_ptr() as usize));
    }
    unpack_short(packed.data(), mcx)
}

#[cold]
#[inline(never)]
fn unpack_short(data: &[u8], mcx: Mcx<'_>) -> PgResult<Datum> {
    let mut image = PgVec::new_in(mcx);
    vec_append_bytes(&mut image, &set_varsize_4b(data.len() + VARHDRSZ))?;
    vec_append_bytes(&mut image, data)?;
    Ok(Datum::from_usize(image.leak().as_ptr() as usize))
}
