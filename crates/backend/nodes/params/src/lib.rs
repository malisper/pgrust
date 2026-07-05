//! params.c residue over the types_portal registry slices. The registry
//! carries no paramFetch/paramCompile hooks, so C's per-param hook probes are
//! structurally absent; paramValuesStr lives with the future error-context
//! consumer (ParamsErrorCallback unported until then).

use core::fmt::Write;

use datum::Datum;
use mcx::{vec_append_bytes, vec_with_capacity_in, Mcx, PgString, PgVec};
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::PgResult;
use types_portal::params::ParamExternData;

#[cfg(test)]
mod tests;

fn typlenbyval(ptype: Oid) -> PgResult<(i16, bool)> {
    if OidIsValid(ptype) {
        lsyscache::get_typlenbyval(ptype)
    } else {
        // No type OID: assume by-value, like copyParamList does.
        Ok((core::mem::size_of::<Datum>() as i16, true))
    }
}

/// `copyParamList`: static self-contained copy; by-ref datum values land in `mcx`.
pub fn copy_param_list<'mcx>(
    mcx: Mcx<'mcx>,
    from: &[ParamExternData],
) -> PgResult<PgVec<'mcx, ParamExternData>> {
    let mut out = vec_with_capacity_in(mcx, from.len())?;
    for oprm in from {
        let mut nprm = *oprm;
        if !nprm.isnull && OidIsValid(nprm.ptype) {
            let (typlen, typbyval) = lsyscache::get_typlenbyval(nprm.ptype)?;
            nprm.value = adt_scalar::datum_copy(mcx, nprm.value, typbyval, typlen)?;
        }
        out.push(nprm);
    }
    Ok(out)
}

/// `EstimateParamListSpace`.
pub fn estimate_param_list_space(params: &[ParamExternData]) -> PgResult<usize> {
    let mut sz = core::mem::size_of::<i32>();
    for prm in params {
        sz += core::mem::size_of::<Oid>() + core::mem::size_of::<u16>();
        let (typlen, typbyval) = typlenbyval(prm.ptype)?;
        sz += adt_scalar::datum_estimate_space(prm.value, prm.isnull, typbyval, typlen)?;
    }
    Ok(sz)
}

/// `SerializeParamList`: nparams, then per param type OID + pflags + datum,
/// byte layout C-exact.
pub fn serialize_param_list<'mcx>(
    params: &[ParamExternData],
    out: &mut PgVec<'mcx, u8>,
) -> PgResult<()> {
    vec_append_bytes(out, &(params.len() as i32).to_ne_bytes())?;
    for prm in params {
        vec_append_bytes(out, &prm.ptype.to_ne_bytes())?;
        vec_append_bytes(out, &prm.pflags.to_ne_bytes())?;
        let (typlen, typbyval) = typlenbyval(prm.ptype)?;
        adt_scalar::datum_serialize(prm.value, prm.isnull, typbyval, typlen, out)?;
    }
    Ok(())
}

/// `RestoreParamList`; by-ref datum payloads land in `mcx`.
pub fn restore_param_list<'mcx>(
    mcx: Mcx<'mcx>,
    cursor: &mut &[u8],
) -> PgResult<PgVec<'mcx, ParamExternData>> {
    let nparams =
        i32::from_ne_bytes(cursor[..4].try_into().expect("restore_param_list: short header"));
    *cursor = &cursor[4..];
    let mut out = vec_with_capacity_in(mcx, nparams.max(0) as usize)?;
    for _ in 0..nparams {
        let ptype = Oid::from_ne_bytes(cursor[..4].try_into().expect("short ptype"));
        *cursor = &cursor[4..];
        let pflags = u16::from_ne_bytes(cursor[..2].try_into().expect("short pflags"));
        *cursor = &cursor[2..];
        let (value, isnull) = adt_scalar::datum_restore(mcx, cursor)?;
        out.push(ParamExternData { value, isnull, pflags, ptype });
    }
    Ok(out)
}

/// `BuildParamLogString`; `None` mirrors C's NULL (aborted transaction — the
/// registry's hookless case never bails).
pub fn build_param_log_string<'mcx>(
    mcx: Mcx<'mcx>,
    params: &[ParamExternData],
    known_text_values: Option<&[Option<&str>]>,
    maxlen: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    if xact::IsAbortedTransactionBlockState() {
        return Ok(None);
    }
    let mut buf = PgString::new_in(mcx);
    for (paramno, param) in params.iter().enumerate() {
        write!(buf, "{}${} = ", if paramno > 0 { ", " } else { "" }, paramno + 1)
            .expect("params: oom building log string");
        if param.isnull || !OidIsValid(param.ptype) {
            buf.try_push_str("NULL")?;
        } else if let Some(known) =
            known_text_values.and_then(|k| k.get(paramno).copied().flatten())
        {
            conv::append_string_info_string_quoted(&mut buf, known, maxlen)?;
        } else {
            let (typoutput, _typisvarlena) = lsyscache::getTypeOutputInfo(param.ptype)?;
            let mut finfo = fmgr_seams::fmgr_info::call(typoutput)?;
            let d = types_fmgr::function_call1_coll_in(&mut finfo, InvalidOid, mcx, param.value)?;
            // SAFETY: out functions return a NUL-terminated cstring datum,
            // consumed before finfo scratch dies.
            let s = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
            conv::append_string_info_string_quoted(&mut buf, s.to_str().expect("non-UTF-8 output"), maxlen)?;
        }
    }
    Ok(Some(buf))
}
