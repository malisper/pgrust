pub mod simple;
pub mod synonym;
pub mod thesaurus;
#[cfg(test)]
mod tests;

use ::datum::Datum;
use ::mcx::{alloc_in, vec_with_capacity_in, Mcx};
use ::ts_cache::lookup_ts_dictionary_cache;
use ::ts_locale::dict_api::{lexize_result_ref, DictInitData, LexizeResult};
use ::ts_locale::DictSubState;
use ::types_core::TEXTOID;
use ::types_error::{PgError, PgResult};
use ::types_fmgr::{byref_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

fn text_image_datum<'m>(mcx: Mcx<'m>, s: &[u8]) -> PgResult<Datum> {
    let mut v = vec_with_capacity_in(mcx, s.len() + 4)?;
    v.extend_from_slice(&((((s.len() + 4) as u32) << 2).to_ne_bytes()));
    v.extend_from_slice(s);
    let d = Datum::from_usize(v.as_ptr() as usize);
    core::mem::forget(v);
    Ok(d)
}

// ts_lexize (dict.c).
pub fn fc_ts_lexize(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let dict_id = fcinfo.arg_oid(0);
    // SAFETY: catalog arg 1 is a non-null text.
    let input = unsafe { fcinfo.arg_varlena_packed(1)? };
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let token = input.data();

    let dict = lookup_ts_dictionary_cache(dict_id)?;
    let mut dstate =
        DictSubState { isend: false, getnext: false, private_state: core::ptr::null_mut() };
    let mut res_word = dict.call_lexize(mcx, token, Some(&mut dstate))?;
    if dstate.getnext {
        dstate.isend = true;
        let ptr = dict.call_lexize(mcx, token, Some(&mut dstate))?;
        if ptr != 0 {
            res_word = ptr;
        }
    }

    // SAFETY: result words live in `mcx`.
    let Some(LexizeResult(res)) = (unsafe { lexize_result_ref(res_word) }) else {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    };

    let img = if res.is_empty() {
        ::datum::array_build::construct_empty_array_image(mcx, TEXTOID)?
    } else {
        let mut da = vec_with_capacity_in(mcx, res.len())?;
        for lex in res.iter() {
            da.push(text_image_datum(mcx, &lex.lexeme)?);
        }
        ::datum::array_build::construct_array_image(mcx, &da, TEXTOID, -1, false, b'i')?
    };
    byref_result(mcx, &img)
}

fn arg_dict_ptr(fcinfo: &Fcinfo) -> usize {
    fcinfo.arg(0).as_usize()
}

fn arg_token<'a>(fcinfo: &'a Fcinfo) -> &'a [u8] {
    let len = fcinfo.arg(2).as_i32().max(0) as usize;
    // SAFETY: dict_api lexize convention — arg1 points at `len` live bytes.
    unsafe { core::slice::from_raw_parts(fcinfo.arg(1).as_usize() as *const u8, len) }
}

fn lexize_datum<'m>(mcx: Mcx<'m>, res: Option<LexizeResult<'m>>) -> PgResult<Datum> {
    match res {
        None => Ok(Datum::from_usize(0)),
        Some(r) => {
            let (ptr, _) = ::mcx::PgBox::into_raw_with_allocator(alloc_in(mcx, r)?);
            Ok(Datum::from_usize(ptr as usize))
        }
    }
}

pub fn fc_dsimple_init(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg0 is the DictInitData built by the ts_cache dictionary loader.
    let init = unsafe { &*(arg_dict_ptr(fcinfo) as *const DictInitData<'static>) };
    let d = simple::dsimple_init(init)?;
    {
        let (ptr, _) = ::mcx::PgBox::into_raw_with_allocator(alloc_in(init.mcx, d)?);
        Ok(Datum::from_usize(ptr as usize))
    }
}

pub fn fc_dsimple_lexize(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg0 came from fc_dsimple_init and outlives the cache entry.
    let d = unsafe { &*(arg_dict_ptr(fcinfo) as *const simple::DictSimple) };
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    lexize_datum(mcx, simple::dsimple_lexize(mcx, d, arg_token(fcinfo))?)
}

pub fn fc_dsynonym_init(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg0 is the DictInitData built by the ts_cache dictionary loader.
    let init = unsafe { &*(arg_dict_ptr(fcinfo) as *const DictInitData<'static>) };
    let d = synonym::dsynonym_init(init)?;
    {
        let (ptr, _) = ::mcx::PgBox::into_raw_with_allocator(alloc_in(init.mcx, d)?);
        Ok(Datum::from_usize(ptr as usize))
    }
}

pub fn fc_dsynonym_lexize(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg0 came from fc_dsynonym_init and outlives the cache entry.
    let d = unsafe { &*(arg_dict_ptr(fcinfo) as *const synonym::DictSyn) };
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    lexize_datum(mcx, synonym::dsynonym_lexize(mcx, d, arg_token(fcinfo))?)
}

pub fn fc_thesaurus_init(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg0 is the DictInitData built by the ts_cache dictionary loader.
    let init = unsafe { &*(arg_dict_ptr(fcinfo) as *const DictInitData<'static>) };
    let d = thesaurus::thesaurus_init(init)?;
    {
        let (ptr, _) = ::mcx::PgBox::into_raw_with_allocator(alloc_in(init.mcx, d)?);
        Ok(Datum::from_usize(ptr as usize))
    }
}

pub fn fc_thesaurus_lexize(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let dstate_word = fcinfo.arg(3).as_usize();
    if fcinfo.nargs != 4 || dstate_word == 0 {
        return Err(PgError::error("forbidden call of thesaurus or nested call").into());
    }
    // SAFETY: arg0 came from fc_thesaurus_init; arg3 is the caller's live
    // DictSubState; single-threaded backend.
    let d = unsafe { &mut *(arg_dict_ptr(fcinfo) as *mut thesaurus::DictThesaurus) };
    let dstate = unsafe { &mut *(dstate_word as *mut DictSubState) };
    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let res = thesaurus::thesaurus_lexize(mcx, d, arg_token(fcinfo), dstate)?;
    lexize_datum(mcx, res.map(LexizeResult))
}

pub mod builtins {
    use super::*;

    const fn b(
        foid: ::types_core::Oid,
        name: &'static str,
        nargs: i16,
        func: ::types_fmgr::PGFunction,
    ) -> FmgrBuiltin {
        FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
    }

    pub const DICT_BUILTINS: &[FmgrBuiltin] = &[
        b(3723, "ts_lexize", 2, fc_ts_lexize),
        b(3725, "dsimple_init", 1, fc_dsimple_init),
        b(3726, "dsimple_lexize", 4, fc_dsimple_lexize),
        b(3728, "dsynonym_init", 1, fc_dsynonym_init),
        b(3729, "dsynonym_lexize", 4, fc_dsynonym_lexize),
        b(3740, "thesaurus_init", 1, fc_thesaurus_init),
        b(3741, "thesaurus_lexize", 4, fc_thesaurus_lexize),
    ];
}
