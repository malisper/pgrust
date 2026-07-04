use ::datum::Datum;
use ::mcx::alloc_in;
use ::ts_locale::dict_api::DictInitData;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

use crate::dict_ispell::{dispell_init, dispell_lexize, DictISpell};

pub fn fc_dispell_init(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 is the DictInitData built by the dictionary loader
    // (dict_api convention).
    let init = unsafe { &*(fcinfo.arg(0).as_usize() as *const DictInitData<'static>) };
    let d = dispell_init(init)?;
    let (ptr, _) = ::mcx::PgBox::into_raw_with_allocator(alloc_in(init.mcx, d)?);
    Ok(Datum::from_usize(ptr as usize))
}

pub fn fc_dispell_lexize(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: dict_api lexize convention; the dict pointer came from
    // fc_dispell_init and lives in the dictionary cache context.
    let d = unsafe { &*(fcinfo.arg(0).as_usize() as *const DictISpell) };
    let len = fcinfo.arg(2).as_i32().max(0) as usize;
    let token = unsafe { core::slice::from_raw_parts(fcinfo.arg(1).as_usize() as *const u8, len) };
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    match dispell_lexize(mcx, d, token)? {
        Some(res) => {
            let (ptr, _) = ::mcx::PgBox::into_raw_with_allocator(alloc_in(mcx, res)?);
            Ok(Datum::from_usize(ptr as usize))
        }
        None => Ok(Datum::from_usize(0)),
    }
}

const fn b(foid: ::types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const SPELL_BUILTINS: &[FmgrBuiltin] = &[
    b(3731, "dispell_init", 1, fc_dispell_init),
    b(3732, "dispell_lexize", 4, fc_dispell_lexize),
];
