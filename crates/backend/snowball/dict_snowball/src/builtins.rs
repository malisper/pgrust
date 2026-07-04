use ::datum::Datum;
use ::mcx::alloc_in;
use ::ts_locale::dict_api::DictInitData;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

use crate::dict::{dsnowball_init, dsnowball_lexize, DictSnowball};

pub fn fc_dsnowball_init(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: arg 0 is the DictInitData the ts_cache dictionary loader built
    // for this call (dict_api convention).
    let init = unsafe { &*(fcinfo.arg(0).as_usize() as *const DictInitData<'static>) };
    let d = dsnowball_init(init)?;
    let (ptr, _) = ::mcx::PgBox::into_raw_with_allocator(alloc_in(init.mcx, d)?);
    Ok(Datum::from_usize(ptr as usize))
}

pub fn fc_dsnowball_lexize(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: args follow the dict_api lexize convention; the dict pointer
    // came from fc_dsnowball_init and outlives the cache entry.
    let d = unsafe { &*(fcinfo.arg(0).as_usize() as *const DictSnowball) };
    let len = fcinfo.arg(2).as_i32().max(0) as usize;
    let token = unsafe { core::slice::from_raw_parts(fcinfo.arg(1).as_usize() as *const u8, len) };
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let res = dsnowball_lexize(mcx, d, token)?;
    let (ptr, _) = ::mcx::PgBox::into_raw_with_allocator(alloc_in(mcx, res)?);
    Ok(Datum::from_usize(ptr as usize))
}

// dsnowball_init/dsnowball_lexize come from snowball_create.sql at initdb
// (prolang c, '$libdir/dict_snowball', unpinned OIDs) — no fixed-OID rows for
// the CANONICAL-checked table. fmgr's C-language leg resolves them by
// (probin library, prosrc symbol) from SNOWBALL_CLANG.
pub const SNOWBALL_BUILTINS: &[FmgrBuiltin] = &[];

pub const SNOWBALL_LIBRARY: &str = "dict_snowball";

pub const SNOWBALL_CLANG: &[(&str, i16, PGFunction)] = &[
    ("dsnowball_init", 1, fc_dsnowball_init),
    ("dsnowball_lexize", 4, fc_dsnowball_lexize),
];
