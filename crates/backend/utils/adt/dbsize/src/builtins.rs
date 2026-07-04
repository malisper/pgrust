//! fmgr wrappers (`fc_*`) + `DBSIZE_BUILTINS` for fmgr-core.

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

pub fn fc_pg_size_bytes(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let a = unsafe { fcinfo.arg_varlena_packed(0)? };
    let s = String::from_utf8_lossy(a.data());
    Ok(Datum::from_i64(crate::pg_size_bytes(&s)?))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

// pg_relation_size(regclass, text). C stats the segment files
// (calculate_relation_size); one backend + full-page segments make
// smgrnblocks * BLCKSZ the same number without the fs walk.
pub fn fc_pg_relation_size(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let rel_oid = fcinfo.arg_oid(0);
    // SAFETY: catalog arg 1 is a non-null text varlena (strict fn).
    let forkname_b = unsafe { fcinfo.arg_varlena_packed(1)? };
    let forkname = String::from_utf8_lossy(forkname_b.data()).into_owned();
    let forknum = match forkname.as_str() {
        "main" => types_core::ForkNumber::MAIN_FORKNUM,
        "fsm" => types_core::ForkNumber::FSM_FORKNUM,
        "vm" => types_core::ForkNumber::VISIBILITYMAP_FORKNUM,
        "init" => types_core::ForkNumber::INIT_FORKNUM,
        other => {
            return Err(Box::new(
                ::types_error::PgError::error(format!("invalid fork name: \"{other}\""))
                    .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_hint("Valid fork names are \"main\", \"fsm\", \"vm\", and \"init\"."),
            ))
        }
    };
    let mcx = fcinfo.result_mcx();
    let Some(rel) =
        relation_seams::try_relation_open::call(mcx, rel_oid, types_rel::AccessShareLock)?
    else {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    };
    let key = ::types_storage::RelFileLocatorBackend {
        locator: rel.rd_locator.get(),
        backend: rel.rd_backend,
    };
    let size = if smgr_seams::smgr_exists::call(key, forknum)? {
        smgr_seams::smgr_nblocks::call(key, forknum)? as i64 * types_core::BLCKSZ as i64
    } else {
        0
    };
    rel.close(types_rel::AccessShareLock)?;
    Ok(Datum::from_i64(size))
}

pub const DBSIZE_BUILTINS: &[FmgrBuiltin] = &[
    b(3334, "pg_size_bytes", 1, fc_pg_size_bytes),
    b(2332, "pg_relation_size", 2, fc_pg_relation_size),
];
