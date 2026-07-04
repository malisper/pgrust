use datum::Datum;
use types_error::PgResult;
use types_fmgr::{byref_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

// C returns namein(DatabaseEncoding->name): a NAMEDATALEN block.
pub fn fc_getdatabaseencoding(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let name = crate::GetDatabaseEncodingName();
    let mut buf = [0u8; 64];
    buf[..name.len()].copy_from_slice(name.as_bytes());
    byref_result(fcinfo.result_mcx(), &buf)
}

// pg_proc.dat rows (all proisstrict, none retset), OID-ascending.
pub const MBUTILS_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: 1039,
    name: "getdatabaseencoding",
    nargs: 0,
    strict: true,
    retset: false,
    func: fc_getdatabaseencoding,
}];
