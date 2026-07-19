//! contrib/amcheck — relation integrity verification (bt_index_check,
//! bt_index_parent_check, verify_heapam, gin_index_check).

mod common;
mod gin;
mod heapam;
mod nbtree_check;

use datum::Datum;
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

const LIBRARY: &str = "amcheck";

fn void() -> Datum {
    Datum::from_usize(0)
}

fn fc_bt_index_check(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let indrelid = fcinfo.arg_oid(0);
    let n = fcinfo.nargs();
    let heapallindexed = n >= 2 && fcinfo.arg_bool(1);
    let checkunique = n >= 3 && fcinfo.arg_bool(2);

    nbtree_check::bt_index_check_internal(
        fcinfo.result_mcx(),
        indrelid,
        false,
        heapallindexed,
        false,
        checkunique,
    )?;
    Ok(void())
}

fn fc_bt_index_parent_check(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let indrelid = fcinfo.arg_oid(0);
    let n = fcinfo.nargs();
    let heapallindexed = n >= 2 && fcinfo.arg_bool(1);
    let rootdescend = n >= 3 && fcinfo.arg_bool(2);
    let checkunique = n >= 4 && fcinfo.arg_bool(3);

    nbtree_check::bt_index_check_internal(
        fcinfo.result_mcx(),
        indrelid,
        true,
        heapallindexed,
        rootdescend,
        checkunique,
    )?;
    Ok(void())
}

fn fc_verify_heapam(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    heapam::verify_heapam(flinfo, fcinfo)
}

fn fc_gin_index_check(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let indrelid = fcinfo.arg_oid(0);
    gin::gin_index_check_internal(fcinfo.result_mcx(), indrelid)?;
    Ok(void())
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "bt_index_check" => fc_bt_index_check,
        "bt_index_parent_check" => fc_bt_index_parent_check,
        "verify_heapam" => fc_verify_heapam,
        "gin_index_check" => fc_gin_index_check,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}
