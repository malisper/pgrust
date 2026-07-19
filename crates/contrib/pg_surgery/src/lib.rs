//! Port of `contrib/pg_surgery` — heap_force_kill / heap_force_freeze.

use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

const LIBRARY: &str = "pg_surgery";

fn fc_heap_force_kill(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<datum::Datum> {
    panic!("pg_surgery: heap_force_kill not yet implemented in this lane increment");
}

fn fc_heap_force_freeze(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<datum::Datum> {
    panic!("pg_surgery: heap_force_freeze not yet implemented in this lane increment");
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "heap_force_kill" => fc_heap_force_kill,
        "heap_force_freeze" => fc_heap_force_freeze,
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
