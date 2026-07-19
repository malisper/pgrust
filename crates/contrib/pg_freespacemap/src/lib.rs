//! `contrib/pg_freespacemap/pg_freespacemap.c` — the `pg_freespace()` SQL
//! function over the FSM fork.

use datum::Datum;
use types_core::{BlockNumber, MaxBlockNumber};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_WRONG_OBJECT_TYPE,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_rel::pg_class::RELKIND_HAS_STORAGE;

const LIBRARY: &str = "pg_freespacemap";

fn fc_pg_freespace(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let relid = fcinfo.arg(0).as_oid();
    let blkno = fcinfo.arg(1).as_i64();

    let mcx = fcinfo.result_mcx();
    let rel = relation::relation_open(mcx, relid, types_rel::AccessShareLock)?;

    if !RELKIND_HAS_STORAGE(rel.rd_rel.relkind) {
        let detail = pg_class_seams::errdetail_relkind_not_supported::call(rel.rd_rel.relkind)?;
        return Err(Box::new(
            PgError::error(format!("relation \"{}\" does not have storage", rel.name()))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(detail),
        ));
    }

    if blkno < 0 || blkno > MaxBlockNumber as i64 {
        return Err(Box::new(
            PgError::error("invalid block number")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }

    let freespace = freespace::GetRecordedFreeSpace(&rel, blkno as BlockNumber)?;

    rel.close(types_rel::AccessShareLock)?;
    Ok(Datum::from_i16(freespace as i16))
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "pg_freespace" => fc_pg_freespace,
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
