//! tablespace.c: TablespaceCreateDbspace exists-fastpath + get_tablespace_oid.

#![allow(non_snake_case)]

use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{AttrNumber, InvalidOid, Oid, NAMEDATALEN};
use ::types_error::{PgError, PgResult, ERROR};

pub const TableSpaceRelationId: Oid = 1213;

pub fn TablespaceCreateDbspace(spc_oid: Oid, db_oid: Oid, _is_redo: bool) -> PgResult<()> {
    let ctx = MemoryContext::new("TablespaceCreateDbspace");
    let dir = relpath::GetDatabasePath(ctx.mcx(), db_oid, spc_oid)?;
    if std::path::Path::new(dir.as_str()).is_dir() {
        return Ok(());
    }
    panic!(
        "unported callee reached from tablespace.c: TablespaceCreateDbspace \
         directory-creation arm ({})",
        dir.as_str()
    );
}

const Anum_pg_tablespace_oid: usize = 1;
const Anum_pg_tablespace_spcname: usize = 2;

// get_tablespace_oid (tablespace.c): C seq-scans pg_tablespace with a
// spcname key.
pub fn get_tablespace_oid(mcx: Mcx<'_>, tablespacename: &str, missing_ok: bool) -> PgResult<Oid> {
    let rel = table::table_open(mcx, TableSpaceRelationId, types_rel::AccessShareLock)?;
    let n = NAMEDATALEN as usize;
    let mut name_buf: PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    let take = tablespacename.len().min(n - 1);
    mcx::vec_append_bytes(&mut name_buf, &tablespacename.as_bytes()[..take])?;
    mcx::vec_append_bytes(&mut name_buf, &[0u8; 64][..n - take])?;
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = Anum_pg_tablespace_spcname as AttrNumber;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_NAMEEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_NAMEEQ) failed: {e:?}"));
    key.sk_argument = datum::Datum::from_usize(name_buf.as_ptr() as usize);
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[key])?;
    let result = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => {
            let mut isnull = false;
            // SAFETY: oid is a fixed NOT NULL pg_tablespace column.
            unsafe {
                types_tuple::heap_getattr(
                    tup,
                    Anum_pg_tablespace_oid as i32,
                    rel.descr(),
                    &mut isnull,
                )
            }
            .as_oid()
        }
        None => InvalidOid,
    };
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    if result == InvalidOid && !missing_ok {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("tablespace \"{tablespacename}\" does not exist"),
            )
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    Ok(result)
}

pub fn init_seams() {
    tablespace_seams::tablespace_create_dbspace::set(TablespaceCreateDbspace);
}
