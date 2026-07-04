//! tablespace.c: TablespaceCreateDbspace exists-fastpath only.

#![allow(non_snake_case)]

use ::mcx::MemoryContext;
use ::types_core::Oid;
use ::types_error::PgResult;

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

// get_tablespace_oid (tablespace.c). C heap-scans pg_tablespace on the
// few-entries theory; indexOK=false mirrors that.
pub fn get_tablespace_oid<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    tablespacename: &str,
    missing_ok: bool,
) -> PgResult<Oid> {
    const TABLESPACE_RELATION_ID: Oid = 1213;
    const TABLESPACE_OID_INDEX_ID: Oid = 2697;
    const Anum_pg_tablespace_oid: i32 = 1;
    const Anum_pg_tablespace_spcname: types_core::AttrNumber = 2;
    let rel = table::table_open(mcx, TABLESPACE_RELATION_ID, types_rel::AccessShareLock)?;
    let n = types_core::NAMEDATALEN as usize;
    let mut namebuf: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, n)?;
    let take = tablespacename.len().min(n - 1);
    ::mcx::vec_append_bytes(&mut namebuf, &tablespacename.as_bytes()[..take])?;
    ::mcx::vec_append_bytes(&mut namebuf, &[0u8; 64][..n - take])?;
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = Anum_pg_tablespace_spcname;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_NAMEEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_NAMEEQ) failed: {e:?}"));
    key.sk_argument = datum::Datum::from_usize(namebuf.as_ptr() as usize);
    let keys = [key];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, TABLESPACE_OID_INDEX_ID, false, None, &keys)?;
    let mut isnull = false;
    let result = match genam::systable_getnext(mcx, &mut scan)? {
        // SAFETY: pg_tablespace oid is a fixed NOT NULL column under its descriptor.
        Some(tup) => unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_tablespace_oid, rel.descr(), &mut isnull)
        }
        .as_oid(),
        None => types_core::InvalidOid,
    };
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    if result == types_core::InvalidOid && !missing_ok {
        return Err(Box::new(
            types_error::PgError::new(
                types_error::ERROR,
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
