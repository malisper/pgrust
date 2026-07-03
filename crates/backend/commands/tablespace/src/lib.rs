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

pub fn init_seams() {
    tablespace_seams::tablespace_create_dbspace::set(TablespaceCreateDbspace);
}
