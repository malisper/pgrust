use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    // TablespaceCreateDbspace(spcOid, dbOid, isRedo) (tablespace.c).
    pub fn tablespace_create_dbspace(spc_oid: Oid, db_oid: Oid, is_redo: bool) -> PgResult<()>
);
