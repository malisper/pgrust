use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    // get_database_name (dbcommands.c). Owned String: every varsup caller
    // feeds a #[cold] wraparound ereport, never a datum path.
    pub fn get_database_name(dbid: Oid) -> PgResult<Option<String>>
);
