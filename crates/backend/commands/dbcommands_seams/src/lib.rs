use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    // get_database_name (dbcommands.c). Owned String: every varsup caller
    // feeds a #[cold] wraparound ereport, never a datum path.
    pub fn get_database_name(dbid: Oid) -> PgResult<Option<String>>
);

seam_core::seam!(
    // get_database_oid (dbcommands.c).
    pub fn get_database_oid<'a, 'mcx>(
        mcx: mcx::Mcx<'mcx>,
        dbname: &'a str,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    // dbase_redo (dbcommands.c) — the Database rmgr rm_redo callback; rmgr's
    // table row delegates here (a direct rmgr -> dbcommands dep would cycle
    // through checkpointer/transam_xlog).
    pub fn dbase_redo(record: &mut xlogreader_seams::XLogReaderState) -> PgResult<()>
);
