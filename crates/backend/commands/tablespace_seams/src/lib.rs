use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    // TablespaceCreateDbspace(spcOid, dbOid, isRedo) (tablespace.c).
    pub fn tablespace_create_dbspace(spc_oid: Oid, db_oid: Oid, is_redo: bool) -> PgResult<()>
);

seam_core::seam!(
    // get_tablespace_oid (tablespace.c) — GRANT name resolution (a direct
    // aclchk -> commands_tablespace dep would cycle).
    pub fn get_tablespace_oid<'a, 'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tablespacename: &'a str,
        missing_ok: bool,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    // get_tablespace_name (tablespace.c) — dbsize's aclcheck_error name (a
    // direct dbsize -> commands_tablespace dep would cycle through fmgr).
    pub fn get_tablespace_name<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        spc_oid: Oid,
    ) -> PgResult<Option<types_tuple::NameData>>
);

seam_core::seam!(
    // PrepareTempTablespaces (tablespace.c) — fd's configured-list arm (a
    // direct fd -> commands_tablespace dep would cycle).
    pub fn prepare_temp_tablespaces() -> PgResult<()>
);

seam_core::seam!(
    // tblspc_redo (tablespace.c) — the Tablespace rmgr rm_redo callback;
    // rmgr's table row delegates here (a direct rmgr -> commands_tablespace
    // dep would cycle through checkpointer/transam_xlog).
    pub fn tblspc_redo(record: &mut xlogreader_seams::XLogReaderState) -> PgResult<()>
);
