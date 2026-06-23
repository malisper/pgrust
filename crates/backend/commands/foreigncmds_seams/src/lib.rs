//! Seam declarations for the `backend-commands-foreigncmds` unit
//! (`commands/foreigncmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use ::types_core::Oid;
use ::types_error::PgResult;

seam_core::seam!(
    /// `AlterForeignServerOwner_oid(srvId, newOwnerId)` (foreigncmds.c):
    /// change a foreign server's owner during REASSIGN OWNED. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn alter_foreign_server_owner_oid(srv_id: Oid, new_owner_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterForeignDataWrapperOwner_oid(fdwId, newOwnerId)` (foreigncmds.c):
    /// change a foreign-data wrapper's owner during REASSIGN OWNED. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn alter_foreign_data_wrapper_owner_oid(fdw_id: Oid, new_owner_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// The fdwvalidator OID for the foreign table `relid`'s server's FDW
    /// (the first half of `ATExecAlterColumnGenericOptions`, tablecmds.c:15955:
    /// `GetForeignServer(fttableform->ftserver)` then
    /// `GetForeignDataWrapper(server->fdwid)->fdwvalidator`). Errors with
    /// "foreign table \"...\" does not exist" when `relid` has no
    /// `pg_foreign_table` row. The relation name for the error is supplied by
    /// the caller (it already holds the open relation).
    pub fn foreign_table_fdwvalidator(relid: Oid, relname: &str) -> PgResult<Oid>
);

seam_core::seam!(
    /// `transformGenericOptions(catalogId, oldDatum, options, fdwvalidator)`
    /// (foreigncmds.c:65) — merge the SET/ADD/DROP `new_options` into the
    /// `old_options` (each item `(name, value)`; `value` `None` is a
    /// value-less option), run the `"="`-in-name rejection, and (when
    /// `fdwvalidator` is valid) call the FDW validator. The `new_options`
    /// carry the parser `DefElemAction` discriminant as `action` (0 UNSPEC /
    /// 1 SET / 2 ADD / 3 DROP); `old_options` are existing values (UNSPEC).
    /// Returns the merged option list (`(name, value)`); an empty result is
    /// the C `PointerGetDatum(NULL)` "no options" array. Can `ereport(ERROR)`
    /// (option not found / duplicate / validator), carried on `Err`.
    pub fn transform_generic_options(
        catalog_id: Oid,
        old_options: &[(String, Option<String>)],
        new_options: &[(String, Option<String>, i32)],
        fdwvalidator: Oid,
    ) -> PgResult<Vec<(String, Option<String>)>>
);
