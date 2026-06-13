//! Seam declarations for the `backend-foreign-foreign` unit
//! (`foreign/foreign.c`) plus the `pg_foreign_*` catalog DML that
//! `commands/foreigncmds.c` issues against these catalogs.
//!
//! `foreign.c` owns the read accessors over `pg_foreign_data_wrapper`,
//! `pg_foreign_server`, and `pg_user_mapping` (`GetForeignDataWrapper`,
//! `GetForeignServerByName`, `get_foreign_server_oid`, ...). The catalog-row
//! inserts/updates/syscache-lookups foreigncmds performs (`heap_form_tuple` +
//! `CatalogTupleInsert`/`Update`, `SearchSysCacheCopy1`, `GetNewOidWithIndex`,
//! `SysCacheGetAttr` decode) collapse, in the owned tree, into one by-value
//! seam per catalog row operation: the C `Datum`/`HeapTuple`/`values[]`/
//! `nulls[]`/`repl_*[]` plumbing belongs to this catalog-access layer, not to
//! the command driver. The FDW options validator call (`OidFunctionCall2`)
//! and the IMPORT FOREIGN SCHEMA FDW-callback / parse-execute machinery are
//! here too.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgString, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_foreigncmds::{
    DefElem, FdwOwnerRow, FdwUpdateRow, ForeignDataWrapper, ForeignServer, ImportForeignSchemaStmt,
    ServerOwnerRow, ServerUpdateRow,
};

/* ---- read accessors (foreign/foreign.c) ---- */

seam_core::seam!(
    /// `GetForeignDataWrapper(fdwid)` (foreign.c): the FDW descriptor by OID.
    /// `elog(ERROR)` on cache lookup failure, carried on `Err`. Allocated in
    /// `mcx`.
    pub fn get_foreign_data_wrapper<'mcx>(
        mcx: Mcx<'mcx>,
        fdwid: Oid,
    ) -> PgResult<ForeignDataWrapper<'mcx>>
);

seam_core::seam!(
    /// `GetForeignDataWrapperByName(fdwname, missing_ok)` (foreign.c): the FDW
    /// descriptor by name. With `missing_ok = false` a missing FDW raises
    /// (`Err`); with `missing_ok = true` it is `Ok(None)`. Allocated in `mcx`.
    pub fn get_foreign_data_wrapper_by_name<'mcx>(
        mcx: Mcx<'mcx>,
        fdwname: &str,
        missing_ok: bool,
    ) -> PgResult<Option<ForeignDataWrapper<'mcx>>>
);

seam_core::seam!(
    /// `GetForeignServerByName(srvname, missing_ok)` (foreign.c): the server
    /// descriptor by name. With `missing_ok = false` a missing server raises
    /// (`Err`); with `missing_ok = true` it is `Ok(None)`. Allocated in `mcx`.
    pub fn get_foreign_server_by_name<'mcx>(
        mcx: Mcx<'mcx>,
        srvname: &str,
        missing_ok: bool,
    ) -> PgResult<Option<ForeignServer<'mcx>>>
);

seam_core::seam!(
    /// `get_foreign_server_oid(servername, missing_ok)` (foreign.c): the
    /// server OID by name, or `InvalidOid` when absent and `missing_ok`.
    pub fn get_foreign_server_oid(servername: &str, missing_ok: bool) -> PgResult<Oid>
);

/* ---- FDW options validation + IMPORT (foreign.c / fdwapi.c) ---- */

seam_core::seam!(
    /// `OidFunctionCall2(fdwvalidator, optionsArray, catalogId)`: run the FDW
    /// options validator on the merged option list (C passes the `text[]`
    /// array; `None` options are passed as an empty array). The validator may
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn validate_options<'mcx>(
        fdwvalidator: Oid,
        options: &[DefElem<'mcx>],
        catalog_id: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `GetFdwRoutine(fdwhandler)`, the `fdw_routine->ImportForeignSchema ==
    /// NULL` guard (`ERRCODE_FDW_NO_SCHEMAS`), the FDW `ImportForeignSchema`
    /// callback, `pg_parse_query`, the IMPORT-table filter
    /// (`IsImportableForeignTable`, owned by foreign.c), and `ProcessUtility`
    /// of the returned `CREATE FOREIGN TABLE` commands (foreigncmds.c:1520-1604).
    /// `stmt` carries `list_type`/`table_list` for the filter and the
    /// schema-name rewrite target. Raises on any failure.
    pub fn import_foreign_schema_exec(
        stmt: &ImportForeignSchemaStmt<'_>,
        serverid: Oid,
        fdwhandler: Oid,
        fdwname: &str,
    ) -> PgResult<()>
);

/* ---- pg_foreign_data_wrapper catalog DML ---- */

seam_core::seam!(
    /// Insert a `pg_foreign_data_wrapper` row (`CreateForeignDataWrapper`'s
    /// `heap_form_tuple` + `CatalogTupleInsert`); returns the freshly-assigned
    /// OID (`GetNewOidWithIndex`).
    pub fn insert_fdw<'mcx>(
        fdwname: &str,
        owner: Oid,
        handler: Oid,
        validator: Oid,
        options: Option<&[DefElem<'mcx>]>,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// Apply the `AlterForeignDataWrapper` tuple update (only the `Some`
    /// columns are replaced; `options = Some(None)` stores SQL NULL).
    pub fn update_fdw<'mcx>(
        fdwid: Oid,
        handler: Option<Oid>,
        validator: Option<Oid>,
        options: Option<Option<&[DefElem<'mcx>]>>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterForeignDataWrapperOwner_internal`'s tuple update: set
    /// `fdwowner = new_owner` and `aclnewowner(fdwacl, old, new)` when the ACL
    /// is non-NULL, then `CatalogTupleUpdate`.
    pub fn fdw_set_owner(fdwid: Oid, old_owner: Oid, new_owner: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `SearchSysCacheCopy1(FOREIGNDATAWRAPPERNAME, name)` projected to
    /// `(fdwid, fdwvalidator)`; `None` when absent.
    pub fn fdw_lookup_by_name(fdwname: &str) -> PgResult<Option<FdwUpdateRow>>
);

seam_core::seam!(
    /// `SearchSysCacheCopy1(FOREIGNDATAWRAPPERNAME, name)` projected to
    /// `(fdwid, fdwname, fdwowner)`; `None` when absent. Allocated in `mcx`.
    pub fn fdw_owner_row_by_name<'mcx>(
        mcx: Mcx<'mcx>,
        fdwname: &str,
    ) -> PgResult<Option<FdwOwnerRow<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCacheCopy1(FOREIGNDATAWRAPPEROID, fdwid)` projected to
    /// `(fdwid, fdwname, fdwowner)`; `None` when absent. Allocated in `mcx`.
    pub fn fdw_owner_row_by_oid<'mcx>(
        mcx: Mcx<'mcx>,
        fdwid: Oid,
    ) -> PgResult<Option<FdwOwnerRow<'mcx>>>
);

seam_core::seam!(
    /// `SysCacheGetAttr(FOREIGNDATAWRAPPEROID, fdwid, fdwoptions)` decoded into
    /// a `DefElem` list (NULL → empty). Allocated in `mcx`.
    pub fn fdw_options<'mcx>(mcx: Mcx<'mcx>, fdwid: Oid) -> PgResult<PgVec<'mcx, DefElem<'mcx>>>
);

/* ---- pg_foreign_server catalog DML ---- */

seam_core::seam!(
    /// Insert a `pg_foreign_server` row (`CreateForeignServer`); returns the
    /// freshly-assigned OID.
    pub fn insert_server<'mcx>(
        servername: &str,
        owner: Oid,
        fdwid: Oid,
        servertype: Option<&str>,
        version: Option<&str>,
        options: Option<&[DefElem<'mcx>]>,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// Apply the `AlterForeignServer` tuple update (`version = Some(None)`
    /// stores SQL NULL).
    pub fn update_server<'mcx>(
        serverid: Oid,
        version: Option<Option<&str>>,
        options: Option<Option<&[DefElem<'mcx>]>>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AlterForeignServerOwner_internal`'s tuple update: set
    /// `srvowner = new_owner` and `aclnewowner(srvacl, old, new)` when the ACL
    /// is non-NULL, then `CatalogTupleUpdate`.
    pub fn server_set_owner(serverid: Oid, old_owner: Oid, new_owner: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `SearchSysCacheCopy1(FOREIGNSERVERNAME, name)` projected to
    /// `(serverid, srvfdw)`; `None` when absent.
    pub fn server_lookup_by_name(servername: &str) -> PgResult<Option<ServerUpdateRow>>
);

seam_core::seam!(
    /// `SearchSysCacheCopy1(FOREIGNSERVERNAME, name)` projected to
    /// `(serverid, srvname, srvowner, srvfdw)`; `None` when absent. Allocated
    /// in `mcx`.
    pub fn server_owner_row_by_name<'mcx>(
        mcx: Mcx<'mcx>,
        servername: &str,
    ) -> PgResult<Option<ServerOwnerRow<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCacheCopy1(FOREIGNSERVEROID, srvid)` projected to
    /// `(serverid, srvname, srvowner, srvfdw)`; `None` when absent. Allocated
    /// in `mcx`.
    pub fn server_owner_row_by_oid<'mcx>(
        mcx: Mcx<'mcx>,
        serverid: Oid,
    ) -> PgResult<Option<ServerOwnerRow<'mcx>>>
);

seam_core::seam!(
    /// `SysCacheGetAttr(FOREIGNSERVEROID, serverid, srvoptions)` decoded into a
    /// `DefElem` list (NULL → empty). Allocated in `mcx`.
    pub fn server_options<'mcx>(
        mcx: Mcx<'mcx>,
        serverid: Oid,
    ) -> PgResult<PgVec<'mcx, DefElem<'mcx>>>
);

/* ---- pg_user_mapping catalog DML ---- */

seam_core::seam!(
    /// `GetSysCacheOid2(USERMAPPINGUSERSERVER, useid, serverid)`: the mapping
    /// OID, or `InvalidOid` when absent.
    pub fn usermapping_oid(useid: Oid, serverid: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// Insert a `pg_user_mapping` row (`CreateUserMapping`); returns the
    /// freshly-assigned OID.
    pub fn insert_usermapping<'mcx>(
        useid: Oid,
        serverid: Oid,
        options: Option<&[DefElem<'mcx>]>,
    ) -> PgResult<Oid>
);

seam_core::seam!(
    /// Apply the `AlterUserMapping` `umoptions` tuple update.
    pub fn update_usermapping<'mcx>(
        umid: Oid,
        options: Option<Option<&[DefElem<'mcx>]>>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `SysCacheGetAttr(USERMAPPINGUSERSERVER, umid, umoptions)` decoded into a
    /// `DefElem` list (NULL → empty). Allocated in `mcx`.
    pub fn usermapping_options<'mcx>(
        mcx: Mcx<'mcx>,
        umid: Oid,
    ) -> PgResult<PgVec<'mcx, DefElem<'mcx>>>
);

seam_core::seam!(
    /// `MappingUserName(useid)` (foreign.h macro): `"public"` for
    /// `InvalidOid`, else `GetUserNameFromId(useid, false)`. Allocated in
    /// `mcx`.
    pub fn mapping_user_name<'mcx>(mcx: Mcx<'mcx>, useid: Oid) -> PgResult<PgString<'mcx>>
);

/* ---- pg_foreign_table catalog DML ---- */

seam_core::seam!(
    /// Insert a `pg_foreign_table` row (`CreateForeignTable`'s
    /// `heap_form_tuple` + `CatalogTupleInsert`).
    pub fn insert_foreign_table<'mcx>(
        relid: Oid,
        serverid: Oid,
        options: Option<&[DefElem<'mcx>]>,
    ) -> PgResult<()>
);
