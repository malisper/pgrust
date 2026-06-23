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

#![allow(clippy::result_large_err)]

use ::mcx::{Mcx, PgString, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_foreigncmds::{
    DefElem, FdwOwnerRow, FdwUpdateRow, ForeignDataWrapper, ForeignServer, ImportForeignSchemaStmt,
    ImportRawStmt, RawStmtHandle, ServerOwnerRow, ServerUpdateRow,
};
use ::nodes::{
    AsyncRequestData, EStateData, FdwRoutine, ForeignScanState, ParallelContext,
    ParallelWorkerContext,
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
    /// `GetForeignDataWrapperExtended(fdwid, FDW_MISSING_OK ? : 0)->fdwname`
    /// (foreign.c) for the `getObjectDescription` FDW arm: the FDW's name, or
    /// `Ok(None)` when the row vanished and `missing_ok`. Allocated in `mcx`.
    pub fn foreign_data_wrapper_name<'mcx>(
        mcx: Mcx<'mcx>,
        fdwid: Oid,
        missing_ok: bool,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `GetForeignServerExtended(serverid, FSV_MISSING_OK ? : 0)->servername`
    /// (foreign.c) for the `getObjectDescription` server arm: the server's
    /// name, or `Ok(None)` when the row vanished and `missing_ok`. Allocated in
    /// `mcx`.
    pub fn foreign_server_name<'mcx>(
        mcx: Mcx<'mcx>,
        serverid: Oid,
        missing_ok: bool,
    ) -> PgResult<Option<PgString<'mcx>>>
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

seam_core::seam!(
    /// `get_foreign_data_wrapper_oid(fdwname, missing_ok)` (foreign.c): the
    /// FDW's OID. With `missing_ok = false` a missing wrapper raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`); with `missing_ok = true` it is
    /// `Ok(InvalidOid)`.
    pub fn get_foreign_data_wrapper_oid(fdwname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// The `relation_is_updatable` (rewriteHandler.c:2952) foreign-table leg:
    /// the bitmask of `CMD_*` events the foreign relation `relid` supports.
    ///
    /// ```c
    /// FdwRoutine *fdwroutine = GetFdwRoutineForRelation(rel, false);
    /// if (fdwroutine->IsForeignRelUpdatable != NULL)
    ///     events |= fdwroutine->IsForeignRelUpdatable(rel);
    /// else {
    ///     if (fdwroutine->ExecForeignInsert != NULL) events |= (1 << CMD_INSERT);
    ///     if (fdwroutine->ExecForeignUpdate != NULL) events |= (1 << CMD_UPDATE);
    ///     if (fdwroutine->ExecForeignDelete != NULL) events |= (1 << CMD_DELETE);
    /// }
    /// ```
    ///
    /// This computation is homed in the foreign owner because the repo's
    /// [`::nodes::FdwRoutine`] carrier is trimmed to the scan/parallel/async
    /// callback-presence flags and does NOT model `IsForeignRelUpdatable` /
    /// `ExecForeignInsert` / `ExecForeignUpdate` / `ExecForeignDelete` — so the
    /// rewriteHandler caller cannot read them off the routine. The owning unit
    /// (`backend-foreign-foreign`, which holds `GetFdwRoutineForRelation` and the
    /// full FDW routine) installs this when the modify-callback carrier lands;
    /// until then a call panics loudly (mirror-PG seam-and-panic).
    pub fn foreign_rel_updatable_events(relid: Oid) -> PgResult<i32>
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
    /// `GetFdwRoutine(fdwhandler)` then the FDW `ImportForeignSchema(stmt,
    /// serverid)` callback (fdwapi.c). Returns the FDW-generated command
    /// strings (the C `List *` of `char *`), or `None` when the routine's
    /// `ImportForeignSchema` field is NULL — the C `fdw_routine->
    /// ImportForeignSchema == NULL` test, whose `ERRCODE_FDW_NO_SCHEMAS`
    /// `ereport` the command driver raises in-crate. Allocated in `mcx`. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn fdw_import_foreign_schema<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &ImportForeignSchemaStmt<'_>,
        serverid: Oid,
        fdwhandler: Oid,
    ) -> PgResult<Option<PgVec<'mcx, PgString<'mcx>>>>
);

seam_core::seam!(
    /// `IsImportableForeignTable(tablename, stmt)` (foreign.c): apply the IMPORT
    /// FOREIGN SCHEMA `LIMIT TO`/`EXCEPT` table-list filter — `true` if the
    /// table should be imported. `stmt` carries `list_type`/`table_list`. Pure;
    /// returns the filter decision.
    pub fn is_importable_foreign_table(
        tablename: &str,
        stmt: &ImportForeignSchemaStmt<'_>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// Project one raw parse tree (`RawStmt *`) the IMPORT loop received from
    /// `pg_parse_query` into the fields the command driver branches on: the
    /// `nodeTag(rs->stmt)` classification ([`ImportRawStmt`]), the table name
    /// (`cstmt->base.relation->relname`), the `stmt_location`/`stmt_len`, and
    /// the embedded statement node (`rs->stmt`). The driver owns the
    /// type-check `elog`, the filter `continue`, and the `PlannedStmt` build;
    /// this seam only reads the unported parser node's fields.
    pub fn import_classify_raw_stmt(raw: RawStmtHandle) -> PgResult<ImportRawStmt>
);

seam_core::seam!(
    /// `cstmt->base.relation->schemaname = pstrdup(local_schema)` — the IMPORT
    /// loop's schema-name rewrite, applied to the embedded
    /// `CreateForeignTableStmt`'s `RangeVar` before the command is executed.
    /// Mutates the unported parser node in place.
    pub fn import_set_schemaname(raw: RawStmtHandle, local_schema: &str) -> PgResult<()>
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

// ===========================================================================
// FDW-routine lookup (foreign/foreign.c) and FDW-provider callbacks
// (foreign/fdwapi.h) — reached by backend-executor-nodeForeignscan.
// ===========================================================================

// ---------------------------------------------------------------------------
// foreign/foreign.c — FDW handler-table lookup.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `GetFdwRoutineForRelation(relation, makecopy)` (foreign.c): resolve the
    /// FDW handler table for the relation `node.ss.ss_currentRelation`
    /// (already opened), returning the trimmed presence table. Reads the
    /// relcache / `pg_foreign_*` catalogs (fallible: `ereport(ERROR)` on a bad
    /// handler).
    pub fn get_fdw_routine_for_relation<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<FdwRoutine>
);

seam_core::seam!(
    /// `GetFdwRoutineByServerId(serverid)` (foreign.c): resolve the FDW handler
    /// table from the foreign-server OID (used when `scanrelid == 0`, i.e. a
    /// pushed-down join with no base relation). Fallible.
    pub fn get_fdw_routine_by_server_id(serverid: Oid) -> PgResult<FdwRoutine>
);

// ---------------------------------------------------------------------------
// FDW-provider callbacks (fdwapi.h). The provider stores per-scan state in
// `node.fdw_state` and writes result tuples into the node's scan slot
// (`node.ss.ss_ScanTupleSlot`, addressed in the EState slot pool).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `node->fdwroutine->BeginForeignScan(node, eflags)`.
    pub fn begin_foreign_scan<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        eflags: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->fdwroutine->BeginDirectModify(node, eflags)`.
    pub fn begin_direct_modify<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
        eflags: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `slot = node->fdwroutine->IterateForeignScan(node)` (SELECT): the FDW
    /// stores the next tuple into the node's scan slot and the seam returns
    /// whether a tuple is available (the C `!TupIsNull(slot)`).
    pub fn iterate_foreign_scan<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `slot = node->fdwroutine->IterateDirectModify(node)`. Same convention as
    /// [`iterate_foreign_scan`].
    pub fn iterate_direct_modify<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `node->fdwroutine->ReScanForeignScan(node)`.
    pub fn rescan_foreign_scan<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->fdwroutine->EndForeignScan(node)`.
    pub fn end_foreign_scan<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->fdwroutine->EndDirectModify(node)`.
    pub fn end_direct_modify<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->fdwroutine->RecheckForeignScan(node, slot)` — recheck a tuple in
    /// an EvalPlanQual recheck (the node has verified the callback is present).
    /// Returns the FDW's verdict; the FDW may replace the slot's tuple in place.
    pub fn recheck_foreign_scan<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `slot->tts_tableOid = RelationGetRelid(node->ss.ss_currentRelation)`:
    /// stamp the `tableoid` system column into the FDW-returned scan slot, only
    /// when `plan->fsSystemCol` and the slot is non-empty. Reads the relid from
    /// the relcache entry and writes the slot payload (slot-owned), so it is
    /// reached through this seam until the slot payload model lands.
    pub fn stamp_scan_slot_tableoid<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// --- parallel DSM callbacks ---

seam_core::seam!(
    /// `node->pscan_len = node->fdwroutine->EstimateDSMForeignScan(node, pcxt)`.
    pub fn estimate_dsm_foreign_scan<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        pcxt: &mut ParallelContext,
    ) -> PgResult<usize>
);

seam_core::seam!(
    /// `node->fdwroutine->InitializeDSMForeignScan(node, pcxt, coordinate)` —
    /// the coordinate is the DSM chunk `shm_toc_allocate` returned (storage-
    /// owned); the provider initializes it and the node inserts it into the TOC.
    pub fn initialize_dsm_foreign_scan<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        pcxt: &mut ParallelContext,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->fdwroutine->ReInitializeDSMForeignScan(node, pcxt, coordinate)`.
    pub fn reinitialize_dsm_foreign_scan<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        pcxt: &mut ParallelContext,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->fdwroutine->InitializeWorkerForeignScan(node, pwcxt->toc, coordinate)`.
    pub fn initialize_worker_foreign_scan<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        pwcxt: &mut ParallelWorkerContext,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->fdwroutine->ShutdownForeignScan(node)` (the node verified the
    /// callback is present).
    pub fn shutdown_foreign_scan<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()>
);

// --- async-execution callbacks ---
//
// The three async entry points run `node->fdwroutine->ForeignAsync{Request,
// ConfigureWait,Notify}(areq)` where `node = (ForeignScanState *)
// areq->requestee`. The owned tree never reconstructs the `areq->requestee`
// raw back-pointer (the dispatch resolves the requestee `ForeignScanState`
// before reaching here), so the requestee node is passed by reference (value
// carrier, replacing the opaque `requestee`) alongside the `AsyncRequest`
// payload. The `fdwroutine` callbacks are FDW-extension-owned, so these stay
// uninstalled until such an extension lands (sanctioned FLOOR).

seam_core::seam!(
    /// `node->fdwroutine->ForeignAsyncRequest(areq)` — `node` is the requestee
    /// `ForeignScanState`; `areq` is the request record it fills.
    pub fn foreign_async_request<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        areq: &mut AsyncRequestData,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->fdwroutine->ForeignAsyncConfigureWait(areq)`.
    pub fn foreign_async_configure_wait<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        areq: &mut AsyncRequestData,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `node->fdwroutine->ForeignAsyncNotify(areq)`.
    pub fn foreign_async_notify<'mcx>(
        node: &mut ForeignScanState<'mcx>,
        areq: &mut AsyncRequestData,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `GetForeignServer(serverid)` (foreign.c): the server descriptor by OID;
    /// `elog(ERROR)` on cache lookup failure (carried on `Err`). Allocated in
    /// `mcx`.
    pub fn get_foreign_server<'mcx>(
        mcx: Mcx<'mcx>,
        serverid: Oid,
    ) -> PgResult<ForeignServer<'mcx>>
);

seam_core::seam!(
    /// `GetForeignDataWrapperExtended(fdwid, missing_ok)` (foreign.c): the FDW
    /// descriptor by OID. With `missing_ok = true` a missing wrapper is
    /// `Ok(None)`; otherwise `elog(ERROR)` (carried on `Err`). Allocated in
    /// `mcx`.
    pub fn get_foreign_data_wrapper_extended<'mcx>(
        mcx: Mcx<'mcx>,
        fdwid: Oid,
        missing_ok: bool,
    ) -> PgResult<Option<ForeignDataWrapper<'mcx>>>
);

seam_core::seam!(
    /// `GetForeignServerExtended(serverid, missing_ok)` (foreign.c): the
    /// server descriptor by OID. With `missing_ok = true` a missing server is
    /// `Ok(None)`; otherwise `elog(ERROR)` (carried on `Err`). Allocated in
    /// `mcx`.
    pub fn get_foreign_server_extended<'mcx>(
        mcx: Mcx<'mcx>,
        serverid: Oid,
        missing_ok: bool,
    ) -> PgResult<Option<ForeignServer<'mcx>>>
);

seam_core::seam!(
    /// `SearchSysCache1(FOREIGNTABLEREL, relid)` → `ftserver`
    /// (`ATExecAlterColumnGenericOptions`, tablecmds.c:15980): the foreign
    /// server OID for foreign table `relid`. `None` when `relid` has no
    /// `pg_foreign_table` row (the C `!HeapTupleIsValid(tuple)` → "foreign
    /// table does not exist" branch, raised by the caller).
    pub fn foreign_table_server_oid(relid: Oid) -> PgResult<Option<Oid>>
);

seam_core::seam!(
    /// `SearchSysCacheCopy1(FOREIGNTABLEREL, relid)` →
    /// `SysCacheGetAttr(ftoptions)` decoded to `(name, value)` pairs
    /// (`ATExecGenericOptions`, tablecmds.c:18696): the foreign table's current
    /// generic options. `None` when `relid` has no `pg_foreign_table` row.
    pub fn foreign_table_options(relid: Oid) -> PgResult<Option<Vec<(String, String)>>>
);
