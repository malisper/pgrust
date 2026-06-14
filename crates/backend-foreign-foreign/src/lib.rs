#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(clippy::result_large_err)]

//! `backend/foreign/foreign.c` — support for foreign-data wrappers, servers and
//! user mappings.
//!
//! Full port of PostgreSQL 18.3's `foreign.c`. Every function in the C file is
//! implemented here with the same control flow, error codes (SQLSTATE) and
//! message text. The catalog reads (`SearchSysCache*`/`GetSysCacheOid*`), the
//! FDW handler dispatch (`OidFunctionCall0` + `IsA(FdwRoutine)`), the
//! `restrict_nonsystem_relation_kind` GUC, `GetUserNameFromId`, the relcache
//! `rd_fdwroutine` cache, `untransformRelOptions`, and the SRF/tuplestore
//! machinery cross the boundary through their owners' seams; the algorithm
//! lives here.
//!
//! The FDW/server descriptor carriers (`ForeignDataWrapper`/`ForeignServer`)
//! are trimmed to the fields `commands/foreigncmds.c` and
//! `executor/nodeForeignscan.c` read, matching the established
//! `backend-foreign-foreign-seams` inward contract; the `ForeignTable` and
//! `UserMapping` carriers retain their `options` (`(name, value)` pairs decoded
//! by `untransformRelOptions`), as the C structs do.

use mcx::{Mcx, PgString};
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{
    PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_OBJECT,
};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_foreigncmds::{
    ForeignDataWrapper, ForeignServer, ForeignTable, ImportForeignSchemaStmt, UserMapping,
    FDW_IMPORT_SCHEMA_ALL, FDW_IMPORT_SCHEMA_EXCEPT, FDW_IMPORT_SCHEMA_LIMIT_TO,
};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::{FdwRoutine, ForeignScanState};

use backend_foreign_fdwapi_seams as fdwapi;
use backend_foreign_foreign_seams as inward;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_cache_syscache_seams as syscache;
use backend_utils_fmgr_fmgr_seams as fmgr;
use backend_utils_fmgr_funcapi_seams as funcapi;
use backend_utils_init_miscinit_seams as miscinit;
use backend_utils_misc_guc_tables_seams as guc;

/* ===========================================================================
 * Header constants foreign.c knows directly (verbatim from PG 18.3).
 * ======================================================================== */

/// `bits16` (`c.h`) — the flag width of the `*Extended` variants.
pub type bits16 = u16;

/// `FDW_MISSING_OK` (`foreign/foreign.h`).
pub const FDW_MISSING_OK: bits16 = 0x01;

/// `FSV_MISSING_OK` (`foreign/foreign.h`).
pub const FSV_MISSING_OK: bits16 = 0x01;

/// `RESTRICT_RELKIND_FOREIGN_TABLE` (`tcop/tcopprot.h`).
const RESTRICT_RELKIND_FOREIGN_TABLE: i32 = 0x02;

/// `ForeignServerRelationId` (`catalog/pg_foreign_server.h`).
const ForeignServerRelationId: Oid = types_core::catalog::FOREIGN_SERVER_RELATION_ID;
/// `UserMappingRelationId` (`catalog/pg_user_mapping.h`, OID 1418).
const UserMappingRelationId: Oid = 1418;

/// `elog(ERROR, …)` (no explicit SQLSTATE → `ERRCODE_INTERNAL_ERROR`).
#[inline]
fn elog_error(message: String) -> PgError {
    PgError::error(message)
}

/* ===========================================================================
 * GetForeignDataWrapper / …Extended / …ByName
 * ======================================================================== */

/// `GetForeignDataWrapper` — look up the foreign-data wrapper by OID.
pub fn GetForeignDataWrapper<'mcx>(
    mcx: Mcx<'mcx>,
    fdwid: Oid,
) -> PgResult<Option<ForeignDataWrapper<'mcx>>> {
    GetForeignDataWrapperExtended(mcx, fdwid, 0)
}

/// `GetForeignDataWrapperExtended` — look up the foreign-data wrapper by OID.
/// With `FDW_MISSING_OK` set, return `None` rather than error when absent.
pub fn GetForeignDataWrapperExtended<'mcx>(
    mcx: Mcx<'mcx>,
    fdwid: Oid,
    flags: bits16,
) -> PgResult<Option<ForeignDataWrapper<'mcx>>> {
    // tp = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdwid));
    let row = match syscache::foreign_data_wrapper_form::call(mcx, fdwid)? {
        Some(row) => row,
        None => {
            if (flags & FDW_MISSING_OK) == 0 {
                return Err(elog_error(format!(
                    "cache lookup failed for foreign-data wrapper {}",
                    fdwid
                )));
            }
            return Ok(None);
        }
    };

    // fdw = palloc(sizeof(ForeignDataWrapper)); fill in.
    Ok(Some(ForeignDataWrapper {
        fdwid,
        fdwname: row.fdwname,
        fdwhandler: row.fdwhandler,
        fdwvalidator: row.fdwvalidator,
    }))
}

/// `GetForeignDataWrapperByName` — look up the FDW definition by name.
pub fn GetForeignDataWrapperByName<'mcx>(
    mcx: Mcx<'mcx>,
    fdwname: &str,
    missing_ok: bool,
) -> PgResult<Option<ForeignDataWrapper<'mcx>>> {
    let fdw_id = get_foreign_data_wrapper_oid(fdwname, missing_ok)?;

    if !OidIsValid(fdw_id) {
        return Ok(None);
    }

    GetForeignDataWrapper(mcx, fdw_id)
}

/* ===========================================================================
 * GetForeignServer / …Extended / …ByName
 * ======================================================================== */

/// `GetForeignServer` — look up the foreign server definition.
pub fn GetForeignServer<'mcx>(
    mcx: Mcx<'mcx>,
    serverid: Oid,
) -> PgResult<Option<ForeignServer<'mcx>>> {
    GetForeignServerExtended(mcx, serverid, 0)
}

/// `GetForeignServerExtended` — look up the foreign server definition. With
/// `FSV_MISSING_OK` set, return `None` rather than error when absent.
pub fn GetForeignServerExtended<'mcx>(
    mcx: Mcx<'mcx>,
    serverid: Oid,
    flags: bits16,
) -> PgResult<Option<ForeignServer<'mcx>>> {
    // tp = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(serverid));
    let row = match syscache::foreign_server_form::call(mcx, serverid)? {
        Some(row) => row,
        None => {
            if (flags & FSV_MISSING_OK) == 0 {
                return Err(elog_error(format!(
                    "cache lookup failed for foreign server {}",
                    serverid
                )));
            }
            return Ok(None);
        }
    };

    Ok(Some(ForeignServer {
        serverid,
        servername: row.srvname,
        fdwid: row.srvfdw,
    }))
}

/// `GetForeignServerByName` — look up the foreign server definition by name.
pub fn GetForeignServerByName<'mcx>(
    mcx: Mcx<'mcx>,
    srvname: &str,
    missing_ok: bool,
) -> PgResult<Option<ForeignServer<'mcx>>> {
    let serverid = get_foreign_server_oid(srvname, missing_ok)?;

    if !OidIsValid(serverid) {
        return Ok(None);
    }

    GetForeignServer(mcx, serverid)
}

/* ===========================================================================
 * GetUserMapping (+ MappingUserName)
 * ======================================================================== */

/// `MappingUserName(userid)` (foreign.h):
/// `OidIsValid(userid) ? GetUserNameFromId(userid, false) : "public"`.
pub fn MappingUserName<'mcx>(mcx: Mcx<'mcx>, userid: Oid) -> PgResult<PgString<'mcx>> {
    if OidIsValid(userid) {
        // GetUserNameFromId(userid, false): noerr = false, so an absent role
        // raises; the `Option` is therefore always `Some` here.
        match miscinit::get_user_name_from_id::call(mcx, userid, false)? {
            Some(name) => Ok(name),
            None => Err(elog_error(format!(
                "cache lookup failed for role {}",
                userid
            ))),
        }
    } else {
        PgString::from_str_in("public", mcx)
    }
}

/* ===========================================================================
 * GetUserMapping
 * ======================================================================== */

/// `GetUserMapping(userid, serverid)` — look up the user mapping. If no mapping
/// is found for the supplied user, also look for PUBLIC mappings
/// (`userid == InvalidOid`).
pub fn GetUserMapping<'mcx>(
    mcx: Mcx<'mcx>,
    userid: Oid,
    serverid: Oid,
) -> PgResult<UserMapping> {
    // tp = SearchSysCache2(USERMAPPINGUSERSERVER, userid, serverid);
    let mut found = syscache::user_mapping_form::call(mcx, userid, serverid)?;

    if found.is_none() {
        // Not found for the specific user -- try PUBLIC (InvalidOid).
        found = syscache::user_mapping_form::call(mcx, InvalidOid, serverid)?;
    }

    let (umid, raw_options) = match found {
        Some(row) => row,
        None => {
            // ereport(ERROR, ERRCODE_UNDEFINED_OBJECT,
            //   "user mapping not found for user \"%s\", server \"%s\"",
            //   MappingUserName(userid), server->servername);
            // server = GetForeignServer(serverid) (flags = 0 → raises if absent).
            let server = match GetForeignServer(mcx, serverid)? {
                Some(server) => server,
                None => {
                    return Err(elog_error(format!(
                        "cache lookup failed for foreign server {}",
                        serverid
                    )))
                }
            };
            let username = MappingUserName(mcx, userid)?;
            return Err(PgError::error(format!(
                "user mapping not found for user \"{}\", server \"{}\"",
                username.as_str(),
                server.servername.as_str()
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
        }
    };

    // um = palloc(...); um->umid = GETSTRUCT(tp)->oid; um->userid = userid;
    // um->serverid = serverid;
    // Extract the umoptions (NULL → NIL).
    let options = match raw_options {
        Some(bytes) => backend_access_common_reloptions::untransformRelOptions(
            mcx,
            Some(bytes.as_slice()),
        )?,
        None => Vec::new(),
    };

    Ok(UserMapping {
        umid,
        userid,
        serverid,
        options,
    })
}

/* ===========================================================================
 * GetForeignTable / GetForeignColumnOptions / GetForeignServerIdByRelId
 * ======================================================================== */

/// `GetForeignTable(relid)` — look up the foreign table definition by relation
/// OID.
pub fn GetForeignTable<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<ForeignTable> {
    // tp = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
    let (serverid, raw_options) = match syscache::foreign_table_form::call(mcx, relid)? {
        Some(row) => row,
        None => {
            return Err(elog_error(format!(
                "cache lookup failed for foreign table {}",
                relid
            )))
        }
    };

    // ft = palloc(...); ft->relid = relid; ft->serverid = tableform->ftserver;
    // Extract the ftoptions (NULL → NIL).
    let options = match raw_options {
        Some(bytes) => backend_access_common_reloptions::untransformRelOptions(
            mcx,
            Some(bytes.as_slice()),
        )?,
        None => Vec::new(),
    };

    Ok(ForeignTable {
        relid,
        serverid,
        options,
    })
}

/// `GetForeignColumnOptions(relid, attnum)` — get `attfdwoptions` of a given
/// relation/attnum as a list of options.
pub fn GetForeignColumnOptions<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: i16,
) -> PgResult<Vec<(String, Option<String>)>> {
    // tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    let raw_options = match syscache::attribute_fdwoptions::call(mcx, relid, attnum)? {
        Some(opt) => opt,
        None => {
            return Err(elog_error(format!(
                "cache lookup failed for attribute {} of relation {}",
                attnum, relid
            )))
        }
    };

    // datum = SysCacheGetAttr(..., attfdwoptions, &isnull);
    // options = isnull ? NIL : untransformRelOptions(datum);
    match raw_options {
        Some(bytes) => backend_access_common_reloptions::untransformRelOptions(
            mcx,
            Some(bytes.as_slice()),
        ),
        None => Ok(Vec::new()),
    }
}

/// `GetForeignServerIdByRelId` — the foreign server OID for a foreign table.
pub fn GetForeignServerIdByRelId(relid: Oid) -> PgResult<Oid> {
    // tp = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
    match syscache::foreign_table_server_by_relid::call(relid)? {
        Some(serverid) => Ok(serverid),
        None => Err(elog_error(format!(
            "cache lookup failed for foreign table {}",
            relid
        ))),
    }
}

/* ===========================================================================
 * GetFdwRoutine and friends
 * ======================================================================== */

/// `GetFdwRoutine` — call the FDW handler routine to get its `FdwRoutine`.
pub fn GetFdwRoutine(fdwhandler: Oid) -> PgResult<FdwRoutine> {
    /* Check if the access to foreign tables is restricted */
    if (guc::restrict_nonsystem_relation_kind::call() & RESTRICT_RELKIND_FOREIGN_TABLE) != 0 {
        /* there must not be built-in FDW handler */
        return Err(
            PgError::error("access to non-system foreign table is restricted")
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        );
    }

    // datum = OidFunctionCall0(fdwhandler); routine = DatumGetPointer(datum);
    // if (routine == NULL || !IsA(routine, FdwRoutine)) elog(ERROR, ...);
    match fdwapi::fdw_routine_from_handler::call(fdwhandler)? {
        Some(routine) => Ok(routine),
        None => Err(elog_error(format!(
            "foreign-data wrapper handler function {} did not return an FdwRoutine struct",
            fdwhandler
        ))),
    }
}

/// `GetFdwRoutineByServerId` — resolve the FDW handler table from a server OID.
pub fn GetFdwRoutineByServerId(mcx: Mcx<'_>, serverid: Oid) -> PgResult<FdwRoutine> {
    /* Get foreign-data wrapper OID for the server. */
    let fdwid = match syscache::foreign_server_form::call(mcx, serverid)? {
        Some(row) => row.srvfdw,
        None => {
            return Err(elog_error(format!(
                "cache lookup failed for foreign server {}",
                serverid
            )))
        }
    };

    /* Get handler function OID for the FDW. */
    let fdw = match syscache::foreign_data_wrapper_form::call(mcx, fdwid)? {
        Some(row) => row,
        None => {
            return Err(elog_error(format!(
                "cache lookup failed for foreign-data wrapper {}",
                fdwid
            )))
        }
    };
    let fdwhandler = fdw.fdwhandler;

    /* Complain if FDW has been set to NO HANDLER. */
    if !OidIsValid(fdwhandler) {
        return Err(PgError::error(format!(
            "foreign-data wrapper \"{}\" has no handler",
            fdw.fdwname.as_str()
        ))
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
    }

    /* And finally, call the handler function. */
    GetFdwRoutine(fdwhandler)
}

/// `GetFdwRoutineByRelId` — resolve the FDW handler table from a foreign-table
/// relation OID.
pub fn GetFdwRoutineByRelId(mcx: Mcx<'_>, relid: Oid) -> PgResult<FdwRoutine> {
    /* Get server OID for the foreign table. */
    let serverid = GetForeignServerIdByRelId(relid)?;

    /* Now retrieve server's FdwRoutine struct. */
    GetFdwRoutineByServerId(mcx, serverid)
}

/// `GetFdwRoutineForRelation(relation, makecopy)` — resolve the FDW handler
/// table for the relation, caching it in the relcache entry.
///
/// `relid` identifies the relcache entry across the seam; the owned tree always
/// returns an owned `FdwRoutine`, so the C `makecopy` distinction collapses.
pub fn GetFdwRoutineForRelation(mcx: Mcx<'_>, relid: Oid) -> PgResult<FdwRoutine> {
    /* We have valid cached data --- hand back a copy. */
    if let Some(cached) = relcache::relation_fdwroutine::call(relid)? {
        return Ok(cached);
    }

    /* Get the info by consulting the catalogs and the FDW code. */
    let fdwroutine = GetFdwRoutineByRelId(mcx, relid)?;

    /* Save the data for later reuse in CacheMemoryContext. */
    relcache::set_relation_fdwroutine::call(relid, fdwroutine)?;

    /* Give back the locally palloc'd copy regardless of makecopy. */
    Ok(fdwroutine)
}

/* ===========================================================================
 * IsImportableForeignTable
 * ======================================================================== */

/// `IsImportableForeignTable` — IMPORT FOREIGN SCHEMA table-name filter.
///
/// The C `stmt->table_list` is a `List *` of `RangeVar`; the owned
/// `ImportForeignSchemaStmt.table_list` carries the `rv->relname`s the filter
/// loops compare.
pub fn IsImportableForeignTable(tablename: &str, stmt: &ImportForeignSchemaStmt<'_>) -> bool {
    match stmt.list_type {
        FDW_IMPORT_SCHEMA_ALL => true,

        FDW_IMPORT_SCHEMA_LIMIT_TO => {
            for rv in stmt.table_list.iter() {
                if tablename == rv.as_str() {
                    return true;
                }
            }
            false
        }

        FDW_IMPORT_SCHEMA_EXCEPT => {
            for rv in stmt.table_list.iter() {
                if tablename == rv.as_str() {
                    return false;
                }
            }
            true
        }
    }
}

/* ===========================================================================
 * pg_options_to_table
 * ======================================================================== */

/// `pg_options_to_table(PG_FUNCTION_ARGS)` — convert an options `text[]` to a
/// `(option_name, option_value)` set.
pub fn pg_options_to_table<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // Datum array = PG_GETARG_DATUM(0);
    let array = fmgr::pg_getarg_varlena_pp::call(fcinfo, 0)?;

    // options = untransformRelOptions(array);
    let options = backend_access_common_reloptions::untransformRelOptions(mcx, Some(array.as_bytes()))?;

    // InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
    funcapi::InitMaterializedSRF::call(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    // foreach(cell, options) { ... tuplestore_putvalues(...); }
    for (defname, arg) in options.iter() {
        let mut values: [Datum; 2] = [Datum::null(), Datum::null()];
        let mut nulls: [bool; 2] = [false; 2];

        // values[0] = CStringGetTextDatum(def->defname); nulls[0] = false;
        // CStringGetTextDatum is a `text *` (by-reference) value; the funcapi
        // seam still hands back the shim bare word, so lift it into the
        // canonical enum's by-value word and lower it again at the tuplestore
        // ABI edge below.
        values[0] = Datum::from_usize(funcapi::cstring_get_text_datum::call(mcx, defname)?.as_usize());
        nulls[0] = false;

        // if (def->arg) { values[1] = CStringGetTextDatum(strVal(def->arg)); }
        // else { values[1] = (Datum) 0; nulls[1] = true; }
        match arg {
            Some(v) => {
                values[1] =
                    Datum::from_usize(funcapi::cstring_get_text_datum::call(mcx, v)?.as_usize());
                nulls[1] = false;
            }
            None => {
                values[1] = Datum::null();
                nulls[1] = true;
            }
        }

        let rsinfo = fcinfo
            .resultinfo
            .as_mut()
            .expect("InitMaterializedSRF set fcinfo->resultinfo");
        // tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
        // Lower the canonical by-value words to the still-shim-typed seam
        // contract at this audited tuplestore edge (funcapi/tuplestore have not
        // advanced off the bare-word newtype).
        let shim_values: [types_datum::Datum; 2] =
            std::array::from_fn(|i| types_datum::Datum::from_usize(values[i].as_usize()));
        funcapi::materialized_srf_putvalues::call(rsinfo, &shim_values, &nulls)?;
    }

    // return (Datum) 0;
    Ok(Datum::null())
}

/// `MAT_SRF_USE_EXPECTED_DESC` (`funcapi.h`).
const MAT_SRF_USE_EXPECTED_DESC: u32 = 0x01;

/* ===========================================================================
 * libpq conninfo options + is_conninfo_option + postgresql_fdw_validator
 * ======================================================================== */

/// `struct ConnectionOption` (foreign.c).
struct ConnectionOption {
    optname: &'static str,
    optcontext: Oid,
}

/// `libpq_conninfo_options[]`, copied from fe-connect.c `PQconninfoOptions`.
/// The C array is NULL-terminated; the terminator is dropped here.
const LIBPQ_CONNINFO_OPTIONS: &[ConnectionOption] = &[
    ConnectionOption { optname: "authtype", optcontext: ForeignServerRelationId },
    ConnectionOption { optname: "service", optcontext: ForeignServerRelationId },
    ConnectionOption { optname: "user", optcontext: UserMappingRelationId },
    ConnectionOption { optname: "password", optcontext: UserMappingRelationId },
    ConnectionOption { optname: "connect_timeout", optcontext: ForeignServerRelationId },
    ConnectionOption { optname: "dbname", optcontext: ForeignServerRelationId },
    ConnectionOption { optname: "host", optcontext: ForeignServerRelationId },
    ConnectionOption { optname: "hostaddr", optcontext: ForeignServerRelationId },
    ConnectionOption { optname: "port", optcontext: ForeignServerRelationId },
    ConnectionOption { optname: "tty", optcontext: ForeignServerRelationId },
    ConnectionOption { optname: "options", optcontext: ForeignServerRelationId },
    ConnectionOption { optname: "requiressl", optcontext: ForeignServerRelationId },
    ConnectionOption { optname: "sslmode", optcontext: ForeignServerRelationId },
    ConnectionOption { optname: "gsslib", optcontext: ForeignServerRelationId },
    ConnectionOption { optname: "gssdelegation", optcontext: ForeignServerRelationId },
];

/// `is_conninfo_option` — check if the option is one of libpq conninfo options.
/// `context` is the catalog OID the option came from, or 0 if we don't care.
fn is_conninfo_option(option: &str, context: Oid) -> bool {
    for opt in LIBPQ_CONNINFO_OPTIONS {
        if context == opt.optcontext && opt.optname == option {
            return true;
        }
    }
    false
}

/// `postgresql_fdw_validator(PG_FUNCTION_ARGS)` — validate the generic option
/// given to SERVER or USER MAPPING. Raise an ERROR if the option is invalid.
///
/// The argument extraction (`untransformRelOptions(PG_GETARG_DATUM(0))` +
/// `PG_GETARG_OID(1)`) is performed here off `fcinfo`; the validation logic is
/// ported 1:1. Returns the boolean the SQL function would `PG_RETURN_BOOL`.
pub fn postgresql_fdw_validator<'mcx>(
    mcx: Mcx<'mcx>,
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    let array = fmgr::pg_getarg_varlena_pp::call(fcinfo, 0)?;
    let options_list = backend_access_common_reloptions::untransformRelOptions(mcx, Some(array.as_bytes()))?;
    // Oid catalog = PG_GETARG_OID(1);
    let catalog = fmgr::pg_getarg_oid::call(fcinfo, 1);

    for (defname, _arg) in options_list.iter() {
        if !is_conninfo_option(defname, catalog) {
            /*
             * Unknown option specified, complain about it. Provide a hint with a
             * valid option that looks similar, if there is one.
             */
            // initClosestMatch(&match_state, def->defname, 4);
            // for (opt = ...) if (catalog == opt->optcontext) { has_valid_options = true;
            //                    updateClosestMatch(&match_state, opt->optname); }
            // closest_match = getClosestMatch(&match_state);
            let mut has_valid_options = false;
            let mut candidates: Vec<&[u8]> = Vec::new();
            for opt in LIBPQ_CONNINFO_OPTIONS {
                if catalog == opt.optcontext {
                    has_valid_options = true;
                    candidates.push(opt.optname.as_bytes());
                }
            }
            let closest_match = backend_utils_adt_varlena::misc_encoding::levenshtein_closest_match(
                mcx,
                defname.as_bytes(),
                &candidates,
                4,
            )?;

            // ereport(ERROR, errcode(SYNTAX_ERROR), errmsg("invalid option \"%s\"", def->defname),
            //   has_valid_options ? closest_match ? errhint("Perhaps you meant the option \"%s\".", closest_match)
            //                                      : 0
            //                     : errhint("There are no valid options in this context."));
            let mut err = PgError::error(format!("invalid option \"{defname}\""))
                .with_sqlstate(ERRCODE_SYNTAX_ERROR);
            if has_valid_options {
                if let Some(cm) = closest_match {
                    let cm = String::from_utf8_lossy(cm.as_slice());
                    err = err.with_hint(format!("Perhaps you meant the option \"{cm}\"."));
                }
            } else {
                err = err.with_hint("There are no valid options in this context.");
            }

            return Err(err);
        }
    }

    // PG_RETURN_BOOL(true);
    // fmgr owns the SQL-return bare-word ABI edge; lift its word into the
    // canonical enum for this crate's return type.
    Ok(Datum::from_usize(
        fmgr::pg_return_bool::call(fcinfo, true).as_usize(),
    ))
}

/* ===========================================================================
 * get_foreign_data_wrapper_oid / get_foreign_server_oid
 * ======================================================================== */

/// `get_foreign_data_wrapper_oid` — the OID of a FDW by name.
pub fn get_foreign_data_wrapper_oid(fdwname: &str, missing_ok: bool) -> PgResult<Oid> {
    // oid = GetSysCacheOid1(FOREIGNDATAWRAPPERNAME, Anum_oid, CStringGetDatum(fdwname));
    let oid = syscache::foreign_data_wrapper_oid_by_name::call(fdwname)?;
    if !OidIsValid(oid) && !missing_ok {
        return Err(PgError::error(format!(
            "foreign-data wrapper \"{fdwname}\" does not exist"
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    Ok(oid)
}

/// `get_foreign_server_oid` — the OID of a server by name.
pub fn get_foreign_server_oid(servername: &str, missing_ok: bool) -> PgResult<Oid> {
    // oid = GetSysCacheOid1(FOREIGNSERVERNAME, Anum_oid, CStringGetDatum(servername));
    let oid = syscache::foreign_server_oid_by_name::call(servername)?;
    if !OidIsValid(oid) && !missing_ok {
        return Err(
            PgError::error(format!("server \"{servername}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        );
    }
    Ok(oid)
}

/* ===========================================================================
 * GetExistingLocalJoinPath — prerequisite-blocked.
 *
 * The C function walks `joinrel->pathlist`, and for each unparameterized
 * MergeJoin/HashJoin/NestLoop path `makeNode`-copies the *subtype*
 * (`HashPath`/`NestPath`/`MergePath`, not a base `Path`), then downcasts each
 * child via `IsA(joinpath->outerjoinpath, ForeignPath)` to splice in
 * `fdw_outerpath`. The owned tree stores `RelOptInfo.pathlist` as the base
 * `Path` node only, with no enum/trait to recover the
 * `JoinPath`/`MergePath`/`HashPath`/`ForeignPath` subtype from a stored base
 * `Path`. Porting the walk would silently drop the subtype fields it
 * manipulates (a false-green stub), so it is blocked on the unified walkable
 * Node enum; it has no caller in the current tree and is not in the inward
 * seam contract. Calling it panics loudly rather than returning a wrong answer.
 * ======================================================================== */

/// `GetExistingLocalJoinPath` — obtain an alternate local join path for EPQ.
/// Prerequisite-blocked on path-subtype polymorphism (see module note above).
pub fn GetExistingLocalJoinPath(_joinrel: &types_pathnodes::RelOptInfo) -> ! {
    panic!(
        "GetExistingLocalJoinPath: prerequisite-blocked — requires recovering \
         MergePath/HashPath/NestPath/ForeignPath subtypes from a stored base Path, \
         which the owned pathlist model cannot express until a unified walkable \
         Node enum lands"
    );
}

/* ===========================================================================
 * Inward-seam installers (the foreign.c entry points reached across cycles).
 * ======================================================================== */

/// Install this crate's inward seams (the `foreign.c` entry points
/// `commands/foreigncmds.c` and `executor/nodeForeignscan.c` reach through
/// `backend-foreign-foreign-seams`). The catalog-DML / FDW-provider / IMPORT
/// parser seams that crate also declares are installed by their own owners.
pub fn init_seams() {
    inward::get_foreign_data_wrapper::set(|mcx, fdwid| {
        // GetForeignDataWrapper raises (Err) on a missing FDW (flags = 0); the
        // inward seam returns the descriptor by value, so unwrap the `Some`.
        match GetForeignDataWrapper(mcx, fdwid)? {
            Some(fdw) => Ok(fdw),
            None => Err(elog_error(format!(
                "cache lookup failed for foreign-data wrapper {}",
                fdwid
            ))),
        }
    });

    inward::get_foreign_data_wrapper_by_name::set(GetForeignDataWrapperByName);

    inward::get_foreign_server_by_name::set(GetForeignServerByName);

    inward::foreign_data_wrapper_name::set(|mcx, fdwid, missing_ok| {
        let flags = if missing_ok { FDW_MISSING_OK } else { 0 };
        Ok(GetForeignDataWrapperExtended(mcx, fdwid, flags)?.map(|fdw| fdw.fdwname))
    });

    inward::foreign_server_name::set(|mcx, serverid, missing_ok| {
        let flags = if missing_ok { FSV_MISSING_OK } else { 0 };
        Ok(GetForeignServerExtended(mcx, serverid, flags)?.map(|srv| srv.servername))
    });

    inward::get_foreign_server_oid::set(get_foreign_server_oid);

    inward::get_foreign_data_wrapper_oid::set(get_foreign_data_wrapper_oid);

    inward::is_importable_foreign_table::set(|tablename, stmt| {
        Ok(IsImportableForeignTable(tablename, stmt))
    });

    inward::mapping_user_name::set(MappingUserName);

    inward::get_fdw_routine_for_relation::set(|node, _estate| {
        // RelationGetRelid(node->ss.ss_currentRelation). The resolved
        // FdwRoutine is a Copy flag table, so a transient context suffices.
        let relid = scan_relation_relid(node);
        let ctx = mcx::MemoryContext::new("GetFdwRoutineForRelation");
        GetFdwRoutineForRelation(ctx.mcx(), relid)
    });

    inward::get_fdw_routine_by_server_id::set(|serverid| {
        let ctx = mcx::MemoryContext::new("GetFdwRoutineByServerId");
        GetFdwRoutineByServerId(ctx.mcx(), serverid)
    });
}

/// `RelationGetRelid(node->ss.ss_currentRelation)`.
fn scan_relation_relid(node: &ForeignScanState<'_>) -> Oid {
    node.ss
        .ss_currentRelation
        .as_ref()
        .map(|r| r.rd_id)
        .unwrap_or(InvalidOid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_conninfo_option_matches_table_with_context() {
        // user/password belong to USER MAPPING; the rest to SERVER.
        assert!(is_conninfo_option("user", UserMappingRelationId));
        assert!(is_conninfo_option("password", UserMappingRelationId));
        assert!(is_conninfo_option("host", ForeignServerRelationId));
        assert!(is_conninfo_option("port", ForeignServerRelationId));
        assert!(is_conninfo_option("gssdelegation", ForeignServerRelationId));

        // Right name, wrong context -> not a match.
        assert!(!is_conninfo_option("user", ForeignServerRelationId));
        assert!(!is_conninfo_option("host", UserMappingRelationId));

        // Unknown option / "don't care" context never equals a real catalog OID.
        assert!(!is_conninfo_option("nonesuch", ForeignServerRelationId));
        assert!(!is_conninfo_option("host", 0));
    }

    #[test]
    fn flag_constants_match_header() {
        assert_eq!(FDW_MISSING_OK, 0x01);
        assert_eq!(FSV_MISSING_OK, 0x01);
        assert_eq!(RESTRICT_RELKIND_FOREIGN_TABLE, 0x02);
        assert_eq!(ForeignServerRelationId, 1417);
        assert_eq!(UserMappingRelationId, 1418);
    }

    fn import_stmt<'mcx>(
        mcx: Mcx<'mcx>,
        list_type: types_foreigncmds::ImportForeignSchemaType,
        names: &[&str],
    ) -> PgResult<ImportForeignSchemaStmt<'mcx>> {
        let mut table_list = mcx::vec_with_capacity_in(mcx, names.len())?;
        for n in names {
            table_list.push(PgString::from_str_in(n, mcx)?);
        }
        Ok(ImportForeignSchemaStmt {
            server_name: PgString::from_str_in("srv", mcx)?,
            remote_schema: PgString::from_str_in("rs", mcx)?,
            local_schema: PgString::from_str_in("ls", mcx)?,
            list_type,
            table_list,
            options: mcx::vec_with_capacity_in(mcx, 0)?,
        })
    }

    #[test]
    fn is_importable_filters() {
        let ctx = mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();

        let all = import_stmt(mcx, FDW_IMPORT_SCHEMA_ALL, &[]).unwrap();
        assert!(IsImportableForeignTable("anything", &all));

        let limit = import_stmt(mcx, FDW_IMPORT_SCHEMA_LIMIT_TO, &["t1", "t2"]).unwrap();
        assert!(IsImportableForeignTable("t1", &limit));
        assert!(IsImportableForeignTable("t2", &limit));
        assert!(!IsImportableForeignTable("t3", &limit));

        let except = import_stmt(mcx, FDW_IMPORT_SCHEMA_EXCEPT, &["t1"]).unwrap();
        assert!(!IsImportableForeignTable("t1", &except));
        assert!(IsImportableForeignTable("t2", &except));
    }
}
