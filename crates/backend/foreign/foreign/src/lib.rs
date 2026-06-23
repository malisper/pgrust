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
use types_tuple::heaptuple::Datum;
use types_foreigncmds::{
    ForeignDataWrapper, ForeignServer, ForeignTable, ImportForeignSchemaStmt, UserMapping,
    FDW_IMPORT_SCHEMA_ALL, FDW_IMPORT_SCHEMA_EXCEPT, FDW_IMPORT_SCHEMA_LIMIT_TO,
};
use ::nodes::fmgr::FunctionCallInfoBaseData;
use nodes::{FdwRoutine, ForeignScanState};

use fdwapi_seams as fdwapi;
use foreign_seams as inward;
use relcache_seams as relcache;
use syscache_seams as syscache;
use fmgr_seams as fmgr;
use funcapi_seams as funcapi;
use miscinit_seams as miscinit;
use guc_tables_seams as guc;

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
const ForeignServerRelationId: Oid = ::types_core::catalog::FOREIGN_SERVER_RELATION_ID;
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
        Some(bytes) => common_reloptions::untransformRelOptions(
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
        Some(bytes) => common_reloptions::untransformRelOptions(
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

/// Seam body for `foreign_table_options`: the foreign table's current
/// `ftoptions` decoded into owned `(name, value)` pairs (`None` ⇒ no
/// `pg_foreign_table` row). Strings are owned, so the per-call context is
/// freed before returning.
fn foreign_table_options_impl(relid: Oid) -> PgResult<Option<Vec<(String, String)>>> {
    let ctx = MemoryContext::new("foreign_table_options");
    let mcx = ctx.mcx();
    let form = syscache::foreign_table_form::call(mcx, relid)?;
    let Some((_serverid, raw_options)) = form else {
        return Ok(None);
    };
    let pairs = match raw_options {
        Some(bytes) => {
            common_reloptions::untransformRelOptions(mcx, Some(bytes.as_slice()))?
        }
        None => Vec::new(),
    };
    Ok(Some(
        pairs
            .into_iter()
            .map(|(n, v)| (n, v.unwrap_or_default()))
            .collect(),
    ))
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
        Some(bytes) => common_reloptions::untransformRelOptions(
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
    let options = common_reloptions::untransformRelOptions(mcx, Some(array.as_bytes()))?;

    // InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
    funcapi::InitMaterializedSRF::call(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    // foreach(cell, options) { ... tuplestore_putvalues(...); }
    for (defname, arg) in options.iter() {
        let mut values: [Datum<'mcx>; 2] = [Datum::null(), Datum::null()];
        let mut nulls: [bool; 2] = [false; 2];

        // values[0] = CStringGetTextDatum(def->defname); nulls[0] = false;
        values[0] = funcapi::cstring_get_text_datum::call(mcx, defname)?;
        nulls[0] = false;

        // if (def->arg) { values[1] = CStringGetTextDatum(strVal(def->arg)); }
        // else { values[1] = (Datum) 0; nulls[1] = true; }
        match arg {
            Some(v) => {
                values[1] = funcapi::cstring_get_text_datum::call(mcx, v)?;
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
        funcapi::materialized_srf_putvalues::call(rsinfo, &values, &nulls)?;
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
    let options_list = common_reloptions::untransformRelOptions(mcx, Some(array.as_bytes()))?;
    // Oid catalog = PG_GETARG_OID(1);
    let catalog = fmgr::pg_getarg_oid::call(fcinfo, 1);

    // PG_RETURN_BOOL(...).
    Ok(Datum::from_bool(postgresql_fdw_validator_core(
        mcx,
        &options_list,
        catalog,
    )?))
}

/// The validation body of `postgresql_fdw_validator`, over the already-decoded
/// option list (the `untransformRelOptions` output `(defname, arg)` pairs) and
/// catalog OID. Raises an ERROR if any option is invalid; returns `true`
/// otherwise. Shared by the fmgr `fcinfo` entry point and the `fmgr_builtins`
/// fc-adapter (which decodes the `text[]` arg off the by-reference lane).
pub fn postgresql_fdw_validator_core<'mcx>(
    mcx: Mcx<'mcx>,
    options_list: &[(String, Option<String>)],
    catalog: Oid,
) -> PgResult<bool> {
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
            let closest_match = varlena::misc_encoding::levenshtein_closest_match(
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

    Ok(true)
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
 * GetExistingLocalJoinPath (foreign.c:742).
 * ======================================================================== */

/// `IS_JOIN_REL(rel)` (pathnodes.h) — true for `RELOPT_JOINREL` /
/// `RELOPT_OTHER_JOINREL`.
#[inline]
fn is_join_rel(rel: &::pathnodes::RelOptInfo) -> bool {
    rel.reloptkind == ::pathnodes::RELOPT_JOINREL
        || rel.reloptkind == ::pathnodes::RELOPT_OTHER_JOINREL
}

/// `GetExistingLocalJoinPath` — get a copy of an existing local path for a join
/// relation, usually to obtain an alternate local path for EPQ checks.
///
/// Right now this only supports unparameterized foreign joins, so we only search
/// for unparameterized MergeJoin/HashJoin/NestLoop paths in `joinrel`'s path
/// list. If the inner or outer subpath of the chosen path is a `ForeignPath`
/// corresponding to a pushed-down join, we replace it with its `fdw_outerpath`,
/// so the returned path is built entirely of local join strategies. The result
/// is a shallow copy of the original (no need to copy the substructure), so it
/// is a freshly-allocated [`PathId`] into `root.path_arena`; returns `None` if
/// no suitable path is found.
///
/// Mirrors `foreign.c:742`. In the owned tree paths live in
/// `PlannerInfo::path_arena` and `RelOptInfo::pathlist` holds [`PathId`]
/// handles, so the C `(Path *) subtype` up/down-casts and `makeNode`+`memcpy`
/// copy become [`PathNode`] enum matches plus a clone pushed back into the
/// arena.
pub fn GetExistingLocalJoinPath(
    root: &mut ::pathnodes::PlannerInfo,
    joinrel: ::pathnodes::RelId,
) -> Option<::pathnodes::PathId> {
    use ::nodes::nodehashjoin::T_HashJoin;
    use ::nodes::nodemergejoin::T_MergeJoin;
    use ::nodes::nodenestloop::T_NestLoop;
    use ::pathnodes::PathNode;

    // Assert(IS_JOIN_REL(joinrel));
    debug_assert!(is_join_rel(root.rel(joinrel)));

    // foreach(lc, joinrel->pathlist)
    let pathlist = root.rel(joinrel).pathlist.clone();
    for path_id in pathlist {
        // Path *path = (Path *) lfirst(lc);
        let path = root.path(path_id);

        // Skip parameterized paths.
        if path.base().param_info.is_some() {
            continue;
        }

        // switch (path->pathtype): makeNode(subtype) + memcpy(.., path, ..)
        // copies the *subtype* node. Recover it from the stored PathNode and
        // clone it into a fresh `joinpath` candidate. Anything that is not a
        // MergeJoin/HashJoin/NestLoop is skipped — we don't know if the
        // corresponding plan would build the output row from whole-row
        // references of base relations and execute the EPQ checks.
        let pathtype = path.base().pathtype;
        let mut joinpath: PathNode = if pathtype == T_HashJoin {
            match path {
                PathNode::HashPath(p) => PathNode::HashPath(p.clone()),
                _ => continue,
            }
        } else if pathtype == T_NestLoop {
            match path {
                PathNode::NestPath(p) => PathNode::NestPath(p.clone()),
                _ => continue,
            }
        } else if pathtype == T_MergeJoin {
            match path {
                PathNode::MergePath(p) => PathNode::MergePath(p.clone()),
                _ => continue,
            }
        } else {
            continue;
        };

        // The cloned subtype shares its JoinPath base regardless of variant;
        // grab a &mut JoinPath view to manipulate outer/inner subpaths exactly
        // as the C `(JoinPath *) hash_path/nest_path/merge_path` up-cast does.
        let jpath: &mut ::pathnodes::JoinPath = match &mut joinpath {
            PathNode::HashPath(p) => &mut p.jpath,
            PathNode::NestPath(p) => &mut p.jpath,
            PathNode::MergePath(p) => &mut p.jpath,
            _ => unreachable!("joinpath is one of Hash/Nest/Merge by construction"),
        };

        // If either inner or outer path is a ForeignPath corresponding to a
        // pushed-down join, replace it with the fdw_outerpath, so that we
        // maintain a path for EPQ checks built entirely of local join
        // strategies.

        // if (IsA(joinpath->outerjoinpath, ForeignPath))
        if let Some(outer_id) = jpath.outerjoinpath {
            if let PathNode::ForeignPath(foreign_path) = root.path(outer_id) {
                // if (IS_JOIN_REL(foreign_path->path.parent))
                if is_join_rel(root.rel(foreign_path.path.parent)) {
                    // joinpath->outerjoinpath = foreign_path->fdw_outerpath;
                    let fdw_outerpath = foreign_path.fdw_outerpath;
                    jpath.outerjoinpath = fdw_outerpath;

                    // if (joinpath->path.pathtype == T_MergeJoin)
                    if jpath.path.pathtype == T_MergeJoin {
                        // If the new outer path is already well enough ordered
                        // for the mergejoin, we can skip doing an explicit sort.
                        let new_outer_pathkeys: Vec<::pathnodes::PathKey> = fdw_outerpath
                            .map(|id| root.path(id).base().pathkeys.clone())
                            .unwrap_or_default();
                        if let PathNode::MergePath(merge_path) = &mut joinpath {
                            if !merge_path.outersortkeys.is_empty() {
                                let (contained, n) =
                                    pathkeys::pathkeys_count_contained_in(
                                        &merge_path.outersortkeys,
                                        &new_outer_pathkeys,
                                    );
                                merge_path.outer_presorted_keys = n;
                                if contained {
                                    merge_path.outersortkeys = Vec::new();
                                }
                            }
                        }
                    }
                }
            }
        }

        // Re-borrow the JoinPath view for the inner side (the outer block above
        // borrowed `root` immutably, which must end before we touch it again).
        let jpath: &mut ::pathnodes::JoinPath = match &mut joinpath {
            PathNode::HashPath(p) => &mut p.jpath,
            PathNode::NestPath(p) => &mut p.jpath,
            PathNode::MergePath(p) => &mut p.jpath,
            _ => unreachable!("joinpath is one of Hash/Nest/Merge by construction"),
        };

        // if (IsA(joinpath->innerjoinpath, ForeignPath))
        if let Some(inner_id) = jpath.innerjoinpath {
            if let PathNode::ForeignPath(foreign_path) = root.path(inner_id) {
                // if (IS_JOIN_REL(foreign_path->path.parent))
                if is_join_rel(root.rel(foreign_path.path.parent)) {
                    // joinpath->innerjoinpath = foreign_path->fdw_outerpath;
                    let fdw_outerpath = foreign_path.fdw_outerpath;
                    jpath.innerjoinpath = fdw_outerpath;

                    // if (joinpath->path.pathtype == T_MergeJoin)
                    if jpath.path.pathtype == T_MergeJoin {
                        // If the new inner path is already well enough ordered
                        // for the mergejoin, we can skip doing an explicit sort.
                        let new_inner_pathkeys: Vec<::pathnodes::PathKey> = fdw_outerpath
                            .map(|id| root.path(id).base().pathkeys.clone())
                            .unwrap_or_default();
                        if let PathNode::MergePath(merge_path) = &mut joinpath {
                            if !merge_path.innersortkeys.is_empty()
                                && pathkeys::pathkeys_contained_in(
                                    &merge_path.innersortkeys,
                                    &new_inner_pathkeys,
                                )
                            {
                                merge_path.innersortkeys = Vec::new();
                            }
                        }
                    }
                }
            }
        }

        // return (Path *) joinpath; — materialize the shallow copy in the arena
        // and hand back its handle.
        return Some(root.alloc_path(joinpath));
    }
    // return NULL;
    None
}

/* ===========================================================================
 * pg_foreign_* catalog DML + options decode + IMPORT raw-stmt projection.
 *
 * These back the catalog-write/DDL seams `commands/foreigncmds.c` issues
 * against the `pg_foreign_*` catalogs. In PostgreSQL these are inline
 * `heap_form_tuple` + `CatalogTupleInsert`/`Update` / `SearchSysCacheCopy1` /
 * `GetNewOidWithIndex` / `SysCacheGetAttr`-decode sequences inside
 * foreigncmds.c; in the owned tree the C `Datum`/`HeapTuple`/`values[]`/
 * `nulls[]`/`repl_*[]` plumbing belongs to the catalog-access layer, so each
 * catalog-row operation is a single by-value seam here. The relation is opened
 * directly via `backend-access-table-::table::table_open` (mirrors the merged
 * `pg_namespace`/`pg_am` ports); the row OID is assigned via
 * `GetNewOidWithIndex` (direct call into merged `backend-catalog-catalog`);
 * the `heap_form_tuple` + `CatalogTupleInsert`/`Update` value layer crosses
 * the `catalog/indexing.c`-owned `catalog_tuple_{insert,update}_pg_foreign_*`
 * seams (which panic until `indexing.c` lands, exactly as the merged
 * `pg_namespace`/`pg_am`/`pg_enum` inserts/updates do — sanctioned
 * mirror-pg-and-panic).
 * ======================================================================== */

use ::mcx::MemoryContext;
use types_foreigncmds::{
    DefElem, DefElemArg, ForeignDataWrapperRelationId as FdwRelationId,
    ForeignServerRelationId as SrvRelationId, ForeignTableRelationId as FtRelationId,
    UserMappingRelationId as UmRelationId, FdwOwnerRow, FdwUpdateRow, ImportRawStmt, RawStmtHandle,
    ServerOwnerRow, ServerUpdateRow,
};
use types_foreigncmds::{
    Anum_pg_foreign_data_wrapper_oid, Anum_pg_foreign_server_oid, Anum_pg_user_mapping_oid,
    ForeignDataWrapperOidIndexId, ForeignServerOidIndexId, PgForeignDataWrapperInsertRow,
    PgForeignDataWrapperUpdateRow, PgForeignServerInsertRow, PgForeignServerUpdateRow,
    PgForeignTableInsertRow, PgUserMappingInsertRow, PgUserMappingUpdateRow, UserMappingOidIndexId,
};
use ::types_storage::lock::RowExclusiveLock;

use ::table::table_open;
use ::catalog_catalog::GetNewOidWithIndex;
use indexing_seams as indexing;
use pg_depend_seams as pg_depend;
use ::types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_NORMAL};

/// `defGetString(def)` (`commands/define.c`) restricted to the value-node
/// variants a generic FDW option carries. `def->arg == NULL` raises
/// `ERRCODE_SYNTAX_ERROR` "%s requires a parameter". `T_Integer`/`T_Float`/
/// `T_Boolean`/`T_String` render exactly as the C; `T_List` (a qualified name)
/// is `NameListToString` (dotted join), matching the C arm.
fn def_get_string(def: &DefElem<'_>) -> PgResult<String> {
    match &def.arg {
        None => Err(PgError::error(format!(
            "{} requires a parameter",
            def.defname.as_str()
        ))
        .with_sqlstate(ERRCODE_SYNTAX_ERROR)),
        Some(arg) => Ok(match &**arg {
            DefElemArg::Integer(v) => format!("{v}"),
            DefElemArg::Float(s) => s.as_str().to_string(),
            DefElemArg::Boolean(b) => {
                if *b {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            DefElemArg::String(s) => s.as_str().to_string(),
            DefElemArg::NameList(names) => names
                .iter()
                .map(|n| n.as_str())
                .collect::<Vec<_>>()
                .join("."),
        }),
    }
}

/// Render a merged option list to the `(name, value)` pairs the catalog-write
/// carrier stores (the C `optionListToArray` packing of `"name=value"` text
/// varlenas; `value = defGetString(def)`). An empty / `None` option list is
/// the C `PointerGetDatum(NULL)` "store SQL NULL" case → `None`.
fn options_to_pairs(options: Option<&[DefElem<'_>]>) -> PgResult<Option<Vec<(String, String)>>> {
    match options {
        None => Ok(None),
        Some(opts) => {
            let mut pairs = Vec::with_capacity(opts.len());
            for def in opts {
                pairs.push((def.defname.as_str().to_string(), def_get_string(def)?));
            }
            Ok(Some(pairs))
        }
    }
}

/// Decode the raw `*options` `text[]` varlena bytes a syscache projection seam
/// returned into the `DefElem` list `transformGenericOptions` merges against
/// (the C `untransformRelOptions`). A SQL-NULL column (`None`) is the empty
/// list. Mirrors the existing `GetForeignTable`/`GetUserMapping` decode in this
/// crate.
fn untransform_options<'mcx>(
    mcx: Mcx<'mcx>,
    raw: Option<Option<::mcx::PgVec<'mcx, u8>>>,
) -> PgResult<::mcx::PgVec<'mcx, DefElem<'mcx>>> {
    // The projection seam returns `Some(bytes)` when the row was present; a
    // cache miss (`None`) is treated as no options (the caller already validated
    // the object exists). `Some(None)` is the SQL-NULL column.
    let pairs = match raw {
        Some(Some(bytes)) => common_reloptions::untransformRelOptions(
            mcx,
            Some(bytes.as_slice()),
        )?,
        Some(None) | None => Vec::new(),
    };
    let mut out = ::mcx::vec_with_capacity_in(mcx, pairs.len())?;
    for (name, value) in pairs {
        out.push(DefElem {
            defname: PgString::from_str_in(&name, mcx)?,
            arg: match value {
                Some(v) => Some(::mcx::alloc_in(
                    mcx,
                    DefElemArg::String(PgString::from_str_in(&v, mcx)?),
                )?),
                None => None,
            },
            defaction: ::types_foreigncmds::DEFELEM_UNSPEC,
            location: -1,
        });
    }
    Ok(out)
}

/* ---- read/lookup seams (syscache projections) ---- */

/// `SearchSysCacheCopy1(FOREIGNDATAWRAPPERNAME, name)` projected to
/// `(fdwid, fdwvalidator)` (`AlterForeignDataWrapper`'s `fdw_lookup_by_name`).
fn fdw_lookup_by_name(fdwname: &str) -> PgResult<Option<FdwUpdateRow>> {
    let fdwid = syscache::foreign_data_wrapper_oid_by_name::call(fdwname)?;
    if !OidIsValid(fdwid) {
        return Ok(None);
    }
    let ctx = MemoryContext::new("fdw_lookup_by_name");
    let row = match syscache::foreign_data_wrapper_form::call(ctx.mcx(), fdwid)? {
        Some(row) => row,
        None => return Ok(None),
    };
    Ok(Some(FdwUpdateRow {
        fdwid,
        fdwvalidator: row.fdwvalidator,
    }))
}

/// `(fdwid, fdwname, fdwowner)` by FDW name (the FDW owner-change path).
fn fdw_owner_row_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    fdwname: &str,
) -> PgResult<Option<FdwOwnerRow<'mcx>>> {
    let fdwid = syscache::foreign_data_wrapper_oid_by_name::call(fdwname)?;
    if !OidIsValid(fdwid) {
        return Ok(None);
    }
    fdw_owner_row_by_oid(mcx, fdwid)
}

/// `(fdwid, fdwname, fdwowner)` by FDW OID.
fn fdw_owner_row_by_oid<'mcx>(mcx: Mcx<'mcx>, fdwid: Oid) -> PgResult<Option<FdwOwnerRow<'mcx>>> {
    let row = match syscache::foreign_data_wrapper_form::call(mcx, fdwid)? {
        Some(row) => row,
        None => return Ok(None),
    };
    Ok(Some(FdwOwnerRow {
        fdwid,
        fdwname: row.fdwname,
        fdwowner: row.fdwowner,
    }))
}

/// `SysCacheGetAttr(FOREIGNDATAWRAPPEROID, fdwid, fdwoptions)` decoded into a
/// `DefElem` list (NULL → empty).
fn fdw_options<'mcx>(mcx: Mcx<'mcx>, fdwid: Oid) -> PgResult<::mcx::PgVec<'mcx, DefElem<'mcx>>> {
    let raw = syscache::foreign_data_wrapper_options::call(mcx, fdwid)?;
    untransform_options(mcx, raw)
}

/// `(serverid, srvfdw)` by server name (`AlterForeignServer`).
fn server_lookup_by_name(servername: &str) -> PgResult<Option<ServerUpdateRow>> {
    let serverid = syscache::foreign_server_oid_by_name::call(servername)?;
    if !OidIsValid(serverid) {
        return Ok(None);
    }
    let ctx = MemoryContext::new("server_lookup_by_name");
    let row = match syscache::foreign_server_form::call(ctx.mcx(), serverid)? {
        Some(row) => row,
        None => return Ok(None),
    };
    Ok(Some(ServerUpdateRow {
        serverid,
        srvfdw: row.srvfdw,
    }))
}

/// `(serverid, srvname, srvowner, srvfdw)` by server name.
fn server_owner_row_by_name<'mcx>(
    mcx: Mcx<'mcx>,
    servername: &str,
) -> PgResult<Option<ServerOwnerRow<'mcx>>> {
    let serverid = syscache::foreign_server_oid_by_name::call(servername)?;
    if !OidIsValid(serverid) {
        return Ok(None);
    }
    server_owner_row_by_oid(mcx, serverid)
}

/// `(serverid, srvname, srvowner, srvfdw)` by server OID.
fn server_owner_row_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    serverid: Oid,
) -> PgResult<Option<ServerOwnerRow<'mcx>>> {
    let row = match syscache::foreign_server_form::call(mcx, serverid)? {
        Some(row) => row,
        None => return Ok(None),
    };
    Ok(Some(ServerOwnerRow {
        serverid,
        srvname: row.srvname,
        srvowner: row.srvowner,
        srvfdw: row.srvfdw,
    }))
}

/// `SysCacheGetAttr(FOREIGNSERVEROID, serverid, srvoptions)` → `DefElem` list.
fn server_options<'mcx>(
    mcx: Mcx<'mcx>,
    serverid: Oid,
) -> PgResult<::mcx::PgVec<'mcx, DefElem<'mcx>>> {
    let raw = syscache::foreign_server_options::call(mcx, serverid)?;
    untransform_options(mcx, raw)
}

/// `GetSysCacheOid2(USERMAPPINGUSERSERVER, useid, serverid)` → the mapping OID,
/// or `InvalidOid`.
fn usermapping_oid(useid: Oid, serverid: Oid) -> PgResult<Oid> {
    let ctx = MemoryContext::new("usermapping_oid");
    let found = syscache::user_mapping_form::call(ctx.mcx(), useid, serverid)?;
    match found {
        Some((umid, _)) => Ok(umid),
        None => Ok(InvalidOid),
    }
}

/// `SysCacheGetAttr(USERMAPPINGUSERSERVER, umid, umoptions)` → `DefElem` list.
fn usermapping_options<'mcx>(
    mcx: Mcx<'mcx>,
    umid: Oid,
) -> PgResult<::mcx::PgVec<'mcx, DefElem<'mcx>>> {
    let raw = syscache::user_mapping_options_by_oid::call(mcx, umid)?;
    untransform_options(mcx, raw)
}

/* ---- FDW options validator (transformGenericOptions tail) ---- */

/// `OidFunctionCall2(fdwvalidator, optionsArray, ObjectIdGetDatum(catalogId))`
/// — run the FDW options validator on the merged option list. C builds a
/// `text[]` `Datum` from the option list (`optionListToArray`, packing
/// `"name=value"` text varlenas; an empty list → an empty array so the
/// validator need not be non-strict), then `OidFunctionCall2`s the validator.
///
/// The owned tree lowers the array onto the fmgr by-reference lane via the
/// canonical `ByRef` `Datum`: `construct_text_array_bytes` packs the
/// `"name=value"` elements into the flat array varlena image (the same bytes
/// a `RefPayload::Varlena` carries), and `function_call2_coll_datum` resolves
/// the validator by OID and dispatches with the array as arg0 (by-reference)
/// and `ObjectIdGetDatum(catalogId)` as arg1 (by-value). The validator
/// (`postgresql_fdw_validator` and any builtin/SQL validator) reads arg0 back
/// with `untransformRelOptions(PG_GETARG_DATUM(0))`, exactly as C does. A
/// validator `ereport(ERROR)` is carried back on `Err`.
fn validate_options(fdwvalidator: Oid, options: &[DefElem<'_>], catalog_id: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("validate_options");
    let mcx = ctx.mcx();

    // optionListToArray(options): each option packed as "name=value" text.
    // `value = defGetString(def)` (the existing options_to_pairs helper).
    let pairs = options_to_pairs(Some(options))?.unwrap_or_default();
    let elems: Vec<String> = pairs
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect();
    let elem_refs: Vec<&str> = elems.iter().map(|s| s.as_str()).collect();
    // An empty list → construct_empty_array(TEXTOID), matching C's "pass a null
    // options list as an empty array" so the validator need not be non-strict.
    let array_image =
        arrayfuncs_seams::construct_text_array_bytes::call(mcx, &elem_refs)?;

    // OidFunctionCall2(fdwvalidator, PointerGetDatum(array), ObjectIdGetDatum(catalogId)).
    // arg0 crosses on the by-reference lane (ByRef text[] image); arg1 by value.
    let arg0 = Datum::ByRef(array_image);
    let arg1 = Datum::ByVal(catalog_id as usize);
    fmgr::function_call2_coll_datum::call(mcx, fdwvalidator, InvalidOid, arg0, arg1)?;
    Ok(())
}

/* ---- catalog inserts (open rel + GetNewOidWithIndex + indexing insert) ---- */

/// `CreateForeignDataWrapper`'s `pg_foreign_data_wrapper` row insert.
fn insert_fdw(
    fdwname: &str,
    owner: Oid,
    handler: Oid,
    validator: Oid,
    options: Option<&[DefElem<'_>]>,
) -> PgResult<Oid> {
    let ctx = MemoryContext::new("insert_fdw");
    let rel = table_open(ctx.mcx(), FdwRelationId, RowExclusiveLock)?;
    let fdw_id = GetNewOidWithIndex(
        &rel,
        ForeignDataWrapperOidIndexId,
        Anum_pg_foreign_data_wrapper_oid,
    )?;
    let row = PgForeignDataWrapperInsertRow {
        oid: fdw_id,
        fdwname: fdwname.to_string(),
        fdwowner: owner,
        fdwhandler: handler,
        fdwvalidator: validator,
        options: options_to_pairs(options)?,
    };
    indexing::catalog_tuple_insert_pg_foreign_data_wrapper::call(&rel, &row)?;
    rel.close(RowExclusiveLock)?;
    Ok(fdw_id)
}

/// `CreateForeignServer`'s `pg_foreign_server` row insert.
fn insert_server(
    servername: &str,
    owner: Oid,
    fdwid: Oid,
    servertype: Option<&str>,
    version: Option<&str>,
    options: Option<&[DefElem<'_>]>,
) -> PgResult<Oid> {
    let ctx = MemoryContext::new("insert_server");
    let rel = table_open(ctx.mcx(), SrvRelationId, RowExclusiveLock)?;
    let srv_id = GetNewOidWithIndex(&rel, ForeignServerOidIndexId, Anum_pg_foreign_server_oid)?;
    let row = PgForeignServerInsertRow {
        oid: srv_id,
        srvname: servername.to_string(),
        srvowner: owner,
        srvfdw: fdwid,
        srvtype: servertype.map(|s| s.to_string()),
        srvversion: version.map(|s| s.to_string()),
        options: options_to_pairs(options)?,
    };
    indexing::catalog_tuple_insert_pg_foreign_server::call(&rel, &row)?;
    rel.close(RowExclusiveLock)?;
    Ok(srv_id)
}

/// `CreateUserMapping`'s `pg_user_mapping` row insert.
fn insert_usermapping(
    useid: Oid,
    serverid: Oid,
    options: Option<&[DefElem<'_>]>,
) -> PgResult<Oid> {
    let ctx = MemoryContext::new("insert_usermapping");
    let rel = table_open(ctx.mcx(), UmRelationId, RowExclusiveLock)?;
    let um_id = GetNewOidWithIndex(&rel, UserMappingOidIndexId, Anum_pg_user_mapping_oid)?;
    let row = PgUserMappingInsertRow {
        oid: um_id,
        umuser: useid,
        umserver: serverid,
        options: options_to_pairs(options)?,
    };
    indexing::catalog_tuple_insert_pg_user_mapping::call(&rel, &row)?;
    rel.close(RowExclusiveLock)?;
    Ok(um_id)
}

/// `CreateForeignTable`'s `pg_foreign_table` row insert + the pg_class →
/// pg_foreign_server dependency the C records afterwards (the consumer
/// delegates both into this seam). `pg_foreign_table` has no OID column.
fn insert_foreign_table(
    relid: Oid,
    serverid: Oid,
    options: Option<&[DefElem<'_>]>,
) -> PgResult<()> {
    let ctx = MemoryContext::new("insert_foreign_table");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, FtRelationId, RowExclusiveLock)?;
    let row = PgForeignTableInsertRow {
        ftrelid: relid,
        ftserver: serverid,
        options: options_to_pairs(options)?,
    };
    indexing::catalog_tuple_insert_pg_foreign_table::call(&rel, &row)?;
    rel.close(RowExclusiveLock)?;

    /* Add pg_class dependency on the server. */
    let myself = ObjectAddress {
        classId: ::types_foreigncmds::RelationRelationId,
        objectId: relid,
        objectSubId: 0,
    };
    let referenced = ObjectAddress {
        classId: SrvRelationId,
        objectId: serverid,
        objectSubId: 0,
    };
    pg_depend::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;
    Ok(())
}

/* ---- catalog updates (open rel + indexing update seam) ---- */

/// `AlterForeignDataWrapper`'s tuple update.
fn update_fdw(
    fdwid: Oid,
    handler: Option<Oid>,
    validator: Option<Oid>,
    options: Option<Option<&[DefElem<'_>]>>,
) -> PgResult<()> {
    let ctx = MemoryContext::new("update_fdw");
    let rel = table_open(ctx.mcx(), FdwRelationId, RowExclusiveLock)?;
    let row = PgForeignDataWrapperUpdateRow {
        handler,
        validator,
        options: match options {
            None => None,
            Some(inner) => Some(options_to_pairs(inner)?),
        },
    };
    indexing::catalog_tuple_update_pg_foreign_data_wrapper::call(&rel, fdwid, &row)?;
    rel.close(RowExclusiveLock)
}

/// `AlterForeignServer`'s tuple update.
fn update_server(
    serverid: Oid,
    version: Option<Option<&str>>,
    options: Option<Option<&[DefElem<'_>]>>,
) -> PgResult<()> {
    let ctx = MemoryContext::new("update_server");
    let rel = table_open(ctx.mcx(), SrvRelationId, RowExclusiveLock)?;
    let row = PgForeignServerUpdateRow {
        version: version.map(|v| v.map(|s| s.to_string())),
        options: match options {
            None => None,
            Some(inner) => Some(options_to_pairs(inner)?),
        },
    };
    indexing::catalog_tuple_update_pg_foreign_server::call(&rel, serverid, &row)?;
    rel.close(RowExclusiveLock)
}

/// `AlterUserMapping`'s tuple update.
fn update_usermapping(umid: Oid, options: Option<Option<&[DefElem<'_>]>>) -> PgResult<()> {
    let ctx = MemoryContext::new("update_usermapping");
    let rel = table_open(ctx.mcx(), UmRelationId, RowExclusiveLock)?;
    let row = PgUserMappingUpdateRow {
        options: match options {
            None => None,
            Some(inner) => Some(options_to_pairs(inner)?),
        },
    };
    indexing::catalog_tuple_update_pg_user_mapping::call(&rel, umid, &row)?;
    rel.close(RowExclusiveLock)
}

/// `AlterForeignDataWrapperOwner_internal`'s tuple update.
fn fdw_set_owner(fdwid: Oid, old_owner: Oid, new_owner: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("fdw_set_owner");
    let rel = table_open(ctx.mcx(), FdwRelationId, RowExclusiveLock)?;
    indexing::catalog_tuple_update_owner_pg_foreign_data_wrapper::call(
        &rel, fdwid, old_owner, new_owner,
    )?;
    rel.close(RowExclusiveLock)
}

/// `AlterForeignServerOwner_internal`'s tuple update.
fn server_set_owner(serverid: Oid, old_owner: Oid, new_owner: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("server_set_owner");
    let rel = table_open(ctx.mcx(), SrvRelationId, RowExclusiveLock)?;
    indexing::catalog_tuple_update_owner_pg_foreign_server::call(
        &rel, serverid, old_owner, new_owner,
    )?;
    rel.close(RowExclusiveLock)
}

/* ---- IMPORT FOREIGN SCHEMA raw-stmt projection ---- */

/// Project one `RawStmt *` the IMPORT loop received into the fields the command
/// driver branches on. The raw parse tree (`RawStmtHandle`) is an unported
/// parser node with no installed field-accessor seam, so this reaches the
/// unported parser-node accessor and seam-and-panics. It is reached only after
/// `fdw_import_foreign_schema` (a runtime FDW vtable dispatch, no provider
/// ported) returns commands, so the path is already unreachable at runtime; the
/// DDL seam is installed here so it leaves CONTRACT_RECONCILE_PENDING.
fn import_classify_raw_stmt(_raw: RawStmtHandle) -> PgResult<ImportRawStmt> {
    panic!(
        "import_classify_raw_stmt: reading a RawStmt parser node's \
         nodeTag/relname/stmt_location/stmt_len/stmt requires the unported \
         parser-node field accessor — seam-and-panic until it lands"
    );
}

/// `cstmt->base.relation->schemaname = pstrdup(local_schema)` — mutate the
/// unported parser node in place. Same unported parser-node accessor as
/// [`import_classify_raw_stmt`]; seam-and-panic.
fn import_set_schemaname(_raw: RawStmtHandle, _local_schema: &str) -> PgResult<()> {
    panic!(
        "import_set_schemaname: rewriting the embedded \
         CreateForeignTableStmt's RangeVar schemaname requires the unported \
         parser-node accessor — seam-and-panic until it lands"
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
    // plancat.c outward seams owned by foreign.c (FDW catalog/GUC reads).
    plancat_ext_seams::get_foreign_server_id_by_rel_id::set(
        GetForeignServerIdByRelId,
    );
    plancat_ext_seams::foreign_table_access_restricted::set(|| {
        (guc::restrict_nonsystem_relation_kind::call() & RESTRICT_RELKIND_FOREIGN_TABLE) != 0
    });
    // `GetFdwRoutineForRelation(relation, true) != NULL` (plancat.c). The C
    // resolves the FDW routine (raising if the wrapper has no handler) and
    // stores presence in `RelOptInfo::has_fdwroutine`; it is never NULL on
    // success, so a clean resolve yields `true`.
    plancat_ext_seams::rel_has_fdwroutine::set(|relid| {
        let ctx = ::mcx::MemoryContext::new("rel_has_fdwroutine");
        GetFdwRoutineForRelation(ctx.mcx(), relid)?;
        Ok(true)
    });

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

    inward::get_foreign_data_wrapper_extended::set(|mcx, fdwid, missing_ok| {
        let flags = if missing_ok { FDW_MISSING_OK } else { 0 };
        GetForeignDataWrapperExtended(mcx, fdwid, flags)
    });

    inward::get_foreign_data_wrapper_by_name::set(GetForeignDataWrapperByName);

    inward::foreign_table_server_oid::set(|relid| {
        // SearchSysCache1(FOREIGNTABLEREL, relid) → ftserver; None when the
        // foreign table has no pg_foreign_table row.
        syscache::foreign_table_server_by_relid::call(relid)
    });

    inward::foreign_table_options::set(|relid| foreign_table_options_impl(relid));

    inward::get_foreign_server::set(|mcx, serverid| {
        // GetForeignServer raises (Err) on a missing server (flags = 0); the
        // inward seam returns the descriptor by value, so unwrap the `Some`.
        match GetForeignServer(mcx, serverid)? {
            Some(srv) => Ok(srv),
            None => Err(elog_error(format!(
                "cache lookup failed for foreign server {}",
                serverid
            ))),
        }
    });

    inward::get_foreign_server_extended::set(|mcx, serverid, missing_ok| {
        let flags = if missing_ok { FSV_MISSING_OK } else { 0 };
        GetForeignServerExtended(mcx, serverid, flags)
    });

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
        let ctx = ::mcx::MemoryContext::new("GetFdwRoutineForRelation");
        GetFdwRoutineForRelation(ctx.mcx(), relid)
    });

    inward::get_fdw_routine_by_server_id::set(|serverid| {
        let ctx = ::mcx::MemoryContext::new("GetFdwRoutineByServerId");
        GetFdwRoutineByServerId(ctx.mcx(), serverid)
    });

    /* ----- pg_foreign_* catalog DML + options decode + IMPORT seams ----- */
    /* foreigncmds.c issues these against the pg_foreign_* catalogs; the
     * heap_form_tuple + CatalogTupleInsert/Update value layer crosses the
     * catalog/indexing.c seams (which panic until indexing.c lands). */
    inward::insert_fdw::set(insert_fdw);
    inward::update_fdw::set(update_fdw);
    inward::fdw_set_owner::set(fdw_set_owner);
    inward::fdw_lookup_by_name::set(fdw_lookup_by_name);
    inward::fdw_owner_row_by_name::set(fdw_owner_row_by_name);
    inward::fdw_owner_row_by_oid::set(fdw_owner_row_by_oid);
    inward::fdw_options::set(fdw_options);

    inward::insert_server::set(insert_server);
    inward::update_server::set(update_server);
    inward::server_set_owner::set(server_set_owner);
    inward::server_lookup_by_name::set(server_lookup_by_name);
    inward::server_owner_row_by_name::set(server_owner_row_by_name);
    inward::server_owner_row_by_oid::set(server_owner_row_by_oid);
    inward::server_options::set(server_options);

    inward::insert_usermapping::set(insert_usermapping);
    inward::update_usermapping::set(update_usermapping);
    inward::usermapping_oid::set(usermapping_oid);
    inward::usermapping_options::set(usermapping_options);

    inward::insert_foreign_table::set(insert_foreign_table);

    inward::validate_options::set(validate_options);
    inward::import_classify_raw_stmt::set(import_classify_raw_stmt);
    inward::import_set_schemaname::set(import_set_schemaname);

    // Register the foreign.c fmgr builtins (postgresql_fdw_validator) so the
    // OidFunctionCall2(fdwvalidator, ...) dispatch from validate_options
    // resolves the validator by OID.
    fmgr_builtins::register_foreign_builtins();
}

mod fmgr_builtins;

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
        list_type: ::types_foreigncmds::ImportForeignSchemaType,
        names: &[&str],
    ) -> PgResult<ImportForeignSchemaStmt<'mcx>> {
        let mut table_list = ::mcx::vec_with_capacity_in(mcx, names.len())?;
        for n in names {
            table_list.push(PgString::from_str_in(n, mcx)?);
        }
        Ok(ImportForeignSchemaStmt {
            server_name: PgString::from_str_in("srv", mcx)?,
            remote_schema: PgString::from_str_in("rs", mcx)?,
            local_schema: PgString::from_str_in("ls", mcx)?,
            list_type,
            table_list,
            options: ::mcx::vec_with_capacity_in(mcx, 0)?,
        })
    }

    #[test]
    fn is_importable_filters() {
        let ctx = ::mcx::MemoryContext::new("test");
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
