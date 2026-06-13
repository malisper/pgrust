#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `backend/commands/amcmds.c` — routines for SQL commands that manipulate
//! access methods (`CREATE ACCESS METHOD`, plus the `get_*_am_oid` /
//! `get_am_name` lookup helpers).
//!
//! Every C function — public and file-static — is present with its original
//! name, branch order, error codes / messages / SQLSTATE, dependency-recording
//! order, and post-create hook invocation preserved:
//!
//!   * [`CreateAccessMethod`] — the `CREATE ACCESS METHOD` command driver;
//!   * `get_am_type_oid` — the shared name→OID worker, and its three public
//!     wrappers [`get_index_am_oid`] / [`get_table_am_oid`] / [`get_am_oid`];
//!   * [`get_am_name`] — OID→name;
//!   * `get_am_type_string` — the single-character AM type → display string;
//!   * `lookup_am_handler_func` — handler-name → OID, with return-type check.
//!
//! Genuine externals cross owner seams: the superuser permission check
//! (`superuser()` → miscinit), the `pg_am` syscache lookups (`GetSysCacheOid1`
//! / `SearchSysCache1` → syscache), the `pg_am` object-CREATE tuple insert
//! (`table_open` → the table-access crate directly; `GetNewOidWithIndex` /
//! `heap_form_tuple` / `CatalogTupleInsert` → indexing), the fmgr/`Datum` layer
//! of `lookup_am_handler_func` (`LookupFuncName` → parse_func;
//! `get_func_rettype` / `get_func_name` → lsyscache; `format_type_extended`
//! (= `format_type_be`) → format_type), dependency recording
//! (`recordDependencyOn` / `recordDependencyOnCurrentExtension` → pg_depend),
//! and the post-create object-access hook (`InvokeObjectPostCreateHook` →
//! objectaccess).

use mcx::{Mcx, PgString};
use types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_NORMAL};

use backend_utils_error::{ereport, PgResult};
use types_error::{
    ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_FUNCTION, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};

use types_catalog::catalog::{ACCESS_METHOD_RELATION_ID, PROCEDURE_RELATION_ID};
use types_core::{
    InvalidOid, Oid, OidIsValid, INDEX_AM_HANDLEROID, INTERNALOID, TABLE_AM_HANDLEROID,
};
use types_nodes::parsenodes::CreateAmStmt;
use types_storage::lock::RowExclusiveLock;

use backend_access_table_table::{table_close, table_open};
use backend_catalog_indexing_seams::catalog_tuple_insert_pg_am;
use backend_catalog_objectaccess_seams::invoke_object_post_create_hook;
use backend_catalog_pg_depend_seams::{recordDependencyOn, recordDependencyOnCurrentExtension};
use backend_parser_parse_func_seams::lookup_func_name;
use backend_utils_adt_format_type_seams::format_type_be;
use backend_utils_cache_lsyscache_seams::{get_func_name, get_func_rettype};
use backend_utils_cache_syscache_seams::{get_am_oid_by_name, search_am_by_name, search_am_name};
use backend_utils_init_miscinit_seams::superuser;

/// `AMTYPE_INDEX` — index access method (`catalog/pg_am.h`).
const AMTYPE_INDEX: u8 = b'i';
/// `AMTYPE_TABLE` — table access method (`catalog/pg_am.h`).
const AMTYPE_TABLE: u8 = b't';

/// `'\0'` — the "any AM type" sentinel `get_am_oid` passes to `get_am_type_oid`.
const NO_AMTYPE: u8 = 0;

/*
 * CreateAccessMethod
 *		Registers a new access method.
 */
pub fn CreateAccessMethod(mcx: Mcx<'_>, stmt: &CreateAmStmt) -> PgResult<ObjectAddress> {
    let amoid: Oid;
    let amhandler: Oid;

    // `stmt->amname` is `char *` in C (always present for `CREATE ACCESS
    // METHOD`); the owned node models it as `Option<String>`.
    let amname: &str = stmt.amname.as_deref().unwrap_or("");

    let rel = table_open(mcx, ACCESS_METHOD_RELATION_ID, RowExclusiveLock)?;

    /* Must be superuser */
    if !superuser::call(mcx)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied to create access method \"{amname}\""
            ))
            .errhint("Must be superuser to create an access method.")
            .into_error());
    }

    /* Check if name is used */
    amoid = get_am_oid_by_name::call(amname)?;
    if OidIsValid(amoid) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("access method \"{amname}\" already exists"))
            .into_error());
    }

    /*
     * Get the handler function oid, verifying the AM type while at it.
     */
    amhandler = lookup_am_handler_func(mcx, &stmt.handler_name, stmt.amtype)?;

    /*
     * Insert tuple into pg_am.
     *
     * memset(values/nulls); GetNewOidWithIndex; the values[] fill
     * (namein(amname), amhandler, amtype); heap_form_tuple; CatalogTupleInsert;
     * heap_freetuple — performed as one catalog operation, returning amoid.
     */
    let amoid = catalog_tuple_insert_pg_am::call(&rel, amname, amhandler, stmt.amtype)?;

    let myself = ObjectAddress {
        classId: ACCESS_METHOD_RELATION_ID,
        objectId: amoid,
        objectSubId: 0,
    };

    /* Record dependency on handler function */
    let referenced = ObjectAddress {
        classId: PROCEDURE_RELATION_ID,
        objectId: amhandler,
        objectSubId: 0,
    };

    recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;

    recordDependencyOnCurrentExtension::call(mcx, &myself, false)?;

    invoke_object_post_create_hook::call(ACCESS_METHOD_RELATION_ID, amoid, 0)?;

    table_close(rel, RowExclusiveLock)?;

    Ok(myself)
}

/*
 * get_am_type_oid
 *		Worker for various get_am_*_oid variants
 *
 * If missing_ok is false, throw an error if access method not found.  If true,
 * just return InvalidOid.
 *
 * If amtype is not '\0', an error is raised if the AM found is not of the given
 * type.
 */
fn get_am_type_oid(mcx: Mcx<'_>, amname: &str, amtype: u8, missing_ok: bool) -> PgResult<Oid> {
    let mut oid: Oid = InvalidOid;

    let tup = search_am_by_name::call(mcx, amname)?;
    if let Some(amform) = tup {
        if amtype != NO_AMTYPE && amform.amtype != amtype {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "access method \"{}\" is not of type {}",
                    amform.amname.as_str(),
                    get_am_type_string(amtype)?
                ))
                .into_error());
        }

        oid = amform.oid;
        /* ReleaseSysCache(tup) — performed within the seam's lookup. */
    }

    if !OidIsValid(oid) && !missing_ok {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("access method \"{amname}\" does not exist"))
            .into_error());
    }
    Ok(oid)
}

/*
 * get_index_am_oid - given an access method name, look up its OID
 *		and verify it corresponds to an index AM.
 */
pub fn get_index_am_oid(mcx: Mcx<'_>, amname: &str, missing_ok: bool) -> PgResult<Oid> {
    get_am_type_oid(mcx, amname, AMTYPE_INDEX, missing_ok)
}

/*
 * get_table_am_oid - given an access method name, look up its OID
 *		and verify it corresponds to a table AM.
 */
pub fn get_table_am_oid(mcx: Mcx<'_>, amname: &str, missing_ok: bool) -> PgResult<Oid> {
    get_am_type_oid(mcx, amname, AMTYPE_TABLE, missing_ok)
}

/*
 * get_am_oid - given an access method name, look up its OID.
 *		The type is not checked.
 */
pub fn get_am_oid(mcx: Mcx<'_>, amname: &str, missing_ok: bool) -> PgResult<Oid> {
    get_am_type_oid(mcx, amname, NO_AMTYPE, missing_ok)
}

/*
 * get_am_name - given an access method OID, look up its name.
 *
 * Returns `None` (the C `NULL`) when the OID has no `pg_am` tuple.
 */
pub fn get_am_name<'mcx>(mcx: Mcx<'mcx>, amOid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    let mut result: Option<PgString<'mcx>> = None; /* char *result = NULL; */

    // `SearchSysCache1(AMOID, ObjectIdGetDatum(amOid))` + (when valid)
    // `pstrdup(NameStr(amform->amname))`; the syscache projection seam does the
    // GETSTRUCT + name copy into `mcx` and owns the `ReleaseSysCache`. A cache
    // miss (`!HeapTupleIsValid`) returns `Ok(None)`, leaving `result` NULL.
    if let Some(amname) = search_am_name::call(mcx, amOid)? {
        result = Some(amname);
    }
    Ok(result)
}

/*
 * Convert single-character access method type into string for error reporting.
 */
fn get_am_type_string(amtype: u8) -> PgResult<&'static str> {
    match amtype {
        AMTYPE_INDEX => Ok("INDEX"),
        AMTYPE_TABLE => Ok("TABLE"),
        _ => {
            /* shouldn't happen */
            Err(ereport(ERROR)
                .errmsg_internal(format!("invalid access method type '{}'", amtype as char))
                .into_error())
        }
    }
}

/*
 * Convert a handler function name to an Oid.  If the return type of the
 * function doesn't match the given AM type, an error is raised.
 *
 * This function either return valid function Oid or throw an error.
 */
fn lookup_am_handler_func(mcx: Mcx<'_>, handler_name: &[String], amtype: u8) -> PgResult<Oid> {
    let handlerOid: Oid;
    let funcargtypes: [Oid; 1] = [INTERNALOID];
    let expectedType: Oid;

    // `if (handler_name == NIL)` — the owned tree models the `List *` as a
    // `Vec<String>`; the empty list is the NIL case.
    if handler_name.is_empty() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg("handler function is not specified")
            .into_error());
    }

    /* handlers have one argument of type internal */
    let funcname: Vec<PgString<'_>> = handler_name
        .iter()
        .map(|s| PgString::from_str_in(s, mcx))
        .collect::<PgResult<Vec<_>>>()?;
    handlerOid = lookup_func_name::call(&funcname, 1, &funcargtypes, false)?;

    /* check that handler has the correct return type */
    match amtype {
        AMTYPE_INDEX => {
            expectedType = INDEX_AM_HANDLEROID;
        }
        AMTYPE_TABLE => {
            expectedType = TABLE_AM_HANDLEROID;
        }
        _ => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!(
                    "unrecognized access method type \"{}\"",
                    amtype as char
                ))
                .into_error());
        }
    }

    if get_func_rettype::call(handlerOid)? != expectedType {
        let funcname = get_func_name::call(mcx, handlerOid)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "function {} must return type {}",
                funcname,
                format_type_be::call(mcx, expectedType)?.as_str()
            ))
            .into_error());
    }

    Ok(handlerOid)
}

/// Install every seam this crate owns. amcmds owns the inward seam
/// `get_index_am_oid` (declared in `backend-commands-amcmds-seams`, called by
/// the unported `opclasscmds`/`indexcmds`/`tablecmds` callers across a cycle).
pub fn init_seams() {
    backend_commands_amcmds_seams::get_index_am_oid::set(|amname, missing_ok| {
        // The inward seam returns the (Copy) `Oid`; `get_index_am_oid` only
        // needs an `Mcx` for the wrong-type error message's transient name
        // copy, which is materialized into the `PgError` before this scratch
        // context drops.
        let scratch = mcx::MemoryContext::new("amcmds get_index_am_oid");
        get_index_am_oid(scratch.mcx(), amname, missing_ok)
    });
}
