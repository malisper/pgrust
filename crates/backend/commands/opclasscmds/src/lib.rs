#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

//! `backend/commands/opclasscmds.c` — CREATE/ALTER OPERATOR CLASS & OPERATOR
//! FAMILY support (PostgreSQL 18.3).
//!
//! Every C function is implemented in-crate with identical branch order,
//! item-type dispatch, strategy/support-proc validity checks, error
//! codes/messages/SQLSTATEs, and dependency-classification semantics. The
//! catalog DML (`table_open`, the `pg_opfamily`/`pg_opclass`/`pg_amop`/
//! `pg_amproc` tuple inserts, the default-opclass scan, the `pg_depend` /
//! `pg_shdepend` / extension dependency records, and the event-trigger /
//! object-access hooks) crosses to its owners through their `-seams` crates;
//! the orchestration (which rows, which dependencies, in which order) stays
//! here.

use mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgVec};
use utils_error::ereport;

use amapi_seams::{am_adjust_members, get_index_am_info};
use genam_seams as genam_seams;
use aclchk_seams::{aclcheck_error, object_aclcheck};
use catalog_seams::is_pinned_object;
use dependency_seams::perform_deletion;
use indexing_seams::{
    catalog_tuple_insert_pg_amop, catalog_tuple_insert_pg_amproc,
    catalog_tuple_insert_pg_opclass, catalog_tuple_insert_pg_opfamily,
};
use catalog_namespace::{
    DeconstructQualifiedName, LookupExplicitNamespace, NameListToString, OpclassnameGetOpcid,
    OpfamilynameGetOpfid, QualifiedNameGetCreationNamespace,
};
use objectaccess_seams::{object_access_hook_present, run_object_post_create_hook};
use pg_depend_seams::{recordDependencyOn, recordDependencyOnCurrentExtension};
use pg_shdepend_seams::recordDependencyOnOwner;
use amcmds_seams::get_index_am_oid;
use event_trigger_seams::{
    event_trigger_collect_alter_opfam, event_trigger_collect_create_opclass,
    event_trigger_collect_simple_command,
};
use parse_func_seams::lookup_func_with_args;
use parse_oper_seams::{lookup_oper_name, lookup_oper_with_args};
use parse_type_seams::{typename_to_string, typename_type_id};
use format_type_seams::format_type_be;
use lsyscache_seams::{
    get_am_name, get_func_signature, op_input_types,
};
use syscache_seams::{
    amop_oid, amproc_oid, get_opfamily_oid as syscache_opfamily_oid,
    opclass_exists, opfamily_exists, oper_row_by_oid, proc_row_by_oid,
};
use miscinit_seams::{get_user_id, superuser_arg};

use types_acl::{AclMode, ACLCHECK_OK, ACL_CREATE};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_core::fmgr::F_OIDEQ;
use scankey::ScanKeyInit;
// All in-crate Datum traffic — including the `ScanKeyData.sk_argument`
// scan-key edge stamped by `ScanKeyInit` — uses the canonical unified
// `types_tuple::Datum<'mcx>` enum (`heaptuple::Datum`).
use heaptuple::heap_deform_tuple;
use types_tuple::heaptuple::ItemPointerData;
use types_catalog::catalog::{
    ACCESS_METHOD_OPERATOR_RELATION_ID, ACCESS_METHOD_PROCEDURE_RELATION_ID,
    ACCESS_METHOD_RELATION_ID, NAMESPACE_RELATION_ID, OPERATOR_CLASS_RELATION_ID,
    OPERATOR_FAMILY_RELATION_ID, OPERATOR_RELATION_ID, PROCEDURE_RELATION_ID, TYPE_RELATION_ID,
};
use types_catalog::catalog_dependency::{
    ObjectAddress, DEPENDENCY_AUTO, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL,
};
use types_catalog::opclasscmds_catalog::{
    Anum_pg_opclass_opcdefault, Anum_pg_opclass_opcintype, Anum_pg_opclass_opcmethod,
    Anum_pg_opclass_opcname,
    FormData_pg_amop, FormData_pg_amproc,
    FormData_pg_opclass, FormData_pg_opfamily, OpclassAmNameNspIndexId,
};
use types_core::primitive::{Oid, OidIsValid};
use types_core::primitive::InvalidOid;
use types_core::catalog::{
    BOOLOID, BTREE_AM_OID, INT4OID, INT8OID, INTERNALOID, VOIDOID,
};
use types_error::PgResult;
use ::nodes::parsenodes::OBJECT_SCHEMA;
use opclass::{
    AlterOpFamilyStmt, CreateOpClassItem, CreateOpClassStmt, CreateOpFamilyStmt,
    ObjectWithArgs, OpFamilyMember, StringNode, TypeName, AMOP_ORDER, AMOP_SEARCH,
    OPCLASS_ITEM_FUNCTION, OPCLASS_ITEM_OPERATOR, OPCLASS_ITEM_STORAGETYPE,
};
use types_tuple::heaptuple::Datum;

use types_error::{
    ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT, ERROR,
};

// ---------------------------------------------------------------------------
// Local proc-number constants (verified against access/nbtree.h, access/hash.h).
// ---------------------------------------------------------------------------
const BTORDER_PROC: i32 = 1; // access/nbtree.h
const BTSORTSUPPORT_PROC: i32 = 2;
const BTINRANGE_PROC: i32 = 3;
const BTEQUALIMAGE_PROC: i32 = 4;
const BTSKIPSUPPORT_PROC: i32 = 6;
const HASHSTANDARD_PROC: i32 = 1; // access/hash.h
const HASHEXTENDED_PROC: i32 = 2;

/// `SHRT_MAX` (`<limits.h>`) — the cap applied when an AM's `amstrategies` is 0.
const SHRT_MAX: i32 = 32767;

mod convert;

/// `pub fn init_seams()` — opclasscmds owns the full-name-list
/// `get_opclass_oid` / `get_opfamily_oid` lookup seams (the `objectaddress.c`
/// `get_object_address_opcf` resolution path's callees), plus the
/// `ProcessUtilitySlow` (`utility.c`) dispatch seams for CREATE OPERATOR
/// CLASS/FAMILY and ALTER OPERATOR FAMILY.
pub fn init_seams() {
    use utility_out_seams as rt;

    opclasscmds_seams::get_opclass_oid::set(seam_get_opclass_oid);
    opclasscmds_seams::get_opfamily_oid::set(seam_get_opfamily_oid);

    // ProcessUtilitySlow dispatch (utility.c): the C `castNode(...)` is the
    // runtime tag assert, mirrored here as the parse-node accessor miss.
    rt::define_op_class::set(|mcx, parsetree| match parsetree.as_createopclassstmt() {
        Some(stmt) => {
            let resolved = convert::create_op_class_stmt(stmt)?;
            DefineOpClass(mcx, &resolved)
        }
        None => Err(types_error::PgError::error(
            "define_op_class: parse tree is not a CreateOpClassStmt",
        )),
    });
    rt::define_op_family::set(|mcx, parsetree| match parsetree.as_createopfamilystmt() {
        Some(stmt) => {
            let resolved = convert::create_op_family_stmt(stmt)?;
            DefineOpFamily(mcx, &resolved)
        }
        None => Err(types_error::PgError::error(
            "define_op_family: parse tree is not a CreateOpFamilyStmt",
        )),
    });
    rt::alter_op_family::set(|mcx, parsetree| match parsetree.as_alteropfamilystmt() {
        Some(stmt) => {
            let resolved = convert::alter_op_family_stmt(stmt)?;
            AlterOpFamily(mcx, &resolved)
        }
        None => Err(types_error::PgError::error(
            "alter_op_family: parse tree is not an AlterOpFamilyStmt",
        )),
    });
}

/// Adapt the seam-borne `&[&str]` qualified name into the owner's
/// `Vec<StringNode>` image, then call the in-crate lookup.
fn seam_get_opclass_oid(
    mcx: Mcx<'_>,
    am_id: Oid,
    opclassname: &[&str],
    missing_ok: bool,
) -> PgResult<Oid> {
    let owned: Vec<StringNode> = opclassname
        .iter()
        .map(|s| StringNode { sval: Some((*s).to_string()) })
        .collect();
    get_opclass_oid(mcx, am_id, &owned, missing_ok)
}

fn seam_get_opfamily_oid(
    mcx: Mcx<'_>,
    am_id: Oid,
    opfamilyname: &[&str],
    missing_ok: bool,
) -> PgResult<Oid> {
    let owned: Vec<StringNode> = opfamilyname
        .iter()
        .map(|s| StringNode { sval: Some((*s).to_string()) })
        .collect();
    get_opfamily_oid(mcx, am_id, &owned, missing_ok)
}

/// Render a `&[StringNode]` as a `NameList` (`&[Option<String>]`) for the
/// `backend-catalog-namespace` helpers, mirroring a PostgreSQL `List *` of
/// `String` nodes. (Owned `String`s: the namespace `NameList` vocabulary on
/// main is std-String-based — these transient name lists feed it directly, the
/// way C's `List *` feeds the same helpers.)
fn name_list(names: &[StringNode]) -> Vec<Option<String>> {
    names.iter().map(|s| s.sval.clone()).collect()
}

// ---------------------------------------------------------------------------
// OpFamilyCacheLookup / get_opfamily_oid
// ---------------------------------------------------------------------------

/// `OpFamilyCacheLookup(amID, opfamilyname, missing_ok)` (opclasscmds.c:80) —
/// resolve a (possibly qualified) opfamily name to its OID, or return
/// `Ok(None)` when not found and `missing_ok`, else raise.
fn OpFamilyCacheLookup(
    mcx: Mcx<'_>,
    amID: Oid,
    opfamilyname: &[StringNode],
    missing_ok: bool,
) -> PgResult<Option<Oid>> {
    let names = name_list(opfamilyname);

    /* deconstruct the name list */
    let (schemaname, opfname) = DeconstructQualifiedName(mcx, &names)?;

    let opf: Option<Oid> = if let Some(schemaname) = schemaname {
        /* Look in specific schema only */
        let namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;
        if !OidIsValid(namespaceId) {
            None
        } else {
            let oid = syscache_opfamily_oid::call(amID, opfname, namespaceId)?;
            if OidIsValid(oid) {
                Some(oid)
            } else {
                None
            }
        }
    } else {
        /* Unqualified opfamily name, so search the search path */
        let opfID = OpfamilynameGetOpfid(mcx, amID, opfname)?;
        if !OidIsValid(opfID) {
            None
        } else {
            Some(opfID)
        }
    };

    if opf.is_none() && !missing_ok {
        let amname = match get_am_name::call(mcx, amID)? {
            Some(name) => name.as_str().to_string(),
            None => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("cache lookup failed for access method {amID}"))
                    .into_error());
            }
        };
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "operator family \"{}\" does not exist for access method \"{}\"",
                NameListToString(mcx, &names)?.as_str(),
                amname
            ))
            .into_error());
    }

    Ok(opf)
}

/// `get_opfamily_oid(amID, opfamilyname, missing_ok)` (opclasscmds.c:138).
pub fn get_opfamily_oid(
    mcx: Mcx<'_>,
    am_id: Oid,
    opfamilyname: &[StringNode],
    missing_ok: bool,
) -> PgResult<Oid> {
    match OpFamilyCacheLookup(mcx, am_id, opfamilyname, missing_ok)? {
        None => Ok(InvalidOid),
        Some(opf_id) => Ok(opf_id),
    }
}

// ---------------------------------------------------------------------------
// OpClassCacheLookup / get_opclass_oid
// ---------------------------------------------------------------------------

/// `OpClassCacheLookup(amID, opclassname, missing_ok)` (opclasscmds.c:161).
fn OpClassCacheLookup(
    mcx: Mcx<'_>,
    amID: Oid,
    opclassname: &[StringNode],
    missing_ok: bool,
) -> PgResult<Option<Oid>> {
    let names = name_list(opclassname);

    /* deconstruct the name list */
    let (schemaname, opcname) = DeconstructQualifiedName(mcx, &names)?;

    let opc: Option<Oid> = if let Some(schemaname) = schemaname {
        /* Look in specific schema only */
        let namespaceId = LookupExplicitNamespace(schemaname, missing_ok)?;
        if !OidIsValid(namespaceId) {
            None
        } else {
            let oid = syscache_seams::get_opclass_oid::call(
                amID, opcname, namespaceId,
            )?;
            if OidIsValid(oid) {
                Some(oid)
            } else {
                None
            }
        }
    } else {
        /* Unqualified opclass name, so search the search path */
        let opcID = OpclassnameGetOpcid(mcx, amID, opcname)?;
        if !OidIsValid(opcID) {
            None
        } else {
            Some(opcID)
        }
    };

    if opc.is_none() && !missing_ok {
        let amname = match get_am_name::call(mcx, amID)? {
            Some(name) => name.as_str().to_string(),
            None => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("cache lookup failed for access method {amID}"))
                    .into_error());
            }
        };
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "operator class \"{}\" does not exist for access method \"{}\"",
                NameListToString(mcx, &names)?.as_str(),
                amname
            ))
            .into_error());
    }

    Ok(opc)
}

/// `get_opclass_oid(amID, opclassname, missing_ok)` (opclasscmds.c:219).
pub fn get_opclass_oid(
    mcx: Mcx<'_>,
    am_id: Oid,
    opclassname: &[StringNode],
    missing_ok: bool,
) -> PgResult<Oid> {
    match OpClassCacheLookup(mcx, am_id, opclassname, missing_ok)? {
        None => Ok(InvalidOid),
        Some(opc_id) => Ok(opc_id),
    }
}

// ---------------------------------------------------------------------------
// CreateOpFamily
// ---------------------------------------------------------------------------

/// `CreateOpFamily(stmt, opfname, namespaceoid, amoid)` (opclasscmds.c:242) —
/// make the catalog entry for a new operator family. Caller did the permission
/// checks.
fn CreateOpFamily(
    mcx: Mcx<'_>,
    stmt: &CreateOpFamilyStmt,
    opfname: &str,
    namespaceoid: Oid,
    amoid: Oid,
) -> PgResult<ObjectAddress> {
    let amname = stmt.amname.clone().unwrap_or_default();

    let rel = table::table_open(
        mcx,
        OPERATOR_FAMILY_RELATION_ID,
        types_storage::lock::RowExclusiveLock,
    )?;

    /*
     * Make sure there is no existing opfamily of this name (this is just to
     * give a more friendly error message than "duplicate key").
     */
    if opfamily_exists::call(amoid, opfname, namespaceoid)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "operator family \"{opfname}\" for access method \"{amname}\" already exists"
            ))
            .into_error());
    }

    /* Okay, let's create the pg_opfamily entry. */
    let owner = get_user_id::call();
    let opfamilyoid = catalog_tuple_insert_pg_opfamily::call(
        &rel,
        &FormData_pg_opfamily {
            opfmethod: amoid,
            opfname: opfname.to_string(),
            opfnamespace: namespaceoid,
            opfowner: owner,
        },
    )?;

    /* Create dependencies for the opfamily proper. */
    let myself = ObjectAddress {
        classId: OPERATOR_FAMILY_RELATION_ID,
        objectId: opfamilyoid,
        objectSubId: 0,
    };

    /* dependency on access method */
    recordDependencyOn::call(
        mcx,
        &myself,
        &ObjectAddress {
            classId: ACCESS_METHOD_RELATION_ID,
            objectId: amoid,
            objectSubId: 0,
        },
        DEPENDENCY_AUTO,
    )?;

    /* dependency on namespace */
    recordDependencyOn::call(
        mcx,
        &myself,
        &ObjectAddress {
            classId: NAMESPACE_RELATION_ID,
            objectId: namespaceoid,
            objectSubId: 0,
        },
        DEPENDENCY_NORMAL,
    )?;

    /* dependency on owner */
    recordDependencyOnOwner::call(OPERATOR_FAMILY_RELATION_ID, opfamilyoid, owner)?;

    /* dependency on extension */
    recordDependencyOnCurrentExtension::call(mcx, &myself, false)?;

    /* Report the new operator family to possibly interested event triggers */
    event_trigger_collect_simple_command::call(
        myself,
        types_catalog::catalog_dependency::InvalidObjectAddress,
        stmt,
    )?;

    /* Post creation hook for new operator family */
    InvokeObjectPostCreateHook(OPERATOR_FAMILY_RELATION_ID, opfamilyoid, 0)?;

    rel.close(types_storage::lock::RowExclusiveLock)?;

    Ok(myself)
}

// ---------------------------------------------------------------------------
// DefineOpClass
// ---------------------------------------------------------------------------

/// `DefineOpClass(stmt)` (opclasscmds.c:332) — CREATE OPERATOR CLASS.
pub fn DefineOpClass(mcx: Mcx<'_>, stmt: &CreateOpClassStmt) -> PgResult<ObjectAddress> {
    let amname = stmt.amname.clone().unwrap_or_default();
    let opfamilyoid: Oid; /* oid of containing opfamily */
    let mut operators: PgVec<'_, OpFamilyMember> = vec_with_capacity_in(mcx, 0)?;
    let mut procedures: PgVec<'_, OpFamilyMember> = vec_with_capacity_in(mcx, 0)?;

    /* Convert list of names to a name and namespace */
    let opclassname = name_list(&stmt.opclassname);
    let (namespaceoid, opcname) = QualifiedNameGetCreationNamespace(mcx, &opclassname)?;
    let opcname = opcname.to_string();

    /* Check we have creation rights in target namespace */
    let aclresult = object_aclcheck::call(
        NAMESPACE_RELATION_ID,
        namespaceoid,
        get_user_id::call(),
        ACL_CREATE as AclMode,
    )?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error::call(
            aclresult,
            OBJECT_SCHEMA,
            get_namespace_name_for_acl(mcx, namespaceoid)?,
        )?;
    }

    /* Get necessary info about access method */
    let amoid = get_index_am_oid::call(&amname, false)?;
    let amroutine = get_index_am_info::call(amoid)?;

    let mut maxOpNumber = amroutine.amstrategies; /* amstrategies value */
    /* if amstrategies is zero, just enforce that op numbers fit in int16 */
    if maxOpNumber <= 0 {
        maxOpNumber = SHRT_MAX;
    }
    let maxProcNumber = amroutine.amsupport; /* amsupport value */
    let optsProcNumber = amroutine.amoptsprocnum; /* amoptsprocnum value */
    let amstorage = amroutine.amstorage; /* amstorage flag */

    /* XXX Should we make any privilege check against the AM? */

    /* Currently, we require superuser privileges to create an opclass. */
    if !superuser_arg::call(get_user_id::call())? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("must be superuser to create an operator class")
            .into_error());
    }

    /* Look up the datatype */
    let datatype = node_to_type_name(stmt.datatype.as_ref())?;
    let typeoid = typename_type_id::call(datatype)?;

    /*
     * Look up the containing operator family, or create one if FAMILY option
     * was omitted and there's not a match already.
     */
    if !stmt.opfamilyname.is_empty() {
        opfamilyoid = get_opfamily_oid(mcx, amoid, &stmt.opfamilyname, false)?;
    } else {
        /* Lookup existing family of same name and namespace */
        let existing = syscache_opfamily_oid::call(amoid, &opcname, namespaceoid)?;
        if OidIsValid(existing) {
            opfamilyoid = existing;
        } else {
            /* Create it ... no need for more permissions ... */
            let opfstmt = CreateOpFamilyStmt {
                opfamilyname: stmt.opclassname.clone(),
                amname: stmt.amname.clone(),
            };
            let tmpAddr = CreateOpFamily(mcx, &opfstmt, &opcname, namespaceoid, amoid)?;
            opfamilyoid = tmpAddr.objectId;
        }
    }

    /* Storage datatype is optional */
    let mut storageoid: Oid = InvalidOid;

    /* Scan the "items" list to obtain additional info. */
    for item in &stmt.items {
        let itemtype = item.itemtype;

        if itemtype == OPCLASS_ITEM_OPERATOR {
            if item.number <= 0 || item.number > maxOpNumber {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!(
                        "invalid operator number {}, must be between 1 and {maxOpNumber}",
                        item.number
                    ))
                    .into_error());
            }
            let name = node_to_object_with_args(item.name.as_ref())?;
            let operOid = if !name.objargs.is_empty() {
                lookup_oper_with_args::call(name, false)?
            } else {
                /* Default to binary op on input datatype */
                lookup_oper_name::call(&name.objname, typeoid, typeoid)?
            };

            let sortfamilyOid = if !item.order_family.is_empty() {
                get_opfamily_oid(mcx, BTREE_AM_OID, &item.order_family, false)?
            } else {
                InvalidOid
            };

            /* Save the info */
            let mut member = new_member();
            member.is_func = false;
            member.object = operOid;
            member.number = item.number;
            member.sortfamily = sortfamilyOid;
            assignOperTypes(mcx, &mut member, amoid, typeoid)?;
            addFamilyMember(mcx, &mut operators, member)?;
        } else if itemtype == OPCLASS_ITEM_FUNCTION {
            if item.number <= 0 || item.number > maxProcNumber {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!(
                        "invalid function number {}, must be between 1 and {maxProcNumber}",
                        item.number
                    ))
                    .into_error());
            }
            let name = node_to_object_with_args(item.name.as_ref())?;
            let funcOid = lookup_func_with_args::call(name, false)?;
            /* Save the info */
            let mut member = new_member();
            member.is_func = true;
            member.object = funcOid;
            member.number = item.number;

            /* allow overriding of the function's actual arg types */
            if !item.class_args.is_empty() {
                let (lt, rt_) = processTypesSpec(&item.class_args)?;
                member.lefttype = lt;
                member.righttype = rt_;
            }

            assignProcTypes(mcx, &mut member, amoid, typeoid, optsProcNumber)?;
            addFamilyMember(mcx, &mut procedures, member)?;
        } else if itemtype == OPCLASS_ITEM_STORAGETYPE {
            if OidIsValid(storageoid) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("storage type specified more than once")
                    .into_error());
            }
            storageoid = typename_type_id::call(node_to_type_name(item.storedtype.as_ref())?)?;
        } else {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized item type: {itemtype}"))
                .into_error());
        }
    }

    /* If storagetype is specified, make sure it's legal. */
    if OidIsValid(storageoid) {
        /* Just drop the spec if same as column datatype */
        if storageoid == typeoid {
            storageoid = InvalidOid;
        } else if !amstorage {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!(
                    "storage type cannot be different from data type for access method \"{amname}\""
                ))
                .into_error());
        }
    }

    let rel = table::table_open(
        mcx,
        OPERATOR_CLASS_RELATION_ID,
        types_storage::lock::RowExclusiveLock,
    )?;

    /*
     * Make sure there is no existing opclass of this name (this is just to
     * give a more friendly error message than "duplicate key").
     */
    if opclass_exists::call(amoid, &opcname, namespaceoid)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "operator class \"{opcname}\" for access method \"{amname}\" already exists"
            ))
            .into_error());
    }

    /*
     * If we are creating a default opclass, check there isn't one already.
     * (Note we do not restrict this test to visible opclasses; this ensures
     * that typcache.c can find unique solutions to its questions.)
     */
    if stmt.isDefault {
        // ScanKeyInit(&skey[0], Anum_pg_opclass_opcmethod, BTEqualStrategyNumber,
        //             F_OIDEQ, ObjectIdGetDatum(amoid));
        let mut key = ScanKeyData::empty();
        ScanKeyInit(
            &mut key,
            Anum_pg_opclass_opcmethod,
            BTEqualStrategyNumber,
            F_OIDEQ,
            // `ScanKeyData.sk_argument` is the canonical unified `Datum<'mcx>`
            // (the Datum-unification keystone flipped this edge).
            Datum::from_oid(amoid),
        )?;
        let keys = [key];

        // scan = systable_beginscan(rel, OpclassAmNameNspIndexId, true, NULL, 1, skey);
        // while (HeapTupleIsValid(tup = systable_getnext(scan))) { ... }
        // systable_endscan(scan);
        let mut ret: PgResult<()> = Ok(());
        systable_scan_foreach(&rel, OpclassAmNameNspIndexId, &keys, |row| {
            let opcintype = column_oid(row, Anum_pg_opclass_opcintype);
            let opcdefault = column_bool(row, Anum_pg_opclass_opcdefault);
            if opcintype == typeoid && opcdefault {
                let existing_opcname = column_name(row, Anum_pg_opclass_opcname);
                ret = Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_OBJECT)
                    .errmsg(format!(
                        "could not make operator class \"{opcname}\" be default for type {}",
                        typename_to_string::call(mcx, node_to_type_name(stmt.datatype.as_ref())?)?
                            .as_str()
                    ))
                    .errdetail(format!(
                        "Operator class \"{existing_opcname}\" already is the default."
                    ))
                    .into_error());
                return Ok(false);
            }
            Ok(true)
        })?;
        ret?;
    }

    /* Okay, let's create the pg_opclass entry. */
    let owner = get_user_id::call();
    let opclassoid = catalog_tuple_insert_pg_opclass::call(
        &rel,
        &FormData_pg_opclass {
            opcmethod: amoid,
            opcname: opcname.clone(),
            opcnamespace: namespaceoid,
            opcowner: owner,
            opcfamily: opfamilyoid,
            opcintype: typeoid,
            opcdefault: stmt.isDefault,
            opckeytype: storageoid,
        },
    )?;

    /*
     * Now that we have the opclass OID, set up default dependency info for the
     * pg_amop and pg_amproc entries. Historically, CREATE OPERATOR CLASS has
     * created hard dependencies on the opclass, so that's what we use.
     */
    for op in operators.iter_mut() {
        op.ref_is_hard = true;
        op.ref_is_family = false;
        op.refobjid = opclassoid;
    }
    for proc in procedures.iter_mut() {
        proc.ref_is_hard = true;
        proc.ref_is_family = false;
        proc.refobjid = opclassoid;
    }

    /*
     * Let the index AM editorialize on the dependency choices. It could also do
     * further validation on the operators and functions, if it likes.
     */
    if amroutine.has_adjustmembers {
        let (ops, procs) =
            am_adjust_members::call(mcx, amoid, opfamilyoid, opclassoid, operators, procedures)?;
        operators = ops;
        procedures = procs;
    }

    /*
     * Now add tuples to pg_amop and pg_amproc tying in the operators and
     * functions. Dependencies on them are inserted, too.
     */
    storeOperators(mcx, &stmt.opfamilyname, amoid, opfamilyoid, &operators, false)?;
    storeProcedures(mcx, &stmt.opfamilyname, amoid, opfamilyoid, &procedures, false)?;

    /* let event triggers know what happened */
    event_trigger_collect_create_opclass::call(stmt, opclassoid, &operators, &procedures)?;

    /*
     * Create dependencies for the opclass proper. Note: we do not need a
     * dependency link to the AM, because that exists through the opfamily.
     */
    let myself = ObjectAddress {
        classId: OPERATOR_CLASS_RELATION_ID,
        objectId: opclassoid,
        objectSubId: 0,
    };

    /* dependency on namespace */
    recordDependencyOn::call(
        mcx,
        &myself,
        &ObjectAddress {
            classId: NAMESPACE_RELATION_ID,
            objectId: namespaceoid,
            objectSubId: 0,
        },
        DEPENDENCY_NORMAL,
    )?;

    /* dependency on opfamily */
    recordDependencyOn::call(
        mcx,
        &myself,
        &ObjectAddress {
            classId: OPERATOR_FAMILY_RELATION_ID,
            objectId: opfamilyoid,
            objectSubId: 0,
        },
        DEPENDENCY_AUTO,
    )?;

    /* dependency on indexed datatype */
    recordDependencyOn::call(
        mcx,
        &myself,
        &ObjectAddress {
            classId: TYPE_RELATION_ID,
            objectId: typeoid,
            objectSubId: 0,
        },
        DEPENDENCY_NORMAL,
    )?;

    /* dependency on storage datatype */
    if OidIsValid(storageoid) {
        recordDependencyOn::call(
            mcx,
            &myself,
            &ObjectAddress {
                classId: TYPE_RELATION_ID,
                objectId: storageoid,
                objectSubId: 0,
            },
            DEPENDENCY_NORMAL,
        )?;
    }

    /* dependency on owner */
    recordDependencyOnOwner::call(OPERATOR_CLASS_RELATION_ID, opclassoid, owner)?;

    /* dependency on extension */
    recordDependencyOnCurrentExtension::call(mcx, &myself, false)?;

    /* Post creation hook for new operator class */
    InvokeObjectPostCreateHook(OPERATOR_CLASS_RELATION_ID, opclassoid, 0)?;

    rel.close(types_storage::lock::RowExclusiveLock)?;

    Ok(myself)
}

// ---------------------------------------------------------------------------
// DefineOpFamily
// ---------------------------------------------------------------------------

/// `DefineOpFamily(stmt)` (opclasscmds.c:771) — CREATE OPERATOR FAMILY.
pub fn DefineOpFamily(mcx: Mcx<'_>, stmt: &CreateOpFamilyStmt) -> PgResult<ObjectAddress> {
    /* Convert list of names to a name and namespace */
    let opfamilyname = name_list(&stmt.opfamilyname);
    let (namespaceoid, opfname) = QualifiedNameGetCreationNamespace(mcx, &opfamilyname)?;
    let opfname = opfname.to_string();

    /* Check we have creation rights in target namespace */
    let aclresult = object_aclcheck::call(
        NAMESPACE_RELATION_ID,
        namespaceoid,
        get_user_id::call(),
        ACL_CREATE as AclMode,
    )?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error::call(
            aclresult,
            OBJECT_SCHEMA,
            get_namespace_name_for_acl(mcx, namespaceoid)?,
        )?;
    }

    /* Get access method OID, throwing an error if it doesn't exist. */
    let amname = stmt.amname.clone().unwrap_or_default();
    let amoid = get_index_am_oid::call(&amname, false)?;

    /* XXX Should we make any privilege check against the AM? */

    /* Currently, we require superuser privileges to create an opfamily. */
    if !superuser_arg::call(get_user_id::call())? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("must be superuser to create an operator family")
            .into_error());
    }

    /* Insert pg_opfamily catalog entry */
    CreateOpFamily(mcx, stmt, &opfname, namespaceoid, amoid)
}

// ---------------------------------------------------------------------------
// AlterOpFamily
// ---------------------------------------------------------------------------

/// `AlterOpFamily(stmt)` (opclasscmds.c:817) — ALTER OPERATOR FAMILY ADD/DROP.
pub fn AlterOpFamily(mcx: Mcx<'_>, stmt: &AlterOpFamilyStmt) -> PgResult<Oid> {
    let amname = stmt.amname.clone().unwrap_or_default();

    /* Get necessary info about access method */
    let amoid = get_index_am_oid::call(&amname, false)?;
    let amroutine = get_index_am_info::call(amoid)?;

    let mut maxOpNumber = amroutine.amstrategies;
    if maxOpNumber <= 0 {
        maxOpNumber = SHRT_MAX;
    }
    let maxProcNumber = amroutine.amsupport;
    let optsProcNumber = amroutine.amoptsprocnum;

    /* XXX Should we make any privilege check against the AM? */

    /* Look up the opfamily */
    let opfamilyoid = get_opfamily_oid(mcx, amoid, &stmt.opfamilyname, false)?;

    /* Currently, we require superuser privileges to alter an opfamily. */
    if !superuser_arg::call(get_user_id::call())? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("must be superuser to alter an operator family")
            .into_error());
    }

    /* ADD and DROP cases need separate code from here on down. */
    if stmt.isDrop {
        AlterOpFamilyDrop(mcx, stmt, amoid, opfamilyoid, maxOpNumber, maxProcNumber, &stmt.items)?;
    } else {
        AlterOpFamilyAdd(
            mcx,
            stmt,
            amoid,
            opfamilyoid,
            maxOpNumber,
            maxProcNumber,
            optsProcNumber,
            &stmt.items,
        )?;
    }

    Ok(opfamilyoid)
}

/// `AlterOpFamilyAdd` (opclasscmds.c:880) — ADD part of ALTER OP FAMILY.
fn AlterOpFamilyAdd(
    mcx: Mcx<'_>,
    stmt: &AlterOpFamilyStmt,
    amoid: Oid,
    opfamilyoid: Oid,
    maxOpNumber: i32,
    maxProcNumber: i32,
    optsProcNumber: i32,
    items: &[CreateOpClassItem],
) -> PgResult<()> {
    let amroutine = get_index_am_info::call(amoid)?;
    let mut operators: PgVec<'_, OpFamilyMember> = vec_with_capacity_in(mcx, 0)?;
    let mut procedures: PgVec<'_, OpFamilyMember> = vec_with_capacity_in(mcx, 0)?;

    /* Scan the "items" list to obtain additional info. */
    for item in items {
        let itemtype = item.itemtype;

        if itemtype == OPCLASS_ITEM_OPERATOR {
            if item.number <= 0 || item.number > maxOpNumber {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!(
                        "invalid operator number {}, must be between 1 and {maxOpNumber}",
                        item.number
                    ))
                    .into_error());
            }
            let name = node_to_object_with_args(item.name.as_ref())?;
            let operOid = if !name.objargs.is_empty() {
                lookup_oper_with_args::call(name, false)?
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg("operator argument types must be specified in ALTER OPERATOR FAMILY")
                    .into_error());
            };

            let sortfamilyOid = if !item.order_family.is_empty() {
                get_opfamily_oid(mcx, BTREE_AM_OID, &item.order_family, false)?
            } else {
                InvalidOid
            };

            /* Save the info */
            let mut member = new_member();
            member.is_func = false;
            member.object = operOid;
            member.number = item.number;
            member.sortfamily = sortfamilyOid;
            /* Historically, ALTER ADD has created soft dependencies */
            member.ref_is_hard = false;
            member.ref_is_family = true;
            member.refobjid = opfamilyoid;
            assignOperTypes(mcx, &mut member, amoid, InvalidOid)?;
            addFamilyMember(mcx, &mut operators, member)?;
        } else if itemtype == OPCLASS_ITEM_FUNCTION {
            if item.number <= 0 || item.number > maxProcNumber {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!(
                        "invalid function number {}, must be between 1 and {maxProcNumber}",
                        item.number
                    ))
                    .into_error());
            }
            let name = node_to_object_with_args(item.name.as_ref())?;
            let funcOid = lookup_func_with_args::call(name, false)?;

            /* Save the info */
            let mut member = new_member();
            member.is_func = true;
            member.object = funcOid;
            member.number = item.number;
            /* Historically, ALTER ADD has created soft dependencies */
            member.ref_is_hard = false;
            member.ref_is_family = true;
            member.refobjid = opfamilyoid;

            /* allow overriding of the function's actual arg types */
            if !item.class_args.is_empty() {
                let (lt, rt_) = processTypesSpec(&item.class_args)?;
                member.lefttype = lt;
                member.righttype = rt_;
            }

            assignProcTypes(mcx, &mut member, amoid, InvalidOid, optsProcNumber)?;
            addFamilyMember(mcx, &mut procedures, member)?;
        } else if itemtype == OPCLASS_ITEM_STORAGETYPE {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("STORAGE cannot be specified in ALTER OPERATOR FAMILY")
                .into_error());
        } else {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized item type: {itemtype}"))
                .into_error());
        }
    }

    /*
     * Let the index AM editorialize on the dependency choices.
     */
    if amroutine.has_adjustmembers {
        let (ops, procs) = am_adjust_members::call(
            mcx,
            amoid,
            opfamilyoid,
            InvalidOid, /* no specific opclass */
            operators,
            procedures,
        )?;
        operators = ops;
        procedures = procs;
    }

    /*
     * Add tuples to pg_amop and pg_amproc tying in the operators and functions.
     */
    storeOperators(mcx, &stmt.opfamilyname, amoid, opfamilyoid, &operators, true)?;
    storeProcedures(mcx, &stmt.opfamilyname, amoid, opfamilyoid, &procedures, true)?;

    /* make information available to event triggers */
    event_trigger_collect_alter_opfam::call(stmt, opfamilyoid, &operators, &procedures)?;

    Ok(())
}

/// `AlterOpFamilyDrop` (opclasscmds.c:1029) — DROP part of ALTER OP FAMILY.
fn AlterOpFamilyDrop(
    mcx: Mcx<'_>,
    stmt: &AlterOpFamilyStmt,
    _amoid: Oid,
    opfamilyoid: Oid,
    maxOpNumber: i32,
    maxProcNumber: i32,
    items: &[CreateOpClassItem],
) -> PgResult<()> {
    let mut operators: PgVec<'_, OpFamilyMember> = vec_with_capacity_in(mcx, 0)?;
    let mut procedures: PgVec<'_, OpFamilyMember> = vec_with_capacity_in(mcx, 0)?;

    /* Scan the "items" list to obtain additional info. */
    for item in items {
        let itemtype = item.itemtype;

        if itemtype == OPCLASS_ITEM_OPERATOR {
            if item.number <= 0 || item.number > maxOpNumber {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!(
                        "invalid operator number {}, must be between 1 and {maxOpNumber}",
                        item.number
                    ))
                    .into_error());
            }
            let (lefttype, righttype) = processTypesSpec(&item.class_args)?;
            /* Save the info */
            let mut member = new_member();
            member.is_func = false;
            member.number = item.number;
            member.lefttype = lefttype;
            member.righttype = righttype;
            addFamilyMember(mcx, &mut operators, member)?;
        } else if itemtype == OPCLASS_ITEM_FUNCTION {
            if item.number <= 0 || item.number > maxProcNumber {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!(
                        "invalid function number {}, must be between 1 and {maxProcNumber}",
                        item.number
                    ))
                    .into_error());
            }
            let (lefttype, righttype) = processTypesSpec(&item.class_args)?;
            /* Save the info */
            let mut member = new_member();
            member.is_func = true;
            member.number = item.number;
            member.lefttype = lefttype;
            member.righttype = righttype;
            addFamilyMember(mcx, &mut procedures, member)?;
        } else {
            /* OPCLASS_ITEM_STORAGETYPE: grammar prevents this from appearing */
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized item type: {itemtype}"))
                .into_error());
        }
    }

    /* Remove tuples from pg_amop and pg_amproc. */
    dropOperators(mcx, &stmt.opfamilyname, opfamilyoid, &operators)?;
    dropProcedures(mcx, &stmt.opfamilyname, opfamilyoid, &procedures)?;

    /* make information available to event triggers */
    event_trigger_collect_alter_opfam::call(stmt, opfamilyoid, &operators, &procedures)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// processTypesSpec
// ---------------------------------------------------------------------------

/// `processTypesSpec(args, &lefttype, &righttype)` (opclasscmds.c:1107) — deal
/// with explicit arg types used in ALTER ADD/DROP. Returns
/// `(lefttype, righttype)`.
fn processTypesSpec(args: &[TypeName]) -> PgResult<(Oid, Oid)> {
    debug_assert!(!args.is_empty()); /* Assert(args != NIL); */

    let lefttype = typename_type_id::call(&args[0])?;

    let righttype = if args.len() > 1 {
        typename_type_id::call(&args[1])?
    } else {
        lefttype
    };

    if args.len() > 2 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("one or two argument types must be specified")
            .into_error());
    }

    Ok((lefttype, righttype))
}

// ---------------------------------------------------------------------------
// assignOperTypes
// ---------------------------------------------------------------------------

/// `assignOperTypes(member, amoid, typeoid)` (opclasscmds.c:1136) — determine
/// the lefttype/righttype to assign to an operator, and do validity checking.
fn assignOperTypes(
    mcx: Mcx<'_>,
    member: &mut OpFamilyMember,
    amoid: Oid,
    _typeoid: Oid,
) -> PgResult<()> {
    /* Fetch the operator definition */
    let opform = match operator_form(mcx, member.object)? {
        Some(o) => o,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for operator {}", member.object))
                .into_error());
        }
    };

    /* Opfamily operators must be binary. */
    if opform.oprkind != b'b' {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("index operators must be binary")
            .into_error());
    }

    if OidIsValid(member.sortfamily) {
        /* Ordering op, check index supports that. */
        let amroutine = get_index_am_info::call(amoid)?;

        if !amroutine.amcanorderbyop {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!(
                    "access method \"{}\" does not support ordering operators",
                    am_name_for_error(amoid)
                ))
                .into_error());
        }
    } else {
        /* Search operators must return boolean. */
        if opform.oprresult != BOOLOID {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("index search operators must return boolean")
                .into_error());
        }
    }

    /* If lefttype/righttype isn't specified, use the operator's input types */
    if !OidIsValid(member.lefttype) {
        member.lefttype = opform.oprleft;
    }
    if !OidIsValid(member.righttype) {
        member.righttype = opform.oprright;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// assignProcTypes
// ---------------------------------------------------------------------------

/// `assignProcTypes(member, amoid, typeoid, opclassOptsProcNum)`
/// (opclasscmds.c:1202) — determine the lefttype/righttype to assign to a
/// support procedure, and do validity checking.
fn assignProcTypes(
    mcx: Mcx<'_>,
    member: &mut OpFamilyMember,
    amoid: Oid,
    typeoid: Oid,
    opclassOptsProcNum: i32,
) -> PgResult<()> {
    /* Fetch the procedure definition */
    let procform = match proc_form(mcx, member.object)? {
        Some(p) => p,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for function {}", member.object))
                .into_error());
        }
    };

    /* Check the signature of the opclass options parsing function */
    if member.number == opclassOptsProcNum {
        if OidIsValid(typeoid) {
            if (OidIsValid(member.lefttype) && member.lefttype != typeoid)
                || (OidIsValid(member.righttype) && member.righttype != typeoid)
            {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("associated data types for operator class options parsing functions must match opclass input type")
                    .into_error());
            }
        } else if member.lefttype != member.righttype {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("left and right associated data types for operator class options parsing functions must match")
                .into_error());
        }

        if procform.prorettype != VOIDOID
            || procform.pronargs != 1
            || proarg(&procform, 0) != INTERNALOID
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("invalid operator class options parsing function")
                .errhint("Valid signature of operator class options parsing function is (internal) RETURNS void.")
                .into_error());
        }
    }
    /*
     * Ordering comparison procs must be 2-arg procs returning int4. Ordering
     * sortsupport procs must take internal and return void. Ordering in_range
     * procs must be 5-arg procs returning bool. Ordering equalimage procs must
     * take 1 arg and return bool. Hashing support proc 1 must be a 1-arg proc
     * returning int4, while proc 2 must be a 2-arg proc returning int8.
     * Otherwise we don't know.
     */
    else if get_index_am_info::call(amoid)?.amcanorder {
        if member.number == BTORDER_PROC {
            if procform.pronargs != 2 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("ordering comparison functions must have two arguments")
                    .into_error());
            }
            if procform.prorettype != INT4OID {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("ordering comparison functions must return integer")
                    .into_error());
            }

            if !OidIsValid(member.lefttype) {
                member.lefttype = proarg(&procform, 0);
            }
            if !OidIsValid(member.righttype) {
                member.righttype = proarg(&procform, 1);
            }
        } else if member.number == BTSORTSUPPORT_PROC {
            if procform.pronargs != 1 || proarg(&procform, 0) != INTERNALOID {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("ordering sort support functions must accept type \"internal\"")
                    .into_error());
            }
            if procform.prorettype != VOIDOID {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("ordering sort support functions must return void")
                    .into_error());
            }
            /* Can't infer lefttype/righttype from proc, so use default rule */
        } else if member.number == BTINRANGE_PROC {
            if procform.pronargs != 5 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("ordering in_range functions must have five arguments")
                    .into_error());
            }
            if procform.prorettype != BOOLOID {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("ordering in_range functions must return boolean")
                    .into_error());
            }

            if !OidIsValid(member.lefttype) {
                member.lefttype = proarg(&procform, 0);
            }
            if !OidIsValid(member.righttype) {
                member.righttype = proarg(&procform, 2);
            }
        } else if member.number == BTEQUALIMAGE_PROC {
            if procform.pronargs != 1 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("ordering equal image functions must have one argument")
                    .into_error());
            }
            if procform.prorettype != BOOLOID {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("ordering equal image functions must return boolean")
                    .into_error());
            }

            /* Reject cross-type ALTER OPERATOR FAMILY ... ADD FUNCTION 4. */
            if member.lefttype != member.righttype {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("ordering equal image functions must not be cross-type")
                    .into_error());
            }
        } else if member.number == BTSKIPSUPPORT_PROC {
            if procform.pronargs != 1 || proarg(&procform, 0) != INTERNALOID {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("btree skip support functions must accept type \"internal\"")
                    .into_error());
            }
            if procform.prorettype != VOIDOID {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("btree skip support functions must return void")
                    .into_error());
            }

            /* Reject cross-type ALTER OPERATOR FAMILY ... ADD FUNCTION 6. */
            if member.lefttype != member.righttype {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("btree skip support functions must not be cross-type")
                    .into_error());
            }
        }
    } else if get_index_am_info::call(amoid)?.amcanhash {
        if member.number == HASHSTANDARD_PROC {
            if procform.pronargs != 1 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("hash function 1 must have one argument")
                    .into_error());
            }
            if procform.prorettype != INT4OID {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("hash function 1 must return integer")
                    .into_error());
            }
        } else if member.number == HASHEXTENDED_PROC {
            if procform.pronargs != 2 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("hash function 2 must have two arguments")
                    .into_error());
            }
            if procform.prorettype != INT8OID {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("hash function 2 must return bigint")
                    .into_error());
            }
        }

        /* If lefttype/righttype isn't specified, use the proc's input type */
        if !OidIsValid(member.lefttype) {
            member.lefttype = proarg(&procform, 0);
        }
        if !OidIsValid(member.righttype) {
            member.righttype = proarg(&procform, 0);
        }
    }

    /*
     * The default in CREATE OPERATOR CLASS is to use the class' opcintype as
     * lefttype and righttype. In CREATE or ALTER OPERATOR FAMILY, opcintype
     * isn't available, so make the user specify the types.
     */
    if !OidIsValid(member.lefttype) {
        member.lefttype = typeoid;
    }
    if !OidIsValid(member.righttype) {
        member.righttype = typeoid;
    }

    if !OidIsValid(member.lefttype) || !OidIsValid(member.righttype) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("associated data types must be specified for index support function")
            .into_error());
    }

    Ok(())
}

/// `procform->proargtypes.values[i]`, reading from the bundled arg-type vector.
/// Out-of-bounds (a malformed catalog) maps to `InvalidOid`; the surrounding
/// `pronargs` checks gate the indices the C code actually reads.
fn proarg(procform: &ProcFormFields, i: usize) -> Oid {
    procform.proargtypes.get(i).copied().unwrap_or(InvalidOid)
}

// ---------------------------------------------------------------------------
// addFamilyMember
// ---------------------------------------------------------------------------

/// `addFamilyMember(list, member)` (opclasscmds.c:1416) — add a new family
/// member to the appropriate list, after checking for duplicated strategy or
/// proc number.
fn addFamilyMember(
    mcx: Mcx<'_>,
    list: &mut PgVec<'_, OpFamilyMember>,
    member: OpFamilyMember,
) -> PgResult<()> {
    for old in list.iter() {
        if old.number == member.number
            && old.lefttype == member.lefttype
            && old.righttype == member.righttype
        {
            if member.is_func {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!(
                        "function number {} for ({},{}) appears more than once",
                        member.number,
                        format_type_be::call(mcx, member.lefttype)?.as_str(),
                        format_type_be::call(mcx, member.righttype)?.as_str()
                    ))
                    .into_error());
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!(
                        "operator number {} for ({},{}) appears more than once",
                        member.number,
                        format_type_be::call(mcx, member.lefttype)?.as_str(),
                        format_type_be::call(mcx, member.righttype)?.as_str()
                    ))
                    .into_error());
            }
        }
    }
    list.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<OpFamilyMember>()))?;
    list.push(member);
    Ok(())
}

// ---------------------------------------------------------------------------
// storeOperators
// ---------------------------------------------------------------------------

/// `storeOperators(opfamilyname, amoid, opfamilyoid, operators, isAdd)`
/// (opclasscmds.c:1453) — dump the operators to pg_amop with their pg_depend
/// entries.
fn storeOperators(
    mcx: Mcx<'_>,
    opfamilyname: &[StringNode],
    amoid: Oid,
    opfamilyoid: Oid,
    operators: &[OpFamilyMember],
    isAdd: bool,
) -> PgResult<()> {
    let opfamilyname_list = name_list(opfamilyname);

    let rel = table::table_open(
        mcx,
        ACCESS_METHOD_OPERATOR_RELATION_ID,
        types_storage::lock::RowExclusiveLock,
    )?;

    for op in operators {
        /*
         * If adding to an existing family, check for conflict with an existing
         * pg_amop entry (just to give a nicer error message)
         */
        if isAdd
            && OidIsValid(amop_oid::call(opfamilyoid, op.lefttype, op.righttype, op.number as i16)?)
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!(
                    "operator {}({},{}) already exists in operator family \"{}\"",
                    op.number,
                    format_type_be::call(mcx, op.lefttype)?.as_str(),
                    format_type_be::call(mcx, op.righttype)?.as_str(),
                    NameListToString(mcx, &opfamilyname_list)?.as_str()
                ))
                .into_error());
        }

        let oppurpose: i8 = if OidIsValid(op.sortfamily) {
            AMOP_ORDER
        } else {
            AMOP_SEARCH
        };

        /* Create the pg_amop entry */
        let entryoid = catalog_tuple_insert_pg_amop::call(
            &rel,
            &FormData_pg_amop {
                amopfamily: opfamilyoid,
                amoplefttype: op.lefttype,
                amoprighttype: op.righttype,
                amopstrategy: op.number as i16,
                amoppurpose: oppurpose,
                amopopr: op.object,
                amopmethod: amoid,
                amopsortfamily: op.sortfamily,
            },
        )?;

        /* Make its dependencies */
        let myself = ObjectAddress {
            classId: ACCESS_METHOD_OPERATOR_RELATION_ID,
            objectId: entryoid,
            objectSubId: 0,
        };

        /* see comments in amapi.h about dependency strength */
        recordDependencyOn::call(
            mcx,
            &myself,
            &ObjectAddress {
                classId: OPERATOR_RELATION_ID,
                objectId: op.object,
                objectSubId: 0,
            },
            if op.ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO },
        )?;

        recordDependencyOn::call(
            mcx,
            &myself,
            &ObjectAddress {
                classId: if op.ref_is_family {
                    OPERATOR_FAMILY_RELATION_ID
                } else {
                    OPERATOR_CLASS_RELATION_ID
                },
                objectId: op.refobjid,
                objectSubId: 0,
            },
            if op.ref_is_hard { DEPENDENCY_INTERNAL } else { DEPENDENCY_AUTO },
        )?;

        if typeDepNeeded(mcx, op.lefttype, op)? {
            recordDependencyOn::call(
                mcx,
                &myself,
                &ObjectAddress {
                    classId: TYPE_RELATION_ID,
                    objectId: op.lefttype,
                    objectSubId: 0,
                },
                if op.ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO },
            )?;
        }

        if op.lefttype != op.righttype && typeDepNeeded(mcx, op.righttype, op)? {
            recordDependencyOn::call(
                mcx,
                &myself,
                &ObjectAddress {
                    classId: TYPE_RELATION_ID,
                    objectId: op.righttype,
                    objectSubId: 0,
                },
                if op.ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO },
            )?;
        }

        /* A search operator also needs a dep on the referenced opfamily */
        if OidIsValid(op.sortfamily) {
            recordDependencyOn::call(
                mcx,
                &myself,
                &ObjectAddress {
                    classId: OPERATOR_FAMILY_RELATION_ID,
                    objectId: op.sortfamily,
                    objectSubId: 0,
                },
                if op.ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO },
            )?;
        }

        /* Post create hook of this access method operator */
        InvokeObjectPostCreateHook(ACCESS_METHOD_OPERATOR_RELATION_ID, entryoid, 0)?;
    }

    rel.close(types_storage::lock::RowExclusiveLock)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// storeProcedures
// ---------------------------------------------------------------------------

/// `storeProcedures(opfamilyname, amoid, opfamilyoid, procedures, isAdd)`
/// (opclasscmds.c:1583) — dump the support routines to pg_amproc with deps.
fn storeProcedures(
    mcx: Mcx<'_>,
    opfamilyname: &[StringNode],
    _amoid: Oid,
    opfamilyoid: Oid,
    procedures: &[OpFamilyMember],
    isAdd: bool,
) -> PgResult<()> {
    let opfamilyname_list = name_list(opfamilyname);

    let rel = table::table_open(
        mcx,
        ACCESS_METHOD_PROCEDURE_RELATION_ID,
        types_storage::lock::RowExclusiveLock,
    )?;

    for proc in procedures {
        /*
         * If adding to an existing family, check for conflict with an existing
         * pg_amproc entry (just to give a nicer error message)
         */
        if isAdd
            && OidIsValid(amproc_oid::call(
                opfamilyoid,
                proc.lefttype,
                proc.righttype,
                proc.number as i16,
            )?)
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!(
                    "function {}({},{}) already exists in operator family \"{}\"",
                    proc.number,
                    format_type_be::call(mcx, proc.lefttype)?.as_str(),
                    format_type_be::call(mcx, proc.righttype)?.as_str(),
                    NameListToString(mcx, &opfamilyname_list)?.as_str()
                ))
                .into_error());
        }

        /* Create the pg_amproc entry */
        let entryoid = catalog_tuple_insert_pg_amproc::call(
            &rel,
            &FormData_pg_amproc {
                amprocfamily: opfamilyoid,
                amproclefttype: proc.lefttype,
                amprocrighttype: proc.righttype,
                amprocnum: proc.number as i16,
                amproc: proc.object,
            },
        )?;

        /* Make its dependencies */
        let myself = ObjectAddress {
            classId: ACCESS_METHOD_PROCEDURE_RELATION_ID,
            objectId: entryoid,
            objectSubId: 0,
        };

        recordDependencyOn::call(
            mcx,
            &myself,
            &ObjectAddress {
                classId: PROCEDURE_RELATION_ID,
                objectId: proc.object,
                objectSubId: 0,
            },
            if proc.ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO },
        )?;

        recordDependencyOn::call(
            mcx,
            &myself,
            &ObjectAddress {
                classId: if proc.ref_is_family {
                    OPERATOR_FAMILY_RELATION_ID
                } else {
                    OPERATOR_CLASS_RELATION_ID
                },
                objectId: proc.refobjid,
                objectSubId: 0,
            },
            if proc.ref_is_hard { DEPENDENCY_INTERNAL } else { DEPENDENCY_AUTO },
        )?;

        if typeDepNeeded(mcx, proc.lefttype, proc)? {
            recordDependencyOn::call(
                mcx,
                &myself,
                &ObjectAddress {
                    classId: TYPE_RELATION_ID,
                    objectId: proc.lefttype,
                    objectSubId: 0,
                },
                if proc.ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO },
            )?;
        }

        if proc.lefttype != proc.righttype && typeDepNeeded(mcx, proc.righttype, proc)? {
            recordDependencyOn::call(
                mcx,
                &myself,
                &ObjectAddress {
                    classId: TYPE_RELATION_ID,
                    objectId: proc.righttype,
                    objectSubId: 0,
                },
                if proc.ref_is_hard { DEPENDENCY_NORMAL } else { DEPENDENCY_AUTO },
            )?;
        }

        /* Post create hook of access method procedure */
        InvokeObjectPostCreateHook(ACCESS_METHOD_PROCEDURE_RELATION_ID, entryoid, 0)?;
    }

    rel.close(types_storage::lock::RowExclusiveLock)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// typeDepNeeded
// ---------------------------------------------------------------------------

/// `typeDepNeeded(typid, member)` (opclasscmds.c:1699) — detect whether a
/// pg_amop or pg_amproc entry needs an explicit dependency on its lefttype or
/// righttype.
fn typeDepNeeded(mcx: Mcx<'_>, typid: Oid, member: &OpFamilyMember) -> PgResult<bool> {
    let mut result = true;

    /* If the type is pinned, we don't need a dependency. */
    if is_pinned_object::call(TYPE_RELATION_ID, typid) {
        return Ok(false);
    }

    /* Nope, so check the input types of the function or operator. */
    if member.is_func {
        let argtypes = get_func_signature::call(mcx, member.object)?;
        for &argtype in &argtypes {
            if typid == argtype {
                result = false; /* match, no dependency needed */
                break;
            }
        }
    } else {
        let (lefttype, righttype) = op_input_types::call(member.object)?;
        if typid == lefttype || typid == righttype {
            result = false; /* match, no dependency needed */
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// dropOperators / dropProcedures
// ---------------------------------------------------------------------------

/// `dropOperators(opfamilyname, amoid, opfamilyoid, operators)`
/// (opclasscmds.c:1749) — remove operator entries from an opfamily (always
/// RESTRICT, loose members only).
fn dropOperators(
    mcx: Mcx<'_>,
    opfamilyname: &[StringNode],
    opfamilyoid: Oid,
    operators: &[OpFamilyMember],
) -> PgResult<()> {
    let opfamilyname_list = name_list(opfamilyname);

    for op in operators {
        let amopid = amop_oid::call(opfamilyoid, op.lefttype, op.righttype, op.number as i16)?;
        if !OidIsValid(amopid) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "operator {}({},{}) does not exist in operator family \"{}\"",
                    op.number,
                    format_type_be::call(mcx, op.lefttype)?.as_str(),
                    format_type_be::call(mcx, op.righttype)?.as_str(),
                    NameListToString(mcx, &opfamilyname_list)?.as_str()
                ))
                .into_error());
        }

        perform_deletion::call(
            ACCESS_METHOD_OPERATOR_RELATION_ID,
            amopid,
            0,
            ::nodes::parsenodes::DROP_RESTRICT,
            0,
        )?;
    }
    Ok(())
}

/// `dropProcedures(opfamilyname, amoid, opfamilyoid, procedures)`
/// (opclasscmds.c:1789) — remove procedure entries from an opfamily.
fn dropProcedures(
    mcx: Mcx<'_>,
    opfamilyname: &[StringNode],
    opfamilyoid: Oid,
    procedures: &[OpFamilyMember],
) -> PgResult<()> {
    let opfamilyname_list = name_list(opfamilyname);

    for op in procedures {
        let amprocid = amproc_oid::call(opfamilyoid, op.lefttype, op.righttype, op.number as i16)?;
        if !OidIsValid(amprocid) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "function {}({},{}) does not exist in operator family \"{}\"",
                    op.number,
                    format_type_be::call(mcx, op.lefttype)?.as_str(),
                    format_type_be::call(mcx, op.righttype)?.as_str(),
                    NameListToString(mcx, &opfamilyname_list)?.as_str()
                ))
                .into_error());
        }

        perform_deletion::call(
            ACCESS_METHOD_PROCEDURE_RELATION_ID,
            amprocid,
            0,
            ::nodes::parsenodes::DROP_RESTRICT,
            0,
        )?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// IsThereOpClassInNamespace / IsThereOpFamilyInNamespace
// ---------------------------------------------------------------------------

/// `IsThereOpClassInNamespace(opcname, opcmethod, opcnamespace)`
/// (opclasscmds.c:1829) — subroutine for ALTER OPERATOR CLASS SET SCHEMA/RENAME.
pub fn IsThereOpClassInNamespace(
    mcx: Mcx<'_>,
    opcname: &str,
    opcmethod: Oid,
    opcnamespace: Oid,
) -> PgResult<()> {
    /* make sure the new name doesn't exist */
    if opclass_exists::call(opcmethod, opcname, opcnamespace)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "operator class \"{opcname}\" for access method \"{}\" already exists in schema \"{}\"",
                am_name_for_error(opcmethod),
                namespace_name_for_error(mcx, opcnamespace)?.unwrap_or_default()
            ))
            .into_error());
    }
    Ok(())
}

/// `IsThereOpFamilyInNamespace(opfname, opfmethod, opfnamespace)`
/// (opclasscmds.c:1852) — subroutine for ALTER OPERATOR FAMILY SET SCHEMA/RENAME.
pub fn IsThereOpFamilyInNamespace(
    mcx: Mcx<'_>,
    opfname: &str,
    opfmethod: Oid,
    opfnamespace: Oid,
) -> PgResult<()> {
    /* make sure the new name doesn't exist */
    if opfamily_exists::call(opfmethod, opfname, opfnamespace)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "operator family \"{opfname}\" for access method \"{}\" already exists in schema \"{}\"",
                am_name_for_error(opfmethod),
                namespace_name_for_error(mcx, opfnamespace)?.unwrap_or_default()
            ))
            .into_error());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// `Form_pg_operator` fields `assignOperTypes` reads.
struct OperatorFormFields {
    oprkind: u8,
    oprresult: Oid,
    oprleft: Oid,
    oprright: Oid,
}

/// `SearchSysCache1(OPEROID, operOid)` + `GETSTRUCT`, projected; `None` on a
/// cache miss.
fn operator_form(mcx: Mcx<'_>, oper_oid: Oid) -> PgResult<Option<OperatorFormFields>> {
    // The OperRow projection carries the fields assignOperTypes reads.
    match oper_row_by_oid::call(mcx, oper_oid)? {
        Some(row) => Ok(Some(OperatorFormFields {
            oprkind: row.oprkind,
            oprresult: row.oprresult,
            oprleft: row.oprleft,
            oprright: row.oprright,
        })),
        None => Ok(None),
    }
}

/// `Form_pg_proc` fields `assignProcTypes` reads.
struct ProcFormFields {
    prorettype: Oid,
    pronargs: i16,
    proargtypes: Vec<Oid>,
}

/// `SearchSysCache1(PROCOID, funcOid)` + `GETSTRUCT`, projected; `None` on a
/// cache miss.
fn proc_form(mcx: Mcx<'_>, func_oid: Oid) -> PgResult<Option<ProcFormFields>> {
    match proc_row_by_oid::call(mcx, func_oid)? {
        Some(row) => Ok(Some(ProcFormFields {
            prorettype: row.prorettype,
            pronargs: row.pronargs as i16,
            proargtypes: row.proargtypes.iter().copied().collect(),
        })),
        None => Ok(None),
    }
}

/// A fresh, zeroed `OpFamilyMember` (the C `palloc0(sizeof(OpFamilyMember))`).
fn new_member() -> OpFamilyMember {
    OpFamilyMember {
        is_func: false,
        object: InvalidOid,
        number: 0,
        lefttype: InvalidOid,
        righttype: InvalidOid,
        sortfamily: InvalidOid,
        ref_is_hard: false,
        ref_is_family: false,
        refobjid: InvalidOid,
    }
}

/// Unwrap a node that must be a `TypeName`, mirroring the C `(TypeName *)`
/// cast (the grammar guarantees it). Here the field is already typed, so this
/// is the `Some`-guaranteed unwrap with the internal-error fallback C would
/// reach only on a corrupt parse tree.
fn node_to_type_name(node: Option<&TypeName>) -> PgResult<&TypeName> {
    match node {
        Some(tn) => Ok(tn),
        None => Err(ereport(ERROR)
            .errmsg_internal("opclasscmds: expected a TypeName node")
            .into_error()),
    }
}

/// Unwrap a node that must be an `ObjectWithArgs`.
fn node_to_object_with_args(node: Option<&ObjectWithArgs>) -> PgResult<&ObjectWithArgs> {
    match node {
        Some(owa) => Ok(owa),
        None => Err(ereport(ERROR)
            .errmsg_internal("opclasscmds: item->name is not an ObjectWithArgs")
            .into_error()),
    }
}

/// `InvokeObjectPostCreateHook(classId, objectId, subId)` — the macro's
/// `if (object_access_hook)` guard plus `RunObjectPostCreateHook(..., false)`.
fn InvokeObjectPostCreateHook(class_id: Oid, object_id: Oid, sub_id: i32) -> PgResult<()> {
    if object_access_hook_present::call() {
        run_object_post_create_hook::call(class_id, object_id, sub_id, false)?;
    }
    Ok(())
}

/// `get_am_name(amoid)` rendered for an error message; `""` if unknown. Used
/// where the C interpolates `get_am_name(...)` directly into an `errmsg`.
fn am_name_for_error(amoid: Oid) -> String {
    let cx = mcx::MemoryContext::new("opclasscmds am_name");
    get_am_name::call(cx.mcx(), amoid)
        .ok()
        .flatten()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default()
}

/// `get_namespace_name(nspid)` for the ACL error path: `Some(name)` to the
/// `aclcheck_error` objname.
fn get_namespace_name_for_acl(mcx: Mcx<'_>, nspid: Oid) -> PgResult<Option<String>> {
    Ok(
        lsyscache_seams::get_namespace_name::call(mcx, nspid)?
            .map(|s| s.as_str().to_string()),
    )
}

/// `get_namespace_name(nspid)` rendered for an error message.
fn namespace_name_for_error(mcx: Mcx<'_>, nspid: Oid) -> PgResult<Option<String>> {
    Ok(
        lsyscache_seams::get_namespace_name::call(mcx, nspid)?
            .map(|s| s.as_str().to_string()),
    )
}

/// One scanned `pg_opclass` row: the heap TID (`tup->t_self`) plus the
/// `heap_deform_tuple` projection of the whole row (`GETSTRUCT(tup)`).
struct SysScanRow<'a> {
    #[allow(dead_code)]
    tid: ItemPointerData,
    cols: &'a [(Datum<'a>, bool)],
}

/// `systable_beginscan(rel, indexId, true, NULL, nkeys, key)` +
/// `while ((tup = systable_getnext(scan)))` + `systable_endscan(scan)`
/// (the genam iterator): invoke `body` once per matching row, in scan order.
/// `body` returning `Ok(true)` continues, `Ok(false)` stops early (the C
/// `break`); an `Err` propagates after the scan is ended (the `SysScanGuard`
/// `Drop` covers the error path). The deformed columns land in a scratch
/// context dropped at the end of each iteration.
fn systable_scan_foreach(
    rel: &rel::RelationData<'_>,
    index_id: Oid,
    keys: &[ScanKeyData],
    mut body: impl FnMut(&SysScanRow<'_>) -> PgResult<bool>,
) -> PgResult<()> {
    let mut scan = genam_seams::systable_beginscan::call(rel, index_id, true, None, keys)?;
    loop {
        let scratch = MemoryContext::new("systable_scan_foreach row");
        let smcx = scratch.mcx();
        let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
            break;
        };
        let cols = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let row = SysScanRow {
            tid: tup.tuple.t_self,
            cols: &cols,
        };
        let keep_going = body(&row)?;
        if !keep_going {
            break;
        }
    }
    scan.end()
}

/// Read a by-value `Oid` column from a deformed row (`GETSTRUCT(tup)->col`).
fn column_oid(row: &SysScanRow<'_>, attno: i16) -> Oid {
    match &row.cols[(attno - 1) as usize].0 {
        Datum::ByVal(d) => Datum::from_usize(*d).as_oid(),
        Datum::ByRef(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => InvalidOid,
    }
}

/// Read a by-value `bool` column from a deformed row.
fn column_bool(row: &SysScanRow<'_>, attno: i16) -> bool {
    match &row.cols[(attno - 1) as usize].0 {
        Datum::ByVal(d) => Datum::from_usize(*d).as_bool(),
        Datum::ByRef(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => false,
    }
}

/// Read a `NameData` column (`char[NAMEDATALEN]`, NUL-padded) from a deformed
/// row as a `String`, mirroring C's `NameStr(...)`.
fn column_name(row: &SysScanRow<'_>, attno: i16) -> String {
    match &row.cols[(attno - 1) as usize].0 {
        Datum::ByRef(bytes) => {
            let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
            String::from_utf8_lossy(&bytes[..end]).into_owned()
        }
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => String::new(),
    }
}
