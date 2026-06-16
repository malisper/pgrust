#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]
// pg_type.c uses the `!(a == b)` / `!(a == b || a == c)` alignment-validation
// idiom verbatim; keep the boolean shape identical to C.
#![allow(clippy::nonminimal_bool)]

//! Idiomatic port of `backend/catalog/pg_type.c` — routines to support
//! manipulation of the `pg_type` relation.
//!
//! Faithful 1:1 port of every C function: [`TypeShellMake`], [`TypeCreate`],
//! [`GenerateTypeDependencies`], [`RenameTypeInternal`], [`makeArrayTypeName`],
//! [`moveArrayTypeName`], [`makeMultirangeTypeName`] — original branch order,
//! validation, error codes/messages/SQLSTATE, and dependency-recording order
//! preserved.
//!
//! ## Shape of this port
//!
//!   * The decision logic — the `pg_type` column population, the
//!     pass-by-value/varlena/cstring alignment validation, the shell-type
//!     upgrade-vs-insert branch, the `GenerateTypeDependencies` recording order,
//!     and the array/multirange name generation — runs in-crate over owned
//!     values ([`TypeFormFields`] / [`Oid`] / [`String`]).
//!   * The catalog crates below `pg_type.c` are called **directly**:
//!     `recordDependencyOn` / `recordDependencyOnCurrentExtension` /
//!     `deleteDependencyRecordsFor` (`pg_depend.c`), `recordDependencyOnOwner` /
//!     `deleteSharedDependencyRecordsFor` (`pg_shdepend.c`),
//!     `new_object_addresses` / `add_exact_object_address` /
//!     `record_object_address_dependencies` / `recordDependencyOnExpr`
//!     (`dependency.c`), the `invoke_object_post_create_hook` /
//!     `invoke_object_post_alter_hook` hooks (`objectaccess.c`), and `table_open`
//!     / `Relation::close` (`access/table`).
//!   * The catalog-tuple value layer (`heap_form_tuple` / `heap_modify_tuple` /
//!     `GetNewOidWithIndex` / `CatalogTupleInsert/Update`) is owned by
//!     `catalog/indexing.c` and crosses through that owner's `-seams` crate. The
//!     syscache probes (`TYPENAMENSP` / `TYPEOID`), the lsyscache helpers, the
//!     ACL default + new-ACL dependency recording, `stringToNode`,
//!     `makeObjectName`, `pg_mbcliplen`, `CommandCounterIncrement`, and the
//!     binary-upgrade globals all cross their owners' seams (loud-panic until
//!     the owner lands).

extern crate alloc;

use alloc::string::{String, ToString};

use mcx::MemoryContext;

use types_array::ArrayType;
use types_catalog::catalog::{
    COLLATION_RELATION_ID, NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID, RELATION_RELATION_ID,
};
use types_catalog::catalog_dependency::{
    ObjectAddress, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL,
};
use types_catalog::pg_type::{
    type_create_fields, Anum_pg_type_oid, PgTypeInsertRow, TypeCreateParams, TypeFormFields,
    TypeOidIndexId, TypeRelationId,
    TYPTYPE_MULTIRANGE,
};
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_core::fmgr::F_OIDEQ;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::backend_access_common_heaptuple::Datum as HeapDatum;
use backend_utils_error::ereport;
use types_error::{PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
use types_storage::lock::RowExclusiveLock;
use types_tuple::heaptuple::{
    DEFAULT_COLLATION_OID, TYPALIGN_CHAR, TYPALIGN_DOUBLE, TYPALIGN_INT, TYPALIGN_SHORT,
    TYPSTORAGE_PLAIN,
};

use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_access_table_table::table_open;
use backend_catalog_dependency::{
    add_exact_object_address, new_object_addresses, record_object_address_dependencies,
    recordDependencyOnExpr,
};
use backend_catalog_objectaccess::{invoke_object_post_alter_hook, invoke_object_post_create_hook};
use backend_catalog_pg_depend::{deleteDependencyRecordsFor, recordDependencyOn};
use backend_catalog_pg_shdepend::{deleteSharedDependencyRecordsFor, recordDependencyOnOwner};

use backend_catalog_aclchk_seams::{aclcheck_error, get_user_default_acl, record_dependency_on_new_acl};
use backend_catalog_binary_upgrade_seams as binary_upgrade_seams;
use backend_catalog_indexing_seams as indexing_seams;
use backend_catalog_pg_depend_seams::recordDependencyOnCurrentExtension;
use backend_nodes_read_seams::string_to_node;
use backend_utils_cache_lsyscache_seams::{get_array_type, get_element_type, get_typisdefined};
use backend_utils_cache_syscache_seams::{get_type_oid, pg_type_form, type_exists};
use backend_utils_init_miscinit_seams::is_bootstrap_processing_mode;

use types_acl::acl::ACLCHECK_NOT_OWNER;
use types_nodes::parsenodes::OBJECT_TYPE;

/// `RELKIND_COMPOSITE_TYPE` (`catalog/pg_class.h`) — `pg_class.relkind` for a
/// stand-alone composite type.
const RELKIND_COMPOSITE_TYPE: i8 = types_tuple::access::RELKIND_COMPOSITE_TYPE as i8;

/// `NAMEDATALEN` (`pg_config_manual.h`).
const NAMEDATALEN: i32 = types_core::NAMEDATALEN;

/// `ObjectAddressSet(object, classId, objectId)` (objectaddress.h).
#[inline]
fn object_address(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// `namestrcpy(name, s)` — truncate `s` to a NUL-terminated `NameData`
/// (`NAMEDATALEN - 1` bytes max); the seam wraps the bytes into the on-disk
/// `NameData` Datum.
fn namestrcpy(s: &str) -> String {
    let limit = (NAMEDATALEN - 1) as usize;
    let take = limit.min(s.len());
    let mut end = take;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

/// `pnstrdup(s, len)` — a copy of the first `len` *bytes* of `s` (or all of `s`
/// if shorter); pg_type.c feeds only ASCII type names, so the UTF-8-boundary
/// floor is a no-op for those.
fn pnstrdup(s: &str, len: usize) -> String {
    let take = len.min(s.len());
    let mut end = take;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

/* ----------------------------------------------------------------
 *		TypeShellMake
 *
 *		Insert a "shell" tuple into the pg_type relation: valid but dummy
 *		values, `typisdefined` false.  The I/O functions for the type link
 *		to this tuple; the full CREATE TYPE later replaces the bogus values.
 * ----------------------------------------------------------------
 */
/// `TypeShellMake(typeName, typeNamespace, ownerId)` (pg_type.c:56-182).
pub fn TypeShellMake(typeName: &str, typeNamespace: Oid, ownerId: Oid) -> PgResult<ObjectAddress> {
    debug_assert!(!typeName.is_empty());

    let ctx = MemoryContext::new("TypeShellMake");
    let mcx = ctx.mcx();

    /* open pg_type */
    let pg_type_desc = table_open(mcx, TypeRelationId, RowExclusiveLock)?;

    /* dummy values (int4-like), typtype = pseudo (extra insurance) */
    let mut fields = TypeFormFields::shell(namestrcpy(typeName), typeNamespace, ownerId);

    /* Use binary-upgrade override for pg_type.oid? */
    let typoid: Oid = if binary_upgrade_seams::is_binary_upgrade::call() {
        let next = binary_upgrade_seams::consume_next_pg_type_oid::call();
        if !OidIsValid(next) {
            pg_type_desc.close(RowExclusiveLock)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("pg_type OID value not set when in binary upgrade mode")
                .into_error());
        }
        next
    } else {
        indexing_seams::get_new_oid_with_index_pg_type::call(&pg_type_desc)?
    };
    fields.oid = typoid;

    /* create a new type tuple and insert it (heap_form_tuple + CatalogTupleInsert) */
    let row = PgTypeInsertRow {
        fields: fields.clone(),
        typdefaultbin: None,
        typdefault: None,
        typacl: None,
    };
    indexing_seams::catalog_tuple_insert_pg_type::call(&pg_type_desc, &row)?;

    /* Create dependencies.  We can/must skip this in bootstrap mode. */
    if !is_bootstrap_processing_mode::call() {
        generate_type_dependencies(
            mcx,
            &fields,
            None,
            None,
            0,
            false,
            false,
            true, /* make extension dependency */
            false,
        )?;
    }

    /* Post creation hook for new shell type */
    invoke_object_post_create_hook(TypeRelationId, typoid, 0, false)?;

    /* clean up and return the type-oid */
    pg_type_desc.close(RowExclusiveLock)?;

    Ok(object_address(TypeRelationId, typoid))
}

/* ----------------------------------------------------------------
 *		TypeCreate
 *
 *		All the work to define a new type.  Returns the ObjectAddress.
 *		If newTypeOid is zero, a new OID is created; else use exactly it.
 * ----------------------------------------------------------------
 */
/// `TypeCreate(...)` (pg_type.c:194-520).
pub fn TypeCreate(params: TypeCreateParams) -> PgResult<ObjectAddress> {
    let internalSize = params.internal_size;
    let alignment = params.alignment;
    let storage = params.storage;
    let passedByValue = params.passed_by_value;

    /*
     * Validate size: positive (fixed) or -1 (varlena) or -2 (cstring).
     */
    if !(internalSize > 0 || internalSize == -1 || internalSize == -2) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!("invalid type internal size {internalSize}"))
            .into_error());
    }

    if passedByValue {
        /*
         * Pass-by-value types must have a fixed length supported by fetch_att()
         * and store_att_byval(), with matching alignment.  Must match
         * access/tupmacs.h!
         */
        if internalSize == core::mem::size_of::<i8>() as i16 {
            if alignment != TYPALIGN_CHAR {
                return Err(invalid_byval_alignment(alignment, internalSize));
            }
        } else if internalSize == core::mem::size_of::<i16>() as i16 {
            if alignment != TYPALIGN_SHORT {
                return Err(invalid_byval_alignment(alignment, internalSize));
            }
        } else if internalSize == core::mem::size_of::<i32>() as i16 {
            if alignment != TYPALIGN_INT {
                return Err(invalid_byval_alignment(alignment, internalSize));
            }
        }
        /* #if SIZEOF_DATUM == 8 */
        else if internalSize == core::mem::size_of::<u64>() as i16 {
            if alignment != TYPALIGN_DOUBLE {
                return Err(invalid_byval_alignment(alignment, internalSize));
            }
        }
        /* #endif */
        else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!(
                    "internal size {internalSize} is invalid for passed-by-value type"
                ))
                .into_error());
        }
    } else {
        /* varlena types must have int align or better */
        if internalSize == -1 && !(alignment == TYPALIGN_INT || alignment == TYPALIGN_DOUBLE) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!(
                    "alignment \"{}\" is invalid for variable-length type",
                    alignment as u8 as char
                ))
                .into_error());
        }
        /* cstring must have char alignment */
        if internalSize == -2 && !(alignment == TYPALIGN_CHAR) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!(
                    "alignment \"{}\" is invalid for variable-length type",
                    alignment as u8 as char
                ))
                .into_error());
        }
    }

    /* Only varlena types can be toasted */
    if storage != TYPSTORAGE_PLAIN && internalSize != -1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("fixed-size types must have storage PLAIN")
            .into_error());
    }

    /*
     * This is a dependent type if it's an implicitly-created array type or
     * multirange type, or a relation rowtype that's not a composite type.
     */
    let isDependentType: bool = params.is_implicit_array
        || params.type_type == TYPTYPE_MULTIRANGE
        || (OidIsValid(params.relation_oid) && params.relation_kind != RELKIND_COMPOSITE_TYPE);

    /* fixed-part values (namestrcpy applied to typname) */
    let mut fields = type_create_fields(&params);
    fields.typname = namestrcpy(&params.type_name);

    /*
     * Initialize the type's ACL.  Dependent types don't get one.
     */
    let typacl: Option<ArrayType> = if isDependentType {
        None
    } else {
        get_user_default_acl::call(OBJECT_TYPE, params.owner_id, params.type_namespace)?
    };

    let ctx = MemoryContext::new("TypeCreate");
    let mcx = ctx.mcx();

    /*
     * open pg_type and prepare to insert or update a row.  (Update will not
     * work in bootstrap mode, but we don't expect to overwrite shells then.)
     */
    let pg_type_desc = table_open(mcx, TypeRelationId, RowExclusiveLock)?;

    let typeObjectId: Oid;
    let mut rebuildDeps = false;

    let existing = get_type_oid::call(&params.type_name, params.type_namespace)?;
    if OidIsValid(existing) {
        /* fetch the existing (shell) row to inspect typisdefined/typowner */
        let typform = fetch_type_form_internal(existing)?;

        /* not already defined?  (It may exist as a shell type.) */
        if typform.typisdefined {
            pg_type_desc.close(RowExclusiveLock)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("type \"{}\" already exists", params.type_name))
                .into_error());
        }

        /* shell type must have been created by the same owner */
        if typform.typowner != params.owner_id {
            pg_type_desc.close(RowExclusiveLock)?;
            /* aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TYPE, typeName) — always raises */
            aclcheck_error::call(
                ACLCHECK_NOT_OWNER,
                OBJECT_TYPE,
                Some(params.type_name.clone()),
            )?;
            unreachable!("aclcheck_error(ACLCHECK_NOT_OWNER, ...) always raises");
        }

        /* trouble if caller wanted to force the OID */
        if OidIsValid(params.new_type_oid) {
            pg_type_desc.close(RowExclusiveLock)?;
            return Err(ereport(ERROR)
                .errmsg("cannot assign new OID to existing shell type")
                .into_error());
        }

        typeObjectId = typform.oid;
        fields.oid = typeObjectId;

        /* Okay to update existing shell type tuple (replaces all but oid). */
        let row = build_insert_row(&params, fields.clone(), &typacl)?;
        indexing_seams::catalog_tuple_update_pg_type::call(&pg_type_desc, &row)?;

        rebuildDeps = true; /* get rid of shell type's dependencies */
    } else {
        /* Force the OID if requested by caller */
        if OidIsValid(params.new_type_oid) {
            typeObjectId = params.new_type_oid;
        }
        /* Use binary-upgrade override for pg_type.oid, if supplied. */
        else if binary_upgrade_seams::is_binary_upgrade::call() {
            let next = binary_upgrade_seams::consume_next_pg_type_oid::call();
            if !OidIsValid(next) {
                pg_type_desc.close(RowExclusiveLock)?;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg("pg_type OID value not set when in binary upgrade mode")
                    .into_error());
            }
            typeObjectId = next;
        } else {
            typeObjectId = indexing_seams::get_new_oid_with_index_pg_type::call(&pg_type_desc)?;
        }

        fields.oid = typeObjectId;

        let row = build_insert_row(&params, fields.clone(), &typacl)?;
        indexing_seams::catalog_tuple_insert_pg_type::call(&pg_type_desc, &row)?;
    }

    /* Create dependencies.  We can/must skip this in bootstrap mode. */
    if !is_bootstrap_processing_mode::call() {
        generate_type_dependencies(
            mcx,
            &fields,
            params.default_type_bin.clone(),
            typacl.clone(),
            params.relation_kind,
            params.is_implicit_array,
            isDependentType,
            true, /* make extension dependency */
            rebuildDeps,
        )?;
    }

    /* Post creation hook for new type */
    invoke_object_post_create_hook(TypeRelationId, typeObjectId, 0, false)?;

    pg_type_desc.close(RowExclusiveLock)?;

    Ok(object_address(TypeRelationId, typeObjectId))
}

/// `errmsg("alignment \"%c\" is invalid for passed-by-value type of size %d")`.
fn invalid_byval_alignment(alignment: i8, internal_size: i16) -> types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
        .errmsg(format!(
            "alignment \"{}\" is invalid for passed-by-value type of size {}",
            alignment as u8 as char, internal_size
        ))
        .into_error()
}

/// Assemble the [`PgTypeInsertRow`] from a `TypeCreate` `params` + the assigned
/// fixed `fields` + the resolved `typacl` (`CStringGetTextDatum` of the cooked
/// / human-readable defaults reduces to carrying the owned text).
fn build_insert_row(
    params: &TypeCreateParams,
    fields: TypeFormFields,
    typacl: &Option<ArrayType>,
) -> PgResult<PgTypeInsertRow> {
    Ok(PgTypeInsertRow {
        fields,
        typdefaultbin: params.default_type_bin.clone(),
        typdefault: params.default_type_value.clone(),
        typacl: typacl.clone(),
    })
}

/*
 * GenerateTypeDependencies: build the dependencies needed for a type.
 *
 * (See pg_type.c:522-555 for the full contract on the flags.)
 */
/// `GenerateTypeDependencies(...)` (pg_type.c:556-753), value-typed: the formed
/// `pg_type` row crosses as [`TypeFormFields`]; `default_expr_bin` is the cooked
/// default's `nodeToString` text (`None` = SQL NULL); `typacl` the ACL array.
fn generate_type_dependencies(
    mcx: mcx::Mcx<'_>,
    typeForm: &TypeFormFields,
    default_expr_bin: Option<String>,
    typacl: Option<ArrayType>,
    relationKind: i8,
    isImplicitArray: bool,
    isDependentType: bool,
    makeExtensionDep: bool,
    rebuild: bool,
) -> PgResult<()> {
    let typeObjectId = typeForm.oid;

    /* If rebuild, first flush old dependencies, except extension deps */
    if rebuild {
        deleteDependencyRecordsFor(TypeRelationId, typeObjectId, true)?;
        deleteSharedDependencyRecordsFor(TypeRelationId, typeObjectId, 0)?;
    }

    let myself = object_address(TypeRelationId, typeObjectId);

    /*
     * Dependencies on namespace, owner, ACL.  Skip for a dependent type (it has
     * them indirectly), except multiranges still need a namespace dependency.
     */
    let mut addrs_normal = new_object_addresses();

    if !isDependentType || typeForm.typtype == TYPTYPE_MULTIRANGE {
        let referenced = object_address(NAMESPACE_RELATION_ID, typeForm.typnamespace);
        add_exact_object_address(&referenced, &mut addrs_normal);
    }

    if !isDependentType {
        recordDependencyOnOwner(TypeRelationId, typeObjectId, typeForm.typowner)?;
        record_dependency_on_new_acl::call(
            TypeRelationId,
            typeObjectId,
            0,
            typeForm.typowner,
            typacl,
        )?;
    }

    /* Extension dependency if requested. */
    if makeExtensionDep {
        recordDependencyOnCurrentExtension::call(mcx, &myself, rebuild)?;
    }

    /* Normal dependencies on the I/O and support functions */
    for proc in [
        typeForm.typinput,
        typeForm.typoutput,
        typeForm.typreceive,
        typeForm.typsend,
        typeForm.typmodin,
        typeForm.typmodout,
        typeForm.typanalyze,
        typeForm.typsubscript,
    ] {
        if OidIsValid(proc) {
            let referenced = object_address(PROCEDURE_RELATION_ID, proc);
            add_exact_object_address(&referenced, &mut addrs_normal);
        }
    }

    /* Normal dependency from a domain to its base type. */
    if OidIsValid(typeForm.typbasetype) {
        let referenced = object_address(TypeRelationId, typeForm.typbasetype);
        add_exact_object_address(&referenced, &mut addrs_normal);
    }

    /*
     * Normal dependency from a domain to its collation.  The default collation
     * is pinned, so don't bother recording it.
     */
    if OidIsValid(typeForm.typcollation) && typeForm.typcollation != DEFAULT_COLLATION_OID {
        let referenced = object_address(COLLATION_RELATION_ID, typeForm.typcollation);
        add_exact_object_address(&referenced, &mut addrs_normal);
    }

    record_object_address_dependencies(&myself, &mut addrs_normal, DEPENDENCY_NORMAL)?;
    /* free_object_addresses: the owned ObjectAddresses drops here. */
    drop(addrs_normal);

    /* Normal dependency on the default expression. */
    if let Some(bin) = default_expr_bin {
        let expr = string_to_node::call(mcx, &bin)?;
        recordDependencyOnExpr(&myself, &expr, &[], DEPENDENCY_NORMAL)?;
    }

    /*
     * If the type is a rowtype for a relation, mark it as internally dependent
     * on the relation, unless it is a stand-alone composite type relation (then
     * reverse the dependency).
     */
    if OidIsValid(typeForm.typrelid) {
        let referenced = object_address(RELATION_RELATION_ID, typeForm.typrelid);
        if relationKind != RELKIND_COMPOSITE_TYPE {
            recordDependencyOn(mcx, &myself, &referenced, DEPENDENCY_INTERNAL)?;
        } else {
            recordDependencyOn(mcx, &referenced, &myself, DEPENDENCY_INTERNAL)?;
        }
    }

    /*
     * If the type is an implicitly-created array type, mark it as internally
     * dependent on the element type; otherwise the dependency is normal.
     */
    if OidIsValid(typeForm.typelem) {
        let referenced = object_address(TypeRelationId, typeForm.typelem);
        recordDependencyOn(
            mcx,
            &myself,
            &referenced,
            if isImplicitArray {
                DEPENDENCY_INTERNAL
            } else {
                DEPENDENCY_NORMAL
            },
        )?;
    }

    /*
     * Note: a multirange's internal dependency on its range type is recorded by
     * RangeCreate(), not here (see pg_type.c:744-752).
     */
    Ok(())
}

/*
 * RenameTypeInternal
 *		Rename a type and any associated array type.  Caller checked privs.
 */
/// `RenameTypeInternal(typeOid, newTypeName, typeNamespace)` (pg_type.c:764-830).
pub fn RenameTypeInternal(typeOid: Oid, newTypeName: &str, typeNamespace: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("RenameTypeInternal");
    let mcx = ctx.mcx();

    let pg_type_desc = table_open(mcx, TypeRelationId, RowExclusiveLock)?;

    let typ = fetch_type_form_internal(typeOid)?;

    /* We are not supposed to be changing schemas here */
    debug_assert_eq!(typeNamespace, typ.typnamespace);

    let arrayOid = typ.typarray;

    /* Check for a conflicting type name. */
    let oldTypeOid = get_type_oid::call(newTypeName, typeNamespace)?;

    /*
     * If there is one, see if it's an autogenerated array type and rename it
     * out of the way (skip for a shell type — moveArrayTypeName misbehaves
     * there).  Otherwise give a friendlier error than unique-index violation.
     */
    if OidIsValid(oldTypeOid) {
        if get_typisdefined::call(oldTypeOid)?
            && moveArrayTypeName(oldTypeOid, newTypeName, typeNamespace)?
        {
            /* successfully dodged the problem */
        } else {
            pg_type_desc.close(RowExclusiveLock)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("type \"{newTypeName}\" already exists"))
                .into_error());
        }
    }

    /* OK, do the rename (namestrcpy + CatalogTupleUpdate on the held tuple). */
    let new_name = namestrcpy(newTypeName);
    indexing_seams::catalog_tuple_update_typname_pg_type::call(&pg_type_desc, typeOid, &new_name)?;

    invoke_object_post_alter_hook(TypeRelationId, typeOid, 0, InvalidOid, false)?;

    pg_type_desc.close(RowExclusiveLock)?;

    /*
     * If the type has an array type, recurse — unless we already renamed that
     * array type above (eg renaming "foo" to "_foo").
     */
    if OidIsValid(arrayOid) && arrayOid != oldTypeOid {
        let arrname = makeArrayTypeName(newTypeName, typeNamespace)?;
        RenameTypeInternal(arrayOid, &arrname, typeNamespace)?;
    }

    Ok(())
}

/*
 * makeArrayTypeName
 *	  Given a base type name, make an array type name for it.
 */
/// `makeArrayTypeName(typeName, typeNamespace)` (pg_type.c:839-877).
pub fn makeArrayTypeName(typeName: &str, typeNamespace: Oid) -> PgResult<String> {
    let mut pass: i32 = 0;

    /* First, try with no numeric suffix (makeObjectName with empty name1). */
    let mut arr_name = make_array_object_name(typeName, None)?;

    loop {
        if !type_exists::call(&arr_name, typeNamespace)? {
            break;
        }
        /* That attempt conflicted.  Prepare a new name with some digits. */
        pass += 1;
        arr_name = make_array_object_name(typeName, Some(pass))?;
    }

    Ok(arr_name)
}

/// `makeObjectName("", typeName, suffix)` — array names farm out to
/// makeObjectName with an empty first component (pg_type.c:861/873).
fn make_array_object_name(typeName: &str, pass: Option<i32>) -> PgResult<String> {
    let label = pass.map(|p| p.to_string()).unwrap_or_default();
    backend_commands_indexcmds_seams::make_object_name::call("", typeName, &label)
}

/*
 * moveArrayTypeName
 *	  Try to reassign an array type name the user wants to use.
 *
 * Returns true if successfully moved (also true for a shell type).
 */
/// `moveArrayTypeName(typeOid, typeName, typeNamespace)` (pg_type.c:904-940).
pub fn moveArrayTypeName(typeOid: Oid, typeName: &str, typeNamespace: Oid) -> PgResult<bool> {
    /* We need do nothing if it's a shell type. */
    if !get_typisdefined::call(typeOid)? {
        return Ok(true);
    }

    /* Can't change it if it's not an autogenerated array type. */
    let elemOid = get_element_type::call(typeOid)?.unwrap_or(InvalidOid);
    if !OidIsValid(elemOid) || get_array_type::call(elemOid)?.unwrap_or(InvalidOid) != typeOid {
        return Ok(false);
    }

    /*
     * Use makeArrayTypeName to pick an unused modification of the name.
     */
    let newname = makeArrayTypeName(typeName, typeNamespace)?;

    /* Apply the rename */
    RenameTypeInternal(typeOid, &newname, typeNamespace)?;

    /*
     * Bump the command counter so a subsequent makeArrayTypeName sees this.
     */
    backend_access_transam_xact_seams::command_counter_increment::call()?;

    Ok(true)
}

/*
 * makeMultirangeTypeName
 *	  Given a range type name, make a multirange type name for it.
 */
/// `makeMultirangeTypeName(rangeTypeName, typeNamespace)` (pg_type.c:949-982).
pub fn makeMultirangeTypeName(rangeTypeName: &str, typeNamespace: Oid) -> PgResult<String> {
    /*
     * If the range type name contains "range" change that to "multirange";
     * otherwise add "_multirange" to the end.
     */
    let mut buf: String = if let Some(offset) = rangeTypeName.find("range") {
        let prefix = &rangeTypeName[..offset];
        let rangestr = &rangeTypeName[offset..];
        format!("{prefix}multi{rangestr}")
    } else {
        let clipped = pnstrdup(rangeTypeName, (NAMEDATALEN - 12) as usize);
        format!("{clipped}_multirange")
    };

    /* clip it at NAMEDATALEN-1 bytes */
    let buflen = buf.len() as i32;
    let clip = backend_utils_mb_mbutils_seams::pg_mbcliplen::call(buf.as_bytes(), buflen, NAMEDATALEN - 1);
    buf.truncate(clip as usize);

    if type_exists::call(&buf, typeNamespace)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("type \"{buf}\" already exists"))
            .errdetail(format!(
                "Failed while creating a multirange type for type \"{rangeTypeName}\"."
            ))
            .errhint(
                "You can manually specify a multirange type name using the \"multirange_type_name\" attribute.",
            )
            .into_error());
    }

    Ok(buf)
}

/// `SearchSysCacheCopy1(TYPEOID, typeOid)` + `GETSTRUCT(Form_pg_type)` projected
/// to the value-typed [`TypeFormFields`]; raises `cache lookup failed for type
/// %u` (`elog(ERROR)`) when the row is absent, mirroring the C
/// `!HeapTupleIsValid` branch. The fixed columns come from the syscache
/// `pg_type_form` projection.
fn fetch_type_form_internal(type_oid: Oid) -> PgResult<TypeFormFields> {
    match pg_type_form::call(type_oid)? {
        Some(f) => Ok(form_to_fields(&f)),
        None => Err(ereport(ERROR)
            .errmsg(format!("cache lookup failed for type {type_oid}"))
            .into_error()),
    }
}

/// Project the on-disk [`FormData_pg_type`](types_tuple::pg_type::FormData_pg_type)
/// fixed-part struct onto the owned [`TypeFormFields`] (the `RegProcedure`
/// columns are `Oid`s; `typname` `NameData` → `String`).
fn form_to_fields(f: &types_tuple::pg_type::FormData_pg_type) -> TypeFormFields {
    TypeFormFields {
        oid: f.oid,
        typname: String::from_utf8_lossy(f.typname.name_str()).into_owned(),
        typnamespace: f.typnamespace,
        typowner: f.typowner,
        typlen: f.typlen,
        typbyval: f.typbyval,
        typtype: f.typtype,
        typcategory: f.typcategory,
        typispreferred: f.typispreferred,
        typisdefined: f.typisdefined,
        typdelim: f.typdelim,
        typrelid: f.typrelid,
        typsubscript: f.typsubscript,
        typelem: f.typelem,
        typarray: f.typarray,
        typinput: f.typinput,
        typoutput: f.typoutput,
        typreceive: f.typreceive,
        typsend: f.typsend,
        typmodin: f.typmodin,
        typmodout: f.typmodout,
        typanalyze: f.typanalyze,
        typalign: f.typalign,
        typstorage: f.typstorage,
        typnotnull: f.typnotnull,
        typbasetype: f.typbasetype,
        typtypmod: f.typtypmod,
        typndims: f.typndims,
        typcollation: f.typcollation,
    }
}

/// Install this unit's inward seams ([`backend_catalog_pg_type_seams`]) and the
/// cross-crate `type_shell_make` decl that `functioncmds.c` consumes.
/// `get_new_type_oid()` — the non-binary-upgrade OID source shared by
/// `typecmds.c`'s `AssignType{Array,Multirange,MultirangeArray}Oid`:
/// `table_open(TypeRelationId, AccessShareLock)` +
/// `GetNewOidWithIndex(pg_type, TypeOidIndexId, Anum_pg_type_oid)` +
/// `table_close(...)`.
fn get_new_type_oid() -> PgResult<Oid> {
    let ctx = MemoryContext::new("AssignTypeOid");
    /* The C opens AccessShareLock; GetNewOidWithIndex only reads the index. */
    let pg_type_desc = table_open(ctx.mcx(), TypeRelationId, RowExclusiveLock)?;
    let oid = indexing_seams::get_new_oid_with_index_pg_type::call(&pg_type_desc)?;
    pg_type_desc.close(RowExclusiveLock)?;
    Ok(oid)
}

/// Guts of `RemoveTypeById` (typecmds.c:656) on the pg_type side:
/// `table_open(TypeRelationId, RowExclusiveLock)` + `SearchSysCache1(TYPEOID)` +
/// `CatalogTupleDelete(&tup->t_self)` + `table_close`. Returns the deleted
/// row's `typtype` so the caller can do the by-hand enum/range cleanup.
///
/// The cached form (for `typtype`) is read via the `pg_type_form` projection
/// before the row is removed; the tuple itself is located by a `systable` scan
/// on `TypeOidIndexId` to obtain its `t_self` for the catalog delete (the
/// repo's value-typed equivalent of `SearchSysCache1` + `&tup->t_self`).
fn remove_type_catalog_row(typeOid: Oid) -> PgResult<i8> {
    /* Read typtype from the cached form before deleting (elog on miss). */
    let typtype = match pg_type_form::call(typeOid)? {
        Some(form) => form.typtype,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for type {typeOid}"))
                .into_error());
        }
    };

    let ctx = MemoryContext::new("RemoveTypeById");
    let relation = table_open(ctx.mcx(), TypeRelationId, RowExclusiveLock)?;

    let key = [{
        let mut k = ScanKeyData::empty();
        ScanKeyInit(
            &mut k,
            Anum_pg_type_oid,
            BTEqualStrategyNumber,
            F_OIDEQ,
            HeapDatum::from_oid(typeOid),
        )?;
        k
    }];

    let mut scan =
        genam_seams::systable_beginscan::call(&relation, TypeOidIndexId, true, None, &key)?;
    let scratch = MemoryContext::new("RemoveTypeById scan row");
    if let Some(tup) = genam_seams::systable_getnext::call(scratch.mcx(), scan.desc_mut())? {
        /* CatalogTupleDelete(relation, &tup->t_self); */
        indexing_seams::catalog_tuple_delete::call(&relation, tup.tuple.t_self)?;
    } else {
        scan.end()?;
        relation.close(RowExclusiveLock)?;
        return Err(ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for type {typeOid}"))
            .into_error());
    }
    scan.end()?;

    relation.close(RowExclusiveLock)?;

    Ok(typtype)
}

// ===========================================================================
// F3/F4 single-row pg_type mutators (commands/typecmds.c ALTER TYPE/DOMAIN).
//
// Each owns the pg_type WRITE for one ALTER path: open pg_type
// (RowExclusiveLock), read the row's form for any values needed, call the
// narrow `catalog_tuple_update_*_pg_type` indexing seam (heap_modify_tuple over
// only the targeted columns), then perform the GenerateTypeDependencies +
// InvokeObjectPostAlterHook calls where C does, and close. typecmds does NO
// datum writes and NO GenerateTypeDependencies of its own.
// ===========================================================================

/// `AlterTypeOwnerInternal`'s SINGLE-ROW write (typecmds.c:3985): set
/// `typowner = new_owner_id` and (when typacl non-NULL)
/// `typacl = aclnewowner(...)` on the held tuple, then `CatalogTupleUpdate`. No
/// recursion to array/multirange here (typecmds does that).
fn set_type_owner(type_oid: Oid, new_owner_id: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("AlterTypeOwnerInternal");
    let rel = table_open(ctx.mcx(), TypeRelationId, RowExclusiveLock)?;
    indexing_seams::catalog_tuple_update_typowner_typacl_pg_type::call(
        &rel,
        type_oid,
        new_owner_id,
    )?;
    rel.close(RowExclusiveLock)?;
    Ok(())
}

/// `AlterTypeNamespaceInternal`'s single-row `typnamespace` write
/// (typecmds.c:4233) + `CatalogTupleUpdate`. typecmds calls this only when
/// `oldNspOid != nspOid`.
fn set_type_namespace(type_oid: Oid, nsp_oid: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("AlterTypeNamespaceInternal");
    let rel = table_open(ctx.mcx(), TypeRelationId, RowExclusiveLock)?;
    indexing_seams::catalog_tuple_update_typnamespace_pg_type::call(&rel, type_oid, nsp_oid)?;
    rel.close(RowExclusiveLock)?;
    Ok(())
}

/// AlterDomain* single-row `typnotnull` write + `CatalogTupleUpdate`.
fn set_type_not_null(type_oid: Oid, not_null: bool) -> PgResult<()> {
    let ctx = MemoryContext::new("AlterDomainNotNull");
    let rel = table_open(ctx.mcx(), TypeRelationId, RowExclusiveLock)?;
    indexing_seams::catalog_tuple_update_typnotnull_pg_type::call(&rel, type_oid, not_null)?;
    rel.close(RowExclusiveLock)?;
    Ok(())
}

/// `AlterDomainDefault`'s pg_type write (typecmds.c:2707): replace
/// `typdefault`/`typdefaultbin` (`None` = SQL NULL), `CatalogTupleUpdate`, then
/// `GenerateTypeDependencies(newtuple, defaultExpr, NULL, 0, false, false,
/// false, true)` + `InvokeObjectPostAlterHook`.
fn set_domain_default(
    type_oid: Oid,
    default_value: Option<String>,
    default_bin: Option<String>,
) -> PgResult<()> {
    let ctx = MemoryContext::new("AlterDomainDefault");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, TypeRelationId, RowExclusiveLock)?;

    indexing_seams::catalog_tuple_update_typdefault_pg_type::call(
        &rel,
        type_oid,
        default_value,
        default_bin.clone(),
    )?;

    /* Rebuild dependencies (GenerateTypeDependencies on the new tuple). The
     * cooked defaultExpr crosses as its nodeToString binary text. */
    let typeForm = fetch_type_form_internal(type_oid)?;
    generate_type_dependencies(
        mcx,
        &typeForm,
        default_bin, /* defaultExpr (binary text) */
        None,        /* don't have typacl handy */
        0,           /* relation kind is n/a */
        false,       /* a domain isn't an implicit array */
        false,       /* nor is it any kind of dependent type */
        false,       /* don't touch extension membership */
        true,        /* We do need to rebuild dependencies */
    )?;

    invoke_object_post_alter_hook(TypeRelationId, type_oid, 0, InvalidOid, false)?;

    rel.close(RowExclusiveLock)?;
    Ok(())
}

/// `AlterTypeRecurse`'s per-row update (typecmds.c:4561): the gated `replaces[]`
/// write via the narrow indexing seam, `GenerateTypeDependencies(newtup, NULL,
/// NULL, 0, is_implicit_array, is_implicit_array, false, true)`,
/// `InvokeObjectPostAlterHook`. Returns the row's `typarray` OID so typecmds can
/// recurse to the array type.
fn alter_type_recurse_update(
    type_oid: Oid,
    is_implicit_array: bool,
    attr: types_catalog::pg_type::TypeAttrUpdate,
) -> PgResult<Oid> {
    let ctx = MemoryContext::new("AlterTypeRecurse");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, TypeRelationId, RowExclusiveLock)?;

    let arrtypoid =
        indexing_seams::catalog_tuple_update_attrs_pg_type::call(&rel, type_oid, attr)?;

    /* Rebuild dependencies for this type. */
    let typeForm = fetch_type_form_internal(type_oid)?;
    generate_type_dependencies(
        mcx,
        &typeForm,
        None,              /* don't have defaultExpr handy */
        None,              /* don't have typacl handy */
        0,                 /* we rejected composite types above */
        is_implicit_array, /* it might be an array */
        is_implicit_array, /* dependent iff it's array */
        false,             /* don't touch extension membership */
        true,
    )?;

    invoke_object_post_alter_hook(TypeRelationId, type_oid, 0, InvalidOid, false)?;

    rel.close(RowExclusiveLock)?;
    Ok(arrtypoid)
}

/// `AlterTypeRecurse`'s domain scan (typecmds.c:4682): `systable_beginscan(
/// pg_type, InvalidOid, false, NULL, 1, key[typbasetype = base_type_oid])`,
/// returning the OIDs of rows with `typbasetype = base_type_oid` AND
/// `typtype == TYPTYPE_DOMAIN`. The full-table (non-index) scan + per-row
/// `GETSTRUCT` deform run here; typecmds re-fires `AlterTypeRecurse` over each.
fn scan_domains_over_basetype(base_type_oid: Oid) -> PgResult<Vec<Oid>> {
    use types_catalog::pg_type::{Anum_pg_type_typbasetype, TYPTYPE_DOMAIN};

    let ctx = MemoryContext::new("scan_domains_over_basetype");
    let relation = table_open(ctx.mcx(), TypeRelationId, RowExclusiveLock)?;

    let key = [{
        let mut k = ScanKeyData::empty();
        ScanKeyInit(
            &mut k,
            Anum_pg_type_typbasetype,
            BTEqualStrategyNumber,
            F_OIDEQ,
            HeapDatum::from_oid(base_type_oid),
        )?;
        k
    }];

    /* systable_beginscan(catalog, InvalidOid, false, NULL, 1, key) — a
     * non-index full scan over pg_type (there is no index on typbasetype). */
    let mut scan =
        genam_seams::systable_beginscan::call(&relation, InvalidOid, false, None, &key)?;

    let mut result: Vec<Oid> = Vec::new();
    loop {
        let scratch = MemoryContext::new("scan_domains_over_basetype row");
        let Some(tup) = genam_seams::systable_getnext::call(scratch.mcx(), scan.desc_mut())? else {
            break;
        };

        /* GETSTRUCT(Form_pg_type): deform the scanned tuple against pg_type's
         * descriptor and read oid (attnum 1) + typtype (attnum 7). */
        let cols = backend_access_common_heaptuple::heap_deform_tuple(
            scratch.mcx(),
            &tup.tuple,
            &relation.rd_att,
            &tup.data,
        )?;

        let domain_oid = cols[(Anum_pg_type_oid - 1) as usize].0.as_oid();
        let typtype = cols[(types_catalog::pg_type::Anum_pg_type_typtype - 1) as usize]
            .0
            .as_char();

        /* Shouldn't have a nonzero typbasetype in a non-domain, but let's check */
        if typtype != TYPTYPE_DOMAIN {
            continue;
        }
        result.push(domain_oid);
    }
    scan.end()?;

    relation.close(RowExclusiveLock)?;
    Ok(result)
}

pub fn init_seams() {
    use backend_catalog_pg_type_seams as s;
    s::get_new_type_oid::set(get_new_type_oid);
    s::set_type_owner::set(set_type_owner);
    s::set_type_namespace::set(set_type_namespace);
    s::set_type_not_null::set(set_type_not_null);
    s::set_domain_default::set(|type_oid, default_value, default_bin| {
        set_domain_default(type_oid, default_value, default_bin)
    });
    s::alter_type_recurse_update::set(alter_type_recurse_update);
    s::scan_domains_over_basetype::set(scan_domains_over_basetype);
    s::remove_type_catalog_row::set(remove_type_catalog_row);
    s::type_create::set(TypeCreate);
    s::rename_type_internal::set(|type_oid, new_name, nsp| {
        RenameTypeInternal(type_oid, &new_name, nsp)
    });
    s::make_array_type_name::set(|name, nsp| makeArrayTypeName(&name, nsp));
    s::move_array_type_name::set(|oid, name, nsp| moveArrayTypeName(oid, &name, nsp));
    s::make_multirange_type_name::set(|name, nsp| makeMultirangeTypeName(&name, nsp));
    s::generate_type_dependencies::set(
        |type_form, default_expr_bin, typacl, relation_kind, is_implicit_array, is_dependent_type, make_extension_dep, rebuild| {
            let ctx = MemoryContext::new("GenerateTypeDependencies");
            generate_type_dependencies(
                ctx.mcx(),
                &type_form,
                default_expr_bin,
                typacl,
                relation_kind,
                is_implicit_array,
                is_dependent_type,
                make_extension_dep,
                rebuild,
            )
        },
    );

    /* `TypeShellMake` is consumed by functioncmds.c; its decl lives in that
     * unit's first-consumer seam crate. Install it cross-crate (pg_type.c owns
     * the C function). */
    backend_commands_functioncmds_seams::type_shell_make::set(|typname, nsp, owner| {
        TypeShellMake(&typname, nsp, owner)
    });
}
