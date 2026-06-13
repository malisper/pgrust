#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! Port of `backend/catalog/pg_namespace.c` — routines to support manipulation
//! of the `pg_namespace` relation.
//!
//! Exported (non-static) C functions:
//!   * [`NamespaceCreate`] (pg_namespace.c) — in-crate.
//!
//! The control flow is reproduced exactly. The cross-subsystem externals cross
//! through their owners' `-seams` crates: the `SearchSysCacheExists1`
//! duplicate-name probe (`backend-utils-cache-syscache`), `get_user_default_acl`
//! / `recordDependencyOnNewAcl` (`backend-catalog-aclchk`), the
//! `GetNewOidWithIndex` + `namestrcpy` + `heap_form_tuple` + `CatalogTupleInsert`
//! value layer (`backend-catalog-indexing`), `recordDependencyOnOwner`
//! (`backend-catalog-pg-shdepend`), `recordDependencyOnCurrentExtension`
//! (`backend-catalog-pg-depend`), and the `InvokeObjectPostCreateHook` hook
//! (`backend-catalog-objectaccess`). The relation is opened directly through
//! `backend-access-table-table::table_open`, which returns the owned
//! `Relation` handle.
//!
//! The default ACL (`Acl *` = `ArrayType`) crosses opaquely from its producer
//! (`get_user_default_acl`) to its consumers (the row-form value layer and
//! `recordDependencyOnNewAcl`); `None` is the C `nspacl == NULL`.

use backend_utils_error::ereport;
use mcx::MemoryContext;
use types_catalog::catalog::NAMESPACE_RELATION_ID;
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{PgResult, ERRCODE_DUPLICATE_SCHEMA, ERROR};
use types_nodes::parsenodes::ObjectType;
use types_storage::lock::RowExclusiveLock;

use backend_access_table_table::table_open;
use backend_catalog_aclchk_seams::{get_user_default_acl, record_dependency_on_new_acl};
use backend_catalog_indexing_seams::catalog_tuple_insert_pg_namespace;
use backend_catalog_objectaccess_seams::{object_access_hook_present, run_object_post_create_hook};
use backend_catalog_pg_depend_seams::recordDependencyOnCurrentExtension;
use backend_catalog_pg_shdepend_seams::recordDependencyOnOwner;
use backend_utils_cache_syscache_seams::namespace_name_exists;

/// `NamespaceRelationId` — `pg_namespace`.
const NamespaceRelationId: Oid = NAMESPACE_RELATION_ID;

/// NamespaceCreate
///   Create a namespace (schema) with the given name and owner OID.
///
/// If `isTemp` is true, this schema is a per-backend schema for holding
/// temporary tables.  Currently, it is used to prevent it from being linked as
/// a member of any active extension.  (If someone does CREATE TEMP TABLE in an
/// extension script, we don't want the temp schema to become part of the
/// extension.)  And to avoid checking for default ACL for temp namespace (as it
/// is not necessary).
///
/// The C `if (!nspName) elog(ERROR, "no namespace name supplied")` null guard is
/// subsumed by the non-null `&str` type: a schema name cannot be null.
pub fn NamespaceCreate(nspName: &str, ownerId: Oid, isTemp: bool) -> PgResult<Oid> {
    let nspoid: Oid;

    /* sanity checks */
    /* `if (!nspName)` — enforced by the non-null `&str` type (see above). */

    /* make sure there is no existing namespace of same name */
    if namespace_name_exists::call(nspName)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_SCHEMA)
            .errmsg(format!("schema \"{nspName}\" already exists"))
            .into_error());
    }

    let nspacl = if !isTemp {
        get_user_default_acl::call(ObjectType::Schema, ownerId, InvalidOid)?
    } else {
        None
    };

    /* The C `CurrentMemoryContext` for `table_open` / the extension lookup. */
    let ctx = MemoryContext::new("NamespaceCreate");
    let mcx = ctx.mcx();

    let nspdesc = table_open(mcx, NamespaceRelationId, RowExclusiveLock)?;

    /*
     * Allocate the new OID, copy the name, and form + insert the pg_namespace
     * row.  The `values[]`/`nulls[]` / `namestrcpy` / `heap_form_tuple` /
     * `CatalogTupleInsert` value layer is owned by `catalog/indexing.c`; it
     * assigns the OID via `GetNewOidWithIndex(rel, NamespaceOidIndexId,
     * Anum_pg_namespace_oid)` and returns it.  `nspacl == None` ⇒
     * `nulls[Anum_pg_namespace_nspacl - 1] = true`.
     */
    nspoid = catalog_tuple_insert_pg_namespace::call(&nspdesc, nspName, ownerId, nspacl)?;
    debug_assert!(OidIsValid(nspoid));

    nspdesc.close(RowExclusiveLock)?;

    /* Record dependencies */
    let myself = ObjectAddress {
        classId: NamespaceRelationId,
        objectId: nspoid,
        objectSubId: 0,
    };

    /* dependency on owner */
    recordDependencyOnOwner::call(NamespaceRelationId, nspoid, ownerId)?;

    /* dependencies on roles mentioned in default ACL */
    record_dependency_on_new_acl::call(NamespaceRelationId, nspoid, 0, ownerId, nspacl)?;

    /* dependency on extension ... but not for magic temp schemas */
    if !isTemp {
        recordDependencyOnCurrentExtension::call(mcx, &myself, false)?;
    }

    /* Post creation hook for new schema */
    if object_access_hook_present::call() {
        run_object_post_create_hook::call(NamespaceRelationId, nspoid, 0, false)?;
    }

    Ok(nspoid)
}

/// Install this unit's inward seam ([`backend_catalog_pg_namespace_seams`]).
pub fn init_seams() {
    backend_catalog_pg_namespace_seams::namespace_create::set(NamespaceCreate);
}
