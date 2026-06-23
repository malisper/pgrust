//! Unit tests for the pure (no-catalog-access) classification predicates.
//!
//! These exercise the hard-wired OID lists and the `FirstUnpinnedObjectId`
//! cutoff. The `Relation`-taking entry points and seam-routed externals are not
//! exercised here (they require an installed runtime); the value-level logic
//! they delegate to is covered through its plain-value forms
//! (`IsSystemClass`/`IsToastNamespace`/`IsCatalogRelationOid`).

use super::*;
use ::types_catalog::catalog::TYPE_RELATION_ID;

/// Build a `PgClassForm` with just the namespace field set — the only field
/// `IsSystemClass` reads on the toast branch.
fn class_in_namespace(relnamespace: Oid) -> PgClassForm {
    PgClassForm {
        relnamespace,
        ..PgClassForm::default()
    }
}

#[test]
fn catalog_relation_oid_uses_pinned_cutoff() {
    assert!(IsCatalogRelationOid(RELATION_RELATION_ID));
    assert!(IsCatalogRelationOid(FIRST_UNPINNED_OBJECT_ID - 1));
    assert!(!IsCatalogRelationOid(FIRST_UNPINNED_OBJECT_ID));
    assert!(!IsCatalogRelationOid(FIRST_UNPINNED_OBJECT_ID + 1));
}

#[test]
fn catalog_text_unique_index_list() {
    assert!(IsCatalogTextUniqueIndexOid(PARAMETER_ACL_PARNAME_INDEX_ID));
    assert!(IsCatalogTextUniqueIndexOid(REPLICATION_ORIGIN_NAME_INDEX));
    assert!(IsCatalogTextUniqueIndexOid(SEC_LABEL_OBJECT_INDEX_ID));
    assert!(IsCatalogTextUniqueIndexOid(SHARED_SEC_LABEL_OBJECT_INDEX_ID));
    assert!(!IsCatalogTextUniqueIndexOid(CLASS_OID_INDEX_ID));
}

#[test]
fn inplace_update_oid_is_class_or_database() {
    assert!(IsInplaceUpdateOid(RELATION_RELATION_ID));
    assert!(IsInplaceUpdateOid(DATABASE_RELATION_ID));
    assert!(!IsInplaceUpdateOid(TYPE_RELATION_ID));
}

#[test]
fn catalog_namespace_is_pg_catalog() {
    assert!(IsCatalogNamespace(PG_CATALOG_NAMESPACE));
    assert!(!IsCatalogNamespace(PG_TOAST_NAMESPACE));
    assert!(!IsCatalogNamespace(PG_PUBLIC_NAMESPACE));
}

#[test]
fn reserved_name_prefix() {
    assert!(IsReservedName("pg_class"));
    assert!(IsReservedName("pg_"));
    assert!(!IsReservedName("pg"));
    assert!(!IsReservedName("public"));
    assert!(!IsReservedName(""));
    assert!(!IsReservedName("xg_foo"));
}

#[test]
fn shared_relation_list() {
    // A shared catalog, one of its indexes, and one of its toast tables.
    assert!(IsSharedRelation(TABLE_SPACE_RELATION_ID));
    assert!(IsSharedRelation(TABLESPACE_OID_INDEX_ID));
    assert!(IsSharedRelation(PG_TABLESPACE_TOAST_TABLE));
    // A non-shared (database-local) catalog.
    assert!(!IsSharedRelation(RELATION_RELATION_ID));
    assert!(!IsSharedRelation(CLASS_OID_INDEX_ID));
}

#[test]
fn pinned_object_rules() {
    assert!(!IsPinnedObject(NAMESPACE_RELATION_ID, FIRST_UNPINNED_OBJECT_ID));
    assert!(!IsPinnedObject(LARGE_OBJECT_RELATION_ID, 1));
    assert!(!IsPinnedObject(NAMESPACE_RELATION_ID, PG_PUBLIC_NAMESPACE));
    assert!(IsPinnedObject(NAMESPACE_RELATION_ID, PG_CATALOG_NAMESPACE));
    assert!(!IsPinnedObject(DATABASE_RELATION_ID, 1));
    // Any other initdb-created object (a pinned OID under a normal class): pinned.
    assert!(IsPinnedObject(TYPE_RELATION_ID, TYPE_RELATION_ID));
}

#[test]
fn toast_namespace_fast_path() {
    // pg_toast matches before the isTempToastNamespace delegation.
    assert!(IsToastNamespace(PG_TOAST_NAMESPACE));
    // No temp-toast namespace is set in a unit-test backend, so a non-toast
    // namespace is not a toast namespace.
    assert!(!IsToastNamespace(PG_CATALOG_NAMESPACE));
}

#[test]
fn system_class_uses_catalog_cutoff_first() {
    // A pinned OID is a system class regardless of namespace (short-circuits
    // before the toast branch).
    let user_ns_class = class_in_namespace(PG_PUBLIC_NAMESPACE);
    assert!(IsSystemClass(RELATION_RELATION_ID, &user_ns_class));
    // A pg_toast class is a system class via the toast branch.
    let toast_class = class_in_namespace(PG_TOAST_NAMESPACE);
    assert!(IsSystemClass(FIRST_UNPINNED_OBJECT_ID + 5, &toast_class));
}
