//! F0 keystone — `ObjectProperty[]` accessors and the small catalog-property
//! helpers from objectaddress.c (C 2629-2789).
//!
//! These are pure lookups over the static [`OBJECT_PROPERTY`] table; the bodies
//! are scaffolded as mirror-and-panic and filled in the F0 keystone fill stage.

use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::ObjectType;

use crate::tables::ObjectPropertyType;

/// `get_object_property_data(Oid class_id)` (objectaddress.c 2755): find the
/// [`ObjectPropertyType`] row whose `class_oid == class_id`; `elog(ERROR,
/// "unrecognized class: %u")` if none matches (carried on `Err`).
pub fn get_object_property_data(_class_id: Oid) -> PgResult<&'static ObjectPropertyType> {
    panic!("decomp: get_object_property_data not yet filled")
}

/// `get_object_class_descr(Oid class_id)` (objectaddress.c 2629).
pub fn get_object_class_descr(_class_id: Oid) -> PgResult<&'static str> {
    panic!("decomp: get_object_class_descr not yet filled")
}

/// `get_object_oid_index(Oid class_id)` (objectaddress.c 2637).
pub fn get_object_oid_index(_class_id: Oid) -> PgResult<Oid> {
    panic!("decomp: get_object_oid_index not yet filled")
}

/// `get_object_catcache_oid(Oid class_id)` (objectaddress.c 2645).
pub fn get_object_catcache_oid(_class_id: Oid) -> PgResult<i32> {
    panic!("decomp: get_object_catcache_oid not yet filled")
}

/// `get_object_catcache_name(Oid class_id)` (objectaddress.c 2653).
pub fn get_object_catcache_name(_class_id: Oid) -> PgResult<i32> {
    panic!("decomp: get_object_catcache_name not yet filled")
}

/// `get_object_attnum_oid(Oid class_id)` (objectaddress.c 2661).
pub fn get_object_attnum_oid(_class_id: Oid) -> PgResult<i16> {
    panic!("decomp: get_object_attnum_oid not yet filled")
}

/// `get_object_attnum_name(Oid class_id)` (objectaddress.c 2669).
pub fn get_object_attnum_name(_class_id: Oid) -> PgResult<i16> {
    panic!("decomp: get_object_attnum_name not yet filled")
}

/// `get_object_attnum_namespace(Oid class_id)` (objectaddress.c 2677).
pub fn get_object_attnum_namespace(_class_id: Oid) -> PgResult<i16> {
    panic!("decomp: get_object_attnum_namespace not yet filled")
}

/// `get_object_attnum_owner(Oid class_id)` (objectaddress.c 2685).
pub fn get_object_attnum_owner(_class_id: Oid) -> PgResult<i16> {
    panic!("decomp: get_object_attnum_owner not yet filled")
}

/// `get_object_attnum_acl(Oid class_id)` (objectaddress.c 2693).
pub fn get_object_attnum_acl(_class_id: Oid) -> PgResult<i16> {
    panic!("decomp: get_object_attnum_acl not yet filled")
}

/// `get_object_type(Oid class_id, Oid object_id)` (objectaddress.c 2708): the
/// `ObjectType` for a row, special-casing relations (relkind→objtype) and
/// constraints. `Err` carries the catalog `ereport(ERROR)`s.
pub fn get_object_type(_class_id: Oid, _object_id: Oid) -> PgResult<ObjectType> {
    panic!("decomp: get_object_type not yet filled")
}

/// `get_object_namensp_unique(Oid class_id)` (objectaddress.c 2726).
pub fn get_object_namensp_unique(_class_id: Oid) -> PgResult<bool> {
    panic!("decomp: get_object_namensp_unique not yet filled")
}

/// `is_objectclass_supported(Oid class_id)` (objectaddress.c): whether the
/// class appears in `ObjectProperty[]`. Total; cannot `ereport`.
pub fn is_objectclass_supported(_class_id: Oid) -> bool {
    panic!("decomp: is_objectclass_supported not yet filled")
}
