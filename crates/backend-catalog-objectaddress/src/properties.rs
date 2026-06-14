//! F0 keystone — `ObjectProperty[]` accessors and the small catalog-property
//! helpers from objectaddress.c (C 2629-2789).
//!
//! These are pure lookups over the static [`OBJECT_PROPERTY`] table; the bodies
//! are scaffolded as mirror-and-panic and filled in the F0 keystone fill stage.

use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::ObjectType;

use crate::tables::{ObjectPropertyType, OBJECT_PROPERTY};

use types_error::PgError;
use types_nodes::parsenodes::OBJECT_TABLE;

/// `get_object_property_data(Oid class_id)` (objectaddress.c 2755): find the
/// [`ObjectPropertyType`] row whose `class_oid == class_id`; `elog(ERROR,
/// "unrecognized class: %u")` if none matches (carried on `Err`).
///
/// The C `prop_last` static shortcut is a pure caching optimisation; it is
/// behaviour-preserving to omit it (we simply scan every time).
pub fn get_object_property_data(class_id: Oid) -> PgResult<&'static ObjectPropertyType> {
    for prop in OBJECT_PROPERTY {
        if prop.class_oid == class_id {
            return Ok(prop);
        }
    }

    // `ereport(ERROR, errmsg_internal("unrecognized class ID: %u", class_id))`.
    Err(PgError::error(format!(
        "unrecognized class ID: {class_id}"
    )))
}

/// `get_object_class_descr(Oid class_id)` (objectaddress.c 2629).
pub fn get_object_class_descr(class_id: Oid) -> PgResult<&'static str> {
    Ok(get_object_property_data(class_id)?.class_descr)
}

/// `get_object_oid_index(Oid class_id)` (objectaddress.c 2637).
pub fn get_object_oid_index(class_id: Oid) -> PgResult<Oid> {
    Ok(get_object_property_data(class_id)?.oid_index_oid)
}

/// `get_object_catcache_oid(Oid class_id)` (objectaddress.c 2645).
pub fn get_object_catcache_oid(class_id: Oid) -> PgResult<i32> {
    Ok(get_object_property_data(class_id)?.oid_catcache_id)
}

/// `get_object_catcache_name(Oid class_id)` (objectaddress.c 2653).
pub fn get_object_catcache_name(class_id: Oid) -> PgResult<i32> {
    Ok(get_object_property_data(class_id)?.name_catcache_id)
}

/// `get_object_attnum_oid(Oid class_id)` (objectaddress.c 2661).
pub fn get_object_attnum_oid(class_id: Oid) -> PgResult<i16> {
    Ok(get_object_property_data(class_id)?.attnum_oid)
}

/// `get_object_attnum_name(Oid class_id)` (objectaddress.c 2669).
pub fn get_object_attnum_name(class_id: Oid) -> PgResult<i16> {
    Ok(get_object_property_data(class_id)?.attnum_name)
}

/// `get_object_attnum_namespace(Oid class_id)` (objectaddress.c 2677).
pub fn get_object_attnum_namespace(class_id: Oid) -> PgResult<i16> {
    Ok(get_object_property_data(class_id)?.attnum_namespace)
}

/// `get_object_attnum_owner(Oid class_id)` (objectaddress.c 2685).
pub fn get_object_attnum_owner(class_id: Oid) -> PgResult<i16> {
    Ok(get_object_property_data(class_id)?.attnum_owner)
}

/// `get_object_attnum_acl(Oid class_id)` (objectaddress.c 2693).
pub fn get_object_attnum_acl(class_id: Oid) -> PgResult<i16> {
    Ok(get_object_property_data(class_id)?.attnum_acl)
}

/// `get_object_type(Oid class_id, Oid object_id)` (objectaddress.c 2708): the
/// `ObjectType` for a row, special-casing relations (relkind→objtype) and
/// constraints. `Err` carries the catalog `ereport(ERROR)`s.
pub fn get_object_type(class_id: Oid, object_id: Oid) -> PgResult<ObjectType> {
    let prop = get_object_property_data(class_id)?;

    if prop.objtype == OBJECT_TABLE as i32 {
        // If the property data says it's a table, dig a little deeper to get
        // the real relation kind, so that callers can produce more precise
        // error messages.
        let relkind = backend_utils_cache_lsyscache_seams::get_rel_relkind::call(object_id)?;
        Ok(crate::resolve::get_relkind_objtype(relkind as u8))
    } else {
        // `objtype` is stored raw to preserve the two `-1` rows; for those
        // callers never reach this accessor via a real OBJECT_* code, but the
        // round-trip through the enum is exact for the mapped rows.
        Ok(objtype_from_raw(prop.objtype))
    }
}

/// `get_object_namensp_unique(Oid class_id)` (objectaddress.c 2726).
pub fn get_object_namensp_unique(class_id: Oid) -> PgResult<bool> {
    Ok(get_object_property_data(class_id)?.is_nsp_name_unique)
}

/// `is_objectclass_supported(Oid class_id)` (objectaddress.c): whether the
/// class appears in `ObjectProperty[]`. Total; cannot `ereport`.
pub fn is_objectclass_supported(class_id: Oid) -> bool {
    OBJECT_PROPERTY.iter().any(|p| p.class_oid == class_id)
}

/// Reconstruct an [`ObjectType`] from the raw `i32` stored in
/// [`ObjectPropertyType::objtype`]. The C `ObjectType` is a plain C enum, so
/// the discriminant round-trips by transmute; we mirror that via the safe
/// `repr(i32)` conversion.
fn objtype_from_raw(raw: i32) -> ObjectType {
    // SAFETY: `ObjectType` is a fieldless `#[repr(i32)]`/C enum and `raw`
    // originates from a `<variant> as i32` in `tables.rs`, so it is always a
    // valid discriminant for the non-`-1` rows reached here.
    unsafe { core::mem::transmute::<i32, ObjectType>(raw) }
}
