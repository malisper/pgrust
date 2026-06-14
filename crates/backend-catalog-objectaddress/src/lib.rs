//! `catalog/objectaddress.c` — object-address resolution, description,
//! identity, and the SQL-callable object-introspection functions.
//!
//! NEEDS_DECOMP scaffold. The crate is split into five families:
//!
//! - **F0 keystone** ([`consts`], [`tables`], [`properties`], [`resolve`]):
//!   the resolution model (`get_object_address[_rv]` + the 13
//!   `get_object_address_*` helpers), the `ObjectProperty[]` (37 rows) and
//!   `ObjectTypeMap[]` (59 rows) tables + property accessors,
//!   `get_catalog_object_by_oid[_extended]`, ownership/namespace checks, and
//!   the string↔objtype / relkind maps. F0 installs all 5 PINNED inward seams.
//! - **F1** ([`description`]): `getObjectDescription` (~41 arms).
//! - **F2** ([`type_description`]): `getObjectTypeDescription` + the
//!   relation/constraint/procedure type-disambiguation helpers.
//! - **F3** ([`identity`]): `getObjectIdentity[Parts]` + the per-class
//!   identity helpers.
//! - **F4** ([`fmgr_sql`]): the SQL-callable leg (`pg_get_object_address` /
//!   `pg_describe_object` / `pg_identify_object[_as_address]` / `pg_get_acl`)
//!   + the `text[]`↔`List` bridges, gated on the Datum/ArrayType value lane.
//!
//! Every function body is scaffolded as `panic!("decomp: <fn> not yet
//! filled")`; the fill stages replace them with the faithful C logic.

#![allow(clippy::too_many_arguments)]

// F0 keystone
pub mod consts;
pub mod properties;
pub mod resolve;
pub mod tables;

// F1 / F2 / F3 / F4 families
pub mod description;
pub mod fmgr_sql;
pub mod identity;
pub mod type_description;

/// Install this unit's inward seams. Wired into `seams-init::init_all`.
///
/// objectaddress owns 5 PINNED inward seams (declared in
/// `backend-catalog-objectaddress-seams`):
/// `get_object_address`, `get_object_namespace`, `check_object_ownership`,
/// `get_object_description`, `get_relkind_objtype`. Until F1 lands,
/// `get_object_description` routes to F1's [`description::get_object_description`]
/// (which mirror-and-panics within the crate).
pub fn init_seams() {
    use backend_catalog_objectaddress_seams as seams;

    seams::get_object_address::set(resolve::get_object_address);
    seams::get_object_namespace::set(resolve::get_object_namespace);
    seams::check_object_ownership::set(resolve::check_object_ownership);
    seams::get_object_description::set(description::get_object_description);
    seams::get_relkind_objtype::set(resolve::get_relkind_objtype);
}
