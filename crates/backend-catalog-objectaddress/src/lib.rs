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
//!   + the `text[]`↔`List` bridges. `pg_get_acl` reads the object's
//!   `aclitem[]` column through the indexing owner's `get_acl_datum` seam
//!   (the catalog-tuple read + value lane); the remaining bridges are still
//!   gated on the Datum/ArrayType value lane.
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
pub mod fmgr_builtins;
pub mod fmgr_sql;
pub mod identity;
pub mod auth_member_lookup;
pub mod rewrite_lookup;
pub mod trigger_lookup;
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
    seams::get_object_catcache_oid::set(properties::get_object_catcache_oid);
    seams::get_object_attnum_oid::set(properties::get_object_attnum_oid);
    seams::get_object_oid_index::set(properties::get_object_oid_index);
    seams::get_object_class_descr::set(properties::get_object_class_descr);

    // pg_rewrite by-oid projections (no RULEOID syscache exists): the
    // `getObjectDescription` / `getObjectIdentityParts` / `RemoveRewriteRuleById`
    // OCLASS_REWRITE legs fetch `(ev_class, rulename)` by rule oid through these.
    backend_utils_cache_syscache_seams::rewrite_class_name::set(
        rewrite_lookup::rewrite_class_name,
    );
    backend_utils_cache_syscache_seams::rewrite_name_evclass::set(
        rewrite_lookup::rewrite_name_evclass,
    );

    // pg_trigger by-oid projection (no TRIGGEROID syscache exists): the
    // `getObjectDescription` OCLASS_TRIGGER leg fetches `(tgrelid, tgname)` by
    // trigger oid through this.
    backend_utils_cache_syscache_seams::trigger_relid_name::set(
        trigger_lookup::trigger_relid_name,
    );
    backend_utils_cache_syscache_seams::trigger_name_relid::set(
        trigger_lookup::trigger_name_relid,
    );

    // pg_auth_members by-oid projection (no by-oid syscache exists): the
    // `getObjectDescription` / `getObjectIdentityParts` role-membership legs
    // (AuthMemRelationId) fetch `(member, roleid)` by pg_auth_members oid
    // through this — used to build the DROP ROLE dependency DETAIL.
    backend_utils_cache_syscache_seams::auth_member_member_role::set(
        auth_member_lookup::auth_member_member_role,
    );

    // Register this crate's SQL-callable fmgr builtins (C: their
    // `fmgr_builtins[]` rows) into the fmgr-core builtin table.
    fmgr_builtins::register_objectaddress_builtins();
}
