//! `Acl` array construction and the privilege-mask algebra (`utils/adt/acl.c`).
//!
//! Covers the C-API `Acl` operations (`allocacl`, `make_empty_acl`, `aclcopy`,
//! `aclconcat`, `aclmerge`, `aclitemsort`, `aclequal`, `check_acl`,
//! `aclupdate`, `aclnewowner`, `check_circularity`, `recursive_revoke`,
//! `aclmask`, `aclmask_direct`, `aclmembers`), the SQL operators
//! (`aclinsert`, `aclremove`, `aclcontains`, `makeaclitem`, `aclexplode`),
//! and the priv-string conversion helpers shared with `has_privilege`
//! (`convert_aclright_to_string`, `convert_any_priv_string`).

use mcx::Mcx;
use types_acl::{AclItem, AclMaskHow, AclMode};
use types_core::Oid;
use types_error::PgResult;

/// `allocacl` (acl.c) — allocate a zeroed `Acl` array of `n` items in `mcx`.
pub fn allocacl<'mcx>(_mcx: Mcx<'mcx>, _n: i32) -> PgResult<&'mcx mut [AclItem]> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::allocacl")
}

/// `make_empty_acl` (acl.c) — allocate an empty `Acl` array in `mcx`.
pub fn make_empty_acl<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<&'mcx mut [AclItem]> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::make_empty_acl")
}

/// `aclcopy` (acl.c) — duplicate an `Acl` array into `mcx`.
pub fn aclcopy<'mcx>(_mcx: Mcx<'mcx>, _orig: &[AclItem]) -> PgResult<&'mcx mut [AclItem]> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclcopy")
}

/// `aclconcat` (acl.c) — concatenate two `Acl` arrays into `mcx`.
pub fn aclconcat<'mcx>(
    _mcx: Mcx<'mcx>,
    _left: &[AclItem],
    _right: &[AclItem],
) -> PgResult<&'mcx mut [AclItem]> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclconcat")
}

/// `aclmerge` (acl.c) — merge two `Acl` arrays, OR-ing rights per grantee.
pub fn aclmerge<'mcx>(
    _mcx: Mcx<'mcx>,
    _left: &[AclItem],
    _right: &[AclItem],
    _owner_id: Oid,
) -> PgResult<&'mcx mut [AclItem]> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclmerge")
}

/// `aclitemsort` (acl.c) — sort an `Acl` array in place into canonical order.
pub fn aclitemsort(_acl: &mut [AclItem]) {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclitemsort")
}

/// `aclequal` (acl.c) — are two `Acl` arrays equal as sets?
pub fn aclequal(_left: &[AclItem], _right: &[AclItem]) -> bool {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclequal")
}

/// `check_acl` (acl.c) — validate an `Acl` array's varlena shape; errors on bad.
pub fn check_acl(_acl: &[AclItem]) -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::check_acl")
}

/// `aclupdate` (acl.c) — apply `mod_aip` (with `modechg`) to `old_acl`,
/// producing a new array in `mcx`. `modechg` is acl.c's `int` mode-change code.
pub fn aclupdate<'mcx>(
    _mcx: Mcx<'mcx>,
    _old_acl: &[AclItem],
    _mod_aip: &AclItem,
    _modechg: i32,
    _owner_id: Oid,
    _behavior: i32,
) -> PgResult<&'mcx mut [AclItem]> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclupdate")
}

/// `aclnewowner` (acl.c) — rewrite an `Acl` array for an ownership change.
pub fn aclnewowner<'mcx>(
    _mcx: Mcx<'mcx>,
    _old_acl: &[AclItem],
    _old_owner_id: Oid,
    _new_owner_id: Oid,
) -> PgResult<&'mcx mut [AclItem]> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclnewowner")
}

/// `check_circularity` (acl.c) — guard against grant cycles before an update.
pub fn check_circularity(
    _old_acl: &[AclItem],
    _mod_aip: &AclItem,
    _owner_id: Oid,
) -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::check_circularity")
}

/// `recursive_revoke` (acl.c) — cascade-revoke privileges no longer grantable.
/// `behavior` is the C `DropBehavior`.
pub fn recursive_revoke<'mcx>(
    _mcx: Mcx<'mcx>,
    _acl: &[AclItem],
    _grantee: Oid,
    _revoke_privs: AclMode,
    _owner_id: Oid,
    _behavior: i32,
) -> PgResult<&'mcx mut [AclItem]> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::recursive_revoke")
}

/// `aclmask` (acl.c) — privilege bits in `acl` available to `roleid`.
pub fn aclmask(
    _acl: &[AclItem],
    _roleid: Oid,
    _owner_id: Oid,
    _mask: AclMode,
    _how: AclMaskHow,
) -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclmask")
}

/// `aclmask_direct` (acl.c) — like `aclmask` but without role-membership
/// expansion (direct grants to `roleid` only).
pub fn aclmask_direct(
    _acl: &[AclItem],
    _roleid: Oid,
    _owner_id: Oid,
    _mask: AclMode,
    _how: AclMaskHow,
) -> AclMode {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclmask_direct")
}

/// `aclmembers` (acl.c) — distinct role OIDs mentioned in `acl`, into `mcx`.
pub fn aclmembers<'mcx>(_mcx: Mcx<'mcx>, _acl: &[AclItem]) -> PgResult<&'mcx mut [Oid]> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclmembers")
}

/// `aclinsert` (acl.c) — deprecated SQL stub (`PG_FUNCTION_ARGS`).
pub fn aclinsert() -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclinsert")
}

/// `aclremove` (acl.c) — deprecated SQL stub (`PG_FUNCTION_ARGS`).
pub fn aclremove() -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclremove")
}

/// `aclcontains` (acl.c) — SQL: is an aclitem present in an acl array?
pub fn aclcontains() -> PgResult<bool> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclcontains")
}

/// `makeaclitem` (acl.c) — SQL: build an aclitem from grantee/grantor/privs.
pub fn makeaclitem() -> PgResult<AclItem> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::makeaclitem")
}

/// `aclexplode` (acl.c) — SQL SRF: expand an acl array into rows.
pub fn aclexplode() -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::aclexplode")
}

/// `convert_aclright_to_string` (acl.c) — privilege bit to its keyword text.
pub fn convert_aclright_to_string(_aclright: i32) -> &'static str {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::convert_aclright_to_string")
}

/// `convert_any_priv_string` (acl.c) — parse a comma-separated privilege list
/// against a `priv_map` table into an `AclMode`.
pub fn convert_any_priv_string(_privileges: &[(&str, AclMode)]) -> PgResult<AclMode> {
    todo!("scaffold: backend-utils-adt-acl::acl_ops::convert_any_priv_string")
}
