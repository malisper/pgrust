//! Built-in default ACLs (`utils/adt/acl.c`).
//!
//! `acldefault` builds the hardwired default ACL for a given object type and
//! owner; `acldefault_sql` is its SQL-callable wrapper.

use mcx::Mcx;
use types_acl::AclItem;
use types_core::Oid;
use types_error::PgResult;

/// `acldefault` (acl.c) — the default ACL for `objtype` owned by `owner_id`.
///
/// `objtype` is PostgreSQL's `ObjectType`; modeled here as `i32` for the
/// scaffold (it becomes the real `ObjectType` enum when this family lands).
/// Allocates the result `Acl` array in `mcx`.
pub fn acldefault<'mcx>(
    _mcx: Mcx<'mcx>,
    _objtype: i32,
    _owner_id: Oid,
) -> PgResult<&'mcx mut [AclItem]> {
    todo!("scaffold: backend-utils-adt-acl::acldefault::acldefault")
}

/// `acldefault_sql` (acl.c) — SQL wrapper over `acldefault` (`PG_FUNCTION_ARGS`).
pub fn acldefault_sql() -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::acldefault::acldefault_sql")
}
