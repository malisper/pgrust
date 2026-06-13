//! `aclitem` text I/O and hashing (`utils/adt/acl.c`).
//!
//! `getid`/`putid`/`is_safe_acl_char`/`aclparse` parse and emit the
//! `grantee=privs/grantor` external form; `aclitemin`/`aclitemout` are the
//! SQL type's `_in`/`_out`; `hash_aclitem*` and `aclitem_eq`/`_match`/
//! `Comparator` support hashing and sorting.

use types_acl::AclItem;
use types_error::PgResult;

/// `is_safe_acl_char` (acl.c) — is `c` allowed unquoted in an ACL identifier?
/// `is_getid` distinguishes the parsing context.
pub fn is_safe_acl_char(_c: u8, _is_getid: bool) -> bool {
    todo!("scaffold: backend-utils-adt-acl::aclitem_io::is_safe_acl_char")
}

/// `getid` (acl.c) — read one identifier from `s` into `n`, returning the
/// position past it. `escontext` carries soft-error reporting.
pub fn getid() -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::aclitem_io::getid")
}

/// `putid` (acl.c) — append identifier `s` to `p`, quoting as needed.
pub fn putid() {
    todo!("scaffold: backend-utils-adt-acl::aclitem_io::putid")
}

/// `aclparse` (acl.c) — parse one external aclitem from `s` into `aip`.
pub fn aclparse(_aip: &mut AclItem) -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::aclitem_io::aclparse")
}

/// `aclitemin` (acl.c) — `aclitem` type input function (`PG_FUNCTION_ARGS`).
pub fn aclitemin() -> PgResult<AclItem> {
    todo!("scaffold: backend-utils-adt-acl::aclitem_io::aclitemin")
}

/// `aclitemout` (acl.c) — `aclitem` type output function (`PG_FUNCTION_ARGS`).
pub fn aclitemout(_item: &AclItem) -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::aclitem_io::aclitemout")
}

/// `aclitem_match` (acl.c) — do two items share grantee and grantor?
pub fn aclitem_match(_a1: &AclItem, _a2: &AclItem) -> bool {
    todo!("scaffold: backend-utils-adt-acl::aclitem_io::aclitem_match")
}

/// `aclitemComparator` (acl.c) — qsort comparator over `AclItem`.
pub fn aclitem_comparator(_a1: &AclItem, _a2: &AclItem) -> core::cmp::Ordering {
    todo!("scaffold: backend-utils-adt-acl::aclitem_io::aclitem_comparator")
}

/// `aclitem_eq` (acl.c) — SQL equality of two aclitems (`PG_FUNCTION_ARGS`).
pub fn aclitem_eq(_a1: &AclItem, _a2: &AclItem) -> bool {
    todo!("scaffold: backend-utils-adt-acl::aclitem_io::aclitem_eq")
}

/// `hash_aclitem` (acl.c) — 32-bit hash of an aclitem (`PG_FUNCTION_ARGS`).
pub fn hash_aclitem(_item: &AclItem) -> u32 {
    todo!("scaffold: backend-utils-adt-acl::aclitem_io::hash_aclitem")
}

/// `hash_aclitem_extended` (acl.c) — 64-bit seeded hash (`PG_FUNCTION_ARGS`).
pub fn hash_aclitem_extended(_item: &AclItem, _seed: u64) -> u64 {
    todo!("scaffold: backend-utils-adt-acl::aclitem_io::hash_aclitem_extended")
}
