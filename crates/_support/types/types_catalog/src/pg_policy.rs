//! `pg_policy` catalog row layout, attribute numbers, and the
//! INSERT / UPDATE carriers (`catalog/pg_policy.h`, PostgreSQL 18.3).
//!
//! `pg_policy` records row-level-security (RLS) policies. `CreatePolicy`
//! (`commands/policy.c`) forms the row — `oid`, `polname`, `polrelid`,
//! `polcmd`, `polpermissive`, the `polroles` `oid[]` array, and the nullable
//! `polqual` / `polwithcheck` `pg_node_tree` columns — and `CatalogTupleInsert`s
//! it.  The catalog-indexing owner forms the heap tuple from
//! [`PgPolicyInsertRow`]; `AlterPolicy` / `RemoveRoleFromObjectPolicy` re-form
//! selected columns via [`PgPolicyUpdateRow`].

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use ::types_core::primitive::Oid;

/* ==========================================================================
 * Catalog relation + index OIDs (pg_policy.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `PolicyRelationId` — `pg_policy` (OID 3256).
pub const PolicyRelationId: Oid = 3256;
/// `PolicyOidIndexId` — `pg_policy_oid_index` (OID 3257).
pub const PolicyOidIndexId: Oid = 3257;
/// `PolicyPolrelidPolnameIndexId` — `pg_policy_polrelid_polname_index`
/// (OID 3258), btree(polrelid oid_ops, polname name_ops).
pub const PolicyPolrelidPolnameIndexId: Oid = 3258;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_policy).
 * ======================================================================== */

pub const Anum_pg_policy_oid: i16 = 1;
pub const Anum_pg_policy_polname: i16 = 2;
pub const Anum_pg_policy_polrelid: i16 = 3;
pub const Anum_pg_policy_polcmd: i16 = 4;
pub const Anum_pg_policy_polpermissive: i16 = 5;
pub const Anum_pg_policy_polroles: i16 = 6;
pub const Anum_pg_policy_polqual: i16 = 7;
pub const Anum_pg_policy_polwithcheck: i16 = 8;

/// `Natts_pg_policy` — number of columns (pg_policy.h).
pub const Natts_pg_policy: usize = 8;

/* ==========================================================================
 * Row carriers.
 * ======================================================================== */

/// The fixed-width scalar columns of a scanned `pg_policy` row
/// (`(Form_pg_policy) GETSTRUCT(tup)`). `polname` is the `NameData` rendered as
/// a `String`. The variable-length `polroles` / `polqual` / `polwithcheck`
/// columns are not part of this fixed projection.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FormData_pg_policy {
    pub oid: Oid,
    pub polname: String,
    pub polrelid: Oid,
    /// One of `ACL_*_CHR`, or `'*'` for all. Stored as the on-disk signed byte.
    pub polcmd: i8,
    pub polpermissive: bool,
}

/// The values `CreatePolicy` (`commands/policy.c`) builds for `heap_form_tuple`
/// + `CatalogTupleInsert`. The `oid` column is freshly allocated by the owner
/// via `GetNewOidWithIndex`, so it is NOT carried here. `polroles` is the role
/// OID list to encode as an `oid[]` array (`construct_array_builtin(..., OIDOID)`,
/// `BKI_FORCE_NOT_NULL`). `polqual` / `polwithcheck` are the `nodeToString`
/// images of the quals (`None` => stored NULL).
#[derive(Clone, Debug)]
pub struct PgPolicyInsertRow {
    pub polname: String,
    pub polrelid: Oid,
    pub polcmd: i8,
    pub polpermissive: bool,
    pub polroles: Vec<Oid>,
    pub polqual: Option<String>,
    pub polwithcheck: Option<String>,
}

/// The selectively-replaced columns for an `AlterPolicy` /
/// `RemoveRoleFromObjectPolicy` `heap_modify_tuple` + `CatalogTupleUpdate`. A
/// `None` field means `replaces[...] = false` (keep the existing value); a
/// `Some` means `replaces[...] = true` with the carried value. For `polqual` /
/// `polwithcheck` the outer `Option` is the replace flag and the inner `Option`
/// the NULL-or-text value.
#[derive(Clone, Debug, Default)]
pub struct PgPolicyUpdateRow {
    pub polroles: Option<Vec<Oid>>,
    pub polqual: Option<Option<String>>,
    pub polwithcheck: Option<Option<String>>,
}
