//! ABI vocabulary for `backend/commands/policy.c` — row-level-security (RLS)
//! policy DDL (CREATE / ALTER / DROP / RENAME POLICY).
//!
//! These `#[repr(C)]` definitions mirror the C declarations in
//!   * `src/include/nodes/parsenodes.h`   (`CreatePolicyStmt`, `AlterPolicyStmt`)
//!   * `src/include/catalog/pg_policy.h` + `pg_policy_d.h`
//!     (`FormData_pg_policy`, the relation/index OIDs, `Anum_pg_policy_*`,
//!     `Natts_pg_policy`)
//!   * `src/include/nodes/nodetags.h`     (`T_CreatePolicyStmt`,
//!     `T_AlterPolicyStmt` — values 178/179 for PostgreSQL 18.3)
//!
//! This module is referenced by path (`pg_ffi_fgram::policy::*`), NOT glob-re-
//! exported at the crate root, mirroring the `tcop` / `dbcommands_abi`
//! convention: it carries widely-named items (`PolicyRelationId`, the
//! `Anum_*`/`Natts_*` macros, the `T_*` tags) that overlap other modules, so it
//! is named explicitly to avoid the ambiguous-glob trap.
//!
//! Types defined elsewhere that policy.c also needs are re-used here, never
//! re-defined: `RangeVar`, `RoleSpec`, `RenameStmt`, `Node`, `List`,
//! `ObjectAddress`, the `ACL_*_CHR` privilege chars, `ACL_ID_PUBLIC`, and the
//! `RELKIND_*` / dependency / relation-OID constants.

use core::ffi::c_char;

use crate::{List, NameData, Node, NodeTag, Oid, RangeVar};

// ---------------------------------------------------------------------------
// Parse-node tags (nodes/nodetags.h, PostgreSQL 18.3).
// ---------------------------------------------------------------------------

/// `T_CreatePolicyStmt` (nodetags.h:195).
pub const T_CreatePolicyStmt: NodeTag = 178;
/// `T_AlterPolicyStmt` (nodetags.h:196).
pub const T_AlterPolicyStmt: NodeTag = 179;

// ---------------------------------------------------------------------------
// Parse nodes (nodes/parsenodes.h).
// ---------------------------------------------------------------------------

/// `typedef struct CreatePolicyStmt` (parsenodes.h:3058) — the parsetree for
/// the CREATE POLICY command.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreatePolicyStmt {
    pub type_: NodeTag,
    /// Policy's name.
    pub policy_name: *mut c_char,
    /// The table name the policy applies to.
    pub table: *mut RangeVar,
    /// The command name the policy applies to (`all`/`select`/`insert`/
    /// `update`/`delete`).
    pub cmd_name: *mut c_char,
    /// Restrictive or permissive policy.
    pub permissive: bool,
    /// The roles associated with the policy.
    pub roles: *mut List,
    /// The policy's USING condition.
    pub qual: *mut Node,
    /// The policy's WITH CHECK condition.
    pub with_check: *mut Node,
}

/// `typedef struct AlterPolicyStmt` (parsenodes.h:3074) — the parsetree for the
/// ALTER POLICY command.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterPolicyStmt {
    pub type_: NodeTag,
    /// Policy's name.
    pub policy_name: *mut c_char,
    /// The table name the policy applies to.
    pub table: *mut RangeVar,
    /// The roles associated with the policy.
    pub roles: *mut List,
    /// The policy's USING condition.
    pub qual: *mut Node,
    /// The policy's WITH CHECK condition.
    pub with_check: *mut Node,
}

// ---------------------------------------------------------------------------
// pg_policy catalog (catalog/pg_policy.h, catalog/pg_policy_d.h).
// ---------------------------------------------------------------------------

/// `PolicyRelationId` — `pg_policy` (pg_policy_d.h:23).  Re-uses the canonical
/// value in `crate::catalog` so it can never drift.
pub const PolicyRelationId: Oid = crate::catalog::POLICY_RELATION_ID;
/// `PolicyOidIndexId` — `pg_policy_oid_index` (pg_policy_d.h:24).
pub const PolicyOidIndexId: Oid = 3257;
/// `PolicyPolrelidPolnameIndexId` — `pg_policy_polrelid_polname_index`
/// (pg_policy_d.h:25).
pub const PolicyPolrelidPolnameIndexId: Oid = 3258;

/// `pg_policy` TOAST relation OID (pg_policy.h:53 `DECLARE_TOAST`).
pub const PG_POLICY_TOAST_TABLE: Oid = 4167;
/// `pg_policy` TOAST index OID (pg_policy.h:53 `DECLARE_TOAST`).
pub const PG_POLICY_TOAST_INDEX: Oid = 4168;

// Attribute numbers (1-based) — pg_policy_d.h:27-34.
pub const Anum_pg_policy_oid: i16 = 1;
pub const Anum_pg_policy_polname: i16 = 2;
pub const Anum_pg_policy_polrelid: i16 = 3;
pub const Anum_pg_policy_polcmd: i16 = 4;
pub const Anum_pg_policy_polpermissive: i16 = 5;
pub const Anum_pg_policy_polroles: i16 = 6;
pub const Anum_pg_policy_polqual: i16 = 7;
pub const Anum_pg_policy_polwithcheck: i16 = 8;

/// `Natts_pg_policy` — number of (fixed) columns (pg_policy_d.h:36).
pub const Natts_pg_policy: usize = 8;

/// `FormData_pg_policy` (pg_policy.h:29-44) — the fixed-width prefix of a
/// `pg_policy` tuple (`GETSTRUCT`).  The variable-length attributes
/// (`polroles`, `polqual`, `polwithcheck`) live past this struct and are read
/// with `heap_getattr`, exactly as in C.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct FormData_pg_policy {
    /// Policy OID.
    pub oid: Oid,
    /// Policy name.
    pub polname: NameData,
    /// OID of the relation the policy is on (`BKI_LOOKUP(pg_class)`).
    pub polrelid: Oid,
    /// One of the `ACL_*_CHR` chars, or `'*'` for ALL.
    pub polcmd: c_char,
    /// Restrictive (false) or permissive (true).
    pub polpermissive: bool,
}

/// `Form_pg_policy` — a pointer to a `pg_policy` row in the `GETSTRUCT` format.
pub type Form_pg_policy = *mut FormData_pg_policy;
