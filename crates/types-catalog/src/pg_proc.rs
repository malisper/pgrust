//! `pg_proc` catalog vocabulary (`catalog/pg_proc.h`, PostgreSQL 18.3) used by
//! the `backend-catalog-pg-proc` owner (`ProcedureCreate`) and its
//! `commands/functioncmds.c` consumer.
//!
//! This module supplies the catalog relation / index OIDs, the attribute
//! numbers (`Anum_pg_proc_*`), `Natts_pg_proc`, the `prokind` / `provolatile`
//! / `proparallel` / `proargmode` single-`char` codes pg_proc.c writes, and the
//! value-layer row carrier that crosses the catalog-tuple seams
//! ([`PgProcInsertRow`] / [`ProcFormFields`]).

extern crate alloc;

use alloc::string::String;

use types_array::ArrayType;
use types_core::primitive::Oid;

/* ==========================================================================
 * Catalog relation + index OIDs (pg_proc.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `ProcedureRelationId` — `pg_proc` (OID 1255).
pub const ProcedureRelationId: Oid = 1255;
/// `ProcedureOidIndexId` — `pg_proc_oid_index` (OID 2690).
pub const ProcedureOidIndexId: Oid = 2690;
/// `ProcedureNameArgsNspIndexId` — `pg_proc_proname_args_nsp_index` (OID 2691).
pub const ProcedureNameArgsNspIndexId: Oid = 2691;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_proc; 1-based).
 * Verified field-for-field against catalog/pg_proc.h (PostgreSQL 18.3).
 * ======================================================================== */

pub const Anum_pg_proc_oid: i16 = 1;
pub const Anum_pg_proc_proname: i16 = 2;
pub const Anum_pg_proc_pronamespace: i16 = 3;
pub const Anum_pg_proc_proowner: i16 = 4;
pub const Anum_pg_proc_prolang: i16 = 5;
pub const Anum_pg_proc_procost: i16 = 6;
pub const Anum_pg_proc_prorows: i16 = 7;
pub const Anum_pg_proc_provariadic: i16 = 8;
pub const Anum_pg_proc_prosupport: i16 = 9;
pub const Anum_pg_proc_prokind: i16 = 10;
pub const Anum_pg_proc_prosecdef: i16 = 11;
pub const Anum_pg_proc_proleakproof: i16 = 12;
pub const Anum_pg_proc_proisstrict: i16 = 13;
pub const Anum_pg_proc_proretset: i16 = 14;
pub const Anum_pg_proc_provolatile: i16 = 15;
pub const Anum_pg_proc_proparallel: i16 = 16;
pub const Anum_pg_proc_pronargs: i16 = 17;
pub const Anum_pg_proc_pronargdefaults: i16 = 18;
pub const Anum_pg_proc_prorettype: i16 = 19;
pub const Anum_pg_proc_proargtypes: i16 = 20;
pub const Anum_pg_proc_proallargtypes: i16 = 21;
pub const Anum_pg_proc_proargmodes: i16 = 22;
pub const Anum_pg_proc_proargnames: i16 = 23;
pub const Anum_pg_proc_proargdefaults: i16 = 24;
pub const Anum_pg_proc_protrftypes: i16 = 25;
pub const Anum_pg_proc_prosrc: i16 = 26;
pub const Anum_pg_proc_probin: i16 = 27;
pub const Anum_pg_proc_prosqlbody: i16 = 28;
pub const Anum_pg_proc_proconfig: i16 = 29;
pub const Anum_pg_proc_proacl: i16 = 30;

/// `Natts_pg_proc` — number of columns of `pg_proc`.
pub const Natts_pg_proc: usize = 30;

/* ==========================================================================
 * prokind / provolatile / proparallel codes (pg_proc.h PROKIND_ / PROVOLATILE_
 * / PROPARALLEL_ macros).
 * ======================================================================== */

/// `PROKIND_FUNCTION` — a plain function.
pub const PROKIND_FUNCTION: i8 = b'f' as i8;
/// `PROKIND_AGGREGATE` — an aggregate function.
pub const PROKIND_AGGREGATE: i8 = b'a' as i8;
/// `PROKIND_WINDOW` — a window function.
pub const PROKIND_WINDOW: i8 = b'w' as i8;
/// `PROKIND_PROCEDURE` — a procedure.
pub const PROKIND_PROCEDURE: i8 = b'p' as i8;

/* ==========================================================================
 * proargmode codes (pg_proc.h PROARGMODE_ macros).
 * ======================================================================== */

/// `PROARGMODE_IN` — input parameter.
pub const PROARGMODE_IN: i8 = b'i' as i8;
/// `PROARGMODE_OUT` — output parameter.
pub const PROARGMODE_OUT: i8 = b'o' as i8;
/// `PROARGMODE_INOUT` — input/output parameter.
pub const PROARGMODE_INOUT: i8 = b'b' as i8;
/// `PROARGMODE_VARIADIC` — variadic array parameter.
pub const PROARGMODE_VARIADIC: i8 = b'v' as i8;
/// `PROARGMODE_TABLE` — table-function column.
pub const PROARGMODE_TABLE: i8 = b't' as i8;

/// `SQLlanguageId` (`catalog/pg_language.h`) — OID of the `sql` language (14).
pub const SQLlanguageId: Oid = 14;

/* ==========================================================================
 * Value-layer carrier consumed by the catalog-tuple seams.
 * ======================================================================== */

/// The fixed-width `Form_pg_proc` columns plus the variable-length columns of
/// one `pg_proc` row, ready for the catalog-tuple owner (`catalog/indexing.c`)
/// to form and insert/update. The owner has already assigned `oid`.
///
/// The variable-length columns cross as their already-marshalled idiomatic
/// forms (the C `nodeToString`/`construct_array`/`oidvector` framing happens at
/// the boundary): `proargtypes` is the input-argument OID vector;
/// `proallargtypes` / `proargmodes` / `proargnames` / `protrftypes` /
/// `proconfig` are `Option<Vec<…>>` (`None` ≡ the C `nulls[…] = true`);
/// `proargdefaults` / `prosqlbody` cross as their `nodeToString` text;
/// `prosrc` is the function source; `probin` the binary reference; `proacl`
/// the default ACL array. Field types/order verified against
/// `FormData_pg_proc` (catalog/pg_proc.h).
#[derive(Clone, Debug)]
pub struct PgProcInsertRow {
    /// The fixed-width columns (`oid` is the row OID the owner assigned).
    pub fields: ProcFormFields,
    /// `proargtypes` (`oidvector`, `BKI_FORCE_NOT_NULL`) — input parameter types.
    pub proargtypes: alloc::vec::Vec<Oid>,
    /// `proallargtypes` (`Oid[]`) — all parameter types, `None` ≡ SQL NULL.
    pub proallargtypes: Option<alloc::vec::Vec<Oid>>,
    /// `proargmodes` (`char[]`) — parameter modes, `None` ≡ SQL NULL.
    pub proargmodes: Option<alloc::vec::Vec<i8>>,
    /// `proargnames` (`text[]`) — parameter names, `None` ≡ SQL NULL. Each entry
    /// `None` is an unnamed parameter (the C "" empty-name slot).
    pub proargnames: Option<alloc::vec::Vec<Option<String>>>,
    /// `proargdefaults` (`pg_node_tree`) — `nodeToString(parameterDefaults)`,
    /// `None` ≡ SQL NULL (the C `parameterDefaults == NIL`).
    pub proargdefaults: Option<String>,
    /// `protrftypes` (`Oid[]`) — transform types, `None` ≡ SQL NULL.
    pub protrftypes: Option<alloc::vec::Vec<Oid>>,
    /// `prosrc` (`text`, `BKI_FORCE_NOT_NULL`) — the function source text.
    pub prosrc: String,
    /// `probin` (`text`) — the binary reference, `None` ≡ SQL NULL.
    pub probin: Option<String>,
    /// `prosqlbody` (`pg_node_tree`) — `nodeToString(prosqlbody)`, `None` ≡ SQL
    /// NULL.
    pub prosqlbody: Option<String>,
    /// `proconfig` (`text[]`) — GUC set clauses (`"name=value"`), `None` ≡ SQL
    /// NULL.
    pub proconfig: Option<alloc::vec::Vec<String>>,
    /// `proacl` (`aclitem[]`) — the default ACL array, `None` ≡ SQL NULL.
    pub proacl: Option<ArrayType>,
}

/// The fixed-width `Form_pg_proc` columns of one `pg_proc` row. Field
/// types/order verified against `FormData_pg_proc` (catalog/pg_proc.h); the
/// `oidvector`/`text[]`/`pg_node_tree`/`aclitem[]` variable-length columns ride
/// on [`PgProcInsertRow`] instead.
#[derive(Clone, Debug, PartialEq)]
pub struct ProcFormFields {
    pub oid: Oid,
    /// `proname` — the routine name (`NameData`).
    pub proname: String,
    pub pronamespace: Oid,
    pub proowner: Oid,
    pub prolang: Oid,
    pub procost: f32,
    pub prorows: f32,
    pub provariadic: Oid,
    pub prosupport: Oid,
    pub prokind: i8,
    pub prosecdef: bool,
    pub proleakproof: bool,
    pub proisstrict: bool,
    pub proretset: bool,
    pub provolatile: i8,
    pub proparallel: i8,
    pub pronargs: i16,
    pub pronargdefaults: i16,
    pub prorettype: Oid,
}
