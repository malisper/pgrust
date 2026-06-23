//! `pg_rewrite` catalog row layout and constants (`catalog/pg_rewrite.h`,
//! PostgreSQL 18.3), trimmed to what the `backend-rewrite-rewriteDefine` port
//! (CREATE RULE) reads and writes.

use ::types_core::primitive::{AttrNumber, Oid};

/* ==========================================================================
 * Catalog relation + index OIDs (pg_rewrite.h CATALOG / DECLARE_*).
 * ======================================================================== */

/// `RewriteRelationId` — `pg_rewrite` (OID 2618).
pub const RewriteRelationId: Oid = 2618;
/// `RewriteOidIndexId` — `pg_rewrite_oid_index` (OID 2692), the unique pkey on
/// `(oid)`.
pub const RewriteOidIndexId: Oid = 2692;
/// `RewriteRelRulenameIndexId` — `pg_rewrite_rel_rulename_index` (OID 2693),
/// the unique index on `(ev_class, rulename)`.
pub const RewriteRelRulenameIndexId: Oid = 2693;

/* ==========================================================================
 * Attribute numbers (genbki, field order of FormData_pg_rewrite).
 * ======================================================================== */

pub const Anum_pg_rewrite_oid: AttrNumber = 1;
pub const Anum_pg_rewrite_rulename: AttrNumber = 2;
pub const Anum_pg_rewrite_ev_class: AttrNumber = 3;
pub const Anum_pg_rewrite_ev_type: AttrNumber = 4;
pub const Anum_pg_rewrite_ev_enabled: AttrNumber = 5;
pub const Anum_pg_rewrite_is_instead: AttrNumber = 6;
pub const Anum_pg_rewrite_ev_qual: AttrNumber = 7;
pub const Anum_pg_rewrite_ev_action: AttrNumber = 8;

/// `Natts_pg_rewrite` — number of columns.
pub const Natts_pg_rewrite: usize = 8;

/* ==========================================================================
 * ev_enabled / ev_type firing-condition constants (rewriteDefine.h).
 * ======================================================================== */

/// `RULE_FIRES_ON_ORIGIN` — `'O'`, the default `ev_enabled` for a new rule.
pub const RULE_FIRES_ON_ORIGIN: u8 = b'O';
/// `RULE_FIRES_ALWAYS` — `'A'`.
pub const RULE_FIRES_ALWAYS: u8 = b'A';
/// `RULE_FIRES_ON_REPLICA` — `'R'`.
pub const RULE_FIRES_ON_REPLICA: u8 = b'R';
/// `RULE_DISABLED` — `'D'`.
pub const RULE_DISABLED: u8 = b'D';

/// `ViewSelectRuleName` (`rewriteSupport.h`) — the mandatory name of a view's
/// ON SELECT rule.
pub const ViewSelectRuleName: &str = "_RETURN";

/* ==========================================================================
 * Row carrier.
 * ======================================================================== */

/// The fixed-length columns of one scanned `pg_rewrite` row
/// (`(Form_pg_rewrite) GETSTRUCT(tup)`), trimmed to what rewriteDefine.c reads
/// via `GETSTRUCT` (`oid`, `ev_class`, `ev_type`, `ev_enabled`). The two
/// variable-length `pg_node_tree` columns (`ev_qual`, `ev_action`) live past
/// the fixed area and are not projected here.
#[derive(Clone, Copy, Debug)]
pub struct FormData_pg_rewrite {
    pub oid: Oid,
    /// `NameData rulename`, as a fixed 64-byte image.
    pub rulename: [u8; 64],
    pub ev_class: Oid,
    pub ev_type: u8,
    pub ev_enabled: u8,
    pub is_instead: bool,
}
