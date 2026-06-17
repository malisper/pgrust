//! `utils/adt/ruleutils.c` — the constraint-definition deparser
//! (`pg_get_constraintdef` / `pg_get_constraintdef_ext`, the
//! `pg_get_constraintdef_worker` body, ruleutils.c 2146-2660).
//!
//! # Status: structurally present, catalog-substrate-bounded (seam-and-panic)
//!
//! The SQL-callable entries (`pg_get_constraintdef`, `pg_get_constraintdef_ext`)
//! are registered fmgr builtins (`fmgr_builtins.rs`) and dispatch into
//! [`pg_get_constraintdef_worker`] here. The CHECK-constraint arm's expression
//! rendering would be driven by the ported `deparse_expression` /
//! `get_rule_expr` engine in this crate.
//!
//! The worker cannot yet run, because it opens with an MVCC `systable_beginscan`
//! over `pg_constraint` keyed by OID and projects `Form_pg_constraint`
//! (ruleutils.c 2200-2240) — `contype` / `conrelid` / `conkey` / `confkey` /
//! `conpfeqop` / `conexclop` / `conbin` / … — followed (per `contype`) by
//! `generate_relation_name` for the owning/foreign table, `get_attname` for the
//! key columns, `generate_operator_name` for FK/exclusion operators,
//! `decompile_column_index` array deforms, and `get_constraint_name`. The
//! `pg_constraint` scan-and-project owner, `generate_relation_name`, and the
//! attribute-name reader are unported for this entry, so the worker mirrors PG
//! up to that first scan and then panics — it does **not** fabricate a
//! definition.
//!
//! This is the documented seam boundary: the FK/PK/UNIQUE/EXCLUDE arms need the
//! `pg_constraint` projection + name generators; the CHECK arm additionally
//! reaches the (ported) `deparse_expression` over `conbin`. When those catalog
//! readers land, the worker body fills in here without touching the fmgr layer.

use mcx::{Mcx, PgString};
use types_core::primitive::Oid;
use types_error::PgResult;

/// `pg_get_constraintdef_worker(constraintId, fullCommand, prettyFlags,
/// missing_ok)` (ruleutils.c 2193-2620). Returns the constraint definition
/// text, or `Ok(None)` when `missing_ok` and the constraint is gone (the fmgr
/// callers pass `missing_ok = true`).
///
/// SEAM-AND-PANIC: the body's first step is the MVCC `pg_constraint` scan,
/// whose owner (the by-OID `pg_constraint` projection to `Form_pg_constraint`)
/// is unported for this entry. See the module docs for the full unported
/// dependency list.
pub fn pg_get_constraintdef_worker<'mcx>(
    _mcx: Mcx<'mcx>,
    constraint_id: Oid,
    _full_command: bool,
    _pretty_flags: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    // C: scandesc = systable_beginscan(conDesc, ConstraintOidIndexId, …);
    //    tup = systable_getnext(scandesc);
    //    conForm = (Form_pg_constraint) GETSTRUCT(tup);  (ruleutils.c 2210-2240)
    panic!(
        "ruleutils pg_get_constraintdef_worker(constraint {}): the pg_constraint \
         by-OID projection (-> Form_pg_constraint), generate_relation_name, the \
         attribute-name reader, and generate_operator_name owners are unported \
         for this entry — the CHECK arm's deparse_expression over conbin is \
         ported, but the catalog reads that feed every arm are not yet installed",
        constraint_id
    );
}
