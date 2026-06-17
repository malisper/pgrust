//! `utils/adt/ruleutils.c` — the index-definition deparser
//! (`pg_get_indexdef` / `pg_get_indexdef_ext`, the `pg_get_indexdef_worker`
//! body, ruleutils.c 1178-1660).
//!
//! # Status: structurally present, catalog-substrate-bounded (seam-and-panic)
//!
//! The SQL-callable entries (`pg_get_indexdef`, `pg_get_indexdef_ext`) are
//! registered fmgr builtins (`fmgr_builtins.rs`) and dispatch into
//! [`pg_get_indexdef_worker`] here. The worker's deparse half — column
//! expressions, predicates, opclass / collation rendering — is the ported
//! `get_rule_expr` / `deparse_expression_pretty` engine in this crate.
//!
//! The worker cannot yet run, because its **first** operation is
//! `SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexrelid))` (ruleutils.c
//! 1303) — a read of the `pg_index` syscache projected to `Form_pg_index`
//! (`indrelid` / `indnkeyatts` / `indnatts` / `indisunique` / `indkey` /
//! `indcollation` / `indclass` / `indoption` / `indexprs` / `indpred` / …),
//! followed by `SearchSysCache1(RELOID)` for the index `pg_class` row,
//! `SearchSysCache1(AMOID)` for the access method, `GetIndexAmRoutine`,
//! `deconstruct_array`/`oidvector`/`int2vector` deforms of the index key
//! arrays, `generate_relation_name` for the table, and the per-AM
//! `amcanorder` / opclass-options rendering. None of those catalog readers /
//! name generators is installed for this entry (the `INDEXRELID` syscache
//! projection, `GetIndexAmRoutine`, and `generate_relation_name` owners are all
//! unported), so the worker mirrors PG up to that first read and then panics —
//! it does **not** fabricate a definition.
//!
//! This is the documented seam boundary: when the `pg_index` syscache
//! projection + `generate_relation_name` + AM-routine owners land, the worker
//! body (the column/predicate deparse it would drive is already ported) can be
//! filled in here without touching the fmgr layer.

use mcx::{Mcx, PgString};
use types_core::primitive::Oid;
use types_error::PgResult;

/// `pg_get_indexdef_worker(indexrelid, colno, …, prettyFlags, missing_ok)`
/// (ruleutils.c 1269-1660). Returns the index definition text, or `Ok(None)`
/// when `missing_ok` and the index is gone (the fmgr callers pass
/// `missing_ok = true`).
///
/// SEAM-AND-PANIC: the body's first step is the `pg_index` syscache read, whose
/// owner (the `INDEXRELID` syscache projection to `Form_pg_index`) is unported
/// for this entry. See the module docs for the full unported dependency list.
pub fn pg_get_indexdef_worker<'mcx>(
    _mcx: Mcx<'mcx>,
    indexrelid: Oid,
    _colno: i32,
    _pretty_flags: i32,
) -> PgResult<Option<PgString<'mcx>>> {
    // C: ht_idx = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexrelid));
    //    idxrec = (Form_pg_index) GETSTRUCT(ht_idx);  (ruleutils.c 1303-1310)
    panic!(
        "ruleutils pg_get_indexdef_worker(index {}): the pg_index syscache \
         projection (SearchSysCache1(INDEXRELID) -> Form_pg_index), \
         GetIndexAmRoutine, the index-key oidvector/int2vector deforms, and \
         generate_relation_name owners are unported for this entry — the \
         deparse half (get_rule_expr over the index expressions/predicate) is \
         ported, but the catalog reads that feed it are not yet installed",
        indexrelid
    );
}
