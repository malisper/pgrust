//! `utils/adt/ruleutils.c` — the extended-statistics object deparser
//! (`pg_get_statisticsobjdef` / `pg_get_statisticsobjdef_columns` /
//! `pg_get_statisticsobjdef_expressions`, sharing the
//! `pg_get_statisticsobj_worker` body, ruleutils.c 1606-1900).
//!
//! # Status: structurally present, catalog-substrate-bounded (seam-and-panic)
//!
//! The SQL-callable entries are registered fmgr builtins (`fmgr_builtins.rs`)
//! and dispatch into [`pg_get_statisticsobj_worker`] here. Mirroring the C body,
//! the worker opens with `SearchSysCache1(STATEXTOID, statextid)` (available via
//! the `statext_search_tuple` syscache seam) and then must deform
//! `Form_pg_statistic_ext` (`stxrelid` / `stxname` / `stxnamespace` / `stxkeys`
//! / `stxkind` / `stxexprs`), call `get_attname` for the simple column keys,
//! build a `deparse_context_for` over the owning relation, and
//! `deparse_expression_pretty` each expression key.
//!
//! The `Form_pg_statistic_ext` field deform (the `int2vector` `stxkeys`, the
//! `char[]` `stxkind` array, the `pg_node_tree` `stxexprs`) projection owner is
//! unported for this entry — exactly the same documented seam boundary as the
//! sibling `pg_get_constraintdef_worker`. The worker mirrors PG up to the
//! syscache fetch and then panics; it does **not** fabricate a definition.
//!
//! In the catalog-empty common case (a relation with no extended statistics, the
//! psql `\d` describe target list), the `pg_statistic_ext` scan returns no rows,
//! the SQL-level join produces no input to these functions, and the worker is
//! never actually invoked — only resolved (`fmgr_info`). Registering the entries
//! here is what lets that resolution succeed.

use mcx::{Mcx, PgString};
use types_core::primitive::Oid;
use types_error::PgResult;

/// `pg_get_statisticsobj_worker(statextid, columns_only, missing_ok)`
/// (ruleutils.c 1654-1837). Returns the decompiled statistics-object text, or
/// `Ok(None)` when `missing_ok` and the object is gone (all three fmgr callers
/// pass `missing_ok = true`).
///
/// SEAM-AND-PANIC: after the (ported) `STATEXTOID` syscache fetch, the body
/// deforms `Form_pg_statistic_ext` (`stxkeys` `int2vector`, `stxkind` `char[]`
/// `ArrayType`, `stxexprs` `pg_node_tree`) and calls `get_attname` /
/// `get_namespace_name_or_temp` / `generate_relation_name`; those catalog
/// projection owners are unported for this entry. See the module docs.
pub fn pg_get_statisticsobj_worker<'mcx>(
    mcx: Mcx<'mcx>,
    statextid: Oid,
    _columns_only: bool,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    // C: statexttup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statextid));
    //    if (!HeapTupleIsValid(statexttup)) {
    //        if (missing_ok) return NULL;
    //        elog(ERROR, "cache lookup failed for statistics object %u", statextid);
    //    }
    let tup = backend_utils_cache_syscache_seams::statext_search_tuple::call(mcx, statextid)?;
    if tup.is_none() {
        if missing_ok {
            // C: return NULL;
            return Ok(None);
        }
        // C: elog(ERROR, "cache lookup failed for statistics object %u", statextid);
        return Err(types_error::PgError::error(alloc::format!(
            "cache lookup failed for statistics object {statextid}"
        )));
    }

    // C: deform Form_pg_statistic_ext (stxkeys/stxkind/stxexprs/stxrelid/…),
    //    get_attname() each simple key, deparse_context_for() + deparse each
    //    expression key (ruleutils.c 1684-1835).
    panic!(
        "ruleutils pg_get_statisticsobj_worker(statext {}): the Form_pg_statistic_ext \
         field deform (stxkeys int2vector, stxkind char[] ArrayType, stxexprs \
         pg_node_tree) projection owner, get_attname, and \
         get_namespace_name_or_temp / generate_relation_name are unported for this \
         entry — the deparse_expression_pretty engine over stxexprs is ported, but \
         the catalog deform that feeds it is not yet installed",
        statextid
    );
}
