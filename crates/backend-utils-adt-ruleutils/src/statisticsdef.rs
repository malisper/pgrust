//! `utils/adt/ruleutils.c` — the extended-statistics object deparser
//! (`pg_get_statisticsobjdef` / `pg_get_statisticsobjdef_columns` /
//! `pg_get_statisticsobjdef_expressions`, sharing the
//! `pg_get_statisticsobj_worker` body, ruleutils.c 1606-1900).
//!
//! The worker reverse-lists an extended-statistics object from its catalog
//! row. It reads `Form_pg_statistic_ext` (via the `statext_objdef_fields`
//! syscache projection: `stxnamespace` / `stxname` / `stxrelid` / `stxkeys`
//! int2vector / `stxkind` `char[]` array / `stxexprs` `pg_node_tree`), calls
//! `get_attname` for each simple column key, builds a `deparse_context_for`
//! over the owning relation, and `deparse_expression_pretty` each expression
//! key through the ported deparse engine.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use mcx::{Mcx, PgString};
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};

use types_catalog::pg_statistic_ext::{
    STATS_EXT_DEPENDENCIES, STATS_EXT_MCV, STATS_EXT_NDISTINCT,
};

/// `PRETTYFLAG_PAREN` (ruleutils.c 88).
const PRETTYFLAG_PAREN: i32 = 0x0001;

/// `pg_get_statisticsobj_worker(statextid, columns_only, missing_ok)`
/// (ruleutils.c 1654-1837). Returns the decompiled statistics-object text, or
/// `Ok(None)` when `missing_ok` and the object is gone (all three fmgr callers
/// pass `missing_ok = true`).
pub fn pg_get_statisticsobj_worker<'mcx>(
    mcx: Mcx<'mcx>,
    statextid: Oid,
    columns_only: bool,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    // statexttup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statextid));
    // if (!HeapTupleIsValid(statexttup)) {
    //     if (missing_ok) return NULL;
    //     elog(ERROR, "cache lookup failed for statistics object %u", statextid);
    // }
    //
    // The projection deforms the fields the worker reads off GETSTRUCT plus the
    // stxkeys/stxkind/stxexprs variable-length attributes in one syscache fetch.
    let fields =
        backend_utils_cache_syscache_seams::statext_objdef_fields::call(mcx, statextid)?;
    let (stxnamespace, stxname, stxrelid, stxkeys, stxkind, stxexprs_text) = match fields {
        Some(t) => t,
        None => {
            if missing_ok {
                // C: return NULL;
                return Ok(None);
            }
            // C: elog(ERROR, "cache lookup failed for statistics object %u", statextid);
            return Err(PgError::error(format!(
                "cache lookup failed for statistics object {statextid}"
            )));
        }
    };

    // Get the statistics expressions, if any.  (NOTE: we do not use the
    // relcache versions of the expressions, because we want to display
    // non-const-folded expressions.)
    //   exprs = (List *) stringToNode(exprsString);
    let exprs = match &stxexprs_text {
        Some(s) => Some(backend_nodes_read_seams::string_to_node::call(mcx, s.as_str())?),
        None => None,
    };
    let exprs_items: Vec<_> = exprs
        .as_ref()
        .and_then(|n| n.as_list())
        .map(|items| items.iter().collect())
        .unwrap_or_default();

    // ncolumns = statextrec->stxkeys.dim1 + list_length(exprs);
    let ncolumns = stxkeys.len() + exprs_items.len();

    let mut buf = String::new();

    if !columns_only {
        // nsp = get_namespace_name_or_temp(statextrec->stxnamespace);
        // appendStringInfo(&buf, "CREATE STATISTICS %s",
        //                  quote_qualified_identifier(nsp, NameStr(statextrec->stxname)));
        let nsp =
            backend_utils_cache_lsyscache_seams::get_namespace_name_or_temp::call(mcx, stxnamespace)?;
        let qualified = backend_utils_adt_ruleutils_seams::quote_qualified_identifier::call(
            mcx,
            nsp.as_ref().map(|n| n.as_str()),
            stxname.as_str(),
        )?;
        buf.push_str("CREATE STATISTICS ");
        buf.push_str(qualified.as_str());

        // Decode the stxkind column so that we know which stats types to print.
        // (C validated ARR_NDIM/ARR_HASNULL/ARR_ELEMTYPE in the projection; here
        // the projection already returned the 1-D char[] element values.)
        let mut ndistinct_enabled = false;
        let mut dependencies_enabled = false;
        let mut mcv_enabled = false;
        for &enabled in stxkind.iter() {
            let enabled = enabled as i8;
            if enabled == STATS_EXT_NDISTINCT {
                ndistinct_enabled = true;
            } else if enabled == STATS_EXT_DEPENDENCIES {
                dependencies_enabled = true;
            } else if enabled == STATS_EXT_MCV {
                mcv_enabled = true;
            }
            // ignore STATS_EXT_EXPRESSIONS (it's built automatically)
        }

        // If any option is disabled, then we'll need to append the types clause
        // to show which options are enabled.  We omit the types clause on purpose
        // when all options are enabled, so a pg_dump/pg_restore will create all
        // statistics types on a newer postgres version, if the statistics had all
        // options enabled on the original version.
        //
        // But if the statistics is defined on just a single column, it has to be
        // an expression statistics. In that case we don't need to specify kinds.
        if (!ndistinct_enabled || !dependencies_enabled || !mcv_enabled) && ncolumns > 1 {
            let mut gotone = false;

            buf.push_str(" (");

            if ndistinct_enabled {
                buf.push_str("ndistinct");
                gotone = true;
            }

            if dependencies_enabled {
                if gotone {
                    buf.push_str(", ");
                }
                buf.push_str("dependencies");
                gotone = true;
            }

            if mcv_enabled {
                if gotone {
                    buf.push_str(", ");
                }
                buf.push_str("mcv");
            }

            buf.push(')');
        }

        buf.push_str(" ON ");
    }

    // decode simple column references
    //   for (colno = 0; colno < statextrec->stxkeys.dim1; colno++) { ... }
    let mut colno: i32 = 0;
    for &attnum in stxkeys.iter() {
        if colno > 0 {
            buf.push_str(", ");
        }

        // attname = get_attname(statextrec->stxrelid, attnum, false);
        let attname = backend_utils_cache_lsyscache_seams::get_attname::call(
            mcx,
            stxrelid,
            attnum as types_core::AttrNumber,
            false,
        )?
        .ok_or_else(|| {
            PgError::error(format!(
                "cache lookup failed for attribute {attnum} of relation {stxrelid}"
            ))
        })?;

        let q = backend_utils_adt_ruleutils_seams::quote_identifier::call(mcx, attname.as_str())?;
        buf.push_str(q.as_str());

        colno += 1;
    }

    // context = deparse_context_for(get_relation_name(statextrec->stxrelid),
    //                               statextrec->stxrelid);
    let relname = backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, stxrelid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for relation {stxrelid}")))?;

    // foreach(lc, exprs) { str = deparse_expression_pretty(expr, ...); ... }
    for expr in &exprs_items {
        let pretty_flags = PRETTYFLAG_PAREN;
        let context = crate::deparse_context_for(mcx, relname.as_str(), stxrelid)?;
        let str = crate::deparse_expression_pretty(
            mcx,
            expr.as_ref(),
            context,
            false,
            false,
            pretty_flags,
            0,
        )?;

        if colno > 0 {
            buf.push_str(", ");
        }

        // Need parens if it's not a bare function call.
        if crate::expr_deparse::looks_like_function_pub(expr.as_ref()) {
            buf.push_str(str.as_str());
        } else {
            buf.push('(');
            buf.push_str(str.as_str());
            buf.push(')');
        }

        colno += 1;
    }

    if !columns_only {
        // appendStringInfo(&buf, " FROM %s",
        //                  generate_relation_name(statextrec->stxrelid, NIL));
        let from_name = crate::generate_relation_name_catalog(mcx, stxrelid, false)?;
        buf.push_str(" FROM ");
        buf.push_str(from_name.as_str());
    }

    Ok(Some(PgString::from_str_in(&buf, mcx)?))
}
