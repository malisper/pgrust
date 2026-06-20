//! `utils/adt/ruleutils.c` — the partition-key-definition deparser
//! (`pg_get_partkeydef` / `pg_get_partkeydef_columns`, the
//! `pg_get_partkeydef_worker` body, ruleutils.c 1902-2088).
//!
//! The worker reverse-lists a partitioned table's partition key from its
//! `pg_partitioned_table` row: `RANGE (…)` / `LIST (…)` / `HASH (…)` with the
//! per-key column / expression list, each decorated with `COLLATE` and the
//! operator class name when not the column default. It reads the catalog row
//! via the `open_partrel_tuple` syscache projection (`SearchSysCache1(PARTRELID)`
//! + the `partclass` / `partcollation` oidvectors + the **non-const-folded**
//! `partexprs` `pg_node_tree`, exactly as the C wants for display), then renders
//! names through `generate_collation_name` / [`crate::get_opclass_name`] and the
//! expression columns through the ported `deparse_expression_pretty` engine.

use alloc::format;
use alloc::string::String;
use mcx::{Mcx, PgString};
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use types_partition::{
    PARTITION_STRATEGY_HASH, PARTITION_STRATEGY_LIST, PARTITION_STRATEGY_RANGE,
};

use crate::{
    deparse_context_for, deparse_expression_pretty, get_opclass_name, generate_collation_name,
    oid_is_valid_pub, quote_identifier,
};

/// `pg_get_partkeydef_worker(relid, prettyFlags, attrsOnly, missing_ok)`
/// (ruleutils.c 1936-2088). The SQL-callable `pg_get_partkeydef` passes
/// `attrsOnly = false`, `missing_ok = true`; the internal
/// `pg_get_partkeydef_columns` passes `attrsOnly = true`, `missing_ok = false`.
///
/// Returns the partition-key definition text, or `Ok(None)` when `missing_ok`
/// and the relation is not partitioned / gone.
pub fn pg_get_partkeydef_worker<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    pretty_flags: i32,
    attrs_only: bool,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    // tuple = SearchSysCache1(PARTRELID, relid); the projection reads partstrat/
    // partnatts (scalar) + partclass/partcollation (oidvector) + partexprs
    // (pg_node_tree, raw stringToNode — NOT const-folded, matching the C).
    let info = match backend_utils_cache_syscache_seams::open_partrel_tuple::call(mcx, relid)? {
        Some(i) => i,
        None => {
            if missing_ok {
                return Ok(None);
            }
            return Err(PgError::error(format!(
                "cache lookup failed for partition key of {relid}"
            )));
        }
    };

    let partclass = &info.partclass;
    let partcollation = &info.partcollation;
    let partexprs = &info.partexprs;
    // partexpr_item = list_head(partexprs).
    let mut partexpr_idx: usize = 0;

    // context = deparse_context_for(get_relation_name(relid), relid);
    let relname = backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, relid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for relation {relid}")))?;

    let mut buf = String::new();

    match info.strategy {
        s if s == PARTITION_STRATEGY_HASH => {
            if !attrs_only {
                buf.push_str("HASH");
            }
        }
        s if s == PARTITION_STRATEGY_LIST => {
            if !attrs_only {
                buf.push_str("LIST");
            }
        }
        s if s == PARTITION_STRATEGY_RANGE => {
            if !attrs_only {
                buf.push_str("RANGE");
            }
        }
        other => {
            return Err(PgError::error(format!(
                "unexpected partition strategy: {}",
                other as i32
            )))
        }
    }

    if !attrs_only {
        buf.push_str(" (");
    }
    let mut sep = "";
    for keyno in 0..info.partnatts as usize {
        let attnum = info.partattrs[keyno];

        buf.push_str(sep);
        sep = ", ";

        let keycoltype: Oid;
        let keycolcollation: Oid;

        if attnum != 0 {
            // Simple attribute reference.
            let attname = backend_utils_cache_lsyscache_seams::get_attname::call(
                mcx, relid, attnum, false,
            )?
            .ok_or_else(|| {
                PgError::error(format!(
                    "cache lookup failed for attribute {attnum} of relation {relid}"
                ))
            })?;
            let q = quote_identifier(mcx, attname.as_str())?;
            buf.push_str(q.as_str());
            let (typid, _typmod, coll) =
                backend_utils_cache_lsyscache_seams::get_atttypetypmodcoll::call(relid, attnum)?;
            keycoltype = typid;
            keycolcollation = coll;
        } else {
            // Expression.
            let partkey = partexprs
                .get(partexpr_idx)
                .ok_or_else(|| PgError::error("too few entries in partexprs list"))?;
            partexpr_idx += 1;

            // The partexprs cells are bare `Expr`s; wrap as a Node for the
            // deparse engine / nodeFuncs.
            let node = types_nodes::nodes::Node::mk_expr(mcx, partkey.clone_in(mcx)?)?;
            let node_box = mcx::alloc_in(mcx, node)?;

            // str = deparse_expression_pretty(partkey, context, false, false, ...).
            let context = deparse_context_for(mcx, relname.as_str(), relid)?;
            let s = deparse_expression_pretty(
                mcx,
                node_box.as_ref(),
                context,
                false,
                false,
                pretty_flags,
                0,
            )?;
            // Need parens if it's not a bare function call.
            if crate::expr_deparse::looks_like_function_pub(node_box.as_ref()) {
                buf.push_str(s.as_str());
            } else {
                buf.push('(');
                buf.push_str(s.as_str());
                buf.push(')');
            }

            keycoltype = crate::expr_deparse::expr_type_of_node(node_box.as_ref())?;
            keycolcollation = crate::expr_deparse::expr_collation_of_node(node_box.as_ref())?;
        }

        // Add collation, if not default for column.
        let partcoll = partcollation[keyno];
        if !attrs_only && oid_is_valid_pub(partcoll) && partcoll != keycolcollation {
            let cn = generate_collation_name(mcx, partcoll)?;
            buf.push_str(" COLLATE ");
            buf.push_str(cn.as_str());
        }

        // Add the operator class name, if not default.
        if !attrs_only {
            get_opclass_name(mcx, &mut buf, partclass[keyno], keycoltype)?;
        }
    }

    if !attrs_only {
        buf.push(')');
    }

    Ok(Some(PgString::from_str_in(&buf, mcx)?))
}
