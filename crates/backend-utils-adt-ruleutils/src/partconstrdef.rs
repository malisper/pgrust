//! `utils/adt/ruleutils.c` — the partition-constraint deparsers
//! (`pg_get_partition_constraintdef`, ruleutils.c 2090-2120, and
//! `pg_get_partconstrdef_string`, ruleutils.c 2122-2137).
//!
//! Both build the partition's implicit CHECK expression with
//! `get_partition_qual_relid` (partcache.c, reached through its seam — the
//! qual generators `get_qual_for_range` / `get_qual_for_list` /
//! `get_qual_for_hash` live in partbounds), then deparse it against a
//! single-relation deparse context via the ported expression deparser.
//!
//! The SQL-callable `pg_get_partition_constraintdef(oid) -> text` (fmgr OID
//! 3408) returns NULL for a relation with no partition constraint (a default
//! partition that is the only partition, or a non-partition OID). The internal
//! `pg_get_partconstrdef_string(partitionId, aliasname)` returns the
//! plain (no pretty-printing) constraint text with the caller's alias, used by
//! `ri_triggers.c` to build the partition-aware FK enforcement query.

use mcx::{Mcx, PgString, PgVec};
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};

use crate::{deparse_context_for, deparse_expression, deparse_expression_pretty, PRETTYFLAG_INDENT};

/// `pg_get_partition_constraintdef(relationId)` (ruleutils.c 2090-2119): the
/// partition constraint expression as text for the input relation, or
/// `Ok(None)` (the C `PG_RETURN_NULL()`) when there is no partition
/// constraint.
pub fn pg_get_partition_constraintdef<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    // constr_expr = get_partition_qual_relid(relationId);
    let constr_expr =
        backend_utils_cache_partcache_seams::get_partition_qual_relid::call(mcx, relation_id)?;

    // Quick exit if no partition constraint.
    let constr_expr = match constr_expr {
        Some(e) => e,
        None => return Ok(None),
    };

    // Deparse and return the constraint expression.
    // prettyFlags = PRETTYFLAG_INDENT;
    let pretty_flags = PRETTYFLAG_INDENT;
    // context = deparse_context_for(get_relation_name(relationId), relationId);
    let relname = backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, relation_id)?
        .ok_or_else(|| {
            PgError::error(alloc::format!("cache lookup failed for relation {relation_id}"))
        })?;
    let context = deparse_context_for(mcx, relname.as_str(), relation_id)?;
    // consrc = deparse_expression_pretty((Node *) constr_expr, context, false,
    //                                    false, prettyFlags, 0);
    let consrc = deparse_expression_pretty(
        mcx,
        constr_expr.as_ref(),
        context,
        false,
        false,
        pretty_flags,
        0,
    )?;

    // PG_RETURN_TEXT_P(string_to_text(consrc));
    Ok(Some(consrc))
}

/// `pg_get_partconstrdef_string(partitionId, aliasname)` (ruleutils.c
/// 2122-2137): the partition constraint as a plain (no pretty-printing) string
/// with the given alias. Unlike the SQL-callable entry, the C body always
/// builds a deparse context and calls `deparse_expression` even when
/// `constr_expr` is NULL — deparsing a NULL expression yields the empty string
/// — so this returns `Ok(None)` (caller treats it as the empty constraint)
/// only in that no-constraint case, matching what `ri_triggers.c` expects.
pub fn pg_get_partconstrdef_string<'mcx>(
    mcx: Mcx<'mcx>,
    partition_id: Oid,
    aliasname: &str,
) -> PgResult<Option<PgString<'mcx>>> {
    // constr_expr = get_partition_qual_relid(partitionId);
    let constr_expr =
        backend_utils_cache_partcache_seams::get_partition_qual_relid::call(mcx, partition_id)?;
    // context = deparse_context_for(aliasname, partitionId);
    let context: PgVec<'mcx, _> = deparse_context_for(mcx, aliasname, partition_id)?;

    // return deparse_expression((Node *) constr_expr, context, true, false);
    // C passes a possibly-NULL Node*; deparsing NULL produces the empty string.
    // The owned model has no NULL Node, so map the no-constraint case to the
    // empty string explicitly (ri_triggers treats empty == no constraint).
    match constr_expr {
        Some(e) => {
            let s = deparse_expression(mcx, e.as_ref(), context, true, false)?;
            Ok(Some(s))
        }
        None => Ok(Some(PgString::from_str_in("", mcx)?)),
    }
}
