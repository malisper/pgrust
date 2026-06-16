//! Top-level utility-statement entry points (`parse_utilcmd.c`).
//!
//! [`transformCreateStmt`] is GROUNDED: the `ParseState` / `CreateStmtContext`
//! setup, the element dispatch, the not-null propagation, the index-constraint /
//! FK / CHECK postprocessing, and the before/after output assembly all run
//! in-crate over the owned node tree; only the creation-namespace lookup, type
//! validation, and the catalog/relcache leaves cross the outward seams.
//!
//! [`transformAlterTableStmt`] / [`transformIndexStmt`] / [`transformStatsStmt`]
//! open the target relation through the relcache and walk USING / index / stats
//! expressions; that machinery is not reachable from this crate, so they route
//! through the outward seams. [`transformRuleStmt`] (the inward seam this crate
//! owns) ports the entry point and delegates the relcache OLD/NEW fake-RTE +
//! analyze.c-driven action transform to the outward seam.

use mcx::{Mcx, PgBox, PgString, PgVec};

use backend_utils_error::ereport;
use types_core::Oid;
use types_error::{
    ErrorLocation, PgResult, ERRCODE_DUPLICATE_TABLE, ERRCODE_INVALID_OBJECT_DEFINITION, ERROR,
    NOTICE,
};

use types_nodes::copy_query::Query;
use types_nodes::ddlnodes::{CreateStmt, RuleStmt};
use types_nodes::nodes::Node;

use backend_parser_parse_utilcmd_outward_seams as sx;
use backend_parser_small1::make_parsestate;

use crate::column::transformColumnDefinition;
use crate::constraint::{transformCheckConstraints, transformTableConstraint};
use crate::core::{CreateStmtContext, NodePtr};
use crate::fk_check_attrs::transformFKConstraints;
use crate::index_constraint::transformIndexConstraints;
use crate::like::{transformOfType, transformTableLikeClause};

const INVALID_OID: Oid = 0;
const RELPERSISTENCE_TEMP: i8 = b't' as i8;

/// `ErrorLocation` for `ereport(...).finish(...)` non-error emits in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("parse_utilcmd.c", 0, funcname)
}

/// `transformCreateStmt` — parse analysis for CREATE TABLE. Returns a list of
/// utility commands to be executed in sequence (the transformed `CreateStmt`,
/// preceded by `cxt.blist` and followed by `cxt.alist` / `save_alist`).
pub fn transformCreateStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: NodePtr<'mcx>,
    query_string: &str,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    // The caller hands us a CreateStmt (or CreateForeignTableStmt). The shared
    // struct is `CreateStmt`; the foreign-table variant carries the same fields.
    let (mut stmt, isforeign, stmt_type): (CreateStmt<'mcx>, bool, &'static str) =
        match PgBox::into_inner(stmt) {
            Node::CreateStmt(s) => (s, false, "CREATE TABLE"),
            Node::CreateForeignTableStmt(s) => {
                (PgBox::into_inner(s.base), true, "CREATE FOREIGN TABLE")
            }
            other => unreachable!("transformCreateStmt: not a CreateStmt node: {}", other.node_tag()),
        };

    // Set up pstate.
    let mut pstate = make_parsestate(mcx, None)?;
    pstate.p_sourcetext = Some(PgString::from_str_in(query_string, mcx)?);

    // Look up (and permission-check / lock) the creation namespace. Returns the
    // (mutated) relation node, any preexisting relation of that name, and the
    // namespace name (used to schema-qualify the relation).
    let relation = match stmt.relation.take() {
        Some(rv) => rv,
        None => unreachable!("CreateStmt.relation must be a RangeVar"),
    };
    let (relation, existing_relid, namespace_name) =
        sx::RangeVarGetAndCheckCreationNamespace::call(mcx, relation, stmt.if_not_exists)?;
    stmt.relation = Some(relation);

    // Pull the (possibly-mutated) relation's schemaname / relpersistence / name.
    let (schemaname_is_none, relpersistence, relname) = match stmt.relation.as_deref() {
        Some(Node::RangeVar(rv)) => (
            rv.schemaname.is_none(),
            rv.relpersistence,
            rv.relname.as_ref().map_or_else(alloc::string::String::new, |s| s.as_str().into()),
        ),
        _ => unreachable!("CreateStmt.relation must be a RangeVar"),
    };

    // IF NOT EXISTS and the relation already exists: bail with a NOTICE.
    if stmt.if_not_exists && existing_relid != INVALID_OID {
        // (checkMembershipInCurrentExtension would run here, behind the namespace
        // lookup above.)
        ereport(NOTICE)
            .errcode(ERRCODE_DUPLICATE_TABLE)
            .errmsg(alloc::format!("relation \"{relname}\" already exists, skipping"))
            .finish(here("transformCreateStmt"))?;
        return Ok(PgVec::new_in(mcx));
    }

    // If the target name isn't schema-qualified, make it so (unless a local temp
    // table, which is effectively in pg_temp).
    if schemaname_is_none && relpersistence != RELPERSISTENCE_TEMP {
        if let Some(Node::RangeVar(rv)) = stmt.relation.as_deref_mut() {
            rv.schemaname = namespace_name;
        }
    }

    // Set up CreateStmtContext.
    let ispartitioned = stmt.partspec.is_some();
    let oftype = stmt.ofTypename.is_some();
    let relation_clone = match stmt.relation.as_deref() {
        Some(n) => Some(mcx::alloc_in(mcx, n.clone_in(mcx)?)?),
        None => None,
    };
    let inh_relations = {
        let mut v: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
        for n in stmt.inhRelations.iter() {
            v.push(mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
        }
        v
    };
    let mut cxt = CreateStmtContext {
        mcx,
        pstate,
        stmtType: stmt_type,
        relation: relation_clone,
        rel_oid: INVALID_OID,
        inhRelations: inh_relations,
        isforeign,
        isalter: false,
        columns: PgVec::new_in(mcx),
        ckconstraints: PgVec::new_in(mcx),
        nnconstraints: PgVec::new_in(mcx),
        fkconstraints: PgVec::new_in(mcx),
        ixconstraints: PgVec::new_in(mcx),
        likeclauses: PgVec::new_in(mcx),
        blist: PgVec::new_in(mcx),
        alist: PgVec::new_in(mcx),
        pkey: None,
        ispartitioned,
        partbound: match stmt.partbound.as_deref() {
            Some(n) => Some(mcx::alloc_in(mcx, n.clone_in(mcx)?)?),
            None => None,
        },
        ofType: oftype,
    };

    // grammar enforces: !stmt->ofTypename || !stmt->inhRelations
    if let Some(of_typename) = stmt.ofTypename.as_deref() {
        let of = mcx::alloc_in(mcx, of_typename.clone_in(mcx)?)?;
        transformOfType(&mut cxt, of)?;
    }

    if stmt.partspec.is_some() && !stmt.inhRelations.is_empty() && stmt.partbound.is_none() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("cannot create partitioned table as inheritance child")
            .into_error());
    }

    // Run through each primary element, separating column defs from constraints.
    let table_elts = core::mem::replace(&mut stmt.tableElts, PgVec::new_in(mcx));
    for element in table_elts {
        match element.as_ref() {
            Node::ColumnDef(_) => transformColumnDefinition(&mut cxt, element)?,
            Node::Constraint(_) => transformTableConstraint(&mut cxt, element)?,
            Node::TableLikeClause(_) => transformTableLikeClause(&mut cxt, element)?,
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(alloc::format!(
                        "unrecognized node type: {}",
                        other.node_tag()
                    ))
                    .into_error());
            }
        }
    }

    // Transfer anything already in cxt.alist into save_alist.
    let save_alist = core::mem::replace(&mut cxt.alist, PgVec::new_in(mcx));

    // Before processing index constraints, scan all not-null constraints to
    // propagate the is_not_null flag to each corresponding ColumnDef.
    propagate_notnull(&mut cxt);

    // Postprocess constraints that give rise to index definitions.
    transformIndexConstraints(&mut cxt)?;

    // Re-consideration of LIKE clauses happens after index creation but before
    // foreign keys.
    let likeclauses = core::mem::replace(&mut cxt.likeclauses, PgVec::new_in(mcx));
    cxt.alist.extend(likeclauses);

    // Postprocess foreign-key constraints.
    transformFKConstraints(&mut cxt, true, false)?;

    // Postprocess check constraints (skip validation for new non-foreign tables).
    let skip = !cxt.isforeign;
    transformCheckConstraints(&mut cxt, skip);

    // Output results.
    stmt.tableElts = core::mem::replace(&mut cxt.columns, PgVec::new_in(mcx));
    stmt.constraints = core::mem::replace(&mut cxt.ckconstraints, PgVec::new_in(mcx));
    stmt.nnconstraints = core::mem::replace(&mut cxt.nnconstraints, PgVec::new_in(mcx));

    let mut result = core::mem::replace(&mut cxt.blist, PgVec::new_in(mcx));
    result.push(mcx::alloc_in(mcx, Node::CreateStmt(stmt))?);
    let alist = core::mem::replace(&mut cxt.alist, PgVec::new_in(mcx));
    result.extend(alist);
    result.extend(save_alist);

    Ok(result)
}

/// The not-null-propagation loop from `transformCreateStmt`: for each table-level
/// NOT NULL constraint, set `is_not_null` on the matching `ColumnDef`.
fn propagate_notnull(cxt: &mut CreateStmtContext<'_>) {
    // Collect target column names first (strVal(linitial(nn->keys))).
    let mut colnames: alloc::vec::Vec<alloc::string::String> = alloc::vec::Vec::new();
    for nn in cxt.nnconstraints.iter() {
        if let Node::Constraint(c) = nn.as_ref() {
            if let Some(k) = c.keys.first() {
                if let Node::String(s) = k.as_ref() {
                    colnames.push(s.sval.as_str().into());
                }
            }
        }
    }

    for colname in colnames {
        for cd in cxt.columns.iter_mut() {
            if let Node::ColumnDef(col) = cd.as_mut() {
                // not our column?
                if col.colname.as_ref().map(PgString::as_str) != Some(colname.as_str()) {
                    continue;
                }
                // Already marked not-null? Nothing to do
                if col.is_not_null {
                    break;
                }
                // Bingo, we're done for this constraint
                col.is_not_null = true;
                break;
            }
        }
    }
}

/// `transformAlterTableStmt` — parse analysis for ALTER TABLE. The
/// per-subcommand dispatch opens the target relation through the relcache and
/// walks USING / ADD-COLUMN expressions; routed through the outward seam.
/// Returns `(stmt, beforeStmts, afterStmts)`.
pub fn transformAlterTableStmt<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    stmt: NodePtr<'mcx>,
    query_string: &str,
) -> PgResult<(
    NodePtr<'mcx>,
    PgVec<'mcx, NodePtr<'mcx>>,
    PgVec<'mcx, NodePtr<'mcx>>,
)> {
    sx::transformAlterTableStmt::call(mcx, relid, stmt, query_string)
}

/// `transformIndexStmt` — parse analysis for CREATE INDEX / ALTER TABLE. Opens
/// the parent relation by OID, adds it to the rtable, and transforms the WHERE
/// predicate and any index-element expressions; routed through the outward seam.
pub fn transformIndexStmt<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    stmt: NodePtr<'mcx>,
    query_string: &str,
) -> PgResult<NodePtr<'mcx>> {
    // Nothing to do if the statement is already transformed.
    if let Node::IndexStmt(s) = stmt.as_ref() {
        if s.transformed {
            return Ok(stmt);
        }
    }
    sx::transformIndexStmt::call(mcx, relid, stmt, query_string)
}

/// `transformStatsStmt` — parse analysis for CREATE STATISTICS. Walks the stat
/// expressions over the parent relation's columns; routed through the seam.
pub fn transformStatsStmt<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    stmt: NodePtr<'mcx>,
    query_string: &str,
) -> PgResult<NodePtr<'mcx>> {
    // Nothing to do if the statement is already transformed.
    if let Node::CreateStatsStmt(s) = stmt.as_ref() {
        if s.transformed {
            return Ok(stmt);
        }
    }
    sx::transformStatsStmt::call(mcx, relid, stmt, query_string)
}

/// `transformRuleStmt` — parse analysis for CREATE RULE. Builds the OLD/NEW
/// pseudo-relation rtable and runs each action statement through analyze.c;
/// the relcache OLD/NEW fake-RTE + analyze-driven transform is routed through
/// the outward seam. Returns `(actions, where_clause)`.
pub fn transformRuleStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &RuleStmt<'_>,
    query_string: &str,
) -> PgResult<(PgVec<'mcx, Query<'mcx>>, Option<Node<'mcx>>)> {
    let stmt = stmt.clone_in(mcx)?;
    sx::transformRuleStmtCatalog::call(mcx, &stmt, query_string)
}
