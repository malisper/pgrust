//! `transformIndexConstraints` + `transformIndexConstraint` (`parse_utilcmd.c`).
//!
//! `transformIndexConstraints` (the index-redundancy dedup over `equal()`) and
//! the `IndexStmt`-skeleton construction in `transformIndexConstraint` are
//! node-independent and ported 1:1. The catalog-resident leaf of
//! `transformIndexConstraint` — the ALTER TABLE ADD CONSTRAINT USING INDEX path,
//! the inherited-table column search, the WITHOUT OVERLAPS type check, the
//! `SystemAttributeByName` lookups, and the PRIMARY-KEY-implied not-null
//! additions — crosses the outward seam.

use mcx::{Mcx, PgString, PgVec};

use backend_nodes_equalfuncs::equal_node;
use backend_utils_error::ereport;
use types_error::{PgResult, ERRCODE_INVALID_TABLE_DEFINITION, ERROR};

use types_nodes::ddlnodes::{IndexStmt, CONSTR_EXCLUSION, CONSTR_PRIMARY};
use types_nodes::nodes::Node;

use backend_parser_parse_utilcmd_outward_seams as sx;

use crate::core::{CreateStmtContext, NodePtr};
use crate::errpos::parser_errposition;

const DEFAULT_INDEX_TYPE: &str = "btree";

/// `transformIndexConstraints` — process constraints that give rise to index
/// definitions, then remove redundant index specifications (UNIQUE PRIMARY KEY,
/// etc.), keeping the PK index in preference; the surviving `IndexStmt`s are
/// appended to `cxt.alist`.
pub fn transformIndexConstraints(cxt: &mut CreateStmtContext<'_>) -> PgResult<()> {
    let mcx = cxt.mcx;

    let mut indexlist: PgVec<'_, NodePtr<'_>> = PgVec::new_in(mcx);

    // Run through the constraints that need to generate an index.
    let ixconstraints = core::mem::replace(&mut cxt.ixconstraints, PgVec::new_in(mcx));
    for constraint in ixconstraints {
        // Assert(contype == PRIMARY | UNIQUE | EXCLUSION) is implicit.
        let index = transformIndexConstraint(constraint, cxt)?;
        indexlist.push(index);
    }

    // Scan the index list and remove any redundant index specifications.
    let mut finalindexlist: PgVec<'_, NodePtr<'_>> = PgVec::new_in(mcx);

    // If we have a pkey, keep it in preference to others; record whether each
    // entry in `indexlist` is the pkey by identity (C compares pointers).
    if let Some(pkey) = cxt.pkey.take() {
        // We will re-store cxt.pkey below; mark this index so the loop skips it.
        finalindexlist.push(pkey);
        // Re-set cxt.pkey to point at the same node now living in finalindexlist.
        cxt.pkey = Some(mcx::alloc_in(mcx, finalindexlist[0].clone_in(mcx)?)?);
    }

    for index in indexlist {
        // if it's pkey, it's already in finalindexlist
        if cxt.pkey.is_some() && index_equals_pkey(&index, cxt) {
            continue;
        }

        let mut keep = true;
        for k in 0..finalindexlist.len() {
            if indexes_equivalent(&index, &finalindexlist[k]) {
                // priorindex->unique |= index->unique;
                let index_unique = as_index(&index).unique;
                let index_idxname = match &as_index(&index).idxname {
                    Some(s) => Some(s.clone_in(mcx)?),
                    None => None,
                };
                if let Node::IndexStmt(prior) = finalindexlist[k].as_mut() {
                    prior.unique |= index_unique;
                    // Transfer the name to the prior index if it's unnamed.
                    if prior.idxname.is_none() {
                        prior.idxname = index_idxname;
                    }
                }
                keep = false;
                break;
            }
        }

        if keep {
            finalindexlist.push(index);
        }
    }

    // Now append all the IndexStmts to cxt->alist.
    cxt.alist.extend(finalindexlist);
    Ok(())
}

fn as_index<'a, 'mcx>(n: &'a NodePtr<'mcx>) -> &'a IndexStmt<'mcx> {
    match n.as_ref() {
        Node::IndexStmt(i) => i,
        _ => unreachable!("transformIndexConstraints: expected IndexStmt"),
    }
}

/// `index == cxt->pkey` — in C this is pointer identity. We model the pkey as a
/// separate node; the pkey index is the first element of finalindexlist and is
/// skipped from the dedup loop. Since the pkey was produced by the same
/// `transformIndexConstraint` call that filled `indexlist`, the unique
/// PRIMARY-KEY index in `indexlist` is the one whose `primary` flag is set and
/// equals the pkey definition.
fn index_equals_pkey(index: &NodePtr<'_>, cxt: &CreateStmtContext<'_>) -> bool {
    match (index.as_ref(), cxt.pkey.as_deref()) {
        (Node::IndexStmt(i), Some(Node::IndexStmt(_pk))) => i.primary,
        _ => false,
    }
}

/// The C `equal(...)` cluster comparing two `IndexStmt`s for redundancy.
fn indexes_equivalent(index: &NodePtr<'_>, prior: &NodePtr<'_>) -> bool {
    let (i, p) = match (index.as_ref(), prior.as_ref()) {
        (Node::IndexStmt(i), Node::IndexStmt(p)) => (i, p),
        _ => return false,
    };
    equal_node_vec(&i.indexParams, &p.indexParams)
        && equal_node_vec(&i.indexIncludingParams, &p.indexIncludingParams)
        && equal_opt_node(&i.whereClause, &p.whereClause)
        && equal_node_vec(&i.excludeOpNames, &p.excludeOpNames)
        && opt_str_eq(&i.accessMethod, &p.accessMethod)
        && i.nulls_not_distinct == p.nulls_not_distinct
        && i.deferrable == p.deferrable
        && i.initdeferred == p.initdeferred
}

fn equal_node_vec(a: &PgVec<'_, NodePtr<'_>>, b: &PgVec<'_, NodePtr<'_>>) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| equal_node(x.as_ref(), y.as_ref()))
}

fn equal_opt_node(a: &Option<NodePtr<'_>>, b: &Option<NodePtr<'_>>) -> bool {
    match (a.as_deref(), b.as_deref()) {
        (None, None) => true,
        (Some(x), Some(y)) => equal_node(x, y),
        _ => false,
    }
}

fn opt_str_eq(a: &Option<PgString<'_>>, b: &Option<PgString<'_>>) -> bool {
    a.as_ref().map(PgString::as_str) == b.as_ref().map(PgString::as_str)
}

/// `transformIndexConstraint` — transform one UNIQUE / PRIMARY KEY / EXCLUDE
/// constraint into an `IndexStmt`. The skeleton fields are filled here; the
/// catalog-resident column resolution (USING INDEX, inhRelations, WITHOUT
/// OVERLAPS, SystemAttribute*) lives behind the outward seam, which also
/// appends any PRIMARY-KEY-implied not-null constraints.
fn transformIndexConstraint<'mcx>(
    constraint: NodePtr<'mcx>,
    cxt: &mut CreateStmtContext<'mcx>,
) -> PgResult<NodePtr<'mcx>> {
    let mcx = cxt.mcx;

    let (contype, conname, location, nulls_not_distinct, without_overlaps, deferrable, initdeferred,
         options, indexspace, where_clause, access_method, reset_default_tblspc) =
        match constraint.as_ref() {
            Node::Constraint(c) => (
                c.contype,
                opt_clone(mcx, &c.conname)?,
                c.location,
                c.nulls_not_distinct,
                c.without_overlaps,
                c.deferrable,
                c.initdeferred,
                clone_vec(mcx, &c.options)?,
                opt_clone(mcx, &c.indexspace)?,
                opt_clone_node(mcx, &c.where_clause)?,
                opt_clone(mcx, &c.access_method)?,
                c.reset_default_tblspc,
            ),
            other => unreachable!("transformIndexConstraint: not a Constraint: {}", other.node_tag()),
        };

    let primary = contype == CONSTR_PRIMARY;
    if primary {
        if cxt.pkey.is_some() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(alloc::format!(
                    "multiple primary keys for table \"{}\" are not allowed",
                    cxt.relname()
                ))
                .errposition(parser_errposition(&cxt.pstate, location))
                .into_error());
        }
    }

    let access_method = match access_method {
        Some(s) => Some(s),
        None => Some(PgString::from_str_in(DEFAULT_INDEX_TYPE, mcx)?),
    };

    let index = IndexStmt {
        idxname: conname,
        relation: clone_relation_opt(cxt)?,
        accessMethod: access_method,
        tableSpace: indexspace,
        indexParams: PgVec::new_in(mcx),
        indexIncludingParams: PgVec::new_in(mcx),
        options,
        whereClause: where_clause,
        excludeOpNames: PgVec::new_in(mcx),
        idxcomment: None,
        indexOid: 0,
        oldNumber: 0,
        oldCreateSubid: 0,
        oldFirstRelfilelocatorSubid: 0,
        unique: contype != CONSTR_EXCLUSION,
        nulls_not_distinct,
        primary,
        isconstraint: true,
        iswithoutoverlaps: without_overlaps,
        deferrable,
        initdeferred,
        transformed: false,
        concurrent: false,
        if_not_exists: false,
        reset_default_tblspc,
    };
    let index = mcx::alloc_in(mcx, Node::IndexStmt(index))?;

    if primary {
        // cxt->pkey = index; record a clone so finalindexlist owns the live copy.
        cxt.pkey = Some(mcx::alloc_in(mcx, index.clone_in(mcx)?)?);
    }

    // The remaining work — USING INDEX validity checks, breaking apart the
    // EXCLUDE pairs, resolving UNIQUE/PRIMARY key column names against
    // cxt.columns / system attributes / inherited relations, the WITHOUT
    // OVERLAPS range-type check, and adding PRIMARY-KEY-implied not-null
    // constraints — needs the relcache / syscache / parse_type. Delegate it,
    // marshalling the accumulator state across the seam.
    let relation = clone_relation(cxt)?;
    let columns = clone_vec(mcx, &cxt.columns)?;
    let inh_relations = clone_vec(mcx, &cxt.inhRelations)?;
    let (index, extra_nn) = sx::transformIndexConstraintCatalog::call(
        mcx,
        &cxt.pstate,
        constraint,
        index,
        relation,
        cxt.rel_oid,
        cxt.isalter,
        columns,
        inh_relations,
    )?;
    cxt.nnconstraints.extend(extra_nn);

    Ok(index)
}

fn opt_clone<'mcx>(mcx: Mcx<'mcx>, s: &Option<PgString<'_>>) -> PgResult<Option<PgString<'mcx>>> {
    match s {
        Some(s) => Ok(Some(s.clone_in(mcx)?)),
        None => Ok(None),
    }
}

fn opt_clone_node<'mcx>(
    mcx: Mcx<'mcx>,
    n: &Option<NodePtr<'_>>,
) -> PgResult<Option<NodePtr<'mcx>>> {
    match n.as_deref() {
        Some(n) => Ok(Some(mcx::alloc_in(mcx, n.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

fn clone_vec<'mcx>(
    mcx: Mcx<'mcx>,
    v: &PgVec<'_, NodePtr<'_>>,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut out: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    for n in v.iter() {
        out.push(mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
    }
    Ok(out)
}

fn clone_relation<'mcx>(cxt: &CreateStmtContext<'mcx>) -> PgResult<NodePtr<'mcx>> {
    let mcx = cxt.mcx;
    match cxt.relation.as_deref() {
        Some(n) => mcx::alloc_in(mcx, n.clone_in(mcx)?),
        None => Err(types_error::PgError::error(
            "transformIndexConstraint requires cxt.relation",
        )),
    }
}

fn clone_relation_opt<'mcx>(cxt: &CreateStmtContext<'mcx>) -> PgResult<Option<NodePtr<'mcx>>> {
    let mcx = cxt.mcx;
    match cxt.relation.as_deref() {
        Some(n) => Ok(Some(mcx::alloc_in(mcx, n.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}
