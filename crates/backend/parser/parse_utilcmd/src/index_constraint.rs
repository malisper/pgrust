//! `transformIndexConstraints` + `transformIndexConstraint` (`parse_utilcmd.c`).
//!
//! `transformIndexConstraints` (the index-redundancy dedup over `equal()`) and
//! the `IndexStmt`-skeleton construction in `transformIndexConstraint` are
//! node-independent and ported 1:1. The catalog-resident leaf of
//! `transformIndexConstraint` — the ALTER TABLE ADD CONSTRAINT USING INDEX path,
//! the inherited-table column search, the WITHOUT OVERLAPS type check, the
//! `SystemAttributeByName` lookups, and the PRIMARY-KEY-implied not-null
//! additions — crosses the outward seam.

use alloc::string::ToString;

use mcx::{Mcx, PgString, PgVec};

use equalfuncs::equal_node;
use utils_error::ereport;
use types_error::{
    PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DUPLICATE_COLUMN, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_COLUMN, ERRCODE_UNDEFINED_OBJECT,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};

use nodes::ddlnodes::{IndexElem, IndexStmt, CONSTR_EXCLUSION, CONSTR_PRIMARY};
use nodes::nodes::{ntag, Node};
use nodes::parsestmt::ParseState;
use nodes::rawnodes::{SORTBY_DEFAULT, SORTBY_NULLS_DEFAULT};
use types_core::primitive::{InvalidOid, OidIsValid};
use types_core::catalog::BTREE_AM_OID;
use types_core::Oid;

use types_storage::lock::{AccessShareLock, NoLock};
use common_relation::{relation_open, relation_openrv};
use types_tuple::{RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};

use plancat_ext_seams as plancat_ext;
use parse_utilcmd_outward_seams as sx;

use crate::column::make_not_null_constraint;
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
                if let Some(prior) = finalindexlist[k].as_indexstmt_mut() {
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
    match n.as_indexstmt() {
        Some(i) => i,
        None => unreachable!("transformIndexConstraints: expected IndexStmt"),
    }
}

/// `index == cxt->pkey` — in C this is pointer identity. We model the pkey as a
/// separate node; the pkey index is the first element of finalindexlist and is
/// skipped from the dedup loop. Since the pkey was produced by the same
/// `transformIndexConstraint` call that filled `indexlist`, the unique
/// PRIMARY-KEY index in `indexlist` is the one whose `primary` flag is set and
/// equals the pkey definition.
fn index_equals_pkey(index: &NodePtr<'_>, cxt: &CreateStmtContext<'_>) -> bool {
    match (index.as_ref().as_indexstmt(), cxt.pkey.as_deref().and_then(|n| n.as_indexstmt())) {
        (Some(i), Some(_pk)) => i.primary,
        _ => false,
    }
}

/// The C `equal(...)` cluster comparing two `IndexStmt`s for redundancy.
fn indexes_equivalent(index: &NodePtr<'_>, prior: &NodePtr<'_>) -> bool {
    let (i, p) = match (index.as_ref().as_indexstmt(), prior.as_ref().as_indexstmt()) {
        (Some(i), Some(p)) => (i, p),
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
        match constraint.node_tag() {
            ntag::T_Constraint => {
                let c = constraint.expect_constraint();
                (
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
                )
            }
            _ => unreachable!("transformIndexConstraint: not a Constraint: {}", constraint.node_tag()),
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
    let index = mcx::alloc_in(mcx, Node::mk_index_stmt(mcx, index)?)?;

    // The remaining work — USING INDEX validity checks, breaking apart the
    // EXCLUDE pairs, resolving UNIQUE/PRIMARY key column names against
    // cxt.columns / system attributes / inherited relations, the WITHOUT
    // OVERLAPS range-type check, and adding PRIMARY-KEY-implied not-null
    // constraints — needs the relcache / syscache / parse_type. Delegate it,
    // marshalling the accumulator state across the seam.
    let relation = clone_relation(cxt)?;
    let columns = clone_vec(mcx, &cxt.columns)?;
    let inh_relations = clone_vec(mcx, &cxt.inhRelations)?;
    let existing_nn = clone_vec(mcx, &cxt.nnconstraints)?;
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
        existing_nn,
    )?;
    cxt.nnconstraints.extend(extra_nn);

    // C sets `cxt->pkey = index;` by pointer before the column-resolution work,
    // so the resolved indexParams stay visible through cxt->pkey. We model pkey
    // as a separate owned node, so the clone must be taken *after* the catalog
    // seam has filled the index's indexParams (taking it before — as the
    // skeleton — would record an empty-keyed pkey, which then wins the
    // redundancy dedup and reaches DefineIndex with no columns).
    if primary {
        cxt.pkey = Some(mcx::alloc_in(mcx, index.clone_in(mcx)?)?);
    }

    Ok(index)
}

/// `strVal(lfirst(lc))` — read the string payload of a `String` value node.
fn str_val<'a>(n: &'a NodePtr<'_>) -> &'a str {
    match n.node_tag() {
        ntag::T_String => n.expect_string().sval.as_str(),
        _ => unreachable!("expected String value node, got {}", n.node_tag()),
    }
}

/// A zeroed `IndexElem` whose only set field is `name` (the simple
/// column-name index element makeNode(IndexElem) builds in
/// `transformIndexConstraint`).
fn make_index_elem<'mcx>(mcx: Mcx<'mcx>, key: &str) -> PgResult<NodePtr<'mcx>> {
    mcx::alloc_in(
        mcx,
        Node::mk_index_elem(mcx, IndexElem {
            name: Some(PgString::from_str_in(key, mcx)?),
            expr: None,
            indexcolname: None,
            collation: PgVec::new_in(mcx),
            opclass: PgVec::new_in(mcx),
            opclassopts: PgVec::new_in(mcx),
            ordering: SORTBY_DEFAULT,
            nulls_ordering: SORTBY_NULLS_DEFAULT,
        })?,
    )
}

/// The catalog-resident leaf of `transformIndexConstraint` (`parse_utilcmd.c`),
/// installed behind [`sx::transformIndexConstraintCatalog`]. Given the partly
/// built `IndexStmt`, the source `Constraint`, the table's `RangeVar`, the
/// column / inherited-relation accumulators, and whether this is ALTER TABLE,
/// it finishes the index definition (filling `indexParams` /
/// `indexIncludingParams` / `excludeOpNames`) and returns the finished
/// `IndexStmt` together with any PRIMARY-KEY-implied not-null constraints.
#[allow(clippy::too_many_arguments)]
pub fn transform_index_constraint_catalog<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'mcx>,
    mut constraint: NodePtr<'mcx>,
    mut index: NodePtr<'mcx>,
    _relation: NodePtr<'mcx>,
    rel_oid: Oid,
    isalter: bool,
    mut columns: PgVec<'mcx, NodePtr<'mcx>>,
    inh_relations: PgVec<'mcx, NodePtr<'mcx>>,
    existing_nn: PgVec<'mcx, NodePtr<'mcx>>,
) -> PgResult<(NodePtr<'mcx>, PgVec<'mcx, NodePtr<'mcx>>)> {
    // PRIMARY-KEY-implied not-null constraints accumulate here and are returned
    // to the caller (which appends them to cxt->nnconstraints).
    let mut extra_nn: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);

    let is_primary = match index.as_indexstmt() {
        Some(i) => i.primary,
        None => unreachable!("transformIndexConstraintCatalog: index is not an IndexStmt"),
    };

    // ALTER TABLE ADD CONSTRAINT ... USING INDEX: look up the existing index and
    // verify it, copying its key/INCLUDE columns into constraint->keys /
    // ->including. This block mutates the source Constraint, so it runs before
    // the immutable `con` borrow below is taken.
    {
        let (indexname, location, contype) = {
            let con = constraint.expect_constraint();
            (
                con.indexname.as_ref().map(|s| s.as_str().to_string()),
                con.location,
                con.contype,
            )
        };

        if let Some(index_name) = indexname {
            // Grammar should only allow PRIMARY and UNIQUE constraints, and no
            // explicit column list (constraint->keys == NIL).
            debug_assert!(contype == CONSTR_PRIMARY || contype == nodes::ddlnodes::CONSTR_UNIQUE);

            // Must be ALTER, not CREATE, but grammar doesn't enforce that.
            if !isalter {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("cannot use an existing index in CREATE TABLE")
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }

            // Open the heap relation (already locked by the ALTER) to read its
            // namespace and TupleDesc.
            let heap_rel = relation_open(mcx, rel_oid, NoLock)?;
            let heap_namespace = heap_rel.rd_rel.relnamespace;

            // Look for the index in the same schema as the table.
            let index_oid = lsyscache::relation::get_relname_relid(&index_name, heap_namespace)?;
            if !OidIsValid(index_oid) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(alloc::format!("index \"{index_name}\" does not exist"))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }

            // Open the index (this throws if it is not an index).
            let index_rel = relation_open(mcx, index_oid, AccessShareLock)?;
            let index_form = match &index_rel.rd_index {
                Some(f) => f,
                None => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                        .errmsg(alloc::format!("\"{index_name}\" is not an index"))
                        .errposition(parser_errposition(pstate, location))
                        .into_error());
                }
            };

            // Check that it does not have an associated constraint already.
            if OidIsValid(pg_depend_seams::get_index_constraint::call(index_oid)?) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(alloc::format!(
                        "index \"{index_name}\" is already associated with a constraint"
                    ))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }

            // Perform validity checks on the index.
            if index_form.indrelid != rel_oid {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(alloc::format!(
                        "index \"{}\" does not belong to table \"{}\"",
                        index_name,
                        heap_rel.name()
                    ))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }

            if !index_form.indisvalid {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(alloc::format!("index \"{index_name}\" is not valid"))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }

            // Today we forbid non-unique indexes.
            if !index_form.indisunique {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(alloc::format!("\"{index_name}\" is not a unique index"))
                    .errdetail(
                        "Cannot create a primary key or unique constraint using such an index."
                            .to_string(),
                    )
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }

            // BuildIndexInfo gives the expression / predicate detection and the
            // per-column heap attribute numbers (ii_IndexAttrNumbers == indkey).
            let index_info =
                index_seams::build_index_info::call(mcx, &index_rel)?;

            if index_info.ii_Expressions.as_ref().is_some_and(|e| !e.is_empty()) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(alloc::format!("index \"{index_name}\" contains expressions"))
                    .errdetail(
                        "Cannot create a primary key or unique constraint using such an index."
                            .to_string(),
                    )
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }

            if index_info.ii_Predicate.as_ref().is_some_and(|p| !p.is_empty()) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(alloc::format!("\"{index_name}\" is a partial index"))
                    .errdetail(
                        "Cannot create a primary key or unique constraint using such an index."
                            .to_string(),
                    )
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }

            // It's probably unsafe to change a deferred index to non-deferred.
            // (A non-constraint index couldn't be deferred anyway, so this case
            // should never occur; no need to sweat, but let's check it.)
            let con_deferrable = constraint.expect_constraint().deferrable;
            if !index_form.indimmediate && !con_deferrable {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(alloc::format!("\"{index_name}\" is a deferrable index"))
                    .errdetail(
                        "Cannot create a non-deferrable constraint using a deferrable index."
                            .to_string(),
                    )
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }

            // Insist on it being a btree. We must have an index that exactly
            // matches what you'd get from plain ADD CONSTRAINT syntax, else dump
            // and reload will produce a different index (breaking pg_upgrade in
            // particular). get_index_am_oid(DEFAULT_INDEX_TYPE, false) is BTREE_AM_OID.
            if index_rel.rd_rel.relam != BTREE_AM_OID {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(alloc::format!("index \"{index_name}\" is not a btree"))
                    .errposition(parser_errposition(pstate, location))
                    .into_error());
            }

            let indnatts = index_form.indnatts as usize;
            let indnkeyatts = index_form.indnkeyatts as usize;

            // Accumulate the new constraint->keys / ->including String nodes plus
            // any PRIMARY-KEY-implied not-null constraints.
            let mut new_keys: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
            let mut new_including: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);

            for i in 0..indnatts {
                let attnum = index_info.ii_IndexAttrNumbers[i];

                // We shouldn't see attnum == 0 here (expression indexes already
                // rejected). Resolve the heap attribute: typid / collation from
                // the TupleDesc (attnum > 0) or SystemAttributeDefinition (< 0).
                let (atttypid, attcollation) = if attnum > 0 {
                    debug_assert!((attnum as usize) <= heap_rel.rd_att.natts as usize);
                    let att = heap_rel.rd_att.attr((attnum - 1) as usize);
                    (att.atttypid, att.attcollation)
                } else {
                    let (typid, _typmod, coll) =
                        plancat_ext::system_attribute_definition::call(attnum as i32)?;
                    (typid, coll)
                };
                let attname = lsyscache::attribute::get_attname(mcx, rel_oid, attnum, false)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();

                if i < indnkeyatts {
                    // Insist on default opclass, collation, and sort options.
                    // C: `attoptions != (Datum) 0`. `get_attoptions` returns
                    // `Ok(None)` when the pg_attribute.attoptions attr is SQL NULL
                    // (no per-column options), `Ok(Some(_))` when set.
                    let has_attoptions =
                        lsyscache::attribute::get_attoptions(mcx, index_oid, (i + 1) as i16)?
                            .is_some();
                    let defopclass = lsyscache_seams::get_default_opclass::call(
                        atttypid,
                        index_rel.rd_rel.relam,
                    )?;
                    let indclass_i = lsyscache::relation::get_index_column_opclass(index_oid, (i + 1) as i32)?;
                    let indcoll_i = index_rel.rd_indcollation.get(i).copied().unwrap_or(InvalidOid);
                    let indoption_i = index_rel.rd_indoption.get(i).copied().unwrap_or(0);

                    if indclass_i != defopclass
                        || attcollation != indcoll_i
                        || has_attoptions
                        || indoption_i != 0
                    {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                            .errmsg(alloc::format!(
                                "index \"{}\" column number {} does not have default sorting behavior",
                                index_name,
                                i + 1
                            ))
                            .errdetail(
                                "Cannot create a primary key or unique constraint using such an index."
                                    .to_string(),
                            )
                            .errposition(parser_errposition(pstate, location))
                            .into_error());
                    }

                    // If a PK, ensure the columns get not null constraints.
                    if contype == CONSTR_PRIMARY {
                        let nn = make_not_null_constraint(mcx, &attname)?;
                        extra_nn.push(mcx::alloc_in(mcx, Node::mk_constraint(mcx, nn)?)?);
                    }

                    new_keys.push(mcx::alloc_in(
                        mcx,
                        Node::mk_string(mcx, nodes::value::StringNode {
                            sval: PgString::from_str_in(&attname, mcx)?,
                        })?,
                    )?);
                } else {
                    new_including.push(mcx::alloc_in(
                        mcx,
                        Node::mk_string(mcx, nodes::value::StringNode {
                            sval: PgString::from_str_in(&attname, mcx)?,
                        })?,
                    )?);
                }
            }

            // Close the index relation but keep the lock (NoLock close releases
            // only the relcache reference; the AccessShareLock is kept).
            index_rel.close(NoLock)?;
            heap_rel.close(NoLock)?;

            // Scribble the resolved keys / including onto the source constraint,
            // and set index->indexOid.
            if let Some(c) = constraint.as_constraint_mut() {
                c.keys = new_keys;
                c.including = new_including;
            }
            if let Some(i) = index.as_indexstmt_mut() {
                i.indexOid = index_oid;
            }
        }
    }

    let con = match constraint.node_tag() {
        ntag::T_Constraint => constraint.expect_constraint(),
        _ => unreachable!("transformIndexConstraintCatalog: not a Constraint: {}", constraint.node_tag()),
    };
    let contype = con.contype;

    // EXCLUDE: break the (IndexElem, opname) pairs apart.
    if contype == CONSTR_EXCLUSION {
        for pair in con.exclusions.iter() {
            let (elem, opname) = match pair.as_ref().as_list() {
                Some(items) if items.len() == 2 => (
                    mcx::alloc_in(mcx, items[0].clone_in(mcx)?)?,
                    mcx::alloc_in(mcx, items[1].clone_in(mcx)?)?,
                ),
                _ => unreachable!("EXCLUDE pair is not a 2-element List"),
            };
            if let Some(i) = index.as_indexstmt_mut() {
                i.indexParams.push(elem);
                i.excludeOpNames.push(opname);
            }
        }
    } else {
        // UNIQUE / PRIMARY KEY: a list of column names.
        let n_keys = con.keys.len();
        for (kidx, key_node) in con.keys.iter().enumerate() {
            let key = str_val(key_node);
            let mut found = false;
            #[allow(unused_assignments)]
            let mut key_typid = InvalidOid;
            let mut col_idx: Option<usize> = None;

            for (i, c) in columns.iter().enumerate() {
                if let Some(col) = c.as_columndef() {
                    if col.colname.as_ref().map(PgString::as_str) == Some(key) {
                        found = true;
                        col_idx = Some(i);
                        break;
                    }
                }
            }

            if found {
                let ci = col_idx.unwrap();
                if contype == CONSTR_PRIMARY && !isalter {
                    let is_not_null = match columns[ci].as_columndef() {
                        Some(col) => col.is_not_null,
                        None => false,
                    };
                    if is_not_null {
                        // Verify any existing not-null constraint isn't NO INHERIT.
                        // C scans the whole cxt->nnconstraints list: both the
                        // pre-existing constraints (existing_nn, which carries any
                        // table-level `NOT NULL ... NO INHERIT`) and the
                        // PRIMARY-KEY-implied ones added earlier in this loop
                        // (extra_nn). Stop at the first match for this column.
                        for nn in existing_nn.iter().chain(extra_nn.iter()) {
                            if let Some(nnc) = nn.as_constraint() {
                                if nnc.keys.first().map(|k| str_val(k)) == Some(key) {
                                    if nnc.is_no_inherit {
                                        return Err(ereport(ERROR)
                                            .errcode(ERRCODE_SYNTAX_ERROR)
                                            .errmsg(alloc::format!(
                                                "conflicting NO INHERIT declaration for not-null constraint on column \"{}\"",
                                                key
                                            ))
                                            .into_error());
                                    }
                                    break;
                                }
                            }
                        }
                    } else {
                        if let Some(col) = columns[ci].as_columndef_mut() {
                            col.is_not_null = true;
                        }
                        let nn = make_not_null_constraint(mcx, key)?;
                        extra_nn.push(mcx::alloc_in(mcx, Node::mk_constraint(mcx, nn)?)?);
                    }
                }
                // (contype == PRIMARY && isalter) — Assert(column->is_not_null),
                // already handled by ATPrepAddPrimaryKey; nothing to do.
            } else if plancat_ext::system_attribute_by_name::call(key)?.is_some() {
                // A system column in the new table; accept it (never null).
                found = true;
            } else if !inh_relations.is_empty() {
                // Inherited tables: search each parent's TupleDesc for the key
                // column (table_openrv + RelationGetDescr per parent). On a
                // match, a PRIMARY KEY also adds a NOT NULL constraint for the
                // inherited column, and we capture the column's type OID for the
                // WITHOUT OVERLAPS check.
                let (inh_found, inh_typid) =
                    find_inherited_key_column(mcx, &inh_relations, key)?;
                if inh_found {
                    found = true;
                    key_typid = inh_typid;
                    if is_primary {
                        let nn = make_not_null_constraint(mcx, key)?;
                        extra_nn.push(mcx::alloc_in(mcx, Node::mk_constraint(mcx, nn)?)?);
                    }
                }
            }

            if !found && !isalter {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(alloc::format!("column \"{}\" named in key does not exist", key))
                    .errposition(parser_errposition(pstate, con.location))
                    .into_error());
            }

            // Check for PRIMARY KEY(foo, foo).
            if let Some(i) = index.as_ref().as_indexstmt() {
                for iparam in i.indexParams.iter() {
                    if let Some(e) = iparam.as_ref().as_indexelem() {
                        if e.name.as_ref().map(PgString::as_str) == Some(key) {
                            let code = ERRCODE_DUPLICATE_COLUMN;
                            let msg = if is_primary {
                                alloc::format!("column \"{}\" appears twice in primary key constraint", key)
                            } else {
                                alloc::format!("column \"{}\" appears twice in unique constraint", key)
                            };
                            return Err(ereport(ERROR)
                                .errcode(code)
                                .errmsg(msg)
                                .errposition(parser_errposition(pstate, con.location))
                                .into_error());
                        }
                    }
                }
            }

            // WITHOUT OVERLAPS: the last key must be a range/multirange type.
            if con.without_overlaps && kidx == n_keys - 1 {
                if !found && isalter {
                    // Look up the column type on the existing table. If we can't
                    // find it, let things fail in DefineIndex. (The ALTER already
                    // holds a lock on the heap; NoLock just gets the relcache
                    // reference.)
                    let rel = relation_open(mcx, rel_oid, NoLock)?;
                    let natts = rel.rd_att.natts as usize;
                    for i in 0..natts {
                        let attr = rel.rd_att.attr(i);
                        // C breaks (not continues) on the first dropped column.
                        if attr.attisdropped {
                            break;
                        }
                        if attr.attname.name_str() == key.as_bytes() {
                            found = true;
                            key_typid = attr.atttypid;
                            break;
                        }
                    }
                    rel.close(NoLock)?;
                }
                if found {
                    // typid may already be set from an inherited parent's
                    // attribute (or the ALTER scan above). Otherwise resolve the
                    // new column's declared TypeName: typenameTypeId(NULL, ...).
                    if !OidIsValid(key_typid) {
                        if let Some(ci) = col_idx {
                            if let Some(col) = columns[ci].as_columndef() {
                                if let Some(type_name) = col.typeName.as_deref() {
                                    let tn = crate::coltype::raw_typename_to_parse(type_name)?;
                                    key_typid =
                                        parse_type::typenameTypeId(mcx, None, &tn)?;
                                }
                            }
                        }
                    }

                    let is_range = OidIsValid(key_typid)
                        && lsyscache::type_::type_is_range(key_typid)?;
                    let is_multirange = OidIsValid(key_typid)
                        && lsyscache::type_::type_is_multirange(key_typid)?;
                    if !OidIsValid(key_typid) || !(is_range || is_multirange) {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_DATATYPE_MISMATCH)
                            .errmsg(alloc::format!(
                                "column \"{}\" in WITHOUT OVERLAPS is not a range or multirange type",
                                key
                            ))
                            .errposition(parser_errposition(pstate, con.location))
                            .into_error());
                    }
                }
            }

            let iparam = make_index_elem(mcx, key)?;
            if let Some(i) = index.as_indexstmt_mut() {
                i.indexParams.push(iparam);
            }
        }

        if con.without_overlaps {
            if con.keys.len() < 2 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg("constraint using WITHOUT OVERLAPS needs at least two columns")
                    .into_error());
            }
            if let Some(i) = index.as_indexstmt_mut() {
                i.accessMethod = Some(PgString::from_str_in("gist", mcx)?);
            }
        }
    }

    // Add included columns (INCLUDE list). Like the simple-column path above,
    // but no NOT NULL marking and no duplicate-column complaint.
    for key_node in con.including.iter() {
        let key = str_val(key_node);
        let mut found = false;

        for c in columns.iter() {
            if let Some(col) = c.as_columndef() {
                if col.colname.as_ref().map(PgString::as_str) == Some(key) {
                    found = true;
                    break;
                }
            }
        }

        if !found {
            if plancat_ext::system_attribute_by_name::call(key)?.is_some() {
                found = true;
            } else if !inh_relations.is_empty() {
                // INCLUDE column resolved only via an inherited parent: search
                // each parent's TupleDesc, like the key path above. No NOT NULL
                // marking and no duplicate-column complaint for INCLUDE columns.
                let (inh_found, _inh_typid) =
                    find_inherited_key_column(mcx, &inh_relations, key)?;
                if inh_found {
                    found = true;
                }
            }
        }

        if !found && !isalter {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(alloc::format!("column \"{}\" named in key does not exist", key))
                .errposition(parser_errposition(pstate, con.location))
                .into_error());
        }

        let iparam = make_index_elem(mcx, key)?;
        if let Some(i) = index.as_indexstmt_mut() {
            i.indexIncludingParams.push(iparam);
        }
    }

    // `columns` may have been mutated (is_not_null); the caller's accumulator
    // effect flows through `extra_nn`, so dropping the local copy is correct.
    drop(columns);

    Ok((index, extra_nn))
}

/// Search the inherited parent relations for a key/INCLUDE column named `key`.
///
/// Mirrors the `else if (cxt->inhRelations)` branch of `transformIndexConstraint`
/// (`parse_utilcmd.c`): for each parent `RangeVar` it does `table_openrv` with
/// `AccessShareLock`, checks the relkind, then walks the parent's `TupleDesc`
/// looking for a non-dropped attribute whose name equals `key`. On a match it
/// records `found = true` and captures the column's `atttypid` (used by the
/// caller for the WITHOUT OVERLAPS range-type check).
///
/// Returns `(found, typid)`.
fn find_inherited_key_column<'mcx>(
    mcx: Mcx<'mcx>,
    inh_relations: &PgVec<'mcx, NodePtr<'mcx>>,
    key: &str,
) -> PgResult<(bool, Oid)> {
    let mut found = false;
    let mut typid = InvalidOid;

    for inh_node in inh_relations.iter() {
        let inh = inh_node.as_ref().as_rangevar().unwrap_or_else(|| {
            unreachable!(
                "transformIndexConstraint: inhRelations entry is not a RangeVar: {}",
                inh_node.as_ref().node_tag()
            )
        });
        let access_rv = crate::like::access_range_var(inh);

        let rel = relation_openrv(mcx, &access_rv, AccessShareLock)?;

        // Check user requested inheritance from valid relkind.
        let relkind = rel.rd_rel.relkind;
        if relkind != RELKIND_RELATION
            && relkind != RELKIND_FOREIGN_TABLE
            && relkind != RELKIND_PARTITIONED_TABLE
        {
            let relname = inh.relname.as_ref().map(PgString::as_str).unwrap_or("");
            rel.close(NoLock)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(alloc::format!(
                    "inherited relation \"{}\" is not a table or foreign table",
                    relname
                ))
                .into_error());
        }

        let natts = rel.rd_att.natts as usize;
        for count in 0..natts {
            let inhattr = rel.rd_att.attr(count);
            if inhattr.attisdropped {
                continue;
            }
            if inhattr.attname.name_str() == key.as_bytes() {
                found = true;
                typid = inhattr.atttypid;
                break;
            }
        }

        // table_close(rel, NoLock): release the relcache reference but keep the
        // AccessShareLock for the duration of the transaction.
        rel.close(NoLock)?;

        if found {
            break;
        }
    }

    Ok((found, typid))
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
