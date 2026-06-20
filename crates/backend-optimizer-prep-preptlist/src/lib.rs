//! `backend/optimizer/prep/preptlist.c` — SELECT core of the parse-tree
//! targetlist preprocessor.
//!
//! 1:1 port of PostgreSQL 18.3 `preprocess_targetlist` (SELECT path) plus the
//! standalone `get_plan_rowmark` lookup, over this repo's lifetime-free owned
//! `Query<'mcx>` model and the `PlannerInfo` arena handle world.
//!
//! ## What this unit is
//!
//! This crate is the new owner of `optimizer/prep/preptlist.c`. preptlist
//! preprocesses `root->parse->targetList` into `root->processed_tlist`. It owns:
//!
//! * `preprocess_targetlist` — the driver, called from `grouping_planner`
//!   (planner.c). Declared as the inward seam
//!   [`backend_optimizer_prep_preptlist_seams::preprocess_targetlist`] and
//!   installed by [`init_seams`]. SELECT core + the **INSERT leg**
//!   (`expand_insert_targetlist`: open the target relation, fill missing
//!   columns with NULL `Const`s in attribute order) are ported. The
//!   UPDATE/DELETE/MERGE row-identity junk-column stanza, the MERGE per-action
//!   handling, the FOR-UPDATE/SHARE rowMarks junk stanza, and the
//!   cross-relation RETURNING junk stanza seam-and-panic until the DML-analyze
//!   family + the PlanRowMark-carrier keystone land.
//! * `extract_update_targetlist_colnos` — the UPDATE colno extractor, a plain
//!   `pub fn` (its only caller is `preprocess_targetlist`'s UPDATE leg, in this
//!   crate, and INSERT...ON CONFLICT in nodeModifyTable's planning, same layer).
//! * `get_plan_rowmark` — the `PlanRowMark` lookup, a plain `pub fn`. It backs
//!   the already-declared+consumed cross-unit seam
//!   `backend_optimizer_util_restrictinfo_seams::has_plan_rowmark` (used by
//!   indxpath `check_index_predicates`), installed here by [`init_seams`].
//!
//! ## Model notes
//!
//! * The C `preprocess_targetlist(PlannerInfo *root)` reads `root->parse` (the
//!   top `Query`) and writes `root->processed_tlist`/`root->update_colnos`.
//!   `PlannerInfo` is lifetime-free here and the top `Query` lives in the
//!   [`PlannerRun`](types_pathnodes::planner_run::PlannerRun) store behind
//!   `root.parse`'s `QueryId`. The planner driver resolves it
//!   (`run.resolve_mut(root.parse)`) and threads the `&mut Query` alongside
//!   `&mut PlannerInfo`; the two are distinct objects so there's no aliasing
//!   conflict. `mcx` is the planner-run context new nodes allocate in.
//! * `root->processed_tlist` is a `List *` of `TargetEntry *` that, in C,
//!   aliases the TLEs of `parse->targetList` (for SELECT, with no INSERT
//!   expansion / no junk additions, it is exactly `parse->targetList`). This
//!   repo carries `processed_tlist` as a `Vec<NodeId>` of arena handles into
//!   `PlannerInfo.node_arena` ([`ArenaNode::TargetEntry`] id-space), so each
//!   resolved `TargetEntry<'mcx>` is **deep-cloned** into the arena via
//!   `TargetEntry::clone_in` / `Expr::clone_in` (keystone #280 — a shallow
//!   `.clone()` panics on `Aggref`/`SubLink`/`SubPlan` children a TLE's expr can
//!   carry) and the resulting handle stored. The clone is the faithful analogue
//!   of the C alias: downstream planner stages read `processed_tlist` through
//!   the arena exactly as C reads the `TargetEntry *` list, and the SELECT path
//!   never mutates the source `parse->targetList` TLEs after this point (the
//!   in-place renumbering only happens on the UPDATE leg, deferred).
//! * The FOR-UPDATE/SHARE rowMarks junk-column stanza walks `root->rowMarks`
//!   (`List *` of `PlanRowMark *`). Here `rowMarks` is `Vec<NodeId>` of opaque
//!   handles with no backing store — `PlanRowMark`s are produced by
//!   `preprocess_rowmarks` (planmain.c, unported), which runs before this pass.
//!   The list is therefore always empty on every currently reachable path; when
//!   it is non-empty we cannot resolve `rc->rti`/`rc->allMarkTypes` to build the
//!   junk Vars, so we seam-and-panic rather than silently skip required columns
//!   (the PlanRowMark-carrier keystone must land first). Same sanctioned
//!   boundary as `remove_useless_result_rtes` (prepjointree FAMILY 5).

#![no_std]
#![allow(non_snake_case)]
// The project-wide error contract is the un-boxed `PgResult`.
#![allow(clippy::result_large_err)]

extern crate alloc;

use types_core::primitive::AttrNumber;
use types_core::{INT4OID, InvalidOid, Oid};
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::nodes::CmdType;
use types_nodes::primnodes::Expr;
use types_pathnodes::{NodeId, PlanRowMarkId, PlannerInfo, TargetEntryNode};
use types_rel::Relation;
use types_tuple::backend_access_common_heaptuple::Datum;

// ===========================================================================
// preprocess_targetlist (preptlist.c:64) — SELECT core
// ===========================================================================

/// `preprocess_targetlist(root)` (preptlist.c:64) — driver for preprocessing
/// the parse-tree targetlist. SELECT path.
///
/// See the crate docs for the carrier model and the DML/rowMarks deferrals.
pub fn preprocess_targetlist<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &mut Query<'mcx>,
    // The FOR-UPDATE/SHARE PlanRowMark values (resolved by the caller from
    // root->rowMarks; PlanRowMark is a Copy value, so passing the resolved slice
    // avoids threading the `&PlannerRun` registry through this owner).
    rowmarks: &[types_nodes::nodelockrows::PlanRowMark],
) -> PgResult<()> {
    let result_relation = parse.resultRelation;
    let command_type = parse.commandType;

    // C 80-94: if there is a result relation, open it (INSERT/UPDATE/DELETE/
    // MERGE) so we can look for missing columns; else Assert(SELECT). Previous
    // code already acquired at least AccessShareLock, so we pass NoLock.
    let target_relation: Option<Relation<'mcx>> = if result_relation != 0 {
        let result_relation = result_relation as usize;
        // C: target_rte = rt_fetch(result_relation, range_table). Sanity-check
        // it is a real relation, else parser/rewriter messed up.
        let target_rte = parse.rtable.get(result_relation - 1).ok_or_else(|| {
            types_error::PgError::error(alloc::format!(
                "preprocess_targetlist: result relation {result_relation} out of range",
            ))
        })?;
        if target_rte.rtekind != types_nodes::parsenodes::RTEKind::RTE_RELATION {
            return Err(types_error::PgError::error(alloc::string::String::from(
                "result relation must be a regular relation",
            )));
        }
        let relid = target_rte.relid;
        Some(backend_access_table_table::table_open(mcx, relid, 0 /* NoLock */)?)
    } else {
        debug_assert!(command_type == CmdType::CMD_SELECT);
        None
    };

    // C 105-117: tlist = parse->targetList; INSERT expands it to the exact
    // attribute order, UPDATE extracts the target colnos.
    let mut tlist: alloc::vec::Vec<NodeId> = match command_type {
        CmdType::CMD_INSERT => {
            let rel = target_relation
                .as_ref()
                .expect("preprocess_targetlist: INSERT with no result relation (rewriter bug)");
            expand_insert_targetlist(mcx, root, parse, rel)?
        }
        CmdType::CMD_UPDATE => {
            // Materialize the parser tlist into the arena, then renumber.
            let materialized = materialize_tlist(mcx, root, parse)?;
            root.update_colnos = extract_update_targetlist_colnos(root, &materialized);
            materialized
        }
        CmdType::CMD_SELECT => materialize_tlist(mcx, root, parse)?,
        CmdType::CMD_DELETE | CmdType::CMD_MERGE => {
            // DELETE has no tlist expansion; MERGE per-action handling is
            // deferred to the MERGE-analyze family.
            if command_type == CmdType::CMD_MERGE {
                panic!(
                    "preprocess_targetlist: MERGE per-action targetlist / join-condition Var \
                     collection not yet ported (needs the MERGE-analyze family)"
                );
            }
            materialize_tlist(mcx, root, parse)?
        }
        other => panic!("preprocess_targetlist: unexpected command type {other:?}"),
    };

    // C 119-132: non-inherited UPDATE/DELETE/MERGE junk row-identity columns.
    // add_row_identity_columns reads root->processed_tlist, so it must be the
    // current tlist; stash it onto root, call, and take it back. (For the
    // non-inherited target the new ctid junk TLE is appended directly.)
    if command_type == CmdType::CMD_UPDATE
        || command_type == CmdType::CMD_DELETE
        || command_type == CmdType::CMD_MERGE
    {
        let rel = target_relation
            .as_ref()
            .expect("preprocess_targetlist: DML with no result relation (rewriter bug)");
        let relkind = rel.rd_rel.relkind;
        let relid = rel.rd_id;
        let has_delete_row_trigger = rel
            .rd_trigdesc
            .as_deref()
            .map(|td| td.trig_delete_after_row || td.trig_delete_before_row)
            .unwrap_or(false);

        root.processed_tlist = core::mem::take(&mut tlist);
        backend_optimizer_util_appendinfo_seams::add_row_identity_columns::call(
            root,
            result_relation as u32,
            command_type,
            relid,
            relkind,
            has_delete_row_trigger,
            result_relation as u32,
        )?;
        tlist = core::mem::take(&mut root.processed_tlist);
    }

    // C 229-287: rowMarks junk-column stanza (FOR UPDATE/SHARE locking +
    // EvalPlanQual). For each PlanRowMark add resjunk Vars the executor's
    // EvalPlanQual / row locking needs (ctid for a TID-fetchable mark; whole-row
    // for a ROW_MARK_COPY mark; tableoid for an inheritance parent).
    const ROW_MARK_COPY: i32 = types_nodes::execnodes::RowMarkType::Copy as u32 as i32;
    for rc in rowmarks {
        // Child rels use the same junk attrs as their parents (C:237-238).
        if rc.rti != rc.prti {
            continue;
        }

        if rc.allMarkTypes & !(1 << ROW_MARK_COPY) != 0 {
            // Need to fetch TID: makeVar(rti, SelfItemPointerAttributeNumber,
            // TIDOID, -1, InvalidOid, 0) labeled "ctid%u" (C:240-254).
            let var = backend_nodes_core::makefuncs::make_var(
                rc.rti as i32,
                types_tuple::heaptuple::SelfItemPointerAttributeNumber,
                types_tuple::heaptuple::TIDOID,
                -1,
                InvalidOid,
                0,
            );
            let resname = alloc::format!("ctid{}", rc.rowmarkId);
            let expr_id = root.alloc_node(types_nodes::primnodes::Expr::Var(var));
            let resno = (tlist.len() + 1) as AttrNumber;
            let te = TargetEntryNode {
                expr: expr_id,
                resno,
                resname: Some(resname),
                ressortgroupref: 0,
                resorigtbl: InvalidOid,
                resorigcol: 0,
                resjunk: true,
            };
            tlist.push(root.alloc_targetentry(te));
        }
        if rc.allMarkTypes & (1 << ROW_MARK_COPY) != 0 {
            // Need the whole row as a junk var (makeWholeRowVar) (C:255-267).
            // Reached for a ROW_MARK_COPY mark (a non-relation / view RTE):
            // makeWholeRowVar(rt_fetch(rc->rti, range_table), rc->rti, 0, false).
            let rte = parse.rtable.get((rc.rti - 1) as usize).ok_or_else(|| {
                types_error::PgError::error(alloc::format!(
                    "preprocess_targetlist: ROW_MARK_COPY rti {} out of range",
                    rc.rti
                ))
            })?;
            let var =
                backend_nodes_core::makefuncs::make_whole_row_var(rte, rc.rti as i32, 0, false)?;
            let resname = alloc::format!("wholerow{}", rc.rowmarkId);
            let expr_id = root.alloc_node(types_nodes::primnodes::Expr::Var(var));
            let resno = (tlist.len() + 1) as AttrNumber;
            let te = TargetEntryNode {
                expr: expr_id,
                resno,
                resname: Some(resname),
                ressortgroupref: 0,
                resorigtbl: InvalidOid,
                resorigcol: 0,
                resjunk: true,
            };
            tlist.push(root.alloc_targetentry(te));
        }

        // If parent of an inheritance tree, always fetch the tableoid too
        // (C:269-285). isParent is false for a non-inherited base relation.
        if rc.isParent {
            panic!(
                "preprocess_targetlist: inheritance-parent rowmark tableoid junk Var \
                 (isParent) not yet ported (needs the inheritance-expansion family)"
            );
        }
    }

    // C 296-325: if the query has a RETURNING list, add resjunk entries for any
    // Vars used in RETURNING that belong to other relations, so they are
    // available for the RETURNING calculation. Vars of the result rel don't need
    // adding (they refer to the actual heap tuple). The whole stanza is skipped
    // unless there is more than one rtable entry, exactly as C.
    if !parse.returningList.is_empty() && parse.rtable.len() > 1 {
        let flags = backend_optimizer_util_vars::PVC_RECURSE_AGGREGATES
            | backend_optimizer_util_vars::PVC_RECURSE_WINDOWFUNCS
            | backend_optimizer_util_vars::PVC_INCLUDE_PLACEHOLDERS;

        // pull_var_clause((Node *) parse->returningList, ...) — run over each
        // RETURNING TargetEntry's expr (equivalent to walking the C List).
        let mut vars: alloc::vec::Vec<types_nodes::primnodes::Expr> = alloc::vec::Vec::new();
        for tle in parse.returningList.iter() {
            if let Some(expr) = tle.expr.as_deref() {
                let node = types_nodes::nodes::Node::mk_expr(mcx, expr.clone_in(mcx)?)?;
                for v in backend_optimizer_util_vars::pull_var_clause(mcx, &node, flags)? {
                    vars.push(v);
                }
            }
        }

        for var in vars.into_iter() {
            // if (IsA(var, Var) && var->varno == result_relation) continue;
            if let Some(v) = var.as_var() {
                if v.varno == result_relation {
                    continue; // don't need it
                }
            }

            // if (tlist_member((Expr *) var, tlist)) continue;
            let already = tlist.iter().any(|&id| {
                let expr_id = root.targetentry(id).expr;
                backend_nodes_equalfuncs_seams::equal_expr::call(&var, root.node(expr_id))
            });
            if already {
                continue; // already got it
            }

            // tle = makeTargetEntry(var, list_length(tlist)+1, NULL, true);
            let expr_id = root.alloc_node(var);
            let resno = (tlist.len() + 1) as AttrNumber;
            let te = TargetEntryNode {
                expr: expr_id,
                resno,
                resname: None,
                ressortgroupref: 0,
                resorigtbl: InvalidOid,
                resorigcol: 0,
                resjunk: true,
            };
            tlist.push(root.alloc_targetentry(te));
        }
    }

    // C 327: root->processed_tlist = tlist.
    root.processed_tlist = tlist;

    // C 329-330: target_relation is closed here in C (table_close(NoLock)); our
    // `Relation` releases on drop.
    drop(target_relation);
    Ok(())
}

/// Materialize `parse.targetList` (owned `TargetEntry<'mcx>`s) into the
/// node_arena, deep-cloning each TLE and its expr tree (the C alias of
/// `parse->targetList`; #280 — a shallow clone panics on Aggref/SubLink/SubPlan
/// children). Returns the resulting `NodeId` handles in order.
fn materialize_tlist<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &Query<'mcx>,
) -> PgResult<alloc::vec::Vec<NodeId>> {
    let mut out: alloc::vec::Vec<NodeId> = alloc::vec::Vec::with_capacity(parse.targetList.len());
    for tle in parse.targetList.iter() {
        let expr_src = tle.expr.as_deref().expect(
            "preprocess_targetlist: TargetEntry with NULL expr in targetList (parser bug)",
        );
        let expr_clone = expr_src.clone_in(mcx)?;
        let expr_id = root.alloc_node(expr_clone);
        let te_id = root.alloc_targetentry(target_entry_node_from(tle, expr_id));
        out.push(te_id);
    }
    Ok(out)
}

/// Build a [`TargetEntryNode`] mirroring an owned [`TargetEntry`], with its expr
/// already allocated into the arena under `expr_id`.
fn target_entry_node_from(
    tle: &types_nodes::primnodes::TargetEntry<'_>,
    expr_id: NodeId,
) -> TargetEntryNode {
    TargetEntryNode {
        expr: expr_id,
        resno: tle.resno,
        resname: tle
            .resname
            .as_ref()
            .map(|s| alloc::string::String::from(s.as_str())),
        ressortgroupref: tle.ressortgroupref,
        resorigtbl: tle.resorigtbl,
        resorigcol: tle.resorigcol,
        resjunk: tle.resjunk,
    }
}

// ===========================================================================
// expand_insert_targetlist (preptlist.c:382)
// ===========================================================================

/// `expand_insert_targetlist(root, tlist, rel)` (preptlist.c:382) — given a
/// parser-generated INSERT targetlist and the result relation, add targetlist
/// entries for any missing attributes and ensure the non-junk attributes appear
/// in proper field order.
///
/// The rewriter has already ordered the supplied TLEs; we scan the relation's
/// tuple descriptor and, for each attribute with no matching non-junk TLE,
/// synthesize a NULL `Const` (the rewriter would have substituted any non-NULL
/// default). Dropped columns get an `INT4` NULL; generated columns a NULL of the
/// base type (no domain constraints, since the value is ignored); normal columns
/// a NULL of the column type via `coerce_null_to_domain` (which applies domain
/// constraints). Remaining resjunk TLEs are appended with renumbered resnos.
fn expand_insert_targetlist<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    parse: &Query<'mcx>,
    rel: &Relation<'mcx>,
) -> PgResult<alloc::vec::Vec<NodeId>> {
    let mut new_tlist: alloc::vec::Vec<NodeId> = alloc::vec::Vec::new();

    // C: scan the tuple descriptor to make sure we have all user attributes in
    // the right order.
    let numattrs = rel.rd_att.natts;
    // Index into the supplied (owned) parser tlist.
    let mut tlist_pos: usize = 0;

    let mut attrno: AttrNumber = 1;
    while (attrno as i32) <= numattrs {
        let att_tup = rel.rd_att.attr((attrno - 1) as usize);

        // Try to consume the next supplied TLE if it matches this attribute.
        let mut matched: Option<NodeId> = None;
        if let Some(old_tle) = parse.targetList.get(tlist_pos) {
            if !old_tle.resjunk && old_tle.resno == attrno {
                let expr_src = old_tle.expr.as_deref().expect(
                    "expand_insert_targetlist: TargetEntry with NULL expr (parser bug)",
                );
                let expr_clone = expr_src.clone_in(mcx)?;
                let expr_id = root.alloc_node(expr_clone);
                let te_id = root.alloc_targetentry(target_entry_node_from(old_tle, expr_id));
                matched = Some(te_id);
                tlist_pos += 1;
            }
        }

        let new_tle = match matched {
            Some(id) => id,
            None => {
                // Didn't find a matching tlist entry; make a NULL one.
                let new_expr: Expr = if att_tup.attisdropped {
                    // Insert NULL for dropped column, labeled INT4.
                    Expr::Const(backend_nodes_core::makefuncs::make_const(
                        mcx,
                        INT4OID,
                        -1,
                        InvalidOid,
                        4, /* sizeof(int32) */
                        Datum::ByVal(0),
                        true, /* isnull */
                        true, /* byval */
                    )?)
                } else if att_tup.attgenerated != 0 {
                    // Generated column: NULL of the base type, no domain
                    // constraints (the value is ignored at execution).
                    let (base_type_id, base_type_mod) =
                        backend_utils_cache_lsyscache_seams::get_base_type_and_typmod::call(
                            att_tup.atttypid,
                        )?;
                    // C seeds `baseTypeMod = att_tup->atttypmod` and overwrites
                    // it only when walking down a domain (the column type was a
                    // domain). The repo seam seeds -1 and returns the base
                    // typmod (-1 for a non-domain), so use the column typmod
                    // when the type is unchanged, else the returned base typmod.
                    let type_mod = if base_type_id == att_tup.atttypid {
                        att_tup.atttypmod
                    } else {
                        base_type_mod
                    };
                    Expr::Const(backend_nodes_core::makefuncs::make_const(
                        mcx,
                        base_type_id,
                        type_mod,
                        att_tup.attcollation,
                        att_tup.attlen as i32,
                        Datum::ByVal(0),
                        true, /* isnull */
                        att_tup.attbyval,
                    )?)
                } else {
                    // Normal column: NULL of the column datatype, applying any
                    // domain constraints (coerce_null_to_domain).
                    backend_parser_coerce_seams::coerce_null_to_domain::call(
                        mcx,
                        att_tup.atttypid,
                        att_tup.atttypmod,
                        att_tup.attcollation,
                        att_tup.attlen as i32,
                        att_tup.attbyval,
                    )?
                    // C: if the result is not a bare Const, run
                    // eval_const_expressions. coerce_null_to_domain returns a
                    // bare Const for non-domain types; the domain case (a
                    // CoerceToDomain node) needs eval_const_expressions, which
                    // routes through the clauses owner when reached.
                };

                let expr_id = root.alloc_node(new_expr);
                let resname =
                    alloc::string::String::from_utf8_lossy(att_tup.attname.name_str())
                        .into_owned();
                let te_node = TargetEntryNode {
                    expr: expr_id,
                    resno: attrno,
                    resname: Some(resname),
                    ressortgroupref: 0,
                    resorigtbl: InvalidOid,
                    resorigcol: 0,
                    resjunk: false,
                };
                root.alloc_targetentry(te_node)
            }
        };

        new_tlist.push(new_tle);
        attrno += 1;
    }

    // C: remaining tlist entries must be resjunk; append them with resnos
    // higher than the last real attribute (renumbering, since we may have
    // inserted NULL entries above).
    while let Some(old_tle) = parse.targetList.get(tlist_pos) {
        if !old_tle.resjunk {
            return Err(types_error::PgError::error(alloc::string::String::from(
                "targetlist is not sorted correctly",
            )));
        }
        let expr_src = old_tle
            .expr
            .as_deref()
            .expect("expand_insert_targetlist: resjunk TLE with NULL expr (parser bug)");
        let expr_clone = expr_src.clone_in(mcx)?;
        let expr_id = root.alloc_node(expr_clone);
        let mut te_node = target_entry_node_from(old_tle, expr_id);
        te_node.resno = attrno;
        new_tlist.push(root.alloc_targetentry(te_node));
        attrno += 1;
        tlist_pos += 1;
    }

    Ok(new_tlist)
}

// ===========================================================================
// extract_update_targetlist_colnos (preptlist.c:347)
// ===========================================================================

/// `extract_update_targetlist_colnos(tlist)` (preptlist.c:347) — extract the
/// target-table column numbers an UPDATE's targetlist assigns to, then renumber
/// the TLEs to the sequential convention.
///
/// The C convention: an UPDATE's non-resjunk TLE `resno` is the target column
/// number; this pulls those into a separate list and rewrites each `resno` to a
/// consecutive 1..n. Operates on the in-arena `TargetEntryNode`s addressed by
/// `tlist` (the resolved UPDATE targetlist's arena handles).
///
/// Only reachable from the UPDATE leg of `preprocess_targetlist` and from
/// INSERT...ON CONFLICT...UPDATE planning — both DML, deferred. Ported eagerly
/// (pure renumbering over the arena) so the DML legs only need to call it.
pub fn extract_update_targetlist_colnos(
    root: &mut PlannerInfo,
    tlist: &[NodeId],
) -> alloc::vec::Vec<AttrNumber> {
    let mut update_colnos: alloc::vec::Vec<AttrNumber> = alloc::vec::Vec::new();
    let mut nextresno: AttrNumber = 1;
    for &id in tlist.iter() {
        let tle = root.targetentry_mut(id);
        if !tle.resjunk {
            update_colnos.push(tle.resno);
        }
        tle.resno = nextresno;
        nextresno += 1;
    }
    update_colnos
}

// ===========================================================================
// get_plan_rowmark (preptlist.c:525)
// ===========================================================================

/// `get_plan_rowmark(rowmarks, rtindex)` (preptlist.c:525) — locate the
/// `PlanRowMark` for the given RT index, or `None` if none.
///
/// In C, `rowmarks` is a `List *` of `PlanRowMark *` and the function scans for
/// `rc->rti == rtindex`. Here `rowmarks` is a `Vec<NodeId>` of opaque handles
/// with no backing store (`PlanRowMark`s come from the unported
/// `preprocess_rowmarks`); the list is empty on every reachable path, so the
/// scan finds nothing and returns `None`. A non-empty list means a DML/locking
/// path that needs the PlanRowMark-carrier keystone to resolve `rc->rti` — we
/// seam-and-panic there rather than silently return `None` (which would
/// mis-report "no rowmark" and skip required junk-column / locking logic).
///
/// Returns the matching [`PlanRowMarkId`] handle or `None`. The only consumer in
/// the current tree is `check_index_predicates` (indxpath), which only needs the
/// not-NULL test — see [`has_plan_rowmark`].
///
/// The PlanRowMark-carrier keystone landed the [`PlanRowMarkId`] store on
/// `PlannerRun`, so `rowMarks` now carries resolvable handles; but resolving
/// `rc->rti` to compare against `rtindex` needs the `&PlannerRun` value, which
/// this lookup is not yet threaded (its only caller, `seam_has_plan_rowmark`,
/// receives only `&PlannerInfo`). On every reachable SELECT path `root.rowMarks`
/// is empty (it is filled by the still-unported `preprocess_rowmarks`), so the
/// list-walk is a no-op; a non-empty list is unreachable until the DML/locking
/// analyze family threads the run, so we seam-and-panic rather than silently
/// return `None`.
pub fn get_plan_rowmark(rowmarks: &[PlanRowMarkId], _rtindex: u32) -> Option<PlanRowMarkId> {
    if rowmarks.is_empty() {
        return None;
    }
    panic!(
        "get_plan_rowmark: PlanRowMark lookup not yet threaded the `&PlannerRun` run context \
         needed to resolve `rc->rti` — `rowMarks` now carries `PlanRowMarkId` handles into the \
         run's rowmark store, but `seam_has_plan_rowmark` receives only `&PlannerInfo`; needs the \
         DML/locking analyze family to thread the run (`preprocess_rowmarks` owner)"
    );
}

/// Backs `backend_optimizer_util_restrictinfo_seams::has_plan_rowmark`: does the
/// query carry a `PlanRowMark` for `rtindex`? (`get_plan_rowmark(...) != NULL`.)
///
/// C site: `check_index_predicates` (indxpath.c:4029) ORs this with
/// `bms_is_member(rel->relid, root->all_result_relids)` to detect a
/// FOR-UPDATE/target relation. On the SELECT path `root.rowMarks` is empty so
/// this is `false`; the DML/locking path panics inside `get_plan_rowmark`.
fn seam_has_plan_rowmark(root: &PlannerInfo, rtindex: u32) -> bool {
    get_plan_rowmark(&root.rowMarks, rtindex).is_some()
}

// ===========================================================================
// seam wiring
// ===========================================================================

/// Install the seams this unit owns. Wired into the central init sequence.
pub fn init_seams() {
    backend_optimizer_prep_preptlist_seams::preprocess_targetlist::set(preprocess_targetlist);
    backend_optimizer_util_restrictinfo_seams::has_plan_rowmark::set(seam_has_plan_rowmark);
}

#[cfg(test)]
mod tests;
