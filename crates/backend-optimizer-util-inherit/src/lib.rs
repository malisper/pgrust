#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

//! `backend/optimizer/util/inherit.c` — Inheritance / partition / UNION-ALL
//! child-relation expansion.
//!
//! `expand_inherited_rtentry` expands a range-table entry whose `inh` bit is set
//! into its child "otherrels". Three cases:
//!
//!   * **RTE_SUBQUERY** — a UNION-ALL appendrel. `pull_up_simple_union_all`
//!     (prepjointree.c) already built the child RTEs and `AppendRelInfo`s;
//!     `expand_appendrel_subquery` just materialises a child `RelOptInfo` per
//!     member via `build_simple_rel`.
//!
//!   * **RTE_RELATION, RELKIND_PARTITIONED_TABLE** — partitioned table.
//!     `expand_partitioned_rtentry` prunes partitions and builds a child RTE +
//!     `AppendRelInfo` + `RelOptInfo` per surviving partition, recursing on
//!     partitioned children. The pruning step `prune_append_rel_partitions`
//!     (partprune.c) and the `PlannerGlobal::partition_directory` are not yet
//!     ported (partprune is keystone-blocked), so that branch reaches the
//!     `prune_append_rel_partitions` seam and panics loudly there.
//!
//!   * **RTE_RELATION, ordinary table** — traditional-inheritance parent.
//!     `find_all_inheritors` scans `pg_inherits` transitively, and one child RTE
//!     + `AppendRelInfo` + `RelOptInfo` is built per inheritor
//!     (`expand_single_inheritance_child`). This is the path a
//!     `SELECT ... FROM parent` over an `INHERITS` hierarchy takes.

extern crate alloc;

use alloc::string::ToString;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::{Index, InvalidAttrNumber};
use types_core::catalog::OIDOID;
use types_core::InvalidOid;
use types_error::{PgError, PgResult};
use types_nodes::nodelockrows::{PlanRowMark, RowMarkType, ROW_MARK_COPY};
use types_nodes::parsenodes::{RangeTblEntry, RTEKind};
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::{planner_rt_fetch, PlannerRun};
use types_pathnodes::{
    AppendRelInfo, NodeId, PlanRowMarkId, PlannerInfo, RelId, Relids,
};
use types_rel::Relation;
use types_tuple::access::RELKIND_PARTITIONED_TABLE;
use types_tuple::heaptuple::{
    FirstLowInvalidHeapAttributeNumber, SelfItemPointerAttributeNumber,
    TableOidAttributeNumber, TIDOID,
};

use backend_nodes_core::makefuncs;
use backend_optimizer_util_appendinfo::{adjust_appendrel_attrs, make_append_rel_info};
use backend_optimizer_util_relnode::{build_simple_rel, expand_planner_arrays};
use backend_optimizer_util_relnode_seams as bms;

use backend_access_table_table_seams as tbl;
use backend_catalog_pg_inherits_seams as pginh;
use backend_optimizer_path_equivclass_ext_seams as eqext;
use backend_optimizer_plan_planner_seams as planner;
use backend_optimizer_util_appendinfo_seams as aiseam;
use backend_optimizer_util_joininfo_ext_seams as joinext;
use backend_partitioning_partprune_seams as partprune;

/// `NoLock` — the planner already holds the parent's lock.
const NO_LOCK: i32 = 0;

/// Install the inherit.c seams owned here: `expand_inherited_rtentry` (declared
/// in `backend-optimizer-plan-init-subselect-ext-seams`) and
/// `apply_child_basequals` (declared in `backend-optimizer-util-relnode-ext-seams`).
pub fn init_seams() {
    backend_optimizer_plan_init_subselect_ext_seams::expand_inherited_rtentry::set(
        |run, root, rti| expand_inherited_rtentry(run, root, rti),
    );
    backend_optimizer_util_relnode_ext_seams::apply_child_basequals::set(
        |run, root, parent, rel, rti, appinfo| {
            apply_child_basequals(run, root, parent, rel, rti, appinfo)
        },
    );
}

/// `get_plan_rowmark(rowMarks, rti)` (planmain.c) — the `PlanRowMark` for the
/// relation at `rti`, resolving each `rowMarks` handle through the run store.
fn get_plan_rowmark(
    run: &PlannerRun,
    root: &PlannerInfo,
    rti: Index,
) -> Option<PlanRowMarkId> {
    for &id in root.rowMarks.iter() {
        if run.resolve_rowmark(id).rti == rti {
            return Some(id);
        }
    }
    None
}

/// `expand_inherited_rtentry(root, rel, rte, rti)` (inherit.c:85) — expand a
/// range-table entry with `inh` set into its inheritance/partition/UNION-ALL
/// children.
pub fn expand_inherited_rtentry<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rti: i32,
) -> PgResult<()> {
    // Assert(rte->inh) — the caller (add_other_rels_to_query) already gated on it.
    debug_assert!(planner_rt_fetch(run, root, rti as Index).inh);

    if planner_rt_fetch(run, root, rti as Index).rtekind == RTEKind::RTE_SUBQUERY {
        return expand_appendrel_subquery(run, root, rti);
    }

    // Assert(rte->rtekind == RTE_RELATION).
    debug_assert_eq!(
        planner_rt_fetch(run, root, rti as Index).rtekind,
        RTEKind::RTE_RELATION
    );

    let parent_oid = planner_rt_fetch(run, root, rti as Index).relid;
    let lockmode = planner_rt_fetch(run, root, rti as Index).rellockmode;

    let mcx = run.mcx();

    // The rewriter already holds an appropriate lock on the parent, so open it
    // without locking. Child relations are locked as we add them.
    let oldrelation = tbl::table_open::call(mcx, parent_oid, NO_LOCK)?;

    // If parent is selected FOR UPDATE/SHARE, mark its PlanRowMark isParent=true
    // and remember the prior allMarkTypes / isParent.
    let oldrc = get_plan_rowmark(run, root, rti as Index);
    let (old_isParent, old_allMarkTypes) = if let Some(rc_id) = oldrc {
        let rc = run.resolve_rowmark(rc_id);
        let was_parent = rc.isParent;
        let allmt = rc.allMarkTypes;
        run.resolve_rowmark_mut(rc_id).isParent = true;
        (was_parent, allmt)
    } else {
        (false, 0)
    };

    let parent_relkind = oldrelation.rd_rel.relkind;

    if parent_relkind == RELKIND_PARTITIONED_TABLE {
        // Partitioned table — set up for partitioning.
        debug_assert_eq!(
            planner_rt_fetch(run, root, rti as Index).relkind as u8,
            RELKIND_PARTITIONED_TABLE
        );

        // getRTEPermissionInfo(parse->rteperminfos, rte)->updatedCols, used as
        // the root partrel's parent_updatedCols.
        expand_partitioned_rtentry(run, root, rti as Index, &oldrelation, oldrc, lockmode)?;
    } else {
        // Ordinary table — traditional inheritance.  (Partitioned tables can't
        // have inheritance children, so the two cases are mutually exclusive.)

        // Scan for all members of the inheritance set, acquiring needed locks.
        // find_all_inheritors keeps the per-child locks (lockmode); we re-open
        // each child below without locking.
        let inh_oids = pginh::find_all_inheritors::call(mcx, parent_oid, lockmode)?;

        // The table itself is always found; treat an only-self set as normal
        // inheritance (we no longer special-case the no-children situation).
        debug_assert!(!inh_oids.is_empty());
        debug_assert_eq!(inh_oids[0], parent_oid);

        // Expand simple_rel_array and friends to hold the child objects.
        expand_planner_arrays(root, inh_oids.len() as i32);

        // Expand children in find_all_inheritors order.
        for &child_oid in inh_oids.iter() {
            // We already hold the required locks; just open.
            let child_owned;
            let newrelation: &Relation<'mcx> = if child_oid != parent_oid {
                child_owned = Some(tbl::table_open::call(mcx, child_oid, NO_LOCK)?);
                child_owned.as_ref().unwrap()
            } else {
                child_owned = None;
                &oldrelation
            };

            // Silently ignore children that are temp tables of other backends —
            // their buffers are not safely accessible.
            if child_oid != parent_oid && relation_is_other_temp(newrelation)? {
                if let Some(cr) = child_owned {
                    cr.close(lockmode)?;
                }
                continue;
            }

            // Create RTE and AppendRelInfo, plus PlanRowMark if needed.
            let child_rt_index = expand_single_inheritance_child(
                run,
                root,
                rti as Index,
                &oldrelation,
                oldrc,
                newrelation,
            )?;

            // Create the otherrel RelOptInfo too.
            let parent_rel = root.simple_rel_array[rti as usize]
                .expect("expand_inherited_rtentry: parent rel slot empty");
            build_simple_rel(run, root, child_rt_index as i32, Some(parent_rel))?;

            // Close child relations, but keep locks.
            if let Some(cr) = child_owned {
                cr.close(NO_LOCK)?;
            }
        }
    }

    // Some children might require different mark types reported into oldrc; if
    // so, add the matching resjunk Vars to the top-level targetlist and the
    // parent's reltarget (matching what preprocess_targetlist would have added).
    if let Some(rc_id) = oldrc {
        let (rc_rti, rc_rowmark_id, new_all_mark_types) = {
            let rc = run.resolve_rowmark(rc_id);
            (rc.rti, rc.rowmarkId, rc.allMarkTypes)
        };
        let mut newvars: Vec<Expr> = Vec::new();

        // Add TID junk Var if needed, unless we had it already.
        if (new_all_mark_types & !(1 << ROW_MARK_COPY)) != 0
            && (old_allMarkTypes & !(1 << ROW_MARK_COPY)) == 0
        {
            let var = Expr::Var(makefuncs::make_var(
                rc_rti as i32,
                SelfItemPointerAttributeNumber,
                TIDOID,
                -1,
                InvalidOid,
                0,
            ));
            push_rowmark_junk_var(root, &var, &alloc::format!("ctid{}", rc_rowmark_id));
            newvars.push(var);
        }

        // Add whole-row junk Var if needed, unless we had it already.
        if (new_all_mark_types & (1 << ROW_MARK_COPY)) != 0
            && (old_allMarkTypes & (1 << ROW_MARK_COPY)) == 0
        {
            let rte = planner_rt_fetch(run, root, rc_rti);
            let var = Expr::Var(makefuncs::make_whole_row_var(rte, rc_rti as i32, 0, false)?);
            push_rowmark_junk_var(root, &var, &alloc::format!("wholerow{}", rc_rowmark_id));
            newvars.push(var);
        }

        // Add tableoid junk Var, unless we had it already.
        if !old_isParent {
            let var = Expr::Var(makefuncs::make_var(
                rc_rti as i32,
                TableOidAttributeNumber,
                OIDOID,
                -1,
                InvalidOid,
                0,
            ));
            push_rowmark_junk_var(root, &var, &alloc::format!("tableoid{}", rc_rowmark_id));
            newvars.push(var);
        }

        // Add the newly added Vars to the parent's reltarget.  The children's
        // reltargets are made later.
        let singleton = bms::relids_make_singleton::call(0);
        eqext::add_vars_to_targetlist::call(root, newvars, singleton)?;
    }

    oldrelation.close(NO_LOCK)?;
    Ok(())
}

/// Build a resjunk `TargetEntry` for a rowmark junk Var and append it to
/// `root->processed_tlist` (mirrors C's
/// `lappend(root->processed_tlist, makeTargetEntry(...))`).
fn push_rowmark_junk_var(root: &mut PlannerInfo, var: &Expr, resname: &str) {
    let expr_id = root.alloc_node(var.clone());
    let tle = types_pathnodes::TargetEntryNode {
        expr: expr_id,
        resno: (root.processed_tlist.len() + 1) as i16,
        resname: Some(resname.to_string()),
        ressortgroupref: 0,
        resorigtbl: InvalidOid,
        resorigcol: 0,
        resjunk: true,
    };
    let tle_id = root.alloc_targetentry(tle);
    root.processed_tlist.push(tle_id);
}

/// Convert the i32-alias `LockClauseStrength` (`PlanRowMark.strength`) to the
/// `rawnodes::LockClauseStrength` enum the planner's `select_rowmark_type`
/// speaks. Repr values match (`lockoptions.h`, verified 0..=4).
fn lock_clause_strength_from_i32(s: i32) -> types_nodes::rawnodes::LockClauseStrength {
    use types_nodes::rawnodes::LockClauseStrength as L;
    match s {
        0 => L::LCS_NONE,
        1 => L::LCS_FORKEYSHARE,
        2 => L::LCS_FORSHARE,
        3 => L::LCS_FORNOKEYUPDATE,
        4 => L::LCS_FORUPDATE,
        other => panic!("lock_clause_strength_from_i32: invalid strength {}", other),
    }
}

/// `RELATION_IS_OTHER_TEMP(rel)` (rel.h): a temp relation belonging to another
/// session — `relpersistence == 't' && !rd_islocaltemp`.
fn relation_is_other_temp(rel: &Relation<'_>) -> PgResult<bool> {
    Ok(rel.rd_rel.relpersistence == types_tuple::access::RELPERSISTENCE_TEMP
        && !backend_utils_cache_relcache_seams::rd_islocaltemp::call(rel)?)
}

/// `expand_partitioned_rtentry(...)` (inherit.c:317) — recursively expand an RTE
/// for a partitioned table.
fn expand_partitioned_rtentry<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    parent_rt_index: Index,
    _parentrel: &Relation<'mcx>,
    _top_parentrc: Option<PlanRowMarkId>,
    _lockmode: i32,
) -> PgResult<()> {
    // check_stack_depth(); Assert(parentrte->inh);
    debug_assert!(planner_rt_fetch(run, root, parent_rt_index).inh);

    // partdesc = PartitionDirectoryLookup(root->glob->partition_directory,
    // parentrel); root->partColsUpdated |= has_partition_attrs(...);
    // relinfo->live_parts = prune_append_rel_partitions(relinfo);
    //
    // The PartitionDirectory lives on PlannerGlobal::partition_directory (not yet
    // modeled) and prune_append_rel_partitions is owned by partprune.c, which is
    // keystone-blocked on the PartitionPruneStep carrier (see the
    // partprune-blocked memory note). Mirror PG and panic precisely at the first
    // unported substrate — partition pruning.
    let parent_rel = root.simple_rel_array[parent_rt_index as usize]
        .expect("expand_partitioned_rtentry: parent rel slot empty");
    let _ = partprune::prune_append_rel_partitions::call(root, parent_rel)?;

    unreachable!(
        "expand_partitioned_rtentry: prune_append_rel_partitions panics until \
         partprune.c lands (and PlannerGlobal::partition_directory is modeled)"
    )
}

/// `expand_single_inheritance_child(...)` (inherit.c:460) — build a child
/// `RangeTblEntry` and an `AppendRelInfo`, plus maybe a `PlanRowMark`. Returns
/// the child RT index.
fn expand_single_inheritance_child<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    parent_rt_index: Index,
    parentrel: &Relation<'mcx>,
    top_parentrc: Option<PlanRowMarkId>,
    childrel: &Relation<'mcx>,
) -> PgResult<Index> {
    let mcx = run.mcx();
    let parent_oid = parentrel.rd_id;
    let child_oid = childrel.rd_id;
    let child_relkind = childrel.rd_rel.relkind;

    // Build an RTE for the child as a flat copy of the parent's RTE, replacing
    // relid/relkind/inh, clearing securityQuals (the parent's RLS conditions are
    // propagated through baserestrictinfo, not duplicated here) and perminfoindex
    // (no permission checking on child RTEs).
    let mut childrte: RangeTblEntry<'mcx> =
        planner_rt_fetch(run, root, parent_rt_index).clone_in(mcx)?;
    debug_assert_eq!(childrte.rtekind, RTEKind::RTE_RELATION);
    childrte.relid = child_oid;
    childrte.relkind = child_relkind as i8;
    // A partitioned child needs further expansion.
    if childrte.relkind as u8 == RELKIND_PARTITIONED_TABLE {
        debug_assert_ne!(child_oid, parent_oid);
        childrte.inh = true;
    } else {
        childrte.inh = false;
    }
    childrte.securityQuals = mcx::PgVec::new_in(mcx);
    childrte.perminfoindex = 0;

    // Build the child column alias list (parent's query-assigned names where the
    // child column maps back to a parent column; the child's own name for new
    // columns; empty string for dropped columns) so ruleutils prints correct
    // child-column aliases. Done before linking the appinfo into root because it
    // reads appinfo->parent_colnos.
    let appinfo = make_append_rel_info(
        root,
        parentrel,
        childrel,
        parent_rt_index,
        // childRTindex filled in after we know the table length below; but
        // make_append_rel_info only uses it for translated Vars' varno.
        (run.rtable(root.parse).len() + 1) as Index,
    )?;

    // tablesample is probably null, but copy it (clone_in already copied it).

    // Construct the alias / eref from the parent alias name + child colnames.
    let parent_aliasname = {
        let parentrte = planner_rt_fetch(run, root, parent_rt_index);
        parentrte
            .eref
            .as_ref()
            .and_then(|e| e.aliasname.as_ref().map(|s| s.as_str().to_string()))
            .unwrap_or_default()
    };
    let parent_colname_count = {
        let parentrte = planner_rt_fetch(run, root, parent_rt_index);
        parentrte.eref.as_ref().map(|e| e.colnames.len()).unwrap_or(0)
    };

    let child_tupdesc = &childrel.rd_att;
    let mut child_colnames: mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> =
        mcx::PgVec::new_in(mcx);
    for cattno in 0..(child_tupdesc.natts as usize) {
        let att = child_tupdesc.attr(cattno);
        let attname: alloc::string::String = if att.attisdropped {
            // Always an empty string for a dropped column.
            alloc::string::String::new()
        } else {
            let pc = *appinfo.parent_colnos.get(cattno).unwrap_or(&0) as i32;
            if pc > 0 && (pc as usize) <= parent_colname_count {
                // Duplicate the query-assigned parent column name.
                let parentrte = planner_rt_fetch(run, root, parent_rt_index);
                let eref = parentrte.eref.as_ref().unwrap();
                node_string_value(&eref.colnames[(pc - 1) as usize])
            } else {
                // New column — use its real name.
                alloc::string::String::from_utf8_lossy(att.attname.name_str()).into_owned()
            }
        };
        child_colnames.push(make_string_node(mcx, &attname)?);
    }

    let alias = makefuncs::make_alias(mcx, &parent_aliasname, child_colnames)?;
    let alias_box = mcx::alloc_in(mcx, alias)?;
    // childrte->alias = childrte->eref = makeAlias(...). The two share the value;
    // clone the second.
    let eref_alias = {
        let a = &*alias_box;
        let mut colnames2: mcx::PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> =
            mcx::PgVec::new_in(mcx);
        for cn in a.colnames.iter() {
            colnames2.push(make_string_node(mcx, &node_string_value(cn))?);
        }
        makefuncs::make_alias(mcx, &parent_aliasname, colnames2)?
    };
    childrte.alias = Some(alias_box);
    childrte.eref = Some(mcx::alloc_in(mcx, eref_alias)?);

    // Link the child RTE into parse->rtable; childRTindex = list_length(rtable).
    run.rtable_mut(root.parse).push(childrte);
    let child_rt_index = run.rtable(root.parse).len() as Index;

    // Intern the child RTE into the run RTE store and record it in
    // simple_rte_array, and the appinfo in append_rel_array (the caller already
    // grew both arrays via expand_planner_arrays / setup).
    let childrte_id = {
        // Move a clone of the just-pushed RTE into the RTE store (simple_rte_array
        // resolves through this store).
        let pushed = run.rtable(root.parse).last().unwrap().clone_in(mcx)?;
        run.intern_rte(pushed)
    };

    debug_assert!((child_rt_index as i32) < root.simple_rel_array_size);
    root.simple_rte_array[child_rt_index as usize] = childrte_id;
    debug_assert!(root.append_rel_array[child_rt_index as usize].is_none());
    root.append_rel_array[child_rt_index as usize] = Some(appinfo.clone());

    // Also append to root->append_rel_list.
    root.append_rel_list.push(appinfo);

    // Build a PlanRowMark if the parent is marked FOR UPDATE/SHARE.
    if let Some(top_id) = top_parentrc {
        let (top_rti, top_rowmark_id, top_strength, top_wait_policy) = {
            let rc = run.resolve_rowmark(top_id);
            (rc.rti, rc.rowmarkId, rc.strength, rc.waitPolicy)
        };
        // Reselect rowmark type — child relkind might differ from the parent.
        // `PlanRowMark.strength`/`markType` are the i32-alias representations; the
        // seam speaks the enum forms (same repr values, verified 0..=5).
        let strength_enum = lock_clause_strength_from_i32(top_strength);
        let mark_type_i32: RowMarkType = {
            let crte = run.resolve_rte(childrte_id);
            planner::select_rowmark_type::call(crte, strength_enum)? as i32
        };
        let mut childrc = PlanRowMark::default();
        childrc.rti = child_rt_index;
        childrc.prti = top_rti;
        childrc.rowmarkId = top_rowmark_id;
        childrc.markType = mark_type_i32;
        childrc.allMarkTypes = 1 << mark_type_i32;
        childrc.strength = top_strength;
        childrc.waitPolicy = top_wait_policy;
        // Mark partitioned-child rowmarks as parent rowmarks so the executor
        // ignores them (but their existence still locks the child tables).
        childrc.isParent = child_relkind == RELKIND_PARTITIONED_TABLE;

        // Include child's rowmark type in top parent's allMarkTypes.
        run.resolve_rowmark_mut(top_id).allMarkTypes |= childrc.allMarkTypes;

        let new_rc_id = run.intern_rowmark(childrc);
        root.rowMarks.push(new_rc_id);
    }

    // If this is a child of the query target relation (UPDATE/DELETE/MERGE), add
    // it to all_result_relids, and (for non-partitioned children) to
    // leaf_result_relids with required row-identity data.
    if bms::relids_is_member::call(parent_rt_index as i32, &root.all_result_relids) {
        root.all_result_relids =
            bms::relids_add_member::call(root.all_result_relids.take(), child_rt_index as i32);

        if child_relkind != RELKIND_PARTITIONED_TABLE {
            root.leaf_result_relids =
                bms::relids_add_member::call(root.leaf_result_relids.take(), child_rt_index as i32);

            // Assume all child target relations need a junk "tableoid" column.
            let rrvar = makefuncs::make_var(
                child_rt_index as i32,
                TableOidAttributeNumber,
                OIDOID,
                -1,
                InvalidOid,
                0,
            );
            // add_row_identity_var(root, rrvar, childRTindex, "tableoid") +
            // add_row_identity_columns(root, childRTindex, childrte, childrel).
            // These are appendinfo-owned; the row-identity-var helper is private,
            // and the column helper's foreign-table delete-trigger predicate is a
            // seam — both are reached only on the inherited UPDATE/DELETE/MERGE
            // target path. add_row_identity_var has no public/seam entry, so this
            // sub-branch mirrors PG and panics until appendinfo exposes it.
            let _ = rrvar;
            return Err(PgError::error(
                "expand_single_inheritance_child: inherited UPDATE/DELETE/MERGE \
                 child target row-identity registration (add_row_identity_var) is \
                 not exposed by the appendinfo owner yet",
            ));
        }
    }

    Ok(child_rt_index)
}

/// `makeString(pstrdup(s))` as a value-node `NodePtr` — the parser's colname
/// representation (`Node::String`).
fn make_string_node<'mcx>(
    mcx: Mcx<'mcx>,
    s: &str,
) -> PgResult<types_nodes::nodes::NodePtr<'mcx>> {
    let node = types_nodes::nodes::Node::mk_string(
        mcx,
        types_nodes::value::StringNode {
            sval: mcx::PgString::from_str_in(s, mcx)?,
        },
    );
    mcx::alloc_in(mcx, node)
}

/// Read the `String` value out of a value-node `NodePtr` (the parser's
/// `makeString` colname). Empty when the node is not a String.
fn node_string_value(np: &types_nodes::nodes::NodePtr<'_>) -> alloc::string::String {
    match (**np).as_string() {
        Some(s) => s.sval.as_str().to_string(),
        None => alloc::string::String::new(),
    }
}

/// `get_rel_all_updated_cols(root, rel)` (inherit.c:655) — the set of columns of
/// a simple relation that are updated by this query, mapped to `rel`'s numbering.
pub fn get_rel_all_updated_cols<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    rel: RelId,
) -> PgResult<Relids> {
    // Assert(root->parse->commandType == CMD_UPDATE); Assert(IS_SIMPLE_REL(rel)).
    let result_relation = run.resolve(root.parse).resultRelation;
    let rte = planner_rt_fetch(run, root, result_relation as Index);
    let perminfo_idx = backend_parser_relation_seams::get_rte_permission_info::call(
        &run.resolve(root.parse).rteperminfos,
        rte,
    )?;
    // Convert the RTEPermissionInfo bitmapset (`types_nodes::Bitmapset`, words:
    // PgVec<u64>) into a planner `Relids` by walking its set bits.
    let mut updated_cols: Relids = None;
    if let Some(bms) = run.resolve(root.parse).rteperminfos[perminfo_idx]
        .updatedCols
        .as_ref()
    {
        for (wi, &word) in bms.words.iter().enumerate() {
            let mut w = word;
            while w != 0 {
                let bit = w.trailing_zeros() as i32;
                let member = (wi as i32) * 64 + bit;
                updated_cols = bms::relids_add_member::call(updated_cols.take(), member);
                w &= w - 1;
            }
        }
    }

    let rel_relid = root.rel(rel).relid;
    if rel_relid != result_relation as Index {
        // IS_OTHER_REL(rel): translate to this descendant's numbering.
        let top_parent_rel = find_base_rel(root, result_relation as Index);
        updated_cols =
            translate_col_privs_multilevel(root, rel, top_parent_rel, updated_cols)?;
    }

    // Add generated columns that depend on the updatedCols.
    let extra = backend_optimizer_util_plancat::get_dependent_generated_columns(
        run,
        root,
        root.rel(rel).relid,
        &updated_cols,
    )?;
    Ok(bms::relids_add_members::call(updated_cols, &extra))
}

/// `find_base_rel(root, relid)` (relnode.c) over the simple_rel_array.
fn find_base_rel(root: &PlannerInfo, relid: Index) -> RelId {
    root.simple_rel_array[relid as usize].expect("find_base_rel: no rel for relid")
}

/// `expand_appendrel_subquery(root, rel, rte, rti)` (inherit.c:798) — add
/// children of an appendrel `RTE_SUBQUERY` (a UNION-ALL parent). The
/// `AppendRelInfo`s were already built by `pull_up_simple_union_all`; build a
/// child `RelOptInfo` for each, recursing if a child is itself an appendrel.
fn expand_appendrel_subquery<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rti: i32,
) -> PgResult<()> {
    let rel = root.simple_rel_array[rti as usize]
        .expect("expand_appendrel_subquery: parent rel slot is empty");

    // Snapshot the child relids for this parent (append_rel_list holds all append
    // rels; ignore others). Collected up front to release the borrow on
    // append_rel_list before build_simple_rel takes &mut root.
    let child_rtindexes: Vec<Index> = root
        .append_rel_list
        .iter()
        .filter(|appinfo| appinfo.parent_relid == rti as Index)
        .map(|appinfo| appinfo.child_relid)
        .collect();

    for child_rtindex in child_rtindexes {
        // The child RTE should already exist (pull_up_simple_union_all).
        debug_assert!((child_rtindex as i32) < root.simple_rel_array_size);

        build_simple_rel(run, root, child_rtindex as i32, Some(rel))?;

        // Child may itself be an inherited rel, either table or subquery.
        if planner_rt_fetch(run, root, child_rtindex).inh {
            expand_inherited_rtentry(run, root, child_rtindex as i32)?;
        }
    }

    Ok(())
}

/// `translate_col_privs(parent_privs, translated_vars)` (inherit.c:709) —
/// translate a bitmapset of per-column privileges from the parent rel's
/// attribute numbering to the child's, using `appinfo->translated_vars`.
///
/// A parent whole-row reference is **not** translated into a child whole-row
/// reference; instead the per-column bits for all inherited columns are set.
pub fn translate_col_privs(
    root: &PlannerInfo,
    parent_privs: &Relids,
    translated_vars: &[NodeId],
) -> Relids {
    let mut child_privs: Relids = None;
    let flhan = FirstLowInvalidHeapAttributeNumber as i32;

    // System attributes have the same numbers in all tables.
    let mut attno = flhan + 1;
    while attno < 0 {
        if bms::relids_is_member::call(attno - flhan, parent_privs) {
            child_privs = bms::relids_add_member::call(child_privs, attno - flhan);
        }
        attno += 1;
    }

    // Check if parent has a whole-row reference.
    let whole_row =
        bms::relids_is_member::call((InvalidAttrNumber as i32) - flhan, parent_privs);

    // Translate the regular user attributes via the vars list.
    let mut attno = InvalidAttrNumber as i32;
    for id in translated_vars.iter() {
        attno += 1;
        // C: `Var *var = lfirst_node(Var, lc); if (var == NULL) continue;`
        if *id == NodeId::default() {
            continue;
        }
        let var = root
            .node(*id)
            .as_var()
            .expect("translate_col_privs: translated_var is not a Var");
        if whole_row || bms::relids_is_member::call(attno - flhan, parent_privs) {
            child_privs =
                bms::relids_add_member::call(child_privs, (var.varattno as i32) - flhan);
        }
    }

    child_privs
}

/// `translate_col_privs_multilevel(root, rel, parent_rel, parent_cols)`
/// (inherit.c:759) — recursively translate the column numbers in `parent_cols`
/// to the column numbers of the descendant relation `rel`, given the top parent
/// `parent_rel`.
pub fn translate_col_privs_multilevel(
    root: &PlannerInfo,
    rel: RelId,
    parent_rel: RelId,
    parent_cols: Relids,
) -> PgResult<Relids> {
    let mut parent_cols = parent_cols;

    // Fast path for the easy case.
    if parent_cols.is_none() {
        return Ok(None);
    }

    let rel_parent = root.rel(rel).parent;
    let rel_relid = root.rel(rel).relid;

    // Recurse if the immediate parent is not the top parent.
    if rel_parent != Some(parent_rel) {
        match rel_parent {
            Some(p) => {
                parent_cols = translate_col_privs_multilevel(root, p, parent_rel, parent_cols)?;
            }
            None => {
                return Err(PgError::error(alloc::format!(
                    "rel with relid {} is not a child rel",
                    rel_relid
                )));
            }
        }
    }

    // Now translate for this child.
    debug_assert!(!root.append_rel_array.is_empty());
    let vars = {
        let appinfo: &AppendRelInfo = root.append_rel_array[rel_relid as usize]
            .as_ref()
            .ok_or_else(|| {
                PgError::error(
                    "translate_col_privs_multilevel: append_rel_array[rel->relid] is NULL",
                )
            })?;
        appinfo.translated_vars.clone()
    };

    Ok(translate_col_privs(root, &parent_cols, &vars))
}

/// `expand_partitioned_rtentry` helper symbol — kept for symmetry with C; the
/// public path is `expand_inherited_rtentry`.
#[allow(dead_code)]
fn _expand_partitioned_rtentry_sym() {}

/// `apply_child_basequals(root, parentrel, childrel, childRTE, appinfo)`
/// (inherit.c:841) — populate `childrel`'s base restriction quals from
/// `parentrel`'s, translating Vars through `appinfo`, re-checking for quals that
/// const-fold to TRUE/FALSE for this child, and pulling up the child RTE's own
/// securityQuals. Returns `false` if a qual is provably always-false (the child
/// can be pruned).
pub fn apply_child_basequals<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    parentrel: RelId,
    childrel: RelId,
    child_rti: Index,
    appinfo: &AppendRelInfo,
) -> PgResult<bool> {
    const UINT_MAX: Index = u32::MAX;
    let mcx = run.mcx();

    let mut childquals: Vec<types_pathnodes::RinfoId> = Vec::new();
    let mut cq_min_security: Index = UINT_MAX;

    // Process each parent RestrictInfo separately (to keep per-qual security
    // levels): translate Vars, const-fold, and reconstitute.
    let parent_rinfos = root.rel(parentrel).baserestrictinfo.clone();
    let appinfos = [appinfo.clone()];
    for rinfo_id in parent_rinfos {
        // rinfo->clause, security_level, is_pushed_down, has_clone, is_clone.
        let (clause, is_pushed_down, has_clone, is_clone, security_level) = {
            let rinfo = root.rinfo(rinfo_id);
            (
                root.node(rinfo.clause).clone(),
                rinfo.is_pushed_down,
                rinfo.has_clone,
                rinfo.is_clone,
                rinfo.security_level,
            )
        };

        let childqual = adjust_appendrel_attrs(root, clause, &appinfos)?;
        let childqual =
            backend_optimizer_util_clauses::fold::eval_const_expressions(mcx, childqual)?;

        // Flat-out constant?
        if let Expr::Const(c) = &childqual {
            if c.constisnull || !const_bool_is_true(c) {
                // Reduces to constant FALSE or NULL.
                return Ok(false);
            }
            // Reduces to constant TRUE — drop it.
            continue;
        }

        // Might be an AND clause; flatten it.
        for onecq in makefuncs::make_ands_implicit(Some(childqual)) {
            // Pseudoconstant: no Vars at this level and no volatile functions.
            let node = types_nodes::nodes::Node::mk_expr(run.mcx(), onecq.clone());
            let pseudoconstant = !backend_optimizer_util_vars::var::contain_vars_of_level(&node, 0)
                && !backend_optimizer_util_clauses::grounded::contain_volatile_functions(Some(
                    &onecq,
                ))?;
            if pseudoconstant {
                // Tell createplan.c to check for gating quals.
                root.hasPseudoConstantQuals = true;
            }

            let childrinfo = backend_optimizer_util_joininfo::restrictinfo::make_restrictinfo(
                root,
                onecq,
                is_pushed_down,
                has_clone,
                is_clone,
                pseudoconstant,
                security_level,
                None,
                None,
                None,
            )?;

            // Proven always false / always true?
            let clause_expr = root.node(root.rinfo(childrinfo).clause).clone();
            if joinext::restriction_is_always_false::call(root, &clause_expr) {
                return Ok(false);
            }
            if joinext::restriction_is_always_true::call(root, &clause_expr) {
                continue;
            }

            childquals.push(childrinfo);
            cq_min_security = cq_min_security.min(security_level);
        }
    }

    // In addition to the inherited quals, pull up any securityQuals on this child
    // RTE (only possible for UNION-ALL appendrels). Similar to
    // process_security_barrier_quals on the parent, but no general deductions.
    let security_qual_sets = collect_child_security_quals(run, root, child_rti, mcx)?;
    let mut security_level: Index = 0;
    for qualset in security_qual_sets {
        for qual in qualset {
            let ri = backend_optimizer_util_joininfo::restrictinfo::make_restrictinfo(
                root,
                qual,
                true,
                false,
                false,
                false,
                security_level,
                None,
                None,
                None,
            )?;
            childquals.push(ri);
            cq_min_security = cq_min_security.min(security_level);
        }
        security_level += 1;
    }
    debug_assert!(security_level <= root.qual_security_level);

    // Store the child's baserestrictinfo + minimum security level.
    root.rel_mut(childrel).baserestrictinfo = childquals;
    root.rel_mut(childrel).baserestrict_min_security = cq_min_security;

    Ok(true)
}

/// `DatumGetBool(((Const *) childqual)->constvalue)` — whether a boolean `Const`
/// is TRUE.
fn const_bool_is_true(c: &types_nodes::primnodes::Const) -> bool {
    c.constvalue.as_bool()
}

/// Collect the child RTE's `securityQuals` as a list of conjunct-lists, each
/// conjunct cloned into `mcx` as an owned `Expr`. C iterates
/// `childRTE->securityQuals` (a `List` of `List *`), and each inner list is a set
/// of qual `Expr`s at one security level.
fn collect_child_security_quals<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    child_rti: Index,
    mcx: Mcx<'mcx>,
) -> PgResult<Vec<Vec<Expr>>> {
    let rte = planner_rt_fetch(run, root, child_rti);
    let mut out: Vec<Vec<Expr>> = Vec::new();
    for qualset_np in rte.securityQuals.iter() {
        // Each securityQuals element is itself a List of Exprs (a qual set).
        let mut set: Vec<Expr> = Vec::new();
        let qualset_node = &**qualset_np;
        if let Some(list) = qualset_node.as_list() {
            for q in list.iter() {
                if let Some(e) = (**q).as_expr() {
                    set.push(e.clone_in(mcx)?);
                }
            }
        } else if let Some(e) = qualset_node.as_expr() {
            // Defensive: a bare Expr (shouldn't happen, but mirror clone path).
            set.push(e.clone_in(mcx)?);
        }
        out.push(set);
    }
    Ok(out)
}
