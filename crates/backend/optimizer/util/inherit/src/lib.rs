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

use ::mcx::Mcx;
use ::types_core::primitive::{Index, InvalidAttrNumber};
use ::types_core::catalog::OIDOID;
use ::types_core::InvalidOid;
use ::types_error::{PgError, PgResult};
use ::nodes::nodelockrows::{PlanRowMark, RowMarkType, ROW_MARK_COPY};
use ::nodes::parsenodes::{RangeTblEntry, RTEKind};
use ::nodes::primnodes::Expr;
use ::pathnodes::planner_run::{planner_rt_fetch, PlannerRun};
use ::pathnodes::{
    AppendRelInfo, NodeId, PlanRowMarkId, PlannerInfo, RelId, Relids,
};
use ::rel::Relation;
use ::types_tuple::access::RELKIND_PARTITIONED_TABLE;
use ::types_tuple::heaptuple::{
    FirstLowInvalidHeapAttributeNumber, SelfItemPointerAttributeNumber,
    TableOidAttributeNumber, TIDOID,
};

use ::nodes_core::makefuncs;
use ::appendinfo::{adjust_appendrel_attrs_in, make_append_rel_info};
use ::relnode::{build_simple_rel, expand_planner_arrays};
use relnode_seams as bms;

use table_seams as tbl;
use pg_inherits_seams as pginh;
use equivclass_ext_seams as eqext;
use planner_seams as planner;
use appendinfo_seams as aiseam;
use joininfo_ext_seams as joinext;
use partprune_seams as partprune;

/// `NoLock` — the planner already holds the parent's lock.
const NO_LOCK: i32 = 0;

/// Install the inherit.c seams owned here: `expand_inherited_rtentry` (declared
/// in `backend-optimizer-plan-init-subselect-ext-seams`) and
/// `apply_child_basequals` (declared in `backend-optimizer-util-relnode-ext-seams`).
pub fn init_seams() {
    init_subselect_ext_seams::expand_inherited_rtentry::set(
        |run, root, rti| expand_inherited_rtentry(run, root, rti),
    );
    relnode_ext_seams::apply_child_basequals::set(
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
        // the root partrel's parent_updatedCols.  For a SELECT this is empty
        // (NULL); for UPDATE/DELETE/MERGE it carries the updated columns of the
        // root partitioned target, translated to the parent's numbering at each
        // recursion level.
        // getRTEPermissionInfo(...)->updatedCols, converted from the parsenodes
        // `Bitmapset` (attno-offset by FirstLowInvalidHeapAttributeNumber) into a
        // planner `Relids` (same numbering), which both `has_partition_attrs` and
        // the recursive `translate_col_privs` speak.
        let parent_updated_cols: Relids = {
            let rte = planner_rt_fetch(run, root, rti as Index);
            let perm_idx = parser_relation_seams::get_rte_permission_info::call(
                &run.resolve(root.parse).rteperminfos,
                rte,
            )?;
            updated_bitmapset_to_relids(
                run.resolve(root.parse).rteperminfos[perm_idx]
                    .updatedCols
                    .as_deref(),
            )
        };
        expand_partitioned_rtentry(
            run,
            root,
            rti as Index,
            &oldrelation,
            &parent_updated_cols,
            oldrc,
            lockmode,
        )?;
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
        eqext::add_vars_to_targetlist::call(run.mcx(), root, newvars, singleton)?;
    }

    oldrelation.close(NO_LOCK)?;
    Ok(())
}

/// Build a resjunk `TargetEntry` for a rowmark junk Var and append it to
/// `root->processed_tlist` (mirrors C's
/// `lappend(root->processed_tlist, makeTargetEntry(...))`).
fn push_rowmark_junk_var(root: &mut PlannerInfo, var: &Expr, resname: &str) {
    let expr_id = root.alloc_node(var.clone());
    let tle = ::pathnodes::TargetEntryNode {
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
fn lock_clause_strength_from_i32(s: i32) -> ::nodes::rawnodes::LockClauseStrength {
    use ::nodes::rawnodes::LockClauseStrength as L;
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
    Ok(rel.rd_rel.relpersistence == ::types_tuple::access::RELPERSISTENCE_TEMP
        && !relcache_seams::rd_islocaltemp::call(rel)?)
}

/// `expand_partitioned_rtentry(...)` (inherit.c:317) — recursively expand an RTE
/// for a partitioned table.
fn expand_partitioned_rtentry<'mcx>(
    run: &mut PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    parent_rt_index: Index,
    parentrel: &Relation<'mcx>,
    parent_updated_cols: &Relids,
    top_parentrc: Option<PlanRowMarkId>,
    lockmode: i32,
) -> PgResult<()> {
    // check_stack_depth(); Assert(parentrte->inh);
    debug_assert!(planner_rt_fetch(run, root, parent_rt_index).inh);

    let mcx = run.mcx();

    let parent_rel = root.simple_rel_array[parent_rt_index as usize]
        .expect("expand_partitioned_rtentry: parent rel slot empty");

    // partdesc = PartitionDirectoryLookup(root->glob->partition_directory,
    // parentrel).  The PartitionDirectory was created and the descriptor pinned
    // by set_relation_partition_info (build_simple_rel) for this parent rel, so
    // looking it up here reuses that per-query-stable descriptor (no fresh
    // build, no extra relation pin).  We need partdesc->oids and partdesc->nparts
    // to materialise the live partitions.
    let partdesc = {
        let glob = root
            .glob
            .as_mut()
            .expect("expand_partitioned_rtentry: root->glob is NULL");
        partitioning_core_seams::partition_directory_lookup::call(
            mcx,
            &mut glob.partition_directory,
            parentrel.alias(),
        )?
    };
    // A partitioned table should always have a partition descriptor.
    let nparts = partdesc.nparts;

    // Note down whether any partition key cols are being updated.  Though it's
    // the root partitioned table's updatedCols we are interested in,
    // parent_updatedCols (provided by the caller) contains the root partrel's
    // updatedCols translated to match the attribute ordering of parentrel.
    if !root.partColsUpdated {
        let mut used_in_expr = false;
        let uses_part_attr = catalog_partition::has_partition_attrs(
            mcx,
            parentrel,
            parent_updated_cols.as_deref(),
            Some(&mut used_in_expr),
        )?;
        root.partColsUpdated = uses_part_attr;
    }

    // Nothing further to do here if there are no partitions.
    if nparts == 0 {
        return Ok(());
    }

    // Perform partition pruning using restriction clauses assigned to the parent
    // relation.  live_parts will contain PartitionDesc indexes of partitions
    // that survive pruning.  Below, we will initialize child objects for the
    // surviving partitions.
    let live_parts = partprune::prune_append_rel_partitions::call(run, root, parent_rel)?;
    root.rel_mut(parent_rel).live_parts = live_parts;

    // Expand simple_rel_array and friends to hold child objects.
    let num_live_parts = {
        let lp = &root.rel(parent_rel).live_parts;
        bms::relids_num_members::call(lp)
    };
    if num_live_parts > 0 {
        expand_planner_arrays(root, num_live_parts);
    }

    // We also store partition RelOptInfo pointers in the parent relation.
    // palloc0(nparts): slots for pruned partitions stay NULL.
    debug_assert!(root.rel(parent_rel).part_rels.is_empty());
    {
        let mut part_rels: Vec<Option<RelId>> = Vec::with_capacity(nparts as usize);
        part_rels.resize(nparts as usize, None);
        root.rel_mut(parent_rel).part_rels = part_rels;
    }

    // Create a child RTE for each live partition.  Unlike traditional
    // inheritance, we don't build a child RTE for the partitioned table itself,
    // because it's not going to be scanned.
    let mut i: i32 = -1;
    loop {
        i = {
            let lp = &root.rel(parent_rel).live_parts;
            bms::relids_next_member::call(lp, i)
        };
        if i < 0 {
            break;
        }

        let child_oid = partdesc.oids[i as usize];

        // Open rel, acquiring required locks.  If a partition was recently
        // detached and then dropped, opening it fails; behave as though the
        // partition had been pruned.
        let childrel = match tbl::try_table_open::call(mcx, child_oid, lockmode)? {
            None => {
                let lp = root.rel_mut(parent_rel).live_parts.take();
                root.rel_mut(parent_rel).live_parts = relids_del_member(lp, i);
                continue;
            }
            Some(c) => c,
        };

        // Temporary partitions belonging to other sessions should have been
        // disallowed at definition; double-check for paranoia's sake.
        if relation_is_other_temp(&childrel)? {
            childrel.close(NO_LOCK)?;
            return Err(PgError::error(
                "temporary relation from another session found as partition",
            ));
        }

        let child_relkind = childrel.rd_rel.relkind;

        // Create RTE and AppendRelInfo, plus PlanRowMark if needed.
        let child_rt_index = expand_single_inheritance_child(
            run,
            root,
            parent_rt_index,
            parentrel,
            top_parentrc,
            &childrel,
        )?;

        // Create the otherrel RelOptInfo too.
        let childrelinfo = build_simple_rel(run, root, child_rt_index as i32, Some(parent_rel))?;
        root.rel_mut(parent_rel).part_rels[i as usize] = Some(childrelinfo);
        let child_relids = bms::relids_copy::call(&root.rel(childrelinfo).relids);
        let merged = {
            let all = root.rel_mut(parent_rel).all_partrels.take();
            bms::relids_add_members::call(all, &child_relids)
        };
        root.rel_mut(parent_rel).all_partrels = merged;

        // If this child is itself partitioned, recurse.
        if child_relkind == RELKIND_PARTITIONED_TABLE {
            // child_updatedCols = translate_col_privs(parent_updatedCols,
            //                                         appinfo->translated_vars);
            let translated_vars = root.append_rel_array[child_rt_index as usize]
                .as_ref()
                .expect("expand_partitioned_rtentry: append_rel_array slot empty")
                .translated_vars
                .clone();
            let child_updated_cols =
                translate_col_privs(root, parent_updated_cols, &translated_vars);

            expand_partitioned_rtentry(
                run,
                root,
                child_rt_index,
                &childrel,
                &child_updated_cols,
                top_parentrc,
                lockmode,
            )?;
        }

        // Close child relation, but keep locks.
        childrel.close(NO_LOCK)?;
    }

    Ok(())
}

/// `bms_del_member(a, x)` over a planner `Relids`, via the
/// `relids_del_members` set-difference seam with a locally-built singleton
/// (mirrors the pattern used in optimizer-path-small / appendinfo).
fn relids_del_member(a: Relids, x: i32) -> Relids {
    let single = bms::relids_make_singleton::call(x);
    pathnode_seams::relids_del_members::call(a, &single)
}

/// Convert the parsenodes `Bitmapset` carried by `RTEPermissionInfo.updatedCols`
/// (member numbers offset by `FirstLowInvalidHeapAttributeNumber`) into a
/// planner `Relids` (same numbering, distinct `Bitmapset` type), by walking its
/// set bits.  `None`/empty maps to `None`.
fn updated_bitmapset_to_relids(bms: Option<&::nodes::Bitmapset<'_>>) -> Relids {
    let mut out: Relids = None;
    if let Some(b) = bms {
        for (wi, &word) in b.words.iter().enumerate() {
            let mut w = word;
            while w != 0 {
                let bit = w.trailing_zeros() as i32;
                let member = (wi as i32) * 64 + bit;
                out = bms::relids_add_member::call(out.take(), member);
                w &= w - 1;
            }
        }
    }
    out
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
    childrte.securityQuals = ::mcx::PgVec::new_in(mcx);
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
    let mut child_colnames: ::mcx::PgVec<'mcx, ::nodes::nodes::NodePtr<'mcx>> =
        ::mcx::PgVec::new_in(mcx);
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
    let alias_box = ::mcx::alloc_in(mcx, alias)?;
    // childrte->alias = childrte->eref = makeAlias(...). The two share the value;
    // clone the second.
    let eref_alias = {
        let a = &*alias_box;
        let mut colnames2: ::mcx::PgVec<'mcx, ::nodes::nodes::NodePtr<'mcx>> =
            ::mcx::PgVec::new_in(mcx);
        for cn in a.colnames.iter() {
            colnames2.push(make_string_node(mcx, &node_string_value(cn))?);
        }
        makefuncs::make_alias(mcx, &parent_aliasname, colnames2)?
    };
    childrte.alias = Some(alias_box);
    childrte.eref = Some(::mcx::alloc_in(mcx, eref_alias)?);

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

        // Non-leaf partitions don't need any row identity info.
        if child_relkind != RELKIND_PARTITIONED_TABLE {
            root.leaf_result_relids =
                bms::relids_add_member::call(root.leaf_result_relids.take(), child_rt_index as i32);

            // If we have any child target relations, assume they all need to
            // generate a junk "tableoid" column. (If only one child survives
            // pruning, we wouldn't really need this, but it's not worth thrashing
            // about to avoid it.)  rrvar = makeVar(childRTindex,
            // TableOidAttributeNumber, OIDOID, -1, InvalidOid, 0).
            let rrvar = makefuncs::make_var(
                child_rt_index as i32,
                TableOidAttributeNumber,
                OIDOID,
                -1,
                InvalidOid,
                0,
            );
            // add_row_identity_var(root, rrvar, childRTindex, "tableoid").
            // result_relation = root->parse->resultRelation (threaded through the
            // seam because the QueryId resolves only via the run).
            let result_relation = run.resolve(root.parse).resultRelation as Index;
            appendinfo_seams::add_row_identity_var::call(
                root,
                rrvar,
                child_rt_index,
                "tableoid",
                result_relation,
            )?;

            // Register any row-identity columns needed by this child.
            // add_row_identity_columns(root, childRTindex, childrte, childrel).
            // The C reads command_type from root->parse->commandType, relid/relkind
            // from childrte, and the foreign-table delete-row-trigger predicate from
            // childrel->trigdesc; the seam takes them pre-resolved.
            let command_type = run.resolve(root.parse).commandType;
            let has_delete_row_trigger = childrel
                .rd_trigdesc
                .as_deref()
                .map(|td| td.trig_delete_after_row || td.trig_delete_before_row)
                .unwrap_or(false);
            appendinfo_seams::add_row_identity_columns::call(
                root,
                child_rt_index,
                command_type,
                child_oid,
                child_relkind,
                has_delete_row_trigger,
                result_relation,
            )?;
        }
    }

    Ok(child_rt_index)
}

/// `makeString(pstrdup(s))` as a value-node `NodePtr` — the parser's colname
/// representation (`Node::String`).
fn make_string_node<'mcx>(
    mcx: Mcx<'mcx>,
    s: &str,
) -> PgResult<::nodes::nodes::NodePtr<'mcx>> {
    let node = ::nodes::nodes::Node::mk_string(
        mcx,
        ::nodes::value::StringNode {
            sval: ::mcx::PgString::from_str_in(s, mcx)?,
        },
    )?;
    ::mcx::alloc_in(mcx, node)
}

/// Read the `String` value out of a value-node `NodePtr` (the parser's
/// `makeString` colname). Empty when the node is not a String.
fn node_string_value(np: &::nodes::nodes::NodePtr<'_>) -> alloc::string::String {
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
    let perminfo_idx = parser_relation_seams::get_rte_permission_info::call(
        &run.resolve(root.parse).rteperminfos,
        rte,
    )?;
    // Convert the RTEPermissionInfo bitmapset (`::nodes::Bitmapset`, words:
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
    let extra = plancat::get_dependent_generated_columns(
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

    let mut childquals: Vec<::pathnodes::RinfoId> = Vec::new();
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
                // Deep-copy via `clone_in` — the derived `Expr::clone` panics on
                // an owned-subtree child (e.g. a subquery qual carrying a SubLink).
                root.node(rinfo.clause).clone_in(mcx)?,
                rinfo.is_pushed_down,
                rinfo.has_clone,
                rinfo.is_clone,
                rinfo.security_level,
            )
        };

        let childqual = adjust_appendrel_attrs_in(mcx, root, clause, &appinfos)?;
        let childqual =
            clauses::fold::eval_const_expressions(mcx, childqual)?;

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
            // clone_in: the derived `Expr::clone` panics on an owned-subtree
            // child (a child qual may carry a SubPlan, e.g. an EXISTS in an OR).
            let node = ::nodes::nodes::Node::mk_expr(run.mcx(), onecq.clone_in(run.mcx())?)?;
            let pseudoconstant = !vars::var::contain_vars_of_level(&node, 0)
                && !clauses::grounded::contain_volatile_functions(Some(
                    &onecq,
                ))?;
            if pseudoconstant {
                // Tell createplan.c to check for gating quals.
                root.hasPseudoConstantQuals = true;
            }

            let childrinfo = joininfo::restrictinfo::make_restrictinfo(
                mcx,
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

            // Proven always false / always true?  Pass the child RestrictInfo so
            // the has_clone/is_clone guard and orclause OR-recursion apply (per
            // inherit.c:910/913, which pass childrinfo).
            if joinext::restriction_is_always_false::call(root, childrinfo) {
                return Ok(false);
            }
            if joinext::restriction_is_always_true::call(root, childrinfo) {
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
            let ri = joininfo::restrictinfo::make_restrictinfo(
                mcx,
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
fn const_bool_is_true(c: &::nodes::primnodes::Const) -> bool {
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
) -> PgResult<Vec<Vec<Expr<'mcx>>>> {
    let rte = planner_rt_fetch(run, root, child_rti);
    let mut out: Vec<Vec<Expr<'mcx>>> = Vec::new();
    for qualset_np in rte.securityQuals.iter() {
        // Each securityQuals element is itself a List of Exprs (a qual set).
        let mut set: Vec<Expr<'mcx>> = Vec::new();
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
