//! pathkeys.c. Canonicalization makes PathKey value equality C's pointer
//! equality; EC machinery lives in equivclass.rs.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::parsenodes::SortGroupClause;
use types_nodes::Node;
use types_pathnodes::{EcId, PathKey, COMPARE_EQ, COMPARE_GT, COMPARE_LT};

pub use types_pathnodes::{
    compare_pathkeys, pathkeys_contained_in, pathkeys_count_contained_in, PathKeysComparison,
};

use crate::run::PlannerRun;

pub fn get_sortgroupclause_expr<'mcx>(
    sortcl: &SortGroupClause,
    tlist: &NodeList<'mcx>,
) -> Node<'mcx> {
    for tle_node in tlist {
        let tle = tle_node
            .as_target_entry()
            .expect("tlist holds TargetEntries");
        if tle.ressortgroupref == sortcl.tleSortGroupRef {
            return tle.expr;
        }
    }
    panic!("ORDER/GROUP BY expression not found in targetlist");
}

// The _extended form with remove_redundant/remove_group_rtindex/
// set_ec_sortref all false.
pub fn make_pathkeys_for_sortclauses<'mcx>(
    run: &mut PlannerRun<'mcx>,
    sortclauses: &NodeList<'mcx>,
    tlist: &NodeList<'mcx>,
) -> PgResult<PgVec<'mcx, PathKey>> {
    let mut pathkeys: PgVec<'mcx, PathKey> = PgVec::new_in(run.mcx);
    for sc_node in sortclauses {
        let sortcl = sc_node
            .as_sort_group_clause()
            .expect("sortClause holds SortGroupClauses");
        let sortkey = get_sortgroupclause_expr(sortcl, tlist);
        assert!(
            sortcl.sortop != 0,
            "make_pathkeys_for_sortclauses: unsortable clause"
        );
        let pathkey = make_pathkey_from_sortop(
            run,
            sortkey,
            sortcl.sortop,
            sortcl.reverse_sort,
            sortcl.nulls_first,
            sortcl.tleSortGroupRef,
        )?;
        if !pathkey_is_redundant(run, pathkey, &pathkeys) {
            pathkeys.push(pathkey);
        }
    }
    Ok(pathkeys)
}

/// The `_extended` form over interned SortGroupClause ids (GROUP BY/DISTINCT
/// lanes); returns (pathkeys, sortable). `remove_group_rtindex`: the
/// groupClause sits logically below the grouping step, so its sort
/// expressions shed the grouping RT index first (pathkeys.c:1405-1412).
pub fn make_pathkeys_for_sortclauses_extended<'mcx>(
    run: &mut PlannerRun<'mcx>,
    sortclauses: &mut PgVec<'mcx, types_pathnodes::NodeId>,
    tlist: &NodeList<'mcx>,
    remove_redundant: bool,
    remove_group_rtindex: bool,
    set_ec_sortref: bool,
) -> PgResult<(PgVec<'mcx, PathKey>, bool)> {
    let mut pathkeys: PgVec<'mcx, PathKey> = PgVec::new_in(run.mcx);
    let mut sortable = true;
    let mut i = 0;
    while i < sortclauses.len() {
        let sortcl = *run
            .root
            .expr_node(sortclauses[i])
            .as_sort_group_clause()
            .expect("sortclause cell");
        let mut sortkey = get_sortgroupclause_expr(&sortcl, tlist);
        if sortcl.sortop == 0 {
            sortable = false;
            i += 1;
            continue;
        }
        if remove_group_rtindex {
            assert!(run.root.group_rtindex > 0);
            sortkey = crate::flatten_group::strip_group_nulling(
                run.mcx,
                sortkey,
                run.root.group_rtindex,
            )?
            .unwrap_or(sortkey);
        }
        let pathkey = make_pathkey_from_sortop(
            run,
            sortkey,
            sortcl.sortop,
            sortcl.reverse_sort,
            sortcl.nulls_first,
            sortcl.tleSortGroupRef,
        )?;
        if set_ec_sortref {
            let ec = pathkey.pk_eclass.expect("canonical pathkey has an eclass");
            if run.root.ec(ec).ec_sortref == 0 {
                run.root.ec_mut(ec).ec_sortref = sortcl.tleSortGroupRef;
            }
        }
        if !pathkey_is_redundant(run, pathkey, &pathkeys) {
            pathkeys.push(pathkey);
            i += 1;
        } else if remove_redundant {
            sortclauses.remove(i);
        } else {
            i += 1;
        }
    }
    Ok((pathkeys, sortable))
}

pub struct GroupByOrdering<'mcx> {
    pub pathkeys: PgVec<'mcx, PathKey>,
    pub clauses: PgVec<'mcx, types_pathnodes::NodeId>,
}

// group_keys_reorder_by_pathkeys (pathkeys.c): clauses matched by ec_sortref.
pub(crate) fn group_keys_reorder_by_pathkeys<'mcx>(
    run: &PlannerRun<'mcx>,
    path_pathkeys: &[PathKey],
    group_pathkeys: &mut PgVec<'mcx, PathKey>,
    group_clauses: &mut PgVec<'mcx, types_pathnodes::NodeId>,
    num_groupby_pathkeys: usize,
) -> usize {
    if path_pathkeys.is_empty() || group_pathkeys.is_empty() {
        return 0;
    }
    let mcx = run.mcx;
    let mut new_pathkeys: PgVec<'mcx, PathKey> = PgVec::new_in(mcx);
    let mut new_clauses: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    // Match only within the leading num_groupby_pathkeys of group_pathkeys:
    // the tail holds aggregate-ORDER-BY pathkeys whose ec_sortref does not
    // reference the query targetlist (pathkeys.c:380-392).
    let grouping_pathkeys = &group_pathkeys[..num_groupby_pathkeys.min(group_pathkeys.len())];
    for (i, pk) in path_pathkeys.iter().enumerate() {
        if i >= num_groupby_pathkeys || !grouping_pathkeys.contains(pk) {
            break;
        }
        let ec = pk.pk_eclass.expect("canonical pathkey has an eclass");
        let sortref = run.root.ec(ec).ec_sortref;
        // Since C commit 1349d27 a pathkey from the underlying node can lack a
        // sortref or a matching clause in processed_groupClause; both end the
        // usable prefix (pathkeys.c:404-427).
        if sortref == 0 {
            break;
        }
        let Some(sgc) = group_clauses.iter().copied().find(|&id| {
            run.root
                .expr_node(id)
                .as_sort_group_clause()
                .expect("group clause cell")
                .tleSortGroupRef
                == sortref
        }) else {
            break;
        };
        debug_assert!(
            run.root.expr_node(sgc).as_sort_group_clause().expect("group clause cell").sortop != 0
        );
        new_pathkeys.push(*pk);
        new_clauses.push(sgc);
    }
    let n = new_pathkeys.len();
    for pk in group_pathkeys.iter() {
        if !new_pathkeys.contains(pk) {
            new_pathkeys.push(*pk);
        }
    }
    for &c in group_clauses.iter() {
        if !new_clauses.contains(&c) {
            new_clauses.push(c);
        }
    }
    *group_pathkeys = new_pathkeys;
    *group_clauses = new_clauses;
    n
}

/// C `get_useful_group_keys_orderings`.
pub fn get_useful_group_keys_orderings<'mcx>(
    run: &mut PlannerRun<'mcx>,
    path_pathkeys: &[PathKey],
) -> PgVec<'mcx, GroupByOrdering<'mcx>> {
    let mcx = run.mcx;
    let mut infos: PgVec<'mcx, GroupByOrdering<'mcx>> = PgVec::new_in(mcx);
    infos.push(GroupByOrdering {
        pathkeys: crate::relnode::pgvec_clone_shallow(mcx, &run.root.group_pathkeys),
        clauses: crate::relnode::pgvec_clone_shallow(mcx, &run.root.processed_groupClause),
    });
    if !crate::gucs::enable_group_by_reordering() {
        return infos;
    }
    // Grouping sets have their own, more complex ordering logic.
    if !run.parse().groupingSets.is_nil() {
        return infos;
    }
    if !path_pathkeys.is_empty() && !pathkeys_contained_in(path_pathkeys, &run.root.group_pathkeys)
    {
        let mut pathkeys = crate::relnode::pgvec_clone_shallow(mcx, &run.root.group_pathkeys);
        let mut clauses = crate::relnode::pgvec_clone_shallow(mcx, &run.root.processed_groupClause);
        let num = run.root.num_groupby_pathkeys as usize;
        let n =
            group_keys_reorder_by_pathkeys(run, path_pathkeys, &mut pathkeys, &mut clauses, num);
        if n > 0
            && (crate::gucs::enable_incremental_sort() || n == num)
            && compare_pathkeys(&pathkeys, &run.root.group_pathkeys) != PathKeysComparison::Equal
        {
            infos.push(GroupByOrdering { pathkeys, clauses });
        }
    }
    infos
}

// build_expression_pathkey (pathkeys.c).
pub fn build_expression_pathkey<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Node<'mcx>,
    opno: u32,
    rel: &types_pathnodes::Relids<'mcx>,
    create_it: bool,
) -> PgResult<PgVec<'mcx, PathKey>> {
    let (opfamily, opcintype, cmptype) = lsyscache::amop::get_ordering_op_properties(opno)?
        .unwrap_or_else(|| panic!("operator {opno} is not a valid ordering operator"));
    let collation = expr_collation(expr);
    let cpathkey = make_pathkey_from_sortinfo(
        run,
        expr,
        opfamily,
        opcintype,
        collation,
        cmptype == COMPARE_GT,
        cmptype == COMPARE_GT,
        0,
        rel,
        create_it,
    )?;
    let mut pathkeys = PgVec::new_in(run.mcx);
    if let Some(pk) = cpathkey {
        pathkeys.push(pk);
    }
    Ok(pathkeys)
}

pub fn make_pathkey_from_sortop<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Node<'mcx>,
    ordering_op: u32,
    reverse_sort: bool,
    nulls_first: bool,
    sortref: u32,
) -> PgResult<PathKey> {
    let (opfamily, opcintype, _cmptype) = lsyscache::amop::get_ordering_op_properties(ordering_op)?
        .unwrap_or_else(|| panic!("operator {ordering_op} is not a valid ordering operator"));
    let collation = expr_collation(expr);
    Ok(make_pathkey_from_sortinfo(
        run,
        expr,
        opfamily,
        opcintype,
        collation,
        reverse_sort,
        nulls_first,
        sortref,
        &crate::relnode::RELIDS_UNSET,
        true,
    )?
    .expect("create_it pathkey"))
}

#[allow(clippy::too_many_arguments)]
fn make_pathkey_from_sortinfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Node<'mcx>,
    opfamily: u32,
    opcintype: u32,
    collation: u32,
    reverse_sort: bool,
    nulls_first: bool,
    sortref: u32,
    rel: &types_pathnodes::Relids<'mcx>,
    create_it: bool,
) -> PgResult<Option<PathKey>> {
    let cmptype = if reverse_sort { COMPARE_GT } else { COMPARE_LT };
    let equality_op = lsyscache::amop::get_opfamily_member_for_cmptype(
        opfamily, opcintype, opcintype, COMPARE_EQ,
    )?;
    assert!(
        equality_op != 0,
        "missing operator {COMPARE_EQ}({opcintype},{opcintype}) in opfamily {opfamily}"
    );
    let opfamilies = lsyscache::amop::get_mergejoin_opfamilies(run.mcx, equality_op)?;
    assert!(
        !opfamilies.is_empty(),
        "could not find opfamilies for equality operator {equality_op}"
    );
    let Some(eclass) = crate::equivclass::get_eclass_for_sort_expr(
        run,
        expr,
        &opfamilies,
        opcintype,
        collation,
        sortref,
        rel,
        create_it,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(make_canonical_pathkey(
        run,
        eclass,
        opfamily,
        cmptype,
        nulls_first,
    )))
}

// pgrcolumnar sorted-scan pathkeys (allpaths::create_pgrcolumnar_sorted_paths):
// default ascending nulls-last pathkey for an admitted pgrcolumnar column Var;
// EC lookup only (create_it=false — an EC nobody asked for is a pathkey
// nobody can use, the index-path discipline).
pub fn make_pathkey_from_sortinfo_existing<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Node<'mcx>,
    rel: &types_pathnodes::Relids<'mcx>,
) -> PgResult<Option<PathKey>> {
    let Some(v) = expr.as_var() else { return Ok(None) };
    use types_core::catalog::{DATEOID, INT2OID, INT4OID, INT8OID, TEXTOID, TIMESTAMPOID,
        VARCHAROID};
    // Default btree "<" operators (fixed catalog oids).
    let lt_op: u32 = match v.vartype {
        INT2OID => 95,
        INT4OID => 97,
        INT8OID => 412,
        DATEOID => 1095,
        TIMESTAMPOID => 2062,
        TEXTOID | VARCHAROID => 664,
        _ => return Ok(None),
    };
    let Some((opfamily, opcintype, _cmptype)) =
        lsyscache::amop::get_ordering_op_properties(lt_op)?
    else {
        return Ok(None);
    };
    make_pathkey_from_sortinfo(
        run,
        expr,
        opfamily,
        opcintype,
        expr_collation(expr),
        false,
        false,
        0,
        rel,
        false,
    )
}

// build_index_pathkeys (pathkeys.c): key columns of an ordered (btree) index;
// caller runs truncate_useless_pathkeys.
pub fn build_index_pathkeys<'mcx>(
    run: &mut PlannerRun<'mcx>,
    index: &types_pathnodes::IndexOptInfo<'mcx>,
    scandir: types_pathnodes::ScanDirection,
) -> PgResult<PgVec<'mcx, PathKey>> {
    let mut retval: PgVec<'mcx, PathKey> = PgVec::new_in(run.mcx);
    if index.sortopfamily.is_empty() {
        return Ok(retval);
    }
    for i in 0..index.nkeycolumns as usize {
        let indexkey = *run.root.expr_node(index.indextlist[i]);
        let indexkey = indexkey
            .as_target_entry()
            .expect("indextlist holds TargetEntries")
            .expr;
        let (reverse_sort, nulls_first) = if scandir == types_pathnodes::BackwardScanDirection {
            (!index.reverse_sort[i], !index.nulls_first[i])
        } else {
            (index.reverse_sort[i], index.nulls_first[i])
        };
        let index_relids = types_pathnodes::relids::relids_copy(
            run.mcx,
            &run.root.rel(index.rel.expect("index has a rel")).relids,
        );
        let cpathkey = make_pathkey_from_sortinfo(
            run,
            indexkey,
            index.sortopfamily[i],
            index.opcintype[i],
            index.indexcollations[i],
            reverse_sort,
            nulls_first,
            0,
            &index_relids,
            false,
        )?;
        match cpathkey {
            Some(pk) => {
                if !pathkey_is_redundant(run, pk, &retval) {
                    retval.push(pk);
                }
            }
            None => {
                if crate::indxpath::indexcol_is_bool_constant_for_query(run, index, i)? {
                    continue;
                }
                break;
            }
        }
    }
    Ok(retval)
}

// build_partition_pathkeys (pathkeys.c): ordering induced by the partition
// bounds; NULL partition sorts last, so backward scans get nulls_first.
pub fn build_partition_pathkeys<'mcx>(
    run: &mut PlannerRun<'mcx>,
    partrel: types_pathnodes::RelId,
    backward: bool,
) -> PgResult<(PgVec<'mcx, PathKey>, bool)> {
    let mut retval: PgVec<'mcx, PathKey> = PgVec::new_in(run.mcx);
    let partnatts = run
        .root
        .rel(partrel)
        .part_scheme
        .as_ref()
        .expect("build_partition_pathkeys on an unpartitioned rel")
        .partnatts as usize;
    for i in 0..partnatts {
        let (key_col, opfamily, opcintype, collation) = {
            let rel = run.root.rel(partrel);
            let scheme = rel.part_scheme.as_ref().unwrap();
            (
                *run.root.expr_node(rel.partexprs[i][0]),
                scheme.partopfamily[i],
                scheme.partopcintype[i],
                scheme.partcollation[i],
            )
        };
        let part_relids =
            types_pathnodes::relids::relids_copy(run.mcx, &run.root.rel(partrel).relids);
        let cpathkey = make_pathkey_from_sortinfo(
            run, key_col, opfamily, opcintype, collation, backward, backward, 0, &part_relids,
            false,
        )?;
        match cpathkey {
            Some(pk) => {
                if !pathkey_is_redundant(run, pk, &retval) {
                    retval.push(pk);
                }
            }
            None => {
                if !partkey_is_bool_constant_for_query(run, partrel, i) {
                    return Ok((retval, true));
                }
            }
        }
    }
    Ok((retval, false))
}

// partkey_is_bool_constant_for_query (pathkeys.c).
fn partkey_is_bool_constant_for_query(
    run: &PlannerRun<'_>,
    partrel: types_pathnodes::RelId,
    partkeycol: usize,
) -> bool {
    const BOOL_BTREE_FAM_OID: u32 = 424;
    const BOOL_HASH_FAM_OID: u32 = 2222;
    let rel = run.root.rel(partrel);
    let opfamily = rel.part_scheme.as_ref().unwrap().partopfamily[partkeycol];
    if opfamily != BOOL_BTREE_FAM_OID && opfamily != BOOL_HASH_FAM_OID {
        return false;
    }
    let partexpr = *run.root.expr_node(rel.partexprs[partkeycol][0]);
    for &rid in rel.baserestrictinfo.iter() {
        let rinfo = run.root.rinfo(rid);
        if rinfo.pseudoconstant {
            continue;
        }
        let clause = *run.root.expr_node(rinfo.clause);
        if types_nodes::equal(partexpr, clause) {
            return true;
        }
        if clauses::is_notclause(clause) {
            let arg = clause.as_bool_expr().unwrap().args.nth(0);
            if types_nodes::equal(partexpr, arg) {
                return true;
            }
        }
    }
    false
}

// append_pathkeys (pathkeys.c).
pub fn append_pathkeys<'mcx>(
    run: &PlannerRun<'mcx>,
    target: &mut PgVec<'mcx, PathKey>,
    source: &[PathKey],
) {
    debug_assert!(!target.is_empty());
    for &pk in source {
        if !pathkey_is_redundant(run, pk, target) {
            target.push(pk);
        }
    }
}

// get_cheapest_parallel_safe_total_inner (pathkeys.c).
pub fn get_cheapest_parallel_safe_total_inner(
    run: &PlannerRun<'_>,
    paths: &[types_pathnodes::PathId],
) -> Option<types_pathnodes::PathId> {
    paths.iter().copied().find(|&pid| {
        let path = run.root.path(pid).base();
        path.parallel_safe && path.param_info.is_none()
    })
}

pub use types_pathnodes::run::{SubPathKeyDesc, SubPathKeyMember, SubTle};

// Subquery pathkeys reference the subroot's EC arena; this materializes what
// convert_subquery_pathkeys reads from them so the conversion can run against
// the outer root (C follows pointers across PlannerInfos instead).
pub fn extract_subquery_pathkey_descs<'mcx>(
    run: &PlannerRun<'mcx>,
    subpath: types_pathnodes::PathId,
) -> PgVec<'mcx, SubPathKeyDesc<'mcx>> {
    let mcx = run.mcx;
    let mut descs: PgVec<'mcx, SubPathKeyDesc<'mcx>> = PgVec::new_in(mcx);
    for pk in run.root.path(subpath).base().pathkeys.iter() {
        let ec_id = pk.pk_eclass.expect("canonical pathkey has an eclass");
        let ec = run.root.ec(ec_id);
        let mut members: PgVec<'mcx, SubPathKeyMember<'mcx>> = PgVec::new_in(mcx);
        for &em_id in ec.ec_members.iter() {
            let em = run.root.em(em_id);
            debug_assert!(!em.em_is_child);
            members.push(SubPathKeyMember {
                expr: *run.root.expr_node(em.em_expr),
                datatype: em.em_datatype,
            });
        }
        descs.push(SubPathKeyDesc {
            has_volatile: ec.ec_has_volatile,
            sortref: ec.ec_sortref,
            members,
            opfamilies: crate::relnode::pgvec_clone_shallow(mcx, &ec.ec_opfamilies),
            collation: ec.ec_collation,
            pk_opfamily: pk.pk_opfamily,
            pk_cmptype: pk.pk_cmptype,
            pk_nulls_first: pk.pk_nulls_first,
        });
    }
    descs
}

// make_tlist_from_pathtarget (tlist.c) over the subpath's pathtarget, reduced
// to what find_var_for_subquery_tle consumes (never resjunk).
pub fn extract_subquery_tlist<'mcx>(
    run: &PlannerRun<'mcx>,
    subpath: types_pathnodes::PathId,
) -> PgVec<'mcx, SubTle<'mcx>> {
    let mcx = run.mcx;
    let target = run.root.path_pathtarget(subpath);
    let mut tlist: PgVec<'mcx, SubTle<'mcx>> = PgVec::new_in(mcx);
    for (i, &eid) in target.exprs.iter().enumerate() {
        tlist.push(SubTle {
            expr: *run.root.expr_node(eid),
            resno: i as i32 + 1,
            sortgroupref: target.sortgrouprefs.get(i).copied().unwrap_or(0),
        });
    }
    tlist
}

// find_var_for_subquery_tle (pathkeys.c).
fn find_var_for_subquery_tle<'mcx>(
    run: &PlannerRun<'mcx>,
    rel: types_pathnodes::RelId,
    tle: &SubTle<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    let mcx = run.mcx;
    for &id in run.root.rel_reltarget(rel).exprs.iter() {
        let node = *run.root.expr_node(id);
        let Some(var) = node.as_var() else { continue };
        debug_assert!(var.varno as u32 == run.root.rel(rel).relid);
        if var.varattno as i32 == tle.resno {
            let copy = types_nodes::primnodes::Var {
                varnullingrels: var.varnullingrels.clone_in(mcx)?,
                ..*var
            };
            return Ok(Some(Node::mk(mcx, copy)?));
        }
    }
    Ok(None)
}

// convert_subquery_pathkeys (pathkeys.c), over extracted subroot descriptors.
pub fn convert_subquery_pathkeys<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: types_pathnodes::RelId,
    sub_pathkeys: &[SubPathKeyDesc<'mcx>],
    subquery_tlist: &[SubTle<'mcx>],
) -> PgResult<PgVec<'mcx, PathKey>> {
    let mcx = run.mcx;
    let outer_query_keys = run.root.query_pathkeys.len();
    let rel_relids = types_pathnodes::relids::relids_copy(mcx, &run.root.rel(rel).relids);
    let mut retval: PgVec<'mcx, PathKey> = PgVec::new_in(mcx);

    for desc in sub_pathkeys {
        let mut best_pathkey: Option<PathKey> = None;
        if desc.has_volatile {
            assert!(desc.sortref != 0, "volatile EquivalenceClass has no sortref");
            let tle = subquery_tlist
                .iter()
                .find(|t| t.sortgroupref == desc.sortref)
                .expect("volatile pathkey sortref has a tlist entry");
            if let Some(outer_var) = find_var_for_subquery_tle(run, rel, tle)? {
                debug_assert_eq!(desc.members.len(), 1);
                let sub_datatype = desc.members[0].datatype;
                if let Some(outer_ec) = crate::equivclass::get_eclass_for_sort_expr(
                    run,
                    outer_var,
                    &desc.opfamilies,
                    sub_datatype,
                    desc.collation,
                    0,
                    &rel_relids,
                    false,
                )? {
                    best_pathkey = Some(make_canonical_pathkey(
                        run,
                        outer_ec,
                        desc.pk_opfamily,
                        desc.pk_cmptype,
                        desc.pk_nulls_first,
                    ));
                }
            }
        } else {
            let mut best_score = -1i64;
            for m in desc.members.iter() {
                let (sub_expr, sub_expr_type) = (m.expr, m.datatype);
                for tle in subquery_tlist {
                    let Some(outer_var) = find_var_for_subquery_tle(run, rel, tle)? else {
                        continue;
                    };
                    let tle_expr = crate::equivclass::canonicalize_ec_expression(
                        mcx,
                        tle.expr,
                        sub_expr_type,
                        desc.collation,
                    )?;
                    if !types_nodes::equal(tle_expr, sub_expr) {
                        continue;
                    }
                    let Some(outer_ec) = crate::equivclass::get_eclass_for_sort_expr(
                        run,
                        outer_var,
                        &desc.opfamilies,
                        sub_expr_type,
                        desc.collation,
                        0,
                        &rel_relids,
                        false,
                    )?
                    else {
                        continue;
                    };
                    let outer_pk = make_canonical_pathkey(
                        run,
                        outer_ec,
                        desc.pk_opfamily,
                        desc.pk_cmptype,
                        desc.pk_nulls_first,
                    );
                    let mut score = run.root.ec(outer_ec).ec_members.len() as i64 - 1;
                    if retval.len() < outer_query_keys
                        && run.root.query_pathkeys[retval.len()] == outer_pk
                    {
                        score += 1;
                    }
                    if score > best_score {
                        best_pathkey = Some(outer_pk);
                        best_score = score;
                    }
                }
            }
        }
        let Some(best) = best_pathkey else { break };
        if !pathkey_is_redundant(run, best, &retval) {
            retval.push(best);
        }
    }
    Ok(retval)
}

pub fn make_canonical_pathkey(
    run: &mut PlannerRun<'_>,
    eclass: EcId,
    opfamily: u32,
    cmptype: i32,
    nulls_first: bool,
) -> PathKey {
    assert!(
        run.root.ec_merging_done,
        "too soon to build canonical pathkeys"
    );
    debug_assert!(run.root.ec(eclass).ec_merged.is_none());
    for pk in run.root.canon_pathkeys.iter() {
        if pk.pk_eclass == Some(eclass)
            && pk.pk_opfamily == opfamily
            && pk.pk_cmptype == cmptype
            && pk.pk_nulls_first == nulls_first
        {
            return *pk;
        }
    }
    let pk = PathKey {
        pk_eclass: Some(eclass),
        pk_opfamily: opfamily,
        pk_cmptype: cmptype,
        pk_nulls_first: nulls_first,
    };
    run.root.canon_pathkeys.push(pk);
    pk
}

pub fn pathkey_is_redundant(run: &PlannerRun<'_>, new_pathkey: PathKey, pathkeys: &[PathKey]) -> bool {
    // EC_MUST_BE_REDUNDANT: a const EC admits only one key value.
    if run.root.ec(new_pathkey.pk_eclass.unwrap()).ec_has_const {
        return true;
    }
    pathkeys
        .iter()
        .any(|old| old.pk_eclass == new_pathkey.pk_eclass)
}

pub fn initialize_mergeclause_eclasses(
    run: &mut PlannerRun<'_>,
    rinfo: types_pathnodes::RinfoId,
) -> PgResult<()> {
    debug_assert!(!run.root.rinfo(rinfo).mergeopfamilies.is_empty());
    debug_assert!(run.root.rinfo(rinfo).left_ec.is_none());
    debug_assert!(run.root.rinfo(rinfo).right_ec.is_none());
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let o = clause.as_op_expr().expect("mergeclause is an OpExpr");
    let (lefttype, righttype) = lsyscache::op_input_types(o.opno)?;
    let opfamilies =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rinfo(rinfo).mergeopfamilies);
    let left_ec = crate::equivclass::get_eclass_for_sort_expr(
        run,
        o.args.nth(0),
        &opfamilies,
        lefttype,
        o.inputcollid,
        0,
        &crate::relnode::RELIDS_UNSET,
        true,
    )?;
    let right_ec = crate::equivclass::get_eclass_for_sort_expr(
        run,
        o.args.nth(1),
        &opfamilies,
        righttype,
        o.inputcollid,
        0,
        &crate::relnode::RELIDS_UNSET,
        true,
    )?;
    let r = run.root.rinfo_mut(rinfo);
    r.left_ec = left_ec;
    r.right_ec = right_ec;
    Ok(())
}

pub fn update_mergeclause_eclasses(
    run: &mut PlannerRun<'_>,
    rinfo: types_pathnodes::RinfoId,
) -> PgResult<()> {
    debug_assert!(!run.root.rinfo(rinfo).mergeopfamilies.is_empty());
    let left = run
        .root
        .rinfo(rinfo)
        .left_ec
        .expect("mergeclause left_ec set");
    let right = run
        .root
        .rinfo(rinfo)
        .right_ec
        .expect("mergeclause right_ec set");
    let left = run.root.ec_canonical(left);
    let right = run.root.ec_canonical(right);
    let r = run.root.rinfo_mut(rinfo);
    r.left_ec = Some(left);
    r.right_ec = Some(right);
    Ok(())
}

fn mergeclause_outer_inner_ecs(
    run: &PlannerRun<'_>,
    rinfo: types_pathnodes::RinfoId,
) -> (Option<EcId>, Option<EcId>) {
    let ri = run.root.rinfo(rinfo);
    if ri.outer_is_left {
        (ri.left_ec, ri.right_ec)
    } else {
        (ri.right_ec, ri.left_ec)
    }
}

pub fn find_mergeclauses_for_outer_pathkeys<'mcx>(
    run: &mut PlannerRun<'mcx>,
    pathkeys: &[PathKey],
    restrictinfos: &[types_pathnodes::RinfoId],
) -> PgResult<PgVec<'mcx, types_pathnodes::RinfoId>> {
    let mut mergeclauses: PgVec<'mcx, types_pathnodes::RinfoId> = PgVec::new_in(run.mcx);
    for &rid in restrictinfos {
        update_mergeclause_eclasses(run, rid)?;
    }
    for pathkey in pathkeys {
        let mut matched = false;
        for &rid in restrictinfos {
            let (oec, _) = mergeclause_outer_inner_ecs(run, rid);
            if oec == pathkey.pk_eclass {
                mergeclauses.push(rid);
                matched = true;
            }
        }
        if !matched {
            break;
        }
    }
    Ok(mergeclauses)
}

pub fn select_outer_pathkeys_for_merge<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mergeclauses: &[types_pathnodes::RinfoId],
    joinrel: types_pathnodes::RelId,
) -> PgResult<PgVec<'mcx, PathKey>> {
    let mcx = run.mcx;
    let n_clauses = mergeclauses.len();
    let mut pathkeys: PgVec<'mcx, PathKey> = PgVec::new_in(mcx);
    if n_clauses == 0 {
        return Ok(pathkeys);
    }

    let mut ecs: PgVec<'mcx, EcId> = PgVec::new_in(mcx);
    let mut scores: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    for &rid in mergeclauses {
        update_mergeclause_eclasses(run, rid)?;
        let (oec, _) = mergeclause_outer_inner_ecs(run, rid);
        let oeclass = oec.expect("mergeclause has an outer EC");
        if ecs.contains(&oeclass) {
            continue;
        }
        let mut score = 0;
        for &em_id in run.root.ec(oeclass).ec_members.iter() {
            let em = run.root.em(em_id);
            debug_assert!(!em.em_is_child);
            if !em.em_is_const
                && !crate::relnode::relids_overlap(&em.em_relids, &run.root.rel(joinrel).relids)
            {
                score += 1;
            }
        }
        ecs.push(oeclass);
        scores.push(score);
    }

    if !run.root.query_pathkeys.is_empty() {
        let query_pathkeys = crate::relnode::pgvec_clone_shallow(mcx, &run.root.query_pathkeys);
        let mut matches = 0usize;
        let mut have_all = true;
        for qpk in query_pathkeys.iter() {
            let qec = qpk.pk_eclass.expect("canonical pathkey has an eclass");
            if ecs.contains(&qec) {
                matches += 1;
            } else {
                have_all = false;
                break;
            }
        }
        if have_all {
            pathkeys.extend(query_pathkeys.iter().copied());
            for qpk in query_pathkeys.iter() {
                let qec = qpk.pk_eclass.unwrap();
                if let Some(j) = ecs.iter().position(|&e| e == qec) {
                    scores[j] = -1;
                }
            }
        } else if matches == n_clauses {
            pathkeys.extend(query_pathkeys.iter().take(matches).copied());
            return Ok(pathkeys);
        }
    }

    loop {
        let mut best_j = 0usize;
        let mut best_score = scores[0];
        for j in 1..ecs.len() {
            if scores[j] > best_score {
                best_j = j;
                best_score = scores[j];
            }
        }
        if best_score < 0 {
            break;
        }
        let ec = ecs[best_j];
        scores[best_j] = -1;
        let opfamily = run.root.ec(ec).ec_opfamilies[0];
        let pathkey = make_canonical_pathkey(run, ec, opfamily, COMPARE_LT, false);
        debug_assert!(!pathkey_is_redundant(run, pathkey, &pathkeys));
        pathkeys.push(pathkey);
    }
    Ok(pathkeys)
}

pub fn make_inner_pathkeys_for_merge<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mergeclauses: &[types_pathnodes::RinfoId],
    outer_pathkeys: &[PathKey],
) -> PgResult<PgVec<'mcx, PathKey>> {
    let mut pathkeys: PgVec<'mcx, PathKey> = PgVec::new_in(run.mcx);
    let mut lastoeclass: Option<EcId> = None;
    let mut opathkey: Option<PathKey> = None;
    let mut lop = outer_pathkeys.iter();

    for &rid in mergeclauses {
        update_mergeclause_eclasses(run, rid)?;
        let (oeclass, ieclass) = mergeclause_outer_inner_ecs(run, rid);
        if oeclass != lastoeclass {
            let Some(&opk) = lop.next() else {
                panic!("too few pathkeys for mergeclauses");
            };
            opathkey = Some(opk);
            lastoeclass = opk.pk_eclass;
            assert!(
                oeclass == lastoeclass,
                "outer pathkeys do not match mergeclause"
            );
        }
        let opk = opathkey.unwrap();
        let pathkey = if ieclass == oeclass {
            opk
        } else {
            make_canonical_pathkey(
                run,
                ieclass.expect("mergeclause has an inner EC"),
                opk.pk_opfamily,
                opk.pk_cmptype,
                opk.pk_nulls_first,
            )
        };
        if !pathkey_is_redundant(run, pathkey, &pathkeys) {
            pathkeys.push(pathkey);
        }
    }
    Ok(pathkeys)
}

pub fn trim_mergeclauses_for_inner_pathkeys<'mcx>(
    run: &PlannerRun<'mcx>,
    mergeclauses: &[types_pathnodes::RinfoId],
    pathkeys: &[PathKey],
) -> PgVec<'mcx, types_pathnodes::RinfoId> {
    let mut new_mergeclauses: PgVec<'mcx, types_pathnodes::RinfoId> = PgVec::new_in(run.mcx);
    if pathkeys.is_empty() {
        return new_mergeclauses;
    }
    let mut lip = pathkeys.iter();
    let mut pathkey_ec = lip.next().unwrap().pk_eclass;
    let mut matched_pathkey = false;

    for &rid in mergeclauses {
        let (_, clause_ec) = mergeclause_outer_inner_ecs(run, rid);
        if clause_ec != pathkey_ec {
            if !matched_pathkey {
                break;
            }
            let Some(next) = lip.next() else {
                break;
            };
            pathkey_ec = next.pk_eclass;
            matched_pathkey = false;
        }
        if clause_ec == pathkey_ec {
            new_mergeclauses.push(rid);
            matched_pathkey = true;
        } else {
            break;
        }
    }
    new_mergeclauses
}

// build_join_pathkeys (pathkeys.c); FULL/RIGHT/RIGHT_ANTI (NIL result) are
// loud upstream of add_paths_to_joinrel.
pub fn build_join_pathkeys<'mcx>(
    run: &mut PlannerRun<'mcx>,
    joinrel: types_pathnodes::RelId,
    jointype: u32,
    outer_pathkeys: &[PathKey],
) -> PgResult<PgVec<'mcx, PathKey>> {
    if matches!(
        jointype,
        types_pathnodes::JOIN_FULL | types_pathnodes::JOIN_RIGHT | types_pathnodes::JOIN_RIGHT_ANTI
    ) {
        return Ok(PgVec::new_in(run.mcx));
    }
    truncate_useless_pathkeys(run, joinrel, outer_pathkeys)
}

pub fn truncate_useless_pathkeys<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: types_pathnodes::RelId,
    pathkeys: &[PathKey],
) -> PgResult<PgVec<'mcx, PathKey>> {
    let mut nuseful = pathkeys_useful_for_merging(run, rel, pathkeys)?;
    nuseful = nuseful.max(pathkeys_useful_for_ordering(run, pathkeys));
    nuseful = nuseful.max(pathkeys_useful_for_grouping(run, pathkeys));
    nuseful = nuseful.max(pathkeys_useful_for_distinct(run, pathkeys));
    nuseful = nuseful.max(pathkeys_useful_for_setop(run, pathkeys));
    let mut out: PgVec<'mcx, PathKey> = PgVec::new_in(run.mcx);
    out.extend(pathkeys.iter().take(nuseful).copied());
    Ok(out)
}

fn pathkeys_useful_for_merging(
    run: &mut PlannerRun<'_>,
    rel: types_pathnodes::RelId,
    pathkeys: &[PathKey],
) -> PgResult<usize> {
    let mut useful = 0usize;
    for pathkey in pathkeys {
        if !right_merge_direction(run, pathkey) {
            break;
        }
        let mut matched = false;
        if run.root.rel(rel).has_eclass_joins
            && crate::equivclass::eclass_useful_for_merging(
                run,
                pathkey.pk_eclass.expect("canonical pathkey has an eclass"),
                rel,
            )
        {
            matched = true;
        } else {
            let joininfo =
                crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel).joininfo);
            for &rid in joininfo.iter() {
                if run.root.rinfo(rid).mergeopfamilies.is_empty() {
                    continue;
                }
                update_mergeclause_eclasses(run, rid)?;
                let ri = run.root.rinfo(rid);
                if pathkey.pk_eclass == ri.left_ec || pathkey.pk_eclass == ri.right_ec {
                    matched = true;
                    break;
                }
            }
        }
        if matched {
            useful += 1;
        } else {
            break;
        }
    }
    Ok(useful)
}

fn right_merge_direction(run: &PlannerRun<'_>, pathkey: &PathKey) -> bool {
    for qpk in run.root.query_pathkeys.iter() {
        if pathkey.pk_eclass == qpk.pk_eclass && pathkey.pk_opfamily == qpk.pk_opfamily {
            return pathkey.pk_cmptype == qpk.pk_cmptype;
        }
    }
    pathkey.pk_cmptype == COMPARE_LT
}

fn pathkeys_useful_for_ordering(run: &PlannerRun<'_>, pathkeys: &[PathKey]) -> usize {
    pathkeys_count_contained_in(&run.root.query_pathkeys, pathkeys).1
}

fn pathkeys_useful_for_grouping(run: &PlannerRun<'_>, pathkeys: &[PathKey]) -> usize {
    if run.root.group_pathkeys.is_empty() {
        return 0;
    }
    let mut n = 0;
    for pathkey in pathkeys {
        if !run.root.group_pathkeys.contains(pathkey) {
            break;
        }
        n += 1;
    }
    n
}

fn pathkeys_useful_for_distinct(run: &PlannerRun<'_>, pathkeys: &[PathKey]) -> usize {
    if run.root.distinct_pathkeys.is_empty() {
        return 0;
    }
    let mut n = 0;
    for pathkey in pathkeys {
        if !run.root.distinct_pathkeys.contains(pathkey) {
            break;
        }
        n += 1;
    }
    n
}

fn pathkeys_useful_for_setop(run: &PlannerRun<'_>, pathkeys: &[PathKey]) -> usize {
    pathkeys_count_contained_in(&run.root.setop_pathkeys, pathkeys).1
}

// get_cheapest_path_for_pathkeys (pathkeys.c); partial paths never reach
// here.
pub fn get_cheapest_path_for_pathkeys<'mcx>(
    run: &PlannerRun<'mcx>,
    paths: &[types_pathnodes::PathId],
    pathkeys: &[PathKey],
    required_outer: &types_pathnodes::Relids<'mcx>,
    cost_criterion: crate::pathnode::CostSelector,
    require_parallel_safe: bool,
) -> Option<types_pathnodes::PathId> {
    let mut matched_path: Option<types_pathnodes::PathId> = None;
    for &pid in paths {
        let path = run.root.path(pid).base();
        if require_parallel_safe && !path.parallel_safe {
            continue;
        }
        if let Some(m) = matched_path {
            if crate::pathnode::compare_path_costs(run.root.path(m).base(), path, cost_criterion)
                <= 0
            {
                continue;
            }
        }
        if pathkeys_contained_in(pathkeys, &path.pathkeys)
            && types_pathnodes::relids::relids_is_subset(
                crate::pathnode::path_req_outer(path),
                required_outer,
            )
        {
            matched_path = Some(pid);
        }
    }
    matched_path
}

// get_cheapest_fractional_path_for_pathkeys (pathkeys.c); required_outer is
// always empty on this lane.
pub fn get_cheapest_fractional_path_for_pathkeys(
    run: &PlannerRun<'_>,
    paths: &[types_pathnodes::PathId],
    pathkeys: &[PathKey],
    fraction: f64,
) -> Option<types_pathnodes::PathId> {
    let mut matched_path: Option<types_pathnodes::PathId> = None;
    for &pid in paths {
        let path = run.root.path(pid).base();
        if let Some(m) = matched_path {
            if crate::pathnode::compare_fractional_path_costs(
                run.root.path(m).base(),
                path,
                fraction,
            ) <= 0
            {
                continue;
            }
        }
        if pathkeys_contained_in(pathkeys, &path.pathkeys) && path.param_info.is_none() {
            matched_path = Some(pid);
        }
    }
    matched_path
}

// exprCollation (nodeFuncs.c) over the sort-key shapes this lane carries.
pub fn expr_collation(node: Node<'_>) -> u32 {
    use types_nodes::NodeTag;
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().varcollid,
        NodeTag::T_Const => node.as_const().unwrap().constcollid,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resultcollid,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opcollid,
        NodeTag::T_DistinctExpr => node.as_distinct_expr().unwrap().opcollid,
        NodeTag::T_BooleanTest
        | NodeTag::T_RowExpr
        | NodeTag::T_BoolExpr
        | NodeTag::T_GroupingFunc
        | NodeTag::T_NullTest => 0,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funccollid,
        NodeTag::T_Param => node.as_param().unwrap().paramcollid,
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggcollid,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wincollid,
        NodeTag::T_SubPlan => {
            use types_nodes::primnodes::SubLinkType;
            let sp = node.as_sub_plan().unwrap();
            match sp.subLinkType {
                SubLinkType::EXPR_SUBLINK | SubLinkType::ARRAY_SUBLINK => sp.firstColCollation,
                // C: a MULTIEXPR SubPlan's dummy result is an uncollatable RECORD.
                SubLinkType::MULTIEXPR_SUBLINK => ::types_core::InvalidOid,
                _ => 0,
            }
        }
        NodeTag::T_AlternativeSubPlan => expr_collation(
            node.as_alternative_sub_plan()
                .unwrap()
                .subplans
                .first()
                .expect("alternatives"),
        ),
        NodeTag::T_SubLink => {
            use types_nodes::primnodes::SubLinkType;
            let sl = node.as_sub_link().unwrap();
            match sl.subLinkType {
                SubLinkType::EXPR_SUBLINK | SubLinkType::ARRAY_SUBLINK => {
                    let tent = sl
                        .subselect
                        .as_query()
                        .unwrap_or_else(|| {
                            panic!("cannot get collation for untransformed sublink")
                        })
                        .targetList
                        .first()
                        .expect("sublink tlist")
                        .as_target_entry()
                        .expect("tlist entry");
                    expr_collation(tent.expr)
                }
                _ => 0,
            }
        }
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casecollid,
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().collation,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resultcollid,
        NodeTag::T_CoerceToDomain => node.as_coerce_to_domain().unwrap().resultcollid,
        NodeTag::T_CoerceToDomainValue => node.as_coerce_to_domain_value().unwrap().collation,
        _ => nodes_core::expr_collation(node),
    }
}

// has_useful_pathkeys (pathkeys.c).
pub(crate) fn has_useful_pathkeys(run: &crate::run::PlannerRun<'_>, rel: types_pathnodes::RelId) -> bool {
    if !run.root.rel(rel).joininfo.is_empty() || run.root.rel(rel).has_eclass_joins {
        return true;
    }
    if !run.root.group_pathkeys.is_empty() {
        return true;
    }
    !run.root.query_pathkeys.is_empty()
}
