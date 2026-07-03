//! pathkeys.c + equivclass.c's get_eclass_for_sort_expr slice (EC merging is
//! the M2 join lane). Canonicalization makes PathKey value equality C's
//! pointer equality.

use mcx::PgVec;
use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::parsenodes::SortGroupClause;
use types_nodes::Node;
use types_pathnodes::{
    EcId, EquivalenceClass, EquivalenceMember, PathKey, COMPARE_EQ, COMPARE_GT, COMPARE_LT,
};

use crate::run::PlannerRun;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PathKeysComparison {
    Equal,
    Better1,
    Better2,
    Different,
}

pub fn compare_pathkeys(keys1: &[PathKey], keys2: &[PathKey]) -> PathKeysComparison {
    for (k1, k2) in keys1.iter().zip(keys2.iter()) {
        if k1 != k2 {
            return PathKeysComparison::Different;
        }
    }
    match keys1.len().cmp(&keys2.len()) {
        core::cmp::Ordering::Greater => PathKeysComparison::Better1,
        core::cmp::Ordering::Less => PathKeysComparison::Better2,
        core::cmp::Ordering::Equal => PathKeysComparison::Equal,
    }
}

// Returns (contained, n leading matches).
pub fn pathkeys_count_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> (bool, usize) {
    let mut n = 0;
    for (k1, k2) in keys1.iter().zip(keys2.iter()) {
        if k1 != k2 {
            return (false, n);
        }
        n += 1;
    }
    (n == keys1.len(), n)
}

pub fn pathkeys_contained_in(keys1: &[PathKey], keys2: &[PathKey]) -> bool {
    matches!(
        compare_pathkeys(keys1, keys2),
        PathKeysComparison::Equal | PathKeysComparison::Better2
    )
}

pub fn get_sortgroupclause_expr<'mcx>(
    sortcl: &SortGroupClause,
    tlist: &NodeList<'mcx>,
) -> Node<'mcx> {
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("tlist holds TargetEntries");
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
        let sortcl = sc_node.as_sort_group_clause().expect("sortClause holds SortGroupClauses");
        let sortkey = get_sortgroupclause_expr(sortcl, tlist);
        assert!(sortcl.sortop != 0, "make_pathkeys_for_sortclauses: unsortable clause");
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
/// lanes); returns (pathkeys, sortable). `remove_group_rtindex` is dead (no
/// RTE_GROUP on this lane, loud upstream).
pub fn make_pathkeys_for_sortclauses_extended<'mcx>(
    run: &mut PlannerRun<'mcx>,
    sortclauses: &mut PgVec<'mcx, types_pathnodes::NodeId>,
    tlist: &NodeList<'mcx>,
    remove_redundant: bool,
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
        let sortkey = get_sortgroupclause_expr(&sortcl, tlist);
        if sortcl.sortop == 0 {
            sortable = false;
            i += 1;
            continue;
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
fn group_keys_reorder_by_pathkeys<'mcx>(
    run: &PlannerRun<'mcx>,
    path_pathkeys: &[PathKey],
    group_pathkeys: &mut PgVec<'mcx, PathKey>,
    group_clauses: &mut PgVec<'mcx, types_pathnodes::NodeId>,
    num_groupby_pathkeys: usize,
) -> usize {
    if group_pathkeys.is_empty() || group_clauses.is_empty() {
        return 0;
    }
    let mcx = run.mcx;
    let mut new_pathkeys: PgVec<'mcx, PathKey> = PgVec::new_in(mcx);
    let mut new_clauses: PgVec<'mcx, types_pathnodes::NodeId> = PgVec::new_in(mcx);
    for (i, pk) in path_pathkeys.iter().enumerate() {
        if i >= num_groupby_pathkeys || !group_pathkeys.contains(pk) {
            break;
        }
        let ec = pk.pk_eclass.expect("canonical pathkey has an eclass");
        let sortref = run.root.ec(ec).ec_sortref;
        assert!(sortref > 0, "pathkey EC of a group clause has no sortref");
        let sgc = group_clauses
            .iter()
            .copied()
            .find(|&id| {
                run.root
                    .expr_node(id)
                    .as_sort_group_clause()
                    .expect("group clause cell")
                    .tleSortGroupRef
                    == sortref
            })
            .expect("group clause matching the pathkey sortref");
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

/// C `get_useful_group_keys_orderings` (grouping sets loud upstream).
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
    if !path_pathkeys.is_empty()
        && !pathkeys_contained_in(path_pathkeys, &run.root.group_pathkeys)
    {
        let mut pathkeys =
            crate::relnode::pgvec_clone_shallow(mcx, &run.root.group_pathkeys);
        let mut clauses =
            crate::relnode::pgvec_clone_shallow(mcx, &run.root.processed_groupClause);
        let num = run.root.num_groupby_pathkeys as usize;
        let n = group_keys_reorder_by_pathkeys(run, path_pathkeys, &mut pathkeys, &mut clauses, num);
        if n > 0
            && (crate::gucs::enable_incremental_sort() || n == num)
            && compare_pathkeys(&pathkeys, &run.root.group_pathkeys) != PathKeysComparison::Equal
        {
            infos.push(GroupByOrdering { pathkeys, clauses });
        }
    }
    infos
}

fn make_pathkey_from_sortop<'mcx>(
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
        run, expr, opfamily, opcintype, collation, reverse_sort, nulls_first, sortref, true,
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
    create_it: bool,
) -> PgResult<Option<PathKey>> {
    let cmptype = if reverse_sort { COMPARE_GT } else { COMPARE_LT };
    let equality_op =
        lsyscache::amop::get_opfamily_member_for_cmptype(opfamily, opcintype, opcintype, COMPARE_EQ)?;
    assert!(
        equality_op != 0,
        "missing operator {COMPARE_EQ}({opcintype},{opcintype}) in opfamily {opfamily}"
    );
    let opfamilies = lsyscache::amop::get_mergejoin_opfamilies(run.mcx, equality_op)?;
    assert!(!opfamilies.is_empty(), "could not find opfamilies for equality operator {equality_op}");
    let Some(eclass) =
        get_eclass_for_sort_expr(run, expr, &opfamilies, opcintype, collation, sortref, create_it)?
    else {
        return Ok(None);
    };
    Ok(Some(make_canonical_pathkey(run, eclass, opfamily, cmptype, nulls_first)))
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
        let indexkey = indexkey.as_target_entry().expect("indextlist holds TargetEntries").expr;
        let (reverse_sort, nulls_first) = if scandir == types_pathnodes::BackwardScanDirection {
            (!index.reverse_sort[i], !index.nulls_first[i])
        } else {
            (index.reverse_sort[i], index.nulls_first[i])
        };
        let cpathkey = make_pathkey_from_sortinfo(
            run,
            indexkey,
            index.sortopfamily[i],
            index.opcintype[i],
            index.indexcollations[i],
            reverse_sort,
            nulls_first,
            0,
            false,
        )?;
        match cpathkey {
            Some(pk) => {
                if !pathkey_is_redundant(run, pk, &retval) {
                    retval.push(pk);
                }
            }
            None => {
                // indexcol_is_bool_constant_for_query: bool equality never
                // reaches an EC; the continue leg must be loud, not a lost
                // sort order.
                const BOOLOID: u32 = 16;
                assert!(
                    index.opcintype[i] != BOOLOID,
                    "indexcol_is_bool_constant_for_query (pathkeys.c): M2 boolean-index lane"
                );
                break;
            }
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
    assert!(run.root.ec_merging_done, "too soon to build canonical pathkeys");
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

fn pathkey_is_redundant(run: &PlannerRun<'_>, new_pathkey: PathKey, pathkeys: &[PathKey]) -> bool {
    // EC_MUST_BE_REDUNDANT: a const EC admits only one key value.
    if run.root.ec(new_pathkey.pk_eclass.unwrap()).ec_has_const {
        return true;
    }
    pathkeys.iter().any(|old| old.pk_eclass == new_pathkey.pk_eclass)
}

// rel=NULL; jdomain is always the top domain here.
fn get_eclass_for_sort_expr<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Node<'mcx>,
    opfamilies: &PgVec<'mcx, u32>,
    opcintype: u32,
    collation: u32,
    sortref: u32,
    create_it: bool,
) -> PgResult<Option<EcId>> {
    let expr = canonicalize_ec_expression(run.mcx, expr, opcintype, collation)?;

    for i in 0..run.root.eq_classes.len() {
        let id = EcId(i as u32);
        let ec = run.root.ec(id);
        if ec.ec_has_volatile && (sortref == 0 || sortref != ec.ec_sortref) {
            continue;
        }
        if collation != ec.ec_collation {
            continue;
        }
        if ec.ec_opfamilies.as_slice() != opfamilies.as_slice() {
            continue;
        }
        let n_members = ec.ec_members.len();
        for m in 0..n_members {
            let em_id = run.root.ec(id).ec_members[m];
            let em = run.root.em(em_id);
            if em.em_is_child {
                continue;
            }
            // C skips const members from a different JoinDomain; every EC
            // built here carries the top domain (em_jdomain None), so const
            // members always match.
            if opcintype == em.em_datatype && types_nodes::equal(*run.root.expr_node(em.em_expr), expr)
            {
                return Ok(Some(id));
            }
        }
    }

    if !create_it {
        return Ok(None);
    }

    let mcx = run.mcx;
    let has_volatile = clauses::contain_volatile_functions(expr)?;
    assert!(!(has_volatile && sortref == 0), "volatile EquivalenceClass has no sortref");
    let expr_relids = pull_varnos_relids(run, expr)?;
    // make_eq_member marks empty-relids members const; get_eclass_for_sort_expr
    // then un-marks when volatile/SRF/agg/window can hide in a sort expr.
    let mut em_is_const = expr_relids.is_none();
    if em_is_const
        && (has_volatile
            || expression_returns_set(expr)?
            || clauses::contain_agg_clause(expr)?
            || clauses::contain_window_function(expr)?)
    {
        em_is_const = false;
    }
    // C copyObject's the expr into the EC; the arena share is our copy model.
    let em_expr = run.intern_expr(expr);
    let em = run.root.alloc_em(EquivalenceMember {
        em_expr,
        em_relids: expr_relids,
        em_is_const,
        em_is_child: false,
        em_datatype: opcintype,
        em_jdomain: None,
        em_parent: None,
    });

    let mut ec = EquivalenceClass::new(mcx);
    ec.ec_opfamilies = crate::relnode::pgvec_clone_shallow(mcx, opfamilies);
    ec.ec_collation = collation;
    ec.ec_members.push(em);
    ec.ec_relids = pull_varnos_relids(run, expr)?;
    ec.ec_has_const = em_is_const;
    ec.ec_has_volatile = has_volatile;
    ec.ec_sortref = sortref;
    ec.ec_min_security = u32::MAX;
    ec.ec_max_security = 0;
    let id = run.root.alloc_ec(ec);

    // ec_merging_done mop-up: extend each mentioned rel's eclass_indexes.
    debug_assert!(run.root.ec_merging_done);
    for rti in vars::pull_varnos(mcx, expr)?.iter() {
        if let Some(Some(rel_id)) = run.root.simple_rel_array.get(rti as usize).copied() {
            let updated = crate::relnode::relids_union(
                mcx,
                &run.root.rel(rel_id).eclass_indexes,
                &crate::relnode::relids_singleton(mcx, id.0),
            );
            run.root.rel_mut(rel_id).eclass_indexes = updated;
        }
    }
    Ok(Some(id))
}

// canonicalize_ec_expression (equivclass.c).
fn canonicalize_ec_expression<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    expr: Node<'mcx>,
    req_type: u32,
    req_collation: u32,
) -> PgResult<Node<'mcx>> {
    use types_core::catalog::RECORDOID;
    use types_nodes::primnodes::CoercionForm;
    let (expr_type, expr_typmod) = crate::costsize::expr_type_typmod(expr);
    let req_type = if clauses::fold::is_polymorphic_type(req_type) || req_type == RECORDOID {
        expr_type
    } else {
        req_type
    };
    if expr_type != req_type || expr_collation(expr) != req_collation {
        let req_typmod = if expr_type != req_type { -1 } else { expr_typmod };
        return clauses::fold::apply_relabel_type(
            mcx,
            expr,
            req_type,
            req_typmod,
            req_collation,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        );
    }
    Ok(expr)
}

// exprCollation (nodeFuncs.c) over the sort-key shapes this lane carries.
pub fn expr_collation(node: Node<'_>) -> u32 {
    use types_nodes::NodeTag;
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().varcollid,
        NodeTag::T_Const => node.as_const().unwrap().constcollid,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opcollid,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funccollid,
        NodeTag::T_Param => node.as_param().unwrap().paramcollid,
        NodeTag::T_Aggref => node.as_aggref().unwrap().aggcollid,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wincollid,
        NodeTag::T_SubPlan => {
            use types_nodes::primnodes::SubLinkType;
            let sp = node.as_sub_plan().unwrap();
            match sp.subLinkType {
                SubLinkType::EXPR_SUBLINK | SubLinkType::ARRAY_SUBLINK => sp.firstColCollation,
                SubLinkType::MULTIEXPR_SUBLINK => {
                    panic!("exprCollation (nodeFuncs.c): MULTIEXPR SubPlan not ported")
                }
                _ => 0,
            }
        }
        NodeTag::T_AlternativeSubPlan => expr_collation(
            node.as_alternative_sub_plan().unwrap().subplans.first().expect("alternatives"),
        ),
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casecollid,
        NodeTag::T_CaseTestExpr => node.as_case_test_expr().unwrap().collation,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resultcollid,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resultcollid,
        other => panic!("exprCollation (nodeFuncs.c): {other:?}; M2 expression lane"),
    }
}

// expression_returns_set (nodeFuncs.c); lives here rather than nodes_core to
// keep this lane out of a concurrently-edited crate.
fn expression_returns_set(expr: Node<'_>) -> PgResult<bool> {
    struct W;
    impl<'mcx> nodes_core::NodeWalker<'mcx> for W {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            use types_nodes::NodeTag;
            match node.node_tag() {
                NodeTag::T_FuncExpr => {
                    if node.as_func_expr().unwrap().funcretset {
                        return Ok(true);
                    }
                    nodes_core::expression_tree_walker(node, self)
                }
                NodeTag::T_OpExpr => {
                    if node.as_op_expr().unwrap().opretset {
                        return Ok(true);
                    }
                    nodes_core::expression_tree_walker(node, self)
                }
                NodeTag::T_Aggref | NodeTag::T_GroupingFunc | NodeTag::T_WindowFunc => Ok(false),
                _ => nodes_core::expression_tree_walker(node, self),
            }
        }
    }
    nodes_core::NodeWalker::visit(&mut W, expr)
}

fn pull_varnos_relids<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
) -> PgResult<types_pathnodes::Relids<'mcx>> {
    let mcx = run.mcx;
    let bms = vars::pull_varnos(mcx, node)?;
    let mut out: types_pathnodes::Relids<'mcx> = None;
    for x in bms.iter() {
        out = crate::relnode::relids_union(mcx, &out, &crate::relnode::relids_singleton(mcx, x as u32));
    }
    Ok(out)
}

// initialize/update_mergeclause_eclasses (pathkeys.c) fused: C fills the EC
// links during qual distribution (process_equivalence); this lane's
// eclass-lite ECs (single-expr, never merged) are created on first use here
// via the same get_eclass_for_sort_expr, so mergeclause/pathkey EC identity
// holds. ec_merged chasing is vacuous (no EC merging happens).
pub fn update_mergeclause_eclasses(
    run: &mut PlannerRun<'_>,
    rinfo: types_pathnodes::RinfoId,
) -> PgResult<()> {
    debug_assert!(!run.root.rinfo(rinfo).mergeopfamilies.is_empty());
    if let Some(lec) = run.root.rinfo(rinfo).left_ec {
        debug_assert!(run.root.ec(lec).ec_merged.is_none());
        debug_assert!(run
            .root
            .ec(run.root.rinfo(rinfo).right_ec.unwrap())
            .ec_merged
            .is_none());
        return Ok(());
    }
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let o = clause.as_op_expr().expect("mergeclause is an OpExpr");
    let (lefttype, righttype) = lsyscache::op_input_types(o.opno)?;
    let opfamilies =
        crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rinfo(rinfo).mergeopfamilies);
    let left_ec =
        get_eclass_for_sort_expr(run, o.args.nth(0), &opfamilies, lefttype, o.inputcollid, 0, true)?;
    let right_ec =
        get_eclass_for_sort_expr(run, o.args.nth(1), &opfamilies, righttype, o.inputcollid, 0, true)?;
    let r = run.root.rinfo_mut(rinfo);
    r.left_ec = left_ec;
    r.right_ec = right_ec;
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
            assert!(oeclass == lastoeclass, "outer pathkeys do not match mergeclause");
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
        types_pathnodes::JOIN_FULL
            | types_pathnodes::JOIN_RIGHT
            | types_pathnodes::JOIN_RIGHT_ANTI
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
        debug_assert!(!run.root.rel(rel).has_eclass_joins);
        let joininfo = crate::relnode::pgvec_clone_shallow(run.mcx, &run.root.rel(rel).joininfo);
        let mut matched = false;
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

// get_cheapest_path_for_pathkeys (pathkeys.c); required_outer is always empty
// and partial paths never reach here.
pub fn get_cheapest_path_for_pathkeys(
    run: &PlannerRun<'_>,
    paths: &[types_pathnodes::PathId],
    pathkeys: &[PathKey],
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
        if pathkeys_contained_in(pathkeys, &path.pathkeys) && path.param_info.is_none() {
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
