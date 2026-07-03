//! pathkeys.c + the get_eclass_for_sort_expr slice of equivclass.c (the full
//! EC merging machinery is the M2 join lane; equivclass' CATALOG row stays
//! todo and points here). Canonical PathKeys are values here: canonicalization
//! makes value equality C's pointer equality.

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

// pathkeys_count_contained_in (pathkeys.c): (contained, n leading matches).
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

// get_sortgroupclause_expr via get_sortgroupref_tle (tlist.c).
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

// make_pathkeys_for_sortclauses (pathkeys.c): the _extended form with
// remove_redundant/remove_group_rtindex/set_ec_sortref all false; unsortable
// clauses can't reach here (parse_clause resolved a real sortop).
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
    make_pathkey_from_sortinfo(
        run, expr, opfamily, opcintype, collation, reverse_sort, nulls_first, sortref,
    )
}

// make_pathkey_from_sortinfo (pathkeys.c); rel=NULL, create_it=true.
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
) -> PgResult<PathKey> {
    let cmptype = if reverse_sort { COMPARE_GT } else { COMPARE_LT };
    let equality_op =
        lsyscache::amop::get_opfamily_member_for_cmptype(opfamily, opcintype, opcintype, COMPARE_EQ)?;
    assert!(
        equality_op != 0,
        "missing operator {COMPARE_EQ}({opcintype},{opcintype}) in opfamily {opfamily}"
    );
    let opfamilies = lsyscache::amop::get_mergejoin_opfamilies(run.mcx, equality_op)?;
    assert!(!opfamilies.is_empty(), "could not find opfamilies for equality operator {equality_op}");
    let eclass = get_eclass_for_sort_expr(run, expr, &opfamilies, opcintype, collation, sortref)?;
    Ok(make_canonical_pathkey(run, eclass, opfamily, cmptype, nulls_first))
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

// pathkey_is_redundant (pathkeys.c): EC-const case can't arise while quals
// skip the EC detour (initsplan divergence note).
fn pathkey_is_redundant(run: &PlannerRun<'_>, new_pathkey: PathKey, pathkeys: &[PathKey]) -> bool {
    debug_assert!(!run.root.ec(new_pathkey.pk_eclass.unwrap()).ec_has_const);
    pathkeys.iter().any(|old| old.pk_eclass == new_pathkey.pk_eclass)
}

// get_eclass_for_sort_expr (equivclass.c), create_it=true, rel=NULL. The
// jdomain const matching leg is vacuous: no EC member here is a const.
fn get_eclass_for_sort_expr<'mcx>(
    run: &mut PlannerRun<'mcx>,
    expr: Node<'mcx>,
    opfamilies: &PgVec<'mcx, u32>,
    opcintype: u32,
    collation: u32,
    sortref: u32,
) -> PgResult<EcId> {
    let expr = canonicalize_ec_expression(expr, opcintype, collation);

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
            debug_assert!(!em.em_is_const);
            if opcintype == em.em_datatype && types_nodes::equal(*run.root.expr_node(em.em_expr), expr)
            {
                return Ok(id);
            }
        }
    }

    let mcx = run.mcx;
    let has_volatile = clauses::contain_volatile_functions(expr)?;
    assert!(!(has_volatile && sortref == 0), "volatile EquivalenceClass has no sortref");
    let expr_relids = pull_varnos_relids(run, expr)?;
    let is_const = expr_relids.is_none() && !has_volatile;
    assert!(!is_const, "get_eclass_for_sort_expr (equivclass.c): const sort expr; M2 lane");
    // C copyObject's the expr into the EC; the arena share is our copy model.
    let em_expr = run.intern_expr(expr);
    let em = run.root.alloc_em(EquivalenceMember {
        em_expr,
        em_relids: expr_relids,
        em_is_const: false,
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
    Ok(id)
}

// canonicalize_ec_expression (equivclass.c): the expr must expose opcintype;
// the RelabelType wrap/strip legs are loud.
fn canonicalize_ec_expression<'mcx>(expr: Node<'mcx>, req_type: u32, _req_collation: u32) -> Node<'mcx> {
    let (expr_type, _) = crate::costsize::expr_type_typmod(expr);
    assert!(
        expr_type == req_type,
        "canonicalize_ec_expression (equivclass.c): relabel leg; M2 lane"
    );
    expr
}

// exprCollation (nodeFuncs.c) over the sort-key shapes this lane carries.
pub fn expr_collation(node: Node<'_>) -> u32 {
    use types_nodes::NodeTag;
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().varcollid,
        NodeTag::T_Const => node.as_const().unwrap().constcollid,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opcollid,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funccollid,
        other => panic!("exprCollation (nodeFuncs.c): {other:?}; M2 expression lane"),
    }
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
