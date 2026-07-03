//! selfuncs.c slice: eqsel over Var-op-Const with no pg_statistic tuple,
//! plus btcostestimate/genericcostestimate; a live stats tuple panics.

use types_error::PgResult;
use types_nodes::parsenodes::RTEKind;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{NodeId, PathNode, RelId, RinfoId, JOIN_INNER};

use crate::gucs;
use crate::run::PlannerRun;

pub const DEFAULT_EQ_SEL: f64 = 0.005;
pub const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;
pub const DEFAULT_NUM_DISTINCT: f64 = 200.0;
const DEFAULT_PAGE_CPU_MULTIPLIER: f64 = 50.0;
const BOOLOID: u32 = 16;
const SELF_ITEM_POINTER_ATTRIBUTE_NUMBER: i16 = -1;
const TABLE_OID_ATTRIBUTE_NUMBER: i16 = -6;

fn clamp_probability(p: f64) -> f64 {
    p.clamp(0.0, 1.0)
}

// VariableStatData (selfuncs.h); statsTuple is absent on this lane.
pub struct VariableStatData {
    pub var: Option<NodeId>,
    pub rel: Option<RelId>,
    pub vartype: u32,
    pub isunique: bool,
}

// scalarltsel/scalargtsel family (selfuncs.c), no-statsTuple arm: without a
// pg_statistic row the mcv/histogram fractions are absent and C lands on
// DEFAULT_INEQ_SEL; a live stats tuple panics inside examine_variable.
pub fn scalarineqsel_wrapper<'mcx>(
    run: &mut PlannerRun<'mcx>,
    args: &[NodeId],
    varrelid: i32,
) -> PgResult<f64> {
    let Some((_vardata, other, _varonleft)) = get_restriction_variable(run, args, varrelid)?
    else {
        return Ok(DEFAULT_INEQ_SEL);
    };
    match other.as_const() {
        Some(c) if c.constisnull => Ok(0.0),
        _ => Ok(DEFAULT_INEQ_SEL),
    }
}

pub fn eqsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: u32,
    args: &[NodeId],
    varrelid: i32,
    collation: u32,
) -> PgResult<f64> {
    let _ = collation;
    let Some((vardata, other, varonleft)) = get_restriction_variable(run, args, varrelid)? else {
        return Ok(DEFAULT_EQ_SEL);
    };
    let _ = varonleft;
    let selec = match other.as_const() {
        Some(c) => var_eq_const(run, &vardata, operator, c.constisnull)?,
        None => panic!("var_eq_non_const (selfuncs.c): M2 selfuncs lane"),
    };
    Ok(selec)
}
fn get_restriction_variable<'mcx>(
    run: &mut PlannerRun<'mcx>,
    args: &[NodeId],
    varrelid: i32,
) -> PgResult<Option<(VariableStatData, Node<'mcx>, bool)>> {
    if args.len() != 2 {
        return Ok(None);
    }
    let left = *run.root.expr_node(args[0]);
    let right = *run.root.expr_node(args[1]);
    let vardata = examine_variable(run, args[0], left, varrelid)?;
    let rdata = examine_variable(run, args[1], right, varrelid)?;

    if vardata.rel.is_some() && rdata.rel.is_none() {
        if right.node_tag() != NodeTag::T_Const {
            panic!("estimate_expression_value (clauses.c): M2 expression lane");
        }
        return Ok(Some((vardata, right, true)));
    }
    if vardata.rel.is_none() && rdata.rel.is_some() {
        if left.node_tag() != NodeTag::T_Const {
            panic!("estimate_expression_value (clauses.c): M2 expression lane");
        }
        return Ok(Some((rdata, left, false)));
    }
    Ok(None)
}

// examine_variable (selfuncs.c), plain-Var and pseudo-constant arms.
pub fn examine_variable<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node_id: NodeId,
    node: Node<'mcx>,
    varrelid: i32,
) -> PgResult<VariableStatData> {
    let (vartype, _) = crate::costsize::expr_type_typmod(node);
    let mut vardata = VariableStatData { var: None, rel: None, vartype, isunique: false };

    if let Some(var) = node.as_var() {
        if varrelid == 0 || varrelid == var.varno {
            let rel = crate::relnode::find_base_rel(&run.root, var.varno);
            vardata.var = Some(node_id);
            vardata.rel = Some(rel);
            vardata.isunique = crate::plancat::has_unique_index(run, rel, var.varattno);
            examine_simple_variable(run, var.varno, var.varattno)?;
            return Ok(vardata);
        }
        panic!("examine_variable (selfuncs.c): foreign-rel Var; M2 join lane");
    }
    match node.node_tag() {
        NodeTag::T_Const => Ok(vardata),
        // A var-free Aggref (HAVING quals): C's expression leg finds no
        // relids and returns "don't know" (no rel, no stats).
        NodeTag::T_Aggref => Ok(vardata),
        other => panic!("examine_variable (selfuncs.c): {other:?}; M2 expression lane"),
    }
}

// examine_simple_variable (selfuncs.c): the STATRELATTINH probe; a live stats
// tuple routes to the M2 stats lane.
fn examine_simple_variable(run: &PlannerRun<'_>, varno: i32, varattno: i16) -> PgResult<()> {
    let rte = run.rte(varno as usize);
    if rte.rtekind != RTEKind::RTE_RELATION {
        panic!("examine_simple_variable (selfuncs.c): {:?}; M2 lane", rte.rtekind);
    }
    if syscache_seams::lookup_pg_statistic_shape::call(rte.relid, varattno, rte.inh)?.is_some() {
        panic!("examine_simple_variable (selfuncs.c): pg_statistic tuple present; M2 stats lane");
    }
    Ok(())
}

// get_variable_numdistinct (selfuncs.c), no-statsTuple arms. Returns
// (ndistinct, isdefault).
pub fn get_variable_numdistinct(run: &PlannerRun<'_>, vardata: &VariableStatData) -> (f64, bool) {
    let stanullfrac = 0.0f64;
    let mut stadistinct = if vardata.vartype == BOOLOID {
        2.0
    } else {
        let attno = vardata
            .var
            .and_then(|id| run.root.expr_node(id).as_var().map(|v| v.varattno));
        match attno {
            Some(SELF_ITEM_POINTER_ATTRIBUTE_NUMBER) => -1.0,
            Some(TABLE_OID_ATTRIBUTE_NUMBER) => 1.0,
            _ => 0.0,
        }
    };
    if vardata.isunique {
        stadistinct = -1.0 * (1.0 - stanullfrac);
    }
    if stadistinct > 0.0 {
        return (crate::costsize::clamp_row_est(stadistinct), false);
    }
    let Some(rel) = vardata.rel else {
        return (DEFAULT_NUM_DISTINCT, true);
    };
    let ntuples = run.root.rel(rel).tuples;
    if ntuples <= 0.0 {
        return (DEFAULT_NUM_DISTINCT, true);
    }
    if stadistinct < 0.0 {
        return (crate::costsize::clamp_row_est(-stadistinct * ntuples), false);
    }
    if ntuples < DEFAULT_NUM_DISTINCT {
        return (crate::costsize::clamp_row_est(ntuples), false);
    }
    (DEFAULT_NUM_DISTINCT, true)
}

// var_eq_const (selfuncs.c), no-statsTuple arms (nullfrac 0, negate=false).
fn var_eq_const(
    run: &PlannerRun<'_>,
    vardata: &VariableStatData,
    _oproid: u32,
    constisnull: bool,
) -> PgResult<f64> {
    if constisnull {
        return Ok(0.0);
    }
    let selec = if vardata.isunique
        && vardata.rel.is_some_and(|r| run.root.rel(r).tuples >= 1.0)
    {
        1.0 / run.root.rel(vardata.rel.unwrap()).tuples
    } else {
        1.0 / get_variable_numdistinct(run, vardata).0
    };
    Ok(clamp_probability(selec))
}

pub struct AmCostEstimate {
    pub index_startup_cost: f64,
    pub index_total_cost: f64,
    pub index_selectivity: f64,
    pub index_correlation: f64,
    pub index_pages: f64,
}

// amcostestimate dispatch: closed set over the committed index AMs (rule 4).
pub fn amcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let relam = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            panic!("amcostestimate: not an IndexPath")
        };
        ip.indexinfo.as_ref().expect("indexinfo set").relam
    };
    match types_relscan::IndexAmKind::from_relam(relam) {
        types_relscan::IndexAmKind::Btree => btcostestimate(run, path_id, loop_count),
        #[allow(unreachable_patterns)]
        other => panic!("amcostestimate (selfuncs.c): {other:?}; M2 index-AM lane"),
    }
}

struct GenericCosts {
    num_index_tuples: f64,
    num_sa_scans: f64,
    index_startup_cost: f64,
    index_total_cost: f64,
    index_selectivity: f64,
    index_correlation: f64,
    num_index_pages: f64,
}

// genericcostestimate (selfuncs.c); num_sa_scans arrives preset (no SAOP).
fn genericcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
    costs: &mut GenericCosts,
) -> PgResult<()> {
    let (index_quals, has_orderbys, index_pages, index_tuples, index_rel, reltablespace) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        (
            get_quals_from_indexclauses(run, path_id),
            !ip.indexorderbys.is_empty(),
            index.pages,
            index.tuples,
            index.rel.expect("index rel set"),
            index.reltablespace,
        )
    };
    assert!(!has_orderbys, "genericcostestimate (selfuncs.c): indexorderbys; M2 amcanorderbyop lane");
    let index_rel_relid = run.root.rel(index_rel).relid as i32;
    let index_rel_tuples = run.root.rel(index_rel).tuples;

    // add_predicate_to_index_quals: identity for a non-partial index.
    debug_assert!(costs.num_sa_scans >= 1.0);
    let num_sa_scans = costs.num_sa_scans;

    let index_selectivity = crate::clausesel::clauselist_selectivity(
        run,
        &index_quals,
        index_rel_relid,
        JOIN_INNER,
        None,
    )?;

    let mut num_index_tuples = costs.num_index_tuples;
    if num_index_tuples <= 0.0 {
        num_index_tuples = index_selectivity * index_rel_tuples;
        num_index_tuples = (num_index_tuples / num_sa_scans).round_ties_even();
    }
    if num_index_tuples > index_tuples {
        num_index_tuples = index_tuples;
    }
    if num_index_tuples < 1.0 {
        num_index_tuples = 1.0;
    }

    let num_index_pages = if index_pages > 1 && index_tuples > 1.0 {
        (num_index_tuples * index_pages as f64 / index_tuples).ceil()
    } else {
        1.0
    };

    let (spc_random_page_cost, _) = crate::costsize::get_tablespace_page_costs(reltablespace);

    let num_scans = num_sa_scans * loop_count;
    let mut index_total_cost = if num_scans > 1.0 {
        let pages_fetched = crate::costsize::index_pages_fetched(
            run,
            num_index_pages * num_scans,
            index_pages,
            index_pages as f64,
        );
        (pages_fetched * spc_random_page_cost) / loop_count
    } else {
        num_index_pages * spc_random_page_cost
    };

    let qual_arg_cost = index_other_operands_eval_cost(run, &index_quals)?;
    let qual_op_cost = gucs::cpu_operator_cost() * index_quals.len() as f64;

    let index_startup_cost = qual_arg_cost;
    index_total_cost += qual_arg_cost;
    index_total_cost += num_index_tuples * num_sa_scans * (gucs::cpu_index_tuple_cost() + qual_op_cost);

    costs.index_startup_cost = index_startup_cost;
    costs.index_total_cost = index_total_cost;
    costs.index_selectivity = index_selectivity;
    costs.index_correlation = 0.0;
    costs.num_index_pages = num_index_pages;
    costs.num_index_tuples = num_index_tuples;
    costs.num_sa_scans = num_sa_scans;
    Ok(())
}

// get_quals_from_indexclauses (selfuncs.c).
fn get_quals_from_indexclauses<'mcx>(
    run: &PlannerRun<'mcx>,
    path_id: types_pathnodes::PathId,
) -> mcx::PgVec<'mcx, RinfoId> {
    let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
    let mut out = mcx::PgVec::new_in(run.mcx);
    for ic in ip.indexclauses.iter() {
        for &r in ic.indexquals.iter() {
            out.push(r);
        }
    }
    out
}

// index_other_operands_eval_cost (selfuncs.c).
fn index_other_operands_eval_cost(
    run: &mut PlannerRun<'_>,
    index_quals: &[RinfoId],
) -> PgResult<f64> {
    let mut qual_arg_cost = 0.0;
    for &rid in index_quals {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        let other_operand = match clause.node_tag() {
            // indexkey is always the left operand of a fixed indexqual.
            NodeTag::T_OpExpr => Some(clause.as_op_expr().unwrap().args.nth(1)),
            other => panic!("index_other_operands_eval_cost (selfuncs.c): {other:?}; M2 lane"),
        };
        if let Some(op) = other_operand {
            let cost = crate::costsize::cost_qual_eval_node(op)?;
            qual_arg_cost += cost.startup + cost.per_tuple;
        }
    }
    Ok(qual_arg_cost)
}

// btcostestimate (selfuncs.c); the boundary-qual walk sees only OpExprs.
fn btcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let (indexclauses, index_unique, index_nkeycolumns, index_tuples, index_tree_height, index_rel, opfamilies) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            panic!("btcostestimate: not an IndexPath")
        };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        let mut fams = mcx::PgVec::new_in(run.mcx);
        fams.extend(index.opfamily.iter().copied());
        (
            ip.indexclauses.clone(),
            index.unique,
            index.nkeycolumns,
            index.tuples,
            index.tree_height.get(),
            index.rel.expect("index rel set"),
            fams,
        )
    };
    let index_rel_relid = run.root.rel(index_rel).relid as i32;
    let index_rel_tuples = run.root.rel(index_rel).tuples;
    let index_pages = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
        ip.indexinfo.as_ref().unwrap().pages
    };

    let mut index_bound_quals: mcx::PgVec<'_, RinfoId> = mcx::PgVec::new_in(run.mcx);
    let mut indexcol: i32 = 0;
    let mut eq_qual_here = false;
    let num_sa_scans = 1.0f64;

    for iclause in indexclauses.iter() {
        if indexcol < iclause.indexcol as i32 {
            // A column gap means nbtree would consider skip arrays.
            if eq_qual_here {
                indexcol += 1;
            }
            eq_qual_here = false;
            if indexcol < iclause.indexcol as i32 {
                panic!("btcostestimate (selfuncs.c): skip-array column gap; M2 skip-scan lane");
            }
        }
        debug_assert!(indexcol == iclause.indexcol as i32);

        for &rid in iclause.indexquals.iter() {
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            let clause_op = match clause.node_tag() {
                NodeTag::T_OpExpr => clause.as_op_expr().unwrap().opno,
                other => panic!("btcostestimate (selfuncs.c): indexqual {other:?}; M2 lane"),
            };
            let op_strategy =
                lsyscache::get_op_opfamily_strategy(clause_op, opfamilies[indexcol as usize])?;
            debug_assert!(op_strategy != 0);
            if op_strategy == lsyscache::BTEqualStrategyNumber as i32 {
                eq_qual_here = true;
            }
            index_bound_quals.push(rid);
        }
    }

    let num_index_tuples = if index_unique
        && indexcol == index_nkeycolumns - 1
        && eq_qual_here
    {
        1.0
    } else {
        let btree_selectivity = crate::clausesel::clauselist_selectivity(
            run,
            &index_bound_quals,
            index_rel_relid,
            JOIN_INNER,
            None,
        )?;
        let nit = btree_selectivity * index_rel_tuples;
        debug_assert!(num_sa_scans == 1.0);
        (nit / num_sa_scans).round_ties_even()
    };

    let mut costs = GenericCosts {
        num_index_tuples,
        num_sa_scans,
        index_startup_cost: 0.0,
        index_total_cost: 0.0,
        index_selectivity: 0.0,
        index_correlation: 0.0,
        num_index_pages: 0.0,
    };
    genericcostestimate(run, path_id, loop_count, &mut costs)?;

    let cpu_operator_cost = gucs::cpu_operator_cost();
    if index_tuples > 1.0 {
        let descent_cost = (index_tuples.ln() / 2.0f64.ln()).ceil() * cpu_operator_cost;
        costs.index_startup_cost += descent_cost;
        costs.index_total_cost += costs.num_sa_scans * descent_cost;
    }
    let descent_cost =
        (index_tree_height as f64 + 1.0) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.index_startup_cost += descent_cost;
    costs.index_total_cost += costs.num_sa_scans * descent_cost;

    // btcost_correlation over the leading simple column; no stats -> 0.
    {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
        let attno = ip.indexinfo.as_ref().unwrap().indexkeys[0] as i16;
        let rte = run.rte(index_rel_relid as usize);
        if syscache_seams::lookup_pg_statistic_shape::call(rte.relid, attno, rte.inh)?.is_some() {
            panic!("btcost_correlation (selfuncs.c): pg_statistic tuple present; M2 stats lane");
        }
    }
    debug_assert!(costs.index_correlation == 0.0);
    let _ = index_pages;

    Ok(AmCostEstimate {
        index_startup_cost: costs.index_startup_cost,
        index_total_cost: costs.index_total_cost,
        index_selectivity: costs.index_selectivity,
        index_correlation: costs.index_correlation,
        index_pages: costs.num_index_pages,
    })
}

// estimate_num_groups (selfuncs.c), no-stats Var-only leg; other families
// and multivariate/extended stats are M3 lanes.
pub fn estimate_num_groups<'mcx>(
    run: &mut PlannerRun<'mcx>,
    group_exprs: &[(NodeId, Node<'mcx>)],
    input_rows: f64,
) -> PgResult<f64> {
    let input_rows = crate::costsize::clamp_row_est(input_rows);
    if group_exprs.is_empty() {
        return Ok(1.0);
    }

    struct GroupVarInfo {
        var: NodeId,
        rel: RelId,
        ndistinct: f64,
    }
    let mcx = run.mcx;
    let mut varinfos: mcx::PgVec<'_, GroupVarInfo> = mcx::PgVec::new_in(mcx);
    for &(id, node) in group_exprs {
        match node.node_tag() {
            NodeTag::T_Const => continue,
            NodeTag::T_Var => {}
            other => panic!(
                "estimate_num_groups (selfuncs.c): grouping expr {other:?}; M3 expression lane"
            ),
        }
        let v = node.as_var().unwrap();
        let dup = varinfos.iter().any(|vi| {
            let u = run.root.expr_node(vi.var).as_var().unwrap();
            u.varno == v.varno && u.varattno == v.varattno
        });
        if dup {
            continue;
        }
        let vardata = examine_variable(run, id, node, 0)?;
        let (ndistinct, _isdefault) = get_variable_numdistinct(run, &vardata);
        varinfos.push(GroupVarInfo {
            var: id,
            rel: vardata.rel.expect("grouping Var has a base rel"),
            ndistinct,
        });
    }
    if varinfos.is_empty() {
        return Ok(1.0);
    }

    let mut numdistinct = 1.0f64;
    let mut remaining = varinfos;
    while !remaining.is_empty() {
        let rel_id = remaining[0].rel;
        let mut reldistinct = 1.0f64;
        let mut relmaxndistinct = 1.0f64;
        let mut relvarcount = 0usize;
        let mut rest: mcx::PgVec<'_, GroupVarInfo> = mcx::PgVec::new_in(mcx);
        for vi in remaining {
            if vi.rel == rel_id {
                reldistinct *= vi.ndistinct;
                if relmaxndistinct < vi.ndistinct {
                    relmaxndistinct = vi.ndistinct;
                }
                relvarcount += 1;
            } else {
                rest.push(vi);
            }
        }
        let (rel_tuples, rel_rows) = {
            let rel = run.root.rel(rel_id);
            (rel.tuples, rel.rows)
        };
        if rel_tuples > 0.0 {
            let mut clamp = rel_tuples;
            if relvarcount > 1 {
                clamp *= 0.1;
                if clamp < relmaxndistinct {
                    clamp = relmaxndistinct.min(rel_tuples);
                }
            }
            if reldistinct > clamp {
                reldistinct = clamp;
            }
            if reldistinct > 0.0 && rel_rows < rel_tuples {
                // Dell'Era approximation of Yao's formula.
                reldistinct *=
                    1.0 - ((rel_tuples - rel_rows) / rel_tuples).powf(rel_tuples / reldistinct);
            }
            numdistinct *= crate::costsize::clamp_row_est(reldistinct);
        }
        remaining = rest;
    }

    let numdistinct = numdistinct.ceil();
    Ok(numdistinct.clamp(1.0, input_rows))
}

// eqjoinsel (selfuncs.c), no-pg_statistic arms (a live stats tuple panics in
// examine_simple_variable): nullfracs are 0 and no MCV lists exist, so
// eqjoinsel_inner reduces to 1/max(nd1, nd2).
pub fn eqjoinsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    _operator: u32,
    args: &[NodeId],
    jointype: types_pathnodes::JoinType,
    sjinfo: Option<&types_pathnodes::SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    assert!(args.len() == 2, "eqjoinsel (selfuncs.c): non-binary clause");
    let sj_jointype = sjinfo.map_or(jointype, |sj| sj.jointype);
    let left = *run.root.expr_node(args[0]);
    let right = *run.root.expr_node(args[1]);
    let vardata1 = examine_variable(run, args[0], left, 0)?;
    let vardata2 = examine_variable(run, args[1], right, 0)?;
    let (nd1, _isdefault1) = get_variable_numdistinct(run, &vardata1);
    let (nd2, _isdefault2) = get_variable_numdistinct(run, &vardata2);

    let selec_inner = 1.0 / nd1.max(nd2);
    let selec = match sj_jointype {
        JOIN_INNER | types_pathnodes::JOIN_LEFT | types_pathnodes::JOIN_FULL => selec_inner,
        other => panic!("eqjoinsel (selfuncs.c): jointype {other} (eqjoinsel_semi); M2 semi-join lane"),
    };
    Ok(clamp_probability(selec))
}
