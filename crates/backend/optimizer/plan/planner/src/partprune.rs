// partprune.c, planner side: gen_partprune_steps + plan-time pruning
// (prune_append_rel_partitions) + make_partition_pruneinfo. The bound
// matching kernel is the partprune crate; runtime step evaluation lives in
// the executor.
#![allow(non_snake_case)]

use mcx::PgVec;
use types_core::{InvalidOid, Oid};
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::{IntList, NodeList, OidList};
use types_nodes::primnodes::{BoolExprType, BoolTestType, NullTestType, ParamKind};
use types_nodes::{Node, NodeTag};
use types_pathnodes::RelId;
use clauses::NodeWalker;

use partprune::{
    BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, BTGreaterStrategyNumber,
    BTLessStrategyNumber, BTLessEqualStrategyNumber, BTMaxStrategyNumber, HTEqualStrategyNumber,
    HTMaxStrategyNumber, InvalidStrategy, PruneStepResult, PARTITION_MAX_KEYS,
};

use crate::relnode::find_base_rel;
use crate::run::PlannerRun;

const BTORDER_PROC: i16 = 1;
const HASHEXTENDED_PROC: i16 = 2;
const BOOL_BTREE_FAM_OID: Oid = 424;
const BOOL_HASH_FAM_OID: Oid = 2222;
const BOOLEAN_EQUAL_OPERATOR: Oid = 91;
const PROVOLATILE_IMMUTABLE: i8 = b'i' as i8;

const PARTITION_STRATEGY_HASH: u8 = b'h';
const PARTITION_STRATEGY_LIST: u8 = b'l';
const PARTITION_STRATEGY_RANGE: u8 = b'r';

// partitions_are_ordered (partbounds.c): RANGE unless a live DEFAULT
// partition; LIST unless live interleaved partitions; HASH never.
pub(crate) fn partitions_are_ordered(
    boundinfo: &types_pathnodes::PartitionBoundInfoData<'_>,
    live_parts: &types_pathnodes::Relids<'_>,
) -> bool {
    match boundinfo.strategy as u8 {
        PARTITION_STRATEGY_RANGE => {
            boundinfo.default_index < 0
                || !types_pathnodes::relids::relids_is_member(boundinfo.default_index, live_parts)
        }
        PARTITION_STRATEGY_LIST => {
            !types_pathnodes::relids::relids_overlap(live_parts, &boundinfo.interleaved_parts)
        }
        _ => false,
    }
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum PartClauseTarget {
    Planner,
    Initial,
    Exec,
}

#[derive(Clone)]
struct PartClauseInfo<'mcx> {
    keyno: usize,
    opno: Oid,
    op_is_ne: bool,
    expr: Node<'mcx>,
    cmpfn: Oid,
    op_strategy: u16,
}

pub(crate) struct GenCtx<'mcx> {
    rel: RelId,
    target: PartClauseTarget,
    pub(crate) steps: NodeList<'mcx>,
    pub(crate) has_mutable_op: bool,
    pub(crate) has_mutable_arg: bool,
    pub(crate) has_exec_param: bool,
    pub(crate) contradictory: bool,
    next_step_id: i32,
    strategy: u8,
    partnatts: usize,
    partopfamily: PgVec<'mcx, Oid>,
    partopcintype: PgVec<'mcx, Oid>,
    partcollation: PgVec<'mcx, Oid>,
    partsupfunc_oids: PgVec<'mcx, Oid>,
    partkeys: PgVec<'mcx, Node<'mcx>>,
    has_default: bool,
}

// gen_partprune_steps (partprune.c).
pub(crate) fn gen_partprune_steps<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    clauses: &[Node<'mcx>],
    target: PartClauseTarget,
) -> PgResult<GenCtx<'mcx>> {
    let mcx = run.mcx;
    let (strategy, partnatts, has_default) = {
        let r = run.root.rel(rel);
        let ps = r.part_scheme.as_ref().expect("partitioned rel has a part_scheme");
        (
            ps.strategy as u8,
            ps.partnatts as usize,
            r.boundinfo.as_ref().is_some_and(|b| b.default_index != -1),
        )
    };
    let mut ctx = GenCtx {
        rel,
        target,
        steps: NodeList::nil(),
        has_mutable_op: false,
        has_mutable_arg: false,
        has_exec_param: false,
        contradictory: false,
        next_step_id: 0,
        strategy,
        partnatts,
        partopfamily: PgVec::new_in(mcx),
        partopcintype: PgVec::new_in(mcx),
        partcollation: PgVec::new_in(mcx),
        partsupfunc_oids: PgVec::new_in(mcx),
        partkeys: PgVec::new_in(mcx),
        has_default,
    };
    {
        let r = run.root.rel(rel);
        let ps = r.part_scheme.as_ref().unwrap();
        for i in 0..partnatts {
            ctx.partopfamily.push(ps.partopfamily[i]);
            ctx.partopcintype.push(ps.partopcintype[i]);
            ctx.partcollation.push(ps.partcollation[i]);
            ctx.partsupfunc_oids.push(ps.partsupfunc[i].fn_oid);
        }
    }
    for i in 0..partnatts {
        let id = run.root.rel(rel).partexprs[i][0];
        ctx.partkeys.push(*run.root.expr_node(id));
    }

    let mut all_clauses: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    all_clauses.extend(clauses.iter().copied());
    if has_default && !run.root.rel(rel).partition_qual.is_empty() {
        for i in 0..run.root.rel(rel).partition_qual.len() {
            let id = run.root.rel(rel).partition_qual[i];
            all_clauses.push(*run.root.expr_node(id));
        }
    }

    gen_partprune_steps_internal(run, &mut ctx, &all_clauses)?;
    Ok(ctx)
}

// prune_append_rel_partitions (partprune.c): part_rels-index set of the
// surviving partitions, computed from rel->baserestrictinfo.
pub(crate) fn prune_append_rel_partitions<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
) -> PgResult<Bitmapset<'mcx>> {
    let mcx = run.mcx;
    debug_assert!(run.root.rel(rel).part_scheme.is_some());
    let nparts = run.root.rel(rel).nparts;
    if nparts == 0 {
        return Ok(Bitmapset::empty());
    }
    let nclauses = run.root.rel(rel).baserestrictinfo.len();
    let all = |mcx| -> PgResult<Bitmapset<'mcx>> {
        let mut b = Bitmapset::empty();
        partprune::bms_add_range(mcx, &mut b, 0, nparts - 1)?;
        Ok(b)
    };
    if !crate::gucs::enable_partition_pruning() || nclauses == 0 {
        return all(mcx);
    }
    let mut clauses: PgVec<'mcx, Node<'mcx>> = mcx::vec_with_capacity_in(mcx, nclauses)?;
    for i in 0..nclauses {
        let rid = run.root.rel(rel).baserestrictinfo[i];
        clauses.push(*run.root.expr_node(run.root.rinfo(rid).clause));
    }
    let ctx = gen_partprune_steps(run, rel, &clauses, PartClauseTarget::Planner)?;
    if ctx.contradictory {
        return Ok(Bitmapset::empty());
    }
    if ctx.steps.is_nil() {
        return all(mcx);
    }
    get_matching_partitions_planner(run, rel, &ctx.steps)
}

// get_matching_partitions (partprune.c), planner arm: step exprs are Consts.
fn get_matching_partitions_planner<'mcx>(
    run: &PlannerRun<'mcx>,
    rel: RelId,
    steps: &NodeList<'mcx>,
) -> PgResult<Bitmapset<'mcx>> {
    let mcx = run.mcx;
    let r = run.root.rel(rel);
    let ps = r.part_scheme.as_ref().expect("part_scheme");
    let boundinfo = r.boundinfo.as_ref().expect("boundinfo");
    let strategy = ps.strategy as u8;
    let partnatts = ps.partnatts as i32;
    let num_steps = steps.len();
    let mut results: Vec<Option<PruneStepResult<'mcx>>> = Vec::new();
    results.resize_with(num_steps, || None);
    for step in steps.iter() {
        match step.node_tag() {
            NodeTag::T_PartitionPruneStepOp => {
                let op = step.as_partition_prune_step_op().unwrap();
                let res = perform_pruning_base_step_planner(
                    mcx,
                    &**boundinfo,
                    strategy,
                    partnatts,
                    &**ps,
                    op,
                )?;
                results[op.step_id as usize] = Some(res);
            }
            NodeTag::T_PartitionPruneStepCombine => {
                let c = step.as_partition_prune_step_combine().unwrap();
                let res = partprune::perform_pruning_combine_step(
                    mcx,
                    &**boundinfo,
                    c.combineOp,
                    c.step_id,
                    c.source_stepids.iter(),
                    &results,
                )?;
                results[c.step_id as usize] = Some(res);
            }
            other => panic!("invalid pruning step type: {other:?}"),
        }
    }
    let final_result = results[num_steps - 1].as_ref().expect("final step evaluated");
    partprune::matching_bounds_to_partitions(mcx, &**boundinfo, final_result, strategy)
}

// perform_pruning_base_step (partprune.c), Const-only planner arm; cross-type
// cmpfns are resolved fresh per step (plan-time only path).
fn perform_pruning_base_step_planner<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    boundinfo: &types_pathnodes::PartitionBoundInfoData<'mcx>,
    strategy: u8,
    partnatts: i32,
    ps: &types_pathnodes::PartitionSchemeData<'mcx>,
    opstep: &types_nodes::plannodes::PartitionPruneStepOp<'mcx>,
) -> PgResult<PruneStepResult<'mcx>> {
    debug_assert_eq!(opstep.exprs.len(), opstep.cmpfns.len());
    let mut values = [datum::Datum::null(); PARTITION_MAX_KEYS];
    let mut supfuncs: Vec<types_fmgr::FmgrInfo> = Vec::with_capacity(partnatts as usize);
    let mut nvalues = 0i32;
    let mut it = opstep.exprs.iter().zip(opstep.cmpfns.iter());
    for keyno in 0..partnatts {
        if opstep.nullkeys.is_member(keyno) {
            continue;
        }
        if keyno > nvalues && strategy == PARTITION_STRATEGY_RANGE {
            break;
        }
        if let Some((expr, cmpfn)) = it.next() {
            let c = expr
                .as_const()
                .expect("planner pruning step expr is a Const");
            if c.constisnull {
                return Ok(PruneStepResult::empty());
            }
            debug_assert!(cmpfn != InvalidOid);
            supfuncs.push(
                fmgr_core::fmgr_info(cmpfn)
                    .unwrap_or_else(|e| panic!("fmgr_info({cmpfn}) failed: {e:?}")),
            );
            values[keyno as usize] = c.constvalue;
            nvalues += 1;
        }
    }

    let sup_call = |f: &mut types_fmgr::FmgrInfo, coll: Oid, a: datum::Datum, b: datum::Datum| {
        // range_cmp (range-typed partition keys) detoasts through the result
        // mcx; arm the frame with call-lifetime scratch.
        let scratch = ::mcx::MemoryContext::new("partprune cmp");
        let mut fcinfo = types_fmgr::LocalFcinfo::<2>::new(coll);
        // SAFETY: scratch outlives this call.
        unsafe { fcinfo.set_result_mcx(scratch.mcx()) };
        fcinfo.set_arg(0, a);
        fcinfo.set_arg(1, b);
        let r = f
            .invoke(&mut fcinfo)
            .unwrap_or_else(|e| panic!("partition comparison function failed: {e:?}"));
        assert!(!fcinfo.isnull, "partition comparison function returned NULL");
        r
    };

    match strategy {
        PARTITION_STRATEGY_HASH => {
            let mut sf = supfuncs;
            partprune::get_matching_hash_bounds(
                mcx,
                boundinfo,
                partnatts,
                opstep.opstrategy,
                nvalues,
                &opstep.nullkeys,
                || {
                    let mut row_hash = 0u64;
                    let mut vi = 0usize;
                    for keyno in 0..partnatts as usize {
                        if opstep.nullkeys.is_member(keyno as i32) {
                            continue;
                        }
                        let h = sup_call(
                            &mut sf[vi],
                            ps.partcollation[keyno],
                            values[keyno],
                            datum::Datum::from_u64(partprune::HASH_PARTITION_SEED),
                        );
                        row_hash = partprune::hash_combine64(row_hash, h.as_u64());
                        vi += 1;
                    }
                    row_hash
                },
            )
        }
        PARTITION_STRATEGY_LIST => {
            let mut sf = supfuncs;
            let coll = ps.partcollation[0];
            partprune::get_matching_list_bounds(
                mcx,
                boundinfo,
                opstep.opstrategy,
                nvalues,
                &opstep.nullkeys,
                |bound| sup_call(&mut sf[0], coll, bound, values[0]).as_i32(),
            )
        }
        PARTITION_STRATEGY_RANGE => {
            let mut sf = supfuncs;
            partprune::get_matching_range_bounds(
                mcx,
                boundinfo,
                partnatts,
                opstep.opstrategy,
                nvalues,
                &opstep.nullkeys,
                &mut |j: i32, bound: datum::Datum| {
                    sup_call(&mut sf[j as usize], ps.partcollation[j as usize], bound, values[j as usize])
                        .as_i32()
                },
            )
        }
        other => panic!("unexpected partition strategy: {}", other as char),
    }
}

fn strip_relabel(mut n: Node<'_>) -> Node<'_> {
    while let Some(r) = n.as_relabel_type() {
        n = r.arg;
    }
    n
}

fn const_is_false(n: Node<'_>) -> bool {
    n.as_const().is_some_and(|c| c.constisnull || !c.constvalue.as_bool())
}

// gen_partprune_steps_internal (partprune.c).
fn gen_partprune_steps_internal<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ctx: &mut GenCtx<'mcx>,
    clauses: &[Node<'mcx>],
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    // A defaulted sub-partitioned rel whose partition constraint is refuted
    // by the clauses scans nothing; steps can't prune its default partition
    // (partprune.c:1013).
    if ctx.has_default && !run.root.rel(ctx.rel).partition_qual.is_empty() {
        let mut partqual: Vec<Node<'mcx>> = Vec::new();
        for i in 0..run.root.rel(ctx.rel).partition_qual.len() {
            let id = run.root.rel(ctx.rel).partition_qual[i];
            partqual.push(*run.root.expr_node(id));
        }
        if crate::predtest::predicate_refuted_by(mcx, &partqual, clauses, false)? {
            ctx.contradictory = true;
            return Ok(NodeList::nil());
        }
    }
    let mut keyclauses: [Vec<PartClauseInfo<'mcx>>; PARTITION_MAX_KEYS] =
        core::array::from_fn(|_| Vec::new());
    let mut nullkeys = Bitmapset::empty();
    let mut notnullkeys = Bitmapset::empty();
    let mut generate_opsteps = false;
    let mut result = NodeList::nil();

    for &clause in clauses {
        if clause.node_tag() == NodeTag::T_Const && const_is_false(clause) {
            ctx.contradictory = true;
            return Ok(NodeList::nil());
        }
        if let Some(be) = clause.as_bool_expr() {
            if be.boolop == BoolExprType::OR_EXPR {
                let mut arg_stepids = IntList::nil();
                let mut all_args_contradictory = true;
                debug_assert!(!ctx.contradictory);
                for arg in be.args.iter() {
                    let argsteps = gen_partprune_steps_internal(run, ctx, &[arg])?;
                    let arg_contradictory = ctx.contradictory;
                    ctx.contradictory = false;
                    if arg_contradictory {
                        continue;
                    }
                    all_args_contradictory = false;
                    if !argsteps.is_nil() {
                        let last = argsteps.nth(argsteps.len() - 1);
                        arg_stepids.lappend(mcx, step_id_of(last))?;
                    } else {
                        let sid = gen_prune_step_combine(
                            run,
                            ctx,
                            IntList::nil(),
                            types_nodes::plannodes::PARTPRUNE_COMBINE_UNION,
                        )?;
                        arg_stepids.lappend(mcx, sid)?;
                    }
                }
                if all_args_contradictory {
                    ctx.contradictory = true;
                    return Ok(NodeList::nil());
                }
                if !arg_stepids.is_nil() {
                    let sid = gen_prune_step_combine(
                        run,
                        ctx,
                        arg_stepids,
                        types_nodes::plannodes::PARTPRUNE_COMBINE_UNION,
                    )?;
                    result.lappend(mcx, step_node(ctx, sid))?;
                }
                continue;
            } else if be.boolop == BoolExprType::AND_EXPR {
                let args: Vec<Node<'mcx>> = be.args.iter().collect();
                let argsteps = gen_partprune_steps_internal(run, ctx, &args)?;
                if ctx.contradictory {
                    return Ok(NodeList::nil());
                }
                if !argsteps.is_nil() {
                    result.lappend(mcx, argsteps.nth(argsteps.len() - 1))?;
                }
                continue;
            }
        }

        for i in 0..ctx.partnatts {
            let partkey = ctx.partkeys[i];
            let mut clause_is_not_null = false;
            let mut pc: Option<PartClauseInfo<'mcx>> = None;
            let mut clause_steps = NodeList::nil();
            match match_clause_to_partition_key(
                run,
                ctx,
                clause,
                partkey,
                i,
                &mut clause_is_not_null,
                &mut pc,
                &mut clause_steps,
            )? {
                PartClauseMatchStatus::MatchClause => {
                    let pc = pc.expect("MatchClause sets pc");
                    if nullkeys.is_member(i as i32) {
                        ctx.contradictory = true;
                        return Ok(NodeList::nil());
                    }
                    generate_opsteps = true;
                    keyclauses[i].push(pc);
                }
                PartClauseMatchStatus::MatchNullness => {
                    if !clause_is_not_null {
                        if notnullkeys.is_member(i as i32) || !keyclauses[i].is_empty() {
                            ctx.contradictory = true;
                            return Ok(NodeList::nil());
                        }
                        nullkeys.add_member(mcx, i as i32)?;
                    } else {
                        if nullkeys.is_member(i as i32) {
                            ctx.contradictory = true;
                            return Ok(NodeList::nil());
                        }
                        notnullkeys.add_member(mcx, i as i32)?;
                    }
                }
                PartClauseMatchStatus::MatchSteps => {
                    debug_assert!(!clause_steps.is_nil());
                    for s in clause_steps.iter() {
                        result.lappend(mcx, s)?;
                    }
                }
                PartClauseMatchStatus::MatchContradict => {
                    ctx.contradictory = true;
                    return Ok(NodeList::nil());
                }
                PartClauseMatchStatus::NoMatch => {
                    continue;
                }
                PartClauseMatchStatus::Unsupported => {}
            }
            break;
        }
    }

    if !nullkeys.is_empty()
        && (ctx.strategy == PARTITION_STRATEGY_LIST
            || ctx.strategy == PARTITION_STRATEGY_RANGE
            || (ctx.strategy == PARTITION_STRATEGY_HASH
                && nullkeys.num_members() == ctx.partnatts as i32))
    {
        let sid = gen_prune_step_op(
            run,
            ctx,
            InvalidStrategy,
            false,
            NodeList::nil(),
            OidList::nil(),
            nullkeys,
        )?;
        result.lappend(mcx, step_node(ctx, sid))?;
    } else if generate_opsteps {
        let opsteps = gen_prune_steps_from_opexps(run, ctx, &mut keyclauses, &nullkeys)?;
        for s in opsteps.iter() {
            result.lappend(mcx, s)?;
        }
    } else if notnullkeys.num_members() == ctx.partnatts as i32 {
        let sid = gen_prune_step_op(
            run,
            ctx,
            InvalidStrategy,
            false,
            NodeList::nil(),
            OidList::nil(),
            Bitmapset::empty(),
        )?;
        result.lappend(mcx, step_node(ctx, sid))?;
    }

    if result.len() > 1 {
        let mut step_ids = IntList::nil();
        for s in result.iter() {
            step_ids.lappend(mcx, step_id_of(s))?;
        }
        let sid = gen_prune_step_combine(
            run,
            ctx,
            step_ids,
            types_nodes::plannodes::PARTPRUNE_COMBINE_INTERSECT,
        )?;
        result.lappend(mcx, step_node(ctx, sid))?;
    }

    Ok(result)
}

fn step_id_of(step: Node<'_>) -> i32 {
    match step.node_tag() {
        NodeTag::T_PartitionPruneStepOp => step.as_partition_prune_step_op().unwrap().step_id,
        NodeTag::T_PartitionPruneStepCombine => {
            step.as_partition_prune_step_combine().unwrap().step_id
        }
        other => panic!("invalid pruning step type: {other:?}"),
    }
}

fn step_node<'mcx>(ctx: &GenCtx<'mcx>, step_id: i32) -> Node<'mcx> {
    for s in ctx.steps.iter() {
        if step_id_of(s) == step_id {
            return s;
        }
    }
    panic!("pruning step {step_id} not in context");
}

fn gen_prune_step_op<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ctx: &mut GenCtx<'mcx>,
    opstrategy: u16,
    op_is_ne: bool,
    exprs: NodeList<'mcx>,
    cmpfns: OidList<'mcx>,
    nullkeys: Bitmapset<'mcx>,
) -> PgResult<i32> {
    let mcx = run.mcx;
    let mut op = Node::build::<types_nodes::plannodes::PartitionPruneStepOp>(mcx)?;
    op.step_id = ctx.next_step_id;
    ctx.next_step_id += 1;
    op.opstrategy = if op_is_ne { InvalidStrategy } else { opstrategy };
    debug_assert_eq!(exprs.len(), cmpfns.len());
    op.exprs = exprs;
    op.cmpfns = cmpfns;
    op.nullkeys = nullkeys;
    let id = op.step_id;
    ctx.steps.lappend(mcx, op.seal())?;
    Ok(id)
}

fn gen_prune_step_combine<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ctx: &mut GenCtx<'mcx>,
    source_stepids: IntList<'mcx>,
    combine_op: u32,
) -> PgResult<i32> {
    let mcx = run.mcx;
    let mut c = Node::build::<types_nodes::plannodes::PartitionPruneStepCombine>(mcx)?;
    c.step_id = ctx.next_step_id;
    ctx.next_step_id += 1;
    c.combineOp = combine_op;
    c.source_stepids = source_stepids;
    let id = c.step_id;
    ctx.steps.lappend(mcx, c.seal())?;
    Ok(id)
}

// gen_prune_steps_from_opexps (partprune.c).
fn gen_prune_steps_from_opexps<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ctx: &mut GenCtx<'mcx>,
    keyclauses: &mut [Vec<PartClauseInfo<'mcx>>; PARTITION_MAX_KEYS],
    nullkeys: &Bitmapset<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    let mut opsteps = NodeList::nil();
    let mut btree_clauses: [Vec<PartClauseInfo<'mcx>>; BTMaxStrategyNumber + 1] =
        core::array::from_fn(|_| Vec::new());
    let mut hash_clauses: [Vec<PartClauseInfo<'mcx>>; HTMaxStrategyNumber + 1] =
        core::array::from_fn(|_| Vec::new());

    'keys: for i in 0..ctx.partnatts {
        let clauselist = core::mem::take(&mut keyclauses[i]);
        let mut consider_next_key = true;
        if ctx.strategy == PARTITION_STRATEGY_RANGE && clauselist.is_empty() {
            break;
        }
        if ctx.strategy == PARTITION_STRATEGY_HASH
            && clauselist.is_empty()
            && !nullkeys.is_member(i as i32)
        {
            return Ok(NodeList::nil());
        }
        for mut pc in clauselist {
            if pc.op_strategy == InvalidStrategy {
                let (strat, _lt, _rt) =
                    lsyscache::get_op_opfamily_properties(pc.opno, ctx.partopfamily[i], false)?;
                pc.op_strategy = strat as u16;
            }
            match ctx.strategy {
                PARTITION_STRATEGY_LIST | PARTITION_STRATEGY_RANGE => {
                    if pc.op_strategy == BTLessStrategyNumber
                        || pc.op_strategy == BTGreaterStrategyNumber
                    {
                        consider_next_key = false;
                    }
                    btree_clauses[pc.op_strategy as usize].push(pc);
                }
                PARTITION_STRATEGY_HASH => {
                    assert!(
                        pc.op_strategy == HTEqualStrategyNumber,
                        "invalid clause for hash partitioning"
                    );
                    hash_clauses[pc.op_strategy as usize].push(pc);
                }
                other => panic!("invalid partition strategy: {}", other as char),
            }
        }
        if !consider_next_key {
            break 'keys;
        }
    }

    match ctx.strategy {
        PARTITION_STRATEGY_LIST | PARTITION_STRATEGY_RANGE => {
            for strat in 1..=BTMaxStrategyNumber {
                for ci in 0..btree_clauses[strat].len() {
                    let pc = btree_clauses[strat][ci].clone();
                    if pc.keyno == 0 {
                        debug_assert_eq!(pc.op_strategy as usize, strat);
                        let pc_steps = get_steps_using_prefix(
                            run,
                            ctx,
                            strat as u16,
                            pc.op_is_ne,
                            pc.expr,
                            pc.cmpfn,
                            Bitmapset::empty(),
                            &[],
                        )?;
                        for s in pc_steps.iter() {
                            opsteps.lappend(mcx, s)?;
                        }
                        continue;
                    }
                    // Prefix: = clauses for earlier keys, plus <=/>= clauses
                    // matching the step's direction, in ascending keyno order.
                    let mut prefix: Vec<PartClauseInfo<'mcx>> = Vec::new();
                    let mut eq_i = 0usize;
                    let mut le_i = 0usize;
                    let mut ge_i = 0usize;
                    let mut prefix_valid = true;
                    let eq_clauses = &btree_clauses[BTEqualStrategyNumber as usize];
                    let le_clauses = &btree_clauses[BTLessEqualStrategyNumber as usize];
                    let ge_clauses = &btree_clauses[BTGreaterEqualStrategyNumber as usize];
                    for keyno in 0..pc.keyno {
                        let mut pk_has_clauses = false;
                        while eq_i < eq_clauses.len() {
                            let eqpc = &eq_clauses[eq_i];
                            if eqpc.keyno == keyno {
                                prefix.push(eqpc.clone());
                                pk_has_clauses = true;
                                eq_i += 1;
                            } else {
                                debug_assert!(eqpc.keyno > keyno);
                                break;
                            }
                        }
                        if strat == BTLessStrategyNumber as usize
                            || strat == BTLessEqualStrategyNumber as usize
                        {
                            while le_i < le_clauses.len() {
                                let lepc = &le_clauses[le_i];
                                if lepc.keyno == keyno {
                                    prefix.push(lepc.clone());
                                    pk_has_clauses = true;
                                    le_i += 1;
                                } else {
                                    debug_assert!(lepc.keyno > keyno);
                                    break;
                                }
                            }
                        }
                        if strat == BTGreaterStrategyNumber as usize
                            || strat == BTGreaterEqualStrategyNumber as usize
                        {
                            while ge_i < ge_clauses.len() {
                                let gepc = &ge_clauses[ge_i];
                                if gepc.keyno == keyno {
                                    prefix.push(gepc.clone());
                                    pk_has_clauses = true;
                                    ge_i += 1;
                                } else {
                                    debug_assert!(gepc.keyno > keyno);
                                    break;
                                }
                            }
                        }
                        if !pk_has_clauses {
                            prefix_valid = false;
                            break;
                        }
                    }
                    if prefix_valid {
                        prefix.sort_by_key(|p| p.keyno);
                        debug_assert_eq!(pc.op_strategy as usize, strat);
                        let pc_steps = get_steps_using_prefix(
                            run,
                            ctx,
                            strat as u16,
                            pc.op_is_ne,
                            pc.expr,
                            pc.cmpfn,
                            Bitmapset::empty(),
                            &prefix,
                        )?;
                        for s in pc_steps.iter() {
                            opsteps.lappend(mcx, s)?;
                        }
                    } else {
                        break;
                    }
                }
            }
        }
        PARTITION_STRATEGY_HASH => {
            let eq_clauses = &hash_clauses[HTEqualStrategyNumber as usize];
            if !eq_clauses.is_empty() {
                let last_keyno = eq_clauses.last().unwrap().keyno;
                let mut prefix: Vec<PartClauseInfo<'mcx>> = Vec::new();
                let mut start = 0usize;
                for (idx, pc) in eq_clauses.iter().enumerate() {
                    if pc.keyno == last_keyno {
                        start = idx;
                        break;
                    }
                    prefix.push(pc.clone());
                }
                for pc in &eq_clauses[start..] {
                    debug_assert_eq!(pc.op_strategy, HTEqualStrategyNumber);
                    let pc_steps = get_steps_using_prefix(
                        run,
                        ctx,
                        HTEqualStrategyNumber,
                        false,
                        pc.expr,
                        pc.cmpfn,
                        nullkeys.clone_in(mcx)?,
                        &prefix,
                    )?;
                    for s in pc_steps.iter() {
                        opsteps.lappend(mcx, s)?;
                    }
                }
            }
        }
        other => panic!("invalid partition strategy: {}", other as char),
    }
    Ok(opsteps)
}

// get_steps_using_prefix + _recurse (partprune.c).
#[allow(clippy::too_many_arguments)]
fn get_steps_using_prefix<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ctx: &mut GenCtx<'mcx>,
    step_opstrategy: u16,
    step_op_is_ne: bool,
    step_lastexpr: Node<'mcx>,
    step_lastcmpfn: Oid,
    step_nullkeys: Bitmapset<'mcx>,
    prefix: &[PartClauseInfo<'mcx>],
) -> PgResult<NodeList<'mcx>> {
    debug_assert!(step_nullkeys.is_empty() || ctx.strategy == PARTITION_STRATEGY_HASH);
    let mcx = run.mcx;
    if prefix.is_empty() {
        let sid = gen_prune_step_op(
            run,
            ctx,
            step_opstrategy,
            step_op_is_ne,
            NodeList::make1(mcx, step_lastexpr)?,
            OidList::make1(mcx, step_lastcmpfn)?,
            step_nullkeys,
        )?;
        return NodeList::make1(mcx, step_node(ctx, sid));
    }
    get_steps_using_prefix_recurse(
        run,
        ctx,
        step_opstrategy,
        step_op_is_ne,
        step_lastexpr,
        step_lastcmpfn,
        &step_nullkeys,
        prefix,
        0,
        &mut Vec::new(),
        &mut Vec::new(),
    )
}

#[allow(clippy::too_many_arguments)]
fn get_steps_using_prefix_recurse<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ctx: &mut GenCtx<'mcx>,
    step_opstrategy: u16,
    step_op_is_ne: bool,
    step_lastexpr: Node<'mcx>,
    step_lastcmpfn: Oid,
    step_nullkeys: &Bitmapset<'mcx>,
    prefix: &[PartClauseInfo<'mcx>],
    start: usize,
    step_exprs: &mut Vec<Node<'mcx>>,
    step_cmpfns: &mut Vec<Oid>,
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    let mut result = NodeList::nil();
    let cur_keyno = prefix[start].keyno;
    let final_keyno = prefix.last().unwrap().keyno;

    if cur_keyno < final_keyno {
        let mut next_start = start;
        while next_start < prefix.len() && prefix[next_start].keyno <= cur_keyno {
            next_start += 1;
        }
        for pc in &prefix[start..] {
            if pc.keyno != cur_keyno {
                debug_assert!(pc.keyno > cur_keyno);
                break;
            }
            step_exprs.push(pc.expr);
            step_cmpfns.push(pc.cmpfn);
            let moresteps = get_steps_using_prefix_recurse(
                run,
                ctx,
                step_opstrategy,
                step_op_is_ne,
                step_lastexpr,
                step_lastcmpfn,
                step_nullkeys,
                prefix,
                next_start,
                step_exprs,
                step_cmpfns,
            )?;
            for s in moresteps.iter() {
                result.lappend(mcx, s)?;
            }
            step_exprs.pop();
            step_cmpfns.pop();
        }
    } else {
        debug_assert!(
            step_exprs.len() == cur_keyno || !step_nullkeys.is_empty(),
            "prefix covers all earlier keys"
        );
        for pc in &prefix[start..] {
            debug_assert_eq!(pc.keyno, cur_keyno);
            let mut exprs = NodeList::nil();
            let mut cmpfns = OidList::nil();
            for &e in step_exprs.iter() {
                exprs.lappend(mcx, e)?;
            }
            for &f in step_cmpfns.iter() {
                cmpfns.lappend(mcx, f)?;
            }
            exprs.lappend(mcx, pc.expr)?;
            exprs.lappend(mcx, step_lastexpr)?;
            cmpfns.lappend(mcx, pc.cmpfn)?;
            cmpfns.lappend(mcx, step_lastcmpfn)?;
            let sid = gen_prune_step_op(
                run,
                ctx,
                step_opstrategy,
                step_op_is_ne,
                exprs,
                cmpfns,
                step_nullkeys.clone_in(mcx)?,
            )?;
            result.lappend(mcx, step_node(ctx, sid))?;
        }
    }
    Ok(result)
}

#[derive(Clone, Copy)]
enum PartClauseMatchStatus {
    NoMatch,
    MatchClause,
    MatchNullness,
    MatchSteps,
    MatchContradict,
    Unsupported,
}

fn part_coll_matches_expr_coll(partcoll: Oid, exprcoll: Oid) -> bool {
    partcoll == InvalidOid || partcoll == exprcoll
}

// match_clause_to_partition_key (partprune.c).
#[allow(clippy::too_many_arguments)]
fn match_clause_to_partition_key<'mcx>(
    run: &mut PlannerRun<'mcx>,
    ctx: &mut GenCtx<'mcx>,
    clause: Node<'mcx>,
    partkey: Node<'mcx>,
    partkeyidx: usize,
    clause_is_not_null: &mut bool,
    pc: &mut Option<PartClauseInfo<'mcx>>,
    clause_steps: &mut NodeList<'mcx>,
) -> PgResult<PartClauseMatchStatus> {
    let mcx = run.mcx;
    let partopfamily = ctx.partopfamily[partkeyidx];
    let partcoll = ctx.partcollation[partkeyidx];

    let mut outconst: Option<Node<'mcx>> = None;
    let mut notclause = false;
    let boolmatchstatus =
        match_boolean_partition_clause(mcx, partopfamily, clause, partkey, &mut outconst, &mut notclause)?;

    match boolmatchstatus {
        PartClauseMatchStatus::MatchClause => {
            if notclause {
                let btest = clause
                    .as_boolean_test()
                    .expect("notclause only set for BooleanTests");
                let mut new_booltest =
                    Node::build::<types_nodes::primnodes::BooleanTest>(mcx)?;
                new_booltest.arg = btest.arg;
                new_booltest.booltesttype = match btest.booltesttype {
                    BoolTestType::IS_NOT_TRUE => BoolTestType::IS_FALSE,
                    BoolTestType::IS_NOT_FALSE => BoolTestType::IS_TRUE,
                    other => panic!("unexpected booltesttype {other:?}"),
                };
                new_booltest.location = -1;
                let mut nulltest = Node::build::<types_nodes::primnodes::NullTest>(mcx)?;
                nulltest.arg = Some(partkey);
                nulltest.nulltesttype = NullTestType::IS_NULL;
                nulltest.argisrow = false;
                nulltest.location = -1;
                let mut or = Node::build::<types_nodes::primnodes::BoolExpr>(mcx)?;
                or.boolop = BoolExprType::OR_EXPR;
                or.args = NodeList::nil();
                or.args.lappend(mcx, new_booltest.seal())?;
                or.args.lappend(mcx, nulltest.seal())?;
                or.location = -1;
                let or_clause = [or.seal()];
                *clause_steps = gen_partprune_steps_internal(run, ctx, &or_clause)?;
                if ctx.contradictory {
                    return Ok(PartClauseMatchStatus::MatchContradict);
                } else if clause_steps.is_nil() {
                    return Ok(PartClauseMatchStatus::Unsupported);
                }
                return Ok(PartClauseMatchStatus::MatchSteps);
            }
            *pc = Some(PartClauseInfo {
                keyno: partkeyidx,
                opno: BOOLEAN_EQUAL_OPERATOR,
                op_is_ne: false,
                expr: outconst.expect("boolean match sets outconst"),
                cmpfn: ctx.partsupfunc_oids[partkeyidx],
                op_strategy: InvalidStrategy,
            });
            return Ok(PartClauseMatchStatus::MatchClause);
        }
        PartClauseMatchStatus::MatchNullness => {
            *clause_is_not_null = notclause;
            return Ok(PartClauseMatchStatus::MatchNullness);
        }
        _ => {}
    }

    if let Some(opclause) = clause.as_op_expr() {
        if opclause.args.len() == 2 {
            let leftop = strip_relabel(opclause.args.nth(0));
            let rightop = strip_relabel(opclause.args.nth(1));
            let mut opno = opclause.opno;
            let mut negator = InvalidOid;
            let mut is_opne_listp = false;

            let expr;
            if types_nodes::equal::equal(leftop, partkey) {
                expr = rightop;
            } else if types_nodes::equal::equal(rightop, partkey) {
                opno = lsyscache::get_commutator(opno)?;
                if opno == InvalidOid {
                    return Ok(PartClauseMatchStatus::Unsupported);
                }
                expr = leftop;
            } else {
                return Ok(PartClauseMatchStatus::NoMatch);
            }

            if !part_coll_matches_expr_coll(partcoll, opclause.inputcollid) {
                return Ok(PartClauseMatchStatus::NoMatch);
            }

            let op_strategy;
            let op_righttype;
            if lsyscache::op_in_opfamily(opno, partopfamily)? {
                let (strat, _lt, rt) =
                    lsyscache::get_op_opfamily_properties(opno, partopfamily, false)?;
                op_strategy = strat as u16;
                op_righttype = rt;
            } else {
                if ctx.strategy != PARTITION_STRATEGY_LIST {
                    return Ok(PartClauseMatchStatus::Unsupported);
                }
                negator = lsyscache::get_negator(opno)?;
                let mut strat_rt = None;
                if negator != InvalidOid && lsyscache::op_in_opfamily(negator, partopfamily)? {
                    let (strat, _lt, rt) =
                        lsyscache::get_op_opfamily_properties(negator, partopfamily, false)?;
                    if strat as u16 == BTEqualStrategyNumber {
                        is_opne_listp = true;
                        strat_rt = Some((strat as u16, rt));
                    }
                }
                if !is_opne_listp {
                    return Ok(PartClauseMatchStatus::NoMatch);
                }
                let (strat, rt) = strat_rt.unwrap();
                op_strategy = strat;
                op_righttype = rt;
            }

            if !lsyscache::op_strict(opno)? {
                return Ok(PartClauseMatchStatus::Unsupported);
            }

            if expr.node_tag() != NodeTag::T_Const {
                if ctx.target == PartClauseTarget::Planner {
                    return Ok(PartClauseMatchStatus::Unsupported);
                }
                if vars::contain_var_clause(expr)? {
                    return Ok(PartClauseMatchStatus::Unsupported);
                }
                if clauses::contain_volatile_functions(expr)? {
                    return Ok(PartClauseMatchStatus::Unsupported);
                }
                if contains_exec_param(expr)? {
                    ctx.has_exec_param = true;
                    if ctx.target != PartClauseTarget::Exec {
                        return Ok(PartClauseMatchStatus::Unsupported);
                    }
                } else {
                    ctx.has_mutable_arg = true;
                }
            }

            if lsyscache::op_volatile(opno)? != PROVOLATILE_IMMUTABLE {
                ctx.has_mutable_op = true;
                if ctx.target == PartClauseTarget::Planner {
                    return Ok(PartClauseMatchStatus::Unsupported);
                }
            }

            let cmpfn = if op_righttype == ctx.partopcintype[partkeyidx] {
                ctx.partsupfunc_oids[partkeyidx]
            } else {
                let f = match ctx.strategy {
                    PARTITION_STRATEGY_LIST | PARTITION_STRATEGY_RANGE => {
                        lsyscache::get_opfamily_proc(
                            partopfamily,
                            ctx.partopcintype[partkeyidx],
                            op_righttype,
                            BTORDER_PROC,
                        )?
                    }
                    PARTITION_STRATEGY_HASH => lsyscache::get_opfamily_proc(
                        partopfamily,
                        op_righttype,
                        op_righttype,
                        HASHEXTENDED_PROC,
                    )?,
                    other => panic!("invalid partition strategy: {}", other as char),
                };
                if f == InvalidOid {
                    return Ok(PartClauseMatchStatus::NoMatch);
                }
                f
            };

            *pc = Some(if is_opne_listp {
                debug_assert!(negator != InvalidOid);
                PartClauseInfo {
                    keyno: partkeyidx,
                    opno: negator,
                    op_is_ne: true,
                    expr,
                    cmpfn,
                    op_strategy: InvalidStrategy,
                }
            } else {
                PartClauseInfo {
                    keyno: partkeyidx,
                    opno,
                    op_is_ne: false,
                    expr,
                    cmpfn,
                    op_strategy,
                }
            });
            return Ok(PartClauseMatchStatus::MatchClause);
        }
    }

    if let Some(saop) = clause.as_scalar_array_op_expr() {
        let saop_op = saop.opno;
        let saop_coll = saop.inputcollid;
        let leftop = strip_relabel(saop.args.nth(0));
        let rightop = saop.args.nth(1);

        if !types_nodes::equal::equal(leftop, partkey)
            || !part_coll_matches_expr_coll(partcoll, saop.inputcollid)
        {
            return Ok(PartClauseMatchStatus::NoMatch);
        }

        if !lsyscache::op_in_opfamily(saop_op, partopfamily)? {
            if ctx.strategy != PARTITION_STRATEGY_LIST {
                return Ok(PartClauseMatchStatus::NoMatch);
            }
            let negator = lsyscache::get_negator(saop_op)?;
            if negator != InvalidOid && lsyscache::op_in_opfamily(negator, partopfamily)? {
                let (strat, _lt, _rt) =
                    lsyscache::get_op_opfamily_properties(negator, partopfamily, false)?;
                if strat as u16 != BTEqualStrategyNumber {
                    return Ok(PartClauseMatchStatus::NoMatch);
                }
            } else {
                return Ok(PartClauseMatchStatus::NoMatch);
            }
        }

        if !lsyscache::op_strict(saop_op)? {
            return Ok(PartClauseMatchStatus::Unsupported);
        }

        if rightop.node_tag() != NodeTag::T_Const {
            if ctx.target == PartClauseTarget::Planner {
                return Ok(PartClauseMatchStatus::Unsupported);
            }
            if vars::contain_var_clause(rightop)? {
                return Ok(PartClauseMatchStatus::Unsupported);
            }
            if clauses::contain_volatile_functions(rightop)? {
                return Ok(PartClauseMatchStatus::Unsupported);
            }
            if contains_exec_param(rightop)? {
                ctx.has_exec_param = true;
                if ctx.target != PartClauseTarget::Exec {
                    return Ok(PartClauseMatchStatus::Unsupported);
                }
            } else {
                ctx.has_mutable_arg = true;
            }
        }

        if lsyscache::op_volatile(saop_op)? != PROVOLATILE_IMMUTABLE {
            ctx.has_mutable_op = true;
            if ctx.target == PartClauseTarget::Planner {
                return Ok(PartClauseMatchStatus::Unsupported);
            }
        }

        let mut elem_exprs: Vec<Node<'mcx>> = Vec::new();
        if let Some(arr) = rightop.as_const() {
            if arr.constisnull {
                return Ok(PartClauseMatchStatus::MatchContradict);
            }
            let p = arr.constvalue.as_usize() as *const u8;
            // SAFETY: array Consts are 4B-header inline images (parser and
            // eval_const_expressions both produce untoasted arrays).
            let img = unsafe {
                let b0 = *p;
                assert!(b0 != 0x01 && b0 & 0x03 == 0, "toasted/packed array const");
                core::slice::from_raw_parts(
                    p,
                    arrayfuncs::arr_size(core::slice::from_raw_parts(p, 4)),
                )
            };
            let elemtype = arrayfuncs::arr_elemtype(img);
            let (elmlen, elmbyval, elmalign) = lsyscache::get_typlenbyvalalign(elemtype)?;
            let (elem_values, elem_nulls) =
                arrayfuncs::deconstruct_array(mcx, img, elmlen as i32, elmbyval, elmalign as u8, true)?;
            for (i, &v) in elem_values.iter().enumerate() {
                if elem_nulls[i] {
                    if saop.useOr {
                        continue;
                    }
                    return Ok(PartClauseMatchStatus::MatchContradict);
                }
                elem_exprs.push(Node::mk(
                    mcx,
                    types_nodes::primnodes::Const {
                        consttype: elemtype,
                        consttypmod: -1,
                        constcollid: arr.constcollid,
                        constlen: elmlen as i32,
                        constvalue: v,
                        constisnull: false,
                        constbyval: elmbyval,
                        location: -1,
                    },
                )?);
            }
        } else if let Some(arrexpr) = rightop.as_array_expr() {
            if arrexpr.multidims {
                return Ok(PartClauseMatchStatus::Unsupported);
            }
            elem_exprs.extend(arrexpr.elements.iter());
        } else {
            return Ok(PartClauseMatchStatus::Unsupported);
        }

        let mut elem_clauses: Vec<Node<'mcx>> = Vec::with_capacity(elem_exprs.len());
        for elem in elem_exprs {
            let mut op = Node::build::<types_nodes::primnodes::OpExpr>(mcx)?;
            op.opno = saop_op;
            op.opfuncid = lsyscache::get_opcode(saop_op)?;
            op.opresulttype = types_core::BOOLOID;
            op.opretset = false;
            op.opcollid = InvalidOid;
            op.inputcollid = saop_coll;
            op.args = NodeList::nil();
            op.args.lappend(mcx, leftop)?;
            op.args.lappend(mcx, elem)?;
            op.location = -1;
            elem_clauses.push(op.seal());
        }

        let steps_input: Vec<Node<'mcx>> = if saop.useOr && elem_clauses.len() > 1 {
            let mut or = Node::build::<types_nodes::primnodes::BoolExpr>(mcx)?;
            or.boolop = BoolExprType::OR_EXPR;
            or.args = NodeList::nil();
            for c in &elem_clauses {
                or.args.lappend(mcx, *c)?;
            }
            or.location = -1;
            vec![or.seal()]
        } else {
            elem_clauses
        };

        *clause_steps = gen_partprune_steps_internal(run, ctx, &steps_input)?;
        if ctx.contradictory {
            return Ok(PartClauseMatchStatus::MatchContradict);
        } else if clause_steps.is_nil() {
            return Ok(PartClauseMatchStatus::Unsupported);
        }
        return Ok(PartClauseMatchStatus::MatchSteps);
    }

    if let Some(nulltest) = clause.as_null_test() {
        let arg = strip_relabel(nulltest.arg.expect("NullTest.arg"));
        if !types_nodes::equal::equal(arg, partkey) {
            return Ok(PartClauseMatchStatus::NoMatch);
        }
        *clause_is_not_null = nulltest.nulltesttype == NullTestType::IS_NOT_NULL;
        return Ok(PartClauseMatchStatus::MatchNullness);
    }

    Ok(boolmatchstatus)
}

// match_boolean_partition_clause (partprune.c).
fn match_boolean_partition_clause<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    partopfamily: Oid,
    clause: Node<'mcx>,
    partkey: Node<'mcx>,
    outconst: &mut Option<Node<'mcx>>,
    notclause: &mut bool,
) -> PgResult<PartClauseMatchStatus> {
    *outconst = None;
    *notclause = false;

    if partopfamily != BOOL_BTREE_FAM_OID && partopfamily != BOOL_HASH_FAM_OID {
        return Ok(PartClauseMatchStatus::Unsupported);
    }

    if let Some(btest) = clause.as_boolean_test() {
        let leftop = strip_relabel(btest.arg.expect("BooleanTest.arg"));
        if types_nodes::equal::equal(leftop, partkey) {
            return Ok(match btest.booltesttype {
                BoolTestType::IS_NOT_TRUE | BoolTestType::IS_TRUE => {
                    *notclause = btest.booltesttype == BoolTestType::IS_NOT_TRUE;
                    *outconst = Some(clauses::make_bool_const(mcx, true, false)?);
                    PartClauseMatchStatus::MatchClause
                }
                BoolTestType::IS_NOT_FALSE | BoolTestType::IS_FALSE => {
                    *notclause = btest.booltesttype == BoolTestType::IS_NOT_FALSE;
                    *outconst = Some(clauses::make_bool_const(mcx, false, false)?);
                    PartClauseMatchStatus::MatchClause
                }
                BoolTestType::IS_NOT_UNKNOWN | BoolTestType::IS_UNKNOWN => {
                    *notclause = btest.booltesttype == BoolTestType::IS_NOT_UNKNOWN;
                    PartClauseMatchStatus::MatchNullness
                }
            });
        }
        return Ok(PartClauseMatchStatus::NoMatch);
    }

    let is_not_clause = clauses::is_notclause(clause);
    let leftop = if is_not_clause {
        strip_relabel(clause.as_bool_expr().unwrap().args.nth(0))
    } else {
        strip_relabel(clause)
    };

    if types_nodes::equal::equal(leftop, partkey) {
        *outconst = Some(clauses::make_bool_const(mcx, !is_not_clause, false)?);
    } else if types_nodes::equal::equal(clauses::negate_clause(mcx, leftop)?, partkey) {
        *outconst = Some(clauses::make_bool_const(mcx, is_not_clause, false)?);
    } else {
        return Ok(PartClauseMatchStatus::NoMatch);
    }
    Ok(PartClauseMatchStatus::MatchClause)
}

struct ExecParamWalker(bool);

impl<'mcx> NodeWalker<'mcx> for ExecParamWalker {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(p) = node.as_param() {
            if p.paramkind == ParamKind::PARAM_EXEC {
                self.0 = true;
                return Ok(true);
            }
            return Ok(false);
        }
        clauses::expression_tree_walker(node, self)
    }
}

fn contains_exec_param(expr: Node<'_>) -> PgResult<bool> {
    let mut w = ExecParamWalker(false);
    let _ = w.visit(expr)?;
    Ok(w.0)
}

// pull_exec_paramids (partprune.c).
struct ExecParamIdsWalker<'mcx> {
    mcx: mcx::Mcx<'mcx>,
    ids: Bitmapset<'mcx>,
}

impl<'mcx> NodeWalker<'mcx> for ExecParamIdsWalker<'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(p) = node.as_param() {
            if p.paramkind == ParamKind::PARAM_EXEC {
                self.ids.add_member(self.mcx, p.paramid)?;
            }
            return Ok(false);
        }
        clauses::expression_tree_walker(node, self)
    }
}

// get_partkey_exec_paramids (partprune.c).
fn get_partkey_exec_paramids<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    steps: &NodeList<'mcx>,
) -> PgResult<Bitmapset<'mcx>> {
    let mut w = ExecParamIdsWalker { mcx, ids: Bitmapset::empty() };
    for step in steps.iter() {
        let Some(op) = step.as_partition_prune_step_op() else { continue };
        for expr in op.exprs.iter() {
            if expr.node_tag() != NodeTag::T_Const {
                let _ = w.visit(expr)?;
            }
        }
    }
    Ok(w.ids)
}

// make_partition_pruneinfo (partprune.c). Returns the index into the run's
// pending pruneinfo list (root->partPruneInfos), or -1.
pub(crate) fn make_partition_pruneinfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parentrel: RelId,
    subpaths: &[types_pathnodes::PathId],
    prunequal: &[Node<'mcx>],
) -> PgResult<i32> {
    let mcx = run.mcx;
    let rel_array_size = run.root.simple_rel_array.len();
    let mut allpartrelids: Vec<Bitmapset<'mcx>> = Vec::new();
    let mut relid_subplan_map = vec![0i32; rel_array_size];

    for (i, &sp) in subpaths.iter().enumerate() {
        let pathrel = run.root.path(sp).base().parent;
        if run.root.rel(pathrel).reloptkind == types_pathnodes::RELOPT_OTHER_MEMBER_REL {
            let mut prel = pathrel;
            let mut partrelids = Bitmapset::empty();
            loop {
                let prelid = run.root.rel(prel).relid as usize;
                debug_assert!(prelid < rel_array_size);
                let appinfo = run.root.append_rel_array[prelid]
                    .as_ref()
                    .expect("other-member rel has an AppendRelInfo");
                let parent_relid = appinfo.parent_relid;
                prel = find_base_rel(&run.root, parent_relid as i32);
                if run.root.rel(prel).part_scheme.is_none() {
                    break;
                }
                partrelids.add_member(mcx, parent_relid as i32)?;
                if prel == parentrel {
                    break;
                }
                if run.root.rel(prel).reloptkind != types_pathnodes::RELOPT_OTHER_MEMBER_REL {
                    break;
                }
            }
            if !partrelids.is_empty() {
                add_part_relids(mcx, &mut allpartrelids, partrelids)?;
                debug_assert!(relid_subplan_map[run.root.rel(pathrel).relid as usize] == 0);
                relid_subplan_map[run.root.rel(pathrel).relid as usize] = i as i32 + 1;
            }
        }
    }

    let mut prunerelinfos: Vec<NodeList<'mcx>> = Vec::new();
    let mut allmatchedsubplans = Bitmapset::empty();
    for partrelids in &allpartrelids {
        let mut matchedsubplans = Bitmapset::empty();
        let pinfolist = make_partitionedrel_pruneinfo(
            run,
            parentrel,
            prunequal,
            partrelids,
            &relid_subplan_map,
            &mut matchedsubplans,
        )?;
        if !pinfolist.is_nil() {
            prunerelinfos.push(pinfolist);
            allmatchedsubplans.add_members(mcx, &matchedsubplans)?;
        }
    }

    if prunerelinfos.is_empty() {
        return Ok(-1);
    }

    let mut pruneinfo = Node::build::<types_nodes::plannodes::PartitionPruneInfo>(mcx)?;
    {
        let mut relids = Bitmapset::empty();
        for m in crate::relnode::relids_members(&run.root.rel(parentrel).relids) {
            relids.add_member(mcx, m)?;
        }
        pruneinfo.relids = relids;
    }
    let mut infos = NodeList::nil();
    for l in prunerelinfos {
        infos.lappend(mcx, Node::mk_list(mcx, l)?)?;
    }
    pruneinfo.prune_infos = infos;
    if (allmatchedsubplans.num_members() as usize) < subpaths.len() {
        let mut other = Bitmapset::empty();
        partprune::bms_add_range(mcx, &mut other, 0, subpaths.len() as i32 - 1)?;
        other.del_members(&allmatchedsubplans);
        pruneinfo.other_subplans = other;
    }

    run.pending_part_prune_infos.lappend(mcx, pruneinfo.seal())?;
    Ok(run.pending_part_prune_infos.len() as i32 - 1)
}

// add_part_relids (partprune.c).
fn add_part_relids<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    allpartrelids: &mut Vec<Bitmapset<'mcx>>,
    partrelids: Bitmapset<'mcx>,
) -> PgResult<()> {
    let targetpart = partrelids.next_member(-1);
    debug_assert!(targetpart > 0);
    for cur in allpartrelids.iter_mut() {
        if cur.next_member(-1) == targetpart {
            cur.add_members(mcx, &partrelids)?;
            return Ok(());
        }
    }
    allpartrelids.push(partrelids);
    Ok(())
}

// make_partitionedrel_pruneinfo (partprune.c).
fn make_partitionedrel_pruneinfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parentrel: RelId,
    prunequal: &[Node<'mcx>],
    partrelids: &Bitmapset<'mcx>,
    relid_subplan_map: &[i32],
    matchedsubplans: &mut Bitmapset<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    let rel_array_size = run.root.simple_rel_array.len();
    let mut relid_subpart_map = vec![0i32; rel_array_size];
    let mut pinfolist = NodeList::nil();
    let mut doruntimeprune = false;
    let mut targetpart: Option<RelId> = None;
    let mut prunequal_tr: Vec<Node<'mcx>> = prunequal.to_vec();

    let mut i = 1;
    let mut rti = partrelids.next_member(-1);
    while rti > 0 {
        let subpart = find_base_rel(&run.root, rti);
        debug_assert!((rti as usize) < rel_array_size);
        relid_subpart_map[rti as usize] = i;
        i += 1;

        let partprunequal: Vec<Node<'mcx>>;
        if targetpart.is_none() {
            targetpart = Some(subpart);
            // prunequal arrives phrased for parentrel; a UNION ALL or
            // partitionwise parent needs a translation down to the target
            // partitioned rel, kept in prunequal_tr for the later children
            // (partprune.c:510).
            if !crate::relnode::relids_equal(
                &run.root.rel(parentrel).relids,
                &run.root.rel(subpart).relids,
            ) {
                let relids =
                    crate::relnode::relids_copy(mcx, &run.root.rel(subpart).relids);
                let appinfos = crate::inherit::find_appinfos_by_relids(run, &relids);
                let mut translated = Vec::with_capacity(prunequal_tr.len());
                for q in prunequal_tr {
                    translated.push(crate::inherit::adjust_appendrel_attrs_multi(
                        run, q, &appinfos,
                    )?);
                }
                prunequal_tr = translated;
            }
            partprunequal = prunequal_tr.clone();
        } else {
            // adjust_appendrel_attrs_multilevel: translate from targetpart
            // down to subpart along the parent chain.
            let mut chain: Vec<RelId> = Vec::new();
            let mut cur = subpart;
            while cur != targetpart.unwrap() {
                chain.push(cur);
                cur = run.root.rel(cur).parent.expect("child rel has a parent");
            }
            let mut quals = prunequal_tr.clone();
            for &level in chain.iter().rev() {
                let relid = run.root.rel(level).relid as usize;
                let appinfo = run.root.append_rel_array[relid]
                    .clone()
                    .expect("child rel has an AppendRelInfo");
                let mut translated = Vec::with_capacity(quals.len());
                for q in quals {
                    translated.push(crate::inherit::adjust_appendrel_attrs(run, q, &appinfo)?);
                }
                quals = translated;
            }
            partprunequal = quals;
        }

        let ctx = gen_partprune_steps(run, subpart, &partprunequal, PartClauseTarget::Initial)?;
        if ctx.contradictory {
            return Ok(NodeList::nil());
        }
        let initial_pruning_steps = if ctx.has_mutable_op || ctx.has_mutable_arg {
            ctx.steps
        } else {
            NodeList::nil()
        };
        let (exec_pruning_steps, execparamids) = if ctx.has_exec_param {
            let ectx = gen_partprune_steps(run, subpart, &partprunequal, PartClauseTarget::Exec)?;
            if ectx.contradictory {
                return Ok(NodeList::nil());
            }
            let ids = get_partkey_exec_paramids(mcx, &ectx.steps)?;
            if ids.is_empty() {
                (NodeList::nil(), Bitmapset::empty())
            } else {
                (ectx.steps, ids)
            }
        } else {
            (NodeList::nil(), Bitmapset::empty())
        };

        if !initial_pruning_steps.is_nil() || !exec_pruning_steps.is_nil() {
            doruntimeprune = true;
        }

        let mut pinfo = Node::build::<types_nodes::plannodes::PartitionedRelPruneInfo>(mcx)?;
        pinfo.rtindex = rti as u32;
        pinfo.initial_pruning_steps = initial_pruning_steps;
        pinfo.exec_pruning_steps = exec_pruning_steps;
        pinfo.execparamids = execparamids;
        pinfolist.lappend(mcx, pinfo.seal())?;

        rti = partrelids.next_member(rti);
    }

    if !doruntimeprune {
        return Ok(NodeList::nil());
    }

    for pnode in pinfolist.iter() {
        let rtindex = pnode.as_partitioned_rel_prune_info().unwrap().rtindex;
        let subpart = find_base_rel(&run.root, rtindex as i32);
        let nparts = run.root.rel(subpart).nparts;
        let mut subplan_map = mcx::vec_from_elem_in(mcx, -1i32, nparts as usize);
        let mut subpart_map = mcx::vec_from_elem_in(mcx, -1i32, nparts as usize);
        let mut relid_map = mcx::vec_from_elem_in(mcx, 0 as Oid, nparts as usize);
        let mut leafpart_rti_map = mcx::vec_from_elem_in(mcx, 0i32, nparts as usize);
        let mut present_parts = Bitmapset::empty();

        for pi in crate::relnode::relids_members(&run.root.rel(subpart).live_parts) {
            let partrel = run.root.rel(subpart).part_rels[pi as usize]
                .expect("live partition has a RelOptInfo");
            let prelid = run.root.rel(partrel).relid as usize;
            let subplanidx = relid_subplan_map[prelid] - 1;
            let subpartidx = relid_subpart_map[prelid] - 1;
            subplan_map[pi as usize] = subplanidx;
            subpart_map[pi as usize] = subpartidx;
            relid_map[pi as usize] = run.rte(prelid).relid;
            if subplanidx >= 0 {
                present_parts.add_member(mcx, pi)?;
                if run.root.rel(partrel).nparts == -1 {
                    leafpart_rti_map[pi as usize] = prelid as i32;
                }
                matchedsubplans.add_member(mcx, subplanidx)?;
            } else if subpartidx >= 0 {
                present_parts.add_member(mcx, pi)?;
            }
        }

        debug_assert!(!present_parts.is_empty());
        // SAFETY: exclusive ownership of the freshly built pruneinfo nodes.
        unsafe {
            pnode.with_mut::<types_nodes::plannodes::PartitionedRelPruneInfo, _>(|p| {
                p.present_parts = present_parts;
                p.nparts = nparts;
                p.subplan_map = subplan_map.leak();
                p.subpart_map = subpart_map.leak();
                p.relid_map = relid_map.leak();
                p.leafpart_rti_map = leafpart_rti_map.leak();
            })
        }
        .expect("PartitionedRelPruneInfo node");
    }

    Ok(pinfolist)
}
