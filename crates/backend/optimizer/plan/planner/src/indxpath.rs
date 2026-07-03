//! indxpath.c slice: create_index_paths over restriction clauses for the
//! Var-op-Const btree shape; everything else loud or dead upstream.


use mcx::PgVec;
use types_error::PgResult;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{IndexClause, IndexOptInfo, PathId, RelId, RinfoId};

use crate::pathnode::add_path;
use crate::relnode::{relids_is_member, relids_is_subset};
use crate::run::PlannerRun;

pub struct IndexClauseSet<'mcx> {
    pub nonempty: bool,
    pub indexclauses: PgVec<'mcx, PgVec<'mcx, IndexClause<'mcx>>>,
}

impl<'mcx> IndexClauseSet<'mcx> {
    fn new(mcx: mcx::Mcx<'mcx>, ncols: usize) -> Self {
        let mut indexclauses = PgVec::new_in(mcx);
        for _ in 0..ncols {
            indexclauses.push(PgVec::new_in(mcx));
        }
        IndexClauseSet { nonempty: false, indexclauses }
    }
}

// check_index_predicates (indxpath.c): the non-partial arm shares the rel's
// baserestrictinfo with each index (partial indexes panicked upstream).
pub fn check_index_predicates<'mcx>(run: &mut PlannerRun<'mcx>, rel: RelId) {
    let mcx = run.mcx;
    let nindexes = run.root.rel(rel).indexlist.len();
    for i in 0..nindexes {
        let index = run.root.rel(rel).indexlist[i];
        assert!(index.indpred.is_empty(), "check_index_predicates (indxpath.c): M2 partial-index lane");
        let mut clauses = PgVec::new_in(mcx);
        clauses.extend(run.root.rel(rel).baserestrictinfo.iter().copied());
        *index.indrestrictinfo.borrow_mut() = clauses;
        index.predOK.set(false);
    }
}

// create_index_paths (indxpath.c), restriction-clause arm; join/eclass
// matching is dead while joininfo/eq_classes are empty (asserted).
pub fn create_index_paths<'mcx>(run: &mut PlannerRun<'mcx>, rel: RelId) -> PgResult<()> {
    if run.root.rel(rel).indexlist.is_empty() {
        return Ok(());
    }
    // Single-member, non-const ECs (sort-expr ECs) derive no implied
    // equalities; anything richer is the join lane.
    let ec_can_derive = (0..run.root.eq_classes.len()).any(|i| {
        let ec = run.root.ec(types_pathnodes::EcId(i as u32));
        ec.ec_members.len() > 1 || ec.ec_has_const
    });
    assert!(
        !ec_can_derive,
        "match_eclass_clauses_to_index (indxpath.c): M2 join lane"
    );
    // DIVERGENCE: match_join_clauses_to_index is skipped -- it only yields
    // parameterized index paths, which every consumer on this lane rejects
    // loudly; plan choice (not results) can differ where one would win.

    // C runs generate_bitmap_or_paths here; skipping it would silently
    // diverge plan choice on indexed rels.
    for i in 0..run.root.rel(rel).baserestrictinfo.len() {
        let rid = run.root.rel(rel).baserestrictinfo[i];
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if clauses::is_orclause(clause) {
            panic!(
                "generate_bitmap_or_paths (indxpath.c): OR restriction on an indexed rel; \
                 M2 OR-index/BitmapOr lane"
            );
        }
    }

    let mut bitindexpaths: PgVec<'mcx, PathId> = PgVec::new_in(run.mcx);
    let nindexes = run.root.rel(rel).indexlist.len();
    for idx in 0..nindexes {
        let index = run.root.rel(rel).indexlist[idx];
        if !index.indpred.is_empty() && !index.predOK.get() {
            continue;
        }
        let mut rclauseset = IndexClauseSet::new(run.mcx, index.nkeycolumns as usize);
        match_restriction_clauses_to_index(run, &index, &mut rclauseset)?;
        get_index_paths(run, rel, &index, &rclauseset, &mut bitindexpaths)?;
    }

    if !bitindexpaths.is_empty() {
        let bitmapqual = crate::pathnode::choose_bitmap_and(run, rel, &bitindexpaths);
        let bpath = crate::pathnode::create_bitmap_heap_path(run, rel, bitmapqual, 1.0)?;
        add_path(run, rel, bpath);
    }
    Ok(())
}

fn match_restriction_clauses_to_index<'mcx>(
    run: &mut PlannerRun<'mcx>,
    index: &IndexOptInfo<'mcx>,
    clauseset: &mut IndexClauseSet<'mcx>,
) -> PgResult<()> {
    let clauses = index.indrestrictinfo.borrow().clone();
    for &rinfo in clauses.iter() {
        match_clause_to_index(run, rinfo, index, clauseset)?;
    }
    Ok(())
}

fn match_clause_to_index<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    index: &IndexOptInfo<'mcx>,
    clauseset: &mut IndexClauseSet<'mcx>,
) -> PgResult<()> {
    if run.root.rinfo(rinfo).pseudoconstant {
        return Ok(());
    }
    // restriction_is_securely_promotable.
    {
        let r = run.root.rinfo(rinfo);
        let index_rel = index.rel.expect("index rel set");
        if !(r.security_level <= run.root.rel(index_rel).baserestrict_min_security || r.leakproof)
        {
            return Ok(());
        }
    }
    for indexcol in 0..index.nkeycolumns as usize {
        if clauseset.indexclauses[indexcol].iter().any(|ic| ic.rinfo == Some(rinfo)) {
            return Ok(());
        }
        if let Some(iclause) = match_clause_to_indexcol(run, rinfo, indexcol, index)? {
            clauseset.indexclauses[indexcol].push(iclause);
            clauseset.nonempty = true;
            return Ok(());
        }
    }
    Ok(())
}

// match_clause_to_indexcol (indxpath.c). Boolean opfamilies (BOOL_BTREE 424 /
// BOOL_HASH 2222) take the match_boolean_index_clause arm in C.
fn match_clause_to_indexcol<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo<'mcx>,
) -> PgResult<Option<IndexClause<'mcx>>> {
    debug_assert!(indexcol < index.nkeycolumns as usize);
    const BOOL_BTREE_FAM_OID: u32 = 424;
    const BOOL_HASH_FAM_OID: u32 = 2222;
    let opfamily = index.opfamily[indexcol];
    if opfamily == BOOL_BTREE_FAM_OID || opfamily == BOOL_HASH_FAM_OID {
        panic!("match_boolean_index_clause (indxpath.c): M2 boolean-index lane");
    }

    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    match clause.node_tag() {
        NodeTag::T_OpExpr => match_opclause_to_indexcol(run, rinfo, indexcol, index),
        NodeTag::T_FuncExpr | NodeTag::T_RelabelType => panic!(
            "match_funcclause_to_indexcol (indxpath.c): M2 support-function lane"
        ),
        // SAOP/RowCompare/NullTest/OR can't be built by the live qual lane.
        _ => Ok(None),
    }
}

// match_opclause_to_indexcol (indxpath.c), indexkey-op-const arm.
fn match_opclause_to_indexcol<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo<'mcx>,
) -> PgResult<Option<IndexClause<'mcx>>> {
    let index_relid = run.root.rel(index.rel.expect("index rel set")).relid;
    let opfamily = index.opfamily[indexcol];
    let idxcollation = index.indexcollations[indexcol];

    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let op = clause.as_op_expr().expect("OpExpr");
    if op.args.len() != 2 {
        return Ok(None);
    }
    let leftop = op.args.nth(0);
    let rightop = op.args.nth(1);
    let left_matches = match_index_to_operand(run, leftop, indexcol, index);
    let right_matches = match_index_to_operand(run, rightop, indexcol, index);

    if left_matches
        && !relids_is_member(index_relid as i32, &run.root.rinfo(rinfo).right_relids)
        && !clauses::contain_volatile_functions(rightop)?
    {
        if index_coll_matches_expr_coll(idxcollation, op.inputcollid)
            && lsyscache::op_in_opfamily(op.opno, opfamily)?
        {
            return Ok(Some(IndexClause {
                rinfo: Some(rinfo),
                indexquals: {
                    let mut v = PgVec::new_in(run.mcx);
                    v.push(rinfo);
                    v
                },
                lossy: false,
                indexcol: indexcol as i16,
                indexcols: PgVec::new_in(run.mcx),
            }));
        }
        panic!("get_index_clause_from_support (indxpath.c): M2 support-function lane");
    }

    if right_matches
        && !relids_is_member(index_relid as i32, &run.root.rinfo(rinfo).left_relids)
        && !clauses::contain_volatile_functions(leftop)?
    {
        panic!("commute_restrictinfo (indxpath.c): const-op-indexkey; M2 commutation lane");
    }

    Ok(None)
}

// IndexCollMatchesExprColl (indxpath.c).
fn index_coll_matches_expr_coll(idxcollation: u32, exprcollation: u32) -> bool {
    idxcollation == 0 || idxcollation == exprcollation
}

// match_index_to_operand (indxpath.c), simple-column arm; expression columns
// panicked in get_relation_info.
pub fn match_index_to_operand(
    run: &PlannerRun<'_>,
    mut operand: Node<'_>,
    indexcol: usize,
    index: &IndexOptInfo<'_>,
) -> bool {
    while operand.node_tag() == NodeTag::T_RelabelType {
        operand = operand.as_relabel_type().unwrap().arg;
    }
    let index_relid = run.root.rel(index.rel.expect("index rel set")).relid;
    let indkey = index.indexkeys[indexcol];
    debug_assert!(indkey != 0, "expression index survived get_relation_info");
    if let Some(var) = operand.as_var() {
        if var.varno as u32 == index_relid
            && indkey == var.varattno as i32
            && var.varnullingrels.is_empty()
        {
            return true;
        }
    }
    false
}

// get_index_paths (indxpath.c). btree has amhasgettuple; the bitmap
// collection feeds create_index_paths' (deferred) bitmap arm.
fn get_index_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    index: &'mcx IndexOptInfo<'mcx>,
    clauses: &IndexClauseSet<'mcx>,
    bitindexpaths: &mut PgVec<'mcx, PathId>,
) -> PgResult<()> {
    let indexpaths = build_index_paths(run, rel, index, clauses)?;
    for &ipath in indexpaths.iter() {
        if index.amhasgettuple {
            add_path(run, rel, ipath);
        }
        if index.amhasgetbitmap {
            let (no_pathkeys, selec) = {
                let p = run.root.path(ipath);
                let sel = match p {
                    types_pathnodes::PathNode::IndexPath(ip) => ip.indexselectivity,
                    _ => 1.0,
                };
                (p.base().pathkeys.is_empty(), sel)
            };
            if no_pathkeys || selec < 1.0 {
                bitindexpaths.push(ipath);
            }
        }
    }
    Ok(())
}

// build_index_paths (indxpath.c), ScanTypeControl ST_ANYSCAN arm with no
// SAOP/pathkey/parallel legs live.
fn build_index_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    index: &'mcx IndexOptInfo<'mcx>,
    clauses: &IndexClauseSet<'mcx>,
) -> PgResult<PgVec<'mcx, PathId>> {
    let mcx = run.mcx;
    let mut result: PgVec<'mcx, PathId> = PgVec::new_in(mcx);

    let mut index_clauses: PgVec<'mcx, IndexClause<'mcx>> = PgVec::new_in(mcx);
    debug_assert!(run.root.rel(rel).lateral_relids.is_none());
    for indexcol in 0..index.nkeycolumns as usize {
        for ic in clauses.indexclauses[indexcol].iter() {
            let rid = ic.rinfo.expect("IndexClause rinfo");
            debug_assert!(relids_is_subset(
                &run.root.rinfo(rid).clause_relids,
                &run.root.rel(rel).relids
            ));
            index_clauses.push(ic.clone());
        }
        if index_clauses.is_empty() && !index.amoptionalkey {
            return Ok(result);
        }
    }

    let loop_count = 1.0;

    // has_useful_pathkeys (allpaths.c); amcanorderbyop is false for btree so
    // the match_pathkeys_to_index arm is dead.
    let pathkeys_possibly_useful = !run.root.rel(rel).joininfo.is_empty()
        || run.root.rel(rel).has_eclass_joins
        || !run.root.query_pathkeys.is_empty();
    let index_is_ordered = !index.sortopfamily.is_empty();
    let useful_pathkeys: PgVec<'mcx, types_pathnodes::PathKey> =
        if index_is_ordered && pathkeys_possibly_useful {
            let index_pathkeys = crate::pathkeys::build_index_pathkeys(
                run,
                index,
                types_pathnodes::ForwardScanDirection,
            )?;
            crate::pathkeys::truncate_useless_pathkeys(run, rel, &index_pathkeys)?
        } else {
            PgVec::new_in(mcx)
        };

    let index_only_scan = check_index_only(run, rel, index);

    if !index_clauses.is_empty() || !useful_pathkeys.is_empty() || index_only_scan {
        let forward_clauses = {
            let mut v: PgVec<'mcx, IndexClause<'mcx>> = PgVec::new_in(mcx);
            v.extend(index_clauses.iter().cloned());
            v
        };
        let ipath = crate::pathnode::create_index_path(
            run,
            index,
            forward_clauses,
            useful_pathkeys,
            types_pathnodes::ForwardScanDirection,
            index_only_scan,
            loop_count,
        )?;
        result.push(ipath);
        // Parallel index scan (partial paths): M3 lane.
        debug_assert!(run.root.rel(rel).partial_pathlist.is_empty());
    }

    if index_is_ordered && pathkeys_possibly_useful {
        let index_pathkeys = crate::pathkeys::build_index_pathkeys(
            run,
            index,
            types_pathnodes::BackwardScanDirection,
        )?;
        let useful_pathkeys =
            crate::pathkeys::truncate_useless_pathkeys(run, rel, &index_pathkeys)?;
        if !useful_pathkeys.is_empty() {
            let ipath = crate::pathnode::create_index_path(
                run,
                index,
                index_clauses,
                useful_pathkeys,
                types_pathnodes::BackwardScanDirection,
                index_only_scan,
                loop_count,
            )?;
            result.push(ipath);
        }
    }

    Ok(result)
}

// check_index_only (indxpath.c).
fn check_index_only(run: &PlannerRun<'_>, rel: RelId, index: &IndexOptInfo<'_>) -> bool {
    if !crate::gucs::enable_indexonlyscan() {
        return false;
    }
    // Attrs needed above the scan plus all baserestrictinfo Vars, each
    // checked against returnable index columns.
    let r = run.root.rel(rel);
    let mut needed: mcx::PgVec<'_, i16> = mcx::PgVec::new_in(run.mcx);
    for (i, an) in r.attr_needed.iter().enumerate() {
        if !crate::relnode::relids_is_empty(an) {
            needed.push(i as i16 + r.min_attr);
        }
    }
    for &rid in r.baserestrictinfo.iter() {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        collect_varattnos(run, clause, r.relid as i32, &mut needed);
    }
    needed.sort_unstable();
    needed.dedup();

    for attno in needed {
        if attno == 0 {
            return false;
        }
        let mut found = false;
        for c in 0..index.ncolumns as usize {
            if index.indexkeys[c] == attno as i32 && index.canreturn[c] {
                found = true;
                break;
            }
        }
        if !found {
            return false;
        }
    }
    true
}

fn collect_varattnos(run: &PlannerRun<'_>, node: Node<'_>, relid: i32, out: &mut mcx::PgVec<'_, i16>) {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().unwrap();
            if v.varno == relid && v.varlevelsup == 0 {
                out.push(v.varattno);
            }
        }
        NodeTag::T_Const | NodeTag::T_Param => {}
        NodeTag::T_OpExpr => {
            for a in &node.as_op_expr().unwrap().args {
                collect_varattnos(run, a, relid, out);
            }
        }
        NodeTag::T_RelabelType => {
            collect_varattnos(run, node.as_relabel_type().unwrap().arg, relid, out)
        }
        other => panic!("pull_varattnos (var.c) via check_index_only: {other:?}; M2 lane"),
    }
}
