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

    // C calls generate_bitmap_or_paths unconditionally; the OR pre-scan skips
    // its two list copies on the OR-free common path (strictly less work).
    let has_or = (0..run.root.rel(rel).baserestrictinfo.len()).any(|i| {
        let rid = run.root.rel(rel).baserestrictinfo[i];
        clauses::is_orclause(*run.root.expr_node(run.root.rinfo(rid).clause))
    });
    if has_or {
        let mut baserestrict: PgVec<'mcx, RinfoId> = PgVec::new_in(run.mcx);
        baserestrict.extend(run.root.rel(rel).baserestrictinfo.iter().copied());
        let orpaths = generate_bitmap_or_paths(run, rel, &baserestrict, &[])?;
        bitindexpaths.extend(orpaths.iter().copied());
    }

    if !bitindexpaths.is_empty() {
        let bitmapqual = choose_bitmap_and(run, rel, &bitindexpaths)?;
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
        // match_funcclause_to_indexcol (indxpath.c).
        NodeTag::T_FuncExpr => {
            let f = clause.as_func_expr().unwrap();
            let funcid = f.funcid;
            for (indexarg, op) in f.args.iter().enumerate() {
                if match_index_to_operand(run, op, indexcol, index) {
                    return get_index_clause_from_support(
                        run,
                        rinfo,
                        funcid,
                        indexarg as i32,
                        indexcol,
                        index,
                    );
                }
            }
            Ok(None)
        }
        NodeTag::T_RelabelType => panic!(
            "match_clause_to_indexcol (indxpath.c): RelabelType clause; M2 lane"
        ),
        NodeTag::T_NullTest if index.amsearchnulls => {
            let nt = clause.as_null_test().unwrap();
            if !nt.argisrow
                && match_index_to_operand(run, nt.arg.expect("NullTest.arg"), indexcol, index)
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
            Ok(None)
        }
        NodeTag::T_ScalarArrayOpExpr => {
            // match_saopclause_to_indexcol (indxpath.c): loud where C would
            // build an amsearcharray index clause, None where C also fails.
            let sa = clause.as_scalar_array_op_expr().unwrap();
            if sa.useOr
                && index.amsearcharray
                && match_index_to_operand(run, sa.args.nth(0), indexcol, index)
                && index_coll_matches_expr_coll(
                    index.indexcollations[indexcol],
                    sa.inputcollid,
                )
                && lsyscache::op_in_opfamily(sa.opno, index.opfamily[indexcol])?
            {
                panic!("match_saopclause_to_indexcol (indxpath.c): M2 SAOP-indexqual lane");
            }
            Ok(None)
        }
        // RowCompare/NullTest/OR can't be built by the live qual lane.
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
        let opfuncid = lsyscache::get_opcode(op.opno)?;
        if let Some(ic) =
            get_index_clause_from_support(run, rinfo, opfuncid, 0, indexcol, index)?
        {
            return Ok(Some(ic));
        }
    }

    if right_matches
        && !relids_is_member(index_relid as i32, &run.root.rinfo(rinfo).left_relids)
        && !clauses::contain_volatile_functions(leftop)?
    {
        panic!("commute_restrictinfo (indxpath.c): const-op-indexkey; M2 commutation lane");
    }

    Ok(None)
}

// get_index_clause_from_support (indxpath.c): closed-set dispatch on the
// prosupport oid instead of C's fmgr detour (rule 4); like_regex_support
// (like_support.c) is the only in-core SupportRequestIndexCondition provider
// besides tsmatchsel's, which stays loud.
fn get_index_clause_from_support<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    funcid: u32,
    indexarg: i32,
    indexcol: usize,
    index: &IndexOptInfo<'mcx>,
) -> PgResult<Option<IndexClause<'mcx>>> {
    use crate::like_support::PatternType;
    let shape = syscache_seams::pg_proc_cost_shape::call(funcid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcid}"));
    if shape.prosupport == 0 {
        return Ok(None);
    }
    let ptype = match shape.prosupport {
        1023 => PatternType::Like,
        1025 => PatternType::LikeIc,
        1364 => PatternType::Regex,
        1024 => PatternType::RegexIc,
        6242 => PatternType::Prefix,
        other => panic!(
            "get_index_clause_from_support (indxpath.c): prosupport {other}; M2 lane"
        ),
    };
    // like_regex_support: no reverse-match operators, indexkey-on-left only.
    if indexarg != 0 {
        return Ok(None);
    }
    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let op = clause.as_op_expr().expect("support request over an OpExpr");
    let Some(exprs) = crate::like_support::match_pattern_prefix(
        run,
        op.args.nth(0),
        op.args.nth(1),
        ptype,
        op.inputcollid,
        index.opfamily[indexcol],
        index.indexcollations[indexcol],
    )?
    else {
        return Ok(None);
    };
    let mut indexquals = PgVec::new_in(run.mcx);
    for expr in exprs.iter() {
        // make_simple_restrictinfo (restrictinfo.h).
        indexquals.push(crate::initsplan::make_restrictinfo(
            run, *expr, true, false, false, false, 0, None, None, None,
        )?);
    }
    Ok(Some(IndexClause {
        rinfo: Some(rinfo),
        indexquals,
        lossy: true,
        indexcol: indexcol as i16,
        indexcols: PgVec::new_in(run.mcx),
    }))
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
    let indexpaths = build_index_paths(run, rel, index, clauses, false)?;
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

// build_index_paths (indxpath.c), ST_ANYSCAN (bitmap=false) and ST_BITMAPSCAN
// (bitmap=true) arms; no SAOP/parallel legs live.
fn build_index_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    index: &'mcx IndexOptInfo<'mcx>,
    clauses: &IndexClauseSet<'mcx>,
    bitmap: bool,
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
    // the match_pathkeys_to_index arm is dead. Bitmap scans never provide
    // ordering (C ST_BITMAPSCAN: useful_pathkeys = NIL).
    let pathkeys_possibly_useful = !bitmap
        && (!run.root.rel(rel).joininfo.is_empty()
            || run.root.rel(rel).has_eclass_joins
            || !run.root.query_pathkeys.is_empty());
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

    let index_only_scan = !bitmap && check_index_only(run, rel, index);

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
        NodeTag::T_ScalarArrayOpExpr => {
            for a in &node.as_scalar_array_op_expr().unwrap().args {
                collect_varattnos(run, a, relid, out);
            }
        }
        NodeTag::T_ArrayExpr => {
            for e in &node.as_array_expr().unwrap().elements {
                collect_varattnos(run, e, relid, out);
            }
        }
        NodeTag::T_NullTest => {
            collect_varattnos(run, node.as_null_test().unwrap().arg.expect("NullTest.arg"), relid, out)
        }
        NodeTag::T_BooleanTest => collect_varattnos(
            run,
            node.as_boolean_test().unwrap().arg.expect("BooleanTest.arg"),
            relid,
            out,
        ),
        NodeTag::T_DistinctExpr => {
            for a in &node.as_distinct_expr().unwrap().args {
                collect_varattnos(run, a, relid, out);
            }
        }
        NodeTag::T_BoolExpr => {
            for a in &node.as_bool_expr().unwrap().args {
                collect_varattnos(run, a, relid, out);
            }
        }
        NodeTag::T_FuncExpr => {
            for a in &node.as_func_expr().unwrap().args {
                collect_varattnos(run, a, relid, out);
            }
        }
        other => panic!("pull_varattnos (var.c) via check_index_only: {other:?}; M2 lane"),
    }
}

// Sub-RestrictInfo for one OR arm. C divergence: make_restrictinfo here never
// runs make_sub_restrictinfos (orclause stays None), so the arm rinfos are
// built on first use; the per-arm selectivity memo is scoped to this planning
// pass, the numerics are C's.
fn or_arm_rinfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    parent: RinfoId,
    arm: Node<'mcx>,
) -> PgResult<RinfoId> {
    let mcx = run.mcx;
    let (is_pushed_down, has_clone, is_clone, pseudoconstant, security_level, req, incompat, outer) = {
        let p = run.root.rinfo(parent);
        (
            p.is_pushed_down,
            p.has_clone,
            p.is_clone,
            p.pseudoconstant,
            p.security_level,
            crate::relnode::relids_copy(mcx, &p.required_relids),
            crate::relnode::relids_copy(mcx, &p.incompatible_relids),
            crate::relnode::relids_copy(mcx, &p.outer_relids),
        )
    };
    crate::initsplan::make_restrictinfo(
        run,
        arm,
        is_pushed_down,
        has_clone,
        is_clone,
        pseudoconstant,
        security_level,
        req,
        incompat,
        outer,
    )
}

// group_similar_or_args (indxpath.c): only the ungrouped outcome is live —
// two similar arms (same indexable column/operator/collation) would be fused
// into an SAOP-matchable sub-rinfo, which is the OR-to-SAOP lane.
fn assert_no_similar_or_groups<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    arm_rinfos: &[RinfoId],
) -> PgResult<()> {
    #[derive(Clone, Copy, PartialEq)]
    struct Key {
        indexnum: usize,
        colnum: usize,
        opno: u32,
        inputcollid: u32,
    }
    let relid = run.root.rel(rel).relid as i32;
    let mut keys: PgVec<'mcx, Option<Key>> = PgVec::new_in(run.mcx);
    for &rid in arm_rinfos {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        let Some(op) = clause.as_op_expr() else {
            keys.push(None);
            continue;
        };
        if op.args.len() != 2 {
            keys.push(None);
            continue;
        }
        let strip = |mut n: Node<'mcx>| {
            while let Some(r) = n.as_relabel_type() {
                n = r.arg;
            }
            n
        };
        let leftop = strip(op.args.nth(0));
        let rightop = strip(op.args.nth(1));
        let (in_left, in_right) = {
            let r = run.root.rinfo(rid);
            (
                relids_is_member(relid, &r.left_relids),
                relids_is_member(relid, &r.right_relids),
            )
        };
        let (opno, nonconst) = if in_right && !in_left && !clauses::contain_volatile_functions(leftop)? {
            let comm = lsyscache::get_commutator(op.opno)?;
            if comm == 0 {
                keys.push(None);
                continue;
            }
            (comm, rightop)
        } else if in_left && !in_right && !clauses::contain_volatile_functions(rightop)? {
            (op.opno, leftop)
        } else {
            keys.push(None);
            continue;
        };
        let mut key = None;
        let nindexes = run.root.rel(rel).indexlist.len();
        'indexes: for indexnum in 0..nindexes {
            let index = run.root.rel(rel).indexlist[indexnum];
            if !index.amhasgetbitmap || !index.amsearcharray {
                continue;
            }
            for colnum in 0..index.nkeycolumns as usize {
                if match_index_to_operand(run, nonconst, colnum, index) {
                    key = Some(Key { indexnum, colnum, opno, inputcollid: op.inputcollid });
                    break 'indexes;
                }
            }
        }
        keys.push(key);
    }
    for i in 0..keys.len() {
        let Some(k) = keys[i] else { continue };
        for j in i + 1..keys.len() {
            if keys[j] == Some(k) {
                panic!(
                    "group_similar_or_args (indxpath.c): similar OR arms; \
                     M2 OR-to-SAOP lane (match_orclause_to_indexcol)"
                );
            }
        }
    }
    Ok(())
}

// build_paths_for_OR (indxpath.c); partial indexes loud upstream, so the
// useful_predicate leg is dead.
fn build_paths_for_or<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    clauses: &[RinfoId],
    other_clauses: &[RinfoId],
) -> PgResult<PgVec<'mcx, PathId>> {
    let mut result: PgVec<'mcx, PathId> = PgVec::new_in(run.mcx);
    let nindexes = run.root.rel(rel).indexlist.len();
    for i in 0..nindexes {
        let index = run.root.rel(rel).indexlist[i];
        if !index.amhasgetbitmap {
            continue;
        }
        debug_assert!(index.indpred.is_empty());
        let mut clauseset = IndexClauseSet::new(run.mcx, index.nkeycolumns as usize);
        for &r in clauses {
            match_clause_to_index(run, r, index, &mut clauseset)?;
        }
        if !clauseset.nonempty {
            continue;
        }
        for &r in other_clauses {
            match_clause_to_index(run, r, index, &mut clauseset)?;
        }
        let paths = build_index_paths(run, rel, index, &clauseset, true)?;
        result.extend(paths.iter().copied());
    }
    Ok(result)
}

// generate_bitmap_or_paths (indxpath.c).
pub fn generate_bitmap_or_paths<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    clauses: &[RinfoId],
    other_clauses: &[RinfoId],
) -> PgResult<PgVec<'mcx, PathId>> {
    let mcx = run.mcx;
    let mut result: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
    let mut all_clauses: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    all_clauses.extend(clauses.iter().copied());
    all_clauses.extend(other_clauses.iter().copied());

    for &rid in clauses {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        if !clauses::is_orclause(clause) {
            continue;
        }

        enum Arm<'mcx> {
            Simple(RinfoId),
            And(PgVec<'mcx, RinfoId>),
        }
        let mut arms: PgVec<'mcx, Arm<'mcx>> = PgVec::new_in(mcx);
        let mut simple_rids: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
        for arg in &clause.as_bool_expr().expect("OR clause").args {
            if clauses::is_andclause(arg) {
                let mut andargs: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
                for a in &arg.as_bool_expr().expect("AND clause").args {
                    debug_assert!(!clauses::is_andclause(a), "unflattened AND");
                    andargs.push(or_arm_rinfo(run, rid, a)?);
                }
                arms.push(Arm::And(andargs));
            } else {
                let arid = or_arm_rinfo(run, rid, arg)?;
                simple_rids.push(arid);
                arms.push(Arm::Simple(arid));
            }
        }
        assert_no_similar_or_groups(run, rel, &simple_rids)?;

        let mut pathlist: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
        let mut matched_all = true;
        for arm in arms.iter() {
            let indlist = match arm {
                Arm::And(andargs) => {
                    let mut il = build_paths_for_or(run, rel, andargs, &all_clauses)?;
                    let sub = generate_bitmap_or_paths(run, rel, andargs, &all_clauses)?;
                    il.extend(sub.iter().copied());
                    il
                }
                Arm::Simple(arid) => build_paths_for_or(
                    run,
                    rel,
                    core::slice::from_ref(arid),
                    &all_clauses,
                )?,
            };
            if indlist.is_empty() {
                matched_all = false;
                break;
            }
            pathlist.push(choose_bitmap_and(run, rel, &indlist)?);
        }
        if matched_all && !pathlist.is_empty() {
            result.push(crate::pathnode::create_bitmap_or_path(run, rel, pathlist)?);
        }
    }
    Ok(result)
}

struct PathClauseUsage<'mcx> {
    path: PathId,
    quals: PgVec<'mcx, Node<'mcx>>,
    preds: PgVec<'mcx, Node<'mcx>>,
    clauseids: types_nodes::bitmapset::Bitmapset<'mcx>,
    unclassifiable: bool,
}

// choose_bitmap_and (indxpath.c): O(N^2) AND-group search over the
// clause-usage-deduplicated candidates.
pub fn choose_bitmap_and<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    paths: &[PathId],
) -> PgResult<PathId> {
    let mcx = run.mcx;
    debug_assert!(!paths.is_empty());
    if paths.len() == 1 {
        return Ok(paths[0]);
    }

    let mut clauselist: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    let mut infos: PgVec<'mcx, PathClauseUsage<'mcx>> = PgVec::new_in(mcx);
    for &p in paths {
        let info = classify_index_clause_usage(run, p, &mut clauselist)?;
        if info.unclassifiable {
            infos.push(info);
            continue;
        }
        let dup = infos.iter().position(|e| {
            !e.unclassifiable && info.clauseids.equal(&e.clauseids)
        });
        match dup {
            Some(i) => {
                let (ncost, _) = crate::costsize::cost_bitmap_tree_node(run, info.path);
                let (ocost, _) = crate::costsize::cost_bitmap_tree_node(run, infos[i].path);
                if ncost < ocost {
                    infos[i] = info;
                }
            }
            None => infos.push(info),
        }
    }
    if infos.len() == 1 {
        return Ok(infos[0].path);
    }

    // path_usage_comparator; sort_by is stable where C's qsort is not — a
    // difference only on exact (cost, selectivity) ties.
    infos.sort_by(|a, b| {
        let (ac, asel) = crate::costsize::cost_bitmap_tree_node(run, a.path);
        let (bc, bsel) = crate::costsize::cost_bitmap_tree_node(run, b.path);
        ac.partial_cmp(&bc)
            .expect("bitmap path cost is not NaN")
            .then(asel.partial_cmp(&bsel).expect("bitmap selectivity is not NaN"))
    });

    let mut bestpaths: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
    let mut bestcost = 0.0;
    for i in 0..infos.len() {
        let mut curpaths: PgVec<'mcx, PathId> = PgVec::new_in(mcx);
        curpaths.push(infos[i].path);
        let mut costsofar = bitmap_scan_cost_est(run, rel, infos[i].path)?;
        let mut clauseidsofar = types_nodes::bitmapset::Bitmapset::empty();
        clauseidsofar.add_members(mcx, &infos[i].clauseids)?;
        for j in i + 1..infos.len() {
            if infos[j].clauseids.overlap(&clauseidsofar) {
                continue;
            }
            // The preds redundancy check (predicate_implied_by) is dead:
            // partial indexes are loud upstream.
            debug_assert!(infos[j].preds.is_empty());
            curpaths.push(infos[j].path);
            let newcost = bitmap_and_cost_est(run, rel, &curpaths)?;
            if newcost < costsofar {
                costsofar = newcost;
                clauseidsofar.add_members(mcx, &infos[j].clauseids)?;
            } else {
                curpaths.pop();
            }
        }
        if i == 0 || costsofar < bestcost {
            bestpaths = curpaths;
            bestcost = costsofar;
        }
    }
    if bestpaths.len() == 1 {
        return Ok(bestpaths[0]);
    }
    crate::pathnode::create_bitmap_and_path(run, rel, bestpaths)
}

fn classify_index_clause_usage<'mcx>(
    run: &PlannerRun<'mcx>,
    path: PathId,
    clauselist: &mut PgVec<'mcx, Node<'mcx>>,
) -> PgResult<PathClauseUsage<'mcx>> {
    let mcx = run.mcx;
    let mut quals: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    let mut preds: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    find_indexpath_quals(run, path, &mut quals, &mut preds);
    if quals.len() + preds.len() > 100 {
        return Ok(PathClauseUsage {
            path,
            quals,
            preds,
            clauseids: types_nodes::bitmapset::Bitmapset::empty(),
            unclassifiable: true,
        });
    }
    let mut clauseids = types_nodes::bitmapset::Bitmapset::empty();
    for i in 0..quals.len() {
        let pos = find_list_position(quals[i], clauselist);
        clauseids.add_member(mcx, pos as i32)?;
    }
    for i in 0..preds.len() {
        let pos = find_list_position(preds[i], clauselist);
        clauseids.add_member(mcx, pos as i32)?;
    }
    Ok(PathClauseUsage { path, quals, preds, clauseids, unclassifiable: false })
}

fn find_indexpath_quals<'mcx>(
    run: &PlannerRun<'mcx>,
    path: PathId,
    quals: &mut PgVec<'mcx, Node<'mcx>>,
    preds: &mut PgVec<'mcx, Node<'mcx>>,
) {
    match run.root.path(path) {
        types_pathnodes::PathNode::BitmapAndPath(p) => {
            for i in 0..p.bitmapquals.len() {
                find_indexpath_quals(run, p.bitmapquals[i], quals, preds);
            }
        }
        types_pathnodes::PathNode::BitmapOrPath(p) => {
            for i in 0..p.bitmapquals.len() {
                find_indexpath_quals(run, p.bitmapquals[i], quals, preds);
            }
        }
        types_pathnodes::PathNode::IndexPath(ip) => {
            for ic in ip.indexclauses.iter() {
                let rid = ic.rinfo.expect("IndexClause rinfo");
                quals.push(*run.root.expr_node(run.root.rinfo(rid).clause));
            }
            debug_assert!(ip.indexinfo.expect("indexinfo set").indpred.is_empty());
        }
        other => panic!(
            "find_indexpath_quals (indxpath.c): pathtype {}",
            other.base().pathtype
        ),
    }
}

fn find_list_position<'mcx>(node: Node<'mcx>, list: &mut PgVec<'mcx, Node<'mcx>>) -> usize {
    for (i, old) in list.iter().enumerate() {
        if types_nodes::equal(node, *old) {
            return i;
        }
    }
    list.push(node);
    list.len() - 1
}

// bitmap_scan_cost_est (indxpath.c). C costs a throwaway stack BitmapHeapPath;
// the arena copy here is same-lifetime garbage.
fn bitmap_scan_cost_est<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    ipath: PathId,
) -> PgResult<f64> {
    let bpath = crate::pathnode::create_bitmap_heap_path(run, rel, ipath, 1.0)?;
    Ok(run.root.path(bpath).base().total_cost)
}

// bitmap_and_cost_est (indxpath.c).
fn bitmap_and_cost_est<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    paths: &[PathId],
) -> PgResult<f64> {
    let mut quals: PgVec<'mcx, PathId> = PgVec::new_in(run.mcx);
    quals.extend(paths.iter().copied());
    let apath = crate::pathnode::create_bitmap_and_path(run, rel, quals)?;
    bitmap_scan_cost_est(run, rel, apath)
}
