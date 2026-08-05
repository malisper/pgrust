use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
use types_nodes::{Node, NodeTag};

use crate::run::PlannerRun;

const OIDOID: u32 = 26;
const REGCLASSOID: u32 = 2205;
const FIRST_UNPINNED_OBJECT_ID: u32 = 12000;
// syscache.h: PROCOID. record_plan_function_dependency keys PlanInvalItems on
// it because plancache.c registers PlanCacheObjectCallback for PROCOID.
const PROCOID: i32 = 47;
// cache_syscache TYPEOID: record_plan_type_dependency's cache (setrefs.c:3608).
const TYPEOID: i32 = cache_syscache::cacheinfo::TYPEOID;

// No AlternativeSubPlans.
pub fn set_plan_references<'mcx>(run: &mut PlannerRun<'mcx>, plan: Node<'mcx>) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    let rtoffset = run.glob.finalrtable.len() as i32;
    add_rtes_to_flat_rtable(run)?;
    // Flat rtoffset-adjusted AppendRelInfo copies for EXPLAIN's appendrel
    // deparse (child Vars print as the parent's column above Append nodes).
    for appinfo in run.root.append_rel_list.iter() {
        let node = Node::mk(
            mcx,
            types_nodes::plannodes::AppendRelInfo {
                parent_relid: appinfo.parent_relid + rtoffset as u32,
                child_relid: appinfo.child_relid + rtoffset as u32,
                parent_reltype: appinfo.parent_reltype,
                child_reltype: appinfo.child_reltype,
                num_child_cols: appinfo.num_child_cols,
                parent_colnos: mcx::slice_borrow_in(mcx, &appinfo.parent_colnos)?,
                parent_reloid: appinfo.parent_reloid,
            },
        )?;
        run.glob.append_relations.lappend(mcx, node)?;
    }
    // Flat PlanRowMark copies, RT indexes adjusted, rowmarkId untouched.
    for i in 0..run.root.rowMarks.len() {
        let mut rc = *run.rowmark(run.root.rowMarks[i]);
        rc.rti += rtoffset as u32;
        rc.prti += rtoffset as u32;
        run.glob.finalrowmarks.lappend(mcx, Node::mk(mcx, rc)?)?;
    }
    set_plan_refs(run, plan, rtoffset)
}

// Top-level flat copy with sub-structure zapped; alias/eref stay by ref.
fn add_rtes_to_flat_rtable(run: &mut PlannerRun<'_>) -> PgResult<()> {
    let parse = run.parse();
    let mut saw_subquery = false;
    for rte_node in &parse.rtable {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        saw_subquery |= rte.rtekind == RTEKind::RTE_SUBQUERY;
        add_rte_to_flat_rtable(run, &parse.rteperminfos, rte)?;
    }
    // Subquery-free queries never reach the dead-subquery pass (outlined off
    // the set_plan_references hot body — select1 instruction bracket).
    if saw_subquery {
        dead_subquery_pass(run, false)?;
    }
    Ok(())
}

// C's `recursing` mode keeps only relation RTEs (and former relations);
// live subqueries flatten through the SubqueryScan plan walk instead.
#[cold]
#[inline(never)]
fn add_rtes_to_flat_rtable_recursing(run: &mut PlannerRun<'_>, recursing: bool) -> PgResult<()> {
    let parse = run.parse();
    for rte_node in &parse.rtable {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        if !recursing
            || rte.rtekind == RTEKind::RTE_RELATION
            || (rte.rtekind == RTEKind::RTE_SUBQUERY && rte.relid != 0)
        {
            add_rte_to_flat_rtable(run, &parse.rteperminfos, rte)?;
        }
    }
    dead_subquery_pass(run, recursing)
}

// C's dead-subquery pass: subqueries not referenced by the plan tree must
// contribute their RTEs anyway so the executor still checks permissions.
// No subroot = never planned (self-contradictory constraints); a subroot
// whose FINAL rel is dummy planned to nothing and never gets a
// SubqueryScan walk.
#[cold]
#[inline(never)]
fn dead_subquery_pass(run: &mut PlannerRun<'_>, recursing: bool) -> PgResult<()> {
    let parse = run.parse();
    for (i, rte_node) in parse.rtable.iter().enumerate() {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        let rti = (i + 1) as i32;
        if rte.rtekind == RTEKind::RTE_SUBQUERY
            && !rte.inh
            && rti < run.root.simple_rel_array_size
        {
            if let Some(rel) = run.root.simple_rel_array[rti as usize] {
                match run.root.rel(rel).subroot_idx {
                    None => {
                        let subquery =
                            rte.subquery.expect("unplanned subquery RTE keeps its Query");
                        flatten_unplanned_rtes(run, subquery)?;
                    }
                    Some(idx) => {
                        run.swap_with_rel_subroot(idx);
                        let final_rel = crate::relnode::fetch_upper_rel(
                            &mut run.root,
                            types_pathnodes::UPPERREL_FINAL,
                        );
                        let r = if recursing
                            || crate::joinrels::is_dummy_rel(&run.root, final_rel)
                        {
                            add_rtes_to_flat_rtable_recursing(run, true)
                        } else {
                            Ok(())
                        };
                        run.swap_with_rel_subroot(idx);
                        r?;
                    }
                }
            }
        }
    }
    Ok(())
}

// flatten_unplanned_rtes/flatten_rtes_walker (setrefs.c).
fn flatten_unplanned_rtes<'mcx>(
    run: &mut PlannerRun<'mcx>,
    subquery: &'mcx types_nodes::parsenodes::Query<'mcx>,
) -> PgResult<()> {
    struct W<'a, 'b, 'mcx> {
        run: &'a mut PlannerRun<'mcx>,
        query: &'b types_nodes::parsenodes::Query<'mcx>,
    }
    impl<'a, 'b, 'mcx> nodes_core::NodeWalker<'mcx> for W<'a, 'b, 'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(rte) = node.as_range_tbl_entry() {
                if rte.rtekind == RTEKind::RTE_RELATION
                    || (rte.rtekind == RTEKind::RTE_SUBQUERY && rte.relid != 0)
                {
                    add_rte_to_flat_rtable(self.run, &self.query.rteperminfos, rte)?;
                }
                return Ok(false);
            }
            nodes_core::expression_tree_walker(node, self)
        }
        fn visit_query_ref(
            &mut self,
            q: &'mcx types_nodes::parsenodes::Query<'mcx>,
        ) -> PgResult<bool> {
            let save = self.query;
            self.query = q;
            let r = nodes_core::query_tree_walker(q, self, nodes_core::QTW_EXAMINE_RTES_BEFORE)?;
            self.query = save;
            Ok(r)
        }
    }
    let mut w = W { run, query: subquery };
    nodes_core::query_tree_walker(subquery, &mut w, nodes_core::QTW_EXAMINE_RTES_BEFORE)?;
    Ok(())
}

fn add_rte_to_flat_rtable<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rteperminfos: &NodeList<'mcx>,
    rte: &RangeTblEntry<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    {
        // C shares the RTEPermissionInfo node into glob->finalrteperminfos and
        // renumbers the copied RTE's index.
        let mut new_perminfoindex = 0;
        if rte.perminfoindex > 0 {
            let perminfo = parse_relation::getRTEPermissionInfo(rteperminfos, rte)?;
            run.glob.finalrteperminfos.lappend(mcx, perminfo)?;
            new_perminfoindex = run.glob.finalrteperminfos.len() as types_core::Index;
        }
        let newrte = Node::mk(
            mcx,
            RangeTblEntry {
                alias: rte.alias,
                eref: rte.eref,
                rtekind: rte.rtekind,
                relid: rte.relid,
                inh: rte.inh,
                relkind: rte.relkind,
                rellockmode: rte.rellockmode,
                perminfoindex: new_perminfoindex,
                tablesample: None,
                subquery: None,
                security_barrier: rte.security_barrier,
                jointype: rte.jointype,
                joinmergedcols: rte.joinmergedcols,
                joinaliasvars: NodeList::nil(),
                joinleftcols: types_nodes::list::IntList::nil(),
                joinrightcols: types_nodes::list::IntList::nil(),
                join_using_alias: None,
                functions: NodeList::nil(),
                funcordinality: rte.funcordinality,
                tablefunc: None,
                values_lists: NodeList::nil(),
                ctename: rte.ctename,
                ctelevelsup: rte.ctelevelsup,
                self_reference: rte.self_reference,
                coltypes: types_nodes::list::OidList::nil(),
                coltypmods: types_nodes::list::IntList::nil(),
                colcollations: types_nodes::list::OidList::nil(),
                enrname: rte.enrname,
                enrtuples: rte.enrtuples,
                groupexprs: NodeList::nil(),
                lateral: rte.lateral,
                inFromCl: rte.inFromCl,
                securityQuals: NodeList::nil(),
            },
        )?;
        run.glob.finalrtable.lappend(mcx, newrte)?;
        if rte.rtekind == RTEKind::RTE_RELATION
            || (rte.rtekind == RTEKind::RTE_SUBQUERY && rte.relid != 0)
        {
            run.glob.relation_oids.lappend(mcx, rte.relid)?;
            let rti = run.glob.finalrtable.len() as i32;
            run.glob.all_relids.add_member(mcx, rti)?;
        }
    }
    Ok(())
}

fn set_plan_refs<'mcx>(run: &mut PlannerRun<'mcx>, plan: Node<'mcx>, rtoffset: i32) -> PgResult<Node<'mcx>> {
    let plan_node_id = run.glob.last_plan_node_id;
    run.glob.last_plan_node_id += 1;
    // SAFETY: the plan tree was just built by createplan and is exclusively
    // ours until returned (C mutates it in place the same way).
    unsafe { plan.with_plan_mut(|p| p.plan_node_id = plan_node_id) }.expect("plan node");

    match plan.node_tag() {
        NodeTag::T_Result => {
            let r = plan.as_result().unwrap();
            if r.plan.lefttree.is_some() {
                set_upper_references(run, plan, rtoffset)?;
            } else {
                debug_assert!(r.plan.qual.is_nil());
                // A childless Result can carry unresolved ROWID_VAR Vars (a
                // target relation emptied by constraint exclusion); replace
                // them with typed NULLs as C does before fix_scan_list.
                for tle_node in &r.plan.targetlist {
                    let tle = tle_node.as_target_entry().expect("tlist cell");
                    let is_rowid = tle
                        .expr
                        .as_var()
                        .is_some_and(|v| v.varno == types_nodes::primnodes::ROWID_VAR);
                    if is_rowid {
                        let v = tle.expr.as_var().unwrap();
                        let nc = crate::inherit::make_null_const(
                            run.mcx,
                            v.vartype,
                            v.vartypmod,
                            v.varcollid,
                        )?;
                        // SAFETY: exclusive plan-tree ownership (prologue note).
                        unsafe {
                            tle_node.with_mut::<types_nodes::TargetEntry, _>(|t| t.expr = nc)
                        }
                        .expect("TargetEntry");
                    }
                }
                if let Some(tl) = fix_scan_list(run, &r.plan.targetlist, rtoffset, r.plan.plan_rows)? {
                    // SAFETY: exclusive plan-tree ownership (prologue note).
                    unsafe { plan.with_plan_mut(|p| p.targetlist = tl) }.expect("plan node");
                }
            }
            if let Some(rcq) = plan.as_result().unwrap().resconstantqual {
                let list = rcq.as_list().expect("resconstantqual is a List");
                if let Some(fixed) = fix_scan_list(run, list, rtoffset, 1.0)? {
                    let fixed = Node::mk_list(run.mcx, fixed)?;
                    // SAFETY: exclusive plan-tree ownership (prologue note).
                    unsafe {
                        plan.with_mut::<types_nodes::plannodes::Result, _>(|r| {
                            r.resconstantqual = Some(fixed)
                        })
                    }
                    .expect("Result node");
                }
            }
        }
        NodeTag::T_SeqScan => {
            let s = plan.as_seq_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            if rtoffset != 0 || tl.is_some() || qual.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::SeqScan, _>(|s| {
                        if let Some(tl) = tl {
                            s.scan.plan.targetlist = tl;
                        }
                        if let Some(q) = qual {
                            s.scan.plan.qual = q;
                        }
                        s.scan.scanrelid += rtoffset as u32;
                    })
                }
                .expect("SeqScan node");
            }
        }
        NodeTag::T_ForeignScan => {
            let s = plan.as_foreign_scan().unwrap();
            if !s.fdw_scan_tlist.is_nil() || s.scan.scanrelid == 0 {
                panic!(
                    "set_foreignscan_references (setrefs.c): fdw_scan_tlist / \
                     scanrelid==0 (foreign join/upper) arm unported"
                );
            }
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            let fe = fix_scan_list(run, &s.fdw_exprs, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            let frq =
                fix_scan_list(run, &s.fdw_recheck_quals, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            if rtoffset != 0 || tl.is_some() || qual.is_some() || fe.is_some() || frq.is_some() {
                let (fs_relids, fs_base_relids) = if rtoffset != 0 {
                    let shift = |old: &types_nodes::bitmapset::Bitmapset<'mcx>| {
                        let mut shifted = types_nodes::bitmapset::Bitmapset::empty();
                        let mut m = old.next_member(-1);
                        while m >= 0 {
                            shifted.add_member(run.mcx, m + rtoffset)?;
                            m = old.next_member(m);
                        }
                        Ok::<_, types_error::PgError>(shifted)
                    };
                    (Some(shift(&s.fs_relids)?), Some(shift(&s.fs_base_relids)?))
                } else {
                    (None, None)
                };
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::ForeignScan, _>(|p| {
                        if let Some(v) = tl {
                            p.scan.plan.targetlist = v;
                        }
                        if let Some(v) = qual {
                            p.scan.plan.qual = v;
                        }
                        if let Some(v) = fe {
                            p.fdw_exprs = v;
                        }
                        if let Some(v) = frq {
                            p.fdw_recheck_quals = v;
                        }
                        if let Some(v) = fs_relids {
                            p.fs_relids = v;
                        }
                        if let Some(v) = fs_base_relids {
                            p.fs_base_relids = v;
                        }
                        p.scan.scanrelid += rtoffset as u32;
                        if p.resultRelation > 0 {
                            p.resultRelation += rtoffset as u32;
                        }
                    })
                }
                .expect("ForeignScan node");
            }
        }
        NodeTag::T_SampleScan => {
            let s = plan.as_sample_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            let tsc = s
                .tablesample
                .expect("SampleScan has a tablesample clause")
                .as_table_sample_clause()
                .expect("tablesample is a TableSampleClause");
            let take_mutator = rtoffset != 0
                || run.glob.has_alternative_subplans
                || !run.root.minmax_aggs.is_empty();
            let args = fix_scan_list(run, &tsc.args, rtoffset, 1.0)?;
            let repeatable = match tsc.repeatable {
                Some(r) if take_mutator => Some(fix_scan_expr_mutator(run, r, rtoffset, 1.0)?),
                Some(r) => {
                    fix_scan_expr_walker(run, r)?;
                    None
                }
                None => None,
            };
            let new_tsc = if args.is_some() || repeatable.is_some() {
                let t = types_nodes::TableSampleClause {
                    tsmhandler: tsc.tsmhandler,
                    args: match args {
                        Some(a) => a,
                        None => tsc.args.clone_in(run.mcx)?,
                    },
                    repeatable: repeatable.or(tsc.repeatable),
                };
                Some(Node::mk(run.mcx, t)?)
            } else {
                None
            };
            if rtoffset != 0 || tl.is_some() || qual.is_some() || new_tsc.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::SampleScan, _>(|p| {
                        if let Some(v) = tl {
                            p.scan.plan.targetlist = v;
                        }
                        if let Some(v) = qual {
                            p.scan.plan.qual = v;
                        }
                        if let Some(t) = new_tsc {
                            p.tablesample = Some(t);
                        }
                        p.scan.scanrelid += rtoffset as u32;
                    })
                }
                .expect("SampleScan node");
            }
        }
        NodeTag::T_TidScan => {
            let s = plan.as_tid_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            let tq = fix_scan_list(run, &s.tidquals, rtoffset, 1.0)?;
            if rtoffset != 0 || tl.is_some() || qual.is_some() || tq.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::TidScan, _>(|p| {
                        if let Some(v) = tl {
                            p.scan.plan.targetlist = v;
                        }
                        if let Some(v) = qual {
                            p.scan.plan.qual = v;
                        }
                        if let Some(v) = tq {
                            p.tidquals = v;
                        }
                        p.scan.scanrelid += rtoffset as u32;
                    })
                }
                .expect("TidScan node");
            }
        }
        NodeTag::T_TidRangeScan => {
            let s = plan.as_tid_range_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            let tq = fix_scan_list(run, &s.tidrangequals, rtoffset, 1.0)?;
            if rtoffset != 0 || tl.is_some() || qual.is_some() || tq.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::TidRangeScan, _>(|p| {
                        if let Some(v) = tl {
                            p.scan.plan.targetlist = v;
                        }
                        if let Some(v) = qual {
                            p.scan.plan.qual = v;
                        }
                        if let Some(v) = tq {
                            p.tidrangequals = v;
                        }
                        p.scan.scanrelid += rtoffset as u32;
                    })
                }
                .expect("TidRangeScan node");
            }
        }
        NodeTag::T_IndexScan => {
            let s = plan.as_index_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let nr = s.scan.plan.plan_rows;
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, nr)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * nr)?;
            // indexqual/indexorderby run once per rescan (runtime keys), so
            // C costs their AlternativeSubPlan pick at num_exec = 1.
            let iq = fix_scan_list(run, &s.indexqual, rtoffset, 1.0)?;
            let iqo = fix_scan_list(run, &s.indexqualorig, rtoffset, 2.0 * nr)?;
            let iob = fix_scan_list(run, &s.indexorderby, rtoffset, 1.0)?;
            let iobo = fix_scan_list(run, &s.indexorderbyorig, rtoffset, 2.0 * nr)?;
            if rtoffset != 0
                || tl.is_some()
                || qual.is_some()
                || iq.is_some()
                || iqo.is_some()
                || iob.is_some()
                || iobo.is_some()
            {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::IndexScan, _>(|p| {
                        p.scan.scanrelid += rtoffset as u32;
                        if let Some(v) = tl { p.scan.plan.targetlist = v; }
                        if let Some(v) = qual { p.scan.plan.qual = v; }
                        if let Some(v) = iq { p.indexqual = v; }
                        if let Some(v) = iqo { p.indexqualorig = v; }
                        if let Some(v) = iob { p.indexorderby = v; }
                        if let Some(v) = iobo { p.indexorderbyorig = v; }
                    })
                }
                .expect("IndexScan node");
            }
        }
        NodeTag::T_IndexOnlyScan => set_indexonlyscan_references(run, plan, rtoffset)?,
        NodeTag::T_BitmapIndexScan => {
            let s = plan.as_bitmap_index_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            debug_assert!(s.scan.plan.targetlist.is_nil() && s.scan.plan.qual.is_nil());
            let nr = s.scan.plan.plan_rows;
            // indexqual runs once per rescan (runtime keys): num_exec = 1.
            let iq = fix_scan_list(run, &s.indexqual, rtoffset, 1.0)?;
            let iqo = fix_scan_list(run, &s.indexqualorig, rtoffset, 2.0 * nr)?;
            if rtoffset != 0 || iq.is_some() || iqo.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::BitmapIndexScan, _>(|p| {
                        p.scan.scanrelid += rtoffset as u32;
                        if let Some(v) = iq { p.indexqual = v; }
                        if let Some(v) = iqo { p.indexqualorig = v; }
                    })
                }
                .expect("BitmapIndexScan node");
            }
        }
        NodeTag::T_BitmapHeapScan => {
            let s = plan.as_bitmap_heap_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let nr = s.scan.plan.plan_rows;
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, nr)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * nr)?;
            let bqo = fix_scan_list(run, &s.bitmapqualorig, rtoffset, 2.0 * nr)?;
            if rtoffset != 0 || tl.is_some() || qual.is_some() || bqo.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::BitmapHeapScan, _>(|p| {
                        p.scan.scanrelid += rtoffset as u32;
                        if let Some(v) = tl { p.scan.plan.targetlist = v; }
                        if let Some(v) = qual { p.scan.plan.qual = v; }
                        if let Some(v) = bqo { p.bitmapqualorig = v; }
                    })
                }
                .expect("BitmapHeapScan node");
            }
        }
        NodeTag::T_BitmapAnd => {
            let s = plan.as_bitmap_and().unwrap();
            debug_assert!(s.plan.targetlist.is_nil() && s.plan.qual.is_nil());
            let mut fixed = NodeList::nil();
            for child in &s.bitmapplans {
                fixed.lappend(run.mcx, set_plan_refs(run, child, rtoffset)?)?;
            }
            // SAFETY: exclusive plan-tree ownership (prologue note).
            unsafe {
                plan.with_mut::<types_nodes::plannodes::BitmapAnd, _>(|s| {
                    s.bitmapplans = fixed;
                })
            }
            .expect("BitmapAnd node");
        }
        NodeTag::T_BitmapOr => {
            let s = plan.as_bitmap_or().unwrap();
            debug_assert!(s.plan.targetlist.is_nil() && s.plan.qual.is_nil());
            let mut fixed = NodeList::nil();
            for child in &s.bitmapplans {
                fixed.lappend(run.mcx, set_plan_refs(run, child, rtoffset)?)?;
            }
            // SAFETY: exclusive plan-tree ownership (prologue note).
            unsafe {
                plan.with_mut::<types_nodes::plannodes::BitmapOr, _>(|s| {
                    s.bitmapplans = fixed;
                })
            }
            .expect("BitmapOr node");
        }
        NodeTag::T_FunctionScan => {
            let s = plan.as_function_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            let fns = if rtoffset == 0
                && !run.glob.has_alternative_subplans
                && run.root.minmax_aggs.is_empty()
                && run.glob.last_ph_id == 0
            {
                for rtfunc_node in &s.functions {
                    let rtfunc = rtfunc_node.as_range_tbl_function().expect("functions cell");
                    if let Some(fexpr) = rtfunc.funcexpr {
                        fix_scan_expr_walker(run, fexpr)?;
                    }
                }
                None
            } else {
                let mcx = run.mcx;
                let mut out = NodeList::nil();
                for rtfunc_node in &s.functions {
                    let rtfunc = rtfunc_node.as_range_tbl_function().expect("functions cell");
                    let funcexpr = match rtfunc.funcexpr {
                        Some(fexpr) => Some(fix_scan_expr_mutator(run, fexpr, rtoffset, 1.0)?),
                        None => None,
                    };
                    out.lappend(
                        mcx,
                        Node::mk(
                            mcx,
                            types_nodes::parsenodes::RangeTblFunction {
                                funcexpr,
                                funccolcount: rtfunc.funccolcount,
                                funccolnames: rtfunc.funccolnames.clone_in(mcx)?,
                                funccoltypes: rtfunc.funccoltypes.clone_in(mcx)?,
                                funccoltypmods: rtfunc.funccoltypmods.clone_in(mcx)?,
                                funccolcollations: rtfunc.funccolcollations.clone_in(mcx)?,
                                funcparams: rtfunc.funcparams.clone_in(mcx)?,
                            },
                        )?,
                    )?;
                }
                Some(out)
            };
            if rtoffset != 0 || tl.is_some() || qual.is_some() || fns.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::FunctionScan, _>(|s| {
                        if let Some(tl) = tl {
                            s.scan.plan.targetlist = tl;
                        }
                        if let Some(q) = qual {
                            s.scan.plan.qual = q;
                        }
                        if let Some(f) = fns {
                            s.functions = f;
                        }
                        s.scan.scanrelid += rtoffset as u32;
                    })
                }
                .expect("FunctionScan node");
            }
        }
        NodeTag::T_TableFuncScan => {
            let s = plan.as_table_func_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            if let Some(tf) = s.tablefunc {
                fix_scan_expr_walker(run, tf)?;
            }
            if rtoffset != 0 || tl.is_some() || qual.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::TableFuncScan, _>(|s| {
                        if let Some(tl) = tl {
                            s.scan.plan.targetlist = tl;
                        }
                        if let Some(q) = qual {
                            s.scan.plan.qual = q;
                        }
                        s.scan.scanrelid += rtoffset as u32;
                    })
                }
                .expect("TableFuncScan node");
            }
        }
        NodeTag::T_ValuesScan => {
            let s = plan.as_values_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            let vls = fix_scan_list(run, &s.values_lists, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            if rtoffset != 0 || tl.is_some() || qual.is_some() || vls.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::ValuesScan, _>(|s| {
                        if let Some(tl) = tl {
                            s.scan.plan.targetlist = tl;
                        }
                        if let Some(q) = qual {
                            s.scan.plan.qual = q;
                        }
                        if let Some(v) = vls {
                            s.values_lists = v;
                        }
                        s.scan.scanrelid += rtoffset as u32;
                    })
                }
                .expect("ValuesScan node");
            }
        }
        NodeTag::T_CteScan => {
            let s = plan.as_cte_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            if rtoffset != 0 || tl.is_some() || qual.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::CteScan, _>(|s| {
                        if let Some(tl) = tl {
                            s.scan.plan.targetlist = tl;
                        }
                        if let Some(q) = qual {
                            s.scan.plan.qual = q;
                        }
                        s.scan.scanrelid += rtoffset as u32;
                    })
                }
                .expect("CteScan node");
            }
        }
        NodeTag::T_NamedTuplestoreScan => {
            let s = plan.as_named_tuplestore_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            if rtoffset != 0 || tl.is_some() || qual.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::NamedTuplestoreScan, _>(|s| {
                        if let Some(tl) = tl {
                            s.scan.plan.targetlist = tl;
                        }
                        if let Some(q) = qual {
                            s.scan.plan.qual = q;
                        }
                        s.scan.scanrelid += rtoffset as u32;
                    })
                }
                .expect("NamedTuplestoreScan node");
            }
        }
        NodeTag::T_WorkTableScan => {
            let s = plan.as_work_table_scan().unwrap();
            debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
            let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
            let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
            if rtoffset != 0 || tl.is_some() || qual.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::WorkTableScan, _>(|s| {
                        if let Some(tl) = tl {
                            s.scan.plan.targetlist = tl;
                        }
                        if let Some(q) = qual {
                            s.scan.plan.qual = q;
                        }
                        s.scan.scanrelid += rtoffset as u32;
                    })
                }
                .expect("WorkTableScan node");
            }
        }
        NodeTag::T_RecursiveUnion => {
            // Evaluates neither targetlist nor quals.
            set_dummy_tlist_references(run, plan, rtoffset)?;
            debug_assert!(plan.as_plan().unwrap().qual.is_nil());
        }
        NodeTag::T_ProjectSet => {
            set_upper_references(run, plan, rtoffset)?;
        }
        NodeTag::T_Group => {
            set_upper_references(run, plan, rtoffset)?;
        }
        NodeTag::T_Agg => {
            // C's set_plan_refs never walks agg->chain: the chain Aggs
            // carry NIL tlists/quals and stripped Sorts; nothing to fix.
            let a = plan.as_agg().unwrap();
            debug_assert!(a.chain.is_nil() || !a.groupingSets.is_nil());
            for chain_node in &a.chain {
                let c = chain_node.as_agg().expect("chain cell is an Agg");
                debug_assert!(c.plan.targetlist.is_nil() && c.plan.qual.is_nil());
            }
            if a.aggsplit & types_pathnodes::AGGSPLITOP_COMBINE != 0 {
                let mut new_tlist = NodeList::nil();
                for t in &a.plan.targetlist {
                    new_tlist.lappend(run.mcx, convert_combining_aggrefs(run, t)?)?;
                }
                let mut new_qual = NodeList::nil();
                for q in &a.plan.qual {
                    new_qual.lappend(run.mcx, convert_combining_aggrefs(run, q)?)?;
                }
                // SAFETY: exclusive plan-tree ownership (C rewrites in place).
                unsafe {
                    plan.with_plan_mut(|p| {
                        p.targetlist = new_tlist;
                        p.qual = new_qual;
                    })
                }
                .expect("plan node");
            }
            set_upper_references(run, plan, rtoffset)?;
        }
        NodeTag::T_WindowAgg => {
            let w = plan.as_window_agg().unwrap();
            // WindowFunc refs in the runcondition swap to Vars reading the
            // WindowAgg's own output slot (matched against the tlist before
            // set_upper_references rewrites it).
            if !w.runCondition.is_nil() {
                let mut out = NodeList::nil();
                for rc in &w.runCondition {
                    out.lappend(
                        run.mcx,
                        fix_windowagg_condition_expr(run.mcx, rc, &w.plan.targetlist)?,
                    )?;
                }
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::WindowAgg, _>(|p| {
                        p.runCondition = out;
                    })
                }
                .expect("WindowAgg node");
            }
            if let Some(off) = w.startOffset {
                fix_frame_offset(run, off, rtoffset)?;
            }
            if let Some(off) = w.endOffset {
                fix_frame_offset(run, off, rtoffset)?;
            }
            set_upper_references(run, plan, rtoffset)?;
            let w = plan.as_window_agg().unwrap();
            let nexec = w.plan.plan_rows;
            let fixed_rc = fix_scan_list(run, &w.runCondition, rtoffset, nexec)?;
            let fixed_orig = fix_scan_list(run, &w.runConditionOrig, rtoffset, nexec)?;
            if fixed_rc.is_some() || fixed_orig.is_some() {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::WindowAgg, _>(|p| {
                        if let Some(l) = fixed_rc {
                            p.runCondition = l;
                        }
                        if let Some(l) = fixed_orig {
                            p.runConditionOrig = l;
                        }
                    })
                }
                .expect("WindowAgg node");
            }
        }
        NodeTag::T_Memoize => {
            // The tlist is the child's (never evaluated); only the cache-key
            // exprs need fixing.
            set_dummy_tlist_references(run, plan, rtoffset)?;
            let m = plan.as_memoize().unwrap();
            debug_assert!(m.plan.qual.is_nil());
            if let Some(fixed) = fix_scan_list(run, &m.param_exprs, rtoffset, m.plan.plan_rows)? {
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::Memoize, _>(|p| {
                        p.param_exprs = fixed;
                    });
                }
            }
        }
        NodeTag::T_Sort | NodeTag::T_IncrementalSort | NodeTag::T_Unique
        | NodeTag::T_Material => {
            // Neither evaluates its tlist; fixed up for EXPLAIN only.
            set_dummy_tlist_references(run, plan, rtoffset)?;
            debug_assert!(plan.as_plan().unwrap().qual.is_nil());
        }
        NodeTag::T_LockRows => {
            let l = plan.as_lock_rows().unwrap();
            set_dummy_tlist_references(run, plan, rtoffset)?;
            debug_assert!(l.plan.qual.is_nil());
            if rtoffset != 0 {
                for rc_node in &plan.as_lock_rows().unwrap().rowMarks {
                    // SAFETY: exclusive plan-tree ownership (prologue note).
                    unsafe {
                        rc_node.with_mut::<types_nodes::plannodes::PlanRowMark, _>(|rc| {
                            rc.rti += rtoffset as u32;
                            rc.prti += rtoffset as u32;
                        })
                    }
                    .expect("PlanRowMark");
                }
            }
        }
        NodeTag::T_Limit => {
            let l = plan.as_limit().unwrap();
            set_dummy_tlist_references(run, plan, rtoffset)?;
            debug_assert!(l.plan.qual.is_nil());
            if let Some(off) = l.limitOffset {
                fix_scan_expr_walker(run, off)?;
            }
            if let Some(cnt) = l.limitCount {
                fix_scan_expr_walker(run, cnt)?;
            }
        }
        NodeTag::T_NestLoop => {
            set_join_references(run, plan, rtoffset)?;
        }
        NodeTag::T_MergeJoin | NodeTag::T_HashJoin => {
            set_join_references(run, plan, rtoffset)?;
        }
        NodeTag::T_Hash => {
            set_hash_references(run, plan, rtoffset)?;
        }
        NodeTag::T_Gather | NodeTag::T_GatherMerge => {
            set_upper_references(run, plan, rtoffset)?;
            set_param_references(run, plan)?;
        }
        NodeTag::T_ModifyTable => {
            let m = plan.as_modify_table().unwrap();
            debug_assert!(m.plan.targetlist.is_nil() && m.plan.qual.is_nil());
            if rtoffset != 0 {
                for rc_node in &plan.as_modify_table().unwrap().rowMarks {
                    // SAFETY: exclusive plan-tree ownership (prologue note).
                    unsafe {
                        rc_node.with_mut::<types_nodes::plannodes::PlanRowMark, _>(|rc| {
                            rc.rti += rtoffset as u32;
                            rc.prti += rtoffset as u32;
                        })
                    }
                    .expect("PlanRowMark");
                }
            }
            let m = plan.as_modify_table().unwrap();
            if !m.withCheckOptionLists.is_nil() {
                debug_assert_eq!(m.withCheckOptionLists.len(), m.resultRelations.len());
                let mut new_wlists = NodeList::nil();
                let mut any_wco_change = false;
                for wlist_node in &m.withCheckOptionLists {
                    let wlist = wlist_node.as_list().expect("withCheckOptionLists cell");
                    let mut new_wlist = NodeList::nil();
                    for wco_node in wlist {
                        let wco =
                            wco_node.as_with_check_option().expect("WCO cell");
                        let mut new_wco_node = wco_node;
                        if let Some(q) = wco.qual {
                            let ql = q.as_list().expect("preprocessed WCO qual is a list");
                            // C fix_scan_expr rewrites in place (subplan
                            // references force the rewriting path); a changed
                            // qual re-wraps into a fresh WCO node.
                            if let Some(fixed) = fix_scan_list(run, ql, rtoffset, 1.0)? {
                                any_wco_change = true;
                                new_wco_node = Node::mk(
                                    run.mcx,
                                    types_nodes::parsenodes::WithCheckOption {
                                        kind: wco.kind,
                                        relname: wco.relname,
                                        polname: wco.polname,
                                        qual: Some(Node::mk_list(run.mcx, fixed)?),
                                        cascaded: wco.cascaded,
                                    },
                                )?;
                            }
                        }
                        new_wlist.lappend(run.mcx, new_wco_node)?;
                    }
                    new_wlists.lappend(run.mcx, Node::mk_list(run.mcx, new_wlist)?)?;
                }
                if any_wco_change {
                    // SAFETY: exclusive plan-tree ownership (prologue note).
                    unsafe {
                        plan.with_mut::<types_nodes::plannodes::ModifyTable, _>(|p| {
                            p.withCheckOptionLists = new_wlists;
                        })
                    }
                    .expect("ModifyTable node");
                }
            }
            let has_returning = !m.returningLists.is_nil();
            if has_returning {
                debug_assert_eq!(m.returningLists.len(), m.resultRelations.len());
                // set_returning_clause_references (setrefs.c:3399): per result
                // rel, non-result-relation Vars become OUTER_VAR over the
                // subplan tlist (the plan slot is the RETURNING projection's
                // outer tuple); result-rel Vars stay scan Vars. C builds the
                // index with build_tlist_index_other_vars, which excludes the
                // result relation's own Vars from capture.
                let subplan = m
                    .plan
                    .lefttree
                    .expect("ModifyTable has a subplan")
                    .as_plan()
                    .expect("plan node");
                let empty = NodeList::nil();
                let mut new_lists = NodeList::nil();
                for (rlist_node, resultrel) in
                    m.returningLists.iter().zip(m.resultRelations.iter())
                {
                    let rlist =
                        rlist_node.as_list().expect("returningLists cell is a List");
                    let other_vars =
                        build_tlist_other_vars(run, &subplan.targetlist, resultrel)?;
                    let fixed = fix_join_expr_list(
                        run,
                        rlist,
                        &other_vars,
                        &empty,
                        rtoffset,
                        NrmMatch::Equal,
                        resultrel,
                        subplan.plan_rows,
                    )?;
                    new_lists.lappend(run.mcx, Node::mk_list(run.mcx, fixed)?)?;
                }
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::ModifyTable, _>(|p| {
                        p.returningLists = new_lists;
                    })
                }
                .expect("ModifyTable node");
            }
            // The EXCLUDED pseudo-rel is a 'pseudo join' inner side: its Vars
            // in onConflictSet/onConflictWhere become INNER_VAR over the
            // exclRelTlist; result-relation Vars pass through unchanged.
            if !m.onConflictSet.is_nil() {
                let empty = NodeList::nil();
                let acceptable_rel =
                    m.resultRelations.iter().next().expect("ModifyTable result relation");
                let new_set = fix_join_expr_list(
                    run,
                    &m.onConflictSet,
                    &empty,
                    &m.exclRelTlist,
                    rtoffset,
                    NrmMatch::Equal,
                    acceptable_rel,
                    2.0 * m.plan.plan_rows,
                )?;
                let new_where = match m.onConflictWhere {
                    None => None,
                    Some(w) => {
                        let wl = w.as_list().expect("preprocessed onConflictWhere is a list");
                        let mapped = fix_join_expr_list(
                            run,
                            wl,
                            &empty,
                            &m.exclRelTlist,
                            rtoffset,
                            NrmMatch::Equal,
                            acceptable_rel,
                            2.0 * m.plan.plan_rows,
                        )?;
                        Some(Node::mk_list(run.mcx, mapped)?)
                    }
                };
                let fixed = fix_scan_list(run, &m.exclRelTlist, rtoffset, 1.0)?;
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::ModifyTable, _>(|p| {
                        p.onConflictSet = new_set;
                        p.onConflictWhere = new_where;
                        if let Some(fixed) = fixed {
                            p.exclRelTlist = fixed;
                        }
                    })
                }
                .expect("ModifyTable node");
            }
            if !m.mergeActionLists.is_nil() {
                // Source-relation Vars in the action targetlists/quals and
                // join conditions become INNER_VAR over the subplan tlist
                // (ecxt_innertuple = the join output); result-relation Vars
                // pass through to read the scan slot (the old target tuple).
                let subplan_tlist = &m
                    .plan
                    .lefttree
                    .expect("ModifyTable has a subplan")
                    .as_plan()
                    .expect("plan node")
                    .targetlist;
                let empty = NodeList::nil();
                debug_assert_eq!(m.mergeActionLists.len(), m.resultRelations.len());
                debug_assert_eq!(m.mergeJoinConditions.len(), m.resultRelations.len());
                let plan_rows = m.plan.plan_rows;
                let mut new_mjc = NodeList::nil();
                let mut mjc_changed = false;
                for ((mal_node, mjc_node), resultrel) in m
                    .mergeActionLists
                    .iter()
                    .zip(m.mergeJoinConditions.iter())
                    .zip(m.resultRelations.iter())
                {
                    let mal = mal_node.as_list().expect("mergeActionLists cell is a List");
                    for action_node in mal {
                        let action =
                            action_node.as_merge_action().expect("MergeAction cell");
                        let new_tlist = fix_join_expr_list(
                            run,
                            &action.targetList,
                            &empty,
                            subplan_tlist,
                            rtoffset,
                            NrmMatch::Equal,
                            resultrel,
                            plan_rows,
                        )?;
                        let new_qual = match action.qual {
                            None => None,
                            Some(q) => {
                                let ql =
                                    q.as_list().expect("preprocessed action qual is a list");
                                let mapped = fix_join_expr_list(
                                    run,
                                    ql,
                                    &empty,
                                    subplan_tlist,
                                    rtoffset,
                                    NrmMatch::Equal,
                                    resultrel,
                                    plan_rows,
                                )?;
                                Some(Node::mk_list(run.mcx, mapped)?)
                            }
                        };
                        // SAFETY: exclusive plan-tree ownership (prologue note).
                        unsafe {
                            action_node.with_mut::<types_nodes::primnodes::MergeAction, _>(
                                |a| {
                                    a.targetList = new_tlist;
                                    a.qual = new_qual;
                                },
                            )
                        }
                        .expect("MergeAction node");
                    }
                    // The retained join condition (BY SOURCE recheck) maps
                    // like the action quals: source Vars become INNER_VAR
                    // over the subplan tlist, target Vars pass through.
                    let jc = mjc_node.as_list().expect("mergeJoinConditions cell is a List");
                    if !jc.is_nil() {
                        let mapped = fix_join_expr_list(
                            run,
                            jc,
                            &empty,
                            subplan_tlist,
                            rtoffset,
                            NrmMatch::Equal,
                            resultrel,
                            plan_rows,
                        )?;
                        new_mjc.lappend(run.mcx, Node::mk_list(run.mcx, mapped)?)?;
                        mjc_changed = true;
                    } else {
                        new_mjc.lappend(run.mcx, mjc_node)?;
                    }
                }
                if mjc_changed {
                    // SAFETY: exclusive plan-tree ownership (prologue note).
                    unsafe {
                        plan.with_mut::<types_nodes::plannodes::ModifyTable, _>(|p| {
                            p.mergeJoinConditions = new_mjc;
                        })
                    }
                    .expect("ModifyTable node");
                }
            }
            if rtoffset != 0 {
                // wCTE subplans: the CTE subroot's rtable lands in the
                // flattened rangetable at rtoffset.
                let mut new_rr = types_nodes::list::IntList::nil();
                for rti in m.resultRelations.iter() {
                    new_rr.lappend(run.mcx, rti + rtoffset)?;
                }
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe {
                    plan.with_mut::<types_nodes::plannodes::ModifyTable, _>(|p| {
                        p.nominalRelation += rtoffset as types_core::Index;
                        if p.rootRelation != 0 {
                            p.rootRelation += rtoffset as types_core::Index;
                        }
                        // C adds unconditionally, even when unused (0).
                        p.exclRelRTI += rtoffset as types_core::Index;
                        p.resultRelations = new_rr;
                    })
                }
                .expect("ModifyTable node");
            }
            let m = plan.as_modify_table().unwrap();
            for rti in m.resultRelations.iter() {
                run.glob.result_relations.lappend(run.mcx, rti)?;
            }
            if m.rootRelation != 0 {
                run.glob.result_relations.lappend(run.mcx, m.rootRelation as i32)?;
            }
            if has_returning {
                // C copyObject's the first RETURNING list into the visible
                // tlist (EXPLAIN + the node's result slot descriptor); the
                // cells are shared here — the executor never mutates them.
                let first = plan
                    .as_modify_table()
                    .unwrap()
                    .returningLists
                    .nth(0)
                    .as_list()
                    .expect("returningLists cell is a List")
                    .clone_in(run.mcx)?;
                // SAFETY: exclusive plan-tree ownership (prologue note).
                unsafe { plan.with_plan_mut(|p| p.targetlist = first) }.expect("plan node");
            }
        }
        NodeTag::T_Append => {
            return set_append_references(run, plan, rtoffset);
        }
        NodeTag::T_MergeAppend => {
            return set_mergeappend_references(run, plan, rtoffset);
        }
        NodeTag::T_SubqueryScan => {
            return set_subqueryscan_references(run, plan, rtoffset);
        }
        NodeTag::T_SetOp => {
            // SetOp returns its input tuples unmodified; dummy tlist for EXPLAIN.
            set_dummy_tlist_references(run, plan, rtoffset)?;
            debug_assert!(plan.as_plan().unwrap().qual.is_nil());
        }
        other => panic!("set_plan_refs (setrefs.c): {other:?}; M2 plan lane"),
    }

    let base = plan.as_plan().expect("plan node");
    if let Some(child) = base.lefttree {
        let new_child = set_plan_refs(run, child, rtoffset)?;
        // SAFETY: same exclusive plan-tree ownership as the prologue above.
        unsafe { plan.with_plan_mut(|p| p.lefttree = Some(new_child)) }.expect("plan node");
    }
    let base = plan.as_plan().expect("plan node");
    if let Some(child) = base.righttree {
        debug_assert!(matches!(
            plan.node_tag(),
            NodeTag::T_NestLoop
                | NodeTag::T_MergeJoin
                | NodeTag::T_HashJoin
                | NodeTag::T_SetOp
                | NodeTag::T_RecursiveUnion
        ));
        let new_child = set_plan_refs(run, child, rtoffset)?;
        // SAFETY: same exclusive plan-tree ownership as the prologue above.
        unsafe { plan.with_plan_mut(|p| p.righttree = Some(new_child)) }.expect("plan node");
    }
    #[cfg(debug_assertions)]
    audit_no_phv(plan);
    Ok(plan)
}

// C's post-fix invariant (fix_scan_expr_walker's Assert, setrefs.c): no
// PlaceHolderVar survives into the finished plan's expressions.
#[cfg(debug_assertions)]
fn audit_no_phv(plan: Node<'_>) {
    struct PhvAudit {
        tag: NodeTag,
        field: &'static str,
    }
    impl<'mcx> nodes_core::NodeWalker<'mcx> for PhvAudit {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(phv) = node.as_place_holder_var() {
                panic!(
                    "setrefs audit: PlaceHolderVar phid={} phexpr={:?} survived in {:?} {}",
                    phv.phid,
                    phv.phexpr.node_tag(),
                    self.tag,
                    self.field,
                );
            }
            nodes_core::expression_tree_walker(node, self)
        }
    }
    let base = plan.as_plan().expect("plan node");
    for (field, list) in [("targetlist", &base.targetlist), ("qual", &base.qual)] {
        let mut w = PhvAudit { tag: plan.node_tag(), field };
        for n in list {
            let _ = nodes_core::expression_tree_walker(n, &mut w);
        }
    }
    let joinqual = match plan.node_tag() {
        NodeTag::T_NestLoop => Some(&plan.as_nest_loop().unwrap().join.joinqual),
        NodeTag::T_MergeJoin => Some(&plan.as_merge_join().unwrap().join.joinqual),
        NodeTag::T_HashJoin => Some(&plan.as_hash_join().unwrap().join.joinqual),
        _ => None,
    };
    if let Some(list) = joinqual {
        let mut w = PhvAudit { tag: plan.node_tag(), field: "joinqual" };
        for n in list {
            let _ = nodes_core::expression_tree_walker(n, &mut w);
        }
    }
}

// set_upper_references (setrefs.c): retarget an upper node's tlist at its
// subplan's output; the sortgroupref fast path is the M3 grouping lane.
// set_param_references (setrefs.c): record which external initplan output
// Params feed the Gather subtree (the executor rebroadcasts their values to
// workers). extParam is empty unless ss_finalize_plan ran (paramExecTypes
// non-NIL), exactly as in C.
fn set_param_references<'mcx>(run: &PlannerRun<'mcx>, plan: Node<'mcx>) -> PgResult<()> {
    let mcx = run.mcx;
    debug_assert!(matches!(plan.node_tag(), NodeTag::T_Gather | NodeTag::T_GatherMerge));
    let base = plan.as_plan().expect("plan node");
    let lefttree = base.lefttree.expect("Gather has a subplan");
    let ext = &lefttree.as_plan().expect("plan node").extParam;
    if ext.is_empty() {
        return Ok(());
    }
    let mut init_set_param = types_nodes::Bitmapset::empty();
    for root in run
        .suspended_roots
        .iter()
        .map(|s| &s.root)
        .chain(core::iter::once(&run.root))
    {
        for &ipid in root.init_plans.iter() {
            let sp = root
                .expr_node(ipid)
                .as_sub_plan()
                .expect("init_plans holds SubPlan nodes");
            for p in sp.setParam.iter() {
                init_set_param.add_member(mcx, p)?;
            }
        }
    }
    let init_param = ext.intersect(&init_set_param, mcx)?;
    // SAFETY: exclusive plan-tree ownership (prologue note).
    unsafe {
        if plan.node_tag() == NodeTag::T_Gather {
            plan.with_mut::<types_nodes::plannodes::Gather, _>(|g| g.initParam = init_param)
        } else {
            plan.with_mut::<types_nodes::plannodes::GatherMerge, _>(|g| g.initParam = init_param)
        }
    }
    .expect("Gather node");
    Ok(())
}

// convert_combining_aggrefs (setrefs.c): split each Aggref of a combining
// Agg into a child (AGGSPLIT_INITIAL_SERIAL, keeps args/filter) consumed by a
// parent (AGGSPLIT_FINAL_DESERIAL, single-TLE arg, no filter); the following
// set_upper_references resolves the child against the subplan tlist.
fn convert_combining_aggrefs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    if node.node_tag() == NodeTag::T_Aggref {
        let a = node.as_aggref().expect("Aggref");
        assert!(
            a.aggorder.is_nil() && a.aggdistinct.is_nil(),
            "convert_combining_aggrefs: ordered/distinct agg cannot be partialized"
        );
        let child_args = a.args.clone_in(mcx)?;
        let child = crate::grouping::make_marked_aggref(
            run,
            a,
            types_pathnodes::AGGSPLIT_INITIAL_SERIAL,
            child_args,
            a.aggfilter,
        )?;
        let mut parent_args = NodeList::nil();
        parent_args
            .lappend(mcx, Node::mk_target_entry(mcx, child, 1, None, false)?)?;
        return crate::grouping::make_marked_aggref(
            run,
            a,
            types_pathnodes::AGGSPLIT_FINAL_DESERIAL,
            parent_args,
            None,
        );
    }
    Ok(nodes_core::expression_tree_mutator(mcx, node, &mut |n| {
        convert_combining_aggrefs(run, n).map(Some)
    })?
    .unwrap_or(node))
}

fn set_upper_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let mcx = run.mcx;
    // A grouping-sets Agg's tlist/qual Vars and PHVs carry the grouping
    // step's RT index in their nullingrels; drop it first so the subplan
    // matches are exact (setrefs.c:2490-2510).
    if plan.node_tag() == types_nodes::NodeTag::T_Agg && run.root.group_rtindex > 0 {
        let agg = plan.as_agg().expect("Agg");
        if !agg.groupingSets.is_nil() {
            let p = plan.as_plan().expect("plan node");
            let new_tlist = crate::flatten_group::strip_group_nulling_list(
                mcx,
                &p.targetlist,
                run.root.group_rtindex,
            )?;
            let new_qual = crate::flatten_group::strip_group_nulling_list(
                mcx,
                &p.qual,
                run.root.group_rtindex,
            )?;
            if new_tlist.is_some() || new_qual.is_some() {
                // SAFETY: exclusive plan-tree ownership.
                unsafe {
                    plan.with_plan_mut(|pm| {
                        if let Some(t) = new_tlist {
                            pm.targetlist = t;
                        }
                        if let Some(q) = new_qual {
                            pm.qual = q;
                        }
                    })
                }
                .expect("plan node");
            }
        }
    }
    let base = plan.as_plan().expect("plan node");
    let subplan = base.lefttree.expect("upper node has a subplan");
    let subplan_tlist = &subplan.as_plan().expect("plan node").targetlist;

    let mut output_targetlist = NodeList::nil();
    for tle_node in &base.targetlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let mut newexpr = if tle.ressortgroupref != 0 {
            search_indexed_tlist_for_sortgroupref(run, tle.expr, tle.ressortgroupref, subplan_tlist)?
        } else {
            None
        };
        if newexpr.is_none() {
            newexpr = Some(fix_upper_expr(run, tle.expr, subplan_tlist, rtoffset, types_nodes::primnodes::OUTER_VAR, base.plan_rows)?);
        }
        let newexpr = newexpr.unwrap();
        // flatCopyTargetEntry + new expr.
        let new_tle = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: newexpr,
                resno: tle.resno,
                resname: tle.resname,
                ressortgroupref: tle.ressortgroupref,
                resorigtbl: tle.resorigtbl,
                resorigcol: tle.resorigcol,
                resjunk: tle.resjunk,
            },
        )?;
        output_targetlist.lappend(mcx, new_tle)?;
    }
    let mut output_qual = NodeList::nil();
    for qual_node in &base.qual {
        output_qual.lappend(mcx, fix_upper_expr(run, qual_node, subplan_tlist, rtoffset, types_nodes::primnodes::OUTER_VAR, 2.0 * base.plan_rows)?)?;
    }
    // SAFETY: exclusive plan-tree ownership (C rewrites the same lists in place).
    unsafe {
        plan.with_plan_mut(|p| {
            p.targetlist = output_targetlist;
            p.qual = output_qual;
        })
    }
    .expect("plan node");
    Ok(())
}

// set_indexonlyscan_references (setrefs.c): heap Vars in tlist/qual/
// recheckqual retarget to INDEX_VAR positions through the stripped indextlist.
fn set_indexonlyscan_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let mcx = run.mcx;
    let (stripped, tlist, qual, recheckqual) = {
        let s = plan.as_index_only_scan().expect("IndexOnlyScan node");
        debug_assert!(s.scan.scanrelid as i32 + rtoffset > 0);
        let mut stripped = NodeList::nil();
        for tle_node in &s.indextlist {
            if !tle_node.as_target_entry().expect("TargetEntry").resjunk {
                stripped.lappend(mcx, tle_node)?;
            }
        }
        (
            stripped,
            s.scan.plan.targetlist.clone_in(mcx)?,
            s.scan.plan.qual.clone_in(mcx)?,
            s.recheckqual.clone_in(mcx)?,
        )
    };
    const INDEX_VAR: i32 = types_nodes::primnodes::INDEX_VAR;
    let plan_rows = plan.as_plan().expect("plan node").plan_rows;
    let mut new_tlist = NodeList::nil();
    for tle_node in &tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let newexpr = fix_upper_expr(run, tle.expr, &stripped, rtoffset, INDEX_VAR, plan_rows)?;
        new_tlist.lappend(
            mcx,
            Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr: newexpr,
                    resno: tle.resno,
                    resname: tle.resname,
                    ressortgroupref: tle.ressortgroupref,
                    resorigtbl: tle.resorigtbl,
                    resorigcol: tle.resorigcol,
                    resjunk: tle.resjunk,
                },
            )?,
        )?;
    }
    let mut new_qual = NodeList::nil();
    for qual_node in &qual {
        new_qual.lappend(mcx, fix_upper_expr(run, qual_node, &stripped, rtoffset, INDEX_VAR, 2.0 * plan_rows)?)?;
    }
    let mut new_recheck = NodeList::nil();
    for qual_node in &recheckqual {
        new_recheck.lappend(mcx, fix_upper_expr(run, qual_node, &stripped, rtoffset, INDEX_VAR, 2.0 * plan_rows)?)?;
    }
    let (iq, iob, itl) = {
        let s = plan.as_index_only_scan().unwrap();
        (
            // indexqual/indexorderby run once per rescan (runtime keys):
            // num_exec = 1; indextlist is per output tuple (NUM_EXEC_TLIST).
            fix_scan_list(run, &s.indexqual, rtoffset, 1.0)?,
            fix_scan_list(run, &s.indexorderby, rtoffset, 1.0)?,
            fix_scan_list(run, &s.indextlist, rtoffset, s.scan.plan.plan_rows)?,
        )
    };
    // SAFETY: exclusive plan-tree ownership (prologue note).
    unsafe {
        plan.with_mut::<types_nodes::plannodes::IndexOnlyScan, _>(|s| {
            s.scan.scanrelid += rtoffset as u32;
            s.scan.plan.targetlist = new_tlist;
            s.scan.plan.qual = new_qual;
            s.recheckqual = new_recheck;
            if let Some(v) = iq {
                s.indexqual = v;
            }
            if let Some(v) = iob {
                s.indexorderby = v;
            }
            if let Some(v) = itl {
                s.indextlist = v;
            }
        })
    }
    .expect("IndexOnlyScan node");
    Ok(())
}

// search_indexed_tlist_for_sortgroupref (setrefs.c).
fn search_indexed_tlist_for_sortgroupref<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    sortgroupref: u32,
    subplan_tlist: &NodeList<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    for tle_node in subplan_tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        if tle.ressortgroupref != sortgroupref || !types_nodes::equal(tle.expr, node) {
            continue;
        }
        let (vartype, vartypmod) = crate::costsize::expr_type_typmod(node);
        let newvar = types_nodes::primnodes::Var {
            varno: types_nodes::primnodes::OUTER_VAR,
            varattno: tle.resno,
            vartype,
            vartypmod,
            varcollid: exprs_collation(node),
            varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
            varnosyn: 0,
            varattnosyn: 0,
            location: expr_location(node),
        };
        return Ok(Some(Node::mk(run.mcx, newvar)?));
    }
    Ok(None)
}

fn exprs_collation(node: Node<'_>) -> u32 {
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().varcollid,
        NodeTag::T_Const => node.as_const().unwrap().constcollid,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().funccollid,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().opcollid,
        NodeTag::T_WindowFunc => node.as_window_func().unwrap().wincollid,
        NodeTag::T_CaseExpr => node.as_case_expr().unwrap().casecollid,
        NodeTag::T_RowExpr => 0,
        NodeTag::T_CoalesceExpr => node.as_coalesce_expr().unwrap().coalescecollid,
        NodeTag::T_MinMaxExpr => node.as_min_max_expr().unwrap().minmaxcollid,
        NodeTag::T_RelabelType => node.as_relabel_type().unwrap().resultcollid,
        NodeTag::T_CoerceViaIO => node.as_coerce_via_io().unwrap().resultcollid,
        _ => nodes_core::expr_collation(node),
    }
}

fn expr_location(node: Node<'_>) -> i32 {
    match node.node_tag() {
        NodeTag::T_Var => node.as_var().unwrap().location,
        NodeTag::T_Const => node.as_const().unwrap().location,
        NodeTag::T_FuncExpr => node.as_func_expr().unwrap().location,
        NodeTag::T_OpExpr => node.as_op_expr().unwrap().location,
        _ => -1,
    }
}

// fix_upper_expr_mutator (setrefs.c) over the plain-agg tlist shapes.
fn fix_upper_expr<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    subplan_tlist: &NodeList<'mcx>,
    rtoffset: i32,
    newvarno: i32,
    num_exec: f64,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    if node.node_tag() == NodeTag::T_PlaceHolderVar {
        let phv = node.as_place_holder_var().unwrap();
        if let Some(new) =
            search_tlist_for_phv(run, phv, subplan_tlist, newvarno, NrmMatch::Equal)?
        {
            return Ok(new);
        }
        return fix_upper_expr(run, phv.phexpr, subplan_tlist, rtoffset, newvarno, num_exec);
    }
    // search_indexed_tlist_for_non_var: an upper node consuming a value the
    // subplan already computed (Aggref/WindowFunc in a lower tlist) reads it
    // as an OUTER Var instead of re-evaluating. Consts are never replaced
    // (setrefs.c search_indexed_tlist_for_non_var).
    if node.node_tag() != NodeTag::T_Var && node.node_tag() != NodeTag::T_Const {
        for tle_node in subplan_tlist {
            let tle = tle_node.as_target_entry().expect("TargetEntry");
            if types_nodes::equal(tle.expr, node) {
                let (vartype, vartypmod) = crate::costsize::expr_type_typmod(node);
                return Node::mk(
                    mcx,
                    types_nodes::primnodes::Var {
                        varno: newvarno,
                        varattno: tle.resno,
                        vartype,
                        vartypmod,
                        varcollid: exprs_collation(node),
                        varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
                        varlevelsup: 0,
                        varreturningtype:
                            types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
                        varnosyn: 0,
                        varattnosyn: 0,
                        location: expr_location(node),
                    },
                );
            }
        }
    }
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().expect("Var");
            search_indexed_tlist_for_var(run, var, subplan_tlist, rtoffset, newvarno)
        }
        NodeTag::T_Param => fix_param_node(run, node),
        NodeTag::T_Const
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_NextValueExpr => {
            fix_scan_expr_walker(run, node)?;
            Ok(node)
        }
        NodeTag::T_Aggref => {
            if let Some(prm) = find_minmax_agg_replacement_param(&run.root, node) {
                return Ok(*run.root.expr_node(prm));
            }
            let a = node.as_aggref().expect("Aggref");
            record_plan_function_dependency(run, a.aggfnoid)?;
            let mut aggdirectargs = NodeList::nil();
            for d in &a.aggdirectargs {
                aggdirectargs.lappend(
                    mcx,
                    fix_upper_expr(run, d, subplan_tlist, rtoffset, newvarno, num_exec)?,
                )?;
            }
            let aggfilter = match a.aggfilter {
                None => None,
                Some(f) => {
                    Some(fix_upper_expr(run, f, subplan_tlist, rtoffset, newvarno, num_exec)?)
                }
            };
            let mut args = NodeList::nil();
            for arg_node in &a.args {
                let arg = arg_node.as_target_entry().expect("agg arg is a TLE");
                let newexpr = fix_upper_expr(run, arg.expr, subplan_tlist, rtoffset, newvarno, num_exec)?;
                let new_tle = Node::mk(
                    mcx,
                    types_nodes::primnodes::TargetEntry {
                        expr: newexpr,
                        resno: arg.resno,
                        resname: arg.resname,
                        ressortgroupref: arg.ressortgroupref,
                        resorigtbl: arg.resorigtbl,
                        resorigcol: arg.resorigcol,
                        resjunk: arg.resjunk,
                    },
                )?;
                args.lappend(mcx, new_tle)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::Aggref {
                    aggfnoid: a.aggfnoid,
                    aggtype: a.aggtype,
                    aggcollid: a.aggcollid,
                    inputcollid: a.inputcollid,
                    aggtranstype: a.aggtranstype,
                    aggargtypes: a.aggargtypes.clone_in(mcx)?,
                    aggdirectargs,
                    args,
                    // SortGroupClause cells pass through unchanged (C
                    // expression_tree_mutator leaves non-expression nodes).
                    aggorder: a.aggorder.clone_in(mcx)?,
                    aggdistinct: a.aggdistinct.clone_in(mcx)?,
                    aggfilter,
                    aggstar: a.aggstar,
                    aggvariadic: a.aggvariadic,
                    aggkind: a.aggkind,
                    aggpresorted: a.aggpresorted,
                    agglevelsup: a.agglevelsup,
                    aggsplit: a.aggsplit,
                    aggno: a.aggno,
                    aggtransno: a.aggtransno,
                    location: a.location,
                },
            )
        }
        NodeTag::T_GroupingFunc => {
            // fix_expr_common (setrefs.c): cols built from refs through
            // root->grouping_map; args mutated like any other expression.
            let g = node.as_grouping_func().expect("GroupingFunc");
            let grouping_map = &run.root.grouping_map;
            debug_assert!(!grouping_map.is_empty() || g.cols.is_nil());
            let cols = if !run.root.grouping_map.is_empty() {
                let mut cols = types_nodes::list::IntList::nil();
                for r in &g.refs {
                    cols.lappend(mcx, run.root.grouping_map[r as usize] as i32)?;
                }
                debug_assert!(
                    g.cols.is_nil() || g.cols.iter().eq(cols.iter()),
                    "GroupingFunc cols disagree with grouping_map"
                );
                cols
            } else {
                g.cols.clone_in(mcx)?
            };
            let mut args = NodeList::nil();
            for arg in &g.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::GroupingFunc {
                    args,
                    refs: g.refs.clone_in(mcx)?,
                    cols,
                    agglevelsup: g.agglevelsup,
                    location: g.location,
                },
            )
        }
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().expect("WindowFunc");
            record_plan_function_dependency(run, wf.winfnoid)?;
            let mut args = NodeList::nil();
            for arg in &wf.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            let aggfilter = match wf.aggfilter {
                None => None,
                Some(f) => {
                    Some(fix_upper_expr(run, f, subplan_tlist, rtoffset, newvarno, num_exec)?)
                }
            };
            let mut run_condition = NodeList::nil();
            for rc in &wf.runCondition {
                run_condition.lappend(
                    mcx,
                    fix_upper_expr(run, rc, subplan_tlist, rtoffset, newvarno, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::WindowFunc {
                    winfnoid: wf.winfnoid,
                    wintype: wf.wintype,
                    wincollid: wf.wincollid,
                    inputcollid: wf.inputcollid,
                    args,
                    aggfilter,
                    runCondition: run_condition,
                    winref: wf.winref,
                    winstar: wf.winstar,
                    winagg: wf.winagg,
                    location: wf.location,
                },
            )
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().expect("OpExpr");
            let opfuncid = set_opfuncid(o)?;
            record_plan_function_dependency(run, opfuncid)?;
            let mut args = NodeList::nil();
            for arg in &o.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::OpExpr {
                    opno: o.opno,
                    opfuncid,
                    opresulttype: o.opresulttype,
                    opretset: o.opretset,
                    opcollid: o.opcollid,
                    inputcollid: o.inputcollid,
                    args,
                    location: o.location,
                },
            )
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().expect("FuncExpr");
            record_plan_function_dependency(run, f.funcid)?;
            let mut args = NodeList::nil();
            for arg in &f.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::FuncExpr {
                    funcid: f.funcid,
                    funcresulttype: f.funcresulttype,
                    funcretset: f.funcretset,
                    funcvariadic: f.funcvariadic,
                    funcformat: f.funcformat,
                    funccollid: f.funccollid,
                    inputcollid: f.inputcollid,
                    args,
                    location: f.location,
                },
            )
        }
        NodeTag::T_CoerceToDomain => {
            let c = node.as_coerce_to_domain().unwrap();
            let arg = fix_upper_expr(run, c.arg, subplan_tlist, rtoffset, newvarno, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::CoerceToDomain {
                    arg,
                    resulttype: c.resulttype,
                    resulttypmod: c.resulttypmod,
                    resultcollid: c.resultcollid,
                    coercionformat: c.coercionformat,
                    location: c.location,
                },
            )
        }
        NodeTag::T_FieldSelect => {
            let f = node.as_field_select().unwrap();
            let arg = fix_upper_expr(run, f.arg, subplan_tlist, rtoffset, newvarno, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::FieldSelect {
                    arg,
                    fieldnum: f.fieldnum,
                    resulttype: f.resulttype,
                    resulttypmod: f.resulttypmod,
                    resultcollid: f.resultcollid,
                },
            )
        }
        NodeTag::T_FieldStore => {
            let f = node.as_field_store().unwrap();
            let arg = fix_upper_expr(run, f.arg, subplan_tlist, rtoffset, newvarno, num_exec)?;
            let mut newvals = NodeList::nil();
            for v in &f.newvals {
                newvals.lappend(mcx, fix_upper_expr(run, v, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::FieldStore {
                    arg,
                    newvals,
                    fieldnums: f.fieldnums.clone_in(mcx)?,
                    resulttype: f.resulttype,
                },
            )
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().expect("RelabelType");
            let arg = fix_upper_expr(run, r.arg, subplan_tlist, rtoffset, newvarno, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::RelabelType {
                    arg,
                    resulttype: r.resulttype,
                    resulttypmod: r.resulttypmod,
                    resultcollid: r.resultcollid,
                    relabelformat: r.relabelformat,
                    location: r.location,
                },
            )
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().expect("BoolExpr");
            let mut args = NodeList::nil();
            for arg in &b.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::BoolExpr { boolop: b.boolop, args, location: b.location },
            )
        }
        NodeTag::T_NullTest => {
            let nt = node.as_null_test().unwrap();
            let arg = fix_upper_expr(run, nt.arg.expect("NullTest.arg"), subplan_tlist, rtoffset, newvarno, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::NullTest {
                    arg: Some(arg),
                    nulltesttype: nt.nulltesttype,
                    argisrow: nt.argisrow,
                    location: nt.location,
                },
            )
        }
        NodeTag::T_BooleanTest => {
            let bt = node.as_boolean_test().unwrap();
            let arg = fix_upper_expr(run, bt.arg.expect("BooleanTest.arg"), subplan_tlist, rtoffset, newvarno, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::BooleanTest {
                    arg: Some(arg),
                    booltesttype: bt.booltesttype,
                    location: bt.location,
                },
            )
        }
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().unwrap();
            let opfuncid =
                if d.opfuncid != 0 { d.opfuncid } else { lsyscache::get_opcode(d.opno)? };
            record_plan_function_dependency(run, opfuncid)?;
            let mut args = NodeList::nil();
            for arg in &d.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::DistinctExpr {
                    opno: d.opno,
                    opfuncid,
                    opresulttype: d.opresulttype,
                    opretset: d.opretset,
                    opcollid: d.opcollid,
                    inputcollid: d.inputcollid,
                    args,
                    location: d.location,
                },
            )
        }
        NodeTag::T_NullIfExpr => {
            let d = node.as_null_if_expr().unwrap();
            let opfuncid =
                if d.opfuncid != 0 { d.opfuncid } else { lsyscache::get_opcode(d.opno)? };
            record_plan_function_dependency(run, opfuncid)?;
            let mut args = NodeList::nil();
            for arg in &d.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::NullIfExpr {
                    opno: d.opno,
                    opfuncid,
                    opresulttype: d.opresulttype,
                    opretset: d.opretset,
                    opcollid: d.opcollid,
                    inputcollid: d.inputcollid,
                    args,
                    location: d.location,
                },
            )
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            let testexpr = match sp.testexpr {
                None => None,
                Some(te) => Some(fix_upper_expr(run, te, subplan_tlist, rtoffset, newvarno, num_exec)?),
            };
            let mut args = NodeList::nil();
            for arg in &sp.args {
                args.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::SubPlan {
                    subLinkType: sp.subLinkType,
                    testexpr,
                    paramIds: sp.paramIds.clone_in(mcx)?,
                    plan_id: sp.plan_id,
                    plan_name: sp.plan_name,
                    firstColType: sp.firstColType,
                    firstColTypmod: sp.firstColTypmod,
                    firstColCollation: sp.firstColCollation,
                    useHashTable: sp.useHashTable,
                    unknownEqFalse: sp.unknownEqFalse,
                    parallel_safe: sp.parallel_safe,
                    setParam: sp.setParam.clone_in(mcx)?,
                    parParam: sp.parParam.clone_in(mcx)?,
                    args,
                    startup_cost: sp.startup_cost,
                    per_call_cost: sp.per_call_cost,
                },
            )
        }
        NodeTag::T_AlternativeSubPlan => {
            let chosen =
                fix_alternative_subplan(run, node.as_alternative_sub_plan().unwrap(), num_exec);
            fix_upper_expr(run, chosen, subplan_tlist, rtoffset, newvarno, num_exec)
        }
        NodeTag::T_RowExpr => {
            let r = node.as_row_expr().unwrap();
            let mut args = NodeList::nil();
            for a in &r.args {
                args.lappend(
                    mcx,
                    fix_upper_expr(run, a, subplan_tlist, rtoffset, newvarno, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::RowExpr {
                    args,
                    row_typeid: r.row_typeid,
                    row_format: r.row_format,
                    colnames: r.colnames.clone_in(mcx)?,
                    location: r.location,
                },
            )
        }
        NodeTag::T_CaseTestExpr => Ok(node),
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            let arg = match c.arg {
                Some(a) => Some(fix_upper_expr(run, a, subplan_tlist, rtoffset, newvarno, num_exec)?),
                None => None,
            };
            let mut args = NodeList::nil();
            for w in &c.args {
                let cw = w.as_case_when().expect("CaseWhen");
                let expr = fix_upper_expr(
                    run,
                    cw.expr.expect("CaseWhen.expr"),
                    subplan_tlist,
                    rtoffset,
                    newvarno,
                    num_exec,
                )?;
                let result = fix_upper_expr(
                    run,
                    cw.result.expect("CaseWhen.result"),
                    subplan_tlist,
                    rtoffset,
                    newvarno,
                    num_exec,
                )?;
                args.lappend(
                    mcx,
                    Node::mk(
                        mcx,
                        types_nodes::primnodes::CaseWhen {
                            expr: Some(expr),
                            result: Some(result),
                            location: cw.location,
                        },
                    )?,
                )?;
            }
            let defresult = match c.defresult {
                Some(d) => Some(fix_upper_expr(run, d, subplan_tlist, rtoffset, newvarno, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::CaseExpr {
                    casetype: c.casetype,
                    casecollid: c.casecollid,
                    arg,
                    args,
                    defresult,
                    location: c.location,
                },
            )
        }
        NodeTag::T_CoalesceExpr => {
            let co = node.as_coalesce_expr().unwrap();
            let mut args = NodeList::nil();
            for a in &co.args {
                args.lappend(mcx, fix_upper_expr(run, a, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::CoalesceExpr {
                    coalescetype: co.coalescetype,
                    coalescecollid: co.coalescecollid,
                    args,
                    location: co.location,
                },
            )
        }
        NodeTag::T_MinMaxExpr => {
            let mm = node.as_min_max_expr().unwrap();
            let mut args = NodeList::nil();
            for a in &mm.args {
                args.lappend(mcx, fix_upper_expr(run, a, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::MinMaxExpr {
                    minmaxtype: mm.minmaxtype,
                    minmaxcollid: mm.minmaxcollid,
                    inputcollid: mm.inputcollid,
                    op: mm.op,
                    args,
                    location: mm.location,
                },
            )
        }
        NodeTag::T_WindowFuncRunCondition => {
            let rc = node.as_window_func_run_condition().expect("WindowFuncRunCondition");
            let arg = fix_upper_expr(run, rc.arg, subplan_tlist, rtoffset, newvarno, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::WindowFuncRunCondition {
                    opno: rc.opno,
                    inputcollid: rc.inputcollid,
                    wfunc_left: rc.wfunc_left,
                    arg,
                },
            )
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = node.as_scalar_array_op_expr().unwrap();
            // fix_expr_common: set_sa_opfuncid + dependency; eval_const_
            // expressions resolved opfuncid on every live path here.
            debug_assert!(sa.opfuncid != 0, "fix_upper_expr_mutator: unresolved sa opfuncid");
            record_plan_function_dependency(run, sa.opfuncid)?;
            if sa.hashfuncid != 0 {
                record_plan_function_dependency(run, sa.hashfuncid)?;
            }
            if sa.negfuncid != 0 {
                record_plan_function_dependency(run, sa.negfuncid)?;
            }
            let mut args = NodeList::nil();
            for a in &sa.args {
                args.lappend(mcx, fix_upper_expr(run, a, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::ScalarArrayOpExpr {
                    opno: sa.opno,
                    opfuncid: sa.opfuncid,
                    hashfuncid: sa.hashfuncid,
                    negfuncid: sa.negfuncid,
                    useOr: sa.useOr,
                    inputcollid: sa.inputcollid,
                    args,
                    location: sa.location,
                },
            )
        }
        NodeTag::T_ArrayExpr => {
            let a = node.as_array_expr().unwrap();
            let mut elements = NodeList::nil();
            for e in &a.elements {
                elements.lappend(mcx, fix_upper_expr(run, e, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::ArrayExpr {
                    array_typeid: a.array_typeid,
                    array_collid: a.array_collid,
                    element_typeid: a.element_typeid,
                    elements,
                    multidims: a.multidims,
                    list_start: a.list_start,
                    list_end: a.list_end,
                    location: a.location,
                },
            )
        }
        NodeTag::T_CoerceViaIO => {
            let cv = node.as_coerce_via_io().unwrap();
            let arg = fix_upper_expr(run, cv.arg, subplan_tlist, rtoffset, newvarno, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::CoerceViaIO {
                    arg,
                    resulttype: cv.resulttype,
                    resultcollid: cv.resultcollid,
                    coerceformat: cv.coerceformat,
                    location: cv.location,
                },
            )
        }
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            let arg = fix_upper_expr(run, a.arg, subplan_tlist, rtoffset, newvarno, num_exec)?;
            let elemexpr = match a.elemexpr {
                Some(e) => {
                    Some(fix_upper_expr(run, e, subplan_tlist, rtoffset, newvarno, num_exec)?)
                }
                None => None,
            };
            Node::mk(mcx, types_nodes::primnodes::ArrayCoerceExpr { arg, elemexpr, ..*a })
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let c = node.as_convert_rowtype_expr().unwrap();
            let arg = fix_upper_expr(run, c.arg, subplan_tlist, rtoffset, newvarno, num_exec)?;
            Node::mk(mcx, types_nodes::primnodes::ConvertRowtypeExpr { arg, ..*c })
        }
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().unwrap();
            let mut upper = types_nodes::OptNodeList::nil();
            for e in &sr.refupperindexpr {
                let e = match e {
                    Some(e) => Some(fix_upper_expr(run, e, subplan_tlist, rtoffset, newvarno, num_exec)?),
                    None => None,
                };
                upper.lappend(mcx, e)?;
            }
            let mut lower = types_nodes::OptNodeList::nil();
            for e in &sr.reflowerindexpr {
                let e = match e {
                    Some(e) => Some(fix_upper_expr(run, e, subplan_tlist, rtoffset, newvarno, num_exec)?),
                    None => None,
                };
                lower.lappend(mcx, e)?;
            }
            let refexpr = match sr.refexpr {
                Some(e) => Some(fix_upper_expr(run, e, subplan_tlist, rtoffset, newvarno, num_exec)?),
                None => None,
            };
            let refassgnexpr = match sr.refassgnexpr {
                Some(e) => Some(fix_upper_expr(run, e, subplan_tlist, rtoffset, newvarno, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::SubscriptingRef {
                    refcontainertype: sr.refcontainertype,
                    refelemtype: sr.refelemtype,
                    refrestype: sr.refrestype,
                    reftypmod: sr.reftypmod,
                    refcollid: sr.refcollid,
                    refupperindexpr: upper,
                    reflowerindexpr: lower,
                    refexpr,
                    refassgnexpr,
                },
            )
        }

        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            let raw = fix_upper_expr(run, j.raw_expr.expect("raw_expr"), subplan_tlist, rtoffset, newvarno, num_exec)?;
            let formatted = fix_upper_expr(run, j.formatted_expr.expect("formatted_expr"), subplan_tlist, rtoffset, newvarno, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonValueExpr {
                    raw_expr: Some(raw),
                    formatted_expr: Some(formatted),
                    format: j.format,
                },
            )
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            let mut args = NodeList::nil();
            for a in &c.args {
                args.lappend(mcx, fix_upper_expr(run, a, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            let func = match c.func {
                Some(f) => Some(fix_upper_expr(run, f, subplan_tlist, rtoffset, newvarno, num_exec)?),
                None => None,
            };
            let coercion = match c.coercion {
                Some(co) => Some(fix_upper_expr(run, co, subplan_tlist, rtoffset, newvarno, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonConstructorExpr {
                    r#type: c.r#type,
                    args,
                    func,
                    coercion,
                    returning: c.returning,
                    absent_on_null: c.absent_on_null,
                    unique: c.unique,
                    location: c.location,
                },
            )
        }
        NodeTag::T_JsonIsPredicate => {
            let p = node.as_json_is_predicate().unwrap();
            let expr = fix_upper_expr(run, p.expr.expect("expr"), subplan_tlist, rtoffset, newvarno, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonIsPredicate {
                    expr: Some(expr),
                    format: p.format,
                    item_type: p.item_type,
                    unique_keys: p.unique_keys,
                    location: p.location,
                },
            )
        }
        NodeTag::T_JsonBehavior => {
            let b = node.as_json_behavior().unwrap();
            let expr = match b.expr {
                Some(e) => Some(fix_upper_expr(run, e, subplan_tlist, rtoffset, newvarno, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonBehavior {
                    btype: b.btype,
                    expr,
                    coerce: b.coerce,
                    location: b.location,
                },
            )
        }
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            let formatted = fix_upper_expr(run, j.formatted_expr.expect("formatted_expr"), subplan_tlist, rtoffset, newvarno, num_exec)?;
            let path_spec = fix_upper_expr(run, j.path_spec.expect("path_spec"), subplan_tlist, rtoffset, newvarno, num_exec)?;
            let mut passing_values = NodeList::nil();
            for v in &j.passing_values {
                passing_values.lappend(mcx, fix_upper_expr(run, v, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            let on_empty = match j.on_empty {
                Some(e) => Some(fix_upper_expr(run, e, subplan_tlist, rtoffset, newvarno, num_exec)?),
                None => None,
            };
            let on_error = match j.on_error {
                Some(e) => Some(fix_upper_expr(run, e, subplan_tlist, rtoffset, newvarno, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonExpr {
                    op: j.op,
                    column_name: j.column_name,
                    formatted_expr: Some(formatted),
                    format: j.format,
                    path_spec: Some(path_spec),
                    returning: j.returning,
                    passing_names: j.passing_names.clone_in(mcx)?,
                    passing_values,
                    on_empty,
                    on_error,
                    use_io_coercion: j.use_io_coercion,
                    use_json_coercion: j.use_json_coercion,
                    wrapper: j.wrapper,
                    omit_quotes: j.omit_quotes,
                    collation: j.collation,
                    location: j.location,
                },
            )
        }
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            let mut largs = NodeList::nil();
            for arg in &rc.largs {
                largs.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            let mut rargs = NodeList::nil();
            for arg in &rc.rargs {
                rargs.lappend(mcx, fix_upper_expr(run, arg, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::RowCompareExpr {
                    cmptype: rc.cmptype,
                    opnos: rc.opnos.clone_in(mcx)?,
                    opfamilies: rc.opfamilies.clone_in(mcx)?,
                    inputcollids: rc.inputcollids.clone_in(mcx)?,
                    largs,
                    rargs,
                },
            )
        }
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            let mut named_args = NodeList::nil();
            for a in &x.named_args {
                named_args.lappend(mcx, fix_upper_expr(run, a, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            let mut args = NodeList::nil();
            for a in &x.args {
                args.lappend(mcx, fix_upper_expr(run, a, subplan_tlist, rtoffset, newvarno, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::XmlExpr {
                    op: x.op,
                    name: x.name,
                    named_args,
                    arg_names: x.arg_names.clone_in(mcx)?,
                    args,
                    xmloption: x.xmloption,
                    indent: x.indent,
                    r#type: x.r#type,
                    typmod: x.typmod,
                    location: x.location,
                },
            )
        }
        other => panic!("fix_upper_expr_mutator (setrefs.c): {other:?}; M3 expression lane"),
    }
}

// search_indexed_tlist_for_var (setrefs.c), upper leg (NRM_EQUAL callers);
// a miss is C's elog(ERROR). The nullingrels cross-check mirrors C's elog
// guard; the emitted Var keeps the reference Var's nullingrels (C copyVar).
fn search_indexed_tlist_for_var<'mcx>(
    run: &mut PlannerRun<'mcx>,
    var: &types_nodes::primnodes::Var<'mcx>,
    subplan_tlist: &NodeList<'mcx>,
    rtoffset: i32,
    newvarno: i32,
) -> PgResult<Node<'mcx>> {
    debug_assert!(var.varlevelsup == 0);
    for tle_node in subplan_tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let Some(sub) = tle.expr.as_var() else { continue };
        if sub.varno == var.varno && sub.varattno == var.varattno {
            assert!(
                var.varattno <= 0 || sub.varnullingrels.equal(&var.varnullingrels),
                "wrong varnullingrels {:?} (expected {:?}) for Var {}/{}",
                var.varnullingrels,
                sub.varnullingrels,
                var.varno,
                var.varattno
            );
            let mut newvar = types_nodes::primnodes::Var {
                varno: newvarno,
                varattno: tle.resno,
                vartype: var.vartype,
                vartypmod: var.vartypmod,
                varcollid: var.varcollid,
                varnullingrels: var.varnullingrels.clone_in(run.mcx)?,
                varlevelsup: 0,
                varreturningtype: var.varreturningtype,
                varnosyn: var.varnosyn,
                varattnosyn: var.varattnosyn,
                location: var.location,
            };
            if newvar.varnosyn > 0 {
                newvar.varnosyn = newvar.varnosyn.wrapping_add(rtoffset as u32);
            }
            return Node::mk(run.mcx, newvar);
        }
    }
    panic!("variable not found in subplan target list");
}

// fix_scan_expr over a WindowAgg frame offset: offsets are Var-free (parser
// enforced), so C's mutator leg is the identity copy; the walker leg covers
// fix_expr_common bookkeeping.
fn fix_frame_offset<'mcx>(
    run: &mut PlannerRun<'mcx>,
    off: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let _ = rtoffset;
    fix_scan_expr_walker(run, off)
}

// fix_scan_expr (setrefs.c): rtoffset==0 walks in place (fix_expr_common
// only, returns None); rtoffset>0 is the subplan pass and takes C's mutator
// leg, rebuilding the expressions with renumbered varnos.
// fix_windowagg_condition_expr / set_windowagg_runcondition_references
// (setrefs.c): a WindowFunc in the runcondition becomes a Var (varno 0)
// reading the value from the slot the projection just stored it into.
fn fix_windowagg_condition_expr<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    node: Node<'mcx>,
    tlist: &NodeList<'mcx>,
) -> PgResult<Node<'mcx>> {
    if node.node_tag() == NodeTag::T_WindowFunc {
        for tle_node in tlist {
            let tle = tle_node.as_target_entry().expect("TargetEntry");
            if types_nodes::equal(tle.expr, node) {
                let (vartype, vartypmod) = crate::costsize::expr_type_typmod(node);
                return Node::mk(
                    mcx,
                    types_nodes::primnodes::Var {
                        varno: 0,
                        varattno: tle.resno,
                        vartype,
                        vartypmod,
                        varcollid: exprs_collation(node),
                        varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
                        varlevelsup: 0,
                        varreturningtype:
                            types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
                        varnosyn: 0,
                        varattnosyn: 0,
                        location: expr_location(node),
                    },
                );
            }
        }
        panic!("WindowFunc not found in subplan target lists");
    }
    let mut m =
        |n: Node<'mcx>| -> PgResult<Option<Node<'mcx>>> {
            fix_windowagg_condition_expr(mcx, n, tlist).map(Some)
        };
    Ok(nodes_core::expression_tree_mutator(mcx, node, &mut m)?.unwrap_or(node))
}

fn fix_scan_list<'mcx>(
    run: &mut PlannerRun<'mcx>,
    list: &NodeList<'mcx>,
    rtoffset: i32,
    num_exec: f64,
) -> PgResult<Option<NodeList<'mcx>>> {
    if rtoffset == 0
        && !run.glob.has_alternative_subplans
        && run.root.minmax_aggs.is_empty()
        && run.glob.last_ph_id == 0
        // PARAM_MULTIEXPR Params need the rewriting path (fix_param_node).
        && run.root.multiexpr_params.is_empty()
    {
        for node in list {
            fix_scan_expr_walker(run, node)?;
        }
        return Ok(None);
    }
    let mut out = NodeList::nil();
    for node in list {
        out.lappend(run.mcx, fix_scan_expr_mutator(run, node, rtoffset, num_exec)?)?;
    }
    Ok(Some(out))
}

// fix_param_node (setrefs.c): PARAM_MULTIEXPR Params encode
// (subLinkId << 16 | colno) and are replaced by the output PARAM_EXEC
// Param the MULTIEXPR subplan registered in root.multiexpr_params; every
// other Param passes through (immutable arena share stands in for C's
// copyObject).
fn fix_param_node<'mcx>(run: &mut PlannerRun<'mcx>, node: Node<'mcx>) -> PgResult<Node<'mcx>> {
    let p = node.as_variant::<types_nodes::primnodes::Param>().expect("Param");
    if p.paramkind == types_nodes::primnodes::ParamKind::PARAM_MULTIEXPR {
        let subqueryid = (p.paramid >> 16) as usize;
        let colno = (p.paramid & 0xFFFF) as usize;
        if subqueryid == 0 || subqueryid > run.root.multiexpr_params.len() {
            panic!("unexpected PARAM_MULTIEXPR ID: {}", p.paramid);
        }
        let params = &run.root.multiexpr_params[subqueryid - 1];
        if colno == 0 || colno > params.len() {
            panic!("unexpected PARAM_MULTIEXPR ID: {}", p.paramid);
        }
        return Ok(*run.root.expr_node(params[colno - 1]));
    }
    fix_scan_expr_walker(run, node)?;
    Ok(node)
}

// fix_scan_expr_mutator (setrefs.c) over the shapes subplan trees carry.
fn fix_scan_expr_mutator<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    rtoffset: i32,
    num_exec: f64,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    match node.node_tag() {
        NodeTag::T_CurrentOfExpr => {
            let c = node.as_current_of_expr().unwrap();
            let mut new = *c;
            new.cvarno = (new.cvarno as i32 + rtoffset) as u32;
            Ok(Node::mk(mcx, new)?)
        }
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            debug_assert!(var.varlevelsup == 0);
            let mut newvar = types_nodes::primnodes::Var {
                varno: var.varno,
                varattno: var.varattno,
                vartype: var.vartype,
                vartypmod: var.vartypmod,
                varcollid: var.varcollid,
                varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
                varlevelsup: 0,
                varreturningtype: var.varreturningtype,
                varnosyn: var.varnosyn,
                varattnosyn: var.varattnosyn,
                location: var.location,
            };
            if newvar.varno > 0 {
                newvar.varno += rtoffset;
            }
            if newvar.varnosyn > 0 {
                // C Index is unsigned: special-varno Vars (varnosyn = INDEX_VAR
                // as unsigned) wrap here in C too; the value is never read.
                newvar.varnosyn = newvar.varnosyn.wrapping_add(rtoffset as u32);
            }
            Node::mk(mcx, newvar)
        }
        NodeTag::T_Param => fix_param_node(run, node),
        NodeTag::T_Const
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_NextValueExpr => {
            fix_scan_expr_walker(run, node)?;
            Ok(node)
        }
        // At scan level, always evaluate the contained expr (C comment).
        NodeTag::T_PlaceHolderVar => {
            let phv = node.as_place_holder_var().unwrap();
            fix_scan_expr_mutator(run, phv.phexpr, rtoffset, num_exec)
        }
        NodeTag::T_Aggref => {
            let prm = find_minmax_agg_replacement_param(&run.root, node)
                .expect("Aggref outside a minmax Result reaches fix_upper_expr");
            Ok(*run.root.expr_node(prm))
        }
        NodeTag::T_TargetEntry => {
            let tle = node.as_target_entry().unwrap();
            let newexpr = fix_scan_expr_mutator(run, tle.expr, rtoffset, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr: newexpr,
                    resno: tle.resno,
                    resname: tle.resname,
                    ressortgroupref: tle.ressortgroupref,
                    resorigtbl: tle.resorigtbl,
                    resorigcol: tle.resorigcol,
                    resjunk: tle.resjunk,
                },
            )
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            let opfuncid = set_opfuncid(o)?;
            record_plan_function_dependency(run, opfuncid)?;
            let mut args = NodeList::nil();
            for arg in &o.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::OpExpr {
                    opno: o.opno,
                    opfuncid,
                    opresulttype: o.opresulttype,
                    opretset: o.opretset,
                    opcollid: o.opcollid,
                    inputcollid: o.inputcollid,
                    args,
                    location: o.location,
                },
            )
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            record_plan_function_dependency(run, f.funcid)?;
            let mut args = NodeList::nil();
            for arg in &f.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::FuncExpr {
                    funcid: f.funcid,
                    funcresulttype: f.funcresulttype,
                    funcretset: f.funcretset,
                    funcvariadic: f.funcvariadic,
                    funcformat: f.funcformat,
                    funccollid: f.funccollid,
                    inputcollid: f.inputcollid,
                    args,
                    location: f.location,
                },
            )
        }
        NodeTag::T_CoerceToDomain => {
            let c = node.as_coerce_to_domain().unwrap();
            let arg = fix_scan_expr_mutator(run, c.arg, rtoffset, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::CoerceToDomain {
                    arg,
                    resulttype: c.resulttype,
                    resulttypmod: c.resulttypmod,
                    resultcollid: c.resultcollid,
                    coercionformat: c.coercionformat,
                    location: c.location,
                },
            )
        }
        NodeTag::T_FieldSelect => {
            let f = node.as_field_select().unwrap();
            let arg = fix_scan_expr_mutator(run, f.arg, rtoffset, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::FieldSelect {
                    arg,
                    fieldnum: f.fieldnum,
                    resulttype: f.resulttype,
                    resulttypmod: f.resulttypmod,
                    resultcollid: f.resultcollid,
                },
            )
        }
        NodeTag::T_FieldStore => {
            let f = node.as_field_store().unwrap();
            let arg = fix_scan_expr_mutator(run, f.arg, rtoffset, num_exec)?;
            let mut newvals = NodeList::nil();
            for v in &f.newvals {
                newvals.lappend(mcx, fix_scan_expr_mutator(run, v, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::FieldStore {
                    arg,
                    newvals,
                    fieldnums: f.fieldnums.clone_in(mcx)?,
                    resulttype: f.resulttype,
                },
            )
        }
        NodeTag::T_ReturningExpr => {
            let r = node.as_returning_expr().unwrap();
            let retexpr = fix_scan_expr_mutator(run, r.retexpr, rtoffset, num_exec)?;
            Node::mk(mcx, types_nodes::primnodes::ReturningExpr { retexpr, ..*r })
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            let arg = fix_scan_expr_mutator(run, r.arg, rtoffset, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::RelabelType {
                    arg,
                    resulttype: r.resulttype,
                    resulttypmod: r.resulttypmod,
                    resultcollid: r.resultcollid,
                    relabelformat: r.relabelformat,
                    location: r.location,
                },
            )
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            let mut args = NodeList::nil();
            for arg in &b.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::BoolExpr { boolop: b.boolop, args, location: b.location },
            )
        }
        NodeTag::T_NullTest => {
            let nt = node.as_null_test().unwrap();
            let arg = fix_scan_expr_mutator(run, nt.arg.expect("NullTest.arg"), rtoffset, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::NullTest {
                    arg: Some(arg),
                    nulltesttype: nt.nulltesttype,
                    argisrow: nt.argisrow,
                    location: nt.location,
                },
            )
        }
        NodeTag::T_BooleanTest => {
            let bt = node.as_boolean_test().unwrap();
            let arg = fix_scan_expr_mutator(run, bt.arg.expect("BooleanTest.arg"), rtoffset, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::BooleanTest {
                    arg: Some(arg),
                    booltesttype: bt.booltesttype,
                    location: bt.location,
                },
            )
        }
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().unwrap();
            let opfuncid =
                if d.opfuncid != 0 { d.opfuncid } else { lsyscache::get_opcode(d.opno)? };
            record_plan_function_dependency(run, opfuncid)?;
            let mut args = NodeList::nil();
            for arg in &d.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::DistinctExpr {
                    opno: d.opno,
                    opfuncid,
                    opresulttype: d.opresulttype,
                    opretset: d.opretset,
                    opcollid: d.opcollid,
                    inputcollid: d.inputcollid,
                    args,
                    location: d.location,
                },
            )
        }
        NodeTag::T_NullIfExpr => {
            let d = node.as_null_if_expr().unwrap();
            let opfuncid =
                if d.opfuncid != 0 { d.opfuncid } else { lsyscache::get_opcode(d.opno)? };
            record_plan_function_dependency(run, opfuncid)?;
            let mut args = NodeList::nil();
            for arg in &d.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::NullIfExpr {
                    opno: d.opno,
                    opfuncid,
                    opresulttype: d.opresulttype,
                    opretset: d.opretset,
                    opcollid: d.opcollid,
                    inputcollid: d.inputcollid,
                    args,
                    location: d.location,
                },
            )
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            let testexpr = match sp.testexpr {
                None => None,
                Some(te) => Some(fix_scan_expr_mutator(run, te, rtoffset, num_exec)?),
            };
            let mut args = NodeList::nil();
            for arg in &sp.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::SubPlan {
                    subLinkType: sp.subLinkType,
                    testexpr,
                    paramIds: sp.paramIds.clone_in(mcx)?,
                    plan_id: sp.plan_id,
                    plan_name: sp.plan_name,
                    firstColType: sp.firstColType,
                    firstColTypmod: sp.firstColTypmod,
                    firstColCollation: sp.firstColCollation,
                    useHashTable: sp.useHashTable,
                    unknownEqFalse: sp.unknownEqFalse,
                    parallel_safe: sp.parallel_safe,
                    setParam: sp.setParam.clone_in(mcx)?,
                    parParam: sp.parParam.clone_in(mcx)?,
                    args,
                    startup_cost: sp.startup_cost,
                    per_call_cost: sp.per_call_cost,
                },
            )
        }
        NodeTag::T_AlternativeSubPlan => {
            let chosen =
                fix_alternative_subplan(run, node.as_alternative_sub_plan().unwrap(), num_exec);
            fix_scan_expr_mutator(run, chosen, rtoffset, num_exec)
        }
        NodeTag::T_CaseTestExpr => Ok(node),
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            let arg = match c.arg {
                Some(a) => Some(fix_scan_expr_mutator(run, a, rtoffset, num_exec)?),
                None => None,
            };
            let mut args = NodeList::nil();
            for w in &c.args {
                let cw = w.as_case_when().expect("CaseWhen");
                let expr = fix_scan_expr_mutator(run, cw.expr.expect("CaseWhen.expr"), rtoffset, num_exec)?;
                let result =
                    fix_scan_expr_mutator(run, cw.result.expect("CaseWhen.result"), rtoffset, num_exec)?;
                args.lappend(
                    mcx,
                    Node::mk(
                        mcx,
                        types_nodes::primnodes::CaseWhen {
                            expr: Some(expr),
                            result: Some(result),
                            location: cw.location,
                        },
                    )?,
                )?;
            }
            let defresult = match c.defresult {
                Some(d) => Some(fix_scan_expr_mutator(run, d, rtoffset, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::CaseExpr {
                    casetype: c.casetype,
                    casecollid: c.casecollid,
                    arg,
                    args,
                    defresult,
                    location: c.location,
                },
            )
        }
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().unwrap();
            let mut upper = types_nodes::OptNodeList::nil();
            for e in &sr.refupperindexpr {
                let e = match e {
                    Some(e) => Some(fix_scan_expr_mutator(run, e, rtoffset, num_exec)?),
                    None => None,
                };
                upper.lappend(mcx, e)?;
            }
            let mut lower = types_nodes::OptNodeList::nil();
            for e in &sr.reflowerindexpr {
                let e = match e {
                    Some(e) => Some(fix_scan_expr_mutator(run, e, rtoffset, num_exec)?),
                    None => None,
                };
                lower.lappend(mcx, e)?;
            }
            let refexpr = match sr.refexpr {
                Some(e) => Some(fix_scan_expr_mutator(run, e, rtoffset, num_exec)?),
                None => None,
            };
            let refassgnexpr = match sr.refassgnexpr {
                Some(e) => Some(fix_scan_expr_mutator(run, e, rtoffset, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::SubscriptingRef {
                    refcontainertype: sr.refcontainertype,
                    refelemtype: sr.refelemtype,
                    refrestype: sr.refrestype,
                    reftypmod: sr.reftypmod,
                    refcollid: sr.refcollid,
                    refupperindexpr: upper,
                    reflowerindexpr: lower,
                    refexpr,
                    refassgnexpr,
                },
            )
        }
        NodeTag::T_RowExpr => {
            let r = node.as_row_expr().unwrap();
            let mut args = NodeList::nil();
            for a in &r.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, a, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::RowExpr {
                    args,
                    row_typeid: r.row_typeid,
                    row_format: r.row_format,
                    colnames: r.colnames.clone_in(mcx)?,
                    location: r.location,
                },
            )
        }
        NodeTag::T_CoalesceExpr => {
            let co = node.as_coalesce_expr().unwrap();
            let mut args = NodeList::nil();
            for a in &co.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, a, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::CoalesceExpr {
                    coalescetype: co.coalescetype,
                    coalescecollid: co.coalescecollid,
                    args,
                    location: co.location,
                },
            )
        }
        NodeTag::T_MinMaxExpr => {
            let mm = node.as_min_max_expr().unwrap();
            let mut args = NodeList::nil();
            for a in &mm.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, a, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::MinMaxExpr {
                    minmaxtype: mm.minmaxtype,
                    minmaxcollid: mm.minmaxcollid,
                    inputcollid: mm.inputcollid,
                    op: mm.op,
                    args,
                    location: mm.location,
                },
            )
        }
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().expect("WindowFunc");
            record_plan_function_dependency(run, wf.winfnoid)?;
            let mut args = NodeList::nil();
            for arg in &wf.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            let aggfilter = match wf.aggfilter {
                None => None,
                Some(f) => Some(fix_scan_expr_mutator(run, f, rtoffset, num_exec)?),
            };
            let mut run_condition = NodeList::nil();
            for rc in &wf.runCondition {
                run_condition.lappend(mcx, fix_scan_expr_mutator(run, rc, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::WindowFunc {
                    winfnoid: wf.winfnoid,
                    wintype: wf.wintype,
                    wincollid: wf.wincollid,
                    inputcollid: wf.inputcollid,
                    args,
                    aggfilter,
                    runCondition: run_condition,
                    winref: wf.winref,
                    winstar: wf.winstar,
                    winagg: wf.winagg,
                    location: wf.location,
                },
            )
        }
        NodeTag::T_WindowFuncRunCondition => {
            let rc = node.as_window_func_run_condition().expect("WindowFuncRunCondition");
            let arg = fix_scan_expr_mutator(run, rc.arg, rtoffset, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::WindowFuncRunCondition {
                    opno: rc.opno,
                    inputcollid: rc.inputcollid,
                    wfunc_left: rc.wfunc_left,
                    arg,
                },
            )
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = node.as_scalar_array_op_expr().unwrap();
            // fix_expr_common: set_sa_opfuncid + dependency; eval_const_
            // expressions resolved opfuncid on every live path here.
            debug_assert!(sa.opfuncid != 0, "fix_scan_expr_mutator: unresolved sa opfuncid");
            record_plan_function_dependency(run, sa.opfuncid)?;
            if sa.hashfuncid != 0 {
                record_plan_function_dependency(run, sa.hashfuncid)?;
            }
            if sa.negfuncid != 0 {
                record_plan_function_dependency(run, sa.negfuncid)?;
            }
            let mut args = NodeList::nil();
            for arg in &sa.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::ScalarArrayOpExpr {
                    opno: sa.opno,
                    opfuncid: sa.opfuncid,
                    hashfuncid: sa.hashfuncid,
                    negfuncid: sa.negfuncid,
                    useOr: sa.useOr,
                    inputcollid: sa.inputcollid,
                    args,
                    location: sa.location,
                },
            )
        }
        NodeTag::T_ArrayExpr => {
            let a = node.as_array_expr().unwrap();
            let mut elements = NodeList::nil();
            for e in &a.elements {
                elements.lappend(mcx, fix_scan_expr_mutator(run, e, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::ArrayExpr {
                    array_typeid: a.array_typeid,
                    array_collid: a.array_collid,
                    element_typeid: a.element_typeid,
                    elements,
                    multidims: a.multidims,
                    list_start: a.list_start,
                    list_end: a.list_end,
                    location: a.location,
                },
            )
        }
        NodeTag::T_CoerceViaIO => {
            let cv = node.as_coerce_via_io().unwrap();
            let arg = fix_scan_expr_mutator(run, cv.arg, rtoffset, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::CoerceViaIO {
                    arg,
                    resulttype: cv.resulttype,
                    resultcollid: cv.resultcollid,
                    coerceformat: cv.coerceformat,
                    location: cv.location,
                },
            )
        }
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            let arg = fix_scan_expr_mutator(run, a.arg, rtoffset, num_exec)?;
            let elemexpr = match a.elemexpr {
                Some(e) => Some(fix_scan_expr_mutator(run, e, rtoffset, num_exec)?),
                None => None,
            };
            Node::mk(mcx, types_nodes::primnodes::ArrayCoerceExpr { arg, elemexpr, ..*a })
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let c = node.as_convert_rowtype_expr().unwrap();
            let arg = fix_scan_expr_mutator(run, c.arg, rtoffset, num_exec)?;
            Node::mk(mcx, types_nodes::primnodes::ConvertRowtypeExpr { arg, ..*c })
        }
        NodeTag::T_List => {
            let mut out = NodeList::nil();
            for cell in node.as_list().unwrap() {
                out.lappend(mcx, fix_scan_expr_mutator(run, cell, rtoffset, num_exec)?)?;
            }
            Node::mk_list(mcx, out)
        }

        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            let raw = fix_scan_expr_mutator(run, j.raw_expr.expect("raw_expr"), rtoffset, num_exec)?;
            let formatted = fix_scan_expr_mutator(run, j.formatted_expr.expect("formatted_expr"), rtoffset, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonValueExpr {
                    raw_expr: Some(raw),
                    formatted_expr: Some(formatted),
                    format: j.format,
                },
            )
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            let mut args = NodeList::nil();
            for a in &c.args {
                args.lappend(mcx, fix_scan_expr_mutator(run, a, rtoffset, num_exec)?)?;
            }
            let func = match c.func {
                Some(f) => Some(fix_scan_expr_mutator(run, f, rtoffset, num_exec)?),
                None => None,
            };
            let coercion = match c.coercion {
                Some(co) => Some(fix_scan_expr_mutator(run, co, rtoffset, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonConstructorExpr {
                    r#type: c.r#type,
                    args,
                    func,
                    coercion,
                    returning: c.returning,
                    absent_on_null: c.absent_on_null,
                    unique: c.unique,
                    location: c.location,
                },
            )
        }
        NodeTag::T_JsonIsPredicate => {
            let p = node.as_json_is_predicate().unwrap();
            let expr = fix_scan_expr_mutator(run, p.expr.expect("expr"), rtoffset, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonIsPredicate {
                    expr: Some(expr),
                    format: p.format,
                    item_type: p.item_type,
                    unique_keys: p.unique_keys,
                    location: p.location,
                },
            )
        }
        NodeTag::T_JsonBehavior => {
            let b = node.as_json_behavior().unwrap();
            let expr = match b.expr {
                Some(e) => Some(fix_scan_expr_mutator(run, e, rtoffset, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonBehavior {
                    btype: b.btype,
                    expr,
                    coerce: b.coerce,
                    location: b.location,
                },
            )
        }
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            let formatted = fix_scan_expr_mutator(run, j.formatted_expr.expect("formatted_expr"), rtoffset, num_exec)?;
            let path_spec = fix_scan_expr_mutator(run, j.path_spec.expect("path_spec"), rtoffset, num_exec)?;
            let mut passing_values = NodeList::nil();
            for v in &j.passing_values {
                passing_values.lappend(mcx, fix_scan_expr_mutator(run, v, rtoffset, num_exec)?)?;
            }
            let on_empty = match j.on_empty {
                Some(e) => Some(fix_scan_expr_mutator(run, e, rtoffset, num_exec)?),
                None => None,
            };
            let on_error = match j.on_error {
                Some(e) => Some(fix_scan_expr_mutator(run, e, rtoffset, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonExpr {
                    op: j.op,
                    column_name: j.column_name,
                    formatted_expr: Some(formatted),
                    format: j.format,
                    path_spec: Some(path_spec),
                    returning: j.returning,
                    passing_names: j.passing_names.clone_in(mcx)?,
                    passing_values,
                    on_empty,
                    on_error,
                    use_io_coercion: j.use_io_coercion,
                    use_json_coercion: j.use_json_coercion,
                    wrapper: j.wrapper,
                    omit_quotes: j.omit_quotes,
                    collation: j.collation,
                    location: j.location,
                },
            )
        }
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            let mut largs = NodeList::nil();
            for arg in &rc.largs {
                largs.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            let mut rargs = NodeList::nil();
            for arg in &rc.rargs {
                rargs.lappend(mcx, fix_scan_expr_mutator(run, arg, rtoffset, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::RowCompareExpr {
                    cmptype: rc.cmptype,
                    opnos: rc.opnos.clone_in(mcx)?,
                    opfamilies: rc.opfamilies.clone_in(mcx)?,
                    inputcollids: rc.inputcollids.clone_in(mcx)?,
                    largs,
                    rargs,
                },
            )
        }
        other => panic!("fix_scan_expr_mutator (setrefs.c): {other:?}; M2 expression lane"),
    }
}

fn fix_scan_expr_walker<'mcx>(run: &mut PlannerRun<'mcx>, node: Node<'mcx>) -> PgResult<()> {
    match node.node_tag() {
        // fix_expr_common touches no Var fields; INDEX_VAR Vars pass through.
        NodeTag::T_Var => Ok(()),
        // fix_param_node: only PARAM_MULTIEXPR is rewritten, and the walker
        // shortcut is disabled while multiexpr_params is non-empty; PARAM_EXEC
        // passes through.
        NodeTag::T_Param => {
            debug_assert!(
                node.as_variant::<types_nodes::primnodes::Param>().unwrap().paramkind
                    != types_nodes::primnodes::ParamKind::PARAM_MULTIEXPR
            );
            Ok(())
        }
        // fix_expr_common has nothing to record for a SQLValueFunction.
        NodeTag::T_SQLValueFunction | NodeTag::T_NextValueExpr => Ok(()),
        // fix_expr_common ignores CurrentOfExpr; rtoffset==0 leaves cvarno.
        NodeTag::T_CurrentOfExpr => Ok(()),
        NodeTag::T_RelabelType => {
            fix_scan_expr_walker(run, node.as_relabel_type().unwrap().arg)
        }
        NodeTag::T_FieldSelect => {
            fix_scan_expr_walker(run, node.as_field_select().unwrap().arg)
        }
        NodeTag::T_FieldStore => {
            let f = node.as_field_store().unwrap();
            fix_scan_expr_walker(run, f.arg)?;
            for v in &f.newvals {
                fix_scan_expr_walker(run, v)?;
            }
            Ok(())
        }
        NodeTag::T_CoerceToDomain => {
            fix_scan_expr_walker(run, node.as_coerce_to_domain().unwrap().arg)
        }
        NodeTag::T_Const => {
            let c = node.as_const().unwrap();
            // ISREGCLASSCONST: OIDOID accepted because oideq-style folding
            // coerces regclass Consts to plain OID.
            if (c.consttype == REGCLASSOID || c.consttype == OIDOID) && !c.constisnull {
                run.glob
                    .relation_oids
                    .lappend(run.mcx, c.constvalue.as_u32())?;
            }
            Ok(())
        }
        NodeTag::T_TargetEntry => {
            fix_scan_expr_walker(run, node.as_target_entry().unwrap().expr)
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            // The walker cannot write the set_opfuncid memo back into the
            // shared node; assert it was already resolved (the mutator arms
            // handle the switched-clause InvalidOid case).
            debug_assert!(o.opfuncid != 0, "fix_scan_expr_walker: unresolved opfuncid");
            record_plan_function_dependency(run, o.opfuncid)?;
            for arg in &o.args {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            record_plan_function_dependency(run, f.funcid)?;
            for arg in &f.args {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        NodeTag::T_List => {
            for cell in node.as_list().unwrap() {
                fix_scan_expr_walker(run, cell)?;
            }
            Ok(())
        }
        NodeTag::T_BoolExpr => {
            for arg in &node.as_bool_expr().unwrap().args {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        NodeTag::T_NullTest => match node.as_null_test().unwrap().arg {
            Some(arg) => fix_scan_expr_walker(run, arg),
            None => Ok(()),
        },
        NodeTag::T_BooleanTest => match node.as_boolean_test().unwrap().arg {
            Some(arg) => fix_scan_expr_walker(run, arg),
            None => Ok(()),
        },
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().unwrap();
            debug_assert!(d.opfuncid != 0, "fix_scan_expr_walker: unresolved opfuncid");
            record_plan_function_dependency(run, d.opfuncid)?;
            for arg in &d.args {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        NodeTag::T_NullIfExpr => {
            let d = node.as_null_if_expr().unwrap();
            debug_assert!(d.opfuncid != 0, "fix_scan_expr_walker: unresolved opfuncid");
            record_plan_function_dependency(run, d.opfuncid)?;
            for arg in &d.args {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            if let Some(te) = sp.testexpr {
                fix_scan_expr_walker(run, te)?;
            }
            for arg in &sp.args {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = node.as_scalar_array_op_expr().unwrap();
            // fix_expr_common: set_sa_opfuncid + dependency; eval_const_
            // expressions resolved opfuncid on every live path here.
            debug_assert!(sa.opfuncid != 0, "fix_scan_expr_walker: unresolved sa opfuncid");
            record_plan_function_dependency(run, sa.opfuncid)?;
            if sa.hashfuncid != 0 {
                record_plan_function_dependency(run, sa.hashfuncid)?;
            }
            if sa.negfuncid != 0 {
                record_plan_function_dependency(run, sa.negfuncid)?;
            }
            for arg in &sa.args {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        NodeTag::T_CaseTestExpr => Ok(()),
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            for a in x.named_args.iter().chain(x.args.iter()) {
                fix_scan_expr_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_TableFunc => {
            let tf = node.as_table_func().unwrap();
            for a in
                tf.ns_uris.iter().chain(tf.colvalexprs.iter().flatten()).chain(tf.passingvalexprs.iter())
            {
                fix_scan_expr_walker(run, a)?;
            }
            for a in tf.colexprs.iter().chain(tf.coldefexprs.iter()).flatten() {
                fix_scan_expr_walker(run, a)?;
            }
            if let Some(d) = tf.docexpr {
                fix_scan_expr_walker(run, d)?;
            }
            if let Some(r) = tf.rowexpr {
                fix_scan_expr_walker(run, r)?;
            }
            Ok(())
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(a) = c.arg {
                fix_scan_expr_walker(run, a)?;
            }
            for w in &c.args {
                let cw = w.as_case_when().expect("CaseWhen");
                fix_scan_expr_walker(run, cw.expr.expect("CaseWhen.expr"))?;
                fix_scan_expr_walker(run, cw.result.expect("CaseWhen.result"))?;
            }
            match c.defresult {
                Some(d) => fix_scan_expr_walker(run, d),
                None => Ok(()),
            }
        }
        NodeTag::T_CoerceViaIO => fix_scan_expr_walker(run, node.as_coerce_via_io().unwrap().arg),
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            fix_scan_expr_walker(run, a.arg)?;
            match a.elemexpr {
                Some(e) => fix_scan_expr_walker(run, e),
                None => Ok(()),
            }
        }
        NodeTag::T_ConvertRowtypeExpr => {
            fix_scan_expr_walker(run, node.as_convert_rowtype_expr().unwrap().arg)
        }
        NodeTag::T_ArrayExpr => {
            for e in &node.as_array_expr().unwrap().elements {
                fix_scan_expr_walker(run, e)?;
            }
            Ok(())
        }
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().unwrap();
            for e in sr.refupperindexpr.iter().flatten() {
                fix_scan_expr_walker(run, e)?;
            }
            for e in sr.reflowerindexpr.iter().flatten() {
                fix_scan_expr_walker(run, e)?;
            }
            if let Some(e) = sr.refexpr {
                fix_scan_expr_walker(run, e)?;
            }
            if let Some(e) = sr.refassgnexpr {
                fix_scan_expr_walker(run, e)?;
            }
            Ok(())
        }
        NodeTag::T_RowExpr => {
            for e in &node.as_row_expr().unwrap().args {
                fix_scan_expr_walker(run, e)?;
            }
            Ok(())
        }
        NodeTag::T_CoalesceExpr => {
            for e in &node.as_coalesce_expr().unwrap().args {
                fix_scan_expr_walker(run, e)?;
            }
            Ok(())
        }
        NodeTag::T_MinMaxExpr => {
            for e in &node.as_min_max_expr().unwrap().args {
                fix_scan_expr_walker(run, e)?;
            }
            Ok(())
        }
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            record_plan_function_dependency(run, wf.winfnoid)?;
            for arg in &wf.args {
                fix_scan_expr_walker(run, arg)?;
            }
            for rc in &wf.runCondition {
                fix_scan_expr_walker(run, rc)?;
            }
            match wf.aggfilter {
                Some(f) => fix_scan_expr_walker(run, f),
                None => Ok(()),
            }
        }
        NodeTag::T_WindowFuncRunCondition => {
            fix_scan_expr_walker(run, node.as_window_func_run_condition().unwrap().arg)
        }

        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            fix_scan_expr_walker(run, j.raw_expr.expect("raw_expr"))?;
            fix_scan_expr_walker(run, j.formatted_expr.expect("formatted_expr"))
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for a in &c.args {
                fix_scan_expr_walker(run, a)?;
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                fix_scan_expr_walker(run, e)?;
            }
            Ok(())
        }
        NodeTag::T_JsonIsPredicate => {
            fix_scan_expr_walker(run, node.as_json_is_predicate().unwrap().expr.expect("expr"))
        }
        NodeTag::T_JsonBehavior => match node.as_json_behavior().unwrap().expr {
            Some(e) => fix_scan_expr_walker(run, e),
            None => Ok(()),
        },
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            for e in [j.formatted_expr, j.path_spec, j.on_empty, j.on_error]
                .into_iter()
                .flatten()
            {
                fix_scan_expr_walker(run, e)?;
            }
            for v in &j.passing_values {
                fix_scan_expr_walker(run, v)?;
            }
            Ok(())
        }
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            for arg in rc.largs.iter().chain(rc.rargs.iter()) {
                fix_scan_expr_walker(run, arg)?;
            }
            Ok(())
        }
        NodeTag::T_ReturningExpr => {
            fix_scan_expr_walker(run, node.as_returning_expr().unwrap().retexpr)
        }
        other => panic!("fix_scan_expr_walker (setrefs.c): {other:?}; M2 expression lane"),
    }
}

// fix_alternative_subplan (setrefs.c): keep the cheapest member for the
// expected execution count. Divergence: C NULLs the losers out of
// glob->subplans so the executor never initializes them; here they stay
// (initialized but never executed or displayed).
fn fix_alternative_subplan<'mcx>(
    run: &mut PlannerRun<'mcx>,
    asplan: &'mcx types_nodes::primnodes::AlternativeSubPlan<'mcx>,
    num_exec: f64,
) -> Node<'mcx> {
    let _ = run;
    let mut best: Option<(Node<'mcx>, f64)> = None;
    for sub_node in &asplan.subplans {
        let sp = sub_node.as_sub_plan().expect("AlternativeSubPlan member");
        let curcost = sp.startup_cost + num_exec * sp.per_call_cost;
        // Ties prefer the later plan (bias against fast-start), as C.
        if best.as_ref().is_none_or(|(_, c)| curcost <= *c) {
            best = Some((sub_node, curcost));
        }
    }
    best.expect("AlternativeSubPlan has members").0
}

// set_opfuncid (nodeFuncs.c): get_switched_clauses hands over commuted
// OpExprs with opfuncid = InvalidOid; C's fix_expr_common resolves them here.
fn set_opfuncid(o: &types_nodes::primnodes::OpExpr<'_>) -> PgResult<u32> {
    if o.opfuncid != 0 {
        return Ok(o.opfuncid);
    }
    lsyscache::get_opcode(o.opno)
}

// Built-ins (OID < FirstUnpinnedObjectId) are assumed immutable and untracked.
pub(crate) fn record_plan_function_dependency<'mcx>(
    run: &mut PlannerRun<'mcx>,
    funcid: u32,
) -> PgResult<()> {
    if funcid < FIRST_UNPINNED_OBJECT_ID {
        return Ok(());
    }
    let hash_value = syscache_seams::syscache_hash_value_procoid::call(funcid)?;
    let item = Node::mk(
        run.mcx,
        types_nodes::plannodes::PlanInvalItem { cacheId: PROCOID, hashValue: hash_value },
    )?;
    run.glob.inval_items.lappend(run.mcx, item)?;
    Ok(())
}

// record_plan_type_dependency (setrefs.c:3594): folded constraint-less
// domains; ALTER DOMAIN's manual pg_type sinval rebuilds the plan.
pub(crate) fn record_plan_type_dependency<'mcx>(
    run: &mut PlannerRun<'mcx>,
    typid: u32,
) -> PgResult<()> {
    if typid < FIRST_UNPINNED_OBJECT_ID {
        return Ok(());
    }
    let hash_value = syscache_seams::syscache_hash_value_typeoid::call(typid)?;
    let item = Node::mk(
        run.mcx,
        types_nodes::plannodes::PlanInvalItem { cacheId: TYPEOID, hashValue: hash_value },
    )?;
    run.glob.inval_items.lappend(run.mcx, item)?;
    Ok(())
}

fn set_dummy_tlist_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let mcx = run.mcx;
    let mut output_targetlist = NodeList::nil();
    for tle_node in &plan.as_plan().expect("plan node").targetlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let oldexpr = tle.expr;
        // Consts stay Consts (cleaner EXPLAIN; C keeps the whole TLE).
        if oldexpr.node_tag() == NodeTag::T_Const {
            output_targetlist.lappend(mcx, tle_node)?;
            continue;
        }
        let (vartype, vartypmod) = crate::costsize::expr_type_typmod(oldexpr);
        let varcollid = crate::pathkeys::expr_collation(oldexpr);
        let mut newvar = types_nodes::primnodes::Var {
            varno: types_nodes::primnodes::OUTER_VAR,
            varattno: tle.resno,
            vartype,
            vartypmod,
            varcollid,
            varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: Default::default(),
            varnosyn: 0,
            varattnosyn: 0,
            location: -1,
        };
        if let Some(oldvar) = oldexpr.as_var() {
            if oldvar.varnosyn > 0 {
                newvar.varnosyn = oldvar.varnosyn.wrapping_add(rtoffset as u32);
                newvar.varattnosyn = oldvar.varattnosyn;
            }
        }
        let new_tle = Node::mk(
            mcx,
            types_nodes::primnodes::TargetEntry {
                expr: Node::mk(mcx, newvar)?,
                resno: tle.resno,
                resname: tle.resname,
                ressortgroupref: tle.ressortgroupref,
                resorigtbl: tle.resorigtbl,
                resorigcol: tle.resorigcol,
                resjunk: tle.resjunk,
            },
        )?;
        output_targetlist.lappend(mcx, new_tle)?;
    }
    // SAFETY: exclusive plan-tree ownership (C rewrites the list in place).
    unsafe { plan.with_plan_mut(|p| p.targetlist = output_targetlist) }.expect("plan node");
    Ok(())
}

// set_join_references (setrefs.c), inner-nestloop arm: joinqual and tlist
// Vars retarget onto the child tlists as OUTER_VAR/INNER_VAR. C builds
// indexed_tlists; the linear probe is the set_upper_references divergence
// (cold, tlists tiny). nestParams/merge/hash legs are dead or loud upstream.
fn set_join_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let base = plan.as_plan().expect("join plan");
    let outer_tlist = &base.lefttree.expect("join outer plan").as_plan().unwrap().targetlist;
    let inner_tlist = &base.righttree.expect("join inner plan").as_plan().unwrap().targetlist;

    let is_hash = plan.node_tag() == NodeTag::T_HashJoin;
    let is_merge = plan.node_tag() == NodeTag::T_MergeJoin;
    let (joinqual_src, jointype) = if is_hash {
        let j = plan.as_hash_join().unwrap();
        (&j.join.joinqual, j.join.jointype)
    } else if is_merge {
        let j = plan.as_merge_join().unwrap();
        (&j.join.joinqual, j.join.jointype)
    } else {
        let j = plan.as_nest_loop().unwrap();
        (&j.join.joinqual, j.join.jointype)
    };
    let above_nrm = if jointype == types_nodes::JoinType::JOIN_INNER {
        NrmMatch::Equal
    } else {
        NrmMatch::Superset
    };
    // nestParams reference the outer plan's tlist (paths were never
    // reparameterized, so translate via the outer itlist like C's
    // fix_upper_expr(OUTER_VAR) leg).
    if !is_hash && !is_merge {
        let nl = plan.as_nest_loop().unwrap();
        if !nl.nestParams.is_nil() {
            let mcx = run.mcx;
            let empty = NodeList::nil();
            let mut new_params = NodeList::nil();
            for nlp_node in &nl.nestParams {
                let nlp = nlp_node.as_nest_loop_param().expect("nestParams cell");
                let mut single = NodeList::nil();
                single.lappend(mcx, nlp.paramval)?;
                // C uses NRM_SUBSET here: unreparameterized paths' Vars/PHVs
                // may carry a nullingrels subset of the outer side's.
                let fixed = fix_join_expr_list(
                    run, &single, outer_tlist, &empty, rtoffset, NrmMatch::Subset, 0,
                    base.plan_rows,
                )?;
                let paramval = fixed.nth(0);
                let ok = paramval
                    .as_var()
                    .is_some_and(|v| v.varno == types_nodes::primnodes::OUTER_VAR);
                assert!(ok, "NestLoopParam was not reduced to a simple Var");
                new_params.lappend(
                    mcx,
                    types_nodes::Node::mk(
                        mcx,
                        types_nodes::plannodes::NestLoopParam {
                            paramno: nlp.paramno,
                            paramval,
                        },
                    )?,
                )?;
            }
            // SAFETY: exclusive plan-tree ownership (C rewrites in place).
            unsafe {
                plan.with_mut::<types_nodes::plannodes::NestLoop, _>(|p| {
                    p.nestParams = new_params;
                })
            }
            .expect("NestLoop");
        }
    }

    let joinqual = fix_join_expr_list(
        run, joinqual_src, outer_tlist, inner_tlist, rtoffset, NrmMatch::Equal, 0,
        2.0 * base.plan_rows,
    )?;
    let targetlist = fix_join_expr_list(
        run, &base.targetlist, outer_tlist, inner_tlist, rtoffset, above_nrm, 0, base.plan_rows,
    )?;
    let qual = fix_join_expr_list(
        run, &base.qual, outer_tlist, inner_tlist, rtoffset, above_nrm, 0, 2.0 * base.plan_rows,
    )?;

    if is_hash {
        let hj = plan.as_hash_join().unwrap();
        let hashclauses = fix_join_expr_list(
            run,
            &hj.hashclauses,
            outer_tlist,
            inner_tlist,
            rtoffset,
            NrmMatch::Equal,
            0,
            2.0 * base.plan_rows,
        )?;
        // HashJoin's hashkeys look up outer tuples: outer itlist -> OUTER_VAR.
        let empty = NodeList::nil();
        let hashkeys = fix_join_expr_list(
            run, &hj.hashkeys, outer_tlist, &empty, rtoffset, NrmMatch::Equal, 0,
            2.0 * base.plan_rows,
        )?;
        // SAFETY: exclusive plan-tree ownership (C rewrites in place).
        unsafe {
            plan.with_mut::<types_nodes::plannodes::HashJoin, _>(|p| {
                p.join.joinqual = joinqual;
                p.join.plan.targetlist = targetlist;
                p.join.plan.qual = qual;
                p.hashclauses = hashclauses;
                p.hashkeys = hashkeys;
            })
        }
        .expect("HashJoin node");
    } else if is_merge {
        let mj = plan.as_merge_join().unwrap();
        let mergeclauses = fix_join_expr_list(
            run,
            &mj.mergeclauses,
            outer_tlist,
            inner_tlist,
            rtoffset,
            NrmMatch::Equal,
            0,
            2.0 * base.plan_rows,
        )?;
        // SAFETY: exclusive plan-tree ownership (C rewrites in place).
        unsafe {
            plan.with_mut::<types_nodes::plannodes::MergeJoin, _>(|p| {
                p.join.joinqual = joinqual;
                p.join.plan.targetlist = targetlist;
                p.join.plan.qual = qual;
                p.mergeclauses = mergeclauses;
            })
        }
        .expect("MergeJoin node");
    } else {
        // SAFETY: exclusive plan-tree ownership (C rewrites in place).
        unsafe {
            plan.with_mut::<types_nodes::plannodes::NestLoop, _>(|p| {
                p.join.joinqual = joinqual;
                p.join.plan.targetlist = targetlist;
                p.join.plan.qual = qual;
            })
        }
        .expect("NestLoop node");
    }
    Ok(())
}

// setrefs.c NullingRelsMatch.
#[derive(Clone, Copy, PartialEq, Debug)]
enum NrmMatch {
    Equal,
    Subset,
    Superset,
}

// set_hash_references (setrefs.c): the Hash node's hashkeys reference its own
// outer plan (the HashJoin inner) mapped to OUTER_VAR; the tlist is a dummy
// passthrough (Hash doesn't project).
fn set_hash_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let hash = plan.as_hash().expect("Hash");
    let outer_tlist =
        &hash.plan.lefttree.expect("Hash outer plan").as_plan().unwrap().targetlist;
    let empty = NodeList::nil();
    let hashkeys =
        fix_join_expr_list(run, &hash.hashkeys, outer_tlist, &empty, rtoffset, NrmMatch::Equal, 0, 2.0 * hash.plan.plan_rows)?;
    // SAFETY: exclusive plan-tree ownership (C rewrites in place).
    unsafe { plan.with_mut::<types_nodes::plannodes::Hash, _>(|p| p.hashkeys = hashkeys) }
        .expect("Hash node");
    set_dummy_tlist_references(run, plan, rtoffset)?;
    debug_assert!(plan.as_plan().unwrap().qual.is_nil());
    Ok(())
}

// build_tlist_index_other_vars (setrefs.c): searchable subset of a tlist for
// RETURNING fixup — Vars of relations other than ignore_rel, plus
// PlaceHolderVar entries (C keeps has_ph_vars searching the whole tlist).
fn build_tlist_other_vars<'mcx>(
    run: &mut PlannerRun<'mcx>,
    tlist: &NodeList<'mcx>,
    ignore_rel: i32,
) -> PgResult<NodeList<'mcx>> {
    let mut out = NodeList::nil();
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        match tle.expr.node_tag() {
            NodeTag::T_Var => {
                if tle.expr.as_var().unwrap().varno != ignore_rel {
                    out.lappend(run.mcx, tle_node)?;
                }
            }
            NodeTag::T_PlaceHolderVar => out.lappend(run.mcx, tle_node)?,
            _ => {}
        }
    }
    Ok(out)
}

fn fix_join_expr_list<'mcx>(
    run: &mut PlannerRun<'mcx>,
    list: &NodeList<'mcx>,
    outer_tlist: &NodeList<'mcx>,
    inner_tlist: &NodeList<'mcx>,
    rtoffset: i32,
    nrm_match: NrmMatch,
    acceptable_rel: i32,
    num_exec: f64,
) -> PgResult<NodeList<'mcx>> {
    let mut out = NodeList::nil();
    for node in list {
        out.lappend(
            run.mcx,
            fix_join_expr_mutator(run, node, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?,
        )?;
    }
    Ok(out)
}

// search_indexed_tlist_for_non_var (setrefs.c), join leg: whole-expression
// match against a subplan tlist entry. Consts are excluded (C: replacing one
// could confuse the Const-mutation exemption); the emitted Var clears
// varnosyn/varattnosyn (was never a plain Var).
fn search_join_tlist_for_non_var<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    node: Node<'mcx>,
    subplan_tlist: &NodeList<'mcx>,
    newvarno: i32,
) -> PgResult<Option<Node<'mcx>>> {
    if node.node_tag() == NodeTag::T_Const {
        return Ok(None);
    }
    for tle_node in subplan_tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        if matches!(tle.expr.node_tag(), NodeTag::T_Var | NodeTag::T_PlaceHolderVar) {
            continue;
        }
        if types_nodes::equal(tle.expr, node) {
            let (vartype, vartypmod) = crate::costsize::expr_type_typmod(node);
            return Ok(Some(Node::mk(
                mcx,
                types_nodes::primnodes::Var {
                    varno: newvarno,
                    varattno: tle.resno,
                    vartype,
                    vartypmod,
                    varcollid: exprs_collation(node),
                    varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
                    varlevelsup: 0,
                    varreturningtype:
                        types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
                    varnosyn: 0,
                    varattnosyn: 0,
                    location: expr_location(node),
                },
            )?));
        }
    }
    Ok(None)
}

// fix_join_expr_mutator (setrefs.c) over the shapes this lane can carry.
// fix_join_expr_mutator's acceptable_rel arm (C copyVar; keeps nullingrels).
fn fix_join_acceptable_rel_var<'mcx>(
    mcx: ::mcx::Mcx<'mcx>,
    var: &types_nodes::primnodes::Var<'mcx>,
    rtoffset: i32,
    node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    if rtoffset == 0 {
        return Ok(node);
    }
    let mut newvar = types_nodes::primnodes::Var {
        varno: var.varno + rtoffset,
        varattno: var.varattno,
        vartype: var.vartype,
        vartypmod: var.vartypmod,
        varcollid: var.varcollid,
        varnullingrels: var.varnullingrels.clone_in(mcx)?,
        varlevelsup: var.varlevelsup,
        varreturningtype: var.varreturningtype,
        varnosyn: var.varnosyn,
        varattnosyn: var.varattnosyn,
        location: var.location,
    };
    if newvar.varnosyn > 0 {
        newvar.varnosyn = newvar.varnosyn.wrapping_add(rtoffset as u32);
    }
    Node::mk(mcx, newvar)
}

fn fix_join_expr_mutator<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
    outer_tlist: &NodeList<'mcx>,
    inner_tlist: &NodeList<'mcx>,
    rtoffset: i32,
    nrm_match: NrmMatch,
    acceptable_rel: i32,
    num_exec: f64,
) -> PgResult<Node<'mcx>> {
    let mcx = run.mcx;
    // C probes search_indexed_tlist_for_non_var on every non-Var/PHV node
    // before the per-tag recursion (setrefs.c fix_join_expr_mutator): a
    // whole subexpression the subplan already computes is consumed as an
    // OUTER/INNER Var instead of being re-evaluated at the join.
    if !matches!(node.node_tag(), NodeTag::T_Var | NodeTag::T_PlaceHolderVar) {
        if let Some(new) = search_join_tlist_for_non_var(
            mcx,
            node,
            outer_tlist,
            types_nodes::primnodes::OUTER_VAR,
        )? {
            return Ok(new);
        }
        if let Some(new) = search_join_tlist_for_non_var(
            mcx,
            node,
            inner_tlist,
            types_nodes::primnodes::INNER_VAR,
        )? {
            return Ok(new);
        }
    }
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            // C fix_join_expr_mutator's OLD/NEW invariant, resolved here
            // directly: C's itlist excludes the result relation, so an
            // OLD/NEW Var can only pass through as acceptable_rel.
            if var.varreturningtype
                != types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT
            {
                // C additionally requires outer_itlist != NULL, but the
                // RETURNING fixup's other-vars index can be an empty list
                // here (indistinguishable from absent); inner-nil plus
                // acceptable_rel identifies the RETURNING context.
                assert!(
                    inner_tlist.is_nil() && acceptable_rel != 0,
                    "variable returning old/new found outside RETURNING list"
                );
                assert!(
                    var.varno == acceptable_rel,
                    "wrong varno {} (expected {}) for variable returning old/new",
                    var.varno,
                    acceptable_rel
                );
                return fix_join_acceptable_rel_var(mcx, var, rtoffset, node);
            }
            if let Some(new) = search_join_tlist_for_var(
                run,
                var,
                outer_tlist,
                types_nodes::primnodes::OUTER_VAR,
                rtoffset,
                nrm_match,
            )? {
                return Ok(new);
            }
            if let Some(new) = search_join_tlist_for_var(
                run,
                var,
                inner_tlist,
                types_nodes::primnodes::INNER_VAR,
                rtoffset,
                nrm_match,
            )? {
                return Ok(new);
            }
            // Result-relation Vars stay scan Vars, shifted into the
            // flattened rangetable (C copyVar + varno += rtoffset).
            if acceptable_rel != 0 && var.varno == acceptable_rel {
                return fix_join_acceptable_rel_var(mcx, var, rtoffset, node);
            }
            panic!("variable not found in subplan target lists");
        }
        NodeTag::T_PlaceHolderVar => {
            let phv = node.as_place_holder_var().unwrap();
            if let Some(new) = search_tlist_for_phv(
                run,
                phv,
                outer_tlist,
                types_nodes::primnodes::OUTER_VAR,
                nrm_match,
            )? {
                return Ok(new);
            }
            if let Some(new) = search_tlist_for_phv(
                run,
                phv,
                inner_tlist,
                types_nodes::primnodes::INNER_VAR,
                nrm_match,
            )? {
                return Ok(new);
            }
            fix_join_expr_mutator(run, phv.phexpr, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)
        }
        NodeTag::T_Const | NodeTag::T_SQLValueFunction | NodeTag::T_NextValueExpr => {
            fix_scan_expr_walker(run, node)?;
            Ok(node)
        }
        // C fix_expr_common has no MergeSupportFunc leg: the node passes
        // through untouched to the executor (EEOP_MERGE_SUPPORT_FUNC).
        NodeTag::T_MergeSupportFunc => Ok(node),
        // No C case: falls to C's expression_tree_mutator default.
        NodeTag::T_ReturningExpr => {
            let r = node.as_returning_expr().unwrap();
            let retexpr = fix_join_expr_mutator(run, r.retexpr, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?;
            Node::mk(mcx, types_nodes::primnodes::ReturningExpr { retexpr, ..*r })
        }
        NodeTag::T_TargetEntry => {
            let tle = node.as_target_entry().unwrap();
            let newexpr =
                fix_join_expr_mutator(run, tle.expr, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::TargetEntry {
                    expr: newexpr,
                    resno: tle.resno,
                    resname: tle.resname,
                    ressortgroupref: tle.ressortgroupref,
                    resorigtbl: tle.resorigtbl,
                    resorigcol: tle.resorigcol,
                    resjunk: tle.resjunk,
                },
            )
        }
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            let opfuncid = set_opfuncid(o)?;
            record_plan_function_dependency(run, opfuncid)?;
            let mut args = NodeList::nil();
            for arg in &o.args {
                args.lappend(
                    mcx,
                    fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::OpExpr {
                    opno: o.opno,
                    opfuncid,
                    opresulttype: o.opresulttype,
                    opretset: o.opretset,
                    opcollid: o.opcollid,
                    inputcollid: o.inputcollid,
                    args,
                    location: o.location,
                },
            )
        }
        NodeTag::T_CoerceToDomain => {
            let c = node.as_coerce_to_domain().unwrap();
            let arg = fix_join_expr_mutator(run, c.arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::CoerceToDomain {
                    arg,
                    resulttype: c.resulttype,
                    resulttypmod: c.resulttypmod,
                    resultcollid: c.resultcollid,
                    coercionformat: c.coercionformat,
                    location: c.location,
                },
            )
        }
        NodeTag::T_FieldSelect => {
            let f = node.as_field_select().unwrap();
            let arg = fix_join_expr_mutator(run, f.arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::FieldSelect {
                    arg,
                    fieldnum: f.fieldnum,
                    resulttype: f.resulttype,
                    resulttypmod: f.resulttypmod,
                    resultcollid: f.resultcollid,
                },
            )
        }
        NodeTag::T_FieldStore => {
            let f = node.as_field_store().unwrap();
            let arg = fix_join_expr_mutator(run, f.arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?;
            let mut newvals = NodeList::nil();
            for v in &f.newvals {
                newvals.lappend(mcx, fix_join_expr_mutator(run, v, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::FieldStore {
                    arg,
                    newvals,
                    fieldnums: f.fieldnums.clone_in(mcx)?,
                    resulttype: f.resulttype,
                },
            )
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            let arg = fix_join_expr_mutator(run, r.arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::RelabelType {
                    arg,
                    resulttype: r.resulttype,
                    resulttypmod: r.resulttypmod,
                    resultcollid: r.resultcollid,
                    relabelformat: r.relabelformat,
                    location: r.location,
                },
            )
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            let mut args = NodeList::nil();
            for arg in &b.args {
                args.lappend(
                    mcx,
                    fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::BoolExpr { boolop: b.boolop, args, location: b.location },
            )
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            record_plan_function_dependency(run, f.funcid)?;
            let mut args = NodeList::nil();
            for arg in &f.args {
                args.lappend(
                    mcx,
                    fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::FuncExpr {
                    funcid: f.funcid,
                    funcresulttype: f.funcresulttype,
                    funcretset: f.funcretset,
                    funcvariadic: f.funcvariadic,
                    funcformat: f.funcformat,
                    funccollid: f.funccollid,
                    inputcollid: f.inputcollid,
                    args,
                    location: f.location,
                },
            )
        }
        NodeTag::T_NullTest => {
            let nt = node.as_null_test().unwrap();
            let arg = match nt.arg {
                None => None,
                Some(a) => Some(fix_join_expr_mutator(
                    run, a, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec,
                )?),
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::NullTest {
                    arg,
                    nulltesttype: nt.nulltesttype,
                    argisrow: nt.argisrow,
                    location: nt.location,
                },
            )
        }
        NodeTag::T_BooleanTest => {
            let bt = node.as_boolean_test().unwrap();
            let arg = match bt.arg {
                None => None,
                Some(a) => Some(fix_join_expr_mutator(
                    run, a, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec,
                )?),
            };
            Node::mk(
                mcx,
                types_nodes::BooleanTest {
                    arg,
                    booltesttype: bt.booltesttype,
                    location: bt.location,
                },
            )
        }
        NodeTag::T_DistinctExpr => {
            let d = node.as_distinct_expr().unwrap();
            let opfuncid =
                if d.opfuncid != 0 { d.opfuncid } else { lsyscache::get_opcode(d.opno)? };
            record_plan_function_dependency(run, opfuncid)?;
            let mut args = NodeList::nil();
            for arg in &d.args {
                args.lappend(
                    mcx,
                    fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::DistinctExpr {
                    opno: d.opno,
                    opfuncid,
                    opresulttype: d.opresulttype,
                    opretset: d.opretset,
                    opcollid: d.opcollid,
                    inputcollid: d.inputcollid,
                    args,
                    location: d.location,
                },
            )
        }
        NodeTag::T_NullIfExpr => {
            let d = node.as_null_if_expr().unwrap();
            let opfuncid =
                if d.opfuncid != 0 { d.opfuncid } else { lsyscache::get_opcode(d.opno)? };
            record_plan_function_dependency(run, opfuncid)?;
            let mut args = NodeList::nil();
            for arg in &d.args {
                args.lappend(mcx, fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::NullIfExpr {
                    opno: d.opno,
                    opfuncid,
                    opresulttype: d.opresulttype,
                    opretset: d.opretset,
                    opcollid: d.opcollid,
                    inputcollid: d.inputcollid,
                    args,
                    location: d.location,
                },
            )
        }
        NodeTag::T_Param => fix_param_node(run, node),
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            let testexpr = match sp.testexpr {
                None => None,
                Some(te) => Some(fix_join_expr_mutator(
                    run, te, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec,
                )?),
            };
            let mut args = NodeList::nil();
            for arg in &sp.args {
                args.lappend(
                    mcx,
                    fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::SubPlan {
                    subLinkType: sp.subLinkType,
                    testexpr,
                    paramIds: sp.paramIds.clone_in(mcx)?,
                    plan_id: sp.plan_id,
                    plan_name: sp.plan_name,
                    firstColType: sp.firstColType,
                    firstColTypmod: sp.firstColTypmod,
                    firstColCollation: sp.firstColCollation,
                    useHashTable: sp.useHashTable,
                    unknownEqFalse: sp.unknownEqFalse,
                    parallel_safe: sp.parallel_safe,
                    setParam: sp.setParam.clone_in(mcx)?,
                    parParam: sp.parParam.clone_in(mcx)?,
                    args,
                    startup_cost: sp.startup_cost,
                    per_call_cost: sp.per_call_cost,
                },
            )
        }
        NodeTag::T_AlternativeSubPlan => {
            let chosen =
                fix_alternative_subplan(run, node.as_alternative_sub_plan().unwrap(), num_exec);
            fix_join_expr_mutator(run, chosen, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)
        }
        NodeTag::T_SubscriptingRef => {
            let sr = node.as_subscripting_ref().unwrap();
            let mut upper = types_nodes::OptNodeList::nil();
            for e in &sr.refupperindexpr {
                let e = match e {
                    Some(e) => Some(fix_join_expr_mutator(
                        run, e, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel,
                        num_exec,
                    )?),
                    None => None,
                };
                upper.lappend(mcx, e)?;
            }
            let mut lower = types_nodes::OptNodeList::nil();
            for e in &sr.reflowerindexpr {
                let e = match e {
                    Some(e) => Some(fix_join_expr_mutator(
                        run, e, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel,
                        num_exec,
                    )?),
                    None => None,
                };
                lower.lappend(mcx, e)?;
            }
            let refexpr = match sr.refexpr {
                Some(e) => Some(fix_join_expr_mutator(
                    run, e, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec,
                )?),
                None => None,
            };
            let refassgnexpr = match sr.refassgnexpr {
                Some(e) => Some(fix_join_expr_mutator(
                    run, e, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec,
                )?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::SubscriptingRef {
                    refcontainertype: sr.refcontainertype,
                    refelemtype: sr.refelemtype,
                    refrestype: sr.refrestype,
                    reftypmod: sr.reftypmod,
                    refcollid: sr.refcollid,
                    refupperindexpr: upper,
                    reflowerindexpr: lower,
                    refexpr,
                    refassgnexpr,
                },
            )
        }
        NodeTag::T_RowExpr => {
            let r = node.as_row_expr().unwrap();
            let mut args = NodeList::nil();
            for a in &r.args {
                args.lappend(
                    mcx,
                    fix_join_expr_mutator(run, a, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::RowExpr {
                    args,
                    row_typeid: r.row_typeid,
                    row_format: r.row_format,
                    colnames: r.colnames.clone_in(mcx)?,
                    location: r.location,
                },
            )
        }
        NodeTag::T_CaseTestExpr => Ok(node),
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            let arg = match c.arg {
                Some(a) => Some(fix_join_expr_mutator(
                    run, a, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec,
                )?),
                None => None,
            };
            let mut args = NodeList::nil();
            for w in &c.args {
                let cw = w.as_case_when().expect("CaseWhen");
                let expr = fix_join_expr_mutator(
                    run,
                    cw.expr.expect("CaseWhen.expr"),
                    outer_tlist,
                    inner_tlist,
                    rtoffset,
                    nrm_match,
                    acceptable_rel,
                    num_exec,
                )?;
                let result = fix_join_expr_mutator(
                    run,
                    cw.result.expect("CaseWhen.result"),
                    outer_tlist,
                    inner_tlist,
                    rtoffset,
                    nrm_match,
                    acceptable_rel,
                    num_exec,
                )?;
                args.lappend(
                    mcx,
                    Node::mk(
                        mcx,
                        types_nodes::primnodes::CaseWhen {
                            expr: Some(expr),
                            result: Some(result),
                            location: cw.location,
                        },
                    )?,
                )?;
            }
            let defresult = match c.defresult {
                Some(d) => Some(fix_join_expr_mutator(
                    run, d, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec,
                )?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::CaseExpr {
                    casetype: c.casetype,
                    casecollid: c.casecollid,
                    arg,
                    args,
                    defresult,
                    location: c.location,
                },
            )
        }
        NodeTag::T_CoerceViaIO => {
            let cv = node.as_coerce_via_io().unwrap();
            let arg = fix_join_expr_mutator(
                run, cv.arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec,
            )?;
            Node::mk(
                mcx,
                types_nodes::primnodes::CoerceViaIO {
                    arg,
                    resulttype: cv.resulttype,
                    resultcollid: cv.resultcollid,
                    coerceformat: cv.coerceformat,
                    location: cv.location,
                },
            )
        }
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            let arg = fix_join_expr_mutator(
                run, a.arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec,
            )?;
            let elemexpr = match a.elemexpr {
                Some(e) => Some(fix_join_expr_mutator(
                    run, e, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec,
                )?),
                None => None,
            };
            Node::mk(mcx, types_nodes::primnodes::ArrayCoerceExpr { arg, elemexpr, ..*a })
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let c = node.as_convert_rowtype_expr().unwrap();
            let arg = fix_join_expr_mutator(
                run, c.arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec,
            )?;
            Node::mk(mcx, types_nodes::primnodes::ConvertRowtypeExpr { arg, ..*c })
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let sa = node.as_scalar_array_op_expr().unwrap();
            // fix_expr_common: set_sa_opfuncid + dependency; eval_const_
            // expressions resolved opfuncid on every live path here.
            debug_assert!(sa.opfuncid != 0, "fix_join_expr_mutator: unresolved sa opfuncid");
            record_plan_function_dependency(run, sa.opfuncid)?;
            if sa.hashfuncid != 0 {
                record_plan_function_dependency(run, sa.hashfuncid)?;
            }
            if sa.negfuncid != 0 {
                record_plan_function_dependency(run, sa.negfuncid)?;
            }
            let mut args = NodeList::nil();
            for arg in &sa.args {
                args.lappend(
                    mcx,
                    fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::ScalarArrayOpExpr {
                    opno: sa.opno,
                    opfuncid: sa.opfuncid,
                    hashfuncid: sa.hashfuncid,
                    negfuncid: sa.negfuncid,
                    useOr: sa.useOr,
                    inputcollid: sa.inputcollid,
                    args,
                    location: sa.location,
                },
            )
        }
        NodeTag::T_CoalesceExpr => {
            let co = node.as_coalesce_expr().unwrap();
            let mut args = NodeList::nil();
            for arg in &co.args {
                args.lappend(
                    mcx,
                    fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::CoalesceExpr {
                    coalescetype: co.coalescetype,
                    coalescecollid: co.coalescecollid,
                    args,
                    location: co.location,
                },
            )
        }
        NodeTag::T_ArrayExpr => {
            let a = node.as_array_expr().unwrap();
            let mut elements = NodeList::nil();
            for e in &a.elements {
                elements.lappend(
                    mcx,
                    fix_join_expr_mutator(run, e, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::ArrayExpr {
                    array_typeid: a.array_typeid,
                    array_collid: a.array_collid,
                    element_typeid: a.element_typeid,
                    elements,
                    multidims: a.multidims,
                    list_start: a.list_start,
                    list_end: a.list_end,
                    location: a.location,
                },
            )
        }
        NodeTag::T_MinMaxExpr => {
            let mm = node.as_min_max_expr().unwrap();
            let mut args = NodeList::nil();
            for a in &mm.args {
                args.lappend(
                    mcx,
                    fix_join_expr_mutator(run, a, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?,
                )?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::MinMaxExpr {
                    minmaxtype: mm.minmaxtype,
                    minmaxcollid: mm.minmaxcollid,
                    inputcollid: mm.inputcollid,
                    op: mm.op,
                    args,
                    location: mm.location,
                },
            )
        }

        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            let raw = fix_join_expr_mutator(run, j.raw_expr.expect("raw_expr"), outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?;
            let formatted = fix_join_expr_mutator(run, j.formatted_expr.expect("formatted_expr"), outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonValueExpr {
                    raw_expr: Some(raw),
                    formatted_expr: Some(formatted),
                    format: j.format,
                },
            )
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            let mut args = NodeList::nil();
            for a in &c.args {
                args.lappend(mcx, fix_join_expr_mutator(run, a, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?)?;
            }
            let func = match c.func {
                Some(f) => Some(fix_join_expr_mutator(run, f, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?),
                None => None,
            };
            let coercion = match c.coercion {
                Some(co) => Some(fix_join_expr_mutator(run, co, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonConstructorExpr {
                    r#type: c.r#type,
                    args,
                    func,
                    coercion,
                    returning: c.returning,
                    absent_on_null: c.absent_on_null,
                    unique: c.unique,
                    location: c.location,
                },
            )
        }
        NodeTag::T_JsonIsPredicate => {
            let p = node.as_json_is_predicate().unwrap();
            let expr = fix_join_expr_mutator(run, p.expr.expect("expr"), outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?;
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonIsPredicate {
                    expr: Some(expr),
                    format: p.format,
                    item_type: p.item_type,
                    unique_keys: p.unique_keys,
                    location: p.location,
                },
            )
        }
        NodeTag::T_JsonBehavior => {
            let b = node.as_json_behavior().unwrap();
            let expr = match b.expr {
                Some(e) => Some(fix_join_expr_mutator(run, e, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonBehavior {
                    btype: b.btype,
                    expr,
                    coerce: b.coerce,
                    location: b.location,
                },
            )
        }
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            let formatted = fix_join_expr_mutator(run, j.formatted_expr.expect("formatted_expr"), outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?;
            let path_spec = fix_join_expr_mutator(run, j.path_spec.expect("path_spec"), outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?;
            let mut passing_values = NodeList::nil();
            for v in &j.passing_values {
                passing_values.lappend(mcx, fix_join_expr_mutator(run, v, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?)?;
            }
            let on_empty = match j.on_empty {
                Some(e) => Some(fix_join_expr_mutator(run, e, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?),
                None => None,
            };
            let on_error = match j.on_error {
                Some(e) => Some(fix_join_expr_mutator(run, e, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?),
                None => None,
            };
            Node::mk(
                mcx,
                types_nodes::primnodes::JsonExpr {
                    op: j.op,
                    column_name: j.column_name,
                    formatted_expr: Some(formatted),
                    format: j.format,
                    path_spec: Some(path_spec),
                    returning: j.returning,
                    passing_names: j.passing_names.clone_in(mcx)?,
                    passing_values,
                    on_empty,
                    on_error,
                    use_io_coercion: j.use_io_coercion,
                    use_json_coercion: j.use_json_coercion,
                    wrapper: j.wrapper,
                    omit_quotes: j.omit_quotes,
                    collation: j.collation,
                    location: j.location,
                },
            )
        }
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            let mut largs = NodeList::nil();
            for arg in &rc.largs {
                largs.lappend(mcx, fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?)?;
            }
            let mut rargs = NodeList::nil();
            for arg in &rc.rargs {
                rargs.lappend(mcx, fix_join_expr_mutator(run, arg, outer_tlist, inner_tlist, rtoffset, nrm_match, acceptable_rel, num_exec)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::RowCompareExpr {
                    cmptype: rc.cmptype,
                    opnos: rc.opnos.clone_in(mcx)?,
                    opfamilies: rc.opfamilies.clone_in(mcx)?,
                    inputcollids: rc.inputcollids.clone_in(mcx)?,
                    largs,
                    rargs,
                },
            )
        }
        other => panic!("fix_join_expr_mutator (setrefs.c): {other:?}; M2 expression lane"),
    }
}

// search_indexed_tlist_for_var, join leg: miss returns None so the caller can
// probe the other side. The nullingrels cross-check mirrors C's elog guard;
// the emitted Var keeps the reference Var's nullingrels (C copyVar).
// build_tlist_index_other_vars (setrefs.c): restrict the matchable tlist to
// Vars of rels other than ignore_rel (PlaceHolderVars stay matchable;
// has_non_vars is never set, matching the search functions' Var/PHV-only
// matching here).
fn build_other_vars_tlist<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    tlist: &NodeList<'mcx>,
    ignore_rel: i32,
) -> PgResult<NodeList<'mcx>> {
    let mut out = NodeList::nil();
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let keep = match tle.expr.node_tag() {
            NodeTag::T_Var => tle.expr.as_var().unwrap().varno != ignore_rel,
            NodeTag::T_PlaceHolderVar => true,
            _ => false,
        };
        if keep {
            out.lappend(mcx, tle_node)?;
        }
    }
    Ok(out)
}

fn search_join_tlist_for_var<'mcx>(
    run: &mut PlannerRun<'mcx>,
    var: &types_nodes::primnodes::Var<'mcx>,
    tlist: &NodeList<'mcx>,
    newvarno: i32,
    rtoffset: i32,
    nrm_match: NrmMatch,
) -> PgResult<Option<Node<'mcx>>> {
    debug_assert!(var.varlevelsup == 0);
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let Some(sub) = tle.expr.as_var() else { continue };
        if sub.varno == var.varno && sub.varattno == var.varattno {
            assert!(
                var.varattno <= 0
                    || match nrm_match {
                        NrmMatch::Subset => var.varnullingrels.is_subset(&sub.varnullingrels),
                        NrmMatch::Superset => sub.varnullingrels.is_subset(&var.varnullingrels),
                        NrmMatch::Equal => sub.varnullingrels.equal(&var.varnullingrels),
                    },
                "wrong varnullingrels {:?} (expected {:?}, nrm {:?}) for Var {}/{}",
                var.varnullingrels,
                sub.varnullingrels,
                nrm_match,
                var.varno,
                var.varattno
            );
            let mut newvar = types_nodes::primnodes::Var {
                varno: newvarno,
                varattno: tle.resno,
                vartype: var.vartype,
                vartypmod: var.vartypmod,
                varcollid: var.varcollid,
                varnullingrels: var.varnullingrels.clone_in(run.mcx)?,
                varlevelsup: 0,
                varreturningtype: var.varreturningtype,
                varnosyn: var.varnosyn,
                varattnosyn: var.varattnosyn,
                location: var.location,
            };
            if newvar.varnosyn > 0 {
                newvar.varnosyn = newvar.varnosyn.wrapping_add(rtoffset as u32);
            }
            return Ok(Some(Node::mk(run.mcx, newvar)?));
        }
    }
    Ok(None)
}

// search_indexed_tlist_for_phv (setrefs.c): phid-only match, phnullingrels
// cross-checked per NrmMatch; the emitted Var is makeVarFromTargetEntry with
// varnosyn/varattnosyn zeroed (never a plain Var).
fn search_tlist_for_phv<'mcx>(
    run: &mut PlannerRun<'mcx>,
    phv: &types_nodes::primnodes::PlaceHolderVar<'mcx>,
    tlist: &NodeList<'mcx>,
    newvarno: i32,
    nrm_match: NrmMatch,
) -> PgResult<Option<Node<'mcx>>> {
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        let Some(sub) = tle.expr.as_place_holder_var() else { continue };
        if sub.phid != phv.phid {
            continue;
        }
        assert!(
            match nrm_match {
                NrmMatch::Subset => phv.phnullingrels.is_subset(&sub.phnullingrels),
                NrmMatch::Superset => sub.phnullingrels.is_subset(&phv.phnullingrels),
                NrmMatch::Equal => sub.phnullingrels.equal(&phv.phnullingrels),
            },
            "wrong phnullingrels {:?} (expected {:?}, nrm {:?}) for PlaceHolderVar {}",
            phv.phnullingrels,
            sub.phnullingrels,
            nrm_match,
            phv.phid
        );
        let (vartype, vartypmod) = crate::costsize::expr_type_typmod(tle.expr);
        return Ok(Some(Node::mk(
            run.mcx,
            types_nodes::primnodes::Var {
                varno: newvarno,
                varattno: tle.resno,
                vartype,
                vartypmod,
                varcollid: exprs_collation(tle.expr),
                varnullingrels: types_nodes::bitmapset::Bitmapset::empty(),
                varlevelsup: 0,
                varreturningtype: types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
                varnosyn: 0,
                varattnosyn: 0,
                location: -1,
            },
        )?));
    }
    Ok(None)
}

// set_append_references (setrefs.c); part_prune_index and parallel-aware legs
// have no lane.
fn set_append_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    let aplan = plan.as_append().expect("Append node");
    debug_assert!(aplan.plan.qual.is_nil());

    let mut new_children = NodeList::nil();
    for child in &aplan.appendplans {
        new_children.lappend(run.mcx, set_plan_refs(run, child, rtoffset)?)?;
    }
    let single = (new_children.len() == 1).then(|| new_children.nth(0));
    // SAFETY: exclusive plan-tree ownership (prologue note in set_plan_refs).
    unsafe {
        plan.with_mut::<types_nodes::plannodes::Append, _>(|p| p.appendplans = new_children)
    }
    .expect("Append node");

    if let Some(child) = single {
        if child.as_plan().expect("plan node").parallel_aware
            == plan.as_plan().unwrap().parallel_aware
        {
            return clean_up_removed_plan_level(run, plan, child);
        }
    }

    set_dummy_tlist_references(run, plan, rtoffset)?;
    if rtoffset != 0 {
        let old = &plan.as_append().unwrap().apprelids;
        let mut shifted = types_nodes::bitmapset::Bitmapset::empty();
        let mut m = old.next_member(-1);
        while m >= 0 {
            shifted.add_member(run.mcx, m + rtoffset)?;
            m = old.next_member(m);
        }
        // SAFETY: exclusive plan-tree ownership (prologue note).
        unsafe {
            plan.with_mut::<types_nodes::plannodes::Append, _>(|p| p.apprelids = shifted)
        }
        .expect("Append node");
    }
    if plan.as_append().unwrap().part_prune_index >= 0 {
        let old_index = plan.as_append().unwrap().part_prune_index;
        let new_index = register_partpruneinfo(run, old_index, rtoffset)?;
        // SAFETY: exclusive plan-tree ownership (prologue note).
        unsafe {
            plan.with_mut::<types_nodes::plannodes::Append, _>(|p| p.part_prune_index = new_index)
        }
        .expect("Append node");
    }

    debug_assert!(plan.as_plan().unwrap().lefttree.is_none());
    debug_assert!(plan.as_plan().unwrap().righttree.is_none());
    Ok(plan)
}

// set_mergeappend_references (setrefs.c); a lone child whose parallel
// awareness matches elides the MergeAppend entirely.
fn set_mergeappend_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    let mplan = plan.as_merge_append().expect("MergeAppend node");
    debug_assert!(mplan.plan.qual.is_nil());

    let mut new_children = NodeList::nil();
    for child in &mplan.mergeplans {
        new_children.lappend(run.mcx, set_plan_refs(run, child, rtoffset)?)?;
    }
    let single = (new_children.len() == 1).then(|| new_children.nth(0));
    // SAFETY: exclusive plan-tree ownership (prologue note in set_plan_refs).
    unsafe {
        plan.with_mut::<types_nodes::plannodes::MergeAppend, _>(|p| p.mergeplans = new_children)
    }
    .expect("MergeAppend node");

    if let Some(child) = single {
        if child.as_plan().expect("plan node").parallel_aware
            == plan.as_plan().unwrap().parallel_aware
        {
            return clean_up_removed_plan_level(run, plan, child);
        }
    }

    set_dummy_tlist_references(run, plan, rtoffset)?;
    if rtoffset != 0 {
        let old = &plan.as_merge_append().unwrap().apprelids;
        let mut shifted = types_nodes::bitmapset::Bitmapset::empty();
        let mut m = old.next_member(-1);
        while m >= 0 {
            shifted.add_member(run.mcx, m + rtoffset)?;
            m = old.next_member(m);
        }
        // SAFETY: exclusive plan-tree ownership (prologue note).
        unsafe {
            plan.with_mut::<types_nodes::plannodes::MergeAppend, _>(|p| p.apprelids = shifted)
        }
        .expect("MergeAppend node");
    }
    if plan.as_merge_append().unwrap().part_prune_index >= 0 {
        let old_index = plan.as_merge_append().unwrap().part_prune_index;
        let new_index = register_partpruneinfo(run, old_index, rtoffset)?;
        // SAFETY: exclusive plan-tree ownership (prologue note).
        unsafe {
            plan.with_mut::<types_nodes::plannodes::MergeAppend, _>(|p| {
                p.part_prune_index = new_index
            })
        }
        .expect("MergeAppend node");
    }

    debug_assert!(plan.as_plan().unwrap().lefttree.is_none());
    debug_assert!(plan.as_plan().unwrap().righttree.is_none());
    Ok(plan)
}

// register_partpruneinfo (setrefs.c): move the pending pruneinfo into
// glob.part_prune_infos, offsetting RT indexes and recording the startup-
// prunable leaf RTIs.
fn register_partpruneinfo<'mcx>(
    run: &mut PlannerRun<'mcx>,
    part_prune_index: i32,
    rtoffset: i32,
) -> PgResult<i32> {
    let mcx = run.mcx;
    assert!(
        part_prune_index >= 0
            && (part_prune_index as usize) < run.pending_part_prune_infos.len()
    );
    let pnode = run.pending_part_prune_infos.nth(part_prune_index as usize);
    let pinfo = pnode.as_partition_prune_info().expect("PartitionPruneInfo node");

    if rtoffset != 0 {
        let mut shifted = types_nodes::bitmapset::Bitmapset::empty();
        let mut m = pinfo.relids.next_member(-1);
        while m >= 0 {
            shifted.add_member(mcx, m + rtoffset)?;
            m = pinfo.relids.next_member(m);
        }
        // SAFETY: exclusive ownership of the pending pruneinfo tree.
        unsafe {
            pnode.with_mut::<types_nodes::plannodes::PartitionPruneInfo, _>(|p| {
                p.relids = shifted;
            })
        }
        .expect("PartitionPruneInfo node");
    }

    for l in pinfo.prune_infos.iter() {
        let prune_infos = l.as_list().expect("prune_infos cell is a List");
        for prel_node in prune_infos.iter() {
            let prel = prel_node
                .as_partitioned_rel_prune_info()
                .expect("PartitionedRelPruneInfo node");
            fix_pruning_steps(run, &prel.initial_pruning_steps, rtoffset)?;
            fix_pruning_steps(run, &prel.exec_pruning_steps, rtoffset)?;
            let has_initial = !prel.initial_pruning_steps.is_nil();
            let mut prunable: Vec<i32> = Vec::new();
            for i in 0..prel.nparts as usize {
                if prel.leafpart_rti_map[i] != 0 && has_initial {
                    prunable.push(prel.leafpart_rti_map[i] + rtoffset);
                }
            }
            // SAFETY: exclusive ownership of the pending pruneinfo tree.
            unsafe {
                prel_node.with_mut::<types_nodes::plannodes::PartitionedRelPruneInfo, _>(|p| {
                    p.rtindex += rtoffset as u32;
                    if rtoffset != 0 {
                        let mut map = mcx::vec_from_elem_in(mcx, 0i32, p.nparts as usize);
                        for i in 0..p.nparts as usize {
                            map[i] = if p.leafpart_rti_map[i] != 0 {
                                p.leafpart_rti_map[i] + rtoffset
                            } else {
                                0
                            };
                        }
                        p.leafpart_rti_map = map.leak();
                    }
                })
            }
            .expect("PartitionedRelPruneInfo node");
            for rti in prunable {
                run.glob.prunable_relids.add_member(mcx, rti)?;
            }
        }
    }

    run.glob.part_prune_infos.lappend(mcx, pnode)?;
    Ok(run.glob.part_prune_infos.len() as i32 - 1)
}

// fix_scan_list over each PartitionPruneStepOp's exprs (the step nodes
// themselves are not expression nodes, so the walker sees only the exprs).
fn fix_pruning_steps<'mcx>(
    run: &mut PlannerRun<'mcx>,
    steps: &NodeList<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    for step in steps.iter() {
        let Some(op) = step.as_partition_prune_step_op() else { continue };
        if let Some(fixed) = fix_scan_list(run, &op.exprs, rtoffset, 1.0)? {
            // SAFETY: exclusive ownership of the pending pruneinfo tree.
            unsafe {
                step.with_mut::<types_nodes::plannodes::PartitionPruneStepOp, _>(|s| {
                    s.exprs = fixed;
                })
            }
            .expect("PartitionPruneStepOp node");
        }
    }
    Ok(())
}

// set_subqueryscan_references (setrefs.c): the subplan is processed under the
// rel's subroot; trivial scans are elided.
fn set_subqueryscan_references<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    let s = plan.as_subquery_scan().expect("SubqueryScan node");
    let rel = crate::relnode::find_base_rel(&run.root, s.scan.scanrelid as i32);
    let idx = run.root.rel(rel).subroot_idx.expect("subquery rel has a subroot");
    let subplan = s.subplan.expect("SubqueryScan has a subplan");

    run.swap_with_rel_subroot(idx);
    let new_subplan = set_plan_references(run, subplan);
    run.swap_with_rel_subroot(idx);
    let new_subplan = new_subplan?;
    // SAFETY: exclusive plan-tree ownership (prologue note).
    unsafe {
        plan.with_mut::<types_nodes::plannodes::SubqueryScan, _>(|p| {
            p.subplan = Some(new_subplan)
        })
    }
    .expect("SubqueryScan node");

    if trivial_subqueryscan(plan) {
        return clean_up_removed_plan_level(run, plan, new_subplan);
    }

    let s = plan.as_subquery_scan().unwrap();
    let tl = fix_scan_list(run, &s.scan.plan.targetlist, rtoffset, s.scan.plan.plan_rows)?;
    let qual = fix_scan_list(run, &s.scan.plan.qual, rtoffset, 2.0 * s.scan.plan.plan_rows)?;
    // SAFETY: exclusive plan-tree ownership (prologue note).
    unsafe {
        plan.with_mut::<types_nodes::plannodes::SubqueryScan, _>(|p| {
            if let Some(tl) = tl {
                p.scan.plan.targetlist = tl;
            }
            if let Some(q) = qual {
                p.scan.plan.qual = q;
            }
            p.scan.scanrelid += rtoffset as u32;
        })
    }
    .expect("SubqueryScan node");
    Ok(plan)
}

const SUBQUERY_SCAN_TRIVIAL: u32 = 1;
const SUBQUERY_SCAN_NONTRIVIAL: u32 = 2;

// trivial_subqueryscan (setrefs.c), scanstatus memo included.
fn trivial_subqueryscan(plan: Node<'_>) -> bool {
    let s = plan.as_subquery_scan().expect("SubqueryScan node");
    match s.scanstatus {
        SUBQUERY_SCAN_TRIVIAL => return true,
        SUBQUERY_SCAN_NONTRIVIAL => return false,
        _ => {}
    }
    let set_status = |v: u32| {
        // SAFETY: exclusive plan-tree ownership (prologue note); scanstatus is
        // a memo the executor also reads.
        unsafe {
            plan.with_mut::<types_nodes::plannodes::SubqueryScan, _>(|p| p.scanstatus = v)
        }
        .expect("SubqueryScan node");
    };
    set_status(SUBQUERY_SCAN_NONTRIVIAL);

    if !s.scan.plan.qual.is_nil() {
        return false;
    }
    let sub_tlist = &s.subplan.expect("SubqueryScan has a subplan").as_plan().unwrap().targetlist;
    if s.scan.plan.targetlist.len() != sub_tlist.len() {
        return false;
    }
    for (attrno, (p_node, c_node)) in
        s.scan.plan.targetlist.iter().zip(sub_tlist.iter()).enumerate()
    {
        let ptle = p_node.as_target_entry().expect("tlist cell");
        let ctle = c_node.as_target_entry().expect("tlist cell");
        if ptle.resjunk != ctle.resjunk {
            return false;
        }
        if let Some(var) = ptle.expr.as_var() {
            debug_assert!(var.varno as u32 == s.scan.scanrelid && var.varlevelsup == 0);
            if var.varattno != (attrno + 1) as i16 {
                return false;
            }
        } else if ptle.expr.node_tag() == NodeTag::T_Const {
            if !types_nodes::equal(ptle.expr, ctle.expr) {
                return false;
            }
        } else {
            return false;
        }
    }
    set_status(SUBQUERY_SCAN_TRIVIAL);
    true
}

// clean_up_removed_plan_level (setrefs.c).
fn clean_up_removed_plan_level<'mcx>(
    run: &PlannerRun<'mcx>,
    parent: Node<'mcx>,
    child: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let pplan = parent.as_plan().expect("plan node");
    if !pplan.initPlan.is_nil() {
        // SS_compute_initplan_cost: move the initplans and their run costs.
        let mut initplan_cost = 0.0;
        let mut unsafe_initplans = false;
        for sp_node in &pplan.initPlan {
            let sp = sp_node.as_sub_plan().expect("initPlan holds SubPlan nodes");
            initplan_cost += sp.startup_cost + sp.per_call_cost;
            if !sp.parallel_safe {
                unsafe_initplans = true;
            }
        }
        let mcx = run.mcx;
        let mut merged = pplan.initPlan.clone_in(mcx)?;
        merged.concat(mcx, &child.as_plan().unwrap().initPlan)?;
        // SAFETY: exclusive plan-tree ownership (prologue note).
        unsafe {
            child.with_plan_mut(|c| {
                c.startup_cost += initplan_cost;
                c.total_cost += initplan_cost;
                if unsafe_initplans {
                    c.parallel_safe = false;
                }
                c.initPlan = merged;
            })
        }
        .expect("plan node");
    }
    crate::createplan::apply_tlist_labeling(child, &pplan.targetlist);
    Ok(child)
}

/// find_minmax_agg_replacement_param (setrefs.c); the returned NodeId is the
/// InitPlan output Param in the current root's arena.
pub(crate) fn find_minmax_agg_replacement_param<'mcx>(
    root: &types_pathnodes::PlannerInfo<'mcx>,
    node: Node<'mcx>,
) -> Option<types_pathnodes::NodeId> {
    let aggref = node.as_aggref()?;
    if root.minmax_aggs.is_empty() || aggref.args.len() != 1 {
        return None;
    }
    let cur_target = aggref
        .args
        .nth(0)
        .as_target_entry()
        .expect("Aggref.args holds TargetEntries")
        .expr;
    for i in 0..root.minmax_aggs.len() {
        let mm = *root.minmax_agg_info(root.minmax_aggs[i]);
        if mm.aggfnoid == aggref.aggfnoid
            && types_nodes::equal(*root.expr_node(mm.target), cur_target)
        {
            return Some(mm.param);
        }
    }
    None
}
