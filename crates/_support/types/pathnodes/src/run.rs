use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::{IntList, NodeList, OidList};
use types_nodes::parsenodes::{Query, RangeTblEntry};
use types_nodes::plannodes::PlanRowMark;
use crate::{
    MinMaxAggInfo, NodeId, Path, PathTarget, PlanRowMarkId, PlannerInfo, PtId, QueryId, RangeTblEntryId, RelId,
};

// PlannerGlobal, types_nodes-payload form. C shares one glob by pointer across
// an invocation's sub-Query levels; PlannerRun is that invocation, so
// root.glob stays None.
pub struct Glob<'mcx> {
    pub bound_params: types_portal::ParamListHandle,
    pub parallel_mode_ok: bool,
    pub parallel_mode_needed: bool,
    pub max_parallel_hazard: i8,
    pub transient_plan: bool,
    pub depends_on_role: bool,
    pub last_ph_id: u32,
    pub last_row_mark_id: u32,
    pub last_plan_node_id: i32,
    pub finalrtable: NodeList<'mcx>,
    pub finalrteperminfos: NodeList<'mcx>,
    pub finalrowmarks: NodeList<'mcx>,
    pub subplans: NodeList<'mcx>,
    pub rewind_plan_ids: Bitmapset<'mcx>,
    pub result_relations: IntList<'mcx>,
    pub append_relations: NodeList<'mcx>,
    pub part_prune_infos: NodeList<'mcx>,
    pub relation_oids: OidList<'mcx>,
    pub inval_items: NodeList<'mcx>,
    pub param_exec_types: OidList<'mcx>,
    pub all_relids: Bitmapset<'mcx>,
    pub has_alternative_subplans: bool,
    pub prunable_relids: Bitmapset<'mcx>,
}

impl Glob<'_> {
    pub fn new() -> Self {
        Glob {
            bound_params: types_portal::ParamListHandle::NULL,
            parallel_mode_ok: false,
            parallel_mode_needed: false,
            max_parallel_hazard: 0,
            transient_plan: false,
            depends_on_role: false,
            last_ph_id: 0,
            last_row_mark_id: 0,
            last_plan_node_id: 0,
            finalrtable: NodeList::nil(),
            finalrteperminfos: NodeList::nil(),
            finalrowmarks: NodeList::nil(),
            subplans: NodeList::nil(),
            rewind_plan_ids: Bitmapset::empty(),
            result_relations: IntList::nil(),
            append_relations: NodeList::nil(),
            part_prune_infos: NodeList::nil(),
            relation_oids: OidList::nil(),
            inval_items: NodeList::nil(),
            param_exec_types: OidList::nil(),
            all_relids: Bitmapset::empty(),
            has_alternative_subplans: false,
            prunable_relids: Bitmapset::empty(),
        }
    }
}

// One planning level's PlannerInfo plus its tlist share (C keeps these on the
// PlannerInfo itself; glob->subroots is the list of them).
pub struct SubrootState<'mcx> {
    pub root: PlannerInfo<'mcx>,
    pub processed_tlist: Option<&'mcx NodeList<'mcx>>,
}


// Materialized subquery-pathkey inputs (planner pathkeys.rs extracts these
// from a subroot before it is parked; convert_subquery_pathkeys replays them
// against the outer root — C follows pointers across PlannerInfos instead).
pub struct SubPathKeyMember<'mcx> {
    pub expr: types_nodes::Node<'mcx>,
    pub datatype: u32,
}

pub struct SubPathKeyDesc<'mcx> {
    pub has_volatile: bool,
    pub sortref: u32,
    pub members: PgVec<'mcx, SubPathKeyMember<'mcx>>,
    pub opfamilies: PgVec<'mcx, u32>,
    pub collation: u32,
    pub pk_opfamily: u32,
    pub pk_cmptype: i32,
    pub pk_nulls_first: bool,
}

pub struct SubTle<'mcx> {
    pub expr: types_nodes::Node<'mcx>,
    pub resno: i32,
    pub sortgroupref: u32,
}

// Per-CTE-plan pathkey inputs (ctepath->pathkeys + cteplan->targetlist in
// C's set_cte_pathlist), captured by ss_process_ctes before the CTE's root
// is parked.
pub struct CteSubpathInfo<'mcx> {
    pub plan_id: i32,
    pub pathkey_descs: PgVec<'mcx, SubPathKeyDesc<'mcx>>,
    pub tlist: PgVec<'mcx, SubTle<'mcx>>,
}

pub struct PlannerRun<'mcx> {
    pub mcx: Mcx<'mcx>,
    pub root: PlannerInfo<'mcx>,
    pub glob: Glob<'mcx>,
    pub queries: PgVec<'mcx, &'mcx Query<'mcx>>,
    /// C root->processed_tlist shares the (preprocessed) parse targetList
    /// pointer; root.processed_tlist (NodeId form) stays empty.
    pub processed_tlist: Option<&'mcx NodeList<'mcx>>,
    /// standard_planner's cheap parallel-mode tests; the tree scan they gate
    /// runs after the Query is sealed (see standard_planner).
    pub assess_parallel: bool,
    /// C's parent_root chain: levels suspended while a sub-Query is planned.
    pub suspended_roots: PgVec<'mcx, SubrootState<'mcx>>,
    /// C glob->subroots, index-aligned with glob.subplans.
    pub subroots: PgVec<'mcx, SubrootState<'mcx>>,
    /// C rel->subroot for planned RTE_SUBQUERY rels, keyed by
    /// RelOptInfo.subroot_idx (RelOptInfo can't own a PlannerRun-tlist pair).
    pub rel_subroots: PgVec<'mcx, SubrootState<'mcx>>,
    /// planagg subroots (C MinMaxAggInfo.subroot), parked between
    /// build_minmax_path and create_minmaxagg_plan; winners move to subroots.
    pub minmax_subroots: PgVec<'mcx, Option<SubrootState<'mcx>>>,
    /// C qp_extra.activeWindows (WindowClause nodes in execution order).
    pub active_windows: PgVec<'mcx, types_nodes::Node<'mcx>>,
    /// C qp_extra is grouping_planner-local: a nested subquery_planner must
    /// not clobber the suspended level's activeWindows (stacked alongside
    /// suspended_roots).
    pub suspended_active_windows: PgVec<'mcx, PgVec<'mcx, types_nodes::Node<'mcx>>>,
    /// C qp_extra.setop.
    pub qp_setop: Option<&'mcx types_nodes::parsenodes::SetOperationStmt<'mcx>>,
    /// PlanRowMark store: C shares the nodes by pointer between
    /// root->rowMarks and plan nodes; levels' root.rowMarks hold ids here
    /// (all-scalar payload, materialized as nodes at createplan/setrefs).
    pub rowmarks: PgVec<'mcx, PlanRowMark>,
    /// C qp_extra.gset_data / grouping_planner's gset_data local.
    pub gset_data: Option<GroupingSetsData<'mcx>>,
    /// C root->partPruneInfos, run-global (append-only across levels; the
    /// Append's part_prune_index indexes here until setrefs registers the
    /// entry into glob.part_prune_infos).
    pub pending_part_prune_infos: NodeList<'mcx>,
    /// Pathkey inputs per materialized CTE plan (see CteSubpathInfo).
    pub cte_subpath_infos: PgVec<'mcx, CteSubpathInfo<'mcx>>,
    /// rel_subroots index holding the parent level while its child subroot is
    /// swapped in (C: the child's parent_root link, which selfuncs climbs for
    /// uplevel CTE refs); set only around such re-entries.
    pub swapped_parent_subroot: Option<usize>,
    /// M5-3 coverage-keyed Gather suppression (docs/design/m5-planner.md
    /// §2.3): the memoized per-run probe verdict. None = not yet computed.
    /// Computed only for the TOP-LEVEL query level (the bootstrap probe
    /// never suppresses subquery levels) and only under
    /// pgrust.parallel_engine=runtime, so legacy planning never writes it.
    pub m5_suppress_gather: Option<bool>,
    /// NLIDX (GL-NLIDX-2) query-SHAPE memo — the rel-independent half of the
    /// rel-aware nlidx suppression probe (the election check is per-rel and
    /// never memoized here).
    pub m5_nlidx_shape: Option<bool>,
    /// Per-planning-cycle attribute-statistics memo (replanfix2 T1):
    /// (relid, attnum, inh) -> arena-leaked decoded pg_statistic bundle
    /// (None = row absent, negative-memoized). Values are planner-crate
    /// pointers (`selfuncs::get_att_stats` owns both sides), kept OPAQUE
    /// here so this _support crate takes no backend dependency. RefCell
    /// because the selfuncs read paths hold `&PlannerRun` alongside subroot
    /// borrows (planning is single-threaded). COST ONLY: same lookups, same
    /// values, freed with the run's arena — bounded per cycle, never
    /// cross-query state.
    pub att_stats_memo:
        core::cell::RefCell<PgVec<'mcx, ((u32, i16, bool), Option<core::ptr::NonNull<()>>)>>,
    /// Per-planning-cycle syscache memos (syscache-memo lane; the replanfix2
    /// close-out's residual bucket: operator-shape 16x, ACL mask 8x, amop
    /// probes 14x per replan). Same discipline as att_stats_memo: COST ONLY —
    /// identical seam calls and values, arena-scoped, dies with the run,
    /// never cross-query. ONE opaque pointer to a lazily arena-allocated
    /// memo block (`planner::syscache_memo` owns the block type and the
    /// only cast pair; this _support crate takes no backend dependency).
    /// Lazy + single-word so planning cycles that never repeat a catalog
    /// lookup (SELECT 1: the instr guard) pay one None store — the guard
    /// stays EXACTLY flat (a 4-field eager form measured +19 instr/q).
    pub syscache_memos: core::cell::Cell<Option<core::ptr::NonNull<()>>>,
}

// A run is forgotten at the planner boundary (mcx reset reclaims), never
// dropped; the census keeps every member forget-safe.
mcx::forget_safe_struct!(
    Glob<'_> { bound_params, parallel_mode_ok, parallel_mode_needed,
        max_parallel_hazard, transient_plan, depends_on_role, last_ph_id,
        last_row_mark_id, last_plan_node_id, finalrtable, finalrteperminfos,
        finalrowmarks, subplans, rewind_plan_ids, result_relations,
        append_relations, part_prune_infos, relation_oids, inval_items,
        param_exec_types, all_relids, has_alternative_subplans, prunable_relids },
    SubrootState<'_> { root, processed_tlist },
    SubPathKeyMember<'_> { expr, datatype },
    SubPathKeyDesc<'_> { has_volatile, sortref, members, opfamilies,
        collation, pk_opfamily, pk_cmptype, pk_nulls_first },
    SubTle<'_> { expr, resno, sortgroupref },
    CteSubpathInfo<'_> { plan_id, pathkey_descs, tlist },
    PlannerRun<'_> { mcx, root, glob, queries, processed_tlist,
        assess_parallel, suspended_roots, subroots, rel_subroots,
        minmax_subroots, active_windows, suspended_active_windows, qp_setop,
        rowmarks, gset_data, pending_part_prune_infos, cte_subpath_infos,
        swapped_parent_subroot, m5_suppress_gather, m5_nlidx_shape, att_stats_memo,
        syscache_memos },
);

impl<'mcx> PlannerRun<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        PlannerRun {
            mcx,
            root: PlannerInfo::new(mcx),
            glob: Glob::new(),
            queries: PgVec::new_in(mcx),
            processed_tlist: None,
            assess_parallel: false,
            suspended_roots: PgVec::new_in(mcx),
            subroots: PgVec::new_in(mcx),
            rel_subroots: PgVec::new_in(mcx),
            minmax_subroots: PgVec::new_in(mcx),
            active_windows: PgVec::new_in(mcx),
            suspended_active_windows: PgVec::new_in(mcx),
            qp_setop: None,
            rowmarks: PgVec::new_in(mcx),
            gset_data: None,
            pending_part_prune_infos: NodeList::nil(),
            cte_subpath_infos: PgVec::new_in(mcx),
            swapped_parent_subroot: None,
            m5_suppress_gather: None,
            m5_nlidx_shape: None,
            att_stats_memo: core::cell::RefCell::new(PgVec::new_in(mcx)),
            syscache_memos: core::cell::Cell::new(None),
        }
    }

    pub fn add_rowmark(&mut self, rm: PlanRowMark) -> PlanRowMarkId {
        let id = PlanRowMarkId(self.rowmarks.len() as u32);
        self.rowmarks.push(rm);
        id
    }

    pub fn rowmark(&self, id: PlanRowMarkId) -> &PlanRowMark {
        &self.rowmarks[id.0 as usize]
    }

    pub fn rowmark_mut(&mut self, id: PlanRowMarkId) -> &mut PlanRowMark {
        &mut self.rowmarks[id.0 as usize]
    }

    /// Suspend the current level and make a fresh child root current
    /// (C: subquery_planner building a child PlannerInfo with parent_root).
    /// outer_params is materialized here instead of C's end-of-level
    /// SS_identify_outer_params: ancestors are frozen while a child plans on
    /// the uncorrelated lane (correlated plan_params writes are loud panics),
    /// so the push-time snapshot equals C's end-of-level read.
    pub fn push_root(&mut self) -> PgResult<()> {
        let outer = self.identify_outer_params()?;
        let mut new_root = PlannerInfo::new(self.mcx);
        new_root.query_level = self.root.query_level + 1;
        new_root.outer_params = outer;
        let old = core::mem::replace(&mut self.root, new_root);
        let processed_tlist = self.processed_tlist.take();
        self.suspended_roots.push(SubrootState { root: old, processed_tlist });
        let aw = core::mem::replace(&mut self.active_windows, PgVec::new_in(self.mcx));
        self.suspended_active_windows.push(aw);
        Ok(())
    }

    /// push_root for a planagg subroot: C memcpy's the parent PlannerInfo
    /// (make_minmax_subroot carries the copied fields) instead of starting
    /// blank, then plans with query_planner directly.
    pub fn push_minmax_root(&mut self) -> PgResult<()> {
        let outer = self.identify_outer_params()?;
        let mut new_root = self.root.make_minmax_subroot();
        new_root.outer_params = outer;
        let old = core::mem::replace(&mut self.root, new_root);
        let processed_tlist = self.processed_tlist.take();
        self.suspended_roots.push(SubrootState { root: old, processed_tlist });
        let aw = core::mem::replace(&mut self.active_windows, PgVec::new_in(self.mcx));
        self.suspended_active_windows.push(aw);
        Ok(())
    }

    /// Restore the parent level, parking the finished minmax subroot; returns
    /// its index (MinMaxAggInfo.subroot_idx).
    pub fn pop_root_to_minmax_subroot(&mut self) -> usize {
        let parent = self.suspended_roots.pop().expect("pop_root_to_minmax_subroot without push");
        let sub = core::mem::replace(&mut self.root, parent.root);
        let sub_tlist = core::mem::replace(&mut self.processed_tlist, parent.processed_tlist);
        self.active_windows =
            self.suspended_active_windows.pop().expect("pop without active-windows push");
        self.minmax_subroots.push(Some(SubrootState { root: sub, processed_tlist: sub_tlist }));
        self.minmax_subroots.len() - 1
    }

    /// Restore the parent level; the finished child joins glob's subroots.
    /// Returns the subroot index (plan_id - 1).
    /// outer_params is recomputed here: correlated planning added ancestor
    /// plan_params entries after the push-time snapshot (C's end-of-level
    /// SS_identify_outer_params timing).
    pub fn pop_root_to_subroot(&mut self) -> usize {
        let outer = {
            let mut outer: crate::Relids<'mcx> = crate::relids::relids_empty();
            if !self.glob.param_exec_types.is_nil() {
                for i in 0..self.suspended_roots.len() {
                    // SAFETY-free split: scan borrows one suspended root at a time.
                    let root = &self.suspended_roots[i].root;
                    Self::scan_outer_params(self.mcx, &mut outer, root);
                }
            }
            outer
        };
        let parent = self.suspended_roots.pop().expect("pop_root_to_subroot without push");
        let mut sub = core::mem::replace(&mut self.root, parent.root);
        if !self.glob.param_exec_types.is_nil() {
            sub.outer_params = outer;
        }
        let sub_tlist = core::mem::replace(&mut self.processed_tlist, parent.processed_tlist);
        self.active_windows =
            self.suspended_active_windows.pop().expect("pop without active-windows push");
        self.subroots.push(SubrootState { root: sub, processed_tlist: sub_tlist });
        self.subroots.len() - 1
    }

    /// Restore the parent level, detaching the finished child into
    /// rel_subroots (C: rel->subroot). Returns the rel_subroots index.
    /// outer_params is recomputed as in pop_root_to_subroot: a LATERAL
    /// subquery adds ancestor plan_params entries after the push-time
    /// snapshot.
    pub fn pop_root_to_rel_subroot(&mut self) -> usize {
        let outer = {
            let mut outer: crate::Relids<'mcx> = crate::relids::relids_empty();
            if !self.glob.param_exec_types.is_nil() {
                for i in 0..self.suspended_roots.len() {
                    let root = &self.suspended_roots[i].root;
                    Self::scan_outer_params(self.mcx, &mut outer, root);
                }
            }
            outer
        };
        let parent = self.suspended_roots.pop().expect("pop_root_to_rel_subroot without push");
        let mut sub = core::mem::replace(&mut self.root, parent.root);
        if !self.glob.param_exec_types.is_nil() {
            sub.outer_params = outer;
        }
        let sub_tlist = core::mem::replace(&mut self.processed_tlist, parent.processed_tlist);
        self.active_windows =
            self.suspended_active_windows.pop().expect("pop without active-windows push");
        self.rel_subroots.push(SubrootState { root: sub, processed_tlist: sub_tlist });
        self.rel_subroots.len() - 1
    }

    /// Swap the current level with a stored subquery subroot (symmetric; call
    /// twice to enter and leave, as C passes rel->subroot to the callee).
    pub fn swap_with_rel_subroot(&mut self, idx: usize) {
        let s = &mut self.rel_subroots[idx];
        core::mem::swap(&mut self.root, &mut s.root);
        core::mem::swap(&mut self.processed_tlist, &mut s.processed_tlist);
    }

    /// Abandon the current child level without registering it (C's
    /// convert_EXISTS_to_ANY twin whose path failed the hashability check:
    /// the subroot is dropped, never appended to glob->subroots).
    pub fn pop_root_discard(&mut self) {
        let parent = self.suspended_roots.pop().expect("pop_root_discard without push");
        self.root = parent.root;
        self.processed_tlist = parent.processed_tlist;
    }

    fn scan_outer_params(
        mcx: Mcx<'mcx>,
        outer: &mut crate::Relids<'mcx>,
        root: &PlannerInfo<'mcx>,
    ) {
        let mut add = |outer: &mut crate::Relids<'mcx>, id: i32| {
            *outer = crate::relids::relids_union(
                mcx,
                outer,
                &crate::relids::relids_singleton(mcx, id as u32),
            );
        };
        for &pid in root.plan_params.iter() {
            add(outer, root.planner_param_item(pid).paramId);
        }
        for &ipid in root.init_plans.iter() {
            let sp = root
                .expr_node(ipid)
                .as_sub_plan()
                .expect("init_plans holds SubPlan nodes");
            for p in sp.setParam.iter() {
                add(outer, p);
            }
        }
        if root.wt_param_id >= 0 {
            add(outer, root.wt_param_id);
        }
    }

    // SS_identify_outer_params (subselect.c) over the ancestor chain,
    // current root included (it is the child's immediate parent).
    fn identify_outer_params(&mut self) -> PgResult<crate::Relids<'mcx>> {
        if self.glob.param_exec_types.is_nil() {
            return Ok(crate::relids::relids_empty());
        }
        let mcx = self.mcx;
        let mut outer: crate::Relids<'mcx> = crate::relids::relids_empty();
        let scan = |outer: &mut crate::Relids<'mcx>, root: &PlannerInfo<'mcx>| {
            let mut add = |outer: &mut crate::Relids<'mcx>, id: i32| {
                *outer = crate::relids::relids_union(
                    mcx,
                    outer,
                    &crate::relids::relids_singleton(mcx, id as u32),
                );
            };
            for &pid in root.plan_params.iter() {
                add(outer, root.planner_param_item(pid).paramId);
            }
            for &ipid in root.init_plans.iter() {
                let sp = root
                    .expr_node(ipid)
                    .as_sub_plan()
                    .expect("init_plans holds SubPlan nodes");
                for p in sp.setParam.iter() {
                    add(outer, p);
                }
            }
            if root.wt_param_id >= 0 {
                add(outer, root.wt_param_id);
            }
        };
        for s in self.suspended_roots.iter() {
            scan(&mut outer, &s.root);
        }
        scan(&mut outer, &self.root);
        Ok(outer)
    }

    pub fn intern_query(&mut self, parse: &'mcx Query<'mcx>) -> QueryId {
        let id = QueryId(self.queries.len() as u32);
        self.queries.push(parse);
        id
    }

    pub fn parse(&self) -> &'mcx Query<'mcx> {
        self.queries[self.root.parse.0 as usize]
    }

    pub fn rte(&self, varno: usize) -> &'mcx RangeTblEntry<'mcx> {
        match self.root.simple_rte_array[varno] {
            RangeTblEntryId::Parse { query, index } => self.queries[query.0 as usize]
                .rtable
                .nth(index as usize)
                .as_range_tbl_entry()
                .expect("rtable cell is a RangeTblEntry"),
            other => panic!("rte({varno}): unresolvable {other:?}"),
        }
    }

    // The rtable Node cell itself, for the few C sites that scribble on an
    // RTE during planning (e.g. reparameterize_path_by_child's tablesample
    // translation, pathnode.c:4464-4477).
    pub fn rte_cell(&self, varno: usize) -> types_nodes::Node<'mcx> {
        match self.root.simple_rte_array[varno] {
            RangeTblEntryId::Parse { query, index } => {
                self.queries[query.0 as usize].rtable.nth(index as usize)
            }
            other => panic!("rte_cell({varno}): unresolvable {other:?}"),
        }
    }

    pub fn intern_expr(&mut self, node: types_nodes::Node<'mcx>) -> NodeId {
        self.root.alloc_expr_node(node)
    }

    pub fn processed_tlist(&self) -> &'mcx NodeList<'mcx> {
        self.processed_tlist
            .expect("processed_tlist set by preprocess_targetlist")
    }

    pub fn rel_reltarget_id(&self, rel: RelId) -> PtId {
        self.root.rel(rel).pathtarget_id.expect("rel has reltarget")
    }

    pub fn pathtarget(&self, id: PtId) -> &PathTarget<'mcx> {
        self.root.pathtarget(id)
    }
}

// grouping_sets_data (planner.c).
pub struct GroupingSetsData<'mcx> {
    pub rollups: PgVec<'mcx, crate::RollupData<'mcx>>,
    pub any_hashable: bool,
    pub unsortable_refs: PgVec<'mcx, u32>,
    pub unhashable_refs: PgVec<'mcx, u32>,
    pub unsortable_sets: PgVec<'mcx, crate::GroupingSetData<'mcx>>,
    pub hash_sets_idx: PgVec<'mcx, PgVec<'mcx, i32>>,
    pub tleref_to_colnum_map: PgVec<'mcx, i32>,
    pub dNumHashGroups: f64,
}

mcx::forget_safe_struct!(GroupingSetsData<'_> {
    rollups, any_hashable, unsortable_refs, unhashable_refs, unsortable_sets,
    hash_sets_idx, tleref_to_colnum_map, dNumHashGroups,
});


pub fn subroot_path_base<'a, 'mcx>(
    run: &'a PlannerRun<'mcx>,
    mminfo: &MinMaxAggInfo,
) -> &'a Path<'mcx> {
    let idx = mminfo.subroot_idx.expect("minmax agg has a subroot");
    let pid = mminfo.subroot_path.expect("minmax agg has a path");
    run.minmax_subroots[idx]
        .as_ref()
        .expect("minmax subroot still parked")
        .root
        .path(pid)
        .base()
}

pub fn init_dummy_sjinfo<'mcx>(
    run: &PlannerRun<'mcx>,
    left_relids: crate::Relids<'mcx>,
    right_relids: crate::Relids<'mcx>,
) -> crate::SpecialJoinInfo<'mcx> {
    crate::SpecialJoinInfo {
        min_lefthand: crate::relids::relids_copy(run.mcx, &left_relids),
        min_righthand: crate::relids::relids_copy(run.mcx, &right_relids),
        syn_lefthand: left_relids,
        syn_righthand: right_relids,
        jointype: crate::JOIN_INNER,
        ojrelid: 0,
        commute_above_l: crate::relids::relids_empty(),
        commute_above_r: crate::relids::relids_empty(),
        commute_below_l: crate::relids::relids_empty(),
        commute_below_r: crate::relids::relids_empty(),
        lhs_strict: false,
        semi_can_btree: false,
        semi_can_hash: false,
        semi_operators: PgVec::new_in(run.mcx),
        semi_rhs_exprs: PgVec::new_in(run.mcx),
    }
}

// RINFO_IS_PUSHED_DOWN (pathnodes.h).
pub fn rinfo_is_pushed_down(
    run: &PlannerRun<'_>,
    rid: crate::RinfoId,
    joinrelids: &crate::Relids<'_>,
) -> bool {
    let ri = run.root.rinfo(rid);
    ri.is_pushed_down || !crate::relids::relids_is_subset(&ri.required_relids, joinrelids)
}

// get_sortgrouplist_exprs (tlist.c) into estimate_num_groups' input shape.
pub fn sortgrouplist_exprs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    clauses: &[crate::NodeId],
    tlist: &::types_nodes::list::NodeList<'mcx>,
) -> mcx::PgVec<'mcx, (crate::NodeId, ::types_nodes::Node<'mcx>)> {
    let mut exprs = mcx::PgVec::new_in(run.mcx);
    for &gc_id in clauses {
        let sgref = run
            .root
            .expr_node(gc_id)
            .as_sort_group_clause()
            .expect("sortgroup clause cell")
            .tleSortGroupRef;
        let tle_node = tlist
            .iter()
            .find(|n| n.as_target_entry().expect("tlist cell").ressortgroupref == sgref)
            .expect("sortgroupref has a tlist entry");
        let expr = tle_node.as_target_entry().unwrap().expr;
        let id = run.intern_expr(expr);
        exprs.push((id, expr));
    }
    exprs
}
