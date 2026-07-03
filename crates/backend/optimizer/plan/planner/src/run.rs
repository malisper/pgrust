use mcx::{Mcx, PgVec};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::{IntList, NodeList, OidList};
use types_nodes::parsenodes::{Query, RangeTblEntry};
use types_pathnodes::{
    NodeId, PathTarget, PlannerInfo, PtId, QueryId, RangeTblEntryId, RelId,
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
    /// C qp_extra.activeWindows (WindowClause nodes in execution order).
    pub active_windows: PgVec<'mcx, types_nodes::Node<'mcx>>,
    /// C qp_extra.setop.
    pub qp_setop: Option<&'mcx types_nodes::parsenodes::SetOperationStmt<'mcx>>,
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
    PlannerRun<'_> { mcx, root, glob, queries, processed_tlist,
        assess_parallel, suspended_roots, subroots, rel_subroots,
        active_windows, qp_setop },
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
            active_windows: PgVec::new_in(mcx),
            qp_setop: None,
        }
    }

    /// Suspend the current level and make a fresh child root current
    /// (C: subquery_planner building a child PlannerInfo with parent_root).
    /// outer_params is materialized here instead of C's end-of-level
    /// SS_identify_outer_params: ancestors are frozen while a child plans on
    /// the uncorrelated lane (correlated plan_params writes are loud panics),
    /// so the push-time snapshot equals C's end-of-level read.
    pub fn push_root(&mut self) -> types_error::PgResult<()> {
        let outer = self.identify_outer_params()?;
        let mut new_root = PlannerInfo::new(self.mcx);
        new_root.query_level = self.root.query_level + 1;
        new_root.outer_params = outer;
        let old = core::mem::replace(&mut self.root, new_root);
        let processed_tlist = self.processed_tlist.take();
        self.suspended_roots.push(SubrootState { root: old, processed_tlist });
        Ok(())
    }

    /// Restore the parent level; the finished child joins glob's subroots.
    /// Returns the subroot index (plan_id - 1).
    /// outer_params is recomputed here: correlated planning added ancestor
    /// plan_params entries after the push-time snapshot (C's end-of-level
    /// SS_identify_outer_params timing).
    pub fn pop_root_to_subroot(&mut self) -> usize {
        let outer = {
            let mut outer: types_pathnodes::Relids<'mcx> = None;
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
        self.subroots.push(SubrootState { root: sub, processed_tlist: sub_tlist });
        self.subroots.len() - 1
    }

    /// Restore the parent level, detaching the finished child into
    /// rel_subroots (C: rel->subroot). Returns the rel_subroots index.
    pub fn pop_root_to_rel_subroot(&mut self) -> usize {
        let parent = self.suspended_roots.pop().expect("pop_root_to_rel_subroot without push");
        let sub = core::mem::replace(&mut self.root, parent.root);
        let sub_tlist = core::mem::replace(&mut self.processed_tlist, parent.processed_tlist);
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
        outer: &mut types_pathnodes::Relids<'mcx>,
        root: &PlannerInfo<'mcx>,
    ) {
        let mut add = |outer: &mut types_pathnodes::Relids<'mcx>, id: i32| {
            *outer = crate::relnode::relids_union(
                mcx,
                outer,
                &crate::relnode::relids_singleton(mcx, id as u32),
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
    fn identify_outer_params(&mut self) -> types_error::PgResult<types_pathnodes::Relids<'mcx>> {
        if self.glob.param_exec_types.is_nil() {
            return Ok(None);
        }
        let mcx = self.mcx;
        let mut outer: types_pathnodes::Relids<'mcx> = None;
        let scan = |outer: &mut types_pathnodes::Relids<'mcx>, root: &PlannerInfo<'mcx>| {
            let mut add = |outer: &mut types_pathnodes::Relids<'mcx>, id: i32| {
                *outer = crate::relnode::relids_union(
                    mcx,
                    outer,
                    &crate::relnode::relids_singleton(mcx, id as u32),
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
