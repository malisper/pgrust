use mcx::{Mcx, PgVec};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::{IntList, NodeList, OidList};
use types_nodes::parsenodes::{Query, RangeTblEntry};
use types_pathnodes::{
    NodeId, PathTarget, PlannerInfo, PtId, QueryId, RangeTblEntryId, RelId,
};

// PlannerGlobal, types_nodes-payload form. C shares one glob by pointer across
// an invocation's sub-Query levels; PlannerRun is that invocation, so
// root.glob stays None. boundParams unthreaded (fold.rs top note).
pub struct Glob<'mcx> {
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
    pub prunable_relids: Bitmapset<'mcx>,
}

impl Glob<'_> {
    pub fn new() -> Self {
        Glob {
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
            prunable_relids: Bitmapset::empty(),
        }
    }
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
}

impl<'mcx> PlannerRun<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        PlannerRun {
            mcx,
            root: PlannerInfo::new(mcx),
            glob: Glob::new(),
            queries: PgVec::new_in(mcx),
            processed_tlist: None,
            assess_parallel: false,
        }
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
