use mcx::Mcx;
use types_error::PgResult;
use types_nodes::parsenodes::Query;
use types_nodes::plannodes::PlannedStmt;
use types_portal::ParamListHandle;
use types_nodes::Node;
use types_pathnodes::run::PlannerRun;
use types_pathnodes::{IndexOptInfo, JoinType, NodeId, PathId, QualCost, Relids, RinfoId, SpecialJoinInfo};
use types_rel::Relation;

seam_core::seam!(
    pub fn planner<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        parse: Query<'mcx>,
        query_string: &'a str,
        cursor_options: i32,
        bound_params: ParamListHandle,
    ) -> PgResult<PlannedStmt<'mcx>>
);

/// amcostestimate output shape (C fills the out-params of the AM handler).
pub struct AmCostEstimate {
    pub index_startup_cost: f64,
    pub index_total_cost: f64,
    pub index_selectivity: f64,
    pub index_correlation: f64,
    pub index_pages: f64,
}

seam_core::seam!(
    pub fn clauselist_selectivity<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        clauses: &'a [RinfoId],
        varrelid: i32,
        jointype: JoinType,
        sjinfo: Option<&'a SpecialJoinInfo<'mcx>>,
    ) -> PgResult<f64>
);

seam_core::seam!(
    pub fn clause_selectivity<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        rinfo: RinfoId,
        varrelid: i32,
        jointype: JoinType,
        sjinfo: Option<&'a SpecialJoinInfo<'mcx>>,
    ) -> PgResult<f64>
);

seam_core::seam!(
    pub fn make_restrictinfo<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        clause: Node<'mcx>,
        is_pushed_down: bool,
        has_clone: bool,
        is_clone: bool,
        pseudoconstant: bool,
        security_level: u32,
        required_relids: Relids<'mcx>,
        incompatible_relids: Relids<'mcx>,
        outer_relids: Relids<'mcx>,
    ) -> PgResult<RinfoId>
);

seam_core::seam!(
    pub fn amcostestimate<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        path_id: PathId,
        loop_count: f64,
    ) -> PgResult<AmCostEstimate>
);

seam_core::seam!(
    pub fn estimate_num_groups<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        group_exprs: &'a [(NodeId, Node<'mcx>)],
        input_rows: f64,
    ) -> PgResult<f64>
);

seam_core::seam!(
    pub fn estimate_array_length<'a>(node: Node<'a>) -> f64
);

seam_core::seam!(
    pub fn mergejoinscansel<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        rinfo: RinfoId,
        opfamily: u32,
        cmptype: i32,
        nulls_first: bool,
    ) -> PgResult<(f64, f64, f64, f64)>
);

seam_core::seam!(
    pub fn estimate_hash_bucket_stats<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        hashkey: Node<'mcx>,
        virtualbuckets: f64,
    ) -> PgResult<(f64, f64)>
);

seam_core::seam!(
    pub fn add_function_cost<'a>(funcid: u32, cost: &'a mut QualCost) -> PgResult<()>
);

seam_core::seam!(
    pub fn get_function_rows<'a>(funcid: u32, node: Option<Node<'a>>) -> PgResult<f64>
);

seam_core::seam!(
    pub fn get_rel_data_width<'a, 'mcx>(
        rel: &'a Relation<'mcx>,
        attr_widths: Option<&'a mut [i32]>,
        min_attr: i16,
    ) -> PgResult<i32>
);

seam_core::seam!(
    pub fn match_index_to_operand<'a, 'mcx>(
        run: &'a PlannerRun<'mcx>,
        operand: Node<'mcx>,
        indexcol: usize,
        index: &'a IndexOptInfo<'mcx>,
    ) -> bool
);
