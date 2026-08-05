use mcx::Mcx;
use types_error::PgResult;
use types_nodes::parsenodes::Query;
use types_nodes::plannodes::PlannedStmt;
use types_portal::ParamListHandle;
use types_nodes::Node;
use types_pathnodes::run::PlannerRun;
use types_pathnodes::{AppendRelInfo, EcId, IndexOptInfo, JoinType, NodeId, PathId, PathKey, PlannerInfo, QualCost, RelId, Relids, RinfoId, ScanDirection, SpecialJoinInfo};
use types_rel::Relation;
use types_core::Oid;
use datum::Datum;

seam_core::seam!(
    // parse is arena-resident and mutated in place (C mutates the Query and
    // shares root->parse by pointer); by-value transit paid two ~Query-sized
    // copies per statement on the trivial-plan path.
    pub fn planner<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        parse: &'mcx mut Query<'mcx>,
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
    pub fn estimate_num_groups_estinfo<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        group_exprs: &'a [(NodeId, Node<'mcx>)],
        input_rows: f64,
    ) -> PgResult<(f64, bool)>
);

seam_core::seam!(
    // run=None is C's NULL root (inline_function's cost probe): the
    // pg_statistic DECHIST arm is skipped.
    pub fn estimate_array_length<'a, 'mcx>(
        run: Option<&'a mut PlannerRun<'mcx>>,
        node: Node<'mcx>,
    ) -> PgResult<f64>
);

seam_core::seam!(
    pub fn query_supports_distinctness<'a, 'mcx>(query: &'a Query<'mcx>) -> bool
);

seam_core::seam!(
    pub fn query_is_distinct_for<'a, 'mcx>(
        query: &'a Query<'mcx>,
        colnos: &'a [i16],
        opids: &'a [Oid],
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn make_pathkey_from_sortop<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        expr: Node<'mcx>,
        ordering_op: Oid,
        reverse_sort: bool,
        nulls_first: bool,
        sortref: u32,
    ) -> PgResult<PathKey>
);

seam_core::seam!(
    pub fn pathkey_is_redundant<'a, 'mcx>(
        run: &'a PlannerRun<'mcx>,
        new_pathkey: PathKey,
        pathkeys: &'a [PathKey],
    ) -> bool
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
    pub fn estimate_multivariate_bucketsize<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        inner: types_pathnodes::RelId,
        hashclauses: &'a [RinfoId],
    ) -> PgResult<(mcx::PgVec<'mcx, RinfoId>, f64)>
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

seam_core::seam!(
    pub fn generate_join_implied_equalities<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        join_relids: &'a Relids<'mcx>,
        outer_relids: &'a Relids<'mcx>,
        inner_rel: RelId,
        sjinfo: Option<&'a SpecialJoinInfo<'mcx>>,
    ) -> PgResult<mcx::PgVec<'mcx, RinfoId>>
);

seam_core::seam!(
    pub fn generate_join_implied_equalities_for_ecs<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        eclasses: &'a [EcId],
        join_relids: &'a Relids<'mcx>,
        outer_relids: &'a Relids<'mcx>,
        inner_rel: RelId,
    ) -> PgResult<mcx::PgVec<'mcx, RinfoId>>
);

seam_core::seam!(
    pub fn find_derived_clause_for_ec_member<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        ec: EcId,
        em: types_pathnodes::EmId,
    ) -> Option<RinfoId>
);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PatternType {
    Like,
    LikeIc,
    Regex,
    RegexIc,
    Prefix,
}

seam_core::seam!(
    pub fn distribute_restrictinfo_to_rels<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        rinfo: RinfoId,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn build_implied_join_equality<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        opno: u32,
        collation: u32,
        item1: Node<'mcx>,
        item2: Node<'mcx>,
        qualscope: Relids<'mcx>,
        security_level: u32,
    ) -> PgResult<RinfoId>
);

seam_core::seam!(
    pub fn process_implied_equality<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        opno: u32,
        collation: u32,
        item1: Node<'mcx>,
        item2: Node<'mcx>,
        qualscope: Relids<'mcx>,
        security_level: u32,
        both_const: bool,
    ) -> PgResult<Option<RinfoId>>
);

seam_core::seam!(
    pub fn pull_var_nodes<'a, 'mcx>(node: Node<'mcx>, out: &'a mut mcx::PgVec<'mcx, Node<'mcx>>)
);

seam_core::seam!(
    pub fn pull_varnos_relids<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        node: Node<'mcx>,
    ) -> PgResult<Relids<'mcx>>
);

seam_core::seam!(
    pub fn add_vars_to_targetlist<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        vars: &'a [Node<'mcx>],
        where_needed: &'a Relids<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn add_vars_to_attr_needed<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        vars: &'a [Node<'mcx>],
        where_needed: &'a Relids<'mcx>,
    )
);

seam_core::seam!(
    pub fn remove_rel_from_restrictinfo<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        rid: RinfoId,
        relid: i32,
        ojrelid: i32,
    )
);

seam_core::seam!(
    pub fn adjust_appendrel_attrs<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        node: Node<'mcx>,
        appinfo: &'a AppendRelInfo<'mcx>,
    ) -> PgResult<Node<'mcx>>
);

seam_core::seam!(
    pub fn adjust_appendrel_attrs_multi<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        node: Node<'mcx>,
        appinfos: &'a [AppendRelInfo<'mcx>],
    ) -> PgResult<Node<'mcx>>
);

seam_core::seam!(
    pub fn adjust_appendrel_attrs_multilevel<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        node: Node<'mcx>,
        childrel: RelId,
        parentrel: RelId,
    ) -> PgResult<Node<'mcx>>
);

seam_core::seam!(
    pub fn adjust_child_rinfo_multilevel<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        rid: RinfoId,
        childrel: RelId,
        parentrel: RelId,
    ) -> PgResult<RinfoId>
);

seam_core::seam!(
    pub fn expr_collation<'a>(node: Node<'a>) -> u32
);

seam_core::seam!(
    pub fn commute_restrictinfo<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        rid: RinfoId,
        comm_op: Oid,
    ) -> PgResult<RinfoId>
);

seam_core::seam!(
    pub fn is_dummy_rel<'a, 'mcx>(root: &'a PlannerInfo<'mcx>, rel: RelId) -> bool
);

seam_core::seam!(
    pub fn make_opclause<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        opno: Oid,
        leftop: Node<'mcx>,
        rightop: Node<'mcx>,
        inputcollid: Oid,
    ) -> PgResult<Node<'mcx>>
);

seam_core::seam!(
    pub fn match_pattern_prefix<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        leftop: Node<'mcx>,
        rightop: Node<'mcx>,
        ptype: PatternType,
        expr_coll: Oid,
        opfamily: Oid,
        indexcollation: Oid,
    ) -> PgResult<Option<mcx::PgVec<'mcx, Node<'mcx>>>>
);

seam_core::seam!(
    pub fn predicate_implied_by<'a, 'mcx>(
        mcx: Mcx<'mcx>,
        predicate_list: &'a [Node<'mcx>],
        clause_list: &'a [Node<'mcx>],
        weak: bool,
    ) -> PgResult<bool>
);

seam_core::seam!(
    pub fn build_index_pathkeys<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        index: &'a IndexOptInfo<'mcx>,
        scandir: ScanDirection,
    ) -> PgResult<mcx::PgVec<'mcx, PathKey>>
);

seam_core::seam!(
    pub fn truncate_useless_pathkeys<'a, 'mcx>(
        run: &'a mut PlannerRun<'mcx>,
        rel: RelId,
        pathkeys: &'a [PathKey],
    ) -> PgResult<mcx::PgVec<'mcx, PathKey>>
);

seam_core::seam!(
    pub fn inet_ref(d: Datum) -> adt_network::InetRef<'static>
);
