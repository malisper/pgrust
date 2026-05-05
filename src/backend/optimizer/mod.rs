// :HACK: Keep the historical root optimizer path while planning lives in
// `pgrust_optimizer`. Root services bridge planner calls back to executor,
// rewrite, access-method, and datetime runtime code that still belongs here.

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use pgrust_optimizer::{
    CPU_OPERATOR_COST, CPU_TUPLE_COST, DEFAULT_EQ_SEL, DEFAULT_INEQ_SEL, build_join_paths,
    extract_hash_join_clauses, extract_merge_join_clauses, make_restrict_info, predicate_cost,
    pull_up_sublinks,
};

use pgrust_optimizer::runtime::{
    BinaryEvalOp, DateTimeConfig, OptimizerEvalError, OptimizerServices, UnaryEvalOp,
};

use crate::backend::parser::CatalogLookup;
use crate::include::nodes::datetime::DateADT;
use crate::include::nodes::datum::{RecordValue, Value};
use crate::include::nodes::parsenodes::{ParseError, Query, SqlType};
use crate::include::nodes::pathnodes::PlannerConfig;
use crate::include::nodes::plannodes::{Plan, PlannedStmt};
use crate::include::nodes::primnodes::{BuiltinScalarFunction, Expr, RelationPrivilegeRequirement};

pub(crate) mod partition_prune {
    pub(crate) use pgrust_optimizer::partition_prune::{
        partition_may_satisfy_filter_with_runtime_values, relation_may_satisfy_own_partition_bound,
        relation_may_satisfy_own_partition_bound_with_runtime_values,
    };
}

#[cfg(test)]
pub(crate) mod bestpath {
    pub(crate) use pgrust_optimizer::bestpath::*;
}

#[cfg(test)]
pub(crate) mod groupby_rewrite {
    pub(crate) use pgrust_optimizer::groupby_rewrite::*;
}

#[cfg(test)]
pub(crate) mod joininfo {
    pub(crate) use pgrust_optimizer::joininfo::*;
}

#[cfg(test)]
pub(crate) mod path {
    pub(crate) use pgrust_optimizer::path::*;
}

#[cfg(test)]
pub(crate) mod pathnodes {
    pub(crate) use pgrust_optimizer::pathnodes::{PathMethods, rte_slot_id};
}

#[cfg(test)]
pub(crate) mod rewrite {
    pub(crate) use pgrust_optimizer::rewrite::*;
}

#[cfg(test)]
pub(crate) mod root {
    pub(crate) use pgrust_optimizer::root::*;
}

#[cfg(test)]
pub(crate) mod setrefs {
    pub(crate) use pgrust_optimizer::setrefs::{
        validate_executable_plan_for_tests, validate_executable_plan_for_tests_with_params,
        validate_planner_path_for_tests,
    };
}

#[cfg(test)]
pub(crate) mod util {
    pub(crate) use pgrust_optimizer::util::{
        lower_pathkeys_for_path, lower_pathkeys_for_rel, normalize_rte_path,
        path_exposes_required_pathkey_identity, project_to_slot_layout_internal,
        rel_exposes_required_pathkey_identity, required_query_pathkeys_for_path,
        required_query_pathkeys_for_rel,
    };
}

struct RootOptimizerServices;

static ROOT_OPTIMIZER_SERVICES: RootOptimizerServices = RootOptimizerServices;

fn with_root_optimizer_services<T>(f: impl FnOnce() -> T) -> T {
    crate::backend::parser::analyze::with_root_analyze_services(|| {
        pgrust_optimizer::with_optimizer_services(&ROOT_OPTIMIZER_SERVICES, f)
    })
}

pub(crate) fn planner(
    query: Query,
    catalog: &dyn CatalogLookup,
) -> Result<PlannedStmt, ParseError> {
    with_root_optimizer_services(|| pgrust_optimizer::planner(query, catalog))
}

pub(crate) fn planner_with_config(
    query: Query,
    catalog: &dyn CatalogLookup,
    config: PlannerConfig,
) -> Result<PlannedStmt, ParseError> {
    with_root_optimizer_services(|| pgrust_optimizer::planner_with_config(query, catalog, config))
}

pub(crate) fn fold_query_constants(query: Query) -> Result<Query, ParseError> {
    with_root_optimizer_services(|| pgrust_optimizer::fold_query_constants(query))
}

pub(crate) fn fold_expr_constants(expr: Expr) -> Result<Expr, ParseError> {
    with_root_optimizer_services(|| pgrust_optimizer::fold_expr_constants(expr))
}

pub(crate) fn planner_with_param_base(
    query: Query,
    catalog: &dyn CatalogLookup,
    next_param_id: usize,
) -> Result<(PlannedStmt, usize), ParseError> {
    with_root_optimizer_services(|| {
        pgrust_optimizer::planner_with_param_base(query, catalog, next_param_id)
    })
}

pub(crate) fn planner_with_param_base_and_config(
    query: Query,
    catalog: &dyn CatalogLookup,
    next_param_id: usize,
    config: PlannerConfig,
) -> Result<(PlannedStmt, usize), ParseError> {
    with_root_optimizer_services(|| {
        pgrust_optimizer::planner_with_param_base_and_config(query, catalog, next_param_id, config)
    })
}

pub(crate) fn finalize_expr_subqueries(
    expr: Expr,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> Expr {
    with_root_optimizer_services(|| {
        pgrust_optimizer::finalize_expr_subqueries(expr, catalog, subplans)
    })
}

pub(crate) fn finalize_plan_subqueries(
    plan: Plan,
    catalog: &dyn CatalogLookup,
    subplans: &mut Vec<Plan>,
) -> Plan {
    with_root_optimizer_services(|| {
        pgrust_optimizer::finalize_plan_subqueries(plan, catalog, subplans)
    })
}

fn optimizer_error_from_exec(err: crate::backend::executor::ExecError) -> OptimizerEvalError {
    match err {
        crate::backend::executor::ExecError::Parse(err) => OptimizerEvalError::Parse(err),
        crate::backend::executor::ExecError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        } => OptimizerEvalError::DetailedError {
            message,
            detail,
            hint,
            sqlstate,
        },
        crate::backend::executor::ExecError::DivisionByZero(message) => {
            OptimizerEvalError::DivisionByZero(message.into())
        }
        other => OptimizerEvalError::Other(format!("{other:?}")),
    }
}

impl OptimizerServices for RootOptimizerServices {
    fn cast_value(&self, value: Value, ty: SqlType) -> Result<Value, OptimizerEvalError> {
        crate::backend::executor::cast_value(value, ty).map_err(optimizer_error_from_exec)
    }

    fn cast_value_with_source_type(
        &self,
        value: Value,
        source_type: Option<SqlType>,
        ty: SqlType,
        catalog: Option<&dyn CatalogLookup>,
        _datetime_config: &DateTimeConfig,
    ) -> Result<Value, OptimizerEvalError> {
        crate::backend::executor::cast_value_with_source_type_catalog_and_config(
            value,
            source_type,
            ty,
            catalog,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        )
        .map_err(optimizer_error_from_exec)
    }

    fn compare_order_values(
        &self,
        left: &Value,
        right: &Value,
        collation_oid: Option<u32>,
        nulls_first: Option<bool>,
        descending: bool,
    ) -> Result<std::cmp::Ordering, OptimizerEvalError> {
        crate::backend::executor::compare_order_values(
            left,
            right,
            collation_oid,
            nulls_first,
            descending,
        )
        .map_err(optimizer_error_from_exec)
    }

    fn compare_values(
        &self,
        op: &'static str,
        left: Value,
        right: Value,
        collation_oid: Option<u32>,
    ) -> Result<Value, OptimizerEvalError> {
        crate::backend::executor::expr_ops::compare_values(op, left, right, collation_oid)
            .map_err(optimizer_error_from_exec)
    }

    fn eval_unary_op(&self, op: UnaryEvalOp, value: Value) -> Result<Value, OptimizerEvalError> {
        let result = match op {
            UnaryEvalOp::Negate => crate::backend::executor::expr_ops::negate_value(value),
            UnaryEvalOp::BitwiseNot => crate::backend::executor::expr_ops::bitwise_not_value(value),
        };
        result.map_err(optimizer_error_from_exec)
    }

    fn eval_binary_op(
        &self,
        op: BinaryEvalOp,
        left: Value,
        right: Value,
    ) -> Result<Value, OptimizerEvalError> {
        let result = match op {
            BinaryEvalOp::Add => crate::backend::executor::expr_ops::add_values(left, right),
            BinaryEvalOp::Sub => crate::backend::executor::expr_ops::sub_values(left, right),
            BinaryEvalOp::BitwiseAnd => {
                crate::backend::executor::expr_ops::bitwise_and_values(left, right)
            }
            BinaryEvalOp::BitwiseOr => {
                crate::backend::executor::expr_ops::bitwise_or_values(left, right)
            }
            BinaryEvalOp::BitwiseXor => {
                crate::backend::executor::expr_ops::bitwise_xor_values(left, right)
            }
            BinaryEvalOp::ShiftLeft => {
                crate::backend::executor::expr_ops::shift_left_values(left, right)
            }
            BinaryEvalOp::ShiftRight => {
                crate::backend::executor::expr_ops::shift_right_values(left, right)
            }
            BinaryEvalOp::Mul => crate::backend::executor::expr_ops::mul_values(left, right),
            BinaryEvalOp::Div => crate::backend::executor::expr_ops::div_values(left, right),
            BinaryEvalOp::Mod => crate::backend::executor::expr_ops::mod_values(left, right),
            BinaryEvalOp::Concat => crate::backend::executor::expr_ops::concat_values(left, right),
        };
        result.map_err(optimizer_error_from_exec)
    }

    fn not_equal_values(
        &self,
        left: Value,
        right: Value,
        collation_oid: Option<u32>,
    ) -> Result<Value, OptimizerEvalError> {
        crate::backend::executor::expr_ops::not_equal_values(left, right, collation_oid)
            .map_err(optimizer_error_from_exec)
    }

    fn order_values(
        &self,
        op: &'static str,
        left: Value,
        right: Value,
        collation_oid: Option<u32>,
    ) -> Result<Value, OptimizerEvalError> {
        crate::backend::executor::expr_ops::order_values(op, left, right, collation_oid)
            .map_err(optimizer_error_from_exec)
    }

    fn order_record_image_values(
        &self,
        op: &'static str,
        left: &RecordValue,
        right: &RecordValue,
    ) -> Result<Value, OptimizerEvalError> {
        crate::backend::executor::expr_ops::order_record_image_values(op, left, right)
            .map_err(optimizer_error_from_exec)
    }

    fn values_are_distinct(&self, left: &Value, right: &Value) -> bool {
        crate::backend::executor::expr_ops::values_are_distinct(left, right)
    }

    fn statistics_value_key(&self, value: &Value) -> Option<String> {
        crate::backend::statistics::types::statistics_value_key(value)
    }

    fn eval_geometry_function(
        &self,
        func: BuiltinScalarFunction,
        values: &[Value],
    ) -> Option<Result<Value, OptimizerEvalError>> {
        pgrust_expr::eval_geometry_function(func, values)
            .map(|result| result.map_err(|err| optimizer_error_from_exec(err.into())))
    }

    fn eval_power_function(&self, values: &[Value]) -> Result<Value, OptimizerEvalError> {
        pgrust_expr::eval_power_function(values)
            .map_err(|err| optimizer_error_from_exec(err.into()))
    }

    fn eval_range_function(
        &self,
        func: BuiltinScalarFunction,
        values: &[Value],
        result_type: Option<SqlType>,
        func_variadic: bool,
        catalog: Option<&dyn CatalogLookup>,
        _datetime_config: &DateTimeConfig,
    ) -> Option<Result<Value, OptimizerEvalError>> {
        crate::backend::executor::expr_range::eval_range_function(
            func,
            values,
            result_type,
            func_variadic,
            catalog,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        )
        .map(|result| result.map_err(optimizer_error_from_exec))
    }

    fn parse_date_text(
        &self,
        text: &str,
        _datetime_config: &DateTimeConfig,
    ) -> Result<DateADT, String> {
        crate::backend::utils::time::date::parse_date_text(
            text,
            &crate::backend::utils::misc::guc_datetime::DateTimeConfig::default(),
        )
        .map_err(|err| format!("{err:?}"))
    }

    fn hash_value_extended(
        &self,
        value: &Value,
        opclass: Option<u32>,
        seed: u64,
    ) -> Result<Option<u64>, String> {
        crate::backend::access::hash::support::hash_value_extended(value, opclass, seed)
    }

    fn hash_combine64(&self, left: u64, right: u64) -> u64 {
        crate::backend::access::hash::support::hash_combine64(left, right)
    }

    fn access_method_supports_index_scan(&self, am_oid: u32) -> bool {
        crate::backend::access::index::amapi::index_am_handler(am_oid)
            .is_some_and(|routine| routine.amgettuple.is_some())
    }

    fn access_method_supports_bitmap_scan(&self, am_oid: u32) -> bool {
        crate::backend::access::index::amapi::index_am_handler(am_oid)
            .is_some_and(|routine| routine.amgetbitmap.is_some())
    }

    fn pg_rewrite_query(
        &self,
        query: Query,
        catalog: &dyn CatalogLookup,
    ) -> Result<Vec<Query>, ParseError> {
        crate::backend::rewrite::pg_rewrite_query(query, catalog)
    }

    fn collect_query_relation_privileges(
        &self,
        query: &Query,
    ) -> Vec<RelationPrivilegeRequirement> {
        crate::backend::rewrite::collect_query_relation_privileges(query)
    }

    fn render_explain_expr(&self, expr: &Expr, column_names: &[String]) -> String {
        crate::backend::executor::render_explain_expr(expr, column_names)
    }
}
