use std::cmp::Ordering;

use crate::backend::executor::compare_order_values;
use crate::backend::parser::{
    CatalogLookup, PartitionBoundSpec, PartitionRangeDatumValue, PartitionStrategy,
    SerializedPartitionValue, deserialize_partition_bound, partition_value_to_value,
    relation_partition_spec,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::{
    BoolExprType, BuiltinScalarFunction, Expr, OpExprKind, ScalarFunctionImpl,
};

pub(super) fn partition_may_satisfy_filter(
    catalog: &dyn CatalogLookup,
    parent_oid: u32,
    child_oid: u32,
    filter: Option<&Expr>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    let (Some(parent), Some(child)) = (
        catalog.relation_by_oid(parent_oid),
        catalog.relation_by_oid(child_oid),
    ) else {
        return true;
    };
    let Ok(spec) = relation_partition_spec(&parent) else {
        return true;
    };
    let Some(bound) = child
        .relpartbound
        .as_deref()
        .and_then(|text| deserialize_partition_bound(text).ok())
    else {
        return true;
    };
    let sibling_bounds = catalog
        .inheritance_children(parent_oid)
        .into_iter()
        .filter(|row| !row.inhdetachpending)
        .filter_map(|row| catalog.relation_by_oid(row.inhrelid))
        .filter_map(|rel| {
            rel.relpartbound
                .and_then(|text| deserialize_partition_bound(&text).ok())
        })
        .collect::<Vec<_>>();
    expr_may_match_bound(filter, &spec, &bound, &sibling_bounds)
}

fn expr_may_match_bound(
    expr: &Expr,
    spec: &crate::backend::parser::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
) -> bool {
    match expr {
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            BoolExprType::And => {
                range_may_satisfy_conjunction(expr, spec, bound, sibling_bounds).unwrap_or(true)
                    && bool_expr
                        .args
                        .iter()
                        .all(|arg| expr_may_match_bound(arg, spec, bound, sibling_bounds))
            }
            BoolExprType::Or => bool_expr
                .args
                .iter()
                .any(|arg| expr_may_match_bound(arg, spec, bound, sibling_bounds)),
            BoolExprType::Not => true,
        },
        Expr::IsNull(inner) => partition_key_index(inner, spec)
            .map(|index| bound_may_contain_value(spec, bound, sibling_bounds, index, &Value::Null))
            .unwrap_or(true),
        Expr::IsNotNull(inner) => partition_key_index(inner, spec)
            .map(|index| bound_may_contain_non_null(spec, bound, index))
            .unwrap_or(true),
        Expr::Op(op) => {
            let [left, right] = op.args.as_slice() else {
                return true;
            };
            let Some((key_index, key_on_left, value)) =
                partition_key_const_cmp(left, right, spec, op.collation_oid)
            else {
                return true;
            };
            bound_may_satisfy_comparison(
                spec,
                bound,
                sibling_bounds,
                key_index,
                key_on_left,
                op.op,
                &value,
                op.collation_oid,
            )
        }
        _ => true,
    }
}

fn partition_key_const_cmp(
    left: &Expr,
    right: &Expr,
    spec: &crate::backend::parser::LoweredPartitionSpec,
    query_collation_oid: Option<u32>,
) -> Option<(usize, bool, Value)> {
    for (index, key_expr) in spec.key_exprs.iter().enumerate() {
        let key_collation_oid = spec.partcollation.get(index).copied().unwrap_or(0);
        if collation_mismatch(query_collation_oid, key_collation_oid) {
            continue;
        }
        if partition_key_expr_matches(left, key_expr).is_some()
            && let Some(value) = const_value(right)
        {
            return Some((index, true, value));
        }
        if partition_key_expr_matches(right, key_expr).is_some()
            && let Some(value) = const_value(left)
        {
            return Some((index, false, value));
        }
    }
    None
}

fn partition_key_index(
    expr: &Expr,
    spec: &crate::backend::parser::LoweredPartitionSpec,
) -> Option<usize> {
    spec.key_exprs
        .iter()
        .position(|key_expr| partition_key_expr_matches(expr, key_expr).is_some())
}

fn partition_key_expr_matches<'a>(expr: &'a Expr, key_expr: &Expr) -> Option<&'a Expr> {
    let normalized = normalize_key_expr(expr);
    (normalized == normalize_key_expr(key_expr)).then_some(expr)
}

fn normalize_key_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::BpcharToText)
            ) && func.args.len() == 1 =>
        {
            normalize_key_expr(&func.args[0])
        }
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => normalize_key_expr(inner),
        other => other,
    }
}

fn collation_mismatch(query_collation_oid: Option<u32>, key_collation_oid: u32) -> bool {
    query_collation_oid.is_some_and(|oid| key_collation_oid != 0 && oid != key_collation_oid)
}

fn const_value(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Const(value) => Some(value.clone()),
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => const_value(inner),
        _ => None,
    }
}

fn bound_may_satisfy_comparison(
    spec: &crate::backend::parser::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    key_index: usize,
    key_on_left: bool,
    op: OpExprKind,
    value: &Value,
    collation_oid: Option<u32>,
) -> bool {
    let op = if key_on_left { op } else { commute_op(op) };
    match op {
        OpExprKind::Eq => bound_may_contain_value(spec, bound, sibling_bounds, key_index, value),
        OpExprKind::NotEq => {
            bound_may_contain_non_equal_value(spec, bound, key_index, value, collation_oid)
        }
        OpExprKind::Lt | OpExprKind::LtEq | OpExprKind::Gt | OpExprKind::GtEq => {
            bound_may_overlap_inequality(spec, bound, key_index, op, value, collation_oid)
        }
        _ => true,
    }
}

fn commute_op(op: OpExprKind) -> OpExprKind {
    match op {
        OpExprKind::Lt => OpExprKind::Gt,
        OpExprKind::LtEq => OpExprKind::GtEq,
        OpExprKind::Gt => OpExprKind::Lt,
        OpExprKind::GtEq => OpExprKind::LtEq,
        other => other,
    }
}

fn bound_may_contain_value(
    spec: &crate::backend::parser::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    key_index: usize,
    value: &Value,
) -> bool {
    match (&spec.strategy, bound) {
        (PartitionStrategy::List, PartitionBoundSpec::List { values, is_default }) => {
            if key_index != 0 {
                return true;
            }
            if *is_default {
                !sibling_bounds.iter().any(|sibling| {
                    matches!(sibling, PartitionBoundSpec::List { values, is_default: false } if values.iter().any(|item| serialized_value_eq(item, value)))
                })
            } else {
                values.iter().any(|item| serialized_value_eq(item, value))
            }
        }
        (PartitionStrategy::Range, _) => range_may_satisfy_conjunction_value(
            spec,
            bound,
            sibling_bounds,
            key_index,
            value,
            OpExprKind::Eq,
            None,
        )
        .unwrap_or(true),
        _ => true,
    }
}

fn bound_may_contain_non_null(
    spec: &crate::backend::parser::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    key_index: usize,
) -> bool {
    match (&spec.strategy, bound) {
        (PartitionStrategy::List, PartitionBoundSpec::List { values, is_default }) => {
            if key_index != 0 {
                return true;
            }
            *is_default
                || values
                    .iter()
                    .any(|value| !matches!(value, SerializedPartitionValue::Null))
        }
        (PartitionStrategy::Range, PartitionBoundSpec::Range { is_default, .. }) => *is_default,
        _ => true,
    }
}

fn bound_may_contain_non_equal_value(
    spec: &crate::backend::parser::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    key_index: usize,
    value: &Value,
    collation_oid: Option<u32>,
) -> bool {
    match (&spec.strategy, bound) {
        (PartitionStrategy::List, PartitionBoundSpec::List { values, is_default }) => {
            if key_index != 0 {
                return true;
            }
            *is_default
                || values.iter().any(|item| {
                    compare_partition_value(item, value, collation_oid) != Some(Ordering::Equal)
                })
        }
        _ => true,
    }
}

fn bound_may_overlap_inequality(
    spec: &crate::backend::parser::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    key_index: usize,
    op: OpExprKind,
    value: &Value,
    collation_oid: Option<u32>,
) -> bool {
    match (&spec.strategy, bound) {
        (PartitionStrategy::List, PartitionBoundSpec::List { values, is_default }) => {
            if key_index != 0 {
                return true;
            }
            *is_default
                || values.iter().any(|item| {
                    compare_partition_value(item, value, collation_oid)
                        .map(|cmp| cmp_satisfies(cmp, op))
                        .unwrap_or(true)
                })
        }
        (PartitionStrategy::Range, _) => range_may_satisfy_conjunction_value(
            spec,
            bound,
            &[],
            key_index,
            value,
            op,
            collation_oid,
        )
        .unwrap_or(true),
        _ => true,
    }
}

fn serialized_value_eq(value: &SerializedPartitionValue, other: &Value) -> bool {
    partition_value_to_value(value) == *other
}

fn compare_partition_value(
    value: &SerializedPartitionValue,
    other: &Value,
    collation_oid: Option<u32>,
) -> Option<Ordering> {
    compare_order_values(
        &partition_value_to_value(value),
        other,
        collation_oid,
        None,
        false,
    )
    .ok()
}

fn cmp_satisfies(cmp: Ordering, op: OpExprKind) -> bool {
    match op {
        OpExprKind::Lt => cmp == Ordering::Less,
        OpExprKind::LtEq => cmp != Ordering::Greater,
        OpExprKind::Gt => cmp == Ordering::Greater,
        OpExprKind::GtEq => cmp != Ordering::Less,
        _ => true,
    }
}

#[derive(Clone, Debug)]
struct ScalarBound {
    value: Value,
    inclusive: bool,
}

#[derive(Clone, Debug, Default)]
struct KeyConstraint {
    lower: Option<ScalarBound>,
    upper: Option<ScalarBound>,
    equal: Option<Value>,
    requires_null: bool,
}

#[derive(Debug)]
enum ConstraintApplyResult {
    Applied,
    Ignored,
    Contradiction,
}

fn range_may_satisfy_conjunction(
    expr: &Expr,
    spec: &crate::backend::parser::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
) -> Option<bool> {
    if !matches!(spec.strategy, PartitionStrategy::Range) {
        return None;
    }
    let mut constraints = vec![KeyConstraint::default(); spec.key_exprs.len()];
    let mut saw_constraint = false;
    for conjunct in flatten_and_exprs(expr) {
        match apply_range_constraint(conjunct, spec, &mut constraints) {
            ConstraintApplyResult::Applied => saw_constraint = true,
            ConstraintApplyResult::Ignored => {}
            ConstraintApplyResult::Contradiction => return Some(false),
        }
    }
    if !saw_constraint {
        return None;
    }
    range_bound_may_overlap_constraints(spec, bound, sibling_bounds, &constraints)
}

fn range_may_satisfy_conjunction_value(
    spec: &crate::backend::parser::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    key_index: usize,
    value: &Value,
    op: OpExprKind,
    collation_oid: Option<u32>,
) -> Option<bool> {
    if !matches!(spec.strategy, PartitionStrategy::Range) {
        return None;
    }
    let mut constraints = vec![KeyConstraint::default(); spec.key_exprs.len()];
    if !add_comparison_constraint(
        &mut constraints,
        spec,
        key_index,
        op,
        value.clone(),
        collation_oid,
    ) {
        return Some(false);
    }
    range_bound_may_overlap_constraints(spec, bound, sibling_bounds, &constraints)
}

fn flatten_and_exprs(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => {
            bool_expr.args.iter().flat_map(flatten_and_exprs).collect()
        }
        other => vec![other],
    }
}

fn apply_range_constraint(
    expr: &Expr,
    spec: &crate::backend::parser::LoweredPartitionSpec,
    constraints: &mut [KeyConstraint],
) -> ConstraintApplyResult {
    match expr {
        Expr::IsNull(inner) => {
            let Some(index) = partition_key_index(inner, spec) else {
                return ConstraintApplyResult::Ignored;
            };
            constraints[index].requires_null = true;
            ConstraintApplyResult::Applied
        }
        Expr::IsNotNull(inner) => {
            if partition_key_index(inner, spec).is_some() {
                ConstraintApplyResult::Applied
            } else {
                ConstraintApplyResult::Ignored
            }
        }
        Expr::Op(op) => {
            let [left, right] = op.args.as_slice() else {
                return ConstraintApplyResult::Ignored;
            };
            let Some((key_index, key_on_left, value)) =
                partition_key_const_cmp(left, right, spec, op.collation_oid)
            else {
                return ConstraintApplyResult::Ignored;
            };
            let collation_oid = op.collation_oid;
            let op = if key_on_left {
                op.op
            } else {
                commute_op(op.op)
            };
            if add_comparison_constraint(constraints, spec, key_index, op, value, collation_oid) {
                ConstraintApplyResult::Applied
            } else {
                ConstraintApplyResult::Contradiction
            }
        }
        _ => ConstraintApplyResult::Ignored,
    }
}

fn add_comparison_constraint(
    constraints: &mut [KeyConstraint],
    spec: &crate::backend::parser::LoweredPartitionSpec,
    key_index: usize,
    op: OpExprKind,
    value: Value,
    collation_oid: Option<u32>,
) -> bool {
    if key_index >= constraints.len() {
        return true;
    }
    if matches!(value, Value::Null) {
        if op == OpExprKind::Eq {
            constraints[key_index].requires_null = true;
            return true;
        }
        return true;
    }
    let key_collation_oid = spec.partcollation.get(key_index).copied().unwrap_or(0);
    let collation_oid = collation_oid.or((key_collation_oid != 0).then_some(key_collation_oid));
    let constraint = &mut constraints[key_index];
    match op {
        OpExprKind::Eq => add_equality_constraint(constraint, value, collation_oid),
        OpExprKind::Lt => {
            add_upper_constraint(constraint, value, false, collation_oid);
            constraint_is_possible(constraint, collation_oid)
        }
        OpExprKind::LtEq => {
            add_upper_constraint(constraint, value, true, collation_oid);
            constraint_is_possible(constraint, collation_oid)
        }
        OpExprKind::Gt => {
            add_lower_constraint(constraint, value, false, collation_oid);
            constraint_is_possible(constraint, collation_oid)
        }
        OpExprKind::GtEq => {
            add_lower_constraint(constraint, value, true, collation_oid);
            constraint_is_possible(constraint, collation_oid)
        }
        _ => true,
    }
}

fn add_equality_constraint(
    constraint: &mut KeyConstraint,
    value: Value,
    collation_oid: Option<u32>,
) -> bool {
    if let Some(existing) = &constraint.equal
        && compare_order_values(existing, &value, collation_oid, None, false).ok()
            != Some(Ordering::Equal)
    {
        return false;
    }
    constraint.equal = Some(value);
    constraint_is_possible(constraint, collation_oid)
}

fn add_lower_constraint(
    constraint: &mut KeyConstraint,
    value: Value,
    inclusive: bool,
    collation_oid: Option<u32>,
) {
    let new_bound = ScalarBound { value, inclusive };
    let should_replace = match &constraint.lower {
        None => true,
        Some(existing) => match compare_order_values(
            &new_bound.value,
            &existing.value,
            collation_oid,
            None,
            false,
        )
        .ok()
        {
            Some(Ordering::Greater) => true,
            Some(Ordering::Equal) => !new_bound.inclusive && existing.inclusive,
            _ => false,
        },
    };
    if should_replace {
        constraint.lower = Some(new_bound);
    }
}

fn add_upper_constraint(
    constraint: &mut KeyConstraint,
    value: Value,
    inclusive: bool,
    collation_oid: Option<u32>,
) {
    let new_bound = ScalarBound { value, inclusive };
    let should_replace = match &constraint.upper {
        None => true,
        Some(existing) => match compare_order_values(
            &new_bound.value,
            &existing.value,
            collation_oid,
            None,
            false,
        )
        .ok()
        {
            Some(Ordering::Less) => true,
            Some(Ordering::Equal) => !new_bound.inclusive && existing.inclusive,
            _ => false,
        },
    };
    if should_replace {
        constraint.upper = Some(new_bound);
    }
}

fn constraint_is_possible(constraint: &KeyConstraint, collation_oid: Option<u32>) -> bool {
    let Some(lower) = &constraint.lower else {
        return equality_satisfies_bounds(constraint, collation_oid);
    };
    let Some(upper) = &constraint.upper else {
        return equality_satisfies_bounds(constraint, collation_oid);
    };
    match compare_order_values(&lower.value, &upper.value, collation_oid, None, false).ok() {
        Some(Ordering::Less) => equality_satisfies_bounds(constraint, collation_oid),
        Some(Ordering::Equal) if lower.inclusive && upper.inclusive => {
            equality_satisfies_bounds(constraint, collation_oid)
        }
        Some(_) => false,
        None => true,
    }
}

fn equality_satisfies_bounds(constraint: &KeyConstraint, collation_oid: Option<u32>) -> bool {
    let Some(equal) = &constraint.equal else {
        return true;
    };
    if let Some(lower) = &constraint.lower {
        match compare_order_values(equal, &lower.value, collation_oid, None, false).ok() {
            Some(Ordering::Less) => return false,
            Some(Ordering::Equal) if !lower.inclusive => return false,
            _ => {}
        }
    }
    if let Some(upper) = &constraint.upper {
        match compare_order_values(equal, &upper.value, collation_oid, None, false).ok() {
            Some(Ordering::Greater) => return false,
            Some(Ordering::Equal) if !upper.inclusive => return false,
            _ => {}
        }
    }
    true
}

fn range_bound_may_overlap_constraints(
    spec: &crate::backend::parser::LoweredPartitionSpec,
    bound: &PartitionBoundSpec,
    sibling_bounds: &[PartitionBoundSpec],
    constraints: &[KeyConstraint],
) -> Option<bool> {
    let PartitionBoundSpec::Range {
        from,
        to,
        is_default,
    } = bound
    else {
        return None;
    };
    if *is_default {
        if let Some(values) = exact_constraint_values(constraints) {
            return Some(!sibling_bounds.iter().any(|sibling| {
                !sibling.is_default()
                    && range_bound_contains_tuple(sibling, &values, &spec.partcollation)
                        == Some(true)
            }));
        }
        return Some(true);
    }
    if constraints
        .iter()
        .any(|constraint| constraint.requires_null)
    {
        return Some(false);
    }
    let query_min = query_bound_components(constraints, QueryBoundSide::Lower);
    let query_max = query_bound_components(constraints, QueryBoundSide::Upper);
    if compare_query_bound_to_range_bound(
        &query_max,
        QueryBoundSide::Upper,
        from,
        &spec.partcollation,
    ) == Some(Ordering::Less)
    {
        return Some(false);
    }
    if compare_query_bound_to_range_bound(
        &query_min,
        QueryBoundSide::Lower,
        to,
        &spec.partcollation,
    )
    .is_some_and(|cmp| cmp != Ordering::Less)
    {
        return Some(false);
    }
    Some(true)
}

fn exact_constraint_values(constraints: &[KeyConstraint]) -> Option<Vec<Value>> {
    constraints
        .iter()
        .map(|constraint| {
            if constraint.requires_null {
                return None;
            }
            constraint.equal.clone()
        })
        .collect()
}

fn range_bound_contains_tuple(
    bound: &PartitionBoundSpec,
    values: &[Value],
    collations: &[u32],
) -> Option<bool> {
    let PartitionBoundSpec::Range {
        from,
        to,
        is_default,
    } = bound
    else {
        return None;
    };
    if *is_default || values.iter().any(|value| matches!(value, Value::Null)) {
        return Some(false);
    }
    Some(
        compare_tuple_to_range_bound(values, from, collations) != Some(Ordering::Less)
            && compare_tuple_to_range_bound(values, to, collations) == Some(Ordering::Less),
    )
}

fn compare_tuple_to_range_bound(
    values: &[Value],
    range: &[PartitionRangeDatumValue],
    collations: &[u32],
) -> Option<Ordering> {
    for (index, value) in values.iter().enumerate() {
        let range_component = range
            .get(index)
            .unwrap_or(&PartitionRangeDatumValue::MaxValue);
        let collation_oid = collations.get(index).copied().filter(|oid| *oid != 0);
        let cmp = match range_component {
            PartitionRangeDatumValue::MinValue => Ordering::Greater,
            PartitionRangeDatumValue::MaxValue => Ordering::Less,
            PartitionRangeDatumValue::Value(range_value) => compare_order_values(
                value,
                &partition_value_to_value(range_value),
                collation_oid,
                None,
                false,
            )
            .ok()?,
        };
        if cmp != Ordering::Equal {
            return Some(cmp);
        }
    }
    Some(Ordering::Equal)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryBoundSide {
    Lower,
    Upper,
}

#[derive(Clone, Debug)]
enum QueryBoundComponent<'a> {
    NegInfinity,
    PosInfinity,
    Value { value: &'a Value, inclusive: bool },
}

fn query_bound_components(
    constraints: &[KeyConstraint],
    side: QueryBoundSide,
) -> Vec<QueryBoundComponent<'_>> {
    constraints
        .iter()
        .map(|constraint| {
            if let Some(equal) = &constraint.equal {
                return QueryBoundComponent::Value {
                    value: equal,
                    inclusive: true,
                };
            }
            match side {
                QueryBoundSide::Lower => constraint
                    .lower
                    .as_ref()
                    .map(|bound| QueryBoundComponent::Value {
                        value: &bound.value,
                        inclusive: bound.inclusive,
                    })
                    .unwrap_or(QueryBoundComponent::NegInfinity),
                QueryBoundSide::Upper => constraint
                    .upper
                    .as_ref()
                    .map(|bound| QueryBoundComponent::Value {
                        value: &bound.value,
                        inclusive: bound.inclusive,
                    })
                    .unwrap_or(QueryBoundComponent::PosInfinity),
            }
        })
        .collect()
}

fn compare_query_bound_to_range_bound(
    query: &[QueryBoundComponent<'_>],
    side: QueryBoundSide,
    range: &[PartitionRangeDatumValue],
    collations: &[u32],
) -> Option<Ordering> {
    for (index, query_component) in query.iter().enumerate() {
        let range_component = range
            .get(index)
            .unwrap_or(&PartitionRangeDatumValue::MaxValue);
        let collation_oid = collations.get(index).copied().filter(|oid| *oid != 0);
        let cmp = compare_query_component_to_range_component(
            query_component,
            range_component,
            side,
            collation_oid,
        )?;
        if cmp != Ordering::Equal {
            return Some(cmp);
        }
    }
    Some(Ordering::Equal)
}

fn compare_query_component_to_range_component(
    query: &QueryBoundComponent<'_>,
    range: &PartitionRangeDatumValue,
    side: QueryBoundSide,
    collation_oid: Option<u32>,
) -> Option<Ordering> {
    match (query, range) {
        (QueryBoundComponent::NegInfinity, PartitionRangeDatumValue::MinValue)
        | (QueryBoundComponent::PosInfinity, PartitionRangeDatumValue::MaxValue) => {
            Some(Ordering::Equal)
        }
        (QueryBoundComponent::NegInfinity, _) => Some(Ordering::Less),
        (QueryBoundComponent::PosInfinity, _) => Some(Ordering::Greater),
        (_, PartitionRangeDatumValue::MinValue) => Some(Ordering::Greater),
        (_, PartitionRangeDatumValue::MaxValue) => Some(Ordering::Less),
        (
            QueryBoundComponent::Value { value, inclusive },
            PartitionRangeDatumValue::Value(range_value),
        ) => {
            let cmp = compare_order_values(
                value,
                &partition_value_to_value(range_value),
                collation_oid,
                None,
                false,
            )
            .ok()?;
            if cmp != Ordering::Equal || *inclusive {
                return Some(cmp);
            }
            match side {
                QueryBoundSide::Lower => Some(Ordering::Greater),
                QueryBoundSide::Upper => Some(Ordering::Less),
            }
        }
    }
}
