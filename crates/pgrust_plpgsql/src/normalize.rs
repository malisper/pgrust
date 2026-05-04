use pgrust_analyze::SlotScopeColumn;
use pgrust_nodes::parsenodes::{
    ArraySubscript, Assignment, AssignmentTarget, AssignmentTargetIndirection, CteBody,
    DeleteStatement, FromItem, GroupByItem, InsertSource, InsertStatement, JoinConstraint,
    MergeAction, MergeInsertSource, MergeStatement, OnConflictAction, OnConflictClause,
    OnConflictTarget, OrderByItem, RawWindowFrame, RawWindowFrameBound, RawWindowSpec, RawXmlExpr,
    SelectItem, SelectStatement, SqlCallArgs, SqlCaseWhen, SqlExpr, Statement, UpdateStatement,
    ValuesStatement, XmlTableColumn,
};
use pgrust_nodes::{SqlType, SqlTypeKind};

use crate::{PlpgsqlVariableConflict, is_internal_plpgsql_name, plpgsql_var_alias};

#[derive(Debug, Clone)]
pub struct PlpgsqlVarRef {
    pub slot: usize,
    pub ty: SqlType,
}

#[derive(Debug, Clone)]
pub struct PlpgsqlLabeledVarRef {
    pub var: PlpgsqlVarRef,
    pub alias: String,
}

pub trait PlpgsqlNormalizeEnv {
    fn get_var(&self, name: &str) -> Option<PlpgsqlVarRef>;
    fn get_labeled_var(&self, label: &str, name: &str) -> Option<PlpgsqlLabeledVarRef>;
    fn get_relation_field(&self, relation: &str, field: &str) -> Option<SlotScopeColumn>;
    fn get_labeled_relation_field(
        &self,
        label: &str,
        relation: &str,
        field: &str,
    ) -> Option<SlotScopeColumn>;
    fn variable_conflict(&self) -> PlpgsqlVariableConflict;
}

pub fn normalize_plpgsql_sql_statement(
    stmt: Statement,
    env: &impl PlpgsqlNormalizeEnv,
) -> Statement {
    match stmt {
        Statement::Update(mut stmt) => {
            for assignment in &mut stmt.assignments {
                assignment.expr = normalize_plpgsql_expr(assignment.expr.clone(), env);
                normalize_assignment_target_subscripts(&mut assignment.target, env);
            }
            if let Some(where_clause) = stmt.where_clause.take() {
                stmt.where_clause = Some(normalize_plpgsql_expr(where_clause, env));
            }
            for item in &mut stmt.returning {
                item.expr = normalize_plpgsql_expr(item.expr.clone(), env);
            }
            Statement::Update(stmt)
        }
        Statement::Delete(mut stmt) => {
            if let Some(where_clause) = stmt.where_clause.take() {
                stmt.where_clause = Some(normalize_plpgsql_expr(where_clause, env));
            }
            for item in &mut stmt.returning {
                item.expr = normalize_plpgsql_expr(item.expr.clone(), env);
            }
            Statement::Delete(stmt)
        }
        Statement::Insert(mut stmt) => {
            normalize_insert_source(&mut stmt.source, env);
            if let Some(clause) = &mut stmt.on_conflict
                && matches!(clause.action, OnConflictAction::Update)
            {
                for assignment in &mut clause.assignments {
                    assignment.expr = normalize_plpgsql_expr(assignment.expr.clone(), env);
                    normalize_assignment_target_subscripts(&mut assignment.target, env);
                }
                if let Some(predicate) = clause.where_clause.take() {
                    clause.where_clause = Some(normalize_plpgsql_expr(predicate, env));
                }
            }
            for item in &mut stmt.returning {
                item.expr = normalize_plpgsql_expr(item.expr.clone(), env);
            }
            Statement::Insert(stmt)
        }
        other => other,
    }
}

fn normalize_assignment_target_subscripts(
    target: &mut AssignmentTarget,
    env: &impl PlpgsqlNormalizeEnv,
) {
    for subscript in &mut target.subscripts {
        if let Some(lower) = subscript.lower.take() {
            subscript.lower = Some(Box::new(normalize_plpgsql_expr(*lower, env)));
        }
        if let Some(upper) = subscript.upper.take() {
            subscript.upper = Some(Box::new(normalize_plpgsql_expr(*upper, env)));
        }
    }
    for indirection in &mut target.indirection {
        if let AssignmentTargetIndirection::Subscript(subscript) = indirection {
            if let Some(lower) = subscript.lower.take() {
                subscript.lower = Some(Box::new(normalize_plpgsql_expr(*lower, env)));
            }
            if let Some(upper) = subscript.upper.take() {
                subscript.upper = Some(Box::new(normalize_plpgsql_expr(*upper, env)));
            }
        }
    }
}

fn normalize_insert_source(source: &mut InsertSource, env: &impl PlpgsqlNormalizeEnv) {
    match source {
        InsertSource::Values(rows) => {
            for row in rows {
                for expr in row {
                    *expr = normalize_plpgsql_expr(expr.clone(), env);
                }
            }
        }
        InsertSource::Select(select) => {
            *select = Box::new(normalize_plpgsql_select((**select).clone(), env));
        }
        InsertSource::DefaultValues => {}
    }
}

pub fn normalize_plpgsql_expr(expr: SqlExpr, env: &impl PlpgsqlNormalizeEnv) -> SqlExpr {
    match expr {
        SqlExpr::Column(name) => {
            if let Some(expr) = normalize_labeled_column_name(&name, env) {
                return expr;
            }
            if env.variable_conflict() == PlpgsqlVariableConflict::UseVariable
                && !name.contains('.')
                && !is_internal_plpgsql_name(&name)
                && let Some(var) = env.get_var(&name)
            {
                return SqlExpr::Column(plpgsql_var_alias(var.slot));
            }
            if let Some((base, field)) = name.rsplit_once('.')
                && let Some(var) = env.get_var(base)
                && matches!(var.ty.kind, SqlTypeKind::Record | SqlTypeKind::Composite)
            {
                return SqlExpr::FieldSelect {
                    expr: Box::new(SqlExpr::Column(base.to_string())),
                    field: field.to_string(),
                };
            }
            SqlExpr::Column(name)
        }
        SqlExpr::FieldSelect { expr, field } => {
            if let Some(normalized) = normalize_labeled_field_select(&expr, &field, env) {
                return normalized;
            }
            SqlExpr::FieldSelect {
                expr: Box::new(normalize_plpgsql_expr(*expr, env)),
                field,
            }
        }
        SqlExpr::Add(left, right) => SqlExpr::Add(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Sub(left, right) => SqlExpr::Sub(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::BitAnd(left, right) => SqlExpr::BitAnd(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::BitOr(left, right) => SqlExpr::BitOr(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::BitXor(left, right) => SqlExpr::BitXor(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Shl(left, right) => SqlExpr::Shl(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Shr(left, right) => SqlExpr::Shr(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Mul(left, right) => SqlExpr::Mul(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Div(left, right) => SqlExpr::Div(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Mod(left, right) => SqlExpr::Mod(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Concat(left, right) => SqlExpr::Concat(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::BinaryOperator { op, left, right } => SqlExpr::BinaryOperator {
            op,
            left: Box::new(normalize_plpgsql_expr(*left, env)),
            right: Box::new(normalize_plpgsql_expr(*right, env)),
        },
        SqlExpr::UnaryPlus(inner) => {
            SqlExpr::UnaryPlus(Box::new(normalize_plpgsql_expr(*inner, env)))
        }
        SqlExpr::Negate(inner) => SqlExpr::Negate(Box::new(normalize_plpgsql_expr(*inner, env))),
        SqlExpr::BitNot(inner) => SqlExpr::BitNot(Box::new(normalize_plpgsql_expr(*inner, env))),
        SqlExpr::Subscript { expr, index } => SqlExpr::Subscript {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            index,
        },
        SqlExpr::GeometryUnaryOp { op, expr } => SqlExpr::GeometryUnaryOp {
            op,
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
        },
        SqlExpr::GeometryBinaryOp { op, left, right } => SqlExpr::GeometryBinaryOp {
            op,
            left: Box::new(normalize_plpgsql_expr(*left, env)),
            right: Box::new(normalize_plpgsql_expr(*right, env)),
        },
        SqlExpr::PrefixOperator { op, expr } => SqlExpr::PrefixOperator {
            op,
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
        },
        SqlExpr::Cast(inner, ty) => {
            SqlExpr::Cast(Box::new(normalize_plpgsql_expr(*inner, env)), ty)
        }
        SqlExpr::Collate { expr, collation } => SqlExpr::Collate {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            collation,
        },
        SqlExpr::AtTimeZone { expr, zone } => SqlExpr::AtTimeZone {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            zone: Box::new(normalize_plpgsql_expr(*zone, env)),
        },
        SqlExpr::Eq(left, right) => SqlExpr::Eq(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::NotEq(left, right) => SqlExpr::NotEq(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Lt(left, right) => SqlExpr::Lt(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::LtEq(left, right) => SqlExpr::LtEq(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Gt(left, right) => SqlExpr::Gt(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::GtEq(left, right) => SqlExpr::GtEq(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::RegexMatch(left, right) => SqlExpr::RegexMatch(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => SqlExpr::Like {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            pattern: Box::new(normalize_plpgsql_expr(*pattern, env)),
            escape: escape.map(|expr| Box::new(normalize_plpgsql_expr(*expr, env))),
            case_insensitive,
            negated,
        },
        SqlExpr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => SqlExpr::Similar {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            pattern: Box::new(normalize_plpgsql_expr(*pattern, env)),
            escape: escape.map(|expr| Box::new(normalize_plpgsql_expr(*expr, env))),
            negated,
        },
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => SqlExpr::Case {
            arg: arg.map(|expr| Box::new(normalize_plpgsql_expr(*expr, env))),
            args: args
                .into_iter()
                .map(|arm| normalize_plpgsql_case_when(arm, env))
                .collect(),
            defresult: defresult.map(|expr| Box::new(normalize_plpgsql_expr(*expr, env))),
        },
        SqlExpr::And(left, right) => SqlExpr::And(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Or(left, right) => SqlExpr::Or(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::Not(inner) => SqlExpr::Not(Box::new(normalize_plpgsql_expr(*inner, env))),
        SqlExpr::IsNull(inner) => SqlExpr::IsNull(Box::new(normalize_plpgsql_expr(*inner, env))),
        SqlExpr::IsNotNull(inner) => {
            SqlExpr::IsNotNull(Box::new(normalize_plpgsql_expr(*inner, env)))
        }
        SqlExpr::IsDistinctFrom(left, right) => SqlExpr::IsDistinctFrom(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::IsNotDistinctFrom(left, right) => SqlExpr::IsNotDistinctFrom(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::ArrayLiteral(items) => SqlExpr::ArrayLiteral(
            items
                .into_iter()
                .map(|item| normalize_plpgsql_expr(item, env))
                .collect(),
        ),
        SqlExpr::Row(items) => SqlExpr::Row(
            items
                .into_iter()
                .map(|item| normalize_plpgsql_expr(item, env))
                .collect(),
        ),
        SqlExpr::ArrayOverlap(left, right) => SqlExpr::ArrayOverlap(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::ArrayContains(left, right) => SqlExpr::ArrayContains(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::ArrayContained(left, right) => SqlExpr::ArrayContained(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbContains(left, right) => SqlExpr::JsonbContains(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbContained(left, right) => SqlExpr::JsonbContained(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbExists(left, right) => SqlExpr::JsonbExists(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbExistsAny(left, right) => SqlExpr::JsonbExistsAny(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbExistsAll(left, right) => SqlExpr::JsonbExistsAll(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbPathExists(left, right) => SqlExpr::JsonbPathExists(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonbPathMatch(left, right) => SqlExpr::JsonbPathMatch(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::ScalarSubquery(select) => {
            SqlExpr::ScalarSubquery(Box::new(normalize_plpgsql_select(*select, env)))
        }
        SqlExpr::ArraySubquery(select) => {
            SqlExpr::ArraySubquery(Box::new(normalize_plpgsql_select(*select, env)))
        }
        SqlExpr::Exists(select) => {
            SqlExpr::Exists(Box::new(normalize_plpgsql_select(*select, env)))
        }
        SqlExpr::InSubquery {
            expr,
            subquery,
            negated,
        } => SqlExpr::InSubquery {
            expr: Box::new(normalize_plpgsql_expr(*expr, env)),
            subquery: Box::new(normalize_plpgsql_select(*subquery, env)),
            negated,
        },
        SqlExpr::QuantifiedSubquery {
            left,
            op,
            is_all,
            subquery,
        } => SqlExpr::QuantifiedSubquery {
            left: Box::new(normalize_plpgsql_expr(*left, env)),
            op,
            is_all,
            subquery: Box::new(normalize_plpgsql_select(*subquery, env)),
        },
        SqlExpr::QuantifiedArray {
            left,
            op,
            is_all,
            array,
        } => SqlExpr::QuantifiedArray {
            left: Box::new(normalize_plpgsql_expr(*left, env)),
            op,
            is_all,
            array: Box::new(normalize_plpgsql_expr(*array, env)),
        },
        SqlExpr::ArraySubscript { array, subscripts } => SqlExpr::ArraySubscript {
            array: Box::new(normalize_plpgsql_expr(*array, env)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| normalize_plpgsql_array_subscript(subscript, env))
                .collect(),
        },
        SqlExpr::Xml(xml) => SqlExpr::Xml(Box::new(normalize_plpgsql_xml(*xml, env))),
        SqlExpr::JsonGet(left, right) => SqlExpr::JsonGet(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonGetText(left, right) => SqlExpr::JsonGetText(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonPath(left, right) => SqlExpr::JsonPath(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::JsonPathText(left, right) => SqlExpr::JsonPathText(
            Box::new(normalize_plpgsql_expr(*left, env)),
            Box::new(normalize_plpgsql_expr(*right, env)),
        ),
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            within_group,
            distinct,
            func_variadic,
            filter,
            null_treatment,
            over,
        } => SqlExpr::FuncCall {
            name,
            args: normalize_plpgsql_call_args(args, env),
            order_by: order_by
                .into_iter()
                .map(|item| normalize_plpgsql_order_by_item(item, env))
                .collect(),
            within_group: within_group.map(|items| {
                items
                    .into_iter()
                    .map(|item| normalize_plpgsql_order_by_item(item, env))
                    .collect()
            }),
            distinct,
            func_variadic,
            filter: filter.map(|expr| Box::new(normalize_plpgsql_expr(*expr, env))),
            null_treatment,
            over: over.map(|spec| normalize_plpgsql_window_spec(spec, env)),
        },
        other => other,
    }
}

fn normalize_plpgsql_case_when(
    mut arm: SqlCaseWhen,
    env: &impl PlpgsqlNormalizeEnv,
) -> SqlCaseWhen {
    arm.expr = normalize_plpgsql_expr(arm.expr, env);
    arm.result = normalize_plpgsql_expr(arm.result, env);
    arm
}

fn normalize_plpgsql_call_args(args: SqlCallArgs, env: &impl PlpgsqlNormalizeEnv) -> SqlCallArgs {
    match args {
        SqlCallArgs::Star => SqlCallArgs::Star,
        SqlCallArgs::Args(args) => SqlCallArgs::Args(
            args.into_iter()
                .map(|mut arg| {
                    arg.value = normalize_plpgsql_expr(arg.value, env);
                    arg
                })
                .collect(),
        ),
    }
}

fn normalize_plpgsql_order_by_item(
    mut item: OrderByItem,
    env: &impl PlpgsqlNormalizeEnv,
) -> OrderByItem {
    item.expr = normalize_plpgsql_expr(item.expr, env);
    item
}

fn normalize_plpgsql_array_subscript(
    mut subscript: ArraySubscript,
    env: &impl PlpgsqlNormalizeEnv,
) -> ArraySubscript {
    subscript.lower = subscript
        .lower
        .map(|expr| Box::new(normalize_plpgsql_expr(*expr, env)));
    subscript.upper = subscript
        .upper
        .map(|expr| Box::new(normalize_plpgsql_expr(*expr, env)));
    subscript
}

fn normalize_plpgsql_xml(mut xml: RawXmlExpr, env: &impl PlpgsqlNormalizeEnv) -> RawXmlExpr {
    xml.named_args = xml
        .named_args
        .into_iter()
        .map(|expr| normalize_plpgsql_expr(expr, env))
        .collect();
    xml.args = xml
        .args
        .into_iter()
        .map(|expr| normalize_plpgsql_expr(expr, env))
        .collect();
    xml
}

fn normalize_plpgsql_window_spec(
    mut spec: RawWindowSpec,
    env: &impl PlpgsqlNormalizeEnv,
) -> RawWindowSpec {
    spec.partition_by = spec
        .partition_by
        .into_iter()
        .map(|expr| normalize_plpgsql_expr(expr, env))
        .collect();
    spec.order_by = spec
        .order_by
        .into_iter()
        .map(|item| normalize_plpgsql_order_by_item(item, env))
        .collect();
    spec.frame = spec
        .frame
        .map(|frame| Box::new(normalize_plpgsql_window_frame(*frame, env)));
    spec
}

fn normalize_plpgsql_window_frame(
    mut frame: RawWindowFrame,
    env: &impl PlpgsqlNormalizeEnv,
) -> RawWindowFrame {
    frame.start_bound = normalize_plpgsql_window_frame_bound(frame.start_bound, env);
    frame.end_bound = normalize_plpgsql_window_frame_bound(frame.end_bound, env);
    frame
}

fn normalize_plpgsql_window_frame_bound(
    bound: RawWindowFrameBound,
    env: &impl PlpgsqlNormalizeEnv,
) -> RawWindowFrameBound {
    match bound {
        RawWindowFrameBound::OffsetPreceding(expr) => {
            RawWindowFrameBound::OffsetPreceding(Box::new(normalize_plpgsql_expr(*expr, env)))
        }
        RawWindowFrameBound::OffsetFollowing(expr) => {
            RawWindowFrameBound::OffsetFollowing(Box::new(normalize_plpgsql_expr(*expr, env)))
        }
        other => other,
    }
}

fn normalize_labeled_column_name(name: &str, env: &impl PlpgsqlNormalizeEnv) -> Option<SqlExpr> {
    let (label_and_var, field) = name.rsplit_once('.')?;
    let Some((label, qualifier)) = label_and_var.rsplit_once('.') else {
        return env
            .get_labeled_var(label_and_var, field)
            .map(|scope_var| SqlExpr::Column(scope_var.alias.clone()));
    };
    if let Some(scope_var) = env.get_labeled_var(label, qualifier)
        && matches!(
            scope_var.var.ty.kind,
            SqlTypeKind::Record | SqlTypeKind::Composite
        )
    {
        return Some(SqlExpr::FieldSelect {
            expr: Box::new(SqlExpr::Column(scope_var.alias.clone())),
            field: field.to_string(),
        });
    }
    if env
        .get_labeled_relation_field(label, qualifier, field)
        .is_some()
    {
        return Some(SqlExpr::FieldSelect {
            expr: Box::new(SqlExpr::Column(qualifier.to_string())),
            field: field.to_string(),
        });
    }
    None
}

fn normalize_labeled_field_select(
    expr: &SqlExpr,
    field: &str,
    env: &impl PlpgsqlNormalizeEnv,
) -> Option<SqlExpr> {
    if let SqlExpr::Column(label) = expr
        && let Some(scope_var) = env.get_labeled_var(label, field)
    {
        return Some(SqlExpr::Column(scope_var.alias.clone()));
    }

    if let SqlExpr::Column(label) = expr
        && let Some((qualifier, nested_field)) = field.rsplit_once('.')
    {
        if let Some(scope_var) = env.get_labeled_var(label, qualifier)
            && matches!(
                scope_var.var.ty.kind,
                SqlTypeKind::Record | SqlTypeKind::Composite
            )
        {
            return Some(SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(scope_var.alias.clone())),
                field: nested_field.to_string(),
            });
        }
        if env
            .get_labeled_relation_field(label, qualifier, nested_field)
            .is_some()
        {
            return Some(SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(qualifier.to_string())),
                field: nested_field.to_string(),
            });
        }
    }

    let SqlExpr::FieldSelect {
        expr,
        field: qualifier,
    } = expr
    else {
        return None;
    };
    let SqlExpr::Column(label) = expr.as_ref() else {
        return None;
    };
    if let Some((qualifier, nested_field)) = field.rsplit_once('.') {
        if let Some(scope_var) = env.get_labeled_var(label, qualifier)
            && matches!(
                scope_var.var.ty.kind,
                SqlTypeKind::Record | SqlTypeKind::Composite
            )
        {
            return Some(SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(scope_var.alias.clone())),
                field: nested_field.to_string(),
            });
        }
        if env
            .get_labeled_relation_field(label, qualifier, nested_field)
            .is_some()
        {
            return Some(SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(qualifier.to_string())),
                field: nested_field.to_string(),
            });
        }
    }
    if let Some(scope_var) = env.get_labeled_var(label, qualifier)
        && matches!(
            scope_var.var.ty.kind,
            SqlTypeKind::Record | SqlTypeKind::Composite
        )
    {
        return Some(SqlExpr::FieldSelect {
            expr: Box::new(SqlExpr::Column(scope_var.alias.clone())),
            field: field.to_string(),
        });
    }
    if env
        .get_labeled_relation_field(label, qualifier, field)
        .is_some()
    {
        return Some(SqlExpr::FieldSelect {
            expr: Box::new(SqlExpr::Column(qualifier.clone())),
            field: field.to_string(),
        });
    }
    None
}

pub fn normalize_plpgsql_select(
    mut stmt: SelectStatement,
    env: &impl PlpgsqlNormalizeEnv,
) -> SelectStatement {
    stmt.with = stmt
        .with
        .into_iter()
        .map(|mut cte| {
            cte.body = normalize_plpgsql_cte_body(cte.body, env);
            cte
        })
        .collect();
    stmt.targets = stmt
        .targets
        .into_iter()
        .map(|mut target| {
            target.expr = normalize_plpgsql_expr(target.expr, env);
            target
        })
        .collect();
    stmt.where_clause = stmt
        .where_clause
        .map(|expr| normalize_plpgsql_expr(expr, env));
    stmt.group_by = stmt
        .group_by
        .into_iter()
        .map(|item| normalize_plpgsql_group_by_item(item, env))
        .collect();
    stmt.having = stmt.having.map(|expr| normalize_plpgsql_expr(expr, env));
    stmt.order_by = stmt
        .order_by
        .into_iter()
        .map(|mut item| {
            item.expr = normalize_plpgsql_expr(item.expr, env);
            item
        })
        .collect();
    stmt.from = stmt.from.map(|from| normalize_plpgsql_from_item(from, env));
    if let Some(set_operation) = stmt.set_operation.as_mut() {
        set_operation.inputs = set_operation
            .inputs
            .drain(..)
            .map(|input| normalize_plpgsql_select(input, env))
            .collect();
    }
    stmt
}

fn normalize_plpgsql_group_by_item(
    item: GroupByItem,
    env: &impl PlpgsqlNormalizeEnv,
) -> GroupByItem {
    match item {
        GroupByItem::Expr(expr) => GroupByItem::Expr(normalize_plpgsql_expr(expr, env)),
        GroupByItem::Empty => GroupByItem::Empty,
        GroupByItem::List(exprs) => GroupByItem::List(
            exprs
                .into_iter()
                .map(|expr| normalize_plpgsql_expr(expr, env))
                .collect(),
        ),
        GroupByItem::Rollup(items) => GroupByItem::Rollup(
            items
                .into_iter()
                .map(|item| normalize_plpgsql_group_by_item(item, env))
                .collect(),
        ),
        GroupByItem::Cube(items) => GroupByItem::Cube(
            items
                .into_iter()
                .map(|item| normalize_plpgsql_group_by_item(item, env))
                .collect(),
        ),
        GroupByItem::Sets(items) => GroupByItem::Sets(
            items
                .into_iter()
                .map(|item| normalize_plpgsql_group_by_item(item, env))
                .collect(),
        ),
    }
}

fn normalize_plpgsql_cte_body(body: CteBody, env: &impl PlpgsqlNormalizeEnv) -> CteBody {
    match body {
        CteBody::Select(select) => {
            CteBody::Select(Box::new(normalize_plpgsql_select(*select, env)))
        }
        CteBody::Values(values) => CteBody::Values(normalize_plpgsql_values(values, env)),
        CteBody::Insert(insert) => {
            CteBody::Insert(Box::new(normalize_plpgsql_insert(*insert, env)))
        }
        CteBody::Update(update) => {
            CteBody::Update(Box::new(normalize_plpgsql_update(*update, env)))
        }
        CteBody::Delete(delete) => {
            CteBody::Delete(Box::new(normalize_plpgsql_delete(*delete, env)))
        }
        CteBody::Merge(merge) => CteBody::Merge(Box::new(normalize_plpgsql_merge(*merge, env))),
        CteBody::RecursiveUnion {
            all,
            left_nested,
            anchor_with_is_subquery,
            anchor,
            recursive,
        } => CteBody::RecursiveUnion {
            all,
            left_nested,
            anchor_with_is_subquery,
            anchor: Box::new(normalize_plpgsql_cte_body(*anchor, env)),
            recursive: Box::new(normalize_plpgsql_select(*recursive, env)),
        },
    }
}

pub fn normalize_plpgsql_insert(
    mut stmt: InsertStatement,
    env: &impl PlpgsqlNormalizeEnv,
) -> InsertStatement {
    stmt.with = stmt
        .with
        .into_iter()
        .map(|mut cte| {
            cte.body = normalize_plpgsql_cte_body(cte.body, env);
            cte
        })
        .collect();
    stmt.columns = stmt.columns.map(|columns| {
        columns
            .into_iter()
            .map(|target| normalize_plpgsql_assignment_target(target, env))
            .collect()
    });
    stmt.source = match stmt.source {
        InsertSource::Values(rows) => InsertSource::Values(
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| normalize_plpgsql_expr(expr, env))
                        .collect()
                })
                .collect(),
        ),
        InsertSource::DefaultValues => InsertSource::DefaultValues,
        InsertSource::Select(select) => {
            InsertSource::Select(Box::new(normalize_plpgsql_select(*select, env)))
        }
    };
    stmt.on_conflict = stmt
        .on_conflict
        .map(|clause| normalize_plpgsql_on_conflict(clause, env));
    let returning_targets = std::mem::take(&mut stmt.returning.targets);
    stmt.returning.targets = returning_targets
        .into_iter()
        .map(|item| normalize_plpgsql_select_item(item, env))
        .collect();
    stmt
}

pub fn normalize_plpgsql_update(
    mut stmt: UpdateStatement,
    env: &impl PlpgsqlNormalizeEnv,
) -> UpdateStatement {
    stmt.with = stmt
        .with
        .into_iter()
        .map(|mut cte| {
            cte.body = normalize_plpgsql_cte_body(cte.body, env);
            cte
        })
        .collect();
    stmt.assignments = stmt
        .assignments
        .into_iter()
        .map(|assignment| normalize_plpgsql_assignment(assignment, env))
        .collect();
    stmt.from = stmt.from.map(|from| normalize_plpgsql_from_item(from, env));
    stmt.where_clause = stmt
        .where_clause
        .map(|expr| normalize_plpgsql_expr(expr, env));
    let returning_targets = std::mem::take(&mut stmt.returning.targets);
    stmt.returning.targets = returning_targets
        .into_iter()
        .map(|item| normalize_plpgsql_select_item(item, env))
        .collect();
    stmt
}

pub fn normalize_plpgsql_delete(
    mut stmt: DeleteStatement,
    env: &impl PlpgsqlNormalizeEnv,
) -> DeleteStatement {
    stmt.with = stmt
        .with
        .into_iter()
        .map(|mut cte| {
            cte.body = normalize_plpgsql_cte_body(cte.body, env);
            cte
        })
        .collect();
    stmt.where_clause = stmt
        .where_clause
        .map(|expr| normalize_plpgsql_expr(expr, env));
    stmt.using = stmt
        .using
        .map(|from| normalize_plpgsql_from_item(from, env));
    let returning_targets = std::mem::take(&mut stmt.returning.targets);
    stmt.returning.targets = returning_targets
        .into_iter()
        .map(|item| normalize_plpgsql_select_item(item, env))
        .collect();
    stmt
}

fn normalize_plpgsql_merge(
    mut stmt: MergeStatement,
    env: &impl PlpgsqlNormalizeEnv,
) -> MergeStatement {
    stmt.with = stmt
        .with
        .into_iter()
        .map(|mut cte| {
            cte.body = normalize_plpgsql_cte_body(cte.body, env);
            cte
        })
        .collect();
    stmt.source = normalize_plpgsql_from_item(stmt.source, env);
    stmt.join_condition = normalize_plpgsql_expr(stmt.join_condition, env);
    stmt.when_clauses = stmt
        .when_clauses
        .into_iter()
        .map(|mut clause| {
            clause.condition = clause
                .condition
                .map(|expr| normalize_plpgsql_expr(expr, env));
            clause.action = match clause.action {
                MergeAction::DoNothing => MergeAction::DoNothing,
                MergeAction::Delete => MergeAction::Delete,
                MergeAction::Update { assignments } => MergeAction::Update {
                    assignments: assignments
                        .into_iter()
                        .map(|assignment| normalize_plpgsql_assignment(assignment, env))
                        .collect(),
                },
                MergeAction::Insert {
                    columns,
                    overriding,
                    source,
                } => MergeAction::Insert {
                    columns: columns.map(|columns| {
                        columns
                            .into_iter()
                            .map(|target| normalize_plpgsql_assignment_target(target, env))
                            .collect()
                    }),
                    overriding,
                    source: match source {
                        MergeInsertSource::Values(values) => MergeInsertSource::Values(
                            values
                                .into_iter()
                                .map(|expr| normalize_plpgsql_expr(expr, env))
                                .collect(),
                        ),
                        MergeInsertSource::DefaultValues => MergeInsertSource::DefaultValues,
                    },
                },
            };
            clause
        })
        .collect();
    let returning_targets = std::mem::take(&mut stmt.returning.targets);
    stmt.returning.targets = returning_targets
        .into_iter()
        .map(|item| normalize_plpgsql_select_item(item, env))
        .collect();
    stmt
}

pub fn normalize_plpgsql_values(
    mut values: ValuesStatement,
    env: &impl PlpgsqlNormalizeEnv,
) -> ValuesStatement {
    values.with = values
        .with
        .into_iter()
        .map(|mut cte| {
            cte.body = normalize_plpgsql_cte_body(cte.body, env);
            cte
        })
        .collect();
    values.rows = values
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|expr| normalize_plpgsql_expr(expr, env))
                .collect()
        })
        .collect();
    values.order_by = values
        .order_by
        .into_iter()
        .map(|mut item| {
            item.expr = normalize_plpgsql_expr(item.expr, env);
            item
        })
        .collect();
    values
}

fn normalize_plpgsql_on_conflict(
    mut clause: OnConflictClause,
    env: &impl PlpgsqlNormalizeEnv,
) -> OnConflictClause {
    clause.target = clause.target.map(|target| match target {
        OnConflictTarget::Inference(mut inference) => {
            inference.elements = inference
                .elements
                .into_iter()
                .map(|mut elem| {
                    elem.expr = normalize_plpgsql_expr(elem.expr, env);
                    elem
                })
                .collect();
            inference.predicate = inference
                .predicate
                .map(|expr| normalize_plpgsql_expr(expr, env));
            OnConflictTarget::Inference(inference)
        }
        OnConflictTarget::Constraint(name) => OnConflictTarget::Constraint(name),
    });
    clause.assignments = clause
        .assignments
        .into_iter()
        .map(|assignment| normalize_plpgsql_assignment(assignment, env))
        .collect();
    clause.where_clause = clause
        .where_clause
        .map(|expr| normalize_plpgsql_expr(expr, env));
    clause
}

pub fn normalize_plpgsql_select_item(
    mut item: SelectItem,
    env: &impl PlpgsqlNormalizeEnv,
) -> SelectItem {
    item.expr = normalize_plpgsql_expr(item.expr, env);
    item
}

fn normalize_plpgsql_assignment(
    mut assignment: Assignment,
    env: &impl PlpgsqlNormalizeEnv,
) -> Assignment {
    assignment.target = normalize_plpgsql_assignment_target(assignment.target, env);
    assignment.expr = normalize_plpgsql_expr(assignment.expr, env);
    assignment
}

fn normalize_plpgsql_assignment_target(
    mut target: AssignmentTarget,
    env: &impl PlpgsqlNormalizeEnv,
) -> AssignmentTarget {
    target.subscripts = target
        .subscripts
        .into_iter()
        .map(|subscript| normalize_plpgsql_array_subscript(subscript, env))
        .collect();
    target.indirection = target
        .indirection
        .into_iter()
        .map(|step| match step {
            AssignmentTargetIndirection::Subscript(subscript) => {
                AssignmentTargetIndirection::Subscript(normalize_plpgsql_array_subscript(
                    subscript, env,
                ))
            }
            AssignmentTargetIndirection::Field(field) => AssignmentTargetIndirection::Field(field),
        })
        .collect();
    target
}

fn normalize_plpgsql_from_item(item: FromItem, env: &impl PlpgsqlNormalizeEnv) -> FromItem {
    match item {
        FromItem::Values { rows } => FromItem::Values {
            rows: rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|expr| normalize_plpgsql_expr(expr, env))
                        .collect()
                })
                .collect(),
        },
        FromItem::FunctionCall {
            name,
            args,
            func_variadic,
            with_ordinality,
        } => FromItem::FunctionCall {
            name,
            args: args
                .into_iter()
                .map(|mut arg| {
                    arg.value = normalize_plpgsql_expr(arg.value, env);
                    arg
                })
                .collect(),
            func_variadic,
            with_ordinality,
        },
        FromItem::XmlTable(mut table) => {
            table.namespaces = table
                .namespaces
                .into_iter()
                .map(|mut namespace| {
                    namespace.uri = normalize_plpgsql_expr(namespace.uri, env);
                    namespace
                })
                .collect();
            table.row_path = normalize_plpgsql_expr(table.row_path, env);
            table.document = normalize_plpgsql_expr(table.document, env);
            table.columns = table
                .columns
                .into_iter()
                .map(|column| match column {
                    XmlTableColumn::Regular {
                        name,
                        type_name,
                        path,
                        default,
                        not_null,
                    } => XmlTableColumn::Regular {
                        name,
                        type_name,
                        path: path.map(|expr| normalize_plpgsql_expr(expr, env)),
                        default: default.map(|expr| normalize_plpgsql_expr(expr, env)),
                        not_null,
                    },
                    XmlTableColumn::Ordinality { name } => XmlTableColumn::Ordinality { name },
                })
                .collect();
            FromItem::XmlTable(table)
        }
        FromItem::Lateral(source) => {
            FromItem::Lateral(Box::new(normalize_plpgsql_from_item(*source, env)))
        }
        FromItem::DerivedTable(select) => {
            FromItem::DerivedTable(Box::new(normalize_plpgsql_select(*select, env)))
        }
        FromItem::Join {
            left,
            right,
            kind,
            constraint,
        } => FromItem::Join {
            left: Box::new(normalize_plpgsql_from_item(*left, env)),
            right: Box::new(normalize_plpgsql_from_item(*right, env)),
            kind,
            constraint: match constraint {
                JoinConstraint::On(expr) => JoinConstraint::On(normalize_plpgsql_expr(expr, env)),
                other => other,
            },
        },
        FromItem::Alias {
            source,
            alias,
            column_aliases,
            preserve_source_names,
        } => FromItem::Alias {
            source: Box::new(normalize_plpgsql_from_item(*source, env)),
            alias,
            column_aliases,
            preserve_source_names,
        },
        other => other,
    }
}
