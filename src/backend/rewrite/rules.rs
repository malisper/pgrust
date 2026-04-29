use crate::backend::parser::{
    Assignment, AssignmentTarget, AssignmentTargetIndirection, CatalogLookup, InsertSource,
    InsertStatement, RawTypeName, SelectStatement, SqlCallArgs, SqlExpr, SqlType, SqlTypeKind,
    Statement, UpdateStatement, parse_statement, sql_type_name,
};
use crate::include::catalog::PgRewriteRow;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;

pub(crate) fn split_stored_rule_action_sql(ev_action: &str) -> Vec<&str> {
    if ev_action.is_empty() {
        Vec::new()
    } else {
        ev_action
            .split(";\n")
            .map(str::trim)
            .filter(|sql| !sql.is_empty())
            .collect()
    }
}

pub(crate) fn format_stored_rule_definition(rule: &PgRewriteRow, relation_name: &str) -> String {
    format_stored_rule_definition_inner(rule, relation_name, None)
}

pub(crate) fn format_stored_rule_definition_with_catalog(
    rule: &PgRewriteRow,
    relation_name: &str,
    catalog: &dyn CatalogLookup,
) -> String {
    format_stored_rule_definition_inner(rule, relation_name, Some(catalog))
}

fn format_stored_rule_definition_inner(
    rule: &PgRewriteRow,
    relation_name: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> String {
    if rule.rulename == "_RETURN"
        && rule.ev_type == '1'
        && rule.ev_qual.is_empty()
        && rule.is_instead
        && let Some(catalog) = catalog
        && let Some(relation) = catalog.lookup_relation_by_oid(rule.ev_class)
        && let Ok(action) =
            super::views::format_view_definition(rule.ev_class, &relation.desc, catalog)
    {
        return format!(
            "CREATE RULE \"_RETURN\" AS\n    ON SELECT TO {relation_name} DO INSTEAD  {action}"
        );
    }
    let mut definition = format!(
        "CREATE RULE {} AS ON {} TO {}",
        rule.rulename,
        format_rule_event(rule.ev_type),
        relation_name,
    );
    if !rule.ev_qual.is_empty() {
        definition.push_str(" WHERE ");
        definition.push_str(&rule.ev_qual);
    }
    definition.push_str(" DO ");
    definition.push_str(if rule.is_instead { "INSTEAD" } else { "ALSO" });

    let actions = split_stored_rule_action_sql(&rule.ev_action);
    if actions.is_empty() {
        definition.push_str(" NOTHING");
    } else if actions.len() == 1 {
        definition.push(' ');
        definition.push_str(&format_stored_rule_action(
            actions[0],
            relation_name,
            catalog,
        ));
    } else {
        definition.push_str(" (\n");
        for (index, action) in actions.iter().enumerate() {
            definition.push_str("    ");
            definition.push_str(&format_stored_rule_action(action, relation_name, catalog));
            if index + 1 != actions.len() {
                definition.push_str(";\n");
            } else {
                definition.push('\n');
            }
        }
        definition.push(')');
    }

    definition
}

fn format_stored_rule_action(
    action: &str,
    relation_name: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> String {
    match parse_statement(action) {
        Ok(Statement::Insert(stmt)) => {
            format_rule_insert_statement(&stmt, catalog).unwrap_or_else(|| action.to_string())
        }
        Ok(Statement::Update(stmt)) => format_rule_update_statement(&stmt, relation_name, catalog)
            .unwrap_or_else(|| action.to_string()),
        _ => action.to_string(),
    }
}

fn format_rule_update_statement(
    stmt: &UpdateStatement,
    relation_name: &str,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<String> {
    if stmt.with_recursive
        || !stmt.with.is_empty()
        || stmt.target_alias.is_some()
        || stmt.only
        || stmt.from.is_some()
        || !stmt.returning.is_empty()
    {
        return None;
    }

    let catalog = catalog?;
    let relation = catalog.lookup_any_relation(&stmt.table_name)?;
    let ctx = UpdateRuleExprContext {
        relation_name,
        desc: &relation.desc,
        catalog,
    };
    let assignments = stmt
        .assignments
        .iter()
        .map(|assignment| format_rule_update_assignment(assignment, &ctx))
        .collect::<Option<Vec<_>>>()?
        .join(", ");

    let mut sql = format!("UPDATE {} SET {assignments}", stmt.table_name);
    if let Some(where_clause) = &stmt.where_clause {
        sql.push_str("\n  WHERE ");
        sql.push_str(&render_update_rule_expr(where_clause, &ctx, None));
    }
    Some(sql)
}

fn format_rule_update_assignment(
    assignment: &Assignment,
    ctx: &UpdateRuleExprContext<'_>,
) -> Option<String> {
    let target_type =
        resolve_rule_assignment_target_type(&assignment.target, ctx.desc, ctx.catalog);
    Some(format!(
        "{} = {}",
        render_rule_assignment_target(&assignment.target),
        render_update_rule_expr(&assignment.expr, ctx, target_type)
    ))
}

struct UpdateRuleExprContext<'a> {
    relation_name: &'a str,
    desc: &'a RelationDesc,
    catalog: &'a dyn CatalogLookup,
}

fn render_update_rule_expr(
    expr: &SqlExpr,
    ctx: &UpdateRuleExprContext<'_>,
    target_type: Option<SqlType>,
) -> String {
    match expr {
        SqlExpr::Column(name) => render_update_rule_column(name, ctx),
        SqlExpr::IntegerLiteral(value) | SqlExpr::NumericLiteral(value) => {
            render_update_rule_numeric_literal(value, target_type)
        }
        SqlExpr::Const(Value::Int16(value)) => {
            render_update_rule_numeric_literal(&value.to_string(), target_type)
        }
        SqlExpr::Const(Value::Int32(value)) => {
            render_update_rule_numeric_literal(&value.to_string(), target_type)
        }
        SqlExpr::Const(Value::Int64(value)) => {
            render_update_rule_numeric_literal(&value.to_string(), target_type)
        }
        SqlExpr::Const(Value::Float64(value)) => {
            render_update_rule_numeric_literal(&value.to_string(), target_type)
        }
        SqlExpr::Const(Value::Numeric(value)) => {
            render_update_rule_numeric_literal(&value.render(), target_type)
        }
        SqlExpr::Const(Value::Text(value)) => render_rule_string_literal(value, target_type),
        SqlExpr::Add(left, right) => render_update_rule_binary_expr(left, "+", right, ctx),
        SqlExpr::Sub(left, right) => render_update_rule_binary_expr(left, "-", right, ctx),
        SqlExpr::Mul(left, right) => render_update_rule_binary_expr(left, "*", right, ctx),
        SqlExpr::Div(left, right) => render_update_rule_binary_expr(left, "/", right, ctx),
        SqlExpr::Eq(left, right) => render_update_rule_binary_expr(left, "=", right, ctx),
        SqlExpr::NotEq(left, right) => render_update_rule_binary_expr(left, "<>", right, ctx),
        SqlExpr::Lt(left, right) => render_update_rule_binary_expr(left, "<", right, ctx),
        SqlExpr::LtEq(left, right) => render_update_rule_binary_expr(left, "<=", right, ctx),
        SqlExpr::Gt(left, right) => render_update_rule_binary_expr(left, ">", right, ctx),
        SqlExpr::GtEq(left, right) => render_update_rule_binary_expr(left, ">=", right, ctx),
        SqlExpr::Cast(inner, ty) => {
            format!(
                "{}::{}",
                render_update_rule_expr(inner, ctx, None),
                render_raw_type_name(ty)
            )
        }
        SqlExpr::FieldSelect { expr, field } => render_update_rule_field_select(expr, field, ctx),
        SqlExpr::ArraySubscript { array, subscripts } => {
            let mut out = render_update_rule_expr(array, ctx, None);
            for subscript in subscripts {
                out.push('[');
                if let Some(lower) = &subscript.lower {
                    out.push_str(&render_update_rule_expr(lower, ctx, None));
                }
                if subscript.is_slice {
                    out.push(':');
                    if let Some(upper) = &subscript.upper {
                        out.push_str(&render_update_rule_expr(upper, ctx, None));
                    }
                }
                out.push(']');
            }
            out
        }
        SqlExpr::ArrayLiteral(elements) => format!(
            "ARRAY[{}]",
            elements
                .iter()
                .map(|element| render_update_rule_expr(element, ctx, None))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        SqlExpr::Row(fields) => format!(
            "ROW({})",
            fields
                .iter()
                .map(|field| render_update_rule_expr(field, ctx, None))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        SqlExpr::FuncCall { name, args, .. } => render_update_rule_func_call(name, args, ctx),
        _ => render_rule_expr(expr, target_type),
    }
}

fn render_update_rule_column(name: &str, ctx: &UpdateRuleExprContext<'_>) -> String {
    if rule_relation_column_type(name, ctx).is_some() {
        format!("{}.{}", ctx.relation_name, name)
    } else {
        name.to_string()
    }
}

fn render_update_rule_numeric_literal(value: &str, target_type: Option<SqlType>) -> String {
    match target_type.map(|ty| ty.kind) {
        Some(SqlTypeKind::Float8) => format!("{value}::double precision"),
        Some(SqlTypeKind::Float4) => format!("{value}::real"),
        _ => value.to_string(),
    }
}

fn render_update_rule_binary_expr(
    left: &SqlExpr,
    op: &str,
    right: &SqlExpr,
    ctx: &UpdateRuleExprContext<'_>,
) -> String {
    let display_type = rule_update_binary_display_type(left, right, ctx);
    format!(
        "{} {op} {}",
        render_update_rule_expr(left, ctx, display_type),
        render_update_rule_expr(right, ctx, display_type)
    )
}

fn render_update_rule_field_select(
    inner: &SqlExpr,
    field: &str,
    ctx: &UpdateRuleExprContext<'_>,
) -> String {
    if let SqlExpr::Column(name) = inner
        && matches!(name.to_ascii_lowercase().as_str(), "old" | "new")
    {
        return format!("{name}.{field}");
    }

    let rendered = render_update_rule_expr(inner, ctx, None);
    if matches!(inner, SqlExpr::ArraySubscript { .. }) {
        format!("{rendered}.{field}")
    } else {
        format!("({rendered}).{field}")
    }
}

fn render_update_rule_func_call(
    name: &str,
    args: &SqlCallArgs,
    ctx: &UpdateRuleExprContext<'_>,
) -> String {
    if args.is_star() {
        return format!("{name}(*)");
    }
    format!(
        "{}({})",
        name,
        args.args()
            .iter()
            .map(|arg| {
                let rendered = render_update_rule_expr(&arg.value, ctx, None);
                if let Some(name) = &arg.name {
                    format!("{name} => {rendered}")
                } else {
                    rendered
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn rule_update_binary_display_type(
    left: &SqlExpr,
    right: &SqlExpr,
    ctx: &UpdateRuleExprContext<'_>,
) -> Option<SqlType> {
    let left_type = rule_update_expr_type(left, ctx);
    let right_type = rule_update_expr_type(right, ctx);
    left_type
        .filter(|ty| rule_type_should_drive_numeric_literal(*ty))
        .or_else(|| right_type.filter(|ty| rule_type_should_drive_numeric_literal(*ty)))
}

fn rule_type_should_drive_numeric_literal(ty: SqlType) -> bool {
    !ty.is_array && matches!(ty.kind, SqlTypeKind::Float8 | SqlTypeKind::Float4)
}

fn rule_update_expr_type(expr: &SqlExpr, ctx: &UpdateRuleExprContext<'_>) -> Option<SqlType> {
    match expr {
        SqlExpr::Column(name) => rule_relation_column_type(name, ctx),
        SqlExpr::FieldSelect { expr, field } => {
            resolve_rule_field_type(rule_update_expr_type(expr, ctx)?, field, ctx.catalog)
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            let mut current = rule_update_expr_type(array, ctx)?;
            for subscript in subscripts {
                current = rule_navigation_sql_type(current, ctx.catalog);
                if current.is_array {
                    current = if subscript.is_slice {
                        SqlType::array_of(current.element_type())
                    } else {
                        current.element_type()
                    };
                } else {
                    return None;
                }
            }
            Some(current)
        }
        SqlExpr::Cast(_, ty) => ty.as_builtin(),
        SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right) => {
            rule_update_expr_type(left, ctx).or_else(|| rule_update_expr_type(right, ctx))
        }
        _ => None,
    }
}

fn rule_relation_column_type(name: &str, ctx: &UpdateRuleExprContext<'_>) -> Option<SqlType> {
    ctx.desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(name))
        .map(|column| column.sql_type)
}

fn format_rule_insert_statement(
    stmt: &InsertStatement,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<String> {
    if stmt.with_recursive
        || !stmt.with.is_empty()
        || stmt.table_alias.is_some()
        || stmt.on_conflict.is_some()
        || !stmt.returning.is_empty()
    {
        return None;
    }

    let mut sql = format!("INSERT INTO {}", stmt.table_name);
    if let Some(columns) = &stmt.columns {
        sql.push_str(" (");
        sql.push_str(
            &columns
                .iter()
                .map(render_rule_assignment_target)
                .collect::<Vec<_>>()
                .join(", "),
        );
        sql.push(')');
    }

    let target_types = rule_insert_target_types(stmt, catalog);
    match &stmt.source {
        InsertSource::Values(rows) => {
            if rows.len() == 1 {
                sql.push_str("\n  VALUES ");
            } else {
                sql.push_str(" VALUES ");
            }
            sql.push_str(
                &rows
                    .iter()
                    .map(|row| {
                        let separator = if rows.len() == 1 { ", " } else { "," };
                        format!(
                            "({})",
                            row.iter()
                                .enumerate()
                                .map(|(index, expr)| {
                                    render_rule_expr(
                                        expr,
                                        target_types
                                            .as_ref()
                                            .and_then(|types| types.get(index).copied()),
                                    )
                                })
                                .collect::<Vec<_>>()
                                .join(separator)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        InsertSource::Select(select) => {
            sql.push_str("  ");
            sql.push_str(&render_rule_select(select)?);
        }
        InsertSource::DefaultValues => sql.push_str(" DEFAULT VALUES"),
    }
    Some(sql)
}

fn rule_insert_target_types(
    stmt: &InsertStatement,
    catalog: Option<&dyn CatalogLookup>,
) -> Option<Vec<SqlType>> {
    let catalog = catalog?;
    let relation = catalog.lookup_any_relation(&stmt.table_name)?;
    if let Some(columns) = &stmt.columns {
        columns
            .iter()
            .map(|target| resolve_rule_assignment_target_type(target, &relation.desc, catalog))
            .collect()
    } else {
        Some(
            relation
                .desc
                .columns
                .iter()
                .filter(|column| !column.dropped)
                .map(|column| column.sql_type)
                .collect(),
        )
    }
}

fn rule_navigation_sql_type(sql_type: SqlType, catalog: &dyn CatalogLookup) -> SqlType {
    let Some(domain) = catalog.domain_by_type_oid(sql_type.type_oid) else {
        return sql_type;
    };
    if sql_type.is_array && !domain.sql_type.is_array {
        SqlType::array_of(domain.sql_type)
    } else {
        domain.sql_type
    }
}

fn resolve_rule_assignment_target_type(
    target: &AssignmentTarget,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Option<SqlType> {
    let column = desc
        .columns
        .iter()
        .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(&target.column))?;
    let mut current = column.sql_type;
    for step in &target.indirection {
        current = rule_navigation_sql_type(current, catalog);
        match step {
            AssignmentTargetIndirection::Subscript(subscript) => {
                if current.kind == SqlTypeKind::Jsonb && !current.is_array {
                    current = SqlType::new(SqlTypeKind::Jsonb);
                } else if current.kind == SqlTypeKind::Point && !current.is_array {
                    current = SqlType::new(SqlTypeKind::Float8);
                } else if current.is_array {
                    current = if subscript.is_slice {
                        SqlType::array_of(current.element_type())
                    } else {
                        current.element_type()
                    };
                } else {
                    return None;
                }
            }
            AssignmentTargetIndirection::Field(field) => {
                current = resolve_rule_field_type(current, field, catalog)?;
            }
        }
    }
    Some(current)
}

fn resolve_rule_field_type(
    row_type: SqlType,
    field: &str,
    catalog: &dyn CatalogLookup,
) -> Option<SqlType> {
    let row_type = rule_navigation_sql_type(row_type, catalog);
    if matches!(row_type.kind, SqlTypeKind::Composite) && row_type.typrelid != 0 {
        let relation = catalog.lookup_relation_by_oid(row_type.typrelid)?;
        return relation
            .desc
            .columns
            .iter()
            .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(field))
            .map(|column| column.sql_type);
    }
    None
}

fn render_rule_assignment_target(target: &AssignmentTarget) -> String {
    let mut out = target.column.clone();
    for step in &target.indirection {
        match step {
            AssignmentTargetIndirection::Field(field) => {
                out.push('.');
                out.push_str(field);
            }
            AssignmentTargetIndirection::Subscript(subscript) => {
                out.push('[');
                if let Some(lower) = &subscript.lower {
                    out.push_str(&render_rule_expr(lower, None));
                }
                if subscript.is_slice {
                    out.push(':');
                    if let Some(upper) = &subscript.upper {
                        out.push_str(&render_rule_expr(upper, None));
                    }
                }
                out.push(']');
            }
        }
    }
    out
}

fn render_rule_select(select: &SelectStatement) -> Option<String> {
    if select.with_recursive
        || !select.with.is_empty()
        || select.distinct
        || select.from.is_some()
        || select.where_clause.is_some()
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.window_clauses.is_empty()
        || !select.order_by.is_empty()
        || select.limit.is_some()
        || select.offset.is_some()
        || select.locking_clause.is_some()
        || select.set_operation.is_some()
    {
        return None;
    }
    Some(format!(
        "SELECT {}",
        select
            .targets
            .iter()
            .map(|target| render_rule_expr(&target.expr, None))
            .collect::<Vec<_>>()
            .join(",\n            ")
    ))
}

fn render_rule_expr(expr: &SqlExpr, target_type: Option<SqlType>) -> String {
    match expr {
        SqlExpr::Column(name) => name.clone(),
        SqlExpr::Default => "DEFAULT".into(),
        SqlExpr::IntegerLiteral(value) | SqlExpr::NumericLiteral(value) => value.clone(),
        SqlExpr::Const(Value::Int16(value)) => value.to_string(),
        SqlExpr::Const(Value::Int32(value)) => value.to_string(),
        SqlExpr::Const(Value::Int64(value)) => value.to_string(),
        SqlExpr::Const(Value::Text(value)) => render_rule_string_literal(value, target_type),
        SqlExpr::Const(Value::TextRef(_, _)) => "''".into(),
        SqlExpr::Const(Value::Null) => "NULL".into(),
        SqlExpr::Cast(inner, ty) => {
            format!(
                "{}::{}",
                render_rule_expr(inner, None),
                render_raw_type_name(ty)
            )
        }
        SqlExpr::Add(left, right) => render_rule_binary_expr(left, "+", right),
        SqlExpr::Sub(left, right) => render_rule_binary_expr(left, "-", right),
        SqlExpr::Mul(left, right) => render_rule_binary_expr(left, "*", right),
        SqlExpr::Div(left, right) => render_rule_binary_expr(left, "/", right),
        SqlExpr::Eq(left, right) => render_rule_binary_expr(left, "=", right),
        SqlExpr::NotEq(left, right) => render_rule_binary_expr(left, "<>", right),
        SqlExpr::Lt(left, right) => render_rule_binary_expr(left, "<", right),
        SqlExpr::LtEq(left, right) => render_rule_binary_expr(left, "<=", right),
        SqlExpr::Gt(left, right) => render_rule_binary_expr(left, ">", right),
        SqlExpr::GtEq(left, right) => render_rule_binary_expr(left, ">=", right),
        SqlExpr::ArrayLiteral(elements) => format!(
            "ARRAY[{}]",
            elements
                .iter()
                .map(|element| render_rule_expr(element, None))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        SqlExpr::Row(fields) => format!(
            "ROW({})",
            fields
                .iter()
                .map(|field| render_rule_expr(field, None))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        SqlExpr::FuncCall { name, args, .. } => render_rule_func_call(name, args),
        SqlExpr::FieldSelect { expr, field } => {
            format!("{}.{}", render_rule_expr(expr, None), field)
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            let mut out = render_rule_expr(array, None);
            for subscript in subscripts {
                out.push('[');
                if let Some(lower) = &subscript.lower {
                    out.push_str(&render_rule_expr(lower, None));
                }
                if subscript.is_slice {
                    out.push(':');
                    if let Some(upper) = &subscript.upper {
                        out.push_str(&render_rule_expr(upper, None));
                    }
                }
                out.push(']');
            }
            out
        }
        _ => format!("{expr:?}"),
    }
}

fn render_rule_binary_expr(left: &SqlExpr, op: &str, right: &SqlExpr) -> String {
    format!(
        "{} {} {}",
        render_rule_expr(left, None),
        op,
        render_rule_expr(right, None)
    )
}

fn render_rule_string_literal(value: &str, target_type: Option<SqlType>) -> String {
    let mut rendered = format!("'{}'", value.replace('\'', "''"));
    if target_type.is_some_and(|ty| !ty.is_array && ty.kind == SqlTypeKind::Text) {
        rendered.push_str("::text");
    }
    rendered
}

fn render_rule_func_call(name: &str, args: &SqlCallArgs) -> String {
    if args.is_star() {
        return format!("{name}(*)");
    }
    format!(
        "{}({})",
        name,
        args.args()
            .iter()
            .map(|arg| {
                let rendered = render_rule_expr(&arg.value, None);
                if let Some(name) = &arg.name {
                    format!("{name} => {rendered}")
                } else {
                    rendered
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn render_raw_type_name(ty: &RawTypeName) -> String {
    match ty {
        RawTypeName::Builtin(ty) => sql_type_name(*ty),
        RawTypeName::Named { name, array_bounds } => {
            format!("{}{}", name, "[]".repeat(*array_bounds))
        }
        RawTypeName::Serial(kind) => format!("{kind:?}").to_ascii_lowercase(),
        RawTypeName::Record => "record".into(),
    }
}

fn format_rule_event(ev_type: char) -> &'static str {
    match ev_type {
        '1' => "SELECT",
        '2' => "UPDATE",
        '3' => "INSERT",
        '4' => "DELETE",
        _ => "UNKNOWN",
    }
}
