use crate::backend::parser::analyze::analyze_select_query_with_outer;
use crate::backend::parser::{CatalogLookup, ParseError, SqlType, SqlTypeKind, Statement};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{
    JoinTreeNode, Query, RangeTblEntryKind, SelectStatement, ViewCheckOption,
};
use crate::include::nodes::primnodes::{
    Aggref, BoolExprType, BuiltinScalarFunction, Expr, FuncExpr, JoinType, OpExprKind,
    RelationDesc, ScalarFunctionImpl, TargetEntry, Var, attrno_index,
};

const RETURN_RULE_NAME: &str = "_RETURN";

fn view_display_name(relation_oid: u32, alias: Option<&str>) -> String {
    alias
        .map(str::to_string)
        .unwrap_or_else(|| format!("view {relation_oid}"))
}

fn return_rule_sql(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    display_name: &str,
) -> Result<String, ParseError> {
    let mut rows = catalog.rewrite_rows_for_relation(relation_oid);
    rows.retain(|row| row.rulename == RETURN_RULE_NAME);
    match rows.as_slice() {
        [row] => Ok(row.ev_action.clone()),
        [] => Err(ParseError::UnexpectedToken {
            expected: "view _RETURN rule",
            actual: format!("missing rewrite rule for view {display_name}"),
        }),
        _ => Err(ParseError::UnexpectedToken {
            expected: "single view _RETURN rule",
            actual: format!("multiple rewrite rules for view {display_name}"),
        }),
    }
}

pub(crate) fn split_stored_view_definition_sql(sql: &str) -> (&str, ViewCheckOption) {
    let normalized = sql.trim().trim_end_matches(';').trim();
    let lowered = normalized.to_ascii_lowercase();

    if lowered.ends_with("with local check option") {
        let cutoff = normalized.len() - "with local check option".len();
        return (normalized[..cutoff].trim(), ViewCheckOption::Local);
    }
    if lowered.ends_with("with cascaded check option") {
        let cutoff = normalized.len() - "with cascaded check option".len();
        return (normalized[..cutoff].trim(), ViewCheckOption::Cascaded);
    }
    if lowered.ends_with("with check option") {
        let cutoff = normalized.len() - "with check option".len();
        return (normalized[..cutoff].trim(), ViewCheckOption::Cascaded);
    }
    (normalized, ViewCheckOption::None)
}

pub(crate) fn format_view_definition(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<String, ParseError> {
    let query = load_view_return_query(relation_oid, relation_desc, None, catalog, &[])?;
    Ok(render_view_query(&query, catalog))
}

fn validate_view_shape(
    query: &Query,
    relation_desc: &RelationDesc,
    display_name: &str,
) -> Result<(), ParseError> {
    let actual_columns = query.columns();
    if actual_columns.len() != relation_desc.columns.len() {
        return Err(ParseError::UnexpectedToken {
            expected: "view query width matching stored view columns",
            actual: format!("stale view definition for {display_name}"),
        });
    }
    for (actual_column, stored_column) in
        actual_columns.into_iter().zip(relation_desc.columns.iter())
    {
        if !actual_column.name.eq_ignore_ascii_case(&stored_column.name)
            || actual_column.sql_type != stored_column.sql_type
        {
            return Err(ParseError::UnexpectedToken {
                expected: "view query columns matching stored view descriptor",
                actual: format!("stale view definition for {display_name}"),
            });
        }
    }
    Ok(())
}

pub(crate) fn load_view_return_query(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    alias: Option<&str>,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<Query, ParseError> {
    let select = load_view_return_select(relation_oid, alias, catalog, expanded_views)?;
    let mut next_views = expanded_views.to_vec();
    next_views.push(relation_oid);
    let (query, _) =
        analyze_select_query_with_outer(&select, catalog, &[], None, &[], &next_views)?;
    let display_name = view_display_name(relation_oid, alias);
    validate_view_shape(&query, relation_desc, &display_name)?;
    Ok(query)
}

pub(crate) fn load_view_return_select(
    relation_oid: u32,
    alias: Option<&str>,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<SelectStatement, ParseError> {
    let display_name = view_display_name(relation_oid, alias);
    if expanded_views.contains(&relation_oid) {
        return Err(ParseError::RecursiveView(display_name));
    }
    let sql = return_rule_sql(catalog, relation_oid, &display_name)?;
    let (sql, _) = split_stored_view_definition_sql(&sql);
    // :HACK: PostgreSQL stores analyzed rule query trees in `pg_rewrite`.
    // pgrust still stores SQL text and reparses it here until the catalog
    // format is upgraded to preserve analyzed query trees directly.
    let stmt = crate::backend::parser::parse_statement(&sql)?;
    let Statement::Select(select) = stmt else {
        return Err(ParseError::UnexpectedToken {
            expected: "SELECT view definition",
            actual: sql.to_string(),
        });
    };
    Ok(select)
}

pub(crate) fn rewrite_view_relation_query(
    relation_oid: u32,
    relation_desc: &RelationDesc,
    alias: Option<&str>,
    catalog: &dyn CatalogLookup,
    expanded_views: &[u32],
) -> Result<Query, ParseError> {
    load_view_return_query(relation_oid, relation_desc, alias, catalog, expanded_views)
}

fn render_view_query(query: &Query, catalog: &dyn CatalogLookup) -> String {
    let mut lines = vec![format!(
        " SELECT {}",
        query
            .target_list
            .iter()
            .filter(|target| !target.resjunk)
            .map(|target| render_target_entry(target, query, catalog))
            .collect::<Vec<_>>()
            .join(", ")
    )];

    if let Some(jointree) = &query.jointree {
        lines.push(format!(
            "   FROM {}",
            render_from_node(query, jointree, catalog, 3)
        ));
    }
    if let Some(where_qual) = &query.where_qual {
        lines.push(format!(
            "  WHERE {}",
            render_expr(where_qual, query, catalog)
        ));
    }
    if !query.group_by.is_empty() {
        lines.push(format!(
            "  GROUP BY {}",
            query
                .group_by
                .iter()
                .map(|expr| render_expr(expr, query, catalog))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(having_qual) = &query.having_qual {
        lines.push(format!(
            "  HAVING {}",
            render_expr(having_qual, query, catalog)
        ));
    }
    lines.join("\n") + ";"
}

fn render_target_entry(target: &TargetEntry, query: &Query, catalog: &dyn CatalogLookup) -> String {
    let rendered = match &target.expr {
        Expr::Var(var) if join_using_var_needs_cast(var, target.sql_type, query) => format!(
            "({})::{}",
            var_name(var, query).unwrap_or_else(|| format!("var{}", var.varattno)),
            render_sql_type(target.sql_type)
        ),
        _ => render_expr(&target.expr, query, catalog),
    };
    if rendered == quote_identifier_if_needed(&target.name) {
        rendered
    } else {
        format!("{rendered} AS {}", quote_identifier_if_needed(&target.name))
    }
}

fn render_from_node(
    query: &Query,
    node: &JoinTreeNode,
    catalog: &dyn CatalogLookup,
    indent: usize,
) -> String {
    match node {
        JoinTreeNode::RangeTblRef(index) => render_rte(query, *index, catalog),
        JoinTreeNode::JoinExpr {
            left,
            right,
            kind,
            quals,
            rtindex,
        } => {
            let left_sql = render_from_node(query, left, catalog, indent + 3);
            let right_sql = render_from_node(query, right, catalog, indent + 3);
            let using_cols = query
                .rtable
                .get(rtindex.saturating_sub(1))
                .and_then(|rte| match &rte.kind {
                    RangeTblEntryKind::Join { joinmergedcols, .. } => Some(
                        rte.desc
                            .columns
                            .iter()
                            .take(*joinmergedcols)
                            .map(|column| quote_identifier_if_needed(&column.name))
                            .collect::<Vec<_>>(),
                    ),
                    _ => None,
                })
                .unwrap_or_default();
            let constraint = if using_cols.is_empty() {
                format!("ON {}", render_expr(quals, query, catalog))
            } else {
                format!("USING ({})", using_cols.join(", "))
            };
            format!(
                "({left_sql}\n{}{} JOIN {} {})",
                " ".repeat(indent + 3),
                render_join_type(*kind),
                right_sql,
                constraint
            )
        }
    }
}

fn render_rte(query: &Query, index: usize, catalog: &dyn CatalogLookup) -> String {
    let Some(rte) = query.rtable.get(index.saturating_sub(1)) else {
        return format!("rte{index}");
    };
    match &rte.kind {
        RangeTblEntryKind::Relation { relation_oid, .. } => {
            let base = relation_sql_name(*relation_oid, catalog)
                .unwrap_or_else(|| format!("rel{relation_oid}"));
            let relname = catalog
                .class_row_by_oid(*relation_oid)
                .map(|class| class.relname)
                .unwrap_or_default();
            if let Some(alias) = &rte.alias
                && !alias.eq_ignore_ascii_case(&relname)
            {
                format!("{base} {}", quote_identifier_if_needed(alias))
            } else {
                base
            }
        }
        RangeTblEntryKind::Subquery { query } => {
            let rendered = render_view_query(query, catalog);
            let body = rendered.strip_suffix(';').unwrap_or(&rendered);
            match &rte.alias {
                Some(alias) => format!("({body}) {}", quote_identifier_if_needed(alias)),
                None => format!("({body})"),
            }
        }
        RangeTblEntryKind::Join { .. } => rte.alias.clone().unwrap_or_else(|| "join".into()),
        RangeTblEntryKind::Values { .. } => "(VALUES (...))".into(),
        RangeTblEntryKind::Function { .. } => "function_call".into(),
        RangeTblEntryKind::Result => "(RESULT)".into(),
        RangeTblEntryKind::WorkTable { worktable_id } => format!("worktable {worktable_id}"),
        RangeTblEntryKind::Cte { cte_id, .. } => format!("cte {cte_id}"),
    }
}

fn render_expr(expr: &Expr, query: &Query, catalog: &dyn CatalogLookup) -> String {
    match expr {
        Expr::Var(var) => var_name(var, query).unwrap_or_else(|| format!("var{}", var.varattno)),
        Expr::Const(value) => render_literal(value),
        Expr::Cast(inner, ty) => {
            if matches!(**inner, Expr::Const(_)) {
                format!(
                    "{}::{}",
                    render_expr(inner, query, catalog),
                    render_sql_type(*ty)
                )
            } else {
                format!(
                    "({})::{}",
                    render_expr(inner, query, catalog),
                    render_sql_type(*ty)
                )
            }
        }
        Expr::Aggref(aggref) => render_aggregate(aggref, query, catalog),
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            BoolExprType::Not => format!(
                "NOT {}",
                render_wrapped_expr(&bool_expr.args[0], query, catalog)
            ),
            BoolExprType::And => bool_expr
                .args
                .iter()
                .map(|arg| render_wrapped_expr(arg, query, catalog))
                .collect::<Vec<_>>()
                .join(" AND "),
            BoolExprType::Or => bool_expr
                .args
                .iter()
                .map(|arg| render_wrapped_expr(arg, query, catalog))
                .collect::<Vec<_>>()
                .join(" OR "),
        },
        Expr::Op(op) => render_op(op.op, &op.args, query, catalog),
        Expr::Func(func) => render_function(func, query, catalog),
        Expr::IsNull(inner) => format!("{} IS NULL", render_wrapped_expr(inner, query, catalog)),
        Expr::IsNotNull(inner) => {
            format!("{} IS NOT NULL", render_wrapped_expr(inner, query, catalog))
        }
        Expr::IsDistinctFrom(left, right) => format!(
            "{} IS DISTINCT FROM {}",
            render_wrapped_expr(left, query, catalog),
            render_wrapped_expr(right, query, catalog)
        ),
        Expr::IsNotDistinctFrom(left, right) => format!(
            "{} IS NOT DISTINCT FROM {}",
            render_wrapped_expr(left, query, catalog),
            render_wrapped_expr(right, query, catalog)
        ),
        Expr::Coalesce(left, right) => format!(
            "COALESCE({}, {})",
            render_expr(left, query, catalog),
            render_expr(right, query, catalog)
        ),
        _ => format!("{expr:?}"),
    }
}

fn render_wrapped_expr(expr: &Expr, query: &Query, catalog: &dyn CatalogLookup) -> String {
    match expr {
        Expr::Op(_) | Expr::Bool(_) => format!("({})", render_expr(expr, query, catalog)),
        _ => render_expr(expr, query, catalog),
    }
}

fn render_function(func: &FuncExpr, query: &Query, catalog: &dyn CatalogLookup) -> String {
    let name = match func.implementation {
        ScalarFunctionImpl::Builtin(builtin) => render_builtin_function_name(builtin).into(),
        ScalarFunctionImpl::UserDefined { proc_oid } => catalog
            .proc_row_by_oid(proc_oid)
            .map(|row| row.proname)
            .unwrap_or_else(|| format!("proc_{proc_oid}")),
    };
    format!(
        "{}({})",
        name,
        func.args
            .iter()
            .map(|arg| render_expr(arg, query, catalog))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn render_aggregate(aggref: &Aggref, query: &Query, catalog: &dyn CatalogLookup) -> String {
    let name = catalog
        .proc_row_by_oid(aggref.aggfnoid)
        .map(|row| row.proname)
        .unwrap_or_else(|| format!("agg_{}", aggref.aggfnoid));
    let mut args = aggref
        .args
        .iter()
        .map(|arg| render_expr(arg, query, catalog))
        .collect::<Vec<_>>();
    if aggref.aggdistinct && !args.is_empty() {
        args[0] = format!("DISTINCT {}", args[0]);
    }
    format!("{name}({})", args.join(", "))
}

fn render_op(op: OpExprKind, args: &[Expr], query: &Query, catalog: &dyn CatalogLookup) -> String {
    match (op, args) {
        (OpExprKind::UnaryPlus, [arg]) => format!("+{}", render_wrapped_expr(arg, query, catalog)),
        (OpExprKind::Negate, [arg]) => format!("-{}", render_wrapped_expr(arg, query, catalog)),
        (OpExprKind::BitNot, [arg]) => format!("~{}", render_wrapped_expr(arg, query, catalog)),
        (_, [left, right]) => format!(
            "{} {} {}",
            render_wrapped_expr(left, query, catalog),
            render_binary_operator(op),
            render_wrapped_expr(right, query, catalog)
        ),
        _ => format!("{op:?}"),
    }
}

fn render_binary_operator(op: OpExprKind) -> &'static str {
    match op {
        OpExprKind::Add => "+",
        OpExprKind::Sub => "-",
        OpExprKind::Mul => "*",
        OpExprKind::Div => "/",
        OpExprKind::Mod => "%",
        OpExprKind::BitAnd => "&",
        OpExprKind::BitOr => "|",
        OpExprKind::BitXor => "#",
        OpExprKind::Shl => "<<",
        OpExprKind::Shr => ">>",
        OpExprKind::Concat => "||",
        OpExprKind::Eq => "=",
        OpExprKind::NotEq => "<>",
        OpExprKind::Lt => "<",
        OpExprKind::LtEq => "<=",
        OpExprKind::Gt => ">",
        OpExprKind::GtEq => ">=",
        OpExprKind::RegexMatch => "~",
        OpExprKind::ArrayOverlap => "&&",
        OpExprKind::ArrayContains => "@>",
        OpExprKind::ArrayContained => "<@",
        OpExprKind::JsonbContains => "@>",
        OpExprKind::JsonbContained => "<@",
        OpExprKind::JsonbExists => "?",
        OpExprKind::JsonbExistsAny => "?|",
        OpExprKind::JsonbExistsAll => "?&",
        OpExprKind::JsonbPathExists => "@?",
        OpExprKind::JsonbPathMatch => "@@",
        OpExprKind::JsonGet => "->",
        OpExprKind::JsonGetText => "->>",
        OpExprKind::JsonPath => "#>",
        OpExprKind::JsonPathText => "#>>",
        OpExprKind::UnaryPlus | OpExprKind::Negate | OpExprKind::BitNot => "",
    }
}

fn render_join_type(kind: JoinType) -> &'static str {
    match kind {
        JoinType::Inner => "INNER",
        JoinType::Cross => "CROSS",
        JoinType::Left => "LEFT",
        JoinType::Right => "RIGHT",
        JoinType::Full => "FULL",
    }
}

fn render_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Bool(true) => "true".into(),
        Value::Bool(false) => "false".into(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Text(_) | Value::TextRef(_, _) => {
            format!(
                "'{}'",
                value.as_text().unwrap_or_default().replace('\'', "''")
            )
        }
        Value::Numeric(numeric) => numeric.render(),
        other => format!("{other:?}"),
    }
}

fn render_sql_type(ty: SqlType) -> &'static str {
    if ty.is_array {
        return "array";
    }
    match ty.kind {
        SqlTypeKind::Bool => "boolean",
        SqlTypeKind::Int2 => "smallint",
        SqlTypeKind::Int4 => "integer",
        SqlTypeKind::Int8 => "bigint",
        SqlTypeKind::Float4 => "real",
        SqlTypeKind::Float8 => "double precision",
        SqlTypeKind::Numeric => "numeric",
        SqlTypeKind::Text => "text",
        SqlTypeKind::Name => "name",
        SqlTypeKind::Oid => "oid",
        SqlTypeKind::RegClass => "regclass",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        _ => "text",
    }
}

fn var_name(var: &Var, query: &Query) -> Option<String> {
    let column_index = attrno_index(var.varattno)?;
    query
        .rtable
        .get(var.varno.checked_sub(1)?)
        .and_then(|rte| rte.desc.columns.get(column_index))
        .map(|column| quote_identifier_if_needed(&column.name))
}

fn relation_sql_name(relation_oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    catalog
        .class_row_by_oid(relation_oid)
        .map(|class| quote_identifier_if_needed(&class.relname))
}

fn join_using_var_needs_cast(var: &Var, sql_type: SqlType, query: &Query) -> bool {
    let Some(column_index) = attrno_index(var.varattno) else {
        return false;
    };
    let Some(rte) = query.rtable.get(var.varno.saturating_sub(1)) else {
        return false;
    };
    match &rte.kind {
        RangeTblEntryKind::Join {
            jointype,
            joinmergedcols,
            ..
        } => {
            matches!(jointype, JoinType::Left | JoinType::Right | JoinType::Full)
                && column_index < *joinmergedcols
                && var.vartype == sql_type
        }
        _ => false,
    }
}

fn render_builtin_function_name(func: BuiltinScalarFunction) -> &'static str {
    match func {
        BuiltinScalarFunction::CurrentDatabase => "current_database",
        BuiltinScalarFunction::PgGetUserById => "pg_get_userbyid",
        BuiltinScalarFunction::PgGetExpr => "pg_get_expr",
        BuiltinScalarFunction::PgGetViewDef => "pg_get_viewdef",
        _ => "function",
    }
}

fn quote_identifier_if_needed(identifier: &str) -> String {
    let needs_quotes = identifier.is_empty()
        || identifier.chars().enumerate().any(|(index, ch)| {
            !(ch == '_' || ch.is_ascii_alphanumeric()) || (index == 0 && ch.is_ascii_digit())
        })
        || identifier != identifier.to_ascii_lowercase();
    if needs_quotes {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    } else {
        identifier.to_string()
    }
}
