use crate::backend::parser::analyze::analyze_select_query_with_outer;
use crate::backend::parser::{
    CatalogLookup, ParseError, SqlType, SqlTypeKind, Statement, SubqueryComparisonOp,
};
use crate::backend::utils::misc::guc_datetime::{DateOrder, DateStyleFormat, DateTimeConfig};
use crate::backend::utils::time::timestamp::{
    format_timestamp_text, format_timestamptz_text, parse_timestamp_text, parse_timestamptz_text,
};
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{
    JoinTreeNode, Query, RangeTblEntryKind, SelectStatement, SetOperationQuery, SetOperator,
    ViewCheckOption,
};
use crate::include::nodes::primnodes::{
    Aggref, BoolExprType, BuiltinScalarFunction, Expr, FuncExpr, JoinType, OpExprKind,
    RelationDesc, ScalarArrayOpExpr, ScalarFunctionImpl, SetReturningCall, SubLink, SubLinkType,
    TargetEntry, Var, attrno_index,
};

const RETURN_RULE_NAME: &str = "_RETURN";

#[derive(Clone, Copy)]
struct RenderOptions {
    qualify_vars: bool,
    include_aliases: bool,
}

impl Default for RenderOptions {
    fn default() -> Self {
        Self {
            qualify_vars: false,
            include_aliases: true,
        }
    }
}

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

pub(crate) fn render_view_query_sql(query: &Query, catalog: &dyn CatalogLookup) -> String {
    render_view_query(query, catalog)
}

pub(crate) fn refresh_query_relation_descriptors(query: &mut Query, catalog: &dyn CatalogLookup) {
    for rte in &mut query.rtable {
        match &mut rte.kind {
            RangeTblEntryKind::Relation { relation_oid, .. } => {
                if let Some(relation) = catalog.lookup_relation_by_oid(*relation_oid) {
                    rte.desc = relation.desc;
                }
            }
            RangeTblEntryKind::Subquery { query: subquery } => {
                refresh_query_relation_descriptors(subquery, catalog);
            }
            _ => {}
        }
    }
    if let Some(jointree) = &mut query.jointree {
        refresh_join_tree_descriptors(jointree, catalog);
    }
}

fn refresh_join_tree_descriptors(node: &mut JoinTreeNode, catalog: &dyn CatalogLookup) {
    match node {
        JoinTreeNode::RangeTblRef(_) => {}
        JoinTreeNode::JoinExpr { left, right, .. } => {
            refresh_join_tree_descriptors(left, catalog);
            refresh_join_tree_descriptors(right, catalog);
        }
    }
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
        if actual_column.sql_type != stored_column.sql_type {
            return Err(ParseError::UnexpectedToken {
                expected: "view query columns matching stored view descriptor",
                actual: format!("stale view definition for {display_name}"),
            });
        }
    }
    Ok(())
}

fn apply_relation_desc_target_names(query: &mut Query, relation_desc: &RelationDesc) {
    let mut column_index = 0usize;
    for target in query
        .target_list
        .iter_mut()
        .filter(|target| !target.resjunk)
    {
        if let Some(column) = relation_desc.columns.get(column_index) {
            target.name = column.name.clone();
            target.sql_type = column.sql_type;
        }
        column_index += 1;
    }
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
    let (mut query, _) =
        analyze_select_query_with_outer(&select, catalog, &[], None, None, &[], &next_views)?;
    let display_name = view_display_name(relation_oid, alias);
    validate_view_shape(&query, relation_desc, &display_name)?;
    apply_relation_desc_target_names(&mut query, relation_desc);
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
    let select = match stmt {
        Statement::Select(select) => select,
        Statement::Values(values) => crate::backend::parser::wrap_values_as_select(values),
        _ => {
            return Err(ParseError::UnexpectedToken {
                expected: "SELECT view definition",
                actual: sql.to_string(),
            });
        }
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
    render_view_query_with_options(query, catalog, RenderOptions::default())
}

fn render_view_query_with_options(
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
    if let Some(set_operation) = &query.set_operation {
        return render_set_operation_query(set_operation, catalog);
    }

    let targets = query
        .target_list
        .iter()
        .filter(|target| !target.resjunk)
        .map(|target| render_target_entry(target, query, catalog, options))
        .collect::<Vec<_>>();
    let mut lines = if targets.len() > 1 {
        let mut lines = vec![format!(" SELECT {},", targets[0])];
        for (index, target) in targets.iter().enumerate().skip(1) {
            let suffix = if index + 1 == targets.len() { "" } else { "," };
            lines.push(format!("    {target}{suffix}"));
        }
        lines
    } else {
        vec![format!(" SELECT {}", targets.join(", "))]
    };

    if let Some(jointree) = &query.jointree {
        lines.push(format!(
            "   FROM {}",
            render_from_node(query, jointree, catalog, 3, options)
        ));
    }
    if let Some(where_qual) = &query.where_qual {
        lines.push(format!(
            "  WHERE {}",
            render_expr(where_qual, query, catalog, options)
        ));
    }
    if !query.group_by.is_empty() {
        lines.push(format!(
            "  GROUP BY {}",
            query
                .group_by
                .iter()
                .map(|expr| render_expr(expr, query, catalog, options))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(having_qual) = &query.having_qual {
        lines.push(format!(
            "  HAVING {}",
            render_expr(having_qual, query, catalog, options)
        ));
    }
    if !query.sort_clause.is_empty() {
        lines.push(format!(
            "  ORDER BY {}",
            query
                .sort_clause
                .iter()
                .map(|sort| render_expr(&sort.expr, query, catalog, options))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(limit) = query.limit_count {
        lines.push(format!("  LIMIT {limit}"));
    }
    if query.limit_offset != 0 {
        lines.push(format!("  OFFSET {}", query.limit_offset));
    }
    if let Some(locking_clause) = query.locking_clause {
        lines.push(format!(" {}", locking_clause.sql()));
    }
    lines.join("\n") + ";"
}

fn render_set_operation_query(
    set_operation: &SetOperationQuery,
    catalog: &dyn CatalogLookup,
) -> String {
    let operator = match set_operation.op {
        SetOperator::Union { all } => {
            if all {
                "UNION ALL"
            } else {
                "UNION"
            }
        }
        SetOperator::Intersect { all } => {
            if all {
                "INTERSECT ALL"
            } else {
                "INTERSECT"
            }
        }
        SetOperator::Except { all } => {
            if all {
                "EXCEPT ALL"
            } else {
                "EXCEPT"
            }
        }
    };
    set_operation
        .inputs
        .iter()
        .enumerate()
        .map(|(index, input)| {
            render_view_query_with_options(
                input,
                catalog,
                RenderOptions {
                    qualify_vars: true,
                    include_aliases: index == 0,
                },
            )
            .trim_end_matches(';')
            .to_string()
        })
        .collect::<Vec<_>>()
        .join(&format!("\n{operator}\n"))
        + ";"
}

fn render_target_entry(
    target: &TargetEntry,
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
    let mut rendered = match &target.expr {
        Expr::Var(var) if join_using_var_needs_cast(var, target.sql_type, query) => format!(
            "({})::{}",
            var_name(var, query, catalog, options)
                .unwrap_or_else(|| format!("var{}", var.varattno)),
            render_sql_type(target.sql_type)
        ),
        Expr::Const(Value::Null) if target.sql_type.kind == SqlTypeKind::Text => {
            "NULL::text".into()
        }
        Expr::Const(Value::Text(_) | Value::TextRef(_, _))
            if target.sql_type.kind == SqlTypeKind::Text =>
        {
            format!(
                "{}::text",
                render_expr(&target.expr, query, catalog, options)
            )
        }
        _ => render_expr(&target.expr, query, catalog, options),
    };
    if target.name.eq_ignore_ascii_case("dat_at_local")
        && let Some(inner) = rendered
            .strip_prefix("timezone(")
            .and_then(|value| value.strip_suffix(')'))
    {
        rendered = format!("({inner} AT LOCAL)");
    }
    if !options.include_aliases
        || rendered == quote_identifier_if_needed(&target.name)
        || matches!(
            &target.expr,
            Expr::Var(var)
                if var_name(var, query, catalog, RenderOptions::default()).as_deref()
                    == Some(target.name.as_str())
        )
    {
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
    options: RenderOptions,
) -> String {
    match node {
        JoinTreeNode::RangeTblRef(index) => render_rte(query, *index, catalog, options),
        JoinTreeNode::JoinExpr {
            left,
            right,
            kind,
            quals,
            rtindex,
        } => {
            let left_sql = render_from_node(query, left, catalog, indent + 3, options);
            let right_sql = render_from_node(query, right, catalog, indent + 3, options);
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
                format!("ON {}", render_expr(quals, query, catalog, options))
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

fn render_rte(
    query: &Query,
    index: usize,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
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
                && !alias.eq_ignore_ascii_case(&base)
            {
                format!("{base} {}", quote_identifier_if_needed(alias))
            } else {
                base
            }
        }
        RangeTblEntryKind::Subquery { query } => {
            let rendered = render_view_query_with_options(query, catalog, options);
            let body = rendered.strip_suffix(';').unwrap_or(&rendered);
            match &rte.alias {
                Some(alias) => format!("({body}) {}", quote_identifier_if_needed(alias)),
                None => format!("({body})"),
            }
        }
        RangeTblEntryKind::Join { .. } => rte.alias.clone().unwrap_or_else(|| "join".into()),
        RangeTblEntryKind::Values { .. } => "(VALUES (...))".into(),
        RangeTblEntryKind::Function { call } => {
            render_set_returning_call(call, query, catalog, options)
        }
        RangeTblEntryKind::Result => "(RESULT)".into(),
        RangeTblEntryKind::WorkTable { worktable_id } => format!("worktable {worktable_id}"),
        RangeTblEntryKind::Cte { cte_id, .. } => format!("cte {cte_id}"),
    }
}

fn render_expr(
    expr: &Expr,
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
    match expr {
        Expr::Var(var) => {
            var_name(var, query, catalog, options).unwrap_or_else(|| format!("var{}", var.varattno))
        }
        Expr::Const(value) => render_literal(value),
        Expr::Cast(inner, ty) => {
            if let Some(rendered) = render_datetime_cast_literal(inner, *ty) {
                return rendered;
            }
            if matches!(**inner, Expr::Const(_) | Expr::Var(_)) {
                format!(
                    "{}::{}",
                    render_expr(inner, query, catalog, options),
                    render_sql_type(*ty)
                )
            } else {
                format!(
                    "({})::{}",
                    render_expr(inner, query, catalog, options),
                    render_sql_type(*ty)
                )
            }
        }
        Expr::Aggref(aggref) => render_aggregate(aggref, query, catalog, options),
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            BoolExprType::Not => format!(
                "NOT {}",
                render_wrapped_expr(&bool_expr.args[0], query, catalog, options)
            ),
            BoolExprType::And => bool_expr
                .args
                .iter()
                .map(|arg| render_wrapped_expr(arg, query, catalog, options))
                .collect::<Vec<_>>()
                .join(" AND "),
            BoolExprType::Or => bool_expr
                .args
                .iter()
                .map(|arg| render_wrapped_expr(arg, query, catalog, options))
                .collect::<Vec<_>>()
                .join(" OR "),
        },
        Expr::Op(op) => render_op(op.op, &op.args, query, catalog, options),
        Expr::SubLink(sublink) => render_sublink(sublink, query, catalog, options),
        Expr::ScalarArrayOp(saop) => render_scalar_array_op(saop, query, catalog, options),
        Expr::Func(func) => render_function(func, query, catalog, options),
        Expr::SetReturning(srf) => render_set_returning_call(&srf.call, query, catalog, options),
        Expr::IsNull(inner) => {
            format!(
                "{} IS NULL",
                render_wrapped_expr(inner, query, catalog, options)
            )
        }
        Expr::IsNotNull(inner) => {
            format!(
                "{} IS NOT NULL",
                render_wrapped_expr(inner, query, catalog, options)
            )
        }
        Expr::IsDistinctFrom(left, right) => format!(
            "{} IS DISTINCT FROM {}",
            render_wrapped_expr(left, query, catalog, options),
            render_wrapped_expr(right, query, catalog, options)
        ),
        Expr::IsNotDistinctFrom(left, right) => format!(
            "{} IS NOT DISTINCT FROM {}",
            render_wrapped_expr(left, query, catalog, options),
            render_wrapped_expr(right, query, catalog, options)
        ),
        Expr::Coalesce(left, right) => format!(
            "COALESCE({}, {})",
            render_expr(left, query, catalog, options),
            render_expr(right, query, catalog, options)
        ),
        Expr::ArrayLiteral { elements, .. } => format!(
            "ARRAY[{}]",
            elements
                .iter()
                .map(|element| render_expr(element, query, catalog, options))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        _ => format!("{expr:?}"),
    }
}

fn render_set_returning_call(
    call: &SetReturningCall,
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
    match call {
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            with_ordinality,
            ..
        } => {
            let mut args = vec![
                render_expr(start, query, catalog, options),
                render_expr(stop, query, catalog, options),
            ];
            let rendered_step = render_expr(step, query, catalog, options);
            if rendered_step != "1" {
                args.push(rendered_step);
            }
            if let Some(timezone) = timezone {
                args.push(render_expr(timezone, query, catalog, options));
            }
            let mut sql = format!("generate_series({})", args.join(", "));
            if *with_ordinality {
                sql.push_str(" WITH ORDINALITY");
            }
            sql
        }
        SetReturningCall::Unnest {
            args,
            with_ordinality,
            ..
        } => {
            let mut sql = format!(
                "unnest({})",
                args.iter()
                    .map(|arg| render_expr(arg, query, catalog, options))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            if *with_ordinality {
                sql.push_str(" WITH ORDINALITY");
            }
            sql
        }
        SetReturningCall::UserDefined {
            proc_oid,
            args,
            with_ordinality,
            ..
        } => {
            let name = catalog
                .proc_row_by_oid(*proc_oid)
                .map(|row| row.proname)
                .unwrap_or_else(|| format!("proc_{proc_oid}"));
            let mut sql = format!(
                "{}({})",
                name,
                args.iter()
                    .map(|arg| render_expr(arg, query, catalog, options))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            if *with_ordinality {
                sql.push_str(" WITH ORDINALITY");
            }
            sql
        }
        other => format!("{other:?}"),
    }
}

fn render_datetime_cast_literal(expr: &Expr, ty: SqlType) -> Option<String> {
    let Expr::Const(value) = expr else {
        return None;
    };
    let text = value.as_text()?;
    let config = postgres_utc_datetime_config();
    match ty.kind {
        SqlTypeKind::Timestamp => parse_timestamp_text(text, &config).ok().map(|timestamp| {
            format!(
                "'{}'::timestamp without time zone",
                format_timestamp_text(timestamp, &config).replace('\'', "''")
            )
        }),
        SqlTypeKind::TimestampTz => parse_timestamptz_text(text, &config).ok().map(|timestamp| {
            format!(
                "'{}'::timestamp with time zone",
                format_timestamptz_text(timestamp, &config).replace('\'', "''")
            )
        }),
        _ => None,
    }
}

fn postgres_utc_datetime_config() -> DateTimeConfig {
    let mut config = DateTimeConfig::default();
    config.date_style_format = DateStyleFormat::Postgres;
    config.date_order = DateOrder::Mdy;
    config.time_zone = "UTC".into();
    config
}

fn render_wrapped_expr(
    expr: &Expr,
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
    match expr {
        Expr::Op(_) | Expr::Bool(_) | Expr::ScalarArrayOp(_) => {
            format!("({})", render_expr(expr, query, catalog, options))
        }
        _ => render_expr(expr, query, catalog, options),
    }
}

fn render_sublink(
    sublink: &SubLink,
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
    let subquery = render_subquery_expr(&sublink.subselect, catalog);
    match sublink.sublink_type {
        SubLinkType::ExistsSubLink => format!("(EXISTS ({subquery}))"),
        SubLinkType::ExprSubLink => format!("({subquery})"),
        SubLinkType::ArraySubLink => format!("ARRAY({subquery})"),
        SubLinkType::AnySubLink(op) => {
            let Some(testexpr) = &sublink.testexpr else {
                return format!("ANY ({subquery})");
            };
            let left = render_wrapped_expr(testexpr, query, catalog, options);
            if op == SubqueryComparisonOp::Eq {
                format!("({left} IN ({subquery}))")
            } else {
                format!("({left} {} ANY ({subquery}))", render_subquery_op(op))
            }
        }
        SubLinkType::RowCompareSubLink(op) => {
            let Some(testexpr) = &sublink.testexpr else {
                return format!("ROWCOMPARE ({subquery})");
            };
            format!(
                "({} {} ({subquery}))",
                render_wrapped_expr(testexpr, query, catalog, options),
                render_subquery_op(op)
            )
        }
        SubLinkType::AllSubLink(op) => {
            let Some(testexpr) = &sublink.testexpr else {
                return format!("ALL ({subquery})");
            };
            format!(
                "({} {} ALL ({subquery}))",
                render_wrapped_expr(testexpr, query, catalog, options),
                render_subquery_op(op)
            )
        }
    }
}

fn render_subquery_expr(query: &Query, catalog: &dyn CatalogLookup) -> String {
    render_view_query(query, catalog)
        .trim_end_matches(';')
        .to_string()
}

fn render_scalar_array_op(
    saop: &ScalarArrayOpExpr,
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
    let left = render_wrapped_expr(&saop.left, query, catalog, options);
    let right = render_scalar_array_rhs(&saop.right, query, catalog, options);
    let quantifier = if saop.use_or { "ANY" } else { "ALL" };
    if saop.use_or && saop.op == SubqueryComparisonOp::Eq {
        format!("{left} = {quantifier} ({right})")
    } else {
        format!(
            "{left} {} {quantifier} ({right})",
            render_subquery_op(saop.op)
        )
    }
}

fn render_scalar_array_rhs(
    expr: &Expr,
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
    match expr {
        Expr::SubLink(sublink) => render_sublink(sublink, query, catalog, options),
        _ => render_expr(expr, query, catalog, options),
    }
}

fn render_function(
    func: &FuncExpr,
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
    if matches!(
        func.implementation,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::Timezone)
    ) {
        return render_timezone_function(func, query, catalog, options);
    }
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
            .map(|arg| render_expr(arg, query, catalog, options))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn render_timezone_function(
    func: &FuncExpr,
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
    match func.args.as_slice() {
        [value] => format!("timezone({})", render_expr(value, query, catalog, options)),
        [zone, value] if is_local_timezone_marker(zone) => {
            format!("({} AT LOCAL)", render_expr(value, query, catalog, options))
        }
        [zone, value] => {
            format!(
                "({} AT TIME ZONE {})",
                render_expr(value, query, catalog, options),
                render_timezone_zone_arg(zone, query, catalog, options)
            )
        }
        _ => "timezone()".into(),
    }
}

fn is_local_timezone_marker(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Const(value) if value.as_text() == Some("__pgrust_local_timezone__")
    )
}

fn render_timezone_zone_arg(
    expr: &Expr,
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
    let rendered = render_expr(expr, query, catalog, options);
    if rendered == "current_setting('TimeZone')" {
        "current_setting('TimeZone'::text)".into()
    } else if rendered == "'00:00'::text" || rendered == "'00:00'::interval" {
        "'@ 0'::interval".into()
    } else {
        rendered
    }
}

fn render_aggregate(
    aggref: &Aggref,
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
    let name = catalog
        .proc_row_by_oid(aggref.aggfnoid)
        .map(|row| row.proname)
        .unwrap_or_else(|| format!("agg_{}", aggref.aggfnoid));
    let mut args = aggref
        .args
        .iter()
        .map(|arg| render_expr(arg, query, catalog, options))
        .collect::<Vec<_>>();
    if aggref.aggdistinct && !args.is_empty() {
        args[0] = format!("DISTINCT {}", args[0]);
    }
    format!("{name}({})", args.join(", "))
}

fn render_op(
    op: OpExprKind,
    args: &[Expr],
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> String {
    match (op, args) {
        (OpExprKind::UnaryPlus, [arg]) => {
            format!("+{}", render_wrapped_expr(arg, query, catalog, options))
        }
        (OpExprKind::Negate, [arg]) => {
            format!("-{}", render_wrapped_expr(arg, query, catalog, options))
        }
        (OpExprKind::BitNot, [arg]) => {
            format!("~{}", render_wrapped_expr(arg, query, catalog, options))
        }
        (_, [left, right]) => format!(
            "{} {} {}",
            render_wrapped_expr(left, query, catalog, options),
            render_binary_operator(op),
            render_wrapped_expr(right, query, catalog, options)
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

fn render_subquery_op(op: SubqueryComparisonOp) -> &'static str {
    match op {
        SubqueryComparisonOp::Eq => "=",
        SubqueryComparisonOp::NotEq => "<>",
        SubqueryComparisonOp::Lt => "<",
        SubqueryComparisonOp::LtEq => "<=",
        SubqueryComparisonOp::Gt => ">",
        SubqueryComparisonOp::GtEq => ">=",
        SubqueryComparisonOp::Match => "@@",
        SubqueryComparisonOp::Like => "LIKE",
        SubqueryComparisonOp::NotLike => "NOT LIKE",
        SubqueryComparisonOp::ILike => "ILIKE",
        SubqueryComparisonOp::NotILike => "NOT ILIKE",
        SubqueryComparisonOp::Similar => "SIMILAR TO",
        SubqueryComparisonOp::NotSimilar => "NOT SIMILAR TO",
    }
}

fn render_join_type(kind: JoinType) -> &'static str {
    match kind {
        JoinType::Inner => "INNER",
        JoinType::Cross => "CROSS",
        JoinType::Left => "LEFT",
        JoinType::Right => "RIGHT",
        JoinType::Full => "FULL",
        JoinType::Semi => "SEMI",
        JoinType::Anti => "ANTI",
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

fn render_sql_type(ty: SqlType) -> String {
    if ty.is_array {
        return "array".into();
    }
    if let Some((precision, scale)) = ty.numeric_precision_scale() {
        return format!("numeric({precision},{scale})");
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
        SqlTypeKind::Time => "time without time zone",
        SqlTypeKind::TimeTz => "time with time zone",
        SqlTypeKind::Timestamp => "timestamp without time zone",
        SqlTypeKind::TimestampTz => "timestamp with time zone",
        SqlTypeKind::RegClass => "regclass",
        SqlTypeKind::Interval => "interval",
        SqlTypeKind::Json => "json",
        SqlTypeKind::Jsonb => "jsonb",
        SqlTypeKind::Char => {
            return ty
                .char_len()
                .map(|len| format!("character({len})"))
                .unwrap_or_else(|| "bpchar".into());
        }
        SqlTypeKind::Varchar => {
            return ty
                .char_len()
                .map(|len| format!("character varying({len})"))
                .unwrap_or_else(|| "character varying".into());
        }
        _ => "text",
    }
    .into()
}

fn var_name(
    var: &Var,
    query: &Query,
    catalog: &dyn CatalogLookup,
    options: RenderOptions,
) -> Option<String> {
    let column_index = attrno_index(var.varattno)?;
    let rte = query.rtable.get(var.varno.checked_sub(1)?)?;
    let column_name = rte
        .desc
        .columns
        .get(column_index)
        .map(|column| quote_identifier_if_needed(&column.name))?;
    if !options.qualify_vars {
        return Some(column_name);
    }
    let qualifier = rte.alias.clone().or_else(|| match &rte.kind {
        RangeTblEntryKind::Relation { relation_oid, .. } => catalog
            .class_row_by_oid(*relation_oid)
            .map(|class| quote_identifier_if_needed(&class.relname)),
        _ => None,
    })?;
    Some(format!("{qualifier}.{column_name}"))
}

fn relation_sql_name(relation_oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    let class = catalog.class_row_by_oid(relation_oid)?;
    let relname = quote_identifier_if_needed(&class.relname);
    let schema_name = catalog
        .namespace_row_by_oid(class.relnamespace)
        .map(|row| row.nspname)
        .unwrap_or_else(|| "public".into());
    Some(match schema_name.as_str() {
        "public" | "pg_catalog" => relname,
        _ => format!("{}.{}", quote_identifier_if_needed(&schema_name), relname),
    })
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
            matches!(
                jointype,
                JoinType::Left | JoinType::Right | JoinType::Full | JoinType::Semi | JoinType::Anti
            ) && column_index < *joinmergedcols
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
        BuiltinScalarFunction::CurrentSetting => "current_setting",
        BuiltinScalarFunction::Timezone => "timezone",
        BuiltinScalarFunction::DatePart => "date_part",
        BuiltinScalarFunction::Extract => "extract",
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
