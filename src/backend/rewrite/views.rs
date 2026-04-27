use crate::backend::executor::{parse_interval_text_value, render_interval_text_with_config};
use crate::backend::parser::analyze::analyze_select_query_with_outer;
use crate::backend::parser::{
    CatalogLookup, ParseError, SqlType, SqlTypeKind, Statement, SubqueryComparisonOp,
};
use crate::backend::utils::misc::guc_datetime::{
    DateOrder, DateStyleFormat, DateTimeConfig, IntervalStyle,
};
use crate::backend::utils::time::timestamp::{
    format_timestamp_text, format_timestamptz_text, parse_timestamp_text, parse_timestamptz_text,
};
use crate::include::catalog::PUBLIC_NAMESPACE_OID;
use crate::include::nodes::datum::{IntervalValue, Value};
use crate::include::nodes::parsenodes::{
    JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind, SelectStatement, SetOperationQuery,
    ViewCheckOption, WindowFrameExclusion, WindowFrameMode,
};
use crate::include::nodes::primnodes::{
    Aggref, BoolExprType, BuiltinScalarFunction, Expr, FuncExpr, JoinType, OpExprKind,
    RelationDesc, SELF_ITEM_POINTER_ATTR_NO, ScalarArrayOpExpr, ScalarFunctionImpl,
    SetReturningCall, SubLink, SubLinkType, TABLE_OID_ATTR_NO, TargetEntry, Var, WindowClause,
    WindowFrameBound, WindowFuncExpr, WindowFuncKind, attrno_index, expr_sql_type_hint,
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

pub(crate) fn render_view_query_sql(query: &Query, catalog: &dyn CatalogLookup) -> String {
    let rendered = render_view_query(query, catalog);
    let body = rendered.strip_suffix(';').unwrap_or(&rendered).trim_start();
    normalize_deparsed_view_sql_for_parser(body)
}

fn normalize_deparsed_view_sql_for_parser(sql: &str) -> String {
    const KEYWORDS: &[&str] = &[
        "ALL",
        "AND",
        "AS",
        "BY",
        "CROSS",
        "DISTINCT",
        "EXCEPT",
        "FOR",
        "FROM",
        "FULL",
        "GROUP",
        "HAVING",
        "INNER",
        "INTERSECT",
        "JOIN",
        "LEFT",
        "LIMIT",
        "LOCAL",
        "LOCKED",
        "NOWAIT",
        "OFFSET",
        "ON",
        "OPTION",
        "OR",
        "ORDER",
        "RIGHT",
        "SELECT",
        "SHARE",
        "SKIP",
        "UNION",
        "UPDATE",
        "USING",
        "VALUES",
        "WHERE",
        "WITH",
    ];

    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => {
                let quote = bytes[i];
                out.push(quote as char);
                i += 1;
                while i < bytes.len() {
                    out.push(bytes[i] as char);
                    if bytes[i] == quote {
                        if i + 1 < bytes.len() && bytes[i + 1] == quote {
                            i += 1;
                            out.push(bytes[i] as char);
                        } else {
                            i += 1;
                            break;
                        }
                    }
                    i += 1;
                }
            }
            byte if byte.is_ascii_alphabetic() => {
                let start = i;
                i += 1;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let word = &sql[start..i];
                if KEYWORDS.iter().any(|keyword| *keyword == word) {
                    out.push_str(&word.to_ascii_lowercase());
                } else {
                    out.push_str(word);
                }
            }
            byte => {
                out.push(byte as char);
                i += 1;
            }
        }
    }
    out
}

pub(crate) fn refresh_query_relation_descriptors(query: &mut Query, catalog: &dyn CatalogLookup) {
    for rte in &mut query.rtable {
        let relation_oid = match &rte.kind {
            RangeTblEntryKind::Relation { relation_oid, .. } => Some(*relation_oid),
            _ => None,
        };
        if let Some(relation_oid) = relation_oid {
            if let Some(relation) = catalog.lookup_relation_by_oid(relation_oid) {
                rte.desc = relation.desc;
                if !rte_is_user_aliased_relation(rte, catalog) {
                    rte.eref.colnames = rte_current_visible_colnames(rte);
                }
            }
            continue;
        }
        if let RangeTblEntryKind::Subquery { query: subquery } = &mut rte.kind {
            refresh_query_relation_descriptors(subquery, catalog);
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
    let mut stmt = crate::backend::parser::parse_statement(&sql)?;
    if is_unsupported_select_statement(&stmt) {
        stmt =
            crate::backend::parser::parse_statement(&normalize_deparsed_view_sql_for_parser(sql))?;
    }
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

fn is_unsupported_select_statement(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Unsupported(crate::backend::parser::UnsupportedStatement {
            feature: "SELECT form",
            ..
        })
    )
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
    if let Some(set_operation) = &query.set_operation {
        return render_set_operation_query(set_operation, query, catalog);
    }
    let select_keyword = render_select_keyword(query, catalog);
    let targets = query
        .target_list
        .iter()
        .filter(|target| !target.resjunk)
        .map(|target| render_target_entry(target, query, catalog))
        .collect::<Vec<_>>();
    let mut lines = if targets.len() > 1 {
        let mut lines = vec![format!(" {select_keyword} {},", targets[0])];
        for (index, target) in targets.iter().enumerate().skip(1) {
            let suffix = if index + 1 == targets.len() { "" } else { "," };
            lines.push(format!("    {target}{suffix}"));
        }
        lines
    } else {
        vec![format!(" {select_keyword} {}", targets.join(", "))]
    };

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
    if !query.sort_clause.is_empty() {
        lines.push(format!(
            "  ORDER BY {}",
            query
                .sort_clause
                .iter()
                .map(|sort| render_expr(&sort.expr, query, catalog))
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

fn render_select_keyword(query: &Query, catalog: &dyn CatalogLookup) -> String {
    if !query.distinct {
        return "SELECT".into();
    }
    if query.distinct_on.is_empty() {
        return "SELECT DISTINCT".into();
    }
    format!(
        "SELECT DISTINCT ON ({})",
        query
            .distinct_on
            .iter()
            .map(|clause| render_expr(&clause.expr, query, catalog))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn render_set_operation_query(
    set_operation: &SetOperationQuery,
    query: &Query,
    catalog: &dyn CatalogLookup,
) -> String {
    let op = match set_operation.op {
        crate::include::nodes::parsenodes::SetOperator::Union { all } => {
            if all {
                "UNION ALL"
            } else {
                "UNION"
            }
        }
        crate::include::nodes::parsenodes::SetOperator::Intersect { all } => {
            if all {
                "INTERSECT ALL"
            } else {
                "INTERSECT"
            }
        }
        crate::include::nodes::parsenodes::SetOperator::Except { all } => {
            if all {
                "EXCEPT ALL"
            } else {
                "EXCEPT"
            }
        }
    };
    let mut rendered = set_operation
        .inputs
        .iter()
        .enumerate()
        .map(|(index, input)| {
            let sql = render_set_operation_input_query(input, catalog, index == 0);
            sql.strip_suffix(';').unwrap_or(&sql).to_string()
        })
        .collect::<Vec<_>>()
        .join(&format!("\n{op}\n"));
    if !query.sort_clause.is_empty() {
        rendered.push_str("\n  ORDER BY ");
        rendered.push_str(
            &query
                .sort_clause
                .iter()
                .map(|sort| render_expr(&sort.expr, query, catalog))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    rendered.push(';');
    rendered
}

fn render_set_operation_input_query(
    query: &Query,
    catalog: &dyn CatalogLookup,
    include_aliases: bool,
) -> String {
    let select_keyword = render_select_keyword(query, catalog);
    let targets = query
        .target_list
        .iter()
        .filter(|target| !target.resjunk)
        .map(|target| render_set_operation_target_entry(target, query, catalog, include_aliases))
        .collect::<Vec<_>>();
    let mut lines = if targets.len() > 1 {
        let mut lines = vec![format!(" {select_keyword} {},", targets[0])];
        for (index, target) in targets.iter().enumerate().skip(1) {
            let suffix = if index + 1 == targets.len() { "" } else { "," };
            lines.push(format!("    {target}{suffix}"));
        }
        lines
    } else {
        vec![format!(" {select_keyword} {}", targets.join(", "))]
    };
    if let Some(jointree) = &query.jointree {
        lines.push(format!(
            "   FROM {}",
            render_from_node(query, jointree, catalog, 3)
        ));
    }
    if let Some(where_qual) = &query.where_qual {
        lines.push(format!(
            "  WHERE {}",
            render_set_operation_expr(where_qual, query, catalog)
        ));
    }
    lines.join("\n") + ";"
}

fn render_set_operation_target_entry(
    target: &TargetEntry,
    query: &Query,
    catalog: &dyn CatalogLookup,
    include_aliases: bool,
) -> String {
    let rendered = render_set_operation_expr(&target.expr, query, catalog);
    if !include_aliases || rendered_matches_target_name(&rendered, &target.name) {
        rendered
    } else {
        format!("{rendered} AS {}", quote_identifier_if_needed(&target.name))
    }
}

fn render_set_operation_expr(expr: &Expr, query: &Query, catalog: &dyn CatalogLookup) -> String {
    match expr {
        Expr::Var(var) => qualified_var_name(var, query, catalog)
            .unwrap_or_else(|| format!("var{}", var.varattno)),
        Expr::Const(value) => render_literal(value),
        Expr::Op(op) => render_set_operation_op(op.op, &op.args, query, catalog),
        _ => render_expr(expr, query, catalog),
    }
}

fn render_set_operation_wrapped_expr(
    expr: &Expr,
    query: &Query,
    catalog: &dyn CatalogLookup,
) -> String {
    match expr {
        Expr::Op(_) | Expr::Bool(_) | Expr::ScalarArrayOp(_) => {
            format!("({})", render_set_operation_expr(expr, query, catalog))
        }
        _ => render_set_operation_expr(expr, query, catalog),
    }
}

fn render_set_operation_op(
    op: OpExprKind,
    args: &[Expr],
    query: &Query,
    catalog: &dyn CatalogLookup,
) -> String {
    match (op, args) {
        (OpExprKind::UnaryPlus, [arg]) => {
            format!(
                "+{}",
                render_set_operation_wrapped_expr(arg, query, catalog)
            )
        }
        (OpExprKind::Negate, [arg]) => {
            format!(
                "-{}",
                render_set_operation_wrapped_expr(arg, query, catalog)
            )
        }
        (OpExprKind::BitNot, [arg]) => {
            format!(
                "~{}",
                render_set_operation_wrapped_expr(arg, query, catalog)
            )
        }
        (_, [left, right]) => format!(
            "{} {} {}",
            render_set_operation_wrapped_expr(left, query, catalog),
            render_binary_operator(op),
            render_set_operation_wrapped_expr(right, query, catalog)
        ),
        _ => format!("{op:?}"),
    }
}

fn render_target_entry(target: &TargetEntry, query: &Query, catalog: &dyn CatalogLookup) -> String {
    let mut rendered = match &target.expr {
        Expr::Var(var) if join_using_var_needs_cast(var, target.sql_type, query) => format!(
            "({})::{}",
            var_name(var, query, catalog).unwrap_or_else(|| format!("var{}", var.varattno)),
            render_sql_type(target.sql_type)
        ),
        Expr::Const(Value::Null) if target.sql_type.kind == SqlTypeKind::Text => {
            "NULL::text".into()
        }
        Expr::Const(Value::Text(_) | Value::TextRef(_, _))
            if target.sql_type.kind == SqlTypeKind::Text =>
        {
            format!("{}::text", render_expr(&target.expr, query, catalog))
        }
        _ => render_expr(&target.expr, query, catalog),
    };
    if target.name.eq_ignore_ascii_case("dat_at_local")
        && let Some(inner) = rendered
            .strip_prefix("timezone(")
            .and_then(|value| value.strip_suffix(')'))
    {
        rendered = format!("({inner} AT LOCAL)");
    }
    if rendered_matches_target_name(&rendered, &target.name) {
        rendered
    } else {
        format!("{rendered} AS {}", quote_identifier_if_needed(&target.name))
    }
}

fn rendered_matches_target_name(rendered: &str, target_name: &str) -> bool {
    let quoted = quote_identifier_if_needed(target_name);
    rendered == quoted
        || rendered
            .rsplit_once('.')
            .is_some_and(|(_, column)| column == quoted)
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
            let join_rte = query.rtable.get(rtindex.saturating_sub(1));
            let using_cols = join_rte
                .and_then(|rte| match &rte.kind {
                    RangeTblEntryKind::Join { joinmergedcols, .. } => Some(
                        rte_effective_colnames(rte)
                            .into_iter()
                            .take(*joinmergedcols)
                            .collect::<Vec<_>>(),
                    ),
                    _ => None,
                })
                .unwrap_or_default()
                .into_iter()
                .map(|name| quote_identifier_if_needed(&name))
                .collect::<Vec<_>>();
            let constraint = if matches!(kind, JoinType::Cross) && using_cols.is_empty() {
                String::new()
            } else if using_cols.is_empty() {
                format!(" ON {}", render_expr(quals, query, catalog))
            } else {
                format!(" USING ({})", using_cols.join(", "))
            };
            let mut rendered = format!(
                "{left_sql}\n{}{} {}{}",
                " ".repeat(indent + 3),
                render_join_keyword(*kind),
                right_sql,
                constraint
            );
            if let Some(rte) = join_rte
                && let Some(alias) = &rte.alias
            {
                let alias_sql = render_alias_clause(
                    query,
                    *rtindex,
                    catalog,
                    rte,
                    Some(alias),
                    AliasColumnListMode::IfNeeded,
                );
                if rte.alias_preserves_source_names {
                    rendered = format!("{rendered} AS {alias_sql}");
                } else {
                    rendered = format!("({rendered}) {alias_sql}");
                }
            }
            rendered
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
            let alias = relation_alias_for_render(query, index, catalog, rte, &relname);
            if alias.is_some() {
                format!(
                    "{base} {}",
                    render_alias_clause(
                        query,
                        index,
                        catalog,
                        rte,
                        alias.as_deref(),
                        AliasColumnListMode::IfNeeded
                    )
                )
            } else {
                base
            }
        }
        RangeTblEntryKind::Subquery { query } => {
            let rendered = render_view_query(query, catalog);
            let body = rendered.strip_suffix(';').unwrap_or(&rendered);
            match &rte.alias {
                Some(alias) => format!(
                    "({body}) {}",
                    render_alias_clause(
                        query,
                        index,
                        catalog,
                        rte,
                        Some(alias),
                        AliasColumnListMode::IfNeeded
                    )
                ),
                None => format!("({body})"),
            }
        }
        RangeTblEntryKind::Join { .. } => rte.alias.clone().unwrap_or_else(|| "join".into()),
        RangeTblEntryKind::Values { rows, .. } => {
            let rows = rows
                .iter()
                .map(|row| {
                    format!(
                        "({})",
                        row.iter()
                            .map(|expr| render_expr(expr, query, catalog))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let mut rendered = format!("( VALUES {rows})");
            if let Some(alias) = &rte.alias {
                rendered.push(' ');
                rendered.push_str(&render_alias_clause(
                    query,
                    index,
                    catalog,
                    rte,
                    Some(alias),
                    AliasColumnListMode::Always,
                ));
            }
            rendered
        }
        RangeTblEntryKind::Function { call } => {
            let mut rendered = render_set_returning_call(call, query, catalog);
            if let Some(alias) = &rte.alias {
                rendered.push(' ');
                rendered.push_str(&render_alias_clause(
                    query,
                    index,
                    catalog,
                    rte,
                    Some(alias),
                    AliasColumnListMode::Always,
                ));
            }
            rendered
        }
        RangeTblEntryKind::Result => "(RESULT)".into(),
        RangeTblEntryKind::WorkTable { worktable_id } => format!("worktable {worktable_id}"),
        RangeTblEntryKind::Cte { cte_id, .. } => format!("cte {cte_id}"),
    }
}

#[derive(Clone, Copy)]
enum AliasColumnListMode {
    Always,
    IfNeeded,
}

fn relation_alias_for_render(
    query: &Query,
    index: usize,
    catalog: &dyn CatalogLookup,
    rte: &RangeTblEntry,
    relname: &str,
) -> Option<String> {
    if rte
        .alias
        .as_deref()
        .and_then(|alias| alias.rsplit('.').next())
        .is_some_and(|alias_relname| alias_relname.eq_ignore_ascii_case(relname))
    {
        return None;
    }
    let effective_colnames = rte_effective_colnames_for_query(query, index, catalog);
    let current_colnames = rte_current_visible_colnames(rte);
    if effective_colnames != current_colnames {
        return Some(rte.eref.aliasname.clone());
    }
    rte.alias
        .as_ref()
        .filter(|alias| !alias.eq_ignore_ascii_case(relname))
        .cloned()
}

fn render_alias_clause(
    query: &Query,
    index: usize,
    catalog: &dyn CatalogLookup,
    rte: &RangeTblEntry,
    alias: Option<&str>,
    mode: AliasColumnListMode,
) -> String {
    let alias = alias.unwrap_or(&rte.eref.aliasname);
    let mut rendered = quote_identifier_if_needed(alias);
    let effective_colnames = rte_effective_colnames_for_query(query, index, catalog);
    let current_colnames = rte_current_visible_colnames(rte);
    let include_colnames = match mode {
        AliasColumnListMode::Always => !effective_colnames.is_empty(),
        AliasColumnListMode::IfNeeded => effective_colnames != current_colnames,
    };
    if include_colnames {
        rendered.push('(');
        rendered.push_str(
            &effective_colnames
                .iter()
                .map(|name| quote_identifier_if_needed(name))
                .collect::<Vec<_>>()
                .join(", "),
        );
        rendered.push(')');
    }
    rendered
}

fn rte_effective_colnames(rte: &RangeTblEntry) -> Vec<String> {
    let mut names = Vec::new();
    let mut visible_index = 0usize;
    for (physical_index, column) in rte.desc.columns.iter().enumerate() {
        if column.dropped {
            continue;
        }
        let name = rte
            .eref
            .colnames
            .get(physical_index)
            .or_else(|| rte.eref.colnames.get(visible_index))
            .cloned()
            .unwrap_or_else(|| column.name.clone());
        names.push(name);
        visible_index += 1;
    }
    names
}

fn rte_effective_colnames_for_query(
    query: &Query,
    index: usize,
    catalog: &dyn CatalogLookup,
) -> Vec<String> {
    let Some(rte) = query.rtable.get(index.saturating_sub(1)) else {
        return Vec::new();
    };
    let mut names = rte_effective_colnames(rte);
    if !rte_is_user_aliased_relation(rte, catalog) {
        return names;
    }
    let mut used = query
        .rtable
        .iter()
        .take(index.saturating_sub(1))
        .flat_map(rte_effective_colnames)
        .map(|name| name.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    for name in &mut names {
        if !used.contains(&name.to_ascii_lowercase()) {
            used.insert(name.to_ascii_lowercase());
            continue;
        }
        let base = name.clone();
        let mut suffix = 1usize;
        loop {
            let candidate = format!("{base}_{suffix}");
            if !used.contains(&candidate.to_ascii_lowercase()) {
                *name = candidate.clone();
                used.insert(candidate.to_ascii_lowercase());
                break;
            }
            suffix += 1;
        }
    }
    names
}

fn rte_is_user_aliased_relation(rte: &RangeTblEntry, catalog: &dyn CatalogLookup) -> bool {
    let Some(alias) = &rte.alias else {
        return false;
    };
    let RangeTblEntryKind::Relation { relation_oid, .. } = &rte.kind else {
        return false;
    };
    catalog
        .class_row_by_oid(*relation_oid)
        .is_some_and(|class| {
            !alias
                .rsplit('.')
                .next()
                .is_some_and(|alias_relname| alias_relname.eq_ignore_ascii_case(&class.relname))
        })
}

fn rte_current_visible_colnames(rte: &RangeTblEntry) -> Vec<String> {
    rte.desc
        .columns
        .iter()
        .filter(|column| !column.dropped)
        .map(|column| column.name.clone())
        .collect()
}

fn render_set_returning_call(
    call: &SetReturningCall,
    query: &Query,
    catalog: &dyn CatalogLookup,
) -> String {
    let (name, args, with_ordinality) = match call {
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            with_ordinality,
            ..
        } => {
            let mut args = vec![start, stop];
            if !is_default_generate_series_step(step) || timezone.is_some() {
                args.push(step);
            }
            if let Some(timezone) = timezone {
                args.push(timezone);
            }
            ("generate_series".to_string(), args, *with_ordinality)
        }
        SetReturningCall::GenerateSubscripts {
            array,
            dimension,
            reverse,
            with_ordinality,
            ..
        } => {
            let mut args = vec![array, dimension];
            if let Some(reverse) = reverse {
                args.push(reverse);
            }
            ("generate_subscripts".to_string(), args, *with_ordinality)
        }
        SetReturningCall::Unnest {
            args,
            with_ordinality,
            ..
        } => (
            "unnest".to_string(),
            args.iter().collect(),
            *with_ordinality,
        ),
        SetReturningCall::JsonTableFunction {
            kind,
            args,
            with_ordinality,
            ..
        } => (
            json_table_function_name(*kind).to_string(),
            args.iter().collect(),
            *with_ordinality,
        ),
        SetReturningCall::JsonRecordFunction {
            kind,
            args,
            with_ordinality,
            ..
        } => (
            kind.name().to_string(),
            args.iter().collect(),
            *with_ordinality,
        ),
        SetReturningCall::RegexTableFunction {
            kind,
            args,
            with_ordinality,
            ..
        } => (
            regex_table_function_name(*kind).to_string(),
            args.iter().collect(),
            *with_ordinality,
        ),
        SetReturningCall::StringTableFunction {
            kind,
            args,
            with_ordinality,
            ..
        } => (
            string_table_function_name(*kind).to_string(),
            args.iter().collect(),
            *with_ordinality,
        ),
        SetReturningCall::PartitionTree { relid, .. } => {
            ("pg_partition_tree".to_string(), vec![relid], false)
        }
        SetReturningCall::PartitionAncestors { relid, .. } => {
            ("pg_partition_ancestors".to_string(), vec![relid], false)
        }
        SetReturningCall::PgLockStatus {
            with_ordinality, ..
        } => ("pg_lock_status".to_string(), Vec::new(), *with_ordinality),
        SetReturningCall::TxidSnapshotXip {
            arg,
            with_ordinality,
            ..
        } => ("txid_snapshot_xip".to_string(), vec![arg], *with_ordinality),
        SetReturningCall::TextSearchTableFunction {
            kind,
            args,
            with_ordinality,
            ..
        } => (
            match kind {
                crate::include::nodes::primnodes::TextSearchTableFunction::TokenType => {
                    "ts_token_type"
                }
                crate::include::nodes::primnodes::TextSearchTableFunction::Parse => "ts_parse",
                crate::include::nodes::primnodes::TextSearchTableFunction::Debug => "ts_debug",
                crate::include::nodes::primnodes::TextSearchTableFunction::Stat => "ts_stat",
            }
            .to_string(),
            args.iter().collect(),
            *with_ordinality,
        ),
        SetReturningCall::UserDefined {
            proc_oid,
            args,
            with_ordinality,
            ..
        } => (
            catalog
                .proc_row_by_oid(*proc_oid)
                .map(|row| row.proname)
                .unwrap_or_else(|| format!("proc_{proc_oid}")),
            args.iter().collect(),
            *with_ordinality,
        ),
    };
    let mut rendered = format!(
        "{}({})",
        quote_identifier_if_needed(&name),
        args.into_iter()
            .map(|arg| render_wrapped_expr(arg, query, catalog))
            .collect::<Vec<_>>()
            .join(", ")
    );
    if with_ordinality {
        rendered.push_str(" WITH ORDINALITY");
    }
    rendered
}

fn json_table_function_name(
    kind: crate::include::nodes::primnodes::JsonTableFunction,
) -> &'static str {
    match kind {
        crate::include::nodes::primnodes::JsonTableFunction::ObjectKeys => "json_object_keys",
        crate::include::nodes::primnodes::JsonTableFunction::Each => "json_each",
        crate::include::nodes::primnodes::JsonTableFunction::EachText => "json_each_text",
        crate::include::nodes::primnodes::JsonTableFunction::ArrayElements => "json_array_elements",
        crate::include::nodes::primnodes::JsonTableFunction::ArrayElementsText => {
            "json_array_elements_text"
        }
        crate::include::nodes::primnodes::JsonTableFunction::JsonbPathQuery => "jsonb_path_query",
        crate::include::nodes::primnodes::JsonTableFunction::JsonbObjectKeys => "jsonb_object_keys",
        crate::include::nodes::primnodes::JsonTableFunction::JsonbEach => "jsonb_each",
        crate::include::nodes::primnodes::JsonTableFunction::JsonbEachText => "jsonb_each_text",
        crate::include::nodes::primnodes::JsonTableFunction::JsonbArrayElements => {
            "jsonb_array_elements"
        }
        crate::include::nodes::primnodes::JsonTableFunction::JsonbArrayElementsText => {
            "jsonb_array_elements_text"
        }
    }
}

fn regex_table_function_name(
    kind: crate::include::nodes::primnodes::RegexTableFunction,
) -> &'static str {
    match kind {
        crate::include::nodes::primnodes::RegexTableFunction::Matches => "regexp_matches",
        crate::include::nodes::primnodes::RegexTableFunction::SplitToTable => {
            "regexp_split_to_table"
        }
    }
}

fn string_table_function_name(
    kind: crate::include::nodes::primnodes::StringTableFunction,
) -> &'static str {
    match kind {
        crate::include::nodes::primnodes::StringTableFunction::StringToTable => "string_to_table",
    }
}

fn is_default_generate_series_step(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Const(Value::Int16(1) | Value::Int32(1) | Value::Int64(1))
    )
}

fn render_expr(expr: &Expr, query: &Query, catalog: &dyn CatalogLookup) -> String {
    match expr {
        Expr::Var(var) => {
            var_name(var, query, catalog).unwrap_or_else(|| format!("var{}", var.varattno))
        }
        Expr::Const(value) => render_literal(value),
        Expr::Cast(inner, ty) => {
            if let Some(rendered) = render_datetime_cast_literal(inner, *ty) {
                return rendered;
            }
            if matches!(**inner, Expr::Const(_) | Expr::Var(_)) {
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
        Expr::SubLink(sublink) => render_sublink(sublink, query, catalog),
        Expr::ScalarArrayOp(saop) => render_scalar_array_op(saop, query, catalog),
        Expr::Func(func) => render_function(func, query, catalog),
        Expr::WindowFunc(window_func) => render_window_function(window_func, query, catalog),
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
        Expr::ArrayLiteral { elements, .. } => format!(
            "ARRAY[{}]",
            elements
                .iter()
                .map(|element| render_expr(element, query, catalog))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        _ => format!("{expr:?}"),
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
        SqlTypeKind::Interval => parse_interval_text_value(text).ok().map(|interval| {
            format!(
                "'{}'::interval",
                render_view_interval_text(interval).replace('\'', "''")
            )
        }),
        _ => None,
    }
}

fn postgres_utc_datetime_config() -> DateTimeConfig {
    let mut config = DateTimeConfig::default();
    config.date_style_format = DateStyleFormat::Postgres;
    config.date_order = DateOrder::Mdy;
    config.interval_style = IntervalStyle::PostgresVerbose;
    config.time_zone = "UTC".into();
    config
}

fn render_view_interval_text(interval: IntervalValue) -> String {
    render_interval_text_with_config(interval, &postgres_utc_datetime_config())
}

fn render_wrapped_expr(expr: &Expr, query: &Query, catalog: &dyn CatalogLookup) -> String {
    match expr {
        Expr::Op(_) | Expr::Bool(_) | Expr::ScalarArrayOp(_) => {
            format!("({})", render_expr(expr, query, catalog))
        }
        _ => render_expr(expr, query, catalog),
    }
}

fn render_sublink(sublink: &SubLink, query: &Query, catalog: &dyn CatalogLookup) -> String {
    let subquery = render_subquery_expr(&sublink.subselect, catalog);
    match sublink.sublink_type {
        SubLinkType::ExistsSubLink => format!("(EXISTS ({subquery}))"),
        SubLinkType::ExprSubLink => format!("({subquery})"),
        SubLinkType::ArraySubLink => format!("ARRAY({subquery})"),
        SubLinkType::AnySubLink(op) => {
            let Some(testexpr) = &sublink.testexpr else {
                return format!("ANY ({subquery})");
            };
            let left = render_wrapped_expr(testexpr, query, catalog);
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
                render_wrapped_expr(testexpr, query, catalog),
                render_subquery_op(op)
            )
        }
        SubLinkType::AllSubLink(op) => {
            let Some(testexpr) = &sublink.testexpr else {
                return format!("ALL ({subquery})");
            };
            format!(
                "({} {} ALL ({subquery}))",
                render_wrapped_expr(testexpr, query, catalog),
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
) -> String {
    let left = render_wrapped_expr(&saop.left, query, catalog);
    let right = render_scalar_array_rhs(&saop.right, query, catalog);
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

fn render_scalar_array_rhs(expr: &Expr, query: &Query, catalog: &dyn CatalogLookup) -> String {
    match expr {
        Expr::SubLink(sublink) => render_sublink(sublink, query, catalog),
        _ => render_expr(expr, query, catalog),
    }
}

fn render_window_function(
    func: &WindowFuncExpr,
    query: &Query,
    catalog: &dyn CatalogLookup,
) -> String {
    let call = match &func.kind {
        WindowFuncKind::Aggregate(aggref) => render_aggregate(aggref, query, catalog),
        WindowFuncKind::Builtin(kind) => format!(
            "{}({})",
            kind.name(),
            func.args
                .iter()
                .map(|arg| render_expr(arg, query, catalog))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };
    let over = query
        .window_clauses
        .get(func.winref.saturating_sub(1))
        .map(|clause| render_window_clause(clause, query, catalog))
        .unwrap_or_default();
    format!("{call} OVER ({over})")
}

fn render_window_clause(
    clause: &WindowClause,
    query: &Query,
    catalog: &dyn CatalogLookup,
) -> String {
    let mut parts = Vec::new();
    if !clause.spec.partition_by.is_empty() {
        parts.push(format!(
            "PARTITION BY {}",
            clause
                .spec
                .partition_by
                .iter()
                .map(|expr| render_expr(expr, query, catalog))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !clause.spec.order_by.is_empty() {
        parts.push(format!(
            "ORDER BY {}",
            clause
                .spec
                .order_by
                .iter()
                .map(|item| render_window_order_by_item(item, query, catalog))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(frame) = render_window_frame(clause, query, catalog) {
        parts.push(frame);
    }
    parts.join(" ")
}

fn render_window_order_by_item(
    item: &crate::include::nodes::primnodes::OrderByEntry,
    query: &Query,
    catalog: &dyn CatalogLookup,
) -> String {
    let mut rendered = render_expr(&item.expr, query, catalog);
    if item.descending {
        rendered.push_str(" DESC");
    }
    match item.nulls_first {
        Some(true) => rendered.push_str(" NULLS FIRST"),
        Some(false) => rendered.push_str(" NULLS LAST"),
        None => {}
    }
    rendered
}

fn render_window_frame(
    clause: &WindowClause,
    query: &Query,
    catalog: &dyn CatalogLookup,
) -> Option<String> {
    let frame = &clause.spec.frame;
    if frame.mode == WindowFrameMode::Range
        && matches!(frame.start_bound, WindowFrameBound::UnboundedPreceding)
        && matches!(frame.end_bound, WindowFrameBound::CurrentRow)
        && frame.exclusion == WindowFrameExclusion::NoOthers
    {
        return None;
    }
    let mode = match frame.mode {
        WindowFrameMode::Rows => "ROWS",
        WindowFrameMode::Range => "RANGE",
        WindowFrameMode::Groups => "GROUPS",
    };
    let mut rendered = if matches!(frame.end_bound, WindowFrameBound::CurrentRow) {
        format!(
            "{mode} {}",
            render_window_frame_start_bound(&frame.start_bound, frame.mode, query, catalog)
        )
    } else {
        format!(
            "{mode} BETWEEN {} AND {}",
            render_window_frame_bound(&frame.start_bound, frame.mode, query, catalog),
            render_window_frame_bound(&frame.end_bound, frame.mode, query, catalog)
        )
    };
    match frame.exclusion {
        WindowFrameExclusion::NoOthers => {}
        WindowFrameExclusion::CurrentRow => rendered.push_str(" EXCLUDE CURRENT ROW"),
        WindowFrameExclusion::Group => rendered.push_str(" EXCLUDE GROUP"),
        WindowFrameExclusion::Ties => rendered.push_str(" EXCLUDE TIES"),
    }
    Some(rendered)
}

fn render_window_frame_start_bound(
    bound: &WindowFrameBound,
    mode: WindowFrameMode,
    query: &Query,
    catalog: &dyn CatalogLookup,
) -> String {
    match bound {
        WindowFrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".into(),
        WindowFrameBound::OffsetPreceding(offset) => {
            format!(
                "{} PRECEDING",
                render_window_frame_offset_expr(&offset.expr, mode, query, catalog)
            )
        }
        WindowFrameBound::CurrentRow => "CURRENT ROW".into(),
        WindowFrameBound::OffsetFollowing(offset) => {
            format!(
                "{} FOLLOWING",
                render_window_frame_offset_expr(&offset.expr, mode, query, catalog)
            )
        }
        WindowFrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".into(),
    }
}

fn render_window_frame_bound(
    bound: &WindowFrameBound,
    mode: WindowFrameMode,
    query: &Query,
    catalog: &dyn CatalogLookup,
) -> String {
    render_window_frame_start_bound(bound, mode, query, catalog)
}

fn render_window_frame_offset_expr(
    expr: &Expr,
    mode: WindowFrameMode,
    query: &Query,
    catalog: &dyn CatalogLookup,
) -> String {
    if matches!(mode, WindowFrameMode::Rows | WindowFrameMode::Groups)
        && let Expr::Cast(inner, ty) = expr
        && ty.kind == SqlTypeKind::Int8
        && !ty.is_array
        && expr_sql_type_hint(inner).is_some_and(|inner_type| {
            matches!(
                inner_type.kind,
                SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8
            ) && !inner_type.is_array
        })
    {
        return render_expr(inner, query, catalog);
    }
    render_expr(expr, query, catalog)
}

fn render_function(func: &FuncExpr, query: &Query, catalog: &dyn CatalogLookup) -> String {
    if matches!(
        func.implementation,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::Timezone)
    ) {
        return render_timezone_function(func, query, catalog);
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
            .map(|arg| render_expr(arg, query, catalog))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn render_timezone_function(func: &FuncExpr, query: &Query, catalog: &dyn CatalogLookup) -> String {
    match func.args.as_slice() {
        [value] => format!("timezone({})", render_expr(value, query, catalog)),
        [zone, value] if is_local_timezone_marker(zone) => {
            format!("({} AT LOCAL)", render_expr(value, query, catalog))
        }
        [zone, value] => {
            format!(
                "({} AT TIME ZONE {})",
                render_expr(value, query, catalog),
                render_timezone_zone_arg(zone, query, catalog)
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

fn render_timezone_zone_arg(expr: &Expr, query: &Query, catalog: &dyn CatalogLookup) -> String {
    let rendered = render_expr(expr, query, catalog);
    if rendered == "current_setting('TimeZone')" {
        "current_setting('TimeZone'::text)".into()
    } else if rendered == "'00:00'::text" || rendered == "'00:00'::interval" {
        "'@ 0'::interval".into()
    } else {
        rendered
    }
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
    if name.eq_ignore_ascii_case("count") && args.is_empty() {
        args.push("*".into());
    }
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

fn render_join_keyword(kind: JoinType) -> &'static str {
    match kind {
        JoinType::Inner => "JOIN",
        JoinType::Cross => "CROSS JOIN",
        JoinType::Left => "LEFT JOIN",
        JoinType::Right => "RIGHT JOIN",
        JoinType::Full => "FULL JOIN",
        JoinType::Semi => "SEMI JOIN",
        JoinType::Anti => "ANTI JOIN",
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
        Value::Interval(interval) => {
            format!(
                "'{}'::interval",
                render_view_interval_text(*interval).replace('\'', "''")
            )
        }
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

fn var_name(var: &Var, query: &Query, catalog: &dyn CatalogLookup) -> Option<String> {
    let rtindex = var.varno.checked_sub(1)? + 1;
    let rte = query.rtable.get(rtindex.saturating_sub(1))?;
    let system_column_name = system_column_name(var.varattno);
    let column_index = attrno_index(var.varattno);
    let column_name = match (system_column_name, column_index) {
        (Some(name), _) => name.to_string(),
        (None, Some(index)) => rte_column_name(query, rtindex, catalog, index)?,
        (None, None) => return None,
    };
    match &rte.kind {
        RangeTblEntryKind::Join {
            joinmergedcols,
            joinaliasvars,
            ..
        } => {
            if rte.alias.is_some() && !rte.alias_preserves_source_names {
                return Some(qualify_column(&rte.eref.aliasname, &column_name));
            }
            if column_index.is_some_and(|index| index < *joinmergedcols) {
                return Some(quote_identifier_if_needed(&column_name));
            }
            column_index
                .and_then(|index| joinaliasvars.get(index))
                .map(|expr| render_expr(expr, query, catalog))
                .or_else(|| Some(quote_identifier_if_needed(&column_name)))
        }
        RangeTblEntryKind::Relation { relkind, .. } if *relkind == 'v' => {
            Some(quote_identifier_if_needed(&column_name))
        }
        RangeTblEntryKind::Relation { .. }
            if !rte_is_user_aliased_relation(rte, catalog)
                && column_is_unambiguous(query, rtindex, &column_name) =>
        {
            Some(quote_identifier_if_needed(&column_name))
        }
        RangeTblEntryKind::Result => Some(quote_identifier_if_needed(&column_name)),
        _ => Some(qualify_column(&rte.eref.aliasname, &column_name)),
    }
}

fn system_column_name(attno: i32) -> Option<&'static str> {
    match attno {
        TABLE_OID_ATTR_NO => Some("tableoid"),
        SELF_ITEM_POINTER_ATTR_NO => Some("ctid"),
        _ => None,
    }
}

fn column_is_unambiguous(query: &Query, rtindex: usize, column_name: &str) -> bool {
    let needle = column_name.to_ascii_lowercase();
    let mut matches = 0usize;
    for (index, rte) in query.rtable.iter().enumerate() {
        if matches!(rte.kind, RangeTblEntryKind::Join { .. }) {
            continue;
        }
        let names = if index + 1 == rtindex {
            rte_effective_colnames(rte)
        } else {
            rte_current_visible_colnames(rte)
        };
        matches += names
            .into_iter()
            .filter(|name| name.eq_ignore_ascii_case(&needle))
            .count();
    }
    matches <= 1
}

fn rte_column_name(
    query: &Query,
    rtindex: usize,
    catalog: &dyn CatalogLookup,
    column_index: usize,
) -> Option<String> {
    rte_effective_colnames_for_query(query, rtindex, catalog)
        .get(column_index)
        .cloned()
}

fn qualify_column(alias: &str, column: &str) -> String {
    format!(
        "{}.{}",
        quote_identifier_if_needed(alias),
        quote_identifier_if_needed(column)
    )
}

fn qualified_var_name(var: &Var, query: &Query, catalog: &dyn CatalogLookup) -> Option<String> {
    let rtindex = var.varno.checked_sub(1)? + 1;
    let rte = query.rtable.get(rtindex.saturating_sub(1))?;
    let column_index = attrno_index(var.varattno)?;
    let column_name = rte_column_name(query, rtindex, catalog, column_index)?;
    let qualifier = match &rte.kind {
        RangeTblEntryKind::Relation { relation_oid, .. } => {
            relation_sql_name(*relation_oid, catalog).unwrap_or_else(|| rte.eref.aliasname.clone())
        }
        _ => rte.eref.aliasname.clone(),
    };
    Some(format!(
        "{}.{}",
        qualifier,
        quote_identifier_if_needed(&column_name)
    ))
}

fn relation_sql_name(relation_oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    let class = catalog.class_row_by_oid(relation_oid)?;
    if class.relnamespace == PUBLIC_NAMESPACE_OID {
        return Some(quote_identifier_if_needed(&class.relname));
    }
    let namespace = catalog.namespace_row_by_oid(class.relnamespace)?;
    Some(format!(
        "{}.{}",
        quote_identifier_if_needed(&namespace.nspname),
        quote_identifier_if_needed(&class.relname)
    ))
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
        BuiltinScalarFunction::PgGetPartKeyDef => "pg_get_partkeydef",
        BuiltinScalarFunction::PgGetRuleDef => "pg_get_ruledef",
        BuiltinScalarFunction::PgGetViewDef => "pg_get_viewdef",
        BuiltinScalarFunction::CurrentSetting => "current_setting",
        BuiltinScalarFunction::Now => "now",
        BuiltinScalarFunction::TransactionTimestamp => "transaction_timestamp",
        BuiltinScalarFunction::StatementTimestamp => "statement_timestamp",
        BuiltinScalarFunction::ClockTimestamp => "clock_timestamp",
        BuiltinScalarFunction::Timezone => "timezone",
        BuiltinScalarFunction::DatePart => "date_part",
        BuiltinScalarFunction::Extract => "extract",
        BuiltinScalarFunction::Lower => "lower",
        BuiltinScalarFunction::Upper => "upper",
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
