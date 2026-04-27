use crate::backend::executor::jsonb::render_jsonb_bytes;
use crate::backend::executor::jsonpath::canonicalize_jsonpath;
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
use crate::include::nodes::datum::{IntervalValue, Value};
use crate::include::nodes::parsenodes::{
    JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind, SelectStatement, SetOperator,
    ViewCheckOption, WindowFrameExclusion, WindowFrameMode,
};
use crate::include::nodes::primnodes::{
    Aggref, BoolExprType, BuiltinScalarFunction, Expr, FuncExpr, JoinType, OpExprKind,
    RelationDesc, SELF_ITEM_POINTER_ATTR_NO, ScalarArrayOpExpr, ScalarFunctionImpl,
    SetReturningCall, SqlJsonTable, SqlJsonTableBehavior, SqlJsonTableColumn,
    SqlJsonTableColumnKind, SqlJsonTablePlan, SqlJsonTableQuotes, SqlJsonTableWrapper, SubLink,
    SubLinkType, TABLE_OID_ATTR_NO, TargetEntry, Var, WindowClause, WindowFrameBound,
    WindowFuncExpr, WindowFuncKind, attrno_index, expr_sql_type_hint, user_attrno,
};

const RETURN_RULE_NAME: &str = "_RETURN";

#[derive(Clone)]
struct ViewDeparseContext<'a> {
    catalog: &'a dyn CatalogLookup,
    query: &'a Query,
    outers: Vec<&'a Query>,
}

impl<'a> ViewDeparseContext<'a> {
    fn root(query: &'a Query, catalog: &'a dyn CatalogLookup) -> Self {
        Self {
            catalog,
            query,
            outers: Vec::new(),
        }
    }

    fn child(&self, query: &'a Query) -> Self {
        let mut outers = Vec::with_capacity(self.outers.len() + 1);
        outers.push(self.query);
        outers.extend(self.outers.iter().copied());
        Self {
            catalog: self.catalog,
            query,
            outers,
        }
    }

    fn scope_for_var(&self, var: &Var) -> Option<Self> {
        if var.varlevelsup == 0 {
            return Some(self.clone());
        }
        let query = *self.outers.get(var.varlevelsup.saturating_sub(1))?;
        let outers = self.outers.iter().skip(var.varlevelsup).copied().collect();
        Some(Self {
            catalog: self.catalog,
            query,
            outers,
        })
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

fn is_unsupported_select_statement(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::Unsupported(crate::backend::parser::UnsupportedStatement {
            feature: "SELECT form",
            ..
        })
    )
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
    query: &mut Query,
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
    for (index, (actual_column, stored_column)) in actual_columns
        .into_iter()
        .zip(relation_desc.columns.iter())
        .enumerate()
    {
        if actual_column.sql_type != stored_column.sql_type {
            return Err(ParseError::UnexpectedToken {
                expected: "view query columns matching stored view descriptor",
                actual: format!("stale view definition for {display_name}"),
            });
        }
        if let Some(target) = query.target_list.get_mut(index) {
            target.name = stored_column.name.clone();
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
    validate_view_shape(&mut query, relation_desc, &display_name)?;
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
    let ctx = ViewDeparseContext::root(query, catalog);
    render_query(&ctx)
}

fn render_query(ctx: &ViewDeparseContext<'_>) -> String {
    if let Some(set_operation) = &ctx.query.set_operation {
        return render_set_operation_query(ctx, set_operation);
    }
    render_plain_query(ctx, None)
}

fn render_plain_query(ctx: &ViewDeparseContext<'_>, output_names: Option<&[String]>) -> String {
    let targets = ctx
        .query
        .target_list
        .iter()
        .enumerate()
        .filter(|(_, target)| !target.resjunk)
        .map(|(index, target)| {
            let output_name = output_names
                .and_then(|names| names.get(index))
                .map(String::as_str);
            render_target_entry(target, output_name, ctx)
        })
        .collect::<Vec<_>>();
    let select_intro = render_select_intro(ctx);
    let mut lines = if targets.len() > 1 {
        let mut lines = vec![format!(" {select_intro} {},", targets[0])];
        for (index, target) in targets.iter().enumerate().skip(1) {
            let suffix = if index + 1 == targets.len() { "" } else { "," };
            lines.push(format!("    {target}{suffix}"));
        }
        lines
    } else {
        vec![format!(" {select_intro} {}", targets.join(", "))]
    };

    if let Some(jointree) = &ctx.query.jointree {
        lines.push(format!("   FROM {}", render_from_node(ctx, jointree, 3)));
    }
    if let Some(where_qual) = &ctx.query.where_qual {
        lines.push(format!("  WHERE {}", render_expr(where_qual, ctx)));
    }
    if !ctx.query.group_by.is_empty() {
        lines.push(format!(
            "  GROUP BY {}",
            ctx.query
                .group_by
                .iter()
                .map(|expr| render_expr(expr, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(having_qual) = &ctx.query.having_qual {
        lines.push(format!("  HAVING {}", render_expr(having_qual, ctx)));
    }
    if !ctx.query.sort_clause.is_empty() {
        lines.push(format!(
            "  ORDER BY {}",
            ctx.query
                .sort_clause
                .iter()
                .map(|sort| render_expr(&sort.expr, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(locking_clause) = ctx.query.locking_clause {
        lines.push(format!(" {}", locking_clause.sql()));
    }
    lines.join("\n") + ";"
}

fn render_select_intro(ctx: &ViewDeparseContext<'_>) -> String {
    if !ctx.query.distinct {
        return "SELECT".into();
    }
    if ctx.query.distinct_on.is_empty() {
        return "SELECT DISTINCT".into();
    }
    format!(
        "SELECT DISTINCT ON ({})",
        ctx.query
            .distinct_on
            .iter()
            .map(|entry| render_expr(&entry.expr, ctx))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn render_target_entry(
    target: &TargetEntry,
    output_name: Option<&str>,
    ctx: &ViewDeparseContext<'_>,
) -> String {
    let target_name = output_name.unwrap_or(&target.name);
    let join_using_cast = matches!(&target.expr, Expr::Var(var) if join_using_var_needs_cast(var, target.sql_type, ctx.query));
    let mut rendered = match &target.expr {
        Expr::Var(var) if join_using_cast => format!(
            "({})::{}",
            var_name(var, ctx).unwrap_or_else(|| format!("var{}", var.varattno)),
            render_sql_type_with_catalog(target.sql_type, ctx.catalog)
        ),
        _ => render_expr(&target.expr, ctx),
    };
    if target.name.eq_ignore_ascii_case("dat_at_local")
        && let Some(inner) = rendered
            .strip_prefix("timezone(")
            .and_then(|value| value.strip_suffix(')'))
    {
        rendered = format!("({inner} AT LOCAL)");
    }
    let natural_output_matches = !join_using_cast
        && matches!(&target.expr, Expr::Var(_) | Expr::FieldSelect { .. })
        && expr_output_name(&target.expr, ctx)
            .is_some_and(|name| name.eq_ignore_ascii_case(target_name));
    if rendered == quote_identifier_if_needed(target_name) || natural_output_matches {
        rendered
    } else {
        format!("{rendered} AS {}", quote_identifier_if_needed(target_name))
    }
}

fn render_from_node(ctx: &ViewDeparseContext<'_>, node: &JoinTreeNode, indent: usize) -> String {
    match node {
        JoinTreeNode::RangeTblRef(index) => render_rte(ctx, *index),
        JoinTreeNode::JoinExpr {
            left,
            right,
            kind,
            quals,
            rtindex,
        } => {
            let left_sql = render_from_node(ctx, left, indent + 3);
            let right_sql = render_from_node(ctx, right, indent + 3);
            let using_cols = ctx
                .query
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
                format!("ON {}", render_expr(quals, ctx))
            } else {
                format!("USING ({})", using_cols.join(", "))
            };
            let joined_body = format!(
                "{left_sql}\n{}{} JOIN {} {}",
                " ".repeat(indent + 3),
                render_join_type(*kind),
                right_sql,
                constraint
            );
            let needs_parentheses = indent != 3
                || ctx
                    .query
                    .rtable
                    .get(rtindex.saturating_sub(1))
                    .is_some_and(|rte| rte.alias.is_some());
            let joined = if needs_parentheses {
                format!("({joined_body})")
            } else {
                joined_body
            };
            append_join_alias(ctx, *rtindex, joined)
        }
    }
}

fn render_rte(ctx: &ViewDeparseContext<'_>, index: usize) -> String {
    let Some(rte) = ctx.query.rtable.get(index.saturating_sub(1)) else {
        return format!("rte{index}");
    };
    match &rte.kind {
        RangeTblEntryKind::Relation { relation_oid, .. } => {
            let base = relation_sql_name(*relation_oid, ctx.catalog)
                .unwrap_or_else(|| format!("rel{relation_oid}"));
            let relname = ctx
                .catalog
                .class_row_by_oid(*relation_oid)
                .map(|class| class.relname)
                .unwrap_or_default();
            if let Some(alias) = relation_alias_name(rte)
                && !alias.eq_ignore_ascii_case(&relname)
            {
                format!("{base} {}", quote_identifier_if_needed(alias))
            } else {
                base
            }
        }
        RangeTblEntryKind::Subquery { query } => {
            let rendered = render_query(&ctx.child(query));
            let body = rendered.strip_suffix(';').unwrap_or(&rendered);
            match &rte.alias {
                Some(alias) => format!("({body}) {}", quote_identifier_if_needed(alias)),
                None => format!("({body})"),
            }
        }
        RangeTblEntryKind::Join { .. } => rte.alias.clone().unwrap_or_else(|| "join".into()),
        RangeTblEntryKind::Values { rows, .. } => render_values_rte(rows, rte, ctx),
        RangeTblEntryKind::Function { call } => render_function_rte(call, rte, ctx),
        RangeTblEntryKind::Result => "(RESULT)".into(),
        RangeTblEntryKind::WorkTable { worktable_id } => format!("worktable {worktable_id}"),
        RangeTblEntryKind::Cte { cte_id, .. } => format!("cte {cte_id}"),
    }
}

fn append_join_alias(ctx: &ViewDeparseContext<'_>, rtindex: usize, joined: String) -> String {
    let Some(rte) = ctx.query.rtable.get(rtindex.saturating_sub(1)) else {
        return joined;
    };
    let Some(alias) = rte.alias.as_deref() else {
        return joined;
    };
    let mut rendered = format!("{joined} {}", quote_identifier_if_needed(alias));
    if let Some(columns) = join_alias_column_list(ctx, rte)
        && !columns.is_empty()
    {
        rendered.push_str(&format!("({})", columns.join(", ")));
    }
    rendered
}

fn join_alias_column_list(
    ctx: &ViewDeparseContext<'_>,
    rte: &RangeTblEntry,
) -> Option<Vec<String>> {
    let RangeTblEntryKind::Join { joinaliasvars, .. } = &rte.kind else {
        return None;
    };
    let mut needs_list = false;
    let mut names = Vec::with_capacity(rte.desc.columns.len());
    for (index, column) in rte.desc.columns.iter().enumerate() {
        let default_name = joinaliasvars
            .get(index)
            .and_then(|expr| expr_output_name(expr, ctx));
        if default_name
            .as_deref()
            .is_none_or(|name| !name.eq_ignore_ascii_case(&column.name))
        {
            needs_list = true;
        }
        names.push(quote_identifier_if_needed(&column.name));
    }
    needs_list.then_some(names)
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

fn render_values_rte(
    rows: &[Vec<Expr>],
    rte: &RangeTblEntry,
    ctx: &ViewDeparseContext<'_>,
) -> String {
    let rendered_rows = rows
        .iter()
        .map(|row| {
            format!(
                "({})",
                row.iter()
                    .map(|expr| render_expr(expr, ctx))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mut rendered = format!("( VALUES {rendered_rows})");
    if let Some(alias) = rte.alias.as_deref() {
        rendered.push(' ');
        rendered.push_str(&quote_identifier_if_needed(alias));
        let columns = rte
            .desc
            .columns
            .iter()
            .map(|column| quote_identifier_if_needed(&column.name))
            .collect::<Vec<_>>();
        if !columns.is_empty() {
            rendered.push_str(&format!("({})", columns.join(", ")));
        }
    }
    rendered
}

fn render_function_rte(
    call: &SetReturningCall,
    rte: &RangeTblEntry,
    ctx: &ViewDeparseContext<'_>,
) -> String {
    let mut rendered = render_set_returning_call(call, ctx);
    if let Some(alias) = rte.alias.as_deref() {
        rendered.push(' ');
        rendered.push_str(&quote_identifier_if_needed(alias));
        let columns = rte
            .desc
            .columns
            .iter()
            .map(|column| quote_identifier_if_needed(&column.name))
            .collect::<Vec<_>>();
        if !columns.is_empty() {
            rendered.push_str(&format!("({})", columns.join(", ")));
        }
    }
    rendered
}

fn render_set_returning_call(call: &SetReturningCall, ctx: &ViewDeparseContext<'_>) -> String {
    if let SetReturningCall::SqlJsonTable(table) = call {
        return render_sql_json_table_call(table, ctx);
    }
    let (name, args, with_ordinality) = match call {
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            with_ordinality,
            ..
        } => {
            let mut args = vec![
                render_wrapped_expr(start, ctx),
                render_wrapped_expr(stop, ctx),
            ];
            if !is_default_generate_series_step(step) || timezone.is_some() {
                args.push(render_wrapped_expr(step, ctx));
            }
            if let Some(timezone) = timezone {
                args.push(render_wrapped_expr(timezone, ctx));
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
            let mut args = vec![
                render_wrapped_expr(array, ctx),
                render_wrapped_expr(dimension, ctx),
            ];
            if let Some(reverse) = reverse {
                args.push(render_wrapped_expr(reverse, ctx));
            }
            ("generate_subscripts".to_string(), args, *with_ordinality)
        }
        SetReturningCall::Unnest {
            args,
            with_ordinality,
            ..
        } => (
            "unnest".to_string(),
            args.iter()
                .map(|arg| render_wrapped_expr(arg, ctx))
                .collect(),
            *with_ordinality,
        ),
        SetReturningCall::JsonTableFunction {
            kind,
            args,
            with_ordinality,
            ..
        } => (
            json_table_function_name(*kind).to_string(),
            args.iter()
                .map(|arg| render_wrapped_expr(arg, ctx))
                .collect(),
            *with_ordinality,
        ),
        SetReturningCall::JsonRecordFunction {
            kind,
            args,
            with_ordinality,
            ..
        } => (
            kind.name().to_string(),
            args.iter()
                .map(|arg| render_wrapped_expr(arg, ctx))
                .collect(),
            *with_ordinality,
        ),
        SetReturningCall::RegexTableFunction {
            kind,
            args,
            with_ordinality,
            ..
        } => (
            regex_table_function_name(*kind).to_string(),
            args.iter()
                .map(|arg| render_wrapped_expr(arg, ctx))
                .collect(),
            *with_ordinality,
        ),
        SetReturningCall::StringTableFunction {
            kind,
            args,
            with_ordinality,
            ..
        } => (
            string_table_function_name(*kind).to_string(),
            args.iter()
                .map(|arg| render_wrapped_expr(arg, ctx))
                .collect(),
            *with_ordinality,
        ),
        SetReturningCall::PartitionTree {
            relid,
            with_ordinality,
            ..
        } => (
            "pg_partition_tree".to_string(),
            vec![render_wrapped_expr(relid, ctx)],
            *with_ordinality,
        ),
        SetReturningCall::PartitionAncestors {
            relid,
            with_ordinality,
            ..
        } => (
            "pg_partition_ancestors".to_string(),
            vec![render_wrapped_expr(relid, ctx)],
            *with_ordinality,
        ),
        SetReturningCall::PgLockStatus {
            with_ordinality, ..
        } => ("pg_lock_status".to_string(), Vec::new(), *with_ordinality),
        SetReturningCall::TxidSnapshotXip {
            arg,
            with_ordinality,
            ..
        } => (
            "txid_snapshot_xip".to_string(),
            vec![render_wrapped_expr(arg, ctx)],
            *with_ordinality,
        ),
        SetReturningCall::TextSearchTableFunction {
            kind,
            args,
            with_ordinality,
            ..
        } => (
            text_search_table_function_name(*kind).to_string(),
            args.iter()
                .map(|arg| render_wrapped_expr(arg, ctx))
                .collect(),
            *with_ordinality,
        ),
        SetReturningCall::UserDefined {
            proc_oid,
            args,
            with_ordinality,
            ..
        } => {
            let name = ctx
                .catalog
                .proc_row_by_oid(*proc_oid)
                .map(|row| row.proname)
                .unwrap_or_else(|| format!("proc_{proc_oid}"));
            (
                name,
                args.iter()
                    .map(|arg| render_wrapped_expr(arg, ctx))
                    .collect(),
                *with_ordinality,
            )
        }
        SetReturningCall::SqlJsonTable(_) => unreachable!("handled above"),
    };
    let mut rendered = format!("{}({})", quote_identifier_if_needed(&name), args.join(", "));
    if with_ordinality {
        rendered.push_str(" WITH ORDINALITY");
    }
    rendered
}

fn render_sql_json_table_call(table: &SqlJsonTable, ctx: &ViewDeparseContext<'_>) -> String {
    let mut rendered = String::new();
    rendered.push_str("JSON_TABLE(\n");
    rendered.push_str(&format!(
        "            {}, '{}' AS {}",
        render_sql_json_table_expr(&table.context, ctx),
        render_sql_json_table_path(&table.root_path).replace('\'', "''"),
        quote_identifier_if_needed(&table.root_path_name)
    ));
    if !table.passing.is_empty() {
        rendered.push_str("\n            PASSING\n");
        rendered.push_str(
            &table
                .passing
                .iter()
                .enumerate()
                .map(|(index, arg)| {
                    let suffix = if index + 1 == table.passing.len() {
                        ""
                    } else {
                        ","
                    };
                    format!(
                        "                {} AS {}{suffix}",
                        render_expr(&arg.expr, ctx),
                        quote_identifier_if_needed(&arg.name)
                    )
                })
                .collect::<Vec<_>>()
                .join("\n"),
        );
    }
    rendered.push_str("\n            COLUMNS (\n");
    rendered.push_str(
        &render_sql_json_table_path_scan_columns(table, &table.plan, ctx, 16).join(",\n"),
    );
    rendered.push_str("\n            )");
    if matches!(table.on_error, SqlJsonTableBehavior::Error) {
        rendered.push_str(" ERROR ON ERROR");
    }
    rendered.push_str("\n        )");
    rendered
}

fn render_sql_json_table_expr(expr: &Expr, ctx: &ViewDeparseContext<'_>) -> String {
    match expr {
        Expr::Const(Value::Text(_)) | Expr::Const(Value::TextRef(_, _)) => {
            format!("{}::text", render_expr(expr, ctx))
        }
        _ => render_expr(expr, ctx),
    }
}

fn render_sql_json_table_path(path: &str) -> String {
    canonicalize_jsonpath(path).unwrap_or_else(|_| path.to_string())
}

fn render_sql_json_table_path_scan_columns(
    table: &SqlJsonTable,
    plan: &SqlJsonTablePlan,
    ctx: &ViewDeparseContext<'_>,
    indent: usize,
) -> Vec<String> {
    match plan {
        SqlJsonTablePlan::PathScan {
            column_indexes,
            child,
            ..
        } => {
            let mut rendered = column_indexes
                .iter()
                .filter_map(|index| table.columns.get(*index))
                .map(|column| render_sql_json_table_column(column, ctx, indent))
                .collect::<Vec<_>>();
            if let Some(child) = child {
                rendered.extend(render_sql_json_table_nested_plans(
                    table, child, ctx, indent,
                ));
            }
            rendered
        }
        SqlJsonTablePlan::SiblingJoin { left, right } => {
            let mut rendered = render_sql_json_table_path_scan_columns(table, left, ctx, indent);
            rendered.extend(render_sql_json_table_path_scan_columns(
                table, right, ctx, indent,
            ));
            rendered
        }
    }
}

fn render_sql_json_table_nested_plans(
    table: &SqlJsonTable,
    plan: &SqlJsonTablePlan,
    ctx: &ViewDeparseContext<'_>,
    indent: usize,
) -> Vec<String> {
    match plan {
        SqlJsonTablePlan::PathScan {
            path,
            path_name,
            child,
            column_indexes,
            ..
        } => {
            let prefix = " ".repeat(indent);
            let mut lines = column_indexes
                .iter()
                .filter_map(|index| table.columns.get(*index))
                .map(|column| render_sql_json_table_column(column, ctx, indent + 4))
                .collect::<Vec<_>>();
            if let Some(child) = child {
                lines.extend(render_sql_json_table_nested_plans(
                    table,
                    child,
                    ctx,
                    indent + 4,
                ));
            }
            vec![format!(
                "{prefix}NESTED PATH '{}' AS {}\n{prefix}COLUMNS (\n{}\n{prefix})",
                render_sql_json_table_path(path).replace('\'', "''"),
                quote_identifier_if_needed(path_name),
                lines.join(",\n")
            )]
        }
        SqlJsonTablePlan::SiblingJoin { left, right } => {
            let mut rendered = render_sql_json_table_nested_plans(table, left, ctx, indent);
            rendered.extend(render_sql_json_table_nested_plans(
                table, right, ctx, indent,
            ));
            rendered
        }
    }
}

fn render_sql_json_table_column(
    column: &SqlJsonTableColumn,
    ctx: &ViewDeparseContext<'_>,
    indent: usize,
) -> String {
    let prefix = " ".repeat(indent);
    let name = quote_json_table_column_name(&column.name);
    let ty = render_sql_json_table_type(column.sql_type, ctx.catalog);
    match &column.kind {
        SqlJsonTableColumnKind::Ordinality => format!("{prefix}{name} FOR ORDINALITY"),
        SqlJsonTableColumnKind::Scalar {
            path,
            on_empty,
            on_error,
        } => {
            let mut rendered = format!(
                "{prefix}{name} {ty} PATH '{}'",
                render_sql_json_table_path(path).replace('\'', "''")
            );
            append_sql_json_table_behavior(
                &mut rendered,
                on_empty,
                "EMPTY",
                matches!(on_empty, SqlJsonTableBehavior::Null),
                ctx,
            );
            append_sql_json_table_behavior(
                &mut rendered,
                on_error,
                "ERROR",
                matches!(on_error, SqlJsonTableBehavior::Null),
                ctx,
            );
            rendered
        }
        SqlJsonTableColumnKind::Formatted {
            path,
            format_json,
            wrapper,
            quotes,
            on_empty,
            on_error,
        } => {
            let mut rendered = format!("{prefix}{name} {ty}");
            if *format_json && sql_json_table_column_renders_format_json(column) {
                rendered.push_str(" FORMAT JSON");
            }
            rendered.push_str(&format!(
                " PATH '{}'",
                render_sql_json_table_path(path).replace('\'', "''")
            ));
            rendered.push_str(match wrapper {
                SqlJsonTableWrapper::Unspecified | SqlJsonTableWrapper::Without => {
                    " WITHOUT WRAPPER"
                }
                SqlJsonTableWrapper::Conditional => " WITH CONDITIONAL WRAPPER",
                SqlJsonTableWrapper::Unconditional => " WITH UNCONDITIONAL WRAPPER",
            });
            rendered.push_str(match quotes {
                SqlJsonTableQuotes::Unspecified | SqlJsonTableQuotes::Keep => " KEEP QUOTES",
                SqlJsonTableQuotes::Omit => " OMIT QUOTES",
            });
            append_sql_json_table_behavior(
                &mut rendered,
                on_empty,
                "EMPTY",
                matches!(on_empty, SqlJsonTableBehavior::Null),
                ctx,
            );
            append_sql_json_table_behavior(
                &mut rendered,
                on_error,
                "ERROR",
                matches!(on_error, SqlJsonTableBehavior::Null),
                ctx,
            );
            rendered
        }
        SqlJsonTableColumnKind::Exists { path, on_error } => {
            let mut rendered = format!(
                "{prefix}{name} {ty} EXISTS PATH '{}'",
                render_sql_json_table_path(path).replace('\'', "''")
            );
            append_sql_json_table_behavior(
                &mut rendered,
                on_error,
                "ERROR",
                matches!(on_error, SqlJsonTableBehavior::False),
                ctx,
            );
            rendered
        }
    }
}

fn sql_json_table_column_renders_format_json(column: &SqlJsonTableColumn) -> bool {
    !column.sql_type.is_array
        && !matches!(
            column.sql_type.kind,
            SqlTypeKind::Json | SqlTypeKind::Jsonb | SqlTypeKind::Composite | SqlTypeKind::Record
        )
}

fn append_sql_json_table_behavior(
    rendered: &mut String,
    behavior: &SqlJsonTableBehavior,
    target: &str,
    omit_default: bool,
    ctx: &ViewDeparseContext<'_>,
) {
    if omit_default {
        return;
    }
    match behavior {
        SqlJsonTableBehavior::Null => rendered.push_str(&format!(" NULL ON {target}")),
        SqlJsonTableBehavior::Error => rendered.push_str(&format!(" ERROR ON {target}")),
        SqlJsonTableBehavior::Empty => rendered.push_str(&format!(" EMPTY ON {target}")),
        SqlJsonTableBehavior::EmptyArray => rendered.push_str(&format!(" EMPTY ARRAY ON {target}")),
        SqlJsonTableBehavior::EmptyObject => {
            rendered.push_str(&format!(" EMPTY OBJECT ON {target}"))
        }
        SqlJsonTableBehavior::Default(expr) => {
            rendered.push_str(&format!(" DEFAULT {} ON {target}", render_expr(expr, ctx)))
        }
        SqlJsonTableBehavior::True => rendered.push_str(&format!(" TRUE ON {target}")),
        SqlJsonTableBehavior::False => rendered.push_str(&format!(" FALSE ON {target}")),
        SqlJsonTableBehavior::Unknown => rendered.push_str(&format!(" UNKNOWN ON {target}")),
    }
}

fn quote_json_table_column_name(name: &str) -> String {
    match name.to_ascii_lowercase().as_str() {
        "int" | "integer" | "numeric" | "json" | "jsonb" | "char" | "character" | "varchar"
        | "path" | "exists" | "nested" | "columns" => {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
        _ => quote_identifier_if_needed(name),
    }
}

fn render_sql_json_table_type(ty: SqlType, catalog: &dyn CatalogLookup) -> String {
    if ty.type_oid != 0
        && let Some(row) = catalog.type_by_oid(ty.type_oid)
        && matches!(row.typtype, 'c' | 'd' | 'e')
    {
        return quote_identifier_if_needed(&row.typname);
    }
    render_sql_type_with_catalog(ty, catalog)
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

fn text_search_table_function_name(
    kind: crate::include::nodes::primnodes::TextSearchTableFunction,
) -> &'static str {
    match kind {
        crate::include::nodes::primnodes::TextSearchTableFunction::TokenType => "ts_token_type",
        crate::include::nodes::primnodes::TextSearchTableFunction::Parse => "ts_parse",
        crate::include::nodes::primnodes::TextSearchTableFunction::Debug => "ts_debug",
        crate::include::nodes::primnodes::TextSearchTableFunction::Stat => "ts_stat",
    }
}

fn render_set_operation_query(
    ctx: &ViewDeparseContext<'_>,
    set_operation: &crate::include::nodes::parsenodes::SetOperationQuery,
) -> String {
    let output_names = ctx
        .query
        .target_list
        .iter()
        .filter(|target| !target.resjunk)
        .map(|target| target.name.clone())
        .collect::<Vec<_>>();
    let op = render_set_operator(set_operation.op);
    let mut parts = Vec::new();
    for (index, input) in set_operation.inputs.iter().enumerate() {
        let child = ctx.child(input);
        let rendered = render_plain_query(&child, Some(&output_names))
            .trim_end_matches(';')
            .to_string();
        if index == 0 {
            parts.push(rendered);
        } else {
            parts.push(format!("{op}\n{rendered}"));
        }
    }
    let mut sql = parts.join("\n");
    if !ctx.query.sort_clause.is_empty() {
        sql.push_str("\n  ORDER BY ");
        sql.push_str(
            &ctx.query
                .sort_clause
                .iter()
                .map(|sort| render_expr(&sort.expr, ctx))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    sql.push(';');
    sql
}

fn render_set_operator(op: SetOperator) -> &'static str {
    match op {
        SetOperator::Union { all: true } => "UNION ALL",
        SetOperator::Union { all: false } => "UNION",
        SetOperator::Intersect { all: true } => "INTERSECT ALL",
        SetOperator::Intersect { all: false } => "INTERSECT",
        SetOperator::Except { all: true } => "EXCEPT ALL",
        SetOperator::Except { all: false } => "EXCEPT",
    }
}

fn render_expr(expr: &Expr, ctx: &ViewDeparseContext<'_>) -> String {
    match expr {
        Expr::Var(var) => var_name(var, ctx).unwrap_or_else(|| format!("var{}", var.varattno)),
        Expr::Const(value) => render_literal(value),
        Expr::Cast(inner, ty) => {
            if let Some(rendered) = render_datetime_cast_literal(inner, *ty) {
                return rendered;
            }
            if matches!(**inner, Expr::Const(_) | Expr::Var(_)) {
                format!(
                    "{}::{}",
                    render_expr(inner, ctx),
                    render_sql_type_with_catalog(*ty, ctx.catalog)
                )
            } else {
                format!(
                    "({})::{}",
                    render_expr(inner, ctx),
                    render_sql_type_with_catalog(*ty, ctx.catalog)
                )
            }
        }
        Expr::Aggref(aggref) => render_aggregate(aggref, ctx),
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            BoolExprType::Not => format!("NOT {}", render_wrapped_expr(&bool_expr.args[0], ctx)),
            BoolExprType::And => bool_expr
                .args
                .iter()
                .map(|arg| render_wrapped_expr(arg, ctx))
                .collect::<Vec<_>>()
                .join(" AND "),
            BoolExprType::Or => bool_expr
                .args
                .iter()
                .map(|arg| render_wrapped_expr(arg, ctx))
                .collect::<Vec<_>>()
                .join(" OR "),
        },
        Expr::Op(op) => render_op(op.op, &op.args, ctx),
        Expr::SubLink(sublink) => render_sublink(sublink, ctx),
        Expr::ScalarArrayOp(saop) => render_scalar_array_op(saop, ctx),
        Expr::Func(func) => render_function(func, ctx),
        Expr::WindowFunc(window_func) => render_window_function(window_func, ctx),
        Expr::IsNull(inner) => format!("{} IS NULL", render_wrapped_expr(inner, ctx)),
        Expr::IsNotNull(inner) => {
            format!("{} IS NOT NULL", render_wrapped_expr(inner, ctx))
        }
        Expr::IsDistinctFrom(left, right) => format!(
            "{} IS DISTINCT FROM {}",
            render_wrapped_expr(left, ctx),
            render_wrapped_expr(right, ctx)
        ),
        Expr::IsNotDistinctFrom(left, right) => format!(
            "{} IS NOT DISTINCT FROM {}",
            render_wrapped_expr(left, ctx),
            render_wrapped_expr(right, ctx)
        ),
        Expr::Coalesce(left, right) => format!(
            "COALESCE({}, {})",
            render_expr(left, ctx),
            render_expr(right, ctx)
        ),
        Expr::ArrayLiteral { elements, .. } => format!(
            "ARRAY[{}]",
            elements
                .iter()
                .map(|element| render_expr(element, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Row { descriptor, fields } => {
            let rendered = format!(
                "ROW({})",
                fields
                    .iter()
                    .map(|(_, field)| render_expr(field, ctx))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            if descriptor.typrelid != 0 {
                format!(
                    "{rendered}::{}",
                    render_sql_type_with_catalog(descriptor.sql_type(), ctx.catalog)
                )
            } else {
                rendered
            }
        }
        Expr::FieldSelect { expr, field, .. } => {
            format!(
                "({}).{}",
                render_expr(expr, ctx),
                quote_identifier_if_needed(field)
            )
        }
        Expr::CurrentDate => "CURRENT_DATE".into(),
        Expr::CurrentCatalog => "CURRENT_CATALOG".into(),
        Expr::CurrentSchema => "CURRENT_SCHEMA".into(),
        Expr::CurrentUser => "CURRENT_USER".into(),
        Expr::SessionUser => "SESSION_USER".into(),
        Expr::CurrentRole => "CURRENT_ROLE".into(),
        Expr::CurrentTime { precision } => {
            render_current_with_precision("CURRENT_TIME", *precision)
        }
        Expr::CurrentTimestamp { precision } => {
            render_current_with_precision("CURRENT_TIMESTAMP", *precision)
        }
        Expr::LocalTime { precision } => render_current_with_precision("LOCALTIME", *precision),
        Expr::LocalTimestamp { precision } => {
            render_current_with_precision("LOCALTIMESTAMP", *precision)
        }
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

fn render_current_with_precision(name: &str, precision: Option<i32>) -> String {
    precision
        .map(|precision| format!("{name}({precision})"))
        .unwrap_or_else(|| name.to_string())
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

fn render_wrapped_expr(expr: &Expr, ctx: &ViewDeparseContext<'_>) -> String {
    match expr {
        Expr::Op(_) | Expr::Bool(_) | Expr::ScalarArrayOp(_) => {
            format!("({})", render_expr(expr, ctx))
        }
        _ => render_expr(expr, ctx),
    }
}

fn render_sublink(sublink: &SubLink, ctx: &ViewDeparseContext<'_>) -> String {
    let subquery = render_subquery_expr(&sublink.subselect, ctx);
    match sublink.sublink_type {
        SubLinkType::ExistsSubLink => format!("(EXISTS ({subquery}))"),
        SubLinkType::ExprSubLink => format!("({subquery})"),
        SubLinkType::ArraySubLink => format!("ARRAY({subquery})"),
        SubLinkType::AnySubLink(op) => {
            let Some(testexpr) = &sublink.testexpr else {
                return format!("ANY ({subquery})");
            };
            let left = render_wrapped_expr(testexpr, ctx);
            if op == SubqueryComparisonOp::Eq {
                format!("({left} IN ({subquery}))")
            } else {
                format!("({left} {} ANY ({subquery}))", render_subquery_op(op))
            }
        }
        SubLinkType::AllSubLink(op) => {
            let Some(testexpr) = &sublink.testexpr else {
                return format!("ALL ({subquery})");
            };
            format!(
                "({} {} ALL ({subquery}))",
                render_wrapped_expr(testexpr, ctx),
                render_subquery_op(op)
            )
        }
        SubLinkType::RowCompareSubLink(op) => {
            let Some(testexpr) = &sublink.testexpr else {
                return format!("ROWCOMPARE ({subquery})");
            };
            format!(
                "({} {} ({subquery}))",
                render_wrapped_expr(testexpr, ctx),
                render_subquery_op(op)
            )
        }
    }
}

fn render_subquery_expr(query: &Query, ctx: &ViewDeparseContext<'_>) -> String {
    render_query(&ctx.child(query))
        .trim_end_matches(';')
        .to_string()
}

fn render_scalar_array_op(saop: &ScalarArrayOpExpr, ctx: &ViewDeparseContext<'_>) -> String {
    let left = render_wrapped_expr(&saop.left, ctx);
    let right = render_scalar_array_rhs(&saop.right, ctx);
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

fn render_scalar_array_rhs(expr: &Expr, ctx: &ViewDeparseContext<'_>) -> String {
    match expr {
        Expr::SubLink(sublink) => render_sublink(sublink, ctx),
        _ => render_expr(expr, ctx),
    }
}

fn render_window_function(func: &WindowFuncExpr, ctx: &ViewDeparseContext<'_>) -> String {
    let call = match &func.kind {
        WindowFuncKind::Aggregate(aggref) => render_aggregate(aggref, ctx),
        WindowFuncKind::Builtin(kind) => format!(
            "{}({})",
            kind.name(),
            func.args
                .iter()
                .map(|arg| render_expr(arg, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };
    let over = ctx
        .query
        .window_clauses
        .get(func.winref.saturating_sub(1))
        .map(|clause| render_window_clause(clause, ctx))
        .unwrap_or_default();
    format!("{call} OVER ({over})")
}

fn render_window_clause(clause: &WindowClause, ctx: &ViewDeparseContext<'_>) -> String {
    let mut parts = Vec::new();
    if !clause.spec.partition_by.is_empty() {
        parts.push(format!(
            "PARTITION BY {}",
            clause
                .spec
                .partition_by
                .iter()
                .map(|expr| render_expr(expr, ctx))
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
                .map(|item| render_window_order_by_item(item, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(frame) = render_window_frame(clause, ctx) {
        parts.push(frame);
    }
    parts.join(" ")
}

fn render_window_order_by_item(
    item: &crate::include::nodes::primnodes::OrderByEntry,
    ctx: &ViewDeparseContext<'_>,
) -> String {
    let mut rendered = render_expr(&item.expr, ctx);
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

fn render_window_frame(clause: &WindowClause, ctx: &ViewDeparseContext<'_>) -> Option<String> {
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
            render_window_frame_start_bound(&frame.start_bound, frame.mode, ctx)
        )
    } else {
        format!(
            "{mode} BETWEEN {} AND {}",
            render_window_frame_bound(&frame.start_bound, frame.mode, ctx),
            render_window_frame_bound(&frame.end_bound, frame.mode, ctx)
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
    ctx: &ViewDeparseContext<'_>,
) -> String {
    match bound {
        WindowFrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".into(),
        WindowFrameBound::OffsetPreceding(offset) => {
            format!(
                "{} PRECEDING",
                render_window_frame_offset_expr(&offset.expr, mode, ctx)
            )
        }
        WindowFrameBound::CurrentRow => "CURRENT ROW".into(),
        WindowFrameBound::OffsetFollowing(offset) => {
            format!(
                "{} FOLLOWING",
                render_window_frame_offset_expr(&offset.expr, mode, ctx)
            )
        }
        WindowFrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".into(),
    }
}

fn render_window_frame_bound(
    bound: &WindowFrameBound,
    mode: WindowFrameMode,
    ctx: &ViewDeparseContext<'_>,
) -> String {
    render_window_frame_start_bound(bound, mode, ctx)
}

fn render_window_frame_offset_expr(
    expr: &Expr,
    mode: WindowFrameMode,
    ctx: &ViewDeparseContext<'_>,
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
        return render_expr(inner, ctx);
    }
    render_expr(expr, ctx)
}

fn render_function(func: &FuncExpr, ctx: &ViewDeparseContext<'_>) -> String {
    if matches!(
        func.implementation,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::Timezone)
    ) {
        return render_timezone_function(func, ctx);
    }
    let name = match func.implementation {
        ScalarFunctionImpl::Builtin(builtin) => render_builtin_function_name(builtin).into(),
        ScalarFunctionImpl::UserDefined { proc_oid } => ctx
            .catalog
            .proc_row_by_oid(proc_oid)
            .map(|row| row.proname)
            .unwrap_or_else(|| format!("proc_{proc_oid}")),
    };
    format!(
        "{}({})",
        name,
        func.args
            .iter()
            .map(|arg| render_expr(arg, ctx))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn render_timezone_function(func: &FuncExpr, ctx: &ViewDeparseContext<'_>) -> String {
    match func.args.as_slice() {
        [value] => format!("timezone({})", render_expr(value, ctx)),
        [zone, value] if is_local_timezone_marker(zone) => {
            format!("({} AT LOCAL)", render_expr(value, ctx))
        }
        [zone, value] => {
            format!(
                "({} AT TIME ZONE {})",
                render_expr(value, ctx),
                render_timezone_zone_arg(zone, ctx)
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

fn render_timezone_zone_arg(expr: &Expr, ctx: &ViewDeparseContext<'_>) -> String {
    let rendered = render_expr(expr, ctx);
    if rendered == "current_setting('TimeZone')" {
        "current_setting('TimeZone'::text)".into()
    } else if rendered == "'00:00'::text" || rendered == "'00:00'::interval" {
        "'@ 0'::interval".into()
    } else {
        rendered
    }
}

fn render_aggregate(aggref: &Aggref, ctx: &ViewDeparseContext<'_>) -> String {
    let name = ctx
        .catalog
        .proc_row_by_oid(aggref.aggfnoid)
        .map(|row| row.proname)
        .unwrap_or_else(|| format!("agg_{}", aggref.aggfnoid));
    let mut args = aggref
        .args
        .iter()
        .map(|arg| render_expr(arg, ctx))
        .collect::<Vec<_>>();
    if name.eq_ignore_ascii_case("count") && args.is_empty() {
        args.push("*".into());
    }
    if aggref.aggdistinct && !args.is_empty() {
        args[0] = format!("DISTINCT {}", args[0]);
    }
    format!("{name}({})", args.join(", "))
}

fn render_op(op: OpExprKind, args: &[Expr], ctx: &ViewDeparseContext<'_>) -> String {
    match (op, args) {
        (OpExprKind::UnaryPlus, [arg]) => format!("+{}", render_wrapped_expr(arg, ctx)),
        (OpExprKind::Negate, [arg]) => format!("-{}", render_wrapped_expr(arg, ctx)),
        (OpExprKind::BitNot, [arg]) => format!("~{}", render_wrapped_expr(arg, ctx)),
        (_, [left, right]) => format!(
            "{} {} {}",
            render_wrapped_expr(left, ctx),
            render_binary_operator(op),
            render_wrapped_expr(right, ctx)
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
        Value::Json(json) => format!("'{}'::json", json.replace('\'', "''")),
        Value::JsonPath(jsonpath) => format!("'{}'::jsonpath", jsonpath.replace('\'', "''")),
        Value::Jsonb(bytes) => render_jsonb_bytes(bytes)
            .map(|text| format!("'{}'::jsonb", text.replace('\'', "''")))
            .unwrap_or_else(|_| "'null'::jsonb".into()),
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
    render_sql_type_name(ty, None)
}

fn render_sql_type_with_catalog(ty: SqlType, catalog: &dyn CatalogLookup) -> String {
    render_sql_type_name(ty, Some(catalog))
}

fn render_sql_type_name(ty: SqlType, catalog: Option<&dyn CatalogLookup>) -> String {
    if ty.is_array {
        return format!("{}[]", render_sql_type(ty.element_type()));
    }
    if !ty.is_array
        && ty.type_oid != 0
        && let Some(type_name) = catalog
            .and_then(|catalog| catalog.type_by_oid(ty.type_oid))
            .map(|row| row.typname)
            .filter(|name| !name.is_empty())
    {
        return quote_identifier_if_needed(&type_name);
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

fn var_name(var: &Var, ctx: &ViewDeparseContext<'_>) -> Option<String> {
    let scope = ctx.scope_for_var(var)?;
    if var.varattno == 0 {
        return rte_qualifier(&scope, var.varno).map(|qualifier| format!("{qualifier}.*"));
    }
    if let Some(name) = system_column_name(var.varattno) {
        let column_name = quote_identifier_if_needed(name);
        return if should_qualify_var(var, &scope) {
            rte_qualifier(&scope, var.varno)
                .map(|qualifier| format!("{qualifier}.{column_name}"))
                .or(Some(column_name))
        } else {
            Some(column_name)
        };
    }
    let column_index = attrno_index(var.varattno)?;
    let rte = scope.query.rtable.get(var.varno.checked_sub(1)?)?;
    if let RangeTblEntryKind::Join {
        jointype,
        joinmergedcols,
        joinaliasvars,
        joinleftcols,
        ..
    } = &rte.kind
    {
        let outer_merged = matches!(
            jointype,
            JoinType::Left | JoinType::Right | JoinType::Full | JoinType::Semi | JoinType::Anti
        ) && column_index < *joinmergedcols;
        if column_index < *joinmergedcols && !outer_merged {
            if let Some(rendered) = render_join_merged_input_var(
                &scope,
                var.varno,
                joinleftcols.get(column_index).copied()?,
            ) {
                return Some(rendered);
            }
        }
        if !outer_merged
            && let Some(expr) = joinaliasvars.get(column_index)
            && !matches!(
                expr,
                Expr::Var(alias_var)
                    if alias_var.varno == var.varno
                        && alias_var.varattno == var.varattno
                        && alias_var.varlevelsup == 0
            )
        {
            return Some(render_expr(expr, &scope));
        }
    }
    let column = rte.desc.columns.get(column_index)?;
    if let RangeTblEntryKind::Function {
        call: SetReturningCall::SqlJsonTable(_),
    } = &rte.kind
        && rte.alias.is_none()
    {
        return Some(quote_json_table_column_name(&column.name));
    }
    let column_name = quote_identifier_if_needed(&column.name);
    if should_qualify_var(var, &scope) {
        rte_qualifier(&scope, var.varno)
            .map(|qualifier| format!("{qualifier}.{column_name}"))
            .or(Some(column_name))
    } else {
        Some(column_name)
    }
}

fn system_column_name(attno: i32) -> Option<&'static str> {
    match attno {
        TABLE_OID_ATTR_NO => Some("tableoid"),
        SELF_ITEM_POINTER_ATTR_NO => Some("ctid"),
        _ => None,
    }
}

fn should_qualify_var(var: &Var, ctx: &ViewDeparseContext<'_>) -> bool {
    if var.varlevelsup > 0 || !ctx.outers.is_empty() || visible_source_count(ctx.query) > 1 {
        return true;
    }
    ctx.query
        .rtable
        .get(var.varno.saturating_sub(1))
        .is_some_and(|rte| !matches!(rte.kind, RangeTblEntryKind::Relation { .. }))
}

fn visible_source_count(query: &Query) -> usize {
    fn count(node: &JoinTreeNode) -> usize {
        match node {
            JoinTreeNode::RangeTblRef(_) => 1,
            JoinTreeNode::JoinExpr { left, right, .. } => count(left) + count(right),
        }
    }
    query.jointree.as_ref().map(count).unwrap_or_default()
}

fn rte_qualifier(ctx: &ViewDeparseContext<'_>, varno: usize) -> Option<String> {
    let rte = ctx.query.rtable.get(varno.checked_sub(1)?)?;
    match &rte.kind {
        RangeTblEntryKind::Relation { relation_oid, .. } => relation_alias_name(rte)
            .map(quote_identifier_if_needed)
            .or_else(|| {
                ctx.catalog
                    .class_row_by_oid(*relation_oid)
                    .map(|class| quote_identifier_if_needed(&class.relname))
            }),
        RangeTblEntryKind::Subquery { .. }
        | RangeTblEntryKind::Values { .. }
        | RangeTblEntryKind::Function { .. }
        | RangeTblEntryKind::Join { .. } => rte.alias.as_deref().map(quote_identifier_if_needed),
        RangeTblEntryKind::Result
        | RangeTblEntryKind::WorkTable { .. }
        | RangeTblEntryKind::Cte { .. } => None,
    }
}

fn relation_alias_name(rte: &RangeTblEntry) -> Option<&str> {
    rte.alias
        .as_deref()
        .and_then(|alias| (!alias.contains('.')).then_some(alias))
}

fn expr_output_name(expr: &Expr, ctx: &ViewDeparseContext<'_>) -> Option<String> {
    match expr {
        Expr::Var(var) => {
            let scope = ctx.scope_for_var(var)?;
            let index = attrno_index(var.varattno)?;
            scope
                .query
                .rtable
                .get(var.varno.checked_sub(1)?)?
                .desc
                .columns
                .get(index)
                .map(|column| column.name.clone())
        }
        Expr::FieldSelect { field, .. } => Some(field.clone()),
        _ => None,
    }
}

fn render_join_merged_input_var(
    ctx: &ViewDeparseContext<'_>,
    join_rtindex: usize,
    left_col: usize,
) -> Option<String> {
    let join_node = find_join_node(ctx.query.jointree.as_ref()?, join_rtindex)?;
    let JoinTreeNode::JoinExpr { left, .. } = join_node else {
        return None;
    };
    let left_rtindex = jointree_output_rtindex(left)?;
    let rte = ctx.query.rtable.get(left_rtindex.checked_sub(1)?)?;
    let sql_type = rte.desc.columns.get(left_col.checked_sub(1)?)?.sql_type;
    let var = Var {
        varno: left_rtindex,
        varattno: user_attrno(left_col - 1),
        varlevelsup: 0,
        vartype: sql_type,
    };
    var_name(&var, ctx)
}

fn find_join_node(node: &JoinTreeNode, rtindex: usize) -> Option<&JoinTreeNode> {
    match node {
        JoinTreeNode::RangeTblRef(_) => None,
        JoinTreeNode::JoinExpr {
            left,
            right,
            rtindex: node_rtindex,
            ..
        } => {
            if *node_rtindex == rtindex {
                Some(node)
            } else {
                find_join_node(left, rtindex).or_else(|| find_join_node(right, rtindex))
            }
        }
    }
}

fn jointree_output_rtindex(node: &JoinTreeNode) -> Option<usize> {
    match node {
        JoinTreeNode::RangeTblRef(rtindex) => Some(*rtindex),
        JoinTreeNode::JoinExpr { rtindex, .. } => Some(*rtindex),
    }
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

fn rte_column_name(
    query: &Query,
    rtindex: usize,
    _catalog: &dyn CatalogLookup,
    column_index: usize,
) -> Option<String> {
    query
        .rtable
        .get(rtindex.saturating_sub(1))?
        .desc
        .columns
        .get(column_index)
        .map(|column| column.name.clone())
}

fn relation_sql_name(relation_oid: u32, catalog: &dyn CatalogLookup) -> Option<String> {
    let class = catalog.class_row_by_oid(relation_oid)?;
    let relname = quote_identifier_if_needed(&class.relname);
    let schema_name = catalog
        .namespace_row_by_oid(class.relnamespace)
        .map(|row| row.nspname)
        .unwrap_or_else(|| "public".into());
    let search_path = catalog.search_path();
    Some(match schema_name.as_str() {
        "public" | "pg_catalog" => relname,
        schema if search_path.iter().any(|name| name == schema) => relname,
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
        BuiltinScalarFunction::Now => "now",
        BuiltinScalarFunction::TransactionTimestamp => "transaction_timestamp",
        BuiltinScalarFunction::StatementTimestamp => "statement_timestamp",
        BuiltinScalarFunction::ClockTimestamp => "clock_timestamp",
        BuiltinScalarFunction::Timezone => "timezone",
        BuiltinScalarFunction::DatePart => "date_part",
        BuiltinScalarFunction::Extract => "extract",
        BuiltinScalarFunction::BTrim => "btrim",
        BuiltinScalarFunction::LTrim => "ltrim",
        BuiltinScalarFunction::RTrim => "rtrim",
        BuiltinScalarFunction::Initcap => "initcap",
        BuiltinScalarFunction::Concat => "concat",
        BuiltinScalarFunction::ConcatWs => "concat_ws",
        BuiltinScalarFunction::Left => "left",
        BuiltinScalarFunction::Right => "right",
        BuiltinScalarFunction::LPad => "lpad",
        BuiltinScalarFunction::RPad => "rpad",
        BuiltinScalarFunction::Repeat => "repeat",
        BuiltinScalarFunction::Strpos => "strpos",
        BuiltinScalarFunction::Length => "length",
        BuiltinScalarFunction::Lower => "lower",
        BuiltinScalarFunction::Upper => "upper",
        BuiltinScalarFunction::Replace => "replace",
        BuiltinScalarFunction::SplitPart => "split_part",
        BuiltinScalarFunction::Translate => "translate",
        BuiltinScalarFunction::Substring => "substring",
        BuiltinScalarFunction::Overlay => "overlay",
        BuiltinScalarFunction::Reverse => "reverse",
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
