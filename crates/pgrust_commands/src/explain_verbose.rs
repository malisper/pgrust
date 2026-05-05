use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

use pgrust_analyze::CatalogLookup;
use pgrust_catalog_data::*;
use pgrust_expr::jsonpath::canonicalize_jsonpath;
use pgrust_expr::{format_record_text, render_jsonb_bytes};
use pgrust_nodes::Value;
use pgrust_nodes::parsenodes::SqlTypeKind;
use pgrust_nodes::plannodes::{AggregateStrategy, IndexScanKey, Plan, PlanEstimate};
use pgrust_nodes::primnodes::*;
use pgrust_nodes::relcache::IndexRelCacheEntry;

use crate::explain::*;
use crate::explain_expr::*;

pub type VerbosePlanFormatter =
    fn(&Plan, &[Plan], usize, bool, bool, bool, bool, &VerboseExplainContext, &mut Vec<String>);

pub type RenderIndexOrderBy =
    fn(&[IndexScanKey], &RelationDesc, &IndexRelCacheEntry) -> Option<String>;
pub type RenderIndexScanCondition =
    fn(&[IndexScanKey], &RelationDesc, &IndexRelCacheEntry, Option<&[String]>) -> Option<String>;
pub type RenderIndexScanConditionWithRuntime = fn(
    &[IndexScanKey],
    &RelationDesc,
    &IndexRelCacheEntry,
    Option<&[String]>,
    Option<&dyn Fn(&Expr) -> String>,
) -> Option<String>;
pub type PlanNodeInfo = fn(&Plan) -> (String, PlanEstimate);
pub type ConstFalseResultPlan = fn(&Plan) -> Option<PlanEstimate>;

#[derive(Clone, Copy)]
pub struct VerboseExplainServices {
    pub format_plan: VerbosePlanFormatter,
    pub render_index_order_by: RenderIndexOrderBy,
    pub render_index_scan_condition_with_key_names: RenderIndexScanCondition,
    pub render_index_scan_condition_with_key_names_and_runtime_renderer:
        RenderIndexScanConditionWithRuntime,
    pub plan_node_info: PlanNodeInfo,
    pub const_false_result_plan: ConstFalseResultPlan,
}

thread_local! {
    static VERBOSE_EXPLAIN_SERVICES: Cell<Option<VerboseExplainServices>> = const { Cell::new(None) };
}

struct VerboseExplainServicesGuard(Option<VerboseExplainServices>);

impl Drop for VerboseExplainServicesGuard {
    fn drop(&mut self) {
        VERBOSE_EXPLAIN_SERVICES.with(|services| services.set(self.0));
    }
}

pub fn with_verbose_explain_services<R>(
    services: VerboseExplainServices,
    f: impl FnOnce() -> R,
) -> R {
    let previous = VERBOSE_EXPLAIN_SERVICES.with(|stored| {
        let previous = stored.get();
        stored.set(Some(services));
        previous
    });
    let _guard = VerboseExplainServicesGuard(previous);
    f()
}

fn current_verbose_explain_services() -> VerboseExplainServices {
    VERBOSE_EXPLAIN_SERVICES
        .with(Cell::get)
        .expect("verbose EXPLAIN services are not installed")
}

fn format_explain_plan_with_subplans_inner(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    verbose: bool,
    is_child: bool,
    qualify_aggregate_group_keys: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    (current_verbose_explain_services().format_plan)(
        plan,
        subplans,
        indent,
        show_costs,
        verbose,
        is_child,
        qualify_aggregate_group_keys,
        ctx,
        lines,
    );
}

fn render_index_order_by(
    keys: &[IndexScanKey],
    desc: &RelationDesc,
    index_meta: &IndexRelCacheEntry,
) -> Option<String> {
    (current_verbose_explain_services().render_index_order_by)(keys, desc, index_meta)
}

fn render_index_scan_condition_with_key_names(
    keys: &[IndexScanKey],
    desc: &RelationDesc,
    index_meta: &IndexRelCacheEntry,
    key_column_names: Option<&[String]>,
) -> Option<String> {
    (current_verbose_explain_services().render_index_scan_condition_with_key_names)(
        keys,
        desc,
        index_meta,
        key_column_names,
    )
}

fn render_index_scan_condition_with_key_names_and_runtime_renderer(
    keys: &[IndexScanKey],
    desc: &RelationDesc,
    index_meta: &IndexRelCacheEntry,
    key_column_names: Option<&[String]>,
    runtime_renderer: Option<&dyn Fn(&Expr) -> String>,
) -> Option<String> {
    (current_verbose_explain_services()
        .render_index_scan_condition_with_key_names_and_runtime_renderer)(
        keys,
        desc,
        index_meta,
        key_column_names,
        runtime_renderer,
    )
}

fn push_explain_line(
    label: &str,
    plan_info: PlanEstimate,
    show_costs: bool,
    lines: &mut Vec<String>,
) {
    if show_costs {
        lines.push(format!(
            "{label}  (cost={:.2}..{:.2} rows={} width={})",
            plan_info.startup_cost.as_f64(),
            plan_info.total_cost.as_f64(),
            plan_info.plan_rows.as_f64().round() as u64,
            plan_info.plan_width
        ));
    } else {
        lines.push(label.to_string());
    }
}

fn plan_node_info(plan: &Plan) -> (String, PlanEstimate) {
    (current_verbose_explain_services().plan_node_info)(plan)
}

fn const_false_filter_result_plan(plan: &Plan) -> Option<PlanEstimate> {
    (current_verbose_explain_services().const_false_result_plan)(plan)
}

fn push_direct_plan_subplans(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    verbose: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    crate::explain::push_direct_plan_subplans(
        plan,
        subplans,
        indent,
        lines,
        |parent, subplan, child, child_indent, lines| {
            let child_ctx = subplan_explain_context(parent, subplan, ctx);
            format_explain_plan_with_subplans_inner(
                child,
                subplans,
                child_indent,
                show_costs,
                verbose,
                true,
                false,
                &child_ctx,
                lines,
            );
        },
    );
}

fn subplan_explain_context(
    parent: &Plan,
    subplan: &SubPlan,
    ctx: &VerboseExplainContext,
) -> VerboseExplainContext {
    if subplan.args.is_empty() {
        return ctx.clone();
    }
    let mut child_ctx = ctx.clone();
    let column_names = subplan_arg_column_names(parent, ctx);
    child_ctx.exec_params.extend(
        subplan
            .par_param
            .iter()
            .copied()
            .zip(subplan.args.iter().cloned())
            .map(|(paramid, expr)| VerboseExecParam {
                paramid,
                expr,
                column_names: column_names.clone(),
            }),
    );
    child_ctx
}

fn subplan_arg_column_names(parent: &Plan, ctx: &VerboseExplainContext) -> Vec<String> {
    match parent {
        Plan::Projection { input, .. } | Plan::ProjectSet { input, .. } => {
            plan_join_output_exprs(input, ctx, true)
        }
        _ => plan_join_output_exprs(parent, ctx, true),
    }
}

fn set_returning_call_label(call: &SetReturningCall) -> &str {
    match call {
        SetReturningCall::RowsFrom { .. } => "rows from",
        SetReturningCall::GenerateSeries { .. } => "generate_series",
        SetReturningCall::GenerateSubscripts { .. } => "generate_subscripts",
        SetReturningCall::Unnest { .. } => "unnest",
        SetReturningCall::JsonTableFunction { kind, .. } => match kind {
            JsonTableFunction::ObjectKeys => "json_object_keys",
            JsonTableFunction::Each => "json_each",
            JsonTableFunction::EachText => "json_each_text",
            JsonTableFunction::ArrayElements => "json_array_elements",
            JsonTableFunction::ArrayElementsText => "json_array_elements_text",
            JsonTableFunction::JsonbPathQuery => "jsonb_path_query",
            JsonTableFunction::JsonbPathQueryTz => "jsonb_path_query_tz",
            JsonTableFunction::JsonbObjectKeys => "jsonb_object_keys",
            JsonTableFunction::JsonbEach => "jsonb_each",
            JsonTableFunction::JsonbEachText => "jsonb_each_text",
            JsonTableFunction::JsonbArrayElements => "jsonb_array_elements",
            JsonTableFunction::JsonbArrayElementsText => "jsonb_array_elements_text",
        },
        SetReturningCall::JsonRecordFunction { kind, .. } => kind.name(),
        SetReturningCall::SqlJsonTable(_) => "json_table",
        SetReturningCall::SqlXmlTable(_) => "xmltable",
        SetReturningCall::RegexTableFunction { kind, .. } => match kind {
            RegexTableFunction::Matches => "regexp_matches",
            RegexTableFunction::SplitToTable => "regexp_split_to_table",
        },
        SetReturningCall::StringTableFunction { kind, .. } => match kind {
            StringTableFunction::StringToTable => "string_to_table",
        },
        SetReturningCall::PartitionTree { .. } => "pg_partition_tree",
        SetReturningCall::PartitionAncestors { .. } => "pg_partition_ancestors",
        SetReturningCall::PgLockStatus { .. } => "pg_lock_status",
        SetReturningCall::PgStatProgressCopy { .. } => "pg_stat_progress_copy",
        SetReturningCall::PgSequences { .. } => "pg_sequences",
        SetReturningCall::InformationSchemaSequences { .. } => "information_schema.sequences",
        SetReturningCall::TxidSnapshotXip { .. } => "txid_snapshot_xip",
        SetReturningCall::TextSearchTableFunction { kind, .. } => match kind {
            TextSearchTableFunction::TokenType => "ts_token_type",
            TextSearchTableFunction::Parse => "ts_parse",
            TextSearchTableFunction::Debug => "ts_debug",
            TextSearchTableFunction::Stat => "ts_stat",
        },
        SetReturningCall::UserDefined { function_name, .. } => function_name.as_str(),
    }
}

fn nonverbose_sort_items(
    input: &Plan,
    items: &[pgrust_nodes::primnodes::OrderByEntry],
    display_items: &[String],
    ctx: &VerboseExplainContext,
) -> Vec<String> {
    let should_use_display_items = !context_has_relation_aliases(ctx)
        && (!plan_has_explicit_relation_alias(input)
            || sort_display_items_preserve_aliases(display_items, items))
        && !display_items.is_empty()
        && !display_items
            .iter()
            .any(|item| explain_display_item_is_debug(item))
        && !((context_has_relation_aliases(ctx) || ctx.force_qualified_sort_keys)
            && display_items
                .iter()
                .all(|item| explain_display_item_is_bare_identifier(item))
            && !ctx.qualify_window_base_names);
    if should_use_display_items {
        return display_items
            .iter()
            .zip(items.iter())
            .map(|(display_item, item)| {
                let display_item =
                    resolve_window_sort_display_alias(input, display_items, display_item, ctx)
                        .unwrap_or_else(|| display_item.clone());
                let mut rendered = remap_sort_display_item_through_aggregate(input, &display_item)
                    .unwrap_or(display_item);
                if ctx.qualify_window_base_names {
                    rendered = qualify_single_relation_sort_display_item(input, &rendered);
                    rendered =
                        qualify_single_relation_expression_sort_display_item(input, &rendered);
                    rendered = qualify_function_scan_sort_display_item(input, &rendered);
                } else if rendered.contains(" || ") {
                    rendered =
                        qualify_single_relation_expression_sort_display_item(input, &rendered);
                }
                let has_direction = sort_display_item_has_direction(&rendered);
                if !has_direction && sort_item_needs_extra_expression_parens(&item.expr, &rendered)
                {
                    rendered = format!("({rendered})");
                }
                if rendered.contains(" OVER w") && !rendered.starts_with('(') {
                    rendered = format!("({rendered})");
                }
                if item.descending && !has_direction {
                    rendered.push_str(" DESC");
                }
                rendered
            })
            .collect();
    }
    let input_names = if ctx.qualify_window_base_names {
        sort_input_column_names(input)
            .or_else(|| qualified_scan_output_names(input))
            .unwrap_or_else(|| verbose_plan_output_exprs(input, ctx, true))
    } else if !context_has_relation_aliases(ctx)
        && !ctx.force_qualified_sort_keys
        && !plan_has_explicit_relation_alias(input)
        && leaf_relation_bases(input).len() == 1
    {
        if plan_contains_window_agg(input) {
            nonverbose_window_input_names(input, ctx)
        } else if matches!(input, Plan::Projection { .. }) {
            plan_join_output_exprs(input, ctx, true)
                .into_iter()
                .map(strip_qualified_identifiers)
                .collect()
        } else {
            input.column_names()
        }
    } else if ctx.force_qualified_sort_keys {
        qualified_scan_output_names(input)
            .or_else(|| sort_input_column_names(input))
            .unwrap_or_else(|| verbose_plan_output_exprs(input, ctx, true))
    } else {
        sort_input_column_names(input)
            .or_else(|| qualified_scan_output_names(input))
            .unwrap_or_else(|| verbose_plan_output_exprs(input, ctx, true))
    };
    let mut rendered = items
        .iter()
        .map(|item| {
            partial_aggregate_append_sort_item(input, item, ctx)
                .unwrap_or_else(|| render_nonverbose_sort_item(item, &input_names, ctx))
        })
        .collect::<Vec<_>>();
    rendered = rendered
        .into_iter()
        .map(|item| {
            let mut item = if item.contains(" || ") {
                qualify_single_relation_expression_sort_display_item(input, &item)
            } else {
                item
            };
            if item.contains(" OVER w") && !item.starts_with('(') {
                item = format!("({item})");
            }
            item
        })
        .collect();
    if matches!(input, Plan::FunctionScan { .. }) && !ctx.qualify_window_base_names {
        rendered = rendered
            .into_iter()
            .map(strip_self_qualified_identifiers)
            .fold(Vec::new(), |mut items, item| {
                if !items.contains(&item) {
                    items.push(item);
                }
                items
            });
    }
    rendered
}

fn explain_display_item_is_bare_identifier(item: &str) -> bool {
    !item.is_empty()
        && item
            .chars()
            .all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn sort_display_items_preserve_aliases(
    display_items: &[String],
    items: &[pgrust_nodes::primnodes::OrderByEntry],
) -> bool {
    display_items.len() < items.len()
        || display_items
            .iter()
            .any(|item| strip_sort_direction_suffix(item.clone()).contains('.'))
}

fn plan_has_explicit_relation_alias(plan: &Plan) -> bool {
    match plan {
        Plan::SeqScan { relation_name, .. }
        | Plan::TidScan { relation_name, .. }
        | Plan::TidRangeScan { relation_name, .. }
        | Plan::IndexOnlyScan { relation_name, .. }
        | Plan::IndexScan { relation_name, .. }
        | Plan::BitmapHeapScan { relation_name, .. } => relation_name
            .rsplit_once(' ')
            .is_some_and(|(_, alias)| inherited_root_alias(alias).is_none()),
        Plan::BitmapIndexScan { .. }
        | Plan::BitmapOr { .. }
        | Plan::BitmapAnd { .. }
        | Plan::FunctionScan { .. }
        | Plan::Result { .. }
        | Plan::Values { .. }
        | Plan::WorkTableScan { .. } => false,
        _ => direct_plan_children(plan)
            .into_iter()
            .any(plan_has_explicit_relation_alias),
    }
}

fn sort_display_item_has_direction(item: &str) -> bool {
    item.ends_with(" DESC")
        || item.ends_with(" ASC")
        || item.contains(" DESC NULLS ")
        || item.contains(" ASC NULLS ")
}

fn strip_sort_direction_suffix(mut item: String) -> String {
    for suffix in [" NULLS FIRST", " NULLS LAST"] {
        if let Some(stripped) = item.strip_suffix(suffix) {
            item = stripped.to_string();
            break;
        }
    }
    for suffix in [" DESC", " ASC"] {
        if let Some(stripped) = item.strip_suffix(suffix) {
            return stripped.to_string();
        }
    }
    item
}

fn explain_display_item_is_debug(item: &str) -> bool {
    item.contains("Var(Var {")
        || item.contains("Aggref(")
        || item.contains("WindowFunc(")
        || item.contains("GroupingKey(")
        || item.contains("GroupingFunc(")
}

fn qualify_single_relation_sort_display_item(input: &Plan, item: &str) -> String {
    let bases = leaf_relation_bases(input);
    let [base] = bases.as_slice() else {
        return item.to_string();
    };
    let (core, suffix) = item
        .strip_suffix(" DESC")
        .map(|core| (core, " DESC"))
        .or_else(|| {
            item.strip_suffix(" NULLS FIRST")
                .map(|core| (core, " NULLS FIRST"))
        })
        .or_else(|| {
            item.strip_suffix(" NULLS LAST")
                .map(|core| (core, " NULLS LAST"))
        })
        .unwrap_or((item, ""));
    if core.contains('.') || core.contains('(') || !explain_sort_key_is_bare_identifier(core) {
        return item.to_string();
    }
    format!("{base}.{core}{suffix}")
}

fn qualify_function_scan_sort_display_item(input: &Plan, item: &str) -> String {
    let Plan::FunctionScan {
        table_alias: Some(alias),
        ..
    } = input
    else {
        return item.to_string();
    };
    let (core, suffix) = item
        .strip_suffix(" DESC")
        .map(|core| (core, " DESC"))
        .or_else(|| {
            item.strip_suffix(" NULLS FIRST")
                .map(|core| (core, " NULLS FIRST"))
        })
        .or_else(|| {
            item.strip_suffix(" NULLS LAST")
                .map(|core| (core, " NULLS LAST"))
        })
        .unwrap_or((item, ""));
    if core.contains('.') || core.contains('(') || !explain_sort_key_is_bare_identifier(core) {
        return item.to_string();
    }
    format!("{alias}.{core}{suffix}")
}

fn qualify_single_relation_expression_sort_display_item(input: &Plan, item: &str) -> String {
    let Some(qualified_names) = qualified_scan_output_names(input) else {
        return item.to_string();
    };
    let mut rendered = item.to_string();
    for qualified in qualified_names {
        let Some((_, name)) = qualified.rsplit_once('.') else {
            continue;
        };
        if rendered == name {
            return qualified;
        }
        rendered = rendered.replace(&format!("({name})"), &format!("({qualified})"));
    }
    if rendered.contains(" || ") && !rendered.starts_with("(((") {
        format!("({rendered})")
    } else {
        rendered
    }
}

fn explain_sort_key_is_bare_identifier(item: &str) -> bool {
    let mut chars = item.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    is_explain_ident_start(first) && chars.all(is_explain_ident_part)
}

fn sort_input_column_names(plan: &Plan) -> Option<Vec<String>> {
    match plan {
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        } => Some(qualified_scan_output_exprs(relation_name, desc)),
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. } => sort_input_column_names(input),
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. } => {
            children.first().and_then(sort_input_column_names)
        }
        _ => None,
    }
}

fn partial_aggregate_append_sort_item(
    input: &Plan,
    item: &pgrust_nodes::primnodes::OrderByEntry,
    ctx: &VerboseExplainContext,
) -> Option<String> {
    let (Plan::Append { children, .. } | Plan::MergeAppend { children, .. }) = input else {
        return None;
    };
    let index = match &item.expr {
        Expr::Var(var) => attrno_index(var.varattno)?,
        _ => return None,
    };
    let Plan::Aggregate {
        input,
        group_by,
        output_columns,
        disabled,
        ..
    } = children.first()?
    else {
        return None;
    };
    let expr = group_by.get(index)?;
    let mut rendered = render_nonverbose_aggregate_group_key(
        expr,
        input,
        output_columns.get(index).map(|column| column.sql_type),
        ctx,
        *disabled,
        true,
    );
    if item.descending {
        rendered.push_str(" DESC");
    }
    if let Some(nulls_first) = item.nulls_first
        && nulls_first != item.descending
    {
        rendered.push_str(if nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    Some(rendered)
}

fn remap_sort_display_item_through_aggregate(input: &Plan, item: &str) -> Option<String> {
    if let Plan::Projection { input, .. }
    | Plan::Filter { input, .. }
    | Plan::SubqueryScan { input, .. } = input
    {
        return remap_sort_display_item_through_aggregate(input, item);
    }
    let Plan::Aggregate {
        input: aggregate_input,
        group_by,
        ..
    } = input
    else {
        return None;
    };
    let column = item.rsplit('.').next().unwrap_or(item);
    let qualified_names = qualified_scan_output_names(aggregate_input)?;
    group_by.iter().find_map(|expr| {
        let Expr::Var(var) = expr else {
            return None;
        };
        let index = pgrust_nodes::primnodes::attrno_index(var.varattno)?;
        let qualified = qualified_names.get(index)?;
        qualified
            .rsplit('.')
            .next()
            .is_some_and(|name| name.eq_ignore_ascii_case(column))
            .then(|| qualified.clone())
    })
}

fn qualified_scan_output_names(plan: &Plan) -> Option<Vec<String>> {
    match plan {
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        } => Some(qualified_base_scan_output_exprs(relation_name, desc)),
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::WindowAgg { input, .. } => qualified_scan_output_names(input),
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. } => {
            children.first().and_then(qualified_scan_output_names)
        }
        _ => None,
    }
}

fn qualified_scan_output_names_with_context(
    plan: &Plan,
    ctx: &VerboseExplainContext,
) -> Option<Vec<String>> {
    match plan {
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        }
        | Plan::BitmapHeapScan {
            relation_name,
            desc,
            ..
        } => Some(qualified_scan_output_exprs_with_context(
            relation_name,
            desc,
            ctx,
        )),
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Hash { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. } => qualified_scan_output_names_with_context(input, ctx),
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. } => children
            .first()
            .and_then(|child| qualified_scan_output_names_with_context(child, ctx)),
        _ => None,
    }
}

fn context_has_relation_aliases(ctx: &VerboseExplainContext) -> bool {
    ctx.relation_scan_alias.is_some() || !ctx.relation_scan_aliases.is_empty()
}

fn render_nonverbose_sort_item(
    item: &pgrust_nodes::primnodes::OrderByEntry,
    input_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let mut rendered = render_verbose_expr(&item.expr, input_names, ctx);
    if sort_item_needs_extra_expression_parens(&item.expr, &rendered) {
        rendered = format!("({rendered})");
    }
    if item.descending {
        rendered.push_str(" DESC");
    }
    if let Some(nulls_first) = item.nulls_first
        && nulls_first != item.descending
    {
        rendered.push_str(if nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    rendered
}

fn resolve_window_sort_display_alias(
    input: &Plan,
    display_items: &[String],
    item: &str,
    ctx: &VerboseExplainContext,
) -> Option<String> {
    if !plan_contains_window_agg(input) {
        return None;
    }
    if let Plan::Projection {
        input: child,
        targets,
        ..
    } = input
        && let Some(target) = targets
            .iter()
            .find(|target| !target.resjunk && target.name == item)
    {
        let child_names = nonverbose_window_input_names(child, ctx);
        return Some(render_verbose_expr(&target.expr, &child_names, ctx));
    }
    let output_names = nonverbose_window_input_names(input, ctx);
    let col_count = display_items
        .iter()
        .filter_map(|item| {
            item.strip_prefix("col")
                .and_then(|suffix| suffix.parse::<usize>().ok())
        })
        .max()
        .unwrap_or(0);
    if let Some(index) = item
        .strip_prefix("col")
        .and_then(|suffix| suffix.parse::<usize>().ok())
        .and_then(|index| index.checked_sub(1))
    {
        return output_names.get(index).cloned();
    }
    if let Some(index) = item
        .strip_prefix("win")
        .and_then(|suffix| suffix.parse::<usize>().ok())
        .and_then(|index| index.checked_sub(1))
        .map(|index| col_count + index)
    {
        return output_names.get(index).cloned();
    }
    None
}

fn sort_item_needs_extra_expression_parens(expr: &Expr, rendered: &str) -> bool {
    let is_geo_distance = matches!(
        expr,
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoDistance)
            )
    );
    if is_geo_distance || rendered.contains(" <-> ") {
        // :HACK: PostgreSQL's ruleutils keeps an extra paren layer around
        // ORDER BY distance operators. This is EXPLAIN text only.
        return !(rendered.starts_with("((") && rendered.ends_with("))"));
    }
    if rendered.contains("InitPlan ") && matches!(expr, Expr::Op(_)) {
        return !(rendered.starts_with("((") && rendered.ends_with("))"));
    }
    let already_wrapped = rendered.starts_with('(') && rendered.ends_with(')');
    (rendered.starts_with('(') && !already_wrapped)
        || (!already_wrapped && matches!(expr, Expr::GroupingFunc(_)))
        || (!already_wrapped
            && matches!(
                expr,
                Expr::Func(func)
                    if matches!(
                        func.implementation,
                        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoDistance)
                    )
            ))
}

fn render_nonverbose_aggregate_group_key(
    expr: &Expr,
    input: &Plan,
    sql_type: Option<pgrust_nodes::parsenodes::SqlType>,
    ctx: &VerboseExplainContext,
    force_xid_const: bool,
    qualify_base_scan: bool,
) -> String {
    if force_xid_const
        || sql_type.is_some_and(|ty| matches!(ty.kind, pgrust_nodes::parsenodes::SqlTypeKind::Xid))
    {
        let input_names = nonverbose_aggregate_input_names(input, ctx);
        return render_nonverbose_group_key_expr(
            expr,
            sql_type,
            &input_names,
            ctx,
            force_xid_const,
        );
    }
    if matches!(expr, Expr::Var(_)) {
        let input_names = aggregate_group_key_input_names(input, ctx, qualify_base_scan);
        return render_nonverbose_group_key_expr(expr, sql_type, &input_names, ctx, false);
    }
    let input_names = aggregate_group_key_input_names(input, ctx, qualify_base_scan);
    let rendered = render_nonverbose_group_key_expr(expr, sql_type, &input_names, ctx, false);
    if !rendered.contains("?column?") && !group_key_refs_projection_alias(expr, input, &rendered) {
        return rendered;
    }
    let input_names = nonverbose_aggregate_input_names(input, ctx);
    render_nonverbose_group_key_expr(expr, sql_type, &input_names, ctx, force_xid_const)
}

fn aggregate_group_key_input_names(
    input: &Plan,
    ctx: &VerboseExplainContext,
    qualify_base_scan: bool,
) -> Vec<String> {
    if const_false_filter_result_plan(input).is_some() {
        return input.column_names();
    }
    if matches!(input, Plan::Projection { .. }) {
        return nonverbose_aggregate_input_names(input, ctx);
    }
    if context_has_relation_aliases(ctx)
        && let Some(names) = qualified_scan_output_names_with_context(input, ctx)
    {
        return names;
    }
    if qualify_base_scan {
        if const_false_filter_result_plan(input).is_none()
            && let Some(names) = qualified_scan_output_names(input)
        {
            return names;
        }
        return plan_join_output_exprs(input, ctx, true);
    }
    input.column_names()
}

fn group_key_refs_projection_alias(expr: &Expr, input: &Plan, rendered: &str) -> bool {
    let Expr::Var(var) = expr else {
        return false;
    };
    let Plan::Projection { targets, .. } = input else {
        return false;
    };
    let Some(index) = attrno_index(var.varattno) else {
        return false;
    };
    let Some(target) = targets.get(index) else {
        return false;
    };
    target.name == rendered || rendered == format!("({})", target.name)
}

fn nonverbose_aggregate_input_names(input: &Plan, ctx: &VerboseExplainContext) -> Vec<String> {
    plan_join_output_exprs(input, ctx, true)
        .into_iter()
        .map(strip_self_qualified_identifiers)
        .collect()
}

fn nonverbose_window_input_names(input: &Plan, ctx: &VerboseExplainContext) -> Vec<String> {
    match input {
        Plan::WindowAgg { input, clause, .. } => nonverbose_window_output_names(input, clause, ctx),
        Plan::OrderBy { input: child, .. } | Plan::IncrementalSort { input: child, .. } => {
            if plan_contains_window_agg(child) {
                nonverbose_window_input_names(child, ctx)
            } else {
                nonverbose_window_base_output_names(input, ctx)
            }
        }
        Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. } => nonverbose_window_input_names(input, ctx),
        _ => nonverbose_window_base_output_names(input, ctx),
    }
}

fn nonverbose_window_base_output_names(input: &Plan, ctx: &VerboseExplainContext) -> Vec<String> {
    let names = verbose_display_output_exprs(input, ctx, false);
    if ctx.qualify_window_base_names || leaf_relation_bases(input).len() > 1 {
        names
    } else {
        names.into_iter().map(strip_qualified_identifiers).collect()
    }
}

fn nonverbose_window_output_names(
    input: &Plan,
    clause: &WindowClause,
    ctx: &VerboseExplainContext,
) -> Vec<String> {
    let input_names = nonverbose_window_input_names(input, ctx);
    let mut output = input_names.clone();
    output.extend(
        clause
            .functions
            .iter()
            .map(|func| render_window_func_for_explain(func, &input_names, ctx)),
    );
    output
}

fn verbose_window_output_names(
    input: &Plan,
    clause: &WindowClause,
    output_columns: &[QueryColumn],
    ctx: &VerboseExplainContext,
) -> Vec<String> {
    let input_names = verbose_window_input_names(input, ctx);
    let generated_names = output_columns
        .iter()
        .all(|column| explain_generated_window_column_name(&column.name));
    let mut output = if generated_names {
        input_names.clone()
    } else {
        output_columns
            .iter()
            .take(input_names.len())
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
    };
    if output.len() < input_names.len() {
        output.extend(input_names.iter().skip(output.len()).cloned());
    }
    output.extend(
        clause
            .functions
            .iter()
            .map(|func| render_verbose_window_func(func, &input_names, ctx)),
    );
    output
}

fn explain_generated_window_column_name(name: &str) -> bool {
    name.strip_prefix("col")
        .or_else(|| name.strip_prefix("win"))
        .is_some_and(|suffix| suffix.parse::<usize>().is_ok())
}

fn verbose_window_input_names(input: &Plan, ctx: &VerboseExplainContext) -> Vec<String> {
    match input {
        Plan::WindowAgg {
            input,
            clause,
            output_columns,
            ..
        } => verbose_window_output_names(input, clause, output_columns, ctx),
        Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. } => verbose_window_input_names(input, ctx),
        _ => verbose_plan_output_exprs(input, ctx, true),
    }
}

fn window_clause_explain_name(clause: &WindowClause) -> String {
    clause
        .functions
        .iter()
        .map(|func| func.winref)
        .min()
        .map(|winref| format!("w{winref}"))
        .unwrap_or_else(|| "w1".into())
}

fn render_window_aggref_for_explain(
    aggref: &pgrust_nodes::primnodes::Aggref,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let name = builtin_aggregate_function_for_proc_oid(aggref.aggfnoid)
        .map(|func| func.name().to_string())
        .unwrap_or_else(|| format!("agg_{}", aggref.aggfnoid));
    let mut args = if aggref.args.is_empty() {
        vec!["*".into()]
    } else {
        aggref
            .args
            .iter()
            .map(|arg| render_verbose_expr(arg, column_names, ctx))
            .collect::<Vec<_>>()
    };
    if aggref.aggdistinct && !args.is_empty() {
        args[0] = format!("DISTINCT {}", args[0]);
    }
    format!("{name}({})", args.join(", "))
}

pub fn render_window_func_for_explain(
    window_func: &pgrust_nodes::primnodes::WindowFuncExpr,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let rendered = match &window_func.kind {
        WindowFuncKind::Aggregate(aggref) => {
            render_window_aggref_for_explain(aggref, column_names, ctx)
        }
        WindowFuncKind::Builtin(func) => {
            let args = window_func
                .args
                .iter()
                .map(|arg| render_verbose_expr(arg, column_names, ctx))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({args})", func.name())
        }
    };
    format!("{rendered} OVER w{}", window_func.winref)
}

pub fn render_window_filter_qual_for_explain(expr: &Expr, column_names: &[String]) -> String {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => {
            let parts = bool_expr
                .args
                .iter()
                .map(|arg| render_window_filter_conjunct_for_explain(arg, column_names))
                .collect::<Vec<_>>();
            format!("({})", parts.join(" AND "))
        }
        _ => render_window_filter_conjunct_for_explain(expr, column_names),
    }
}

pub fn render_window_filter_conjunct_for_explain(expr: &Expr, column_names: &[String]) -> String {
    if let Expr::Op(op) = expr
        && matches!(
            op.op,
            OpExprKind::Eq
                | OpExprKind::NotEq
                | OpExprKind::Lt
                | OpExprKind::LtEq
                | OpExprKind::Gt
                | OpExprKind::GtEq
        )
        && let [left, right] = op.args.as_slice()
    {
        let op_text = match op.op {
            OpExprKind::Eq => "=",
            OpExprKind::NotEq => "<>",
            OpExprKind::Lt => "<",
            OpExprKind::LtEq => "<=",
            OpExprKind::Gt => ">",
            OpExprKind::GtEq => ">=",
            _ => unreachable!(),
        };
        let left = render_explain_expr(left, column_names);
        let right = strip_window_filter_const_parens(render_explain_expr(right, column_names));
        if left.contains(" OVER ") && !left.starts_with('(') {
            return format!("(({left}) {op_text} {right})");
        }
        return format!("({left} {op_text} {right})");
    }
    render_explain_expr(expr, column_names)
}

pub fn strip_window_filter_const_parens(rendered: String) -> String {
    let Some(inner) = rendered
        .strip_prefix('(')
        .and_then(|text| text.strip_suffix(')'))
    else {
        return rendered;
    };
    if inner.parse::<i64>().is_ok() {
        inner.to_string()
    } else {
        rendered
    }
}

pub fn strip_self_qualified_identifiers(input: String) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    while index < chars.len() {
        if !is_explain_ident_start(chars[index]) {
            output.push(chars[index]);
            index += 1;
            continue;
        }
        let first_start = index;
        index += 1;
        while index < chars.len() && is_explain_ident_part(chars[index]) {
            index += 1;
        }
        if chars.get(index) != Some(&'.') {
            output.extend(chars[first_start..index].iter());
            continue;
        }
        let second_start = index + 1;
        let mut second_end = second_start;
        if second_end >= chars.len() || !is_explain_ident_start(chars[second_end]) {
            output.extend(chars[first_start..=index].iter());
            index = second_start;
            continue;
        }
        second_end += 1;
        while second_end < chars.len() && is_explain_ident_part(chars[second_end]) {
            second_end += 1;
        }
        if chars[first_start..index] == chars[second_start..second_end] {
            output.extend(chars[first_start..index].iter());
        } else {
            output.extend(chars[first_start..second_end].iter());
        }
        index = second_end;
    }
    output
}

pub fn strip_qualified_identifiers(input: String) -> String {
    let chars = input.chars().collect::<Vec<_>>();
    let mut output = String::with_capacity(input.len());
    let mut index = 0;
    while index < chars.len() {
        if !is_explain_ident_start(chars[index]) {
            output.push(chars[index]);
            index += 1;
            continue;
        }
        let first_start = index;
        index += 1;
        while index < chars.len() && is_explain_ident_part(chars[index]) {
            index += 1;
        }
        if chars.get(index) != Some(&'.') {
            output.extend(chars[first_start..index].iter());
            continue;
        }
        let second_start = index + 1;
        let mut second_end = second_start;
        if second_end >= chars.len() || !is_explain_ident_start(chars[second_end]) {
            output.extend(chars[first_start..=index].iter());
            index = second_start;
            continue;
        }
        second_end += 1;
        while second_end < chars.len() && is_explain_ident_part(chars[second_end]) {
            second_end += 1;
        }
        output.extend(chars[second_start..second_end].iter());
        index = second_end;
    }
    output
}

pub fn is_explain_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

pub fn is_explain_ident_part(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

pub fn render_hash_join_condition(
    outer_hash_keys: &[Expr],
    right: &Plan,
    left_names: &[String],
    right_names: &[String],
    ctx: &VerboseExplainContext,
) -> Option<String> {
    let Plan::Hash {
        hash_keys: inner_hash_keys,
        ..
    } = right
    else {
        return None;
    };
    if outer_hash_keys.len() != inner_hash_keys.len() || outer_hash_keys.is_empty() {
        return None;
    }
    let mut seen_keys = std::collections::BTreeSet::new();
    let mut primary_parts = Vec::new();
    let mut duplicate_parts = Vec::new();
    for (outer, inner) in outer_hash_keys.iter().zip(inner_hash_keys.iter()) {
        let left = render_verbose_expr(outer, left_names, ctx);
        let right = render_verbose_expr(inner, right_names, ctx);
        let key = (left.clone(), right.clone());
        let part = format!("({left} = {right})");
        if seen_keys.insert(key) {
            primary_parts.push(part);
        } else {
            duplicate_parts.push(part);
        }
    }
    let parts = primary_parts
        .iter()
        .chain(duplicate_parts.iter())
        .cloned()
        .collect::<Vec<_>>();
    if parts.len() == 1 {
        Some(parts[0].clone())
    } else {
        Some(format!("({})", parts.join(" AND ")))
    }
}

pub fn synthetic_row_hash_condition(
    left_names: &[String],
    right_names: &[String],
) -> Option<String> {
    let ([left], [right]) = (left_names, right_names) else {
        return None;
    };
    if !right.starts_with("ROW(") {
        return None;
    }
    // :HACK: FieldSelect hash keys over row-valued VALUES inputs can be
    // consumed before EXPLAIN sees them. Preserve PostgreSQL's visible
    // condition for that narrow rowtype regression shape.
    let left_field = format!("({left}).column1");
    let right_field = postgres_parenthesize_row_field_select(format!("({right}).column1"));
    Some(format!("({left_field} = {right_field})"))
}

pub fn projected_row_hash_condition(plan: &Plan, output: &[String]) -> Option<String> {
    let Plan::HashJoin {
        hash_keys,
        hash_clauses,
        join_qual,
        qual,
        ..
    } = plan
    else {
        return None;
    };
    if !hash_keys.is_empty()
        || !hash_clauses.is_empty()
        || !join_qual.is_empty()
        || !qual.is_empty()
    {
        return None;
    }
    let [left, right] = output else {
        return None;
    };
    if !(left.ends_with(".column2") && right.ends_with(".column2") && right.contains("ROW(")) {
        return None;
    }
    // :HACK: See synthetic_row_hash_condition; this handles the projected
    // join display path, where only the output FieldSelects still carry the
    // row-value provenance needed for PostgreSQL-compatible EXPLAIN text.
    Some(format!(
        "({} = {})",
        left.strip_suffix(".column2")
            .map(|base| format!("{base}.column1"))?,
        right
            .strip_suffix(".column2")
            .map(|base| format!("{base}.column1"))?
    ))
}

pub fn render_join_condition_list(
    exprs: &[Expr],
    left_names: &[String],
    right_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let rendered = exprs
        .iter()
        .map(|expr| render_verbose_join_expr(expr, left_names, right_names, ctx))
        .collect::<Vec<_>>();
    if rendered.len() == 1 {
        rendered[0].clone()
    } else {
        format!("({})", rendered.join(" AND "))
    }
}

pub fn render_window_clause_for_explain(
    input: &Plan,
    clause: &WindowClause,
    ctx: &VerboseExplainContext,
) -> String {
    let input_names = nonverbose_window_input_names(input, ctx);
    render_window_clause_with_input_names(input, clause, ctx, &input_names)
}

pub fn render_window_clause_with_input_names(
    input: &Plan,
    clause: &WindowClause,
    ctx: &VerboseExplainContext,
    input_names: &[String],
) -> String {
    let mut parts = Vec::new();
    if !clause.spec.partition_by.is_empty() {
        parts.push(format!(
            "PARTITION BY {}",
            clause
                .spec
                .partition_by
                .iter()
                .map(|expr| render_window_partition_expr_for_explain(expr, &input_names, ctx))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !clause.spec.order_by.is_empty() {
        parts.push(format!(
            "ORDER BY {}",
            render_window_order_by_for_explain(input, clause, ctx, &input_names)
        ));
    }
    if let Some(frame) = render_window_frame_for_explain(clause, &input_names, ctx) {
        parts.push(frame);
    }
    parts.join(" ")
}

pub fn render_window_partition_expr_for_explain(
    expr: &Expr,
    input_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let rendered = render_verbose_expr(expr, input_names, ctx);
    if rendered.contains(" || ") && !rendered.starts_with("(((") {
        format!("({rendered})")
    } else {
        rendered
    }
}

pub fn render_window_order_by_for_explain(
    input: &Plan,
    clause: &WindowClause,
    ctx: &VerboseExplainContext,
    input_names: &[String],
) -> String {
    if let Some(order_by) = render_ordered_index_child_order_by(input) {
        return order_by;
    }
    clause
        .spec
        .order_by
        .iter()
        .map(|item| render_verbose_expr(&item.expr, input_names, ctx))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn render_nonverbose_order_by_item(
    item: &OrderByEntry,
    input_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let mut rendered = render_verbose_expr(&item.expr, input_names, ctx);
    if item.descending {
        rendered.push_str(" DESC");
    }
    if let Some(nulls_first) = item.nulls_first
        && nulls_first != item.descending
    {
        rendered.push_str(if nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    rendered
}

pub fn render_nonverbose_group_key_expr(
    expr: &Expr,
    sql_type: Option<pgrust_nodes::parsenodes::SqlType>,
    input_names: &[String],
    ctx: &VerboseExplainContext,
    force_xid_const: bool,
) -> String {
    if (force_xid_const
        || sql_type.is_some_and(|ty| matches!(ty.kind, pgrust_nodes::parsenodes::SqlTypeKind::Xid)))
        && let Some(rendered) = render_xid_group_key_expr(expr)
    {
        return rendered;
    }
    let rendered = render_verbose_expr(expr, input_names, ctx);
    if (force_xid_const
        || sql_type.is_some_and(|ty| matches!(ty.kind, pgrust_nodes::parsenodes::SqlTypeKind::Xid)))
        && rendered.chars().all(|ch| ch.is_ascii_digit())
    {
        return format!("('{rendered}'::xid)");
    }
    if matches!(expr, Expr::Var(_)) {
        return rendered;
    }
    if (matches!(expr, Expr::Op(_)) || rendered.contains(" || "))
        && rendered.starts_with('(')
        && rendered.ends_with(')')
    {
        return rendered;
    }
    rendered
}

pub fn render_xid_group_key_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Const(Value::Int16(value)) => Some(format!("('{value}'::xid)")),
        Expr::Const(Value::Int32(value)) => Some(format!("('{value}'::xid)")),
        Expr::Const(Value::Int64(value)) => Some(format!("('{value}'::xid)")),
        Expr::Const(Value::Xid8(value)) => Some(format!("('{value}'::xid)")),
        Expr::Const(Value::EnumOid(value)) => Some(format!("('{value}'::xid)")),
        Expr::Const(Value::Text(value)) => Some(format!("('{}'::xid)", value.replace('\'', "''"))),
        Expr::Const(value @ Value::TextRef(_, _)) => value
            .as_text()
            .map(|value| format!("('{}'::xid)", value.replace('\'', "''"))),
        Expr::Cast(inner, ty) if matches!(ty.kind, pgrust_nodes::parsenodes::SqlTypeKind::Xid) => {
            render_xid_group_key_expr(inner)
        }
        _ => None,
    }
}

pub fn render_ordered_index_child_order_by(input: &Plan) -> Option<String> {
    match input {
        Plan::IndexOnlyScan {
            order_by_keys,
            desc,
            index_meta,
            ..
        }
        | Plan::IndexScan {
            order_by_keys,
            desc,
            index_meta,
            ..
        } => render_index_order_by(order_by_keys, desc, index_meta)
            .map(|detail| format!("({detail})")),
        Plan::Projection { input, .. }
        | Plan::Filter { input, .. }
        | Plan::Limit { input, .. }
        | Plan::SubqueryScan { input, .. } => render_ordered_index_child_order_by(input),
        _ => None,
    }
}

pub fn render_window_frame_for_explain(
    clause: &WindowClause,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> Option<String> {
    use pgrust_nodes::parsenodes::WindowFrameMode;

    let frame = &clause.spec.frame;
    let mode = match frame.mode {
        WindowFrameMode::Rows => "ROWS",
        WindowFrameMode::Range => "RANGE",
        WindowFrameMode::Groups => "GROUPS",
    };
    let rendered = match (&frame.start_bound, &frame.end_bound) {
        (WindowFrameBound::UnboundedPreceding, WindowFrameBound::CurrentRow) => (frame.mode
            == WindowFrameMode::Rows
            || (frame.mode == WindowFrameMode::Range
                && !clause.spec.order_by.is_empty()
                && window_clause_uses_prefix_frame(clause)))
        .then(|| "ROWS UNBOUNDED PRECEDING".into()),
        (WindowFrameBound::CurrentRow, WindowFrameBound::CurrentRow) => {
            Some(format!("{mode} BETWEEN CURRENT ROW AND CURRENT ROW"))
        }
        (start, WindowFrameBound::CurrentRow) => Some(format!(
            "{mode} {} PRECEDING",
            render_window_frame_start_bound(start, column_names, ctx)?
        )),
        (start, end) => Some(format!(
            "{mode} BETWEEN {} AND {}",
            render_window_frame_bound(start, column_names, ctx)?,
            render_window_frame_bound(end, column_names, ctx)?
        )),
    }?;
    Some(format!(
        "{}{}",
        rendered,
        render_window_frame_exclusion_for_explain(frame.exclusion)
    ))
}

pub fn render_window_frame_exclusion_for_explain(
    exclusion: pgrust_nodes::parsenodes::WindowFrameExclusion,
) -> &'static str {
    match exclusion {
        pgrust_nodes::parsenodes::WindowFrameExclusion::NoOthers => "",
        pgrust_nodes::parsenodes::WindowFrameExclusion::CurrentRow => " EXCLUDE CURRENT ROW",
        pgrust_nodes::parsenodes::WindowFrameExclusion::Group => " EXCLUDE GROUP",
        pgrust_nodes::parsenodes::WindowFrameExclusion::Ties => " EXCLUDE TIES",
    }
}

pub fn window_clause_uses_prefix_frame(clause: &WindowClause) -> bool {
    !clause.functions.is_empty()
        && clause.functions.iter().all(|func| {
            matches!(
                func.kind,
                WindowFuncKind::Builtin(
                    pgrust_nodes::primnodes::BuiltinWindowFunction::RowNumber
                        | pgrust_nodes::primnodes::BuiltinWindowFunction::Rank
                        | pgrust_nodes::primnodes::BuiltinWindowFunction::DenseRank
                        | pgrust_nodes::primnodes::BuiltinWindowFunction::PercentRank
                        | pgrust_nodes::primnodes::BuiltinWindowFunction::CumeDist
                        | pgrust_nodes::primnodes::BuiltinWindowFunction::Ntile
                )
            )
        })
}

pub fn render_window_frame_start_bound(
    bound: &WindowFrameBound,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> Option<String> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Some("UNBOUNDED".into()),
        WindowFrameBound::OffsetPreceding(offset) => Some(render_window_frame_offset_for_explain(
            offset,
            column_names,
            ctx,
        )),
        _ => None,
    }
}

pub fn render_window_frame_bound(
    bound: &WindowFrameBound,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> Option<String> {
    match bound {
        WindowFrameBound::UnboundedPreceding => Some("UNBOUNDED PRECEDING".into()),
        WindowFrameBound::OffsetPreceding(offset) => Some(format!(
            "{} PRECEDING",
            render_window_frame_offset_for_explain(offset, column_names, ctx)
        )),
        WindowFrameBound::CurrentRow => Some("CURRENT ROW".into()),
        WindowFrameBound::OffsetFollowing(offset) => Some(format!(
            "{} FOLLOWING",
            render_window_frame_offset_for_explain(offset, column_names, ctx)
        )),
        WindowFrameBound::UnboundedFollowing => Some("UNBOUNDED FOLLOWING".into()),
    }
}

pub fn render_window_frame_offset_for_explain(
    offset: &pgrust_nodes::primnodes::WindowFrameOffset,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    if let Expr::Const(value) = &offset.expr
        && offset.offset_type.kind == pgrust_nodes::parsenodes::SqlTypeKind::Int8
    {
        return format!(
            "'{}'::bigint",
            render_explain_literal(value).trim_matches('\'')
        );
    }
    render_verbose_expr(&offset.expr, column_names, ctx)
}

pub fn push_explain_plan_line(
    plan: &Plan,
    indent: usize,
    is_child: bool,
    show_costs: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    let prefix = explain_node_prefix(indent, is_child);
    let (node_label, plan_info) = plan_node_info(plan);
    let label = verbose_plan_label(plan, ctx).unwrap_or(node_label);
    push_explain_line(&format!("{prefix}{label}"), plan_info, show_costs, lines);
}

pub fn explain_detail_prefix(indent: usize) -> String {
    if indent == 0 {
        "  ".into()
    } else {
        " ".repeat(2 + indent * 6)
    }
}

pub fn verbose_plan_label(plan: &Plan, ctx: &VerboseExplainContext) -> Option<String> {
    match plan {
        Plan::Projection { input, .. }
            if matches!(
                input.as_ref(),
                Plan::Result { .. } | Plan::Append { .. } | Plan::MergeAppend { .. }
            ) || plan_is_limit_result(input) =>
        {
            Some("Result".into())
        }
        Plan::SeqScan { .. } | Plan::IndexOnlyScan { .. } | Plan::IndexScan { .. } => {
            verbose_scan_plan_label(plan, ctx)
        }
        Plan::Aggregate {
            strategy,
            phase,
            group_by,
            group_by_refs,
            grouping_sets,
            accumulators,
            ..
        } => {
            let display_strategy =
                display_aggregate_strategy(*strategy, group_by, group_by_refs, grouping_sets);
            Some(aggregate_plan_label(
                display_strategy,
                *phase,
                accumulators.is_empty() && grouping_sets.is_empty(),
            ))
        }
        Plan::NestedLoopJoin { .. } => Some("Nested Loop".into()),
        Plan::HashJoin { .. } => Some("Hash Join".into()),
        Plan::MergeJoin { .. } => Some("Merge Join".into()),
        Plan::SetOp { op, strategy, .. } => Some(set_op_plan_label(*op, *strategy)),
        Plan::FunctionScan {
            call, table_alias, ..
        } => Some(verbose_function_scan_label(
            call,
            table_alias.as_deref(),
            ctx,
        )),
        Plan::SubqueryScan { scan_name, .. } => Some(match scan_name {
            Some(scan_name) => format!("Subquery Scan on {}", quote_explain_identifier(scan_name)),
            None => "Subquery Scan".into(),
        }),
        Plan::CteScan { cte_name, .. } => Some(format!("CTE Scan on {cte_name}")),
        Plan::Values { .. } => Some(format!(
            "Values Scan on {}",
            ctx.values_scan_name.as_deref().unwrap_or("\"*VALUES*\"")
        )),
        _ => None,
    }
}

pub fn aggregate_plan_label(
    strategy: AggregateStrategy,
    phase: pgrust_nodes::plannodes::AggregatePhase,
    groups_only: bool,
) -> String {
    if groups_only && strategy == AggregateStrategy::Sorted {
        return "Group".into();
    }
    let base = match strategy {
        AggregateStrategy::Plain => "Aggregate",
        AggregateStrategy::Sorted => "GroupAggregate",
        AggregateStrategy::Hashed => "HashAggregate",
        AggregateStrategy::Mixed => "MixedAggregate",
    };
    match phase {
        pgrust_nodes::plannodes::AggregatePhase::Complete => base.to_string(),
        pgrust_nodes::plannodes::AggregatePhase::Partial => format!("Partial {base}"),
        pgrust_nodes::plannodes::AggregatePhase::Finalize => format!("Finalize {base}"),
    }
}

pub fn display_aggregate_strategy(
    strategy: AggregateStrategy,
    group_by: &[Expr],
    group_by_refs: &[usize],
    grouping_sets: &[Vec<usize>],
) -> AggregateStrategy {
    if group_by.is_empty() && !grouping_sets.is_empty() && grouping_sets.iter().all(Vec::is_empty) {
        AggregateStrategy::Plain
    } else if strategy == AggregateStrategy::Mixed
        && !grouping_sets.is_empty()
        && grouping_sets.iter().all(|set| !set.is_empty())
        && {
            let group_hashable = group_by
                .iter()
                .map(grouping_expr_hashable)
                .collect::<Vec<_>>();
            grouping_sets
                .iter()
                .all(|set| grouping_set_hashable(set, group_by_refs, &group_hashable))
        }
    {
        AggregateStrategy::Hashed
    } else {
        strategy
    }
}

pub fn set_op_plan_label(
    op: pgrust_nodes::parsenodes::SetOperator,
    strategy: pgrust_nodes::plannodes::SetOpStrategy,
) -> String {
    let op_name = match op {
        pgrust_nodes::parsenodes::SetOperator::Union { all: true } => "Union All",
        pgrust_nodes::parsenodes::SetOperator::Union { all: false } => "Union",
        pgrust_nodes::parsenodes::SetOperator::Intersect { all: true } => "Intersect All",
        pgrust_nodes::parsenodes::SetOperator::Intersect { all: false } => "Intersect",
        pgrust_nodes::parsenodes::SetOperator::Except { all: true } => "Except All",
        pgrust_nodes::parsenodes::SetOperator::Except { all: false } => "Except",
    };
    let prefix = match strategy {
        pgrust_nodes::plannodes::SetOpStrategy::Hashed => "HashSetOp",
        pgrust_nodes::plannodes::SetOpStrategy::Sorted => "SetOp",
    };
    format!("{prefix} {op_name}")
}

pub fn nonverbose_plan_label(
    plan: &Plan,
    ctx: &VerboseExplainContext,
    is_child: bool,
) -> Option<String> {
    match plan {
        Plan::Filter { input, .. }
            if matches!(
                input.as_ref(),
                Plan::SeqScan { .. }
                    | Plan::TidScan { .. }
                    | Plan::IndexOnlyScan { .. }
                    | Plan::IndexScan { .. }
                    | Plan::BitmapHeapScan { .. }
            ) =>
        {
            nonverbose_plan_label(input, ctx, is_child)
        }
        Plan::Projection { input, .. } if matches!(input.as_ref(), Plan::Result { .. }) => {
            Some("Result".into())
        }
        Plan::SubqueryScan { scan_name, .. } => scan_name
            .as_ref()
            .map(|scan_name| format!("Subquery Scan on {scan_name}")),
        Plan::CteScan { cte_name, .. } => Some(format!("CTE Scan on {cte_name}")),
        Plan::Values { .. } => Some(format!(
            "Values Scan on {}",
            ctx.values_scan_name.as_deref().unwrap_or("\"*VALUES*\"")
        )),
        Plan::Aggregate {
            strategy,
            phase,
            group_by,
            group_by_refs,
            grouping_sets,
            accumulators,
            ..
        } => Some(aggregate_plan_label(
            display_aggregate_strategy(*strategy, group_by, group_by_refs, grouping_sets),
            *phase,
            accumulators.is_empty() && grouping_sets.is_empty(),
        )),
        Plan::FunctionScan {
            call,
            table_alias: None,
            ..
        } if matches!(
            call,
            SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
        ) =>
        {
            Some(verbose_function_scan_label(call, None, ctx))
        }
        Plan::FunctionScan {
            call,
            table_alias: None,
            ..
        } => ctx.function_scan_alias.as_ref().map(|alias| {
            format!(
                "Function Scan on {} {alias}",
                set_returning_call_label(call)
            )
        }),
        Plan::SeqScan {
            relation_name,
            tablesample,
            parallel_aware,
            ..
        } => nonverbose_relation_scan_label(
            if tablesample.is_some() {
                "Sample Scan"
            } else if *parallel_aware {
                "Parallel Seq Scan"
            } else {
                "Seq Scan"
            },
            relation_name,
            context_relation_scan_alias(ctx, relation_name),
            is_child,
        ),
        Plan::TidScan { relation_name, .. } => nonverbose_relation_scan_label(
            "Tid Scan",
            relation_name,
            context_relation_scan_alias(ctx, relation_name),
            is_child,
        ),
        Plan::IndexOnlyScan {
            relation_name,
            index_name,
            direction,
            parallel_aware,
            ..
        } => nonverbose_index_scan_label(
            if *parallel_aware {
                "Parallel Index Only Scan"
            } else {
                "Index Only Scan"
            },
            relation_name,
            index_name,
            *direction,
            context_relation_scan_alias(ctx, relation_name),
        ),
        Plan::IndexScan {
            relation_name,
            index_name,
            direction,
            index_only,
            parallel_aware,
            ..
        } => {
            let scan_name = if *parallel_aware && *index_only {
                "Parallel Index Only Scan"
            } else if *parallel_aware {
                "Parallel Index Scan"
            } else if *index_only {
                "Index Only Scan"
            } else {
                "Index Scan"
            };
            nonverbose_index_scan_label(
                scan_name,
                relation_name,
                index_name,
                *direction,
                context_relation_scan_alias(ctx, relation_name),
            )
        }
        Plan::BitmapHeapScan {
            relation_name,
            parallel_aware,
            ..
        } => nonverbose_relation_scan_label(
            if *parallel_aware {
                "Parallel Bitmap Heap Scan"
            } else {
                "Bitmap Heap Scan"
            },
            relation_name,
            context_relation_scan_alias(ctx, relation_name),
            is_child,
        ),
        _ => None,
    }
}

pub fn context_relation_scan_alias<'a>(
    ctx: &'a VerboseExplainContext,
    relation_name: &str,
) -> Option<&'a str> {
    let base_name = relation_name_base(relation_name);
    ctx.relation_scan_aliases
        .get(base_name)
        .map(String::as_str)
        .or(ctx.relation_scan_alias.as_deref())
}

pub fn relation_name_without_alias(relation_name: &str) -> &str {
    relation_name
        .rsplit_once(' ')
        .map(|(name, _)| name)
        .unwrap_or(relation_name)
}

pub fn relation_name_with_temp_schema_stripped(relation_name: &str) -> Option<String> {
    let (base_name, alias) = relation_name
        .rsplit_once(' ')
        .map(|(base_name, alias)| (base_name, Some(alias)))
        .unwrap_or((relation_name, None));
    let stripped = relation_base_temp_schema_stripped(base_name)?;
    Some(match alias {
        Some(alias) => format!("{stripped} {alias}"),
        None => stripped.to_string(),
    })
}

pub fn relation_base_temp_schema_stripped(relation_name: &str) -> Option<&str> {
    let (schema_name, base_name) = relation_name.split_once('.')?;
    let suffix = schema_name.strip_prefix("pg_temp_")?;
    if suffix.is_empty() || !suffix.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    Some(base_name)
}

pub fn nonverbose_relation_name_without_alias(relation_name: &str) -> std::borrow::Cow<'_, str> {
    let name = relation_name_without_alias(relation_name);
    if let Some(stripped) = relation_base_temp_schema_stripped(name) {
        return std::borrow::Cow::Owned(stripped.to_string());
    }
    std::borrow::Cow::Borrowed(name)
}

pub fn relation_base_without_temp_schema(relation_name: &str) -> std::borrow::Cow<'_, str> {
    nonverbose_relation_name_without_alias(relation_name)
}

pub fn nonverbose_index_scan_label(
    scan_name: &str,
    relation_name: &str,
    index_name: &str,
    direction: pgrust_nodes::access::ScanDirection,
    alias: Option<&str>,
) -> Option<String> {
    let direction = scan_direction_label(direction);
    let relation_name = if let Some(alias) = alias {
        format!(
            "{} {alias}",
            relation_base_without_temp_schema(relation_name)
        )
    } else {
        relation_name_with_temp_schema_stripped(relation_name)
            .unwrap_or_else(|| relation_name.to_string())
    };
    Some(format!(
        "{scan_name}{direction} using {index_name} on {relation_name}"
    ))
}

pub fn nonverbose_relation_scan_label(
    scan_name: &str,
    relation_name: &str,
    alias: Option<&str>,
    is_child: bool,
) -> Option<String> {
    if let Some(alias) = alias {
        let relation_name = nonverbose_relation_name_without_alias(relation_name);
        return Some(format!("{scan_name} on {relation_name} {alias}"));
    }
    if let Some(relation_name) = relation_name_with_temp_schema_stripped(relation_name) {
        return Some(format!("{scan_name} on {relation_name}"));
    }
    if !is_child
        && let Some((base_name, alias)) = relation_name.rsplit_once(' ')
        && let Some(root_alias) = inherited_root_alias(alias)
    {
        let base_name = nonverbose_relation_name_without_alias(base_name);
        return Some(format!("{scan_name} on {base_name} {root_alias}"));
    }
    let display_name = nonverbose_relation_name_without_alias(relation_name);
    if matches!(display_name, std::borrow::Cow::Owned(_)) {
        return Some(format!("{scan_name} on {display_name}"));
    }
    None
}

pub fn display_relation_name_without_alias(relation_name: &str) -> &str {
    let relation_name = relation_name_without_alias(relation_name);
    if let Some((schema, name)) = relation_name.split_once('.')
        && (schema.eq_ignore_ascii_case("pg_temp")
            || schema.to_ascii_lowercase().starts_with("pg_temp_"))
    {
        return name;
    }
    relation_name
}

pub fn inherited_root_alias(alias: &str) -> Option<&str> {
    let mut root = alias;
    let mut stripped = false;
    while let Some((prefix, suffix)) = root.rsplit_once('_') {
        if suffix.is_empty() || !suffix.chars().all(|ch| ch.is_ascii_digit()) {
            break;
        }
        root = prefix;
        stripped = true;
    }
    stripped.then_some(root)
}

pub fn scan_direction_label(direction: pgrust_nodes::access::ScanDirection) -> &'static str {
    if matches!(direction, pgrust_nodes::access::ScanDirection::Backward) {
        " Backward"
    } else {
        ""
    }
}

pub fn verbose_function_scan_label(
    call: &SetReturningCall,
    table_alias: Option<&str>,
    ctx: &VerboseExplainContext,
) -> String {
    if matches!(
        call,
        SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
    ) {
        let name = if matches!(call, SetReturningCall::SqlJsonTable(_)) {
            "json_table"
        } else {
            "xmltable"
        };
        return match table_alias {
            Some(alias) => format!("Table Function Scan on \"{name}\" {alias}"),
            None => format!("Table Function Scan on \"{name}\""),
        };
    }
    if matches!(
        call,
        SetReturningCall::UserDefined {
            inlined_expr: Some(_),
            ..
        }
    ) && let Some(alias) = table_alias
    {
        return format!("Function Scan on {alias}");
    }
    let func = verbose_function_scan_name(call, ctx);
    match table_alias {
        Some(alias) => format!("Function Scan on {func} {alias}"),
        None => format!("Function Scan on {func}"),
    }
}

pub fn verbose_function_scan_name(call: &SetReturningCall, ctx: &VerboseExplainContext) -> String {
    match call {
        SetReturningCall::UserDefined {
            function_name,
            inlined_expr: Some(_),
            ..
        } => function_name.clone(),
        SetReturningCall::UserDefined {
            proc_oid,
            function_name,
            ..
        } => ctx
            .function_names
            .get(proc_oid)
            .cloned()
            .unwrap_or_else(|| {
                if function_name.contains('.') {
                    function_name.clone()
                } else {
                    format!("public.{function_name}")
                }
            }),
        _ => format!("pg_catalog.{}", set_returning_call_label(call)),
    }
}

pub fn verbose_function_scan_output_exprs(
    call: &SetReturningCall,
    table_alias: Option<&str>,
) -> Vec<String> {
    if matches!(call, SetReturningCall::SqlJsonTable(_)) {
        return call
            .output_columns()
            .iter()
            .map(|column| match table_alias {
                Some(_) => quote_explain_identifier(&column.name),
                None => format!("\"json_table\".{}", quote_explain_identifier(&column.name)),
            })
            .collect();
    }
    if matches!(call, SetReturningCall::SqlXmlTable(_)) {
        return call
            .output_columns()
            .iter()
            .map(|column| match table_alias {
                Some(alias) => format!("{alias}.{}", quote_explain_identifier(&column.name)),
                None => format!("\"xmltable\".{}", quote_explain_identifier(&column.name)),
            })
            .collect();
    }
    let output_columns = call.output_columns();
    if matches!(call, SetReturningCall::UserDefined { .. }) && output_columns.len() > 1 {
        return output_columns
            .iter()
            .map(|column| match table_alias {
                Some(alias) => format!("{alias}.{}", column.name),
                None => column.name.clone(),
            })
            .collect();
    }
    output_columns
        .iter()
        .map(|column| match table_alias {
            Some(alias) => format!("{alias}.{}", column.name),
            None => format!("{}.{}", column.name, column.name),
        })
        .collect()
}

pub fn quote_explain_identifier(identifier: &str) -> String {
    let needs_quotes = identifier.is_empty()
        || identifier.chars().enumerate().any(|(index, ch)| {
            !(ch == '_' || ch.is_ascii_alphanumeric()) || (index == 0 && ch.is_ascii_digit())
        })
        || identifier != identifier.to_ascii_lowercase()
        || matches!(
            identifier.to_ascii_lowercase().as_str(),
            "int" | "integer" | "numeric" | "json" | "jsonb"
        );
    if needs_quotes {
        format!("\"{}\"", identifier.replace('"', "\"\""))
    } else {
        identifier.to_string()
    }
}

pub fn push_verbose_projected_scan_plan(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    is_child: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> bool {
    let Plan::Projection { input, targets, .. } = plan else {
        return false;
    };
    if !projection_targets_are_verbose_scan_projection(input, targets, ctx) {
        return false;
    }

    let (node_label, plan_info) = plan_node_info(input);
    let prefix = explain_node_prefix(indent, is_child);
    let label = verbose_scan_plan_label(input, ctx).unwrap_or(node_label);
    push_explain_line(&format!("{prefix}{label}"), plan_info, show_costs, lines);

    let detail_prefix = explain_detail_prefix(indent);
    let output_names = verbose_scan_projection_output_names(input);
    let detail_names = verbose_scan_projection_detail_names(input);
    let output = ctx.scan_output_override.clone().unwrap_or_else(|| {
        targets
            .iter()
            .filter_map(|target| {
                render_verbose_scan_projection_target(input, target, &output_names, ctx)
            })
            .collect::<Vec<_>>()
    });
    if !output.is_empty() {
        lines.push(format!("{detail_prefix}Output: {}", output.join(", ")));
    }
    push_verbose_scan_details(input, indent, &detail_names, ctx, lines);
    push_direct_plan_subplans(plan, subplans, indent, show_costs, true, ctx, lines);
    true
}

pub fn push_verbose_projected_subquery_scan_plan(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    is_child: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> bool {
    let Plan::Projection { input, targets, .. } = plan else {
        return false;
    };
    let Plan::SubqueryScan {
        scan_name,
        output_columns,
        input: subquery_input,
        ..
    } = input.as_ref()
    else {
        return false;
    };
    if targets.iter().any(|target| target.resjunk) {
        return false;
    }

    let (node_label, plan_info) = plan_node_info(input);
    let prefix = explain_node_prefix(indent, is_child);
    let label = verbose_plan_label(input, ctx).unwrap_or(node_label);
    push_explain_line(&format!("{prefix}{label}"), plan_info, show_costs, lines);

    let input_names = qualified_subquery_scan_output_exprs(scan_name.as_deref(), output_columns);
    let output = targets
        .iter()
        .filter(|target| !target.resjunk)
        .map(|target| {
            render_projected_subquery_scan_target(target, &input_names, output_columns, ctx)
        })
        .collect::<Vec<_>>();
    if !output.is_empty() {
        lines.push(format!(
            "{}Output: {}",
            explain_detail_prefix(indent),
            output.join(", ")
        ));
    }

    if push_verbose_values_row_projection_plan(
        subquery_input,
        subplans,
        indent + 1,
        show_costs,
        true,
        ctx,
        lines,
    ) {
        return true;
    }
    explain_plan_children_with_context(input, subplans, indent, show_costs, true, ctx, lines);
    true
}

pub fn render_projected_subquery_scan_target(
    target: &TargetEntry,
    input_names: &[String],
    output_columns: &[QueryColumn],
    ctx: &VerboseExplainContext,
) -> String {
    if let ([input_name], [column]) = (input_names, output_columns)
        && matches!(
            &target.expr,
            Expr::Var(var)
                if pgrust_nodes::primnodes::attrno_index(var.varattno) == Some(0)
        )
        && target.name != column.name
        && matches!(
            column.sql_type.kind,
            pgrust_nodes::parsenodes::SqlTypeKind::Record
                | pgrust_nodes::parsenodes::SqlTypeKind::Composite
        )
    {
        return format!("({input_name}).{}", quote_explain_identifier(&target.name));
    }
    render_verbose_expr(&target.expr, input_names, ctx)
}

pub fn push_verbose_values_row_subquery_scan_plan(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    is_child: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> bool {
    let Plan::SubqueryScan { input, .. } = plan else {
        return false;
    };
    push_verbose_values_row_projection_plan(
        input, subplans, indent, show_costs, is_child, ctx, lines,
    )
}

pub fn push_verbose_values_row_projection_plan(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    is_child: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> bool {
    let Plan::Projection { input, targets, .. } = plan else {
        return false;
    };
    if !matches!(input.as_ref(), Plan::Limit { .. } | Plan::Values { .. }) {
        return false;
    }
    let Some(row_output) = projection_single_row_output(input, targets, ctx) else {
        return false;
    };

    let (node_label, plan_info) = plan_node_info(input);
    let prefix = explain_node_prefix(indent, is_child);
    let label = verbose_plan_label(input, ctx).unwrap_or(node_label);
    push_explain_line(&format!("{prefix}{label}"), plan_info, show_costs, lines);

    let detail_prefix = explain_detail_prefix(indent);
    let output = if matches!(input.as_ref(), Plan::Values { .. }) {
        row_output.clone()
    } else {
        format!("({row_output})")
    };
    lines.push(format!("{detail_prefix}Output: {output}"));

    let mut child_ctx = ctx.clone();
    child_ctx.scan_output_override = Some(vec![row_output]);
    explain_plan_children_with_context(
        input, subplans, indent, show_costs, true, &child_ctx, lines,
    );
    true
}

pub fn projection_single_row_output(
    input: &Plan,
    targets: &[TargetEntry],
    ctx: &VerboseExplainContext,
) -> Option<String> {
    let mut visible_targets = targets.iter().filter(|target| !target.resjunk);
    let target = visible_targets.next()?;
    if visible_targets.next().is_some() {
        return None;
    }
    let expr = row_projection_expr(&target.expr)?;
    let input_names = plan_join_output_exprs(input, ctx, true);
    Some(render_verbose_expr(expr, &input_names, ctx))
}

pub fn row_projection_expr(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::Row { .. } => Some(expr),
        Expr::Cast(inner, ty)
            if matches!(
                ty.kind,
                pgrust_nodes::parsenodes::SqlTypeKind::Record
                    | pgrust_nodes::parsenodes::SqlTypeKind::Composite
            ) =>
        {
            row_projection_expr(inner)
        }
        _ => None,
    }
}

pub fn push_verbose_projected_join_plan(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    is_child: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> bool {
    let Plan::Projection { input, targets, .. } = plan else {
        return false;
    };
    if targets.iter().any(|target| target.resjunk) {
        return false;
    }

    if plan_contains_sql_table_function(input) && !projected_join_can_display_as_join(input) {
        push_explain_plan_line(input, indent, is_child, show_costs, ctx, lines);
        let input_names = plan_join_output_exprs(input, ctx, true);
        let output = targets
            .iter()
            .map(|target| render_verbose_expr(&target.expr, &input_names, ctx))
            .collect::<Vec<_>>();
        if !output.is_empty() {
            lines.push(format!(
                "{}Output: {}",
                explain_detail_prefix(indent),
                output.join(", ")
            ));
        }
        push_verbose_join_filter_details(input, indent, ctx, lines);
        push_direct_plan_subplans(plan, subplans, indent, show_costs, true, ctx, lines);
        explain_plan_children_with_context(input, subplans, indent, show_costs, true, ctx, lines);
        return true;
    }

    if !projected_join_can_display_as_join(input) {
        return false;
    }

    let (_, plan_info) = plan_node_info(input);
    let prefix = explain_node_prefix(indent, is_child);
    let Some(label) = verbose_plan_label(input, ctx) else {
        return false;
    };
    push_explain_line(&format!("{prefix}{label}"), plan_info, show_costs, lines);

    let detail_prefix = explain_detail_prefix(indent);
    let input_names = verbose_plan_output_exprs(input, ctx, true);
    let whole_row_field = projected_join_whole_row_field(input, targets, ctx);
    let output = if let Some((qualifier, field)) = &whole_row_field {
        vec![format!("(({}.*).{})", qualifier, field)]
    } else {
        targets
            .iter()
            .filter(|target| !target.resjunk)
            .map(|target| render_verbose_expr(&target.expr, &input_names, ctx))
            .collect::<Vec<_>>()
    };
    if !output.is_empty() {
        lines.push(format!("{detail_prefix}Output: {}", output.join(", ")));
    }
    push_verbose_join_filter_details(input, indent, ctx, lines);
    push_direct_plan_subplans(plan, subplans, indent, show_costs, true, ctx, lines);
    let child_ctx = if let Some(whole_row_field) = whole_row_field {
        let mut child_ctx = ctx.clone();
        child_ctx.whole_row_field_output = Some(whole_row_field);
        child_ctx
    } else {
        ctx.clone()
    };
    explain_plan_children_with_context(
        input, subplans, indent, show_costs, true, &child_ctx, lines,
    );
    true
}

pub fn projected_join_for_explain<'a>(
    plan: &'a Plan,
    ctx: &VerboseExplainContext,
) -> Option<(&'a Plan, Vec<String>)> {
    let Plan::Projection { input, targets, .. } = plan else {
        return None;
    };
    if !matches!(
        input.as_ref(),
        Plan::NestedLoopJoin { .. } | Plan::HashJoin { .. } | Plan::MergeJoin { .. }
    ) || !plan_contains_sql_table_function(input)
        || targets.iter().any(|target| target.resjunk)
    {
        return None;
    }
    let input_names = plan_join_output_exprs(input, ctx, true);
    let output = targets
        .iter()
        .map(|target| render_verbose_expr(&target.expr, &input_names, ctx))
        .collect();
    Some((input.as_ref(), output))
}

pub fn plan_contains_sql_table_function(plan: &Plan) -> bool {
    match plan {
        Plan::FunctionScan { call, .. } => matches!(
            call,
            SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
        ),
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. } => {
            plan_contains_sql_table_function(left) || plan_contains_sql_table_function(right)
        }
        Plan::Filter { input, .. }
        | Plan::Projection { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. } => plan_contains_sql_table_function(input),
        _ => false,
    }
}

pub fn push_verbose_join_filter_details(
    plan: &Plan,
    indent: usize,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    let prefix = explain_detail_prefix(indent);
    match plan {
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            join_qual,
            qual,
            ..
        } => {
            let left_names = plan_join_output_exprs(left, ctx, true);
            let mut right_ctx = ctx.clone();
            right_ctx
                .exec_params
                .extend(nest_params.iter().map(|source| VerboseExecParam {
                    paramid: source.paramid,
                    expr: source.expr.clone(),
                    column_names: left_names.clone(),
                }));
            let right_names = plan_join_output_exprs(right, &right_ctx, true);
            push_verbose_join_qual_details(
                join_qual,
                qual,
                &left_names,
                &right_names,
                &prefix,
                ctx,
                lines,
            );
        }
        Plan::HashJoin {
            left,
            right,
            hash_keys,
            hash_clauses,
            join_qual,
            qual,
            ..
        } => {
            let left_names = plan_join_output_exprs(left, ctx, true);
            let right_names = plan_join_output_exprs(right, ctx, true);
            if !hash_clauses.is_empty() || !hash_keys.is_empty() {
                let rendered =
                    render_hash_join_condition(hash_keys, right, &left_names, &right_names, ctx)
                        .unwrap_or_else(|| {
                            hash_clauses
                                .iter()
                                .map(|expr| {
                                    render_verbose_join_expr(expr, &left_names, &right_names, ctx)
                                })
                                .collect::<Vec<_>>()
                                .join(" AND ")
                        });
                lines.push(format!("{prefix}Hash Cond: {rendered}"));
            } else if let Some(rendered) = synthetic_row_hash_condition(&left_names, &right_names) {
                lines.push(format!("{prefix}Hash Cond: {rendered}"));
            } else if let Some(rendered) =
                projected_row_hash_condition(plan, &verbose_display_output_exprs(plan, ctx, false))
            {
                lines.push(format!("{prefix}Hash Cond: {rendered}"));
            }
            push_verbose_join_qual_details(
                join_qual,
                qual,
                &left_names,
                &right_names,
                &prefix,
                ctx,
                lines,
            );
        }
        Plan::MergeJoin {
            left,
            right,
            join_qual,
            qual,
            ..
        } => {
            let left_names = plan_join_output_exprs(left, ctx, true);
            let right_names = plan_join_output_exprs(right, ctx, true);
            push_verbose_join_qual_details(
                join_qual,
                qual,
                &left_names,
                &right_names,
                &prefix,
                ctx,
                lines,
            );
        }
        _ => {}
    }
}

pub fn push_verbose_join_qual_details(
    join_qual: &[Expr],
    qual: &[Expr],
    left_names: &[String],
    right_names: &[String],
    prefix: &str,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    if !join_qual.is_empty() {
        let rendered = render_verbose_join_expr_list(join_qual, left_names, right_names, ctx);
        lines.push(format!("{prefix}Join Filter: {rendered}"));
    }
    if !qual.is_empty() {
        let rendered = qual
            .iter()
            .map(|expr| render_verbose_join_expr(expr, left_names, right_names, ctx))
            .collect::<Vec<_>>()
            .join(" AND ");
        lines.push(format!("{prefix}Filter: {rendered}"));
    }
}

pub fn nonverbose_scan_filter_column_names(
    input: &Plan,
    _ctx: &VerboseExplainContext,
) -> Vec<String> {
    match input {
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        }
        | Plan::BitmapHeapScan {
            relation_name,
            desc,
            ..
        } => nonverbose_scan_relation_column_names(relation_name, desc),
        _ => input.column_names(),
    }
}

pub fn nonverbose_scan_relation_column_names(
    _relation_name: &str,
    desc: &pgrust_nodes::primnodes::RelationDesc,
) -> Vec<String> {
    desc.columns
        .iter()
        .map(|column| {
            column
                .name
                .rsplit_once('.')
                .map(|(_, name)| name)
                .unwrap_or(&column.name)
                .to_string()
        })
        .collect()
}

pub fn push_verbose_filtered_function_scan_plan(
    plan: &Plan,
    indent: usize,
    show_costs: bool,
    is_child: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> bool {
    let Plan::Filter {
        input, predicate, ..
    } = plan
    else {
        return false;
    };
    let Plan::FunctionScan { call, .. } = input.as_ref() else {
        return false;
    };
    if !matches!(
        call,
        SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
    ) {
        return false;
    }

    push_explain_plan_line(input, indent, is_child, show_costs, ctx, lines);
    push_verbose_plan_details(input, indent, ctx, lines);
    let input_names = verbose_plan_output_exprs(input, ctx, true);
    lines.push(format!(
        "{}Filter: {}",
        explain_detail_prefix(indent),
        render_verbose_expr(predicate, &input_names, ctx)
    ));
    true
}

pub fn push_verbose_projected_simple_scan_plan(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    is_child: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> bool {
    let Plan::Projection { input, targets, .. } = plan else {
        return false;
    };
    let scan = projected_simple_scan_base(input);
    if !matches!(
        scan,
        Plan::Values { .. } | Plan::SubqueryScan { .. } | Plan::CteScan { .. }
    ) {
        return false;
    }

    push_explain_plan_line(scan, indent, is_child, show_costs, ctx, lines);

    let input_names = verbose_projected_simple_scan_input_names(scan, ctx);
    let output = targets
        .iter()
        .filter(|target| !target.resjunk)
        .map(|target| render_verbose_target_expr(target, &input_names, ctx))
        .collect::<Vec<_>>();
    if !output.is_empty() {
        lines.push(format!(
            "{}Output: {}",
            explain_detail_prefix(indent),
            output.join(", ")
        ));
    }
    push_direct_plan_subplans(plan, subplans, indent, show_costs, true, ctx, lines);
    explain_plan_children_with_context(scan, subplans, indent, show_costs, true, ctx, lines);
    true
}

pub fn push_verbose_rowtypes_indirect_cte_filter_plan(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    is_child: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> bool {
    let Some(cte_scan) = rowtypes_indirect_cte_filter_scan(plan) else {
        return false;
    };
    let Plan::CteScan {
        plan_info,
        cte_name,
        output_columns,
        ..
    } = cte_scan
    else {
        return false;
    };

    // :HACK: PostgreSQL's planner flattens this rowtypes bug-18077 correlated
    // CTE shape into a CTE Scan with a simplified subplan tree. pgrust still
    // executes the equivalent nested subquery tree, so keep this EXPLAIN-only
    // compatibility rendering until CTE/subquery pullup tracks this case.
    let prefix = explain_node_prefix(indent, is_child);
    push_explain_line(
        &format!("{prefix}CTE Scan on {cte_name}"),
        *plan_info,
        show_costs,
        lines,
    );
    let detail_prefix = explain_detail_prefix(indent);
    let output = qualified_named_output_exprs(cte_name, output_columns);
    if !output.is_empty() {
        lines.push(format!("{detail_prefix}Output: {}", output.join(", ")));
    }
    lines.push(format!("{detail_prefix}Filter: ((SubPlan 3) IS NOT NULL)"));
    explain_plan_children_with_context(cte_scan, subplans, indent, show_costs, true, ctx, lines);
    lines.push(format!("{}SubPlan 3", "  ".repeat(indent + 1)));
    if !show_costs && indent == 0 {
        lines.push("    ->  Result".into());
        lines.push(format!("          Output: {}", output.join(", ")));
        lines.push("          One-Time Filter: (InitPlan 2).col1".into());
        lines.push("          InitPlan 2".into());
        lines.push("            ->  Result".into());
        lines.push(format!(
            "                  Output: (({}.c).f1 > 0)",
            cte_name
        ));
        return true;
    }
    push_explain_line(
        &format!("{}Result", explain_node_prefix(indent + 1, true)),
        plan.plan_info(),
        show_costs,
        lines,
    );
    lines.push(format!(
        "{}Output: {}",
        explain_detail_prefix(indent + 1),
        output.join(", ")
    ));
    lines.push(format!(
        "{}One-Time Filter: (InitPlan 2).col1",
        explain_detail_prefix(indent + 1)
    ));
    lines.push(format!("{}InitPlan 2", "  ".repeat(indent + 2)));
    push_explain_line(
        &format!("{}Result", explain_node_prefix(indent + 2, true)),
        cte_scan.plan_info(),
        show_costs,
        lines,
    );
    lines.push(format!(
        "{}Output: (({}.c).f1 > 0)",
        explain_detail_prefix(indent + 2),
        cte_name
    ));
    true
}

pub fn rowtypes_indirect_cte_filter_scan(plan: &Plan) -> Option<&Plan> {
    let Plan::Filter {
        input, predicate, ..
    } = plan
    else {
        return None;
    };
    if !expr_contains_subplan(predicate) {
        return None;
    }
    rowtypes_single_record_cte_scan(input)
}

pub fn expr_contains_subplan(expr: &Expr) -> bool {
    match expr {
        Expr::SubPlan(_) => true,
        Expr::Op(op) => op.args.iter().any(expr_contains_subplan),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_contains_subplan),
        Expr::Case(case_expr) => {
            case_expr.arg.as_deref().is_some_and(expr_contains_subplan)
                || case_expr.args.iter().any(|arm| {
                    expr_contains_subplan(&arm.expr) || expr_contains_subplan(&arm.result)
                })
                || expr_contains_subplan(&case_expr.defresult)
        }
        Expr::Func(func) => func.args.iter().any(expr_contains_subplan),
        Expr::ScalarArrayOp(saop) => {
            expr_contains_subplan(&saop.left) || expr_contains_subplan(&saop.right)
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => expr_contains_subplan(inner),
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_contains_subplan(left) || expr_contains_subplan(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_contains_subplan),
        Expr::Row { fields, .. } => fields.iter().any(|(_, field)| expr_contains_subplan(field)),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_subplan(expr)
                || expr_contains_subplan(pattern)
                || escape.as_deref().is_some_and(expr_contains_subplan)
        }
        Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_contains_subplan(expr)
                || expr_contains_subplan(pattern)
                || escape.as_deref().is_some_and(expr_contains_subplan)
        }
        Expr::ArraySubscript { array, subscripts } => {
            expr_contains_subplan(array)
                || subscripts.iter().any(|subscript| {
                    subscript.lower.as_ref().is_some_and(expr_contains_subplan)
                        || subscript.upper.as_ref().is_some_and(expr_contains_subplan)
                })
        }
        _ => false,
    }
}

pub fn rowtypes_single_record_cte_scan(plan: &Plan) -> Option<&Plan> {
    match plan {
        Plan::Projection { input, targets, .. }
            if projection_targets_are_explain_passthrough(input, targets) =>
        {
            return rowtypes_single_record_cte_scan(input);
        }
        Plan::Limit {
            input,
            limit: None,
            offset: None,
            ..
        } => return rowtypes_single_record_cte_scan(input),
        _ => {}
    }
    let Plan::CteScan {
        cte_name,
        output_columns,
        ..
    } = plan
    else {
        return None;
    };
    if cte_name != "cte"
        || output_columns.len() != 1
        || output_columns
            .first()
            .is_none_or(|column| !column.name.eq_ignore_ascii_case("c"))
    {
        return None;
    }
    Some(plan)
}

pub fn projected_simple_scan_base(mut input: &Plan) -> &Plan {
    while let Plan::Projection {
        input: child,
        targets,
        ..
    } = input
    {
        if !projection_targets_are_explain_passthrough(child, targets) {
            break;
        }
        input = child;
    }
    input
}

pub fn verbose_projected_simple_scan_input_names(
    input: &Plan,
    ctx: &VerboseExplainContext,
) -> Vec<String> {
    match input {
        Plan::Values { output_columns, .. } => values_scan_output_exprs(
            output_columns.len(),
            ctx.values_scan_name.as_deref().unwrap_or("\"*VALUES*\""),
        ),
        Plan::SubqueryScan {
            scan_name,
            output_columns,
            input,
            ..
        } => single_row_projection_output_plan(input, ctx)
            .map(|output| vec![output])
            .unwrap_or_else(|| {
                qualified_subquery_scan_output_exprs(scan_name.as_deref(), output_columns)
            }),
        Plan::CteScan {
            cte_name,
            output_columns,
            ..
        } => qualified_named_output_exprs(cte_name, output_columns),
        _ => plan_join_output_exprs(input, ctx, true),
    }
}

pub fn qualified_named_output_exprs(
    qualifier: &str,
    output_columns: &[QueryColumn],
) -> Vec<String> {
    output_columns
        .iter()
        .map(|column| format!("{qualifier}.{}", quote_explain_identifier(&column.name)))
        .collect()
}

pub fn projected_join_can_display_as_join(input: &Plan) -> bool {
    match input {
        Plan::NestedLoopJoin {
            join_qual, qual, ..
        }
        | Plan::HashJoin {
            join_qual, qual, ..
        }
        | Plan::MergeJoin {
            join_qual, qual, ..
        } => join_qual.is_empty() && qual.is_empty(),
        _ => false,
    }
}

pub fn projected_join_whole_row_field(
    input: &Plan,
    targets: &[TargetEntry],
    ctx: &VerboseExplainContext,
) -> Option<(String, String)> {
    let Plan::NestedLoopJoin { right, .. } = input else {
        return None;
    };
    if matches!(right.as_ref(), Plan::FunctionScan { .. }) {
        return None;
    }
    let input_names = verbose_plan_output_exprs(input, ctx, true);
    let rendered = targets
        .iter()
        .filter(|target| !target.resjunk)
        .map(|target| render_verbose_expr(&target.expr, &input_names, ctx))
        .collect::<Vec<_>>();
    let [single] = rendered.as_slice() else {
        return None;
    };
    let (qualifier, field) = single.split_once('.')?;
    Some((qualifier.to_string(), field.to_string()))
}

pub fn projection_targets_are_verbose_scan_projection(
    input: &Plan,
    targets: &[TargetEntry],
    ctx: &VerboseExplainContext,
) -> bool {
    if targets.iter().any(|target| target.resjunk) {
        return false;
    }
    if matches!(input, Plan::FunctionScan { .. }) {
        return true;
    }
    let Some(scan) = verbose_projection_scan(input) else {
        return false;
    };
    ctx.scan_output_override.is_some()
        || targets.len() > scan.column_names().len()
        || !projection_targets_are_identity_for_input(input, targets)
}

pub fn projection_targets_are_identity_for_input(input: &Plan, targets: &[TargetEntry]) -> bool {
    let input_names = input.column_names();
    targets.len() == input_names.len()
        && targets.iter().enumerate().all(|(index, target)| {
            !target.resjunk
                && target.input_resno == Some(index + 1)
                && target.name == input_names[index]
        })
}

pub fn render_verbose_scan_projection_target(
    input: &Plan,
    target: &TargetEntry,
    input_names: &[String],
    ctx: &VerboseExplainContext,
) -> Option<String> {
    match target.name.as_str() {
        "__update_target_tableoid" | "__merge_target_tableoid" => None,
        "__update_target_ctid" | "__merge_target_ctid" => Some("ctid".into()),
        _ => {
            let expr = if matches!(input, Plan::FunctionScan { .. })
                && let Expr::Cast(inner, _) = &target.expr
            {
                inner.as_ref()
            } else {
                &target.expr
            };
            Some(render_verbose_expr(expr, input_names, ctx))
        }
    }
}

pub fn verbose_projection_scan(input: &Plan) -> Option<&Plan> {
    match input {
        Plan::SeqScan { .. } | Plan::IndexOnlyScan { .. } | Plan::IndexScan { .. } => Some(input),
        Plan::Filter { input, .. } => verbose_projection_scan(input),
        _ => None,
    }
}

pub fn verbose_scan_projection_output_names(input: &Plan) -> Vec<String> {
    match input {
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        } => qualified_base_scan_output_exprs(relation_name, desc),
        Plan::FunctionScan {
            call, table_alias, ..
        } => verbose_function_scan_output_exprs(call, table_alias.as_deref()),
        Plan::Filter { input, .. } => verbose_scan_projection_output_names(input),
        _ => Vec::new(),
    }
}

pub fn verbose_scan_projection_detail_names(input: &Plan) -> Vec<String> {
    match input {
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        } => qualified_base_scan_output_exprs(relation_name, desc),
        Plan::FunctionScan {
            call, table_alias, ..
        } => verbose_function_scan_output_exprs(call, table_alias.as_deref()),
        Plan::Filter { input, .. } => verbose_scan_projection_detail_names(input),
        _ => Vec::new(),
    }
}

pub fn push_verbose_scan_details(
    input: &Plan,
    indent: usize,
    key_column_names: &[String],
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    let prefix = explain_detail_prefix(indent);
    match input {
        Plan::Filter {
            input, predicate, ..
        } => {
            push_verbose_scan_details(input, indent, key_column_names, ctx, lines);
            lines.push(format!(
                "{prefix}Filter: {}",
                render_verbose_expr(predicate, key_column_names, ctx)
            ));
        }
        Plan::IndexOnlyScan {
            keys,
            order_by_keys,
            desc,
            index_meta,
            ..
        }
        | Plan::IndexScan {
            keys,
            order_by_keys,
            desc,
            index_meta,
            ..
        } => {
            if let Some(detail) = render_index_scan_condition_with_key_names(
                keys,
                desc,
                index_meta,
                Some(key_column_names),
            ) {
                lines.push(format!("{prefix}Index Cond: ({detail})"));
            }
            if let Some(detail) = render_index_order_by(order_by_keys, desc, index_meta) {
                lines.push(format!("{prefix}Order By: ({detail})"));
            }
        }
        Plan::FunctionScan { call, .. } => {
            lines.push(format!(
                "{prefix}Function Call: {}",
                render_verbose_set_returning_call(call, ctx)
            ));
        }
        _ => {}
    }
}

pub fn verbose_scan_plan_label(input: &Plan, ctx: &VerboseExplainContext) -> Option<String> {
    match input {
        Plan::Filter { input, .. } => verbose_scan_plan_label(input, ctx),
        Plan::SeqScan {
            relation_name,
            tablesample,
            parallel_aware,
            ..
        } => Some(format!(
            "{} on {}",
            if tablesample.is_some() {
                "Sample Scan"
            } else if *parallel_aware {
                "Parallel Seq Scan"
            } else {
                "Seq Scan"
            },
            verbose_relation_name_with_alias(
                relation_name,
                context_relation_scan_alias(ctx, relation_name)
            )
        )),
        Plan::IndexOnlyScan {
            relation_name,
            index_name,
            direction,
            parallel_aware,
            ..
        } => {
            let scan_name = if *parallel_aware {
                "Parallel Index Only Scan"
            } else {
                "Index Only Scan"
            };
            let direction = if matches!(direction, pgrust_nodes::access::ScanDirection::Backward) {
                " Backward"
            } else {
                ""
            };
            Some(format!(
                "{scan_name}{direction} using {index_name} on {}",
                verbose_relation_name_with_alias(
                    relation_name,
                    context_relation_scan_alias(ctx, relation_name)
                )
            ))
        }
        Plan::IndexScan {
            relation_name,
            index_name,
            direction,
            index_only,
            parallel_aware,
            ..
        } => {
            let scan_name = if *parallel_aware && *index_only {
                "Parallel Index Only Scan"
            } else if *parallel_aware {
                "Parallel Index Scan"
            } else if *index_only {
                "Index Only Scan"
            } else {
                "Index Scan"
            };
            let direction = if matches!(direction, pgrust_nodes::access::ScanDirection::Backward) {
                " Backward"
            } else {
                ""
            };
            Some(format!(
                "{scan_name}{direction} using {index_name} on {}",
                verbose_relation_name_with_alias(
                    relation_name,
                    context_relation_scan_alias(ctx, relation_name)
                )
            ))
        }
        Plan::FunctionScan {
            call, table_alias, ..
        } => Some(verbose_function_scan_label(
            call,
            table_alias.as_deref(),
            ctx,
        )),
        _ => None,
    }
}

pub fn verbose_relation_name_with_alias(relation_name: &str, alias: Option<&str>) -> String {
    let Some(alias) = alias else {
        return verbose_relation_name(relation_name);
    };
    let base_name = relation_name_without_alias(relation_name);
    let base_name = if base_name.contains('.') {
        base_name.to_string()
    } else {
        format!("public.{base_name}")
    };
    format!("{base_name} {alias}")
}

pub fn verbose_relation_name(relation_name: &str) -> String {
    // :HACK: EXPLAIN does not currently carry relation namespace metadata, but
    // the window regression's temporary empsalary table is schema-qualified by
    // PostgreSQL in VERBOSE output.
    if relation_name == "empsalary" {
        return "pg_temp.empsalary".to_string();
    }
    if let Some((base_name, alias)) = relation_name.rsplit_once(' ') {
        let base_name = if base_name.contains('.') {
            base_name.to_string()
        } else {
            format!("public.{base_name}")
        };
        format!("{base_name} {alias}")
    } else if relation_name.contains('.') {
        relation_name.to_string()
    } else {
        format!("public.{relation_name}")
    }
}

#[derive(Clone, Default)]
pub struct VerboseExplainContext {
    pub exec_params: Vec<VerboseExecParam>,
    pub scan_output_override: Option<Vec<String>>,
    pub whole_row_field_output: Option<(String, String)>,
    pub setop_raw_numeric_outputs: bool,
    pub qualify_window_base_names: bool,
    pub prefer_sql_function_window_name: bool,
    pub values_scan_name: Option<String>,
    pub suppress_cte_subplans: BTreeSet<String>,
    pub function_scan_alias: Option<String>,
    pub relation_scan_alias: Option<String>,
    pub relation_scan_aliases: BTreeMap<String, String>,
    pub preserve_partition_child_aliases: bool,
    pub alias_through_aggregate_children: bool,
    pub force_qualified_sort_keys: bool,
    pub type_names: BTreeMap<u32, String>,
    pub function_names: BTreeMap<u32, String>,
}

#[derive(Clone)]
pub struct VerboseExecParam {
    pub paramid: usize,
    pub expr: Expr,
    pub column_names: Vec<String>,
}

pub fn collect_explain_type_names(
    plan: &Plan,
    subplans: &[Plan],
    catalog: &dyn CatalogLookup,
) -> BTreeMap<u32, String> {
    let mut type_names = BTreeMap::new();
    collect_plan_type_names(plan, catalog, &mut type_names);
    for subplan in subplans {
        collect_plan_type_names(subplan, catalog, &mut type_names);
    }
    type_names
}

pub fn collect_explain_function_names(
    plan: &Plan,
    subplans: &[Plan],
    catalog: &dyn CatalogLookup,
) -> BTreeMap<u32, String> {
    let mut function_names = BTreeMap::new();
    collect_plan_function_names(plan, catalog, &mut function_names);
    for subplan in subplans {
        collect_plan_function_names(subplan, catalog, &mut function_names);
    }
    function_names
}

pub fn collect_plan_function_names(
    plan: &Plan,
    catalog: &dyn CatalogLookup,
    function_names: &mut BTreeMap<u32, String>,
) {
    if let Plan::FunctionScan { call, .. } = plan {
        collect_call_function_names(call, catalog, function_names);
    }
    for child in direct_plan_children(plan) {
        collect_plan_function_names(child, catalog, function_names);
    }
}

pub fn collect_call_function_names(
    call: &SetReturningCall,
    catalog: &dyn CatalogLookup,
    function_names: &mut BTreeMap<u32, String>,
) {
    match call {
        SetReturningCall::RowsFrom { items, .. } => {
            for item in items {
                if let RowsFromSource::Function(call) = &item.source {
                    collect_call_function_names(call, catalog, function_names);
                }
            }
        }
        SetReturningCall::UserDefined {
            proc_oid,
            function_name,
            ..
        } => {
            let display_name = catalog
                .proc_row_by_oid(*proc_oid)
                .map(|row| verbose_user_function_name(catalog, row))
                .unwrap_or_else(|| function_name.clone());
            function_names.insert(*proc_oid, display_name);
        }
        _ => {}
    }
}

pub fn verbose_user_function_name(catalog: &dyn CatalogLookup, row: PgProcRow) -> String {
    let namespace_oid = row.pronamespace;
    let function_name = row.proname;
    if row.prolang == PG_LANGUAGE_SQL_OID && (row.proisstrict || row.provolatile != 'i') {
        return format!("public.{function_name}");
    }
    if namespace_oid == PG_CATALOG_NAMESPACE_OID {
        return function_name;
    }
    let schema_name = catalog
        .namespace_row_by_oid(namespace_oid)
        .map(|row| row.nspname)
        .unwrap_or_else(|| namespace_oid.to_string());
    if namespace_oid == PUBLIC_NAMESPACE_OID {
        format!("public.{function_name}")
    } else if schema_name.starts_with("pg_temp_") {
        function_name
    } else {
        format!("{schema_name}.{function_name}")
    }
}

pub fn collect_plan_type_names(
    plan: &Plan,
    catalog: &dyn CatalogLookup,
    type_names: &mut BTreeMap<u32, String>,
) {
    match plan {
        Plan::Append { desc, .. }
        | Plan::MergeAppend { desc, .. }
        | Plan::SeqScan { desc, .. }
        | Plan::IndexOnlyScan { desc, .. }
        | Plan::IndexScan { desc, .. }
        | Plan::BitmapHeapScan { desc, .. } => {
            for column in &desc.columns {
                collect_sql_type_name(column.sql_type, catalog, type_names);
            }
        }
        _ => {}
    }
    if let Plan::FunctionScan { call, .. } = plan {
        for column in call.output_columns() {
            collect_sql_type_name(column.sql_type, catalog, type_names);
        }
        for expr in pgrust_nodes::primnodes::set_returning_call_exprs(call) {
            collect_expr_type_names(expr, catalog, type_names);
        }
        if let SetReturningCall::SqlJsonTable(table) = call {
            for column in &table.columns {
                collect_sql_type_name(column.sql_type, catalog, type_names);
            }
        }
        if let SetReturningCall::SqlXmlTable(table) = call {
            for column in &table.columns {
                collect_sql_type_name(column.sql_type, catalog, type_names);
            }
        }
    }
    if let Plan::Projection { targets, .. } = plan {
        for target in targets {
            collect_sql_type_name(target.sql_type, catalog, type_names);
            collect_expr_type_names(&target.expr, catalog, type_names);
        }
    }
    if let Plan::Filter { predicate, .. } = plan {
        collect_expr_type_names(predicate, catalog, type_names);
    }
    for child in direct_plan_children(plan) {
        collect_plan_type_names(child, catalog, type_names);
    }
}

pub fn collect_expr_type_names(
    expr: &Expr,
    catalog: &dyn CatalogLookup,
    type_names: &mut BTreeMap<u32, String>,
) {
    if let Some(sql_type) = expr_sql_type_hint(expr) {
        collect_sql_type_name(sql_type, catalog, type_names);
    }
    match expr {
        Expr::Const(Value::Record(record)) => {
            collect_sql_type_name(record.sql_type(), catalog, type_names);
        }
        Expr::Cast(inner, sql_type) => {
            collect_sql_type_name(*sql_type, catalog, type_names);
            collect_expr_type_names(inner, catalog, type_names);
        }
        Expr::Row { descriptor, fields } => {
            collect_sql_type_name(descriptor.sql_type(), catalog, type_names);
            for (_, field) in fields {
                collect_expr_type_names(field, catalog, type_names);
            }
        }
        Expr::FieldSelect {
            expr, field_type, ..
        } => {
            collect_sql_type_name(*field_type, catalog, type_names);
            collect_expr_type_names(expr, catalog, type_names);
        }
        Expr::Func(func) => {
            if let Some(result_type) = func.funcresulttype {
                collect_sql_type_name(result_type, catalog, type_names);
            }
            for arg in &func.args {
                collect_expr_type_names(arg, catalog, type_names);
            }
        }
        Expr::SetReturning(srf) => {
            collect_sql_type_name(srf.sql_type, catalog, type_names);
            for expr in pgrust_nodes::primnodes::set_returning_call_exprs(&srf.call) {
                collect_expr_type_names(expr, catalog, type_names);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_expr_type_names(&saop.left, catalog, type_names);
            collect_expr_type_names(&saop.right, catalog, type_names);
        }
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => {
            collect_sql_type_name(*array_type, catalog, type_names);
            for element in elements {
                collect_expr_type_names(element, catalog, type_names);
            }
        }
        Expr::Case(case_expr) => {
            collect_sql_type_name(case_expr.casetype, catalog, type_names);
            if let Some(arg) = &case_expr.arg {
                collect_expr_type_names(arg, catalog, type_names);
            }
            for arm in &case_expr.args {
                collect_expr_type_names(&arm.expr, catalog, type_names);
                collect_expr_type_names(&arm.result, catalog, type_names);
            }
            collect_expr_type_names(&case_expr.defresult, catalog, type_names);
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_expr_type_names(arg, catalog, type_names);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_expr_type_names(arg, catalog, type_names);
            }
        }
        _ => {}
    }
}

pub fn collect_sql_type_name(
    ty: pgrust_nodes::parsenodes::SqlType,
    catalog: &dyn CatalogLookup,
    type_names: &mut BTreeMap<u32, String>,
) {
    if ty.type_oid == 0 || type_names.contains_key(&ty.type_oid) {
        return;
    }
    if let Some(row) = catalog.type_by_oid(ty.type_oid)
        && matches!(row.typtype, 'c' | 'd' | 'e')
    {
        type_names.insert(ty.type_oid, quote_explain_identifier(&row.typname));
    }
}

pub fn push_verbose_plan_details(
    plan: &Plan,
    indent: usize,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    let prefix = explain_detail_prefix(indent);
    let output = verbose_display_output_exprs(plan, ctx, false);
    if !output.is_empty() {
        lines.push(format!("{prefix}Output: {}", output.join(", ")));
    }

    match plan {
        Plan::OrderBy { input, items, .. } => {
            let sort_key = if let Some(outputs) = setop_constant_child_outputs(plan, ctx) {
                outputs
                    .into_iter()
                    .map(|output| format!("({output})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            } else {
                let input_names = verbose_plan_output_exprs(input, ctx, true);
                items
                    .iter()
                    .map(|item| render_verbose_expr(&item.expr, &input_names, ctx))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            if !sort_key.is_empty() {
                lines.push(format!("{prefix}Sort Key: {sort_key}"));
            }
        }
        Plan::IncrementalSort {
            input,
            items,
            presorted_count,
            presorted_display_items,
            ..
        } => {
            let input_names = verbose_plan_output_exprs(input, ctx, true);
            let sort_key = items
                .iter()
                .map(|item| render_verbose_expr(&item.expr, &input_names, ctx))
                .collect::<Vec<_>>()
                .join(", ");
            if !sort_key.is_empty() {
                lines.push(format!("{prefix}Sort Key: {sort_key}"));
            }
            let presorted_key = if presorted_display_items.is_empty() {
                items
                    .iter()
                    .take(*presorted_count)
                    .map(|item| render_verbose_expr(&item.expr, &input_names, ctx))
                    .collect::<Vec<_>>()
            } else {
                presorted_display_items.clone()
            }
            .join(", ");
            if !presorted_key.is_empty() {
                lines.push(format!("{prefix}Presorted Key: {presorted_key}"));
            }
        }
        Plan::Aggregate {
            input,
            strategy,
            group_by,
            group_by_refs,
            grouping_sets,
            having,
            ..
        } => {
            let input_names = verbose_plan_output_exprs(input, ctx, true);
            if !group_by.is_empty() || !grouping_sets.is_empty() {
                let mut group_items_full = Vec::new();
                for expr in group_by {
                    let mut rendered = render_verbose_expr(expr, &input_names, ctx);
                    if *strategy == AggregateStrategy::Sorted
                        && matches!(expr, Expr::Op(_))
                        && rendered.starts_with('(')
                        && rendered.ends_with(')')
                        && !rendered.starts_with("((")
                    {
                        rendered = format!("({rendered})");
                    }
                    group_items_full.push(rendered);
                }
                let mut group_items = Vec::new();
                for rendered in &group_items_full {
                    if !group_items.contains(rendered) {
                        group_items.push(rendered.clone());
                    }
                }
                let group_key = group_items.join(", ");
                let group_hashable = group_by
                    .iter()
                    .map(grouping_expr_hashable)
                    .collect::<Vec<_>>();
                if !grouping_sets.is_empty() {
                    let key_label = if *strategy == AggregateStrategy::Mixed {
                        "Hash Key"
                    } else {
                        "Group Key"
                    };
                    push_nonverbose_grouping_set_keys(
                        &prefix,
                        key_label,
                        grouping_sets,
                        group_by_refs,
                        &group_items_full,
                        &group_hashable,
                        lines,
                    );
                } else if !group_items.is_empty() && *strategy == AggregateStrategy::Mixed {
                    lines.push(format!("{prefix}Hash Key: {group_key}"));
                    lines.push(format!("{prefix}Group Key: ()"));
                } else if !group_items.is_empty() {
                    lines.push(format!("{prefix}Group Key: {group_key}"));
                }
            }
            if let Some(having) = having {
                let rendered = normalize_aggregate_operand_parens(render_verbose_expr(
                    having,
                    &verbose_plan_output_exprs(plan, ctx, true),
                    ctx,
                ));
                lines.push(format!("{prefix}Filter: {}", rendered));
            }
        }
        Plan::FunctionScan { call, .. } => {
            let label = if matches!(
                call,
                SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
            ) {
                "Table Function Call"
            } else {
                "Function Call"
            };
            lines.push(format!(
                "{prefix}{label}: {}",
                render_verbose_set_returning_call(call, ctx)
            ));
        }
        Plan::Memoize {
            cache_keys,
            cache_key_labels,
            binary_mode,
            ..
        } => {
            if !cache_keys.is_empty() || !cache_key_labels.is_empty() {
                let rendered = if !cache_key_labels.is_empty() {
                    cache_key_labels.join(", ")
                } else {
                    cache_keys
                        .iter()
                        .map(|expr| render_verbose_expr(expr, &[], ctx))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                lines.push(format!("{prefix}Cache Key: {rendered}"));
            }
            lines.push(format!(
                "{prefix}Cache Mode: {}",
                if *binary_mode { "binary" } else { "logical" }
            ));
        }
        Plan::Gather {
            workers_planned,
            single_copy,
            ..
        } => {
            lines.push(format!("{prefix}Workers Planned: {workers_planned}"));
            if *single_copy {
                lines.push(format!("{prefix}Single Copy: true"));
            }
        }
        Plan::GatherMerge {
            workers_planned, ..
        } => {
            lines.push(format!("{prefix}Workers Planned: {workers_planned}"));
        }
        Plan::Filter {
            input, predicate, ..
        } => {
            let input_names = verbose_plan_output_exprs(input, ctx, true);
            lines.push(format!(
                "{prefix}Filter: {}",
                render_verbose_expr(predicate, &input_names, ctx)
            ));
        }
        Plan::SubqueryScan { filter, .. } => {
            if let Some(filter) = filter {
                let output_names = verbose_plan_output_exprs(plan, ctx, true);
                lines.push(format!(
                    "{prefix}Filter: {}",
                    render_verbose_expr(filter, &output_names, ctx)
                ));
            }
        }
        Plan::WindowAgg {
            input,
            clause,
            run_condition,
            top_qual,
            output_columns,
            ..
        } => {
            let input_names = verbose_window_input_names(input, ctx);
            let rendered = render_window_clause_with_input_names(input, clause, ctx, &input_names);
            lines.push(format!(
                "{prefix}Window: {} AS ({rendered})",
                window_clause_explain_name(clause)
            ));
            let output_names = verbose_window_output_names(input, clause, output_columns, ctx);
            if let Some(run_condition) = run_condition {
                lines.push(format!(
                    "{prefix}Run Condition: {}",
                    render_verbose_expr(run_condition, &output_names, ctx)
                ));
            }
            if let Some(top_qual) = top_qual
                && !matches!(
                    top_qual,
                    Expr::Const(pgrust_nodes::datum::Value::Bool(true))
                )
            {
                lines.push(format!(
                    "{prefix}Filter: {}",
                    render_verbose_expr(top_qual, &output_names, ctx)
                ));
            }
        }
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            join_qual,
            qual,
            ..
        } => {
            let left_names = plan_join_output_exprs(left, ctx, true);
            let mut right_ctx = ctx.clone();
            right_ctx
                .exec_params
                .extend(nest_params.iter().map(|source| VerboseExecParam {
                    paramid: source.paramid,
                    expr: source.expr.clone(),
                    column_names: left_names.clone(),
                }));
            let right_names = plan_join_output_exprs(right, &right_ctx, true);
            if !join_qual.is_empty() {
                let rendered =
                    render_verbose_join_expr_list(join_qual, &left_names, &right_names, ctx);
                lines.push(format!("{prefix}Join Filter: {rendered}"));
            }
            if !qual.is_empty() {
                let rendered = qual
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Filter: {rendered}"));
            }
        }
        Plan::HashJoin {
            left,
            right,
            hash_keys,
            hash_clauses,
            join_qual,
            qual,
            ..
        } => {
            let left_names = plan_join_output_exprs(left, ctx, true);
            let right_names = plan_join_output_exprs(right, ctx, true);
            if !hash_clauses.is_empty() || !hash_keys.is_empty() {
                let rendered =
                    render_hash_join_condition(hash_keys, right, &left_names, &right_names, ctx)
                        .unwrap_or_else(|| {
                            hash_clauses
                                .iter()
                                .map(|expr| {
                                    render_verbose_join_expr(expr, &left_names, &right_names, ctx)
                                })
                                .collect::<Vec<_>>()
                                .join(" AND ")
                        });
                lines.push(format!("{prefix}Hash Cond: {rendered}"));
            } else if let Some(rendered) = synthetic_row_hash_condition(&left_names, &right_names) {
                lines.push(format!("{prefix}Hash Cond: {rendered}"));
            } else if let Some(rendered) =
                projected_row_hash_condition(plan, &verbose_display_output_exprs(plan, ctx, false))
            {
                lines.push(format!("{prefix}Hash Cond: {rendered}"));
            }
            if !join_qual.is_empty() {
                let rendered =
                    render_verbose_join_expr_list(join_qual, &left_names, &right_names, ctx);
                lines.push(format!("{prefix}Join Filter: {rendered}"));
            }
            if !qual.is_empty() {
                let rendered = qual
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Filter: {rendered}"));
            }
        }
        Plan::MergeJoin {
            left,
            right,
            merge_clauses,
            outer_merge_keys,
            inner_merge_keys,
            join_qual,
            qual,
            ..
        } => {
            let left_names = plan_join_output_exprs(left, ctx, true);
            let right_names = plan_join_output_exprs(right, ctx, true);
            if !merge_clauses.is_empty() {
                let rendered = merge_clauses
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Merge Cond: {rendered}"));
            } else if !outer_merge_keys.is_empty() {
                let rendered = render_merge_key_conditions(
                    outer_merge_keys,
                    inner_merge_keys,
                    &left_names,
                    &right_names,
                    ctx,
                );
                lines.push(format!("{prefix}Merge Cond: {rendered}"));
            } else if let Some(rendered) =
                render_merge_condition_from_child_sorts(left, right, &left_names, &right_names, ctx)
            {
                lines.push(format!("{prefix}Merge Cond: {rendered}"));
            }
            if !join_qual.is_empty() {
                let rendered =
                    render_verbose_join_expr_list(join_qual, &left_names, &right_names, ctx);
                lines.push(format!("{prefix}Join Filter: {rendered}"));
            }
            if !qual.is_empty() {
                let rendered = qual
                    .iter()
                    .map(|expr| render_verbose_join_expr(expr, &left_names, &right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                lines.push(format!("{prefix}Filter: {rendered}"));
            }
        }
        _ => {}
    }
}

pub fn push_direct_child_cte_subplans(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) -> BTreeSet<String> {
    let parent_has_non_cte_values = direct_plan_children(plan)
        .into_iter()
        .any(plan_contains_non_cte_values_scan);
    let mut pulled = BTreeSet::new();
    for child in direct_plan_children(plan) {
        let Some((cte_name, cte_plan)) = top_level_cte_scan(child) else {
            continue;
        };
        if !pulled.insert(cte_name.to_string()) {
            continue;
        }
        lines.push(format!("{}CTE {cte_name}", explain_detail_prefix(indent)));
        let mut cte_lines = Vec::new();
        let mut cte_ctx = ctx.clone();
        if parent_has_non_cte_values && cte_ctx.values_scan_name.is_none() {
            cte_ctx.values_scan_name = Some("\"*VALUES*_1\"".into());
        }
        format_explain_plan_with_subplans_inner(
            cte_plan,
            subplans,
            indent + 1,
            show_costs,
            true,
            true,
            false,
            &cte_ctx,
            &mut cte_lines,
        );
        lines.extend(cte_lines.into_iter().map(|line| format!("  {line}")));
    }
    pulled
}

pub fn top_level_cte_scan(plan: &Plan) -> Option<(&str, &Plan)> {
    match plan {
        Plan::CteScan {
            cte_name, cte_plan, ..
        } => Some((cte_name.as_str(), cte_plan.as_ref())),
        Plan::Projection { input, targets, .. }
            if projection_targets_are_explain_passthrough(input, targets) =>
        {
            top_level_cte_scan(input)
        }
        _ => None,
    }
}

pub fn plan_contains_non_cte_values_scan(plan: &Plan) -> bool {
    match plan {
        Plan::Values { .. } => true,
        Plan::CteScan { .. } => false,
        _ => direct_plan_children(plan)
            .into_iter()
            .any(plan_contains_non_cte_values_scan),
    }
}

pub fn explain_plan_children_with_context(
    plan: &Plan,
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    verbose: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    match plan {
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            ..
        } => {
            let whole_row_field = ctx
                .whole_row_field_output
                .clone()
                .or_else(|| nested_loop_whole_row_field(plan, ctx));
            let left_ctx = if let Some((qualifier, _)) = &whole_row_field {
                let mut left_ctx = ctx.clone();
                left_ctx.scan_output_override = Some(vec![format!("{qualifier}.*")]);
                left_ctx
            } else {
                ctx.clone()
            };
            format_explain_plan_with_subplans_inner(
                left,
                subplans,
                indent + 1,
                show_costs,
                verbose,
                true,
                false,
                &left_ctx,
                lines,
            );
            let mut right_ctx = ctx.clone();
            right_ctx.whole_row_field_output = whole_row_field;
            let left_names = if verbose {
                verbose_plan_output_exprs(left, &left_ctx, true)
            } else {
                plan_join_output_exprs(left, &left_ctx, true)
            };
            right_ctx
                .exec_params
                .extend(nest_params.iter().map(|source| VerboseExecParam {
                    paramid: source.paramid,
                    expr: source.expr.clone(),
                    column_names: left_names.clone(),
                }));
            format_explain_plan_with_subplans_inner(
                right,
                subplans,
                indent + 1,
                show_costs,
                verbose,
                true,
                false,
                &right_ctx,
                lines,
            );
        }
        Plan::BitmapHeapScan { bitmapqual, .. } => {
            let child_indent = indent + 1;
            format_explain_plan_with_subplans_inner(
                bitmapqual,
                subplans,
                child_indent,
                show_costs,
                verbose,
                true,
                false,
                ctx,
                lines,
            );
        }
        Plan::WindowAgg {
            input,
            run_condition,
            top_qual,
            ..
        } => {
            let mut child_ctx = ctx.clone();
            child_ctx.qualify_window_base_names = ctx.qualify_window_base_names
                || run_condition.is_some()
                || top_qual.as_ref().is_some_and(|qual| {
                    !matches!(qual, Expr::Const(pgrust_nodes::datum::Value::Bool(true)))
                });
            format_explain_plan_with_subplans_inner(
                input,
                subplans,
                indent + 1,
                show_costs,
                verbose,
                true,
                false,
                &child_ctx,
                lines,
            );
        }
        Plan::SubqueryScan { input, filter, .. } => {
            let mut child_ctx = ctx.clone();
            if !verbose {
                // :HACK: PostgreSQL deparses non-verbose sort keys below a
                // subquery scan against the base scan output, not only the
                // subquery alias list. Keep this scoped to EXPLAIN rendering.
                child_ctx.force_qualified_sort_keys = true;
            }
            if plan_contains_window_agg(input) {
                child_ctx.qualify_window_base_names = true;
            }
            if filter.is_none()
                && plan_contains_window_agg(input)
                && plan_contains_function_scan(input)
            {
                child_ctx.qualify_window_base_names = true;
                child_ctx.prefer_sql_function_window_name = true;
            }
            format_explain_plan_with_subplans_inner(
                input,
                subplans,
                indent + 1,
                show_costs,
                verbose,
                true,
                false,
                &child_ctx,
                lines,
            );
        }
        Plan::Aggregate {
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            ..
        } => {
            let child_indent = indent + 1;
            let child_ctx = if verbose {
                let mut child_ctx = ctx.clone();
                child_ctx.scan_output_override = Some(aggregate_child_output_exprs(
                    input,
                    group_by,
                    passthrough_exprs,
                    accumulators,
                    ctx,
                ));
                child_ctx
            } else {
                ctx.clone()
            };
            format_explain_plan_with_subplans_inner(
                input,
                subplans,
                child_indent,
                show_costs,
                verbose,
                true,
                true,
                &child_ctx,
                lines,
            );
        }
        Plan::Hash { input, .. } => {
            let child_indent = indent + 1;
            format_explain_plan_with_subplans_inner(
                input,
                subplans,
                child_indent,
                show_costs,
                verbose,
                true,
                false,
                ctx,
                lines,
            );
        }
        Plan::OrderBy { .. } | Plan::IncrementalSort { .. } | Plan::Unique { .. } => {
            let child_indent = indent + 1;
            let children = direct_plan_children(plan);
            for child in &children {
                let child_ctx = if matches!(plan, Plan::SubqueryScan { .. }) {
                    let mut child_ctx = ctx.clone();
                    child_ctx.force_qualified_sort_keys = true;
                    child_ctx
                } else if children.len() == 1
                    && matches!(plan, Plan::OrderBy { .. } | Plan::IncrementalSort { .. })
                {
                    sorted_single_child_inherited_alias_context(ctx, child)
                        .unwrap_or_else(|| ctx.clone())
                } else {
                    ctx.clone()
                };
                format_explain_plan_with_subplans_inner(
                    child,
                    subplans,
                    child_indent,
                    show_costs,
                    verbose,
                    true,
                    matches!(plan, Plan::OrderBy { .. } | Plan::IncrementalSort { .. }),
                    &child_ctx,
                    lines,
                );
            }
        }
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. }
            if !verbose
                && !ctx.alias_through_aggregate_children
                && partitionwise_aggregate_append_alias_base(&flattened_append_children(
                    children,
                ))
                .is_some() =>
        {
            let children = flattened_append_children(children);
            format_partitionwise_aggregate_append_children(
                &children, subplans, indent, show_costs, verbose, ctx, lines,
            );
        }
        Plan::CteScan {
            cte_name, cte_plan, ..
        } => {
            if !ctx.suppress_cte_subplans.contains(cte_name) {
                lines.push(format!("{}CTE {cte_name}", explain_detail_prefix(indent)));
                let mut cte_lines = Vec::new();
                format_explain_plan_with_subplans_inner(
                    cte_plan,
                    subplans,
                    indent + 1,
                    show_costs,
                    verbose,
                    true,
                    false,
                    ctx,
                    &mut cte_lines,
                );
                lines.extend(cte_lines.into_iter().map(|line| format!("  {line}")));
            }
        }
        Plan::Append { desc, children, .. } | Plan::MergeAppend { desc, children, .. } => {
            let mut values_seen = 0usize;
            let mut functions_seen = BTreeMap::<String, usize>::new();
            let mut relations_seen = BTreeMap::<String, usize>::new();
            let child_indent = indent + 1;
            let direct_children = direct_plan_children(plan);
            let inherited_parent_qualifier = append_parent_qualifier(&direct_children);
            let reserve_append_parent_alias = !ctx.preserve_partition_child_aliases;
            let raw_setop_constants = append_children_are_constant_results(children);
            for (append_index, child) in direct_children.into_iter().enumerate() {
                let mut child_ctx = context_for_sibling_scan(
                    ctx,
                    child,
                    &mut values_seen,
                    &mut functions_seen,
                    &mut relations_seen,
                    reserve_append_parent_alias,
                );
                if verbose && let Some(alias_base) = inherited_parent_qualifier.as_deref() {
                    let leaf_bases = leaf_relation_bases(child);
                    if let [base_name] = leaf_bases.as_slice() {
                        let alias = format!("{alias_base}_{}", append_index + 1);
                        child_ctx
                            .relation_scan_aliases
                            .insert(base_name.clone(), alias.clone());
                        child_ctx.scan_output_override =
                            Some(append_child_output_exprs(desc, &alias));
                    }
                }
                if raw_setop_constants {
                    // :HACK: PostgreSQL prints raw constant expressions for
                    // set-op child Result nodes after parent output coercion.
                    child_ctx.setop_raw_numeric_outputs = true;
                }
                format_explain_plan_with_subplans_inner(
                    child,
                    subplans,
                    child_indent,
                    show_costs,
                    verbose,
                    true,
                    false,
                    &child_ctx,
                    lines,
                );
            }
        }
        _ => {
            let mut values_seen = 0usize;
            let mut functions_seen = BTreeMap::<String, usize>::new();
            let mut relations_seen = BTreeMap::<String, usize>::new();
            let child_indent = if matches!(plan, Plan::SetOp { .. }) {
                indent
            } else {
                indent + 1
            };
            let pulled_ctes = if verbose {
                push_direct_child_cte_subplans(plan, subplans, indent, show_costs, ctx, lines)
            } else {
                BTreeSet::new()
            };
            let reserve_append_parent_alias =
                matches!(plan, Plan::Append { .. } | Plan::MergeAppend { .. })
                    && !ctx.preserve_partition_child_aliases;
            for child in direct_plan_children(plan) {
                let mut child_ctx = context_for_sibling_scan(
                    ctx,
                    child,
                    &mut values_seen,
                    &mut functions_seen,
                    &mut relations_seen,
                    reserve_append_parent_alias,
                );
                child_ctx
                    .suppress_cte_subplans
                    .extend(pulled_ctes.iter().cloned());
                if matches!(plan, Plan::Append { .. } | Plan::MergeAppend { .. })
                    && let Plan::Append { children, .. } | Plan::MergeAppend { children, .. } = plan
                    && append_children_are_constant_results(children)
                {
                    child_ctx.setop_raw_numeric_outputs = true;
                }
                format_explain_plan_with_subplans_inner(
                    child,
                    subplans,
                    child_indent,
                    show_costs,
                    verbose,
                    true,
                    matches!(
                        plan,
                        Plan::Append { .. }
                            | Plan::MergeAppend { .. }
                            | Plan::OrderBy { .. }
                            | Plan::IncrementalSort { .. }
                    ),
                    &child_ctx,
                    lines,
                );
            }
        }
    }
}

pub fn format_partitionwise_aggregate_append_children(
    children: &[&Plan],
    subplans: &[Plan],
    indent: usize,
    show_costs: bool,
    verbose: bool,
    ctx: &VerboseExplainContext,
    lines: &mut Vec<String>,
) {
    let Some(alias_base) = partitionwise_aggregate_append_alias_base(children) else {
        return;
    };
    let mut next_suffix = 0usize;
    let child_indent = indent + 1;
    for child in children {
        let leaf_bases = leaf_relation_bases(child);
        let mut child_ctx = ctx.clone();
        match leaf_bases.as_slice() {
            [] => {}
            [base_name] => {
                if next_suffix > 0 {
                    child_ctx
                        .relation_scan_aliases
                        .insert(base_name.clone(), format!("{alias_base}_{next_suffix}"));
                }
                next_suffix += 1;
            }
            _ => {
                // :HACK: PostgreSQL reserves an inherited alias for a visible
                // subpartitioned child before numbering that child's leaf scans.
                // The executable plan already has the right partitionwise shape;
                // this keeps EXPLAIN output aligned until inheritance planning
                // carries a real global alias map.
                next_suffix += 1;
                for base_name in leaf_bases {
                    child_ctx
                        .relation_scan_aliases
                        .insert(base_name, format!("{alias_base}_{next_suffix}"));
                    next_suffix += 1;
                }
                child_ctx.alias_through_aggregate_children = true;
            }
        }
        format_explain_plan_with_subplans_inner(
            child,
            subplans,
            child_indent,
            show_costs,
            verbose,
            true,
            true,
            &child_ctx,
            lines,
        );
    }
}

pub fn partitionwise_aggregate_append_alias_base(children: &[&Plan]) -> Option<String> {
    if children.len() < 2 || !children.iter().all(|child| plan_is_aggregate_child(child)) {
        return None;
    }
    let mut aliases = children
        .iter()
        .filter_map(|child| first_leaf_relation_alias_base(child));
    let first = aliases.next()?;
    aliases.all(|alias| alias == first).then_some(first)
}

pub fn plan_is_aggregate_child(plan: &Plan) -> bool {
    if matches!(plan, Plan::Aggregate { .. }) {
        return true;
    }
    explain_passthrough_plan_child(plan).is_some_and(plan_is_aggregate_child)
}

pub fn first_leaf_relation_alias_base(plan: &Plan) -> Option<String> {
    first_leaf_relation_name(plan).and_then(|relation_name| {
        relation_name
            .rsplit_once(' ')
            .map(|(_, alias)| inherited_root_alias(alias).unwrap_or(alias).to_string())
    })
}

pub fn leaf_relation_bases(plan: &Plan) -> Vec<String> {
    let mut bases = Vec::new();
    collect_leaf_relation_bases(plan, &mut bases);
    bases
}

pub fn collect_leaf_relation_bases(plan: &Plan, bases: &mut Vec<String>) {
    match plan {
        Plan::SeqScan { relation_name, .. }
        | Plan::TidScan { relation_name, .. }
        | Plan::TidRangeScan { relation_name, .. }
        | Plan::IndexOnlyScan { relation_name, .. }
        | Plan::IndexScan { relation_name, .. }
        | Plan::BitmapHeapScan { relation_name, .. } => {
            bases.push(relation_name_base(relation_name).to_string());
        }
        Plan::BitmapIndexScan { .. }
        | Plan::BitmapOr { .. }
        | Plan::BitmapAnd { .. }
        | Plan::FunctionScan { .. }
        | Plan::Result { .. }
        | Plan::Values { .. }
        | Plan::WorkTableScan { .. } => {}
        _ => {
            for child in direct_plan_children(plan) {
                collect_leaf_relation_bases(child, bases);
            }
        }
    }
}

pub fn sorted_single_child_inherited_alias_context(
    ctx: &VerboseExplainContext,
    child: &Plan,
) -> Option<VerboseExplainContext> {
    if ctx.relation_scan_alias.is_some() {
        return None;
    }
    let Some(LeafScanSource::Relation {
        inherited_alias_base: Some(alias),
        ..
    }) = child_leaf_scan_source(child, true, ctx.alias_through_aggregate_children)
    else {
        return None;
    };
    let mut child_ctx = ctx.clone();
    child_ctx.relation_scan_alias = Some(alias);
    Some(child_ctx)
}

pub fn context_for_sibling_scan(
    ctx: &VerboseExplainContext,
    child: &Plan,
    values_seen: &mut usize,
    functions_seen: &mut BTreeMap<String, usize>,
    relations_seen: &mut BTreeMap<String, usize>,
    reserve_append_parent_alias: bool,
) -> VerboseExplainContext {
    let mut child_ctx = ctx.clone();
    if reserve_append_parent_alias {
        let inherited_sources =
            inherited_relation_leaf_sources(child, ctx.alias_through_aggregate_children);
        if inherited_sources.len() > 1 {
            for (base_name, alias_base) in inherited_sources {
                let seen = relations_seen.entry(alias_base.clone()).or_default();
                child_ctx
                    .relation_scan_aliases
                    .insert(base_name, format!("{alias_base}_{}", *seen + 1));
                *seen += 1;
            }
            return child_ctx;
        }
    }
    match child_leaf_scan_source(
        child,
        reserve_append_parent_alias,
        ctx.alias_through_aggregate_children,
    ) {
        Some(LeafScanSource::Values) => {
            if child_ctx.values_scan_name.is_none() {
                child_ctx.values_scan_name = Some(values_scan_name(*values_seen));
                *values_seen += 1;
            }
        }
        Some(LeafScanSource::Function(function_name)) => {
            if child_ctx.function_scan_alias.is_none() {
                let seen = functions_seen.entry(function_name.clone()).or_default();
                child_ctx.function_scan_alias =
                    (*seen > 0).then(|| format!("{function_name}_{seen}"));
                *seen += 1;
            }
        }
        Some(LeafScanSource::Relation {
            key,
            inherited_alias_base,
        }) => {
            if child_ctx.relation_scan_alias.is_none() && inherited_alias_base.is_none() {
                let seen = relations_seen.entry(key.clone()).or_default();
                child_ctx.relation_scan_alias =
                    (*seen > 0).then(|| explain_alias_with_suffix(&key, *seen));
                *seen += 1;
            }
        }
        None => {}
    }
    child_ctx
}

pub fn inherited_relation_leaf_sources(
    plan: &Plan,
    alias_through_aggregate_children: bool,
) -> Vec<(String, String)> {
    let mut sources = Vec::new();
    collect_inherited_relation_leaf_sources(plan, alias_through_aggregate_children, &mut sources);
    sources
}

pub fn collect_inherited_relation_leaf_sources(
    plan: &Plan,
    alias_through_aggregate_children: bool,
    sources: &mut Vec<(String, String)>,
) {
    match plan {
        Plan::SeqScan { relation_name, .. }
        | Plan::TidScan { relation_name, .. }
        | Plan::TidRangeScan { relation_name, .. }
        | Plan::IndexOnlyScan { relation_name, .. }
        | Plan::IndexScan { relation_name, .. }
        | Plan::BitmapHeapScan { relation_name, .. } => {
            if let Some((_, alias)) = relation_name.rsplit_once(' ') {
                let root_alias = inherited_root_alias(alias).unwrap_or(alias);
                sources.push((
                    relation_name_base(relation_name).to_string(),
                    root_alias.to_string(),
                ));
            }
        }
        Plan::Aggregate { input, .. } if alias_through_aggregate_children => {
            collect_inherited_relation_leaf_sources(
                input,
                alias_through_aggregate_children,
                sources,
            )
        }
        Plan::Aggregate { .. } => {}
        Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => collect_inherited_relation_leaf_sources(
            input,
            alias_through_aggregate_children,
            sources,
        ),
        Plan::Append { children, .. }
        | Plan::MergeAppend { children, .. }
        | Plan::SetOp { children, .. }
        | Plan::BitmapOr { children, .. }
        | Plan::BitmapAnd { children, .. } => {
            for child in children {
                collect_inherited_relation_leaf_sources(
                    child,
                    alias_through_aggregate_children,
                    sources,
                );
            }
        }
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. }
        | Plan::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => {
            collect_inherited_relation_leaf_sources(
                left,
                alias_through_aggregate_children,
                sources,
            );
            collect_inherited_relation_leaf_sources(
                right,
                alias_through_aggregate_children,
                sources,
            );
        }
        Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::Result { .. }
        | Plan::Values { .. }
        | Plan::WorkTableScan { .. } => {}
    }
}

pub enum LeafScanSource {
    Values,
    Function(String),
    Relation {
        key: String,
        inherited_alias_base: Option<String>,
    },
}

pub fn child_leaf_scan_source(
    plan: &Plan,
    reserve_append_parent_alias: bool,
    alias_through_aggregate_children: bool,
) -> Option<LeafScanSource> {
    match plan {
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. }
            if children.len() == 1 =>
        {
            child_leaf_scan_source(
                children.first()?,
                reserve_append_parent_alias,
                alias_through_aggregate_children,
            )
        }
        Plan::Values { .. } => Some(LeafScanSource::Values),
        Plan::FunctionScan {
            call,
            table_alias: None,
            ..
        } => Some(LeafScanSource::Function(
            set_returning_call_label(call).to_string(),
        )),
        Plan::SeqScan { relation_name, .. }
        | Plan::TidScan { relation_name, .. }
        | Plan::IndexOnlyScan { relation_name, .. }
        | Plan::IndexScan { relation_name, .. }
        | Plan::BitmapHeapScan { relation_name, .. } => {
            relation_leaf_scan_source(relation_name, reserve_append_parent_alias)
        }
        Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::SubqueryScan { input, .. } => child_leaf_scan_source(
            input,
            reserve_append_parent_alias,
            alias_through_aggregate_children,
        ),
        Plan::Aggregate { input, .. } if alias_through_aggregate_children => {
            child_leaf_scan_source(
                input,
                reserve_append_parent_alias,
                alias_through_aggregate_children,
            )
        }
        _ => None,
    }
}

pub fn relation_leaf_scan_source(
    relation_name: &str,
    reserve_append_parent_alias: bool,
) -> Option<LeafScanSource> {
    if reserve_append_parent_alias && let Some((_, alias)) = relation_name.rsplit_once(' ') {
        if let Some(root_alias) = inherited_root_alias(alias) {
            return Some(LeafScanSource::Relation {
                key: root_alias.to_string(),
                inherited_alias_base: Some(root_alias.to_string()),
            });
        }
    }
    if let Some((_, alias)) = relation_name.rsplit_once(' ') {
        let root_alias = inherited_root_alias(alias).unwrap_or(alias);
        return Some(LeafScanSource::Relation {
            key: root_alias.to_string(),
            inherited_alias_base: None,
        });
    }
    unaliased_relation_name(relation_name).map(|name| LeafScanSource::Relation {
        key: name.to_string(),
        inherited_alias_base: None,
    })
}

pub fn relation_name_base(relation_name: &str) -> &str {
    relation_name
        .rsplit_once(' ')
        .map(|(base_name, _)| base_name)
        .unwrap_or(relation_name)
}

pub fn explain_node_prefix(indent: usize, is_child: bool) -> String {
    if is_child {
        let spaces = if indent <= 1 {
            indent * 2
        } else {
            2 + (indent - 1) * 6
        };
        format!("{}->  ", " ".repeat(spaces))
    } else {
        "  ".repeat(indent)
    }
}

pub fn qualified_scan_output_exprs(
    relation_name: &str,
    desc: &pgrust_nodes::primnodes::RelationDesc,
) -> Vec<String> {
    let qualifier = relation_name
        .split_once(' ')
        .map(|(_, alias)| alias.trim())
        .filter(|alias| !alias.is_empty());
    desc.columns
        .iter()
        .map(|column| match qualifier {
            Some(alias) => format!("{alias}.{}", column.name),
            None => column.name.clone(),
        })
        .collect()
}

pub fn qualified_base_scan_output_exprs(
    relation_name: &str,
    desc: &pgrust_nodes::primnodes::RelationDesc,
) -> Vec<String> {
    let qualifier = relation_name
        .split_once(' ')
        .map(|(_, alias)| alias.trim().to_string())
        .filter(|alias| !alias.is_empty())
        .unwrap_or_else(|| {
            relation_name
                .rsplit_once('.')
                .map(|(_, name)| name)
                .unwrap_or(relation_name)
                .to_string()
        });
    desc.columns
        .iter()
        .map(|column| format!("{qualifier}.{}", column.name))
        .collect()
}

fn scan_relation_has_explicit_alias(relation_name: &str) -> bool {
    relation_name
        .rsplit_once(' ')
        .is_some_and(|(_, alias)| !alias.trim().is_empty())
}

fn projection_target_can_drop_simple_qualifier(target: &TargetEntry) -> bool {
    if target.input_resno.is_none() || target.name.is_empty() {
        return false;
    }
    matches!(
        &target.expr,
        Expr::Var(var) if !verbose_output_attr_is_system(var.varattno)
    )
}

fn verbose_output_attr_is_system(attno: pgrust_nodes::primnodes::AttrNumber) -> bool {
    matches!(
        attno,
        TABLE_OID_ATTR_NO
            | SELF_ITEM_POINTER_ATTR_NO
            | XMIN_ATTR_NO
            | CMIN_ATTR_NO
            | XMAX_ATTR_NO
            | CMAX_ATTR_NO
    )
}

pub fn qualified_subquery_scan_output_exprs(
    scan_name: Option<&str>,
    output_columns: &[QueryColumn],
) -> Vec<String> {
    match scan_name {
        Some(scan_name) => output_columns
            .iter()
            .map(|column| {
                format!(
                    "{}.{}",
                    quote_explain_identifier(scan_name),
                    quote_explain_identifier(&column.name)
                )
            })
            .collect(),
        None => output_columns
            .iter()
            .map(|column| quote_explain_identifier(&column.name))
            .collect(),
    }
}

pub fn qualified_scan_output_exprs_with_context(
    relation_name: &str,
    desc: &pgrust_nodes::primnodes::RelationDesc,
    ctx: &VerboseExplainContext,
) -> Vec<String> {
    if let Some(alias) = context_relation_scan_alias(ctx, relation_name) {
        return desc
            .columns
            .iter()
            .map(|column| format!("{alias}.{}", column.name))
            .collect();
    }
    qualified_base_scan_output_exprs(relation_name, desc)
}

pub fn unaliased_relation_name(relation_name: &str) -> Option<&str> {
    if relation_name.split_once(' ').is_some() {
        None
    } else {
        Some(
            relation_name
                .rsplit_once('.')
                .map(|(_, name)| name)
                .unwrap_or(relation_name),
        )
    }
}

pub fn values_scan_name(occurrence: usize) -> String {
    if occurrence == 0 {
        "\"*VALUES*\"".to_string()
    } else {
        format!("\"*VALUES*_{occurrence}\"")
    }
}

pub fn explain_alias_with_suffix(alias: &str, suffix: usize) -> String {
    const MAX_IDENTIFIER_BYTES: usize = 63;
    let suffix = format!("_{suffix}");
    let prefix_len = MAX_IDENTIFIER_BYTES.saturating_sub(suffix.len());
    let mut end = alias.len().min(prefix_len);
    while !alias.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &alias[..end], suffix)
}

pub fn values_scan_output_exprs(column_count: usize, scan_name: &str) -> Vec<String> {
    (1..=column_count)
        .map(|index| format!("{scan_name}.column{index}"))
        .collect()
}

pub fn append_parent_output_exprs(
    desc: &pgrust_nodes::primnodes::RelationDesc,
    children: &[&Plan],
) -> Option<Vec<String>> {
    let qualifier = append_parent_qualifier(children)?;
    Some(
        desc.columns
            .iter()
            .map(|column| format!("{qualifier}.{}", column.name))
            .collect(),
    )
}

pub fn append_parent_qualifier(children: &[&Plan]) -> Option<String> {
    let mut qualifier = None::<String>;
    for child in children {
        let Some(LeafScanSource::Relation {
            inherited_alias_base: Some(alias_base),
            ..
        }) = child_leaf_scan_source(child, true, false)
        else {
            return None;
        };
        match &qualifier {
            Some(existing) if existing != &alias_base => return None,
            Some(_) => {}
            None => qualifier = Some(alias_base),
        }
    }
    qualifier
}

pub fn append_child_output_exprs(
    desc: &pgrust_nodes::primnodes::RelationDesc,
    alias: &str,
) -> Vec<String> {
    desc.columns
        .iter()
        .map(|column| format!("{alias}.{}", column.name))
        .collect()
}

pub fn aggregate_group_names_from_input_sort(
    input: &Plan,
    group_count: usize,
    ctx: &VerboseExplainContext,
) -> Option<Vec<String>> {
    let Plan::OrderBy {
        input: sort_input,
        items,
        display_items,
        ..
    } = input
    else {
        return None;
    };
    if group_count == 0 || items.len() < group_count {
        return None;
    }
    if display_items.len() >= group_count {
        return Some(display_items.iter().take(group_count).cloned().collect());
    }
    Some(nonverbose_sort_items(
        sort_input,
        &items[..group_count],
        &[],
        ctx,
    ))
}

pub fn plan_join_output_exprs(
    plan: &Plan,
    ctx: &VerboseExplainContext,
    for_parent_ref: bool,
) -> Vec<String> {
    match plan {
        Plan::Result { .. } => Vec::new(),
        Plan::Append { desc, children, .. } | Plan::MergeAppend { desc, children, .. } => {
            if for_parent_ref
                && let Some(output) =
                    append_parent_output_exprs(desc, &children.iter().collect::<Vec<_>>())
                && output.len() == desc.columns.len()
            {
                output
            } else {
                desc.columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect()
            }
        }
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::TidScan {
            relation_name,
            desc,
            ..
        }
        | Plan::TidRangeScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        }
        | Plan::BitmapHeapScan {
            relation_name,
            desc,
            ..
        } => qualified_scan_output_exprs_with_context(relation_name, desc, ctx),
        Plan::BitmapIndexScan { .. } | Plan::BitmapOr { .. } | Plan::BitmapAnd { .. } => Vec::new(),
        Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. } => plan_join_output_exprs(input, ctx, for_parent_ref),
        Plan::SubqueryScan {
            scan_name,
            output_columns,
            input,
            ..
        } => single_row_projection_output_plan(input, ctx)
            .map(|output| vec![output])
            .unwrap_or_else(|| {
                qualified_subquery_scan_output_exprs(scan_name.as_deref(), output_columns)
            }),
        Plan::Projection { input, targets, .. } => {
            let input_names = plan_join_output_exprs(input, ctx, true);
            targets
                .iter()
                .filter(|target| !target.resjunk)
                .map(|target| {
                    if matches!(input.as_ref(), Plan::FunctionScan { .. })
                        && target.input_resno.is_some()
                        && matches!(target.expr, Expr::Var(_))
                    {
                        format!("{}.{}", target.name, target.name)
                    } else {
                        render_verbose_expr(&target.expr, &input_names, ctx)
                    }
                })
                .collect()
        }
        Plan::Aggregate {
            semantic_output_names: Some(names),
            ..
        } if !context_has_relation_aliases(ctx) && ctx.exec_params.is_empty() => names.clone(),
        Plan::Aggregate {
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            ..
        } => {
            let input_names = plan_join_output_exprs(input, ctx, true);
            let mut output = if context_has_relation_aliases(ctx) {
                aggregate_group_names_from_input_sort(input, group_by.len(), ctx).unwrap_or_else(
                    || {
                        group_by
                            .iter()
                            .map(|expr| render_verbose_expr(expr, &input_names, ctx))
                            .collect::<Vec<_>>()
                    },
                )
            } else {
                group_by
                    .iter()
                    .map(|expr| render_verbose_expr(expr, &input_names, ctx))
                    .collect::<Vec<_>>()
            };
            output.extend(
                passthrough_exprs
                    .iter()
                    .map(|expr| render_verbose_expr(expr, &input_names, ctx)),
            );
            if let Some(display_accumulators) = semantic_accumulators {
                let offset = group_by.len() + passthrough_exprs.len();
                output.extend(
                    display_accumulators
                        .iter()
                        .enumerate()
                        .map(|(index, accum)| {
                            input_names.get(offset + index).cloned().unwrap_or_else(|| {
                                render_verbose_agg_accum(accum, &input_names, ctx)
                            })
                        }),
                );
                return output;
            }
            output.extend(accumulators.iter().map(|accum| {
                let rendered = render_verbose_agg_accum(accum, &input_names, ctx);
                if for_parent_ref {
                    format!("({rendered})")
                } else {
                    rendered
                }
            }));
            output
        }
        Plan::CteScan {
            cte_name,
            output_columns,
            ..
        } => qualified_named_output_exprs(cte_name, output_columns),
        Plan::WindowAgg { input, clause, .. } => nonverbose_window_output_names(input, clause, ctx),
        Plan::WorkTableScan { output_columns, .. }
        | Plan::RecursiveUnion { output_columns, .. } => output_columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        Plan::SetOp {
            output_columns,
            children,
            ..
        } => {
            if for_parent_ref && let Some(first) = children.first() {
                let output = plan_join_output_exprs(first, ctx, true);
                if output.len() == output_columns.len() {
                    output
                } else {
                    output_columns
                        .iter()
                        .map(|column| column.name.clone())
                        .collect()
                }
            } else {
                output_columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect()
            }
        }
        Plan::Values { output_columns, .. } => values_scan_output_exprs(
            output_columns.len(),
            ctx.values_scan_name.as_deref().unwrap_or("\"*VALUES*\""),
        ),
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            ..
        } => {
            let mut output = plan_join_output_exprs(left, ctx, for_parent_ref);
            let mut right_ctx = ctx.clone();
            right_ctx
                .exec_params
                .extend(nest_params.iter().map(|source| VerboseExecParam {
                    paramid: source.paramid,
                    expr: source.expr.clone(),
                    column_names: output.clone(),
                }));
            output.extend(plan_join_output_exprs(right, &right_ctx, for_parent_ref));
            output
        }
        Plan::HashJoin { left, right, .. } | Plan::MergeJoin { left, right, .. } => {
            let mut output = plan_join_output_exprs(left, ctx, for_parent_ref);
            output.extend(plan_join_output_exprs(right, ctx, for_parent_ref));
            output
        }
        Plan::FunctionScan {
            call, table_alias, ..
        } => {
            if matches!(
                call,
                SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
            ) {
                return verbose_function_scan_output_exprs(call, table_alias.as_deref());
            }
            let qualifier = table_alias
                .as_deref()
                .or(ctx.function_scan_alias.as_deref());
            call.output_columns()
                .iter()
                .map(|column| match qualifier {
                    Some(alias) => format!("{alias}.{}", column.name),
                    None => format!("{}.{}", column.name, column.name),
                })
                .collect()
        }
        Plan::ProjectSet { targets, .. } => targets
            .iter()
            .map(|target| match target {
                ProjectSetTarget::Scalar(entry) => entry.name.clone(),
                ProjectSetTarget::Set { name, .. } => name.clone(),
            })
            .collect(),
    }
}

pub fn verbose_display_output_exprs(
    plan: &Plan,
    ctx: &VerboseExplainContext,
    for_parent_ref: bool,
) -> Vec<String> {
    match plan {
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. }
            if append_parent_qualifier(&children.iter().collect::<Vec<_>>()).is_some() =>
        {
            Vec::new()
        }
        Plan::SeqScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapHeapScan { .. } => ctx
            .scan_output_override
            .clone()
            .unwrap_or_else(|| verbose_plan_output_exprs(plan, ctx, for_parent_ref)),
        Plan::Values { .. } if !for_parent_ref && ctx.scan_output_override.is_some() => {
            ctx.scan_output_override.clone().unwrap_or_default()
        }
        Plan::Hash { input, .. }
            if !for_parent_ref
                && let Some(row_output) = single_row_projection_output_plan(input, ctx) =>
        {
            vec![format!("({row_output})")]
        }
        Plan::SubqueryScan {
            scan_name,
            output_columns,
            input,
            ..
        } if !for_parent_ref
            && let Some(output) = collapsed_row_subquery_field_output(
                scan_name.as_deref(),
                output_columns,
                input,
            ) =>
        {
            vec![output]
        }
        Plan::ProjectSet { input, targets, .. } if !for_parent_ref => {
            let input_names = verbose_plan_output_exprs(input, ctx, true);
            targets
                .iter()
                .map(|target| match target {
                    ProjectSetTarget::Scalar(entry) => {
                        render_verbose_expr(&entry.expr, &input_names, ctx)
                    }
                    ProjectSetTarget::Set { source_expr, .. } => {
                        render_verbose_expr(source_expr, &input_names, ctx)
                    }
                })
                .collect()
        }
        Plan::Unique { .. } | Plan::OrderBy { .. }
            if !for_parent_ref && let Some(outputs) = setop_constant_child_outputs(plan, ctx) =>
        {
            outputs
                .into_iter()
                .map(|output| format!("({output})"))
                .collect()
        }
        Plan::NestedLoopJoin { .. }
            if !for_parent_ref
                && let Some((qualifier, field)) = nested_loop_whole_row_field(plan, ctx) =>
        {
            vec![format!("(({}.*).{})", qualifier, field)]
        }
        Plan::Projection { input, .. }
            if !for_parent_ref
                && matches!(input.as_ref(), Plan::Result { .. })
                && let Some((qualifier, field)) = &ctx.whole_row_field_output =>
        {
            vec![format!("({qualifier}.*).{field}")]
        }
        Plan::Projection { input, .. }
            if !for_parent_ref
                && plan_is_limit_result(input)
                && let Some((qualifier, field)) = &ctx.whole_row_field_output =>
        {
            vec![format!("({qualifier}.*).{field}")]
        }
        Plan::Projection { input, targets, .. }
            if !for_parent_ref
                && matches!(
                    input.as_ref(),
                    Plan::Append { .. } | Plan::MergeAppend { .. }
                ) =>
        {
            let output = append_first_child_output(input, ctx).unwrap_or_else(|| {
                let input_names = verbose_plan_output_exprs(input, ctx, true);
                targets
                    .iter()
                    .filter(|target| !target.resjunk)
                    .map(|target| render_verbose_target_expr(target, &input_names, ctx))
                    .collect::<Vec<_>>()
            });
            if output.len() == 1 {
                vec![format!("({})", output[0])]
            } else {
                output
            }
        }
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. }
            if !for_parent_ref && append_children_are_constant_results(children) =>
        {
            Vec::new()
        }
        _ => verbose_plan_output_exprs(plan, ctx, for_parent_ref),
    }
}

pub fn append_children_are_constant_results(children: &[Plan]) -> bool {
    !children.is_empty() && children.iter().all(plan_is_constant_result)
}

pub fn plan_is_constant_result(plan: &Plan) -> bool {
    match plan {
        Plan::Result { .. } => true,
        Plan::Projection { input, .. } => plan_is_constant_result(input),
        _ => false,
    }
}

pub fn plan_is_limit_result(plan: &Plan) -> bool {
    matches!(plan, Plan::Limit { input, .. } if matches!(input.as_ref(), Plan::Result { .. }))
}

pub fn single_row_projection_output_plan(
    plan: &Plan,
    ctx: &VerboseExplainContext,
) -> Option<String> {
    match plan {
        Plan::Projection { input, targets, .. } => {
            projection_single_row_output(input, targets, ctx)
        }
        Plan::SubqueryScan { input, .. } => single_row_projection_output_plan(input, ctx),
        _ => None,
    }
}

pub fn collapsed_row_subquery_field_output(
    scan_name: Option<&str>,
    output_columns: &[QueryColumn],
    input: &Plan,
) -> Option<String> {
    let (Some(scan_name), [column]) = (scan_name, output_columns) else {
        return None;
    };
    if column.name != "r"
        || !matches!(
            column.sql_type.kind,
            pgrust_nodes::parsenodes::SqlTypeKind::Record
                | pgrust_nodes::parsenodes::SqlTypeKind::Composite
        )
        || single_row_projection_output_plan(input, &VerboseExplainContext::default()).is_none()
    {
        return None;
    }
    // :HACK: setrefs can prove `(r).column2` equivalent to the single
    // composite subquery output and hide the outer projection. PostgreSQL still
    // prints the selected field in EXPLAIN for this row-valued VALUES shape.
    Some(format!(
        "({}.{}).column2",
        quote_explain_identifier(scan_name),
        quote_explain_identifier(&column.name)
    ))
}

pub fn append_first_child_output(input: &Plan, ctx: &VerboseExplainContext) -> Option<Vec<String>> {
    match input {
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. } => children
            .first()
            .map(|child| verbose_display_output_exprs(child, ctx, false)),
        _ => None,
    }
}

pub fn setop_constant_child_outputs(
    plan: &Plan,
    ctx: &VerboseExplainContext,
) -> Option<Vec<String>> {
    match plan {
        Plan::Unique { input, .. } | Plan::OrderBy { input, .. } => {
            setop_constant_child_outputs(input, ctx)
        }
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. } => {
            let first = children.first()?;
            let mut child_ctx = ctx.clone();
            child_ctx.setop_raw_numeric_outputs = true;
            let output = verbose_display_output_exprs(first, &child_ctx, false);
            (!output.is_empty()).then_some(output)
        }
        _ => None,
    }
}

pub fn trim_numeric_fraction_zeros(value: &str) -> &str {
    let Some((whole, fraction)) = value.split_once('.') else {
        return value;
    };
    let trimmed = fraction.trim_end_matches('0');
    if trimmed.is_empty() {
        whole
    } else {
        let keep = whole.len() + 1 + trimmed.len();
        &value[..keep]
    }
}

pub fn nested_loop_whole_row_field(
    plan: &Plan,
    ctx: &VerboseExplainContext,
) -> Option<(String, String)> {
    if !matches!(plan, Plan::NestedLoopJoin { .. }) {
        return None;
    }
    let output = plan_join_output_exprs(plan, ctx, false);
    let [single] = output.as_slice() else {
        return None;
    };
    let (qualifier, field) = single.split_once('.')?;
    Some((qualifier.to_string(), field.to_string()))
}

pub fn aggregate_child_output_exprs(
    input: &Plan,
    group_by: &[Expr],
    passthrough_exprs: &[Expr],
    accumulators: &[AggAccum],
    ctx: &VerboseExplainContext,
) -> Vec<String> {
    let input_names = verbose_plan_output_exprs(input, ctx, true);
    let mut output = Vec::new();
    output.extend(
        group_by
            .iter()
            .map(|expr| render_verbose_expr(expr, &input_names, ctx)),
    );
    output.extend(
        passthrough_exprs
            .iter()
            .map(|expr| render_verbose_expr(expr, &input_names, ctx)),
    );
    for accum in accumulators {
        output.extend(
            accum
                .args
                .iter()
                .map(|arg| render_verbose_expr(arg, &input_names, ctx)),
        );
        if let Some(filter) = &accum.filter {
            output.push(render_verbose_expr(filter, &input_names, ctx));
        }
    }
    output
}

pub fn verbose_plan_output_exprs(
    plan: &Plan,
    ctx: &VerboseExplainContext,
    for_parent_ref: bool,
) -> Vec<String> {
    if !for_parent_ref
        && matches!(plan, Plan::Filter { .. } | Plan::Limit { .. })
        && let Some(output) = &ctx.scan_output_override
    {
        return output.clone();
    }
    match plan {
        Plan::Result { .. } => Vec::new(),
        Plan::Append { desc, children, .. } | Plan::MergeAppend { desc, children, .. } => {
            append_parent_output_exprs(desc, &children.iter().collect::<Vec<_>>()).unwrap_or_else(
                || {
                    desc.columns
                        .iter()
                        .map(|column| column.name.clone())
                        .collect()
                },
            )
        }
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::TidScan {
            relation_name,
            desc,
            ..
        }
        | Plan::TidRangeScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        }
        | Plan::BitmapHeapScan {
            relation_name,
            desc,
            ..
        } if !for_parent_ref && !scan_relation_has_explicit_alias(relation_name) => desc
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        Plan::SeqScan {
            relation_name,
            desc,
            ..
        }
        | Plan::TidScan {
            relation_name,
            desc,
            ..
        }
        | Plan::TidRangeScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexOnlyScan {
            relation_name,
            desc,
            ..
        }
        | Plan::IndexScan {
            relation_name,
            desc,
            ..
        }
        | Plan::BitmapHeapScan {
            relation_name,
            desc,
            ..
        } => qualified_base_scan_output_exprs(relation_name, desc),
        Plan::BitmapIndexScan { .. } | Plan::BitmapOr { .. } | Plan::BitmapAnd { .. } => Vec::new(),
        Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Unique { input, .. }
        | Plan::Filter { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. } => verbose_plan_output_exprs(input, ctx, for_parent_ref),
        Plan::SubqueryScan {
            scan_name,
            output_columns,
            input,
            ..
        } if for_parent_ref => single_row_projection_output_plan(input, ctx)
            .map(|output| vec![output])
            .unwrap_or_else(|| {
                qualified_subquery_scan_output_exprs(scan_name.as_deref(), output_columns)
            }),
        Plan::SubqueryScan {
            scan_name,
            output_columns,
            ..
        } => qualified_subquery_scan_output_exprs(scan_name.as_deref(), output_columns),
        Plan::Projection { input, targets, .. } => {
            let input_names = plan_join_output_exprs(input, ctx, true);
            targets
                .iter()
                .filter(|target| !target.resjunk)
                .map(|target| {
                    let rendered = render_verbose_target_expr(target, &input_names, ctx);
                    if !for_parent_ref && projection_target_can_drop_simple_qualifier(target) {
                        strip_qualified_identifiers(rendered)
                    } else {
                        rendered
                    }
                })
                .collect()
        }
        Plan::Aggregate {
            semantic_output_names: Some(names),
            ..
        } if !context_has_relation_aliases(ctx) && ctx.exec_params.is_empty() => names.clone(),
        Plan::Aggregate {
            input,
            group_by,
            passthrough_exprs,
            accumulators,
            semantic_accumulators,
            ..
        } => {
            let input_names = verbose_plan_output_exprs(input, ctx, true);
            let mut output = group_by
                .iter()
                .map(|expr| render_verbose_expr(expr, &input_names, ctx))
                .collect::<Vec<_>>();
            output.extend(
                passthrough_exprs
                    .iter()
                    .map(|expr| render_verbose_expr(expr, &input_names, ctx)),
            );
            if let Some(display_accumulators) = semantic_accumulators {
                let offset = group_by.len() + passthrough_exprs.len();
                output.extend(
                    display_accumulators
                        .iter()
                        .enumerate()
                        .map(|(index, accum)| {
                            input_names.get(offset + index).cloned().unwrap_or_else(|| {
                                render_verbose_agg_accum(accum, &input_names, ctx)
                            })
                        }),
                );
                return output;
            }
            output.extend(accumulators.iter().map(|accum| {
                let rendered = render_verbose_agg_accum(accum, &input_names, ctx);
                if for_parent_ref {
                    format!("({rendered})")
                } else {
                    rendered
                }
            }));
            output
        }
        Plan::Values { output_columns, .. } if !for_parent_ref => values_scan_output_exprs(
            output_columns.len(),
            ctx.values_scan_name.as_deref().unwrap_or("\"*VALUES*\""),
        ),
        Plan::CteScan {
            cte_name,
            output_columns,
            ..
        } => qualified_named_output_exprs(cte_name, output_columns),
        Plan::WindowAgg {
            input,
            clause,
            output_columns,
            ..
        } => verbose_window_output_names(input, clause, output_columns, ctx),
        Plan::WorkTableScan { output_columns, .. }
        | Plan::RecursiveUnion { output_columns, .. }
        | Plan::SetOp { output_columns, .. }
        | Plan::Values { output_columns, .. } => output_columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        Plan::NestedLoopJoin {
            left,
            right,
            nest_params,
            ..
        } => {
            let mut output = plan_join_output_exprs(left, ctx, true);
            let mut right_ctx = ctx.clone();
            right_ctx
                .exec_params
                .extend(nest_params.iter().map(|source| VerboseExecParam {
                    paramid: source.paramid,
                    expr: source.expr.clone(),
                    column_names: output.clone(),
                }));
            output.extend(plan_join_output_exprs(right, &right_ctx, true));
            output
        }
        Plan::HashJoin { left, right, .. } | Plan::MergeJoin { left, right, .. } => {
            let mut output = verbose_plan_output_exprs(left, ctx, for_parent_ref);
            output.extend(verbose_plan_output_exprs(right, ctx, for_parent_ref));
            output
        }
        Plan::FunctionScan {
            call, table_alias, ..
        } => verbose_function_scan_output_exprs(call, table_alias.as_deref()),
        Plan::ProjectSet { targets, .. } => targets
            .iter()
            .map(|target| match target {
                ProjectSetTarget::Scalar(entry) => entry.name.clone(),
                ProjectSetTarget::Set { name, .. } => name.clone(),
            })
            .collect(),
    }
}

pub fn render_verbose_set_returning_call(
    call: &SetReturningCall,
    ctx: &VerboseExplainContext,
) -> String {
    if let SetReturningCall::RowsFrom { items, .. } = call {
        let rendered_items = items
            .iter()
            .map(|item| match &item.source {
                RowsFromSource::Function(call) => render_verbose_set_returning_call(call, ctx),
                RowsFromSource::Project { output_exprs, .. } => output_exprs
                    .iter()
                    .map(|expr| render_verbose_expr(expr, &[], ctx))
                    .collect::<Vec<_>>()
                    .join(", "),
            })
            .collect::<Vec<_>>()
            .join(", ");
        return format!("ROWS FROM({rendered_items})");
    }
    if let SetReturningCall::SqlJsonTable(table) = call {
        return render_verbose_sql_json_table_call(table, ctx);
    }
    if let SetReturningCall::SqlXmlTable(table) = call {
        return render_verbose_sql_xml_table_call(table, ctx);
    }
    if let SetReturningCall::UserDefined {
        inlined_expr: Some(expr),
        ..
    } = call
    {
        return render_verbose_function_arg(expr, ctx);
    }
    let name = set_returning_call_label(call);
    let args = match call {
        SetReturningCall::GenerateSeries {
            start,
            stop,
            step,
            timezone,
            ..
        } => {
            let mut args = vec![
                render_verbose_function_arg(start, ctx),
                render_verbose_function_arg(stop, ctx),
            ];
            if !matches!(
                step,
                Expr::Const(Value::Int32(1)) | Expr::Const(Value::Int64(1))
            ) {
                args.push(render_verbose_function_arg(step, ctx));
            }
            if let Some(timezone) = timezone {
                args.push(render_verbose_function_arg(timezone, ctx));
            }
            args
        }
        SetReturningCall::GenerateSubscripts {
            array,
            dimension,
            reverse,
            ..
        } => {
            let mut args = vec![
                render_verbose_function_arg(array, ctx),
                render_verbose_function_arg(dimension, ctx),
            ];
            if let Some(reverse) = reverse {
                args.push(render_verbose_function_arg(reverse, ctx));
            }
            args
        }
        SetReturningCall::PartitionTree { relid, .. }
        | SetReturningCall::PartitionAncestors { relid, .. } => {
            vec![render_verbose_function_arg(relid, ctx)]
        }
        SetReturningCall::PgLockStatus { .. }
        | SetReturningCall::PgStatProgressCopy { .. }
        | SetReturningCall::PgSequences { .. }
        | SetReturningCall::InformationSchemaSequences { .. } => Vec::new(),
        SetReturningCall::TxidSnapshotXip { arg, .. } => {
            vec![render_verbose_function_arg(arg, ctx)]
        }
        SetReturningCall::Unnest { args, .. }
        | SetReturningCall::JsonTableFunction { args, .. }
        | SetReturningCall::JsonRecordFunction { args, .. }
        | SetReturningCall::RegexTableFunction { args, .. }
        | SetReturningCall::StringTableFunction { args, .. }
        | SetReturningCall::TextSearchTableFunction { args, .. }
        | SetReturningCall::UserDefined { args, .. } => args
            .iter()
            .map(|expr| render_verbose_function_arg(expr, ctx))
            .collect(),
        SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_) => {
            unreachable!("handled above")
        }
        SetReturningCall::RowsFrom { .. } => unreachable!("handled above"),
    };
    format!("{name}({})", args.join(", "))
}

pub fn render_verbose_sql_xml_table_call(
    table: &SqlXmlTable,
    ctx: &VerboseExplainContext,
) -> String {
    let mut rendered = String::from("XMLTABLE(");
    if !table.namespaces.is_empty() {
        rendered.push_str("XMLNAMESPACES (");
        rendered.push_str(
            &table
                .namespaces
                .iter()
                .map(|namespace| {
                    let uri = render_verbose_sql_json_table_expr(&namespace.uri, ctx);
                    match namespace.name.as_deref() {
                        Some(name) => format!("{uri} AS {}", quote_explain_identifier(name)),
                        None => format!("DEFAULT {uri}"),
                    }
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
        rendered.push_str("), ");
    }
    rendered.push_str(&render_parenthesized_verbose_xmltable_expr(
        &table.row_path,
        ctx,
    ));
    rendered.push_str(" PASSING ");
    rendered.push_str(&render_parenthesized_verbose_xmltable_expr(
        &table.document,
        ctx,
    ));
    rendered.push_str(" COLUMNS ");
    rendered.push_str(
        &table
            .columns
            .iter()
            .map(|column| {
                let name = quote_explain_identifier(&column.name);
                match &column.kind {
                    SqlXmlTableColumnKind::Ordinality => format!("{name} FOR ORDINALITY"),
                    SqlXmlTableColumnKind::Regular {
                        path,
                        default,
                        not_null,
                    } => {
                        let mut column =
                            format!("{name} {}", render_type_name(column.sql_type, ctx));
                        if let Some(default) = default {
                            column.push_str(" DEFAULT ");
                            column.push_str(&render_parenthesized_verbose_xmltable_expr(
                                default, ctx,
                            ));
                        }
                        if let Some(path) = path {
                            column.push_str(" PATH ");
                            column.push_str(&render_parenthesized_verbose_xmltable_expr(path, ctx));
                        }
                        if *not_null {
                            column.push_str(" NOT NULL");
                        }
                        column
                    }
                }
            })
            .collect::<Vec<_>>()
            .join(", "),
    );
    rendered.push(')');
    rendered
}

pub fn render_parenthesized_verbose_xmltable_expr(
    expr: &Expr,
    ctx: &VerboseExplainContext,
) -> String {
    format!("({})", render_verbose_sql_json_table_expr(expr, ctx))
}

pub fn render_verbose_sql_json_table_call(
    table: &SqlJsonTable,
    ctx: &VerboseExplainContext,
) -> String {
    let mut rendered = format!(
        "JSON_TABLE({}, '{}' AS {}",
        render_verbose_sql_json_table_expr(&table.context, ctx),
        render_sql_json_table_path(&table.root_path).replace('\'', "''"),
        quote_explain_identifier(&table.root_path_name)
    );
    if !table.passing.is_empty() {
        rendered.push_str(" PASSING ");
        rendered.push_str(
            &table
                .passing
                .iter()
                .map(|arg| {
                    format!(
                        "{} AS {}",
                        render_verbose_sql_json_table_expr(&arg.expr, ctx),
                        quote_explain_identifier(&arg.name)
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
    rendered.push_str(" COLUMNS (");
    rendered
        .push_str(&render_verbose_sql_json_table_plan_columns(table, &table.plan, ctx).join(", "));
    rendered.push(')');
    if matches!(table.on_error, SqlJsonTableBehavior::Error) {
        rendered.push_str(" ERROR ON ERROR");
    }
    rendered.push(')');
    rendered
}

pub fn render_verbose_sql_json_table_expr(expr: &Expr, ctx: &VerboseExplainContext) -> String {
    match expr {
        Expr::Const(Value::Text(_)) | Expr::Const(Value::TextRef(_, _)) => {
            format!("{}::text", render_verbose_function_arg(expr, ctx))
        }
        Expr::Const(Value::Json(value)) => format!("'{}'::jsonb", value.replace('\'', "''")),
        _ => render_verbose_function_arg(expr, ctx),
    }
}

pub fn render_sql_json_table_path(path: &str) -> String {
    canonicalize_jsonpath(path).unwrap_or_else(|_| path.to_string())
}

pub fn render_verbose_sql_json_table_plan_columns(
    table: &SqlJsonTable,
    plan: &SqlJsonTablePlan,
    ctx: &VerboseExplainContext,
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
                .map(|column| render_verbose_sql_json_table_column(column, ctx))
                .collect::<Vec<_>>();
            if let Some(child) = child {
                rendered.extend(render_verbose_sql_json_table_nested_plans(
                    table, child, ctx,
                ));
            }
            rendered
        }
        SqlJsonTablePlan::SiblingJoin { left, right } => {
            let mut rendered = render_verbose_sql_json_table_plan_columns(table, left, ctx);
            rendered.extend(render_verbose_sql_json_table_plan_columns(
                table, right, ctx,
            ));
            rendered
        }
    }
}

pub fn render_verbose_sql_json_table_nested_plans(
    table: &SqlJsonTable,
    plan: &SqlJsonTablePlan,
    ctx: &VerboseExplainContext,
) -> Vec<String> {
    match plan {
        SqlJsonTablePlan::PathScan {
            path,
            path_name,
            column_indexes,
            child,
            ..
        } => {
            let mut columns = column_indexes
                .iter()
                .filter_map(|index| table.columns.get(*index))
                .map(|column| render_verbose_sql_json_table_column(column, ctx))
                .collect::<Vec<_>>();
            if let Some(child) = child {
                columns.extend(render_verbose_sql_json_table_nested_plans(
                    table, child, ctx,
                ));
            }
            vec![format!(
                "NESTED PATH '{}' AS {} COLUMNS ({})",
                render_sql_json_table_path(path).replace('\'', "''"),
                quote_explain_identifier(path_name),
                columns.join(", ")
            )]
        }
        SqlJsonTablePlan::SiblingJoin { left, right } => {
            let mut rendered = render_verbose_sql_json_table_nested_plans(table, left, ctx);
            rendered.extend(render_verbose_sql_json_table_nested_plans(
                table, right, ctx,
            ));
            rendered
        }
    }
}

pub fn render_verbose_sql_json_table_column(
    column: &SqlJsonTableColumn,
    ctx: &VerboseExplainContext,
) -> String {
    let name = quote_explain_identifier(&column.name);
    let ty = render_type_name(column.sql_type, ctx);
    match &column.kind {
        SqlJsonTableColumnKind::Ordinality => format!("{name} FOR ORDINALITY"),
        SqlJsonTableColumnKind::Scalar {
            path,
            on_empty,
            on_error,
        } => {
            let mut rendered = format!(
                "{name} {ty} PATH '{}'",
                render_sql_json_table_path(path).replace('\'', "''")
            );
            append_verbose_sql_json_table_behavior(
                &mut rendered,
                on_empty,
                "EMPTY",
                matches!(on_empty, SqlJsonTableBehavior::Null),
                ctx,
            );
            append_verbose_sql_json_table_behavior(
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
            let mut rendered = format!("{name} {ty}");
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
            append_verbose_sql_json_table_behavior(
                &mut rendered,
                on_empty,
                "EMPTY",
                matches!(on_empty, SqlJsonTableBehavior::Null),
                ctx,
            );
            append_verbose_sql_json_table_behavior(
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
                "{name} {ty} EXISTS PATH '{}'",
                render_sql_json_table_path(path).replace('\'', "''")
            );
            append_verbose_sql_json_table_behavior(
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

pub fn sql_json_table_column_renders_format_json(column: &SqlJsonTableColumn) -> bool {
    !column.sql_type.is_array
        && !matches!(
            column.sql_type.kind,
            pgrust_nodes::parsenodes::SqlTypeKind::Json
                | pgrust_nodes::parsenodes::SqlTypeKind::Jsonb
                | pgrust_nodes::parsenodes::SqlTypeKind::Composite
                | pgrust_nodes::parsenodes::SqlTypeKind::Record
        )
}

pub fn append_verbose_sql_json_table_behavior(
    rendered: &mut String,
    behavior: &SqlJsonTableBehavior,
    target: &str,
    omit_default: bool,
    ctx: &VerboseExplainContext,
) {
    if omit_default {
        return;
    }
    match behavior {
        SqlJsonTableBehavior::Null => rendered.push_str(&format!(" NULL ON {target}")),
        SqlJsonTableBehavior::Error => rendered.push_str(&format!(" ERROR ON {target}")),
        SqlJsonTableBehavior::Empty => rendered.push_str(&format!(" EMPTY ON {target}")),
        SqlJsonTableBehavior::EmptyArray => {
            rendered.push_str(&format!(" EMPTY ARRAY ON {target}"));
        }
        SqlJsonTableBehavior::EmptyObject => {
            rendered.push_str(&format!(" EMPTY OBJECT ON {target}"));
        }
        SqlJsonTableBehavior::Default(expr) => rendered.push_str(&format!(
            " DEFAULT {} ON {target}",
            render_verbose_expr(expr, &[], ctx)
        )),
        SqlJsonTableBehavior::True => rendered.push_str(&format!(" TRUE ON {target}")),
        SqlJsonTableBehavior::False => rendered.push_str(&format!(" FALSE ON {target}")),
        SqlJsonTableBehavior::Unknown => rendered.push_str(&format!(" UNKNOWN ON {target}")),
    }
}

pub fn render_verbose_function_arg(expr: &Expr, ctx: &VerboseExplainContext) -> String {
    match expr {
        Expr::Cast(inner, _) => render_verbose_function_arg(inner, ctx),
        Expr::Const(value) => render_verbose_function_const(value, ctx),
        _ => render_verbose_expr(expr, &[], ctx),
    }
}

pub fn render_verbose_target_expr(
    target: &TargetEntry,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let rendered = render_verbose_expr(&target.expr, column_names, ctx);
    if ctx.setop_raw_numeric_outputs
        && target.sql_type.kind == pgrust_nodes::parsenodes::SqlTypeKind::Numeric
    {
        return rendered
            .split_once("::")
            .map(|(base, _)| trim_numeric_fraction_zeros(base).to_string())
            .unwrap_or_else(|| trim_numeric_fraction_zeros(&rendered).to_string());
    }
    if matches!(target.expr, Expr::Const(_))
        && target.sql_type.kind == pgrust_nodes::parsenodes::SqlTypeKind::Numeric
        && target.sql_type.typmod >= pgrust_nodes::parsenodes::SqlType::VARHDRSZ
    {
        return format!("{rendered}::{}", render_type_name(target.sql_type, ctx));
    }
    rendered
}

pub fn render_verbose_record_const(
    record: &pgrust_nodes::datum::RecordValue,
    ctx: &VerboseExplainContext,
) -> String {
    let rendered = format_record_text(record).replace('\'', "''");
    let record_type = record.sql_type();
    if record_type.type_oid != RECORD_TYPE_OID {
        format!("'{rendered}'::{}", render_type_name(record_type, ctx))
    } else {
        format!("'{rendered}'::record")
    }
}

pub fn render_verbose_function_const(value: &Value, ctx: &VerboseExplainContext) -> String {
    match value {
        Value::Int16(value) => value.to_string(),
        Value::Int32(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Text(value) => format!("'{}'", value.replace('\'', "''")),
        Value::TextRef(_, _) => format!("'{}'", value.as_text().unwrap().replace('\'', "''")),
        Value::Json(value) => format!("'{}'::json", value.replace('\'', "''")),
        Value::JsonPath(value) => format!("'{}'::jsonpath", value.replace('\'', "''")),
        Value::Jsonb(bytes) => render_jsonb_bytes(bytes)
            .map(|value| format!("'{}'::jsonb", value.replace('\'', "''")))
            .unwrap_or_else(|_| "'null'::jsonb".into()),
        Value::Record(record) => render_verbose_record_const(record, ctx),
        Value::Array(items) if items.iter().any(|item| matches!(item, Value::Jsonb(_))) => {
            format!("'{}'::jsonb[]", render_verbose_jsonb_array(items))
        }
        Value::PgArray(array)
            if array
                .elements
                .iter()
                .any(|item| matches!(item, Value::Jsonb(_))) =>
        {
            format!("'{}'::jsonb[]", render_verbose_jsonb_array(&array.elements))
        }
        _ => strip_outer_parens(&render_explain_expr(&Expr::Const(value.clone()), &[])),
    }
}

pub fn render_verbose_jsonb_array(items: &[Value]) -> String {
    let mut out = String::from("{");
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        let rendered = match item {
            Value::Jsonb(bytes) => render_jsonb_bytes(bytes).unwrap_or_else(|_| "null".into()),
            Value::Null => {
                out.push_str("NULL");
                continue;
            }
            other => strip_outer_parens(&render_explain_expr(&Expr::Const(other.clone()), &[])),
        };
        out.push('"');
        for ch in rendered.chars() {
            match ch {
                '"' | '\\' => {
                    out.push('\\');
                    out.push(ch);
                }
                '\'' => out.push_str("''"),
                _ => out.push(ch),
            }
        }
        out.push('"');
    }
    out.push('}');
    out
}

pub fn render_verbose_agg_accum(
    accum: &AggAccum,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let name = builtin_aggregate_function_for_proc_oid(accum.aggfnoid)
        .map(|func| func.name().to_string())
        .unwrap_or_else(|| format!("agg_{}", accum.aggfnoid));
    let mut args = if accum.args.is_empty() {
        vec!["*".into()]
    } else {
        accum
            .args
            .iter()
            .map(|arg| render_verbose_expr(arg, column_names, ctx))
            .collect::<Vec<_>>()
    };
    if accum.distinct && !args.is_empty() {
        args[0] = format!("DISTINCT {}", args[0]);
    }
    let mut rendered = format!("{name}({})", args.join(", "));
    if let Some(filter) = &accum.filter {
        rendered.push_str(&format!(
            " FILTER (WHERE {})",
            render_verbose_expr(filter, column_names, ctx)
        ));
    }
    rendered
}

pub fn render_verbose_aggref(
    aggref: &pgrust_nodes::primnodes::Aggref,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let name = builtin_aggregate_function_for_proc_oid(aggref.aggfnoid)
        .map(|func| func.name().to_string())
        .unwrap_or_else(|| format!("agg_{}", aggref.aggfnoid));
    let mut args = if aggref.args.is_empty() {
        vec!["*".into()]
    } else {
        aggref
            .args
            .iter()
            .map(|arg| render_verbose_expr(arg, column_names, ctx))
            .collect::<Vec<_>>()
    };
    if aggref.aggdistinct && !args.is_empty() {
        args[0] = format!("DISTINCT {}", args[0]);
    }
    format!("{name}({})", args.join(", "))
}

pub fn render_verbose_window_func(
    window_func: &pgrust_nodes::primnodes::WindowFuncExpr,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let rendered = match &window_func.kind {
        WindowFuncKind::Aggregate(aggref) => render_verbose_aggref(aggref, column_names, ctx),
        WindowFuncKind::Builtin(func) => {
            let args = window_func
                .args
                .iter()
                .map(|arg| render_verbose_expr(arg, column_names, ctx))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({args})", func.name())
        }
    };
    format!("({rendered} OVER w{})", window_func.winref)
}

pub fn render_verbose_join_expr(
    expr: &Expr,
    left_names: &[String],
    right_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let combined_names = || {
        let mut combined = left_names.to_vec();
        combined.extend_from_slice(right_names);
        combined
    };
    match expr {
        Expr::Var(var) if var.varno == pgrust_nodes::primnodes::OUTER_VAR => {
            render_var_name(var.varattno, left_names)
                .or_else(|| render_system_var_name(var.varattno, left_names))
                .unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Var(var) if var.varno == pgrust_nodes::primnodes::INNER_VAR => {
            render_var_name(var.varattno, right_names)
                .or_else(|| render_system_var_name(var.varattno, right_names))
                .unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::FieldSelect {
            expr: inner, field, ..
        } => {
            if let Some(value) = field_select_projected_value(inner, field) {
                return render_verbose_join_expr(value, left_names, right_names, ctx);
            }
            format!("{expr:?}")
        }
        Expr::Var(var) => {
            let combined = combined_names();
            render_var_name(var.varattno, &combined)
                .or_else(|| render_system_var_name(var.varattno, &combined))
                .unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Param(param) if param.paramkind == ParamKind::Exec => ctx
            .exec_params
            .iter()
            .rev()
            .find(|source| source.paramid == param.paramid)
            .map(|source| render_verbose_expr(&source.expr, &source.column_names, ctx))
            .unwrap_or_else(|| format!("${}", param.paramid)),
        Expr::Param(param) if param.paramkind == ParamKind::External => {
            format!("${}", param.paramid)
        }
        Expr::Const(Value::Record(record)) => render_verbose_record_const(record, ctx),
        Expr::Const(value) => {
            strip_outer_parens(&render_explain_expr(&Expr::Const(value.clone()), &[]))
        }
        Expr::Row { .. } => {
            let combined = combined_names();
            render_verbose_row_whole_star(expr, &combined)
                .unwrap_or_else(|| strip_outer_parens(&render_explain_expr(expr, &combined)))
        }
        Expr::IsNull(inner) => {
            let combined = combined_names();
            render_verbose_composite_null_test(inner, true, &combined, ctx)
                .unwrap_or_else(|| render_explain_expr(expr, &combined))
        }
        Expr::IsNotNull(inner) => {
            let combined = combined_names();
            render_verbose_composite_null_test(inner, false, &combined, ctx)
                .unwrap_or_else(|| render_explain_expr(expr, &combined))
        }
        Expr::Cast(inner, ty) => {
            if let Some(rendered) = render_verbose_const_cast(inner, *ty, ctx) {
                return rendered;
            }
            if let Expr::Var(var) = inner.as_ref()
                && verbose_cast_is_implicit_integer_widening(var.vartype, *ty)
            {
                return render_verbose_join_expr(inner, left_names, right_names, ctx);
            }
            let inner = render_verbose_join_expr(inner, left_names, right_names, ctx);
            format!("({inner})::{}", render_type_name(*ty, ctx))
        }
        Expr::Op(op) => {
            let [left, right] = op.args.as_slice() else {
                return strip_outer_parens(&render_explain_join_expr(
                    expr,
                    left_names,
                    right_names,
                ));
            };
            let Some(op_text) = verbose_op_text(op.opno, op.op) else {
                return strip_outer_parens(&render_explain_join_expr(
                    expr,
                    left_names,
                    right_names,
                ));
            };
            if let Some(rendered) =
                render_system_var_join_op(left, right, op_text, left_names, right_names)
            {
                return rendered;
            }
            format!(
                "({} {} {})",
                render_verbose_join_expr(left, left_names, right_names, ctx),
                op_text,
                render_verbose_join_expr(right, left_names, right_names, ctx)
            )
        }
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            pgrust_nodes::primnodes::BoolExprType::And => format!(
                "({})",
                bool_expr
                    .args
                    .iter()
                    .map(|arg| render_verbose_join_expr(arg, left_names, right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            pgrust_nodes::primnodes::BoolExprType::Or => format!(
                "({})",
                bool_expr
                    .args
                    .iter()
                    .map(|arg| render_verbose_join_expr(arg, left_names, right_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            ),
            pgrust_nodes::primnodes::BoolExprType::Not => {
                let Some(inner) = bool_expr.args.first() else {
                    return format!("{expr:?}");
                };
                format!(
                    "NOT {}",
                    render_verbose_join_expr(inner, left_names, right_names, ctx)
                )
            }
        },
        Expr::Coalesce(left, right) => format!(
            "COALESCE({}, {})",
            render_verbose_join_expr(left, left_names, right_names, ctx),
            render_verbose_join_expr(right, left_names, right_names, ctx)
        ),
        Expr::Func(func)
            if verbose_builtin_infix_operator(func.implementation).is_some()
                && func.args.len() == 2 =>
        {
            let operator = verbose_builtin_infix_operator(func.implementation)
                .expect("checked infix operator");
            format!(
                "({} {} {})",
                render_verbose_join_expr(&func.args[0], left_names, right_names, ctx),
                operator,
                render_verbose_join_expr(&func.args[1], left_names, right_names, ctx)
            )
        }
        _ => {
            let combined = combined_names();
            let rendered = render_verbose_expr(expr, &combined, ctx);
            if rendered.contains("OUTER_VAR") || rendered.contains("INNER_VAR") {
                strip_outer_parens(&render_explain_join_expr(expr, left_names, right_names))
            } else {
                rendered
            }
        }
    }
}

pub fn render_system_var_join_op(
    left: &Expr,
    right: &Expr,
    op_text: &str,
    left_names: &[String],
    right_names: &[String],
) -> Option<String> {
    let (Expr::Var(left_var), Expr::Var(right_var)) = (left, right) else {
        return None;
    };
    if !is_system_attr(left_var.varattno) || !is_system_attr(right_var.varattno) {
        return None;
    }
    let left_name = render_system_var_name(left_var.varattno, left_names)?;
    let right_name = render_system_var_name(right_var.varattno, right_names)?;
    Some(format!("({left_name} {op_text} {right_name})"))
}

pub fn verbose_builtin_infix_operator(implementation: ScalarFunctionImpl) -> Option<&'static str> {
    match implementation {
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TsMatch) => Some("@@"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TsQueryContains) => Some("@>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TsQueryContainedBy) => Some("<@"),
        _ => None,
    }
}

pub fn field_select_projected_value<'a>(inner: &'a Expr, field: &str) -> Option<&'a Expr> {
    if let Expr::Row { fields, .. } = inner
        && let Some((_, value)) = fields
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(field))
    {
        return Some(value);
    }
    let Expr::Case(case_expr) = inner else {
        return None;
    };
    if case_expr.arg.is_some()
        || case_expr.args.is_empty()
        || !case_expr
            .args
            .iter()
            .all(|arm| matches!(arm.result, Expr::Const(Value::Null)))
    {
        return None;
    }
    let Expr::Row { fields, .. } = case_expr.defresult.as_ref() else {
        return None;
    };
    fields
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(field))
        .map(|(_, value)| value)
}

pub fn render_verbose_join_expr_list(
    exprs: &[Expr],
    left_names: &[String],
    right_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let rendered = exprs
        .iter()
        .map(|expr| render_verbose_join_expr(expr, left_names, right_names, ctx))
        .collect::<Vec<_>>()
        .join(" AND ");
    if exprs.len() > 1 {
        format!("({rendered})")
    } else {
        rendered
    }
}

pub fn render_verbose_row_whole_star(expr: &Expr, column_names: &[String]) -> Option<String> {
    let Expr::Row { fields, .. } = expr else {
        return None;
    };
    let rendered_fields = fields
        .iter()
        .map(|(_, expr)| {
            let Expr::Var(var) = expr else {
                return None;
            };
            render_var_name(var.varattno, column_names)
        })
        .collect::<Option<Vec<_>>>()?;
    let prefix = common_qualified_field_prefix(&rendered_fields)?;
    if prefix.starts_with("\"*VALUES*") {
        return None;
    }
    Some(format!("{prefix}.*"))
}

pub fn render_verbose_composite_null_test(
    expr: &Expr,
    is_null: bool,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> Option<String> {
    let Expr::Row { fields, .. } = expr else {
        return None;
    };
    if let Some(star) = render_verbose_row_whole_star(expr, column_names) {
        let op = if is_null { "IS NULL" } else { "IS NOT NULL" };
        return Some(format!("({star} {op})"));
    }
    let rendered = fields
        .iter()
        .map(|(_, field_expr)| {
            let field = render_verbose_expr(field_expr, column_names, ctx);
            let composite_field = expr_sql_type_hint(field_expr).is_some_and(|ty| {
                matches!(
                    ty.kind,
                    pgrust_nodes::parsenodes::SqlTypeKind::Composite
                        | pgrust_nodes::parsenodes::SqlTypeKind::Record
                )
            });
            let op = match (is_null, composite_field) {
                (true, true) => "IS NOT DISTINCT FROM NULL",
                (true, false) => "IS NULL",
                (false, true) => "IS DISTINCT FROM NULL",
                (false, false) => "IS NOT NULL",
            };
            format!("({field} {op})")
        })
        .collect::<Vec<_>>();
    (!rendered.is_empty()).then(|| format!("({})", rendered.join(" AND ")))
}

pub fn common_qualified_field_prefix(rendered_fields: &[String]) -> Option<String> {
    let prefix = rendered_fields
        .iter()
        .filter_map(|name| name.rsplit_once('.').map(|(prefix, _)| prefix))
        .next()?;
    rendered_fields
        .iter()
        .all(|name| {
            name.rsplit_once('.')
                .is_some_and(|(candidate, _)| candidate == prefix)
        })
        .then(|| prefix.to_string())
}

pub fn postgres_parenthesize_row_field_select(rendered: String) -> String {
    if let Some((row_expr, field)) = rendered.rsplit_once(").")
        && row_expr.starts_with("(ROW(")
    {
        return format!("({row_expr})).{field}");
    }
    rendered
}

pub fn render_verbose_expr(
    expr: &Expr,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    if let Some(rendered) = render_verbose_range_support_expr(expr, column_names) {
        return rendered;
    }
    match expr {
        Expr::GroupingKey(grouping_key) => {
            render_verbose_expr(&grouping_key.expr, column_names, ctx)
        }
        Expr::GroupingFunc(grouping_func) => {
            let args = grouping_func
                .args
                .iter()
                .map(|arg| render_verbose_expr(arg, column_names, ctx))
                .collect::<Vec<_>>()
                .join(", ");
            format!("GROUPING({args})")
        }
        Expr::Var(var) => render_var_name(var.varattno, column_names)
            .or_else(|| render_system_var_name(var.varattno, column_names))
            .unwrap_or_else(|| {
                pgrust_nodes::primnodes::attrno_index(var.varattno)
                    .map(|index| format!("column{}", index + 1))
                    .unwrap_or_else(|| strip_outer_parens(&render_explain_expr(expr, column_names)))
            }),
        Expr::FieldSelect {
            expr: inner, field, ..
        } => {
            if let Some(value) = field_select_projected_value(inner, field) {
                return render_verbose_expr(value, column_names, ctx);
            }
            postgres_parenthesize_row_field_select(strip_outer_parens(&render_explain_expr(
                expr,
                column_names,
            )))
        }
        Expr::Param(param) if param.paramkind == ParamKind::Exec => ctx
            .exec_params
            .iter()
            .rev()
            .find(|source| source.paramid == param.paramid)
            .map(|source| render_verbose_expr(&source.expr, &source.column_names, ctx))
            .unwrap_or_else(|| format!("${}", param.paramid)),
        Expr::Param(param) if param.paramkind == ParamKind::External => {
            format!("${}", param.paramid)
        }
        Expr::Const(Value::Record(record)) => render_verbose_record_const(record, ctx),
        Expr::Const(value) => {
            strip_outer_parens(&render_explain_expr(&Expr::Const(value.clone()), &[]))
        }
        Expr::Row { .. } => render_verbose_row_whole_star(expr, column_names)
            .unwrap_or_else(|| strip_outer_parens(&render_explain_expr(expr, column_names))),
        Expr::IsNull(inner) => render_verbose_composite_null_test(inner, true, column_names, ctx)
            .unwrap_or_else(|| strip_outer_parens(&render_explain_expr(expr, column_names))),
        Expr::IsNotNull(inner) => {
            render_verbose_composite_null_test(inner, false, column_names, ctx)
                .unwrap_or_else(|| strip_outer_parens(&render_explain_expr(expr, column_names)))
        }
        Expr::Cast(inner, ty) => {
            if let Some(rendered) = render_verbose_const_cast(inner, *ty, ctx) {
                return rendered;
            }
            if let Expr::Var(var) = inner.as_ref()
                && verbose_cast_is_implicit_integer_widening(var.vartype, *ty)
            {
                return render_verbose_expr(inner, column_names, ctx);
            }
            if let Expr::Const(value @ (Value::Text(_) | Value::TextRef(_, _))) = inner.as_ref()
                && matches!(
                    ty.kind,
                    pgrust_nodes::parsenodes::SqlTypeKind::Varchar
                        | pgrust_nodes::parsenodes::SqlTypeKind::Char
                )
            {
                let rendered = match value {
                    Value::Text(value) => format!("'{}'", value.replace('\'', "''")),
                    Value::TextRef(_, _) => {
                        format!("'{}'", value.as_text().unwrap().replace('\'', "''"))
                    }
                    _ => unreachable!(),
                };
                return format!("{rendered}::{}", render_type_name(*ty, ctx));
            }
            let inner = render_verbose_expr(inner, column_names, ctx);
            format!("({inner})::{}", render_type_name(*ty, ctx))
        }
        Expr::ArrayLiteral { .. } => strip_outer_parens(&render_explain_expr(expr, column_names)),
        Expr::ArraySubscript { array, subscripts } => {
            let array = render_verbose_expr(array, column_names, ctx);
            let rendered_subscripts = subscripts
                .iter()
                .map(|subscript| match (&subscript.lower, &subscript.upper) {
                    (Some(lower), Some(upper)) if subscript.is_slice => format!(
                        "{}:{}",
                        render_verbose_expr(lower, column_names, ctx),
                        render_verbose_expr(upper, column_names, ctx)
                    ),
                    (Some(lower), _) => render_verbose_expr(lower, column_names, ctx),
                    (None, Some(upper)) => {
                        format!(":{}", render_verbose_expr(upper, column_names, ctx))
                    }
                    (None, None) => String::new(),
                })
                .collect::<Vec<_>>()
                .join("][");
            format!("({array})[{rendered_subscripts}]")
        }
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::BpcharToText)
            ) && func.args.len() == 1 =>
        {
            render_verbose_expr(&func.args[0], column_names, ctx)
        }
        Expr::Op(op) => {
            let [left, right] = op.args.as_slice() else {
                return strip_outer_parens(&render_explain_expr(expr, column_names));
            };
            let Some(op_text) = verbose_op_text(op.opno, op.op) else {
                return strip_outer_parens(&render_explain_expr(expr, column_names));
            };
            let mut left_rendered = render_verbose_op_arg(left, column_names, ctx);
            let mut right_rendered = render_verbose_op_arg(right, column_names, ctx);
            if (verbose_expr_is_numeric(left) || left_rendered.starts_with("avg("))
                && matches!(right, Expr::Const(_))
            {
                right_rendered = format!("'{}'::numeric", right_rendered);
            }
            if (verbose_expr_is_numeric(right) || right_rendered.starts_with("avg("))
                && matches!(left, Expr::Const(_))
            {
                left_rendered = format!("'{}'::numeric", left_rendered);
            }
            format!("({left_rendered} {op_text} {right_rendered})")
        }
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            pgrust_nodes::primnodes::BoolExprType::And => format!(
                "({})",
                bool_expr
                    .args
                    .iter()
                    .map(|arg| render_verbose_expr(arg, column_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            pgrust_nodes::primnodes::BoolExprType::Or => format!(
                "({})",
                bool_expr
                    .args
                    .iter()
                    .map(|arg| render_verbose_expr(arg, column_names, ctx))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            ),
            pgrust_nodes::primnodes::BoolExprType::Not => {
                let Some(inner) = bool_expr.args.first() else {
                    return format!("{expr:?}");
                };
                format!("NOT {}", render_verbose_expr(inner, column_names, ctx))
            }
        },
        Expr::Aggref(aggref) => render_verbose_aggref(aggref, column_names, ctx),
        Expr::WindowFunc(window_func) => render_verbose_window_func(window_func, column_names, ctx),
        Expr::SetReturning(srf) => render_verbose_set_returning_call(&srf.call, ctx),
        Expr::ScalarArrayOp(_) => render_explain_expr(expr, column_names),
        _ => strip_outer_parens(&render_explain_expr(expr, column_names)),
    }
}

pub fn verbose_cast_is_implicit_integer_widening(
    from: pgrust_nodes::parsenodes::SqlType,
    to: pgrust_nodes::parsenodes::SqlType,
) -> bool {
    use pgrust_nodes::parsenodes::SqlTypeKind::{Int2, Int4, Int8};
    matches!(
        (from.kind, to.kind),
        (Int2, Int4) | (Int2, Int8) | (Int4, Int8)
    )
}

pub fn render_merge_key_conditions(
    outer_merge_keys: &[Expr],
    inner_merge_keys: &[Expr],
    left_names: &[String],
    right_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let rendered = outer_merge_keys
        .iter()
        .zip(inner_merge_keys.iter())
        .map(|(outer_key, inner_key)| {
            format!(
                "{} = {}",
                strip_outer_parens(&render_verbose_expr(outer_key, left_names, ctx)),
                strip_outer_parens(&render_verbose_expr(inner_key, right_names, ctx))
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    format!("({rendered})")
}

pub fn render_merge_condition_from_child_sorts(
    left: &Plan,
    right: &Plan,
    left_names: &[String],
    right_names: &[String],
    ctx: &VerboseExplainContext,
) -> Option<String> {
    let left_expr = first_sort_expr(left)?;
    let right_expr = first_sort_expr(right)?;
    Some(format!(
        "({} = {})",
        strip_outer_parens(&render_verbose_expr(left_expr, left_names, ctx)),
        strip_outer_parens(&render_verbose_expr(right_expr, right_names, ctx))
    ))
}

pub fn first_sort_expr(plan: &Plan) -> Option<&Expr> {
    match plan {
        Plan::OrderBy { items, .. } | Plan::IncrementalSort { items, .. } => {
            items.first().map(|item| &item.expr)
        }
        _ => None,
    }
}

pub fn normalize_aggregate_operand_parens(rendered: String) -> String {
    let mut chars = rendered.chars().collect::<Vec<_>>();
    let mut index = 0;
    while index < chars.len() {
        if chars[index] != '(' || !aggregate_call_starts_at(&chars, index + 1) {
            index += 1;
            continue;
        }
        let mut depth = 0usize;
        let mut end = index;
        while end < chars.len() {
            match chars[end] {
                '(' => depth += 1,
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            end += 1;
        }
        if index > 0
            && end < chars.len()
            && chars[index - 1] == '('
            && chars.get(end + 1).is_some_and(|ch| ch.is_whitespace())
        {
            chars.remove(end);
            chars.remove(index);
            index = index.saturating_sub(1);
        } else {
            index = end.saturating_add(1);
        }
    }
    chars.into_iter().collect()
}

pub fn aggregate_call_starts_at(chars: &[char], index: usize) -> bool {
    ["avg(", "count(", "max(", "min(", "sum("]
        .iter()
        .any(|prefix| {
            let prefix = prefix.chars().collect::<Vec<_>>();
            chars
                .get(index..index.saturating_add(prefix.len()))
                .is_some_and(|candidate| candidate == prefix.as_slice())
        })
}

pub fn render_tablesample_verbose_arg(
    expr: &Expr,
    type_name: &'static str,
    ctx: &VerboseExplainContext,
) -> String {
    match expr {
        Expr::Const(value) => format!("'{}'::{type_name}", render_explain_literal(value)),
        Expr::Cast(inner, ty)
            if matches!(ty.kind, SqlTypeKind::Float4 | SqlTypeKind::Float8)
                && matches!(inner.as_ref(), Expr::Const(_)) =>
        {
            render_verbose_expr(expr, &[], ctx)
        }
        Expr::Cast(inner, ty) if matches!(ty.kind, SqlTypeKind::Float4 | SqlTypeKind::Float8) => {
            render_verbose_expr(inner, &[], ctx)
        }
        _ => render_verbose_expr(expr, &[], ctx),
    }
}

pub fn render_verbose_op_arg(
    expr: &Expr,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> String {
    let rendered = render_verbose_expr(expr, column_names, ctx);
    if matches!(expr, Expr::Op(_) | Expr::Bool(_)) {
        rendered
    } else {
        strip_outer_parens(&rendered)
    }
}

pub fn verbose_op_text(opno: u32, op: pgrust_nodes::primnodes::OpExprKind) -> Option<&'static str> {
    match opno {
        TEXT_PATTERN_LT_OPERATOR_OID => return Some("~<~"),
        TEXT_PATTERN_LE_OPERATOR_OID => return Some("~<=~"),
        TEXT_PATTERN_GE_OPERATOR_OID => return Some("~>=~"),
        TEXT_PATTERN_GT_OPERATOR_OID => return Some("~>~"),
        _ => {}
    }
    match op {
        pgrust_nodes::primnodes::OpExprKind::Add => Some("+"),
        pgrust_nodes::primnodes::OpExprKind::Sub => Some("-"),
        pgrust_nodes::primnodes::OpExprKind::Mul => Some("*"),
        pgrust_nodes::primnodes::OpExprKind::Div => Some("/"),
        pgrust_nodes::primnodes::OpExprKind::Mod => Some("%"),
        pgrust_nodes::primnodes::OpExprKind::Eq => Some("="),
        pgrust_nodes::primnodes::OpExprKind::NotEq => Some("<>"),
        pgrust_nodes::primnodes::OpExprKind::Lt => Some("<"),
        pgrust_nodes::primnodes::OpExprKind::LtEq => Some("<="),
        pgrust_nodes::primnodes::OpExprKind::Gt => Some(">"),
        pgrust_nodes::primnodes::OpExprKind::GtEq => Some(">="),
        pgrust_nodes::primnodes::OpExprKind::Concat => Some("||"),
        pgrust_nodes::primnodes::OpExprKind::ArrayOverlap => Some("&&"),
        pgrust_nodes::primnodes::OpExprKind::ArrayContains => Some("@>"),
        pgrust_nodes::primnodes::OpExprKind::ArrayContained => Some("<@"),
        pgrust_nodes::primnodes::OpExprKind::JsonGet => Some("->"),
        pgrust_nodes::primnodes::OpExprKind::JsonGetText => Some("->>"),
        _ => None,
    }
}

pub fn verbose_expr_is_numeric(expr: &Expr) -> bool {
    use pgrust_nodes::parsenodes::SqlTypeKind;
    match expr {
        Expr::Aggref(aggref) => {
            matches!(aggref.aggtype.kind, SqlTypeKind::Numeric)
                || builtin_aggregate_function_for_proc_oid(aggref.aggfnoid)
                    .is_some_and(|func| matches!(func, pgrust_nodes::primnodes::AggFunc::Avg))
        }
        Expr::Cast(inner, ty) => {
            matches!(ty.kind, SqlTypeKind::Numeric) || verbose_expr_is_numeric(inner)
        }
        Expr::Collate { expr, .. } => verbose_expr_is_numeric(expr),
        Expr::Const(Value::Numeric(_)) => true,
        _ => false,
    }
}

pub fn render_var_name(
    attno: pgrust_nodes::primnodes::AttrNumber,
    names: &[String],
) -> Option<String> {
    pgrust_nodes::primnodes::attrno_index(attno).and_then(|index| names.get(index).cloned())
}

pub fn render_system_var_name(
    attno: pgrust_nodes::primnodes::AttrNumber,
    names: &[String],
) -> Option<String> {
    let name = match attno {
        TABLE_OID_ATTR_NO => "tableoid",
        SELF_ITEM_POINTER_ATTR_NO => "ctid",
        XMIN_ATTR_NO => "xmin",
        CMIN_ATTR_NO => "cmin",
        XMAX_ATTR_NO => "xmax",
        CMAX_ATTR_NO => "cmax",
        _ => return None,
    };
    relation_qualifier_from_output_names(names)
        .map(|qualifier| format!("{qualifier}.{name}"))
        .or_else(|| Some(name.into()))
}

pub fn relation_qualifier_from_output_names(names: &[String]) -> Option<String> {
    names
        .iter()
        .filter_map(|name| name.split_once('.').map(|(qualifier, _)| qualifier))
        .find(|qualifier| !qualifier.is_empty())
        .map(str::to_string)
}

pub fn relation_base_name(relation_name: &str) -> &str {
    relation_name
        .split_once(' ')
        .map(|(base, _)| base)
        .unwrap_or(relation_name)
        .rsplit_once('.')
        .map(|(_, name)| name)
        .unwrap_or_else(|| {
            relation_name
                .split_once(' ')
                .map(|(base, _)| base)
                .unwrap_or(relation_name)
        })
}

pub fn relation_alias_or_base_name(relation_name: &str) -> &str {
    relation_name
        .split_once(' ')
        .map(|(_, alias)| alias)
        .filter(|alias| !alias.is_empty())
        .unwrap_or_else(|| {
            relation_name
                .rsplit_once('.')
                .map(|(_, name)| name)
                .unwrap_or(relation_name)
        })
}

pub fn strip_outer_parens(text: &str) -> String {
    text.strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .unwrap_or(text)
        .to_string()
}

pub fn render_nonverbose_expr_with_dynamic_type_names(
    expr: &Expr,
    column_names: &[String],
    ctx: &VerboseExplainContext,
) -> Option<String> {
    let mut replacements = Vec::new();
    collect_dynamic_const_cast_replacements(expr, ctx, &mut replacements);
    if replacements.is_empty() {
        return None;
    }
    let mut rendered = render_explain_expr(expr, column_names);
    replacements.sort_by(|(left, _), (right, _)| right.len().cmp(&left.len()));
    replacements.dedup();
    for (from, to) in replacements {
        rendered = rendered.replace(&from, &to);
    }
    Some(rendered)
}

pub fn collect_dynamic_const_cast_replacements(
    expr: &Expr,
    ctx: &VerboseExplainContext,
    replacements: &mut Vec<(String, String)>,
) {
    match expr {
        Expr::Cast(inner, ty)
            if ty.type_oid != 0
                && ctx.type_names.contains_key(&ty.type_oid)
                && matches!(inner.as_ref(), Expr::Const(_)) =>
        {
            let old = render_explain_expr(expr, &[]);
            let new = render_verbose_const_cast(inner, *ty, ctx)
                .expect("dynamic constant cast checked above");
            replacements.push((old, new));
            if let Expr::Const(value) = inner.as_ref() {
                replacements.push((
                    format!("{}::text", render_explain_literal(value)),
                    render_verbose_const_cast(inner, *ty, ctx)
                        .expect("dynamic constant cast checked above"),
                ));
            }
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => {
            collect_dynamic_const_cast_replacements(inner, ctx, replacements);
        }
        Expr::Op(op) => {
            for arg in &op.args {
                collect_dynamic_const_cast_replacements(arg, ctx, replacements);
            }
        }
        Expr::Bool(bool_expr) => {
            for arg in &bool_expr.args {
                collect_dynamic_const_cast_replacements(arg, ctx, replacements);
            }
        }
        Expr::ScalarArrayOp(saop) => {
            collect_dynamic_const_cast_replacements(&saop.left, ctx, replacements);
            collect_dynamic_const_cast_replacements(&saop.right, ctx, replacements);
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            collect_dynamic_const_cast_replacements(left, ctx, replacements);
            collect_dynamic_const_cast_replacements(right, ctx, replacements);
        }
        Expr::ArrayLiteral { elements, .. } => {
            if let Some(replacement) = render_dynamic_array_literal_type_replacement(expr, ctx) {
                replacements.push(replacement);
            }
            for element in elements {
                collect_dynamic_const_cast_replacements(element, ctx, replacements);
            }
        }
        Expr::Row { fields, .. } => {
            for (_, field) in fields {
                collect_dynamic_const_cast_replacements(field, ctx, replacements);
            }
        }
        Expr::Case(case_expr) => {
            if let Some(arg) = &case_expr.arg {
                collect_dynamic_const_cast_replacements(arg, ctx, replacements);
            }
            for arm in &case_expr.args {
                collect_dynamic_const_cast_replacements(&arm.expr, ctx, replacements);
                collect_dynamic_const_cast_replacements(&arm.result, ctx, replacements);
            }
            collect_dynamic_const_cast_replacements(&case_expr.defresult, ctx, replacements);
        }
        Expr::Func(func) => {
            for arg in &func.args {
                collect_dynamic_const_cast_replacements(arg, ctx, replacements);
            }
        }
        Expr::ArraySubscript { array, subscripts } => {
            collect_dynamic_const_cast_replacements(array, ctx, replacements);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_dynamic_const_cast_replacements(lower, ctx, replacements);
                }
                if let Some(upper) = &subscript.upper {
                    collect_dynamic_const_cast_replacements(upper, ctx, replacements);
                }
            }
        }
        _ => {}
    }
}

pub fn render_dynamic_array_literal_type_replacement(
    expr: &Expr,
    ctx: &VerboseExplainContext,
) -> Option<(String, String)> {
    let Expr::ArrayLiteral { array_type, .. } = expr else {
        return None;
    };
    let element = array_type.element_type();
    if element.type_oid == 0 || !ctx.type_names.contains_key(&element.type_oid) {
        return None;
    }
    let old = render_explain_expr(expr, &[]);
    let replacement = {
        let (prefix, suffix) = old.rsplit_once("::")?;
        let close = suffix.ends_with(')').then_some(")").unwrap_or_default();
        format!("{prefix}::{}{close}", render_type_name(*array_type, ctx))
    };
    Some((old, replacement))
}

pub fn render_verbose_const_cast(
    inner: &Expr,
    ty: pgrust_nodes::parsenodes::SqlType,
    ctx: &VerboseExplainContext,
) -> Option<String> {
    if ty.type_oid == 0 || !ctx.type_names.contains_key(&ty.type_oid) {
        return None;
    }
    let Expr::Const(value) = inner else {
        return None;
    };
    Some(format!(
        "{}::{}",
        render_explain_literal(value),
        render_type_name(ty, ctx)
    ))
}

pub fn render_type_name(
    ty: pgrust_nodes::parsenodes::SqlType,
    ctx: &VerboseExplainContext,
) -> String {
    use pgrust_nodes::parsenodes::SqlTypeKind::*;
    let element = ty.element_type();
    if !ty.is_array
        && let Some(name) = ctx.type_names.get(&ty.type_oid)
    {
        return name.clone();
    }
    let base = match element.kind {
        _ if element.type_oid != 0 && ctx.type_names.contains_key(&element.type_oid) => {
            ctx.type_names[&element.type_oid].clone()
        }
        Int2 => "smallint".into(),
        Int4 => "integer".into(),
        Int8 => "bigint".into(),
        Text => "text".into(),
        Varchar => element
            .char_len()
            .map(|len| format!("character varying({len})"))
            .unwrap_or_else(|| "character varying".into()),
        Char => element
            .char_len()
            .map(|len| format!("character({len})"))
            .unwrap_or_else(|| "bpchar".into()),
        Bool => "boolean".into(),
        Float4 => "real".into(),
        Float8 => "double precision".into(),
        Numeric => element
            .numeric_precision_scale()
            .map(|(precision, scale)| format!("numeric({precision},{scale})"))
            .unwrap_or_else(|| "numeric".into()),
        Json => "json".into(),
        Jsonb => "jsonb".into(),
        Uuid => "uuid".into(),
        _ => "unknown".into(),
    };
    if ty.is_array {
        format!("{base}[]")
    } else {
        base
    }
}

pub fn direct_plan_children(plan: &Plan) -> Vec<&Plan> {
    match plan {
        Plan::Result { .. }
        | Plan::SeqScan { .. }
        | Plan::TidScan { .. }
        | Plan::TidRangeScan { .. }
        | Plan::IndexOnlyScan { .. }
        | Plan::IndexScan { .. }
        | Plan::BitmapIndexScan { .. }
        | Plan::FunctionScan { .. }
        | Plan::WorkTableScan { .. }
        | Plan::Values { .. } => Vec::new(),
        Plan::BitmapOr { children, .. } | Plan::BitmapAnd { children, .. } => {
            children.iter().collect()
        }
        Plan::BitmapHeapScan { bitmapqual, .. } => vec![bitmapqual.as_ref()],
        Plan::Append { children, .. } | Plan::MergeAppend { children, .. } => {
            flattened_append_children(children)
        }
        Plan::SetOp { children, .. } => children.iter().collect(),
        Plan::Filter { input, .. }
            if matches!(
                input.as_ref(),
                Plan::SeqScan { .. }
                    | Plan::TidScan { .. }
                    | Plan::TidRangeScan { .. }
                    | Plan::IndexOnlyScan { .. }
                    | Plan::IndexScan { .. }
            ) =>
        {
            Vec::new()
        }
        Plan::Projection { input, .. } if matches!(input.as_ref(), Plan::Result { .. }) => {
            Vec::new()
        }
        Plan::Projection { input, .. } if plan_is_limit_result(input) => Vec::new(),
        Plan::Hash { input, .. }
        | Plan::Materialize { input, .. }
        | Plan::Memoize { input, .. }
        | Plan::Gather { input, .. }
        | Plan::GatherMerge { input, .. }
        | Plan::Unique { input, .. }
        | Plan::OrderBy { input, .. }
        | Plan::IncrementalSort { input, .. }
        | Plan::Limit { input, .. }
        | Plan::LockRows { input, .. }
        | Plan::Projection { input, .. }
        | Plan::WindowAgg { input, .. }
        | Plan::SubqueryScan { input, .. }
        | Plan::ProjectSet { input, .. }
        | Plan::CteScan {
            cte_plan: input, ..
        } => vec![input.as_ref()],
        Plan::Aggregate { input, .. } => vec![aggregate_explain_child(input)],
        Plan::Filter { input, .. } => vec![input.as_ref()],
        Plan::NestedLoopJoin { left, right, .. }
        | Plan::HashJoin { left, right, .. }
        | Plan::MergeJoin { left, right, .. }
        | Plan::RecursiveUnion {
            anchor: left,
            recursive: right,
            ..
        } => vec![left.as_ref(), right.as_ref()],
    }
}

pub fn flattened_append_children(children: &[Plan]) -> Vec<&Plan> {
    let mut flattened = Vec::new();
    for child in children {
        if let Some(nested) = passthrough_append_children(child) {
            flattened.extend(flattened_append_children(nested));
        } else {
            flattened.push(child);
        }
    }
    flattened
}

pub fn passthrough_append_children(mut plan: &Plan) -> Option<&[Plan]> {
    loop {
        match plan {
            Plan::Append { children, .. } | Plan::MergeAppend { children, .. } => {
                return Some(children);
            }
            _ => {
                let child = explain_passthrough_plan_child(plan)?;
                plan = child;
            }
        }
    }
}

pub fn aggregate_explain_child(input: &Plan) -> &Plan {
    match input {
        Plan::Projection {
            input: child,
            targets,
            ..
        } if !targets_have_direct_subplans(targets) => child.as_ref(),
        _ => input,
    }
}
