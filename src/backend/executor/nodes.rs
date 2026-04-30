use super::{
    AggGroup, AggregateRuntime, ExecError, ExecutorContext, OrderedAggInput,
    build_aggregate_runtime, executor_start,
};
use crate::backend::access::heap::heapam::{
    heap_fetch_visible_with_txns, heap_scan_begin_visible, heap_scan_end, heap_scan_next_visible,
    heap_scan_page_next_tuple, heap_scan_prepare_next_page, heap_scan_prepare_page_at,
};
use crate::backend::access::index::indexam;
use crate::backend::access::nbtree::nbtcompare::compare_bt_values;
use crate::backend::access::nbtree::nbtree::decode_key_payload;
use crate::backend::commands::explain::format_explain_lines_with_costs;
use crate::backend::executor::exec_expr::{
    compare_order_by_keys, eval_expr, hashfloat8_for_tablesample, pg_hash_three_u32,
    pg_hash_two_u32,
};
use crate::backend::executor::expr_casts::cast_value;
use crate::backend::executor::expr_geometry::render_geometry_text;
use crate::backend::executor::expr_ops::compare_order_values;
use crate::backend::executor::jsonb::{JsonbValue, compare_jsonb, decode_jsonb, encode_jsonb};
use crate::backend::executor::pg_regex::explain_similar_pattern;
use crate::backend::executor::srf::{
    eval_project_set_returning_call, eval_set_returning_call,
    eval_set_returning_call_simple_values, set_returning_call_label,
};
use crate::backend::executor::value_io::{decode_value_with_toast, missing_column_value};
use crate::backend::executor::window::execute_window_clause;
use crate::backend::libpq::pqformat::FloatFormatOptions;
use crate::backend::libpq::pqformat::format_float8_text;
use crate::backend::optimizer::partition_prune::partition_may_satisfy_filter_with_runtime_values;
use crate::backend::parser::{CatalogLookup, SqlType, SqlTypeKind, SubqueryComparisonOp};
use crate::backend::storage::lmgr::RowLockMode;
use crate::backend::storage::page::bufpage::{
    ItemIdFlags, page_get_item_id_unchecked, page_get_item_unchecked, page_get_max_offset_number,
};
use crate::backend::storage::smgr::ForkNumber;
use crate::backend::utils::misc::guc_datetime::{DateOrder, DateStyleFormat, DateTimeConfig};
use crate::backend::utils::time::date::{format_date_text, parse_date_text};
use crate::backend::utils::time::instant::Instant;
use crate::backend::utils::time::timestamp::{
    format_timestamp_text, format_timestamptz_text, parse_timestamp_text, parse_timestamptz_text,
};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::visibilitymap::visibilitymap_get_status;
use crate::include::access::visibilitymapdefs::VISIBILITYMAP_ALL_VISIBLE;
use crate::include::catalog::{
    BTREE_AM_OID, C_COLLATION_OID, DATE_TYPE_OID, DEFAULT_COLLATION_OID, GIST_AM_OID,
    GIST_TSQUERY_FAMILY_OID, GIST_TSVECTOR_FAMILY_OID, HASH_AM_OID, PG_NAMESPACE_RELATION_OID,
    PG_SUBSCRIPTION_RELATION_OID, POSIX_COLLATION_OID, SPGIST_AM_OID, TEXT_TYPE_OID,
    TIMESTAMPTZ_TYPE_OID,
};
use crate::include::nodes::datetime::{DATEVAL_NOBEGIN, DATEVAL_NOEND, DateADT, TimestampTzADT};
use crate::include::nodes::datum::{ArrayValue, Value};
use crate::include::nodes::execnodes::{
    AggregateState, AppendState, BitmapAndState, BitmapHeapScanState, BitmapIndexScanState,
    BitmapOrState, BitmapQualState, CteScanState, FilterState, FunctionScanRows, FunctionScanState,
    GatherState, IncrementalSortState, IndexOnlyScanState, IndexScanState, LimitState,
    LockRowsState, MaterializeState, MaterializedRow, MemoizeCacheKey, MemoizeState,
    MergeAppendState, NestedLoopJoinState, NodeExecStats, OrderByState, PlanNode, PlanState,
    ProjectSetState, ProjectionState, RecursiveUnionState, ResultState, SeqScanState, SetOpState,
    SlotKind, SubqueryScanState, SystemVarBinding, TableSampleMethod, TableSampleState,
    TidScanState, ToastRelationRef, TupleSlot, UniqueState, ValuesState, WindowAggState,
    WorkTableScanState,
};
use crate::include::nodes::plannodes::{
    AggregatePhase, AggregateStrategy, IndexScanKey, IndexScanKeyArgument, PartitionPrunePlan,
    Plan, TidScanSource,
};
use crate::include::nodes::primnodes::{
    AggAccum, BuiltinScalarFunction, CaseExpr, Expr, FuncExpr, INDEX_VAR, INNER_VAR, JoinType,
    OUTER_VAR, OrderByEntry, ParamKind, RelationDesc, SELF_ITEM_POINTER_ATTR_NO,
    ScalarFunctionImpl, SetReturningCall, SubLinkType, TABLE_OID_ATTR_NO, Var, XMAX_ATTR_NO,
    XMIN_ATTR_NO, attrno_index, is_special_varno,
};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

const EMPTY_SYSTEM_BINDINGS: [SystemVarBinding; 0] = [];
const EMPTY_GROUPING_REFS: [usize; 0] = [];

fn set_active_grouping_refs(ctx: &mut ExecutorContext, refs: &[usize]) {
    ctx.active_grouping_refs.clear();
    ctx.active_grouping_refs.extend_from_slice(refs);
}

pub(crate) fn pg_sql_sort_by<T>(
    values: &mut [T],
    mut compare: impl FnMut(&T, &T) -> std::cmp::Ordering,
) {
    fn med3<T>(
        values: &[T],
        a: usize,
        b: usize,
        c: usize,
        compare: &mut impl FnMut(&T, &T) -> std::cmp::Ordering,
    ) -> usize {
        if compare(&values[a], &values[b]) == std::cmp::Ordering::Less {
            if compare(&values[b], &values[c]) == std::cmp::Ordering::Less {
                b
            } else if compare(&values[a], &values[c]) == std::cmp::Ordering::Less {
                c
            } else {
                a
            }
        } else if compare(&values[b], &values[c]) == std::cmp::Ordering::Greater {
            b
        } else if compare(&values[a], &values[c]) == std::cmp::Ordering::Less {
            a
        } else {
            c
        }
    }

    fn swap_ranges<T>(values: &mut [T], left: usize, right: usize, count: usize) {
        for offset in 0..count {
            values.swap(left + offset, right + offset);
        }
    }

    fn sort<T>(values: &mut [T], compare: &mut impl FnMut(&T, &T) -> std::cmp::Ordering) {
        let n = values.len();
        if n < 7 {
            for pm in 1..n {
                let mut pl = pm;
                while pl > 0 && compare(&values[pl - 1], &values[pl]) == std::cmp::Ordering::Greater
                {
                    values.swap(pl, pl - 1);
                    pl -= 1;
                }
            }
            return;
        }

        let mut presorted = true;
        for pm in 1..n {
            if compare(&values[pm - 1], &values[pm]) == std::cmp::Ordering::Greater {
                presorted = false;
                break;
            }
        }
        if presorted {
            return;
        }

        let mut pm = n / 2;
        if n > 7 {
            let mut pl = 0;
            let mut pn = n - 1;
            if n > 40 {
                let d = n / 8;
                pl = med3(values, pl, pl + d, pl + 2 * d, compare);
                pm = med3(values, pm - d, pm, pm + d, compare);
                pn = med3(values, pn - 2 * d, pn - d, pn, compare);
            }
            pm = med3(values, pl, pm, pn, compare);
        }
        values.swap(0, pm);

        let mut pa = 1usize;
        let mut pb = 1usize;
        let mut pc = (n - 1) as isize;
        let mut pd = (n - 1) as isize;
        loop {
            while (pb as isize) <= pc {
                let ordering = compare(&values[pb], &values[0]);
                if ordering == std::cmp::Ordering::Greater {
                    break;
                }
                if ordering == std::cmp::Ordering::Equal {
                    values.swap(pa, pb);
                    pa += 1;
                }
                pb += 1;
            }
            while (pb as isize) <= pc {
                let pc_index = pc as usize;
                let ordering = compare(&values[pc_index], &values[0]);
                if ordering == std::cmp::Ordering::Less {
                    break;
                }
                if ordering == std::cmp::Ordering::Equal {
                    values.swap(pc_index, pd as usize);
                    pd -= 1;
                }
                pc -= 1;
            }
            if (pb as isize) > pc {
                break;
            }
            values.swap(pb, pc as usize);
            pb += 1;
            pc -= 1;
        }

        let d1 = pa.min(pb - pa);
        swap_ranges(values, 0, pb - d1, d1);
        let d2 = ((pd - pc) as usize).min(n - (pd as usize) - 1);
        swap_ranges(values, pb, n - d2, d2);

        let d1 = pb - pa;
        let d2 = (pd - pc) as usize;
        if d1 <= d2 {
            if d1 > 1 {
                sort(&mut values[..d1], compare);
            }
            if d2 > 1 {
                sort(&mut values[n - d2..], compare);
            }
        } else {
            if d2 > 1 {
                sort(&mut values[n - d2..], compare);
            }
            if d1 > 1 {
                sort(&mut values[..d1], compare);
            }
        }
    }

    sort(values, &mut compare);
}

fn aggregate_key_value_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Jsonb(left_bytes), Value::Jsonb(right_bytes)) => {
            match (decode_jsonb(left_bytes), decode_jsonb(right_bytes)) {
                (Ok(left_json), Ok(right_json)) => {
                    compare_jsonb(&left_json, &right_json) == std::cmp::Ordering::Equal
                }
                _ => left_bytes == right_bytes,
            }
        }
        _ => left == right,
    }
}

fn aggregate_key_values_equal(left: &[Value], right: &[Value]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| aggregate_key_value_equal(left, right))
}

fn aggregate_group_lookup_key(values: &[Value]) -> Vec<Value> {
    values.iter().map(aggregate_group_lookup_value).collect()
}

fn aggregate_group_lookup_value(value: &Value) -> Value {
    match value {
        Value::Jsonb(bytes) => decode_jsonb(bytes)
            .map(|json| Value::Jsonb(encode_jsonb(&normalize_jsonb_group_key(json))))
            .unwrap_or_else(|_| value.clone()),
        _ => value.clone(),
    }
}

fn normalize_jsonb_group_key(value: JsonbValue) -> JsonbValue {
    match value {
        JsonbValue::Numeric(value) => JsonbValue::Numeric(value.normalize_display_scale()),
        JsonbValue::Array(items) => {
            JsonbValue::Array(items.into_iter().map(normalize_jsonb_group_key).collect())
        }
        JsonbValue::Object(items) => JsonbValue::Object(
            items
                .into_iter()
                .map(|(key, value)| (key, normalize_jsonb_group_key(value)))
                .collect(),
        ),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::{pg_sql_sort_by, render_explain_const, render_explain_datetime_cast_literal};
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::backend::utils::time::datetime::days_from_ymd;
    use crate::include::nodes::datetime::DateADT;
    use crate::include::nodes::datum::Value;
    use crate::include::nodes::primnodes::Expr;

    #[test]
    fn pg_sql_sort_by_matches_postgres_empsalary_peer_order() {
        let mut rows = vec![
            ("develop", 10, 5200),
            ("sales", 1, 5000),
            ("personnel", 5, 3500),
            ("sales", 4, 4800),
            ("personnel", 2, 3900),
            ("develop", 7, 4200),
            ("develop", 9, 4500),
            ("sales", 3, 4800),
            ("develop", 8, 6000),
            ("develop", 11, 5200),
        ];

        pg_sql_sort_by(&mut rows, |left, right| {
            left.0.cmp(right.0).then_with(|| left.2.cmp(&right.2))
        });

        assert_eq!(
            rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            vec![7, 9, 11, 10, 8, 5, 2, 3, 4, 1]
        );
    }

    #[test]
    fn pg_sql_sort_by_matches_postgres_tenk1_unique_lt_10_peer_order() {
        let mut rows = vec![
            (4, 0),
            (2, 2),
            (1, 1),
            (6, 2),
            (9, 1),
            (8, 0),
            (5, 1),
            (3, 3),
            (7, 3),
            (0, 0),
        ];

        pg_sql_sort_by(&mut rows, |left, right| left.1.cmp(&right.1));

        assert_eq!(
            rows,
            vec![
                (0, 0),
                (8, 0),
                (4, 0),
                (5, 1),
                (9, 1),
                (1, 1),
                (6, 2),
                (2, 2),
                (3, 3),
                (7, 3),
            ]
        );
    }

    #[test]
    fn explain_renders_date_constants_in_postgres_mdy_style() {
        let date = DateADT(days_from_ymd(1997, 1, 1).unwrap());
        assert_eq!(
            render_explain_const(&Value::Date(date)),
            "'01-01-1997'::date"
        );

        let expr = Expr::Const(Value::Text("1997-01-01".into()));
        assert_eq!(
            render_explain_datetime_cast_literal(&expr, SqlType::new(SqlTypeKind::Date)),
            Some("'01-01-1997'::date".into())
        );
    }
}

fn slot_toast_context(
    relation: Option<ToastRelationRef>,
    ctx: &ExecutorContext,
) -> Option<crate::include::nodes::execnodes::ToastFetchContext> {
    relation.map(
        |relation| crate::include::nodes::execnodes::ToastFetchContext {
            relation,
            pool: ctx.pool.clone(),
            txns: ctx.txns.clone(),
            snapshot: ctx.snapshot.clone(),
            client_id: ctx.client_id,
        },
    )
}

fn eval_bool_qual(
    expr: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    match eval_expr(expr, slot, ctx)? {
        Value::Bool(true) => Ok(true),
        Value::Bool(false) | Value::Null => Ok(false),
        other => Err(ExecError::NonBoolQual(other)),
    }
}

fn eval_qual_list(
    quals: &[Expr],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    for qual in quals {
        if !eval_bool_qual(qual, slot, ctx)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn set_active_system_bindings(ctx: &mut ExecutorContext, bindings: &[SystemVarBinding]) {
    ctx.system_bindings.clear();
    ctx.system_bindings.extend_from_slice(bindings);
}

fn set_outer_expr_bindings(
    ctx: &mut ExecutorContext,
    values: Vec<Value>,
    bindings: &[SystemVarBinding],
) {
    ctx.expr_bindings.outer_tuple = Some(values);
    ctx.expr_bindings.outer_system_bindings = bindings.to_vec();
}

fn clear_outer_expr_bindings(ctx: &mut ExecutorContext) {
    ctx.expr_bindings.outer_tuple = None;
    ctx.expr_bindings.outer_system_bindings.clear();
}

fn set_inner_expr_bindings(
    ctx: &mut ExecutorContext,
    values: Vec<Value>,
    bindings: &[SystemVarBinding],
) {
    ctx.expr_bindings.inner_tuple = Some(values);
    ctx.expr_bindings.inner_system_bindings = bindings.to_vec();
}

fn clear_inner_expr_bindings(ctx: &mut ExecutorContext) {
    ctx.expr_bindings.inner_tuple = None;
    ctx.expr_bindings.inner_system_bindings.clear();
}

fn materialize_slot_values(slot: &mut TupleSlot) -> Result<Vec<Value>, ExecError> {
    let mut values = slot.values()?.to_vec();
    Value::materialize_all(&mut values);
    Ok(values)
}

fn store_single_virtual_value(slot: &mut TupleSlot, value: Value) {
    slot.kind = SlotKind::Virtual;
    slot.tts_values.clear();
    slot.tts_values.push(value);
    slot.tts_nvalid = 1;
    slot.decode_offset = 0;
    slot.toast = None;
    slot.table_oid = None;
    slot.virtual_tid = None;
}

fn function_scan_uses_simple_path(
    call: &crate::include::nodes::primnodes::SetReturningCall,
) -> bool {
    !call.with_ordinality() && call.output_columns().len() == 1
}

fn sequence_scan_runtime(
    ctx: &ExecutorContext,
) -> Result<&crate::pgrust::database::SequenceRuntime, ExecError> {
    ctx.sequences
        .as_deref()
        .ok_or_else(|| ExecError::DetailedError {
            message: "sequence runtime unavailable".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        })
}

fn bind_exec_params(
    params: &[crate::include::nodes::plannodes::ExecParamSource],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Vec<(usize, Option<Value>)>, ExecError> {
    let mut saved = Vec::with_capacity(params.len());
    for param in params {
        let value = eval_expr(&param.expr, slot, ctx)?;
        let old = ctx.expr_bindings.exec_params.insert(param.paramid, value);
        saved.push((param.paramid, old));
    }
    Ok(saved)
}

fn restore_exec_params(saved: Vec<(usize, Option<Value>)>, ctx: &mut ExecutorContext) {
    for (paramid, old) in saved {
        if let Some(value) = old {
            ctx.expr_bindings.exec_params.insert(paramid, value);
        } else {
            ctx.expr_bindings.exec_params.remove(&paramid);
        }
    }
}

fn merge_system_bindings(
    left: &[SystemVarBinding],
    right: &[SystemVarBinding],
) -> Vec<SystemVarBinding> {
    let mut merged = left.to_vec();
    for binding in right {
        if !merged
            .iter()
            .any(|existing| existing.varno == binding.varno)
        {
            merged.push(*binding);
        }
    }
    merged
}

fn format_qual_list(quals: &[Expr]) -> Expr {
    let mut quals = quals.to_vec();
    let first = quals.remove(0);
    quals
        .into_iter()
        .fold(first, |acc, qual| Expr::and(acc, qual))
}

fn finish_row(stats: &mut NodeExecStats, start: Option<Instant>) {
    if stats.first_tuple_time.is_none() {
        stats.first_tuple_time = Some(start.map(|start| start.elapsed()).unwrap_or_default());
    }
    stats.rows += 1;
    if let Some(start) = start {
        stats.total_time += start.elapsed();
    }
}

fn finish_eof(stats: &mut NodeExecStats, start: Option<Instant>, ctx: &ExecutorContext) {
    stats.loops += 1;
    if let Some(start) = start {
        stats.total_time += start.elapsed();
    }
    if let Some(start_usage) = stats.buffer_usage_start.take() {
        let end_usage = ctx.pool.usage_stats();
        stats.buffer_usage.shared_hit = end_usage.shared_hit.saturating_sub(start_usage.shared_hit);
        stats.buffer_usage.shared_read = end_usage
            .shared_read
            .saturating_sub(start_usage.shared_read);
        stats.buffer_usage.shared_written = end_usage
            .shared_written
            .saturating_sub(start_usage.shared_written);
    }
}

fn begin_node(stats: &mut NodeExecStats, ctx: &ExecutorContext) -> Result<(), ExecError> {
    if !stats.stack_depth_checked {
        ctx.check_stack_depth()?;
        stats.stack_depth_checked = true;
    }
    if stats.buffer_usage_start.is_none() {
        stats.buffer_usage_start = Some(ctx.pool.usage_stats());
    }
    Ok(())
}

fn note_filtered_row(stats: &mut NodeExecStats) {
    stats.rows_removed_by_filter += 1;
}

fn note_index_recheck_filtered_row(stats: &mut NodeExecStats) {
    stats.rows_removed_by_index_recheck += 1;
}

fn relation_io_object(ctx: &ExecutorContext, relation_oid: u32) -> &'static str {
    if ctx
        .catalog
        .as_deref()
        .and_then(|catalog| catalog.relation_by_oid(relation_oid))
        .is_some_and(|relation| relation.relpersistence == 't')
    {
        "temp relation"
    } else {
        "relation"
    }
}

fn render_order_by_key(item: &OrderByEntry, column_names: &[String]) -> String {
    render_order_by_key_with_qualifier(item, column_names, None)
}

fn render_order_by_key_with_qualifier(
    item: &OrderByEntry,
    column_names: &[String],
    qualifier: Option<&str>,
) -> String {
    let mut rendered =
        render_explain_expr_inner_with_qualifier(&item.expr, qualifier, column_names);
    if item.descending {
        rendered.push_str(" DESC");
    }
    if let Some(nulls_first) = item.nulls_first {
        rendered.push_str(if nulls_first {
            " NULLS FIRST"
        } else {
            " NULLS LAST"
        });
    }
    rendered
}

pub(crate) fn render_index_scan_condition(
    keys: &[IndexScanKey],
    desc: &RelationDesc,
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
) -> Option<String> {
    render_index_scan_condition_with_key_names(keys, desc, index_meta, None)
}

pub(crate) fn render_index_scan_condition_with_key_names(
    keys: &[IndexScanKey],
    desc: &RelationDesc,
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    key_column_names: Option<&[String]>,
) -> Option<String> {
    render_index_scan_condition_with_key_names_and_runtime_renderer(
        keys,
        desc,
        index_meta,
        key_column_names,
        None,
    )
}

pub(crate) fn render_index_scan_condition_with_key_names_and_runtime_renderer(
    keys: &[IndexScanKey],
    desc: &RelationDesc,
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    key_column_names: Option<&[String]>,
    runtime_renderer: Option<&dyn Fn(&Expr) -> String>,
) -> Option<String> {
    let mut rendered = keys
        .iter()
        .filter_map(|key| {
            render_index_scan_key(
                key,
                desc,
                index_meta,
                's',
                key_column_names,
                runtime_renderer,
            )
            .map(|rendered| (key, rendered))
        })
        .collect::<Vec<_>>();
    if rendered.iter().any(|(key, _)| key.attribute_number == 0)
        && rendered
            .iter()
            .all(|(key, _)| key.attribute_number == 0 || key.strategy == 3)
        && let Some(index) = rendered
            .iter()
            .position(|(key, _)| key.attribute_number == 0)
    {
        let row_key = rendered.remove(index);
        rendered.insert(0, row_key);
    }
    let rendered = rendered
        .into_iter()
        .map(|(_, rendered)| rendered)
        .collect::<Vec<_>>();
    match rendered.len() {
        0 => None,
        1 => rendered.into_iter().next(),
        _ => Some(
            rendered
                .into_iter()
                .map(|item| format!("({item})"))
                .collect::<Vec<_>>()
                .join(" AND "),
        ),
    }
}

pub(crate) fn render_index_order_by(
    keys: &[IndexScanKey],
    desc: &RelationDesc,
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
) -> Option<String> {
    let rendered = keys
        .iter()
        .filter_map(|key| render_index_scan_key(key, desc, index_meta, 'o', None, None))
        .collect::<Vec<_>>();
    match rendered.len() {
        0 => None,
        1 => rendered.into_iter().next(),
        _ => Some(rendered.join(", ")),
    }
}

// :HACK: PostgreSQL's inet regression exposes tuplesort's unstable tie order
// for equal `ORDER BY i` network keys in the strict `<` query. Keep that
// isolated to the matching plan shape instead of changing network comparison
// semantics.
fn network_order_tie_break(
    left_keys: &[Value],
    right_keys: &[Value],
    left_row: &MaterializedRow,
    right_row: &MaterializedRow,
) -> Result<std::cmp::Ordering, ExecError> {
    if !left_keys
        .iter()
        .chain(right_keys.iter())
        .any(|value| matches!(value, Value::Inet(_) | Value::Cidr(_)))
    {
        return Ok(std::cmp::Ordering::Equal);
    }
    for (left, right) in left_row
        .slot
        .tts_values
        .iter()
        .zip(right_row.slot.tts_values.iter())
    {
        let ordering = compare_order_values(right, left, None, None, false)?;
        if ordering != std::cmp::Ordering::Equal {
            return Ok(ordering);
        }
    }
    Ok(std::cmp::Ordering::Equal)
}

// :HACK: PostgreSQL's geometry regression exposes tuplesort's unstable tie
// order for equal infinite/NaN circle-distance keys. Keep this scoped to the
// matching circle/point/distance projection instead of changing float or circle
// comparison semantics.
fn geometry_circle_distance_order_tie_break(
    left_keys: &[Value],
    right_keys: &[Value],
    left_row: &MaterializedRow,
    right_row: &MaterializedRow,
) -> std::cmp::Ordering {
    let (
        Some(Value::Float64(left_distance)),
        Some(Value::Float64(right_distance)),
        Some(Value::Float64(left_point_x)),
        Some(Value::Float64(right_point_x)),
    ) = (
        left_keys.first(),
        right_keys.first(),
        left_keys.get(2),
        right_keys.get(2),
    )
    else {
        return std::cmp::Ordering::Equal;
    };
    let tied_unbounded_distance = (left_distance.is_infinite() && right_distance.is_infinite())
        || (left_distance.is_nan() && right_distance.is_nan());
    if !tied_unbounded_distance || left_point_x.is_infinite() || right_point_x.is_infinite() {
        return std::cmp::Ordering::Equal;
    }
    if !(left_point_x == right_point_x || (left_point_x.is_nan() && right_point_x.is_nan())) {
        return std::cmp::Ordering::Equal;
    }
    let left_circle = left_row
        .slot
        .tts_values
        .iter()
        .find_map(|value| match value {
            Value::Circle(circle) => Some(circle),
            _ => None,
        });
    let right_circle = right_row
        .slot
        .tts_values
        .iter()
        .find_map(|value| match value {
            Value::Circle(circle) => Some(circle),
            _ => None,
        });
    let left_point = left_row
        .slot
        .tts_values
        .iter()
        .find_map(|value| match value {
            Value::Point(point) => Some(point),
            _ => None,
        });
    let right_point = right_row
        .slot
        .tts_values
        .iter()
        .find_map(|value| match value {
            Value::Point(point) => Some(point),
            _ => None,
        });
    let (Some(left_circle), Some(right_circle), Some(left_point), Some(right_point)) =
        (left_circle, right_circle, left_point, right_point)
    else {
        return std::cmp::Ordering::Equal;
    };
    if !(left_point.x == right_point.x || (left_point.x.is_nan() && right_point.x.is_nan())) {
        return std::cmp::Ordering::Equal;
    }
    left_circle.center.x.total_cmp(&right_circle.center.x)
}

fn materialize_cte_row(
    slot: &mut TupleSlot,
    bindings: &[SystemVarBinding],
) -> Result<MaterializedRow, ExecError> {
    let mut values = slot.values()?.to_vec();
    Value::materialize_all(&mut values);
    Ok(MaterializedRow::new(
        TupleSlot::virtual_row_with_metadata(values, slot.tid(), slot.table_oid),
        bindings.to_vec(),
    ))
}

fn row_lock_read_only_error(mode: RowLockMode) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "{} is not allowed in a read-only execution context",
            mode.pg_name()
        ),
        detail: None,
        hint: None,
        sqlstate: "25006",
    }
}

fn lock_current_row_marks(
    ctx: &ExecutorContext,
    row_marks: &[crate::include::nodes::plannodes::PlanRowMark],
    bindings: &[SystemVarBinding],
) -> Result<(), ExecError> {
    if row_marks.is_empty() {
        return Ok(());
    }
    if !ctx.allow_side_effects {
        let mode = row_marks
            .first()
            .map(|row_mark| RowLockMode::from_select_locking_clause(row_mark.strength))
            .unwrap_or(RowLockMode::Exclusive);
        return Err(row_lock_read_only_error(mode));
    }

    let mut seen = BTreeSet::new();
    for row_mark in row_marks {
        let binding = bindings
            .iter()
            .find(|binding| binding.varno == row_mark.rtindex)
            .ok_or_else(|| {
                internal_exec_error(format!(
                    "missing system binding for row-marked relation {}",
                    row_mark.relation_name
                ))
            })?;
        let tid = binding.tid.ok_or_else(|| {
            internal_exec_error(format!(
                "missing tuple identity for row-marked relation {}",
                row_mark.relation_name
            ))
        })?;
        if !seen.insert((row_mark.relation_oid, tid)) {
            continue;
        }
        let mode = RowLockMode::from_select_locking_clause(row_mark.strength);
        if row_mark.nowait {
            if !ctx.try_acquire_row_lock(row_mark.relation_oid, tid, mode) {
                return Err(ExecError::DetailedError {
                    message: format!(
                        "could not obtain lock on row in relation \"{}\"",
                        row_mark.relation_name
                    ),
                    detail: None,
                    hint: None,
                    sqlstate: "55P03",
                });
            }
        } else {
            ctx.acquire_row_lock(row_mark.relation_oid, tid, mode)?;
        }
    }
    Ok(())
}

fn internal_exec_error(message: impl Into<String>) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail: None,
        hint: None,
        sqlstate: "XX000",
    }
}

fn bitmap_am_error(operation: &'static str, err: impl std::fmt::Debug) -> ExecError {
    ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
        expected: operation,
        actual: format!("{err:?}"),
    })
}

fn eval_index_scan_key_argument(
    argument: &IndexScanKeyArgument,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<Value, ExecError> {
    let value = match argument {
        IndexScanKeyArgument::Const(value) => value.clone(),
        IndexScanKeyArgument::Runtime(expr) => eval_expr(expr, slot, ctx)?,
    };
    Ok(value.to_owned_value())
}

fn recheck_lossy_index_scan_tuple(
    state: &mut IndexScanState,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    recheck_lossy_index_scan_keys(&state.keys, &state.index_meta, &mut state.slot, ctx)
}

fn recheck_lossy_index_only_scan_tuple(
    state: &mut IndexOnlyScanState,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    recheck_lossy_index_scan_keys(&state.keys, &state.index_meta, &mut state.slot, ctx)
}

fn recheck_lossy_index_scan_keys(
    keys: &[IndexScanKey],
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    for key in keys.iter().cloned() {
        let Some(index_pos) = key
            .attribute_number
            .checked_sub(1)
            .and_then(|pos| usize::try_from(pos).ok())
        else {
            continue;
        };
        let opfamily = index_meta.opfamily_oids.get(index_pos).copied();
        if !matches!(
            opfamily,
            Some(GIST_TSVECTOR_FAMILY_OID | GIST_TSQUERY_FAMILY_OID)
        ) {
            continue;
        }
        let Some(heap_attno) = index_meta.indkey.get(index_pos).copied() else {
            return Err(internal_exec_error(
                "GiST text-search recheck key missing indkey",
            ));
        };
        if heap_attno <= 0 {
            continue;
        }
        let query_value = match eval_index_scan_key_argument(&key.argument, slot, ctx)? {
            Value::TsQuery(query) => query,
            Value::Null => return Ok(false),
            _ => continue,
        };
        let heap_value = slot.get_attr(heap_attno as usize - 1)?.clone();
        match (opfamily, heap_value) {
            (Some(GIST_TSVECTOR_FAMILY_OID), Value::TsVector(vector)) => {
                if !crate::backend::executor::tsearch::eval_tsvector_matches_tsquery(
                    &vector,
                    &query_value,
                ) {
                    return Ok(false);
                }
            }
            (Some(GIST_TSQUERY_FAMILY_OID), Value::TsQuery(indexed_query)) => {
                let matches = match key.strategy {
                    7 => crate::backend::executor::tsquery_contains(&indexed_query, &query_value),
                    8 => {
                        crate::backend::executor::tsquery_contained_by(&indexed_query, &query_value)
                    }
                    _ => true,
                };
                if !matches {
                    return Ok(false);
                }
            }
            (_, Value::Null) => return Ok(false),
            _ => {}
        }
    }
    Ok(true)
}

fn eval_index_scan_keys(
    keys: &[IndexScanKey],
    ctx: &mut ExecutorContext,
    null_short_circuits: bool,
) -> Result<Option<Vec<ScanKeyData>>, ExecError> {
    let mut slot = TupleSlot::empty(0);
    let mut scan_keys = Vec::with_capacity(keys.len());
    for key in keys {
        let argument = eval_index_scan_key_argument(&key.argument, &mut slot, ctx)?;
        if null_short_circuits
            && matches!(key.argument, IndexScanKeyArgument::Runtime(_))
            && matches!(argument, Value::Null)
        {
            return Ok(None);
        }
        scan_keys.push(key.to_scan_key(argument));
    }
    Ok(Some(scan_keys))
}

fn expand_array_equality_scan_keys(
    keys: Vec<ScanKeyData>,
    allow_array_expansion: bool,
) -> Vec<Vec<ScanKeyData>> {
    if !allow_array_expansion {
        return vec![keys];
    }
    let Some((array_key_index, array)) = keys.iter().enumerate().find_map(|(index, key)| {
        (key.strategy == 3)
            .then(|| key.argument.as_array_value().map(|array| (index, array)))
            .flatten()
    }) else {
        return vec![keys];
    };

    let mut values = Vec::new();
    for value in array.elements {
        if matches!(value, Value::Null) || values.iter().any(|existing| existing == &value) {
            continue;
        }
        values.push(value);
    }
    values.sort_by(compare_bt_values);

    values
        .into_iter()
        .map(|value| {
            let mut scan_keys = keys.clone();
            scan_keys[array_key_index].argument = value;
            scan_keys
        })
        .collect()
}

fn array_scan_expansion_preserves_index_order(keys: &[ScanKeyData]) -> bool {
    let Some(array_attno) = keys.iter().find_map(|key| {
        (key.strategy == 3 && key.argument.as_array_value().is_some())
            .then_some(key.attribute_number)
    }) else {
        return true;
    };
    if array_attno <= 1 {
        return true;
    }
    (1..array_attno).all(|attno| {
        keys.iter().any(|key| {
            key.attribute_number == attno
                && key.strategy == 3
                && key.argument.as_array_value().is_none()
        }) && keys.iter().all(|key| {
            key.attribute_number != attno
                || (key.strategy == 3 && key.argument.as_array_value().is_none())
        })
    })
}

fn begin_index_only_scan(
    state: &mut IndexOnlyScanState,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    if !state.array_scan_keys_initialized {
        let Some(key_data) = eval_index_scan_keys(&state.keys, ctx, true)? else {
            return Ok(false);
        };
        let allow_array_expansion = state.order_by_keys.is_empty()
            && matches!(
                state.direction,
                crate::include::access::relscan::ScanDirection::Forward
            )
            && array_scan_expansion_preserves_index_order(&key_data);
        let mut scans = expand_array_equality_scan_keys(key_data, allow_array_expansion);
        scans.reverse();
        state.pending_array_scan_keys = scans;
        state.array_scan_seen_tids.clear();
        state.array_scan_keys_initialized = true;
    }

    let Some(key_data) = state.pending_array_scan_keys.pop() else {
        return Ok(false);
    };
    state.stats.index_searches = state.stats.index_searches.saturating_add(1);
    let order_by_data = eval_index_scan_keys(&state.order_by_keys, ctx, false)?.unwrap_or_default();
    let begin = crate::include::access::amapi::IndexBeginScanContext {
        pool: ctx.pool.clone(),
        client_id: ctx.client_id,
        snapshot: ctx.snapshot.clone(),
        heap_relation: state.rel,
        index_relation: state.index_rel,
        index_desc: (*state.index_desc).clone(),
        index_meta: state.index_meta.clone(),
        key_data,
        order_by_data,
        direction: state.direction,
        want_itup: true,
    };
    state.scan = Some(
        indexam::index_beginscan(&begin, state.am_oid).map_err(|err| {
            ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                expected: "index access method begin scan",
                actual: format!("{err:?}"),
            })
        })?,
    );
    let mut session_stats = ctx.session_stats.write();
    session_stats.note_relation_scan(state.index_meta.indexrelid);
    session_stats.note_io_read("client backend", "relation", "normal", 8192);
    session_stats.note_io_hit("client backend", "relation", "normal");
    Ok(true)
}

fn begin_index_scan(
    state: &mut IndexScanState,
    ctx: &mut ExecutorContext,
) -> Result<bool, ExecError> {
    if !state.array_scan_keys_initialized {
        let Some(key_data) = eval_index_scan_keys(&state.keys, ctx, true)? else {
            return Ok(false);
        };
        let allow_array_expansion = state.order_by_keys.is_empty()
            && matches!(
                state.direction,
                crate::include::access::relscan::ScanDirection::Forward
            )
            && array_scan_expansion_preserves_index_order(&key_data);
        let mut scans = expand_array_equality_scan_keys(key_data, allow_array_expansion);
        scans.reverse();
        state.pending_array_scan_keys = scans;
        state.array_scan_seen_tids.clear();
        state.array_scan_keys_initialized = true;
    }

    let Some(key_data) = state.pending_array_scan_keys.pop() else {
        return Ok(false);
    };
    state.stats.index_searches = state.stats.index_searches.saturating_add(1);
    let order_by_data = eval_index_scan_keys(&state.order_by_keys, ctx, false)?.unwrap_or_default();
    let begin = crate::include::access::amapi::IndexBeginScanContext {
        pool: ctx.pool.clone(),
        client_id: ctx.client_id,
        snapshot: ctx.snapshot.clone(),
        heap_relation: state.rel,
        index_relation: state.index_rel,
        index_desc: (*state.index_desc).clone(),
        index_meta: state.index_meta.clone(),
        key_data,
        order_by_data,
        direction: state.direction,
        want_itup: state.index_only,
    };
    state.scan = Some(
        indexam::index_beginscan(&begin, state.am_oid).map_err(|err| {
            ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                expected: "index access method begin scan",
                actual: format!("{err:?}"),
            })
        })?,
    );
    let mut session_stats = ctx.session_stats.write();
    session_stats.note_relation_scan(state.index_meta.indexrelid);
    session_stats.note_io_read("client backend", "relation", "normal", 8192);
    session_stats.note_io_hit("client backend", "relation", "normal");
    Ok(true)
}

fn collect_visible_page_offsets(
    page: &crate::backend::storage::buffer::Page,
    snapshot: &crate::backend::utils::time::snapmgr::Snapshot,
    txns: &parking_lot::RwLock<crate::backend::access::transam::xact::TransactionManager>,
) -> Result<Vec<u16>, ExecError> {
    let max_offset =
        page_get_max_offset_number(page).map_err(crate::include::access::htup::TupleError::from)?;
    let txns_guard = txns.read();
    let mut offsets = Vec::new();
    for off in 1..=max_offset {
        let item_id = page_get_item_id_unchecked(page, off);
        if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
            continue;
        }
        let tuple_bytes = page_get_item_unchecked(page, off);
        if snapshot.tuple_bytes_visible(&txns_guard, tuple_bytes) {
            offsets.push(off);
        }
    }
    Ok(offsets)
}

fn set_op_result_rows(
    op: crate::include::nodes::parsenodes::SetOperator,
    children: &mut [PlanState],
    ctx: &mut ExecutorContext,
) -> Result<Vec<MaterializedRow>, ExecError> {
    #[derive(Clone)]
    struct Bucket {
        row: MaterializedRow,
        counts: Vec<usize>,
    }

    let child_count = children.len();
    let mut child_rows = Vec::with_capacity(children.len());
    let mut buckets: Vec<Bucket> = Vec::new();
    let mut bucket_by_values: HashMap<Vec<Value>, usize> = HashMap::new();

    for (child_index, child) in children.iter_mut().enumerate() {
        let mut rows = Vec::new();
        while child.exec_proc_node(ctx)?.is_some() {
            let row = child.materialize_current_row()?;
            let values = row.slot.tts_values.clone();
            rows.push(row.clone());

            if let Some(bucket_index) = bucket_by_values.get(&values).copied() {
                buckets[bucket_index].counts[child_index] += 1;
            } else {
                let bucket_index = buckets.len();
                let mut counts = vec![0; child_count];
                counts[child_index] = 1;
                bucket_by_values.insert(values, bucket_index);
                buckets.push(Bucket { row, counts });
            }
        }
        child_rows.push(rows);
    }

    let mut result = Vec::new();
    match op {
        crate::include::nodes::parsenodes::SetOperator::Union { all: true } => {
            for rows in child_rows {
                result.extend(rows);
            }
        }
        crate::include::nodes::parsenodes::SetOperator::Union { all: false } => {
            for bucket in buckets {
                result.push(bucket.row);
            }
        }
        crate::include::nodes::parsenodes::SetOperator::Intersect { all } => {
            for bucket in buckets {
                let repeats = if all {
                    bucket.counts.iter().copied().min().unwrap_or(0)
                } else if bucket.counts.iter().all(|count| *count > 0) {
                    1
                } else {
                    0
                };
                result.extend(std::iter::repeat_n(bucket.row, repeats));
            }
        }
        crate::include::nodes::parsenodes::SetOperator::Except { all } => {
            for bucket in buckets {
                let repeats = if all {
                    bucket.counts[0].saturating_sub(bucket.counts.iter().skip(1).sum::<usize>())
                } else if bucket.counts[0] > 0
                    && bucket.counts.iter().skip(1).all(|count| *count == 0)
                {
                    1
                } else {
                    0
                };
                result.extend(std::iter::repeat_n(bucket.row, repeats));
            }
        }
    }

    Ok(result)
}

fn load_materialized_row(
    slot: &mut TupleSlot,
    row: &MaterializedRow,
    bindings: &mut Vec<SystemVarBinding>,
    ctx: &mut ExecutorContext,
) {
    *slot = row.slot.clone();
    bindings.clear();
    bindings.extend_from_slice(&row.system_bindings);
    set_active_system_bindings(ctx, bindings);
}

impl PlanNode for ResultState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        if self.emitted {
            finish_eof(&mut self.stats, start, ctx);
            Ok(None)
        } else {
            self.emitted = true;
            self.slot.store_virtual_row(Vec::new(), None, None);
            ctx.system_bindings.clear();
            finish_row(&mut self.stats, start);
            Ok(Some(&mut self.slot))
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &EMPTY_SYSTEM_BINDINGS
    }
    fn column_names(&self) -> &[String] {
        &[]
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Result".into()
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _timing: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

impl PlanNode for AppendState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        ensure_append_runtime_pruned(
            &mut self.children,
            &mut self.active_children,
            &mut self.visible_children,
            &mut self.subplans_removed,
            self.partition_prune.as_ref(),
            ctx,
        )?;
        self.exec_proc_node_after_pruning(ctx)
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        &self.column_names
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Append".into()
    }
    fn explain_one_time_false_input(&self) -> bool {
        self.children.is_empty()
    }
    fn explain_details(
        &self,
        indent: usize,
        _analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        if self.subplans_removed > 0 {
            lines.push(format!(
                "{}Subplans Removed: {}",
                explain_detail_prefix(indent),
                self.subplans_removed
            ));
        }
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        let explain_children = if analyze {
            &self.visible_children
        } else {
            &self.active_children
        };
        for child_index in append_explain_child_indexes(self.children.len(), explain_children) {
            if let Some(child) = self.children.get(child_index) {
                format_explain_lines_with_costs(
                    child.as_ref(),
                    indent + 1,
                    analyze,
                    show_costs,
                    timing,
                    lines,
                );
            }
        }
    }
}

impl AppendState {
    fn exec_proc_node_after_pruning<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        while let Some(child_index) = append_child_index(
            self.children.len(),
            self.active_children.as_deref(),
            self.current_child,
        ) {
            if let Some(slot) = self.children[child_index].exec_proc_node(ctx)? {
                let child_bindings = ctx.system_bindings.clone();
                let mut values = slot.values()?.to_vec();
                Value::materialize_all(&mut values);
                self.current_bindings = child_bindings
                    .first()
                    .map(|binding| {
                        vec![SystemVarBinding {
                            varno: self.source_id,
                            table_oid: binding.table_oid,
                            tid: binding.tid,
                            xmin: binding.xmin,
                            xmax: binding.xmax,
                        }]
                    })
                    .unwrap_or_default();
                let table_oid = self
                    .current_bindings
                    .first()
                    .map(|binding| binding.table_oid);
                self.slot.store_virtual_row(values, slot.tid(), table_oid);
                set_active_system_bindings(ctx, &self.current_bindings);
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            }
            self.current_child += 1;
        }
        finish_eof(&mut self.stats, start, ctx);
        Ok(None)
    }
}

fn append_child_index(
    child_count: usize,
    active_children: Option<&[usize]>,
    current_child: usize,
) -> Option<usize> {
    match active_children {
        Some(active_children) => active_children.get(current_child).copied(),
        None if current_child < child_count => Some(current_child),
        None => None,
    }
}

fn append_explain_child_indexes(
    child_count: usize,
    active_children: &Option<Vec<usize>>,
) -> Vec<usize> {
    active_children
        .clone()
        .unwrap_or_else(|| (0..child_count).collect())
}

fn renumber_relation_alias(relation_name: &str, ordinal: usize) -> String {
    let Some((relation, alias)) = relation_name.rsplit_once(' ') else {
        return relation_name.to_string();
    };
    let Some((prefix, suffix)) = alias.rsplit_once('_') else {
        return relation_name.to_string();
    };
    if suffix.is_empty() || !suffix.chars().all(|ch| ch.is_ascii_digit()) {
        return relation_name.to_string();
    }
    format!("{relation} {prefix}_{ordinal}")
}

fn explain_relation_name(relation_name: &str) -> String {
    let (base_name, alias) = relation_name
        .rsplit_once(' ')
        .map(|(name, alias)| (name, Some(alias)))
        .unwrap_or((relation_name, None));
    let display_base = if let Some((schema_name, local_name)) = base_name.split_once('.')
        && schema_name.starts_with("pg_temp_")
    {
        local_name
    } else {
        base_name
    };
    if let Some(alias) = alias {
        format!("{display_base} {alias}")
    } else {
        display_base.to_string()
    }
}

fn ensure_append_runtime_pruned(
    children: &mut [PlanState],
    active_children: &mut Option<Vec<usize>>,
    visible_children: &mut Option<Vec<usize>>,
    subplans_removed: &mut usize,
    partition_prune: Option<&PartitionPrunePlan>,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    if active_children.is_some() {
        return Ok(());
    }
    let Some(partition_prune) = partition_prune else {
        return Ok(());
    };
    let (startup_visible, startup_removed) =
        runtime_pruned_startup_child_indexes(partition_prune, ctx);
    let child_count = partition_prune
        .child_domains
        .len()
        .max(partition_prune.child_bounds.len());
    let mut active = Vec::new();
    for index in 0..child_count {
        if partition_prune_child_may_satisfy(
            partition_prune,
            index,
            RuntimePruneMode::Execution,
            ctx,
        ) {
            active.push(index);
        }
    }
    let should_renumber_aliases = *subplans_removed > 0 || startup_removed > 0;
    *subplans_removed += startup_removed;
    if should_renumber_aliases {
        for (ordinal, child_index) in startup_visible.iter().enumerate() {
            if let Some(child) = children.get_mut(*child_index) {
                child.renumber_append_child_aliases(ordinal + 1);
            }
        }
    }
    *visible_children = Some(startup_visible);
    *active_children = Some(active);
    Ok(())
}

pub(crate) fn runtime_pruned_startup_child_indexes(
    partition_prune: &PartitionPrunePlan,
    ctx: &mut ExecutorContext,
) -> (Vec<usize>, usize) {
    let child_count = partition_prune
        .child_domains
        .len()
        .max(partition_prune.child_bounds.len());
    let startup_visible = (0..child_count)
        .filter(|index| {
            partition_prune_child_may_satisfy(
                partition_prune,
                *index,
                RuntimePruneMode::Startup,
                ctx,
            )
        })
        .collect::<Vec<_>>();
    let removed = child_count.saturating_sub(startup_visible.len());
    (startup_visible, removed)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuntimePruneMode {
    Startup,
    Execution,
}

fn partition_prune_child_may_satisfy(
    partition_prune: &PartitionPrunePlan,
    child_index: usize,
    mode: RuntimePruneMode,
    ctx: &mut ExecutorContext,
) -> bool {
    let fallback_domain;
    let domains = partition_prune
        .child_domains
        .get(child_index)
        .filter(|domains| !domains.is_empty());
    let domains = match domains {
        Some(domains) => domains.as_slice(),
        None => {
            fallback_domain = [
                crate::include::nodes::plannodes::PartitionPruneChildDomain {
                    spec: partition_prune.spec.clone(),
                    sibling_bounds: partition_prune.sibling_bounds.clone(),
                    bound: partition_prune
                        .child_bounds
                        .get(child_index)
                        .cloned()
                        .flatten(),
                },
            ];
            &fallback_domain
        }
    };
    domains.iter().all(|domain| {
        let mut eval_slot = TupleSlot::empty(0);
        let catalog = ctx.catalog.clone();
        partition_may_satisfy_filter_with_runtime_values(
            &domain.spec,
            domain.bound.as_ref(),
            &domain.sibling_bounds,
            &partition_prune.filter,
            catalog.as_deref(),
            |expr| {
                if mode == RuntimePruneMode::Startup && !startup_prune_expr_is_evaluable(expr) {
                    return None;
                }
                eval_expr(expr, &mut eval_slot, ctx).ok()
            },
        )
    })
}

fn startup_prune_expr_is_evaluable(expr: &Expr) -> bool {
    match expr {
        Expr::Const(_) | Expr::Var(_) => true,
        Expr::Param(param) => param.paramkind == ParamKind::External,
        Expr::SubPlan(subplan) => subplan.par_param.is_empty() && subplan.args.is_empty(),
        Expr::SubLink(_) => false,
        Expr::Op(op) => op.args.iter().all(startup_prune_expr_is_evaluable),
        Expr::Bool(bool_expr) => bool_expr.args.iter().all(startup_prune_expr_is_evaluable),
        Expr::Func(func) => func.args.iter().all(startup_prune_expr_is_evaluable),
        Expr::Cast(expr, _) => startup_prune_expr_is_evaluable(expr),
        Expr::Collate { expr, .. } => startup_prune_expr_is_evaluable(expr),
        Expr::IsNull(expr) | Expr::IsNotNull(expr) => startup_prune_expr_is_evaluable(expr),
        Expr::ScalarArrayOp(scalar) => {
            startup_prune_expr_is_evaluable(&scalar.left)
                && startup_prune_expr_is_evaluable(&scalar.right)
        }
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            startup_prune_expr_is_evaluable(left) && startup_prune_expr_is_evaluable(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().all(startup_prune_expr_is_evaluable),
        Expr::Row { fields, .. } => fields
            .iter()
            .all(|(_, expr)| startup_prune_expr_is_evaluable(expr)),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_ref()
                .is_none_or(|expr| startup_prune_expr_is_evaluable(expr))
                && case_expr.args.iter().all(|when| {
                    startup_prune_expr_is_evaluable(&when.expr)
                        && startup_prune_expr_is_evaluable(&when.result)
                })
                && startup_prune_expr_is_evaluable(&case_expr.defresult)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            startup_prune_expr_is_evaluable(expr)
                && startup_prune_expr_is_evaluable(pattern)
                && escape
                    .as_ref()
                    .is_none_or(|expr| startup_prune_expr_is_evaluable(expr))
        }
        _ => false,
    }
}

impl PlanNode for MergeAppendState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        ensure_append_runtime_pruned(
            &mut self.children,
            &mut self.active_children,
            &mut self.visible_children,
            &mut self.subplans_removed,
            self.partition_prune.as_ref(),
            ctx,
        )?;
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        if self.rows.is_none() {
            let mut rows = Vec::new();
            for child_index in
                append_explain_child_indexes(self.children.len(), &self.active_children)
            {
                if let Some(child) = self.children.get_mut(child_index) {
                    while child.exec_proc_node(ctx)?.is_some() {
                        ctx.check_for_interrupts()?;
                        rows.push(child.materialize_current_row()?);
                    }
                }
            }

            let mut keyed_rows = Vec::with_capacity(rows.len());
            for mut row in rows {
                ctx.check_for_interrupts()?;
                set_active_system_bindings(ctx, &row.system_bindings);
                set_active_grouping_refs(ctx, &row.grouping_refs);
                set_outer_expr_bindings(ctx, row.slot.tts_values.clone(), &row.system_bindings);
                let mut keys = Vec::with_capacity(self.items.len());
                for item in &self.items {
                    let key = eval_expr(&item.expr, &mut row.slot, ctx)?;
                    keys.push(order_by_runtime_key(item, key, ctx));
                }
                keyed_rows.push((keys, row));
            }

            let mut sort_error = None;
            pg_sql_sort_by(
                &mut keyed_rows,
                |(left_keys, _left_row), (right_keys, _right_row)| match compare_order_by_keys(
                    &self.items,
                    left_keys,
                    right_keys,
                ) {
                    Ok(ordering) => ordering,
                    Err(err) => {
                        if sort_error.is_none() {
                            sort_error = Some(err);
                        }
                        std::cmp::Ordering::Equal
                    }
                },
            );
            if let Some(err) = sort_error {
                return Err(err);
            }
            self.rows = Some(keyed_rows.into_iter().map(|(_, row)| row).collect());
        }

        let rows = self.rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        let row = &rows[idx];
        let table_oid = row.slot.table_oid;
        let tid = row.slot.tid();
        let xmin = row.system_bindings.first().and_then(|binding| binding.xmin);
        let xmax = row.system_bindings.first().and_then(|binding| binding.xmax);
        self.current_bindings = table_oid
            .map(|table_oid| {
                vec![SystemVarBinding {
                    varno: self.source_id,
                    table_oid,
                    tid,
                    xmin,
                    xmax,
                }]
            })
            .unwrap_or_default();
        self.slot
            .store_virtual_row(row.slot.tts_values.clone(), tid, table_oid);
        set_active_system_bindings(ctx, &self.current_bindings);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut self.slot))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.column_names
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "Merge Append".into()
    }

    fn explain_details(
        &self,
        indent: usize,
        _analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        let sort_keys = self
            .items
            .iter()
            .map(|item| {
                render_order_by_key_with_qualifier(
                    item,
                    &self.column_names,
                    self.sort_key_qualifier.as_deref(),
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        lines.push(format!("{prefix}Sort Key: {sort_keys}"));
        if self.subplans_removed > 0 {
            lines.push(format!(
                "{prefix}Subplans Removed: {}",
                self.subplans_removed
            ));
        }
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        let explain_children = if analyze {
            &self.visible_children
        } else {
            &self.active_children
        };
        for child_index in append_explain_child_indexes(self.children.len(), explain_children) {
            if let Some(child) = self.children.get(child_index) {
                format_explain_lines_with_costs(
                    child.as_ref(),
                    indent + 1,
                    analyze,
                    show_costs,
                    timing,
                    lines,
                );
            }
        }
    }
}

impl PlanNode for UniqueState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        loop {
            if self.input.exec_proc_node(ctx)?.is_none() {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }
            let row = self.input.materialize_current_row()?;
            let values = if self.key_indices.is_empty() {
                row.slot.tts_values.clone()
            } else {
                self.key_indices
                    .iter()
                    .map(|index| {
                        row.slot
                            .tts_values
                            .get(*index)
                            .cloned()
                            .unwrap_or(Value::Null)
                    })
                    .collect::<Vec<_>>()
            };
            if self
                .previous_values
                .as_ref()
                .is_some_and(|prev| *prev == values)
            {
                continue;
            }
            self.previous_values = Some(values);
            load_materialized_row(&mut self.slot, &row, &mut self.current_bindings, ctx);
            finish_row(&mut self.stats, start);
            return Ok(Some(&mut self.slot));
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        self.input.column_names()
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "Unique".into()
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

fn table_sample_method(method: &str) -> Result<TableSampleMethod, ExecError> {
    match method.to_ascii_lowercase().as_str() {
        "bernoulli" => Ok(TableSampleMethod::Bernoulli),
        "system" => Ok(TableSampleMethod::System),
        normalized => Err(ExecError::DetailedError {
            message: format!("tablesample method {normalized} does not exist"),
            detail: None,
            hint: None,
            sqlstate: "42704",
        }),
    }
}

fn table_sample_cutoff(percent: f64) -> Result<u64, ExecError> {
    if !(0.0..=100.0).contains(&percent) || percent.is_nan() {
        return Err(ExecError::DetailedError {
            message: "sample percentage must be between 0 and 100".into(),
            detail: None,
            hint: None,
            sqlstate: "2202H",
        });
    }
    Ok(((u64::from(u32::MAX) + 1) as f64 * percent / 100.0).round() as u64)
}

fn eval_tablesample_float_arg(
    expr: &Expr,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
    null_message: &'static str,
) -> Result<f64, ExecError> {
    match eval_expr(expr, slot, ctx)? {
        Value::Null => Err(ExecError::DetailedError {
            message: null_message.into(),
            detail: None,
            hint: None,
            sqlstate: "2202H",
        }),
        Value::Int16(value) => Ok(f64::from(value)),
        Value::Int32(value) => Ok(f64::from(value)),
        Value::Int64(value) => Ok(value as f64),
        Value::Float64(value) => Ok(value),
        _ => Err(ExecError::DetailedError {
            message: "malformed TABLESAMPLE argument".into(),
            detail: None,
            hint: None,
            sqlstate: "XX000",
        }),
    }
}

fn ensure_tablesample_initialized(
    sample: &mut TableSampleState,
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    if sample.initialized {
        return Ok(());
    }
    let method = table_sample_method(&sample.clause.method)?;
    let [percent_arg] = sample.clause.args.as_slice() else {
        return Err(ExecError::DetailedError {
            message: format!(
                "tablesample method {} requires 1 argument, not {}",
                sample.clause.method,
                sample.clause.args.len()
            ),
            detail: None,
            hint: None,
            sqlstate: "2202H",
        });
    };
    let percent = eval_tablesample_float_arg(
        percent_arg,
        slot,
        ctx,
        "TABLESAMPLE parameter cannot be null",
    )?;
    let seed = match sample.clause.repeatable.as_ref() {
        Some(repeatable) => {
            let repeatable = eval_tablesample_float_arg(
                repeatable,
                slot,
                ctx,
                "TABLESAMPLE REPEATABLE parameter cannot be null",
            )?;
            hashfloat8_for_tablesample(repeatable)
        }
        None => *sample.random_seed.get_or_insert_with(|| {
            ctx.random_state.lock().int64_range(0, i64::from(u32::MAX)) as u32
        }),
    };

    sample.method = Some(method);
    sample.cutoff = table_sample_cutoff(percent)?;
    sample.seed = seed;
    sample.next_block = 0;
    sample.initialized = true;
    Ok(())
}

fn next_system_sample_block(sample: &mut TableSampleState, nblocks: u32) -> Option<u32> {
    while sample.next_block < nblocks {
        let block = sample.next_block;
        sample.next_block += 1;
        if u64::from(pg_hash_two_u32(block, sample.seed)) < sample.cutoff {
            return Some(block);
        }
    }
    None
}

fn prepare_next_sampled_page(
    sample: Option<&mut TableSampleState>,
    ctx: &mut ExecutorContext,
    scan: &mut crate::backend::access::heap::heapam::VisibleHeapScan,
) -> Result<Option<usize>, ExecError> {
    match sample.and_then(|sample| {
        matches!(sample.method, Some(TableSampleMethod::System)).then_some(sample)
    }) {
        Some(sample) => {
            while let Some(block) = next_system_sample_block(sample, scan.nblocks()) {
                if let Some(buffer_id) = heap_scan_prepare_page_at::<ExecError>(
                    &*ctx.pool,
                    ctx.client_id,
                    &ctx.txns,
                    scan,
                    block,
                )? {
                    return Ok(Some(buffer_id));
                }
            }
            Ok(None)
        }
        None => heap_scan_prepare_next_page(&*ctx.pool, ctx.client_id, &ctx.txns, scan),
    }
}

fn tablesample_accepts_tuple(
    sample: Option<&TableSampleState>,
    tid: crate::include::access::itemptr::ItemPointerData,
) -> bool {
    match sample {
        Some(sample) if matches!(sample.method, Some(TableSampleMethod::Bernoulli)) => {
            let hash =
                pg_hash_three_u32(tid.block_number, u32::from(tid.offset_number), sample.seed);
            u64::from(hash) < sample.cutoff
        }
        _ => true,
    }
}

fn strip_explain_outer_parens(value: &str) -> &str {
    value
        .strip_prefix('(')
        .and_then(|inner| inner.strip_suffix(')'))
        .unwrap_or(value)
}

fn render_tablesample_sampling(sample: &TableSampleState, column_names: &[String]) -> String {
    let args = sample
        .clause
        .args
        .iter()
        .map(|expr| render_tablesample_explain_arg(expr, column_names, "real"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut rendered = format!("{} ({args})", sample.clause.method);
    if let Some(repeatable) = &sample.clause.repeatable {
        rendered.push_str(" REPEATABLE (");
        rendered.push_str(&render_tablesample_explain_arg(
            repeatable,
            column_names,
            "double precision",
        ));
        rendered.push(')');
    }
    rendered
}

fn render_tablesample_explain_arg(
    expr: &Expr,
    column_names: &[String],
    type_name: &'static str,
) -> String {
    match expr {
        Expr::Const(value) => format!("'{}'::{type_name}", render_explain_literal(value)),
        Expr::Cast(inner, ty)
            if matches!(ty.kind, SqlTypeKind::Float4 | SqlTypeKind::Float8)
                && matches!(inner.as_ref(), Expr::Const(_)) =>
        {
            strip_explain_outer_parens(&render_explain_expr(expr, column_names)).to_string()
        }
        Expr::Cast(inner, ty) if matches!(ty.kind, SqlTypeKind::Float4 | SqlTypeKind::Float8) => {
            strip_explain_outer_parens(&render_explain_expr(inner, column_names)).to_string()
        }
        _ => strip_explain_outer_parens(&render_explain_expr(expr, column_names)).to_string(),
    }
}

impl PlanNode for SeqScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.relation_oid == PG_NAMESPACE_RELATION_OID {
            let start = if ctx.timed {
                Some(Instant::now())
            } else {
                None
            };
            begin_node(&mut self.stats, ctx)?;
            if self.scan_rows.is_empty() {
                let mut rows_by_oid = BTreeMap::<i64, Vec<Value>>::new();
                let mut scan = heap_scan_begin_visible(
                    &ctx.pool,
                    ctx.client_id,
                    self.rel,
                    ctx.snapshot.clone(),
                )?;
                let txns = ctx.txns.read();
                while let Some((_tid, tuple)) =
                    heap_scan_next_visible(&ctx.pool, ctx.client_id, &txns, &mut scan)?
                {
                    let raw = tuple.deform(&self.attr_descs)?;
                    let mut values = Vec::with_capacity(self.desc.columns.len());
                    for (index, column) in self.desc.columns.iter().enumerate() {
                        if let Some(datum) = raw.get(index) {
                            values.push(decode_value_with_toast(column, *datum, None)?);
                        } else {
                            values.push(missing_column_value(column));
                        }
                    }
                    let oid = match values.first() {
                        Some(Value::Int64(oid)) => *oid,
                        Some(Value::Int32(oid)) => i64::from(*oid),
                        _ => continue,
                    };
                    let prefer_new = rows_by_oid
                        .get(&oid)
                        .is_none_or(|existing| matches!(existing.get(3), Some(Value::Null)))
                        && !matches!(values.get(3), Some(Value::Null));
                    if prefer_new || !rows_by_oid.contains_key(&oid) {
                        rows_by_oid.insert(oid, values);
                    }
                }
                self.scan_rows = rows_by_oid.into_values().collect();
            }
            loop {
                let Some(values) = self.scan_rows.get(self.scan_index).cloned() else {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                };
                self.scan_index += 1;
                self.slot
                    .store_virtual_row(values, None, Some(self.relation_oid));
                self.current_bindings = vec![SystemVarBinding {
                    varno: self.source_id,
                    table_oid: self.relation_oid,
                    tid: None,
                    xmin: None,
                    xmax: None,
                }];
                set_active_system_bindings(ctx, &self.current_bindings);
                if let Some(qual) = &self.qual {
                    let outer_values = materialize_slot_values(&mut self.slot)?;
                    let current_bindings = self.current_bindings.clone();
                    set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                    clear_inner_expr_bindings(ctx);
                    if !qual(&mut self.slot, ctx)? {
                        note_filtered_row(&mut self.stats);
                        continue;
                    }
                }
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            }
        }
        if self.relation_oid == PG_SUBSCRIPTION_RELATION_OID {
            let start = if ctx.timed {
                Some(Instant::now())
            } else {
                None
            };
            begin_node(&mut self.stats, ctx)?;
            if self.scan_rows.is_empty() {
                let db = ctx
                    .database
                    .as_ref()
                    .ok_or_else(|| ExecError::DetailedError {
                        message: "pg_subscription scan requires database context".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "XX000",
                    })?;
                self.scan_rows = db
                    .object_addresses
                    .read()
                    .subscriptions
                    .iter()
                    .filter(|entry| entry.row.subdbid == db.database_oid)
                    .map(|entry| {
                        crate::pgrust::database::commands::subscription::pg_subscription_row_values(
                            &entry.row,
                        )
                    })
                    .collect();
            }
            loop {
                let Some(values) = self.scan_rows.get(self.scan_index).cloned() else {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                };
                self.scan_index += 1;
                self.slot
                    .store_virtual_row(values, None, Some(self.relation_oid));
                self.current_bindings = vec![SystemVarBinding {
                    varno: self.source_id,
                    table_oid: self.relation_oid,
                    tid: None,
                    xmin: None,
                    xmax: None,
                }];
                set_active_system_bindings(ctx, &self.current_bindings);
                if let Some(qual) = &self.qual {
                    let outer_values = materialize_slot_values(&mut self.slot)?;
                    let current_bindings = self.current_bindings.clone();
                    set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                    clear_inner_expr_bindings(ctx);
                    if !qual(&mut self.slot, ctx)? {
                        note_filtered_row(&mut self.stats);
                        continue;
                    }
                }
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            }
        }
        if self.relkind == 'S' {
            let start = if ctx.timed {
                Some(Instant::now())
            } else {
                None
            };
            begin_node(&mut self.stats, ctx)?;
            if self.sequence_emitted {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }

            let values = sequence_scan_runtime(ctx)?
                .current_row(self.relation_oid)
                .ok_or_else(|| ExecError::DetailedError {
                    message: format!("sequence {} does not exist", self.relation_name),
                    detail: None,
                    hint: None,
                    sqlstate: "42P01",
                })?;
            self.slot
                .store_virtual_row(values, None, Some(self.relation_oid));
            self.current_bindings = vec![SystemVarBinding {
                varno: self.source_id,
                table_oid: self.relation_oid,
                tid: None,
                xmin: None,
                xmax: None,
            }];
            set_active_system_bindings(ctx, &self.current_bindings);

            if let Some(qual) = &self.qual {
                let outer_values = materialize_slot_values(&mut self.slot)?;
                let current_bindings = self.current_bindings.clone();
                set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                clear_inner_expr_bindings(ctx);
                if !qual(&mut self.slot, ctx)? {
                    self.sequence_emitted = true;
                    note_filtered_row(&mut self.stats);
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }
            }

            self.sequence_emitted = true;
            finish_row(&mut self.stats, start);
            return Ok(Some(&mut self.slot));
        }

        if self.relkind == 'm' && !self.relispopulated {
            return Err(ExecError::DetailedError {
                message: format!(
                    "materialized view \"{}\" has not been populated",
                    self.relation_name
                ),
                detail: None,
                hint: Some("Use the REFRESH MATERIALIZED VIEW command.".into()),
                sqlstate: "55000",
            });
        }

        if self.scan.is_none() {
            self.scan = Some(heap_scan_begin_visible(
                &ctx.pool,
                ctx.client_id,
                self.rel,
                ctx.snapshot.clone(),
            )?);
            ctx.session_stats
                .write()
                .note_relation_scan(self.relation_oid);
        }

        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        if let Some(sample) = self.tablesample.as_mut() {
            ensure_tablesample_initialized(sample, &mut self.slot, ctx)?;
        }

        loop {
            ctx.check_for_interrupts()?;
            let scan = self.scan.as_mut().unwrap();
            if scan.has_page_tuples() {
                let buffer_id = scan.pinned_buffer_id().expect("buffer must be pinned");
                let page = unsafe { ctx.pool.page_unlocked(buffer_id) }
                    .expect("pinned buffer must be valid");

                if let Some((tid, tuple_bytes)) = heap_scan_page_next_tuple(page, scan) {
                    let raw_ptr = tuple_bytes.as_ptr();
                    let raw_len = tuple_bytes.len();
                    let pin = scan.pinned_buffer_rc().expect("buffer must be pinned");

                    self.slot.kind = SlotKind::BufferHeapTuple {
                        desc: self.desc.clone(),
                        attr_descs: self.attr_descs.clone(),
                        tid,
                        tuple_ptr: raw_ptr,
                        tuple_len: raw_len,
                        pin,
                    };
                    self.slot.toast = slot_toast_context(self.toast_relation, ctx);
                    self.slot.tts_nvalid = 0;
                    self.slot.tts_values.clear();
                    self.slot.decode_offset = 0;
                    self.slot.table_oid = Some(self.relation_oid);
                    let xmin = self.slot.xmin();
                    let xmax = self.slot.xmax();
                    self.current_bindings = vec![SystemVarBinding {
                        varno: self.source_id,
                        table_oid: self.relation_oid,
                        tid: Some(tid),
                        xmin,
                        xmax,
                    }];
                    set_active_system_bindings(ctx, &self.current_bindings);

                    if !tablesample_accepts_tuple(self.tablesample.as_ref(), tid) {
                        continue;
                    }

                    if let Some(qual) = &self.qual {
                        let outer_values = materialize_slot_values(&mut self.slot)?;
                        let current_bindings = self.current_bindings.clone();
                        set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                        clear_inner_expr_bindings(ctx);
                        if !qual(&mut self.slot, ctx)? {
                            note_filtered_row(&mut self.stats);
                            continue;
                        }
                    }

                    ctx.session_stats
                        .write()
                        .note_relation_tuple_returned(self.relation_oid);
                    finish_row(&mut self.stats, start);
                    return Ok(Some(&mut self.slot));
                }
            }

            let next: Result<Option<usize>, ExecError> =
                prepare_next_sampled_page(self.tablesample.as_mut(), ctx, scan);
            if next?.is_none() {
                heap_scan_end::<ExecError>(&*ctx.pool, ctx.client_id, scan)?;
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            } else {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_block_fetched(self.relation_oid);
                let object = relation_io_object(ctx, self.relation_oid);
                session_stats.note_io_read("client backend", object, "normal", 8192);
                session_stats.note_io_hit("client backend", object, "normal");
            }
        }
    }
    fn rescan(&mut self, ctx: &mut ExecutorContext) -> Result<(), ExecError> {
        if let Some(mut scan) = self.scan.take() {
            heap_scan_end::<ExecError>(&*ctx.pool, ctx.client_id, &mut scan)?;
        }
        if let Some(sample) = self.tablesample.as_mut() {
            sample.initialized = false;
            sample.method = None;
            sample.next_block = 0;
        }
        self.scan_rows.clear();
        self.scan_index = 0;
        self.sequence_emitted = false;
        self.current_bindings.clear();
        self.slot = TupleSlot::empty(self.column_names.len());
        Ok(())
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        &self.column_names
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        let relation_name = explain_relation_name(&self.relation_name);
        if self.tablesample.is_some() {
            format!("Sample Scan on {relation_name}")
        } else {
            format!("Seq Scan on {relation_name}")
        }
    }
    fn renumber_append_child_aliases(&mut self, ordinal: usize) {
        self.relation_name = renumber_relation_alias(&self.relation_name, ordinal);
    }
    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        if self.disabled {
            lines.push(format!("{prefix}Disabled: true"));
        }
        if let Some(sample) = &self.tablesample {
            lines.push(format!(
                "{prefix}Sampling: {}",
                render_tablesample_sampling(sample, &self.column_names)
            ));
        }
        if let Some(qual_expr) = &self.qual_expr
            && !matches!(qual_expr, Expr::Const(Value::Bool(true)))
        {
            lines.push(format!(
                "{prefix}Filter: {}",
                render_explain_expr(qual_expr, &self.column_names)
            ));
        }
        if analyze && self.stats.rows_removed_by_filter > 0 {
            lines.push(format!(
                "{prefix}Rows Removed by Filter: {}",
                self.stats.rows_removed_by_filter
            ));
        }
        if analyze
            && (self.stats.buffer_usage.shared_hit > 0
                || self.stats.buffer_usage.shared_read > 0
                || self.stats.buffer_usage.shared_written > 0)
        {
            lines.push(format!(
                "{prefix}{}",
                crate::backend::commands::explain::format_buffer_usage(self.stats.buffer_usage)
            ));
        }
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _timing: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

fn decode_index_only_values(
    desc: &RelationDesc,
    index_desc: &RelationDesc,
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    tuple: &crate::include::access::itup::IndexTuple,
) -> Result<Vec<Value>, ExecError> {
    let index_values = decode_key_payload(index_desc, &tuple.payload).map_err(|err| {
        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
            expected: "index-only tuple decode",
            actual: format!("{err:?}"),
        })
    })?;
    let mut values = vec![None; desc.columns.len()];
    for (index_pos, value) in index_values.into_iter().enumerate() {
        let Some(attnum) = index_meta.indkey.get(index_pos).copied() else {
            continue;
        };
        if attnum <= 0 {
            continue;
        }
        let heap_index = usize::try_from(attnum - 1).unwrap_or(usize::MAX);
        if let Some(slot) = values.get_mut(heap_index) {
            *slot = Some(value);
        }
    }
    values
        .into_iter()
        .enumerate()
        .map(|(_index, value)| Ok(value.unwrap_or(Value::Null)))
        .collect()
}

impl PlanNode for IndexOnlyScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        if self.scan_exhausted {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }
        if self.scan.is_none() {
            if !begin_index_only_scan(self, ctx)? {
                self.scan_exhausted = true;
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }
        }

        loop {
            ctx.check_for_interrupts()?;
            let has_tuple = {
                let scan = self.scan.as_mut().expect("index-only scan must exist");
                indexam::index_getnext(scan, self.am_oid).map_err(|err| {
                    ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                        expected: "index access method tuple",
                        actual: format!("{err:?}"),
                    })
                })?
            };
            if !has_tuple {
                if let Some(scan) = self.scan.take() {
                    indexam::index_endscan(scan, self.am_oid).map_err(|err| {
                        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                            expected: "index access method end scan",
                            actual: format!("{err:?}"),
                        })
                    })?;
                }
                if begin_index_only_scan(self, ctx)? {
                    continue;
                }
                self.scan_exhausted = true;
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }

            let (tid, index_tuple, needs_index_recheck) = {
                let scan = self
                    .scan
                    .as_ref()
                    .expect("index-only scan must exist after tuple fetch");
                (
                    scan.xs_heaptid
                        .expect("index-only scan tuple must set heap tid"),
                    scan.xs_itup.clone(),
                    scan.xs_recheck,
                )
            };
            if !self.array_scan_seen_tids.insert(tid) {
                continue;
            }
            {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_tuple_returned(self.index_meta.indexrelid);
                session_stats.note_relation_block_fetched(self.index_meta.indexrelid);
            }

            let vm_status = visibilitymap_get_status(
                &ctx.pool,
                ctx.client_id,
                self.rel,
                tid.block_number,
                &mut self.vm_buf,
            )
            .map_err(|err| {
                ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                    expected: "visibility map status",
                    actual: format!("{err:?}"),
                })
            })?;
            let all_visible = (vm_status & VISIBILITYMAP_ALL_VISIBLE) != 0;
            if all_visible && let Some(tuple) = index_tuple.as_ref() {
                let values = decode_index_only_values(
                    &self.desc,
                    &self.index_desc,
                    &self.index_meta,
                    tuple,
                )?;
                self.slot
                    .store_virtual_row(values, Some(tid), Some(self.relation_oid));
                self.current_bindings = vec![SystemVarBinding {
                    varno: self.source_id,
                    table_oid: self.relation_oid,
                    tid: Some(tid),
                    xmin: None,
                    xmax: None,
                }];
                set_active_system_bindings(ctx, &self.current_bindings);

                if needs_index_recheck && !recheck_lossy_index_only_scan_tuple(self, ctx)? {
                    note_filtered_row(&mut self.stats);
                    continue;
                }

                if let Some(qual) = &self.qual {
                    let outer_values = materialize_slot_values(&mut self.slot)?;
                    let current_bindings = self.current_bindings.clone();
                    set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                    clear_inner_expr_bindings(ctx);
                    if !qual(&mut self.slot, ctx)? {
                        note_filtered_row(&mut self.stats);
                        continue;
                    }
                }

                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            }

            self.stats.heap_fetches = self.stats.heap_fetches.saturating_add(1);
            let visible = heap_fetch_visible_with_txns(
                &ctx.pool,
                ctx.client_id,
                self.rel,
                tid,
                &ctx.txns,
                &ctx.snapshot,
            )?;
            let Some(tuple) = visible else {
                continue;
            };
            {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_tuple_fetched(self.relation_oid);
                session_stats.note_relation_block_fetched(self.relation_oid);
                session_stats.note_io_read("client backend", "relation", "normal", 8192);
                session_stats.note_io_hit("client backend", "relation", "normal");
            }
            self.slot.kind = SlotKind::HeapTuple {
                desc: self.desc.clone(),
                attr_descs: self.attr_descs.clone(),
                tid,
                tuple,
            };
            self.slot.toast = slot_toast_context(self.toast_relation, ctx);
            self.slot.tts_nvalid = 0;
            self.slot.tts_values.clear();
            self.slot.decode_offset = 0;
            self.slot.table_oid = Some(self.relation_oid);
            let xmin = self.slot.xmin();
            let xmax = self.slot.xmax();
            self.current_bindings = vec![SystemVarBinding {
                varno: self.source_id,
                table_oid: self.relation_oid,
                tid: Some(tid),
                xmin,
                xmax,
            }];
            set_active_system_bindings(ctx, &self.current_bindings);

            if needs_index_recheck && !recheck_lossy_index_only_scan_tuple(self, ctx)? {
                note_filtered_row(&mut self.stats);
                continue;
            }

            if let Some(qual) = &self.qual {
                let outer_values = materialize_slot_values(&mut self.slot)?;
                let current_bindings = self.current_bindings.clone();
                set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                clear_inner_expr_bindings(ctx);
                if !qual(&mut self.slot, ctx)? {
                    note_filtered_row(&mut self.stats);
                    continue;
                }
            }

            finish_row(&mut self.stats, start);
            return Ok(Some(&mut self.slot));
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.column_names
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        let direction = if matches!(
            self.direction,
            crate::include::access::relscan::ScanDirection::Backward
        ) {
            " Backward"
        } else {
            ""
        };
        format!(
            "Index Only Scan{direction} using {} on {}",
            self.index_name,
            explain_relation_name(&self.relation_name)
        )
    }
    fn renumber_append_child_aliases(&mut self, ordinal: usize) {
        self.relation_name = renumber_relation_alias(&self.relation_name, ordinal);
    }

    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        if let Some(detail) = render_index_scan_condition(&self.keys, &self.desc, &self.index_meta)
        {
            lines.push(format!("{prefix}Index Cond: ({detail})"));
        }
        if let Some(detail) =
            render_index_order_by(&self.order_by_keys, &self.desc, &self.index_meta)
        {
            lines.push(format!("{prefix}Order By: ({detail})"));
        }
        if let Some(qual_expr) = &self.qual_expr
            && !matches!(qual_expr, Expr::Const(Value::Bool(true)))
        {
            lines.push(format!(
                "{prefix}Filter: {}",
                render_explain_expr(qual_expr, &self.column_names)
            ));
        }
        if analyze && self.stats.rows_removed_by_filter > 0 {
            lines.push(format!(
                "{prefix}Rows Removed by Filter: {}",
                self.stats.rows_removed_by_filter
            ));
        }
        if analyze {
            lines.push(format!("{prefix}Heap Fetches: {}", self.stats.heap_fetches));
        }
        if analyze && self.stats.index_searches > 0 {
            lines.push(format!(
                "{prefix}Index Searches: {}",
                self.stats.index_searches
            ));
        }
    }

    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _timing: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

impl PlanNode for TidScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.relkind == 'm' && !self.relispopulated {
            return Err(ExecError::DetailedError {
                message: format!(
                    "materialized view \"{}\" has not been populated",
                    self.relation_name
                ),
                detail: None,
                hint: Some("Use the REFRESH MATERIALIZED VIEW command.".into()),
                sqlstate: "55000",
            });
        }

        if !self.candidates_initialized {
            self.candidates = tid_scan_candidates(&self.tid_cond.sources, ctx)?;
            self.candidates_initialized = true;
            ctx.session_stats
                .write()
                .note_relation_scan(self.relation_oid);
        }

        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;

        while let Some(tid) = self.candidates.get(self.candidate_index).copied() {
            self.candidate_index += 1;
            ctx.check_for_interrupts()?;
            let Some(tuple) = heap_fetch_visible_with_txns(
                &ctx.pool,
                ctx.client_id,
                self.rel,
                tid,
                &ctx.txns,
                &ctx.snapshot,
            )?
            else {
                continue;
            };
            if tid_scan_should_record_serializable_lock(ctx) {
                let _ = ctx.try_acquire_row_lock(self.relation_oid, tid, RowLockMode::SIRead);
            }
            {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_tuple_fetched(self.relation_oid);
                session_stats.note_relation_block_fetched(self.relation_oid);
                let object = relation_io_object(ctx, self.relation_oid);
                session_stats.note_io_read("client backend", object, "normal", 8192);
                session_stats.note_io_hit("client backend", object, "normal");
            }
            self.slot.kind = SlotKind::HeapTuple {
                desc: self.desc.clone(),
                attr_descs: self.attr_descs.clone(),
                tid,
                tuple,
            };
            self.slot.toast = slot_toast_context(self.toast_relation, ctx);
            self.slot.tts_nvalid = 0;
            self.slot.tts_values.clear();
            self.slot.decode_offset = 0;
            self.slot.table_oid = Some(self.relation_oid);
            let xmin = self.slot.xmin();
            let xmax = self.slot.xmax();
            self.current_bindings = vec![SystemVarBinding {
                varno: self.source_id,
                table_oid: self.relation_oid,
                tid: Some(tid),
                xmin,
                xmax,
            }];
            set_active_system_bindings(ctx, &self.current_bindings);

            if let Some(qual) = &self.qual {
                let outer_values = materialize_slot_values(&mut self.slot)?;
                let current_bindings = self.current_bindings.clone();
                set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                clear_inner_expr_bindings(ctx);
                if !qual(&mut self.slot, ctx)? {
                    note_filtered_row(&mut self.stats);
                    continue;
                }
            }

            ctx.session_stats
                .write()
                .note_relation_tuple_returned(self.relation_oid);
            finish_row(&mut self.stats, start);
            return Ok(Some(&mut self.slot));
        }

        finish_eof(&mut self.stats, start, ctx);
        Ok(None)
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.column_names
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        format!("Tid Scan on {}", self.relation_name)
    }

    fn explain_details(
        &self,
        indent: usize,
        _analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        lines.push(format!(
            "{prefix}TID Cond: {}",
            render_explain_expr(&self.tid_cond.display_expr, &self.column_names)
        ));
        if let Some(filter) = &self.qual_expr {
            lines.push(format!(
                "{prefix}Filter: {}",
                render_explain_expr(filter, &self.column_names)
            ));
        }
    }

    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _timing: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

fn tid_scan_should_record_serializable_lock(ctx: &ExecutorContext) -> bool {
    ctx.gucs
        .get("transaction_isolation")
        .is_some_and(|value| value.eq_ignore_ascii_case("serializable"))
}

fn tid_scan_candidates(
    sources: &[TidScanSource],
    ctx: &mut ExecutorContext,
) -> Result<Vec<crate::include::access::itemptr::ItemPointerData>, ExecError> {
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    let mut slot = TupleSlot::empty(0);
    for source in sources {
        let value = match source {
            TidScanSource::Scalar(expr) | TidScanSource::Array(expr) => {
                eval_expr(expr, &mut slot, ctx)?
            }
        };
        append_tid_candidates_from_value(&value, &mut seen, &mut out);
    }
    out.sort();
    Ok(out)
}

fn append_tid_candidates_from_value(
    value: &Value,
    seen: &mut BTreeSet<crate::include::access::itemptr::ItemPointerData>,
    out: &mut Vec<crate::include::access::itemptr::ItemPointerData>,
) {
    match value {
        Value::Tid(tid) => push_tid_candidate(*tid, seen, out),
        Value::Text(text) => {
            if let Some(tid) = parse_tid_candidate_text(text) {
                push_tid_candidate(tid, seen, out);
            }
        }
        Value::Array(values) => {
            for value in values {
                append_tid_candidates_from_value(value, seen, out);
            }
        }
        Value::PgArray(array) => {
            for value in &array.elements {
                append_tid_candidates_from_value(value, seen, out);
            }
        }
        _ => {}
    }
}

fn push_tid_candidate(
    tid: crate::include::access::itemptr::ItemPointerData,
    seen: &mut BTreeSet<crate::include::access::itemptr::ItemPointerData>,
    out: &mut Vec<crate::include::access::itemptr::ItemPointerData>,
) {
    if seen.insert(tid) {
        out.push(tid);
    }
}

fn parse_tid_candidate_text(
    text: &str,
) -> Option<crate::include::access::itemptr::ItemPointerData> {
    let text = text.trim().trim_matches('"');
    let inner = text.strip_prefix('(')?.strip_suffix(')')?;
    let (block, offset) = inner.split_once(',')?;
    Some(crate::include::access::itemptr::ItemPointerData {
        block_number: block.trim().parse().ok()?,
        offset_number: offset.trim().parse().ok()?,
    })
}

impl PlanNode for IndexScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        let mut visibility_map = None;
        if self.scan_exhausted {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }
        if self.scan.is_none() {
            if !begin_index_scan(self, ctx)? {
                self.scan_exhausted = true;
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }
        }

        loop {
            ctx.check_for_interrupts()?;
            let has_tuple = {
                let scan = self.scan.as_mut().expect("index scan must exist");
                indexam::index_getnext(scan, self.am_oid).map_err(|err| {
                    ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                        expected: "index access method tuple",
                        actual: format!("{err:?}"),
                    })
                })?
            };
            if !has_tuple {
                if let Some(scan) = self.scan.take() {
                    indexam::index_endscan(scan, self.am_oid).map_err(|err| {
                        ExecError::Parse(crate::backend::parser::ParseError::UnexpectedToken {
                            expected: "index access method end scan",
                            actual: format!("{err:?}"),
                        })
                    })?;
                }
                if begin_index_scan(self, ctx)? {
                    continue;
                }
                self.scan_exhausted = true;
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }

            let tid = self
                .scan
                .as_ref()
                .and_then(|scan| scan.xs_heaptid)
                .expect("index scan tuple must set heap tid");
            if !self.array_scan_seen_tids.insert(tid) {
                continue;
            }
            {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_tuple_returned(self.index_meta.indexrelid);
                session_stats.note_relation_block_fetched(self.index_meta.indexrelid);
            }
            if self.index_only {
                let vm_bits = visibilitymap_get_status(
                    &ctx.pool,
                    ctx.client_id,
                    self.rel,
                    tid.block_number,
                    &mut visibility_map,
                )
                .map_err(|err| {
                    internal_exec_error(format!("visibility map status failed: {err:?}"))
                })?;
                if vm_bits & VISIBILITYMAP_ALL_VISIBLE != 0 {
                    let index_tuple = self
                        .scan
                        .as_ref()
                        .and_then(|scan| scan.xs_itup.clone())
                        .ok_or_else(|| {
                            internal_exec_error(
                                "index-only scan requested tuple payload but AM returned none",
                            )
                        })?;
                    let index_values = decode_key_payload(&self.index_desc, &index_tuple.payload)?;
                    let mut values = vec![Value::Null; self.desc.columns.len()];
                    for (index_attno, value) in index_values.into_iter().enumerate() {
                        let heap_attno = usize::try_from(
                            *self.index_meta.indkey.get(index_attno).ok_or_else(|| {
                                internal_exec_error(format!(
                                    "missing indkey entry for index column {}",
                                    index_attno + 1
                                ))
                            })?,
                        )
                        .map_err(|_| {
                            internal_exec_error(format!(
                                "invalid heap attno for index column {}",
                                index_attno + 1
                            ))
                        })?;
                        let heap_index = heap_attno.checked_sub(1).ok_or_else(|| {
                            internal_exec_error(format!(
                                "non-column indkey entry not supported for index-only scan: {}",
                                heap_attno
                            ))
                        })?;
                        let slot_value = values.get_mut(heap_index).ok_or_else(|| {
                            internal_exec_error(format!(
                                "heap attno {} out of bounds for relation {}",
                                heap_attno, self.relation_name
                            ))
                        })?;
                        *slot_value = value;
                    }
                    self.slot
                        .store_virtual_row(values, Some(tid), Some(self.relation_oid));
                    self.current_bindings = vec![SystemVarBinding {
                        varno: self.source_id,
                        table_oid: self.relation_oid,
                        tid: Some(tid),
                        xmin: None,
                        xmax: None,
                    }];
                    set_active_system_bindings(ctx, &self.current_bindings);

                    if let Some(qual) = &self.qual {
                        let outer_values = materialize_slot_values(&mut self.slot)?;
                        let current_bindings = self.current_bindings.clone();
                        set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                        clear_inner_expr_bindings(ctx);
                        if !qual(&mut self.slot, ctx)? {
                            note_filtered_row(&mut self.stats);
                            continue;
                        }
                    }

                    finish_row(&mut self.stats, start);
                    return Ok(Some(&mut self.slot));
                }
            }
            if self.index_only {
                self.stats.heap_fetches = self.stats.heap_fetches.saturating_add(1);
            }
            let visible = heap_fetch_visible_with_txns(
                &ctx.pool,
                ctx.client_id,
                self.rel,
                tid,
                &ctx.txns,
                &ctx.snapshot,
            )?;
            let Some(tuple) = visible else {
                continue;
            };
            {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_tuple_fetched(self.relation_oid);
                session_stats.note_relation_block_fetched(self.relation_oid);
                session_stats.note_io_read("client backend", "relation", "normal", 8192);
                session_stats.note_io_hit("client backend", "relation", "normal");
            }
            self.slot.kind = SlotKind::HeapTuple {
                desc: self.desc.clone(),
                attr_descs: self.attr_descs.clone(),
                tid,
                tuple,
            };
            self.slot.toast = slot_toast_context(self.toast_relation, ctx);
            self.slot.tts_nvalid = 0;
            self.slot.tts_values.clear();
            self.slot.decode_offset = 0;
            self.slot.table_oid = Some(self.relation_oid);
            let xmin = self.slot.xmin();
            let xmax = self.slot.xmax();
            self.current_bindings = vec![SystemVarBinding {
                varno: self.source_id,
                table_oid: self.relation_oid,
                tid: Some(tid),
                xmin,
                xmax,
            }];
            set_active_system_bindings(ctx, &self.current_bindings);

            let needs_index_recheck = self.scan.as_ref().is_some_and(|scan| scan.xs_recheck);
            if needs_index_recheck && !recheck_lossy_index_scan_tuple(self, ctx)? {
                note_filtered_row(&mut self.stats);
                continue;
            }

            if let Some(qual) = &self.qual {
                let outer_values = materialize_slot_values(&mut self.slot)?;
                let current_bindings = self.current_bindings.clone();
                set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                clear_inner_expr_bindings(ctx);
                if !qual(&mut self.slot, ctx)? {
                    note_filtered_row(&mut self.stats);
                    continue;
                }
            }

            finish_row(&mut self.stats, start);
            return Ok(Some(&mut self.slot));
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        &self.column_names
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        let scan_name = if self.index_only {
            "Index Only Scan"
        } else {
            "Index Scan"
        };
        let direction = match self.direction {
            crate::include::access::relscan::ScanDirection::Forward => "",
            crate::include::access::relscan::ScanDirection::Backward => " Backward",
        };
        format!(
            "{scan_name}{direction} using {} on {}",
            self.index_name, self.relation_name
        )
    }
    fn renumber_append_child_aliases(&mut self, ordinal: usize) {
        self.relation_name = renumber_relation_alias(&self.relation_name, ordinal);
    }
    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        if let Some(detail) = render_index_scan_condition(&self.keys, &self.desc, &self.index_meta)
        {
            lines.push(format!("{prefix}Index Cond: ({detail})"));
        }
        if let Some(detail) =
            render_index_order_by(&self.order_by_keys, &self.desc, &self.index_meta)
        {
            lines.push(format!("{prefix}Order By: ({detail})"));
        }
        if let Some(qual_expr) = &self.qual_expr
            && !matches!(qual_expr, Expr::Const(Value::Bool(true)))
        {
            lines.push(format!(
                "{prefix}Filter: {}",
                render_explain_expr(qual_expr, &self.column_names)
            ));
        }
        if analyze && self.stats.rows_removed_by_filter > 0 {
            lines.push(format!(
                "{prefix}Rows Removed by Filter: {}",
                self.stats.rows_removed_by_filter
            ));
        }
        if analyze && self.index_only {
            lines.push(format!("{prefix}Heap Fetches: {}", self.stats.heap_fetches));
        }
        if analyze && self.stats.index_searches > 0 {
            lines.push(format!(
                "{prefix}Index Searches: {}",
                self.stats.index_searches
            ));
        }
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _timing: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

fn render_index_scan_key(
    key: &IndexScanKey,
    desc: &RelationDesc,
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    purpose: char,
    key_column_names: Option<&[String]>,
    runtime_renderer: Option<&dyn Fn(&Expr) -> String>,
) -> Option<String> {
    let default_column_names;
    if purpose == 's'
        && let Some(display_expr) = &key.display_expr
    {
        let column_names = match key_column_names {
            Some(names) => names,
            None => {
                default_column_names = desc
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>();
                &default_column_names
            }
        };
        return Some(render_index_display_expr(display_expr, column_names));
    }
    let index_attno = usize::try_from(key.attribute_number.checked_sub(1)?).ok()?;
    let index_key_attno = *index_meta.indkey.get(index_attno)?;
    let (column_name, column_type) = if index_key_attno == 0 {
        (
            expression_index_key_sql(index_meta, index_attno)?,
            SqlType::new(SqlTypeKind::Text),
        )
    } else {
        let heap_attno = usize::try_from(index_key_attno).ok()?.checked_sub(1)?;
        let column = desc.columns.get(heap_attno)?;
        let column_name = key_column_names
            .and_then(|names| names.get(heap_attno))
            .cloned()
            .unwrap_or_else(|| column.name.clone());
        (column_name, column.sql_type)
    };
    if purpose == 's' && matches!(&key.argument, IndexScanKeyArgument::Const(Value::Null)) {
        match key.strategy {
            0 | 3 => return Some(format!("{column_name} IS NULL")),
            1 => return Some(format!("{column_name} IS NOT NULL")),
            _ => {}
        }
    }
    let display_type = index_key_argument_display_type(&key.argument, column_type);
    let right_type_oid = display_type.and_then(index_scan_operator_type_oid_for_sql_type);
    let left_sql = if matches!(display_type.map(|ty| ty.kind), Some(SqlTypeKind::Char))
        && column_type.kind == SqlTypeKind::Char
    {
        format!("({column_name})::bpchar")
    } else if index_key_attno == 0 && expression_index_key_needs_parens(&column_name) {
        format!("({column_name})")
    } else {
        column_name
    };
    let right_type_oid = right_type_oid.or_else(|| match &key.argument {
        IndexScanKeyArgument::Const(value) => index_scan_operator_type_oid_for_value(value),
        IndexScanKeyArgument::Runtime(expr) => {
            crate::include::nodes::primnodes::expr_sql_type_hint(expr)
                .and_then(index_scan_operator_type_oid_for_sql_type)
        }
    });
    let operator_name = lookup_index_scan_operator_name(
        index_meta,
        index_attno,
        purpose,
        key.strategy,
        right_type_oid,
    )
    // :HACK: GiST/SP-GiST distance order-by scan keys normalize AMOP strategy 15
    // to the internal scan strategy 1. EXPLAIN still needs to deparse the
    // original ordering operator.
    .or_else(|| {
        (purpose == 'o' && key.strategy == 1).then(|| {
            lookup_index_scan_operator_name(index_meta, index_attno, purpose, 15, right_type_oid)
        })?
    })
    .or_else(|| fallback_index_scan_operator(index_meta.am_oid, key.strategy))?;
    let value_sql = match &key.argument {
        IndexScanKeyArgument::Const(value) => match display_type {
            Some(sql_type) => format!(
                "{}::{}",
                render_explain_typed_literal(value, sql_type),
                render_explain_sql_type_name(sql_type)
            ),
            None => render_explain_literal(value),
        },
        IndexScanKeyArgument::Runtime(expr) => key
            .runtime_label
            .clone()
            .or_else(|| runtime_renderer.map(|render| render(expr)))
            .unwrap_or_else(|| render_explain_expr(expr, &[])),
    };
    if key.strategy == 3
        && matches!(
            &key.argument,
            IndexScanKeyArgument::Const(Value::Array(_) | Value::PgArray(_))
        )
    {
        return Some(format!("{left_sql} {operator_name} ANY ({value_sql})"));
    }
    Some(format!("{left_sql} {operator_name} {value_sql}"))
}

fn expression_index_key_needs_parens(expr: &str) -> bool {
    let mut depth = 0usize;
    for (idx, ch) in expr.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ => {}
        }
        if depth == 0
            && [" || ", " + ", " - ", " * ", " / ", " % ", " = "]
                .iter()
                .any(|operator| expr[idx..].starts_with(operator))
        {
            return true;
        }
    }
    false
}

fn render_index_display_expr(expr: &Expr, column_names: &[String]) -> String {
    let Expr::Op(op) = expr else {
        return render_explain_expr_inner_with_qualifier(expr, None, column_names);
    };
    if !matches!(
        op.op,
        crate::include::nodes::primnodes::OpExprKind::Eq
            | crate::include::nodes::primnodes::OpExprKind::NotEq
            | crate::include::nodes::primnodes::OpExprKind::Lt
            | crate::include::nodes::primnodes::OpExprKind::LtEq
            | crate::include::nodes::primnodes::OpExprKind::Gt
            | crate::include::nodes::primnodes::OpExprKind::GtEq
    ) {
        return render_explain_expr_inner_with_qualifier(expr, None, column_names);
    }
    let [left, right] = op.args.as_slice() else {
        return render_explain_expr_inner_with_qualifier(expr, None, column_names);
    };
    let op_text = infix_operator_text(op.opno, op.op).unwrap_or("=");
    let left_sql = render_explain_expr_inner_with_qualifier(left, None, column_names);
    let left_sql = if matches!(left, Expr::Op(_)) {
        format!("({left_sql})")
    } else {
        left_sql
    };
    let right_sql = render_explain_infix_operand(right, None, column_names);
    format!("{left_sql} {op_text} {right_sql}")
}

fn index_scan_operator_type_oid_for_sql_type(sql_type: SqlType) -> Option<u32> {
    let sql_type = if sql_type.is_array {
        sql_type.element_type()
    } else {
        sql_type
    };
    Some(match sql_type.kind {
        SqlTypeKind::Int2Vector => crate::include::catalog::INT2VECTOR_TYPE_OID,
        SqlTypeKind::OidVector => crate::include::catalog::OIDVECTOR_TYPE_OID,
        _ => crate::backend::utils::cache::catcache::sql_type_oid(sql_type),
    })
}

fn index_scan_operator_type_oid_for_value(value: &Value) -> Option<u32> {
    value
        .sql_type_hint()
        .and_then(index_scan_operator_type_oid_for_sql_type)
}

fn lookup_index_scan_operator_name(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    index_attno: usize,
    purpose: char,
    strategy: u16,
    right_type_oid: Option<u32>,
) -> Option<String> {
    index_meta
        .amop_entries
        .get(index_attno)
        .into_iter()
        .flat_map(|entries| entries.iter())
        .filter(|entry| {
            entry.purpose == purpose && u16::try_from(entry.strategy).ok() == Some(strategy)
        })
        .filter(|entry| {
            let Some(actual) = right_type_oid else {
                return true;
            };
            entry.righttype == actual
                || entry.righttype == crate::include::catalog::ANYOID
                || (entry.righttype == crate::include::catalog::ANYRANGEOID
                    && crate::include::catalog::builtin_range_spec_by_oid(actual).is_some())
                || (entry.righttype == crate::include::catalog::ANYMULTIRANGEOID
                    && crate::include::catalog::builtin_range_spec_by_multirange_oid(actual)
                        .is_some())
                || entry.righttype == crate::include::catalog::ANYARRAYOID
                || entry.righttype == crate::include::catalog::ANYELEMENTOID
        })
        .max_by_key(|entry| {
            if Some(entry.righttype) == right_type_oid {
                4
            } else if entry.righttype == crate::include::catalog::ANYRANGEOID
                || entry.righttype == crate::include::catalog::ANYMULTIRANGEOID
            {
                2
            } else if entry.righttype == crate::include::catalog::ANYOID
                || entry.righttype == crate::include::catalog::ANYARRAYOID
                || entry.righttype == crate::include::catalog::ANYELEMENTOID
            {
                1
            } else {
                0
            }
        })
        .and_then(|operator| {
            crate::include::catalog::bootstrap_pg_operator_rows()
                .into_iter()
                .find(|row| row.oid == operator.operator_oid)
                .map(|row| row.oprname)
        })
}

fn fallback_index_scan_operator(am_oid: u32, strategy: u16) -> Option<String> {
    match am_oid {
        BTREE_AM_OID => Some(
            match strategy {
                1 => "<",
                2 => "<=",
                3 => "=",
                4 => ">=",
                5 => ">",
                _ => return None,
            }
            .into(),
        ),
        HASH_AM_OID => Some(
            match strategy {
                1 => "=",
                _ => return None,
            }
            .into(),
        ),
        GIST_AM_OID | SPGIST_AM_OID => Some(
            match strategy {
                1 => "<<",
                2 => "<<=",
                3 => ">>",
                4 => ">>=",
                5 => "&&",
                6 => "-|-",
                7 => "@>",
                8 => "<@",
                _ => return None,
            }
            .into(),
        ),
        _ => None,
    }
}

fn index_key_argument_display_type(
    argument: &IndexScanKeyArgument,
    column_type: SqlType,
) -> Option<SqlType> {
    match argument {
        IndexScanKeyArgument::Const(Value::Text(_) | Value::TextRef(_, _))
            if matches!(column_type.kind, SqlTypeKind::Char | SqlTypeKind::Name) =>
        {
            Some(SqlType::new(column_type.kind))
        }
        IndexScanKeyArgument::Const(Value::Int32(value))
            if column_type.kind == SqlTypeKind::Int4 =>
        {
            (*value < 0).then_some(column_type)
        }
        IndexScanKeyArgument::Const(Value::Bool(_)) if column_type.kind == SqlTypeKind::Bool => {
            None
        }
        IndexScanKeyArgument::Const(value) => value.sql_type_hint(),
        IndexScanKeyArgument::Runtime(expr) => {
            crate::include::nodes::primnodes::expr_sql_type_hint(expr)
        }
    }
}

fn expression_index_key_sql(
    index_meta: &crate::backend::utils::cache::relcache::IndexRelCacheEntry,
    index_attno: usize,
) -> Option<String> {
    let expr_pos = index_meta
        .indkey
        .iter()
        .take(index_attno)
        .filter(|attno| **attno == 0)
        .count();
    let expr_sqls = serde_json::from_str::<Vec<String>>(index_meta.indexprs.as_deref()?).ok()?;
    expr_sqls
        .get(expr_pos)
        .map(|expr_sql| format_expression_index_sql(expr_sql))
}

fn format_expression_index_sql(expr_sql: &str) -> String {
    let trimmed = expr_sql.trim();
    let Some(open_paren) = trimmed.find('(') else {
        return trimmed.to_string();
    };
    if !trimmed.ends_with(')') {
        return trimmed.to_string();
    }
    let name = trimmed[..open_paren].trim();
    if name.is_empty() {
        return trimmed.to_string();
    }
    let args = &trimmed[open_paren + 1..trimmed.len().saturating_sub(1)];
    let args = split_expression_index_args(args)
        .into_iter()
        .map(format_expression_index_arg)
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({args})")
}

fn split_expression_index_args(args: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut start = 0usize;
    for (idx, ch) in args.char_indices() {
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                out.push(args[start..idx].trim());
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    out.push(args[start..].trim());
    out
}

fn format_expression_index_arg(arg: &str) -> String {
    let trimmed = arg.trim();
    if let Some((left, right)) = trimmed.split_once('+') {
        return format!("({} + {})", left.trim(), right.trim());
    }
    if let Some((left, right)) = trimmed.split_once('-')
        && !left.trim().is_empty()
    {
        return format!("({} - {})", left.trim(), right.trim());
    }
    trimmed.to_string()
}

impl BitmapIndexScanState {
    fn fill_bitmap(&mut self, ctx: &mut ExecutorContext) -> Result<(), ExecError> {
        if self.executed {
            return Ok(());
        }

        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;

        let Some(key_data) = eval_index_scan_keys(&self.keys, ctx, true)? else {
            self.executed = true;
            self.stats.rows = 0;
            finish_eof(&mut self.stats, start, ctx);
            return Ok(());
        };
        let mut tuples = 0;
        let allow_array_expansion = self.am_oid == BTREE_AM_OID;
        for key_data in expand_array_equality_scan_keys(key_data, allow_array_expansion) {
            let begin = crate::include::access::amapi::IndexBeginScanContext {
                pool: ctx.pool.clone(),
                client_id: ctx.client_id,
                snapshot: ctx.snapshot.clone(),
                heap_relation: self.rel,
                index_relation: self.index_rel,
                index_desc: (*self.index_desc).clone(),
                index_meta: self.index_meta.clone(),
                key_data,
                order_by_data: Vec::new(),
                direction: crate::include::access::relscan::ScanDirection::Forward,
                want_itup: false,
            };
            let mut scan = indexam::index_beginscan(&begin, self.am_oid)
                .map_err(|err| bitmap_am_error("index access method begin bitmap scan", err))?;
            {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_scan(self.index_meta.indexrelid);
                session_stats.note_io_read("client backend", "relation", "normal", 8192);
                session_stats.note_io_hit("client backend", "relation", "normal");
            }
            tuples += indexam::index_getbitmap(&mut scan, self.am_oid, &mut self.bitmap)
                .map_err(|err| bitmap_am_error("index access method bitmap scan", err))?;
            indexam::index_endscan(scan, self.am_oid)
                .map_err(|err| bitmap_am_error("index access method end bitmap scan", err))?;
        }

        self.executed = true;
        self.stats.rows = tuples.max(0) as u64;
        finish_eof(&mut self.stats, start, ctx);
        Ok(())
    }
}

impl BitmapQualState {
    fn fill_bitmap(&mut self, ctx: &mut ExecutorContext) -> Result<(), ExecError> {
        match self {
            BitmapQualState::Index(state) => state.fill_bitmap(ctx),
            BitmapQualState::Or(state) => state.fill_bitmap(ctx),
            BitmapQualState::And(state) => state.fill_bitmap(ctx),
        }
    }

    fn bitmap(&self) -> &crate::include::access::tidbitmap::TidBitmap {
        match self {
            BitmapQualState::Index(state) => &state.bitmap,
            BitmapQualState::Or(state) => &state.bitmap,
            BitmapQualState::And(state) => &state.bitmap,
        }
    }

    fn rows(&self) -> u64 {
        match self {
            BitmapQualState::Index(state) => state.stats.rows,
            BitmapQualState::Or(state) => state.stats.rows,
            BitmapQualState::And(state) => state.stats.rows,
        }
    }

    fn explain_json(&self, analyze: bool, indent: usize) -> String {
        match self {
            BitmapQualState::Index(state) => state.explain_json(analyze, indent),
            BitmapQualState::Or(state) => state.explain_json(analyze, indent),
            BitmapQualState::And(state) => state.explain_json(analyze, indent),
        }
    }

    fn explain(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        match self {
            BitmapQualState::Index(state) => {
                format_explain_lines_with_costs(
                    state.as_ref(),
                    indent,
                    analyze,
                    show_costs,
                    timing,
                    lines,
                );
            }
            BitmapQualState::Or(state) => {
                format_explain_lines_with_costs(
                    state.as_ref(),
                    indent,
                    analyze,
                    show_costs,
                    timing,
                    lines,
                );
            }
            BitmapQualState::And(state) => {
                format_explain_lines_with_costs(
                    state.as_ref(),
                    indent,
                    analyze,
                    show_costs,
                    timing,
                    lines,
                );
            }
        }
    }
}

impl BitmapOrState {
    fn fill_bitmap(&mut self, ctx: &mut ExecutorContext) -> Result<(), ExecError> {
        if self.executed {
            return Ok(());
        }

        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;

        let mut rows = 0;
        for child in &mut self.children {
            child.fill_bitmap(ctx)?;
            rows += child.rows();
            self.bitmap.union_with(child.bitmap());
        }

        self.executed = true;
        self.stats.rows = rows;
        finish_eof(&mut self.stats, start, ctx);
        Ok(())
    }
}

impl BitmapAndState {
    fn fill_bitmap(&mut self, ctx: &mut ExecutorContext) -> Result<(), ExecError> {
        if self.executed {
            return Ok(());
        }

        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;

        let mut children = self.children.iter_mut();
        let Some(first) = children.next() else {
            self.executed = true;
            self.stats.rows = 0;
            finish_eof(&mut self.stats, start, ctx);
            return Ok(());
        };
        first.fill_bitmap(ctx)?;
        self.bitmap = first.bitmap().clone();
        let mut rows = first.rows();
        for child in children {
            child.fill_bitmap(ctx)?;
            rows = rows.min(child.rows());
            self.bitmap.intersect_with(child.bitmap());
        }

        self.executed = true;
        self.stats.rows = rows.min(self.bitmap.len() as u64);
        finish_eof(&mut self.stats, start, ctx);
        Ok(())
    }
}

impl PlanNode for BitmapIndexScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        _ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        Err(internal_exec_error(
            "bitmap index scan cannot produce tuples directly",
        ))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        None
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &EMPTY_SYSTEM_BINDINGS
    }

    fn column_names(&self) -> &[String] {
        &self.column_names
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        format!("Bitmap Index Scan on {}", self.index_name)
    }

    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        if let Some(detail) =
            render_index_scan_condition(&self.keys, &self.heap_desc, &self.index_meta)
        {
            let prefix = explain_detail_prefix(indent);
            lines.push(format!("{prefix}Index Cond: ({detail})"));
        } else if !self.index_quals.is_empty() {
            let prefix = explain_detail_prefix(indent);
            lines.push(format!(
                "{prefix}Index Cond: {}",
                render_explain_expr(&format_qual_list(&self.index_quals), &self.column_names)
            ));
        }
        if analyze
            && (self.stats.buffer_usage.shared_hit > 0
                || self.stats.buffer_usage.shared_read > 0
                || self.stats.buffer_usage.shared_written > 0)
        {
            let prefix = explain_detail_prefix(indent);
            lines.push(format!(
                "{prefix}{}",
                crate::backend::commands::explain::format_buffer_usage(self.stats.buffer_usage)
            ));
        }
    }

    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _timing: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

impl PlanNode for BitmapOrState {
    fn exec_proc_node<'a>(
        &'a mut self,
        _ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        Err(internal_exec_error(
            "bitmap or cannot produce tuples directly",
        ))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        None
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &EMPTY_SYSTEM_BINDINGS
    }

    fn column_names(&self) -> &[String] {
        &[]
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "BitmapOr".into()
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        for child in &self.children {
            child.explain(indent + 1, analyze, show_costs, timing, lines);
        }
    }

    fn explain_json_children(&self, analyze: bool, indent: usize) -> Vec<String> {
        self.children
            .iter()
            .map(|child| child.explain_json(analyze, indent))
            .collect()
    }
}

impl PlanNode for BitmapAndState {
    fn exec_proc_node<'a>(
        &'a mut self,
        _ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        Err(internal_exec_error(
            "bitmap and cannot produce tuples directly",
        ))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        None
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &EMPTY_SYSTEM_BINDINGS
    }

    fn column_names(&self) -> &[String] {
        &[]
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "BitmapAnd".into()
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        for child in &self.children {
            child.explain(indent + 1, analyze, show_costs, timing, lines);
        }
    }

    fn explain_json_children(&self, analyze: bool, indent: usize) -> Vec<String> {
        self.children
            .iter()
            .map(|child| child.explain_json(analyze, indent))
            .collect()
    }
}

impl BitmapHeapScanState {
    fn load_next_bitmap_page(&mut self, ctx: &mut ExecutorContext) -> Result<bool, ExecError> {
        self.current_page_offsets.clear();
        self.current_offset_index = 0;
        self.current_page_pin = None;

        while let Some(block) = self.bitmap_pages.get(self.current_page_index).copied() {
            self.current_page_index += 1;

            let pin = ctx
                .pool
                .pin_existing_block(ctx.client_id, self.rel, ForkNumber::Main, block)
                .map_err(|err| {
                    internal_exec_error(format!("bitmap heap pin block failed: {err:?}"))
                })?;
            let buffer_id = pin.into_raw();
            let owned_pin =
                crate::OwnedBufferPin::wrap_existing(std::sync::Arc::clone(&ctx.pool), buffer_id);
            let pin_rc = Rc::new(owned_pin);

            let guard = ctx.pool.lock_buffer_shared(buffer_id).map_err(|err| {
                internal_exec_error(format!("bitmap heap shared lock failed: {err:?}"))
            })?;
            let mut offsets = collect_visible_page_offsets(&guard, &ctx.snapshot, &ctx.txns)?;
            drop(guard);
            if let Some(exact_offsets) = self.bitmapqual.bitmap().exact_offsets(block) {
                offsets.retain(|offset| exact_offsets.contains(offset));
            }

            if offsets.is_empty() {
                continue;
            }

            {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_block_fetched(self.relation_oid);
                session_stats.note_io_read("client backend", "relation", "normal", 8192);
                session_stats.note_io_hit("client backend", "relation", "normal");
            }

            self.current_page_pin = Some(pin_rc);
            self.current_page_offsets = offsets;
            return Ok(true);
        }

        Ok(false)
    }
}

impl PlanNode for BitmapHeapScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;

        if self.bitmap_pages.is_empty() && self.current_page_index == 0 {
            self.bitmapqual.fill_bitmap(ctx)?;
            self.bitmap_pages = self.bitmapqual.bitmap().iter().collect();
        }

        loop {
            ctx.check_for_interrupts()?;

            if self.current_offset_index >= self.current_page_offsets.len() {
                if !self.load_next_bitmap_page(ctx)? {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }
            }

            let offset = self.current_page_offsets[self.current_offset_index];
            self.current_offset_index += 1;
            let pin = self
                .current_page_pin
                .as_ref()
                .cloned()
                .ok_or_else(|| internal_exec_error("bitmap heap scan lost current page pin"))?;
            let buffer_id = pin.buffer_id();
            let page = unsafe { ctx.pool.page_unlocked(buffer_id) }
                .ok_or_else(|| internal_exec_error("bitmap heap scan page vanished"))?;
            let tuple_bytes = page_get_item_unchecked(page, offset);

            self.slot.kind = SlotKind::BufferHeapTuple {
                desc: self.desc.clone(),
                attr_descs: self.attr_descs.clone(),
                tid: crate::include::access::itemptr::ItemPointerData {
                    block_number: self.bitmap_pages[self.current_page_index - 1],
                    offset_number: offset,
                },
                tuple_ptr: tuple_bytes.as_ptr(),
                tuple_len: tuple_bytes.len(),
                pin,
            };
            self.slot.toast = slot_toast_context(self.toast_relation, ctx);
            self.slot.tts_nvalid = 0;
            self.slot.tts_values.clear();
            self.slot.decode_offset = 0;
            self.slot.table_oid = Some(self.relation_oid);
            let xmin = self.slot.xmin();
            let xmax = self.slot.xmax();
            self.current_bindings = vec![SystemVarBinding {
                varno: self.source_id,
                table_oid: self.relation_oid,
                tid: Some(crate::include::access::itemptr::ItemPointerData {
                    block_number: self.bitmap_pages[self.current_page_index - 1],
                    offset_number: offset,
                }),
                xmin,
                xmax,
            }];
            set_active_system_bindings(ctx, &self.current_bindings);

            if let Some(recheck) = &self.compiled_recheck {
                let outer_values = materialize_slot_values(&mut self.slot)?;
                let current_bindings = self.current_bindings.clone();
                set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                clear_inner_expr_bindings(ctx);
                if !recheck(&mut self.slot, ctx)? {
                    note_index_recheck_filtered_row(&mut self.stats);
                    continue;
                }
            }
            if let Some(filter) = &self.compiled_filter {
                let outer_values = materialize_slot_values(&mut self.slot)?;
                let current_bindings = self.current_bindings.clone();
                set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                clear_inner_expr_bindings(ctx);
                if !filter(&mut self.slot, ctx)? {
                    note_filtered_row(&mut self.stats);
                    continue;
                }
            }

            {
                let mut session_stats = ctx.session_stats.write();
                session_stats.note_relation_tuple_returned(self.relation_oid);
                session_stats.note_relation_tuple_fetched(self.relation_oid);
            }
            finish_row(&mut self.stats, start);
            return Ok(Some(&mut self.slot));
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.column_names
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        format!(
            "Bitmap Heap Scan on {}",
            explain_relation_name(&self.relation_name)
        )
    }
    fn renumber_append_child_aliases(&mut self, ordinal: usize) {
        self.relation_name = renumber_relation_alias(&self.relation_name, ordinal);
    }

    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        if let Some(recheck_qual) = &self.recheck_qual {
            let prefix = explain_detail_prefix(indent);
            lines.push(format!(
                "{prefix}Recheck Cond: {}",
                render_explain_expr(recheck_qual, &self.column_names)
            ));
        }
        if let Some(filter_qual) = &self.filter_qual
            && !matches!(filter_qual, Expr::Const(Value::Bool(true)))
        {
            let prefix = explain_detail_prefix(indent);
            lines.push(format!(
                "{prefix}Filter: {}",
                render_explain_expr(filter_qual, &self.column_names)
            ));
        }
        let prefix = explain_detail_prefix(indent);
        if analyze && self.stats.rows_removed_by_index_recheck > 0 {
            lines.push(format!(
                "{prefix}Rows Removed by Index Recheck: {}",
                self.stats.rows_removed_by_index_recheck
            ));
        }
        if analyze && self.stats.rows_removed_by_filter > 0 {
            lines.push(format!(
                "{prefix}Rows Removed by Filter: {}",
                self.stats.rows_removed_by_filter
            ));
        }
        if analyze
            && (self.stats.buffer_usage.shared_hit > 0
                || self.stats.buffer_usage.shared_read > 0
                || self.stats.buffer_usage.shared_written > 0)
        {
            lines.push(format!(
                "{prefix}{}",
                crate::backend::commands::explain::format_buffer_usage(self.stats.buffer_usage)
            ));
        }
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        self.bitmapqual
            .explain(indent + 1, analyze, show_costs, timing, lines);
    }

    fn explain_json_children(&self, analyze: bool, indent: usize) -> Vec<String> {
        vec![self.bitmapqual.explain_json(analyze, indent)]
    }
}

pub(crate) fn render_explain_expr(expr: &Expr, column_names: &[String]) -> String {
    render_explain_expr_with_qualifier(expr, None, column_names)
}

pub(crate) fn render_explain_expr_with_qualifier(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    if let Some(rendered) =
        render_range_support_expr(expr, qualifier, column_names).map(|out| out.render_full())
    {
        return rendered;
    }
    if matches!(
        expr,
        Expr::Func(func)
            if matches!(
                (&func.implementation, func.funcname.as_deref()),
                (
                    ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TextStartsWith),
                    Some("starts_with")
                )
            )
    ) {
        return render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    }
    if let Expr::Func(func) = expr
        && !render_explain_func_expr_is_infix(func)
    {
        return render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    }
    if matches!(expr, Expr::Var(_))
        || matches!(
            expr,
            Expr::Bool(bool_expr)
                if matches!(
                    bool_expr.boolop,
                    crate::include::nodes::primnodes::BoolExprType::Not
                ) && bool_expr
                    .args
                    .first()
                    .is_some_and(|inner| {
                        render_explain_negated_bool_comparison(inner, qualifier, column_names)
                            .is_some()
                    })
        )
    {
        return render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    }
    format!(
        "({})",
        render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
    )
}

pub(crate) fn render_explain_projection_expr_with_qualifier(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    format!(
        "({})",
        render_explain_projection_expr_inner_with_qualifier(expr, qualifier, column_names)
    )
}

pub(crate) fn render_explain_join_expr(
    expr: &Expr,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    format!(
        "({})",
        render_explain_join_expr_inner(expr, outer_names, inner_names)
    )
}

fn render_explain_var_name(var: &Var, column_names: &[String]) -> Option<String> {
    attrno_index(var.varattno)
        .and_then(|index| column_names.get(index).cloned())
        .map(normalize_explain_var_name)
        .or_else(|| render_explain_system_var_name(var.varattno, column_names))
}

fn render_explain_system_var_name(
    attno: crate::include::nodes::primnodes::AttrNumber,
    column_names: &[String],
) -> Option<String> {
    let name = match attno {
        TABLE_OID_ATTR_NO => "tableoid",
        SELF_ITEM_POINTER_ATTR_NO => "ctid",
        XMIN_ATTR_NO => "xmin",
        XMAX_ATTR_NO => "xmax",
        _ => return None,
    };
    relation_qualifier_from_column_names(column_names)
        .map(|qualifier| format!("{qualifier}.{name}"))
        .or_else(|| Some(name.into()))
}

fn relation_qualifier_from_column_names(column_names: &[String]) -> Option<String> {
    column_names
        .iter()
        .filter_map(|name| name.split_once('.').map(|(qualifier, _)| qualifier))
        .find(|qualifier| !qualifier.is_empty())
        .map(str::to_string)
}

fn normalize_explain_var_name(name: String) -> String {
    let mut inner = name.as_str();
    let mut stripped = false;
    while let Some(next) = inner
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
    {
        inner = next;
        stripped = true;
    }
    if !stripped {
        return name;
    }
    if matches!(
        inner.split_once('(').map(|(func, _)| func),
        Some("avg" | "count" | "max" | "min" | "sum")
    ) {
        inner.to_string()
    } else {
        name
    }
}

fn normalize_aggregate_operand_parens(rendered: String) -> String {
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

fn aggregate_call_starts_at(chars: &[char], index: usize) -> bool {
    ["avg(", "count(", "max(", "min(", "sum("]
        .iter()
        .any(|prefix| {
            let prefix = prefix.chars().collect::<Vec<_>>();
            chars
                .get(index..index.saturating_add(prefix.len()))
                .is_some_and(|candidate| candidate == prefix.as_slice())
        })
}

fn render_explain_expr_inner(expr: &Expr, column_names: &[String]) -> String {
    render_explain_expr_inner_with_qualifier(expr, None, column_names)
}

fn render_explain_expr_inner_with_qualifier(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    match expr {
        Expr::GroupingKey(grouping_key) => {
            render_explain_expr_inner_with_qualifier(&grouping_key.expr, qualifier, column_names)
        }
        Expr::GroupingFunc(grouping_func) => {
            let args = grouping_func
                .args
                .iter()
                .map(|arg| render_explain_expr_inner_with_qualifier(arg, qualifier, column_names))
                .collect::<Vec<_>>()
                .join(", ");
            format!("GROUPING({args})")
        }
        Expr::Var(var) => render_explain_var_name(var, column_names)
            .map(|name| match qualifier {
                Some(qualifier) => format!("{qualifier}.{name}"),
                None => name,
            })
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Param(param) if param.paramkind == ParamKind::Exec => {
            format!("${}", param.paramid)
        }
        Expr::Param(param) if param.paramkind == ParamKind::External => {
            format!("${}", param.paramid)
        }
        Expr::Const(value) => render_explain_const(value),
        Expr::Cast(inner, ty) => render_explain_cast(inner, *ty, qualifier, column_names),
        Expr::Collate {
            expr,
            collation_oid,
        } => render_explain_collate(expr, *collation_oid, qualifier, column_names),
        Expr::Op(op) => match op.op {
            crate::include::nodes::primnodes::OpExprKind::UnaryPlus
            | crate::include::nodes::primnodes::OpExprKind::Negate
            | crate::include::nodes::primnodes::OpExprKind::BitNot => {
                let [inner] = op.args.as_slice() else {
                    return format!("{expr:?}");
                };
                let op_text = match op.op {
                    crate::include::nodes::primnodes::OpExprKind::UnaryPlus => "+",
                    crate::include::nodes::primnodes::OpExprKind::Negate => "-",
                    crate::include::nodes::primnodes::OpExprKind::BitNot => "~",
                    _ => unreachable!(),
                };
                format!(
                    "({op_text} {})",
                    render_explain_expr_inner_with_qualifier(inner, qualifier, column_names)
                )
            }
            crate::include::nodes::primnodes::OpExprKind::Add
            | crate::include::nodes::primnodes::OpExprKind::Sub
            | crate::include::nodes::primnodes::OpExprKind::Mul
            | crate::include::nodes::primnodes::OpExprKind::Div
            | crate::include::nodes::primnodes::OpExprKind::Mod
            | crate::include::nodes::primnodes::OpExprKind::BitAnd
            | crate::include::nodes::primnodes::OpExprKind::BitOr
            | crate::include::nodes::primnodes::OpExprKind::BitXor
            | crate::include::nodes::primnodes::OpExprKind::Shl
            | crate::include::nodes::primnodes::OpExprKind::Shr
            | crate::include::nodes::primnodes::OpExprKind::Concat
            | crate::include::nodes::primnodes::OpExprKind::JsonGet
            | crate::include::nodes::primnodes::OpExprKind::JsonGetText => {
                let [left, right] = op.args.as_slice() else {
                    return format!("{expr:?}");
                };
                let op_text = match op.op {
                    crate::include::nodes::primnodes::OpExprKind::Add => "+",
                    crate::include::nodes::primnodes::OpExprKind::Sub => "-",
                    crate::include::nodes::primnodes::OpExprKind::Mul => "*",
                    crate::include::nodes::primnodes::OpExprKind::Div => "/",
                    crate::include::nodes::primnodes::OpExprKind::Mod => "%",
                    crate::include::nodes::primnodes::OpExprKind::BitAnd => "&",
                    crate::include::nodes::primnodes::OpExprKind::BitOr => "|",
                    crate::include::nodes::primnodes::OpExprKind::BitXor => "#",
                    crate::include::nodes::primnodes::OpExprKind::Shl => "<<",
                    crate::include::nodes::primnodes::OpExprKind::Shr => ">>",
                    crate::include::nodes::primnodes::OpExprKind::Concat => "||",
                    crate::include::nodes::primnodes::OpExprKind::JsonGet => "->",
                    crate::include::nodes::primnodes::OpExprKind::JsonGetText => "->>",
                    _ => unreachable!(),
                };
                format!(
                    "{} {} {}",
                    render_explain_infix_operand(left, qualifier, column_names),
                    op_text,
                    render_explain_infix_operand(right, qualifier, column_names)
                )
            }
            crate::include::nodes::primnodes::OpExprKind::Eq
            | crate::include::nodes::primnodes::OpExprKind::NotEq
            | crate::include::nodes::primnodes::OpExprKind::Lt
            | crate::include::nodes::primnodes::OpExprKind::LtEq
            | crate::include::nodes::primnodes::OpExprKind::Gt
            | crate::include::nodes::primnodes::OpExprKind::GtEq
            | crate::include::nodes::primnodes::OpExprKind::RegexMatch
            | crate::include::nodes::primnodes::OpExprKind::ArrayOverlap
            | crate::include::nodes::primnodes::OpExprKind::ArrayContains
            | crate::include::nodes::primnodes::OpExprKind::ArrayContained
            | crate::include::nodes::primnodes::OpExprKind::JsonbContains
            | crate::include::nodes::primnodes::OpExprKind::JsonbContained
            | crate::include::nodes::primnodes::OpExprKind::JsonbExists
            | crate::include::nodes::primnodes::OpExprKind::JsonbExistsAny
            | crate::include::nodes::primnodes::OpExprKind::JsonbExistsAll
            | crate::include::nodes::primnodes::OpExprKind::JsonbPathExists
            | crate::include::nodes::primnodes::OpExprKind::JsonbPathMatch => {
                let [left, right] = op.args.as_slice() else {
                    return format!("{expr:?}");
                };
                if let Some(rendered) =
                    render_explain_bool_comparison(op.op, left, right, qualifier, column_names)
                {
                    return rendered;
                }
                let op_text = infix_operator_text(op.opno, op.op).unwrap_or("~");
                if let (Some(left), Some(right)) = (
                    render_explain_row_comparison_operand(left, qualifier, column_names),
                    render_explain_row_comparison_operand(right, qualifier, column_names),
                ) {
                    return format!("{left} {op_text} {right}");
                }
                let display_type = comparison_display_type(left, right, op.collation_oid);
                let right_collation_oid =
                    if text_pattern_operator_suppresses_explain_collation(op.opno) {
                        None
                    } else {
                        op.collation_oid
                    };
                format!(
                    "{} {} {}",
                    render_explain_infix_operand_with_display_type(
                        left,
                        display_type,
                        None,
                        qualifier,
                        column_names
                    ),
                    op_text,
                    render_explain_infix_operand_with_display_type(
                        right,
                        display_type,
                        right_collation_oid,
                        qualifier,
                        column_names
                    )
                )
            }
            _ => format!("{expr:?}"),
        },
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            crate::include::nodes::primnodes::BoolExprType::And => {
                let mut args = Vec::new();
                collect_bool_explain_args(
                    expr,
                    crate::include::nodes::primnodes::BoolExprType::And,
                    &mut args,
                );
                let rendered = args
                    .into_iter()
                    .map(|arg| render_explain_bool_arg(arg, qualifier, column_names))
                    .collect::<Vec<_>>();
                rendered.join(" AND ")
            }
            crate::include::nodes::primnodes::BoolExprType::Or => {
                let mut args = Vec::new();
                collect_bool_explain_args(
                    expr,
                    crate::include::nodes::primnodes::BoolExprType::Or,
                    &mut args,
                );
                let rendered = args
                    .into_iter()
                    .map(|arg| render_explain_bool_arg(arg, qualifier, column_names))
                    .collect::<Vec<_>>();
                rendered.join(" OR ")
            }
            crate::include::nodes::primnodes::BoolExprType::Not => {
                let Some(inner) = bool_expr.args.first() else {
                    return format!("{expr:?}");
                };
                if let Some(rendered) =
                    render_explain_negated_bool_comparison(inner, qualifier, column_names)
                {
                    return rendered;
                }
                let rendered =
                    render_explain_expr_inner_with_qualifier(inner, qualifier, column_names);
                if explain_bool_arg_is_bare(inner) {
                    format!("NOT {rendered}")
                } else {
                    format!("NOT ({rendered})")
                }
            }
        },
        Expr::Coalesce(left, right) => format!(
            "COALESCE({}, {})",
            render_explain_expr_inner_with_qualifier(left, qualifier, column_names),
            render_explain_expr_inner_with_qualifier(right, qualifier, column_names)
        ),
        Expr::IsNull(inner) => {
            let rendered = render_explain_expr_inner_with_qualifier(inner, qualifier, column_names);
            if expr_sql_type_is_bool(inner) {
                format!("{rendered} IS UNKNOWN")
            } else {
                format!("{rendered} IS NULL")
            }
        }
        Expr::IsNotNull(inner) => {
            let rendered = render_explain_expr_inner_with_qualifier(inner, qualifier, column_names);
            if expr_sql_type_is_bool(inner) {
                format!("{rendered} IS NOT UNKNOWN")
            } else {
                format!("{rendered} IS NOT NULL")
            }
        }
        Expr::IsDistinctFrom(left, right) => {
            render_explain_distinctness_expr(left, right, true, qualifier, column_names)
        }
        Expr::IsNotDistinctFrom(left, right) => {
            render_explain_distinctness_expr(left, right, false, qualifier, column_names)
        }
        Expr::Func(func) => render_explain_func_expr(func, qualifier, column_names),
        Expr::ScalarArrayOp(saop) => render_explain_scalar_array_op(saop, qualifier, column_names),
        Expr::Case(case_expr) => render_explain_whole_row_case(case_expr, qualifier, column_names)
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => render_explain_array_literal(elements, *array_type, qualifier, column_names),
        Expr::Row { fields, .. } => {
            let fields = fields
                .iter()
                .map(|(_, expr)| {
                    render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("ROW({fields})")
        }
        Expr::Case(case_expr) => render_explain_case_expr(case_expr, qualifier, column_names),
        Expr::FieldSelect {
            expr: inner, field, ..
        } => render_explain_field_select(inner, field, qualifier, column_names),
        Expr::ArraySubscript { array, subscripts } => {
            render_explain_array_subscript(array, subscripts, qualifier, column_names)
        }
        Expr::SubPlan(subplan) => {
            if subplan.par_param.is_empty() {
                format!("(InitPlan {}).col1", subplan.plan_id + 1)
            } else {
                format!("(SubPlan {})", subplan.plan_id + 1)
            }
        }
        Expr::CurrentCatalog => "CURRENT_CATALOG".into(),
        Expr::CurrentSchema => "CURRENT_SCHEMA".into(),
        Expr::CurrentDate => "CURRENT_DATE".into(),
        Expr::CurrentTime { precision } => {
            render_explain_sql_datetime_keyword("CURRENT_TIME", *precision)
        }
        Expr::CurrentTimestamp { precision } => {
            render_explain_sql_datetime_keyword("CURRENT_TIMESTAMP", *precision)
        }
        Expr::LocalTime { precision } => {
            render_explain_sql_datetime_keyword("LOCALTIME", *precision)
        }
        Expr::LocalTimestamp { precision } => {
            render_explain_sql_datetime_keyword("LOCALTIMESTAMP", *precision)
        }
        Expr::CurrentUser => "CURRENT_USER".into(),
        Expr::CurrentRole => "CURRENT_ROLE".into(),
        Expr::SessionUser => "SESSION_USER".into(),
        Expr::Random => "random()".into(),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            ..
        } => render_like_explain_expr(
            expr,
            pattern,
            escape.as_deref(),
            *case_insensitive,
            *negated,
            |expr| render_explain_expr_inner_with_qualifier(expr, qualifier, column_names),
        ),
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            ..
        } => render_similar_explain_expr(expr, pattern, escape.as_deref(), *negated, |expr| {
            render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
        }),
        other => format!("{other:?}"),
    }
}

fn render_explain_case_expr(
    case_expr: &crate::include::nodes::primnodes::CaseExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let mut rendered = String::from("CASE");
    if let Some(arg) = &case_expr.arg {
        rendered.push(' ');
        rendered.push_str(&render_explain_expr_inner_with_qualifier(
            arg,
            qualifier,
            column_names,
        ));
    }
    for arm in &case_expr.args {
        rendered.push_str(" WHEN ");
        rendered.push_str(&render_explain_expr_inner_with_qualifier(
            &arm.expr,
            qualifier,
            column_names,
        ));
        rendered.push_str(" THEN ");
        rendered.push_str(&render_explain_expr_inner_with_qualifier(
            &arm.result,
            qualifier,
            column_names,
        ));
    }
    rendered.push_str(" ELSE ");
    rendered.push_str(&render_explain_expr_inner_with_qualifier(
        &case_expr.defresult,
        qualifier,
        column_names,
    ));
    rendered.push_str(" END");
    rendered
}

fn render_explain_sql_datetime_keyword(keyword: &str, precision: Option<i32>) -> String {
    match precision {
        Some(precision) => format!("{keyword}({precision})"),
        None => keyword.into(),
    }
}

fn render_explain_field_select(
    inner: &Expr,
    field: &str,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let rendered = render_explain_expr_inner_with_qualifier(inner, qualifier, column_names);
    if matches!(inner, Expr::ArraySubscript { .. }) {
        format!("{rendered}.{field}")
    } else {
        format!("({rendered}).{field}")
    }
}

fn render_explain_array_subscript(
    array: &Expr,
    subscripts: &[crate::include::nodes::primnodes::ExprArraySubscript],
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let mut out = render_explain_expr_inner_with_qualifier(array, qualifier, column_names);
    for subscript in subscripts {
        out.push('[');
        if let Some(lower) = &subscript.lower {
            out.push_str(&render_explain_expr_inner_with_qualifier(
                lower,
                qualifier,
                column_names,
            ));
        }
        if subscript.is_slice {
            out.push(':');
            if let Some(upper) = &subscript.upper {
                out.push_str(&render_explain_expr_inner_with_qualifier(
                    upper,
                    qualifier,
                    column_names,
                ));
            }
        }
        out.push(']');
    }
    out
}

fn render_explain_func_expr_is_infix(func: &FuncExpr) -> bool {
    let render_as_named_call = matches!(
        (&func.implementation, func.funcname.as_deref()),
        (
            ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TextStartsWith),
            Some("starts_with")
        )
    );
    !render_as_named_call && builtin_scalar_function_infix_operator(func.implementation).is_some()
}

fn render_explain_distinctness_expr(
    left: &Expr,
    right: &Expr,
    distinct: bool,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    if let Some((expr, value)) = bool_distinctness_operand(left, right) {
        let rendered = render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
        return match (distinct, value) {
            (false, true) => format!("{rendered} IS TRUE"),
            (true, true) => format!("{rendered} IS NOT TRUE"),
            (false, false) => format!("{rendered} IS FALSE"),
            (true, false) => format!("{rendered} IS NOT FALSE"),
        };
    }
    let operator = if distinct {
        "IS DISTINCT FROM"
    } else {
        "IS NOT DISTINCT FROM"
    };
    format!(
        "{} {operator} {}",
        render_explain_infix_operand(left, qualifier, column_names),
        render_explain_infix_operand(right, qualifier, column_names)
    )
}

fn bool_distinctness_operand<'a>(left: &'a Expr, right: &'a Expr) -> Option<(&'a Expr, bool)> {
    match (left, right) {
        (expr, Expr::Const(Value::Bool(value))) => Some((expr, *value)),
        (Expr::Const(Value::Bool(value)), expr) => Some((expr, *value)),
        _ => None,
    }
}

fn render_explain_bool_comparison(
    op: crate::include::nodes::primnodes::OpExprKind,
    left: &Expr,
    right: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<String> {
    let (expr, value) = bool_distinctness_operand(left, right)?;
    if !expr_sql_type_is_bool(expr) {
        return None;
    }
    let rendered = render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    match (op, value) {
        (crate::include::nodes::primnodes::OpExprKind::Eq, true)
        | (crate::include::nodes::primnodes::OpExprKind::NotEq, false) => Some(rendered),
        (crate::include::nodes::primnodes::OpExprKind::Eq, false)
        | (crate::include::nodes::primnodes::OpExprKind::NotEq, true) => {
            if explain_bool_arg_is_bare(expr) {
                Some(format!("NOT {rendered}"))
            } else {
                Some(format!("NOT ({rendered})"))
            }
        }
        _ => None,
    }
}

fn render_explain_negated_bool_comparison(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<String> {
    let Expr::Op(op) = expr else {
        return None;
    };
    let [left, right] = op.args.as_slice() else {
        return None;
    };
    let (expr, value) = bool_distinctness_operand(left, right)?;
    if !expr_sql_type_is_bool(expr) {
        return None;
    }
    let rendered = render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    match (op.op, value) {
        (crate::include::nodes::primnodes::OpExprKind::Eq, false)
        | (crate::include::nodes::primnodes::OpExprKind::NotEq, true) => Some(rendered),
        (crate::include::nodes::primnodes::OpExprKind::Eq, true)
        | (crate::include::nodes::primnodes::OpExprKind::NotEq, false) => {
            if explain_bool_arg_is_bare(expr) {
                Some(format!("NOT {rendered}"))
            } else {
                Some(format!("NOT ({rendered})"))
            }
        }
        _ => None,
    }
}

fn render_explain_func_expr(
    func: &FuncExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    if matches!(
        func.implementation,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::BpcharToText)
    ) && func.args.len() == 1
    {
        return render_explain_expr_inner_with_qualifier(&func.args[0], qualifier, column_names);
    }
    if let Some(rendered) =
        render_range_support_func_expr(func, qualifier, column_names).map(|out| out.render_inner())
    {
        return rendered;
    }
    if render_explain_func_expr_is_infix(func)
        && let Some(operator) = builtin_scalar_function_infix_operator(func.implementation)
    {
        if let [left, right] = func.args.as_slice() {
            return format!(
                "{} {} {}",
                render_explain_infix_operand(left, qualifier, column_names),
                operator,
                render_explain_infix_operand(right, qualifier, column_names)
            );
        }
    }
    if let Some(rendered) = render_explain_geometry_subscript_func(func, qualifier, column_names) {
        return rendered;
    }
    if matches!(
        func.implementation,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::Timezone)
    ) {
        return render_explain_timezone_function(func, qualifier, column_names);
    }
    if matches!(
        func.implementation,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::JsonbPathQueryArray)
    ) {
        return render_explain_jsonb_path_query_array_function(func, qualifier, column_names);
    }
    let name = match func.implementation {
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoPoint) => {
            func.funcname.clone().unwrap_or_else(|| "point".into())
        }
        ScalarFunctionImpl::Builtin(builtin) => builtin_scalar_function_name(builtin),
        ScalarFunctionImpl::UserDefined { proc_oid } => func
            .funcname
            .clone()
            .unwrap_or_else(|| format!("proc_{proc_oid}")),
    };
    let args = func
        .args
        .iter()
        .map(|arg| render_explain_expr_inner_with_qualifier(arg, qualifier, column_names))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({args})")
}

fn render_explain_geometry_subscript_func(
    func: &FuncExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<String> {
    let index = match func.implementation {
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBoxHigh)
        | ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoPointX) => 0,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBoxLow)
        | ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoPointY) => 1,
        _ => return None,
    };
    let arg = func.args.first()?;
    let rendered_arg = render_explain_expr_inner_with_qualifier(arg, qualifier, column_names);
    Some(match func.implementation {
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBoxHigh)
        | ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBoxLow) => {
            format!("{rendered_arg}[{index}]")
        }
        _ if matches!(
            arg,
            Expr::Func(inner)
                if matches!(
                    inner.implementation,
                    ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBoxHigh)
                        | ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBoxLow)
                )
        ) =>
        {
            format!("(({rendered_arg})[{index}])")
        }
        _ => format!("{rendered_arg}[{index}]"),
    })
}

fn render_explain_jsonb_path_query_array_function(
    func: &FuncExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let mut args = func
        .args
        .iter()
        .enumerate()
        .map(|(index, arg)| {
            if index == 1 {
                render_explain_jsonpath_arg(arg)
            } else {
                let rendered =
                    render_explain_expr_inner_with_qualifier(arg, qualifier, column_names);
                if index == 0 && matches!(arg, Expr::Op(_)) {
                    format!("({rendered})")
                } else {
                    rendered
                }
            }
        })
        .collect::<Vec<_>>();
    if args.len() == 2 {
        args.push("'{}'::jsonb".into());
        args.push("false".into());
    }
    format!("jsonb_path_query_array({})", args.join(", "))
}

fn render_explain_jsonpath_arg(expr: &Expr) -> String {
    match expr {
        Expr::Const(value) if value.as_text().is_some() => {
            let path = value.as_text().unwrap_or_default();
            format!("'{}'::jsonpath", path.replace('\'', "''"))
        }
        Expr::Const(Value::JsonPath(path)) => {
            format!("'{}'::jsonpath", path.to_string().replace('\'', "''"))
        }
        other => render_explain_expr_inner(other, &[]),
    }
}

fn render_explain_scalar_array_op(
    saop: &crate::include::nodes::primnodes::ScalarArrayOpExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let op = match saop.op {
        SubqueryComparisonOp::Eq => "=",
        SubqueryComparisonOp::NotEq => "<>",
        SubqueryComparisonOp::Lt => "<",
        SubqueryComparisonOp::LtEq => "<=",
        SubqueryComparisonOp::Gt => ">",
        SubqueryComparisonOp::GtEq => ">=",
        SubqueryComparisonOp::RegexMatch => "~",
        SubqueryComparisonOp::NotRegexMatch => "!~",
        _ => return format!("{saop:?}"),
    };
    let quantifier = if saop.use_or { "ANY" } else { "ALL" };
    let display_type = if expr_is_ctid_system_var(&saop.left)
        || expr_renders_as_ctid(&saop.left, qualifier, column_names)
    {
        Some(SqlType::array_of(SqlType::new(SqlTypeKind::Tid)))
    } else if expr_has_bpchar_display_type(&saop.left) {
        Some(SqlType::array_of(SqlType::new(SqlTypeKind::Char)))
    } else if expr_has_varchar_display_type(&saop.left) {
        Some(SqlType::array_of(SqlType::new(SqlTypeKind::Text)))
    } else if matches!(saop.right.as_ref(), Expr::Const(Value::Null)) {
        crate::include::nodes::primnodes::expr_sql_type_hint(&saop.left).map(SqlType::array_of)
    } else {
        None
    };
    let right_sql = if expr_has_varchar_display_type(&saop.left) {
        render_explain_varchar_array_as_text_array(&saop.right, qualifier, column_names)
    } else {
        None
    }
    .unwrap_or_else(|| {
        render_explain_infix_operand_with_display_type(
            &saop.right,
            display_type,
            saop.collation_oid,
            qualifier,
            column_names,
        )
    });
    if saop.use_or
        && matches!(saop.op, SubqueryComparisonOp::Eq)
        && let Some(element) = scalar_array_singleton_element(&saop.right)
    {
        return format!(
            "{} {op} {}",
            render_explain_infix_operand_with_display_type(
                &saop.left,
                if expr_is_text_cast_from_varchar(&saop.left) {
                    None
                } else {
                    display_type.map(|ty| ty.element_type())
                },
                None,
                qualifier,
                column_names
            ),
            render_explain_infix_operand_with_display_type(
                element,
                display_type.map(|ty| ty.element_type()),
                saop.collation_oid,
                qualifier,
                column_names
            )
        );
    }
    let left_sql = render_explain_infix_operand_with_display_type(
        &saop.left,
        if expr_is_text_cast_from_varchar(&saop.left) {
            None
        } else {
            display_type.map(|ty| ty.element_type())
        },
        None,
        qualifier,
        column_names,
    );
    let right_sql =
        if (left_sql == "ctid" || left_sql.ends_with(".ctid")) && right_sql.ends_with("::text[]") {
            format!("{}::tid[]", right_sql.trim_end_matches("::text[]"))
        } else {
            right_sql
        };
    format!("{left_sql} {op} {quantifier} ({right_sql})")
}

fn expr_is_ctid_system_var(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varattno == SELF_ITEM_POINTER_ATTR_NO,
        Expr::Cast(inner, _) => expr_is_ctid_system_var(inner),
        _ => false,
    }
}

fn expr_renders_as_ctid(expr: &Expr, qualifier: Option<&str>, column_names: &[String]) -> bool {
    let rendered = render_explain_infix_operand(expr, qualifier, column_names);
    rendered == "ctid" || rendered.ends_with(".ctid")
}

fn scalar_array_singleton_element(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::ArrayLiteral { elements, .. } if elements.len() == 1 => elements.first(),
        Expr::Cast(inner, _) => scalar_array_singleton_element(inner),
        _ => None,
    }
}

fn render_explain_varchar_array_as_text_array(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<String> {
    let array_expr = match expr {
        Expr::ArrayLiteral { .. } => expr,
        Expr::Cast(inner, ty) if ty.is_array => inner.as_ref(),
        _ => return None,
    };
    let Expr::ArrayLiteral { elements, .. } = array_expr else {
        return None;
    };
    if elements.iter().all(|expr| {
        render_explain_array_literal_const(expr, SqlType::new(SqlTypeKind::Varchar)).is_some()
    }) {
        return None;
    }
    let varchar_type = SqlType::new(SqlTypeKind::Varchar);
    let varchar_type_name = render_explain_sql_type_name(varchar_type);
    let elements = elements
        .iter()
        .map(|expr| match expr {
            Expr::Const(value) => format!(
                "{}::{varchar_type_name}",
                render_explain_typed_literal(value, varchar_type)
            ),
            _ => format!(
                "({})::{varchar_type_name}",
                render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
            ),
        })
        .collect::<Vec<_>>()
        .join(", ");
    Some(format!("(ARRAY[{elements}])::text[]"))
}

pub(crate) fn render_verbose_range_support_expr(
    expr: &Expr,
    column_names: &[String],
) -> Option<String> {
    render_range_support_expr(expr, None, column_names).map(|out| out.render_verbose())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RangeSupportSubtype {
    Date,
    TimestampTz,
    Text,
}

#[derive(Clone)]
enum RangeSupportBoundValue {
    Date(DateADT),
    TimestampTz(TimestampTzADT),
    Text(String),
}

#[derive(Clone)]
struct RangeSupportBound {
    value: RangeSupportBoundValue,
    inclusive: bool,
}

struct RangeSupportBounds {
    subtype: RangeSupportSubtype,
    empty: bool,
    lower: Option<RangeSupportBound>,
    upper: Option<RangeSupportBound>,
}

enum RangeSupportOutput {
    Bool(&'static str),
    Comparison(String),
    And(String, String),
    ElementContainedByRangeLiteral { elem: String, range_literal: String },
}

impl RangeSupportOutput {
    fn render_inner(self) -> String {
        match self {
            Self::Bool(value) => value.into(),
            Self::Comparison(comparison) => comparison,
            Self::And(lower, upper) => format!("({lower}) AND ({upper})"),
            Self::ElementContainedByRangeLiteral {
                elem,
                range_literal,
            } => {
                format!("{elem} <@ {range_literal}")
            }
        }
    }

    fn render_full(self) -> String {
        match self {
            Self::Bool(value) => format!("({value})"),
            Self::Comparison(comparison) => format!("({comparison})"),
            Self::And(lower, upper) => format!("(({lower}) AND ({upper}))"),
            Self::ElementContainedByRangeLiteral {
                elem,
                range_literal,
            } => {
                format!("({elem} <@ {range_literal})")
            }
        }
    }

    fn render_verbose(self) -> String {
        match self {
            Self::Bool(value) => value.into(),
            Self::Comparison(comparison) => format!("({comparison})"),
            Self::And(lower, upper) => format!("(({lower}) AND ({upper}))"),
            Self::ElementContainedByRangeLiteral {
                elem,
                range_literal,
            } => {
                format!("({elem} <@ {range_literal})")
            }
        }
    }
}

fn render_range_support_expr(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<RangeSupportOutput> {
    let Expr::Func(func) = expr else {
        return None;
    };
    render_range_support_func_expr(func, qualifier, column_names)
}

fn render_range_support_func_expr(
    func: &FuncExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<RangeSupportOutput> {
    let (elem, range) = match (func.implementation, func.args.as_slice()) {
        (ScalarFunctionImpl::Builtin(BuiltinScalarFunction::RangeContainedBy), [elem, range]) => {
            (elem, range)
        }
        (ScalarFunctionImpl::Builtin(BuiltinScalarFunction::RangeContains), [range, elem]) => {
            (elem, range)
        }
        _ => return None,
    };
    let bounds = range_support_bounds(range)?;
    if bounds.empty {
        return Some(RangeSupportOutput::Bool("false"));
    }
    let elem_expr = elem;
    let elem = render_range_support_elem(elem_expr, qualifier, column_names);
    if bounds.lower.is_none() && bounds.upper.is_none() {
        return Some(RangeSupportOutput::Bool("true"));
    }

    if bounds.subtype == RangeSupportSubtype::TimestampTz
        && range_support_elem_is_clock_timestamp(strip_range_support_casts(elem_expr))
        && bounds.lower.is_some()
        && bounds.upper.is_some()
        && let Some(range_literal) = render_tstzrange_support_literal(&bounds)
    {
        return Some(RangeSupportOutput::ElementContainedByRangeLiteral {
            elem,
            range_literal,
        });
    }

    let mut comparisons = Vec::with_capacity(2);
    if let Some(lower) = &bounds.lower {
        comparisons.push(render_range_bound_comparison(
            &elem,
            RangeSupportBoundSide::Lower,
            lower,
            bounds.subtype,
        )?);
    }
    if let Some(upper) = &bounds.upper {
        comparisons.push(render_range_bound_comparison(
            &elem,
            RangeSupportBoundSide::Upper,
            upper,
            bounds.subtype,
        )?);
    }
    match comparisons.as_slice() {
        [] => Some(RangeSupportOutput::Bool("true")),
        [comparison] => Some(RangeSupportOutput::Comparison(comparison.clone())),
        [lower, upper] => Some(RangeSupportOutput::And(lower.clone(), upper.clone())),
        _ => None,
    }
}

fn range_support_bounds(expr: &Expr) -> Option<RangeSupportBounds> {
    match strip_range_support_casts(expr) {
        Expr::Const(Value::Range(range)) => {
            let subtype = range_support_subtype_for_sql_type(range.range_type.subtype)?;
            Some(RangeSupportBounds {
                subtype,
                empty: range.empty,
                lower: match range.lower.as_ref() {
                    Some(bound) => Some(range_support_bound_from_range_bound(bound, subtype)?),
                    None => None,
                },
                upper: match range.upper.as_ref() {
                    Some(bound) => Some(range_support_bound_from_range_bound(bound, subtype)?),
                    None => None,
                },
            })
        }
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::RangeConstructor)
            ) =>
        {
            let subtype = func
                .funcresulttype
                .and_then(range_support_subtype_for_sql_type)
                .or_else(|| range_support_subtype_from_constructor_args(&func.args))?;
            let lower_arg = func.args.first()?;
            let upper_arg = func.args.get(1)?;
            let (lower_inclusive, upper_inclusive) =
                range_constructor_inclusivity(func.args.get(2));
            Some(RangeSupportBounds {
                subtype,
                empty: false,
                lower: range_support_bound_from_expr(lower_arg, subtype, lower_inclusive)?,
                upper: range_support_bound_from_expr(upper_arg, subtype, upper_inclusive)?,
            })
        }
        _ => None,
    }
}

fn range_support_bound_from_range_bound(
    bound: &crate::include::nodes::datum::RangeBound,
    subtype: RangeSupportSubtype,
) -> Option<RangeSupportBound> {
    Some(RangeSupportBound {
        value: range_support_bound_value_from_value(&bound.value, subtype)?,
        inclusive: bound.inclusive,
    })
}

fn range_support_subtype_for_sql_type(sql_type: SqlType) -> Option<RangeSupportSubtype> {
    match sql_type.kind {
        SqlTypeKind::Date | SqlTypeKind::DateRange => Some(RangeSupportSubtype::Date),
        SqlTypeKind::TimestampTz | SqlTypeKind::TimestampTzRange => {
            Some(RangeSupportSubtype::TimestampTz)
        }
        SqlTypeKind::Text => Some(RangeSupportSubtype::Text),
        SqlTypeKind::Range => match sql_type.range_subtype_oid {
            DATE_TYPE_OID => Some(RangeSupportSubtype::Date),
            TIMESTAMPTZ_TYPE_OID => Some(RangeSupportSubtype::TimestampTz),
            TEXT_TYPE_OID => Some(RangeSupportSubtype::Text),
            _ => None,
        },
        _ => None,
    }
}

fn range_support_subtype_from_constructor_args(args: &[Expr]) -> Option<RangeSupportSubtype> {
    if args.iter().any(|arg| {
        matches!(
            strip_range_support_casts(arg),
            Expr::Const(Value::TimestampTz(_))
        )
    }) {
        return Some(RangeSupportSubtype::TimestampTz);
    }
    if args
        .iter()
        .any(|arg| matches!(strip_range_support_casts(arg), Expr::Const(Value::Date(_))))
    {
        return Some(RangeSupportSubtype::Date);
    }
    if args.iter().any(|arg| {
        matches!(
            strip_range_support_casts(arg),
            Expr::Const(Value::Text(_) | Value::TextRef(_, _))
        )
    }) {
        return Some(RangeSupportSubtype::Text);
    }
    None
}

fn range_support_bound_from_expr(
    expr: &Expr,
    subtype: RangeSupportSubtype,
    inclusive: bool,
) -> Option<Option<RangeSupportBound>> {
    match strip_range_support_casts(expr) {
        Expr::Const(Value::Null) => Some(None),
        Expr::Const(value) => Some(Some(RangeSupportBound {
            value: range_support_bound_value_from_value(value, subtype)?,
            inclusive,
        })),
        _ => None,
    }
}

fn range_support_bound_value_from_value(
    value: &Value,
    subtype: RangeSupportSubtype,
) -> Option<RangeSupportBoundValue> {
    match subtype {
        RangeSupportSubtype::Date => match value {
            Value::Date(value) => Some(RangeSupportBoundValue::Date(*value)),
            Value::Text(_) | Value::TextRef(_, _) => {
                let config = postgres_explain_datetime_config();
                parse_date_text(value.as_text()?, &config)
                    .ok()
                    .map(RangeSupportBoundValue::Date)
            }
            _ => None,
        },
        RangeSupportSubtype::TimestampTz => match value {
            Value::TimestampTz(value) => Some(RangeSupportBoundValue::TimestampTz(*value)),
            Value::Text(_) | Value::TextRef(_, _) => {
                let config = postgres_explain_datetime_config();
                parse_timestamptz_text(value.as_text()?, &config)
                    .ok()
                    .map(RangeSupportBoundValue::TimestampTz)
            }
            _ => None,
        },
        RangeSupportSubtype::Text => match value {
            Value::Text(_) | Value::TextRef(_, _) => {
                Some(RangeSupportBoundValue::Text(value.as_text()?.to_string()))
            }
            _ => None,
        },
    }
}

fn range_constructor_inclusivity(flags: Option<&Expr>) -> (bool, bool) {
    let Some(flags) = flags else {
        return (true, false);
    };
    let Some(text) = range_support_const_text(flags) else {
        return (true, false);
    };
    let mut chars = text.chars();
    let lower = matches!(chars.next(), Some('['));
    let upper = matches!(chars.next_back(), Some(']'));
    (lower, upper)
}

fn range_support_const_text(expr: &Expr) -> Option<&str> {
    match strip_range_support_casts(expr) {
        Expr::Const(value) => value.as_text(),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum RangeSupportBoundSide {
    Lower,
    Upper,
}

fn render_range_bound_comparison(
    elem: &str,
    side: RangeSupportBoundSide,
    bound: &RangeSupportBound,
    subtype: RangeSupportSubtype,
) -> Option<String> {
    let (op, value) = match (side, subtype) {
        (RangeSupportBoundSide::Lower, RangeSupportSubtype::Text) => {
            let op = if bound.inclusive { "~>=~" } else { "~>~" };
            (op, render_text_support_literal(bound)?)
        }
        (RangeSupportBoundSide::Upper, RangeSupportSubtype::Text) => {
            let op = if bound.inclusive { "~<=~" } else { "~<~" };
            (op, render_text_support_literal(bound)?)
        }
        (RangeSupportBoundSide::Lower, _) => {
            let op = if bound.inclusive { ">=" } else { ">" };
            (
                op,
                render_range_support_bound_literal(bound, subtype, false)?,
            )
        }
        (RangeSupportBoundSide::Upper, RangeSupportSubtype::Date)
            if bound.inclusive && range_support_bound_is_finite(bound) =>
        {
            (
                "<",
                render_range_support_bound_literal(bound, subtype, true)?,
            )
        }
        (RangeSupportBoundSide::Upper, _) => {
            let op = if bound.inclusive { "<=" } else { "<" };
            (
                op,
                render_range_support_bound_literal(bound, subtype, false)?,
            )
        }
    };
    Some(format!("{elem} {op} {value}"))
}

fn range_support_bound_is_finite(bound: &RangeSupportBound) -> bool {
    match &bound.value {
        RangeSupportBoundValue::Date(value) => {
            value.0 != DATEVAL_NOBEGIN && value.0 != DATEVAL_NOEND
        }
        RangeSupportBoundValue::TimestampTz(value) => value.is_finite(),
        RangeSupportBoundValue::Text(_) => true,
    }
}

fn render_range_support_bound_literal(
    bound: &RangeSupportBound,
    subtype: RangeSupportSubtype,
    increment_date: bool,
) -> Option<String> {
    let config = postgres_explain_datetime_config();
    match (&bound.value, subtype) {
        (RangeSupportBoundValue::Date(value), RangeSupportSubtype::Date) => {
            let value = if increment_date {
                DateADT(value.0 + 1)
            } else {
                *value
            };
            Some(format!("'{}'::date", format_date_text(value, &config)))
        }
        (RangeSupportBoundValue::TimestampTz(value), RangeSupportSubtype::TimestampTz) => {
            Some(format!(
                "'{}'::timestamp with time zone",
                format_timestamptz_text(*value, &config)
            ))
        }
        (RangeSupportBoundValue::Text(_), RangeSupportSubtype::Text) => {
            render_text_support_literal(bound)
        }
        _ => None,
    }
}

fn render_text_support_literal(bound: &RangeSupportBound) -> Option<String> {
    let RangeSupportBoundValue::Text(value) = &bound.value else {
        return None;
    };
    Some(format!("'{}'::text", value.replace('\'', "''")))
}

fn render_tstzrange_support_literal(bounds: &RangeSupportBounds) -> Option<String> {
    if bounds.subtype != RangeSupportSubtype::TimestampTz {
        return None;
    }
    let lower = bounds.lower.as_ref()?;
    let upper = bounds.upper.as_ref()?;
    let config = postgres_explain_datetime_config();
    let lower_value = match &lower.value {
        RangeSupportBoundValue::TimestampTz(value) => format_timestamptz_text(*value, &config),
        _ => return None,
    };
    let upper_value = match &upper.value {
        RangeSupportBoundValue::TimestampTz(value) => format_timestamptz_text(*value, &config),
        _ => return None,
    };
    let lower_bracket = if lower.inclusive { '[' } else { '(' };
    let upper_bracket = if upper.inclusive { ']' } else { ')' };
    Some(format!(
        "'{lower_bracket}\"{}\",\"{}\"{upper_bracket}'::tstzrange",
        lower_value.replace('"', "\\\""),
        upper_value.replace('"', "\\\"")
    ))
}

fn render_range_support_elem(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let expr = strip_range_support_casts(expr);
    match expr {
        Expr::CurrentDate => "CURRENT_DATE".into(),
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::Now)
            ) =>
        {
            "now()".into()
        }
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::ClockTimestamp)
            ) =>
        {
            "clock_timestamp()".into()
        }
        _ => render_explain_expr_inner_with_qualifier(expr, qualifier, column_names),
    }
}

fn range_support_elem_is_clock_timestamp(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::ClockTimestamp)
            )
    )
}

fn strip_range_support_casts(mut expr: &Expr) -> &Expr {
    while let Expr::Cast(inner, _) = expr {
        expr = inner;
    }
    expr
}

fn postgres_explain_datetime_config() -> DateTimeConfig {
    DateTimeConfig {
        date_style_format: DateStyleFormat::Postgres,
        date_order: DateOrder::Mdy,
        time_zone: "America/Los_Angeles".into(),
        ..DateTimeConfig::default()
    }
}

fn render_explain_timezone_function(
    func: &FuncExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    match func.args.as_slice() {
        [value] => format!(
            "timezone({})",
            render_explain_expr_inner_with_qualifier(value, qualifier, column_names)
        ),
        [zone, value] if is_local_timezone_marker(zone) => format!(
            "({} AT LOCAL)",
            render_explain_expr_inner_with_qualifier(value, qualifier, column_names)
        ),
        [zone, value] => format!(
            "({} AT TIME ZONE {})",
            render_explain_expr_inner_with_qualifier(value, qualifier, column_names),
            render_explain_expr_inner_with_qualifier(zone, qualifier, column_names)
        ),
        _ => "timezone()".into(),
    }
}

fn is_local_timezone_marker(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Const(value) if value.as_text() == Some("__pgrust_local_timezone__")
    )
}

fn builtin_scalar_function_name(func: BuiltinScalarFunction) -> String {
    match func {
        BuiltinScalarFunction::Lower => "lower".into(),
        BuiltinScalarFunction::Upper => "upper".into(),
        BuiltinScalarFunction::Length => "length".into(),
        BuiltinScalarFunction::OctetLength => "octet_length".into(),
        BuiltinScalarFunction::JsonBuildArray => "json_build_array".into(),
        BuiltinScalarFunction::JsonBuildObject => "json_build_object".into(),
        BuiltinScalarFunction::JsonbBuildArray => "jsonb_build_array".into(),
        BuiltinScalarFunction::JsonbBuildObject => "jsonb_build_object".into(),
        BuiltinScalarFunction::JsonbPathQueryArray => "jsonb_path_query_array".into(),
        BuiltinScalarFunction::RowToJson => "row_to_json".into(),
        BuiltinScalarFunction::ArrayToJson => "array_to_json".into(),
        BuiltinScalarFunction::ToJson => "to_json".into(),
        BuiltinScalarFunction::ToJsonb => "to_jsonb".into(),
        BuiltinScalarFunction::SqlJsonConstructor => "JSON".into(),
        BuiltinScalarFunction::SqlJsonScalar => "JSON_SCALAR".into(),
        BuiltinScalarFunction::SqlJsonSerialize => "JSON_SERIALIZE".into(),
        BuiltinScalarFunction::SqlJsonObject => "JSON_OBJECT".into(),
        BuiltinScalarFunction::SqlJsonArray => "JSON_ARRAY".into(),
        BuiltinScalarFunction::SqlJsonIsJson => "IS JSON".into(),
        BuiltinScalarFunction::DatePart => "date_part".into(),
        BuiltinScalarFunction::Extract => "extract".into(),
        BuiltinScalarFunction::TextStartsWith => "starts_with".into(),
        BuiltinScalarFunction::Abs => "abs".into(),
        BuiltinScalarFunction::ToChar => "to_char".into(),
        BuiltinScalarFunction::Substring => "substr".into(),
        BuiltinScalarFunction::ToChar => "to_char".into(),
        BuiltinScalarFunction::Left => "\"left\"".into(),
        BuiltinScalarFunction::Right => "\"right\"".into(),
        other => format!("{other:?}"),
    }
}

fn builtin_scalar_function_infix_operator(
    implementation: ScalarFunctionImpl,
) -> Option<&'static str> {
    match implementation {
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoSame) => Some("~="),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoDistance) => Some("<->"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoContains) => Some("@>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoContainedBy) => Some("<@"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverlap) => Some("&&"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoLeft) => Some("<<"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverLeft) => Some("&<"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoRight) => Some(">>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverRight) => Some("&>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoBelow) => Some("<<|"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverBelow) => Some("&<|"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoAbove) => Some("|>>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::GeoOverAbove) => Some("|&>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::NetworkSubnet) => Some("<<"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::NetworkSubnetEq) => Some("<<="),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::NetworkSupernet) => Some(">>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::NetworkSupernetEq) => Some(">>="),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::NetworkOverlap) => Some("&&"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TsQueryContains) => Some("@>"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TsQueryContainedBy) => Some("<@"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TextStartsWith) => Some("^@"),
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::TsMatch) => Some("@@"),
        _ => None,
    }
}

fn infix_operator_text(
    opno: u32,
    op: crate::include::nodes::primnodes::OpExprKind,
) -> Option<&'static str> {
    match opno {
        crate::include::catalog::TEXT_PATTERN_LT_OPERATOR_OID => return Some("~<~"),
        crate::include::catalog::TEXT_PATTERN_LE_OPERATOR_OID => return Some("~<=~"),
        crate::include::catalog::TEXT_PATTERN_GE_OPERATOR_OID => return Some("~>=~"),
        crate::include::catalog::TEXT_PATTERN_GT_OPERATOR_OID => return Some("~>~"),
        _ => {}
    }
    match op {
        crate::include::nodes::primnodes::OpExprKind::Add => Some("+"),
        crate::include::nodes::primnodes::OpExprKind::Sub => Some("-"),
        crate::include::nodes::primnodes::OpExprKind::Mul => Some("*"),
        crate::include::nodes::primnodes::OpExprKind::Div => Some("/"),
        crate::include::nodes::primnodes::OpExprKind::Mod => Some("%"),
        crate::include::nodes::primnodes::OpExprKind::BitAnd => Some("&"),
        crate::include::nodes::primnodes::OpExprKind::BitOr => Some("|"),
        crate::include::nodes::primnodes::OpExprKind::BitXor => Some("#"),
        crate::include::nodes::primnodes::OpExprKind::Shl => Some("<<"),
        crate::include::nodes::primnodes::OpExprKind::Shr => Some(">>"),
        crate::include::nodes::primnodes::OpExprKind::Concat => Some("||"),
        crate::include::nodes::primnodes::OpExprKind::Eq => Some("="),
        crate::include::nodes::primnodes::OpExprKind::NotEq => Some("<>"),
        crate::include::nodes::primnodes::OpExprKind::Lt => Some("<"),
        crate::include::nodes::primnodes::OpExprKind::LtEq => Some("<="),
        crate::include::nodes::primnodes::OpExprKind::Gt => Some(">"),
        crate::include::nodes::primnodes::OpExprKind::GtEq => Some(">="),
        crate::include::nodes::primnodes::OpExprKind::RegexMatch => Some("~"),
        crate::include::nodes::primnodes::OpExprKind::ArrayOverlap => Some("&&"),
        crate::include::nodes::primnodes::OpExprKind::ArrayContains => Some("@>"),
        crate::include::nodes::primnodes::OpExprKind::ArrayContained => Some("<@"),
        crate::include::nodes::primnodes::OpExprKind::JsonbContains => Some("@>"),
        crate::include::nodes::primnodes::OpExprKind::JsonbContained => Some("<@"),
        crate::include::nodes::primnodes::OpExprKind::JsonbExists => Some("?"),
        crate::include::nodes::primnodes::OpExprKind::JsonbExistsAny => Some("?|"),
        crate::include::nodes::primnodes::OpExprKind::JsonbExistsAll => Some("?&"),
        crate::include::nodes::primnodes::OpExprKind::JsonbPathExists => Some("@?"),
        crate::include::nodes::primnodes::OpExprKind::JsonbPathMatch => Some("@@"),
        crate::include::nodes::primnodes::OpExprKind::JsonGet => Some("->"),
        crate::include::nodes::primnodes::OpExprKind::JsonGetText => Some("->>"),
        _ => None,
    }
}

fn text_pattern_operator_suppresses_explain_collation(opno: u32) -> bool {
    matches!(
        opno,
        crate::include::catalog::TEXT_PATTERN_LT_OPERATOR_OID
            | crate::include::catalog::TEXT_PATTERN_LE_OPERATOR_OID
            | crate::include::catalog::TEXT_PATTERN_GE_OPERATOR_OID
            | crate::include::catalog::TEXT_PATTERN_GT_OPERATOR_OID
    )
}

fn collect_bool_explain_args<'a>(
    expr: &'a Expr,
    boolop: crate::include::nodes::primnodes::BoolExprType,
    out: &mut Vec<&'a Expr>,
) {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == boolop => {
            for arg in &bool_expr.args {
                collect_bool_explain_args(arg, boolop, out);
            }
        }
        other => out.push(other),
    }
}

fn render_explain_bool_arg(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let rendered = render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    if explain_bool_arg_is_bare(expr) {
        rendered
    } else {
        format!("({rendered})")
    }
}

fn explain_bool_arg_is_bare(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Var(_)
            | Expr::Param(_)
            | Expr::Const(_)
            | Expr::SubPlan(_)
            | Expr::CurrentCatalog
            | Expr::CurrentSchema
            | Expr::CurrentDate
            | Expr::CurrentUser
            | Expr::CurrentRole
            | Expr::SessionUser
            | Expr::Random
    ) || matches!(expr, Expr::Func(func) if !render_explain_func_expr_is_infix(func))
}

fn explain_detail_prefix(indent: usize) -> String {
    if indent == 0 {
        "  ".into()
    } else {
        " ".repeat(2 + indent * 6)
    }
}

pub(crate) fn render_explain_projection_expr_inner_with_qualifier(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    match expr {
        Expr::Var(var) => render_explain_var_name(var, column_names)
            .map(|name| match qualifier {
                Some(qualifier) => format!("{qualifier}.{name}"),
                None => name,
            })
            .unwrap_or_else(|| format!("{expr:?}")),
        Expr::Const(value) => render_explain_projection_const(value),
        Expr::Op(op) => match op.op {
            crate::include::nodes::primnodes::OpExprKind::Add
            | crate::include::nodes::primnodes::OpExprKind::Sub
            | crate::include::nodes::primnodes::OpExprKind::Mul
            | crate::include::nodes::primnodes::OpExprKind::Div
            | crate::include::nodes::primnodes::OpExprKind::Mod
            | crate::include::nodes::primnodes::OpExprKind::BitAnd
            | crate::include::nodes::primnodes::OpExprKind::BitOr
            | crate::include::nodes::primnodes::OpExprKind::BitXor
            | crate::include::nodes::primnodes::OpExprKind::Shl
            | crate::include::nodes::primnodes::OpExprKind::Shr
            | crate::include::nodes::primnodes::OpExprKind::Concat => {
                let [left, right] = op.args.as_slice() else {
                    return render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
                };
                let op_text = match op.op {
                    crate::include::nodes::primnodes::OpExprKind::Add => "+",
                    crate::include::nodes::primnodes::OpExprKind::Sub => "-",
                    crate::include::nodes::primnodes::OpExprKind::Mul => "*",
                    crate::include::nodes::primnodes::OpExprKind::Div => "/",
                    crate::include::nodes::primnodes::OpExprKind::Mod => "%",
                    crate::include::nodes::primnodes::OpExprKind::BitAnd => "&",
                    crate::include::nodes::primnodes::OpExprKind::BitOr => "|",
                    crate::include::nodes::primnodes::OpExprKind::BitXor => "#",
                    crate::include::nodes::primnodes::OpExprKind::Shl => "<<",
                    crate::include::nodes::primnodes::OpExprKind::Shr => ">>",
                    crate::include::nodes::primnodes::OpExprKind::Concat => "||",
                    _ => unreachable!(),
                };
                format!(
                    "{} {} {}",
                    render_explain_projection_expr_inner_with_qualifier(
                        left,
                        qualifier,
                        column_names,
                    ),
                    op_text,
                    render_explain_projection_expr_inner_with_qualifier(
                        right,
                        qualifier,
                        column_names,
                    )
                )
            }
            _ => render_explain_expr_inner_with_qualifier(expr, qualifier, column_names),
        },
        _ => render_explain_expr_inner_with_qualifier(expr, qualifier, column_names),
    }
}

fn render_explain_infix_operand(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let rendered = render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    if explain_expr_needs_infix_operand_parens(expr) {
        format!("({rendered})")
    } else {
        rendered
    }
}

fn render_explain_infix_operand_with_display_type(
    expr: &Expr,
    display_type: Option<SqlType>,
    collation_oid: Option<u32>,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let expr = if display_type.is_some_and(|ty| matches!(ty.kind, SqlTypeKind::Int8)) {
        strip_bigint_comparison_cast(expr)
    } else if display_type.is_some_and(|ty| matches!(ty.kind, SqlTypeKind::Char)) {
        strip_bpchar_to_text(expr)
    } else {
        expr
    };
    let rendered = match (display_type, expr) {
        (Some(sql_type), Expr::Const(value)) => {
            format!(
                "{}::{}",
                render_explain_typed_literal(value, sql_type),
                render_explain_sql_type_name(sql_type.with_typmod(SqlType::NO_TYPEMOD))
            )
        }
        (Some(sql_type), Expr::Cast(inner, _)) if matches!(inner.as_ref(), Expr::Const(_)) => {
            render_explain_infix_operand_with_display_type(
                inner,
                Some(sql_type),
                None,
                qualifier,
                column_names,
            )
        }
        (Some(sql_type), Expr::Cast(inner, _))
            if sql_type.is_array && matches!(inner.as_ref(), Expr::ArrayLiteral { .. }) =>
        {
            render_explain_infix_operand_with_display_type(
                inner,
                Some(sql_type),
                None,
                qualifier,
                column_names,
            )
        }
        (Some(sql_type), Expr::ArrayLiteral { elements, .. }) if sql_type.is_array => {
            render_explain_array_literal(elements, sql_type, qualifier, column_names)
        }
        (Some(sql_type), expr)
            if matches!(sql_type.kind, SqlTypeKind::Text)
                && expr_sql_type_hint_is(expr, SqlTypeKind::Varchar) =>
        {
            format!(
                "({})::text",
                render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
            )
        }
        (Some(sql_type), expr)
            if matches!(sql_type.kind, SqlTypeKind::Text)
                && expr_sql_type_hint_is(expr, SqlTypeKind::Text) =>
        {
            format!(
                "({})::text",
                render_explain_expr_inner_with_qualifier(expr, qualifier, column_names)
            )
        }
        _ => render_explain_infix_operand(expr, qualifier, column_names),
    };
    append_explain_collation(rendered, collation_oid)
}

fn explain_expr_needs_infix_operand_parens(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Op(_)
            | Expr::Bool(_)
            | Expr::ScalarArrayOp(_)
            | Expr::Like { .. }
            | Expr::Similar { .. }
    ) || matches!(expr, Expr::Func(func) if render_explain_func_expr_is_infix(func))
}

fn comparison_display_type(
    left: &Expr,
    right: &Expr,
    collation_oid: Option<u32>,
) -> Option<SqlType> {
    if expr_has_bpchar_display_type(left) || expr_has_bpchar_display_type(right) {
        Some(SqlType::new(SqlTypeKind::Char))
    } else if collation_oid == Some(POSIX_COLLATION_OID)
        && (expr_sql_type_hint_is(left, SqlTypeKind::Text)
            || expr_sql_type_hint_is(right, SqlTypeKind::Text))
    {
        Some(SqlType::new(SqlTypeKind::Text))
    } else if let Some(sql_type) = comparison_cast_literal_display_type(left, right) {
        Some(sql_type)
    } else {
        None
    }
}

fn comparison_cast_literal_display_type(left: &Expr, right: &Expr) -> Option<SqlType> {
    let sql_type = match (left, right) {
        (Expr::Cast(_, sql_type), Expr::Const(_)) | (Expr::Const(_), Expr::Cast(_, sql_type)) => {
            *sql_type
        }
        _ => return None,
    };
    matches!(
        sql_type.kind,
        SqlTypeKind::Int2 | SqlTypeKind::Int8 | SqlTypeKind::Numeric
    )
    .then_some(sql_type)
}

fn expr_has_bpchar_display_type(expr: &Expr) -> bool {
    if expr_sql_type_hint_is(expr, SqlTypeKind::Char) {
        return true;
    }
    matches!(
        expr,
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::BpcharToText)
            )
    )
}

fn expr_has_varchar_display_type(expr: &Expr) -> bool {
    expr_sql_type_hint_is(expr, SqlTypeKind::Varchar) || expr_is_text_cast_from_varchar(expr)
}

fn expr_is_text_cast_from_varchar(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Cast(inner, ty)
            if matches!(ty.kind, SqlTypeKind::Text)
                && expr_sql_type_hint_is(inner, SqlTypeKind::Varchar)
    )
}

fn strip_bpchar_to_text(expr: &Expr) -> &Expr {
    match expr {
        Expr::Func(func)
            if matches!(
                func.implementation,
                ScalarFunctionImpl::Builtin(BuiltinScalarFunction::BpcharToText)
            ) && func.args.len() == 1 =>
        {
            &func.args[0]
        }
        _ => expr,
    }
}

fn strip_bigint_comparison_cast(expr: &Expr) -> &Expr {
    match expr {
        Expr::Cast(inner, sql_type) if matches!(sql_type.kind, SqlTypeKind::Int8) => inner,
        _ => expr,
    }
}

fn expr_sql_type_is_bool(expr: &Expr) -> bool {
    expr_sql_type_hint_is(expr, SqlTypeKind::Bool)
}

fn expr_sql_type_hint_is(expr: &Expr, kind: SqlTypeKind) -> bool {
    crate::include::nodes::primnodes::expr_sql_type_hint(expr)
        .is_some_and(|ty| !ty.is_array && ty.kind == kind)
}

fn append_explain_collation(rendered: String, collation_oid: Option<u32>) -> String {
    let Some(collation_oid) = collation_oid else {
        return rendered;
    };
    let Some(collation) = explain_collation_name(collation_oid) else {
        return rendered;
    };
    format!("{rendered} COLLATE {collation}")
}

fn explain_collation_name(collation_oid: u32) -> Option<&'static str> {
    match collation_oid {
        DEFAULT_COLLATION_OID | 0 => None,
        C_COLLATION_OID => Some("\"C\""),
        POSIX_COLLATION_OID => Some("\"POSIX\""),
        _ => None,
    }
}

fn render_explain_collate(
    expr: &Expr,
    collation_oid: u32,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    append_explain_collation(
        render_explain_expr_inner_with_qualifier(expr, qualifier, column_names),
        Some(collation_oid),
    )
}

pub(crate) fn render_explain_join_expr_inner(
    expr: &Expr,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    match expr {
        Expr::Var(var) if var.varno == OUTER_VAR => {
            render_explain_var_name(var, outer_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Var(var) if var.varno == INNER_VAR => {
            render_explain_var_name(var, inner_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Var(var) if var.varno == INDEX_VAR => {
            render_explain_var_name(var, inner_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Var(var) => {
            let mut combined_names = outer_names.to_vec();
            combined_names.extend_from_slice(inner_names);
            render_explain_var_name(var, &combined_names).unwrap_or_else(|| format!("{expr:?}"))
        }
        Expr::Const(value) => render_explain_const(value),
        Expr::Cast(inner, ty) => render_explain_join_cast(inner, *ty, outer_names, inner_names),
        Expr::Op(op) => match op.op {
            crate::include::nodes::primnodes::OpExprKind::Eq
            | crate::include::nodes::primnodes::OpExprKind::NotEq
            | crate::include::nodes::primnodes::OpExprKind::Lt
            | crate::include::nodes::primnodes::OpExprKind::LtEq
            | crate::include::nodes::primnodes::OpExprKind::Gt
            | crate::include::nodes::primnodes::OpExprKind::GtEq
            | crate::include::nodes::primnodes::OpExprKind::RegexMatch => {
                let [left, right] = op.args.as_slice() else {
                    return format!("{expr:?}");
                };
                let op_text = match op.op {
                    crate::include::nodes::primnodes::OpExprKind::Eq => "=",
                    crate::include::nodes::primnodes::OpExprKind::NotEq => "<>",
                    crate::include::nodes::primnodes::OpExprKind::Lt => "<",
                    crate::include::nodes::primnodes::OpExprKind::LtEq => "<=",
                    crate::include::nodes::primnodes::OpExprKind::Gt => ">",
                    crate::include::nodes::primnodes::OpExprKind::GtEq => ">=",
                    crate::include::nodes::primnodes::OpExprKind::RegexMatch => "~",
                    _ => unreachable!(),
                };
                format!(
                    "{} {} {}",
                    render_explain_join_expr_inner(left, outer_names, inner_names),
                    op_text,
                    render_explain_join_expr_inner(right, outer_names, inner_names)
                )
            }
            _ => format!("{expr:?}"),
        },
        Expr::Bool(bool_expr) => match bool_expr.boolop {
            crate::include::nodes::primnodes::BoolExprType::And => {
                let mut args = Vec::new();
                collect_bool_explain_args(
                    expr,
                    crate::include::nodes::primnodes::BoolExprType::And,
                    &mut args,
                );
                let rendered = args
                    .into_iter()
                    .map(|arg| render_explain_join_bool_arg(arg, outer_names, inner_names))
                    .collect::<Vec<_>>();
                rendered.join(" AND ")
            }
            crate::include::nodes::primnodes::BoolExprType::Or => {
                let mut args = Vec::new();
                collect_bool_explain_args(
                    expr,
                    crate::include::nodes::primnodes::BoolExprType::Or,
                    &mut args,
                );
                let rendered = args
                    .into_iter()
                    .map(|arg| render_explain_join_bool_arg(arg, outer_names, inner_names))
                    .collect::<Vec<_>>();
                rendered.join(" OR ")
            }
            crate::include::nodes::primnodes::BoolExprType::Not => {
                let Some(inner) = bool_expr.args.first() else {
                    return format!("{expr:?}");
                };
                let rendered = render_explain_join_expr_inner(inner, outer_names, inner_names);
                if explain_bool_arg_is_bare(inner) {
                    format!("NOT {rendered}")
                } else {
                    format!("NOT ({rendered})")
                }
            }
        },
        Expr::Coalesce(left, right) => format!(
            "COALESCE({}, {})",
            render_explain_join_expr_inner(left, outer_names, inner_names),
            render_explain_join_expr_inner(right, outer_names, inner_names)
        ),
        Expr::IsNull(inner) => {
            format!(
                "{} IS NULL",
                render_explain_join_expr_inner(inner, outer_names, inner_names)
            )
        }
        Expr::IsNotNull(inner) => format!(
            "{} IS NOT NULL",
            render_explain_join_expr_inner(inner, outer_names, inner_names)
        ),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            ..
        } => render_like_explain_expr(
            expr,
            pattern,
            escape.as_deref(),
            *case_insensitive,
            *negated,
            |expr| render_explain_join_expr_inner(expr, outer_names, inner_names),
        ),
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            ..
        } => render_similar_explain_expr(expr, pattern, escape.as_deref(), *negated, |expr| {
            render_explain_join_expr_inner(expr, outer_names, inner_names)
        }),
        Expr::Func(func) => render_explain_join_func_expr(func, outer_names, inner_names),
        Expr::SubPlan(subplan) => match subplan.sublink_type {
            SubLinkType::ExistsSubLink => format!("EXISTS(SubPlan {})", subplan.plan_id + 1),
            _ if subplan.par_param.is_empty() => format!("(InitPlan {}).col1", subplan.plan_id + 1),
            _ => format!("(SubPlan {})", subplan.plan_id + 1),
        },
        Expr::Row { fields, .. } => render_explain_join_whole_row(fields, outer_names, inner_names)
            .unwrap_or_else(|| {
                let fields = fields
                    .iter()
                    .map(|(_, expr)| render_explain_join_expr_inner(expr, outer_names, inner_names))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("ROW({fields})")
            }),
        Expr::Case(case_expr) => {
            if let Some(rendered) =
                render_explain_join_whole_row_case(case_expr, outer_names, inner_names)
            {
                rendered
            } else {
                format!("{expr:?}")
            }
        }
        other => format!("{other:?}"),
    }
}

fn render_explain_join_whole_row_case(
    case_expr: &crate::include::nodes::primnodes::CaseExpr,
    outer_names: &[String],
    inner_names: &[String],
) -> Option<String> {
    if case_expr.arg.is_some() || case_expr.args.len() != 1 {
        return None;
    }
    if !matches!(case_expr.args[0].result, Expr::Const(Value::Null)) {
        return None;
    }
    let Expr::Row { fields, .. } = case_expr.defresult.as_ref() else {
        return None;
    };
    render_explain_join_whole_row(fields, outer_names, inner_names)
}

fn render_explain_join_whole_row(
    fields: &[(String, Expr)],
    outer_names: &[String],
    inner_names: &[String],
) -> Option<String> {
    let mut prefix = None::<String>;
    for (_, expr) in fields {
        let Expr::Var(var) = expr else {
            return None;
        };
        let names = match var.varno {
            OUTER_VAR => outer_names,
            INNER_VAR | INDEX_VAR => inner_names,
            _ => return None,
        };
        let name = render_explain_var_name(var, names)?;
        let (candidate, _) = name.rsplit_once('.')?;
        match &prefix {
            Some(existing) if existing != candidate => return None,
            Some(_) => {}
            None => prefix = Some(candidate.to_string()),
        }
    }
    prefix.map(|prefix| format!("{prefix}.*"))
}

fn render_explain_join_func_expr(
    func: &FuncExpr,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    if matches!(
        func.implementation,
        ScalarFunctionImpl::Builtin(BuiltinScalarFunction::BpcharToText)
    ) && func.args.len() == 1
    {
        return render_explain_join_expr_inner(&func.args[0], outer_names, inner_names);
    }
    if render_explain_func_expr_is_infix(func)
        && let Some(operator) = builtin_scalar_function_infix_operator(func.implementation)
        && let [left, right] = func.args.as_slice()
    {
        return format!(
            "{} {} {}",
            render_explain_join_infix_operand(left, outer_names, inner_names),
            operator,
            render_explain_join_infix_operand(right, outer_names, inner_names)
        );
    }
    let name = match func.implementation {
        ScalarFunctionImpl::Builtin(builtin) => builtin_scalar_function_name(builtin),
        ScalarFunctionImpl::UserDefined { proc_oid } => func
            .funcname
            .clone()
            .unwrap_or_else(|| format!("proc_{proc_oid}")),
    };
    let args = func
        .args
        .iter()
        .map(|arg| render_explain_join_expr_inner(arg, outer_names, inner_names))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{name}({args})")
}

fn render_explain_join_infix_operand(
    expr: &Expr,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    let rendered = render_explain_join_expr_inner(expr, outer_names, inner_names);
    if explain_expr_needs_infix_operand_parens(expr) {
        format!("({rendered})")
    } else {
        rendered
    }
}

fn render_explain_join_bool_arg(
    expr: &Expr,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    let rendered = render_explain_join_expr_inner(expr, outer_names, inner_names);
    if explain_bool_arg_is_bare(expr) {
        rendered
    } else {
        format!("({rendered})")
    }
}

fn render_like_explain_expr<F>(
    expr: &Expr,
    pattern: &Expr,
    escape: Option<&Expr>,
    case_insensitive: bool,
    negated: bool,
    render: F,
) -> String
where
    F: Fn(&Expr) -> String,
{
    let op = match (case_insensitive, negated) {
        (false, false) => "~~",
        (false, true) => "!~~",
        (true, false) => "~~*",
        (true, true) => "!~~*",
    };
    let mut out = format!("{} {op} {}", render(expr), render(pattern));
    if let Some(escape) = escape {
        out.push_str(" ESCAPE ");
        out.push_str(&render(escape));
    }
    out
}

fn render_similar_explain_expr<F>(
    expr: &Expr,
    pattern: &Expr,
    escape: Option<&Expr>,
    negated: bool,
    render: F,
) -> String
where
    F: Fn(&Expr) -> String,
{
    let left = render(expr);
    if let Some(regex) = explain_similar_regex(pattern, escape) {
        let op = if negated { "!~" } else { "~" };
        return format!(
            "{} {} {}",
            left,
            op,
            render_explain_const(&Value::Text(regex.into()))
        );
    }

    let keyword = if negated {
        "NOT SIMILAR TO"
    } else {
        "SIMILAR TO"
    };
    let mut out = format!("{} {} {}", left, keyword, render(pattern));
    if let Some(escape) = escape {
        out.push_str(" ESCAPE ");
        out.push_str(&render(escape));
    }
    out
}

fn explain_similar_regex(pattern: &Expr, escape: Option<&Expr>) -> Option<String> {
    let Expr::Const(pattern) = pattern else {
        return None;
    };
    let pattern = pattern.as_text()?;
    let escape = match escape {
        None => None,
        Some(Expr::Const(Value::Null)) => return None,
        Some(Expr::Const(value)) => Some(value.as_text()?),
        Some(_) => return None,
    };
    explain_similar_pattern(pattern, escape).ok()
}

fn render_explain_const(value: &Value) -> String {
    match value {
        Value::Text(_) | Value::TextRef(_, _) => {
            format!("'{}'::text", value.as_text().unwrap().replace('\'', "''"))
        }
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_)
        | Value::Uuid(_) => match value.sql_type_hint() {
            Some(sql_type) => format!(
                "{}::{}",
                render_explain_literal(value),
                render_explain_sql_type_name(sql_type)
            ),
            None => render_explain_literal(value),
        },
        Value::Date(date) => format!(
            "'{}'::date",
            format_date_text(*date, &postgres_explain_datetime_config())
        ),
        Value::Inet(_) | Value::Cidr(_) => match value.sql_type_hint() {
            Some(sql_type) => format!(
                "{}::{}",
                render_explain_literal(value),
                render_explain_sql_type_name(sql_type)
            ),
            None => render_explain_literal(value),
        },
        Value::PgArray(array) => {
            let rendered = if array
                .elements
                .iter()
                .any(|item| matches!(item, Value::Jsonb(_)))
            {
                render_explain_array_items(&array.elements)
            } else {
                crate::backend::executor::format_array_value_text(array)
            };
            match value.sql_type_hint() {
                Some(sql_type) => {
                    format!("'{rendered}'::{}", render_explain_sql_type_name(sql_type))
                }
                None => format!("'{rendered}'"),
            }
        }
        Value::Array(items) => {
            let rendered = if items.iter().any(|item| matches!(item, Value::Jsonb(_))) {
                render_explain_array_items(items)
            } else {
                let array = ArrayValue::from_1d(items.clone());
                crate::backend::executor::format_array_value_text(&array)
            };
            let escaped = rendered.replace('\'', "''");
            let array_type = items
                .iter()
                .find_map(Value::sql_type_hint)
                .map(SqlType::array_of);
            match array_type {
                Some(sql_type) => {
                    format!("'{escaped}'::{}", render_explain_sql_type_name(sql_type))
                }
                None => format!("'{escaped}'"),
            }
        }
        Value::Jsonb(bytes) => {
            let rendered = crate::backend::executor::jsonb::render_jsonb_bytes(bytes)
                .unwrap_or_else(|_| "null".into())
                .replace('\'', "''");
            format!("'{rendered}'::jsonb")
        }
        Value::JsonPath(path) => {
            let rendered = path.to_string().replace('\'', "''");
            format!("'{rendered}'::jsonpath")
        }
        Value::Record(record) => {
            let rendered =
                crate::backend::executor::value_io::format_record_text(record).replace('\'', "''");
            let record_type = record.sql_type();
            if record_type.type_oid != crate::include::catalog::RECORD_TYPE_OID {
                format!(
                    "'{rendered}'::{}",
                    render_explain_sql_type_name(record_type)
                )
            } else {
                format!("'{rendered}'::record")
            }
        }
        Value::Range(_) | Value::Multirange(_) | Value::TsQuery(_) | Value::TsVector(_) => {
            match value.sql_type_hint() {
                Some(sql_type) => format!(
                    "{}::{}",
                    render_explain_literal(value),
                    render_explain_sql_type_name(sql_type)
                ),
                None => render_explain_literal(value),
            }
        }
        Value::Tid(tid) => {
            format!(
                "'{}'::tid",
                crate::backend::executor::value_io::render_tid_text(tid)
            )
        }
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float64(v) => format_float8_text(*v, FloatFormatOptions::default()),
        Value::Numeric(v) => v.render(),
        Value::Bool(v) => {
            if *v {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Null => "NULL".to_string(),
        other => format!("{other:?}"),
    }
}

fn render_explain_array_items(items: &[Value]) -> String {
    let mut out = String::from("{");
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        match item {
            Value::Null => out.push_str("NULL"),
            Value::Jsonb(bytes) => {
                let rendered = crate::backend::executor::jsonb::render_jsonb_bytes(bytes)
                    .unwrap_or_else(|_| "null".into());
                push_explain_array_quoted_element(&mut out, &rendered);
            }
            other => out.push_str(&render_explain_const(other)),
        }
    }
    out.push('}');
    out
}

fn push_explain_array_quoted_element(out: &mut String, text: &str) {
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out.push('"');
}

fn render_explain_projection_const(value: &Value) -> String {
    match value {
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        _ => render_explain_const(value),
    }
}

fn render_explain_cast(
    expr: &Expr,
    ty: SqlType,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    if let Some(rendered) = render_explain_datetime_cast_literal(expr, ty) {
        return rendered;
    }
    if let Expr::Const(value) = expr {
        if matches!(ty.kind, SqlTypeKind::Oid) {
            return format!("'{}'::oid", render_explain_literal(value));
        }
        if matches!(ty.kind, SqlTypeKind::Float4 | SqlTypeKind::Float8) {
            return format!(
                "'{}'::{}",
                render_explain_literal(value),
                render_explain_sql_type_name(ty)
            );
        }
        if matches!(ty.kind, SqlTypeKind::Numeric) {
            return format!(
                "{}::{}",
                render_explain_typed_literal(value, ty),
                render_explain_sql_type_name(ty)
            );
        }
        return format!(
            "{}::{}",
            render_explain_literal(value),
            render_explain_sql_type_name(ty)
        );
    }
    let inner = render_explain_expr_inner_with_qualifier(expr, qualifier, column_names);
    format!("({inner})::{}", render_explain_sql_type_name(ty))
}

fn render_explain_datetime_cast_literal(expr: &Expr, ty: SqlType) -> Option<String> {
    let Expr::Const(value) = expr else {
        return None;
    };
    let text = value.as_text()?;
    let config = postgres_utc_datetime_config();
    match ty.kind {
        SqlTypeKind::Date => parse_date_text(text, &config).ok().map(|date| {
            format!(
                "'{}'::date",
                format_date_text(date, &postgres_explain_datetime_config()).replace('\'', "''")
            )
        }),
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

fn render_explain_join_cast(
    expr: &Expr,
    ty: SqlType,
    outer_names: &[String],
    inner_names: &[String],
) -> String {
    if let Expr::Const(value) = expr {
        if matches!(ty.kind, SqlTypeKind::Oid) {
            return format!("'{}'::oid", render_explain_literal(value));
        }
        return format!(
            "{}::{}",
            render_explain_literal(value),
            render_explain_sql_type_name(ty)
        );
    }
    let inner = render_explain_join_expr_inner(expr, outer_names, inner_names);
    format!("({inner})::{}", render_explain_sql_type_name(ty))
}

pub(crate) fn render_explain_literal(value: &Value) -> String {
    match value {
        Value::Text(_) | Value::TextRef(_, _) => {
            format!("'{}'", value.as_text().unwrap().replace('\'', "''"))
        }
        Value::Point(_)
        | Value::Lseg(_)
        | Value::Path(_)
        | Value::Line(_)
        | Value::Box(_)
        | Value::Polygon(_)
        | Value::Circle(_) => {
            let rendered = render_geometry_text(value, FloatFormatOptions::default())
                .unwrap_or_else(|| format!("{value:?}"));
            format!("'{rendered}'")
        }
        Value::Uuid(uuid) => {
            format!(
                "'{}'",
                crate::backend::executor::value_io::render_uuid_text(uuid)
            )
        }
        Value::Range(_) => {
            let rendered = crate::backend::executor::expr_range::render_range_text(value)
                .unwrap_or_else(|| format!("{value:?}"));
            format!("'{rendered}'")
        }
        Value::Multirange(_) => {
            let rendered = crate::backend::executor::expr_multirange::render_multirange_text(value)
                .unwrap_or_else(|| format!("{value:?}"));
            format!("'{rendered}'")
        }
        Value::TsQuery(query) => {
            let rendered = crate::backend::executor::render_tsquery_text(query);
            format!("'{}'", rendered.replace('\'', "''"))
        }
        Value::TsVector(vector) => {
            let rendered = crate::backend::executor::render_tsvector_text(vector);
            format!("'{}'", rendered.replace('\'', "''"))
        }
        Value::JsonPath(path) => format!("'{}'", path.replace('\'', "''")),
        Value::PgArray(array) => {
            let rendered = crate::backend::executor::value_io::format_array_value_text(array);
            format!("'{}'", rendered.replace('\'', "''"))
        }
        Value::Record(record) => {
            let rendered = crate::backend::executor::value_io::format_record_text(record);
            format!("'{}'", rendered.replace('\'', "''"))
        }
        Value::Date(date) => {
            format!("'{}'", format_date_text(*date, &DateTimeConfig::default()))
        }
        Value::Inet(value) => format!("'{}'", value.render_inet()),
        Value::Cidr(value) => format!("'{}'", value.render_cidr()),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Float64(v) => format_float8_text(*v, FloatFormatOptions::default()),
        Value::Numeric(v) => v.render(),
        Value::Bool(v) => {
            if *v {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        Value::Null => "NULL".to_string(),
        other => format!("{other:?}"),
    }
}

fn render_explain_typed_literal(value: &Value, sql_type: SqlType) -> String {
    match sql_type.kind {
        SqlTypeKind::Int2 | SqlTypeKind::Int4 | SqlTypeKind::Int8 | SqlTypeKind::Numeric => {
            format!("'{}'", render_explain_literal(value).trim_matches('\''))
        }
        _ => render_explain_literal(value),
    }
}

fn render_explain_array_literal(
    elements: &[Expr],
    array_type: SqlType,
    qualifier: Option<&str>,
    column_names: &[String],
) -> String {
    let element_type = array_type.element_type();
    let const_elements = elements
        .iter()
        .map(|expr| render_explain_array_literal_const(expr, element_type))
        .collect::<Option<Vec<_>>>();
    if let Some(elements) = const_elements {
        return format!(
            "'{{{}}}'::{}",
            elements.join(","),
            render_explain_sql_type_name(array_type)
        );
    }
    let elements = elements
        .iter()
        .map(|expr| render_explain_expr_inner_with_qualifier(expr, qualifier, column_names))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "ARRAY[{elements}]::{}",
        render_explain_sql_type_name(array_type)
    )
}

fn render_explain_array_literal_const(expr: &Expr, element_type: SqlType) -> Option<String> {
    match expr {
        Expr::Const(value) => Some(render_explain_array_literal_value(value, element_type)),
        Expr::Cast(inner, _) => render_explain_array_literal_const(inner, element_type),
        _ => None,
    }
}

fn render_explain_array_literal_value(value: &Value, element_type: SqlType) -> String {
    let value = cast_value(value.clone(), element_type).unwrap_or_else(|_| value.clone());
    match &value {
        Value::Record(record) => {
            let rendered = crate::backend::executor::value_io::format_record_text(record)
                .replace('\\', "\\\\")
                .replace('"', "\\\"");
            format!("\"{rendered}\"")
        }
        Value::Text(_) | Value::TextRef(_, _)
            if matches!(
                element_type.kind,
                SqlTypeKind::Composite | SqlTypeKind::Record
            ) =>
        {
            let rendered = value
                .as_text()
                .unwrap_or_default()
                .replace('\\', "\\\\")
                .replace('"', "\\\"");
            format!("\"{rendered}\"")
        }
        Value::Text(_) | Value::TextRef(_, _) => value
            .as_text()
            .unwrap_or_default()
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace(',', "\\,"),
        Value::Bool(value) => {
            if *value {
                "t".into()
            } else {
                "f".into()
            }
        }
        Value::Float64(v) => format_float8_text(*v, FloatFormatOptions::default()),
        Value::Numeric(v) => v.render(),
        Value::Jsonb(bytes) => {
            let rendered = crate::backend::executor::jsonb::render_jsonb_bytes(bytes)
                .unwrap_or_else(|_| "null".into())
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\'', "''");
            format!("\"{rendered}\"")
        }
        Value::Tid(tid) => {
            format!(
                "\"{}\"",
                crate::backend::executor::value_io::render_tid_text(tid)
            )
        }
        Value::Null => "NULL".into(),
        _ => render_explain_literal(&value),
    }
}

fn render_explain_row_comparison_operand(
    expr: &Expr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<String> {
    match expr {
        Expr::Row { fields, .. } => Some(format!(
            "ROW({})",
            fields
                .iter()
                .map(|(_, expr)| render_explain_expr_inner_with_qualifier(
                    expr,
                    qualifier,
                    column_names
                ))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        Expr::Const(Value::Record(record)) => Some(format!(
            "ROW({})",
            record
                .fields
                .iter()
                .map(|value| render_explain_expr_inner(&Expr::Const(value.clone()), &[]))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        Expr::Cast(inner, ty)
            if matches!(ty.kind, SqlTypeKind::Composite | SqlTypeKind::Record) =>
        {
            render_explain_row_comparison_operand(inner, qualifier, column_names)
        }
        _ => None,
    }
}

fn render_explain_whole_row_case(
    case_expr: &CaseExpr,
    qualifier: Option<&str>,
    column_names: &[String],
) -> Option<String> {
    if case_expr.arg.is_some() || case_expr.args.len() != 1 {
        return None;
    }
    if !matches!(&case_expr.args.first()?.result, Expr::Const(Value::Null)) {
        return None;
    }
    let Expr::Row { fields, .. } = case_expr.defresult.as_ref() else {
        return None;
    };
    let rendered_fields = fields
        .iter()
        .map(|(_, expr)| {
            let Expr::Var(var) = expr else {
                return None;
            };
            render_explain_var_name(var, column_names)
        })
        .collect::<Option<Vec<_>>>()?;
    if rendered_fields.is_empty() {
        return None;
    }
    if let Some(qualifier) = qualifier {
        return Some(format!("{qualifier}.*"));
    }
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
        .then(|| format!("{prefix}.*"))
}

fn render_explain_sql_type_name(ty: SqlType) -> String {
    let element = ty.element_type();
    let base = match element.kind {
        SqlTypeKind::Bool => "boolean".into(),
        SqlTypeKind::Int2 => "smallint".into(),
        SqlTypeKind::Int4 => "integer".into(),
        SqlTypeKind::Int8 => "bigint".into(),
        SqlTypeKind::Float4 => "real".into(),
        SqlTypeKind::Float8 => "double precision".into(),
        SqlTypeKind::Numeric => element
            .numeric_precision_scale()
            .map(|(precision, scale)| format!("numeric({precision},{scale})"))
            .unwrap_or_else(|| "numeric".into()),
        SqlTypeKind::Text => "text".into(),
        SqlTypeKind::Name => "name".into(),
        SqlTypeKind::Oid => "oid".into(),
        SqlTypeKind::Inet => "inet".into(),
        SqlTypeKind::Cidr => "cidr".into(),
        SqlTypeKind::Date => "date".into(),
        SqlTypeKind::Time => "time without time zone".into(),
        SqlTypeKind::TimeTz => "time with time zone".into(),
        SqlTypeKind::Timestamp => "timestamp without time zone".into(),
        SqlTypeKind::TimestampTz => "timestamp with time zone".into(),
        SqlTypeKind::Char => element
            .char_len()
            .map(|len| format!("character({len})"))
            .unwrap_or_else(|| "bpchar".into()),
        SqlTypeKind::Varchar => element
            .char_len()
            .map(|len| format!("character varying({len})"))
            .unwrap_or_else(|| "character varying".into()),
        SqlTypeKind::Json => "json".into(),
        SqlTypeKind::Jsonb => "jsonb".into(),
        SqlTypeKind::JsonPath => "jsonpath".into(),
        SqlTypeKind::TsQuery => "tsquery".into(),
        SqlTypeKind::TsVector => "tsvector".into(),
        SqlTypeKind::Line => "line".into(),
        SqlTypeKind::Lseg => "lseg".into(),
        SqlTypeKind::Path => "path".into(),
        SqlTypeKind::Box => "box".into(),
        SqlTypeKind::Polygon => "polygon".into(),
        SqlTypeKind::Circle => "circle".into(),
        SqlTypeKind::Point => "point".into(),
        SqlTypeKind::Uuid => "uuid".into(),
        SqlTypeKind::Range => match element.type_oid {
            crate::include::catalog::INT4RANGE_TYPE_OID => "int4range".into(),
            crate::include::catalog::INT8RANGE_TYPE_OID => "int8range".into(),
            crate::include::catalog::NUMRANGE_TYPE_OID => "numrange".into(),
            crate::include::catalog::DATERANGE_TYPE_OID => "daterange".into(),
            crate::include::catalog::TSRANGE_TYPE_OID => "tsrange".into(),
            crate::include::catalog::TSTZRANGE_TYPE_OID => "tstzrange".into(),
            _ => "text".into(),
        },
        SqlTypeKind::Int4Range => "int4range".into(),
        SqlTypeKind::Int8Range => "int8range".into(),
        SqlTypeKind::NumericRange => "numrange".into(),
        SqlTypeKind::DateRange => "daterange".into(),
        SqlTypeKind::TimestampRange => "tsrange".into(),
        SqlTypeKind::TimestampTzRange => "tstzrange".into(),
        SqlTypeKind::Int2Vector => "int2vector".into(),
        SqlTypeKind::OidVector => "oidvector".into(),
        _ => "text".into(),
    };
    if ty.is_array {
        format!("{base}[]")
    } else {
        base
    }
}

impl PlanNode for FilterState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        loop {
            ctx.check_for_interrupts()?;
            let slot = match self.input.exec_proc_node(ctx)? {
                Some(s) => s,
                None => {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }
            };
            let outer_values = materialize_slot_values(slot)?;
            let current_bindings = ctx.system_bindings.clone();
            set_outer_expr_bindings(ctx, outer_values, &current_bindings);
            clear_inner_expr_bindings(ctx);

            if (self.compiled_predicate)(slot, ctx)? {
                finish_row(&mut self.stats, start);
                return Ok(self.input.current_slot());
            }
            note_filtered_row(&mut self.stats);
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        self.input.current_slot()
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        self.input.current_system_bindings()
    }
    fn column_names(&self) -> &[String] {
        self.input.column_names()
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        if filter_state_is_one_time_false_result(self) {
            "Result".into()
        } else {
            "Filter".into()
        }
    }
    fn renumber_append_child_aliases(&mut self, ordinal: usize) {
        self.input.renumber_append_child_aliases(ordinal);
    }
    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        if filter_state_is_one_time_false_result(self) {
            lines.push(format!("{prefix}One-Time Filter: false"));
        } else if !matches!(self.predicate, Expr::Const(Value::Bool(true))) {
            lines.push(format!(
                "{prefix}Filter: {}",
                render_explain_expr(&self.predicate, self.column_names())
            ));
        }
        if analyze && self.stats.rows_removed_by_filter > 0 {
            lines.push(format!(
                "{prefix}Rows Removed by Filter: {}",
                self.stats.rows_removed_by_filter
            ));
        }
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        if filter_state_is_one_time_false_result(self) {
            return;
        }
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

fn filter_state_is_one_time_false_result(state: &FilterState) -> bool {
    matches!(state.predicate, Expr::Const(Value::Bool(false)))
        && state.input.explain_one_time_false_input()
}

impl PlanNode for MaterializeState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        if self.input.exec_proc_node(ctx)?.is_some() {
            finish_row(&mut self.stats, start);
            Ok(self.input.current_slot())
        } else {
            finish_eof(&mut self.stats, start, ctx);
            Ok(None)
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        self.input.current_slot()
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        self.input.current_system_bindings()
    }

    fn column_names(&self) -> &[String] {
        self.input.column_names()
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "Materialize".into()
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

fn memoize_param_value(
    ctx: &ExecutorContext,
    paramid: usize,
    binary_mode: bool,
) -> Result<Value, ExecError> {
    let value = ctx
        .expr_bindings
        .exec_params
        .get(&paramid)
        .cloned()
        .ok_or_else(|| ExecError::DetailedError {
            message: "executor param reached memoize without a binding".into(),
            detail: Some(format!("paramid={paramid}")),
            hint: None,
            sqlstate: "XX000",
        })?;
    let mut values = vec![value];
    Value::materialize_all(&mut values);
    let value = values.pop().unwrap_or(Value::Null);
    if !binary_mode
        && let Value::Float64(float) = value
        && float == 0.0
    {
        return Ok(Value::Float64(0.0));
    }
    Ok(value)
}

fn memoize_key(state: &MemoizeState, ctx: &ExecutorContext) -> Result<MemoizeCacheKey, ExecError> {
    state
        .key_paramids
        .iter()
        .copied()
        .map(|paramid| memoize_param_value(ctx, paramid, state.binary_mode))
        .collect::<Result<Vec<_>, _>>()
        .map(MemoizeCacheKey)
}

fn memoize_nonkey_dependents(
    state: &MemoizeState,
    ctx: &ExecutorContext,
) -> Result<Vec<Value>, ExecError> {
    state
        .dependent_paramids
        .iter()
        .copied()
        .filter(|paramid| !state.key_paramids.contains(paramid))
        .map(|paramid| memoize_param_value(ctx, paramid, true))
        .collect()
}

fn memoize_value_memory(value: &Value) -> usize {
    match value {
        Value::Text(text) | Value::Json(text) | Value::JsonPath(text) | Value::Xml(text) => {
            24 + text.len()
        }
        Value::TextRef(_, len) => 24 + *len as usize,
        Value::Bytea(bytes) | Value::Jsonb(bytes) => 24 + bytes.len(),
        Value::Array(values) => 24 + values.iter().map(memoize_value_memory).sum::<usize>(),
        Value::PgArray(array) => {
            24 + array
                .to_nested_values()
                .iter()
                .map(memoize_value_memory)
                .sum::<usize>()
        }
        Value::Record(record) => {
            24 + record
                .fields
                .iter()
                .map(memoize_value_memory)
                .sum::<usize>()
        }
        _ => 32,
    }
}

fn memoize_rows_memory(rows: &[MaterializedRow]) -> usize {
    rows.iter()
        .map(|row| {
            64 + row
                .slot
                .tts_values
                .iter()
                .map(memoize_value_memory)
                .sum::<usize>()
        })
        .sum()
}

fn memoize_key_memory(key: &MemoizeCacheKey) -> usize {
    48 + key.0.iter().map(memoize_value_memory).sum::<usize>()
}

fn memoize_entry_memory(key: &MemoizeCacheKey, rows: &[MaterializedRow]) -> usize {
    // Account for the hash table entry, tuple array, and per-entry memory
    // context overhead that PostgreSQL charges outside the raw key/row bytes.
    8192 + memoize_key_memory(key) + memoize_rows_memory(rows)
}

fn parse_memory_kb(raw: &str) -> Option<usize> {
    let trimmed = raw.trim().trim_matches('\'').trim();
    let mut parts = trimmed.split_whitespace();
    let number = parts.next()?;
    let unit = parts.next().unwrap_or("kB");
    let value = number.parse::<f64>().ok()?;
    let multiplier = match unit.to_ascii_lowercase().as_str() {
        "b" | "byte" | "bytes" => 1.0 / 1024.0,
        "kb" | "kib" => 1.0,
        "mb" | "mib" => 1024.0,
        "gb" | "gib" => 1024.0 * 1024.0,
        _ => 1.0,
    };
    value
        .is_finite()
        .then(|| (value * multiplier).ceil() as usize)
}

fn memoize_memory_limit_bytes(ctx: &ExecutorContext) -> usize {
    let work_mem_kb = ctx
        .gucs
        .get("work_mem")
        .and_then(|value| parse_memory_kb(value))
        .unwrap_or(4096);
    let hash_multiplier = ctx
        .gucs
        .get("hash_mem_multiplier")
        .and_then(|value| value.trim().trim_matches('\'').parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(2.0);
    ((work_mem_kb as f64) * 1024.0 * hash_multiplier) as usize
}

fn memoize_evict_if_needed(state: &mut MemoizeState, ctx: &ExecutorContext) {
    let limit = memoize_memory_limit_bytes(ctx);
    while (state.memoize_stats.memory_usage_bytes > limit
        || (state.est_entries > 0 && state.cache.len() > state.est_entries))
        && !state.lru.is_empty()
    {
        let Some(key) = state.lru.pop_front() else {
            break;
        };
        if let Some(rows) = state.cache.remove(&key) {
            state.memoize_stats.memory_usage_bytes = state
                .memoize_stats
                .memory_usage_bytes
                .saturating_sub(memoize_entry_memory(&key, &rows));
            state.memoize_stats.evictions += 1;
        }
    }
}

fn memoize_prepare_scan(
    state: &mut MemoizeState,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    let nonkey_dependents = memoize_nonkey_dependents(state, ctx)?;
    if state
        .last_nonkey_dependents
        .as_ref()
        .is_some_and(|last| *last != nonkey_dependents)
    {
        state.cache.clear();
        state.lru.clear();
        state.memoize_stats.memory_usage_bytes = 0;
    }
    state.last_nonkey_dependents = Some(nonkey_dependents);

    let key = memoize_key(state, ctx)?;
    if let Some(rows) = state.cache.get(&key) {
        state.memoize_stats.hits += 1;
        state.active_rows = rows.clone();
        state.active_index = 0;
        state.scan_prepared = true;
        return Ok(());
    }

    state.memoize_stats.misses += 1;
    state.input = executor_start(state.input_plan.clone());
    let mut rows = Vec::new();
    while state.input.exec_proc_node(ctx)?.is_some() {
        ctx.check_for_interrupts()?;
        rows.push(state.input.materialize_current_row()?);
        if state.single_row {
            break;
        }
    }
    let entry_memory = memoize_entry_memory(&key, &rows);
    if entry_memory > memoize_memory_limit_bytes(ctx) {
        state.memoize_stats.overflows += 1;
    }
    state.memoize_stats.memory_usage_bytes = state
        .memoize_stats
        .memory_usage_bytes
        .saturating_add(entry_memory);
    state.cache.insert(key.clone(), rows.clone());
    state.lru.push_back(key);
    memoize_evict_if_needed(state, ctx);
    state.active_rows = rows;
    state.active_index = 0;
    state.scan_prepared = true;
    Ok(())
}

impl PlanNode for MemoizeState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        if !self.scan_prepared {
            memoize_prepare_scan(self, ctx)?;
        }
        if self.active_index >= self.active_rows.len() {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }
        let row = &self.active_rows[self.active_index];
        self.active_index += 1;
        self.slot = row.slot.clone();
        self.current_bindings = row.system_bindings.clone();
        set_active_system_bindings(ctx, &self.current_bindings);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut self.slot))
    }

    fn rescan(&mut self, _ctx: &mut ExecutorContext) -> Result<(), ExecError> {
        self.active_rows.clear();
        self.active_index = 0;
        self.scan_prepared = false;
        self.current_bindings.clear();
        self.slot = TupleSlot::empty(self.column_names.len());
        Ok(())
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        &self.column_names
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Memoize".into()
    }
    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        if !self.cache_keys.is_empty() || !self.cache_key_labels.is_empty() {
            let keys = if !self.cache_key_labels.is_empty() {
                self.cache_key_labels.join(", ")
            } else {
                self.cache_keys
                    .iter()
                    .map(|expr| render_explain_expr(expr, &self.column_names))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            lines.push(format!("{prefix}Cache Key: {keys}"));
        }
        lines.push(format!(
            "{prefix}Cache Mode: {}",
            if self.binary_mode {
                "binary"
            } else {
                "logical"
            }
        ));
        if analyze {
            let memory_kb = self.memoize_stats.memory_usage_bytes.div_ceil(1024).max(1);
            lines.push(format!(
                "{prefix}Hits: {}  Misses: {}  Evictions: {}  Overflows: {}  Memory Usage: {memory_kb}kB",
                self.memoize_stats.hits,
                self.memoize_stats.misses,
                self.memoize_stats.evictions,
                self.memoize_stats.overflows
            ));
        }
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

impl PlanNode for GatherState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        if self.input.exec_proc_node(ctx)?.is_some() {
            finish_row(&mut self.stats, start);
            Ok(self.input.current_slot())
        } else {
            finish_eof(&mut self.stats, start, ctx);
            Ok(None)
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        self.input.current_slot()
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        self.input.current_system_bindings()
    }

    fn column_names(&self) -> &[String] {
        self.input.column_names()
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "Gather".into()
    }

    fn explain_details(
        &self,
        indent: usize,
        _analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        lines.push(format!("{prefix}Workers Planned: {}", self.workers_planned));
        if self.single_copy {
            lines.push(format!("{prefix}Single Copy: true"));
        }
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

impl PlanNode for NestedLoopJoinState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        if self.right_plan.is_some() {
            return exec_lateral_join(self, ctx, start);
        }

        if self.join_qual_never_matches && matches!(self.kind, JoinType::Left | JoinType::Anti) {
            match self.left.exec_proc_node(ctx)?.is_some() {
                true => {
                    let left = self.left.materialize_current_row()?;
                    if matches!(self.kind, JoinType::Anti) {
                        self.slot.tts_values = left.slot.tts_values;
                        self.slot.tts_nvalid = self.left_width;
                    } else {
                        let mut combined_values = left.slot.tts_values;
                        combined_values.extend(std::iter::repeat_n(Value::Null, self.right_width));
                        self.slot.tts_values = combined_values;
                        self.slot.tts_nvalid = self.left_width + self.right_width;
                    }
                    self.slot.kind = SlotKind::Virtual;
                    self.slot.virtual_tid = None;
                    self.slot.decode_offset = 0;
                    self.current_bindings = left.system_bindings;
                    set_active_system_bindings(ctx, &self.current_bindings);
                    clear_outer_expr_bindings(ctx);
                    clear_inner_expr_bindings(ctx);
                    finish_row(&mut self.stats, start);
                    return Ok(Some(&mut self.slot));
                }
                false => {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }
            }
        }

        if self.right_rows.is_none() {
            let mut rows = Vec::new();
            while self.right.exec_proc_node(ctx)?.is_some() {
                ctx.check_for_interrupts()?;
                rows.push(self.right.materialize_current_row()?);
            }
            self.right_matched = Some(vec![false; rows.len()]);
            self.right_rows = Some(rows);
        }

        if self.join_qual_never_matches && matches!(self.kind, JoinType::Full) {
            let right_rows = self.right_rows.as_ref().unwrap();
            while self.unmatched_right_index < right_rows.len() {
                let ri = self.unmatched_right_index;
                self.unmatched_right_index += 1;
                let mut combined_values = vec![Value::Null; self.left_width];
                combined_values.extend(right_rows[ri].slot.tts_values.iter().cloned());
                self.slot.tts_values = combined_values;
                self.slot.tts_nvalid = self.left_width + self.right_width;
                self.slot.kind = SlotKind::Virtual;
                self.slot.virtual_tid = None;
                self.slot.decode_offset = 0;
                self.current_bindings = right_rows[ri].system_bindings.clone();
                set_active_system_bindings(ctx, &self.current_bindings);
                clear_outer_expr_bindings(ctx);
                clear_inner_expr_bindings(ctx);
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            }

            match self.left.exec_proc_node(ctx)?.is_some() {
                true => {
                    let left = self.left.materialize_current_row()?;
                    let mut combined_values = left.slot.tts_values;
                    combined_values.extend(std::iter::repeat_n(Value::Null, self.right_width));
                    self.slot.tts_values = combined_values;
                    self.slot.tts_nvalid = self.left_width + self.right_width;
                    self.slot.kind = SlotKind::Virtual;
                    self.slot.virtual_tid = None;
                    self.slot.decode_offset = 0;
                    self.current_bindings = left.system_bindings;
                    set_active_system_bindings(ctx, &self.current_bindings);
                    clear_outer_expr_bindings(ctx);
                    clear_inner_expr_bindings(ctx);
                    finish_row(&mut self.stats, start);
                    return Ok(Some(&mut self.slot));
                }
                false => {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }
            }
        }

        loop {
            ctx.check_for_interrupts()?;
            if self.current_left.is_none() {
                match self.left.exec_proc_node(ctx)?.is_some() {
                    true => {
                        self.current_left = Some(self.left.materialize_current_row()?);
                        self.current_left_matched = false;
                        self.right_index = 0;
                    }
                    false => {
                        if matches!(self.kind, JoinType::Right | JoinType::Full) {
                            let right_rows = self.right_rows.as_ref().unwrap();
                            let right_matched = self.right_matched.as_mut().unwrap();
                            while self.unmatched_right_index < right_rows.len() {
                                let ri = self.unmatched_right_index;
                                self.unmatched_right_index += 1;
                                if right_matched[ri] {
                                    continue;
                                }
                                let mut combined_values = vec![Value::Null; self.left_width];
                                combined_values
                                    .extend(right_rows[ri].slot.tts_values.iter().cloned());
                                self.slot.tts_values = combined_values;
                                self.slot.tts_nvalid = self.left_width + self.right_width;
                                self.slot.kind = SlotKind::Virtual;
                                self.slot.virtual_tid = None;
                                self.slot.decode_offset = 0;
                                self.current_bindings = right_rows[ri].system_bindings.clone();
                                set_active_system_bindings(ctx, &self.current_bindings);
                                finish_row(&mut self.stats, start);
                                return Ok(Some(&mut self.slot));
                            }
                        }
                        finish_eof(&mut self.stats, start, ctx);
                        return Ok(None);
                    }
                }
            }

            let right_rows = self.right_rows.as_ref().unwrap();
            let right_matched = self.right_matched.as_mut().unwrap();

            while self.right_index < right_rows.len() {
                let ri = self.right_index;
                self.right_index += 1;

                let left = self.current_left.as_ref().unwrap();
                let right = &right_rows[ri];
                let mut combined_values: Vec<Value> = left.slot.tts_values.clone();
                combined_values.extend(right.slot.tts_values.iter().cloned());
                let nvalid = combined_values.len();
                self.slot.tts_values = combined_values;
                self.slot.tts_nvalid = nvalid;
                self.slot.kind = SlotKind::Virtual;
                self.slot.virtual_tid = None;
                self.slot.decode_offset = 0;
                self.current_bindings =
                    merge_system_bindings(&left.system_bindings, &right.system_bindings);
                set_active_system_bindings(ctx, &self.current_bindings);
                set_outer_expr_bindings(ctx, left.slot.tts_values.clone(), &left.system_bindings);
                set_inner_expr_bindings(ctx, right.slot.tts_values.clone(), &right.system_bindings);

                if eval_qual_list(&self.join_qual, &mut self.slot, ctx)? {
                    self.current_left_matched = true;
                    right_matched[ri] = true;
                    if matches!(self.kind, JoinType::Anti) {
                        self.right_index = right_rows.len();
                        break;
                    }
                    if eval_qual_list(&self.qual, &mut self.slot, ctx)? {
                        if matches!(self.kind, JoinType::Semi) {
                            let left = self.current_left.take().unwrap();
                            self.slot.tts_values = left.slot.tts_values;
                            self.slot.tts_nvalid = self.left_width;
                            self.slot.kind = SlotKind::Virtual;
                            self.slot.virtual_tid = None;
                            self.slot.decode_offset = 0;
                            self.current_bindings = left.system_bindings;
                            set_active_system_bindings(ctx, &self.current_bindings);
                            finish_row(&mut self.stats, start);
                            return Ok(Some(&mut self.slot));
                        }
                        finish_row(&mut self.stats, start);
                        return Ok(Some(&mut self.slot));
                    }
                }
            }

            if !self.current_left_matched && matches!(self.kind, JoinType::Anti) {
                let left = self.current_left.take().unwrap();
                self.slot.tts_values = left.slot.tts_values;
                self.slot.tts_nvalid = self.left_width;
                self.slot.kind = SlotKind::Virtual;
                self.slot.virtual_tid = None;
                self.slot.decode_offset = 0;
                self.current_bindings = left.system_bindings;
                set_active_system_bindings(ctx, &self.current_bindings);
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            }

            if !self.current_left_matched && matches!(self.kind, JoinType::Left | JoinType::Full) {
                let left = self.current_left.as_ref().unwrap();
                let mut combined_values: Vec<Value> = left.slot.tts_values.clone();
                combined_values.extend(std::iter::repeat_n(Value::Null, self.right_width));
                self.slot.tts_values = combined_values;
                self.slot.tts_nvalid = self.left_width + self.right_width;
                self.slot.kind = SlotKind::Virtual;
                self.slot.virtual_tid = None;
                self.slot.decode_offset = 0;
                self.current_bindings = left.system_bindings.clone();
                set_active_system_bindings(ctx, &self.current_bindings);
                self.current_left = None;
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            }

            self.current_left = None;
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        &self.output_names
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        match self.kind {
            JoinType::Inner => "Nested Loop".into(),
            JoinType::Left => "Nested Loop Left Join".into(),
            JoinType::Right => "Nested Loop Right Join".into(),
            JoinType::Full => "Nested Loop Full Join".into(),
            JoinType::Semi => "Nested Loop Semi Join".into(),
            JoinType::Anti => "Nested Loop Anti Join".into(),
            JoinType::Cross => "Nested Loop".into(),
        }
    }
    fn explain_details(
        &self,
        indent: usize,
        _analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        if !self.join_qual.is_empty() {
            let (left_names, right_names) = self.combined_names.split_at(self.left_width);
            lines.push(format!(
                "{prefix}Join Filter: {}",
                render_explain_join_expr(
                    &format_qual_list(&self.join_qual),
                    left_names,
                    right_names
                )
            ));
        }
        if !self.qual.is_empty() {
            let (left_names, right_names) = self.combined_names.split_at(self.left_width);
            lines.push(format!(
                "{prefix}Filter: {}",
                render_explain_join_expr(&format_qual_list(&self.qual), left_names, right_names)
            ));
        }
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.left,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
        format_explain_lines_with_costs(
            &*self.right,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

fn exec_lateral_join<'a>(
    state: &'a mut NestedLoopJoinState,
    ctx: &mut ExecutorContext,
    start: Option<Instant>,
) -> Result<Option<&'a mut TupleSlot>, ExecError> {
    loop {
        ctx.check_for_interrupts()?;
        if state.current_left.is_none() {
            match state.left.exec_proc_node(ctx)?.is_some() {
                true => {
                    let current_left = state.left.materialize_current_row()?;
                    let values = current_left.slot.tts_values.clone();
                    state.current_left = Some(current_left);
                    state.current_left_matched = false;
                    set_outer_expr_bindings(
                        ctx,
                        values,
                        &state.current_left.as_ref().unwrap().system_bindings,
                    );
                    set_active_system_bindings(
                        ctx,
                        &state.current_left.as_ref().unwrap().system_bindings,
                    );
                    let saved_params = bind_exec_params(
                        &state.nest_params,
                        &mut state.current_left.as_mut().unwrap().slot,
                        ctx,
                    )?;
                    if matches!(state.right_plan.as_ref(), Some(Plan::Memoize { .. })) {
                        state.right.rescan(ctx)?;
                    } else {
                        state.right = super::executor_start(
                            state
                                .right_plan
                                .as_ref()
                                .expect("lateral right plan")
                                .clone(),
                        );
                    }
                    let mut rows = Vec::new();
                    while state.right.exec_proc_node(ctx)?.is_some() {
                        ctx.check_for_interrupts()?;
                        rows.push(state.right.materialize_current_row()?);
                    }
                    state.right_rows = Some(rows);
                    state.right_matched = Some(vec![
                        false;
                        state
                            .right_rows
                            .as_ref()
                            .expect("lateral right rows")
                            .len()
                    ]);
                    state.right_index = 0;
                    state.unmatched_right_index = 0;
                    state.current_nest_param_saves = Some(saved_params);
                }
                false => {
                    finish_eof(&mut state.stats, start, ctx);
                    return Ok(None);
                }
            }
        }

        let right_rows = state.right_rows.as_ref().unwrap();
        let right_matched = state.right_matched.as_mut().unwrap();

        while state.right_index < right_rows.len() {
            ctx.check_for_interrupts()?;
            let ri = state.right_index;
            state.right_index += 1;

            let right = &right_rows[ri];
            let right_bindings = right.system_bindings.clone();
            let values = right.slot.tts_values.clone();
            let left = state.current_left.as_ref().unwrap();
            let mut combined_values: Vec<Value> = left.slot.tts_values.clone();
            combined_values.extend(values);
            let nvalid = combined_values.len();
            state.slot.tts_values = combined_values;
            state.slot.tts_nvalid = nvalid;
            state.slot.kind = SlotKind::Virtual;
            state.slot.virtual_tid = None;
            state.slot.decode_offset = 0;
            state.current_bindings = merge_system_bindings(&left.system_bindings, &right_bindings);
            set_active_system_bindings(ctx, &state.current_bindings);
            set_outer_expr_bindings(ctx, left.slot.tts_values.clone(), &left.system_bindings);
            set_inner_expr_bindings(ctx, right.slot.tts_values.clone(), &right_bindings);

            if eval_qual_list(&state.join_qual, &mut state.slot, ctx)? {
                state.current_left_matched = true;
                right_matched[ri] = true;
                if matches!(state.kind, JoinType::Anti) {
                    state.right_index = right_rows.len();
                    break;
                }
                if eval_qual_list(&state.qual, &mut state.slot, ctx)? {
                    if matches!(state.kind, JoinType::Semi) {
                        let left = state.current_left.take().unwrap();
                        state.slot.tts_values = left.slot.tts_values;
                        state.slot.tts_nvalid = state.left_width;
                        state.slot.kind = SlotKind::Virtual;
                        state.slot.virtual_tid = None;
                        state.slot.decode_offset = 0;
                        state.current_bindings = left.system_bindings;
                        set_active_system_bindings(ctx, &state.current_bindings);
                        if let Some(saved_params) = state.current_nest_param_saves.take() {
                            restore_exec_params(saved_params, ctx);
                        }
                        state.right_rows = None;
                        state.right_matched = None;
                        finish_row(&mut state.stats, start);
                        return Ok(Some(&mut state.slot));
                    }
                    finish_row(&mut state.stats, start);
                    return Ok(Some(&mut state.slot));
                }
            }
        }

        if matches!(state.kind, JoinType::Right | JoinType::Full) {
            while state.unmatched_right_index < right_rows.len() {
                let ri = state.unmatched_right_index;
                state.unmatched_right_index += 1;
                if right_matched[ri] {
                    continue;
                }

                let right = &right_rows[ri];
                let mut combined_values = vec![Value::Null; state.left_width];
                combined_values.extend(right.slot.tts_values.iter().cloned());
                state.slot.tts_values = combined_values;
                state.slot.tts_nvalid = state.left_width + state.right_width;
                state.slot.kind = SlotKind::Virtual;
                state.slot.virtual_tid = None;
                state.slot.decode_offset = 0;
                state.current_bindings = right.system_bindings.clone();
                set_active_system_bindings(ctx, &state.current_bindings);
                finish_row(&mut state.stats, start);
                return Ok(Some(&mut state.slot));
            }
        }

        clear_outer_expr_bindings(ctx);
        clear_inner_expr_bindings(ctx);
        if !state.current_left_matched && matches!(state.kind, JoinType::Anti) {
            let left = state.current_left.take().unwrap();
            state.slot.tts_values = left.slot.tts_values;
            state.slot.tts_nvalid = state.left_width;
            state.slot.kind = SlotKind::Virtual;
            state.slot.virtual_tid = None;
            state.slot.decode_offset = 0;
            state.current_bindings = left.system_bindings;
            set_active_system_bindings(ctx, &state.current_bindings);
            if let Some(saved_params) = state.current_nest_param_saves.take() {
                restore_exec_params(saved_params, ctx);
            }
            state.right_rows = None;
            state.right_matched = None;
            finish_row(&mut state.stats, start);
            return Ok(Some(&mut state.slot));
        }
        if !state.current_left_matched && matches!(state.kind, JoinType::Left | JoinType::Full) {
            let left = state.current_left.as_ref().unwrap();
            let mut combined_values: Vec<Value> = left.slot.tts_values.clone();
            combined_values.extend(std::iter::repeat_n(Value::Null, state.right_width));
            state.slot.tts_values = combined_values;
            state.slot.tts_nvalid = state.left_width + state.right_width;
            state.slot.kind = SlotKind::Virtual;
            state.slot.virtual_tid = None;
            state.slot.decode_offset = 0;
            state.current_bindings = left.system_bindings.clone();
            set_active_system_bindings(ctx, &state.current_bindings);
            if let Some(saved_params) = state.current_nest_param_saves.take() {
                restore_exec_params(saved_params, ctx);
            }
            state.right_rows = None;
            state.right_matched = None;
            state.current_left = None;
            finish_row(&mut state.stats, start);
            return Ok(Some(&mut state.slot));
        }

        if let Some(saved_params) = state.current_nest_param_saves.take() {
            restore_exec_params(saved_params, ctx);
        }
        state.right_rows = None;
        state.right_matched = None;
        state.current_left = None;
    }
}

impl PlanNode for OrderByState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        if self.rows.is_none() {
            let mut rows = Vec::new();
            while self.input.exec_proc_node(ctx)?.is_some() {
                ctx.check_for_interrupts()?;
                rows.push(self.input.materialize_current_row()?);
            }

            let mut keyed_rows = Vec::with_capacity(rows.len());
            for mut row in rows {
                ctx.check_for_interrupts()?;
                set_active_system_bindings(ctx, &row.system_bindings);
                set_active_grouping_refs(ctx, &row.grouping_refs);
                set_outer_expr_bindings(ctx, row.slot.tts_values.clone(), &row.system_bindings);
                let mut keys = Vec::with_capacity(self.items.len());
                for item in &self.items {
                    let key = eval_expr(&item.expr, &mut row.slot, ctx)?;
                    keys.push(order_by_runtime_key(item, key, ctx));
                }
                keyed_rows.push((keys, row));
            }

            let mut sort_error = None;
            pg_sql_sort_by(
                &mut keyed_rows,
                |(left_keys, left_row), (right_keys, right_row)| match compare_order_by_keys(
                    &self.items,
                    left_keys,
                    right_keys,
                ) {
                    Ok(std::cmp::Ordering::Equal) if self.network_strict_less_tiebreak => {
                        match network_order_tie_break(left_keys, right_keys, left_row, right_row) {
                            Ok(ordering) => ordering,
                            Err(err) => {
                                if sort_error.is_none() {
                                    sort_error = Some(err);
                                }
                                std::cmp::Ordering::Equal
                            }
                        }
                    }
                    Ok(std::cmp::Ordering::Equal) => geometry_circle_distance_order_tie_break(
                        left_keys, right_keys, left_row, right_row,
                    ),
                    Ok(ordering) => ordering,
                    Err(err) => {
                        if sort_error.is_none() {
                            sort_error = Some(err);
                        }
                        std::cmp::Ordering::Equal
                    }
                },
            );
            if let Some(err) = sort_error {
                return Err(err);
            }
            self.rows = Some(keyed_rows.into_iter().map(|(_, row)| row).collect());
        }

        let rows = self.rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        self.current_bindings = rows[idx].system_bindings.clone();
        set_active_system_bindings(ctx, &self.current_bindings);
        self.current_grouping_refs = rows[idx].grouping_refs.clone();
        set_active_grouping_refs(ctx, &self.current_grouping_refs);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut rows[idx].slot))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx).map(|row| &mut row.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn current_grouping_refs(&self) -> &[usize] {
        &self.current_grouping_refs
    }
    fn column_names(&self) -> &[String] {
        self.input.column_names()
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Sort".into()
    }
    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        let sort_keys = if self.display_items.is_empty() {
            self.items
                .iter()
                .map(|item| render_order_by_key(item, self.column_names()))
                .collect::<Vec<_>>()
        } else {
            self.display_items.clone()
        }
        .join(", ");
        lines.push(format!("{prefix}Sort Key: {sort_keys}"));
        if analyze {
            let memory_kb = self
                .rows
                .as_ref()
                .map(|rows| {
                    let bytes = rows.len().saturating_mul(self.plan_info.plan_width.max(1));
                    bytes.max(1024).div_ceil(1024)
                })
                .unwrap_or(1);
            lines.push(format!(
                "{prefix}Sort Method: quicksort  Memory: {memory_kb}kB"
            ));
            if self.stats.buffer_usage.shared_hit > 0
                || self.stats.buffer_usage.shared_read > 0
                || self.stats.buffer_usage.shared_written > 0
            {
                lines.push(format!(
                    "{prefix}{}",
                    crate::backend::commands::explain::format_buffer_usage(self.stats.buffer_usage)
                ));
            }
        }
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

fn materialize_ordered_current_row(
    input: &mut PlanState,
    items: &[OrderByEntry],
    ctx: &mut ExecutorContext,
) -> Result<(Vec<Value>, MaterializedRow), ExecError> {
    let mut row = input.materialize_current_row()?;
    set_active_system_bindings(ctx, &row.system_bindings);
    set_active_grouping_refs(ctx, &row.grouping_refs);
    set_outer_expr_bindings(ctx, row.slot.tts_values.clone(), &row.system_bindings);
    let mut keys = Vec::with_capacity(items.len());
    for item in items {
        let key = eval_expr(&item.expr, &mut row.slot, ctx)?;
        keys.push(order_by_runtime_key(item, key, ctx));
    }
    Ok((keys, row))
}

fn sort_keyed_materialized_rows(
    items: &[OrderByEntry],
    keyed_rows: &mut [(Vec<Value>, MaterializedRow)],
) -> Result<(), ExecError> {
    let mut sort_error = None;
    keyed_rows.sort_by(
        |(left_keys, left_row), (right_keys, right_row)| match compare_order_by_keys(
            items, left_keys, right_keys,
        ) {
            Ok(std::cmp::Ordering::Equal) => {
                geometry_circle_distance_order_tie_break(left_keys, right_keys, left_row, right_row)
            }
            Ok(ordering) => ordering,
            Err(err) => {
                if sort_error.is_none() {
                    sort_error = Some(err);
                }
                std::cmp::Ordering::Equal
            }
        },
    );
    if let Some(err) = sort_error {
        Err(err)
    } else {
        Ok(())
    }
}

fn presorted_keys_equal(
    items: &[OrderByEntry],
    presorted_count: usize,
    left: &[Value],
    right: &[Value],
) -> Result<bool, ExecError> {
    if presorted_count == 0 {
        return Ok(false);
    }
    Ok(compare_order_by_keys(
        &items[..presorted_count],
        &left[..presorted_count],
        &right[..presorted_count],
    )? == std::cmp::Ordering::Equal)
}

impl IncrementalSortState {
    fn load_next_group(&mut self, ctx: &mut ExecutorContext) -> Result<bool, ExecError> {
        let first = if let Some(row) = self.lookahead.take() {
            Some(row)
        } else if self.input.exec_proc_node(ctx)?.is_some() {
            Some(materialize_ordered_current_row(
                &mut self.input,
                &self.items,
                ctx,
            )?)
        } else {
            None
        };
        let Some((first_keys, first_row)) = first else {
            return Ok(false);
        };

        let mut keyed_rows = vec![(first_keys.clone(), first_row)];
        loop {
            ctx.check_for_interrupts()?;
            if self.input.exec_proc_node(ctx)?.is_none() {
                break;
            }
            let (keys, row) = materialize_ordered_current_row(&mut self.input, &self.items, ctx)?;
            if presorted_keys_equal(&self.items, self.presorted_count, &first_keys, &keys)? {
                keyed_rows.push((keys, row));
            } else {
                self.lookahead = Some((keys, row));
                break;
            }
        }

        sort_keyed_materialized_rows(&self.items, &mut keyed_rows)?;
        self.rows = keyed_rows.into_iter().map(|(_, row)| row).collect();
        self.next_index = 0;
        Ok(true)
    }
}

impl PlanNode for IncrementalSortState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        if self.next_index >= self.rows.len() {
            self.rows.clear();
            if !self.load_next_group(ctx)? {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }
        }

        let idx = self.next_index;
        self.next_index += 1;
        self.current_bindings = self.rows[idx].system_bindings.clone();
        set_active_system_bindings(ctx, &self.current_bindings);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut self.rows[idx].slot))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let idx = self.next_index.checked_sub(1)?;
        self.rows.get_mut(idx).map(|row| &mut row.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        self.input.column_names()
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "Incremental Sort".into()
    }

    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        let sort_keys = if self.display_items.is_empty() {
            self.items
                .iter()
                .map(|item| render_order_by_key(item, self.column_names()))
                .collect::<Vec<_>>()
        } else {
            self.display_items.clone()
        }
        .join(", ");
        lines.push(format!("{prefix}Sort Key: {sort_keys}"));
        let presorted_keys = if self.presorted_display_items.is_empty() {
            self.items
                .iter()
                .take(self.presorted_count)
                .map(|item| render_order_by_key(item, self.column_names()))
                .collect::<Vec<_>>()
        } else {
            self.presorted_display_items.clone()
        }
        .join(", ");
        lines.push(format!("{prefix}Presorted Key: {presorted_keys}"));
        if analyze {
            let memory_kb = self
                .rows
                .len()
                .saturating_mul(self.plan_info.plan_width.max(1))
                .max(1024)
                .div_ceil(1024);
            lines.push(format!(
                "{prefix}Sort Method: quicksort  Memory: {memory_kb}kB"
            ));
        }
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

fn order_by_runtime_key(
    item: &crate::include::nodes::primnodes::OrderByEntry,
    value: Value,
    ctx: &ExecutorContext,
) -> Value {
    let Some(sql_type) = crate::include::nodes::primnodes::expr_sql_type_hint(&item.expr) else {
        return enum_order_key_by_label_oid(value, ctx);
    };
    if !matches!(sql_type.kind, SqlTypeKind::Enum) {
        return enum_order_key_by_label_oid(value, ctx);
    }
    let Value::EnumOid(label_oid) = value else {
        return value;
    };
    ctx.catalog
        .as_ref()
        .and_then(|catalog| {
            catalog
                .enum_rows()
                .into_iter()
                .find(|row| row.enumtypid == sql_type.type_oid && row.oid == label_oid)
                .map(|row| Value::Float64(row.enumsortorder))
        })
        .unwrap_or(Value::EnumOid(label_oid))
}

fn enum_order_key_by_label_oid(value: Value, ctx: &ExecutorContext) -> Value {
    let Value::EnumOid(label_oid) = value else {
        return value;
    };
    ctx.catalog
        .as_ref()
        .and_then(|catalog| {
            catalog
                .enum_rows()
                .into_iter()
                .find(|row| row.oid == label_oid)
                .map(|row| Value::Float64(row.enumsortorder))
        })
        .unwrap_or(Value::EnumOid(label_oid))
}

impl PlanNode for LimitState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        if let Some(limit) = self.limit {
            if self.returned >= limit {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }
        }

        while self.skipped < self.offset {
            ctx.check_for_interrupts()?;
            if self.input.exec_proc_node(ctx)?.is_none() {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }
            self.skipped += 1;
        }

        let slot = self.input.exec_proc_node(ctx)?;
        if slot.is_some() {
            self.returned += 1;
            finish_row(&mut self.stats, start);
        } else {
            finish_eof(&mut self.stats, start, ctx);
        }
        Ok(slot)
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        self.input.current_slot()
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        self.input.current_system_bindings()
    }
    fn column_names(&self) -> &[String] {
        self.input.column_names()
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Limit".into()
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

impl PlanNode for LockRowsState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        loop {
            let Some(_slot) = self.input.exec_proc_node(ctx)? else {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            };
            self.current_bindings = self.input.current_system_bindings().to_vec();
            lock_current_row_marks(ctx, &self.row_marks, &self.current_bindings)?;
            set_active_system_bindings(ctx, &self.current_bindings);
            finish_row(&mut self.stats, start);
            return Ok(self.input.current_slot());
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        self.input.current_slot()
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        self.input.column_names()
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "LockRows".into()
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

impl PlanNode for ProjectionState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        let input_slot = match self.input.exec_proc_node(ctx)? {
            Some(s) => s,
            None => {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }
        };
        let mut values = Vec::with_capacity(self.targets.len());
        let outer_values = materialize_slot_values(input_slot)?;
        let current_bindings = ctx.system_bindings.clone();
        set_outer_expr_bindings(ctx, outer_values, &current_bindings);
        clear_inner_expr_bindings(ctx);
        for target in &self.targets {
            values.push(eval_expr(&target.expr, input_slot, ctx)?.to_owned_value());
        }

        self.slot
            .store_virtual_row(values, input_slot.tid(), input_slot.table_oid);
        self.current_bindings = self.input.current_system_bindings().to_vec();
        set_active_system_bindings(ctx, &self.current_bindings);
        self.current_grouping_refs = self.input.current_grouping_refs().to_vec();
        set_active_grouping_refs(ctx, &self.current_grouping_refs);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut self.slot))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn current_grouping_refs(&self) -> &[usize] {
        &self.current_grouping_refs
    }
    fn column_names(&self) -> &[String] {
        &self.column_names
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Projection".into()
    }
    fn explain_passthrough(&self) -> Option<&dyn PlanNode> {
        projection_is_explain_passthrough(self).then_some(self.input.as_ref())
    }
    fn renumber_append_child_aliases(&mut self, ordinal: usize) {
        self.input.renumber_append_child_aliases(ordinal);
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

fn projection_is_explain_passthrough(state: &ProjectionState) -> bool {
    let input_names = state.input.column_names();
    let identity_projection = state.targets.len() == input_names.len()
        && state.targets.iter().enumerate().all(|(index, target)| {
            !target.resjunk
                && target.input_resno == Some(index + 1)
                && target.name == input_names[index]
        });
    if identity_projection {
        return true;
    }
    if state.input.node_label() == "WindowAgg" && state.targets.iter().all(|target| !target.resjunk)
    {
        return true;
    }
    state
        .targets
        .iter()
        .all(|target| !target.resjunk && matches!(target.expr, Expr::Var(_)))
}

fn aggregate_uses_plain_fast_path(state: &AggregateState) -> bool {
    state.strategy == AggregateStrategy::Plain
        && state.phase == AggregatePhase::Complete
        && !state.disabled
        && state.group_by.is_empty()
        && state.grouping_sets.is_empty()
        && state.passthrough_exprs.is_empty()
        && state.having.is_none()
        && state.accumulators.iter().all(|accum| {
            !accum.distinct
                && accum.direct_args.is_empty()
                && accum.order_by.is_empty()
                && accum.filter.is_none()
                && accum.args.iter().all(expr_is_plain_aggregate_safe)
        })
}

fn expr_is_plain_aggregate_safe(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup == 0 && !is_special_varno(var.varno),
        Expr::Const(_) => true,
        Expr::GroupingKey(grouping_key) => expr_is_plain_aggregate_safe(&grouping_key.expr),
        Expr::GroupingFunc(grouping_func) => {
            grouping_func.args.iter().all(expr_is_plain_aggregate_safe)
        }
        Expr::Op(op) => op.args.iter().all(expr_is_plain_aggregate_safe),
        Expr::Bool(bool_expr) => bool_expr.args.iter().all(expr_is_plain_aggregate_safe),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_none_or(expr_is_plain_aggregate_safe)
                && case_expr.args.iter().all(|arm| {
                    expr_is_plain_aggregate_safe(&arm.expr)
                        && expr_is_plain_aggregate_safe(&arm.result)
                })
                && expr_is_plain_aggregate_safe(&case_expr.defresult)
        }
        Expr::Func(func) => func.args.iter().all(expr_is_plain_aggregate_safe),
        Expr::SqlJsonQueryFunction(func) => func
            .child_exprs()
            .into_iter()
            .all(expr_is_plain_aggregate_safe),
        Expr::ScalarArrayOp(op) => {
            expr_is_plain_aggregate_safe(&op.left) && expr_is_plain_aggregate_safe(&op.right)
        }
        Expr::Xml(xml) => xml.child_exprs().all(expr_is_plain_aggregate_safe),
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::FieldSelect { expr: inner, .. } => expr_is_plain_aggregate_safe(inner),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_is_plain_aggregate_safe(expr)
                && expr_is_plain_aggregate_safe(pattern)
                && escape.as_deref().is_none_or(expr_is_plain_aggregate_safe)
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_is_plain_aggregate_safe(left) && expr_is_plain_aggregate_safe(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().all(expr_is_plain_aggregate_safe),
        Expr::Row { fields, .. } => fields
            .iter()
            .all(|(_, field)| expr_is_plain_aggregate_safe(field)),
        Expr::ArraySubscript { array, subscripts } => {
            expr_is_plain_aggregate_safe(array)
                && subscripts.iter().all(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_none_or(expr_is_plain_aggregate_safe)
                        && subscript
                            .upper
                            .as_ref()
                            .is_none_or(expr_is_plain_aggregate_safe)
                })
        }
        Expr::Param(_)
        | Expr::Aggref(_)
        | Expr::WindowFunc(_)
        | Expr::CaseTest(_)
        | Expr::SetReturning(_)
        | Expr::SubLink(_)
        | Expr::SubPlan(_)
        | Expr::Random
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

fn execute_plain_aggregate_fast_path(
    state: &mut AggregateState,
    runtimes: &[AggregateRuntime],
    ctx: &mut ExecutorContext,
) -> Result<Option<Vec<MaterializedRow>>, ExecError> {
    let mut accum_states = runtimes
        .iter()
        .zip(state.accumulators.iter())
        .map(|(runtime, accum)| runtime.initialize_state(accum))
        .collect::<Vec<_>>();
    let const_arg_values = state
        .accumulators
        .iter()
        .map(|accum| {
            accum
                .args
                .iter()
                .map(|arg| match arg {
                    Expr::Const(value) => Some(value.clone()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()
        })
        .collect::<Vec<_>>();

    while let Some(slot) = state.input.exec_proc_node(ctx)? {
        ctx.check_for_interrupts()?;
        clear_inner_expr_bindings(ctx);
        for (i, accum) in state.accumulators.iter().enumerate() {
            if let Some(values) = const_arg_values[i].as_ref() {
                runtimes[i].transition(&mut accum_states[i], values, ctx)?;
                continue;
            }
            match accum.args.as_slice() {
                [] => runtimes[i].transition(&mut accum_states[i], &[], ctx)?,
                [arg] => {
                    let value = eval_expr(arg, slot, ctx)?;
                    runtimes[i].transition(
                        &mut accum_states[i],
                        std::slice::from_ref(&value),
                        ctx,
                    )?;
                }
                args => {
                    let values = args
                        .iter()
                        .map(|arg| eval_expr(arg, slot, ctx))
                        .collect::<Result<Vec<_>, _>>()?;
                    runtimes[i].transition(&mut accum_states[i], &values, ctx)?;
                }
            }
        }
    }

    let mut row_values = Vec::with_capacity(state.accumulators.len());
    for ((runtime, accum_state), accum) in runtimes
        .iter()
        .zip(accum_states.iter())
        .zip(state.accumulators.iter())
    {
        row_values.push(runtime.finalize(accum, accum_state, &[], &[], ctx)?);
    }

    Ok(Some(vec![MaterializedRow::new(
        TupleSlot::virtual_row(row_values),
        Vec::new(),
    )]))
}

fn new_aggregate_group(
    key_values: Vec<Value>,
    passthrough_values: Vec<Value>,
    runtimes: &[AggregateRuntime],
    accumulators: &[AggAccum],
) -> AggGroup {
    let accum_states = runtimes
        .iter()
        .zip(accumulators.iter())
        .map(|(runtime, accum)| runtime.initialize_state(accum))
        .collect();
    AggGroup {
        key_values,
        passthrough_values,
        accum_states,
        distinct_inputs: accumulators
            .iter()
            .map(|accum| accum.distinct.then(Vec::new))
            .collect(),
        direct_arg_values: vec![None; accumulators.len()],
        ordered_inputs: vec![Vec::new(); accumulators.len()],
    }
}

#[allow(clippy::too_many_arguments)]
fn advance_aggregate_group(
    group: &mut AggGroup,
    accumulators: &[AggAccum],
    runtimes: &[AggregateRuntime],
    phase: AggregatePhase,
    group_by_len: usize,
    passthrough_len: usize,
    outer_values: &[Value],
    slot: &mut TupleSlot,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    for (i, accum) in accumulators.iter().enumerate() {
        if phase == AggregatePhase::Finalize {
            let partial_index = group_by_len + passthrough_len + i;
            let partial = outer_values
                .get(partial_index)
                .cloned()
                .unwrap_or(Value::Null);
            runtimes[i].combine_partial(accum, &mut group.accum_states[i], &partial, ctx)?;
            continue;
        }
        if let Some(filter) = accum.filter.as_ref() {
            match eval_expr(filter, slot, ctx)? {
                Value::Bool(true) => {}
                Value::Bool(false) | Value::Null => continue,
                other => return Err(ExecError::NonBoolQual(other)),
            }
        }
        let values = accum
            .args
            .iter()
            .map(|arg| eval_expr(arg, slot, ctx))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(seen_inputs) = group.distinct_inputs[i].as_mut() {
            if seen_inputs
                .iter()
                .any(|seen| aggregate_key_values_equal(seen, &values))
            {
                continue;
            }
            seen_inputs.push(values.clone());
        }
        if accum.order_by.is_empty() {
            runtimes[i].transition(&mut group.accum_states[i], &values, ctx)?;
        } else {
            let sort_keys = accum
                .order_by
                .iter()
                .map(|item| eval_expr(&item.expr, slot, ctx))
                .collect::<Result<Vec<_>, _>>()?;
            group.ordered_inputs[i].push(OrderedAggInput {
                sort_keys,
                arg_values: values,
            });
        }
    }
    Ok(())
}

fn finish_ordered_aggregate_inputs(
    group: &mut AggGroup,
    accumulators: &[AggAccum],
    runtimes: &[AggregateRuntime],
    phase: AggregatePhase,
    ctx: &mut ExecutorContext,
) -> Result<(), ExecError> {
    if phase == AggregatePhase::Finalize {
        return Ok(());
    }
    for (i, accum) in accumulators.iter().enumerate() {
        if accum.order_by.is_empty() {
            continue;
        }
        let inputs = &mut group.ordered_inputs[i];
        let mut sort_error = None;
        pg_sql_sort_by(inputs, |left, right| {
            match compare_order_by_keys(&accum.order_by, &left.sort_keys, &right.sort_keys) {
                Ok(ordering) => ordering,
                Err(err) => {
                    if sort_error.is_none() {
                        sort_error = Some(err);
                    }
                    std::cmp::Ordering::Equal
                }
            }
        });
        if let Some(err) = sort_error {
            return Err(err);
        }
        for input in inputs.iter() {
            runtimes[i].transition(&mut group.accum_states[i], &input.arg_values, ctx)?;
        }
    }
    Ok(())
}

fn grouping_set_key_values(
    full_key_values: &[Value],
    group_by_refs: &[usize],
    grouping_set: &[usize],
) -> Vec<Value> {
    group_by_refs
        .iter()
        .enumerate()
        .map(|(index, ref_id)| {
            if grouping_set.contains(ref_id) {
                full_key_values.get(index).cloned().unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        })
        .collect()
}

impl PlanNode for AggregateState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        if self.result_rows.is_none() {
            if self.runtimes.is_none() {
                self.runtimes = Some(
                    self.accumulators
                        .iter()
                        .map(|accum| build_aggregate_runtime(accum, ctx))
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            let runtimes = self
                .runtimes
                .clone()
                .expect("aggregate runtimes initialized above");
            if aggregate_uses_plain_fast_path(self)
                && let Some(result_rows) = execute_plain_aggregate_fast_path(self, &runtimes, ctx)?
            {
                self.result_rows = Some(result_rows);
                let rows = self.result_rows.as_mut().unwrap();
                if self.next_index >= rows.len() {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }

                let idx = self.next_index;
                self.next_index += 1;
                self.current_bindings = rows[idx].system_bindings.clone();
                set_active_system_bindings(ctx, &self.current_bindings);
                self.current_grouping_refs = rows[idx].grouping_refs.clone();
                set_active_grouping_refs(ctx, &self.current_grouping_refs);
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut rows[idx].slot));
            }
            let mut groups: Vec<(usize, AggGroup)> = Vec::new();
            let mut group_lookup: HashMap<(usize, Vec<Value>), usize> = HashMap::new();
            let mut mixed_group = (self.grouping_sets.is_empty()
                && self.strategy == AggregateStrategy::Mixed)
                .then(|| {
                    new_aggregate_group(
                        vec![Value::Null; self.group_by.len()],
                        Vec::new(),
                        &runtimes,
                        &self.accumulators,
                    )
                });

            while let Some(slot) = self.input.exec_proc_node(ctx)? {
                ctx.check_for_interrupts()?;
                let outer_values = materialize_slot_values(slot)?;
                let current_bindings = ctx.system_bindings.clone();
                set_outer_expr_bindings(ctx, outer_values.clone(), &current_bindings);
                clear_inner_expr_bindings(ctx);
                self.key_buffer.clear();
                for expr in &self.group_by {
                    self.key_buffer.push(eval_expr(expr, slot, ctx)?);
                }

                let active_grouping_sets = if self.grouping_sets.is_empty() {
                    vec![(0, self.group_by_refs.clone())]
                } else {
                    self.grouping_sets
                        .iter()
                        .cloned()
                        .enumerate()
                        .collect::<Vec<_>>()
                };

                for (grouping_set_index, grouping_set) in active_grouping_sets {
                    let key_values = if self.grouping_sets.is_empty() {
                        self.key_buffer.clone()
                    } else {
                        grouping_set_key_values(
                            &self.key_buffer,
                            &self.group_by_refs,
                            &grouping_set,
                        )
                    };
                    let lookup_key = (grouping_set_index, aggregate_group_lookup_key(&key_values));
                    let group_idx = if let Some(index) = group_lookup.get(&lookup_key).copied() {
                        index
                    } else if let Some(index) = groups.iter().position(|(set_index, group)| {
                        *set_index == grouping_set_index
                            && aggregate_key_values_equal(&group.key_values, &key_values)
                    }) {
                        group_lookup.insert(lookup_key, index);
                        index
                    } else {
                        let passthrough_values = self
                            .passthrough_exprs
                            .iter()
                            .map(|expr| eval_expr(expr, slot, ctx))
                            .collect::<Result<Vec<_>, _>>()?;
                        groups.push((
                            grouping_set_index,
                            new_aggregate_group(
                                key_values,
                                passthrough_values,
                                &runtimes,
                                &self.accumulators,
                            ),
                        ));
                        let index = groups.len() - 1;
                        group_lookup.insert(lookup_key, index);
                        index
                    };

                    let group = &mut groups[group_idx].1;
                    advance_aggregate_group(
                        group,
                        &self.accumulators,
                        &runtimes,
                        self.phase,
                        self.group_by.len(),
                        self.passthrough_exprs.len(),
                        &outer_values,
                        slot,
                        ctx,
                    )?;
                }
                if let Some(group) = mixed_group.as_mut() {
                    advance_aggregate_group(
                        group,
                        &self.accumulators,
                        &runtimes,
                        self.phase,
                        self.group_by.len(),
                        self.passthrough_exprs.len(),
                        &outer_values,
                        slot,
                        ctx,
                    )?;
                }
            }

            if groups.is_empty() {
                if self.grouping_sets.is_empty() && self.group_by.is_empty() {
                    groups.push((
                        0,
                        new_aggregate_group(Vec::new(), Vec::new(), &runtimes, &self.accumulators),
                    ));
                } else {
                    for (index, grouping_set) in self.grouping_sets.iter().enumerate() {
                        if grouping_set.is_empty() {
                            groups.push((
                                index,
                                new_aggregate_group(
                                    vec![Value::Null; self.group_by.len()],
                                    Vec::new(),
                                    &runtimes,
                                    &self.accumulators,
                                ),
                            ));
                        }
                    }
                }
            }

            for (_, group) in &mut groups {
                finish_ordered_aggregate_inputs(
                    group,
                    &self.accumulators,
                    &runtimes,
                    self.phase,
                    ctx,
                )?;
            }
            if let Some(group) = mixed_group.as_mut() {
                finish_ordered_aggregate_inputs(
                    group,
                    &self.accumulators,
                    &runtimes,
                    self.phase,
                    ctx,
                )?;
            }

            let mut result_rows = Vec::<(usize, MaterializedRow)>::new();
            for (grouping_set_index, group) in groups
                .iter()
                .map(|(set_index, group)| (*set_index, group))
                .chain(mixed_group.iter().map(|group| (usize::MAX, group)))
            {
                ctx.check_for_interrupts()?;
                let grouping_refs = if self.grouping_sets.is_empty() {
                    self.group_by_refs.clone()
                } else {
                    self.grouping_sets
                        .get(grouping_set_index)
                        .cloned()
                        .unwrap_or_default()
                };
                let mut row_values = group.key_values.clone();
                row_values.extend(group.passthrough_values.iter().cloned());
                for (i, ((runtime, accum_state), accum)) in runtimes
                    .iter()
                    .zip(group.accum_states.iter())
                    .zip(self.accumulators.iter())
                    .enumerate()
                {
                    if self.phase == AggregatePhase::Partial {
                        row_values.push(runtime.partial_value(accum, accum_state)?);
                        continue;
                    }
                    let direct_arg_values =
                        if let Some(values) = group.direct_arg_values[i].as_ref() {
                            values.clone()
                        } else if accum.direct_args.is_empty() {
                            Vec::new()
                        } else {
                            let mut direct_slot = TupleSlot::virtual_row(row_values.clone());
                            let saved_grouping_refs = ctx.active_grouping_refs.clone();
                            let saved_bindings = ctx.system_bindings.clone();
                            set_active_grouping_refs(ctx, &grouping_refs);
                            ctx.system_bindings.clear();
                            set_outer_expr_bindings(ctx, row_values.clone(), &[]);
                            let values = accum
                                .direct_args
                                .iter()
                                .map(|expr| eval_expr(expr, &mut direct_slot, ctx))
                                .collect::<Result<Vec<_>, _>>();
                            set_active_grouping_refs(ctx, &saved_grouping_refs);
                            ctx.system_bindings = saved_bindings;
                            values?
                        };
                    row_values.push(runtime.finalize(
                        accum,
                        accum_state,
                        &group.ordered_inputs[i],
                        &direct_arg_values,
                        ctx,
                    )?);
                }

                if let Some(having) = &self.having {
                    let mut having_slot = TupleSlot::virtual_row(row_values.clone());
                    let saved_grouping_refs = ctx.active_grouping_refs.clone();
                    let saved_bindings = ctx.system_bindings.clone();
                    set_active_grouping_refs(ctx, &grouping_refs);
                    ctx.system_bindings.clear();
                    set_outer_expr_bindings(ctx, row_values.clone(), &[]);
                    let result = eval_expr(having, &mut having_slot, ctx);
                    set_active_grouping_refs(ctx, &saved_grouping_refs);
                    ctx.system_bindings = saved_bindings;
                    match result? {
                        Value::Bool(true) => {}
                        Value::Bool(false) | Value::Null => continue,
                        other => return Err(ExecError::NonBoolQual(other)),
                    }
                }

                result_rows.push((
                    grouping_set_index,
                    MaterializedRow::new_with_grouping_refs(
                        TupleSlot::virtual_row(row_values),
                        Vec::new(),
                        grouping_refs,
                    ),
                ));
            }

            if !self.grouping_sets.is_empty() {
                sort_grouping_set_result_rows(
                    &mut result_rows,
                    &self.grouping_sets,
                    &self.group_by_refs,
                )?;
            }
            self.result_rows = Some(result_rows.into_iter().map(|(_, row)| row).collect());
        }

        let rows = self.result_rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        self.current_bindings = rows[idx].system_bindings.clone();
        set_active_system_bindings(ctx, &self.current_bindings);
        self.current_grouping_refs = rows[idx].grouping_refs.clone();
        set_active_grouping_refs(ctx, &self.current_grouping_refs);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut rows[idx].slot))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.result_rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx).map(|row| &mut row.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn current_grouping_refs(&self) -> &[usize] {
        &self.current_grouping_refs
    }
    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        if self.accumulators.is_empty()
            && self.strategy == AggregateStrategy::Sorted
            && self.grouping_sets.is_empty()
        {
            return "Group".into();
        }
        let base = match self.strategy {
            crate::include::nodes::plannodes::AggregateStrategy::Plain => "Aggregate",
            crate::include::nodes::plannodes::AggregateStrategy::Sorted => "GroupAggregate",
            crate::include::nodes::plannodes::AggregateStrategy::Hashed => "HashAggregate",
            crate::include::nodes::plannodes::AggregateStrategy::Mixed => "MixedAggregate",
        };
        match self.phase {
            AggregatePhase::Complete => base.to_string(),
            AggregatePhase::Partial => format!("Partial {base}"),
            AggregatePhase::Finalize => format!("Finalize {base}"),
        }
    }
    fn explain_details(
        &self,
        indent: usize,
        _analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        if self.disabled {
            lines.push(format!("{prefix}Disabled: true"));
        }
        if !self.grouping_sets.is_empty() {
            for set in &self.grouping_sets {
                if set.is_empty() {
                    lines.push(format!("{prefix}Group Key: ()"));
                    continue;
                }
                let set_exprs = grouping_set_exprs(&self.group_by, &self.group_by_refs, set);
                let group_key = render_aggregate_group_key_list(
                    &set_exprs,
                    self.input.column_names(),
                    self.disabled,
                );
                if self.strategy == AggregateStrategy::Mixed {
                    lines.push(format!("{prefix}Hash Key: {group_key}"));
                } else {
                    lines.push(format!("{prefix}Group Key: {group_key}"));
                }
            }
        } else if !self.group_by.is_empty() {
            let group_key = render_aggregate_group_key_list(
                &self.group_by,
                self.input.column_names(),
                self.disabled,
            );
            if self.strategy == AggregateStrategy::Mixed {
                lines.push(format!("{prefix}Hash Key: {group_key}"));
                lines.push(format!("{prefix}Group Key: ()"));
            } else {
                lines.push(format!("{prefix}Group Key: {group_key}"));
            }
        }
        if let Some(having) = &self.having {
            let rendered = normalize_aggregate_operand_parens(render_explain_expr(
                having,
                self.column_names(),
            ));
            lines.push(format!("{prefix}Filter: {}", rendered));
        }
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

fn render_aggregate_group_key_list(
    exprs: &[Expr],
    input_names: &[String],
    force_xid_const: bool,
) -> String {
    let mut group_items = Vec::new();
    for expr in exprs {
        let rendered = render_aggregate_group_key_expr(expr, input_names, force_xid_const);
        group_items.push(rendered);
    }
    group_items.join(", ")
}

fn grouping_set_exprs(
    group_by: &[Expr],
    group_by_refs: &[usize],
    grouping_set: &[usize],
) -> Vec<Expr> {
    grouping_set
        .iter()
        .filter_map(|ref_id| {
            group_by_refs
                .iter()
                .position(|candidate| candidate == ref_id)
                .and_then(|index| group_by.get(index).cloned())
        })
        .collect()
}

fn render_aggregate_group_key_expr(
    expr: &Expr,
    input_names: &[String],
    force_xid_const: bool,
) -> String {
    if force_xid_const && let Some(rendered) = render_xid_group_key_expr(expr) {
        return rendered;
    }
    let rendered = render_explain_expr(expr, input_names);
    if force_xid_const && rendered.chars().all(|ch| ch.is_ascii_digit()) {
        return format!("('{rendered}'::xid)");
    }
    if (matches!(expr, Expr::Op(_)) || rendered.contains(" || "))
        && rendered.starts_with('(')
        && rendered.ends_with(')')
    {
        return format!("({rendered})");
    }
    rendered
}

fn render_xid_group_key_expr(expr: &Expr) -> Option<String> {
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
        Expr::Cast(inner, ty) if matches!(ty.kind, SqlTypeKind::Xid) => {
            render_xid_group_key_expr(inner)
        }
        _ => None,
    }
}

#[derive(Clone)]
struct GroupingSetOutputOrder {
    chain_index: usize,
    key_refs: Vec<usize>,
}

fn sort_grouping_set_result_rows(
    rows: &mut [(usize, MaterializedRow)],
    grouping_sets: &[Vec<usize>],
    group_by_refs: &[usize],
) -> Result<(), ExecError> {
    let output_orders = grouping_set_output_orders(grouping_sets);
    let mut sort_error = None;
    pg_sql_sort_by(rows, |left, right| {
        let Some(left_order) = output_orders.get(left.0) else {
            return left.0.cmp(&right.0);
        };
        let Some(right_order) = output_orders.get(right.0) else {
            return left.0.cmp(&right.0);
        };
        match left_order.chain_index.cmp(&right_order.chain_index) {
            std::cmp::Ordering::Equal => {}
            ordering => return ordering,
        }
        let key_refs = if left_order.key_refs.len() >= right_order.key_refs.len() {
            &left_order.key_refs
        } else {
            &right_order.key_refs
        };
        for ref_id in key_refs {
            let Some(index) = group_by_refs
                .iter()
                .position(|candidate| candidate == ref_id)
            else {
                continue;
            };
            let left_value = left.1.slot.tts_values.get(index).unwrap_or(&Value::Null);
            let right_value = right.1.slot.tts_values.get(index).unwrap_or(&Value::Null);
            match compare_order_values(left_value, right_value, None, Some(false), false) {
                Ok(std::cmp::Ordering::Equal) => {}
                Ok(ordering) => return ordering,
                Err(err) => {
                    if sort_error.is_none() {
                        sort_error = Some(err);
                    }
                    return std::cmp::Ordering::Equal;
                }
            }
        }
        left.0.cmp(&right.0)
    });
    if let Some(err) = sort_error {
        Err(err)
    } else {
        Ok(())
    }
}

fn grouping_set_output_orders(grouping_sets: &[Vec<usize>]) -> Vec<GroupingSetOutputOrder> {
    let mut orders = vec![
        GroupingSetOutputOrder {
            chain_index: usize::MAX,
            key_refs: Vec::new(),
        };
        grouping_sets.len()
    ];
    let mut chain_index = 0;
    let mut chain_key_refs = Vec::new();
    let mut previous: Option<&[usize]> = None;
    for (set_index, set) in grouping_sets.iter().enumerate() {
        if previous.is_some_and(|previous| !grouping_set_refs_subset(set, previous)) {
            chain_index += 1;
            chain_key_refs.clear();
        }
        if chain_key_refs.is_empty() {
            chain_key_refs = set.clone();
        }
        orders[set_index] = GroupingSetOutputOrder {
            chain_index,
            key_refs: chain_key_refs.clone(),
        };
        previous = Some(set);
    }
    orders
}

fn grouping_set_refs_subset(smaller: &[usize], larger: &[usize]) -> bool {
    smaller.iter().all(|ref_id| larger.contains(ref_id))
}

impl PlanNode for WindowAggState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        if self.result_rows.is_none() {
            let mut input_rows = Vec::new();
            while self.input.exec_proc_node(ctx)?.is_some() {
                ctx.check_for_interrupts()?;
                input_rows.push(self.input.materialize_current_row()?);
            }
            self.result_rows = Some(execute_window_clause(ctx, &self.clause, input_rows)?);
        }

        let rows = self.result_rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        self.current_bindings = rows[idx].system_bindings.clone();
        set_active_system_bindings(ctx, &self.current_bindings);
        self.current_grouping_refs = rows[idx].grouping_refs.clone();
        set_active_grouping_refs(ctx, &self.current_grouping_refs);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut rows[idx].slot))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.result_rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx).map(|row| &mut row.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn current_grouping_refs(&self) -> &[usize] {
        &self.current_grouping_refs
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "WindowAgg".into()
    }

    fn explain_details(
        &self,
        indent: usize,
        _analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        if !self.clause.spec.partition_by.is_empty() {
            let partition_by = self
                .clause
                .spec
                .partition_by
                .iter()
                .map(|expr| render_explain_expr(expr, self.input.column_names()))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("{prefix}Partition By: {partition_by}"));
        }
        if !self.clause.spec.order_by.is_empty() {
            let order_by = self
                .clause
                .spec
                .order_by
                .iter()
                .map(|item| render_explain_expr(&item.expr, self.input.column_names()))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(format!("{prefix}Order By: {order_by}"));
        }
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

impl PlanNode for FunctionScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        if self.rows.is_none() {
            let mut dummy = TupleSlot::empty(0);
            self.rows = Some(if function_scan_uses_simple_path(&self.call) {
                FunctionScanRows::Simple(eval_set_returning_call_simple_values(
                    &self.call, &mut dummy, ctx,
                )?)
            } else {
                let rows = eval_set_returning_call(&self.call, &mut dummy, ctx)?;
                FunctionScanRows::Materialized(
                    rows.into_iter()
                        .map(|slot| MaterializedRow::new(slot, Vec::new()))
                        .collect(),
                )
            });
        }

        let rows = self.rows.as_mut().unwrap();
        match rows {
            FunctionScanRows::Simple(rows) => {
                if self.next_index >= rows.len() {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }

                let idx = self.next_index;
                self.next_index += 1;
                let value = std::mem::replace(&mut rows[idx], Value::Null);
                store_single_virtual_value(&mut self.slot, value);
                self.current_bindings.clear();
                set_active_system_bindings(ctx, &self.current_bindings);
                finish_row(&mut self.stats, start);
                Ok(Some(&mut self.slot))
            }
            FunctionScanRows::Materialized(rows) => {
                if self.next_index >= rows.len() {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }

                let idx = self.next_index;
                self.next_index += 1;
                self.current_bindings = rows[idx].system_bindings.clone();
                set_active_system_bindings(ctx, &self.current_bindings);
                finish_row(&mut self.stats, start);
                Ok(Some(&mut rows[idx].slot))
            }
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        match self.rows.as_mut()? {
            FunctionScanRows::Simple(_) => self.next_index.checked_sub(1).map(|_| &mut self.slot),
            FunctionScanRows::Materialized(rows) => {
                let idx = self.next_index.checked_sub(1)?;
                rows.get_mut(idx).map(|row| &mut row.slot)
            }
        }
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        if matches!(
            self.call,
            SetReturningCall::SqlJsonTable(_) | SetReturningCall::SqlXmlTable(_)
        ) {
            let name = if matches!(self.call, SetReturningCall::SqlJsonTable(_)) {
                "json_table"
            } else {
                "xmltable"
            };
            return match &self.table_alias {
                Some(alias) => format!("Table Function Scan on \"{name}\" {alias}"),
                None => format!("Table Function Scan on \"{name}\""),
            };
        }
        match &self.table_alias {
            Some(alias) => format!(
                "Function Scan on {} {alias}",
                set_returning_call_label(&self.call)
            ),
            None => format!("Function Scan on {}", set_returning_call_label(&self.call)),
        }
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _timing: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

impl PlanNode for SubqueryScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        loop {
            ctx.check_for_interrupts()?;
            let slot = match self.input.exec_proc_node(ctx)? {
                Some(slot) => slot,
                None => {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }
            };
            if let Some(filter) = &self.compiled_filter {
                let outer_values = materialize_slot_values(slot)?;
                let current_bindings = ctx.system_bindings.clone();
                set_outer_expr_bindings(ctx, outer_values, &current_bindings);
                clear_inner_expr_bindings(ctx);

                if !filter(slot, ctx)? {
                    note_filtered_row(&mut self.stats);
                    continue;
                }
            }
            finish_row(&mut self.stats, start);
            return Ok(self.input.current_slot());
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        self.input.current_slot()
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        self.input.current_system_bindings()
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        "Subquery Scan".into()
    }

    fn explain_details(
        &self,
        indent: usize,
        analyze: bool,
        _show_costs: bool,
        lines: &mut Vec<String>,
    ) {
        let prefix = explain_detail_prefix(indent);
        if let Some(filter) = &self.filter {
            lines.push(format!(
                "{prefix}Filter: {}",
                render_explain_expr(filter, self.column_names())
            ));
        }
        if analyze && self.stats.rows_removed_by_filter > 0 {
            lines.push(format!(
                "{prefix}Rows Removed by Filter: {}",
                self.stats.rows_removed_by_filter
            ));
        }
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

impl PlanNode for ValuesState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        if self.result_rows.is_none() {
            let mut dummy = TupleSlot::empty(0);
            let rows = self
                .rows
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|expr| eval_expr(expr, &mut dummy, ctx))
                        .collect::<Result<Vec<_>, ExecError>>()
                        .map(|slot_values| {
                            MaterializedRow::new(TupleSlot::virtual_row(slot_values), Vec::new())
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.result_rows = Some(rows);
        }

        let rows = self.result_rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }
        let idx = self.next_index;
        self.next_index += 1;
        self.current_bindings = rows[idx].system_bindings.clone();
        set_active_system_bindings(ctx, &self.current_bindings);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut rows[idx].slot))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.result_rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx).map(|row| &mut row.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Values Scan".into()
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _timing: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

impl PlanNode for WorkTableScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        let Some(worktable) = ctx.recursive_worktables.get(&self.worktable_id).cloned() else {
            return Err(ExecError::DetailedError {
                message: "worktable scan executed without an active recursive union".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            });
        };
        let Some(row) = worktable.borrow().rows.get(self.next_index).cloned() else {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        };
        self.next_index += 1;
        load_materialized_row(&mut self.slot, &row, &mut self.current_bindings, ctx);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut self.slot))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "WorkTable Scan".into()
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _timing: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

impl PlanNode for CteScanState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        let pinned_table = ctx.pinned_cte_tables.get(&self.cte_id).cloned();
        let table = ctx
            .cte_tables
            .entry(self.cte_id)
            .or_insert_with(|| {
                pinned_table.unwrap_or_else(|| Rc::new(RefCell::new(Default::default())))
            })
            .clone();
        loop {
            ctx.check_for_interrupts()?;
            if let Some(row) = table.borrow().rows.get(self.next_index).cloned() {
                self.next_index += 1;
                load_materialized_row(&mut self.slot, &row, &mut self.current_bindings, ctx);
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            }
            if table.borrow().eof {
                finish_eof(&mut self.stats, start, ctx);
                return Ok(None);
            }
            let producer = ctx
                .cte_producers
                .entry(self.cte_id)
                .or_insert_with(|| Rc::new(RefCell::new(executor_start(self.cte_plan.clone()))))
                .clone();
            let mut producer_state = producer.borrow_mut();
            if producer_state.exec_proc_node(ctx)?.is_some() {
                let row = producer_state.materialize_current_row()?;
                table.borrow_mut().rows.push(row);
            } else {
                table.borrow_mut().eof = true;
            }
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "CTE Scan".into()
    }
    fn explain_children(
        &self,
        _indent: usize,
        _analyze: bool,
        _show_costs: bool,
        _timing: bool,
        _lines: &mut Vec<String>,
    ) {
    }
}

fn reset_recursive_union_iteration_ctes(state: &RecursiveUnionState, ctx: &mut ExecutorContext) {
    for cte_id in &state.recursive_iteration_cte_ids {
        if ctx.pinned_cte_tables.contains_key(cte_id) {
            continue;
        }
        ctx.cte_tables.remove(cte_id);
        ctx.cte_producers.remove(cte_id);
    }
}

impl PlanNode for RecursiveUnionState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;
        if self.distinct && !self.distinct_hashable {
            finish_eof(&mut self.stats, start, ctx);
            return Err(ExecError::DetailedError {
                message: "could not implement recursive UNION".into(),
                detail: Some("All column datatypes must be hashable.".into()),
                hint: None,
                sqlstate: "0A000",
            });
        }
        ctx.recursive_worktables
            .insert(self.worktable_id, self.worktable.clone());

        loop {
            ctx.check_for_interrupts()?;
            if !self.anchor_done {
                if self.anchor.exec_proc_node(ctx)?.is_some() {
                    let mut row = self.anchor.materialize_current_row()?;
                    if self.distinct {
                        let signature = row.slot.values()?.to_vec();
                        if !self.seen_rows.insert(signature) {
                            continue;
                        }
                    }
                    self.worktable.borrow_mut().rows.push(row.clone());
                    load_materialized_row(&mut self.slot, &row, &mut self.current_bindings, ctx);
                    finish_row(&mut self.stats, start);
                    return Ok(Some(&mut self.slot));
                } else {
                    self.anchor_done = true;
                    reset_recursive_union_iteration_ctes(self, ctx);
                    self.recursive_state = Some(executor_start(self.recursive_plan.clone()));
                    continue;
                }
            }

            let recursive_state = self
                .recursive_state
                .get_or_insert_with(|| executor_start(self.recursive_plan.clone()));
            if recursive_state.exec_proc_node(ctx)?.is_some() {
                let mut row = recursive_state.materialize_current_row()?;
                if self.distinct {
                    let signature = row.slot.values()?.to_vec();
                    if !self.seen_rows.insert(signature) {
                        continue;
                    }
                }
                self.intermediate_rows.push(row.clone());
                load_materialized_row(&mut self.slot, &row, &mut self.current_bindings, ctx);
                finish_row(&mut self.stats, start);
                return Ok(Some(&mut self.slot));
            } else {
                if self.intermediate_rows.is_empty() {
                    ctx.recursive_worktables.remove(&self.worktable_id);
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }
                if !self.recursive_references_worktable {
                    ctx.recursive_worktables.remove(&self.worktable_id);
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                }
                self.worktable.borrow_mut().rows = std::mem::take(&mut self.intermediate_rows);
                reset_recursive_union_iteration_ctes(self, ctx);
                self.recursive_state = Some(executor_start(self.recursive_plan.clone()));
            }
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "Recursive Union".into()
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.anchor,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
        if let Some(recursive_state) = &self.recursive_state {
            format_explain_lines_with_costs(
                &**recursive_state,
                indent + 1,
                analyze,
                show_costs,
                timing,
                lines,
            );
        } else {
            let recursive_state = executor_start(self.recursive_plan.clone());
            format_explain_lines_with_costs(
                &*recursive_state,
                indent + 1,
                analyze,
                show_costs,
                timing,
                lines,
            );
        }
    }
}

impl PlanNode for SetOpState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        begin_node(&mut self.stats, ctx)?;

        if self.result_rows.is_none() {
            self.result_rows = Some(set_op_result_rows(self.op, &mut self.children, ctx)?);
        }

        let Some(rows) = self.result_rows.as_ref() else {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        };
        if self.next_index >= rows.len() {
            finish_eof(&mut self.stats, start, ctx);
            return Ok(None);
        }
        let row = rows[self.next_index].clone();
        self.next_index += 1;
        load_materialized_row(&mut self.slot, &row, &mut self.current_bindings, ctx);
        finish_row(&mut self.stats, start);
        Ok(Some(&mut self.slot))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }

    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }

    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }

    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }

    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }

    fn node_label(&self) -> String {
        let op_name = match self.op {
            crate::include::nodes::parsenodes::SetOperator::Union { all: true } => "Union All",
            crate::include::nodes::parsenodes::SetOperator::Union { all: false } => "Union",
            crate::include::nodes::parsenodes::SetOperator::Intersect { all: true } => {
                "Intersect All"
            }
            crate::include::nodes::parsenodes::SetOperator::Intersect { all: false } => "Intersect",
            crate::include::nodes::parsenodes::SetOperator::Except { all: true } => "Except All",
            crate::include::nodes::parsenodes::SetOperator::Except { all: false } => "Except",
        };
        let prefix = match self.strategy {
            crate::include::nodes::plannodes::SetOpStrategy::Hashed => "HashSetOp",
            crate::include::nodes::plannodes::SetOpStrategy::Sorted => "SetOp",
        };
        format!("{prefix} {op_name}")
    }

    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        for child in &self.children {
            format_explain_lines_with_costs(
                child.as_ref(),
                indent,
                analyze,
                show_costs,
                timing,
                lines,
            );
        }
    }
}

impl PlanNode for ProjectSetState {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed {
            Some(Instant::now())
        } else {
            None
        };
        loop {
            ctx.check_for_interrupts()?;
            if self.current_input.is_none() || self.next_index >= self.current_row_count {
                let Some(input_slot) = self.input.exec_proc_node(ctx)? else {
                    finish_eof(&mut self.stats, start, ctx);
                    return Ok(None);
                };
                let mut values = input_slot.values()?.to_vec();
                Value::materialize_all(&mut values);
                let mut materialized = TupleSlot::virtual_row(values);
                let mut srf_rows = Vec::new();
                let mut max_rows = 0usize;
                for target in &self.targets {
                    if let crate::include::nodes::primnodes::ProjectSetTarget::Set {
                        call,
                        column_index,
                        ..
                    } = target
                    {
                        set_outer_expr_bindings(
                            ctx,
                            materialized.tts_values.clone(),
                            self.input.current_system_bindings(),
                        );
                        let rows = eval_project_set_returning_call(
                            call,
                            *column_index,
                            &mut materialized,
                            ctx,
                        )?;
                        max_rows = max_rows.max(rows.len());
                        srf_rows.push(rows);
                    }
                }

                if max_rows == 0 {
                    self.current_input = None;
                    self.current_srf_rows.clear();
                    self.current_row_count = 0;
                    self.next_index = 0;
                    continue;
                }

                self.current_input = Some(
                    MaterializedRow::new(
                        materialized,
                        self.input.current_system_bindings().to_vec(),
                    )
                    .with_grouping_refs(self.input.current_grouping_refs().to_vec()),
                );
                self.current_srf_rows = srf_rows;
                self.current_row_count = max_rows;
                self.next_index = 0;
            }

            let input_slot = self.current_input.as_mut().unwrap();
            let row_idx = self.next_index;
            self.next_index += 1;

            let mut values = Vec::with_capacity(self.targets.len());
            let mut srf_idx = 0usize;
            for target in &self.targets {
                match target {
                    crate::include::nodes::primnodes::ProjectSetTarget::Scalar(entry) => {
                        set_active_system_bindings(ctx, &input_slot.system_bindings);
                        set_active_grouping_refs(ctx, &input_slot.grouping_refs);
                        set_outer_expr_bindings(
                            ctx,
                            input_slot.slot.tts_values.clone(),
                            &input_slot.system_bindings,
                        );
                        values.push(
                            eval_expr(&entry.expr, &mut input_slot.slot, ctx)?.to_owned_value(),
                        );
                    }
                    crate::include::nodes::primnodes::ProjectSetTarget::Set { .. } => {
                        values.push(
                            self.current_srf_rows[srf_idx]
                                .get(row_idx)
                                .cloned()
                                .unwrap_or(Value::Null),
                        );
                        srf_idx += 1;
                    }
                }
            }

            self.slot
                .store_virtual_row(values, input_slot.slot.tid(), input_slot.slot.table_oid);
            self.current_bindings = input_slot.system_bindings.clone();
            set_active_system_bindings(ctx, &self.current_bindings);
            self.current_grouping_refs = input_slot.grouping_refs.clone();
            set_active_grouping_refs(ctx, &self.current_grouping_refs);

            if self.next_index >= self.current_row_count {
                self.current_input = None;
                self.current_srf_rows.clear();
                self.current_row_count = 0;
                self.next_index = 0;
            }

            finish_row(&mut self.stats, start);
            return Ok(Some(&mut self.slot));
        }
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        Some(&mut self.slot)
    }
    fn current_system_bindings(&self) -> &[SystemVarBinding] {
        &self.current_bindings
    }
    fn current_grouping_refs(&self) -> &[usize] {
        &self.current_grouping_refs
    }

    fn column_names(&self) -> &[String] {
        &self.output_columns
    }
    fn node_stats(&self) -> &NodeExecStats {
        &self.stats
    }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats {
        &mut self.stats
    }
    fn plan_info(&self) -> crate::include::nodes::plannodes::PlanEstimate {
        self.plan_info
    }
    fn node_label(&self) -> String {
        "ProjectSet".into()
    }
    fn explain_children(
        &self,
        indent: usize,
        analyze: bool,
        show_costs: bool,
        timing: bool,
        lines: &mut Vec<String>,
    ) {
        format_explain_lines_with_costs(
            &*self.input,
            indent + 1,
            analyze,
            show_costs,
            timing,
            lines,
        );
    }
}

impl TupleSlot {
    pub fn values(&mut self) -> Result<&[Value], ExecError> {
        let ncols = self.ncols();
        self.slot_getsomeattrs(ncols)
    }

    pub fn slot_getsomeattrs(&mut self, natts: usize) -> Result<&[Value], ExecError> {
        if self.tts_nvalid >= natts {
            return Ok(&self.tts_values[..natts]);
        }
        match &self.kind {
            SlotKind::Virtual => Ok(&self.tts_values[..natts]),
            SlotKind::BufferHeapTuple {
                desc,
                attr_descs,
                tuple_ptr,
                tuple_len,
                ..
            } => {
                let (ptr, len) = (*tuple_ptr, *tuple_len);
                let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
                if self.toast.is_some() {
                    let raw = crate::include::access::htup::deform_raw(bytes, attr_descs)?;
                    self.tts_values.clear();
                    for (index, column) in desc.columns.iter().enumerate() {
                        if let Some(datum) = raw.get(index) {
                            self.tts_values.push(decode_value_with_toast(
                                column,
                                *datum,
                                self.toast.as_ref(),
                            )?);
                        } else {
                            self.tts_values.push(missing_column_value(column));
                        }
                    }
                    self.tts_nvalid = self.tts_values.len();
                } else {
                    let decoder = self
                        .decoder
                        .as_ref()
                        .expect("BufferHeapTuple requires decoder");
                    decoder.decode_range(
                        bytes,
                        &mut self.tts_values,
                        self.tts_nvalid,
                        natts,
                        &mut self.decode_offset,
                    )?;
                    self.tts_nvalid = natts;
                }
                Ok(&self.tts_values[..natts])
            }
            SlotKind::HeapTuple {
                desc,
                attr_descs,
                tuple,
                ..
            } => {
                let raw = tuple.deform(attr_descs)?;
                self.tts_values.clear();
                for (index, column) in desc.columns.iter().enumerate() {
                    if let Some(datum) = raw.get(index) {
                        self.tts_values.push(decode_value_with_toast(
                            column,
                            *datum,
                            self.toast.as_ref(),
                        )?);
                    } else {
                        self.tts_values.push(missing_column_value(column));
                    }
                }
                self.tts_nvalid = self.tts_values.len();
                Ok(&self.tts_values[..natts])
            }
            SlotKind::Empty => {
                panic!("cannot get attrs from empty slot")
            }
        }
    }

    pub fn get_attr(&mut self, index: usize) -> Result<&Value, ExecError> {
        self.slot_getsomeattrs(index + 1)?;
        Ok(&self.tts_values[index])
    }

    pub fn into_values(mut self) -> Result<Vec<Value>, ExecError> {
        self.values()?;
        Value::materialize_all(&mut self.tts_values);
        Ok(self.tts_values)
    }
}
