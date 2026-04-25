use super::*;
use std::cell::RefCell;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AggregateClauseKind {
    Other,
    SelectTarget,
    Where,
    Having,
    Filter,
    GroupBy,
    OrderBy,
    JoinOn,
    FromSubselect,
    FromFunction,
    Policy,
    IndexPredicate,
    CopyWhere,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum AggregateOwnership {
    CurrentLevel,
    OuterLevel(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AggregateRefUsage {
    pub(super) agg: CollectedAggregate,
    pub(super) ownership: AggregateOwnership,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct AggregateUsageSummary {
    pub(super) has_local_agg: bool,
    pub(super) local_aggs: Vec<CollectedAggregate>,
    pub(super) outer_agg_refs: Vec<AggregateRefUsage>,
}

#[derive(Debug, Clone)]
pub(crate) struct VisibleAggregateScope {
    pub(super) input_scope: BoundScope,
    pub(super) grouped_outer: Option<GroupedOuterScope>,
    pub(super) aggs: Vec<CollectedAggregate>,
    pub(super) levelsup: usize,
}

#[derive(Debug, Clone, Default)]
struct AggregateExprInfo {
    min_varlevel: Option<usize>,
    agg_refs: Vec<AggregateRefUsage>,
}

thread_local! {
    static VISIBLE_AGG_SCOPE_STACK: RefCell<Vec<VisibleAggregateScope>> = const { RefCell::new(Vec::new()) };
    static LOCAL_AGG_SCOPE_STACK: RefCell<Vec<VisibleAggregateScope>> = const { RefCell::new(Vec::new()) };
}

impl AggregateClauseKind {
    fn aggregate_error(self) -> Option<ParseError> {
        let message = match self {
            AggregateClauseKind::Where => "aggregate functions are not allowed in WHERE",
            AggregateClauseKind::GroupBy => "aggregate functions are not allowed in GROUP BY",
            AggregateClauseKind::Filter => "aggregate functions are not allowed in FILTER",
            AggregateClauseKind::JoinOn => "aggregate functions are not allowed in JOIN conditions",
            AggregateClauseKind::FromSubselect => {
                "aggregate functions are not allowed in FROM clause of their own query level"
            }
            AggregateClauseKind::FromFunction => {
                "aggregate functions are not allowed in functions in FROM"
            }
            AggregateClauseKind::Policy => {
                "aggregate functions are not allowed in policy expressions"
            }
            AggregateClauseKind::IndexPredicate => {
                "aggregate functions are not allowed in index predicates"
            }
            AggregateClauseKind::CopyWhere => {
                "aggregate functions are not allowed in COPY FROM WHERE conditions"
            }
            AggregateClauseKind::Other
            | AggregateClauseKind::SelectTarget
            | AggregateClauseKind::Having
            | AggregateClauseKind::OrderBy => return None,
        };
        Some(ParseError::DetailedError {
            message: message.into(),
            detail: None,
            hint: None,
            sqlstate: "42803",
        })
    }
}

impl AggregateExprInfo {
    fn note_varlevel(&mut self, level: usize) {
        match self.min_varlevel {
            Some(existing) if existing <= level => {}
            _ => self.min_varlevel = Some(level),
        }
    }

    fn min_agglevel(&self) -> Option<usize> {
        self.agg_refs.iter().map(|usage| usage.levelsup()).min()
    }

    fn merge(&mut self, other: Self) {
        if let Some(level) = other.min_varlevel {
            self.note_varlevel(level);
        }
        for agg_ref in other.agg_refs {
            if !self.agg_refs.contains(&agg_ref) {
                self.agg_refs.push(agg_ref);
            }
        }
    }

    fn translated_from_child(self) -> Self {
        let mut translated = Self::default();
        if let Some(level) = self.min_varlevel.and_then(|level| level.checked_sub(1)) {
            translated.note_varlevel(level);
        }
        for agg_ref in self.agg_refs {
            let Some(levelsup) = agg_ref.levelsup().checked_sub(1) else {
                continue;
            };
            let usage = AggregateRefUsage {
                agg: agg_ref.agg,
                ownership: if levelsup == 0 {
                    AggregateOwnership::CurrentLevel
                } else {
                    AggregateOwnership::OuterLevel(levelsup)
                },
            };
            if !translated.agg_refs.contains(&usage) {
                translated.agg_refs.push(usage);
            }
        }
        translated
    }

    fn into_summary(self) -> AggregateUsageSummary {
        let mut local_aggs = Vec::new();
        let mut outer_agg_refs = Vec::new();
        for agg_ref in self.agg_refs {
            match agg_ref.ownership {
                AggregateOwnership::CurrentLevel => {
                    if !local_aggs.contains(&agg_ref.agg) {
                        local_aggs.push(agg_ref.agg);
                    }
                }
                AggregateOwnership::OuterLevel(_) => {
                    if !outer_agg_refs.contains(&agg_ref) {
                        outer_agg_refs.push(agg_ref);
                    }
                }
            }
        }
        AggregateUsageSummary {
            has_local_agg: !local_aggs.is_empty(),
            local_aggs,
            outer_agg_refs,
        }
    }
}

impl AggregateRefUsage {
    fn levelsup(&self) -> usize {
        match self.ownership {
            AggregateOwnership::CurrentLevel => 0,
            AggregateOwnership::OuterLevel(levelsup) => levelsup,
        }
    }
}

pub(super) fn with_visible_aggregate_scope<T>(
    scope: Option<VisibleAggregateScope>,
    f: impl FnOnce() -> Result<T, ParseError>,
) -> Result<T, ParseError> {
    if let Some(scope) = scope {
        VISIBLE_AGG_SCOPE_STACK.with(|stack| stack.borrow_mut().push(scope));
        let result = f();
        VISIBLE_AGG_SCOPE_STACK.with(|stack| {
            let popped = stack.borrow_mut().pop();
            debug_assert!(popped.is_some(), "visible aggregate scope stack underflow");
        });
        result
    } else {
        f()
    }
}

pub(super) fn current_visible_aggregate_scope() -> Option<VisibleAggregateScope> {
    VISIBLE_AGG_SCOPE_STACK.with(|stack| stack.borrow().last().cloned())
}

pub(super) fn with_local_aggregate_scope<T>(
    scope: Option<VisibleAggregateScope>,
    f: impl FnOnce() -> Result<T, ParseError>,
) -> Result<T, ParseError> {
    if let Some(scope) = scope {
        LOCAL_AGG_SCOPE_STACK.with(|stack| stack.borrow_mut().push(scope));
        let result = f();
        LOCAL_AGG_SCOPE_STACK.with(|stack| {
            let popped = stack.borrow_mut().pop();
            debug_assert!(popped.is_some(), "local aggregate scope stack underflow");
        });
        result
    } else {
        f()
    }
}

pub(super) fn current_local_aggregate_scope() -> Option<VisibleAggregateScope> {
    LOCAL_AGG_SCOPE_STACK.with(|stack| stack.borrow().last().cloned())
}

pub(super) fn child_visible_aggregate_scope() -> Option<VisibleAggregateScope> {
    current_local_aggregate_scope().or_else(|| {
        current_visible_aggregate_scope().map(|mut scope| {
            scope.levelsup += 1;
            scope
        })
    })
}

pub(super) fn build_local_aggregate_scope(
    input_scope: &BoundScope,
    grouped_outer: Option<&GroupedOuterScope>,
    aggs: &[CollectedAggregate],
) -> Option<VisibleAggregateScope> {
    if aggs.is_empty() {
        None
    } else {
        Some(VisibleAggregateScope {
            input_scope: input_scope.clone(),
            grouped_outer: grouped_outer.cloned(),
            aggs: aggs.to_vec(),
            levelsup: 1,
        })
    }
}

pub(super) fn match_visible_aggregate_call(
    name: &str,
    direct_args: &[SqlFunctionArg],
    args: &SqlCallArgs,
    order_by: &[OrderByItem],
    distinct: bool,
    func_variadic: bool,
    filter: Option<&SqlExpr>,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Option<(usize, VisibleAggregateScope)> {
    let scope = current_visible_aggregate_scope()?;
    let owner_scope = &scope.input_scope;
    let owner_outer_scopes = outer_scopes.get(scope.levelsup..).unwrap_or(&[]);
    let index = scope.aggs.iter().position(|agg| {
        agg.matches_call(
            name,
            direct_args,
            args,
            order_by,
            distinct,
            func_variadic,
            filter,
        ) || aggregate_calls_match_semantically(
            agg,
            name,
            direct_args,
            args,
            order_by,
            distinct,
            func_variadic,
            filter,
            owner_scope,
            catalog,
            owner_outer_scopes,
            ctes,
        )
    })?;
    Some((index, scope))
}

fn bind_aggregate_match_args(
    args: &SqlCallArgs,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Result<Vec<(Option<String>, Expr)>, ParseError> {
    if args.is_star() {
        return Ok(Vec::new());
    }
    args.args()
        .iter()
        .map(|arg| {
            Ok((
                arg.name.clone(),
                bind_expr_with_outer_and_ctes(
                    &arg.value,
                    scope,
                    catalog,
                    outer_scopes,
                    None,
                    ctes,
                )?,
            ))
        })
        .collect()
}

fn bind_aggregate_match_order_by(
    order_by: &[OrderByItem],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Result<Vec<(Expr, bool, Option<bool>, Option<String>)>, ParseError> {
    order_by
        .iter()
        .map(|item| {
            Ok((
                bind_expr_with_outer_and_ctes(
                    &item.expr,
                    scope,
                    catalog,
                    outer_scopes,
                    None,
                    ctes,
                )?,
                item.descending,
                item.nulls_first,
                item.using_operator.clone(),
            ))
        })
        .collect()
}

fn aggregate_calls_match_semantically(
    collected: &CollectedAggregate,
    name: &str,
    direct_args: &[SqlFunctionArg],
    args: &SqlCallArgs,
    order_by: &[OrderByItem],
    distinct: bool,
    func_variadic: bool,
    filter: Option<&SqlExpr>,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> bool {
    if !collected.name.eq_ignore_ascii_case(name)
        || collected.distinct != distinct
        || collected.func_variadic != func_variadic
        || collected.direct_args.len() != direct_args.len()
        || collected.args.is_star() != args.is_star()
        || collected.args.args().len() != args.args().len()
        || collected.order_by.len() != order_by.len()
    {
        return false;
    }

    let bound_collected_direct =
        bind_aggregate_match_arg_list(&collected.direct_args, scope, catalog, outer_scopes, ctes);
    let bound_direct =
        bind_aggregate_match_arg_list(direct_args, scope, catalog, outer_scopes, ctes);
    let bound_collected_args =
        bind_aggregate_match_args(&collected.args, scope, catalog, outer_scopes, ctes);
    let bound_args = bind_aggregate_match_args(args, scope, catalog, outer_scopes, ctes);
    let bound_collected_order =
        bind_aggregate_match_order_by(&collected.order_by, scope, catalog, outer_scopes, ctes);
    let bound_order = bind_aggregate_match_order_by(order_by, scope, catalog, outer_scopes, ctes);
    let bound_collected_filter = collected
        .filter
        .as_ref()
        .map(|expr| bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, None, ctes))
        .transpose();
    let bound_filter = filter
        .map(|expr| bind_expr_with_outer_and_ctes(expr, scope, catalog, outer_scopes, None, ctes))
        .transpose();

    matches!(
        (
            bound_collected_direct,
            bound_direct,
            bound_collected_args,
            bound_args,
            bound_collected_order,
            bound_order,
            bound_collected_filter,
            bound_filter,
        ),
        (
            Ok(collected_direct),
            Ok(direct),
            Ok(collected_args),
            Ok(args),
            Ok(collected_order),
            Ok(order),
            Ok(collected_filter),
            Ok(filter),
        ) if collected_direct == direct
            && collected_args == args
            && collected_order == order
            && collected_filter == filter
    )
}

fn bind_aggregate_match_arg_list(
    args: &[SqlFunctionArg],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Result<Vec<Expr>, ParseError> {
    args.iter()
        .map(|arg| {
            bind_expr_with_outer_and_ctes(&arg.value, scope, catalog, outer_scopes, None, ctes)
        })
        .collect()
}

pub(super) fn dedupe_local_aggregate_list(
    aggs: &[CollectedAggregate],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    ctes: &[BoundCte],
) -> Vec<CollectedAggregate> {
    let mut deduped: Vec<CollectedAggregate> = Vec::new();
    for agg in aggs {
        let duplicate = deduped.iter().any(|existing| {
            existing.matches_call(
                &agg.name,
                &agg.direct_args,
                &agg.args,
                &agg.order_by,
                agg.distinct,
                agg.func_variadic,
                agg.filter.as_ref(),
            ) || aggregate_calls_match_semantically(
                existing,
                &agg.name,
                &agg.direct_args,
                &agg.args,
                &agg.order_by,
                agg.distinct,
                agg.func_variadic,
                agg.filter.as_ref(),
                scope,
                catalog,
                outer_scopes,
                ctes,
            )
        });
        if !duplicate {
            deduped.push(agg.clone());
        }
    }
    deduped
}

pub(super) fn collect_local_aggregates(
    exprs: &[&SqlExpr],
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<Vec<CollectedAggregate>, ParseError> {
    let mut info = AggregateExprInfo::default();
    for expr in exprs {
        info.merge(analyze_expr_internal(
            expr,
            AggregateClauseKind::Other,
            scope,
            catalog,
            outer_scopes,
            grouped_outer,
            ctes,
            expanded_views,
        )?);
    }
    Ok(dedupe_local_aggregate_list(
        &info.into_summary().local_aggs,
        scope,
        catalog,
        outer_scopes,
        ctes,
    ))
}

pub(super) fn analyze_expr_aggregates_in_clause(
    expr: &SqlExpr,
    clause: AggregateClauseKind,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<AggregateUsageSummary, ParseError> {
    let info = analyze_expr_internal(
        expr,
        clause,
        scope,
        catalog,
        outer_scopes,
        grouped_outer,
        ctes,
        expanded_views,
    )?;
    let summary = info.into_summary();
    if summary.has_local_agg
        && let Some(err) = clause.aggregate_error()
    {
        return Err(err);
    }
    Ok(summary)
}

fn resolve_select_order_by_expr<'a>(expr: &'a SqlExpr, targets: &'a [SelectItem]) -> &'a SqlExpr {
    match expr {
        SqlExpr::Collate { expr, .. } => resolve_select_order_by_expr(expr, targets),
        SqlExpr::IntegerLiteral(value) => value
            .parse::<usize>()
            .ok()
            .filter(|ordinal| *ordinal > 0 && *ordinal <= targets.len())
            .map(|ordinal| &targets[ordinal - 1].expr)
            .unwrap_or(expr),
        SqlExpr::Column(name) => targets
            .iter()
            .find(|target| target.output_name.eq_ignore_ascii_case(name))
            .map(|target| &target.expr)
            .unwrap_or(expr),
        _ => expr,
    }
}

fn analyze_select_usage_with_outer(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<AggregateExprInfo, ParseError> {
    let local_ctes = bind_ctes(
        stmt.with_recursive,
        &stmt.with,
        catalog,
        outer_scopes,
        grouped_outer.clone(),
        outer_ctes,
        expanded_views,
    )?;
    let mut visible_ctes = local_ctes.clone();
    visible_ctes.extend_from_slice(outer_ctes);

    let (_, scope) = if let Some(from) = &stmt.from {
        bind_from_item_with_ctes(
            from,
            catalog,
            outer_scopes,
            grouped_outer.as_ref(),
            &visible_ctes,
            expanded_views,
        )?
    } else {
        (AnalyzedFrom::result(), empty_scope())
    };

    let mut info = AggregateExprInfo::default();

    if let Some(setop) = &stmt.set_operation {
        for input in &setop.inputs {
            info.merge(analyze_select_usage_with_outer(
                input,
                catalog,
                outer_scopes,
                grouped_outer.clone(),
                &visible_ctes,
                expanded_views,
            )?);
        }
    }

    if let Some(predicate) = &stmt.where_clause {
        info.merge(analyze_expr_internal(
            predicate,
            AggregateClauseKind::Where,
            &scope,
            catalog,
            outer_scopes,
            grouped_outer.as_ref(),
            &visible_ctes,
            expanded_views,
        )?);
    }
    for target in &stmt.targets {
        info.merge(analyze_expr_internal(
            &target.expr,
            AggregateClauseKind::SelectTarget,
            &scope,
            catalog,
            outer_scopes,
            grouped_outer.as_ref(),
            &visible_ctes,
            expanded_views,
        )?);
    }
    for group_expr in &stmt.group_by {
        info.merge(analyze_expr_internal(
            group_expr,
            AggregateClauseKind::GroupBy,
            &scope,
            catalog,
            outer_scopes,
            grouped_outer.as_ref(),
            &visible_ctes,
            expanded_views,
        )?);
    }
    if let Some(having) = &stmt.having {
        info.merge(analyze_expr_internal(
            having,
            AggregateClauseKind::Having,
            &scope,
            catalog,
            outer_scopes,
            grouped_outer.as_ref(),
            &visible_ctes,
            expanded_views,
        )?);
    }
    for order_by in &stmt.order_by {
        info.merge(analyze_expr_internal(
            resolve_select_order_by_expr(&order_by.expr, &stmt.targets),
            AggregateClauseKind::OrderBy,
            &scope,
            catalog,
            outer_scopes,
            grouped_outer.as_ref(),
            &visible_ctes,
            expanded_views,
        )?);
    }

    Ok(info)
}

fn analyze_expr_internal(
    expr: &SqlExpr,
    clause: AggregateClauseKind,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<&GroupedOuterScope>,
    ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<AggregateExprInfo, ParseError> {
    if matches_grouped_outer_expr(expr, grouped_outer) {
        return analyze_expr_internal(
            expr,
            clause,
            scope,
            catalog,
            outer_scopes,
            None,
            ctes,
            expanded_views,
        );
    }

    let mut info = AggregateExprInfo::default();
    match expr {
        SqlExpr::Column(name) => {
            if name == "*" {
                return Ok(info);
            }
            if let Some(relation_name) = name.strip_suffix(".*") {
                if let Some(level) =
                    relation_row_reference_level(scope, outer_scopes, relation_name)
                {
                    info.note_varlevel(level);
                }
                return Ok(info);
            }
            if let Some(system_column) =
                resolve_system_column_with_outer(scope, outer_scopes, name)?
            {
                info.note_varlevel(system_column.varlevelsup);
                return Ok(info);
            }
            match resolve_column_with_outer(scope, outer_scopes, name, grouped_outer) {
                Ok(ResolvedColumn::Local(_)) => info.note_varlevel(0),
                Ok(ResolvedColumn::Outer { depth, .. }) => info.note_varlevel(depth + 1),
                Err(ParseError::UnknownColumn(_))
                    if relation_row_reference_level(scope, outer_scopes, name).is_some() =>
                {
                    info.note_varlevel(
                        relation_row_reference_level(scope, outer_scopes, name)
                            .expect("checked above"),
                    );
                }
                Err(err) => return Err(err),
            }
        }
        SqlExpr::Default
        | SqlExpr::Const(_)
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => {}
        SqlExpr::FuncCall {
            name,
            args,
            order_by,
            within_group,
            distinct,
            func_variadic,
            filter,
            over,
        } => {
            let is_aggregate = over.is_none()
                && aggregate_call_matches_catalog(catalog, name, args, within_group.as_deref());
            let aggregate_grouped_outer = if is_aggregate { None } else { grouped_outer };
            let direct_grouped_outer = grouped_outer;
            for arg in args.args() {
                info.merge(analyze_expr_internal(
                    &arg.value,
                    AggregateClauseKind::Other,
                    scope,
                    catalog,
                    outer_scopes,
                    if within_group.is_some() {
                        direct_grouped_outer
                    } else {
                        aggregate_grouped_outer
                    },
                    ctes,
                    expanded_views,
                )?);
            }
            for item in order_by {
                info.merge(analyze_expr_internal(
                    &item.expr,
                    AggregateClauseKind::OrderBy,
                    scope,
                    catalog,
                    outer_scopes,
                    aggregate_grouped_outer,
                    ctes,
                    expanded_views,
                )?);
            }
            if let Some(items) = within_group.as_deref() {
                for item in items {
                    info.merge(analyze_expr_internal(
                        &item.expr,
                        AggregateClauseKind::OrderBy,
                        scope,
                        catalog,
                        outer_scopes,
                        aggregate_grouped_outer,
                        ctes,
                        expanded_views,
                    )?);
                }
            }
            if let Some(filter) = filter.as_deref() {
                info.merge(analyze_expr_internal(
                    filter,
                    AggregateClauseKind::Filter,
                    scope,
                    catalog,
                    outer_scopes,
                    aggregate_grouped_outer,
                    ctes,
                    expanded_views,
                )?);
            }

            if is_aggregate {
                let min_agglevel = info.min_agglevel();
                let ownership_level = match (info.min_varlevel, min_agglevel) {
                    (Some(var_level), Some(agg_level)) => var_level.min(agg_level),
                    (Some(var_level), None) => var_level,
                    (None, Some(agg_level)) => agg_level,
                    (None, None) => 0,
                };
                if min_agglevel.is_some_and(|agg_level| agg_level == ownership_level) {
                    return Err(ParseError::DetailedError {
                        message: "aggregate function calls cannot be nested".into(),
                        detail: None,
                        hint: None,
                        sqlstate: "42803",
                    });
                }
                let usage = AggregateRefUsage {
                    agg: CollectedAggregate {
                        name: name.clone(),
                        direct_args: if within_group.is_some() {
                            args.args().to_vec()
                        } else {
                            Vec::new()
                        },
                        args: within_group
                            .as_deref()
                            .map(hypothetical_aggregate_args)
                            .unwrap_or_else(|| args.clone()),
                        order_by: within_group.clone().unwrap_or_else(|| order_by.clone()),
                        distinct: *distinct,
                        func_variadic: *func_variadic,
                        filter: filter.as_deref().cloned(),
                    },
                    ownership: if ownership_level == 0 {
                        AggregateOwnership::CurrentLevel
                    } else {
                        AggregateOwnership::OuterLevel(ownership_level)
                    },
                };
                if !info.agg_refs.contains(&usage) {
                    info.agg_refs.push(usage);
                }
            }
        }
        SqlExpr::ArrayLiteral(elements) | SqlExpr::Row(elements) => {
            for element in elements {
                info.merge(analyze_expr_internal(
                    element,
                    AggregateClauseKind::Other,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    expanded_views,
                )?);
            }
        }
        SqlExpr::FieldSelect { expr, field } if field == "*" => {
            if !matches!(expr.as_ref(), SqlExpr::Column(_)) {
                info.merge(analyze_expr_internal(
                    expr,
                    AggregateClauseKind::Other,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    expanded_views,
                )?);
            }
        }
        SqlExpr::PrefixOperator { expr, .. }
        | SqlExpr::FieldSelect { expr, .. }
        | SqlExpr::UnaryPlus(expr)
        | SqlExpr::Negate(expr)
        | SqlExpr::BitNot(expr)
        | SqlExpr::Not(expr)
        | SqlExpr::IsNull(expr)
        | SqlExpr::IsNotNull(expr)
        | SqlExpr::GeometryUnaryOp { expr, .. }
        | SqlExpr::Subscript { expr, .. }
        | SqlExpr::Cast(expr, _)
        | SqlExpr::Collate { expr, .. } => {
            info.merge(analyze_expr_internal(
                expr,
                AggregateClauseKind::Other,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
                expanded_views,
            )?);
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            info.merge(analyze_expr_internal(
                array,
                AggregateClauseKind::Other,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
                expanded_views,
            )?);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    info.merge(analyze_expr_internal(
                        lower,
                        AggregateClauseKind::Other,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                        expanded_views,
                    )?);
                }
                if let Some(upper) = &subscript.upper {
                    info.merge(analyze_expr_internal(
                        upper,
                        AggregateClauseKind::Other,
                        scope,
                        catalog,
                        outer_scopes,
                        grouped_outer,
                        ctes,
                        expanded_views,
                    )?);
                }
            }
        }
        SqlExpr::BinaryOperator { left, right, .. }
        | SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right)
        | SqlExpr::Concat(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::RegexMatch(left, right)
        | SqlExpr::And(left, right)
        | SqlExpr::Or(left, right)
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonbExistsAny(left, right)
        | SqlExpr::JsonbExistsAll(left, right)
        | SqlExpr::JsonbPathExists(left, right)
        | SqlExpr::JsonbPathMatch(left, right)
        | SqlExpr::GeometryBinaryOp { left, right, .. }
        | SqlExpr::QuantifiedArray {
            left, array: right, ..
        } => {
            info.merge(analyze_expr_internal(
                left,
                AggregateClauseKind::Other,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
                expanded_views,
            )?);
            info.merge(analyze_expr_internal(
                right,
                AggregateClauseKind::Other,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
                expanded_views,
            )?);
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            info.merge(analyze_expr_internal(
                expr,
                AggregateClauseKind::Other,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
                expanded_views,
            )?);
            info.merge(analyze_expr_internal(
                pattern,
                AggregateClauseKind::Other,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
                expanded_views,
            )?);
            if let Some(escape) = escape {
                info.merge(analyze_expr_internal(
                    escape,
                    AggregateClauseKind::Other,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    expanded_views,
                )?);
            }
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                info.merge(analyze_expr_internal(
                    arg,
                    AggregateClauseKind::Other,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    expanded_views,
                )?);
            }
            for arm in args {
                info.merge(analyze_expr_internal(
                    &arm.expr,
                    AggregateClauseKind::Other,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    expanded_views,
                )?);
                info.merge(analyze_expr_internal(
                    &arm.result,
                    AggregateClauseKind::Other,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    expanded_views,
                )?);
            }
            if let Some(defresult) = defresult {
                info.merge(analyze_expr_internal(
                    defresult,
                    AggregateClauseKind::Other,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    expanded_views,
                )?);
            }
        }
        SqlExpr::ScalarSubquery(select)
        | SqlExpr::ArraySubquery(select)
        | SqlExpr::Exists(select) => {
            info.merge(
                analyze_select_usage_with_outer(
                    select,
                    catalog,
                    &child_outer_scopes(scope, outer_scopes),
                    None,
                    ctes,
                    expanded_views,
                )?
                .translated_from_child(),
            );
        }
        SqlExpr::InSubquery { expr, subquery, .. } => {
            info.merge(analyze_expr_internal(
                expr,
                AggregateClauseKind::Other,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
                expanded_views,
            )?);
            info.merge(
                analyze_select_usage_with_outer(
                    subquery,
                    catalog,
                    &child_outer_scopes(scope, outer_scopes),
                    None,
                    ctes,
                    expanded_views,
                )?
                .translated_from_child(),
            );
        }
        SqlExpr::QuantifiedSubquery { left, subquery, .. } => {
            info.merge(analyze_expr_internal(
                left,
                AggregateClauseKind::Other,
                scope,
                catalog,
                outer_scopes,
                grouped_outer,
                ctes,
                expanded_views,
            )?);
            info.merge(
                analyze_select_usage_with_outer(
                    subquery,
                    catalog,
                    &child_outer_scopes(scope, outer_scopes),
                    None,
                    ctes,
                    expanded_views,
                )?
                .translated_from_child(),
            );
        }
        SqlExpr::Xml(xml) => {
            for child in xml.child_exprs() {
                info.merge(analyze_expr_internal(
                    child,
                    AggregateClauseKind::Other,
                    scope,
                    catalog,
                    outer_scopes,
                    grouped_outer,
                    ctes,
                    expanded_views,
                )?);
            }
        }
    }
    if clause != AggregateClauseKind::Other
        && info
            .agg_refs
            .iter()
            .any(|agg_ref| matches!(agg_ref.ownership, AggregateOwnership::CurrentLevel))
        && let Some(err) = clause.aggregate_error()
    {
        return Err(err);
    }
    Ok(info)
}

fn child_outer_scopes(scope: &BoundScope, outer_scopes: &[BoundScope]) -> Vec<BoundScope> {
    let mut child_outer = Vec::with_capacity(outer_scopes.len() + 1);
    child_outer.push(scope.clone());
    child_outer.extend_from_slice(outer_scopes);
    child_outer
}
