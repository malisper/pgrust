use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundModifyRowSource {
    Heap,
    Index {
        index: BoundIndexRelation,
        keys: Vec<crate::include::access::scankey::ScanKeyData>,
    },
}

#[derive(Debug, Clone)]
struct IndexableQual {
    column: usize,
    strategy: u16,
    argument: Value,
    expr: Expr,
}

#[derive(Debug, Clone)]
struct ChosenIndexPath {
    index: BoundIndexRelation,
    keys: Vec<crate::include::access::scankey::ScanKeyData>,
    residual: Option<Expr>,
    direction: crate::include::access::relscan::ScanDirection,
    has_qual: bool,
    usable_prefix: usize,
    removes_order: bool,
}

pub(super) fn maybe_rewrite_index_scan(plan: Plan, catalog: &dyn CatalogLookup) -> Plan {
    let (rel, relation_oid, toast, desc, filter, order_items) = match plan {
        Plan::SeqScan {
            rel,
            relation_oid,
            toast,
            desc,
        } => (rel, relation_oid, toast, desc, None, None),
        Plan::Filter { input, predicate } => match *input {
            Plan::SeqScan {
                rel,
                relation_oid,
                toast,
                desc,
            } => (rel, relation_oid, toast, desc, Some(predicate), None),
            other => {
                return Plan::Filter {
                    input: Box::new(other),
                    predicate,
                };
            }
        },
        Plan::OrderBy { input, items } => match *input {
            Plan::SeqScan {
                rel,
                relation_oid,
                toast,
                desc,
            } => (rel, relation_oid, toast, desc, None, Some(items)),
            Plan::Filter { input, predicate } => match *input {
                Plan::SeqScan {
                    rel,
                    relation_oid,
                    toast,
                    desc,
                } => (rel, relation_oid, toast, desc, Some(predicate), Some(items)),
                other => {
                    return Plan::OrderBy {
                        input: Box::new(Plan::Filter {
                            input: Box::new(other),
                            predicate,
                        }),
                        items,
                    };
                }
            },
            other => {
                return Plan::OrderBy {
                    input: Box::new(other),
                    items,
                };
            }
        },
        other => return other,
    };

    let indexes = catalog.index_relations_for_heap(relation_oid);
    choose_index_scan(rel, relation_oid, toast, desc, filter, order_items, indexes)
}

fn rebuild_scan_plan(
    rel: RelFileLocator,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    filter: Option<Expr>,
    order_items: Option<Vec<OrderByEntry>>,
) -> Plan {
    let mut plan = Plan::SeqScan {
        rel,
        relation_oid,
        toast,
        desc,
    };
    if let Some(predicate) = filter {
        plan = Plan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }
    if let Some(items) = order_items {
        plan = Plan::OrderBy {
            input: Box::new(plan),
            items,
        };
    }
    plan
}

fn choose_index_scan(
    rel: RelFileLocator,
    relation_oid: u32,
    toast: Option<ToastRelationRef>,
    desc: RelationDesc,
    filter: Option<Expr>,
    order_items: Option<Vec<OrderByEntry>>,
    indexes: Vec<BoundIndexRelation>,
) -> Plan {
    let Some(chosen) = choose_index_path(filter.as_ref(), order_items.as_deref(), &indexes) else {
        return rebuild_scan_plan(rel, relation_oid, toast, desc, filter, order_items);
    };

    let mut plan = Plan::IndexScan {
        rel,
        index_rel: chosen.index.rel,
        am_oid: chosen.index.index_meta.am_oid,
        toast,
        desc: desc.clone(),
        index_meta: chosen.index.index_meta.clone(),
        keys: chosen.keys,
        direction: chosen.direction,
    };
    if let Some(predicate) = chosen.residual {
        plan = Plan::Filter {
            input: Box::new(plan),
            predicate,
        };
    }
    if !chosen.removes_order
        && let Some(items) = order_items
    {
        plan = Plan::OrderBy {
            input: Box::new(plan),
            items,
        };
    }

    plan
}

fn choose_index_path(
    filter: Option<&Expr>,
    order_items: Option<&[OrderByEntry]>,
    indexes: &[BoundIndexRelation],
) -> Option<ChosenIndexPath> {
    let conjuncts = filter.map(flatten_and_conjuncts).unwrap_or_default();
    let parsed_quals = conjuncts
        .iter()
        .filter_map(indexable_qual)
        .collect::<Vec<_>>();

    let mut best: Option<ChosenIndexPath> = None;
    for index in indexes.iter().filter(|index| {
        index.index_meta.indisvalid
            && index.index_meta.indisready
            && !index.index_meta.indkey.is_empty()
            && index.index_meta.am_oid == crate::include::catalog::BTREE_AM_OID
    }) {
        let mut used = vec![false; parsed_quals.len()];
        let mut keys = Vec::new();
        let mut equality_prefix = 0usize;

        for attnum in &index.index_meta.indkey {
            let column = attnum.saturating_sub(1) as usize;
            if let Some((qual_idx, qual)) = parsed_quals
                .iter()
                .enumerate()
                .find(|(idx, qual)| !used[*idx] && qual.column == column && qual.strategy == 3)
            {
                used[qual_idx] = true;
                equality_prefix += 1;
                keys.push(crate::include::access::scankey::ScanKeyData {
                    attribute_number: equality_prefix as i16,
                    strategy: qual.strategy,
                    argument: qual.argument.clone(),
                });
                continue;
            }
            if let Some((qual_idx, qual)) = parsed_quals
                .iter()
                .enumerate()
                .find(|(idx, qual)| !used[*idx] && qual.column == column)
            {
                used[qual_idx] = true;
                keys.push(crate::include::access::scankey::ScanKeyData {
                    attribute_number: (equality_prefix + 1) as i16,
                    strategy: qual.strategy,
                    argument: qual.argument.clone(),
                });
            }
            break;
        }

        let usable_prefix = keys.len();
        let order_match =
            order_items.and_then(|items| index_order_match(items, index, equality_prefix));
        let has_qual = usable_prefix > 0;
        if !has_qual && order_match.is_none() {
            continue;
        }
        let residual = {
            let used_exprs = parsed_quals
                .iter()
                .enumerate()
                .filter_map(|(idx, qual)| {
                    used.get(idx)
                        .copied()
                        .unwrap_or(false)
                        .then_some(&qual.expr)
                })
                .collect::<Vec<_>>();
            let residual = conjuncts
                .iter()
                .filter(|expr| !used_exprs.iter().any(|used_expr| *used_expr == *expr))
                .cloned()
                .collect::<Vec<_>>();
            and_exprs(residual)
        };

        let chosen = ChosenIndexPath {
            index: index.clone(),
            keys,
            residual,
            direction: order_match
                .as_ref()
                .map(|(_, direction)| *direction)
                .unwrap_or(crate::include::access::relscan::ScanDirection::Forward),
            has_qual,
            usable_prefix,
            removes_order: order_match.is_some(),
        };

        match &best {
            None => best = Some(chosen),
            Some(existing) => {
                if (
                    chosen.has_qual as u8,
                    chosen.usable_prefix,
                    chosen.removes_order as u8,
                ) > (
                    existing.has_qual as u8,
                    existing.usable_prefix,
                    existing.removes_order as u8,
                ) {
                    best = Some(chosen);
                }
            }
        }
    }

    best
}

pub(super) fn choose_modify_row_source(
    predicate: Option<&Expr>,
    indexes: &[BoundIndexRelation],
) -> BoundModifyRowSource {
    if let Some(chosen) =
        choose_index_path(predicate, None, indexes).filter(|chosen| chosen.has_qual)
    {
        BoundModifyRowSource::Index {
            index: chosen.index,
            keys: chosen.keys,
        }
    } else {
        BoundModifyRowSource::Heap
    }
}

fn index_order_match(
    items: &[OrderByEntry],
    index: &BoundIndexRelation,
    equality_prefix: usize,
) -> Option<(usize, crate::include::access::relscan::ScanDirection)> {
    if items.is_empty() {
        return None;
    }
    let mut direction = None;
    let mut matched = 0usize;
    for (idx, item) in items.iter().enumerate() {
        let Expr::Column(column) = item.expr else {
            break;
        };
        let Some(attnum) = index.index_meta.indkey.get(equality_prefix + idx) else {
            break;
        };
        if *attnum as usize != column + 1 {
            break;
        }
        let item_direction = if item.descending {
            crate::include::access::relscan::ScanDirection::Backward
        } else {
            crate::include::access::relscan::ScanDirection::Forward
        };
        if let Some(existing) = direction {
            if existing != item_direction {
                return None;
            }
        } else {
            direction = Some(item_direction);
        }
        matched += 1;
    }
    (matched == items.len()).then_some((
        matched,
        direction.unwrap_or(crate::include::access::relscan::ScanDirection::Forward),
    ))
}

fn flatten_and_conjuncts(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::And(left, right) => {
            let mut out = flatten_and_conjuncts(left);
            out.extend(flatten_and_conjuncts(right));
            out
        }
        other => vec![other.clone()],
    }
}

fn indexable_qual(expr: &Expr) -> Option<IndexableQual> {
    fn mk(column: usize, strategy: u16, argument: &Value, expr: &Expr) -> Option<IndexableQual> {
        Some(IndexableQual {
            column,
            strategy,
            argument: argument.clone(),
            expr: expr.clone(),
        })
    }

    match expr {
        Expr::Eq(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 3, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 3, value, expr),
            _ => None,
        },
        Expr::Lt(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 1, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 5, value, expr),
            _ => None,
        },
        Expr::LtEq(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 2, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 4, value, expr),
            _ => None,
        },
        Expr::Gt(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 5, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 1, value, expr),
            _ => None,
        },
        Expr::GtEq(left, right) => match (&**left, &**right) {
            (Expr::Column(column), Expr::Const(value)) => mk(*column, 4, value, expr),
            (Expr::Const(value), Expr::Column(column)) => mk(*column, 2, value, expr),
            _ => None,
        },
        _ => None,
    }
}

fn and_exprs(mut exprs: Vec<Expr>) -> Option<Expr> {
    if exprs.is_empty() {
        return None;
    }
    let first = exprs.remove(0);
    Some(
        exprs
            .into_iter()
            .fold(first, |acc, expr| Expr::And(Box::new(acc), Box::new(expr))),
    )
}

pub(super) fn bind_order_by_items(
    items: &[OrderByItem],
    targets: &[TargetEntry],
    bind_expr: impl Fn(&SqlExpr) -> Result<Expr, ParseError>,
) -> Result<Vec<crate::backend::executor::OrderByEntry>, ParseError> {
    items
        .iter()
        .map(|item| {
            let expr = match &item.expr {
                SqlExpr::IntegerLiteral(value) => {
                    if let Ok(ordinal) = value.parse::<usize>() {
                        if ordinal > 0 && ordinal <= targets.len() {
                            targets[ordinal - 1].expr.clone()
                        } else {
                            return Err(ParseError::UnexpectedToken {
                                expected: "ORDER BY position in select list",
                                actual: value.clone(),
                            });
                        }
                    } else {
                        bind_expr(&item.expr)?
                    }
                }
                SqlExpr::Column(name) => {
                    if let Some(target) = targets
                        .iter()
                        .find(|target| target.name.eq_ignore_ascii_case(name))
                    {
                        target.expr.clone()
                    } else {
                        bind_expr(&item.expr)?
                    }
                }
                _ => bind_expr(&item.expr)?,
            };
            Ok(crate::backend::executor::OrderByEntry {
                expr,
                descending: item.descending,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}
