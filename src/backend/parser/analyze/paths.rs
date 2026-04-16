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
}

#[derive(Debug, Clone)]
struct ChosenIndexPath {
    index: BoundIndexRelation,
    keys: Vec<crate::include::access::scankey::ScanKeyData>,
    has_qual: bool,
    usable_prefix: usize,
    removes_order: bool,
}

fn simple_column_index(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Column(index) => Some(*index),
        Expr::Var(var) if var.varlevelsup == 0 && !crate::include::nodes::primnodes::is_system_attr(var.varattno) => {
            crate::include::nodes::primnodes::attrno_index(var.varattno)
        }
        _ => None,
    }
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
        let chosen = ChosenIndexPath {
            index: index.clone(),
            keys,
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
        let Some(column) = simple_column_index(&item.expr) else {
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
        Expr::Bool(bool_expr)
            if matches!(
                bool_expr.boolop,
                crate::include::nodes::primnodes::BoolExprType::And
            ) =>
        {
            let mut out = Vec::new();
            for arg in &bool_expr.args {
                out.extend(flatten_and_conjuncts(arg));
            }
            out
        }
        other => vec![other.clone()],
    }
}

fn indexable_qual(expr: &Expr) -> Option<IndexableQual> {
    fn mk(column: usize, strategy: u16, argument: &Value) -> Option<IndexableQual> {
        Some(IndexableQual {
            column,
            strategy,
            argument: argument.clone(),
        })
    }

    match expr {
        Expr::Op(op) => match op.op {
            crate::include::nodes::primnodes::OpExprKind::Eq => match op.args.as_slice() {
                [left, Expr::Const(value)] => simple_column_index(left)
                    .and_then(|column| mk(column, 3, value)),
                [Expr::Const(value), right] => simple_column_index(right)
                    .and_then(|column| mk(column, 3, value)),
                _ => None,
            },
            crate::include::nodes::primnodes::OpExprKind::Lt => match op.args.as_slice() {
                [left, Expr::Const(value)] => simple_column_index(left)
                    .and_then(|column| mk(column, 1, value)),
                [Expr::Const(value), right] => simple_column_index(right)
                    .and_then(|column| mk(column, 5, value)),
                _ => None,
            },
            crate::include::nodes::primnodes::OpExprKind::LtEq => match op.args.as_slice() {
                [left, Expr::Const(value)] => simple_column_index(left)
                    .and_then(|column| mk(column, 2, value)),
                [Expr::Const(value), right] => simple_column_index(right)
                    .and_then(|column| mk(column, 4, value)),
                _ => None,
            },
            crate::include::nodes::primnodes::OpExprKind::Gt => match op.args.as_slice() {
                [left, Expr::Const(value)] => simple_column_index(left)
                    .and_then(|column| mk(column, 5, value)),
                [Expr::Const(value), right] => simple_column_index(right)
                    .and_then(|column| mk(column, 1, value)),
                _ => None,
            },
            crate::include::nodes::primnodes::OpExprKind::GtEq => match op.args.as_slice() {
                [left, Expr::Const(value)] => simple_column_index(left)
                    .and_then(|column| mk(column, 4, value)),
                [Expr::Const(value), right] => simple_column_index(right)
                    .and_then(|column| mk(column, 2, value)),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
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
                ressortgroupref: 0,
                descending: item.descending,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}
