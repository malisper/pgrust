use super::index_predicates::predicate_implies_index_predicate;
use super::*;
use crate::backend::executor::cast_value;
use crate::backend::utils::cache::catcache::sql_type_oid;
use crate::include::catalog::{
    BTREE_AM_OID, GIST_AM_OID, HASH_AM_OID, SPGIST_AM_OID, bootstrap_pg_operator_rows,
    builtin_scalar_function_for_proc_oid, proc_oid_for_builtin_scalar_function,
};
use crate::include::nodes::primnodes::{BuiltinScalarFunction, OpExprKind, expr_sql_type_hint};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundModifyRowSource {
    Heap,
    Index {
        index: BoundIndexRelation,
        keys: Vec<crate::include::access::scankey::ScanKeyData>,
    },
}

#[derive(Debug, Clone)]
enum IndexStrategyLookup {
    Operator { oid: u32, kind: OpExprKind },
    Proc(u32),
}

#[derive(Debug, Clone)]
struct IndexableQual {
    column: Option<usize>,
    key_expr: Expr,
    lookup: IndexStrategyLookup,
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
    // :HACK: GiST build/maintenance now supports expression keys, but planner
    // path matching here still only recognizes bare table Vars. Deferred work
    // is to match quals and ORDER BY items against bound index expressions too.
    match strip_casts(expr) {
        Expr::Var(var)
            if var.varlevelsup == 0
                && !crate::include::nodes::primnodes::is_system_attr(var.varattno) =>
        {
            crate::include::nodes::primnodes::attrno_index(var.varattno)
        }
        _ => None,
    }
}

fn strip_casts(expr: &Expr) -> &Expr {
    match expr {
        Expr::Func(func)
            if matches!(
                func.implementation,
                crate::include::nodes::primnodes::ScalarFunctionImpl::Builtin(
                    BuiltinScalarFunction::BpcharToText
                )
            ) && func.args.len() == 1 =>
        {
            strip_casts(&func.args[0])
        }
        Expr::Cast(inner, _) | Expr::Collate { expr: inner, .. } => strip_casts(inner),
        other => other,
    }
}

fn const_argument(expr: &Expr) -> Option<Value> {
    match expr {
        Expr::Const(value) => Some(value.clone()),
        Expr::Cast(inner, ty) => {
            const_argument(inner).and_then(|value| cast_value(value, *ty).ok())
        }
        _ => None,
    }
}

fn simple_index_column(index: &BoundIndexRelation, index_pos: usize) -> Option<usize> {
    let attnum = *index.index_meta.indkey.get(index_pos)?;
    (attnum > 0).then_some((attnum - 1) as usize)
}

fn index_expression_position(index: &BoundIndexRelation, index_pos: usize) -> Option<usize> {
    if *index.index_meta.indkey.get(index_pos)? != 0 {
        return None;
    }
    Some(
        index
            .index_meta
            .indkey
            .iter()
            .take(index_pos)
            .filter(|attnum| **attnum == 0)
            .count(),
    )
}

fn index_key_matches_qual(
    index: &BoundIndexRelation,
    index_pos: usize,
    qual: &IndexableQual,
) -> bool {
    if let Some(column) = simple_index_column(index, index_pos) {
        return qual.column == Some(column);
    }
    let Some(expr_pos) = index_expression_position(index, index_pos) else {
        return false;
    };
    index
        .index_exprs
        .get(expr_pos)
        .is_some_and(|index_expr| strip_casts(index_expr) == strip_casts(&qual.key_expr))
}

fn value_type_oid(value: &Value) -> Option<u32> {
    value.sql_type_hint().map(sql_type_oid)
}

fn operator_commutator_oid(operator_oid: u32) -> Option<u32> {
    bootstrap_pg_operator_rows()
        .into_iter()
        .find(|row| row.oid == operator_oid)
        .and_then(|row| (row.oprcom != 0).then_some(row.oprcom))
}

fn commuted_builtin_function(func: BuiltinScalarFunction) -> Option<BuiltinScalarFunction> {
    Some(match func {
        BuiltinScalarFunction::GeoLeft => BuiltinScalarFunction::GeoRight,
        BuiltinScalarFunction::GeoRight => BuiltinScalarFunction::GeoLeft,
        BuiltinScalarFunction::GeoOverLeft => BuiltinScalarFunction::GeoOverRight,
        BuiltinScalarFunction::GeoOverRight => BuiltinScalarFunction::GeoOverLeft,
        BuiltinScalarFunction::GeoOverlap => BuiltinScalarFunction::GeoOverlap,
        BuiltinScalarFunction::GeoSame => BuiltinScalarFunction::GeoSame,
        BuiltinScalarFunction::GeoContains => BuiltinScalarFunction::GeoContainedBy,
        BuiltinScalarFunction::GeoContainedBy => BuiltinScalarFunction::GeoContains,
        BuiltinScalarFunction::GeoOverBelow => BuiltinScalarFunction::GeoOverAbove,
        BuiltinScalarFunction::GeoOverAbove => BuiltinScalarFunction::GeoOverBelow,
        BuiltinScalarFunction::GeoBelow => BuiltinScalarFunction::GeoAbove,
        BuiltinScalarFunction::GeoAbove => BuiltinScalarFunction::GeoBelow,
        BuiltinScalarFunction::RangeStrictLeft => BuiltinScalarFunction::RangeStrictRight,
        BuiltinScalarFunction::RangeStrictRight => BuiltinScalarFunction::RangeStrictLeft,
        BuiltinScalarFunction::RangeOverLeft => BuiltinScalarFunction::RangeOverRight,
        BuiltinScalarFunction::RangeOverRight => BuiltinScalarFunction::RangeOverLeft,
        BuiltinScalarFunction::RangeOverlap => BuiltinScalarFunction::RangeOverlap,
        BuiltinScalarFunction::RangeAdjacent => BuiltinScalarFunction::RangeAdjacent,
        BuiltinScalarFunction::RangeContains => BuiltinScalarFunction::RangeContainedBy,
        BuiltinScalarFunction::RangeContainedBy => BuiltinScalarFunction::RangeContains,
        BuiltinScalarFunction::NetworkSubnet => BuiltinScalarFunction::NetworkSupernet,
        BuiltinScalarFunction::NetworkSubnetEq => BuiltinScalarFunction::NetworkSupernetEq,
        BuiltinScalarFunction::NetworkSupernet => BuiltinScalarFunction::NetworkSubnet,
        BuiltinScalarFunction::NetworkSupernetEq => BuiltinScalarFunction::NetworkSubnetEq,
        BuiltinScalarFunction::NetworkOverlap => BuiltinScalarFunction::NetworkOverlap,
        _ => return None,
    })
}

fn commuted_op_expr_kind(kind: OpExprKind) -> Option<OpExprKind> {
    Some(match kind {
        OpExprKind::Eq => OpExprKind::Eq,
        OpExprKind::Lt => OpExprKind::Gt,
        OpExprKind::LtEq => OpExprKind::GtEq,
        OpExprKind::Gt => OpExprKind::Lt,
        OpExprKind::GtEq => OpExprKind::LtEq,
        _ => return None,
    })
}

fn btree_builtin_strategy(kind: OpExprKind) -> Option<u16> {
    Some(match kind {
        OpExprKind::Lt => 1,
        OpExprKind::LtEq => 2,
        OpExprKind::Eq => 3,
        OpExprKind::GtEq => 4,
        OpExprKind::Gt => 5,
        _ => return None,
    })
}

fn commuted_function_proc_oid(funcid: u32) -> Option<u32> {
    let builtin = builtin_scalar_function_for_proc_oid(funcid)?;
    let commuted = commuted_builtin_function(builtin)?;
    proc_oid_for_builtin_scalar_function(commuted)
}

fn gist_builtin_strategy(proc_oid: u32, argument: &Value) -> Option<u16> {
    let builtin = builtin_scalar_function_for_proc_oid(proc_oid)?;
    Some(match builtin {
        BuiltinScalarFunction::GeoLeft => 1,
        BuiltinScalarFunction::GeoOverLeft => 2,
        BuiltinScalarFunction::GeoOverlap => 3,
        BuiltinScalarFunction::GeoOverRight => 4,
        BuiltinScalarFunction::GeoRight => 5,
        BuiltinScalarFunction::GeoSame => 6,
        BuiltinScalarFunction::GeoContains => 7,
        BuiltinScalarFunction::GeoContainedBy => 8,
        BuiltinScalarFunction::GeoOverBelow => 9,
        BuiltinScalarFunction::GeoBelow => 10,
        BuiltinScalarFunction::GeoAbove => 11,
        BuiltinScalarFunction::GeoOverAbove => 12,
        BuiltinScalarFunction::RangeStrictLeft => 1,
        BuiltinScalarFunction::RangeOverLeft => 2,
        BuiltinScalarFunction::RangeOverlap => 3,
        BuiltinScalarFunction::RangeOverRight => 4,
        BuiltinScalarFunction::RangeStrictRight => 5,
        BuiltinScalarFunction::RangeAdjacent => 6,
        BuiltinScalarFunction::RangeContains => {
            if matches!(argument, Value::Range(_)) {
                7
            } else {
                16
            }
        }
        BuiltinScalarFunction::RangeContainedBy => 8,
        BuiltinScalarFunction::NetworkSubnet => 1,
        BuiltinScalarFunction::NetworkSubnetEq => 2,
        BuiltinScalarFunction::NetworkSupernet => 3,
        BuiltinScalarFunction::NetworkSupernetEq => 4,
        BuiltinScalarFunction::NetworkOverlap => 5,
        _ => return None,
    })
}

fn qual_strategy(
    index: &BoundIndexRelation,
    index_pos: usize,
    qual: &IndexableQual,
) -> Option<u16> {
    match qual.lookup {
        IndexStrategyLookup::Operator { oid, kind } => index
            .index_meta
            .amop_strategy_for_operator(&index.desc, index_pos, oid, value_type_oid(&qual.argument))
            .or_else(|| {
                (index.index_meta.am_oid == BTREE_AM_OID)
                    .then(|| btree_builtin_strategy(kind))
                    .flatten()
                    .or_else(|| {
                        (index.index_meta.am_oid == HASH_AM_OID && kind == OpExprKind::Eq)
                            .then_some(1)
                    })
            }),
        IndexStrategyLookup::Proc(proc_oid) => index
            .index_meta
            .amop_strategy_for_proc(
                &index.desc,
                index_pos,
                proc_oid,
                value_type_oid(&qual.argument),
            )
            .or_else(|| {
                (index.index_meta.am_oid == GIST_AM_OID || index.index_meta.am_oid == SPGIST_AM_OID)
                    .then(|| gist_builtin_strategy(proc_oid, &qual.argument))
                    .flatten()
            }),
    }
}

fn build_btree_scan_keys(
    index: &BoundIndexRelation,
    parsed_quals: &[IndexableQual],
) -> (Vec<crate::include::access::scankey::ScanKeyData>, usize) {
    let mut used = vec![false; parsed_quals.len()];
    let mut keys = Vec::new();
    let mut equality_prefix = 0usize;

    for index_pos in 0..index.index_meta.indkey.len() {
        let Some(column) = simple_index_column(index, index_pos) else {
            break;
        };
        if let Some((qual_idx, strategy, argument)) =
            parsed_quals.iter().enumerate().find_map(|(idx, qual)| {
                if used[idx] || qual.column != Some(column) {
                    return None;
                }
                let strategy = qual_strategy(index, index_pos, qual)?;
                (strategy == 3).then_some((idx, strategy, qual.argument.clone()))
            })
        {
            used[qual_idx] = true;
            equality_prefix += 1;
            keys.push(crate::include::access::scankey::ScanKeyData {
                attribute_number: (index_pos + 1) as i16,
                strategy,
                argument,
            });
            continue;
        }
        if let Some((qual_idx, strategy, argument)) =
            parsed_quals.iter().enumerate().find_map(|(idx, qual)| {
                if used[idx] || qual.column != Some(column) {
                    return None;
                }
                let strategy = qual_strategy(index, index_pos, qual)?;
                Some((idx, strategy, qual.argument.clone()))
            })
        {
            used[qual_idx] = true;
            keys.push(crate::include::access::scankey::ScanKeyData {
                attribute_number: (index_pos + 1) as i16,
                strategy,
                argument,
            });
        }
        break;
    }

    (keys, equality_prefix)
}

fn build_gist_scan_keys(
    index: &BoundIndexRelation,
    parsed_quals: &[IndexableQual],
) -> Vec<crate::include::access::scankey::ScanKeyData> {
    parsed_quals
        .iter()
        .filter_map(|qual| {
            let (index_pos, strategy) =
                (0..index.index_meta.indkey.len()).find_map(|index_pos| {
                    (index_key_matches_qual(index, index_pos, qual))
                        .then(|| qual_strategy(index, index_pos, qual))
                        .flatten()
                        .map(|strategy| (index_pos, strategy))
                })?;
            Some(crate::include::access::scankey::ScanKeyData {
                attribute_number: (index_pos + 1) as i16,
                strategy,
                argument: qual.argument.clone(),
            })
        })
        .collect()
}

fn build_hash_scan_keys(
    index: &BoundIndexRelation,
    parsed_quals: &[IndexableQual],
) -> Vec<crate::include::access::scankey::ScanKeyData> {
    if index.index_meta.indkey.len() != 1 {
        return Vec::new();
    }
    parsed_quals
        .iter()
        .find_map(|qual| {
            if !index_key_matches_qual(index, 0, qual) {
                return None;
            }
            let strategy = qual_strategy(index, 0, qual)?;
            (strategy == 1).then_some(crate::include::access::scankey::ScanKeyData {
                attribute_number: 1,
                strategy,
                argument: qual.argument.clone(),
            })
        })
        .into_iter()
        .collect()
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
            && !index.index_meta.indisexclusion
            && !index.index_meta.indkey.is_empty()
            && predicate_implies_index_predicate(filter, index.index_predicate.as_ref())
    }) {
        let (keys, _equality_prefix, removes_order) = match index.index_meta.am_oid {
            BTREE_AM_OID => {
                let (keys, equality_prefix) = build_btree_scan_keys(index, &parsed_quals);
                let removes_order = order_items
                    .and_then(|items| index_order_match(items, index, equality_prefix))
                    .is_some();
                (keys, equality_prefix, removes_order)
            }
            GIST_AM_OID | SPGIST_AM_OID => (build_gist_scan_keys(index, &parsed_quals), 0, false),
            HASH_AM_OID => (build_hash_scan_keys(index, &parsed_quals), 0, false),
            _ => continue,
        };

        let usable_prefix = keys.len();
        let has_qual = usable_prefix > 0;
        if !has_qual && !removes_order {
            continue;
        }
        let chosen = ChosenIndexPath {
            index: index.clone(),
            keys,
            has_qual,
            usable_prefix,
            removes_order,
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
    if index.index_meta.am_oid != BTREE_AM_OID || items.is_empty() {
        return None;
    }
    let mut direction = None;
    let mut matched = 0usize;
    for (idx, item) in items.iter().enumerate() {
        let Some(column) = simple_column_index(&item.expr) else {
            break;
        };
        let Some(index_column) = simple_index_column(index, equality_prefix + idx) else {
            break;
        };
        if index_column != column {
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
    fn mk(key_expr: &Expr, lookup: IndexStrategyLookup, argument: Value) -> Option<IndexableQual> {
        Some(IndexableQual {
            column: simple_column_index(key_expr),
            key_expr: strip_casts(key_expr).clone(),
            lookup,
            argument,
        })
    }

    match strip_casts(expr) {
        Expr::Op(op) if op.args.len() == 2 => {
            let left = strip_casts(&op.args[0]);
            let right = &op.args[1];
            if let Some(value) = const_argument(right) {
                return mk(
                    left,
                    IndexStrategyLookup::Operator {
                        oid: op.opno,
                        kind: op.op,
                    },
                    value,
                );
            }
            if let Some(value) = const_argument(&op.args[0]) {
                return mk(
                    strip_casts(&op.args[1]),
                    IndexStrategyLookup::Operator {
                        oid: operator_commutator_oid(op.opno).unwrap_or(0),
                        kind: commuted_op_expr_kind(op.op)?,
                    },
                    value,
                );
            }
            None
        }
        Expr::Func(func) if func.args.len() == 2 => {
            let left = strip_casts(&func.args[0]);
            let right = &func.args[1];
            if let Some(value) = const_argument(right) {
                return mk(left, IndexStrategyLookup::Proc(func.funcid), value);
            }
            if let Some(value) = const_argument(&func.args[0]) {
                return mk(
                    strip_casts(&func.args[1]),
                    IndexStrategyLookup::Proc(commuted_function_proc_oid(func.funcid)?),
                    value,
                );
            }
            None
        }
        _ => None,
    }
}

pub(super) fn bind_order_by_items(
    items: &[OrderByItem],
    targets: &[TargetEntry],
    catalog: &dyn CatalogLookup,
    bind_expr: impl Fn(&SqlExpr) -> Result<Expr, ParseError>,
) -> Result<Vec<crate::backend::executor::OrderByEntry>, ParseError> {
    items
        .iter()
        .map(|item| {
            let (raw_expr, explicit_collation) = match &item.expr {
                SqlExpr::Collate { expr, collation } => (expr.as_ref(), Some(collation.as_str())),
                other => (other, None),
            };
            let (expr, ressortgroupref) = match raw_expr {
                SqlExpr::IntegerLiteral(value) => {
                    if let Ok(ordinal) = value.parse::<usize>() {
                        if ordinal > 0 && ordinal <= targets.len() {
                            let target = &targets[ordinal - 1];
                            (
                                target.expr.clone(),
                                if target.ressortgroupref != 0 {
                                    target.ressortgroupref
                                } else {
                                    target.resno
                                },
                            )
                        } else {
                            return Err(ParseError::UnexpectedToken {
                                expected: "ORDER BY position in select list",
                                actual: value.clone(),
                            });
                        }
                    } else {
                        (bind_expr(raw_expr)?, 0)
                    }
                }
                SqlExpr::Column(name) => {
                    if let Some(target) = targets
                        .iter()
                        .find(|target| target.name.eq_ignore_ascii_case(name))
                    {
                        (
                            target.expr.clone(),
                            if target.ressortgroupref != 0 {
                                target.ressortgroupref
                            } else {
                                target.resno
                            },
                        )
                    } else {
                        (bind_expr(raw_expr)?, 0)
                    }
                }
                _ => (bind_expr(raw_expr)?, 0),
            };
            let expr_type = expr_sql_type_hint(&expr).unwrap_or(SqlType::new(SqlTypeKind::Text));
            let expr = match explicit_collation {
                Some(collation) => bind_explicit_collation(expr, expr_type, collation, catalog)?,
                None => expr,
            };
            build_bound_order_by_entry(item, expr, ressortgroupref, catalog)
        })
        .collect()
}
