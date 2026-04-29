use std::cmp::Ordering;

use crate::backend::executor::compare_order_values;
use crate::backend::parser::CatalogLookup;
use crate::backend::parser::{
    PartitionBoundSpec, PartitionRangeDatumValue, PartitionStrategy, partition_value_to_value,
};
use crate::include::catalog::PgInheritsRow;
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{RangeTblEntry, RangeTblEntryKind, RangeTblEref};
use crate::include::nodes::pathnodes::{
    AppendRelInfo, PartitionInfo, PartitionMember, PlannerInfo, PlannerPartitionChildBound,
    RelOptInfo, RelOptKind,
};
use crate::include::nodes::primnodes::{
    ColumnDesc, Expr, ExprArraySubscript, RelationDesc, Var, user_attrno,
};

use super::joininfo;
use super::partition_cache;

pub(super) fn expand_inherited_rtentries(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) {
    let mut parent_rtindex = 1;
    while parent_rtindex <= root.parse.rtable.len() {
        let Some(parent_rte) = root.parse.rtable.get(parent_rtindex - 1).cloned() else {
            parent_rtindex += 1;
            continue;
        };
        let RangeTblEntryKind::Relation {
            relation_oid,
            relkind,
            tablesample,
            ..
        } = parent_rte.kind.clone()
        else {
            parent_rtindex += 1;
            continue;
        };
        if !parent_rte.inh || !matches!(relkind, 'r' | 'p') {
            parent_rtindex += 1;
            continue;
        }

        let child_rows = if relkind == 'p' {
            ordered_partition_children(root, catalog, relation_oid)
        } else {
            catalog
                .find_all_inheritors(relation_oid)
                .into_iter()
                .filter(|oid| *oid != relation_oid)
                .map(|oid| PgInheritsRow {
                    inhrelid: oid,
                    inhparent: relation_oid,
                    inhseqno: 1,
                    inhdetachpending: false,
                })
                .map(|row| PlannerPartitionChildBound { row, bound: None })
                .collect()
        };
        if child_rows.is_empty() {
            parent_rtindex += 1;
            continue;
        }
        let parent_restrictinfo = root
            .simple_rel_array
            .get(parent_rtindex)
            .and_then(Option::as_ref)
            .map(|rel| rel.baserestrictinfo.clone())
            .unwrap_or_default();
        let mut partition_members = Vec::new();
        let parent_alias = parent_rte
            .alias
            .clone()
            .or_else(|| {
                catalog
                    .class_row_by_oid(relation_oid)
                    .map(|row| row.relname)
            })
            .unwrap_or_else(|| relation_oid.to_string());
        let mut child_alias_index = 1usize;
        let parent_source_desc = catalog
            .relation_by_oid(relation_oid)
            .map(|relation| relation.desc);

        for child_row in child_rows {
            let child_oid = child_row.row.inhrelid;
            let Some(child) = catalog.relation_by_oid(child_oid) else {
                continue;
            };
            if relkind == 'p' && !matches!(child.relkind, 'r' | 'p') {
                continue;
            }
            let child_rtindex = root.parse.rtable.len() + 1;
            let translated_vars = translate_parent_vars_to_child(
                &parent_rte.desc,
                parent_source_desc.as_ref(),
                child_rtindex,
                &child.desc,
            );
            let child_alias = if child_alias_index == 1 {
                parent_alias.clone()
            } else {
                format!("{parent_alias}_{}", child_alias_index - 1)
            };
            let child_rte = RangeTblEntry {
                alias: Some(child_alias.clone()),
                alias_preserves_source_names: false,
                eref: RangeTblEref {
                    aliasname: child_alias,
                    colnames: child
                        .desc
                        .columns
                        .iter()
                        .map(|column| column.name.clone())
                        .collect(),
                },
                desc: child.desc.clone(),
                inh: false,
                security_quals: Vec::new(),
                permission: None,
                kind: RangeTblEntryKind::Relation {
                    rel: child.rel,
                    relation_oid: child.relation_oid,
                    relkind: child.relkind,
                    relispopulated: child.relispopulated,
                    toast: child.toast,
                    tablesample: tablesample.clone(),
                },
            };
            let mut child_rte = child_rte;
            child_rte.inh = relkind == 'p' && child.relkind == 'p';
            child_alias_index += 1;
            root.parse.rtable.push(child_rte.clone());
            root.simple_rel_array
                .push(Some(RelOptInfo::from_rte(child_rtindex, &child_rte)));
            root.append_rel_infos.push(Some(AppendRelInfo {
                parent_relid: parent_rtindex,
                child_relid: child_rtindex,
                translated_vars: translated_vars.clone(),
            }));
            if let Some(rel) = root
                .simple_rel_array
                .get_mut(child_rtindex)
                .and_then(Option::as_mut)
            {
                rel.reloptkind = RelOptKind::OtherMemberRel;
                rel.baserestrictinfo = parent_restrictinfo
                    .iter()
                    .map(|restrict| {
                        joininfo::translated_restrict_info(
                            translate_append_rel_expr(
                                restrict.clause.clone(),
                                &AppendRelInfo {
                                    parent_relid: parent_rtindex,
                                    child_relid: child_rtindex,
                                    translated_vars: translated_vars.clone(),
                                },
                            ),
                            restrict,
                        )
                    })
                    .collect();
            }
            if relkind == 'p' {
                partition_members.push(PartitionMember {
                    relids: vec![child_rtindex],
                    bound: child_row.bound.clone(),
                });
            }
        }
        if relkind == 'p'
            && let Some(partition_info) = partition_info_for_parent(
                root,
                catalog,
                relation_oid,
                parent_rtindex,
                &parent_rte,
                partition_members,
            )
            && let Some(parent_rel) = root
                .simple_rel_array
                .get_mut(parent_rtindex)
                .and_then(Option::as_mut)
        {
            parent_rel.consider_partitionwise_join = root.config.enable_partitionwise_join;
            parent_rel.partition_info = Some(partition_info);
        }
        parent_rtindex += 1;
    }
}

fn ordered_partition_children(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    parent_oid: u32,
) -> Vec<PlannerPartitionChildBound> {
    let strategy =
        partition_cache::partition_spec(root, catalog, parent_oid).map(|spec| spec.strategy);
    let mut children = partition_cache::partition_child_bounds(root, catalog, parent_oid);
    children.sort_by(|left, right| {
        compare_partition_children(strategy, left, right)
            .then_with(|| left.row.inhseqno.cmp(&right.row.inhseqno))
            .then_with(|| left.row.inhrelid.cmp(&right.row.inhrelid))
    });
    children
}

fn compare_partition_children(
    strategy: Option<PartitionStrategy>,
    left: &PlannerPartitionChildBound,
    right: &PlannerPartitionChildBound,
) -> Ordering {
    match (strategy, left.bound.as_ref(), right.bound.as_ref()) {
        (_, Some(left), Some(right)) if left.is_default() || right.is_default() => {
            left.is_default().cmp(&right.is_default())
        }
        (Some(PartitionStrategy::List), Some(left), Some(right)) => {
            compare_list_bounds(left, right)
        }
        (Some(PartitionStrategy::Range), Some(left), Some(right)) => {
            compare_range_bounds(left, right)
        }
        (Some(PartitionStrategy::Hash), Some(left), Some(right)) => {
            compare_hash_bounds(left, right)
        }
        (_, Some(_), None) => Ordering::Less,
        (_, None, Some(_)) => Ordering::Greater,
        _ => Ordering::Equal,
    }
}

fn compare_list_bounds(left: &PartitionBoundSpec, right: &PartitionBoundSpec) -> Ordering {
    let (
        PartitionBoundSpec::List {
            values: left_values,
            ..
        },
        PartitionBoundSpec::List {
            values: right_values,
            ..
        },
    ) = (left, right)
    else {
        return Ordering::Equal;
    };
    left_values
        .iter()
        .zip(right_values.iter())
        .map(compare_list_value_for_ordering)
        .find(|ordering| *ordering != Ordering::Equal)
        .unwrap_or_else(|| left_values.len().cmp(&right_values.len()))
}

fn compare_list_value_for_ordering(
    left: (
        &crate::backend::parser::SerializedPartitionValue,
        &crate::backend::parser::SerializedPartitionValue,
    ),
) -> Ordering {
    match left {
        (
            crate::backend::parser::SerializedPartitionValue::Null,
            crate::backend::parser::SerializedPartitionValue::Null,
        ) => Ordering::Equal,
        (crate::backend::parser::SerializedPartitionValue::Null, _) => Ordering::Greater,
        (_, crate::backend::parser::SerializedPartitionValue::Null) => Ordering::Less,
        (left, right) => compare_order_values(
            &partition_value_to_value(left),
            &partition_value_to_value(right),
            None,
            Some(true),
            false,
        )
        .unwrap_or(Ordering::Equal),
    }
}

fn compare_range_bounds(left: &PartitionBoundSpec, right: &PartitionBoundSpec) -> Ordering {
    let (
        PartitionBoundSpec::Range {
            from: left_from,
            to: left_to,
            ..
        },
        PartitionBoundSpec::Range {
            from: right_from,
            to: right_to,
            ..
        },
    ) = (left, right)
    else {
        return Ordering::Equal;
    };
    compare_range_datums(left_from, right_from)
        .then_with(|| compare_range_datums(left_to, right_to))
}

fn compare_range_datums(
    left: &[PartitionRangeDatumValue],
    right: &[PartitionRangeDatumValue],
) -> Ordering {
    left.iter()
        .zip(right.iter())
        .map(compare_range_datum)
        .find(|ordering| *ordering != Ordering::Equal)
        .unwrap_or_else(|| left.len().cmp(&right.len()))
}

fn compare_range_datum(left: (&PartitionRangeDatumValue, &PartitionRangeDatumValue)) -> Ordering {
    match left {
        (PartitionRangeDatumValue::MinValue, PartitionRangeDatumValue::MinValue)
        | (PartitionRangeDatumValue::MaxValue, PartitionRangeDatumValue::MaxValue) => {
            Ordering::Equal
        }
        (PartitionRangeDatumValue::MinValue, _) | (_, PartitionRangeDatumValue::MaxValue) => {
            Ordering::Less
        }
        (PartitionRangeDatumValue::MaxValue, _) | (_, PartitionRangeDatumValue::MinValue) => {
            Ordering::Greater
        }
        (PartitionRangeDatumValue::Value(left), PartitionRangeDatumValue::Value(right)) => {
            compare_order_values(
                &partition_value_to_value(left),
                &partition_value_to_value(right),
                None,
                Some(true),
                false,
            )
            .unwrap_or(Ordering::Equal)
        }
    }
}

fn compare_hash_bounds(left: &PartitionBoundSpec, right: &PartitionBoundSpec) -> Ordering {
    match (left, right) {
        (
            PartitionBoundSpec::Hash {
                modulus: left_modulus,
                remainder: left_remainder,
            },
            PartitionBoundSpec::Hash {
                modulus: right_modulus,
                remainder: right_remainder,
            },
        ) => left_modulus
            .cmp(right_modulus)
            .then_with(|| left_remainder.cmp(right_remainder)),
        _ => Ordering::Equal,
    }
}

fn partition_info_for_parent(
    root: &PlannerInfo,
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    parent_rtindex: usize,
    parent_rte: &RangeTblEntry,
    members: Vec<PartitionMember>,
) -> Option<PartitionInfo> {
    let spec = partition_cache::partition_spec(root, catalog, relation_oid)?;
    let parent_translation = AppendRelInfo {
        parent_relid: 1,
        child_relid: parent_rtindex,
        translated_vars: parent_rte
            .desc
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                Expr::Var(Var {
                    varno: parent_rtindex,
                    varattno: user_attrno(index),
                    varlevelsup: 0,
                    vartype: column.sql_type,
                })
            })
            .collect(),
    };
    let key_exprs = spec
        .key_exprs
        .iter()
        .cloned()
        .map(|expr| translate_append_rel_expr(expr, &parent_translation))
        .collect::<Vec<_>>();
    if key_exprs.len() != spec.partattrs.len() || members.is_empty() {
        return None;
    }
    Some(PartitionInfo {
        strategy: spec.strategy,
        partattrs: spec.partattrs,
        partclass: spec.partclass,
        partcollation: spec.partcollation,
        key_exprs,
        members,
    })
}

pub(super) fn translate_append_rel_expr(expr: Expr, info: &AppendRelInfo) -> Expr {
    match expr {
        Expr::Var(var) if var.varlevelsup == 0 && var.varno == info.parent_relid => info
            .translated_vars
            .get(crate::include::nodes::primnodes::attrno_index(var.varattno).unwrap_or(usize::MAX))
            .cloned()
            .unwrap_or(Expr::Var(var)),
        Expr::Op(op) => Expr::Op(Box::new(crate::include::nodes::primnodes::OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| translate_append_rel_expr(arg, info))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(crate::include::nodes::primnodes::BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| translate_append_rel_expr(arg, info))
                .collect(),
            ..*bool_expr
        })),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(translate_append_rel_expr(*arg, info))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: translate_append_rel_expr(arm.expr, info),
                    result: translate_append_rel_expr(arm.result, info),
                })
                .collect(),
            defresult: Box::new(translate_append_rel_expr(*case_expr.defresult, info)),
            ..*case_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(crate::include::nodes::primnodes::FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| translate_append_rel_expr(arg, info))
                .collect(),
            ..*func
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(
            crate::include::nodes::primnodes::ScalarArrayOpExpr {
                left: Box::new(translate_append_rel_expr(*saop.left, info)),
                right: Box::new(translate_append_rel_expr(*saop.right, info)),
                ..*saop
            },
        )),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(translate_append_rel_expr(*inner, info)), ty),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(translate_append_rel_expr(*expr, info)),
            pattern: Box::new(translate_append_rel_expr(*pattern, info)),
            escape: escape.map(|expr| Box::new(translate_append_rel_expr(*expr, info))),
            case_insensitive,
            negated,
            collation_oid,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
            collation_oid,
        } => Expr::Similar {
            expr: Box::new(translate_append_rel_expr(*expr, info)),
            pattern: Box::new(translate_append_rel_expr(*pattern, info)),
            escape: escape.map(|expr| Box::new(translate_append_rel_expr(*expr, info))),
            negated,
            collation_oid,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(translate_append_rel_expr(*inner, info))),
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(translate_append_rel_expr(*inner, info)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(translate_append_rel_expr(*left, info)),
            Box::new(translate_append_rel_expr(*right, info)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(translate_append_rel_expr(*left, info)),
            Box::new(translate_append_rel_expr(*right, info)),
        ),
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(translate_append_rel_expr(*left, info)),
            Box::new(translate_append_rel_expr(*right, info)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|element| translate_append_rel_expr(element, info))
                .collect(),
            array_type,
        },
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(translate_append_rel_expr(*array, info)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| translate_append_rel_expr(expr, info)),
                    upper: subscript
                        .upper
                        .map(|expr| translate_append_rel_expr(expr, info)),
                })
                .collect(),
        },
        other => other,
    }
}

pub(super) fn append_child_rtindexes(root: &PlannerInfo, parent_rtindex: usize) -> Vec<usize> {
    root.append_rel_infos
        .iter()
        .enumerate()
        .skip(1)
        .filter_map(|(rtindex, info)| {
            info.as_ref()
                .filter(|info| info.parent_relid == parent_rtindex)
                .map(|_| rtindex)
        })
        .collect()
}

pub(super) fn append_translation(
    root: &PlannerInfo,
    child_rtindex: usize,
) -> Option<&AppendRelInfo> {
    root.append_rel_infos
        .get(child_rtindex)
        .and_then(Option::as_ref)
}

fn translate_parent_vars_to_child(
    parent_desc: &RelationDesc,
    parent_source_desc: Option<&RelationDesc>,
    child_rtindex: usize,
    child_desc: &RelationDesc,
) -> Vec<Expr> {
    parent_desc
        .columns
        .iter()
        .enumerate()
        .map(|(parent_index, parent_column)| {
            let lookup_column = parent_source_desc
                .and_then(|desc| desc.columns.get(parent_index))
                .filter(|source_column| source_column.sql_type == parent_column.sql_type)
                .unwrap_or(parent_column);
            translate_parent_column_to_child(lookup_column, child_rtindex, child_desc)
        })
        .collect()
}

fn translate_parent_column_to_child(
    parent_column: &ColumnDesc,
    child_rtindex: usize,
    child_desc: &RelationDesc,
) -> Expr {
    child_desc
        .columns
        .iter()
        .enumerate()
        .find(|(_, child_column)| {
            !child_column.dropped
                && child_column.name.eq_ignore_ascii_case(&parent_column.name)
                && child_column.sql_type == parent_column.sql_type
        })
        .map(|(index, child_column)| {
            Expr::Var(Var {
                varno: child_rtindex,
                varattno: user_attrno(index),
                varlevelsup: 0,
                vartype: child_column.sql_type,
            })
        })
        .unwrap_or(Expr::Const(Value::Null))
}
