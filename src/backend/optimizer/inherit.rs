use crate::backend::parser::CatalogLookup;
use crate::include::nodes::datum::Value;
use crate::include::nodes::parsenodes::{RangeTblEntry, RangeTblEntryKind};
use crate::include::nodes::pathnodes::{
    AppendRelInfo, PlannerInfo, RelOptInfo, RelOptKind, RestrictInfo,
};
use crate::include::nodes::primnodes::{Expr, ExprArraySubscript, RelationDesc, Var, user_attrno};

pub(super) fn expand_inherited_rtentries(root: &mut PlannerInfo, catalog: &dyn CatalogLookup) {
    let original_len = root.parse.rtable.len();
    for parent_rtindex in 1..=original_len {
        let Some(parent_rte) = root.parse.rtable.get(parent_rtindex - 1).cloned() else {
            continue;
        };
        let RangeTblEntryKind::Relation {
            relation_oid,
            relkind,
            ..
        } = parent_rte.kind
        else {
            continue;
        };
        if !parent_rte.inh || relkind != 'r' {
            continue;
        }

        let inheritors = catalog.find_all_inheritors(relation_oid);
        if inheritors.len() <= 1 {
            continue;
        }
        let parent_restrictinfo = root
            .simple_rel_array
            .get(parent_rtindex)
            .and_then(Option::as_ref)
            .map(|rel| rel.baserestrictinfo.clone())
            .unwrap_or_default();

        for child_oid in inheritors.into_iter().filter(|oid| *oid != relation_oid) {
            let Some(child) = catalog.relation_by_oid(child_oid) else {
                continue;
            };
            let child_rtindex = root.parse.rtable.len() + 1;
            let translated_vars =
                translate_parent_vars_to_child(&parent_rte.desc, child_rtindex, &child.desc);
            let child_rte = RangeTblEntry {
                alias: None,
                desc: child.desc.clone(),
                inh: false,
                security_quals: Vec::new(),
                kind: RangeTblEntryKind::Relation {
                    rel: child.rel,
                    relation_oid: child.relation_oid,
                    relkind: child.relkind,
                    toast: child.toast,
                },
            };
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
                    .map(|restrict| RestrictInfo {
                        clause: translate_append_rel_expr(
                            restrict.clause.clone(),
                            &AppendRelInfo {
                                parent_relid: parent_rtindex,
                                child_relid: child_rtindex,
                                translated_vars: translated_vars.clone(),
                            },
                        ),
                        required_relids: vec![child_rtindex],
                        is_pushed_down: restrict.is_pushed_down,
                    })
                    .collect();
            }
        }
    }
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
        } => Expr::Like {
            expr: Box::new(translate_append_rel_expr(*expr, info)),
            pattern: Box::new(translate_append_rel_expr(*pattern, info)),
            escape: escape.map(|expr| Box::new(translate_append_rel_expr(*expr, info))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(translate_append_rel_expr(*expr, info)),
            pattern: Box::new(translate_append_rel_expr(*pattern, info)),
            escape: escape.map(|expr| Box::new(translate_append_rel_expr(*expr, info))),
            negated,
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
    child_rtindex: usize,
    child_desc: &RelationDesc,
) -> Vec<Expr> {
    parent_desc
        .columns
        .iter()
        .map(|parent_column| {
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
        })
        .collect()
}
