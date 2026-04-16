use super::*;
use crate::include::executor::execdesc::CommandType;
use crate::include::nodes::parsenodes::{JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind};
use crate::include::nodes::primnodes::{
    Aggref, BoolExpr, FuncExpr, OpExpr, ScalarArrayOpExpr, SubLink, user_attrno,
};
use crate::include::nodes::primnodes::{ExprArraySubscript, JoinType, Var};

#[derive(Debug, Clone)]
pub(super) struct AnalyzedFrom {
    pub(super) rtable: Vec<RangeTblEntry>,
    pub(super) jointree: Option<JoinTreeNode>,
    pub(super) output_columns: Vec<QueryColumn>,
    pub(super) output_exprs: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub(super) struct JoinAliasInfo {
    pub(super) output_columns: Vec<QueryColumn>,
    pub(super) output_exprs: Vec<Expr>,
    pub(super) joinmergedcols: usize,
    pub(super) joinleftcols: Vec<usize>,
    pub(super) joinrightcols: Vec<usize>,
}

pub(crate) fn analyze_select_query_with_outer(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(Query, BoundScope), ParseError> {
    super::bind_select_query_with_outer(
        stmt,
        catalog,
        outer_scopes,
        grouped_outer,
        outer_ctes,
        expanded_views,
    )
}

pub(super) fn analyze_values_query_with_outer(
    stmt: &ValuesStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(Query, BoundScope), ParseError> {
    super::bind_values_query_with_outer(
        stmt,
        catalog,
        outer_scopes,
        grouped_outer,
        outer_ctes,
        expanded_views,
    )
}

impl AnalyzedFrom {
    pub(super) fn result() -> Self {
        Self {
            rtable: Vec::new(),
            jointree: None,
            output_columns: Vec::new(),
            output_exprs: Vec::new(),
        }
    }

    pub(super) fn relation(
        relation_name: String,
        rel: crate::RelFileLocator,
        relation_oid: u32,
        relkind: char,
        toast: Option<ToastRelationRef>,
        inh: bool,
        desc: RelationDesc,
    ) -> Self {
        let output_columns = desc
            .columns
            .iter()
            .map(|column| QueryColumn {
                name: column.name.clone(),
                sql_type: column.sql_type,
            })
            .collect::<Vec<_>>();
        Self {
            rtable: vec![RangeTblEntry {
                alias: Some(relation_name),
                desc,
                inh,
                kind: RangeTblEntryKind::Relation {
                    rel,
                    relation_oid,
                    relkind,
                    toast,
                },
            }],
            jointree: Some(JoinTreeNode::RangeTblRef(1)),
            output_exprs: rte_output_exprs(1, &output_columns),
            output_columns,
        }
    }

    pub(super) fn values(rows: Vec<Vec<Expr>>, output_columns: Vec<QueryColumn>) -> Self {
        let desc = RelationDesc {
            columns: output_columns
                .iter()
                .map(|column| column_desc(column.name.clone(), column.sql_type, true))
                .collect(),
        };
        Self {
            rtable: vec![RangeTblEntry {
                alias: None,
                desc,
                inh: false,
                kind: RangeTblEntryKind::Values {
                    rows,
                    output_columns: output_columns.clone(),
                },
            }],
            jointree: Some(JoinTreeNode::RangeTblRef(1)),
            output_exprs: rte_output_exprs(1, &output_columns),
            output_columns,
        }
    }

    pub(super) fn function(call: SetReturningCall) -> Self {
        let output_columns = call.output_columns().to_vec();
        let desc = RelationDesc {
            columns: output_columns
                .iter()
                .map(|column| column_desc(column.name.clone(), column.sql_type, true))
                .collect(),
        };
        Self {
            rtable: vec![RangeTblEntry {
                alias: None,
                desc,
                inh: false,
                kind: RangeTblEntryKind::Function { call },
            }],
            jointree: Some(JoinTreeNode::RangeTblRef(1)),
            output_exprs: rte_output_exprs(1, &output_columns),
            output_columns,
        }
    }

    pub(super) fn worktable(worktable_id: usize, output_columns: Vec<QueryColumn>) -> Self {
        let desc = RelationDesc {
            columns: output_columns
                .iter()
                .map(|column| column_desc(column.name.clone(), column.sql_type, true))
                .collect(),
        };
        Self {
            rtable: vec![RangeTblEntry {
                alias: None,
                desc,
                inh: false,
                kind: RangeTblEntryKind::WorkTable { worktable_id },
            }],
            jointree: Some(JoinTreeNode::RangeTblRef(1)),
            output_exprs: rte_output_exprs(1, &output_columns),
            output_columns,
        }
    }

    pub(super) fn cte_scan(cte_id: usize, query: Query) -> Self {
        let output_columns = query.columns();
        let desc = RelationDesc {
            columns: output_columns
                .iter()
                .map(|column| column_desc(column.name.clone(), column.sql_type, true))
                .collect(),
        };
        Self {
            rtable: vec![RangeTblEntry {
                alias: None,
                desc,
                inh: false,
                kind: RangeTblEntryKind::Cte {
                    cte_id,
                    query: Box::new(query),
                },
            }],
            jointree: Some(JoinTreeNode::RangeTblRef(1)),
            output_exprs: rte_output_exprs(1, &output_columns),
            output_columns,
        }
    }

    pub(super) fn subquery(query: Query) -> Self {
        let output_columns = query.columns();
        let desc = RelationDesc {
            columns: output_columns
                .iter()
                .map(|column| column_desc(column.name.clone(), column.sql_type, true))
                .collect(),
        };
        Self {
            rtable: vec![RangeTblEntry {
                alias: None,
                desc,
                inh: false,
                kind: RangeTblEntryKind::Subquery {
                    query: Box::new(query),
                },
            }],
            jointree: Some(JoinTreeNode::RangeTblRef(1)),
            output_exprs: rte_output_exprs(1, &output_columns),
            output_columns,
        }
    }

    pub(super) fn join(
        left: Self,
        right: Self,
        kind: JoinType,
        on: Expr,
        alias_info: Option<JoinAliasInfo>,
    ) -> Self {
        let right = right.shift_rtindexes(left.rtable.len());
        let mut child_output_columns = left.output_columns.clone();
        child_output_columns.extend(right.output_columns.clone());
        let mut child_output_exprs = left.output_exprs.clone();
        child_output_exprs.extend(right.output_exprs.clone());
        let mut rtable = left.rtable;
        rtable.extend(right.rtable);
        let join_rtindex = rtable.len() + 1;
        let (output_columns, joinaliasvars, joinmergedcols, joinleftcols, joinrightcols) =
            match alias_info {
                Some(alias_info) => (
                    alias_info.output_columns,
                    alias_info
                        .output_exprs
                        .into_iter()
                        .map(|expr| rewrite_expr_columns(expr, &child_output_exprs))
                        .collect(),
                    alias_info.joinmergedcols,
                    alias_info.joinleftcols,
                    alias_info.joinrightcols,
                ),
                None => (
                    child_output_columns.clone(),
                    child_output_exprs.clone(),
                    0,
                    (1..=left.output_columns.len()).collect(),
                    (1..=right.output_columns.len()).collect(),
                ),
            };
        let output_exprs = rte_output_exprs(join_rtindex, &output_columns);
        let jointree = match (left.jointree, right.jointree) {
            (Some(left_tree), Some(right_tree)) => Some(JoinTreeNode::JoinExpr {
                left: Box::new(left_tree),
                right: Box::new(right_tree),
                kind,
                quals: rewrite_expr_columns(on, &child_output_exprs),
                rtindex: join_rtindex,
            }),
            (Some(tree), None) | (None, Some(tree)) => Some(tree),
            (None, None) => None,
        };
        let desc = RelationDesc {
            columns: output_columns
                .iter()
                .map(|column| column_desc(column.name.clone(), column.sql_type, true))
                .collect(),
        };
        rtable.push(RangeTblEntry {
            alias: None,
            desc,
            inh: false,
            kind: RangeTblEntryKind::Join {
                jointype: kind,
                joinmergedcols,
                joinaliasvars,
                joinleftcols,
                joinrightcols,
            },
        });
        Self {
            rtable,
            jointree,
            output_columns,
            output_exprs,
        }
    }

    pub(super) fn with_projection(self, targets: Vec<TargetEntry>) -> Self {
        Self::subquery(query_from_from_projection(self, targets))
    }

    pub(super) fn desc(&self) -> RelationDesc {
        RelationDesc {
            columns: self
                .output_columns
                .iter()
                .map(|column| column_desc(column.name.clone(), column.sql_type, true))
                .collect(),
        }
    }

    fn shift_rtindexes(self, offset: usize) -> Self {
        if offset == 0 {
            return self;
        }
        Self {
            rtable: self
                .rtable
                .into_iter()
                .map(|entry| shift_rte_rtindexes(entry, offset))
                .collect(),
            jointree: self
                .jointree
                .map(|node| shift_jointree_rtindexes(node, offset)),
            output_columns: self.output_columns,
            output_exprs: self
                .output_exprs
                .into_iter()
                .map(|expr| shift_expr_rtindexes(expr, offset))
                .collect(),
        }
    }
}

pub(super) fn query_from_from_projection(input: AnalyzedFrom, targets: Vec<TargetEntry>) -> Query {
    let AnalyzedFrom {
        rtable,
        jointree,
        output_columns,
        output_exprs,
    } = input;
    let target_list = normalize_target_list(if targets.is_empty() {
        identity_target_list(&output_columns, &output_exprs)
    } else {
        rewrite_target_entries(targets, &output_exprs)
    });
    Query {
        command_type: CommandType::Select,
        rtable,
        jointree,
        target_list,
        where_qual: None,
        group_by: Vec::new(),
        accumulators: Vec::new(),
        having_qual: None,
        sort_clause: Vec::new(),
        limit_count: None,
        limit_offset: 0,
        project_set: None,
        recursive_union: None,
    }
}

fn shift_jointree_rtindexes(node: JoinTreeNode, offset: usize) -> JoinTreeNode {
    match node {
        JoinTreeNode::RangeTblRef(rtindex) => JoinTreeNode::RangeTblRef(rtindex + offset),
        JoinTreeNode::JoinExpr {
            left,
            right,
            kind,
            quals,
            rtindex,
        } => JoinTreeNode::JoinExpr {
            left: Box::new(shift_jointree_rtindexes(*left, offset)),
            right: Box::new(shift_jointree_rtindexes(*right, offset)),
            kind,
            quals: shift_expr_rtindexes(quals, offset),
            rtindex: rtindex + offset,
        },
    }
}

fn shift_rte_rtindexes(entry: RangeTblEntry, offset: usize) -> RangeTblEntry {
    if offset == 0 {
        return entry;
    }
    RangeTblEntry {
        kind: match entry.kind {
            RangeTblEntryKind::Join {
                jointype,
                joinmergedcols,
                joinaliasvars,
                joinleftcols,
                joinrightcols,
            } => RangeTblEntryKind::Join {
                jointype,
                joinmergedcols,
                joinaliasvars: joinaliasvars
                    .into_iter()
                    .map(|expr| shift_expr_rtindexes(expr, offset))
                    .collect(),
                joinleftcols,
                joinrightcols,
            },
            other => other,
        },
        ..entry
    }
}

pub(super) fn shift_expr_rtindexes(expr: Expr, offset: usize) -> Expr {
    match expr {
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| shift_expr_rtindexes(arg, offset))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| shift_expr_rtindexes(arg, offset))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| shift_expr_rtindexes(arg, offset))
                .collect(),
            ..*func
        })),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| shift_expr_rtindexes(arg, offset))
                .collect(),
            ..*aggref
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(shift_expr_rtindexes(*saop.left, offset)),
            right: Box::new(shift_expr_rtindexes(*saop.right, offset)),
            ..*saop
        })),
        Expr::Var(mut var) => {
            if var.varlevelsup == 0 {
                var.varno += offset;
            }
            Expr::Var(var)
        }
        expr @ (Expr::Param(_)
        | Expr::OuterColumn { .. }
        | Expr::Column(_)
        | Expr::Const(_)
        | Expr::Random) => expr,
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(shift_expr_rtindexes(*inner, offset)), ty),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(shift_expr_rtindexes(*expr, offset)),
            pattern: Box::new(shift_expr_rtindexes(*pattern, offset)),
            escape: escape.map(|expr| Box::new(shift_expr_rtindexes(*expr, offset))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(shift_expr_rtindexes(*expr, offset)),
            pattern: Box::new(shift_expr_rtindexes(*pattern, offset)),
            escape: escape.map(|expr| Box::new(shift_expr_rtindexes(*expr, offset))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(shift_expr_rtindexes(*inner, offset))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(shift_expr_rtindexes(*inner, offset))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(shift_expr_rtindexes(*left, offset)),
            Box::new(shift_expr_rtindexes(*right, offset)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(shift_expr_rtindexes(*left, offset)),
            Box::new(shift_expr_rtindexes(*right, offset)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|expr| shift_expr_rtindexes(expr, offset))
                .collect(),
            array_type,
        },
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| Box::new(shift_expr_rtindexes(*expr, offset))),
            ..*sublink
        })),
        Expr::SubPlan(_) => unreachable!("semantic analyze should not shift planned subqueries"),
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(shift_expr_rtindexes(*left, offset)),
            Box::new(shift_expr_rtindexes(*right, offset)),
        ),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(shift_expr_rtindexes(*arg, offset))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: shift_expr_rtindexes(arm.expr, offset),
                    result: shift_expr_rtindexes(arm.result, offset),
                })
                .collect(),
            defresult: Box::new(shift_expr_rtindexes(*case_expr.defresult, offset)),
            ..*case_expr
        })),
        Expr::CaseTest(case_test) => Expr::CaseTest(case_test),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(shift_expr_rtindexes(*array, offset)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| shift_expr_rtindexes(expr, offset)),
                    upper: subscript
                        .upper
                        .map(|expr| shift_expr_rtindexes(expr, offset)),
                })
                .collect(),
        },
        expr @ (Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }) => expr,
    }
}

pub(super) fn identity_target_list(
    columns: &[QueryColumn],
    output_exprs: &[Expr],
) -> Vec<TargetEntry> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            TargetEntry::new(
                column.name.clone(),
                output_exprs
                    .get(index)
                    .cloned()
                    .unwrap_or(Expr::Column(index)),
                column.sql_type,
                index + 1,
            )
        })
        .collect()
}

pub(super) fn normalize_target_list(mut targets: Vec<TargetEntry>) -> Vec<TargetEntry> {
    for (index, target) in targets.iter_mut().enumerate() {
        target.resno = index + 1;
    }
    targets
}

fn rte_output_exprs(rtindex: usize, columns: &[QueryColumn]) -> Vec<Expr> {
    columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Expr::Var(Var {
                varno: rtindex,
                varattno: user_attrno(index),
                varlevelsup: 0,
                vartype: column.sql_type,
            })
        })
        .collect()
}

pub(super) fn rewrite_target_entries(
    targets: Vec<TargetEntry>,
    output_exprs: &[Expr],
) -> Vec<TargetEntry> {
    targets
        .into_iter()
        .map(|target| {
            let input_resno = match &target.expr {
                Expr::Column(index) => Some(index + 1),
                _ => target.input_resno,
            };
            TargetEntry {
                expr: rewrite_expr_columns(target.expr, output_exprs),
                input_resno,
                ..target
            }
        })
        .collect()
}

pub(super) fn rewrite_order_by_entries(
    items: Vec<OrderByEntry>,
    output_exprs: &[Expr],
) -> Vec<OrderByEntry> {
    items
        .into_iter()
        .map(|item| OrderByEntry {
            expr: rewrite_expr_columns(item.expr, output_exprs),
            ressortgroupref: item.ressortgroupref,
            descending: item.descending,
            nulls_first: item.nulls_first,
        })
        .collect()
}

pub(super) fn rewrite_project_set_targets(
    targets: Vec<ProjectSetTarget>,
    output_exprs: &[Expr],
) -> Vec<ProjectSetTarget> {
    targets
        .into_iter()
        .map(|target| match target {
            ProjectSetTarget::Scalar(entry) => ProjectSetTarget::Scalar(TargetEntry {
                expr: rewrite_expr_columns(entry.expr, output_exprs),
                ..entry
            }),
            ProjectSetTarget::Set {
                name,
                call,
                sql_type,
                column_index,
            } => ProjectSetTarget::Set {
                name,
                call: rewrite_set_returning_call(call, output_exprs),
                sql_type,
                column_index,
            },
        })
        .collect()
}

fn rewrite_set_returning_call(call: SetReturningCall, output_exprs: &[Expr]) -> SetReturningCall {
    match call {
        SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start,
            stop,
            step,
            output,
        } => SetReturningCall::GenerateSeries {
            func_oid,
            func_variadic,
            start: rewrite_expr_columns(start, output_exprs),
            stop: rewrite_expr_columns(stop, output_exprs),
            step: rewrite_expr_columns(step, output_exprs),
            output,
        },
        SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args,
            output_columns,
        } => SetReturningCall::Unnest {
            func_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
                .collect(),
            output_columns,
        },
        SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
        } => SetReturningCall::JsonTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
                .collect(),
            output_columns,
        },
        SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args,
            output_columns,
        } => SetReturningCall::RegexTableFunction {
            func_oid,
            func_variadic,
            kind,
            args: args
                .into_iter()
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
                .collect(),
            output_columns,
        },
        SetReturningCall::TextSearchTableFunction {
            kind,
            args,
            output_columns,
        } => SetReturningCall::TextSearchTableFunction {
            kind,
            args: args
                .into_iter()
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
                .collect(),
            output_columns,
        },
        SetReturningCall::UserDefined {
            proc_oid,
            func_variadic,
            args,
            output_columns,
        } => SetReturningCall::UserDefined {
            proc_oid,
            func_variadic,
            args: args
                .into_iter()
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
                .collect(),
            output_columns,
        },
    }
}

pub(super) fn rewrite_agg_accums(
    accumulators: Vec<AggAccum>,
    output_exprs: &[Expr],
) -> Vec<AggAccum> {
    accumulators
        .into_iter()
        .map(|accum| AggAccum {
            args: accum
                .args
                .into_iter()
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
                .collect(),
            ..accum
        })
        .collect()
}

pub(super) fn rewrite_expr_columns(expr: Expr, output_exprs: &[Expr]) -> Expr {
    match expr {
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| rewrite_expr_columns(arg, output_exprs))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| rewrite_expr_columns(arg, output_exprs))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| rewrite_expr_columns(arg, output_exprs))
                .collect(),
            ..*func
        })),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| rewrite_expr_columns(arg, output_exprs))
                .collect(),
            ..*aggref
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(rewrite_expr_columns(*saop.left, output_exprs)),
            right: Box::new(rewrite_expr_columns(*saop.right, output_exprs)),
            ..*saop
        })),
        Expr::Column(index) => output_exprs
            .get(index)
            .cloned()
            .unwrap_or(Expr::Column(index)),
        expr @ (Expr::Param(_)
        | Expr::OuterColumn { .. }
        | Expr::Var(_)
        | Expr::Const(_)
        | Expr::Random) => expr,
        Expr::Cast(inner, ty) => {
            Expr::Cast(Box::new(rewrite_expr_columns(*inner, output_exprs)), ty)
        }
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Expr::Like {
            expr: Box::new(rewrite_expr_columns(*expr, output_exprs)),
            pattern: Box::new(rewrite_expr_columns(*pattern, output_exprs)),
            escape: escape.map(|expr| Box::new(rewrite_expr_columns(*expr, output_exprs))),
            case_insensitive,
            negated,
        },
        Expr::Similar {
            expr,
            pattern,
            escape,
            negated,
        } => Expr::Similar {
            expr: Box::new(rewrite_expr_columns(*expr, output_exprs)),
            pattern: Box::new(rewrite_expr_columns(*pattern, output_exprs)),
            escape: escape.map(|expr| Box::new(rewrite_expr_columns(*expr, output_exprs))),
            negated,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_expr_columns(*inner, output_exprs))),
        Expr::IsNotNull(inner) => {
            Expr::IsNotNull(Box::new(rewrite_expr_columns(*inner, output_exprs)))
        }
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|expr| rewrite_expr_columns(expr, output_exprs))
                .collect(),
            array_type,
        },
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| Box::new(rewrite_expr_columns(*expr, output_exprs))),
            ..*sublink
        })),
        Expr::SubPlan(_) => {
            unreachable!("semantic analyze should not rewrite planned subqueries")
        }
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rewrite_expr_columns(*left, output_exprs)),
            Box::new(rewrite_expr_columns(*right, output_exprs)),
        ),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(rewrite_expr_columns(*arg, output_exprs))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: rewrite_expr_columns(arm.expr, output_exprs),
                    result: rewrite_expr_columns(arm.result, output_exprs),
                })
                .collect(),
            defresult: Box::new(rewrite_expr_columns(*case_expr.defresult, output_exprs)),
            ..*case_expr
        })),
        Expr::CaseTest(case_test) => Expr::CaseTest(case_test),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_expr_columns(*array, output_exprs)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| rewrite_expr_columns(expr, output_exprs)),
                    upper: subscript
                        .upper
                        .map(|expr| rewrite_expr_columns(expr, output_exprs)),
                })
                .collect(),
        },
        expr @ (Expr::CurrentDate
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }) => expr,
    }
}
