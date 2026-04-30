use super::*;
use crate::include::executor::execdesc::CommandType;
use crate::include::nodes::parsenodes::{
    JoinTreeNode, Query, RangeTblEntry, RangeTblEntryKind, RangeTblEref, RecursiveUnionQuery,
    SetOperationQuery, TableSampleClause,
};
use crate::include::nodes::primnodes::{
    Aggref, BoolExpr, FuncExpr, GroupingFuncExpr, GroupingKeyExpr, OpExpr, OrderByEntry,
    RelationPrivilegeMask, RelationPrivilegeRequirement, RowsFromItem, RowsFromSource,
    ScalarArrayOpExpr, SetReturningExpr, SubLink, SubPlan, WindowClause, WindowFrame,
    WindowFrameBound, WindowFuncExpr, WindowFuncKind, WindowSpec, attrno_index, is_special_varno,
    is_system_attr, user_attrno,
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

fn rte_eref(aliasname: impl Into<String>, columns: &[QueryColumn]) -> RangeTblEref {
    RangeTblEref {
        aliasname: aliasname.into(),
        colnames: columns.iter().map(|column| column.name.clone()).collect(),
    }
}

pub(crate) fn analyze_select_query_with_outer(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    outer_scopes: &[BoundScope],
    grouped_outer: Option<GroupedOuterScope>,
    visible_agg_scope: Option<&VisibleAggregateScope>,
    outer_ctes: &[BoundCte],
    expanded_views: &[u32],
) -> Result<(Query, BoundScope), ParseError> {
    super::bind_select_query_with_outer(
        stmt,
        catalog,
        outer_scopes,
        grouped_outer,
        visible_agg_scope,
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
        relispopulated: bool,
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
                wire_type_oid: None,
            })
            .collect::<Vec<_>>();
        let privilege_name = relation_name.clone();
        let permission = RelationPrivilegeRequirement::new(
            relation_oid,
            privilege_name,
            relkind,
            RelationPrivilegeMask::select(),
        );
        Self {
            rtable: vec![RangeTblEntry {
                alias: Some(relation_name.clone()),
                alias_preserves_source_names: false,
                eref: rte_eref(relation_name, &output_columns),
                desc,
                inh,
                security_quals: Vec::new(),
                permission: Some(permission),
                kind: RangeTblEntryKind::Relation {
                    rel,
                    relation_oid,
                    relkind,
                    relispopulated,
                    toast,
                    tablesample: None,
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
                alias_preserves_source_names: false,
                eref: rte_eref("*VALUES*", &output_columns),
                desc,
                inh: false,
                security_quals: Vec::new(),
                permission: None,
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
        let relation_name = match &call {
            SetReturningCall::RowsFrom { .. } => "rows_from",
            SetReturningCall::SqlJsonTable(_) => "json_table",
            SetReturningCall::SqlXmlTable(_) => "xmltable",
            _ => "function_call",
        };
        let desc = RelationDesc {
            columns: output_columns
                .iter()
                .map(|column| column_desc(column.name.clone(), column.sql_type, true))
                .collect(),
        };
        Self {
            rtable: vec![RangeTblEntry {
                alias: None,
                alias_preserves_source_names: false,
                eref: rte_eref(relation_name, &output_columns),
                desc,
                inh: false,
                security_quals: Vec::new(),
                permission: None,
                kind: RangeTblEntryKind::Function { call },
            }],
            jointree: Some(JoinTreeNode::RangeTblRef(1)),
            output_exprs: rte_output_exprs(1, &output_columns),
            output_columns,
        }
    }

    pub(super) fn project_function(
        output_exprs: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
        display_sql: Option<String>,
        alias: Option<String>,
    ) -> Self {
        let mut plan = Self::function(SetReturningCall::RowsFrom {
            items: vec![RowsFromItem {
                source: RowsFromSource::Project {
                    output_exprs,
                    output_columns: output_columns.clone(),
                    display_sql,
                },
                column_definitions: false,
            }],
            output_columns: output_columns.clone(),
            with_ordinality: false,
        });
        if let Some(alias) = alias
            && let Some(rte) = plan.rtable.last_mut()
        {
            rte.alias = Some(alias.clone());
            rte.eref.aliasname = alias;
        }
        plan
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
                alias_preserves_source_names: false,
                eref: rte_eref(format!("worktable {worktable_id}"), &output_columns),
                desc,
                inh: false,
                security_quals: Vec::new(),
                permission: None,
                kind: RangeTblEntryKind::WorkTable { worktable_id },
            }],
            jointree: Some(JoinTreeNode::RangeTblRef(1)),
            output_exprs: rte_output_exprs(1, &output_columns),
            output_columns,
        }
    }

    pub(super) fn cte_scan(cte_name: String, cte_id: usize, query: Query) -> Self {
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
                alias_preserves_source_names: false,
                eref: rte_eref(cte_name, &output_columns),
                desc,
                inh: false,
                security_quals: Vec::new(),
                permission: None,
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
                alias_preserves_source_names: false,
                eref: rte_eref("subquery", &output_columns),
                desc,
                inh: false,
                security_quals: Vec::new(),
                permission: None,
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
                    alias_info.output_exprs,
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
                quals: on,
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
            alias_preserves_source_names: false,
            eref: rte_eref("join", &output_columns),
            desc,
            inh: false,
            security_quals: Vec::new(),
            permission: None,
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
        targets
    });
    Query {
        command_type: CommandType::Select,
        depends_on_row_security: false,
        rtable,
        jointree,
        target_list,
        distinct: false,
        distinct_on: Vec::new(),
        where_qual: None,
        group_by: Vec::new(),
        group_by_refs: Vec::new(),
        grouping_sets: Vec::new(),
        accumulators: Vec::new(),
        window_clauses: Vec::new(),
        having_qual: None,
        sort_clause: Vec::new(),
        constraint_deps: Vec::new(),
        limit_count: None,
        limit_offset: None,
        locking_clause: None,
        locking_targets: Vec::new(),
        locking_nowait: false,
        row_marks: Vec::new(),
        has_target_srfs: false,
        recursive_union: None,
        set_operation: None,
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
    let security_quals = entry
        .security_quals
        .into_iter()
        .map(|expr| shift_expr_rtindexes(expr, offset))
        .collect();
    RangeTblEntry {
        security_quals,
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

pub(crate) fn shift_expr_rtindexes(expr: Expr, offset: usize) -> Expr {
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
        Expr::SqlJsonQueryFunction(func) => Expr::SqlJsonQueryFunction(Box::new(
            (*func).map_exprs(|expr| shift_expr_rtindexes(expr, offset)),
        )),
        Expr::SetReturning(srf) => Expr::SetReturning(Box::new(SetReturningExpr {
            call: srf
                .call
                .map_exprs(|expr| shift_expr_rtindexes(expr, offset)),
            ..*srf
        })),
        Expr::Xml(xml) => Expr::Xml(Box::new(crate::include::nodes::primnodes::XmlExpr {
            named_args: xml
                .named_args
                .into_iter()
                .map(|arg| shift_expr_rtindexes(arg, offset))
                .collect(),
            args: xml
                .args
                .into_iter()
                .map(|arg| shift_expr_rtindexes(arg, offset))
                .collect(),
            ..*xml
        })),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| shift_expr_rtindexes(arg, offset))
                .collect(),
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| OrderByEntry {
                    expr: shift_expr_rtindexes(item.expr, offset),
                    ..item
                })
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| shift_expr_rtindexes(expr, offset)),
            ..*aggref
        })),
        Expr::GroupingKey(grouping_key) => Expr::GroupingKey(Box::new(GroupingKeyExpr {
            expr: Box::new(shift_expr_rtindexes(*grouping_key.expr, offset)),
            ..*grouping_key
        })),
        Expr::GroupingFunc(grouping_func) => Expr::GroupingFunc(Box::new(GroupingFuncExpr {
            args: grouping_func
                .args
                .into_iter()
                .map(|arg| shift_expr_rtindexes(arg, offset))
                .collect(),
            ..*grouping_func
        })),
        Expr::WindowFunc(window_func) => {
            Expr::WindowFunc(Box::new(crate::include::nodes::primnodes::WindowFuncExpr {
                kind: match window_func.kind {
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Aggregate(
                            match shift_expr_rtindexes(Expr::Aggref(Box::new(aggref)), offset) {
                                Expr::Aggref(aggref) => *aggref,
                                other => unreachable!(
                                    "window aggregate shift returned non-Aggref: {other:?}"
                                ),
                            },
                        )
                    }
                    crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind)
                    }
                },
                args: window_func
                    .args
                    .into_iter()
                    .map(|arg| shift_expr_rtindexes(arg, offset))
                    .collect(),
                ..*window_func
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(shift_expr_rtindexes(*saop.left, offset)),
            right: Box::new(shift_expr_rtindexes(*saop.right, offset)),
            ..*saop
        })),
        Expr::Var(mut var) => {
            if var.varlevelsup == 0 && !is_special_varno(var.varno) {
                var.varno += offset;
            }
            Expr::Var(var)
        }
        expr @ (Expr::Param(_) | Expr::Const(_) | Expr::Random) => expr,
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(shift_expr_rtindexes(*inner, offset)), ty),
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(shift_expr_rtindexes(*expr, offset)),
            collation_oid,
        },
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(shift_expr_rtindexes(*expr, offset)),
            pattern: Box::new(shift_expr_rtindexes(*pattern, offset)),
            escape: escape.map(|expr| Box::new(shift_expr_rtindexes(*expr, offset))),
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
            expr: Box::new(shift_expr_rtindexes(*expr, offset)),
            pattern: Box::new(shift_expr_rtindexes(*pattern, offset)),
            escape: escape.map(|expr| Box::new(shift_expr_rtindexes(*expr, offset))),
            negated,
            collation_oid,
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
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, shift_expr_rtindexes(expr, offset)))
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(shift_expr_rtindexes(*expr, offset)),
            field,
            field_type,
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
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
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
                output_exprs.get(index).cloned().unwrap_or_else(|| {
                    panic!(
                        "identity target list missing output expr for column {}",
                        index + 1
                    )
                }),
                column.sql_type,
                index + 1,
            )
            .with_input_resno(index + 1)
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

pub(crate) fn rewrite_local_vars_for_output_exprs(
    expr: Expr,
    source_varno: usize,
    output_exprs: &[Expr],
) -> Expr {
    rewrite_local_vars_for_output_exprs_impl(expr, source_varno, output_exprs, false, 0)
}

pub(crate) fn rewrite_planned_local_vars_for_output_exprs(
    expr: Expr,
    source_varno: usize,
    output_exprs: &[Expr],
) -> Expr {
    rewrite_local_vars_for_output_exprs_impl(expr, source_varno, output_exprs, true, 0)
}

fn rewrite_local_vars_for_output_exprs_impl(
    expr: Expr,
    source_varno: usize,
    output_exprs: &[Expr],
    allow_planned_subqueries: bool,
    source_varlevelsup: usize,
) -> Expr {
    let rewrite_local_vars_for_output_exprs = |expr, source_varno, output_exprs| {
        rewrite_local_vars_for_output_exprs_impl(
            expr,
            source_varno,
            output_exprs,
            allow_planned_subqueries,
            source_varlevelsup,
        )
    };
    match expr {
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| rewrite_local_vars_for_output_exprs(arg, source_varno, output_exprs))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| rewrite_local_vars_for_output_exprs(arg, source_varno, output_exprs))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| rewrite_local_vars_for_output_exprs(arg, source_varno, output_exprs))
                .collect(),
            ..*func
        })),
        Expr::SqlJsonQueryFunction(func) => {
            Expr::SqlJsonQueryFunction(Box::new((*func).map_exprs(|expr| {
                rewrite_local_vars_for_output_exprs(expr, source_varno, output_exprs)
            })))
        }
        Expr::SetReturning(srf) => Expr::SetReturning(Box::new(SetReturningExpr {
            call: srf.call.map_exprs(|expr| {
                rewrite_local_vars_for_output_exprs(expr, source_varno, output_exprs)
            }),
            ..*srf
        })),
        Expr::Xml(xml) => Expr::Xml(Box::new(crate::include::nodes::primnodes::XmlExpr {
            named_args: xml
                .named_args
                .into_iter()
                .map(|arg| rewrite_local_vars_for_output_exprs(arg, source_varno, output_exprs))
                .collect(),
            args: xml
                .args
                .into_iter()
                .map(|arg| rewrite_local_vars_for_output_exprs(arg, source_varno, output_exprs))
                .collect(),
            ..*xml
        })),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            args: aggref
                .args
                .into_iter()
                .map(|arg| rewrite_local_vars_for_output_exprs(arg, source_varno, output_exprs))
                .collect(),
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| OrderByEntry {
                    expr: rewrite_local_vars_for_output_exprs(
                        item.expr,
                        source_varno,
                        output_exprs,
                    ),
                    ..item
                })
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| rewrite_local_vars_for_output_exprs(expr, source_varno, output_exprs)),
            ..*aggref
        })),
        Expr::GroupingKey(grouping_key) => Expr::GroupingKey(Box::new(GroupingKeyExpr {
            expr: Box::new(rewrite_local_vars_for_output_exprs(
                *grouping_key.expr,
                source_varno,
                output_exprs,
            )),
            ..*grouping_key
        })),
        Expr::GroupingFunc(grouping_func) => Expr::GroupingFunc(Box::new(GroupingFuncExpr {
            args: grouping_func
                .args
                .into_iter()
                .map(|arg| rewrite_local_vars_for_output_exprs(arg, source_varno, output_exprs))
                .collect(),
            ..*grouping_func
        })),
        Expr::WindowFunc(window_func) => {
            Expr::WindowFunc(Box::new(crate::include::nodes::primnodes::WindowFuncExpr {
                kind: match window_func.kind {
                    crate::include::nodes::primnodes::WindowFuncKind::Aggregate(aggref) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Aggregate(
                            match rewrite_local_vars_for_output_exprs(
                                Expr::Aggref(Box::new(aggref)),
                                source_varno,
                                output_exprs,
                            ) {
                                Expr::Aggref(aggref) => *aggref,
                                other => unreachable!(
                                    "window aggregate rewrite returned non-Aggref: {other:?}"
                                ),
                            },
                        )
                    }
                    crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind) => {
                        crate::include::nodes::primnodes::WindowFuncKind::Builtin(kind)
                    }
                },
                args: window_func
                    .args
                    .into_iter()
                    .map(|arg| rewrite_local_vars_for_output_exprs(arg, source_varno, output_exprs))
                    .collect(),
                ..*window_func
            }))
        }
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(rewrite_local_vars_for_output_exprs(
                *saop.left,
                source_varno,
                output_exprs,
            )),
            right: Box::new(rewrite_local_vars_for_output_exprs(
                *saop.right,
                source_varno,
                output_exprs,
            )),
            ..*saop
        })),
        Expr::Var(var)
            if var.varlevelsup == source_varlevelsup
                && var.varno == source_varno
                && !is_system_attr(var.varattno) =>
        {
            let replacement = output_exprs
                .get(attrno_index(var.varattno).unwrap_or(usize::MAX))
                .cloned()
                .unwrap_or_else(|| {
                    panic!(
                        "rewrite_local_vars_for_output_exprs missing output expr for local Var attno {}; \
                         parser/analyze should provide explicit output identity",
                        var.varattno
                    )
                });
            raise_expr_varlevels(replacement, source_varlevelsup)
        }
        expr @ (Expr::Param(_) | Expr::Var(_) | Expr::Const(_) | Expr::Random) => expr,
        Expr::Cast(inner, ty) => Expr::Cast(
            Box::new(rewrite_local_vars_for_output_exprs(
                *inner,
                source_varno,
                output_exprs,
            )),
            ty,
        ),
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(rewrite_local_vars_for_output_exprs(
                *expr,
                source_varno,
                output_exprs,
            )),
            collation_oid,
        },
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(rewrite_local_vars_for_output_exprs(
                *expr,
                source_varno,
                output_exprs,
            )),
            pattern: Box::new(rewrite_local_vars_for_output_exprs(
                *pattern,
                source_varno,
                output_exprs,
            )),
            escape: escape.map(|expr| {
                Box::new(rewrite_local_vars_for_output_exprs(
                    *expr,
                    source_varno,
                    output_exprs,
                ))
            }),
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
            expr: Box::new(rewrite_local_vars_for_output_exprs(
                *expr,
                source_varno,
                output_exprs,
            )),
            pattern: Box::new(rewrite_local_vars_for_output_exprs(
                *pattern,
                source_varno,
                output_exprs,
            )),
            escape: escape.map(|expr| {
                Box::new(rewrite_local_vars_for_output_exprs(
                    *expr,
                    source_varno,
                    output_exprs,
                ))
            }),
            negated,
            collation_oid,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(rewrite_local_vars_for_output_exprs(
            *inner,
            source_varno,
            output_exprs,
        ))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(rewrite_local_vars_for_output_exprs(
            *inner,
            source_varno,
            output_exprs,
        ))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(rewrite_local_vars_for_output_exprs(
                *left,
                source_varno,
                output_exprs,
            )),
            Box::new(rewrite_local_vars_for_output_exprs(
                *right,
                source_varno,
                output_exprs,
            )),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(rewrite_local_vars_for_output_exprs(
                *left,
                source_varno,
                output_exprs,
            )),
            Box::new(rewrite_local_vars_for_output_exprs(
                *right,
                source_varno,
                output_exprs,
            )),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|expr| rewrite_local_vars_for_output_exprs(expr, source_varno, output_exprs))
                .collect(),
            array_type,
        },
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| {
                    (
                        name,
                        rewrite_local_vars_for_output_exprs(expr, source_varno, output_exprs),
                    )
                })
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(rewrite_local_vars_for_output_exprs(
                *expr,
                source_varno,
                output_exprs,
            )),
            field,
            field_type,
        },
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: sublink.testexpr.map(|expr| {
                Box::new(rewrite_local_vars_for_output_exprs(
                    *expr,
                    source_varno,
                    output_exprs,
                ))
            }),
            subselect: Box::new(rewrite_query_local_vars_for_output_exprs(
                *sublink.subselect,
                source_varno,
                output_exprs,
                allow_planned_subqueries,
                source_varlevelsup + 1,
            )),
            sublink_type: sublink.sublink_type,
        })),
        Expr::SubPlan(subplan) if allow_planned_subqueries => Expr::SubPlan(Box::new(SubPlan {
            testexpr: subplan.testexpr.map(|expr| {
                Box::new(rewrite_local_vars_for_output_exprs(
                    *expr,
                    source_varno,
                    output_exprs,
                ))
            }),
            args: subplan
                .args
                .into_iter()
                .map(|expr| rewrite_local_vars_for_output_exprs(expr, source_varno, output_exprs))
                .collect(),
            ..*subplan
        })),
        Expr::SubPlan(_) => {
            unreachable!("semantic analyze should not rewrite planned subqueries")
        }
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(rewrite_local_vars_for_output_exprs(
                *left,
                source_varno,
                output_exprs,
            )),
            Box::new(rewrite_local_vars_for_output_exprs(
                *right,
                source_varno,
                output_exprs,
            )),
        ),
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr.arg.map(|arg| {
                Box::new(rewrite_local_vars_for_output_exprs(
                    *arg,
                    source_varno,
                    output_exprs,
                ))
            }),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: rewrite_local_vars_for_output_exprs(arm.expr, source_varno, output_exprs),
                    result: rewrite_local_vars_for_output_exprs(
                        arm.result,
                        source_varno,
                        output_exprs,
                    ),
                })
                .collect(),
            defresult: Box::new(rewrite_local_vars_for_output_exprs(
                *case_expr.defresult,
                source_varno,
                output_exprs,
            )),
            ..*case_expr
        })),
        Expr::CaseTest(case_test) => Expr::CaseTest(case_test),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(rewrite_local_vars_for_output_exprs(
                *array,
                source_varno,
                output_exprs,
            )),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript.lower.map(|expr| {
                        rewrite_local_vars_for_output_exprs(expr, source_varno, output_exprs)
                    }),
                    upper: subscript.upper.map(|expr| {
                        rewrite_local_vars_for_output_exprs(expr, source_varno, output_exprs)
                    }),
                })
                .collect(),
        },
        expr @ (Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }) => expr,
    }
}

fn rewrite_query_local_vars_for_output_exprs(
    mut query: Query,
    source_varno: usize,
    output_exprs: &[Expr],
    allow_planned_subqueries: bool,
    source_varlevelsup: usize,
) -> Query {
    let rewrite_expr = |expr| {
        rewrite_local_vars_for_output_exprs_impl(
            expr,
            source_varno,
            output_exprs,
            allow_planned_subqueries,
            source_varlevelsup,
        )
    };

    query.target_list = query
        .target_list
        .into_iter()
        .map(|target| TargetEntry {
            expr: rewrite_expr(target.expr),
            ..target
        })
        .collect();
    query.where_qual = query.where_qual.map(rewrite_expr);
    query.group_by = query.group_by.into_iter().map(rewrite_expr).collect();
    query.accumulators = query
        .accumulators
        .into_iter()
        .map(|accum| AggAccum {
            direct_args: accum.direct_args.into_iter().map(rewrite_expr).collect(),
            args: accum.args.into_iter().map(rewrite_expr).collect(),
            order_by: accum
                .order_by
                .into_iter()
                .map(|item| OrderByEntry {
                    expr: rewrite_expr(item.expr),
                    ..item
                })
                .collect(),
            filter: accum.filter.map(rewrite_expr),
            ..accum
        })
        .collect();
    query.having_qual = query.having_qual.map(rewrite_expr);
    query.sort_clause = query
        .sort_clause
        .into_iter()
        .map(|clause| SortGroupClause {
            expr: rewrite_expr(clause.expr),
            ..clause
        })
        .collect();
    query.window_clauses = query
        .window_clauses
        .into_iter()
        .map(|clause| WindowClause {
            spec: WindowSpec {
                partition_by: clause
                    .spec
                    .partition_by
                    .into_iter()
                    .map(rewrite_expr)
                    .collect(),
                order_by: clause
                    .spec
                    .order_by
                    .into_iter()
                    .map(|item| OrderByEntry {
                        expr: rewrite_expr(item.expr),
                        ..item
                    })
                    .collect(),
                frame: rewrite_window_frame_exprs(clause.spec.frame, &rewrite_expr),
            },
            functions: clause.functions,
        })
        .collect();
    query.jointree = query
        .jointree
        .map(|jointree| rewrite_jointree_local_vars(jointree, &rewrite_expr));
    query.rtable = query
        .rtable
        .into_iter()
        .map(|mut rte| {
            rte.security_quals = rte.security_quals.into_iter().map(rewrite_expr).collect();
            rte.kind = match rte.kind {
                RangeTblEntryKind::Relation {
                    rel,
                    relation_oid,
                    relkind,
                    relispopulated,
                    toast,
                    tablesample,
                } => RangeTblEntryKind::Relation {
                    rel,
                    relation_oid,
                    relkind,
                    relispopulated,
                    toast,
                    tablesample: tablesample.map(|sample| TableSampleClause {
                        method: sample.method,
                        args: sample.args.into_iter().map(rewrite_expr).collect(),
                        repeatable: sample.repeatable.map(rewrite_expr),
                    }),
                },
                RangeTblEntryKind::Join {
                    jointype,
                    joinmergedcols,
                    joinaliasvars,
                    joinleftcols,
                    joinrightcols,
                } => RangeTblEntryKind::Join {
                    jointype,
                    joinmergedcols,
                    joinaliasvars: joinaliasvars.into_iter().map(rewrite_expr).collect(),
                    joinleftcols,
                    joinrightcols,
                },
                RangeTblEntryKind::Values {
                    rows,
                    output_columns,
                } => RangeTblEntryKind::Values {
                    rows: rows
                        .into_iter()
                        .map(|row| row.into_iter().map(rewrite_expr).collect())
                        .collect(),
                    output_columns,
                },
                RangeTblEntryKind::Function { call } => RangeTblEntryKind::Function {
                    call: call.map_exprs(rewrite_expr),
                },
                RangeTblEntryKind::Cte { cte_id, query } => RangeTblEntryKind::Cte {
                    cte_id,
                    query: Box::new(rewrite_query_local_vars_for_output_exprs(
                        *query,
                        source_varno,
                        output_exprs,
                        allow_planned_subqueries,
                        source_varlevelsup + 1,
                    )),
                },
                RangeTblEntryKind::Subquery { query } => RangeTblEntryKind::Subquery {
                    query: Box::new(rewrite_query_local_vars_for_output_exprs(
                        *query,
                        source_varno,
                        output_exprs,
                        allow_planned_subqueries,
                        source_varlevelsup + 1,
                    )),
                },
                kind @ (RangeTblEntryKind::Result | RangeTblEntryKind::WorkTable { .. }) => kind,
            };
            rte
        })
        .collect();
    query.recursive_union = query.recursive_union.map(|union| {
        Box::new(RecursiveUnionQuery {
            anchor: rewrite_query_local_vars_for_output_exprs(
                union.anchor,
                source_varno,
                output_exprs,
                allow_planned_subqueries,
                source_varlevelsup,
            ),
            recursive: rewrite_query_local_vars_for_output_exprs(
                union.recursive,
                source_varno,
                output_exprs,
                allow_planned_subqueries,
                source_varlevelsup,
            ),
            ..*union
        })
    });
    query.set_operation = query.set_operation.map(|setop| {
        Box::new(SetOperationQuery {
            inputs: setop
                .inputs
                .into_iter()
                .map(|input| {
                    rewrite_query_local_vars_for_output_exprs(
                        input,
                        source_varno,
                        output_exprs,
                        allow_planned_subqueries,
                        source_varlevelsup,
                    )
                })
                .collect(),
            ..*setop
        })
    });
    query
}

fn rewrite_jointree_local_vars(
    node: JoinTreeNode,
    rewrite_expr: &impl Fn(Expr) -> Expr,
) -> JoinTreeNode {
    match node {
        JoinTreeNode::RangeTblRef(_) => node,
        JoinTreeNode::JoinExpr {
            left,
            right,
            kind,
            quals,
            rtindex,
        } => JoinTreeNode::JoinExpr {
            left: Box::new(rewrite_jointree_local_vars(*left, rewrite_expr)),
            right: Box::new(rewrite_jointree_local_vars(*right, rewrite_expr)),
            kind,
            quals: rewrite_expr(quals),
            rtindex,
        },
    }
}

fn rewrite_window_frame_exprs(
    frame: WindowFrame,
    rewrite_expr: &impl Fn(Expr) -> Expr,
) -> WindowFrame {
    WindowFrame {
        start_bound: rewrite_window_frame_bound(frame.start_bound, rewrite_expr),
        end_bound: rewrite_window_frame_bound(frame.end_bound, rewrite_expr),
        ..frame
    }
}

fn rewrite_window_frame_bound(
    bound: WindowFrameBound,
    rewrite_expr: &impl Fn(Expr) -> Expr,
) -> WindowFrameBound {
    match bound {
        WindowFrameBound::OffsetPreceding(mut offset) => {
            offset.expr = rewrite_expr(offset.expr);
            WindowFrameBound::OffsetPreceding(offset)
        }
        WindowFrameBound::OffsetFollowing(mut offset) => {
            offset.expr = rewrite_expr(offset.expr);
            WindowFrameBound::OffsetFollowing(offset)
        }
        other => other,
    }
}

fn raise_expr_varlevels(expr: Expr, delta: usize) -> Expr {
    if delta == 0 {
        return expr;
    }
    match expr {
        Expr::Var(mut var) => {
            var.varlevelsup += delta;
            Expr::Var(var)
        }
        Expr::GroupingKey(grouping_key) => Expr::GroupingKey(Box::new(GroupingKeyExpr {
            expr: Box::new(raise_expr_varlevels(*grouping_key.expr, delta)),
            ..*grouping_key
        })),
        Expr::GroupingFunc(grouping_func) => Expr::GroupingFunc(Box::new(GroupingFuncExpr {
            args: grouping_func
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, delta))
                .collect(),
            ..*grouping_func
        })),
        Expr::Aggref(aggref) => Expr::Aggref(Box::new(Aggref {
            direct_args: aggref
                .direct_args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, delta))
                .collect(),
            args: aggref
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, delta))
                .collect(),
            aggorder: aggref
                .aggorder
                .into_iter()
                .map(|item| OrderByEntry {
                    expr: raise_expr_varlevels(item.expr, delta),
                    ..item
                })
                .collect(),
            aggfilter: aggref
                .aggfilter
                .map(|expr| raise_expr_varlevels(expr, delta)),
            ..*aggref
        })),
        Expr::WindowFunc(window_func) => Expr::WindowFunc(Box::new(WindowFuncExpr {
            kind: match window_func.kind {
                WindowFuncKind::Aggregate(aggref) => {
                    let Expr::Aggref(aggref) =
                        raise_expr_varlevels(Expr::Aggref(Box::new(aggref)), delta)
                    else {
                        unreachable!()
                    };
                    WindowFuncKind::Aggregate(*aggref)
                }
                WindowFuncKind::Builtin(kind) => WindowFuncKind::Builtin(kind),
            },
            args: window_func
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, delta))
                .collect(),
            ..*window_func
        })),
        Expr::Op(op) => Expr::Op(Box::new(OpExpr {
            args: op
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, delta))
                .collect(),
            ..*op
        })),
        Expr::Bool(bool_expr) => Expr::Bool(Box::new(BoolExpr {
            args: bool_expr
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, delta))
                .collect(),
            ..*bool_expr
        })),
        Expr::Func(func) => Expr::Func(Box::new(FuncExpr {
            args: func
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, delta))
                .collect(),
            ..*func
        })),
        Expr::SqlJsonQueryFunction(func) => Expr::SqlJsonQueryFunction(Box::new(
            func.map_exprs(|expr| raise_expr_varlevels(expr, delta)),
        )),
        Expr::SetReturning(srf) => Expr::SetReturning(Box::new(SetReturningExpr {
            call: srf.call.map_exprs(|expr| raise_expr_varlevels(expr, delta)),
            ..*srf
        })),
        Expr::Xml(xml) => Expr::Xml(Box::new(crate::include::nodes::primnodes::XmlExpr {
            named_args: xml
                .named_args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, delta))
                .collect(),
            args: xml
                .args
                .into_iter()
                .map(|arg| raise_expr_varlevels(arg, delta))
                .collect(),
            ..*xml
        })),
        Expr::ScalarArrayOp(saop) => Expr::ScalarArrayOp(Box::new(ScalarArrayOpExpr {
            left: Box::new(raise_expr_varlevels(*saop.left, delta)),
            right: Box::new(raise_expr_varlevels(*saop.right, delta)),
            ..*saop
        })),
        Expr::Cast(inner, ty) => Expr::Cast(Box::new(raise_expr_varlevels(*inner, delta)), ty),
        Expr::Collate {
            expr,
            collation_oid,
        } => Expr::Collate {
            expr: Box::new(raise_expr_varlevels(*expr, delta)),
            collation_oid,
        },
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
            collation_oid,
        } => Expr::Like {
            expr: Box::new(raise_expr_varlevels(*expr, delta)),
            pattern: Box::new(raise_expr_varlevels(*pattern, delta)),
            escape: escape.map(|expr| Box::new(raise_expr_varlevels(*expr, delta))),
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
            expr: Box::new(raise_expr_varlevels(*expr, delta)),
            pattern: Box::new(raise_expr_varlevels(*pattern, delta)),
            escape: escape.map(|expr| Box::new(raise_expr_varlevels(*expr, delta))),
            negated,
            collation_oid,
        },
        Expr::IsNull(inner) => Expr::IsNull(Box::new(raise_expr_varlevels(*inner, delta))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(raise_expr_varlevels(*inner, delta))),
        Expr::IsDistinctFrom(left, right) => Expr::IsDistinctFrom(
            Box::new(raise_expr_varlevels(*left, delta)),
            Box::new(raise_expr_varlevels(*right, delta)),
        ),
        Expr::IsNotDistinctFrom(left, right) => Expr::IsNotDistinctFrom(
            Box::new(raise_expr_varlevels(*left, delta)),
            Box::new(raise_expr_varlevels(*right, delta)),
        ),
        Expr::ArrayLiteral {
            elements,
            array_type,
        } => Expr::ArrayLiteral {
            elements: elements
                .into_iter()
                .map(|expr| raise_expr_varlevels(expr, delta))
                .collect(),
            array_type,
        },
        Expr::Row { descriptor, fields } => Expr::Row {
            descriptor,
            fields: fields
                .into_iter()
                .map(|(name, expr)| (name, raise_expr_varlevels(expr, delta)))
                .collect(),
        },
        Expr::FieldSelect {
            expr,
            field,
            field_type,
        } => Expr::FieldSelect {
            expr: Box::new(raise_expr_varlevels(*expr, delta)),
            field,
            field_type,
        },
        Expr::Coalesce(left, right) => Expr::Coalesce(
            Box::new(raise_expr_varlevels(*left, delta)),
            Box::new(raise_expr_varlevels(*right, delta)),
        ),
        Expr::ArraySubscript { array, subscripts } => Expr::ArraySubscript {
            array: Box::new(raise_expr_varlevels(*array, delta)),
            subscripts: subscripts
                .into_iter()
                .map(|subscript| ExprArraySubscript {
                    is_slice: subscript.is_slice,
                    lower: subscript
                        .lower
                        .map(|expr| raise_expr_varlevels(expr, delta)),
                    upper: subscript
                        .upper
                        .map(|expr| raise_expr_varlevels(expr, delta)),
                })
                .collect(),
        },
        Expr::SubLink(sublink) => Expr::SubLink(Box::new(SubLink {
            testexpr: sublink
                .testexpr
                .map(|expr| Box::new(raise_expr_varlevels(*expr, delta))),
            ..*sublink
        })),
        Expr::SubPlan(subplan) => Expr::SubPlan(Box::new(SubPlan {
            testexpr: subplan
                .testexpr
                .map(|expr| Box::new(raise_expr_varlevels(*expr, delta))),
            args: subplan
                .args
                .into_iter()
                .map(|expr| raise_expr_varlevels(expr, delta))
                .collect(),
            ..*subplan
        })),
        expr @ (Expr::Param(_)
        | Expr::Const(_)
        | Expr::Random
        | Expr::CaseTest(_)
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. }) => expr,
        Expr::Case(case_expr) => Expr::Case(Box::new(crate::include::nodes::primnodes::CaseExpr {
            arg: case_expr
                .arg
                .map(|arg| Box::new(raise_expr_varlevels(*arg, delta))),
            args: case_expr
                .args
                .into_iter()
                .map(|arm| crate::include::nodes::primnodes::CaseWhen {
                    expr: raise_expr_varlevels(arm.expr, delta),
                    result: raise_expr_varlevels(arm.result, delta),
                })
                .collect(),
            defresult: Box::new(raise_expr_varlevels(*case_expr.defresult, delta)),
            ..*case_expr
        })),
    }
}
