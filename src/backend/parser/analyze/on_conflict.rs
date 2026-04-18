use super::query::rewrite_local_vars_for_output_exprs;
use super::*;
use crate::include::catalog::BTREE_AM_OID;
use crate::include::nodes::parsenodes::{
    OnConflictAction, OnConflictClause, OnConflictInferenceSpec, OnConflictTarget, SqlExpr,
};
use crate::include::nodes::primnodes::{INNER_VAR, OUTER_VAR, Var, user_attrno};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundOnConflictClause {
    pub arbiter_indexes: Vec<BoundIndexRelation>,
    pub action: BoundOnConflictAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundOnConflictAction {
    Nothing,
    Update {
        assignments: Vec<BoundAssignment>,
        predicate: Option<Expr>,
    },
}

pub(super) fn bind_on_conflict_clause(
    clause: &OnConflictClause,
    relation_name: &str,
    relation_oid: u32,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
    local_ctes: &[BoundCte],
) -> Result<BoundOnConflictClause, ParseError> {
    let arbiter_indexes =
        resolve_arbiter_indexes(clause, relation_name, relation_oid, desc, catalog)?;
    let action = match clause.action {
        OnConflictAction::Nothing => BoundOnConflictAction::Nothing,
        OnConflictAction::Update => {
            if clause.target.is_none() {
                return Err(ParseError::UnexpectedToken {
                    expected: "ON CONFLICT inference specification or constraint name",
                    actual:
                        "ON CONFLICT DO UPDATE requires inference specification or constraint name"
                            .into(),
                });
            }
            let target_scope = scope_for_relation(Some(relation_name), desc);
            let excluded_scope = scope_for_executor_relation("excluded", desc, 2);
            let raw_scope = combine_scopes(&target_scope, &excluded_scope);
            let outer_output_exprs = executor_output_exprs(desc, OUTER_VAR);
            let inner_output_exprs = executor_output_exprs(desc, INNER_VAR);
            let assignments = clause
                .assignments
                .iter()
                .map(|assignment| {
                    let target = super::modify::bind_assignment_target(
                        &assignment.target,
                        &target_scope,
                        catalog,
                        local_ctes,
                    )?;
                    let expr = bind_expr_with_outer_and_ctes(
                        &assignment.expr,
                        &raw_scope,
                        catalog,
                        &[],
                        None,
                        local_ctes,
                    )?;
                    let expr = rewrite_local_vars_for_output_exprs(expr, 1, &outer_output_exprs);
                    let expr = rewrite_local_vars_for_output_exprs(expr, 2, &inner_output_exprs);
                    Ok(BoundAssignment {
                        column_index: target.column_index,
                        subscripts: target.subscripts,
                        expr,
                    })
                })
                .collect::<Result<Vec<_>, ParseError>>()?;
            let predicate = clause
                .where_clause
                .as_ref()
                .map(|expr| {
                    let expr = bind_expr_with_outer_and_ctes(
                        expr,
                        &raw_scope,
                        catalog,
                        &[],
                        None,
                        local_ctes,
                    )?;
                    let expr = rewrite_local_vars_for_output_exprs(expr, 1, &outer_output_exprs);
                    Ok(rewrite_local_vars_for_output_exprs(
                        expr,
                        2,
                        &inner_output_exprs,
                    ))
                })
                .transpose()?;
            BoundOnConflictAction::Update {
                assignments,
                predicate,
            }
        }
    };
    Ok(BoundOnConflictClause {
        arbiter_indexes,
        action,
    })
}

fn resolve_arbiter_indexes(
    clause: &OnConflictClause,
    relation_name: &str,
    relation_oid: u32,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<BoundIndexRelation>, ParseError> {
    match clause.target.as_ref() {
        None => Ok(inferable_unique_indexes(
            &catalog.index_relations_for_heap(relation_oid),
        )),
        Some(OnConflictTarget::Inference(spec)) => {
            let scope = scope_for_relation(Some(relation_name), desc);
            let requested = normalize_attnums(
                &supported_inference_columns(spec)?
                    .iter()
                    .map(|column| {
                        let index = resolve_column(&scope, column)?;
                        Ok((index + 1) as i16)
                    })
                    .collect::<Result<Vec<_>, ParseError>>()?,
            );
            let matches = inferable_unique_indexes(&catalog.index_relations_for_heap(relation_oid))
                .into_iter()
                .filter(|index| normalize_attnums(&index.index_meta.indkey) == requested)
                .collect::<Vec<_>>();
            if matches.is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "inferable unique btree index",
                    actual:
                        "there is no unique or exclusion constraint matching the ON CONFLICT specification"
                            .into(),
                });
            }
            Ok(matches)
        }
        Some(OnConflictTarget::Constraint(name)) => {
            let row = catalog
                .constraint_rows_for_relation(relation_oid)
                .into_iter()
                .find(|row| row.conname.eq_ignore_ascii_case(name))
                .ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "existing constraint name",
                    actual: format!(
                        "constraint \"{}\" for table \"{}\" does not exist",
                        name, relation_name
                    ),
                })?;
            if row.conindid == 0 {
                return Err(ParseError::UnexpectedToken {
                    expected: "index-backed constraint",
                    actual: "constraint in ON CONFLICT clause has no associated index".into(),
                });
            }
            let index = inferable_unique_indexes(&catalog.index_relations_for_heap(relation_oid))
                .into_iter()
                .find(|index| index.relation_oid == row.conindid)
                .ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "plain-column unique btree index",
                    actual:
                        "there is no unique or exclusion constraint matching the ON CONFLICT specification"
                            .into(),
                })?;
            Ok(vec![index])
        }
    }
}

fn supported_inference_columns(spec: &OnConflictInferenceSpec) -> Result<Vec<String>, ParseError> {
    if spec.predicate.is_some() {
        return Err(ParseError::FeatureNotSupported(
            "ON CONFLICT inference WHERE".into(),
        ));
    }

    spec.elements
        .iter()
        .map(|element| {
            if element.collation.is_some() {
                return Err(ParseError::FeatureNotSupported(
                    "ON CONFLICT inference collation".into(),
                ));
            }
            if element.opclass.is_some() {
                return Err(ParseError::FeatureNotSupported(
                    "ON CONFLICT inference operator class".into(),
                ));
            }
            match &element.expr {
                SqlExpr::Column(name) => Ok(name.clone()),
                _ => Err(ParseError::FeatureNotSupported(
                    "ON CONFLICT inference expressions".into(),
                )),
            }
        })
        .collect()
}

fn inferable_unique_indexes(indexes: &[BoundIndexRelation]) -> Vec<BoundIndexRelation> {
    indexes
        .iter()
        .filter(|index| {
            index.index_meta.indisunique
                && index.index_meta.indisvalid
                && index.index_meta.indisready
                && index.index_meta.am_oid == BTREE_AM_OID
                && index
                    .index_meta
                    .indpred
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or("")
                    .is_empty()
                && index
                    .index_meta
                    .indexprs
                    .as_deref()
                    .map(str::trim)
                    .unwrap_or("")
                    .is_empty()
        })
        .cloned()
        .collect()
}

fn normalize_attnums(attnums: &[i16]) -> Vec<i16> {
    let mut out = attnums.to_vec();
    out.sort_unstable();
    out.dedup();
    out
}

fn executor_output_exprs(desc: &RelationDesc, varno: usize) -> Vec<Expr> {
    desc.columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Expr::Var(Var {
                varno,
                varattno: user_attrno(index),
                varlevelsup: 0,
                vartype: column.sql_type,
            })
        })
        .collect()
}

fn scope_for_executor_relation(
    relation_name: &str,
    desc: &RelationDesc,
    varno: usize,
) -> BoundScope {
    BoundScope {
        desc: desc.clone(),
        output_exprs: executor_output_exprs(desc, varno),
        columns: desc
            .columns
            .iter()
            .map(|column| ScopeColumn {
                output_name: column.name.clone(),
                hidden: column.dropped,
                qualified_only: false,
                relation_names: vec![relation_name.to_string()],
                hidden_invalid_relation_names: vec![],
                hidden_missing_relation_names: vec![],
            })
            .collect(),
        relations: vec![ScopeRelation {
            relation_names: vec![relation_name.to_string()],
            hidden_invalid_relation_names: vec![],
            hidden_missing_relation_names: vec![],
            system_varno: None,
        }],
    }
}
