use super::query::rewrite_local_vars_for_output_exprs;
use super::*;
use crate::include::catalog::BTREE_AM_OID;
use crate::include::nodes::primnodes::{BoolExprType, OpExprKind};
use crate::include::nodes::parsenodes::{
    OnConflictAction, OnConflictClause, OnConflictTarget,
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
            let requested = spec
                .elements
                .iter()
                .map(|element| bind_inference_element(element, &scope, catalog))
                .collect::<Result<Vec<_>, _>>()?;
            let requested_predicate = spec
                .predicate
                .as_ref()
                .map(|predicate| bind_expr_with_outer_and_ctes(
                    predicate,
                    &scope,
                    catalog,
                    &[],
                    None,
                    &[],
                ))
                .transpose()?;
            let matches = inferable_unique_indexes(&catalog.index_relations_for_heap(relation_oid))
                .into_iter()
                .filter(|index| {
                    index_matches_inference(
                        index,
                        &requested,
                        requested_predicate.as_ref(),
                        relation_name,
                        desc,
                        catalog,
                    )
                    .unwrap_or(false)
                })
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct BoundInferenceElement {
    expr: Expr,
    opclass_oid: Option<u32>,
    collation_oid: Option<u32>,
}

fn inferable_unique_indexes(indexes: &[BoundIndexRelation]) -> Vec<BoundIndexRelation> {
    indexes
        .iter()
        .filter(|index| {
            index.index_meta.indisunique
                && index.index_meta.indisvalid
                && index.index_meta.indisready
                && index.index_meta.am_oid == BTREE_AM_OID
        })
        .cloned()
        .collect()
}

fn bind_inference_element(
    element: &crate::include::nodes::parsenodes::OnConflictInferenceElem,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
) -> Result<BoundInferenceElement, ParseError> {
    let expr = bind_expr_with_outer_and_ctes(&element.expr, scope, catalog, &[], None, &[])?;
    let expr_type = infer_sql_expr_type_with_ctes(&element.expr, scope, catalog, &[], None, &[]);
    Ok(BoundInferenceElement {
        expr,
        opclass_oid: element
            .opclass
            .as_ref()
            .map(|name| resolve_opclass_oid(name, expr_type, catalog))
            .transpose()?,
        collation_oid: element
            .collation
            .as_ref()
            .map(|name| resolve_collation_oid(name, catalog))
            .transpose()?,
    })
}

fn resolve_opclass_oid(
    name: &str,
    _expr_type: SqlType,
    catalog: &dyn CatalogLookup,
) -> Result<u32, ParseError> {
    let normalized = normalize_lookup_name(name);
    catalog
        .opclass_rows()
        .into_iter()
        .find(|row| row.opcmethod == BTREE_AM_OID && row.opcname.eq_ignore_ascii_case(normalized))
        .map(|row| row.oid)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "known btree operator class",
            actual: name.to_string(),
        })
}

fn resolve_collation_oid(name: &str, catalog: &dyn CatalogLookup) -> Result<u32, ParseError> {
    let normalized = normalize_lookup_name(name);
    catalog
        .collation_rows()
        .into_iter()
        .find(|row| row.collname.eq_ignore_ascii_case(normalized))
        .map(|row| row.oid)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "known collation",
            actual: name.to_string(),
        })
}

fn index_matches_inference(
    index: &BoundIndexRelation,
    requested: &[BoundInferenceElement],
    requested_predicate: Option<&Expr>,
    relation_name: &str,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<bool, ParseError> {
    let key_count = index.index_meta.indnkeyatts.max(0) as usize;
    let indexed_elements = index_key_elements(index, key_count, relation_name, desc, catalog)?;
    if requested.is_empty() || indexed_elements.is_empty() {
        return Ok(false);
    }
    if !requested
        .iter()
        .all(|element| indexed_elements.iter().any(|candidate| inference_element_matches(element, candidate)))
    {
        return Ok(false);
    }
    if !indexed_elements
        .iter()
        .all(|candidate| requested.iter().any(|element| inference_element_matches(element, candidate)))
    {
        return Ok(false);
    }
    index_predicate_matches(index, requested_predicate, relation_name, desc, catalog)
}

fn index_key_elements(
    index: &BoundIndexRelation,
    key_count: usize,
    relation_name: &str,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<BoundInferenceElement>, ParseError> {
    let mut expr_index = 0usize;
    let mut elements = Vec::with_capacity(key_count);
    for position in 0..key_count {
        let attnum = *index.index_meta.indkey.get(position).unwrap_or(&0);
        let expr = if attnum > 0 {
            let column_index = column_index_for_attnum(desc, attnum)?;
            bind_relation_expr(
                &desc.columns[column_index].name,
                Some(relation_name),
                desc,
                catalog,
            )?
        } else {
            let expr = index
                .index_exprs
                .get(expr_index)
                .cloned()
                .ok_or_else(|| ParseError::UnexpectedToken {
                    expected: "bound index expression",
                    actual: "index expression metadata mismatch".into(),
                })?;
            expr_index += 1;
            expr
        };
        elements.push(BoundInferenceElement {
            expr,
            opclass_oid: index.index_meta.indclass.get(position).copied().filter(|oid| *oid != 0),
            collation_oid: index
                .index_meta
                .indcollation
                .get(position)
                .copied()
                .filter(|oid| *oid != 0),
        });
    }
    Ok(elements)
}

fn inference_element_matches(
    requested: &BoundInferenceElement,
    indexed: &BoundInferenceElement,
) -> bool {
    requested.expr == indexed.expr
        && requested
            .opclass_oid
            .is_none_or(|opclass_oid| indexed.opclass_oid == Some(opclass_oid))
        && requested
            .collation_oid
            .is_none_or(|collation_oid| indexed.collation_oid == Some(collation_oid))
}

fn index_predicate_matches(
    index: &BoundIndexRelation,
    requested_predicate: Option<&Expr>,
    relation_name: &str,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<bool, ParseError> {
    let Some(predicate_sql) = index.index_meta.indpred.as_deref().map(str::trim) else {
        return Ok(requested_predicate.is_none());
    };
    if predicate_sql.is_empty() {
        return Ok(requested_predicate.is_none());
    }
    let Some(requested_predicate) = requested_predicate else {
        return Ok(false);
    };
    let index_predicate = bind_relation_expr(predicate_sql, Some(relation_name), desc, catalog)?;
    let index_conjuncts = flatten_and_clauses(&index_predicate);
    let requested_conjuncts = flatten_and_clauses(requested_predicate);
    Ok(index_conjuncts
        .iter()
        .all(|conjunct| requested_conjuncts.contains(conjunct)))
}

fn flatten_and_clauses(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::Bool(bool_expr) if bool_expr.boolop == BoolExprType::And => bool_expr
            .args
            .iter()
            .flat_map(flatten_and_clauses)
            .collect(),
        Expr::Op(op)
            if op.op == OpExprKind::Eq
                || op.op == OpExprKind::NotEq
                || op.op == OpExprKind::Lt
                || op.op == OpExprKind::LtEq
                || op.op == OpExprKind::Gt
                || op.op == OpExprKind::GtEq =>
        {
            vec![expr.clone()]
        }
        _ => vec![expr.clone()],
    }
}

fn normalize_lookup_name(name: &str) -> &str {
    name.strip_prefix("pg_catalog.").unwrap_or(name)
}

fn column_index_for_attnum(desc: &RelationDesc, attnum: i16) -> Result<usize, ParseError> {
    if attnum <= 0 {
        return Err(ParseError::UnexpectedToken {
            expected: "user column attribute number",
            actual: attnum.to_string(),
        });
    }
    let column_index = (attnum - 1) as usize;
    let Some(column) = desc.columns.get(column_index) else {
        return Err(ParseError::UnexpectedToken {
            expected: "valid column attribute number",
            actual: attnum.to_string(),
        });
    };
    if column.dropped {
        return Err(ParseError::UnexpectedToken {
            expected: "non-dropped column attribute number",
            actual: attnum.to_string(),
        });
    }
    Ok(column_index)
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
