use super::query::rewrite_local_vars_for_output_exprs;
use super::*;
use crate::include::catalog::BTREE_AM_OID;
use crate::include::nodes::parsenodes::{OnConflictAction, OnConflictClause, OnConflictTarget};
use crate::include::nodes::primnodes::{AttrNumber, INNER_VAR, OUTER_VAR, Var, user_attrno};
use std::collections::{BTreeSet, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundOnConflictClause {
    pub arbiter_indexes: Vec<BoundIndexRelation>,
    pub arbiter_exclusion_constraints: Vec<BoundExclusionConstraint>,
    pub arbiter_temporal_constraints: Vec<BoundTemporalConstraint>,
    pub action: BoundOnConflictAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BoundOnConflictAction {
    Nothing,
    Update {
        assignments: Vec<BoundAssignment>,
        predicate: Option<Expr>,
        conflict_visibility_checks: Vec<crate::backend::rewrite::RlsWriteCheck>,
        update_write_checks: Vec<crate::backend::rewrite::RlsWriteCheck>,
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
    match clause.action {
        OnConflictAction::Nothing => {
            let arbiters = resolve_arbiters(clause, relation_name, relation_oid, desc, catalog)?;
            Ok(BoundOnConflictClause {
                arbiter_indexes: arbiters.indexes,
                arbiter_exclusion_constraints: arbiters.exclusion_constraints,
                arbiter_temporal_constraints: arbiters.temporal_constraints,
                action: BoundOnConflictAction::Nothing,
            })
        }
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
                    super::modify::ensure_generated_assignment_allowed(
                        desc,
                        &target,
                        Some(&assignment.expr),
                    )?;
                    let expr = if matches!(assignment.expr, SqlExpr::Default)
                        && desc.columns[target.column_index].generated.is_some()
                    {
                        Expr::Const(Value::Null)
                    } else {
                        bind_expr_with_outer_and_ctes(
                            &assignment.expr,
                            &raw_scope,
                            catalog,
                            &[],
                            None,
                            local_ctes,
                        )?
                    };
                    let expr = rewrite_local_vars_for_output_exprs(expr, 1, &outer_output_exprs);
                    let expr = rewrite_local_vars_for_output_exprs(expr, 2, &inner_output_exprs);
                    Ok(BoundAssignment {
                        column_index: target.column_index,
                        subscripts: target.subscripts,
                        field_path: target.field_path,
                        indirection: target.indirection,
                        target_sql_type: target.target_sql_type,
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
            let arbiters = resolve_arbiters(clause, relation_name, relation_oid, desc, catalog)?;
            if !arbiters.temporal_constraints.is_empty()
                || !arbiters.exclusion_constraints.is_empty()
            {
                return Err(ParseError::DetailedError {
                    message: "ON CONFLICT DO UPDATE not supported with exclusion constraints"
                        .into(),
                    detail: None,
                    hint: None,
                    sqlstate: "0A000",
                });
            }
            Ok(BoundOnConflictClause {
                arbiter_indexes: arbiters.indexes,
                arbiter_exclusion_constraints: arbiters.exclusion_constraints,
                arbiter_temporal_constraints: arbiters.temporal_constraints,
                action: BoundOnConflictAction::Update {
                    assignments,
                    predicate,
                    conflict_visibility_checks: Vec::new(),
                    update_write_checks: Vec::new(),
                },
            })
        }
    }
}

pub(super) struct BoundOnConflictArbiters {
    pub(super) indexes: Vec<BoundIndexRelation>,
    pub(super) exclusion_constraints: Vec<BoundExclusionConstraint>,
    pub(super) temporal_constraints: Vec<BoundTemporalConstraint>,
}

pub(super) fn resolve_arbiters(
    clause: &OnConflictClause,
    relation_name: &str,
    relation_oid: u32,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<BoundOnConflictArbiters, ParseError> {
    let inference_scope = scope_for_relation(Some(relation_name), desc);
    resolve_arbiters_with_inference_scope(
        clause,
        relation_name,
        relation_oid,
        desc,
        &inference_scope,
        catalog,
    )
}

pub(super) fn resolve_arbiters_with_inference_scope(
    clause: &OnConflictClause,
    relation_name: &str,
    relation_oid: u32,
    desc: &RelationDesc,
    inference_scope: &BoundScope,
    catalog: &dyn CatalogLookup,
) -> Result<BoundOnConflictArbiters, ParseError> {
    match clause.target.as_ref() {
        None => {
            let indexes = inferable_unique_indexes(&catalog.index_relations_for_heap(relation_oid));
            reject_unsupported_arbiter_indexes(&indexes)?;
            let temporal_constraints =
                temporal_constraints_for_relation(relation_oid, desc, catalog)?;
            Ok(BoundOnConflictArbiters {
                indexes,
                exclusion_constraints: Vec::new(),
                temporal_constraints,
            })
        }
        Some(OnConflictTarget::Inference(spec)) => {
            let requested = spec
                .elements
                .iter()
                .map(|element| bind_inference_element(element, inference_scope, catalog))
                .collect::<Result<Vec<_>, _>>()?;
            let requested_predicate = spec
                .predicate
                .as_ref()
                .map(|predicate| {
                    bind_expr_with_outer_and_ctes(
                        predicate,
                        inference_scope,
                        catalog,
                        &[],
                        None,
                        &[],
                    )
                })
                .transpose()?;
            let mut seen = HashSet::new();
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
                .filter(|index| seen.insert(index.relation_oid))
                .collect::<Vec<_>>();
            if matches.is_empty() {
                return Err(ParseError::UnexpectedToken {
                    expected: "inferable unique btree index",
                    actual:
                        "there is no unique or exclusion constraint matching the ON CONFLICT specification"
                            .into(),
                });
            }
            reject_unsupported_arbiter_indexes(&matches)?;
            Ok(BoundOnConflictArbiters {
                indexes: matches,
                exclusion_constraints: Vec::new(),
                temporal_constraints: Vec::new(),
            })
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
            if row.contype == crate::include::catalog::CONSTRAINT_EXCLUSION {
                if row.condeferrable {
                    return Err(ParseError::FeatureNotSupported(
                        "ON CONFLICT does not support deferrable unique constraints/exclusion constraints as arbiters"
                            .into(),
                    ));
                }
                return Ok(BoundOnConflictArbiters {
                    indexes: Vec::new(),
                    exclusion_constraints: vec![super::constraints::bind_exclusion_constraint(
                        row, desc, catalog,
                    )?],
                    temporal_constraints: Vec::new(),
                });
            }
            if row.conperiod {
                return Ok(BoundOnConflictArbiters {
                    indexes: Vec::new(),
                    exclusion_constraints: Vec::new(),
                    temporal_constraints: vec![super::constraints::bind_temporal_constraint(
                        row, desc,
                    )?],
                });
            }
            if catalog
                .index_relations_for_heap(relation_oid)
                .into_iter()
                .find(|index| index.relation_oid == row.conindid)
                .is_some_and(|index| !index.index_meta.indimmediate)
            {
                return Err(ParseError::FeatureNotSupported(
                    "ON CONFLICT does not support deferrable unique constraints as arbiters".into(),
                ));
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
            reject_unsupported_arbiter_indexes(std::slice::from_ref(&index))?;
            Ok(BoundOnConflictArbiters {
                indexes: vec![index],
                exclusion_constraints: Vec::new(),
                temporal_constraints: Vec::new(),
            })
        }
    }
}

fn temporal_constraints_for_relation(
    relation_oid: u32,
    desc: &RelationDesc,
    catalog: &dyn CatalogLookup,
) -> Result<Vec<BoundTemporalConstraint>, ParseError> {
    catalog
        .constraint_rows_for_relation(relation_oid)
        .into_iter()
        .filter(|row| {
            row.conperiod
                && matches!(
                    row.contype,
                    crate::include::catalog::CONSTRAINT_PRIMARY
                        | crate::include::catalog::CONSTRAINT_UNIQUE
                )
        })
        .map(|row| super::constraints::bind_temporal_constraint(row, desc))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BoundInferenceElement {
    expr: Expr,
    plain_attnum: Option<AttrNumber>,
    opclass: Option<BoundOpclassSpec>,
    collation_oid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BoundOpclassSpec {
    family_oid: u32,
    input_type_oid: u32,
}

fn inferable_unique_indexes(indexes: &[BoundIndexRelation]) -> Vec<BoundIndexRelation> {
    indexes
        .iter()
        .filter(|index| {
            index.index_meta.indisunique
                && index.index_meta.indimmediate
                && index.index_meta.indisvalid
                && index.index_meta.indisready
                && index.index_meta.am_oid == BTREE_AM_OID
        })
        .cloned()
        .collect()
}

fn reject_unsupported_arbiter_indexes(indexes: &[BoundIndexRelation]) -> Result<(), ParseError> {
    if indexes.iter().any(|index| !index.index_meta.indimmediate) {
        return Err(ParseError::FeatureNotSupported(
            "ON CONFLICT does not support deferrable unique constraints as arbiters".into(),
        ));
    }
    if indexes
        .iter()
        .any(|index| index.index_predicate.as_ref().is_some_and(expr_uses_ctid))
    {
        return Err(ParseError::FeatureNotSupported(
            "ON CONFLICT with partial indexes whose predicate uses ctid".into(),
        ));
    }
    Ok(())
}

fn bind_inference_element(
    element: &crate::include::nodes::parsenodes::OnConflictInferenceElem,
    scope: &BoundScope,
    catalog: &dyn CatalogLookup,
) -> Result<BoundInferenceElement, ParseError> {
    let expr = bind_expr_with_outer_and_ctes(&element.expr, scope, catalog, &[], None, &[])?;
    Ok(BoundInferenceElement {
        plain_attnum: plain_attnum(&expr),
        expr,
        opclass: element
            .opclass
            .as_ref()
            .map(|name| resolve_opclass_spec(name, catalog))
            .transpose()?,
        collation_oid: element
            .collation
            .as_ref()
            .map(|name| resolve_collation_oid(name, catalog))
            .transpose()?,
    })
}

fn resolve_opclass_spec(
    name: &str,
    catalog: &dyn CatalogLookup,
) -> Result<BoundOpclassSpec, ParseError> {
    let normalized = normalize_lookup_name(name);
    catalog
        .opclass_rows()
        .into_iter()
        .find(|row| row.opcmethod == BTREE_AM_OID && row.opcname.eq_ignore_ascii_case(normalized))
        .map(bound_opclass_spec_from_row)
        .ok_or_else(|| ParseError::UnexpectedToken {
            expected: "known btree operator class",
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
    if requested_plain_attnums(requested) != indexed_plain_attnums(&indexed_elements) {
        return Ok(false);
    }
    if !requested.iter().all(|element| {
        indexed_elements
            .iter()
            .any(|candidate| inference_element_matches(element, candidate))
    }) {
        return Ok(false);
    }
    if !indexed_elements
        .iter()
        .filter(|candidate| candidate.plain_attnum.is_none())
        .all(|candidate| {
            requested
                .iter()
                .any(|element| candidate.expr == element.expr)
        })
    {
        return Ok(false);
    }
    Ok(index_predicate_matches(index, requested_predicate))
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
            let expr = index.index_exprs.get(expr_index).cloned().ok_or_else(|| {
                ParseError::UnexpectedToken {
                    expected: "bound index expression",
                    actual: "index expression metadata mismatch".into(),
                }
            })?;
            expr_index += 1;
            expr
        };
        elements.push(BoundInferenceElement {
            plain_attnum: (attnum > 0).then_some(AttrNumber::from(attnum)),
            expr,
            opclass: index_opclass_spec(index, position, catalog),
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
        && requested.opclass.as_ref().is_none_or(|requested_opclass| {
            indexed.opclass.as_ref().is_some_and(|indexed_opclass| {
                requested_opclass.family_oid == indexed_opclass.family_oid
                    && requested_opclass.input_type_oid == indexed_opclass.input_type_oid
            })
        })
        && requested
            .collation_oid
            .is_none_or(|collation_oid| indexed.collation_oid == Some(collation_oid))
}

fn plain_attnum(expr: &Expr) -> Option<AttrNumber> {
    match expr {
        Expr::Var(var) if var.varattno > 0 => Some(var.varattno),
        _ => None,
    }
}

fn requested_plain_attnums(requested: &[BoundInferenceElement]) -> BTreeSet<AttrNumber> {
    requested
        .iter()
        .filter_map(|element| element.plain_attnum)
        .collect()
}

fn indexed_plain_attnums(indexed: &[BoundInferenceElement]) -> BTreeSet<AttrNumber> {
    indexed
        .iter()
        .filter_map(|element| element.plain_attnum)
        .collect()
}

fn bound_opclass_spec_from_row(row: crate::include::catalog::PgOpclassRow) -> BoundOpclassSpec {
    BoundOpclassSpec {
        family_oid: row.opcfamily,
        input_type_oid: row.opcintype,
    }
}

fn index_opclass_spec(
    index: &BoundIndexRelation,
    position: usize,
    catalog: &dyn CatalogLookup,
) -> Option<BoundOpclassSpec> {
    let oid = index
        .index_meta
        .indclass
        .get(position)
        .copied()
        .filter(|oid| *oid != 0)?;
    let cached_family = index
        .index_meta
        .opfamily_oids
        .get(position)
        .copied()
        .filter(|oid| *oid != 0);
    let cached_input_type = index
        .index_meta
        .opcintype_oids
        .get(position)
        .copied()
        .filter(|oid| *oid != 0);
    match (cached_family, cached_input_type) {
        (Some(family_oid), Some(input_type_oid)) => Some(BoundOpclassSpec {
            family_oid,
            input_type_oid,
        }),
        _ => catalog
            .opclass_rows()
            .into_iter()
            .find(|row| row.oid == oid)
            .map(bound_opclass_spec_from_row),
    }
}

fn index_predicate_matches(index: &BoundIndexRelation, requested_predicate: Option<&Expr>) -> bool {
    predicate_implies_index_predicate(requested_predicate, index.index_predicate.as_ref())
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
                collation_oid: None,
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
                source_relation_oid: None,
                source_attno: None,
                source_columns: Vec::new(),
            })
            .collect(),
        relations: vec![ScopeRelation {
            relation_names: vec![relation_name.to_string()],
            hidden_invalid_relation_names: vec![],
            hidden_missing_relation_names: vec![],
            system_varno: None,
            relation_oid: None,
        }],
    }
}
