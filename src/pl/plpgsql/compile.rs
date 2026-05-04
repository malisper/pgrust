#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::Expr;
use crate::backend::executor::RelationDesc;
use crate::backend::optimizer::finalize_expr_subqueries;
use crate::backend::parser::analyze::scope_for_relation;
use crate::backend::parser::{
    AliasColumnSpec, BoundCte, BoundScope, CatalogLookup, FromItem, GroupByItem, ParseError,
    RawWindowFrameBound, RawWindowSpec, SelectStatement, SlotScopeColumn, SqlExpr, SqlType,
    SqlTypeKind, Statement, TablePersistence, bind_delete_with_outer_scopes,
    bind_insert_with_outer_scopes, bind_scalar_expr_in_named_slot_scope,
    bind_update_with_outer_scopes, parse_expr, parse_statement, parse_type_name,
    pg_plan_query_with_outer_scopes_and_ctes, pg_plan_query_with_outer_scopes_and_ctes_config,
    pg_plan_values_query_with_outer_scopes_and_ctes,
    pg_plan_values_query_with_outer_scopes_and_ctes_config, resolve_raw_type_name,
};
use crate::backend::utils::record::assign_anonymous_record_descriptor;
use crate::include::catalog::{EVENT_TRIGGER_TYPE_OID, PgProcRow, RECORD_TYPE_OID};
use crate::include::nodes::pathnodes::PlannerConfig;
use crate::include::nodes::plannodes::PlannedStmt;
use crate::include::nodes::primnodes::{Param, ParamKind, QueryColumn, Var, user_attrno};
use pgrust_nodes::TriggerTransitionTable;
use pgrust_plpgsql::{
    CompiledAssignIndirection, CompiledBlock, CompiledCursorOpenSource,
    CompiledEventTriggerBindings, CompiledExceptionHandler, CompiledExpr, CompiledForQuerySource,
    CompiledForQueryTarget, CompiledFunction, CompiledFunctionSlot, CompiledIndirectAssignTarget,
    CompiledOutputSlot, CompiledSelectIntoTarget, CompiledStmt, CompiledStrictParam,
    CompiledTriggerBindings, CompiledTriggerRelation, CompiledTriggerTransitionCte, CompiledVar,
    DeclaredCursorParam, FunctionReturnContract, PlpgsqlLabeledVarRef, PlpgsqlNormalizeEnv,
    PlpgsqlVarRef, PlpgsqlVariableConflict, RuntimeSqlScope, TriggerReturnedRow,
    count_raise_placeholders, declared_cursor_args_context, dollar_quote_tag_at, dynamic_shape_sql,
    dynamic_sql_literal, exception_condition_name_sqlstate, find_keyword_at_top_level,
    find_next_top_level_keyword, identifier_position, is_identifier_char, is_identifier_start,
    is_internal_plpgsql_name, is_plpgsql_label_alias, is_unsupported_plpgsql_transaction_command,
    looks_like_aggregate_expr, nonstandard_string_literals_from_gucs, normalize_plpgsql_delete,
    normalize_plpgsql_expr, normalize_plpgsql_insert, normalize_plpgsql_select,
    normalize_plpgsql_sql_statement, normalize_plpgsql_update, normalize_plpgsql_values,
    parse_plpgsql_query_condition, parse_proc_argtype_oids, parse_select_into_assign_target,
    parse_select_into_assign_targets, persistent_object_transition_table_reference_name,
    plpgsql_label_alias, plpgsql_var_alias, positional_parameter_var_name,
    print_strict_params_directive, rewrite_plpgsql_assignment_query_expr, runtime_sql_param_id,
    should_defer_plpgsql_sql_to_runtime, should_fallback_to_runtime_sql,
    split_cte_prefixed_select_into_target, split_dml_returning_into_targets,
    split_select_into_target, split_select_with_into_targets, static_query_source_known_columns,
    target_entry_query_column, transaction_command_name, variable_conflict_from_gucs,
    variable_conflict_mode,
};

#[cfg(test)]
use pgrust_plpgsql::rewrite_plpgsql_query_condition;

use super::ast::{
    AliasTarget, AssignTarget, Block, CursorArg, CursorDecl, Decl, ForQuerySource, ForTarget,
    OpenCursorSource, RaiseCondition, RaiseLevel, RaiseUsingOption, Stmt, VarDecl,
};
use super::gram::parse_block;

#[derive(Debug, Clone)]
struct DeclaredCursor {
    query: String,
    scrollable: bool,
    params: Vec<DeclaredCursorParam>,
}

#[derive(Debug, Clone)]
struct ScopeVar {
    slot: usize,
    ty: SqlType,
    constant: bool,
    not_null: bool,
}

#[derive(Debug, Clone)]
struct LabeledScopeVar {
    var: ScopeVar,
    alias: String,
}

#[derive(Debug, Clone)]
struct RelationScopeVar {
    name: String,
    columns: Vec<SlotScopeColumn>,
    trigger_row: Option<TriggerReturnedRow>,
}

#[derive(Debug, Clone)]
struct LabeledScope {
    label: String,
    vars: HashMap<String, LabeledScopeVar>,
    relation_scopes: Vec<RelationScopeVar>,
}

#[derive(Debug, Clone, Default)]
struct CompileEnv {
    vars: HashMap<String, ScopeVar>,
    relation_scopes: Vec<RelationScopeVar>,
    labeled_scopes: Vec<LabeledScope>,
    local_ctes: Vec<BoundCte>,
    declared_cursors: HashMap<String, DeclaredCursor>,
    open_cursor_shapes: HashMap<usize, Vec<QueryColumn>>,
    parameter_slots: Vec<ScopeVar>,
    positional_parameter_names: Vec<String>,
    exception_sqlstate: Option<ScopeVar>,
    exception_sqlerrm: Option<ScopeVar>,
    variable_conflict: PlpgsqlVariableConflict,
    nonstandard_string_literals: bool,
    next_slot: usize,
}

impl CompileEnv {
    fn child(&self) -> Self {
        self.clone()
    }

    fn define_var(&mut self, name: &str, ty: SqlType) -> usize {
        self.define_var_with_options(name, ty, false, false)
    }

    fn define_var_with_options(
        &mut self,
        name: &str,
        ty: SqlType,
        constant: bool,
        not_null: bool,
    ) -> usize {
        let slot = self.allocate_slot();
        self.vars.insert(
            name.to_ascii_lowercase(),
            ScopeVar {
                slot,
                ty,
                constant,
                not_null,
            },
        );
        slot
    }

    fn allocate_slot(&mut self) -> usize {
        let slot = self.next_slot;
        self.next_slot += 1;
        slot
    }

    fn define_exception_slots(&mut self) -> (usize, usize) {
        let text_ty = SqlType::new(SqlTypeKind::Text);
        let sqlstate_slot = self.allocate_slot();
        let sqlerrm_slot = self.allocate_slot();
        self.exception_sqlstate = Some(ScopeVar {
            slot: sqlstate_slot,
            ty: text_ty,
            constant: false,
            not_null: false,
        });
        self.exception_sqlerrm = Some(ScopeVar {
            slot: sqlerrm_slot,
            ty: text_ty,
            constant: false,
            not_null: false,
        });
        (sqlstate_slot, sqlerrm_slot)
    }

    fn with_exception_vars<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, ParseError>,
    ) -> Result<T, ParseError> {
        let saved_sqlstate = self.vars.insert(
            "sqlstate".into(),
            self.exception_sqlstate
                .clone()
                .ok_or(ParseError::UnexpectedEof)?,
        );
        let saved_sqlerrm = self.vars.insert(
            "sqlerrm".into(),
            self.exception_sqlerrm
                .clone()
                .ok_or(ParseError::UnexpectedEof)?,
        );
        let result = f(self);
        restore_optional_var(&mut self.vars, "sqlstate", saved_sqlstate);
        restore_optional_var(&mut self.vars, "sqlerrm", saved_sqlerrm);
        result
    }

    fn define_parameter_var(&mut self, name: &str, ty: SqlType) -> usize {
        let slot = self.define_var(name, ty);
        self.parameter_slots.push(ScopeVar {
            slot,
            ty,
            constant: false,
            not_null: false,
        });
        let positional_name = positional_parameter_var_name(self.parameter_slots.len());
        self.vars.insert(
            positional_name.clone(),
            ScopeVar {
                slot,
                ty,
                constant: false,
                not_null: false,
            },
        );
        self.positional_parameter_names.push(positional_name);
        slot
    }

    fn define_alias(&mut self, name: &str, slot: usize, ty: SqlType) {
        self.vars.insert(
            name.to_ascii_lowercase(),
            ScopeVar {
                slot,
                ty,
                constant: false,
                not_null: false,
            },
        );
    }

    fn update_slot_type(&mut self, slot: usize, ty: SqlType) {
        for var in self.vars.values_mut() {
            if var.slot == slot {
                var.ty = ty;
            }
        }
        for parameter in &mut self.parameter_slots {
            if parameter.slot == slot {
                parameter.ty = ty;
            }
        }
        for scope in &mut self.labeled_scopes {
            for var in scope.vars.values_mut() {
                if var.var.slot == slot {
                    var.var.ty = ty;
                }
            }
        }
    }

    fn get_var(&self, name: &str) -> Option<&ScopeVar> {
        self.vars.get(&name.to_ascii_lowercase())
    }

    fn get_labeled_var(&self, label: &str, name: &str) -> Option<&LabeledScopeVar> {
        self.labeled_scopes
            .iter()
            .rev()
            .find(|scope| scope.label.eq_ignore_ascii_case(label))
            .and_then(|scope| scope.vars.get(&name.to_ascii_lowercase()))
    }

    fn get_labeled_relation_field(
        &self,
        label: &str,
        relation: &str,
        field: &str,
    ) -> Option<&SlotScopeColumn> {
        self.labeled_scopes
            .iter()
            .rev()
            .find(|scope| scope.label.eq_ignore_ascii_case(label))
            .and_then(|scope| {
                scope
                    .relation_scopes
                    .iter()
                    .find(|relation_scope| relation_scope.name.eq_ignore_ascii_case(relation))
            })
            .and_then(|scope| {
                scope
                    .columns
                    .iter()
                    .find(|column| !column.hidden && column.name.eq_ignore_ascii_case(field))
            })
    }

    fn push_label_scope(&mut self, label: &str) {
        let scope_index = self.labeled_scopes.len();
        let captured = self
            .vars
            .iter()
            .filter(|(name, _)| !is_plpgsql_label_alias(name))
            .map(|(name, var)| {
                let alias = plpgsql_label_alias(scope_index, var.slot, name);
                (
                    name.clone(),
                    LabeledScopeVar {
                        var: var.clone(),
                        alias,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        for var in captured.values() {
            self.vars.insert(var.alias.clone(), var.var.clone());
        }
        self.labeled_scopes.push(LabeledScope {
            label: label.to_ascii_lowercase(),
            vars: captured,
            relation_scopes: self.relation_scopes.clone(),
        });
    }

    fn get_parameter(&self, index: usize) -> Option<&ScopeVar> {
        self.parameter_slots.get(index.saturating_sub(1))
    }

    fn positional_parameter_name(&self, index: usize) -> Option<&str> {
        self.positional_parameter_names
            .get(index.saturating_sub(1))
            .map(String::as_str)
    }

    fn define_relation_scope(
        &mut self,
        name: &str,
        desc: &RelationDesc,
    ) -> CompiledTriggerRelation {
        let mut slots = Vec::with_capacity(desc.columns.len());
        let mut field_names = Vec::with_capacity(desc.columns.len());
        let mut field_types = Vec::with_capacity(desc.columns.len());
        let mut not_null = Vec::with_capacity(desc.columns.len());
        let mut columns = Vec::with_capacity(desc.columns.len());
        for column in &desc.columns {
            let slot = self.next_slot;
            self.next_slot += 1;
            slots.push(slot);
            field_names.push(column.name.clone());
            field_types.push(column.sql_type);
            not_null.push(!column.storage.nullable);
            columns.push(SlotScopeColumn {
                slot,
                name: column.name.clone(),
                sql_type: column.sql_type,
                hidden: column.dropped,
            });
        }
        self.relation_scopes.push(RelationScopeVar {
            name: name.to_ascii_lowercase(),
            columns,
            trigger_row: None,
        });
        CompiledTriggerRelation {
            slots,
            field_names,
            field_types,
            not_null,
        }
    }

    fn define_trigger_relation_scope(
        &mut self,
        name: &str,
        desc: &RelationDesc,
        trigger_row: TriggerReturnedRow,
    ) -> CompiledTriggerRelation {
        let relation = self.define_relation_scope(name, desc);
        if let Some(scope) = self
            .relation_scopes
            .iter_mut()
            .find(|scope| scope.name.eq_ignore_ascii_case(name))
        {
            scope.trigger_row = Some(trigger_row);
        }
        relation
    }

    fn define_relation_alias(&mut self, name: &str, target: TriggerReturnedRow) -> bool {
        let source_name = match target {
            TriggerReturnedRow::New => "new",
            TriggerReturnedRow::Old => "old",
        };
        let Some(source) = self
            .relation_scopes
            .iter()
            .find(|scope| scope.name.eq_ignore_ascii_case(source_name))
            .cloned()
        else {
            return false;
        };
        self.relation_scopes.push(RelationScopeVar {
            name: name.to_ascii_lowercase(),
            columns: source.columns,
            trigger_row: Some(target),
        });
        true
    }

    fn get_relation_field(&self, relation: &str, field: &str) -> Option<&SlotScopeColumn> {
        self.relation_scopes
            .iter()
            .find(|scope| scope.name.eq_ignore_ascii_case(relation))
            .and_then(|scope| {
                scope
                    .columns
                    .iter()
                    .find(|column| !column.hidden && column.name.eq_ignore_ascii_case(field))
            })
    }

    fn trigger_relation_return_row(&self, relation: &str) -> Option<TriggerReturnedRow> {
        self.relation_scopes
            .iter()
            .find(|scope| scope.name.eq_ignore_ascii_case(relation))
            .and_then(|scope| scope.trigger_row)
    }

    fn visible_columns(&self) -> Vec<(String, SqlType)> {
        let mut ordered = self
            .vars
            .iter()
            .map(|(name, var)| (var.slot, name.clone(), var.ty))
            .collect::<Vec<_>>();
        ordered.sort_by_key(|(slot, _, _)| *slot);
        ordered
            .into_iter()
            .map(|(_, name, ty)| (name, ty))
            .collect()
    }

    fn define_cursor(
        &mut self,
        name: &str,
        query: &str,
        scrollable: bool,
        params: Vec<DeclaredCursorParam>,
    ) {
        self.declared_cursors.insert(
            name.to_ascii_lowercase(),
            DeclaredCursor {
                query: query.to_string(),
                scrollable,
                params,
            },
        );
    }

    fn declared_cursor(&self, name: &str) -> Option<&DeclaredCursor> {
        self.declared_cursors.get(&name.to_ascii_lowercase())
    }

    fn visible_sql_columns(&self) -> Vec<(String, SqlType)> {
        let mut columns = self.visible_columns();
        columns.extend(
            self.relation_scopes
                .iter()
                .map(|scope| (scope.name.clone(), SqlType::record(RECORD_TYPE_OID))),
        );
        columns
    }

    fn slot_columns(&self) -> Vec<SlotScopeColumn> {
        let mut ordered = self
            .vars
            .iter()
            .map(|(name, var)| SlotScopeColumn {
                slot: var.slot,
                name: name.clone(),
                sql_type: var.ty,
                hidden: false,
            })
            .collect::<Vec<_>>();
        for (name, var) in &self.vars {
            if is_internal_plpgsql_name(name) {
                continue;
            }
            ordered.push(SlotScopeColumn {
                slot: var.slot,
                name: plpgsql_var_alias(var.slot),
                sql_type: var.ty,
                hidden: false,
            });
        }
        ordered.sort_by(|left, right| {
            left.slot
                .cmp(&right.slot)
                .then_with(|| left.name.cmp(&right.name))
        });
        ordered.dedup_by(|left, right| left.name == right.name);
        ordered
    }

    fn relation_slot_scopes(&self) -> Vec<(String, Vec<SlotScopeColumn>)> {
        self.relation_scopes
            .iter()
            .map(|scope| (scope.name.clone(), scope.columns.clone()))
            .collect()
    }
}

impl PlpgsqlNormalizeEnv for CompileEnv {
    fn get_var(&self, name: &str) -> Option<PlpgsqlVarRef> {
        CompileEnv::get_var(self, name).map(|var| PlpgsqlVarRef {
            slot: var.slot,
            ty: var.ty,
        })
    }

    fn get_labeled_var(&self, label: &str, name: &str) -> Option<PlpgsqlLabeledVarRef> {
        CompileEnv::get_labeled_var(self, label, name).map(|scope_var| PlpgsqlLabeledVarRef {
            var: PlpgsqlVarRef {
                slot: scope_var.var.slot,
                ty: scope_var.var.ty,
            },
            alias: scope_var.alias.clone(),
        })
    }

    fn get_relation_field(&self, relation: &str, field: &str) -> Option<SlotScopeColumn> {
        CompileEnv::get_relation_field(self, relation, field).cloned()
    }

    fn get_labeled_relation_field(
        &self,
        label: &str,
        relation: &str,
        field: &str,
    ) -> Option<SlotScopeColumn> {
        CompileEnv::get_labeled_relation_field(self, label, relation, field).cloned()
    }

    fn variable_conflict(&self) -> PlpgsqlVariableConflict {
        self.variable_conflict
    }
}

fn restore_optional_var(vars: &mut HashMap<String, ScopeVar>, name: &str, saved: Option<ScopeVar>) {
    match saved {
        Some(var) => {
            vars.insert(name.into(), var);
        }
        None => {
            vars.remove(name);
        }
    }
}

pub(crate) fn compile_do_block(
    block: &Block,
    catalog: &dyn CatalogLookup,
) -> Result<CompiledBlock, ParseError> {
    compile_do_block_with_gucs(block, catalog, None)
}

pub(crate) fn compile_do_block_with_gucs(
    block: &Block,
    catalog: &dyn CatalogLookup,
    gucs: Option<&HashMap<String, String>>,
) -> Result<CompiledBlock, ParseError> {
    let mut env = CompileEnv::default();
    env.variable_conflict = variable_conflict_from_gucs(gucs);
    env.nonstandard_string_literals = nonstandard_string_literals_from_gucs(gucs);
    let _ = env.define_var("found", SqlType::new(SqlTypeKind::Bool));
    let _ = env.define_exception_slots();
    compile_block(block, catalog, &mut env, None)
}

pub(crate) fn compile_do_function(
    block: &Block,
    catalog: &dyn CatalogLookup,
    gucs: Option<&HashMap<String, String>>,
) -> Result<CompiledFunction, ParseError> {
    let mut env = CompileEnv::default();
    env.variable_conflict = variable_conflict_from_gucs(gucs);
    env.nonstandard_string_literals = nonstandard_string_literals_from_gucs(gucs);
    let found_slot = env.define_var("found", SqlType::new(SqlTypeKind::Bool));
    let (sqlstate_slot, sqlerrm_slot) = env.define_exception_slots();
    let return_contract = FunctionReturnContract::Scalar {
        ty: SqlType::new(SqlTypeKind::Void),
        setof: false,
        output_slot: None,
    };
    let body = compile_block(block, catalog, &mut env, Some(&return_contract))?;
    Ok(CompiledFunction {
        name: "inline_code_block".into(),
        proc_oid: 0,
        proowner: 0,
        prosecdef: false,
        provolatile: 'v',
        proconfig: None,
        print_strict_params: None,
        parameter_slots: Vec::new(),
        context_arg_type_names: Vec::new(),
        output_slots: Vec::new(),
        body,
        return_contract,
        found_slot,
        sqlstate_slot,
        sqlerrm_slot,
        local_ctes: Vec::new(),
        trigger_transition_ctes: Vec::new(),
    })
}

pub(crate) fn compile_function_from_proc(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
    gucs: Option<&HashMap<String, String>>,
) -> Result<CompiledFunction, ParseError> {
    if row.prorettype == EVENT_TRIGGER_TYPE_OID {
        return Err(ParseError::DetailedError {
            message: "trigger functions can only be called as triggers".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }
    let block = parse_block(&row.prosrc)?;
    let print_strict_params = print_strict_params_directive(&row.prosrc);
    let mut env = CompileEnv::default();
    env.variable_conflict = variable_conflict_mode(&row.prosrc, gucs);
    env.nonstandard_string_literals = nonstandard_string_literals_from_gucs(gucs);
    let mut parameter_slots = Vec::new();
    let mut output_slots = Vec::new();

    let input_type_oids =
        parse_proc_argtype_oids(&row.proargtypes).ok_or_else(|| ParseError::UnexpectedToken {
            expected: "valid pg_proc.proargtypes",
            actual: row.proargtypes.clone(),
        })?;
    let input_types = input_type_oids
        .iter()
        .map(|oid| {
            catalog
                .type_by_oid(*oid)
                .map(|ty| ty.sql_type)
                .ok_or_else(|| ParseError::UnsupportedType(oid.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;

    if let (Some(all_arg_types), Some(arg_modes)) = (&row.proallargtypes, &row.proargmodes) {
        let arg_names = row.proargnames.clone().unwrap_or_default();
        for (index, (type_oid, mode)) in all_arg_types.iter().zip(arg_modes.iter()).enumerate() {
            let sql_type = catalog
                .type_by_oid(*type_oid)
                .map(|ty| ty.sql_type)
                .ok_or_else(|| ParseError::UnsupportedType(type_oid.to_string()))?;
            let name = arg_names
                .get(index)
                .cloned()
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| format!("column{}", index + 1));
            match *mode {
                b'i' | b'v' => {
                    let slot = env.define_parameter_var(&name, sql_type);
                    parameter_slots.push(CompiledFunctionSlot {
                        name,
                        slot,
                        ty: sql_type,
                    });
                }
                b'b' => {
                    let slot = env.define_parameter_var(&name, sql_type);
                    parameter_slots.push(CompiledFunctionSlot {
                        name: name.clone(),
                        slot,
                        ty: sql_type,
                    });
                    output_slots.push(CompiledOutputSlot {
                        name: name.clone(),
                        slot,
                        column: QueryColumn {
                            name,
                            sql_type,
                            wire_type_oid: None,
                        },
                    });
                }
                b'o' | b't' => {
                    let slot = env.define_parameter_var(&name, sql_type);
                    output_slots.push(CompiledOutputSlot {
                        name: name.clone(),
                        slot,
                        column: QueryColumn {
                            name,
                            sql_type,
                            wire_type_oid: None,
                        },
                    });
                }
                _ => {}
            }
        }
    } else {
        let arg_names = row.proargnames.clone().unwrap_or_default();
        for (index, sql_type) in input_types.into_iter().enumerate() {
            let name = arg_names
                .get(index)
                .cloned()
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| format!("arg{}", index + 1));
            let slot = env.define_parameter_var(&name, sql_type);
            parameter_slots.push(CompiledFunctionSlot {
                name,
                slot,
                ty: sql_type,
            });
        }
    }

    let found_slot = env.define_var("found", SqlType::new(SqlTypeKind::Bool));
    let (sqlstate_slot, sqlerrm_slot) = env.define_exception_slots();
    env.push_label_scope(&row.proname);

    let return_contract = function_return_contract(row, catalog, &output_slots)?;
    let body = compile_block(&block, catalog, &mut env, Some(&return_contract))?;
    let context_arg_type_names = input_type_oids
        .iter()
        .map(|oid| crate::backend::executor::expr_reg::format_type_text(*oid, None, catalog))
        .collect();
    Ok(CompiledFunction {
        name: row.proname.clone(),
        proc_oid: row.oid,
        proowner: row.proowner,
        prosecdef: row.prosecdef,
        provolatile: row.provolatile,
        proconfig: row.proconfig.clone(),
        print_strict_params,
        parameter_slots,
        context_arg_type_names,
        output_slots,
        body,
        return_contract,
        found_slot,
        sqlstate_slot,
        sqlerrm_slot,
        local_ctes: Vec::new(),
        trigger_transition_ctes: Vec::new(),
    })
}

pub(crate) fn compile_trigger_function_from_proc(
    row: &PgProcRow,
    relation_desc: &RelationDesc,
    transition_tables: &[TriggerTransitionTable],
    catalog: &dyn CatalogLookup,
    gucs: Option<&HashMap<String, String>>,
) -> Result<CompiledFunction, ParseError> {
    let block = parse_block(&row.prosrc)?;
    let print_strict_params = print_strict_params_directive(&row.prosrc);
    let mut env = CompileEnv::default();
    env.variable_conflict = variable_conflict_mode(&row.prosrc, gucs);
    env.nonstandard_string_literals = nonstandard_string_literals_from_gucs(gucs);
    let mut trigger_transition_ctes = Vec::new();
    env.local_ctes = transition_tables
        .iter()
        .map(|table| {
            let cte = crate::backend::parser::bound_cte_from_materialized_rows(
                table.name.clone(),
                &table.desc,
                &[],
            );
            trigger_transition_ctes.push(CompiledTriggerTransitionCte {
                name: table.name.clone(),
                cte_id: cte.cte_id,
            });
            cte
        })
        .collect();
    let bindings = seed_trigger_env(&mut env, relation_desc);
    let found_slot = env.define_var("found", SqlType::new(SqlTypeKind::Bool));
    let (sqlstate_slot, sqlerrm_slot) = env.define_exception_slots();
    env.push_label_scope(&row.proname);
    let return_contract = FunctionReturnContract::Trigger { bindings };
    let body = compile_block(&block, catalog, &mut env, Some(&return_contract))?;
    Ok(CompiledFunction {
        name: row.proname.clone(),
        proc_oid: row.oid,
        proowner: row.proowner,
        prosecdef: row.prosecdef,
        provolatile: row.provolatile,
        proconfig: row.proconfig.clone(),
        print_strict_params,
        parameter_slots: Vec::new(),
        context_arg_type_names: Vec::new(),
        output_slots: Vec::new(),
        body,
        return_contract,
        found_slot,
        sqlstate_slot,
        sqlerrm_slot,
        local_ctes: env.local_ctes.clone(),
        trigger_transition_ctes,
    })
}

pub(crate) fn compile_event_trigger_function_from_proc(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
) -> Result<CompiledFunction, ParseError> {
    let block = parse_block(&row.prosrc)?;
    let mut env = CompileEnv::default();
    let bindings = seed_event_trigger_env(&mut env);
    let found_slot = env.define_var("found", SqlType::new(SqlTypeKind::Bool));
    let (sqlstate_slot, sqlerrm_slot) = env.define_exception_slots();
    let return_contract = FunctionReturnContract::EventTrigger { bindings };
    let body = compile_block(&block, catalog, &mut env, Some(&return_contract))?;
    Ok(CompiledFunction {
        name: row.proname.clone(),
        proc_oid: row.oid,
        proowner: row.proowner,
        prosecdef: row.prosecdef,
        provolatile: row.provolatile,
        proconfig: row.proconfig.clone(),
        print_strict_params: None,
        parameter_slots: Vec::new(),
        context_arg_type_names: Vec::new(),
        output_slots: Vec::new(),
        body,
        return_contract,
        found_slot,
        sqlstate_slot,
        sqlerrm_slot,
        local_ctes: Vec::new(),
        trigger_transition_ctes: Vec::new(),
    })
}

fn function_return_contract(
    row: &PgProcRow,
    catalog: &dyn CatalogLookup,
    output_slots: &[CompiledOutputSlot],
) -> Result<FunctionReturnContract, ParseError> {
    if row.prokind == 'p' {
        return if output_slots.is_empty() {
            Ok(FunctionReturnContract::Scalar {
                ty: SqlType::new(SqlTypeKind::Void),
                setof: false,
                output_slot: None,
            })
        } else {
            Ok(FunctionReturnContract::FixedRow {
                columns: output_slots
                    .iter()
                    .map(|slot| slot.column.clone())
                    .collect(),
                setof: false,
                uses_output_vars: true,
                composite_typrelid: None,
            })
        };
    }

    let result_type = catalog
        .type_by_oid(row.prorettype)
        .map(|ty| ty.sql_type)
        .unwrap_or_else(|| SqlType::record(RECORD_TYPE_OID));
    if row.proretset {
        return Ok(match result_type.kind {
            SqlTypeKind::Record => {
                if output_slots.is_empty() {
                    FunctionReturnContract::AnonymousRecord { setof: true }
                } else {
                    FunctionReturnContract::FixedRow {
                        columns: output_slots
                            .iter()
                            .map(|slot| slot.column.clone())
                            .collect(),
                        setof: true,
                        uses_output_vars: true,
                        composite_typrelid: None,
                    }
                }
            }
            SqlTypeKind::Composite => {
                let relation = catalog
                    .lookup_relation_by_oid(result_type.typrelid)
                    .ok_or_else(|| ParseError::UnsupportedType(result_type.typrelid.to_string()))?;
                FunctionReturnContract::FixedRow {
                    columns: relation
                        .desc
                        .columns
                        .into_iter()
                        .filter(|column| !column.dropped)
                        .map(|column| QueryColumn {
                            name: column.name,
                            sql_type: column.sql_type,
                            wire_type_oid: None,
                        })
                        .collect(),
                    setof: true,
                    uses_output_vars: false,
                    composite_typrelid: Some(result_type.typrelid),
                }
            }
            _ => FunctionReturnContract::Scalar {
                ty: result_type,
                setof: true,
                output_slot: output_slots.first().map(|slot| slot.slot),
            },
        });
    }

    match result_type.kind {
        SqlTypeKind::Trigger => Err(ParseError::FeatureNotSupported(
            "trigger functions cannot be called in SQL expressions".into(),
        )),
        SqlTypeKind::Record if !output_slots.is_empty() => Ok(FunctionReturnContract::FixedRow {
            columns: output_slots
                .iter()
                .map(|slot| slot.column.clone())
                .collect(),
            setof: false,
            uses_output_vars: true,
            composite_typrelid: None,
        }),
        SqlTypeKind::Record => Ok(FunctionReturnContract::AnonymousRecord { setof: false }),
        SqlTypeKind::Composite => {
            let relation = catalog
                .lookup_relation_by_oid(result_type.typrelid)
                .ok_or_else(|| ParseError::UnsupportedType(result_type.typrelid.to_string()))?;
            Ok(FunctionReturnContract::FixedRow {
                columns: relation
                    .desc
                    .columns
                    .into_iter()
                    .filter(|column| !column.dropped)
                    .map(|column| QueryColumn {
                        name: column.name,
                        sql_type: column.sql_type,
                        wire_type_oid: None,
                    })
                    .collect(),
                setof: false,
                uses_output_vars: false,
                composite_typrelid: Some(result_type.typrelid),
            })
        }
        _ => Ok(FunctionReturnContract::Scalar {
            ty: result_type,
            setof: false,
            output_slot: output_slots.first().map(|slot| slot.slot),
        }),
    }
}

fn compile_block(
    block: &Block,
    catalog: &dyn CatalogLookup,
    outer: &mut CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledBlock, ParseError> {
    let mut env = outer.child();
    let mut local_slots = Vec::new();
    for decl in &block.declarations {
        match decl {
            Decl::Var(decl) => local_slots.push(compile_var_decl(decl, catalog, &mut env)?),
            Decl::Cursor(decl) => local_slots.push(compile_cursor_decl(decl, catalog, &mut env)?),
            Decl::Alias(decl) => compile_alias_decl(decl, &mut env)?,
        }
    }
    if let Some(label) = &block.label {
        env.push_label_scope(label);
    }
    let statements = block
        .statements
        .iter()
        .map(|stmt| compile_stmt(stmt, catalog, &mut env, return_contract))
        .collect::<Result<Vec<_>, _>>()?;
    let exception_handlers = block
        .exception_handlers
        .iter()
        .map(|handler| {
            let statements = env.with_exception_vars(|handler_env| {
                compile_stmt_list(&handler.statements, catalog, handler_env, return_contract)
            })?;
            Ok(CompiledExceptionHandler {
                conditions: handler.conditions.clone(),
                statements,
            })
        })
        .collect::<Result<Vec<_>, ParseError>>()?;
    outer.next_slot = outer.next_slot.max(env.next_slot);
    Ok(CompiledBlock {
        local_slots,
        statements,
        exception_handlers,
        exception_sqlstate_slot: env.exception_sqlstate.as_ref().map(|var| var.slot),
        exception_sqlerrm_slot: env.exception_sqlerrm.as_ref().map(|var| var.slot),
        total_slots: outer.next_slot,
    })
}

fn compile_var_decl(
    decl: &VarDecl,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledVar, ParseError> {
    let ty = resolve_decl_type(&decl.type_name, catalog, env)?;
    let default_expr = decl
        .default_expr
        .as_deref()
        .map(|expr| compile_assignment_expr_text(expr, catalog, env))
        .transpose()?;
    let slot = env.define_var_with_options(&decl.name, ty, decl.constant, decl.strict);
    Ok(CompiledVar {
        name: decl.name.clone(),
        slot,
        ty,
        default_expr,
        not_null: decl.strict,
        line: decl.line,
    })
}

fn compile_cursor_decl(
    decl: &CursorDecl,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledVar, ParseError> {
    let ty = SqlType::new(SqlTypeKind::Text)
        .with_identity(crate::include::catalog::REFCURSOR_TYPE_OID, 0);
    let slot = env.define_var(&decl.name, ty);
    env.define_cursor(
        &decl.name,
        &decl.query,
        decl.scrollable,
        decl.params
            .iter()
            .map(|param| DeclaredCursorParam {
                name: param.name.clone(),
                type_name: param.type_name.clone(),
                ty: param.ty,
            })
            .collect(),
    );
    Ok(CompiledVar {
        name: decl.name.clone(),
        slot,
        ty,
        default_expr: Some(compile_expr_text(
            &format!("'{}'", decl.name.replace('\'', "''")),
            catalog,
            env,
        )?),
        not_null: false,
        line: 1,
    })
}

fn resolve_decl_type(
    type_name: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<SqlType, ParseError> {
    let trimmed = type_name.trim();
    let lowered = trimmed.to_ascii_lowercase();
    if let Some(prefix) = lowered.strip_suffix("%type") {
        let original_prefix = &trimmed[..prefix.len()];
        if !original_prefix.contains('.')
            && let Some(var) = env.get_var(original_prefix.trim())
        {
            return Ok(var.ty);
        }
        let Some((relation_name, column_name)) = original_prefix.trim().rsplit_once('.') else {
            return Err(ParseError::UnexpectedToken {
                expected: "PL/pgSQL %TYPE reference in relation.column form",
                actual: type_name.into(),
            });
        };
        let relation = catalog
            .lookup_any_relation(relation_name.trim())
            .ok_or_else(|| ParseError::UnsupportedType(relation_name.trim().into()))?;
        return relation
            .desc
            .columns
            .iter()
            .find(|column| !column.dropped && column.name.eq_ignore_ascii_case(column_name.trim()))
            .map(|column| column.sql_type)
            .ok_or_else(|| ParseError::UnknownColumn(original_prefix.trim().into()));
    }
    if let Some(prefix) = lowered.strip_suffix("%rowtype") {
        let relation_name = &trimmed[..prefix.len()];
        let relation = catalog
            .lookup_any_relation(relation_name.trim())
            .ok_or_else(|| ParseError::UnsupportedType(relation_name.trim().into()))?;
        return Ok(relation_row_type(&relation, catalog));
    }
    resolve_raw_type_name(&parse_type_name(trimmed)?, catalog)
}

fn relation_row_type(
    relation: &crate::backend::parser::BoundRelation,
    catalog: &dyn CatalogLookup,
) -> SqlType {
    catalog
        .type_rows()
        .into_iter()
        .find(|row| row.typrelid == relation.relation_oid)
        .map(|row| SqlType::named_composite(row.oid, relation.relation_oid))
        .unwrap_or_else(|| SqlType::record(RECORD_TYPE_OID))
}

fn compile_alias_decl(
    decl: &super::ast::AliasDecl,
    env: &mut CompileEnv,
) -> Result<(), ParseError> {
    match decl.target {
        AliasTarget::Parameter(index) => {
            let parameter =
                env.get_parameter(index)
                    .cloned()
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "function parameter referenced by ALIAS FOR",
                        actual: format!("${index}"),
                    })?;
            env.define_alias(&decl.name, parameter.slot, parameter.ty);
        }
        AliasTarget::New => {
            if !env.define_relation_alias(&decl.name, TriggerReturnedRow::New) {
                return Err(ParseError::UnexpectedToken {
                    expected: "trigger NEW row available for ALIAS FOR",
                    actual: "NEW".into(),
                });
            }
        }
        AliasTarget::Old => {
            if !env.define_relation_alias(&decl.name, TriggerReturnedRow::Old) {
                return Err(ParseError::UnexpectedToken {
                    expected: "trigger OLD row available for ALIAS FOR",
                    actual: "OLD".into(),
                });
            }
        }
    }
    Ok(())
}

fn compile_stmt(
    stmt: &Stmt,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledStmt, ParseError> {
    Ok(match stmt {
        Stmt::WithLine { line, stmt } => CompiledStmt::WithLine {
            line: *line,
            stmt: Box::new(compile_stmt(stmt, catalog, env, return_contract)?),
        },
        Stmt::Block(block) => {
            CompiledStmt::Block(compile_block(block, catalog, env, return_contract)?)
        }
        Stmt::Assign { target, expr, line } => {
            if let AssignTarget::Name(name) = target
                && let Some(row) = env.trigger_relation_return_row(name)
            {
                CompiledStmt::AssignTriggerRow {
                    row,
                    expr: compile_assignment_expr_text(expr, catalog, env)?,
                    line: *line,
                }
            } else if let Some(target) = compile_indirect_assign_target(target, catalog, env)? {
                CompiledStmt::AssignIndirect {
                    target,
                    expr: compile_assignment_expr_text(expr, catalog, env)?,
                    line: *line,
                }
            } else if let AssignTarget::Subscript { name, subscripts } = target {
                let (slot, root_ty, _, _) =
                    resolve_assign_target(&AssignTarget::Name(name.clone()), env)?;
                CompiledStmt::AssignSubscript {
                    slot,
                    root_ty,
                    target_ty: subscripted_assignment_target_type(
                        root_ty,
                        subscripts.len(),
                        catalog,
                    )?,
                    subscripts: subscripts
                        .iter()
                        .map(|subscript| compile_expr_text(subscript, catalog, env))
                        .collect::<Result<Vec<_>, _>>()?,
                    expr: compile_assignment_expr_text(expr, catalog, env)?,
                    line: *line,
                }
            } else {
                let (slot, ty, name, not_null) = resolve_assign_target(target, env)?;
                CompiledStmt::Assign {
                    slot,
                    ty,
                    name,
                    not_null,
                    expr: compile_assignment_expr_text(expr, catalog, env)?,
                    line: *line,
                }
            }
        }
        Stmt::Null => CompiledStmt::Null,
        Stmt::If {
            branches,
            else_branch,
        } => CompiledStmt::If {
            branches: branches
                .iter()
                .map(|(condition, body)| {
                    Ok((
                        compile_condition_text(condition, catalog, env)?,
                        compile_stmt_list(body, catalog, env, return_contract)?,
                    ))
                })
                .collect::<Result<_, ParseError>>()?,
            else_branch: compile_stmt_list(else_branch, catalog, env, return_contract)?,
        },
        Stmt::While { condition, body } => CompiledStmt::While {
            condition: compile_condition_text(condition, catalog, env)?,
            body: compile_stmt_list(body, catalog, env, return_contract)?,
        },
        Stmt::Loop { body } => CompiledStmt::Loop {
            body: compile_stmt_list(body, catalog, env, return_contract)?,
        },
        Stmt::Exit { condition } => CompiledStmt::Exit {
            condition: condition
                .as_deref()
                .map(|condition| compile_condition_text(condition, catalog, env))
                .transpose()?,
        },
        Stmt::ForInt {
            var_name,
            start_expr,
            end_expr,
            body,
        } => {
            let mut loop_env = env.child();
            let slot = loop_env.define_var(var_name, SqlType::new(SqlTypeKind::Int4));
            let body = compile_stmt_list(body, catalog, &mut loop_env, return_contract)?;
            env.next_slot = env.next_slot.max(loop_env.next_slot);
            CompiledStmt::ForInt {
                slot,
                start_expr: compile_expr_text(start_expr, catalog, env)?,
                end_expr: compile_expr_text(end_expr, catalog, env)?,
                body,
            }
        }
        Stmt::ForQuery {
            target,
            source,
            body,
        } => compile_for_query_stmt(target, source, body, catalog, env, return_contract)?,
        Stmt::ForEach {
            target,
            slice,
            array_expr,
            body,
        } => compile_foreach_stmt(
            target,
            *slice,
            array_expr,
            body,
            catalog,
            env,
            return_contract,
        )?,
        Stmt::Raise {
            level,
            condition,
            message,
            params,
            using_options,
        } => compile_raise_stmt(
            level,
            condition,
            message,
            params,
            using_options,
            catalog,
            env,
        )?,
        Stmt::Assert { condition, message } => CompiledStmt::Assert {
            condition: compile_condition_text(condition, catalog, env)?,
            message: message
                .as_deref()
                .map(|expr| compile_expr_text(expr, catalog, env))
                .transpose()?,
        },
        Stmt::Continue { condition } => CompiledStmt::Continue {
            condition: condition
                .as_deref()
                .map(|expr| compile_condition_text(expr, catalog, env))
                .transpose()?,
        },
        Stmt::Return { expr, line } => {
            compile_return_stmt(expr.as_deref(), *line, catalog, env, return_contract)?
        }
        Stmt::ReturnNext { expr } => {
            compile_return_next_stmt(expr.as_deref(), catalog, env, return_contract)?
        }
        Stmt::ReturnQuery { source } => {
            compile_return_query_stmt(source, catalog, env, return_contract)?
        }
        Stmt::Perform { sql, line } => compile_perform_stmt(sql, *line, catalog, env)?,
        Stmt::DynamicExecute {
            sql_expr,
            strict,
            into_targets,
            using_exprs,
            line,
        } => compile_dynamic_execute_stmt(
            sql_expr,
            *strict,
            into_targets,
            using_exprs,
            *line,
            catalog,
            env,
        )?,
        Stmt::GetDiagnostics { stacked, items } => {
            let items = items
                .iter()
                .map(|(target, item)| Ok((compile_select_into_target(target, env)?, item.clone())))
                .collect::<Result<Vec<_>, ParseError>>()?;
            CompiledStmt::GetDiagnostics {
                stacked: *stacked,
                items,
            }
        }
        Stmt::OpenCursor { name, source } => compile_open_cursor_stmt(name, source, catalog, env)?,
        Stmt::FetchCursor {
            name,
            direction,
            targets,
        } => {
            let (slot, _, _, _) = resolve_assign_target(&AssignTarget::Name(name.clone()), env)?;
            let cursor_shape = env.open_cursor_shapes.get(&slot).cloned();
            let mut targets = targets
                .iter()
                .map(|target| compile_select_into_target(target, env))
                .collect::<Result<Vec<_>, _>>()?;
            apply_cursor_shape_to_fetch_targets(&mut targets, cursor_shape.as_deref(), env);
            CompiledStmt::FetchCursor {
                slot,
                direction: *direction,
                targets,
            }
        }
        Stmt::MoveCursor { name, direction } => {
            let (slot, _, _, _) = resolve_assign_target(&AssignTarget::Name(name.clone()), env)?;
            CompiledStmt::MoveCursor {
                slot,
                direction: *direction,
            }
        }
        Stmt::CloseCursor { name } => {
            let (slot, _, _, _) = resolve_assign_target(&AssignTarget::Name(name.clone()), env)?;
            CompiledStmt::CloseCursor { slot }
        }
        Stmt::ExecSql { sql } => compile_exec_sql_stmt(sql, catalog, env)?,
    })
}

fn compile_raise_stmt(
    level: &RaiseLevel,
    condition: &Option<RaiseCondition>,
    message: &Option<String>,
    params: &[String],
    using_options: &[RaiseUsingOption],
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    if condition.is_none() && message.is_none() && params.is_empty() && using_options.is_empty() {
        return Ok(CompiledStmt::Reraise);
    }

    let mut sqlstate = None::<String>;
    let mut default_message = None::<String>;
    let condition_sets_errcode = condition.is_some();
    match condition {
        Some(RaiseCondition::SqlState(value)) => {
            sqlstate = Some(value.clone());
            default_message = Some(value.clone());
        }
        Some(RaiseCondition::ConditionName(name)) => {
            sqlstate = Some(
                exception_condition_name_sqlstate(name)
                    .unwrap_or("P0001")
                    .to_string(),
            );
            default_message = Some(name.clone());
        }
        None => {}
    }

    let mut message_expr = None::<String>;
    let mut detail_expr = None::<String>;
    let mut hint_expr = None::<String>;
    let mut errcode_expr = None::<String>;
    let mut column_expr = None::<String>;
    let mut constraint_expr = None::<String>;
    let mut datatype_expr = None::<String>;
    let mut table_expr = None::<String>;
    let mut schema_expr = None::<String>;
    for option in using_options {
        match option.name.to_ascii_lowercase().as_str() {
            "message" => {
                if message.is_some() || message_expr.is_some() {
                    return duplicate_raise_option("MESSAGE");
                }
                message_expr = Some(option.expr.clone());
            }
            "detail" => {
                if detail_expr.is_some() {
                    return duplicate_raise_option("DETAIL");
                }
                detail_expr = Some(option.expr.clone());
            }
            "hint" => {
                if hint_expr.is_some() {
                    return duplicate_raise_option("HINT");
                }
                hint_expr = Some(option.expr.clone());
            }
            "errcode" => {
                if condition_sets_errcode || errcode_expr.is_some() {
                    return duplicate_raise_option("ERRCODE");
                }
                errcode_expr = Some(option.expr.clone());
            }
            "column" | "column_name" => {
                if column_expr.is_some() {
                    return duplicate_raise_option("COLUMN");
                }
                column_expr = Some(option.expr.clone());
            }
            "constraint" | "constraint_name" => {
                if constraint_expr.is_some() {
                    return duplicate_raise_option("CONSTRAINT");
                }
                constraint_expr = Some(option.expr.clone());
            }
            "datatype" | "datatype_name" => {
                if datatype_expr.is_some() {
                    return duplicate_raise_option("DATATYPE");
                }
                datatype_expr = Some(option.expr.clone());
            }
            "table" | "table_name" => {
                if table_expr.is_some() {
                    return duplicate_raise_option("TABLE");
                }
                table_expr = Some(option.expr.clone());
            }
            "schema" | "schema_name" => {
                if schema_expr.is_some() {
                    return duplicate_raise_option("SCHEMA");
                }
                schema_expr = Some(option.expr.clone());
            }
            _ => {}
        }
    }

    let message = message
        .as_ref()
        .map(|message| {
            if env.nonstandard_string_literals {
                decode_nonstandard_backslash_escapes(message)
            } else {
                message.clone()
            }
        })
        .or(default_message)
        .or_else(|| {
            if message_expr.is_none() {
                Some(sqlstate.clone().unwrap_or_else(|| "P0001".into()))
            } else {
                None
            }
        });

    if let Some(message) = &message {
        let placeholder_count = count_raise_placeholders(message);
        if placeholder_count != params.len() {
            return Err(ParseError::UnexpectedToken {
                expected: "RAISE placeholder count matching argument count",
                actual: format!(
                    "message has {placeholder_count} placeholders but {} arguments were provided",
                    params.len()
                ),
            });
        }
    } else if !params.is_empty() {
        return Err(ParseError::UnexpectedToken {
            expected: "RAISE format string before parameter list",
            actual: format!("{params:?}"),
        });
    }

    Ok(CompiledStmt::Raise {
        line: 1,
        level: level.clone(),
        sqlstate,
        message,
        message_expr: message_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        detail_expr: detail_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        hint_expr: hint_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        errcode_expr: errcode_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        column_expr: column_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        constraint_expr: constraint_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        datatype_expr: datatype_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        table_expr: table_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        schema_expr: schema_expr
            .as_deref()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .transpose()?,
        params: params
            .iter()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .collect::<Result<_, _>>()?,
    })
}

fn duplicate_raise_option<T>(name: &str) -> Result<T, ParseError> {
    Err(ParseError::UnexpectedToken {
        expected: "RAISE option specified once",
        actual: format!("RAISE option already specified: {name}"),
    })
}

fn compile_return_stmt(
    expr: Option<&str>,
    line: usize,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledStmt, ParseError> {
    let Some(contract) = return_contract else {
        return Err(ParseError::FeatureNotSupported(
            "RETURN is only supported inside CREATE FUNCTION".into(),
        ));
    };
    match (contract, expr) {
        (FunctionReturnContract::Trigger { .. }, Some(expr))
            if env.trigger_relation_return_row(expr.trim()).is_some() =>
        {
            Ok(CompiledStmt::ReturnTriggerRow {
                row: env
                    .trigger_relation_return_row(expr.trim())
                    .ok_or(ParseError::UnexpectedEof)?,
            })
        }
        (FunctionReturnContract::Trigger { .. }, Some(expr))
            if expr.trim().eq_ignore_ascii_case("null") =>
        {
            Ok(CompiledStmt::ReturnTriggerNull)
        }
        (FunctionReturnContract::Trigger { .. }, None) => Ok(CompiledStmt::ReturnTriggerNoValue),
        (FunctionReturnContract::Trigger { .. }, Some(_)) => Err(ParseError::FeatureNotSupported(
            "trigger RETURN expressions must be NEW, OLD, or NULL".into(),
        )),
        (FunctionReturnContract::EventTrigger { .. }, None) => {
            Ok(CompiledStmt::ReturnTriggerNoValue)
        }
        (FunctionReturnContract::EventTrigger { .. }, Some(_)) => Err(ParseError::DetailedError {
            message: "RETURN cannot have a parameter in function returning event_trigger".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        }),
        (
            FunctionReturnContract::Scalar {
                output_slot: Some(_),
                ..
            }
            | FunctionReturnContract::FixedRow {
                uses_output_vars: true,
                ..
            },
            Some(_),
        ) => Err(ParseError::DetailedError {
            message: "RETURN cannot have a parameter in function with OUT parameters".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        }),
        (
            FunctionReturnContract::Scalar {
                ty,
                output_slot: None,
                setof: false,
            },
            Some(_),
        ) if ty.kind == SqlTypeKind::Void => Err(ParseError::DetailedError {
            message: "RETURN cannot have a parameter in function returning void".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        }),
        (FunctionReturnContract::Scalar { setof: false, .. }, Some(expr)) => {
            if let Some((sql, plan)) = compile_return_select_expr(expr, catalog, env)? {
                return Ok(CompiledStmt::ReturnSelect { plan, sql, line });
            }
            if let Some(sql) = runtime_return_query_sql(expr, env)? {
                return Ok(CompiledStmt::ReturnRuntimeQuery {
                    sql,
                    scope: runtime_sql_scope(env),
                    line,
                });
            }
            Ok(CompiledStmt::Return {
                expr: Some(compile_expr_text(expr, catalog, env)?),
                line,
            })
        }
        (
            FunctionReturnContract::Scalar {
                ty,
                output_slot,
                setof,
                ..
            },
            None,
        ) if output_slot.is_some() || *setof || ty.kind == SqlTypeKind::Void => {
            Ok(CompiledStmt::Return { expr: None, line })
        }
        (FunctionReturnContract::FixedRow { .. }, None)
        | (FunctionReturnContract::AnonymousRecord { .. }, None) => {
            Ok(CompiledStmt::Return { expr: None, line })
        }
        (
            FunctionReturnContract::FixedRow { setof: false, .. }
            | FunctionReturnContract::AnonymousRecord { setof: false },
            Some(expr),
        ) => Ok(CompiledStmt::Return {
            expr: Some(compile_expr_text(expr, catalog, env)?),
            line,
        }),
        _ => Err(ParseError::FeatureNotSupported(
            "RETURN expr is only supported for scalar function returns".into(),
        )),
    }
}

fn runtime_return_query_sql(expr: &str, env: &CompileEnv) -> Result<Option<String>, ParseError> {
    let Some(from_idx) = find_keyword_at_top_level(expr, "from") else {
        return Ok(None);
    };
    let before_from = expr[..from_idx].trim();
    let after_from = expr[from_idx..].trim();
    if before_from.is_empty() || after_from.is_empty() {
        return Ok(None);
    }
    Ok(Some(rewrite_plpgsql_sql_text(
        &format!("select {before_from} {after_from}"),
        env,
    )?))
}

fn compile_return_next_stmt(
    expr: Option<&str>,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledStmt, ParseError> {
    let Some(contract) = return_contract else {
        return Err(ParseError::FeatureNotSupported(
            "RETURN NEXT is only supported inside CREATE FUNCTION".into(),
        ));
    };
    match (contract, expr) {
        (FunctionReturnContract::Trigger { .. }, _)
        | (FunctionReturnContract::EventTrigger { .. }, _) => Err(ParseError::FeatureNotSupported(
            "RETURN NEXT is not valid in trigger functions".into(),
        )),
        (FunctionReturnContract::Scalar { setof: true, .. }, Some(expr)) => {
            Ok(CompiledStmt::ReturnNext {
                expr: Some(compile_expr_text(expr, catalog, env)?),
            })
        }
        (FunctionReturnContract::FixedRow { setof: true, .. }, Some(expr))
        | (FunctionReturnContract::AnonymousRecord { setof: true }, Some(expr)) => {
            Ok(CompiledStmt::ReturnNext {
                expr: Some(compile_expr_text(expr, catalog, env)?),
            })
        }
        (
            FunctionReturnContract::Scalar {
                setof: true,
                output_slot: Some(_),
                ..
            },
            None,
        ) => Ok(CompiledStmt::ReturnNext { expr: None }),
        (
            FunctionReturnContract::FixedRow {
                setof: true,
                uses_output_vars: true,
                ..
            },
            None,
        ) => Ok(CompiledStmt::ReturnNext { expr: None }),
        _ => Err(ParseError::FeatureNotSupported(
            "RETURN NEXT is not valid for this function return contract".into(),
        )),
    }
}

fn plan_select_for_env(
    stmt: &crate::backend::parser::SelectStatement,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<PlannedStmt, ParseError> {
    validate_select_variable_conflicts(stmt, catalog, env)?;
    let stmt = normalize_plpgsql_select(stmt.clone(), env);
    pg_plan_query_with_outer_scopes_and_ctes_config(
        &stmt,
        catalog,
        &[outer_scope_for_sql(env)],
        &env.local_ctes,
        plpgsql_planner_config(),
    )
}

fn plan_values_for_env(
    stmt: &crate::backend::parser::ValuesStatement,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<PlannedStmt, ParseError> {
    let stmt = normalize_plpgsql_values(stmt.clone(), env);
    pg_plan_values_query_with_outer_scopes_and_ctes_config(
        &stmt,
        catalog,
        &[outer_scope_for_sql(env)],
        &env.local_ctes,
        plpgsql_planner_config(),
    )
}

fn plpgsql_planner_config() -> PlannerConfig {
    PlannerConfig {
        fold_constants: false,
        ..PlannerConfig::default()
    }
}

fn validate_select_variable_conflicts(
    stmt: &SelectStatement,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<(), ParseError> {
    if env.variable_conflict != PlpgsqlVariableConflict::Error {
        return Ok(());
    }
    let Some(from) = stmt.from.as_ref() else {
        return Ok(());
    };
    let mut from_columns = HashSet::new();
    collect_from_item_column_names(from, catalog, env, &mut from_columns);
    if from_columns.is_empty() {
        return Ok(());
    }
    let mut refs = Vec::new();
    collect_select_column_refs(stmt, &mut refs);
    for name in refs {
        if from_columns.contains(&name.to_ascii_lowercase()) && env.get_var(&name).is_some() {
            return Err(ambiguous_plpgsql_column_error(&name));
        }
    }
    Ok(())
}

fn ambiguous_plpgsql_column_error(name: &str) -> ParseError {
    ParseError::DetailedError {
        message: format!("column reference \"{name}\" is ambiguous"),
        detail: Some("It could refer to either a PL/pgSQL variable or a table column.".into()),
        hint: None,
        sqlstate: "42702",
    }
}

fn collect_from_item_column_names(
    item: &FromItem,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
    names: &mut HashSet<String>,
) {
    match item {
        FromItem::Table { name, .. } => {
            if let Some(cte) = env
                .local_ctes
                .iter()
                .find(|cte| cte.name.eq_ignore_ascii_case(name))
            {
                names.extend(
                    cte.desc
                        .columns
                        .iter()
                        .filter(|column| !column.dropped)
                        .map(|column| column.name.to_ascii_lowercase()),
                );
                return;
            }
            if let Some(relation) = catalog.lookup_any_relation(name) {
                names.extend(
                    relation
                        .desc
                        .columns
                        .iter()
                        .filter(|column| !column.dropped)
                        .map(|column| column.name.to_ascii_lowercase()),
                );
            }
        }
        FromItem::Alias {
            source,
            column_aliases,
            ..
        } => match column_aliases {
            AliasColumnSpec::Names(alias_names) if !alias_names.is_empty() => {
                names.extend(alias_names.iter().map(|name| name.to_ascii_lowercase()));
            }
            AliasColumnSpec::Definitions(defs) if !defs.is_empty() => {
                names.extend(defs.iter().map(|def| def.name.to_ascii_lowercase()));
            }
            _ => collect_from_item_column_names(source, catalog, env, names),
        },
        FromItem::Join { left, right, .. } => {
            collect_from_item_column_names(left, catalog, env, names);
            collect_from_item_column_names(right, catalog, env, names);
        }
        FromItem::Lateral(source) => collect_from_item_column_names(source, catalog, env, names),
        _ => {}
    }
}

fn collect_select_column_refs(stmt: &SelectStatement, refs: &mut Vec<String>) {
    for target in &stmt.targets {
        collect_expr_column_refs(&target.expr, refs);
    }
    if let Some(expr) = &stmt.where_clause {
        collect_expr_column_refs(expr, refs);
    }
    for item in &stmt.group_by {
        collect_group_by_item_column_refs(item, refs);
    }
    if let Some(expr) = &stmt.having {
        collect_expr_column_refs(expr, refs);
    }
    for item in &stmt.order_by {
        collect_expr_column_refs(&item.expr, refs);
    }
}

fn collect_group_by_item_column_refs(item: &GroupByItem, refs: &mut Vec<String>) {
    match item {
        GroupByItem::Expr(expr) => collect_expr_column_refs(expr, refs),
        GroupByItem::List(exprs) => {
            for expr in exprs {
                collect_expr_column_refs(expr, refs);
            }
        }
        GroupByItem::Empty => {}
        GroupByItem::Rollup(items) | GroupByItem::Cube(items) | GroupByItem::Sets(items) => {
            for item in items {
                collect_group_by_item_column_refs(item, refs);
            }
        }
    }
}

fn collect_expr_column_refs(expr: &SqlExpr, refs: &mut Vec<String>) {
    match expr {
        SqlExpr::Column(name) if !name.contains('.') && !is_internal_plpgsql_name(name) => {
            refs.push(name.clone());
        }
        SqlExpr::Add(left, right)
        | SqlExpr::Sub(left, right)
        | SqlExpr::BitAnd(left, right)
        | SqlExpr::BitOr(left, right)
        | SqlExpr::BitXor(left, right)
        | SqlExpr::Shl(left, right)
        | SqlExpr::Shr(left, right)
        | SqlExpr::Mul(left, right)
        | SqlExpr::Div(left, right)
        | SqlExpr::Mod(left, right)
        | SqlExpr::Concat(left, right)
        | SqlExpr::Eq(left, right)
        | SqlExpr::NotEq(left, right)
        | SqlExpr::Lt(left, right)
        | SqlExpr::LtEq(left, right)
        | SqlExpr::Gt(left, right)
        | SqlExpr::GtEq(left, right)
        | SqlExpr::RegexMatch(left, right)
        | SqlExpr::And(left, right)
        | SqlExpr::Or(left, right)
        | SqlExpr::IsDistinctFrom(left, right)
        | SqlExpr::IsNotDistinctFrom(left, right)
        | SqlExpr::Overlaps(left, right)
        | SqlExpr::ArrayOverlap(left, right)
        | SqlExpr::ArrayContains(left, right)
        | SqlExpr::ArrayContained(left, right)
        | SqlExpr::JsonbContains(left, right)
        | SqlExpr::JsonbContained(left, right)
        | SqlExpr::JsonbExists(left, right)
        | SqlExpr::JsonbExistsAny(left, right)
        | SqlExpr::JsonbExistsAll(left, right)
        | SqlExpr::JsonbPathExists(left, right)
        | SqlExpr::JsonbPathMatch(left, right)
        | SqlExpr::JsonGet(left, right)
        | SqlExpr::JsonGetText(left, right)
        | SqlExpr::JsonPath(left, right)
        | SqlExpr::JsonPathText(left, right) => {
            collect_expr_column_refs(left, refs);
            collect_expr_column_refs(right, refs);
        }
        SqlExpr::BinaryOperator { left, right, .. } => {
            collect_expr_column_refs(left, refs);
            collect_expr_column_refs(right, refs);
        }
        SqlExpr::UnaryPlus(expr)
        | SqlExpr::Negate(expr)
        | SqlExpr::BitNot(expr)
        | SqlExpr::PrefixOperator { expr, .. }
        | SqlExpr::Cast(expr, _)
        | SqlExpr::Not(expr)
        | SqlExpr::IsNull(expr)
        | SqlExpr::IsNotNull(expr)
        | SqlExpr::FieldSelect { expr, .. } => collect_expr_column_refs(expr, refs),
        SqlExpr::Subscript { expr, .. } => collect_expr_column_refs(expr, refs),
        SqlExpr::GeometryUnaryOp { expr, .. } => collect_expr_column_refs(expr, refs),
        SqlExpr::GeometryBinaryOp { left, right, .. } => {
            collect_expr_column_refs(left, refs);
            collect_expr_column_refs(right, refs);
        }
        SqlExpr::Collate { expr, .. } => collect_expr_column_refs(expr, refs),
        SqlExpr::AtTimeZone { expr, zone } => {
            collect_expr_column_refs(expr, refs);
            collect_expr_column_refs(zone, refs);
        }
        SqlExpr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | SqlExpr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            collect_expr_column_refs(expr, refs);
            collect_expr_column_refs(pattern, refs);
            if let Some(escape) = escape {
                collect_expr_column_refs(escape, refs);
            }
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                collect_expr_column_refs(arg, refs);
            }
            for arm in args {
                collect_expr_column_refs(&arm.expr, refs);
                collect_expr_column_refs(&arm.result, refs);
            }
            if let Some(defresult) = defresult {
                collect_expr_column_refs(defresult, refs);
            }
        }
        SqlExpr::ArrayLiteral(items) | SqlExpr::Row(items) => {
            for item in items {
                collect_expr_column_refs(item, refs);
            }
        }
        SqlExpr::QuantifiedArray { left, array, .. } => {
            collect_expr_column_refs(left, refs);
            collect_expr_column_refs(array, refs);
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            collect_expr_column_refs(array, refs);
            for subscript in subscripts {
                if let Some(lower) = &subscript.lower {
                    collect_expr_column_refs(lower, refs);
                }
                if let Some(upper) = &subscript.upper {
                    collect_expr_column_refs(upper, refs);
                }
            }
        }
        SqlExpr::Xml(xml) => {
            for child in xml.child_exprs() {
                collect_expr_column_refs(child, refs);
            }
        }
        SqlExpr::JsonQueryFunction(func) => {
            for child in func.child_exprs() {
                collect_expr_column_refs(child, refs);
            }
        }
        SqlExpr::FuncCall {
            args,
            order_by,
            within_group,
            filter,
            over,
            ..
        } => {
            for arg in args.args() {
                collect_expr_column_refs(&arg.value, refs);
            }
            for item in order_by {
                collect_expr_column_refs(&item.expr, refs);
            }
            if let Some(within_group) = within_group {
                for item in within_group {
                    collect_expr_column_refs(&item.expr, refs);
                }
            }
            if let Some(filter) = filter {
                collect_expr_column_refs(filter, refs);
            }
            if let Some(over) = over {
                collect_window_column_refs(over, refs);
            }
        }
        _ => {}
    }
}

fn collect_window_column_refs(spec: &RawWindowSpec, refs: &mut Vec<String>) {
    for expr in &spec.partition_by {
        collect_expr_column_refs(expr, refs);
    }
    for item in &spec.order_by {
        collect_expr_column_refs(&item.expr, refs);
    }
    if let Some(frame) = &spec.frame {
        collect_window_frame_bound_refs(&frame.start_bound, refs);
        collect_window_frame_bound_refs(&frame.end_bound, refs);
    }
}

fn collect_window_frame_bound_refs(bound: &RawWindowFrameBound, refs: &mut Vec<String>) {
    match bound {
        RawWindowFrameBound::OffsetPreceding(expr) | RawWindowFrameBound::OffsetFollowing(expr) => {
            collect_expr_column_refs(expr, refs)
        }
        RawWindowFrameBound::UnboundedPreceding
        | RawWindowFrameBound::CurrentRow
        | RawWindowFrameBound::UnboundedFollowing => {}
    }
}

fn compile_static_query_source(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
    expected: &'static str,
) -> Result<PlannedStmt, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    match parse_statement(&rewritten_sql)? {
        Statement::Select(stmt) => plan_select_for_env(&stmt, catalog, env),
        Statement::Values(stmt) => plan_values_for_env(&stmt, catalog, env),
        other => Err(ParseError::UnexpectedToken {
            expected,
            actual: format!("{other:?}"),
        }),
    }
}

fn compile_return_query_stmt(
    source: &ForQuerySource,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledStmt, ParseError> {
    let Some(contract) = return_contract else {
        return Err(ParseError::FeatureNotSupported(
            "RETURN QUERY is only supported inside CREATE FUNCTION".into(),
        ));
    };
    let is_setof = match contract {
        FunctionReturnContract::Scalar { setof, .. }
        | FunctionReturnContract::FixedRow { setof, .. }
        | FunctionReturnContract::AnonymousRecord { setof } => *setof,
        FunctionReturnContract::Trigger { .. } | FunctionReturnContract::EventTrigger { .. } => {
            false
        }
    };
    if !is_setof {
        return Err(ParseError::FeatureNotSupported(
            "RETURN QUERY requires a set-returning function".into(),
        ));
    }

    let source = match source {
        ForQuerySource::Static(sql) => compile_return_query_static_source(sql, catalog, env)?,
        ForQuerySource::Execute {
            sql_expr,
            using_exprs,
        } => CompiledForQuerySource::Dynamic {
            sql_expr: compile_expr_text(sql_expr, catalog, env)?,
            using_exprs: using_exprs
                .iter()
                .map(|expr| compile_expr_text(expr, catalog, env))
                .collect::<Result<Vec<_>, _>>()?,
        },
        ForQuerySource::Cursor { .. } => {
            return Err(ParseError::UnexpectedToken {
                expected: "RETURN QUERY SELECT ..., VALUES (...), or EXECUTE ...",
                actual: "cursor query source".into(),
            });
        }
    };
    Ok(CompiledStmt::ReturnQuery { source })
}

fn compile_return_query_static_source(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledForQuerySource, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    match parse_statement(&rewritten_sql)? {
        Statement::Select(stmt) => Ok(CompiledForQuerySource::Static {
            plan: plan_select_for_env(&stmt, catalog, env)?,
        }),
        Statement::Values(stmt) => Ok(CompiledForQuerySource::Static {
            plan: plan_values_for_env(&stmt, catalog, env)?,
        }),
        Statement::CreateTableAs(_) => Ok(CompiledForQuerySource::NoTuples {
            sql: normalize_sql_context_text(&rewritten_sql),
        }),
        Statement::Unsupported(unsupported)
            if unsupported.feature == "SELECT form"
                && find_next_top_level_keyword(&unsupported.sql, &["into"]).is_some() =>
        {
            Ok(CompiledForQuerySource::NoTuples {
                sql: normalize_sql_context_text(&unsupported.sql),
            })
        }
        other => Err(ParseError::UnexpectedToken {
            expected: "RETURN QUERY SELECT ... or RETURN QUERY VALUES (...)",
            actual: format!("{other:?}"),
        }),
    }
}

fn normalize_sql_context_text(sql: &str) -> String {
    pgrust_plpgsql::normalize_sql_context_text(sql)
}

fn normalize_nonstandard_string_literals(sql: &str) -> String {
    pgrust_protocol::sql::normalize_nonstandard_string_literals(sql)
}

fn decode_nonstandard_backslash_escapes(value: &str) -> String {
    pgrust_plpgsql::decode_nonstandard_backslash_escapes(value)
}

fn compile_perform_stmt(
    sql: &str,
    line: usize,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    let planned = plan_select_for_env(
        &crate::backend::parser::parse_select(&format!("select {rewritten_sql}"))?,
        catalog,
        env,
    )?;
    Ok(CompiledStmt::Perform {
        plan: planned,
        line,
        sql: Some(format!("SELECT {}", sql.trim())),
    })
}

fn compile_dynamic_execute_stmt(
    sql_expr: &str,
    strict: bool,
    into_targets: &[AssignTarget],
    using_exprs: &[String],
    line: usize,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let mut targets = into_targets
        .iter()
        .map(|target| compile_select_into_target(target, env))
        .collect::<Result<Vec<_>, _>>()?;
    if let [target] = targets.as_mut_slice()
        && target.ty.kind == SqlTypeKind::Record
        && let Some(result_columns) =
            dynamic_sql_literal_result_columns(sql_expr, using_exprs, catalog, env)
    {
        let descriptor = assign_anonymous_record_descriptor(
            result_columns
                .iter()
                .map(|column| (column.name.clone(), column.sql_type))
                .collect(),
        );
        let ty = descriptor.sql_type();
        env.update_slot_type(target.slot, ty);
        target.ty = ty;
    }
    Ok(CompiledStmt::DynamicExecute {
        sql_expr: compile_expr_text(sql_expr, catalog, env)?,
        strict,
        into_targets: targets,
        using_exprs: using_exprs
            .iter()
            .map(|expr| compile_expr_text(expr, catalog, env))
            .collect::<Result<_, _>>()?,
        line,
    })
}

fn compile_cursor_open_source(
    name: &str,
    source: &OpenCursorSource,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<(CompiledCursorOpenSource, bool, Option<PlannedStmt>), ParseError> {
    match source {
        OpenCursorSource::Static(sql) => {
            let plan = compile_static_query_source(sql, catalog, env, "cursor query")?;
            Ok((
                CompiledCursorOpenSource::Static { plan: plan.clone() },
                true,
                Some(plan),
            ))
        }
        OpenCursorSource::Dynamic {
            sql_expr,
            using_exprs,
        } => Ok((
            CompiledCursorOpenSource::Dynamic {
                sql_expr: compile_expr_text(sql_expr, catalog, env)?,
                using_exprs: using_exprs
                    .iter()
                    .map(|expr| compile_expr_text(expr, catalog, env))
                    .collect::<Result<Vec<_>, _>>()?,
            },
            true,
            None,
        )),
        OpenCursorSource::Declared { args } => {
            let cursor =
                env.declared_cursor(name)
                    .cloned()
                    .ok_or_else(|| ParseError::UnexpectedToken {
                        expected: "declared cursor query or OPEN cursor FOR query",
                        actual: name.to_string(),
                    })?;
            let (args, arg_context) =
                compile_declared_cursor_args(name, args, &cursor.params, catalog, env)?;
            let shape_plan = plan_declared_cursor_query_for_shape(&cursor, catalog, env).ok();
            Ok((
                CompiledCursorOpenSource::Declared {
                    query: cursor.query,
                    params: cursor.params,
                    args,
                    arg_context,
                },
                cursor.scrollable,
                shape_plan,
            ))
        }
    }
}

fn compile_declared_cursor_args(
    cursor_name: &str,
    args: &[CursorArg],
    params: &[DeclaredCursorParam],
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<(Vec<CompiledExpr>, Option<String>), ParseError> {
    let mut assigned = vec![None::<String>; params.len()];
    for (arg_index, arg) in args.iter().enumerate() {
        match arg {
            CursorArg::Positional(expr) => {
                let Some(param) = params.get(arg_index) else {
                    return Err(ParseError::UnexpectedToken {
                        expected: "cursor argument",
                        actual: format!("too many arguments for cursor \"{cursor_name}\""),
                    });
                };
                if assigned[arg_index].is_some() {
                    return Err(duplicate_cursor_param_error(cursor_name, &param.name));
                }
                assigned[arg_index] = Some(expr.clone());
            }
            CursorArg::Named { name, expr } => {
                let Some(index) = params
                    .iter()
                    .position(|param| param.name.eq_ignore_ascii_case(name))
                else {
                    return Err(ParseError::UnexpectedToken {
                        expected: "cursor argument name",
                        actual: format!(
                            "cursor \"{cursor_name}\" has no argument named \"{name}\""
                        ),
                    });
                };
                if assigned[index].is_some() {
                    return Err(duplicate_cursor_param_error(
                        cursor_name,
                        &params[index].name,
                    ));
                }
                assigned[index] = Some(expr.clone());
            }
        }
    }
    if let Some(param) = params
        .iter()
        .zip(&assigned)
        .find_map(|(param, expr)| expr.is_none().then_some(param))
    {
        return Err(ParseError::UnexpectedToken {
            expected: "cursor argument",
            actual: format!(
                "not enough arguments for cursor \"{cursor_name}\"; missing \"{}\"",
                param.name
            ),
        });
    }
    let param_names = params
        .iter()
        .map(|param| param.name.clone())
        .collect::<Vec<_>>();
    let arg_context = declared_cursor_args_context(&assigned, &param_names);
    let args = assigned
        .into_iter()
        .map(|expr| compile_expr_text(&expr.expect("checked above"), catalog, env))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((args, arg_context))
}

fn duplicate_cursor_param_error(cursor_name: &str, param_name: &str) -> ParseError {
    ParseError::UnexpectedToken {
        expected: "cursor argument",
        actual: format!(
            "value for parameter \"{param_name}\" of cursor \"{cursor_name}\" specified more than once"
        ),
    }
}

fn plan_declared_cursor_query_for_shape(
    cursor: &DeclaredCursor,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<PlannedStmt, ParseError> {
    let sql = rewrite_declared_cursor_params_for_plan(&cursor.query, &cursor.params)?;
    compile_static_query_source(&sql, catalog, env, "cursor query")
}

fn compile_open_cursor_stmt(
    name: &str,
    source: &OpenCursorSource,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let var = env
        .get_var(name)
        .ok_or_else(|| ParseError::UnknownColumn(name.to_string()))?;
    let slot = var.slot;
    let constant = var.constant;
    let (source, scrollable, shape_plan) = compile_cursor_open_source(name, source, catalog, env)?;
    if let Some(plan) = shape_plan {
        env.open_cursor_shapes.insert(slot, plan.columns());
    } else {
        env.open_cursor_shapes.remove(&slot);
    }
    Ok(CompiledStmt::OpenCursor {
        slot,
        name: name.to_string(),
        source,
        scrollable,
        constant,
    })
}

fn compile_exec_sql_stmt(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    if let Some(name) = persistent_object_transition_table_reference(sql, &env.local_ctes) {
        return Err(ParseError::DetailedError {
            message: format!(
                "transition table \"{name}\" cannot be referenced in a persistent object"
            ),
            detail: None,
            hint: None,
            sqlstate: "0A000",
        });
    }

    if let Some((target_name, select_sql)) =
        split_cte_prefixed_select_into_target(sql).or_else(|| split_select_into_target(sql))
    {
        let targets = parse_select_into_assign_targets(&target_name)?;
        return match compile_select_into_stmt(&select_sql, &targets, false, catalog, env) {
            Ok(stmt) => Ok(stmt),
            Err(err) if should_fallback_to_runtime_sql(&err) => {
                compile_runtime_select_into_stmt(&select_sql, &targets, false, env)
            }
            Err(err) => Err(err),
        };
    }

    if let Some((target_names, select_sql, strict)) = split_select_with_into_targets(sql) {
        let targets = target_names
            .iter()
            .map(|target| parse_select_into_assign_target(target))
            .collect::<Result<Vec<_>, _>>()?;
        return match compile_select_into_stmt(&select_sql, &targets, strict, catalog, env) {
            Ok(stmt) => Ok(stmt),
            Err(err) if should_fallback_to_runtime_sql(&err) => {
                compile_runtime_select_into_stmt(&select_sql, &targets, strict, env)
            }
            Err(err) => Err(err),
        };
    }
    if let Some((exec_sql, target_names)) = split_dml_returning_into_targets(sql) {
        let targets = target_names
            .iter()
            .map(|target| parse_select_into_assign_target(target))
            .collect::<Result<Vec<_>, _>>()?;
        return compile_exec_returning_into_stmt(&exec_sql, &targets, catalog, env);
    }

    if is_unsupported_plpgsql_transaction_command(sql) {
        return Ok(CompiledStmt::UnsupportedTransactionCommand {
            command: transaction_command_name(sql).to_string(),
        });
    }

    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    let stmt = normalize_plpgsql_sql_statement(parse_statement(&rewritten_sql)?, env);
    let outer_scope = outer_scope_for_sql(env);
    let outer_scopes = [outer_scope];
    match stmt {
        Statement::Select(stmt) => Ok(CompiledStmt::Perform {
            plan: plan_select_for_env(&stmt, catalog, env)?,
            line: 1,
            sql: Some(rewritten_sql.clone()),
        }),
        Statement::Values(stmt) => Ok(CompiledStmt::Perform {
            plan: plan_values_for_env(&stmt, catalog, env)?,
            line: 1,
            sql: Some(rewritten_sql.clone()),
        }),
        Statement::Insert(stmt) => match bind_insert_with_outer_scopes(
            &normalize_plpgsql_insert(stmt, env),
            catalog,
            &outer_scopes,
        ) {
            Ok(stmt) => Ok(CompiledStmt::ExecInsert { stmt }),
            Err(err) if should_defer_plpgsql_sql_to_runtime(&err) => Ok(CompiledStmt::RuntimeSql {
                sql: rewritten_sql,
                scope: runtime_sql_scope(env),
            }),
            Err(err) => Err(err),
        },
        Statement::Update(stmt) => match bind_update_with_outer_scopes(
            &normalize_plpgsql_update(stmt, env),
            catalog,
            &outer_scopes,
        ) {
            Ok(stmt) => Ok(CompiledStmt::ExecUpdate { stmt }),
            Err(err) if should_defer_plpgsql_sql_to_runtime(&err) => Ok(CompiledStmt::RuntimeSql {
                sql: rewritten_sql,
                scope: runtime_sql_scope(env),
            }),
            Err(err) => Err(err),
        },
        Statement::Delete(stmt) => match bind_delete_with_outer_scopes(
            &normalize_plpgsql_delete(stmt, env),
            catalog,
            &outer_scopes,
        ) {
            Ok(stmt) => Ok(CompiledStmt::ExecDelete { stmt }),
            Err(err) if should_defer_plpgsql_sql_to_runtime(&err) => Ok(CompiledStmt::RuntimeSql {
                sql: rewritten_sql,
                scope: runtime_sql_scope(env),
            }),
            Err(err) => Err(err),
        },
        Statement::Merge(_) => Ok(CompiledStmt::RuntimeSql {
            sql: rewritten_sql,
            scope: runtime_sql_scope(env),
        }),
        Statement::CreateTable(stmt) if stmt.persistence == TablePersistence::Temporary => {
            Ok(CompiledStmt::ExecSql { sql: rewritten_sql })
        }
        Statement::CreateTable(stmt) => Ok(CompiledStmt::CreateTable { stmt }),
        Statement::CreateTableAs(stmt) => Ok(CompiledStmt::CreateTableAs { stmt }),
        Statement::Analyze(_) => Ok(CompiledStmt::ExecSql { sql: rewritten_sql }),
        Statement::CreateView(_) | Statement::DropTable(_) => Ok(CompiledStmt::RuntimeSql {
            sql: rewritten_sql,
            scope: runtime_sql_scope(env),
        }),
        Statement::Set(stmt) if stmt.name.eq_ignore_ascii_case("jit") => {
            // :HACK: pgrust has no JIT subsystem; PL/pgSQL regression helpers
            // use SET LOCAL jit=0 only to stabilize EXPLAIN.
            Ok(CompiledStmt::Null)
        }
        Statement::Set(stmt) => Ok(CompiledStmt::SetGuc {
            name: stmt.name,
            value: stmt.value,
            is_local: stmt.is_local,
        }),
        Statement::CommentOnFunction(stmt) => Ok(CompiledStmt::CommentOnFunction { stmt }),
        other => Err(ParseError::UnexpectedToken {
            expected: "PL/pgSQL SQL statement",
            actual: format!("{other:?}"),
        }),
    }
}

fn persistent_object_transition_table_reference(
    sql: &str,
    local_ctes: &[BoundCte],
) -> Option<String> {
    persistent_object_transition_table_reference_name(
        sql,
        local_ctes.iter().map(|cte| cte.name.as_str()),
    )
}

fn runtime_sql_scope(env: &CompileEnv) -> RuntimeSqlScope {
    RuntimeSqlScope {
        columns: env.slot_columns(),
        relation_scopes: env.relation_slot_scopes(),
    }
}

fn outer_scope_for_sql(env: &CompileEnv) -> BoundScope {
    runtime_sql_bound_scope(&runtime_sql_scope(env))
}

pub(crate) fn runtime_sql_bound_scope(scope: &RuntimeSqlScope) -> BoundScope {
    bound_scope_from_slot_columns(
        scope.columns.clone(),
        scope.relation_scopes.clone(),
        |column| {
            Expr::Var(Var {
                varno: 1,
                varattno: user_attrno(column.slot),
                varlevelsup: 0,
                vartype: column.sql_type,
                collation_oid: None,
            })
        },
    )
}

pub(crate) fn runtime_sql_param_bound_scope(scope: &RuntimeSqlScope) -> BoundScope {
    bound_scope_from_slot_columns(
        scope.columns.clone(),
        scope.relation_scopes.clone(),
        |column| {
            Expr::Param(Param {
                paramkind: ParamKind::External,
                paramid: runtime_sql_param_id(column.slot),
                paramtype: column.sql_type,
            })
        },
    )
}

fn bound_scope_from_slot_columns(
    columns: Vec<SlotScopeColumn>,
    relation_scopes: Vec<(String, Vec<SlotScopeColumn>)>,
    mut slot_expr: impl FnMut(&SlotScopeColumn) -> Expr,
) -> BoundScope {
    let desc = RelationDesc {
        columns: columns
            .iter()
            .map(|column| column_desc(column.name.clone(), column.sql_type, true))
            .chain(relation_scopes.iter().flat_map(|(_, columns)| {
                columns
                    .iter()
                    .map(|column| column_desc(column.name.clone(), column.sql_type, true))
            }))
            .collect(),
    };
    let mut output_exprs = columns.iter().map(&mut slot_expr).collect::<Vec<_>>();
    let mut scope_columns = columns
        .into_iter()
        .map(|column| crate::backend::parser::analyze::ScopeColumn {
            output_name: column.name,
            hidden: column.hidden,
            qualified_only: false,
            relation_names: Vec::new(),
            relation_output_exprs: Vec::new(),
            hidden_invalid_relation_names: Vec::new(),
            hidden_missing_relation_names: Vec::new(),
            source_relation_oid: None,
            source_attno: None,
            source_columns: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mut relations = Vec::new();
    for (relation_name, relation_columns) in relation_scopes {
        let relation_desc = RelationDesc {
            columns: relation_columns
                .iter()
                .map(|column| column_desc(column.name.clone(), column.sql_type, true))
                .collect(),
        };
        let mut relation_scope = scope_for_relation(Some(&relation_name), &relation_desc);
        for scope_column in &mut relation_scope.columns {
            scope_column.qualified_only = true;
        }
        relations.extend(relation_scope.relations);
        for column in relation_columns {
            output_exprs.push(slot_expr(&column));
        }
        scope_columns.extend(relation_scope.columns);
    }
    BoundScope {
        output_exprs,
        desc,
        columns: scope_columns,
        relations,
    }
}

fn rewrite_plpgsql_sql_text(sql: &str, env: &CompileEnv) -> Result<String, ParseError> {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut idx = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if in_single {
            out.push(ch);
            idx += 1;
            if ch == '\'' {
                if bytes.get(idx) == Some(&b'\'') {
                    out.push('\'');
                    idx += 1;
                    continue;
                }
                in_single = false;
            }
            continue;
        }
        if in_double {
            out.push(ch);
            idx += 1;
            if ch == '"' {
                if bytes.get(idx) == Some(&b'"') {
                    out.push('"');
                    idx += 1;
                    continue;
                }
                in_double = false;
            }
            continue;
        }
        if let Some(tag) = dollar_quote_tag_at(sql, idx) {
            if let Some(close) = sql[idx + tag.len()..].find(tag) {
                let end = idx + tag.len() + close + tag.len();
                out.push_str(&sql[idx..end]);
                idx = end;
            } else {
                out.push_str(&sql[idx..]);
                break;
            }
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                out.push(ch);
                idx += 1;
                continue;
            }
            '"' => {
                in_double = true;
                out.push(ch);
                idx += 1;
                continue;
            }
            '$' => {
                let mut end = idx + 1;
                while let Some(byte) = bytes.get(end) {
                    if !byte.is_ascii_digit() {
                        break;
                    }
                    end += 1;
                }
                if end > idx + 1 && (end == bytes.len() || !is_identifier_char(bytes[end] as char))
                {
                    let index = sql[idx + 1..end].parse::<usize>().map_err(|_| {
                        ParseError::UnexpectedToken {
                            expected: "valid positional parameter reference",
                            actual: sql[idx..end].to_string(),
                        }
                    })?;
                    let name = env.positional_parameter_name(index).ok_or_else(|| {
                        ParseError::UnexpectedToken {
                            expected: "existing positional parameter reference",
                            actual: sql[idx..end].to_string(),
                        }
                    })?;
                    out.push_str(name);
                    idx = end;
                    continue;
                }
            }
            _ => {}
        }

        out.push(ch);
        idx += 1;
    }
    Ok(out)
}

fn rewrite_declared_cursor_params_for_plan(
    sql: &str,
    params: &[DeclaredCursorParam],
) -> Result<String, ParseError> {
    if params.is_empty() {
        return Ok(sql.to_string());
    }
    rewrite_identifier_refs(sql, |ident| {
        params
            .iter()
            .find(|param| param.name.eq_ignore_ascii_case(ident))
            .map(|param| format!("(null::{})", param.type_name))
    })
}

fn rewrite_identifier_refs<F>(sql: &str, mut replacement: F) -> Result<String, ParseError>
where
    F: FnMut(&str) -> Option<String>,
{
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut idx = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    while idx < bytes.len() {
        let ch = bytes[idx] as char;
        if in_single {
            out.push(ch);
            idx += 1;
            if ch == '\'' {
                if bytes.get(idx) == Some(&b'\'') {
                    out.push('\'');
                    idx += 1;
                    continue;
                }
                in_single = false;
            }
            continue;
        }
        if in_double {
            out.push(ch);
            idx += 1;
            if ch == '"' {
                if bytes.get(idx) == Some(&b'"') {
                    out.push('"');
                    idx += 1;
                    continue;
                }
                in_double = false;
            }
            continue;
        }
        if let Some(tag) = dollar_quote_tag_at(sql, idx) {
            if let Some(close) = sql[idx + tag.len()..].find(tag) {
                let end = idx + tag.len() + close + tag.len();
                out.push_str(&sql[idx..end]);
                idx = end;
            } else {
                out.push_str(&sql[idx..]);
                break;
            }
            continue;
        }

        match ch {
            '\'' => {
                in_single = true;
                out.push(ch);
                idx += 1;
                continue;
            }
            '"' => {
                in_double = true;
                out.push(ch);
                idx += 1;
                continue;
            }
            _ if is_identifier_start(ch) => {
                let start = idx;
                idx += 1;
                while idx < bytes.len() && is_identifier_char(bytes[idx] as char) {
                    idx += 1;
                }
                let ident = &sql[start..idx];
                if let Some(value) = replacement(ident) {
                    out.push_str(&value);
                } else {
                    out.push_str(ident);
                }
                continue;
            }
            _ => {}
        }

        out.push(ch);
        idx += 1;
    }
    Ok(out)
}

fn compile_select_into_stmt(
    select_sql: &str,
    target_refs: &[AssignTarget],
    strict: bool,
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(select_sql, env)?;
    let planned = plan_select_for_env(
        &crate::backend::parser::parse_select(&rewritten_sql)?,
        catalog,
        env,
    )?;
    let mut targets = target_refs
        .iter()
        .map(|target| compile_select_into_target(target, env))
        .collect::<Result<Vec<_>, _>>()?;
    if let [target] = targets.as_mut_slice()
        && target.ty.kind == SqlTypeKind::Record
    {
        let descriptor = assign_anonymous_record_descriptor(
            planned
                .columns()
                .into_iter()
                .map(|column| (column.name, column.sql_type))
                .collect(),
        );
        let ty = descriptor.sql_type();
        env.update_slot_type(target.slot, ty);
        target.ty = ty;
    }
    Ok(CompiledStmt::SelectInto {
        plan: planned,
        targets,
        strict,
        strict_params: strict_params_for_sql(select_sql, env),
    })
}

fn compile_runtime_select_into_stmt(
    select_sql: &str,
    target_refs: &[AssignTarget],
    strict: bool,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let targets = target_refs
        .iter()
        .map(|target| compile_select_into_target(target, env))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(CompiledStmt::RuntimeSelectInto {
        sql: rewrite_plpgsql_sql_text(select_sql, env)?,
        scope: runtime_sql_scope(env),
        targets,
        strict,
        strict_params: strict_params_for_sql(select_sql, env),
    })
}

fn strict_params_for_sql(sql: &str, env: &CompileEnv) -> Vec<CompiledStrictParam> {
    let mut params = env
        .vars
        .iter()
        .filter(|(name, _)| {
            !name.starts_with('$')
                && !is_plpgsql_label_alias(name)
                && identifier_position(sql, name).is_some()
        })
        .map(|(name, var)| {
            (
                identifier_position(sql, name).unwrap_or(usize::MAX),
                CompiledStrictParam {
                    name: name.clone(),
                    slot: var.slot,
                },
            )
        })
        .collect::<Vec<_>>();
    params.sort_by_key(|(position, _)| *position);
    params.into_iter().map(|(_, param)| param).collect()
}

fn compile_for_query_stmt(
    target: &ForTarget,
    source: &ForQuerySource,
    body: &[Stmt],
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledStmt, ParseError> {
    let mut implicit_env = implicit_query_loop_record_name(target, env).map(|name| {
        let mut loop_env = env.child();
        loop_env.define_var(name, SqlType::record(RECORD_TYPE_OID));
        loop_env
    });

    let (source, static_columns) = match source {
        ForQuerySource::Static(sql) => {
            match compile_static_query_source(
                sql,
                catalog,
                env,
                "FOR ... IN query LOOP supports SELECT or VALUES; use EXECUTE for dynamic SQL",
            ) {
                Ok(plan) => {
                    let columns =
                        static_query_source_known_columns(sql).unwrap_or_else(|| plan.columns());
                    (
                        CompiledForQuerySource::Static { plan: plan.clone() },
                        Some(columns),
                    )
                }
                Err(err) if should_fallback_to_runtime_sql(&err) => (
                    CompiledForQuerySource::Runtime {
                        sql: rewrite_plpgsql_sql_text(sql, env)?,
                        scope: runtime_sql_scope(env),
                    },
                    static_query_source_known_columns(sql),
                ),
                Err(err) => return Err(err),
            }
        }
        ForQuerySource::Execute {
            sql_expr,
            using_exprs,
        } => (
            CompiledForQuerySource::Dynamic {
                sql_expr: compile_expr_text(sql_expr, catalog, env)?,
                using_exprs: using_exprs
                    .iter()
                    .map(|expr| compile_expr_text(expr, catalog, env))
                    .collect::<Result<Vec<_>, _>>()?,
            },
            None,
        ),
        ForQuerySource::Cursor { name, args } => {
            let (slot, _, _, _) = resolve_assign_target(&AssignTarget::Name(name.clone()), env)?;
            let source = OpenCursorSource::Declared { args: args.clone() };
            let (source, scrollable, shape_plan) =
                compile_cursor_open_source(name, &source, catalog, env)?;
            (
                CompiledForQuerySource::Cursor {
                    slot,
                    name: name.clone(),
                    source,
                    scrollable,
                },
                shape_plan.map(|plan| plan.columns()),
            )
        }
    };
    let target_env = implicit_env.as_mut().unwrap_or(env);
    let target = compile_for_query_target(target, target_env, static_columns.as_deref())?;
    let body = compile_stmt_list(body, catalog, target_env, return_contract)?;
    if let Some(loop_env) = implicit_env {
        env.next_slot = env.next_slot.max(loop_env.next_slot);
    }
    Ok(CompiledStmt::ForQuery {
        target,
        source,
        body,
    })
}

fn implicit_query_loop_record_name<'a>(target: &'a ForTarget, env: &CompileEnv) -> Option<&'a str> {
    match target {
        ForTarget::Single(AssignTarget::Name(name)) if env.get_var(name).is_none() => {
            Some(name.as_str())
        }
        _ => None,
    }
}

fn compile_foreach_stmt(
    target: &ForTarget,
    slice: usize,
    array_expr: &str,
    body: &[Stmt],
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<CompiledStmt, ParseError> {
    Ok(CompiledStmt::ForEach {
        target: compile_for_query_target(target, env, None)?,
        slice,
        array_expr: compile_expr_text(array_expr, catalog, env)?,
        body: compile_stmt_list(body, catalog, env, return_contract)?,
    })
}

fn compile_for_query_target(
    target: &ForTarget,
    env: &mut CompileEnv,
    static_columns: Option<&[QueryColumn]>,
) -> Result<CompiledForQueryTarget, ParseError> {
    let target_refs: &[AssignTarget] = match target {
        ForTarget::Single(target) => std::slice::from_ref(target),
        ForTarget::List(targets) => targets,
    };

    let mut targets = target_refs
        .iter()
        .map(|target| compile_select_into_target(target, env))
        .collect::<Result<Vec<_>, _>>()?;

    if targets.len() > 1
        && targets
            .iter()
            .any(|target| matches!(target.ty.kind, SqlTypeKind::Record | SqlTypeKind::Composite))
    {
        return Err(ParseError::UnexpectedToken {
            expected: "scalar loop variables for multi-target query FOR loop",
            actual: format!("{target:?}"),
        });
    }

    if let ([target], Some(columns)) = (targets.as_mut_slice(), static_columns)
        && target.ty.kind == SqlTypeKind::Record
    {
        let descriptor = assign_anonymous_record_descriptor(
            columns
                .iter()
                .map(|column| (column.name.clone(), column.sql_type))
                .collect(),
        );
        let ty = descriptor.sql_type();
        env.update_slot_type(target.slot, ty);
        target.ty = ty;
    }

    Ok(CompiledForQueryTarget { targets })
}

fn apply_cursor_shape_to_fetch_targets(
    targets: &mut [CompiledSelectIntoTarget],
    columns: Option<&[QueryColumn]>,
    env: &mut CompileEnv,
) {
    let ([target], Some(columns)) = (targets, columns) else {
        return;
    };
    if target.ty.kind != SqlTypeKind::Record {
        return;
    }
    let descriptor = assign_anonymous_record_descriptor(
        columns
            .iter()
            .map(|column| (column.name.clone(), column.sql_type))
            .collect(),
    );
    let ty = descriptor.sql_type();
    env.update_slot_type(target.slot, ty);
    target.ty = ty;
}

fn compile_exec_returning_into_stmt(
    sql: &str,
    target_refs: &[AssignTarget],
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
) -> Result<CompiledStmt, ParseError> {
    let stmt = parse_statement(sql)?;
    let outer_scope = outer_scope_for_sql(env);
    match stmt {
        Statement::Insert(stmt) => {
            let stmt = normalize_plpgsql_insert(stmt, env);
            let bound = bind_insert_with_outer_scopes(&stmt, catalog, &[outer_scope])?;
            let targets = compile_dml_into_targets(
                target_refs,
                bound
                    .returning
                    .iter()
                    .map(target_entry_query_column)
                    .collect(),
                env,
            )?;
            Ok(CompiledStmt::ExecInsertInto {
                stmt: bound,
                targets,
            })
        }
        Statement::Update(stmt) => {
            let stmt = normalize_plpgsql_update(stmt, env);
            let bound = bind_update_with_outer_scopes(&stmt, catalog, &[outer_scope])?;
            let targets = compile_dml_into_targets(
                target_refs,
                bound
                    .returning
                    .iter()
                    .map(target_entry_query_column)
                    .collect(),
                env,
            )?;
            Ok(CompiledStmt::ExecUpdateInto {
                stmt: bound,
                targets,
            })
        }
        Statement::Delete(stmt) => {
            let stmt = normalize_plpgsql_delete(stmt, env);
            let bound = bind_delete_with_outer_scopes(&stmt, catalog, &[outer_scope])?;
            let targets = compile_dml_into_targets(
                target_refs,
                bound
                    .returning
                    .iter()
                    .map(target_entry_query_column)
                    .collect(),
                env,
            )?;
            Ok(CompiledStmt::ExecDeleteInto {
                stmt: bound,
                targets,
            })
        }
        Statement::Merge(_) => compile_runtime_select_into_stmt(sql, target_refs, false, env),
        other => Err(ParseError::UnexpectedToken {
            expected: "INSERT/UPDATE/DELETE/MERGE ... RETURNING ... INTO",
            actual: format!("{other:?}"),
        }),
    }
}

fn compile_dml_into_targets(
    target_refs: &[AssignTarget],
    result_columns: Vec<QueryColumn>,
    env: &mut CompileEnv,
) -> Result<Vec<CompiledSelectIntoTarget>, ParseError> {
    let mut targets = target_refs
        .iter()
        .map(|target| compile_select_into_target(target, env))
        .collect::<Result<Vec<_>, _>>()?;
    if let [target] = targets.as_mut_slice()
        && target.ty.kind == SqlTypeKind::Record
    {
        let descriptor = assign_anonymous_record_descriptor(
            result_columns
                .iter()
                .map(|column| (column.name.clone(), column.sql_type))
                .collect(),
        );
        let ty = descriptor.sql_type();
        env.update_slot_type(target.slot, ty);
        target.ty = ty;
    }
    Ok(targets)
}

fn dynamic_sql_literal_result_columns(
    sql_expr: &str,
    using_exprs: &[String],
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Option<Vec<QueryColumn>> {
    let sql = dynamic_sql_literal(sql_expr)?;
    let sql = dynamic_shape_sql(&sql, using_exprs);
    let outer_scope = outer_scope_for_sql(env);
    let stmt = parse_statement(&sql).ok()?;
    match stmt {
        Statement::Select(stmt) => pg_plan_query_with_outer_scopes_and_ctes(
            &stmt,
            catalog,
            std::slice::from_ref(&outer_scope),
            &env.local_ctes,
        )
        .ok()
        .map(|plan| plan.columns()),
        Statement::Values(stmt) => pg_plan_values_query_with_outer_scopes_and_ctes(
            &stmt,
            catalog,
            std::slice::from_ref(&outer_scope),
            &env.local_ctes,
        )
        .ok()
        .map(|plan| plan.columns()),
        Statement::Insert(stmt) => {
            bind_insert_with_outer_scopes(&stmt, catalog, std::slice::from_ref(&outer_scope))
                .ok()
                .map(|bound| {
                    bound
                        .returning
                        .iter()
                        .map(target_entry_query_column)
                        .collect()
                })
        }
        Statement::Update(stmt) => {
            bind_update_with_outer_scopes(&stmt, catalog, std::slice::from_ref(&outer_scope))
                .ok()
                .map(|bound| {
                    bound
                        .returning
                        .iter()
                        .map(target_entry_query_column)
                        .collect()
                })
        }
        Statement::Delete(stmt) => {
            bind_delete_with_outer_scopes(&stmt, catalog, std::slice::from_ref(&outer_scope))
                .ok()
                .map(|bound| {
                    bound
                        .returning
                        .iter()
                        .map(target_entry_query_column)
                        .collect()
                })
        }
        _ => None,
    }
}

fn compile_stmt_list(
    statements: &[Stmt],
    catalog: &dyn CatalogLookup,
    env: &mut CompileEnv,
    return_contract: Option<&FunctionReturnContract>,
) -> Result<Vec<CompiledStmt>, ParseError> {
    statements
        .iter()
        .map(|stmt| compile_stmt(stmt, catalog, env, return_contract))
        .collect()
}

fn compile_expr_text(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledExpr, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    compile_expr_sql(&rewritten_sql, sql.trim(), catalog, env)
}

fn compile_assignment_expr_text(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledExpr, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    let rewritten_sql =
        rewrite_plpgsql_assignment_query_expr(&rewritten_sql).unwrap_or(rewritten_sql);
    compile_expr_sql(&rewritten_sql, sql.trim(), catalog, env)
}

fn compile_expr_sql(
    sql: &str,
    source: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledExpr, ParseError> {
    let normalized_sql;
    let sql = if env.nonstandard_string_literals {
        normalized_sql = normalize_nonstandard_string_literals(sql);
        normalized_sql.as_str()
    } else {
        sql
    };
    let parsed = normalize_plpgsql_expr(parse_expr(sql)?, env);
    let (expr, sql_type) = match bind_scalar_expr_in_named_slot_scope(
        &parsed,
        &env.relation_slot_scopes(),
        &env.slot_columns(),
        catalog,
        &env.local_ctes,
    ) {
        Ok(bound) => bound,
        Err(err) => {
            if let Some(expr) = bind_dynamic_record_field_expr(&parsed, env) {
                (expr, SqlType::new(SqlTypeKind::Text))
            } else {
                return Err(err);
            }
        }
    };
    let _ = sql_type;
    let mut subplans = Vec::new();
    let expr = finalize_expr_subqueries(expr, catalog, &mut subplans);
    Ok(CompiledExpr::Scalar {
        expr,
        subplans,
        source: source.trim().to_string(),
    })
}

fn bind_dynamic_record_field_expr(expr: &SqlExpr, env: &CompileEnv) -> Option<Expr> {
    let (name, field) = match expr {
        SqlExpr::FieldSelect { expr, field } => {
            let SqlExpr::Column(name) = expr.as_ref() else {
                return None;
            };
            (name.as_str(), field.as_str())
        }
        SqlExpr::Column(name) => {
            let (name, field) = name.rsplit_once('.')?;
            (name, field)
        }
        _ => return None,
    };
    let var = env.get_var(name)?;
    if !matches!(var.ty.kind, SqlTypeKind::Record) || var.ty.typmod > 0 {
        return None;
    }
    Some(Expr::FieldSelect {
        expr: Box::new(Expr::Var(Var {
            varno: 1,
            varattno: user_attrno(var.slot),
            varlevelsup: 0,
            vartype: var.ty,
            collation_oid: None,
        })),
        field: field.to_string(),
        field_type: SqlType::new(SqlTypeKind::Text),
    })
}

fn compile_condition_text(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<CompiledExpr, ParseError> {
    match compile_expr_text(sql, catalog, env) {
        Ok(expr) => Ok(expr),
        Err(ParseError::UnexpectedToken { actual, .. }) if actual == "aggregate function" => {
            if let Some(condition) = parse_plpgsql_query_condition(sql) {
                let query_sql = format!(
                    "select {} from {}",
                    condition.left_expr, condition.from_clause
                );
                let select = normalize_plpgsql_select(
                    crate::backend::parser::parse_select(&query_sql)?,
                    env,
                );
                let plan = plan_select_for_env(&select, catalog, env)?;
                let rhs = match compile_expr_text(condition.right_expr, catalog, env)? {
                    CompiledExpr::Scalar { expr, subplans, .. } if subplans.is_empty() => expr,
                    CompiledExpr::Scalar { .. } => {
                        return Err(ParseError::FeatureNotSupported(
                            "query-style PL/pgSQL conditions do not support subqueries on the comparison value".into(),
                        ));
                    }
                    CompiledExpr::QueryCompare { .. } => {
                        return Err(ParseError::FeatureNotSupported(
                            "query-style PL/pgSQL conditions do not support query comparisons on both sides".into(),
                        ))
                    }
                };
                return Ok(CompiledExpr::QueryCompare {
                    plan,
                    op: condition.op,
                    rhs,
                    source: sql.trim().to_string(),
                });
            }
            Err(ParseError::UnexpectedToken {
                expected: "non-aggregate expression",
                actual,
            })
        }
        Err(err) => Err(err),
    }
}

fn compile_return_select_expr(
    sql: &str,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<Option<(String, PlannedStmt)>, ParseError> {
    let rewritten_sql = rewrite_plpgsql_sql_text(sql, env)?;
    let Some(from_idx) = find_keyword_at_top_level(&rewritten_sql, "from") else {
        return Ok(None);
    };
    let expr = rewritten_sql[..from_idx].trim();
    let from_clause = rewritten_sql[from_idx + "from".len()..].trim();
    if expr.is_empty() || from_clause.is_empty() || !looks_like_aggregate_expr(expr) {
        return Ok(None);
    }
    // :HACK: PL/pgSQL normally compiles SQL expressions through SPI.  Keep this
    // focused on PostgreSQL regression's aggregate RETURN shape until PL/pgSQL
    // has a general SPI expression-plan path.
    let query_sql = format!("select {expr} from {from_clause}");
    let select = normalize_plpgsql_select(crate::backend::parser::parse_select(&query_sql)?, env);
    let plan = plan_select_for_env(&select, catalog, env)?;
    Ok(Some((query_sql, plan)))
}

fn seed_trigger_env(env: &mut CompileEnv, relation_desc: &RelationDesc) -> CompiledTriggerBindings {
    let new_row = env.define_trigger_relation_scope("new", relation_desc, TriggerReturnedRow::New);
    let old_row = env.define_trigger_relation_scope("old", relation_desc, TriggerReturnedRow::Old);
    let tg_name_slot = env.define_var("tg_name", SqlType::new(SqlTypeKind::Text));
    let tg_op_slot = env.define_var("tg_op", SqlType::new(SqlTypeKind::Text));
    let tg_when_slot = env.define_var("tg_when", SqlType::new(SqlTypeKind::Text));
    let tg_level_slot = env.define_var("tg_level", SqlType::new(SqlTypeKind::Text));
    let tg_relid_slot = env.define_var("tg_relid", SqlType::new(SqlTypeKind::Oid));
    let tg_nargs_slot = env.define_var("tg_nargs", SqlType::new(SqlTypeKind::Int4));
    let tg_argv_slot = env.define_var(
        "tg_argv",
        SqlType::array_of(SqlType::new(SqlTypeKind::Text)),
    );
    let tg_table_name_slot = env.define_var("tg_table_name", SqlType::new(SqlTypeKind::Text));
    env.define_alias(
        "tg_relname",
        tg_table_name_slot,
        SqlType::new(SqlTypeKind::Text),
    );
    let tg_table_schema_slot = env.define_var("tg_table_schema", SqlType::new(SqlTypeKind::Text));

    CompiledTriggerBindings {
        new_row,
        old_row,
        tg_name_slot,
        tg_op_slot,
        tg_when_slot,
        tg_level_slot,
        tg_relid_slot,
        tg_nargs_slot,
        tg_argv_slot,
        tg_table_name_slot,
        tg_table_schema_slot,
    }
}

fn seed_event_trigger_env(env: &mut CompileEnv) -> CompiledEventTriggerBindings {
    let tg_event_slot = env.define_var("tg_event", SqlType::new(SqlTypeKind::Text));
    let tg_tag_slot = env.define_var("tg_tag", SqlType::new(SqlTypeKind::Text));
    CompiledEventTriggerBindings {
        tg_event_slot,
        tg_tag_slot,
    }
}

fn resolve_assign_target(
    target: &AssignTarget,
    env: &CompileEnv,
) -> Result<(usize, SqlType, Option<String>, bool), ParseError> {
    match target {
        AssignTarget::Name(name) => env
            .get_var(name)
            .map(|var| {
                if var.constant {
                    Err(ParseError::DetailedError {
                        message: format!("variable \"{name}\" is declared CONSTANT"),
                        detail: None,
                        hint: None,
                        sqlstate: "22005",
                    })
                } else {
                    Ok((var.slot, var.ty, Some(name.clone()), var.not_null))
                }
            })
            .transpose()?
            .ok_or_else(|| ParseError::UnknownColumn(name.clone())),
        AssignTarget::Parameter(index) => env
            .get_parameter(*index)
            .map(|var| (var.slot, var.ty, None, false))
            .ok_or_else(|| ParseError::UnexpectedToken {
                expected: "existing positional parameter reference",
                actual: format!("${index}"),
            }),
        AssignTarget::Field { relation, field } => env
            .get_relation_field(relation, field)
            .map(|column| (column.slot, column.sql_type, None, false))
            .ok_or_else(|| ParseError::UnknownColumn(format!("{relation}.{field}"))),
        AssignTarget::Subscript { name, .. } => env
            .get_var(name)
            .map(|var| (var.slot, var.ty, Some(name.clone()), var.not_null))
            .ok_or_else(|| ParseError::UnknownColumn(name.clone())),
        AssignTarget::FieldSubscript {
            relation, field, ..
        } => env
            .get_relation_field(relation, field)
            .map(|column| (column.slot, column.sql_type, None, false))
            .or_else(|| {
                env.get_var(relation)
                    .map(|var| (var.slot, var.ty, None, false))
            })
            .ok_or_else(|| ParseError::UnknownColumn(format!("{relation}.{field}"))),
    }
}

fn compile_indirect_assign_target(
    target: &AssignTarget,
    catalog: &dyn CatalogLookup,
    env: &CompileEnv,
) -> Result<Option<CompiledIndirectAssignTarget>, ParseError> {
    match target {
        AssignTarget::Field { relation, field } => {
            let Some(var) = env.get_var(relation) else {
                return Ok(None);
            };
            Ok(Some(CompiledIndirectAssignTarget {
                slot: var.slot,
                ty: var.ty,
                indirection: vec![CompiledAssignIndirection::Field(field.clone())],
            }))
        }
        AssignTarget::FieldSubscript {
            relation,
            field,
            subscripts,
        } => {
            if let Some(var) = env.get_var(relation) {
                let mut indirection = Vec::with_capacity(subscripts.len() + 1);
                indirection.push(CompiledAssignIndirection::Field(field.clone()));
                indirection.extend(
                    subscripts
                        .iter()
                        .map(|subscript| {
                            compile_expr_text(subscript, catalog, env)
                                .map(CompiledAssignIndirection::Subscript)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                );
                return Ok(Some(CompiledIndirectAssignTarget {
                    slot: var.slot,
                    ty: var.ty,
                    indirection,
                }));
            }

            let column = env
                .get_relation_field(relation, field)
                .ok_or_else(|| ParseError::UnknownColumn(format!("{relation}.{field}")))?;
            Ok(Some(CompiledIndirectAssignTarget {
                slot: column.slot,
                ty: column.sql_type,
                indirection: subscripts
                    .iter()
                    .map(|subscript| {
                        compile_expr_text(subscript, catalog, env)
                            .map(CompiledAssignIndirection::Subscript)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            }))
        }
        _ => Ok(None),
    }
}

fn subscripted_assignment_target_type(
    root_ty: SqlType,
    subscript_count: usize,
    catalog: &dyn CatalogLookup,
) -> Result<SqlType, ParseError> {
    let ty = plpgsql_assignment_navigation_sql_type(root_ty, catalog);
    if subscript_count == 0 {
        return Ok(ty);
    }
    if !ty.is_array {
        return Err(ParseError::DetailedError {
            message: format!(
                "cannot subscript type {} because it does not support subscripting",
                crate::backend::parser::analyze::sql_type_name(ty)
            ),
            detail: None,
            hint: None,
            sqlstate: "42804",
        });
    }
    Ok(plpgsql_assignment_navigation_sql_type(
        ty.element_type(),
        catalog,
    ))
}

fn plpgsql_assignment_navigation_sql_type(mut ty: SqlType, catalog: &dyn CatalogLookup) -> SqlType {
    loop {
        let Some(domain) = catalog.domain_by_type_oid(ty.type_oid) else {
            return ty;
        };
        if ty.is_array && !domain.sql_type.is_array {
            return SqlType::array_of(domain.sql_type);
        }
        ty = domain.sql_type;
    }
}

fn compile_select_into_target(
    target: &AssignTarget,
    env: &CompileEnv,
) -> Result<CompiledSelectIntoTarget, ParseError> {
    let (slot, ty, name, not_null) = resolve_assign_target(target, env)?;
    Ok(CompiledSelectIntoTarget {
        slot,
        ty,
        name,
        not_null,
    })
}

#[allow(dead_code)]
pub(crate) fn compile_decl_type(type_name: &str) -> Result<SqlType, ParseError> {
    parse_type_name(type_name).and_then(|ty| match ty {
        crate::backend::parser::RawTypeName::Builtin(sql_type) => Ok(sql_type),
        crate::backend::parser::RawTypeName::Serial(kind) => {
            Err(ParseError::FeatureNotSupported(format!(
                "{} is only allowed in CREATE TABLE / ALTER TABLE ADD COLUMN",
                match kind {
                    crate::backend::parser::SerialKind::Small => "smallserial",
                    crate::backend::parser::SerialKind::Regular => "serial",
                    crate::backend::parser::SerialKind::Big => "bigserial",
                }
            )))
        }
        crate::backend::parser::RawTypeName::Record => {
            Err(ParseError::UnsupportedType("record".into()))
        }
        crate::backend::parser::RawTypeName::Named { name, .. } => {
            Err(ParseError::UnsupportedType(name))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{BoundRelation, InsertSource};

    struct EmptyCatalog;

    impl CatalogLookup for EmptyCatalog {
        fn lookup_any_relation(&self, _name: &str) -> Option<BoundRelation> {
            None
        }
    }

    #[test]
    fn rewrites_plpgsql_count_condition_with_from_clause() {
        assert_eq!(
            rewrite_plpgsql_query_condition("count(*) = 0 from Room where roomno = new.roomno"),
            Some("(select count(*) from Room where roomno = new.roomno) = 0".into())
        );
    }

    #[test]
    fn normalizes_labeled_record_field_reference() {
        let mut env = CompileEnv::default();
        let slot = env.define_var("item", SqlType::record(RECORD_TYPE_OID));
        env.push_label_scope("outer");

        let parsed = parse_expr("\"outer\".item.note").unwrap();
        assert_eq!(
            normalize_plpgsql_expr(parsed, &env),
            SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(plpgsql_label_alias(0, slot, "item"))),
                field: "note".into(),
            }
        );
    }

    #[test]
    fn normalizes_labeled_scalar_variable_reference() {
        let mut env = CompileEnv::default();
        let slot = env.define_var("param1", SqlType::new(SqlTypeKind::Int4));
        env.push_label_scope("pl_qual_names");
        env.define_var("param1", SqlType::new(SqlTypeKind::Int4));

        let parsed = parse_expr("pl_qual_names.param1").unwrap();
        assert_eq!(
            normalize_plpgsql_expr(parsed, &env),
            SqlExpr::Column(plpgsql_label_alias(0, slot, "param1")),
        );
    }

    #[test]
    fn labeled_record_field_reference_survives_inner_shadowing() {
        let mut env = CompileEnv::default();
        let outer_slot = env.define_var("rec", SqlType::record(RECORD_TYPE_OID));
        env.push_label_scope("outer");
        env.define_var("rec", SqlType::record(RECORD_TYPE_OID));

        let parsed = parse_expr("\"outer\".rec.backlink").unwrap();
        assert_eq!(
            normalize_plpgsql_expr(parsed, &env),
            SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column(plpgsql_label_alias(0, outer_slot, "rec"))),
                field: "backlink".into(),
            }
        );
    }

    #[test]
    fn normalizes_record_field_references_in_insert_values() {
        let mut env = CompileEnv::default();
        env.define_var("obj", SqlType::record(RECORD_TYPE_OID));

        let Statement::Insert(stmt) =
            parse_statement("insert into dropped_objects (object_type) values (obj.object_type)")
                .unwrap()
        else {
            panic!("expected INSERT statement");
        };
        let normalized = normalize_plpgsql_insert(stmt, &env);

        let InsertSource::Values(rows) = normalized.source else {
            panic!("expected INSERT VALUES source");
        };
        assert_eq!(
            rows[0][0],
            SqlExpr::FieldSelect {
                expr: Box::new(SqlExpr::Column("obj".into())),
                field: "object_type".into(),
            }
        );
    }

    #[test]
    fn rewrites_plpgsql_count_condition_with_greater_than() {
        assert_eq!(
            rewrite_plpgsql_query_condition("count(*) > 0 from Hub where name = old.hubname"),
            Some("(select count(*) from Hub where name = old.hubname) > 0".into())
        );
    }

    #[test]
    fn rewrites_plpgsql_assignment_query_expr() {
        assert_eq!(
            rewrite_plpgsql_assignment_query_expr(
                "retval || slotno::text from HSlot where slotname = psrec.slotlink"
            ),
            Some(
                "(select retval || slotno::text from HSlot where slotname = psrec.slotlink)".into()
            )
        );
    }

    #[test]
    fn ignores_normal_scalar_conditions() {
        assert_eq!(
            rewrite_plpgsql_query_condition("new.slotno < 1 or new.slotno > hubrec.nslots"),
            None
        );
    }

    #[test]
    fn aliases_trigger_relation_scope() {
        let mut env = CompileEnv::default();
        let desc = RelationDesc {
            columns: Vec::new(),
        };
        let bindings = seed_trigger_env(&mut env, &desc);

        assert!(env.define_relation_alias("ps", TriggerReturnedRow::New));
        assert_eq!(
            env.trigger_relation_return_row("ps"),
            Some(TriggerReturnedRow::New)
        );

        let stmt = compile_return_stmt(
            Some("ps"),
            1,
            &EmptyCatalog,
            &env,
            Some(&FunctionReturnContract::Trigger { bindings }),
        )
        .unwrap();
        assert!(matches!(
            stmt,
            CompiledStmt::ReturnTriggerRow {
                row: TriggerReturnedRow::New
            }
        ));
    }
}
