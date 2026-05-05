use crate::backend::executor::{
    ConstraintTiming, ExecError, ExecutorContext, Expr, RelationDesc, SessionReplicationRole,
    TupleSlot, Value, eval_expr,
};
use crate::backend::parser::{
    CatalogLookup, SqlTypeKind, TriggerLevel, bind_scalar_expr_in_named_relation_scope, parse_expr,
};
use crate::include::catalog::PgTriggerRow;
pub(crate) use crate::pl::plpgsql::TriggerTransitionCapture;
use crate::pl::plpgsql::{
    TriggerCallContext, TriggerFunctionResult, TriggerOperation, TriggerTransitionTable,
    execute_user_defined_trigger_function,
};
pub(crate) use pgrust_commands::trigger::trigger_is_enabled_for_session;
use pgrust_commands::trigger::{
    BuiltinTriggerFunction, TriggerFunctionKind, clone_or_null_row, materialized_row,
    resolve_relation_names, rewrite_trigger_system_column_refs, trigger_is_before,
    trigger_is_instead, trigger_is_row, trigger_matches_event, trigger_timing,
    trigger_uses_transition_tables, trigger_when_local_columns, trigger_when_local_values,
};

#[derive(Debug, Clone)]
struct LoadedTrigger {
    row: PgTriggerRow,
    when_expr: Option<Expr>,
    function: TriggerFunctionKind,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeTriggers {
    relation_oid: u32,
    relation_desc: RelationDesc,
    table_name: String,
    table_schema: String,
    event: TriggerOperation,
    triggers: Vec<LoadedTrigger>,
}

impl RuntimeTriggers {
    pub(crate) fn load(
        catalog: &dyn CatalogLookup,
        relation_oid: u32,
        relation_name: &str,
        relation_desc: &RelationDesc,
        event: TriggerOperation,
        modified_attnums: &[i16],
        session_replication_role: SessionReplicationRole,
    ) -> Result<Self, ExecError> {
        let (table_name, table_schema) =
            resolve_relation_names(catalog, relation_oid, relation_name);
        let mut triggers = catalog
            .trigger_rows_for_relation(relation_oid)
            .into_iter()
            .filter(|row| !row.tgisinternal)
            .filter(|row| trigger_is_enabled_for_session(row, session_replication_role))
            .filter(|row| trigger_matches_event(row, event, modified_attnums))
            .map(|row| {
                let function = load_trigger_function(catalog, &row)?;
                let when_expr = compile_when_expr(catalog, relation_desc, event, &row)?;
                Ok(LoadedTrigger {
                    row,
                    when_expr,
                    function,
                })
            })
            .collect::<Result<Vec<_>, ExecError>>()?;
        triggers.sort_by(|left, right| left.row.tgname.cmp(&right.row.tgname));
        Ok(Self {
            relation_oid,
            relation_desc: relation_desc.clone(),
            table_name,
            table_schema,
            event,
            triggers,
        })
    }

    pub(crate) fn before_statement(&self, ctx: &mut ExecutorContext) -> Result<(), ExecError> {
        self.fire_statement_triggers(true, None, ctx)
    }

    pub(crate) fn after_statement(
        &self,
        capture: Option<&TriggerTransitionCapture>,
        ctx: &mut ExecutorContext,
    ) -> Result<(), ExecError> {
        self.fire_statement_triggers(false, capture, ctx)
    }

    pub(crate) fn new_transition_capture(&self) -> TriggerTransitionCapture {
        TriggerTransitionCapture::default()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.triggers.is_empty()
    }

    pub(crate) fn capture_insert_row(&self, capture: &mut TriggerTransitionCapture, row: &[Value]) {
        if self
            .triggers
            .iter()
            .any(|trigger| trigger.row.tgnewtable.is_some())
        {
            capture.new_rows.push(materialized_row(row));
        }
    }

    pub(crate) fn capture_update_row(
        &self,
        capture: &mut TriggerTransitionCapture,
        old_row: &[Value],
        new_row: &[Value],
    ) {
        if self
            .triggers
            .iter()
            .any(|trigger| trigger_uses_transition_tables(&trigger.row))
        {
            capture.old_rows.push(materialized_row(old_row));
            capture.new_rows.push(materialized_row(new_row));
        }
    }

    pub(crate) fn capture_delete_row(&self, capture: &mut TriggerTransitionCapture, row: &[Value]) {
        if self
            .triggers
            .iter()
            .any(|trigger| trigger.row.tgoldtable.is_some())
        {
            capture.old_rows.push(materialized_row(row));
        }
    }

    pub(crate) fn has_instead_row_triggers(&self) -> bool {
        self.triggers.iter().any(|trigger| {
            trigger_is_instead(trigger.row.tgtype) && trigger_is_row(trigger.row.tgtype)
        })
    }

    pub(crate) fn has_before_row_insert(&self) -> bool {
        self.triggers.iter().any(|trigger| {
            trigger_is_before(trigger.row.tgtype) && trigger_is_row(trigger.row.tgtype)
        })
    }

    pub(crate) fn before_row_insert(
        &self,
        mut new_row: Vec<Value>,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<Vec<Value>>, ExecError> {
        for trigger in self.triggers.iter().filter(|trigger| {
            trigger_is_before(trigger.row.tgtype) && trigger_is_row(trigger.row.tgtype)
        }) {
            if !self.when_passes(trigger, None, Some(&new_row), ctx)? {
                continue;
            }
            match self.execute_trigger(trigger, None, Some(&new_row), None, ctx)? {
                TriggerFunctionResult::SkipRow | TriggerFunctionResult::NoValue => return Ok(None),
                TriggerFunctionResult::ReturnNew(values)
                | TriggerFunctionResult::ReturnOld(values) => new_row = values,
            }
        }
        Ok(Some(new_row))
    }

    pub(crate) fn after_row_insert(
        &self,
        new_row: &[Value],
        ctx: &mut ExecutorContext,
    ) -> Result<(), ExecError> {
        self.fire_after_row(None, Some(new_row), None, false, ctx)
    }

    pub(crate) fn instead_row_insert(
        &self,
        mut new_row: Vec<Value>,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<Vec<Value>>, ExecError> {
        for trigger in self.triggers.iter().filter(|trigger| {
            trigger_is_instead(trigger.row.tgtype) && trigger_is_row(trigger.row.tgtype)
        }) {
            if !self.when_passes(trigger, None, Some(&new_row), ctx)? {
                continue;
            }
            match self.execute_trigger(trigger, None, Some(&new_row), None, ctx)? {
                TriggerFunctionResult::SkipRow | TriggerFunctionResult::NoValue => return Ok(None),
                TriggerFunctionResult::ReturnNew(values)
                | TriggerFunctionResult::ReturnOld(values) => new_row = values,
            }
        }
        Ok(Some(new_row))
    }

    pub(crate) fn before_row_update(
        &self,
        old_row: &[Value],
        mut new_row: Vec<Value>,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<Vec<Value>>, ExecError> {
        for trigger in self.triggers.iter().filter(|trigger| {
            trigger_is_before(trigger.row.tgtype) && trigger_is_row(trigger.row.tgtype)
        }) {
            if !self.when_passes(trigger, Some(old_row), Some(&new_row), ctx)? {
                continue;
            }
            match self.execute_trigger(trigger, Some(old_row), Some(&new_row), None, ctx)? {
                TriggerFunctionResult::SkipRow | TriggerFunctionResult::NoValue => return Ok(None),
                TriggerFunctionResult::ReturnNew(values)
                | TriggerFunctionResult::ReturnOld(values) => new_row = values,
            }
        }
        Ok(Some(new_row))
    }

    pub(crate) fn after_row_update(
        &self,
        old_row: &[Value],
        new_row: &[Value],
        ctx: &mut ExecutorContext,
    ) -> Result<(), ExecError> {
        self.fire_after_row(Some(old_row), Some(new_row), None, false, ctx)
    }

    pub(crate) fn instead_row_update(
        &self,
        old_row: &[Value],
        mut new_row: Vec<Value>,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<Vec<Value>>, ExecError> {
        for trigger in self.triggers.iter().filter(|trigger| {
            trigger_is_instead(trigger.row.tgtype) && trigger_is_row(trigger.row.tgtype)
        }) {
            if !self.when_passes(trigger, Some(old_row), Some(&new_row), ctx)? {
                continue;
            }
            match self.execute_trigger(trigger, Some(old_row), Some(&new_row), None, ctx)? {
                TriggerFunctionResult::SkipRow | TriggerFunctionResult::NoValue => return Ok(None),
                TriggerFunctionResult::ReturnNew(values)
                | TriggerFunctionResult::ReturnOld(values) => new_row = values,
            }
        }
        Ok(Some(new_row))
    }

    pub(crate) fn before_row_delete(
        &self,
        old_row: &[Value],
        ctx: &mut ExecutorContext,
    ) -> Result<bool, ExecError> {
        for trigger in self.triggers.iter().filter(|trigger| {
            trigger_is_before(trigger.row.tgtype) && trigger_is_row(trigger.row.tgtype)
        }) {
            if !self.when_passes(trigger, Some(old_row), None, ctx)? {
                continue;
            }
            match self.execute_trigger(trigger, Some(old_row), None, None, ctx)? {
                TriggerFunctionResult::SkipRow | TriggerFunctionResult::NoValue => {
                    return Ok(false);
                }
                TriggerFunctionResult::ReturnNew(_) | TriggerFunctionResult::ReturnOld(_) => {}
            }
        }
        Ok(true)
    }

    pub(crate) fn after_row_delete(
        &self,
        old_row: &[Value],
        ctx: &mut ExecutorContext,
    ) -> Result<(), ExecError> {
        self.fire_after_row(Some(old_row), None, None, false, ctx)
    }

    pub(crate) fn instead_row_delete(
        &self,
        mut old_row: Vec<Value>,
        ctx: &mut ExecutorContext,
    ) -> Result<Option<Vec<Value>>, ExecError> {
        for trigger in self.triggers.iter().filter(|trigger| {
            trigger_is_instead(trigger.row.tgtype) && trigger_is_row(trigger.row.tgtype)
        }) {
            if !self.when_passes(trigger, Some(&old_row), None, ctx)? {
                continue;
            }
            match self.execute_trigger(trigger, Some(&old_row), None, None, ctx)? {
                TriggerFunctionResult::SkipRow | TriggerFunctionResult::NoValue => return Ok(None),
                TriggerFunctionResult::ReturnNew(values)
                | TriggerFunctionResult::ReturnOld(values) => old_row = values,
            }
        }
        Ok(Some(old_row))
    }

    pub(crate) fn after_transition_rows(
        &self,
        capture: &TriggerTransitionCapture,
        ctx: &mut ExecutorContext,
    ) -> Result<(), ExecError> {
        match self.event {
            TriggerOperation::Insert => {
                for new_row in &capture.new_rows {
                    self.fire_after_row(None, Some(new_row), Some(capture), true, ctx)?;
                }
            }
            TriggerOperation::Update => {
                for (old_row, new_row) in capture.old_rows.iter().zip(capture.new_rows.iter()) {
                    self.fire_after_row(Some(old_row), Some(new_row), Some(capture), true, ctx)?;
                }
            }
            TriggerOperation::Delete => {
                for old_row in &capture.old_rows {
                    self.fire_after_row(Some(old_row), None, Some(capture), true, ctx)?;
                }
            }
            TriggerOperation::Truncate => {}
        }
        Ok(())
    }

    fn fire_statement_triggers(
        &self,
        before: bool,
        capture: Option<&TriggerTransitionCapture>,
        ctx: &mut ExecutorContext,
    ) -> Result<(), ExecError> {
        for trigger in self.triggers.iter().filter(|trigger| {
            trigger_is_before(trigger.row.tgtype) == before && !trigger_is_row(trigger.row.tgtype)
        }) {
            if !self.when_passes(trigger, None, None, ctx)? {
                continue;
            }
            let _ = self.execute_trigger(trigger, None, None, capture, ctx)?;
        }
        Ok(())
    }

    fn fire_after_row(
        &self,
        old_row: Option<&[Value]>,
        new_row: Option<&[Value]>,
        capture: Option<&TriggerTransitionCapture>,
        transition_only: bool,
        ctx: &mut ExecutorContext,
    ) -> Result<(), ExecError> {
        with_after_trigger_snapshot(ctx, |ctx| {
            for trigger in self.triggers.iter().filter(|trigger| {
                !trigger_is_before(trigger.row.tgtype)
                    && !trigger_is_instead(trigger.row.tgtype)
                    && trigger_is_row(trigger.row.tgtype)
                    && trigger_uses_transition_tables(&trigger.row) == transition_only
            }) {
                if ctx.security_restricted
                    && trigger.row.tgdeferrable
                    && ctx.constraint_timing(
                        trigger.row.tgconstraint,
                        trigger.row.tgdeferrable,
                        trigger.row.tginitdeferred,
                    ) == ConstraintTiming::Deferred
                {
                    return Err(ExecError::DetailedError {
                        message:
                            "cannot fire deferred trigger within security-restricted operation"
                                .into(),
                        detail: None,
                        hint: None,
                        sqlstate: "0A000",
                    });
                }
                if !self.when_passes(trigger, old_row, new_row, ctx)? {
                    continue;
                }
                if trigger.row.tgdeferrable
                    && ctx.constraint_timing(
                        trigger.row.oid,
                        trigger.row.tgdeferrable,
                        trigger.row.tginitdeferred,
                    ) == crate::backend::executor::ConstraintTiming::Deferred
                    && let TriggerFunctionKind::Plpgsql(proc_oid) = &trigger.function
                    && let Some(tracker) = ctx.deferred_foreign_keys.as_ref()
                {
                    tracker.record_user_constraint_trigger(
                        trigger.row.oid,
                        *proc_oid,
                        self.trigger_call_context(trigger, old_row, new_row, capture),
                    );
                    continue;
                }
                let _ = self.execute_trigger(trigger, old_row, new_row, capture, ctx)?;
            }
            Ok(())
        })
    }

    fn when_passes(
        &self,
        trigger: &LoadedTrigger,
        old_row: Option<&[Value]>,
        new_row: Option<&[Value]>,
        ctx: &mut ExecutorContext,
    ) -> Result<bool, ExecError> {
        let Some(expr) = &trigger.when_expr else {
            return Ok(true);
        };
        let mut slot = TupleSlot::virtual_row(self.when_tuple_values(trigger, old_row, new_row));
        slot.table_oid = Some(self.relation_oid);
        match eval_expr(expr, &mut slot, ctx)? {
            Value::Bool(true) => Ok(true),
            Value::Bool(false) | Value::Null => Ok(false),
            other => Err(ExecError::NonBoolQual(other)),
        }
    }

    fn when_tuple_values(
        &self,
        trigger: &LoadedTrigger,
        old_row: Option<&[Value]>,
        new_row: Option<&[Value]>,
    ) -> Vec<Value> {
        if !trigger_is_row(trigger.row.tgtype) {
            return Vec::new();
        }
        let mut values = trigger_when_local_values(self.relation_oid, self.event);
        match self.event {
            TriggerOperation::Insert => {
                values.extend(clone_or_null_row(new_row, self.relation_desc.columns.len()));
            }
            TriggerOperation::Update => {
                values.extend(clone_or_null_row(new_row, self.relation_desc.columns.len()));
                values.extend(clone_or_null_row(old_row, self.relation_desc.columns.len()));
            }
            TriggerOperation::Delete => {
                values.extend(clone_or_null_row(old_row, self.relation_desc.columns.len()));
            }
            TriggerOperation::Truncate => {}
        }
        values
    }

    fn execute_trigger(
        &self,
        trigger: &LoadedTrigger,
        old_row: Option<&[Value]>,
        new_row: Option<&[Value]>,
        capture: Option<&TriggerTransitionCapture>,
        ctx: &mut ExecutorContext,
    ) -> Result<TriggerFunctionResult, ExecError> {
        let call = self.trigger_call_context(trigger, old_row, new_row, capture);
        ctx.trigger_depth = ctx.trigger_depth.saturating_add(1);
        let result = match &trigger.function {
            TriggerFunctionKind::Plpgsql(proc_oid) => {
                execute_user_defined_trigger_function(*proc_oid, &call, ctx)
            }
            TriggerFunctionKind::Builtin(function) => {
                execute_builtin_trigger_function(*function, &call)
            }
        };
        ctx.trigger_depth = ctx.trigger_depth.saturating_sub(1);
        result
    }

    fn trigger_call_context(
        &self,
        trigger: &LoadedTrigger,
        old_row: Option<&[Value]>,
        new_row: Option<&[Value]>,
        capture: Option<&TriggerTransitionCapture>,
    ) -> TriggerCallContext {
        TriggerCallContext {
            relation_desc: self.relation_desc.clone(),
            relation_oid: self.relation_oid,
            table_name: self.table_name.clone(),
            table_schema: self.table_schema.clone(),
            trigger_name: trigger.row.tgname.clone(),
            trigger_args: trigger.row.tgargs.clone(),
            timing: trigger_timing(trigger.row.tgtype),
            level: if trigger_is_row(trigger.row.tgtype) {
                TriggerLevel::Row
            } else {
                TriggerLevel::Statement
            },
            op: self.event,
            new_row: new_row.map(|row| row.to_vec()),
            old_row: old_row.map(|row| row.to_vec()),
            transition_tables: self.transition_tables_for_trigger(trigger, capture),
        }
    }

    fn transition_tables_for_trigger(
        &self,
        trigger: &LoadedTrigger,
        capture: Option<&TriggerTransitionCapture>,
    ) -> Vec<TriggerTransitionTable> {
        let Some(capture) = capture else {
            return Vec::new();
        };
        let mut tables = Vec::new();
        if let Some(name) = trigger.row.tgoldtable.as_ref() {
            tables.push(TriggerTransitionTable {
                name: name.clone(),
                desc: self.relation_desc.clone(),
                rows: capture.old_rows.clone(),
            });
        }
        if let Some(name) = trigger.row.tgnewtable.as_ref() {
            tables.push(TriggerTransitionTable {
                name: name.clone(),
                desc: self.relation_desc.clone(),
                rows: capture.new_rows.clone(),
            });
        }
        tables
    }
}

fn with_after_trigger_snapshot<T>(
    ctx: &mut ExecutorContext,
    f: impl FnOnce(&mut ExecutorContext) -> Result<T, ExecError>,
) -> Result<T, ExecError> {
    let saved_current_cid = ctx.snapshot.current_cid;
    ctx.snapshot.current_cid = ctx.next_command_id.saturating_add(1);
    let result = f(ctx);
    ctx.snapshot.current_cid = saved_current_cid;
    result
}

fn load_trigger_function(
    catalog: &dyn CatalogLookup,
    row: &PgTriggerRow,
) -> Result<TriggerFunctionKind, ExecError> {
    pgrust_commands::trigger::load_trigger_function(catalog, row).map_err(|err| {
        ExecError::DetailedError {
            message: err.message,
            detail: err.detail,
            hint: None,
            sqlstate: err.sqlstate,
        }
    })
}

fn execute_builtin_trigger_function(
    function: BuiltinTriggerFunction,
    call: &TriggerCallContext,
) -> Result<TriggerFunctionResult, ExecError> {
    pgrust_commands::trigger::execute_builtin_trigger_function(
        function,
        call,
        |config, document| {
            crate::backend::tsearch::to_tsvector_with_config_name(Some(config), document, None)
                .map(Value::TsVector)
        },
    )
    .map_err(|err| ExecError::DetailedError {
        message: err.message,
        detail: err.detail,
        hint: None,
        sqlstate: err.sqlstate,
    })
}

fn compile_when_expr(
    catalog: &dyn CatalogLookup,
    relation_desc: &RelationDesc,
    event: TriggerOperation,
    row: &PgTriggerRow,
) -> Result<Option<Expr>, ExecError> {
    let Some(sql) = row.tgqual.as_deref() else {
        return Ok(None);
    };
    let mut parsed = parse_expr(sql).map_err(ExecError::Parse)?;
    rewrite_trigger_system_column_refs(&mut parsed);
    let mut relation_scopes = Vec::new();
    if trigger_is_row(row.tgtype) {
        match event {
            TriggerOperation::Insert => relation_scopes.push(("new", relation_desc)),
            TriggerOperation::Update => {
                relation_scopes.push(("new", relation_desc));
                relation_scopes.push(("old", relation_desc));
            }
            TriggerOperation::Delete => relation_scopes.push(("old", relation_desc)),
            TriggerOperation::Truncate => {}
        }
    }
    let local_columns = trigger_when_local_columns(event);
    let (expr, sql_type) = bind_scalar_expr_in_named_relation_scope(
        &parsed,
        &relation_scopes,
        &local_columns,
        catalog,
    )
    .map_err(ExecError::Parse)?;
    if sql_type.kind != SqlTypeKind::Bool {
        return Err(trigger_runtime_error(
            "trigger WHEN condition must return type boolean",
            Some(sql.to_string()),
        ));
    }
    Ok(Some(expr))
}

pub(crate) fn relation_has_instead_row_trigger(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    event: TriggerOperation,
) -> bool {
    pgrust_commands::trigger::relation_has_instead_row_trigger(catalog, relation_oid, event)
}

fn trigger_runtime_error(message: &str, detail: Option<String>) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail,
        hint: None,
        sqlstate: "0A000",
    }
}
