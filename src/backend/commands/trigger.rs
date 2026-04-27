use crate::backend::executor::{
    ExecError, ExecutorContext, Expr, RelationDesc, SessionReplicationRole, TupleSlot, Value,
    eval_expr,
};
use crate::backend::parser::{
    CatalogLookup, RawWindowFrameBound, SqlCallArgs, SqlExpr, SqlType, SqlTypeKind, TriggerLevel,
    TriggerTiming, bind_scalar_expr_in_named_relation_scope, parse_expr,
};
use crate::backend::utils::cache::visible_catalog::VisibleCatalog;
use crate::include::catalog::{
    PG_CATALOG_NAMESPACE_OID, PG_LANGUAGE_INTERNAL_OID, PG_TOAST_NAMESPACE_OID,
    PUBLIC_NAMESPACE_OID, PgTriggerRow,
};
use crate::pl::plpgsql::{
    TriggerCallContext, TriggerFunctionResult, TriggerOperation, TriggerTransitionTable,
    execute_user_defined_trigger_function,
};

const TRIGGER_TYPE_ROW: i16 = 1 << 0;
const TRIGGER_TYPE_BEFORE: i16 = 1 << 1;
const TRIGGER_TYPE_INSERT: i16 = 1 << 2;
const TRIGGER_TYPE_DELETE: i16 = 1 << 3;
const TRIGGER_TYPE_UPDATE: i16 = 1 << 4;
const TRIGGER_TYPE_TRUNCATE: i16 = 1 << 5;
const TRIGGER_TYPE_INSTEAD: i16 = 1 << 6;

const TRIGGER_DISABLED: char = 'D';
const TRIGGER_ENABLED_ORIGIN: char = 'O';
const TRIGGER_ENABLED_REPLICA: char = 'R';
const TRIGGER_ENABLED_ALWAYS: char = 'A';

const TRIGGER_NEW_TABLEOID_COLUMN: &str = "__trigger_new_tableoid";
const TRIGGER_OLD_TABLEOID_COLUMN: &str = "__trigger_old_tableoid";
const TRIGGER_NEW_CTID_COLUMN: &str = "__trigger_new_ctid";
const TRIGGER_OLD_CTID_COLUMN: &str = "__trigger_old_ctid";

#[derive(Debug, Clone, Copy)]
enum BuiltinTriggerFunction {
    SuppressRedundantUpdates,
    TsVectorUpdate,
}

#[derive(Debug, Clone)]
enum LoadedTriggerFunction {
    Plpgsql(u32),
    Builtin(BuiltinTriggerFunction),
}

#[derive(Debug, Clone)]
struct LoadedTrigger {
    row: PgTriggerRow,
    when_expr: Option<Expr>,
    function: LoadedTriggerFunction,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TriggerTransitionCapture {
    old_rows: Vec<Vec<Value>>,
    new_rows: Vec<Vec<Value>>,
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
        catalog: &VisibleCatalog,
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
                if !self.when_passes(trigger, old_row, new_row, ctx)? {
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
        let call = TriggerCallContext {
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
        };
        ctx.trigger_depth = ctx.trigger_depth.saturating_add(1);
        let result = match trigger.function {
            LoadedTriggerFunction::Plpgsql(proc_oid) => {
                execute_user_defined_trigger_function(proc_oid, &call, ctx)
            }
            LoadedTriggerFunction::Builtin(function) => {
                execute_builtin_trigger_function(function, &call)
            }
        };
        ctx.trigger_depth = ctx.trigger_depth.saturating_sub(1);
        result
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

fn clone_or_null_row(row: Option<&[Value]>, width: usize) -> Vec<Value> {
    match row {
        Some(row) => row.to_vec(),
        None => vec![Value::Null; width],
    }
}

fn materialized_row(row: &[Value]) -> Vec<Value> {
    let mut values = row.to_vec();
    Value::materialize_all(&mut values);
    values
}

fn load_trigger_function(
    catalog: &VisibleCatalog,
    row: &PgTriggerRow,
) -> Result<LoadedTriggerFunction, ExecError> {
    let proc_row = catalog.proc_row_by_oid(row.tgfoid).ok_or_else(|| {
        trigger_runtime_error(
            "trigger function does not exist",
            Some(format!("missing pg_proc row for oid {}", row.tgfoid)),
        )
    })?;
    if proc_row.prolang == PG_LANGUAGE_INTERNAL_OID {
        return match proc_row.proname.as_str() {
            "suppress_redundant_updates_trigger" => Ok(LoadedTriggerFunction::Builtin(
                BuiltinTriggerFunction::SuppressRedundantUpdates,
            )),
            "tsvector_update_trigger" | "tsvector_update_trigger_column" => Ok(
                LoadedTriggerFunction::Builtin(BuiltinTriggerFunction::TsVectorUpdate),
            ),
            _ => Err(trigger_runtime_error(
                "unsupported internal trigger function",
                Some(proc_row.proname),
            )),
        };
    }
    Ok(LoadedTriggerFunction::Plpgsql(row.tgfoid))
}

fn execute_builtin_trigger_function(
    function: BuiltinTriggerFunction,
    call: &TriggerCallContext,
) -> Result<TriggerFunctionResult, ExecError> {
    match function {
        BuiltinTriggerFunction::SuppressRedundantUpdates => {
            if call.timing != TriggerTiming::Before
                || call.level != TriggerLevel::Row
                || call.op != TriggerOperation::Update
            {
                return Err(trigger_runtime_error(
                    "suppress_redundant_updates_trigger must be fired BEFORE UPDATE FOR EACH ROW",
                    None,
                ));
            }
            let old_row = call.old_row.as_ref().ok_or_else(|| {
                trigger_runtime_error(
                    "suppress_redundant_updates_trigger requires OLD row data",
                    None,
                )
            })?;
            let new_row = call.new_row.as_ref().ok_or_else(|| {
                trigger_runtime_error(
                    "suppress_redundant_updates_trigger requires NEW row data",
                    None,
                )
            })?;
            if old_row == new_row {
                Ok(TriggerFunctionResult::NoValue)
            } else {
                Ok(TriggerFunctionResult::ReturnNew(new_row.clone()))
            }
        }
        BuiltinTriggerFunction::TsVectorUpdate => {
            if call.timing != TriggerTiming::Before
                || call.level != TriggerLevel::Row
                || !matches!(call.op, TriggerOperation::Insert | TriggerOperation::Update)
            {
                return Err(trigger_runtime_error(
                    "tsvector_update_trigger must be fired BEFORE INSERT OR UPDATE FOR EACH ROW",
                    None,
                ));
            }
            if call.trigger_args.len() < 3 {
                return Err(trigger_runtime_error(
                    "tsvector_update_trigger requires target column, configuration, and source columns",
                    None,
                ));
            }
            let mut new_row = call.new_row.clone().ok_or_else(|| {
                trigger_runtime_error("tsvector_update_trigger requires NEW row data", None)
            })?;
            let target_index = trigger_column_index(&call.relation_desc, &call.trigger_args[0])?;
            // :HACK: tsvector_update_trigger_column should read the regconfig
            // from a row column. The current trigger runtime only preserves
            // trigger argv text here, so both builtin trigger variants treat
            // argv[1] as the configuration name.
            let config_name = call.trigger_args[1].as_str();
            let mut document = String::new();
            for source_name in &call.trigger_args[2..] {
                let source_index = trigger_column_index(&call.relation_desc, source_name)?;
                let Some(value) = new_row.get(source_index) else {
                    continue;
                };
                if matches!(value, Value::Null) {
                    continue;
                }
                if !document.is_empty() {
                    document.push(' ');
                }
                document.push_str(value.as_text().unwrap_or_default());
            }
            let vector = crate::backend::tsearch::to_tsvector_with_config_name(
                Some(config_name),
                &document,
                None,
            )
            .map_err(|message| {
                trigger_runtime_error(
                    "tsvector_update_trigger failed to build tsvector",
                    Some(message),
                )
            })?;
            if target_index >= new_row.len() {
                return Err(trigger_runtime_error(
                    "tsvector_update_trigger target column is outside NEW row",
                    None,
                ));
            }
            new_row[target_index] = Value::TsVector(vector);
            Ok(TriggerFunctionResult::ReturnNew(new_row))
        }
    }
}

fn trigger_column_index(desc: &RelationDesc, name: &str) -> Result<usize, ExecError> {
    desc.columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(name) && !column.dropped)
        .ok_or_else(|| {
            trigger_runtime_error(
                "trigger column does not exist",
                Some(format!("column \"{name}\" was not found")),
            )
        })
}

fn compile_when_expr(
    catalog: &VisibleCatalog,
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

fn resolve_relation_names(
    catalog: &VisibleCatalog,
    relation_oid: u32,
    relation_name: &str,
) -> (String, String) {
    let Some((cached_name, entry)) = catalog
        .relcache()
        .entries()
        .find(|(_, entry)| entry.relation_oid == relation_oid)
    else {
        return split_relation_name(relation_name, None);
    };
    split_relation_name(cached_name, Some(entry.namespace_oid))
}

fn split_relation_name(name: &str, namespace_oid: Option<u32>) -> (String, String) {
    if let Some((schema_name, table_name)) = name.rsplit_once('.') {
        return (table_name.to_string(), schema_name.to_string());
    }
    (
        name.to_string(),
        namespace_oid
            .map(namespace_name_for_oid)
            .unwrap_or_else(|| "public".to_string()),
    )
}

fn namespace_name_for_oid(namespace_oid: u32) -> String {
    match namespace_oid {
        PUBLIC_NAMESPACE_OID => "public".into(),
        PG_CATALOG_NAMESPACE_OID => "pg_catalog".into(),
        PG_TOAST_NAMESPACE_OID => "pg_toast".into(),
        _ => "public".into(),
    }
}

fn trigger_when_local_columns(event: TriggerOperation) -> Vec<(String, SqlType)> {
    match event {
        TriggerOperation::Insert => vec![
            (
                TRIGGER_NEW_TABLEOID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Oid),
            ),
            (
                TRIGGER_NEW_CTID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Tid),
            ),
        ],
        TriggerOperation::Update => vec![
            (
                TRIGGER_NEW_TABLEOID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Oid),
            ),
            (
                TRIGGER_NEW_CTID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Tid),
            ),
            (
                TRIGGER_OLD_TABLEOID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Oid),
            ),
            (
                TRIGGER_OLD_CTID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Tid),
            ),
        ],
        TriggerOperation::Delete => vec![
            (
                TRIGGER_OLD_TABLEOID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Oid),
            ),
            (
                TRIGGER_OLD_CTID_COLUMN.into(),
                SqlType::new(SqlTypeKind::Tid),
            ),
        ],
    }
}

fn trigger_when_local_values(relation_oid: u32, event: TriggerOperation) -> Vec<Value> {
    let tableoid = Value::Int64(i64::from(relation_oid));
    match event {
        TriggerOperation::Insert => vec![tableoid, Value::Null],
        TriggerOperation::Update => vec![tableoid.clone(), Value::Null, tableoid, Value::Null],
        TriggerOperation::Delete => vec![tableoid, Value::Null],
    }
}

fn rewrite_trigger_system_column_refs(expr: &mut SqlExpr) {
    match expr {
        SqlExpr::Column(name) => {
            let lowered = name.to_ascii_lowercase();
            if lowered == "new.tableoid" {
                *name = TRIGGER_NEW_TABLEOID_COLUMN.into();
            } else if lowered == "old.tableoid" {
                *name = TRIGGER_OLD_TABLEOID_COLUMN.into();
            } else if lowered == "new.ctid" {
                *name = TRIGGER_NEW_CTID_COLUMN.into();
            } else if lowered == "old.ctid" {
                *name = TRIGGER_OLD_CTID_COLUMN.into();
            }
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
            rewrite_trigger_system_column_refs(left);
            rewrite_trigger_system_column_refs(right);
        }
        SqlExpr::BinaryOperator { left, right, .. }
        | SqlExpr::GeometryBinaryOp { left, right, .. }
        | SqlExpr::AtTimeZone {
            expr: left,
            zone: right,
        } => {
            rewrite_trigger_system_column_refs(left);
            rewrite_trigger_system_column_refs(right);
        }
        SqlExpr::UnaryPlus(inner)
        | SqlExpr::Negate(inner)
        | SqlExpr::BitNot(inner)
        | SqlExpr::GeometryUnaryOp { expr: inner, .. }
        | SqlExpr::PrefixOperator { expr: inner, .. }
        | SqlExpr::Cast(inner, _)
        | SqlExpr::IsNull(inner)
        | SqlExpr::IsNotNull(inner)
        | SqlExpr::Not(inner)
        | SqlExpr::FieldSelect { expr: inner, .. }
        | SqlExpr::Subscript { expr: inner, .. }
        | SqlExpr::Collate { expr: inner, .. } => rewrite_trigger_system_column_refs(inner),
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
            rewrite_trigger_system_column_refs(expr);
            rewrite_trigger_system_column_refs(pattern);
            if let Some(escape) = escape {
                rewrite_trigger_system_column_refs(escape);
            }
        }
        SqlExpr::Case {
            arg,
            args,
            defresult,
        } => {
            if let Some(arg) = arg {
                rewrite_trigger_system_column_refs(arg);
            }
            for when in args {
                rewrite_trigger_system_column_refs(&mut when.expr);
                rewrite_trigger_system_column_refs(&mut when.result);
            }
            if let Some(defresult) = defresult {
                rewrite_trigger_system_column_refs(defresult);
            }
        }
        SqlExpr::ArrayLiteral(values) | SqlExpr::Row(values) => {
            for value in values {
                rewrite_trigger_system_column_refs(value);
            }
        }
        SqlExpr::InSubquery { expr, .. } => rewrite_trigger_system_column_refs(expr),
        SqlExpr::QuantifiedSubquery { left, .. } => rewrite_trigger_system_column_refs(left),
        SqlExpr::QuantifiedArray { left, array, .. } => {
            rewrite_trigger_system_column_refs(left);
            rewrite_trigger_system_column_refs(array);
        }
        SqlExpr::ArraySubscript { array, subscripts } => {
            rewrite_trigger_system_column_refs(array);
            for subscript in subscripts {
                if let Some(lower) = &mut subscript.lower {
                    rewrite_trigger_system_column_refs(lower);
                }
                if let Some(upper) = &mut subscript.upper {
                    rewrite_trigger_system_column_refs(upper);
                }
            }
        }
        SqlExpr::Xml(xml) => {
            for arg in &mut xml.named_args {
                rewrite_trigger_system_column_refs(arg);
            }
            for arg in &mut xml.args {
                rewrite_trigger_system_column_refs(arg);
            }
        }
        SqlExpr::FuncCall {
            args,
            order_by,
            filter,
            over,
            ..
        } => {
            if let SqlCallArgs::Args(args) = args {
                for arg in args {
                    rewrite_trigger_system_column_refs(&mut arg.value);
                }
            }
            for item in order_by {
                rewrite_trigger_system_column_refs(&mut item.expr);
            }
            if let Some(filter) = filter {
                rewrite_trigger_system_column_refs(filter);
            }
            if let Some(over) = over {
                for expr in &mut over.partition_by {
                    rewrite_trigger_system_column_refs(expr);
                }
                for item in &mut over.order_by {
                    rewrite_trigger_system_column_refs(&mut item.expr);
                }
                if let Some(frame) = &mut over.frame {
                    rewrite_trigger_window_bound(&mut frame.start_bound);
                    rewrite_trigger_window_bound(&mut frame.end_bound);
                }
            }
        }
        SqlExpr::Const(_)
        | SqlExpr::Default
        | SqlExpr::IntegerLiteral(_)
        | SqlExpr::NumericLiteral(_)
        | SqlExpr::ScalarSubquery(_)
        | SqlExpr::ArraySubquery(_)
        | SqlExpr::Exists(_)
        | SqlExpr::Random
        | SqlExpr::CurrentDate
        | SqlExpr::CurrentCatalog
        | SqlExpr::CurrentSchema
        | SqlExpr::CurrentUser
        | SqlExpr::SessionUser
        | SqlExpr::CurrentRole
        | SqlExpr::CurrentTime { .. }
        | SqlExpr::CurrentTimestamp { .. }
        | SqlExpr::LocalTime { .. }
        | SqlExpr::LocalTimestamp { .. } => {}
    }
}

fn rewrite_trigger_window_bound(bound: &mut RawWindowFrameBound) {
    match bound {
        RawWindowFrameBound::OffsetPreceding(expr) | RawWindowFrameBound::OffsetFollowing(expr) => {
            rewrite_trigger_system_column_refs(expr);
        }
        RawWindowFrameBound::UnboundedPreceding
        | RawWindowFrameBound::CurrentRow
        | RawWindowFrameBound::UnboundedFollowing => {}
    }
}

fn trigger_matches_event(
    row: &PgTriggerRow,
    event: TriggerOperation,
    modified_attnums: &[i16],
) -> bool {
    let matches_event = match event {
        TriggerOperation::Insert => (row.tgtype & TRIGGER_TYPE_INSERT) != 0,
        TriggerOperation::Update => (row.tgtype & TRIGGER_TYPE_UPDATE) != 0,
        TriggerOperation::Delete => (row.tgtype & TRIGGER_TYPE_DELETE) != 0,
    };
    if !matches_event {
        return false;
    }
    if !matches!(event, TriggerOperation::Update) || row.tgattr.is_empty() {
        return true;
    }
    row.tgattr
        .iter()
        .any(|attnum| modified_attnums.iter().any(|modified| modified == attnum))
}

fn trigger_is_row(tgtype: i16) -> bool {
    (tgtype & TRIGGER_TYPE_ROW) != 0
}

fn trigger_is_before(tgtype: i16) -> bool {
    (tgtype & TRIGGER_TYPE_BEFORE) != 0
}

fn trigger_is_instead(tgtype: i16) -> bool {
    (tgtype & TRIGGER_TYPE_INSTEAD) != 0
}

fn trigger_uses_transition_tables(row: &PgTriggerRow) -> bool {
    row.tgoldtable.is_some() || row.tgnewtable.is_some()
}

fn trigger_timing(tgtype: i16) -> TriggerTiming {
    if trigger_is_instead(tgtype) {
        TriggerTiming::Instead
    } else if trigger_is_before(tgtype) {
        TriggerTiming::Before
    } else {
        TriggerTiming::After
    }
}

pub(crate) fn trigger_is_enabled_for_session(
    row: &PgTriggerRow,
    role: SessionReplicationRole,
) -> bool {
    match row.tgenabled {
        TRIGGER_ENABLED_ALWAYS => true,
        TRIGGER_ENABLED_REPLICA => role == SessionReplicationRole::Replica,
        TRIGGER_ENABLED_ORIGIN => role != SessionReplicationRole::Replica,
        TRIGGER_DISABLED => false,
        _ => false,
    }
}

pub(crate) fn relation_has_instead_row_trigger(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    event: TriggerOperation,
) -> bool {
    catalog
        .trigger_rows_for_relation(relation_oid)
        .into_iter()
        .any(|row| {
            trigger_is_instead(row.tgtype)
                && trigger_is_row(row.tgtype)
                && trigger_matches_event(&row, event, &[])
        })
}

fn trigger_runtime_error(message: &str, detail: Option<String>) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail,
        hint: None,
        sqlstate: "0A000",
    }
}
