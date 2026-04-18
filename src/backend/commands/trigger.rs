use crate::backend::executor::{
    ExecError, ExecutorContext, Expr, RelationDesc, TupleSlot, Value, eval_expr,
};
use crate::backend::parser::{
    SqlTypeKind, TriggerLevel, TriggerTiming, bind_scalar_expr_in_named_relation_scope, parse_expr,
};
use crate::backend::utils::cache::visible_catalog::VisibleCatalog;
use crate::include::catalog::{
    PG_CATALOG_NAMESPACE_OID, PG_TOAST_NAMESPACE_OID, PUBLIC_NAMESPACE_OID, PgTriggerRow,
};
use crate::pl::plpgsql::{
    TriggerCallContext, TriggerFunctionResult, TriggerOperation,
    execute_user_defined_trigger_function,
};

const TRIGGER_TYPE_ROW: i16 = 1 << 0;
const TRIGGER_TYPE_BEFORE: i16 = 1 << 1;
const TRIGGER_TYPE_INSERT: i16 = 1 << 2;
const TRIGGER_TYPE_DELETE: i16 = 1 << 3;
const TRIGGER_TYPE_UPDATE: i16 = 1 << 4;

#[derive(Debug, Clone)]
struct LoadedTrigger {
    row: PgTriggerRow,
    when_expr: Option<Expr>,
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
    ) -> Result<Self, ExecError> {
        let (table_name, table_schema) =
            resolve_relation_names(catalog, relation_oid, relation_name);
        let mut triggers = catalog
            .trigger_rows_for_relation(relation_oid)
            .into_iter()
            .filter(|row| row.tgenabled == 'O')
            .filter(|row| trigger_matches_event(row, event, modified_attnums))
            .map(|row| {
                let when_expr = compile_when_expr(catalog, relation_desc, event, &row)?;
                Ok(LoadedTrigger { row, when_expr })
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
        self.fire_statement_triggers(true, ctx)
    }

    pub(crate) fn after_statement(&self, ctx: &mut ExecutorContext) -> Result<(), ExecError> {
        self.fire_statement_triggers(false, ctx)
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
            match self.execute_trigger(trigger, None, Some(&new_row), ctx)? {
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
        self.fire_after_row(None, Some(new_row), ctx)
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
            match self.execute_trigger(trigger, Some(old_row), Some(&new_row), ctx)? {
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
        self.fire_after_row(Some(old_row), Some(new_row), ctx)
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
            match self.execute_trigger(trigger, Some(old_row), None, ctx)? {
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
        self.fire_after_row(Some(old_row), None, ctx)
    }

    fn fire_statement_triggers(
        &self,
        before: bool,
        ctx: &mut ExecutorContext,
    ) -> Result<(), ExecError> {
        for trigger in self.triggers.iter().filter(|trigger| {
            trigger_is_before(trigger.row.tgtype) == before && !trigger_is_row(trigger.row.tgtype)
        }) {
            if !self.when_passes(trigger, None, None, ctx)? {
                continue;
            }
            match self.execute_trigger(trigger, None, None, ctx)? {
                TriggerFunctionResult::SkipRow | TriggerFunctionResult::NoValue => {}
                TriggerFunctionResult::ReturnNew(_) | TriggerFunctionResult::ReturnOld(_) => {
                    return Err(trigger_runtime_error(
                        "statement triggers must return null",
                        Some(format!(
                            "trigger \"{}\" returned a row value",
                            trigger.row.tgname
                        )),
                    ));
                }
            }
        }
        Ok(())
    }

    fn fire_after_row(
        &self,
        old_row: Option<&[Value]>,
        new_row: Option<&[Value]>,
        ctx: &mut ExecutorContext,
    ) -> Result<(), ExecError> {
        for trigger in self.triggers.iter().filter(|trigger| {
            !trigger_is_before(trigger.row.tgtype) && trigger_is_row(trigger.row.tgtype)
        }) {
            if !self.when_passes(trigger, old_row, new_row, ctx)? {
                continue;
            }
            let _ = self.execute_trigger(trigger, old_row, new_row, ctx)?;
        }
        Ok(())
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
        match self.event {
            TriggerOperation::Insert => {
                clone_or_null_row(new_row, self.relation_desc.columns.len())
            }
            TriggerOperation::Update => {
                let mut values = clone_or_null_row(new_row, self.relation_desc.columns.len());
                values.extend(clone_or_null_row(old_row, self.relation_desc.columns.len()));
                values
            }
            TriggerOperation::Delete => {
                clone_or_null_row(old_row, self.relation_desc.columns.len())
            }
        }
    }

    fn execute_trigger(
        &self,
        trigger: &LoadedTrigger,
        old_row: Option<&[Value]>,
        new_row: Option<&[Value]>,
        ctx: &mut ExecutorContext,
    ) -> Result<TriggerFunctionResult, ExecError> {
        execute_user_defined_trigger_function(
            trigger.row.tgfoid,
            &TriggerCallContext {
                relation_desc: self.relation_desc.clone(),
                relation_oid: self.relation_oid,
                table_name: self.table_name.clone(),
                table_schema: self.table_schema.clone(),
                trigger_name: trigger.row.tgname.clone(),
                trigger_args: trigger.row.tgargs.clone(),
                timing: if trigger_is_before(trigger.row.tgtype) {
                    TriggerTiming::Before
                } else {
                    TriggerTiming::After
                },
                level: if trigger_is_row(trigger.row.tgtype) {
                    TriggerLevel::Row
                } else {
                    TriggerLevel::Statement
                },
                op: self.event,
                new_row: new_row.map(|row| row.to_vec()),
                old_row: old_row.map(|row| row.to_vec()),
            },
            ctx,
        )
    }
}

fn clone_or_null_row(row: Option<&[Value]>, width: usize) -> Vec<Value> {
    match row {
        Some(row) => row.to_vec(),
        None => vec![Value::Null; width],
    }
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
    let parsed = parse_expr(sql).map_err(ExecError::Parse)?;
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
    let (expr, sql_type) =
        bind_scalar_expr_in_named_relation_scope(&parsed, &relation_scopes, &[], catalog)
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

fn trigger_runtime_error(message: &str, detail: Option<String>) -> ExecError {
    ExecError::DetailedError {
        message: message.into(),
        detail,
        hint: None,
        sqlstate: "0A000",
    }
}
