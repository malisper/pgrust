use pgrust_analyze::CatalogLookup;
use pgrust_catalog_data::{
    PG_CATALOG_NAMESPACE_OID, PG_LANGUAGE_INTERNAL_OID, PG_TOAST_NAMESPACE_OID,
    PUBLIC_NAMESPACE_OID, PgTriggerRow,
};
use pgrust_nodes::parsenodes::{
    JsonTableBehavior, RawWindowFrameBound, SqlCallArgs, SqlExpr, SqlType, SqlTypeKind,
    TriggerLevel, TriggerTiming,
};
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_nodes::{
    SessionReplicationRole, TriggerCallContext, TriggerFunctionResult, TriggerOperation, Value,
};

pub const TRIGGER_TYPE_ROW: i16 = 1 << 0;
pub const TRIGGER_TYPE_BEFORE: i16 = 1 << 1;
pub const TRIGGER_TYPE_INSERT: i16 = 1 << 2;
pub const TRIGGER_TYPE_DELETE: i16 = 1 << 3;
pub const TRIGGER_TYPE_UPDATE: i16 = 1 << 4;
pub const TRIGGER_TYPE_TRUNCATE: i16 = 1 << 5;
pub const TRIGGER_TYPE_INSTEAD: i16 = 1 << 6;

pub const TRIGGER_DISABLED: char = 'D';
pub const TRIGGER_ENABLED_ORIGIN: char = 'O';
pub const TRIGGER_ENABLED_REPLICA: char = 'R';
pub const TRIGGER_ENABLED_ALWAYS: char = 'A';

pub const TRIGGER_NEW_TABLEOID_COLUMN: &str = "__trigger_new_tableoid";
pub const TRIGGER_OLD_TABLEOID_COLUMN: &str = "__trigger_old_tableoid";
pub const TRIGGER_NEW_CTID_COLUMN: &str = "__trigger_new_ctid";
pub const TRIGGER_OLD_CTID_COLUMN: &str = "__trigger_old_ctid";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinTriggerFunction {
    SuppressRedundantUpdates,
    TsVectorUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerFunctionKind {
    Plpgsql(u32),
    Builtin(BuiltinTriggerFunction),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerLoadError {
    pub message: String,
    pub detail: Option<String>,
    pub sqlstate: &'static str,
}

impl TriggerLoadError {
    fn unsupported(message: impl Into<String>, detail: Option<String>) -> Self {
        Self {
            message: message.into(),
            detail,
            sqlstate: "0A000",
        }
    }
}

pub fn trigger_matches_event(
    row: &PgTriggerRow,
    event: TriggerOperation,
    modified_attnums: &[i16],
) -> bool {
    let matches_event = match event {
        TriggerOperation::Insert => (row.tgtype & TRIGGER_TYPE_INSERT) != 0,
        TriggerOperation::Update => (row.tgtype & TRIGGER_TYPE_UPDATE) != 0,
        TriggerOperation::Delete => (row.tgtype & TRIGGER_TYPE_DELETE) != 0,
        TriggerOperation::Truncate => (row.tgtype & TRIGGER_TYPE_TRUNCATE) != 0,
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

pub fn trigger_is_row(tgtype: i16) -> bool {
    (tgtype & TRIGGER_TYPE_ROW) != 0
}

pub fn trigger_is_before(tgtype: i16) -> bool {
    (tgtype & TRIGGER_TYPE_BEFORE) != 0
}

pub fn trigger_is_instead(tgtype: i16) -> bool {
    (tgtype & TRIGGER_TYPE_INSTEAD) != 0
}

pub fn trigger_uses_transition_tables(row: &PgTriggerRow) -> bool {
    row.tgoldtable.is_some() || row.tgnewtable.is_some()
}

pub fn relation_has_instead_row_trigger(
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

pub fn trigger_timing(tgtype: i16) -> TriggerTiming {
    if trigger_is_instead(tgtype) {
        TriggerTiming::Instead
    } else if trigger_is_before(tgtype) {
        TriggerTiming::Before
    } else {
        TriggerTiming::After
    }
}

pub fn trigger_is_enabled_for_session(row: &PgTriggerRow, role: SessionReplicationRole) -> bool {
    match row.tgenabled {
        TRIGGER_ENABLED_ALWAYS => true,
        TRIGGER_ENABLED_REPLICA => role == SessionReplicationRole::Replica,
        TRIGGER_ENABLED_ORIGIN => role != SessionReplicationRole::Replica,
        TRIGGER_DISABLED => false,
        _ => false,
    }
}

pub fn load_trigger_function(
    catalog: &dyn CatalogLookup,
    row: &PgTriggerRow,
) -> Result<TriggerFunctionKind, TriggerLoadError> {
    let proc_row = catalog.proc_row_by_oid(row.tgfoid).ok_or_else(|| {
        TriggerLoadError::unsupported(
            "trigger function does not exist",
            Some(format!("missing pg_proc row for oid {}", row.tgfoid)),
        )
    })?;
    if proc_row.prolang == PG_LANGUAGE_INTERNAL_OID {
        return match proc_row.proname.as_str() {
            "suppress_redundant_updates_trigger" => Ok(TriggerFunctionKind::Builtin(
                BuiltinTriggerFunction::SuppressRedundantUpdates,
            )),
            "tsvector_update_trigger" | "tsvector_update_trigger_column" => Ok(
                TriggerFunctionKind::Builtin(BuiltinTriggerFunction::TsVectorUpdate),
            ),
            _ => Err(TriggerLoadError::unsupported(
                "unsupported internal trigger function",
                Some(proc_row.proname),
            )),
        };
    }
    Ok(TriggerFunctionKind::Plpgsql(row.tgfoid))
}

pub fn execute_builtin_trigger_function(
    function: BuiltinTriggerFunction,
    call: &TriggerCallContext,
    tsvector_builder: impl FnOnce(&str, &str) -> Result<Value, String>,
) -> Result<TriggerFunctionResult, TriggerLoadError> {
    match function {
        BuiltinTriggerFunction::SuppressRedundantUpdates => {
            execute_suppress_redundant_updates(call)
        }
        BuiltinTriggerFunction::TsVectorUpdate => execute_tsvector_update(call, tsvector_builder),
    }
}

fn execute_suppress_redundant_updates(
    call: &TriggerCallContext,
) -> Result<TriggerFunctionResult, TriggerLoadError> {
    if call.timing != TriggerTiming::Before
        || call.level != TriggerLevel::Row
        || call.op != TriggerOperation::Update
    {
        return Err(TriggerLoadError::unsupported(
            "suppress_redundant_updates_trigger must be fired BEFORE UPDATE FOR EACH ROW",
            None,
        ));
    }
    let old_row = call.old_row.as_ref().ok_or_else(|| {
        TriggerLoadError::unsupported(
            "suppress_redundant_updates_trigger requires OLD row data",
            None,
        )
    })?;
    let new_row = call.new_row.as_ref().ok_or_else(|| {
        TriggerLoadError::unsupported(
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

fn execute_tsvector_update(
    call: &TriggerCallContext,
    tsvector_builder: impl FnOnce(&str, &str) -> Result<Value, String>,
) -> Result<TriggerFunctionResult, TriggerLoadError> {
    if call.timing != TriggerTiming::Before
        || call.level != TriggerLevel::Row
        || !matches!(call.op, TriggerOperation::Insert | TriggerOperation::Update)
    {
        return Err(TriggerLoadError::unsupported(
            "tsvector_update_trigger must be fired BEFORE INSERT OR UPDATE FOR EACH ROW",
            None,
        ));
    }
    if call.trigger_args.len() < 3 {
        return Err(TriggerLoadError::unsupported(
            "tsvector_update_trigger requires target column, configuration, and source columns",
            None,
        ));
    }
    let mut new_row = call.new_row.clone().ok_or_else(|| {
        TriggerLoadError::unsupported("tsvector_update_trigger requires NEW row data", None)
    })?;
    let target_index = trigger_column_index(&call.relation_desc, &call.trigger_args[0])?;
    // :HACK: tsvector_update_trigger_column should read the regconfig from a
    // row column. The current trigger runtime only preserves trigger argv text
    // here, so both builtin trigger variants treat argv[1] as the config name.
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
    let vector = tsvector_builder(config_name, &document).map_err(|message| {
        TriggerLoadError::unsupported(
            "tsvector_update_trigger failed to build tsvector",
            Some(message),
        )
    })?;
    if target_index >= new_row.len() {
        return Err(TriggerLoadError::unsupported(
            "tsvector_update_trigger target column is outside NEW row",
            None,
        ));
    }
    new_row[target_index] = vector;
    Ok(TriggerFunctionResult::ReturnNew(new_row))
}

pub fn trigger_column_index(desc: &RelationDesc, name: &str) -> Result<usize, TriggerLoadError> {
    desc.columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(name) && !column.dropped)
        .ok_or_else(|| {
            TriggerLoadError::unsupported(
                "trigger column does not exist",
                Some(format!("column \"{name}\" was not found")),
            )
        })
}

pub fn trigger_when_local_columns(event: TriggerOperation) -> Vec<(String, SqlType)> {
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
        TriggerOperation::Truncate => Vec::new(),
    }
}

pub fn trigger_when_local_values(relation_oid: u32, event: TriggerOperation) -> Vec<Value> {
    let tableoid = Value::Int64(i64::from(relation_oid));
    match event {
        TriggerOperation::Insert => vec![tableoid, Value::Null],
        TriggerOperation::Update => vec![tableoid.clone(), Value::Null, tableoid, Value::Null],
        TriggerOperation::Delete => vec![tableoid, Value::Null],
        TriggerOperation::Truncate => Vec::new(),
    }
}

pub fn clone_or_null_row(row: Option<&[Value]>, width: usize) -> Vec<Value> {
    match row {
        Some(row) => row.to_vec(),
        None => vec![Value::Null; width],
    }
}

pub fn materialized_row(row: &[Value]) -> Vec<Value> {
    let mut values = row.to_vec();
    Value::materialize_all(&mut values);
    values
}

pub fn resolve_relation_names(
    catalog: &dyn CatalogLookup,
    relation_oid: u32,
    relation_name: &str,
) -> (String, String) {
    let Some(entry) = catalog.relation_by_oid(relation_oid) else {
        return split_relation_name(relation_name, None);
    };
    let namespace_name = catalog
        .namespace_row_by_oid(entry.namespace_oid)
        .map(|row| row.nspname)
        .unwrap_or_else(|| namespace_name_for_oid(entry.namespace_oid));
    let table_name = relation_name
        .rsplit_once('.')
        .map(|(_, table_name)| table_name.to_string())
        .unwrap_or_else(|| relation_name.to_string());
    (table_name, namespace_name)
}

pub fn split_relation_name(name: &str, namespace_oid: Option<u32>) -> (String, String) {
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

pub fn namespace_name_for_oid(namespace_oid: u32) -> String {
    match namespace_oid {
        PUBLIC_NAMESPACE_OID => "public".into(),
        PG_CATALOG_NAMESPACE_OID => "pg_catalog".into(),
        PG_TOAST_NAMESPACE_OID => "pg_toast".into(),
        _ => "public".into(),
    }
}

pub fn rewrite_trigger_system_column_refs(expr: &mut SqlExpr) {
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
        SqlExpr::Parameter(_) => {}
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
        SqlExpr::JsonQueryFunction(func) => {
            rewrite_trigger_system_column_refs(&mut func.context);
            rewrite_trigger_system_column_refs(&mut func.path);
            for arg in &mut func.passing {
                rewrite_trigger_system_column_refs(&mut arg.expr);
            }
            if let Some(JsonTableBehavior::Default(expr)) = &mut func.on_empty {
                rewrite_trigger_system_column_refs(expr);
            }
            if let Some(JsonTableBehavior::Default(expr)) = &mut func.on_error {
                rewrite_trigger_system_column_refs(expr);
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
        | SqlExpr::ParamRef(_)
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
        | SqlExpr::User
        | SqlExpr::SystemUser
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

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_catalog_data::desc::column_desc;
    use pgrust_nodes::{SqlType, SqlTypeKind};

    fn row(tgtype: i16) -> PgTriggerRow {
        PgTriggerRow {
            oid: 1,
            tgrelid: 2,
            tgparentid: 0,
            tgname: "trg".into(),
            tgfoid: 3,
            tgtype,
            tgenabled: TRIGGER_ENABLED_ORIGIN,
            tgisinternal: false,
            tgconstrrelid: 0,
            tgconstrindid: 0,
            tgconstraint: 0,
            tgdeferrable: false,
            tginitdeferred: false,
            tgnargs: 0,
            tgattr: Vec::new(),
            tgargs: Vec::new(),
            tgqual: None,
            tgoldtable: None,
            tgnewtable: None,
        }
    }

    #[test]
    fn trigger_event_matching_honors_update_columns() {
        let mut row = row(TRIGGER_TYPE_ROW | TRIGGER_TYPE_UPDATE);
        assert!(trigger_matches_event(&row, TriggerOperation::Update, &[]));
        row.tgattr = vec![2];
        assert!(trigger_matches_event(&row, TriggerOperation::Update, &[2]));
        assert!(!trigger_matches_event(&row, TriggerOperation::Update, &[1]));
        assert!(!trigger_matches_event(&row, TriggerOperation::Insert, &[2]));
    }

    #[test]
    fn trigger_enablement_follows_replication_role() {
        let mut row = row(TRIGGER_TYPE_INSERT);
        assert!(trigger_is_enabled_for_session(
            &row,
            SessionReplicationRole::Origin
        ));
        row.tgenabled = TRIGGER_ENABLED_REPLICA;
        assert!(!trigger_is_enabled_for_session(
            &row,
            SessionReplicationRole::Origin
        ));
        assert!(trigger_is_enabled_for_session(
            &row,
            SessionReplicationRole::Replica
        ));
    }

    #[derive(Default)]
    struct TestCatalog {
        trigger_rows: Vec<PgTriggerRow>,
    }

    impl CatalogLookup for TestCatalog {
        fn lookup_any_relation(&self, _name: &str) -> Option<pgrust_analyze::BoundRelation> {
            None
        }

        fn trigger_rows_for_relation(&self, relation_oid: u32) -> Vec<PgTriggerRow> {
            self.trigger_rows
                .iter()
                .filter(|row| row.tgrelid == relation_oid)
                .cloned()
                .collect()
        }
    }

    #[test]
    fn relation_has_instead_row_trigger_checks_event_and_level() {
        let catalog = TestCatalog {
            trigger_rows: vec![
                row(TRIGGER_TYPE_ROW | TRIGGER_TYPE_INSTEAD | TRIGGER_TYPE_INSERT),
                row(TRIGGER_TYPE_INSTEAD | TRIGGER_TYPE_UPDATE),
            ],
        };

        assert!(relation_has_instead_row_trigger(
            &catalog,
            2,
            TriggerOperation::Insert
        ));
        assert!(!relation_has_instead_row_trigger(
            &catalog,
            2,
            TriggerOperation::Update
        ));
    }

    #[test]
    fn split_relation_name_uses_schema_or_namespace_default() {
        assert_eq!(
            split_relation_name("custom.t", None),
            ("t".into(), "custom".into())
        );
        assert_eq!(
            split_relation_name("pg_type", Some(PG_CATALOG_NAMESPACE_OID)),
            ("pg_type".into(), "pg_catalog".into())
        );
    }

    #[test]
    fn clone_or_null_row_preserves_width_for_missing_row() {
        assert_eq!(clone_or_null_row(None, 2), vec![Value::Null, Value::Null]);
        assert_eq!(
            clone_or_null_row(Some(&[Value::Int32(1)]), 2),
            vec![Value::Int32(1)]
        );
    }

    fn trigger_call(op: TriggerOperation) -> TriggerCallContext {
        TriggerCallContext {
            relation_desc: RelationDesc {
                columns: vec![
                    column_desc("tsv", SqlType::new(SqlTypeKind::TsVector), false),
                    column_desc("body", SqlType::new(SqlTypeKind::Text), false),
                ],
            },
            relation_oid: 42,
            table_name: "t".into(),
            table_schema: "public".into(),
            trigger_name: "trg".into(),
            trigger_args: vec!["tsv".into(), "english".into(), "body".into()],
            timing: TriggerTiming::Before,
            level: TriggerLevel::Row,
            op,
            new_row: Some(vec![Value::Null, Value::Text("hello world".into())]),
            old_row: Some(vec![Value::Null, Value::Text("old".into())]),
            transition_tables: Vec::new(),
        }
    }

    #[test]
    fn suppress_redundant_updates_returns_no_value_for_identical_rows() {
        let mut call = trigger_call(TriggerOperation::Update);
        call.new_row = Some(vec![Value::Int32(1)]);
        call.old_row = Some(vec![Value::Int32(1)]);

        let result = execute_builtin_trigger_function(
            BuiltinTriggerFunction::SuppressRedundantUpdates,
            &call,
            |_, _| unreachable!(),
        )
        .unwrap();

        assert_eq!(result, TriggerFunctionResult::NoValue);
    }

    #[test]
    fn tsvector_update_builds_document_with_callback() {
        let result = execute_builtin_trigger_function(
            BuiltinTriggerFunction::TsVectorUpdate,
            &trigger_call(TriggerOperation::Insert),
            |config, document| {
                assert_eq!(config, "english");
                assert_eq!(document, "hello world");
                Ok(Value::Text("vector".into()))
            },
        )
        .unwrap();

        assert!(matches!(
            result,
            TriggerFunctionResult::ReturnNew(row)
                if row == vec![Value::Text("vector".into()), Value::Text("hello world".into())]
        ));
    }
}
