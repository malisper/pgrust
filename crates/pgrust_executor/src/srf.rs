use pgrust_catalog_data::statistics_payload::decode_pg_mcv_list_payload;
use pgrust_catalog_data::{
    BOOL_TYPE_OID, CURRENT_DATABASE_OID, INT2_TYPE_OID, INT4_TYPE_OID, INT8_TYPE_OID,
    REGCLASS_TYPE_OID, REGTYPE_TYPE_OID, SYSTEM_CATALOG_FOREIGN_KEYS, TEXT_TYPE_OID,
};
use pgrust_nodes::SqlTypeKind;
use pgrust_nodes::datetime::{TimestampTzADT, USECS_PER_DAY, USECS_PER_SEC};
use pgrust_nodes::datum::{ArrayValue, Value};
use pgrust_nodes::primnodes::{
    Expr, QueryColumn, RowsFromItem, RowsFromSource, SetReturningCall, TextSearchTableFunction,
    set_returning_call_exprs,
};
use pgrust_nodes::tsearch::TsWeight;
use pgrust_nodes::{EventTriggerDdlCommandRow, EventTriggerDroppedObjectRow};

#[derive(Debug, Clone, PartialEq)]
pub enum UnnestError {
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum PgOptionsToTableError {
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum SrfValueError {
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    UnsupportedTsStatQuery,
    DirectoryOpen {
        display_name: String,
        message: String,
    },
    DirectoryRead {
        display_name: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerateSubscriptsError {
    Int4OutOfRange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceViewRow {
    pub oid: u32,
    pub schema: String,
    pub name: String,
    pub owner: String,
    pub type_oid: u32,
    pub start: i64,
    pub minvalue: i64,
    pub maxvalue: i64,
    pub increment: i64,
    pub cycle: bool,
    pub cache: i64,
    pub last_value: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorViewRow {
    pub name: String,
    pub statement: String,
    pub is_holdable: bool,
    pub is_binary: bool,
    pub is_scrollable: bool,
    pub creation_time: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedStatementViewRow {
    pub name: String,
    pub statement: String,
    pub prepare_time: i64,
    pub parameter_type_oids: Vec<u32>,
    pub result_type_oids: Option<Vec<u32>>,
    pub from_sql: bool,
    pub generic_plans: i64,
    pub custom_plans: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedXactViewRow {
    pub transaction: u32,
    pub gid: String,
    pub prepared_at: i64,
    pub owner_name: String,
    pub database_name: String,
}

#[derive(Debug, Clone)]
pub struct UnnestRows {
    arrays: Vec<Option<Vec<Value>>>,
    max_len: usize,
    output_width: usize,
    expand_single_composite: bool,
    index: usize,
}

impl UnnestRows {
    pub fn new(
        arrays: Vec<Option<Vec<Value>>>,
        output_width: usize,
        expand_single_composite: bool,
    ) -> Self {
        let max_len = arrays
            .iter()
            .filter_map(|array| array.as_ref().map(Vec::len))
            .max()
            .unwrap_or(0);
        Self {
            arrays,
            max_len,
            output_width,
            expand_single_composite,
            index: 0,
        }
    }

    pub fn with_args_and_columns(
        arrays: Vec<Option<Vec<Value>>>,
        args: &[Expr],
        output_columns: &[QueryColumn],
    ) -> Self {
        Self::new(
            arrays,
            output_columns.len(),
            unnest_expands_single_composite_arg(args, output_columns),
        )
    }

    pub fn next_row(&mut self) -> Result<Option<Vec<Value>>, UnnestError> {
        if self.index >= self.max_len {
            return Ok(None);
        }
        let idx = self.index;
        self.index += 1;

        if self.expand_single_composite {
            let value = self
                .arrays
                .first()
                .and_then(|array| array.as_ref())
                .and_then(|values| values.get(idx))
                .cloned()
                .unwrap_or(Value::Null);
            let mut fields = match value {
                Value::Record(record) => record.fields,
                Value::Null => vec![Value::Null; self.output_width],
                other => {
                    return Err(UnnestError::TypeMismatch {
                        op: "unnest",
                        left: other,
                        right: Value::Null,
                    });
                }
            };
            fields.resize(self.output_width, Value::Null);
            fields.truncate(self.output_width);
            return Ok(Some(fields));
        }

        let mut row = Vec::with_capacity(self.arrays.len());
        for array in &self.arrays {
            match array {
                Some(values) => row.push(values.get(idx).cloned().unwrap_or(Value::Null)),
                None => row.push(Value::Null),
            }
        }
        Ok(Some(row))
    }
}

#[derive(Debug, Clone)]
pub struct PgOptionsToTableRows {
    values: std::vec::IntoIter<Value>,
}

impl PgOptionsToTableRows {
    pub fn new(values: Vec<Value>) -> Self {
        Self {
            values: values.into_iter(),
        }
    }

    pub fn next_row(&mut self) -> Result<Option<Vec<Value>>, PgOptionsToTableError> {
        for value in self.values.by_ref() {
            if matches!(value, Value::Null) {
                continue;
            }
            let Some(option) = value.as_text() else {
                return Err(PgOptionsToTableError::TypeMismatch {
                    op: "pg_options_to_table",
                    left: value,
                    right: Value::Null,
                });
            };
            let (name, option_value) = option
                .split_once('=')
                .map(|(name, value)| (name, Value::Text(value.into())))
                .unwrap_or((option, Value::Null));
            return Ok(Some(vec![Value::Text(name.into()), option_value]));
        }
        Ok(None)
    }
}

pub fn unnest_array_values(array: ArrayValue) -> Vec<Value> {
    array.elements
}

pub fn publication_names_from_values(values: &[Value]) -> Result<Vec<String>, SrfValueError> {
    if values.len() == 1 {
        return publication_names_from_single_value(&values[0]);
    }
    values
        .iter()
        .filter(|value| !matches!(value, Value::Null))
        .map(publication_name_from_value)
        .collect()
}

fn publication_names_from_single_value(value: &Value) -> Result<Vec<String>, SrfValueError> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Array(values) => publication_names_from_array_values(values),
        Value::PgArray(array) => publication_names_from_array_values(&array.elements),
        other => {
            if let Some(array) = normalize_array_value(other) {
                publication_names_from_array_values(&array.elements)
            } else {
                Ok(vec![publication_name_from_value(other)?])
            }
        }
    }
}

fn publication_names_from_array_values(values: &[Value]) -> Result<Vec<String>, SrfValueError> {
    values
        .iter()
        .filter(|value| !matches!(value, Value::Null))
        .map(publication_name_from_value)
        .collect()
}

fn publication_name_from_value(value: &Value) -> Result<String, SrfValueError> {
    value
        .as_text()
        .map(ToOwned::to_owned)
        .ok_or_else(|| SrfValueError::TypeMismatch {
            op: "pg_get_publication_tables",
            left: value.clone(),
            right: Value::Text(String::new().into()),
        })
}

fn normalize_array_value(value: &Value) -> Option<ArrayValue> {
    match value {
        Value::PgArray(array) => Some(array.clone()),
        Value::Array(items) => Some(ArrayValue::from_1d(items.clone())),
        _ => None,
    }
}

pub fn srf_file_timestamp_value(time: std::io::Result<std::time::SystemTime>) -> Value {
    const UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS: i64 = 10_957;
    match time
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
    {
        Some(duration) => {
            let usecs =
                duration.as_secs() as i64 * USECS_PER_SEC + i64::from(duration.subsec_micros());
            Value::TimestampTz(TimestampTzADT(
                usecs - UNIX_EPOCH_TO_POSTGRES_EPOCH_DAYS * USECS_PER_DAY,
            ))
        }
        None => Value::Null,
    }
}

pub fn srf_io_error_message(err: &std::io::Error) -> String {
    match err.kind() {
        std::io::ErrorKind::NotFound => "No such file or directory".into(),
        _ => err.to_string(),
    }
}

pub fn directory_entry_rows(
    path: &std::path::Path,
    display_name: &str,
    missing_ok: bool,
    include_metadata: bool,
) -> Result<Vec<Vec<Value>>, SrfValueError> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(err) if missing_ok && err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(err) => {
            return Err(SrfValueError::DirectoryOpen {
                display_name: display_name.into(),
                message: srf_io_error_message(&err),
            });
        }
    };
    let mut rows = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| SrfValueError::DirectoryRead {
            display_name: display_name.into(),
            message: err.to_string(),
        })?;
        let name = entry.file_name().to_string_lossy().to_string();
        if include_metadata {
            let metadata = entry.metadata().ok();
            rows.push(vec![
                Value::Text(name.into()),
                Value::Int64(metadata.as_ref().map(|m| m.len() as i64).unwrap_or(0)),
                metadata
                    .map(|m| srf_file_timestamp_value(m.modified()))
                    .unwrap_or(Value::Null),
            ]);
        } else {
            rows.push(vec![Value::Text(name.into())]);
        }
    }
    rows.sort_by(|left, right| {
        let left = left.first().and_then(Value::as_text).unwrap_or("");
        let right = right.first().and_then(Value::as_text).unwrap_or("");
        left.cmp(right)
    });
    Ok(rows)
}

pub fn pg_ls_dir_rows(
    path: &std::path::Path,
    display_name: &str,
    missing_ok: bool,
    include_dot_dirs: bool,
) -> Result<Vec<Vec<Value>>, SrfValueError> {
    let mut rows = directory_entry_rows(path, display_name, missing_ok, false)?;
    if display_name == "."
        && !rows
            .iter()
            .any(|row| row.first().and_then(Value::as_text) == Some("base"))
    {
        // :HACK: pgrust's storage layout is not PostgreSQL's base/ tree yet,
        // but data-directory inspection functions expose that top-level name.
        rows.push(vec![Value::Text("base".into())]);
    }
    if include_dot_dirs {
        rows.push(vec![Value::Text(".".into())]);
        rows.push(vec![Value::Text("..".into())]);
    }
    rows.sort_by(|left, right| {
        let left = left.first().and_then(Value::as_text).unwrap_or("");
        let right = right.first().and_then(Value::as_text).unwrap_or("");
        left.cmp(right)
    });
    Ok(rows)
}

pub fn pg_ls_named_dir_rows(
    path: &std::path::Path,
    display_name: &str,
    synthesize_wal_segment: bool,
    wal_segment_size_bytes: i64,
    statement_timestamp_usecs: i64,
) -> Result<Vec<Vec<Value>>, SrfValueError> {
    let mut rows = directory_entry_rows(path, display_name, true, true)?;
    if rows.is_empty() && synthesize_wal_segment {
        rows.push(vec![
            Value::Text("000000010000000000000000".into()),
            Value::Int64(wal_segment_size_bytes),
            Value::TimestampTz(TimestampTzADT(statement_timestamp_usecs)),
        ]);
    }
    Ok(rows)
}

pub fn text_array<'a>(values: impl IntoIterator<Item = &'a str>) -> Value {
    Value::PgArray(
        ArrayValue::from_1d(
            values
                .into_iter()
                .map(|value| Value::Text(value.to_string().into()))
                .collect(),
        )
        .with_element_type_oid(TEXT_TYPE_OID),
    )
}

pub fn int4_array(values: impl IntoIterator<Item = i32>) -> Value {
    Value::PgArray(
        ArrayValue::from_1d(values.into_iter().map(Value::Int32).collect())
            .with_element_type_oid(INT4_TYPE_OID),
    )
}

pub fn regtype_array(values: impl IntoIterator<Item = u32>) -> Value {
    Value::PgArray(
        ArrayValue::from_1d(
            values
                .into_iter()
                .map(|oid| Value::Int64(i64::from(oid)))
                .collect(),
        )
        .with_element_type_oid(REGTYPE_TYPE_OID),
    )
}

pub fn text_search_table_function_for_proc_src(prosrc: &str) -> Option<TextSearchTableFunction> {
    match prosrc {
        "ts_token_type_byid" | "ts_token_type_byname" => Some(TextSearchTableFunction::TokenType),
        "ts_parse_byid" | "ts_parse_byname" => Some(TextSearchTableFunction::Parse),
        "ts_debug" => Some(TextSearchTableFunction::Debug),
        "ts_stat1" | "ts_stat2" => Some(TextSearchTableFunction::Stat),
        _ => None,
    }
}

pub fn parse_ts_stat_weights(value: &str) -> Vec<TsWeight> {
    value.chars().filter_map(TsWeight::from_char).collect()
}

pub fn parse_ts_stat_select(query: &str) -> Result<(&str, &str), SrfValueError> {
    let trimmed = query.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    let Some(rest) = lower.strip_prefix("select ") else {
        return Err(SrfValueError::UnsupportedTsStatQuery);
    };
    let Some(from_pos) = rest.find(" from ") else {
        return Err(SrfValueError::UnsupportedTsStatQuery);
    };
    let column_start = "select ".len();
    let column_end = column_start + from_pos;
    let table_start = column_end + " from ".len();
    let column = trimmed[column_start..column_end].trim();
    let table = trimmed[table_start..]
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .trim();
    if column.is_empty() || table.is_empty() || column.contains(',') {
        return Err(SrfValueError::UnsupportedTsStatQuery);
    }
    Ok((column.trim_matches('"'), table.trim_matches('"')))
}

pub fn sequence_type_display(type_oid: u32) -> (&'static str, i32) {
    match type_oid {
        INT2_TYPE_OID => ("smallint", 16),
        INT4_TYPE_OID => ("integer", 32),
        INT8_TYPE_OID => ("bigint", 64),
        _ => ("bigint", 64),
    }
}

pub fn pg_sequences_rows(rows: impl IntoIterator<Item = SequenceViewRow>) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            let (type_name, _) = sequence_type_display(row.type_oid);
            let last_value = row.last_value.map(Value::Int64).unwrap_or(Value::Null);
            vec![
                Value::Text(row.schema.into()),
                Value::Text(row.name.into()),
                Value::Text(row.owner.into()),
                Value::Text(type_name.into()),
                Value::Int64(row.start),
                Value::Int64(row.minvalue),
                Value::Int64(row.maxvalue),
                Value::Int64(row.increment),
                Value::Bool(row.cycle),
                Value::Int64(row.cache),
                last_value,
            ]
        })
        .collect()
}

pub fn information_schema_sequence_rows(
    sequence_catalog_name: &str,
    rows: impl IntoIterator<Item = SequenceViewRow>,
) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            let (type_name, precision) = sequence_type_display(row.type_oid);
            vec![
                Value::Text(sequence_catalog_name.to_string().into()),
                Value::Text(row.schema.into()),
                Value::Text(row.name.into()),
                Value::Text(type_name.into()),
                Value::Int32(precision),
                Value::Int32(2),
                Value::Int32(0),
                Value::Text(row.start.to_string().into()),
                Value::Text(row.minvalue.to_string().into()),
                Value::Text(row.maxvalue.to_string().into()),
                Value::Text(row.increment.to_string().into()),
                Value::Text(if row.cycle { "YES" } else { "NO" }.into()),
            ]
        })
        .collect()
}

pub fn pg_cursor_rows(rows: impl IntoIterator<Item = CursorViewRow>) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            vec![
                Value::Text(row.name.into()),
                Value::Text(row.statement.into()),
                Value::Bool(row.is_holdable),
                Value::Bool(row.is_binary),
                Value::Bool(row.is_scrollable),
                Value::TimestampTz(TimestampTzADT(row.creation_time)),
            ]
        })
        .collect()
}

pub fn pg_prepared_statement_rows(
    rows: impl IntoIterator<Item = PreparedStatementViewRow>,
) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            vec![
                Value::Text(row.name.into()),
                Value::Text(row.statement.into()),
                Value::TimestampTz(TimestampTzADT(row.prepare_time)),
                regtype_array(row.parameter_type_oids),
                row.result_type_oids
                    .map(regtype_array)
                    .unwrap_or(Value::Null),
                Value::Bool(row.from_sql),
                Value::Int64(row.generic_plans),
                Value::Int64(row.custom_plans),
            ]
        })
        .collect()
}

pub fn pg_prepared_xact_rows(
    rows: impl IntoIterator<Item = PreparedXactViewRow>,
) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            vec![
                Value::Int64(i64::from(row.transaction)),
                Value::Text(row.gid.into()),
                Value::TimestampTz(TimestampTzADT(row.prepared_at)),
                Value::Text(row.owner_name.into()),
                Value::Text(row.database_name.into()),
            ]
        })
        .collect()
}

pub fn pg_mcv_list_item_rows(values: &[Value]) -> Result<Vec<Vec<Value>>, String> {
    let [Value::Bytea(bytes)] = values else {
        return Ok(Vec::new());
    };
    let payload = decode_pg_mcv_list_payload(bytes)?;
    Ok(payload
        .items
        .into_iter()
        .enumerate()
        .map(|(index, item)| {
            let values = item
                .values
                .iter()
                .map(|value| {
                    value
                        .as_ref()
                        .map(|value| Value::Text(value.clone().into()))
                        .unwrap_or(Value::Null)
                })
                .collect::<Vec<_>>();
            let nulls = item
                .values
                .iter()
                .map(|value| Value::Bool(value.is_none()))
                .collect::<Vec<_>>();
            vec![
                Value::Int32(index as i32),
                Value::PgArray(ArrayValue::from_1d(values).with_element_type_oid(TEXT_TYPE_OID)),
                Value::PgArray(ArrayValue::from_1d(nulls).with_element_type_oid(BOOL_TYPE_OID)),
                Value::Float64(item.frequency),
                Value::Float64(item.base_frequency),
            ]
        })
        .collect())
}

pub fn pg_get_catalog_foreign_key_rows() -> Vec<Vec<Value>> {
    SYSTEM_CATALOG_FOREIGN_KEYS
        .iter()
        .map(|row| {
            let fk_columns = row
                .fk_columns
                .iter()
                .map(|column| Value::Text((*column).into()))
                .collect();
            let pk_columns = row
                .pk_columns
                .iter()
                .map(|column| Value::Text((*column).into()))
                .collect();
            vec![
                Value::Int32(row.fk_table_oid as i32),
                Value::PgArray(
                    ArrayValue::from_1d(fk_columns).with_element_type_oid(TEXT_TYPE_OID),
                ),
                Value::Int32(row.pk_table_oid as i32),
                Value::PgArray(
                    ArrayValue::from_1d(pk_columns).with_element_type_oid(TEXT_TYPE_OID),
                ),
                Value::Bool(row.is_array),
                Value::Bool(row.is_opt),
            ]
        })
        .collect()
}

pub fn pg_tablespace_databases_rows(values: &[Value]) -> Vec<Vec<Value>> {
    if matches!(values.first(), Some(Value::Null) | None) {
        return Vec::new();
    }
    vec![vec![Value::Int64(i64::from(CURRENT_DATABASE_OID))]]
}

pub fn pg_backend_memory_context_rows() -> Vec<Vec<Value>> {
    [
        memory_context_row(
            "TopMemoryContext",
            None,
            "AllocSet",
            1,
            &[1],
            8192,
            1,
            1024,
            4,
        ),
        memory_context_row(
            "CacheMemoryContext",
            None,
            "AllocSet",
            2,
            &[1, 1],
            16384,
            2,
            2048,
            8,
        ),
        memory_context_row(
            "CatalogCache",
            Some("pg_class"),
            "AllocSet",
            3,
            &[1, 1, 1],
            8192,
            1,
            1024,
            3,
        ),
        memory_context_row(
            "Type information cache",
            None,
            "AllocSet",
            3,
            &[1, 1, 2],
            8192,
            1,
            1024,
            3,
        ),
        memory_context_row("Caller tuples", None, "Bump", 2, &[1, 2], 8192, 2, 1024, 0),
    ]
    .into_iter()
    .collect()
}

#[allow(clippy::too_many_arguments)]
fn memory_context_row(
    name: &str,
    ident: Option<&str>,
    typ: &str,
    level: i32,
    path: &[i32],
    total_bytes: i64,
    total_nblocks: i64,
    free_bytes: i64,
    free_chunks: i64,
) -> Vec<Value> {
    vec![
        Value::Text(name.into()),
        ident
            .map(|ident| Value::Text(ident.into()))
            .unwrap_or(Value::Null),
        Value::Text(typ.into()),
        Value::Int32(level),
        int4_array(path.iter().copied()),
        Value::Int64(total_bytes),
        Value::Int64(total_nblocks),
        Value::Int64(free_bytes),
        Value::Int64(free_chunks),
        Value::Int64(total_bytes.saturating_sub(free_bytes)),
    ]
}

pub fn pg_config_fallback_rows() -> Vec<Vec<Value>> {
    [
        ("BINDIR", "/usr/local/pgsql/bin"),
        ("DOCDIR", "/usr/local/pgsql/share/doc"),
        ("HTMLDIR", "/usr/local/pgsql/share/doc/html"),
        ("INCLUDEDIR", "/usr/local/pgsql/include"),
        ("PKGINCLUDEDIR", "/usr/local/pgsql/include/postgresql"),
        ("INCLUDEDIR-SERVER", "/usr/local/pgsql/include/server"),
        ("LIBDIR", "/usr/local/pgsql/lib"),
        ("PKGLIBDIR", "/usr/local/pgsql/lib/postgresql"),
        ("LOCALEDIR", "/usr/local/pgsql/share/locale"),
        ("MANDIR", "/usr/local/pgsql/share/man"),
        ("SHAREDIR", "/usr/local/pgsql/share/postgresql"),
        ("SYSCONFDIR", "/usr/local/pgsql/etc"),
        (
            "PGXS",
            "/usr/local/pgsql/lib/postgresql/pgxs/src/makefiles/pgxs.mk",
        ),
        ("CONFIGURE", ""),
        ("CC", "cc"),
        ("CPPFLAGS", ""),
        ("CFLAGS", "-O2"),
        ("CFLAGS_SL", ""),
        ("LDFLAGS", ""),
        ("LDFLAGS_EX", ""),
        ("LDFLAGS_SL", ""),
        ("LIBS", ""),
        ("VERSION", "PostgreSQL 18"),
    ]
    .into_iter()
    .map(|(name, setting)| vec![Value::Text(name.into()), Value::Text(setting.into())])
    .collect()
}

#[cfg(not(target_arch = "wasm32"))]
pub fn local_pg_config_rows() -> Option<Vec<Vec<Value>>> {
    let output = std::process::Command::new("pg_config").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let rows = stdout
        .lines()
        .filter_map(|line| line.split_once(" = "))
        .map(|(name, setting)| {
            vec![
                Value::Text(name.to_string().into()),
                Value::Text(setting.to_string().into()),
            ]
        })
        .collect::<Vec<_>>();
    Some(rows)
}

#[cfg(target_arch = "wasm32")]
pub fn local_pg_config_rows() -> Option<Vec<Vec<Value>>> {
    None
}

pub fn pg_hba_file_rule_rows() -> Vec<Vec<Value>> {
    vec![vec![
        Value::Int32(1),
        Value::Text("pg_hba.conf".into()),
        Value::Int32(1),
        Value::Text("local".into()),
        text_array(["all"]),
        text_array(["all"]),
        Value::Null,
        Value::Null,
        Value::Text("trust".into()),
        text_array(std::iter::empty::<&str>()),
        Value::Null,
    ]]
}

pub fn pg_show_all_settings_rows(
    wal_segment_size: &str,
    output_columns: &[QueryColumn],
) -> Vec<Vec<Value>> {
    let mut settings = vec![(
        "wal_segment_size",
        wal_segment_size,
        "Write-Ahead Log / Settings",
        "Sets the size of WAL files held for WAL records.",
        "integer",
    )];
    const ENABLE_SETTINGS: &[(&str, &str, &str, &str, &str)] = &[
        (
            "default_statistics_target",
            "100",
            "Query Tuning / Other Planner Options",
            "Sets the default statistics target.",
            "integer",
        ),
        (
            "enable_async_append",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_bitmapscan",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_distinct_reordering",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_gathermerge",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_group_by_reordering",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_hashagg",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_hashjoin",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_incremental_sort",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_indexonlyscan",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_indexscan",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_material",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_memoize",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_mergejoin",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_nestloop",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_parallel_append",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_parallel_hash",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_partition_pruning",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_partitionwise_aggregate",
            "off",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_partitionwise_join",
            "off",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_presorted_aggregate",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_self_join_elimination",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_seqscan",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_sort",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
        (
            "enable_tidscan",
            "on",
            "Query Tuning / Planner Method Configuration",
            "Enables a planner method.",
            "bool",
        ),
    ];
    settings.extend(ENABLE_SETTINGS.iter().copied());
    settings
        .iter()
        .map(|(name, setting, category, description, vartype)| {
            output_columns
                .iter()
                .map(|column| match column.name.as_str() {
                    "name" => Value::Text((*name).into()),
                    "setting" => Value::Text((*setting).into()),
                    "unit" => Value::Null,
                    "category" => Value::Text((*category).into()),
                    "short_desc" => Value::Text((*description).into()),
                    "extra_desc" => Value::Null,
                    "context" => Value::Text("user".into()),
                    "vartype" => Value::Text((*vartype).into()),
                    "source" => Value::Text("default".into()),
                    "min_val" => Value::Null,
                    "max_val" => Value::Null,
                    "enumvals" => Value::Null,
                    "boot_val" => Value::Text((*setting).into()),
                    "reset_val" => Value::Text((*setting).into()),
                    "sourcefile" => Value::Null,
                    "sourceline" => Value::Null,
                    "pending_restart" => Value::Bool(false),
                    _ => Value::Null,
                })
                .collect()
        })
        .collect()
}

pub fn event_trigger_dropped_object_rows(
    rows: impl IntoIterator<Item = EventTriggerDroppedObjectRow>,
) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            vec![
                Value::Int64(i64::from(row.classid)),
                Value::Int64(i64::from(row.objid)),
                Value::Int32(row.objsubid),
                Value::Bool(row.original),
                Value::Bool(row.normal),
                Value::Bool(row.is_temporary),
                Value::Text(row.object_type.into()),
                row.schema_name
                    .map(|schema| Value::Text(schema.into()))
                    .unwrap_or(Value::Null),
                row.object_name
                    .map(|name| Value::Text(name.into()))
                    .unwrap_or(Value::Null),
                Value::Text(row.object_identity.into()),
                Value::Array(
                    row.address_names
                        .into_iter()
                        .map(|name| Value::Text(name.into()))
                        .collect(),
                ),
                Value::Array(
                    row.address_args
                        .into_iter()
                        .map(|arg| Value::Text(arg.into()))
                        .collect(),
                ),
            ]
        })
        .collect()
}

pub fn event_trigger_ddl_command_rows(
    rows: impl IntoIterator<Item = EventTriggerDdlCommandRow>,
) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|row| {
            vec![
                Value::Int64(0),
                Value::Int64(0),
                Value::Int32(0),
                Value::Text(row.command_tag.into()),
                Value::Text(row.object_type.into()),
                row.schema_name
                    .map(|schema| Value::Text(schema.into()))
                    .unwrap_or(Value::Null),
                Value::Text(row.object_identity.into()),
                Value::Bool(false),
                Value::Null,
            ]
        })
        .collect()
}

pub fn partition_lookup_oid(value: Value, op: &'static str) -> Result<Option<u32>, SrfValueError> {
    match value {
        Value::Null => Ok(None),
        Value::Int32(v) if v >= 0 => Ok(Some(v as u32)),
        Value::Int64(v) if v >= 0 && v <= i64::from(u32::MAX) => Ok(Some(v as u32)),
        other => Err(SrfValueError::TypeMismatch {
            op,
            left: other,
            right: Value::Int64(i64::from(REGCLASS_TYPE_OID)),
        }),
    }
}

pub fn set_returning_call_label(call: &SetReturningCall) -> &str {
    match call {
        SetReturningCall::RowsFrom { .. } => "rows from",
        SetReturningCall::GenerateSeries { .. } => "generate_series",
        SetReturningCall::GenerateSubscripts { .. } => "generate_subscripts",
        SetReturningCall::Unnest { .. } => "unnest",
        SetReturningCall::JsonTableFunction { kind, .. } => match kind {
            pgrust_nodes::primnodes::JsonTableFunction::ObjectKeys => "json_object_keys",
            pgrust_nodes::primnodes::JsonTableFunction::Each => "json_each",
            pgrust_nodes::primnodes::JsonTableFunction::EachText => "json_each_text",
            pgrust_nodes::primnodes::JsonTableFunction::ArrayElements => "json_array_elements",
            pgrust_nodes::primnodes::JsonTableFunction::ArrayElementsText => {
                "json_array_elements_text"
            }
            pgrust_nodes::primnodes::JsonTableFunction::JsonbPathQuery => "jsonb_path_query",
            pgrust_nodes::primnodes::JsonTableFunction::JsonbPathQueryTz => "jsonb_path_query_tz",
            pgrust_nodes::primnodes::JsonTableFunction::JsonbObjectKeys => "jsonb_object_keys",
            pgrust_nodes::primnodes::JsonTableFunction::JsonbEach => "jsonb_each",
            pgrust_nodes::primnodes::JsonTableFunction::JsonbEachText => "jsonb_each_text",
            pgrust_nodes::primnodes::JsonTableFunction::JsonbArrayElements => {
                "jsonb_array_elements"
            }
            pgrust_nodes::primnodes::JsonTableFunction::JsonbArrayElementsText => {
                "jsonb_array_elements_text"
            }
        },
        SetReturningCall::JsonRecordFunction { kind, .. } => kind.name(),
        SetReturningCall::SqlJsonTable(_) => "json_table",
        SetReturningCall::SqlXmlTable(_) => "xmltable",
        SetReturningCall::RegexTableFunction { kind, .. } => match kind {
            pgrust_nodes::primnodes::RegexTableFunction::Matches => "regexp_matches",
            pgrust_nodes::primnodes::RegexTableFunction::SplitToTable => "regexp_split_to_table",
        },
        SetReturningCall::StringTableFunction { kind, .. } => match kind {
            pgrust_nodes::primnodes::StringTableFunction::StringToTable => "string_to_table",
        },
        SetReturningCall::PartitionTree { .. } => "pg_partition_tree",
        SetReturningCall::PartitionAncestors { .. } => "pg_partition_ancestors",
        SetReturningCall::PgLockStatus { .. } => "pg_lock_status",
        SetReturningCall::PgStatProgressCopy { .. } => "pg_stat_progress_copy",
        SetReturningCall::PgSequences { .. } => "pg_sequences",
        SetReturningCall::InformationSchemaSequences { .. } => "information_schema.sequences",
        SetReturningCall::TxidSnapshotXip { .. } => "txid_snapshot_xip",
        SetReturningCall::TextSearchTableFunction { kind, .. } => match kind {
            pgrust_nodes::primnodes::TextSearchTableFunction::TokenType => "ts_token_type",
            pgrust_nodes::primnodes::TextSearchTableFunction::Parse => "ts_parse",
            pgrust_nodes::primnodes::TextSearchTableFunction::Debug => "ts_debug",
            pgrust_nodes::primnodes::TextSearchTableFunction::Stat => "ts_stat",
        },
        SetReturningCall::UserDefined { function_name, .. } => function_name.as_str(),
    }
}

pub fn generate_subscripts_values(
    array: &ArrayValue,
    dimension: i32,
    reverse: bool,
) -> Result<Vec<Value>, GenerateSubscriptsError> {
    if dimension < 1 {
        return Ok(Vec::new());
    }
    let Some(dim) = array.dimensions.get((dimension - 1) as usize) else {
        return Ok(Vec::new());
    };
    if dim.length == 0 {
        return Ok(Vec::new());
    }
    let lower = dim.lower_bound;
    let upper = lower
        .checked_add(dim.length as i32)
        .and_then(|value| value.checked_sub(1))
        .ok_or(GenerateSubscriptsError::Int4OutOfRange)?;
    let range: Box<dyn Iterator<Item = i32>> = if reverse {
        Box::new((lower..=upper).rev())
    } else {
        Box::new(lower..=upper)
    };
    Ok(range.map(Value::Int32).collect())
}

pub fn pg_wait_event_rows() -> Vec<Vec<Value>> {
    [
        (
            "Activity",
            "AutoVacuumMain",
            "autovacuum launcher is waiting",
        ),
        (
            "BufferPin",
            "BufferPin",
            "waiting to acquire a pin on a buffer",
        ),
        (
            "Client",
            "ClientRead",
            "waiting to read data from the client",
        ),
        ("Extension", "Extension", "waiting in an extension"),
        ("IO", "DataFileRead", "waiting for a data file read"),
        (
            "IPC",
            "BgWorkerShutdown",
            "waiting for background worker shutdown",
        ),
        ("LWLock", "BufferContent", "waiting for a lightweight lock"),
        ("Lock", "Relation", "waiting for a relation lock"),
        ("Timeout", "PgSleep", "waiting due to pg_sleep"),
    ]
    .into_iter()
    .map(|(typ, name, description)| {
        vec![
            Value::Text(typ.into()),
            Value::Text(name.into()),
            Value::Text(description.into()),
        ]
    })
    .collect()
}

pub fn pg_timezone_name_rows() -> Vec<Vec<Value>> {
    (-12i32..=14)
        .map(|offset_hours| {
            let sign = if offset_hours < 0 { "minus" } else { "plus" };
            vec![
                Value::Text(format!("Etc/GMT/{sign}/{}", offset_hours.abs()).into()),
                Value::Text(format!("GMT{offset_hours:+03}").into()),
                interval_seconds(offset_hours * 60 * 60),
                Value::Bool(false),
            ]
        })
        .collect()
}

pub fn pg_timezone_abbrev_rows() -> Vec<Vec<Value>> {
    let mut rows = (-12i32..=14)
        .map(|offset_hours| {
            vec![
                Value::Text(format!("TZA{offset_hours:+03}").into()),
                interval_seconds(offset_hours * 60 * 60),
                Value::Bool(false),
            ]
        })
        .collect::<Vec<_>>();
    rows.push(vec![
        Value::Text("LMT".into()),
        interval_seconds(-(7 * 60 * 60 + 52 * 60 + 58)),
        Value::Bool(false),
    ]);
    rows
}

fn interval_seconds(seconds: i32) -> Value {
    Value::Interval(pgrust_nodes::datum::IntervalValue {
        time_micros: i64::from(seconds) * USECS_PER_SEC,
        days: 0,
        months: 0,
    })
}

pub fn single_row_function_scan_values(value: Value, output_width: usize) -> Vec<Vec<Value>> {
    let values = match value {
        Value::Record(record) if output_width == record.fields.len() => record.fields,
        Value::Null if output_width != 1 => vec![Value::Null; output_width],
        other if output_width == 1 => vec![other],
        other => vec![other],
    };
    vec![values]
}

pub fn function_output_columns(
    output_columns: &[QueryColumn],
    with_ordinality: bool,
) -> &[QueryColumn] {
    if with_ordinality {
        output_columns
            .split_last()
            .map(|(_, base)| base)
            .unwrap_or(output_columns)
    } else {
        output_columns
    }
}

pub fn combine_rows_from_item_values(
    item_widths: &[usize],
    mut function_rows: Vec<Vec<Vec<Value>>>,
) -> Vec<Vec<Value>> {
    let max_rows = function_rows.iter().map(Vec::len).max().unwrap_or(0);
    let mut output = Vec::with_capacity(max_rows);
    for row_index in 0..max_rows {
        let mut values = Vec::new();
        for (width, rows) in item_widths.iter().zip(function_rows.iter_mut()) {
            if let Some(row) = rows.get_mut(row_index) {
                values.extend(row.iter().cloned());
            } else {
                values.extend(std::iter::repeat_n(Value::Null, *width));
            }
        }
        output.push(values);
    }
    output
}

pub fn rows_from_cache_key(item_index: usize, item: &RowsFromItem) -> Option<String> {
    (!rows_from_item_uses_outer_columns(item))
        .then(|| format!("rows_from_item:{item_index}:{:?}", item.source))
}

pub fn rows_from_item_uses_outer_columns(item: &RowsFromItem) -> bool {
    match &item.source {
        RowsFromSource::Function(call) => set_returning_call_exprs(call)
            .into_iter()
            .any(expr_uses_outer_columns),
        RowsFromSource::Project { output_exprs, .. } => {
            output_exprs.iter().any(expr_uses_outer_columns)
        }
    }
}

pub fn expr_uses_outer_columns(expr: &Expr) -> bool {
    match expr {
        Expr::Var(var) => var.varlevelsup > 0,
        Expr::Param(_) => true,
        Expr::Aggref(aggref) => {
            aggref.args.iter().any(expr_uses_outer_columns)
                || aggref
                    .aggfilter
                    .as_ref()
                    .is_some_and(expr_uses_outer_columns)
        }
        Expr::GroupingKey(grouping_key) => expr_uses_outer_columns(&grouping_key.expr),
        Expr::GroupingFunc(grouping_func) => grouping_func.args.iter().any(expr_uses_outer_columns),
        Expr::WindowFunc(window_func) => {
            window_func.args.iter().any(expr_uses_outer_columns)
                || match &window_func.kind {
                    pgrust_nodes::primnodes::WindowFuncKind::Aggregate(aggref) => aggref
                        .aggfilter
                        .as_ref()
                        .is_some_and(expr_uses_outer_columns),
                    pgrust_nodes::primnodes::WindowFuncKind::Builtin(_) => false,
                }
        }
        Expr::Op(op) => op.args.iter().any(expr_uses_outer_columns),
        Expr::Bool(bool_expr) => bool_expr.args.iter().any(expr_uses_outer_columns),
        Expr::Case(case_expr) => {
            case_expr
                .arg
                .as_deref()
                .is_some_and(expr_uses_outer_columns)
                || case_expr.args.iter().any(|arm| {
                    expr_uses_outer_columns(&arm.expr) || expr_uses_outer_columns(&arm.result)
                })
                || expr_uses_outer_columns(&case_expr.defresult)
        }
        Expr::CaseTest(_) => false,
        Expr::Func(func) => func.args.iter().any(expr_uses_outer_columns),
        Expr::SqlJsonQueryFunction(func) => {
            func.child_exprs().into_iter().any(expr_uses_outer_columns)
        }
        Expr::SetReturning(srf) => set_returning_call_exprs(&srf.call)
            .into_iter()
            .any(expr_uses_outer_columns),
        Expr::SubLink(sublink) => sublink
            .testexpr
            .as_deref()
            .is_some_and(expr_uses_outer_columns),
        Expr::SubPlan(subplan) => subplan
            .testexpr
            .as_deref()
            .is_some_and(expr_uses_outer_columns),
        Expr::ScalarArrayOp(saop) => {
            expr_uses_outer_columns(&saop.left) || expr_uses_outer_columns(&saop.right)
        }
        Expr::Cast(inner, _)
        | Expr::Collate { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner) => expr_uses_outer_columns(inner),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        }
        | Expr::Similar {
            expr,
            pattern,
            escape,
            ..
        } => {
            expr_uses_outer_columns(expr)
                || expr_uses_outer_columns(pattern)
                || escape.as_deref().is_some_and(expr_uses_outer_columns)
        }
        Expr::IsDistinctFrom(left, right)
        | Expr::IsNotDistinctFrom(left, right)
        | Expr::Coalesce(left, right) => {
            expr_uses_outer_columns(left) || expr_uses_outer_columns(right)
        }
        Expr::ArrayLiteral { elements, .. } => elements.iter().any(expr_uses_outer_columns),
        Expr::Row { fields, .. } => fields.iter().any(|(_, expr)| expr_uses_outer_columns(expr)),
        Expr::FieldSelect { expr, .. } => expr_uses_outer_columns(expr),
        Expr::ArraySubscript { array, subscripts } => {
            expr_uses_outer_columns(array)
                || subscripts.iter().any(|subscript| {
                    subscript
                        .lower
                        .as_ref()
                        .is_some_and(expr_uses_outer_columns)
                        || subscript
                            .upper
                            .as_ref()
                            .is_some_and(expr_uses_outer_columns)
                })
        }
        Expr::Xml(xml) => xml.child_exprs().any(expr_uses_outer_columns),
        Expr::Const(_)
        | Expr::Random
        | Expr::CurrentDate
        | Expr::CurrentCatalog
        | Expr::CurrentSchema
        | Expr::CurrentUser
        | Expr::SessionUser
        | Expr::User
        | Expr::SystemUser
        | Expr::CurrentRole
        | Expr::CurrentTime { .. }
        | Expr::CurrentTimestamp { .. }
        | Expr::LocalTime { .. }
        | Expr::LocalTimestamp { .. } => false,
    }
}

pub fn unnest_expands_single_composite_arg(args: &[Expr], output_columns: &[QueryColumn]) -> bool {
    if args.len() != 1 {
        return false;
    }
    if let Some(arg_type) = pgrust_nodes::primnodes::expr_sql_type_hint(&args[0]) {
        let element_type = if arg_type.is_array {
            arg_type.element_type()
        } else {
            arg_type
        };
        return matches!(
            element_type.kind,
            SqlTypeKind::Composite | SqlTypeKind::Record
        ) && (output_columns.len() != 1
            || output_columns
                .first()
                .is_some_and(|column| !column.name.eq_ignore_ascii_case("unnest")));
    }
    output_columns.len() > 1
        && output_columns
            .first()
            .is_some_and(|column| !column.name.eq_ignore_ascii_case("unnest"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_nodes::{SqlType, SqlTypeKind};

    fn column(name: &str, kind: SqlTypeKind) -> QueryColumn {
        QueryColumn {
            name: name.into(),
            sql_type: SqlType::new(kind),
            wire_type_oid: None,
        }
    }

    #[test]
    fn unnest_rows_null_pad_shorter_arrays() {
        let mut rows = UnnestRows::new(
            vec![
                Some(vec![Value::Int32(1), Value::Int32(2)]),
                Some(vec![Value::Text("a".into())]),
                None,
            ],
            3,
            false,
        );

        assert_eq!(
            rows.next_row().unwrap(),
            Some(vec![Value::Int32(1), Value::Text("a".into()), Value::Null])
        );
        assert_eq!(
            rows.next_row().unwrap(),
            Some(vec![Value::Int32(2), Value::Null, Value::Null])
        );
        assert_eq!(rows.next_row().unwrap(), None);
    }

    #[test]
    fn unnest_rows_expand_composite_records() {
        let record = Value::Record(pgrust_nodes::datum::RecordValue::anonymous(vec![
            ("a".into(), Value::Int32(1)),
            ("b".into(), Value::Text("x".into())),
        ]));
        let mut rows = UnnestRows::new(vec![Some(vec![record])], 3, true);

        assert_eq!(
            rows.next_row().unwrap(),
            Some(vec![Value::Int32(1), Value::Text("x".into()), Value::Null])
        );
        assert_eq!(rows.next_row().unwrap(), None);
    }

    #[test]
    fn unnest_rows_reject_non_record_when_expanding() {
        let mut rows = UnnestRows::new(vec![Some(vec![Value::Int32(1)])], 2, true);

        assert_eq!(
            rows.next_row().unwrap_err(),
            UnnestError::TypeMismatch {
                op: "unnest",
                left: Value::Int32(1),
                right: Value::Null,
            }
        );
    }

    #[test]
    fn single_composite_arg_expands_for_named_record_outputs() {
        let arg = Expr::Cast(
            Box::new(Expr::Const(Value::Null)),
            SqlType::array_of(SqlType::named_composite(42, 7)),
        );
        let columns = vec![
            column("a", SqlTypeKind::Int4),
            column("b", SqlTypeKind::Text),
        ];

        assert!(unnest_expands_single_composite_arg(&[arg], &columns));
    }

    #[test]
    fn single_row_function_scan_shapes_records_and_nulls() {
        let record = Value::Record(pgrust_nodes::datum::RecordValue::anonymous(vec![
            ("a".into(), Value::Int32(1)),
            ("b".into(), Value::Text("x".into())),
        ]));

        assert_eq!(
            single_row_function_scan_values(record, 2),
            vec![vec![Value::Int32(1), Value::Text("x".into())]]
        );
        assert_eq!(
            single_row_function_scan_values(Value::Null, 3),
            vec![vec![Value::Null, Value::Null, Value::Null]]
        );
    }

    #[test]
    fn combine_rows_from_items_null_pads_shorter_outputs() {
        let rows = combine_rows_from_item_values(
            &[2, 1],
            vec![
                vec![vec![Value::Int32(1), Value::Int32(2)]],
                vec![vec![Value::Text("a".into())], vec![Value::Text("b".into())]],
            ],
        );

        assert_eq!(
            rows,
            vec![
                vec![Value::Int32(1), Value::Int32(2), Value::Text("a".into())],
                vec![Value::Null, Value::Null, Value::Text("b".into())],
            ]
        );
    }

    #[test]
    fn pg_options_to_table_splits_names_and_values() {
        let mut rows = PgOptionsToTableRows::new(vec![
            Value::Null,
            Value::Text("fillfactor=70".into()),
            Value::Text("toast.autovacuum_enabled".into()),
        ]);

        assert_eq!(
            rows.next_row().unwrap(),
            Some(vec![
                Value::Text("fillfactor".into()),
                Value::Text("70".into())
            ])
        );
        assert_eq!(
            rows.next_row().unwrap(),
            Some(vec![
                Value::Text("toast.autovacuum_enabled".into()),
                Value::Null
            ])
        );
        assert_eq!(rows.next_row().unwrap(), None);
    }

    #[test]
    fn pg_options_to_table_rejects_non_text_options() {
        let mut rows = PgOptionsToTableRows::new(vec![Value::Int32(1)]);

        assert_eq!(
            rows.next_row().unwrap_err(),
            PgOptionsToTableError::TypeMismatch {
                op: "pg_options_to_table",
                left: Value::Int32(1),
                right: Value::Null,
            }
        );
    }

    #[test]
    fn generate_subscripts_respects_lower_bound_and_reverse() {
        let array = ArrayValue::from_dimensions(
            vec![pgrust_nodes::datum::ArrayDimension {
                lower_bound: 3,
                length: 3,
            }],
            vec![
                Value::Text("a".into()),
                Value::Text("b".into()),
                Value::Text("c".into()),
            ],
        );

        assert_eq!(
            generate_subscripts_values(&array, 1, false).unwrap(),
            vec![Value::Int32(3), Value::Int32(4), Value::Int32(5)]
        );
        assert_eq!(
            generate_subscripts_values(&array, 1, true).unwrap(),
            vec![Value::Int32(5), Value::Int32(4), Value::Int32(3)]
        );
    }

    #[test]
    fn publication_names_accept_scalar_array_and_variadic_values() {
        assert_eq!(
            publication_names_from_values(&[Value::Text("pub1".into())]).unwrap(),
            vec!["pub1".to_string()]
        );
        assert_eq!(
            publication_names_from_values(&[Value::PgArray(ArrayValue::from_1d(vec![
                Value::Text("pub1".into()),
                Value::Null,
                Value::Text("pub2".into()),
            ]))])
            .unwrap(),
            vec!["pub1".to_string(), "pub2".to_string()]
        );
        assert_eq!(
            publication_names_from_values(&[
                Value::Text("pub1".into()),
                Value::Null,
                Value::Text("pub2".into()),
            ])
            .unwrap(),
            vec!["pub1".to_string(), "pub2".to_string()]
        );
    }

    #[test]
    fn publication_names_reject_non_text_values() {
        assert_eq!(
            publication_names_from_values(&[Value::Int32(1)]).unwrap_err(),
            SrfValueError::TypeMismatch {
                op: "pg_get_publication_tables",
                left: Value::Int32(1),
                right: Value::Text(String::new().into()),
            }
        );
    }

    #[test]
    fn system_srf_array_helpers_assign_element_type_oids() {
        assert_eq!(
            text_array(["all"]),
            Value::PgArray(
                ArrayValue::from_1d(vec![Value::Text("all".into())])
                    .with_element_type_oid(TEXT_TYPE_OID)
            )
        );
        assert_eq!(
            int4_array([1, 2]),
            Value::PgArray(
                ArrayValue::from_1d(vec![Value::Int32(1), Value::Int32(2)])
                    .with_element_type_oid(INT4_TYPE_OID)
            )
        );
        assert_eq!(
            regtype_array([23]),
            Value::PgArray(
                ArrayValue::from_1d(vec![Value::Int64(23)]).with_element_type_oid(REGTYPE_TYPE_OID)
            )
        );
    }

    #[test]
    fn text_search_proc_src_maps_supported_table_functions() {
        assert_eq!(
            text_search_table_function_for_proc_src("ts_debug"),
            Some(TextSearchTableFunction::Debug)
        );
        assert_eq!(
            text_search_table_function_for_proc_src("ts_stat2"),
            Some(TextSearchTableFunction::Stat)
        );
        assert_eq!(text_search_table_function_for_proc_src("unknown"), None);
    }

    #[test]
    fn ts_stat_select_parser_accepts_simple_selects_only() {
        assert_eq!(
            parse_ts_stat_select(r#" SELECT "body" FROM "docs"; "#).unwrap(),
            ("body", "docs")
        );
        assert!(matches!(
            parse_ts_stat_select("select a, b from docs"),
            Err(SrfValueError::UnsupportedTsStatQuery)
        ));
    }

    #[test]
    fn sequence_type_display_matches_integral_sequence_types() {
        assert_eq!(sequence_type_display(INT2_TYPE_OID), ("smallint", 16));
        assert_eq!(sequence_type_display(INT4_TYPE_OID), ("integer", 32));
        assert_eq!(sequence_type_display(INT8_TYPE_OID), ("bigint", 64));
        assert_eq!(sequence_type_display(0), ("bigint", 64));
    }

    #[test]
    fn pg_sequences_rows_shape_sequence_view_values() {
        let row = SequenceViewRow {
            oid: 42,
            schema: "public".into(),
            name: "s".into(),
            owner: "postgres".into(),
            type_oid: INT4_TYPE_OID,
            start: 1,
            minvalue: 1,
            maxvalue: i64::from(i32::MAX),
            increment: 2,
            cycle: true,
            cache: 3,
            last_value: Some(7),
        };

        assert_eq!(
            pg_sequences_rows([row]),
            vec![vec![
                Value::Text("public".into()),
                Value::Text("s".into()),
                Value::Text("postgres".into()),
                Value::Text("integer".into()),
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(i64::from(i32::MAX)),
                Value::Int64(2),
                Value::Bool(true),
                Value::Int64(3),
                Value::Int64(7),
            ]]
        );
    }

    #[test]
    fn information_schema_sequence_rows_shape_sequence_metadata() {
        let row = SequenceViewRow {
            oid: 42,
            schema: "public".into(),
            name: "s".into(),
            owner: "postgres".into(),
            type_oid: INT4_TYPE_OID,
            start: 1,
            minvalue: 1,
            maxvalue: i64::from(i32::MAX),
            increment: 2,
            cycle: false,
            cache: 3,
            last_value: Some(7),
        };

        assert_eq!(
            information_schema_sequence_rows("regression", [row]),
            vec![vec![
                Value::Text("regression".into()),
                Value::Text("public".into()),
                Value::Text("s".into()),
                Value::Text("integer".into()),
                Value::Int32(32),
                Value::Int32(2),
                Value::Int32(0),
                Value::Text("1".into()),
                Value::Text("1".into()),
                Value::Text(i32::MAX.to_string().into()),
                Value::Text("2".into()),
                Value::Text("NO".into()),
            ]]
        );
    }

    #[test]
    fn session_srf_rows_shape_cursor_and_prepared_statement_views() {
        assert_eq!(
            pg_cursor_rows([CursorViewRow {
                name: "c".into(),
                statement: "select 1".into(),
                is_holdable: true,
                is_binary: false,
                is_scrollable: true,
                creation_time: 123,
            }]),
            vec![vec![
                Value::Text("c".into()),
                Value::Text("select 1".into()),
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(true),
                Value::TimestampTz(TimestampTzADT(123)),
            ]]
        );

        assert_eq!(
            pg_prepared_statement_rows([PreparedStatementViewRow {
                name: "p".into(),
                statement: "select $1".into(),
                prepare_time: 456,
                parameter_type_oids: vec![INT4_TYPE_OID],
                result_type_oids: Some(vec![TEXT_TYPE_OID]),
                from_sql: false,
                generic_plans: 2,
                custom_plans: 3,
            }]),
            vec![vec![
                Value::Text("p".into()),
                Value::Text("select $1".into()),
                Value::TimestampTz(TimestampTzADT(456)),
                regtype_array(vec![INT4_TYPE_OID]),
                regtype_array(vec![TEXT_TYPE_OID]),
                Value::Bool(false),
                Value::Int64(2),
                Value::Int64(3),
            ]]
        );
    }

    #[test]
    fn prepared_xact_rows_shape_transaction_metadata() {
        assert_eq!(
            pg_prepared_xact_rows([PreparedXactViewRow {
                transaction: 42,
                gid: "gx".into(),
                prepared_at: 789,
                owner_name: "postgres".into(),
                database_name: "regression".into(),
            }]),
            vec![vec![
                Value::Int64(42),
                Value::Text("gx".into()),
                Value::TimestampTz(TimestampTzADT(789)),
                Value::Text("postgres".into()),
                Value::Text("regression".into()),
            ]]
        );
    }

    #[test]
    fn mcv_list_item_rows_decode_payload_to_system_view_rows() {
        let bytes = pgrust_catalog_data::statistics_payload::encode_pg_mcv_list_payload(
            &pgrust_catalog_data::statistics_payload::PgMcvListPayload {
                items: vec![pgrust_catalog_data::statistics_payload::PgMcvItem {
                    values: vec![Some("a".into()), None],
                    frequency: 0.25,
                    base_frequency: 0.125,
                }],
            },
        )
        .unwrap();

        let rows = pg_mcv_list_item_rows(&[Value::Bytea(bytes)]).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int32(0));
        assert_eq!(
            rows[0][1],
            Value::PgArray(
                ArrayValue::from_1d(vec![Value::Text("a".into()), Value::Null])
                    .with_element_type_oid(TEXT_TYPE_OID)
            )
        );
        assert_eq!(
            rows[0][2],
            Value::PgArray(
                ArrayValue::from_1d(vec![Value::Bool(false), Value::Bool(true)])
                    .with_element_type_oid(BOOL_TYPE_OID)
            )
        );
        assert_eq!(rows[0][3], Value::Float64(0.25));
        assert_eq!(rows[0][4], Value::Float64(0.125));
    }

    #[test]
    fn system_srf_static_rows_have_expected_shapes() {
        assert!(
            pg_get_catalog_foreign_key_rows()
                .into_iter()
                .all(|row| row.len() == 6)
        );
        assert_eq!(pg_tablespace_databases_rows(&[]), Vec::<Vec<Value>>::new());
        assert_eq!(
            pg_tablespace_databases_rows(&[Value::Int64(1)]),
            vec![vec![Value::Int64(i64::from(CURRENT_DATABASE_OID))]]
        );
        assert!(
            pg_backend_memory_context_rows()
                .into_iter()
                .any(|row| row.first() == Some(&Value::Text("TopMemoryContext".into())))
        );
        assert_eq!(pg_hba_file_rule_rows()[0][4], text_array(["all"]),);
        assert!(pg_config_fallback_rows().into_iter().any(|row| row
            == vec![
                Value::Text("VERSION".into()),
                Value::Text("PostgreSQL 18".into())
            ]));
    }

    #[test]
    fn directory_entry_rows_respects_missing_ok() {
        let missing = std::path::Path::new("/definitely/not/a/pgrust/test/path");
        assert_eq!(
            directory_entry_rows(missing, "missing", true, false).unwrap(),
            Vec::<Vec<Value>>::new()
        );
        assert!(matches!(
            directory_entry_rows(missing, "missing", false, false),
            Err(SrfValueError::DirectoryOpen { .. })
        ));
    }

    #[test]
    fn event_trigger_rows_shape_sql_visible_values() {
        let dropped = event_trigger_dropped_object_rows([EventTriggerDroppedObjectRow {
            classid: 1,
            objid: 2,
            objsubid: 3,
            original: true,
            normal: false,
            is_temporary: false,
            object_type: "table".into(),
            schema_name: Some("public".into()),
            object_name: Some("t".into()),
            object_identity: "public.t".into(),
            address_names: vec!["public".into(), "t".into()],
            address_args: vec![],
        }]);
        assert_eq!(dropped[0][0], Value::Int64(1));
        assert_eq!(dropped[0][6], Value::Text("table".into()));
        assert_eq!(
            dropped[0][10],
            Value::Array(vec![Value::Text("public".into()), Value::Text("t".into())])
        );

        let ddl = event_trigger_ddl_command_rows([EventTriggerDdlCommandRow {
            command_tag: "CREATE TABLE".into(),
            object_type: "table".into(),
            schema_name: None,
            object_identity: "public.t".into(),
        }]);
        assert_eq!(ddl[0][3], Value::Text("CREATE TABLE".into()));
        assert_eq!(ddl[0][5], Value::Null);
    }

    #[test]
    fn partition_lookup_oid_accepts_regclass_oids() {
        assert_eq!(
            partition_lookup_oid(Value::Null, "pg_partition_tree").unwrap(),
            None
        );
        assert_eq!(
            partition_lookup_oid(Value::Int32(42), "pg_partition_tree").unwrap(),
            Some(42)
        );
        assert!(matches!(
            partition_lookup_oid(Value::Int64(-1), "pg_partition_tree"),
            Err(SrfValueError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn pg_show_all_settings_uses_requested_columns() {
        let columns = vec![
            column("name", SqlTypeKind::Text),
            column("setting", SqlTypeKind::Text),
        ];
        let rows = pg_show_all_settings_rows("16777216", &columns);
        assert!(rows.contains(&vec![
            Value::Text("wal_segment_size".into()),
            Value::Text("16777216".into()),
        ]));
        assert!(rows.iter().any(|row| {
            row == &vec![
                Value::Text("enable_hashjoin".into()),
                Value::Text("on".into()),
            ]
        }));
    }

    #[test]
    fn pg_wait_events_include_core_lock_event() {
        assert!(pg_wait_event_rows().into_iter().any(|row| {
            row == vec![
                Value::Text("Lock".into()),
                Value::Text("Relation".into()),
                Value::Text("waiting for a relation lock".into()),
            ]
        }));
    }

    #[test]
    fn timezone_abbrevs_include_lmt() {
        assert!(
            pg_timezone_abbrev_rows()
                .into_iter()
                .any(|row| row.first() == Some(&Value::Text("LMT".into())))
        );
    }
}
