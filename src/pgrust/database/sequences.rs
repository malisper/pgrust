use super::*;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{
    SequenceOptionsPatchSpec, SequenceOptionsSpec, SequenceOwnedByClause, SerialKind, SqlType,
    SqlTypeKind,
};
use crate::include::catalog::{INT2_TYPE_OID, INT4_TYPE_OID, INT8_TYPE_OID};
use crate::include::nodes::datum::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SequenceState {
    pub(crate) last_value: i64,
    pub(crate) is_called: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SequenceOwnedByRef {
    pub(crate) relation_oid: u32,
    pub(crate) attnum: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SequenceOptions {
    pub(crate) type_oid: u32,
    pub(crate) increment: i64,
    pub(crate) minvalue: i64,
    pub(crate) maxvalue: i64,
    pub(crate) start: i64,
    pub(crate) cache: i64,
    pub(crate) cycle: bool,
    pub(crate) owned_by: Option<SequenceOwnedByRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SequenceData {
    pub(crate) options: SequenceOptions,
    pub(crate) state: SequenceState,
}

#[derive(Debug, Clone)]
pub(crate) enum SequenceMutationEffect {
    Upsert {
        relation_oid: u32,
        previous: Option<SequenceData>,
        new: SequenceData,
        persistent: bool,
    },
    Drop {
        relation_oid: u32,
        persistent: bool,
    },
}

#[derive(Debug)]
pub struct SequenceRuntime {
    data_dir: Option<PathBuf>,
    data: RwLock<HashMap<u32, SequenceData>>,
    currvals: RwLock<HashMap<(ClientId, u32), i64>>,
}

impl SequenceRuntime {
    pub(crate) fn load(
        base_dir: Option<&Path>,
        catalog: &CatalogStore,
    ) -> Result<Self, CatalogError> {
        let data_dir = base_dir.map(Path::to_path_buf);
        let mut data = HashMap::new();
        for (_, entry) in catalog
            .catalog_snapshot()?
            .entries()
            .filter(|(_, entry)| entry.relkind == 'S')
        {
            let payload = load_sequence_file(data_dir.as_deref(), entry.relation_oid)?;
            data.insert(entry.relation_oid, payload);
        }
        Ok(Self {
            data_dir,
            data: RwLock::new(data),
            currvals: RwLock::new(HashMap::new()),
        })
    }

    pub(crate) fn new_ephemeral() -> Self {
        Self {
            data_dir: None,
            data: RwLock::new(HashMap::new()),
            currvals: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn sequence_relation_desc() -> RelationDesc {
        RelationDesc {
            columns: vec![
                column_desc("last_value", SqlType::new(SqlTypeKind::Int8), false),
                column_desc("log_cnt", SqlType::new(SqlTypeKind::Int8), false),
                column_desc("is_called", SqlType::new(SqlTypeKind::Bool), false),
            ],
        }
    }

    pub(crate) fn sql_type_for_type_oid(type_oid: u32) -> Option<SqlType> {
        match type_oid {
            INT2_TYPE_OID => Some(SqlType::new(SqlTypeKind::Int2)),
            INT4_TYPE_OID => Some(SqlType::new(SqlTypeKind::Int4)),
            INT8_TYPE_OID => Some(SqlType::new(SqlTypeKind::Int8)),
            _ => None,
        }
    }

    pub(crate) fn sequence_data(&self, relation_oid: u32) -> Option<SequenceData> {
        self.data.read().get(&relation_oid).cloned()
    }

    pub(crate) fn all_sequences(&self) -> Vec<(u32, SequenceData)> {
        self.data
            .read()
            .iter()
            .map(|(oid, data)| (*oid, data.clone()))
            .collect()
    }

    pub(crate) fn apply_upsert(
        &self,
        relation_oid: u32,
        new: SequenceData,
        persistent: bool,
    ) -> SequenceMutationEffect {
        let previous = self.data.write().insert(relation_oid, new.clone());
        SequenceMutationEffect::Upsert {
            relation_oid,
            previous,
            new,
            persistent,
        }
    }

    pub(crate) fn queue_drop(&self, relation_oid: u32, persistent: bool) -> SequenceMutationEffect {
        SequenceMutationEffect::Drop {
            relation_oid,
            persistent,
        }
    }

    pub(crate) fn finalize_committed_effects(
        &self,
        effects: &[SequenceMutationEffect],
    ) -> Result<(), ExecError> {
        for effect in effects {
            match effect {
                SequenceMutationEffect::Upsert {
                    relation_oid,
                    persistent,
                    ..
                } => {
                    if *persistent {
                        let current = self.data.read().get(relation_oid).cloned();
                        if let Some(current) = current {
                            write_sequence_file(self.data_dir.as_deref(), *relation_oid, &current)
                                .map_err(sequence_io_error)?;
                        }
                    }
                }
                SequenceMutationEffect::Drop {
                    relation_oid,
                    persistent,
                } => {
                    self.data.write().remove(relation_oid);
                    self.currvals
                        .write()
                        .retain(|(_, oid), _| *oid != *relation_oid);
                    if *persistent {
                        delete_sequence_file(self.data_dir.as_deref(), *relation_oid)
                            .map_err(sequence_io_error)?;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn finalize_aborted_effects(&self, effects: &[SequenceMutationEffect]) {
        for effect in effects.iter().rev() {
            if let SequenceMutationEffect::Upsert {
                relation_oid,
                previous,
                ..
            } = effect
            {
                let mut data = self.data.write();
                if let Some(previous) = previous {
                    data.insert(*relation_oid, previous.clone());
                } else {
                    data.remove(relation_oid);
                }
            }
        }
    }

    pub(crate) fn clear_currvals_for_client(&self, client_id: ClientId) {
        self.currvals
            .write()
            .retain(|(owner, _), _| *owner != client_id);
    }

    pub(crate) fn current_row(&self, relation_oid: u32) -> Option<Vec<Value>> {
        let state = self.data.read().get(&relation_oid)?.state;
        Some(vec![
            Value::Int64(state.last_value),
            Value::Int64(0),
            Value::Bool(state.is_called),
        ])
    }

    pub(crate) fn next_value(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        persistent: bool,
    ) -> Result<i64, ExecError> {
        let next = self.allocate_value(relation_oid, persistent)?;
        self.currvals
            .write()
            .insert((client_id, relation_oid), next);
        Ok(next)
    }

    pub(crate) fn allocate_value(
        &self,
        relation_oid: u32,
        persistent: bool,
    ) -> Result<i64, ExecError> {
        let mut data = self.data.write();
        let entry = data
            .get_mut(&relation_oid)
            .ok_or_else(|| missing_sequence_error(relation_oid))?;
        let value = advance_sequence(entry)?;
        if persistent
            && let Some(existing) = sequence_file_path(self.data_dir.as_deref(), relation_oid)
                .filter(|path| path.exists())
        {
            write_sequence_file_at_path(&existing, entry).map_err(sequence_io_error)?;
        }
        Ok(value)
    }

    pub(crate) fn curr_value(
        &self,
        client_id: ClientId,
        relation_oid: u32,
    ) -> Result<i64, ExecError> {
        self.currvals
            .read()
            .get(&(client_id, relation_oid))
            .copied()
            .ok_or_else(|| ExecError::DetailedError {
                message: format!(
                    "currval of sequence {relation_oid} is not yet defined in this session"
                ),
                detail: None,
                hint: None,
                sqlstate: "55000",
            })
    }

    pub(crate) fn set_value(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        value: i64,
        is_called: bool,
        persistent: bool,
    ) -> Result<i64, ExecError> {
        {
            let mut data = self.data.write();
            let entry = data
                .get_mut(&relation_oid)
                .ok_or_else(|| missing_sequence_error(relation_oid))?;
            if value < entry.options.minvalue || value > entry.options.maxvalue {
                return Err(sequence_bounds_error(relation_oid));
            }
            entry.state.last_value = value;
            entry.state.is_called = is_called;
            if persistent {
                if let Some(existing) = sequence_file_path(self.data_dir.as_deref(), relation_oid)
                    .filter(|path| path.exists())
                {
                    write_sequence_file_at_path(&existing, entry).map_err(sequence_io_error)?;
                }
            }
        }
        if is_called {
            self.currvals
                .write()
                .insert((client_id, relation_oid), value);
        } else {
            self.currvals.write().remove(&(client_id, relation_oid));
        }
        Ok(value)
    }
}

pub(crate) fn sequence_type_oid_for_serial_kind(kind: SerialKind) -> u32 {
    match kind {
        SerialKind::Small => INT2_TYPE_OID,
        SerialKind::Regular => INT4_TYPE_OID,
        SerialKind::Big => INT8_TYPE_OID,
    }
}

pub(crate) fn sequence_type_oid_for_sql_type(sql_type: SqlType) -> Result<u32, ParseError> {
    match sql_type.kind {
        SqlTypeKind::Int2 => Ok(INT2_TYPE_OID),
        SqlTypeKind::Int4 => Ok(INT4_TYPE_OID),
        SqlTypeKind::Int8 => Ok(INT8_TYPE_OID),
        _ => Err(ParseError::UnexpectedToken {
            expected: "integer sequence type",
            actual: format!("{sql_type:?}"),
        }),
    }
}

pub(crate) fn default_sequence_name_base(table_name: &str, column_name: &str) -> String {
    let table = table_name.rsplit('.').next().unwrap_or(table_name);
    format!(
        "{}_{}_seq",
        table.to_ascii_lowercase(),
        column_name.to_ascii_lowercase()
    )
}

pub(crate) fn format_nextval_default(sequence_name: &str, sql_type: SqlType) -> String {
    let cast_name = match sql_type.kind {
        SqlTypeKind::Int2 => "int2",
        SqlTypeKind::Int4 => "int4",
        SqlTypeKind::Int8 => "int8",
        _ => "int8",
    };
    format!("nextval('{sequence_name}')::{cast_name}")
}

pub(crate) fn format_nextval_default_oid(sequence_oid: u32, sql_type: SqlType) -> String {
    let cast_name = match sql_type.kind {
        SqlTypeKind::Int2 => "int2",
        SqlTypeKind::Int4 => "int4",
        SqlTypeKind::Int8 => "int8",
        _ => "int8",
    };
    format!("nextval({sequence_oid})::{cast_name}")
}

pub(crate) fn default_sequence_oid_from_default_expr(default_expr: &str) -> Option<u32> {
    let expr = default_expr.trim();
    let rest = expr.strip_prefix("nextval(")?;
    if let Some(oid_end) = rest.find("::oid)") {
        return rest[..oid_end].trim().parse::<u32>().ok();
    }
    let oid_end = rest.find(')')?;
    rest[..oid_end].trim().parse::<u32>().ok()
}

pub(crate) fn resolve_sequence_options_spec(
    spec: &SequenceOptionsSpec,
    type_oid: u32,
) -> Result<SequenceOptions, ParseError> {
    let defaults = default_sequence_options(type_oid)?;
    let increment = spec.increment.unwrap_or(defaults.increment);
    let minvalue = spec
        .minvalue
        .unwrap_or(Some(defaults.minvalue))
        .unwrap_or_else(|| default_minvalue(type_oid, increment));
    let maxvalue = spec
        .maxvalue
        .unwrap_or(Some(defaults.maxvalue))
        .unwrap_or_else(|| default_maxvalue(type_oid, increment));
    let start = spec
        .start
        .unwrap_or_else(|| if increment > 0 { minvalue } else { maxvalue });
    let cache = spec.cache.unwrap_or(1);
    let cycle = spec.cycle.unwrap_or(false);
    validate_sequence_numbers(minvalue, maxvalue, start, increment, cache)?;
    let owned_by = match spec.owned_by.as_ref() {
        Some(SequenceOwnedByClause::None) | None => None,
        Some(SequenceOwnedByClause::Column { .. }) => None,
    };
    Ok(SequenceOptions {
        type_oid,
        increment,
        minvalue,
        maxvalue,
        start,
        cache,
        cycle,
        owned_by,
    })
}

pub(crate) fn apply_sequence_option_patch(
    current: &SequenceOptions,
    patch: &SequenceOptionsPatchSpec,
) -> Result<(SequenceOptions, Option<SequenceState>), ParseError> {
    let increment = patch.increment.unwrap_or(current.increment);
    let minvalue = patch
        .minvalue
        .unwrap_or(Some(current.minvalue))
        .unwrap_or_else(|| default_minvalue(current.type_oid, increment));
    let maxvalue = patch
        .maxvalue
        .unwrap_or(Some(current.maxvalue))
        .unwrap_or_else(|| default_maxvalue(current.type_oid, increment));
    let start = patch.start.unwrap_or(current.start);
    let cache = patch.cache.unwrap_or(current.cache);
    let cycle = patch.cycle.unwrap_or(current.cycle);
    validate_sequence_numbers(minvalue, maxvalue, start, increment, cache)?;
    let mut next = current.clone();
    next.increment = increment;
    next.minvalue = minvalue;
    next.maxvalue = maxvalue;
    next.start = start;
    next.cache = cache;
    next.cycle = cycle;
    let restart = patch.restart.map(|value| SequenceState {
        last_value: value.unwrap_or(next.start),
        is_called: false,
    });
    if matches!(patch.owned_by, Some(SequenceOwnedByClause::None)) {
        next.owned_by = None;
    }
    Ok((next, restart))
}

pub(crate) fn initial_sequence_state(options: &SequenceOptions) -> SequenceState {
    SequenceState {
        last_value: options.start,
        is_called: false,
    }
}

fn default_sequence_options(type_oid: u32) -> Result<SequenceOptions, ParseError> {
    let increment = 1i64;
    let minvalue = default_minvalue(type_oid, increment);
    let maxvalue = default_maxvalue(type_oid, increment);
    Ok(SequenceOptions {
        type_oid,
        increment,
        minvalue,
        maxvalue,
        start: minvalue,
        cache: 1,
        cycle: false,
        owned_by: None,
    })
}

fn default_minvalue(type_oid: u32, increment: i64) -> i64 {
    if increment > 0 {
        1
    } else {
        match type_oid {
            INT2_TYPE_OID => i16::MIN as i64,
            INT4_TYPE_OID => i32::MIN as i64,
            _ => i64::MIN,
        }
    }
}

fn default_maxvalue(type_oid: u32, increment: i64) -> i64 {
    if increment > 0 {
        match type_oid {
            INT2_TYPE_OID => i16::MAX as i64,
            INT4_TYPE_OID => i32::MAX as i64,
            _ => i64::MAX,
        }
    } else {
        -1
    }
}

fn validate_sequence_numbers(
    minvalue: i64,
    maxvalue: i64,
    start: i64,
    increment: i64,
    cache: i64,
) -> Result<(), ParseError> {
    if increment == 0 {
        return Err(ParseError::UnexpectedToken {
            expected: "non-zero INCREMENT",
            actual: "INCREMENT 0".into(),
        });
    }
    if cache <= 0 {
        return Err(ParseError::UnexpectedToken {
            expected: "positive CACHE value",
            actual: cache.to_string(),
        });
    }
    if minvalue > maxvalue {
        return Err(ParseError::UnexpectedToken {
            expected: "MINVALUE <= MAXVALUE",
            actual: format!("{minvalue} > {maxvalue}"),
        });
    }
    if start < minvalue || start > maxvalue {
        return Err(ParseError::UnexpectedToken {
            expected: "START value within sequence bounds",
            actual: start.to_string(),
        });
    }
    Ok(())
}

fn advance_sequence(entry: &mut SequenceData) -> Result<i64, ExecError> {
    if !entry.state.is_called {
        entry.state.is_called = true;
        return Ok(entry.state.last_value);
    }

    let increment = entry.options.increment;
    let next = entry
        .state
        .last_value
        .checked_add(increment)
        .ok_or_else(|| sequence_bounds_error(0))?;

    let wrapped = if increment > 0 {
        if next > entry.options.maxvalue {
            if entry.options.cycle {
                entry.options.minvalue
            } else {
                return Err(sequence_bounds_error(0));
            }
        } else {
            next
        }
    } else if next < entry.options.minvalue {
        if entry.options.cycle {
            entry.options.maxvalue
        } else {
            return Err(sequence_bounds_error(0));
        }
    } else {
        next
    };

    entry.state.last_value = wrapped;
    entry.state.is_called = true;
    Ok(wrapped)
}

fn missing_sequence_error(relation_oid: u32) -> ExecError {
    ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
}

fn sequence_bounds_error(relation_oid: u32) -> ExecError {
    let name = if relation_oid == 0 {
        "sequence".to_string()
    } else {
        format!("sequence {relation_oid}")
    };
    ExecError::DetailedError {
        message: format!("nextval: reached maximum value of {name}").into(),
        detail: None,
        hint: None,
        sqlstate: "2200H",
    }
}

fn sequence_io_error(error: std::io::Error) -> ExecError {
    ExecError::Parse(ParseError::UnexpectedToken {
        expected: "sequence state persistence",
        actual: error.to_string(),
    })
}

fn sequence_dir(base_dir: Option<&Path>) -> Option<PathBuf> {
    base_dir.map(|base| base.join("pg_sequences"))
}

fn sequence_file_path(base_dir: Option<&Path>, relation_oid: u32) -> Option<PathBuf> {
    sequence_dir(base_dir).map(|dir| dir.join(format!("{relation_oid}.json")))
}

fn load_sequence_file(
    base_dir: Option<&Path>,
    relation_oid: u32,
) -> Result<SequenceData, CatalogError> {
    let path = sequence_file_path(base_dir, relation_oid)
        .ok_or_else(|| CatalogError::Corrupt("durable sequence requires data directory"))?;
    let text = fs::read_to_string(&path).map_err(|e| CatalogError::Io(e.to_string()))?;
    serde_json::from_str(&text).map_err(|_| CatalogError::Corrupt("invalid sequence state file"))
}

fn write_sequence_file(
    base_dir: Option<&Path>,
    relation_oid: u32,
    data: &SequenceData,
) -> Result<(), std::io::Error> {
    let path = sequence_file_path(base_dir, relation_oid)
        .ok_or_else(|| std::io::Error::other("durable sequence requires data directory"))?;
    write_sequence_file_at_path(&path, data)
}

fn write_sequence_file_at_path(path: &Path, data: &SequenceData) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let text = serde_json::to_string_pretty(data).map_err(std::io::Error::other)?;
    fs::write(path, text)
}

fn delete_sequence_file(base_dir: Option<&Path>, relation_oid: u32) -> Result<(), std::io::Error> {
    let Some(path) = sequence_file_path(base_dir, relation_oid) else {
        return Ok(());
    };
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}
