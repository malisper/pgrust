#![allow(dead_code)]

use super::*;
use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{
    CatalogLookup, RawTypeName, SequenceOptionsPatchSpec, SequenceOptionsSpec,
    SequenceOwnedByClause, SerialKind, SqlType, SqlTypeKind,
};
use crate::include::catalog::{INT2_TYPE_OID, INT4_TYPE_OID, INT8_TYPE_OID, PgSequenceRow};
use crate::include::nodes::datum::Value;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SequenceState {
    pub(crate) last_value: i64,
    #[serde(default)]
    pub(crate) log_cnt: i64,
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

pub(crate) fn pg_sequence_row(seqrelid: u32, data: &SequenceData) -> PgSequenceRow {
    PgSequenceRow {
        seqrelid,
        seqtypid: data.options.type_oid,
        seqstart: data.options.start,
        seqincrement: data.options.increment,
        seqmax: data.options.maxvalue,
        seqmin: data.options.minvalue,
        seqcache: data.options.cache,
        seqcycle: data.options.cycle,
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    lastvals: RwLock<HashMap<ClientId, (u32, i64)>>,
    caches: RwLock<HashMap<(ClientId, u32), SequenceCacheBlock>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SequenceCacheBlock {
    next_value: i64,
    last_value: i64,
    increment: i64,
    remaining: i64,
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
            // PostgreSQL treats temp relation state as backend-local. Stale
            // temp sequence catalog rows are cleaned when the temp namespace is
            // reused, not loaded as durable sequence state during database open.
            .filter(|(_, entry)| entry.relkind == 'S' && entry.relpersistence != 't')
        {
            let payload = load_sequence_file(data_dir.as_deref(), entry.relation_oid)?;
            data.insert(entry.relation_oid, payload);
        }
        Ok(Self {
            data_dir,
            data: RwLock::new(data),
            currvals: RwLock::new(HashMap::new()),
            lastvals: RwLock::new(HashMap::new()),
            caches: RwLock::new(HashMap::new()),
        })
    }

    pub(crate) fn new_ephemeral() -> Self {
        Self {
            data_dir: None,
            data: RwLock::new(HashMap::new()),
            currvals: RwLock::new(HashMap::new()),
            lastvals: RwLock::new(HashMap::new()),
            caches: RwLock::new(HashMap::new()),
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
        self.caches
            .write()
            .retain(|(_, oid), _| *oid != relation_oid);
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
                    self.lastvals
                        .write()
                        .retain(|_, (oid, _)| *oid != *relation_oid);
                    self.caches
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
        self.lastvals.write().remove(&client_id);
        self.caches
            .write()
            .retain(|(owner, _), _| *owner != client_id);
    }

    pub(crate) fn clear_session_sequence_state(&self, client_id: ClientId) {
        self.clear_currvals_for_client(client_id);
    }

    pub(crate) fn current_row(&self, relation_oid: u32) -> Option<Vec<Value>> {
        let state = self.data.read().get(&relation_oid)?.state;
        Some(vec![
            Value::Int64(state.last_value),
            Value::Int64(state.log_cnt),
            Value::Bool(state.is_called),
        ])
    }

    pub(crate) fn next_value(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        persistent: bool,
    ) -> Result<i64, ExecError> {
        let next = self.next_cached_value(client_id, relation_oid, persistent)?;
        self.currvals
            .write()
            .insert((client_id, relation_oid), next);
        self.lastvals
            .write()
            .insert(client_id, (relation_oid, next));
        Ok(next)
    }

    fn next_cached_value(
        &self,
        client_id: ClientId,
        relation_oid: u32,
        persistent: bool,
    ) -> Result<i64, ExecError> {
        let cache_key = (client_id, relation_oid);
        {
            let mut caches = self.caches.write();
            if let Some(block) = caches.get_mut(&cache_key)
                && block.remaining > 0
            {
                let value = block.next_value;
                block.remaining -= 1;
                if block.remaining == 0 {
                    let _ = block;
                    caches.remove(&cache_key);
                } else {
                    block.next_value = block
                        .next_value
                        .checked_add(block.increment)
                        .ok_or_else(|| sequence_bounds_error(0, block.increment, false))?;
                }
                return Ok(value);
            }
        }

        let (value, block) = self.reserve_cache_block(relation_oid, persistent)?;
        if let Some(block) = block {
            self.caches.write().insert(cache_key, block);
        }
        Ok(value)
    }

    fn reserve_cache_block(
        &self,
        relation_oid: u32,
        persistent: bool,
    ) -> Result<(i64, Option<SequenceCacheBlock>), ExecError> {
        let mut data = self.data.write();
        let entry = data
            .get_mut(&relation_oid)
            .ok_or_else(|| missing_sequence_error(relation_oid))?;
        let (first, block) = reserve_sequence_cache(entry)?;
        if persistent
            && let Some(existing) = sequence_file_path(self.data_dir.as_deref(), relation_oid)
                .filter(|path| path.exists())
        {
            write_sequence_file_at_path(&existing, entry).map_err(sequence_io_error)?;
        }
        Ok((first, block))
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

    pub(crate) fn last_value(&self, client_id: ClientId) -> Result<(u32, i64), ExecError> {
        self.lastvals
            .read()
            .get(&client_id)
            .copied()
            .ok_or_else(|| ExecError::DetailedError {
                message: "lastval is not yet defined in this session".into(),
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
                return Err(setval_bounds_error(relation_oid, entry, value));
            }
            entry.state.last_value = value;
            entry.state.is_called = is_called;
            entry.state.log_cnt = if is_called { 32 } else { 0 };
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
            self.lastvals
                .write()
                .insert(client_id, (relation_oid, value));
        } else {
            self.currvals.write().remove(&(client_id, relation_oid));
        }
        self.caches
            .write()
            .retain(|(_, oid), _| *oid != relation_oid);
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

pub(crate) fn sequence_type_oid_for_raw_type(type_name: &RawTypeName) -> Result<u32, ParseError> {
    match type_name {
        RawTypeName::Builtin(sql_type) => {
            sequence_type_oid_for_sql_type(*sql_type).map_err(|_| ParseError::DetailedError {
                message: "sequence type must be smallint, integer, or bigint".into(),
                detail: None,
                hint: None,
                sqlstate: "0A000",
            })
        }
        RawTypeName::Named { name, .. } => match name.to_ascii_lowercase().as_str() {
            "pg_catalog.int2" | "int2" | "smallint" => Ok(INT2_TYPE_OID),
            "pg_catalog.int4" | "int4" | "int" | "integer" => Ok(INT4_TYPE_OID),
            "pg_catalog.int8" | "int8" | "bigint" => Ok(INT8_TYPE_OID),
            other => Err(ParseError::UnsupportedType(other.to_string())),
        },
        RawTypeName::Serial(_) | RawTypeName::Record => Err(ParseError::DetailedError {
            message: "sequence type must be smallint, integer, or bigint".into(),
            detail: None,
            hint: None,
            sqlstate: "0A000",
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

pub(crate) fn default_sequence_oid_from_default_expr_with_catalog(
    default_expr: &str,
    catalog: &dyn CatalogLookup,
) -> Option<u32> {
    if let Some(oid) = default_sequence_oid_from_default_expr(default_expr) {
        return Some(oid);
    }
    let expr = default_expr.trim();
    let rest = expr.strip_prefix("nextval(")?.trim_start();
    let literal = rest.strip_prefix('\'')?;
    let mut value = String::new();
    let mut chars = literal.char_indices().peekable();
    let mut end = None;
    while let Some((idx, ch)) = chars.next() {
        if ch == '\'' {
            if matches!(chars.peek(), Some((_, '\''))) {
                value.push('\'');
                let _ = chars.next();
            } else {
                end = Some(idx + 1);
                break;
            }
        } else {
            value.push(ch);
        }
    }
    let after_literal = literal[end?..].trim_start();
    if after_literal.to_ascii_lowercase().starts_with("::text") {
        return None;
    }
    if !(after_literal.starts_with(')')
        || after_literal.to_ascii_lowercase().starts_with("::regclass"))
    {
        return None;
    }
    catalog
        .lookup_any_relation(&value)
        .filter(|relation| relation.relkind == 'S')
        .map(|relation| relation.relation_oid)
}

pub(crate) fn resolve_sequence_options_spec(
    spec: &SequenceOptionsSpec,
    type_oid: u32,
) -> Result<SequenceOptions, ParseError> {
    let increment = spec.increment.unwrap_or(1);
    let minvalue = match spec.minvalue {
        Some(Some(value)) => value,
        Some(None) | None => default_minvalue(type_oid, increment),
    };
    let maxvalue = match spec.maxvalue {
        Some(Some(value)) => value,
        Some(None) | None => default_maxvalue(type_oid, increment),
    };
    let start = spec
        .start
        .unwrap_or_else(|| if increment > 0 { minvalue } else { maxvalue });
    let cache = spec.cache.unwrap_or(1);
    let cycle = spec.cycle.unwrap_or(false);
    validate_sequence_numbers(type_oid, minvalue, maxvalue, start, increment, cache)?;
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
    let type_oid = if let Some(ref as_type) = patch.as_type {
        sequence_type_oid_for_raw_type(as_type)?
    } else {
        current.type_oid
    };
    let increment = patch.increment.unwrap_or(current.increment);
    let (old_type_min, old_type_max) = sequence_type_range(current.type_oid);
    let (new_type_min, new_type_max) = sequence_type_range(type_oid);
    let minvalue = match patch.minvalue {
        Some(Some(value)) => value,
        Some(None) => default_minvalue(type_oid, increment),
        None if current.type_oid != type_oid && current.minvalue == old_type_min => new_type_min,
        None if current.minvalue == default_minvalue(current.type_oid, current.increment) => {
            default_minvalue(type_oid, increment)
        }
        None => current.minvalue,
    };
    let maxvalue = match patch.maxvalue {
        Some(Some(value)) => value,
        Some(None) => default_maxvalue(type_oid, increment),
        None if current.type_oid != type_oid && current.maxvalue == old_type_max => new_type_max,
        None if current.maxvalue == default_maxvalue(current.type_oid, current.increment) => {
            default_maxvalue(type_oid, increment)
        }
        None => current.maxvalue,
    };
    let start = patch.start.unwrap_or(current.start);
    let cache = patch.cache.unwrap_or(current.cache);
    let cycle = patch.cycle.unwrap_or(current.cycle);
    validate_sequence_numbers(type_oid, minvalue, maxvalue, start, increment, cache)?;
    let mut next = current.clone();
    next.type_oid = type_oid;
    next.increment = increment;
    next.minvalue = minvalue;
    next.maxvalue = maxvalue;
    next.start = start;
    next.cache = cache;
    next.cycle = cycle;
    let restart = if let Some(value) = patch.restart {
        let last_value = value.unwrap_or(next.start);
        validate_restart_value(last_value, next.minvalue, next.maxvalue)?;
        Some(SequenceState {
            last_value,
            log_cnt: 0,
            is_called: false,
        })
    } else {
        None
    };
    if matches!(patch.owned_by, Some(SequenceOwnedByClause::None)) {
        next.owned_by = None;
    }
    Ok((next, restart))
}

pub(crate) fn initial_sequence_state(options: &SequenceOptions) -> SequenceState {
    SequenceState {
        last_value: options.start,
        log_cnt: 0,
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
    type_oid: u32,
    minvalue: i64,
    maxvalue: i64,
    start: i64,
    increment: i64,
    cache: i64,
) -> Result<(), ParseError> {
    let (type_min, type_max) = sequence_type_range(type_oid);
    if increment == 0 {
        return Err(ParseError::DetailedError {
            message: "INCREMENT must not be zero".into(),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    if cache <= 0 {
        return Err(ParseError::DetailedError {
            message: format!("CACHE ({cache}) must be greater than zero"),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    if minvalue < type_min || minvalue > type_max {
        return Err(ParseError::DetailedError {
            message: format!(
                "MINVALUE ({minvalue}) is out of range for sequence data type {}",
                sequence_type_name(type_oid)
            ),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    if maxvalue < type_min || maxvalue > type_max {
        return Err(ParseError::DetailedError {
            message: format!(
                "MAXVALUE ({maxvalue}) is out of range for sequence data type {}",
                sequence_type_name(type_oid)
            ),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    if minvalue > maxvalue {
        return Err(ParseError::DetailedError {
            message: format!("MINVALUE ({minvalue}) must be less than MAXVALUE ({maxvalue})"),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    if start < minvalue {
        return Err(ParseError::DetailedError {
            message: format!("START value ({start}) cannot be less than MINVALUE ({minvalue})"),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    if start > maxvalue {
        return Err(ParseError::DetailedError {
            message: format!("START value ({start}) cannot be greater than MAXVALUE ({maxvalue})"),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    Ok(())
}

fn sequence_type_range(type_oid: u32) -> (i64, i64) {
    match type_oid {
        INT2_TYPE_OID => (i16::MIN as i64, i16::MAX as i64),
        INT4_TYPE_OID => (i32::MIN as i64, i32::MAX as i64),
        _ => (i64::MIN, i64::MAX),
    }
}

fn sequence_type_name(type_oid: u32) -> &'static str {
    match type_oid {
        INT2_TYPE_OID => "smallint",
        INT4_TYPE_OID => "integer",
        INT8_TYPE_OID => "bigint",
        _ => "bigint",
    }
}

fn validate_restart_value(value: i64, minvalue: i64, maxvalue: i64) -> Result<(), ParseError> {
    if value < minvalue {
        return Err(ParseError::DetailedError {
            message: format!("RESTART value ({value}) cannot be less than MINVALUE ({minvalue})"),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    if value > maxvalue {
        return Err(ParseError::DetailedError {
            message: format!(
                "RESTART value ({value}) cannot be greater than MAXVALUE ({maxvalue})"
            ),
            detail: None,
            hint: None,
            sqlstate: "22023",
        });
    }
    Ok(())
}

fn advance_sequence(entry: &mut SequenceData) -> Result<i64, ExecError> {
    if !entry.state.is_called {
        entry.state.is_called = true;
        entry.state.log_cnt = 32;
        return Ok(entry.state.last_value);
    }

    let increment = entry.options.increment;
    let next = entry
        .state
        .last_value
        .checked_add(increment)
        .ok_or_else(|| sequence_bounds_error(0, increment, false))?;

    let wrapped = if increment > 0 {
        if next > entry.options.maxvalue {
            if entry.options.cycle {
                entry.options.minvalue
            } else {
                return Err(sequence_bounds_error(0, increment, false));
            }
        } else {
            next
        }
    } else if next < entry.options.minvalue {
        if entry.options.cycle {
            entry.options.maxvalue
        } else {
            return Err(sequence_bounds_error(0, increment, false));
        }
    } else {
        next
    };

    entry.state.last_value = wrapped;
    entry.state.is_called = true;
    entry.state.log_cnt = 32;
    Ok(wrapped)
}

fn reserve_sequence_cache(
    entry: &mut SequenceData,
) -> Result<(i64, Option<SequenceCacheBlock>), ExecError> {
    let increment = entry.options.increment;
    let cache = entry.options.cache.max(1);
    let mut values = Vec::new();
    let mut candidate = if entry.state.is_called {
        entry
            .state
            .last_value
            .checked_add(increment)
            .ok_or_else(|| sequence_bounds_error(0, increment, false))?
    } else {
        entry.state.last_value
    };

    for _ in 0..cache {
        let value = normalize_sequence_value(entry, candidate)?;
        values.push(value);
        candidate = value
            .checked_add(increment)
            .ok_or_else(|| sequence_bounds_error(0, increment, false))?;
    }

    let first = values[0];
    let last = *values
        .last()
        .expect("sequence cache reserves at least one value");
    entry.state.last_value = last;
    entry.state.is_called = true;
    entry.state.log_cnt = 32;

    let block = if values.len() > 1 {
        Some(SequenceCacheBlock {
            next_value: values[1],
            last_value: last,
            increment,
            remaining: (values.len() - 1) as i64,
        })
    } else {
        None
    };
    Ok((first, block))
}

fn normalize_sequence_value(entry: &SequenceData, value: i64) -> Result<i64, ExecError> {
    let increment = entry.options.increment;
    if increment > 0 && value > entry.options.maxvalue {
        if entry.options.cycle {
            return Ok(entry.options.minvalue);
        }
        return Err(sequence_bounds_error(0, increment, false));
    }
    if increment < 0 && value < entry.options.minvalue {
        if entry.options.cycle {
            return Ok(entry.options.maxvalue);
        }
        return Err(sequence_bounds_error(0, increment, false));
    }
    Ok(value)
}

fn missing_sequence_error(relation_oid: u32) -> ExecError {
    ExecError::Parse(ParseError::TableDoesNotExist(relation_oid.to_string()))
}

fn sequence_bounds_error(relation_oid: u32, increment: i64, is_setval: bool) -> ExecError {
    let name = if relation_oid == 0 {
        "sequence".to_string()
    } else {
        format!("sequence {relation_oid}")
    };
    let bound = if increment < 0 { "minimum" } else { "maximum" };
    let function = if is_setval { "setval" } else { "nextval" };
    ExecError::DetailedError {
        message: format!("{function}: reached {bound} value of {name}").into(),
        detail: None,
        hint: None,
        sqlstate: "2200H",
    }
}

fn setval_bounds_error(relation_oid: u32, entry: &SequenceData, value: i64) -> ExecError {
    ExecError::DetailedError {
        message: format!(
            "setval: value {value} is out of bounds for sequence {relation_oid} ({}, {})",
            entry.options.minvalue, entry.options.maxvalue
        ),
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
