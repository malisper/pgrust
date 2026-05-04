use std::collections::HashMap;
use std::sync::Arc;

use pgrust_catalog_data::PgProcRow;
use pgrust_nodes::{SqlType, TriggerCallContext, primnodes::RelationDesc};

use crate::{is_polymorphic_type_oid, parse_proc_argtype_oids};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PlpgsqlFunctionCacheKey {
    Routine {
        proc_oid: u32,
        resolved_result_type: Option<SqlType>,
        actual_arg_types: Vec<Option<SqlType>>,
    },
    Trigger {
        proc_oid: u32,
        relation_shape: RelationShape,
        transition_tables: Vec<TransitionTableShape>,
    },
    EventTrigger {
        proc_oid: u32,
    },
}

impl PlpgsqlFunctionCacheKey {
    fn proc_oid(&self) -> u32 {
        match self {
            Self::Routine { proc_oid, .. }
            | Self::Trigger { proc_oid, .. }
            | Self::EventTrigger { proc_oid } => *proc_oid,
        }
    }
}

pub fn routine_cache_key(
    row: &PgProcRow,
    resolved_result_type: Option<SqlType>,
    actual_arg_types: &[Option<SqlType>],
) -> PlpgsqlFunctionCacheKey {
    if row_uses_polymorphic_types(row) {
        PlpgsqlFunctionCacheKey::Routine {
            proc_oid: row.oid,
            resolved_result_type,
            actual_arg_types: actual_arg_types.to_vec(),
        }
    } else {
        PlpgsqlFunctionCacheKey::Routine {
            proc_oid: row.oid,
            resolved_result_type: None,
            actual_arg_types: Vec::new(),
        }
    }
}

pub fn row_uses_polymorphic_types(row: &PgProcRow) -> bool {
    is_polymorphic_type_oid(row.prorettype)
        || parse_proc_argtype_oids(&row.proargtypes)
            .unwrap_or_default()
            .into_iter()
            .any(is_polymorphic_type_oid)
        || row
            .proallargtypes
            .as_ref()
            .is_some_and(|types| types.iter().copied().any(is_polymorphic_type_oid))
}

pub fn trigger_cache_key(proc_oid: u32, call: &TriggerCallContext) -> PlpgsqlFunctionCacheKey {
    PlpgsqlFunctionCacheKey::Trigger {
        proc_oid,
        relation_shape: RelationShape::from_desc(&call.relation_desc),
        transition_tables: call
            .transition_tables
            .iter()
            .map(|table| TransitionTableShape {
                name: table.name.clone(),
                relation_shape: RelationShape::from_desc(&table.desc),
            })
            .collect(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RelationShape {
    columns: Vec<ColumnShape>,
}

impl RelationShape {
    pub fn from_desc(desc: &RelationDesc) -> Self {
        Self {
            columns: desc
                .columns
                .iter()
                .map(|column| ColumnShape {
                    name: column.name.clone(),
                    sql_type: column.sql_type,
                    dropped: column.dropped,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ColumnShape {
    name: String,
    sql_type: SqlType,
    dropped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransitionTableShape {
    pub name: String,
    pub relation_shape: RelationShape,
}

#[derive(Debug, Clone)]
struct PlpgsqlFunctionCacheEntry<T> {
    proc_row: PgProcRow,
    compiled: Arc<T>,
}

#[derive(Debug)]
pub struct PlpgsqlFunctionCache<T> {
    entries: HashMap<PlpgsqlFunctionCacheKey, PlpgsqlFunctionCacheEntry<T>>,
}

impl<T> Default for PlpgsqlFunctionCache<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

impl<T> PlpgsqlFunctionCache<T> {
    pub fn get_valid(
        &self,
        key: &PlpgsqlFunctionCacheKey,
        current_row: &PgProcRow,
    ) -> Option<Arc<T>> {
        let entry = self.entries.get(key)?;
        (entry.proc_row == *current_row).then(|| Arc::clone(&entry.compiled))
    }

    pub fn insert(&mut self, key: PlpgsqlFunctionCacheKey, proc_row: PgProcRow, compiled: Arc<T>) {
        self.entries
            .insert(key, PlpgsqlFunctionCacheEntry { proc_row, compiled });
    }

    pub fn remove_proc(&mut self, proc_oid: u32) {
        self.entries.retain(|key, _| key.proc_oid() != proc_oid);
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
