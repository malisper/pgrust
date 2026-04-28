use std::collections::HashMap;
use std::sync::Arc;

use crate::backend::executor::RelationDesc;
use crate::backend::parser::SqlType;
use crate::include::catalog::PgProcRow;

use super::compile::CompiledFunction;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum PlpgsqlFunctionCacheKey {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RelationShape {
    columns: Vec<ColumnShape>,
}

impl RelationShape {
    pub(crate) fn from_desc(desc: &RelationDesc) -> Self {
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
pub(crate) struct TransitionTableShape {
    pub(crate) name: String,
    pub(crate) relation_shape: RelationShape,
}

#[derive(Debug, Clone)]
struct PlpgsqlFunctionCacheEntry {
    proc_row: PgProcRow,
    compiled: Arc<CompiledFunction>,
}

#[derive(Debug, Default)]
pub struct PlpgsqlFunctionCache {
    entries: HashMap<PlpgsqlFunctionCacheKey, PlpgsqlFunctionCacheEntry>,
}

impl PlpgsqlFunctionCache {
    pub(crate) fn get_valid(
        &self,
        key: &PlpgsqlFunctionCacheKey,
        current_row: &PgProcRow,
    ) -> Option<Arc<CompiledFunction>> {
        let entry = self.entries.get(key)?;
        (entry.proc_row == *current_row).then(|| Arc::clone(&entry.compiled))
    }

    pub(crate) fn insert(
        &mut self,
        key: PlpgsqlFunctionCacheKey,
        proc_row: PgProcRow,
        compiled: Arc<CompiledFunction>,
    ) {
        self.entries
            .insert(key, PlpgsqlFunctionCacheEntry { proc_row, compiled });
    }

    pub(crate) fn remove_proc(&mut self, proc_oid: u32) {
        self.entries.retain(|key, _| key.proc_oid() != proc_oid);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}
