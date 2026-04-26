use std::collections::HashMap;

use crate::backend::executor::{ExecError, QueryColumn, Value, exec_next};
use crate::backend::parser::ParseError;
use crate::pgrust::session::SelectGuard;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortalStatus {
    Ready,
    Active,
    Done,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortalStrategy {
    OneSelect,
    OneReturning,
    UtilSelect,
    MultiQuery,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CursorOptions {
    pub holdable: bool,
    pub binary: bool,
    pub scroll: bool,
    pub no_scroll: bool,
    pub visible: bool,
}

impl CursorOptions {
    pub fn protocol() -> Self {
        Self {
            no_scroll: true,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortalFetchLimit {
    All,
    Count(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortalFetchDirection {
    Forward(PortalFetchLimit),
    Backward(PortalFetchLimit),
    Absolute(i64),
    Relative(i64),
    First,
    Last,
    Prior,
    Next,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortalRunResult {
    pub columns: Vec<QueryColumn>,
    pub column_names: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub processed: usize,
    pub completed: bool,
    pub command_tag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorViewRow {
    pub name: String,
    pub statement: String,
    pub is_holdable: bool,
    pub is_binary: bool,
    pub is_scrollable: bool,
}

pub enum PortalExecution {
    Streaming(SelectGuard),
    Materialized {
        columns: Vec<QueryColumn>,
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
        pos: usize,
    },
    PendingSql {
        sql: String,
        columns: Option<Vec<QueryColumn>>,
        column_names: Vec<String>,
    },
    CommandDone,
}

pub struct Portal {
    pub name: String,
    pub prep_stmt_name: Option<String>,
    pub source_text: String,
    pub command_tag: String,
    pub status: PortalStatus,
    pub strategy: PortalStrategy,
    pub options: CursorOptions,
    pub result_formats: Vec<i16>,
    pub created_in_transaction: bool,
    pub execution: PortalExecution,
}

impl Portal {
    pub fn streaming_select(
        name: String,
        source_text: String,
        prep_stmt_name: Option<String>,
        result_formats: Vec<i16>,
        options: CursorOptions,
        created_in_transaction: bool,
        guard: SelectGuard,
    ) -> Self {
        Self {
            name,
            prep_stmt_name,
            source_text,
            command_tag: "SELECT".into(),
            status: PortalStatus::Ready,
            strategy: PortalStrategy::OneSelect,
            options,
            result_formats,
            created_in_transaction,
            execution: PortalExecution::Streaming(guard),
        }
    }

    pub fn materialized_select(
        name: String,
        source_text: String,
        prep_stmt_name: Option<String>,
        result_formats: Vec<i16>,
        options: CursorOptions,
        created_in_transaction: bool,
        columns: Vec<QueryColumn>,
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
    ) -> Self {
        Self {
            name,
            prep_stmt_name,
            source_text,
            command_tag: "SELECT".into(),
            status: PortalStatus::Ready,
            strategy: PortalStrategy::OneSelect,
            options,
            result_formats,
            created_in_transaction,
            execution: PortalExecution::Materialized {
                columns,
                column_names,
                rows,
                pos: 0,
            },
        }
    }

    pub fn pending_sql(
        name: String,
        source_text: String,
        prep_stmt_name: Option<String>,
        result_formats: Vec<i16>,
        options: CursorOptions,
        created_in_transaction: bool,
        columns: Option<Vec<QueryColumn>>,
    ) -> Self {
        let column_names = columns
            .as_ref()
            .map(|columns| columns.iter().map(|column| column.name.clone()).collect())
            .unwrap_or_default();
        Self {
            name,
            prep_stmt_name,
            source_text: source_text.clone(),
            command_tag: String::new(),
            status: PortalStatus::Ready,
            strategy: PortalStrategy::MultiQuery,
            options,
            result_formats,
            created_in_transaction,
            execution: PortalExecution::PendingSql {
                sql: source_text,
                columns,
                column_names,
            },
        }
    }

    pub fn columns(&self) -> Option<Vec<QueryColumn>> {
        match &self.execution {
            PortalExecution::Streaming(guard) => Some(guard.columns.clone()),
            PortalExecution::Materialized { columns, .. } => Some(columns.clone()),
            PortalExecution::PendingSql { columns, .. } => columns.clone(),
            PortalExecution::CommandDone => None,
        }
    }

    pub fn column_names(&self) -> Vec<String> {
        match &self.execution {
            PortalExecution::Streaming(guard) => guard.column_names.clone(),
            PortalExecution::Materialized { column_names, .. } => column_names.clone(),
            PortalExecution::PendingSql { column_names, .. } => column_names.clone(),
            PortalExecution::CommandDone => Vec::new(),
        }
    }

    pub fn materialize_all(&mut self) -> Result<(), ExecError> {
        let PortalExecution::Streaming(guard) = &mut self.execution else {
            return Ok(());
        };
        let columns = guard.columns.clone();
        let column_names = guard.column_names.clone();
        let mut rows = Vec::new();
        while let Some(slot) = exec_next(&mut guard.state, &mut guard.ctx)? {
            let mut values = slot.values()?.to_vec();
            Value::materialize_all(&mut values);
            rows.push(values);
        }
        self.execution = PortalExecution::Materialized {
            columns,
            column_names,
            rows,
            pos: 0,
        };
        self.status = PortalStatus::Ready;
        Ok(())
    }

    pub fn fetch_forward(&mut self, limit: PortalFetchLimit) -> Result<PortalRunResult, ExecError> {
        self.status = PortalStatus::Active;
        let mut result = match &mut self.execution {
            PortalExecution::Streaming(guard) => fetch_streaming_forward(guard, limit),
            PortalExecution::Materialized {
                columns,
                column_names,
                rows,
                pos,
            } => Ok(fetch_materialized_forward(
                columns,
                column_names,
                rows,
                pos,
                limit,
            )),
            PortalExecution::PendingSql { .. } | PortalExecution::CommandDone => {
                Ok(PortalRunResult {
                    columns: Vec::new(),
                    column_names: Vec::new(),
                    rows: Vec::new(),
                    processed: 0,
                    completed: true,
                    command_tag: None,
                })
            }
        };
        match &result {
            Ok(result) if result.completed => self.status = PortalStatus::Done,
            Ok(_) => self.status = PortalStatus::Ready,
            Err(_) => self.status = PortalStatus::Failed,
        }
        if let Ok(result) = &mut result
            && result.completed
            && !self.command_tag.is_empty()
            && self.command_tag != "SELECT"
        {
            result.command_tag = Some(self.command_tag.clone());
        }
        result
    }

    pub fn fetch_direction(
        &mut self,
        direction: PortalFetchDirection,
        move_only: bool,
    ) -> Result<PortalRunResult, ExecError> {
        if matches!(
            direction,
            PortalFetchDirection::Backward(_)
                | PortalFetchDirection::Prior
                | PortalFetchDirection::Absolute(_)
                | PortalFetchDirection::Relative(_)
                | PortalFetchDirection::First
                | PortalFetchDirection::Last
        ) && self.options.no_scroll
        {
            return Err(ExecError::Parse(ParseError::DetailedError {
                message: "cursor can only scan forward".into(),
                detail: None,
                hint: Some("Declare it with SCROLL option to enable backward scan.".into()),
                sqlstate: "55000",
            }));
        }
        if !self.options.no_scroll {
            self.materialize_all()?;
        }
        let mut result = match direction {
            PortalFetchDirection::Forward(limit) => self.fetch_forward(limit)?,
            PortalFetchDirection::Next => self.fetch_forward(PortalFetchLimit::Count(1))?,
            PortalFetchDirection::Backward(limit) => self.fetch_materialized_backward(limit)?,
            PortalFetchDirection::Prior => {
                self.fetch_materialized_backward(PortalFetchLimit::Count(1))?
            }
            PortalFetchDirection::First => self.fetch_materialized_absolute(1)?,
            PortalFetchDirection::Last => self.fetch_materialized_absolute(-1)?,
            PortalFetchDirection::Absolute(offset) => self.fetch_materialized_absolute(offset)?,
            PortalFetchDirection::Relative(offset) => self.fetch_materialized_relative(offset)?,
        };
        if move_only {
            result.rows.clear();
        }
        Ok(result)
    }

    fn fetch_materialized_backward(
        &mut self,
        limit: PortalFetchLimit,
    ) -> Result<PortalRunResult, ExecError> {
        let PortalExecution::Materialized {
            columns,
            column_names,
            rows,
            pos,
        } = &mut self.execution
        else {
            return Err(ExecError::DetailedError {
                message: "portal is not materialized".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            });
        };
        let count = match limit {
            PortalFetchLimit::All => *pos,
            PortalFetchLimit::Count(count) => count.min(*pos),
        };
        let end = *pos;
        let start = end.saturating_sub(count);
        let out = rows[start..end].iter().rev().cloned().collect::<Vec<_>>();
        *pos = start;
        Ok(PortalRunResult {
            columns: columns.clone(),
            column_names: column_names.clone(),
            processed: out.len(),
            rows: out,
            completed: *pos == 0,
            command_tag: None,
        })
    }

    fn fetch_materialized_absolute(&mut self, offset: i64) -> Result<PortalRunResult, ExecError> {
        let len = self.materialized_len()?;
        let target = if offset < 0 {
            len.saturating_sub(offset.unsigned_abs() as usize)
                .saturating_add(1)
        } else {
            offset as usize
        };
        self.set_materialized_pos(target.saturating_sub(1))?;
        self.fetch_forward(PortalFetchLimit::Count(1))
    }

    fn fetch_materialized_relative(&mut self, offset: i64) -> Result<PortalRunResult, ExecError> {
        let current = self.materialized_pos()? as i64;
        let target = (current + offset).max(0) as usize;
        self.set_materialized_pos(target.saturating_sub(1))?;
        self.fetch_forward(PortalFetchLimit::Count(1))
    }

    fn materialized_len(&self) -> Result<usize, ExecError> {
        match &self.execution {
            PortalExecution::Materialized { rows, .. } => Ok(rows.len()),
            _ => Err(ExecError::DetailedError {
                message: "portal is not materialized".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            }),
        }
    }

    fn materialized_pos(&self) -> Result<usize, ExecError> {
        match &self.execution {
            PortalExecution::Materialized { pos, .. } => Ok(*pos),
            _ => Err(ExecError::DetailedError {
                message: "portal is not materialized".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            }),
        }
    }

    fn set_materialized_pos(&mut self, new_pos: usize) -> Result<(), ExecError> {
        match &mut self.execution {
            PortalExecution::Materialized { rows, pos, .. } => {
                *pos = new_pos.min(rows.len());
                Ok(())
            }
            _ => Err(ExecError::DetailedError {
                message: "portal is not materialized".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            }),
        }
    }

    pub fn cursor_view_row(&self) -> Option<CursorViewRow> {
        self.options.visible.then(|| CursorViewRow {
            name: self.name.clone(),
            statement: self.source_text.clone(),
            is_holdable: self.options.holdable,
            is_binary: self.options.binary,
            is_scrollable: self.options.scroll,
        })
    }
}

fn fetch_streaming_forward(
    guard: &mut SelectGuard,
    limit: PortalFetchLimit,
) -> Result<PortalRunResult, ExecError> {
    let mut rows = Vec::new();
    let max_rows = match limit {
        PortalFetchLimit::All => usize::MAX,
        PortalFetchLimit::Count(count) => count,
    };
    let mut completed = false;
    while rows.len() < max_rows {
        match exec_next(&mut guard.state, &mut guard.ctx)? {
            Some(slot) => {
                let mut values = slot.values()?.to_vec();
                Value::materialize_all(&mut values);
                rows.push(values);
            }
            None => {
                completed = true;
                break;
            }
        }
    }
    if matches!(limit, PortalFetchLimit::All) {
        completed = true;
    }
    Ok(PortalRunResult {
        columns: guard.columns.clone(),
        column_names: guard.column_names.clone(),
        processed: rows.len(),
        rows,
        completed,
        command_tag: None,
    })
}

fn fetch_materialized_forward(
    columns: &[QueryColumn],
    column_names: &[String],
    rows: &[Vec<Value>],
    pos: &mut usize,
    limit: PortalFetchLimit,
) -> PortalRunResult {
    let remaining = rows.len().saturating_sub(*pos);
    let count = match limit {
        PortalFetchLimit::All => remaining,
        PortalFetchLimit::Count(count) => count.min(remaining),
    };
    let start = *pos;
    let end = start + count;
    let out = rows[start..end].to_vec();
    *pos = end;
    PortalRunResult {
        columns: columns.to_vec(),
        column_names: column_names.to_vec(),
        processed: out.len(),
        rows: out,
        completed: *pos >= rows.len(),
        command_tag: None,
    }
}

#[derive(Default)]
pub struct PortalManager {
    portals: HashMap<String, Portal>,
}

impl PortalManager {
    pub fn contains(&self, name: &str) -> bool {
        self.portals.contains_key(name)
    }

    pub fn insert(
        &mut self,
        portal: Portal,
        allow_replace: bool,
        silent_replace: bool,
    ) -> Result<(), ExecError> {
        if self.portals.contains_key(&portal.name) {
            if allow_replace {
                self.portals.remove(&portal.name);
            } else {
                return Err(ExecError::DetailedError {
                    message: format!("portal \"{}\" already exists", portal.name),
                    detail: None,
                    hint: None,
                    sqlstate: "42P03",
                });
            }
        }
        if !silent_replace || !portal.name.is_empty() {
            self.portals.insert(portal.name.clone(), portal);
        } else {
            self.portals.insert(String::new(), portal);
        }
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&Portal> {
        self.portals.get(name)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut Portal> {
        self.portals.get_mut(name)
    }

    pub fn take(&mut self, name: &str) -> Option<Portal> {
        self.portals.remove(name)
    }

    pub fn put(&mut self, portal: Portal) {
        self.portals.insert(portal.name.clone(), portal);
    }

    pub fn remove(&mut self, name: &str) {
        self.portals.remove(name);
    }

    pub fn clear(&mut self) {
        self.portals.clear();
    }

    pub fn close_all_visible(&mut self) {
        self.portals.retain(|_, portal| !portal.options.visible);
    }

    pub fn drop_transaction_portals(&mut self, commit: bool) {
        self.portals.retain(|_, portal| {
            if portal.options.holdable && commit {
                true
            } else {
                !portal.created_in_transaction
            }
        });
        if commit {
            for portal in self.portals.values_mut() {
                portal.created_in_transaction = false;
            }
        }
    }

    pub fn cursor_view_rows(&self) -> Vec<CursorViewRow> {
        let mut rows = self
            .portals
            .values()
            .filter_map(Portal::cursor_view_row)
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| left.name.cmp(&right.name));
        rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::parser::{SqlType, SqlTypeKind};

    fn int_column() -> QueryColumn {
        QueryColumn {
            name: "id".into(),
            sql_type: SqlType::new(SqlTypeKind::Int4),
            wire_type_oid: None,
        }
    }

    fn materialized_portal(name: &str, visible: bool) -> Portal {
        Portal::materialized_select(
            name.into(),
            "select id from t".into(),
            None,
            Vec::new(),
            CursorOptions {
                visible,
                ..CursorOptions::default()
            },
            false,
            vec![int_column()],
            vec!["id".into()],
            vec![
                vec![Value::Int32(1)],
                vec![Value::Int32(2)],
                vec![Value::Int32(3)],
            ],
        )
    }

    #[test]
    fn unnamed_portal_replacement_drops_old_portal() {
        let mut manager = PortalManager::default();
        manager
            .insert(materialized_portal("", false), true, true)
            .unwrap();
        let mut replacement = materialized_portal("", false);
        replacement.source_text = "select 2".into();
        manager.insert(replacement, true, true).unwrap();

        assert_eq!(manager.portals.len(), 1);
        assert_eq!(manager.get("").unwrap().source_text, "select 2");
    }

    #[test]
    fn duplicate_named_portal_errors() {
        let mut manager = PortalManager::default();
        manager
            .insert(materialized_portal("c", true), false, false)
            .unwrap();
        let err = manager
            .insert(materialized_portal("c", true), false, false)
            .unwrap_err();

        assert!(matches!(
            err,
            ExecError::DetailedError {
                sqlstate: "42P03",
                ..
            }
        ));
    }

    #[test]
    fn close_all_visible_keeps_protocol_portals() {
        let mut manager = PortalManager::default();
        manager
            .insert(materialized_portal("sql_cursor", true), false, false)
            .unwrap();
        manager
            .insert(materialized_portal("protocol_portal", false), false, false)
            .unwrap();

        manager.close_all_visible();

        assert!(manager.get("sql_cursor").is_none());
        assert!(manager.get("protocol_portal").is_some());
    }

    #[test]
    fn materialized_fetch_updates_status_and_position() {
        let mut portal = materialized_portal("c", true);

        let first = portal.fetch_forward(PortalFetchLimit::Count(1)).unwrap();
        assert_eq!(first.rows, vec![vec![Value::Int32(1)]]);
        assert_eq!(portal.status, PortalStatus::Ready);

        let rest = portal.fetch_forward(PortalFetchLimit::All).unwrap();
        assert_eq!(
            rest.rows,
            vec![vec![Value::Int32(2)], vec![Value::Int32(3)]]
        );
        assert_eq!(portal.status, PortalStatus::Done);
    }
}
