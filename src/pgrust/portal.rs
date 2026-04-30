use std::collections::HashMap;

use crate::backend::executor::{ExecError, QueryColumn, Value, exec_next};
use crate::backend::parser::ParseError;
use crate::include::access::htup::ItemPointerData;
use crate::include::nodes::execnodes::SystemVarBinding;
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
    pub current_row: Option<PositionedCursorRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorViewRow {
    pub name: String,
    pub statement: String,
    pub is_holdable: bool,
    pub is_binary: bool,
    pub is_scrollable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PositionedCursorRow {
    pub table_oid: u32,
    pub tid: ItemPointerData,
}

pub enum PortalExecution {
    Streaming(SelectGuard),
    Materialized {
        columns: Vec<QueryColumn>,
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
        row_positions: Vec<Option<PositionedCursorRow>>,
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
    pub created_savepoint_depth: usize,
    pub execution: PortalExecution,
    current_row: Option<PositionedCursorRow>,
}

impl Portal {
    pub fn streaming_select(
        name: String,
        source_text: String,
        prep_stmt_name: Option<String>,
        result_formats: Vec<i16>,
        options: CursorOptions,
        created_in_transaction: bool,
        created_savepoint_depth: usize,
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
            created_savepoint_depth,
            execution: PortalExecution::Streaming(guard),
            current_row: None,
        }
    }

    pub fn materialized_select(
        name: String,
        source_text: String,
        prep_stmt_name: Option<String>,
        result_formats: Vec<i16>,
        options: CursorOptions,
        created_in_transaction: bool,
        created_savepoint_depth: usize,
        columns: Vec<QueryColumn>,
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
    ) -> Self {
        let row_positions = vec![None; rows.len()];
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
            created_savepoint_depth,
            execution: PortalExecution::Materialized {
                columns,
                column_names,
                rows,
                row_positions,
                pos: 0,
            },
            current_row: None,
        }
    }

    pub fn pending_sql(
        name: String,
        source_text: String,
        prep_stmt_name: Option<String>,
        result_formats: Vec<i16>,
        options: CursorOptions,
        created_in_transaction: bool,
        created_savepoint_depth: usize,
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
            created_savepoint_depth,
            execution: PortalExecution::PendingSql {
                sql: source_text,
                columns,
                column_names,
            },
            current_row: None,
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
        let mut row_positions = Vec::new();
        while let Some(slot) = exec_next(&mut guard.state, &mut guard.ctx)? {
            let tid = slot.tid();
            let table_oid = slot.table_oid;
            let mut values = slot.values()?.to_vec();
            Value::materialize_all(&mut values);
            rows.push(values);
            row_positions.push(positioned_row_from_metadata(
                tid,
                table_oid,
                guard.state.current_system_bindings(),
            ));
        }
        self.execution = PortalExecution::Materialized {
            columns,
            column_names,
            rows,
            row_positions,
            // PostgreSQL cursor positions are 0 before the first row,
            // 1..=N on a row, and N+1 after the last row.
            pos: 0,
        };
        self.current_row = None;
        self.status = PortalStatus::Ready;
        Ok(())
    }

    pub fn fetch_forward(&mut self, limit: PortalFetchLimit) -> Result<PortalRunResult, ExecError> {
        if self.status == PortalStatus::Failed {
            return Err(portal_cannot_run_error(&self.name));
        }
        self.status = PortalStatus::Active;
        let mut result = match &mut self.execution {
            PortalExecution::Streaming(guard) => fetch_streaming_forward(guard, limit),
            PortalExecution::Materialized {
                columns,
                column_names,
                rows,
                row_positions,
                pos,
            } => Ok(fetch_materialized_forward(
                columns,
                column_names,
                rows,
                row_positions,
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
                    current_row: None,
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
        if let Ok(result) = &result {
            self.current_row = result.current_row;
        } else {
            self.current_row = None;
        }
        result
    }

    pub fn fetch_direction(
        &mut self,
        direction: PortalFetchDirection,
        move_only: bool,
    ) -> Result<PortalRunResult, ExecError> {
        if self.status == PortalStatus::Failed {
            return Err(portal_cannot_run_error(&self.name));
        }
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
            if let Err(err) = self.materialize_all() {
                self.status = PortalStatus::Failed;
                self.current_row = None;
                return Err(err);
            }
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
        self.current_row = result.current_row;
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
            row_positions,
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
        let len = rows.len();
        let start_row = if *pos > len {
            len
        } else {
            pos.saturating_sub(1)
        };
        let count = match limit {
            PortalFetchLimit::All => start_row,
            PortalFetchLimit::Count(count) => count.min(start_row),
        };
        let first_returned = start_row.saturating_sub(count).saturating_add(1);
        let start_idx = first_returned.saturating_sub(1);
        let end_idx = start_row;
        let out = rows[start_idx..end_idx]
            .iter()
            .rev()
            .cloned()
            .collect::<Vec<_>>();
        let current_row = row_positions[start_idx..end_idx]
            .iter()
            .rev()
            .flatten()
            .next()
            .copied();
        *pos = if count == 0 {
            *pos
        } else if count == start_row {
            0
        } else {
            first_returned
        };
        Ok(PortalRunResult {
            columns: columns.clone(),
            column_names: column_names.clone(),
            processed: out.len(),
            rows: out,
            completed: *pos == 0,
            command_tag: None,
            current_row,
        })
    }

    fn fetch_materialized_absolute(&mut self, offset: i64) -> Result<PortalRunResult, ExecError> {
        let len = self.materialized_len()?;
        let target = if offset == 0 {
            0
        } else if offset < 0 {
            len.saturating_add(1)
                .saturating_sub(offset.unsigned_abs() as usize)
        } else {
            offset as usize
        };
        self.fetch_materialized_row_at(target)
    }

    fn fetch_materialized_relative(&mut self, offset: i64) -> Result<PortalRunResult, ExecError> {
        let current = self.materialized_pos()? as i64;
        let target = (current + offset).max(0) as usize;
        self.fetch_materialized_row_at(target)
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
                *pos = new_pos.min(rows.len().saturating_add(1));
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

    fn fetch_materialized_row_at(&mut self, target: usize) -> Result<PortalRunResult, ExecError> {
        let len = self.materialized_len()?;
        if target == 0 {
            self.set_materialized_pos(0)?;
            return self.empty_materialized_result();
        }
        if target > len {
            self.set_materialized_pos(len.saturating_add(1))?;
            return self.empty_materialized_result();
        }
        self.set_materialized_pos(target.saturating_sub(1))?;
        self.fetch_forward(PortalFetchLimit::Count(1))
    }

    fn empty_materialized_result(&self) -> Result<PortalRunResult, ExecError> {
        let PortalExecution::Materialized {
            columns,
            column_names,
            ..
        } = &self.execution
        else {
            return Err(ExecError::DetailedError {
                message: "portal is not materialized".into(),
                detail: None,
                hint: None,
                sqlstate: "XX000",
            });
        };
        Ok(PortalRunResult {
            columns: columns.clone(),
            column_names: column_names.clone(),
            rows: Vec::new(),
            processed: 0,
            completed: true,
            command_tag: None,
            current_row: None,
        })
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

    pub fn current_positioned_row(&self) -> Option<PositionedCursorRow> {
        self.current_row
    }

    pub fn replace_current_positioned_row(
        &mut self,
        old: PositionedCursorRow,
        new: PositionedCursorRow,
    ) {
        if self.current_row == Some(old) {
            self.current_row = Some(new);
        }
        if let PortalExecution::Materialized {
            row_positions, pos, ..
        } = &mut self.execution
            && *pos > 0
        {
            let index = *pos - 1;
            if row_positions.get(index).copied().flatten() == Some(old) {
                row_positions[index] = Some(new);
            }
        }
    }
}

fn fetch_streaming_forward(
    guard: &mut SelectGuard,
    limit: PortalFetchLimit,
) -> Result<PortalRunResult, ExecError> {
    let mut rows = Vec::new();
    let mut current_row = None;
    let max_rows = match limit {
        PortalFetchLimit::All => usize::MAX,
        PortalFetchLimit::Count(count) => count,
    };
    let mut completed = false;
    while rows.len() < max_rows {
        match exec_next(&mut guard.state, &mut guard.ctx)? {
            Some(slot) => {
                let tid = slot.tid();
                let table_oid = slot.table_oid;
                let mut values = slot.values()?.to_vec();
                Value::materialize_all(&mut values);
                rows.push(values);
                current_row = positioned_row_from_metadata(
                    tid,
                    table_oid,
                    guard.state.current_system_bindings(),
                );
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
        current_row,
    })
}

fn fetch_materialized_forward(
    columns: &[QueryColumn],
    column_names: &[String],
    rows: &[Vec<Value>],
    row_positions: &[Option<PositionedCursorRow>],
    pos: &mut usize,
    limit: PortalFetchLimit,
) -> PortalRunResult {
    let start = (*pos).min(rows.len());
    let remaining = rows.len().saturating_sub(start);
    let count = match limit {
        PortalFetchLimit::All => remaining,
        PortalFetchLimit::Count(count) => count.min(remaining),
    };
    let end = start + count;
    let out = rows[start..end].to_vec();
    let current_row = row_positions[start..end]
        .iter()
        .rev()
        .flatten()
        .next()
        .copied();
    *pos = match limit {
        PortalFetchLimit::All => rows.len().saturating_add(1),
        PortalFetchLimit::Count(requested) if requested > remaining => rows.len().saturating_add(1),
        PortalFetchLimit::Count(_) if count == 0 && *pos == rows.len() => {
            rows.len().saturating_add(1)
        }
        PortalFetchLimit::Count(_) => end,
    };
    PortalRunResult {
        columns: columns.to_vec(),
        column_names: column_names.to_vec(),
        processed: out.len(),
        rows: out,
        completed: *pos >= rows.len(),
        command_tag: None,
        current_row,
    }
}

fn positioned_row_from_metadata(
    tid: Option<ItemPointerData>,
    table_oid: Option<u32>,
    bindings: &[SystemVarBinding],
) -> Option<PositionedCursorRow> {
    if let (Some(tid), Some(table_oid)) = (tid, table_oid) {
        return Some(PositionedCursorRow { table_oid, tid });
    }
    let mut positioned = bindings.iter().filter_map(|binding| {
        binding.tid.map(|tid| PositionedCursorRow {
            table_oid: binding.table_oid,
            tid,
        })
    });
    let first = positioned.next()?;
    positioned.next().is_none().then_some(first)
}

fn portal_cannot_run_error(name: &str) -> ExecError {
    ExecError::Parse(ParseError::DetailedError {
        message: format!("portal \"{name}\" cannot be run"),
        detail: None,
        hint: None,
        sqlstate: "55000",
    })
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

    pub fn drop_portals_created_at_or_after_savepoint(&mut self, depth: usize) {
        self.portals
            .retain(|_, portal| portal.created_savepoint_depth < depth);
    }

    pub fn release_savepoint_depth(&mut self, depth: usize) {
        for portal in self.portals.values_mut() {
            if portal.created_savepoint_depth >= depth {
                portal.created_savepoint_depth = depth.saturating_sub(1);
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
            0,
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

    #[test]
    fn materialized_scroll_distinguishes_last_row_from_after_end() {
        let mut portal = materialized_portal("c", true);

        let last = portal
            .fetch_direction(PortalFetchDirection::Last, false)
            .unwrap();
        assert_eq!(last.rows, vec![vec![Value::Int32(3)]]);

        let prev = portal
            .fetch_direction(
                PortalFetchDirection::Backward(PortalFetchLimit::Count(1)),
                false,
            )
            .unwrap();
        assert_eq!(prev.rows, vec![vec![Value::Int32(2)]]);

        let next = portal
            .fetch_direction(PortalFetchDirection::Next, false)
            .unwrap();
        assert_eq!(next.rows, vec![vec![Value::Int32(3)]]);

        let past_end = portal
            .fetch_direction(PortalFetchDirection::Next, false)
            .unwrap();
        assert!(past_end.rows.is_empty());

        let from_after_end = portal
            .fetch_direction(
                PortalFetchDirection::Backward(PortalFetchLimit::Count(1)),
                false,
            )
            .unwrap();
        assert_eq!(from_after_end.rows, vec![vec![Value::Int32(3)]]);

        let next_after_refetching_last = portal
            .fetch_direction(PortalFetchDirection::Next, false)
            .unwrap();
        assert!(next_after_refetching_last.rows.is_empty());
    }
}
