use std::collections::BTreeMap;

use crate::ClientId;
use crate::backend::access::transam::xact::{CommandId, TransactionId};
use crate::backend::catalog::CatalogError;
use crate::backend::executor::SessionReplicationRole;
use crate::backend::utils::cache::syscache::BackendCacheContext;
use crate::backend::utils::time::snapmgr::get_catalog_snapshot;
use crate::include::catalog::{PgEventTriggerRow, sort_pg_event_trigger_rows};
use crate::pgrust::database::Database;

const EVENT_TRIGGER_DISABLED: char = 'D';
const EVENT_TRIGGER_ENABLED_ORIGIN: char = 'O';
const EVENT_TRIGGER_ENABLED_REPLICA: char = 'R';
const EVENT_TRIGGER_ENABLED_ALWAYS: char = 'A';

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum EventTriggerEvent {
    DdlCommandStart,
    DdlCommandEnd,
    SqlDrop,
    TableRewrite,
    Login,
}

impl EventTriggerEvent {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            name if name.eq_ignore_ascii_case("ddl_command_start") => Some(Self::DdlCommandStart),
            name if name.eq_ignore_ascii_case("ddl_command_end") => Some(Self::DdlCommandEnd),
            name if name.eq_ignore_ascii_case("sql_drop") => Some(Self::SqlDrop),
            name if name.eq_ignore_ascii_case("table_rewrite") => Some(Self::TableRewrite),
            name if name.eq_ignore_ascii_case("login") => Some(Self::Login),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct EventTriggerCacheItem {
    row: PgEventTriggerRow,
    tags: Option<Vec<String>>,
}

impl EventTriggerCacheItem {
    fn new(row: PgEventTriggerRow) -> Self {
        let tags = row.evttags.as_ref().map(|tags| {
            tags.iter()
                .map(|tag| tag.to_ascii_uppercase())
                .collect::<Vec<_>>()
        });
        Self { row, tags }
    }

    fn matches_tag(&self, tag: &str) -> bool {
        self.tags
            .as_ref()
            .is_none_or(|tags| tags.iter().any(|candidate| candidate == tag))
    }
}

#[derive(Debug, Default, Clone)]
pub struct EventTriggerCache {
    by_event: BTreeMap<EventTriggerEvent, Vec<EventTriggerCacheItem>>,
}

impl EventTriggerCache {
    pub fn from_rows(mut rows: Vec<PgEventTriggerRow>) -> Self {
        sort_pg_event_trigger_rows(&mut rows);
        let mut by_event = BTreeMap::new();
        for row in rows {
            if row.evtenabled == EVENT_TRIGGER_DISABLED {
                continue;
            }
            let Some(event) = EventTriggerEvent::from_name(&row.evtevent) else {
                continue;
            };
            by_event
                .entry(event)
                .or_insert_with(Vec::new)
                .push(EventTriggerCacheItem::new(row));
        }
        Self { by_event }
    }

    pub fn may_fire(&self, event: &str, tag: &str, role: SessionReplicationRole) -> bool {
        !self.matching_rows(event, tag, role).is_empty()
    }

    pub fn matching_rows(
        &self,
        event: &str,
        tag: &str,
        role: SessionReplicationRole,
    ) -> Vec<PgEventTriggerRow> {
        let Some(event) = EventTriggerEvent::from_name(event) else {
            return Vec::new();
        };
        let tag = tag.to_ascii_uppercase();
        self.by_event
            .get(&event)
            .into_iter()
            .flat_map(|rows| rows.iter())
            .filter(|item| {
                event_trigger_enabled_for_session(item.row.evtenabled, role)
                    && item.matches_tag(&tag)
            })
            .map(|item| item.row.clone())
            .collect()
    }
}

pub fn event_trigger_cache(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Result<EventTriggerCache, CatalogError> {
    if txn_ctx.is_none() {
        db.accept_invalidation_messages(client_id);
    }

    let cache_ctx = BackendCacheContext::from(txn_ctx);
    if let Some(cache) = db
        .backend_cache_states
        .read()
        .get(&client_id)
        .filter(|state| state.event_trigger_cache_ctx == Some(cache_ctx))
        .and_then(|state| state.event_trigger_cache.clone())
    {
        return Ok(cache);
    }

    let snapshot = get_catalog_snapshot(db, client_id, txn_ctx, None)
        .ok_or_else(|| CatalogError::Corrupt("missing catalog snapshot"))?;
    let rows = {
        let txns = db.txns.read();
        db.catalog
            .read()
            .event_trigger_rows_with_snapshot(&db.pool, &txns, &snapshot, client_id)?
    };
    let cache = EventTriggerCache::from_rows(rows);

    let mut states = db.backend_cache_states.write();
    let state = states.entry(client_id).or_default();
    state.event_trigger_cache_ctx = Some(cache_ctx);
    state.event_trigger_cache = Some(cache.clone());
    Ok(cache)
}

fn event_trigger_enabled_for_session(evtenabled: char, role: SessionReplicationRole) -> bool {
    match evtenabled {
        EVENT_TRIGGER_DISABLED => false,
        EVENT_TRIGGER_ENABLED_REPLICA => role == SessionReplicationRole::Replica,
        EVENT_TRIGGER_ENABLED_ORIGIN => role != SessionReplicationRole::Replica,
        EVENT_TRIGGER_ENABLED_ALWAYS => true,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        oid: u32,
        evtevent: &str,
        evtenabled: char,
        evttags: Option<Vec<&str>>,
    ) -> PgEventTriggerRow {
        PgEventTriggerRow {
            oid,
            evtname: format!("evt_{oid}"),
            evtevent: evtevent.to_string(),
            evtowner: 10,
            evtfoid: oid + 100,
            evtenabled,
            evttags: evttags.map(|tags| tags.into_iter().map(str::to_string).collect()),
        }
    }

    #[test]
    fn matching_rows_respect_event_tag_and_enable_mode() {
        let cache = EventTriggerCache::from_rows(vec![
            row(
                1,
                "ddl_command_start",
                EVENT_TRIGGER_ENABLED_ORIGIN,
                Some(vec!["CREATE TABLE"]),
            ),
            row(
                2,
                "ddl_command_start",
                EVENT_TRIGGER_ENABLED_REPLICA,
                Some(vec!["CREATE INDEX"]),
            ),
            row(3, "ddl_command_start", EVENT_TRIGGER_ENABLED_ALWAYS, None),
            row(
                4,
                "ddl_command_end",
                EVENT_TRIGGER_ENABLED_ALWAYS,
                Some(vec!["CREATE TABLE"]),
            ),
            row(5, "ddl_command_start", EVENT_TRIGGER_DISABLED, None),
        ]);

        let origin_rows = cache
            .matching_rows(
                "DDL_COMMAND_START",
                "create table",
                SessionReplicationRole::Origin,
            )
            .into_iter()
            .map(|row| row.oid)
            .collect::<Vec<_>>();
        assert_eq!(origin_rows, vec![1, 3]);

        let replica_rows = cache
            .matching_rows(
                "ddl_command_start",
                "CREATE INDEX",
                SessionReplicationRole::Replica,
            )
            .into_iter()
            .map(|row| row.oid)
            .collect::<Vec<_>>();
        assert_eq!(replica_rows, vec![2, 3]);

        assert!(!cache.may_fire(
            "ddl_command_end",
            "CREATE INDEX",
            SessionReplicationRole::Origin
        ));
    }
}
