use std::collections::HashMap;

use crate::backend::access::transam::xact::{
    CommandId, INVALID_TRANSACTION_ID, Snapshot, TransactionId,
};
use crate::backend::catalog::loader::{
    load_visible_am_rows, load_visible_attrdef_rows, load_visible_attribute_rows,
    load_visible_class_rows, load_visible_index_rows, load_visible_namespace_rows,
    load_visible_type_rows,
};
use crate::backend::utils::cache::relcache::RelCacheEntry;
use crate::include::catalog::{
    PgAmRow, PgAttrdefRow, PgAttributeRow, PgClassRow, PgIndexRow, PgNamespaceRow, PgTypeRow,
};
use crate::pgrust::database::Database;
use crate::ClientId;

#[derive(Debug, Default, Clone)]
pub struct SessionCatalogState {
    pub catalog_snapshot: Option<Snapshot>,
    pub namespace_rows: Option<Vec<PgNamespaceRow>>,
    pub class_rows: Option<Vec<PgClassRow>>,
    pub attribute_rows: Option<Vec<PgAttributeRow>>,
    pub attrdef_rows: Option<Vec<PgAttrdefRow>>,
    pub type_rows: Option<Vec<PgTypeRow>>,
    pub index_rows: Option<Vec<PgIndexRow>>,
    pub am_rows: Option<Vec<PgAmRow>>,
    pub relation_entries_by_oid: HashMap<u32, RelCacheEntry>,
}

pub fn invalidate_session_catalog_state(db: &Database, client_id: ClientId) {
    db.session_catalog_states.write().remove(&client_id);
}

pub fn catalog_snapshot_for_lookup(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Option<Snapshot> {
    if let Some(snapshot) = db
        .session_catalog_states
        .read()
        .get(&client_id)
        .and_then(|state| state.catalog_snapshot.clone())
    {
        return Some(snapshot);
    }

    let snapshot = {
        let txns = db.txns.read();
        match txn_ctx {
            Some((xid, cid)) => txns.snapshot_for_command(xid, cid).ok(),
            None => txns.snapshot(INVALID_TRANSACTION_ID).ok(),
        }
    }?;

    db.session_catalog_states
        .write()
        .entry(client_id)
        .or_default()
        .catalog_snapshot = Some(snapshot.clone());
    Some(snapshot)
}

pub fn ensure_namespace_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgNamespaceRow> {
    if let Some(rows) = db
        .session_catalog_states
        .read()
        .get(&client_id)
        .and_then(|state| state.namespace_rows.clone())
    {
        return rows;
    }
    let Some(snapshot) = catalog_snapshot_for_lookup(db, client_id, txn_ctx) else {
        return Vec::new();
    };
    let rows = {
        let catalog = db.catalog.read();
        let txns = db.txns.read();
        load_visible_namespace_rows(catalog.base_dir(), &db.pool, &txns, &snapshot, client_id)
            .unwrap_or_default()
    };
    db.session_catalog_states
        .write()
        .entry(client_id)
        .or_default()
        .namespace_rows = Some(rows.clone());
    rows
}

pub fn ensure_class_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgClassRow> {
    if let Some(rows) = db
        .session_catalog_states
        .read()
        .get(&client_id)
        .and_then(|state| state.class_rows.clone())
    {
        return rows;
    }
    let Some(snapshot) = catalog_snapshot_for_lookup(db, client_id, txn_ctx) else {
        return Vec::new();
    };
    let rows = {
        let catalog = db.catalog.read();
        let txns = db.txns.read();
        load_visible_class_rows(catalog.base_dir(), &db.pool, &txns, &snapshot, client_id)
            .unwrap_or_default()
    };
    db.session_catalog_states
        .write()
        .entry(client_id)
        .or_default()
        .class_rows = Some(rows.clone());
    rows
}

pub fn ensure_attribute_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgAttributeRow> {
    if let Some(rows) = db
        .session_catalog_states
        .read()
        .get(&client_id)
        .and_then(|state| state.attribute_rows.clone())
    {
        return rows;
    }
    let Some(snapshot) = catalog_snapshot_for_lookup(db, client_id, txn_ctx) else {
        return Vec::new();
    };
    let rows = {
        let catalog = db.catalog.read();
        let txns = db.txns.read();
        load_visible_attribute_rows(catalog.base_dir(), &db.pool, &txns, &snapshot, client_id)
            .unwrap_or_default()
    };
    db.session_catalog_states
        .write()
        .entry(client_id)
        .or_default()
        .attribute_rows = Some(rows.clone());
    rows
}

pub fn ensure_attrdef_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgAttrdefRow> {
    if let Some(rows) = db
        .session_catalog_states
        .read()
        .get(&client_id)
        .and_then(|state| state.attrdef_rows.clone())
    {
        return rows;
    }
    let Some(snapshot) = catalog_snapshot_for_lookup(db, client_id, txn_ctx) else {
        return Vec::new();
    };
    let rows = {
        let catalog = db.catalog.read();
        let txns = db.txns.read();
        load_visible_attrdef_rows(catalog.base_dir(), &db.pool, &txns, &snapshot, client_id)
            .unwrap_or_default()
    };
    db.session_catalog_states
        .write()
        .entry(client_id)
        .or_default()
        .attrdef_rows = Some(rows.clone());
    rows
}

pub fn ensure_type_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgTypeRow> {
    if let Some(rows) = db
        .session_catalog_states
        .read()
        .get(&client_id)
        .and_then(|state| state.type_rows.clone())
    {
        return rows;
    }
    let Some(snapshot) = catalog_snapshot_for_lookup(db, client_id, txn_ctx) else {
        return Vec::new();
    };
    let rows = {
        let catalog = db.catalog.read();
        let txns = db.txns.read();
        load_visible_type_rows(catalog.base_dir(), &db.pool, &txns, &snapshot, client_id)
            .unwrap_or_default()
    };
    db.session_catalog_states
        .write()
        .entry(client_id)
        .or_default()
        .type_rows = Some(rows.clone());
    rows
}

pub fn ensure_index_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgIndexRow> {
    if let Some(rows) = db
        .session_catalog_states
        .read()
        .get(&client_id)
        .and_then(|state| state.index_rows.clone())
    {
        return rows;
    }
    let Some(snapshot) = catalog_snapshot_for_lookup(db, client_id, txn_ctx) else {
        return Vec::new();
    };
    let rows = {
        let catalog = db.catalog.read();
        let txns = db.txns.read();
        load_visible_index_rows(catalog.base_dir(), &db.pool, &txns, &snapshot, client_id)
            .unwrap_or_default()
    };
    db.session_catalog_states
        .write()
        .entry(client_id)
        .or_default()
        .index_rows = Some(rows.clone());
    rows
}

pub fn ensure_am_rows(
    db: &Database,
    client_id: ClientId,
    txn_ctx: Option<(TransactionId, CommandId)>,
) -> Vec<PgAmRow> {
    if let Some(rows) = db
        .session_catalog_states
        .read()
        .get(&client_id)
        .and_then(|state| state.am_rows.clone())
    {
        return rows;
    }
    let Some(snapshot) = catalog_snapshot_for_lookup(db, client_id, txn_ctx) else {
        return Vec::new();
    };
    let rows = {
        let catalog = db.catalog.read();
        let txns = db.txns.read();
        load_visible_am_rows(catalog.base_dir(), &db.pool, &txns, &snapshot, client_id)
            .unwrap_or_default()
    };
    db.session_catalog_states
        .write()
        .entry(client_id)
        .or_default()
        .am_rows = Some(rows.clone());
    rows
}
