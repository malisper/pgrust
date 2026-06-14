//! Public types of the event-trigger cache (`utils/evtcache.h`).
//!
//! `EventTriggerEvent` and `EventTriggerCacheItem` are the cache's public ABI,
//! consumed by `commands/event_trigger.c`. They live here (a leaf types crate)
//! so the owning `backend-utils-cache-evtcache` crate and its seam crate can
//! both name them without a dependency cycle.

use mcx::{Mcx, PgBox};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::Bitmapset;

/// `EventTriggerEvent` (evtcache.h) — which event a trigger fires on.
///
/// A real C enum; the discriminants match the header's declaration order
/// (`EVT_DDLCommandStart = 0`, ...), which is the value stored nowhere on disk
/// (the catalog stores the event *name* text; this enum is the decoded form).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum EventTriggerEvent {
    /// `EVT_DDLCommandStart`.
    DdlCommandStart = 0,
    /// `EVT_DDLCommandEnd`.
    DdlCommandEnd = 1,
    /// `EVT_SQLDrop`.
    SqlDrop = 2,
    /// `EVT_TableRewrite`.
    TableRewrite = 3,
    /// `EVT_Login`.
    Login = 4,
}

/// `EventTriggerCacheItem` (evtcache.h) — one event trigger's cached data.
///
/// The `tagset` `Bitmapset` (when present) is allocated in the same `'mcx`
/// context as the rest of the item. `EventCacheLookup` hands callers a copy
/// allocated in *their* context, mirroring the C contract that the caller must
/// copy anything it wants to keep across a catalog operation.
#[derive(Debug)]
pub struct EventTriggerCacheItem<'mcx> {
    /// `fnoid` — function to be called.
    pub fnoid: Oid,
    /// `enabled` — as `SESSION_REPLICATION_ROLE_*` (a `char`).
    pub enabled: i8,
    /// `tagset` — command tags this trigger targets, or `None` if empty
    /// (the C `NULL` Bitmapset).
    pub tagset: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
}

impl<'mcx> EventTriggerCacheItem<'mcx> {
    /// Deep-copy this item into `dst` (the `EventCacheLookup` "copy into the
    /// caller's context" step). Cloning the `tagset` allocates, hence
    /// fallible.
    pub fn clone_in<'b>(&self, dst: Mcx<'b>) -> PgResult<EventTriggerCacheItem<'b>> {
        let tagset = match &self.tagset {
            Some(bms) => Some(mcx::alloc_in(dst, bms.clone_in(dst)?)?),
            None => None,
        };
        Ok(EventTriggerCacheItem {
            fnoid: self.fnoid,
            enabled: self.enabled,
            tagset,
        })
    }
}
