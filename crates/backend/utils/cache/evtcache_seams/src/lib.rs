//! Seam declarations for the `backend-utils-cache-evtcache` unit
//! (`utils/cache/evtcache.c`).
//!
//! `commands/event_trigger.c` calls `EventCacheLookup`, but
//! `BuildEventTriggerCache` scans `pg_event_trigger` (a catalog access that
//! reaches back into command/access machinery), so the consumer crosses to the
//! cache through this seam. The owning unit installs it from its
//! `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `List *EventCacheLookup(EventTriggerEvent event)` (evtcache.c): search
    /// the event cache by trigger event, rebuilding it first if stale.
    ///
    /// The C return is a `List *` of `EventTriggerCacheItem *` owned by the
    /// cache's memory context, which the caller must copy before any catalog
    /// operation; the owned model returns the copy directly — each item (and
    /// its `tagset` Bitmapset) is allocated in the caller's `mcx`. `Err`
    /// carries the rebuild's catalog-scan / decode / OOM error surface.
    pub fn event_cache_lookup<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        event: types_evtcache::EventTriggerEvent,
    ) -> types_error::PgResult<
        mcx::PgVec<'mcx, types_evtcache::EventTriggerCacheItem<'mcx>>,
    >
);
