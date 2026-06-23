//! `backend/utils/cache/evtcache.c` — the event-trigger cache.
//!
//! `EventCacheLookup` rebuilds an in-memory cache keyed by [`EventTriggerEvent`]
//! whose entries are lists of [`EventTriggerCacheItem`], built by scanning
//! `pg_event_trigger` in name order. The cache and its entries live in a
//! dedicated memory context (`EventTriggerCacheContext` in C); rebuilding or
//! invalidating resets that context.
//!
//! Here the context is an [`McxOwned`] co-owning the `"EventTriggerCache"`
//! context and the charged [`CacheState`] (the persistent-state pattern):
//! replacing or dropping it frees every entry, the direct `MemoryContextReset`
//! analog. The one-time "have we registered the invalidation callback yet?"
//! role of the C context's non-NULL-ness is tracked separately by
//! [`CACHE_INITIALIZED`]. PostgreSQL is single-threaded per backend, so the
//! file-scope statics become `thread_local`s.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::RefCell;

use ::mcx::{Mcx, McxOwned, MemoryContext, PgHashMap, PgVec};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_evtcache::{EventTriggerCacheItem, EventTriggerEvent};
use types_tuple::heaptuple::Datum;

use ::heaptuple::heap_deform_tuple;
use genam_seams as genam_seams;
use indexam_seams as indexam_seams;
use ::table::{table_close, table_open};
use nodes_core_seams as bms_seams;
use ::cmdtag::get_command_tag_enum;
use arrayfuncs_seams as array_seams;
use inval_seams as inval_seams;
use ::cache_syscache::EVENTTRIGGEROID;

use ::types_scan::sdir::ScanDirection::ForwardScanDirection;
use ::types_storage::lock::AccessShareLock;

/// `EventTriggerRelationId` (`pg_event_trigger_d.h`).
const EventTriggerRelationId: Oid = 3466;
/// `EventTriggerNameIndexId` (`pg_event_trigger_d.h`) — the
/// `pg_event_trigger_evtname_index`.
const EventTriggerNameIndexId: Oid = 3467;

/// `Anum_pg_event_trigger_evtevent` (`pg_event_trigger.h`): the `evtevent`
/// `NameData` column (1-based attribute 3).
const Anum_pg_event_trigger_evtevent: usize = 3;
/// `Anum_pg_event_trigger_evtfoid` (attribute 5).
const Anum_pg_event_trigger_evtfoid: usize = 5;
/// `Anum_pg_event_trigger_evtenabled` (attribute 6).
const Anum_pg_event_trigger_evtenabled: usize = 6;
/// `Anum_pg_event_trigger_evttags` (attribute 7).
const Anum_pg_event_trigger_evttags: usize = 7;

/// `#define TRIGGER_DISABLED 'D'` (commands/trigger.h): a disabled trigger is
/// skipped during the build (`evtenabled` is a `char`).
const TRIGGER_DISABLED: i8 = b'D' as i8;

/// `EventTriggerCacheStateType` (evtcache.c) — the rebuild state machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EventTriggerCacheStateType {
    /// `ETCS_NEEDS_REBUILD`.
    NeedsRebuild,
    /// `ETCS_REBUILD_STARTED`.
    RebuildStarted,
    /// `ETCS_VALID`.
    Valid,
}
use EventTriggerCacheStateType::*;

/// `EventTriggerCache`'s entries: the dynahash keyed by [`EventTriggerEvent`]
/// whose values are the `List *triggerlist` of items, all charged to the
/// `McxOwned`'s context. `'mcx` is the cache context's lifetime.
struct CacheState<'mcx> {
    mcx: Mcx<'mcx>,
    cache: PgHashMap<'mcx, EventTriggerEvent, PgVec<'mcx, EventTriggerCacheItem<'mcx>>>,
}

::mcx::bind!(CacheTy => CacheState<'mcx>);

thread_local! {
    /// `static HTAB *EventTriggerCache;` + `static MemoryContext
    /// EventTriggerCacheContext;` — `None` is a NULL `HTAB*`. The `McxOwned`
    /// co-owns the charged cache and its context; replacing/dropping it frees
    /// the context (the `MemoryContextReset` analog).
    static EVENT_TRIGGER_CACHE: RefCell<Option<McxOwned<CacheTy>>> = const { RefCell::new(None) };

    /// `static EventTriggerCacheStateType EventTriggerCacheState = ETCS_NEEDS_REBUILD;`
    static EVENT_TRIGGER_CACHE_STATE: RefCell<EventTriggerCacheStateType> =
        const { RefCell::new(NeedsRebuild) };

    /// Whether the cache has ever been set up. In C this is
    /// `EventTriggerCacheContext != NULL`: the first build creates the context
    /// and registers the invalidation callback; subsequent builds reset it.
    static CACHE_INITIALIZED: RefCell<bool> = const { RefCell::new(false) };
}

/// `List *EventCacheLookup(EventTriggerEvent event)`
///
/// Search the event cache by trigger event.
///
/// The C return is a `List *` owned by the cache context, which the caller must
/// copy before any catalog operation; the owned model hands back a copy of the
/// items allocated in the caller's `mcx` (each item's `tagset` Bitmapset is
/// cloned in), which is exactly such a copy.
pub fn EventCacheLookup<'mcx>(
    mcx: Mcx<'mcx>,
    event: EventTriggerEvent,
) -> PgResult<PgVec<'mcx, EventTriggerCacheItem<'mcx>>> {
    // if (EventTriggerCacheState != ETCS_VALID)
    //     BuildEventTriggerCache();
    let valid = EVENT_TRIGGER_CACHE_STATE.with(|s| *s.borrow() == Valid);
    if !valid {
        BuildEventTriggerCache()?;
    }

    // entry = hash_search(EventTriggerCache, &event, HASH_FIND, NULL);
    // return entry != NULL ? entry->triggerlist : NIL;
    EVENT_TRIGGER_CACHE.with(|cell| {
        let slot = cell.borrow();
        let Some(owned) = slot.as_ref() else {
            return Ok(PgVec::new_in(mcx));
        };
        // Copy the triggerlist into the caller's context.
        owned.with(|state| {
            let Some(list) = state.cache.get(&event) else {
                return Ok(PgVec::new_in(mcx));
            };
            let mut out = ::mcx::vec_with_capacity_in(mcx, list.len())?;
            for item in list.iter() {
                out.push(item.clone_in(mcx)?);
            }
            Ok(out)
        })
    })
}

/// `static void BuildEventTriggerCache(void)`
///
/// Rebuild the event trigger cache.
fn BuildEventTriggerCache() -> PgResult<()> {
    // if (EventTriggerCacheContext != NULL) MemoryContextReset(...);
    // else { CreateCacheMemoryContext(); EventTriggerCacheContext = AllocSet...;
    //        CacheRegisterSyscacheCallback(...); }
    //
    // The first time through we register the invalidation callback exactly
    // once; the "reset" branch is subsumed by replacing the McxOwned below.
    let initialized = CACHE_INITIALIZED.with(|c| *c.borrow());
    if !initialized {
        inval_seams::cache_register_syscache_callback::call(
            EVENTTRIGGEROID,
            InvalidateEventCacheCallback,
            datum::datum::Datum::null(),
        )?;
        CACHE_INITIALIZED.with(|c| *c.borrow_mut() = true);
    }

    // Prevent the memory context from being nuked while we're rebuilding.
    // EventTriggerCacheState = ETCS_REBUILD_STARTED;
    EVENT_TRIGGER_CACHE_STATE.with(|s| *s.borrow_mut() = RebuildStarted);

    // Create new hash table, charged to a fresh "EventTriggerCache" context.
    //   cache = hash_create("EventTriggerCacheHash", 32, &ctl, ...);
    let cache = build_cache()?;

    // Install new cache.
    // EventTriggerCache = cache;
    //
    // Replacing the previous McxOwned drops it, freeing its context — the
    // MemoryContextReset/replace analog.
    EVENT_TRIGGER_CACHE.with(|cell| *cell.borrow_mut() = Some(cache));

    // If the cache has been invalidated since we entered this routine, we still
    // use and return the cache we just finished constructing, to avoid infinite
    // loops, but we leave the cache marked stale so that we'll rebuild it again
    // on next access. Otherwise, we mark the cache valid.
    //   if (EventTriggerCacheState == ETCS_REBUILD_STARTED)
    //       EventTriggerCacheState = ETCS_VALID;
    EVENT_TRIGGER_CACHE_STATE.with(|s| {
        let mut state = s.borrow_mut();
        if *state == RebuildStarted {
            *state = Valid;
        }
    });

    Ok(())
}

/// Build the charged cache: open `pg_event_trigger` + its name index under
/// `AccessShareLock`, scan in name order, and build a cache item for each tuple,
/// appending each to the appropriate cache entry. Returns the fully-built
/// `McxOwned`; on any error the partially-built context is dropped (freed).
fn build_cache() -> PgResult<McxOwned<CacheTy>> {
    McxOwned::<CacheTy>::try_new(MemoryContext::new("EventTriggerCache"), |cache_mcx| {
        let mut state = CacheState {
            mcx: cache_mcx,
            cache: PgHashMap::new_in(cache_mcx),
        };

        // Prepare to scan pg_event_trigger in name order. The scan tuples and
        // their decode live in a scratch context, distinct from the cache's.
        //   rel = relation_open(EventTriggerRelationId, AccessShareLock);
        //   irel = index_open(EventTriggerNameIndexId, AccessShareLock);
        //   scan = systable_beginscan_ordered(rel, irel, NULL, 0, NULL);
        let scratch = MemoryContext::new("EventTriggerCache build");
        let smcx = scratch.mcx();

        let rel = table_open(smcx, EventTriggerRelationId, AccessShareLock)?;
        let irel = indexam_seams::index_open::call(smcx, EventTriggerNameIndexId, AccessShareLock)?;
        let mut scan = genam_seams::systable_beginscan_ordered::call(&rel, &irel, None, &[])?;

        // for (;;) { tup = systable_getnext_ordered(scan, ForwardScanDirection);
        //            if (!HeapTupleIsValid(tup)) break; ... }
        while let Some(tup) =
            genam_seams::systable_getnext_ordered::call(smcx, scan.desc_mut(), ForwardScanDirection)?
        {
            let row = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;

            // Skip trigger if disabled.
            // form = (Form_pg_event_trigger) GETSTRUCT(tup);
            // if (form->evtenabled == TRIGGER_DISABLED) continue;
            let evtenabled = byval_char(&row[Anum_pg_event_trigger_evtenabled - 1])?;
            if evtenabled == TRIGGER_DISABLED {
                continue;
            }

            // Decode event name.
            // evtevent = NameStr(form->evtevent);
            let evtevent = name_str(&row[Anum_pg_event_trigger_evtevent - 1])?;
            let event = match evtevent.as_str() {
                "ddl_command_start" => EventTriggerEvent::DdlCommandStart,
                "ddl_command_end" => EventTriggerEvent::DdlCommandEnd,
                "sql_drop" => EventTriggerEvent::SqlDrop,
                "table_rewrite" => EventTriggerEvent::TableRewrite,
                "login" => EventTriggerEvent::Login,
                // else continue;
                _ => continue,
            };

            // Allocate new cache item.
            // item = palloc0(sizeof(EventTriggerCacheItem));
            // item->fnoid = form->evtfoid;
            // item->enabled = form->evtenabled;
            let fnoid = byval_oid(&row[Anum_pg_event_trigger_evtfoid - 1])?;
            let mut item = EventTriggerCacheItem {
                fnoid,
                enabled: evtenabled,
                tagset: None,
            };

            // Decode and sort tags array.
            // evttags = heap_getattr(tup, Anum_pg_event_trigger_evttags, ...);
            // if (!evttags_isnull)
            //     item->tagset = DecodeTextArrayToBitmapset(evttags);
            let (evttags, evttags_isnull) = &row[Anum_pg_event_trigger_evttags - 1];
            if !*evttags_isnull {
                let bytes = match evttags {
                    Datum::ByRef(b) => &b[..],
                    Datum::ByVal(_)
                    | Datum::Cstring(_)
                    | Datum::Composite(_)
                    | Datum::Expanded(_)
                    | Datum::Internal(_) => {
                        return Err(PgError::error("evttags datum is not by-reference"));
                    }
                };
                item.tagset = DecodeTextArrayToBitmapset(state.mcx, bytes)?;
            }

            // Add to cache entry.
            // entry = hash_search(cache, &event, HASH_ENTER, &found);
            // if (found)  entry->triggerlist = lappend(entry->triggerlist, item);
            // else        entry->triggerlist = list_make1(item);
            let cache_mcx = state.mcx;
            if !state.cache.contains_key(&event) {
                state
                    .cache
                    .try_reserve(1)
                    .map_err(|_| cache_mcx.oom(core::mem::size_of::<EventTriggerEvent>()))?;
                state.cache.insert(event, PgVec::new_in(cache_mcx));
            }
            let triggerlist = state
                .cache
                .get_mut(&event)
                .expect("triggerlist present after insert");
            triggerlist
                .try_reserve(1)
                .map_err(|_| cache_mcx.oom(core::mem::size_of::<EventTriggerCacheItem>()))?;
            triggerlist.push(item);
        }

        // Done with pg_event_trigger scan.
        // systable_endscan_ordered(scan);
        // index_close(irel, AccessShareLock);
        // relation_close(rel, AccessShareLock);
        scan.end()?;
        irel.close(AccessShareLock)?;
        table_close(rel, AccessShareLock)?;
        // The scratch context drops here, freeing the transient scan tuples.

        Ok(state)
    })
}

/// `static Bitmapset *DecodeTextArrayToBitmapset(Datum array)`
///
/// Decode text[] to a Bitmapset of CommandTags.
///
/// The detoast + `ARR_NDIM/ARR_HASNULL/ARR_ELEMTYPE` validity check
/// (`elog(ERROR, "expected 1-D text array")`) + `deconstruct_array_builtin` is
/// array machinery, performed behind the arrayfuncs seam; the per-element
/// `GetCommandTagEnum`/`bms_add_member` accumulation loop is this function's own
/// logic.
fn DecodeTextArrayToBitmapset<'mcx>(
    mcx: Mcx<'mcx>,
    array: &[u8],
) -> PgResult<Option<::mcx::PgBox<'mcx, nodes::Bitmapset<'mcx>>>> {
    // arr = DatumGetArrayTypeP(array);
    // if (ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != TEXTOID)
    //     elog(ERROR, "expected 1-D text array");
    // deconstruct_array_builtin(arr, TEXTOID, &elems, NULL, &nelems);
    let elems = array_seams::decode_text_array_to_strings::call(mcx, array)?;

    // for (bms = NULL, i = 0; i < nelems; ++i) {
    //     char *str = TextDatumGetCString(elems[i]);
    //     bms = bms_add_member(bms, GetCommandTagEnum(str));
    //     pfree(str);
    // }
    let mut bms: Option<::mcx::PgBox<'mcx, nodes::Bitmapset<'mcx>>> = None;
    for str in elems.iter() {
        let tag = get_command_tag_enum(str.as_bytes());
        bms = Some(bms_seams::bms_add_member::call(mcx, bms, tag)?);
    }

    // return bms;
    Ok(bms)
}

/// `static void InvalidateEventCacheCallback(Datum arg, int cacheid, uint32 hashvalue)`
///
/// Flush all cache entries when pg_event_trigger is updated.
///
/// This should be rare enough that we don't need to be very granular about it,
/// so we just blow away everything, which also avoids the possibility of memory
/// leaks. The three C arguments are unused.
fn InvalidateEventCacheCallback(_arg: datum::datum::Datum, _cacheid: i32, _hashvalue: u32) {
    // If the cache isn't valid, then there might be a rebuild in progress, so we
    // can't immediately blow it away. But it's advantageous to do this when
    // possible, so as to immediately free memory.
    //   if (EventTriggerCacheState == ETCS_VALID)
    //   {
    //       MemoryContextReset(EventTriggerCacheContext);
    //       EventTriggerCache = NULL;
    //   }
    let valid = EVENT_TRIGGER_CACHE_STATE.with(|s| *s.borrow() == Valid);
    if valid {
        // Dropping the McxOwned frees its context (the MemoryContextReset analog).
        EVENT_TRIGGER_CACHE.with(|cell| *cell.borrow_mut() = None);
    }

    // Mark cache for rebuild.
    // EventTriggerCacheState = ETCS_NEEDS_REBUILD;
    EVENT_TRIGGER_CACHE_STATE.with(|s| *s.borrow_mut() = NeedsRebuild);
}

/// Read a by-value `char` attribute (`evtenabled`).
fn byval_char(col: &(Datum<'_>, bool)) -> PgResult<i8> {
    match &col.0 {
        Datum::ByVal(_) => Ok(col.0.as_i32() as i8),
        Datum::ByRef(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => Err(PgError::error("pg_event_trigger char attr is by-reference")),
    }
}

/// Read a by-value `Oid` attribute (`evtfoid`).
fn byval_oid(col: &(Datum<'_>, bool)) -> PgResult<Oid> {
    match &col.0 {
        Datum::ByVal(_) => Ok(col.0.as_oid()),
        Datum::ByRef(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => Err(PgError::error("pg_event_trigger oid attr is by-reference")),
    }
}

/// `NameStr` of a `NameData` (`name`) attribute, as an owned string (read up to
/// the first NUL of the fixed-width field).
fn name_str(col: &(Datum<'_>, bool)) -> PgResult<String> {
    match &col.0 {
        Datum::ByRef(b) => {
            let len = b.iter().position(|&c| c == 0).unwrap_or(b.len());
            Ok(String::from_utf8_lossy(&b[..len]).into_owned())
        }
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => Err(PgError::error("pg_event_trigger name attr is by-value")),
    }
}

/// This crate declares one inward seam (`event_cache_lookup`); install it.
pub fn init_seams() {
    evtcache_seams::event_cache_lookup::set(EventCacheLookup);
}

#[cfg(test)]
mod tests;
