# Audit: backend-utils-cache-evtcache

Independent function-by-function audit of `port/backend-utils-cache-evtcache`
(commit `1ba2d53d`) against `src/backend/utils/cache/evtcache.c` and the c2rust
rendering `../pgrust/c2rust-runs/backend-utils-cache-evtcache/src/evtcache.rs`.

C source: `src/backend/utils/cache/evtcache.c` (271 lines, 4 function defs).
Port crate: `crates/backend-utils-cache-evtcache/src/lib.rs`.
Types crate: `crates/types-evtcache/src/lib.rs`.
Seam crate: `crates/backend-utils-cache-evtcache-seams/src/lib.rs`.

## 1. Function inventory

The C file defines exactly four functions (one extern, three static); all other
identifiers in the c2rust run (`MemoryContextSwitchTo`, `GETSTRUCT`,
`heap_getattr`, `fetch_att`, `fastgetattr`, `att_isnull`, `HeapTupleHasNulls`,
`Char/Int16/Int32GetDatum`, `Datum/PointerGetDatum`, `TupleDescCompactAttr`) are
header macro/inline expansions pulled in post-preprocessor, not file-local
functions to port.

| # | C function | C loc | Port loc | Verdict |
|---|-----------|-------|----------|---------|
| 1 | `EventCacheLookup` | evtcache.c:62 | lib.rs `EventCacheLookup` | MATCH |
| 2 | `BuildEventTriggerCache` | evtcache.c:76 | lib.rs `BuildEventTriggerCache` + `build_cache` | MATCH |
| 3 | `DecodeTextArrayToBitmapset` | evtcache.c:221 | lib.rs `DecodeTextArrayToBitmapset` | MATCH (own loop in-crate; array machinery SEAMED, see §3) |
| 4 | `InvalidateEventCacheCallback` | evtcache.c:254 | lib.rs `InvalidateEventCacheCallback` | MATCH |

Helpers `byval_char`/`byval_oid`/`name_str` are the port's rendering of the C
`GETSTRUCT`/`NameStr`/`heap_getattr` field projections over a `heap_deform_tuple`
row (this repo has no `Form_pg_event_trigger` struct); behavior-preserving.

## 2. Per-function comparison

### EventCacheLookup — MATCH
- C: `if (state != ETCS_VALID) BuildEventTriggerCache();` → port checks
  `EVENT_TRIGGER_CACHE_STATE != Valid` then calls `BuildEventTriggerCache()?`.
- C: `hash_search(.., HASH_FIND)`; return `triggerlist` or `NIL`. Port looks up
  the `PgHashMap` entry; `None` → empty `PgVec`; else copies each item into the
  caller's `mcx` via `clone_in`.
- The C return is owned by the cache context and the header comment requires the
  caller to copy before any catalog op; pre-copying into the caller `mcx` is the
  documented persistent-state pattern and behavior-preserving.

### BuildEventTriggerCache — MATCH
- First-build branch (`EventTriggerCacheContext == NULL`): create context +
  `CacheRegisterSyscacheCallback(EVENTTRIGGEROID, InvalidateEventCacheCallback, 0)`.
  Port tracks this via `CACHE_INITIALIZED`; on first call registers the callback
  through `inval_seams::cache_register_syscache_callback::call` with
  `EVENTTRIGGEROID` (=26, verified) and `Datum::null()` (the `(Datum) 0` arg).
- Reset branch (context non-NULL → `MemoryContextReset`): subsumed by replacing
  the `McxOwned` (drop = free) each build.
- `EventTriggerCacheState = ETCS_REBUILD_STARTED` set before the scan — matches
  (port sets `RebuildStarted` before `build_cache`).
- Scan: `relation_open(EventTriggerRelationId=3466, AccessShareLock)` /
  `index_open(EventTriggerNameIndexId=3467, AccessShareLock)` /
  `systable_beginscan_ordered` → port `table_open` / `index_open::call` /
  `systable_beginscan_ordered::call` with the same OIDs + lock.
- Loop: `systable_getnext_ordered(.., ForwardScanDirection)`, break on invalid.
  Per-tuple: skip if `evtenabled == TRIGGER_DISABLED` ('D'); decode event name
  with the exact 5-way `strcmp` chain → enum, else `continue`; `palloc0` item
  with `fnoid=evtfoid`, `enabled=evtenabled`; `heap_getattr(evttags)` and if not
  null `tagset = DecodeTextArrayToBitmapset`; `hash_search(HASH_ENTER)` then
  `lappend`/`list_make1`. Port mirrors each step; the found/not-found append is a
  `contains_key` → insert-empty-vec then push, equivalent to lappend/list_make1.
  Attribute numbers (evtevent=3, evtfoid=5, evtenabled=6, evttags=7) verified
  against `pg_event_trigger.h` column order.
- Teardown: `systable_endscan_ordered` / `index_close` / `relation_close` →
  `scan.end()` / `irel.close` / `table_close`, all `AccessShareLock`.
- Install + finalize: `EventTriggerCache = cache`; then
  `if (state == ETCS_REBUILD_STARTED) state = ETCS_VALID;`. Port stores the
  `McxOwned` then flips `RebuildStarted → Valid`, leaving a concurrently-set
  `NeedsRebuild` intact (the stale-after-rebuild path). MATCH.

### DecodeTextArrayToBitmapset — MATCH
- The `DatumGetArrayTypeP` detoast + `ARR_NDIM != 1 || ARR_HASNULL ||
  ARR_ELEMTYPE != TEXTOID` validity check (`elog(ERROR, "expected 1-D text
  array")`) + `deconstruct_array_builtin(arr, TEXTOID, ...)` is array machinery
  owned by arrayfuncs; it is delegated to `array_seams::decode_text_array_to_strings`.
  Verified the owner (`backend-utils-adt-arrayfuncs/src/construct.rs:1190`)
  performs the detoast, the identical NDIM/HASNULL/ELEMTYPE check with the same
  error text, and the TEXTOID deconstruct — nothing is dropped.
- This function's own logic — the `for (bms=NULL; i<nelems; ++i) bms =
  bms_add_member(bms, GetCommandTagEnum(str))` accumulation — lives in-crate:
  `get_command_tag_enum(str)` then `bms_seams::bms_add_member::call`. Empty array
  → `bms` stays `None` (C: stays NULL). The `pfree(str)`/`pfree(elems)` are arena
  drops. MATCH.

### InvalidateEventCacheCallback — MATCH
- C: `if (state == ETCS_VALID) { MemoryContextReset(ctx); EventTriggerCache = NULL; }`
  then `state = ETCS_NEEDS_REBUILD;`. Port: if `Valid`, set the `McxOwned` slot to
  `None` (drop = reset/free + NULL the cache); unconditionally set `NeedsRebuild`.
  The three unused args mirror the C signature. MATCH.

## 3. Seams and wiring

Owned C source = `evtcache.c` → owned seam crate
`backend-utils-cache-evtcache-seams` declares one inward seam
`event_cache_lookup` (consumed by `commands/event_trigger.c` across the
catalog-scan cycle). It is installed by the crate's `init_seams()`
(`event_cache_lookup::set(EventCacheLookup)`), which contains nothing but the
one `set` call. `seams-init::init_all()` calls
`backend_utils_cache_evtcache::init_seams()` (seams-init/src/lib.rs:184; dep
present in Cargo.toml). Both recurrence-guard tests pass.

Outward seam calls, each a thin marshal+delegate across a genuine catalog/access
boundary (no branching or node-building in any seam path):
- `inval_seams::cache_register_syscache_callback` (callback registration).
- `table_open`/`table_close`, `indexam_seams::index_open`,
  `genam_seams::systable_beginscan_ordered`/`systable_getnext_ordered`
  (catalog scan).
- `backend_access_common_heaptuple::heap_deform_tuple` (tuple decode).
- `array_seams::decode_text_array_to_strings` (array detoast/validate/deconstruct
  machinery — owner-faithful, §2).
- `bms_seams::bms_add_member` (Bitmapset, owned by nodes-core).

No function body was replaced by a "delegate elsewhere" seam — the
`DecodeTextArrayToBitmapset` accumulation loop, the build state machine, and the
per-tuple decode/dispatch all live in this crate.

## 3b. Design conformance
- `EventCacheLookup` / `BuildEventTriggerCache` / `DecodeTextArrayToBitmapset`
  are allocating and correctly take `Mcx` + return `PgResult`.
- The per-backend file-scope statics (`EventTriggerCache`, cache state,
  initialized flag) are `thread_local!` `RefCell`s, not shared statics — correct
  for per-backend globals.
- No invented opacity: `EventTriggerEvent` is a real `#[repr(u32)]` enum matching
  the header declaration order; `EventTriggerCacheItem` is the real struct.
- No locks held across `?`; no registry side tables; no unledgered divergence
  markers.

## 4. Constants verified (against headers, not memory)
- `EventTriggerRelationId` = 3466, `EventTriggerNameIndexId` = 3467
  (pg_event_trigger.h CATALOG / DECLARE_UNIQUE_INDEX).
- `Anum` evtevent=3, evtfoid=5, evtenabled=6, evttags=7 (column order
  oid,evtname,evtevent,evtowner,evtfoid,evtenabled,evttags).
- `TRIGGER_DISABLED` = 'D' (commands/trigger.h:154).
- `EVENTTRIGGEROID` = 26 (syscache crate cacheinfo.rs:55; the symbolic syscache id).
- `EVT_*` discriminants 0..4 in header order (evtcache.h).

## Gates
- `cargo check --workspace`: clean (only pre-existing unrelated warnings in
  backend-access-common-printtup).
- `cargo test -p backend-utils-cache-evtcache`: 5 passed.
- `cargo test -p seams-init`: 2 passed (both recurrence-guard checks).

## Verdict: PASS

All four functions MATCH; the single SEAMED path (array machinery) is faithfully
implemented in its arrayfuncs owner with the own-logic accumulation loop kept
in-crate. Inward seam installed and wired; recurrence guard green; all gates
pass.
