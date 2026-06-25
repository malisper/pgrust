# Audit: backend-utils-cache-small

Unit: `backend-utils-cache-small` (`src/backend/utils/cache/{attoptcache,
relfilenumbermap,spccache,syscache,ts_cache}.c`, 2166 lines total,
PostgreSQL 18.3).
Crates audited: `backend-utils-cache-attoptcache`,
`backend-utils-cache-spccache`, `backend-utils-cache-relfilenumbermap`,
`backend-utils-cache-syscache` (+ `cacheinfo.rs`, `projections.rs`),
`backend-utils-cache-ts-cache`, the new seam crates
(`backend-utils-cache-catcache-seams`, `backend-utils-cache-inval-seams`,
`backend-utils-cache-relmapper-seams`, `backend-access-index-genam-seams`,
`backend-access-common-reloptions-seams`,
`backend-optimizer-path-costsize-seams`,
`backend-storage-buffer-bufmgr-seams`, `backend-catalog-namespace-seams`,
`backend-utils-adt-ruleutils-seams`, `backend-commands-tsearchcmds-seams`,
`backend-utils-fmgr-fmgr-seams`), the extended seam crates
(lock/lsyscache/init-small/xact/regproc), and the new type crates
(`types-cache`, `types-reloptions`, `types-storage::lock`,
`types-guc::GucSource`, `types-core::RegProcedure`).
Cross-checked against `../pgrust/c2rust-runs/backend-utils-cache-small/src/*.rs`
and the catalog headers (`pg_class.h`, `pg_attribute.h`, `pg_tablespace.h`,
`pg_opclass.h`, `pg_amop.h`, `pg_amproc.h`, `pg_ts_*.h`, `lockdefs.h`,
`lock.h`, `guc.h`, `stratnum.h`, `fmgroids.h`).
Auditor: independent re-derivation from the C sources; the cacheinfo table and
all OID/attno/enum constants mechanically diffed against the c2rust rendering.

## Sanctioned design divergences audited against (crate docs)

- **Owned-copy tuple model**: C `SearchSysCache*` return refcounted pointers
  into the catcache; here every search returns a `FormedTuple` copy in the
  caller's `mcx` and `ReleaseSysCache` consumes it. The `heap_copytuple`
  convenience wrappers reduce to copying the already-owned tuple. Verified to
  have no caller-visible behavioral consequence within this unit.
- **Per-backend statics as `thread_local!` state** with owning memory contexts
  (the `CacheMemoryContext` analogs); `CreateCacheMemoryContext()` calls have
  no counterpart because the contexts are created with the state.
- **`FmgrInfo` cannot cross seams**: entries carry function OIDs; the eager
  `fmgr_info_cxt` lookup-failure surface is preserved via
  `fmgr_seams::fmgr_info_check` at the same program points. The relfilenumbermap
  scankeys cross unresolved (`sk_procedure = F_OIDEQ`); the C
  `fmgr_info_cxt(F_OIDEQ, ...)` at init cannot fail for a builtin, so no error
  surface is lost.
- **`dictData`** (the template init method's `void *`) crosses as an opaque
  `Datum` word.
- **HTAB → `PgHashMap`**: `pfree`+`HASH_REMOVE` loops become `clear`/`retain`;
  the C `elog(ERROR, "hash table corrupted")` on a HASH_REMOVE miss guards a
  condition that cannot occur for a map removal (the HASH_ENTER duplicate check
  in `RelidByRelfilenumber` *is* preserved as `insert(...).is_some()` →
  `Err("corrupted hashtable")`). attoptcache stores the syscache hash value per
  entry to reproduce `hash_seq_init_with_hash_value` selective flushes.
- **GUC plumbing for `default_text_search_config`**: C's GUC machinery owns the
  string and stores it after calling the assign hook; the crate owns the
  variable so `assign_default_text_search_config` folds the store in. The
  `guc_strdup(LOG, ...)` OOM → `return false` path in the check hook has no
  counterpart for an owned `String` (documented in-source).

## Function inventory

### attoptcache.c (4 functions)

| # | C function | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `InvalidateAttoptCacheCallback` (:53) | attoptcache `InvalidateAttoptCacheCallback` | MATCH | hashvalue==0 → clear all; else remove entries with matching stored syscache hash (C: `hash_seq_init_with_hash_value` walk + remove). |
| 2 | `relatt_cache_syshash` (:84) | `relatt_cache_syshash` | MATCH | `GetSysCacheHashValue2(ATTNUM, attrelid, attnum)`. C passes `int attnum` as Datum; the catcache key hasher truncates via `DatumGetInt16` either way. |
| 3 | `InitializeAttoptCache` (:96) | `InitializeAttoptCache` | MATCH | map + owning context; `CacheRegisterSyscacheCallback(ATTNUM, cb, 0)` via inval seam. |
| 4 | `get_attribute_options` (:127) | `get_attribute_options` | MATCH | lazy init; cache hit returns by-value copy (C: palloc'd copy in caller cx); miss: `SearchSysCache2(ATTNUM, oid, int16)`, missing tuple → no options; `SysCacheGetAttr(attoptions=23)` null → none, else `attribute_reloptions(datum, false)` across the reloptions seam (verbatim varlena bytes, owner detoasts/parses as in C); entry created only after the read (cache-flush ordering comment preserved). `Anum_pg_attribute_attoptions=23` verified against pg_attribute.h and c2rust. |

### spccache.c (6 functions)

| # | C function | Port location | Verdict | Notes |
|---|---|---|---|---|
| 5 | `InvalidateTableSpaceCacheCallback` (:54) | spccache `InvalidateTableSpaceCacheCallback` | MATCH | Flush-all semantics (C walks and removes every entry). |
| 6 | `InitializeTableSpaceCache` (:77) | `InitializeTableSpaceCache` | MATCH | `CacheRegisterSyscacheCallback(TABLESPACEOID, ...)`. |
| 7 | `get_tablespace` (:105) | `get_tablespace` | MATCH | `InvalidOid → MyDatabaseTableSpace` (init-small seam) **before** lookup, after lazy init — same order as C; miss path: `SearchSysCache1(TABLESPACEOID)`, missing → NULL opts; `spcoptions` (Anum=5, verified) null → none else `tablespace_reloptions(datum,false)` seam; entry inserted only after the read. Returns the entry's opts by value (callers only read `opts`). |
| 8 | `get_tablespace_page_costs` (:178) | `get_tablespace_page_costs` | MATCH (fixed) | Nullable out-pointers as `Option<&mut f64>`. **Fix round 1**: original port used `>= 0.0` (not the complement of C's `< 0` under NaN); now `!(x < 0.0)`, bit-exact complement. GUC fallbacks via costsize seam. |
| 9 | `get_tablespace_io_concurrency` (:206) | same name | MATCH | i32 `>= 0` is the exact complement of `< 0`; bufmgr seam fallback. |
| 10 | `get_tablespace_maintenance_io_concurrency` (:216) | same name | MATCH | As above. |

### relfilenumbermap.c (3 functions)

| # | C function | Port location | Verdict | Notes |
|---|---|---|---|---|
| 11 | `RelfilenumberMapInvalidateCallback` (:50) | same name | MATCH | Relcache callback; removes on `relid == InvalidOid` (reset) ∨ entry negative ∨ entry == relid; `expect` mirrors the C Assert (callback registered only after hash creation). |
| 12 | `InitializeRelfilenumberMap` (:85) | same name | MATCH | skey template `[reltablespace(9), relfilenode(8)]`, `BTEqualStrategyNumber=3`, `F_OIDEQ=184` (all verified vs c2rust/headers); hash created with the state; `CacheRegisterRelcacheCallback`. |
| 13 | `RelidByRelfilenumber` (:140) | same name | MATCH | init → `MyDatabaseTableSpace → 0` normalization → cache probe (negative entries returned from cache); shared (`GLOBALTABLESPACE_OID=1664`) → `RelationMapFilenumberToOid(true)` relmapper seam; else batched `systable_scan` (pg_class 1259, `ClassTblspcRelfilenodeIndexId=3455`, index_ok=true) with the per-row loop **in-crate**: `RELPERSISTENCE_TEMP='t'` skip, duplicate → `Err("unexpected duplicate for tablespace %u, relfilenumber %u")` (format matched), `oid` col (Anum=1), Asserts as `debug_assert`; not found → relmapper(false); insert after scan with C's `elog(ERROR, "corrupted hashtable")` on duplicate. pg_class attnos re-derived from pg_class.h (oid=1, relfilenode=8, reltablespace=9, relpersistence=17). |

### syscache.c (26 functions + cacheinfo table)

| # | C function | Port location | Verdict | Notes |
|---|---|---|---|---|
| 14 | `cacheinfo[]` / `SysCacheIdentifier` (syscache_info.h / syscache_ids.h) | `cacheinfo.rs` | MATCH | All 85 entries (reloid, indoid, nkeys, key[4], nbuckets) and all 85 enum ids **mechanically diffed against the c2rust rendering — zero mismatches**. `SysCacheSize = 85`. |
| 15 | `InitCatalogCache` (:108) | `InitCatalogCache` | MATCH | Asserts as `debug_assert`; per-cache `InitCatCache` via seam, `!created` → `Err("could not initialize cache %u (%d)")`; OID lists accumulated, qsort+qunique = `sort_unstable`+`dedup` (Oid unsigned: `Ord` ≡ `pg_cmp_u32`); `CacheInitialized = true`. |
| 16 | `InitCatalogCachePhase2` (:181) | same name | MATCH | `InitCatCachePhase2(cache, true)` per cache via seam. |
| 17 | `SearchSysCache` (:208) | same name | SEAMED | Assert + delegate to `search_cat_cache` (catcache owns the entries — real cycle: inval/catcache ↔ syscache). |
| 18–21 | `SearchSysCache1..4` (:222–:262) | same names | SEAMED | Assert + `cc_nkeys` debug check (via `cache_nkeys` seam) + delegate. |
| 22 | `ReleaseSysCache` (:270) | same name | MATCH | Consumes the owned copy (refcount dropped owner-side at search; sanctioned model). |
| 23 | `SearchSysCacheLocked1` (:289) | same name | MATCH | TID-loop re-derived line by line: invalid-tid first pass; on re-fetch — miss → `LockRelease` + return None; TID equal → return tuple (lock kept); else release and retry; `SET_LOCKTAG_TUPLE(dboid = cc_relisshared ? InvalidOid : MyDatabaseId, cc_reloid, blk, off)` with `LOCKTAG_TUPLE=4`, `DEFAULT_LOCKMETHOD=1`, `InplaceUpdateTupleLock = ExclusiveLock = 7` (lockdefs.h verified); `LockAcquire(..., false, false)`; `AcceptInvalidationMessages()`. ItemPointer helpers match the C macros (`ip_posid != 0` validity; InvalidBlockNumber 0xFFFFFFFF). |
| 24 | `SearchSysCacheCopy` (:380) | same name | MATCH | Copy + release (copy semantics subsumed; heap_copytuple from audited heaptuple unit). |
| 25 | `SearchSysCacheLockedCopy1` (:404) | same name | MATCH | |
| 26 | `SearchSysCacheExists` (:425) | same name | MATCH | |
| 27 | `GetSysCacheOid` (:447) | same name | MATCH | `heap_getattr(oidcol, cc_tupdesc)`; `Assert(!isNull)` as debug_assert; miss → InvalidOid. |
| 28 | `SearchSysCacheAttName` (:478) | same name | MATCH | ATTNAME(6), name key as `SysCacheKey::Str`; `attisdropped` (Anum=17, verified against pg_attribute.h) → release + None. |
| 29 | `SearchSysCacheCopyAttName` (:500) | same name | MATCH | |
| 30 | `SearchSysCacheExistsAttName` (:519) | same name | MATCH | |
| 31 | `SearchSysCacheAttNum` (:539) | same name | MATCH | ATTNUM(7), `Int16GetDatum`. |
| 32 | `SearchSysCacheCopyAttNum` (:561) | same name | MATCH | C returns NULL both for miss and dropped; port `None` for both. |
| 33 | `heap_getattr` (htup_details.h macro) | `heap_getattr` | MATCH | Composed exactly per the macro: `attnum <= 0` → `heap_getsysattr`; `attnum > HeapTupleHeaderGetNatts` → `getmissingattr`; null-bitmap check (`fastgetattr`'s `HeapTupleNoNulls`/`att_isnull`, via audited `heap_attisnull`); else `nocachegetattr`. All pieces from the audited heaptuple unit. |
| 34 | `SysCacheGetAttr` (:583) | same name | MATCH | range/initialized check → `Err("invalid cache ID: %d")` (same message); `cc_tupdesc` invalid → `InitCatCachePhase2(cache, false)` via seam; then heap_getattr against the borrowed tupdesc (callback-shaped seam). |
| 35 | `SysCacheGetAttrNotNull` (:614) | same name | MATCH | Null → `Err("unexpected null value in cached tuple for catalog %s column %s")` with `get_rel_name` (lsyscache seam) and the tupdesc attname. (C `%s` of a NULL `get_rel_name` would print "(null)"; port prints "" — unreachable for catalog relations, noted only.) |
| 36 | `GetSysCacheHashValue` (:642) | same name (+ `GetSysCacheHashValue1/2` macros) | SEAMED | invalid-id check in-crate, hash via catcache seam. |
| 37 | `SearchSysCacheList` (:660) | same name (+ `SearchSysCacheList1` macro) | SEAMED | invalid-id check in-crate; list crosses as member-tuple copies in index order. |
| 38 | `SysCacheInvalidate` (:680) | same name | MATCH | id range error; uninitialized cache → no-op; delegate. |
| 39 | `RelationInvalidatesSnapshotsOnly` (:703) | same name | MATCH | All 7 OIDs (2964, 2608, 1214, 2609, 2396, 3596, 3592) verified against the catalog headers/c2rust. |
| 40 | `RelationHasSysCache` (:726) | same name + `oid_binary_search` | MATCH | Binary search reproduced (`int` low/high widened to i64 so the empty-array `high=-1` can't underflow a usize). |
| 41 | `RelationSupportsSysCache` (:748) | same name | MATCH | |
| 42 | `oid_compare` (:793) | subsumed by `sort_unstable` | MATCH | `pg_cmp_u32` ≡ unsigned `Ord`. |

`projections.rs` (implements the pre-existing `backend-utils-cache-syscache-seams`
declarations owned by this unit): `search_opclass` =
`SearchSysCache1(CLAOID)` + opcname(3)/opcfamily(6)/opcintype(7);
`search_amop_list` = `SearchSysCacheList1(AMOPSTRATEGY)` + amop attnos 3–7,9;
`search_amproc_list` = `SearchSysCacheList1(AMPROCNUM)` + amproc attnos 3–6.
All attnos re-derived from pg_opclass.h/pg_amop.h/pg_amproc.h. Thin
lookup+projection, installed by `init_seams()`. MATCH.

### ts_cache.c (8 functions)

| # | C function | Port location | Verdict | Notes |
|---|---|---|---|---|
| 43 | `InvalidateTSCacheCallBack` (:94) | same name | MATCH | C passes the HTAB address as `arg`; port passes a cache tag (1/2/3) in the Datum — same dispatch. Marks all entries `isvalid=false` (no removal, as in C); config tag also resets `TSCurrentConfigCache`. |
| 44 | `lookup_ts_parser_cache` (:113) | same name | MATCH | Lazy init + `CacheRegisterSyscacheCallback(TSPARSEROID)`; single-entry fast path (last==prsId ∧ valid); miss/invalid: `SearchSysCache1(TSPARSEROID)` miss → `Err("cache lookup failed for text search parser %u")`; sanity checks on prsstart(4)/prstoken(5)/prsend(6) with C messages; fields incl. prsheadline(7)/prslextype(8); `fmgr_info_cxt` × start/token/end (+headline if valid, **not** lextype — as in C) via `fmgr_info_check`; entry inserted, `lastUsedParser` set. Error-mid-build leaves no valid entry (C leaves an invalid one) — externally identical on retry. |
| 45 | `lookup_ts_dictionary_cache` (:204) | same name | MATCH (fixed) | TSDICTOID+TSTEMPLATEOID callbacks; dict lookup → template lookup with all four C error messages verbatim (incl. the C oddity of printing `tmpllexize` (0) in "text search template %u has no lexize method"); dictCtx created ("TS dictionary", ident = dictname) or reset (ident cleared first) — same order as C; `tmplinit` valid → dictinitoption (Anum=6) null → NIL else `deserialize_deflist` + `OidFunctionCall1(tmplinit, options)` across seams; releases; `fmgr_info_check(lexize)`; insert + lastUsed. **Fix round 1**: the original port decoded the `text` varlena in-crate without handling inline-compressed values (C's `deserialize_deflist` does `TextDatumGetCString`, which detoasts); the seam now takes the verbatim varlena bytes and the owner detoasts, matching C's division of labor (and the reloptions-seam pattern). |
| 46 | `init_ts_config_cache` (:392) | same name | MATCH | TSCONFIGOID + TSCONFIGMAP callbacks (split out so `getTSCurrentConfig` activates them before caching, as in C). |
| 47 | `lookup_ts_config_cache` (:413) | same name | MATCH | Miss/invalid: `SearchSysCache1(TSCONFIGOID)` + cfgparser(5) sanity check; ordered scan of pg_ts_config_map (3603 via index 3609 = TSCONFIGMAP cacheinfo row, verified) keyed on mapcfg(1); per-row loop **in-crate**: maptokentype(2) range check (`<= 0 || > 256`), out-of-order check (`< maxtokentype`), new-token-type save-prior logic, MAXDICTSPERTT=100 overflow check, mapdict(4) collection — all four elog messages verbatim; final save (`ndicts > 0` ⇔ the port's `!(mapdicts.is_empty() && maxtokentype == 0)`, proven equivalent: ndicts==0 at loop end ⇔ no rows), `lenmap = maxtokentype+1` with empty slots for absent token types; entry stored in cache context, returned as a caller-`mcx` copy. |
| 48 | `getTSCurrentConfig` (:496) | same name | MATCH | Cached-OID fast path first; unset/empty GUC → `Err("text search configuration isn't set")` or InvalidOid per `emitError`; lazy `init_ts_config_cache`; hard path `stringToQualifiedNameList(soft=false)` + `get_ts_config_oid(missing_ok=false)`; soft path NIL → InvalidOid else `get_ts_config_oid(true)`; result cached (including InvalidOid, which C also stores and which means "retry next time" both places). |
| 49 | `check_default_text_search_config` (:540) | same name | MATCH | Gate `IsTransactionState() && MyDatabaseId != InvalidOid` (xact + init-small seams); soft parse → InvalidOid on bad syntax; `!OidIsValid`: `PGC_S_TEST=12` (guc.h order verified) → NOTICE `ERRCODE_UNDEFINED_OBJECT` "text search configuration \"%s\" does not exist" + accept, else reject; valid: `SearchSysCache1(TSCONFIGOID)` + cfgname(2)/cfgnamespace(3) → `quote_qualified_identifier(get_namespace_name(nsp), name)` (NULL namespace crosses as `None`); `*newval` replaced (guc_strdup OOM path sanctioned). |
| 50 | `assign_default_text_search_config` (:609) | same name | MATCH | Resets `TSCurrentConfigCache`; also stores the owned string (sanctioned GUC-ownership adaptation, documented). |

## Seam audit

Outward seams (all thin marshal + delegate; per-tuple/per-entry logic verified
to stay in the consuming crates):

- `backend-utils-cache-catcache-seams` (new): InitCatCache(Phase2),
  SearchCatCache(1–4, List), GetCatCacheHashValue, CatCacheInvalidate,
  cc_nkeys/cc_relisshared/cc_tupdesc accessors. Real cycle (catcache scans use
  syscache-described catalogs; inval ↔ syscache). `CatCache *` crosses as the
  integer cache id. Declarations match the catcache.c signatures.
- `backend-utils-cache-inval-seams` (new): CacheRegisterSyscacheCallback /
  CacheRegisterRelcacheCallback / AcceptInvalidationMessages. Real cycle
  (inval.c calls `SysCacheInvalidate`). Callback types in `types-cache::inval`.
- `backend-utils-cache-relmapper-seams` (new): RelationMapFilenumberToOid.
- `backend-access-index-genam-seams` (new): `systable_scan` /
  `systable_scan_ordered` — the C begin/getnext/end iterator batched into one
  call returning deformed rows (owner-held scan state cannot cross a seam slot);
  open/lock/deform/close are genam/table-unit logic, and the consuming loops
  (persistence filter, duplicate detection, token-type grouping) remain
  in-crate. Locks persist to end of transaction as in C.
- `backend-access-common-reloptions-seams` (new): attribute_reloptions /
  tablespace_reloptions over verbatim varlena bytes (owner detoasts/validates,
  as in C).
- `backend-optimizer-path-costsize-seams`, `backend-storage-buffer-bufmgr-seams`
  (new): GUC-global reads (random/seq_page_cost, effective/maintenance_io_concurrency).
- `backend-catalog-namespace-seams` (new): get_ts_config_oid.
- `backend-utils-adt-ruleutils-seams` (new): quote_qualified_identifier.
- `backend-commands-tsearchcmds-seams` (new): deserialize_deflist (verbatim
  varlena in, typed `DefElemString` rows out — fixed in round 1, see above).
- `backend-utils-fmgr-fmgr-seams` (new): fmgr_info_check (eager-lookup failure
  surface), oid_function_call_1_deflist (the dictionary-init call shape;
  `dictData` opaque).
- Extended: lock-seams (LockAcquire/LockRelease), lsyscache-seams
  (get_rel_name/get_namespace_name), init-small-seams
  (MyDatabaseId/MyDatabaseTableSpace), xact-seams (IsTransactionState),
  regproc-seams (stringToQualifiedNameList, soft/hard forms).

Inward seams: `backend-utils-cache-syscache-seams` (pre-existing declarations,
docs updated only) — installed by `backend-utils-cache-syscache::init_seams()`,
which contains exactly three `set()` calls. The other four crates declare no
inward seams (`init_seams()` empty). `seams-init::init_all()` calls all five.
No `set()` outside the owner. No body-replaced-by-seam functions: every
function's control flow lives in this unit's crates.

Constants spot-verified against headers (not memory): all 85 cacheinfo rows +
85 ids (mechanical diff vs c2rust), pg_class/pg_attribute/pg_tablespace/
pg_opclass/pg_amop/pg_amproc/pg_ts_* attnos, LOCKTAG layout + lock modes +
`InplaceUpdateTupleLock`, `DEFAULT_LOCKMETHOD`, `LOCKACQUIRE_*`, `GucSource`
order (`PGC_S_TEST=12`), `BTEqualStrategyNumber=3`, `F_OIDEQ=184`,
`GLOBALTABLESPACE_OID=1664`, `RELPERSISTENCE_TEMP='t'`,
snapshot-only relation OIDs, `MAXTOKENTYPE=256`, `MAXDICTSPERTT=100`.

## Findings and fixes

Round 1 (FAIL → fixed in this branch):

1. **DIVERGES — ts_cache dictinitoption decode**: the port's `text_to_string`
   parsed the `text` varlena in-crate but mis-decoded inline-compressed
   varlenas (C reaches `TextDatumGetCString` → `pg_detoast_datum` inside
   `deserialize_deflist`). Fixed by passing the verbatim varlena bytes across
   `tsearchcmds-seams::deserialize_deflist` (owner detoasts, matching C's
   division of labor); `text_to_string` deleted.
2. **DIVERGES (edge) — spccache float predicate**: `get_tablespace_page_costs`
   used `>= 0.0` where C uses `!(x < 0)`; not complements under NaN. Fixed to
   the exact complement `!(x < 0.0)` for both float fields (int fields were
   already exact).

Both fixes re-audited from scratch against the C: see rows 8 and 45.

## Verdict

**PASS** — after the round-1 fixes, every function is MATCH (or SEAMED per the
seam rules), all seams are thin and installed by their owner, and the constant
tables are mechanically verified. Workspace builds clean and all 256 tests pass.
