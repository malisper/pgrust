# Audit: backend-access-common-next

- **Unit**: `backend-access-common-next` (`*/attmap.c`, `*/syncscan.c`, `*/tupconvert.c`)
- **Branch**: `port/backend-access-common-next`
- **Date**: 2026-06-13
- **Auditor model**: Claude Fable 5
- **Verdict**: **PASS**

Completeness oracle: `../pgrust/c2rust-runs/backend-access-common-next/src/{attmap,syncscan,tupconvert}.rs`.
Every C function definition enumerated from the three C sources and compared
function-by-function against the c2rust rendering and the Rust port.

## attmap.c (7 fns)

| C fn (loc) | port (loc) | verdict | notes |
|---|---|---|---|
| `make_attrmap` (attmap.c:40) | `attmap.rs:24` | MATCH | `palloc0` struct + zero-filled `AttrNumber[maplen]` -> `AttrMap{attnums}` reserved+`resize(n,0)`. Adds a negative-maplen guard (C `sizeof*maplen` would wrap); behavior-preserving. `maplen` is `attnums.len()`. |
| `free_attrmap` (attmap.c:56) | `attmap.rs:39` | MATCH | owned-drop reproduces `pfree(attnums); pfree(map)`. |
| `build_attrmap_by_position` (attmap.c:74) | `attmap.rs:51` | MATCH | identical j-cursor / nincols/noutcols/same logic, by-position inner loop, both datatype-mismatch ereports (`errmsg_internal` msg + the two errdetails), unused-input-column loop using `compact_attr`, one-to-one short-circuit -> `Ok(None)`. |
| `build_attrmap_by_name` (attmap.c:174) | `attmap.rs:142` | MATCH | `nextindesc` circular cursor (`-1` init, wrap at innatts), strcmp by `NameStr`, type+typmod check, both "could not convert row type" ereports with `format_type_be`; `missing_ok` path. |
| `build_attrmap_by_name_if_req` (attmap.c:260) | `attmap.rs:213` | MATCH | builds by name then `check_attrmap_match` -> `Ok(None)` / `Ok(Some)`. |
| `check_attrmap_match` (static, attmap.c:287) | `attmap.rs:234` | MATCH | natts equality guard; per-i: `atthasmissing`->false, `attnums[i]==i+1`->continue, dropped+dropped with equal `attlen`/`attalignby`->continue, else false; uses `compact_attr`. |
| (static fwd decl, attmap.c:29) | -- | n/a | declaration only. |

## syncscan.c (5 fns + statics)

| C fn (loc) | port (loc) | verdict | notes |
|---|---|---|---|
| `SyncScanShmemSize` (syncscan.c:126) | `syncscan.rs:133` (`sync_scan_shmem_size`) | MATCH | reports exact C-ABI footprint `offsetof(items) + N*sizeof(ss_lru_item_t)` = 16 + 20*32 = 656 via explicit C-ABI size consts (test `shmem_size_is_c_abi_footprint`). |
| `SyncScanShmemInit` (syncscan.c:135) | `syncscan.rs:140` (`sync_scan_shmem_init`) | MATCH | `!IsUnderPostmaster` init branch reduces to one-time `OnceLock` build of the LRU; all slots invalid + linked head=0/tail=N-1 (test `fresh_lru_is_all_invalid_and_linked`). ShmemInitStruct handshake modeled as process-global -- DESIGN_DEBT logged. |
| `ss_search` (static, syncscan.c:191) | `syncscan.rs:157` | MATCH | head-to-tail walk; match-or-tail take-over (set relid+loc when !match, set loc when set), unlink-from-tail/prev/next + relink-to-head LRU promotion, returns stored location. `Option<usize>` index links == NULL pointers. |
| `ss_get_location` (syncscan.c:254) | `syncscan.rs:213` | MATCH | LWLock-exclusive (Mutex) around `ss_search(...,0,false)`, then `startloc >= relnblocks -> 0` truncation guard. Key is relid (Oid) -- DESIGN_DEBT logged. |
| `ss_report_location` (syncscan.c:289) | `syncscan.rs:232` | MATCH | `location % SYNC_SCAN_REPORT_INTERVAL == 0` throttle; `LWLockConditionalAcquire` -> `try_lock`, `ss_search(...,location,true)`, silent miss on contention. `SYNC_SCAN_REPORT_INTERVAL = 128*1024/8192 = 16` (test). |
| `TRACE_SYNCSCAN` elog/GUC blocks | -- | n/a | `#ifdef TRACE_SYNCSCAN` not in build config; absent from c2rust. Correctly omitted. |

## tupconvert.c (7 fns)

| C fn (loc) | port (loc) | verdict | notes |
|---|---|---|---|
| `convert_tuples_by_position` (tupconvert.c:58) | `tupconvert.rs:48` | MATCH | `build_attrmap_by_position` -> `Ok(None)` on NULL; else build map. Per-conversion workspace Datum arrays (`invalues`/`outvalues`/...) recomputed in `execute_attr_map_tuple` rather than preallocated in the map -- behavior-preserving (the C struct's workspace is private scratch). Descriptors cloned into mcx-owned copies. |
| `convert_tuples_by_name` (tupconvert.c:102) | `tupconvert.rs:72` | MATCH | `build_attrmap_by_name_if_req(...,false)` -> `Ok(None)` / delegate to `_attrmap`. |
| `convert_tuples_by_name_attrmap` (tupconvert.c:124) | `tupconvert.rs:94` | MATCH | `Assert(attrMap != NULL)` modeled by non-optional param; builds map. |
| `execute_attr_map_tuple` (tupconvert.c:154) | `tupconvert.rs:128` | MATCH | deform with offset-by-1 (NULL at [0]), `Assert(maplen==outdesc.natts)` (debug_assert), transpose `outvalues[i]=invalues[attnums[i]]`, `heap_form_tuple`. Owned tuple model (data slice alongside header). |
| `execute_attr_map_slot` (tupconvert.c:192) | `tupconvert.rs:197` | SEAMED | slot value/null payload arrays (`tts_values`/`tts_isnull`) are owned by execTuples' slot-payload layer; the conversion (materialize/clear/transpose/store-virtual incl. the `j==-1`->NULL case) belongs to that owner. Delegated to `backend-executor-execTuples-seams::execute_attr_map_slot_explicit`. Thin marshal: copy borrowed attnums into per-query mcx, one delegate call. Justified cross-cycle seam. |
| `execute_attr_map_cols` (tupconvert.c:252) | `tupconvert.rs:219` | MATCH | `in_cols==NULL`->None fast path; out_attnum loop from `FirstLowInvalidHeapAttributeNumber` to maplen, system-col (<0) identity / 0-skip / user-col mapping with 0-skip, `bms_is_member`/`bms_add_member` with the offset shift. |
| `free_conversion_map` (tupconvert.c:299) | `tupconvert.rs:270` | MATCH | owned-drop reproduces the `free_attrmap` + workspace/`pfree`s; "indesc/outdesc not ours" applies to C's referenced descs, owned map holds its own copies. |

## Seam audit

Owned seam crates (by C-source coverage): `backend-access-common-next-seams`,
`backend-access-common-syncscan-seams`, `backend-access-common-tupconvert-seams`.
Every declaration in all three is installed by this crate's `init_seams()`:

- next-seams: `build_attrmap_by_name_if_req`, `convert_tuples_by_name`,
  `convert_tuples_by_name_attrmap`, `execute_attr_map_cols` -- all 4 set().
- syncscan-seams: `ss_get_location`, `ss_report_location`,
  `sync_scan_shmem_size`, `sync_scan_shmem_init` -- all 4 set().
- tupconvert-seams: `execute_attr_map_slot` -- set().

`init_seams()` contains only `set()` calls. `seams-init::init_all()` calls
`backend_access_common_next::init_seams()`. recurrence_guard tests
(`every_declared_seam_is_installed_by_its_owner`,
`every_seam_installing_crate_is_wired_into_init_all`) PASS.

Only outward seam in a port path: `execute_attr_map_slot` ->
`execute_attr_map_slot_explicit` (thin marshal+delegate, justified by the
execTuples slot-payload ownership boundary). No branching/computation in the
seam path.

## Design conformance

- No own-logic stubs / `todo!()` / `unimplemented!()`.
- All allocators take `Mcx` and return `PgResult` (`make_attrmap`,
  `build_*`, `convert_*`, `execute_attr_map_*`).
- Errors map to `ERRCODE_DATATYPE_MISMATCH` with matching detail.
- syncscan process-global + Oid-key divergences are both ledgered in
  DESIGN_DEBT.md (seam-contract divergence, behavior-equivalent hint).

## Gates

- `cargo check --workspace`: clean (pre-existing unrelated warnings only).
- `cargo test --workspace`: all suites pass; the single
  `backend-utils-misc-timeout::signal_handler_fires_reached_timeouts` flake
  passed on rerun (one of the two documented timeout flakes).
- crate tests (6) + recurrence_guard (2) PASS.
