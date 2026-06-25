# Audit: backend-foreign-foreign (`foreign/foreign.c`)

- Date: 2026-06-13
- Model: Claude Fable 5 (Opus 4.8, 1M context)
- Branch: `port/backend-foreign-foreign`
- Verdict: **PASS** (after one fix round)

Completeness oracle: `../pgrust/c2rust-runs/backend-foreign-foreign/src/foreign.rs`
(21 `foreign.c` functions; the c2rust file also renders the inline `Datum`/
`GETSTRUCT`/`newNode` macro helpers, which are not `foreign.c` logic). Cross-
checked against `../pgrust/postgres-18.3/src/backend/foreign/foreign.c`.

## Per-function table

| # | C fn (foreign.c) | C loc | Port loc (lib.rs) | Verdict |
|---|---|---|---|---|
| 1 | GetForeignDataWrapper | 38 | `GetForeignDataWrapper` | MATCH |
| 2 | GetForeignDataWrapperExtended | 50 | `GetForeignDataWrapperExtended` | MATCH (carrier trimmed; see notes) |
| 3 | GetForeignDataWrapperByName | 97 | `GetForeignDataWrapperByName` | MATCH |
| 4 | GetForeignServer | 112 | `GetForeignServer` | MATCH |
| 5 | GetForeignServerExtended | 124 | `GetForeignServerExtended` | MATCH (carrier trimmed) |
| 6 | GetForeignServerByName | 183 | `GetForeignServerByName` | MATCH |
| 7 | GetUserMapping | 201 | `GetUserMapping` | MATCH (fixed — was MISSING) |
| 8 | GetForeignTable | 255 | `GetForeignTable` | MATCH (fixed — was MISSING) |
| 9 | GetForeignColumnOptions | 293 | `GetForeignColumnOptions` | MATCH (fixed — was MISSING) |
| 10 | GetFdwRoutine | 326 | `GetFdwRoutine` | MATCH |
| 11 | GetForeignServerIdByRelId | 356 | `GetForeignServerIdByRelId` | MATCH |
| 12 | GetFdwRoutineByServerId | 378 | `GetFdwRoutineByServerId` | MATCH |
| 13 | GetFdwRoutineByRelId | 420 | `GetFdwRoutineByRelId` | MATCH |
| 14 | GetFdwRoutineForRelation | 443 | `GetFdwRoutineForRelation` | MATCH (makecopy collapse, see notes) |
| 15 | IsImportableForeignTable | 483 | `IsImportableForeignTable` | MATCH |
| 16 | pg_options_to_table | 523 | `pg_options_to_table` | MATCH |
| 17 | is_conninfo_option | 602 | `is_conninfo_option` | MATCH |
| 18 | postgresql_fdw_validator | 626 | `postgresql_fdw_validator` | MATCH |
| 19 | get_foreign_data_wrapper_oid | 682 | `get_foreign_data_wrapper_oid` | MATCH |
| 20 | get_foreign_server_oid | 705 | `get_foreign_server_oid` | MATCH |
| 21 | GetExistingLocalJoinPath | 742 | `GetExistingLocalJoinPath` | SEAMED-PANIC (prerequisite) |

Plus `MappingUserName` (foreign.h macro, line 21) — implemented as a fn (MATCH);
`libpq_conninfo_options[]` / `struct ConnectionOption` (576/565) — transcribed
value-for-value, 15 entries, contexts verified (MATCH; unit-tested).

## Fix round (FAIL → PASS)

The initial port was **FAIL**: three full `foreign.c` functions had no
implementation anywhere in the tree (not in this crate, not seamed, not in
foreigncmds) — `GetUserMapping`, `GetForeignTable`, `GetForeignColumnOptions`.
The module note claimed they were "owned by foreigncmds catalog-DML seams", but
foreigncmds implements no such logic and the inward seam crate declares no such
seams; the logic was simply absent (MISSING, a hard FAIL per the skill — only
panicking on an unported *callee* is acceptable, not absent own-logic).

Fixed by implementing all three with C-faithful control flow:
- `GetUserMapping`: SearchSysCache2(userid, serverid) → PUBLIC (`InvalidOid`)
  retry → `GetForeignServer(serverid)` + `ERRCODE_UNDEFINED_OBJECT`
  "user mapping not found for user \"%s\", server \"%s\"" with
  `MappingUserName(userid)`; `um->userid` carries the *requested* userid even
  when PUBLIC matched (C line 232); umoptions via `untransformRelOptions`.
- `GetForeignTable`: SearchSysCache1(FOREIGNTABLEREL) → elog "cache lookup
  failed for foreign table %u"; ftserver + ftoptions decode.
- `GetForeignColumnOptions`: SearchSysCache2(ATTNUM, relid, attnum) → elog
  "cache lookup failed for attribute %d of relation %u"; attfdwoptions decode.

Supporting changes (cross-crate, owner-installed when those owners land):
- `types-foreigncmds`: real `ForeignTable` / `UserMapping` carriers, mirroring
  the C structs and retaining `options` as `(name, value)` pairs (the type rule
  — define the real type now, trimmed to consumed fields; here the C fields).
- `backend-utils-cache-syscache-seams`: three new declared reads —
  `foreign_table_form` (ftserver + raw ftoptions), `user_mapping_form`
  (umid + raw umoptions, the raw `SearchSysCache2`, no PUBLIC logic — that lives
  in `GetUserMapping`), `attribute_fdwoptions` (raw attfdwoptions). The syscache
  owner is not a CATALOG-complete unit, so these sit declared-but-uninstalled
  (panic-until-owner-lands), exactly like the pre-existing
  `foreign_table_server_by_relid` / `foreign_*_form` seams — recurrence guard
  confirmed green.

## Notes (accepted divergences, behaviour-preserving)

- WRAPPER/SERVER carriers (`ForeignDataWrapper`/`ForeignServer`) are trimmed to
  the fields the in-tree consumers (foreigncmds, nodeForeignscan) read — they
  drop `owner`/`options`/`servertype`/`serverversion`. This is the established
  inward contract and the matching `*_form` syscache seams already drop those
  columns; no in-tree consumer reads them. The newly-added `ForeignTable`/
  `UserMapping` carriers DO retain options (their C structs' substantive payload
  and trivially available via the existing `untransformRelOptions` helper).
- `GetFdwRoutineForRelation`: the C `makecopy` distinction collapses because the
  resolved `FdwRoutine` is a `Copy` presence-flag table — the owned tree always
  hands back an owned table. Cache-hit returns the (copied) cached table;
  cache-miss returns the freshly-resolved table and stores a copy, exactly as C.
- `pg_options_to_table` / `postgresql_fdw_validator` read arg 0 via
  `pg_getarg_varlena_pp` (detoasts up front) rather than `PG_GETARG_DATUM`;
  equivalent because `untransformRelOptions`'s `DatumGetArrayTypeP` detoasts.
- `postgresql_fdw_validator` hint logic ported 1:1: `has_valid_options ?
  closest_match ? errhint(...) : 0 : errhint("There are no valid options ...")`
  via `levenshtein_closest_match(defname, candidates, 4)` (matches
  initClosestMatch/updateClosestMatch/getClosestMatch with max distance 4).
- `GetExistingLocalJoinPath` is a documented loud panic
  (mirror-PG-and-panic): the C walk `makeNode`-copies path *subtypes*
  (Hash/Nest/Merge/ForeignPath) and downcasts children, which the owned base-
  `Path` pathlist cannot recover until a unified walkable Node enum lands. It
  has no in-tree caller and is not in the inward seam contract — a faithful walk
  is impossible without the subtype model, so it panics rather than silently
  drop subtype fields.

## Seam / wiring audit

- Owned inward seam crate: `backend-foreign-foreign-seams` (covers `foreign.c`
  read accessors + the `pg_foreign_*` catalog DML foreigncmds issues + IMPORT +
  FDW-routine lookup). The `foreign.c`-owned subset installed by this crate's
  `init_seams()`: `get_foreign_data_wrapper[_by_name]`,
  `get_foreign_server_by_name`, `get_foreign_{server,data_wrapper}_oid`,
  `is_importable_foreign_table`, `mapping_user_name`,
  `get_fdw_routine_{for_relation,by_server_id}`. The catalog-DML / FDW-provider /
  IMPORT-parser seams in that bundle are installed by their own owners
  (foreigncmds, fdwapi, parser) — correct per ownership-by-C-coverage.
- `seams-init::init_all()` calls `backend_foreign_foreign::init_seams()`
  (lib.rs:60); Cargo dep present. Recurrence guard
  (`every_seam_installing_crate_is_wired_into_init_all` +
  `every_declared_seam_is_installed_by_its_owner`) PASS.
- No own-logic stubs; no `todo!()`/`unimplemented!()`. All outward calls are
  thin marshal+delegate into real owners.

## Gate

- `cargo check --workspace`: clean (pre-existing warnings only).
- `cargo test --workspace`: pass (ignoring the 2 known timeout flakes).
- `cargo test -p seams-init`: recurrence guard 2/2 pass.
- `cargo test -p backend-foreign-foreign`: 3/3 pass.
