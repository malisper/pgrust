# Audit: backend-catalog-pg-db-role-setting

C source: `src/backend/catalog/pg_db_role_setting.c` (262 lines, 3 functions).
c2rust: `../pgrust/c2rust-runs/backend-catalog-pg-db-role-setting/src/pg_db_role_setting.rs`.
Port: `crates/backend-catalog-pg-db-role-setting/src/lib.rs` (+ `tests.rs`).
Owned seam crate: `crates/backend-catalog-pg-db-role-setting-seams`.

Independent re-derivation from the C and headers; the port's comments were not trusted.

## Function inventory

The C file defines exactly three external functions; there are no statics or
inline helpers. c2rust confirms the same three function bodies (the rest of its
output is the preprocessed header type prelude).

| # | C function (loc) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `AlterSetting` (23-162) | `lib.rs::AlterSetting` | MATCH | three-way branch + inner update/delete decisions carried in-crate; scan/catalog ops SEAMED (see below) |
| 2 | `DropSetting` (169-207) | `lib.rs::DropSetting` | MATCH | OidIsValid key-selection carried in-crate; keyed delete-loop SEAMED |
| 3 | `ApplySetting` (219-261) | `lib.rs::ApplySetting` | MATCH (SEAMED body) | scan loop + `!isnull` guard + ProcessGUCArray SEAMED into the genam-bound owner |

Plus `process_db_role_settings` (re-homed from `postinit.c::process_settings`,
1309-1330): orchestration carried in-crate; prologue/epilogue SEAMED. This is
the `apply_db_role_settings` seam consumed by postinit — correctly homed here
because this unit owns the `pg_db_role_setting` relation.

## Per-function detail

### AlterSetting
- `valuestr = ExtractSetVariableArgs(setstmt)` -> `guc::extract_set_variable_args::call`
  (functioncmds owner; real cross-unit dependency). MATCH.
- `table_open` + 2x `ScanKeyInit` (setdatabase/setrole) + `systable_beginscan` on
  `DbRoleSettingDatidRolidIndexId` + `systable_getnext` + the `setconfig`
  `heap_getattr` decode -> `seam::alter_find` returning `AlterLookup { scan,
  tuple }`. `tuple` faithfully models the three states: `None`
  (`!HeapTupleIsValid`), `Some(None)` (row found, `setconfig` SQL NULL),
  `Some(Some(arr))` (non-NULL `text[]`). MATCH.
- Three-way branch carried in-crate exactly:
  - `kind == VAR_RESET_ALL` and tuple valid: `new = isnull ? NULL :
    GUCArrayReset(...)`; then `if (new)` update else delete. The `isnull` skip
    (no Reset call, fall to delete) is preserved — verified by test
    `reset_all_null_setconfig_skips_reset_but_deletes_tuple`.
  - else if tuple valid: `a = isnull ? NULL : arr`; `valuestr ?
    GUCArrayAdd(a,name,val) : GUCArrayDelete(a,name)`; then `if (a)` update else
    delete.
  - else if valuestr: `GUCArrayAdd(NULL,...)` + insert.
  - (RESET with no tuple: no-op except epilogue — verified.)
- `GUCArrayReset/Add/Delete` -> functioncmds guc seams (external). MATCH.
- `heap_modify_tuple+CatalogTupleUpdate` -> `update_setconfig`;
  `CatalogTupleDelete` -> `delete_found_tuple`;
  `heap_form_tuple+CatalogTupleInsert` -> `insert_setting`. SEAMED (catalog
  write + index maintenance, unported).
- `InvokeObjectPostAlterHookArg(2964, databaseid, 0, roleid, false)` +
  `systable_endscan` + `table_close(rel, NoLock)` -> `alter_finish`. SEAMED.
  All 8 AlterSetting branch outcomes covered by tests.

### DropSetting
- C builds `numkeys` from `OidIsValid(databaseid)` / `OidIsValid(roleid)`, then
  `table_beginscan_catalog` + `heap_getnext` loop with per-tuple
  `CatalogTupleDelete`, `table_close(RowExclusiveLock)`. The valid-OID *decision*
  is carried in-crate (two bools passed to `seam::drop_settings`); the keyed
  catalog scan + delete loop is SEAMED (tableam/heapam/CatalogTuple, unported).
  `debug_assert!` mirrors the "current callers pass >=1 valid OID" invariant
  (non-load-bearing). MATCH.

### ApplySetting
- C body: 2x ScanKeyInit, `systable_beginscan`, `while
  (HeapTupleIsValid(tup=systable_getnext(scan)))` { `heap_getattr` setconfig;
  `if(!isnull) ProcessGUCArray(a, PGC_SUSET, source, GUC_ACTION_SET)` },
  `systable_endscan`. The entire body crosses to `seam::apply_setting`.
  The scan iterator (`systable_getnext`) IS the genam primitive and is unported
  (genam.c = `todo`), so the loop cannot be expressed in-crate; the `!isnull`
  guard and the `PGC_SUSET`/`GUC_ACTION_SET` constants are recorded in the seam
  doc and honored there. This is the repo's sanctioned mirror-and-panic on an
  unported scan owner (same model as the postinit batched-scan precedent), not
  absent own-logic — the function carries no decision distinct from the scan
  itself. MATCH (SEAMED).

### process_db_role_settings (re-homed from postinit::process_settings)
- `IsUnderPostmaster` early-return is applied by the postinit caller (documented).
- `apply_open` (table_open AccessShareLock + RegisterSnapshot(GetCatalogSnapshot)),
  then the four `ApplySetting` calls in exact scope order — DATABASE_USER,
  USER(InvalidOid db), DATABASE(InvalidOid role), GLOBAL(both InvalidOid) — then
  `apply_close` (UnregisterSnapshot + table_close). Order + OID args verified
  against postinit.c:1322-1329. MATCH.

## Constants verified (against headers, not memory)
- `DbRoleSettingRelationId = 2964`, `DbRoleSettingDatidRolidIndexId = 2965`
  (`pg_db_role_setting.h:34,51`).
- setdatabase/setrole/setconfig = attrs 1/2/3, `Natts_pg_db_role_setting = 3`.
- `PGC_SUSET` and `GUC_ACTION_SET` exist in `utils/guc.h:78,203`.
- `BTEqualStrategyNumber` / `F_OIDEQ` are part of the seamed scan setup (genam).

## Seam audit (ownership = C-source coverage)

Owned seam crate: `backend-catalog-pg-db-role-setting-seams` (maps to
pg_db_role_setting.c). 10 declared seams:
- `apply_db_role_settings` — INSTALLED by `init_seams()` ->
  `process_db_role_settings`. This is the only seam this unit can satisfy without
  the relcache/genam.
- `alter_find`, `update_setconfig`, `delete_found_tuple`, `insert_setting`,
  `alter_finish`, `drop_settings`, `apply_setting`, `apply_open`, `apply_close`
  — DECLARED, UNSET, panic-on-call (mirror-and-panic). Each genuinely depends on
  unported owners: relcache `table_open` (relcache = not a crate yet),
  genam `systable_*` (genam.c = `todo`), heapam `heap_getnext`/`heap_form_tuple`,
  and CatalogTuple{Insert,Update,Delete} (catalog indexing, unported). A real
  dependency cycle / missing owner exists for every one, so they cannot be
  installed here. This is sanctioned by the repo's "Mirror PG and panic" rule;
  the decision logic of the C file (the branches) lives in-crate, not in the seams.
- `init_seams()` contains only `set()` and is wired into
  `seams-init::init_all()` (lib.rs:48). recurrence_guard passes.

No outward seam contains branching / node construction / computation — each is a
thin marshal+delegate. The functioncmds `guc_array_*` / `extract_set_variable_args`
seams are pre-existing externals (real cross-unit deps).

## Design conformance
- Allocating/orchestrating fns take `Mcx` and return `PgResult` (AlterSetting,
  alter_finish, apply_open, process_db_role_settings). OK.
- No invented opacity beyond the inherited `SettingScan` handle, which mirrors the
  C `Relation rel` + `SysScanDesc scan` held across the find->mutate->finish
  sequence (relcache not ported). Opacity inherited, not introduced. OK.
- `setconfig text[]` crosses as decoded `Vec<String>` (repo-wide GUC-array
  convention). OK.
- No `todo!()`/`unimplemented!()`; no own-logic stubs; no ambient-global seams;
  no shared statics for per-backend state. OK.

## Fix applied during audit
`tests.rs` used module-level `static mut` recorders; under the Rust 2024
`static_mut_refs` strict UB check, `Vec::push` reallocation through a `&mut` to
the static aborted (SIGABRT) — the unit-test gate FAILED. Rewrote the recorders
onto a process-global `Mutex<TestState>` + a per-test `TEST_LOCK` serializer
(`begin()` / `cfg()` helpers), holding the `STATE` lock only in tightly-scoped
configure/record blocks so it is never held across a call into the code under
test. No logic of the port changed. All 12 decision-tree tests now pass.

## Gates
- `cargo check --workspace`: PASS (warnings only).
- `cargo test -p backend-catalog-pg-db-role-setting`: 12 passed.
- `cargo test -p seams-init`: 2 passed (recurrence_guard green at `audited`).

## Verdict: PASS
Every function MATCH (with legitimate SEAMED scan/catalog ops into unported
owners); zero seam findings; init_seams wired; gates green.
