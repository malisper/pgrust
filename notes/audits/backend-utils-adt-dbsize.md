# Audit: backend-utils-adt-dbsize

C source: `src/backend/utils/adt/dbsize.c` (postgres-18.3, 1046 LOC).
Crate: `crates/backend-utils-adt-dbsize`. Owns no `*-seams` crate (declares its
outbound seams inline). Its `init_seams()` IS reached by `seams-init::init_all`
(it registers the fmgr builtins and installs all outbound seams to their owners).

## Re-audit 2026-06-21 (GRANT-tablespace lane: outbound-seam wiring)

The bodies were already PASS (table below). This pass added the missing
`init_seams()` installs so `pg_tablespace_size` / `pg_database_size` (and `\db+`)
actually execute: `read_dir` (local AllocateDir/ReadDir/FreeDir provider,
errno-capturing, peer to the existing `stat` provider), `tablespace_exists` /
`database_exists` (new syscache projection + seam → SearchSysCacheExists1 on
TABLESPACEOID/DATABASEOID), `get_tablespace_oid` / `get_database_oid`
(missing_ok=false), `object_aclcheck` (AclResult→bool), `aclcheck_error`
(re-derives the non-OK AclResult + looks up the object name, replicating C's
inline `aclcheck_error(aclresult, OBJECT_*, get_*_name(oid))` call site), and
`get_temp_namespace_proc_number`. All installs are thin marshal+delegate to the
owners' already-installed seams; no cycle introduced. Constants header-verified:
ACL_CREATE=1<<9, ACL_CONNECT=1<<11, DatabaseRelationId=1262,
TableSpaceRelationId=1213, ROLE_PG_READ_ALL_STATS=3375. `pg_tablespace_location`
is in tablespace.c (not this unit) and correctly absent. Runtime-verified on a
live cluster: pg_tablespace_size (name+oid, default/global/non-default),
pg_tablespace_location, pg_database_size, and `\db+` all return correct values.
Verdict: **PASS** (unchanged).

## Function-by-function coverage (all 27 functions present, full logic)

| C function | Rust | Notes |
|---|---|---|
| `db_dir_size` | `db_dir_size` | dir walk; `.`/`..` skip, ENOENT→continue, else stat-error. 1:1 |
| `calculate_database_size` | `calculate_database_size` | ACL (object_aclcheck \|\| pg_read_all_stats), base/%u, pg_tblspc scan WITHOUT NULL-check (replicates "could not open directory" on Failed). 1:1 |
| `pg_database_size_oid` | `pg_database_size_oid` | SearchSysCacheExists1→"database with OID %u does not exist"; size==0→NULL |
| `pg_database_size_name` | `pg_database_size_name` | get_database_oid; size==0→NULL |
| `calculate_tablespace_size` | `calculate_tablespace_size` | tblspc!=MyDatabaseTableSpace ACL; base/global/pg_tblspc path; -1→None; S_ISDIR recursion + own size |
| `pg_tablespace_size_oid` | `pg_tablespace_size_oid` | exists check; size<0→NULL |
| `pg_tablespace_size_name` | `pg_tablespace_size_name` | get_tablespace_oid |
| `calculate_relation_size` | `calculate_relation_size` | relpathbackend + segment loop (`%s` / `%s.%u`), ENOENT→break |
| `pg_relation_size` | `pg_relation_size` | try_relation_open→NULL on drop; forkname_to_number; relation_close |
| `calculate_toast_table_size` | `calculate_toast_table_size` | toast heap forks + RelationGetIndexList forks |
| `calculate_table_size` | `calculate_table_size` | heap forks + toast if reltoastrelid valid |
| `calculate_indexes_size` | `calculate_indexes_size` | relhasindex → index forks |
| `pg_table_size` | `pg_table_size` | |
| `pg_indexes_size` | `pg_indexes_size` | |
| `calculate_total_relation_size` | `calculate_total_relation_size` | table + indexes |
| `pg_total_relation_size` | `pg_total_relation_size` | |
| `pg_size_pretty` | `pg_size_pretty` | int64 unit loop, half_rounded, bit-divisor math. Pure, returns owned text |
| `numeric_to_cstring` | `numeric_to_cstring` | numeric_out (real) |
| `numeric_is_less` | `numeric_is_less` | numeric_lt (real) |
| `numeric_absolute` | `numeric_absolute` | numeric_abs (real) |
| `numeric_half_rounded` | `numeric_half_rounded` | int64_to_numeric 0/1/2 + ge/add/sub + div_trunc |
| `numeric_truncated_divide` | `numeric_truncated_divide` | int64_to_numeric divisor + div_trunc |
| `pg_size_pretty_numeric` | `pg_size_pretty_numeric` | numeric unit loop; psprintf → format!; shiftby math |
| `pg_size_bytes` | `pg_size_bytes` | full parser: sign/digits/decimal/exponent(strtol)/unit+alias case-insensitive; numeric_in + numeric_mul + numeric_int8 |
| `pg_relation_filenode` | `pg_relation_filenode` | RELKIND_HAS_STORAGE; relfilenode \|\| RelationMapOidToFilenumber; InvalidRelFileNumber→NULL |
| `pg_filenode_relation` | `pg_filenode_relation` | RelidByRelfilenumber |
| `pg_relation_filepath` | `pg_relation_filepath` | RelationInitPhysicalAddr logic; relpersistence backend dispatch (UNLOGGED/PERMANENT→INVALID, TEMP→namespace proc); elog "invalid relpersistence" |

`numeric_int8` (the SQL wrapper = `numeric_int8_opt_error(num, NULL)`) is ported
inline here (NaN/inf → ERRCODE_FEATURE_NOT_SUPPORTED "cannot convert X to bigint",
out-of-range → ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE "bigint out of range"), built on
the numeric crate's exported primitives `set_var_from_num` + `numericvar_to_int64`
since the crate does not re-export the fmgr-level `numeric_int8`.

## Reconciliation to the repo model

- src-idiomatic's central `seams::backend_utils_adt_dbsize` and toy
  `Numeric{text}` carrier / `MemoryContext`+`PgString` are dropped.
- Numeric arithmetic uses the REAL ported `backend-utils-adt-numeric` directly
  (no cycle; numeric is a leaf), over the packed on-disk image as `&[u8]` /
  `PgVec<'mcx,u8>`. The fmgr cores take a real `mcx::Mcx<'mcx>`.
- `relpathbackend` / `forkname_to_number` call the ported `backend-common-relpath`
  directly (`RelFileLocator`, `ProcNumber`, `ForkNumber` from types-core/-storage).
- The runtime filesystem dir-walk (`read_dir`, `stat`), `check_for_interrupts`,
  and the catalog/acl/syscache/relcache/relmapper/namespace touchpoints are
  declared as inline `seam_core::seam!` OUTBOUND seams (carriers FileStat /
  DirEntry / OpenDir / StatResult / AclObjectType / PgClassForm / OpenRelation
  live in-crate). They are NOT in a `*-seams` crate, so they do not trip the
  recurrence guard, and panic loudly until a runtime/owner installs them — the
  same mirror-PG-and-panic convention `backend-utils-adt-numeric` uses for its
  SRF/planner-support OUTBOUND frames. Nothing here is a silent fallback.

## Scope notes

- `pg_relation_is_publishable` (named in the task) is NOT in dbsize.c — it lives
  in `pg_publication.c`; out of this file's scope.
- Bare-word PGFunction registry (`numeric_out`/`numeric_lt`/... dispatch via
  fmgr OID) is deferred per task; the C `DirectFunctionCall*` round-trips are
  modelled as direct calls into the numeric crate.

## Divergences (sanctioned)

1. `Datum`-returning fmgr fns that `PG_RETURN_NULL` → `PgResult<Option<_>>`;
   `PG_GETARG_*`/detoast is the fmgr layer's job (project convention).
2. `pg_size_pretty` / `pg_size_pretty_numeric` / `pg_relation_filepath` return
   owned `String` instead of `cstring_to_text` text (the fmgr layer wraps it).
3. ereport(ERROR) → `Err(PgError)` via the error spine; `%m` and
   `errcode_for_file_access()` preserved through `with_saved_errno`.

## Tests

8 unit tests pass: pg_size_pretty int64 golden vectors; pg_size_bytes unit/alias/
exponent/sign cases + invalid; pg_size_pretty_numeric golden (real numeric);
db_dir_size walk + missing-dir + stat-error; database_size ACL-denied; and the
base+tablespace sum. Filesystem/catalog seams installed with mocks in the test
binary; numeric is the real crate.
