# Audit: backend-timezone-pgtz (`src/timezone/pgtz.c`)

Independent function-by-function audit against `../pgrust/postgres-18.3/src/timezone/pgtz.c`
and the c2rust rendering `../pgrust/c2rust-runs/backend-timezone-pgtz/src/pgtz.rs`.

Port: `crates/backend-timezone-pgtz/src/lib.rs`,
seams: `crates/backend-timezone-pgtz-seams/src/lib.rs`.

## Function inventory (every C definition)

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `pg_TZDIR` (42, static) | `pg_tzdir` lib.rs:42 | MATCH | non-SYSTEMTZDIR branch (the build config). `get_share_path(my_exec_path, tzdir)` then append `"/timezone"`; memoized via `thread_local TZDIR` (C `static done_tzdir`/`tzdir[MAXPGPATH]` per-backend). `get_share_path` SEAMED to `common-path-seams` (path.c unported, panics until owner) — justified cross-unit callee. MAXPGPATH truncation preserved. |
| `pg_open_tzfile` (76) | `pg_open_tzfile` lib.rs:75 | MATCH | `want_canonical` mirrors non-NULL `canonname`. Length guard `orignamelen+1+name.len() >= MAXPGPATH` → None (C `-1`). as-is fast open only when `!want_canonical`. Directory-level split loop on `/` calls `scan_directory_ci`; on miss returns None. Canon copy truncated to `TZ_STRLEN_MAX` (C `strlcpy(..,TZ_STRLEN_MAX+1)`). Final `open(O_RDONLY|PG_BINARY)` → `File::open` (PG_BINARY no-op on unix). Never ereports. |
| `scan_directory_ci` (151, static) | `scan_directory_ci` lib.rs:141 | MATCH | LOG-severity dir read via `backend_storage_file_fd_seams::read_dir_names_logged` (C `ReadDirExtended(.., LOG)` — failures logged+skipped). Skips leading-`.` entries (security). Case-insensitive match by exact length + `pg_strncasecmp` (direct dep `port-pgstrcasecmp`). Returns the actual entry name (C `strlcpy canonname`). |
| `init_timezone_hashtable` (201, static) | folded into `TIMEZONE_CACHE` thread_local | MATCH | C lazy `if(!timezone_cache) hash_create(...)`; here an always-present empty `HashMap<String,Rc<pg_tz>>` keyed by uppercased name (dynahash HTAB analog, `HASH_STRINGS` case-sensitive key = pre-uppercased). hash_create cannot fail in practice; the `return false`/`return NULL` propagation is vacuous. |
| `pg_tzset` (234) | `pg_tzset` lib.rs:177 | MATCH | `len>TZ_STRLEN_MAX`→None. Uppercase via `pg_toupper`. Cache HASH_FIND. `"GMT"`→`tzparse(.,.,true)`, on fail `elog(ERROR,"could not initialize GMT time zone")`→`PgError::error` (XX000). Else `tzload`; on **any** error → `uppername[0]==':' || !tzparse(.,.,false)` → None, else POSIX canonical = uppername. `Err(_)` covers ENOENT/EINVAL/ENOMEM identically to C `!= 0`. Cache insert keyed by uppername; value `Rc<pg_tz>` (C shares `&tzp->tz`). |
| `pg_tzset_offset` (320) | `pg_tzset_offset` lib.rs:239 | MATCH | `absoffset=|gmtoffset|`. `"%02ld"`→`{:02}` of `/SECS_PER_HOUR`, then conditional `:MM`, then conditional `:SS`. `gmtoffset>0`→`<-off>+off`, else `<+off>-off`. SECS_PER_HOUR=3600 / SECS_PER_MINUTE=60 (timestamp.h). Delegates to `pg_tzset`. |
| `pg_timezone_initialize` (361) | `pg_timezone_initialize` lib.rs:271 | MATCH | `pg_tzset("GMT")` → set `session_timezone` then `log_timezone` (both via `state_pgtz` setters; globals live there per CATALOG). None case `.expect` = loud failure (C derefs NULL; cannot occur for GMT). |
| `pg_tzenumerate_start` (396) | `pg_tzenumerate_start` lib.rs:301 | MATCH | `baselen=strlen(startdir)+1`, depth=0, `AllocateDir(startdir)` via `read_dir_names` (ERROR severity — C `if(!dirdesc) ereport(ERROR, file_access)`). Materializes entry names up front (DIR* OS edge owned by fd seam); behavior-preserving. |
| `pg_tzenumerate_next` (426) | `pg_tzenumerate_next` lib.rs:319 | MATCH | depth-stack walk; end-of-dir pops + depth--. Skip leading-`.`. `get_dirent_type==PGFILETYPE_DIR(3)`→recurse, depth overflow at `MAX_TZDIR_DEPTH-1`(=9) → `errmsg_internal("timezone directory stack overflow")`. Leaf: `tzload(relname,.,false,true)`; **any** error → continue (fixed: now matches C `!= 0 → continue`, see Findings). `pg_tz_acceptable` reject (leap zones) → continue. TZname set from `relname` (C `strlcpy` after the acceptable check; reordering is harmless — `pg_tz_acceptable` reads only `tz.state`). |
| `pg_tzenumerate_end` (414) | `pg_tzenumerate_end` lib.rs:394 | MATCH | C `FreeDir`+`pfree` per depth then `pfree(dir)`; here `PgTzEnum` owns all data, `drop` frees. API-parity wrapper. |

## Seam audit

Owned seam crate (by C-source coverage = pgtz.c): `backend-timezone-pgtz-seams`.
- `pg_open_tzfile` — declared here, **owned by pgtz**, installed in
  `backend-timezone-pgtz::init_seams()` (lib.rs:411). Correct.
- `pg_localtime` — declared in this seam crate but **owned by localtime**
  (localtime.c), installed by `backend-timezone-localtime::init_seams()`
  (per CATALOG: intentional cross-homing). Not pgtz's to install. Correct.

`init_seams()` contains only `set()` calls; wired into
`seams-init::init_all()` (lib.rs:158, after localtime:157).
`recurrence_guard` both checks pass.

Outward seam calls are all thin marshal+delegate into real cross-unit callees:
`common_path_seams::get_share_path` (path.c unported), the
`backend_storage_file_fd_seams` directory edges (fd.c), and the direct dep on
`backend-timezone-localtime` (`tzload`/`tzparse`/`pg_tz_acceptable` — no cycle,
localtime deps only the seam crate). No branching/computation hidden in seams.

## Findings (fixed this round)

1. **`pg_tzenumerate_next` OOM divergence (DIVERGES → fixed → MATCH).**
   The port special-cased `TzLoadError::OutOfMemory` from `tzload` and raised
   `PgError::error("out of memory")`, whereas C ignores the zone on **any**
   nonzero `tzload` errno (`if (tzload(...) != 0) continue;`), ENOMEM included.
   Fixed to `if tzload(...).is_err() { continue; }`, matching C exactly and the
   sibling `pg_tzset` handling (`Err(_)` → fall through). Removed now-unused
   `TzLoadError` import.

No own-logic stubs, no `todo!()`/`unimplemented!()`, no deferred/SEAMED-equivalent
escape of in-crate logic.

## Gates

- `cargo check --workspace`: clean (only pre-existing unrelated warnings in
  `backend-access-common-printtup`).
- `cargo test -p backend-timezone-pgtz`: pass.
- `cargo test -p seams-init`: pass (both recurrence_guard tests green).

## Verdict: PASS
