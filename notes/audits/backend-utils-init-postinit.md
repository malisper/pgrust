# Audit: backend-utils-init-postinit

- Unit: `backend-utils-init-postinit`
- Branch: `port/backend-utils-init-postinit`
- C source: `src/backend/utils/init/postinit.c` (PostgreSQL 18.3, 1450 lines)
- c2rust reference: `../pgrust/c2rust-runs/backend-utils-init-postinit/src/postinit.rs`
- Port: `crates/backend-utils-init-postinit/src/lib.rs`
- Owned seam crate: `crates/backend-utils-init-postinit-seams/src/lib.rs`
- Date: 2026-06-13 (sync reconciliation re-audit; original audit 2026-06-12)
- Model: claude-opus-4-8[1m]
- **Verdict: PASS**

Independent re-audit, re-derived from the C + c2rust + headers; it does not
trust the port's prior committed audit or its self-review. This round both
(a) confirms the previously-failing S1 finding is resolved and (b) found and
fixed a new merge-blocking seam-wiring finding (S2) the prior audit missed.

## 1. Function inventory

postinit.c defines exactly 20 functions (the static prototypes at C 73-87 plus
the non-static `pg_split_opts`, `InitializeMaxBackends`,
`InitializeFastPathLocks`, `BaseInit`, `InitPostgres`). The c2rust run renders
all 20 (no extra statics/inline helpers in this TU). All 20 enumerated below;
none missing.

## 2. Constants verified against headers (not from memory)

| Constant | Header / .dat | Value | Port | OK |
|---|---|---|---|---|
| `Template1DbOid` | pg_database.dat oid=1 | 1 | `TEMPLATE1_DB_OID=1` | yes |
| `DEFAULTTABLESPACE_OID` | pg_tablespace.dat oid=1663 | 1663 | 1663 | yes |
| `ROLE_PG_USE_RESERVED_CONNECTIONS` | pg_authid.dat oid=4550 | 4550 | 4550 | yes |
| `DatabaseRelationId` | pg_database (1262) | 1262 | `DATABASE_RELATION_ID=1262` | yes |
| `MAX_BACKENDS_BITS`/`MAX_BACKENDS` | procnumber.h:38-39 | 18 / (1<<18)-1 | 18 / (1<<18)-1 | yes |
| `NUM_SPECIAL_WORKER_PROCS` | proc.h:448 | 2 | 2 | yes |
| `FP_LOCK_GROUPS_PER_BACKEND_MAX` | proc.h:97 | 1024 | 1024 | yes |
| `FP_LOCK_SLOTS_PER_GROUP` | proc.h:98 | 16 | 16 | yes |
| `INIT_PG_LOAD_SESSION_LIBS/OVERRIDE_ALLOW_CONNS/OVERRIDE_ROLE_LOGIN` | miscadmin.h | 0x1/0x2/0x4 | 0x1/0x2/0x4 | yes |
| `COLLPROVIDER_LIBC` | pg_collation_d.h | 'c' | `b'c' as i8` | yes |

Build config (from c2rust): `USE_SSL` **is** compiled (the `ssl_in_use` /
`be_tls_get_*` block at c2rust line 4069 is present; `port->ssl_in_use` is a
live struct field, deref'd at 4069); `ENABLE_GSS` is **not** (the `gss` field
exists on `Port` but no `port->gss` block is emitted); `EXEC_BACKEND` not set;
`INJECTION_POINT` compiles to nothing.

## 3. Function-by-function

| C function | C lines | Port | Verdict | Notes |
|---|---|---|---|---|
| `GetDatabaseTuple` | 104-142 | `GetDatabaseTuple` | SEAMED | table_open+systable scan by datname+heap_copytuple+close -> `pg-database-seams::get_database_tuple_by_name`, returns decoded `FormPgDatabase`. Thin delegate. OK. |
| `GetDatabaseTupleByOid` | 147-185 | `GetDatabaseTupleByOid` | SEAMED | same by oid -> `get_database_tuple_by_oid`. OK. |
| `PerformAuthentication` | 193-316 | `PerformAuthentication`+`build_auth_logmsg` | MATCH | ClientAuthInProgress set/clear, conn_timing auth_start/end, enable/disable STATEMENT_TIMEOUT (`AuthenticationTimeout*1000`), ClientAuthentication, set_ps_display("authentication"/"startup"), log gating on `log_connections & LOG_CONNECTION_AUTHORIZATION` all present. logmsg assembly mirrors C 268-287: walsender vs normal "authorized: user=" prefix, " database=" iff !am_walsender, " application_name=" iff non-NULL, and the **in-crate** `#ifdef USE_SSL` `if port.ssl_in_use` branch assembling `" SSL enabled (protocol=%s, cipher=%s, bits=%d)"` from the three seamed `be_tls_get_*` accessors (see S1, now resolved). EXEC_BACKEND/GSS `#ifdef`-folded (correct). |
| `CheckMyDatabase` | 322-483 | `CheckMyDatabase` | MATCH | SearchSysCache1 / GETSTRUCT / SysCacheGetAttr* crossed as decoded `FormPgDatabase`; recheck strcmp, IsUnderPostmaster gate, datallowconn/aclcheck/connlimit order preserved (object_aclcheck after am_superuser/override matches C 372-374); encoding GUCs; locale setlocale FATALs; ctype C/POSIX -> database_ctype_is_c; init_database_collation; collation-version WARNING path with datlocprovider==LIBC branch and datlocale-null ERROR all present; SQLSTATEs/severities match. |
| `pg_split_opts` | 496-541 | `pg_split_opts` | MATCH | byte parser: leading-space skip, `\0` break, escape state machine (`\\`), append-on-non-escape, store-per-option; isspace classification matches C locale set; argcp lock-step. Idiomatic StringInfo->PgString, argv->Vec. Behavior identical. |
| `InitializeMaxBackends` | 554-571 | `InitializeMaxBackends` | MATCH | sum = MaxConnections+av_worker_slots+max_worker_processes+max_wal_senders+NUM_SPECIAL_WORKER_PROCS; ERROR/ERRCODE_INVALID_PARAMETER_VALUE on `> MAX_BACKENDS`; errdetail uses `MAX_BACKENDS-(NUM_SPECIAL_WORKER_PROCS-1)`. Assert->debug_assert. Installed as inward seam (see S2). |
| `InitializeFastPathLocks` | 580-601 | `InitializeFastPathLocks` | MATCH | `Max(Min(pg_nextpower2_32(max_locks_per_xact)/FP_LOCK_SLOTS_PER_GROUP, FP_LOCK_GROUPS_PER_BACKEND_MAX),1)`; pg_nextpower2_32 reimpl verified (already-pow2 returns num; else `1<<(leftmost+1)` where leftmost=31-leading_zeros) — spot-checked against C `pg_bitutils.h`; power-of-two post-assert. Installed as inward seam (see S2). |
| `BaseInit` | 611-666 | `BaseInit` | MATCH | exact init sequence: DebugFileOpen, InitFileAccess, pgstat_initialize, pgaio_init_backend, InitSync (create-pending-ops predicate `!IsUnderPostmaster||AmCheckpointer` matches sync.c), smgrinit, InitBufferManagerAccess, InitTemporaryFileAccess, InitXLogInsert, InitLockManagerAccess, ReplicationSlotInitialize — same order, each via owner seam. Installed as inward seam (see S2). |
| `InitPostgres` | 711-1237 | `InitPostgres` | MATCH | Full orchestration verified end-to-end: InitProcessPhase2; pgstat_beinit; bootstrap-gated pgstat_bestart_initial; SharedInvalBackendInit(false); ProcSignalInit(cancel key); 8 RegisterTimeout (incl. CheckDeadLockAlert wrapper) gated on !bootstrap; !IsUnderPostmaster XLOG block (CreateAuxProcessResourceOwner/StartupXLOG/Release(true)/reset/before_shmem_exit x2 in C order); RelationCacheInitialize/InitCatalogCache/InitPlanCache; EnablePortalManager; phase2; before_shmem_exit(ShutdownPostgres); AV-launcher early return; transaction start (set ts + StartTransactionCommand + read-committed); 5-arm user-id/superuser ladder; pgstat_bestart_security; binary-upgrade FATAL; reserved-slot logic (HaveNFreeProcs, nfree<su_reserved, has_privs_of_role); walsender rolreplication FATAL; physical-walsender early-return; dboid resolution (bootstrap shortcut / in_dbname / !OidIsValid bgworker return); LockSharedObject RowExclusiveLock; recheck-by-oid + name mismatch short-circuit (C 1074), strlcpy dbname, database_is_invalid_form FATAL, set tablespace/login-evt/out_dbname; set MyDatabaseId/MyProc.databaseId; InvalidateCatalogSnapshot; GetDatabasePath + access(F_OK) FATALs gated !bootstrap; ValidatePgVersion; SetDatabasePath; phase3; initialize_acl; CheckMyDatabase; process_startup_options; process_settings; PostAuthDelay; InitializeSearchPath; InitializeClientEncoding; InitializeSession; preload libs on flag; pgstat_bestart_final/CommitTransactionCommand gated !bootstrap. All branch predicates, gates, SQLSTATEs match. Bootstrap-mode call installed as inward seam (see S2). |
| `process_startup_options` | 1243-1300 | `process_startup_options` | MATCH | gucctx SU_BACKEND/BACKEND; cmdline maxac=2+(len+1)/2; av[0]="postgres"; pg_split_opts; `ac<maxac` assert; process_postgres_switches; guc_options pair iteration (name,value) -> SetConfigOption(PGC_S_CLIENT). NULL-terminator handled by slice length (idiomatic). |
| `process_settings` | 1308-1330 | `process_settings` | SEAMED | !IsUnderPostmaster early return kept in-crate; the table_open+RegisterSnapshot(GetCatalogSnapshot)+4xApplySetting(scope order)+Unregister+close batched into `pg-db-role-setting-seams::apply_db_role_settings` (genam batched-scan precedent). OK. |
| `ShutdownPostgres` | 1342-1353 | `ShutdownPostgres` | MATCH | AbortOutOfAnyTransaction; LockReleaseAll(USER_LOCKMETHOD,true). |
| `StatementTimeoutHandler` | 1359-1376 | `StatementTimeoutHandler` | MATCH | SIGINT default, SIGTERM if ClientAuthInProgress; HAVE_SETSID kill(-pid)+kill(pid). |
| `LockTimeoutHandler` | 1381-1389 | `LockTimeoutHandler` | MATCH | kill(-pid,SIGINT)+kill(pid,SIGINT). |
| `TransactionTimeoutHandler` | 1391-1397 | `TransactionTimeoutHandler` | MATCH | set pending + InterruptPending + SetLatch(MyLatch). |
| `IdleInTransactionSessionTimeoutHandler` | 1399-1405 | same | MATCH | flag+interrupt+latch. |
| `IdleSessionTimeoutHandler` | 1407-1413 | same | MATCH | flag+interrupt+latch. |
| `IdleStatsUpdateTimeoutHandler` | 1415-1421 | same | MATCH | IdleStatsUpdateTimeoutPending+interrupt+latch. |
| `ClientCheckTimeoutHandler` | 1423-1429 | same | MATCH | CheckClientConnectionPending+interrupt+latch. |
| `ThereIsAtLeastOneRole` | 1434-1450 | `ThereIsAtLeastOneRole` | SEAMED | table_open(AuthIdRelationId)+beginscan_catalog+heap_getnext!=NULL+endscan+close batched into `pg-authid-seams::there_is_at_least_one_role` (returns bool). Owner's scan machinery. OK. |

## 4. Seam audit

**Ownership by C-source coverage.** The unit's only C file is `postinit.c`, so
its owned seam crate is `crates/backend-utils-init-postinit-seams` (the prior
audit incorrectly claimed no such crate existed — that was a false statement
and the source of S2). That crate declares four **inward** seams, all of which
are postinit's own functions called back into by `backend-bootstrap-bootstrap`
(`BootstrapModeMain` drives the per-backend init steps — a real dependency
cycle): `initialize_max_backends`, `initialize_fast_path_locks`, `base_init`,
`init_postgres_bootstrap`.

Per audit-crate step 3, **every declaration in every owned seam crate must be
installed by the crate's `init_seams()`**; an empty installer with owned seam
crates outstanding is an automatic FAIL. After the S2 fix, `init_seams()`
installs all four with `set()` calls (and nothing else), and
`seams-init::init_all()` calls `backend_utils_init_postinit::init_seams()`
(seams-init/src/lib.rs:64). Each installed closure is a thin delegate to the
in-crate function (no branching/computation): three forward directly
(`InitializeMaxBackends`/`BaseInit`, and `InitializeFastPathLocks` unwrapped to
honor its infallible C contract), and `init_postgres_bootstrap(mcx)` calls
`InitPostgres(mcx, None, InvalidOid, None, InvalidOid, 0, None)` — the exact C
`InitPostgres(NULL, InvalidOid, NULL, InvalidOid, 0, NULL)`.

The crate also consumes ~37 seam crates owned by *other* units; each is a thin
marshal+delegate, with two acceptable batched-scan crossings
(`apply_db_role_settings`, `there_is_at_least_one_role`) and decoded-tuple
crossings (`get_database_tuple_*`, `search_database_syscache` returning
`FormPgDatabase`). `FormPgDatabase` (types-catalog) is a real decode of
`FormData_pg_database` columns, not invented opacity (types.md 6-7 OK).
Allocating/fallible seams take `Mcx` and return `PgResult`; no shared statics
for per-backend globals are introduced here.

### Finding S1 (was merge-blocking) — RESOLVED, confirmed this round

The earlier audit flagged that the live `if (port->ssl_in_use)` branch plus the
`" SSL enabled (protocol=%s, cipher=%s, bits=%d)"` format assembly (C 281-287)
had been relocated into a `be-secure-seams::transport_security_logfrag` seam —
postinit's own logic living outside the crate. **Confirmed resolved:**
`crates/backend-libpq-be-secure-seams/src/lib.rs` no longer declares
`transport_security_logfrag`; it declares only the three thin accessors
`be_tls_get_version`/`be_tls_get_cipher` (`-> PgResult<PgString>`) and
`be_tls_get_cipher_bits(port) -> i32` (the exact C accessors). The
`port.ssl_in_use` predicate and the `appendStringInfo` format assembly are now
in-crate in `build_auth_logmsg` (lib.rs 208-237), mirroring C 283-287; only the
SSL-state reads cross the seam. `PerformAuthentication` now MATCHes.

### Finding S2 (merge-blocking) — FOUND and FIXED this round

The owned seam crate `backend-utils-init-postinit-seams` declared four inward
seams consumed by `backend-bootstrap-bootstrap`, but the port's `init_seams()`
was the empty `pub fn init_seams() {}` (with a comment falsely asserting no
owned seam crate exists). Per audit-crate step 3 that is an automatic FAIL: the
four bootstrap-mode entry points would panic-on-call at runtime despite being
fully implemented in the crate.

Fix applied on this branch:
- `init_postgres_bootstrap` seam signature given a `mcx: Mcx<'static>` param
  (the bootstrap caller already holds the process-lifetime context); added the
  `mcx` dep to the seam crate's `Cargo.toml`.
- bootstrap call site updated to pass `mcx`
  (`backend-bootstrap-bootstrap/src/lib.rs:443`).
- postinit `init_seams()` rewritten to `set()` all four seams (thin delegates,
  no other logic); added the seam crate as a dep of the port crate. The stale
  comment was corrected.

Builds clean (`backend-utils-init-postinit{,-seams}`, `backend-bootstrap-bootstrap`,
`seams-init`); the crate's 5 unit tests pass.

## 5. Verdict

**PASS.** All 20 functions MATCH or SEAMED per step 3's rules; every audited
constant is correct (verified against headers/.dat, not memory). S1 (relocated
SSL log-fragment logic) is confirmed resolved at its root. S2 (uninstalled
owned inward seams — an automatic FAIL) was found this round and fixed on the
branch: `init_seams()` now installs all four declarations as thin delegates,
and the re-audited functions/wiring MATCH. CATALOG row may move to `audited`.
Not merged to main.

## Sync reconciliation re-audit (2026-06-13)

Merged current `refs/heads/main` into the branch. Five seam-vocabulary
collisions surfaced (the named one plus four that git auto-merged into
duplicate-definition compile errors / textual conflicts). All resolved by
faithful logic reconciliation, not mechanical union; `cargo check --workspace`
and `cargo test --workspace` both pass clean (1087 test groups ok, 0 failures).

### S3 — named collision: `set_database_path` (miscinit-seams) — RESOLVED

main (from cache-inval) declared `set_database_path(path: &str)` INFALLIBLE —
the inval.c recovery quick-hack that pokes the `DatabasePath` global directly
(paired with `clear_database_path`). postinit needed the real
`SetDatabasePath()` (palloc into TopMemoryContext, FALLIBLE). These are two
distinct C functions; collapsing them would violate
seam-signatures-mirror-c-failure-surface (one infallible, one `PgResult`).

Fix: main's name/shape is authoritative for the inval hack — kept
`set_database_path(path: &str)`. Added a distinct seam
`set_database_path_once(path: &str) -> PgResult<()>` for postinit's real
`SetDatabasePath` (the "one-shot for normal backends" setter), and rewired
postinit's lone consumer (lib.rs:1029) to it. inval's consumer is unchanged.
Two C functions, two seams. No invented decomposition: each seam is a thin
1:1 mirror of its true C function.

### S4 — `invalidate_catalog_snapshot` (snapmgr-seams) — RESOLVED

Both sides declared the *same* C function `InvalidateCatalogSnapshot()` with
the identical infallible signature (postinit added a postinit-consumer copy;
main carried the cache-inval one). This is one C function, not two — collapsed
to main's single declaration; both consumers (postinit + inval) now share it.
Not a decomposition invention; the opposite would be the error.

### S5–S7 — additive seam-block conflicts — RESOLVED (union)

Three owned-by-neighbor seam crates had textual conflicts where each side
appended a *distinct, non-overlapping* seam to the same region:

- `backend-storage-smgr-seams`: HEAD `smgrinit()` + main `smgrdestroyall()`.
- `backend-storage-buffer-bufmgr-seams`: HEAD `init_buffer_manager_access()` +
  main's xlog-replay buffer primitives block.
- `backend-storage-file-fd-seams`: HEAD `init_file_access` /
  `init_temporary_file_access` / `access_f_ok` (+`AccessResult`) + main
  `basic_open_file`.
- `backend-utils-cache-relcache-seams`: HEAD the three
  `relation_cache_initialize*` phases + main `create_fake_relcache_entry` /
  `free_fake_relcache_entry`.

Each resolved by keeping both sides' declarations (no name/signature overlap;
no shared-vocabulary divergence). Signatures unchanged from each origin.

### Verdict

PASS holds. The reconciliation touched only seam declarations and postinit's
two consumer call-sites; no ported logic changed. Owned postinit seams (4
declared / 4 installed in `init_seams`) remain intact post-merge. Not merged
to main, not pushed.
