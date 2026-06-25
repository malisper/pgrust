# Audit: backend-utils-misc-more

Catalog unit `backend-utils-misc-more`
(`*/pg_controldata.c, */ps_status.c, */rls.c, */superuser.c`, all under
`src/backend/utils/misc/`).

Crate: `crates/backend-utils-misc-more`. Audited independently against the C
(`../pgrust/postgres-18.3/src/backend/utils/misc/*.c`) and the c2rust rendering
(`../pgrust/c2rust-runs/backend-utils-misc-more/src/*.rs`).

## Function inventory + verdicts

### superuser.c → src/superuser.rs

| C function | Port | Verdict | Notes |
|---|---|---|---|
| `superuser(void)` | `superuser()` | MATCH | `superuser_arg(GetUserId())`; GetUserId via miscinit seam. |
| `superuser_arg(Oid)` | `superuser_arg()` | MATCH | cache-hit quick-out (`OidIsValid(last)&&last==roleid`); `!IsUnderPostmaster && roleid==BOOTSTRAP_SUPERUSERID` escape; `SearchSysCache1(AUTHOID)` → rolsuper, `!HeapTupleIsValid`→false; lazy `CacheRegisterSyscacheCallback(AUTHOID, RoleidCallback, 0)`; cache fill. |
| `RoleidCallback(Datum,int,uint32)` | `RoleidCallback()` | MATCH | sets `last_roleid = InvalidOid`; signature mirrors `SyscacheCallbackFunction`. |

Statics `last_roleid`/`last_roleid_is_super`/`roleid_callback_registered` are
per-backend → `thread_local!` (AGENTS.md backend-global rule). MATCH.

### rls.c → src/rls.rs

| C function | Port | Verdict | Notes |
|---|---|---|---|
| `check_enable_rls(Oid,Oid,bool)` | `check_enable_rls()` | MATCH | control flow verified line-by-line against c2rust: FirstNormalObjectId guard; RELOID lookup (miss→RLS_NONE); `!relrowsecurity`→RLS_NONE; `has_bypassrls_privilege`→RLS_NONE_ENV; `amowner && (!relforcerowsecurity || InNoForceRLSOperation())`→RLS_NONE_ENV; `!row_security && !noError`→ereport(ERROR, ERRCODE_INSUFFICIENT_PRIVILEGE) with the owner errhint; else RLS_ENABLED. |
| `row_security_active(PG_FUNCTION_ARGS)` | `row_security_active()` | MATCH | `check_enable_rls(oid, InvalidOid, true) == RLS_ENABLED`. |
| `row_security_active_name(PG_FUNCTION_ARGS)` | `row_security_active_name()` | MATCH | name→oid via the namespace seam mirroring `RangeVarGetRelid(makeRangeVarFromNameList(textToQualifiedNameList(name)), NoLock, missing_ok=false)`; missing relation raises ERRCODE_UNDEFINED_TABLE inside the owner. |

`CheckEnableRlsResult` enum values verified vs `utils/rls.h` (RLS_NONE=0,
RLS_NONE_ENV=1, RLS_ENABLED=2). The SQLSTATE/severity and the
owner-conditional errhint string match the C verbatim.

### ps_status.c → src/ps_status.rs

| C function | Port | Verdict | Notes |
|---|---|---|---|
| `save_ps_display_args(int,char**)` | `save_ps_display_args()` | MATCH (platform-trimmed) | records the title-buffer bound. The CLOBBER_ARGV argv/environ relocation needs the raw `argv` `main()` holds, which this crate is not handed; documented in the module header as the one platform-machinery divergence. The buffer-size datum the rest of the logic consumes is preserved. |
| `init_ps_display(const char*)` | `init_ps_display()` | MATCH | `MyBackendType`/`GetBackendTypeDesc` fallback; `!IsUnderPostmaster`/`!saved_args` early returns; `postgres: [cluster: ]fixed_part ` prefix (CLOBBER_ARGV platform → "postgres: " present); force-update via save/restore of update_process_title and `set_ps_display_with_len("",0)`. |
| `update_ps_display_precheck(void)` | `update_ps_display_precheck()` | MATCH | `update_process_title` / `IsUnderPostmaster` / buffer-present gates. |
| `set_ps_display_suffix(const char*)` | `set_ps_display_suffix()` | MATCH | overwrite-existing-suffix via nosuffix_len; the `cur_len+len+1>=size` not-enough-space branch with the `cur_len<size-1` guard and partial fill; flush. |
| `set_ps_display_remove_suffix(void)` | `set_ps_display_remove_suffix()` | MATCH | restores nosuffix_len, clears it; flush. |
| `set_ps_display(const char*)` | `set_ps_display()` | MATCH | `set_ps_display_with_len(activity, strlen)`. |
| `set_ps_display_with_len(const char*,size_t)` | `set_ps_display_with_len()` | MATCH | wipe suffix; rebuild fixed-prefix + activity with bounded truncation; flush. |
| `flush_ps_display(void)` | `flush_ps_display()` + `os_set_proc_title` | MATCH (platform-trimmed) | BSD `setproctitle`; CLOBBER_ARGV transmission is the documented no-op (tracked buffer authoritative). The CLOBBER_ARGV padding clobber of stale bytes is irrelevant once the buffer is an owned String. |
| `get_ps_display(int*)` | `get_ps_display()` | MATCH | returns `(activity, displen)` = buffer minus fixed prefix and its length. |

`PS_PADDING`/WIN32/SETPROCTITLE_FAST branches are outside the build config on
the target platforms; not applicable.

### pg_controldata.c → src/pg_controldata.rs

| C function | Port | Verdict | Notes |
|---|---|---|---|
| `pg_control_system(PG_FUNCTION_ARGS)` | `pg_control_system()` | MATCH | reads controlfile + CRC check; projects pg_control_version, catalog_version_no, system_identifier, time→timestamptz. |
| `pg_control_checkpoint(PG_FUNCTION_ARGS)` | `pg_control_checkpoint()` | MATCH | XLByteToSeg + XLogFileName (xlog_internal.h macros expanded inline) over checkPointCopy.redo/ThisTimeLineID; `"%u:%u"` epoch:xid of nextXid; all 18 fields projected in order. |
| `pg_control_recovery(PG_FUNCTION_ARGS)` | `pg_control_recovery()` | MATCH | minRecoveryPoint/TLI, backupStart/End, backupEndRequired. |
| `pg_control_init(PG_FUNCTION_ARGS)` | `pg_control_init()` | MATCH | all 12 init fields including data_checksum_version and default_char_signedness. |

`get_call_result_type`/`heap_form_tuple` tuple-forming is fmgr glue (TupleDesc
from fcinfo, not modeled here); the functions return owned projection structs,
the C `Datum[]`/`bool[]` analog. The substantive logic (read, CRC, WAL filename,
epoch:xid format, field selection) is all present. `time_t_to_timestamptz`
epoch offset (10957 days) and `XLogSegmentsPerXLogId` (0x100000000/segsz)
constants verified against the C macros.

## Seam audit

Owned inward seam crates (C-source coverage): `backend-utils-misc-more-seams`
(ps_status.c `init_ps_display`) and `backend-utils-misc-ps-status-seams`
(ps_status.c suffix/remove-suffix/update_process_title). Every declaration in
both is installed by `init_seams()`:

- `init_ps_display` → `crate::ps_status::init_ps_display` (infallible surface;
  prefix-OOM aborts, matching the C assert-only contract).
- `set_ps_display_suffix`, `set_ps_display_remove_suffix`,
  `update_process_title` → the matching `ps_status` fns.
- `update_process_title` GUC variable storage installed via `GucVarAccessors`
  (this crate owns that C global).

`init_seams()` contains only `set()`/`install()` calls — no logic. Wired into
`seams-init::init_all()`. PASS.

Outward seams — each justified by a real cycle to an unported or
heavier-layer owner, thin marshal + delegate only:

- `backend_utils_init_miscinit_seams::{get_user_id, in_no_force_rls_operation,
  get_backend_type_desc}` (miscinit.c, unported).
- `backend_utils_cache_syscache_seams::{search_authid_rolsuper,
  search_relation_rls_flags}` (syscache projections, established pattern).
- `backend_utils_cache_inval_seams::cache_register_syscache_callback`,
  `backend_utils_cache_lsyscache_seams::get_rel_name`,
  `backend_utils_adt_acl_seams::has_bypassrls_privilege`,
  `backend_catalog_aclchk_seams::object_ownercheck`,
  `backend_catalog_namespace_seams::range_var_get_relid_from_text`,
  `backend_access_transam_xlog_seams::wal_segment_size`,
  `common_controldata_utils_seams::get_controlfile`.

Direct deps (no cycle): `backend-utils-init-small` (`IsUnderPostmaster`,
`DataDir`), `backend-utils-misc-guc-tables` (`row_security`/`cluster_name`/
`update_process_title` GUC slots), `backend-utils-init-small-seams`
(`my_backend_type`).

## Design conformance

- Allocating fns take `Mcx<'mcx>` + return `PgResult` (pg_controldata WAL
  filename / epoch:xid strings, rls error-message rel-name copy). PASS.
- No invented opacity: real `ControlFileData`/`CheckPoint` from `types-control`;
  `CheckEnableRlsResult` is a real `#[repr(i32)]` enum (not an int alias).
- Per-backend statics → `thread_local!` (superuser cache, ps state, update flag).
- No ambient-global seams: `wal_segment_size`/`in_no_force_rls_operation` are
  parameterless reads of the *owner's* own global (correct; not a foreign
  global modeled as a getter on a consumer).
- No lock held across `?`: the C `ControlFileLock` LWLock wraps the controlfile
  read; that lock lives inside the controldata reader owner (the seam's failure
  surface reflects the read). No lock is acquired in this crate.
- FATAL/ERROR C sites → `Err(PgError)` with matching SQLSTATE.

## Verdict: PASS

Every function MATCH (two platform-trimmed MATCHes in ps_status with the
CLOBBER_ARGV transmission divergence documented in the module header and here —
the substantive buffer logic is complete). Seams installed and thin; design
rules satisfied.
