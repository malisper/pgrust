# Audit: backend-utils-misc-ps-status

**Verdict: PASS**
**Date: 2026-06-13**
**Model: claude-opus-4-8[1m]**

Catalog unit `backend-utils-misc-ps-status` — C source
`src/backend/utils/misc/ps_status.c`. The unit ships inside the combined crate
`crates/backend-utils-misc-more` (module `src/ps_status.rs`); this audit covers
only the `ps_status.c` surface and the seam crates that map to it.

Audited independently against:
- C: `../pgrust/postgres-18.3/src/backend/utils/misc/ps_status.c`
- c2rust: `../pgrust/c2rust-runs/backend-utils-misc-more/src/ps_status.rs`
- Port: `crates/backend-utils-misc-more/src/ps_status.rs`

## Build config

The c2rust run is the darwin build, so the live preprocessor branches are
`PS_USE_CLOBBER_ARGV` (Linux/macOS/Solaris) and `PS_PADDING == '\0'`. The
`PS_USE_NONE`, `PS_USE_SETPROCTITLE[_FAST]`, and `PS_USE_WIN32` branches are not
in the build and are reproduced by the port only as the documented `cfg`
fallbacks in `os_set_proc_title`. `DEFAULT_UPDATE_PROCESS_TITLE` is `true` on
non-Windows (`ps_status.h:19`); the port uses `true`. Verified, not transcribed
from memory.

## Function inventory + verdicts

| C function (line) | Port | Verdict | Notes |
|---|---|---|---|
| `save_ps_display_args(int, char**)` (119) | `save_ps_display_args(&[impl AsRef<str>])` | MATCH (model) | The contiguous-argv/environ scan, environ relocation, argv copy, and `_NSGetArgv()` fix are the `PS_USE_CLOBBER_ARGV` platform machinery — physically clobbering the argv region — which the port deliberately does not reproduce (documented in the module header). The portable datum the buffer algorithm consumes is `ps_buffer_size`; the port records `sum(arg.len()+1)` and sets `saved_args=true` (the `save_argv != NULL` proxy). See "Platform machinery" below — the buffer-sizing approximation is the one ledgered divergence and is intrinsic to not owning raw argv. |
| `init_ps_display(const char*)` (269) | `init_ps_display(Option<&str>)` | MATCH | `fixed_part==NULL` → `GetBackendTypeDesc(MyBackendType)` (seamed to miscinit + init-small); `!IsUnderPostmaster` early return; `!save_argv`→`!saved_args` early return; prefix is `"postgres: %s "` (no cluster) or `"postgres: %s: %s "` (cluster set), matching the CLOBBER_ARGV `PROGRAM_NAME_PREFIX "postgres: "`; `cluster_name` read via guc-tables var accessor; `fixed_size = strlen(prefix)`; force-update idiom (save title GUC, set true, `set_ps_display("")`, restore). The CLOBBER_ARGV "point extra argv slots at end_of_area" loop is platform machinery (not reproduced). |
| `update_ps_display_precheck(void)` (345, static) | `update_ps_display_precheck(&PsState)` | MATCH | `!update_process_title`→false; `!IsUnderPostmaster`→false; CLOBBER_ARGV `!ps_buffer`→false modeled as `!saved_args`→false. |
| `set_ps_display_suffix(const char*)` (371) | `set_ps_display_suffix(&str)` | MATCH | precheck gate; overwrite-existing-suffix via `nosuffix_len`; the `cur_len+len+1 >= buffer_size` overflow branch with the inner `cur_len < buffer_size-1` guard, the `' '` separator, and bounded fill all reproduced (`push_truncated` reserves the trailing NUL byte, matching C's forced `ps_buffer[size-1]='\0'`); else branch appends space + full suffix. Re-derived both branches byte-for-byte. |
| `set_ps_display_remove_suffix(void)` (423) | `set_ps_display_remove_suffix()` | MATCH | precheck gate; `nosuffix_len==0`→no-op; truncate to `nosuffix_len`, reset `nosuffix_len=0`. |
| `set_ps_display_with_len(const char*, size_t)` (453) | `set_ps_display_with_len(&str, usize)` | MATCH | `Assert(strlen==len)`→`debug_assert_eq!`; wipe suffix; the `fixed_size+len >= buffer_size` truncation branch (`buffer_size-fixed_size-1` bytes + forced NUL) vs. full-copy branch reproduced via truncate-to-`fixed_size` + `push_truncated`. |
| `set_ps_display(const char*)` (64, c2rust inline) | `set_ps_display(&str)` | MATCH | thin wrapper → `set_ps_display_with_len(activity, activity.len())`. |
| `flush_ps_display(void)` (488, static) | `flush_ps_display(&str)` + `os_set_proc_title` | MATCH (platform) | On the build platform (CLOBBER_ARGV) the body is only the `last_status_len > cur_len` MemSet-padding of the physical argv region (`PS_PADDING='\0'`, `MEMSET_LOOP_LIMIT=1024`). That clobber exists solely to erase stale bytes in the shared argv area; the port keeps the title in an owned `String`, so there are no stale physical bytes to clear and the padding has no observable analogue. `setproctitle`/Win32 branches are not in the build and appear as documented `cfg` fallbacks. |
| `get_ps_display(int*)` (532) | `get_ps_display() -> (String, usize)` | MATCH | CLOBBER_ARGV `!ps_buffer`→`("",0)` (modeled by clamped slice when buffer empty); otherwise activity slice at `+fixed_size` and `cur_len-fixed_size` length. |

Helpers with no direct C counterpart: `update_process_title()`/
`set_update_process_title()` expose the `update_process_title` GUC storage that
C keeps as the module global (line 31); `push_truncated` is the safe analogue of
C's bounded `memcpy` into the fixed `ps_buffer` (reserves one byte for the NUL,
truncates on a UTF-8 char boundary). Both verified to preserve C bounds exactly.

### Globals

`update_process_title`, `ps_buffer*`, `last_status_len`, `save_argc/argv` are C
module statics = per-backend state, correctly modeled as `thread_local!` STATE
(AGENTS.md "Backend-global state"); no shared static introduced. The
`update_process_title` GUC storage is installed into the guc-tables var accessor
from this crate's `init_seams()` (the owner). Conforms.

## Platform machinery (ledgered divergence)

`PS_USE_CLOBBER_ARGV` works by overwriting the contiguous `argv`/`environ`
region `main()` was handed. The portable buffer algorithm
(prefix/activity/suffix bookkeeping, truncation) is reproduced exactly; the
three platform-specific pieces are not, and are documented in the module header:

1. the contiguous-region scan + environ relocation + argv copy in
   `save_ps_display_args` (consequently `ps_buffer_size` is approximated as the
   summed argv bytes rather than `end_of_area - argv[0]`);
2. pointing the spare `save_argv[i]` slots at `end_of_area`;
3. the `flush_ps_display` MemSet padding of the physical argv tail.

These are the OS transmission mechanism, not absent program logic: the tracked
buffer is authoritative and every caller (`get_ps_display` and the seam
consumers) observes the correct title. This is a platform-machinery boundary,
not a `PARTIAL`/`DIVERGES` of the buffer algorithm, and is the standard
treatment for argv-clobber in this repo (the title is observable through the
crate's own state, no `&'static mut` argv handle is invented). No SQLSTATE/error
path is involved (`save_ps_display_args` uses `write_stderr`+`exit` for OOM, pre
-elog; the port allocates a `String` so the OOM path does not arise).

## Seam audit (step 3)

**Owned seam crates** (every `crates/X-seams` whose `X` maps to `ps_status.c`):
`backend-utils-misc-ps-status-seams` and `backend-utils-misc-more-seams` (the
latter also carries `rls.c`'s `check_enable_rls`, outside this unit's scope).

Inward seams declared and their install status after this audit's fix:

| Seam crate :: decl | Installed by `backend-utils-misc-more::init_seams()` |
|---|---|
| `ps-status-seams::set_ps_display_suffix` | yes |
| `ps-status-seams::set_ps_display_remove_suffix` | yes |
| `ps-status-seams::set_ps_display(String)` | yes (added) |
| `ps-status-seams::update_process_title` | yes |
| `ps-status-seams::init_ps_display(&[u8])` | yes (added) |
| `more-seams::init_ps_display(Option<&str>)` | yes |
| `more-seams::set_ps_display(&str)` | yes (added) |

`seams-init::init_all()` calls `backend_utils_misc_more::init_seams()`
(`crates/seams-init/src/lib.rs:71`). Each installer is a thin
argument-convert + single delegate (the `&[u8]` one reads the NUL-terminated
`bgw_name` C string and `from_utf8_lossy`s it — pure marshalling, no branching
logic). No `set()` lives outside the owner. No outward seam calls originate in
`ps_status.rs` except `GetBackendTypeDesc`/`MyBackendType` in `init_ps_display`,
each a justified thin delegate to the real owner (miscinit / init-small) across
a genuine dependency direction. Conforms.

### Finding (FIXED on this branch)

Initial state of `init_seams()` declared but did **not** install three inward
seams that have live callers:

- `ps-status-seams::set_ps_display` — called by `backend-tcop-backend-startup`
  and `backend-postmaster-pgarch` (3 sites);
- `ps-status-seams::init_ps_display(&[u8])` — called by
  `backend-postmaster-bgworker`;
- `more-seams::set_ps_display(&str)` — called by `backend-utils-init-postinit`
  (2 sites).

These would have panicked at runtime ("seam not installed"). Per SKILL §3 an
uninstalled seam in an owned crate is an automatic finding. Fixed by adding the
three `::set()` installs to `backend-utils-misc-more/src/init_seams()`
delegating to `ps_status::set_ps_display` / `ps_status::init_ps_display`.
Workspace re-build of the crate, `seams-init`, and all five consumer crates is
clean.

## §3b design conformance

No invented opacity (no new handles/types; bytes→`&str` marshalling only).
No allocating seam without `Mcx`/`PgResult` (the title `String` is owned
backend-local state, not an Mcx allocation; the seams are infallible matching
the C `void`/assert-only surface). No shared static for per-backend globals
(`thread_local!`). No ambient-global seam, no lock held across `?`, no
registry-shaped side table, no unledgered divergence marker (the platform
boundary is documented in the module header and here). Conforms.

## Conclusion

Every `ps_status.c` function is MATCH (with the argv-clobber transmission
mechanism documented as a platform-machinery boundary, not absent logic). The
seam-install finding is fixed; all owned ps_status seams are now installed by
the owning crate's `init_seams()`, reachable from `seams-init::init_all()`, and
every live consumer compiles. **PASS.**
