# Audit: backend-utils-fmgr-dfmgr

- **Date:** 2026-06-12
- **Model:** Opus 4.8 (1M context) — claude-opus-4-8[1m]
- **Branch:** port/backend-utils-fmgr-dfmgr
- **Unit c_sources:** `src/backend/utils/fmgr/dfmgr.c`
- **C:** `../pgrust/postgres-18.3/src/backend/utils/fmgr/dfmgr.c`
- **c2rust:** `../pgrust/c2rust-runs/backend-utils-fmgr-dfmgr/src/dfmgr.rs`
- **Port:** `crates/backend-utils-fmgr-dfmgr/src/lib.rs`, `crates/types-dfmgr/src/lib.rs`,
  `crates/port-dynloader-seams/src/lib.rs`, `crates/backend-utils-fmgr-dfmgr-seams/src/lib.rs`

Independent from-scratch re-derivation from the C; the port's comments, its
self-review, the prior audit body, and a green build were not trusted. Every C
function definition enumerated and compared against the C, the c2rust render,
and the port. Constants re-checked against the headers. Seams and design
conformance re-derived.

## Top-line verdict: **PASS**

This re-audit reaches PASS after one fix-and-re-audit round on a **new**
finding (S1) that the prior audit body missed. The prior FAIL finding **D1**
(ABI-extra `strcmp` semantics) is independently **confirmed resolved**.

## Function inventory & verdicts

16 function definitions in `dfmgr.c` (the `incompatible_module_error` forward
decl at line 72 and its definition at line 316 are one function).

| # | C function | C loc | Port loc | Verdict | Notes |
|---|-----------|-------|----------|---------|-------|
| 1 | `load_external_function` | 94 | lib.rs:96 | MATCH | `$libdir/` simple-name strip (`strncmp 8` + no further separator → `strip_simple_libdir_prefix`), expand, `internal_load_library`, symbol lookup via `function_exists` seam, `ERRCODE_UNDEFINED_FUNCTION` with exact `could not find function "%s" in file "%s"` text when not found and `signal_not_found`. `*filehandle` out-param → returned `LibraryHandle`. |
| 2 | `load_file` | 148 | lib.rs:131 | MATCH | restricted-name check first, expand, `internal_load_library`; unit return. |
| 3 | `lookup_external_function` | 170 | lib.rs:148 | SEAMED | bare `dlsym(filehandle,funcname) != NULL` → `loader::function_exists` (bool, no error surface, matching the C `void *` return). Thin marshal+delegate. |
| 4 | `internal_load_library` | 188 | lib.rs:157 | MATCH | name-scan → stat (errors via seam) → `SAME_INODE` inode-scan → open (dlopen+magic) → ABI check (`len != sizeof` OR `abi_fields` compare) → dlclose + `incompatible_module_error`; missing-magic → dlclose + "missing magic block" + PG_MODULE_MAGIC hint; `_PG_init` call; link into list (`malloc` → fallible `try_reserve` → `ERRCODE_OUT_OF_MEMORY`). Ordering and dlclose-on-failure mirrored. |
| 5 | `incompatible_module_error` | 316 | lib.rs:262 | **MATCH** (D1 fixed) | version-mismatch branch (`%d` if ≥1000 else `%d.%d`, server `version/100`), ABI-extra branch (now C-string compare — see D1), per-field detail branch (FUNC_MAX_ARGS, INDEX_MAX_KEYS, NAMEDATALEN, FLOAT8PASSBYVAL true/false; `\n`-joined; "unexpected length or padding" fallback). C `pg_noreturn ereport(ERROR)` → returns `PgError`. |
| 6 | `get_first_loaded_module` | 424 | lib.rs:418 | MATCH | `file_list` head → index 0 / `None`. |
| 7 | `get_next_loaded_module` | 430 | lib.rs:430 | MATCH | `dfptr->next` → index+1 / `None`. |
| 8 | `get_loaded_module_details` | 444 | lib.rs:443 | MATCH | `filename` / `magic->name` / `magic->version`; latter two `Option` (NULL-able). |
| 9 | `expand_dynamic_library_name` | 465 | lib.rs:541 | MATCH | have-slash (`substitute_path_macro` + `pg_file_exists`) vs no-slash (`find_in_path`); `name+DLSUFFIX` retry; `pstrdup(name)` fallback. `DLSUFFIX=".so"`. palloc → fallible `PgString` in `mcx`. |
| 10 | `check_restricted_library_name` | 520 | lib.rs:584 | MATCH | must start `"$libdir/plugins/"` (16) and no separator after → `ERRCODE_INSUFFICIENT_PRIVILEGE`, exact text. |
| 11 | `substitute_path_macro` | 535 | lib.rs:602 | MATCH | non-`$` start → copy; sep = first_dir_separator or strlen; macro length/equality check → `ERRCODE_INVALID_NAME`; else `value + tail`. |
| 12 | `find_in_path` | 573 | lib.rs:633 | MATCH | empty-path → None; per-component zero-length (`sep==Some(0)`) → `ERRCODE_INVALID_NAME`, `substitute_path_macro`, `canonicalize_path` seam, `is_absolute_path` seam → `ERRCODE_INVALID_NAME`, `mangled/basename`, `pg_file_exists`; loop advance `p[len]=='\0'` break. `elog(DEBUG3)` trace omitted (diagnostic, no behavior). Trailing-`:` rejected by the absolute-path check, matching C. |
| 13 | `find_rendezvous_variable` | 663 | lib.rs:460 | MATCH | lazy hash create + `HASH_ENTER`, init `0`(NULL) on first use; returns current value. C `void **` write-through split into get + `set_rendezvous_variable` (process-lifetime token). |
| 14 | `EstimateLibraryStateSpace` | 701 | lib.rs:479 | MATCH | `size = 1 + Σ(strlen(filename)+1)`. `add_size` overflow-ereport → `saturating_add`. |
| 15 | `SerializeLibraryState` | 718 | lib.rs:492 | MATCH | per-filename NUL-terminated copy + final NUL; C `Assert(len<maxsize)` → buffer-fit precheck. |
| 16 | `RestoreLibraryState` | 740 | lib.rs:513 | MATCH | walk NUL-separated, double-NUL-terminated blob; `internal_load_library` each. |

In-crate helpers (supporting the above, not separate C functions):
`strip_simple_libdir_prefix`, `first_dir_separator`, `first_path_var_separator`
(non-Windows `common/path.c` byte scans), `same_inode` (`SAME_INODE`),
`abi_extra_cstr` (the D1 fix), `abi_extra_string`, `bool_text`,
`magic_struct_len`, `build_field_mismatch_detail`, `check_module_magic`,
`pkglib_path`/`dynamic_library_path`/`set_dynamic_library_path` accessors — all
reviewed, consistent with the C.

## Constants verified (against headers, not memory)

- `Pg_abi_values` (`fmgr.h:466`): `version, funcmaxargs, indexmaxkeys, namedatalen, float8byval` (5×`int`) + `char abi_extra[32]` → matches `types_core::fmgr::PgAbiValues`.
- `Pg_magic_struct` (`fmgr.h:477`): `int len; Pg_abi_values abi_fields; const char *name; const char *version;`. `magic_struct_len()` = 4 + (5·4+32) + 8 + 8 = **72** (64-bit, no tail padding). Correct.
- `PG_VERSION_NUM = 180003` (→ `version` 1800); `FUNC_MAX_ARGS = 100`
  (`pg_config_manual.h:43`); `INDEX_MAX_KEYS = 32` (`:69`); `NAMEDATALEN = 64`
  (`:29`); `FLOAT8PASSBYVAL = true` (`c.h:602`, build → 1); `FMGR_ABI_EXTRA =
  "PostgreSQL"` (`:60`) NUL-padded; `PG_MAGIC_FUNCTION_NAME_STRING =
  "Pg_magic_func"`; `_PG_init`. All match `types-core`.
- SQLSTATEs: `ERRCODE_UNDEFINED_FUNCTION`, `ERRCODE_OUT_OF_MEMORY`,
  `ERRCODE_INSUFFICIENT_PRIVILEGE`, `ERRCODE_INVALID_NAME` — present at the same
  predicates as C; `errcode_for_file_access()` is the stat-seam's responsibility.

## Findings

### D1 — `incompatible_module_error` ABI-extra comparison — **RESOLVED (confirmed)**

`dfmgr.c:348` compares `strcmp(module_magic_data->abi_extra,
magic_data.abi_extra)` — C-string semantics that stop at the first NUL. The
port (`lib.rs:295`) now compares `abi_extra_cstr(&module_magic_data.abi_extra)
!= abi_extra_cstr(&magic_data.abi_extra)`, where `abi_extra_cstr` returns the
NUL-terminated prefix (`lib.rs:742`). On a module whose `abi_extra` matches
`"PostgreSQL\0"` but differs only in bytes after the NUL — the only input that
reaches this function with all abi_fields otherwise equal (after the full
32-byte `memcmp` in `internal_load_library` has already failed) — the port now
falls through to the field-by-field section, finds everything equal, and emits
`"... magic block mismatch"` / `"Magic block has unexpected length or padding
difference."`, byte-identical to C. The regression test
`abi_extra_compares_as_c_string_not_full_array` (tests.rs:240) exercises exactly
that input and asserts both the message and the fallback detail. Independently
re-derived and confirmed.

### S1 — owned inward seam crate had uninstalled declarations — **FIXED this round**

`crates/backend-utils-fmgr-dfmgr-seams` is an owned seam crate (its name maps to
this unit / dfmgr.c, and its module doc states "The owning unit installs these
from its `init_seams()`"). It was introduced by the consumer ports
`backend-utils-fmgr-core` (`fmgr.c`) and `backend-replication-logical-logical`
(`logical.c`) and arrived on this branch via the main-merge `c63ad553`. It
declares three seams:

- `load_external_function(probin, prosrc, function_id) -> LoadedExternalFunc`
  (consumed by fmgr-core `fmgr_info_c_lang`);
- `load_output_plugin(plugin) -> u32` (consumed by logical.c `LoadOutputPlugin`);
- `invoke_output_plugin_callback(inv) -> bool` (consumed by logical.c's `*_cb_wrapper`).

The crate's `init_seams()` was empty (`pub fn init_seams() {}`). Per audit-crate
step 3, *every declaration in every owned seam crate must be installed by the
crate's `init_seams()`; an empty installer with owned seam crates outstanding is
an automatic FAIL.* None of the three were installed anywhere in the workspace.
(The prior audit body wrongly asserted "no dfmgr-mapped `-seams` crate exists" —
it predated the merge that added it.)

**Fix (this branch):** `backend-utils-fmgr-dfmgr::init_seams()` now installs all
three (`lib.rs`). Each installer is a thin marshal+delegate that composes this
crate's own `load_external_function` with the OS-loader runtime for the parts
that live inside a `dlopen`'d library — declared as new
`port-dynloader-seams` edges (`fetch_finfo_record`, `plugin_init`,
`invoke_output_plugin_callback`):

- `load_external_function` installer: `load_external_function(probin, prosrc,
  true)` (dfmgr's own logic) then `loader::fetch_finfo_record(handle, prosrc)`
  (fmgr.c's `fetch_finfo_record`, an OS-symbol read);
- `load_output_plugin` installer: `load_external_function(plugin,
  "_PG_output_plugin_init", false)` then `loader::plugin_init(handle)` (the
  plugin's vtable hook);
- `invoke_output_plugin_callback` installer: pure delegate to
  `loader::invoke_output_plugin_callback(inv)` (loaded-symbol dispatch).

No branching, node construction, or computation lives in the seam path beyond
the single dfmgr load + single loader delegate; the foreign-symbol work is
pushed to the platform loader runtime, consistent with the existing
`open_library`/`call_pg_init` modeling. `seams-init::init_all()` already calls
`backend_utils_fmgr_dfmgr::init_seams()` (`seams-init/src/lib.rs:64`). The new
loader seams panic loudly until the loader runtime lands (mirror-PG-and-panic),
which is the correct deferral for an unported callee. `cargo build --workspace`
and `cargo test -p backend-utils-fmgr-dfmgr` (19 passed) are green; `seams-init`
and `backend-replication-logical-logical` build/test clean (no "installed
twice").

## Seam audit

**Owned seam crates** — every `crates/X-seams` where `X` maps to a C file in
this unit's `c_sources` (= `dfmgr.c`): **`backend-utils-fmgr-dfmgr-seams`**
(the inward seam crate). After this round, every one of its three declarations
is installed by `backend-utils-fmgr-dfmgr::init_seams()`, which contains nothing
but `set()` calls and is wired into `seams-init::init_all()`. No uninstalled
declaration and no `set()` outside the owner.

**Outward seams** (owned by other units / platform runtime — genuine
cross-subsystem edges, thin marshal+delegate, no in-seam branching/computation;
panic until their owners land, per mirror-PG-and-panic):

- `port-dynloader-seams` — the OS dynamic loader (`<dlfcn.h>`/`stat(2)`):
  `stat_identity`→`stat`; `open_library`→`dlopen`+`dlsym(Pg_magic_func)`+magic;
  `call_pg_init`→`dlsym(_PG_init)`+invoke; `close_library`→`dlclose`;
  `function_exists`→`dlsym != NULL`; plus the three new symbol-invocation edges
  used by the installers (`fetch_finfo_record`, `plugin_init`,
  `invoke_output_plugin_callback`). The opaque `void *` handle never crosses as
  a pointer — integer `LibraryHandle` token (types.md rules 6–7 respected).
- `common-path-seams` (`canonicalize_path`, `is_absolute_path`) — `common/path.c`.
- `backend-storage-file-fd-seams::pg_file_exists` — `storage/file/fd.c`; returns
  `PgResult<bool>` because it can `ereport(ERROR)`.

`pkglib_path` (globals.c) is read **directly** from its ported owner
`backend-utils-init-small` (acyclic → no seam). `Dynamic_library_path` is
dfmgr.c's own GUC variable → backend-local `thread_local!` with a setter (owned
here, not a seam).

No function body was replaced by a "call elsewhere"; all dfmgr logic lives in
this crate.

## Design conformance (step 3b)

- Opacity inherited, not invented: `LibraryHandle` stands in for the genuinely
  opaque OS `dlopen` `void *` handle (integer token, documented).
  `Pg_magic_struct`/`LoadedModule`/`FileIdentity`/`PgAbiValues` are the real C
  types with header-verified values.
- Allocating functions take `Mcx<'mcx>` and return `PgResult<PgString>`
  (`expand_dynamic_library_name`, `substitute_path_macro`, `find_in_path`); the
  loaded-files spine growth is fallible (`try_reserve` → `ERRCODE_OUT_OF_MEMORY`).
  The two `load_*` installers create a transient `MemoryContext` for the path
  expansion (no caller-supplied mcx in the seam signature), mirroring C, which
  `pfree`s the expanded name within the same call.
- Per-backend statics (`file_list`/`file_tail`, `rendezvousHash`,
  `Dynamic_library_path`) are `thread_local!`, not shared statics.
- No ambient-global getter seams, no locks held across `?`, no registry-shaped
  side tables, no unledgered divergence markers.

## Spot-check of MATCH verdicts (re-derived in detail)

- `find_in_path` trailing-`:`: a trailing `:` advances `p` past the separator
  to the terminator; the next scan returns NULL, `len = 0`, and the empty
  component is canonicalized and rejected by the absolute-path check — NOT the
  zero-length check. The port reproduces this (`sep == Some(0)` triggers only on
  a separator at offset 0). Verified.
- D1 reachability: the full-32-byte `abi_fields` compare in
  `internal_load_library` (`magic.abi_fields != PgAbiValues::server()`) is
  field-wise but covers all 32 `abi_extra` bytes, so a post-NUL-only difference
  fails that compare and enters `incompatible_module_error` — precisely the
  case the C-string compare in S1's sibling D1 must then treat as equal.
  Verified against both the fix and its regression test.

## Disposition

D1 confirmed resolved; S1 found and fixed this round; both re-audited from
scratch and clean. All functions MATCH or SEAMED; the owned seam crate is fully
installed; design conformance holds. **PASS.** `CATALOG.tsv` may stay
`audited`.
