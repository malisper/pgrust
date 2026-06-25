# Audit: backend-access-common-more

Unit `backend-access-common-more` — C sources `printtup.c` + `reloptions.c`.
Ported crates: `backend-access-common-printtup`,
`backend-access-common-reloptions` (plus owner seam crates
`backend-utils-adt-arrayfuncs-seams`, `backend-utils-misc-guc-seams`,
`backend-commands-define-seams`, and the extended `amapi` /
`reloptions` seams).

Audit performed independently from the C source
(`postgres-18.3/src/backend/access/common/{printtup,reloptions}.c`) and the
c2rust rendering (`c2rust-runs/backend-access-common-more/src/`).

## printtup.c (`crates/backend-access-common-printtup/src/lib.rs`)

| C function (line) | Port | Verdict | Notes |
|---|---|---|---|
| `printtup_create_DR` (71) | `DR_printtup::printtup_create_DR` | MATCH | sendDescrip = (dest==DestRemote); fields zeroed; lifecycle hooks are free fns. |
| `SetRemoteDestReceiverParams` (100) | `SetRemoteDestReceiverParams` | MATCH | Portal held by runtime; the C Assert on mydest reproduced as debug_assert. |
| `printtup_startup` (111) | `printtup_startup` | MATCH | initStringInfo over Mcx; tmpcontext owned by runtime; sends RowDescription when sendDescrip. `operation` unused in C. |
| `SendRowDescriptionMessage` (166) | `SendRowDescriptionMessage` | MATCH | natts + per-attr loop; getBaseTypeAndTypmod; resjunk-skip tlist walk; formats NULL → 0; field write order/byte widths identical (pq_send* == pq_write* over PgVec). enlargeStringInfo is pure pre-reservation, no behavioral effect. |
| `printtup_prepare_info` (250) | `printtup_prepare_info` | MATCH | clears myinfo; numAttrs<=0 early return; format 0→text,1→binary,else ERRCODE_INVALID_PARAMETER_VALUE "unsupported format code". |
| `printtup` (304) | `printtup` | MATCH | re-derive on descriptor identity or nattrs change; slot_getallattrs; tmpcontext enter/exit around loop; null→int32(-1); text→sendcountedtext(strlen); binary→int32(VARSIZE-VARHDRSZ)+sendbytes. VALGRIND check has no safe-Rust analogue. Returns true. |
| `printtup_shutdown` (389) | `printtup_shutdown` | MATCH | clears myinfo/attrinfo; buf+tmpcontext freed by runtime. |
| `printtup_destroy` (413) | `printtup_destroy` | MATCH | consuming drop (C pfree(self)). |
| `printatt` (423) | `printatt` | MATCH | format string and byval 't'/'f' rendering reproduced exactly. |
| `debugStartup` (444) | `debugStartup` | MATCH | per-attr printatt(i+1, ..., NULL) + "\t----\n". |
| `debugtup` (462) | `debugtup` | MATCH | per-attr: skip null; getTypeOutputInfo+OidOutputFunctionCall; printatt with value; "\t----\n"; returns true. Uses slot_getallattrs once instead of per-attr slot_getattr — observationally identical. |

Seam audit: this crate owns **no** seams. Externals (slot, lsyscache,
fmgr, portal, mcxt) go through the stateful `PrinttupRuntime` trait supplied
per receiver, with fail-safe error defaults; the control flow around each
trait call is ported 1:1. `init_seams()` is correctly empty. Message-byte
construction uses this repo's `backend-libpq-pqformat` send primitives (sink is
inside `pq_endmessage_reuse`), so no comm seam is needed. No findings.

## reloptions.c (`crates/backend-access-common-reloptions/src/lib.rs`)

All 41 reloptions.c functions enumerated from the C and cross-checked against
the c2rust rendering; every one has a counterpart.

| C function (line) | Port | Verdict | Notes |
|---|---|---|---|
| built-in tables boolRelOpts/intRelOpts/realRelOpts/enumRelOpts/stringRelOpts (94-562) | `bool/int/real/enum/string_rel_opts` | MATCH | Every value transcribed; constants verified vs headers (HEAP/BTREE/HASH/GIST/SPGIST fillfactors, MAX_IO_CONCURRENCY=1000, MAX_KILOBYTES=INT_MAX, TOAST_TUPLE_TARGET via MaximumBytesPerTuple). Enum symbol order AUTO=0/OFF=1/ON=2, NOT_SET=0/LOCAL=1/CASCADED=2 verified vs utils/rel.h. |
| `initialize_reloptions` (591) | `initialize_reloptions` | MATCH | rebuilds relOpts from builtins + custom; sets namelen/type; need_initialization=false. Assert(DoLockModesConflict) is a debug check. |
| `add_reloption_kind` (694) | `add_reloption_kind` | MATCH | limit at RELOPT_KIND_MAX → ERRCODE_PROGRAM_LIMIT_EXCEEDED; shift left. |
| `add_reloption` (711) | `add_reloption` | MATCH | append custom; need_initialization=true (Vec grow replaces manual realloc). |
| `init_local_reloptions` (745) | `init_local_reloptions` | MATCH | |
| `register_reloptions_validator` (758) | `register_reloptions_validator` | MATCH | |
| `add_local_reloption` (768) | `add_local_reloption` | MATCH | Assert(offset<size) as debug_assert. |
| `allocate_reloption` (786) | `allocate_reloption` | MATCH | type-agnostic field init; TopMemoryContext placement is owned-tree (decision 5). |
| `init_bool_reloption`/`add_bool_reloption`/`add_local_bool_reloption` (843-884) | same | MATCH | |
| `init_int_reloption`/`add_int_reloption`/`add_local_int_reloption` (892-938) | same | MATCH | |
| `init_real_reloption`/`add_real_reloption`/`add_local_real_reloption` (944-993) | same | MATCH | |
| `init_enum_reloption`/`add_enum_reloption`/`add_local_enum_reloption` (999-1057) | same | MATCH | |
| `init_string_reloption`/`add_string_reloption`/`add_local_string_reloption` (1063-1141) | same | MATCH | validator(default) sanity check; default_isnull/default_len/"" handling matched. |
| `transformRelOptions` (1167) | `transformRelOptions` | **MATCH (fixed)** | See finding F1 — empty-deflist return-oldOptions corrected. Namespace filtering, dedup-keep, RESET checks, oids filter, name "=" check, "name=value"/"name=true" flattening all match. |
| `untransformRelOptions` (1351) | `untransformRelOptions` | MATCH | split on first '='; returns (name, Some(val))/(name, None). |
| `extractRelOptions` (1399) | `extractRelOptions` | MATCH | relkind dispatch identical; isnull→None; foreign→None; default Assert(false). |
| `parseRelOptionsInternal` (1447) | `parseRelOptionsInternal` | MATCH | match by namelen+'='+strncmp; parse_one_reloption; unrecognized under validate → ERRCODE_INVALID_PARAMETER_VALUE. |
| `parseRelOptions` (1519) | `parseRelOptions` | MATCH | builds expected set by kind bit; numoptions==0 ⇒ empty Vec (= C NULL). |
| `parseLocalRelOptions` (1561) | `parseLocalRelOptions` | MATCH | |
| `parse_one_reloption` (1589) | `parse_one_reloption` | MATCH | duplicate-set error; per-type parse + bounds; enum default-on-no-validate; %f detail via 6-digit format; string nofree. |
| `allocateReloptStruct` (1722) | `allocateReloptStruct` | MATCH | base + per-string fill_cb/len+1; palloc0 → zeroed Vec via try_reserve. |
| `fillRelOptions` (1762) | `fillRelOptions` | MATCH | offset table writes, isset_offset, string fill_cb/strcpy paths, SET_VARSIZE. Byte-buffer path used only by build_local_reloptions; native-endian writes match C struct layout. |
| `default_reloptions` (1869) | `default_reloptions` | MATCH | typed-tree fill keyed by name == byte-offset table; unknown-under-validate errors as C fillRelOptions; vacuum_truncate isset recorded. |
| `build_reloptions` (1943) | `build_reloptions` | MATCH | numoptions==0 ⇒ None. |
| `build_local_reloptions` (1980) | `build_local_reloptions` | MATCH | byte-buffer path retained (opaque AM layout); validators run under validate. |
| `partitioned_table_reloptions` (2020) | `partitioned_table_reloptions` | MATCH | validate&&Some ⇒ ERRCODE_WRONG_OBJECT_TYPE + hint. |
| `view_reloptions` (2034) | `view_reloptions` | MATCH | |
| `heap_reloptions` (2055) | `heap_reloptions` | MATCH | TOAST adjustments (fillfactor=100, analyze_threshold=-1, analyze_scale_factor=-1); RELATION/MATVIEW→HEAP; else None. |
| `index_reloptions` (2090) | `index_reloptions` | MATCH | strict (None datum→None); delegates to amoptions via amapi seam. |
| `attribute_reloptions` (2105) | `attribute_reloptions` | MATCH | typed AttributeOpts fill. |
| `tablespace_reloptions` (2122) | `tablespace_reloptions` | MATCH | typed TableSpaceOpts fill. |
| `AlterTableGetRelOptionsLockLevel` (2144) | `AlterTableGetRelOptionsLockLevel` | MATCH | empty→AccessExclusiveLock; exact-name match (strncmp namelen+1) ⇒ `name == defname`; max lockmode. |
| `parse_bool` (utils/adt/bool.c) | `builtin_parse_bool` | MATCH | in-crate pure port; spellings/lengths verified by test. |
| `pg_strcasecmp` (port) | `pg_strcasecmp` | MATCH | ASCII case-fold compare. |

### Findings

**F1 (fixed) — `transformRelOptions` empty-deflist return.** The original
port returned `Ok(None)` for an empty `defList`, but C returns `oldOptions`
verbatim; `None` is also the function's "(Datum) 0 / clear options" return, so
the two distinct C outcomes collapsed and the caller could not distinguish
"keep existing options" from "clear options". Fixed: empty `defList` now
re-hands the old `text[]` (deconstruct then reconstruct, content-equal to C's
returned `oldOptions`) as a `Datum`, and only returns `None` when
`old_options` was itself absent (= C `(Datum) 0`). The empty-but-non-null
old-array micro-edge (returns `None` instead of an empty-array Datum) is not
reachable for catalog reloptions and is documented inline.

### Seam audit

- `backend-utils-adt-arrayfuncs-seams` (`deconstruct_text_array` /
  `construct_text_array`): justified — the `text[]` array routines live in the
  unported `backend-utils-adt-array-more`; thin marshal + delegate, take `Mcx`.
- `backend-utils-misc-guc-seams` (`parse_int` / `parse_real`): justified —
  `utils/misc/guc.c`; infallible `Option` returns; reloptions always uses
  flags=0/no-units, faithfully represented.
- `backend-commands-define-seams` (`def_get_string` / `def_get_boolean` +
  `DefElemArg` projection): justified — `commands/define.c` value-node switch;
  thin delegate.
- `backend-access-index-amapi-seams::am_reloptions`: justified — the index AM's
  `amoptions` callback; thin delegate.
- This crate **owns** `attribute_reloptions` / `tablespace_reloptions`
  (declared in `backend-access-common-reloptions-seams`) and installs both in
  `init_seams()`; the seam bodies are thin (scratch context + one in-crate
  call). `seams-init::init_all()` calls
  `backend_access_common_reloptions::init_seams()`. No uninstalled seam, no
  `set()` outside the owner. No findings.

### Design conformance

- Per-backend file-scope statics (`relOpts`, `last_assigned_kind`,
  `custom_options`, `need_initialization`) are in a `thread_local!`
  (one backend = one thread) — conforms.
- Option-definition tables are backend-lifetime owned `String`/`Vec`
  (mcx decision 5); query-lifetime parse copies allocate through the threaded
  `Mcx`; result buffers via `Mcx`/`try_reserve` — conforms.
- No invented opacity, no ambient-global seams, no locks across `?`, no
  registry-shaped side tables. `relopt_kind`/`relopt_type` as `i32` aliases is
  recorded in DESIGN_DEBT.

## Verdict

**PASS.** One logic finding (F1) was found and fixed; the fixed function was
re-audited from scratch against the C and now matches. Every function is
`MATCH`; all seams justified and installed; design rules satisfied. Build and
unit tests green for both crates.
