# Audit: backend-utils-adt-pg-locale-icu

- **Unit**: `backend-utils-adt-pg-locale-icu`
- **C source**: `src/backend/utils/adt/pg_locale_icu.c` (postgres-18.3, 1023 lines)
- **Branch**: `port/backend-utils-adt-pg-locale-icu`
- **Date**: 2026-06-12
- **Model**: Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Verdict**: **PASS**

Crates audited: `crates/backend-utils-adt-pg-locale-icu`,
`crates/backend-utils-adt-pg-locale-icu-seams`, `crates/types-locale`.
Independent re-derivation from the C source and headers
(`utils/pg_locale.h`, `catalog/pg_collation.h`, `utils/errcodes.h`), cross-checked
against `../pgrust/c2rust-runs/backend-utils-adt-pg-locale-icu/src/pg_locale_icu.rs`.

## Build profile (decisive)

The entire body of `pg_locale_icu.c` except `create_pg_locale_icu` is wrapped in
`#ifdef USE_ICU`. The migration build profile compiles with **ICU disabled**.
This is confirmed independently from the c2rust run, which ran post-preprocessor
against the build's compile_commands and kept **exactly one** function definition
(`c2rust-runs/.../src/pg_locale_icu.rs:644`, `create_pg_locale_icu`) — and within
it only the `#else` arm (the bare `ereport(ERROR, ERRCODE_FEATURE_NOT_SUPPORTED)`
plus an unreachable `return NULL`). Every `#ifdef USE_ICU` function (and the ICU
arm of `create_pg_locale_icu`) is absent from the compiled object; per the SKILL
these `#if`-excluded branches have no compiled counterpart and are out of scope
for this profile. They land with the ICU subsystem when ICU is enabled.

## Per-function inventory

Every function definition in the C file, with compile status in this profile.

| C function | C loc | Compiled (no-ICU)? | Port loc | Verdict |
|---|---|---|---|---|
| `create_pg_locale_icu` (#else arm) | `:142` (else `211-218`) | Yes | `crates/backend-utils-adt-pg-locale-icu/src/lib.rs:33` | SEAMED (MATCH on live arm) |
| `pg_ucol_open` | `:229` | No (`#ifdef USE_ICU`) | — | N/A — uncompiled, lands with ICU |
| `make_icu_collator` | `:319` | No | — | N/A — uncompiled |
| `strlower_icu` | `:382` | No | — | N/A — uncompiled |
| `strtitle_icu` | `:402` | No | — | N/A — uncompiled |
| `strupper_icu` | `:422` | No | — | N/A — uncompiled |
| `strfold_icu` | `:442` | No | — | N/A — uncompiled |
| `strncoll_icu_utf8` | `:470` (`#ifdef HAVE_UCOL_STRCOLLUTF8`) | No | — | N/A — uncompiled |
| `strnxfrm_icu` | `:495` | No | — | N/A — uncompiled |
| `strnxfrm_prefix_icu_utf8` | `:542` | No | — | N/A — uncompiled |
| `get_collation_actual_version_icu` | `:573` | No | — | N/A — uncompiled |
| `icu_to_uchar` | `:601` | No | — | N/A — uncompiled |
| `icu_from_uchar` | `:628` | No | — | N/A — uncompiled |
| `icu_convert_case` | `:659` | No | — | N/A — uncompiled |
| `u_strToTitle_default_BI` | `:686` | No | — | N/A — uncompiled |
| `u_strFoldCase_default` | `:696` | No | — | N/A — uncompiled |
| `strncoll_icu` | `:738` | No | — | N/A — uncompiled |
| `strnxfrm_prefix_icu` | `:787` | No | — | N/A — uncompiled |
| `init_icu_converter` | `:837` | No | — | N/A — uncompiled |
| `uchar_length` | `:869` | No | — | N/A — uncompiled |
| `uchar_convert` | `:888` | No | — | N/A — uncompiled |
| `icu_set_collation_attributes` | `:916` | No | — | N/A — uncompiled |

Non-function `#ifdef USE_ICU` items also uncompiled: the `collate_methods_icu` /
`collate_methods_icu_utf8` static vtables (`:121`, `:128`) and the `icu_converter`
static (`:79`).

### `create_pg_locale_icu` detail (the only compiled function)

C `#else` arm (`:211-218`): `ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
errmsg("ICU is not supported in this build")));` then unreachable `return NULL`.

c2rust (`:644-685`): `errstart`/`errcode`/`errmsg`/`errfinish` with the SQLSTATE
char-arithmetic encoding decoding to `0A000`, message `"ICU is not supported in
this build"`, file `../src/backend/utils/adt/pg_locale_icu.c`, line `215`, func
`create_pg_locale_icu`, then `return NULL`.

Port (`src/lib.rs:33-41`): returns
`Err(PgError::new(ERROR, "ICU is not supported in this build")
.with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
.with_error_location(ErrorLocation::new("../src/backend/utils/adt/pg_locale_icu.c", 215, "create_pg_locale_icu")))`.

Checks:
- **Severity** `ERROR` — matches `ereport(ERROR, ...)`.
- **SQLSTATE** `ERRCODE_FEATURE_NOT_SUPPORTED` = `make_sqlstate(*b"0A000")`
  (`types-error/src/error.rs:69`), verified against the c2rust `errcode`
  char-arithmetic (`'0','A','0','0','0'`). Match.
- **Message** byte-identical `"ICU is not supported in this build"`.
- **Error location** file/line(215)/func match the c2rust `errfinish` literals.
- **No value path** — C's `return NULL` is unreachable after ereport(ERROR);
  the Rust `Err(..)` is the sole exit. Match.
- **Args** — live arm never reads `collid`/`context`; both `_`-prefixed. Match.

Verdict: MATCH on the compiled arm. Classified SEAMED because the entry point is
reached through the owned seam (below). No logic was relocated out of the crate —
the entire live body lives here.

## Seam audit

**Owned seam crates** (by C-source coverage): the single C file `pg_locale_icu.c`
maps to exactly one seam crate, `backend-utils-adt-pg-locale-icu-seams`. No other
`*-seams` crate covers a C file in this unit.

- Declaration `create_pg_locale_icu` (seams `src/lib.rs:19-23`), signature
  `fn(mcx::Mcx<'mcx>, Oid) -> PgResult<PgLocale<'mcx>>`; the owner's impl
  signature (`src/lib.rs:33`) matches exactly.
- **Installed**: `init_seams()` (`src/lib.rs:44-46`) contains only the single
  `set()` call installing this declaration. No uninstalled declaration; no
  `set()` outside the owner.
- **Wired**: `seams-init/src/lib.rs:55` calls `init_seams()` from `init_all()`.
- **Cycle justification**: `pg_locale.c` (unit `backend-utils-adt-pg-locale`)
  calls `create_pg_locale_icu` while `pg_locale_icu.c` consumes `pg_locale.h`
  vocabulary — a genuine cycle once `pg_locale.c` lands. Seam justified.
- **Thinness**: the seam path is pure dispatch; the body is a single `Err(..)`
  construction (the live C arm) with no branching/computation in the seam.

No seam findings.

## Design conformance (step 3b)

- **No invented opacity**: `PgLocale<'mcx> = PgBox<'mcx, PgLocaleStruct>` is the
  real `pg_locale_struct *`, not a stand-in (types.md rules 6-7). The ICU `info`
  union arm and `collate` vtable are deferred to the `pg_locale.c` owner with
  documented rationale (`types-locale/src/lib.rs`).
- **Allocating entry point**: the seam takes `Mcx<'mcx>` and returns `PgResult`
  — the allocation-capable, failure-capable shape required (in the ICU build it
  allocates in `context` and can `ereport(ERROR)`). Conforms to Mcx+PgResult and
  the failure-surface rule.
- **No shared statics / ambient-global seams / locks across `?` / registry side
  tables / unledgered divergence markers** introduced.
- **`CollProvider` codes** verified against `catalog/pg_collation.h`
  (`'d'/'b'/'i'/'c'`). Match.

No design findings.

## Verdict

**PASS.** The only function compiled in the ICU-disabled profile,
`create_pg_locale_icu` (`#else` arm), is reproduced exactly — severity, SQLSTATE
`0A000`, message, error location. All other functions are `#ifdef USE_ICU` and
uncompiled, so they are correctly out of scope and not ported. The single owned
seam is declared, installed by an `init_seams()` containing only `set()`, wired
into `seams-init`, justified by a real cycle, and thin. Zero seam findings, zero
design findings. Crates build clean.
