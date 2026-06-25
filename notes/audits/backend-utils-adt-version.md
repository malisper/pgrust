# Audit: backend-utils-adt-version

C source: `src/backend/utils/adt/version.c` (PostgreSQL 18.3).
Crate: `crates/backend-utils-adt-version`.

## Function inventory

Enumerated from `version.c` and cross-checked against
`../pgrust/c2rust-runs/backend-utils-adt-version/src/version.rs`. The C file
defines exactly one function (no statics, no inline helpers).

| C function (loc) | Port loc | Verdict | Notes |
|---|---|---|---|
| `pgsql_version` (version.c:21) | `src/lib.rs::pgsql_version` | MATCH / SEAMED | C body is `PG_RETURN_TEXT_P(cstring_to_text(PG_VERSION_STR))`. Port: `cstring_to_text::call(mcx, PG_VERSION_STR)` returning the `text` `Datum`. `fcinfo` carries no args (the C reads none). The `cstring_to_text` call delegates to its real owner `backend-utils-adt-varlena` (a different C unit, `varlena.c`) — a legitimate cross-crate call, thin marshal+delegate, no logic in the path. |

## Constants

- `PG_VERSION_STR` — the configured banner. Verified byte-for-byte against the
  c2rust rendering of this same unit: `"PostgreSQL 18.3 on aarch64-darwin,
  compiled by clang-21.0.0, 64-bit"`. (configure synthesizes this from
  `PG_VERSION` + host + compiler + word size; the c2rust run is the
  authoritative configured value for the porting target.)

## Seam / wiring audit

- This unit's only C file is `version.c`. No other crate calls into it (the SQL
  `version()` function, `pg_proc` OID 89, is a leaf reached only via fmgr
  dispatch / BKI catalog data, not across a Rust dependency cycle). Therefore
  there is **no owned `*-seams` crate** for this unit, and an empty `init_seams()`
  is correct (not a FAIL — the empty-installer FAIL rule applies only when owned
  seam crates exist with undeclared installs).
- `init_seams()` is wired into `seams-init::init_all()` for uniformity; the
  `recurrence_guard` tests pass (both `every_seam_installing_crate_is_wired_into_init_all`
  and `every_declared_seam_is_installed_by_its_owner`).
- The single outward call, `backend_utils_adt_varlena_seams::cstring_to_text`,
  is the canonical existing seam (grep-confirmed in the repo; not invented). It
  takes `Mcx` and returns `PgResult<Datum>` — the allocating-fn contract is
  satisfied. It panics until the varlena owner lands, which is the correct
  mirror-PG-and-panic behavior.

## Design conformance

- No invented opacity; no stand-in type aliases.
- Allocating call carries `Mcx` + `PgResult`.
- No statics, no locks, no `todo!()`/`unimplemented!()`, no registry side tables.
- No unledgered divergence markers.

## Verdict

**PASS.** The one function matches; the single delegation is a justified,
thin cross-unit seam call. `cargo check -p backend-utils-adt-version -p
seams-init` is clean; `recurrence_guard` passes.

## Independent re-audit (2026-06-13, claude-opus-4-8[1m])

Re-derived from scratch against the completeness oracle
`../pgrust/c2rust-runs/backend-utils-adt-version/src/version.rs` and the C
`src/backend/utils/adt/version.c`: the unit defines exactly one function,
`pgsql_version`, with no statics or inline helpers. Confirmed:

- `pgsql_version` MATCH/SEAMED — body `cstring_to_text::call(mcx, PG_VERSION_STR)`
  is the faithful rendering of `PG_RETURN_TEXT_P(cstring_to_text(PG_VERSION_STR))`;
  no args read (C reads none); no logic in the seam path.
- `PG_VERSION_STR` byte-for-byte equal to the c2rust-rendered configured banner.
- `cstring_to_text` seam signature in `backend-utils-adt-varlena-seams`
  (`fn cstring_to_text<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum>`) matches
  the call exactly; it is a real existing seam, not invented.
- No owned `*-seams` crate (version.c has no cyclic inward consumer); empty
  `init_seams()` is correct and wired into `seams-init::init_all()`.
- `recurrence_guard` (both checks) passes; `cargo check --workspace` clean
  (only pre-existing warnings); `cargo test --workspace` green (no new failures).
- No `todo!()`/`unimplemented!()`, no own-logic stubs, no invented opacity, no
  unledgered divergence.

Verdict unchanged: **PASS.**
