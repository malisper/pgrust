# Audit: backend-access-common-scankey

C source: `src/backend/access/common/scankey.c` (scan key support code).
c2rust: `../pgrust/c2rust-runs/backend-access-common-small/src/scankey.rs`.
Port: `crates/backend-access-common-scankey/src/lib.rs`.

## Function inventory

`scankey.c` defines exactly three public functions (no statics, no inline
helpers); c2rust renders all three. Each gets a row.

| # | C function | C loc | Port loc | Verdict | Notes |
|---|------------|-------|----------|---------|-------|
| 1 | `ScanKeyEntryInitialize` | scankey.c:38 | lib.rs `ScanKeyEntryInitialize` | MATCH | See below. |
| 2 | `ScanKeyInit` | scankey.c:75 | lib.rs `ScanKeyInit` | MATCH | Pre-existing; re-verified. |
| 3 | `ScanKeyEntryInitializeWithInfo` | scankey.c:97 | lib.rs `ScanKeyEntryInitializeWithInfo` | MATCH | See below. |

## Per-function comparison

### 1. ScanKeyEntryInitialize — MATCH
C stamps `sk_flags/sk_attno/sk_strategy/sk_subtype/sk_collation/sk_argument`
verbatim from the parameters, then branches on `RegProcedureIsValid(procedure)`
(`procedure != InvalidOid`):
- valid → `fmgr_info(procedure, &entry->sk_func)` (eager resolution).
- invalid → `Assert(flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL))` then
  `MemSet(&entry->sk_func, 0, sizeof(...))`.

Port stamps the same six fields, then `if procedure != InvalidOid` calls
`fmgr_seams::fmgr_info_check::call(procedure)?` (the lookup half of `fmgr_info`,
preserving C's eager lookup-failure `ereport(ERROR)` surface via `PgResult`)
and records `FmgrInfo { fn_oid: procedure, ..Default::default() }` — identical
to how the audited `ScanKeyInit` already crosses the fmgr seam in this owned
`FmgrInfo` model. The else branch is `debug_assert!(flags & (SK_SEARCHNULL |
SK_SEARCHNOTNULL) != 0)` (mirroring C's `Assert`, debug-only) followed by
`FmgrInfo::empty()` (the zeroed `sk_func`). `RegProcedureIsValid`,
`SK_SEARCHNULL=0x40`, `SK_SEARCHNOTNULL=0x80` verified against
`access/skey.h` / headers. MATCH.

### 2. ScanKeyInit — MATCH
flags=0, subtype=InvalidOid, collation=C_COLLATION_OID (950, verified against
`catalog/pg_collation.h` literal in c2rust); the other fields stamped from
params; eager `fmgr_info` via `fmgr_info_check` seam + OID-carrying `FmgrInfo`.
Matches C line-for-line. (Unchanged by this pass; re-derived.)

### 3. ScanKeyEntryInitializeWithInfo — MATCH
C stamps the six plain fields, then
`fmgr_info_copy(&entry->sk_func, finfo, CurrentMemoryContext)`. `fmgr_info_copy`
(fmgr.c) is `memcpy(dst, src, sizeof(FmgrInfo))` followed by resetting
`dst->fn_mcxt = destcxt` and `dst->fn_extra = NULL`. The repo's owned
`types-core::FmgrInfo` carries only the resolved metadata (`fn_addr`, `fn_oid`,
`fn_nargs`, `fn_strict`, `fn_retset`, `fn_stats`) — it has no `fn_extra` /
`fn_mcxt` subsidiary-context fields, so the C copy-plus-reset collapses to a
plain value copy `entry.sk_func = *finfo` (FmgrInfo is `Copy`). Behaviorally
identical on every input. The function is infallible (no fmgr lookup), so it
returns `()` like C's `void`, not `PgResult`. MATCH.

## Seam audit

The crate owns no `*-seams` crate (no C file in its `c_sources` has a
corresponding seam crate; `backend-access-common-scankey-seams` does not
exist). `init_seams()` is therefore correctly absent — nothing to install.

Outward seam usage: `fmgr_seams::fmgr_info_check::call` only. Justified by a
real cycle — `fmgr_info` lives in `backend-utils-fmgr-core`, a heavy crate that
(transitively) depends on scan-key vocabulary; the leaf initializer cannot
depend on it directly. The seam path is thin (one OID in, `PgResult<()>` out,
no branching/computation in the seam). The OOID-carrying `FmgrInfo` model
(re-resolve at call time) is the same one the existing `ScanKeyInit` and
syscache callers use; no invented opacity. No findings.

## Design conformance

No allocation (no `Mcx` needed). No shared statics, no ambient-global seams, no
locks. `debug_assert!` mirrors C's `Assert` (compiled out in release, same as
PG's `Assert` under `!USE_ASSERT_CHECKING`). `SK_SEARCHNOTNULL` added to
`types-scan` with the header-verified value `0x0080`. No findings.

## Verdict: PASS

All three functions MATCH; zero seam findings; zero design findings.
