# Audit: backend-utils-adt-int — int2vector family

Scope: the `int2vector` I/O family added to `backend-utils-adt-int`
(`src/lib.rs`, `src/fmgr_builtins.rs`), independently re-derived from
`src/backend/utils/adt/int.c` (PG 18.3). The scalar int2/int4 cores were
audited previously; this audit covers only the new functions.

## Function inventory (int.c int2vector section)

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `buildint2vector(int2s, n)` | int.c:107 | lib.rs `buildint2vector` | MATCH | C `palloc0(Int2VectorSize(n))` + sets ndim=1/dataoffset=0/elemtype=INT2OID/dim1=n/lbound1=0. Port routes through `construct_md_array(elems, NULL, 1, [n], [0], INT2OID, 2, byval=true, TYPALIGN_SHORT='s')` — same 1-D, 0-based, no-NULL, short-aligned INT2 array image. Mirrors the audited `buildoidvector` exactly (OIDOID/4/'i' → INT2OID/2/'s'). The `int2s == NULL` "caller fills later" C variant has no caller in the I/O paths; port builds directly from the slice. |
| `check_valid_int2vector(int2Array)` | int.c:135 | lib.rs `check_valid_int2vector` | MATCH | `ndim != 1 \|\| dataoffset != 0 \|\| elemtype != INT2OID` → ERROR / ERRCODE_DATATYPE_MISMATCH / "array is not a valid int2vector". Port takes the three decoded header fields (carrier lives in arrayfuncs); predicate, SQLSTATE (42804), message text identical. |
| `int2vectorin` | int.c:166 | lib.rs `int2vectorin` (+ `strtol_base10`, `soft_error_or_err`) | MATCH | Loop: skip whitespace; break on end; `strtol(_,_,10)`. Three error branches in C order: (1) `intString == endp` → 22P02 `invalid input syntax for type smallint: "%s"`; (2) `errno==ERANGE \|\| l<SHRT_MIN \|\| l>SHRT_MAX` → 22003 `value "%s" is out of range for type smallint`; (3) `*endp && *endp != ' '` → 22P02. All ported with identical predicates/order/SQLSTATE/messages; `%s` = the failing token (post-whitespace-skip `rest`), verified against expected int2.out. `strtol_base10` faithfully models base-10 strtol (optional +/- sign, decimal digits, stops at first non-digit, no-digit → consumed==0 i.e. `intString==endp`, saturate→ERANGE rejection). Soft `escontext` records error + returns None (C `ereturn`); hard error (NULL ctx) → Err. |
| `int2vectorout` | int.c:224 | lib.rs `int2vectorout` | MATCH | `check_valid_int2vector` first, then space-separated `pg_itoa` of each `dim1` value. Port validates header fields then joins `itoa_string(v)` with single spaces. Caller decodes header + values off the array image. |
| `int2vectorrecv` | int.c:258 | — | DEFERRED (not registered) | Needs `array_recv` with the `flinfo->fn_extra` fcinfo-sharing path; stays in builtin_gap_baseline (2410), exactly as `oidvectorrecv` (2420). Not in scope; not faked. |
| `int2vectorsend` | int.c:298 | — | DEFERRED (not registered) | Delegates to `array_send` (fcinfo sharing); stays in baseline (2411), as `oidvectorsend` (2421). |

## fmgr registration (fmgr_builtins.rs)

| Builtin (OID, pg_proc.dat) | Port | Verdict |
|---|---|---|
| `int2vectorin` (40, nargs 1, strict, !retset) | `fc_int2vectorin` | MATCH — reads cstring arg, calls `int2vectorin` (hard path; soft escontext not on fmgr frame, caught by InputFunctionCallSafe → pg_input_* observe soft failure), writes varlena image. |
| `int2vectorout` (41, nargs 1, strict, !retset) | `fc_int2vectorout` | MATCH — decodes header fields (`arr_ndim`/`arr_dataoffset_field`/`arr_elemtype`) + values (`int2vector_to_i16s_bytes`), calls `int2vectorout`, writes cstring. |

OIDs/nargs verified against pg_proc.dat (oid 40/41). `int2vectorin`/`int2vectorout`
removed from `seams-init/src/builtin_gap_baseline.rs`. The
`builtin_registry_matches_canonical_or_baseline` seams-init test passes,
confirming live registry == canonical minus baseline.

## Seam / wiring audit

- No `int.c`-owned `-seams` crate (its value cores are consumed directly; the
  crate's `init_seams()` is `register_int_builtins()` only). No new seam
  declarations. Conforms to the existing crate design.
- Outward calls: `construct_md_array` / `int2vector_to_i16s_bytes` /
  `foundation::arr_*` are direct deps on `backend-utils-adt-arrayfuncs`
  (acyclic — arrayfuncs does not depend on adt-int). No seam needed.
- New dep edges: `backend-utils-adt-arrayfuncs`, `types-core` (added to
  Cargo.toml). `INT2OID = 21` added to `types-core::catalog` (verified against
  pg_type_d.h).

## Design conformance

- No invented opacity: image is a real `mcx::PgVec<u8>` byte array (the
  oidvector-audited pattern), header decoded into typed fields.
- Allocations are fallible via `Mcx`/`PgResult` (`construct_md_array`); the
  scratch `Vec<i16>`/`Vec<Datum>` collection at the fmgr boundary mirrors the
  audited oidvector adapters.
- No shared statics, no ambient globals, no locks held across `?`.
- `strtol_base10` is in-crate logic (not a seam); error paths are `PgError`
  with C SQLSTATE, not owned-logic panics. fc_ adapters are thin marshal+delegate.

## Verdict: PASS

Every in-scope function MATCH; recv/send deferred consistently with the
audited oidvector recv/send (array_recv/array_send fcinfo path). Empirically
verified end-to-end: `'1 2 3'::int2vector` → `1 2 3`; equality → `t`;
int2.sql int2vector body (pg_input_is_valid / pg_input_error_info × 2) matches
expected int2.out exactly; count(*)=415 unregressed.
