# Audit: backend-access-hashfunc

C source: `src/backend/access/hash/hashfunc.c` (PostgreSQL 18.3, 428 LOC).
c2rust: `c2rust-runs/backend-access-hashfunc/src/hashfunc.rs`.
Port: `crates/backend-access-hashfunc/src/lib.rs`.

Re-derived independently from the C and the c2rust rendering.

## Function inventory + verdicts

| C fn (line) | Port (typed core / fc adapter) | Verdict | Notes |
|---|---|---|---|
| hashchar (48) | hashchar / fc_hashchar | MATCH | `hash_uint32((int32) char)`; PG `char` is signed → `i8 as i32 as u32`. |
| hashcharextended (54) | hashcharextended / fc_hashcharextended | MATCH | extended seed → `PG_GETARG_INT64(1)`. |
| hashint2 (60) | hashint2 / fc_hashint2 | MATCH | `(int32)(int16)` sign-extend. |
| hashint2extended (66) | hashint2extended / fc_hashint2extended | MATCH | |
| hashint4 (72) | hashint4 / fc_hashint4 | MATCH | |
| hashint4extended (78) | hashint4extended / fc_hashint4extended | MATCH | |
| hashint8 (84) | hashint8 + hashint8_fold / fc_hashint8 | MATCH | lohalf ^= (val>=0)?hihalf:~hihalf, then hash_uint32(lohalf). |
| hashint8extended (104) | hashint8extended / fc_hashint8extended | MATCH | same fold + extended mixer. |
| hashoid (117) | hashoid / fc_hashoid | MATCH | |
| hashoidextended (123) | hashoidextended / fc_hashoidextended | MATCH | |
| hashenum (129) | hashenum / fc_hashenum | MATCH | keyed on pg_enum row Oid, same as hashoid. |
| hashenumextended (135) | hashenumextended / fc_hashenumextended | MATCH | |
| hashfloat4 (141) | hashfloat4 / fc_hashfloat4 | MATCH | ±0→0; widen to f64; canonicalize NaN on key8 (not key); hash 8 bytes. |
| hashfloat4extended (177) | hashfloat4extended / fc_hashfloat4extended | MATCH | ±0→seed; same widen/NaN. |
| hashfloat8 (194) | hashfloat8 / fc_hashfloat8 | MATCH | ±0→0; canonicalize NaN; hash 8 bytes. |
| hashfloat8extended (218) | hashfloat8extended / fc_hashfloat8extended | MATCH | ±0→seed. |
| hashoidvector (233) | hashoidvector / fc_hashoidvector | MATCH | check_valid_oidvector (SEAMED), then hash dim1*sizeof(Oid) bytes of values; arg read via PG_GETARG_POINTER → full struct image, header parsed off offsets 4/8/12/16, values at 24 (sizeof(oidvector)==24, verified types-array). |
| hashoidvectorextended (242) | hashoidvectorextended / fc_hashoidvectorextended | MATCH | |
| hashname (253) | hashname / fc_hashname | MATCH | NameStr-trimmed image (strlen bytes), via by-ref lane. |
| hashnameextended (261) | hashnameextended / fc_hashnameextended | MATCH | |
| hashtext (270) | hashtext / fc_hashtext | MATCH | collid==0 → ERRCODE_INDETERMINATE_COLLATION (msg+hint exact); deterministic → hash VARDATA/EXHDR; non-deterministic → pg_strnxfrm sort key + trailing NUL (bsize+1), via pg_strxfrm seam (full blob) + appended NUL. |
| hashtextextended (325) | hashtextextended / fc_hashtextextended | MATCH | same collation logic, extended mixer + seed. |
| hashvarlena (389) | hashvarlena / fc_hashvarlena | MATCH | hash VARDATA_ANY/VARSIZE_ANY_EXHDR. |
| hashvarlenaextended (404) | hashvarlenaextended / fc_hashvarlenaextended | MATCH | |
| hashbytea (419) | hashbytea / fc_hashbytea | MATCH | delegates to hashvarlena. |
| hashbyteaextended (425) | hashbyteaextended / fc_hashbyteaextended | MATCH | delegates to hashvarlenaextended. |
| check_valid_oidvector (oid.c:118, external) | check_valid_oidvector seam | SEAMED | declared in backend-utils-adt-oid-seams; owner backend-utils-adt-scalar (oid.c) unported → panics until it lands. Validates ndim==1 && dataoffset==0 && elemtype==OIDOID, else ERRCODE_DATATYPE_MISMATCH. |

## Constants verified

- pg_proc OIDs (F_HASH*): cross-checked against `src/include/catalog/pg_proc.dat`
  (hashint2=449, hashint2extended=441, hashint4=450, hashint4extended=425,
  hashint8=949, hashint8extended=442, hashfloat4=451, hashfloat4extended=443,
  hashfloat8=452, hashfloat8extended=444, hashoid=453, hashoidextended=445,
  hashchar=454, hashcharextended=446, hashname=455, hashnameextended=447,
  hashtext=400, hashtextextended=448, hashvarlena=456, hashvarlenaextended=772,
  hashbytea=6413, hashbyteaextended=6414, hashoidvector=457,
  hashoidvectorextended=776, hashenum=3515, hashenumextended=3414). MATCH.
- OIDOID=26 (pg_type.dat), used only in the test stand-in for the seam. MATCH.
- ERRCODE_INDETERMINATE_COLLATION = 42P22 (types-error). MATCH.
- sizeof(oidvector) header = 24 (types-array assert). MATCH.

## Bit mixers (not seamed — direct dep)

`hash_any`/`hash_any_extended`/`hash_uint32`/`hash_uint32_extended` are
`common/hashfn.c`. `common-hashfn` is a direct dependency (no cycle:
access/hash → common is acyclic), so they are called directly
(`hash_bytes`/`hash_bytes_extended`/`hash_bytes_uint32`/
`hash_bytes_uint32_extended`), not through a seam — correct per the
"direct dep by default" rule.

## Seam audit

- Owned seam crates by C-source coverage: hashfunc.c maps to no `X-seams`
  crate (its functions are fmgr builtins, registered, not seams). This crate
  therefore declares no inward seams; `init_seams()` performs the builtin
  registration only (`register_hash_builtins`), wired into
  `seams-init::init_all()`. No empty-installer finding (no owned seam crates).
- Outward seams: `check_valid_oidvector` (NEW, backend-utils-adt-oid-seams),
  `collation_is_deterministic` + `pg_strxfrm` (backend-utils-adt-pg-locale-seams).
  Each is a thin marshal+delegate at the call site. The cycle is real
  (access/hash → utils/adt/oid + pg_locale would cycle). pg_strxfrm is an
  allocating seam: takes `Mcx`, returns `PgResult<PgVec<'mcx,u8>>` — conforms.
- The new `check_valid_oidvector` declaration is the signature its owner will
  install (header fields + `PgResult<()>` for the ereport surface), matching the
  C failure surface rule.

## Design conformance

- No invented opacity: oidvector header parsed off the real image; no handle
  stand-ins.
- Allocating path (`strxfrm_with_nul`) uses a local `MemoryContext` (C's
  palloc+pfree of the scratch buffer); fallible via `?`/PgResult.
- Fallible fmgr adapters raise C's ereport via the repo's structured
  `PGRUST-SQLSTATE:` message panic that `invoke_pgfunction` catches — the
  established dispatch contract.
- No shared statics, no ambient-global seams, no locks across `?`, no registry
  side tables.

## Verdict: PASS

All 26 entry points MATCH; the one external callee is SEAMED per the rules;
zero seam findings; design-conformant. 13 unit tests green.
