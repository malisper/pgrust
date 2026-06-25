# Audit: backend-utils-adt-lsn-trigfuncs

- **Unit:** `backend-utils-adt-lsn-trigfuncs` (C: `src/backend/utils/adt/pg_lsn.c`, `src/backend/utils/adt/trigfuncs.c`, PostgreSQL 18.3)
- **Branch:** `port/backend-utils-adt-lsn-trigfuncs`
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) — `claude-opus-4-8[1m]`
- **Verdict:** **PASS**

Independent function-by-function audit per `.claude/skills/audit-crate/SKILL.md`,
re-derived from the C sources, the c2rust rendering, and the Rust port.

## 1. Function inventory

`pg_lsn.c` defines **19** SQL-callable functions plus uses two `hashfunc.c`
helpers (`hashint8`/`hashint8extended`) and one `numeric.c` helper
(`numeric_pg_lsn`). `trigfuncs.c` defines **1** function. Every one gets a row.

### pg_lsn.c (`crates/.../src/pg_lsn.rs`)

| C function | Port | Verdict | Notes |
|---|---|---|---|
| `pg_lsn_in_internal` | `pg_lsn_in_internal` | MATCH | strspn run 1..=8 hex, `/` delim, trailing-NUL check, `(id<<32)|off`. have_error flag preserved. |
| `pg_lsn_in` | `pg_lsn_in` | MATCH | `ereturn` (soft escontext) with 22P02, msg `invalid input syntax for type pg_lsn: "<str>"`. |
| `pg_lsn_out` | `pg_lsn_out` | MATCH | `format!("{:X}/{:X}", lsn>>32, lsn)` == `snprintf("%X/%X", LSN_FORMAT_ARGS)`. |
| `pg_lsn_recv` | `pg_lsn_recv` | MATCH | big-endian int64 read == `pq_getmsgint64`; short buf -> 08P01 (pq_getmsg* protocol-violation). |
| `pg_lsn_send` | `pg_lsn_send` | MATCH | big-endian int64 write == `pq_sendint64`; allocated in mcx (palloc analog). |
| `pg_lsn_eq/ne/lt/gt/le/ge` | same | MATCH | scalar comparisons, byte-identical. |
| `pg_lsn_larger/smaller` | same | MATCH | ternary max/min. |
| `pg_lsn_cmp` | `pg_lsn_cmp` | MATCH | 1/0/-1 in the same branch order. |
| `pg_lsn_hash` | `pg_lsn_hash` | MATCH | delegates to `hashint8(lsn as i64)`. |
| `pg_lsn_hash_extended` | `pg_lsn_hash_extended` | MATCH | delegates to `hashint8extended`. |
| `hashint8` (hashfunc.c) | `hashint8` (in-crate) | MATCH | sign-dependent lo/hi fold -> `hash_uint32` (common-hashfn). hashfunc.c has no crate. |
| `hashint8extended` (hashfunc.c) | `hashint8extended` | MATCH | seeded variant -> `hash_uint32_extended`. |
| `numeric_pg_lsn` (numeric.c) | `numeric_pg_lsn` (in-crate) | MATCH | NaN/Inf -> 0A000 (`cannot convert NaN/infinity to pg_lsn`); else `numericvar_to_uint64`, None -> 22023 `pg_lsn out of range`. numeric crate does not expose it. |
| `pg_lsn_mi` | `pg_lsn_mi` | MATCH | signed decimal string of the unsigned diff -> `numeric_in` (returns on-disk numeric, == DirectFunctionCall3). |
| `pg_lsn_pli` | `pg_lsn_pli` | MATCH | NaN gate (0A000 `cannot add NaN to pg_lsn`) -> numeric_in(lsn) -> numeric_add -> numeric_pg_lsn. |
| `pg_lsn_mii` | `pg_lsn_mii` | MATCH | NaN gate (0A000 `cannot subtract NaN from pg_lsn`) -> numeric_in -> numeric_sub -> numeric_pg_lsn. |

### trigfuncs.c (`crates/.../src/trigfuncs.rs`)

| C function | Port | Verdict | Notes |
|---|---|---|---|
| `suppress_redundant_updates_trigger` | `suppress_redundant_updates_trigger` + `decide`/`tuples_identical`/`payload_eq` | MATCH (body) / SEAMED (fmgr boundary) | 4 protocol checks (39P01, exact msgs), then `t_len==`, `t_hoff==`, `HeapTupleHeaderGetNatts==`, `(infomask&~HEAP_XACT_MASK)==`, and the `memcmp` over `t_len-SizeofHeapTupleHeader` bytes of the `t_bits` FAM tail — same short-circuit order. |
| `TRIGGER_FIRED_BY_UPDATE/BEFORE/FOR_ROW` (trigger.h) | `trigger_fired_*` | MATCH | masks 0x3==0x2, 0x18==0x8, &0x4. |

## 2. Constants verified vs C headers

- `TRIGGER_EVENT_UPDATE 0x2`, `OPMASK 0x3`, `ROW 0x4`, `BEFORE 0x8`, `TIMINGMASK 0x18` — `commands/trigger.h` ✓
- `HEAP_XACT_MASK 0xFFF0`, `SizeofHeapTupleHeader = offsetof(.., t_bits)`, `HeapTupleHeaderGetNatts` — `access/htup_details.h` (types-tuple) ✓
- `MAXPG_LSNCOMPONENT 8`, `MAXPG_LSNLEN 17`, `InvalidXLogRecPtr 0` ✓
- SQLSTATEs: 22P02, 0A000, 22023, 39P01, 08P01 ✓

## 3. Seams and wiring

Owned C files: `pg_lsn.c`, `trigfuncs.c`. Neither has a cyclic inward caller, so
the crate owns **no** `*-seams` crate and correctly has no `init_seams()` (no
auto-FAIL).

Outward calls:
- numeric ops: **direct deps** (no cycle) — `backend-utils-adt-numeric` cores.
- `common-hashfn`: direct dep.
- fmgr/`TriggerData` boundary: `backend-commands-trigger-seams`
  (`called_as_trigger`/`tg_event`/`tg_newtuple`/`tg_trigtuple`). Thin
  marshal+delegate; no logic in the seam path. `trigger.c` (the owner) is not
  yet ported, so these panic until it lands — correct mirror-pg-and-panic, not a
  MISSING body (the whole decision body lives in-crate). `tg_newtuple`/
  `tg_trigtuple` were added to the trigger-seams crate (owned HeapTupleData
  copies). The recurrence guard passes: the owner unit is not COMPLETE, so the
  declared-seam-installed check does not fire.

## 4. Design conformance

- No invented opacity: `TriggerDataRef` is the existing canonical opaque handle;
  no stand-in aliases (grep clean).
- Allocations on palloc paths return `PgResult` over `Mcx` (`pg_lsn_send`,
  numeric bridges); `pg_lsn_out` returns `String` (the cstring output-fn
  contract, == sibling numeric_out / `pstrdup`).
- No shared statics, no ambient-global seams, no held locks, no
  todo!/unimplemented! (grep clean).

## Verdict

**PASS** — every function MATCH (or SEAMED at the genuine fmgr/TriggerData
boundary per step-3 rules). `cargo check --workspace` green; 18 crate tests pass
(regress in/out/recv-send/mi/pli/mii/hash + trigger protocol/comparison);
`seams-init` recurrence guard green.
